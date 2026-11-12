//! The run store under write contention: a run's record append waits out a
//! write lock another writer holds past the busy timeout instead of failing
//! the run's store with "database is locked", and a run's deletion commits
//! its rows in short batches, so other runs' appends keep landing while it
//! runs.

use std::path::Path;
use std::time::Duration;

use fabro_db::{BatchedDelete, BusyRetry, Database, DbPool, DeleteBatches};
use fabro_petri::SqliteRunStore;
use fabro_petri::projector::Projector;
use fabro_types::RunId;
use petri_store::{Access, ExecutionId, LogId, OwnerId, RunKey, RunStore as _};
use petri_testkit::run_store;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::time::sleep;

/// The busy timeout of the pool the appends run on: short, so a lock held
/// "past the busy timeout" takes milliseconds, not the production 5 s.
const SHORT_BUSY_TIMEOUT: Duration = Duration::from_millis(100);

/// A migrated database file, as the server opens it.
async fn migrated(path: &Path) -> Database {
    let database = Database::connect(path).await.expect("the database opens");
    database.migrate().await.expect("the migrations run");
    database
}

/// A second pool over the same file whose busy timeout is
/// [`SHORT_BUSY_TIMEOUT`].
async fn short_busy_pool(path: &Path) -> DbPool {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(SHORT_BUSY_TIMEOUT);
    SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .expect("the short-busy pool opens")
}

fn execution_log() -> LogId {
    LogId::Execution(ExecutionId::new(0))
}

/// The production failure, reproduced: another writer holds the write lock
/// for six busy timeouts. The append waits it out and lands, where it used to
/// fail with "could not begin an append ... database is locked".
#[tokio::test]
async fn an_append_waits_out_a_write_lock_held_past_the_busy_timeout() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("fabro.sqlite3");
    let holder = migrated(&path).await;
    let store = SqliteRunStore::new(short_busy_pool(&path).await);
    let logs = store
        .open(&RunKey::new("appending"), Access::Create {
            owner: OwnerId::new("worker"),
        })
        .await
        .expect("the run is created");

    let held = holder
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("the holder takes the write lock");
    let append = tokio::spawn({
        let logs = logs.clone();
        async move {
            logs.append(&execution_log(), &[run_store::record(0, "step.started")])
                .await
        }
    });
    sleep(SHORT_BUSY_TIMEOUT * 6).await;
    assert!(
        !append.is_finished(),
        "the append waits for the holder instead of failing the run's store"
    );
    held.commit().await.expect("the holder commits");
    append
        .await
        .expect("the append task joins")
        .expect("the append lands once the lock is free");
    assert_eq!(logs.read(&execution_log()).await.expect("reads"), vec![
        run_store::record(0, "step.started")
    ]);
}

/// Seed a run's rows in every table a deletion clears.
async fn seed_run(pool: &DbPool, run_id: &str, records: i64, platform: i64, stream: i64) {
    let body = format!("{{\"pad\":\"{}\"}}", "x".repeat(512));
    let mut transaction = pool.begin().await.expect("a transaction begins");
    sqlx::query("INSERT INTO petri_runs (run_id, created_at_ms) VALUES (?, 0)")
        .bind(run_id)
        .execute(&mut *transaction)
        .await
        .expect("the run row inserts");
    for seq in 0..records {
        sqlx::query(
            "INSERT INTO petri_records (run_id, log, seq, recorded_at, record_json) VALUES (?, \
             'execution 0', ?, 0, ?)",
        )
        .bind(run_id)
        .bind(seq)
        .bind(&body)
        .execute(&mut *transaction)
        .await
        .expect("a Petri record inserts");
    }
    for seq in 1..=platform {
        sqlx::query(
            "INSERT INTO platform_records (run_id, seq, recorded_at, kind, record_json) VALUES \
             (?, ?, 0, 'note', '{}')",
        )
        .bind(run_id)
        .bind(seq)
        .execute(&mut *transaction)
        .await
        .expect("a platform record inserts");
    }
    for seq in 1..=stream {
        sqlx::query(
            "INSERT INTO petri_stream (run_id, stream_seq, item_kind, item_id, event_json) VALUES \
             (?, ?, 'petri', ?, '{}')",
        )
        .bind(run_id)
        .bind(seq)
        .bind(seq.to_string())
        .execute(&mut *transaction)
        .await
        .expect("a stream item inserts");
    }
    sqlx::query(
        "INSERT INTO petri_projection (run_id, projection_json, fold_json, positions_json, \
         stream_seq, updated_at_ms) VALUES (?, '{}', '{}', '{}', ?, 0)",
    )
    .bind(run_id)
    .bind(stream)
    .execute(&mut *transaction)
    .await
    .expect("the projection inserts");
    transaction.commit().await.expect("the run's rows commit");
}

async fn rows_of(pool: &DbPool, run_id: &str) -> [i64; 5] {
    let mut counts = [0; 5];
    for (count, table) in counts.iter_mut().zip([
        "petri_projection",
        "petri_stream",
        "platform_records",
        "petri_records",
        "petri_runs",
    ]) {
        *count = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {table} WHERE run_id = ?"
        )))
        .bind(run_id)
        .fetch_one(pool)
        .await
        .expect("the rows count");
    }
    counts
}

/// A run's deletion commits one short transaction per batch per table, and
/// leaves every other run's rows alone.
#[tokio::test]
async fn a_run_deletion_commits_its_rows_in_batches() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let database = migrated(&dir.path().join("fabro.sqlite3")).await;
    let pool = database.clone_pool();
    let deleted = RunId::new();
    let kept = RunId::new();
    seed_run(&pool, &deleted.to_string(), 450, 120, 50).await;
    seed_run(&pool, &kept.to_string(), 10, 3, 2).await;
    let projector = Projector::new(pool.clone(), pool.clone());

    let done = projector
        .delete_run_in_batches(deleted, DeleteBatches {
            rows:  100,
            pause: Duration::ZERO,
            retry: BusyRetry::DEFAULT,
        })
        .await
        .expect("the run is deleted");

    // projection 1 row: 1 batch; stream 50: 1; platform 120: 2; records
    // 450: 5; the run row: 1.
    assert_eq!(done, BatchedDelete {
        rows:         1 + 50 + 120 + 450 + 1,
        transactions: 1 + 1 + 2 + 5 + 1,
    });
    assert_eq!(rows_of(&pool, &deleted.to_string()).await, [0; 5]);
    assert_eq!(rows_of(&pool, &kept.to_string()).await, [1, 2, 3, 10, 1]);

    // A repeated delete (as after one cut short) finds nothing left.
    projector
        .delete_run(deleted)
        .await
        .expect("a repeated delete succeeds");
}

/// Other runs' appends keep landing while a run with many records is
/// deleted, on a pool whose busy timeout is far shorter than the deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appends_land_while_a_large_run_is_deleted() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("fabro.sqlite3");
    let database = migrated(&path).await;
    let pool = database.clone_pool();
    let deleted = RunId::new();
    seed_run(&pool, &deleted.to_string(), 3000, 300, 300).await;
    let projector = Projector::new(pool.clone(), pool.clone());

    let store = SqliteRunStore::new(short_busy_pool(&path).await);
    let logs = store
        .open(&RunKey::new("live"), Access::Create {
            owner: OwnerId::new("worker"),
        })
        .await
        .expect("the live run is created");

    let delete = tokio::spawn({
        let projector = projector.clone();
        async move {
            projector
                .delete_run_in_batches(deleted, DeleteBatches {
                    rows:  100,
                    pause: Duration::from_millis(5),
                    retry: BusyRetry::DEFAULT,
                })
                .await
        }
    });
    let mut seq = 0;
    while !delete.is_finished() {
        logs.append(&execution_log(), &[run_store::record(seq, "step.progress")])
            .await
            .expect("an append lands during the deletion");
        seq += 1;
    }
    let done = delete
        .await
        .expect("the delete task joins")
        .expect("the run is deleted");
    assert!(
        done.transactions > 30,
        "the deletion ran as many short transactions: {done:?}"
    );
    assert!(seq > 0, "appends ran while the deletion did");
    assert_eq!(
        u64::try_from(logs.read(&execution_log()).await.expect("reads").len()).unwrap(),
        seq
    );
    assert_eq!(rows_of(&pool, &deleted.to_string()).await, [0; 5]);
}
