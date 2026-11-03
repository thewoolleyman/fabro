//! A write lock held past the busy timeout, and a delete of many rows: a
//! retried `BEGIN IMMEDIATE` outlasts the holder where a plain one fails with
//! "database is locked", and a batched delete commits one short transaction
//! per batch, letting another writer in between batches.

use std::path::Path;
use std::time::Duration;

use fabro_db::{
    BatchedDelete, BusyRetry, Database, DeleteBatches, begin_immediate_with, delete_in_batches,
    is_busy,
};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::time::{Instant, sleep};

/// The busy timeout of the pool the tests contend on: short, so a lock held
/// "past the busy timeout" takes milliseconds, not the production 5 s.
const SHORT_BUSY_TIMEOUT: Duration = Duration::from_millis(100);

/// A retry quick enough for tests, with room to outlast the holders below.
const TEST_RETRY: BusyRetry = BusyRetry {
    budget:        Duration::from_secs(10),
    first_backoff: Duration::from_millis(20),
    max_backoff:   Duration::from_millis(100),
};

/// The database as the server opens it, with a `rows` table to write.
async fn database(path: &Path) -> Database {
    let database = Database::connect(path).await.expect("the database opens");
    sqlx::query("CREATE TABLE rows (key TEXT NOT NULL, n INTEGER NOT NULL, body TEXT NOT NULL)")
        .execute(database.pool())
        .await
        .expect("the table is created");
    sqlx::query("CREATE INDEX rows_by_key ON rows(key)")
        .execute(database.pool())
        .await
        .expect("the index is created");
    database
}

/// A second pool on the same file whose busy timeout is
/// [`SHORT_BUSY_TIMEOUT`].
async fn short_busy_pool(path: &Path) -> SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(SHORT_BUSY_TIMEOUT);
    SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .expect("the short-busy pool opens")
}

async fn insert_rows(pool: &SqlitePool, key: &str, count: i64) {
    let mut transaction = pool.begin().await.expect("a transaction begins");
    for n in 0..count {
        sqlx::query("INSERT INTO rows (key, n, body) VALUES (?, ?, ?)")
            .bind(key)
            .bind(n)
            .bind("x".repeat(64))
            .execute(&mut *transaction)
            .await
            .expect("a row inserts");
    }
    transaction.commit().await.expect("the rows commit");
}

async fn count_rows(pool: &SqlitePool, key: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM rows WHERE key = ?")
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("the rows count")
}

#[tokio::test]
async fn a_retried_begin_outlasts_a_write_lock_held_past_the_busy_timeout() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("fabro.sqlite3");
    let holder = database(&path).await;
    let contender = short_busy_pool(&path).await;

    let held = holder
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("the holder takes the write lock");

    // The defect: a plain BEGIN IMMEDIATE waits the busy timeout and fails.
    let plain = contender
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect_err("a plain begin fails while the lock is held");
    assert!(is_busy(&plain), "the failure is SQLITE_BUSY: {plain}");
    assert!(
        plain.to_string().contains("database is locked"),
        "the failure reads as the production one: {plain}"
    );

    // The fix: the retried begin keeps waiting, well past the busy timeout.
    let retried = tokio::spawn({
        let contender = contender.clone();
        async move {
            let mut transaction = begin_immediate_with(&contender, TEST_RETRY).await?;
            sqlx::query("INSERT INTO rows (key, n, body) VALUES ('appended', 0, '')")
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await
        }
    });
    sleep(SHORT_BUSY_TIMEOUT * 6).await;
    assert!(
        !retried.is_finished(),
        "the retried write waits for the holder instead of failing"
    );
    held.commit().await.expect("the holder commits");
    retried
        .await
        .expect("the write task joins")
        .expect("the retried write commits once the lock is free");
    assert_eq!(count_rows(holder.pool(), "appended").await, 1);
}

#[tokio::test]
async fn a_retried_begin_gives_up_when_its_budget_runs_out() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("fabro.sqlite3");
    let holder = database(&path).await;
    let contender = short_busy_pool(&path).await;
    let _held = holder
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("the holder takes the write lock");

    let budget = Duration::from_millis(400);
    let started = Instant::now();
    let error = begin_immediate_with(&contender, BusyRetry {
        budget,
        ..TEST_RETRY
    })
    .await
    .expect_err("the write fails once the budget is spent");
    assert!(is_busy(&error), "the last attempt's busy error is returned");
    let elapsed = started.elapsed();
    assert!(
        elapsed + TEST_RETRY.max_backoff >= budget,
        "the write retried until the budget was nearly spent: {elapsed:?}"
    );
    assert!(
        elapsed < budget + SHORT_BUSY_TIMEOUT * 10,
        "the write stopped within the budget plus about one busy timeout: {elapsed:?}"
    );
}

#[test]
fn only_a_locked_database_counts_as_busy() {
    assert!(!is_busy(&sqlx::Error::RowNotFound));
    assert!(!is_busy(&sqlx::Error::PoolTimedOut));
}

#[tokio::test]
async fn a_delete_of_many_rows_commits_one_transaction_per_batch() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let database = database(&dir.path().join("fabro.sqlite3")).await;
    insert_rows(database.pool(), "deleted", 950).await;
    insert_rows(database.pool(), "kept", 30).await;

    let done = delete_in_batches(database.pool(), "rows", "key", "deleted", DeleteBatches {
        rows:  100,
        pause: Duration::ZERO,
        retry: TEST_RETRY,
    })
    .await
    .expect("the delete runs");

    assert_eq!(done, BatchedDelete {
        rows:         950,
        transactions: 10,
    });
    assert_eq!(count_rows(database.pool(), "deleted").await, 0);
    assert_eq!(count_rows(database.pool(), "kept").await, 30);

    let again = delete_in_batches(database.pool(), "rows", "key", "deleted", DeleteBatches {
        rows:  100,
        pause: Duration::ZERO,
        retry: TEST_RETRY,
    })
    .await
    .expect("a repeated delete runs");
    assert_eq!(again, BatchedDelete {
        rows:         0,
        transactions: 1,
    });
}

#[tokio::test]
async fn a_writer_takes_the_lock_between_batches_of_a_long_delete() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("fabro.sqlite3");
    let database = database(&path).await;
    let writer = short_busy_pool(&path).await;
    let total = 1000;
    insert_rows(database.pool(), "deleted", total).await;

    let delete = tokio::spawn({
        let pool = database.clone_pool();
        async move {
            delete_in_batches(&pool, "rows", "key", "deleted", DeleteBatches {
                rows:  100,
                pause: Duration::from_millis(150),
                retry: TEST_RETRY,
            })
            .await
        }
    });

    // Once the first batch has committed, a writer whose busy timeout is far
    // shorter than the whole delete gets the lock while the delete is still
    // running, and sees the rows not yet deleted.
    while count_rows(database.pool(), "deleted").await == total {
        sleep(Duration::from_millis(5)).await;
    }
    let mut transaction = begin_immediate_with(&writer, TEST_RETRY)
        .await
        .expect("the writer takes the lock mid-delete");
    let remaining = count_rows_in(&mut transaction, "deleted").await;
    sqlx::query("INSERT INTO rows (key, n, body) VALUES ('appended', 0, '')")
        .execute(&mut *transaction)
        .await
        .expect("the writer inserts");
    transaction.commit().await.expect("the writer commits");
    assert!(
        remaining > 0 && remaining < total,
        "the writer committed between batches, with {remaining} of {total} rows left"
    );

    let done = delete
        .await
        .expect("the delete task joins")
        .expect("the delete finishes");
    assert_eq!(done, BatchedDelete {
        rows:         1000,
        transactions: 11,
    });
    assert_eq!(count_rows(database.pool(), "deleted").await, 0);
    assert_eq!(count_rows(database.pool(), "appended").await, 1);
}

async fn count_rows_in(transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>, key: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM rows WHERE key = ?")
        .bind(key)
        .fetch_one(&mut **transaction)
        .await
        .expect("the rows count")
}
