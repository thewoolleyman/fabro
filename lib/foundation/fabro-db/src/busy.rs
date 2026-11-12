//! Writing to a shared SQLite database without failing at the first busy
//! timeout, and deleting many rows without holding the write lock for long.
//!
//! Every pool [`Database::connect`](crate::Database::connect) opens waits up
//! to its busy timeout for the write lock. A writer that holds the lock for
//! longer than that (a large delete on a large store, say) makes every other
//! writer fail with `SQLITE_BUSY`, "database is locked". Two remedies live
//! here, one for each side:
//!
//! - [`begin_immediate`] opens a `BEGIN IMMEDIATE` transaction and, when the
//!   lock stays held past the busy timeout, retries with a bounded backoff
//!   inside a fixed budget ([`BusyRetry`]) instead of failing the write.
//! - [`delete_in_batches`] deletes a key's rows a bounded batch at a time, each
//!   batch in a `BEGIN IMMEDIATE` transaction of its own, pausing between
//!   batches, so no one delete holds the lock anywhere near the busy timeout
//!   and waiting writers get in between batches.

use std::time::Duration;

use sqlx::{AssertSqlSafe, Sqlite, Transaction};
use tokio::time::{Instant, sleep};
use tracing::{debug, warn};

use crate::DbPool;

/// The SQLite primary result code `SQLITE_BUSY`: "database is locked".
const SQLITE_BUSY: i32 = 5;
/// The SQLite primary result code `SQLITE_LOCKED`: "database table is
/// locked".
const SQLITE_LOCKED: i32 = 6;

/// Whether `error` is SQLite reporting the database (or a table of it)
/// locked by another connection: `SQLITE_BUSY` or `SQLITE_LOCKED`, under any
/// extended code.
#[must_use]
pub fn is_busy(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };
    database
        .code()
        .and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| matches!(code & 0xff, SQLITE_BUSY | SQLITE_LOCKED))
}

/// How a write that finds the database locked retries.
///
/// Each attempt itself waits up to the pool's busy timeout (5 s for
/// [`Database::connect`](crate::Database::connect)). A failed attempt sleeps
/// a backoff that starts at `first_backoff` and doubles up to `max_backoff`,
/// and no new attempt starts once `budget` has elapsed since the first, so
/// the whole write ends within `budget` plus one busy timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BusyRetry {
    pub budget:        Duration,
    pub first_backoff: Duration,
    pub max_backoff:   Duration,
}

impl BusyRetry {
    /// The retry the server's record appends use. With the 5 s busy timeout
    /// it ends a write within about 25 s, inside the 30 s request timeout a
    /// run's worker gives the server, so a worker never gives up on an append
    /// the server is still retrying.
    pub const DEFAULT: Self = Self {
        budget:        Duration::from_secs(20),
        first_backoff: Duration::from_millis(50),
        max_backoff:   Duration::from_secs(1),
    };
}

impl Default for BusyRetry {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Begin a `BEGIN IMMEDIATE` transaction on `pool`, retrying under
/// [`BusyRetry::DEFAULT`] while the database is locked.
pub async fn begin_immediate(pool: &DbPool) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    begin_immediate_with(pool, BusyRetry::DEFAULT).await
}

/// Begin a `BEGIN IMMEDIATE` transaction on `pool`, retrying under `retry`
/// while the database is locked. Any other error, and the busy error of the
/// last attempt the budget allows, is returned as it is.
///
/// `BEGIN IMMEDIATE` takes the write lock up front, so in WAL mode the
/// statements the caller then runs in the transaction do not meet a busy
/// lock of their own: retrying the begin retries the whole write.
pub async fn begin_immediate_with(
    pool: &DbPool,
    retry: BusyRetry,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    let started = Instant::now();
    let mut backoff = retry.first_backoff;
    let mut attempt: u32 = 1;
    loop {
        match pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => {
                if attempt > 1 {
                    debug!(
                        attempt,
                        waited_ms = started.elapsed().as_millis(),
                        "SQLite write lock taken after retrying a locked database"
                    );
                }
                return Ok(transaction);
            }
            Err(error) if is_busy(&error) && started.elapsed() + backoff < retry.budget => {
                warn!(
                    attempt,
                    waited_ms = started.elapsed().as_millis(),
                    backoff_ms = backoff.as_millis(),
                    error = %error,
                    "SQLite database locked past the busy timeout; retrying the write"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(retry.max_backoff);
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// How [`delete_in_batches`] paces a delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteBatches {
    /// The most rows one transaction deletes.
    pub rows:  u32,
    /// The pause after each committed batch that deleted a full batch, so
    /// writers waiting on the lock take it before the next batch does.
    pub pause: Duration,
    /// How each batch's transaction retries a locked database.
    pub retry: BusyRetry,
}

impl DeleteBatches {
    /// The pacing run deletion uses: batches small enough to commit in
    /// milliseconds even when rows carry large records.
    pub const DEFAULT: Self = Self {
        rows:  200,
        pause: Duration::from_millis(20),
        retry: BusyRetry::DEFAULT,
    };
}

impl Default for DeleteBatches {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a [`delete_in_batches`] call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchedDelete {
    /// The rows deleted.
    pub rows:         u64,
    /// The write transactions committed, one per batch.
    pub transactions: u64,
}

/// Delete every row of `table` whose `column` equals `key`, at most
/// `batches.rows` rows per `BEGIN IMMEDIATE` transaction, until a batch
/// deletes fewer than a full batch.
///
/// The delete is not atomic across batches: a failure (or a crash) between
/// batches leaves the rows not yet reached, and calling it again deletes
/// them. `table` and `column` are SQL identifiers the caller names in code,
/// never input; `table` must be a rowid table.
pub async fn delete_in_batches(
    pool: &DbPool,
    table: &'static str,
    column: &'static str,
    key: &str,
    batches: DeleteBatches,
) -> Result<BatchedDelete, sqlx::Error> {
    let sql = format!(
        "DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {column} = ? LIMIT ?)"
    );
    let limit = i64::from(batches.rows.max(1));
    let mut done = BatchedDelete::default();
    loop {
        let mut transaction = begin_immediate_with(pool, batches.retry).await?;
        let deleted = sqlx::query(AssertSqlSafe(sql.as_str()))
            .bind(key)
            .bind(limit)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        transaction.commit().await?;
        done.rows += deleted;
        done.transactions += 1;
        if deleted < limit.unsigned_abs() {
            return Ok(done);
        }
        sleep(batches.pause).await;
    }
}
