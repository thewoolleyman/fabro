//! The run's stream as the view tables hold it: one row per folded item,
//! written by a pass and read back in `stream_seq` order.

use fabro_db::DbPool;
use fabro_types::{RunId, RunStreamItem, RunStreamItemKind};
use petri_execution::events::{EventId, EventSource};

use super::{Positions, ProjectError};
use crate::projection::{Item, RunView};

pub(crate) struct StreamRow {
    pub(super) stream_seq: u64,
    pub(super) item_kind:  &'static str,
    pub(super) item_id:    String,
    pub(super) event_json: String,
}

/// Fold the items into the view in order, each at the next delivery
/// sequence, advancing the positions with each, and produce its stream
/// row.
pub(crate) fn stream_rows(
    items: &[Item<'_>],
    view: &mut RunView,
    positions: &mut Positions,
    stream_seq: &mut u64,
) -> Result<Vec<StreamRow>, ProjectError> {
    let mut rows = Vec::with_capacity(items.len());
    for item in items {
        *stream_seq += 1;
        view.fold(item, *stream_seq);
        let row = match item {
            Item::Petri(event) => {
                positions.advance(event.id);
                StreamRow {
                    stream_seq: *stream_seq,
                    item_kind:  "petri",
                    item_id:    event_id_text(&event.id),
                    event_json: serde_json::to_string(event).map_err(ProjectError::Encode)?,
                }
            }
            Item::Platform(record) => {
                positions.platform_seq = record.seq;
                StreamRow {
                    stream_seq: *stream_seq,
                    item_kind:  "platform",
                    item_id:    record.seq.to_string(),
                    event_json: serde_json::to_string(record).map_err(ProjectError::Encode)?,
                }
            }
        };
        rows.push(row);
    }
    Ok(rows)
}

/// The run's stream past the cursor, read from the view tables: up to
/// `limit` rows with `stream_seq > after`, in order, in Fabro's envelope.
pub(super) async fn stream_after(
    views: &DbPool,
    run_id: RunId,
    after: u64,
    limit: usize,
) -> Result<Vec<RunStreamItem>, ProjectError> {
    let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT stream_seq, item_kind, item_id, event_json FROM petri_stream WHERE run_id = ? AND \
         stream_seq > ? ORDER BY stream_seq LIMIT ?",
    )
    .bind(run_id.to_string())
    .bind(column(after))
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(views)
    .await
    .map_err(ProjectError::Database)?;
    rows.into_iter()
        .map(|row| stream_item(run_id, row))
        .collect()
}

/// The newest end of the run's stream past the cursor: the last `limit`
/// rows with `stream_seq > after`, in `stream_seq` order, in Fabro's
/// envelope. A reader that wants the stream's tail reads only the rows it
/// keeps, not every row from the first.
pub(super) async fn stream_tail(
    views: &DbPool,
    run_id: RunId,
    after: u64,
    limit: usize,
) -> Result<Vec<RunStreamItem>, ProjectError> {
    let mut rows: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT stream_seq, item_kind, item_id, event_json FROM petri_stream WHERE run_id = ? AND \
         stream_seq > ? ORDER BY stream_seq DESC LIMIT ?",
    )
    .bind(run_id.to_string())
    .bind(column(after))
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(views)
    .await
    .map_err(ProjectError::Database)?;
    rows.reverse();
    rows.into_iter()
        .map(|row| stream_item(run_id, row))
        .collect()
}

/// One stored stream row in Fabro's envelope, with every node it names
/// reduced to its identity (see [`drop_node_scripts`]).
fn stream_item(
    run_id: RunId,
    (stream_seq, item_kind, item_id, event_json): (i64, String, String, String),
) -> Result<RunStreamItem, ProjectError> {
    let mut item: serde_json::Value =
        serde_json::from_str(&event_json).map_err(ProjectError::Encode)?;
    drop_node_scripts(&mut item);
    let kind = match item_kind.as_str() {
        "platform" => RunStreamItemKind::Platform,
        _ => RunStreamItemKind::Petri,
    };
    let recorded_at = item
        .get("recorded_at")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    Ok(RunStreamItem {
        run_id,
        stream_seq: u64::try_from(stream_seq).unwrap_or(0),
        kind,
        id: item_id,
        recorded_at,
        item,
    })
}

/// Drop the script text from every workflow node the item names.
///
/// Petri's `NodeRef` carries the node's `meta` verbatim, and the Attractor
/// frontend keeps a command node's whole script under `meta.script`, so
/// every item about a command node repeated its script: measured on a
/// five-hour run, 81% of the bytes `GET /runs/{id}/events` served. A node
/// in the stream is identified by its `id`, `name`, `kind` and the rest of
/// its `meta` (`label`, `kind`, `edges`, `synthetic`, `branch`); the script
/// is a fact of the workflow graph, and the run projection's stage keeps it
/// as the stage's `script_invocation`. A node is any object with an
/// integer `id`, a string `name` and `kind`, and an object `meta`, at any
/// depth: the subject's node, a route's `target`, a group's `target`, a
/// clone's `entry`, a branch result's `node`, a fork's `fork`.
pub(crate) fn drop_node_scripts(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            let is_node = object.get("id").is_some_and(serde_json::Value::is_u64)
                && object.get("name").is_some_and(serde_json::Value::is_string)
                && object.get("kind").is_some_and(serde_json::Value::is_string);
            if is_node {
                if let Some(serde_json::Value::Object(meta)) = object.get_mut("meta") {
                    meta.remove("script");
                }
            }
            for child in object.values_mut() {
                drop_node_scripts(child);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                drop_node_scripts(child);
            }
        }
        _ => {}
    }
}

/// A Petri event id as the stream names it: `<log>/<seq>/<index>`.
#[must_use]
pub(crate) fn event_id_text(id: &EventId) -> String {
    format!("{}/{}/{}", log_text(&id.source), id.seq, id.index)
}

pub(super) fn log_text(source: &EventSource) -> String {
    match source {
        EventSource::Coordinator => "coordinator".to_string(),
        EventSource::Execution { execution } => format!("execution {execution}"),
    }
}

pub(super) fn column(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
