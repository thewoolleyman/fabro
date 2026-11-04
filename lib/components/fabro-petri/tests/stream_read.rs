//! The run's stream as the read side serves it: the newest end of a long
//! stream is read without the rows before it, and every workflow node an
//! item names comes back identified by its id, name, kind and display
//! metadata, without the node's script text.

use std::path::Path;

use fabro_db::{Database, DbPool};
use fabro_petri::projector::Projector;
use fabro_types::RunId;
use serde_json::{Value, json};

/// A migrated database file, as the server opens it.
async fn migrated(path: &Path) -> Database {
    let database = Database::connect(path).await.expect("the database opens");
    database.migrate().await.expect("the migrations run");
    database
}

/// Store the items as the run's stream, at `stream_seq` 1, 2, ...
async fn seed_stream(pool: &DbPool, run_id: RunId, items: &[Value]) {
    let mut transaction = pool.begin().await.expect("a transaction begins");
    for (index, item) in items.iter().enumerate() {
        let seq = i64::try_from(index + 1).expect("a small sequence");
        sqlx::query(
            "INSERT INTO petri_stream (run_id, stream_seq, item_kind, item_id, event_json) VALUES \
             (?, ?, 'petri', ?, ?)",
        )
        .bind(run_id.to_string())
        .bind(seq)
        .bind(seq.to_string())
        .bind(item.to_string())
        .execute(&mut *transaction)
        .await
        .expect("a stream item inserts");
    }
    transaction.commit().await.expect("the stream commits");
}

/// A command node as Petri's `NodeRef` carries it: `meta` verbatim from the
/// Attractor frontend, the script included.
fn command_node(id: u64, name: &str) -> Value {
    json!({
        "id": id,
        "name": name,
        "kind": "attractor/command",
        "meta": {
            "label": format!("Run {name}"),
            "kind": "command",
            "edges": {"0": {"condition": "outcome=success"}},
            "script": format!("#!/bin/sh\n# the whole of {name}'s script\nmake {name}\n"),
        }
    })
}

/// A derived `route.applied` item about `from`, routing to `to`, with a
/// record body that carries its own copy of the node.
fn route_applied(from: &str, to: &str) -> Value {
    json!({
        "id": {"log": "execution", "execution": 0, "seq": 5, "index": 1},
        "origin": "derived",
        "context": {"invocation": 0, "execution": 0},
        "recorded_at": 1_789_706_579_000_u64,
        "subject": {
            "node": command_node(2, from),
            "firing": 2, "visit": 1, "attempt": 1, "generation": 0, "branch": {"role": "none"}
        },
        "record": {"seq": 5, "body": {"event": "route.applied", "edge": 0, "node": command_node(2, from)}},
        "derived": {
            "event": "route.applied",
            "target": command_node(3, to),
            "back": false,
            "results": [{"node": command_node(4, "branch"), "status": "success"}]
        }
    })
}

/// Every object under `value` that has a `script` key, by JSON pointer.
fn scripts_in(value: &Value, at: &str, found: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if object.contains_key("script") {
                found.push(at.to_string());
            }
            for (key, child) in object {
                scripts_in(child, &format!("{at}/{key}"), found);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                scripts_in(child, &format!("{at}/{index}"), found);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn the_stream_tail_is_its_newest_items_in_order() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let database = migrated(&dir.path().join("fabro.sqlite3")).await;
    let projector = Projector::new(database.pool().clone(), database.pool().clone());
    let run_id = RunId::new();
    let items: Vec<Value> = (1..=50).map(|n| json!({"n": n})).collect();
    seed_stream(database.pool(), run_id, &items).await;

    let tail = projector
        .stream_tail(run_id, 0, 3)
        .await
        .expect("the tail reads");
    let seqs: Vec<u64> = tail.iter().map(|item| item.stream_seq).collect();
    assert_eq!(
        seqs,
        vec![48, 49, 50],
        "the newest three items, oldest first"
    );
    assert_eq!(tail[0].item, json!({"n": 48}));

    let past_cursor = projector
        .stream_tail(run_id, 49, 3)
        .await
        .expect("the tail past a cursor reads");
    let seqs: Vec<u64> = past_cursor.iter().map(|item| item.stream_seq).collect();
    assert_eq!(seqs, vec![50], "no item at or before the cursor");
}

#[tokio::test]
async fn a_stream_item_names_its_nodes_without_their_scripts() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let database = migrated(&dir.path().join("fabro.sqlite3")).await;
    let projector = Projector::new(database.pool().clone(), database.pool().clone());
    let run_id = RunId::new();
    seed_stream(database.pool(), run_id, &[route_applied("build", "test")]).await;

    for (read, items) in [
        (
            "after",
            projector.stream_after(run_id, 0, 10).await.expect("reads"),
        ),
        (
            "tail",
            projector.stream_tail(run_id, 0, 10).await.expect("reads"),
        ),
    ] {
        assert_eq!(items.len(), 1, "{read}");
        let item = &items[0].item;
        let mut found = Vec::new();
        scripts_in(item, "", &mut found);
        assert!(found.is_empty(), "{read}: scripts left at {found:?}");
        let node = &item["subject"]["node"];
        assert_eq!(node["id"], 2, "{read}");
        assert_eq!(node["name"], "build", "{read}");
        assert_eq!(node["kind"], "attractor/command", "{read}");
        assert_eq!(node["meta"]["label"], "Run build", "{read}");
        assert_eq!(node["meta"]["kind"], "command", "{read}");
        assert_eq!(
            node["meta"]["edges"]["0"]["condition"], "outcome=success",
            "{read}: the edge table a route's condition is read from stays"
        );
        assert_eq!(item["derived"]["target"]["name"], "test", "{read}");
        assert_eq!(
            item["derived"]["target"]["meta"]["label"], "Run test",
            "{read}"
        );
    }
}
