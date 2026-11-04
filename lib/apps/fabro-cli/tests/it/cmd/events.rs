use fabro_test::{fabro_snapshot, json_elapsed_ms_snapshot_filters, test_context};
use httpmock::MockServer;
use serde_json::{Value, json};

use super::support::{mock_resolved_run, setup_detached_dry_run, setup_seeded_completed_dry_run};
use crate::support::unique_run_id;

fn parse_ndjson(stdout: &[u8]) -> Vec<Value> {
    String::from_utf8(stdout.to_vec())
        .expect("stdout should be valid UTF-8")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("events output should be valid NDJSON")
        })
        .collect()
}

/// The name of a stream item: a platform record's kind, or a Petri
/// event's recorded (or derived) `event` tag.
fn item_name(item: &Value) -> Option<&str> {
    if item["kind"] == "platform" {
        return item["item"]["record"]["kind"].as_str();
    }
    item["item"]["record"]["body"]["event"]
        .as_str()
        .or_else(|| item["item"]["derived"]["event"].as_str())
}

fn assert_event_sequence_contains(events: &[Value], expected: &[&str]) {
    let event_names: Vec<&str> = events.iter().filter_map(item_name).collect();

    let mut cursor = 0;
    for expected_name in expected {
        let Some(found_at) = event_names[cursor..]
            .iter()
            .position(|name| name == expected_name)
        else {
            panic!("missing event {expected_name} in sequence: {event_names:?}");
        };
        cursor += found_at + 1;
    }
}

fn assert_events_belong_to_run(events: &[Value], run_id: &str) {
    assert!(!events.is_empty(), "expected at least one event");
    for event in events {
        assert_eq!(
            event["run_id"].as_str(),
            Some(run_id),
            "event should belong to requested run: {event}"
        );
    }
}

#[test]
fn help() {
    let context = test_context!();
    let mut cmd = context.command();
    cmd.args(["events", "--help"]);
    fabro_snapshot!(context.filters(), cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    View the event log of a workflow run

    Usage: fabro events [OPTIONS] <RUN>

    Arguments:
      <RUN>  Run ID prefix or workflow name (most recent run)

    Options:
          --json              Output as JSON [env: FABRO_JSON=]
          --server <SERVER>   Fabro server target: http(s) URL or absolute Unix socket path [env: FABRO_SERVER=]
          --debug             Enable DEBUG-level logging (default is INFO) [env: FABRO_DEBUG=]
      -f, --follow            Follow event output
          --no-upgrade-check  Disable automatic upgrade check [env: FABRO_NO_UPGRADE_CHECK=true]
          --since <SINCE>     Events since timestamp or relative (e.g. "42m", "2h", "2026-01-02T13:00:00Z")
      -n, --tail <TAIL>       Lines from end (default: all)
          --quiet             Suppress non-essential output [env: FABRO_QUIET=]
      -p, --pretty            Formatted colored output with rendered assistant text
          --verbose           Enable verbose output [env: FABRO_VERBOSE=]
      -h, --help              Print help
    ----- stderr -----
    "#);
}

#[test]
fn events_completed_run_outputs_raw_ndjson() {
    let context = test_context!();
    let run = setup_seeded_completed_dry_run(&context);
    let mut cmd = context.command();
    cmd.args(["events", &run.run_id]);
    let output = cmd.output().expect("command should execute");
    assert!(
        output.status.success(),
        "events should succeed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let events = parse_ndjson(&output.stdout);
    assert_events_belong_to_run(&events, &run.run_id);
    assert_event_sequence_contains(&events, &[
        "run.created",
        "run.lifecycle",
        "run.started",
        "step.started",
        "step.finished",
        "run.finished",
        "run.lifecycle",
    ]);
}

#[test]
fn events_completed_run_reads_store_without_progress_jsonl() {
    let context = test_context!();
    let run = setup_seeded_completed_dry_run(&context);

    let mut filters = json_elapsed_ms_snapshot_filters(context.filters());
    filters.push((
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z".to_string(),
        "[TIMESTAMP]".to_string(),
    ));
    filters.push((
        r#""id":"[0-9a-f-]+""#.to_string(),
        r#""id":"[EVENT_ID]""#.to_string(),
    ));
    filters.push((
        r#""recorded_at":\d{13}"#.to_string(),
        r#""recorded_at":[EPOCH_MS]"#.to_string(),
    ));
    let mut cmd = context.command();
    cmd.args(["events", "--tail", "2", &run.run_id]);

    fabro_snapshot!(filters, cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    {"run_id":"[ULID]","stream_seq":67,"kind":"petri","id":"coordinator/7/0","recorded_at":[EPOCH_MS],"item":{"id":{"log":"coordinator","seq":7,"index":0},"origin":"external","context":{},"recorded_at":[EPOCH_MS],"record":{"seq":7,"origin":"external","recorded_at":[EPOCH_MS],"body":{"event":"run.finished","status":"success"}}}}
    {"run_id":"[ULID]","stream_seq":68,"kind":"platform","id":"[EVENT_ID]","recorded_at":[EPOCH_MS],"item":{"seq":14,"recorded_at":[EPOCH_MS],"record":{"kind":"run.lifecycle","transition":"succeeded","status":{"kind":"succeeded","reason":"completed"}}}}
    ----- stderr -----
    "#);
}

#[test]
fn events_tail_limits_output() {
    let context = test_context!();
    let run = setup_seeded_completed_dry_run(&context);
    let mut filters = json_elapsed_ms_snapshot_filters(context.filters());
    filters.push((
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z".to_string(),
        "[TIMESTAMP]".to_string(),
    ));
    filters.push((
        r#""id":"[0-9a-f-]+""#.to_string(),
        r#""id":"[EVENT_ID]""#.to_string(),
    ));
    filters.push((
        r#""recorded_at":\d{13}"#.to_string(),
        r#""recorded_at":[EPOCH_MS]"#.to_string(),
    ));
    let mut cmd = context.command();
    cmd.args(["events", "--tail", "2", &run.run_id]);

    fabro_snapshot!(filters, cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    {"run_id":"[ULID]","stream_seq":67,"kind":"petri","id":"coordinator/7/0","recorded_at":[EPOCH_MS],"item":{"id":{"log":"coordinator","seq":7,"index":0},"origin":"external","context":{},"recorded_at":[EPOCH_MS],"record":{"seq":7,"origin":"external","recorded_at":[EPOCH_MS],"body":{"event":"run.finished","status":"success"}}}}
    {"run_id":"[ULID]","stream_seq":68,"kind":"platform","id":"[EVENT_ID]","recorded_at":[EPOCH_MS],"item":{"seq":14,"recorded_at":[EPOCH_MS],"record":{"kind":"run.lifecycle","transition":"succeeded","status":{"kind":"succeeded","reason":"completed"}}}}
    ----- stderr -----
    "#);
}

#[test]
fn events_since_filters_output() {
    let context = test_context!();
    let run = setup_seeded_completed_dry_run(&context);
    let mut cmd = context.command();
    cmd.args(["events", "--since", "2999-01-01T00:00:00Z", &run.run_id]);

    fabro_snapshot!(context.filters(), cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    ----- stderr -----
    "#);
}

#[test]
fn events_pretty_formats_small_run() {
    let context = test_context!();
    let run = setup_seeded_completed_dry_run(&context);
    let mut filters = context.filters();
    filters.push((r"\b\d{2}:\d{2}:\d{2}\b".to_string(), "[CLOCK]".to_string()));
    filters.push((
        r"\b\d+(\.\d+)?(ms|s)\b".to_string(),
        "[DURATION]".to_string(),
    ));
    filters.push((
        r"Checkpoint [0-9a-f]{7}\b".to_string(),
        "Checkpoint [SHA]".to_string(),
    ));
    let mut cmd = context.command();
    cmd.args(["events", "--pretty", &run.run_id]);

    fabro_snapshot!(filters, cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    [CLOCK] ▶ Run tests and report results  [ULID]
    [CLOCK]   · submitted
    [CLOCK]   · start_requested
    [CLOCK]   · runnable
    [CLOCK]   · starting
    [CLOCK]   · running
    [CLOCK]   Engine: petri run started
    [CLOCK] ▶ Start
    [CLOCK] ✓ Start  [DURATION]
    [CLOCK]   Branch: fabro/run/[ULID] from [SHA]
    [CLOCK]   Git identity: Fabro <noreply@fabro.sh>  default
    [CLOCK]    ⎘ Checkpoint [SHA]
    [CLOCK] ▶ Run Tests
    [CLOCK]    start → run_tests continue
    [CLOCK] ✓ Run Tests  [DURATION]
    [CLOCK]    ⎘ Checkpoint [SHA]
    [CLOCK] ▶ Report
    [CLOCK]    run_tests → report continue
    [CLOCK] ✓ Report  [DURATION]
    [CLOCK]    ⎘ Checkpoint [SHA]
    [CLOCK] ▶ Exit
    [CLOCK]    report → exit continue
    [CLOCK] ✓ Exit  [DURATION]
    [CLOCK]    ⎘ Checkpoint [SHA]
    [CLOCK]   Diff: +0 -0 in 0 file(s)
    [CLOCK] ✓ SUCCEEDED [DURATION]
    [CLOCK]   · succeeded
    ----- stderr -----
    ");
}

#[test]
#[ignore = "pre-existing flake: events --follow hangs against detached dry-run on this branch and on origin/main; tracked separately"]
fn events_follow_detached_run_streams_until_completion() {
    let context = test_context!();
    let run = setup_detached_dry_run(&context);
    let mut cmd = context.command();
    cmd.args(["events", "--follow", &run.run_id]);
    let output = cmd.output().expect("command should execute");
    assert!(
        output.status.success(),
        "events --follow should succeed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let events = parse_ndjson(&output.stdout);
    assert_events_belong_to_run(&events, &run.run_id);
    assert_event_sequence_contains(&events, &[
        "run.created",
        "run.running",
        "stage.started",
        "stage.completed",
        "run.completed",
        "sandbox.stop.completed",
    ]);
}

/// One stream item as `GET /runs/{id}/events` serves it.
fn stream_item_json(run_id: &str, stream_seq: u64) -> Value {
    json!({
        "run_id": run_id,
        "stream_seq": stream_seq,
        "kind": "petri",
        "id": format!("execution 0/{stream_seq}/0"),
        "recorded_at": 1_789_706_579_000_u64 + stream_seq,
        "item": {
            "id": {"log": "execution", "execution": 0, "seq": stream_seq, "index": 0},
            "origin": "external",
            "context": {},
            "record": {"seq": stream_seq, "body": {"event": "step.progress.recorded"}}
        }
    })
}

/// One page of the listing holding the items `seqs`.
fn stream_page_json(run_id: &str, seqs: impl IntoIterator<Item = u64>, has_more: bool) -> Value {
    json!({
        "data": seqs.into_iter().map(|seq| stream_item_json(run_id, seq)).collect::<Vec<_>>(),
        "meta": {"has_more": has_more},
        "event_contract_version": fabro_petri::petri::EVENT_CONTRACT_VERSION,
    })
}

fn stream_seqs(stdout: &[u8]) -> Vec<u64> {
    parse_ndjson(stdout)
        .iter()
        .map(|item| {
            item["stream_seq"]
                .as_u64()
                .expect("an item has a stream_seq")
        })
        .collect()
}

/// `-n 30` on a twelve-thousand-item stream asks for the stream's newest
/// item, then for the thirty items before it, and never for the stream
/// from its first item.
#[test]
fn events_tail_reads_only_the_pages_holding_the_newest_items() {
    let context = test_context!();
    let server = MockServer::start();
    let run_id = unique_run_id();
    let events_path = format!("/api/v1/runs/{run_id}/events");
    let _resolve = mock_resolved_run(&server, &run_id, &run_id);
    let newest = server.mock(|when, then| {
        when.method("GET")
            .path(events_path.as_str())
            .query_param("tail", "true")
            .query_param("limit", "1");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(stream_page_json(&run_id, [12_000], true));
    });
    let last_thirty = server.mock(|when, then| {
        when.method("GET")
            .path(events_path.as_str())
            .query_param_missing("tail")
            .query_param("after", "11970");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(stream_page_json(&run_id, 11_971..=12_000, false));
    });
    let from_the_start = server.mock(|when, then| {
        when.method("GET")
            .path(events_path.as_str())
            .query_param_missing("tail")
            .query_param("after", "0");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(stream_page_json(&run_id, 1..=1000, true));
    });

    let mut cmd = context.command();
    cmd.args([
        "events",
        "--json",
        "-n",
        "30",
        "--server",
        &server.base_url(),
        &run_id,
    ]);
    let output = cmd.output().expect("command should execute");
    assert!(
        output.status.success(),
        "events -n 30 should succeed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stream_seqs(&output.stdout),
        (11_971..=12_000).collect::<Vec<u64>>()
    );
    assert_eq!(newest.calls(), 1, "the stream's end is read once");
    assert_eq!(
        last_thirty.calls(),
        1,
        "the newest thirty items are one page"
    );
    assert_eq!(
        from_the_start.calls(),
        0,
        "the stream is never read from its first item"
    );
}

/// A read stopped part-way has already printed every page it received: the
/// listing's second page fails, and the first page's items are out.
#[test]
fn events_json_writes_each_page_as_it_arrives() {
    let context = test_context!();
    let server = MockServer::start();
    let run_id = unique_run_id();
    let events_path = format!("/api/v1/runs/{run_id}/events");
    let _resolve = mock_resolved_run(&server, &run_id, &run_id);
    let first = server.mock(|when, then| {
        when.method("GET")
            .path(events_path.as_str())
            .query_param("after", "0");
        then.status(200)
            .header("Content-Type", "application/json")
            .json_body(stream_page_json(&run_id, 1..=3, true));
    });
    let second = server.mock(|when, then| {
        when.method("GET")
            .path(events_path.as_str())
            .query_param("after", "3");
        then.status(500)
            .header("Content-Type", "application/json")
            .json_body(json!({"errors": [{"status": "500", "title": "Internal Server Error", "detail": "the read was cut short"}]}));
    });

    let mut cmd = context.command();
    cmd.args(["events", "--json", "--server", &server.base_url(), &run_id]);
    let output = cmd.output().expect("command should execute");
    assert!(
        !output.status.success(),
        "the failed page fails the read\nstdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        stream_seqs(&output.stdout),
        vec![1, 2, 3],
        "the first page was printed before the second was asked for\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(first.calls(), 1);
    assert!(second.calls() >= 1);
}
