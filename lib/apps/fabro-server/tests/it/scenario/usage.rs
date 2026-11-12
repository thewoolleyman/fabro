use axum::body::Body;
use axum::http::{Request, StatusCode};
use tokio::time::sleep;
use tower::ServiceExt;

use crate::helpers::{
    MINIMAL_DOT, POLL_INTERVAL, api, create_and_start_run_from_intent, minimal_intent_json,
    minimal_intent_json_with_dry_run, test_app_state_with_options, test_app_with_scheduler,
    test_settings, wait_for_run_status,
};

const COMMAND_DOT: &str = r#"digraph Test {
    graph [goal="Test"]
    start [shape=Mdiamond]
    echo_task [shape=parallelogram, script="echo command-stage"]
    exit  [shape=Msquare]
    start -> echo_task -> exit
}"#;

const WAIT_DOT: &str = r#"digraph Test {
    graph [goal="Test"]
    start [shape=Mdiamond]
    wait_task [shape=insulator, duration="1ms"]
    exit  [shape=Msquare]
    start -> wait_task -> exit
}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_usage_increments_after_run_completes() {
    let workspace = tempfile::tempdir().unwrap();
    let state = test_app_state_with_options(test_settings(), 5);
    let app = test_app_with_scheduler(state);

    let run_id = create_and_start_run_from_intent(
        &app,
        minimal_intent_json_with_dry_run(&app, MINIMAL_DOT, workspace.path()).await,
    )
    .await;

    // Poll until run completes
    let status = wait_for_run_status(&app, &run_id, &["succeeded", "failed"]).await;
    assert_eq!(status, "succeeded");

    let total_runs = loop {
        let req = Request::builder()
            .method("GET")
            .uri(api("/usage"))
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        let body = crate::helpers::response_json(
            response,
            StatusCode::OK,
            format!("{}:{}", file!(), line!()),
        )
        .await;
        let runs = body["totals"]["runs"].as_i64().unwrap();
        if runs == 1 {
            break runs;
        }
        sleep(POLL_INTERVAL).await;
    };
    assert_eq!(total_runs, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_usage_includes_completed_non_llm_stages() {
    let workspace = tempfile::tempdir().unwrap();
    let state = test_app_state_with_options(test_settings(), 5);
    let app = test_app_with_scheduler(state);

    let run_id = create_and_start_run_from_intent(
        &app,
        minimal_intent_json(&app, WAIT_DOT, workspace.path()).await,
    )
    .await;

    let status = wait_for_run_status(&app, &run_id, &["succeeded", "failed"]).await;
    assert_eq!(status, "succeeded");

    let usage = run_usage(&app, &run_id).await;
    assert_non_llm_usage(&usage, &["wait_task"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_usage_includes_completed_command_stages() {
    let workspace = tempfile::tempdir().unwrap();
    let state = test_app_state_with_options(test_settings(), 5);
    let app = test_app_with_scheduler(state);

    let run_id = create_and_start_run_from_intent(
        &app,
        minimal_intent_json(&app, COMMAND_DOT, workspace.path()).await,
    )
    .await;

    let status = wait_for_run_status(&app, &run_id, &["succeeded", "failed"]).await;
    assert_eq!(status, "succeeded");

    let usage = run_usage(&app, &run_id).await;
    assert_non_llm_usage(&usage, &["echo_task"]);
}

async fn run_usage(app: &axum::Router, run_id: &str) -> serde_json::Value {
    let req = Request::builder()
        .method("GET")
        .uri(api(&format!("/runs/{run_id}/usage")))
        .body(Body::empty())
        .expect("run usage request should build");

    let response = app.clone().oneshot(req).await.unwrap();
    crate::helpers::response_json(
        response,
        StatusCode::OK,
        format!("GET /api/v1/runs/{run_id}/usage"),
    )
    .await
}

fn assert_non_llm_usage(usage: &serde_json::Value, expected_stage_ids: &[&str]) {
    let stages = usage["stages"]
        .as_array()
        .expect("usage response should include stages");
    let mut stage_ids = stages
        .iter()
        .map(|stage| {
            stage["stage"]["id"]
                .as_str()
                .expect("stage should include an id")
                .to_string()
        })
        .collect::<Vec<_>>();
    stage_ids.sort();
    assert_eq!(stage_ids, expected_stage_ids);

    assert!(
        stages.iter().all(|stage| {
            stage["model"].is_null()
                && stage["usage"]["tokens"]["input"] == 0
                && stage["usage"]["tokens"]["output"] == 0
                && stage["usage"]["tokens"]["reasoning"] == 0
                && stage["usage"].get("cost").is_none()
        }),
        "every non-LLM stage should have null model and zero token counts: {stages:?}"
    );

    let stage_wall_sum: u64 = stages
        .iter()
        .map(|stage| {
            stage["timing"]["wall_time_ms"]
                .as_u64()
                .expect("stage should include timing.wall_time_ms")
        })
        .sum();

    assert_eq!(
        usage["by_model"]
            .as_array()
            .expect("usage response should include by_model")
            .len(),
        0
    );
    assert_eq!(usage["totals"]["usage"]["tokens"]["input"], 0);
    assert_eq!(usage["totals"]["usage"]["tokens"]["output"], 0);
    assert!(usage["totals"]["usage"].get("cost").is_none());

    let total_wall_time_ms = usage["totals"]["timing"]["wall_time_ms"]
        .as_u64()
        .expect("totals should include timing.wall_time_ms");
    assert_eq!(
        total_wall_time_ms, stage_wall_sum,
        "total wall_time_ms {total_wall_time_ms} should equal summed stage wall_time_ms {stage_wall_sum}"
    );
}
