use anyhow::Result;
use fabro_workflow::run_status::RunStatus;
use serde::Serialize;

use crate::args::InspectArgs;
use crate::command_context::CommandContext;
use crate::server_client::RunProjection;
use crate::server_runs::ServerRunInfo;

#[derive(Debug, Serialize)]
pub(crate) struct InspectOutput {
    pub run_id:       String,
    pub parent_id:    Option<String>,
    pub status:       RunStatus,
    pub run_spec:     Option<serde_json::Value>,
    pub start_record: Option<serde_json::Value>,
    pub conclusion:   Option<serde_json::Value>,
    pub checkpoint:   Option<serde_json::Value>,
    pub sandbox:      Option<serde_json::Value>,
}

pub(crate) async fn run(args: &InspectArgs, base_ctx: &CommandContext) -> Result<()> {
    let ctx = base_ctx.with_target(&args.server)?;
    let printer = ctx.printer();
    let client = ctx.server().await?;
    let run = ServerRunInfo::from_run(client.resolve_run(&args.run).await?);
    let run_id = run.run_id();
    let state = client.get_run_state(&run_id).await?;
    let mut output = inspect_run_state(&run, state);
    redact_run_environment(&mut output.run_spec);
    let output = fabro_redact::redact_json_value(serde_json::to_value([output])?);
    let json = serde_json::to_string_pretty(&output)?;
    fabro_util::printout!(printer, "{json}");
    Ok(())
}

fn redact_run_environment(run_spec: &mut Option<serde_json::Value>) {
    let Some(env) = run_spec
        .as_mut()
        .and_then(|spec| spec.pointer_mut("/settings/run/environment/env"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    for value in env.values_mut() {
        *value = serde_json::Value::String("REDACTED".to_string());
    }
}

fn inspect_run_state(run: &ServerRunInfo, state: RunProjection) -> InspectOutput {
    let checkpoint = state
        .current_checkpoint()
        .and_then(|record| serde_json::to_value(record).ok());
    InspectOutput {
        run_id: run.run_id().to_string(),
        parent_id: state.parent_id.map(|parent_id| parent_id.to_string()),
        status: state.status,
        run_spec: serde_json::to_value(state.spec).ok(),
        start_record: state
            .start
            .and_then(|record| serde_json::to_value(record).ok()),
        conclusion: state
            .conclusion
            .and_then(|record| serde_json::to_value(record).ok()),
        checkpoint,
        sandbox: state
            .sandbox
            .and_then(|record| serde_json::to_value(record).ok()),
    }
}
