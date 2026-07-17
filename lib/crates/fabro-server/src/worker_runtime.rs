use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;

use anyhow::{Context, Result};
use async_trait::async_trait;
use fabro_static::EnvVars;
use fabro_types::RunId;
use fabro_types::settings::server::LogDestination;
use futures_util::future::BoxFuture;
use tokio::io::AsyncRead;
use tokio::process::Command;

use crate::spawn_env::{apply_worker_env, apply_worker_otel_export_env};

#[async_trait]
pub(crate) trait WorkerRuntime: Send + Sync {
    async fn start(&self, spec: WorkerLaunchSpec) -> Result<StartedWorker>;
    async fn request_stop(&self, worker_ref: &WorkerRef);
    async fn force_stop(&self, worker_ref: &WorkerRef);
    async fn is_alive(&self, worker_ref: &WorkerRef) -> bool;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkerRef {
    /// A worker running as a local subprocess. `pre_exec_setpgid` ensures the
    /// child is the leader of its own process group with `pgid == pid`, so a
    /// single PID identifies both the process and its group.
    Local { pid: u32 },
}

pub(crate) struct WorkerLaunchSpec {
    pub(crate) executable:             PathBuf,
    pub(crate) server_target:          String,
    pub(crate) storage_dir:            PathBuf,
    pub(crate) run_dir:                PathBuf,
    pub(crate) run_id:                 RunId,
    pub(crate) mode:                   &'static str,
    pub(crate) worker_token:           String,
    pub(crate) log_destination:        LogDestination,
    pub(crate) fabro_log:              Option<String>,
    pub(crate) active_config_path:     PathBuf,
    pub(crate) github_app_private_key: Option<String>,
    /// W3C `traceparent` of the server-side `run` span, so the worker can
    /// parent its own `run` span on it and both land in ONE trace. `None` when
    /// OTLP export is off (nothing to propagate).
    ///
    /// Carried on the spec rather than captured at launch time: that keeps
    /// building the command a pure function of the spec (hence testable without
    /// a live span), and keeps it correct if `start` is ever moved onto its own
    /// task, where the run span would no longer be current. It IS current at
    /// today's call site — this is deliberate insulation, not a workaround.
    pub(crate) traceparent:            Option<String>,
}

pub(crate) struct StartedWorker {
    pub(crate) worker_ref: WorkerRef,
    pub(crate) stderr:     Pin<Box<dyn AsyncRead + Send + 'static>>,
    pub(crate) wait:       BoxFuture<'static, Result<WorkerExit>>,
}

#[derive(Debug)]
pub(crate) struct WorkerExit {
    pub(crate) success: bool,
    pub(crate) detail:  String,
}

#[derive(Default)]
pub(crate) struct LocalWorkerRuntime;

impl LocalWorkerRuntime {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn command_for_spec(spec: &WorkerLaunchSpec) -> Command {
        let worker_stdout = match spec.log_destination {
            LogDestination::Stdout => Stdio::inherit(),
            LogDestination::File => Stdio::null(),
        };
        let log_destination_env: &'static str = spec.log_destination.into();

        let mut cmd = Command::new(&spec.executable);
        cmd.arg("__run-worker")
            .arg("--server")
            .arg(&spec.server_target)
            .arg("--storage-dir")
            .arg(&spec.storage_dir)
            .arg("--run-dir")
            .arg(&spec.run_dir)
            .arg("--run-id")
            .arg(spec.run_id.to_string())
            .arg("--mode")
            .arg(spec.mode)
            .stdin(Stdio::null())
            .stdout(worker_stdout)
            .stderr(Stdio::piped());

        apply_worker_env(&mut cmd);
        if let Some(level) = spec.fabro_log.as_deref() {
            cmd.env(EnvVars::FABRO_LOG, level);
        }
        cmd.env(EnvVars::FABRO_LOG_DESTINATION, log_destination_env);
        cmd.env(EnvVars::FABRO_CONFIG, &spec.active_config_path);
        cmd.env_remove(EnvVars::FABRO_WORKER_TOKEN);
        cmd.env(EnvVars::FABRO_WORKER_TOKEN, &spec.worker_token);
        if let Some(pem) = spec.github_app_private_key.as_deref() {
            cmd.env(EnvVars::GITHUB_APP_PRIVATE_KEY, pem);
        }
        // Forward the server's OTLP export config so the worker's `otel_layer`
        // exports its spans to the same collector. The collector's egress
        // credential (OTEL_EXPORTER_OTLP_HEADERS) is deliberately NOT forwarded.
        // MUST stay after `apply_worker_env`: that call `env_clear`s the command,
        // so re-injecting earlier would be wiped and telemetry would silently
        // vanish (a fail-safe direction — loss, never a leak — but still a bug).
        apply_worker_otel_export_env(&mut cmd);
        // Parent the worker's `run` span on the server's, joining what would
        // otherwise be two disconnected traces per run. Per-run data, so unlike
        // the export config above it is threaded through the spec instead of
        // copied from the server's environment. Absent when export is off,
        // leaving the worker span a root exactly as before. MUST also stay
        // after `apply_worker_env` (see above): `env_clear` would wipe it.
        if let Some(traceparent) = spec.traceparent.as_deref() {
            cmd.env(EnvVars::TRACEPARENT, traceparent);
        }

        #[cfg(unix)]
        fabro_proc::pre_exec_setpgid(cmd.as_std_mut());

        cmd
    }
}

#[async_trait]
impl WorkerRuntime for LocalWorkerRuntime {
    async fn start(&self, spec: WorkerLaunchSpec) -> Result<StartedWorker> {
        let mut child = Self::command_for_spec(&spec)
            .spawn()
            .context("spawning run worker process")?;

        let Some(pid) = child.id() else {
            let _ = child.start_kill();
            anyhow::bail!("worker process did not report a PID");
        };
        let Some(stderr) = child.stderr.take() else {
            let _ = child.start_kill();
            anyhow::bail!("worker child stderr should be piped");
        };
        let stderr: Pin<Box<dyn AsyncRead + Send + 'static>> = Box::pin(stderr);
        let wait: BoxFuture<'static, Result<WorkerExit>> = Box::pin(async move {
            let status = child.wait().await.context("worker wait failed")?;
            Ok(WorkerExit {
                success: status.success(),
                detail:  status.to_string(),
            })
        });

        Ok(StartedWorker {
            worker_ref: WorkerRef::Local { pid },
            stderr,
            wait,
        })
    }

    async fn request_stop(&self, worker_ref: &WorkerRef) {
        let WorkerRef::Local { pid } = worker_ref;
        #[cfg(unix)]
        fabro_proc::sigterm_process_group(*pid);
        #[cfg(not(unix))]
        let _ = pid;
    }

    async fn force_stop(&self, worker_ref: &WorkerRef) {
        let WorkerRef::Local { pid } = worker_ref;
        #[cfg(unix)]
        fabro_proc::sigkill_process_group(*pid);
        #[cfg(not(unix))]
        let _ = pid;
    }

    async fn is_alive(&self, worker_ref: &WorkerRef) -> bool {
        let WorkerRef::Local { pid } = worker_ref;
        #[cfg(unix)]
        {
            fabro_proc::process_group_alive(*pid)
        }
        #[cfg(not(unix))]
        {
            fabro_proc::process_running(*pid)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    fn spec_with_traceparent(traceparent: Option<&str>) -> WorkerLaunchSpec {
        WorkerLaunchSpec {
            executable:             PathBuf::from("/usr/bin/true"),
            server_target:          "127.0.0.1:0".to_string(),
            storage_dir:            PathBuf::from("/tmp/fabro-storage"),
            run_dir:                PathBuf::from("/tmp/fabro-run"),
            run_id:                 RunId::new(),
            mode:                   "start",
            worker_token:           "token".to_string(),
            log_destination:        LogDestination::File,
            fabro_log:              None,
            active_config_path:     PathBuf::from("/tmp/fabro.toml"),
            github_app_private_key: None,
            traceparent:            traceparent.map(str::to_string),
        }
    }

    fn env_value(cmd: &Command, key: &str) -> Option<String> {
        cmd.as_std()
            .get_envs()
            .find(|(name, _)| *name == OsStr::new(key))
            .and_then(|(_, value)| value)
            .map(|value| value.to_string_lossy().into_owned())
    }

    #[test]
    fn worker_command_carries_the_captured_traceparent() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let cmd = LocalWorkerRuntime::command_for_spec(&spec_with_traceparent(Some(traceparent)));

        assert_eq!(env_value(&cmd, "TRACEPARENT").as_deref(), Some(traceparent));
    }

    // Export off (nothing captured) must leave the worker exactly as it was
    // before O2: no TRACEPARENT, so its run span stays a root.
    #[test]
    fn worker_command_omits_traceparent_when_none_was_captured() {
        let cmd = LocalWorkerRuntime::command_for_spec(&spec_with_traceparent(None));

        assert_eq!(env_value(&cmd, "TRACEPARENT"), None);
    }
}
