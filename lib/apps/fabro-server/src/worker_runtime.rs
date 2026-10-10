use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;

use anyhow::{Context, Result};
use async_trait::async_trait;
use fabro_static::EnvVars;
use fabro_types::RunId;
use fabro_types::settings::server::LogDestination;
use fabro_util::exit::ExitClass;
use futures_util::future::BoxFuture;
use tokio::io::AsyncRead;
use tokio::process::Command;

use crate::spawn_env::apply_worker_env;

#[async_trait]
pub(crate) trait WorkerRuntime: Send + Sync {
    async fn start(&self, spec: WorkerLaunchSpec) -> Result<StartedWorker>;
    async fn request_stop(&self, worker_ref: &WorkerRef);
    async fn force_stop(&self, worker_ref: &WorkerRef);
    async fn is_alive(&self, worker_ref: &WorkerRef) -> bool;
}

#[derive(Clone, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum WorkerRef {
    /// A worker running as a local subprocess. `pre_exec_setpgid` ensures the
    /// child is the leader of its own process group with `pgid == pid`, so a
    /// single PID identifies both the process and its group.
    Local { pid: u32 },
}

impl WorkerRef {
    pub(crate) fn kind(&self) -> &'static str {
        self.into()
    }
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
    /// The Fabro home the server resolved, so a Petri run's skills step
    /// reads the same home whatever the worker's environment says.
    pub(crate) fabro_home:             PathBuf,
    /// W3C `traceparent` of the server-side `run` span, so the worker parents
    /// its own `run` span on it and both land in ONE trace. `None` when OTLP
    /// export is off (nothing to propagate). Carried on the spec, not captured
    /// at launch, so building the command stays a pure function of the spec.
    pub(crate) traceparent:            Option<String>,
}

pub(crate) struct StartedWorker {
    pub(crate) worker_ref: WorkerRef,
    pub(crate) stderr:     Pin<Box<dyn AsyncRead + Send + 'static>>,
    pub(crate) wait:       BoxFuture<'static, Result<WorkerExit>>,
}

#[derive(Debug)]
pub(crate) struct WorkerExit {
    /// The worker's exit code; `None` when a signal ended it.
    pub(crate) code:   Option<i32>,
    pub(crate) detail: String,
}

impl WorkerExit {
    /// Whether the worker exited with status zero.
    pub(crate) fn succeeded(&self) -> bool {
        self.code == Some(0)
    }

    /// Whether the worker ended the run's lifetime and not the run: its
    /// store failed, and the run resumes ([`ExitClass::Interrupted`]).
    pub(crate) fn interrupted(&self) -> bool {
        self.code == Some(ExitClass::Interrupted.code())
    }
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
            .arg("--fabro-home")
            .arg(&spec.fabro_home)
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
        // Parent the worker's `run` span on the server's. Per-run data, so it
        // rides the spec rather than the server's environment. Absent when
        // export is off, leaving the worker span a root. MUST stay after
        // `apply_worker_env`, whose `env_clear` would wipe it.
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
                code:   status.code(),
                detail: status.to_string(),
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
            fabro_home:             PathBuf::from("/tmp/fabro-home"),
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

    // Export off (nothing captured) leaves the worker as it was: no
    // TRACEPARENT, so its run span stays a root.
    #[test]
    fn worker_command_omits_traceparent_when_none_was_captured() {
        let cmd = LocalWorkerRuntime::command_for_spec(&spec_with_traceparent(None));

        assert_eq!(env_value(&cmd, "TRACEPARENT"), None);
    }
}
