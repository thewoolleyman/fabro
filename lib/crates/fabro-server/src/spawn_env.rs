use std::ffi::OsString;

use fabro_static::EnvVars;
use tokio::process::Command;

const WORKER_ENV_ALLOWLIST: &[&str] = &[
    EnvVars::PATH,
    EnvVars::HOME,
    EnvVars::TMPDIR,
    EnvVars::USER,
    EnvVars::RUST_LOG,
    EnvVars::RUST_BACKTRACE,
    EnvVars::FABRO_LOG,
    EnvVars::FABRO_HOME,
    EnvVars::FABRO_STORAGE_ROOT,
    // Push-credential refresh-ahead tunables (FABRO_PUSH_CRED_REFRESH_*).
    // `run_turn` executes in the worker, so these must survive `env_clear()` to
    // reach the refresh-ahead loop in the ACP handler.
    EnvVars::FABRO_PUSH_CRED_REFRESH_AHEAD,
    EnvVars::FABRO_PUSH_CRED_REFRESH_INTERVAL_SECONDS,
    EnvVars::TERM,
    EnvVars::NO_COLOR,
    EnvVars::CLICOLOR,
    EnvVars::CLICOLOR_FORCE,
];

const RENDER_GRAPH_ENV_ALLOWLIST: &[&str] = &[EnvVars::PATH, EnvVars::HOME, EnvVars::TMPDIR];

// OTLP export config forwarded from the server's environment into the worker so
// the worker's `otel_layer` (fabro-cli) exports its spans to the SAME collector
// the server targets. This is a NON-SECRET allowlist, and it is the allowlist —
// not the denylist below — that provides the fail-closed guarantee: the worker
// env is `env_clear`ed and only these names are copied back, so no credential
// var can reach the worker regardless of what the server env holds. The
// topology this assumes is a LOCAL, no-auth collector (the collector adds
// egress auth); pointing the server's endpoint straight at an authenticated
// backend is unsupported — the worker would inherit the endpoint without the
// auth header. `*_TIMEOUT` / `*_TRACES_TIMEOUT` are non-secret export knobs the
// opentelemetry-otlp SDK reads itself; they have no `fabro_static::EnvVars`
// constant, so they are listed by literal name.
const WORKER_OTEL_EXPORT_ALLOWLIST: &[&str] = &[
    EnvVars::OTEL_EXPORTER_OTLP_ENDPOINT,
    EnvVars::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    EnvVars::OTEL_EXPORTER_OTLP_PROTOCOL,
    EnvVars::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL,
    EnvVars::OTEL_SERVICE_NAME,
    "OTEL_EXPORTER_OTLP_TIMEOUT",
    "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
];

// Belt-and-suspenders only. The OTLP headers vars carry the collector's egress
// credential (e.g. a Honeycomb API key); they are `env_remove`d from the worker
// command. In the current call graph this is redundant with the env_clear +
// allowlist-only forwarding above (nothing sets them after the clear) — it
// exists so an accidental future `cmd.env(HEADERS, ...)` added AFTER the
// forwarding still cannot survive into the worker. Not `fabro_static::EnvVars`
// constants — the `opentelemetry-otlp` SDK reads these literal names itself;
// strip both the base and per-signal spellings.
const WORKER_OTEL_SECRET_DENYLIST: &[&str] = &[
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
];

pub(crate) fn apply_worker_env(cmd: &mut Command) {
    apply_allowlist(cmd, WORKER_ENV_ALLOWLIST, &process_env_var_os);
}

/// Re-inject the server's OTLP export config into an already-`env_clear`ed
/// worker command so the worker exports its spans to the same collector. Call
/// AFTER [`apply_worker_env`] — these are additive re-injections, not survivors
/// of the `env_clear`. The collector's egress credential is never forwarded.
pub(crate) fn apply_worker_otel_export_env(cmd: &mut Command) {
    apply_otel_export(cmd, &process_env_var_os);
}

pub(crate) fn apply_render_graph_env(cmd: &mut Command) {
    apply_allowlist(cmd, RENDER_GRAPH_ENV_ALLOWLIST, &process_env_var_os);
}

#[expect(
    clippy::disallowed_methods,
    reason = "Subprocess env allowlists intentionally copy a narrow process-env subset."
)]
fn process_env_var_os(name: &str) -> Option<OsString> {
    std::env::var_os(name)
}

fn apply_allowlist(cmd: &mut Command, keys: &[&str], lookup: &dyn Fn(&str) -> Option<OsString>) {
    cmd.env_clear();
    for key in keys {
        if let Some(value) = lookup(key) {
            cmd.env(key, value);
        }
    }
}

/// Additive OTLP-export re-injection: forward the non-secret export vars and
/// strip the credential-bearing headers vars. No `env_clear` — the caller has
/// already cleared + allowlisted the worker env.
fn apply_otel_export(cmd: &mut Command, lookup: &dyn Fn(&str) -> Option<OsString>) {
    for key in WORKER_OTEL_EXPORT_ALLOWLIST {
        if let Some(value) = lookup(key) {
            cmd.env(key, value);
        }
    }
    for key in WORKER_OTEL_SECRET_DENYLIST {
        cmd.env_remove(key);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::path::Path;

    use super::{
        RENDER_GRAPH_ENV_ALLOWLIST, WORKER_ENV_ALLOWLIST, apply_allowlist, apply_otel_export,
    };

    fn env_command() -> tokio::process::Command {
        assert!(Path::new("/usr/bin/env").exists());
        tokio::process::Command::new("/usr/bin/env")
    }

    async fn env_output(mut cmd: tokio::process::Command) -> HashMap<String, String> {
        let output = cmd.output().await.expect("running env subprocess");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("parsing env subprocess output as UTF-8")
            .lines()
            .filter_map(|line| {
                let (key, value) = line.split_once('=')?;
                Some((key.to_string(), value.to_string()))
            })
            .collect()
    }

    #[tokio::test]
    async fn worker_allowlist_is_fail_closed() {
        let env = HashMap::from([
            ("PATH".to_string(), "/bin".to_string()),
            ("HOME".to_string(), "/tmp/home".to_string()),
            ("TMPDIR".to_string(), "/tmp".to_string()),
            ("USER".to_string(), "alice".to_string()),
            ("RUST_LOG".to_string(), "debug".to_string()),
            ("FABRO_LOG".to_string(), "debug".to_string()),
            ("FABRO_LOG_DESTINATION".to_string(), "stdout".to_string()),
            ("FABRO_HOME".to_string(), "/tmp/fabro-home".to_string()),
            (
                "FABRO_STORAGE_ROOT".to_string(),
                "/tmp/fabro-storage".to_string(),
            ),
            ("FABRO_PUSH_CRED_REFRESH_AHEAD".to_string(), "0".to_string()),
            (
                "FABRO_PUSH_CRED_REFRESH_INTERVAL_SECONDS".to_string(),
                "1800".to_string(),
            ),
            ("TERM".to_string(), "xterm-256color".to_string()),
            ("NO_COLOR".to_string(), "1".to_string()),
            ("CLICOLOR".to_string(), "0".to_string()),
            ("CLICOLOR_FORCE".to_string(), "1".to_string()),
            ("SESSION_SECRET".to_string(), "leak".to_string()),
            ("FABRO_JWT_PRIVATE_KEY".to_string(), "leak".to_string()),
            ("FABRO_JWT_PUBLIC_KEY".to_string(), "leak".to_string()),
            ("GITHUB_APP_PRIVATE_KEY".to_string(), "leak".to_string()),
            ("GITHUB_APP_CLIENT_SECRET".to_string(), "leak".to_string()),
            ("GITHUB_APP_WEBHOOK_SECRET".to_string(), "leak".to_string()),
            ("FABRO_DEV_TOKEN".to_string(), "garbage".to_string()),
            ("FABRO_WORKER_TOKEN".to_string(), "leak".to_string()),
            ("MY_API_KEY".to_string(), "blocked".to_string()),
        ]);
        let mut cmd = env_command();
        apply_allowlist(&mut cmd, WORKER_ENV_ALLOWLIST, &|name| {
            env.get(name).map(OsString::from)
        });
        cmd.env(
            "FABRO_DEV_TOKEN",
            "fabro_dev_abababababababababababababababababababababababababababababababab",
        );

        let actual = env_output(cmd).await;

        assert_eq!(actual.get("PATH").map(String::as_str), Some("/bin"));
        assert_eq!(actual.get("HOME").map(String::as_str), Some("/tmp/home"));
        assert_eq!(actual.get("FABRO_LOG").map(String::as_str), Some("debug"));
        // Push-credential refresh-ahead tunables must survive env_clear() into
        // the worker so run_turn's refresh-ahead loop can read them.
        assert_eq!(
            actual
                .get("FABRO_PUSH_CRED_REFRESH_AHEAD")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            actual
                .get("FABRO_PUSH_CRED_REFRESH_INTERVAL_SECONDS")
                .map(String::as_str),
            Some("1800")
        );
        assert_eq!(
            actual.get("TERM").map(String::as_str),
            Some("xterm-256color")
        );
        assert_eq!(actual.get("NO_COLOR").map(String::as_str), Some("1"));
        assert_eq!(actual.get("CLICOLOR").map(String::as_str), Some("0"));
        assert_eq!(actual.get("CLICOLOR_FORCE").map(String::as_str), Some("1"));
        assert!(!actual.contains_key("FABRO_LOG_DESTINATION"));
        assert_eq!(
            actual.get("FABRO_DEV_TOKEN").map(String::as_str),
            Some("fabro_dev_abababababababababababababababababababababababababababababababab")
        );
        assert!(!actual.contains_key("SESSION_SECRET"));
        assert!(!actual.contains_key("FABRO_JWT_PRIVATE_KEY"));
        assert!(!actual.contains_key("FABRO_JWT_PUBLIC_KEY"));
        assert!(!actual.contains_key("GITHUB_APP_PRIVATE_KEY"));
        assert!(!actual.contains_key("GITHUB_APP_CLIENT_SECRET"));
        assert!(!actual.contains_key("GITHUB_APP_WEBHOOK_SECRET"));
        assert!(!actual.contains_key("FABRO_WORKER_TOKEN"));
        assert!(!actual.contains_key("MY_API_KEY"));
    }

    #[tokio::test]
    async fn render_graph_allowlist_is_fail_closed() {
        let env = HashMap::from([
            ("PATH".to_string(), "/bin".to_string()),
            ("HOME".to_string(), "/tmp/home".to_string()),
            ("TMPDIR".to_string(), "/tmp".to_string()),
            ("FABRO_TELEMETRY".to_string(), "on".to_string()),
            ("SESSION_SECRET".to_string(), "leak".to_string()),
        ]);
        let mut cmd = env_command();
        apply_allowlist(&mut cmd, RENDER_GRAPH_ENV_ALLOWLIST, &|name| {
            env.get(name).map(OsString::from)
        });
        cmd.env("FABRO_TELEMETRY", "off");

        let actual = env_output(cmd).await;

        assert_eq!(actual.get("PATH").map(String::as_str), Some("/bin"));
        assert_eq!(
            actual.get("FABRO_TELEMETRY").map(String::as_str),
            Some("off")
        );
        assert!(!actual.contains_key("SESSION_SECRET"));
    }

    #[tokio::test]
    async fn worker_otel_export_forwards_config_but_never_headers() {
        // The server's OTLP export config, plus the credential-bearing headers
        // vars the server env may carry for its own outbound export.
        let env = HashMap::from([
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
                "http://collector:4318".to_string(),
            ),
            (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT".to_string(),
                "http://collector:4318/v1/traces".to_string(),
            ),
            (
                "OTEL_EXPORTER_OTLP_PROTOCOL".to_string(),
                "http/json".to_string(),
            ),
            (
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL".to_string(),
                "http/json".to_string(),
            ),
            ("OTEL_SERVICE_NAME".to_string(), "fabro".to_string()),
            ("OTEL_EXPORTER_OTLP_TIMEOUT".to_string(), "5000".to_string()),
            (
                "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT".to_string(),
                "5000".to_string(),
            ),
            (
                "OTEL_EXPORTER_OTLP_HEADERS".to_string(),
                "x-honeycomb-team=secret".to_string(),
            ),
            (
                "OTEL_EXPORTER_OTLP_TRACES_HEADERS".to_string(),
                "x-honeycomb-team=secret".to_string(),
            ),
        ]);
        let mut cmd = env_command();
        cmd.env_clear();
        // Pre-set BOTH headers vars on the worker env so each assertion proves the
        // denylist STRIPS a pre-existing value, not merely that it is never
        // forwarded — making both denylist entries load-bearing.
        cmd.env("OTEL_EXPORTER_OTLP_HEADERS", "x-honeycomb-team=leak");
        cmd.env("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-honeycomb-team=leak");
        apply_otel_export(&mut cmd, &|name| env.get(name).map(OsString::from));

        let actual = env_output(cmd).await;

        assert_eq!(
            actual
                .get("OTEL_EXPORTER_OTLP_ENDPOINT")
                .map(String::as_str),
            Some("http://collector:4318")
        );
        assert_eq!(
            actual
                .get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                .map(String::as_str),
            Some("http://collector:4318/v1/traces")
        );
        assert_eq!(
            actual
                .get("OTEL_EXPORTER_OTLP_PROTOCOL")
                .map(String::as_str),
            Some("http/json")
        );
        assert_eq!(
            actual
                .get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
                .map(String::as_str),
            Some("http/json")
        );
        assert_eq!(
            actual.get("OTEL_SERVICE_NAME").map(String::as_str),
            Some("fabro")
        );
        assert_eq!(
            actual.get("OTEL_EXPORTER_OTLP_TIMEOUT").map(String::as_str),
            Some("5000")
        );
        assert_eq!(
            actual
                .get("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT")
                .map(String::as_str),
            Some("5000")
        );
        // The credential-bearing headers vars must never reach the worker —
        // neither forwarded from the lookup nor surviving a pre-existing value
        // (both are pre-set above, so both denylist entries are exercised).
        assert!(!actual.contains_key("OTEL_EXPORTER_OTLP_HEADERS"));
        assert!(!actual.contains_key("OTEL_EXPORTER_OTLP_TRACES_HEADERS"));
    }
}
