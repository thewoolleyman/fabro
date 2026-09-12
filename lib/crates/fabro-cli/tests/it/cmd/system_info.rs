use fabro_test::{fabro_snapshot, test_context};
use serde_json::Value;

#[test]
fn help() {
    let context = test_context!();
    let mut cmd = context.command();
    cmd.args(["system", "info", "--help"]);
    fabro_snapshot!(context.filters(), cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    Show server runtime information

    Usage: fabro system info [OPTIONS]

    Options:
          --json                       Output as JSON [env: FABRO_JSON=]
          --storage-dir <STORAGE_DIR>  Local storage directory (default: ~/.fabro/storage) [env: FABRO_STORAGE_DIR=]
          --debug                      Enable DEBUG-level logging (default is INFO) [env: FABRO_DEBUG=]
          --server <SERVER>            Fabro server target: http(s) URL or absolute Unix socket path [env: FABRO_SERVER=]
          --no-upgrade-check           Disable automatic upgrade check [env: FABRO_NO_UPGRADE_CHECK=true]
          --quiet                      Suppress non-essential output [env: FABRO_QUIET=]
          --verbose                    Enable verbose output [env: FABRO_VERBOSE=]
      -h, --help                       Print help
    ----- stderr -----
    ");
}

#[test]
fn system_info_json_reports_runtime_fields() {
    let context = test_context!();

    let output = context
        .command()
        .args(["--json", "system", "info"])
        .output()
        .expect("command should run");

    assert!(output.status.success(), "system info failed");
    let value: Value =
        serde_json::from_slice(&output.stdout).expect("system info JSON should parse");
    assert!(value["version"].is_string());
    assert_eq!(
        value["storage_dir"],
        context.storage_dir.display().to_string()
    );
    assert!(value["uptime_secs"].is_number());
    assert!(value["runs"]["total"].is_number());
    assert_eq!(
        value["capabilities"],
        serde_json::json!(["acp.fallback_chain.v1"]),
        "system info must list the ACP fallback chain capability so a client can refuse new grammar against an older server"
    );
}

#[test]
fn system_info_uses_explicit_storage_dir_override() {
    let mut context = test_context!();
    let storage_dir = context.temp_dir.join("alternate-storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    context.write_home(
        ".fabro/settings.toml",
        format!(
            "_version = 1\n\n[server.storage]\nroot = {:?}\n",
            context.storage_dir.display().to_string()
        ),
    );
    context.ensure_home_server_auth_methods();
    context.manage_storage_dir(&storage_dir);

    let output = context
        .command()
        .args([
            "--json",
            "system",
            "info",
            "--storage-dir",
            storage_dir.to_str().unwrap(),
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "system info failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value =
        serde_json::from_slice(&output.stdout).expect("system info JSON should parse");
    assert_eq!(value["storage_dir"], storage_dir.display().to_string());
}
