use agent_client_protocol::schema::{
    ContentBlock, ContentChunk, SessionNotification, SessionUpdate,
};
use serde_json::json;

pub const SESSION_ID: &str = "sess-1";

pub fn agent_message_chunk(session_id: &str, text: &str) -> SessionNotification {
    SessionNotification::new(
        session_id.to_string(),
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(text.to_string()))),
    )
}

pub fn agent_message_chunk_json(session_id: &str, text: &str) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": agent_message_chunk(session_id, text),
    })
}

pub fn fake_acp_agent_script() -> &'static str {
    r#"
import json
import os
import signal
import sys
import time

methods = []
session_id = "sess-1"
prompt_count = 0
# In-protocol config options the agent advertises on session/new and
# accepts through session/set_config_option: a JSON list of
# {"id", "current", "values": [...]} from ACP_CONFIG_OPTIONS.
config_options = json.loads(os.environ.get("ACP_CONFIG_OPTIONS", "[]"))

def render_config_options():
    rendered = []
    for option in config_options:
        rendered.append({
            "id": option["id"],
            "name": option.get("name", option["id"]),
            "category": option.get("category", "model" if option["id"] == "model" else "other"),
            "type": "select",
            "currentValue": option["current"],
            "options": [{"value": value, "name": value} for value in option["values"]],
        })
    return rendered

if os.environ.get("ACP_PID_RECORD"):
    with open(os.environ["ACP_PID_RECORD"], "w", encoding="utf-8") as record:
        record.write(str(os.getpid()))

if os.environ.get("ACP_ENV_RECORD"):
    keys = [
        key.strip()
        for key in os.environ.get(
            "ACP_ENV_RECORD_KEYS",
            "ANTHROPIC_API_KEY,OPENAI_API_KEY,GEMINI_API_KEY",
        ).split(",")
        if key.strip()
    ]
    snapshot = {key: os.environ[key] for key in keys if key in os.environ}
    with open(os.environ["ACP_ENV_RECORD"], "w", encoding="utf-8") as record:
        record.write(json.dumps(snapshot, sort_keys=True))

def handle_sigterm(signum, frame):
    if os.environ.get("ACP_LINGER_TERMINATED"):
        with open(os.environ["ACP_LINGER_TERMINATED"], "w", encoding="utf-8") as record:
            record.write("terminated\n")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

def send(message):
    print(json.dumps(message), flush=True)

def respond(message, result):
    send({"jsonrpc": "2.0", "id": message["id"], "result": result})

def record_methods():
    if os.environ.get("ACP_RECORD"):
        with open(os.environ["ACP_RECORD"], "w", encoding="utf-8") as record:
            record.write("\n".join(methods) + "\n")

def first_prompt_text(message):
    prompt = message.get("params", {}).get("prompt", [])
    if not prompt:
        return ""
    first = prompt[0]
    if isinstance(first, str):
        return first
    return first.get("text", "")

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    methods.append(method)

    if method == "initialize":
        if os.environ.get("ACP_MODE") == "slow_initialize":
            time.sleep(60)
        respond(message, {"protocolVersion": 1, "agentCapabilities": {}})
    elif method == "session/new":
        if os.environ.get("ACP_SESSION_NEW_PARAMS"):
            with open(os.environ["ACP_SESSION_NEW_PARAMS"], "w", encoding="utf-8") as record:
                record.write(json.dumps(message.get("params", {}), separators=(",", ":")))
        result = {"sessionId": session_id}
        if config_options:
            result["configOptions"] = render_config_options()
        respond(message, result)
    elif method == "session/set_config_option":
        params = message.get("params", {})
        config_id = params.get("configId")
        value = params.get("value")
        if os.environ.get("ACP_SET_CONFIG_RECORD"):
            with open(os.environ["ACP_SET_CONFIG_RECORD"], "a", encoding="utf-8") as record:
                record.write(f"{config_id}={value}\n")
        option = next((entry for entry in config_options if entry["id"] == config_id), None)
        # ACP_CONFIG_REFUSE_SET names an advertised option the agent refuses
        # to set with a JSON-RPC error even for an offered value.
        if option is None or value not in option["values"] or os.environ.get("ACP_CONFIG_REFUSE_SET") == config_id:
            send({
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32602, "message": "unknown config option or value"},
            })
        else:
            # ACP_CONFIG_IGNORE_SET names an option the agent acknowledges but
            # never applies: the answer reports the OLD value as current.
            if os.environ.get("ACP_CONFIG_IGNORE_SET") != config_id:
                option["current"] = value
            # ACP_CONFIG_RESET_MODEL_ON names an option whose set resets the
            # model to its first offered value, the way an agent might when a
            # later option changes what the earlier one can be.
            if os.environ.get("ACP_CONFIG_RESET_MODEL_ON") == config_id:
                for entry in config_options:
                    if entry["id"] == "model":
                        entry["current"] = entry["values"][0]
            respond(message, {"configOptions": render_config_options()})
    elif method == "session/prompt":
        prompt_count += 1
        if os.environ.get("ACP_PROMPT_RECORD"):
            with open(os.environ["ACP_PROMPT_RECORD"], "w", encoding="utf-8") as record:
                record.write(json.dumps(message.get("params", {})))
        mode = os.environ.get("ACP_MODE", "normal")
        if mode == "timeout":
            time.sleep(60)
        if mode == "malformed":
            print("malformed json", file=sys.stderr, flush=True)
            print("{not-json", flush=True)
            break
        if mode == "early_exit":
            print("early boom", file=sys.stderr, flush=True)
            sys.exit(2)
        if mode == "tool_then_exit":
            # Report one tool call of the configured kind (default execute),
            # then die with a scripted terminal diagnostic. Used to prove the
            # side-effect gate fails closed after an external-or-unknown tool.
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tool-x",
                        "title": os.environ.get("ACP_TOOL_TITLE", "Bash: git push"),
                        "kind": os.environ.get("ACP_TOOL_KIND", "execute"),
                        "status": "in_progress"
                    }
                }
            })
            time.sleep(0.2)
            print(os.environ.get("ACP_EXIT_DIAGNOSTIC", "early boom"), file=sys.stderr, flush=True)
            sys.exit(2)
        if mode == "diagnostic_exit":
            # Die at the first prompt with a scripted terminal diagnostic and
            # exit status, before any session update: the pre-turn shape of a
            # provider refusing the requested model.
            print(os.environ.get("ACP_EXIT_DIAGNOSTIC", "early boom"), file=sys.stderr, flush=True)
            sys.exit(int(os.environ.get("ACP_EXIT_CODE", "2")))
        if mode == "write_file":
            path = os.environ.get("ACP_WRITE_PATH", "hello.txt")
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "w", encoding="utf-8") as file:
                file.write(os.environ.get("ACP_WRITE_CONTENT", "hello from sandbox\n"))
            if os.environ.get("ACP_WRITE_MTIME_EPOCH"):
                mtime = float(os.environ["ACP_WRITE_MTIME_EPOCH"])
                os.utime(path, (mtime, mtime))
        if mode == "cancel":
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "waiting for cancellation"}
                    }
                }
            })
            for cancel_line in sys.stdin:
                cancel_message = json.loads(cancel_line)
                if cancel_message.get("method") == "session/cancel":
                    with open(os.environ["ACP_CANCEL_RECORD"], "w", encoding="utf-8") as record:
                        record.write("session/cancel\n")
                    respond(message, {"stopReason": "cancelled"})
                    sys.exit(0)
        if mode == "ignore_cancel":
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "waiting for ignored cancellation"}
                    }
                }
            })
            for control_line in sys.stdin:
                control_message = json.loads(control_line)
                methods.append(control_message.get("method"))
                if control_message.get("method") == "session/cancel":
                    if os.environ.get("ACP_CANCEL_RECORD"):
                        with open(os.environ["ACP_CANCEL_RECORD"], "w", encoding="utf-8") as record:
                            record.write("session/cancel\n")
                    record_methods()
                    time.sleep(60)
        if mode == "tool_calls":
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tool-a",
                        "title": "Bash: cargo test",
                        "kind": "execute",
                        "status": "in_progress"
                    }
                }
            })
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "tool-a",
                        "status": "completed"
                    }
                }
            })
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tool-b",
                        "title": "Read: src/main.rs",
                        "kind": "read",
                        "status": "failed"
                    }
                }
            })
        if mode == "backgrounded_tool":
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tool-backgrounded",
                        "title": "Bash: git commit",
                        "kind": "execute",
                        "status": "in_progress"
                    }
                }
            })
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "tool-backgrounded",
                        "status": "completed",
                        "rawOutput": (
                            "Command running in background with ID: task-123. "
                            "Output is being written to: /tmp/task-123.output."
                        )
                    }
                }
            })
            time.sleep(60)
        if mode == "permission":
            send({
                "jsonrpc": "2.0",
                "id": "permission-1",
                "method": "session/request_permission",
                "params": {
                    "sessionId": session_id,
                    "toolCall": {"toolCallId": "tool-1"},
                    "options": [
                        {"optionId": "reject", "name": "Reject", "kind": "reject_once"},
                        {"optionId": "once", "name": "Allow once", "kind": "allow_once"},
                        {"optionId": "always", "name": "Allow always", "kind": "allow_always"}
                    ]
                }
            })
            permission_response = json.loads(sys.stdin.readline())
            with open(os.environ["ACP_PERMISSION"], "w", encoding="utf-8") as permission:
                permission.write(json.dumps(permission_response.get("result", {}), separators=(",", ":")))
        if mode == "permission_concurrent":
            # Ask for permission, then IMMEDIATELY send a session/update WITHOUT
            # waiting for the answer. The client must process this update while
            # its permission handler is still parked on the human -- which only
            # happens if the handler responds from a spawned task and does not
            # block the connection's dispatch loop.
            send({
                "jsonrpc": "2.0",
                "id": "permission-1",
                "method": "session/request_permission",
                "params": {
                    "sessionId": session_id,
                    "toolCall": {"toolCallId": "tool-1"},
                    "options": [
                        {"optionId": "reject", "name": "Reject", "kind": "reject_once"},
                        {"optionId": "always", "name": "Allow always", "kind": "allow_always"}
                    ]
                }
            })
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "concurrent "}
                    }
                }
            })
            permission_response = json.loads(sys.stdin.readline())
            with open(os.environ["ACP_PERMISSION"], "w", encoding="utf-8") as permission:
                permission.write(json.dumps(permission_response.get("result", {}), separators=(",", ":")))
        if mode == "permission_two_timeout":
            # Two permission requests in flight at once -- only reachable because
            # the client handler is non-blocking. Both are expected to time out;
            # the run must still report a single, bounded PermissionTimedOut.
            for request_id, tool_call_id in (("permission-1", "tool-1"), ("permission-2", "tool-2")):
                send({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": session_id,
                        "toolCall": {"toolCallId": tool_call_id},
                        "options": [
                            {"optionId": "always", "name": "Allow always", "kind": "allow_always"}
                        ]
                    }
                })
            # The timed-out permission tears the turn down; block until killed.
            for _line in sys.stdin:
                pass
        if mode == "interrupt_steer":
            if prompt_count == 1:
                send({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "interrupted "}
                        }
                    }
                })
                for control_line in sys.stdin:
                    control_message = json.loads(control_line)
                    methods.append(control_message.get("method"))
                    if control_message.get("method") == "session/cancel":
                        if os.environ.get("ACP_CANCEL_RECORD"):
                            with open(os.environ["ACP_CANCEL_RECORD"], "w", encoding="utf-8") as record:
                                record.write("session/cancel\n")
                        respond(message, {"stopReason": "cancelled"})
                        break
                continue
            if os.environ.get("ACP_STEER_PROMPT_RECORD"):
                with open(os.environ["ACP_STEER_PROMPT_RECORD"], "w", encoding="utf-8") as record:
                    record.write(json.dumps(message.get("params", {}), separators=(",", ":")))
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "steered:" + first_prompt_text(message)}
                    }
                }
            })
            record_methods()
            respond(message, {"stopReason": "end_turn"})
            break
        if mode == "steer":
            if prompt_count == 1:
                send({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "initial "}
                        }
                    }
                })
                respond(message, {"stopReason": "end_turn"})
                continue
            if os.environ.get("ACP_STEER_PROMPT_RECORD"):
                with open(os.environ["ACP_STEER_PROMPT_RECORD"], "w", encoding="utf-8") as record:
                    record.write(json.dumps(message.get("params", {}), separators=(",", ":")))
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "steered:" + first_prompt_text(message)}
                    }
                }
            })
            record_methods()
            respond(message, {"stopReason": "end_turn"})
            break
        for text in ["hello ", "from acp"]:
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": text}
                    }
                }
            })
        record_methods()
        respond(message, {"stopReason": os.environ.get("ACP_STOP_REASON", "end_turn")})
        if mode == "linger_after_response":
            while True:
                time.sleep(1)
        break
    else:
        send({
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "error": {"code": -32601, "message": "method not found"}
        })
"#
}
