use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_client_protocol::schema::{
    CancelNotification, ContentBlock, ContentChunk, InitializeRequest, PermissionOptionKind,
    ProtocolVersion, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionNotification, SessionUpdate, StopReason, ToolCallStatus,
    ToolKind,
};
use agent_client_protocol::util::MatchDispatch;
use agent_client_protocol::{ActiveSession, Agent, Client, Error as ProtocolError, SessionMessage};
use fabro_sandbox::Sandbox;
use fabro_types::{Principal, SteeringMessage};
use fabro_util::time::elapsed_ms;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::command::AcpProcessSpec;
use crate::error::AcpError;
use crate::transport::{SandboxAcpTransport, TransportState};

pub type AcpNaturalCompletionCallback = Arc<dyn Fn() -> bool + Send + Sync>;
pub type AcpSteerPromptCallback = Arc<dyn Fn(String, Option<Principal>) + Send + Sync>;

/// A tool call the agent started or finished, as observed on the ACP
/// `session/update` stream. Bounded by construction: an id, a title, a kind
/// and timing -- never the tool's input or output, which stay in the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcpToolEvent {
    Started {
        tool_call_id: String,
        title:        String,
        kind:         String,
    },
    Completed {
        tool_call_id: String,
        title:        String,
        kind:         String,
        /// `true` when the adapter reported the call `completed`, `false` on
        /// `failed`.
        ok:           bool,
        elapsed_ms:   u64,
    },
}

pub type AcpToolEventCallback = Arc<dyn Fn(AcpToolEvent) + Send + Sync>;

/// Upper bound on the tool title carried in an [`AcpToolEvent`].
pub const TOOL_TITLE_MAX_BYTES: usize = 200;

/// One option the adapter offered on a `session/request_permission`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpPermissionOption {
    pub option_id: String,
    pub name:      String,
    /// The serde name of the option kind: `allow_once`, `allow_always`,
    /// `reject_once` or `reject_always`.
    pub kind:      String,
}

/// A permission request the adapter raised, reduced to what a human needs
/// to decide it: which tool call, what it is, and the offered options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpPermissionQuestion {
    pub tool_call_id: String,
    pub title:        String,
    pub kind:         String,
    pub options:      Vec<AcpPermissionOption>,
}

/// How a parked permission question was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcpPermissionAnswer {
    /// The `option_id` of the chosen option.
    Selected(String),
    /// The question was cancelled, interrupted or skipped: the adapter is
    /// told the request was cancelled and the turn continues.
    Cancelled,
    /// Nobody answered before the question's deadline: the adapter is told
    /// the request was cancelled and the TURN IS ENDED with
    /// [`AcpError::PermissionTimedOut`], so a workflow routes the node to
    /// its human gate rather than letting the agent carry on unanswered.
    TimedOut,
}

pub type AcpPermissionResolver = Arc<
    dyn Fn(AcpPermissionQuestion) -> Pin<Box<dyn Future<Output = AcpPermissionAnswer> + Send>>
        + Send
        + Sync,
>;

/// The serde name of a permission option kind (`allow_once`, ...).
fn permission_kind_name(kind: PermissionOptionKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "other".to_string())
}

/// Reduce a `session/request_permission` to the question a resolver sees.
fn permission_question(request: &RequestPermissionRequest) -> AcpPermissionQuestion {
    let tool_call_id = request.tool_call.tool_call_id.to_string();
    let mut title = request
        .tool_call
        .fields
        .title
        .clone()
        .unwrap_or_else(|| tool_call_id.clone());
    trim_to_head(&mut title, TOOL_TITLE_MAX_BYTES);
    AcpPermissionQuestion {
        tool_call_id,
        title,
        kind: request
            .tool_call
            .fields
            .kind
            .map_or_else(|| "other".to_string(), tool_kind_name),
        options: request
            .options
            .iter()
            .map(|option| AcpPermissionOption {
                option_id: option.option_id.to_string(),
                name:      option.name.clone(),
                kind:      permission_kind_name(option.kind),
            })
            .collect(),
    }
}

/// Turn a resolver's answer into the outcome sent back to the adapter. A
/// selection that names no offered option is a cancellation, never a guess.
fn permission_outcome_for_answer(
    request: &RequestPermissionRequest,
    answer: &AcpPermissionAnswer,
) -> RequestPermissionOutcome {
    match answer {
        AcpPermissionAnswer::Selected(option_id) => request
            .options
            .iter()
            .find(|option| option.option_id.to_string() == *option_id)
            .map_or(RequestPermissionOutcome::Cancelled, |option| {
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                    option.option_id.clone(),
                ))
            }),
        AcpPermissionAnswer::Cancelled | AcpPermissionAnswer::TimedOut => {
            RequestPermissionOutcome::Cancelled
        }
    }
}

const CANCEL_GRACE_PERIOD: Duration = Duration::from_millis(500);

/// Upper bound on the agent text tail carried in [`AcpTurnProgress`].
pub const PROGRESS_TEXT_TAIL_BYTES: usize = 4096;

/// Progress evidence gathered from the live ACP session, reported when a
/// turn ends abnormally (today: on timeout). It exists so that a turn which
/// exceeded its deadline while the agent was busy -- tool calls flowing,
/// text streaming -- is never described as a zero-output hang.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AcpTurnProgress {
    /// Tail of the agent message text accumulated so far, bounded to
    /// [`PROGRESS_TEXT_TAIL_BYTES`] and cut on a char boundary.
    pub text_tail:        String,
    /// Number of `session/update` notifications received.
    pub update_count:     u64,
    /// Number of tool calls the agent started (`SessionUpdate::ToolCall`).
    pub tool_call_count:  u64,
    /// Milliseconds from turn launch to the most recent update, if any.
    pub last_activity_ms: Option<u64>,
}

#[derive(Clone)]
struct ProgressTracker {
    inner:   Arc<Mutex<AcpTurnProgress>>,
    started: Instant,
}

impl ProgressTracker {
    fn new(started: Instant) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AcpTurnProgress::default())),
            started,
        }
    }

    fn note_update(&self, is_tool_call: bool) {
        let mut progress = self.inner.lock().expect("ACP progress lock poisoned");
        progress.update_count += 1;
        if is_tool_call {
            progress.tool_call_count += 1;
        }
        progress.last_activity_ms = Some(elapsed_ms(self.started));
    }

    fn push_text(&self, text: &str) {
        let mut progress = self.inner.lock().expect("ACP progress lock poisoned");
        progress.text_tail.push_str(text);
        trim_to_tail(&mut progress.text_tail, PROGRESS_TEXT_TAIL_BYTES);
    }

    fn snapshot(&self) -> AcpTurnProgress {
        self.inner
            .lock()
            .expect("ACP progress lock poisoned")
            .clone()
    }
}

/// Keep only the last `max_bytes` of `text`, cutting on a char boundary.
fn trim_to_tail(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut cut = text.len() - max_bytes;
    while !text.is_char_boundary(cut) {
        cut += 1;
    }
    text.drain(..cut);
}

/// Keep only the first `max_bytes` of `text`, cutting on a char boundary.
fn trim_to_head(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut cut = max_bytes;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    text.truncate(cut);
}

/// The serde name of a tool kind (`execute`, `read`, ...), which is what the
/// adapter put on the wire.
fn tool_kind_name(kind: ToolKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "other".to_string())
}

fn tool_call_is_terminal(status: ToolCallStatus) -> Option<bool> {
    match status {
        ToolCallStatus::Completed => Some(true),
        ToolCallStatus::Failed => Some(false),
        _ => None,
    }
}

struct OpenToolCall {
    title:   String,
    kind:    String,
    started: Instant,
}

/// Tracks the tool calls a turn has opened so a later `tool_call_update` can
/// be reported against the title, kind and start time of the call it closes.
#[derive(Default)]
struct ToolCallLedger {
    open: HashMap<String, OpenToolCall>,
}

impl ToolCallLedger {
    /// Turn one `session/update` into the tool events it implies: a
    /// `ToolCall` opens a call (and closes it too when it already carries a
    /// terminal status); a `ToolCallUpdate` with a terminal status closes it.
    /// Every other update yields nothing.
    fn observe(&mut self, update: &SessionUpdate, now: Instant) -> Vec<AcpToolEvent> {
        match update {
            SessionUpdate::ToolCall(call) => {
                let tool_call_id = call.tool_call_id.to_string();
                let mut title = call.title.clone();
                trim_to_head(&mut title, TOOL_TITLE_MAX_BYTES);
                let kind = tool_kind_name(call.kind);
                let mut events = Vec::new();
                if !self.open.contains_key(&tool_call_id) {
                    events.push(AcpToolEvent::Started {
                        tool_call_id: tool_call_id.clone(),
                        title:        title.clone(),
                        kind:         kind.clone(),
                    });
                }
                match tool_call_is_terminal(call.status) {
                    Some(ok) => {
                        let elapsed_ms = self
                            .open
                            .remove(&tool_call_id)
                            .map_or(0, |open| elapsed_since(open.started, now));
                        events.push(AcpToolEvent::Completed {
                            tool_call_id,
                            title,
                            kind,
                            ok,
                            elapsed_ms,
                        });
                    }
                    None => {
                        self.open
                            .entry(tool_call_id)
                            .and_modify(|open| {
                                open.title.clone_from(&title);
                                open.kind.clone_from(&kind);
                            })
                            .or_insert(OpenToolCall {
                                title,
                                kind,
                                started: now,
                            });
                    }
                }
                events
            }
            SessionUpdate::ToolCallUpdate(update) => {
                let tool_call_id = update.tool_call_id.to_string();
                if let Some(open) = self.open.get_mut(&tool_call_id) {
                    if let Some(title) = update.fields.title.as_ref() {
                        open.title.clone_from(title);
                        trim_to_head(&mut open.title, TOOL_TITLE_MAX_BYTES);
                    }
                    if let Some(kind) = update.fields.kind {
                        open.kind = tool_kind_name(kind);
                    }
                }
                let Some(ok) = update.fields.status.and_then(tool_call_is_terminal) else {
                    return Vec::new();
                };
                let (title, kind, elapsed_ms) = if let Some(open) = self.open.remove(&tool_call_id)
                {
                    (open.title, open.kind, elapsed_since(open.started, now))
                } else {
                    // A terminal update for a call whose start we never saw:
                    // report it against what the update carries so the
                    // completion is not lost, with no elapsed time.
                    let mut title = update
                        .fields
                        .title
                        .clone()
                        .unwrap_or_else(|| tool_call_id.clone());
                    trim_to_head(&mut title, TOOL_TITLE_MAX_BYTES);
                    let kind = update
                        .fields
                        .kind
                        .map_or_else(|| "other".to_string(), tool_kind_name);
                    (title, kind, 0)
                };
                vec![AcpToolEvent::Completed {
                    tool_call_id,
                    title,
                    kind,
                    ok,
                    elapsed_ms,
                }]
            }
            _ => Vec::new(),
        }
    }
}

fn elapsed_since(started: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(started).as_millis()).unwrap_or(u64::MAX)
}

#[derive(Default)]
struct AcpControlState {
    queue:               VecDeque<SteeringMessage>,
    waiting_for_steer:   bool,
    interrupt_requested: bool,
}

#[derive(Clone, Default)]
pub struct AcpControlHandle {
    state:  Arc<Mutex<AcpControlState>>,
    notify: Arc<Notify>,
}

impl AcpControlHandle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue_bounded(&self, item: SteeringMessage, cap: usize) -> Option<SteeringMessage> {
        self.push_bounded(item, cap, false)
    }

    pub fn interrupt(&self, _actor: Option<Principal>) {
        {
            let mut state = self.state.lock().expect("ACP control lock poisoned");
            if state.queue.is_empty() {
                state.waiting_for_steer = true;
            }
            state.interrupt_requested = true;
        }
        self.notify.notify_one();
    }

    pub fn interrupt_then_enqueue_bounded(
        &self,
        item: SteeringMessage,
        cap: usize,
    ) -> Option<SteeringMessage> {
        self.push_bounded(item, cap, true)
    }

    fn push_bounded(
        &self,
        item: SteeringMessage,
        cap: usize,
        request_interrupt: bool,
    ) -> Option<SteeringMessage> {
        let evicted = {
            let mut state = self.state.lock().expect("ACP control lock poisoned");
            let evicted = if state.queue.len() >= cap {
                state.queue.pop_front()
            } else {
                None
            };
            state.waiting_for_steer = false;
            if request_interrupt {
                state.interrupt_requested = true;
            }
            state.queue.push_back(item);
            evicted
        };
        self.notify.notify_one();
        evicted
    }

    #[must_use]
    pub fn has_pending_control_work(&self) -> bool {
        let state = self.state.lock().expect("ACP control lock poisoned");
        !state.queue.is_empty() || state.waiting_for_steer || state.interrupt_requested
    }

    #[cfg(test)]
    #[must_use]
    pub fn queue_len(&self) -> usize {
        self.state
            .lock()
            .expect("ACP control lock poisoned")
            .queue
            .len()
    }

    fn pop_steer(&self) -> Option<SteeringMessage> {
        let item = {
            let mut state = self.state.lock().expect("ACP control lock poisoned");
            let item = state.queue.pop_front();
            if item.is_some() {
                state.waiting_for_steer = false;
            }
            item
        };
        if item.is_some() {
            self.notify.notify_one();
        }
        item
    }

    fn take_interrupt_requested(&self) -> bool {
        let mut state = self.state.lock().expect("ACP control lock poisoned");
        let requested = state.interrupt_requested;
        state.interrupt_requested = false;
        requested
    }

    fn should_wait_for_steer(&self) -> bool {
        let state = self.state.lock().expect("ACP control lock poisoned");
        state.waiting_for_steer && state.queue.is_empty()
    }

    fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

#[derive(Default)]
pub struct AcpLiveControl {
    pub handle:                AcpControlHandle,
    pub on_natural_completion: Option<AcpNaturalCompletionCallback>,
    pub on_steer_prompt:       Option<AcpSteerPromptCallback>,
}

impl AcpLiveControl {
    #[must_use]
    pub fn new(handle: AcpControlHandle) -> Self {
        Self {
            handle,
            on_natural_completion: None,
            on_steer_prompt: None,
        }
    }
}

pub struct AcpRunRequest {
    pub command:               AcpProcessSpec,
    pub prompt:                String,
    pub cwd:                   String,
    pub timeout_ms:            Option<u64>,
    pub env:                   HashMap<String, String>,
    pub sandbox:               Arc<dyn Sandbox>,
    pub cancel_token:          CancellationToken,
    pub on_activity:           Option<Arc<dyn Fn() + Send + Sync>>,
    /// Receives one event per tool call the agent starts or finishes, so the
    /// run can surface per-tool progress without the adapter's payloads.
    pub on_tool_event:         Option<AcpToolEventCallback>,
    /// Decides the adapter's `session/request_permission` requests. `None`
    /// keeps today's behaviour: the most permissive offered option is chosen
    /// inline, without parking. `Some` parks each request on the resolver.
    pub on_permission_request: Option<AcpPermissionResolver>,
    pub live_control:          Option<AcpLiveControl>,
}

#[derive(Debug)]
pub struct AcpRunResult {
    pub text:        String,
    pub stop_reason: StopReason,
    pub stderr:      String,
    pub duration_ms: u64,
}

pub async fn run_acp_turn(request: AcpRunRequest) -> Result<AcpRunResult, AcpError> {
    let AcpRunRequest {
        command,
        prompt,
        cwd,
        timeout_ms,
        env,
        sandbox,
        cancel_token,
        on_activity,
        on_tool_event,
        on_permission_request,
        live_control,
    } = request;
    let live_control = live_control.unwrap_or_default();
    let start = std::time::Instant::now();
    let state = TransportState::new();
    let progress = ProgressTracker::new(start);
    let live_progress = progress.clone();
    let read_cancel_token = cancel_token.clone();
    let run_cancel_token = cancel_token.clone();
    let permission_cancel_token = cancel_token.clone();
    let transport = SandboxAcpTransport::new(command, cwd.clone(), env, sandbox, state.clone());
    // Set by the permission handler when a parked question went unanswered:
    // the turn is torn down and reported as `PermissionTimedOut` below.
    let permission_timeout: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
    let permission_timeout_for_handler = Arc::clone(&permission_timeout);
    let permission_state = state.clone();

    let run = Client
        .builder()
        .name("fabro")
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _connection| {
                let outcome = if permission_cancel_token.is_cancelled() {
                    RequestPermissionOutcome::Cancelled
                } else if let Some(resolver) = on_permission_request.as_ref() {
                    let question = permission_question(&request);
                    let answer = resolver(question.clone()).await;
                    if answer == AcpPermissionAnswer::TimedOut {
                        *permission_timeout_for_handler
                            .lock()
                            .expect("ACP permission timeout lock poisoned") =
                            Some((question.tool_call_id, question.title));
                        // End the turn: an unanswered permission is a human
                        // decision the loop cannot make, not a denial to work
                        // around. The teardown error is reported after the
                        // timeout slot is read, so a failure here is not lost.
                        let _ = permission_state.terminate().await;
                    }
                    permission_outcome_for_answer(&request, &answer)
                } else {
                    select_permission_outcome(&request)
                };
                responder.respond(RequestPermissionResponse::new(outcome))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(transport, async move |cx| {
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;

            cx.build_session(&cwd)
                .block_task()
                .run_until(async |mut session| {
                    session.send_prompt(prompt)?;
                    read_live_session(
                        &mut session,
                        &read_cancel_token,
                        &live_control.handle,
                        live_control.on_natural_completion.as_ref(),
                        live_control.on_steer_prompt.as_ref(),
                        on_activity.as_ref(),
                        on_tool_event.as_ref(),
                        &live_progress,
                    )
                    .await
                })
                .await
        });

    let cancel_deadline_token = cancel_token.clone();
    let run_outcome = async {
        match timeout_ms {
            Some(timeout_ms) => {
                if let Ok(result) = timeout(Duration::from_millis(timeout_ms), run).await {
                    Ok(result)
                } else {
                    state.terminate().await?;
                    if run_cancel_token.is_cancelled() {
                        return Err(AcpError::Cancelled);
                    }
                    Err(AcpError::TimedOut {
                        exec_output_tail: state.exec_output_tail().await,
                        progress:         progress.snapshot(),
                    })
                }
            }
            None => Ok(run.await),
        }
    };
    let outcome = tokio::select! {
        result = run_outcome => result,
        () = async {
            cancel_deadline_token.cancelled().await;
            sleep(Duration::from_millis(500)).await;
        } => {
            state.terminate().await?;
            return Err(AcpError::Cancelled);
        }
    };
    // Bind before the `if let`: an `if let` scrutinee's temporaries live for the
    // whole construct in edition 2021, so locking inline would hold the guard
    // across the `terminate().await` below and make this future `!Send`.
    let timed_out_permission = permission_timeout
        .lock()
        .expect("ACP permission timeout lock poisoned")
        .take();
    if let Some((tool_call_id, title)) = timed_out_permission {
        state.terminate().await?;
        return Err(AcpError::PermissionTimedOut {
            tool_call_id,
            title,
        });
    }
    let outcome = outcome?;
    let (text, stop_reason) = match outcome {
        Ok(result) => result,
        Err(_) if run_cancel_token.is_cancelled() => {
            state.terminate().await?;
            return Err(AcpError::Cancelled);
        }
        Err(error) => {
            state.terminate().await?;
            if let Some(startup_error) = state.take_startup_error().await {
                return Err(AcpError::Sandbox(startup_error));
            }
            if let Some(process_exit) = state.take_process_exit().await {
                return Err(AcpError::ProcessExited(process_exit));
            }
            return Err(map_protocol_error(error));
        }
    };

    match stop_reason {
        StopReason::EndTurn | StopReason::Refusal => {}
        StopReason::Cancelled => {
            state.terminate().await?;
            return Err(AcpError::Cancelled);
        }
        _ => {
            state.terminate().await?;
            return Err(AcpError::StopReason {
                stop_reason: render_stop_reason(&stop_reason),
                text,
            });
        }
    }

    state.terminate().await?;
    let stderr = state.stderr_tail().await;
    Ok(AcpRunResult {
        text,
        stop_reason,
        stderr,
        duration_ms: elapsed_ms(start),
    })
}

fn map_protocol_error(error: ProtocolError) -> AcpError {
    AcpError::Protocol(error)
}

fn select_permission_outcome(request: &RequestPermissionRequest) -> RequestPermissionOutcome {
    let selected = request
        .options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::AllowAlways)
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| option.kind == PermissionOptionKind::AllowOnce)
        })
        .or_else(|| {
            request.options.iter().find(|option| {
                !matches!(
                    option.kind,
                    PermissionOptionKind::RejectOnce | PermissionOptionKind::RejectAlways
                )
            })
        });

    selected.map_or(RequestPermissionOutcome::Cancelled, |option| {
        RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option.option_id.clone()))
    })
}

async fn read_live_session(
    session: &mut ActiveSession<'_, Agent>,
    cancel_token: &CancellationToken,
    control_handle: &AcpControlHandle,
    on_natural_completion: Option<&AcpNaturalCompletionCallback>,
    on_steer_prompt: Option<&AcpSteerPromptCallback>,
    on_activity: Option<&Arc<dyn Fn() + Send + Sync>>,
    on_tool_event: Option<&AcpToolEventCallback>,
    progress: &ProgressTracker,
) -> Result<(String, StopReason), ProtocolError> {
    let mut text = String::new();
    let mut tools = ToolCallLedger::default();
    let mut prompt_active = true;
    let mut cancel_sent = false;
    let mut last_stop_reason: Option<StopReason> = None;

    loop {
        if !prompt_active {
            if let Some(message) = control_handle.pop_steer() {
                if let Some(on_steer_prompt) = on_steer_prompt {
                    on_steer_prompt(message.text.clone(), message.actor.clone());
                }
                session.send_prompt(message.text)?;
                prompt_active = true;
                cancel_sent = false;
                continue;
            }

            if control_handle.take_interrupt_requested() {
                continue;
            }

            if control_handle.should_wait_for_steer() {
                let notified = control_handle.notified();
                tokio::select! {
                    () = cancel_token.cancelled() => {
                        return Ok((text, StopReason::Cancelled));
                    }
                    () = notified => {}
                }
                continue;
            }

            let stop_reason = last_stop_reason.unwrap_or(StopReason::EndTurn);
            if matches!(stop_reason, StopReason::EndTurn | StopReason::Refusal)
                && on_natural_completion.is_some_and(|callback| !callback())
            {
                // The lease reports pending control work but our flags didn't
                // observe it yet. Wait on a notify so we don't spin.
                let notified = control_handle.notified();
                tokio::select! {
                    () = cancel_token.cancelled() => {
                        return Ok((text, StopReason::Cancelled));
                    }
                    () = notified => {}
                }
                continue;
            }
            return Ok((text, stop_reason));
        }

        if control_handle.take_interrupt_requested() && !cancel_sent {
            cancel_sent = true;
            send_cancel_notification(session)?;
        }

        let control_notified = control_handle.notified();
        tokio::select! {
            update = session.read_update() => {
                if let Some(on_activity) = on_activity {
                    on_activity();
                }
                match update? {
                    SessionMessage::SessionMessage(dispatch) => {
                        MatchDispatch::new(dispatch)
                            .if_notification(async |notification: SessionNotification| {
                                progress.note_update(matches!(
                                    notification.update,
                                    SessionUpdate::ToolCall(_)
                                ));
                                for event in tools.observe(&notification.update, Instant::now()) {
                                    if let Some(on_tool_event) = on_tool_event {
                                        on_tool_event(event);
                                    }
                                }
                                if let SessionUpdate::AgentMessageChunk(ContentChunk {
                                    content: ContentBlock::Text(text_chunk),
                                    ..
                                }) = notification.update {
                                    progress.push_text(&text_chunk.text);
                                    text.push_str(&text_chunk.text);
                                }
                                Ok(())
                            })
                            .await
                            .otherwise_ignore()?;
                    }
                    SessionMessage::StopReason(stop_reason) => {
                        prompt_active = false;
                        cancel_sent = false;
                        last_stop_reason = Some(stop_reason);
                    }
                    _ => {}
                }
            }
            () = control_notified => {
                if control_handle.take_interrupt_requested() && !cancel_sent {
                    cancel_sent = true;
                    send_cancel_notification(session)?;
                }
            }
            () = cancel_token.cancelled(), if !cancel_sent => {
                cancel_sent = true;
                send_cancel_notification(session)?;
            }
            () = sleep(CANCEL_GRACE_PERIOD), if cancel_sent => {
                return Ok((text, StopReason::Cancelled));
            }
        }
    }
}

fn send_cancel_notification(session: &ActiveSession<'_, Agent>) -> Result<(), ProtocolError> {
    session
        .connection()
        .send_notification_to(Agent, CancelNotification::new(session.session_id().clone()))
}

#[must_use]
pub fn render_stop_reason(stop_reason: &StopReason) -> String {
    serde_json::to_value(stop_reason)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{stop_reason:?}"))
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::SessionNotification;

    #[test]
    fn codex_usage_update_session_notification_deserializes() {
        let notification = serde_json::json!({
            "sessionId": "session-1",
            "update": {
                "sessionUpdate": "usage_update",
                "used": 26128,
                "size": 258_400
            }
        });

        serde_json::from_value::<SessionNotification>(notification)
            .expect("Codex ACP usage_update notifications should be ignored, not fatal");
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn tracker_counts_updates_and_tool_calls_and_keeps_a_bounded_tail() {
        let tracker = ProgressTracker::new(Instant::now());
        assert_eq!(tracker.snapshot(), AcpTurnProgress::default());

        tracker.note_update(false);
        tracker.note_update(true);
        tracker.note_update(true);
        tracker.push_text("hello ");
        tracker.push_text(&"x".repeat(PROGRESS_TEXT_TAIL_BYTES));

        let progress = tracker.snapshot();
        assert_eq!(progress.update_count, 3);
        assert_eq!(progress.tool_call_count, 2);
        assert!(progress.last_activity_ms.is_some());
        assert_eq!(progress.text_tail.len(), PROGRESS_TEXT_TAIL_BYTES);
        assert!(!progress.text_tail.contains("hello"));
    }

    #[test]
    fn tool_ledger_opens_and_closes_calls_with_bounded_titles() {
        use agent_client_protocol::schema::{Plan, ToolCall, ToolCallUpdate, ToolCallUpdateFields};

        let mut ledger = ToolCallLedger::default();
        let t0 = Instant::now();
        let started = ledger.observe(
            &SessionUpdate::ToolCall(
                ToolCall::new("call-1", "Bash: sleep 25")
                    .kind(ToolKind::Execute)
                    .status(ToolCallStatus::InProgress),
            ),
            t0,
        );
        assert_eq!(started, vec![AcpToolEvent::Started {
            tool_call_id: "call-1".to_string(),
            title:        "Bash: sleep 25".to_string(),
            kind:         "execute".to_string(),
        }]);

        // A non-terminal update changes nothing but the recorded title.
        let none = ledger.observe(
            &SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                "call-1",
                ToolCallUpdateFields::new().title("Bash: sleep 25 (running)".to_string()),
            )),
            t0 + Duration::from_millis(5),
        );
        assert!(none.is_empty());

        let completed = ledger.observe(
            &SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                "call-1",
                ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
            )),
            t0 + Duration::from_millis(1234),
        );
        assert_eq!(completed, vec![AcpToolEvent::Completed {
            tool_call_id: "call-1".to_string(),
            title:        "Bash: sleep 25 (running)".to_string(),
            kind:         "execute".to_string(),
            ok:           true,
            elapsed_ms:   1234,
        }]);
        assert!(ledger.open.is_empty(), "a closed call must not linger");

        // A failed call reports ok=false; a call opened already-terminal
        // yields both events at once; an unknown id still reports its close.
        let failed = ledger.observe(
            &SessionUpdate::ToolCall(
                ToolCall::new("call-2", "x".repeat(TOOL_TITLE_MAX_BYTES + 50))
                    .kind(ToolKind::Read)
                    .status(ToolCallStatus::Failed),
            ),
            t0,
        );
        assert_eq!(failed.len(), 2);
        assert!(
            matches!(&failed[0], AcpToolEvent::Started { title, kind, .. }
            if title.len() == TOOL_TITLE_MAX_BYTES && kind == "read")
        );
        assert!(matches!(&failed[1], AcpToolEvent::Completed {
            ok: false,
            elapsed_ms: 0,
            ..
        }));
        let orphan = ledger.observe(
            &SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                "never-opened",
                ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
            )),
            t0,
        );
        assert_eq!(orphan, vec![AcpToolEvent::Completed {
            tool_call_id: "never-opened".to_string(),
            title:        "never-opened".to_string(),
            kind:         "other".to_string(),
            ok:           true,
            elapsed_ms:   0,
        }]);

        // Text chunks and plans are not tool events.
        let plan = ledger.observe(&SessionUpdate::Plan(Plan::new(Vec::new())), t0);
        assert!(plan.is_empty());
    }

    #[test]
    fn permission_question_and_answer_mapping_are_bounded_and_exact() {
        use agent_client_protocol::schema::{
            PermissionOption, PermissionOptionId, SessionId, ToolCallUpdate, ToolCallUpdateFields,
        };

        let request = RequestPermissionRequest::new(
            SessionId::new("session-1"),
            ToolCallUpdate::new(
                "call-9",
                ToolCallUpdateFields::new()
                    .title("Bash: rm -rf build/".to_string())
                    .kind(ToolKind::Execute),
            ),
            vec![
                PermissionOption::new(
                    PermissionOptionId::new("allow"),
                    "Allow",
                    PermissionOptionKind::AllowOnce,
                ),
                PermissionOption::new(
                    PermissionOptionId::new("always"),
                    "Always allow",
                    PermissionOptionKind::AllowAlways,
                ),
                PermissionOption::new(
                    PermissionOptionId::new("reject"),
                    "Reject",
                    PermissionOptionKind::RejectOnce,
                ),
            ],
        );

        let question = permission_question(&request);
        assert_eq!(question.tool_call_id, "call-9");
        assert_eq!(question.title, "Bash: rm -rf build/");
        assert_eq!(question.kind, "execute");
        assert_eq!(
            question
                .options
                .iter()
                .map(|option| (option.option_id.as_str(), option.kind.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("allow", "allow_once"),
                ("always", "allow_always"),
                ("reject", "reject_once")
            ]
        );

        // The default (no resolver) still picks the most permissive option.
        assert!(matches!(
            select_permission_outcome(&request),
            RequestPermissionOutcome::Selected(selected) if selected.option_id.to_string() == "always"
        ));
        // A resolver's selection is honoured only when it names an offered option.
        assert!(matches!(
            permission_outcome_for_answer(&request, &AcpPermissionAnswer::Selected("reject".to_string())),
            RequestPermissionOutcome::Selected(selected) if selected.option_id.to_string() == "reject"
        ));
        assert!(matches!(
            permission_outcome_for_answer(
                &request,
                &AcpPermissionAnswer::Selected("nope".to_string())
            ),
            RequestPermissionOutcome::Cancelled
        ));
        assert!(matches!(
            permission_outcome_for_answer(&request, &AcpPermissionAnswer::Cancelled),
            RequestPermissionOutcome::Cancelled
        ));
        assert!(matches!(
            permission_outcome_for_answer(&request, &AcpPermissionAnswer::TimedOut),
            RequestPermissionOutcome::Cancelled
        ));
    }

    #[test]
    fn trim_to_tail_cuts_on_a_char_boundary() {
        let mut text = "aé".repeat(10);
        trim_to_tail(&mut text, 5);
        assert!(text.len() <= 5);
        assert!(std::str::from_utf8(text.as_bytes()).is_ok());
        assert!(text.ends_with('é'));
    }
}
