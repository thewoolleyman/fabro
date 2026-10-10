//! W3C Trace Context capture for worker subprocesses.
//!
//! The server executes a run inside an `info_span!("run", id)`; the actual work
//! happens in a `fabro __run-worker` subprocess that creates its OWN `run`
//! span. Without propagation those are two unrelated traces.
//! [`current_traceparent`] serializes the server's current span context to a
//! W3C `traceparent` string, which the worker launch path passes as the
//! `TRACEPARENT` env var (see `worker_runtime`) and the worker parents its run
//! span on.
//!
//! This is inert unless OTLP export is configured: the `tracing-opentelemetry`
//! layer is installed only when an OTLP endpoint is set (see `fabro-cli`'s
//! `otel` module), and without it the current span carries no valid
//! OpenTelemetry span context, so capture yields `None` and no env var is set.
//!
//! `traceparent` is NOT a secret — it holds trace/span ids and sampling flags
//! only, never a credential — so unlike `OTEL_EXPORTER_OTLP_HEADERS` it is safe
//! to hand to the sandboxed worker.

use std::collections::HashMap;

use fabro_types::trace_link;
use opentelemetry::Context;
use opentelemetry::propagation::TextMapPropagator as _;
use opentelemetry::trace::TraceContextExt as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Tie a run's server-side `run` span, before it is entered, to what launched
/// the run: its parent is the caller's span when the run carries a valid
/// `traceparent` label (`fabro run` copies its `TRACEPARENT` there), and it
/// carries `fabro.run_id` plus the run's correlation labels
/// ([`trace_link::CORRELATION_LABELS`], and only those). Without a label the
/// span stays a root. A no-op when export is off.
pub(crate) fn link_run_span(span: &tracing::Span, run_id: &str, labels: &HashMap<String, String>) {
    if let Some(parent) = trace_link::traceparent(labels).and_then(context_from_traceparent) {
        span.set_parent(parent);
    }
    span.set_attribute("fabro.run_id", run_id.to_owned());
    for (name, value) in trace_link::correlation_attributes(labels) {
        span.set_attribute(name, value);
    }
}

/// Parse a W3C `traceparent` into a parent context, or `None` when it holds
/// no valid span context.
fn context_from_traceparent(value: &str) -> Option<Context> {
    let carrier = HashMap::from([("traceparent".to_string(), value.to_string())]);
    let cx = TraceContextPropagator::new().extract(&carrier);
    cx.span().span_context().is_valid().then_some(cx)
}

/// The W3C `traceparent` for the currently-entered `tracing` span, or `None`
/// when there is no valid OpenTelemetry context to propagate (OTLP export
/// disabled, or no span entered).
///
/// Call this where the target span is unambiguously current. Do NOT call it
/// from a `spawn_blocking`/`tokio::spawn` closure: those run on a thread that
/// does not carry the caller's span, so the capture would silently yield
/// `None`.
pub(crate) fn current_traceparent() -> Option<String> {
    traceparent_from_context(&tracing::Span::current().context())
}

/// Pure serialization of an OpenTelemetry context to a W3C `traceparent`.
/// Returns `None` when the context holds no valid span context — the
/// propagator injects nothing in that case, which is exactly the
/// export-disabled path.
///
/// `tracestate` is deliberately dropped: the propagator writes it alongside
/// `traceparent`, but fabro configures no vendor trace state, so forwarding it
/// would carry an always-empty second variable. Propagate it too if a sampler
/// that populates trace state is ever configured.
fn traceparent_from_context(cx: &Context) -> Option<String> {
    let mut carrier = std::collections::HashMap::<String, String>::new();
    TraceContextPropagator::new().inject_context(cx, &mut carrier);
    carrier.remove("traceparent")
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    };

    use super::*;

    fn context_with_span(trace_id: TraceId, span_id: SpanId, flags: TraceFlags) -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            trace_id,
            span_id,
            flags,
            true,
            TraceState::default(),
        ))
    }

    #[test]
    fn serializes_a_valid_span_context_to_w3c_traceparent() {
        let cx = context_with_span(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").expect("valid trace id"),
            SpanId::from_hex("00f067aa0ba902b7").expect("valid span id"),
            TraceFlags::SAMPLED,
        );
        assert_eq!(
            traceparent_from_context(&cx).as_deref(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }

    // The export-disabled path: no OpenTelemetry layer means no valid span
    // context, and O2 must then behave exactly as it did before — no env var,
    // no worker-side reparenting.
    #[test]
    fn yields_none_for_an_invalid_span_context() {
        assert_eq!(traceparent_from_context(&Context::new()), None);
        let cx = context_with_span(TraceId::INVALID, SpanId::INVALID, TraceFlags::default());
        assert_eq!(traceparent_from_context(&cx), None);
    }

    /// The real export-off shape: a subscriber IS installed and the `run` span
    /// is live, but no OpenTelemetry layer sits under it, so the downcast finds
    /// nothing. Uses a bare registry (not the no-subscriber default) because
    /// with no dispatcher at all the span is disabled outright — that would
    /// reach the same `None` by a different route and prove less.
    #[test]
    fn current_traceparent_is_none_without_an_opentelemetry_layer() {
        let subscriber = tracing_subscriber::registry();

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("run", id = "test");
            let _guard = span.enter();
            assert_eq!(current_traceparent(), None);
        });
    }

    /// Capture against a REAL layer and tracer: exercises the `WithContext`
    /// downcast, the sampler, and the inject round-trip end to end.
    ///
    /// Scope, precisely — this builds the layer from fabro-server's OWN
    /// `tracing-opentelemetry`, so the layer and the downcast unify by
    /// construction. In production the pairing is different: fabro-CLI installs
    /// the layer and fabro-SERVER performs the downcast. This test therefore
    /// CANNOT detect the two crates drifting onto different
    /// `tracing-opentelemetry` versions — it would still pass while the
    /// production downcast failed on a TypeId mismatch and every capture
    /// silently yielded `None`. That specific hazard is pinned by
    /// `tracing_opentelemetry_resolves_to_one_version` below; what this test
    /// does catch is a behavioral break when the crates move together, and any
    /// regression local to this module.
    #[test]
    fn current_traceparent_captures_the_span_when_a_layer_is_installed() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt as _;

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("run", id = "test");
            let _guard = span.enter();

            let traceparent =
                current_traceparent().expect("an installed layer must yield a traceparent");

            // 00-<32 hex trace id>-<16 hex span id>-<2 hex flags>, all non-zero.
            let parts = traceparent.split('-').collect::<Vec<_>>();
            assert_eq!(parts.len(), 4, "malformed traceparent: {traceparent}");
            assert_eq!(parts[0], "00");
            assert_eq!(parts[1].len(), 32);
            assert_eq!(parts[2].len(), 16);
            assert_ne!(parts[1], "0".repeat(32), "trace id must be valid");
            assert_ne!(parts[2], "0".repeat(16), "span id must be valid");
        });
    }

    const CALLER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[derive(Clone, Debug, Default)]
    struct Recorded(std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);

    impl opentelemetry_sdk::trace::SpanExporter for Recorded {
        async fn export(
            &self,
            batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.lock().unwrap().extend(batch);
            Ok(())
        }
    }

    /// Run `body` under a real layer exporting to a recorder; return what it
    /// exported once every span closed.
    fn exported(body: impl FnOnce()) -> Vec<opentelemetry_sdk::trace::SpanData> {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt as _;

        let recorded = Recorded::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(recorded.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        tracing::subscriber::with_default(subscriber, body);
        let spans = recorded.0.lock().unwrap().clone();
        spans
    }

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// The caller's `traceparent` label parents the run span, and the worker
    /// traceparent captured under it carries the caller's trace id, so the
    /// whole chain joins the caller's trace.
    #[test]
    fn a_traceparent_label_parents_the_run_span_on_the_callers_trace() {
        let mut captured = None;
        let spans = exported(|| {
            let span = tracing::info_span!("run", id = "r1");
            link_run_span(&span, "r1", &labels(&[("traceparent", CALLER)]));
            let _guard = span.enter();
            captured = current_traceparent();
        });

        let captured = captured.expect("a traceparent under the linked span");
        assert_eq!(
            captured.split('-').nth(1),
            Some("4bf92f3577b34da6a3ce929d0e0e4736"),
            "the worker's traceparent continues the caller's trace: {captured}"
        );
        let run = spans
            .iter()
            .find(|span| span.name == "run")
            .expect("the run span");
        assert_eq!(
            run.span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(run.parent_span_id.to_string(), "00f067aa0ba902b7");
    }

    #[test]
    fn without_a_traceparent_label_the_run_span_is_a_root() {
        let spans = exported(|| {
            let span = tracing::info_span!("run", id = "r1");
            link_run_span(&span, "r1", &labels(&[("traceparent", "garbage")]));
            let _guard = span.enter();
        });
        let run = spans
            .iter()
            .find(|span| span.name == "run")
            .expect("the run span");
        assert_ne!(
            run.span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(run.parent_span_id, opentelemetry::trace::SpanId::INVALID);
    }

    #[test]
    fn only_the_correlation_labels_reach_the_run_span() {
        let spans = exported(|| {
            let span = tracing::info_span!("run", id = "r1");
            link_run_span(
                &span,
                "r1",
                &labels(&[
                    ("work.item.id", "bd-ib-6vhgqg"),
                    ("livespec.dispatch.id", "0f1e2d3c4b5a69788796a5b4c3d2e1f0"),
                    ("livespec.dispatch.factory", "hp"),
                    ("team", "platform"),
                ]),
            );
            let _guard = span.enter();
        });
        let run = spans
            .iter()
            .find(|span| span.name == "run")
            .expect("the run span");
        let attribute = |key: &str| {
            run.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(attribute("fabro.run_id").as_deref(), Some("r1"));
        assert_eq!(attribute("work.item.id").as_deref(), Some("bd-ib-6vhgqg"));
        assert_eq!(
            attribute("livespec.dispatch.id").as_deref(),
            Some("0f1e2d3c4b5a69788796a5b4c3d2e1f0")
        );
        assert_eq!(
            attribute("livespec.dispatch.factory").as_deref(),
            Some("hp")
        );
        assert_eq!(attribute("team"), None);
    }

    /// Pins the one hazard the capture test above cannot reach: fabro-CLI
    /// installs the OpenTelemetry layer, fabro-SERVER downcasts the subscriber
    /// to `tracing_opentelemetry`'s private `WithContext` to read it, and
    /// that downcast matches on `TypeId`. Two `tracing-opentelemetry`
    /// versions in the tree means two distinct `WithContext` types, a
    /// downcast that always fails, a capture that always yields `None`, and
    /// every run silently back to two disconnected traces — with no error
    /// raised anywhere.
    ///
    /// Scoped to `tracing-opentelemetry` ALONE, deliberately. The downcast type
    /// lives in that crate, so only its duplication produces the silent no-op.
    /// A split in `opentelemetry` / `opentelemetry_sdk` is a different, LOUD
    /// failure: this crate and `tracing-opentelemetry` would exchange
    /// mismatched `Context` types and fail to COMPILE — and the server↔worker
    /// boundary is a W3C `traceparent` STRING, not a shared type, so those two
    /// versions need not even agree across processes. Asserting exactly-one for
    /// them would guard nothing this feature needs while risking a false CI
    /// block the day an unrelated transitive dep pulls a second
    /// `opentelemetry`.
    ///
    /// Reads the lockfile rather than the manifests: only the lockfile shows
    /// what actually RESOLVED, which is what determines the `TypeId`.
    #[expect(
        clippy::disallowed_methods,
        reason = "unit test reading the workspace lockfile; no Tokio runtime and no async path involved"
    )]
    #[test]
    fn tracing_opentelemetry_resolves_to_one_version() {
        // CARGO_MANIFEST_DIR = lib/apps/fabro-server; `ancestors()` index 0 is
        // that path itself, so nth(3) is three parent steps
        // (fabro-server -> apps -> lib -> workspace root).
        let lockfile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("workspace root is three levels above the crate manifest")
            .join("Cargo.lock");
        let contents = std::fs::read_to_string(&lockfile)
            .unwrap_or_else(|err| panic!("reading {}: {err}", lockfile.display()));

        let resolved = contents
            .matches("name = \"tracing-opentelemetry\"\n")
            .count();
        assert_eq!(
            resolved, 1,
            "tracing-opentelemetry resolves to {resolved} versions; it must be exactly 1, or the \
             cli-installed layer and the server-side WithContext downcast stop sharing a TypeId \
             and traceparent capture silently returns None"
        );
    }
}
