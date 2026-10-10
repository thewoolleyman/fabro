//! OTLP/HTTP export for fabro's spans (opt-in observability).
//!
//! This is an ADDITIVE, opt-in path: it activates ONLY when an OTLP endpoint
//! env var is set (`OTEL_EXPORTER_OTLP_ENDPOINT` or
//! `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`). When unset, [`otel_layer`] returns
//! `None`, no global tracer provider is installed, the tracing stack is the
//! existing `fmt`-only configuration, and nothing is exported. When an endpoint
//! IS configured, fabro's `tracing` spans are bridged to OTLP through
//! `tracing-opentelemetry`, and the provider is also installed as the
//! OpenTelemetry global so code that builds spans through the OpenTelemetry API
//! directly (the worker's `run_turn` spans, `fabro_petri::run_turn`) exports
//! through the same pipeline.
//!
//! Export can never fail a run. A malformed endpoint, a panic while building
//! the exporter, or a collector that refuses or never answers all degrade to
//! "no export": the batch processor exports on its own thread and drops what
//! it cannot deliver, and [`shutdown`] is best-effort.
//!
//! Standard OTLP env vars are honored: `OTEL_EXPORTER_OTLP_ENDPOINT` /
//! `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` /
//! `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`, `OTEL_EXPORTER_OTLP_HEADERS`,
//! `OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_SERVICE_NAME` and
//! `OTEL_RESOURCE_ATTRIBUTES`. Endpoint and protocol are resolved here (the
//! per-signal `TRACES_*` variable wins) and set programmatically; headers and
//! timeouts are read by the exporter itself; resource attributes are read by
//! the SDK's environment resource detector; the service name defaults to
//! `fabro`.
//!
//! Protocol: `http/json` and `http/protobuf` are both built. The default when
//! no protocol is configured is `http/json` — a deliberate fork deviation from
//! the OTLP spec default, because the factory's receiver accepts JSON on
//! `/v1/traces`. `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` switches. `grpc`
//! is not built and falls back to the default with a warning.

#![expect(
    clippy::disallowed_methods,
    reason = "intentional process-env lookup facade: reads the OTLP/OTEL env vars (names via fabro_static::EnvVars) to configure the exporter"
)]
#![expect(
    clippy::print_stderr,
    reason = "OTLP setup runs before the tracing subscriber is initialized, so stderr is the only diagnostic sink when the exporter fails to build"
)]

use std::collections::HashMap;
use std::sync::OnceLock;

use fabro_static::EnvVars;
use fabro_types::trace_link;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use tracing::Subscriber;
use tracing_subscriber::registry::LookupSpan;

/// Built once on first [`otel_layer`] call. `Some(None)` means "no endpoint
/// configured / exporter build failed" (export disabled);
/// `Some(Some(provider))` holds the live provider so [`shutdown`] can drain the
/// batch on exit.
static PROVIDER: OnceLock<Option<SdkTracerProvider>> = OnceLock::new();

/// Resolve the traces endpoint from the environment, or `None` to disable
/// export. Resolved here and passed programmatically so a malformed endpoint
/// fails the exporter build instead of silently falling back to localhost.
fn resolve_endpoint() -> Option<String> {
    resolve_endpoint_from(
        std::env::var(EnvVars::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)
            .ok()
            .as_deref(),
        std::env::var(EnvVars::OTEL_EXPORTER_OTLP_ENDPOINT)
            .ok()
            .as_deref(),
    )
}

/// Standard OTLP endpoint precedence for the traces signal: a non-empty
/// per-signal endpoint is used as-is; otherwise a non-empty base endpoint gets
/// `/v1/traces` appended. Empty / whitespace-only values are treated as unset.
/// Returns `None` when neither is set (export disabled, no localhost fallback).
fn resolve_endpoint_from(traces: Option<&str>, base: Option<&str>) -> Option<String> {
    fn non_empty(value: Option<&str>) -> Option<&str> {
        value.map(str::trim).filter(|trimmed| !trimmed.is_empty())
    }
    if let Some(endpoint) = non_empty(traces) {
        return Some(endpoint.to_string());
    }
    let base = non_empty(base)?;
    Some(format!("{}/v1/traces", base.trim_end_matches('/')))
}

/// Read the OTLP protocol from the environment: the per-signal
/// `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` wins over the base
/// `OTEL_EXPORTER_OTLP_PROTOCOL`; empty values are treated as unset.
fn protocol_from_env() -> Protocol {
    fn non_empty(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }
    let raw = non_empty(EnvVars::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL)
        .or_else(|| non_empty(EnvVars::OTEL_EXPORTER_OTLP_PROTOCOL));
    parse_protocol(raw.as_deref()).unwrap_or_else(|| {
        if let Some(value) = raw.as_deref() {
            eprintln!(
                "otel: unsupported OTLP protocol {:?}, using http/json",
                value.trim()
            );
        }
        Protocol::HttpJson
    })
}

/// Pure mapping of a protocol value to a transport, or `None` for an unset or
/// unrecognized value (the caller then defaults to `http/json`).
fn parse_protocol(raw: Option<&str>) -> Option<Protocol> {
    match raw.map(str::trim) {
        Some("http/json") => Some(Protocol::HttpJson),
        Some("http/protobuf") => Some(Protocol::HttpBinary),
        _ => None,
    }
}

fn build_provider() -> Option<SdkTracerProvider> {
    let endpoint = resolve_endpoint()?;

    // The SDK batch processor spawns a background export thread and `expect`s
    // that spawn to succeed; isolate any panic during exporter/provider
    // construction so a failure disables export instead of aborting fabro.
    let provider = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build_provider_inner(&endpoint)
    }))
    .unwrap_or_else(|_| {
        eprintln!("otel: panic while building the OTLP provider, disabling export");
        None
    })?;
    // Code that builds spans through the OpenTelemetry API (rather than
    // through `tracing`) reaches the same exporter through the global.
    global::set_tracer_provider(provider.clone());
    Some(provider)
}

fn build_provider_inner(endpoint: &str) -> Option<SdkTracerProvider> {
    let exporter = match SpanExporter::builder()
        .with_http()
        .with_protocol(protocol_from_env())
        .with_endpoint(endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(err) => {
            eprintln!("otel: failed to build the OTLP span exporter, disabling export: {err}");
            return None;
        }
    };

    let service_name =
        std::env::var(EnvVars::OTEL_SERVICE_NAME).unwrap_or_else(|_| "fabro".to_string());
    // `Resource::builder` runs the SDK's environment detector, so
    // `OTEL_RESOURCE_ATTRIBUTES` (the factory's correlation attributes) lands
    // on every exported span without this module naming any of them.
    let resource = Resource::builder().with_service_name(service_name).build();

    Some(
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    )
}

/// A `tracing_opentelemetry` layer wired to an OTLP/HTTP exporter, or `None`
/// when OTLP is not configured. Added to each subscriber beside the `fmt`
/// layer; `None` is a no-op layer.
pub(crate) fn otel_layer<S>() -> Option<tracing_opentelemetry::OpenTelemetryLayer<S, SdkTracer>>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let provider = PROVIDER.get_or_init(build_provider).as_ref()?;
    let tracer = provider.tracer("fabro");
    Some(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            // Keep exported spans close to fabro's own fields: no synthesized
            // busy/idle timing, thread or source-location attributes.
            .with_tracked_inactivity(false)
            .with_threads(false)
            .with_location(false),
    )
}

/// The OpenTelemetry context to parent this process's root span on, read from
/// the W3C `TRACEPARENT` env var, or `None` when it is unset, empty or
/// malformed.
///
/// The server sets `TRACEPARENT` when it spawns a `fabro __run-worker`
/// subprocess (see fabro-server's `otel_propagation`), so the worker's `run`
/// span joins the server's trace instead of starting a second one.
pub(crate) fn parent_context_from_env() -> Option<opentelemetry::Context> {
    parent_context_from_traceparent(std::env::var(EnvVars::TRACEPARENT).ok().as_deref())
}

/// Pure parse of a `traceparent` value into a parent context. `None` for an
/// unset, empty, or unparseable value: parenting on a context without a valid
/// span would be a no-op with a misleading name.
fn parent_context_from_traceparent(raw: Option<&str>) -> Option<opentelemetry::Context> {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    let value = raw.map(str::trim).filter(|trimmed| !trimmed.is_empty())?;
    let carrier = HashMap::from([("traceparent".to_string(), value.to_string())]);
    let cx = TraceContextPropagator::new().extract(&carrier);
    cx.span().span_context().is_valid().then_some(cx)
}

/// Put `fabro.run_id` and the run's dispatch correlation labels
/// ([`trace_link::CORRELATION_LABELS`], and only those) on a run span. A
/// no-op when export is off.
pub(crate) fn label_run_span(span: &tracing::Span, run_id: &str, labels: &HashMap<String, String>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    span.set_attribute("fabro.run_id", run_id.to_owned());
    for (name, value) in trace_link::correlation_attributes(labels) {
        span.set_attribute(name, value);
    }
}

/// Best-effort final flush and shutdown of the OTLP provider so the current
/// batch drains on a normal exit. No-op when OTLP was never configured. Called
/// once, late in `main`, beside `fabro_telemetry::shutdown()`; the batch
/// processor also exports periodically, so an abrupt exit drops at most the
/// last batch. A collector outage bounds this by the exporter's own timeout.
pub(crate) fn shutdown() {
    if let Some(Some(provider)) = PROVIDER.get() {
        // `shutdown` drains the batch processor; a separate `force_flush`
        // first would double the worst-case stall against a dead collector.
        let _ = provider.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_protocol_recognizes_json_and_protobuf() {
        assert!(matches!(
            parse_protocol(Some("http/json")),
            Some(Protocol::HttpJson)
        ));
        assert!(matches!(
            parse_protocol(Some("  http/json  ")),
            Some(Protocol::HttpJson)
        ));
        assert!(matches!(
            parse_protocol(Some("http/protobuf")),
            Some(Protocol::HttpBinary)
        ));
        assert!(parse_protocol(Some("grpc")).is_none());
        assert!(parse_protocol(Some("")).is_none());
        assert!(parse_protocol(None).is_none());
    }

    #[test]
    fn resolve_endpoint_precedence_and_disabled() {
        assert_eq!(
            resolve_endpoint_from(Some("http://h:4318/v1/traces"), Some("http://base:4318")),
            Some("http://h:4318/v1/traces".to_string())
        );
        assert_eq!(
            resolve_endpoint_from(None, Some("http://base:4318")),
            Some("http://base:4318/v1/traces".to_string())
        );
        assert_eq!(
            resolve_endpoint_from(None, Some("http://base:4318/")),
            Some("http://base:4318/v1/traces".to_string())
        );
        // Empty / whitespace / unset -> disabled (no localhost fallback).
        assert_eq!(resolve_endpoint_from(Some("  "), Some("")), None);
        assert_eq!(resolve_endpoint_from(None, None), None);
    }

    #[test]
    fn parent_context_from_traceparent_extracts_a_valid_w3c_value() {
        use opentelemetry::trace::TraceContextExt as _;

        let cx = parent_context_from_traceparent(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ))
        .expect("a valid traceparent yields a parent context");
        let span_context = cx.span().span_context().clone();
        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert!(span_context.is_sampled());
    }

    #[test]
    fn parent_context_from_traceparent_rejects_unset_empty_and_malformed_values() {
        assert!(parent_context_from_traceparent(None).is_none());
        assert!(parent_context_from_traceparent(Some("")).is_none());
        assert!(parent_context_from_traceparent(Some("   ")).is_none());
        assert!(parent_context_from_traceparent(Some("not-a-traceparent")).is_none());
        assert!(
            parent_context_from_traceparent(Some(
                "00-00000000000000000000000000000000-0000000000000000-01"
            ))
            .is_none()
        );
    }

    /// Export disabled is the default and must leave the process untouched:
    /// with no endpoint the builder short-circuits before any exporter,
    /// export thread, or global provider exists. Exercised against the pure
    /// resolver so the result cannot depend on the test runner's environment.
    #[test]
    fn export_is_disabled_without_an_endpoint() {
        let built = resolve_endpoint_from(None, None).and_then(|e| build_provider_inner(&e));
        assert!(built.is_none());
    }

    /// An unreachable collector must not fail the process: the exporter
    /// builds (the endpoint is well-formed), spans are accepted, and a
    /// shutdown against a port nothing listens on returns within the
    /// exporter's timeout instead of hanging or panicking.
    #[test]
    fn an_unreachable_collector_never_fails_the_caller() {
        use opentelemetry::trace::{Span as _, Tracer as _};

        // Port 9 (discard) on loopback: refused immediately on any host
        // without a listener there.
        let provider = build_provider_inner("http://127.0.0.1:9/v1/traces")
            .expect("a well-formed endpoint builds an exporter");
        let tracer = provider.tracer("test");
        let mut span = tracer.start("run_turn");
        span.end();
        let started = std::time::Instant::now();
        let _ = provider.shutdown();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "shutdown against a dead collector must be bounded"
        );
    }
}
