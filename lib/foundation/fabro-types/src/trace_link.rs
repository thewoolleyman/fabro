//! What ties a run's spans to the trace and the dispatch that started it.
//!
//! Both ride the run's labels, the one per-run, non-secret metadata channel
//! that reaches the server's scheduler and the run's worker alike:
//!
//! - [`TRACEPARENT_LABEL`] holds the W3C `traceparent` of the span that
//!   launched the run. `fabro run` copies its own `TRACEPARENT` environment
//!   variable there, so the server's `run` span (and through it the worker's
//!   spans) joins the caller's trace.
//! - [`CORRELATION_LABELS`] are the dispatch correlation attributes a caller
//!   sets with `--label NAME=VALUE`. Exactly these names are copied onto the
//!   run's spans, and only values shaped like identifiers; every other label
//!   stays off the spans.

use std::collections::HashMap;

/// The label `fabro run` stores the caller's W3C `traceparent` under.
pub const TRACEPARENT_LABEL: &str = "traceparent";

/// The run labels copied onto the run's spans as attributes of the same name.
pub const CORRELATION_LABELS: [&str; 3] = [
    "work.item.id",
    "livespec.dispatch.id",
    "livespec.dispatch.factory",
];

/// The longest correlation value copied onto a span.
pub const MAX_CORRELATION_VALUE_LEN: usize = 128;

/// The correlation attributes `labels` carries, in [`CORRELATION_LABELS`]
/// order: only those names, and only values that are non-empty, at most
/// [`MAX_CORRELATION_VALUE_LEN`] bytes, and made of ASCII letters, digits and
/// `.`, `_`, `:`, `-`. Anything else is dropped rather than shipped.
#[must_use]
pub fn correlation_attributes(labels: &HashMap<String, String>) -> Vec<(String, String)> {
    CORRELATION_LABELS
        .iter()
        .filter_map(|name| {
            let value = labels.get(*name)?.trim();
            is_identifier(value).then(|| ((*name).to_owned(), value.to_owned()))
        })
        .collect()
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CORRELATION_VALUE_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// The run's `traceparent` label, when it holds a valid W3C value.
#[must_use]
pub fn traceparent(labels: &HashMap<String, String>) -> Option<&str> {
    labels
        .get(TRACEPARENT_LABEL)
        .map(|value| value.trim())
        .filter(|value| is_traceparent(value))
}

/// Whether `value` is a W3C `traceparent`: version `00`, a 32-hex trace id
/// and a 16-hex span id that are not all zeros, and 2-hex flags, in lowercase.
#[must_use]
pub fn is_traceparent(value: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    let hex = |part: &str, len: usize| {
        part.len() == len
            && part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    matches!(parts.as_slice(), [version, trace, span, flags]
        if *version == "00"
            && hex(trace, 32)
            && hex(span, 16)
            && hex(flags, 2)
            && trace.bytes().any(|byte| byte != b'0')
            && span.bytes().any(|byte| byte != b'0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn only_the_named_correlation_labels_are_copied() {
        let labels = labels(&[
            ("livespec.dispatch.factory", "hp"),
            ("work.item.id", "bd-ib-6vhgqg"),
            ("livespec.dispatch.id", "0f1e2d3c4b5a69788796a5b4c3d2e1f0"),
            ("team", "factory"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "x-honeycomb-team=secret"),
        ]);

        assert_eq!(correlation_attributes(&labels), vec![
            ("work.item.id".to_owned(), "bd-ib-6vhgqg".to_owned()),
            (
                "livespec.dispatch.id".to_owned(),
                "0f1e2d3c4b5a69788796a5b4c3d2e1f0".to_owned()
            ),
            ("livespec.dispatch.factory".to_owned(), "hp".to_owned()),
        ]);
    }

    #[test]
    fn a_value_not_shaped_like_an_identifier_is_dropped() {
        let long = "a".repeat(MAX_CORRELATION_VALUE_LEN + 1);
        for value in [
            "",
            "  ",
            "has space",
            "a=b",
            "x,y",
            "token\nnext",
            long.as_str(),
        ] {
            assert!(
                correlation_attributes(&labels(&[("work.item.id", value)])).is_empty(),
                "{value:?} must not be copied"
            );
        }
        assert_eq!(
            correlation_attributes(&labels(&[("work.item.id", " bd-ib-1 ")])),
            vec![("work.item.id".to_owned(), "bd-ib-1".to_owned())]
        );
    }

    #[test]
    fn a_valid_traceparent_label_is_read() {
        assert_eq!(
            traceparent(&labels(&[(TRACEPARENT_LABEL, VALID)])),
            Some(VALID)
        );
        assert_eq!(traceparent(&labels(&[])), None);
    }

    #[test]
    fn a_malformed_traceparent_is_rejected() {
        for value in [
            "",
            "not-a-traceparent",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        ] {
            assert!(!is_traceparent(value), "{value:?}");
        }
        assert!(is_traceparent(VALID));
    }
}
