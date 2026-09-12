//! The ONE text comparison every ACP availability decision is spelled with.
//!
//! The contract fixes it: "Text matching is a conjunction after Unicode
//! case-folding and whitespace normalization, never a regular expression."
//! The non-eligible guard markers and the configured `all_literals` are
//! compared against the same diagnostic through this one function, so a
//! guard can never normalize differently from the literal it must outrank.
//!
//! Rust's standard library offers Unicode lowercasing rather than full case
//! folding; the two differ only on a handful of scripts (the German sharp s,
//! for instance), none of which appears in a provider diagnostic this table
//! is measured against. The Dispatcher-side implementation uses Python's
//! `casefold`; for the ASCII English diagnostics both sides match, the two
//! agree byte for byte.

/// One diagnostic or literal reduced to its comparable form: lowercased and
/// whitespace-collapsed, so a literal survives the line wrapping and tab or
/// space drift a provider applies to the same sentence on different
/// transports.
#[must_use]
pub fn normalized(text: &str) -> String {
    text.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether EVERY literal appears in the normalized text. The empty
/// conjunction is reported as NO match rather than the vacuous truth, because
/// a signature matching every diagnostic is the one outcome no caller wants.
#[must_use]
pub fn conjunction_matches(literals: &[impl AsRef<str>], text: &str) -> bool {
    if literals.is_empty() {
        return false;
    }
    let haystack = normalized(text);
    literals
        .iter()
        .all(|literal| haystack.contains(&normalized(literal.as_ref())))
}

/// Whether any ONE of several conjunctions matches in full.
#[must_use]
pub fn matches_any_conjunction(conjunctions: &[&[&str]], text: &str) -> bool {
    conjunctions
        .iter()
        .any(|literals| conjunction_matches(literals, text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_folds_case_and_whitespace() {
        assert_eq!(
            normalized("  The\tRequested   MODEL\n is not\r\n supported "),
            "the requested model is not supported"
        );
    }

    #[test]
    fn conjunction_requires_every_literal_and_refuses_empty() {
        let text = "HTTP 400: The requested model gpt-x is not supported when using Codex with a ChatGPT account";
        assert!(conjunction_matches(
            &[
                "requested model",
                "is not supported when using codex with a chatgpt account"
            ],
            text
        ));
        assert!(!conjunction_matches(
            &["requested model", "usage limit"],
            text
        ));
        let none: [&str; 0] = [];
        assert!(!conjunction_matches(&none, text));
    }

    #[test]
    fn any_conjunction_matches_when_one_does() {
        let text = "error running remote compact task: 404 Not Found at /responses/compact";
        assert!(matches_any_conjunction(
            &[&["hit your usage limit"], &[
                "error running remote compact task",
                "404 not found",
                "responses/compact"
            ],],
            text
        ));
        assert!(!matches_any_conjunction(&[&["hit your usage limit"]], text));
    }
}
