//! Expansion of stream variables in playout item source URIs.
//!
//! Variables use single braces and are distinct from `{{ENV_VAR}}` templates,
//! which are expanded separately (and first):
//!
//! - `{channel_number}` or `{channel_number|default}`, the channel this
//!   playout is transcoding for
//! - `{query:name}` or `{query:name|default}`, a caller-supplied request
//!   parameter, where `name` matches `[A-Za-z0-9_.-]+`
//!
//! A variable resolves to its default (or an empty string) when no value is
//! available. Unrecognized braced text is left untouched, so URLs that happen
//! to contain braces keep working.

use std::collections::{BTreeSet, HashMap};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use url::Url;

// caller-supplied values are substituted into urls that ffmpeg then opens;
// anything longer than this, or carrying control characters, is treated as
// absent so the template's own default is used instead
const MAXIMUM_VALUE_LENGTH: usize = 256;

// rfc 3986 2.3 unreserved: ALPHA / DIGIT / "-" / "." / "_" / "~", the
// characters that need no percent-encoding anywhere in a uri
const ESCAPE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

enum Variable<'a> {
    ChannelNumber,
    Query(&'a str),
}

struct Match<'a> {
    variable: Variable<'a>,
    default: Option<&'a str>,
    /// Byte offset just past the closing `}`.
    end: usize,
}

pub fn has_variables(input: &str) -> bool {
    any_match(input, |_| true)
}

/// Whether a template's result can vary with the values a caller supplies, as
/// opposed to varying only with the channel it plays on.
pub fn has_query_variables(input: &str) -> bool {
    any_match(input, |m| matches!(m.variable, Variable::Query(_)))
}

/// The `query:` variable names a template references, lowercased. Callers use
/// this to discard request parameters that cannot change what the template
/// resolves to.
pub fn query_variable_names(input: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for_each_match(input, |m| {
        if let Variable::Query(key) = m.variable {
            names.insert(key.to_ascii_lowercase());
        }
        true
    });
    names
}

/// Expands a template containing no caller-supplied values, so every variable
/// resolves to its declared default. The result contains only
/// administrator-authored content.
pub fn expand_with_defaults(input: &str) -> String {
    expand(input, None, &HashMap::new(), |v| v.to_owned())
}

/// Substitutes values verbatim, applying no escaping of any kind. Only safe
/// when the destination needs no escaping, such as an argv element passed to a
/// process without a shell. A template expanded into a URL needs
/// [`expand_url`].
pub fn expand_unescaped(
    input: &str,
    channel_number: Option<&str>,
    query_parameters: &HashMap<String, String>,
) -> String {
    expand(input, channel_number, query_parameters, |v| v.to_owned())
}

/// Expands a template whose result is used as a URL. Caller-supplied values
/// are percent-encoded, and the expanded URL is required to keep the scheme,
/// host and port the template resolves to without them. A value that would
/// steer the URL elsewhere, or a template with no origin to preserve, resolves
/// without caller-supplied values at all.
pub fn expand_url(
    input: &str,
    channel_number: Option<&str>,
    query_parameters: &HashMap<String, String>,
) -> String {
    if input.is_empty() {
        return String::new();
    }

    // the channel number and the declared defaults, and nothing a caller
    // supplied; this is both the origin the expanded url has to agree with
    // and the result to fall back to when it does not
    let trusted = expand(input, channel_number, &HashMap::new(), |v| v.to_owned());

    if query_parameters.is_empty() {
        return trusted;
    }

    let expanded = expand(input, channel_number, query_parameters, |v| {
        utf8_percent_encode(v, ESCAPE_SET).to_string()
    });

    // a template that is not an absolute url has no origin to hold the
    // substitution to, so caller-supplied values cannot be bounded and are
    // refused rather than trusted
    let Ok(trusted_url) = Url::parse(&trusted) else {
        return trusted;
    };

    if let Ok(expanded_url) = Url::parse(&expanded)
        && expanded_url
            .scheme()
            .eq_ignore_ascii_case(trusted_url.scheme())
        && hosts_equal(&expanded_url, &trusted_url)
        && expanded_url.port_or_known_default() == trusted_url.port_or_known_default()
    {
        return expanded;
    }

    trusted
}

fn hosts_equal(a: &Url, b: &Url) -> bool {
    match (a.host_str(), b.host_str()) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        (None, None) => true,
        _ => false,
    }
}

fn expand<F: Fn(&str) -> String>(
    input: &str,
    channel_number: Option<&str>,
    query_parameters: &HashMap<String, String>,
    escape: F,
) -> String {
    let mut result = String::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        if input.as_bytes()[i] == b'{'
            && let Some(m) = try_match(&input[i..])
        {
            // defaults and the channel number come from the template and the
            // playout, so they are escaped no more than the surrounding
            // template is; only caller-supplied values cross a trust boundary
            let fallback = m.default.unwrap_or("");
            match m.variable {
                Variable::ChannelNumber => {
                    result.push_str(channel_number.unwrap_or(fallback));
                }
                Variable::Query(key) => {
                    let value = query_parameters
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(key))
                        .map(|(_, v)| v.as_str());
                    match value {
                        Some(v) if is_acceptable_value(v) => result.push_str(&escape(v)),
                        _ => result.push_str(fallback),
                    }
                }
            }
            i += m.end;
            continue;
        }

        let ch = input[i..].chars().next().unwrap_or('\u{FFFD}');
        result.push(ch);
        i += ch.len_utf8();
    }

    result
}

fn any_match(input: &str, predicate: impl Fn(&Match) -> bool) -> bool {
    let mut found = false;
    for_each_match(input, |m| {
        if predicate(&m) {
            found = true;
            return false;
        }
        true
    });
    found
}

/// Calls `f` for every variable in `input`; `f` returns false to stop early.
fn for_each_match(input: &str, mut f: impl FnMut(Match) -> bool) {
    let mut i = 0;
    while i < input.len() {
        if input.as_bytes()[i] == b'{'
            && let Some(m) = try_match(&input[i..])
        {
            let end = m.end;
            if !f(m) {
                return;
            }
            i += end;
            continue;
        }

        i += input[i..].chars().next().map_or(1, |c| c.len_utf8());
    }
}

/// Parses one variable at the start of `input` (which begins with `{`).
fn try_match(input: &str) -> Option<Match<'_>> {
    let rest = &input[1..];

    let (variable, after_name) = if let Some(after) = rest.strip_prefix("channel_number") {
        (Variable::ChannelNumber, after)
    } else {
        let after = rest.strip_prefix("query:")?;
        let key_len = after
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
            .count();
        if key_len == 0 {
            return None;
        }
        (Variable::Query(&after[..key_len]), &after[key_len..])
    };

    let (default, after_default) = match after_name.strip_prefix('|') {
        Some(after) => {
            let default_len = after
                .bytes()
                .take_while(|b| *b != b'{' && *b != b'}')
                .count();
            (Some(&after[..default_len]), &after[default_len..])
        }
        None => (None, after_name),
    };

    if !after_default.starts_with('}') {
        return None;
    }

    Some(Match {
        variable,
        default,
        end: input.len() - after_default.len() + 1,
    })
}

fn is_acceptable_value(value: &str) -> bool {
    value.chars().count() <= MAXIMUM_VALUE_LENGTH && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    mod has_variables {
        use super::*;

        #[test]
        fn false_for_plain_url() {
            assert!(!has_variables("http://localhost:8000/stream.ts"));
        }

        #[test]
        fn false_for_empty() {
            assert!(!has_variables(""));
        }

        #[test]
        fn false_for_unknown_braced_content() {
            assert!(!has_variables("http://localhost/{not_a_variable}"));
        }

        #[test]
        fn true_for_channel_number() {
            assert!(has_variables("http://localhost/{channel_number}"));
        }

        #[test]
        fn true_for_query_variable() {
            assert!(has_variables("http://localhost/?r={query:region}"));
        }
    }

    mod has_query_variables {
        use super::*;

        #[test]
        fn false_for_channel_number_only() {
            assert!(!has_query_variables("http://localhost/{channel_number}"));
        }

        #[test]
        fn true_for_query_variable() {
            assert!(has_query_variables("http://localhost/?r={query:region}"));
        }
    }

    mod query_variable_names_tests {
        use super::*;

        #[test]
        fn collects_lowercased_names() {
            let names = query_variable_names(
                "http://h/{query:Region|a}/x?l={query:lang}&c={channel_number}",
            );
            assert_eq!(
                names.into_iter().collect::<Vec<_>>(),
                vec!["lang".to_owned(), "region".to_owned()]
            );
        }

        #[test]
        fn empty_for_no_query_variables() {
            assert!(query_variable_names("http://localhost/{channel_number}").is_empty());
        }
    }

    mod expand_unescaped_tests {
        use super::*;

        #[test]
        fn expands_channel_number() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?id={channel_number}",
                Some("30"),
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/stream?id=30");
        }

        #[test]
        fn uses_default_when_channel_number_is_missing() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?id={channel_number|0}",
                None,
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/stream?id=0");
        }

        #[test]
        fn expands_to_empty_when_channel_number_is_missing_without_default() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?id={channel_number}",
                None,
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/stream?id=");
        }

        #[test]
        fn expands_query_variable() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?r={query:region}",
                Some("30"),
                &params(&[("region", "midwest")]),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=midwest");
        }

        #[test]
        fn matches_query_variable_case_insensitively() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?r={query:region}",
                Some("30"),
                &params(&[("Region", "midwest")]),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=midwest");
        }

        #[test]
        fn uses_default_when_query_parameter_is_missing() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?r={query:region|default-region}",
                Some("30"),
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=default-region");
        }

        #[test]
        fn expands_to_empty_when_query_parameter_is_missing_without_default() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?r={query:region}",
                Some("30"),
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=");
        }

        #[test]
        fn expands_multiple_variables() {
            let result = expand_unescaped(
                "http://localhost:8000/stream?id=etv-{channel_number}-{query:lang}&l={query:lang}",
                Some("30"),
                &params(&[("lang", "en")]),
            );
            assert_eq!(result, "http://localhost:8000/stream?id=etv-30-en&l=en");
        }

        #[test]
        fn leaves_unknown_braced_content_unchanged() {
            let result = expand_unescaped(
                "http://localhost:8000/{not_a_variable}/stream?id={channel_number}",
                Some("30"),
                &HashMap::new(),
            );
            assert_eq!(
                result,
                "http://localhost:8000/{not_a_variable}/stream?id=30"
            );
        }

        #[test]
        fn expands_script_command_line() {
            let result = expand_unescaped(
                "/usr/local/bin/generate.sh --channel {channel_number} --profile {query:profile|sd}",
                Some("5"),
                &params(&[("profile", "hd")]),
            );
            assert_eq!(
                result,
                "/usr/local/bin/generate.sh --channel 5 --profile hd"
            );
        }
    }

    mod expand_url_tests {
        use super::*;

        #[test]
        fn leaves_ordinary_values_unchanged() {
            let result = expand_url(
                "http://localhost:8000/stream?r={query:region}",
                Some("30"),
                &params(&[("region", "midwest")]),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=midwest");
        }

        #[test]
        fn encodes_value_that_would_inject_another_parameter() {
            let result = expand_url(
                "http://localhost:8000/stream?r={query:region}",
                Some("30"),
                &params(&[("region", "midwest&apikey=stolen")]),
            );
            assert_eq!(
                result,
                "http://localhost:8000/stream?r=midwest%26apikey%3Dstolen"
            );
        }

        #[test]
        fn encodes_value_that_would_traverse_path() {
            let result = expand_url(
                "http://localhost:8000/{query:path}/live.m3u8",
                Some("30"),
                &params(&[("path", "../../admin")]),
            );
            assert_eq!(result, "http://localhost:8000/..%2F..%2Fadmin/live.m3u8");
        }

        #[test]
        fn falls_back_to_defaults_when_value_would_change_host() {
            let result = expand_url(
                "http://{query:host|cdn.example.com}:8000/live.m3u8",
                Some("30"),
                &params(&[("host", "evil.example.com")]),
            );
            assert_eq!(result, "http://cdn.example.com:8000/live.m3u8");
        }

        #[test]
        fn uses_default_when_value_is_too_long() {
            let long_value = "a".repeat(257);
            let result = expand_url(
                "http://localhost:8000/stream?r={query:region|central}",
                Some("30"),
                &params(&[("region", &long_value)]),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=central");
        }

        #[test]
        fn uses_default_when_value_contains_control_characters() {
            let result = expand_url(
                "http://localhost:8000/stream?r={query:region|central}",
                Some("30"),
                &params(&[("region", "mid\nwest")]),
            );
            assert_eq!(result, "http://localhost:8000/stream?r=central");
        }

        #[test]
        fn does_not_encode_administrator_authored_default() {
            let result = expand_url(
                "http://localhost:8000/{query:path|region/west/hd}/live.m3u8",
                Some("30"),
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/region/west/hd/live.m3u8");
        }

        #[test]
        fn does_not_encode_channel_number() {
            let result = expand_url(
                "http://localhost:8000/stream?id={channel_number}",
                Some("30.1"),
                &HashMap::new(),
            );
            assert_eq!(result, "http://localhost:8000/stream?id=30.1");
        }

        // The two tests above supply no parameters, which returns the
        // defaults-only expansion before the encoder is ever reached. These
        // two keep a caller value present so the encoder is live, which is
        // the only state where "trusted content is substituted verbatim" is
        // observable at all.

        #[test]
        fn does_not_encode_defaults_while_encoding_caller_values() {
            let result = expand_url(
                "http://localhost:8000/{query:path|region/west/hd}/{query:name}.m3u8",
                Some("30"),
                &params(&[("name", "a/b")]),
            );
            assert_eq!(result, "http://localhost:8000/region/west/hd/a%2Fb.m3u8");
        }

        #[test]
        fn does_not_encode_channel_number_while_encoding_caller_values() {
            // nothing constrains a channel number to the characters the
            // escape set leaves alone, and it comes from the lineup rather
            // than from the caller
            let result = expand_url(
                "http://localhost:8000/{channel_number}/{query:name}.m3u8",
                Some("west/30"),
                &params(&[("name", "a/b")]),
            );
            assert_eq!(result, "http://localhost:8000/west/30/a%2Fb.m3u8");
        }

        #[test]
        fn refuses_caller_values_when_template_has_no_origin_to_preserve() {
            // a relative template has no scheme, host or port to hold the
            // substitution to, so caller-supplied values cannot be bounded
            let result = expand_url(
                "streams/{query:name|default}.m3u8",
                Some("30"),
                &params(&[("name", "supplied")]),
            );
            assert_eq!(result, "streams/default.m3u8");
        }

        #[test]
        fn resolves_channel_number_without_any_parameters() {
            let result = expand_url(
                "http://headend.local/feeds/{channel_number}/master.m3u8",
                Some("101"),
                &HashMap::new(),
            );
            assert_eq!(result, "http://headend.local/feeds/101/master.m3u8");
        }

        #[test]
        fn compares_origin_using_the_channel_number() {
            // the channel number is part of the origin the caller's value has
            // to agree with, so a template that builds its host from the
            // channel is not mistaken for a redirected one
            let result = expand_url(
                "http://ch{channel_number}.cdn.example/{query:region|central}/live.m3u8",
                Some("30"),
                &params(&[("region", "west")]),
            );
            assert_eq!(result, "http://ch30.cdn.example/west/live.m3u8");
        }

        #[test]
        fn empty_input_stays_empty() {
            assert_eq!(expand_url("", Some("30"), &HashMap::new()), "");
        }
    }

    mod expand_with_defaults_tests {
        use super::*;

        #[test]
        fn uses_defaults_for_all_variables() {
            let result = expand_with_defaults(
                "http://localhost:8000/stream?id={channel_number|1}&r={query:region|central}",
            );
            assert_eq!(result, "http://localhost:8000/stream?id=1&r=central");
        }

        #[test]
        fn expands_to_empty_without_defaults() {
            let result = expand_with_defaults(
                "http://localhost:8000/stream?id={channel_number}&r={query:region}",
            );
            assert_eq!(result, "http://localhost:8000/stream?id=&r=");
        }

        #[test]
        fn consumes_every_variable() {
            // an already-expanded url carries no variables, so it cannot be
            // expanded a second time; callers that need the channel number must
            // expand the stored template rather than a resolved path
            let result = expand_with_defaults(
                "http://localhost:8000/stream?id={channel_number|1}&r={query:region|central}",
            );
            assert!(!has_variables(&result));
        }
    }
}
