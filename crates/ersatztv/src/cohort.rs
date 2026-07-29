//! Cohort identity from request query parameters.
//!
//! A viewer cohort is identified by the custom query parameters a request
//! carries, restricted to the `{query:}` variable names the channel's playout
//! actually references (published by the worker; see
//! [`ersatztv_core::RECOGNIZED_PARAMS_FILE_NAME`]). Restricting matters
//! because players and proxies routinely append parameters of their own, and
//! a parameter that cannot change what is played must not mint a cohort.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

// parameters consumed by streaming itself, never cohort identity
const RESERVED_PARAMETERS: &[&str] = &["mode", "access_token", "index"];

// the characters percent-encoding leaves alone: ALPHA / DIGIT / - . _ ~
const ESCAPE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// The recognized parameter names a channel's worker has published, or an
/// empty set when the worker has not published any (no templated items, or
/// the worker has not started).
pub async fn read_recognized_params(output_folder: &Path) -> BTreeSet<String> {
    let path = output_folder.join(ersatztv_core::RECOGNIZED_PARAMS_FILE_NAME);
    match tokio::fs::read_to_string(&path).await {
        Ok(json) => serde_json::from_str::<Vec<String>>(&json)
            .map(|names| names.into_iter().collect())
            .unwrap_or_default(),
        Err(_) => BTreeSet::new(),
    }
}

/// The request parameters that identify a viewer cohort: reserved names are
/// dropped, only recognized names are kept (compared case-insensitively), and
/// a repeated parameter resolves to its last value. Keys are lowercased so
/// equal cohorts always produce equal maps.
pub fn cohort_parameters(
    query_pairs: &[(String, String)],
    recognized: &BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();

    for (key, value) in query_pairs {
        let key = key.to_ascii_lowercase();

        if RESERVED_PARAMETERS.contains(&key.as_str()) || !recognized.contains(&key) {
            continue;
        }

        result.insert(key, value.clone());
    }

    result
}

/// The canonical query-string form of a cohort: ordinal-sorted, url-encoded
/// pairs. Equal cohorts always produce equal strings, so this is also the
/// cohort's identity key.
pub fn to_query_string(parameters: &BTreeMap<String, String>) -> String {
    parameters
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                utf8_percent_encode(k, ESCAPE_SET),
                utf8_percent_encode(v, ESCAPE_SET)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(input: &[(&str, &str)]) -> Vec<(String, String)> {
        input
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn recognized(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn drops_reserved_parameters() {
        let result = cohort_parameters(
            &pairs(&[("mode", "ts"), ("access_token", "abc"), ("region", "west")]),
            &recognized(&["region", "mode", "access_token"]),
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result["region"], "west");
    }

    #[test]
    fn drops_unrecognized_parameters() {
        let result = cohort_parameters(
            &pairs(&[("region", "west"), ("cachebust", "12345")]),
            &recognized(&["region"]),
        );
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("region"));
    }

    #[test]
    fn matches_recognized_names_case_insensitively() {
        let result = cohort_parameters(&pairs(&[("Region", "west")]), &recognized(&["region"]));
        assert_eq!(result["region"], "west");
    }

    #[test]
    fn repeated_parameter_resolves_to_last_value() {
        let result = cohort_parameters(
            &pairs(&[("region", "east"), ("region", "west")]),
            &recognized(&["region"]),
        );
        assert_eq!(result["region"], "west");
    }

    #[test]
    fn empty_recognized_set_yields_empty_cohort() {
        let result = cohort_parameters(&pairs(&[("region", "west")]), &BTreeSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn canonical_string_is_sorted_and_encoded() {
        let mut parameters = BTreeMap::new();
        parameters.insert(String::from("zone"), String::from("us east"));
        parameters.insert(String::from("region"), String::from("west&x=1"));

        assert_eq!(
            to_query_string(&parameters),
            "region=west%26x%3D1&zone=us%20east"
        );
    }

    #[test]
    fn equal_cohorts_produce_equal_strings() {
        let a = cohort_parameters(
            &pairs(&[("Region", "west"), ("lang", "en")]),
            &recognized(&["region", "lang"]),
        );
        let b = cohort_parameters(
            &pairs(&[("lang", "en"), ("region", "west")]),
            &recognized(&["region", "lang"]),
        );
        assert_eq!(to_query_string(&a), to_query_string(&b));
    }
}
