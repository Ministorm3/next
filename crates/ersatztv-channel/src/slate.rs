//! The per-channel slate side file.
//!
//! Slate is configured in `slate.json` next to the playout folder rather than
//! as a channel.json field: legacy pipes the channel config over stdin and
//! rebuilds it per session, so a file the operator owns is the one place the
//! setting survives in both deployment shapes. Two readers share this file
//! and read different keys from it: the shared session reads `path` once per
//! templated window, and the variant manager reads `default` once per tick.
//! Both parse the whole struct: a key one reader ignores cannot silently
//! diverge between them, and an unknown key never rejects the file. A value
//! of the wrong type rejects the whole file for both readers, by name, and
//! each degrades to its no-config behavior.

use std::path::{Path, PathBuf};

use serde::Deserialize;

pub const SLATE_FILE_NAME: &str = "slate.json";

/// The operator's slate configuration for one channel.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct SlateConfig {
    /// Media the shared session transcodes for a templated window instead of
    /// tuning the live source. Also exactly the per-position fallback a
    /// routed viewer degrades to when its variant is late or dead.
    #[serde(default)]
    pub path: Option<String>,
    /// A raw query string naming the cohort that canonical-empty viewers
    /// (a bare query, or one whose parameters are all unrecognized) are
    /// admitted to. It is canonicalized through the same resolution as a
    /// real request, exactly once: a default that resolves to no cohort is
    /// treated as no default.
    #[serde(default)]
    pub default: Option<String>,
}

/// Where a channel's slate file lives: in the parent of the playout folder,
/// beside `current/`. `None` when the playout folder has no parent to hold
/// one.
pub fn slate_file(playout_folder: &Path) -> Option<PathBuf> {
    Some(playout_folder.parent()?.join(SLATE_FILE_NAME))
}

/// One read of the slate file, with the three outcomes its readers treat
/// differently. Only an absent file is `Missing`, the normal no-slate case;
/// a file that exists but cannot be read or parsed is `Malformed`, carrying
/// the named cause, so a permission or parse problem is never mistaken for
/// an operator removing the file.
pub enum SlateFile {
    Missing,
    Malformed(String),
    Present(SlateConfig),
}

pub async fn read_slate_file(file: &Path) -> SlateFile {
    let bytes = match tokio::fs::read(file).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return SlateFile::Missing,
        Err(e) => return SlateFile::Malformed(format!("unreadable: {e}")),
    };
    match serde_json::from_slice::<SlateConfig>(&bytes) {
        Ok(config) => SlateFile::Present(config),
        Err(e) => SlateFile::Malformed(format!("unparseable: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shared session's `path` read must survive a file that also carries
    /// `default`, and the variant manager's `default` read must survive a
    /// file that only carries `path`: the two readers share one struct so
    /// neither key can break the other.
    #[test]
    fn each_key_parses_with_or_without_the_other() {
        let path_only: SlateConfig = serde_json::from_str(r#"{"path": "/a.mp4"}"#).unwrap();
        assert_eq!(path_only.path.as_deref(), Some("/a.mp4"));
        assert_eq!(path_only.default, None);

        let default_only: SlateConfig =
            serde_json::from_str(r#"{"default": "zip=15216"}"#).unwrap();
        assert_eq!(default_only.path, None);
        assert_eq!(default_only.default.as_deref(), Some("zip=15216"));

        let both: SlateConfig =
            serde_json::from_str(r#"{"path": "/a.mp4", "default": "zip=15216"}"#).unwrap();
        assert_eq!(both.path.as_deref(), Some("/a.mp4"));
        assert_eq!(both.default.as_deref(), Some("zip=15216"));
    }

    /// Operators may carry keys this build does not know, and a future key
    /// must not make today's build refuse the whole file.
    #[test]
    fn unknown_keys_are_tolerated() {
        let config: SlateConfig =
            serde_json::from_str(r#"{"path": "/a.mp4", "note": "off air"}"#).unwrap();
        assert_eq!(config.path.as_deref(), Some("/a.mp4"));
    }

    #[test]
    fn the_slate_file_sits_beside_the_playout_folder() {
        assert_eq!(
            slate_file(Path::new("/channels/5/playout")),
            Some(PathBuf::from("/channels/5/slate.json"))
        );
        assert_eq!(slate_file(Path::new("/")), None);
    }

    #[tokio::test]
    async fn a_missing_file_and_a_malformed_file_are_distinct_outcomes() {
        let folder = tempfile::tempdir().unwrap();
        let file = folder.path().join(SLATE_FILE_NAME);

        assert!(matches!(read_slate_file(&file).await, SlateFile::Missing));

        tokio::fs::write(&file, "{not json").await.unwrap();
        assert!(matches!(
            read_slate_file(&file).await,
            SlateFile::Malformed(_)
        ));

        tokio::fs::write(&file, r#"{"path": "/a.mp4"}"#)
            .await
            .unwrap();
        assert!(matches!(
            read_slate_file(&file).await,
            SlateFile::Present(_)
        ));
    }

    /// A file that exists but cannot be read must not be mistaken for an
    /// absent one: `Missing` reads as "the operator configured nothing",
    /// and a policy silently flipping off on a permission error would be
    /// misattributed to removal. A directory at the slate path is the
    /// deterministic stand-in for an unreadable file.
    #[tokio::test]
    async fn an_unreadable_file_is_named_not_mistaken_for_missing() {
        let folder = tempfile::tempdir().unwrap();
        let file = folder.path().join(SLATE_FILE_NAME);
        tokio::fs::create_dir(&file).await.unwrap();

        match read_slate_file(&file).await {
            SlateFile::Malformed(error) => {
                assert!(error.starts_with("unreadable:"), "got: {error}")
            }
            _ => panic!("a directory at the slate path must read as Malformed"),
        }
    }
}
