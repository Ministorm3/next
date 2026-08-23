# Small shared files: topic-ownership re-anchor (refresh to eb5d808)

Base: main = 4eec042. Target: eb5d808 (current worktree HEAD).
All ranges below are current-file line numbers at eb5d808, from
`git diff main...eb5d808 -- <file>` plus a direct read of the tip content.

---

## crates/ersatztv-channel/src/config.rs (+118/-)

- `use crate::error::{ChannelError, IoContext};` (:11) — **P:io** (import for the io_context sweep below)
- `merged_source: Value` field on `ChannelConfig` (:38-42) — **C3**
- `from_sources()` body (:503-566):
  - stdin read + cwd resolution wrapped in `.io_context_named(...)` (:528-535) — **P:io**
  - `tokio::fs::read_to_string(...).io_context("read the channel config file", config_path)?` (:547) — **P:io**
  - `config_value.clone()` + `channel_config.merged_source = config_value;` (:557,:561) — **C3**
- `merged_source_json()` accessor (:568-573) — **C3**
- `finalize()`: `ChannelConfigExpandOutputFolder` now carries the path string (:585-588) — **P:io**
- `#[cfg(test)] mod tests` (:636-713):
  - `replaying_the_merged_source_reproduces_the_configuration` (:655-692) — **C3** (exercises `merged_source`/`merged_source_json`)
  - `a_config_file_that_cannot_be_read_is_named` (:693-713) — **P:io** (exercises the io_context naming)

No disputes. Owners split cleanly along the seed's stated line.

---

## crates/ersatztv-channel/src/error.rs (+92/-)

Whole file is the `ChannelError` enum + the new `IoContext` trait. Per-region:

- `Io { operation, subject, source }` variant (:20-27 in the enum) — **P:io**
- `ChannelConfigExpandOutputFolder(String)` (:30) — **P:io** (payload added to carry the path)
- `OutputPathNotUtf8 { file, path }` (:33) — **P:io** (consumed at `ersatztv-channel/src/channel_session.rs:194,203,212,221`, inside the seed's IO topic region for that file)
- `PlayoutPathNotUtf8(String)` (:36) — **P:io** (consumed at `playout_loader.rs:97,102`, part of the same file's io_context hunk below)
- `JsonError(#[from] serde_json::Error)` (:45) — **C1** (per seed; not directly named at any call site in the diff — reached only via `?`/`#[from]`, so its consumer lives outside this file's diff, most likely C1's sidecar/cohort json handling)
- `PlayoutJsonVideoSourceRequired` message typo fix "vudei"→"video" (:66-67) — **DISPUTED**: trivial one-word fix, touches neither io nor json machinery, not named by any P/C in the seed. Recommend folding into **P:io** since that branch is the one already rewriting this file end-to-end; low stakes either way.
- `PtsScannerPathNotUtf8(String)` (replacing `PtsScannerFailure`) (:76) — **P:io** (consumed at `pts_scanner.rs:51`, a file not in this file set but clearly IO-sweep territory)
- `IoContext` trait + both impls (:100-150, end of file) — **P:io**

Bottom line: entire file is P:io except the single `JsonError` line (C1), plus one DISPUTED cosmetic typo fix.

---

## crates/ersatztv-channel/src/playout_loader.rs (+113/-)

- `use ersatztv_channel::error::{ChannelError, IoContext};` — **P:io**
- `get_item_by_id()` (:51-66) — **C3**
- `query_variable_names()` (:69-85) — **C1**
- `playout_file_for_time()` (:87-124): `.io_context("scan the playout folder", ...)` and both `PlayoutPathNotUtf8` conversions — **P:io**
- `#[cfg(test)] mod tests` (:132-182, EOF): single test `a_missing_playout_folder_is_not_reported_as_a_config_failure` — **P:io**

No disputes.

---

## crates/ersatztv-channel/src/main.rs (+62/-)

- `Commands::Variant { .. }` enum variant (:44-70) — **C3**
- Idle-timeout match-arm comment rewording only, no logic change (:82-86 area) — **P:idle-and-liveness**
- `Commands::Variant { .. } => { .. }` match arm in `run()` (:110-133): builds `ChannelConfig`, parses `params` via `url::form_urlencoded`, calls `.with_query_parameters(...)`, `run_variant(...)` — **C3**

No disputes.

---

## crates/ersatztv-channel/src/lib.rs (+3/-)

```
pub mod composer;      // C2
pub mod config;        // baseline
pub mod error;         // baseline
pub mod slate;         // C4
pub mod variant_manager; // C3
```
Three added lines, one owner each: `composer` → **C2**, `slate` → **C4**, `variant_manager` → **C3**.

---

## Cargo manifests / lockfile

**crates/ersatztv-channel/Cargo.toml** (+2): `filetime`, `url` deps added — **C3** (matches task note directly).

**Cargo.toml** (workspace, +3):
- `libc = "0.2"` — **P:folder-lock**
- `percent-encoding = "2.3"` — **C1**
- `url = "2.5"` — split consumer: used by both `ersatztv-channel` (C3) and `ersatztv-playout` (see below, P:stream-variables). The workspace-level declaration itself has no single owner; whichever branch lands first should add it, others rebase past it. Not a real dispute, just a shared workspace-key note.

**crates/ersatztv-core/Cargo.toml** (+9):
- `percent-encoding`, `serde` (regular deps) — **C1** (consumed by `cohort.rs`/`sidecar.rs`, confirmed via grep)
- `[target.'cfg(unix)'.dependencies] libc` — **P:folder-lock**
- `[dev-dependencies] filetime, tempfile` — **C1** (seed explicitly assigns these; likely used by C1's sidecar/cohort tests)

**crates/ersatztv-playout/Cargo.toml** (+2): `percent-encoding`, `url` — **P:stream-variables** (both are consumed only by `stream_variables.rs`, confirmed via grep — not by `playout.rs`'s C1 `query_variable_names`, which has no url/percent_encoding usage of its own).

**Cargo.lock** (+9, no new package stanzas): purely the dependency-list lines mirroring the four Cargo.toml changes above — same ownership split, no independent anchor needed.

---

## crates/ersatztv/src/main.rs (server, +247/-)

- `use ersatztv_core::cohort;` / `use ersatztv_core::variant_request;` (:16-17) — **C3**
- `stream()` (:176-235): added `Query(query_pairs)` extractor, `cohort::read_recognized_params/cohort_parameters/to_query_string`, and passes `cohort_query` into `get_multi_variant` — **C3**
- `fix_content_types()` (:236-286): `.m3u`/`.ts` content-type widening, Cache-Control `no-cache` block — **P:hls** (matches task description exactly)
- `get_multi_variant()` (:287-316): new `cohort_query: &str` param, `query_suffix` computed and appended to both the subtitle URI and the `live.m3u8` URI — **C3**
- `channel_playlist()` (:317-~394): `Query(query_pairs)` extractor, `cohort::forward_query_string`, `query_suffix` appended to each `/channel/{number}.m3u8` URL — **C3**
- `session_middleware()` (:409-448): only the added tail block (path/query extraction + `maybe_composed_playlist` call + early return) is new — **C3**. The pre-existing heartbeat-touch body above it is untouched baseline.
- `maybe_composed_playlist()` (:457-503) — **C3**
- `#[cfg(test)] mod tests` (:504-598, EOF): `serve()`, `cache_control()`, `content_type()` helpers + 4 tests (`a_media_playlist_is_never_reused_without_revalidation`, `the_channel_lineup_is_never_reused_without_revalidation`, `a_segment_keeps_its_current_cache_handling`, `only_playlists_and_segments_are_retyped`) — **P:hls** (this is the "eb5d808 cache-header test module" the seed's reconciliation-delta note flags; confirmed it exercises `fix_content_types` exclusively, not any C3 code)

No disputes — matches the task's own file description precisely.

---

## crates/ersatztv/src/channel_session.rs (server, +4/-)

Single hunk: `.kill_on_drop(true)` plus its comment on the channel-process spawn (:29-32) — **P:kill**. The private `channel_binary_path` mentioned in the seed as "reconciled baseline" does not appear in this diff at all (confirmed: it's baseline, not touched by main...eb5d808). No disputes.

---

## crates/ersatztv/src/xmltv.rs (+25/-)

Whole diff is the `<tvg_id>.xml` → `<number>.xml` fallback in `generate_blocking()` (:37-~110, candidates array at :85) — **P:xmltv**, confirmed self-contained (no interleaving with any other owner).

---

## crates/ersatztv-core/src/lib.rs (+11/-)

```
pub mod cohort;                                          // C1
mod folder_lock;                                          // P:folder-lock
mod merge;                                                 // baseline
mod path_resolve;                                          // baseline
pub mod sidecar;                                           // C1
pub mod variant_request;                                   // C1
pub use folder_lock::{FolderLock, lock_folder_exclusive};  // P:folder-lock
...
pub const RECOGNIZED_PARAMS_FILE_NAME: &str = "...";        // C1
```
Clean split, no disputes.

---

## crates/ersatztv-core/Cargo.toml

Covered above under manifests.

---

## crates/ersatztv-playout/src/playout.rs + lib.rs + Cargo.toml

**lib.rs** (+1): `pub mod stream_variables;` — **P:stream-variables**.

**playout.rs** (+158/-):
- `pub slate: Option<PlayoutItemSource>` field + doc comment on `PlayoutItem` (:93, doc block above it) — **C4**
- `slate: None` in the item constructor (:122) — **C4**
- `PlayoutItem::query_variable_names()` (:138-159ish) — **C1**
- `HttpSource`/`RtspSource` `uri` doc-comment additions describing stream-variable syntax (in the enum definition, not a separate line range from the schema but mirrored doc text on `PlayoutItemSource::Http`/`Rtsp`) — **P:stream-variables**
- `PlayoutItemSource::query_variable_names()` (:346-353ish) — **C1**
- `#[cfg(test)] mod tests` additions (:509-609ish):
  - `templated_item()` helper (:516-527) — shared C1/C4 test fixture (builds both the templated http source and an optional slate)
  - `a_playout_without_the_field_still_parses` (:539-549) — **C4**
  - `a_slate_parses_as_the_source_it_is` (:555-577) — **C4**
  - `an_absent_slate_is_omitted_rather_than_written_as_null` (:583-593) — **C4**
  - `a_slate_contributes_no_query_variables` (:597-609) — **C4** (exercises `query_variable_names()`'s interaction with `slate`, but the behavior under test — "slate contributes nothing" — is a C4 concern; C4 is declared to depend on C1 already existing, so this is consistent, not a layering violation)

No disputes; the one shared test helper (`templated_item`) is a minor joint-ownership note, not a conflict.

---

## schema/playout.json (+15/-)

- `"slate"` property on the playout-item schema (:70-80) — **C4**
- `HttpSource.properties.uri.description` (:445) — **P:stream-variables**
- `RtspSource.properties.uri.description` (:544) — **P:stream-variables**
- `DynamicSource.properties.uri` (:624) — unchanged, confirmed not part of this diff (baseline)
- The single `is_live` key (HttpSource) — confirmed already reconciled to one key at baseline; not touched by this diff.

No disputes.

---

## crates/ffpipeline/src/input.rs (+10/-)

Whole diff is `pub loop_when_exhausted: bool` on `ProbedInput` (:385) plus its doc comment — **C4**. Confirmed clean, matches seed exactly.

---

## crates/ffpipeline/src/pipeline.rs (+480/-)

- Import line adding `ProbeResultVideoStream` and `TPadFilter` (:9,:17-18 old numbering) — shared import for P:watermark (`ProbeResultVideoStream`) and P:drift (`TPadFilter`); no functional ownership conflict, just one shared `use` line.
- `PipelineInput::Audio { loop_when_exhausted: bool, .. }` (:220) — **C4**
- `PipelineInput::Video { loop_when_exhausted: bool, .. }` (:229) — **C4**
- `if final_output_settings.pad_to_duration { filters.push(TPadFilter...) }` (:401-405) — **P:drift** (rebuilt/timeline-drift-pad-and-trim's pad/TPad, per seed)
- `loop_when_exhausted: input_settings.audio_input.loop_when_exhausted` / `.video_input...` at input construction (:466,:479) — **C4**
- Graphics `extra_input_args` replaced by call to `watermark_input_args(...)` (:556-560) — **P:watermark**
- `loop_when_exhausted` destructured + `result.extend(loop_input_args(*loop_when_exhausted))` in both the Video (:837,:847) and Audio (:867,:878) input-arg builders — **C4**
- `fn loop_input_args()` (:983-991) — **C4**
- `fn watermark_input_args()` (:1005-1027) — **P:watermark**
- `#[cfg(test)] mod tests` (:1030-1419, EOF) — mixed, by test:
  - `slate_probe`, `slate_pipeline_args`, `slate_pipeline_args_with_graphics`, `slate_pipeline_args_full` (shared harness, incl. the `pad_to_duration: bool` param threaded through), `position_of` (:1047-1178) — **shared C4/P:drift/P:watermark test harness**. `slate_pipeline_args_full`'s `pad_to_duration` parameter is exercised only by the P:drift test below; the `loop_when_exhausted`/slate wiring is exercised by the C4 tests; the harness is also reused unmodified by the P:watermark still-image test. **Note (not a real dispute)**: whichever branch lands this harness first should carry the full parameter set (`pad_to_duration` + `loop_when_exhausted` + graphics), and the other two branches rebase on top rather than re-deriving it — splitting the harness itself would be counterproductive.
  - `a_slate_shorter_than_its_window_loops_to_fill_it` (:1180-1213) — **C4**
  - `a_slate_longer_than_its_window_is_cut_by_the_window_as_before` (:1214-1228) — **C4**
  - `an_item_that_is_not_slate_is_never_looped` (:1229-1238) — **C4**
  - `watermark_stream`, `has_pair` helpers (:1239-1266) — **P:watermark**
  - `still_image_watermarks_follow_the_probed_demuxer` (:1267-1297) — **P:watermark**
  - `a_still_image_layer_follows_the_probed_demuxer_in_the_built_pipeline` (:1298-1335) — **P:watermark**
  - `padding_to_duration_puts_tpad_in_the_chain` (:1336-1384) — **P:drift** (exercises the `pad_to_duration`→TPad wiring; long doc comment explains it's guarding wiring, not arithmetic, and directly references the still-image change from P:watermark's #211 as the reason the defect became visible — cross-branch narrative dependency worth preserving in the commit message when this test is carved out, but not an ownership conflict)
  - `animated_and_video_watermarks_do_not_pin_a_demuxer` (:1385-1398) — **P:watermark**
  - `animated_watermarks_loop_without_reopening_the_input` (:1399-1409) — **P:watermark**

No P:webvtt content appears anywhere in this diff (confirmed by full read) — consistent with the task's expectation that webvtt doesn't touch these two files. No UNASSIGNED regions: everything in both ffpipeline files resolves to C4, P:drift, or P:watermark.

---

## fork-ci / fork-tools / bench (new files only, no interleaving)

- `.github/workflows/fork-docker.yml` — new file, whole file — **fork-ci**
- `tools/twins.py` — new file, whole file — **fork-tools**
- `tools/timeline-bench/{README.md,analyze_arm.py,gen_content.sh,run_arm.sh}` — new files, whole directory — **bench branch, note only** (per instructions, not a P/C owner to carve)

Confirmed via `git diff --summary`: all six are `create mode`, nothing modifies a pre-existing file in these three areas.

---

## Per-file status (one line each)

- `crates/ersatztv-channel/src/config.rs` — clean split: P:io (io_context hunks + one test) vs C3 (merged_source/merged_source_json + one test)
- `crates/ersatztv-channel/src/error.rs` — clean split: P:io (nearly all) vs C1 (JsonError only); one DISPUTED trivial typo fix
- `crates/ersatztv-channel/src/playout_loader.rs` — clean 3-way split: P:io / C1 (query_variable_names) / C3 (get_item_by_id)
- `crates/ersatztv-channel/src/main.rs` — clean split: P:idle-and-liveness (comment only) vs C3 (Commands::Variant, both definition and match arm)
- `crates/ersatztv-channel/src/lib.rs` — clean 3-way: composer→C2, slate→C4, variant_manager→C3
- `crates/ersatztv-channel/Cargo.toml` / workspace `Cargo.toml` / `Cargo.lock` — clean split: C3 (channel's url+filetime), P:folder-lock (libc), C1 (core's percent-encoding/serde/dev-deps), P:stream-variables (playout's percent-encoding/url)
- `crates/ersatztv/src/main.rs` — clean split: P:hls (fix_content_types + its test module) vs C3 (everything cohort/query/composed-playlist)
- `crates/ersatztv/src/channel_session.rs` — entirely P:kill, confirmed channel_binary_path untouched (baseline)
- `crates/ersatztv/src/xmltv.rs` — entirely P:xmltv, confirmed self-contained
- `crates/ersatztv-core/src/lib.rs` — clean split: P:folder-lock vs C1
- `crates/ersatztv-core/Cargo.toml` — clean split: C1 deps vs P:folder-lock's libc
- `crates/ersatztv-playout/src/playout.rs` + `lib.rs` + `Cargo.toml` — clean 3-way: P:stream-variables (uri docs, stream_variables module, its own url/percent-encoding deps) / C1 (query_variable_names) / C4 (slate field + 3 of 4 new tests)
- `schema/playout.json` — clean split: C4 (slate property) vs P:stream-variables (uri descriptions); is_live confirmed already reconciled at baseline
- `crates/ffpipeline/src/input.rs` — entirely C4
- `crates/ffpipeline/src/pipeline.rs` — clean split across C4/P:drift/P:watermark, including the test module; one shared test-harness note (not a dispute)
- `.github/workflows/fork-docker.yml`, `tools/twins.py`, `tools/timeline-bench/*` — new files only, no interleaving: fork-ci / fork-tools / bench(note-only) respectively

## DISPUTED

1. **`crates/ersatztv-channel/src/error.rs:66-67`** — `PlayoutJsonVideoSourceRequired` message typo fix ("vudei" → "video"). Not io, not json, not named by any P/C in the seed. Recommendation: fold into **P:io** since that branch already rewrites this file end-to-end; cost of misattribution is negligible either way.

## UNASSIGNED

None found. Every changed region across all 21 files/paths resolves to a named P-branch or C-layer, or (for the bench tooling) is explicitly note-only per the task's own instruction.
