# Adversarial verification: C1 `122a6c2` (cohort/01-identity-and-sidecar)

Verified 2026-08-21 against TARGET `eb5d808`, parent `e3b8b26` (stack/base tip).
Read-only; no commits, checkouts, or file edits in the worktree.

Commit shape: 11 files, 1052 insertions, 28 deletions.
Per-file added lines: Cargo.lock 3, channel_session.rs 143, error.rs 3,
playlist_manager.rs 160, playout_loader.rs 20, core/Cargo.toml 3, cohort.rs
174, core/lib.rs 9, sidecar.rs 52, variant_request.rs 452, playout.rs 33.

## Verdicts

| Claim | Verdict |
|---|---|
| 1. Faithfulness to target | **PASS** |
| 2. No C3/C4/C5/C6 leakage | **PASS** |
| 3. Completeness vs maps 01 + 02 | **PASS** |

Supplementary gates run (not part of the three claims, both clean):
`cargo check --locked --workspace --all-features --all-targets` finishes with
no errors or warnings; `cargo test --locked --workspace` is all green
(0 failures across every target). Both run with `CARGO_TARGET_DIR` outside the
worktree so nothing was written into the repo.

---

## Claim 1 — faithfulness

Two independent mechanical checks, both run against `git show 122a6c2 -U0`:

1. **Line-set check (count-aware).** Every added line was matched against the
   target file's content with multiplicity, so a line added twice must exist
   twice in the target. Result: 1052 added lines, **7 not present in target**,
   all in `channel_session.rs`, all inside the sanctioned set.
2. **Block check (contiguity).** The stronger test: each contiguous run of
   added lines must appear as a verbatim substring of the target file, which
   catches lines that exist in the target but in a different arrangement.
   Result: 61 contiguous added blocks across the 11 files, **8 not verbatim**,
   all in `channel_session.rs`, all reducible to the same sanctioned set.

Every other file — `cohort.rs`, `sidecar.rs`, `variant_request.rs`,
`core/lib.rs`, `core/Cargo.toml`, `error.rs`, `playout_loader.rs`,
`playout.rs`, `playlist_manager.rs`, `Cargo.lock` — is 100% verbatim at both
line and block granularity.

The 8 non-verbatim blocks, mapped to the sanctioned adaptations:

| Block | Sanctioned form |
|---|---|
| `let is_templated = source_is_templated(&video_source) \|\| source_is_templated(&audio_source);` | yes — target prefixes `slate \|\|` and carries a 3-line slate-contract comment (both C4) |
| `.before_new_pipeline(pts_offset, subtitle_source, &current_item.id, declared_duration_ms, is_templated, false,)` | yes — target passes `slate` in the last position; the parameter name `fallback` in `playlist_manager` is already target-identical (CS-5) |
| `plan_for(&item, true, true, true, 400)` inside `a_templated_plan_ignores_the_stamp_error` | yes — 5-arg intermediate |
| `plan_for(&item, true, true, false, 27)` | yes |
| `plan_for(&item, true, true, false, 0)` | yes |
| `plan_for(&item, true, false, false, 0)` | yes |
| `fn templated_item()` with the JSON inlined and `.unwrap()` | yes — target expresses it as `templated_item_with_slate(None)`; the JSON body is byte-identical to the target fixture's |
| `is_templated: false,` in `a_live_source_never_seeks` | yes — F2 branch-only test, mechanical `TimingPlan` field ripple |

Corroborating detail:

- `plan_for` at C1 is `(item, start_at_zero, realtime, is_templated,
  stamp_error_ms)`; at target it is `(item, slate, is_templated,
  stamp_error_ms)` with `start_at_zero: true, realtime: true` hardcoded in the
  `TimingPlan` literal. Exactly the declared 5-arg intermediate.
- `emission_trim_ms` call sites: 8 in the commit tree, 8 in the target, all
  eight byte-identical including the `false`/`true` third arguments and the
  `plan.is_templated` production call. 1:1.
- `before_new_pipeline`'s signature and body in `playlist_manager.rs` are
  byte-identical to the target's, including the `fallback` parameter name.
- `a_templated_item_is_never_trimmed` and `a_templated_plan_ignores_the_stamp_error`
  differ from target only in the `plan_for` arity / fixture name; docs and
  asserts are byte-identical.

Deletions (28) were also inspected. They are: the base's 2-arg
`emission_trim_ms` calls and signature, the 2-arg `segment()` helper and its
two call sites, the base `before_new_pipeline` call, the 4-arg `plan_for`
calls, the two-line `PlannedTimings` doc (replaced with the target's
three-line doc verbatim), two duplicate `use super::*;` lines, and the
black-air census test's local item fixture. All either replaced by
target-verbatim content or covered by finding F-C1-5 below.

## Claim 2 — no leakage

Grepped the added lines of the commit diff for each named token:

| Token | Code hits | Notes |
|---|---|---|
| `query_parameters` | 0 | |
| `variant_manager` | 0 | no import, no reference |
| `composer::` / `VARIANT_HISTORY_DURATION` | 0 | the C6 const assert is absent |
| `served_window` | 0 | |
| `get_item_by_id` | 0 | confirmed still target-only in `playout_loader.rs` |
| `merged_source` | 0 | `config.rs` untouched |
| `slate` (identifier) | 0 | no field, fn, param, import, or module |

The literal string `slate` is **not** zero: it occurs 4 times, all in doc or
module comments, all inside C1-owned regions, all byte-identical to the
target:

1. `channel_session.rs` `emission_trim_ms` doc, "their slate slots are
   frame-aligned" — map 01 assigns this doc paragraph (target 1522-1525) to C1.
2. `core/sidecar.rs`, the `SidecarPipeline.fallback` field doc (twice). The
   field itself is target-named `fallback` and the whole file is C1's.
3. `playout.rs` `PlayoutItem::query_variable_names` doc, "The slate is
   deliberately not among them" — C1's function per the seed.

`composer` likewise appears twice in prose only: the `sidecar.rs` module doc
and the `composed_playlist_name` doc in `cohort.rs`, both C1-owned files, both
target-verbatim. No compile-time or runtime dependency on any later layer
exists; the workspace builds and tests with C3-C6 absent.

Files touched are exactly the C1 file set plus `Cargo.lock`. Dependency
additions are exactly the seed's list (`percent-encoding`, `serde`, dev
`filetime`, dev `tempfile`); the `Cargo.lock` delta is 3 lines, all inside the
`ersatztv-core` package block.

## Claim 3 — completeness

### Map 01 (channel_session.rs) C1 list, walked

| Region | Present |
|---|---|
| `TimingPlan.is_templated` field | yes |
| `PlannedTimings.declared_duration_ms` field | yes |
| `ChannelSession.published_recognized_params` field | yes |
| `new()` `published_recognized_params: None` | yes |
| `publish_recognized_params()` whole fn | yes (position differs, see F-C1-2) |
| `transcode()` call site | yes |
| `is_templated` computation | yes (sanctioned form) |
| `declared_duration_ms,` / `is_templated,` in the two literals | yes |
| `before_new_pipeline` call growth (+3 args +`fallback`) | yes |
| `emission_trim_ms` templated exemption (doc, param, early return) | yes |
| `build_output_settings` "Two jobs" comment | yes |
| `plan_timings`: `plan.is_templated`, `declared_duration_ms` computation + comment, return field | yes |
| `source_is_templated()` | yes |
| test `a_templated_item_is_never_trimmed` | yes |
| `false`/`true` third args on the P:drift trim tests | yes, all 8 sites |
| test `a_templated_plan_ignores_the_stamp_error` | yes (position differs) |
| `templated_item` fixture | yes (sanctioned inline form) |
| `planned.declared_duration_ms` assertion in `the_trim_reaches_every_stream_the_t_reads` | yes |

Nothing missing.

### Map 02 (playlist_manager.rs) C1 rows, walked

Import line 7, `current_item_id` + `pipelines` fields (117-118) and their
blank separator, `Segment.item_id` (144), both `new()` initializers (187-188),
`before_new_pipeline` signature growth (235-238), the `current_item_id`
assignment + `pipelines.push` (245-252), `item_id: self.current_item_id.clone()`
in the `Segment` push (332), the pipeline-record `retain` + comment (414-421),
the sidecar publish block (438-453) **including its three `io_context` calls
and `SIDECAR_SUFFIX`** per PM-D3, `generate_sidecar` (517-543), the `"item-a"`
extension in `manager_with_segments` (777), the 2-arg to 3-arg `segment()`
growth (782/788), `item_id` in `window_anchored_at` (995), the `"item-a"`
argument in the multi-owner trimming test (1200), and both C1 tests
(`sidecar_maps_segments_to_items_and_pipelines_to_offsets`,
`pipeline_records_prune_with_their_segments`). All present. Nothing missing.

### Reverse check (the decisive one)

`git diff 122a6c2 eb5d808` per file:

- `crates/ersatztv-core/` — **empty**. C1 already reproduces the target's core
  crate byte for byte.
- `error.rs` — **empty**.
- `playout_loader.rs` — 19 lines, exactly `get_item_by_id` (C3).
- `playout.rs` — 117 lines, exactly the `slate` field + schema-side doc +
  slate tests (C4).
- `playlist_manager.rs` — the residual is exactly C6's regions
  (`VARIANT_HISTORY_DURATION` + doc, the const assert, `history` /
  `extended_trim_warned` fields and initializers, `set_history_duration`, the
  extended-trim warn, the `served - self.history` one-liner, the two variant
  tests) plus the inherited test-block ordering artifact of F-C1-3. No C1
  content is outstanding.
- `channel_session.rs` — the residual is C3/C4/C5/C6 content plus the
  sanctioned adaptations plus the findings below.

---

## Findings, by severity

### F-C1-1 (medium; a base/map defect, not a C1 fault)

The `transcode_item` ffmpeg-spawn error that map 01 assigns to **P:io**
(target 1154-1159) is not in the carved branch and is not at stack/base.

- `fix/io-error-naming` (93c77f6) still has, at its line 750:
  `.map_err(|_| ChannelError::StreamFailure(String::from("failed to spawn ffmpeg")))?;`
- Same line survives at `e3b8b26` (stack/base tip) and at `122a6c2`.
- Target has `.map_err(|e| { ChannelError::StreamFailure(format!("failed to spawn ffmpeg {}: {e}", ...` .

No C-layer owns this region, so on the current plan it will never be written
and the final `diff C6 tip vs eb5d808 == EMPTY` gate cannot close. Fix by
amending `fix/io-error-naming` (same class of amend as PM-D1) or by explicitly
assigning the lines to a C-layer. Surfaced during C1 verification only because
C1's residual made it visible; it does not change the C1 verdicts.

### F-C1-2 (low; C1-caused, must be repaired by a later layer)

Three of C1's four new functions are inserted at positions that differ from
the target's module order. Content is identical; only placement differs, and
C1 moved nothing that already existed (a function-order diff of parent vs C1
shows exactly four insertions and zero moves).

| Function | C1 position | Target position |
|---|---|---|
| `publish_recognized_params` | after `prep_output_folder` | before `prep_output_folder` |
| `a_templated_plan_ignores_the_stamp_error` | after `a_virtual_start_offset_produces_no_emission_trim` | after `the_trim_reaches_every_stream_the_t_reads` |
| `templated_item` | after the clamp tests, before `every_black_air_line_...` | after `templated_item_with_slate`, in the slate fixture group |
| `a_templated_item_is_never_trimmed` | correct | correct |

C3 and C4 both rewrite these neighbourhoods, so the repair is cheap, but it
has to be an explicit expectation of their gates or the final empty-diff gate
fails on pure ordering.

### F-C1-3 (low; inherited from base, unrecorded)

`playlist_manager.rs`'s test module is out of target order at stack/base: the
P:io pair (`scanning_a_missing_segment_folder_names_the_folder`,
`trimming_a_segment_whose_file_is_gone_names_the_segment`) sits where the
target has the P:hls `source`/`cue` helpers and their three tests, which the
target places earlier and the io pair last. Map 02's D4 predicted the merge
hazard; the build sheet's findings F1/F2 record only channel_session ordering.
C1 neither caused nor fixed it. It should get an F-entry so a later layer
(C6 is the only one that rewrites this module) is expected to restore order.

### F-C1-4 (informational; a map gap that will bite C4)

Map 01's `plan_for` DUAL note says only "C1 adds `is_templated`, C4 adds
`slate`". It omits that the target's `plan_for` also **drops**
`start_at_zero` and `realtime` as parameters (hardcoding `start_at_zero: true,
realtime: true` in the `TimingPlan` literal). C4 therefore has to delete two
parameters and rewrite five call sites, not just add one. Worth recording so
the C4 gate does not read that churn as unfaithfulness.

### F-C1-5 (informational; recorded so a later audit does not call it overreach)

Two C1 edits touch content C1 does not own, and both converge on the target:

- It deletes two of the four duplicate `use super::*;` lines that base
  assembly left in `channel_session.rs`'s test module (4 at `e3b8b26`, 2 at
  C1, 2 at target). Sanctioned in spirit by PM-D4.
- It swaps the black-air census test
  (`every_black_air_line_names_its_slot_and_shares_one_phrase`) from its local
  inline item fixture to the shared `templated_item()`. Map 01's P:black-air
  note explicitly allows this at C1, and the target uses `templated_item()`
  there.

### F-C1-6 (informational)

A literal grep for `slate` over the commit is not empty (4 prose hits) and for
`composer` is not empty (2 prose hits). Both were checked line by line: every
occurrence is a doc or module comment inside a C1-owned region and is
byte-identical to the target. No identifier, import, field, or call. The
leakage claim holds on substance; the strict "grep returns nothing" reading of
it does not, and should be restated as "no C3-C6 identifiers" in future rounds.
