# Adversarial verification: C3 layer commit 17569a5

Commit: `17569a5` "feat: serve per-cohort variants through worker-owned sessions"
on `cohort/03-sessions-and-serving`. Parent `e02c0e0` (C2). TARGET `eb5d808`.
Verified 2026-08-21 in the read-only stack worktree.

Method: mechanical, not narrative.
- Every `+` line of `git show 17569a5` was extracted per file and looked up in
  `git show eb5d808:<file>` (exact string match, plus an occurrence-count
  check). 1801 non-blank added lines checked.
- `git diff 17569a5 eb5d808` walked hunk by hunk for both files that still
  differ, to classify every residual line by owning layer.
- Leakage grep over the commit's added lines for the C4/C5/C6 vocabulary.
- Independent build gate: `git archive 17569a5` into a scratch tree,
  `CARGO_TARGET_DIR=/tmp/verify-target`.

## Verdicts

1. **FAITHFULNESS — PASS** (one adaptation outside the sanctioned list, LOW).
2. **NO LEAKAGE — PASS** (clean; three prose-only forward references noted).
3. **COMPLETENESS — FAIL** (code complete; two mapped C3-owned *comment*
   regions, 36 lines, were not landed).

Build gate (scratch tree, C3 tip standalone):
`cargo check --workspace --all-features --all-targets` clean,
`cargo clippy --locked --workspace --all-features --all-targets -- -D clippy::all`
clean, `cargo test --workspace` all green (0 failures across 309 executed
unit tests + the ignored ffpipeline integration suites),
`cargo +nightly fmt --all -- --check` clean.

---

## 1. Faithfulness

Files whose every added line is verbatim in TARGET, with **zero** deviations:

| File | added lines | absent from TARGET |
|---|---|---|
| `Cargo.lock` | 2 | 0 |
| `crates/ersatztv-channel/Cargo.toml` | 2 | 0 |
| `crates/ersatztv-channel/src/config.rs` | 69 | 0 |
| `crates/ersatztv-channel/src/lib.rs` | 1 | 0 |
| `crates/ersatztv-channel/src/main.rs` | 56 | 0 |
| `crates/ersatztv-channel/src/playout_loader.rs` | 19 | 0 |
| `crates/ersatztv/src/main.rs` | 101 | 0 |
| `crates/ersatztv-channel/src/channel_session.rs` | 546 | 2 (both sanctioned) |
| `crates/ersatztv-channel/src/variant_manager.rs` | 1005 | 23 (13 sanctioned, 10 = F-C3-1) |

### Sanctioned adaptations — all confirmed present and correct

- **run_variant lacks the three C6 subregions.** Verified by walking
  `git diff 17569a5 eb5d808 -- channel_session.rs`: the only insertions inside
  `run_variant` are (a) the STOPGAP `set_history_duration(VARIANT_HISTORY_DURATION)`
  block with its 6-line comment, (b) the spawned-claim reporting block
  (`spawned_progress_ms` binding + the two `log::info!` arms), (c) the claim-lag
  block. Nothing else.
- **progress_ms shadowed directly.** C3 has
  `let progress_ms = variant_start_progress_ms(progress_ms, anchor, ...)`;
  TARGET inserts `let spawned_progress_ms = progress_ms;` above and passes
  `spawned_progress_ms`. Exactly the one added line `            progress_ms,`
  is absent from TARGET. As sanctioned.
- **4-arg transcode_item in run_variant.** C3:
  `self.transcode_item(&item, true, false, Some(pts)).await?` on one line;
  TARGET: 5 args reflowed onto three lines. Exactly one absent added line.
  As sanctioned (CS-6: C4 edits this call).
- **VariantChannel literal lacks `slate_file`.** Pure omission, no rewritten
  line. Confirmed by reading `spawn_variant_loop` at C3 (4 fields: number,
  output_folder, channel_binary, config_json).
- **variant_manager lacks the C5 default-policy region and the C6 regions.**
  Every target-only function in that file is C5 or C6:
  `resolve_default_policy`, `log_policy_change`, `DefaultPolicy` (C5);
  `audit_served_window`, `deepest_variant_reach_ms`, the torn-request guard,
  the drop-reason reporting, the cohort-request liveness `touch_heartbeat`
  block (C6). The 13 sanctioned absent-added-lines are all signature/return
  shape at the seams those regions cut:
  - `use crate::composer::{SEGMENT_SECONDS, SessionPlaylist};` — TARGET adds
    `ComposedEntry` and `parse_pdt`, used only by C6's `deepest_variant_reach_ms`.
  - `read_requests(channel, &recognized)` → `(…, default_cohort)` returning
    `(Vec<ResolvedRequest>, bool)` — the C5 default and the C6 torn flag.
  - the bare `reap(...)` call → wrapped in C6's `if torn { … } else { … }`.
  - `Err(…) => return Vec::new()` / `return Vec::new()` / `requests` →
    tuple returns; the three `&'static str` reap reasons → `String`.
  - `cohort_query: cohort::to_query_string(&parameters),` → the C5
    default-substitution local.
- **Blank-line normalization.** No blank-only added line is unexplained.

### F-C3-1 (LOW) — an adaptation not on the sanctioned list

`variant_manager.rs`: C3 writes

```rust
async fn is_stale(path: &Path) -> bool {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return false;
    };

    metadata
        .modified()
        .ok()
        .and_then(|m| m.elapsed().ok())
        .is_some_and(|age| age.as_secs() >= SESSION_IDLE_SECONDS)
}
```

TARGET has `async fn staleness(path: &Path) -> Option<Duration>` with an
8-line doc, called as `if let Some(age) = staleness(&path).await`. Ten added
C3 lines are therefore not verbatim in TARGET, and C6 has to rewrite the whole
function rather than only its call site.

The age-returning shape exists *for* C6's drop-reason log (TARGET's own doc
says so: "Returns the age rather than a bool so the caller can say how stale a
dropped request was"), so this is defensible as a C3-form adaptation in the
same family as the `progress_ms` shadow. But it was avoidable: C3 could have
written `staleness` verbatim and called `staleness(&path).await.is_some()`,
leaving one call-site line for C6 instead of a whole-function rewrite.
Not a correctness problem; recorded so the C6 verifier does not read the
rewrite as C6 inventing a function.

### No invention

Every other added line in the commit is byte-identical to TARGET. No C3 line
introduces behaviour, wording, or a test that TARGET does not contain.

---

## 2. No leakage

Grep over the commit's added lines for
`slate | slate_file | DefaultPolicy | resolve_default_policy |
log_policy_change | default_cohort | VARIANT_HISTORY_DURATION |
set_history_duration | audit_served_window | deepest_variant_reach |
served_window | torn | staleness | reach_max | audit_warned`:

**Zero code hits.** Four prose hits, all benign and all verbatim in TARGET:

- `variant_manager.rs:208` — "staleness like any session" (ordinary English).
- `variant_progress_ms` doc — "a fallback (slate) window is produced ahead of
  air"; the code reads `pipeline.fallback`, a C1 field. No `slate` symbol.
- same doc — "already-served slate positions".
- test `a_slate_window_anchors_the_variant_at_the_envelope_start` — sets
  `pipeline.fallback = true`; C1 field only, no C4 dependency.

These are forward references in comments, not leakage. They are also exactly
the CS-5 naming split working as designed (C1 owns `fallback`, C4 renames the
*channel_session* argument; the variant_manager side stays `fallback` in
TARGET too).

File-level check: the commit touches **only** C3-owned files. It does not
touch `composer.rs`, `playlist_manager.rs`, `slate.rs`, `playout.rs`,
`ffpipeline/*`, `schema/playout.json`, or `crates/ersatztv/src/channel_session.rs`.
No C6 observability block in any file is modified or pre-empted.

---

## 3. Completeness

### channel_session.rs — map 01 C3 section

| C3 region (map 01) | present |
|---|---|
| imports `variant_manager` + `{VariantChannel, VariantManager}` | yes |
| `query_parameters` field + doc | yes |
| `query_parameters` init in `new` | yes |
| `with_query_parameters` + doc | yes |
| `spawn_playlist_publisher` extraction (sig, shell, doc ¶1-2, startup comment) | yes |
| deletion of the inline publish loop in `run` | yes |
| `run`: publisher call | yes |
| `run`: `self.spawn_variant_loop();` | yes |
| `run_variant` whole fn minus the three C6 subregions | yes |
| `spawn_variant_loop` whole fn minus the `slate_file` line | yes |
| `expand_stream_variables_url`: `&self.query_parameters` argument | yes |
| `expand_stream_variables_url`: the doc sentence about variant sessions | **NO — F-C3-3** |
| `shared_join_offset_ms` | yes |
| `variant_start_progress_ms` | yes |
| CS-2 residual comments on `input_timing_at` | **NO — F-C3-2** |
| `kill_on_drop` on the variant spawn (variant_manager) | yes |

Tests. Map 01's C3 "tests:" bullet list has **13 bullets = 11 tests + 2
helpers**, not 12 tests; the task brief's "12 tests" is a miscount of that
list. All 13 are present, in TARGET wording:

`variant_envelope` (helper), `window` (helper),
`a_shared_session_that_started_the_item_has_no_join_offset`,
`a_variant_that_opens_on_time_keeps_the_progress_it_was_given`,
`a_live_variant_opening_late_claims_where_the_wall_clock_stands`,
`a_live_source_never_seeks_however_far_the_session_has_progressed`,
`a_file_variant_is_never_moved_by_the_wall_clock`,
`a_late_open_cannot_claim_past_the_envelope`,
`a_shared_session_that_joined_late_reports_how_far_in_it_started`,
`a_variant_of_an_item_started_from_zero_fills_the_whole_remainder`,
`a_variant_of_a_late_joined_item_stops_where_the_shared_envelope_stops`,
`a_variant_produces_nothing_once_the_shared_envelope_is_covered`,
`a_variant_envelope_always_ends_with_the_shared_one`.

The old branch-only `a_live_source_never_seeks` is **deleted** by this commit
(`- fn a_live_source_never_seeks()`), replaced by the target-form test. F2 is
closed for this file, and the deletion is deliberate per CS-3.

### maps 02 / 03

- Map 02 (`playlist_manager.rs`) contains **no C3 rows**. The commit does not
  touch the file. Correct.
- Map 03 C3 rows, all present and, in every case, **byte-identical to TARGET**:
  `config.rs` (merged_source field, `from_sources` body, `merged_source_json`,
  `replaying_the_merged_source_reproduces_the_configuration`);
  `playout_loader.rs::get_item_by_id`; channel `main.rs`
  (`Commands::Variant` variant + match arm); channel `lib.rs`
  `pub mod variant_manager;`; channel `Cargo.toml` (`filetime`, `url`);
  server `main.rs` (cohort/variant_request imports, `stream()` query
  extractor, `get_multi_variant` `cohort_query`, `channel_playlist` query
  suffix, `session_middleware` tail, `maybe_composed_playlist`).
  Workspace `Cargo.toml` `url = "2.5"` was already in `stack/base`
  (confirmed present at 17569a5), so C3 correctly adds nothing there.

### Wholesale files: byte-identity to TARGET

`git diff --quiet 17569a5 eb5d808 -- <file>`:

| File | result |
|---|---|
| `crates/ersatztv-channel/src/config.rs` | **IDENTICAL** |
| `crates/ersatztv-channel/src/playout_loader.rs` | **IDENTICAL** |
| `crates/ersatztv-channel/src/main.rs` | **IDENTICAL** |
| `crates/ersatztv/src/main.rs` | **IDENTICAL** |
| `crates/ersatztv-channel/Cargo.toml` | **IDENTICAL** |
| `Cargo.lock` | **IDENTICAL** |
| `Cargo.toml` (workspace) | **IDENTICAL** |
| `crates/ersatztv-channel/src/lib.rs` | differs by 1 line (`pub mod slate;`, C4) |

All five files the brief names as wholesale are confirmed byte-identical.

---

## Findings by severity

### MEDIUM

**F-C3-2 — the CS-2 residual on `input_timing_at` was not landed.**
Build sheet CS-2: *"input_timing_at #187 doc + live-guard comment … residual-to-target
lines go to C3 (the hazard is variant-borne)"*, and the C3 layer plan lists
"CS-2 residual comments". The commit lands the test the doc refers to
(`a_live_source_never_seeks_however_far_the_session_has_progressed`) but not
the doc. Residual left in `channel_session.rs` at TARGET line 1376ff:

- the 12-line doc block ("The timing decision, split out from the session …
  It holds because of this branch, not because of the data, and there is now a
  test on it.")
- the `live_now` / `remaining` restructure inside the live branch (7 ins) and
  the collapsed `out_point` line (1 ins / 3 del)
- the move of `effective_now` below the clamp plus the 7-line dedup comment
  ("the live guard used to be repeated here, upstream's copy sitting below the
  fork's …") — 14 ins

34 insertions / 9 deletions, behaviourally inert (both forms compute the same
`out_point`). Consequence: no layer above C3 rewrites `input_timing_at`, so
these lines currently have no owner and will fall to the C6 order-alignment
pass. The restructure half is arguably a `stack/base` deviation (map 01 gives
the `live_now` rework to P:drift, and base carries the older `effective_now`
form), but CS-2 explicitly parks the residual on C3.

**F-C3-3 — `expand_stream_variables_url`'s doc sentence was not updated.**
Map 01 UNASSIGNED, explicit: *"C3 then changes only the third argument to
`&self.query_parameters` (line 1299) **and the doc sentence about variant
sessions** (1293-1294)."* C3 changed the argument and left the sentence:

```
/// source URL. The channel session supplies no caller query values, so
/// every `query:` variable resolves to its default.
```

TARGET:

```
/// source URL. Query values arrive only in variant sessions; the shared
/// channel session resolves every `query:` variable to its default.
```

The stale sentence is **false at C3**: `with_query_parameters` exists in this
same commit and variant sessions do supply caller query values. 2 lines.

### LOW

**F-C3-1 — `is_stale` (bool) instead of `staleness` (`Option<Duration>`).**
See §1. Ten added lines not verbatim in TARGET; forces C6 to rewrite a whole
function instead of one call site. Defensible as a C3-form adaptation but not
on the sanctioned list.

### INFORMATIONAL

- **I-1 — function ordering drift (F5), not C3-caused.** At C3
  `prep_output_folder` precedes `publish_recognized_params`; TARGET is the
  reverse (C1 artefact). Likewise `build_output_settings` / `plan_timings` /
  `stamp_error_ms` ordering (P:drift/base artefact). C3 inserted `run_variant`
  and `spawn_variant_loop` in the correct target-relative position. These are
  the F5 order-alignment leftovers and inflate the residual's deletion count.
- **I-2 — `crates/ffpipeline/src/filter_chain.rs` residual has no mapped
  owner.** 9 ins / 6 del: a doc rewording on the `tpad` hardware-chain test
  ("Every templated, variant and slate item sets `pad_to_duration` …") plus two
  inline comments. Map 03's ffpipeline section covers `input.rs` and
  `pipeline.rs` only. On topic it reads C4 (it names slate). Not C3's, but it
  is a gap in map 03 that the C4 verifier should be handed.
- **I-3 — prose forward references to slate in variant_manager.** Three, all
  verbatim in TARGET, all on `pipeline.fallback` (C1). Not leakage; recorded so
  a later audit does not "fix" them into C4.

---

## Residual to TARGET — the explicit C4-C6 budget

`git diff 17569a5 eb5d808 --stat`:

```
 .github/workflows/fork-docker.yml               |   57 +
 crates/ersatztv-channel/src/channel_session.rs  | 1260 ++++++++++++++++-----
 crates/ersatztv-channel/src/composer.rs         |  112 +-
 crates/ersatztv-channel/src/lib.rs              |    1 +
 crates/ersatztv-channel/src/playlist_manager.rs |  321 ++++--
 crates/ersatztv-channel/src/slate.rs            |  150 +++
 crates/ersatztv-channel/src/variant_manager.rs  | 1020 +++++++++++++++++-
 crates/ersatztv-playout/src/playout.rs          |  117 +++
 crates/ffpipeline/src/filter_chain.rs           |   15 +-
 crates/ffpipeline/src/input.rs                  |   10 +
 crates/ffpipeline/src/pipeline.rs               |  407 ++++++--
 crates/ffpipeline/tests/common/mod.rs           |    2 +
 schema/playout.json                             |   11 +
 tools/timeline-bench/README.md                  |   55 +
 tools/timeline-bench/analyze_arm.py             |   61 ++
 tools/timeline-bench/gen_content.sh             |   24 +
 tools/timeline-bench/run_arm.sh                 |   31 +
 tools/twins.py                                  |  239 +++++
 18 files changed, 3380 insertions(+), 513 deletions(-)
```

Per-file attribution (`--numstat`, ins/del):

| File | ins/del | Owed to |
|---|---|---|
| `channel_session.rs` | 1004/256 | C4 (slate block, signature+plan ripple, ~660), C5 (1: `slate_file`), C6 (~47 + the F4 ffmpeg-spawn naming), **C3 arrears 36 (F-C3-2, F-C3-3)**, plus F5 ordering churn |
| `variant_manager.rs` | 991/29 | C5 (default policy + ~13 tests), C6 (served-window audit, reach gauge + 4 tests, drop reasons, torn guard + 2 tests, liveness + 2 tests), F-C3-1 rewrite (10) |
| `playlist_manager.rs` | 217/104 | C6 (map 02: 114 ins) + F5 test-module reordering (the 104 deletions are almost entirely test reflow, not new content) |
| `composer.rs` | 104/8 | C6 (`served_window` + join arithmetic) |
| `slate.rs` | 150/0 | C4 (whole file) |
| `lib.rs` (channel) | 1/0 | C4 (`pub mod slate;`) |
| `playout.rs` | 117/0 | C4 (slate field + tests) |
| `schema/playout.json` | 11/0 | C4 |
| `ffpipeline/input.rs` | 10/0 | C4 |
| `ffpipeline/pipeline.rs` | 297/110 | C4 (`loop_when_exhausted` + tests) |
| `ffpipeline/tests/common/mod.rs` | 2/0 | C4 |
| `ffpipeline/filter_chain.rs` | 9/6 | **unmapped** (I-2; reads C4) |
| `.github/workflows/fork-docker.yml` | 57/0 | fork-ci (merged last) |
| `tools/twins.py`, `tools/timeline-bench/*` | 410/0 | fork-tools / bench (merged last) |

Nothing in the residual belongs to C1, C2, or a P-branch except the F5
ordering churn and I-2, both already on record.

## Recommendation

C3 is sound to build on: it compiles, lints, formats and tests clean on its
own, leaks nothing from C4/C5/C6, and its code content is verbatim TARGET.
Before the C6 gate, amend C3 (or hand to the alignment pass) the 36 lines of
F-C3-2 + F-C3-3, and record F-C3-1 so the C6 verifier expects a `staleness`
rewrite rather than a novel function.
