# Verification panel: C2, C4, C5, C6 (+ alignment), stack-wide invariant

Run 2026-08-21, read-only, in the stack worktree. Target eb5d808.
Method: residual-to-target diffs per layer, a cross-layer churn matrix (which
lines a later layer genuinely removes from an earlier layer's additions), a
move-purity set analysis on the reordering commits, and symbol-availability
spot checks in place of a build (no checkouts permitted).

## Verdicts

| Commit | Faithfulness | Leakage | Verdict |
|---|---|---|---|
| C2 e02c0e0 composer | PASS | PASS | **PASS** |
| C4 2961637 slate-on-shared | PASS | PASS | **PASS** |
| C5 58e3c38 slate default admission | PASS | PASS | **PASS** |
| C6 867345c retention + observability | PASS | n/a (last) | **PASS** |
| 4037081 alignment | PASS on content | n/a | **PASS with finding M2** |
| Stack-wide invariant | — | — | **PASS at 05b2c92, not at 4037081 (M1)** |
| fork/docker-ci, fork/tools | PASS | PASS | **PASS** |

## Stack-wide invariant

`git diff 05b2c92 eb5d808` (stack/recomposed = C6 + both fork merges) is
exactly one file, one hunk: 57 deletions in
`crates/ersatztv-channel/src/channel_session.rs`, the tail of `mod tests`,
containing exactly `pacing_follows_the_caller`,
`a_realtime_item_fills_its_slot_in_one_pipeline`, and
`work_ahead_chunks_a_long_item`. No comment drift, no ordering drift, no
missing file, no mode change. The F1 residual, confirmed.

`git diff 4037081 eb5d808` is NOT that. See finding M1.

Disjointness proof for channel_session, by numstat: C4 to target is 331/301,
which decomposes exactly as C5 (1/0) + C6 (57/3) + alignment (273/241) +
target (0/57). Insertions 1+57+273 = 331, deletions 0+3+241+57 = 301. The
four later hunks therefore do not overlap; no region is written twice.

## Cross-layer churn matrix

Every line a later layer genuinely removes (present in its deletions, absent
from its additions, length > 12) from an earlier layer's additions:

- C4 removes 8 C1 lines and 2 C3 lines: `plan_for(&item, true, ...)` call
  forms and `transcode_item(&item, true, false, ...)`. This is F6 (drop
  `plan_for`'s `start_at_zero`/`realtime`, rewrite the call sites) plus CS-6.
  Sanctioned.
- C5 removes 2 C3 lines: `read_requests(channel, &recognized)` and
  `cohort_query: cohort::to_query_string(&parameters)`. The `default_cohort`
  plumbing. Sanctioned.
- C6 removes 8 C2 lines (the inline late-join `if upgraded` wording and the
  two private `fn parse_pdt`/`fn format_pdt` declarations) and 23 C3 lines
  (`is_stale` becoming `staleness`, the bare drop/reap strings, the composer
  import line). All C6-topical. Sanctioned.
- C6 removes 2 C5 lines: the `read_requests` signature again. Sanctioned.
- The alignment commit removes one line three times over
  (`let item = templated_item();`). See M2.

No unexplained churn anywhere in the stack.

## C2 e02c0e0

Touches `composer.rs` (new, 3691 lines) and one `lib.rs` line. Residual to
target in composer.rs is 112 lines in six hunks and nothing else:
`served_window` + its test, the join-arithmetic block (`walked`,
`last_emitted`, the `held` report), the late-join `how` wording, and the
`pub(crate)` bump on `parse_pdt`/`format_pdt`. All four land in C6.

Leakage: grep of C2's composer.rs for `served_window`, `join arithmetic`,
`walked`, `last_emitted`, `deepest_variant_reach`, `audit_served` returns
nothing. Clean.

Self-containment holds: C2's only non-std import is
`ersatztv_core::sidecar::{PlaylistSidecar, SidecarPipeline}`, which exists at
C1. No `crate::` reference outside `use super::*` in tests. The commit message
states the C6 deferral explicitly.

Forward dependency satisfied: `SEGMENT_SECONDS`, `SERVED_SEGMENTS` and
`HARD_LAG_SEGMENTS` are already `pub` at C2, so C6's `const _: () = assert!`
in playlist_manager compiles against this layer without amending it.

## C4 2961637

Wholesale convergence confirmed and complete: `git diff 2961637 eb5d808` over
ffpipeline, ersatztv-playout, schema, slate.rs, config.rs, both main.rs,
playout_loader.rs, ersatztv-core, Cargo.lock, Cargo.toml, error.rs and lib.rs
is **empty**. Every file C1-C4 own outside the four cohort files is at target
by the end of C4.

Leakage down: no `DefaultPolicy`, `resolve_default_policy`,
`log_policy_change`, `default_cohort`, `slate_file` or `crate::slate` in C4's
variant_manager.rs. Clean.

Leakage up: no `set_history_duration`, `VARIANT_HISTORY`,
`deepest_variant_reach`, `audit_served_window` or `served_window` in C4's
channel_session.rs. Its added `log::` lines are all slate-topical (trim points
ignored, slate file unreadable). Clean.

## C5 58e3c38

The tightest layer in the stack. Two files, +540/-2. Additions are
`enum DefaultPolicy`, `resolve_default_policy`, `log_policy_change`, the
`VariantChannel.slate_file` field and its plumbing, `use crate::slate::{SlateFile,
read_slate_file}`, 13 tests with 2 helpers, and the single
`slate_file: slate::slate_file(...)` line at the VariantChannel construction
in channel_session.rs. The 2 deletions are the `read_requests` signature.

Leakage: `audit_served_window`, `deepest_variant_reach`, `served_window`,
`reach_ms` are all absent. (`reap` and `liveness` do appear in the file, but
as C3 text this layer does not touch.) Clean.

Dependency satisfied: `slate::slate_file`, `SlateFile`, `read_slate_file` and
`SLATE_FILE_NAME` all exist in C4's slate.rs.

## C6 867345c

playlist_manager.rs, the file that looks worst on stat, is the cleanest on
inspection: 217 added / 104 deleted lines reduce to **114 net-new lines** (the
build sheet's number exactly) plus **one** replaced line
(`served - HISTORY_DURATION` becoming `served - self.history`). Everything else
is a pure move. The 114 are `VARIANT_HISTORY_DURATION`, the cross-module
`const _: () = assert!` (PM-D2, correctly here), the `history` and
`extended_trim_warned` fields, `set_history_duration`, the extended-trim
`log::warn!`, and 2 tests.

variant_manager.rs adds `audit_served_window`, `deepest_variant_reach_ms`,
`staleness`, drop/reap reasons, the torn-request guard, cohort liveness, and 8
tests. composer.rs gets the served window and join arithmetic C2 deferred.
channel_session.rs gets the retention stopgap call, the claim/claim-lag
logging, and F4 (the ffmpeg spawn error naming the binary) — all four
disclosed in the commit message.

## Fork branches

Both sit directly on 4eec042 (an ancestor of eb5d808), one commit each.

- `fork/docker-ci` f8d7801: `.github/workflows/fork-docker.yml` only, 57 lines.
  `git diff f8d7801 eb5d808 -- .github/` is empty, so the other four workflow
  files match target too.
- `fork/tools` f47f717: the 5 tools files, 410 lines, modes preserved (both
  shell scripts 100755). `git diff f47f717 eb5d808 -- tools/` is empty.

Neither merge into stack/recomposed introduces anything beyond its own files.

## Findings by severity

**M1 (medium) — the invariant's stated anchor is wrong by two merges.**
`git diff 4037081 eb5d808` reports 7 files, 467 insertions / 57 deletions: the
three tests *plus* all six fork-branch files, which 4037081 does not carry.
The invariant holds only at `stack/recomposed` (05b2c92). The build sheet
states this correctly ("stack/recomposed = C6 + both fork merges. FINAL GATE");
the gate should be run against 05b2c92, never 4037081, or it reports a
467-line false failure.

**M2 (medium) — 4037081 is not purely moves and comment arrears.**
Set analysis of its 273/241 lines finds two test-body edits that are neither:

1. In `the_trim_reaches_every_stream_the_t_reads`, `assert_eq!(planned.video.finish,
   item.finish);` is replaced by `assert_eq!(planned.declared_duration_ms, 10_994);`.
   A real assertion change, and `declared_duration_ms` is a C1 member, so it is
   C1 arrears being backfilled by a chore commit.
2. In `a_templated_plan_ignores_the_stamp_error`, `templated_item()` becomes
   `templated_item_with_slate(None)`. Inert — target defines
   `fn templated_item() { templated_item_with_slate(None) }` — but it is a C4
   arrear, not a comment.

The tree is target-faithful either way; the commit message's "Pure
convergence, no behavior" is true of production code and overstated for the
test module. Everything else in the commit checks out: the `effective_now` to
`live_now` restructure in `input_timing_at` is semantically identical (the
same expression, moved inside a branch that returns), and
`stamp_error_ms`/`emission_trim_ms`/`apply_emission_trim` move intact (76
lines deleted at one hunk, 76 added at another).

**L1 (low) — C6's playlist_manager reorder is undisclosed.** ~200 of its 321
changed lines are the F5 base-caused test reordering (io tests to the module
tail, subtitle helpers hoisted). Verified pure move. The commit message
mentions none of it.

**L2 (low) — C4's ffpipeline convergence is understated in its message.** The
message says it "converges the ffpipeline test-module import grouping"; the
407-line pipeline.rs hunk actually replaces the whole shared test scaffold
(`file_probe`/`file_pipeline_args` giving way to the `slate_probe` family, per
PM-D4) and moves the watermark tests. Verified harmless: the watermark tests
(`still_image_watermarks_follow_the_probed_demuxer`,
`animated_watermarks_loop_without_reopening_the_input`,
`device_name_returns_correct_ffmpeg_device_strings`) exist at stack/base and
are only relocated. Same class as the arrears note already in the build sheet:
if these layers ever ship as PRs, this hunk needs splitting too.

**L3 (low) — the C2 sanctioned-deviation list is short by one item.** C2 also
keeps `parse_pdt`/`format_pdt` private where target has them `pub(crate)`. The
choice is sound and correctly owned: C6 bumps the visibility in the same
commit that adds the `use crate::composer::{..., parse_pdt}` in
variant_manager, so no layer carries an unused `pub(crate)`.

**I1 (info) — C4's slate.rs carries a C5 forward reference.** `SlateConfig.default`,
its two parse tests, and the module doc line "the variant manager reads
`default` once per tick" all land at C4 with no consumer until C5. Sanctioned
by the seed doc ("default key inert until C5") and by whole-file ownership,
but a standalone-PR reviewer of C4 would ask about it.

**I2 (info) — C5 test count.** The build sheet says 15 tests; the commit adds
13 test functions plus 2 helpers (`write_slate`, `answer_for`). Count only.

**I3 (info) — one of the three F1 tests is already covered at target.**
`pacing_follows_the_caller` asserts `output_settings(true, false, false, false).realtime`
and `!output_settings(false, false, false, false).realtime`; target's
`slate_paces_like_every_other_pipeline` asserts both of those and two more. It
is a strict subset. The other two overlap target coverage only partially. Note
also that all three have been adapted through the stack (C4's `plan_for`
signature drop and the `slate`/`is_templated` TimingPlan fields), so an F1
adoption commit must take the 4037081 forms, not the drift branch's originals.

**I4 (info) — not re-verified here.** Compilability and test results at each
layer tip. The worktree is read-only and checkouts were out of scope, so the
verdicts rest on content diffs, the churn matrix, and targeted
symbol-availability checks (C2's pub consts against C6's const assert; C4's
slate.rs against C5's imports; C1's core modules against C2's imports).
