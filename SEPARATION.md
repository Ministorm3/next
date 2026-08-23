# SEPARATION.md — splitting the mega branch into topic branches

Status: DRAFT for Phase 2 review. Built 2026-08-18 from the four Phase 1 audit
reports (see `reports pointer` at the bottom). Nothing here has been executed.

## Targets and invariants

- Base for every branch: `main` = 4eec042 (upstream tip).
- Split source: mega tip 7b6ae2b (`claude/branch-separation-stacking-8e4572`
  == `feat/per-cohort-stream-variants`). 104 commits, of which 78 are
  net-visible, 20 cancelled (10 pairs), 4 merges, 2 docs-only.
- Split by NET DIFF, not history replay. Rebuilt-stack landmarks like the
  9a0197c file move and the ce278c6 rewrite are archaeology; the code is
  written directly in its final home.
- Losslessness gate: merging every branch below (in stack order) must diff
  EXACTLY EMPTY against the recomposition target.
- Shippability gate: merging only the UPSTREAM-CANDIDATE branches must build,
  pass clippy, and pass tests on its own.

## Pre-split reconciliation — DONE 2026-08-19

Landed as five commits, adversarially verified by a three-lens panel and
rewritten once to fix its findings (misplaced Cargo.lock hunk, a rustfmt
violation, message coverage). Series: 65bf053 schema dup key, ae391bb dead
deps + lock + visibility, 764ac9c stamp error fix, 9fbe339 gap log level,
eb5d808 adopted tests. Every commit builds and lints alone under --locked.

**RECOMPOSITION TARGET: 1add35e** (was eb5d808, before that 7b6ae2b; moved 2026-08-23 by the F1 test-adoption commit). The losslessness gate
diffs against this tip. Pre-rewrite series preserved at
backup/pre-reconcile-rewrite until Phase 5.

Name-collision policy for Phase 3: rebuilt branches whose intended name is
taken by a diverged fresh branch are built as `rebuilt/<name>`; names swap
at Phase 5 after the archive decision (D6).

The mega tree and the fresh branches must converge BEFORE the split, or every
topic exists in two permanently drifting variants. Two tiers of commits on
this branch first; the new tip becomes the recomposition target.

Tier 1 — mechanical cleanup (no behavior change):
1. `schema/playout.json`: duplicate `is_live` key in HttpSource properties
   (bad merge; found independently by two auditors; not present on
   `feat/stream-variables`).
2. Unused deps: `time` + `url` on `crates/ersatztv/Cargo.toml`,
   `ersatztv-channel = { path = .. }` in the workspace `Cargo.toml`.
3. `crates/ersatztv/src/channel_session.rs::channel_binary_path`: revert dead
   `pub` to private.

Tier 2 — adoptions from fresh branches that are AHEAD of the mega tree
(behavior-affecting, each its own commit):
4. THE MATERIAL ONE (fidelity report): `fix/timeline-drift-pad-and-trim`
   computes the stamp error as `last_segment_end + start_time_offset -
   transcoded_until` via a named `stamp_error_ms`; the mega tree computes it
   inline WITHOUT `start_time_offset`. On any channel with `virtual_start`
   set, deployed code reads the whole virtual-start offset as drift and claws
   it back at the 500 ms clamp on every item. This is the known S-to-E rule /
   #212 virtual_start defect class. Adopt the branch's computation plus its
   four pinning tests.
5. Black-air schedule-gap line: adopt the branch's `log::debug!` level and
   restore the "filling a schedule gap with black is normal" comment (mega
   logs the normal case at error).
6. Test/prose enrichments that exist only on fresh branches: the 97-line
   cache-header test module, the branch's 13-line RFC comment, the two
   stream-variables encoding tests. Adopt so rebuilt branches can carry them
   without breaking the losslessness gate.

NOT adopted: `fix/segment-trim-served-head`'s implementation (mega's
`served_head: Option` + per-manager `history` is the evolved version that C6
builds on; the fresh branch gets superseded by a rebuilt one).

## Branch inventory

### Prerequisite branches (independent; all based on main; order P1 first)

| # | branch | source commits (mega) | notes |
|---|--------|----------------------|-------|
| P1 | `fix/io-error-naming` | 516b477 (minus composer/variant_manager hunks, which are written into C2/C3) | IoContext sweep: error.rs (all but JsonError), dossier.rs, local_proxy.rs, pts_scanner.rs, config.rs (part), playout_loader.rs (part), playlist_manager io hunks. Touches everything; goes first. |
| P2 | `fix/hls-playlist-conformance` | 3adfbe0, 0ab4bae, 73db24e (+af7021b context, already on main) | EXT-X-VERSION:6, unconditional DISCONTINUITY-SEQUENCE, video/MP2T + .m3u recognition, Cache-Control no-cache, X-TIMESTAMP-MAP. |
| P3 | `fix/webvtt-cue-timing` | 37025d1, 2cab6ea | ffpipeline/web_vtt.rs parser fixes (settings lists, tab separators, arrow-in-payload). Strongest upstream candidate; kept separate from P2 for that reason. |
| P4 | `fix/idle-and-liveness` | 6a5649a, 6ebe5c1 | heartbeat-absent expiry + idle reap clean exit. |
| P5 | `fix/output-folder-lock` | 86da583 | core/folder_lock.rs + channel_session::new + libc dep. NOT part of cohort core types. |
| P6 | `fix/kill-child-processes` | 101c17e (minus variant_manager hunk, written into C3) | kill_on_drop at both spawn sites. |
| P7 | `fix/watermark-cosmetics` | 219548a + the 24539df residue (watermark_input_args helper + pin-absence assertions) | Strip streaming options from watermark sources. The image2 pin itself is NOT in the mega diff (reverted out); it lives standalone on `fix/still-image-watermark-demuxer`. Decision D3 below. |
| P8 | `fix/out-point-slot-clamp` | a65e831 | exists fresh; effective_out_point_ms + input_timing_at. |
| P9 | `fix/black-air-log-census` | part of 516b477 lineage | exists fresh, but DIVERGES from mega tip (branch logs at debug, mega at error). Decision D4 below. |
| P10 | `fix/segment-trim-served-head` | c835f88, f3c2c01, 7402041-residue | exists fresh (c33886a). served_head, trim_cutoff, `now` test seam. C6 builds on it. |
| P11 | `fix/timeline-drift-pad-and-trim` | 8fa0c30, d9e6935, e5a88bf, a004503 (+bac1301 revert residue) | exists fresh (5c422db, f2f6b68). The OutputSettingsPlan/TimingPlan/PlannedTimings seam; C1 and C4 consume it, so it sits below the stack. bench/ variant adds the tool. |
| P12 | `feat/stream-variables` | 2df5da1 lineage | exists fresh (de609c9). HARD stack prerequisite of C1 (query_variable_names -> recognized-params). expand_url escaping/origin pin verified present. |
| P13 | `fix/xmltv-number-fallback` | b0b75c4 | xmltv `<number>.xml` fallback. |

### Cohort stack (each layer based on the previous; full hunk assignment in report C section 2/4)

| # | branch | contents (summary) |
|---|--------|--------------------|
| C1 | `cohort/01-identity-and-sidecar` | core cohort.rs, variant_request.rs, sidecar.rs, RECOGNIZED_PARAMS half of core lib.rs, core Cargo additions; playout query_variable_names; channel_session recognized-params publication + is_templated; playlist_manager sidecar production (Segment.item_id, before_new_pipeline growth, generate_sidecar); error.rs JsonError. |
| C2 | `cohort/02-composer` | composer.rs (minus served_window + join arithmetic, which are C6), lib.rs export. Imports only core sidecar. Cleanest cut in the feature. |
| C3 | `cohort/03-sessions-and-serving` | variant_manager.rs minus default-policy region; channel_session variant loop/run_variant (minus stopgap + claim logging); server main.rs cohort routes; config merged_source; Commands::Variant; get_item_by_id. |
| C4 | `cohort/04-slate-on-shared` | PlayoutItem.slate + schema; ffpipeline loop_when_exhausted; slate.rs whole file (default key inert until C5); channel_session slate machinery; before_new_pipeline fallback->slate. Depends only on C1+P11/P12; may be reordered right after C1. |
| C5 | `cohort/05-slate-default-admission` | e17e5da's variant_manager half: VariantChannel.slate_file, DefaultPolicy, resolve_default_policy, log_policy_change, default_cohort plumbing, 15 tests. Must follow C3 AND C4. |
| C6 | `cohort/06-retention-and-observability` | VARIANT_HISTORY_DURATION stopgap + set_history_duration + extended-trim warn + compile-time assert vs composer; run_variant claim logging (51f80de); served-window audit (7b6ae2b) + reach gauge; join arithmetic (f24da71); late-join wording (1e1b442); drop/reap reasons (9114260); torn-request guard (b0ffb17); cohort-request liveness (f175fc5). Must be last. |

### Fork-only branches

| branch | contents |
|--------|----------|
| `fork/docker-ci` | .github/workflows/fork-docker.yml (3e6d00d, c81eff8) |
| `fork/tools` | tools/twins.py (cb9efb2); timeline-bench lives on bench/timeline-drift-pad-and-trim already |

### Standalone fresh branches (work NOT in the mega diff; keep as-is, audit only)

- `fix/still-image-watermark-demuxer` (223a93b) — the image2 pin, different
  implementation from the reverted mega one; if it ever lands, the P7
  pin-absence assertions must invert.
- `feat/report-schedule-drift`, `docs/clock-domains`,
  `feat/channel-number-variable` (minimal subset of 2df5da1, DIFFERENT syntax
  from P12's `{query:}` design — the two designs cannot both be proposed).

## Buckets (from report D)

- UPSTREAM-CANDIDATE: P1-P8, P10, P11, P13, and C-stack hardening fixes that
  fix general bugs. Padding group (P11) travels as one unit; strip the
  is_templated exemption before proposing.
- FORK-ONLY: fork/docker-ci, fork/tools, the C6 stopgap + assert + audit +
  reach gauge.
- NOT-READY: the cohort feature itself (blockers: composer.rs:2631-2644
  empty-composed-playlist defect; stands on the stopgap), stream variables
  `{query:}` half (blockers: dead exports; meaningless without cohort),
  timeline-bench (analyzer reads the fork sidecar).
- UNSURE (user decides someday, not blocking): sidecar.rs as an upstreamable
  provenance manifest; twins.py; loop_when_exhausted as a standalone
  capability; spawn_playlist_publisher refactor vs just its log lines;
  report-schedule-drift appetite.

## Comment preservation contract

Nine marker comments MUST survive the split verbatim (locations + full quotes
in report D): the four STOPGAP sites, composer.rs:2631-2644 defect statement,
channel_session.rs:1443-1449 absent-guard rationale, variant_manager.rs:150-155
provisional admission home, filter_chain.rs:1174-1185 tpad rebuttal,
composer.rs:2806-2814 non-defect audit note.
Two comments must be DELETED from any future upstream slice (but kept on fork
branches): pipeline.rs:553-555 and pipeline.rs:991-1005 (fork-vs-upstream
relationship notes). variant_request.rs:66-69/:105-109 incident forensics
should be generalised if ever proposed.

## Cancelled pairs (net to zero; build nothing from these)

8fa0c30/bac1301 (redone as d9e6935), 0ee01cc/b93a09e, eb7ae7f/54275c8,
a51816a/94e8539, e7c31ae/f44e2fb, 6c26d16/24539df (residue to P7),
c7ddd84/7ca7f6a, a3734ed-clamp/dc6d17b, 7402041/8d40c88-merge (residue to
P10), 2cc0c5f (nets to zero inside composer.rs). Revert residues that DO
survive (tests, seams) are itemised in report B and assigned above.

## Branch fidelity summary (report A)

No fresh branch is an EXACT extraction; every one differs from the mega tree
at least in prose or tests. Classification:
- Material logic divergence: fix/timeline-drift-pad-and-trim (stamp error,
  see Tier 2 item 4 — the branch is AHEAD of deployed).
- Behavior-visible divergence: fix/black-air-log-census (debug vs error).
- Different implementation, mega version wins: fix/segment-trim-served-head.
- DELIBERATE upstream variant, not superseded (corrected 2026-08-19 from
  the contributions memory): feat/channel-number-variable backs OPEN
  upstream PR #216, and its `{{channel:number}}` syntax was a user-driven
  decision for upstream (single braces rejected there). The mega's
  `{channel_number}` single-brace form is the fork-side design. Keep BOTH;
  never archive a branch backing an open PR.
- Same rule protects: fix/timeline-drift-pad-and-trim (PR #212),
  fix/out-point-slot-clamp (PR #214), fix/black-air-log-census (PR #215).
  Their divergences from the mega tree are partly deliberate upstream-facing
  choices, not only drift. D6 archive candidates are ONLY the stale
  25-behind generation and branches backing no PR.
- Fully standalone: docs/clock-domains (0 of 517 added lines in mega),
  feat/report-schedule-drift, fix/still-image-watermark-demuxer.
- Prose/test/adaptation-only: everything else.

Consequence: every topic branch that overlaps the mega diff gets REBUILT from
the (reconciled) mega tree in Phase 3. Existing fresh branches are inputs and
cross-checks, not survivors; the divergent ones move to archive/ alongside
the stale generation (except the standalone three, which stay).

Cross-branch stacking collisions to handle during rebuild (report A sec. 4):
three branches each append a module literally named `mod tests` to
channel_session.rs; two add `last_segment_end()` with different docs; two
create stream_variables.rs incompatibly. Rebuilt branches must share one
tests-module layout and one canonical helper per name so the stack merges
cleanly.

## Open decisions for Phase 2 review

- D1: Tier 1 mechanical cleanup commits on this branch — approve?
- D2: cohort branch naming `cohort/NN-name` — or keep feat/fix prefixes?
- D3: P7 keeps the watermark_input_args residue + pin-absence assertions
  (faithful to mega tree) — approve?
- D4: Tier 2 adoptions, especially the stamp_error_ms fix (item 4): this
  changes deployed-lineage behavior (fixes the virtual_start-read-as-drift
  defect). Approve adopting on the mega branch before the split?
- D5: C4 position: keep at 04 (after C3) or move right after C1? Mapper says
  either compiles; earlier = smaller top of stack, later = matches history.
- D6: stale earlier-generation branches AND superseded/diverged fresh
  branches -> rename to `archive/` in Phase 5.
- D7: the mega branch is the fork CI trigger (fork-docker.yml builds on push
  to feat/per-cohort-stream-variants). Reconciliation commits stay LOCAL
  until you explicitly say to push that branch; topic-branch pushes are safe.

## Phase 3 track 1 — DONE 2026-08-19: prerequisite branches built

All 13 prerequisite branches exist, each gated on build + fmt + clippy
--locked + full tests in its own worktree (xmltv's gates re-run by hand
after its builder crashed post-commit; feat/stream-variables audited in
place). Builder deviation reports: reports/E-builder-reports-round1.md and
F-builder-reports-round2.md in the session scratchpad.

| branch | sha | note |
|---|---|---|
| fix/io-error-naming | 93c77f6 | io hunks only; JsonError/C-stack omissions listed in report; DRAFT PR upstream: ErsatzTV/next#217 (2026-08-19); publish-loop dedup SPLIT OUT 2026-08-19 (amended from 8fde105) |
| fix/publish-loop-failure-logging | 08416f9 | split from P1: publish-loop failure dedup logging; body drafted in .claude/pr-drafts/, NOT opened; recomposition uses P1+this together |
| fix/hls-playlist-conformance | 17bb0bf | includes reconciled cache tests |
| fix/webvtt-cue-timing | c6e8409 | byte-identical to target file |
| fix/idle-and-liveness | 9eaece6 | |
| fix/output-folder-lock | f63003c | |
| fix/kill-child-processes | 481f47c | |
| fix/watermark-cosmetics | 9516886 | carries image2-absence residue (D3) |
| rebuilt/out-point-slot-clamp | ad56b05 | |
| rebuilt/black-air-log-census | ce813f5 | |
| rebuilt/segment-trim-served-head | 840f000 | trim vs plain HISTORY_DURATION constant (pre-C6 form), as sanctioned |
| rebuilt/timeline-drift-pad-and-trim | 5da691a (3 commits) | 2-arg emission_trim_ms; cohort members omitted |
| feat/stream-variables | de609c9 | USE-AS-IS; one sanctioned adaptation (empty query map until C3) |
| fix/xmltv-number-fallback | 97a3794 | |

Known stacking conflicts recorded by builders (expected, textual, resolve
at recomposition): the shared #[cfg(test)] mod tests tails in
playlist_manager.rs and channel_session.rs (several topics append to the
same position); cosmetic_source exists on BOTH fix/watermark-cosmetics and
the standalone fix/still-image-watermark-demuxer (the older standalone
branch carries its own copy plus the pin; if both ever land, one side
yields).

## Phase 3 track 2 + Phase 4 gates — DONE 2026-08-21

The cohort stack is BUILT: stack/base (main 4eec042 + all 14 P-branches,
206 tests) then cohort/01-identity-and-sidecar (122a6c2, 229),
cohort/02-composer (e02c0e0, 285), cohort/03-sessions-and-serving
(17569a5, 311), cohort/04-slate-on-shared (2961637, 335),
cohort/05-slate-default-admission (58e3c38, 348),
cohort/06-retention-and-observability (4037081 incl. alignment, 359).
fork/docker-ci + fork/tools built. All pushed to the fork, plus
stack/recomposed (C6 + both fork merges).

FINAL RECOMPOSITION GATE: stack/recomposed vs eb5d808 = EXACTLY 57 lines,
the three drift-branch-only tests at the channel_session module tail.
Nothing else differs. That delta IS decision F1 (adopt the tests into the
mega lineage, or drop them from the stack and the drift branch).
SHIPPABILITY GATE: base minus feat/stream-variables builds, clippy-clean,
170 tests green.
Verification: ALL SIX LAYER PANELS PASS (C1, C3, and the C2/C4/C5/C6
panel; reports 05/06/07 in separation-map/). Stack-wide invariant
confirmed at stack/recomposed: the 57-line three-test delta and nothing
else. Attribution caveats for future per-layer PRs are in the build
sheet; shas left stable.

## Execution order (Phase 3)

1. Pre-split cleanup commits (after D1 approval); retag recomposition target.
2. Prerequisite branches P1-P13: parallel agents, worktree isolation, each
   gated on cargo build + clippy + unit tests. P8/P9/P10/P11/P12 already
   exist fresh; they get fidelity-audit reconciliation rather than rebuilds.
3. Cohort stack C1-C6: sequential, in this session, compile gate per layer.
4. Phase 4 gates: losslessness recomposition, upstream-candidate-only build,
   per-branch leakage verification fan-out.
5. Phase 5: archive stale branches, push to fork, commit this manifest.

## Reports pointer

Full evidence in the session scratchpad `sep/reports/`:
A-branch-fidelity.md, B-commit-classification.md, C-cohort-dependency-map.md,
D-fork-only-triage.md, plus phase0-summary.md and diffs/. Copy into the repo
or fork wiki in Phase 5 if wanted.
