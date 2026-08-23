# Build sheet: cohort stack C1-C6

Written 2026-08-20 after the three-map refresh (01/02/03 in this folder).
Target: eb5d808. Base for the stack: `stack/base` = main (4eec042) + all 14
P-branches merged, conflicts resolved toward target content minus
yet-unstacked C-layer bits.

## Adjudicated disputes (authoritative)

- CS-1 pts-log wording (:747): P:io per map; verify at recomposition, not
  re-litigated now.
- CS-2 input_timing_at #187 doc + live-guard comment: owned by whichever
  built branch carries them; residual-to-target lines go to C3 (the hazard
  is variant-borne).
- CS-3 test a_live_source_never_seeks: C3 introduces at its stacked
  position (drift/C1 fixtures already in base by then).
- CS-4 publish-loop before C3: satisfied structurally by stack/base.
- CS-5 before_new_pipeline last arg: C1 introduces `fallback`, C4 renames
  to `slate`, both files in the same layer commit as their counterpart.
- CS-6 run_variant trailing `false` (:582): C4 edits C3's call when C4
  lands.
- CS-7 loop_when_exhausted ripple fields: C4.
- PM-D1 PUBLISH_LEAD doc line (:20): 1-line amend to
  rebuilt/segment-trim-served-head (not a PR branch; safe). Do during base
  assembly.
- PM-D2 const assert (:44-73): stays C6 (topical ownership: it asserts the
  retention budget relationship; C2 merely makes it compilable).
- PM-D3 io_context calls inside the sidecar block (:438-453): C1 lines,
  written by C1, never P:io.
- PM-D4 shared test scaffolding (mod tests, manager_in, segment,
  slate_probe family in ffpipeline): first branch in merge order carries
  it; later conflicts resolve to byte-identical content.
- U1 last_segment_end() accessor: P:drift (its branch diff IS those lines).
- Small-files sweep: no disputes; vudei typo confirmed riding P:io/#217.

## Base assembly (stack/base)

Merge order (conflict-minimizing, io first): fix/io-error-naming,
rebuilt/timeline-drift-pad-and-trim, rebuilt/segment-trim-served-head
(amended +PUBLISH_LEAD doc line), fix/hls-playlist-conformance,
fix/idle-and-liveness, rebuilt/black-air-log-census,
rebuilt/out-point-slot-clamp, fix/watermark-cosmetics,
fix/kill-child-processes, fix/output-folder-lock,
fix/publish-loop-failure-logging, fix/webvtt-cue-timing,
feat/stream-variables, fix/xmltv-number-fallback.

Conflict policy: resolve every conflict to the TARGET (eb5d808) content of
the region MINUS members/arguments/tests the maps assign to C1-C6 (those
arrive with their layer). Known conflict sites: channel_session.rs test
module tail (drift/clamp/black-air), playlist_manager.rs test scaffold
(io/trim/hls/heartbeat) and the multi-topic test
trimming_a_segment_whose_file_is_gone_names_the_segment (io introduces,
trim extends; base carries the trim-extended 2-owner form, C1 adds
"item-a"), ffpipeline pipeline.rs test helpers (watermark/drift carry;
full-parameter form per sweep note).

Base gate: cargo build + clippy --locked + fmt --check + full tests, plus
`git diff stack/base eb5d808 --stat` recorded as the expected C-stack
residual (nothing outside the maps' C-owned regions may appear in it).

## Layer contents (detail: maps 01/02/03; this is the commit plan)

- C1 cohort/01-identity-and-sidecar: core types + Cargo, RECOGNIZED_PARAMS
  lib.rs half, playout query_variable_names, playout_loader
  query_variable_names, channel_session C1 regions (~119 lines incl.
  is_templated threading, declared_duration_ms, 3-arg emission_trim_ms +
  exemption + test third-arg extensions, before_new_pipeline growth with
  `fallback`), playlist_manager C1 regions (153 lines: Segment.item_id,
  pipelines/current_item_id, generate_sidecar, sidecar publish block
  :438-453 with its io_context lines, retain, 2 tests + "item-a"
  extension), error.rs JsonError.
- C2 cohort/02-composer: composer.rs minus served_window + join
  arithmetic (C6), lib.rs pub mod composer.
- C3 cohort/03-sessions-and-serving: variant_manager.rs minus
  default-policy region; channel_session C3 regions (~530 lines; run_variant
  WITHOUT stopgap/claim logging; spawn_playlist_publisher extraction moves
  the deduped loop body); playout_loader get_item_by_id; config
  merged_source/merged_source_json; channel main.rs Commands::Variant;
  server main.rs cohort routes + query plumbing + maybe_composed_playlist +
  middleware hook; expand_stream_variables_url third arg; url+filetime
  deps; CS-2 residual comments; CS-3 test.
- C4 cohort/04-slate-on-shared: playout slate field + schema + tests;
  ffpipeline input.rs (whole), pipeline.rs C4 regions, loop_when_exhausted
  + ripple fields (CS-7); slate.rs; channel_session C4 regions (~660
  lines); fallback->slate rename (CS-5); run_variant trailing false
  (CS-6); test extensions per introduce/extend chains.
- C5 cohort/05-slate-default-admission: variant_manager default-policy
  region + 15 tests; channel_session :622 slate_file line.
- C6 cohort/06-retention-and-observability: playlist_manager C6 regions
  (114 lines incl. const assert, budget fields, warn), channel_session C6
  crumbs (stopgap call, claim logging/lag lines), composer served_window +
  join arithmetic, variant_manager audit/reach + drop/reap reasons +
  torn-request guard + cohort liveness + late-join wording.

Per-layer gate: cargo build + targeted tests, then fmt + clippy at C6;
verification panel per layer (leakage vs maps + faithfulness to target).
Final gates: diff C6 tip vs eb5d808 == EMPTY (after fork/docker-ci and
fork/tools merge, built at the end); upstream-candidate-only build.

## Build-time findings (2026-08-20)

- F1 PENDING USER YES: rebuilt/timeline-drift-pad-and-trim carries three
  tests absent from the target (pacing_follows_the_caller,
  a_realtime_item_fills_its_slot_in_one_pipeline,
  work_ahead_chunks_a_long_item). Proposed: one pure-test adoption commit
  on the mega lineage (tier-2 pattern), moving the recomposition target.
  Until decided, the C6 gate expects exactly these three additions.
- F2: a_live_source_never_seeks sits at module tail at stack/base (parser
  misfile), target order restored by the C-layer that next rewrites the
  module.
- F3: base pinned at 4eec042 although local main moved to f7882aa
  (upstream #218); integration is a separate later step.
- F4 (from C1 verify): the transcode_item ffmpeg-spawn error naming is
  unowned (map said P:io, the io builder excluded it as 99a880a content,
  #217 must not be amended without user permission). ASSIGNED TO C6.
- F5: ordering-alignment items for the final gate: publish_recognized_params
  / a_templated_plan_ignores_the_stamp_error / templated_item positions
  (C1-caused), playlist_manager io-vs-hls test order (base-caused, D4).
  Whichever layer last rewrites each region aligns it; leftovers get an
  order-alignment pass before the C6 gate.
- F6: C4 must also DROP plan_for's start_at_zero/realtime params and
  rewrite five call sites (map 01's DUAL note was incomplete).
- Wording divergence (build_output_settings padding comment) between the
  frozen drift branch and the mega tip: gate-time decision, same family
  as F1.
- C3-verify arrears (F-C3-1/2/3) and the filter_chain gap: all resolved
  materially by C4's wholesale ffpipeline convergence, C6's wholesale
  variant_manager, and the alignment commit on cohort/06; attribution note:
  if layers ever ship as PRs, fold the alignment commit's hunks into their
  topical layers first.

## STACK COMPLETE 2026-08-21

cohort/01..06 built, gated, pushed (tips: C1 122a6c2, C2 e02c0e0, C3
17569a5, C4 2961637, C5 58e3c38, C6 4037081 incl. the alignment commit).
fork/docker-ci + fork/tools built. stack/recomposed = C6 + both fork
merges. FINAL GATE: diff vs eb5d808 = EXACTLY the three drift-branch-only
tests (57 lines, channel_session module tail) = the F1 decision.
Shippability gate: base minus feat/stream-variables builds, clippy-clean,
170 tests green (check/upstream-candidate branch, local only).
Verification: C1 + C3 panels PASS (findings resolved, see arrears note);
C2/C4/C5/C6 panel running.
- C2-C6 panel (report 07): ALL PASS. Attribution caveats kept as record,
  shas left stable: the alignment commit also carries two test-assertion
  arrears (C1's declared_duration_ms assertion, C4's fixture rename), and
  C6/C4 carry undisclosed-but-verified pure test moves; fold into topical
  layers if these ever ship as PRs. C2 also correctly keeps
  parse_pdt/format_pdt private (C6 bumps visibility with the consumer).
- F1 input from the panel: pacing_follows_the_caller is a strict subset of
  target's slate_paces_like_every_other_pipeline (adopting it adds little);
  any adoption must take the 4037081 forms of the three tests, which C4's
  signature changes already adapted.

## F1 RESOLVED 2026-08-23 (user: adopt two, drop one)

- Mega lineage adoption commit 1add35e (tests only): a_realtime_item_fills
  _its_slot_in_one_pipeline + work_ahead_chunks_a_long_item in their
  stacked forms. NEW RECOMPOSITION TARGET: 1add35e (was eb5d808). Local +
  claude/ fork backup only; deployed branch name untouched.
- pacing_follows_the_caller (strict subset of slate_paces) dropped from
  the stack tip (cohort/06 commit 460cd4f) and rewritten out of
  rebuilt/timeline-drift-pad-and-trim (new tip bedab47, force-pushed).
  stack/base still carries it inside the frozen merge history; harmless,
  the tip removal governs.
- ZERO GATE: stack/recomposed (72ed15a) vs 1add35e = 0 diff lines. The
  split now reproduces the deployed lineage EXACTLY.

## UPSTREAM INTEGRATED 2026-08-23 (post-gate, expected)

Merge b94f733 brings the lineage to upstream f7882aa: census converged to
the #215 inlined form (fork helpers + census test deleted per the user:
"no tests. Just do what he did"; gap stays debug, both sides agree), #218
qsv clean. The zero gate remains banked against 1add35e; the separation
branches stay based on 4eec042 and rebase at ship time as normal. Full
gate green at b94f733 (356 tests).

## PHASE 5 COMPLETE 2026-08-23

- SEPARATION.md + separation-map/ committed on the lineage (cacea22).
- 36 superseded/closed branches renamed to archive/* (local + fork; 22 old
  fork names deleted only after their archive twin was verified pushed).
  Untouched: open-PR branches (#212 #214 #216 #217's), prepped branches
  (still-image-watermark-demuxer, report-schedule-drift, docs/clock-domains,
  bench/, stream-variables), deploy/budget-and-tunein, clock-trace probes,
  pre-existing backup/*.
- NAMING DECISION: rebuilt/* stays as the permanent fork-side extraction
  namespace (base names back open PRs or are archived; no swap).
- Junk removed: worktree-wf_* branches, backup/pre-reconcile-rewrite (its
  rewrite was verified equivalent long ago), stray workflow worktree.
