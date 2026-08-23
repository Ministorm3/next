# Ownership map: crates/ersatztv-channel/src/channel_session.rs

Base main = 4eec042, TARGET = eb5d808. Diff: 2311 insertions, 154 deletions
across 29 hunks. All line numbers below are current-tree lines at eb5d808.
File length at TARGET: 3607 lines.

Stack order used: P:io, P:publish-loop, LOCK (fix/output-folder-lock),
P:kill, P:watermark, P:clamp, P:black-air, P:drift, [feat/stream-variables],
C1, C3, C4, C5, C6. (No C2 regions exist in this file.)

Conventions: a "region" is a changed span at TARGET; deletions with no
surviving lines are noted inline under the owner that made them. Shared
functions are broken down to statement/argument/field granularity.

## P:io (fix/io-error-naming)

- imports (9): `IoContext` added to the `ersatztv_channel::error` use.
- ChannelSession::new (194-197, 203-206, 212-215, 221-224): the four
  `OutputPathNotUtf8 { file, path }` error maps, replacing
  `ChannelConfigOutputFolderRequired` on each `.into_string()` failure.
- prep_output_folder (685, 692, 697): the three `.io_context(...)` calls
  ("remove the stale ready file", "empty the output folder", "create the
  output folder") replacing bare `?`/map_err.
- transcode (747): pts scan failure log becomes `log::debug!("{e}")`; the
  old "failed to scan pts time:" prefix deleted (the error is now
  self-describing via IoContext). See DISPUTED.
- transcode_item (1154-1159): ffmpeg spawn error now carries the error and
  the ffmpeg path (`failed to spawn ffmpeg {path}: {e}`) instead of the
  fixed string.

## P:publish-loop (fix/publish-loop-failure-logging)

- spawn_playlist_publisher, interior only (317, 320-333): the
  `last_failure: Option<String>` dedup state, the `match
  playlist_manager.update().await` with "playlist update recovered" info and
  once-per-distinct-message "playlist update failed" warn. In the carved
  P-branch this logic lives in the inline loop in `run`; C3 later extracts
  the loop into this function (see C3).
- spawn_playlist_publisher doc, third paragraph (309-311): "Distinct
  failures are reported once ... without burying the log."
- Deletion it owns: the base loop's `let _ = playlist_manager.update().await;`
  (silently-swallowed failure).

## LOCK (fix/output-folder-lock)

- ChannelSession struct (156-158): `_output_folder_lock:
  ersatztv_core::FolderLock` field plus its doc comment.
- ChannelSession::new (173-188): the `lock_folder_exclusive` block, the
  WouldBlock refusal with the "another channel worker already owns the
  output folder" ChannelStartup error, and the `io_context("lock the output
  folder", ...)` fallthrough (that one call uses P:io's trait; LOCK stacks
  after P:io).
- ChannelSession::new (285): `_output_folder_lock: output_folder_lock` in
  the struct literal.

## P:kill (fix/kill-child-processes)

- transcode_item (1149-1152): `.kill_on_drop(true)` on the shared/ffmpeg
  spawn plus its three-line orphaned-transcoder comment. (The variant
  process spawn's kill_on_drop lives in variant_manager and is C3, not
  here.)

## P:watermark (fix/watermark-cosmetics)

- transcode_item, graphics_fut (910-911, 914): `let source =
  cosmetic_source(layer.source.clone());` feeding both
  `playout_source_to_input_source(source.clone())` and
  `resolve_probe(&source, ...)` in place of `layer.source`.
- cosmetic_source (2183-2213): the function and its doc (strips
  reconnect/reconnect_delay_max/keep_alive/is_live from Http decoration
  sources).
- mod cosmetic_source_tests (3551-3607): both tests
  (`a_cosmetic_http_source_carries_no_streaming_options`,
  `local_sources_pass_through_unchanged`).

## P:clamp (rebuilt/out-point-slot-clamp)

- input_timing_at, interior (1401): `item_slot_ms` derivation.
- input_timing_at, interior (1426-1440): `explicit_out_point_ms` match
  (rewritten from the base's inline `out_point_ms.unwrap_or(...)`), the
  `effective_out_point_ms(...)` call, and the "out_point overruns its slot
  ... clamping to the slot" warn.
- effective_out_point_ms (2483-2506): the function and its doc (clamp an
  explicit out_point to in_point_base + slot; returns overrun).
- tests (3245-3278): the four clamp tests
  `an_item_without_an_explicit_out_point_plays_exactly_its_slot`,
  `an_out_point_inside_the_slot_narrows_what_plays`,
  `an_out_point_past_the_slot_is_clamped_to_the_slot`,
  `an_out_point_clamp_respects_an_explicit_in_point`.

## P:black-air (rebuilt/black-air-log-census)

- transcode (771-780): the two census call sites. 771-775: the schedule-gap
  arm, `log::debug!("{}", no_item_message(...))` with the reconciled
  comment "a schedule gap is the one expected way to air black; the census
  line still counts it, but not at fault level" (9fbe339 confirmed here:
  comment at 772-773, debug call at 774). 777-780: the unselectable arm,
  `log::error!("{}", item_unselectable_message(...))`. Owns the deletion of
  the base's two-branch gap logging and bare `log::error!("{}", err)`.
- transcode (832): `log::error!("{}", item_failed_message(&current_item,
  &e))` replacing "item failed, replacing with black/silence: {e}".
- census functions (2112-2136): the module doc plus `no_item_message`,
  `item_unselectable_message`, `item_failed_message`.
- tests (3521-3548): `every_black_air_line_names_its_slot_and_shares_one_phrase`.
  NOTE: at TARGET this test builds its item via the shared
  `templated_item()` fixture (C1-introduced, see tests note below); the
  carved P-branch must carry its own minimal item fixture, and the
  recomposition may swap it to the shared fixture at C1 or leave it.

## P:drift (rebuilt/timeline-drift-pad-and-trim)

Reminder from the seed: this branch builds the plan structs WITHOUT
slate/is_templated/declared_duration_ms members and emission_trim_ms as the
2-arg core (stamp_error_ms, pipeline_ms). The members and third argument are
added by C1 and C4 as itemized under those layers.

- OutputSettingsPlan (79-95): the struct, its doc, and every field EXCEPT
  `slate` (92, C4). So: channel_config, accel, output_file,
  output_segment_template, troubleshoot, pts_duration, realtime, is_live,
  video_is_still_image.
- TimingPlan (97-110): the struct, its doc, and every field EXCEPT `slate`
  (105, C4) and `is_templated` (107, C1). So: current_item, audio_source,
  video_source, subtitle_source, start_at_zero, realtime, is_live,
  transcoded_until, stamp_error_ms.
- PlannedTimings (112-122): the struct and doc, fields audio, video,
  subtitle, trim_ms. NOT `declared_duration_ms` (120, C1).
- transcode_item (951-963): the `Self::build_output_settings(
  OutputSettingsPlan { ... })` call replacing the ~90-line inline
  OutputSettings literal (owns that deletion), all arguments except the
  `slate,` line (960, C4).
- transcode_item (965-968): `start_at_zero` unchanged in content but kept as
  context; not a changed region.
- transcode_item (970-983): the stamp-error measurement block: the comment,
  the playlist_manager lock + `update().await?`, and the
  `Self::stamp_error_ms(last_segment_end, transcoded_until,
  start_time_offset)` call (named-fn shape is the 764a... reconciliation,
  764ac9c, confirmed here).
- transcode_item (984-1002): the `PlannedTimings { .. } =
  Self::plan_timings(TimingPlan { ... })` destructure and call, replacing
  the three `self.input_timing(...)` calls (owns that deletion). EXCEPT:
  `declared_duration_ms,` (988) and `is_templated,` (999) are C1; `slate,`
  (997) is C4.
- transcode_item (1003-1008): the `emission trim {trim_ms}ms for item ...`
  debug log.
- input_timing_at (1376-1387 doc; 1388-1395 signature; 1408-1424,
  1442-1487 body): rename input_timing -> input_timing_at, drop `&self`,
  add the `transcoded_until: OffsetDateTime` parameter (the testability
  seam); the reworked live branch (live_now clamped into the item,
  1410-1424); the dedup of the second live guard plus its explanatory
  comment (1448-1454); the unchanged-in-spirit tail (progress/limit/finish)
  reflowed. EXCEPT the clamp lines listed under P:clamp (1401, 1426-1440).
  Doc lines 1379-1387 (the PR #187 live-guard rationale) are P:drift here
  but see DISPUTED.
- stamp_error_ms (1489-1507): doc (1489-1500) + function (1501-1507).
  Reconciled location confirmed (764ac9c named fn).
- emission_trim_ms (1509-1539): doc and function, in its 2-arg core form:
  the MAX_CORRECTION_MS clamp and the half-pipeline floor. EXCEPT the
  templated exemption: doc paragraph 1522-1525 ("Templated items are
  exempt..."), the `is_templated: bool` third parameter (1531), and the
  early return (1533-1535) are C1.
- apply_emission_trim (1541-1557): doc + function.
- build_output_settings (1559-1668): the function (extraction of the former
  inline literal), its doc, `pad_to_duration: true` (1636) and the
  quantization/trim rationale comment (1612-1635), the `realtime:
  plan.realtime` line (1645), the still-image frame_rate plan form
  (1647-1651). EXCEPT: comment paragraph 1608-1611 ("Two jobs. A templated
  item may be transcoded in parallel by variant sessions...") is C1
  wording and must be added by C1; comment 1637-1644 ("Slate paces like
  every other pipeline...") is C4.
- plan_timings (1670-1737): doc (1670-1674), the three input_timing_at
  calls (1684-1709), pipeline_ms + trim + apply (1711-1718), and the
  PlannedTimings return (1730-1736, minus the declared_duration_ms field
  line 1734). EXCEPT: `let whole_window = plan.realtime || plan.slate;`
  plus its comment (1676-1682) is C4 (in P:drift the calls pass
  plan.realtime directly); the declared_duration_ms computation and comment
  (1720-1728) and the `declared_duration_ms,` return field (1734) are C1;
  `plan.is_templated` in the trim call (1716) is C1 (P:drift's call is
  2-arg).
- tests, drift group (2587-2664 minus 2666-2671; 2673-2861 minus C1/C4
  lines):
  - `at` helper (2587-2589).
  - `stamp_error_measures_emission_against_the_schedule` (2591-2602),
    `a_virtual_start_offset_is_not_a_stamp_error` (2604-2619),
    `a_real_error_is_still_measured_through_a_virtual_start_offset`
    (2621-2628), `a_virtual_start_offset_produces_no_emission_trim`
    (2630-2636): the four reconciled 764ac9c tests, locations confirmed.
  - `a_small_stamp_clock_error_is_returned_in_full` (2638-2646),
    `a_large_stamp_clock_error_is_clamped_in_both_directions` (2648-2657),
    `a_trim_never_eats_more_than_half_the_pipeline` (2659-2664): DUAL with
    C1: introduced by P:drift with 2-arg emission_trim_ms calls; C1 extends
    every call with the `false` third argument.
  - `a_trim_moves_only_the_out_point` (2673-2707).
  - `test_channel_config` (2709-2730).
  - `output_settings` helper (2732-2745): DUAL with C4: P:drift introduces
    it without the `slate` parameter/field; C4 adds `slate: bool` (2732,
    2741).
  - `every_pipeline_is_padded_to_its_clamp` (2747-2761): DUAL with C4:
    P:drift introduces iterating realtime x is_live; C4 adds the slate loop
    dimension (2752, 2755-2756).
  - `a_still_image_forces_an_output_frame_rate` (2774-2788),
    `the_pts_offset_reaches_the_encoder` (2790-2799): P:drift (their
    output_settings() calls gain a slate argument when C4 lands; C4 edits
    the call sites mechanically).
  - `file_item` (2801-2811).
  - `plan_for` helper (2813-2837): DUAL: P:drift introduces (no
    slate/is_templated params, TimingPlan without those fields); C1 adds
    `is_templated` (2816, 2833); C4 adds `slate` (2815, 2831).
  - `the_trim_reaches_every_stream_the_t_reads` (2839-2853): DUAL with C1:
    P:drift introduces it; the `planned.declared_duration_ms` assertion
    (2850) is added by C1.

## C1 (cohort-identity-and-sidecar)

- imports: none of its own in this file (RECOGNIZED_PARAMS_FILE_NAME and
  stream_variables are referenced fully qualified).
- TimingPlan (107): `is_templated: bool` field.
- PlannedTimings (120): `declared_duration_ms: u64` field.
- ChannelSession struct (150): `published_recognized_params:
  Option<Vec<String>>` field.
- ChannelSession::new (283): `published_recognized_params: None` in the
  struct literal.
- publish_recognized_params (645-677): whole function (collect
  query_variable_names from the loader, compare, write
  RECOGNIZED_PARAMS_FILE_NAME next to the ready file).
- transcode (750): `self.publish_recognized_params().await;` call site (the
  "transcode() call site" of the seed's C1 list).
- transcode_item (945-949): the `is_templated` computation
  `source_is_templated(&video_source) || source_is_templated(&audio_source)`.
  EXCEPT the `slate ||` prefix (949) and the three-line slate-contract
  comment (945-947), which are C4.
- transcode_item (988, 999): `declared_duration_ms,` in the PlannedTimings
  destructure and `is_templated,` in the TimingPlan literal.
- transcode_item, before_new_pipeline call (1127-1138): the three added
  arguments `&current_item.id`, `declared_duration_ms`, `is_templated`
  (1133-1135) plus a fourth `fallback: bool` argument that C1 introduces
  and C4 renames/repurposes to `slate` (1136); the multiline reflow of the
  call is C1's.
- emission_trim_ms (1522-1525, 1531 third param, 1533-1535): the templated
  exemption (doc paragraph, `is_templated: bool`, early `return 0`).
- build_output_settings comment (1608-1611): the "Two jobs / templated item
  transcoded in parallel by variant sessions" padding rationale paragraph.
- plan_timings (1716 `plan.is_templated` argument; 1720-1728
  declared_duration_ms computation + comment; 1734 return field).
- source_is_templated (2215-2225): the function and doc (uses
  stream_variables::has_query_variables; stacks after
  feat/stream-variables).
- tests:
  - `a_templated_item_is_never_trimmed` (2666-2671).
  - the `false`/`true` third arguments on every emission_trim_ms call in
    the P:drift trim tests (see DUAL notes under P:drift).
  - `a_templated_plan_ignores_the_stamp_error` (2855-2865).
  - `templated_item_with_slate` fixture (2925-2944): C1 introduces it as a
    plain `templated_item()`-shaped fixture (templated live URI with
    `{query:zip|10001}`); C4 extends it with the `slate:
    Option<serde_json::Value>` parameter and insertion (2927, 2939-2941).
    DUAL C1+C4.
  - `templated_item` (2946-2948).

## C3 (cohort-sessions-and-serving)

- imports (11-12): `use ersatztv_channel::variant_manager;` and
  `use ersatztv_channel::variant_manager::{VariantChannel, VariantManager};`.
- ChannelSession struct (152-154): `query_parameters:
  HashMap<String, String>` field + doc.
- ChannelSession::new (284): `query_parameters:
  std::collections::HashMap::new()` in the struct literal.
- with_query_parameters (289-296): whole function + doc.
- spawn_playlist_publisher (298-349): the extraction itself: signature
  (312-315), tokio::spawn shell, timeout branch, interval selection, sleep;
  doc paragraphs one and two (298-307, the shared-on-purpose / #202
  rationale); the startup-interval comment naming "a variant's first
  sidecar" (338-339). The failure-dedup interior is P:publish-loop (see
  above). Owns the deletion of the inline copy in `run` (the base's
  `let pm = ...; let tn = ...; tokio::spawn(...)` block).
- run (371): `Self::spawn_playlist_publisher(self.playlist_manager.clone(),
  self.timeout_notify.clone());` call.
- run (373): `self.spawn_variant_loop();` call.
- run_variant (420-598): whole function: doc (420-424),
  signature (425-431), prep_output_folder call, ffmpeg_info/hw_accel setup,
  spawn_playlist_publisher call (460), get_item_by_id (462-465), the
  join-offset derivation comment + `item_duration_ms` / `join_offset_ms` /
  `anchor` (467-480), the live_item detection (482-492), the
  variant_start_progress_ms clamp + comment (494-505), transcoded_until /
  state seeding (527-533), the live air-lock wait loop (535-554), the
  transcode loop with `transcode_item(&item, true, false, Some(pts), false)`
  and no-progress failure (571-592), the final playlist update (594-597).
  EXCEPT the three C6 regions listed under C6 (434-443, 507-525, 556-569).
  Note the transcode_item call's trailing `false` (582) is the slate
  argument: C4 signature ripple, see C4.
- spawn_variant_loop (600-643): whole function: doc (600-605),
  channel_binary resolution + disable warn (607-615), the VariantChannel
  literal (617-623) EXCEPT the `slate_file:` line (622, C5), the spawned
  tick loop and the panic-visibility error log (625-642).
- expand_stream_variables_url body (1299): `&self.query_parameters` as the
  third expand_url argument (the empty-map placeholder it replaces belongs
  to feat/stream-variables; see UNASSIGNED).
- shared_join_offset_ms (2429-2442): function + doc.
- variant_start_progress_ms (2444-2481): function + doc (the late-join
  claim clamp; doc paragraphs on schedule-derivation and PR #187).
- tests:
  - `variant_envelope` helper (2912-2923).
  - `a_shared_session_that_started_the_item_has_no_join_offset`
    (3280-3283).
  - `window` helper (3285-3289).
  - `a_variant_that_opens_on_time_keeps_the_progress_it_was_given`
    (3291-3318).
  - `a_live_variant_opening_late_claims_where_the_wall_clock_stands`
    (3320-3352).
  - `a_live_source_never_seeks_however_far_the_session_has_progressed`
    (3354-3411): exercises input_timing_at (P:drift seam) but exists to pin
    the C3-created hazard (non-zero variant progress selecting the seeking
    branch); uses the C1 `templated_item` fixture. Owner C3; see DISPUTED.
  - `a_file_variant_is_never_moved_by_the_wall_clock` (3413-3428).
  - `a_late_open_cannot_claim_past_the_envelope` (3430-3459).
  - `a_shared_session_that_joined_late_reports_how_far_in_it_started`
    (3461-3466).
  - `a_variant_of_an_item_started_from_zero_fills_the_whole_remainder`
    (3468-3476).
  - `a_variant_of_a_late_joined_item_stops_where_the_shared_envelope_stops`
    (3478-3492).
  - `a_variant_produces_nothing_once_the_shared_envelope_is_covered`
    (3494-3502).
  - `a_variant_envelope_always_ends_with_the_shared_one` (3504-3520).

## C4 (slate-on-shared)

- imports (10): `use ersatztv_channel::slate::{self, SlateFile};` (C5 also
  needs `slate::slate_file` but C4 introduces the import; C5 adds nothing
  to it).
- OutputSettingsPlan (92): `slate: bool` field (declared for test parity;
  not read in build_output_settings' body).
- TimingPlan (105): `slate: bool` field.
- transcode (783-818): the whole slate-on-shared substitution block: the
  five-line intro comment, `let mut slate = false;`, the
  `item_is_templated` match, resolve_slate + the "shared session plays
  slate ... from ..." info log, `slate_item(...)`, and the
  ignored-slate-on-untemplated warn arm.
- transcode (823): the `slate` fifth argument on the main transcode_item
  call.
- transcode (834): the `false` fifth argument on the fake-item retry
  transcode_item call.
- transcode_item signature (853): `slate: bool` parameter.
- transcode_item (945-947 comment, 949 `slate ||` prefix): the slate keeps
  the templated contract extension of C1's is_templated computation.
- transcode_item (960): `slate,` in the OutputSettingsPlan literal.
- transcode_item (997): `slate,` in the TimingPlan literal.
- transcode_item (1039, 1063, 1075): the three `loop_when_exhausted: false`
  fields on the subtitle/audio/video ProbedInput literals (the field itself
  is C4's ffpipeline change).
- transcode_item (1044-1045): the "every input is read once through ...
  slate window then says otherwise" comment.
- transcode_item (1080): `repeat_media_inputs_for_slate(&mut
  input_settings, slate);`.
- transcode_item, before_new_pipeline call (1136): the last argument
  becomes the live `slate` value (C1 introduced the position as
  `fallback`; C4 renames the parameter and wires the real flag).
- run_variant (582): the trailing `false` slate argument on the variant's
  transcode_item call (signature ripple into a C3 function; C4 must land
  this edit when it changes the signature; if C4 is stacked before C3 the
  argument is written by C3 instead. In the seed's declared stack C3
  precedes C4, so C4 edits this line).
- build_output_settings (1637-1644): the "Slate paces like every other
  pipeline" comment block.
- plan_timings (1676-1682): the `whole_window = plan.realtime ||
  plan.slate` line and its slate-must-fill-one-pipeline comment, plus the
  substitution of `whole_window` for `realtime` in the three
  input_timing_at calls (1687, 1695, 1704).
- item_is_templated (1739-1748): function + doc.
- resolve_slate (1750-1763): function + doc.
- load_slate_path (1765-1796): function + doc (SlateFile handling,
  missing-media warn).
- fake_playout_item (1858): `slate: None` field (ripple from
  PlayoutItem.slate).
- SlateOrigin + Display (2227-2245).
- choose_slate (2247-2263).
- usable_item_slate (2265-2288).
- local_slate (2290-2298).
- slate_label (2300-2312).
- slate_item (2314-2338).
- whole_window_slate (2340-2405).
- trim_point_label (2407-2413).
- repeat_media_inputs_for_slate (2415-2427).
- tests:
  - `slate` additions to the P:drift helpers/tests (see DUAL notes under
    P:drift: output_settings helper, every_pipeline_is_padded, plan_for).
  - `slate_paces_like_every_other_pipeline` (2763-2772).
  - `slate_fills_its_whole_window_in_one_pipeline` (2867-2910).
  - slate parameter + insertion on `templated_item_with_slate` (2927,
    2939-2941; DUAL with C1, which owns the base fixture).
  - `side_file_slate` helper (2950-2954).
  - `a_templated_window_is_recognized_before_slate_substitution`
    (2956-2964).
  - `an_item_carrying_a_slate_plays_it_over_the_side_file` (2966-3001).
  - `an_item_without_a_slate_falls_back_to_the_side_file` (3003-3023).
  - `with_no_slate_anywhere_the_live_source_is_tuned` (3025-3043): calls
    C1's source_is_templated and source_is_live; single owner C4 (it tests
    the no-substitution path).
  - `a_slate_naming_media_that_is_not_there_falls_through` (3045-3067).
  - `a_slate_item_keeps_the_window_identity_and_swaps_only_the_source`
    (3069-3093).
  - `trim_points` helper (3095-3112).
  - `a_declared_slate_plays_its_whole_window_whatever_trim_points_it_carries`
    (3114-3151): asserts through P:clamp's effective_out_point_ms; single
    owner C4 (clamp is below it in the stack), noted as cross-topic.
  - `a_remote_slate_loses_its_trim_points_the_same_way` (3153-3176): same
    note.
  - `one_pass_input_settings` helper (3178-3205).
  - `a_slate_window_repeats_its_media_inputs_and_a_scheduled_one_never_does`
    (3207-3243).

## C5 (slate-default-admission)

- spawn_variant_loop, VariantChannel literal (622): the single line
  `slate_file: slate::slate_file(self.channel_config.expanded_playout_folder()),`.
  (The `slate_file` struct member itself is declared in variant_manager;
  only this construction-site line lives in this file.)

## C6 (cohort-retention-and-observability)

- imports (42-44): `VARIANT_HISTORY_DURATION` added to the
  playlist_manager use (the multiline reflow of that use is incidental to
  this addition).
- run_variant (434-443): the STOPGAP block: the comment and
  `self.playlist_manager.lock().await.set_history_duration(VARIANT_HISTORY_DURATION);`.
- run_variant (507-525): the claim-reporting block: the "the spawn line in
  variant_manager reports the progress this worker was ORDERED with"
  comment and both `log::info!` arms ("opened past its envelope; no
  transcode is started" / "opened {progress_ms}ms into its envelope and
  claims that position"). The `variant_start_progress_ms` call it reports
  on (498-505) stays C3.
- run_variant (556-569): the claim-lag instrumentation block: the
  hand-traced-distribution comment and the "begins transcoding
  {claim_lag_ms}ms past its claimed position" info log.

## UNASSIGNED

- expand_stream_variables_url shell (1292-1301 minus the 1299 argument)
  and the two call sites (1324 in the Http arm, 1347 in the Rtsp arm):
  these belong to **feat/stream-variables**, a P-branch the seed lists as
  already carved but which the owner enumeration for this file omits. Not
  a gap in the analysis, a gap in the enumeration: P:stream-variables
  introduces the function with an empty query map and the two
  `self.expand_stream_variables_url(&expand_template(&uri)?)` call sites
  (turning both expand_template-only lines into wrapped calls); C3 then
  changes only the third argument to `&self.query_parameters` (line 1299)
  and the doc sentence about variant sessions (1293-1294). Recommendation:
  extend the owner set with P:stream-variables for exactly these lines, or
  fold them into whatever slot feat/stream-variables occupies in the stack.

Nothing else failed to match an owner.

## DISPUTED

1. transcode (747), `log::debug!("{e}")` replacing "failed to scan pts
   time: {e}": assigned P:io (the sweep made the underlying error
   self-describing, so the prefix became duplication), but the stale IO
   anchors (:191-215, :685-700, :1149-1153) do not list this line, so it
   may have ridden along in a different P commit. It is a one-line
   log-wording change with no dependency either way; recommendation: keep
   with P:io.
2. input_timing_at doc lines 1379-1387 (the PR #187 live-guard paragraphs:
   "what keeps a non-zero progress_ms from turning into an input seek")
   and the dedup comment 1448-1454: assigned P:drift per the stale DRIFT
   anchor (:1383-1504 at the old tip), but the hazard they describe only
   exists once C3's variant_start_progress_ms can produce a non-zero
   progress, and the "there is now a test on it" sentence points at a C3
   test. Recommendation: P:drift carries the staticization and the dedup
   with neutral wording; C3 adds or rewords these paragraphs when it lands
   the guard test. Either split compiles.
3. Test `a_live_source_never_seeks_however_far_the_session_has_progressed`
   (3354-3411): exercises a P:drift function using a C1 fixture to pin a
   C3-created hazard. Assigned C3 (it is C3 that makes the branch load
   bearing, and its doc names 02a05f7/variant_start_progress_ms).
   Alternative: P:drift with a local fixture. Recommendation: C3.
4. spawn_playlist_publisher (298-349) split between C3 (extraction) and
   P:publish-loop (dedup interior): in the carved P-branch the dedup sits
   in run()'s inline loop; when C3 extracts the function it must carry the
   dedup along. The reverse layering (P:publish-loop patches the extracted
   fn) is impossible because P-branches precede C3. Recommendation as
   mapped: P:publish-loop edits run()'s inline loop; C3's extraction moves
   the already-deduped body.
5. before_new_pipeline last argument (1136): C1 introduces the position as
   `fallback` wired to a constant/derived value, C4 renames it to `slate`
   and wires the live flag. If the recomposition prefers no rename churn,
   C1 could name it `slate` from the start with a hardcoded `false`;
   recommendation: keep the seed's fallback-then-rename to match the
   playlist_manager side.
6. run_variant transcode_item call, trailing `false` (582): the seed's
   stack builds C3 before C4, so at C3 time transcode_item has no slate
   parameter and this argument does not exist; C4 adds it here when it
   widens the signature. Mapped to C4. If the stack order ever flips
   (C4 first), the line becomes C3's.
7. `loop_when_exhausted: false` on the three media ProbedInput literals
   (1039, 1063, 1075): mapped to C4 because the ffpipeline field arrives
   with C4's loop_when_exhausted change; they are pure ripple, not slate
   logic, but no other owner can compile without C4's ffpipeline layer.

## Completeness check

Diff totals: 2311 insertions, 154 deletions (29 hunks).

Added-line tally by owner (approximate; ranges rounded to whole regions,
comments included):

- P:io ~27, P:publish-loop ~17, LOCK ~21, P:kill ~4, P:watermark ~91,
  P:clamp ~78, P:black-air ~44, P:drift ~680 (structs 40, transcode_item
  refactor ~60, input_timing_at rework ~70, extracted functions
  1489-1737 core ~230, tests ~240, plus reflow), feat/stream-variables
  (UNASSIGNED) ~13, C1 ~119, C3 ~530, C4 ~660, C5 ~1, C6 ~47.

Sum ~2332 against 2311 recorded insertions: within ~1%, the excess being
whole-region rounding where a region contains unchanged context lines
(notably build_output_settings' moved-but-counted body and the
spawn_playlist_publisher split). Every deletion is accounted for by an
owner above: base error maps (P:io), the inline publish loop (C3
extraction, dedup interior P:publish-loop), the inline OutputSettings
literal and input_timing body (P:drift), the base out_point unwrap
(P:clamp), the base gap/black-air log lines (P:black-air), the base
transcode_item/before_new_pipeline argument lists (C1/C4 ripple), the
bare expand_template uri lines (feat/stream-variables). No changed region
is unmapped.
