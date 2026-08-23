# Seed: layer definitions and stale anchors for the hunk-map refresh

Written 2026-08-20 from the (now lost) Phase 1 dependency report's retained
content plus SEPARATION.md. Line anchors below are STALE: they refer to the
pre-reconciliation tip 7b6ae2b. The refresh re-anchors everything to
eb5d808, the current recomposition target. Base is main = 4eec042.

## The cohort stack to be built (each layer must compile on the previous)

- C1 cohort-identity-and-sidecar: core/sidecar.rs, core/cohort.rs,
  core/variant_request.rs, RECOGNIZED_PARAMS half of core lib.rs, core
  Cargo.toml additions (percent-encoding, serde, dev filetime+tempfile);
  playout::{PlayoutItem,PlayoutItemSource}::query_variable_names;
  playout_loader::query_variable_names; channel_session:
  published_recognized_params field, publish_recognized_params,
  source_is_templated, is_templated computation, the transcode() call site;
  playlist_manager: Segment.item_id, pipelines, current_item_id,
  before_new_pipeline +item_id/+duration_ms/+templated/+fallback,
  generate_sidecar, sidecar publish block, pipelines.retain;
  error.rs::JsonError.
- C2 cohort-composer: composer.rs (minus served_window and the join
  arithmetic, which are C6), lib.rs pub mod composer. Imports only
  ersatztv_core::sidecar.
- C3 cohort-sessions-and-serving: variant_manager.rs MINUS the
  default-policy region (slate_file field, DefaultPolicy,
  resolve_default_policy, log_policy_change, default_cohort plumbing
  through tick/read_requests, 15 default-policy tests, the use
  crate::slate import); lib.rs pub mod variant_manager; channel_session:
  query_parameters field, with_query_parameters,
  spawn_playlist_publisher, spawn_variant_loop (without slate_file),
  run_variant (without retention stopgap and claim logging),
  shared_join_offset_ms, variant_start_progress_ms,
  expand_stream_variables_url body change (empty map ->
  self.query_parameters); playout_loader::get_item_by_id;
  config.rs::{merged_source, merged_source_json}; channel main.rs
  Commands::Variant; server main.rs cohort routes + maybe_composed_playlist
  + session_middleware hook + stream/channel_playlist query plumbing;
  kill_on_drop on the variant spawn; url + filetime deps on
  ersatztv-channel.
- C4 slate-on-shared (depends only on C1 + stream variables):
  playout::PlayoutItem.slate + schema + slate tests; ffpipeline
  loop_when_exhausted (input.rs, pipeline.rs, tests/common); slate.rs whole
  file (default key inert until C5); channel_session: item_is_templated,
  resolve_slate, load_slate_path, SlateOrigin, choose_slate,
  usable_item_slate, local_slate, slate_label, slate_item,
  whole_window_slate, trim_point_label, repeat_media_inputs_for_slate, the
  transcode() substitution block, the slate parameter through
  transcode_item/TimingPlan/OutputSettingsPlan, before_new_pipeline
  fallback argument becoming slate.
- C5 slate-default-admission (after C3 AND C4): the variant_manager
  default-policy region excluded from C3; channel_session slate_file:
  slate::slate_file(..) at the VariantChannel construction.
- C6 cohort-retention-and-observability (LAST): VARIANT_HISTORY_DURATION +
  set_history_duration + history/extended_trim_warned + extended-trim warn
  + const assert against composer (playlist_manager); run_variant's
  set_history_duration call + claim logging; composer::served_window +
  variant_manager::{audit_served_window, deepest_variant_reach_ms}; join
  arithmetic reporting; late-join wording; drop/reap reasons; torn-request
  guard; cohort-request liveness.

## Already carved (P-branches; regions NOT owned by any C layer)

fix/io-error-naming 93c77f6 (IoContext sweep MINUS publish-loop dedup),
fix/publish-loop-failure-logging 08416f9 (the dedup),
fix/hls-playlist-conformance, fix/webvtt-cue-timing, fix/idle-and-liveness,
fix/output-folder-lock, fix/kill-child-processes (shared+ffmpeg spawns;
variant spawn kill_on_drop is C3), fix/watermark-cosmetics,
rebuilt/out-point-slot-clamp, rebuilt/black-air-log-census (census fns,
call sites incl. debug gap level per decision 2026-08-20, two tests),
rebuilt/segment-trim-served-head (served_head/trim_cutoff/now seam, WITHOUT
the C6 history budget), rebuilt/timeline-drift-pad-and-trim (plan structs
WITHOUT slate/is_templated/declared_duration_ms members, 2-arg
emission_trim_ms, stamp_error_ms + tests, pad/TPad), feat/stream-variables,
fix/xmltv-number-fallback.

## Stale topic-table anchors from the lost report (pre-reconciliation tip)

channel_session.rs: LOCK :155-159,:173-190+field; IO :191-215,:685-700,
:1149-1153; KILL :1144 + server file; WM :907-912,:2162-2191,:3484-3531;
CLAMP :1424-1440,:2468-2481,:3170-3204; BLACK :769-772,:2092-2111,
:3449-3483; DRIFT :84-122,:942-984,:1383-1504,:1506-1648,:1650-1714,
:2564-2795; C-layers: the rest (about 900 cohort lines).
playlist_manager.rs: TRIM :16-19,:89-99,:568-579,:581-659(now param,
served_head),:814-861,:988-1069,:1102-1112; HLS :581-659(VERSION:6,
discontinuity),:692-734,:944-986; HB :118-124, heartbeat branch in update,
:1141-1191; C1 sidecar production; C6 retention + const assert :66-74.

## Reconciliation deltas the refresh must account for (post-7b6ae2b)

65bf053 schema dup-key fix; ae391bb dead deps + channel_binary_path
private; 764ac9c stamp_error_ms named fn + 4 tests (channel_session);
9fbe339 gap log::debug! + call-site comment (channel_session :770 area);
eb5d808 cache-header test module (server main.rs), stream_variables comment
+ 2 tests.
