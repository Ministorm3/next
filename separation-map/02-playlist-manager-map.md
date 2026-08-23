# Topic ownership map: `crates/ersatztv-channel/src/playlist_manager.rs`

Base `main` = 4eec042. TARGET = eb5d808 (worktree HEAD, file is 1219 lines).
Diff: `git diff main...eb5d808 -- crates/ersatztv-channel/src/playlist_manager.rs`
= **841 insertions, 44 deletions**.

All line numbers below are TARGET line numbers (eb5d808) unless the text says
"old" (= main line numbers).

## Evidence base

Assignments are not reconstructed from the topic table alone. Every P-owner
below was checked against its already-carved branch, so the "main-based form"
claims are observed, not inferred:

| Owner | Carved branch | Diff vs main for this file |
|---|---|---|
| P:io | `fix/io-error-naming` 93c77f6 | 170 ins / 23 del |
| P:hls | `fix/hls-playlist-conformance` | 86 ins / 15 del |
| P:heartbeat | `fix/idle-and-liveness` | 106 ins / 1 del |
| P:trim | `rebuilt/segment-trim-served-head` | 240 ins / 4 del |
| P:drift | `rebuilt/timeline-drift-pad-and-trim` | 8 ins / 0 del |
| (none) | `fix/publish-loop-failure-logging`, `fix/webvtt-cue-timing`, `rebuilt/black-air-log-census`, `fix/output-folder-lock`, `fix/kill-child-processes`, `feat/stream-variables`, `fix/xmltv-number-fallback`, `rebuilt/out-point-slot-clamp` | **empty for this file** |

Two notes that fall straight out of that table:

- `fix/webvtt-cue-timing` does **not** touch this file. The webvtt cue-timing
  change in `render_subtitle_segment` (full cue range + `X-TIMESTAMP-MAP`
  anchoring) and its three tests are carried by
  `fix/hls-playlist-conformance`. So P:hls owns them here.
- `rebuilt/timeline-drift-pad-and-trim` **does** touch this file — 8 lines the
  brief's owner list does not mention. See UNASSIGNED item U1.

## Owner legend

- **P:io** — `fix/io-error-naming`: `IoContext` sweep.
- **P:hls** — `fix/hls-playlist-conformance`: EXT-X-VERSION:6,
  always-emit DISCONTINUITY-SEQUENCE, webvtt cue timing.
- **P:heartbeat** — `fix/idle-and-liveness`: missing-heartbeat expiry.
- **P:trim** — `rebuilt/segment-trim-served-head`: served_head/trim_cutoff/now
  seam with the plain pre-C6 `HISTORY_DURATION`.
- **P:drift** — `rebuilt/timeline-drift-pad-and-trim`: the `last_segment_end()`
  accessor only.
- **C1** — cohort-identity-and-sidecar (sidecar production).
- **C6** — cohort-retention-and-observability (variant budget + const assert).

---

## 1. File header, constants (lines 1-74)

| Lines | Region | Owner |
|---|---|---|
| 4 | `use std::time::{Duration, Instant};` (adds `Instant`; old 4) | **P:heartbeat** |
| 6 | `use ersatztv_channel::error::{ChannelError, IoContext};` (old 6) | **P:io** |
| 7 | `use ersatztv_core::sidecar::{PlaylistSidecar, SidecarPipeline, SidecarSegment};` | **C1** |
| 16-19 | `HISTORY_DURATION` doc + const (replaces nothing; the old `// 12s` at old 15 is consumed here) | **P:trim** |
| 20 | `/// How far past the wall clock the published window reaches. Twelve seconds.` — replaces old `// 12s` | **DISPUTED D1** |
| 24-42 | `VARIANT_HISTORY_DURATION` STOPGAP doc + const | **C6** |
| 44-73 | Cross-module contract comment + `const _: () = assert!(...)` | **C6** (see note (b)) |

Note (b) — the const assert at 44-73 lives in this file but references
`ersatztv_channel::composer::{HARD_LAG_SEGMENTS, SERVED_SEGMENTS,
SEGMENT_SECONDS}`. `composer` does not exist until **C2**, so the assert
cannot ship in P:trim even though the value it constrains
(`HISTORY_DURATION`, P:trim's constant) does. Earliest *compilable* layer is
C2; the seed assigns it to C6 and that is correct on topic — it is a retention
bound, and the comment's whole argument is about variant twins outliving
their files. Keep it at C6. If a later reorder wants it earlier, C2 is the
floor, never P:trim.

## 2. `struct PlaylistManager` (83-137) and `struct Segment` (139-145)

| Lines | Region | Owner |
|---|---|---|
| 96-106 | `served_head: Option<u64>` + its 11-line doc | **P:trim** |
| 117-118 | `current_item_id: String`, `pipelines: Vec<SidecarPipeline>` | **C1** |
| 119 | blank separator | C1 (introduces the gap C6 then fills) |
| 120-125 | `history: Duration` + `extended_trim_warned: bool` + docs | **C6** |
| 126 | blank separator | C6 |
| 128-134 | `heartbeat_last_seen: Instant` + `heartbeat_missing_warned: bool` + doc | **P:heartbeat** |
| 144 | `item_id: String` on `Segment` | **C1** |

## 3. `PlaylistManager::new` (154-202)

| Lines | Region | Owner |
|---|---|---|
| 176 | `served_head: None,` | **P:trim** |
| 187-188 | `current_item_id: String::new(), pipelines: Vec::new(),` | **C1** |
| 189 | blank separator | C1 |
| 190-191 | `history: HISTORY_DURATION, extended_trim_warned: false,` | **C6** |
| 192 | blank separator | C6 |
| 194-198 | `heartbeat_last_seen: Instant::now(),` + "Starts true" comment + `heartbeat_missing_warned: true,` | **P:heartbeat** |

## 4. Accessors (204-229)

| Lines | Region | Owner |
|---|---|---|
| 204-210 | `set_history_duration` + doc | **C6** |
| 219-226 | `last_segment_end()` accessor + doc | **P:drift** (see U1) |

## 5. `before_new_pipeline` (231-276) — sub-function split

| Lines | Region | Owner |
|---|---|---|
| 235-238 | signature growth: `item_id: &str, duration_ms: u64, templated: bool, fallback: bool` | **C1** |
| 245-252 | `self.current_item_id = ...` + `self.pipelines.push(SidecarPipeline {...})` | **C1** |
| 258-259 | `generate_playlist(..., None, OffsetDateTime::now_utc())` — the `now` argument, rewrapping old 134 onto two lines | **P:trim** |
| 260-272 | the three `.io_context(...)` wrappers on temp/write/rename (replaces old 135-137) | **P:io** |

C1/C4 boundary: the fourth parameter is named `fallback` here and stays
`fallback` in the target. C4 (slate-on-shared) changes only the *argument*
passed at the `channel_session` call site, not this signature. Nothing in
this file is C4's.

## 6. `update()` (278-515) — sub-function split, in body order

This is the function where four owners interleave. Ordered walk:

| Lines | Region | Owner |
|---|---|---|
| 281-283 | `read_dir(...).io_context("scan the segment folder", ...)` | **P:io** |
| 332 | `item_id: self.current_item_id.clone(),` in the `Segment` push | **C1** |
| 351-358 | subtitle-segment temp/write/rename `io_context` | **P:io** |
| 365-374 | empty-subtitle-segment temp/write/rename `io_context` | **P:io** |
| 379 | `let cutoff = self.trim_cutoff();` (replaces old 229 `now_utc() - from_mins(2)`) | **P:trim** |
| 381-389 | extended-trim warn: `if self.history != HISTORY_DURATION && !self.extended_trim_warned { ... log::warn!("STOPGAP retention ...") }` | **C6** |
| 398-400 | `remove_file(&path)...io_context("delete the trimmed segment", ...)` | **P:io** |
| 407-409 | `remove_file(&vtt_path)...io_context("delete the trimmed subtitle segment", ...)` | **P:io** |
| 414-421 | `// drop pipeline records ...` + `self.pipelines.retain(...)` + blank | **C1** |
| 424 | `generate_playlist(|s| s.to_owned(), Some(10), OffsetDateTime::now_utc())` — the `now` argument | **P:trim** |
| 425-437 | served-playlist temp/write/rename `io_context` | **P:io** |
| 438-453 | blank + `// publish the machine-readable sidecar alongside the playlist` + `generate_sidecar()` + `sidecar_file` + temp/write/rename | **C1** (see note (c)) |
| 459 | `OffsetDateTime::now_utc(),` argument in the subtitle `generate_playlist` call | **P:trim** |
| 461-476 | subtitle-playlist temp/write/rename `io_context` | **P:io** |
| 479-481 | ready-file `io_context("publish the ready signal", ...)` | **P:io** |
| 486-492 | `metadata(...).io_context("stat the heartbeat file", ...)` + `modified().io_context(...)` | **P:io** |
| 493-494 | `self.heartbeat_last_seen = Instant::now(); self.heartbeat_missing_warned = false;` | **P:heartbeat** |
| 496-511 | the whole `else` branch: missing-heartbeat comment, one-shot warn, `self.timeout = self.heartbeat_last_seen.elapsed() > HEARTBEAT_FILE_TIMEOUT;` | **P:heartbeat** |

Note (c) — **position of the sidecar publish block inside `update()`**:

- It sits at **438-453**, immediately *after* P:io's served-playlist publish
  region (which ends at 437) and immediately *before* P:trim's `now` argument
  on the subtitle playlist call (459) and P:io's subtitle-playlist wrappers
  (461-476). It is bracketed by P:io regions on both sides.
- It is **strictly above** every P:heartbeat change in this function
  (485-511), by ~30 lines. There is no interleaving with P:heartbeat at all.
- The block's own three fs calls already carry `.io_context("create a temp
  file for the sidecar" / "write the sidecar body for" / "publish the
  sidecar", &sidecar_file)`. Because P:io lands **beneath** C1 in the stack,
  C1 must author this block in its post-io form — the `io_context` calls in
  438-453 are C1's lines, not P:io's, even though the vocabulary and the
  `IoContext` import (line 6) are P:io's. P:io's carved branch does not
  contain them (its sweep predates the sidecar existing).
- Same reasoning for `ersatztv_core::sidecar::SIDECAR_SUFFIX` at 444: C1
  moved the sidecar types into `ersatztv-core`, so the whole `sidecar_file`
  computation is C1's.

## 7. `generate_sidecar` (517-542) and `trim_cutoff` (544-579)

| Lines | Region | Owner |
|---|---|---|
| 517-543 | whole `generate_sidecar` fn + trailing blank | **C1** |
| 544-567 | `trim_cutoff` doc comment (24 lines) | **P:trim** |
| 568-577 | `trim_cutoff` body: `served_head` lookup, `.unwrap_or(self.last_segment_end)` | **P:trim** |
| 578 | `served - self.history` | **C6** — this is the *one line* C6 edits inside a body P:trim carries. P:trim's carved form is `served - HISTORY_DURATION` |
| 580 | blank | P:trim |

## 8. `generate_playlist` (581-659) — sub-function split

| Lines | Region | Owner |
|---|---|---|
| 585 | `now: OffsetDateTime,` parameter | **P:trim** |
| 589-595 | version-6 rationale comment + `#EXT-X-VERSION:6` (replaces old 288 `:7`) | **P:hls** |
| 600 | `let horizon = now + PUBLISH_LEAD;` (replaces old 293 `now_utc() + PUBLISH_LEAD`) | **P:trim** |
| 613 | `self.served_head = Some(clamped_ms);` | **P:trim** |
| 633-639 | rfc8216bis 6.2.2 comment + unconditional `#EXT-X-DISCONTINUITY-SEQUENCE` (replaces the old 325-330 `if > 0` block) | **P:hls** |

The monotonic-clamp lines around 609-612 (`candidate_ms`, `clamped_ms`,
`last_served_media_sequence`) are **unchanged from main** — P:trim only appends
613 to that block.

## 9. `get_new_segment_durations` (661-689) and `render_subtitle_segment` (692-734)

| Lines | Region | Owner |
|---|---|---|
| 666-668 | `read_to_string(...).io_context("read the ffmpeg playlist", path)` | **P:io** |
| 700-704 | rfc8216bis 3.1.4 cue-timeline comment | **P:hls** |
| 706-707 | `X-TIMESTAMP-MAP=LOCAL:{}` + `format_vtt_ts(seg_start_src)` (replaces old 390) | **P:hls** |
| (old 400-404, deleted) | `local_start` / `local_end` computation | **P:hls** |
| 719-720 | `format_vtt_ts(cue.start)` / `format_vtt_ts(cue.end)` (replaces old 407-408) | **P:hls** |

## 10. `mod tests` (736-1219)

### 10a. Scaffold and helpers

| Lines | Region | Owner |
|---|---|---|
| 735-738 | blank + `#[cfg(test)] mod tests { use super::*;` | **SHARED SCAFFOLD** — present verbatim in all four of P:io, P:hls, P:heartbeat, P:trim. Whichever P-branch lands first in the stack introduces it; the rest extend it. Not a topic conflict, but a guaranteed textual conflict at every merge |
| 740-753 | `fn manager()` | **P:trim** (only P:trim carries it) |
| 754 | blank | scaffold |
| 755-771 | `fn manager_in(folder: &Path)` + doc | **SHARED: P:io + P:heartbeat**, byte-identical in both carved branches. First of the two to land introduces it |
| 773-776, 778-780 | `fn manager_with_segments` | **P:trim** |
| 777 | `segment(&format!("live{i:06}.ts"), "item-a", i as i64 * 4)` — the `"item-a"` argument | **C1 extends P:trim** |
| 782-787, 789-790 | `fn segment(...)` helper | **SHARED: P:io + P:trim**, byte-identical (2-arg) in both carved branches |
| 782 (`item_id: &str` param), 788 (`item_id: item_id.to_owned()`) | 2-arg → 3-arg growth | **C1 extends P:io + P:trim** |
| 791-798 | `fn source(cues)` | **P:hls** |
| 800-806 | `fn cue(start, end, text)` | **P:hls** |
| 985-1000 (except 995) | `fn window_anchored_at` + doc | **P:trim** |
| 995 | `item_id: String::from("item-a"),` in the `Segment` literal | **C1 extends P:trim** |
| 1002-1009 | `fn expired(m)` + doc | **P:trim** |

### 10b. Every test, with owner

| Lines | Test | Owner | Multi-topic? |
|---|---|---|---|
| 808-830 | `window_is_placed_from_the_wall_clock` | **P:trim** | no |
| 832-860 | `window_recovers_the_live_edge_after_a_long_stall` | **P:trim** | no |
| 861-910 | `sidecar_maps_segments_to_items_and_pipelines_to_offsets` | **C1** | no |
| 912-941 | `pipeline_records_prune_with_their_segments` | **C1** | no |
| 942-959 | `spanning_cue_keeps_its_full_range_in_every_segment` | **P:hls** | no |
| 961-971 | `timestamp_map_anchors_the_source_timeline_to_the_segment` | **P:hls** | no |
| 973-983 | `cue_that_ended_before_the_segment_is_not_emitted` | **P:hls** | no |
| 1011-1021 | `keeps_two_minutes_of_media_behind_the_live_edge` | **P:trim** | no |
| 1023-1036 | `history_survives_a_channel_running_behind_the_wall_clock` | **P:trim** | no |
| 1038-1053 | `history_survives_production_running_ahead_of_the_served_head` | **P:trim** | no |
| 1055-1063 | `history_behind_the_served_head_stays_bounded` | **P:trim** | no |
| 1064-1084 | `variant_history_keeps_the_whole_envelope_alive` | **C6** | no — but it *depends on* P:trim's `window_anchored_at`/`expired` and on C1's `item_id` field |
| 1086-1099 | `variant_history_still_bounds_a_long_running_item` | **C6** | no (same dependency) |
| 1101-1107 | `nothing_is_trimmed_before_the_window_fills` | **P:trim** | no |
| 1108-1134 | floating io doc comment + `scanning_a_missing_segment_folder_names_the_folder` | **P:io** | no |
| 1136-1153 | `a_missing_heartbeat_arms_the_idle_timeout_after_the_grace` | **P:heartbeat** | no |
| 1155-1166 | `a_missing_heartbeat_within_the_grace_does_not_time_out` | **P:heartbeat** | no |
| 1168-1186 | `a_heartbeat_that_reappears_rearms_the_grace` | **P:heartbeat** | no |
| 1188-1218 | `trimming_a_segment_whose_file_is_gone_names_the_segment` | **P:io introduces; P:trim extends; C1 extends** | **YES — 3 owners** |

**Multi-topic test detail, 1188-1218.** P:io introduces the whole test. Two
later layers edit its body:

- **P:trim extends** with lines 1201-1202:
  `manager.last_segment_end = OffsetDateTime::UNIX_EPOCH + HISTORY_DURATION +
  Duration::from_secs(60);`
  This is *load-bearing*, not cosmetic. In P:io's main-based form the cutoff
  is `now_utc() - 2min`, so a segment stamped at `UNIX_EPOCH` expires for
  free. Once P:trim measures the cutoff from the served head (and the live
  edge stands in before the first render), the segment no longer expires and
  the test stops reaching the `remove_file` failure it exists to pin. P:trim
  **must** add these two lines when it lands on top of P:io, or the P:io test
  silently stops testing anything.
- **C1 extends** line 1200: `segment("live000042.ts", "item-a", 0)` — the
  third argument, from the 2-arg → 3-arg helper growth.

The same 2-arg → 3-arg growth also touches P:trim's `manager_with_segments`
(777) and `window_anchored_at` (995); those are mechanical.

---

## (a) Trim math: what P:trim already carries vs what C6 layers on

Verified against `git diff main...rebuilt/segment-trim-served-head`.

**P:trim carries, in main-based form (240 ins / 4 del), with the plain
pre-C6 constant:**

- `HISTORY_DURATION` doc + `const HISTORY_DURATION: Duration =
  Duration::from_secs(120);`
- `served_head: Option<u64>` field + full doc + `served_head: None` in `new`
- the entire `trim_cutoff()` fn including its 24-line doc — body ends
  `served - HISTORY_DURATION` (a plain module constant read, no `self`)
- `let cutoff = self.trim_cutoff();` in `update`
- the `now: OffsetDateTime` parameter on `generate_playlist`, all three call
  sites passing `OffsetDateTime::now_utc()`, `let horizon = now +
  PUBLISH_LEAD;`, and `self.served_head = Some(clamped_ms);`
- test helpers `manager`, `manager_with_segments`, `segment` (2-arg),
  `window_anchored_at` (no `item_id`), `expired`
- 7 tests: `window_is_placed_from_the_wall_clock`,
  `window_recovers_the_live_edge_after_a_long_stall`,
  `keeps_two_minutes_of_media_behind_the_live_edge`,
  `history_survives_a_channel_running_behind_the_wall_clock`,
  `history_survives_production_running_ahead_of_the_served_head`,
  `history_behind_the_served_head_stays_bounded`,
  `nothing_is_trimmed_before_the_window_fills`

P:trim does **not** carry: `VARIANT_HISTORY_DURATION`, the const assert, the
`history` field, `set_history_duration`, the extended-trim warn, or the two
variant tests. Its 4 deletions are old 15 (`// 12s`), old 134 (the
`generate_playlist` call it rewraps), old 229 (the wall-clock cutoff), old
293 (the wall-clock horizon).

**C6 layers on top of that, exactly:**

1. `VARIANT_HISTORY_DURATION` const + 19-line STOPGAP doc (24-42)
2. the cross-module contract comment + `const _: () = assert!(...)` (44-73)
3. `history: Duration` + `extended_trim_warned: bool` fields (120-125) and
   their `new()` initializers (190-191)
4. `set_history_duration` + doc (204-210)
5. **one-line edit** at 578: `served - HISTORY_DURATION` → `served -
   self.history`. This is the only place C6 rewrites a line P:trim carries.
6. the extended-trim warn block inside the trim loop (381-389), which reads
   `self.history != HISTORY_DURATION` — i.e. it compares against the constant
   P:trim owns, so it cannot exist below C6's field
7. two tests: `variant_history_keeps_the_whole_envelope_alive` (1064-1084),
   `variant_history_still_bounds_a_long_running_item` (1086-1099), both built
   on P:trim's `window_anchored_at` + `expired` helpers

C6 adds nothing to `generate_playlist` and nothing to the served-head
placement. The wall-clock window and the anti-ratchet property are entirely
P:trim's.

---

## DISPUTED

**D1 — line 20, the `PUBLISH_LEAD` doc rewrite.**
Target has `/// How far past the wall clock the published window reaches.
Twelve seconds.` replacing main's `// 12s` (old 15). No carved branch
contains it: P:trim adds `HISTORY_DURATION` directly above and **keeps**
`// 12s` verbatim. Provenance traced to merge commit 8d40c88 ("merge: adopt
upstream wall-clock publish window over the 1x paced head"); it is
fork-authored inside the conflict resolution, not inherited from upstream
(upstream f059e8f is already an ancestor of main and main still reads
`// 12s`).
Reasoning for the dispute: topically it belongs to P:trim (P:trim is the
layer that makes the window wall-clock placed on every render, which is what
the sentence describes), but P:trim as carved does not carry it, so
recomposing P:trim as-is leaves this 1 line owner-less. C6 is the wrong home
— it is not about retention.
**Recommendation:** fold into P:trim (one-line amend to the carved branch).
Second choice: drop it and restore `// 12s`, which costs nothing functional.
Do not park it on C6.

**D2 — the const assert (44-73), earliest legal layer.**
Resolved in favour of C6 per the seed, but recording the tension: the assert
compiles as soon as `composer` exists, i.e. from **C2** onward
(`HARD_LAG_SEGMENTS`, `SERVED_SEGMENTS`, `SEGMENT_SECONDS` are all plain
`pub const`s in `composer.rs` and none is part of C6's `served_window`
carve-out). It constrains `HISTORY_DURATION`, which is P:trim's, yet it can
never live in P:trim. C6 is the right home on topic and is safe; C2 is the
floor if it ever needs to move earlier.

**D3 — ownership of `io_context` calls inside C1-introduced blocks
(438-453).**
These read as P:io vocabulary but are C1 lines: P:io's carved branch cannot
contain them because the sidecar does not exist at that layer. Recorded so a
later audit does not "reunify" them into P:io. Same shape applies to
`SIDECAR_SUFFIX` at 444.

**D4 — shared test scaffolding (735-738, 755-771, 782-790).**
Not a topic dispute but a merge hazard: `mod tests`/`use super::*` is
duplicated across four carved P-branches; `manager_in` across two; `segment`
across two. Each pair/quad is byte-identical, so the resolution is always
"keep one", but every stack rebuild will surface these as conflicts. Decide
the P-branch landing order once and let the first branch own the scaffold.

## UNASSIGNED

**U1 — lines 219-226, `pub fn last_segment_end(&self) -> OffsetDateTime` and
its 4-line doc.**
The brief's owner list for this file
({P:io, P:hls, P:heartbeat, P:trim, C1, C6}) has no home for it. It is not
unassignable, though: `git diff main...rebuilt/timeline-drift-pad-and-trim --
crates/ersatztv-channel/src/playlist_manager.rs` is *exactly* these 8 lines
and nothing else. Its only consumer is
`crates/ersatztv-channel/src/channel_session.rs:979`, inside the
`stamp_error_ms` computation, which the seed assigns to
`rebuilt/timeline-drift-pad-and-trim` (and which commit 764ac9c reworked into
a named fn + 4 tests). **Owner: P:drift** (`rebuilt/timeline-drift-pad-and-trim`).
The seed's stale topic table for `playlist_manager.rs` also omits it — this
is a gap in the inherited table, not a new region. 8 added lines, 0 deleted.

**U2 — three blank separator lines (119, 189, 861).**
119 and 189 separate C1's field/init block from C6's; 861 separates P:trim's
last window test from C1's first sidecar test. Assigned to C1 above by the
"introduces the gap" rule. Zero semantic content; listed only so the
completeness arithmetic below closes.

Nothing else in the diff is unattributable.

---

## Changed-line completeness check

Computed from `git diff main...eb5d808 -U0` hunk headers (42 hunks), then
every added line number assigned to exactly one owner range.

**Insertions: 841 total, 841 assigned, 0 unassigned.**

| Owner | Added lines | Regions |
|---|---|---|
| P:trim | 227 | 24 |
| P:io | 168 | 16 |
| C1 | 153 | 16 |
| C6 | 114 | 9 |
| P:heartbeat | 83 | 9 |
| P:hls | 81 | 10 |
| P:drift | 8 | 1 |
| shared scaffold (`mod tests` + closing brace + one blank) | 6 | 3 |
| DISPUTED D1 (`PUBLISH_LEAD` doc) | 1 | 1 |
| **Total** | **841** | **89** |

Overlap: exactly **one** added line, 578, is a C6 rewrite of a line P:trim
carries in a different form (`self.history` vs `HISTORY_DURATION`); it is
counted once, under C6.

**Deletions: 44 total, 44 assigned, 0 unassigned.**

| Owner | Deleted (old) lines | Which |
|---|---|---|
| P:io | 24 | old 6 (import), 135-137, 146, 213-215, 222-224, 239, 246, 253-256, 263-265, 268, 273-274, 357 |
| P:hls | 15 | old 288 (`VERSION:7`), 325-330 (`if > 0` block), 390 (`LOCAL:00:00:00.000`), 400-404 (`local_start`/`local_end`), 407-408 |
| P:trim | 3 | old 134 (rewrapped `generate_playlist` call), 229 (wall-clock cutoff), 293 (wall-clock horizon) |
| P:heartbeat | 1 | old 4 (`use std::time::Duration;`) |
| DISPUTED D1 | 1 | old 15 (`// 12s`) |
| **Total** | **44** | |

Cross-check against `--stat`: 841 insertions, 44 deletions. Both close
exactly.
