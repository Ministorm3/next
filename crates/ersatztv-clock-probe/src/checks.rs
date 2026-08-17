//! The invariants, as executable statements.
//!
//! Each check names a crossing between two clocks and says what has to hold
//! for that crossing to be sound. A check that has never failed on a trace
//! carrying the defect it describes is not yet an instrument, so every check
//! here has a test that feeds it a trace with the defect present.

use time::{Duration, OffsetDateTime};

use crate::timeline::Timeline;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warn,
    Fail,
}

impl Severity {
    pub fn tag(&self) -> &'static str {
        match self {
            Severity::Info => "ok  ",
            Severity::Warn => "warn",
            Severity::Fail => "FAIL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub check: &'static str,
    pub severity: Severity,
    pub message: String,
}

impl Finding {
    fn new(check: &'static str, severity: Severity, message: String) -> Finding {
        Finding {
            check,
            severity,
            message,
        }
    }
}

/// Thresholds, so that a bench run and a live channel can be judged by
/// different standards without changing the checks.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// How far the emitted clock may stand from the schedule cursor over the
    /// whole run before it counts as drift rather than jitter.
    pub max_drift_ms: i128,
    /// How fast that gap may open, in milliseconds per hour of schedule.
    ///
    /// A rate catches what a total cannot. The 2026-08-14 padding regression
    /// was 113ms over seventeen items, which any tolerable absolute limit
    /// lets through, and which is nonetheless a ratchet that runs for as long
    /// as the channel does.
    pub max_drift_rate_ms_per_hour: i128,
    /// How little retained history is tolerable before a client that pauses
    /// risks meeting a deleted segment.
    pub min_retention_ms: i128,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_drift_ms: 2_000,
            max_drift_rate_ms_per_hour: 250,
            // one target duration of slack under the publish window, which is
            // ten four second segments
            min_retention_ms: 36_000,
        }
    }
}

/// How much schedule a run must cover before a drift rate means anything.
///
/// Below this the rate is mostly the extrapolation: a few milliseconds of
/// ordinary jitter over a minute scales to hundreds per hour and looks exactly
/// like the ratchet the rate exists to catch.
const MIN_RATE_WINDOW_HOURS: f64 = 5.0 / 60.0;

pub fn run(timeline: &Timeline, limits: Limits) -> Vec<Finding> {
    let mut findings = Vec::new();

    findings.extend(record_integrity(timeline));
    findings.extend(split_origin(timeline));
    findings.extend(stamp_drift(timeline, limits));
    findings.extend(seek_purity(timeline));
    findings.extend(trim_safety(timeline));
    findings.extend(retention(timeline, limits));
    findings.extend(publish_horizon(timeline));
    findings.extend(sequence_order(timeline));

    findings
}

/// Did every record the writer made reach the file, and did the wall clock
/// stay put while it did.
fn record_integrity(timeline: &Timeline) -> Vec<Finding> {
    let mut findings = Vec::new();

    if timeline.lost_records > 0 {
        findings.push(Finding::new(
            "records",
            Severity::Warn,
            format!(
                "{} records never reached the file, so any total below is a floor",
                timeline.lost_records
            ),
        ));
    }

    if timeline.clock_steps.is_empty() {
        findings.push(Finding::new(
            "wall-clock",
            Severity::Info,
            String::from("the wall clock tracked the monotonic clock throughout"),
        ));
    } else {
        let worst = timeline
            .clock_steps
            .iter()
            .max_by_key(|s| s.skew_ms().abs())
            .expect("checked non empty");
        findings.push(Finding::new(
            "wall-clock",
            Severity::Fail,
            format!(
                "the system clock stepped {} times, worst {:+}ms at {}; every schedule reading \
                 across a step is suspect",
                timeline.clock_steps.len(),
                worst.skew_ms(),
                worst.at
            ),
        ));
    }

    findings
}

/// The two origins, and what reading the raw difference would give.
///
/// This is not a defect on its own. It is the fact that makes the raw
/// difference wrong, so it is reported whenever it applies.
fn split_origin(timeline: &Timeline) -> Vec<Finding> {
    let Some(origin) = timeline.origin.as_ref() else {
        return vec![Finding::new(
            "split-origin",
            Severity::Warn,
            String::from("no session record in this trace, so the origins are unknown"),
        )];
    };

    if origin.start_time_offset == Duration::ZERO {
        return vec![Finding::new(
            "split-origin",
            Severity::Info,
            String::from("no virtual start, so the schedule and emitted origins agree"),
        )];
    }

    vec![Finding::new(
        "split-origin",
        Severity::Warn,
        format!(
            "virtual start shifts the schedule cursor by {:+}ms while the emitted clock keeps the \
             unshifted origin; an uncorrected difference would read {:+}ms of drift that is not \
             there",
            origin.start_time_offset.whole_milliseconds(),
            -origin.start_time_offset.whole_milliseconds(),
        ),
    )]
}

/// The emitted clock measured against the schedule cursor, corrected.
fn stamp_drift(timeline: &Timeline, limits: Limits) -> Vec<Finding> {
    // the first pipelines join an item mid slot and inherit that offset, so
    // they carry a displacement that is not drift
    const BOOT: usize = 2;

    let rows: Vec<(i128, OffsetDateTime)> = timeline
        .pipelines
        .iter()
        .filter_map(|p| {
            let schedule = p.s_transcoded_until?;
            Some((
                timeline
                    .stamp_error(p.e_at_start, schedule)
                    .whole_milliseconds(),
                schedule,
            ))
        })
        .collect();

    if rows.len() <= BOOT + 1 {
        return vec![Finding::new(
            "stamp-drift",
            Severity::Warn,
            format!(
                "only {} pipelines carried both cursors, too few to leave the boot transient",
                rows.len()
            ),
        )];
    }

    let steady = &rows[BOOT..];
    let total = steady[steady.len() - 1].0 - steady[0].0;
    let steps = (steady.len() - 1) as i128;
    let per_item = total / steps.max(1);

    // per hour of schedule, so that a short bench and a day on air are judged
    // by the same number
    let elapsed_hours = (steady[steady.len() - 1].1 - steady[0].1).as_seconds_f64() / 3_600.0;
    let rate = if elapsed_hours > 0.0 {
        (total as f64 / elapsed_hours) as i128
    } else {
        0
    };

    // a rate read off a window this short is mostly the extrapolation. A few
    // milliseconds of ordinary jitter over a minute scales to hundreds per
    // hour, which is indistinguishable from the ratchet the rate exists to
    // catch, so below the floor the total is the only thing judged.
    let rate_is_meaningful = elapsed_hours >= MIN_RATE_WINDOW_HOURS;

    // a defect moves the same way every item; jitter does not. counting the
    // signs separates the two without needing a long run
    let mut up = 0;
    let mut down = 0;
    for pair in steady.windows(2) {
        match (pair[1].0 - pair[0].0).signum() {
            1 => up += 1,
            -1 => down += 1,
            _ => {}
        }
    }
    let ratchet = up.max(down);

    let severity = if total.abs() > limits.max_drift_ms
        || (rate_is_meaningful && rate.abs() > limits.max_drift_rate_ms_per_hour)
    {
        Severity::Fail
    } else {
        Severity::Info
    };

    let rate_note = if rate_is_meaningful {
        format!("{rate:+}ms per hour of schedule")
    } else {
        format!(
            "{rate:+}ms per hour extrapolated, not judged over only {:.0}s of schedule",
            elapsed_hours * 3_600.0
        )
    };

    let mut findings = vec![Finding::new(
        "stamp-drift",
        severity,
        format!(
            "emitted media ran {total:+}ms against the schedule over {steps} steps, {per_item:+}ms \
             per item, {rate_note}; {ratchet} of {steps} steps moved the same way"
        ),
    )];

    // the same number as a reader who has not met the split origin would get,
    // shown only when the two disagree
    if timeline.offset() != Duration::ZERO
        && let Some(pipeline) = timeline.pipelines.last()
        && let Some(schedule) = pipeline.s_transcoded_until
    {
        findings.push(Finding::new(
            "stamp-drift",
            Severity::Info,
            format!(
                "at the last pipeline the corrected reading is {:+}ms and the uncorrected one is \
                 {:+}ms",
                timeline
                    .stamp_error(pipeline.e_at_start, schedule)
                    .whole_milliseconds(),
                timeline
                    .stamp_error_uncorrected(pipeline.e_at_start, schedule)
                    .whole_milliseconds(),
            ),
        ));
    }

    findings
}

/// No measured quantity may reach an input seek.
///
/// The seek is built from the item's own in point plus the schedule progress.
/// Take the progress back out and what remains is the in point, which cannot
/// change while an item plays. If it moves, something measured has entered a
/// path that only schedule arithmetic is allowed to reach.
fn seek_purity(timeline: &Timeline) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut bases: Vec<(String, i64, u64)> = Vec::new();

    for pipeline in &timeline.pipelines {
        if pipeline.is_live || pipeline.start_at_zero {
            continue;
        }
        if let Some(base) = pipeline.c_base_in_point_ms() {
            bases.push((pipeline.item_id.clone(), base, pipeline.seq));
        }
    }

    let mut drifted = 0;
    for window in bases.windows(2) {
        let (previous_id, previous_base, _) = &window[0];
        let (id, base, seq) = &window[1];
        if previous_id != id {
            continue;
        }
        // a millisecond of slack absorbs the schedule cursor's own rounding
        if (base - previous_base).abs() > 1 {
            drifted += 1;
            findings.push(Finding::new(
                "seek-purity",
                Severity::Fail,
                format!(
                    "pipeline {seq} of item {id} seeks from a base of {base}ms where the previous \
                     pipeline used {previous_base}ms; a measured value has reached the seek"
                ),
            ));
        }
    }

    if drifted == 0 {
        findings.push(Finding::new(
            "seek-purity",
            Severity::Info,
            format!(
                "every seek across {} resumed pipelines came from schedule arithmetic alone",
                bases.len()
            ),
        ));
    }

    findings
}

/// What the trim costs, measured rather than argued.
///
/// The trim compares an emitted stamp against a wall clock cutoff. When the
/// channel runs behind, retained history is the budget minus the lag, and once
/// the lag reaches the budget the loop deletes segments a client is still
/// entitled to ask for.
fn trim_safety(timeline: &Timeline) -> Vec<Finding> {
    let mut findings = Vec::new();

    if timeline.trims.is_empty() {
        return vec![Finding::new(
            "trim-safety",
            Severity::Info,
            String::from("nothing was trimmed in this trace"),
        )];
    }

    // the oldest stamp a client could have been handed, as of each trim
    let mut published_tail = None;
    let mut publishes = timeline.publishes.iter().peekable();
    let mut served_deletions = 0;

    for trim in &timeline.trims {
        while publishes.peek().is_some_and(|p| p.w_utc <= trim.w_utc) {
            let publish = publishes.next().expect("peeked");
            if let Some(tail) = publish.e_tail_pdt {
                published_tail = Some(tail);
            }
        }

        if let Some(tail) = published_tail
            && trim.e_program_date_time >= tail
        {
            served_deletions += 1;
            if served_deletions == 1 {
                findings.push(Finding::new(
                    "trim-safety",
                    Severity::Fail,
                    format!(
                        "{} was deleted while still at or inside the published window, so a client \
                         holding that playlist gets a 404",
                        trim.q_path
                    ),
                ));
            }
        }
    }

    if served_deletions > 1 {
        findings.push(Finding::new(
            "trim-safety",
            Severity::Fail,
            format!("{served_deletions} segments in total were deleted from inside the window"),
        ));
    }

    if served_deletions == 0 {
        findings.push(Finding::new(
            "trim-safety",
            Severity::Info,
            format!(
                "all {} trims removed segments already behind the published window",
                timeline.trims.len()
            ),
        ));
    }

    // which trim this build runs, read from the trace rather than assumed
    let on_wall = timeline
        .trims
        .iter()
        .filter(|t| t.w_cutoff.is_some())
        .count();
    let on_emitted = timeline
        .trims
        .iter()
        .filter(|t| t.e_trim_cutoff.is_some())
        .count();

    if on_wall > 0 {
        findings.push(Finding::new(
            "trim-domain",
            Severity::Warn,
            format!(
                "{on_wall} trims measured an emitted stamp against the wall clock, so retained \
                 history is the budget minus this channel's lag and reaches zero once the lag \
                 reaches the budget"
            ),
        ));
    }

    if on_emitted > 0 {
        findings.push(Finding::new(
            "trim-domain",
            Severity::Info,
            format!(
                "{on_emitted} trims measured against the served position, keeping both sides on \
                 the emitted clock"
            ),
        ));
    }

    findings
}

/// How much history the channel actually kept, and what ate it.
///
/// Retained history has to be read from what is held, not from what was
/// deleted. A trimmed segment is older than the cutoff by definition, so its
/// age is always at least the whole budget and says nothing at all about how
/// much was left behind it. Measuring the deletions instead of the holdings
/// reports every healthy channel as starved.
///
/// The quantity that does erode is the lag of the live edge behind real time.
/// The cutoff walks forward with the wall clock while the stamps do not, so
/// retention is the budget minus that lag, and zero once the lag reaches it.
fn retention(timeline: &Timeline, limits: Limits) -> Vec<Finding> {
    // until the first trim, held history is simply everything the channel has
    // made and is still growing toward the budget. Judging it then reports a
    // channel that has discarded nothing as having discarded too much, which
    // is what a short run does.
    if timeline.trims.is_empty() {
        return vec![Finding::new(
            "retention",
            Severity::Info,
            String::from(
                "nothing has been trimmed yet, so held history is still filling and says nothing \
                 about what this channel retains",
            ),
        )];
    }

    let held: Vec<(i128, i128)> = timeline
        .publishes
        .iter()
        .filter_map(|p| {
            let oldest = p.e_oldest_pdt?;
            Some((
                (p.e_last_segment_end - oldest).whole_milliseconds(),
                (p.w_utc - p.e_last_segment_end).whole_milliseconds(),
            ))
        })
        .collect();

    if held.is_empty() {
        return vec![Finding::new(
            "retention",
            Severity::Warn,
            String::from("no publish records carried a holdings reading, so rerun at level all"),
        )];
    }

    // ignore the fill at startup, when little has been emitted yet and a small
    // holding is expected rather than a symptom
    let settled: Vec<(i128, i128)> = held
        .iter()
        .copied()
        .skip(held.len() / 4)
        .filter(|(span, _)| *span > 0)
        .collect();
    let sample = if settled.is_empty() { &held } else { &settled };

    let worst_span = sample.iter().map(|(span, _)| *span).min().unwrap_or(0);
    let worst_lag = sample.iter().map(|(_, lag)| *lag).max().unwrap_or(0);

    let severity = if worst_span < limits.min_retention_ms {
        Severity::Fail
    } else {
        Severity::Info
    };

    vec![Finding::new(
        "retention",
        severity,
        format!(
            "held history bottomed out at {worst_span}ms, with the live edge at worst {worst_lag}ms \
             behind real time"
        ),
    )]
}

/// Nothing may be published ahead of the horizon.
fn publish_horizon(timeline: &Timeline) -> Vec<Finding> {
    if timeline.publishes.is_empty() {
        return vec![Finding::new(
            "publish-horizon",
            Severity::Warn,
            String::from("no publish records in this trace, so rerun at level all to check it"),
        )];
    }

    let ahead: Vec<&crate::timeline::Publish> = timeline
        .publishes
        .iter()
        .filter(|p| p.e_head_pdt.is_some_and(|head| head > p.w_horizon))
        .collect();

    if ahead.is_empty() {
        let clamped = timeline.publishes.iter().filter(|p| p.clamped()).count();
        return vec![Finding::new(
            "publish-horizon",
            Severity::Info,
            format!(
                "{} windows all stayed behind the horizon; the monotonic clamp held {} of them back",
                timeline.publishes.len(),
                clamped
            ),
        )];
    }

    vec![Finding::new(
        "publish-horizon",
        Severity::Fail,
        format!(
            "{} windows published a segment stamped past the horizon",
            ahead.len()
        ),
    )]
}

/// Sequence counters only ever go up, and file names sort into emission order.
fn sequence_order(timeline: &Timeline) -> Vec<Finding> {
    let mut findings = Vec::new();

    let mut previous_ms = 0u64;
    let mut previous_path = String::new();
    let mut out_of_order = 0;
    let mut regressions = 0;

    for segment in &timeline.segments {
        if segment.q_media_sequence < previous_ms {
            regressions += 1;
        }
        previous_ms = segment.q_media_sequence;

        if !previous_path.is_empty() && segment.q_path <= previous_path {
            out_of_order += 1;
        }
        previous_path = segment.q_path.clone();
    }

    if regressions > 0 {
        findings.push(Finding::new(
            "sequence",
            Severity::Fail,
            format!("the media sequence went backwards {regressions} times"),
        ));
    }

    if out_of_order > 0 {
        findings.push(Finding::new(
            "sequence",
            Severity::Fail,
            format!(
                "{out_of_order} segments have a name that sorts before the one emitted ahead of \
                 them, so a name sort no longer gives emission order"
            ),
        ));
    }

    let clamp_breaks = timeline
        .publishes
        .iter()
        .filter(|p| p.q_clamped < p.q_last_served)
        .count();
    if clamp_breaks > 0 {
        findings.push(Finding::new(
            "sequence",
            Severity::Fail,
            format!("{clamp_breaks} windows started before the previous window did"),
        ));
    }

    if regressions == 0 && out_of_order == 0 && clamp_breaks == 0 {
        findings.push(Finding::new(
            "sequence",
            Severity::Info,
            format!(
                "{} segments kept the media sequence and the name order in step",
                timeline.segments.len()
            ),
        ));
    }

    findings
}

/// The frame quantization law for a padded pipeline.
///
/// A source interval that is not a whole number of frames emits the straddling
/// frame in full, so the emitted duration is the interval rounded up to the
/// next frame boundary. Predicting the overshoot is what separates it from
/// ordinary jitter.
pub fn ceil_law_ms(slot_ms: u64, fps: f64) -> Option<f64> {
    if fps <= 0.0 {
        return None;
    }
    let slot = slot_ms as f64 / 1000.0;
    let frames = (slot * fps - 1e-9).ceil();
    Some((frames / fps - slot) * 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ceil_law_predicts_a_whole_frame_at_most() {
        // a slot one millisecond past a frame boundary at 24000/1001 emits the
        // straddling frame whole
        let fps = 24000.0 / 1001.0;
        let frame_ms = 1000.0 / fps;

        let exact = ceil_law_ms(1_000, fps).expect("a rate");
        assert!(exact >= 0.0 && exact < frame_ms + 0.001);

        // a slot that lands exactly on a frame boundary overshoots by nothing
        let aligned_ms = (frame_ms * 24.0).round() as u64;
        let aligned = ceil_law_ms(aligned_ms, fps).expect("a rate");
        assert!(aligned.abs() < 1.0, "aligned slot overshot by {aligned}ms");
    }

    #[test]
    fn a_rate_of_zero_predicts_nothing() {
        assert!(ceil_law_ms(1_000, 0.0).is_none());
    }
}
