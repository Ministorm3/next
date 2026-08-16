//! The views.
//!
//! Every table puts each column under the letter of the clock it reads, so a
//! row can be scanned across domains without holding the map in your head.

use time::{Duration, OffsetDateTime};

use crate::checks::{Finding, Severity, ceil_law_ms};
use crate::timeline::Timeline;

/// A time as an offset from the run's origin, `+H:MM:SS.mmm`.
pub fn rel(at: OffsetDateTime, origin: OffsetDateTime) -> String {
    let delta = at - origin;
    let sign = if delta.is_negative() { '-' } else { '+' };
    let total = delta.abs();
    format!(
        "{sign}{}:{:02}:{:02}.{:03}",
        total.whole_hours(),
        total.whole_minutes() % 60,
        total.whole_seconds() % 60,
        total.subsec_milliseconds().abs(),
    )
}

fn signed_ms(delta: Duration) -> String {
    format!("{:+}", delta.whole_milliseconds())
}

/// The main table. One row per pipeline, every domain on the same line.
///
/// `drift` is the emitted clock measured against the schedule cursor with the
/// virtual start offset added back. `step` is what the previous item
/// contributed to it, which is where a defect shows up as a repeating value
/// rather than as noise.
pub fn items(timeline: &Timeline) -> String {
    let mut out = String::new();

    let Some(origin) = timeline.origin.as_ref() else {
        return String::from(
            "no session record in this trace, so there is no origin to read from\n",
        );
    };

    out.push_str(&format!(
        "channel {}, origin S {} E {}, virtual start offset {}ms\n\n",
        timeline.channel,
        origin.s_origin,
        origin.e_origin,
        origin.start_time_offset.whole_milliseconds(),
    ));

    out.push_str(&format!(
        "{:>4} {:<22} {:<17} {:>8} {:>13} {:>13} {:>8} {:>8} {:>8} {:>8} {:>7} {:>7} {:>8} {:>10}\n",
        "#",
        "C source",
        "state",
        "W lead",
        "S cursor",
        "E stamp",
        "drift",
        "step",
        "C slot",
        "E emit",
        "err",
        "pred",
        "P off",
        "Q ms/held",
    ));

    let mut previous_drift: Option<i128> = None;

    for pipeline in &timeline.pipelines {
        let Some(schedule) = pipeline.s_transcoded_until else {
            continue;
        };

        let drift = timeline
            .stamp_error(pipeline.e_at_start, schedule)
            .whole_milliseconds();
        let step = match previous_drift {
            Some(previous) => format!("{:+}", drift - previous),
            None => String::from("-"),
        };
        previous_drift = Some(drift);

        // how far production stands ahead of real time; the virtual start
        // offset cancels because both sides carry it
        let lead = schedule - (pipeline.w_start + timeline.offset());

        let slot = pipeline.c_slot_ms();
        let emitted = timeline.emitted_by(pipeline.seq);
        let consumed = pipeline
            .s_finish
            .map(|finish| finish - schedule)
            .unwrap_or(Duration::ZERO);
        let err = emitted - consumed;
        let pred = slot
            .zip(pipeline.fps())
            .and_then(|(slot, fps)| ceil_law_ms(slot, fps));

        out.push_str(&format!(
            "{:>4} {:<22} {:<17} {:>7.1}s {:>13} {:>13} {:>8} {:>8} {:>8} {:>8} {:>7} {:>7} {:>8} {:>10}\n",
            pipeline.seq,
            pipeline.label(),
            pipeline.state.as_deref().unwrap_or("-"),
            lead.as_seconds_f64(),
            rel(schedule, origin.s_origin),
            rel(pipeline.e_at_start, origin.e_origin),
            format!("{drift:+}"),
            step,
            slot.map(|s| s.to_string()).unwrap_or_else(|| String::from("-")),
            if emitted == Duration::ZERO { String::from("-") } else { emitted.whole_milliseconds().to_string() },
            if emitted == Duration::ZERO { String::from("-") } else { signed_ms(err) },
            pred.map(|p| format!("{p:+.0}")).unwrap_or_else(|| String::from("-")),
            pipeline.p_pts_offset_ms.map(|p| p.to_string()).unwrap_or_else(|| String::from("-")),
            format!("{}/{}", pipeline.q_media_sequence, pipeline.q_segments_held),
        ));
    }

    out
}

/// One row per segment. The emitted clock at its finest grain.
pub fn segments(timeline: &Timeline) -> String {
    let mut out = String::new();

    let Some(origin) = timeline.origin.as_ref() else {
        return String::from("no session record in this trace\n");
    };

    if timeline.segments.is_empty() {
        return String::from("no segment records; rerun at level segments or all\n");
    }

    out.push_str(&format!(
        "{:>8} {:<16} {:>13} {:>13} {:>9} {:>9} {:>9} {:>13} {:>3} {:>5}\n",
        "Q ms",
        "Q file",
        "W arrival",
        "E stamp",
        "E dur",
        "W-E lag",
        "P off",
        "P mpegts",
        "D",
        "pipe",
    ));

    for segment in &timeline.segments {
        // how far behind real time the stamp on this segment already was
        let lag = segment.w_utc - segment.e_program_date_time;

        out.push_str(&format!(
            "{:>8} {:<16} {:>13} {:>13} {:>9.3} {:>8.1}s {:>9} {:>13} {:>3} {:>5}\n",
            segment.q_media_sequence,
            segment.q_path,
            rel(segment.w_utc, origin.w_local),
            rel(segment.e_program_date_time, origin.e_origin),
            segment.e_duration_s,
            lag.as_seconds_f64(),
            segment.p_pts_offset_ms,
            segment.p_mpegts_90khz,
            if segment.discontinuity { "yes" } else { "" },
            segment
                .pipeline_seq
                .map(|s| s.to_string())
                .unwrap_or_else(|| String::from("-")),
        ));
    }

    out
}

/// Each crossing named, with what it measured over this run.
pub fn crossings(timeline: &Timeline) -> String {
    let mut out = format!("channel {}\n\n", timeline.channel);

    let leads: Vec<f64> = timeline
        .pipelines
        .iter()
        .filter_map(|p| {
            let schedule = p.s_transcoded_until?;
            Some((schedule - (p.w_start + timeline.offset())).as_seconds_f64())
        })
        .collect();
    out.push_str(&crossing(
        "S against W, work ahead depth",
        "sound, the virtual start offset cancels because both sides carry it",
        &match (
            leads.iter().cloned().fold(f64::INFINITY, f64::min),
            leads.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        ) {
            (min, max) if min.is_finite() => {
                format!("{} pipelines, lead {min:.1}s to {max:.1}s", leads.len())
            }
            _ => String::from("no pipelines carried both readings"),
        },
    ));

    let horizon_breaches = timeline
        .publishes
        .iter()
        .filter(|p| p.e_head_pdt.is_some_and(|head| head > p.w_horizon))
        .count();
    out.push_str(&crossing(
        "E against W, publish horizon",
        "sound, the one deliberate re-coupling of emitted time to real time",
        &format!(
            "{} windows, {} published past the horizon",
            timeline.publishes.len(),
            horizon_breaches
        ),
    ));

    let resumed = timeline
        .pipelines
        .iter()
        .filter(|p| !p.start_at_zero && !p.is_live)
        .count();
    out.push_str(&crossing(
        "S into C, item progress into a source position",
        "sound only while the source plays at 1x from its in point, which is why a live source \
         never takes this path",
        &format!("{resumed} pipelines resumed an item mid slot"),
    ));

    let quantized = timeline
        .pipelines
        .iter()
        .filter_map(|p| p.c_slot_ms().zip(p.fps()))
        .filter_map(|(slot, fps)| ceil_law_ms(slot, fps))
        .sum::<f64>();
    out.push_str(&crossing(
        "C into E, source interval into an emitted duration",
        "sound as intent, inexact upward; the cut emits the straddling frame whole",
        &format!("predicted overshoot totals {quantized:+.0}ms across the run"),
    ));

    out.push_str(&crossing(
        "Q into E, segment ordering",
        "sound while a file name sort gives emission order",
        &format!("{} segments", timeline.segments.len()),
    ));

    let deepest_lag = timeline
        .trims
        .iter()
        .map(|t| (t.w_utc - t.e_program_date_time).whole_milliseconds())
        .max();
    out.push_str(&crossing(
        "E against W, the segment trim",
        "NOT SOUND; retained history is the budget minus the channel's lag, and zero once the lag \
         reaches the budget",
        &match deepest_lag {
            Some(lag) => format!(
                "{} trims, deepest lag behind real time {lag}ms",
                timeline.trims.len()
            ),
            None => String::from("nothing was trimmed"),
        },
    ));

    out
}

fn crossing(name: &str, soundness: &str, measured: &str) -> String {
    format!("{name}\n  {soundness}\n  measured: {measured}\n\n")
}

/// The check results, worst first.
pub fn findings(findings: &[Finding]) -> String {
    let mut sorted: Vec<&Finding> = findings.iter().collect();
    sorted.sort_by_key(|f| std::cmp::Reverse(f.severity));

    let mut out = String::new();
    for finding in sorted {
        out.push_str(&format!(
            "{} {:<16} {}\n",
            finding.severity.tag(),
            finding.check,
            finding.message
        ));
    }
    out
}

/// A few lines saying what the trace covers, so a reader knows what the rest
/// of the output is a view of.
pub fn summary(timeline: &Timeline) -> String {
    let mut out = format!("channel {}\n", timeline.channel);

    match timeline.span() {
        Some((first, last)) => out.push_str(&format!(
            "  covers {} to {}, {:.1} minutes\n",
            first,
            last,
            (last - first).as_seconds_f64() / 60.0
        )),
        None => out.push_str("  no records\n"),
    }

    out.push_str(&format!(
        "  {} pipelines, {} segments, {} trims, {} publishes\n",
        timeline.pipelines.len(),
        timeline.segments.len(),
        timeline.trims.len(),
        timeline.publishes.len(),
    ));

    if let Some(origin) = timeline.origin.as_ref() {
        out.push_str(&format!(
            "  virtual start offset {}ms, segment target {}s\n",
            origin.start_time_offset.whole_milliseconds(),
            origin.segment_seconds,
        ));
    }

    out
}

/// The exit code the check command should use.
pub fn worst(findings: &[Finding]) -> Severity {
    findings
        .iter()
        .map(|f| f.severity)
        .max()
        .unwrap_or(Severity::Info)
}
