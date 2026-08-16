use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use ersatztv_clock_probe::checks::{Limits, Severity};
use ersatztv_clock_probe::{checks, render, timeline};

#[derive(Parser, Debug)]
#[command(version = ersatztv_core::VERSION, about = "Read a channel's clock trace", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// What the trace covers
    Summary {
        #[arg(required = true, num_args = 1..)]
        paths: Vec<PathBuf>,
        #[arg(long)]
        channel: Option<String>,
    },
    /// One row per pipeline, every domain on the same line
    Items {
        #[arg(required = true, num_args = 1..)]
        paths: Vec<PathBuf>,
        #[arg(long)]
        channel: Option<String>,
    },
    /// One row per segment, the emitted clock at its finest grain
    Segments {
        #[arg(required = true, num_args = 1..)]
        paths: Vec<PathBuf>,
        #[arg(long)]
        channel: Option<String>,
    },
    /// Each crossing between two clocks, and what it measured
    Crossings {
        #[arg(required = true, num_args = 1..)]
        paths: Vec<PathBuf>,
        #[arg(long)]
        channel: Option<String>,
    },
    /// Test the invariants and exit non zero on a failure
    Check {
        #[arg(required = true, num_args = 1..)]
        paths: Vec<PathBuf>,
        #[arg(long)]
        channel: Option<String>,
        /// How far the emitted clock may stand from the schedule cursor over
        /// the whole run
        #[arg(long, default_value_t = 2_000)]
        max_drift_ms: i128,
        /// How fast that gap may open, per hour of schedule. A rate catches a
        /// ratchet that any tolerable absolute limit would let through.
        #[arg(long, default_value_t = 250)]
        max_drift_rate_ms_per_hour: i128,
        /// How little retained history is tolerable
        #[arg(long, default_value_t = 36_000)]
        min_retention_ms: i128,
    },
}

impl Command {
    fn paths(&self) -> &[PathBuf] {
        match self {
            Command::Summary { paths, .. }
            | Command::Items { paths, .. }
            | Command::Segments { paths, .. }
            | Command::Crossings { paths, .. }
            | Command::Check { paths, .. } => paths,
        }
    }

    fn channel(&self) -> Option<&str> {
        match self {
            Command::Summary { channel, .. }
            | Command::Items { channel, .. }
            | Command::Segments { channel, .. }
            | Command::Crossings { channel, .. }
            | Command::Check { channel, .. } => channel.as_deref(),
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();

    let timelines = match timeline::load(args.command.paths()) {
        Ok(timelines) => timelines,
        Err(err) => {
            eprintln!("cannot read the trace: {err}");
            return ExitCode::FAILURE;
        }
    };

    let selected: Vec<_> = timelines
        .into_iter()
        .filter(|t| args.command.channel().is_none_or(|c| t.channel == c))
        .collect();

    if selected.is_empty() {
        eprintln!("no records matched");
        return ExitCode::FAILURE;
    }

    let mut worst = Severity::Info;

    for timeline in &selected {
        match &args.command {
            Command::Summary { .. } => print!("{}", render::summary(timeline)),
            Command::Items { .. } => print!("{}", render::items(timeline)),
            Command::Segments { .. } => print!("{}", render::segments(timeline)),
            Command::Crossings { .. } => print!("{}", render::crossings(timeline)),
            Command::Check {
                max_drift_ms,
                max_drift_rate_ms_per_hour,
                min_retention_ms,
                ..
            } => {
                let limits = Limits {
                    max_drift_ms: *max_drift_ms,
                    max_drift_rate_ms_per_hour: *max_drift_rate_ms_per_hour,
                    min_retention_ms: *min_retention_ms,
                };
                let findings = checks::run(timeline, limits);
                println!("channel {}", timeline.channel);
                print!("{}", render::findings(&findings));
                worst = worst.max(render::worst(&findings));
            }
        }
        println!();
    }

    if worst == Severity::Fail {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
