//! Reads a channel worker's clock trace and says what each clock was doing.
//!
//! The worker records readings. This crate does the arithmetic, which is the
//! half that has historically been wrong: every defect in the ledger was a
//! comparison between two clocks that looked like a comparison within one.
//! Keeping the formulas here means a wrong one is a rerun rather than a
//! rebuild and a redeploy, and it means the correction is reviewable next to
//! the rule it implements.
//!
//! The record types come from `ersatztv-core`, the same definitions the worker
//! serializes, so the emitter and the reader cannot drift apart.

pub mod checks;
pub mod render;
pub mod timeline;

pub use checks::{Finding, Limits, Severity};
pub use timeline::{Timeline, load};
