//! Predicted-vs-observed CSV sink for picks.
//!
//! Fed by `KinferenceService::finalize_observation`, behind the
//! `ReportFirstToken` RPC. Rows are formatted by
//! `observations::format_csv_row`.
//!
//! Behavior:
//! - No path → sink is disabled, `append_line` is a no-op.
//! - Path given, file opens → append-mode `BufWriter` under a
//!   `Mutex` for concurrent streams.
//! - Fresh file (didn't exist OR was empty) → header row is written
//!   exactly once before the first observation row.
//! - Path given, open fails → warn log, sink stays disabled.
//! - Every row is flushed.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::observations::CSV_HEADER;

pub struct ObservationSink {
    inner: Option<Mutex<BufWriter<std::fs::File>>>,
}

impl ObservationSink {
    /// Open the sink at `path` (`KINFERENCE_OBSERVATIONS_CSV`, read once at
    /// startup by `Config::load`); see the module docs for the cases.
    pub fn open(path: Option<PathBuf>) -> Self {
        let Some(path) = path else {
            return Self { inner: None };
        };
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => {
                // Checked after the open, not before, to avoid a TOCTOU
                // race.
                let is_empty = f.metadata().map(|m| m.len() == 0).unwrap_or(false);
                let mut writer = BufWriter::new(f);
                if is_empty {
                    // Best-effort: on failure, keep recording data rows.
                    if let Err(e) = writer
                        .write_all(CSV_HEADER.as_bytes())
                        .and_then(|_| writer.write_all(b"\n"))
                        .and_then(|_| writer.flush())
                    {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "failed to write CSV header; \
                             continuing with data-only sink"
                        );
                    } else {
                        tracing::info!(
                            path = %path.display(),
                            "observation sink enabled (new file; header written)"
                        );
                    }
                } else {
                    tracing::info!(
                        path = %path.display(),
                        "observation sink enabled (appending to existing file)"
                    );
                }
                Self {
                    inner: Some(Mutex::new(writer)),
                }
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "failed to open observation sink; CSV disabled"
                );
                Self { inner: None }
            }
        }
    }

    /// Append one CSV row followed by `\n`. No-op when disabled; errors are
    /// logged and otherwise ignored.
    pub fn append_line(&self, line: &str) {
        let Some(mu) = self.inner.as_ref() else {
            return;
        };
        let Ok(mut w) = mu.lock() else {
            tracing::warn!("observation sink mutex poisoned; skipping write");
            return;
        };
        let res = w
            .write_all(line.as_bytes())
            .and_then(|_| w.write_all(b"\n"))
            .and_then(|_| w.flush());
        if let Err(e) = res {
            tracing::warn!(error = %e, "observation sink write failed");
        }
    }
}

#[cfg(test)]
mod tests;
