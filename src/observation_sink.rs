//! Predicted-vs-observed CSV sink for picks.
//!
//! Fed by `KinferenceService::finalize_observation` (in `monitor.rs`), the
//! shared core behind BOTH the gRPC `Picker.ReportFirstToken` RPC and the
//! pick path — every pick that gets a TTFT observation
//! lands here regardless of which interface served it.
//!
//! Row shape is a near-direct port of gw-pc's
//! `crates/gateway-proxy/src/proxy/observation_sink.rs`, so an
//! operator running kinference alongside the gw-pc WASM plugin (which
//! writes under `GATEWAY_PLUGIN_SINK_PATH`) can point that and
//! kinference's `KINFERENCE_OBSERVATIONS_CSV` at separate files and analyze
//! either with the same offline tooling.
//!
//! Behavior:
//! - Env var unset → sink is `None`, `append_line` is a no-op.
//! - Env var set, file opens → append-mode `BufWriter` under a
//!   `Mutex` for concurrent streams.
//! - Fresh file (didn't exist OR was empty) → header row is written
//!   exactly once before the first observation row.
//! - Env var set, open fails → warn log, sink stays disabled.
//! - Writes flush after every row so rows are durable before the
//!   next request is decided.

use std::env;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::observations::CSV_HEADER;

/// Env var the operator sets to point at the CSV output file. The
/// row format mirrors the gw-pc plugin's so the same offline tooling
/// parses either source; the env var name is distinct so an
/// operator running both surfaces can write to separate files.
const ENV_VAR: &str = "KINFERENCE_OBSERVATIONS_CSV";

pub struct ObservationSink {
    inner: Option<Mutex<BufWriter<std::fs::File>>>,
}

impl ObservationSink {
    /// Open the sink based on the `KINFERENCE_OBSERVATIONS_CSV` env
    /// var. Returns a disabled sink (with a warn log) on open
    /// failure; returns a disabled sink silently if the env var is
    /// unset. Writes a header row when the file is freshly created
    /// (didn't exist or was zero-length); appends silently to an
    /// existing non-empty file so cross-run concatenation stays
    /// clean.
    pub fn from_env() -> Self {
        let Some(path) = env::var_os(ENV_VAR).map(PathBuf::from) else {
            return Self { inner: None };
        };
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => {
                // Detect "newly created or empty" via post-open
                // metadata. Checking after the open (rather than
                // `path.exists()` before) avoids a TOCTOU race where
                // a parallel writer creates the file between the
                // existence check and the open.
                let is_empty = f.metadata().map(|m| m.len() == 0).unwrap_or(false);
                let mut writer = BufWriter::new(f);
                if is_empty {
                    // Header is best-effort: log and continue on
                    // failure so the sink keeps recording data rows
                    // even if the header didn't make it (rows
                    // remain interpretable from the column contract
                    // in observations.rs).
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

    /// Append one CSV row followed by `\n`. No-op when the sink is
    /// disabled. Errors are logged at warn level and the request
    /// continues — the sink is diagnostic, not critical.
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
