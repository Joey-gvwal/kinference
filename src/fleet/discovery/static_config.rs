//! `StaticDiscovery`: emits one `Added` per worker from a worker set's
//! `workers_file`, then waits on `cancel`. Workers never disappear in static
//! mode.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::fleet::discovery::{Discovery, WorkerChange, WorkerConfig};
use crate::fleet::error::Error;

pub struct StaticDiscovery {
    workers: Vec<WorkerConfig>,
}

impl StaticDiscovery {
    pub fn new(workers: Vec<WorkerConfig>) -> Self {
        Self { workers }
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    async fn run(self: Arc<Self>, tx: mpsc::Sender<WorkerChange>, cancel: CancellationToken) {
        for w in &self.workers {
            // The coordinator drains promptly at startup.
            if tx.send(WorkerChange::Added(w.clone())).await.is_err() {
                tracing::warn!("discovery: receiver dropped before all static workers were Added");
                return;
            }
        }
        tracing::info!(
            workers = self.workers.len(),
            "StaticDiscovery: all configured workers Added; waiting on shutdown"
        );
        // Static membership never changes; park until cancellation.
        cancel.cancelled().await;
        tracing::debug!("StaticDiscovery: cancellation received, exiting");
    }
}

/// A static worker list, loaded from `discovery.workers_file`.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkersFile {
    #[serde(default)]
    pub workers: Vec<WorkerConfig>,
}

impl WorkersFile {
    pub fn load(path: impl AsRef<Path>) -> Result<Vec<WorkerConfig>, Error> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| Error::ReadWorkersFile {
            path: path.to_path_buf(),
            source,
        })?;
        let wf: Self = toml::from_str(&text).map_err(|source| Error::ParseWorkersFile {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(wf.workers)
    }
}
