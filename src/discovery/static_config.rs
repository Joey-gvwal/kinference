//! `StaticDiscovery`: emits one `Added` per worker from a worker set's
//! `workers_file`, then waits on `cancel`. Workers never disappear in static
//! mode.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::WorkerConfig;
use crate::discovery::{Discovery, WorkerChange};

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
