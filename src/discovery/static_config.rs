//! `StaticDiscovery` — wraps the TOML-loaded `Config.workers` list. Emits
//! one `Added` per worker at startup, then waits on `cancel`. Workers
//! never disappear in static mode.

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
            // `send` would block if the channel is full; we expect the
            // coordinator to consume promptly at startup, and the channel
            // is sized for >= 64 backlog, so this is fine. If the receiver
            // hangs up early, log + bail.
            if tx.send(WorkerChange::Added(w.clone())).await.is_err() {
                tracing::warn!("discovery: receiver dropped before all static workers were Added");
                return;
            }
        }
        tracing::info!(
            workers = self.workers.len(),
            "StaticDiscovery: all configured workers Added; waiting on shutdown"
        );
        // Nothing more to do; static membership never changes. Park until
        // cancellation so the spawned task stays alive (some Tokio versions
        // print a warning for tasks that return immediately while held in
        // a JoinHandle).
        cancel.cancelled().await;
        tracing::debug!("StaticDiscovery: cancellation received, exiting");
    }
}
