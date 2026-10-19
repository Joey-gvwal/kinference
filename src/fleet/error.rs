//! The fleet's error type.

use std::path::PathBuf;

/// Why a fleet operation failed. `Display` says what failed; the cause is
/// reachable through `source`.
#[derive(Debug)]
pub enum Error {
    /// `workers_file` could not be read.
    ReadWorkersFile {
        path: PathBuf,
        source: std::io::Error,
    },
    /// `workers_file` is not a `[[workers]]` TOML file.
    ParseWorkersFile {
        path: PathBuf,
        source: toml::de::Error,
    },
    /// A worker set wants Kubernetes discovery but the server built none.
    KubeDiscoveryUnavailable,
    /// Ingest started for a worker without `zmq_endpoint`.
    NoZmqEndpoint,
    /// A ZMQ call failed; `op` names it.
    Zmq {
        op: &'static str,
        source: zmq::Error,
    },
    /// Registering or waiting on the socket fd with tokio failed.
    Io {
        op: &'static str,
        source: std::io::Error,
    },
    /// A worker's `/metrics` or `/v1/models` request failed.
    Http { url: String, source: reqwest::Error },
    /// A worker answered with a non-2xx status.
    HttpStatus {
        url: String,
        status: reqwest::StatusCode,
    },
    /// A `/metrics` body is not Prometheus text.
    MetricsParse { url: String, source: std::io::Error },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ReadWorkersFile { path, .. } => {
                write!(f, "reading workers file from {}", path.display())
            }
            Error::ParseWorkersFile { path, .. } => {
                write!(f, "parsing workers TOML from {}", path.display())
            }
            Error::KubeDiscoveryUnavailable => f.write_str(
                "worker set needs Kubernetes discovery but no in-cluster client was built at \
                 startup",
            ),
            Error::NoZmqEndpoint => f.write_str("worker has no zmq_endpoint configured"),
            Error::Zmq { op, .. } => write!(f, "zmq: {op}"),
            Error::Io { op, .. } => write!(f, "socket fd: {op}"),
            Error::Http { url, .. } => write!(f, "GET {url}"),
            Error::HttpStatus { url, status } => write!(f, "GET {url}: HTTP {status}"),
            Error::MetricsParse { url, .. } => write!(f, "parsing Prometheus body from {url}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ReadWorkersFile { source, .. } => Some(source),
            Error::ParseWorkersFile { source, .. } => Some(source),
            Error::KubeDiscoveryUnavailable | Error::NoZmqEndpoint | Error::HttpStatus { .. } => {
                None
            }
            Error::Zmq { source, .. } => Some(source),
            Error::Io { source, .. } => Some(source),
            Error::Http { source, .. } => Some(source),
            Error::MetricsParse { source, .. } => Some(source),
        }
    }
}
