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
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ReadWorkersFile { source, .. } => Some(source),
            Error::ParseWorkersFile { source, .. } => Some(source),
            Error::KubeDiscoveryUnavailable => None,
        }
    }
}
