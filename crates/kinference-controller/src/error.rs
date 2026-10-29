//! The control plane's error type.

/// Why a control-plane operation failed. `Display` says what failed; the
/// cause is reachable through `source`.
#[derive(Debug)]
pub enum Error {
    /// No in-cluster service-account credentials for discovery's client.
    InCluster(kube::config::InClusterError),
    /// Neither in-cluster credentials nor a usable kubeconfig.
    Infer(kube::config::InferConfigError),
    /// Building a `kube::Client`; `what` names the config it came from.
    Client {
        what: &'static str,
        source: kube::Error,
    },
    /// An API server call failed; `op` names it.
    Api {
        op: &'static str,
        source: kube::Error,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InCluster(_) => f.write_str(
                "failed to read in-cluster service-account credentials; discovery needs \
                 kinference to run as a pod in the cluster that it watches",
            ),
            Error::Infer(_) => f.write_str(
                "failed to build a Kubernetes client: no in-cluster token and no usable \
                 kubeconfig; turn off kubernetes.enabled and policy.enabled to run without \
                 a cluster",
            ),
            Error::Client { what, .. } => write!(f, "building a Kubernetes client from {what}"),
            Error::Api { op, .. } => f.write_str(op),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::InCluster(source) => Some(source),
            Error::Infer(source) => Some(source),
            Error::Client { source, .. } => Some(source),
            Error::Api { source, .. } => Some(source),
        }
    }
}
