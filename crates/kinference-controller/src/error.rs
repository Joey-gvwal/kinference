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

impl Error {
    /// The error and its causes, joined by ": ", for log lines.
    pub(crate) fn chain(&self) -> String {
        let mut out = self.to_string();
        let mut cause = std::error::Error::source(self);
        while let Some(e) = cause {
            out.push_str(": ");
            out.push_str(&e.to_string());
            cause = e.source();
        }
        out
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InCluster(_) => f.write_str(
                "failed to read in-cluster Kubernetes service-account credentials. \
                 KubeDiscovery only works when kinference runs as a pod inside the \
                 cluster it should watch (~/.kube/config is intentionally NOT honored: \
                 discovered pod IPs would not be reachable from outside the cluster \
                 network)",
            ),
            Error::Infer(_) => f.write_str(
                "failed to build a Kubernetes client (kubernetes.enabled or policy.enabled is \
                 set): no in-cluster service-account token and no usable kubeconfig. Turn both \
                 off to run without a cluster",
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
