//! The router's error type.

/// Why a router operation failed. `Display` says what failed; the cause is
/// reachable through `source`.
#[derive(Debug)]
pub enum Error {
    /// Building a reqwest client; `what` names it.
    HttpClient {
        what: &'static str,
        source: reqwest::Error,
    },
    /// A `/render` request failed in flight, or its body could not be read.
    Render { url: String, source: reqwest::Error },
    /// `/render` answered with a non-2xx status.
    RenderStatus {
        status: reqwest::StatusCode,
        body: String,
    },
    /// A `/render` body is not the JSON expected; `what` names the step.
    RenderDecode {
        what: &'static str,
        source: serde_json::Error,
    },
    /// A `/render` body parsed but has the wrong shape.
    RenderShape(String),
}

impl Error {
    /// The error and its causes, joined by ": ", for the `Unavailable`
    /// reason a tokenize outcome carries.
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
            Error::HttpClient { what, .. } => write!(f, "building {what}"),
            Error::Render { url, .. } => write!(f, "POST {url}"),
            Error::RenderStatus { status, body } => {
                write!(f, "/render returned {status}: {body}")
            }
            Error::RenderDecode { what, .. } => f.write_str(what),
            Error::RenderShape(what) => f.write_str(what),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::HttpClient { source, .. } => Some(source),
            Error::Render { source, .. } => Some(source),
            Error::RenderStatus { .. } | Error::RenderShape(_) => None,
            Error::RenderDecode { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests;
