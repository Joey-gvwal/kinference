//! Why an `InferenceWorkerSet` produced no pool.
//!
//! `worker_sets::desired_pools` produces these; `store` projects them into the
//! reasons status reports. Kept apart from `worker_sets` because that module
//! also holds the watch, and resolving names (`store::resolve`, `kinfctl
//! check`) needs only the verdict, not the watcher.

use super::PoolKey;

/// Why a worker set could not be materialized.
///
/// Every variant is an operator-fixable misconfiguration, and they are kept
/// distinct because the FIX differs: an unknown profile means editing the
/// router's TOML, a missing one means editing the CR. Reporting them alike
/// would send an operator to the wrong file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// `spec.profile` absent. Required by the schema, so this means the object
    /// predates it or bypassed validation.
    MissingProfile,
    /// `spec.profile` names a profile the router does not define. Carries the
    /// defined names, because in practice this is a typo and seeing the list
    /// resolves it immediately.
    UnknownProfile { defined: Vec<String> },
    /// `spec.kvBlockSize` absent, same provenance as `MissingProfile`.
    MissingKvBlockSize,
    /// `spec.kvBlockSize` is zero. The schema forbids it; a `KvIndexer` built
    /// on it would divide by zero.
    InvalidKvBlockSize { got: u32 },
    /// `spec.serviceRef.name` is empty, so there is nothing to watch.
    MissingServiceRef,
    /// A `spec.models` row carries no `name`.
    ///
    /// The row maps an alias onto nothing, so the alias silently would not
    /// resolve. Refused rather than dropped: the schema requires `name`, so this
    /// is only reachable by an object that bypassed validation, and a declared
    /// alias that does not work is a routing lie rather than a cosmetic slip.
    ModelMissingName,
    /// `spec.kvEvents` is present but carries no `port`.
    ///
    /// A contradiction rather than an omission: the block IS the declaration
    /// that these engines publish events, and it does not say where. The schema
    /// requires `port`, so this is only reachable by an object that bypassed
    /// validation — but it has to be reachable, because `port` is `Option` in
    /// Rust precisely so one such object cannot fail a whole watcher page.
    KvEventsMissingPort,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProfile => write!(f, "spec.profile is required"),
            Self::UnknownProfile { defined } => write!(
                f,
                "spec.profile names an undefined routing profile; the router defines [{}]",
                defined.join(", ")
            ),
            Self::MissingKvBlockSize => write!(f, "spec.kvBlockSize is required"),
            Self::InvalidKvBlockSize { got } => {
                write!(f, "spec.kvBlockSize must be > 0, got {got}")
            }
            Self::MissingServiceRef => write!(f, "spec.serviceRef.name is required"),
            Self::ModelMissingName => write!(
                f,
                "a spec.models row has no name; every row needs the name the \
                 engines report for that model"
            ),
            Self::KvEventsMissingPort => write!(
                f,
                "spec.kvEvents is present but has no port; omit the whole block if these \
                 engines publish no KV events"
            ),
        }
    }
}

/// Short machine-readable form, for a status condition's `reason` and for a
/// metric label. Kubernetes conditions want CamelCase here.
impl RejectReason {
    pub fn as_condition_reason(&self) -> &'static str {
        match self {
            Self::MissingProfile => "MissingProfile",
            Self::UnknownProfile { .. } => "UnknownProfile",
            Self::MissingKvBlockSize => "MissingKvBlockSize",
            Self::InvalidKvBlockSize { .. } => "InvalidKvBlockSize",
            Self::MissingServiceRef => "MissingServiceRef",
            Self::ModelMissingName => "ModelMissingName",
            Self::KvEventsMissingPort => "KvEventsMissingPort",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub key: PoolKey,
    pub reason: RejectReason,
}
