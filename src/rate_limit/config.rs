//! `[rate_limit]` — this subsystem's own config section.
//!
//! Lives here rather than in `crate::config` so that changing this
//! subsystem's knobs touches only this subsystem's directory. The on-disk
//! format is unaffected by the move: a TOML section is named by the FIELD on
//! `crate::config::Config` (`rate_limit`), never by the type or the module it
//! is defined in.
//!
//! `enabled` is `pub(super)`, not `pub`: readable by `crate::rate_limit` and
//! by nothing else. `crate::config` publishes the type upward without being
//! able to read into it, and the startup toggle stays a decision this
//! subsystem makes about itself. Fully private would not work — a parent
//! cannot see a child module's private fields.

use serde::Deserialize;

/// `[rate_limit]` — token-budget reserve/reconcile for callers.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RateLimitConfig {
    /// Serve `RateLimiter` on the main gRPC listener.
    ///
    /// **Off by default because the implementation admits everything.** It fails
    /// open in the direction absence already points — a caller with no limiter
    /// is equally unlimited — so the hazard is a false sense of protection
    /// rather than a security hole. Enabling it logs a WARN saying so.
    #[serde(default)]
    pub(super) enabled: bool,
}
