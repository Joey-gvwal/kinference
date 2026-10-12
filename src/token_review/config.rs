//! `[token_review]` — this subsystem's own config section.
//!
//! Lives here rather than in `crate::config` so that changing this
//! subsystem's knobs touches only this subsystem's directory. The on-disk
//! format is unaffected: a TOML section is named by the FIELD on
//! `crate::config::Config` (`token_review`), never by the type or the module
//! it is defined in.
//!
//! Every field is `pub(super)`, so the whole section is readable by
//! `crate::token_review` and by nothing else. `crate::config` publishes the
//! type upward without being able to read into it.

use serde::Deserialize;

/// `[token_review]` — the Kubernetes bearer-token validation service.
///
/// Separate from `[policy]` and `[kubernetes]`: a caller may want token review
/// without CRD-driven routing, or the reverse, and folding them together would
/// make one impossible to enable without the other.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct TokenReviewConfig {
    /// Serve `TokenReview` on the main gRPC listener. Needs `create` on
    /// `tokenreviews` in `authentication.k8s.io` — see
    /// `k8s/kinference/rbac.yaml`.
    #[serde(default)]
    pub(super) enabled: bool,
    /// Wall-clock budget for one apiserver round-trip when the caller does not
    /// supply a deadline of its own. The only budget: the request carries none
    /// of its own.
    #[serde(default = "default_token_review_deadline_ms")]
    pub(super) default_deadline_ms: u64,
    /// How long an AUTHENTICATED result is cached.
    ///
    /// This is the staleness window against revocation, so it trades apiserver
    /// load for how long a withdrawn token keeps working. Bound ServiceAccount
    /// tokens are short-lived anyway, which is what makes a minute defensible.
    #[serde(default = "default_token_review_positive_ttl_secs")]
    pub(super) positive_ttl_secs: u64,
    /// How long a DENIED result is cached. Deliberately shorter than the
    /// positive TTL: a token that has just been granted should start working
    /// promptly, while a token that is simply wrong tends to be presented over
    /// and over and is worth absorbing.
    #[serde(default = "default_token_review_negative_ttl_secs")]
    pub(super) negative_ttl_secs: u64,
    /// Cache size cap.
    ///
    /// Bound ServiceAccount tokens ROTATE, so an unbounded cache grows one
    /// entry per rotation per caller forever. The cap is what makes the
    /// steady-state footprint track live tokens instead of the high-water mark.
    #[serde(default = "default_token_review_max_cache_entries")]
    pub(super) max_cache_entries: usize,
}

impl Default for TokenReviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_deadline_ms: default_token_review_deadline_ms(),
            positive_ttl_secs: default_token_review_positive_ttl_secs(),
            negative_ttl_secs: default_token_review_negative_ttl_secs(),
            max_cache_entries: default_token_review_max_cache_entries(),
        }
    }
}

fn default_token_review_deadline_ms() -> u64 {
    2000
}
fn default_token_review_positive_ttl_secs() -> u64 {
    60
}
fn default_token_review_negative_ttl_secs() -> u64 {
    10
}
fn default_token_review_max_cache_entries() -> usize {
    4096
}
