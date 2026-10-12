use super::*;

fn key() -> ObjectKey {
    ObjectKey::new("infra", "together-prod")
}

#[test]
fn no_samples_means_no_figure_rather_than_zero() {
    let s = ProviderHealthStore::new();
    // The distinction the whole design rests on: an unobserved provider has
    // NO latency, which keeps it eligible, rather than a latency of zero,
    // which would make it win every ranking.
    assert_eq!(s.observed_ttft_ms(&key()), None);
}

#[test]
fn p95_is_an_observed_value() {
    let s = ProviderHealthStore::new();
    for ms in [100.0, 200.0, 300.0, 400.0, 900.0] {
        s.record_success(&key(), ms);
    }
    // Nearest-rank over 5 samples: index ceil(5 * 0.95) - 1 = 4.
    //
    // A NUMBER, and the only form this value now takes: it feeds
    // `EstimatedTTFT` ranking and `backend.observedTTFTP95` predicates, and
    // is no longer formatted for status.
    assert_eq!(s.observed_ttft_ms(&key()), Some(900.0));
}

#[test]
fn suppression_needs_consecutive_failures_and_a_success_resets() {
    let s = ProviderHealthStore::new();
    assert!(!s.record_failure(&key(), FailureKind::Upstream, "503"));
    assert!(!s.record_failure(&key(), FailureKind::Upstream, "503"));
    assert!(!s.is_suppressed(&key()), "two is not a pattern");

    s.record_success(&key(), 120.0);
    assert!(!s.record_failure(&key(), FailureKind::Upstream, "503"));
    assert!(
        !s.is_suppressed(&key()),
        "a success in between means the streak restarted"
    );

    assert!(!s.record_failure(&key(), FailureKind::Upstream, "503"));
    assert!(s.record_failure(&key(), FailureKind::Upstream, "503"));
    assert!(s.is_suppressed(&key()));

    // The transition is reported once, so the log records a state change
    // rather than one line per failed request.
    assert!(!s.record_failure(&key(), FailureKind::Upstream, "503"));

    s.record_success(&key(), 120.0);
    assert!(!s.is_suppressed(&key()), "a success lifts suppression");
}

#[test]
fn failure_kind_routes_the_operator_to_the_right_object() {
    assert!(FailureKind::Unresolved.is_configuration_error());
    assert!(FailureKind::Unauthorized.is_configuration_error());
    assert!(!FailureKind::Upstream.is_configuration_error());
}

#[test]
fn health_reports_the_last_failure_alongside_a_working_provider() {
    let s = ProviderHealthStore::new();
    s.record_failure(&key(), FailureKind::Unauthorized, "401 from vendor");
    s.record_success(&key(), 200.0);
    let h = s.health(&key());
    assert!(!h.suppressed);
    // Recovered, but the operator still gets to see what happened — a wrong
    // Secret key that starts working is worth knowing about.
    assert_eq!(
        h.last_failure.map(|(k, _)| k),
        Some(FailureKind::Unauthorized)
    );
}

#[test]
fn sweep_drops_providers_with_nothing_to_say() {
    let s = ProviderHealthStore::new();
    s.record_success(&key(), 100.0);
    assert_eq!(s.sweep(), 0, "a live sample keeps the entry");
    let empty = ProviderHealthStore::new();
    empty.observed_ttft_ms(&key());
    assert_eq!(empty.sweep(), 0, "nothing was ever recorded");
}

/// A non-streaming success clears the failure streak but contributes no sample.
///
/// Both halves matter and they pull in opposite directions. The sample must be
/// excluded because a non-streaming response's reported first-token time is
/// really total latency, and this p95 is not merely published — `EstimatedTTFT`
/// ranks providers by it and `backend.observedTTFTP95` predicates compare
/// against it, so admitting it would cost the provider picks it should win.
///
/// The recovery must still happen, which is why this is not simply "skip the
/// call": a provider serving only non-streaming traffic would otherwise stay
/// suppressed forever after a single transient failure.
#[test]
fn an_untimed_success_recovers_without_contributing_a_sample() {
    let s = ProviderHealthStore::new();
    let k = key();

    // Suppress it the normal way.
    for _ in 0..3 {
        s.record_failure(&k, FailureKind::Upstream, "vendor returned 503");
    }
    assert!(s.is_suppressed(&k), "three failures must suppress");

    // A non-streaming success: recovery, but no latency.
    s.record_success_untimed(&k);
    assert!(
        !s.is_suppressed(&k),
        "an untimed success must still count as recovery"
    );
    assert_eq!(
        s.observed_ttft_ms(&k),
        None,
        "total latency must not become a TTFT sample"
    );

    // A streaming success does contribute, so the two paths are distinguishable.
    s.record_success(&k, 240.0);
    assert_eq!(s.observed_ttft_ms(&k), Some(240.0));
}

/// An untimed success does not dilute an existing distribution either.
#[test]
fn an_untimed_success_leaves_earlier_samples_untouched() {
    let s = ProviderHealthStore::new();
    let k = key();
    s.record_success(&k, 100.0);
    let before = s.observed_ttft_ms(&k);
    s.record_success_untimed(&k);
    assert_eq!(
        s.observed_ttft_ms(&k),
        before,
        "a non-streaming request must not move the p95 at all"
    );
}
