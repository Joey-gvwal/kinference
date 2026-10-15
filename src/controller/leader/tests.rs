use super::*;

const ME: &str = "cp-0";
const OTHER: &str = "cp-1";
/// Same as the `[leader_election]` default, so the boundary arithmetic in
/// these tests still describes the shipped behavior.
const GRACE: Duration = Duration::from_secs(10);

fn ts(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn spec(holder: &str, renewed_at: i64) -> LeaseSpec {
    LeaseSpec {
        holder_identity: Some(holder.to_string()),
        lease_duration_seconds: Some(15),
        renew_time: Some(MicroTime(ts(renewed_at))),
        acquire_time: Some(MicroTime(ts(renewed_at))),
        lease_transitions: Some(1),
        ..Default::default()
    }
}

#[test]
fn unseen_lease_is_claimed() {
    assert_eq!(next_action(ts(100), None, ME, GRACE), Action::Claim);
}

#[test]
fn free_lease_is_claimed() {
    let s = LeaseSpec::default();
    assert_eq!(next_action(ts(100), Some(&s), ME, GRACE), Action::Claim);
    let empty_holder = LeaseSpec {
        holder_identity: Some(String::new()),
        ..Default::default()
    };
    assert_eq!(
        next_action(ts(100), Some(&empty_holder), ME, GRACE),
        Action::Claim
    );
}

#[test]
fn malformed_claim_is_claimed() {
    // Holder set but no renew time / duration: unenforceable.
    let s = LeaseSpec {
        holder_identity: Some(OTHER.to_string()),
        ..Default::default()
    };
    assert_eq!(next_action(ts(100), Some(&s), ME, GRACE), Action::Claim);
}

#[test]
fn rivals_current_claim_waits_out_its_expiry() {
    // Renewed at t=100, 15s duration: expires at t=115.
    let s = spec(OTHER, 100);
    assert_eq!(
        next_action(ts(100), Some(&s), ME, GRACE),
        Action::WaitUntil(ts(115))
    );
    assert_eq!(next_action(ts(115), Some(&s), ME, GRACE), Action::Claim);
    assert_eq!(next_action(ts(200), Some(&s), ME, GRACE), Action::Claim);
}

#[test]
fn own_fresh_claim_waits_until_renew_point() {
    // Expires at t=115, grace 10s: renew from t=105.
    let s = spec(ME, 100);
    assert_eq!(
        next_action(ts(100), Some(&s), ME, GRACE),
        Action::WaitUntil(ts(105))
    );
}

#[test]
fn own_claim_inside_grace_is_renewed() {
    let s = spec(ME, 100);
    assert_eq!(next_action(ts(105), Some(&s), ME, GRACE), Action::Claim);
    assert_eq!(next_action(ts(114), Some(&s), ME, GRACE), Action::Claim);
    // Even past expiry it's still ours to re-take.
    assert_eq!(next_action(ts(120), Some(&s), ME, GRACE), Action::Claim);
}

#[test]
fn holder_currency_respects_expiry() {
    let s = spec(ME, 100);
    assert!(is_current_holder(Some(&s), ME, ts(110)));
    assert!(!is_current_holder(Some(&s), ME, ts(115)));
    assert!(!is_current_holder(Some(&s), OTHER, ts(110)));
    assert!(!is_current_holder(None, ME, ts(110)));
}
