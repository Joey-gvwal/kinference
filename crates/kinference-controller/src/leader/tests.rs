use super::*;

const ME: &str = "cp-0";
const OTHER: &str = "cp-1";
/// Same as the `leader_election` default, so the boundary arithmetic in
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

/// The loop over a recorded API server: a free Lease is created, claimed,
/// and vacated when shutdown fires.
#[tokio::test]
async fn a_free_lease_is_claimed_and_vacated_on_shutdown() {
    use crate::api::recorded::{Recorded, Write, key};

    let api = Recorded::default();
    let (tx, mut rx) = watch::channel(false);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let params = LeaseParams {
        name: "kinference".into(),
        lease_duration: Duration::from_secs(15),
        renew_grace: GRACE,
        retry: Duration::from_millis(10),
    };
    let task = tokio::spawn({
        let api = api.clone();
        async move {
            run(api, "infra", ME.to_string(), params, tx, async {
                let _ = stopped.await;
            })
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|leader| *leader))
        .await
        .expect("elected within the timeout")
        .expect("the elector holds the sender");
    stop.send(()).expect("the elector is waiting on shutdown");
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the elector stops on shutdown")
        .expect("the elector does not panic");

    let lease = key::<Lease>("infra", "kinference");
    assert_eq!(
        api.writes(),
        vec![
            Write::Create(lease.clone()),
            Write::Replace(lease.clone()),
            Write::Replace(lease),
        ],
        "created empty, claimed, vacated"
    );
    let spec = api
        .object::<Lease>("infra", "kinference")
        .expect("the lease is still there")
        .spec
        .expect("vacating keeps a spec");
    assert_eq!(spec.holder_identity, None);
    assert_eq!(spec.lease_transitions, Some(1));
    assert!(!*rx.borrow(), "demoted on the way out");
}
