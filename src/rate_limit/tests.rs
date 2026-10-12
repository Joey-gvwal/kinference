use super::*;

fn limiter() -> NoopRateLimiter {
    // A throwaway registry, not the process-wide one: these tests assert on the
    // wire contract, and the counters only need somewhere to land.
    NoopRateLimiter::new(Metrics::new(&prometheus::Registry::new()))
}

async fn reserve(l: &NoopRateLimiter, tenant: &str, max: i64) -> ReserveResponse {
    l.reserve(Request::new(ReserveRequest {
        tenant_id: tenant.into(),
        max_tokens: max,
    }))
    .await
    .expect("the stub admits every well-formed reservation")
    .into_inner()
}

#[tokio::test]
async fn every_reservation_is_admitted() {
    let l = limiter();
    let r = reserve(&l, "tenant-a", 100_000).await;
    assert!(r.allowed);
    assert_eq!(r.reason, DenialReason::Unspecified as i32);
    assert_eq!(r.retry_after_ms, 0);
}

/// 0 is the wire value for "not allowed", so a handle must never be 0 — a
/// caller checking `request_id != 0` would otherwise conclude it was denied
/// while `allowed` said true.
#[tokio::test]
async fn handles_are_non_zero_and_unique() {
    let l = limiter();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..100 {
        let id = reserve(&l, "t", 10).await.request_id;
        assert!(id > 0, "0 means `not allowed` on the wire");
        assert!(seen.insert(id), "handles must not repeat: {id}");
    }
}

/// `limit = 0` is the proto's "no window enforced". Reporting a nonzero
/// limit with 0 remaining would read as throttled while admitting
/// everything — the most misleading combination available.
#[tokio::test]
async fn the_stub_reports_no_window_rather_than_a_fake_budget() {
    let r = reserve(&limiter(), "t", 10).await;
    assert_eq!(r.limit, 0, "0 limit means `not applicable`");
    assert_eq!(r.window_ms, 0);
}

#[tokio::test]
async fn a_commit_against_a_real_handle_is_ok() {
    let l = limiter();
    let id = reserve(&l, "t", 100).await.request_id;
    let resp = l
        .commit(Request::new(CommitRequest {
            request_id: id,
            actual_tokens: 42,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, CommitStatus::Ok as i32);
}

/// Committing against 0 means the caller never held a reservation. Surfacing
/// it now matters because the real limiter will answer the same way, and a
/// caller that ignores it will silently leak holds until its window fills.
#[tokio::test]
async fn a_commit_against_zero_is_not_found() {
    let l = limiter();
    for id in [0, -1] {
        let resp = l
            .commit(Request::new(CommitRequest {
                request_id: id,
                actual_tokens: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.status,
            CommitStatus::NotFound as i32,
            "request_id {id} must not be accepted"
        );
    }
}

/// A zero commit is legitimate — a request that failed before producing
/// anything still has to release its hold.
#[tokio::test]
async fn committing_zero_tokens_is_accepted() {
    let l = limiter();
    let id = reserve(&l, "t", 100).await.request_id;
    let resp = l
        .commit(Request::new(CommitRequest {
            request_id: id,
            actual_tokens: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, CommitStatus::Ok as i32);
}

/// `tenant_id` is the isolation boundary; an empty one has no meaning, and
/// accepting it would let the real limiter later collapse every anonymous
/// caller into one shared budget.
#[tokio::test]
async fn an_empty_tenant_is_invalid_argument() {
    for tenant in ["", "   "] {
        let err = limiter()
            .reserve(Request::new(ReserveRequest {
                tenant_id: tenant.into(),
                max_tokens: 10,
            }))
            .await
            .expect_err("an empty tenant_id must be refused even by the stub");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}

#[tokio::test]
async fn a_negative_reservation_is_invalid_argument() {
    let err = limiter()
        .reserve(Request::new(ReserveRequest {
            tenant_id: "t".into(),
            max_tokens: -1,
        }))
        .await
        .expect_err("a negative max_tokens must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// A zero-token reservation is odd but not wrong, and refusing it would make
/// a caller special-case empty prompts.
#[tokio::test]
async fn a_zero_token_reservation_is_allowed() {
    assert!(reserve(&limiter(), "t", 0).await.allowed);
}

/// The `[rate_limit]` section still binds to this subsystem's config type.
///
/// `crate::config::Config` does not set `deny_unknown_fields`, so a section
/// that stops binding — a renamed field, a type moved without its serde name,
/// a typo — deserializes to defaults SILENTLY and the router comes up with the
/// service quietly off. There is no compiler error and no startup error to
/// catch it, so the only guard is asserting the bound value here, from inside
/// the module that can see the private field.
#[test]
fn rate_limit_section_binds_to_this_subsystems_config() {
    let with = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[rate_limit]
enabled = true
"#;
    let cfg: crate::config::Config = toml::from_str(with).expect("parses");
    assert!(
        cfg.rate_limit.enabled,
        "[rate_limit] enabled = true must reach this subsystem's config"
    );

    // And absence still means off, rather than the section being ignored in
    // both directions and the assertion above passing for the wrong reason.
    let without = r#"
listen = "127.0.0.1:50051"
mock_mode = true
"#;
    let cfg: crate::config::Config = toml::from_str(without).expect("parses");
    assert!(
        !cfg.rate_limit.enabled,
        "absent section must default to off"
    );
}
