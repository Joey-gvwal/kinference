use super::*;
use futures::StreamExt as _;

const MB: u64 = 1024 * 1024;

fn store() -> Arc<BodyStore> {
    shared(8 * MB, MB, Duration::from_secs(60))
}

/// Push a body through the normal path and return its stream_id.
fn push(store: &Arc<BodyStore>, body: &[u8]) -> u64 {
    let mut r = store.reservation();
    r.charge(body.len() as u64).expect("within budget");
    store.commit(r, body.to_vec())
}

#[test]
fn full_lifecycle_pushes_picks_and_pulls() {
    let s = store();
    let id = push(&s, b"hello");
    assert_ne!(id, 0, "ids are never 0");
    assert_eq!(s.stats(), (1, 5));

    let handle = s.begin_pick(id).expect("ready body");
    assert_eq!(handle.body().as_slice(), b"hello");
    let response_id = 424242;
    handle.commit(response_id, Arc::new(b"hello".to_vec()));

    let lease = s.take_outbound(response_id).expect("outbound body");
    assert_eq!(lease.body().as_slice(), b"hello");
    drop(lease);
    // Tombstone remains under stream_id but carries no bytes.
    assert_eq!(s.stats().1, 0, "all bytes released once the lease drops");
}

#[test]
fn double_pick_is_failed_precondition() {
    let s = store();
    let id = push(&s, b"body");
    let h = s.begin_pick(id).unwrap();
    h.commit(7, Arc::new(b"body".to_vec()));
    let err = s.begin_pick(id).expect_err("second pick must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
}

#[test]
fn unknown_ids_are_not_found_and_zero_is_invalid() {
    let s = store();
    assert_eq!(s.begin_pick(999).unwrap_err().code(), tonic::Code::NotFound);
    assert_eq!(
        s.begin_pick(0).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        s.take_outbound(999).unwrap_err().code(),
        tonic::Code::NotFound
    );
}

#[test]
fn pull_body_is_single_shot() {
    let s = store();
    let id = push(&s, b"abc");
    s.begin_pick(id)
        .unwrap()
        .commit(5, Arc::new(b"abc".to_vec()));
    assert!(s.take_outbound(5).is_ok());
    assert_eq!(
        s.take_outbound(5).unwrap_err().code(),
        tonic::Code::NotFound,
        "second pull must not find the body"
    );
}

#[test]
fn per_request_budget_rejects() {
    let s = store(); // per-request cap 1 MiB
    let mut r = s.reservation();
    let (err, reason) = r.charge(2 * MB).expect_err("over per-request cap");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(reason, RejectReason::RequestTooLarge);
    drop(r);
    assert_eq!(s.stats().1, 0);
}

#[test]
fn per_request_budget_counts_cumulative_chunks() {
    let s = store();
    let mut r = s.reservation();
    // 1 MiB cap: three 400 KiB chunks must trip on the third.
    let chunk = 400 * 1024;
    r.charge(chunk).unwrap();
    r.charge(chunk).unwrap();
    assert!(r.charge(chunk).is_err(), "cumulative size must be enforced");
}

#[test]
fn server_budget_rejects_when_full() {
    // Global 1 MiB, per-request 1 MiB: two full-size bodies can't coexist.
    let s = shared(MB, MB, Duration::from_secs(60));
    let _held = push(&s, &vec![0u8; MB as usize]);
    let mut r = s.reservation();
    let (err, reason) = r.charge(1024).expect_err("server full");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(reason, RejectReason::ServerFull);
}

#[test]
fn declared_length_is_pre_rejected() {
    let s = store();
    let (err, reason) = s.check_declared_length(9 * MB).expect_err("too large");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(reason, RejectReason::DeclaredTooLarge);
    assert!(s.check_declared_length(1024).is_ok());
}

#[test]
fn abandoned_push_releases_on_drop() {
    let s = store();
    {
        let mut r = s.reservation();
        r.charge(4096).unwrap();
        assert_eq!(s.stats().1, 4096);
        // Dropped without commit (the push broke or went idle before an id
        // was handed out), reclaimed without TTL.
    }
    assert_eq!(s.stats(), (0, 0));
}

#[test]
fn discarded_pick_releases_immediately() {
    let s = store();
    let id = push(&s, b"unroutable");
    s.begin_pick(id).unwrap().discard();
    assert_eq!(s.stats().1, 0, "non-OK pick frees the body at once");
    // The tombstone survives, so a retry is FAILED_PRECONDITION.
    assert_eq!(
        s.begin_pick(id).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}

#[test]
fn unresolved_pick_handle_releases_on_drop() {
    let s = store();
    let id = push(&s, b"abandoned mid-pick");
    drop(s.begin_pick(id).unwrap());
    assert_eq!(s.stats().1, 0, "dropping a handle must not leak bytes");
}

#[test]
fn rewritten_body_reaccounts_the_delta() {
    let s = store();
    let id = push(&s, b"small");
    let h = s.begin_pick(id).unwrap();
    h.commit(11, Arc::new(vec![0u8; 500]));
    assert_eq!(s.stats().1, 500, "outbound size replaces pushed size");
    drop(s.take_outbound(11).unwrap());
    assert_eq!(s.stats().1, 0);
}

#[test]
fn abort_is_idempotent_and_frees_bytes() {
    let s = store();
    let id = push(&s, b"aborted");
    s.begin_pick(id)
        .unwrap()
        .commit(13, Arc::new(b"aborted".to_vec()));
    let (released, bytes) = s.abort(13);
    assert!(released);
    assert_eq!(bytes, 7);
    // Duplicate abort: false, never an error.
    assert_eq!(s.abort(13), (false, 0));
    assert_eq!(s.stats().1, 0);
}

#[test]
fn ttl_sweep_reclaims_and_frees_bytes() {
    let s = shared(8 * MB, MB, Duration::from_millis(1));
    let _id = push(&s, b"forgotten");
    assert_eq!(s.stats().1, 9);
    std::thread::sleep(Duration::from_millis(5));
    let (count, bytes) = s.take_expired();
    assert_eq!((count, bytes), (1, 9));
    assert_eq!(s.stats(), (0, 0));
}

#[test]
fn ttl_sweep_keeps_fresh_entries() {
    let s = store();
    let _id = push(&s, b"fresh");
    assert_eq!(s.take_expired(), (0, 0));
    assert_eq!(s.stats().0, 1);
}

#[test]
fn ids_are_unique_across_many_pushes() {
    let s = shared(8 * MB, MB, Duration::from_secs(60));
    let ids: std::collections::HashSet<u64> = (0..200).map(|_| push(&s, b"x")).collect();
    assert_eq!(ids.len(), 200, "minted ids must not collide");
    assert!(!ids.contains(&0));
}

// --- the push drain ---------------------------------------------------------

fn chunk(bytes: &[u8]) -> Result<BodyChunk, Status> {
    Ok(BodyChunk {
        chunk: bytes.to_vec(),
    })
}

fn push_store() -> Arc<BodyStore> {
    // 1 MiB server-wide, 1 KiB per request, so the budget paths are cheap to
    // hit.
    shared(1024 * 1024, 1024, Duration::from_secs(60))
}

#[tokio::test]
async fn accumulate_concatenates_chunks_until_half_close() {
    let s = push_store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(vec![chunk(b"hello "), chunk(b"world")]);
    let buf = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect("clean half-close");
    assert_eq!(buf, b"hello world", "chunk boundaries carry no meaning");
}

#[tokio::test]
async fn accumulate_empty_body_is_valid() {
    let s = push_store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(Vec::<Result<BodyChunk, Status>>::new());
    let buf = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect("an immediate half-close is a complete (empty) body");
    assert!(buf.is_empty());
}

#[tokio::test]
async fn accumulate_propagates_a_reset_stream() {
    let s = push_store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(vec![
        chunk(b"partial"),
        Err(Status::cancelled("client went away")),
    ]);
    let failure = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect_err("a reset stream must not look like a complete body");
    assert!(matches!(failure, PushFailure::Transport(_)));
    assert_eq!(failure.into_status().code(), tonic::Code::Cancelled);
}

#[tokio::test]
async fn accumulate_enforces_the_byte_budget_mid_stream() {
    let s = push_store(); // 1 KiB per-request cap
    let mut r = s.reservation();
    let big = vec![0u8; 700];
    let mut stream = futures::stream::iter(vec![chunk(&big), chunk(&big)]);
    let failure = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect_err("1400 bytes exceeds the 1 KiB cap");
    assert_eq!(failure.reject_reason(), Some(RejectReason::RequestTooLarge));
    assert_eq!(failure.into_status().code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn declared_length_matching_is_accepted() {
    let s = push_store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(vec![chunk(b"hello "), chunk(b"world")]);
    let buf = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), Some(11))
        .await
        .expect("11 bytes declared, 11 delivered");
    assert_eq!(buf, b"hello world");
}

#[tokio::test]
async fn early_half_close_is_caught_by_declared_length() {
    // The caller cleanly half-closes after sending only part of the body,
    // which only the declared length can catch.
    let s = push_store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(vec![chunk(br#"{"model":"a","mess"#)]);
    let failure = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), Some(64))
        .await
        .expect_err("a truncated body must not look complete");
    assert!(matches!(
        failure,
        PushFailure::LengthMismatch {
            declared: 64,
            received: 18
        }
    ));
    assert_eq!(failure.into_status().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn over_long_body_fails_fast_against_declared_length() {
    let s = push_store();
    let mut r = s.reservation();
    // Declares 4 bytes then sends 10: caught on the chunk that overshoots,
    // without buffering the rest.
    let mut stream = futures::stream::iter(vec![chunk(b"0123456789"), chunk(b"more")]);
    let failure = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), Some(4))
        .await
        .expect_err("over-long body must be refused");
    assert!(matches!(
        failure,
        PushFailure::LengthMismatch { declared: 4, .. }
    ));
    assert_eq!(failure.into_status().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn length_mismatch_leaks_no_bytes() {
    let s = push_store();
    {
        let mut r = s.reservation();
        let mut stream = futures::stream::iter(vec![chunk(b"short")]);
        let _ = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), Some(999)).await;
    }
    assert_eq!(s.stats().1, 0, "a rejected push must release its bytes");
}

#[tokio::test]
async fn accumulate_times_out_on_an_idle_stream() {
    let s = push_store();
    let mut r = s.reservation();
    // A stream that yields one chunk then never completes. Box::pin for the
    // `Unpin` bound.
    let mut stream =
        Box::pin(futures::stream::iter(vec![chunk(b"first")]).chain(futures::stream::pending()));
    let failure = accumulate_body(&mut stream, &mut r, Duration::from_millis(20), None)
        .await
        .expect_err("idle budget must fire");
    assert!(matches!(failure, PushFailure::Idle(_)));
    assert_eq!(failure.into_status().code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn abandoned_accumulate_leaks_no_bytes() {
    let s = push_store();
    {
        let mut r = s.reservation();
        let mut stream = futures::stream::iter(vec![chunk(b"abc"), Err(Status::cancelled("gone"))]);
        let _ = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None).await;
        // `r` drops here without a commit.
    }
    assert_eq!(s.stats().1, 0, "a failed push must release its bytes");
}
