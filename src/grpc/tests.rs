use super::*;
use futures::StreamExt as _;

fn chunk(bytes: &[u8]) -> Result<BodyChunk, Status> {
    Ok(BodyChunk {
        chunk: bytes.to_vec(),
    })
}

fn store() -> Arc<crate::body_store::BodyStore> {
    // 1 MiB server-wide, 1 KiB per request — small caps make the budget
    // paths cheap to hit.
    crate::body_store::shared(1024 * 1024, 1024, Duration::from_secs(60))
}

#[tokio::test]
async fn accumulate_concatenates_chunks_until_half_close() {
    let s = store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(vec![chunk(b"hello "), chunk(b"world")]);
    let buf = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect("clean half-close");
    assert_eq!(buf, b"hello world", "chunk boundaries carry no meaning");
}

#[tokio::test]
async fn accumulate_empty_body_is_valid() {
    let s = store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(Vec::<Result<BodyChunk, Status>>::new());
    let buf = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect("an immediate half-close is a complete (empty) body");
    assert!(buf.is_empty());
}

#[tokio::test]
async fn accumulate_propagates_a_reset_stream() {
    let s = store();
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
    let s = store(); // 1 KiB per-request cap
    let mut r = s.reservation();
    let big = vec![0u8; 700];
    let mut stream = futures::stream::iter(vec![chunk(&big), chunk(&big)]);
    let failure = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None)
        .await
        .expect_err("1400 bytes exceeds the 1 KiB cap");
    assert_eq!(
        failure.reject_reason(),
        Some(crate::body_store::RejectReason::RequestTooLarge)
    );
    assert_eq!(failure.into_status().code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn declared_length_matching_is_accepted() {
    let s = store();
    let mut r = s.reservation();
    let mut stream = futures::stream::iter(vec![chunk(b"hello "), chunk(b"world")]);
    let buf = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), Some(11))
        .await
        .expect("11 bytes declared, 11 delivered");
    assert_eq!(buf, b"hello world");
}

#[tokio::test]
async fn early_half_close_is_caught_by_declared_length() {
    // THE case this check exists for: the caller cleanly half-closes after
    // sending only part of the body. No transport signal distinguishes this
    // from a complete body, so without the declared length we'd forward a
    // truncated request upstream.
    let s = store();
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
    let s = store();
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
    let s = store();
    {
        let mut r = s.reservation();
        let mut stream = futures::stream::iter(vec![chunk(b"short")]);
        let _ = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), Some(999)).await;
    }
    assert_eq!(s.stats().1, 0, "a rejected push must release its bytes");
}

#[tokio::test]
async fn accumulate_times_out_on_an_idle_stream() {
    let s = store();
    let mut r = s.reservation();
    // A stream that yields one chunk then never completes: exactly the
    // "went idle mid-upload" case the idle budget exists for. Box::pin to
    // satisfy the `Unpin` bound (`stream::once` over an async block isn't).
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
    let s = store();
    {
        let mut r = s.reservation();
        let mut stream = futures::stream::iter(vec![chunk(b"abc"), Err(Status::cancelled("gone"))]);
        let _ = accumulate_body(&mut stream, &mut r, Duration::from_secs(5), None).await;
        // `r` drops here without a commit.
    }
    assert_eq!(s.stats().1, 0, "a failed push must release its bytes");
}

/// The wire spells "leave the path alone" as the empty string, so a provider
/// that declares no path must not send one; anything else would make a mixed
/// pool rewrite requests the client already addressed correctly.
#[test]
fn a_provider_destination_carries_the_path_and_empty_means_unchanged() {
    let pick = |path: Option<&str>| crate::monitor::ProviderPick {
        provider: crate::policy::engine::ObjectKey::new("infra", "together"),
        backend_ref: crate::policy::engine::ObjectKey::new("infra", "together-backend"),
        path: path.map(str::to_string),
        upstream_model: "acme/llama-70b".into(),
        canonical_model: "bar".into(),
    };

    let proto::pick_response::Destination::Provider(t) =
        provider_destination(&pick(Some("/serving-endpoints/chat/completions")))
    else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "/serving-endpoints/chat/completions");
    // The backend is still named by reference and fully qualified; the path
    // adds to that rather than replacing any part of it.
    let backend = t.backend_ref.expect("backendRef");
    assert_eq!(backend.namespace, "infra");
    assert_eq!(backend.name, "together-backend");

    let proto::pick_response::Destination::Provider(t) = provider_destination(&pick(None)) else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "", "no declared path sends no path");
}
