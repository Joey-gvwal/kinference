use super::*;
use dynamo_kv_router::indexer::KvIndexerInterface;

fn cfg(name: &str, block_size: u32) -> PoolSpec {
    crate::config::test_spec(name, block_size)
}

/// Dropping one pool must not disturb the process or its siblings.
///
/// `KvIndexer` keeps whatever `CancellationToken` it is constructed with and
/// cancels that token in `Drop`. This used to be the PROCESS token, so
/// dropping any pool would have cancelled the token every other subsystem
/// selects on — the gRPC server, discovery, ingest, the sweeps — i.e. a
/// single pool teardown would shut the whole process down. Nothing drops a
/// pool yet, so it was latent; dynamic pools make it live.
#[tokio::test]
async fn dropping_a_pool_cancels_only_that_pool() {
    let process = CancellationToken::new();
    let a = Pool::from_spec(PoolId::next(), &cfg("a", 16), &process);
    let b = Pool::from_spec(PoolId::next(), &cfg("b", 16), &process);
    let b_cancel = b.cancel.clone();

    let a_token = a.cancel.clone();
    drop(a);

    assert!(
        !process.is_cancelled(),
        "dropping one pool must not cancel the process token"
    );
    assert!(!b_cancel.is_cancelled(), "sibling pool must be untouched");
    assert!(
        a_token.is_cancelled(),
        "the dropped pool's own token should be cancelled by KvIndexer::drop"
    );

    // The sibling's indexer is still live and answering, not merely
    // uncancelled — this is what a pick would do next.
    b.indexer
        .find_matches(Vec::new())
        .await
        .expect("sibling indexer still serves after a sibling pool was dropped");
}

/// Ids are never recycled, so a handle that outlives its pool resolves to
/// nothing rather than to whoever took its place.
///
/// This is the property the whole opaque-id change exists for. When PoolId
/// was a `Vec` index, a freed slot could be reused — and a stale id would
/// then silently decrement another pool's inflight counter, route a model
/// to an unrelated worker set, or publish one pool's endpoint counts into
/// another's CRD status. All of that with no error anywhere.
#[tokio::test]
async fn a_stale_id_resolves_to_nothing_rather_than_to_another_pool() {
    let process = CancellationToken::new();
    let doomed = Arc::new(Pool::from_spec(PoolId::next(), &cfg("a", 16), &process));
    let doomed_id = doomed.id;

    let registry = PoolRegistry::from_pools(vec![doomed.clone()]);
    assert!(registry.snapshot().get(doomed_id).is_some());

    // The pool leaves the fleet, and a replacement arrives.
    let replacement = Arc::new(Pool::from_spec(PoolId::next(), &cfg("b", 16), &process));
    assert_ne!(
        replacement.id, doomed_id,
        "a fresh pool must never inherit a retired pool's id"
    );
    registry.publish(Arc::new(PoolMap::from_pools(vec![replacement.clone()])));

    assert!(
        registry.snapshot().get(doomed_id).is_none(),
        "a stale id must resolve to None, not to the replacement"
    );
    assert!(registry.snapshot().get(replacement.id).is_some());
}

/// A snapshot taken before a change keeps serving the old view, so a reader
/// mid-request never observes the set shifting under it.
/// Publishing must WAKE consumers whose answer depends on the pool set but
/// whose own inputs did not change.
///
/// Policy resolution is the case that matters: it runs on CRD watch events,
/// but whether a destination resolves also depends on which pools exist. A
/// pool created just after the last CRD event would otherwise leave that
/// destination reported unresolved indefinitely, while the router routed to
/// it perfectly well — status contradicting reality, which is worse than
/// either being wrong alone.
#[tokio::test]
async fn publishing_wakes_subscribers() {
    let registry = PoolRegistry::new();
    let mut rx = registry.subscribe();
    assert!(!rx.has_changed().unwrap(), "no publish yet");

    registry.publish(Arc::new(PoolMap::from_pools(vec![test_pool("a")])));
    assert!(rx.has_changed().unwrap(), "a publish must be observable");
    rx.borrow_and_update();

    // Including a publish that EMPTIES the set — a retirement has to reach
    // status just as much as a creation does.
    registry.publish(Arc::new(PoolMap::from_pools(Vec::new())));
    assert!(rx.has_changed().unwrap());
}

#[tokio::test]
async fn an_existing_snapshot_is_unaffected_by_a_later_publish() {
    let process = CancellationToken::new();
    let a = Arc::new(Pool::from_spec(PoolId::next(), &cfg("a", 16), &process));
    let registry = PoolRegistry::from_pools(vec![a.clone()]);

    let held = registry.snapshot();
    registry.publish(Arc::new(PoolMap::default()));

    assert_eq!(held.len(), 1, "the held snapshot is immutable");
    assert!(held.get(a.id).is_some());
    assert!(registry.snapshot().is_empty(), "the new view is empty");
}

/// A pick's inflight bookkeeping survives its pool being destroyed.
///
/// The release path used to resolve `PendingObservation.pool_id` through
/// the fleet — up to `observation_ttl` (600s) after the pick. Carrying the
/// two handles instead makes it total: the pool stays alive exactly as long
/// as a pending observation still needs it, enforced by `Arc` rather than by
/// a retention window someone has to tune.
///
/// `release_inflight_state` itself is private, so this exercises the
/// property it depends on: the handles keep working, and keep the pool
/// alive, after the fleet has dropped it.
#[tokio::test]
async fn inflight_handles_outlive_their_pool() {
    let process = CancellationToken::new();
    let pool = Arc::new(Pool::from_spec(PoolId::next(), &cfg("a", 16), &process));
    let pool_token = pool.cancel.clone();

    // What a pick stashes on its observation record.
    let handles = crate::observations::InflightHandles::of(&pool);
    pool.inflight.inc("10.0.0.1:8000");
    assert_eq!(pool.inflight.get("10.0.0.1:8000"), 1);

    // The fleet drops the pool while the request is still in flight.
    let registry = PoolRegistry::from_pools(vec![pool]);
    registry.publish(Arc::new(PoolMap::default()));
    assert!(registry.snapshot().is_empty());

    // The release still lands, on the right counter.
    handles.inflight.dec("10.0.0.1:8000");
    assert_eq!(
        handles.inflight.get("10.0.0.1:8000"),
        0,
        "the decrement must reach the pool the pick was made against"
    );
    // And note what it does NOT do: the handles hold the two registries,
    // not the pool, so the pool itself was reclaimed the moment the fleet
    // dropped it — OS thread, radix tree and all. Only two small maps
    // outlive it, for as long as an observation still needs them. Holding
    // an `Arc<Pool>` on the record instead would have pinned a whole
    // indexer thread per stray observation for up to `observation_ttl`.
    assert!(
        pool_token.is_cancelled(),
        "the pool is reclaimed promptly; only the inflight maps linger"
    );

    drop(handles);
}

/// Process shutdown must still reach every pool: `child_token` propagates
/// parent → child, so this is one-way isolation, not disconnection.
#[tokio::test]
async fn cancelling_the_process_cancels_every_pool() {
    let process = CancellationToken::new();
    let p = Pool::from_spec(PoolId::next(), &cfg("a", 16), &process);
    assert!(!p.cancel.is_cancelled());
    process.cancel();
    assert!(p.cancel.is_cancelled(), "process shutdown must reach pools");
}
