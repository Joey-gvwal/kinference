use super::*;
use dynamo_kv_router::indexer::KvIndexerInterface;

fn cfg(name: &str, block_size: u32) -> PoolSpec {
    crate::config::test_spec(name, block_size)
}

/// Dropping one pool doesn't disturb the process or its siblings, although
/// `KvIndexer` cancels its token in `Drop`.
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

    // The sibling's indexer still answers, not merely uncancelled.
    b.indexer
        .find_matches(Vec::new())
        .await
        .expect("sibling indexer still serves after a sibling pool was dropped");
}

/// Ids are never recycled, so a handle that outlives its pool resolves to
/// nothing rather than to whoever took its place.
///
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

/// Publishing wakes consumers, such as policy resolution, whose answer
/// depends on the pool set even when their own inputs didn't change.
#[tokio::test]
async fn publishing_wakes_subscribers() {
    let registry = PoolRegistry::new();
    let mut rx = registry.subscribe();
    assert!(!rx.has_changed().unwrap(), "no publish yet");

    registry.publish(Arc::new(PoolMap::from_pools(vec![test_pool("a")])));
    assert!(rx.has_changed().unwrap(), "a publish must be observable");
    rx.borrow_and_update();

    // Including a publish that empties the set.
    registry.publish(Arc::new(PoolMap::from_pools(Vec::new())));
    assert!(rx.has_changed().unwrap());
}

/// A snapshot taken before a change keeps serving the old view, so a reader
/// mid-request never sees the set shift.
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
/// `release_inflight_state` is private, so this checks what it relies on: the
/// handles on the observation record keep working after the fleet drops the
/// pool.
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
    // The handles hold the two registries, not the pool, so the pool itself
    // (indexer thread and radix tree) was reclaimed when the fleet dropped it.
    assert!(
        pool_token.is_cancelled(),
        "the pool is reclaimed promptly; only the inflight maps linger"
    );

    drop(handles);
}

/// Process shutdown still reaches every pool: `child_token` propagates from
/// parent to child.
#[tokio::test]
async fn cancelling_the_process_cancels_every_pool() {
    let process = CancellationToken::new();
    let p = Pool::from_spec(PoolId::next(), &cfg("a", 16), &process);
    assert!(!p.cancel.is_cancelled());
    process.cancel();
    assert!(p.cancel.is_cancelled(), "process shutdown must reach pools");
}
