use super::*;

fn telemetry(capacity: usize) -> Arc<Telemetry> {
    Telemetry::new(capacity, Arc::new(MetricsRegistry::new()))
}

fn meta(worker_id_num: u64, pool: &str) -> WorkerMeta {
    WorkerMeta {
        worker_id_num,
        id: format!("w{worker_id_num}"),
        pool: pool.to_string(),
        namespace: String::new(),
        ip: "10.0.0.1".to_string(),
        port: 8000,
        block_size: 16,
    }
}

/// The slot leak: `worker_id_num` is fleet-wide (in Kubernetes mode it is
/// derived from the pod UID), so two worker sets selecting one pod register
/// the same id twice. The second registration used to pop another slot and
/// overwrite `idx_of`, orphaning the first — repeat that and the fleet
/// stops growing at `telemetry_max_workers` with nothing in the logs to explain it.
#[test]
fn a_duplicate_registration_is_refused_and_leaks_no_slot() {
    let t = telemetry(4);
    assert!(t.register_worker(meta(7, "set-a")).is_ok());
    let err = t
        .register_worker(meta(7, "set-b"))
        .expect_err("a second claim on one worker_id_num must be refused");
    match err {
        RegisterError::AlreadyRegistered { worker_set } => {
            // Names the INCUMBENT, not the caller: the operator needs to
            // know which two Services overlap, and they already know which
            // one they were looking at.
            assert_eq!(worker_set, "set-a");
        }
        other => panic!("expected AlreadyRegistered, got {other:?}"),
    }
    // Three slots left, not two: register until exhaustion and count.
    for i in 0..3 {
        assert!(
            t.register_worker(meta(100 + i, "set-c")).is_ok(),
            "slot {i} should still be free"
        );
    }
    assert!(matches!(
        t.register_worker(meta(200, "set-c")),
        Err(RegisterError::SlotExhausted { capacity: 4 })
    ));
}

/// A refused registration must leave the incumbent's telemetry untouched —
/// same slot, same metadata — or the pool that legitimately owns the worker
/// loses its history to the pool that lost the race.
#[test]
fn a_refused_registration_does_not_disturb_the_incumbent() {
    let t = telemetry(4);
    let slot = t.register_worker(meta(7, "set-a")).unwrap();
    let _ = t.register_worker(meta(7, "set-b"));
    assert_eq!(t.idx_of.get(&7).map(|e| *e.value()), Some(slot));
    let rings = t.rings.lock().unwrap();
    assert_eq!(rings.meta[slot].as_ref().unwrap().pool, "set-a");
}

use crate::pool::test_pool;

/// `Telemetry` used to hold `Vec<Arc<KvIndexer>>`, cloned once at startup
/// for the index gauge. That is a strong reference the pool set cannot
/// revoke, so a pool removed from the registry kept its radix tree AND its
/// indexer OS thread alive for the life of the process. The rollup now
/// reads the registry per tick and holds nothing between them.
#[tokio::test]
async fn telemetry_does_not_pin_a_pool_that_left_the_registry() {
    let _t = telemetry(4);
    let registry = crate::pool::PoolRegistry::new();
    let pool = test_pool("transient");
    let token = pool.cancel.clone();
    registry.publish(Arc::new(crate::pool::PoolMap::from_pools(vec![
        pool.clone(),
    ])));

    // Retire it: publish a set without it, then drop the local handle. If
    // anything else held a strong reference the token would stay live.
    registry.publish(Arc::new(crate::pool::PoolMap::from_pools(Vec::new())));
    drop(pool);

    assert!(
        token.is_cancelled(),
        "a pool removed from the registry must be reclaimed, not pinned by telemetry"
    );

    // Falsifier for the assertion above: an `Arc<KvIndexer>` held outside
    // the pool is exactly what the deleted field was, and it does pin the
    // tree. Without this the test would pass even if the field came back.
    let pinned = test_pool("pinned");
    let token = pinned.cancel.clone();
    let held = pinned.indexer.clone();
    registry.publish(Arc::new(crate::pool::PoolMap::from_pools(Vec::new())));
    drop(pinned);
    assert!(
        !token.is_cancelled(),
        "an outstanding indexer handle should keep the tree alive — \
         if this fails the test above proves nothing"
    );
    drop(held);
}

/// Unregister must return the slot, so a worker legitimately moving between
/// worker sets re-registers cleanly rather than being told it is a duplicate
/// forever.
#[test]
fn a_worker_can_move_between_worker_sets_after_unregistering() {
    let t = telemetry(1);
    t.register_worker(meta(7, "set-a")).unwrap();
    t.unregister_worker(7);
    let slot = t
        .register_worker(meta(7, "set-b"))
        .expect("the only slot should be free again");
    let rings = t.rings.lock().unwrap();
    assert_eq!(rings.meta[slot].as_ref().unwrap().pool, "set-b");
}

/// Slot exhaustion is counted and surfaced, not merely returned as an error.
///
/// It used to be visible only as an ERROR log: a refused worker appears in no
/// metric — not even `kinference_workers`, since it is in neither state bucket —
/// so a fleet running below capacity looked identical to a correctly sized one.
#[test]
fn slot_exhaustion_is_counted_and_the_pool_reports_its_occupancy() {
    let t = telemetry(2);
    assert!(t.register_worker(meta(1, "p")).is_ok());
    assert!(t.register_worker(meta(2, "p")).is_ok());

    let err = t
        .register_worker(meta(3, "p"))
        .expect_err("the third must not fit a pool of two");
    assert!(matches!(err, RegisterError::SlotExhausted { capacity: 2 }));

    let d = t.rollup_once(None);
    assert_eq!(d.slots_used, 2);
    assert_eq!(d.slots_capacity, 2);
    assert_eq!(d.slots_denied, 1, "the refusal must be counted");

    // The counter is a per-tick delta, so a quiet tick reports zero rather than
    // re-reporting the same refusal.
    let d = t.rollup_once(None);
    assert_eq!(d.slots_denied, 0);
    assert_eq!(d.slots_used, 2, "occupancy is a gauge, not a delta");
}

/// A freed slot is reusable, so the cap bounds CONCURRENT workers.
#[test]
fn a_released_slot_is_reusable() {
    let t = telemetry(1);
    assert!(t.register_worker(meta(1, "p")).is_ok());
    assert!(t.register_worker(meta(2, "p")).is_err());
    t.unregister_worker(1);
    assert!(
        t.register_worker(meta(2, "p")).is_ok(),
        "the cap is on concurrent workers, not cumulative"
    );
    assert_eq!(t.rollup_once(None).slots_used, 1);
}

/// `kinference_workers` counts ROUTABLE workers, not dashboard slots.
///
/// The two are deliberately decoupled: the slot pool is a small, preallocated
/// dashboard budget, while the worker gauge has to cover everything actually
/// serving traffic. Before the split, a fleet larger than the pool reported
/// only as many workers as the dashboard could draw.
#[test]
fn worker_health_is_independent_of_the_telemetry_slot_pool() {
    let t = telemetry(2); // room to display two...
    let pool = crate::pool::test_pool("p");
    for i in 1..=5u64 {
        // ...but five are actually registered and routable.
        pool.registry.register(
            i,
            crate::workers::WorkerEntry::from_config(
                &toml::from_str(&format!(
                    "worker_id_str = \"w-{i}\"\nworker_id_num = {i}\nmodel_name = \"m\"\n"
                ))
                .expect("fixture parses"),
                16,
            ),
        );
    }
    let pools = crate::pool::PoolRegistry::from_pools(vec![pool]);

    let (ok, unresponsive) = t.worker_health(&pools);
    assert_eq!(
        ok + unresponsive,
        5,
        "all five must be counted, though only two could ever be displayed"
    );

    // And the slot pool still reports its own, smaller, budget.
    let d = t.rollup_once(None);
    assert_eq!(d.slots_capacity, 2);
    assert_eq!(d.slots_used, 0, "none of them took a display slot here");
}
