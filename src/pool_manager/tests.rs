use super::*;
use std::sync::Mutex;

use dynamo_kv_router::indexer::KvIndexerInterface;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct RecordingHooks {
    created: Mutex<Vec<String>>,
    retired: Mutex<Vec<String>>,
    fail_on: Mutex<Option<String>>,
}

impl PoolHooks for RecordingHooks {
    fn on_create(&self, pool: &Arc<Pool>, _spec: &PoolSpec) -> anyhow::Result<()> {
        if self.fail_on.lock().unwrap().as_deref() == Some(pool.name.as_str()) {
            anyhow::bail!("discovery refused to start");
        }
        self.created.lock().unwrap().push(pool.name.clone());
        Ok(())
    }
    fn on_retire(&self, pool: &Arc<Pool>) {
        self.retired.lock().unwrap().push(pool.name.clone());
    }
}

fn router()
-> Arc<std::sync::RwLock<std::collections::HashMap<String, std::collections::BTreeSet<PoolId>>>> {
    Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()))
}

struct Harness {
    mgr: PoolManager,
    registry: Arc<PoolRegistry>,
    hooks: Arc<RecordingHooks>,
    router: Arc<
        std::sync::RwLock<std::collections::HashMap<String, std::collections::BTreeSet<PoolId>>>,
    >,
    process: CancellationToken,
}

fn harness(reap: ReapPolicy) -> Harness {
    let registry = PoolRegistry::new();
    let hooks = Arc::new(RecordingHooks::default());
    let router = router();
    let process = CancellationToken::new();
    let mgr = PoolManager::new(
        registry.clone(),
        hooks.clone(),
        process.clone(),
        router.clone(),
        reap,
    );
    Harness {
        mgr,
        registry,
        hooks,
        router,
        process,
    }
}

/// Reap as soon as the pool is idle; tests that need a linger set one.
fn eager() -> ReapPolicy {
    ReapPolicy {
        linger: Duration::ZERO,
        backstop: Duration::from_secs(600),
    }
}

fn spec(name: &str, block_size: u32) -> PoolSpec {
    crate::pool::test_spec(name, block_size)
}

fn worker_cfg(num: u64) -> crate::discovery::WorkerConfig {
    toml::from_str(&format!(
        r#"
worker_id_str = "w-{num}"
worker_id_num = {num}
model_name = "old"
http_url = "http://10.0.0.{num}:8000"
"#
    ))
    .expect("valid worker config")
}

#[tokio::test]
async fn create_retire_reap() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    assert_eq!(h.registry.snapshot().len(), 1);
    let token = h
        .mgr
        .live_pool(&PoolKey::local("a"))
        .unwrap()
        .cancel
        .clone();

    let report = h.mgr.reconcile(Vec::new());
    assert_eq!(report.retired, vec![PoolKey::local("a")]);
    assert_eq!(
        h.registry.snapshot().len(),
        0,
        "retire must stop routing immediately"
    );
    assert!(
        !token.is_cancelled(),
        "retire must NOT cancel the token — the KvIndexer's channel send \
         unwraps, so anything still feeding it would panic"
    );
    assert_eq!(h.hooks.retired.lock().unwrap().as_slice(), ["a"]);

    assert_eq!(h.mgr.sweep(), 1);
    assert!(
        token.is_cancelled(),
        "reap must cancel so the indexer thread exits"
    );
    assert!(h.mgr.retired_keys().is_empty());
    assert!(
        !h.process.is_cancelled(),
        "a pool reap must not touch the process"
    );
}

/// A retired pool's indexer keeps answering until it is reaped, since
/// `KvIndexer` panics if reached after cancellation.
#[tokio::test]
async fn a_retired_pool_indexer_still_answers_until_reaped() {
    let mut h = harness(ReapPolicy {
        linger: Duration::from_secs(30),
        backstop: Duration::from_secs(600),
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let pool = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();

    h.mgr.reconcile(Vec::new());
    // Retired, not reaped: in-flight work still finds a working index.
    pool.indexer
        .find_matches(Vec::new())
        .await
        .expect("a retired pool's indexer must still answer");
    assert_eq!(h.mgr.sweep(), 0, "the handle above blocks the reap");
    assert!(!pool.cancel.is_cancelled());
}

/// A request inside the pool holds an `Arc<Pool>`, which blocks reaping.
#[tokio::test]
async fn a_pool_with_an_outstanding_handle_is_not_reaped() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let held = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    let token = held.cancel.clone();

    h.mgr.reconcile(Vec::new());
    assert_eq!(
        h.mgr.sweep(),
        0,
        "an outstanding handle must block the reap"
    );
    assert!(!token.is_cancelled());

    drop(held);
    assert_eq!(h.mgr.sweep(), 1);
    assert!(token.is_cancelled());
}

/// The backstop reaps a pool despite a leaked `Arc`.
#[tokio::test]
async fn the_backstop_reaps_past_the_deadline_even_when_held() {
    let mut h = harness(ReapPolicy {
        linger: Duration::ZERO,
        backstop: Duration::ZERO,
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let _leaked = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    h.mgr.reconcile(Vec::new());
    assert_eq!(
        h.mgr.sweep(),
        1,
        "the backstop must fire regardless of the count"
    );
}

/// Retiring and immediately recreating the same set resurrects it: same
/// `PoolId`, indexer and registry.
#[tokio::test]
async fn resurrection_preserves_the_pool_and_its_cache() {
    // Non-zero linger, so the sweeper can't reap before the recreate.
    let mut h = harness(ReapPolicy {
        linger: Duration::from_secs(30),
        backstop: Duration::from_secs(600),
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    let id_before = before.id;
    let indexer_before = Arc::as_ptr(&before.indexer);
    let registry_before = Arc::as_ptr(&before.registry);
    drop(before);

    h.mgr.reconcile(Vec::new());
    assert_eq!(h.mgr.sweep(), 0, "the linger keeps it resurrectable");

    let report = h.mgr.reconcile(vec![spec("a", 16)]);
    assert_eq!(report.resurrected, vec![PoolKey::local("a")]);
    assert!(report.created.is_empty());
    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_eq!(after.id, id_before, "same PoolId");
    assert_eq!(
        Arc::as_ptr(&after.indexer),
        indexer_before,
        "same KvIndexer — the whole point, or the fleet re-warms from cold"
    );
    assert_eq!(
        Arc::as_ptr(&after.registry),
        registry_before,
        "same worker registry"
    );
    assert!(!after.cancel.is_cancelled());
    assert!(
        h.mgr.retired_keys().is_empty(),
        "resurrection consumes the retired entry"
    );
}

/// A different spec builds a fresh pool rather than resurrecting.
#[tokio::test]
async fn a_changed_spec_does_not_resurrect() {
    let mut h = harness(ReapPolicy {
        linger: Duration::from_secs(30),
        backstop: Duration::from_secs(600),
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let id_before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().id;
    h.mgr.reconcile(Vec::new());

    let report = h.mgr.reconcile(vec![spec("a", 64)]);
    assert_eq!(report.created, vec![PoolKey::local("a")]);
    assert!(report.resurrected.is_empty());
    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_ne!(after.id, id_before);
    assert_eq!(after.block_size, 64);
}

/// An unchanged spec is a no-op.
#[tokio::test]
async fn reconciling_an_unchanged_set_does_nothing() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16), spec("b", 16)]);
    let ids: Vec<PoolId> = [PoolKey::local("a"), PoolKey::local("b")]
        .iter()
        .map(|k| h.mgr.live_pool(k).unwrap().id)
        .collect();

    let report = h.mgr.reconcile(vec![spec("a", 16), spec("b", 16)]);
    assert!(
        report.is_noop(),
        "steady state must be a no-op, got {report:?}"
    );
    let after: Vec<PoolId> = [PoolKey::local("a"), PoolKey::local("b")]
        .iter()
        .map(|k| h.mgr.live_pool(k).unwrap().id)
        .collect();
    assert_eq!(ids, after);
    assert_eq!(h.hooks.created.lock().unwrap().len(), 2, "no re-create");
}

/// Retuning keeps the pool and its cache; only the values change.
#[tokio::test]
async fn a_tuning_change_is_copy_on_write() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    let snapshot_before = h.registry.snapshot();

    let mut next = spec("a", 16);
    next.predictor.base_ms = 999.0;
    let report = h.mgr.reconcile(vec![next]);
    assert_eq!(report.updated, vec![PoolKey::local("a")]);

    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_eq!(after.id, before.id, "same PoolId");
    assert_eq!(after.predictor.base_ms, 999.0);
    assert_eq!(Arc::as_ptr(&after.indexer), Arc::as_ptr(&before.indexer));
    // A reader mid-pick keeps the values it started with.
    assert_eq!(
        snapshot_before.get(before.id).unwrap().predictor.base_ms,
        before.predictor.base_ms
    );
    assert_eq!(
        h.hooks.retired.lock().unwrap().len(),
        0,
        "an update is not a retire"
    );
}

/// Editing a Static set's declared model list reaches the already-registered
/// workers, and the router.
#[tokio::test]
async fn changing_the_declared_model_list_reseeds_the_live_workers() {
    let mut h = harness(eager());
    let mut first = spec("a", 16);
    first.static_models = Some(vec!["old".to_string()]);
    h.mgr.reconcile(vec![first]);

    let pool = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    pool.registry.register(
        1,
        crate::workers::WorkerEntry::from_config(&worker_cfg(1), 16),
    );
    pool.registry.set_served_models(1, &["old".to_string()]);
    crate::service::associate_models(&h.router, pool.id, "a", &["old".to_string()]);

    let mut next = spec("a", 16);
    next.static_models = Some(vec!["new".to_string()]);
    let report = h.mgr.reconcile(vec![next]);
    assert_eq!(report.updated, vec![PoolKey::local("a")]);

    // The worker now serves the new name...
    assert_eq!(
        pool.registry.by_model_len("new"),
        1,
        "the live worker must be re-seeded, not left on the old name"
    );
    assert_eq!(pool.registry.by_model_len("old"), 0);
    // ...and the router agrees, so a request for it lands here.
    let router = h.router.read().unwrap();
    assert!(router.contains_key("new"));
    assert!(
        !router.contains_key("old"),
        "the stale claim must go, or requests route here for a model \
         nothing in this pool serves"
    );
}

/// A block-size change replaces the pool.
#[tokio::test]
async fn a_block_size_change_replaces_the_pool() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let id_before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().id;

    let report = h.mgr.reconcile(vec![spec("a", 64)]);
    assert_eq!(report.replaced, vec![PoolKey::local("a")]);
    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_ne!(after.id, id_before);
    assert_eq!(after.block_size, 64);
    assert_eq!(h.hooks.retired.lock().unwrap().as_slice(), ["a"]);
}

/// A retired pool's model claims are released.
#[tokio::test]
async fn retiring_releases_the_model_claims() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let id = h.mgr.live_pool(&PoolKey::local("a")).unwrap().id;
    crate::service::associate_models(&h.router, id, "a", &["gemma".to_string()]);
    assert!(h.router.read().unwrap().contains_key("gemma"));

    h.mgr.reconcile(Vec::new());
    assert!(
        !h.router.read().unwrap().contains_key("gemma"),
        "a retired pool's claim must not outlive it"
    );
}

/// A pool whose discovery can't start isn't published.
#[tokio::test]
async fn a_failed_create_is_not_published() {
    let mut h = harness(eager());
    *h.hooks.fail_on.lock().unwrap() = Some("bad".to_string());
    let report = h.mgr.reconcile(vec![spec("good", 16), spec("bad", 16)]);
    assert_eq!(report.created, vec![PoolKey::local("good")]);
    assert_eq!(report.failed, vec![PoolKey::local("bad")]);
    assert_eq!(h.registry.snapshot().len(), 1);
    assert!(h.mgr.live_pool(&PoolKey::local("bad")).is_none());
}

/// Churn soak: every pool created ends up reaped with its token cancelled.
#[tokio::test]
async fn churn_leaves_nothing_behind() {
    let mut h = harness(eager());
    let mut tokens = Vec::new();
    for i in 0..200 {
        let name = format!("set-{i}");
        h.mgr.reconcile(vec![spec(&name, 16)]);
        tokens.push(
            h.mgr
                .live_pool(&PoolKey::local(&name))
                .unwrap()
                .cancel
                .clone(),
        );
        h.mgr.sweep();
    }
    h.mgr.reconcile(Vec::new());
    h.mgr.sweep();
    assert!(h.mgr.live_keys().is_empty());
    assert!(h.mgr.retired_keys().is_empty());
    assert_eq!(h.registry.snapshot().len(), 0);
    assert!(
        tokens.iter().all(|t| t.is_cancelled()),
        "every pool's token must be cancelled, or its indexer thread leaks"
    );
    assert!(!h.process.is_cancelled());
}
