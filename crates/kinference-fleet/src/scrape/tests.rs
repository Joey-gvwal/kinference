use super::*;

async fn delayed_http(
    body: &'static str,
) -> (
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let received = socket.read(&mut request).await.unwrap();
        assert!(received > 0);
        started_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    (url, started_rx, release_tx)
}

fn scrape_context(
    pool: &crate::pool::Pool,
    http_url: Option<String>,
    metrics_url: Option<String>,
) -> WorkerContext {
    use crate::discovery::WorkerConfig;
    use crate::workers::WorkerEntry;
    let (eviction_tx, _) = crate::eviction::channel();
    pool.registry.register(
        42,
        WorkerEntry {
            worker_id_str: "worker:5000".into(),
            model_name_hint: None,
            current_models: Vec::new(),
            http_url: http_url.clone(),
            block_size: 64,
            initial_sync_complete: false,
            kv_events_observed: false,
            score_addr: "worker:5000".into(),
            engine: None,
            pod_name: None,
            namespace: None,
            node_name: None,
            node_ip: None,
            pod_ip: None,
            availability_zone: None,
            instance_type: None,
        },
    );
    WorkerContext {
        spec_updates: pool.spec_updates.subscribe(),
        cfg: WorkerConfig {
            worker_id_str: "worker:5000".into(),
            worker_id_num: 42,
            http_url,
            metrics_url,
            ..Default::default()
        },
        endpoint_key: "worker:5000".into(),
        block_size: 64,
        prom: Arc::new(crate::metrics::FleetMetrics::new(
            &prometheus::Registry::new(),
        )),
        registry: pool.registry.clone(),
        indexer: pool.indexer.clone(),
        inflight_prefixes: pool.inflight_prefixes.clone(),
        eviction_tx,
        models: Arc::new(crate::models::ModelIndex::default()),
        pool_id: pool.id,
        pool_name: pool.name.clone(),
        cancel: pool.cancel.child_token(),
    }
}

async fn wait_for_scrape_exit(registry: &Arc<crate::workers::WorkerRegistry>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while Arc::strong_count(registry) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scrape task exited");
}

#[tokio::test]
async fn an_unregistered_worker_does_not_publish_late_warmup_results() {
    let (url, started, release) =
        delayed_http(r#"{"data":[{"id":"late-model","owned_by":"vllm"}]}"#).await;
    let pool = crate::pool::test_pool("late");
    let context = scrape_context(&pool, Some(url), None);
    let index = context.models.clone();
    let samples = Arc::new(MetricsRegistry::new());
    spawn_one_shot_warmup(context, samples.clone());
    started.await.unwrap();
    pool.registry.unregister(42);
    samples.forget(42);
    release.send(()).unwrap();
    wait_for_scrape_exit(&pool.registry).await;
    assert!(samples.models(42).models.is_empty());
    assert!(index.pools_for("late-model").is_empty());
}

#[tokio::test]
async fn a_registered_worker_publishes_its_warmup_results() {
    let (url, started, release) =
        delayed_http(r#"{"data":[{"id":"live-model","owned_by":"vllm"}]}"#).await;
    let pool = crate::pool::test_pool("live");
    let context = scrape_context(&pool, Some(url), None);
    let index = context.models.clone();
    let samples = Arc::new(MetricsRegistry::new());
    spawn_one_shot_warmup(context, samples.clone());
    started.await.unwrap();
    release.send(()).unwrap();
    wait_for_scrape_exit(&pool.registry).await;
    assert_eq!(samples.models(42).models.len(), 1);
    assert_eq!(index.pools_for("live-model"), vec![pool.id]);
    assert_eq!(pool.registry.lookup(42).unwrap().engine, Some(Engine::Vllm));
}

#[tokio::test]
async fn a_cancelled_worker_does_not_publish_late_warmup_results() {
    let (url, started, release) =
        delayed_http(r#"{"data":[{"id":"late-model","owned_by":"vllm"}]}"#).await;
    let pool = crate::pool::test_pool("late");
    let context = scrape_context(&pool, Some(url), None);
    let cancel = context.cancel.clone();
    let index = context.models.clone();
    let samples = Arc::new(MetricsRegistry::new());
    spawn_one_shot_warmup(context, samples.clone());
    started.await.unwrap();
    cancel.cancel();
    samples.forget(42);
    release.send(()).unwrap();
    wait_for_scrape_exit(&pool.registry).await;
    assert!(samples.models(42).models.is_empty());
    assert!(index.pools_for("late-model").is_empty());
}

#[tokio::test]
async fn a_cancelled_worker_does_not_restore_forgotten_metrics() {
    let (url, started, release) = delayed_http("vllm:num_requests_waiting 3\n").await;
    let pool = crate::pool::test_pool("late");
    let context = scrape_context(&pool, None, Some(url));
    let cancel = context.cancel.clone();
    let samples = Arc::new(MetricsRegistry::new());
    spawn_metrics_poller(context, samples.clone(), 1, 0);
    started.await.unwrap();
    cancel.cancel();
    pool.registry.unregister(42);
    samples.forget(42);
    release.send(()).unwrap();
    wait_for_scrape_exit(&pool.registry).await;
    assert!(samples.sample(42).is_none());
}

/// The mean damps a spike; the raw gauge does not. One spike in thirty idle
/// samples lands far below one sustained prompt.
#[test]
fn a_queue_spike_is_damped_by_the_window_but_a_sustained_queue_is_not() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    let tick = |n: u32| t0 + Duration::from_secs(n as u64);

    // 29 idle seconds and one spike to 5.
    let mut spike = 0.0;
    for i in 0..30 {
        let waiting = if i == 14 { 5 } else { 0 };
        spike = reg.record_and_compute_means(1, tick(i), waiting, 0.0).0;
    }
    assert!(
        spike > 0.0 && spike < 0.5,
        "one spike in 30 samples must be well under a single sustained prompt, got {spike}"
    );

    // A different worker, genuinely backed up the whole window.
    let mut sustained = 0.0;
    for i in 0..30 {
        sustained = reg.record_and_compute_means(2, tick(i), 5, 0.0).0;
    }
    assert!(
        (sustained - 5.0).abs() < 0.01,
        "a queue of 5 held for the window reads ~5, got {sustained}"
    );

    // And the separation is what a threshold exploits: `greaterThan: 1` admits
    // the spiky worker and drops the backed-up one.
    assert!(spike < 1.0 && sustained > 1.0, "{spike} vs {sustained}");
}

/// A single sample is a valid mean, with no warm-up zero.
///
/// Unlike the rates, which need two points, so a new saturated worker doesn't
/// read as idle.
#[test]
fn the_first_sample_is_already_a_usable_mean() {
    let reg = MetricsRegistry::new();
    let (q, kv) = reg.record_and_compute_means(1, Instant::now(), 7, 0.93);
    assert_eq!(q, 7.0, "no two-sample floor");
    assert!((kv - 0.93).abs() < 1e-6, "got {kv}");
}

/// Samples older than the window leave, so the mean tracks a recovery.
#[test]
fn the_window_evicts_so_a_recovered_worker_stops_reading_saturated() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    // Saturated for a full window...
    for i in 0..30 {
        reg.record_and_compute_means(1, t0 + Duration::from_secs(i), 10, 0.99);
    }
    // ...then idle for longer than the window.
    let mut q = f32::NAN;
    let mut kv = f32::NAN;
    for i in 31..=70 {
        let (a, b) = reg.record_and_compute_means(1, t0 + Duration::from_secs(i), 0, 0.0);
        q = a;
        kv = b;
    }
    assert_eq!(q, 0.0, "the saturated samples must have aged out, got {q}");
    assert_eq!(kv, 0.0, "got {kv}");
}

/// `forget` drops the history too.
///
/// So a replacement worker doesn't inherit its predecessor's readings.
#[test]
fn forgetting_a_worker_clears_its_window() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    for i in 0..30 {
        reg.record_and_compute_means(1, t0 + Duration::from_secs(i), 10, 0.99);
    }
    reg.forget(1);
    let (q, kv) = reg.record_and_compute_means(1, t0 + Duration::from_secs(30), 0, 0.0);
    assert_eq!(q, 0.0, "history survived forget(), got {q}");
    assert_eq!(kv, 0.0, "got {kv}");
}

/// A worker that has never been scraped is unknown, not idle. `latest` still
/// flattens it to zeros for display; `sample` is what decisions read.
#[test]
fn sample_is_none_until_the_first_scrape() {
    let reg = MetricsRegistry::new();
    assert!(reg.sample(7).is_none());
    assert_eq!(reg.latest(7).waiting_mean, 0.0);
    reg.record_sample_for_test(
        7,
        MetricsSample {
            waiting_mean: 2.5,
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );
    assert_eq!(reg.sample(7).map(|s| s.waiting_mean), Some(2.5));
}
