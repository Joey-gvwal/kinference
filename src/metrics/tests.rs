use super::*;

fn parse_cfg(body: &str) -> (u32, u32) {
    // Mirror the body massage scrape_once does before parsing.
    let body = body.replace("vllm:", "vllm_");
    let scrape = prometheus_parse::Scrape::parse(body.lines().map(|l| Ok(l.to_owned())))
        .expect("synthetic body parses");
    let num_gpu_blocks = scrape
        .samples
        .iter()
        .find(|s| s.metric == "vllm_cache_config_info")
        .and_then(|s| s.labels.get(NUM_GPU_BLOCKS_LABEL))
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    let block_size = scrape
        .samples
        .iter()
        .find(|s| s.metric == "vllm_cache_config_info")
        .and_then(|s| s.labels.get(BLOCK_SIZE_LABEL))
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    (num_gpu_blocks, block_size)
}

/// Canonical vLLM cache_config_info shape: one info-gauge sample
/// with all cache-config fields encoded as labels. The
/// `vllm:`/`vllm_` rewrite in scrape_once is exercised too.
#[test]
fn parse_canonical_vllm_cache_config_info_succeeds() {
    let body = "# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig\n\
                # TYPE vllm:cache_config_info gauge\n\
                vllm:cache_config_info{block_size=\"64\",cache_dtype=\"auto\",\
                cpu_offload_gb=\"0\",enable_prefix_caching=\"True\",engine=\"0\",\
                gpu_memory_utilization=\"0.9\",is_attention_free=\"False\",\
                num_cpu_blocks=\"0\",num_gpu_blocks=\"1234\"} 1.0\n";
    let (gpu, bs) = parse_cfg(body);
    assert_eq!(gpu, 1234, "num_gpu_blocks parses");
    assert_eq!(bs, 64, "block_size parses");
}

/// vLLM build that omits `block_size` from the cache_config_info
/// labels (e.g. an older release or a gateway stripping the
/// label). num_gpu_blocks still works; block_size returns 0 —
/// matches what the production code observes when the
/// `mark_block_size_resolved` fallback fires inappropriately.
#[test]
fn parse_cache_config_info_without_block_size_label() {
    let body = "# TYPE vllm_cache_config_info gauge\n\
                vllm_cache_config_info{engine=\"0\",num_gpu_blocks=\"1234\"} 1.0\n";
    let (gpu, bs) = parse_cfg(body);
    assert_eq!(gpu, 1234);
    assert_eq!(bs, 0, "missing label → 0 (caller treats as unknown)");
}

/// The mean damps a spike; the raw gauge does not.
///
/// This is the whole reason the window exists. On a real fleet the onset of
/// saturation was a single sample at `waiting = 5` that cleared within ten
/// seconds — a policy thresholding the instantaneous value drops the worker on
/// that transient, and under `onEmpty: Reject` can refuse the request over it.
/// One spike in thirty idle samples must land far below one sustained prompt.
#[test]
fn a_queue_spike_is_damped_by_the_window_but_a_sustained_queue_is_not() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    let tick = |n: u32| t0 + Duration::from_secs(n as u64);

    // 29 idle seconds and one spike to 5 — the shape actually observed.
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
/// The rate-computing siblings return `0.0` until they hold two samples,
/// because a rate needs two points. A mean does not — and a warm-up zero would
/// make a freshly discovered, already-saturated worker read as idle for its
/// first tick, which is exactly when a `drop` rule most needs the truth.
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
/// Without this a worker that churns keeps its predecessor's window, so a
/// replacement pod inherits the readings of the pod it replaced — and a
/// `kvCacheUsage` threshold would fire on a GPU that no longer exists.
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
