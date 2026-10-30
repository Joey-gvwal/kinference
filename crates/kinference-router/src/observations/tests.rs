use super::*;
use crate::picker::WorkerScore;
use crate::picker::predicted_wait_ms;
use std::sync::{Arc, Mutex};

fn synthetic_record() -> PendingObservation {
    let worker = WorkerScore {
        name: "qwen-vllm-1".to_string(),
        endpoint: "10.0.0.1:8000".to_string(),
        matched_blocks: 2,
        total_blocks: 4,
        queue_waiting: 2,
        queue_running: 1,
        queue_swapped: 0,
        kv_cache_usage: 0.23,
        completion_rate_per_sec: 2.5,
        block_size: 16,
    };
    let predictor_cfg = PredictorConfig::default();
    let picker_cfg = PickerConfig::default();
    let prediction = predicted_wait_ms(&worker, &predictor_cfg, &[], Instant::now());
    PendingObservation {
        request_id: 42,
        workerset: "set-a".to_string(),
        profile: "h100-tp2".to_string(),
        inflight: InflightHandles {
            inflight: std::sync::Arc::new(kinference_fleet::inflight::InflightRegistry::new()),
            prefixes: std::sync::Arc::new(kinference_fleet::inflight::InflightPrefixes::new()),
        },
        picked_at: Instant::now(),
        chosen_worker: worker,
        prediction,
        q_inflight: 3,
        kv_blocks: 2,
        inflight_workload_blocks: 17,
        q_ms_constant: 1000.0,
        q_ms_workload: 250.0,
        inflight_overlap_blocks: vec![5, 3],
        expected_overlap_alpha: 1.0,
        predictor_cfg,
        picker_cfg,
        external_request_id: Some("trace-abc-123".to_string()),
        replication_fired: false,
        model: "m".to_string(),
        streaming: true,
        qos: Qos::Interactive,
        tenant: "acme".to_string(),
    }
}

#[test]
fn csv_header_matches_format_csv_row_arity() {
    // The header and row column counts must match.
    let header_cols: Vec<&str> = CSV_HEADER.split(',').collect();
    let row = format_csv_row(&synthetic_record(), 0, 0.0, 200);
    let row_cols: Vec<&str> = row.split(',').collect();
    assert_eq!(
        header_cols.len(),
        row_cols.len(),
        "header has {} columns, row has {} — keep CSV_HEADER and \
         format_csv_row in lockstep",
        header_cols.len(),
        row_cols.len(),
    );
    // No trailing newline; the sink appends one.
    assert!(
        !CSV_HEADER.contains('\n'),
        "CSV_HEADER should not contain newlines"
    );
}

#[test]
fn csv_row_has_52_columns() {
    let r = synthetic_record();
    let line = format_csv_row(&r, 523, 111.0, 200);
    let cols: Vec<&str> = line.split(',').collect();
    assert_eq!(
        cols.len(),
        52,
        "expected 52 columns; got {}: {line}",
        cols.len()
    );
    assert_eq!(
        cols[24], "1000.0",
        "q_ms_constant should be in column 24: {line}"
    );
    assert_eq!(
        cols[25], "250.0",
        "q_ms_workload should be in column 25: {line}"
    );
    assert_eq!(
        cols[26], "5;3",
        "inflight_overlap_blocks should be in column 26: {line}"
    );
    assert_eq!(
        cols[27], "1.000",
        "expected_overlap_alpha should be in column 27: {line}"
    );
    // Cols 28-33: PredictorConfig snapshot (queue + cache + attention).
    let pred_default = PredictorConfig::default();
    assert_eq!(
        cols[28].parse::<f32>().unwrap(),
        pred_default.queue_ms_per_entry,
    );
    // Cols 34-37: PickerConfig snapshot (top-K + tie + inflight).
    let pkr_default = PickerConfig::default();
    assert_eq!(cols[34].parse::<u32>().unwrap(), pkr_default.top_k_min);
    assert_eq!(cols[36], "true", "inflight_tracking renders as true/false");
    // Col 38: x_request_id.
    assert_eq!(cols[38], "trace-abc-123");
    // Col 39: q_running_ms.
    let q_running_ms: f32 = cols[39].parse().expect("q_running_ms parses as f32");
    assert!(q_running_ms >= 0.0);
    // Col 40: decode_contention_ms.
    let decode_contention_ms: f32 = cols[40]
        .parse()
        .expect("decode_contention_ms parses as f32");
    assert!(decode_contention_ms >= 0.0);
    // Cols 41-45: q_running U-shape + capacity + decode coef snapshot.
    assert_eq!(
        cols[41].parse::<f32>().unwrap(),
        pred_default.q_running_penalty_peak_ms,
    );
    assert_eq!(
        cols[42].parse::<u32>().unwrap(),
        pred_default.q_running_penalty_peak_qr,
    );
    assert_eq!(
        cols[44].parse::<u32>().unwrap(),
        pred_default.inflight_concurrent_capacity,
    );
    assert_eq!(
        cols[45].parse::<f32>().unwrap(),
        pred_default.forward_pass_ms,
    );
    // Cols 46-48: replication snapshot.
    assert_eq!(
        cols[46].parse::<f32>().unwrap(),
        pkr_default.replication_threshold_pct,
    );
    assert_eq!(
        cols[47].parse::<u32>().unwrap(),
        pkr_default.replication_warm_target,
    );
    // Col 49: replication_fired (true/false on the synthetic record).
    assert_eq!(cols[49], "false", "replication_fired renders as true/false");
    assert!(!line.contains('\n'));
}

#[test]
fn csv_row_renders_replication_fired_as_true_when_set() {
    let mut r = synthetic_record();
    r.replication_fired = true;
    let line = format_csv_row(&r, 100, 0.0, 200);
    let cols: Vec<&str> = line.split(',').collect();
    assert_eq!(cols.len(), 52);
    assert_eq!(cols[49], "true");
}

#[test]
fn csv_row_emits_empty_x_request_id_when_header_unset() {
    let mut r = synthetic_record();
    r.external_request_id = None;
    let line = format_csv_row(&r, 100, 0.0, 200);
    let cols: Vec<&str> = line.split(',').collect();
    assert_eq!(cols.len(), 52);
    assert_eq!(cols[38], "");
}

#[test]
fn csv_row_sanitizes_commas_in_strings() {
    let mut r = synthetic_record();
    r.chosen_worker.name = "weird,name".to_string();
    r.chosen_worker.endpoint = "host,port".to_string();
    r.external_request_id = Some("x,y,z".to_string());
    let line = format_csv_row(&r, 500, 0.0, 200);
    let cols: Vec<&str> = line.split(',').collect();
    assert_eq!(cols.len(), 52);
    assert!(line.contains("weird;name"));
    assert!(line.contains("host;port"));
    assert_eq!(cols[38], "x;y;z");
}

#[test]
fn csv_row_emits_empty_inflight_overlap_list_when_no_dispatches() {
    let mut r = synthetic_record();
    r.inflight_overlap_blocks = vec![];
    let line = format_csv_row(&r, 100, 0.0, 200);
    let cols: Vec<&str> = line.split(',').collect();
    assert_eq!(cols.len(), 52);
    assert_eq!(cols[26], "");
}

/// Collects the JSON log lines that a subscriber writes.
#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Finalize `record` under a JSON subscriber that enables only the
/// observation target, and return the parsed events.
fn finalize_capturing(record: PendingObservation) -> Vec<serde_json::Value> {
    let buf = LogBuf::default();
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let (_registry, prom) = crate::tokenizer::test_support::router_metrics();
    let obs = Observations::new(
        Duration::from_secs(30),
        ObservationSink::open(None),
        Arc::new(kinference_policy::provider_health::ProviderHealthStore::new()),
        prom,
    );
    let id = record.request_id;
    obs.pending.insert(record);
    tracing::subscriber::with_default(subscriber, || {
        assert!(obs.finalize(id, 523, 200));
    });
    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    out.lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("one JSON object per line"))
        .filter(|e| e["target"] == OBSERVATION_TARGET)
        .collect()
}

#[test]
fn finalize_emits_one_observation_event_with_named_fields() {
    let events = finalize_capturing(synthetic_record());
    assert_eq!(events.len(), 1, "{events:?}");
    let e = &events[0];
    assert_eq!(e["level"], "DEBUG");
    assert_eq!(e["response_id"], 42);
    assert_eq!(e["req"], "trace-abc-123");
    assert_eq!(e["workerset"], "set-a");
    assert_eq!(e["profile"], "h100-tp2");
    assert_eq!(e["endpoint"], "10.0.0.1:8000");
    assert_eq!(e["upstream_status"], 200);
    assert_eq!(e["observed_ms"], 523);
    let predicted = e["predicted_ms"].as_f64().unwrap();
    let residual = e["residual_ms"].as_f64().unwrap();
    assert!((523.0 - predicted - residual).abs() < 1e-3, "{e}");
    assert_eq!(e["q_inflight"], 3);
    assert_eq!(e["q_ms_constant"], 1000.0);
    assert_eq!(e["inflight_dispatches"], 2);
    let overlap: Vec<u32> =
        serde_json::from_str(e["inflight_overlap_blocks"].as_str().unwrap()).unwrap();
    assert_eq!(overlap, [5, 3]);
    assert_eq!(e["replication_fired"], false);
    assert_eq!(
        e["queue_ms_per_entry"].as_f64().unwrap() as f32,
        PredictorConfig::default().queue_ms_per_entry
    );
    assert_eq!(e["top_k_min"], PickerConfig::default().top_k_min);
}

#[test]
fn finalize_logs_an_empty_overlap_list_when_nothing_is_in_flight() {
    let mut r = synthetic_record();
    r.inflight_overlap_blocks = vec![];
    let events = finalize_capturing(r);
    assert_eq!(events[0]["inflight_dispatches"], 0);
    assert_eq!(events[0]["inflight_overlap_blocks"], "[]");
}

#[test]
fn finalize_omits_req_when_the_caller_sent_none() {
    let mut r = synthetic_record();
    r.external_request_id = None;
    let events = finalize_capturing(r);
    assert_eq!(events.len(), 1);
    assert!(events[0].get("req").is_none(), "{}", events[0]);
}

#[test]
fn finalize_emits_no_observation_event_for_a_non_streaming_request() {
    let mut r = synthetic_record();
    r.streaming = false;
    assert!(finalize_capturing(r).is_empty());
}

#[test]
fn pending_observations_take_returns_inserted_entry() {
    let pending = PendingObservations::new(Duration::from_secs(30));
    let r = synthetic_record();
    let id = r.request_id;
    pending.insert(r);
    assert_eq!(pending.len(), 1);
    let taken = pending.take(id);
    assert!(taken.is_some());
    assert_eq!(pending.len(), 0);
    // Second take returns None: the entry was consumed.
    assert!(pending.take(id).is_none());
}

#[test]
fn pending_observations_take_returns_none_for_unknown_id() {
    let pending = PendingObservations::new(Duration::from_secs(30));
    assert!(pending.take(999).is_none());
}

#[test]
fn pending_observations_sweep_evicts_expired() {
    let pending = PendingObservations::new(Duration::from_millis(50));
    let mut r = synthetic_record();
    r.picked_at = Instant::now() - Duration::from_secs(1);
    pending.insert(r);
    assert_eq!(pending.len(), 1);
    let evicted = pending.take_expired();
    assert_eq!(evicted.len(), 1);
    assert_eq!(pending.len(), 0);
}

#[test]
fn pending_observations_sweep_keeps_fresh() {
    let pending = PendingObservations::new(Duration::from_secs(30));
    pending.insert(synthetic_record());
    assert_eq!(pending.take_expired().len(), 0);
    assert_eq!(pending.len(), 1);
}

/// `RequestLabels` round-trips the labels a feedback RPC reads back.
///
/// `ReportFinalToken` uses these to label the token and ITL metrics. Both
/// local and provider picks carry the caller's real `qos`.
#[test]
fn request_labels_round_trip_the_metric_labels() {
    let labels = RequestLabels::new(Duration::from_secs(600));

    // A local pick.
    labels.insert(
        1,
        RequestMeta {
            model: "qwen-2.5".to_string(),
            qos: Qos::Batch,
            tenant: "acme".to_string(),
            streaming: true,
            picked_at: Instant::now(),
            provider: None,
        },
    );
    let got = labels.take(1).expect("just inserted");
    assert_eq!(got.model, "qwen-2.5");
    assert_eq!(got.qos, Qos::Batch, "the caller's qos, not a default");
    assert_eq!(got.tenant, "acme");
    assert!(got.provider.is_none());

    // Taking consumes, so a second ReportFinalToken finds nothing.
    assert!(labels.take(1).is_none(), "take must consume the entry");

    // A provider pick carries the same request-side labels plus `provider`.
    // `Deferrable`, since it is neither the default nor the first variant.
    labels.insert(
        2,
        RequestMeta {
            model: "llama-3".to_string(),
            qos: Qos::Deferrable,
            tenant: "globex".to_string(),
            // Non-streaming, whose reported first-token time is total latency.
            streaming: false,
            picked_at: Instant::now(),
            provider: Some(kinference_policy::engine::ObjectKey {
                namespace: "default".to_string(),
                name: "together".to_string(),
            }),
        },
    );
    // A peek must not consume, since provider feedback arrives in stages.
    assert!(labels.provider_pick_for(2).is_some());
    // The peek carries `streaming`, which the provider feedback path needs.
    let peek = labels.provider_pick_for(2).expect("still present");
    assert!(!peek.streaming, "the peek must carry the streaming flag");
    assert_eq!(peek.model, "llama-3");

    let got = labels.take(2).expect("peek must not have consumed it");
    assert_eq!(got.qos, Qos::Deferrable, "off-cluster picks keep their qos");
    assert_eq!(got.tenant, "globex");
    assert!(!got.streaming);
}

#[test]
fn observation_sink_disabled_without_a_path() {
    let sink = ObservationSink::open(None);
    sink.append_line("anything");
}

#[test]
fn observation_sink_writes_header_on_fresh_file() {
    // Distinct path per test so parallel runs don't trample.
    let tmp = std::env::temp_dir().join(format!("kinference-csv-fresh-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let sink = ObservationSink::open(Some(tmp.clone()));
    sink.append_line("row-one");
    sink.append_line("row-two");
    let contents = std::fs::read_to_string(&tmp).expect("sink file written");
    let expected = format!("{CSV_HEADER}\nrow-one\nrow-two\n");
    assert_eq!(contents, expected, "got: {contents:?}");
    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn observation_sink_appends_silently_to_existing_file() {
    // Distinct path per test so parallel runs don't trample.
    let tmp = std::env::temp_dir().join(format!(
        "kinference-csv-existing-{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&tmp);
    // Pre-seed the file. The sink must not prepend a header.
    std::fs::write(&tmp, "pre-existing-row\n").expect("seed");

    let sink = ObservationSink::open(Some(tmp.clone()));
    sink.append_line("new-row");

    let contents = std::fs::read_to_string(&tmp).expect("sink file readable");
    assert_eq!(contents, "pre-existing-row\nnew-row\n", "got: {contents:?}");
    let _ = std::fs::remove_file(&tmp);
}
