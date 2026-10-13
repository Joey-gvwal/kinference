use super::*;

#[test]
fn records_and_accumulates_per_model_and_qos() {
    let m = PromMetrics::new();
    m.record_usage("m-a", "interactive", "", 100, 40);
    m.record_usage("m-b", "batch", "", 7, 3);
    m.record_usage("m-a", "interactive", "", 50, 10); // accumulates onto m-a/interactive

    let out = m.render();
    assert!(
        out.contains(
            "kinference_prompt_tokens_total{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 150"
        ),
        "prompt m-a/interactive should be 100+50=150:\n{out}"
    );
    assert!(
        out.contains(
            "kinference_completion_tokens_total{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 50"
        ),
        "completion m-a/interactive should be 40+10=50:\n{out}"
    );
    // A different qos is a distinct series.
    assert!(
        out.contains(
            "kinference_prompt_tokens_total{model=\"m-b\",qos=\"batch\",tenant=\"default\"} 7"
        ),
        "prompt m-b/batch should be 7:\n{out}"
    );
    assert!(out.contains("# TYPE kinference_prompt_tokens_total counter"));
}

/// The only operator-visible reading of provider latency. A provider pick
/// holds no `PendingObservation`, so it never reaches `record_ttft` and
/// appears in no other histogram.
#[test]
fn provider_ttft_is_recorded_by_provider_and_model() {
    let m = PromMetrics::new();
    m.record_provider_ttft("default/databricks", "qwen-2.5", 420);
    m.record_provider_ttft("default/databricks", "qwen-2.5", 1500);
    m.record_provider_ttft("default/together", "llama-3", 90);
    let out = m.render();

    assert!(
        out.contains("# TYPE kinference_provider_ttft_ms histogram"),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_count{model=\"qwen-2.5\",provider=\"default/databricks\"} 2"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_sum{model=\"qwen-2.5\",provider=\"default/databricks\"} 1920"
        ),
        "{out}"
    );
    // A different provider is a distinct series — the dimension the removed
    // `status.observed.observedTTFTP95` carried, and the reason this exists.
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_count{model=\"llama-3\",provider=\"default/together\"} 1"
        ),
        "{out}"
    );
    // Same bucket edges as `kinference_ttft_ms`: the two get read side by side
    // when deciding whether to spill off-cluster.
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_bucket{model=\"qwen-2.5\",provider=\"default/databricks\",le=\"500\"} 1"
        ),
        "420ms falls in le=500, 1500ms does not:\n{out}"
    );
}

/// An empty label would pool unrelated providers into one series, which is
/// worse than dropping the sample.
#[test]
fn provider_ttft_skips_blank_labels() {
    let m = PromMetrics::new();
    m.record_provider_ttft("", "qwen-2.5", 100);
    m.record_provider_ttft("default/databricks", "", 100);
    let out = m.render();
    assert!(
        !out.contains("kinference_provider_ttft_ms_count"),
        "neither should have been recorded:\n{out}"
    );
}

#[test]
fn length_histograms_carry_model_and_qos() {
    let m = PromMetrics::new();
    m.record_usage("m-a", "interactive", "", 100, 40); // prompt ≤128, completion ≤64
    m.record_usage("m-a", "interactive", "", 5000, 300); // prompt ≤8192, completion ≤512
    let out = m.render();

    assert!(
        out.contains("# TYPE kinference_prompt_length_tokens histogram"),
        "{out}"
    );
    assert!(
        out.contains("kinference_prompt_length_tokens_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 2"),
        "{out}"
    );
    assert!(
        out.contains("kinference_prompt_length_tokens_sum{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 5100"),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_prompt_length_tokens_bucket{model=\"m-a\",qos=\"interactive\",tenant=\"default\",le=\"128\"} 1"
        ),
        "{out}"
    );
    assert!(
        out.contains("kinference_completion_length_tokens_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 2"),
        "{out}"
    );
}

#[test]
fn ttft_histogram_carries_model_and_qos() {
    let m = PromMetrics::new();
    m.record_ttft("m-a", "interactive", "", 250);
    m.record_ttft("m-a", "interactive", "", 1500);
    let out = m.render();

    assert!(out.contains("# TYPE kinference_ttft_ms histogram"), "{out}");
    assert!(
        out.contains(
            "kinference_ttft_ms_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 2"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_ttft_ms_sum{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 1750"
        ),
        "{out}"
    );
    assert!(
        out.contains("kinference_ttft_ms_bucket{model=\"m-a\",qos=\"interactive\",tenant=\"default\",le=\"250\"} 1"),
        "{out}"
    );
}

#[test]
fn itl_buckets_accumulate_cumulatively() {
    let m = PromMetrics::new();
    // Layout 1: 1 gap in bucket 0 (≤5ms), 2 gaps in bucket 7 (≤75ms).
    let mut counts = vec![0u32; 15];
    counts[0] = 1;
    counts[7] = 2;
    m.record_itl("m-a", "interactive", "", 1, &counts);
    let out = m.render();

    // Cumulative: le="5" sees only bucket 0; le="75" sees buckets 0..=7.
    // (The crate sorts labels alphabetically, so `le` renders first here —
    // unlike the native histograms, which append `le` last.)
    assert!(
        out.contains(
            "kinference_itl_ms_bucket{le=\"5\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 1"
        ),
        "le=5 should be 1:\n{out}"
    );
    assert!(
        out.contains(
            "kinference_itl_ms_bucket{le=\"50\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 1"
        ),
        "le=50 should still be 1 (bucket 7 is >50):\n{out}"
    );
    assert!(
        out.contains(
            "kinference_itl_ms_bucket{le=\"75\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 3"
        ),
        "le=75 should be 1+2=3:\n{out}"
    );
    assert!(
        out.contains("kinference_itl_ms_bucket{le=\"+Inf\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 3"),
        "+Inf should be total 3:\n{out}"
    );
    assert!(
        out.contains(
            "kinference_itl_ms_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 3"
        ),
        "count should be 3:\n{out}"
    );
}

#[test]
fn itl_skips_on_layout_or_length_mismatch() {
    let m = PromMetrics::new();
    let good = vec![1u32; 15];
    m.record_itl("m-a", "interactive", "", 1, &good); // total 15
    m.record_itl("m-a", "interactive", "", 2, &good); // wrong layout → skip
    m.record_itl("m-a", "interactive", "", 1, &[0, 1, 2]); // wrong length → skip
    let out = m.render();
    // Only the first (valid) report counts: 15 samples.
    assert!(
        out.contains(
            "kinference_itl_ms_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 15"
        ),
        "only the layout-1, length-15 report should count:\n{out}"
    );
}

#[test]
fn empty_model_is_skipped() {
    let m = PromMetrics::new();
    m.record_usage("", "interactive", "", 100, 40);
    m.record_ttft("", "interactive", "", 100);
    m.record_itl("", "interactive", "", 1, &[1u32; 15]);
    let out = m.render();
    assert!(
        !out.contains("model=\"\""),
        "empty model should be skipped:\n{out}"
    );
}

/// Build a registry with an explicit tenant policy.
fn with(labels: crate::config::TenantLabels) -> PromMetrics {
    PromMetrics::with_config(&crate::config::MetricsConfig {
        tenant_labels: labels,
    })
}

/// The allowlist decides which tenants keep their identity.
#[test]
fn tenant_labels_bucket_per_the_allowlist() {
    use crate::config::TenantLabels;

    // "none": every tenant collapses, including one that looks real.
    let m = with(TenantLabels::None);
    m.record_usage("m", "batch", "acme", 10, 5);
    let out = m.render();
    assert!(
        out.contains(
            r#"kinference_prompt_tokens_total{model="m",qos="batch",tenant="default"} 10"#
        ),
        "none must collapse every tenant:\n{out}"
    );
    assert!(
        !out.contains(r#"tenant="acme""#),
        "acme must not appear:\n{out}"
    );

    // "all": every tenant keeps its identity.
    let m = with(TenantLabels::All);
    m.record_usage("m", "batch", "acme", 10, 5);
    m.record_usage("m", "batch", "globex", 7, 3);
    let out = m.render();
    assert!(out.contains(r#"tenant="acme"} 10"#), "{out}");
    assert!(out.contains(r#"tenant="globex"} 7"#), "{out}");

    // A list: a hit keeps its identity, a miss buckets.
    let m = with(TenantLabels::Allow(vec!["acme".into()]));
    m.record_usage("m", "batch", "acme", 10, 5);
    m.record_usage("m", "batch", "globex", 7, 3);
    let out = m.render();
    assert!(
        out.contains(r#"tenant="acme"} 10"#),
        "allowlist hit:\n{out}"
    );
    assert!(
        out.contains(r#"tenant="default"} 7"#),
        "allowlist miss must bucket:\n{out}"
    );
    assert!(!out.contains(r#"tenant="globex""#), "{out}");
}

/// An empty tenant always buckets, whatever the policy says.
///
/// On the wire an absent tenant is the empty string rather than a presence bit,
/// and it is also what a feedback RPC sees when it found no stashed labels at
/// all. Neither is attributable, so neither gets its own series — note this
/// deliberately differs from the POLICY semantics, where `tenant: ""` is a
/// distinct matchable value.
#[test]
fn an_empty_tenant_always_buckets_under_default() {
    use crate::config::TenantLabels;
    for labels in [
        TenantLabels::None,
        TenantLabels::All,
        TenantLabels::Allow(vec![String::new()]),
    ] {
        let m = with(labels.clone());
        m.record_usage("m", "batch", "", 10, 5);
        let out = m.render();
        assert!(
            out.contains(
                r#"kinference_prompt_tokens_total{model="m",qos="batch",tenant="default"} 10"#
            ),
            "{labels:?} must bucket an empty tenant:\n{out}"
        );
    }
}

/// Where each tenant's requests went, by destination and type, under the
/// same tenant bucketing as every other tenant family.
#[test]
fn routed_requests_count_by_tenant_destination_and_type() {
    use crate::config::TenantLabels;
    use crate::policy::crd::BackendType::{Local, Provider};

    let m = with(TenantLabels::Allow(vec!["acme".into()]));
    m.record_routed("acme", "qwen-ws", Local);
    m.record_routed("acme", "qwen-ws", Local);
    m.record_routed("acme", "databricks", Provider);
    m.record_routed("globex", "qwen-ws", Local);
    let out = m.render();
    for line in [
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="acme",type="Local"} 2"#,
        r#"kinference_routed_requests_total{destination="databricks",tenant="acme",type="Provider"} 1"#,
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(
        !out.contains(r#"tenant="globex""#),
        "not allowlisted: {out}"
    );
}

/// A tenant literally named `default` shares the fallback bucket.
///
/// Documented rather than escaped. Under `All` there is no fallback so it is
/// harmless; under an allowlist it means an allowlisted `default` and every
/// non-allowlisted tenant land on one series.
#[test]
fn a_tenant_named_default_collides_with_the_fallback_bucket() {
    use crate::config::TenantLabels;
    let m = with(TenantLabels::Allow(vec!["default".into()]));
    m.record_usage("m", "batch", "default", 10, 5); // allowlisted
    m.record_usage("m", "batch", "someone-else", 7, 3); // bucketed
    let out = m.render();
    assert!(
        out.contains(
            r#"kinference_prompt_tokens_total{model="m",qos="batch",tenant="default"} 17"#
        ),
        "both land on the same series (10 + 7):\n{out}"
    );
}

/// Every label lands in the position its declaration names.
///
/// `with_label_values` matches by ORDER, not by name, and on `itl_bucket` all
/// four values are `&str` — so transposing `le` and `tenant` compiles cleanly
/// and emits series whose bucket edge is a tenant name. Self-identifying values
/// are what make that visible; asserting the label COUNT would not catch it.
#[test]
fn every_label_lands_in_its_declared_position() {
    use crate::config::TenantLabels;
    let m = with(TenantLabels::All);
    let mut counts = vec![0u32; 15];
    counts[0] = 1;
    m.record_usage("MODEL", "batch", "TENANT", 10, 5);
    m.record_ttft("MODEL", "batch", "TENANT", 250);
    m.record_request_duration("MODEL", "batch", "TENANT", 4_000);
    m.record_itl("MODEL", "batch", "TENANT", 1, &counts);
    let out = m.render();

    // Declared labels sort alphabetically; a native histogram appends `le`
    // after them, while `itl_bucket` declares `le` so it sorts first.
    for expect in [
        r#"kinference_prompt_tokens_total{model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_completion_tokens_total{model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_prompt_length_tokens_bucket{model="MODEL",qos="batch",tenant="TENANT",le="8"}"#,
        r#"kinference_completion_length_tokens_count{model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_ttft_ms_bucket{model="MODEL",qos="batch",tenant="TENANT",le="250"}"#,
        r#"kinference_request_duration_ms_bucket{model="MODEL",qos="batch",tenant="TENANT",le="5000"}"#,
        r#"kinference_itl_ms_bucket{le="5",model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_itl_ms_count{model="MODEL",qos="batch",tenant="TENANT"}"#,
    ] {
        assert!(
            out.contains(expect),
            "missing or transposed:\n  {expect}\n{out}"
        );
    }
    // The transposition this guards against, stated as its own assertion.
    assert!(
        !out.contains(r#"tenant="5""#) && !out.contains(r#"le="TENANT""#),
        "le and tenant are transposed:\n{out}"
    );
}

/// The e2e duration histogram shares `ttft_ms`'s bucket edges where they
/// overlap, so `duration - ttft` reads as decode time, and extends past them
/// because a full generation routinely outruns any TTFT.
#[test]
fn request_duration_is_comparable_to_ttft_and_has_a_longer_tail() {
    let m = PromMetrics::new();
    m.record_ttft("m", "batch", "", 250);
    m.record_request_duration("m", "batch", "", 250);
    let out = m.render();

    // Same edge, same bucket: an equal observation lands identically in both.
    for fam in ["kinference_ttft_ms", "kinference_request_duration_ms"] {
        assert!(
            out.contains(&format!(
                r#"{fam}_bucket{{model="m",qos="batch",tenant="default",le="250"}} 1"#
            )),
            "{fam} must bucket 250 at le=250:\n{out}"
        );
    }

    // The tail exists only on the duration family — a 5-minute generation must
    // not all pile into +Inf.
    assert!(
        out.contains(r#"kinference_request_duration_ms_bucket{model="m",qos="batch",tenant="default",le="300000"}"#),
        "duration needs buckets past ttft's 80s ceiling:\n{out}"
    );
    assert!(
        !out.contains(
            r#"kinference_ttft_ms_bucket{model="m",qos="batch",tenant="default",le="300000"}"#
        ),
        "ttft should NOT have grown a 300s bucket:\n{out}"
    );
}

/// A blank model is skipped, matching every other per-request recorder.
#[test]
fn request_duration_skips_a_blank_model() {
    let m = PromMetrics::new();
    m.record_request_duration("", "batch", "", 1_000);
    assert!(
        !m.render().contains("kinference_request_duration_ms"),
        "a blank model must not be bucketed under an empty label"
    );
}

/// Assert no SERIES line exists for a family.
///
/// Not `!contains(name)`: a family's name also appears in other families' HELP
/// text, which renders into the exposition. Only `#`-prefixed lines are
/// metadata, so a series check must skip them.
fn assert_no_series(out: &str, name: &str) {
    let found: Vec<&str> = out
        .lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(name))
        .collect();
    assert!(found.is_empty(), "expected no {name} series, got {found:?}");
}

/// Assert on a WHOLE rendered line, not a substring.
///
/// `contains` is a trap for numeric assertions: a rendered `... 200` contains
/// `... 20`, so a substring check silently passes on a wrong value that happens
/// to share a prefix. This caught a genuinely vacuous clamp assertion.
fn assert_line(out: &str, expect: &str) {
    let name = expect.split('{').next().unwrap();
    let got = out
        .lines()
        .find(|l| l.starts_with(name) && l.starts_with(&expect[..expect.rfind(' ').unwrap()]));
    assert_eq!(
        got,
        Some(expect),
        "\nexpected line: {expect}\nrendered:\n{out}"
    );
}

/// The counters accumulate blocks and the histogram observes the ratio.
#[test]
fn prefix_match_records_both_numerators_against_one_denominator() {
    let m = PromMetrics::new();
    // Two picks: 8/10 layered with 6 from the radix tree, then 5/10 with 5.
    m.record_prefix_match("m", "batch", 10, 8, 6);
    m.record_prefix_match("m", "batch", 10, 5, 5);
    let out = m.render();

    assert_line(
        &out,
        r#"kinference_prefix_requested_blocks_total{model="m",qos="batch"} 20"#,
    );
    assert_line(
        &out,
        r#"kinference_prefix_matched_blocks_total{model="m",qos="batch"} 13"#,
    );
    assert_line(
        &out,
        r#"kinference_prefix_radix_matched_blocks_total{model="m",qos="batch"} 11"#,
    );
    // 13/20 layered vs 11/20 radix — the difference is the in-flight credit,
    // which is the whole reason both numerators exist.

    assert_line(
        &out,
        r#"kinference_prefix_match_ratio_count{model="m",qos="batch"} 2"#,
    );
    // 0.8 and 0.5 → both at or below le=0.8, only one at or below le=0.5.
    assert_line(
        &out,
        r#"kinference_prefix_match_ratio_bucket{model="m",qos="batch",le="0.5"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_prefix_match_ratio_bucket{model="m",qos="batch",le="0.8"} 2"#,
    );
}

/// A degraded pick records nothing, in BOTH of its shapes.
///
/// This is the trap the whole design is arranged around. A tokenize failure
/// produces `requested == 0, layered == 0`; recording it as a ratio yields 0.0,
/// which is indistinguishable from a genuine cache miss — so a tokenize outage
/// would read as a prefix-routing collapse rather than as an outage. A
/// prefix-index failure can produce `layered > 0` against a zero-length prompt,
/// which is arithmetically incoherent and would divide by zero.
///
/// The call site in `pick_and_record` gates on `degraded_reason.is_none()`;
/// this pins the recorder's own backstop, which is what makes a future caller
/// unable to reintroduce the bug.
#[test]
fn a_degraded_pick_records_no_prefix_metrics() {
    for (label, layered) in [("tokenize", 0), ("prefix_index", 4)] {
        let m = PromMetrics::new();
        m.record_prefix_match("m", "batch", 0, layered, 0);
        assert!(
            !m.render().contains("kinference_prefix_"),
            "{label}-degraded picks must not reach the prefix metrics"
        );
    }
}

/// Matched blocks exceeding the prompt is clamped, and counted.
///
/// Overlap scores are summed across data-parallel ranks, so a DP>1 worker
/// publishing one prefix from two ranks reports roughly double. Left alone the
/// ratio would exceed 1.0; clamped silently, a real misconfiguration would be
/// invisible.
#[test]
fn an_overmatch_is_clamped_to_one_and_counted() {
    let m = PromMetrics::new();
    m.record_prefix_match("m", "batch", 10, 18, 18);
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_prefix_overmatch_total{model="m",qos="batch"} 1"#,
    );
    // Whole-line, not `contains`: an unclamped sum of 1.8 renders as
    // `..._sum{...} 1.8`, which CONTAINS `..._sum{...} 1` as a prefix and would
    // pass a substring check while the clamp was broken.
    let sum_line = out
        .lines()
        .find(|l| l.starts_with("kinference_prefix_match_ratio_sum{"))
        .expect("the histogram must have a _sum line");
    assert_eq!(
        sum_line, r#"kinference_prefix_match_ratio_sum{model="m",qos="batch"} 1"#,
        "the observation must be clamped to 1.0, not 1.8"
    );
    // The counters keep the raw values — only the ratio is clamped, so
    // rate(matched)/rate(requested) still shows the anomaly.
    assert_line(
        &out,
        r#"kinference_prefix_matched_blocks_total{model="m",qos="batch"} 18"#,
    );
}

/// A blank model is skipped, matching every other per-request recorder.
#[test]
fn prefix_match_skips_a_blank_model() {
    let m = PromMetrics::new();
    m.record_prefix_match("", "batch", 10, 8, 6);
    assert!(!m.render().contains("kinference_prefix_"));
}

/// Pick outcomes and latency are labelled by the WIRE outcome.
#[test]
fn picks_are_counted_and_timed_by_outcome() {
    let m = PromMetrics::new();
    m.record_pick("ok", 3.0);
    m.record_pick("ok", 30.0);
    m.record_pick("no_workers", 0.6);
    let out = m.render();

    assert_line(&out, r#"kinference_picks_total{outcome="ok"} 2"#);
    assert_line(&out, r#"kinference_picks_total{outcome="no_workers"} 1"#);
    // Latency is labelled by outcome because a rejected pick is fast and would
    // otherwise flatter the successful distribution.
    assert_line(
        &out,
        r#"kinference_pick_duration_ms_bucket{outcome="ok",le="5"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_pick_duration_ms_bucket{outcome="ok",le="50"} 2"#,
    );
    assert_line(
        &out,
        r#"kinference_pick_duration_ms_count{outcome="no_workers"} 1"#,
    );
}

/// `PickOutcome` maps to the wire names an operator actually sees, not the
/// Rust variant names.
#[test]
fn pick_outcome_labels_match_the_wire_names() {
    use crate::prom::PickOutcome;
    assert_eq!(PickOutcome::Success.as_label(), "ok");
    assert_eq!(PickOutcome::NoOpinion.as_label(), "bad_request");
    assert_eq!(PickOutcome::NotFound.as_label(), "no_workers");
}

/// The three tokenize paths are exhaustive: they sum to the number of attempts.
#[test]
fn tokenize_paths_partition_every_attempt() {
    let m = PromMetrics::new();
    for _ in 0..3 {
        m.record_tokenize_path("local");
        m.record_tokenize_duration(2.0);
    }
    m.record_tokenize_path("http_fallback");
    m.record_tokenize_duration(40.0);
    m.record_tokenize_path("unavailable");
    m.record_tokenize_duration(900.0);
    let out = m.render();

    assert_line(&out, r#"kinference_tokenize_total{path="local"} 3"#);
    assert_line(&out, r#"kinference_tokenize_total{path="http_fallback"} 1"#);
    assert_line(&out, r#"kinference_tokenize_total{path="unavailable"} 1"#);
    // One duration per attempt, so the histogram count is the path total.
    assert_line(&out, "kinference_tokenize_duration_ms_count 5");
}

/// Compare-mode agreement, including the divergence that silently breaks all
/// prefix hashing.
#[test]
fn tokenize_compare_counts_divergences_separately() {
    let m = PromMetrics::new();
    m.record_tokenize_compare("match");
    m.record_tokenize_compare("divergence");
    m.record_tokenize_compare("error");
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_tokenize_compare_total{result="match"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_tokenize_compare_total{result="divergence"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_tokenize_compare_total{result="error"} 1"#,
    );
}

/// Worker states are a gauge, so a recovery must bring the count back down —
/// not leave a stale `unresponsive` behind.
#[test]
fn worker_states_are_a_gauge_and_recover() {
    let m = PromMetrics::new();
    m.set_worker_states(2, 1);
    let out = m.render();
    assert_line(&out, r#"kinference_workers{state="ok"} 2"#);
    assert_line(&out, r#"kinference_workers{state="unresponsive"} 1"#);

    m.set_worker_states(3, 0);
    let out = m.render();
    assert_line(&out, r#"kinference_workers{state="ok"} 3"#);
    assert_line(&out, r#"kinference_workers{state="unresponsive"} 0"#);
}

/// An empty ITL report records nothing, and is not an error.
///
/// The proto prescribes empty `itl_bucket_counts` for a response with fewer
/// than two chunks — every non-streaming request qualifies. It used to fall
/// through to the length-mismatch branch and log a WARN per request, which both
/// spammed the log and made a real length bug indistinguishable from normal
/// traffic.
///
/// This pins the BEHAVIOUR (nothing recorded, correct before and after); the
/// log level is not asserted, since the suite installs no tracing subscriber.
#[test]
fn an_empty_itl_report_is_not_an_error() {
    let m = PromMetrics::new();
    m.record_itl("m", "batch", "", 1, &[]);
    assert!(
        !m.render().contains("kinference_itl_ms"),
        "an empty report has nothing to bucket"
    );
    // A genuinely wrong length is still refused.
    m.record_itl("m", "batch", "", 1, &[1, 2, 3]);
    assert!(!m.render().contains("kinference_itl_ms"));
    // And a correct one still lands.
    m.record_itl("m", "batch", "", 1, &[1u32; 15]);
    assert!(m.render().contains("kinference_itl_ms_count"));
}

/// Build identity is exported, and agrees with what the snapshot reports.
///
/// Conventional Prometheus shape: the value is always 1 and carries no meaning
/// — the information is in the labels. That is what makes `group_left` joins
/// possible, and what lets an alert fire when more than one version is live
/// mid-rollout.
///
/// Present on a FRESH registry, unlike every `*Vec` family, because it is set
/// at construction rather than on first observation. That is deliberate: a
/// router that has served nothing should still say what it is.
#[test]
fn build_info_reports_the_version_at_construction() {
    let out = PromMetrics::new().render();
    assert_line(
        &out,
        &format!(
            r#"kinference_build_info{{version="{}"}} 1"#,
            crate::version::VERSION
        ),
    );
    // The same string the dashboard and `--version` show, so a scrape and a
    // human cannot disagree about which build is running.
    assert!(
        !crate::version::VERSION.is_empty(),
        "an empty version label would be worse than none"
    );
}

/// Engine-measured cached prompt tokens accumulate per tenant.
#[test]
fn cached_prompt_tokens_accumulate_per_tenant() {
    use crate::config::TenantLabels;
    let m = with(TenantLabels::All);
    m.record_cached_prompt_tokens("qwen", "batch", "acme", Some(900));
    m.record_cached_prompt_tokens("qwen", "batch", "acme", Some(100));
    m.record_cached_prompt_tokens("qwen", "batch", "globex", Some(5));
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="qwen",qos="batch",tenant="acme"} 1000"#,
    );
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="qwen",qos="batch",tenant="globex"} 5"#,
    );
}

/// A MISSING cached_tokens is not a zero.
///
/// This is the distinction the whole `optional` field exists for. A caller on an
/// older engine reports nothing; treating that as "zero tokens were cached"
/// would understate the cache hit and therefore OVERCHARGE, silently and in the
/// operator's favour — the worst direction for a billing input to be wrong in.
#[test]
fn an_absent_cached_tokens_is_counted_separately_from_zero() {
    let m = PromMetrics::new();

    // Explicit zero: a real report of no cache hit. It contributes an
    // observation, and nothing to the absent counter.
    m.record_cached_prompt_tokens("m", "batch", "", Some(0));
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="m",qos="batch",tenant="default"} 0"#,
    );
    assert_line(&out, "kinference_cached_tokens_absent_total 0");

    // Absent: the caller said nothing.
    m.record_cached_prompt_tokens("m", "batch", "", None);
    m.record_cached_prompt_tokens("m", "batch", "", None);
    let out = m.render();
    assert_line(&out, "kinference_cached_tokens_absent_total 2");
    // ...and it must NOT have moved the token counter.
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="m",qos="batch",tenant="default"} 0"#,
    );
}

/// A blank model is skipped, matching every other per-request recorder.
#[test]
fn cached_prompt_tokens_skips_a_blank_model() {
    let m = PromMetrics::new();
    m.record_cached_prompt_tokens("", "batch", "", Some(10));
    assert_no_series(&m.render(), "kinference_cached_prompt_tokens_total");
}

// ── Pre-initialization of closed label sets ───────────────────────────────

/// Whole-line match. A bare `contains` would let `... 0` pass against a series
/// whose value merely STARTS with 0 (`0.5`, `40`), which is exactly the class
/// of vacuous assertion these tests exist to rule out.
fn has_line(out: &str, line: &str) -> bool {
    out.lines().any(|l| l == line)
}

/// Every closed-set child exists at zero on a registry that has seen no
/// traffic. This is the whole point of `preinit`: absent and zero are
/// different facts, and only one of them means "this has never happened".
#[test]
fn preinit_creates_every_closed_set_series_at_zero() {
    let out = PromMetrics::new().render();

    let counters: [(&str, &[&str]); 7] = [
        ("kinference_picks_total{outcome=", &PICK_OUTCOMES),
        ("kinference_score_degraded_total{reason=", &DEGRADE_REASONS),
        ("kinference_tokenize_total{path=", &TOKENIZE_PATHS),
        (
            "kinference_tokenize_compare_total{result=",
            &TOKENIZE_COMPARE_RESULTS,
        ),
        ("kinference_index_events_total{kind=", &INDEX_EVENT_KINDS),
        (
            "kinference_body_rejected_total{reason=",
            &BODY_REJECT_REASONS,
        ),
        ("kinference_body_aborted_total{reason=", &BODY_ABORT_REASONS),
    ];
    for (prefix, values) in counters {
        for v in values {
            let line = format!("{prefix}\"{v}\"}} 0");
            assert!(
                has_line(&out, &line),
                "missing seeded series `{line}`:\n{out}"
            );
        }
    }
    for v in POLICY_DECISION_OUTCOMES {
        let line = format!("kinference_policy_decisions_total{{outcome=\"{v}\"}} 0");
        assert!(has_line(&out, &line), "missing `{line}`:\n{out}");
    }
    for v in WORKERSET_REJECT_REASONS {
        let line = format!("kinference_workerset_rejected_total{{reason=\"{v}\"}} 0");
        assert!(has_line(&out, &line), "missing `{line}`:\n{out}");
    }
    // The histogram beside `picks` is deliberately NOT seeded: an all-zero
    // histogram makes `histogram_quantile` return nothing, so it would draw no
    // line while costing ~45 series. Pinned so nobody "fixes" the asymmetry.
    assert!(
        !out.lines()
            .any(|l| l.starts_with("kinference_pick_duration_ms_count{")),
        "pick_duration_ms must NOT be seeded — a zero histogram yields no \
         quantile, so the series would be pure cost:\n{out}"
    );
}

/// The other half of the contract, and the one that would actually hurt to get
/// wrong: the OPEN label sets must stay empty. Seeding a `{model,qos,tenant}`
/// family would invent unbounded cardinality, and seeding `policy_rule_matches`
/// would invent rule names that no policy declares.
#[test]
fn preinit_does_not_seed_open_label_sets() {
    let out = PromMetrics::new().render();
    for family in [
        "kinference_prompt_tokens_total",
        "kinference_completion_tokens_total",
        "kinference_cached_prompt_tokens_total",
        "kinference_request_duration_ms_count",
        "kinference_ttft_ms_count",
        "kinference_itl_ms_count",
        "kinference_provider_ttft_ms_count",
        "kinference_prefix_requested_blocks_total",
        "kinference_policy_rule_matches_total",
    ] {
        let opened = format!("{family}{{");
        assert!(
            !out.lines().any(|l| l.starts_with(&opened)),
            "`{family}` must have NO series before traffic; seeding it would \
             invent cardinality:\n{out}"
        );
    }
}

/// Ties [`PICK_OUTCOMES`] to its source of truth. The exhaustive `match` is
/// the mechanism: adding a variant fails to compile here, rather than silently
/// leaving one outcome unseeded.
#[test]
fn pick_outcome_labels_match_the_enum() {
    use crate::prom::PickOutcome as P;
    let all = [P::Success, P::NoOpinion, P::NotFound];
    for v in all {
        match v {
            P::Success | P::NoOpinion | P::NotFound => {}
        }
    }
    let from_enum: Vec<&str> = all.into_iter().map(|o| o.as_label()).collect();
    assert_eq!(from_enum, PICK_OUTCOMES);
}

#[test]
fn degrade_reason_labels_match_the_enum() {
    use crate::monitor::Degraded as D;
    let all = [D::Tokenize, D::PrefixIndex];
    for v in all {
        match v {
            D::Tokenize | D::PrefixIndex => {}
        }
    }
    let from_enum: Vec<&str> = all.into_iter().map(|r| r.as_label()).collect();
    assert_eq!(from_enum, DEGRADE_REASONS);
}

#[test]
fn body_reject_reason_labels_match_the_enum() {
    use crate::body_store::RejectReason as R;
    let all = [R::DeclaredTooLarge, R::RequestTooLarge, R::ServerFull];
    for v in all {
        match v {
            R::DeclaredTooLarge | R::RequestTooLarge | R::ServerFull => {}
        }
    }
    let from_enum: Vec<&str> = all.into_iter().map(|r| r.as_label()).collect();
    assert_eq!(from_enum, BODY_REJECT_REASONS);
}

/// `cached_tokens_absent` and `cached_tokens_reported` must PARTITION every
/// report — exactly one fires per call. That is what makes
/// `absent / (absent + reported)` an exact share.
///
/// The dashboard previously divided `absent` by a report count taken from a
/// LABELLED family, which is a sum-of-rates over many sparse per-tenant
/// children. That does not equal the rate of their sum, and the panel drifted
/// to 200%. Two unlabelled counters incremented at one call site cannot do
/// that.
#[test]
fn cached_tokens_reported_and_absent_partition_every_report() {
    let m = PromMetrics::new();
    m.record_cached_prompt_tokens("m", "interactive", "", Some(64));
    m.record_cached_prompt_tokens("m", "interactive", "", Some(0)); // a real zero still COUNTS
    m.record_cached_prompt_tokens("m", "interactive", "", None);
    m.record_cached_prompt_tokens("m", "interactive", "", None);
    // Skipped entirely: no model, so neither counter moves.
    m.record_cached_prompt_tokens("", "interactive", "", Some(5));

    let out = m.render();
    assert!(
        has_line(&out, "kinference_cached_tokens_reported_total 2"),
        "{out}"
    );
    assert!(
        has_line(&out, "kinference_cached_tokens_absent_total 2"),
        "{out}"
    );
    // The token SUM cannot substitute for the report count: a reported zero
    // adds nothing to it, which is exactly why the companion counter exists.
    assert!(
        has_line(
            &out,
            "kinference_cached_prompt_tokens_total{model=\"m\",qos=\"interactive\",tenant=\"default\"} 64"
        ),
        "{out}"
    );
}

/// The per-model split must zero a model that disappears, not freeze it.
///
/// A deleted worker set stops appearing in the input; a gauge left alone holds
/// its final count forever, which reads as a fleet that never shrank.
#[test]
fn workers_by_model_zeroes_a_model_that_disappears() {
    let m = PromMetrics::new();
    m.set_worker_states_by_model(&[("gemma".to_string(), 3, 1), ("qwen".to_string(), 2, 0)]);
    let out = m.render();
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"gemma\",state=\"ok\"} 3"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"gemma\",state=\"unresponsive\"} 1"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"qwen\",state=\"ok\"} 2"
        ),
        "{out}"
    );

    // qwen's worker set is deleted.
    m.set_worker_states_by_model(&[("gemma".to_string(), 3, 1)]);
    let out = m.render();
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"qwen\",state=\"ok\"} 0"
        ),
        "qwen must be ZEROED, not left at 2:\n{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"gemma\",state=\"ok\"} 3"
        ),
        "{out}"
    );
}

/// `kinference_workers` stays the authoritative fleet total. The per-model family
/// double-counts a worker that serves several models (LoRA), so the two are
/// deliberately separate rather than one labelled family.
#[test]
fn workers_total_is_independent_of_the_per_model_split() {
    let m = PromMetrics::new();
    m.set_worker_states(4, 0);
    // One worker serves both models, so the per-model counts sum to 5, not 4.
    m.set_worker_states_by_model(&[("gemma".to_string(), 3, 0), ("qwen".to_string(), 2, 0)]);
    let out = m.render();
    assert!(
        has_line(&out, "kinference_workers{state=\"ok\"} 4"),
        "fleet total unchanged:\n{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"gemma\",state=\"ok\"} 3"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_workers_by_model{model=\"qwen\",state=\"ok\"} 2"
        ),
        "{out}"
    );
}

/// Orphaned reports get their own counter rather than only being visible as a
/// difference between two others.
#[test]
fn orphaned_reports_are_counted_directly() {
    let m = PromMetrics::new();
    let out = m.render();
    assert!(
        has_line(&out, "kinference_orphaned_reports_total 0"),
        "unlabelled, so it exists from startup:\n{out}"
    );
    m.record_orphaned_report();
    m.record_orphaned_report();
    let out = m.render();
    assert!(
        has_line(&out, "kinference_orphaned_reports_total 2"),
        "{out}"
    );
}

/// An orphan is labelled `unknown`, not folded into `interactive`.
#[test]
fn unknown_qos_is_its_own_label_value() {
    use crate::monitor::Qos;
    assert_eq!(Qos::Unknown.as_label(), "unknown");
    // The four are distinct: nothing silently aliases onto another class.
    let labels =
        [Qos::Interactive, Qos::Batch, Qos::Deferrable, Qos::Unknown].map(|q| q.as_label());
    let uniq: std::collections::HashSet<&str> = labels.into_iter().collect();
    assert_eq!(uniq.len(), 4, "qos labels must be distinct: {labels:?}");

    // And it renders as a real series, not a dropped one.
    let m = PromMetrics::new();
    m.record_usage("m", Qos::Unknown.as_label(), "", 10, 5);
    let out = m.render();
    assert!(
        has_line(
            &out,
            "kinference_prompt_tokens_total{model=\"m\",qos=\"unknown\",tenant=\"default\"} 10"
        ),
        "{out}"
    );
}

/// The PICK path must keep mapping an unrecognized wire value to `Interactive`.
///
/// Declassifying there would change ROUTING, not just reporting: policy matches
/// on QoS, and `qos: [interactive]` rules would stop firing for callers sending
/// a value kinference does not know.
#[test]
fn from_proto_never_yields_unknown() {
    use crate::monitor::Qos;
    for wire in [-1i32, 0, 1, 2, 3, 99] {
        assert_ne!(
            Qos::from_proto(wire).as_label(),
            "unknown",
            "wire value {wire} must not declassify a pick"
        );
    }
}

/// Evictions are kinference's own decisions, seeded at zero so the graph reads
/// "none so far" rather than showing nothing.
#[test]
fn worker_evictions_are_counted_by_reason() {
    let m = PromMetrics::new();
    let out = m.render();
    for r in EVICTION_REASONS {
        assert!(
            has_line(
                &out,
                &format!("kinference_worker_evictions_total{{reason=\"{r}\"}} 0")
            ),
            "seeded at 0: {r}\n{out}"
        );
    }
    m.record_worker_eviction("block_size_mismatch");
    m.record_worker_eviction("block_size_mismatch");
    m.record_worker_eviction("model_mismatch");
    let out = m.render();
    assert!(
        has_line(
            &out,
            "kinference_worker_evictions_total{reason=\"block_size_mismatch\"} 2"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_worker_evictions_total{reason=\"model_mismatch\"} 1"
        ),
        "{out}"
    );
}

/// The label list must match the enum, and must NOT carry the mismatched
/// values — those are unbounded and would be cardinality, not a reason.
#[test]
fn eviction_reason_labels_match_the_enum() {
    use crate::eviction::EvictionReason as E;
    let all = [
        E::BlockSizeMismatch {
            expected: 256,
            observed: 64,
        },
        E::ModelMismatch {
            expected: vec!["a".into()],
            observed: vec!["b".into()],
        },
    ];
    for v in &all {
        match v {
            E::BlockSizeMismatch { .. } | E::ModelMismatch { .. } => {}
        }
    }
    let from_enum: Vec<&str> = all.iter().map(|r| r.as_label()).collect();
    assert_eq!(from_enum, EVICTION_REASONS);
    // Display carries the numbers; the label must not.
    let d = all[0].to_string();
    assert!(
        d.contains("256") && d.contains("64"),
        "Display should name both: {d}"
    );
    assert!(
        !all[0].as_label().contains("256"),
        "label must be value-free"
    );
}

/// The two calibration gauges publish per (worker, model), and a departed
/// worker leaves no reading behind.
///
/// The `reset()` is the part worth pinning. Without it a worker that is
/// evicted or rolled keeps its last value forever: a stale `0.95` reads as a
/// permanently saturated GPU and a stale `0.0` as permanent headroom. Both are
/// worse than absence, which Prometheus renders as a gap — and both would be
/// read by an operator calibrating a `kvCacheUsage` threshold, so the wrong
/// number here produces a wrong policy.
#[test]
fn worker_saturation_publishes_per_worker_and_does_not_go_stale() {
    let p = PromMetrics::new();
    p.set_worker_saturation(&[
        ("10.0.0.1:8000".into(), "gemma-4".into(), 0.3, 0.21),
        ("10.0.0.2:8000".into(), "gemma-4".into(), 4.5, 0.93),
    ]);
    let out = p.render();
    assert!(
        has_line(
            &out,
            "kinference_worker_queued_prompts_mean30s{model=\"gemma-4\",worker=\"10.0.0.1:8000\"} 0.3"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_worker_kv_cache_usage_mean30s{model=\"gemma-4\",worker=\"10.0.0.2:8000\"} 0.93"
        ),
        "{out}"
    );

    // The second worker goes away; only the first is published now.
    p.set_worker_saturation(&[("10.0.0.1:8000".into(), "gemma-4".into(), 0.0, 0.05)]);
    let out = p.render();
    assert!(
        !out.contains("10.0.0.2:8000"),
        "a departed worker must leave no series behind: {out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_worker_queued_prompts_mean30s{model=\"gemma-4\",worker=\"10.0.0.1:8000\"} 0"
        ),
        "{out}"
    );
}

/// The prediction histogram is labelled per worker and shares `kinference_ttft_ms`'s
/// bucket edges.
///
/// Shared edges are load-bearing, not tidiness: the predicted and the observed
/// distributions exist to be read against each other — "we predicted p95 of
/// 2s, the engine delivered 5s" is the calibration question — and different
/// bucket boundaries would make that comparison a guess.
#[test]
fn predicted_ttft_is_per_worker_and_shares_the_observed_buckets() {
    let p = PromMetrics::new();
    p.record_predicted_ttft("10.0.0.1:8000", "gemma-4", 120.0);
    p.record_predicted_ttft("10.0.0.1:8000", "gemma-4", 3000.0);
    p.record_predicted_ttft("10.0.0.2:8000", "gemma-4", 30.0);
    let out = p.render();

    // Both workers get their own series.
    assert!(
        has_line(
            &out,
            "kinference_predicted_ttft_ms_count{model=\"gemma-4\",worker=\"10.0.0.1:8000\"} 2"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_predicted_ttft_ms_count{model=\"gemma-4\",worker=\"10.0.0.2:8000\"} 1"
        ),
        "{out}"
    );

    // The bucket edges must be exactly the observed histogram's, or the two
    // cannot be compared. Both families must be populated BEFORE the render
    // the comparison reads — a histogram with no observations emits no
    // buckets, so comparing against an empty set would pass vacuously.
    p.record_ttft("gemma-4", "interactive", "acme", 120);
    let out = p.render();
    let edges = |metric: &str| -> Vec<String> {
        out.lines()
            .filter(|l| l.starts_with(&format!("{metric}_bucket{{")))
            .filter_map(|l| {
                let i = l.find("le=\"")? + 4;
                let j = l[i..].find('"')? + i;
                Some(l[i..j].to_string())
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let predicted = edges("kinference_predicted_ttft_ms");
    let observed = edges("kinference_ttft_ms");
    assert!(
        !predicted.is_empty() && !observed.is_empty(),
        "both families must have observations, else this compares empty sets"
    );
    assert_eq!(
        predicted, observed,
        "predicted and observed TTFT must share bucket edges to be comparable"
    );
}
