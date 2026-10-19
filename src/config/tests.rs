use super::*;
use crate::fleet::profile::{PickerConfig, PredictorConfig, TokenizeMode};
use crate::router::metrics::TenantLabels;

#[test]
fn config_without_worker_sets_fails_validate() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses (pools defaults to empty)");
    let err = cfg
        .validate()
        .expect_err("empty worker_sets should fail validate");
    assert!(err.to_string().contains("at least one"));
}

#[test]
fn workerset_with_zero_block_size_fails_validate() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "t"

[[workerset]]
name = "p"
profile = "t"
kv_block_size = 0

[workerset.discovery]
mode = "kubernetes"
service_name = "svc"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    let err = cfg
        .validate()
        .expect_err("zero kv_block_size should fail validate");
    assert!(err.to_string().contains("kv_block_size"));
}

#[test]
fn duplicate_workerset_names_fail_validate() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "t"

[[workerset]]
name = "dup"
profile = "t"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "a"

[[workerset]]
name = "dup"
profile = "t"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "b"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    let err = cfg
        .validate()
        .expect_err("duplicate workerset names should fail");
    assert!(err.to_string().contains("duplicate workerset name"));
}

#[test]
fn static_workerset_without_workers_file_fails_validate() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "t"

[[workerset]]
name = "p"
profile = "t"
kv_block_size = 16
[workerset.discovery]
mode = "static"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    let err = cfg
        .validate()
        .expect_err("static workerset without workers_file should fail");
    assert!(err.to_string().contains("workers_file"));
}

#[test]
fn kubernetes_workerset_without_service_name_fails_validate() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "t"

[[workerset]]
name = "p"
profile = "t"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    let err = cfg
        .validate()
        .expect_err("kubernetes workerset without service_name should fail");
    assert!(err.to_string().contains("service_name"));
}

/// Build a minimal valid config with a `[leader_election]` body appended.
fn cfg_with_leader_election(body: &str) -> Config {
    let toml_str = format!(
        r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "t"

[[workerset]]
name = "p"
profile = "t"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "svc"

[leader_election]
{body}
"#
    );
    toml::from_str(&toml_str).expect("parses")
}

/// `renew_grace >= lease_duration` makes the holder permanently due for
/// renewal, so the elector never sleeps and rewrites the Lease as fast as
/// the apiserver will accept it. Caught at load.
#[test]
fn renew_grace_at_or_above_lease_duration_is_rejected() {
    for grace in [15, 16, 100] {
        let cfg = cfg_with_leader_election(&format!(
            "enabled = true\nlease_duration_secs = 15\nrenew_grace_secs = {grace}"
        ));
        let err = cfg
            .validate()
            .expect_err("grace >= duration must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("renew_grace_secs"), "got: {msg}");
        assert!(
            msg.contains("lease_duration_secs"),
            "the message must name both values so the fix is obvious: {msg}"
        );
    }
    // The shipped default pair is valid.
    cfg_with_leader_election("enabled = true")
        .validate()
        .expect("10 < 15 is fine");
}

#[test]
fn zero_and_empty_leader_election_settings_are_rejected() {
    for body in [
        "enabled = true\nlease_duration_secs = 0",
        "enabled = true\nrenew_grace_secs = 0",
        "enabled = true\nretry_secs = 0",
        "enabled = true\nlease_name = \"  \"",
    ] {
        cfg_with_leader_election(body).validate().unwrap_err();
    }
}

/// While disabled (the default), the timings are not validated.
#[test]
fn leader_election_defaults_off_and_is_not_validated_when_off() {
    let cfg = cfg_with_leader_election("");
    assert!(!cfg.leader_election.enabled);
    assert_eq!(cfg.leader_election.lease_name, "kinference");
    assert_eq!(cfg.leader_election.lease_duration_secs, 15);
    assert_eq!(cfg.leader_election.renew_grace_secs, 10);

    cfg_with_leader_election("lease_duration_secs = 1\nrenew_grace_secs = 99")
        .validate()
        .expect("timings are inert while disabled");
}

#[test]
fn a_profile_that_omits_everything_gets_the_struct_defaults() {
    // A profile with no [profile.predictor] / [profile.picker] tables
    // still resolves; expected_overlap_alpha defaults to 1.0.
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "t"

[[workerset]]
name = "p"
profile = "t"
kv_block_size = 64
[workerset.discovery]
mode = "kubernetes"
service_name = "svc"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    cfg.validate().expect("validates");
    let spec = &cfg.pool_specs().expect("resolves")[0];
    assert_eq!(spec.profile, "t");
    assert_eq!(spec.predictor.expected_overlap_alpha, 1.0);
    assert_eq!(spec.predictor.base_ms, PredictorConfig::default().base_ms);
    assert_eq!(spec.picker.top_k_min, PickerConfig::default().top_k_min);
    assert_eq!(spec.tokenize_mode, TokenizeMode::default());
}

/// An undefined profile error names both the worker set and the profile,
/// and lists the defined profiles.
#[test]
fn an_undefined_profile_is_rejected_and_names_both_sides() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "h100"

[[profile]]
name = "a100"

[[workerset]]
name = "ws"
profile = "typo-h100"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "svc"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    let err = cfg.validate().expect_err("unknown profile must fail");
    let msg = err.to_string();
    assert!(msg.contains("ws"), "names the worker set: {msg}");
    assert!(
        msg.contains("typo-h100"),
        "names the missing profile: {msg}"
    );
    assert!(
        msg.contains("a100") && msg.contains("h100"),
        "lists the defined profiles so the typo is obvious: {msg}"
    );
}

/// Two sets sharing a profile get identical tuning.
#[test]
fn two_worker_sets_sharing_a_profile_get_identical_tuning() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "shared"
[profile.predictor]
base_ms = 777.0
expected_overlap_alpha = 0.8
[profile.picker]
top_k_min = 3
[profile.tokenize]
mode = "local"

[[workerset]]
name = "a"
profile = "shared"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "svc-a"

[[workerset]]
name = "b"
profile = "shared"
kv_block_size = 64
[workerset.discovery]
mode = "kubernetes"
service_name = "svc-b"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    cfg.validate().expect("validates");
    let specs = cfg.pool_specs().expect("resolves");
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].predictor.base_ms, 777.0);
    assert_eq!(specs[0].predictor.expected_overlap_alpha, 0.8);
    assert_eq!(specs[0].picker.top_k_min, 3);
    assert_eq!(specs[0].tokenize_mode, TokenizeMode::Local);
    assert_eq!(specs[0].predictor, specs[1].predictor);
    assert_eq!(specs[0].picker, specs[1].picker);
    // ...but block size stays per set, since it depends on the engines.
    assert_eq!(specs[0].kv_block_size, 16);
    assert_eq!(specs[1].kv_block_size, 64);
}

#[test]
fn duplicate_profile_names_fail_validate() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[profile]]
name = "dup"

[[profile]]
name = "dup"

[[workerset]]
name = "ws"
profile = "dup"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "svc"
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    let err = cfg
        .validate()
        .expect_err("duplicate profile names must fail");
    assert!(err.to_string().contains("duplicate profile name"));
}

/// `profile` has no default: a worker set that omits it fails to parse.
#[test]
fn a_workerset_without_a_profile_does_not_parse() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[[workerset]]
name = "ws"
kv_block_size = 16
[workerset.discovery]
mode = "kubernetes"
service_name = "svc"
"#;
    let err = toml::from_str::<Config>(toml_str).expect_err("missing profile must not parse");
    assert!(err.to_string().contains("profile"), "got: {err}");
}

#[test]
fn workers_file_loads_worker_list() {
    // Write a temp workers file and load it via WorkersFile.
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "kinference-test-workers-{}.toml",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"
[[workers]]
worker_id_str = "w-1"
worker_id_num = 1
model_name = "gemma"
zmq_endpoint = "tcp://127.0.0.1:5557"

[[workers]]
worker_id_str = "w-2"
worker_id_num = 2
model_name = "gemma"
zmq_endpoint = "tcp://127.0.0.1:5558"
"#,
    )
    .expect("write temp workers file");
    let workers = WorkersFile::load(&path).expect("loads");
    assert_eq!(workers.len(), 2);
    assert_eq!(workers[0].worker_id_num, 1);
    assert_eq!(workers[1].worker_id_str, "w-2");
    let _ = std::fs::remove_file(&path);
}

/// `[metrics] tenant_labels` accepts its three forms, and an absent section
/// means `none`.
#[test]
fn tenant_labels_accepts_its_three_forms() {
    let parse = |section: &str| -> TenantLabels {
        let toml_str = format!("listen = \"127.0.0.1:50051\"\nmock_mode = true\n{section}");
        toml::from_str::<Config>(&toml_str)
            .expect("parses")
            .metrics
            .tenant_labels
    };
    assert_eq!(parse(""), TenantLabels::None, "absent section means none");
    assert_eq!(parse("[metrics]\n"), TenantLabels::None, "absent key too");
    assert_eq!(
        parse("[metrics]\ntenant_labels = \"none\"\n"),
        TenantLabels::None
    );
    assert_eq!(
        parse("[metrics]\ntenant_labels = \"all\"\n"),
        TenantLabels::All
    );
    assert_eq!(
        parse("[metrics]\ntenant_labels = [\"acme\", \"globex\"]\n"),
        TenantLabels::Allow(vec!["acme".into(), "globex".into()])
    );
    // An empty list is how a templating tool spells "none".
    assert_eq!(parse("[metrics]\ntenant_labels = []\n"), TenantLabels::None);
}

/// A mistyped value fails the parse, with an error naming the accepted
/// forms, rather than falling back to the default.
#[test]
fn a_mistyped_tenant_labels_value_is_rejected_rather_than_defaulted() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[metrics]
tenant_labels = "sure"
"#;
    let err =
        toml::from_str::<Config>(toml_str).expect_err("a bogus tenant_labels value must not parse");
    let msg = err.to_string();
    assert!(
        msg.contains("none") && msg.contains("all"),
        "the error must name the accepted forms, got: {msg}"
    );
    assert!(
        msg.contains("sure"),
        "the error must quote what was actually written, got: {msg}"
    );
}

/// The body caps are configured in KB and parse as written.
///
/// The default is the KB figure, and the old `_bytes` keys are rejected by
/// `deny_unknown_fields`, so a byte value can't be read as KB.
#[test]
fn body_caps_are_configured_in_kb() {
    let toml_str = r#"
listen = "127.0.0.1:50051"
mock_mode = true

body_max_total_kb = 1024
body_max_request_kb = 64
"#;
    let cfg: Config = toml::from_str(toml_str).expect("parses");
    assert_eq!(cfg.body_max_total_kb, Some(1024));
    assert_eq!(cfg.body_max_request_kb, Some(64));

    // The old spelling is a startup error.
    let stale = r#"
listen = "127.0.0.1:50051"
mock_mode = true

body_max_total_bytes = 268435456
"#;
    let err = toml::from_str::<Config>(stale).expect_err("the old key must not parse");
    let msg = err.to_string();
    assert!(
        msg.contains("body_max_total_bytes"),
        "the error must name the offending key, got: {msg}"
    );
    assert!(
        msg.contains("body_max_total_kb"),
        "and list the accepted ones, got: {msg}"
    );
}

/// A `.json` config loads through the same structs as TOML, so the chart's
/// `toJson` rendering and a hand-written TOML file mean the same thing.
#[test]
fn json_config_loads_like_toml() {
    let dir = tempfile::tempdir().expect("tempdir");
    let toml_path = dir.path().join("config.toml");
    let json_path = dir.path().join("config.json");
    std::fs::write(
        &toml_path,
        r#"
listen = "127.0.0.1:50051"
metrics_poll_ms = 1000

[metrics]
tenant_labels = ["acme"]

[[profile]]
name = "t"
[profile.predictor]
base_ms = 400.0

[kubernetes]
enabled = true
"#,
    )
    .expect("write toml");
    // Helm's `toJson` writes integral numbers without a fraction (`400`, not
    // `400.0`), for float fields as well as integer ones.
    std::fs::write(
        &json_path,
        r#"{
  "listen": "127.0.0.1:50051",
  "metrics_poll_ms": 1000,
  "metrics": {"tenant_labels": ["acme"]},
  "profile": [{"name": "t", "predictor": {"base_ms": 400}}],
  "kubernetes": {"enabled": true}
}"#,
    )
    .expect("write json");
    let from_toml = Config::load(&toml_path).expect("toml loads");
    let from_json = Config::load(&json_path).expect("json loads");
    assert_eq!(format!("{from_toml:?}"), format!("{from_json:?}"));
}

/// `deny_unknown_fields` holds for JSON too: a mistyped top-level key is a
/// startup error, not a silent default.
#[test]
fn json_config_rejects_unknown_top_level_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.json");
    std::fs::write(
        &path,
        r#"{"listen": "127.0.0.1:50051", "dashboard_listen": "0.0.0.0:9001"}"#,
    )
    .expect("write json");
    let err = Config::load(&path).expect_err("unknown key must not load");
    assert!(
        format!("{err:#}").contains("dashboard_listen"),
        "the error must name the offending key, got: {err:#}"
    );
}
