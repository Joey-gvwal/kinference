//! `kinfctl` end to end: the real binary, real files, real exit statuses.
//!
//! The checks themselves are unit-tested in `src/policy/check/tests.rs`. What
//! only a test of the binary can catch is the contract a script or an agent
//! relies on: which inputs it reads, what goes to stdout versus stderr, and
//! that the exit status alone separates "clean", "findings" and "could not
//! check".

use std::io::Write;
use std::process::{Command, Output, Stdio};

const KINFCTL: &str = env!("CARGO_BIN_EXE_kinfctl");
const DEMO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/k8s/examples/demo-configs");
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/kinfctl");

fn kinfctl(args: &[&str]) -> Output {
    Command::new(KINFCTL)
        .args(args)
        .output()
        .expect("kinfctl runs")
}

fn kinfctl_with_stdin(args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(KINFCTL)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("kinfctl runs");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

/// What `kubectl get infpool,iws,infprov -o yaml` prints for the demo fleet:
/// one `kind: List` of the objects as the API server returns them, with no
/// `---` at the end. Their status is deliberately not one this build's types
/// accept: it is the server's, and kinfctl must not trip over it.
fn live_objects() -> String {
    let mut items = Vec::new();
    for f in [
        "pool-all.yaml",
        "workerset-gemma-4.yaml",
        "workerset-qwen-2-5.yaml",
        "workerset-qwen-3-6-35b-a3b.yaml",
        "databricks/provider.yaml",
    ] {
        let text = std::fs::read_to_string(format!("{DEMO}/{f}")).unwrap();
        for mut obj in serde_saphyr::from_multiple::<serde_json::Value>(&text).unwrap() {
            if !obj["kind"]
                .as_str()
                .is_some_and(|k| k.starts_with("Inference"))
            {
                continue;
            }
            let meta = obj["metadata"].as_object_mut().unwrap();
            meta.entry("namespace").or_insert("default".into());
            meta.insert("resourceVersion".into(), "123456".into());
            meta.insert("uid".into(), "6f1c2b9e-0000-4000-8000-000000000000".into());
            obj["status"] = serde_json::json!({
                "conditions": [{"type": "Accepted", "status": "True"}],
            });
            items.push(obj);
        }
    }
    serde_saphyr::to_string(&serde_json::json!({
        "apiVersion": "v1",
        "items": items,
        "kind": "List",
        "metadata": {"resourceVersion": ""},
    }))
    .unwrap()
}

fn demo_files() -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(DEMO)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .map(|p| p.display().to_string())
        .collect();
    files.push(format!("{DEMO}/databricks/provider.yaml"));
    files.sort();
    files
}

#[test]
fn the_demo_configs_are_clean_against_their_pool() {
    let files = demo_files();
    let mut args = vec!["check", "--pool", "default/inference-all"];
    args.extend(files.iter().map(String::as_str));
    let out = kinfctl(&args);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    // Clean is quiet: an `ok` line per policy and nothing else.
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 5, "{stdout}");
    assert!(lines.iter().all(|l| l.ends_with(": ok")), "{stdout}");
}

/// What limited the checks is said once, before the results, not per policy.
#[test]
fn skipped_name_checks_are_noted_once_at_the_top() {
    let (one, two) = (
        format!("{DEMO}/policy-1.yaml"),
        format!("{DEMO}/policy-2.yaml"),
    );
    let out = kinfctl(&["check", one.as_str(), two.as_str()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "skipping is not a finding: {stdout}"
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "{stdout}");
    assert_eq!(
        lines[0],
        "Note: Skipping name checks. Add InferencePool manifests (and if necessary, --pool) to \
         enable.",
        "{stdout}"
    );
    assert!(lines[1..].iter().all(|l| l.ends_with(": ok")), "{stdout}");
}

/// Given only the pool, a correct policy is clean: the destinations the pool
/// lists count by name, and the checks that need their manifests say, once,
/// that they were skipped.
#[test]
fn a_pool_alone_checks_backend_names_and_says_what_it_skipped() {
    let (policy, pool) = (
        format!("{DEMO}/policy-4.yaml"),
        format!("{DEMO}/pool-all.yaml"),
    );
    let out = kinfctl(&[
        "check",
        "--pool",
        "default/inference-all",
        policy.as_str(),
        pool.as_str(),
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    assert_eq!(
        stdout,
        format!(
            "Note: Skipping model and label checks. Add InferenceWorkerSet and InferenceProvider \
             manifests to enable.\n{policy}: default/policy-4: ok\n"
        )
    );
}

/// Every mistake in the fixture is reported with its path and code: as `E`
/// when it is wrong whatever pool serves the policy, `W` when it is wrong for
/// this pool. In text, and in JSON an agent can act on, and the status says
/// there is something to fix.
#[test]
fn a_broken_draft_reports_each_mistake_and_exits_1() {
    let pool = format!("{DEMO}/pool-all.yaml");
    let provider = format!("{DEMO}/databricks/provider.yaml");
    let broken = format!("{FIXTURES}/broken-policy.yaml");
    let mut args = vec![
        "check",
        "--pool",
        "default/inference-all",
        &broken,
        &pool,
        &provider,
    ];
    let sets: Vec<String> = demo_files()
        .into_iter()
        .filter(|f| f.contains("workerset-"))
        .collect();
    args.extend(sets.iter().map(String::as_str));

    let out = kinfctl(&args);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{stdout}");
    let findings: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with("E   ") || l.starts_with("W   "))
        .collect();
    assert_eq!(findings.len(), 5, "{stdout}");
    assert!(
        findings.contains(
            &"W   spec.rules[0].route.select[1].order[0]: UnknownBackend: rule `burst-to-cloud`: \
              `databriks` is not a destination of pool default/inference-all, so nothing matches \
              it. Did you mean `databricks`? Known destinations: databricks, gemma-4-12b, \
              qwen-2-5-32b, qwen-3-6-35b-a3b"
        ),
        "{stdout}"
    );
    assert!(
        findings.iter().any(|l| l.starts_with(
            "E   spec.rules[0].matches[0].tennant: UnknownField: not part of the \
             InferencePolicy schema"
        )),
        "{stdout}"
    );

    args.extend(["-o", "json"]);
    let out = kinfctl(&args);
    assert_eq!(out.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).expect("JSON on stdout");
    assert_eq!(report["ok"], false);
    let p = &report["policies"][0];
    assert_eq!(p["policy"], "default/broken");
    let found = |kind: &str| -> Vec<(String, String)> {
        p[kind]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                (
                    f["code"].as_str().unwrap().to_string(),
                    f["path"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    let (errors, warnings) = (found("errors"), found("warnings"));
    for (code, path) in [
        ("UnknownField", "spec.rules[0].matches[0].tennant"),
        ("EmptyPredicate", "spec.rules[0].matches[0]"),
        (
            "ThresholdOutOfRange",
            "spec.rules[0].route.exclude[0].drop.kvCacheUsage",
        ),
        ("OrderWithoutRandom", "spec.rules[0].route.select[0].order"),
    ] {
        assert!(
            errors.contains(&(code.to_string(), path.to_string())),
            "missing error {code} at {path}: {errors:?}"
        );
    }
    assert_eq!(
        warnings,
        [(
            "UnknownBackend".to_string(),
            "spec.rules[0].route.select[1].order[0]".to_string()
        )]
    );
    assert_eq!(p["errors"][0]["suggestion"], "tenant");
}

#[test]
fn standard_input_is_read_with_a_dash() {
    let text = std::fs::read_to_string(format!("{FIXTURES}/bad-enum-policy.yaml")).unwrap();
    let out = kinfctl_with_stdin(&["check", "-"], &text);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains("<stdin>: default/bad-enum"), "{stdout}");
    assert!(
        stdout.contains(
            "E   spec.rules[0].route.consider[0].type: SchemaError: unknown variant `local`"
        ),
        "{stdout}"
    );
}

/// The documented way to check a draft against the cluster: the live objects
/// on stdin, the draft as its own argument. The tempting alternative,
/// `{ kubectl get … -o yaml; cat policy.yaml; } | kinfctl check -`, joins two
/// manifests with no `---` between them, and must say that is what happened.
#[test]
fn live_objects_on_stdin_are_checked_beside_a_draft() {
    let live = live_objects();
    let policy = format!("{DEMO}/policy-1.yaml");

    let out = kinfctl_with_stdin(
        &[
            "check",
            "--pool",
            "default/inference-all",
            "-",
            policy.as_str(),
        ],
        &live,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{stdout}{stderr}");
    assert_eq!(stdout, format!("{policy}: default/policy-1: ok\n"));

    let joined = live + &std::fs::read_to_string(&policy).unwrap();
    let out = kinfctl_with_stdin(&["check", "--pool", "default/inference-all", "-"], &joined);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.contains("starts another manifest with no `---` line before it"),
        "{stderr}"
    );
}

/// Input that cannot be checked is exit 2 with the reason on stderr — never
/// confusable with a clean check, and never mixed into the report on stdout.
#[test]
fn unusable_input_exits_2_with_the_reason_on_stderr() {
    for (args, says) in [
        (vec!["check", DEMO], "is a directory"),
        (
            vec!["check", "/nonexistent/policy.yaml"],
            "/nonexistent/policy.yaml",
        ),
        (
            vec![
                "check",
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/k8s/examples/demo-configs/pool-all.yaml"
                ),
            ],
            "no InferencePolicy",
        ),
        (
            vec![
                "check",
                "--pool",
                "default/missing",
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/k8s/examples/demo-configs/policy-1.yaml"
                ),
            ],
            "no InferencePool by that name",
        ),
    ] {
        let out = kinfctl(&args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(stderr.contains(says), "{args:?}: {stderr}");
        assert!(out.stdout.is_empty(), "{args:?}: nothing on stdout");
    }
}

/// `--help` is how an assistant learns a CLI, so the examples must be there.
#[test]
fn check_help_carries_examples_and_exit_statuses() {
    let out = kinfctl(&["check", "--help"]);
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(
        help.contains("kubectl get infpool,iws,infprov,refgrant -A -o yaml"),
        "{help}"
    );
    assert!(
        help.contains("Exit status: 0 nothing to fix, 1 findings, 2"),
        "{help}"
    );
}
