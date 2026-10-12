use super::*;
use crate::policy::warning::WarningCode;

fn input(source: &str, text: &str) -> Input {
    Input {
        source: source.into(),
        text: text.into(),
    }
}

fn opts() -> Options {
    Options {
        namespace: "default".into(),
        pool: None,
    }
}

fn read(path: &str) -> Input {
    let full = format!("{}/{path}", env!("CARGO_MANIFEST_DIR"));
    input(
        path,
        &std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("{full}: {e}")),
    )
}

/// The known-keys table is what decides a field is "unknown", so it must be
/// exactly what the CRD schema declares — no more (an unknown field would pass
/// silently) and no less (a valid one would be flagged). Walked from the
/// schema, both directions: every table position must exist with the same
/// keys, and every structural object in the schema must have a table entry.
#[test]
fn known_keys_match_the_crd_schema() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/k8s/crds/kinference.buoyant.io_inferencepolicies.yaml"
    ))
    .expect("the CRD is the source of truth");
    let crd: Value = serde_saphyr::from_str(&text).expect("CRD parses");
    let spec = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    assert!(spec.is_object(), "schema moved?");

    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    fn walk(node: &Value, position: &str, found: &mut BTreeMap<String, BTreeSet<String>>) {
        let node = if node["type"] == "array" {
            &node["items"]
        } else {
            node
        };
        if node.get("x-kubernetes-preserve-unknown-fields") == Some(&Value::Bool(true)) {
            return;
        }
        let Some(props) = node.get("properties").and_then(Value::as_object) else {
            return;
        };
        found.insert(position.to_string(), props.keys().cloned().collect());
        for (k, child) in props {
            walk(child, &format!("{position}.{k}"), found);
        }
    }
    walk(spec, "spec", &mut found);

    let table: BTreeMap<String, BTreeSet<String>> = SPEC_KEYS
        .iter()
        .map(|(p, keys)| (p.to_string(), keys.iter().map(|k| k.to_string()).collect()))
        .collect();
    assert_eq!(
        table, found,
        "SPEC_KEYS has drifted from the CRD schema; update one to match the other"
    );
}

#[test]
fn unknown_fields_are_found_at_any_depth_with_a_suggestion() {
    let r = check(
        &[input(
            "p.yaml",
            r#"
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: p}
spec:
  unmatchd: Reject
  rules:
  - name: r
    matches: [{tennant: acme}]
    route:
      consider: [{models: {fromRequset: true}}]
      exclude:
      - name: e
        drop: {backend: {readyEndpoint: {lessThan: 2}}, kvCacheUsage: {greaterThan: 0.9}}
      select: [{minimize: PredictedTTFT, backends: {selector: {matchLabels: {any-key: ok}}}}]
"#,
        )],
        &opts(),
    )
    .unwrap();
    let got: Vec<(&str, Option<&str>)> = r.policies[0]
        .errors
        .iter()
        .filter(|f| f.code == UNKNOWN_FIELD)
        .map(|u| (u.path.as_str(), u.suggestion.as_deref()))
        .collect();
    assert_eq!(
        got,
        [
            ("spec.rules[0].matches[0].tennant", Some("tenant")),
            (
                "spec.rules[0].route.consider[0].models.fromRequset",
                Some("fromRequest")
            ),
            (
                "spec.rules[0].route.exclude[0].drop.backend.readyEndpoint",
                Some("readyEndpoints")
            ),
            ("spec.unmatchd", Some("unmatched")),
        ],
        "matchLabels keys and predicate values are data, never flagged"
    );
}

#[test]
fn a_deserialization_error_is_located_and_stops_further_checks() {
    let r = check(
        &[input(
            "p.yaml",
            "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
             metadata: {name: p}\nspec:\n  rules:\n  - name: r\n    route:\n      \
             consider: [{type: local}]\n      select: [{minimize: PredictedTTFT}]\n",
        )],
        &opts(),
    )
    .unwrap();
    let p = &r.policies[0];
    assert_eq!(
        p.errors.len(),
        1,
        "nothing compiles from a spec that does not parse: {p:#?}"
    );
    assert_eq!(p.errors[0].code, SCHEMA_ERROR);
    assert_eq!(p.errors[0].path, "spec.rules[0].route.consider[0].type");
    assert!(
        p.errors[0].message.contains("unknown variant `local`"),
        "{p:#?}"
    );
    assert!(p.warnings.is_empty());
    assert!(!r.ok);
}

/// Errors are what is wrong with the policy whatever pool serves it, and
/// warnings what is wrong for the pools it was checked against: a policy can
/// serve several, and a name one lacks may be right for another.
#[test]
fn spec_findings_are_errors_and_name_findings_warnings() {
    let r = check(
        &[
            read("tests/fixtures/kinfctl/broken-policy.yaml"),
            read("k8s/examples/demo-configs/pool-all.yaml"),
            read("k8s/examples/demo-configs/databricks/provider.yaml"),
            read("k8s/examples/demo-configs/workerset-gemma-4.yaml"),
            read("k8s/examples/demo-configs/workerset-qwen-2-5.yaml"),
            read("k8s/examples/demo-configs/workerset-qwen-3-6-35b-a3b.yaml"),
        ],
        &Options {
            namespace: "default".into(),
            pool: Some("default/inference-all".into()),
        },
    )
    .unwrap();
    let p = &r.policies[0];
    let codes = |fs: &[Finding]| fs.iter().map(|f| f.code).collect::<Vec<_>>();
    assert_eq!(
        codes(&p.errors),
        [
            UNKNOWN_FIELD,
            "EmptyPredicate",
            "ThresholdOutOfRange",
            "OrderWithoutRandom"
        ],
        "{p:#?}"
    );
    assert_eq!(codes(&p.warnings), ["UnknownBackend"], "{p:#?}");
}

/// `kubectl get -o yaml` output is one `kind: List`; its items must be read,
/// and an object without a namespace takes `-n`'s.
#[test]
fn a_kubectl_list_is_unwrapped_and_namespaces_default() {
    let r = check(
        &[input(
            "<stdin>",
            r#"
apiVersion: v1
kind: List
items:
- apiVersion: kinference.buoyant.io/v1alpha1
  kind: InferencePool
  metadata: {name: pool}
  spec:
    destinations: [{kind: InferenceWorkerSet, name: ws}]
    policyRef: {name: p}
- apiVersion: kinference.buoyant.io/v1alpha1
  kind: InferenceWorkerSet
  metadata: {name: ws}
  spec: {serviceRef: {name: ws}}
---
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: p}
spec:
  rules:
  - name: r
    route: {select: [{order: [ws, wz]}], tieBreak: [Random]}
"#,
        )],
        &Options {
            namespace: "team-a".into(),
            pool: None,
        },
    )
    .unwrap();
    let p = &r.policies[0];
    assert_eq!(p.policy, "team-a/p");
    assert_eq!(p.checked_against_pools, ["team-a/pool"]);
    assert!(p.name_checks.ran);
    let w: Vec<_> = p
        .warnings
        .iter()
        .map(|w| (w.code, w.path.as_str()))
        .collect();
    assert_eq!(
        w,
        [(
            WarningCode::UnknownBackend.as_str(),
            "spec.rules[0].route.select[0].order[1]"
        )],
        "{p:#?}"
    );
}

#[test]
fn without_a_pool_name_checks_are_skipped_and_say_how_to_run_them() {
    let r = check(&[read("k8s/examples/demo-configs/policy-3.yaml")], &opts()).unwrap();
    let p = &r.policies[0];
    assert!(!p.name_checks.ran);
    assert_eq!(p.name_checks.skipped, Some(NameCheckSkip::NoPool));
    assert!(r.ok, "skipping is not a finding");
}

/// `policy` checked against the demo pool `default/inference-all`, with
/// `files` from the demo configs beside it.
fn against_pool_all(policy: Input, files: &[&str]) -> Report {
    let mut inputs = vec![policy];
    inputs.extend(
        files
            .iter()
            .map(|f| read(&format!("k8s/examples/demo-configs/{f}"))),
    );
    let opts = Options {
        namespace: "default".into(),
        pool: Some("default/inference-all".into()),
    };
    check(&inputs, &opts).unwrap()
}

const ALL_DESTINATIONS: &[&str] = &[
    "pool-all.yaml",
    "workerset-gemma-4.yaml",
    "workerset-qwen-2-5.yaml",
    "workerset-qwen-3-6-35b-a3b.yaml",
    "databricks/provider.yaml",
];

/// A pool that names its destinations says what each is called and which
/// class it is, so backend names and classes can be checked from it alone. A
/// destination missing from the inputs is not missing from the pool.
#[test]
fn a_pool_alone_is_enough_for_backend_names_and_classes() {
    // policy-4 is right for its pool: a local tier, a provider tier, and
    // `databricks` by name. Given only the pool, none of that may look wrong.
    let r = against_pool_all(
        read("k8s/examples/demo-configs/policy-4.yaml"),
        &["pool-all.yaml"],
    );
    let p = &r.policies[0];
    assert!(p.is_clean(), "{p:#?}");
    assert!(p.name_checks.ran);
    assert_eq!(
        p.name_checks.skipped,
        Some(NameCheckSkip::NoDestinationManifests)
    );

    // And a misspelled name still does.
    let r = against_pool_all(
        read("tests/fixtures/kinfctl/broken-policy.yaml"),
        &["pool-all.yaml"],
    );
    let w: Vec<&str> = r.policies[0]
        .warnings
        .iter()
        .map(|w| w.message.as_str())
        .collect();
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(
        w[0].contains("`databriks`") && w[0].contains("Did you mean `databricks`?"),
        "{w:?}"
    );
}

/// Models and labels are declared by the worker sets and providers, not the
/// pool, so without their manifests they go unchecked rather than flagged.
#[test]
fn models_and_labels_need_the_destinations_manifests() {
    let draft = || {
        input(
            "draft.yaml",
            "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
             metadata: {name: draft}\nspec:\n  rules:\n  - name: r\n    \
             matches: [{model: gemma-4-12b}]\n    route:\n      \
             consider: [{backends: {selector: {matchLabels: {gpu: h100}}}}]\n      \
             select: [{minimize: PredictedTTFT}]\n",
        )
    };
    let codes = |r: &Report| -> Vec<&'static str> {
        r.policies[0].warnings.iter().map(|w| w.code).collect()
    };
    assert_eq!(
        codes(&against_pool_all(draft(), ALL_DESTINATIONS)),
        ["UnknownModel", "SelectorMatchesNothing"],
        "`gemma-4-12b` names the worker set, not its model, and no destination is labelled gpu"
    );
    assert!(
        codes(&against_pool_all(draft(), &["pool-all.yaml"])).is_empty(),
        "without the manifests there is nothing to check them against"
    );
}

/// A pool that chooses its destinations by label, or takes every worker set
/// in its namespace, says nothing about them until their manifests are passed.
#[test]
fn a_pool_that_does_not_name_its_destinations_needs_their_manifests() {
    let pool = || {
        input(
            "pool.yaml",
            "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePool\n\
             metadata: {name: everything}\nspec: {}\n",
        )
    };
    let o = Options {
        namespace: "default".into(),
        pool: Some("default/everything".into()),
    };
    let policy = || read("k8s/examples/demo-configs/policy-1.yaml");
    let r = check(&[policy(), pool()], &o).unwrap();
    assert!(!r.policies[0].name_checks.ran);
    assert_eq!(
        r.policies[0].name_checks.skipped,
        Some(NameCheckSkip::DestinationsNotListed)
    );
    assert!(r.ok, "skipping is not a finding");

    let gemma = read("k8s/examples/demo-configs/workerset-gemma-4.yaml");
    let r = check(&[policy(), pool(), gemma], &o).unwrap();
    assert!(r.policies[0].name_checks.ran);
    assert_eq!(r.policies[0].name_checks.skipped, None);
}

/// With worker sets and providers among the inputs, a destination the pool
/// lists but the inputs lack still counts by name: offline it has usually
/// just not been passed. Its models are unknown, so a model only it serves is
/// flagged, and the destination is recorded to say why. `kubectl get` of a
/// cluster whose pool lists a worker set that does not exist reads the same.
#[test]
fn a_listed_destination_without_its_manifest_counts_by_name_only() {
    let draft = input(
        "draft.yaml",
        "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
         metadata: {name: draft}\nspec:\n  rules:\n  - name: r\n    \
         matches: [{model: Qwen/Qwen3.6-35B-A3B}]\n    route:\n      \
         select: [{order: [qwen-3-6-35b-a3b]}]\n      tieBreak: [Random]\n",
    );
    let r = against_pool_all(
        draft,
        &[
            "pool-all.yaml",
            "workerset-gemma-4.yaml",
            "workerset-qwen-2-5.yaml",
            "databricks/provider.yaml",
        ],
    );
    let p = &r.policies[0];
    let codes: Vec<&str> = p.warnings.iter().map(|w| w.code).collect();
    assert_eq!(codes, ["UnknownModel"], "{p:#?}");
    assert!(
        p.name_checks
            .unresolved
            .iter()
            .any(|u| u.name == "qwen-3-6-35b-a3b" && u.reason == "NotFound"),
        "{p:#?}"
    );
}

#[test]
fn input_that_cannot_be_checked_is_an_error_not_a_finding() {
    assert_eq!(
        check(&[input("x", "a: [unclosed\n")], &opts()).unwrap_err(),
        InputError::Parse {
            source: "x".into(),
            message: check(&[input("x", "a: [unclosed\n")], &opts())
                .unwrap_err()
                .to_string()
                .trim_start_matches("x: not valid YAML: ")
                .to_string(),
        }
    );
    assert_eq!(
        check(&[read("k8s/examples/demo-configs/pool-all.yaml")], &opts()).unwrap_err(),
        InputError::NoPolicies
    );
    let o = Options {
        namespace: "default".into(),
        pool: Some("default/nope".into()),
    };
    assert_eq!(
        check(&[read("k8s/examples/demo-configs/policy-1.yaml")], &o).unwrap_err(),
        InputError::PoolNotFound("default/nope".into())
    );
}

/// Two manifests joined with no `---` between them, as `{ kubectl get -o yaml;
/// cat policy.yaml; }` joins them, are one document with every key twice. The
/// error must say so, not pass on serde-saphyr's advice to set one of its
/// options, which nobody running kinfctl can do.
#[test]
fn manifests_joined_without_a_separator_get_a_hint() {
    let joined = "apiVersion: v1\nkind: List\nitems: []\nmetadata:\n  resourceVersion: \"\"\n\
                  apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n";
    let err = check(&[input("x", joined)], &opts())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("hint: line 6 starts another manifest"),
        "{err}"
    );
    assert!(!err.contains("DuplicateKeyPolicy"), "{err}");

    // A key repeated inside one manifest is a plain mistake, not a join.
    let repeated = "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
                    metadata:\n  name: a\n  name: b\n";
    let err = check(&[input("x", repeated)], &opts())
        .unwrap_err()
        .to_string();
    assert!(err.contains("duplicate mapping key: name"), "{err}");
    assert!(!err.contains("hint:"), "{err}");
    assert!(!err.contains("DuplicateKeyPolicy"), "{err}");
}

#[test]
fn the_same_policy_twice_is_an_error_on_the_second() {
    let p = read("k8s/examples/demo-configs/policy-1.yaml");
    let again = input("copy.yaml", &p.text);
    let r = check(&[p, again], &opts()).unwrap();
    assert!(r.policies[0].errors.is_empty());
    assert_eq!(r.policies[1].errors[0].code, DUPLICATE_POLICY);
    assert!(
        r.policies[1].errors[0].message.contains("also defined in"),
        "{:#?}",
        r.policies[1]
    );
}

/// Every manifest that ships must check clean — spec, unknown fields AND names
/// — so a warning a user sees is about their change, never inherited from an
/// example. Lists the files itself: `kinfctl` reads no directories.
///
/// Supersedes the old line-scanning check for unemittable tags: a dead tag is
/// now an `UnemittableTag` warning, and this parses the YAML instead of
/// guessing at it.
#[test]
fn every_shipped_policy_checks_clean_against_its_demo_pool() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/k8s/examples/demo-configs");
    let mut files: Vec<String> = Vec::new();
    for sub in ["", "/databricks"] {
        for e in std::fs::read_dir(format!("{dir}{sub}")).unwrap() {
            let path = e.unwrap().path();
            if path.extension().is_some_and(|x| x == "yaml") {
                files.push(path.display().to_string());
            }
        }
    }
    files.sort();
    let inputs: Vec<Input> = files
        .iter()
        .map(|f| input(f, &std::fs::read_to_string(f).unwrap()))
        .collect();
    let r = check(
        &inputs,
        &Options {
            namespace: "default".into(),
            pool: Some("default/inference-all".into()),
        },
    )
    .unwrap();
    assert_eq!(r.policies.len(), 5, "the demo policies moved? {files:?}");
    for p in &r.policies {
        assert!(p.name_checks.ran, "{}: {:?}", p.policy, p.name_checks);
        assert!(p.is_clean(), "{p:#?}");
    }
    assert!(r.ok, "{:#?}", r.other_errors);
}

/// And every InferencePolicy anywhere under `k8s/`, on its own: the spec
/// checks need no pool, so nothing escapes them by living outside the demo.
#[test]
fn every_policy_under_k8s_passes_the_spec_checks() {
    let mut stack = vec![std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/k8s"
    ))];
    let mut checked = 0;
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|x| x != "yaml") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            // The parse decides what is a policy, not a text match: the CRD
            // itself mentions `kind: InferencePolicy` without being one.
            let r = match check(&[input(&path.display().to_string(), &text)], &opts()) {
                Err(InputError::NoPolicies) => continue,
                other => other.unwrap_or_else(|e| panic!("{}: {e}", path.display())),
            };
            for p in &r.policies {
                checked += 1;
                assert!(p.is_clean(), "{p:#?}");
            }
        }
    }
    assert!(
        checked >= 5,
        "found only {checked} policies under k8s/ — did they move?"
    );
}

/// A long comment block before a list entry is ordinary in a well-documented
/// manifest, and the YAML parser refuses more than 32 in a row. Checked here
/// with 40, above a policy rule — exactly where an explanation would go.
#[test]
fn a_long_comment_block_does_not_stop_the_parse() {
    let comments: String = (0..40)
        .map(|i| format!("    # line {i} of a long explanation\n"))
        .collect();
    let text = format!(
        "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
         metadata: {{name: p}}\nspec:\n  rules:\n{comments}  - name: r\n    route:\n      \
         select: [{{minimize: PredictedTTFT}}]\n"
    );
    let r = check(&[input("p.yaml", &text)], &opts()).expect("parses despite the comments");
    assert_eq!(r.policies.len(), 1);
    assert!(r.ok, "{r:#?}");
}

/// The codes only a manifest check has are documented with `kinfctl`, as the
/// policy codes are in the InferencePolicy reference: a code nobody explains
/// is a dead end for an agent that keys off it.
#[test]
fn kinfctl_codes_are_documented() {
    let doc = include_str!("../../../docs/content/reference/kinfctl.md");
    for code in [SCHEMA_ERROR, UNKNOWN_FIELD, DUPLICATE_POLICY] {
        assert!(
            doc.contains(&format!("| `{code}` |")),
            "kinfctl.md has no row for {code}"
        );
    }
}
