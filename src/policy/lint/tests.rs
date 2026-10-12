use super::*;
use crate::policy::PoolId;
use crate::policy::PoolKey;
use crate::policy::crd::{InferencePolicy, InferencePool, InferenceProvider, InferenceWorkerSet};
use crate::policy::resolve::{PoolIndex, resolve};
use serde_json::json;
use std::sync::Arc;

fn obj<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> Arc<T> {
    Arc::new(serde_json::from_value(v).expect("valid fixture object"))
}

fn iws(
    name: &str,
    models: serde_json::Value,
    labels: serde_json::Value,
) -> Arc<InferenceWorkerSet> {
    obj(json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceWorkerSet",
        "metadata": {"name": name, "namespace": "default", "labels": labels},
        "spec": {"serviceRef": {"name": name}, "models": models}
    }))
}

fn databricks() -> Arc<InferenceProvider> {
    obj(json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceProvider",
        "metadata": {"name": "databricks", "namespace": "default"},
        "spec": {
            "backendRef": {"name": "databricks"},
            "api": "OpenAI",
            "models": [{"name": "databricks-qwen", "aliases": ["qwen-2.5"]}]
        }
    }))
}

fn pool(name: &str, dests: serde_json::Value) -> Arc<InferencePool> {
    obj(json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePool",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"destinations": dests, "policyRef": {"name": "p"}}
    }))
}

fn policy(spec: serde_json::Value) -> Arc<InferencePolicy> {
    obj(json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePolicy",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": spec
    }))
}

/// Resolve through the real `resolve::resolve`, binding every worker set as if
/// discovery had found it — the same thing `kinfctl` does offline.
fn check(
    spec: serde_json::Value,
    sets: Vec<Arc<InferenceWorkerSet>>,
    providers: Vec<Arc<InferenceProvider>>,
    dests: serde_json::Value,
) -> (Vec<PolicyWarning>, Option<String>) {
    let bound: PoolIndex = sets
        .iter()
        .enumerate()
        .map(|(i, w)| {
            (
                PoolKey::namespaced("default", w.metadata.name.clone().unwrap()),
                PoolId::from_raw(i as u64 + 1),
            )
        })
        .collect();
    let index = resolve(
        &[pool("pool", dests)],
        &[policy(spec.clone())],
        &sets,
        &providers,
        &[],
        &[],
        &bound,
        &Default::default(),
    );
    let resolved = index.policies().next().expect("policy published");
    // The server path and the direct call must agree.
    let p = index.lookup("default", "pool").expect("pool resolved");
    let direct = reference_warnings(&serde_json::from_value(spec).unwrap(), &[p.as_ref()]);
    assert_eq!(resolved.reference_warnings, direct.warnings);
    (direct.warnings, direct.models_unchecked)
}

fn fleet() -> (
    Vec<Arc<InferenceWorkerSet>>,
    Vec<Arc<InferenceProvider>>,
    serde_json::Value,
) {
    (
        vec![
            iws(
                "qwen-2-5-32b",
                json!([{"name": "Qwen/Qwen2.5-32B-Instruct-AWQ", "aliases": ["qwen-2.5"]}]),
                json!({"accelerator": "l40s"}),
            ),
            iws(
                "gemma-4-12b",
                json!([{"name": "google/gemma-4-12B-it", "aliases": ["gemma-4"]}]),
                json!({"accelerator": "l40s"}),
            ),
        ],
        vec![databricks()],
        json!([
            {"kind": "InferenceWorkerSet", "name": "qwen-2-5-32b"},
            {"kind": "InferenceWorkerSet", "name": "gemma-4-12b"},
            {"kind": "InferenceProvider", "name": "databricks"}
        ]),
    )
}

#[test]
fn names_that_exist_raise_nothing() {
    let (sets, provs, dests) = fleet();
    let (w, unchecked) = check(
        json!({"rules": [{"name": "r",
            "matches": [{"model": "qwen-2.5"}],
            "route": {
                "consider": [{"models": {"names": ["gemma-4", "qwen-2.5"]},
                              "backends": {"selector": {"matchLabels": {"accelerator": "l40s"}}}}],
                "exclude": [{"name": "e", "drop": {"backend": {"name": "gemma-4-12b"}}}],
                "select": [{"type": "Local", "minimize": "PredictedTTFT"},
                           {"type": "Provider", "order": ["databricks"]}]}}]}),
        sets,
        provs,
        dests,
    );
    assert!(w.is_empty(), "{w:#?}");
    assert_eq!(unchecked, None, "every set declares its models");
}

#[test]
fn a_misspelled_backend_is_located_and_corrected() {
    let (sets, provs, dests) = fleet();
    let (w, _) = check(
        json!({"rules": [{"name": "burst", "route": {"select": [
            {"type": "Local", "minimize": "PredictedTTFT"},
            {"type": "Provider", "order": ["databriks"]}]}}]}),
        sets,
        provs,
        dests,
    );
    assert_eq!(w.len(), 1, "{w:#?}");
    assert_eq!(w[0].code, WarningCode::UnknownBackend);
    assert_eq!(w[0].path, "spec.rules[0].route.select[1].order[0]");
    assert!(
        w[0].message.contains("Did you mean `databricks`?"),
        "{}",
        w[0]
    );
    assert!(w[0].message.contains("pool default/pool"), "{}", w[0]);
}

#[test]
fn a_provider_tier_in_a_pool_without_providers_is_dead() {
    let (sets, _, _) = fleet();
    let (w, _) = check(
        json!({"rules": [{"name": "burst", "route": {"select": [
            {"type": "Local", "minimize": "PredictedTTFT"},
            {"type": "Provider", "order": ["databricks"]}]}}]}),
        sets,
        vec![],
        json!([{"kind": "InferenceWorkerSet", "name": "qwen-2-5-32b"},
               {"kind": "InferenceWorkerSet", "name": "gemma-4-12b"}]),
    );
    let codes: Vec<_> = w.iter().map(|w| (w.code, w.path.as_str())).collect();
    assert_eq!(
        codes,
        [
            (
                WarningCode::NoDestinationsOfType,
                "spec.rules[0].route.select[1].type"
            ),
            (
                WarningCode::UnknownBackend,
                "spec.rules[0].route.select[1].order[0]"
            ),
        ],
        "{w:#?}"
    );
}

#[test]
fn a_selector_matching_no_destination_is_flagged() {
    let (sets, provs, dests) = fleet();
    let (w, _) = check(
        json!({"rules": [{"name": "r", "route": {
            "consider": [{"backends": {"selector": {"matchLabels": {"accelerator": "h100"}}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
        sets,
        provs,
        dests,
    );
    assert_eq!(w.len(), 1, "{w:#?}");
    assert_eq!(w[0].code, WarningCode::SelectorMatchesNothing);
    assert!(w[0].message.contains("accelerator=h100"), "{}", w[0]);
}

#[test]
fn a_misspelled_model_is_caught_when_every_set_declares_its_models() {
    let (sets, provs, dests) = fleet();
    let (w, _) = check(
        json!({"rules": [{"name": "r",
            "matches": [{"model": "qwen2.5"}],
            "route": {"consider": [{"models": {"names": ["gemma4"]}}],
                      "select": [{"minimize": "PredictedTTFT"}]}}]}),
        sets,
        provs,
        dests,
    );
    let got: Vec<_> = w.iter().map(|w| (w.code, w.path.as_str())).collect();
    assert_eq!(
        got,
        [
            (WarningCode::UnknownModel, "spec.rules[0].matches[0].model"),
            (
                WarningCode::UnknownModel,
                "spec.rules[0].route.consider[0].models.names[0]"
            ),
        ],
        "{w:#?}"
    );
    assert!(
        w[0].message.contains("Did you mean `qwen-2.5`?"),
        "{}",
        w[0]
    );
    assert!(w[1].message.contains("Did you mean `gemma-4`?"), "{}", w[1]);
}

/// A set with no `spec.models` rows serves whatever its engine reports, so a
/// model name cannot be ruled out — and the check says it was skipped rather
/// than passing quietly.
#[test]
fn model_names_go_unchecked_when_a_set_declares_nothing() {
    let (mut sets, provs, dests) = fleet();
    sets[1] = iws("gemma-4-12b", json!([]), json!({}));
    let (w, unchecked) = check(
        json!({"rules": [{"name": "r", "route": {
            "consider": [{"models": {"names": ["whatever-model"]}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
        sets,
        provs,
        dests,
    );
    assert!(w.is_empty(), "{w:#?}");
    assert!(unchecked.unwrap().contains("gemma-4-12b"));
}

/// `auto` never reaches a rule — it is resolved first — so naming it is always
/// wrong, whatever the sets declare.
#[test]
fn matching_on_auto_is_always_flagged() {
    let (mut sets, provs, dests) = fleet();
    sets[1] = iws("gemma-4-12b", json!([]), json!({}));
    let (w, _) = check(
        json!({"rules": [{"name": "r", "matches": [{"model": "auto"}],
                          "route": {"select": [{"minimize": "PredictedTTFT"}]}}]}),
        sets,
        provs,
        dests,
    );
    assert_eq!(w.len(), 1, "{w:#?}");
    assert!(
        w[0].message.contains("resolved to a concrete model"),
        "{}",
        w[0]
    );
}

/// An unattached policy has nothing to be checked against, and must say
/// nothing rather than flag every name it uses.
#[test]
fn no_pools_means_no_name_findings() {
    let spec: InferencePolicySpec = serde_json::from_value(json!({"rules": [{"name": "r",
        "route": {"select": [{"order": ["anything"]}], "tieBreak": ["Random"]}}]}))
    .unwrap();
    let r = reference_warnings(&spec, &[]);
    assert!(r.warnings.is_empty());
    assert!(r.models_unchecked.is_none());
}

/// Label subsets work in `select` as well as `consider`, so a tier selector
/// that matches nothing is just as dead.
#[test]
fn a_select_tier_selector_matching_nothing_is_flagged() {
    let (sets, provs, dests) = fleet();
    let (w, _) = check(
        json!({"rules": [{"name": "r", "route": {"select": [
            {"backends": {"selector": {"matchLabels": {"accelerator": "h100"}}},
             "minimize": "PredictedTTFT"},
            {"backends": {"selector": {"matchLabels": {"accelerator": "l40s"}}},
             "minimize": "PredictedTTFT"}]}}]}),
        sets,
        provs,
        dests,
    );
    let got: Vec<_> = w.iter().map(|w| (w.code, w.path.as_str())).collect();
    assert_eq!(
        got,
        [(
            WarningCode::SelectorMatchesNothing,
            "spec.rules[0].route.select[0].backends.selector"
        )],
        "{w:#?}"
    );
    assert!(
        w[0].message.contains("select tier's label selector"),
        "{}",
        w[0]
    );
}
