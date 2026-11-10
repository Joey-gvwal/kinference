use std::collections::BTreeSet;

use super::*;

#[test]
fn admission_cel_rules_match_the_covered_rules() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../charts/kinference-crds/templates/kinference.buoyant.io_inferencepolicies.yaml"
    ))
    .unwrap();
    let crd: Value = serde_saphyr::from_str(&text).unwrap();
    let mut found = BTreeSet::new();
    fn collect(value: &Value, found: &mut BTreeSet<String>) {
        match value {
            Value::Object(map) => {
                if let Some(rules) = map
                    .get("x-kubernetes-validations")
                    .and_then(Value::as_array)
                {
                    found.extend(
                        rules
                            .iter()
                            .map(|rule| rule["rule"].as_str().unwrap().to_string()),
                    );
                }
                for child in map.values() {
                    collect(child, found);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect(child, found);
                }
            }
            _ => {}
        }
    }
    collect(
        &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"],
        &mut found,
    );
    let expected = [
        "has(self.fromRequest) != has(self.names)",
        "!has(self.type) || !has(self.backends)",
        "has(self.minimize) != has(self.order)",
        "!has(self.route.select) || self.route.select.all(t, !has(t.minimize) || t.minimize != 'PredictedTTFT' || has(t.type) && t.type == 'Local' || has(t.backends) && has(t.backends.type) && t.backends.type == 'Local' || has(self.route.consider) && self.route.consider.all(c, has(c.type) && c.type == 'Local' || has(c.backends) && has(c.backends.type) && c.backends.type == 'Local'))",
    ].into_iter().map(str::to_string).collect();
    assert_eq!(
        found, expected,
        "update admission validation and shared fixtures when CEL rules change"
    );
}

#[test]
fn admission_limits_match_the_crd_schema() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../charts/kinference-crds/templates/kinference.buoyant.io_inferencepolicies.yaml"
    ))
    .unwrap();
    let crd: Value = serde_saphyr::from_str(&text).unwrap();
    let schema = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    let mut found = BTreeSet::new();
    fn collect(schema: &Value, position: &str, found: &mut BTreeSet<(String, String, u64)>) {
        for constraint in [
            "minItems",
            "maxItems",
            "minLength",
            "maxLength",
            "minimum",
            "maximum",
            "minProperties",
            "maxProperties",
        ] {
            if let Some(limit) = schema[constraint].as_u64() {
                found.insert((position.into(), constraint.into(), limit));
            }
        }
        if let Some(properties) = schema["properties"].as_object() {
            for (name, child) in properties {
                collect(child, &format!("{position}.{name}"), found);
            }
        }
        if let Some(items) = schema.get("items") {
            let item_position = if items["type"] == "object" {
                position.into()
            } else {
                format!("{position}[]")
            };
            collect(items, &item_position, found);
        }
    }
    collect(schema, "spec", &mut found);
    let expected = LIMITS
        .iter()
        .map(|(position, constraint, limit)| (position.to_string(), constraint.to_string(), *limit))
        .collect();
    assert_eq!(
        found, expected,
        "update offline admission limits when the CRD changes"
    );
}

#[test]
fn admission_bounds_accept_the_limit_and_reject_values_beyond_it() {
    for (position, constraint, limit) in LIMITS {
        let make = |size: u64| match *constraint {
            "minItems" | "maxItems" => Value::Array(vec![Value::Null; size as usize]),
            "maxLength" => Value::String("é".repeat(size as usize)),
            "maxProperties" => Value::Object(
                (0..size)
                    .map(|index| (index.to_string(), Value::String("v".into())))
                    .collect(),
            ),
            "minimum" | "maximum" => Value::from(size),
            _ => unreachable!(),
        };
        let mut errors = Vec::new();
        limits(&make(*limit), position, "spec.test", &mut errors);
        assert!(errors.is_empty(), "{position} {constraint}: {errors:?}");
        let outside = if constraint.starts_with("min") {
            limit - 1
        } else {
            limit + 1
        };
        limits(&make(outside), position, "spec.test", &mut errors);
        assert_eq!(errors.len(), 1, "{position} {constraint}");
        assert_eq!(errors[0].path, "spec.test");
        assert_eq!(errors[0].code, SCHEMA_ERROR);
    }
}

#[test]
fn null_normalization_preserves_predicate_data_and_unknown_fields() {
    let mut spec = serde_json::json!({"rules": [{"name": "r", "matches": [{
        "tenant": {"not": null}, "typo": null
    }], "route": {"onEmpty": null, "select": [{"type": "Local", "minimize": "PredictedTTFT"}]}}]});
    normalize(&mut spec, "spec");
    assert!(spec["rules"][0]["route"].get("onEmpty").is_none());
    assert!(
        spec["rules"][0]["matches"][0]["tenant"]
            .get("not")
            .unwrap()
            .is_null()
    );
    assert!(
        spec["rules"][0]["matches"][0]
            .get("typo")
            .unwrap()
            .is_null()
    );
}
