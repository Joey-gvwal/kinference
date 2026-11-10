use serde_json::Value;

use super::{Finding, SCHEMA_ERROR, known_keys};

const LIMITS: &[(&str, &str, u64)] = &[
    ("spec.rules", "maxItems", 32),
    ("spec.rules.name", "maxLength", 253),
    ("spec.rules.matches", "maxItems", 16),
    ("spec.rules.matches.qos", "minItems", 1),
    ("spec.rules.matches.qos", "maxItems", 3),
    ("spec.rules.route.consider", "minItems", 1),
    ("spec.rules.route.consider", "maxItems", 16),
    ("spec.rules.route.consider.name", "maxLength", 253),
    ("spec.rules.route.consider.weight", "minimum", 1),
    ("spec.rules.route.consider.weight", "maximum", 1_000_000),
    ("spec.rules.route.consider.models.names", "maxItems", 32),
    ("spec.rules.route.consider.models.names[]", "maxLength", 512),
    ("spec.rules.route.consider.backends.names", "maxItems", 32),
    (
        "spec.rules.route.consider.backends.names[]",
        "maxLength",
        253,
    ),
    (
        "spec.rules.route.consider.backends.selector.matchLabels",
        "maxProperties",
        64,
    ),
    ("spec.rules.route.exclude", "maxItems", 16),
    ("spec.rules.route.exclude.name", "maxLength", 253),
    ("spec.rules.route.exclude.if.qos", "minItems", 1),
    ("spec.rules.route.exclude.if.qos", "maxItems", 3),
    ("spec.rules.route.select", "minItems", 1),
    ("spec.rules.route.select", "maxItems", 8),
    ("spec.rules.route.select.backends.names", "maxItems", 32),
    ("spec.rules.route.select.backends.names[]", "maxLength", 253),
    (
        "spec.rules.route.select.backends.selector.matchLabels",
        "maxProperties",
        64,
    ),
    ("spec.rules.route.select.order", "maxItems", 16),
    ("spec.rules.route.select.order[]", "maxLength", 253),
    ("spec.rules.route.tieBreak", "maxItems", 4),
];

pub(super) fn normalize(value: &mut Value, position: &str) {
    let Some(keys) = known_keys(position) else {
        if position.ends_with(".matchLabels")
            && let Some(map) = value.as_object_mut()
        {
            map.retain(|_, value| !value.is_null());
        }
        return;
    };
    if let Some(items) = value.as_array_mut() {
        for item in items {
            normalize(item, position);
        }
    } else if let Some(map) = value.as_object_mut() {
        map.retain(|key, value| !keys.contains(&key.as_str()) || !value.is_null());
        for key in keys {
            if let Some(child) = map.get_mut(*key) {
                normalize(child, &format!("{position}.{key}"));
            }
        }
    }
}

pub(super) fn validate(value: &Value) -> Vec<Finding> {
    let mut errors = Vec::new();
    visit(value, "spec", "spec", &mut errors);
    errors
}

fn error(errors: &mut Vec<Finding>, path: &str, message: impl Into<String>) {
    errors.push(Finding {
        path: path.into(),
        code: SCHEMA_ERROR,
        message: message.into(),
        suggestion: None,
    });
}

fn limits(value: &Value, position: &str, path: &str, errors: &mut Vec<Finding>) {
    for (_, constraint, limit) in LIMITS.iter().filter(|(field, _, _)| *field == position) {
        let observed = match *constraint {
            "minItems" | "maxItems" => value.as_array().map(|items| items.len() as u64),
            "maxLength" => value.as_str().map(|text| text.chars().count() as u64),
            "maxProperties" => value.as_object().map(|map| map.len() as u64),
            "minimum" | "maximum" => value.as_u64(),
            _ => unreachable!(),
        };
        if let Some(observed) = observed
            && if constraint.starts_with("min") {
                observed < *limit
            } else {
                observed > *limit
            }
        {
            error(
                errors,
                path,
                format!("{constraint} is {limit}, got {observed}"),
            );
        }
    }
}

fn local(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("Local")
        || value
            .get("backends")
            .and_then(|backends| backends.get("type"))
            .and_then(Value::as_str)
            == Some("Local")
}

fn visit(value: &Value, position: &str, path: &str, errors: &mut Vec<Finding>) {
    limits(value, position, path, errors);
    if let Some(items) = value.as_array() {
        if position.ends_with(".qos") {
            for (index, item) in items.iter().enumerate() {
                if items[..index].contains(item) {
                    error(
                        errors,
                        &format!("{path}[{index}]"),
                        "set entries must be unique",
                    );
                }
            }
        }
        for (index, item) in items.iter().enumerate() {
            let item_path = format!("{path}[{index}]");
            if known_keys(position).is_some() {
                visit(item, position, &item_path, errors);
            } else {
                limits(item, &format!("{position}[]"), &item_path, errors);
            }
        }
        return;
    }
    let Some(map) = value.as_object() else {
        return;
    };
    if let Some(keys) = known_keys(position) {
        for key in keys {
            if let Some(child) = map.get(*key) {
                visit(
                    child,
                    &format!("{position}.{key}"),
                    &format!("{path}.{key}"),
                    errors,
                );
            }
        }
    }
    match position {
        "spec.rules.route.consider.models"
            if map.contains_key("fromRequest") == map.contains_key("names") =>
        {
            error(errors, path, "exactly one of fromRequest or names");
        }
        "spec.rules.route.consider" | "spec.rules.route.select" => {
            if map.contains_key("type") && map.contains_key("backends") {
                error(
                    errors,
                    path,
                    "type is shorthand for backends.type; do not set both",
                );
            }
            if position.ends_with(".select")
                && map.contains_key("minimize") == map.contains_key("order")
            {
                error(
                    errors,
                    path,
                    "a tier ranks by exactly one of minimize or order",
                );
            }
        }
        "spec.rules" => {
            let route = &value["route"];
            let inherited_local = route
                .get("consider")
                .and_then(Value::as_array)
                .is_some_and(|groups| groups.iter().all(local));
            if !inherited_local
                && route
                    .get("select")
                    .and_then(Value::as_array)
                    .is_some_and(|tiers| {
                        tiers.iter().any(|tier| {
                            tier["minimize"].as_str() == Some("PredictedTTFT") && !local(tier)
                        })
                    })
            {
                error(
                    errors,
                    path,
                    "PredictedTTFT is Local-only: constrain the tier or the candidate set to Local, or use EstimatedTTFT",
                );
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
