//! `Recorded`: the API server over objects the test records up front.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::{ApiServer, Object};

/// (group/Kind, namespace, name).
pub type Key = (String, String, String);

/// The key of `K`'s object `name` in `namespace`.
pub fn key<K: Object>(namespace: &str, name: &str) -> Key {
    (kind::<K>(), namespace.to_string(), name.to_string())
}

fn kind<K: Object>() -> String {
    format!("{}/{}", K::group(&()), K::kind(&()))
}

/// One write, in the order made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Write {
    Create(Key),
    Replace(Key),
}

/// Reads answer from the recording; writes change it and are kept in
/// order. A missing object is a 404; a `create` of an existing object or a
/// `replace` with a stale `resourceVersion` is a 409. Every write stamps a
/// fresh `resourceVersion`.
#[derive(Clone, Default)]
pub struct Recorded(Arc<Mutex<Inner>>);

#[derive(Default)]
struct Inner {
    objects: BTreeMap<Key, serde_json::Value>,
    writes: Vec<Write>,
    version: u64,
}

impl Recorded {
    /// Record `objects` as the API server's current state.
    pub fn with<K: Object>(self, objects: impl IntoIterator<Item = K>) -> Self {
        {
            let mut inner = self.lock();
            for object in objects {
                let namespace = object.meta().namespace.clone().unwrap_or_default();
                let name = object.meta().name.clone().unwrap_or_default();
                inner.store(key::<K>(&namespace, &name), encode(&object));
            }
        }
        self
    }

    pub fn writes(&self) -> Vec<Write> {
        self.lock().writes.clone()
    }

    /// The object as the recording now holds it.
    pub fn object<K: Object>(&self, namespace: &str, name: &str) -> Option<K> {
        self.lock()
            .objects
            .get(&key::<K>(namespace, name))
            .cloned()
            .map(decode)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Inner {
    /// Stamp a fresh `resourceVersion` on `value` and store it under `key`.
    fn store(&mut self, key: Key, mut value: serde_json::Value) -> serde_json::Value {
        self.version += 1;
        value["metadata"]["resourceVersion"] = serde_json::Value::String(self.version.to_string());
        self.objects.insert(key, value.clone());
        value
    }
}

fn encode<K: Object>(object: &K) -> serde_json::Value {
    serde_json::to_value(object).expect("a recorded object serializes")
}

fn decode<K: Object>(value: serde_json::Value) -> K {
    serde_json::from_value(value).expect("a recorded object deserializes")
}

fn resource_version(value: &serde_json::Value) -> Option<&str> {
    value
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
}

fn api_error(code: u16, message: String) -> kube::Error {
    kube::Error::Api(Box::new(kube::core::Status {
        code,
        message,
        ..Default::default()
    }))
}

fn not_found(key: &Key) -> kube::Error {
    api_error(404, format!("{} {}/{} not found", key.0, key.1, key.2))
}

impl ApiServer for Recorded {
    async fn get<K: Object>(&self, namespace: &str, name: &str) -> Result<K, kube::Error> {
        let key = key::<K>(namespace, name);
        self.lock()
            .objects
            .get(&key)
            .cloned()
            .map(decode)
            .ok_or_else(|| not_found(&key))
    }

    async fn create<K: Object>(&self, namespace: &str, object: &K) -> Result<K, kube::Error> {
        let name = object.meta().name.clone().unwrap_or_default();
        let key = key::<K>(namespace, &name);
        let mut inner = self.lock();
        if inner.objects.contains_key(&key) {
            return Err(api_error(409, format!("{} already exists", key.2)));
        }
        let mut value = encode(object);
        value["metadata"]["namespace"] = serde_json::Value::String(namespace.to_string());
        inner.writes.push(Write::Create(key.clone()));
        Ok(decode(inner.store(key, value)))
    }

    async fn replace<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        object: &K,
    ) -> Result<K, kube::Error> {
        let key = key::<K>(namespace, name);
        let mut inner = self.lock();
        let Some(current) = inner.objects.get(&key) else {
            return Err(not_found(&key));
        };
        let value = encode(object);
        if let Some(sent) = resource_version(&value)
            && Some(sent) != resource_version(current)
        {
            return Err(api_error(
                409,
                format!(
                    "{} has been modified; resourceVersion {sent} is stale",
                    key.2
                ),
            ));
        }
        inner.writes.push(Write::Replace(key.clone()));
        Ok(decode(inner.store(key, value)))
    }
}
