//! The `Service` watch: `(namespace, name)` → the live object, plus the one
//! question the rest of kinference asks of it — what NUMBER a port reference means.
//!
//! ## Deliberately NOT in the publish gate
//!
//! [`super::store::PolicyWatcher`] blocks its first publish until every CRD
//! kind has completed its initial list, because resolving mid-list would
//! publish a snapshot claiming every pool had vanished. Services are
//! deliberately outside that gate: no pool, policy, destination or status field
//! is derived from a Service, so letting one hold the gate would mean a cluster
//! that upgraded kinference WITHOUT adding the `services` grant lost policy routing
//! entirely — a 403 is retried forever, the flag would never flip, and the
//! symptom (no routing at all) would point nowhere near the cause.
//!
//! So the index carries its own [`ServiceIndex::listed`] instead, and a lookup
//! against an unlisted index answers [`UnresolvedPort::NotListed`] rather than
//! reporting a Service absent that has merely not been read yet. A consumer
//! that must not guess — a controller about to write a `BackendBinding`, say —
//! waits for that flag; everything else keeps working exactly as before.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{Service, ServicePort};
use kube::ResourceExt;

use super::crd::IntOrString;
use super::store::ObjKey;

/// Every `Service` in scope, keyed by identity.
///
/// Holds the whole object rather than a projection of its ports. The reflector
/// owns these allocations either way, so the index costs one map of pointers,
/// and a projection would have to be widened every time a consumer wants one
/// more field (`spec.type`, `appProtocol`, labels) — which is guesswork this
/// early.
#[derive(Debug, Default)]
pub struct ServiceIndex {
    by_key: HashMap<ObjKey, Arc<Service>>,
    /// False until the reflector's initial list completes. See the module
    /// header: this is per-index rather than folded into the policy publish
    /// gate on purpose.
    listed: bool,
}

/// Why a port reference did not resolve to a number.
///
/// Separate from [`super::store::UnresolvedReason`] because these are facts
/// about a Service, not about a destination, and every one of them is a
/// different fix: `NotListed` is wait, `ServiceNotFound` is a typo in
/// `serviceRef.name`, `UnknownPortName` is a typo in `serviceRef.port`, and
/// `AmbiguousPort` is an omission the operator has to resolve because kinference
/// picking one of several ports would be a guess that routes traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnresolvedPort {
    /// The index has not listed yet, so the answer is UNKNOWN — not negative.
    /// Reported rather than treated as absent so a caller cannot mistake
    /// startup for a missing Service and act on it.
    NotListed,
    /// No `Service` of that name in that namespace. Namespace is part of the
    /// key, so a same-named Service elsewhere is not a match.
    ServiceNotFound,
    /// The Service exposes no port kinference can dial: `spec.ports` is empty, or
    /// the only candidate carries a number outside `1..=65535` (which the
    /// apiserver rejects, but an object predating a validation change might
    /// still hold).
    NoUsablePort,
    /// `serviceRef.port` named a port the Service does not expose.
    UnknownPortName,
    /// `serviceRef.port` gave a number the Service does not expose.
    ///
    /// Refused rather than passed through, and that is the whole point of
    /// reading the Service: a `BackendBinding` naming a port with no listener
    /// resolves, programs a route, and then fails every request — whereas an
    /// unresolved ref can be REPORTED. Note this is the opposite of what
    /// discovery does with an unmatched number, correctly: there the number is
    /// used verbatim with a WARN, because the target port usually equals the
    /// service port and dialing a pod is recoverable.
    PortNotExposed,
    /// The reference was omitted and the Service exposes more than one port, so
    /// there is no unambiguous choice to make.
    AmbiguousPort,
}

impl UnresolvedPort {
    /// Stable string for status conditions and logs, in the same style as
    /// [`super::store::UnresolvedReason::as_str`].
    pub fn as_str(&self) -> &'static str {
        match self {
            UnresolvedPort::NotListed => "NotListed",
            UnresolvedPort::ServiceNotFound => "ServiceNotFound",
            UnresolvedPort::NoUsablePort => "NoUsablePort",
            UnresolvedPort::UnknownPortName => "UnknownPortName",
            UnresolvedPort::PortNotExposed => "PortNotExposed",
            UnresolvedPort::AmbiguousPort => "AmbiguousPort",
        }
    }
}

impl std::fmt::Display for UnresolvedPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            UnresolvedPort::NotListed => "the Service watch has not completed its initial list",
            UnresolvedPort::ServiceNotFound => "no such Service in that namespace",
            UnresolvedPort::NoUsablePort => "the Service exposes no dialable port",
            UnresolvedPort::UnknownPortName => "the Service exposes no port by that name",
            UnresolvedPort::PortNotExposed => "the Service does not expose that port number",
            UnresolvedPort::AmbiguousPort => {
                "no port was named and the Service exposes more than one"
            }
        };
        f.write_str(s)
    }
}

impl ServiceIndex {
    /// Build from a reflector store that has completed its initial list.
    ///
    /// A Service with no namespace is dropped rather than keyed under `""`:
    /// Service is namespace-scoped, so that cannot happen against a real
    /// apiserver, and inventing a key for it would make a bogus object
    /// shadow lookups.
    pub fn build(services: &[Arc<Service>]) -> Self {
        let by_key = services
            .iter()
            .filter_map(|s| Some(((s.namespace()?, s.name_any()), Arc::clone(s))))
            .collect();
        Self {
            by_key,
            listed: true,
        }
    }

    /// The state before the first list completes — empty, and saying so.
    /// [`Default`] is the same thing; this names it at the call site.
    pub fn unlisted() -> Self {
        Self::default()
    }

    /// Whether this index reflects a completed list. A [`Self::get`] miss is
    /// only meaningful when true.
    pub fn listed(&self) -> bool {
        self.listed
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<&Arc<Service>> {
        self.by_key.get(&(namespace.to_string(), name.to_string()))
    }

    pub fn contains(&self, namespace: &str, name: &str) -> bool {
        self.get(namespace, name).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ObjKey, &Arc<Service>)> {
        self.by_key.iter()
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// The Service's declared ports, or why there are none to read.
    pub fn ports(&self, namespace: &str, name: &str) -> Result<&[ServicePort], UnresolvedPort> {
        if !self.listed {
            return Err(UnresolvedPort::NotListed);
        }
        let svc = self
            .get(namespace, name)
            .ok_or(UnresolvedPort::ServiceNotFound)?;
        let ports = svc
            .spec
            .as_ref()
            .map(|s| s.ports.as_deref().unwrap_or(&[]))
            .unwrap_or(&[]);
        if ports.is_empty() {
            return Err(UnresolvedPort::NoUsablePort);
        }
        Ok(ports)
    }

    /// Resolve a CR's port reference to the SERVICE port number.
    ///
    /// `want` is `serviceRef.port` / `kvEvents.port` shaped: a port name, a
    /// number, or absent.
    ///
    /// Absent resolves only when the Service exposes exactly one port. That is
    /// the sole case where omission has one meaning; with two ports, choosing
    /// would be a guess, and a guess here silently sends traffic to the metrics
    /// endpoint.
    pub fn port_number(
        &self,
        namespace: &str,
        name: &str,
        want: Option<&IntOrString>,
    ) -> Result<u16, UnresolvedPort> {
        let ports = self.ports(namespace, name)?;
        let Some(want) = want else {
            return match ports {
                [only] => number(only).ok_or(UnresolvedPort::NoUsablePort),
                _ => Err(UnresolvedPort::AmbiguousPort),
            };
        };
        if let Some(want_name) = want.as_name()
            && let Some(p) = ports.iter().find(|p| p.name.as_deref() == Some(want_name))
        {
            return number(p).ok_or(UnresolvedPort::NoUsablePort);
        }
        // Falling through from a NAME that matched nothing is deliberate: no
        // legal Service port name is all digits (`IANA_SVC_NAME` requires a
        // letter), so a numeric string can only have been meant as the number
        // the apiserver would have accepted for `port: 8000`. `as_number`
        // already parses both spellings.
        match want.as_number() {
            Some(n) if ports.iter().any(|p| p.port == i32::from(n)) => Ok(n),
            Some(_) => Err(UnresolvedPort::PortNotExposed),
            None => Err(UnresolvedPort::UnknownPortName),
        }
    }
}

/// A `ServicePort`'s number as something dialable, or `None` for a value no
/// socket could carry. `port` is `i32` on the wire and `1..=65535` by
/// validation, so this narrows rather than converts.
fn number(p: &ServicePort) -> Option<u16> {
    u16::try_from(p.port).ok()
}

#[cfg(test)]
mod tests;
