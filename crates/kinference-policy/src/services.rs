//! The `Service` index: `(namespace, name)` to the live object, and what port
//! number a port reference means.
//!
//! Services are outside the router's policy publish gate, since nothing in
//! pool or policy resolution reads them; gating on them would let a missing
//! `services` RBAC grant disable policy routing entirely. Instead the index
//! carries its own [`ServiceIndex::listed`], and a lookup against an unlisted
//! index answers [`UnresolvedPort::NotListed`] rather than reporting the
//! Service absent.

use std::collections::HashMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{Service, ServicePort};
use kube::ResourceExt;

use super::crd::IntOrString;
use super::resolve::ObjKey;

/// Every `Service` in scope, keyed by identity.
///
/// Holds the whole object; the reflector owns the allocations anyway.
#[derive(Debug, Default)]
pub struct ServiceIndex {
    by_key: HashMap<ObjKey, Arc<Service>>,
    /// False until the reflector's initial list completes.
    listed: bool,
}

/// Why a port reference did not resolve to a number.
///
/// These are facts about a Service, not a destination, so they are separate
/// from [`super::resolve::UnresolvedReason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnresolvedPort {
    /// The index has not listed yet, so the answer is unknown rather than
    /// negative.
    NotListed,
    /// No `Service` of that name in that namespace.
    ServiceNotFound,
    /// The Service exposes no port kinference can dial: `spec.ports` is empty,
    /// or the matching port's number doesn't fit a `u16`.
    NoUsablePort,
    /// `serviceRef.port` named a port the Service does not expose.
    UnknownPortName,
    /// `serviceRef.port` gave a number the Service does not expose.
    ///
    /// Unlike discovery, which uses an unmatched number verbatim with a
    /// warning, this is refused: a `BackendBinding` naming a port with no
    /// listener would program a route that fails every request.
    PortNotExposed,
    /// The reference was omitted and the Service exposes more than one port.
    AmbiguousPort,
}

impl UnresolvedPort {
    /// Stable string for status conditions and logs.
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
    /// A Service with no namespace, which a real apiserver never returns, is
    /// dropped.
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

    /// The state before the first list completes. Same as [`Default`].
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
    /// Absent resolves only when the Service exposes exactly one port;
    /// otherwise choosing would be a guess, possibly the metrics port.
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
        // A name that matched nothing may still be a numeric string: no legal
        // port name is all digits (`IANA_SVC_NAME` requires a letter).
        match want.as_number() {
            Some(n) if ports.iter().any(|p| p.port == i32::from(n)) => Ok(n),
            Some(_) => Err(UnresolvedPort::PortNotExposed),
            None => Err(UnresolvedPort::UnknownPortName),
        }
    }
}

/// A `ServicePort`'s number as a `u16`, or `None` if it doesn't fit.
fn number(p: &ServicePort) -> Option<u16> {
    u16::try_from(p.port).ok()
}

#[cfg(test)]
mod tests;
