use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use dashmap::DashMap;

/// Verdict returned by [`WorkerRegistry::set_served_models`].
///
/// The canonical model set exists to catch genuine HETEROGENEITY — two
/// different fleets accidentally merged into one pool, which silently corrupts
/// the prefix model because block hashes are pool-scoped. It is not there to
/// freeze the model list.
///
/// So the verdict distinguishes the SET RELATION rather than just equality. A
/// worker that gained a model is not the same fault as one serving something
/// else entirely: vLLM supports loading LoRA adapters at runtime, and treating
/// that as a violation evicted the worker — so rolling an adapter across a fleet
/// walked it pod by pod and emptied the pool, while the set still reported
/// `Accepted=True`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServedModelsVerdict {
    /// Models matched canonical exactly (or canonical was empty and this report
    /// set it). `by_model` was updated for this worker.
    Accepted,
    /// This worker reports a SUPERSET of canonical — it gained models. Canonical
    /// has been widened to match and `by_model` updated. A LoRA adapter being
    /// loaded, or an engine finishing a lazy load.
    Widened { added: Vec<String> },
    /// This worker reports a SUBSET of canonical — it is missing some. Canonical
    /// is left alone and `by_model` reflects only what this worker actually
    /// serves, so nothing routes to it for the missing models. An adapter not
    /// loaded here yet, mid-rollout, or unloaded.
    Narrowed { missing: Vec<String> },
    /// `new_models` was empty — treat as "engine not yet ready", don't pin
    /// canonical, don't update by_model. Caller should skip without complaining.
    Empty,
    /// `new_models` CONFLICTS with canonical: each has something the other
    /// lacks, so this is not an adapter rollout but a different fleet. `by_model`
    /// was NOT updated; the caller should evict.
    Mismatch {
        expected: Vec<String>,
        observed: Vec<String>,
    },
}

use crate::config::{Engine, WorkerConfig};

/// Extract `host:port` from an engine HTTP base URL.
/// `http://127.0.0.1:8000/` → `127.0.0.1:8000`. Returns `None` if the
/// authority can't be isolated.
fn host_port_from_http_url(url: &str) -> Option<String> {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    let authority = after_scheme.split('/').next().unwrap_or("");
    (!authority.is_empty()).then(|| authority.to_string())
}

/// Mutable, runtime-add/remove registry of workers. Maps the numeric
/// `WorkerId` used internally by `dynamo-kv-router` to per-worker fields
/// needed by the gRPC `Score` handler (HTTP base URL, resolved block_size,
/// score address).
///
/// `by_model` is **reactive**: it's not populated from a config-time
/// `model_name` field but rather from each worker's `/v1/models` scrape
/// (via [`Self::set_served_models`]). A worker may serve multiple models
/// (LoRA adapters); each appears as its own entry in this index.
///
/// For static-mode bootstrapping the per-worker `model_name` config field
/// is honored as an _initial_ hint: workers are routable on this hint
/// from the moment they're registered, before their first `/v1/models`
/// scrape lands. Once a real `/v1/models` response arrives, the hint is
/// superseded by the live data (and a WARN logs if they disagree).
#[derive(Debug, Default)]
pub struct WorkerRegistry {
    by_num: DashMap<u64, WorkerEntry>,
    /// `model_name -> {worker_id_num}` reactive index. Guarded by a single
    /// Mutex; updates are rare (register/unregister/set_served_models) and
    /// reads are O(1) lookup followed by a small set scan, so contention
    /// is negligible.
    by_model: Mutex<HashMap<String, HashSet<u64>>>,
    /// Canonical set of model names this fleet serves. Pinned by the
    /// first non-empty `set_served_models` call to enforce the
    /// homogeneous-fleet contract: every subsequent worker reporting
    /// a different set is rejected (and evicted upstream). `None`
    /// before any worker has reported a non-empty list; cleared back
    /// to `None` when the registry becomes empty (which makes the
    /// next worker's first non-empty report re-pin canonical).
    canonical_models: Mutex<Option<BTreeSet<String>>>,
}

#[allow(dead_code)]
// some placement metadata is loaded for K8s/dashboard
// surfacing but isn't read by the score path yet.
#[derive(Debug, Clone)]
pub struct WorkerEntry {
    pub worker_id_str: String,
    /// Optional bootstrap hint from `WorkerConfig.model_name`. Static-mode
    /// workers are routable on this hint immediately on register; K8s-mode
    /// workers leave this `None` (no config-supplied model). Superseded by
    /// `current_models` once `/v1/models` lands.
    pub model_name_hint: Option<String>,
    /// Models this worker is currently serving, as reported by the latest
    /// `/v1/models` scrape. Empty until the first successful scrape.
    pub current_models: Vec<String>,
    /// HTTP base URL for the engine's OpenAI-compatible `/tokenize`. `None`
    /// if not configured.
    pub http_url: Option<String>,
    /// Effective KV-cache hash block_size for this worker. Seeded
    /// from the fleet-wide `Config.kv_block_size` (mandatory; kinference
    /// requires a homogeneous fleet). When the ingest path observes
    /// the engine reporting a different value, the worker is evicted
    /// from the pool — see `ingest::check_block_size_mismatch`. The
    /// picker reads this
    /// at hash-computation time. If the seed value is wrong, the
    /// very first request to this worker mis-routes (we hash at
    /// the seed); subsequent requests use the corrected value.
    pub block_size: u32,
    /// True once the ZMQ listener has finished its initial
    /// replay-drain cycle for this worker and is in steady-state on
    /// the live SUB stream. Combined with [`Self::current_models`]
    /// non-empty, makes this worker a pick CANDIDATE — see
    /// [`Self::is_ready`]. Sticky across libzmq reconnects (degraded
    /// state during a connection blip isn't a correctness issue).
    pub initial_sync_complete: bool,
    /// True once kinference has decoded at least one KV cache event from this
    /// worker — proof the event stream is real, which
    /// [`Self::initial_sync_complete`] cannot give (see
    /// [`WorkerRegistry::note_kv_events_observed`]). Reported as
    /// `InferenceWorkerSet.status.kvEvents.subscribedEndpoints`; NOT part of
    /// readiness, since a worker with no prefix model still serves requests,
    /// just on a degraded `predictedTTFT`.
    pub kv_events_observed: bool,
    /// Identifier surfaced in Score responses and used as the
    /// destination value the pick path sets on
    /// `x-gateway-destination-endpoint`. Must be an `ip:port` pair
    /// because Envoy / agentgateway route to this verbatim — they
    /// don't resolve hostnames at this layer. Resolution policy:
    ///
    /// 1. If the static-config `endpoint` field is set on
    ///    `WorkerConfig`, use it verbatim. This is the operator
    ///    override for cases where `http_url` is a DNS name (e.g.
    ///    an NLB hostname) but the routing target needs to be an
    ///    IP literal.
    /// 2. Else extract `host:port` from `http_url`. K8s discovery
    ///    synthesizes `http_url` as `http://<pod-ip>:<port>/` so
    ///    this already returns an IP-based address.
    /// 3. Else fall back to `worker_id_str` — degenerate, but
    ///    keeps the field non-empty.
    pub score_addr: String,
    /// Engine identity. `None` at registration time; populated once
    /// `MetricsRegistry` has a `/v1/models` response with `owned_by`. Not
    /// used for routing — display + diagnostics only.
    pub engine: Option<Engine>,
    // --- Placement metadata, loaded but not yet consumed by routing. ---
    pub pod_name: Option<String>,
    pub namespace: Option<String>,
    pub node_name: Option<String>,
    pub node_ip: Option<String>,
    pub pod_ip: Option<String>,
    pub availability_zone: Option<String>,
    pub instance_type: Option<String>,
}

impl WorkerEntry {
    /// True when this worker has resolved enough state to be picked
    /// against:
    ///
    /// 1. `current_models` populated — at least one successful
    ///    `/v1/models` scrape so the model→worker index is reactive
    ///    to live data.
    /// 2. `initial_sync_complete` — the ZMQ listener has finished its
    ///    initial replay-drain cycle and is in steady-state live
    ///    receive. Prefix-cache routing depends on the event stream.
    ///
    /// The `/readyz` probe answers 200 when ANY worker passes this
    /// check. The picker (`score_workers`) further filters its
    /// candidate pool to ready workers only.
    ///
    /// `block_size` itself is **not** part of readiness — it's
    /// seeded from the fleet-wide `Config.kv_block_size`. If the
    /// engine reports a different value, the worker is evicted from
    /// the pool rather than the readiness check failing — that keeps
    /// the rest of the fleet serving while the operator fixes the
    /// outlier.
    pub fn is_ready(&self) -> bool {
        self.initial_sync_complete && !self.current_models.is_empty()
    }

    /// Build a `WorkerEntry` from a static-config `WorkerConfig`. Sets the
    /// model hint from `model_name`; leaves `current_models` empty (the
    /// metrics poller fills it on first `/v1/models` success). The
    /// `block_size` argument is the fleet-wide value from
    /// `Config.kv_block_size`.
    pub fn from_config(w: &WorkerConfig, block_size: u32) -> Self {
        // Resolve `score_addr` per the policy on the field
        // doc-comment: explicit `endpoint` wins; else derive from
        // `http_url`; else fall back to the worker id string.
        let score_addr = w
            .endpoint
            .clone()
            .or_else(|| w.http_url.as_deref().and_then(host_port_from_http_url))
            .unwrap_or_else(|| w.worker_id_str.clone());
        let hint = (!w.model_name.is_empty()).then(|| w.model_name.clone());
        Self {
            worker_id_str: w.worker_id_str.clone(),
            model_name_hint: hint,
            current_models: Vec::new(),
            http_url: w.http_url.clone(),
            block_size,
            initial_sync_complete: false,
            kv_events_observed: false,
            score_addr,
            engine: None,
            pod_name: w.pod_name.clone(),
            namespace: w.namespace.clone(),
            node_name: w.node_name.clone(),
            node_ip: w.node_ip.clone(),
            pod_ip: w.pod_ip.clone(),
            availability_zone: w.availability_zone.clone(),
            instance_type: w.instance_type.clone(),
        }
    }
}

#[allow(dead_code)]
impl WorkerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a worker. Returns `false` if a worker with the same
    /// `worker_id_num` is already registered (caller decides how to handle
    /// the dup — typically a WARN + skip).
    pub fn register(&self, worker_id_num: u64, entry: WorkerEntry) -> bool {
        if self.by_num.contains_key(&worker_id_num) {
            return false;
        }
        // If we have a bootstrap hint and no live models yet, seed by_model
        // with the hint so requests are servable before the first /v1/models
        // scrape lands.
        if entry.current_models.is_empty() {
            if let Some(hint) = entry.model_name_hint.as_deref() {
                let mut by_model = self.by_model.lock().unwrap();
                by_model
                    .entry(hint.to_string())
                    .or_default()
                    .insert(worker_id_num);
            }
        } else {
            let mut by_model = self.by_model.lock().unwrap();
            for m in &entry.current_models {
                by_model.entry(m.clone()).or_default().insert(worker_id_num);
            }
        }
        self.by_num.insert(worker_id_num, entry);
        true
    }

    /// Remove a worker from the registry and drop its membership from every
    /// model bucket. Safe to call on a worker that was never registered
    /// (returns `false`).
    ///
    /// If the registry becomes empty as a result, also clears the
    /// canonical models set so the next worker's first non-empty
    /// `/v1/models` report can re-pin it. This is what lets an
    /// operator-driven rolling model swap eventually converge: as
    /// the last old-model worker leaves, canonical resets; the next
    /// new-model worker's report pins the new set.
    pub fn unregister(&self, worker_id_num: u64) -> bool {
        let removed = self.by_num.remove(&worker_id_num);
        if removed.is_none() {
            return false;
        }
        {
            let mut by_model = self.by_model.lock().unwrap();
            // Walk every bucket and evict the worker; cheap because the
            // number of distinct models served by a fleet is tiny
            // (usually 1-3).
            by_model.retain(|_model, set| {
                set.remove(&worker_id_num);
                !set.is_empty()
            });
        }
        if self.by_num.is_empty() {
            let mut canonical = self.canonical_models.lock().unwrap();
            *canonical = None;
        }
        true
    }

    /// Update the worker's set of served models. Called by the metrics
    /// poller after a successful `/v1/models` fetch. Diffs against the
    /// previously-recorded `current_models`; removes from departed
    /// buckets, adds to new buckets, and overwrites the entry's
    /// `current_models`. Idempotent — calling with the same list is a
    /// no-op.
    ///
    /// Enforces the homogeneous-fleet contract: the first non-empty
    /// model list pins the canonical set; every subsequent report is
    /// checked against it. The verdict tells the caller (metrics
    /// poller) what to do — accept and update by_model, ignore an
    /// empty report, or signal eviction for a heterogeneous worker.
    ///
    /// If a `model_name_hint` was used to bootstrap by_model membership
    /// and the new live models DON'T include it, the hint bucket is
    /// dropped silently — the live data wins, with no WARN (a mismatch
    /// here is the operator's "I changed the model" intent, not a bug).
    pub fn set_served_models(
        &self,
        worker_id_num: u64,
        new_models: &[String],
    ) -> ServedModelsVerdict {
        let Some(mut entry_ref) = self.by_num.get_mut(&worker_id_num) else {
            // Worker was unregistered between scrape and update.
            // Treat as not-applicable — neither accept nor mismatch.
            return ServedModelsVerdict::Empty;
        };

        // Empty report means the engine hasn't loaded its models yet
        // (typical right after pod start). Don't pin canonical or
        // mutate by_model.
        if new_models.is_empty() {
            return ServedModelsVerdict::Empty;
        }

        // Canonical check: pin on first non-empty report, compare
        // against pinned set on every subsequent one.
        let new_set: BTreeSet<String> = new_models.iter().cloned().collect();
        // Decided before the by_model update below, and reported back so the
        // caller can log the three non-fatal relations differently. Only a
        // genuine conflict returns early.
        let relation = {
            let mut canonical = self.canonical_models.lock().unwrap();
            match canonical.as_ref() {
                None => {
                    *canonical = Some(new_set.clone());
                    ServedModelsVerdict::Accepted
                }
                Some(pinned) if pinned == &new_set => ServedModelsVerdict::Accepted,
                Some(pinned) if new_set.is_superset(pinned) => {
                    // Widen. The fleet gained a model; canonical follows so the
                    // next worker to report the same set is not treated as a
                    // conflict, which is what turned an adapter rollout into a
                    // pod-by-pod eviction.
                    let added: Vec<String> = new_set.difference(pinned).cloned().collect();
                    *canonical = Some(new_set.clone());
                    ServedModelsVerdict::Widened { added }
                }
                Some(pinned) if new_set.is_subset(pinned) => {
                    // Do NOT narrow canonical: the other workers still serve the
                    // rest, and narrowing would make each of them look like a
                    // superset in turn and re-widen it on the next scrape.
                    let missing: Vec<String> = pinned.difference(&new_set).cloned().collect();
                    ServedModelsVerdict::Narrowed { missing }
                }
                Some(pinned) => {
                    let expected: Vec<String> = pinned.iter().cloned().collect();
                    let observed: Vec<String> = new_set.iter().cloned().collect();
                    return ServedModelsVerdict::Mismatch { expected, observed };
                }
            }
        };

        let entry = entry_ref.value_mut();

        // First-scrape special case: if we previously had only a hint in
        // by_model (current_models was empty), evict the hint bucket
        // before installing live data.
        if entry.current_models.is_empty()
            && let Some(hint) = entry.model_name_hint.as_deref()
        {
            let mut by_model = self.by_model.lock().unwrap();
            if let Some(set) = by_model.get_mut(hint) {
                set.remove(&worker_id_num);
                if set.is_empty() {
                    by_model.remove(hint);
                }
            }
        }

        // Diff current vs new.
        let old: HashSet<&String> = entry.current_models.iter().collect();
        let new: HashSet<&String> = new_models.iter().collect();

        let to_remove: Vec<String> = old.difference(&new).map(|s| (*s).clone()).collect();
        let to_add: Vec<String> = new.difference(&old).map(|s| (*s).clone()).collect();

        if !to_remove.is_empty() || !to_add.is_empty() {
            let mut by_model = self.by_model.lock().unwrap();
            for m in &to_remove {
                if let Some(set) = by_model.get_mut(m) {
                    set.remove(&worker_id_num);
                    if set.is_empty() {
                        by_model.remove(m);
                    }
                }
            }
            for m in &to_add {
                by_model.entry(m.clone()).or_default().insert(worker_id_num);
            }
        }

        entry.current_models = new_models.to_vec();
        // `by_model` reflects what THIS worker serves, whatever the relation —
        // that is what keeps a mid-rollout worker routable for the models it does
        // have while not receiving traffic for the ones it does not.
        relation
    }

    /// How many workers currently serve `model`. Test-facing: routing itself
    /// goes through `workers_for`, but a test asserting a re-seed took effect
    /// wants the count without caring which ids.
    #[cfg(test)]
    pub fn by_model_len(&self, model: &str) -> usize {
        self.by_model
            .lock()
            .unwrap()
            .get(model)
            .map_or(0, |s| s.len())
    }

    /// Re-apply a DECLARED model list to every worker in this set, replacing
    /// the canonical pin.
    ///
    /// Only for `modelDiscovery.mode: Static`, where the CR is authoritative
    /// about what these engines serve. Editing `spec.models` there is a
    /// copy-on-write update, which deliberately SHARES this registry so the
    /// prefix cache survives — but that means the pin and every worker's
    /// `current_models` would otherwise still hold the old list. The change
    /// would sit inert until a worker churned, and that worker would then
    /// mismatch the stale pin and be refused.
    ///
    /// Replacing the pin rather than clearing it is deliberate: clearing would
    /// let whichever worker reported next decide the set, which is the very
    /// race the pin exists to prevent.
    ///
    /// `None` means the set went back to endpoint discovery: drop the pin and
    /// let the next `/v1/models` scrape re-establish it.
    pub fn reset_declared_models(&self, models: Option<&[String]>) -> usize {
        let Some(models) = models else {
            *self.canonical_models.lock().unwrap() = None;
            return 0;
        };
        let pinned: BTreeSet<String> = models.iter().cloned().collect();
        // Pin FIRST. A concurrent scrape landing mid-rebuild then measures
        // against the new list rather than being accepted under the old one.
        *self.canonical_models.lock().unwrap() = Some(pinned);

        let mut by_model = self.by_model.lock().unwrap();
        let mut touched = 0;
        for mut entry in self.by_num.iter_mut() {
            let num = *entry.key();
            for m in entry.value().current_models.iter() {
                if let Some(set) = by_model.get_mut(m) {
                    set.remove(&num);
                }
            }
            entry.value_mut().current_models = models.to_vec();
            for m in models {
                by_model.entry(m.clone()).or_default().insert(num);
            }
            touched += 1;
        }
        by_model.retain(|_, set| !set.is_empty());
        touched
    }

    /// Update the worker's detected engine. Called once per worker after
    /// `/v1/models` returns a recognized `owned_by`. No-op if the worker
    /// has been unregistered.
    pub fn set_engine(&self, worker_id_num: u64, engine: Engine) {
        if let Some(mut entry) = self.by_num.get_mut(&worker_id_num) {
            entry.value_mut().engine = Some(engine);
        }
    }

    /// Mark the worker's initial ZMQ event-stream sync as complete.
    /// Called by the ingest listener once at the transition from
    /// initial replay-drain into steady-state live receive. Sticky:
    /// never set back to false (libzmq's invisible reconnects don't
    /// reset readiness). Returns `true` if the entry existed and was
    /// updated.
    pub fn set_initial_sync_complete(&self, worker_id_num: u64) -> bool {
        match self.by_num.get_mut(&worker_id_num) {
            Some(mut entry) => {
                entry.value_mut().initial_sync_complete = true;
                true
            }
            None => false,
        }
    }

    /// Record that a KV cache event actually arrived from this worker.
    ///
    /// Distinct from [`Self::set_initial_sync_complete`], which fires once the
    /// SUB socket is in steady-state receive — and a ZMQ `connect()` succeeds
    /// against a port where nothing is listening, so that flag cannot tell a
    /// live publisher from a misconfigured one. This one can: it is only ever
    /// set by a frame we decoded.
    ///
    /// Sticky, deliberately. Events flow only when there is traffic, so a
    /// decaying signal would report "not subscribed" for a healthy but idle
    /// fleet — a false alarm on the common case.
    ///
    /// Callers SHOULD gate this on their own local flag so the hot ingest path
    /// takes this write once per listener rather than once per event.
    pub fn note_kv_events_observed(&self, worker_id_num: u64) -> bool {
        match self.by_num.get_mut(&worker_id_num) {
            Some(mut entry) => {
                entry.value_mut().kv_events_observed = true;
                true
            }
            None => false,
        }
    }

    /// True when at least one registered worker has both finished its
    /// `/v1/models` scrape AND completed its initial ZMQ sync. Drives
    /// the process-level `/readyz` probe; see
    /// [`WorkerEntry::is_ready`] for the per-worker definition.
    pub fn any_ready(&self) -> bool {
        self.by_num.iter().any(|e| e.value().is_ready())
    }

    /// Snapshot read of a single worker. Returns an owned clone so the
    /// caller doesn't hold a DashMap guard across awaits.
    pub fn lookup(&self, worker_id_num: u64) -> Option<WorkerEntry> {
        self.by_num.get(&worker_id_num).map(|e| e.value().clone())
    }

    /// Does any worker serve `model_name`? Used by the gRPC handler to
    /// short-circuit a `NotFound` before the full fanout.
    pub fn has_model(&self, model_name: &str) -> bool {
        let by_model = self.by_model.lock().unwrap();
        by_model
            .get(model_name)
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    }

    /// Snapshot of `(worker_id_num, WorkerEntry)` pairs for every worker
    /// that currently serves `model_name`. Entries are cloned to release
    /// the DashMap guard before the caller might `.await` over them. Order
    /// is unspecified (HashSet → Vec).
    pub fn workers_for_model(&self, model_name: &str) -> Vec<(u64, WorkerEntry)> {
        let nums: Vec<u64> = {
            let by_model = self.by_model.lock().unwrap();
            match by_model.get(model_name) {
                None => return Vec::new(),
                Some(set) => set.iter().copied().collect(),
            }
        };
        nums.into_iter()
            .filter_map(|n| self.by_num.get(&n).map(|e| (n, e.value().clone())))
            .collect()
    }

    /// Distinct model names this registry currently serves (the
    /// `by_model` keys). Used by the mock's `"auto"` resolution to pick
    /// a concrete model to route to.
    pub fn known_models(&self) -> Vec<String> {
        self.by_model.lock().unwrap().keys().cloned().collect()
    }

    /// Iterate the current `(worker_id_num, WorkerEntry)` snapshot. Used
    /// by the dashboard / debug paths that need the whole fleet view.
    /// Returns owned clones for the same reason as `workers_for_model`.
    pub fn iter_snapshot(&self) -> Vec<(u64, WorkerEntry)> {
        self.by_num
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }

    /// Just the ids, without cloning every entry.
    ///
    /// For per-tick sweeps that only need to look a worker up elsewhere —
    /// `iter_snapshot` clones a `WorkerEntry` per worker, which is wasteful at
    /// fleet scale when the strings are never read.
    pub fn ids(&self) -> Vec<u64> {
        self.by_num.iter().map(|e| *e.key()).collect()
    }

    pub fn len(&self) -> usize {
        self.by_num.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_num.is_empty()
    }
}

#[cfg(test)]
mod tests;
