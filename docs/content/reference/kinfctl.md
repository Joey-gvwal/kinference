---
title: "kinfctl"
linkTitle: "kinfctl"
weight: 68
description: >
  The command-line checker for InferencePolicy manifests: everything kinference
  can find wrong with a policy, before it is applied.
---

`kinfctl check` reads `InferencePolicy` manifests and reports what is wrong with
them — from the files alone, with no cluster and no running router.

```bash
kinfctl check policy.yaml
```

## Why check before applying

`kubectl apply` catches very little of what can be wrong with a policy, and
nothing at runtime says so either:

- **The API server drops fields it does not know, silently.** A misspelled
  `tennant:` is not an error. The condition vanishes, and the rule matches more
  than its author wrote.
- **Predicate values are free-form in the schema**, so `{greaterThan: "4"}` (a
  quoted number) and `kvCacheUsage: {greaterThan: 80}` (a percentage against a
  0–1 fraction) are both admitted.
- **Names are not checked.** `order: [databriks]` is a valid string that
  refers to nothing.

Each of these produces a policy that is accepted and quietly does less than it
says. `kinfctl check` reports all of them, each with a path into the manifest and
a stable code.

## Install

| From | Command |
| --- | --- |
| The installer | `curl -fsSL https://raw.githubusercontent.com/BuoyantIO/kinference-releases/main/install.sh \| sh` installs the newest release into `~/.kinference/bin`; set `KINFCTL_VERSION` to pin a version, `KINFCTL_INSTALL_DIR` to put it elsewhere |
| A release at [BuoyantIO/kinference-releases](https://github.com/BuoyantIO/kinference-releases/releases) | `curl -fsSL -o kinfctl https://github.com/BuoyantIO/kinference-releases/releases/download/v<version>/kinfctl-<version>-<os>-<arch>`, then `chmod +x kinfctl` and move it onto your `PATH`. `<os>-<arch>` is `darwin-arm64` or `linux-amd64`; a `.sha256` sits next to each binary |
| Source | `just install-cli`, which runs `cargo install --path . --bin kinfctl --locked` |
| The kinference image | `docker run --rm -i --entrypoint kinfctl IMAGE check - < policy.yaml` |

## check

```
kinfctl check [FILE|-]... [--pool NAMESPACE/NAME] [-n NAMESPACE] [-o text|json]
```

| Argument | Default | Effect |
| --- | --- | --- |
| `FILE` | required | One or more manifest files. `-` reads standard input. Directories are refused — pass a shell glob such as `dir/*.yaml` |
| `--pool` | none | Check every policy against this pool, as if attached to it. For a draft no pool references yet. The pool and its destinations must be among the inputs |
| `-n`, `--namespace` | `default` | Namespace for objects that set none, as `kubectl apply -n` would |
| `-o`, `--output` | `text` | `text` for people, `json` for programs |

### Inputs

Every input is a stream of YAML documents; JSON is YAML too. A `kind: List` — what
`kubectl get -o yaml` prints — is unwrapped into its items.

Documents in one input need a `---` line between them. Without one, two manifests
read as a single object with every key twice, and the input is refused. That is
what `cat a.yaml b.yaml` produces when `a.yaml` doesn't end with `---`, so pass the
files as separate arguments instead.

| Kind | Used for |
| --- | --- |
| `InferencePolicy` | Checked |
| `InferencePool`, `InferenceWorkerSet`, `InferenceProvider`, `ReferenceGrant` | Name checks only — see [Name checks](#name-checks) |
| Anything else | Skipped, and counted in the summary |

### What it reports

Every finding is one line: `E` or `W`, a path into the manifest, a stable code,
and what to do about it.

| Letter | Means |
| --- | --- |
| `E` | An error: wrong whatever pool serves the policy |
| `W` | A warning: wrong for the pools it was checked against — a backend or model name they don't have, a class of backend they lack |

A policy can serve several pools, and a name one of them lacks may be right for
another, so a name check is a warning about the pairing rather than an error in
the policy. Both are findings to fix, and either makes the exit status `1`. To
check a policy on its own, pass no pool.

Errors come first, in the order to fix them: these three, which only `kinfctl`
reports, then the codes [from the spec alone]({{< relref "inferencepolicy.md#from-the-spec-alone" >}}).

| Code | Means |
| --- | --- |
| `SchemaError` | The spec does not deserialize — `type: local` where the schema says `Local` or `Provider`. Nothing else is checked for that policy. Also reported, at the top, for a pool, worker set or provider that cannot be read |
| `DuplicatePolicy` | Two inputs define the same `namespace/name`. Applying both would keep only the last |
| `UnknownField` | A key the schema does not declare. The API server would drop it on apply. Comes with the nearest valid key when there is a plausible one |

Warnings are the codes [against the attached pools]({{< relref "inferencepolicy.md#against-the-attached-pools" >}}).

A clean policy prints only its `ok` line. Anything that limited the checks is
noted once, at the top: name checks skipped for want of a pool, model and label
checks skipped for want of the destinations' manifests, and, when a model or label
warning fired, destinations whose manifests were missing, which may be why.

```
Note: Skipping model and label checks. Add InferenceWorkerSet and InferenceProvider manifests to enable.
broken.yaml: default/broken
E   spec.rules[0].matches[0].tennant: UnknownField: not part of the InferencePolicy schema — the API server drops it on apply, silently. Did you mean `tenant`?
E   spec.rules[0].route.exclude[0].drop.kvCacheUsage: ThresholdOutOfRange: rule `burst-to-cloud`: `kvCacheUsage` is a 0–1 fraction (0.8 means 80%), so `kvCacheUsage: {greaterThan: 80}` is never true; this exclusion never drops anything. Did you mean 0.8?
W   spec.rules[0].route.select[1].order[0]: UnknownBackend: rule `burst-to-cloud`: `databriks` is not a destination of pool default/inference-all, so nothing matches it. Did you mean `databricks`? Known destinations: databricks, gemma-4-12b, qwen-2-5-32b, qwen-3-6-35b-a3b

1 policy checked, 1 with findings: 2 error(s), 1 warning(s)
```

### Name checks

The spec checks need only the policy. Checking the *names* a policy uses needs the
pool it will serve, so those are checked only when it is among the inputs:

- By default, against every input pool whose `policyRef` names the policy.
- With `--pool`, against that pool, whether or not it names the policy.
- Otherwise the name checks are skipped, and a note at the top says so.

The pool alone covers backend names and classes, as long as it lists its
destinations by name. Models and labels are declared by the worker sets and
providers, so checking those needs their manifests too:

| Codes | Need |
| --- | --- |
| `UnknownBackend`, `NoDestinationsOfType` | The pool. One that chooses its destinations by label, or lists none and so takes every worker set in its namespace, also needs their manifests |
| `UnknownModel`, `SelectorMatchesNothing` | The worker sets' and providers' manifests |

When a check cannot run, a note at the top says which, and what to add.

Resolution is the router's own, so `kinfctl` cannot disagree with a running
kinference about what a name refers to: selectors, the implicit "every worker set
in the namespace" default and `ReferenceGrant`s all behave as in the cluster. Two
differences. Offline there is no discovery, so every worker set among the inputs is
treated as found. And a destination the pool lists counts by name even when its
manifest is missing, since offline it has usually just not been passed. It
contributes no models or labels, though, and if a model or label warning fires, a
note names it.

To check a draft against what is in the cluster right now, pipe the live objects in
and name the draft beside `-`:

```bash
kubectl get infpool,iws,infprov,refgrant -A -o yaml \
  | kinfctl check --pool default/inference-all - policy.yaml
```

`-A` matters when a pool reaches into another namespace. A destination there, and
the `ReferenceGrant` that allows it, live outside the pool's namespace — a grant
always lives in the namespace being referenced — so a namespace-scoped
`kubectl get` would leave them out, and the destination would not resolve.
`refgrant` needs the Gateway API CRDs; leave it out where they are not installed.

### Exit status

| Status | Means |
| --- | --- |
| `0` | Nothing to fix. Skipped name checks are a note, not a finding |
| `1` | At least one finding, `E` or `W` |
| `2` | The input could not be checked: a file that cannot be read, YAML that does not parse, no `InferencePolicy`, or a `--pool` that is not among the inputs. The reason is on standard error, and nothing is printed to standard output |

### JSON output

`-o json` prints one object. Findings carry the same `code` and `path` as the text
form, and ordering is deterministic, so two runs diff cleanly.

| Field | Type | Meaning |
| --- | --- | --- |
| `ok` | boolean | `true` exactly when the exit status is `0` |
| `policies[].source` | string | The file, or `<stdin>` |
| `policies[].policy` | string | `namespace/name` |
| `policies[].checkedAgainstPools` | string[] | Pools whose destinations the name checks used |
| `policies[].nameChecks.ran` | boolean | Whether any name check ran |
| `policies[].nameChecks.skipped` | string, optional | What did not run, and why: `NoPool` (no pool among the inputs serves the policy; none ran), `DestinationsNotListed` (a pool chooses its destinations by label or namespace, and none of their manifests were passed; none ran) or `NoDestinationManifests` (no worker set or provider manifests were passed; backend names and classes ran, models and labels did not) |
| `policies[].nameChecks.modelsUnchecked` | string, optional | Why model names went unchecked when the rest ran |
| `policies[].nameChecks.unresolved[]` | `{pool, kind, name, reason}` | Destinations of the pools checked against that did not resolve, when model and label checks ran. A model or label only they would supply counts as unknown |
| `policies[].errors[]` | `{path, code, message, suggestion?}` | The `E` findings. `suggestion` is the nearest valid key, for an `UnknownField` |
| `policies[].warnings[]` | `{path, code, message}` | The `W` findings |
| `otherErrors[]` | `{source, document, kind, code, message}` | Pools, worker sets, providers or grants that could not be read. `code` is `SchemaError` |
| `skippedDocuments[]` | `{source, document, kind}` | Documents of other kinds. `document` is 1-based |

## Authoring a policy with an assistant

`kinfctl` is built to be driven by an assistant as much as by a person: every
finding says where it is and how to fix it, `--help` carries worked examples, and
the exit status alone says whether there is more to do. A loop that works:

1. **Give the assistant the fleet.** The pool and destination manifests from your
   repository, or `kubectl get infpool,iws,infprov,refgrant -A -o yaml`. Without
   them it can only guess at backend and model names — the mistake name checks
   exist to catch.
2. **Draft the policy**, starting from the examples in
   [InferencePolicy]({{< relref "inferencepolicy.md" >}}).
3. **Run `kinfctl check --pool NAMESPACE/NAME` until it exits `0`.** Have the
   assistant read `-o json` and fix by `code` and `path`.
4. **`kubectl apply --dry-run=server -f policy.yaml`.** This runs the CRD schema
   and its CEL rules — enums, list limits, mutually exclusive fields — without
   persisting anything.
5. **Apply**, and check `kubectl get infpol` shows no `HasWarnings`.
6. **Exercise it.** Open the dashboard's policy explorer, or call
   [`POST /api/policy/simulate`]({{< relref "policy-api.md" >}}) with the request
   shapes you care about — say, a saturated worker — and confirm the outcome.

## Diagnosing

| Symptom | Cause |
| --- | --- |
| `Note: Skipping name checks.` | No input pool references the policy. Pass the pool, and `--pool` if the policy is not attached yet |
| `Note: Skipping name checks against pools that choose destinations…` | The pool does not list its destinations by name. Pass its worker sets and providers too |
| `Note: Skipping model and label checks.` | Only the pool was passed. Its worker sets and providers declare the models and labels; pass them too |
| `UnknownModel` for a model a destination does serve | That destination's manifest is not among the inputs. A note lists it as `NotFound` |
| `Note: Model names not checked` | A worker set declares no `spec.models`, so it may serve any model name and none can be ruled out |
| Exit `2`, `is a directory` | Directories are not read. Pass files; a glob such as `dir/*.yaml` works |
| Exit `2`, `not valid YAML` | The file does not parse. The message points at the line |
| Exit `2`, `no InferencePolicy` | None of the inputs holds a policy. Pools and worker sets are only read alongside one |
| A `SchemaError` and nothing else | The spec did not deserialize, so nothing else could be checked. Fix it and run again |
