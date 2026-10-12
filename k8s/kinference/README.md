# Deploying kinference

## Prerequisites

- `kubectl` on the target cluster. For EKS: `make kubeconfig` in `aws-harness/`.
- CRDs installed. Cluster-scoped, so not part of `make deploy`:
  ```sh
  make crds                    # add TARGET=local for Docker Desktop
  ```

  **On a fresh cluster the order does not matter. On an UPGRADE it does: deploy
  the new image FIRST, then apply the CRDs.** A schema change is not additive
  from the router's side — the API server prunes any field the new schema no
  longer declares, immediately, on objects already stored. A running router that
  still requires a pruned field cannot deserialize them, and because
  `kube::runtime::watcher` deserializes list pages *wholesale*, one changed
  object fails **every** object of that kind in the namespace. Discovery stops
  for all of them and retries forever:

  ```
  watcher error; continuing kind="InferenceWorkerSet"
    error=failed to perform initial object list: Error deserializing response:
    missing field `upstreamName`
  ```

  The router parses permissively for exactly this reason — every spec field is
  `Option` even where the schema marks it required — so a NEW binary reads both
  the old and the new shape and the upgrade is seamless in that direction only.
- For real traffic: vLLM/SGLang pods Ready behind a selector-based Service (any
  type). Find it with `kubectl get svc | grep -iE "vllm|sglang"` and put the name
  in a worker set's `serviceRef.name`.

## Deploy

### AWS / EKS

```sh
cd k8s/kinference
make ecr-login                                    # 12h token
make crds                                         # once per cluster
make build push deploy TAG=$(git rev-parse --short HEAD)
```

### Docker Desktop

```sh
cd k8s/kinference
make local-setup TARGET=local                     # once: crds + node label
make build push deploy TARGET=local TAG=dev
```

`push` is a no-op locally. The pod becomes Ready with no workers at all — `/readyz`
answers "can I serve a Pick", not "is there anywhere to route to". CRD paths
(policy compilation, status, provider resolution) work; picks return `NO_WORKERS`
until something serves the model.

`make help` prints the resolved target, image and platform.

## The gateway Extension

`extension.yaml` is the `Extension` CR (`gateway.linkerd.io/v1alpha1`, class
`kinference_selector`) a Buoyant gateway prepends to a route's filter chain so every
request asks kinference where to go. It is applied by `make deploy` along with the
rest, and it is not optional: every `BackendBinding` the router publishes names
it via `[policy] backend_binding_extension = "chat-picker"` in
`configmap.yaml`, so without it those bindings program routes whose filter chain
cannot be built.

Its CRD comes from the gateway's Helm chart rather than from `make crds`.

Two couplings to keep:

- **Name.** Rename it in `extension.yaml` and `configmap.yaml` together or in
  neither.
- **Namespace.** It must live in the pools' namespace (`[policy] namespace`),
  because the gateway resolves the reference in the *binding's* namespace and a
  binding lives in its pool's.

```sh
kubectl -n default get extension chat-picker
```

## What gets applied

`rbac.yaml configmap.yaml service.yaml deployment.yaml extension.yaml`, then
everything in `../examples/demo-configs/*.yaml`. Leave the image in `deployment.yaml` alone —
`REPLACE_ME_ECR_REPO/kinference:latest` is substituted at apply time, so plain
`kubectl apply -f .` ships a broken image reference.

## Verify

```sh
kubectl -n default logs deployment/kinference --tail=50      # "Added worker" per Ready pod
kubectl -n default get iws,infpools,infpols,infprovs      # STATE on every kind
```

```sh
kubectl -n default port-forward svc/kinference 9001:9001 &
curl -s localhost:9001/readyz -o /dev/null -w '%{http_code}\n'
curl -s localhost:9001/api/snapshot | jq '.workers | map({id, pool, ip, health, served_models})'
curl -s localhost:9001/metrics | grep -E '^kinference_(policy_decisions|score_degraded|ttft_ms_count|provider_ttft_ms_count)'
# Per-tenant labels: this configmap sets [metrics] tenant_labels = "all", so each
# tenant gets its own series. Everything unattributed reads tenant="default".
curl -s localhost:9001/metrics | grep -E '^kinference_prompt_tokens_total'
```

Per-request predicted-vs-observed rows:

```sh
kubectl -n default cp deploy/kinference:/var/log/kinference/latency.csv ./latency.csv
python3 analysis/analyze-latency.py latency.csv
```

The CSV is an `emptyDir` — rows die with the pod. Swap to a PVC to keep them.

## Switch policies

Policies are applied but inert until a pool names one. `STATE` shows which is live.

```sh
kubectl -n default patch infpool inference-all --type=merge \
  -p '{"spec":{"policyRef":{"name":"policy-4"}}}'

kubectl -n default patch infpool inference-all --type=json \
  -p '[{"op":"remove","path":"/spec/policyRef"}]'        # back to serve-as-asked
```

```sh
kubectl -n default get infpol policy-4 -o jsonpath='{.status.warnings}' | jq
kubectl -n default get infpool inference-all -o jsonpath='{.status.models}' | jq
```

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| Pod `Pending`, `FailedScheduling` | Node lacks `role=system`. `kubectl label nodes --all role=system` |
| `ImagePullBackOff` on `TARGET=local` | Docker Desktop's containerd image store. Toggle Settings → General → "Use containerd…" |
| `CrashLoopBackOff`, `exec format error` | amd64 image on an arm64 node. Rebuild with `TARGET=local` |
| `failed to read in-cluster Kubernetes service-account` | `rbac.yaml` not applied. Apply and roll the pod |
| `endpointslices.discovery.k8s.io: forbidden` | ClusterRoleBinding not propagated. Wait ~30s |
| `workers: 0` in the snapshot | `serviceRef.name` matches no Service, or it has no Ready endpoints. `kubectl get endpointslices -l kubernetes.io/service-name=<name>` |
| `Accepted=False` / `UnknownProfile` on an IWS | `spec.profile` names no `[[profile]]` in `configmap.yaml`. Fix either; no restart needed |
| `overlap 0/N` forever, `index.blocks` 0 | `spec.kvBlockSize` ≠ tokens-per-block the engine **publishes**. Grep `Block not published. Block size must be N tokens` and use that N. Not always `--block-size`: Gemma 4 reports 64, publishes 256 |
| Pool drains after a few `BlockStored` | Persistent block-size mismatch trips the eviction guard (3 consecutive frames) |
| Workers Added but `unresponsive` | Pod IPs unreachable from the kinference pod. Check NetworkPolicies |
| `STATE` blank on a policy | No status written at all — it is in no namespace kinference watches. Check `[policy].namespace` |
| `STATE` has `Unreferenced` on an IWS | No `InferencePool` lists it. Not a fault — it still serves a Pick that names no pool — but no pool-scoped policy applies. The `Referenced` condition carries the patch command |
| `STATE` blank on an IWS | No status written at all — it is in no namespace kinference watches. Check `[policy].namespace` |
| Pod never Ready | Policy is on and the CRD watch has not listed. `curl :9001/readyz` says so; check RBAC on `inferencepools`/`inferencepolicies` |
| Provider always loses / never picked | Suppressed after 3 dispatch failures. `kubectl get infprov <name> -o jsonpath='{.status.conditions}'` |

## Undeploy

```sh
cd k8s/kinference && make undeploy                  # add TARGET=local for Docker Desktop
```

Deletes the Deployment, Service, ConfigMap, RBAC, Extension and everything in
`../examples/demo-configs/`. CRDs are never removed — deleting one garbage-collects every
object of that kind. The leader-election Lease expires on its own.
