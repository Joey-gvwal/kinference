**Memory allocation for Docker Desktop:** the Linux VM Docker Desktop
runs on defaults to a modest memory allocation (often ~2 GiB on macOS).
This deployment needs at least 6 GiB of VM memory to run vLLM +
container runtime overhead comfortably. Bump it in
**Settings → Resources → Memory** (macOS/Windows). If vLLM fails at
startup with `Available memory on node 0 (...) less than desired CPU
memory utilization`, this is the underlying cause — either give the
VM more RAM or lower `--gpu-memory-utilization` (an already-low 0.3 in
the manifest; can go lower).

**2. Deploy**

```bash
kubectl apply -f k8s/dev/dummy-vllm.yaml
```

First startup takes ~30-60s (image pull + HF config download + vLLM
init). Watch progress:

```bash
kubectl get pod -l mm/server-name=dummy-vllm -w
kubectl logs -l mm/server-name=dummy-vllm -f
```

Look for `Uvicorn running on http://0.0.0.0:8000` — that's the ready
signal.

**3. Sanity-check the HTTP endpoint**

From inside another pod on the same cluster:

```bash
kubectl run curl --rm -it --image=curlimages/curl --restart=Never -- \
  curl -s http://dummy-vllm.default.svc.cluster.local:8000/v1/models
```

Or from the host via port-forward:

```bash
kubectl port-forward svc/dummy-vllm 8000:8000 5557:5557 5558:5558
```

Then in another shell:

```bash
curl -s http://127.0.0.1:8000/v1/models
curl -s http://127.0.0.1:8000/v1/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"HuggingFaceTB/SmolLM2-135M-Instruct","prompt":"hi","max_tokens":10}'
```

You'll get 10 random tokens as a response. That's expected (dummy
weights). The point is that the JSON envelope is correct and KV events
fired during prefill+decode.

**4. Point kinference at it**

Depending on where kinference is running:

- **kinference as a pod on the same cluster**: use in-cluster DNS. Set its
  workers.toml (or equivalent) to
  `zmq_endpoint = "tcp://dummy-vllm.default.svc.cluster.local:5557"`,
  `http_url = "http://dummy-vllm.default.svc.cluster.local:8000/"`.
- **kinference on the host** (natively, for development): use the
  port-forward above and point at `127.0.0.1`.

**5. Verify KV events flow**

```bash
kubectl logs -l mm/server-name=dummy-vllm --tail=100 | grep -iE 'kv.*event|publisher'
```

Should see the ZMQ publisher initializing. Then hit `/v1/completions`
a few times and watch kinference's dashboard/logs — cache index should
populate.

**6. Teardown**

```bash
kubectl delete -f k8s/dev/dummy-vllm.yaml
```

## Notes and caveats

- **CPU throughput is orders of magnitude slower** than a real GPU
  pod. Prefill of even a 512-token prompt takes seconds. This is
  correctness-only, never for benchmark/perf.
- **Image `mekayelanik/vllm-cpu:latest`** is a community build; lags
  official vllm/vllm-openai by 1-2 versions. If your integration
  test depends on a specific new vLLM feature, verify the flag
  exists in the pinned version first.
- **HuggingFace fetch on cold start** — the pod re-downloads
  config+tokenizer each time (emptyDir cache). Swap to a PVC if
  repeat startup latency matters.
- **Block size is not annotated** — the manifest omits
  `mm/block-size` deliberately (vLLM auto-picks; kinference discovers at
  runtime). If your kinference version still requires the annotation,
  add it explicitly under `metadata.annotations`.
