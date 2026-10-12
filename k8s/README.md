# KINFERENCE Kubernetes Configs

Everything kinference needs to run on a cluster.

```
k8s/crds/        the API — CustomResourceDefinitions
k8s/kinference/       the deployment — Deployment, Service, RBAC, ConfigMap, example CRs, Makefile
k8s/grafana/     observability — PodMonitor + Grafana dashboard over /metrics
k8s/local-test/  a fake-worker rig for exercising discovery without GPUs
```

Field-by-field reference for the five kinds is in `docs/content/reference/`.

## Getting started

```bash
# Docker Desktop
make -C k8s/kinference local-setup TARGET=local        # crds + node label, once
make -C k8s/kinference build push deploy TARGET=local TAG=dev

# AWS
make -C k8s/kinference crds                            # once per cluster
make -C k8s/kinference ecr-login                       # 12h token
make -C k8s/kinference build push deploy TAG=$(git rev-parse --short HEAD)
```

`local-setup` is `crds` plus the node label a single-node cluster needs; there is
no label to apply on a real cluster, which is why the AWS path calls `crds`
directly. Either target can run `crds` on its own.

Observability is a third separate step, for the same reason `crds` is separate —
it depends on the Prometheus Operator CRDs rather than on kinference:

```bash
make -C k8s/kinference monitoring                      # PodMonitor + Grafana dashboard
```

Nothing else scrapes kinference's `/metrics`, so until that runs the dashboard is
empty and the metrics are reachable only by port-forwarding to `:9001`. See
`k8s/grafana/README.md`.

Those orderings are for a FRESH cluster. Upgrading one whose CRDs are already
installed reverses them — **`build push deploy` first, `crds` second**:

```bash
make -C k8s/kinference build push deploy TARGET=local TAG=dev   # the new binary
make -C k8s/kinference crds TARGET=local                        # then the schema
```

Applying a schema first prunes every field it no longer declares from objects
already stored, and the running router — which still requires them — then fails
to deserialize the whole list page, taking discovery down for every object of
that kind in the namespace. See `k8s/kinference/README.md` for the failure signature.

`make -C k8s/kinference help` prints the resolved target, image and platform, which is
the fastest way to tell which cluster you are about to talk to.
