## Build the code

```bash
cargo build --release
```

## Run standalone (mock mode)

For demos, integration tests of downstream consumers, or local UI work, you
can run kinference with no real LLM workers. Pick responses are synthetic but
deterministic — same input always returns the same chosen endpoint.

```bash
./target/release/kinference --config examples/mock.toml
```

To run against real workers, follow the AWS K8s steps below.

## Deploy the K8s testbed on AWS

```bash
cd aws-harness
make up # bring up the bare cluster with no GPUs
export KUBECONFIG=<file generated from above>
make addons # deploy misc K8s stuff we need
```

## Deploy models

```bash
make scale-gpu N=3 # get 3 GPU nodes. Now it starts to cost money.
make -f Makefile.server server-up CFG=servers/qwen-vllm.toml # deploy 3 instances of qwen on vllm
# now wait 15 minutes ...
```

## Generate port-forward map and model config

```bash
make -f Makefile.server port-forward # maps all the ports!
# Writes two files: config.toml (profiles + worker sets) and
# config.<set>.workers.toml (each set's static endpoints, referenced from
# [workerset.discovery].workers_file).
bash scripts/gen-monitor-config.sh # -> aws-harness/config.toml (+ config.workers.toml)
```

## Run the monitor
```bash
cd ..
RUST_LOG=kinference=debug,dynamo_kv_router=warn,dynamo_kv_router::indexer::kv_indexer=trace ./target/release/kinference --config aws-harness/config.toml
```

## Send some traffic to one of the instances

In a different terminal:

```bash
curl localhost:9000/v1/chat/completions -H 'Content-Type: application/json' -d "{\"model\":\"Qwen/Qwen2.5-32B-Instruct-AWQ\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello there. How are you?\"}],\"max_tokens\":500}"
```

Congrats, you have a functioning system!

## Don't forget to tear down the cluster when you're done

```bash
make scale-gpu N=0 # leave just the rest of the cluster, which is cheap
# and/or...
make down # delete the whole thing
```
