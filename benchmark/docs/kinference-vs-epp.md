Here's how to get a full evaluation of kinference vs EPP (which is llmd's equivalent
endpoint-picking project).

I like to run the LLMs on Kubernetes and everything else locally. This allows
you to move fast by altering kinference (or sometimes, EPP) without needing to futz
with tagging, packaging, redeploying, reading remote logfiles, finding pod
names, or any of that nonsense.

But it does require exposing those LLMs over an NLP, and doing some surgery on
EPP to handle this setup.

For benchmarking we use agentgateway as the core router on both sides, as the
prototype gw-pc router has concurrency issues. This means that kinference will be
called over its ext-proc interface, not the gRPC interface. The ext-proc
interface is silly for this use case (unary gRPC makes a lot more sense) but I
don't think it matters for perf--at least until we start doing tokenization
within the gateway.

We are also going to use remote tokenization ("render" mode) rather than local
mode. Local tokenization would save a ton of latency for kinference, but EPP has
deprecated it for some insane reason so this is how we make this an
apples-to-apples comparison.

## A note about tokenization

Another consequence of this WAN setup where the LLMs are all remote is that
tokenization over the internet introduces some dumb variance here, mostly
because vLLM (which handles the tokenization) for some ridiculous Python reason
has a 5s idle timeout for all TCP connections. And establishing a new connection
from local to AWS empirically takes ~80ms. To avoid hitting this, I purposefully
throttle all tokenization calls for both systems through a *single* vLLM
instance in order to try to keep this connection warm.

(I did spend a few brain cells wondering if I should add some app-level
keepalive mechanic to aovid this but it didn't seem worth it.)

## Deploy the cluster

Ok, here we go. In the aws-harness directory are a whole variety of Makesfiles
and docs. Follow those docs to get the AWS cluster set up with all the goodies
on it, but without GPU nodes.

Note to self: to get your kubectl binary pointed in the right direction, `make
kubeconfig` and then:

```
export KUBECONFIG=~/.kube/model-harness
```

## Deploy the vLLM workers

Assuming you have the cluster set up, deploy 8 vLLM instances with:

```
make -f Makefile.server server-up CFG=servers/gemma-4-12b-vllm.toml REPLICAS=8
```

This may take some time, like 30-45 mins if the cluster has never seen these
images before.

Once the pods are ready, expose the LLMs to the Internet and hope no one finds
them:

```
make expose-workers-regen
```

Now generate the basic kinference config:

```
BASE_HOST=$(make worker-nlb-host) bash scripts/gen-monitor-config.sh > config.toml
```

Finally, update the parameters. I used these for the benchmark I present at the
BAM:

```
kv_block_size = 64

[predictor]
base_ms = 400.0
queue_ms_per_entry = 0.2
attention_uncached_block_ms = 2.3
attention_uncached_per_every_block_ms = 0.03
cache_pressure_penalty_ms = 0.0
q_running_penalty_peak_ms = 0
forward_pass_ms = 5

[picker]
top_k_min = 1
top_k_fraction = 0.0
inflight_tracking = true
replication_threshold_pct = 10
replication_warm_target = 2
tie_tolerance_ms = 0
```

This config file has 8 workers in it. In a minute, we will need to split this
into two groups of 4: half for us and half for EPP.

## Start EPP

We need a build of EPP that supports our mode of operation where the LLM
endpoints are sitting in an AWS cluster while EPP runs locally.

Check out the [llm-d-local](https://github.com/BuoyantIO/llm-d-local) repo and
follow the instructions on how to recursively get the submodules, apply the
patches, and compile EPP.

For EPP there are two important files: config.yaml and endpoints.yaml. We'll
geenrate these in a three-step process.

1) Generate the EPP config from the kinference config:

```
python ./scripts/toml-to-endpoints.py ~/[kinference checkout]/aws-harness/config.toml -o epp/endpoints.yaml
```

2) Edit that file and delete the first half of the endpoints. Those will be used
by kinference.

3) Edit the config file and make sure the tokenizer is pointing at ONE of those
endpoints
```
  - type: token-producer
    parameters:
      modelName: "google/gemma-4-12B-it"
      vllm:
        url: "http://52.43.63.133:12000"
```
Set that `url` field to one of the endpoints.

Then you can finally run EPP:
```
./llm-d-router/bin/epp --config-file=epp/config.yaml --pool-name=file-discovery --pool-namespace=default --grpc-port=9002 --grpc-health-port=9003 --metrics-port=9090 --secure-serving=false --v=4 --zap-log-level=4 --zap-encoder=console 2>&1 | tee epp.log
```

## Start kinference

Now we edit our original config (aws-harness/config.toml) and delete the last
four endpoints. Those are the ones we used for EPP.

I recommend double checking both config files at this point because I have
messed up this very simple operation (split 8 things into two groups of 4)
several times and you get crazy results when there is an overlap in vLLM
instances, or when one of the two has more endpoints than the other.

Start kinference:

```
KINFERENCE_OBSERVATIONS_CSV=latency.csv RUST_LOG=kinference=debug,dynamo_kv_router::indexer::radix_tree=error,dynamo_kv_router=warn ./target/release/kinference --config aws-harness/config.toml | tee kinference.log
```

## Start agentgateway

Finally start the two agentgateways, from the llm-d-local dir:

```
RUST_LOG=debug ./agentgateway --file config.yaml 2>&1 | tee agentgateway.log
```
and
```
RUST_LOG=debug ./agentgateway --file config-epp-on-50052.yaml 2>&1 | tee agentgateway-epp-on-50052.log
```

## Run the benchmark

Now you're ready to benchmark!

From the benchmark dir, here's the exact command I ran:

```
go run ./cmd/bench -base-url http://localhost:8081 -base-url-b http://localhost:8082 -label-a epp -label-b kinference -model "google/gemma-4-12B-it" -no-think -seed `date +%s` --workload multiturn --mt-conversations 32 --mt-turns 8 --mt-system-tokens 16384 --mt-user-tokens 128 --mt-assistant-tokens 4096 --concurrency 32 -warmup-requests 4 | tee report.txt
```

Now latency.csv will have the run data; the logs will be in kinference.log and
epp.log (for EPP); and if you need to, you can distill out timings from EPP
with:

```
python scripts/epp-decision-latency.py epp.log > epp-timings.txt
```

## Repeated experiments against the same seed

If you are running against the same random seed for utmost repeatability, you
should clear the vLLM caches in between each run. To do this you can use the
reset-caches.sh command ... however, it only finds endpoints in the
`config.*toml` directories so it won't get the EPP ones if you have split it out
exactly as I described above. So you may have to handle those manually or do
something else intelligent

You should _also_ restart EPP and kinference because they both save state.

