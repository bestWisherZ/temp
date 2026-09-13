# Multi-host sharding experiment

This repository contains the experiment controller and benchmark harness. It
does not replace the sharding system source repository. The first node binary
was built from `bestWisherZ/chainmaker-go-v2.4.0_alpha` commit `a5935a2`.

Every server also keeps the complete system Git repository, including its
source files and history, at `/root/du_sharding/chainmaker-go-v2.4.0_alpha`.
Its origin is `git@github.com:bestWisherZ/chainmaker-go-v2.4.0_alpha.git`.
System source changes belong there, not in this experiment-tool repository.
The node executable remains in `temp/artifacts/bin/chainmaker`; pulling source
alone does not rebuild that executable. Keep its build revision consistent
with the source revision recorded for an experiment.

All generated files live under `/root/du_sharding`. No controller command
cleans that directory or stops unrelated services. Generated certificates,
private keys, server passwords, runtime data, and binary dependency archives
are not committed to Git.

## Placement

Server numbers refer to the supplied inventory, not operating-system hostnames.

| Role | Server numbers | Private addresses |
| --- | --- | --- |
| Controller and transaction sender | 1 | 10.16.8.2 |
| Bridge org1, org2, org3, org4 | 2, 3, 4, 5 | 10.16.8.3 through 10.16.8.6 |
| Odd business shard org1 through org4 | 6, 8, 10, 12 | 10.16.8.7, .9, .11, .13 |
| Even business shard org1 through org4 | 7, 9, 11, 13 | 10.16.8.8, .10, .12, .14 |

With two business shards there is one node per server, excluding the sender.
Each node uses its own VM container. Existing Kubernetes/containerd services
are not part of this experiment.

## Prerequisites

Git, Python 3.6+ with PyYAML, Docker, and 7z must already be available. The VM
image is the official `chainmakerofficial/chainmaker-vm-engine:v2.4.0`, not the
preinstalled v3.0.1. `fetch_vm_image.py` supports verified offline image transfer
when Docker Hub cannot be reached from the servers.

The v2.4.0 VM requires `--privileged` for its security initialization, as in the
upstream start script. It keeps a private IPC and PID namespace; do not add
`--ipc=host` or `--pid=host`. Its only host mounts are this experiment's data
and log directories. CPU and memory limits apply to this experiment's VM.

`artifacts.tar.gz` is transferred separately to server1 as
`/root/du_sharding/artifacts.tar.gz` and extracted into this repository. It
contains the node, cryptogen and benchmark binaries, plus a private Linux
runtime library directory. `scripts/portable_exec.sh` uses its matching loader;
no server system library is upgraded or overwritten. The VM archive lives at
`/root/du_sharding/vm-engine-v2.4.0.tar.gz`.

## Commands on Server1

Edit only `config.json` before generating a new run. The current configuration
is 100,000 transactions at 5,000 tx/s, with 5% cross-shard traffic,
5 business consensus blocks and 8 bridge consensus blocks per cycle. A prior
10,000-transaction validation at 500 tx/s passed execution and balance checks.
The per-node transaction pool is set to 500,000 to avoid a small queue limit
dominating this burst experiment; this is not a sustained-load capacity claim.

```bash
cd /root/du_sharding/temp
python3 cluster.py generate
python3 cluster.py deploy
python3 cluster.py start
cd scripts
./prepare_contracts.sh
./generate_workload.sh
./run_perf.sh
cd ..
python3 cluster.py audit
python3 cluster.py stop
```

These three shell entry points preserve the familiar prepare / dataset / run
workflow. All read the same repository-root `config.json`; they do not have
independent configurations. Generated `config.used.json` and `bench.json` are
run snapshots, not additional settings to edit. Configuration changes after
`generate` are rejected. The run entry point generates a dataset if missing
and validates/reuses an existing one. It never overwrites an existing run log
or result file. Node startup is concurrent across hosts and waits for all
blockchain modules to start before preparation.

Commands requiring SSH prompt for the password; it is not stored in a file.
Internal SSH uses the supplied private IPs. Only generated node packages are
copied to node hosts, not all organizations' client/admin private keys.

`generate` refuses to overwrite an existing run. For a new clean experiment,
choose another `run_id`; use `stop` to stop and inspect the previous experiment's own nodes
before reusing its ports. Do not run broad `pkill`, Docker prune, or cleanup
commands on these shared machines.

`stop` checks each recorded PID's working directory and command line before
sending SIGTERM, and checks the experiment ownership label before stopping its
VM container. It does not delete node data, logs, or stopped containers.

## Workload and Results

The workload reuses `standard_dfa_experiment` and contract SDK v2.3.9. Each
transaction gets two globally distinct balance addresses within that dataset.
Balances are lazily supplied by the experiment contract, without bulk account
initialization. This is an experimental DFA variant, not unmodified standard
DFA token economics. Reusing a dataset across runs does reuse its addresses.

The sender uses the existing direct SDK route to the source shard for local
transactions and to the bridge for cross-shard transactions. This measures the
sharding execution path without an additional standalone gateway bottleneck.

After confirmations, the harness collects compact timing logs from the org1
replica of every shard and generates the usual outputs in
`/root/du_sharding/runs/<run_id>/out/`: `result.json`, `transactions.csv`,
`blocks.csv`, `phase_sync.csv`, and per-shard CSVs in `shards/`.
The same directory retains `prepare.log`, `generate_workload.log`, `run.log`,
and `audit.json`. Node data and complete node/VM logs remain on their assigned
hosts under `runs/<run_id>/build/release/`; stopping nodes does not remove them.

`block_interval_seconds` uses adjacent commit times on the org1 replica of each
shard. `block_time_seconds` uses the existing proposal timing metadata plus
the observing replica's commit time; it is not the adjacent commit interval.
Cross-host timestamps still
require synchronized clocks. Confirmation means transaction inclusion; inspect
execution results separately before treating a run as successful throughput.
The first low-rate validation is not a saturation benchmark.

## Upstream References

Environment and packaging follow the official [v2.4.0 command-line deployment
guide](https://docs.chainmaker.org.cn/v2.4.0/html/quickstart/%E9%80%9A%E8%BF%87%E5%91%BD%E4%BB%A4%E8%A1%8C%E4%BD%93%E9%AA%8C%E9%93%BE.html).
The templates and benchmark code retain the existing project's behavior.
