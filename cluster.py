#!/usr/bin/env python3
"""Isolated multi-host experiment controller; compatible with Python 3.6."""
import argparse
import concurrent.futures
import getpass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pty
import re
import select
import shlex
import shutil
import socket
import signal
import subprocess
import sys
import tarfile
import time
import termios

import yaml

REPO = Path(__file__).resolve().parent
ROOT = Path("/root/du_sharding")


def dump(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def checked_path(path):
    path = Path(path).resolve()
    if ROOT not in path.parents:
        raise ValueError("path outside experiment root: " + str(path))
    return path


def hosts(cfg):
    rows = []
    for shard in range(cfg["business_shards"] + 1):
        for org in range(1, 5):
            server = org + 1 if shard == 0 else 6 + 2 * (org - 1) + (shard - 1) % 2
            offset = cfg["business_shards"] * 4 if shard == 0 else (shard - 1) * 4
            index = offset + org - 1
            rows.append(dict(shard="bridge-shard" if shard == 0 else "business-shard%d" % shard,
                             key="bridge" if shard == 0 else "business-%d" % shard,
                             org=org, server=server, ip="10.16.8.%d" % (server + 1),
                             p2p=11301+index, rpc=12301+index, sync=13301+index,
                             engine=22351+index, runtime=32351+index))
    return rows


def remote(cfg, server, argv, timeout=180, source=None, destination=None):
    """Only connect to the explicitly assigned experiment addresses."""
    if server not in range(2, 14):
        raise ValueError("invalid remote server")
    target = "root@10.16.8.%d" % (server + 1)
    opts = ["-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
            "-o", "StrictHostKeyChecking=accept-new", "-o", "UserKnownHostsFile=" + str(ROOT / "known_hosts")]
    if source is not None:
        checked_path(destination)
        command = ["scp"] + opts + [str(source), target + ":" + str(destination)]
    else:
        command = ["ssh"] + opts + [target, " ".join(shlex.quote(str(x)) for x in argv)]
    master, slave = pty.openpty()
    def controlling_terminal():
        os.setsid()
        fcntl.ioctl(0, termios.TIOCSCTTY, 0)
    proc = subprocess.Popen(command, stdin=slave, stdout=slave, stderr=slave, close_fds=True, preexec_fn=controlling_terminal)
    os.close(slave)
    output = bytearray()
    prompt = bytearray()
    deadline = time.monotonic() + timeout
    sent = False
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.2)
            if ready:
                try:
                    data = os.read(master, 65536)
                except OSError:
                    break
                if not data:
                    break
                output.extend(data)
                prompt.extend(data)
                if b"password:" in prompt.lower():
                    if sent:
                        raise RuntimeError("SSH authentication rejected")
                    password = os.environ.get("DU_SSH_PASSWORD", "")
                    if not password:
                        raise RuntimeError("SSH password unavailable")
                    os.write(master, (password + "\n").encode())
                    sent = True
                    prompt.clear()
            elif proc.poll() is not None:
                break
        else:
            raise RuntimeError("SSH operation timeout; check remote completion before retrying")
        code = proc.wait(timeout=10)
        text = output.decode(errors="replace")
        if code:
            raise RuntimeError("server%d: %s" % (server, text[-6000:]))
        return text
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=10)
        os.close(master)


def package_name(row):
    return "chainmaker-v2.4.0_alpha-%s-wx-org%d.chainmaker.org" % (row["shard"], row["org"])


def run_root(cfg):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", cfg["run_id"]):
        raise ValueError("invalid run_id")
    return checked_path(ROOT / "runs" / cfg["run_id"])


def node_path(cfg, row):
    return run_root(cfg) / "build" / "release" / row["shard"] / package_name(row)


def generate(cfg):
    root = run_root(cfg)
    if root.exists():
        raise RuntimeError("run directory already exists; choose a fresh run_id, no automatic deletion")
    root.mkdir(parents=True)
    (root / "scripts").mkdir()
    (root / "tools" / "cmc" / "testdata").mkdir(parents=True)
    shutil.copytree(str(REPO / "template" / "config_tpl"), str(root / "config" / "config_tpl"))
    crypto = root / "tools" / "chainmaker-cryptogen"
    shutil.copytree(str(REPO / "template" / "cryptogen-config"), str(crypto / "config"))
    (crypto / "bin").mkdir()
    shutil.copy2(str(REPO / "scripts" / "cryptogen.sh"), str(crypto / "bin" / "chainmaker-cryptogen"))
    os.chmod(str(crypto / "bin" / "chainmaker-cryptogen"), 0o755)
    shutil.copy2(str(REPO / "template" / "prepare_sharding.sh"), str(root / "scripts" / "prepare.sh"))
    answers = [cfg["business_shards"], 4, 4, cfg["schedule_start_height"],
               cfg["intra_consensus_rounds"], cfg["inter_consensus_rounds"], 0, 1, "INFO", "YES", "INFO", "NO"]
    with (root / "generate.log").open("w") as log:
        subprocess.run(["bash", "prepare.sh"], cwd=str(root / "scripts"),
                       input="\n".join(map(str, answers)) + "\n", universal_newlines=True,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    rows = hosts(cfg)
    for row in rows:
        source = root / "build" / row["shard"] / "config" / ("node%d" % row["org"])
        cm = yaml.safe_load((source / "chainmaker.yml").read_text())
        seeds = []
        for peer in [r for r in rows if r["shard"] == row["shard"]]:
            certroot = root / "build" / row["shard"] / "crypto-config" / ("wx-org%d.chainmaker.org" % peer["org"])
            nodeid = (certroot / "node/consensus1/consensus1.nodeid").read_text().strip()
            seeds.append("/ip4/%s/tcp/%d/p2p/%s" % (peer["ip"], peer["p2p"], nodeid))
        cm["net"]["seeds"] = seeds
        cm["net"]["listen_addr"] = "/ip4/0.0.0.0/tcp/%d" % row["p2p"]
        cm["rpc"]["port"] = row["rpc"]
        (source / "chainmaker.yml").write_text(yaml.safe_dump(cm, default_flow_style=False, allow_unicode=True))
        sh = yaml.safe_load((source / "sharding.yml").read_text())
        sh["sync_network"]["port"] = row["sync"]
        sh["sync_network"]["seeds"] = []
        if row["key"] != "bridge":
            bridge = next(r for r in rows if r["key"] == "bridge" and r["org"] == row["org"])
            bid = (root / "build/bridge-shard/crypto-config" / ("wx-org%d.chainmaker.org" % row["org"]) / "node/consensus1/consensus1.nodeid").read_text().strip()
            sh["sync_network"]["seeds"] = ["/ip4/%s/tcp/%d/p2p/%s" % (bridge["ip"], bridge["sync"], bid)]
        (source / "sharding.yml").write_text(yaml.safe_dump(sh, default_flow_style=False, allow_unicode=True))
        dest = node_path(cfg, row)
        org = "wx-org%d.chainmaker.org" % row["org"]
        shutil.copytree(str(source), str(dest / "config" / org))
        (dest / "bin").mkdir()
        (dest / "log").mkdir()
        # Binaries and runtime libraries are immutable shared artifacts, not system installs.
        (dest / "bin" / "chainmaker").symlink_to(REPO / "artifacts/bin/chainmaker")
        (dest / "lib").symlink_to(REPO / "artifacts/lib")
    dump(root / "layout.json", rows)
    dump(root / "config.used.json", cfg)
    bench = dict(cfg, chainmaker_dir=str(root), nodes_per_shard=4, base_rpc_port=12301,
                 node_conn_cnt=10, coordinator_server="sender", amount=1, initial_balance=1000000000,
                 init_accounts=False, ratio_window=1000, seed=20260518, tx_timeout_seconds=60,
                 deploy_timeout_seconds=180, deploy_confirm_wait_seconds=300,
                 height_wait_seconds=60, prepare_target_height=10, prepare_concurrency=3, register_concurrency=3,
                 work_dir=str(root / "out"), servers=[dict(name="sender", host="10.16.8.2", node_id=1, rpc_host="127.0.0.1")],
                 shard_rpc_hosts={r["key"]: r["ip"] for r in rows if r["org"] == 1},
                 metrics_command=["python3", str(REPO / "cluster.py"), "collect", "--config", str(root / "config.used.json")])
    for key, name in [("standard_dfa_experiment_contract_path", "standard_dfa_experiment"),
                      ("poke_block_contract_path", "poke_block"), ("heartbeat_contract_path", "sharding_heartbeat")]:
        bench[key] = str(REPO / "bench/contracts" / name / (name + ".7z"))
    dump(root / "bench.json", bench)
    print("generated:", root, flush=True)


def deploy(cfg):
    root = run_root(cfg)
    for server in sorted({r["server"] for r in hosts(cfg)}):
        remote(cfg, server, ["mkdir", "-p", str(ROOT)])
        remote(cfg, server, ["python3", "-c", "import os,subprocess; p='/root/du_sharding/temp'; subprocess.check_call(['git','-C',p,'pull','--ff-only'] if os.path.isdir(p+'/.git') else ['git','clone','git@github.com:bestWisherZ/temp.git',p])"])
        remote(cfg, server, None, source=ROOT / "artifacts.tar.gz", destination=ROOT / "artifacts.tar.gz", timeout=300)
        remote(cfg, server, ["tar", "xzf", str(ROOT / "artifacts.tar.gz"), "-C", str(REPO)])
        remote(cfg, server, None, source=ROOT / "vm-engine-v2.4.0.tar.gz", destination=ROOT / "vm-engine-v2.4.0.tar.gz", timeout=600)
        remote(cfg, server, ["docker", "load", "-i", str(ROOT / "vm-engine-v2.4.0.tar.gz")], timeout=300)
        archive = root / ("server%d.tar.gz" % server)
        with tarfile.open(str(archive), "w:gz") as tf:
            for row in [r for r in hosts(cfg) if r["server"] == server]:
                path = node_path(cfg, row)
                tf.add(str(path), arcname=str(path.relative_to(ROOT)))
            for filename in ["config.used.json", "layout.json"]:
                tf.add(str(root / filename), arcname=str((root / filename).relative_to(ROOT)))
        remote(cfg, server, None, source=archive, destination=ROOT / archive.name, timeout=180)
        remote(cfg, server, ["tar", "xzf", str(ROOT / archive.name), "-C", str(ROOT)])
        print("deployed server%d" % server, flush=True)


def local_start(cfg, server):
    for row in [r for r in hosts(cfg) if r["server"] == server]:
        dest = node_path(cfg, row)
        pidpath = dest / "node.pid"
        if pidpath.exists():
            raise RuntimeError("PID record exists; inspect status before starting: " + str(pidpath))
        for port in [row[k] for k in ["p2p", "rpc", "sync", "engine", "runtime"]]:
            probe = socket.socket()
            try:
                probe.bind(("0.0.0.0", port))
            finally:
                probe.close()
        org = "wx-org%d.chainmaker.org" % row["org"]
        vmdata, vmlog = dest / "data" / org / "go", dest / "log" / org / "go"
        vmdata.mkdir(parents=True, exist_ok=True)
        vmlog.mkdir(parents=True, exist_ok=True)
        name = "du-%s-%s-org%d" % (cfg["run_id"], row["key"], row["org"])
        env = dict(CHAIN_RPC_PROTOCOL="1", CHAIN_HOST="127.0.0.1", CHAIN_RPC_PORT=str(row["engine"]),
                   SANDBOX_RPC_PORT=str(row["runtime"]), MAX_SEND_MSG_SIZE="100", MAX_RECV_MSG_SIZE="100",
                   MAX_CONN_TIMEOUT="10", MAX_ORIGINAL_PROCESS_NUM="20", CGROUP_DISABLE="true",
                   PROCESS_PRELOAD_DISABLE="false", PROCESS_PRELOAD_NUM_BY_USE_FREQUENCY="10",
                   PROCESS_PRELOAD_NUM_BY_LAST_TIME="10", DOCKERVM_CONTRACT_ENGINE_LOG_LEVEL="INFO",
                   DOCKERVM_SANDBOX_LOG_LEVEL="INFO", DOCKERVM_LOG_IN_CONSOLE="false")
        command = ["docker", "run", "-d", "--privileged", "--name", name, "--label", "du_sharding.run=" + cfg["run_id"],
                   "--network", "host", "--cpus", str(cfg["vm_cpus"]), "--memory", cfg["vm_memory"],
                   "--pids-limit", "4096", "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
                   "-v", str(vmdata) + ":/mount", "-v", str(vmlog) + ":/log"]
        for key, value in env.items():
            command += ["-e", key + "=" + value]
        subprocess.check_call(command + [cfg["vm_image"]])
        runtime_env = dict(os.environ, GOMAXPROCS=str(cfg["node_gomaxprocs"]),
                           PATH=str(dest / "lib") + ":" + os.environ["PATH"])
        with (dest / "log" / "console.log").open("ab") as log:
            process = subprocess.Popen(["bash", str(REPO / "scripts/portable_exec.sh"),
                str(dest / "bin/chainmaker"), "start", "-c", str(dest / "config" / org / "chainmaker.yml")],
                cwd=str(dest / "bin"), env=runtime_env, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        pidpath.write_text(str(process.pid) + "\n")
        print("started", row["key"], org, process.pid, flush=True)


def local_stop(cfg, server):
    for row in [r for r in hosts(cfg) if r["server"] == server]:
        dest = node_path(cfg, row)
        pidpath = dest / "node.pid"
        if pidpath.exists():
            pid = int(pidpath.read_text().strip())
            proc = Path("/proc") / str(pid)
            if proc.exists():
                cmd = (proc / "cmdline").read_bytes()
                if os.readlink(str(proc / "cwd")) != str(dest / "bin") or str(dest / "bin/chainmaker").encode() not in cmd:
                    raise RuntimeError("refusing to signal PID belonging to another process")
                os.kill(pid, signal.SIGTERM)
                deadline = time.monotonic() + 30
                while proc.exists() and time.monotonic() < deadline:
                    if (proc / "stat").read_text().split()[2] == "Z":
                        break
                    time.sleep(0.2)
                else:
                    if proc.exists():
                        raise RuntimeError("node did not stop, no forced kill issued")
            pidpath.rename(dest / "node.pid.stopped")
        name = "du-%s-%s-org%d" % (cfg["run_id"], row["key"], row["org"])
        check = subprocess.run(["docker", "inspect", "--format", '{{ index .Config.Labels "du_sharding.run" }}', name],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if check.returncode == 0:
            if check.stdout.strip() != cfg["run_id"]:
                raise RuntimeError("container ownership label mismatch")
            subprocess.check_call(["docker", "stop", "--time", "20", name])
        print("stopped own node:", row["key"], row["org"], flush=True)


def repair_vm(cfg, server):
    for row in [r for r in hosts(cfg) if r["server"] == server]:
        name = "du-%s-%s-org%d" % (cfg["run_id"], row["key"], row["org"])
        info = json.loads(subprocess.check_output(["docker", "inspect", name]).decode())[0]
        if info["Config"].get("Labels", {}).get("du_sharding.run") != cfg["run_id"]:
            raise RuntimeError("container ownership mismatch")
        if info["HostConfig"]["Privileged"]:
            print("VM already configured:", name)
            continue
        subprocess.check_call(["docker", "stop", "--time", "10", name])
        subprocess.check_call(["docker", "rename", name, name + "-unprivileged"])
        args = ["docker", "run", "-d", "--privileged", "--name", name,
                "--label", "du_sharding.run=" + cfg["run_id"], "--network", "host",
                "--ipc", "private", "--cpus", str(cfg["vm_cpus"]), "--memory", cfg["vm_memory"],
                "--pids-limit", "4096", "--log-opt", "max-size=10m", "--log-opt", "max-file=2"]
        for mount in info["Mounts"]:
            checked_path(mount["Source"])
            args += ["-v", mount["Source"] + ":" + mount["Destination"]]
        for env in info["Config"]["Env"]:
            args += ["-e", env]
        subprocess.check_call(args + [cfg["vm_image"]])


def local_metrics(cfg, server):
    for row in [r for r in hosts(cfg) if r["server"] == server and r["org"] == 1]:
        base = node_path(cfg, row)
        out = run_root(cfg) / "metrics" / (row["key"] + ".log")
        out.parent.mkdir(parents=True, exist_ok=True)
        pattern = re.compile(r"SCHED_METRIC|BLOCK_METRIC|TIMING|block\[|put block|commit block|proposer success|proposing timeout|new height|enter new height")
        with out.open("w") as dest:
            for path in sorted((base / "log").glob("system.log*")):
                with path.open(errors="replace") as src:
                    for line in src:
                        if pattern.search(line):
                            dest.write(line)
        print(str(out), flush=True)


def collect(cfg):
    for row in [r for r in hosts(cfg) if r["org"] == 1]:
        remote(cfg, row["server"], ["python3", str(REPO / "cluster.py"), "local-metrics", "--server", str(row["server"]), "--config", str(run_root(cfg) / "config.used.json")])
        source = run_root(cfg) / "metrics" / (row["key"] + ".log")
        # Metric lines only, not VM payload logs. No credentials leave their owner.
        text = remote(cfg, row["server"], ["cat", str(source)])
        dest = node_path(cfg, row) / "log" / "system.log"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text.replace("\r\n", "\n"))
        print("metrics:", row["key"], "bytes", dest.stat().st_size, flush=True)


def benchmark(cfg):
    root = run_root(cfg)
    config = str(root / "bench.json")
    subprocess.check_call(["python3", str(REPO / "bench/tools/generate_workload.py"), "--config", config])
    subprocess.check_call(["bash", str(REPO / "scripts/portable_exec.sh"), str(REPO / "artifacts/bin/shard-worker"),
                           "run", "-config", config, "-server", "sender", "-dataset",
                           str(root / "out/workload/transactions_all.jsonl"), "-out-dir", str(root / "out")], cwd=str(REPO / "bench"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["generate", "deploy", "start", "stop", "local-start", "local-stop", "local-metrics", "collect", "prepare", "run", "audit", "repair-vm", "local-repair-vm"])
    parser.add_argument("--config", default=str(REPO / "config.json"))
    parser.add_argument("--server", type=int, default=0)
    parser.add_argument("--password-stdin", action="store_true")
    args = parser.parse_args()
    cfg = json.loads(Path(args.config).read_text())
    if Path(cfg["root"]) != ROOT:
        raise ValueError("experiment root must be /root/du_sharding")
    if cfg["business_shards"] not in [2, 4, 8, 16, 32]:
        raise ValueError("unsupported shard count")
    if args.action in ["deploy", "start", "stop", "collect", "run", "repair-vm"] and not os.environ.get("DU_SSH_PASSWORD"):
        os.environ["DU_SSH_PASSWORD"] = sys.stdin.readline().rstrip("\n") if args.password_stdin else getpass.getpass("Cluster SSH password: ")
    if args.action == "generate":
        generate(cfg)
    elif args.action == "deploy":
        deploy(cfg)
    elif args.action == "start":
        for server in sorted({r["server"] for r in hosts(cfg)}):
            print(remote(cfg, server, ["python3", str(REPO / "cluster.py"), "local-start", "--server", str(server), "--config", str(run_root(cfg) / "config.used.json")]), flush=True)
    elif args.action == "local-start":
        local_start(cfg, args.server)
    elif args.action == "stop":
        for server in sorted({r["server"] for r in hosts(cfg)}):
            print(remote(cfg, server, ["python3", str(REPO / "cluster.py"), "local-stop", "--server", str(server), "--config", str(run_root(cfg) / "config.used.json")]), flush=True)
    elif args.action == "local-stop":
        local_stop(cfg, args.server)
    elif args.action == "repair-vm":
        for server in sorted({r["server"] for r in hosts(cfg)}):
            remote(cfg, server, ["git", "-C", str(REPO), "pull", "--ff-only"])
            print(remote(cfg, server, ["python3", str(REPO / "cluster.py"), "local-repair-vm", "--server", str(server), "--config", str(run_root(cfg) / "config.used.json")]), flush=True)
    elif args.action == "local-repair-vm":
        repair_vm(cfg, args.server)
    elif args.action == "local-metrics":
        local_metrics(cfg, args.server)
    elif args.action == "collect":
        collect(cfg)
    elif args.action == "prepare":
        subprocess.check_call(["bash", str(REPO / "scripts/portable_exec.sh"), str(REPO / "artifacts/bin/shard-worker"), "prepare", "-config", str(run_root(cfg) / "bench.json"), "-server", "sender", "-out-dir", str(run_root(cfg) / "out/prepare")], cwd=str(REPO / "bench"))
    elif args.action == "run":
        benchmark(cfg)
    elif args.action == "audit":
        subprocess.check_call(["bash", str(REPO / "scripts/portable_exec.sh"), str(REPO / "artifacts/bin/shard-worker"), "audit", "-config", str(run_root(cfg) / "bench.json"), "-server", "sender"], cwd=str(REPO / "bench"))


if __name__ == "__main__":
    main()
