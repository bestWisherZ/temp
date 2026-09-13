#!/usr/bin/env python3
"""
Generate JSONL workloads for token_business and the two DFA profiles.

standard_dfa_experiment uses deterministic, valid-format ChainMaker addresses.
Every source and destination is globally unique, so business balance keys do
not overlap. standard_dfa keeps the official signer-owned source semantics and
therefore intentionally reports that its SDK-sender workload is conflicting.

The generated workload is sent from server15 only. Each output line is directly
consumable by:

  go run ./cmd/shard-worker run -server server15 -dataset transactions_all.jsonl
"""

import argparse
import hashlib
import json
import random
import socket
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

from config_lib import dump_json, load_config, repo_root, work_dir


TOKEN_CONTRACT = "token_business"
STANDARD_DFA_CONTRACT = "standard_dfa"
EXPERIMENT_DFA_CONTRACT = "standard_dfa_experiment"
SUPPORTED_CONTRACTS = {
    TOKEN_CONTRACT,
    STANDARD_DFA_CONTRACT,
    EXPERIMENT_DFA_CONTRACT,
}


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(
        description="Generate balanced JSONL workload for ChainMaker sharding pressure tests.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", default=str(root / "config.json"), help="JSON config path.")
    parser.add_argument("--output-dir", default="", help="Output directory. Default: <work_dir>/workload.")
    parser.add_argument("--business-shards", type=int, default=0, help="Override business_shards.")
    parser.add_argument("--tx-count", type=int, default=0, help="Override tx_count.")
    parser.add_argument("--cross-ratio", type=float, default=-1, help="Override cross_ratio. 5 means 5%%; 0.05 means 5%%.")
    parser.add_argument("--amount", type=int, default=0, help="Override amount.")
    parser.add_argument("--seed", type=int, default=0, help="Override seed.")
    parser.add_argument("--interactive", action="store_true", help="Prompt for business_shards/cross_ratio/tx_count.")
    parser.add_argument("--print-sample", type=int, default=0, help="Print the first N transactions.")
    return parser.parse_args()


def prompt_int(label: str, default_value: int) -> int:
    while True:
        try:
            raw = input(f"{label} [默认: {default_value}]: ").strip()
        except EOFError:
            return default_value
        if raw == "":
            return default_value
        try:
            value = int(raw)
        except ValueError:
            print("必须是正整数")
            continue
        if value <= 0:
            print("必须大于 0")
            continue
        return value


def prompt_ratio(default_value: float) -> float:
    while True:
        try:
            raw = input(f"请输入跨片交易比例百分比 [默认: {default_value * 100:g}，例如 5 表示 5%]: ").strip()
        except EOFError:
            return default_value
        if raw == "":
            return default_value
        try:
            value = float(raw)
        except ValueError:
            print("跨片交易比例必须是数字")
            continue
        if value > 1:
            value = value / 100.0
        if not 0 <= value <= 1:
            print("跨片交易比例必须在 0 到 100 之间")
            continue
        return value


def normalize_ratio(value: float) -> float:
    if value > 1:
        value = value / 100.0
    return value


def shard_index(account: str, shard_count: int) -> int:
    digest = hashlib.sha256(account.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % shard_count


def shard_name(index: int) -> str:
    return f"business-{index + 1}"


def derive_virtual_account(virtual_index: int, target_shard: int, shard_count: int, max_nonce: int) -> str:
    if virtual_index <= 0:
        raise RuntimeError("virtual account index must be positive")
    prefix = f"acct_{virtual_index:012d}"
    for nonce in range(max_nonce):
        account = f"{prefix}_{nonce}"
        if shard_index(account, shard_count) == target_shard:
            return account
    raise RuntimeError(
        f"cannot derive virtual account {virtual_index} for shard {target_shard + 1} "
        f"within {max_nonce} nonces"
    )


def derive_dfa_address(virtual_index: int, target_shard: int, shard_count: int, max_nonce: int) -> str:
    if virtual_index <= 0:
        raise RuntimeError("DFA address index must be positive")
    for nonce in range(max_nonce):
        digest = hashlib.sha256(f"dfa:{virtual_index}:{nonce}".encode("utf-8")).hexdigest()
        account = digest[:40]
        if account != "0" * 40 and shard_index(account, shard_count) == target_shard:
            return account
    raise RuntimeError(
        f"cannot derive DFA address {virtual_index} for shard {target_shard + 1} "
        f"within {max_nonce} nonces"
    )


def workload_method(contract: str) -> str:
    if contract == STANDARD_DFA_CONTRACT:
        return "Transfer"
    if contract == EXPERIMENT_DFA_CONTRACT:
        return "TransferExperiment"
    return "transfer"


def validate_config(cfg: Dict[str, Any]) -> None:
    if int(cfg["business_shards"]) <= 0:
        raise ValueError("business_shards must be positive")
    if int(cfg["tx_count"]) <= 0:
        raise ValueError("tx_count must be positive")
    if not 0 <= float(cfg["cross_ratio"]) <= 1:
        raise ValueError("cross_ratio must be in [0, 1]")
    if int(cfg["ratio_window"]) <= 0:
        raise ValueError("ratio_window must be positive")
    if int(cfg["amount"]) <= 0:
        raise ValueError("amount must be positive")
    contract = str(cfg.get("workload_contract", TOKEN_CONTRACT)).strip()
    if contract not in SUPPORTED_CONTRACTS:
        raise ValueError(f"unsupported workload_contract: {contract}")
    if contract == STANDARD_DFA_CONTRACT and float(cfg["cross_ratio"]) != 0:
        raise ValueError(
            "standard_dfa keeps signer-owned Transfer semantics; its current SDK-sender "
            "workload supports only cross_ratio=0. Use standard_dfa_experiment for "
            "conflict-free cross-shard pressure tests."
        )


def build_cross_flags(total: int, cross_ratio: float, window: int, rng: random.Random) -> Iterable[bool]:
    remaining = total
    while remaining > 0:
        size = min(window, remaining)
        cross_count = int(size * cross_ratio + 0.5)
        if cross_ratio > 0 and cross_count == 0:
            cross_count = 1
        cross_count = min(cross_count, size)
        flags = [True] * cross_count + [False] * (size - cross_count)
        rng.shuffle(flags)
        yield from flags
        remaining -= size


def build_transactions(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rng = random.Random(int(cfg["seed"]))
    shard_count = int(cfg["business_shards"])
    max_account_nonce = int(cfg.get("max_account_nonce", 4096))
    amount = int(cfg["amount"])
    txs: List[Dict[str, Any]] = []
    total = int(cfg["tx_count"])
    contract = str(cfg.get("workload_contract", TOKEN_CONTRACT)).strip()
    method = workload_method(contract)
    derive_account = derive_virtual_account if contract == TOKEN_CONTRACT else derive_dfa_address
    used_accounts = set()

    for seq, is_cross in enumerate(
        build_cross_flags(total, float(cfg["cross_ratio"]), int(cfg["ratio_window"]), rng),
        start=1,
    ):
        start_idx = (seq - 1) % shard_count
        round_idx = (seq - 1) // shard_count
        if is_cross and shard_count > 1:
            from_idx = start_idx
            to_idx = (from_idx + 1 + (round_idx % (shard_count - 1))) % shard_count
        else:
            from_idx = start_idx
            to_idx = from_idx
        from_virtual_index = seq
        to_virtual_index = seq + total
        from_account = derive_account(from_virtual_index, from_idx, shard_count, max_account_nonce)
        to_account = derive_account(to_virtual_index, to_idx, shard_count, max_account_nonce)
        if from_account == to_account or from_account in used_accounts or to_account in used_accounts:
            raise RuntimeError(f"account collision detected at transaction {seq}")
        used_accounts.add(from_account)
        used_accounts.add(to_account)

        txs.append(
            {
                "seq": seq,
                "workload_tx_id": f"bench_tx_{seq:012d}",
                "from_virtual_index": from_virtual_index,
                "to_virtual_index": to_virtual_index,
                "contract": contract,
                "method": method,
                "from": from_account,
                "to": to_account,
                "amount": amount,
                "from_shard": shard_name(from_idx),
                "to_shard": shard_name(to_idx),
                "tx_type": "cross" if from_idx != to_idx else "intra",
            }
        )
    return txs


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
            count += 1
    return count


def main() -> int:
    args = parse_args()
    root = repo_root()
    cfg = load_config(args.config)
    cfg.pop("accounts_per_shard", None)

    if args.business_shards > 0:
        cfg["business_shards"] = args.business_shards
    if args.tx_count > 0:
        cfg["tx_count"] = args.tx_count
    if args.cross_ratio >= 0:
        cfg["cross_ratio"] = normalize_ratio(args.cross_ratio)
    else:
        cfg["cross_ratio"] = normalize_ratio(float(cfg.get("cross_ratio", 0.05)))
    if args.amount > 0:
        cfg["amount"] = args.amount
    if args.seed > 0:
        cfg["seed"] = args.seed

    if args.interactive:
        cfg["business_shards"] = prompt_int("请输入业务分片数量", int(cfg["business_shards"]))
        cfg["cross_ratio"] = prompt_ratio(float(cfg["cross_ratio"]))
        cfg["tx_count"] = prompt_int("请输入交易数量", int(cfg["tx_count"]))

    cfg.setdefault("ratio_window", 1000)
    cfg.setdefault("max_account_nonce", 4096)
    cfg.setdefault("amount", 1)
    cfg.setdefault("seed", 20260518)
    cfg.setdefault("workload_contract", TOKEN_CONTRACT)
    validate_config(cfg)

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else work_dir(cfg, root) / "workload"
    if not output_dir.is_absolute():
        output_dir = root / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    shard_count = int(cfg["business_shards"])
    txs = build_transactions(cfg)

    used_accounts: Dict[str, Dict[str, Any]] = {}
    for tx in txs:
        for account_field, shard_field, index_field, role in (
            ("from", "from_shard", "from_virtual_index", "from"),
            ("to", "to_shard", "to_virtual_index", "to"),
        ):
            account = str(tx[account_field])
            if account not in used_accounts:
                used_accounts[account] = {
                    "account": account,
                    "virtual_index": tx[index_field],
                    "role": role,
                    "shard": tx[shard_field],
                    "used_index": len(used_accounts),
                    "initial_balance": (
                        "virtual_default_balance"
                        if cfg["workload_contract"] == TOKEN_CONTRACT
                        else "lazy_experiment_balance"
                        if cfg["workload_contract"] == EXPERIMENT_DFA_CONTRACT
                        else "sdk_sender_balance"
                    ),
                }
    account_rows = list(used_accounts.values())

    dump_json(output_dir / "config.used.json", {k: v for k, v in cfg.items() if not k.startswith("_")})
    dump_json(output_dir / "accounts.json", account_rows)
    write_jsonl(output_dir / "transactions_all.jsonl", txs)

    cross = sum(1 for tx in txs if tx["tx_type"] == "cross")
    conflict_free = cfg["workload_contract"] != STANDARD_DFA_CONTRACT
    summary = {
        "host": socket.gethostname(),
        "contract": cfg["workload_contract"],
        "method": workload_method(str(cfg["workload_contract"])),
        "business_shards": shard_count,
        "account_count": len(account_rows),
        "conflict_free": conflict_free,
        "conflict_scope": (
            "no overlapping contract balance keys"
            if conflict_free
            else "transactions on each shard share the SDK sender balance key"
        ),
        "address_model": (
            "deterministic virtual string"
            if cfg["workload_contract"] == TOKEN_CONTRACT
            else "40-character hexadecimal ChainMaker address format"
        ),
        "account_pairing": "from_virtual_index=i,to_virtual_index=i+tx_count",
        "accounts_needed_for_conflict_free": len(txs) * 2,
        "total": len(txs),
        "amount": int(cfg["amount"]),
        "cross_ratio_config": float(cfg["cross_ratio"]),
        "cross": cross,
        "intra": len(txs) - cross,
        "cross_ratio_actual": (cross / len(txs)) if txs else 0,
        "accounts_file": str(output_dir / "accounts.json"),
    }
    dump_json(output_dir / "summary.json", summary)

    print(f"Config: {cfg['_config_path']}")
    print(f"Output: {output_dir}")
    print(f"Contract: {summary['contract']}.{summary['method']}")
    print(f"Business shards: {summary['business_shards']}")
    print(f"Used accounts: {summary['account_count']}")
    print(f"Conflict-free: {summary['conflict_free']} pairing={summary['account_pairing']}")
    print(f"Transactions: {summary['total']}")
    print(f"Cross: {summary['cross']}")
    print(f"Intra: {summary['intra']}")
    print(f"Cross ratio actual: {summary['cross_ratio_actual']:.6f}")
    print("Dataset: transactions_all.jsonl")

    for tx in txs[: max(0, int(args.print_sample))]:
        print(json.dumps(tx, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
