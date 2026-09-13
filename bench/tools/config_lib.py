#!/usr/bin/env python3
import json
import math
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_config(path: Union[str, Path]) -> Dict[str, Any]:
    cfg_path = Path(path).expanduser()
    if not cfg_path.is_absolute():
        cfg_path = repo_root() / cfg_path
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["_config_path"] = str(cfg_path)
    return cfg


def abs_path(root: Path, value: Union[str, Path]) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (root / path).resolve()


def work_dir(cfg: Dict[str, Any], root: Optional[Path] = None) -> Path:
    root = root or repo_root()
    return abs_path(root, str(cfg.get("work_dir", "./out")))


def servers(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = list(cfg.get("servers") or [])
    if not rows:
        raise ValueError("config.servers must not be empty")
    return rows


def server_names(cfg: Dict[str, Any]) -> List[str]:
    return [str(s["name"]) for s in servers(cfg)]


def server_by_name(cfg: Dict[str, Any], name: str) -> Dict[str, Any]:
    name = name.strip()
    for server in servers(cfg):
        if str(server.get("name", "")).strip() == name:
            return server
    raise KeyError(f"server not found in config.servers: {name}")


def detect_server_name(cfg: Dict[str, Any]) -> str:
    host = socket.gethostname().split(".")[0]
    names = server_names(cfg)
    if host in names:
        return host
    coordinator = str(cfg.get("coordinator_server", "")).strip()
    if coordinator:
        return coordinator
    return names[0]


def ssh_target(cfg: Dict[str, Any], server: Dict[str, Any]) -> str:
    user = str(cfg.get("ssh_user", "")).strip()
    host = str(server.get("host", "")).strip()
    if not host:
        raise ValueError(f"server {server.get('name')} host is empty")
    return f"{user}@{host}" if user else host


def remote_dir(server: Dict[str, Any]) -> str:
    value = str(server.get("remote_dir", "")).strip()
    if not value:
        raise ValueError(f"server {server.get('name')} remote_dir is empty")
    return value


def dump_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def ceil_div(value: int, divisor: int) -> int:
    return int(math.ceil(value / divisor))
