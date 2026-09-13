#!/usr/bin/env bash
set -euo pipefail
ROOT=/root/du_sharding/temp
exec bash "$ROOT/scripts/portable_exec.sh" "$ROOT/artifacts/bin/chainmaker-cryptogen" "$@"
