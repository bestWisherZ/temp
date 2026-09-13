#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "$ROOT/artifacts/lib/ld-linux-x86-64.so.2" --library-path "$ROOT/artifacts/lib" "$@"
