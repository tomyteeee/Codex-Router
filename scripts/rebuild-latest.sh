#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

OUT=/tmp/codex-router-latest-rebuild.txt

rm -f "$OUT"

pkill -f 'Codex Router.app' || true
sleep 2

if ! python3 scripts/patch_app.py --force --allow-untested-source >"$OUT" 2>&1; then
    echo "REBUILD FAILED: $OUT"
    exit 1
fi

open "$HOME/Applications/Codex Router.app"

echo "CODEX ROUTER REBUILD PASSED"
echo "Build log: $OUT"
