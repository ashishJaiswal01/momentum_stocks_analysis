#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="$SCRIPT_DIR/.venv/bin/python3"
if [ ! -x "$PYTHON" ]; then
    echo "ERROR: venv not found at $PYTHON" >&2
    exit 1
fi

echo "Starting scan data browser at http://127.0.0.1:5057"
exec "$PYTHON" webapp/app.py
