#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="$SCRIPT_DIR/.venv/bin/python3"
OUTPUT_DIR="${OUTPUT_DIR:-./data}"

if [ ! -x "$PYTHON" ]; then
    echo "ERROR: venv not found at $PYTHON" >&2
    echo "Set it up with: python3 -m venv .venv && ./.venv/bin/pip install yfinance pandas lxml requests" >&2
    exit 1
fi

echo ">>> Step 1/2: Downloading NSE COM-UDiFF bhavcopy"
"$PYTHON" nse_udiff_bhavcopy.py --output "$OUTPUT_DIR" "$@"

echo
echo ">>> Step 2/2: Enriching Nifty 500 momentum metrics"
"$PYTHON" enrich_momentum_metrics.py --output "$OUTPUT_DIR"

echo
echo ">>> Pipeline complete."
