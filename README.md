# Datadownload & Aggregate

Downloads the latest NSE COM-UDiFF Common Bhavcopy, enriches Nifty 500 stocks
with momentum metrics (lifetime ATH, 52-week return, 200 EMA, Nifty 500 /
sector 52-week returns, and TTM PAT ATH), and browses the resulting scan
results in a local web UI.

## First-time setup

Requires Python 3 and a local virtualenv:

```bash
cd "/Users/ashish.jaiswal/Projects/Datadownload&Aggregate"
python3 -m venv .venv
./.venv/bin/pip install yfinance pandas lxml requests flask
chmod +x run_pipeline.sh run_webapp.sh
```

## 1. Download + enrich pipeline

```bash
cd "/Users/ashish.jaiswal/Projects/Datadownload&Aggregate"
./run_pipeline.sh
```

The pipeline:

1. Downloads the latest available NSE bhavcopy (skips weekends; walks back if
   a trading day is missing).
2. Computes momentum metrics for the Nifty 500 universe (Yahoo Finance for
   price-derived metrics, Screener.in for TTM PAT ATH).

Output is written under `data/`:

```
data/bhavcopy/<YYYY-MM-DD>/
  raw/            ZIP from NSE
  extracted/      CSV bhavcopy
  enriched/       momentum_metrics.csv and momentum_metrics.json
  metadata.json
data/bhavcopy/latest.json
```

### Optional flags

Arguments after `./run_pipeline.sh` are passed to the bhavcopy downloader:

```bash
./run_pipeline.sh --date 2026-08-14
./run_pipeline.sh --force
```

To write output somewhere other than `data/`:

```bash
OUTPUT_DIR=./download ./run_pipeline.sh
```

A full run takes ~20-30 minutes, mostly a deliberately slow Screener.in scrape
(1 worker, 3s pacing) — Screener.in briefly rate-limited/blocked this tool at
higher concurrency, so raise `--workers` / lower `--request-delay` in
`enrich_momentum_metrics.py` at your own risk. Pass `--skip-ttm-pat` to that
script directly for a fast, Yahoo-only run.

## 2. Scan results data browser (web UI)

```bash
cd "/Users/ashish.jaiswal/Projects/Datadownload&Aggregate"
./run_webapp.sh
```

Then open **http://127.0.0.1:5057** in your browser (opening `webapp/index.html`
directly as a `file://` path will not work — there's no backend behind it that
way, and the page will show a warning if you try).

Import a 3-Pillar scan CSV (19 columns: `Scan_Date`, `Ticker_Symbol`,
`Company_Name`, `Sector_Index`, pillar PASS/FAIL statuses, returns, TTM PAT,
etc. — see `sample_data/sample_scan_2026-08-14.csv` for an example), then:

- **Filter** by date range, sector, status, min pillars met, entry status,
  suggestion, or ticker/company search.
- **Export** the currently filtered view back out as CSV.
- **Persistence is append-only** — every import adds its rows to a local
  SQLite store (`data/scan_results.db`) without touching existing rows, even
  if a row's `Scan_Date`/`Ticker_Symbol` was already imported before. Re-importing
  the same file twice results in two copies, distinguishable by the
  `Imported At` timestamp column.
- **Clear All Data** wipes the store (confirmation required).

Note: this UI consumes the output of a "3-Pillar scan" scoring stage
(PASS/FAIL pillars, `SUPER_PERFORMER` status, suggested stop-loss/allocation)
that doesn't exist as a script in this project yet — `enrich_momentum_metrics.py`
produces the raw inputs (ATH, returns, EMA, TTM PAT) but not that
classification layer.

### Scan-over-scan comparison

Each import is compared against every ticker's most recent *prior* import
(by import timestamp, not calendar date — so re-importing the same data
twice correctly shows ~0% change, and gaps/duplicate dates don't break it):

- **Entry Status** — `New Entrant` if the ticker has never been imported
  before, otherwise `Existing`.
- **Previous Price** / **Gain/Loss %** — the prior import's
  `Closing_Price_INR` and the percent change since then (`N/A` for new
  entrants or an unparseable price). These are frozen at import time, not
  recomputed later, so historical rows stay accurate as new scans arrive.
- **Suggestion** (`ACCUMULATE` / `HOLD` / `EXIT`) — a rule-based read of the
  existing 3-Pillar fields only (`Pillars_Met_Count`, the individual
  Pillar 1/2/3 PASS/FAIL status, `Relative_Alpha_Pct`). **This app has no
  RSI, volume, or trend-strength data** — the suggestion is not, and cannot
  be, based on technical indicators beyond what the 3-Pillar scan already
  computes. Roughly: `ACCUMULATE` when all three pillars still pass and
  alpha isn't negative; `EXIT` when the ATH-price pillar fails, or
  outperformance fails alongside negative alpha, or at most one pillar is
  met; `HOLD` otherwise. See `compute_suggestion()` in `webapp/app.py` for
  the exact rule.
- **Stocks that drop out of the screen** (present in the previous import,
  absent from the new one) aren't deleted — their historical rows remain —
  but the UI shows a banner listing them right after import.
- Within a single imported file, duplicate tickers are deduplicated (last
  row wins) and ticker symbols are case/whitespace-normalized before any
  comparison.

## Known limitations

- **Sector-index 52-week return** is blank for ~30% of stocks — Yahoo Finance
  doesn't track a usable index for 8 of the 20 NSE macro-sectors (Capital
  Goods, Chemicals, Construction Materials, Consumer Durables, Diversified,
  Power, Telecommunication, Textiles).
- **TTM PAT ATH** is capped at whatever quarters Screener.in's free tier
  exposes (~13 quarters, ~3.25 years) — it's the max TTM within that window,
  not a true multi-decade lifetime ATH.
