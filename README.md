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
./.venv/bin/pip install yfinance pandas lxml requests flask openai python-dotenv
chmod +x run_pipeline.sh run_webapp.sh
cp .env.example .env   # then edit .env and add your own OPENAI_API_KEY
```

`.env` is gitignored — your API key never gets committed. Everything works
without it except the AI commentary column (see step 2 below), which is
simply left blank if no key is configured.

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

## 2. 3-Pillar screening engine

```bash
cd "/Users/ashish.jaiswal/Projects/Datadownload&Aggregate"
./.venv/bin/python3 run_3pillar_scan.py            # scans the latest bhavcopy date
./.venv/bin/python3 run_3pillar_scan.py --date 2026-08-14
```

Classifies every Nifty 500 stock in a market date's `enrich_momentum_metrics.py`
output against three non-negotiable criteria:

- **Pillar 1 (ATH Price)** — within 2% of its lifetime-high price
  (`Dist_From_ATH_Pct >= -2.00%`).
- **Pillar 2 (ATH PAT)** — its latest TTM net profit equals its own
  historical-max TTM PAT (i.e. profits are currently at a record).
- **Pillar 3 (Outperformance)** — trailing 52-week return beats both the
  Nifty 500 benchmark and (where a sector index is available) its sector.

3/3 pillars → `SUPER_PERFORMER`, 2/3 → `PERFORMER`. A stock meeting ≤1 pillar
is dropped from the output entirely *unless* it was already being tracked in
`data/scan_results.db` (the web app's store), in which case it's kept one
more scan with `Status=EXIT_SELL` so the exit is visible instead of the stock
just silently disappearing. Also computes `Relative_Alpha_Pct` (stock return
minus benchmark return) and `Target_Allocation_Pct` (`1.2% ÷ downside risk to
the 200 EMA`, i.e. smaller stop-loss distance → larger suggested position).

Output: `data/bhavcopy/<date>/enriched/3pillar_scan.csv` (flat, no arrow
notation — `Entry_Status`/`Gain_Loss_Pct`/`Suggestion` are left for the web
app to compute at import time from its own history, same as any other
imported file) and `3pillar_scan_meta.json` (counts by status).

Each stock that makes the final cut also gets an `AI_Commentary` column: a
1-2 sentence, factual, plain-English rationale generated via the OpenAI API
(model configurable via `OPENAI_MODEL` in `.env`, default `gpt-4o-mini`) from
the already-computed pillar results. **This is purely narration, not
analysis** — the prompt explicitly forbids introducing new numbers or
recommendations, so it can never override or contradict the deterministic
math above. Pass `--skip-ai-commentary` for a faster run, or just leave
`OPENAI_API_KEY` unset in `.env` — either way the column is simply blank.

## 3. Scan results data browser (web UI)

```bash
cd "/Users/ashish.jaiswal/Projects/Datadownload&Aggregate"
./run_webapp.sh
```

Then open **http://127.0.0.1:5057** in your browser (opening `webapp/index.html`
directly as a `file://` path will not work — there's no backend behind it that
way, and the page will show a warning if you try).

**Run 3-Pillar Scan** (top of the page) lets you pick any date from
`data/bhavcopy/` (dropdown, populated from folders that have enrichment
output; latest pre-selected) and run the screening engine above directly from
the UI — its output is appended into the store the same way a manual CSV
import would be, no separate upload step needed.

Or import a 3-Pillar scan CSV manually (19 columns: `Scan_Date`,
`Ticker_Symbol`, `Company_Name`, `Sector_Index`, pillar PASS/FAIL statuses,
returns, TTM PAT, etc. — see `sample_data/sample_scan_2026-08-14.csv` for an
example), then:

- **Filter** by date range, sector, status, min pillars met, entry status,
  suggestion, or ticker/company search.
- **Reorder columns** by dragging a header left or right; the order is saved
  in the browser (`localStorage`) and persists across reloads. "Reset Columns"
  restores the default order.
- **Export** the currently filtered view back out as CSV.
- **Persistence is append-only** — every import adds its rows to a local
  SQLite store (`data/scan_results.db`) without touching existing rows, even
  if a row's `Scan_Date`/`Ticker_Symbol` was already imported before. Re-importing
  the same file twice results in two copies, distinguishable by the
  `Imported At` timestamp column.
- **Clear All Data** wipes the store (confirmation required).

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

#### Files that already carry their own comparison

Some source files pre-diff two scans themselves rather than leaving it to
this app. Two conventions are recognized automatically:

- **`"previous -> current"` cells** — any cell can encode a change this way
  (e.g. `"3672.80 -> 3706.40"`, `"PASS -> FAIL"`, `"3 -> 2"`); only the
  right-hand (current) value is stored for that field, except
  `Closing_Price_INR`, where the left-hand value seeds `Previous_Closing_Price_INR`
  directly instead of looking it up from prior imports. A cell with no arrow
  is treated as unchanged and used as-is.
- **`Entrant Type`** (`EXISTING`/`NEW_ENTRANT`) and **`% Gain/Loss`** columns,
  if present, are trusted directly for `Entry_Status`/`Gain_Loss_Pct` instead
  of being computed from this app's own import history.

`Suggestion` is always computed independently by this app's own rule (never
read from the source file), regardless of which convention a file uses.

## Known limitations

- **Sector-index 52-week return** is blank for ~30% of stocks — Yahoo Finance
  doesn't track a usable index for 8 of the 20 NSE macro-sectors (Capital
  Goods, Chemicals, Construction Materials, Consumer Durables, Diversified,
  Power, Telecommunication, Textiles).
- **TTM PAT ATH** is capped at whatever quarters Screener.in's free tier
  exposes (~13 quarters, ~3.25 years) — it's the max TTM within that window,
  not a true multi-decade lifetime ATH.
- **`Lifetime_ATH_Price`, 52-week returns, and 200 EMA are always "as of when
  enrich_momentum_metrics.py was run,"** not "as of the market date being
  scanned." Only `Closing_Price_INR` (from the bhavcopy) is truly pinned to
  that date. Re-running the 3-pillar scan for an *older* date after time has
  passed can therefore shift Pillar 1/3 results (e.g. a stock's ATH may have
  risen since, making it look further from its high than it actually was on
  that date). For the *latest* date, scanned promptly, this drift is minimal.
  Run `./run_pipeline.sh` again before scanning if you want genuinely current
  numbers rather than whatever's already on disk.
