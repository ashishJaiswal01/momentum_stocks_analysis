# Datadownload & Aggregate

Downloads the latest NSE COM-UDiFF Common Bhavcopy, enriches Nifty 500 stocks
with momentum metrics, and runs two independent screening strategies over a
shared web UI (two tabs, one per strategy, each with its own SQLite store):

- **Strategy 1** — the original 3-Pillar scan (lifetime ATH price, ATH PAT,
  52-week benchmark/sector outperformance). See section 2.
- **Strategy 2** — a stricter institutional screen with macro/liquidity gates,
  52-week-high + trend, earnings quality/cash-flow/ROE, and 12M-1M dual
  relative strength. See section 4. Fully independent of Strategy 1 - no
  code, data, or UI is shared between them beyond a handful of pure,
  stateless helper functions.

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
is dropped from the output entirely, regardless of whether it was already
being tracked — it simply won't appear in that scan's rows (the web app's
"dropped out of the screen" banner is what surfaces that on import). Also
computes `Relative_Alpha_Pct` (stock return minus benchmark return) and
`Target_Allocation_Pct` (`1.2% ÷ downside risk to the 200 EMA`, i.e. smaller
stop-loss distance → larger suggested position).

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

Each import is compared against every ticker's very *first* ever import (by
import timestamp, not calendar date):

- **Entry Status** — `New Entrant` if the ticker has never been imported
  before, otherwise `Existing`.
- **Original Scan Date** — the `Scan_Date` of that ticker's first-ever
  import. Frozen permanently the moment a ticker is first tracked; never
  overwritten by any later scan, no matter how many more come in. `Scan_Date`
  itself always reflects the current row's own (latest) scan date.
- **Previous Price** / **Gain/Loss %** — the *first-ever* tracked
  `Closing_Price_INR` for that ticker (not the immediately-preceding scan)
  and the percent change since then (`N/A` for new entrants or an
  unparseable price). Like Original Scan Date, `Previous_Closing_Price_INR`
  is frozen at first-import time and never overwritten by any later scan —
  `Gain_Loss_Pct` is therefore cumulative return since a ticker was first
  tracked, not scan-over-scan. These values are computed fresh at each
  import time using whatever was true then, so historical rows stay
  internally consistent as new scans arrive.
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
  right-hand (current) value is ever stored - including for `Closing_Price_INR`,
  where the left-hand side is simply discarded (Previous Price always comes
  from this ticker's own frozen first-ever import, never from a source
  file's embedded diff). A cell with no arrow is treated as unchanged.
- **`Entrant Type`** (`EXISTING`/`NEW_ENTRANT`), if present, is trusted
  directly for `Entry_Status` instead of being computed from this app's own
  import history.

`Gain_Loss_Pct` and `Suggestion` are always computed independently by this
app's own rule (never
read from the source file), regardless of which convention a file uses.

## 4. Momentum Strategy 2: Institutional Momentum Screening & Execution

A separate, stricter screen - completely independent scripts, SQLite store
(`data/scan_results_v2.db`), and web UI tab. Nothing here touches Strategy
1's code, data, or behavior.

```bash
cd "/Users/ashish.jaiswal/Projects/Datadownload&Aggregate"
./.venv/bin/python3 enrich_momentum_metrics_v2.py --date 2026-08-21
./.venv/bin/python3 run_momentum_strategy2_scan.py --date 2026-08-21
```

**Enrichment** (`enrich_momentum_metrics_v2.py`) computes, per stock, from the
*same* single Yahoo Finance price-history fetch and *same* single Screener.in
page fetch already made per stock in Strategy 1's enrichment (no extra
network calls, no extra rate-limit risk): EMA50/EMA200, rolling 52-week high,
ATR14, 12-month-minus-1-month momentum, 90-day average daily traded value,
Market Cap, TTM EPS, 3-year peak EPS and YoY growth, latest annual CFO and
Net Profit, 3-year average ROE, and Debt-to-Equity - plus the same metrics
for the Nifty 500 benchmark and each sector index (SMA50/SMA200 for the
benchmark's own macro-regime check).

**Screening** (`run_momentum_strategy2_scan.py`):

- **Macro regime gate** — Nifty 500 price above its 200-day SMA, with the
  50-day SMA at or above the 200-day SMA. If closed, no *new* Tier-1 (BUY)
  entries are allowed this scan.
- **Liquidity gate** — 90-day ADTV ≥ ₹5 Cr and Market Cap ≥ ₹2,000 Cr. Failing
  this drops a stock entirely, regardless of prior tracking.
- **Pillar 1 (52W-High + Trend)** — close within 10% of the rolling 52-week
  high, and close > EMA50 > EMA200.
- **Pillar 2 (Quality/Cash-Flow)** — (TTM EPS within 5% of its 3-year peak OR
  ≥15% YoY EPS growth) AND (latest annual CFO ≥ latest annual Net Profit) AND
  (3-year avg ROE ≥ 12%, and Debt/Equity < 1.5x for non-Financial-Services
  stocks).
- **Pillar 3 (Dual Relative Strength)** — 12M-1M momentum beats both the
  Nifty 500 and (where available) the stock's sector index over the same
  window.

Tiering (deliberately reuses Strategy 1's status vocabulary so the identical
web UI works unmodified): all 3 pillars + both gates → `SUPER_PERFORMER`
(Tier 1, equal-weighted 5-7% position sized by how many Tier-1 names this
scan found, capped at 20 holdings); Pillar 1 + Pillar 2 only (Pillar 3 may
soften, or the macro gate is closed) → `PERFORMER` (Tier 2, hold); anything
else - Pillar 1 broken, the cash-flow test failed, or the liquidity gate no
longer passed - is dropped from the output entirely, regardless of whether
it was tracked in a previous scan. Suggested stop-loss is
`max(EMA50, close - 3×ATR14)`.

Note: since the app's schema/column names are shared for UI compatibility,
`Lifetime_ATH_Price` holds the **52-week high** here (not a lifetime high),
`Stock_52W_Return_Pct`/`Nifty500_52W_Return_Pct`/`Sector_52W_Return_Pct` hold
**12M-1M momentum** (not plain 52-week return), and `Suggested_200_EMA_SL`
holds the **ATR-based stop** described above - see the docstring in
`run_momentum_strategy2_scan.py` for the full mapping.

The web UI's **Momentum Strategy 2** tab is a complete clone of Strategy 1's
UI (same filters, drag-to-reorder columns, export, append-only persistence,
scan-over-scan comparison, AI commentary) pointed at its own `/api/v2/*`
endpoints and `data/scan_results_v2.db` - including its own date dropdown,
"Run Strategy 2 Scan" button, and a description panel summarizing the
strategy above.

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
  numbers rather than whatever's already on disk. The same caveat applies to
  Strategy 2's enrichment.
- **Strategy 2's CFO≥PAT check compares latest-annual figures, not trailing
  twelve months** — Screener.in's free tier only publishes Cash Flow from
  Operations annually, so it's compared against the latest annual Net Profit
  (not the rolling TTM PAT used elsewhere), for an apples-to-apples annual
  comparison.
- **Strategy 2's Pillar 2 (ROE, Debt-to-Equity) is conservatively FAIL when
  data is missing**, which happens for some stocks depending on how their
  Screener.in page happens to be formatted (not every company publishes a
  "Return on Equity" compounded-growth table in the exact parsed shape) -
  this is a real, expected gap in Screener.in's free-tier data, not a bug.
