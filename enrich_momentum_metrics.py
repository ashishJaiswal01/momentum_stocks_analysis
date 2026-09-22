#!/usr/bin/env python3
"""Momentum-scan enrichment for the NSE Nifty 500 universe.

Consumes the output of nse_udiff_bhavcopy.py (a Market Data Date + list of
traded symbols) and computes, per Nifty 500 constituent:

  1. Lifetime ATH price       - Yahoo Finance, max High over full history
  2. 52-week return           - Yahoo Finance, (close_t - close_t-252) / close_t-252
  3. 200 EMA                  - Yahoo Finance, EWM(span=200) on daily close
  4. Nifty 500 52-week return - Yahoo Finance ^CRSLDX, same formula as #2
  5. Sector-index 52-week return - Yahoo Finance sector index, same formula
  6. TTM PAT ATH               - Screener.in quarterly results, max rolling
                                  4-quarter sum of Net Profit
  7. 50/200 SMA, 14-period RSI, 20-day volume ratio - Yahoo Finance, same
                                  already-fetched OHLCV history as #1-3
  8. Revenue growth YoY, ROCE  - Screener.in annual P&L / top-ratios panel,
                                  same single page fetch as #6 (no extra
                                  network calls)
  9. ROCE industry median      - cross-sectional median of #8 across all
                                  stocks in the same NSE Industry classification

Metrics 7-9 feed the Momentum Score (Price Momentum + Fundamental Momentum +
Relative Strength) computed by run_3pillar_scan.py - see that module's
docstring for the scoring formula.

Requires the .venv set up alongside this script (yfinance, pandas, lxml,
requests):
    ./.venv/bin/python3 enrich_momentum_metrics.py
"""

import argparse
import csv
import io
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

IST = ZoneInfo("Asia/Kolkata")
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
NIFTY500_LIST_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
NIFTY500_BENCHMARK_TICKER = "^CRSLDX"
TRADING_DAYS_52W = 252
YF_CHUNK_SIZE = 150
# Screener.in has no public API and pushes back hard on bursts: a first pass
# at 5 workers / 0.3s pacing drew a mix of HTTP 429s and outright connection
# refusals within seconds and left the host unreachable from this network for
# a while afterwards. These defaults are deliberately conservative; raise
# --workers/lower --request-delay at your own risk of a repeat block.
DEFAULT_WORKERS = 1
DEFAULT_REQUEST_DELAY = 3.0
SCREENER_MAX_RETRIES = 3
SCREENER_BACKOFF_BASE = 5.0
CIRCUIT_BREAKER_THRESHOLD = 6

# Best-effort mapping from the NSE macro-sector classification used in the
# Nifty 500 constituent list ("Industry" column) to a Yahoo Finance NSE
# sectoral-index ticker. Yahoo does not track every NSE sectoral index, so
# coverage is partial by design - sectors with no reliable ticker are left
# unmapped and that stock's sector-return metric is reported blank.
SECTOR_INDEX_MAP = {
    "Automobile and Auto Components": "^CNXAUTO",
    "Financial Services": "NIFTY_FIN_SERVICE.NS",  # ^CNXFIN has no usable history on Yahoo
    "Fast Moving Consumer Goods": "^CNXFMCG",
    "Information Technology": "^CNXIT",
    "Media Entertainment & Publication": "^CNXMEDIA",
    "Metals & Mining": "^CNXMETAL",
    "Healthcare": "^CNXPHARMA",       # proxy: no distinct Yahoo Healthcare index
    "Realty": "^CNXREALTY",
    "Oil Gas & Consumable Fuels": "^CNXENERGY",
    "Services": "^CNXSERVICE",
    "Consumer Services": "^CNXCONSUM",  # proxy: Nifty Consumption index
    "Construction": "^CNXINFRA",        # proxy: Nifty Infra index
}


@dataclass
class PriceMetrics:
    ath: float | None = None
    ema200: float | None = None
    return_52w: float | None = None
    sma50: float | None = None
    sma200: float | None = None
    rsi14: float | None = None
    volume_ratio_20d: float | None = None
    rows: int = 0
    error: str = ""


@dataclass
class TtmPatResult:
    ath: float | None = None
    latest: float | None = None  # most recent rolling 4-quarter TTM PAT (vs. ath = historical max)
    quarters_available: int = 0
    revenue_growth_yoy_pct: float | None = None  # latest FY Sales vs prior FY, from the annual P&L view
    roce_pct: float | None = None  # Screener.in top-ratios ROCE
    source_view: str = ""
    error: str = ""
    blocked: bool = False  # rate-limited / connection-refused, vs. a legitimate miss


class CircuitBreaker:
    """Trips after N consecutive blocking-type failures so we stop hammering
    a host that has started refusing us, instead of grinding through the
    rest of the batch as further failures."""

    def __init__(self, threshold: int):
        self.threshold = threshold
        self._consecutive = 0
        self._tripped = False
        self._lock = threading.Lock()

    def record(self, blocked: bool) -> None:
        with self._lock:
            if blocked:
                self._consecutive += 1
                if self._consecutive >= self.threshold:
                    self._tripped = True
            else:
                self._consecutive = 0

    def is_tripped(self) -> bool:
        with self._lock:
            return self._tripped


@dataclass
class StockRecord:
    symbol: str
    company_name: str
    industry: str
    close_price: float | None
    yahoo_ticker: str = field(init=False)
    price: PriceMetrics = field(default_factory=PriceMetrics)
    sector_ticker: str = ""
    sector_return_52w: float | None = None
    nifty500_return_52w: float | None = None
    ttm_pat: TtmPatResult = field(default_factory=TtmPatResult)
    roce_industry_median_pct: float | None = None

    def __post_init__(self):
        self.yahoo_ticker = f"{self.symbol}.NS"


# --------------------------------------------------------------------------
# Nifty 500 universe + bhavcopy input
# --------------------------------------------------------------------------

def fetch_nifty500_constituents(cache_path: Path, timeout: int, force: bool) -> list[dict]:
    if cache_path.exists() and not force:
        text = cache_path.read_text()
    else:
        req = Request(NIFTY500_LIST_URL, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8-sig")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text)
    return list(csv.DictReader(io.StringIO(text)))


def load_bhavcopy_output(output_dir: Path, market_date: str | None) -> dict:
    if market_date:
        meta_path = output_dir / "bhavcopy" / market_date / "metadata.json"
    else:
        meta_path = output_dir / "bhavcopy" / "latest.json"

    if not meta_path.exists():
        print(f"ERROR: no bhavcopy metadata found at {meta_path}", file=sys.stderr)
        print("Run nse_udiff_bhavcopy.py first to download a bhavcopy report.", file=sys.stderr)
        sys.exit(1)

    meta = json.loads(meta_path.read_text())
    csv_path = Path(meta["extracted_dir"]) / meta["csv_file"]
    if not csv_path.exists():
        print(f"ERROR: extracted bhavcopy CSV missing: {csv_path}", file=sys.stderr)
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    return {"market_data_date": meta["market_data_date"], "rows": rows}


def build_universe(bhavcopy_rows: list[dict], nifty500_rows: list[dict]) -> tuple[list[StockRecord], list[str]]:
    close_by_symbol = {
        r["TckrSymb"]: r.get("ClsPric")
        for r in bhavcopy_rows
        if r.get("Sgmt") == "CM" and r.get("SctySrs") == "EQ"
    }

    records = []
    unmatched = []
    for row in nifty500_rows:
        symbol = row["Symbol"].strip()
        close_raw = close_by_symbol.get(symbol)
        if close_raw is None:
            unmatched.append(symbol)
            continue
        try:
            close_price = float(close_raw)
        except (TypeError, ValueError):
            close_price = None
        records.append(StockRecord(
            symbol=symbol,
            company_name=row["Company Name"].strip(),
            industry=row["Industry"].strip(),
            close_price=close_price,
        ))
    return records, unmatched


# --------------------------------------------------------------------------
# Yahoo Finance: ATH / EMA200 / 52-week return (stocks + benchmark + sectors)
# --------------------------------------------------------------------------

def _rsi_14(close: pd.Series) -> float | None:
    """Standard Wilder's 14-period RSI on daily close."""
    if len(close) < 15:
        return None
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    last_gain, last_loss = avg_gain.iloc[-1], avg_loss.iloc[-1]
    if pd.isna(last_gain) or pd.isna(last_loss):
        return None
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return float(100 - (100 / (1 + rs)))


def compute_price_metrics(close: pd.Series, high: pd.Series, volume: pd.Series) -> PriceMetrics:
    close = close.dropna()
    high = high.dropna()
    volume = volume.dropna()
    if close.empty:
        return PriceMetrics(error="no price history")

    ath = float(high.max()) if not high.empty else None
    ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1])
    sma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
    rsi14 = _rsi_14(close)

    volume_ratio_20d = None
    if len(volume) >= 21:
        prior_20d_avg = volume.iloc[-21:-1].mean()
        if prior_20d_avg:
            volume_ratio_20d = float(volume.iloc[-1] / prior_20d_avg)

    return_52w = None
    if len(close) > TRADING_DAYS_52W:
        base = close.iloc[-(TRADING_DAYS_52W + 1)]
        if base:
            return_52w = float((close.iloc[-1] - base) / base)

    return PriceMetrics(
        ath=ath, ema200=ema200, return_52w=return_52w,
        sma50=sma50, sma200=sma200, rsi14=rsi14, volume_ratio_20d=volume_ratio_20d,
        rows=len(close),
    )


def fetch_all_price_metrics(tickers: list[str], timeout: int) -> dict[str, PriceMetrics]:
    results: dict[str, PriceMetrics] = {}
    unique_tickers = sorted(set(tickers))
    for i in range(0, len(unique_tickers), YF_CHUNK_SIZE):
        chunk = unique_tickers[i:i + YF_CHUNK_SIZE]
        try:
            data = yf.download(
                chunk, period="max", group_by="ticker",
                auto_adjust=False, progress=False, threads=True,
                timeout=timeout,
            )
        except Exception as e:  # yfinance raises a variety of exception types
            for t in chunk:
                results[t] = PriceMetrics(error=f"bulk download failed: {e}")
            continue

        for t in chunk:
            try:
                if len(chunk) == 1:
                    sub = data
                else:
                    sub = data[t]
                results[t] = compute_price_metrics(sub["Close"], sub["High"], sub["Volume"])
            except Exception as e:
                results[t] = PriceMetrics(error=f"no data returned: {e}")
    return results


# --------------------------------------------------------------------------
# Screener.in: TTM PAT ATH from quarterly Net Profit
# --------------------------------------------------------------------------

def _clean_number(raw: str) -> float | None:
    raw = raw.replace(",", "").replace("\xa0", "").strip()
    if raw in ("", "-", "nan", "NaN"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _fetch_html_with_retry(url: str, timeout: int, max_retries: int) -> tuple[str | None, TtmPatResult | None]:
    """Returns (html, None) on success, (None, TtmPatResult) on a final failure.
    A 404 is reported via the sentinel TtmPatResult(error="__not_found__")."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    attempt = 0
    while True:
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace"), None
        except HTTPError as e:
            if e.code == 404:
                return None, TtmPatResult(error="__not_found__")
            if e.code == 429 and attempt < max_retries:
                retry_after = e.headers.get("Retry-After") if e.headers else None
                delay = float(retry_after) if retry_after and retry_after.isdigit() else SCREENER_BACKOFF_BASE * (2 ** attempt)
                time.sleep(delay)
                attempt += 1
                continue
            return None, TtmPatResult(error=f"HTTP {e.code}", blocked=True)
        except OSError as e:
            # Covers URLError (itself an OSError subclass) plus raw socket-level
            # failures - ConnectionResetError, ConnectionRefusedError, TimeoutError -
            # that urlopen doesn't always wrap in URLError.
            if attempt < max_retries:
                time.sleep(SCREENER_BACKOFF_BASE * (2 ** attempt))
                attempt += 1
                continue
            reason = getattr(e, "reason", e)
            return None, TtmPatResult(error=f"network error: {reason}", blocked=True)


def _extract_roce(html: str) -> float | None:
    """Screener.in's top-ratios panel (same fixed-format section used for
    Market Cap etc.) - ROCE is always a current single value, not a
    historical table."""
    start = html.find('id="top-ratios"')
    if start == -1:
        return None
    section = html[start:start + 3000]
    m = re.search(r'ROCE.*?<span class="number">([\d,.\-]+)</span>', section, re.S)
    return _clean_number(m.group(1)) if m else None


def _row_values(table: pd.DataFrame, label_col: str, prefix: str, data_cols: list[str]) -> list[float]:
    labels = table[label_col].astype(str).str.replace("\xa0", " ").str.strip()
    match = labels.str.startswith(prefix)
    if not match.any():
        return []
    row = table.loc[match, data_cols].iloc[0]
    return [v for v in (_clean_number(str(x)) for x in row.tolist()) if v is not None]


def _extract_net_profit_ttm(html: str, view: str) -> TtmPatResult | None:
    """Returns a TtmPatResult on success, or None if this view has no usable
    financial tables at all (caller should fall back to the next view).
    Parses both the quarterly table (TTM PAT ATH/latest, as before) and the
    annual table (revenue growth YoY) from the same single page fetch, plus
    ROCE from the top-ratios panel."""
    date_col_re = re.compile(r"^[A-Za-z]{3} \d{4}$")
    try:
        tables = pd.read_html(io.StringIO(html))
    except ValueError:
        return None

    found_any = False
    ath = latest = None
    quarters_available = 0
    revenue_growth_yoy_pct = None

    for table in tables:
        cols = [str(c) for c in table.columns]
        if not cols:
            continue
        label_col = cols[0]
        date_cols = [c for c in cols if date_col_re.match(c)]
        if not date_cols:
            continue

        months = {c.split()[0] for c in date_cols}
        is_annual = len(months) == 1  # annual columns are all "Mar YYYY"

        if not is_annual:
            values = _row_values(table, label_col, "Net Profit", date_cols)
            if len(values) >= 4:
                # Quarter columns run oldest -> newest, so the last rolling
                # window is the most recent (current) TTM figure.
                rolling_sums = [sum(values[i:i + 4]) for i in range(len(values) - 3)]
                ath, latest = max(rolling_sums), rolling_sums[-1]
                quarters_available = len(values)
                found_any = True
        else:
            sales_vals = _row_values(table, label_col, "Sales", date_cols)
            if len(sales_vals) >= 2 and sales_vals[-2]:
                revenue_growth_yoy_pct = (sales_vals[-1] - sales_vals[-2]) / abs(sales_vals[-2]) * 100
                found_any = True

    roce_pct = _extract_roce(html)
    if roce_pct is not None:
        found_any = True

    if not found_any:
        return None
    return TtmPatResult(
        ath=ath, latest=latest, quarters_available=quarters_available,
        revenue_growth_yoy_pct=revenue_growth_yoy_pct, roce_pct=roce_pct,
        source_view=view,
    )


def fetch_quarterly_net_profit(symbol: str, timeout: int, max_retries: int = SCREENER_MAX_RETRIES) -> TtmPatResult:
    for view, url in (
        ("consolidated", f"https://www.screener.in/company/{symbol}/consolidated/"),
        ("standalone", f"https://www.screener.in/company/{symbol}/"),
    ):
        html, failure = _fetch_html_with_retry(url, timeout, max_retries)
        if failure is not None:
            if failure.error == "__not_found__":
                continue
            return failure

        result = _extract_net_profit_ttm(html, view)
        if result is not None:
            return result
        # This view rendered but had no usable financial tables at all
        # (e.g. a company with no real consolidated financials shows an
        # empty consolidated page) - fall through and try the next view.

    return TtmPatResult(error="No usable financial tables found in consolidated or standalone view")


def compute_industry_median_roce(records: list[StockRecord]) -> None:
    """Cross-sectional pass over the already-fetched ROCE values, grouped by
    the same NSE Industry classification used for sector returns - sets
    roce_industry_median_pct on every record with at least 2 ROCE values
    available in its industry (a lone stock has no peer group to compare
    against)."""
    by_industry: dict[str, list[float]] = {}
    for rec in records:
        if rec.ttm_pat.roce_pct is not None:
            by_industry.setdefault(rec.industry, []).append(rec.ttm_pat.roce_pct)

    medians = {
        industry: float(pd.Series(values).median())
        for industry, values in by_industry.items()
        if len(values) >= 2
    }
    for rec in records:
        rec.roce_industry_median_pct = medians.get(rec.industry)


def enrich_ttm_pat(records: list[StockRecord], workers: int, delay: float, timeout: int) -> None:
    breaker = CircuitBreaker(CIRCUIT_BREAKER_THRESHOLD)

    def worker(rec: StockRecord) -> tuple[str, TtmPatResult]:
        if breaker.is_tripped():
            return rec.symbol, TtmPatResult(
                error="skipped: Screener.in circuit breaker open (too many consecutive blocks)"
            )
        time.sleep(delay)
        try:
            result = fetch_quarterly_net_profit(rec.symbol, timeout)
        except Exception as e:
            # A single symbol's unexpected failure (e.g. a malformed page,
            # an exotic socket error) must not take down a 20+ minute batch.
            result = TtmPatResult(error=f"unexpected error: {e}", blocked=True)
        breaker.record(blocked=result.blocked)
        return rec.symbol, result

    by_symbol = {r.symbol: r for r in records}
    done = 0
    total = len(records)
    breaker_warned = False
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, r) for r in records]
        for fut in as_completed(futures):
            symbol, result = fut.result()
            by_symbol[symbol].ttm_pat = result
            done += 1
            if breaker.is_tripped() and not breaker_warned:
                breaker_warned = True
                print(f"  Screener.in circuit breaker OPEN after {done}/{total} "
                      f"({CIRCUIT_BREAKER_THRESHOLD} consecutive blocks) - "
                      "skipping remaining requests instead of continuing to hammer it", flush=True)
            if done % 25 == 0 or done == total:
                print(f"  Screener.in TTM PAT: {done}/{total} processed", flush=True)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output)
    execution_dt = datetime.now(IST)

    print("=" * 60)
    print("NSE Nifty 500 Momentum Metrics Enrichment")
    print("=" * 60)
    print()
    print(f"Execution Time : {execution_dt.strftime('%Y-%m-%d %H:%M:%S')} IST")

    bhavcopy = load_bhavcopy_output(output_dir, args.date)
    market_date = bhavcopy["market_data_date"]
    print(f"Market Data Date : {market_date}")
    print()

    print("Loading Nifty 500 constituent list ...")
    nifty500_rows = fetch_nifty500_constituents(
        output_dir / "reference" / "ind_nifty500list.csv", args.timeout, args.force_nifty500_refresh,
    )
    print(f"  {len(nifty500_rows)} constituents")

    records, unmatched = build_universe(bhavcopy["rows"], nifty500_rows)
    if args.limit:
        records = records[:args.limit]
    print(f"  {len(records)} matched against bhavcopy ({len(unmatched)} unmatched)")
    if unmatched:
        print(f"  Unmatched (not in {market_date} bhavcopy): {', '.join(unmatched[:15])}"
              + (" ..." if len(unmatched) > 15 else ""))
    print()

    sector_tickers = sorted({SECTOR_INDEX_MAP[r.industry] for r in records if r.industry in SECTOR_INDEX_MAP})
    all_tickers = [r.yahoo_ticker for r in records] + [NIFTY500_BENCHMARK_TICKER] + sector_tickers

    print(f"Fetching Yahoo Finance price history for {len(all_tickers)} tickers "
          f"({len(records)} stocks + 1 benchmark + {len(sector_tickers)} sector indices) ...")
    price_metrics = fetch_all_price_metrics(all_tickers, args.timeout)

    benchmark_return = price_metrics.get(NIFTY500_BENCHMARK_TICKER, PriceMetrics()).return_52w
    sector_returns = {t: price_metrics.get(t, PriceMetrics()).return_52w for t in sector_tickers}

    price_ok = 0
    for rec in records:
        rec.price = price_metrics.get(rec.yahoo_ticker, PriceMetrics(error="not fetched"))
        rec.nifty500_return_52w = benchmark_return
        rec.sector_ticker = SECTOR_INDEX_MAP.get(rec.industry, "")
        rec.sector_return_52w = sector_returns.get(rec.sector_ticker)
        if not rec.price.error:
            price_ok += 1
    print(f"  {price_ok}/{len(records)} stocks OK")
    print()

    if args.skip_ttm_pat:
        print("Skipping TTM PAT ATH (Screener.in) - --skip-ttm-pat set")
    else:
        print(f"Fetching TTM PAT ATH from Screener.in ({args.workers} workers, "
              f"{args.request_delay}s pacing) ...")
        enrich_ttm_pat(records, args.workers, args.request_delay, args.timeout)
        ttm_ok = sum(1 for r in records if not r.ttm_pat.error)
        print(f"  {ttm_ok}/{len(records)} stocks OK")
    print()

    compute_industry_median_roce(records)

    enriched_dir = output_dir / "bhavcopy" / market_date / "enriched"
    enriched_dir.mkdir(parents=True, exist_ok=True)
    csv_path = enriched_dir / "momentum_metrics.csv"
    json_path = enriched_dir / "momentum_metrics.json"

    fieldnames = [
        "symbol", "company_name", "industry", "market_data_date", "close_price",
        "lifetime_ath_price", "return_52w_pct", "ema_200", "sma_50", "sma_200",
        "rsi_14", "volume_ratio_20d",
        "nifty500_return_52w_pct", "sector_index_ticker", "sector_return_52w_pct",
        "ttm_pat_ath_cr", "ttm_pat_latest_cr", "ttm_pat_ath_quarters_available",
        "revenue_growth_yoy_pct", "roce_pct", "roce_industry_median_pct",
        "ttm_pat_source_view", "notes",
    ]
    json_rows = []
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            notes = "; ".join(n for n in (rec.price.error, rec.ttm_pat.error) if n)
            row = {
                "symbol": rec.symbol,
                "company_name": rec.company_name,
                "industry": rec.industry,
                "market_data_date": market_date,
                "close_price": rec.close_price,
                "lifetime_ath_price": rec.price.ath,
                "return_52w_pct": round(rec.price.return_52w * 100, 4) if rec.price.return_52w is not None else None,
                "ema_200": round(rec.price.ema200, 4) if rec.price.ema200 is not None else None,
                "sma_50": round(rec.price.sma50, 4) if rec.price.sma50 is not None else None,
                "sma_200": round(rec.price.sma200, 4) if rec.price.sma200 is not None else None,
                "rsi_14": round(rec.price.rsi14, 2) if rec.price.rsi14 is not None else None,
                "volume_ratio_20d": round(rec.price.volume_ratio_20d, 3) if rec.price.volume_ratio_20d is not None else None,
                "nifty500_return_52w_pct": round(rec.nifty500_return_52w * 100, 4) if rec.nifty500_return_52w is not None else None,
                "sector_index_ticker": rec.sector_ticker or None,
                "sector_return_52w_pct": round(rec.sector_return_52w * 100, 4) if rec.sector_return_52w is not None else None,
                "ttm_pat_ath_cr": rec.ttm_pat.ath,
                "ttm_pat_latest_cr": rec.ttm_pat.latest,
                "ttm_pat_ath_quarters_available": rec.ttm_pat.quarters_available or None,
                "revenue_growth_yoy_pct": round(rec.ttm_pat.revenue_growth_yoy_pct, 2) if rec.ttm_pat.revenue_growth_yoy_pct is not None else None,
                "roce_pct": rec.ttm_pat.roce_pct,
                "roce_industry_median_pct": rec.roce_industry_median_pct,
                "ttm_pat_source_view": rec.ttm_pat.source_view or None,
                "notes": notes,
            }
            writer.writerow(row)
            json_rows.append(row)

    json_path.write_text(json.dumps({
        "execution_date": execution_dt.isoformat(),
        "market_data_date": market_date,
        "record_count": len(records),
        "unmatched_symbols": unmatched,
        "rows": json_rows,
    }, indent=2))

    print("=" * 60)
    print(f"Market Data Date : {market_date}")
    print(f"Records          : {len(records)}")
    print(f"CSV              : {csv_path}")
    print(f"JSON             : {json_path}")
    print("=" * 60)
    return 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich the Nifty 500 universe from an nse_udiff_bhavcopy.py run "
                    "with ATH, 52-week return, 200 EMA, benchmark/sector returns, and TTM PAT ATH.",
    )
    parser.add_argument("--output", type=str, default="data",
                         help="Base directory shared with nse_udiff_bhavcopy.py (default: data)")
    parser.add_argument("--date", type=str, default=None,
                         help="Market data date (YYYY-MM-DD) to enrich. Defaults to the "
                              "downloader's latest.json selection.")
    parser.add_argument("--skip-ttm-pat", action="store_true",
                         help="Skip the Screener.in TTM PAT ATH scrape (faster, Yahoo-only run)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                         help=f"Concurrent Screener.in workers (default: {DEFAULT_WORKERS}, deliberately "
                              "conservative - Screener.in has rate-limited/blocked this tool at higher "
                              "concurrency before; raise at your own risk)")
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY,
                         help=f"Per-request pacing delay in seconds for Screener.in (default: {DEFAULT_REQUEST_DELAY})")
    parser.add_argument("--timeout", type=int, default=30,
                         help="HTTP request timeout in seconds (default: 30)")
    parser.add_argument("--force-nifty500-refresh", action="store_true",
                         help="Re-download the Nifty 500 constituent list instead of using the cached copy")
    parser.add_argument("--limit", type=int, default=None,
                         help="Only process the first N matched symbols (for testing)")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
