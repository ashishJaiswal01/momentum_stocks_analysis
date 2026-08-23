#!/usr/bin/env python3
"""Momentum Strategy-2 enrichment for the NSE Nifty 500 universe.

Fully independent of enrich_momentum_metrics.py (Strategy 1) - separate
script, separate output files, nothing shared or imported between them.

Computes, per stock (all from a single already-fetched Yahoo Finance OHLCV
history + a single already-fetched Screener.in company page - no extra
network calls beyond what Strategy 1 already makes per stock):

  - EMA50, EMA200                    (Yahoo Finance)
  - Rolling 52-week high             (Yahoo Finance, max High, trailing ~252d)
  - ATR14                            (Yahoo Finance, 14-period Average True Range)
  - 12M-1M momentum                  (Yahoo Finance, return from ~252d ago to
                                       ~21d ago - skips the most recent month)
  - 90-day ADTV in Rs Cr             (Yahoo Finance, mean(Volume*Close) / 1e7)
  - Market Cap in Rs Cr              (Screener.in top-ratios)
  - TTM EPS                          (Screener.in quarterly P&L, rolling 4-qtr sum)
  - 3-year peak EPS + YoY growth %   (Screener.in annual P&L, last 3 FY columns)
  - Latest annual CFO, Net Profit    (Screener.in cash flow / annual P&L - CFO
                                       is only published annually, so it's
                                       compared against annual, not TTM, PAT)
  - 3-year average ROE %             (Screener.in "Return on Equity" table)
  - Debt-to-Equity                   (Screener.in balance sheet: Borrowings /
                                       (Equity Capital + Reserves))

Also computes, for the Nifty 500 benchmark (^CRSLDX) and each sector index:
  - SMA50, SMA200, current price     (for the macro regime gate)
  - 12M-1M momentum                  (for the Pillar 3 relative-strength check)

Run with:
    ./.venv/bin/python3 enrich_momentum_metrics_v2.py
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
from urllib.error import HTTPError
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
TRADING_DAYS_YEAR = 252
TRADING_DAYS_MONTH = 21
YF_CHUNK_SIZE = 150
DEFAULT_WORKERS = 1
DEFAULT_REQUEST_DELAY = 3.0
SCREENER_MAX_RETRIES = 3
SCREENER_BACKOFF_BASE = 5.0
CIRCUIT_BREAKER_THRESHOLD = 6

# Independent copy of the same best-effort NSE-sector -> Yahoo sectoral-index
# mapping used by Strategy 1 (kept duplicated on purpose - no cross-imports).
SECTOR_INDEX_MAP = {
    "Automobile and Auto Components": "^CNXAUTO",
    "Financial Services": "NIFTY_FIN_SERVICE.NS",
    "Fast Moving Consumer Goods": "^CNXFMCG",
    "Information Technology": "^CNXIT",
    "Media Entertainment & Publication": "^CNXMEDIA",
    "Metals & Mining": "^CNXMETAL",
    "Healthcare": "^CNXPHARMA",
    "Realty": "^CNXREALTY",
    "Oil Gas & Consumable Fuels": "^CNXENERGY",
    "Services": "^CNXSERVICE",
    "Consumer Services": "^CNXCONSUM",
    "Construction": "^CNXINFRA",
}


@dataclass
class PriceMetricsV2:
    close: float | None = None
    ema50: float | None = None
    ema200: float | None = None
    high_52w: float | None = None
    atr14: float | None = None
    momentum_12m1m: float | None = None
    adtv_90d_cr: float | None = None
    rows: int = 0
    error: str = ""


@dataclass
class BenchmarkMetricsV2:
    price: float | None = None
    sma50: float | None = None
    sma200: float | None = None
    momentum_12m1m: float | None = None
    error: str = ""


@dataclass
class FundamentalMetrics:
    market_cap_cr: float | None = None
    ttm_eps: float | None = None
    eps_3yr_peak: float | None = None
    eps_yoy_growth_pct: float | None = None
    cfo_latest_annual_cr: float | None = None
    net_profit_latest_annual_cr: float | None = None
    ttm_pat_cr: float | None = None
    roe_3yr_avg_pct: float | None = None
    debt_to_equity: float | None = None
    source_view: str = ""
    error: str = ""
    blocked: bool = False


class CircuitBreaker:
    """Trips after N consecutive blocking-type failures so we stop hammering
    a host that has started refusing us."""

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
    price: PriceMetricsV2 = field(default_factory=PriceMetricsV2)
    sector_ticker: str = ""
    sector_momentum_12m1m: float | None = None
    benchmark_momentum_12m1m: float | None = None
    fundamentals: FundamentalMetrics = field(default_factory=FundamentalMetrics)

    def __post_init__(self):
        self.yahoo_ticker = f"{self.symbol}.NS"


# --------------------------------------------------------------------------
# Nifty 500 universe + bhavcopy input (same shape as Strategy 1, independent code)
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
    records, unmatched = [], []
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
            symbol=symbol, company_name=row["Company Name"].strip(),
            industry=row["Industry"].strip(), close_price=close_price,
        ))
    return records, unmatched


# --------------------------------------------------------------------------
# Yahoo Finance: EMA50/200, 52W high, ATR14, 12M-1M momentum, ADTV
# --------------------------------------------------------------------------

def _atr14(high: pd.Series, low: pd.Series, close: pd.Series) -> float | None:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    tr = tr.dropna()
    return float(tr.tail(14).mean()) if len(tr) >= 14 else None


def _momentum_12m_1m(close: pd.Series) -> float | None:
    """Return from ~12 months ago to ~1 month ago, skipping the most recent
    month to avoid short-term reversal noise (classic 12M-1M formation)."""
    if len(close) <= TRADING_DAYS_YEAR:
        return None
    base = close.iloc[-(TRADING_DAYS_YEAR + 1)]
    recent = close.iloc[-(TRADING_DAYS_MONTH + 1)] if len(close) > TRADING_DAYS_MONTH else close.iloc[-1]
    if not base:
        return None
    return float((recent - base) / base)


def compute_price_metrics_v2(close: pd.Series, high: pd.Series, low: pd.Series, volume: pd.Series) -> PriceMetricsV2:
    close = close.dropna()
    if close.empty:
        return PriceMetricsV2(error="no price history")
    high = high.reindex(close.index)
    low = low.reindex(close.index)
    volume = volume.reindex(close.index)

    ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])
    ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1])
    high_52w = float(high.dropna().tail(TRADING_DAYS_YEAR).max()) if not high.dropna().empty else None
    atr14 = _atr14(high, low, close)
    momentum = _momentum_12m_1m(close)

    adtv_90d_cr = None
    traded_value = (volume * close).dropna().tail(90)
    if not traded_value.empty:
        adtv_90d_cr = float(traded_value.mean() / 1e7)

    return PriceMetricsV2(
        close=float(close.iloc[-1]), ema50=ema50, ema200=ema200, high_52w=high_52w,
        atr14=atr14, momentum_12m1m=momentum, adtv_90d_cr=adtv_90d_cr, rows=len(close),
    )


def compute_benchmark_metrics(close: pd.Series) -> BenchmarkMetricsV2:
    close = close.dropna()
    if close.empty:
        return BenchmarkMetricsV2(error="no price history")
    sma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
    return BenchmarkMetricsV2(
        price=float(close.iloc[-1]), sma50=sma50, sma200=sma200,
        momentum_12m1m=_momentum_12m_1m(close),
    )


def fetch_all_price_data(tickers: list[str], timeout: int) -> dict[str, dict]:
    """Returns {ticker: {"close":..,"high":..,"low":..,"volume":..}} of pandas
    Series, or {"error": str} on failure - kept as raw series so callers can
    compute either per-stock or benchmark-style metrics from the same fetch."""
    results: dict[str, dict] = {}
    unique_tickers = sorted(set(tickers))
    for i in range(0, len(unique_tickers), YF_CHUNK_SIZE):
        chunk = unique_tickers[i:i + YF_CHUNK_SIZE]
        try:
            data = yf.download(
                chunk, period="max", group_by="ticker",
                auto_adjust=False, progress=False, threads=True, timeout=timeout,
            )
        except Exception as e:
            for t in chunk:
                results[t] = {"error": f"bulk download failed: {e}"}
            continue
        for t in chunk:
            try:
                sub = data if len(chunk) == 1 else data[t]
                results[t] = {"close": sub["Close"], "high": sub["High"], "low": sub["Low"], "volume": sub["Volume"]}
            except Exception as e:
                results[t] = {"error": f"no data returned: {e}"}
    return results


# --------------------------------------------------------------------------
# Screener.in: fundamentals (Market Cap, EPS, CFO, ROE, Debt/Equity)
# --------------------------------------------------------------------------

def _clean_number(raw: str) -> float | None:
    raw = raw.replace(",", "").replace("\xa0", "").replace("%", "").strip()
    if raw in ("", "-", "nan", "NaN"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _fetch_html_with_retry(url: str, timeout: int, max_retries: int) -> tuple[str | None, FundamentalMetrics | None]:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    attempt = 0
    while True:
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace"), None
        except HTTPError as e:
            if e.code == 404:
                return None, FundamentalMetrics(error="__not_found__")
            if e.code == 429 and attempt < max_retries:
                retry_after = e.headers.get("Retry-After") if e.headers else None
                delay = float(retry_after) if retry_after and retry_after.isdigit() else SCREENER_BACKOFF_BASE * (2 ** attempt)
                time.sleep(delay)
                attempt += 1
                continue
            return None, FundamentalMetrics(error=f"HTTP {e.code}", blocked=True)
        except OSError as e:
            if attempt < max_retries:
                time.sleep(SCREENER_BACKOFF_BASE * (2 ** attempt))
                attempt += 1
                continue
            reason = getattr(e, "reason", e)
            return None, FundamentalMetrics(error=f"network error: {reason}", blocked=True)


def _extract_market_cap(html: str) -> float | None:
    start = html.find('id="top-ratios"')
    if start == -1:
        return None
    section = html[start:start + 3000]
    m = re.search(r'Market Cap.*?<span class="number">([\d,]+)</span>', section, re.S)
    return _clean_number(m.group(1)) if m else None


def _row_values(table: pd.DataFrame, label_col: str, prefix: str, data_cols: list[str]) -> list[float]:
    labels = table[label_col].astype(str).str.replace("\xa0", " ").str.strip()
    match = labels.str.startswith(prefix)
    if not match.any():
        return []
    row = table.loc[match, data_cols].iloc[0]
    return [v for v in (_clean_number(str(x)) for x in row.tolist()) if v is not None]


def _extract_fundamentals(html: str, view: str) -> FundamentalMetrics | None:
    """Returns None if this view has no usable financial tables at all
    (caller falls back to the next view); otherwise a best-effort result
    with whichever individual fields it could find."""
    date_col_re = re.compile(r"^[A-Za-z]{3} \d{4}$")
    try:
        tables = pd.read_html(io.StringIO(html))
    except ValueError:
        return None

    found_any = False
    ttm_pat_cr = ttm_eps = None
    eps_3yr_peak = eps_yoy_growth_pct = None
    cfo_latest = net_profit_latest = None
    roe_3yr = None
    debt_to_equity = None

    for table in tables:
        cols = [str(c) for c in table.columns]
        if not cols:
            continue
        label_col = cols[0]
        date_cols = [c for c in cols if date_col_re.match(c)]

        if date_cols:
            months = {c.split()[0] for c in date_cols}
            is_annual = len(months) == 1  # annual tables use "Mar YYYY" throughout

            if not is_annual:
                pat_vals = _row_values(table, label_col, "Net Profit", date_cols)
                if len(pat_vals) >= 4:
                    ttm_pat_cr = sum(pat_vals[-4:])
                    found_any = True
                eps_vals = _row_values(table, label_col, "EPS in Rs", date_cols)
                if len(eps_vals) >= 4:
                    ttm_eps = sum(eps_vals[-4:])
                    found_any = True
            else:
                eps_vals = _row_values(table, label_col, "EPS in Rs", date_cols)
                if len(eps_vals) >= 2:
                    last_n = eps_vals[-3:] if len(eps_vals) >= 3 else eps_vals
                    eps_3yr_peak = max(last_n)
                    if eps_vals[-2]:
                        eps_yoy_growth_pct = (eps_vals[-1] - eps_vals[-2]) / abs(eps_vals[-2]) * 100
                    found_any = True
                pat_vals = _row_values(table, label_col, "Net Profit", date_cols)
                if pat_vals:
                    net_profit_latest = pat_vals[-1]
                    found_any = True
                cfo_vals = _row_values(table, label_col, "Cash from Operating Activity", date_cols)
                if cfo_vals:
                    cfo_latest = cfo_vals[-1]
                    found_any = True
                equity_vals = _row_values(table, label_col, "Equity Capital", date_cols)
                reserves_vals = _row_values(table, label_col, "Reserves", date_cols)
                borrowings_vals = _row_values(table, label_col, "Borrowings", date_cols)
                if equity_vals and reserves_vals and borrowings_vals:
                    total_equity = equity_vals[-1] + reserves_vals[-1]
                    if total_equity:
                        debt_to_equity = borrowings_vals[-1] / total_equity
                        found_any = True

        elif len(cols) == 2 and cols[0] == "Return on Equity":
            value_col = cols[1]
            labels2 = table[cols[0]].astype(str).str.strip()
            match = labels2.str.contains("3 Years")
            if match.any():
                roe_3yr = _clean_number(str(table.loc[match, value_col].iloc[0]))
                found_any = True

    if not found_any:
        return None

    return FundamentalMetrics(
        market_cap_cr=_extract_market_cap(html),
        ttm_eps=ttm_eps, eps_3yr_peak=eps_3yr_peak, eps_yoy_growth_pct=eps_yoy_growth_pct,
        cfo_latest_annual_cr=cfo_latest, net_profit_latest_annual_cr=net_profit_latest,
        ttm_pat_cr=ttm_pat_cr, roe_3yr_avg_pct=roe_3yr, debt_to_equity=debt_to_equity,
        source_view=view,
    )


def fetch_fundamentals(symbol: str, timeout: int, max_retries: int = SCREENER_MAX_RETRIES) -> FundamentalMetrics:
    for view, url in (
        ("consolidated", f"https://www.screener.in/company/{symbol}/consolidated/"),
        ("standalone", f"https://www.screener.in/company/{symbol}/"),
    ):
        html, failure = _fetch_html_with_retry(url, timeout, max_retries)
        if failure is not None:
            if failure.error == "__not_found__":
                continue
            return failure
        result = _extract_fundamentals(html, view)
        if result is not None:
            return result
    return FundamentalMetrics(error="No usable financial tables found in consolidated or standalone view")


def enrich_fundamentals(records: list[StockRecord], workers: int, delay: float, timeout: int) -> None:
    breaker = CircuitBreaker(CIRCUIT_BREAKER_THRESHOLD)

    def worker(rec: StockRecord) -> tuple[str, FundamentalMetrics]:
        if breaker.is_tripped():
            return rec.symbol, FundamentalMetrics(error="skipped: Screener.in circuit breaker open")
        time.sleep(delay)
        try:
            result = fetch_fundamentals(rec.symbol, timeout)
        except Exception as e:
            result = FundamentalMetrics(error=f"unexpected error: {e}", blocked=True)
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
            by_symbol[symbol].fundamentals = result
            done += 1
            if breaker.is_tripped() and not breaker_warned:
                breaker_warned = True
                print(f"  Screener.in circuit breaker OPEN after {done}/{total} - "
                      "skipping remaining requests", flush=True)
            if done % 25 == 0 or done == total:
                print(f"  Screener.in fundamentals: {done}/{total} processed", flush=True)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output)
    execution_dt = datetime.now(IST)

    print("=" * 60)
    print("Momentum Strategy-2 Enrichment (Nifty 500)")
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
    print()

    sector_tickers = sorted({SECTOR_INDEX_MAP[r.industry] for r in records if r.industry in SECTOR_INDEX_MAP})
    all_tickers = [r.yahoo_ticker for r in records] + [NIFTY500_BENCHMARK_TICKER] + sector_tickers

    print(f"Fetching Yahoo Finance price history for {len(all_tickers)} tickers "
          f"({len(records)} stocks + 1 benchmark + {len(sector_tickers)} sector indices) ...")
    price_data = fetch_all_price_data(all_tickers, args.timeout)

    benchmark_data = price_data.get(NIFTY500_BENCHMARK_TICKER, {})
    benchmark_metrics = (compute_benchmark_metrics(benchmark_data["close"])
                         if "close" in benchmark_data else BenchmarkMetricsV2(error="not fetched"))
    sector_metrics = {}
    for t in sector_tickers:
        d = price_data.get(t, {})
        sector_metrics[t] = compute_benchmark_metrics(d["close"]) if "close" in d else BenchmarkMetricsV2(error="not fetched")

    price_ok = 0
    for rec in records:
        d = price_data.get(rec.yahoo_ticker, {})
        if "close" in d:
            rec.price = compute_price_metrics_v2(d["close"], d["high"], d["low"], d["volume"])
        else:
            rec.price = PriceMetricsV2(error=d.get("error", "not fetched"))
        rec.sector_ticker = SECTOR_INDEX_MAP.get(rec.industry, "")
        rec.benchmark_momentum_12m1m = benchmark_metrics.momentum_12m1m
        rec.sector_momentum_12m1m = sector_metrics.get(rec.sector_ticker, BenchmarkMetricsV2()).momentum_12m1m
        if not rec.price.error:
            price_ok += 1
    print(f"  {price_ok}/{len(records)} stocks OK")
    print(f"  Nifty 500 benchmark: price={benchmark_metrics.price}, "
          f"SMA50={benchmark_metrics.sma50}, SMA200={benchmark_metrics.sma200}")
    print()

    if args.skip_fundamentals:
        print("Skipping Screener.in fundamentals - --skip-fundamentals set")
    else:
        print(f"Fetching fundamentals from Screener.in ({args.workers} workers, "
              f"{args.request_delay}s pacing) ...")
        enrich_fundamentals(records, args.workers, args.request_delay, args.timeout)
        fund_ok = sum(1 for r in records if not r.fundamentals.error)
        print(f"  {fund_ok}/{len(records)} stocks OK")
    print()

    enriched_dir = output_dir / "bhavcopy" / market_date / "enriched"
    enriched_dir.mkdir(parents=True, exist_ok=True)
    csv_path = enriched_dir / "momentum_metrics_v2.csv"
    json_path = enriched_dir / "momentum_metrics_v2.json"

    fieldnames = [
        "symbol", "company_name", "industry", "market_data_date", "close_price",
        "ema_50", "ema_200", "high_52w", "atr_14", "momentum_12m1m_pct", "adtv_90d_cr",
        "sector_index_ticker", "benchmark_momentum_12m1m_pct", "sector_momentum_12m1m_pct",
        "benchmark_price", "benchmark_sma50", "benchmark_sma200",
        "market_cap_cr", "ttm_eps", "eps_3yr_peak", "eps_yoy_growth_pct",
        "cfo_latest_annual_cr", "net_profit_latest_annual_cr", "ttm_pat_cr",
        "roe_3yr_avg_pct", "debt_to_equity", "notes",
    ]
    json_rows = []
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            notes = "; ".join(n for n in (rec.price.error, rec.fundamentals.error) if n)
            row = {
                "symbol": rec.symbol,
                "company_name": rec.company_name,
                "industry": rec.industry,
                "market_data_date": market_date,
                "close_price": rec.close_price,
                "ema_50": round(rec.price.ema50, 4) if rec.price.ema50 is not None else None,
                "ema_200": round(rec.price.ema200, 4) if rec.price.ema200 is not None else None,
                "high_52w": round(rec.price.high_52w, 4) if rec.price.high_52w is not None else None,
                "atr_14": round(rec.price.atr14, 4) if rec.price.atr14 is not None else None,
                "momentum_12m1m_pct": round(rec.price.momentum_12m1m * 100, 4) if rec.price.momentum_12m1m is not None else None,
                "adtv_90d_cr": round(rec.price.adtv_90d_cr, 4) if rec.price.adtv_90d_cr is not None else None,
                "sector_index_ticker": rec.sector_ticker or None,
                "benchmark_momentum_12m1m_pct": round(rec.benchmark_momentum_12m1m * 100, 4) if rec.benchmark_momentum_12m1m is not None else None,
                "sector_momentum_12m1m_pct": round(rec.sector_momentum_12m1m * 100, 4) if rec.sector_momentum_12m1m is not None else None,
                "benchmark_price": round(benchmark_metrics.price, 4) if benchmark_metrics.price is not None else None,
                "benchmark_sma50": round(benchmark_metrics.sma50, 4) if benchmark_metrics.sma50 is not None else None,
                "benchmark_sma200": round(benchmark_metrics.sma200, 4) if benchmark_metrics.sma200 is not None else None,
                "market_cap_cr": rec.fundamentals.market_cap_cr,
                "ttm_eps": rec.fundamentals.ttm_eps,
                "eps_3yr_peak": rec.fundamentals.eps_3yr_peak,
                "eps_yoy_growth_pct": round(rec.fundamentals.eps_yoy_growth_pct, 4) if rec.fundamentals.eps_yoy_growth_pct is not None else None,
                "cfo_latest_annual_cr": rec.fundamentals.cfo_latest_annual_cr,
                "net_profit_latest_annual_cr": rec.fundamentals.net_profit_latest_annual_cr,
                "ttm_pat_cr": rec.fundamentals.ttm_pat_cr,
                "roe_3yr_avg_pct": rec.fundamentals.roe_3yr_avg_pct,
                "debt_to_equity": round(rec.fundamentals.debt_to_equity, 4) if rec.fundamentals.debt_to_equity is not None else None,
                "notes": notes,
            }
            writer.writerow(row)
            json_rows.append(row)

    json_path.write_text(json.dumps({
        "execution_date": execution_dt.isoformat(),
        "market_data_date": market_date,
        "record_count": len(records),
        "unmatched_symbols": unmatched,
        "benchmark": {
            "price": benchmark_metrics.price, "sma50": benchmark_metrics.sma50,
            "sma200": benchmark_metrics.sma200, "momentum_12m1m_pct":
                round(benchmark_metrics.momentum_12m1m * 100, 4) if benchmark_metrics.momentum_12m1m is not None else None,
        },
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
        description="Momentum Strategy-2 enrichment: institutional-grade metrics "
                    "(52W-high/trend, quality/cash-flow, dual relative strength) for the Nifty 500.",
    )
    parser.add_argument("--output", type=str, default="data",
                         help="Base directory shared with nse_udiff_bhavcopy.py (default: data)")
    parser.add_argument("--date", type=str, default=None,
                         help="Market data date (YYYY-MM-DD) to enrich. Defaults to the "
                              "downloader's latest.json selection.")
    parser.add_argument("--skip-fundamentals", action="store_true",
                         help="Skip the Screener.in fundamentals scrape (faster, Yahoo-only run)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                         help=f"Concurrent Screener.in workers (default: {DEFAULT_WORKERS}, deliberately "
                              "conservative - see enrich_momentum_metrics.py for why)")
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
