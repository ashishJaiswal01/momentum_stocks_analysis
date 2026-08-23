#!/usr/bin/env python3
"""Institutional Momentum Screening & Execution engine ("Strategy 2").

Fully independent of run_3pillar_scan.py (Strategy 1) - separate script,
separate SQLite store (data/scan_results_v2.db), nothing shared or imported
between them. Reads data/bhavcopy/<date>/enriched/momentum_metrics_v2.csv
(produced by enrich_momentum_metrics_v2.py).

Step 0 - Macro Regime & Liquidity Gate (non-negotiable pre-conditions):
  - Macro regime: Nifty 500 price > its 200-day SMA, and 50-day SMA >=
    200-day SMA. If this fails, no NEW Tier-1 (BUY) entries are allowed for
    ANY stock this scan - existing Tier-1 candidates are capped to Tier 2
    (HOLD) instead. Existing Tier-3 exits still fire regardless.
  - Liquidity: 90-day ADTV >= Rs 5 Cr AND Market Cap >= Rs 2,000 Cr. A stock
    failing this is dropped entirely, regardless of prior tracking.

Step 1 - The 3-Pillar core screen (per liquid stock):
  Pillar 1 (52W-High Anchor + Trend): close >= 90% of rolling 52-week high,
    AND close > EMA50 > EMA200.
  Pillar 2 (Quality/Cash-Flow): (TTM EPS within 5% of 3yr peak OR EPS YoY
    growth >= 15%) AND (latest annual CFO >= latest annual Net Profit) AND
    (3yr avg ROE >= 12% AND, for non-Financial-Services stocks, Debt/Equity
    < 1.5x).
  Pillar 3 (Dual Relative Strength): 12M-1M momentum beats both the Nifty
    500 benchmark and (where available) the stock's sector index over the
    same window.

Tiering (mirrors Strategy 1's shape so the same web UI works unmodified):
  All 3 pillars + liquidity + macro gate all pass -> Tier 1 "SUPER_PERFORMER"
  Pillar 1 + Pillar 2 pass (Pillar 3 may not, or macro gate is closed)
    -> Tier 2 "PERFORMER"
  Otherwise (including failing the liquidity gate) -> dropped from the output
    entirely, regardless of whether it was tracked in a previous scan.

Suggested stop-loss: max(EMA50, close - 3.0 * ATR14).
Suggested allocation: equal-weight across this scan's Tier-1 names, clamped
to 5%-7% per position (only populated for Tier-1 rows - Tier 2/3 aren't new
entries so a sizing figure doesn't apply).

Output is a flat CSV (no arrow/transition notation) using the same 19-column
scan schema as Strategy 1 (column names repurposed for this strategy's own
metrics - e.g. Lifetime_ATH_Price holds the rolling 52-week high here, not a
lifetime high; see column comments below) - Entry_Status, Gain_Loss_Pct, and
Suggestion are left for the web app to compute at import time, exactly as
for Strategy 1.

Run with:
    ./.venv/bin/python3 run_momentum_strategy2_scan.py --date 2026-08-21
    ./.venv/bin/python3 run_momentum_strategy2_scan.py          # latest date
"""

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

IST = ZoneInfo("Asia/Kolkata")

HIGH_52W_ANCHOR_RATIO = 0.90     # Pillar 1A: close must be >= 90% of 52W high
EPS_NEAR_PEAK_RATIO = 0.95       # Pillar 2A: TTM EPS within 5% of 3yr peak
EPS_YOY_GROWTH_MIN_PCT = 15.0    # Pillar 2A alternative: >=15% YoY EPS growth
ROE_MIN_PCT = 12.0               # Pillar 2C
MAX_DEBT_TO_EQUITY = 1.5         # Pillar 2C, non-financials only
FINANCIAL_SECTOR_NAME = "Financial Services"
ADTV_MIN_CR = 5.0                # Liquidity gate
MARKET_CAP_MIN_CR = 2000.0       # Liquidity gate
ATR_STOP_MULTIPLIER = 3.0
ALLOCATION_MIN_PCT = 5.0
ALLOCATION_MAX_PCT = 7.0
TARGET_HOLDINGS_MAX = 20

OUTPUT_COLUMNS = [
    "Scan_Date", "Ticker_Symbol", "Company_Name", "Sector_Index",
    "Closing_Price_INR", "Lifetime_ATH_Price", "Dist_From_ATH_Pct",
    "Pillar_1_ATH_Price_Status", "Latest_TTM_PAT_Cr", "Pillar_2_ATH_PAT_Status",
    "Stock_52W_Return_Pct", "Nifty500_52W_Return_Pct", "Sector_52W_Return_Pct",
    "Relative_Alpha_Pct", "Pillar_3_Outperformance_Status", "Pillars_Met_Count",
    "Status", "Suggested_200_EMA_SL", "Target_Allocation_Pct", "AI_Commentary",
]

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
AI_COMMENTARY_SYSTEM_PROMPT = (
    "You are a terse equity research assistant. You are given the output of a "
    "purely mechanical, already-completed institutional momentum screen (52-week "
    "high + trend, earnings quality + cash-flow + ROE, dual relative strength) "
    "for one stock. Write exactly 1-2 sentences in plain English explaining why "
    "it has the given status/tier, referencing the specific pillar(s) that "
    "passed or failed. Do not introduce any new numbers, opinions, price "
    "targets, or recommendations beyond restating the mechanical result you "
    "were given."
)


class ScanInputError(Exception):
    """Raised when the inputs needed to run a scan aren't available."""


def _to_float(raw) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _fmt(value: float | None, digits: int = 2) -> str:
    return "" if value is None else f"{value:.{digits}f}"


def _fmt_pct(value: float | None, digits: int = 2) -> str:
    return "N/A" if value is None else f"{value:.{digits}f}%"


@dataclass
class GateResult:
    liquidity_pass: bool
    pillar1: str
    pillar2: str
    pillar2_cashflow_pass: bool  # tracked separately: this alone can trigger Tier 3
    pillar3: str
    pillars_met: int
    dist_from_52w_high_pct: float | None
    relative_alpha_pct: float | None
    stop_loss: float | None


def evaluate_gates(row: dict) -> GateResult:
    close = _to_float(row.get("close_price"))
    ema50 = _to_float(row.get("ema_50"))
    ema200 = _to_float(row.get("ema_200"))
    high_52w = _to_float(row.get("high_52w"))
    atr14 = _to_float(row.get("atr_14"))
    adtv = _to_float(row.get("adtv_90d_cr"))
    market_cap = _to_float(row.get("market_cap_cr"))
    ttm_eps = _to_float(row.get("ttm_eps"))
    eps_3yr_peak = _to_float(row.get("eps_3yr_peak"))
    eps_yoy = _to_float(row.get("eps_yoy_growth_pct"))
    cfo = _to_float(row.get("cfo_latest_annual_cr"))
    net_profit = _to_float(row.get("net_profit_latest_annual_cr"))
    roe_3yr = _to_float(row.get("roe_3yr_avg_pct"))
    debt_to_equity = _to_float(row.get("debt_to_equity"))
    stock_mom = _to_float(row.get("momentum_12m1m_pct"))
    benchmark_mom = _to_float(row.get("benchmark_momentum_12m1m_pct"))
    sector_mom = _to_float(row.get("sector_momentum_12m1m_pct"))
    is_financial = (row.get("industry") or "").strip() == FINANCIAL_SECTOR_NAME

    liquidity_pass = (adtv is not None and adtv >= ADTV_MIN_CR
                       and market_cap is not None and market_cap >= MARKET_CAP_MIN_CR)

    dist_from_high = None
    p1 = "FAIL"
    if close is not None and high_52w:
        dist_from_high = (close - high_52w) / high_52w * 100
        near_high = (close / high_52w) >= HIGH_52W_ANCHOR_RATIO
        trend_aligned = ema50 is not None and ema200 is not None and close > ema50 > ema200
        p1 = "PASS" if near_high and trend_aligned else "FAIL"

    earnings_momentum = (
        (ttm_eps is not None and eps_3yr_peak is not None and eps_3yr_peak > 0
         and ttm_eps >= eps_3yr_peak * EPS_NEAR_PEAK_RATIO)
        or (eps_yoy is not None and eps_yoy >= EPS_YOY_GROWTH_MIN_PCT)
    )
    cashflow_ok = cfo is not None and net_profit is not None and cfo >= net_profit
    capital_efficiency_ok = (
        roe_3yr is not None and roe_3yr >= ROE_MIN_PCT
        and (is_financial or (debt_to_equity is not None and debt_to_equity < MAX_DEBT_TO_EQUITY))
    )
    p2 = "PASS" if earnings_momentum and cashflow_ok and capital_efficiency_ok else "FAIL"

    relative_alpha = None
    p3 = "FAIL"
    if stock_mom is not None and benchmark_mom is not None:
        relative_alpha = stock_mom - benchmark_mom
        beats_benchmark = stock_mom > benchmark_mom
        beats_sector = sector_mom is None or stock_mom > sector_mom
        p3 = "PASS" if beats_benchmark and beats_sector else "FAIL"

    pillars_met = sum(1 for p in (p1, p2, p3) if p == "PASS")

    stop_loss = None
    if ema50 is not None and close is not None and atr14 is not None:
        stop_loss = max(ema50, close - ATR_STOP_MULTIPLIER * atr14)

    return GateResult(
        liquidity_pass=liquidity_pass, pillar1=p1, pillar2=p2,
        pillar2_cashflow_pass=cashflow_ok, pillar3=p3, pillars_met=pillars_met,
        dist_from_52w_high_pct=dist_from_high, relative_alpha_pct=relative_alpha,
        stop_loss=stop_loss,
    )


def load_enriched_rows(output_dir: Path, market_date: str) -> list[dict]:
    csv_path = output_dir / "bhavcopy" / market_date / "enriched" / "momentum_metrics_v2.csv"
    if not csv_path.exists():
        raise ScanInputError(
            f"no Strategy-2 enrichment output found for {market_date}: {csv_path} - "
            f"run enrich_momentum_metrics_v2.py for this date first"
        )
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _build_commentary_prompt(row: dict) -> str:
    return (
        f"Stock: {row['Company_Name']} ({row['Ticker_Symbol']}), Sector: {row['Sector_Index']}\n"
        f"Status: {row['Status']} ({row['Pillars_Met_Count']}/3 pillars met)\n"
        f"Close: Rs {row['Closing_Price_INR']}, 52W High: Rs {row['Lifetime_ATH_Price']}, "
        f"Dist. from 52W High: {row['Dist_From_ATH_Pct']} -> Pillar 1 (52W High + Trend): {row['Pillar_1_ATH_Price_Status']}\n"
        f"Latest TTM PAT: Rs {row['Latest_TTM_PAT_Cr']} Cr -> Pillar 2 (Quality/Cash-Flow/ROE): {row['Pillar_2_ATH_PAT_Status']}\n"
        f"12M-1M Momentum: {row['Stock_52W_Return_Pct']} vs Nifty 500 {row['Nifty500_52W_Return_Pct']} "
        f"vs Sector {row['Sector_52W_Return_Pct']}, Relative Alpha: {row['Relative_Alpha_Pct']} "
        f"-> Pillar 3 (Dual Relative Strength): {row['Pillar_3_Outperformance_Status']}"
    )


def generate_ai_commentary(rows: list[dict]) -> tuple[int, int]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("  Skipping AI commentary: OPENAI_API_KEY not set (see .env.example)")
        for row in rows:
            row["AI_Commentary"] = ""
        return 0, 0

    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    model = os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)

    ok = failed = 0
    for row in rows:
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": AI_COMMENTARY_SYSTEM_PROMPT},
                    {"role": "user", "content": _build_commentary_prompt(row)},
                ],
                max_tokens=120,
                temperature=0.3,
            )
            row["AI_Commentary"] = (resp.choices[0].message.content or "").strip()
            ok += 1
        except Exception as e:
            row["AI_Commentary"] = ""
            failed += 1
            print(f"  AI commentary failed for {row['Ticker_Symbol']}: {e}")
    return ok, failed


def run_scan(output_dir: Path, market_date: str, skip_ai_commentary: bool = False) -> dict:
    rows = load_enriched_rows(output_dir, market_date)

    if not rows:
        raise ScanInputError(f"enrichment file for {market_date} has no rows")

    benchmark_price = _to_float(rows[0].get("benchmark_price"))
    benchmark_sma50 = _to_float(rows[0].get("benchmark_sma50"))
    benchmark_sma200 = _to_float(rows[0].get("benchmark_sma200"))
    macro_gate_pass = (
        benchmark_price is not None and benchmark_sma200 is not None and benchmark_sma50 is not None
        and benchmark_price > benchmark_sma200 and benchmark_sma50 >= benchmark_sma200
    )

    candidates = []  # (row, ticker, gate, tier) - tier assigned before allocation sizing
    super_performer = performer = excluded = 0

    for row in rows:
        ticker = (row.get("symbol") or "").strip().upper()
        if not ticker:
            continue
        gate = evaluate_gates(row)

        if not gate.liquidity_pass:
            excluded += 1
            continue
        elif gate.pillars_met >= 3 and macro_gate_pass:
            tier = "SUPER_PERFORMER"
        elif gate.pillar1 == "PASS" and gate.pillar2 == "PASS":
            tier = "PERFORMER"  # includes the "3/3 but macro gate closed" case (HALT new BUYs)
        else:
            excluded += 1
            continue  # <=1 effective pillar (or a broken Pillar 1/2) - dropped regardless of prior tracking

        if tier == "SUPER_PERFORMER":
            super_performer += 1
        else:
            performer += 1

        candidates.append((row, ticker, gate, tier))

    tier1_count = super_performer
    target_holdings = min(tier1_count, TARGET_HOLDINGS_MAX) if tier1_count else 0
    allocation_pct = None
    if target_holdings:
        allocation_pct = min(ALLOCATION_MAX_PCT, max(ALLOCATION_MIN_PCT, 100.0 / target_holdings))

    out_rows = []
    for row, ticker, gate, tier in candidates:
        out_rows.append({
            "Scan_Date": market_date,
            "Ticker_Symbol": ticker,
            "Company_Name": row.get("company_name") or "",
            "Sector_Index": row.get("industry") or "",
            "Closing_Price_INR": _fmt(_to_float(row.get("close_price"))),
            "Lifetime_ATH_Price": _fmt(_to_float(row.get("high_52w"))),  # NOTE: 52-week high, not lifetime ATH
            "Dist_From_ATH_Pct": _fmt_pct(gate.dist_from_52w_high_pct),
            "Pillar_1_ATH_Price_Status": gate.pillar1,
            "Latest_TTM_PAT_Cr": _fmt(_to_float(row.get("ttm_pat_cr")), 0),
            "Pillar_2_ATH_PAT_Status": gate.pillar2,
            "Stock_52W_Return_Pct": _fmt_pct(_to_float(row.get("momentum_12m1m_pct"))),  # NOTE: 12M-1M momentum
            "Nifty500_52W_Return_Pct": _fmt_pct(_to_float(row.get("benchmark_momentum_12m1m_pct"))),
            "Sector_52W_Return_Pct": _fmt_pct(_to_float(row.get("sector_momentum_12m1m_pct"))),
            "Relative_Alpha_Pct": _fmt_pct(gate.relative_alpha_pct),
            "Pillar_3_Outperformance_Status": gate.pillar3,
            "Pillars_Met_Count": str(gate.pillars_met),
            "Status": tier,
            "Suggested_200_EMA_SL": _fmt(gate.stop_loss),  # NOTE: max(EMA50, close - 3*ATR14)
            "Target_Allocation_Pct": _fmt_pct(allocation_pct) if tier == "SUPER_PERFORMER" and allocation_pct else "",
            "AI_Commentary": "",
        })

    ai_ok = ai_failed = 0
    if skip_ai_commentary:
        print("  Skipping AI commentary (--skip-ai-commentary)")
    elif out_rows:
        ai_ok, ai_failed = generate_ai_commentary(out_rows)

    enriched_dir = output_dir / "bhavcopy" / market_date / "enriched"
    enriched_dir.mkdir(parents=True, exist_ok=True)
    csv_path = enriched_dir / "momentum_strategy2_scan.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(out_rows)

    meta_path = enriched_dir / "momentum_strategy2_scan_meta.json"
    meta = {
        "execution_time": datetime.now(IST).isoformat(),
        "market_data_date": market_date,
        "csv_path": str(csv_path),
        "universe_size": len(rows),
        "macro_gate_pass": macro_gate_pass,
        "benchmark_price": benchmark_price,
        "benchmark_sma50": benchmark_sma50,
        "benchmark_sma200": benchmark_sma200,
        "super_performer": super_performer,
        "performer": performer,
        "excluded": excluded,
        "total_output_rows": len(out_rows),
        "target_allocation_pct": allocation_pct,
        "ai_commentary_ok": ai_ok,
        "ai_commentary_failed": ai_failed,
    }
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta


def resolve_market_date(output_dir: Path, requested: str | None) -> str:
    if requested:
        return requested
    latest_path = output_dir / "bhavcopy" / "latest.json"
    if not latest_path.exists():
        raise ScanInputError(
            f"no bhavcopy metadata found at {latest_path} - "
            f"run nse_udiff_bhavcopy.py first, or pass --date explicitly"
        )
    return json.loads(latest_path.read_text())["market_data_date"]


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Institutional Momentum Screening & Execution engine (Strategy 2) - "
                    "macro/liquidity gates + 3 pillars -> SUPER_PERFORMER/PERFORMER.",
    )
    parser.add_argument("--output", type=str, default="data",
                         help="Base directory shared with the rest of the pipeline (default: data)")
    parser.add_argument("--date", type=str, default=None,
                         help="Market data date (YYYY-MM-DD) to scan. Defaults to the "
                              "downloader's latest.json selection.")
    parser.add_argument("--skip-ai-commentary", action="store_true",
                         help="Skip the OpenAI-generated per-stock rationale")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output)
    try:
        market_date = resolve_market_date(output_dir, args.date)
    except ScanInputError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    print("=" * 60)
    print("Institutional Momentum Screening & Execution (Strategy 2)")
    print("=" * 60)
    print(f"Market Data Date : {market_date}")
    print()

    try:
        meta = run_scan(output_dir, market_date, skip_ai_commentary=args.skip_ai_commentary)
    except ScanInputError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    gate = "OPEN (SMA50 >= SMA200 > price)" if meta["macro_gate_pass"] else "CLOSED (new BUYs halted)"
    print(f"Macro Regime Gate  : {gate}")
    print(f"  Nifty 500 price={meta['benchmark_price']}, SMA50={meta['benchmark_sma50']}, SMA200={meta['benchmark_sma200']}")
    print(f"Universe scanned   : {meta['universe_size']}")
    print(f"SUPER_PERFORMER    : {meta['super_performer']}")
    print(f"PERFORMER          : {meta['performer']}")
    print(f"Excluded (liquidity/pillars): {meta['excluded']}")
    print(f"Total output rows  : {meta['total_output_rows']}")
    print(f"AI commentary      : {meta['ai_commentary_ok']} ok, {meta['ai_commentary_failed']} failed")
    print(f"CSV                : {meta['csv_path']}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
