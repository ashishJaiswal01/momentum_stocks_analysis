#!/usr/bin/env python3
"""3-Pillar Momentum Screening Engine.

Classifies every Nifty 500 stock in a given market date's enriched metrics
(produced by enrich_momentum_metrics.py) against the 3-Pillar framework:

  Pillar 1 (ATH Price)  - within 2% of lifetime high
                          (Dist_From_ATH_Pct >= -2.00%)
  Pillar 2 (ATH PAT)    - latest TTM PAT equals its own historical-max TTM PAT
  Pillar 3 (Outperformance) - trailing 52-week return beats both the Nifty 500
                          benchmark and (where available) its sector index

Status: 3/3 pillars -> SUPER_PERFORMER, 2/3 -> PERFORMER, <=1 -> disqualified.
A newly-disqualified stock is dropped from the output entirely (never
tracked) UNLESS it was already being tracked in a previous scan (read from
the web app's data/scan_results.db), in which case it's kept one more time
with Status=EXIT_SELL so the exit is visible.

Output is a flat CSV (no arrow/transition notation) using the app's plain
19-column scan schema plus one extra AI_Commentary column - Entry_Status,
Gain_Loss_Pct, and Suggestion are left for the web app to compute at import
time, exactly as for any other imported scan file, by comparing against its
own import history.

AI_Commentary is a short, factual, plain-English rationale generated via the
OpenAI API (config in .env - see .env.example) for each stock that makes the
final cut (SUPER_PERFORMER/PERFORMER/EXIT_SELL only, not the full 500-stock
universe, to keep API usage bounded). The underlying pillar math above is
100% deterministic and unaffected by this - the AI only narrates numbers
that were already computed; it never recalculates or overrides them. If no
OPENAI_API_KEY is configured, this column is simply left blank.

Run with:
    ./.venv/bin/python3 run_3pillar_scan.py --date 2026-08-14
    ./.venv/bin/python3 run_3pillar_scan.py          # latest available date
    ./.venv/bin/python3 run_3pillar_scan.py --skip-ai-commentary
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")  # safe no-op if absent

IST = ZoneInfo("Asia/Kolkata")

ATH_PRICE_THRESHOLD_PCT = -2.0   # Pillar 1: must be within 2% of lifetime high
RISK_PER_TRADE_PCT = 1.2         # Allocation numerator: fixed risk budget per position

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
    "purely mechanical, already-completed 3-pillar quantitative screen for one "
    "stock. Write exactly 1-2 sentences in plain English explaining why it has "
    "the given status, referencing the specific pillar(s) that passed or "
    "failed. Do not introduce any new numbers, opinions, price targets, or "
    "recommendations beyond restating the mechanical result you were given."
)


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
class PillarResult:
    dist_from_ath_pct: float | None
    pillar1: str
    pillar2: str
    pillar3: str
    pillars_met: int
    relative_alpha_pct: float | None
    allocation_pct: float | None


def compute_pillars(row: dict) -> PillarResult:
    close = _to_float(row.get("close_price"))
    ath_price = _to_float(row.get("lifetime_ath_price"))
    ema200 = _to_float(row.get("ema_200"))
    stock_52w = _to_float(row.get("return_52w_pct"))
    nifty_52w = _to_float(row.get("nifty500_return_52w_pct"))
    sector_52w = _to_float(row.get("sector_return_52w_pct"))
    ttm_ath = _to_float(row.get("ttm_pat_ath_cr"))
    ttm_latest = _to_float(row.get("ttm_pat_latest_cr"))

    dist_from_ath = None
    if close is not None and ath_price:
        dist_from_ath = (close - ath_price) / ath_price * 100
    pillar1 = "PASS" if dist_from_ath is not None and dist_from_ath >= ATH_PRICE_THRESHOLD_PCT else "FAIL"

    pillar2 = "FAIL"
    if ttm_ath is not None and ttm_latest is not None:
        pillar2 = "PASS" if ttm_latest >= ttm_ath - 0.005 else "FAIL"

    pillar3 = "FAIL"
    if stock_52w is not None and nifty_52w is not None:
        beats_benchmark = stock_52w > nifty_52w
        beats_sector = sector_52w is None or stock_52w > sector_52w
        pillar3 = "PASS" if beats_benchmark and beats_sector else "FAIL"

    pillars_met = sum(1 for p in (pillar1, pillar2, pillar3) if p == "PASS")

    relative_alpha = None
    if stock_52w is not None and nifty_52w is not None:
        relative_alpha = stock_52w - nifty_52w

    allocation = None
    if close and close > 0 and ema200 is not None:
        downside_risk_pct = (close - ema200) / close * 100
        if downside_risk_pct > 0:
            allocation = (RISK_PER_TRADE_PCT / downside_risk_pct) * 100

    return PillarResult(
        dist_from_ath_pct=dist_from_ath,
        pillar1=pillar1, pillar2=pillar2, pillar3=pillar3,
        pillars_met=pillars_met,
        relative_alpha_pct=relative_alpha,
        allocation_pct=allocation,
    )


class ScanInputError(Exception):
    """Raised when the inputs needed to run a scan aren't available."""


def load_enriched_rows(output_dir: Path, market_date: str) -> list[dict]:
    csv_path = output_dir / "bhavcopy" / market_date / "enriched" / "momentum_metrics.csv"
    if not csv_path.exists():
        raise ScanInputError(
            f"no enrichment output found for {market_date}: {csv_path} - "
            f"run enrich_momentum_metrics.py for this date first"
        )
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def get_previously_tracked_tickers(db_path: Path) -> set[str]:
    """Tickers from the single most recent batch already persisted in the
    web app's store - used to decide whether a now-disqualified stock should
    still appear (flagged EXIT_SELL) or be dropped as never having qualified."""
    if not db_path.exists():
        return set()
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT imported_at FROM scan_results ORDER BY imported_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return set()
        tickers = conn.execute(
            'SELECT DISTINCT "Ticker_Symbol" FROM scan_results WHERE imported_at = ?',
            (row[0],),
        ).fetchall()
        return {t[0] for t in tickers}
    except sqlite3.OperationalError:
        return set()
    finally:
        conn.close()


def _build_commentary_prompt(row: dict) -> str:
    return (
        f"Stock: {row['Company_Name']} ({row['Ticker_Symbol']}), Sector: {row['Sector_Index']}\n"
        f"Status: {row['Status']} ({row['Pillars_Met_Count']}/3 pillars met)\n"
        f"Close: Rs {row['Closing_Price_INR']}, Lifetime ATH: Rs {row['Lifetime_ATH_Price']}, "
        f"Dist. from ATH: {row['Dist_From_ATH_Pct']} -> Pillar 1 (ATH Price): {row['Pillar_1_ATH_Price_Status']}\n"
        f"Latest TTM PAT: Rs {row['Latest_TTM_PAT_Cr']} Cr -> Pillar 2 (ATH PAT): {row['Pillar_2_ATH_PAT_Status']}\n"
        f"52W Return: {row['Stock_52W_Return_Pct']} vs Nifty 500 {row['Nifty500_52W_Return_Pct']} "
        f"vs Sector {row['Sector_52W_Return_Pct']}, Relative Alpha: {row['Relative_Alpha_Pct']} "
        f"-> Pillar 3 (Outperformance): {row['Pillar_3_Outperformance_Status']}"
    )


def generate_ai_commentary(rows: list[dict]) -> tuple[int, int]:
    """Fills in AI_Commentary for each row via the OpenAI API, in place.
    Best-effort and additive only: never touches the pillar math, and any
    failure (missing key, network, rate limit) just leaves that row's
    commentary blank rather than failing the whole scan.
    Returns (ok_count, failed_count)."""
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
    previously_tracked = get_previously_tracked_tickers(output_dir / "scan_results.db")

    out_rows = []
    super_performer = performer = exit_sell = excluded = 0

    for row in rows:
        ticker = (row.get("symbol") or "").strip().upper()
        if not ticker:
            continue
        pr = compute_pillars(row)

        if pr.pillars_met >= 3:
            status = "SUPER_PERFORMER"
        elif pr.pillars_met == 2:
            status = "PERFORMER"
        elif ticker in previously_tracked:
            status = "EXIT_SELL"
        else:
            excluded += 1
            continue  # never tracked, still disqualified - not surfaced

        if status == "SUPER_PERFORMER":
            super_performer += 1
        elif status == "PERFORMER":
            performer += 1
        else:
            exit_sell += 1

        out_rows.append({
            "Scan_Date": market_date,
            "Ticker_Symbol": ticker,
            "Company_Name": row.get("company_name") or "",
            "Sector_Index": row.get("industry") or "",
            "Closing_Price_INR": _fmt(_to_float(row.get("close_price"))),
            "Lifetime_ATH_Price": _fmt(_to_float(row.get("lifetime_ath_price"))),
            "Dist_From_ATH_Pct": _fmt_pct(pr.dist_from_ath_pct),
            "Pillar_1_ATH_Price_Status": pr.pillar1,
            "Latest_TTM_PAT_Cr": _fmt(_to_float(row.get("ttm_pat_latest_cr")), 0),
            "Pillar_2_ATH_PAT_Status": pr.pillar2,
            "Stock_52W_Return_Pct": _fmt_pct(_to_float(row.get("return_52w_pct"))),
            "Nifty500_52W_Return_Pct": _fmt_pct(_to_float(row.get("nifty500_return_52w_pct"))),
            "Sector_52W_Return_Pct": _fmt_pct(_to_float(row.get("sector_return_52w_pct"))),
            "Relative_Alpha_Pct": _fmt_pct(pr.relative_alpha_pct),
            "Pillar_3_Outperformance_Status": pr.pillar3,
            "Pillars_Met_Count": str(pr.pillars_met),
            "Status": status,
            "Suggested_200_EMA_SL": _fmt(_to_float(row.get("ema_200"))),
            "Target_Allocation_Pct": _fmt_pct(pr.allocation_pct),
            "AI_Commentary": "",
        })

    ai_ok = ai_failed = 0
    if skip_ai_commentary:
        print(f"  Skipping AI commentary (--skip-ai-commentary)")
    elif out_rows:
        ai_ok, ai_failed = generate_ai_commentary(out_rows)

    enriched_dir = output_dir / "bhavcopy" / market_date / "enriched"
    enriched_dir.mkdir(parents=True, exist_ok=True)
    csv_path = enriched_dir / "3pillar_scan.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(out_rows)

    meta_path = enriched_dir / "3pillar_scan_meta.json"
    meta = {
        "execution_time": datetime.now(IST).isoformat(),
        "market_data_date": market_date,
        "csv_path": str(csv_path),
        "universe_size": len(rows),
        "super_performer": super_performer,
        "performer": performer,
        "exit_sell": exit_sell,
        "excluded": excluded,
        "total_output_rows": len(out_rows),
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
        description="3-Pillar Momentum Screening Engine - classifies stocks from an "
                    "enrich_momentum_metrics.py run into SUPER_PERFORMER/PERFORMER/EXIT_SELL.",
    )
    parser.add_argument("--output", type=str, default="data",
                         help="Base directory shared with the rest of the pipeline (default: data)")
    parser.add_argument("--date", type=str, default=None,
                         help="Market data date (YYYY-MM-DD) to scan. Defaults to the "
                              "downloader's latest.json selection.")
    parser.add_argument("--skip-ai-commentary", action="store_true",
                         help="Skip the OpenAI-generated per-stock rationale (faster, no API key needed)")
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
    print("3-Pillar Momentum Screening Engine")
    print("=" * 60)
    print(f"Market Data Date : {market_date}")
    print()

    try:
        meta = run_scan(output_dir, market_date, skip_ai_commentary=args.skip_ai_commentary)
    except ScanInputError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    print(f"Universe scanned   : {meta['universe_size']}")
    print(f"SUPER_PERFORMER    : {meta['super_performer']}")
    print(f"PERFORMER          : {meta['performer']}")
    print(f"EXIT_SELL          : {meta['exit_sell']}")
    print(f"Excluded (<=1 pillar, never tracked): {meta['excluded']}")
    print(f"Total output rows  : {meta['total_output_rows']}")
    print(f"AI commentary      : {meta['ai_commentary_ok']} ok, {meta['ai_commentary_failed']} failed")
    print(f"CSV                : {meta['csv_path']}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
