#!/usr/bin/env python3
"""Local data browser for 3-Pillar momentum scan results.

Import CSV scan output, persist it into an append-only SQLite store, browse/
filter it in a table, and export the filtered view back out as CSV. Every
import appends its rows as-is - re-importing a file (or a file that overlaps
an earlier one on Scan_Date/Ticker_Symbol) adds duplicates rather than
replacing existing rows, since each import is a distinct historical record.

Run with:
    ./.venv/bin/python3 webapp/app.py
"""

import csv
import io
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_file, send_from_directory

APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DB_PATH = PROJECT_DIR / "data" / "scan_results.db"

# The 3-Pillar scan result schema. Stored as TEXT throughout so values like
# "-0.75%" and "N/A" round-trip losslessly between import and export.
COLUMNS = [
    "Scan_Date", "Ticker_Symbol", "Company_Name", "Sector_Index",
    "Closing_Price_INR", "Lifetime_ATH_Price", "Dist_From_ATH_Pct",
    "Pillar_1_ATH_Price_Status", "Latest_TTM_PAT_Cr", "Pillar_2_ATH_PAT_Status",
    "Stock_52W_Return_Pct", "Nifty500_52W_Return_Pct", "Sector_52W_Return_Pct",
    "Relative_Alpha_Pct", "Pillar_3_Outperformance_Status", "Pillars_Met_Count",
    "Status", "Suggested_200_EMA_SL", "Target_Allocation_Pct",
]
REQUIRED_COLUMNS = ("Scan_Date", "Ticker_Symbol")
SELECT_COLS_SQL = ", ".join(f'"{c}"' for c in COLUMNS)

# Columns the app computes itself at import time by comparing against each
# ticker's most recent prior import - never read from the uploaded CSV, even
# if a column with a matching name is present (e.g. a previously exported file).
DERIVED_COLUMNS = ["Entry_Status", "Previous_Closing_Price_INR", "Gain_Loss_Pct", "Suggestion"]

# Display/export order: identity + entry/gain-loss/suggestion up front, then
# the rest of the existing scan columns in their original order.
DISPLAY_COLUMNS = [
    "Ticker_Symbol", "Company_Name", "Entry_Status", "Closing_Price_INR",
    "Previous_Closing_Price_INR", "Gain_Loss_Pct", "Suggestion",
    "Scan_Date", "Sector_Index", "Lifetime_ATH_Price", "Dist_From_ATH_Pct",
    "Pillar_1_ATH_Price_Status", "Latest_TTM_PAT_Cr", "Pillar_2_ATH_PAT_Status",
    "Stock_52W_Return_Pct", "Nifty500_52W_Return_Pct", "Sector_52W_Return_Pct",
    "Relative_Alpha_Pct", "Pillar_3_Outperformance_Status", "Pillars_Met_Count",
    "Status", "Suggested_200_EMA_SL", "Target_Allocation_Pct",
]
DISPLAY_COLS_SQL = ", ".join(f'"{c}"' for c in DISPLAY_COLUMNS)

# Human-friendly header variants (as seen in spreadsheet exports) mapped to
# the canonical column they mean. Matching also falls back to a punctuation/
# case-insensitive comparison, so this only needs to cover abbreviations that
# fallback wouldn't catch on its own (e.g. "Ticker" vs "Ticker_Symbol").
HEADER_ALIASES = {
    "Scan_Date": ["Scan Date"],
    "Ticker_Symbol": ["Ticker"],
    "Company_Name": ["Company"],
    "Sector_Index": ["Sector"],
    "Closing_Price_INR": ["Close (INR)", "Close", "Closing Price"],
    "Lifetime_ATH_Price": ["ATH (INR)", "ATH Price (INR)", "Lifetime ATH"],
    "Dist_From_ATH_Pct": ["Dist. from ATH", "Dist from ATH", "Dist. from ATH (%)"],
    "Pillar_1_ATH_Price_Status": ["ATH Price", "Pillar 1", "Pillar 1 Status"],
    "Latest_TTM_PAT_Cr": ["TTM PAT (Cr)", "TTM PAT", "Latest TTM PAT"],
    "Pillar_2_ATH_PAT_Status": ["ATH PAT", "Pillar 2", "Pillar 2 Status"],
    "Stock_52W_Return_Pct": ["52W Stock (%)", "52W Stock", "Stock 52W Return"],
    "Nifty500_52W_Return_Pct": ["Nifty 500 52W", "Nifty500 52W", "Nifty 500 52W (%)"],
    "Sector_52W_Return_Pct": ["Sector 52W", "Sector 52W (%)"],
    "Relative_Alpha_Pct": ["Relative Alpha", "Relative Alpha (%)"],
    "Pillar_3_Outperformance_Status": ["Outperformance", "Pillar 3", "Pillar 3 Status"],
    "Pillars_Met_Count": ["Pillars", "Pillars Met"],
    "Suggested_200_EMA_SL": ["200 EMA SL (INR)", "200 EMA SL", "Suggested 200 EMA SL"],
    "Target_Allocation_Pct": ["Allocation (%)", "Allocation", "Target Allocation"],
}


def _normalize_header(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


# Scan_Date is stored as ISO (YYYY-MM-DD) text so date-range filters and
# ORDER BY can compare it lexicographically. Spreadsheet exports commonly use
# DD/MM/YY(YY) instead (the Indian convention), so normalize on the way in.
_DATE_INPUT_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%Y/%m/%d"]


def normalize_scan_date(raw: str) -> tuple[str, bool]:
    """Returns (normalized_or_original, was_recognized)."""
    raw = raw.strip()
    for fmt in _DATE_INPUT_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d"), True
        except ValueError:
            continue
    return raw, False


def resolve_column_mapping(fieldnames: list[str]) -> tuple[dict[str, str], list[str]]:
    """Maps canonical column -> actual CSV header present, tolerating
    spacing/punctuation/case differences and known abbreviations. Returns
    (mapping, unrecognized_headers)."""
    normalized_to_original = {_normalize_header(f): f for f in fieldnames}
    alias_to_canonical = {
        _normalize_header(alias): canonical
        for canonical, aliases in HEADER_ALIASES.items()
        for alias in aliases
    }

    mapping = {}
    for canonical in COLUMNS:
        norm_canonical = _normalize_header(canonical)
        if norm_canonical in normalized_to_original:
            mapping[canonical] = normalized_to_original[norm_canonical]
            continue
        for norm_header, original in normalized_to_original.items():
            if alias_to_canonical.get(norm_header) == canonical:
                mapping[canonical] = original
                break

    mapped_originals = set(mapping.values())
    unrecognized = [f for f in fieldnames if f not in mapped_originals]
    return mapping, unrecognized


def _parse_number(raw: str) -> float | None:
    raw = (raw or "").strip().replace(",", "")
    if not raw or raw.upper() == "N/A":
        return None
    try:
        return float(raw.rstrip("%"))
    except ValueError:
        return None


def _parse_int(raw: str) -> int | None:
    raw = (raw or "").strip()
    try:
        return int(raw)
    except ValueError:
        return None


def format_gain_loss(current_price: str, previous_price: str) -> str:
    current = _parse_number(current_price)
    previous = _parse_number(previous_price)
    if current is None or previous is None or previous == 0:
        return "N/A"
    pct = (current - previous) / previous * 100
    if pct == 0:
        return "0%"
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def compute_suggestion(params: dict) -> str:
    """Rule-based suggestion from the existing 3-Pillar fields only - this
    app has no RSI/volume/trend-strength data to draw on.
    ACCUMULATE - all 3 pillars still PASS and relative alpha isn't negative.
    EXIT       - the ATH-price pillar fails, or outperformance fails together
                 with negative alpha, or at most 1 pillar is met overall.
    HOLD       - everything in between (momentum healthy but not high-conviction)."""
    pillars_met = _parse_int(params.get("Pillars_Met_Count"))
    p1 = (params.get("Pillar_1_ATH_Price_Status") or "").strip().upper()
    p3 = (params.get("Pillar_3_Outperformance_Status") or "").strip().upper()
    alpha = _parse_number(params.get("Relative_Alpha_Pct"))

    if pillars_met is not None and pillars_met >= 3 and p1 == "PASS" and p3 == "PASS" and (alpha is None or alpha >= 0):
        return "ACCUMULATE"
    if p1 == "FAIL" or (p3 == "FAIL" and alpha is not None and alpha < 0) or (pillars_met is not None and pillars_met <= 1):
        return "EXIT"
    return "HOLD"


def get_previous_row(conn: sqlite3.Connection, ticker: str, before_ts: str) -> sqlite3.Row | None:
    """Most recent row for this ticker from strictly before the current
    import's timestamp - i.e. the last scan we had for it before now."""
    return conn.execute(
        'SELECT "Closing_Price_INR" FROM scan_results '
        'WHERE "Ticker_Symbol" = ? AND imported_at < ? '
        'ORDER BY imported_at DESC LIMIT 1',
        (ticker, before_ts),
    ).fetchone()


def get_previous_batch_tickers(conn: sqlite3.Connection, before_ts: str) -> set[str]:
    """Tickers from the single most recent import that happened before now -
    used to detect stocks that dropped out of the screen entirely."""
    row = conn.execute(
        'SELECT imported_at FROM scan_results WHERE imported_at < ? '
        'ORDER BY imported_at DESC LIMIT 1',
        (before_ts,),
    ).fetchone()
    if not row:
        return set()
    tickers = conn.execute(
        'SELECT DISTINCT "Ticker_Symbol" FROM scan_results WHERE imported_at = ?',
        (row[0],),
    ).fetchall()
    return {t[0] for t in tickers}


app = Flask(__name__)


def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    cols_sql = ", ".join(f'"{c}" TEXT' for c in COLUMNS)

    existing_ddl = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='scan_results'"
    ).fetchone()
    if existing_ddl and "PRIMARY KEY" in existing_ddl[0] and "id INTEGER" not in existing_ddl[0]:
        # Migrate from the old upsert-oriented schema (PRIMARY KEY on
        # Scan_Date/Ticker_Symbol) to the append-only schema, preserving
        # whatever rows were already persisted.
        old_rows = conn.execute(f"SELECT {SELECT_COLS_SQL}, imported_at FROM scan_results").fetchall()
        conn.execute("ALTER TABLE scan_results RENAME TO scan_results_old_migrated")
        conn.execute(f"""
            CREATE TABLE scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {cols_sql},
                imported_at TEXT
            )
        """)
        insert_cols = ", ".join(f'"{c}"' for c in COLUMNS)
        insert_ph = ", ".join("?" for _ in COLUMNS)
        for row in old_rows:
            values = [row[c] for c in COLUMNS] + [row["imported_at"]]
            conn.execute(
                f'INSERT INTO scan_results ({insert_cols}, imported_at) VALUES ({insert_ph}, ?)',
                values,
            )
        conn.execute("DROP TABLE scan_results_old_migrated")
    else:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {cols_sql},
                imported_at TEXT
            )
        """)

    # Backfill Entry_Status/Previous_Closing_Price_INR/Gain_Loss_Pct/Suggestion
    # for DBs created before this feature existed. Older rows are left blank
    # (NULL) since they predate the comparison logic - only new imports from
    # here on get these fields populated.
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(scan_results)").fetchall()}
    for col in DERIVED_COLUMNS:
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE scan_results ADD COLUMN "{col}" TEXT')

    conn.commit()
    conn.close()


@app.route("/")
def index():
    return send_from_directory(APP_DIR, "index.html")


@app.route("/api/import", methods=["POST"])
def api_import():
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "no file provided"}), 400

    try:
        text = file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
    except (UnicodeDecodeError, csv.Error) as e:
        return jsonify({"error": f"could not parse CSV: {e}"}), 400

    fieldnames = reader.fieldnames or []
    column_map, unknown = resolve_column_mapping(fieldnames)
    missing = [c for c in REQUIRED_COLUMNS if c not in column_map]
    if missing:
        return jsonify({"error": f"CSV missing required column(s): {missing}"}), 400
    unmapped_expected = [c for c in COLUMNS if c not in column_map]

    raw_rows = list(reader)
    if not raw_rows:
        return jsonify({"error": "CSV has no data rows"}), 400

    # Normalize + deduplicate by ticker within this file - last occurrence wins.
    deduped: dict[str, dict] = {}
    duplicate_tickers = 0
    for row in raw_rows:
        ticker_norm = (row.get(column_map["Ticker_Symbol"]) or "").strip().upper()
        if not ticker_norm:
            continue
        if ticker_norm in deduped:
            duplicate_tickers += 1
        deduped[ticker_norm] = row

    conn = get_db()
    inserted = skipped = 0
    new_entrant_count = existing_count = 0
    unparsed_dates = 0
    now = datetime.now(timezone.utc).isoformat()
    insert_columns = COLUMNS + DERIVED_COLUMNS
    placeholders = ", ".join(f'"{c}"' for c in insert_columns)
    values_ph = ", ".join(f":{c}" for c in insert_columns)
    imported_tickers = set()

    for ticker_norm, row in deduped.items():
        scan_date_raw = (row.get(column_map["Scan_Date"]) or "").strip()
        if not scan_date_raw:
            skipped += 1
            continue
        scan_date, recognized = normalize_scan_date(scan_date_raw)
        if not recognized:
            unparsed_dates += 1

        params = {
            c: (row.get(column_map[c]) or "").strip() if c in column_map else ""
            for c in COLUMNS
        }
        params["Scan_Date"] = scan_date
        params["Ticker_Symbol"] = ticker_norm
        params["imported_at"] = now

        prev = get_previous_row(conn, ticker_norm, now)
        if prev is None:
            params["Entry_Status"] = "New Entrant"
            params["Previous_Closing_Price_INR"] = ""
            params["Gain_Loss_Pct"] = "N/A"
            new_entrant_count += 1
        else:
            params["Entry_Status"] = "Existing"
            params["Previous_Closing_Price_INR"] = prev["Closing_Price_INR"] or ""
            params["Gain_Loss_Pct"] = format_gain_loss(params["Closing_Price_INR"], prev["Closing_Price_INR"])
            existing_count += 1
        params["Suggestion"] = compute_suggestion(params)

        # Always append - never overwrite an existing row, even if this
        # Scan_Date/Ticker_Symbol combo was already imported before.
        conn.execute(f"""
            INSERT INTO scan_results ({placeholders}, imported_at)
            VALUES ({values_ph}, :imported_at)
        """, params)
        inserted += 1
        imported_tickers.add(ticker_norm)

    exited_tickers = sorted(get_previous_batch_tickers(conn, now) - imported_tickers)

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM scan_results").fetchone()[0]
    conn.close()

    return jsonify({
        "inserted": inserted,
        "skipped": skipped,
        "duplicate_tickers_in_file": duplicate_tickers,
        "new_entrants": new_entrant_count,
        "existing": existing_count,
        "exited_tickers": exited_tickers,
        "unrecognized_dates": unparsed_dates,
        "unknown_columns": unknown,
        "unmapped_expected_columns": unmapped_expected,
        "total_records": total,
    })


def _build_filters(args):
    clauses = []
    params = []

    date_from = args.get("date_from", "").strip()
    if date_from:
        clauses.append('"Scan_Date" >= ?')
        params.append(date_from)

    date_to = args.get("date_to", "").strip()
    if date_to:
        clauses.append('"Scan_Date" <= ?')
        params.append(date_to)

    sector = args.get("sector", "").strip()
    if sector:
        clauses.append('"Sector_Index" = ?')
        params.append(sector)

    status = args.get("status", "").strip()
    if status:
        clauses.append('"Status" = ?')
        params.append(status)

    min_pillars = args.get("min_pillars", "").strip()
    if min_pillars:
        clauses.append('CAST("Pillars_Met_Count" AS INTEGER) >= ?')
        params.append(int(min_pillars))

    entry_status = args.get("entry_status", "").strip()
    if entry_status:
        clauses.append('"Entry_Status" = ?')
        params.append(entry_status)

    suggestion = args.get("suggestion", "").strip()
    if suggestion:
        clauses.append('"Suggestion" = ?')
        params.append(suggestion)

    search = args.get("search", "").strip()
    if search:
        clauses.append('("Ticker_Symbol" LIKE ? OR "Company_Name" LIKE ?)')
        like = f"%{search}%"
        params.extend([like, like])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


@app.route("/api/data")
def api_data():
    conn = get_db()
    where, params = _build_filters(request.args)
    rows = conn.execute(
        f'SELECT id, {DISPLAY_COLS_SQL}, imported_at FROM scan_results {where} '
        'ORDER BY "Scan_Date" DESC, "Ticker_Symbol" ASC, imported_at DESC',
        params,
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/meta")
def api_meta():
    conn = get_db()
    sectors = [r[0] for r in conn.execute(
        'SELECT DISTINCT "Sector_Index" FROM scan_results WHERE "Sector_Index" != "" ORDER BY 1'
    ).fetchall()]
    statuses = [r[0] for r in conn.execute(
        'SELECT DISTINCT "Status" FROM scan_results WHERE "Status" != "" ORDER BY 1'
    ).fetchall()]
    entry_statuses = [r[0] for r in conn.execute(
        'SELECT DISTINCT "Entry_Status" FROM scan_results WHERE "Entry_Status" IS NOT NULL AND "Entry_Status" != "" ORDER BY 1'
    ).fetchall()]
    suggestions = [r[0] for r in conn.execute(
        'SELECT DISTINCT "Suggestion" FROM scan_results WHERE "Suggestion" IS NOT NULL AND "Suggestion" != "" ORDER BY 1'
    ).fetchall()]
    bounds = conn.execute('SELECT MIN("Scan_Date"), MAX("Scan_Date") FROM scan_results').fetchone()
    total = conn.execute("SELECT COUNT(*) FROM scan_results").fetchone()[0]
    conn.close()
    return jsonify({
        "sectors": sectors,
        "statuses": statuses,
        "entry_statuses": entry_statuses,
        "suggestions": suggestions,
        "date_min": bounds[0],
        "date_max": bounds[1],
        "total_records": total,
    })


@app.route("/api/export")
def api_export():
    conn = get_db()
    where, params = _build_filters(request.args)
    rows = conn.execute(
        f'SELECT {DISPLAY_COLS_SQL} FROM scan_results {where} '
        'ORDER BY "Scan_Date" DESC, "Ticker_Symbol" ASC',
        params,
    ).fetchall()
    conn.close()

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=DISPLAY_COLUMNS)
    writer.writeheader()
    for r in rows:
        writer.writerow(dict(r))

    mem = io.BytesIO(buf.getvalue().encode("utf-8"))
    filename = f"scan_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=filename)


@app.route("/api/clear", methods=["POST"])
def api_clear():
    conn = get_db()
    conn.execute("DELETE FROM scan_results")
    conn.commit()
    conn.close()
    return jsonify({"status": "cleared"})


if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=5057, debug=False)
