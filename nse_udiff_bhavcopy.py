#!/usr/bin/env python3
"""NSE COM-UDiFF Common Bhavcopy Final (ZIP) downloader.

Fetches the daily Capital Market UDiFF bhavcopy from NSE's public archive
(nsearchives.nseindia.com). Supports downloading a specific trading date, or
auto-discovering the latest available date by walking backwards from today.

Downstream tools should read the Market Data Date and file locations from the
metadata.json / latest.json written alongside the downloaded data, rather
than independently guessing which trading date to use.
"""

import argparse
import csv
import io
import json
import re
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

BASE_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
CSV_NAME_RE = re.compile(r"^BhavCopy_NSE_CM_0_0_0_(\d{8})_F_\d{4}\.csv$")
REQUIRED_COLUMNS = [
    "TradDt", "BizDt", "Sgmt", "ISIN", "TckrSymb",
    "OpnPric", "HghPric", "LwPric", "ClsPric", "TtlTradgVol",
]
MIN_RECORDS = 100
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_TIMEOUT = 30
NETWORK_RETRIES = 2
NETWORK_RETRY_DELAY = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
IST = ZoneInfo("Asia/Kolkata")


@dataclass
class FetchResult:
    ok: bool
    status: int | None = None
    data: bytes = b""
    error: str = ""


@dataclass
class ValidationResult:
    ok: bool
    reason: str = ""
    trading_date: date | None = None
    record_count: int = 0
    csv_filename: str = ""
    csv_bytes: bytes = b""


@dataclass
class AttemptOutcome:
    success: bool
    market_date: date
    result: ValidationResult | None
    zip_path: Path | None
    extracted_dir: Path | None
    url: str


def build_url(d: date) -> str:
    return BASE_URL.format(yyyymmdd=d.strftime("%Y%m%d"))


def fetch_bytes(url: str, timeout: int) -> FetchResult:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    last_error = ""
    for attempt in range(NETWORK_RETRIES + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return FetchResult(ok=True, status=resp.status, data=resp.read())
        except HTTPError as e:
            # A clean HTTP error (404 etc.) means "not available" - not a
            # transient fault - so don't retry it.
            return FetchResult(ok=False, status=e.code, error=f"HTTP {e.code}")
        except (URLError, TimeoutError) as e:
            last_error = getattr(e, "reason", e) or str(e)
            if attempt < NETWORK_RETRIES:
                time.sleep(NETWORK_RETRY_DELAY)
    return FetchResult(ok=False, error=f"network error: {last_error}")


def validate_zip(data: bytes, candidate: date) -> ValidationResult:
    if not data:
        return ValidationResult(False, "empty response body")
    if data[:2] != b"PK":
        return ValidationResult(False, "response is not a ZIP file")

    bio = io.BytesIO(data)
    if not zipfile.is_zipfile(bio):
        return ValidationResult(False, "response is not a valid ZIP file")

    try:
        zf = zipfile.ZipFile(bio)
    except zipfile.BadZipFile as e:
        return ValidationResult(False, f"corrupt ZIP: {e}")

    bad_member = zf.testzip()
    if bad_member is not None:
        return ValidationResult(False, f"corrupt member in ZIP: {bad_member}")

    csv_names = [n for n in zf.namelist() if CSV_NAME_RE.match(Path(n).name)]
    if not csv_names:
        return ValidationResult(False, "ZIP does not contain expected UDiFF CSV file")

    csv_name = csv_names[0]
    raw = zf.read(csv_name)
    if not raw:
        return ValidationResult(False, "extracted CSV is empty")

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        return ValidationResult(False, f"CSV is not valid UTF-8: {e}")

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
    if missing:
        return ValidationResult(False, f"CSV missing required columns: {missing}")

    rows = list(reader)
    record_count = len(rows)
    if record_count < MIN_RECORDS:
        return ValidationResult(False, f"record count too low ({record_count})")

    raw_trad_dt = (rows[0].get("TradDt") or "").strip()
    try:
        trading_date = datetime.strptime(raw_trad_dt, "%Y-%m-%d").date()
    except ValueError:
        return ValidationResult(False, f"invalid trading date in CSV: {raw_trad_dt!r}")

    if trading_date != candidate:
        return ValidationResult(
            False, f"trading date mismatch: expected {candidate}, got {trading_date}"
        )

    if not any((r.get("ClsPric") or "").strip() for r in rows):
        return ValidationResult(False, "closing price field present but empty for all records")

    return ValidationResult(
        ok=True,
        trading_date=trading_date,
        record_count=record_count,
        csv_filename=csv_name,
        csv_bytes=raw,
    )


def attempt_date(d: date, output_dir: Path, force: bool, timeout: int) -> AttemptOutcome:
    url = build_url(d)
    date_dir = output_dir / "bhavcopy" / d.isoformat()
    zip_dir = date_dir / "raw"
    extracted_dir = date_dir / "extracted"
    zip_path = zip_dir / f"BhavCopy_NSE_CM_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip"

    print(f"Checking      : {d.isoformat()} ...", end=" ", flush=True)

    if zip_path.exists() and not force:
        result = validate_zip(zip_path.read_bytes(), d)
        if result.ok:
            print("VALID (cached)")
            return AttemptOutcome(True, d, result, zip_path, extracted_dir, url)
        print(f"cached copy invalid ({result.reason}), re-downloading ...", end=" ", flush=True)

    fetch = fetch_bytes(url, timeout)
    if not fetch.ok:
        print(f"NOT AVAILABLE ({fetch.error})")
        return AttemptOutcome(False, d, None, None, None, url)

    result = validate_zip(fetch.data, d)
    if not result.ok:
        print(f"INVALID ({result.reason})")
        return AttemptOutcome(False, d, result, None, None, url)

    zip_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(fetch.data)
    (extracted_dir / result.csv_filename).write_bytes(result.csv_bytes)

    print("VALID")
    return AttemptOutcome(True, d, result, zip_path, extracted_dir, url)


def write_metadata(
    output_dir: Path,
    execution_dt: datetime,
    mode: str,
    fallback_days: int,
    outcome: AttemptOutcome,
) -> None:
    meta = {
        "execution_date": execution_dt.isoformat(),
        "market_data_date": outcome.market_date.isoformat(),
        "selection_mode": mode,
        "fallback_days": fallback_days,
        "source_url": outcome.url,
        "source": "NSE",
        "zip_path": str(outcome.zip_path),
        "extracted_dir": str(outcome.extracted_dir),
        "csv_file": outcome.result.csv_filename,
        "record_count": outcome.result.record_count,
        "status": "SUCCESS",
    }
    (outcome.extracted_dir.parent / "metadata.json").write_text(json.dumps(meta, indent=2))
    latest_path = output_dir / "bhavcopy" / "latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(meta, indent=2))


def print_header(execution_dt: datetime, mode: str) -> None:
    print("=" * 56)
    print("NSE COM-UDiFF Common Bhavcopy Downloader")
    print("=" * 56)
    print()
    print(f"Execution Time : {execution_dt.strftime('%Y-%m-%d %H:%M:%S')} IST")
    print(f"Mode           : {mode}")
    print()


def print_success_summary(mode: str, fallback_days: int, outcome: AttemptOutcome) -> None:
    fallback_label = f"T-{fallback_days}" if mode == "LATEST" else "N/A"
    print()
    print(f"Selected Date : {outcome.market_date.isoformat()}")
    print(f"Fallback      : {fallback_label}")
    print("Source        : NSE")
    print("Status        : SUCCESS")
    print()
    print(f"ZIP           : {outcome.zip_path}")
    print(f"Extracted To  : {outcome.extracted_dir}/")
    print()
    print("Files         : 1")
    print(f"Records       : {outcome.result.record_count:,}")
    print("=" * 56)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download NSE COM-UDiFF Common Bhavcopy Final (ZIP) reports.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python nse_udiff_bhavcopy.py
  python nse_udiff_bhavcopy.py --date 2026-08-13
  python nse_udiff_bhavcopy.py --output ./data
  python nse_udiff_bhavcopy.py --date 2026-08-13 --output ./data
  python nse_udiff_bhavcopy.py --force
  python nse_udiff_bhavcopy.py --lookback-days 10
""",
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Specific market data date (YYYY-MM-DD). If omitted, the latest "
             "available report is auto-discovered.",
    )
    parser.add_argument(
        "--output", type=str, default="data",
        help="Output base directory (default: data)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force re-download even if a valid cached copy already exists",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"Max calendar days to search backwards in latest mode (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help=f"HTTP request timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output)
    execution_dt = datetime.now(IST)

    if args.date:
        try:
            requested = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: invalid --date value {args.date!r}, expected YYYY-MM-DD", file=sys.stderr)
            return 2

        print_header(execution_dt, "SPECIFIC")
        outcome = attempt_date(requested, output_dir, args.force, args.timeout)

        if not outcome.success:
            print()
            print(f"ERROR: COM-UDiFF Bhavcopy not available for {requested.isoformat()}")
            return 1

        write_metadata(output_dir, execution_dt, "specific", 0, outcome)
        print_success_summary("SPECIFIC", 0, outcome)
        return 0

    print_header(execution_dt, "LATEST")
    exec_date = execution_dt.date()
    selected: AttemptOutcome | None = None
    fallback_days = 0

    for i in range(args.lookback_days + 1):
        candidate = exec_date - timedelta(days=i)
        if candidate.weekday() >= 5:
            # Weekends never publish a bhavcopy; skip the request outright.
            # Holidays aren't special-cased - they simply 404 like any other
            # unavailable date and fall through the normal retry path below.
            print(f"Checking      : {candidate.isoformat()} ... SKIPPED (weekend)")
            continue
        outcome = attempt_date(candidate, output_dir, args.force, args.timeout)
        if outcome.success:
            selected = outcome
            fallback_days = i
            break

    if selected is None:
        print()
        print(f"ERROR: No valid COM-UDiFF Bhavcopy found in the last {args.lookback_days} calendar day(s)")
        return 1

    write_metadata(output_dir, execution_dt, "latest", fallback_days, selected)
    print_success_summary("LATEST", fallback_days, selected)
    return 0


if __name__ == "__main__":
    sys.exit(main())
