#!/usr/bin/env python3
import argparse
import json
import os
from typing import List, Tuple, Set

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

CSV_COLS = [
    "date",
    "winner_team",
    "winner_score",
    "loser_team",
    "loser_score",
    "site_designation",
]

def get_client():
    sa_json = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    return gspread.authorize(creds)

def normalize(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none", "null") else s

def row_key(vals: List[str]) -> Tuple[str, ...]:
    # Use all 6 A–F cells as the identity of a row
    return tuple(normalize(v) for v in vals)

def last_nonempty_row_in_col_a(ws) -> int:
    """
    Robustly find last non-empty row in column A by scanning A:A.
    Avoids issues when other columns have formulas far down.
    """
    col = ws.get("A:A")  # list of rows, each like ["value"]
    last = 0
    for i, row in enumerate(col, start=1):
        v = row[0] if row else ""
        if normalize(v) != "":
            last = i
    return last

def main():
    ap = argparse.ArgumentParser(description="Append games.csv into first blank row of col A (A–F only), with lookback dedupe + verify.")
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--tab", default="Games")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--dedupe-lookback", type=int, default=300,
                    help="How many existing rows (A–F) to scan from the bottom for duplicates (default: 300).")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str).fillna("")
    if df.empty:
        print("CSV is empty. Nothing to write.")
        return

    missing = [c for c in CSV_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")

    df = df[CSV_COLS].copy()

    gc = get_client()
    sh = gc.open_by_key(args.sheet_id)
    ws = sh.worksheet(args.tab)

    last_a = last_nonempty_row_in_col_a(ws)
    start_row = last_a + 1

    # ----- Build existing keys from last N rows (A–F) -----
    lookback = max(0, int(args.dedupe_lookback))
    existing_keys: Set[Tuple[str, ...]] = set()

    if last_a > 0 and lookback > 0:
        lb_start = max(1, last_a - lookback + 1)
        lb_range = f"A{lb_start}:F{last_a}"
        existing_rows = ws.get(lb_range)

        # Ensure each row is length 6
        for r in existing_rows:
            r6 = (r + [""] * 6)[:6]
            existing_keys.add(row_key(r6))

    # ----- Prepare new rows, skipping duplicates -----
    csv_rows = []
    dupes = 0
    for _, r in df.iterrows():
        vals = [normalize(r[c]) for c in CSV_COLS]
        k = row_key(vals)
        if k in existing_keys:
            dupes += 1
            continue
        csv_rows.append(vals)

    if not csv_rows:
        print(f"✅ Opened sheet: {sh.title}")
        print(f"✅ Tab: {args.tab}")
        print(f"✅ Last non-empty row in column A: {last_a}")
        print(f"✅ Dedupe lookback: {lookback} row(s)")
        print(f"✅ CSV rows: {len(df)} | duplicates skipped: {dupes} | new rows: 0")
        print("✅ Nothing to append.")
        return

    end_row = start_row + len(csv_rows) - 1
    write_range = f"A{start_row}:F{end_row}"

    # Write exactly where intended
    ws.update(write_range, csv_rows, value_input_option="USER_ENTERED")

    # Verify by reading back
    written_back = ws.get(write_range)

    print(f"✅ Opened sheet: {sh.title}")
    print(f"✅ Tab: {args.tab}")
    print(f"✅ Last non-empty row in column A: {last_a}")
    print(f"✅ Dedupe lookback: {lookback} row(s)")
    print(f"✅ CSV rows: {len(df)} | duplicates skipped: {dupes} | new rows written: {len(csv_rows)}")
    print(f"✅ Intended write range: {write_range}")
    print(f"✅ Rows read back from sheet: {len(written_back)}")

    print("\n🔎 First 3 rows read back:")
    for r in written_back[:3]:
        print(r)
    print("\n🔎 Last 3 rows read back:")
    for r in written_back[-3:]:
        print(r)

if __name__ == "__main__":
    main()

