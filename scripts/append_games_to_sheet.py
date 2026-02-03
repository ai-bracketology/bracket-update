#!/usr/bin/env python3
import argparse
import json
import os

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

def last_nonempty_row_in_col_a(ws) -> int:
    """
    Robustly find last non-empty row in column A by scanning values in A:A.
    This avoids the "used range" / formula-filled columns problem.
    """
    col = ws.get("A:A")  # list of rows, each like ["value"]
    last = 0
    for i, row in enumerate(col, start=1):
        v = row[0] if row else ""
        if normalize(v) != "":
            last = i
    return last

def main():
    ap = argparse.ArgumentParser(description="Write games.csv into first blank row of col A (A–F only) and verify.")
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--tab", default="Games")
    ap.add_argument("--csv", required=True)
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

    rows = []
    for _, r in df.iterrows():
        rows.append([normalize(r[c]) for c in CSV_COLS])

    end_row = start_row + len(rows) - 1
    rng = f"A{start_row}:F{end_row}"

    # Write exactly where we intend
    ws.update(rng, rows, value_input_option="USER_ENTERED")

    # Read back what we wrote (verification)
    written_back = ws.get(rng)

    print(f"✅ Opened sheet: {sh.title}")
    print(f"✅ Tab: {args.tab}")
    print(f"✅ Last non-empty row in column A: {last_a}")
    print(f"✅ Intended write range: {rng}")
    print(f"✅ Rows in CSV: {len(rows)}")
    print(f"✅ Rows read back from sheet: {len(written_back)}")
    print("\n🔎 First 3 rows read back:")
    for r in written_back[:3]:
        print(r)
    print("\n🔎 Last 3 rows read back:")
    for r in written_back[-3:]:
        print(r)

if __name__ == "__main__":
    main()
