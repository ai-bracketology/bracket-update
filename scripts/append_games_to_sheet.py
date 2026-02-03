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

def main():
    ap = argparse.ArgumentParser(description="Append games CSV rows into a Google Sheet tab (columns A–F only).")
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--tab", default="Games")
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str).fillna("")
    if df.empty:
        print("CSV is empty. Nothing to append.")
        return

    missing = [c for c in CSV_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")

    # Reorder and restrict to exactly A–F fields
    df = df[CSV_COLS].copy()

    gc = get_client()
    sh = gc.open_by_key(args.sheet_id)
    ws = sh.worksheet(args.tab)

    # Determine first empty row in column A
    col_a = ws.col_values(1)  # up to last non-empty cell in col A
    start_row = len(col_a) + 1

    # Convert dataframe to list-of-lists
    rows = []
    for _, r in df.iterrows():
        rows.append([normalize(r[c]) for c in CSV_COLS])

    # Append rows (adds at bottom; doesn't overwrite existing rows)
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    print(f"✅ Opened sheet: {sh.title}")
    print(f"✅ Tab: {args.tab}")
    print(f"✅ Column A last non-empty row was: {len(col_a)}")
    print(f"✅ Appended {len(rows)} row(s), starting at row: {start_row}")
    print("✅ Wrote into columns A–F only (date..site_designation).")

if __name__ == "__main__":
    main()
