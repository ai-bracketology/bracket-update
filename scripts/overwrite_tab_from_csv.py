#!/usr/bin/env python3
import argparse
import json
import os

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_client():
    sa_json = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    return gspread.authorize(creds)

def norm(x):
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none", "null") else s

def main():
    ap = argparse.ArgumentParser(description="Overwrite a sheet tab with CSV contents starting at A1.")
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--tab", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--clear-below", type=int, default=500,
                    help="How many extra rows to blank out below new data (default 500).")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str).fillna("")
    gc = get_client()
    sh = gc.open_by_key(args.sheet_id)
    ws = sh.worksheet(args.tab)

    # Build values: header row + data rows
    values = [list(df.columns)] + df.applymap(norm).values.tolist()

    end_row = len(values)
    end_col = len(values[0])

    # Convert column count to A1 notation end column
    # gspread can update by range like "A1:ZZ1000" as long as you compute the end col.
    def col_letter(n: int) -> str:
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    end_col_letter = col_letter(end_col)
    rng = f"A1:{end_col_letter}{end_row}"

    # Clear then write (values only)
    ws.clear()
    ws.update(rng, values, value_input_option="USER_ENTERED")

    # Optional: blank some extra rows below in case yesterday had more rows/cols
    if args.clear_below > 0:
        blank_start = end_row + 1
        blank_end = end_row + args.clear_below
        blank_rng = f"A{blank_start}:{end_col_letter}{blank_end}"
        ws.update(blank_rng, [[""] * end_col] * args.clear_below, value_input_option="USER_ENTERED")

    print(f"✅ Overwrote tab '{args.tab}' with {len(df)} rows and {len(df.columns)} cols.")
    print(f"✅ Wrote range: {rng}")

if __name__ == "__main__":
    main()
