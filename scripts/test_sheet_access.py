#!/usr/bin/env python3
import json
import os

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_ID = os.environ["SHEET_ID"]
TAB_NAME = os.environ.get("TAB_NAME", "Games")

def main():
    sa_json = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    # Find last non-empty row in column A
    col_a = ws.col_values(1)  # returns values up to last non-empty cell
    last_row = len(col_a)

    print(f"✅ Opened sheet: {sh.title}")
    print(f"✅ Opened tab: {TAB_NAME}")
    print(f"✅ Column A last non-empty row: {last_row}")
    print(f"✅ Header (row 1): {ws.row_values(1)}")

    # Print last 5 rows of the first 6 columns (A:F), if available
    start = max(2, last_row - 4)
    end = last_row
    rng = f"A{start}:F{end}"
    tail = ws.get(rng)

    print(f"\nLast rows preview ({rng}):")
    for r in tail:
        print(r)

if __name__ == "__main__":
    main()
