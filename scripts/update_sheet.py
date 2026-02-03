import argparse
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def load_client():
    sa_json = json.loads(__import__("os").environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    return gspread.authorize(creds)

def overwrite_tab(ws, df: pd.DataFrame):
    # Clear then write header + values
    ws.clear()
    values = [df.columns.tolist()] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    ws.update(values)  # gspread will batch this under the hood for modest sizes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--games-csv", required=True)
    ap.add_argument("--kenpom-csv", required=True)
    ap.add_argument("--games-tab", required=True)
    ap.add_argument("--kenpom-tab", required=True)
    args = ap.parse_args()

    gc = load_client()
    sh = gc.open_by_key(args.sheet_id)

    games_df = pd.read_csv(args.games_csv)
    kp_df = pd.read_csv(args.kenpom_csv)

    games_ws = sh.worksheet(args.games_tab)
    kp_ws = sh.worksheet(args.kenpom_tab)

    overwrite_tab(games_ws, games_df)
    overwrite_tab(kp_ws, kp_df)

if __name__ == "__main__":
    main()
