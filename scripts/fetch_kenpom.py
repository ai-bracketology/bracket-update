#!/usr/bin/env python3
import argparse
import os
import pandas as pd

from kenpompy.utils import login
import kenpompy.summary as kp

def main():
    ap = argparse.ArgumentParser(description="Fetch KenPom efficiency table via kenpompy and save raw CSV.")
    ap.add_argument("--out", default="kenpom.csv")
    ap.add_argument("--season", type=int, default=None, help="Season year (e.g., 2026). Omit for current season.")
    args = ap.parse_args()

    user = os.environ.get("KENPOM_USER")
    pw = os.environ.get("KENPOM_PASS")
    if not user or not pw:
        raise RuntimeError("Missing KENPOM_USER or KENPOM_PASS env vars.")

    browser = login(user, pw)

    if args.season is None:
        df = kp.get_efficiency(browser)
    else:
        # kenpompy accepts season as str/int depending on version; str is safest
        df = kp.get_efficiency(browser, season=str(args.season))

    if df is None or df.empty:
        raise RuntimeError("KenPom efficiency table returned empty. Login may have failed or page format changed.")

    df.to_csv(args.out, index=False)
    print(f"✅ Saved raw KenPom CSV: {args.out}")
    print(f"   rows={len(df)} cols={len(df.columns)}")
    print(f"   columns={list(df.columns)[:12]}{' ...' if len(df.columns) > 12 else ''}")

if __name__ == "__main__":
    main()

