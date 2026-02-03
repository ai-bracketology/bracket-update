#!/usr/bin/env python3
import argparse
import os

import pandas as pd

from kenpompy.utils import login
import kenpompy.summary as kp


def main():
    ap = argparse.ArgumentParser(description="Download KenPom efficiency table via kenpompy and save CSV.")
    ap.add_argument("--out", default="kenpom.csv", help="Output CSV path (default: kenpom.csv)")
    ap.add_argument("--season", type=int, default=None, help="Season year (optional). Omit for current season.")
    args = ap.parse_args()

    user = os.environ.get("KENPOM_USER")
    pw = os.environ.get("KENPOM_PASS")
    if not user or not pw:
        raise RuntimeError("Missing KENPOM_USER or KENPOM_PASS env vars.")

    # Login returns an authenticated browser object
    browser = login(user, pw)

    # Pull efficiency/tempo stats table (summary page)
    if args.season is None:
        df = kp.get_efficiency(browser)
    else:
        df = kp.get_efficiency(browser, season=args.season)

    # Basic cleanup: ensure it's a normal DataFrame and save
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise RuntimeError("KenPom efficiency table returned empty. Login may have failed or page format changed.")

    df.to_csv(args.out, index=False)
    print(f"Saved KenPom efficiency CSV: {args.out} ({len(df)} teams, {len(df.columns)} cols)")
    print(df.head(3))


if __name__ == "__main__":
    main()
