#!/usr/bin/env python3
import argparse
import requests
import pandas as pd

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"

def safe_get(d, path, default=None):
    cur = d
    try:
        for p in path:
            cur = cur[p]
        return cur
    except Exception:
        return default

def clean(s):
    if s is None:
        return ""
    s = str(s).strip()
    return "" if s.lower() in ("", "nan", "none", "null") else s

def combine_location(venue: dict) -> str:
    name  = clean(safe_get(venue, ["fullName"]))
    city  = clean(safe_get(venue, ["address", "city"]))
    state = clean(safe_get(venue, ["address", "state"]))
    place = ", ".join([p for p in (city, state) if p])
    return f"{name} — {place}" if name and place else (name or place)

def fetch_scoreboard_all(date_yyyymmdd: str, timeout=20, page_size=200) -> dict:
    """
    Pull ALL Division I events for a given date by paging ESPN's scoreboard.
    - groups=50 => NCAA Division I
    - limit/offset paging until no new events
    - tz pinned so 'dates' aligns to ET
    """
    session = requests.Session()
    all_events, seen = [], set()
    offset = 0

    while True:
        params = {
            "dates": date_yyyymmdd,
            "groups": 50,              # NCAA Division I
            "limit": page_size,
            "offset": offset,
            "tz": "America/New_York",
        }
        r = session.get(ESPN_SCOREBOARD, params=params,
                        headers={"User-Agent": "Mozilla/5.0 (CBB scraper)"},
                        timeout=timeout)
        r.raise_for_status()
        data = r.json()
        events = data.get("events", []) or []
        if not events:
            break

        new = 0
        for ev in events:
            ev_id = ev.get("id")
            if ev_id and ev_id not in seen:
                seen.add(ev_id)
                all_events.append(ev)
                new += 1

        if new == 0 or len(events) < page_size:
            break

        offset += page_size

    return {"events": all_events}

def parse_completed_games(sb_json: dict):
    """
    Yield dicts for completed games with keys:
    home_team, home_score, away_team, away_score, is_neutral, location
    """
    events = sb_json.get("events", []) or []
    for ev in events:
        comps = ev.get("competitions", []) or []
        if not comps:
            continue
        comp = comps[0]

        completed = bool(safe_get(comp, ["status", "type", "completed"], False))
        if not completed:
            continue

        competitors = comp.get("competitors", []) or []
        if len(competitors) != 2:
            continue

        neutral = bool(comp.get("neutralSite", False))
        venue = comp.get("venue", {}) or {}
        location = combine_location(venue)

        rec = {"is_neutral": neutral, "location": location}
        for c in competitors:
            side = c.get("homeAway")  # 'home' or 'away'
            team_name = clean(safe_get(c, ["team", "displayName"]))
            try:
                score = int(c.get("score")) if c.get("score") is not None else None
            except Exception:
                score = None

            if side == "home":
                rec["home_team"] = team_name
                rec["home_score"] = score
            elif side == "away":
                rec["away_team"] = team_name
                rec["away_score"] = score

        if any(k not in rec for k in ("home_team", "away_team", "home_score", "away_score")):
            continue
        if rec["home_score"] is None or rec["away_score"] is None:
            continue

        yield rec

def build_rows(games_iter):
    rows = []
    for g in games_iter:
        home_win = g["home_score"] > g["away_score"]
        winner_team  = g["home_team"] if home_win else g["away_team"]
        winner_score = max(g["home_score"], g["away_score"])
        loser_team   = g["away_team"] if home_win else g["home_team"]
        loser_score  = min(g["home_score"], g["away_score"])

        if g["is_neutral"]:
            site = "N"
        else:
            site = "H" if home_win else "A"

        rows.append({
            "winner_team":  winner_team,
            "winner_score": winner_score,
            "loser_team":   loser_team,
            "loser_score":  loser_score,
            "location":     g["location"],
            "site_designation": site
        })
    return rows

def scrape_one_day(date_iso: str):
    """Return a DataFrame of completed D-I games for a single ISO date."""
    date_compact = date_iso.replace("-", "")
    display_date = pd.to_datetime(date_iso).strftime("%m-%d-%Y")

    sb = fetch_scoreboard_all(date_compact)
    games = list(parse_completed_games(sb))
    rows = build_rows(games)

    df = pd.DataFrame(rows, columns=[
        "winner_team","winner_score","loser_team","loser_score","site_designation"
    ])
    if not df.empty:
        df.insert(0, "date", display_date)
    return df

def main():
    ap = argparse.ArgumentParser(description="Scrape ALL D-I games from ESPN for a single date or date range and save CSV.")
    ap.add_argument("date", help="Start date in YYYY-MM-DD (e.g., 2025-11-03)")
    ap.add_argument("--end-date", help="End date in YYYY-MM-DD (inclusive). If omitted, only the start date is scraped.")
    ap.add_argument("-o", "--out", help="Combined output CSV (default: update_for_YYYYMMDD.csv or update_for_YYYYMMDD_YYYYMMDD.csv for ranges)")
    ap.add_argument("--per-day", action="store_true", help="Also write a per-day CSV for each date")
    args = ap.parse_args()

    start_iso = args.date
    end_iso = args.end_date or args.date

    # Build the list of ISO dates (inclusive)
    dates = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_iso, end_iso, freq="D")]

    all_frames = []
    total_games = 0

    for d_iso in dates:
        df_day = scrape_one_day(d_iso)
        if args.per_day:
            # write per-day file
            compact = d_iso.replace("-", "")
            out_day = f"update_for_{compact}.csv"
            if df_day is None or df_day.empty:
                print(f"{d_iso}: 0 completed D-I games")
            else:
                df_day.to_csv(out_day, index=False)
                print(f"{d_iso}: Saved {out_day} with {len(df_day)} completed D-I games")
        if df_day is not None and not df_day.empty:
            all_frames.append(df_day)
            total_games += len(df_day)

    if not all_frames:
        print(f"No completed D-I games found for range {start_iso} to {end_iso}.")
        return

    combined = pd.concat(all_frames, ignore_index=True)

    # Choose output name
    if args.out:
        out_path = args.out
    else:
        if start_iso == end_iso:
            out_path = f"update_for_{start_iso.replace('-','')}.csv"
        else:
            out_path = f"update_for_{start_iso.replace('-','')}_{end_iso.replace('-','')}.csv"

    combined.to_csv(out_path, index=False)
    print(f"Saved combined {out_path} with {total_games} completed D-I games across {len(dates)} day(s).")
    if not combined.empty:
        print(combined.head())


if __name__ == "__main__":
    main()
