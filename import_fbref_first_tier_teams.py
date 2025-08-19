import argparse
import os
import time
import random
from typing import Optional, Set, List, Dict

from dotenv import load_dotenv

from clients.fbref import list_first_tier_competitions, list_competition_teams
from db.util import db_connect, upsert_country, upsert_team


def main(limit: Optional[int] = None):
    # Load env for DB connection variables
    load_dotenv()

    comps = list_first_tier_competitions()
    total_all = len(comps)
    print(f"Found {total_all} competitions.", flush=True)

    # Apply start-after filters (by name or by index)
    if ARGS.start_after_name:
        key = ARGS.start_after_name.strip().casefold()
        idx = next((i for i, c in enumerate(comps) if key in str(c.get("comp_name", "")).casefold()), -1)
        if idx >= 0:
            comps = comps[idx + 1 :]
            print(f"Skipping until after name match '{ARGS.start_after_name}' -> starting at index {idx+2}.", flush=True)
        else:
            print(f"Warn: start-after-name '{ARGS.start_after_name}' not found.", flush=True)
    if ARGS.start_after_index is not None:
        skip = max(0, ARGS.start_after_index)
        comps = comps[skip:]
        print(f"Skipping first {skip} competitions by index.", flush=True)

    # Resume support: skip already processed competitions using a progress file
    progress_path = ARGS.progress_file
    done: Set[str] = set()
    if not ARGS.no_resume and os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                done = {line.strip() for line in f if line.strip()}
        except Exception as e:
            print(f"Warn: could not read progress file {progress_path}: {e}", flush=True)
    if done:
        before = len(comps)
        comps = [c for c in comps if c.get("season_url") not in done]
        print(f"Resume: skipping {before - len(comps)} already completed competitions from {progress_path}.", flush=True)
    else:
        if ARGS.no_resume:
            print("Resume disabled: processing from start.", flush=True)
        else:
            print("No existing progress file or it's empty: starting fresh.", flush=True)

    # Apply limit to number of new competitions to process
    if limit is not None:
        comps = comps[: max(0, limit)]
        print(f"Limiting to next {len(comps)} competitions.", flush=True)

    total_inserted = 0
    processed = 0
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                for idx, comp in enumerate(comps, start=1):
                    comp_name = comp.get("comp_name")
                    iso3 = comp.get("country_iso3")
                    season_url = comp.get("season_url")
                    if not season_url:
                        continue
                    print(f"[{idx}/{len(comps)}] Processing: {comp_name} ({iso3}) -> {season_url}", flush=True)

                    # Upsert country by ISO3 (name will be resolved via pycountry if missing)
                    country_id = upsert_country(cur, iso3, None)

                    # Parse teams on the competition season page
                    try:
                        teams = list_competition_teams(season_url)
                    except Exception as e:
                        print(f"  Warn: failed to parse teams for {comp_name}: {e}", flush=True)
                        conn.commit()
                        continue

                    print(f"  Found {len(teams)} teams", flush=True)
                    for j, name in enumerate(teams, start=1):
                        # FBref team list here doesn't provide short_name/founded; use None
                        upsert_team(cur, country_id, iso3, name, None, None)
                        total_inserted += 1
                        if j == len(teams) or j % 10 == 0:
                            print(f"    Upserted {j}/{len(teams)}", flush=True)
                    conn.commit()
                    # Mark this competition as done in the progress file (append)
                    if not ARGS.no_resume:
                        try:
                            with open(progress_path, "a", encoding="utf-8") as f:
                                f.write(season_url + "\n")
                        except Exception as e:
                            print(f"Warn: could not update progress file {progress_path}: {e}", flush=True)
                    processed += 1
                    # Polite delay between competitions to avoid 429
                    pause = random.uniform(0.8, 1.6)
                    print(f"  Committed. Sleeping {pause:.1f}s...", flush=True)
                    time.sleep(pause)
    except KeyboardInterrupt:
        print("Interrupted by user. Writing partial progress summary...", flush=True)
    finally:
        print(f"Done. Competitions processed: {processed}/{len(comps)}. Imported/updated team rows: {total_inserted}", flush=True)

    print(f"Done. Imported/updated team rows: {total_inserted}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Import first-tier teams from FBref into DB (resumable)")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of competitions to import on this run")
    ap.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume; ignore and do not update the progress file",
    )
    ap.add_argument(
        "--progress-file",
        type=str,
        default=".fbref_first_tier_teams.progress",
        help="Path to progress file storing completed competition season URLs",
    )
    ap.add_argument(
        "--start-after-name",
        type=str,
        default=None,
        help="Skip all competitions up to and including the first whose name contains this value (case-insensitive)",
    )
    ap.add_argument(
        "--start-after-index",
        type=int,
        default=None,
        help="Skip the first N competitions (after any name filter)",
    )
    global ARGS  # used by main()
    ARGS = ap.parse_args()
    main(limit=ARGS.limit)
