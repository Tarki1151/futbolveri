import os
import time
import json
import getpass
from typing import Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


API_BASE = "https://api.football-data.org/v4"


def load_config() -> Tuple[str, str, int, str]:
    load_dotenv()
    api_key = os.getenv("FOOTBALL_API")
    if not api_key:
        raise RuntimeError("FOOTBALL_API is not set in .env")
    dbname = os.getenv("PGDATABASE", "futbol")
    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", getpass.getuser())
    return api_key, dbname, port, user if host else "127.0.0.1"


def db_connect(dbname: str, port: int, user: str):
    conn = psycopg2.connect(dbname=dbname, host="127.0.0.1", port=port, user=user)
    conn.autocommit = False
    return conn


def upsert_country(cur, code: Optional[str], name: Optional[str]) -> Optional[int]:
    if not name and not code:
        return None
    # Normalize
    code = (code or "").strip()[:3] or None
    name = (name or "").strip() or None

    # If neither present, skip
    if not code and not name:
        return None

    if code:
        cur.execute(
            """
            INSERT INTO ulkeler (kod, isim)
            VALUES (%s, COALESCE(%s, %s))
            ON CONFLICT (kod) DO UPDATE SET isim = EXCLUDED.isim
            RETURNING id
            """,
            (code, name, name),
        )
    else:
        # No code, try find by name first
        cur.execute("SELECT id, kod FROM ulkeler WHERE LOWER(isim) = LOWER(%s)", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            """
            INSERT INTO ulkeler (kod, isim)
            VALUES (substring(%s from 1 for 3), %s)
            ON CONFLICT (kod) DO UPDATE SET isim = EXCLUDED.isim
            RETURNING id
            """,
            (name.upper(), name),
        )
    return cur.fetchone()[0]


def upsert_team(cur, key: str, name: str, short_name: Optional[str], founded: Optional[int], country_id: Optional[int]):
    cur.execute(
        """
        INSERT INTO takimlar (anahtar, isim, kisa_isim, kurulus_yili, ulke_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (anahtar) DO UPDATE
        SET isim = EXCLUDED.isim,
            kisa_isim = EXCLUDED.kisa_isim,
            kurulus_yili = EXCLUDED.kurulus_yili,
            ulke_id = COALESCE(EXCLUDED.ulke_id, takimlar.ulke_id)
        """,
        (key, name, short_name, founded, country_id),
    )


def get_json(session: requests.Session, url: str, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        resp = session.get(url, timeout=30)
        if resp.status_code == 429:
            # simple backoff
            retry_after = int(resp.headers.get("Retry-After", "10"))
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


def main():
    api_key, dbname, port, user = load_config()

    with db_connect(dbname, port, user) as conn:
        with conn.cursor() as cur:
            session = requests.Session()
            session.headers.update({"X-Auth-Token": api_key})

            # Fetch all top-tier competitions
            comps = get_json(session, f"{API_BASE}/competitions?plan=TIER_ONE")
            competitions = comps.get("competitions", [])
            print(f"Found {len(competitions)} TIER_ONE competitions")

            imported = 0
            seen_team_ids = set()

            for comp in competitions:
                comp_id = comp.get("id")
                comp_name = comp.get("name")
                if not comp_id:
                    continue
                try:
                    teams_res = get_json(session, f"{API_BASE}/competitions/{comp_id}/teams")
                except Exception as e:
                    print(f"Warn: failed teams for competition {comp_id} {comp_name}: {e}")
                    continue

                teams = teams_res.get("teams", [])
                print(f"{comp_name}: {len(teams)} teams")

                for t in teams:
                    tid = t.get("id")
                    if not tid:
                        continue
                    if tid in seen_team_ids:
                        # already handled from another competition
                        continue
                    seen_team_ids.add(tid)

                    t_name = t.get("name") or t.get("shortName") or t.get("tla")
                    short_name = t.get("shortName") or t.get("tla")
                    founded = t.get("founded")

                    area = t.get("area") or {}
                    country_code = area.get("code")  # often 3-letter like ENG, ESP, etc.
                    country_name = area.get("name")

                    country_id = upsert_country(cur, country_code, country_name)

                    team_key = f"fd:team:{tid}"
                    upsert_team(cur, team_key, t_name, short_name, founded, country_id)
                    imported += 1

                # commit per competition to reduce loss on failures
                conn.commit()

            print(f"Imported/updated teams: {imported}")


if __name__ == "__main__":
    main()
