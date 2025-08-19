import os
import time
from typing import Dict, Generator, List, Optional

import requests

API_BASE = "https://api.football-data.org/v4"


def _session(api_key: Optional[str] = None) -> requests.Session:
    """Create a requests session with auth header set."""
    key = api_key or os.getenv("FOOTBALL_API")
    if not key:
        raise RuntimeError("FOOTBALL_API is not set in environment")
    s = requests.Session()
    s.headers.update({"X-Auth-Token": key})
    return s


def _get_json(session: requests.Session, url: str, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        r = session.get(url, timeout=30)
        if r.status_code == 429:
            delay = int(r.headers.get("Retry-After", "10"))
            time.sleep(delay)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


def list_tier_one_competitions(api_key: Optional[str] = None) -> List[Dict]:
    """Return all competitions under plan=TIER_ONE.

    Response items contain at least: id, name, area{name, code}.
    """
    s = _session(api_key)
    data = _get_json(s, f"{API_BASE}/competitions?plan=TIER_ONE")
    return data.get("competitions", [])


def list_competition_teams(competition_id: int, api_key: Optional[str] = None) -> List[Dict]:
    """Return teams for a given competition id.

    Team items contain: id, name, shortName, tla, founded, area{name, code}.
    """
    s = _session(api_key)
    data = _get_json(s, f"{API_BASE}/competitions/{competition_id}/teams")
    return data.get("teams", [])


def iter_all_tier_one_teams(api_key: Optional[str] = None) -> Generator[Dict, None, None]:
    """Yield normalized team dicts from all TIER_ONE competitions.

    Normalized fields:
      provider: 'football-data'
      provider_team_id: int
      name: str
      short_name: Optional[str]
      founded: Optional[int]
      country_code: Optional[str]
      country_name: Optional[str]
    """
    comps = list_tier_one_competitions(api_key)
    s = _session(api_key)
    seen_ids = set()
    for comp in comps:
        cid = comp.get("id")
        cname = comp.get("name")
        if not cid:
            continue
        try:
            teams = _get_json(s, f"{API_BASE}/competitions/{cid}/teams").get("teams", [])
        except Exception as e:
            print(f"Warn: teams fetch failed for competition {cid} {cname}: {e}")
            continue
        for t in teams:
            tid = t.get("id")
            if not tid or tid in seen_ids:
                continue
            seen_ids.add(tid)
            area = t.get("area") or {}
            yield {
                "provider": "football-data",
                "provider_team_id": tid,
                "name": t.get("name") or t.get("shortName") or t.get("tla"),
                "short_name": t.get("shortName") or t.get("tla"),
                "founded": t.get("founded"),
                "country_code": area.get("code"),
                "country_name": area.get("name"),
            }
