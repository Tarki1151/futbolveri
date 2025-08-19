import os
import time
from typing import Dict, Generator, List, Optional

import requests

API_BASE = "https://v3.football.api-sports.io"


def _session(api_key: Optional[str] = None) -> requests.Session:
    key = api_key or os.getenv("API_FOOTBALL_COM")
    if not key:
        raise RuntimeError("API_FOOTBALL_COM is not set in environment")
    s = requests.Session()
    s.headers.update({
        "x-apisports-key": key,
        "Accept": "application/json",
    })
    return s


def _get_json(session: requests.Session, url: str, params: Optional[dict] = None, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        r = session.get(url, params=params, timeout=30)
        if r.status_code == 429:
            # obey header if present
            delay = int(r.headers.get("Retry-After", "10"))
            time.sleep(delay)
            continue
        r.raise_for_status()
        j = r.json()
        return j
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


def list_current_leagues(country: Optional[str] = None, api_key: Optional[str] = None) -> List[Dict]:
    """Return current leagues (type=league). Optionally filter by country name.

    Items contain keys: league{ id, name, type }, country{ name, code }, seasons[{year, current} ...]
    """
    s = _session(api_key)
    params = {"current": "true", "type": "league"}
    if country:
        params["country"] = country
    data = _get_json(s, f"{API_BASE}/leagues", params=params)
    return data.get("response", [])


def get_current_season_year(league_item: Dict) -> Optional[int]:
    seasons = league_item.get("seasons") or []
    for s in seasons:
        if s.get("current"):
            return s.get("year")
    # fallback: latest year
    if seasons:
        years = [s.get("year") for s in seasons if s.get("year")]
        return max(years) if years else None
    return None


_SECOND_TIER_PATTERNS = (
    " 2", " 2.", "II", "2nd", "Second", "Segunda", "Serie B", "Liga 2",
    "2. Bundesliga", "Ligue 2", "Eerste Divisie", "B Nacional", "National League",
    "Primera B", "Superettan", "OBOS-ligaen", "Championship", "J2", "J.2",
)


def looks_like_first_division(name: str) -> bool:
    if not name:
        return False
    lname = name.lower()
    # exclude common second-tier markers
    for p in _SECOND_TIER_PATTERNS:
        if p.lower() in lname:
            return False
    return True


def pick_country_first_division(leagues: List[Dict]) -> Optional[Dict]:
    """Heuristically pick one top division from a list of leagues (same country).
    Strategy: filter out obvious second tiers by name; choose the one with the longest
    continuous history (more seasons) or the first if equal.
    """
    candidates = []
    for item in leagues:
        name = (item.get("league") or {}).get("name") or ""
        if looks_like_first_division(name):
            candidates.append(item)
    if not candidates:
        candidates = leagues
    # sort by number of seasons desc, then by league id asc
    def keyfn(x):
        seasons = x.get("seasons") or []
        return (len(seasons), -int((x.get("league") or {}).get("id") or 0))
    candidates.sort(key=keyfn, reverse=True)
    return candidates[0] if candidates else None


def list_league_teams(league_id: int, season_year: int, api_key: Optional[str] = None) -> List[Dict]:
    s = _session(api_key)
    params = {"league": league_id, "season": season_year}
    data = _get_json(s, f"{API_BASE}/teams", params=params)
    return data.get("response", [])


def iter_country_first_division_teams(country: str, api_key: Optional[str] = None) -> Generator[Dict, None, None]:
    """Yield normalized teams for the country's top division (heuristic) for current season.

    Normalized fields:
      provider: 'api-football'
      provider_team_id: int
      name, short_name, founded
      country_code, country_name
    """
    leagues = list_current_leagues(country=country, api_key=api_key)
    if not leagues:
        return
    top = pick_country_first_division(leagues)
    if not top:
        return
    league = top.get("league") or {}
    l_id = league.get("id")
    season = get_current_season_year(top)
    if not l_id or not season:
        return
    teams = list_league_teams(l_id, season, api_key)
    for t in teams:
        team = t.get("team") or {}
        country_info = t.get("team") or {}
        yield {
            "provider": "api-football",
            "provider_team_id": team.get("id"),
            "name": team.get("name"),
            "short_name": team.get("code") or team.get("name"),
            "founded": team.get("founded"),
            "country_code": (t.get("country") or {}).get("code") or None,
            "country_name": (t.get("country") or {}).get("name") or None,
        }


def search_teams(query: str, api_key: Optional[str] = None) -> List[Dict]:
    """Search teams by free-text query.

    Returns items under response[], each with team{id,name,code}, country{name, code}
    """
    s = _session(api_key)
    data = _get_json(s, f"{API_BASE}/teams", params={"search": query})
    return data.get("response", [])


def recent_fixtures(team_id: int, last_n: int = 10, api_key: Optional[str] = None) -> List[Dict]:
    """Return recent finished fixtures for a team (most recent first)."""
    s = _session(api_key)
    params = {"team": team_id, "last": last_n, "status": "FT"}
    data = _get_json(s, f"{API_BASE}/fixtures", params=params)
    return data.get("response", [])
