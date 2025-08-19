import os
import json
import re
import time
from typing import Dict, Generator, List, Optional, Set

import requests
from bs4 import BeautifulSoup, Comment
from urllib.parse import urlparse
import random

try:
    import requests_cache  # type: ignore
except Exception:  # pragma: no cover - optional dep
    requests_cache = None

COMPS_INDEX = "https://fbref.com/en/comps/"

# Max seconds to wait for a single Retry-After before skipping (can override via env)
MAX_RETRY_AFTER = float(os.getenv("FBREF_MAX_RETRY_AFTER", "300"))

# HTTP cache settings (optional)
CACHE_ENABLED = os.getenv("FBREF_CACHE_ENABLED", "1").lower() not in ("0", "false", "no")
CACHE_TTL = int(os.getenv("FBREF_CACHE_TTL", "86400"))  # seconds
CACHE_NAME = os.getenv("FBREF_CACHE_NAME", ".http_cache/fbref")

# Local competitions list fallback cache
COMPS_CACHE_FILE = os.getenv("FBREF_COMPS_CACHE_FILE", ".http_cache/competitions.json")
COMPS_CACHE_TTL = int(os.getenv("FBREF_COMPS_CACHE_TTL", "604800"))  # 7 days

# Stealth-ish options
DEFAULT_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
UA_POOL = [
    DEFAULT_UA,
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
]
LANG_POOL = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "de-DE,de;q=0.9",
    "tr-TR,tr;q=0.9",
]
UA_OVERRIDE = os.getenv("FBREF_USER_AGENT")
UA_ROTATE = os.getenv("FBREF_UA_ROTATE", "0").lower() in ("1", "true", "yes")
LANG_ROTATE = os.getenv("FBREF_LANG_ROTATE", "0").lower() in ("1", "true", "yes")
REQUEST_JITTER_MAX = float(os.getenv("FBREF_REQUEST_JITTER_MAX", "0.5"))

# Reuse one session across calls to respect cookies and reduce connection churn
_GLOBAL_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    global _GLOBAL_SESSION
    if _GLOBAL_SESSION is None:
        # Create cache directory if configured with a path
        sess: requests.Session
        if CACHE_ENABLED and requests_cache is not None:
            cache_dir = os.path.dirname(CACHE_NAME)
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
            sess = requests_cache.CachedSession(
                cache_name=CACHE_NAME,
                backend="sqlite",
                expire_after=CACHE_TTL,
                allowable_methods=["GET"],
                allowable_codes=[200],
                stale_if_error=True,
                cache_control=True,
            )
        else:
            sess = requests.Session()
        # Set default headers
        sess.headers.update({
            "User-Agent": UA_OVERRIDE or DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": LANG_POOL[0],
            "Referer": "https://fbref.com/",
            "Connection": "keep-alive",
        })
        # Optional proxies
        http_proxy = os.getenv("FBREF_HTTP_PROXY")
        https_proxy = os.getenv("FBREF_HTTPS_PROXY")
        proxies = {}
        if http_proxy:
            proxies["http"] = http_proxy
        if https_proxy:
            proxies["https"] = https_proxy
        if proxies:
            sess.proxies.update(proxies)
        _GLOBAL_SESSION = sess
    return _GLOBAL_SESSION


def _get_html(url: str, max_retries: int = 8) -> str:
    s = _session()
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        # Optional jitter
        if REQUEST_JITTER_MAX > 0:
            time.sleep(random.uniform(0, REQUEST_JITTER_MAX))
        # Rotate UA/Lang if enabled
        if UA_ROTATE and not UA_OVERRIDE:
            ua = random.choice(UA_POOL)
            s.headers["User-Agent"] = ua
        if LANG_ROTATE:
            s.headers["Accept-Language"] = random.choice(LANG_POOL)
        print(f"GET {url} (attempt {attempt}/{max_retries})", flush=True)
        r = s.get(url, timeout=30)
        from_cache = getattr(r, "from_cache", False)
        if from_cache:
            print("  cache HIT", flush=True)
        if r.status_code == 429:
            # Respect Retry-After if provided
            ra = r.headers.get("Retry-After")
            delay = None
            if ra:
                try:
                    delay = float(ra)
                except Exception:
                    delay = None
            if delay is not None and delay > MAX_RETRY_AFTER:
                print(
                    f"  429 Too Many Requests. Retry-After {delay:.0f}s exceeds max {MAX_RETRY_AFTER:.0f}s. Capping to {MAX_RETRY_AFTER:.0f}s.",
                    flush=True,
                )
                wait = MAX_RETRY_AFTER
            else:
                wait = delay if delay is not None else backoff
            # Clear cookies and rotate headers for the next attempt
            try:
                s.cookies.clear()
            except Exception:
                pass
            if UA_ROTATE and not UA_OVERRIDE:
                s.headers["User-Agent"] = random.choice(UA_POOL)
            if LANG_ROTATE:
                s.headers["Accept-Language"] = random.choice(LANG_POOL)
            print(f"  429 Too Many Requests. Backing off {wait:.1f}s...", flush=True)
            time.sleep(wait)
            backoff = min(backoff * 2.0, 20.0)
            continue
        if r.status_code >= 500:
            # Clear cookies and rotate headers for the next attempt
            try:
                s.cookies.clear()
            except Exception:
                pass
            if UA_ROTATE and not UA_OVERRIDE:
                s.headers["User-Agent"] = random.choice(UA_POOL)
            if LANG_ROTATE:
                s.headers["Accept-Language"] = random.choice(LANG_POOL)
            print(f"  {r.status_code} server error. Backing off {backoff:.1f}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 20.0)
            continue
        r.raise_for_status()
        if not from_cache:
            print(f"  OK {url} (cache MISS)", flush=True)
        else:
            print(f"  OK {url} (cached)", flush=True)
        return r.text
    raise RuntimeError(f"Failed to fetch {url}")


_comp_history_re = re.compile(r"^/en/comps/(\d+)/history/.*-Seasons")
_comp_stats_re = re.compile(r"^/en/comps/(\d+)/.*-Stats$")
_country_re = re.compile(r"^/en/country/([A-Z]{3})/")


def list_first_tier_competitions() -> List[Dict]:
    """Parse the FBref competitions page and extract Domestic Leagues - 1st Tier.

    Returns items: { comp_id, comp_name, season_url, country_iso3 }
    Country name is not reliably present here; we'll infer from ISO3 later if needed.
    """
    print("Fetching competitions index...", flush=True)
    try:
        html = _get_html(COMPS_INDEX, max_retries=12)
    except Exception as e:
        # On failure (e.g., long Retry-After), try local JSON fallback if fresh
        if COMPS_CACHE_FILE and os.path.exists(COMPS_CACHE_FILE):
            try:
                with open(COMPS_CACHE_FILE, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                ts = float(payload.get("ts", 0))
                age = time.time() - ts
                data = payload.get("data", [])
                if age <= COMPS_CACHE_TTL and isinstance(data, list) and data:
                    print(
                        f"Using cached competitions list from {COMPS_CACHE_FILE} (age {int(age)}s).",
                        flush=True,
                    )
                    return data
                else:
                    print("Cached competitions list is missing/stale; cannot use fallback.", flush=True)
            except Exception as ce:
                print(f"Warn: failed to read competitions cache {COMPS_CACHE_FILE}: {ce}", flush=True)
        # Re-raise original error if no valid cache
        raise
    soup = BeautifulSoup(html, "lxml")

    # Find the section by its header text
    header = None
    for h2 in soup.find_all("h2"):
        if "Domestic Leagues - 1st Tier" in h2.get_text(strip=True):
            header = h2
            break
    if not header:
        return []

    # Build mapping comp_id -> info by scanning forward until the next H2
    comps: Dict[str, Dict] = {}
    for el in header.next_elements:
        # Stop at the next section header
        if getattr(el, "name", None) == "h2":
            break
        if getattr(el, "name", None) == "a" and el.has_attr("href"):
            anchors = [el]
        elif isinstance(el, Comment):
            try:
                sub = BeautifulSoup(el, "lxml")
                anchors = sub.find_all("a", href=True)
            except Exception:
                anchors = []
        else:
            anchors = []
        for a in anchors:
            href = a["href"]
            text = a.get_text(strip=True) or ""
            path = urlparse(href).path if href.startswith("http") else href
            m_hist = _comp_history_re.match(path)
            m_stats = _comp_stats_re.match(path)
            m_country = _country_re.match(path)
            if m_hist:
                cid = m_hist.group(1)
                comps.setdefault(
                    cid,
                    {
                        "comp_id": cid,
                        "comp_name": text.replace("Seasons", "").strip(),
                        "season_url": None,
                        "country_iso3": None,
                    },
                )
            elif m_stats:
                cid = m_stats.group(1)
                if cid in comps:
                    comps[cid]["season_url"] = f"https://fbref.com{path}"
            elif m_country:
                iso3 = m_country.group(1)
                for cid in list(comps.keys())[::-1]:
                    if comps[cid].get("country_iso3") is None:
                        comps[cid]["country_iso3"] = iso3
                        break

    # Filter only those with season_url
    out = [v for v in comps.values() if v.get("season_url")]
    print(f"Parsed first-tier competitions: {len(out)}", flush=True)
    # Write local competitions cache for future fallback
    try:
        cache_dir = os.path.dirname(COMPS_CACHE_FILE)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        with open(COMPS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "data": out}, f)
    except Exception as we:
        print(f"Warn: failed to write competitions cache {COMPS_CACHE_FILE}: {we}", flush=True)
    return out


def _extract_tables(html: str) -> List[BeautifulSoup]:
    """FBref often comments out tables; parse comments too and return all tables."""
    soup = BeautifulSoup(html, "lxml")
    tables = list(soup.find_all("table"))
    # parse comment blocks for tables
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        try:
            sub = BeautifulSoup(c, "lxml")
            tables.extend(sub.find_all("table"))
        except Exception:
            continue
    return tables


def list_competition_teams(season_url: str) -> List[str]:
    """Return unique squad names for the given competition season page."""
    html = _get_html(season_url)
    tables = _extract_tables(html)
    teams: Set[str] = set()
    for tbl in tables:
        # look for a 'squad' or 'team' column
        headers = [th.get("data-stat") or th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not any(h in ("squad", "team") for h in headers):
            continue
        # Iterate rows; team cell may be in th or td with data-stat='squad' or 'team'
        for row in tbl.find_all("tr"):
            # skip header-like rows
            if row.find_parent("thead") is not None:
                continue
            classes = row.get("class") or []
            if any("thead" in c or "over_header" in c for c in classes):
                continue
            cell = row.find(["th", "td"], attrs={"data-stat": ["squad", "team"]})
            if not cell:
                # try by header text
                ths = row.find_all("th")
                cell = ths[0] if ths else None
            if not cell:
                continue
            name = cell.get_text(" ", strip=True)
            if not name:
                continue
            # skip totals and aggregate rows
            lower = name.lower()
            if (
                "total" in lower
                or name in ("Opponent", "Opponents")
                or name in ("Squad", "Team", "Club", "Teams", "Clubs")
            ):
                continue
            # FBref sometimes prefixes with numbers/ranks
            name = re.sub(r"^\d+\s+", "", name)
            teams.add(name)
    return sorted(teams)


def iter_all_first_tier_teams(sleep_seconds: float = 0.6) -> Generator[Dict, None, None]:
    """Yield normalized teams from all first-tier domestic leagues on FBref.

    Normalized fields:
      provider: 'fbref'
      name, country_code (ISO3), country_name=None
    """
    comps = list_first_tier_competitions()
    for comp in comps:
        iso3 = comp.get("country_iso3")
        season_url = comp.get("season_url")
        if not season_url:
            continue
        try:
            teams = list_competition_teams(season_url)
        except Exception as e:
            print(f"Warn: failed teams for comp {comp.get('comp_name')} ({iso3}): {e}")
            continue
        for name in teams:
            yield {
                "provider": "fbref",
                "name": name,
                "country_code": iso3,
                "country_name": None,
            }
        time.sleep(sleep_seconds)
