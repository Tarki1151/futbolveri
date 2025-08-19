import math
import time
from typing import Dict, List, Any, Optional, Tuple
import difflib
import unicodedata

from clients import api_football as apif
from clients import football_data as fdata

# Simple in-memory cache for Football-Data teams to avoid repeated full scans
_FD_CACHE: Dict[str, Any] = {"ts": 0.0, "teams": []}
_FD_TTL = 6 * 3600  # 6 hours


def _ensure_fd_cache(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    now = time.time()
    if now - _FD_CACHE.get("ts", 0) < _FD_TTL and _FD_CACHE.get("teams"):
        return _FD_CACHE["teams"]
    # Build once from all tier-one teams
    teams: List[Dict[str, Any]] = []
    try:
        for t in fdata.iter_all_tier_one_teams(api_key):
            teams.append(t)
    except Exception:
        teams = []
    _FD_CACHE["ts"] = now
    _FD_CACHE["teams"] = teams
    return teams


def _best_match(name: str, items: List[Dict[str, Any]], key: str) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    name_l = name.lower()
    best = None
    best_score = -1.0
    for it in items:
        s = difflib.SequenceMatcher(a=name_l, b=(it.get(key) or "").lower()).ratio()
        if s > best_score:
            best_score = s
            best = it
    return best


async def resolve_team_providers(team_name: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"api_football": None, "football_data": None}
    # Helpers for normalization
    def _strip_accents(s: str) -> str:
        try:
            return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
        except Exception:
            return s

    def _tr_simplify(s: str) -> str:
        tbl = str.maketrans({
            "ç": "c", "Ç": "C",
            "ğ": "g", "Ğ": "G",
            "ı": "i", "İ": "I",
            "ö": "o", "Ö": "O",
            "ş": "s", "Ş": "S",
            "ü": "u", "Ü": "U",
        })
        return s.translate(tbl)

    # API-Football search with fallbacks
    try:
        res = apif.search_teams(team_name)
        if not res:
            alt = _tr_simplify(team_name)
            if alt != team_name:
                res = apif.search_teams(alt)
        if not res:
            alt2 = _strip_accents(team_name)
            if alt2 != team_name:
                res = apif.search_teams(alt2)
        if res:
            # normalize into items with id+name
            cand = []
            for r in res:
                t = r.get("team") or {}
                cand.append({"id": t.get("id"), "name": t.get("name")})
            best = _best_match(team_name, cand, "name")
            if best and best.get("id"):
                out["api_football"] = best
    except Exception:
        pass

    # Football-Data heuristic match from cached list
    try:
        fd_list = _ensure_fd_cache()
        best_fd = _best_match(team_name, fd_list, "name")
        if best_fd and best_fd.get("provider_team_id"):
            out["football_data"] = {
                "id": best_fd.get("provider_team_id"),
                "name": best_fd.get("name"),
            }
    except Exception:
        pass
    return out


def _recent_goals_from_apif(team_id: int, last_n: int = 10) -> Tuple[float, float]:
    """Return (avg_scored, avg_conceded) using fixtures from at least last 5 years.

    Strategy:
      1) Try last 5 years (date range, paginated).
      2) Fallback to recent windows (last_n, 25, 50) if empty.
      3) If still empty, use neutral prior (1.1, 1.1).
    """
    try:
        fixtures = apif.fixtures_last_years(team_id, years=5)
    except Exception:
        fixtures = []
    if not fixtures:
        fixtures = apif.recent_fixtures(team_id, last_n=last_n) or []
    if not fixtures:
        fixtures = apif.recent_fixtures(team_id, last_n=25) or []
    if not fixtures:
        fixtures = apif.recent_fixtures(team_id, last_n=50) or []
    if not fixtures:
        return (1.1, 1.1)  # neutral prior
    gs = 0.0
    ga = 0.0
    for f in fixtures:
        teams = f.get("teams") or {}
        goals = f.get("goals") or {}
        # Decide home/away perspective
        is_home = False
        try:
            if (teams.get("home") or {}).get("id") == team_id:
                is_home = True
        except Exception:
            is_home = False
        if is_home:
            gs += goals.get("home", 0) or 0
            ga += goals.get("away", 0) or 0
        else:
            gs += goals.get("away", 0) or 0
            ga += goals.get("home", 0) or 0
    n = max(1, len(fixtures))
    return (gs / n, ga / n)


def _poisson_pmf(lmbda: float, k: int) -> float:
    return math.exp(-lmbda) * (lmbda ** k) / math.factorial(k)


def _joint_score_probs(lh: float, la: float, max_goals: int = 10) -> List[List[float]]:
    pmf_h = [_poisson_pmf(lh, i) for i in range(max_goals + 1)]
    pmf_a = [_poisson_pmf(la, j) for j in range(max_goals + 1)]
    mat = [[ph * pa for pa in pmf_a] for ph in pmf_h]
    return mat


def _prob_1x2(mat: List[List[float]]) -> Dict[str, float]:
    p_home = sum(mat[i][j] for i in range(len(mat)) for j in range(len(mat[i])) if i > j)
    p_draw = sum(mat[i][j] for i in range(len(mat)) for j in range(len(mat[i])) if i == j)
    p_away = 1.0 - p_home - p_draw
    return {"MS1": p_home, "MS0": p_draw, "MS2": p_away}


def _prob_ou25(mat: List[List[float]]) -> Dict[str, float]:
    under = 0.0
    for i in range(len(mat)):
        for j in range(len(mat[i])):
            if i + j <= 2:
                under += mat[i][j]
    over = 1.0 - under
    return {"ALT25": under, "UST25": over}


def _prob_btts(mat: List[List[float]]) -> Dict[str, float]:
    yes = 0.0
    for i in range(1, len(mat)):
        for j in range(1, len(mat[i])):
            yes += mat[i][j]
    no = 1.0 - yes
    return {"KGVAR": yes, "KGYOK": no}


async def predict_match(home_name: str, away_name: str, prov_home: Dict[str, Any], prov_away: Dict[str, Any]) -> Dict[str, Any]:
    # Get API-Football signals
    lh = la = 1.2  # priors
    src_used = {"api_football": False, "football_data": False}
    try:
        have_h = prov_home.get("api_football")
        have_a = prov_away.get("api_football")
        if have_h and have_a:
            h_id = prov_home["api_football"]["id"]
            a_id = prov_away["api_football"]["id"]
            h_for, h_against = _recent_goals_from_apif(h_id)
            a_for, a_against = _recent_goals_from_apif(a_id)
            # Blend for expected goals
            lh = 0.6 * h_for + 0.4 * a_against
            la = 0.6 * a_for + 0.4 * h_against
            src_used["api_football"] = True
        elif have_h or have_a:
            # Use whatever is available to move off prior
            if have_h:
                h_id = prov_home["api_football"]["id"]
                h_for, h_against = _recent_goals_from_apif(h_id)
                lh = 0.7 * h_for + 0.3 * lh
                la = 0.6 * la + 0.4 * h_against
            if have_a:
                a_id = prov_away["api_football"]["id"]
                a_for, a_against = _recent_goals_from_apif(a_id)
                la = 0.7 * a_for + 0.3 * la
                lh = 0.6 * lh + 0.4 * a_against
            src_used["api_football"] = True
    except Exception:
        pass

    lh = max(lh, 0.1)
    la = max(la, 0.1)
    mat = _joint_score_probs(lh, la)
    p1x2 = _prob_1x2(mat)
    ou = _prob_ou25(mat)
    btts = _prob_btts(mat)

    # Dixon–Coles adjustment with rho parameter (negative reduces 0-0,1-1 inflation)
    rho = -0.10  # can be tuned later or made configurable
    tau_00 = 1.0 - (lh + la) * rho
    tau_01 = 1.0 + lh * rho
    tau_10 = 1.0 + la * rho
    tau_11 = 1.0 - rho
    mat_dc = [row[:] for row in mat]
    # apply only to (0,0), (0,1), (1,0), (1,1)
    mat_dc[0][0] *= tau_00
    mat_dc[0][1] *= tau_01
    mat_dc[1][0] *= tau_10
    mat_dc[1][1] *= tau_11
    # renormalize
    s = sum(sum(r) for r in mat_dc)
    if s > 0:
        for i in range(len(mat_dc)):
            for j in range(len(mat_dc[i])):
                mat_dc[i][j] /= s
    p1x2_dc = _prob_1x2(mat_dc)
    ou_dc = _prob_ou25(mat_dc)
    btts_dc = _prob_btts(mat_dc)

    # Top pick labels in Nesine style
    best_ms = max(p1x2.items(), key=lambda x: x[1])[0]
    best_ou = max(ou.items(), key=lambda x: x[1])[0]
    best_btts = max(btts.items(), key=lambda x: x[1])[0]
    best_ms_dc = max(p1x2_dc.items(), key=lambda x: x[1])[0]
    best_ou_dc = max(ou_dc.items(), key=lambda x: x[1])[0]
    best_btts_dc = max(btts_dc.items(), key=lambda x: x[1])[0]

    def pct(x: float) -> float:
        return round(100.0 * x, 1)

    return {
        "lambda_home": round(lh, 2),
        "lambda_away": round(la, 2),
        "markets_poisson": {
            "MS": {k: pct(v) for k, v in p1x2.items()},
            "OU25": {k: pct(v) for k, v in ou.items()},
            "BTTS": {k: pct(v) for k, v in btts.items()},
        },
        "top_picks_poisson": [best_ms, best_ou, best_btts],
        "markets_dc": {
            "MS": {k: pct(v) for k, v in p1x2_dc.items()},
            "OU25": {k: pct(v) for k, v in ou_dc.items()},
            "BTTS": {k: pct(v) for k, v in btts_dc.items()},
        },
        "top_picks_dc": [best_ms_dc, best_ou_dc, best_btts_dc],
        "sources": src_used,
        "params": {"rho": rho},
    }
