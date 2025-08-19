from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from typing import List, Dict, Any
import difflib
import os
from dotenv import load_dotenv

from db.util import db_connect, slugify
from app.predictor import predict_match, resolve_team_providers

load_dotenv()
app = FastAPI(title="Futbol Tahmin")

BASE_DIR = os.path.dirname(__file__)
TEMPLATES = Jinja2Templates(directory=os.path.join(os.path.dirname(BASE_DIR), "templates"))

# Mount static
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(BASE_DIR), "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return TEMPLATES.TemplateResponse("index.html", {"request": request})


@app.get("/teams")
async def search_teams(q: str = Query(..., min_length=1), limit: int = 8) -> List[Dict[str, Any]]:
    # Simple fuzzy search over takimlar by name
    sql = """
        SELECT id, isim, anahtar
        FROM takimlar
        WHERE LOWER(isim) LIKE LOWER(%s)
        LIMIT 200
    """
    like = f"%{q}%"
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (like,))
            rows = cur.fetchall()
    # Normalize/filter and score with difflib; dedupe by slug
    blocked_exact = {"opponent", "opponents", "squad", "team", "club", "teams", "clubs"}
    best_by_slug: Dict[str, Dict[str, Any]] = {}
    for rid, name, key in rows:
        nm = (name or "").strip()
        if not nm:
            continue
        lower = nm.lower()
        if lower.startswith("vs "):
            continue
        if lower in blocked_exact:
            continue
        s = slugify(nm)
        ratio = difflib.SequenceMatcher(a=q.lower(), b=lower).ratio()
        cand = {"id": rid, "name": nm, "key": key, "score": ratio}
        prev = best_by_slug.get(s)
        if not prev or cand["score"] > prev["score"]:
            best_by_slug[s] = cand
    scored = list(best_by_slug.values())
    scored.sort(key=lambda x: (-x["score"], len(x["name"])) )
    return scored[:limit]


@app.get("/predict")
async def predict(home: str = Query(...), away: str = Query(...)) -> Dict[str, Any]:
    if not home or not away:
        raise HTTPException(status_code=400, detail="home and away are required")
    if home.strip().lower() == away.strip().lower():
        raise HTTPException(status_code=400, detail="Teams must be different")

    # Resolve to best-matching DB rows
    def resolve_one(name: str):
        sql = """
            SELECT id, isim, anahtar
            FROM takimlar
            WHERE LOWER(isim) LIKE LOWER(%s)
            LIMIT 200
        """
        like = f"%{name}%"
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (like,))
                rows = cur.fetchall()
        if not rows:
            # fallback: try splitting tokens
            tokens = name.split()
            if tokens:
                like = f"%{tokens[0]}%"
                with db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, (like,))
                        rows = cur.fetchall()
        if not rows:
            return None
        # filter artifacts and choose best by difflib
        blocked_exact = {"opponent", "opponents", "squad", "team", "club", "teams", "clubs"}
        filt = []
        for r in rows:
            nm = (r[1] or "").strip()
            if not nm:
                continue
            lower = nm.lower()
            if lower.startswith("vs "):
                continue
            if lower in blocked_exact:
                continue
            filt.append(r)
        if not filt:
            filt = rows
        best = max(filt, key=lambda r: difflib.SequenceMatcher(a=name.lower(), b=(r[1] or '').lower()).ratio())
        return {"id": best[0], "name": best[1], "key": best[2]}

    home_row = resolve_one(home)
    away_row = resolve_one(away)
    if not home_row or not away_row:
        raise HTTPException(status_code=404, detail="Could not resolve teams from database")

    # Resolve provider team ids for both
    providers_home = await resolve_team_providers(home_row["name"])  # type: ignore
    providers_away = await resolve_team_providers(away_row["name"])  # type: ignore

    result = await predict_match(home_row["name"], away_row["name"], providers_home, providers_away)
    return {
        "home": home_row,
        "away": away_row,
        "providers_home": providers_home,
        "providers_away": providers_away,
        "prediction": result,
    }
