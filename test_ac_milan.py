#!/usr/bin/env python3
import json
import os
import sys
import urllib.parse
import urllib.request

ROOT = os.path.dirname(__file__)
ENV_PATH = os.path.join(ROOT, ".env")


def load_env(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def http_get(url: str, headers: dict) -> dict:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
        try:
            return json.loads(data.decode("utf-8"))
        except Exception:
            print(data.decode("utf-8", errors="ignore"))
            raise


def test_football_data():
    key = os.getenv("FOOTBALL_API")
    if not key:
        return {"error": "FOOTBALL_API not set"}
    url = "https://api.football-data.org/v4/competitions/SA/teams"
    headers = {"X-Auth-Token": key}
    j = http_get(url, headers)
    out = []
    for t in j.get("teams", []):
        name = (t.get("name") or "")
        if "milan" in name.lower():
            area = (t.get("area") or {}).get("name")
            out.append({
                "id": t.get("id"),
                "name": t.get("name"),
                "code": t.get("tla"),
                "country": area,
                "founded": t.get("founded"),
            })
    return {"count": len(out), "teams": out}


def test_api_football():
    key = os.getenv("API_FOOTBALL_COM")
    if not key:
        return {"error": "API_FOOTBALL_COM not set"}
    base = "https://v3.football.api-sports.io"
    q = urllib.parse.quote("AC Milan")
    url = f"{base}/teams?search={q}"
    headers = {"x-apisports-key": key, "Accept": "application/json"}
    j = http_get(url, headers)
    out = []
    for item in j.get("response", []):
        team = item.get("team") or {}
        # team.country is a string in this endpoint
        country = team.get("country") or (item.get("country") or {}).get("name")
        name = (team.get("name") or "")
        if "milan" in name.lower():
            out.append({
                "id": team.get("id"),
                "name": team.get("name"),
                "code": team.get("code"),
                "country": country,
                "founded": team.get("founded"),
            })
    return {"count": len(out), "teams": out}


if __name__ == "__main__":
    load_env(ENV_PATH)
    fd = test_football_data()
    af = test_api_football()
    print("Football-Data.org:")
    print(json.dumps(fd, ensure_ascii=False, indent=2))
    print("\nAPI-Football:")
    print(json.dumps(af, ensure_ascii=False, indent=2))
