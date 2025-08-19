import getpass
import os
import re
from typing import Optional, Tuple

import psycopg
import pycountry


def db_connect(
    dbname: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
):
    dbname = dbname or os.getenv("PGDATABASE", "futbol")
    host = host or os.getenv("PGHOST", "127.0.0.1")
    port = port or int(os.getenv("PGPORT", "5432"))
    user = user or os.getenv("PGUSER", getpass.getuser())
    conn = psycopg.connect(dbname=dbname, host=host, port=port, user=user)
    conn.autocommit = False
    return conn


def slugify(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def canonical_team_key(country_code: Optional[str], team_name: str) -> str:
    cc = (country_code or "xx").strip().lower()
    return f"team:{cc}:{slugify(team_name)}"


def _normalize_iso3(code: Optional[str], name: Optional[str]) -> Optional[str]:
    """Best-effort convert country code/name to ISO3 (upper)."""
    c = (code or "").strip()
    n = (name or "").strip()
    if c:
        c_up = c.upper()
        # Try exact alpha_3
        try:
            return pycountry.countries.get(alpha_3=c_up).alpha_3
        except Exception:
            pass
        # Try alpha_2 to alpha_3
        if len(c_up) == 2:
            try:
                return pycountry.countries.get(alpha_2=c_up).alpha_3
            except Exception:
                pass
    # Try by common name
    if n:
        try:
            return pycountry.countries.lookup(n).alpha_3
        except Exception:
            # fallback to first 3 letters of name
            return n[:3].upper()
    return None


def upsert_country(cur, code: Optional[str], name: Optional[str]) -> Optional[int]:
    code_iso3 = _normalize_iso3(code, name)
    c_up = (code or "").strip().upper()
    # Prefer provided name; if missing, try pycountry; fallback to code_iso3
    resolved_name = (name or "").strip() or None
    if not resolved_name and code_iso3:
        try:
            resolved_name = pycountry.countries.get(alpha_3=code_iso3).name
        except Exception:
            resolved_name = None
    if not code_iso3 and not resolved_name and not c_up:
        return None
    if code_iso3:
        cur.execute(
            """
            INSERT INTO ulkeler (kod, isim)
            VALUES (%s, %s)
            ON CONFLICT (kod) DO UPDATE SET isim = EXCLUDED.isim
            RETURNING id
            """,
            (code_iso3, resolved_name or code_iso3),
        )
        return cur.fetchone()[0]
    # Fallback: if we were given a (likely football/IOC) 3-letter code like ENG, KSA, CHI,
    # accept it as-is to avoid NULL country references.
    if c_up and len(c_up) == 3:
        cur.execute(
            """
            INSERT INTO ulkeler (kod, isim)
            VALUES (%s, %s)
            ON CONFLICT (kod) DO UPDATE SET isim = EXCLUDED.isim
            RETURNING id
            """,
            (c_up, resolved_name or c_up),
        )
        return cur.fetchone()[0]
    # no code, insert by name with derived 3-letter code
    cur.execute("SELECT id FROM ulkeler WHERE LOWER(isim)=LOWER(%s)", (resolved_name,))
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
        ((resolved_name or "UNK").upper(), resolved_name or "Unknown"),
    )
    return cur.fetchone()[0]


def upsert_team(
    cur,
    country_id: Optional[int],
    country_code: Optional[str],
    name: str,
    short_name: Optional[str],
    founded: Optional[int],
):
    key = canonical_team_key(country_code, name)
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
