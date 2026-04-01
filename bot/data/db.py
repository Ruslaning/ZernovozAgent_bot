import sqlite3

from config import DB_PATH

DDL = """
CREATE TABLE IF NOT EXISTS applications (
    id              INTEGER PRIMARY KEY,
    transportation_price REAL,
    loading_region  TEXT,
    loading_locality TEXT,
    distance        INTEGER,
    load_size       REAL,
    culture_title   TEXT,
    organization_name TEXT,
    created_at      TEXT,
    rating_value    REAL,
    rating_count    INTEGER,
    stevedore_org   TEXT,
    stevedore_city  TEXT
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)


def upsert_applications(apps: list[dict]) -> int:
    """Insert or replace applications. Returns count of rows upserted."""
    rows = []
    for a in apps:
        rating = a.get("rating")
        rows.append((
            a["id"],
            a["transportation_price"],
            a.get("loading_region"),
            a.get("loading_locality"),
            a.get("distance"),
            a.get("load_size"),
            a.get("culture_title"),
            a.get("organization_name"),
            a.get("created_at"),
            rating["rating"] if rating else None,
            rating["score_count"] if rating else None,
            a.get("stevedore", {}).get("organization_name"),
            a.get("stevedore", {}).get("place_city"),
        ))
    with get_conn() as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO applications
               (id, transportation_price, loading_region, loading_locality,
                distance, load_size, culture_title, organization_name,
                created_at, rating_value, rating_count,
                stevedore_org, stevedore_city)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
    return len(rows)


def get_all_applications() -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM applications").fetchall()
