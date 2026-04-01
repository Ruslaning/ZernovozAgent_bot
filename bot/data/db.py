import sqlite3
from datetime import datetime, timedelta

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

CREATE TABLE IF NOT EXISTS applications_archive (
    id                  INTEGER PRIMARY KEY,
    created_at          TEXT,
    organization_name   TEXT,
    rating_value        REAL,
    rating_count        INTEGER,
    loading_locality    TEXT,
    loading_region      TEXT,
    unloading_terminal  TEXT,
    unloading_city      TEXT,
    culture             TEXT,
    load_size           REAL,
    distance            INTEGER,
    price_per_kg        REAL,
    price_per_km        REAL,
    revenue_per_trip    REAL,
    fetched_at          TEXT
);

CREATE TABLE IF NOT EXISTS port_prices (
    culture     TEXT,
    terminal    TEXT,
    price       REAL,
    updated_at  TEXT,
    PRIMARY KEY (culture, terminal)
);

CREATE TABLE IF NOT EXISTS user_settings (
    user_id         INTEGER PRIMARY KEY,
    margin          REAL DEFAULT 250,
    extra_expenses  REAL DEFAULT 225,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS distances (
    full_address        TEXT PRIMARY KEY,
    dist_novorossiysk   REAL,
    dist_taman          REAL,
    dist_azov           REAL,
    dist_kkz            REAL,
    dist_severskaya     REAL,
    dist_gulkevichi     REAL,
    dist_giaginskaya    REAL,
    dist_temryuk        REAL,
    dist_npk            REAL,
    dist_rovnenskiy     REAL,
    dist_tbilisskaya    REAL,
    dist_kropotkin      REAL
);
"""

INITIAL_PORT_PRICES = [
    ("Пшеница", "НЗТ", 16900),
    ("Пшеница", "НКХП", 16900),
    ("Пшеница", "Тамань", 16900),
    ("Пшеница", "КСК", 16800),
]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)
        # Insert initial port prices if table is empty
        count = conn.execute("SELECT COUNT(*) FROM port_prices").fetchone()[0]
        if count == 0:
            now = datetime.now().isoformat()
            conn.executemany(
                "INSERT INTO port_prices (culture, terminal, price, updated_at) VALUES (?, ?, ?, ?)",
                [(c, t, p, now) for c, t, p in INITIAL_PORT_PRICES],
            )


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


# --- Archive ---

def archive_applications(apps: list[dict]) -> int:
    """INSERT OR IGNORE applications into archive (no duplicates by id)."""
    rows = []
    now = datetime.now().isoformat()
    for a in apps:
        rating = a.get("rating")
        price = a.get("transportation_price")
        distance = a.get("distance")
        load_size = a.get("load_size")

        price_per_km = None
        revenue_per_trip = None
        if price and distance and distance > 0:
            price_per_km = round(price * 1000 / distance, 2)
        if price and load_size:
            revenue_per_trip = round(price * load_size * 1000)

        rows.append((
            a["id"],
            a.get("created_at"),
            a.get("organization_name"),
            rating["rating"] if rating else None,
            rating["score_count"] if rating else None,
            a.get("loading_locality"),
            a.get("loading_region"),
            a.get("stevedore", {}).get("organization_name"),
            a.get("stevedore", {}).get("place_city"),
            a.get("culture_title"),
            load_size,
            distance,
            price,
            price_per_km,
            revenue_per_trip,
            now,
        ))
    with get_conn() as conn:
        # Delete records older than 5 days
        conn.execute(
            "DELETE FROM applications_archive WHERE fetched_at < datetime('now', '-5 days')"
        )
        conn.executemany(
            """INSERT OR IGNORE INTO applications_archive
               (id, created_at, organization_name, rating_value, rating_count,
                loading_locality, loading_region, unloading_terminal, unloading_city,
                culture, load_size, distance, price_per_kg, price_per_km,
                revenue_per_trip, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
    return len(rows)


def get_archive_stats(days: int = 30) -> dict:
    """Return archive statistics for the given period."""
    with get_conn() as conn:
        now = datetime.now()
        period_start = (now - timedelta(days=days)).isoformat()
        period_end = now.isoformat()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        week_start = (now - timedelta(days=7)).isoformat()

        total = conn.execute(
            "SELECT COUNT(*) FROM applications_archive WHERE fetched_at >= ?",
            (period_start,),
        ).fetchone()[0]

        new_today = conn.execute(
            "SELECT COUNT(*) FROM applications_archive WHERE fetched_at >= ?",
            (today_start,),
        ).fetchone()[0]

        new_week = conn.execute(
            "SELECT COUNT(*) FROM applications_archive WHERE fetched_at >= ?",
            (week_start,),
        ).fetchone()[0]

        top_terminals = conn.execute(
            """SELECT unloading_terminal, COUNT(*) as cnt
               FROM applications_archive
               WHERE fetched_at >= ?
               GROUP BY unloading_terminal
               ORDER BY cnt DESC
               LIMIT 5""",
            (period_start,),
        ).fetchall()

        return {
            "total": total,
            "period_start": period_start[:10],
            "period_end": period_end[:10],
            "new_today": new_today,
            "new_week": new_week,
            "top_terminals": [(r[0] or "—", r[1]) for r in top_terminals],
        }


# --- Port prices ---

def get_port_prices() -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM port_prices ORDER BY culture, terminal").fetchall()


def set_port_price(terminal: str, culture: str, price: float) -> None:
    now = datetime.now().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO port_prices (culture, terminal, price, updated_at)
               VALUES (?, ?, ?, ?)""",
            (culture, terminal, price, now),
        )


# --- User settings ---

def get_user_settings(user_id: int) -> dict:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM user_settings WHERE user_id = ?", (user_id,)).fetchone()
        if row:
            return {"margin": row["margin"], "extra_expenses": row["extra_expenses"]}
        return {"margin": 250.0, "extra_expenses": 225.0}


def set_user_margin(user_id: int, margin: float) -> None:
    now = datetime.now().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO user_settings (user_id, margin, extra_expenses, updated_at)
               VALUES (?, ?, 225, ?)
               ON CONFLICT(user_id) DO UPDATE SET margin = ?, updated_at = ?""",
            (user_id, margin, now, margin, now),
        )


def set_user_expenses(user_id: int, extra_expenses: float) -> None:
    now = datetime.now().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO user_settings (user_id, margin, extra_expenses, updated_at)
               VALUES (?, 250, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET extra_expenses = ?, updated_at = ?""",
            (user_id, extra_expenses, now, extra_expenses, now),
        )


# --- Distances ---

def get_all_distances() -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM distances").fetchall()


def find_distance(address: str) -> dict | None:
    """Find distance record using rapidfuzz fuzzy matching (threshold 80%)."""
    from rapidfuzz import fuzz

    rows = get_all_distances()
    if not rows:
        return None

    best_match = None
    best_score = 0
    for row in rows:
        score = fuzz.ratio(address.lower(), row["full_address"].lower())
        if score > best_score:
            best_score = score
            best_match = row

    if best_score >= 80 and best_match is not None:
        terminals = {
            "НЗТ": best_match["dist_novorossiysk"],
            "Тамань": best_match["dist_taman"],
            "Азов": best_match["dist_azov"],
            "ККЗ": best_match["dist_kkz"],
            "Северская": best_match["dist_severskaya"],
            "Гулькевичи": best_match["dist_gulkevichi"],
            "Гиагинская": best_match["dist_giaginskaya"],
            "Темрюк": best_match["dist_temryuk"],
            "НПК": best_match["dist_npk"],
            "Ровненский": best_match["dist_rovnenskiy"],
            "Тбилисская": best_match["dist_tbilisskaya"],
            "Кропоткин": best_match["dist_kropotkin"],
        }
        return {
            "address": best_match["full_address"],
            "score": best_score,
            "distances": terminals,
        }
    return None


def get_archive_for_export(days: int = 30) -> list[sqlite3.Row]:
    """Get archive data for Excel export."""
    period_start = (datetime.now() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        return conn.execute(
            """SELECT * FROM applications_archive
               WHERE fetched_at >= ?
               ORDER BY fetched_at DESC""",
            (period_start,),
        ).fetchall()
