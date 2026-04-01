import sqlite3
from statistics import mean
from bot.data.db import DB_PATH

ANALYSIS_DAYS = 5  # анализируем заявки за последние 5 дней
OUTLIER_THRESHOLD = 0.80  # порог отсева выбросов (из листа "Анализ по км")

DISTANCE_RANGES = [
    (0, 50, "0-50"),
    (50, 100, "50-100"),
    (100, 150, "100-150"),
    (150, 200, "150-200"),
    (200, 300, "200-300"),
    (300, 400, "300-400"),
    (400, 500, "400-500"),
    (500, 700, "500-700"),
    (700, 1000, "700-1000"),
    (1000, 99999, "1000+"),
]

def get_distance_range(km: int) -> str:
    for lo, hi, label in DISTANCE_RANGES:
        if lo <= km < hi:
            return label
    return "1000+"


def analyze_prices(terminal: str = None, days: int = ANALYSIS_DAYS) -> dict:
    """
    Build price matrix from archive: terminal x distance_range -> {min, avg, max, count}
    Uses last N days only. Filters outliers > OUTLIER_THRESHOLD from avg.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if terminal:
        cur.execute("""
            SELECT unloading_terminal, culture, distance, price_per_kg
            FROM applications_archive
            WHERE distance > 0 AND price_per_kg > 0
              AND unloading_terminal = ?
              AND date(fetched_at) >= date('now', ? || ' days')
        """, (terminal, f"-{days}"))
    else:
        cur.execute("""
            SELECT unloading_terminal, culture, distance, price_per_kg
            FROM applications_archive
            WHERE distance > 0 AND price_per_kg > 0
              AND date(fetched_at) >= date('now', ? || ' days')
        """, (f"-{days}",))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {}

    # Group by terminal -> distance_range -> prices
    groups: dict[str, dict[str, list[float]]] = {}
    for r in rows:
        t = r["unloading_terminal"]
        if not t:
            continue
        dr = get_distance_range(r["distance"])
        groups.setdefault(t, {}).setdefault(dr, []).append(r["price_per_kg"])

    # Calculate stats with outlier filtering
    result = {}
    for t, ranges in groups.items():
        result[t] = {}
        for dr, prices in ranges.items():
            if not prices:
                continue
            avg_prelim = mean(prices)
            filtered = [p for p in prices if abs(p - avg_prelim) <= OUTLIER_THRESHOLD]
            if not filtered:
                filtered = prices
            result[t][dr] = {
                "min": round(min(filtered), 2),
                "avg": round(mean(filtered), 2),
                "max": round(max(filtered), 2),
                "count": len(filtered),
            }

    return result


def get_price_for_distance(terminal: str, distance_km: int, culture: str = None, days: int = ANALYSIS_DAYS) -> dict | None:
    """
    Get transport price stats for specific terminal and distance.
    Returns {min, avg, max} in rub/kg.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get prices within ±50km of requested distance
    tolerance = 50
    query = """
        SELECT price_per_kg
        FROM applications_archive
        WHERE distance > 0 AND price_per_kg > 0
          AND unloading_terminal = ?
          AND distance BETWEEN ? AND ?
          AND date(fetched_at) >= date('now', ? || ' days')
    """
    params = [terminal, distance_km - tolerance, distance_km + tolerance, f"-{days}"]

    if culture:
        query += " AND culture = ?"
        params.append(culture)

    cur.execute(query, params)
    rows = [r["price_per_kg"] for r in cur.fetchall()]
    conn.close()

    if not rows:
        return None

    avg_prelim = mean(rows)
    filtered = [p for p in rows if abs(p - avg_prelim) <= OUTLIER_THRESHOLD]
    if not filtered:
        filtered = rows

    return {
        "min": round(min(filtered), 2),
        "avg": round(mean(filtered), 2),
        "max": round(max(filtered), 2),
        "count": len(filtered),
    }
