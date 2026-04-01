import sqlite3
from config import DB_PATH
from datetime import datetime, timedelta


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


DISTANCE_RANGES = [
    (0, 50), (50, 100), (100, 150), (150, 200), (200, 300),
    (300, 400), (400, 500), (500, 700), (700, 1000), (1000, 99999),
]

RANGE_LABELS = {
    (0, 50): "0-50",
    (50, 100): "50-100",
    (100, 150): "100-150",
    (150, 200): "150-200",
    (200, 300): "200-300",
    (300, 400): "300-400",
    (400, 500): "400-500",
    (500, 700): "500-700",
    (700, 1000): "700-1000",
    (1000, 99999): "1000+",
}


def analyze_prices(terminal: str | None = None, days: int = 30) -> dict:
    """
    Analyze prices from archive grouped by terminal and distance range.
    Outlier filter: exclude where abs(price - avg) > 0.6.
    Returns: {terminal -> distance_range_label -> {min, avg, max, count}}
    """
    period_start = (datetime.now() - timedelta(days=days)).isoformat()

    with _get_conn() as conn:
        params = [period_start]
        where = "WHERE fetched_at >= ? AND price_per_kg IS NOT NULL AND distance IS NOT NULL AND distance > 0"
        if terminal:
            where += " AND unloading_terminal = ?"
            params.append(terminal)

        rows = conn.execute(
            f"SELECT unloading_terminal, distance, price_per_kg FROM applications_archive {where}",
            params,
        ).fetchall()

    if not rows:
        return {}

    # Group by terminal
    by_terminal: dict[str, list] = {}
    for r in rows:
        t = r["unloading_terminal"] or "—"
        by_terminal.setdefault(t, []).append(r)

    result = {}
    for term, term_rows in by_terminal.items():
        # Calculate avg for outlier filter
        prices = [r["price_per_kg"] for r in term_rows]
        avg_price = sum(prices) / len(prices)
        filtered = [r for r in term_rows if abs(r["price_per_kg"] - avg_price) <= 0.6]

        if not filtered:
            continue

        range_data = {}
        for low, high in DISTANCE_RANGES:
            in_range = [r for r in filtered if low <= r["distance"] < high]
            if not in_range:
                continue
            p = [r["price_per_kg"] for r in in_range]
            label = RANGE_LABELS[(low, high)]
            range_data[label] = {
                "min": round(min(p), 2),
                "avg": round(sum(p) / len(p), 2),
                "max": round(max(p), 2),
                "count": len(p),
            }

        if range_data:
            result[term] = range_data

    return result


def get_price_for_distance(terminal: str, distance_km: int, days: int = 30) -> dict | None:
    """Get interpolated price for a specific terminal and distance."""
    data = analyze_prices(terminal=terminal, days=days)
    if not data or terminal not in data:
        return None

    ranges = data[terminal]

    # Find exact range
    for low, high in DISTANCE_RANGES:
        label = RANGE_LABELS[(low, high)]
        if low <= distance_km < high and label in ranges:
            return ranges[label]

    # Find nearest range
    best_label = None
    best_diff = float("inf")
    for low, high in DISTANCE_RANGES:
        label = RANGE_LABELS[(low, high)]
        if label not in ranges:
            continue
        mid = (low + high) / 2 if high < 99999 else low + 500
        diff = abs(distance_km - mid)
        if diff < best_diff:
            best_diff = diff
            best_label = label

    if best_label:
        return ranges[best_label]
    return None
