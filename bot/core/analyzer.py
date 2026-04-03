import sqlite3
from datetime import datetime, timedelta
from bot.data.db import DB_PATH

ANALYSIS_DAYS = 5
OUTLIER_THRESHOLD = 0.6
DEFAULT_MIN_PRICE_30KM = 0.55
DISTANCE_TOLERANCE = 10
KM_STEP = 20
KM_RANGE = (30, 830)


def build_price_curve(
    terminal: str,
    culture: str,
    min_price_30km: float = DEFAULT_MIN_PRICE_30KM,
    deviation_threshold: float = OUTLIER_THRESHOLD,
    distance_tolerance: int = DISTANCE_TOLERANCE,
    days: int = ANALYSIS_DAYS,
) -> dict:
    """
    Build continuous price curve using linear interpolation between real data points.
    Reproduces Excel 'Анализ по км' logic exactly.
    Returns {km: price_rub_per_kg}
    """
    km_points = list(range(KM_RANGE[0], KM_RANGE[1] + 1, KM_STEP))
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT price_per_kg, distance FROM applications_archive
        WHERE unloading_terminal = ? AND culture = ?
        AND price_per_kg > 0 AND distance > 0
        AND date(fetched_at) >= ?
    """, (terminal, culture, cutoff))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Step 1: collect MIN prices per km point with tolerance ±10km
    raw_prices = {}
    for km in km_points:
        matching = [r["price_per_kg"] for r in rows if abs(r["distance"] - km) <= distance_tolerance]
        if matching:
            raw_prices[km] = min(matching)

    # Step 2: filter outliers
    if raw_prices:
        avg = sum(raw_prices.values()) / len(raw_prices)
        raw_prices = {km: p for km, p in raw_prices.items() if abs(p - avg) <= deviation_threshold}

    # Step 3: build known points with minimum anchor at 30km
    known_points = sorted(raw_prices.items())
    if not known_points:
        return {}

    # Add minimum price anchor if no data at 30km
    if known_points[0][0] > km_points[0]:
        known_points.insert(0, (km_points[0], min_price_30km))

    # Step 4: linear interpolation between nearest neighbors
    result = {}
    for km in km_points:
        left = right = None
        for kp_km, kp_price in known_points:
            if kp_km <= km:
                left = (kp_km, kp_price)
            if kp_km >= km and right is None:
                right = (kp_km, kp_price)
                break

        if left and right:
            if left[0] == right[0]:
                # Exact match
                result[km] = round(left[1], 2)
            else:
                # Interpolate between nearest left and right
                ratio = (km - left[0]) / (right[0] - left[0])
                result[km] = round(left[1] + ratio * (right[1] - left[1]), 2)
        elif left:
            # Extrapolate right using last slope
            if len(known_points) >= 2:
                last_two = known_points[-2:]
                slope = (last_two[1][1] - last_two[0][1]) / (last_two[1][0] - last_two[0][0])
                result[km] = round(left[1] + slope * (km - left[0]), 2)
            else:
                result[km] = round(left[1], 2)
        elif right:
            result[km] = round(min_price_30km, 2)

    return result


def get_price_for_distance(terminal: str, distance_km: int, culture: str = None, days: int = ANALYSIS_DAYS) -> dict | None:
    """Get interpolated price for specific terminal + distance."""
    culture = culture or "Пшеница"
    curve = build_price_curve(terminal, culture, days=days)
    if not curve:
        return None

    km_points = sorted(curve.keys())

    if distance_km <= km_points[0]:
        p = curve[km_points[0]]
    elif distance_km >= km_points[-1]:
        p = curve[km_points[-1]]
    else:
        p = None
        for i in range(len(km_points) - 1):
            if km_points[i] <= distance_km <= km_points[i + 1]:
                l, r = km_points[i], km_points[i + 1]
                ratio = (distance_km - l) / (r - l)
                p = round(curve[l] + ratio * (curve[r] - curve[l]), 2)
                break

    if p is None:
        return None

    return {"min": p, "avg": p, "max": p, "count": 1}


def analyze_prices(terminal: str = None, days: int = ANALYSIS_DAYS) -> dict:
    """Build price matrix for all terminals/cultures."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if terminal:
        cur.execute("""SELECT DISTINCT unloading_terminal, culture FROM applications_archive
                       WHERE unloading_terminal = ? AND date(fetched_at) >= ?""", (terminal, cutoff))
    else:
        cur.execute("""SELECT DISTINCT unloading_terminal, culture FROM applications_archive
                       WHERE date(fetched_at) >= ?""", (cutoff,))
    pairs = cur.fetchall()
    conn.close()

    result = {}
    for t, c in pairs:
        if not t:
            continue
        curve = build_price_curve(t, c, days=days)
        if curve:
            vals = list(curve.values())
            result.setdefault(t, {})[c] = {
                "min": round(min(vals), 2),
                "avg": round(sum(vals) / len(vals), 2),
                "max": round(max(vals), 2),
                "count": len(vals),
                "curve": curve,
            }
    return result
