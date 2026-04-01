from bot.data.db import get_port_prices, get_user_settings, find_distance, DB_PATH
from bot.core.analyzer import get_price_for_distance, ANALYSIS_DAYS
from rapidfuzz import fuzz, process
import sqlite3


def get_coefficient_static(terminal: str, culture: str, distance_km: int) -> float | None:
    """Fallback: get coefficient from static Excel data (transport_coefficients table)."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT coefficient FROM transport_coefficients
        WHERE terminal = ? AND culture = ?
        ORDER BY ABS(distance_km - ?) ASC LIMIT 1
    """, (terminal, culture, distance_km))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            SELECT coefficient FROM transport_coefficients
            WHERE terminal = ?
            ORDER BY ABS(distance_km - ?) ASC LIMIT 1
        """, (terminal, distance_km))
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def find_distance_from_archive(address: str) -> dict | None:
    """Fallback: find distances from archive by fuzzy matching loading_locality."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT loading_locality, unloading_terminal, distance
        FROM applications_archive WHERE distance > 0
    """)
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return None

    localities = list({r["loading_locality"] for r in rows if r["loading_locality"]})
    match = process.extractOne(address, localities, scorer=fuzz.partial_ratio)
    if not match or match[1] < 50:
        return None

    best_locality = match[0]
    terminal_distances = {}
    for r in rows:
        if r["loading_locality"] == best_locality and r["unloading_terminal"]:
            t = r["unloading_terminal"]
            if t not in terminal_distances:
                terminal_distances[t] = r["distance"]

    return {"address": best_locality, "score": match[1], "distances": terminal_distances, "source": "archive"}


def calculate_farm_prices(address: str, user_id: int, culture: str | None = None) -> dict | None:
    """
    farm_price = port_price - transport_per_ton - margin - extra_expenses
    Transport price: first try dynamic (from archive last 5 days), then static (Excel).
    """
    dist_data = find_distance(address)
    if not dist_data:
        dist_data = find_distance_from_archive(address)
    if not dist_data:
        return None

    settings = get_user_settings(user_id)
    margin = settings["margin"]
    extra_expenses = settings["extra_expenses"]

    port_prices = get_port_prices()
    if culture:
        port_prices = [p for p in port_prices if p["culture"].lower() == culture.lower()]

    results = []
    processed = set()

    for pp in port_prices:
        terminal = pp["terminal"]
        port_price = pp["price"]
        cult = pp["culture"]

        key = f"{terminal}_{cult}"
        if key in processed:
            continue

        dist_km = dist_data["distances"].get(terminal)
        if not dist_km and terminal in ("НЗТ", "НКХП", "КСК"):
            dist_km = (dist_data["distances"].get("Новороссийск") or
                       dist_data["distances"].get("НЗТ") or
                       dist_data["distances"].get("НКХП"))
        if not dist_km or dist_km <= 0:
            continue

        # Try dynamic price from archive (last 5 days)
        price_data = get_price_for_distance(terminal, int(dist_km), cult, ANALYSIS_DAYS)
        source = "archive"

        # Fallback to static Excel coefficients
        if not price_data:
            coef = get_coefficient_static(terminal, cult, int(dist_km))
            if coef:
                price_data = {"min": coef, "avg": coef, "max": coef, "count": 0}
                source = "excel"

        if not price_data:
            continue

        transport_min = round(price_data["min"] * 1000)
        transport_avg = round(price_data["avg"] * 1000)
        transport_max = round(price_data["max"] * 1000)

        farm_max = port_price - transport_min - margin - extra_expenses
        farm_avg = port_price - transport_avg - margin - extra_expenses
        farm_min = port_price - transport_max - margin - extra_expenses

        processed.add(key)
        results.append({
            "terminal": terminal,
            "culture": cult,
            "port_price": port_price,
            "distance_km": dist_km,
            "transport_min": transport_min,
            "transport_avg": transport_avg,
            "transport_max": transport_max,
            "farm_min": round(farm_min),
            "farm_avg": round(farm_avg),
            "farm_max": round(farm_max),
            "data_count": price_data["count"],
            "source": source,
            "margin": margin,
            "extra_expenses": extra_expenses,
        })

    if not results:
        return None

    results.sort(key=lambda x: x["farm_max"], reverse=True)
    return {
        "address": dist_data["address"],
        "match_score": dist_data.get("score", 100),
        "analysis_days": ANALYSIS_DAYS,
        "prices": results,
    }
