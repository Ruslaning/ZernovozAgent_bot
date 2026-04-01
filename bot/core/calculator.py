from bot.data.db import get_port_prices, get_user_settings, find_distance, DB_PATH
from rapidfuzz import fuzz, process
import sqlite3


def get_coefficient(terminal: str, culture: str, distance_km: int) -> float | None:
    """Get transport coefficient (rub/kg) from loaded Google Sheets data."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Find nearest distance with tolerance +-20km
    cur.execute("""
        SELECT coefficient, distance_km
        FROM transport_coefficients
        WHERE terminal = ? AND culture = ?
        ORDER BY ABS(distance_km - ?) ASC
        LIMIT 1
    """, (terminal, culture, distance_km))
    row = cur.fetchone()

    # If not found by culture, try any culture for this terminal
    if not row:
        cur.execute("""
            SELECT coefficient, distance_km
            FROM transport_coefficients
            WHERE terminal = ?
            ORDER BY ABS(distance_km - ?) ASC
            LIMIT 1
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
        FROM applications_archive
        WHERE distance > 0
    """)
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return None

    localities = list({r["loading_locality"] for r in rows if r["loading_locality"]})
    match = process.extractOne(address, localities, scorer=fuzz.token_sort_ratio)
    if not match or match[1] < 40:
        return None

    best_locality = match[0]
    score = match[1]

    terminal_distances = {}
    for r in rows:
        if r["loading_locality"] == best_locality and r["unloading_terminal"]:
            t = r["unloading_terminal"]
            if t not in terminal_distances:
                terminal_distances[t] = r["distance"]

    if not terminal_distances:
        return None

    return {
        "address": best_locality,
        "score": score,
        "distances": terminal_distances,
        "source": "archive"
    }


def calculate_farm_prices(address: str, user_id: int, culture: str | None = None) -> dict | None:
    """
    Calculate farm prices for a given address.
    farm_price = port_price - transport_price_per_ton - margin - extra_expenses
    transport_price_per_ton = coefficient (rub/kg) * 1000
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

    # Get all available terminals from coefficients table
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT terminal FROM transport_coefficients")
    all_terminals = {r[0] for r in cur.fetchall()}
    conn.close()

    results = []
    processed_terminals = set()

    for pp in port_prices:
        terminal = pp["terminal"]
        port_price = pp["price"]
        cult = pp["culture"]

        # Get distance to this terminal
        dist_km = dist_data["distances"].get(terminal)

        # If not found directly, try Novorossiysk for НЗТ/НКХП/КСК
        if not dist_km and terminal in ("НЗТ", "НКХП", "КСК"):
            dist_km = dist_data["distances"].get("Новороссийск") or dist_data["distances"].get("НЗТ")

        if not dist_km or dist_km <= 0:
            continue

        coef = get_coefficient(terminal, cult, int(dist_km))
        if not coef:
            continue

        transport_per_ton = round(coef * 1000)
        farm_price = port_price - transport_per_ton - margin - extra_expenses

        key = f"{terminal}_{cult}"
        if key in processed_terminals:
            continue
        processed_terminals.add(key)

        results.append({
            "terminal": terminal,
            "culture": cult,
            "port_price": port_price,
            "distance_km": dist_km,
            "coef_per_kg": coef,
            "transport_per_ton": transport_per_ton,
            "margin": margin,
            "extra_expenses": extra_expenses,
            "farm_price": round(farm_price),
        })

    if not results:
        return None

    results.sort(key=lambda x: x["farm_price"], reverse=True)

    return {
        "address": dist_data["address"],
        "match_score": dist_data.get("score", 100),
        "prices": results,
    }
