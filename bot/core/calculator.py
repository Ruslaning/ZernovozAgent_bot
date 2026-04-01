from bot.core.analyzer import get_price_for_distance
from bot.data.db import get_port_prices, get_user_settings, find_distance, DB_PATH
from rapidfuzz import fuzz, process
import sqlite3


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

    # Get unique localities
    localities = list({r["loading_locality"] for r in rows if r["loading_locality"]})
    match = process.extractOne(address, localities, scorer=fuzz.token_sort_ratio)
    if not match or match[1] < 40:
        return None

    best_locality = match[0]
    score = match[1]

    # Collect distances to each terminal from archive
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
    transport_price_per_ton = avg_price_per_kg * 1000
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

    # Filter by culture if specified
    if culture:
        port_prices = [p for p in port_prices if p["culture"].lower() == culture.lower()]

    results = []
    for pp in port_prices:
        terminal = pp["terminal"]
        port_price = pp["price"]
        cult = pp["culture"]

        # Find distance to this terminal
        dist_km = dist_data["distances"].get(terminal)
        if not dist_km or dist_km <= 0:
            continue

        # Get transport price from analyzer
        price_data = get_price_for_distance(terminal, int(dist_km))
        if not price_data:
            continue

        transport_price_per_ton = price_data["avg"] * 1000
        farm_price = port_price - transport_price_per_ton - margin - extra_expenses

        results.append({
            "terminal": terminal,
            "culture": cult,
            "port_price": port_price,
            "distance_km": dist_km,
            "transport_per_ton": round(transport_price_per_ton),
            "margin": margin,
            "extra_expenses": extra_expenses,
            "farm_price": round(farm_price),
        })

    if not results:
        return None

    return {
        "address": dist_data["address"],
        "match_score": dist_data["score"],
        "prices": results,
    }
