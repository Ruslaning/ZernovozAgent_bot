import sqlite3
from statistics import mean


def rank_applications(rows: list[sqlite3.Row], top_n: int = 10, limit: int = None) -> list[dict]:
    """
    Filter outliers (price deviation > 0.6 rub/kg from average),
    compute metrics, return top N by rub_per_km descending.
    """
    valid = [r for r in rows if r["transportation_price"] and r["distance"] and r["distance"] > 0]
    if not valid:
        return []

    avg_price = mean(r["transportation_price"] for r in valid)

    filtered = [r for r in valid if abs(r["transportation_price"] - avg_price) <= 0.6]
    if not filtered:
        return []

    ranked: list[dict] = []
    for r in filtered:
        price = r["transportation_price"]
        distance = r["distance"]
        load_size = r["load_size"] or 0

        rub_per_km = price * 1000 / distance
        income_per_trip = price * load_size * 1000

        rating_str = "без рейтинга"
        if r["rating_value"] is not None:
            rating_str = f"{r['rating_value']:.1f} ({r['rating_count']})"

        ranked.append({
            "id": r["id"],
            "loading_locality": r["loading_locality"] or "—",
            "stevedore_city": r["stevedore_city"] or "—",
            "distance": distance,
            "culture_title": r["culture_title"] or "—",
            "load_size": load_size,
            "rating": rating_str,
            "price": price,
            "rub_per_km": round(rub_per_km, 2),
            "income_per_trip": round(income_per_trip),
            "organization_name": r["organization_name"] or "—",
        })

    ranked.sort(key=lambda x: x["rub_per_km"], reverse=True)
    n = limit if limit is not None else top_n
    return ranked[:n]
