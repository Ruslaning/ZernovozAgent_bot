import httpx
from datetime import datetime, timedelta
from config import API_URL

ARCHIVE_DAYS = 5  # only fetch applications from last N days


async def fetch_applications(days: int = ARCHIVE_DAYS) -> list[dict]:
    """
    Fetch fresh applications from zernovozam API.
    API pages are NOT sorted by date, so we must scan all pages.
    Returns only applications created within last `days` days.
    """
    cutoff = datetime.now() - timedelta(days=days)
    all_apps: list[dict] = []

    async with httpx.AsyncClient(timeout=30) as client:
        # Get total pages
        resp = await client.get(API_URL, params={"page": 1})
        resp.raise_for_status()
        body = resp.json()
        last_page = body["data"]["pagination"].get("last_page", 1)

        # Collect from page 1 too
        for app in body["data"]["applications"]:
            created = app.get("created_at", "")
            try:
                if datetime.strptime(created, "%Y-%m-%d %H:%M:%S") >= cutoff:
                    all_apps.append(app)
            except Exception:
                pass

        # Read all remaining pages
        for page in range(2, last_page + 1):
            resp = await client.get(API_URL, params={"page": page})
            resp.raise_for_status()
            apps = resp.json()["data"]["applications"]
            for app in apps:
                created = app.get("created_at", "")
                try:
                    if datetime.strptime(created, "%Y-%m-%d %H:%M:%S") >= cutoff:
                        all_apps.append(app)
                except Exception:
                    pass

    return all_apps
