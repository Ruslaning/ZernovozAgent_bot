import httpx
from datetime import datetime, timedelta
from config import API_URL

ARCHIVE_DAYS = 5  # only fetch applications from last N days


async def fetch_applications(days: int = ARCHIVE_DAYS) -> list[dict]:
    """
    Fetch applications from zernovozam API.
    Stops pagination when created_at is older than `days` days.
    """
    cutoff = datetime.now() - timedelta(days=days)
    all_apps: list[dict] = []
    page = 1

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(API_URL, params={"page": page})
            resp.raise_for_status()
            body = resp.json()
            apps = body["data"]["applications"]
            if not apps:
                break

            stop = False
            for app in apps:
                created = app.get("created_at", "")
                try:
                    dt = datetime.strptime(created, "%Y-%m-%d %H:%M:%S")
                    if dt < cutoff:
                        stop = True
                        break
                except:
                    pass
                all_apps.append(app)

            if stop:
                break

            pagination = body["data"].get("pagination")
            if pagination is None or page >= pagination.get("last_page", page):
                break
            page += 1

    return all_apps
