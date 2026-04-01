import httpx

from config import API_URL


async def fetch_applications() -> list[dict]:
    """Fetch all applications from zernovozam API, handling pagination."""
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
            all_apps.extend(apps)
            pagination = body["data"].get("pagination")
            if pagination is None or page >= pagination.get("last_page", page):
                break
            page += 1

    return all_apps
