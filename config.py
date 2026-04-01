import os
from pathlib import Path

TELEGRAM_BOT_TOKEN = os.environ.get("ZERNOVOZ_BOT_TOKEN", "8628872882:AAEfDsxEIvG_uWY0zJwYfCKx68rQvGhQ428")

API_URL = "https://api.zernovozam.com/api/applications"

DB_PATH = Path(__file__).parent / "zernovoz.db"

SHEET_IDS = {
    "logistics": "1YbQDb9fQ8K--D9UAF638UBAsGvkfF3oq7rAfiupsCA8",
    "prices": "1Edl-O6AzeRnrPld3qVqModilxUr6Q4XJg6qB8a2i62k",
}

SERVICE_ACCOUNT_PATH = Path(r"C:\Users\Kseni\.openclaw\workspace-reader\service_account.json")
