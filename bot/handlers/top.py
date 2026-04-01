from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from bot.data.api_client import fetch_applications
from bot.data.db import upsert_applications, get_all_applications, init_db
from bot.core.ranker import rank_applications


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /top — refresh data from API and show TOP-10 profitable loads."""
    await update.message.reply_text("Загружаю данные...")

    try:
        apps = await fetch_applications()
        upsert_applications(apps)
    except Exception as exc:
        await update.message.reply_text(f"Ошибка загрузки данных: {exc}")
        return

    rows = get_all_applications()
    ranked = rank_applications(rows)

    if not ranked:
        await update.message.reply_text("Нет подходящих заявок.")
        return

    today = datetime.now().strftime("%d.%m.%Y")
    lines = [f"TOP прибыльных загрузок ({today})\n"]

    for i, r in enumerate(ranked, 1):
        lines.append(
            f"{i}. {r['loading_locality']} → {r['stevedore_city']} ({r['distance']} км)\n"
            f"   {r['culture_title']} · {r['load_size']}т · {r['rating']}\n"
            f"   {r['price']} руб/кг · {r['rub_per_km']} руб/км · {r['income_per_trip']} руб/рейс\n"
            f"   {r['organization_name']}"
        )

    text = "\n".join(lines)
    # Telegram message limit is 4096 chars
    if len(text) > 4096:
        text = text[:4090] + "\n..."
    await update.message.reply_text(text)
