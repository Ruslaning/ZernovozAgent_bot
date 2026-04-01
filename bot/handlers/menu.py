from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from bot.data.api_client import fetch_applications
from bot.data.db import upsert_applications, get_all_applications
from bot.core.ranker import rank_applications
from datetime import datetime


MAIN_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("TOP 20 сделок", callback_data="top20")],
    [InlineKeyboardButton("Сколько заявок сейчас", callback_data="count")],
])


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привет! Я Зерновоз-Агент.\nВыбери действие:",
        reply_markup=MAIN_MENU,
    )


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Выбери действие:", reply_markup=MAIN_MENU)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "top20":
        await query.edit_message_text("Загружаю данные из API...")
        try:
            apps = await fetch_applications()
            upsert_applications(apps)
        except Exception as exc:
            await query.edit_message_text(f"Ошибка загрузки: {exc}")
            return

        rows = get_all_applications()
        ranked = rank_applications(rows, limit=20)

        if not ranked:
            await query.edit_message_text("Нет подходящих заявок.")
            return

        today = datetime.now().strftime("%d.%m.%Y")
        lines = [f"TOP-20 прибыльных загрузок ({today})\n"]
        for i, r in enumerate(ranked, 1):
            lines.append(
                f"{i}. {r['loading_locality']} -> {r['stevedore_city']} ({r['distance']} км)\n"
                f"   {r['culture_title']} · {r['load_size']}т · {r['rating']}\n"
                f"   {r['price']} руб/кг · {r['rub_per_km']} руб/км · {r['income_per_trip']} руб/рейс\n"
                f"   {r['organization_name']}\n"
            )

        text = "\n".join(lines)
        if len(text) > 4096:
            text = text[:4090] + "\n..."
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Назад", callback_data="back")]
        ]))

    elif query.data == "count":
        await query.edit_message_text("Загружаю данные из API...")
        try:
            apps = await fetch_applications()
        except Exception as exc:
            await query.edit_message_text(f"Ошибка: {exc}")
            return

        total = len(apps)
        # Разбивка по культурам
        cultures = {}
        for a in apps:
            c = a.get("culture_title", "Прочее")
            cultures[c] = cultures.get(c, 0) + 1

        top_cultures = sorted(cultures.items(), key=lambda x: x[1], reverse=True)[:5]
        lines = [f"Заявок в API прямо сейчас: {total}\n", "По культурам (топ-5):"]
        for name, cnt in top_cultures:
            lines.append(f"  {name}: {cnt}")

        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Назад", callback_data="back")]
        ]))

    elif query.data == "back":
        await query.edit_message_text("Выбери действие:", reply_markup=MAIN_MENU)
