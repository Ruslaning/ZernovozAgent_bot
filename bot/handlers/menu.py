from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from bot.data.api_client import fetch_applications
from bot.data.db import upsert_applications, get_all_applications
from bot.core.ranker import rank_applications
from bot.handlers.archive import archive_callback, export_archive_excel
from bot.handlers.analysis import analysis_callback
from bot.handlers.price import settings_callback
from datetime import datetime


MAIN_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("TOP-20 погрузок", callback_data="top20")],
    [InlineKeyboardButton("Сколько заявок сейчас", callback_data="count")],
    [InlineKeyboardButton("Анализ цен", callback_data="analysis_menu")],
    [InlineKeyboardButton("Цена хозяйства", callback_data="price_menu")],
    [InlineKeyboardButton("Архив", callback_data="archive_menu")],
    [InlineKeyboardButton("Настройки", callback_data="settings_menu")],
])

ANALYSIS_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("Все терминалы", callback_data="analysis_all")],
    [
        InlineKeyboardButton("НЗТ", callback_data="analysis_НЗТ"),
        InlineKeyboardButton("НКХП", callback_data="analysis_НКХП"),
        InlineKeyboardButton("КСК", callback_data="analysis_КСК"),
    ],
    [
        InlineKeyboardButton("Тамань", callback_data="analysis_Тамань"),
        InlineKeyboardButton("Азов", callback_data="analysis_Азов"),
    ],
    [InlineKeyboardButton("Назад", callback_data="back")],
])

ARCHIVE_MENU = InlineKeyboardMarkup([
    [
        InlineKeyboardButton("За 7 дней", callback_data="archive_7"),
        InlineKeyboardButton("За 30 дней", callback_data="archive_30"),
    ],
    [InlineKeyboardButton("Выгрузить Excel", callback_data="archive_excel")],
    [InlineKeyboardButton("Назад", callback_data="back")],
])

SETTINGS_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("Портовые цены", callback_data="settings_port_prices")],
    [InlineKeyboardButton("Изменить маржу", callback_data="settings_margin")],
    [InlineKeyboardButton("Изменить расходы", callback_data="settings_expenses")],
    [InlineKeyboardButton("Назад", callback_data="back")],
])

BACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("Назад", callback_data="back")]
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
    data = query.data

    if data == "top20":
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
            await query.edit_message_text("Нет подходящих заявок.", reply_markup=BACK_BUTTON)
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
        await query.edit_message_text(text, reply_markup=BACK_BUTTON)

    elif data == "count":
        import sqlite3
        from config import DB_PATH
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM applications_archive")
        total = cur.fetchone()[0]
        cur.execute("""SELECT culture, COUNT(*) as cnt FROM applications_archive
                       GROUP BY culture ORDER BY cnt DESC LIMIT 5""")
        top_cultures = cur.fetchall()
        cur.execute("SELECT MIN(fetched_at), MAX(fetched_at) FROM applications_archive")
        period = cur.fetchone()
        conn.close()

        lines = [f"Заявок в архиве: {total}\n"]
        if period[0]:
            from datetime import datetime
            dt_from = datetime.fromisoformat(period[0]).strftime("%d.%m %H:%M")
            dt_to = datetime.fromisoformat(period[1]).strftime("%d.%m %H:%M")
            lines.append(f"Период: {dt_from} — {dt_to}\n")
        lines.append("По культурам (топ-5):")
        for name, cnt in top_cultures:
            lines.append(f"  {name}: {cnt}")

        await query.edit_message_text("\n".join(lines), reply_markup=BACK_BUTTON)

    # --- Submenus ---

    elif data == "analysis_menu":
        await query.edit_message_text("Анализ цен — выберите терминал:", reply_markup=ANALYSIS_MENU)

    elif data == "archive_menu":
        await query.edit_message_text("Архив заявок — выберите период:", reply_markup=ARCHIVE_MENU)

    elif data == "price_menu":
        await query.edit_message_text(
            "Цена хозяйства\n\n"
            "Используйте команду:\n"
            "/price [населённый пункт]\n"
            "/price [населённый пункт] [культура]",
            reply_markup=BACK_BUTTON,
        )

    elif data == "settings_menu":
        await query.edit_message_text("Настройки:", reply_markup=SETTINGS_MENU)

    # --- Analysis callbacks ---

    elif data == "analysis_all":
        await query.edit_message_text("Загружаю анализ...")
        await analysis_callback(update, context, terminal=None)

    elif data.startswith("analysis_"):
        terminal = data.replace("analysis_", "")
        await query.edit_message_text(f"Загружаю анализ для {terminal}...")
        await analysis_callback(update, context, terminal=terminal)

    # --- Archive callbacks ---

    elif data == "archive_7":
        await query.edit_message_text("Загружаю статистику...")
        await archive_callback(update, context, days=7)

    elif data == "archive_30":
        await query.edit_message_text("Загружаю статистику...")
        await archive_callback(update, context, days=30)

    elif data == "archive_excel":
        await export_archive_excel(update, context)

    # --- Settings callbacks ---

    elif data == "settings_port_prices":
        await settings_callback(update, context)

    elif data == "settings_margin":
        await query.edit_message_text(
            "Чтобы изменить маржу, используйте команду:\n/set_margin [значение]\n\nПример: /set_margin 300",
            reply_markup=BACK_BUTTON,
        )

    elif data == "settings_expenses":
        await query.edit_message_text(
            "Чтобы изменить доп. расходы, используйте команду:\n/set_expenses [значение]\n\nПример: /set_expenses 250",
            reply_markup=BACK_BUTTON,
        )

    elif data == "back":
        await query.edit_message_text("Выбери действие:", reply_markup=MAIN_MENU)
