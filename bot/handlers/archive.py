import io
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from bot.data.db import get_archive_stats, get_archive_for_export

BACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("Назад", callback_data="back")]
])


async def archive_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /archive, /archive N (days)."""
    days = 30
    if context.args:
        try:
            days = int(context.args[0])
        except ValueError:
            pass

    stats = get_archive_stats(days)
    text = _format_stats(stats, days)
    await update.message.reply_text(text, reply_markup=BACK_BUTTON)


def _format_stats(stats: dict, days: int) -> str:
    lines = [
        f"📦 Архив заявок за {days} дней",
        f"Период: {stats['period_start']} — {stats['period_end']}",
        f"Всего: {stats['total']}",
        f"Новых сегодня: {stats['new_today']}",
        f"Новых за неделю: {stats['new_week']}",
        "",
        "Топ терминалов:",
    ]
    for name, cnt in stats["top_terminals"]:
        lines.append(f"  {name}: {cnt}")
    return "\n".join(lines)


async def archive_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, days: int) -> None:
    """Handle archive menu button callbacks."""
    query = update.callback_query
    stats = get_archive_stats(days)
    text = _format_stats(stats, days)
    await query.edit_message_text(text, reply_markup=BACK_BUTTON)


async def export_archive_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export archive to Excel and send as file."""
    query = update.callback_query
    await query.edit_message_text("Формирую Excel файл...")

    import openpyxl

    rows = get_archive_for_export(days=30)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Архив заявок"

    headers = [
        "ID", "Дата создания", "Организация", "Рейтинг", "Кол-во оценок",
        "Погрузка", "Регион", "Терминал", "Город выгрузки", "Культура",
        "Объём (т)", "Расстояние (км)", "Цена руб/кг", "Цена руб/км",
        "Доход/рейс", "Дата загрузки",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([
            r["id"], r["created_at"], r["organization_name"],
            r["rating_value"], r["rating_count"],
            r["loading_locality"], r["loading_region"],
            r["unloading_terminal"], r["unloading_city"],
            r["culture"], r["load_size"], r["distance"],
            r["price_per_kg"], r["price_per_km"],
            r["revenue_per_trip"], r["fetched_at"],
        ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    today = datetime.now().strftime("%Y-%m-%d")
    await query.message.reply_document(
        document=buf,
        filename=f"archive_{today}.xlsx",
        caption=f"Архив заявок за 30 дней ({len(rows)} записей)",
    )
    await query.edit_message_text(
        "Файл отправлен!",
        reply_markup=BACK_BUTTON,
    )
