from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from bot.core.calculator import calculate_farm_prices
from bot.data.db import get_port_prices, get_user_settings, set_port_price, set_user_margin, set_user_expenses

# Temporary cache: user_id -> {idx: (locality, culture)}
_price_cache: dict[int, dict[int, tuple[str, str | None]]] = {}

BACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("Назад", callback_data="back")]
])


def find_top_localities(address: str, limit: int = 5) -> list:
    """Find top matching localities from archive."""
    import sqlite3
    from rapidfuzz import fuzz, process
    from bot.data.db import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT loading_locality FROM applications_archive WHERE loading_locality IS NOT NULL")
    localities = [r[0] for r in cur.fetchall()]
    conn.close()
    if not localities:
        return []
    matches = process.extract(address, localities, scorer=fuzz.partial_ratio, limit=limit)
    return [(m[0], m[1]) for m in matches if m[1] >= 50]


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /price [location] [culture]."""
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Использование:\n"
            "/price [населённый пункт]\n"
            "/price [населённый пункт] [культура]",
            reply_markup=BACK_BUTTON,
        )
        return

    user_id = update.effective_user.id
    known_cultures = {"пшеница", "ячмень", "кукуруза", "подсолнечник", "рапс", "горох", "соя"}

    culture = None
    location = " ".join(args)
    if len(args) >= 2 and args[-1].lower() in known_cultures:
        culture = args[-1].capitalize()
        location = " ".join(args[:-1])

    # Find top matches
    matches = find_top_localities(location)
    if not matches:
        await update.message.reply_text(
            f"Пункт '{location}' не найден в архиве заявок.",
            reply_markup=BACK_BUTTON,
        )
        return

    # Store matches in cache, use index in callback_data (max 64 bytes)
    _price_cache[user_id] = {i: (loc, culture) for i, (loc, _) in enumerate(matches)}

    buttons = []
    for i, (loc, score) in enumerate(matches):
        short = loc[:35] + "..." if len(loc) > 35 else loc
        buttons.append([InlineKeyboardButton(
            f"{short} ({score:.0f}%)",
            callback_data=f"pl:{user_id}:{i}"  # max ~20 chars
        )])
    buttons.append([InlineKeyboardButton("Отмена", callback_data="back")])

    await update.message.reply_text(
        f"Уточните пункт погрузки для '{location}':",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _show_price_result(update_or_query, address: str, user_id: int, culture: str | None):
    """Calculate and show farm price result."""
    from bot.core.calculator import calculate_farm_prices

    result = calculate_farm_prices(address, user_id, culture)

    send = (update_or_query.message.reply_text
            if hasattr(update_or_query, 'message')
            else update_or_query.edit_message_text)

    if not result:
        await send(
            f"Нет данных для расчёта по '{address}'.\nНедостаточно заявок в архиве.",
            reply_markup=BACK_BUTTON,
        )
        return

    days = result.get("analysis_days", 5)
    lines = [f"Цена хозяйства: {result['address']}", f"Анализ за {days} дней\n"]

    for p in result["prices"]:
        src = "(архив)" if p["source"] == "archive" else "(Excel)"
        transport_str = (f"{p['transport_min']}–{p['transport_max']}"
                         if p["transport_min"] != p["transport_max"]
                         else str(p["transport_avg"]))
        farm_str = (f"{p['farm_min']}–{p['farm_max']}"
                    if p["farm_min"] != p["farm_max"]
                    else str(p["farm_avg"]))
        cnt = f" · {p['data_count']} зая." if p["data_count"] > 0 else ""
        lines.append(
            f"-> {p['terminal']} ({p['distance_km']} км) {src}{cnt}\n"
            f"  Порт: {p['port_price']} руб/т\n"
            f"  Перевозка: {transport_str} руб/т\n"
            f"  Цена покупки: {farm_str} руб/т\n"
        )

    text = "\n".join(lines).strip()
    if len(text) > 4096:
        text = text[:4090] + "\n..."
    await send(text, reply_markup=BACK_BUTTON)


async def price_location_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle pl: callback — user selected a specific locality by index."""
    query = update.callback_query
    await query.answer()

    # Format: pl:{user_id}:{index}
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.edit_message_text("Ошибка. Попробуйте снова.", reply_markup=BACK_BUTTON)
        return

    uid = int(parts[1])
    idx = int(parts[2])

    cache = _price_cache.get(uid, {})
    if idx not in cache:
        await query.edit_message_text("Данные устарели. Повторите запрос.", reply_markup=BACK_BUTTON)
        return

    locality, culture = cache[idx]
    await _show_price_result(query, locality, uid, culture)


async def set_port_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /set_port_price [terminal] [culture] [price]."""
    args = context.args or []
    if len(args) < 3:
        await update.message.reply_text(
            "Использование: /set_port_price [терминал] [культура] [цена]\n"
            "Пример: /set_port_price НЗТ Пшеница 17000",
            reply_markup=BACK_BUTTON,
        )
        return

    terminal = args[0]
    culture = args[1]
    try:
        price = float(args[2])
    except ValueError:
        await update.message.reply_text("Цена должна быть числом.", reply_markup=BACK_BUTTON)
        return

    set_port_price(terminal, culture, price)
    await update.message.reply_text(
        f"Портовая цена обновлена:\n{culture} → {terminal}: {price} руб/т",
        reply_markup=BACK_BUTTON,
    )


async def set_margin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /set_margin [value]."""
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Использование: /set_margin [значение]\nПример: /set_margin 300",
            reply_markup=BACK_BUTTON,
        )
        return

    try:
        margin = float(args[0])
    except ValueError:
        await update.message.reply_text("Маржа должна быть числом.", reply_markup=BACK_BUTTON)
        return

    user_id = update.effective_user.id
    set_user_margin(user_id, margin)
    await update.message.reply_text(f"Маржа установлена: {margin} руб/т", reply_markup=BACK_BUTTON)


async def set_expenses_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /set_expenses [value]."""
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Использование: /set_expenses [значение]\nПример: /set_expenses 250",
            reply_markup=BACK_BUTTON,
        )
        return

    try:
        expenses = float(args[0])
    except ValueError:
        await update.message.reply_text("Расходы должны быть числом.", reply_markup=BACK_BUTTON)
        return

    user_id = update.effective_user.id
    set_user_expenses(user_id, expenses)
    await update.message.reply_text(f"Доп. расходы установлены: {expenses} руб/т", reply_markup=BACK_BUTTON)


async def prices_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /prices - show current port prices and user settings."""
    user_id = update.effective_user.id
    port_prices = get_port_prices()
    settings = get_user_settings(user_id)

    lines = ["Текущие настройки\n"]
    lines.append(f"Маржа: {settings['margin']} руб/т")
    lines.append(f"Доп. расходы: {settings['extra_expenses']} руб/т")
    lines.append("\nПортовые цены:")

    for pp in port_prices:
        lines.append(f"  {pp['culture']} → {pp['terminal']}: {pp['price']} руб/т")

    await update.message.reply_text("\n".join(lines), reply_markup=BACK_BUTTON)


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle settings menu - show current settings via callback."""
    query = update.callback_query
    user_id = update.effective_user.id
    port_prices = get_port_prices()
    settings = get_user_settings(user_id)

    lines = ["Текущие настройки\n"]
    lines.append(f"Маржа: {settings['margin']} руб/т")
    lines.append(f"Доп. расходы: {settings['extra_expenses']} руб/т")
    lines.append("\nПортовые цены:")
    for pp in port_prices:
        lines.append(f"  {pp['culture']} → {pp['terminal']}: {pp['price']} руб/т")
    lines.append("\nИспользуйте команды:")
    lines.append("/set_port_price [терминал] [культура] [цена]")
    lines.append("/set_margin [значение]")
    lines.append("/set_expenses [значение]")

    await query.edit_message_text("\n".join(lines), reply_markup=BACK_BUTTON)
