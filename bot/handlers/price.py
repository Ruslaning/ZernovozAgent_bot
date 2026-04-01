from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from bot.core.calculator import calculate_farm_prices
from bot.data.db import get_port_prices, get_user_settings, set_port_price, set_user_margin, set_user_expenses

BACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("Назад", callback_data="back")]
])


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /price [location] [culture]."""
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Использование:\n"
            "/price [населённый пункт] - расчёт для всех культур\n"
            "/price [населённый пункт] [культура] - для конкретной культуры",
            reply_markup=BACK_BUTTON,
        )
        return

    user_id = update.effective_user.id

    # Last arg could be a culture name
    culture = None
    location = " ".join(args)

    # Try to detect culture as last argument
    known_cultures = {"пшеница", "ячмень", "кукуруза", "подсолнечник", "рапс", "горох", "соя"}
    if len(args) >= 2 and args[-1].lower() in known_cultures:
        culture = args[-1].capitalize()
        location = " ".join(args[:-1])

    result = calculate_farm_prices(location, user_id, culture)
    if not result:
        await update.message.reply_text(
            f"Не удалось найти данные для '{location}'.\n"
            "Проверьте название или убедитесь, что адрес есть в таблице расстояний.",
            reply_markup=BACK_BUTTON,
        )
        return

    days = result.get("analysis_days", 5)
    lines = [
        f"Цена хозяйства: {result['address']}",
        f"Анализ за последние {days} дней\n",
    ]

    for p in result["prices"]:
        source_label = "(архив)" if p["source"] == "archive" else "(Excel)"
        transport_str = (f"{p['transport_min']}–{p['transport_max']}"
                        if p["transport_min"] != p["transport_max"]
                        else str(p["transport_avg"]))
        farm_str = (f"{p['farm_min']}–{p['farm_max']}"
                   if p["farm_min"] != p["farm_max"]
                   else str(p["farm_avg"]))
        count_str = f" · {p['data_count']} заявок" if p["data_count"] > 0 else ""

        lines.append(
            f"-> {p['terminal']} ({p['distance_km']} км) {source_label}{count_str}\n"
            f"  Порт: {p['port_price']} руб/т\n"
            f"  Перевозка: {transport_str} руб/т\n"
            f"  Цена покупки: {farm_str} руб/т\n"
        )

    text = "\n".join(lines).strip()
    if len(text) > 4096:
        text = text[:4090] + "\n..."
    await update.message.reply_text(text, reply_markup=BACK_BUTTON)


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
