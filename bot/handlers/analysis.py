from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from bot.core.analyzer import analyze_prices, get_price_for_distance

BACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("Назад", callback_data="back")]
])

TERMINAL_MAP = {
    "НЗТ": "НЗТ",
    "НКХП": "НКХП",
    "КСК": "КСК",
    "Тамань": "Тамань",
    "Азов": "Азов",
}


async def analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /analysis, /analysis НЗТ, /analysis НЗТ 350."""
    args = context.args or []

    if len(args) == 0:
        # All terminals
        text = _format_analysis(analyze_prices())
    elif len(args) == 1:
        terminal = args[0]
        text = _format_analysis(analyze_prices(terminal=terminal), terminal)
    else:
        terminal = args[0]
        try:
            distance = int(args[1])
        except ValueError:
            await update.message.reply_text(
                "Использование: /analysis [терминал] [расстояние]",
                reply_markup=BACK_BUTTON,
            )
            return
        price_data = get_price_for_distance(terminal, distance)
        if not price_data:
            text = f"Нет данных для {terminal} на {distance} км"
        else:
            text = (
                f"Анализ цен: {terminal}, {distance} км\n\n"
                f"Мин: {price_data['min']} руб/кг\n"
                f"Средняя: {price_data['avg']} руб/кг\n"
                f"Макс: {price_data['max']} руб/кг\n"
                f"Заявок: {price_data['count']}"
            )

    await update.message.reply_text(text, reply_markup=BACK_BUTTON)


async def analysis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, terminal: str | None = None) -> None:
    """Handle analysis menu button callbacks."""
    query = update.callback_query
    data = analyze_prices(terminal=terminal)
    text = _format_analysis(data, terminal)
    await query.edit_message_text(text, reply_markup=BACK_BUTTON)


def _format_analysis(data: dict, terminal: str | None = None) -> str:
    if not data:
        title = f"Анализ цен: {terminal}" if terminal else "Анализ цен: все терминалы"
        return f"{title}\n\nНет данных в архиве."

    lines = []
    title = f"Анализ цен: {terminal}" if terminal else "Анализ цен: все терминалы"
    lines.append(title)
    lines.append("")

    for term, ranges in data.items():
        if not terminal:
            lines.append(f"📍 {term}")
        for label, stats in sorted(ranges.items(), key=lambda x: _sort_range(x[0])):
            lines.append(
                f"  {label} км: "
                f"{stats['min']}-{stats['max']} руб/кг "
                f"(ср. {stats['avg']}, n={stats['count']})"
            )
        lines.append("")

    text = "\n".join(lines).strip()
    if len(text) > 4096:
        text = text[:4090] + "\n..."
    return text


def _sort_range(label: str) -> int:
    """Sort range labels numerically."""
    first = label.split("-")[0].replace("+", "")
    try:
        return int(first)
    except ValueError:
        return 9999
