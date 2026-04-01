import asyncio
import logging

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler

from config import TELEGRAM_BOT_TOKEN
from bot.data.db import init_db, archive_applications
from bot.data.api_client import fetch_applications
from bot.handlers.top import top_command
from bot.handlers.menu import start_command, menu_command, button_handler
from bot.handlers.archive import archive_command
from bot.handlers.analysis import analysis_command
from bot.handlers.price import (
    price_command,
    set_port_price_command,
    set_margin_command,
    set_expenses_command,
    prices_command,
    price_location_callback,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def archive_job(context) -> None:
    """Periodic job: fetch applications from API and archive them."""
    try:
        apps = await fetch_applications()
        count = archive_applications(apps)
        logger.info(f"Archived {count} applications")
    except Exception as exc:
        logger.error(f"Archive job failed: {exc}")


def main() -> None:
    init_db()
    logger.info("Database initialized")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(CommandHandler("top", top_command))
    app.add_handler(CommandHandler("archive", archive_command))
    app.add_handler(CommandHandler("analysis", analysis_command))
    app.add_handler(CommandHandler("price", price_command))
    app.add_handler(CommandHandler("set_port_price", set_port_price_command))
    app.add_handler(CommandHandler("set_margin", set_margin_command))
    app.add_handler(CommandHandler("set_expenses", set_expenses_command))
    app.add_handler(CommandHandler("prices", prices_command))
    app.add_handler(CallbackQueryHandler(price_location_callback, pattern="^pl:"))
    app.add_handler(CallbackQueryHandler(button_handler))

    # Schedule archive job every 15 minutes
    app.job_queue.run_repeating(archive_job, interval=900, first=10)
    logger.info("Archive job scheduled (every 15 min)")

    logger.info("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main()
