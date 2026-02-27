import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from dotenv import load_dotenv

from app.db import db
from app.llm_service import GigaChatClient
from app.metrics_service import MetricsService

load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")

async def main() -> None:
    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    llm = GigaChatClient()
    metrics = MetricsService()

    @dp.message(F.text)
    async def handle(message: Message) -> None:
        text = (message.text or "").strip()
        if text == "/start":
            await message.answer("Ок")
            return
        try:
            sql = await llm.sql(text)
            res = await metrics.run_sql(sql)
            await message.answer(str(res.value))
        except Exception:
            await message.answer("0")

    await db.connect()
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())