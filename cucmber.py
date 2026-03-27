import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart

TOKEN = "8779834120:AAE_gGbE5RgOd_vZj0XoQgjB-JmP0wJRq5o"

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("Привет! Я эхо-бот 😊 Напиши что-нибудь.")

# Эхо-обработчик
@dp.message()
async def echo_handler(message: types.Message):
    await message.answer(message.text)

# Запуск бота
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())