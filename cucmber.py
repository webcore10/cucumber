import asyncio
import random
import aiosqlite
from datetime import datetime, timedelta
from aiogram.types import LabeledPrice, PreCheckoutQuery
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BotCommand
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


import pytz
from datetime import datetime, timedelta

MSK = pytz.timezone("Europe/Moscow")



TOKEN = "8779834120:AAE_gGbE5RgOd_vZj0XoQgjB-JmP0wJRq5o"

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
DB_NAME = "data/cucumbers.db"
provider_token = ""  # ПУСТАЯ СТРОКА!

def now_msk():
    return datetime.now(MSK)

# -------------------- АДМИН-ПАНЕЛЬ --------------------

class BroadcastState(StatesGroup):
    waiting_message = State()

ADMIN_ID = 5971748042  # твой ID

@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="📊 Список групп", callback_data="admin_groups")],
            [InlineKeyboardButton(text="🧠 Создать задачу", callback_data="admin_task")]
        ]
    )
    await message.answer("⚙️ Админ-панель", reply_markup=kb)

@dp.callback_query(F.data == "admin_broadcast")
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return

    await callback.message.answer("📩 Отправь сообщение для рассылки")
    await state.set_state(BroadcastState.waiting_message)

    await callback.answer()


@dp.message(BroadcastState.waiting_message)
async def process_broadcast(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT DISTINCT chat_id FROM user_chats")
        chats = await cursor.fetchall()

    success = 0
    failed = 0

    for (chat_id,) in chats:
        try:
            await message.copy_to(chat_id)
            success += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1

            # удаляем мёртвые чаты
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "DELETE FROM user_chats WHERE chat_id=?",
                    (chat_id,)
                )
                await db.commit()

    await message.answer(
        f"📢 Рассылка завершена\n\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибки: {failed}"
    )

    await state.clear()

#-------------------- СПИСОК ГРУПП --------------------

@dp.callback_query(F.data == "admin_groups")
async def admin_groups(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT DISTINCT chat_id FROM user_chats"
        )
        chats = await cursor.fetchall()

    if not chats:
        await callback.message.answer("❌ Бот нигде не используется")
        return

    text = "📊 Группы с ботом:\n\n"

    for (chat_id,) in chats:
        try:
            chat = await bot.get_chat(chat_id)
            title = (chat.title or "Без названия").replace("<", "").replace(">", "")

            # считаем игроков
            async with aiosqlite.connect(DB_NAME) as db:
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM users WHERE chat_id=?",
                    (chat_id,)
                )
                count = (await cursor.fetchone())[0]

            # 🔗 ССЫЛКА
            if chat.username:
                link = f"https://t.me/{chat.username}"
            else:
                try:
                    invite = await bot.create_chat_invite_link(chat_id)
                    link = invite.invite_link
                except:
                    link = "❌ Нет доступа к ссылке"

            text += (
                f"• <b>{title}</b>\n"
                f"👥 Игроков: {count}\n"
                f"🔗 {link}\n"
                f"🆔 <code>{chat_id}</code>\n\n"
            )

        except:
            text += f"• ❌ Недоступная группа\n🆔 <code>{chat_id}</code>\n\n"

    await callback.message.answer(text)
    await callback.answer()


#-------------------- СОЗДАНИЕ ЗАДАЧИ --------------------
class TaskState(StatesGroup):
    question = State()
    answer = State()
    reward = State()


@dp.callback_query(F.data == "admin_task")
async def start_task(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return

    await callback.message.answer("✏️ Введи задачу:")
    await state.set_state(TaskState.question)
    await callback.answer()


@dp.message(TaskState.question)
async def task_question(message: Message, state: FSMContext):
    await state.update_data(question=message.text)
    await message.answer("✅ Теперь введи правильный ответ:")
    await state.set_state(TaskState.answer)


@dp.message(TaskState.answer)
async def task_answer(message: Message, state: FSMContext):
    await state.update_data(answer=message.text.lower())
    await message.answer("💰 Введи награду (см):")
    await state.set_state(TaskState.reward)

ACTIVE_TASK = {}  # chat_id: {answer, reward}

@dp.message(TaskState.reward)
async def task_reward(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❗ Введи число")
        return

    reward = int(message.text)
    data = await state.get_data()

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT DISTINCT chat_id FROM user_chats")
        chats = await cursor.fetchall()

    for (chat_id,) in chats:
        try:
            await bot.send_message(
                chat_id,
                f"🧠 ЗАДАЧА!\n\n"
                f"{data['question']}\n\n"
                f"💰 Награда: {reward} см\n"
                f"✍️ Напиши ответ в чат \n ‼️Если вы отправляете праввильный ответ, но не получаете награду, значит, эта задача уже решена в другой группе."
            )

            ACTIVE_TASK[chat_id] = {
                "answer": data["answer"],
                "reward": reward,
                "active": True
            }

        except:
            pass

    await message.answer("✅ Задача отправлена!")
    await state.clear()



@dp.message(F.text & ~F.text.startswith("/"))
async def check_answer(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if chat_id not in ACTIVE_TASK:
        return

    task = ACTIVE_TASK[chat_id]

    if not task.get("active"):
        return

    user_answer = message.text.lower().strip()
    correct_answer = task["answer"].lower().strip()

    if user_answer == correct_answer:
        size, _ = await get_user(user_id, chat_id, message.from_user.full_name)

        size += task["reward"]
        await update_size(user_id, chat_id, size)

        await message.answer(
            f"🎉 {mention(message.from_user)} решил задачу!\n"
            f"+{task['reward']} см\n"
            f"📏 Теперь: {size} см"
        )

        task["active"] = False


# -------------------- ЛУТБОКС --------------------
@dp.message(Command("box"))
async def open_box(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    await save_user_chat(user_id, chat_id)

    now = now_msk()

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT last_box FROM users WHERE user_id=? AND chat_id=?",
            (user_id, chat_id)
        )
        row = await cursor.fetchone()

    last_box = row[0] if row else None

    if last_box:
        last_time = datetime.fromisoformat(last_box)
        if last_time.tzinfo is None:
            last_time = MSK.localize(last_time)
        next_time = last_time + timedelta(hours=1)

        if now < next_time:
            remaining = next_time - now

            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)

            await message.answer(
                f"⏳ {mention(message.from_user)}\n"
                f"Ты уже открывал лутбокс!\n\n"
                f"🕐 Снова можно в: {next_time.strftime('%H:%M:%S')} (МСК)\n"
                f"⏱ Осталось: {hours}ч {minutes}м {seconds}с"
            )
            return

    # 🎲 выпадение
    roll = random.randint(1, 100)

    if roll <= 40:
        reward = random.randint(1, 3)
        rarity = "💩 Мусор"
    elif roll <= 70:
        reward = random.randint(4, 8)
        rarity = "🟢 Обычный"
    elif roll <= 90:
        reward = random.randint(9, 15)
        rarity = "🔵 Редкий"
    elif roll <= 99:
        reward = random.randint(16, 30)
        rarity = "🟣 Эпик"
    else:
        reward = random.randint(50, 100)
        rarity = "🟡 Легендарный"

    size, _ = await get_user(user_id, chat_id, message.from_user.full_name)
    size += reward

    await update_size(user_id, chat_id, size)

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET last_box=? WHERE user_id=? AND chat_id=?",
            (now.isoformat(), user_id, chat_id)
        )
        await db.commit()

    await message.answer(
        f"🎁 {mention(message.from_user)} открыл лутбокс!\n\n"
        f"✨ Редкость: {rarity}\n"
        f"💰 Награда: +{reward} см\n\n"
        f"📏 Теперь: {size} см"
    )



# -------------------- ПОКУПКА --------------------

async def save_user_chat(user_id, chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO user_chats (user_id, chat_id) VALUES (?, ?)",
            (user_id, chat_id)
        )
        await db.commit()

async def get_user_chat(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT chat_id FROM user_chats WHERE user_id=?",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else None



@dp.message(Command("shop"))
async def shop(message: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💰 10 см — ⭐10", callback_data="buy_10")],
            [InlineKeyboardButton(text="💰 50 см — ⭐45", callback_data="buy_50")],
            [InlineKeyboardButton(text="💰 100 см — ⭐80", callback_data="buy_100")]
        ]
    )

    await message.answer(
        f"🛒 {mention(message.from_user)}\nВыбери покупку:",
        reply_markup=kb
    )


@dp.callback_query(F.data.startswith("buy_"))
async def buy_handler(call: CallbackQuery):
    data = call.data

    if data == "buy_10":
        amount = 10
        price = 10
    elif data == "buy_50":
        amount = 50
        price = 45
    elif data == "buy_100":
        amount = 100
        price = 80
    else:
        await call.answer("Ошибка")
        return

    prices = [LabeledPrice(label=f"{amount} см", amount=price)]

    await bot.send_invoice(
        chat_id=call.from_user.id,  # важно: в ЛС
        title="Покупка огурца 🥒",
        description=f"Ты покупаешь {amount} см",
        payload=f"cucumber_{amount}",
        provider_token="",  # ⭐ Stars = пусто
        currency="XTR",
        prices=prices,
        start_parameter="cucumber-shop"
    )

    await call.answer("Проверь личные сообщения. Если ничего то нет, то сначала нажми кнопку START в личных сообщениях бота, а потом перейди к покупке в боте.")


@dp.pre_checkout_query()
async def pre_checkout(pre_checkout_q: PreCheckoutQuery):
    await pre_checkout_q.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    amount = int(payload.split("_")[1])

    user_id = message.from_user.id

    # получаем группу
    chat_id = await get_user_chat(user_id)

    if not chat_id:
        await message.answer("❗ Сначала поиграй в группе")
        return

    size, _ = await get_user(user_id, chat_id, message.from_user.full_name)
    size += amount

    await update_size(user_id, chat_id, size)

    # сообщение в ЛС
    await message.answer(
        f"💰 Покупка успешна!\n+{amount} см"
    )

    # сообщение в группу
    await bot.send_message(
        chat_id,
        f"💰 {mention(message.from_user)} купил {amount} см!\n"
        f"Теперь: {size} см"
    )

# -------------------- НАЛОГ --------------------



async def apply_tax(user_id, chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, last_tax FROM users WHERE user_id=? AND chat_id=?",
            (user_id, chat_id)
        )
        row = await cursor.fetchone()

    if not row:
        return None

    size, last_tax = row

    if size < 1000:
        return None

    now = datetime.now()

    if last_tax:
        last_time = datetime.fromisoformat(last_tax)
        if now - last_time < timedelta(days=1):
            return None

    # списываем налог
    k = size//1000
    size -= 30*k
    if size < 0:
        size = 0

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET size=?, last_tax=? WHERE user_id=? AND chat_id=?",
            (size, now.isoformat(), user_id, chat_id)
        )
        await db.commit()

    return size

# -------------------- УТИЛИТЫ --------------------

def mention(user):
    name = (user.full_name or "Игрок").replace("<", "").replace(">", "")
    try:
        return f"<a href='tg://user?id={user.id}'>{name}</a>"
    except:
        return name

# -------------------- БАЗА --------------------

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER,
            chat_id INTEGER,
            size INTEGER DEFAULT 0,
            last_grow TEXT,
            PRIMARY KEY(user_id, chat_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_chats (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_chats (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER
        )
        """)

    


        await db.execute("""
        CREATE TABLE IF NOT EXISTS fights (
            chat_id INTEGER PRIMARY KEY,
            challenger INTEGER,
            amount INTEGER
        )
        """)
        

        await db.commit()

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA table_info(users)")
        cols = [col[1] for col in await (await db.execute("PRAGMA table_info(users)")).fetchall()]

        if "wins" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN wins INTEGER DEFAULT 0")

        if "loses" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN loses INTEGER DEFAULT 0")

        if "max_size" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN max_size INTEGER DEFAULT 0")
        if "name" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN name TEXT")



        await db.commit()
    


#--------------- РОСТ ----------------- 

async def update_size(user_id, chat_id, size):
    async with aiosqlite.connect(DB_NAME) as db:
        # обновляем максимум
        cursor = await db.execute(
            "SELECT max_size FROM users WHERE user_id=? AND chat_id=?",
            (user_id, chat_id)
        )
        row = await cursor.fetchone()
        max_size = row[0] if row and row[0] else 0

        if size > max_size:
            max_size = size

        await db.execute(
            "UPDATE users SET size=?, max_size=? WHERE user_id=? AND chat_id=?",
            (size, max_size, user_id, chat_id)
        )
        await db.commit()




# -------------------- КОМАНДЫ --------------------

async def set_commands(bot: Bot):
    commands = [
        BotCommand(command="start", description="Запуск"),
        BotCommand(command="grow", description="Вырастить огурец 🌱"),
        BotCommand(command="stats", description="Моя статистика 📊"),
        BotCommand(command="top", description="Топ игроков 🏆"),
        BotCommand(command="fight", description="Создать бой ⚔️"),
        BotCommand(command="shop", description="Магазин 🛒"),
        BotCommand(command="box", description="Открыть лутбокс 🎁"),
    ]
    await bot.set_my_commands(commands)


@dp.message(Command("start"))
async def start(message: Message):
    await message.answer("🥒 Огуречный бот активирован!\nИспользуй /grow")


# -------------------- БАЗА ФУНКЦИИ --------------------

async def get_user(user_id, chat_id, name=None):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, last_grow FROM users WHERE user_id=? AND chat_id=?",
            (user_id, chat_id)
        )
        row = await cursor.fetchone()

        if not row:
            await db.execute(
                "INSERT INTO users (user_id, chat_id, size, name) VALUES (?, ?, 0, ?)",
                (user_id, chat_id, name)
            )
            await db.commit()
            return 0, None

        # обновляем имя (если поменялось)
        if name:
            await db.execute(
                "UPDATE users SET name=? WHERE user_id=? AND chat_id=?",
                (name, user_id, chat_id)
            )
            await db.commit()

        return row
    




# -------------------- GROW --------------------

async def update_last_grow(user_id, chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET last_grow=? WHERE user_id=? AND chat_id=?",
            (now_msk().isoformat(), user_id, chat_id)
        )
        await db.commit()




@dp.message(Command("grow"))
async def grow(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    await save_user_chat(user_id, chat_id)
    size, last_grow = await get_user(user_id, chat_id, message.from_user.full_name)

    now = now_msk()

    if last_grow:
        last_time = datetime.fromisoformat(last_grow)
        if last_time.tzinfo is None:
            last_time = MSK.localize(last_time)
        next_time = last_time + timedelta(days=1)

        if now < next_time:
            remaining = next_time - now

            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)

            await message.answer(
                f"⏳ {mention(message.from_user)}\n"
                f"Ты уже выращивал сегодня!\n\n"
                f"🕐 Снова можно в: {next_time.strftime('%H:%M:%S')} (МСК)\n"
                f"⏱ Осталось: {hours}ч {minutes}м {seconds}с"
            )
            return

    growth = random.randint(1, 50)
    size += growth

    await update_size(user_id, chat_id, size)
    await update_last_grow(user_id, chat_id)

    new_size = int(size - growth*20/100)


    await message.answer(
        f"🌱 {mention(message.from_user)}\n"
        f"+{growth} см\nТеперь: {size} см\n"
        f"💸Вы платите налог 20% см от дохода\n"
        f"Теперь: {new_size} см"
    )
    size = new_size
    await update_size(user_id, chat_id, size)






# -------------------- STATS --------------------


@dp.message(Command("stats"))
async def stats(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    # Получаем данные пользователя
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, wins, loses, max_size FROM users WHERE user_id=? AND chat_id=?",
            (user_id, chat_id)
        )
        row = await cursor.fetchone()

    if not row:
        await message.answer("❌ Нет данных")
        return

    size, wins, loses, max_size = row
    wins = wins or 0
    loses = loses or 0
    max_size = max_size or size

    total_battles = wins + loses
    winrate = int((wins / total_battles) * 100) if total_battles > 0 else 0

    # Определяем роль
    if size < 50:
        role = "🚜 Колхозник"
    elif size < 250:
        role = "🏢 Средний класс"
    elif size < 500:
        role = "💼 Предприниматель"
    elif size < 1000:
        role = "📊 Последний в Forbes"
    elif size < 10000:
        role = "🕶 Друг Коломойского"
    else:
        role = "💍 Муж Марички"

    # Отправка текстовой статистики
    await message.answer(
        f"📊 {mention(message.from_user)}\n"
        f"📏 Размер: {size} см\n"
        f"📈 Макс: {max_size} см\n"
        f"🏆 Победы: {wins} / Поражения: {loses}\n"
        f"💯 Winrate: {winrate}%\n"
        f"🎭 Роль: {role}"
    )


# -------------------- TOP --------------------

@dp.message(Command("top"))
async def top(message: Message):
    chat_id = message.chat.id

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT user_id, size, name FROM users WHERE chat_id=? ORDER BY size DESC LIMIT 10",
            (chat_id,)
        )
        rows = await cursor.fetchall()

    if not rows:
        await message.answer("😢 Нет игроков")
        return

    text = "🏆 Топ огурцов:\n\n"

    medals = ["🥇", "🥈", "🥉"]

    for i, (user_id, size, name) in enumerate(rows, 1):
        # безопасное имя
        display_name = (name or "Игрок").replace("<", "").replace(">", "")

        # медали
        prefix = medals[i-1] if i <= 3 else f"{i}."

        # кликабельное имя
        text += f"{prefix} <a href='tg://user?id={user_id}'>{display_name}</a> — {size} см\n"

    await message.answer(text)

# -------------------- FIGHT --------------------

@dp.message(Command("fight"))
async def fight(message: Message):

    try:
        amount = int(message.text.split()[1])
    except:
        await message.answer("❗ Пример: /fight 10")
        return

    chat_id = message.chat.id
    challenger = message.from_user.id

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM fights WHERE chat_id=?", (chat_id,))
        await db.execute(
            "INSERT INTO fights (chat_id, challenger, amount) VALUES (?, ?, ?)",
            (chat_id, challenger, amount)
        )
        await db.commit()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚔️ Сражаться", callback_data="fight")]
        ]
    )

    await message.answer(
        f"⚔️ {mention(message.from_user)} ищет соперника!\n"
        f"Ставка: {amount} см",
        reply_markup=kb

    )

# -------------------- CALLBACK FIGHT --------------------

@dp.callback_query(F.data == "fight")
async def fight_callback(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT challenger, amount FROM fights WHERE chat_id=?",
            (chat_id,)
        )
        fight = await cursor.fetchone()

    if not fight:
        await callback.answer("Бой уже завершён")
        return

    challenger, amount = fight

    if user_id == challenger:
        await callback.answer("Нельзя драться с собой 😅")
        return

    # проверка размеров
    c_size, _ = await get_user(challenger, chat_id)
    u_size, _ = await get_user(user_id, chat_id)

    if c_size < amount or u_size < amount:
        await callback.answer("У кого-то не хватает см 😢")
        return

    winner = random.choice([challenger, user_id])
    loser = challenger if winner == user_id else user_id

    # обновление размеров
    w_size, _ = await get_user(winner, chat_id)
    l_size, _ = await get_user(loser, chat_id)

    w_size += amount
    l_size = max(0, l_size - amount)

    await update_size(winner, chat_id, w_size)
    await update_size(loser, chat_id, l_size)

    # получаем пользователей
    challenger_user = (await bot.get_chat_member(chat_id, challenger)).user
    opponent_user = callback.from_user

    winner_user = challenger_user if winner == challenger else opponent_user
    loser_user = opponent_user if winner == challenger else challenger_user

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM fights WHERE chat_id=?", (chat_id,))
        await db.commit()

    await callback.message.edit_text(
        f"⚔️ Бой состоялся!\n\n"
        f"🏆 Победитель: {mention(winner_user)}\n"
        f"💀 Проигравший: {mention(loser_user)}\n"
        f"Ставка: {amount} см"
    )

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET wins = COALESCE(wins,0)+1 WHERE user_id=? AND chat_id=?",
            (winner, chat_id)
        )
        await db.execute(
            "UPDATE users SET loses = COALESCE(loses,0)+1 WHERE user_id=? AND chat_id=?",
            (loser, chat_id)
        )
        await db.commit()

    await callback.answer("Бой завершён!")


# -------------------- ЗАПУСК --------------------

async def main():
    await init_db()
    await set_commands(bot)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())