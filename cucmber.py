import asyncio
import os
import random
import aiosqlite
import aiohttp
from datetime import datetime, timedelta
from aiogram.types import LabeledPrice, PreCheckoutQuery
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    CallbackQuery, BotCommand,
    BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats
)
from aiogram.filters import Command, StateFilter
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

import pytz

MSK = pytz.timezone("Europe/Moscow")

TOKEN = "8707444896:AAGUN2mvuXDOzr5zgAKy2ga_9US2Pl70vik"

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

DB_NAME = "cucumbers.db"
ADMIN_ID = 5971748042
BOT_USERNAME = ""


def now_msk():
    return datetime.now(MSK)


# -------------------- АКЦИИ --------------------

STOCKS = {
    "AAPL": "🍎 Apple",
    "TSLA": "⚡ Tesla",
    "NVDA": "🎮 NVIDIA",
    "AMZN": "📦 Amazon",
    "GOOGL": "🔍 Google",
}

VOLATILE_STOCKS = {
    "PEPE": "🐸 PepeToken",
    "DOGE": "🐕 DogeCoin",
    "SHIB": "💀 ShibaInu",
    "MOON": "🌙 MoonCoin",
    "PUMP": "🚀 PumpToken",
    "CUKE": "🥒 CukeCoin",
    "MEME": "😂 MemeCoin",
    "CHAD": "💪 ChadCoin",
    "REKT": "💸 RektCoin",
    "BONK": "🔨 BonkToken",
}

VOLATILE_INITIAL_PRICES = {
    "PEPE": 50.0, "DOGE": 120.0, "SHIB": 30.0, "MOON": 80.0, "PUMP": 200.0,
    "CUKE": 100.0, "MEME": 60.0, "CHAD": 150.0, "REKT": 40.0, "BONK": 75.0,
}

ALL_STOCKS = {**STOCKS, **VOLATILE_STOCKS}
volatile_prices: dict = {}


async def load_volatile_prices():
    global volatile_prices
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT ticker, price FROM volatile_stocks")
        rows = await cursor.fetchall()
    volatile_prices = {t: p for t, p in rows} if rows else dict(VOLATILE_INITIAL_PRICES)


async def save_volatile_prices():
    async with aiosqlite.connect(DB_NAME) as db:
        for ticker, price in volatile_prices.items():
            await db.execute(
                "INSERT OR REPLACE INTO volatile_stocks (ticker, price) VALUES (?, ?)",
                (ticker, price)
            )
        await db.commit()


async def update_volatile_prices():
    global volatile_prices
    while True:
        await asyncio.sleep(600)
        for ticker in VOLATILE_STOCKS:
            current = volatile_prices.get(ticker, VOLATILE_INITIAL_PRICES[ticker])
            change_pct = random.uniform(-0.35, 0.55)
            volatile_prices[ticker] = round(max(1.0, min(50000.0, current * (1 + change_pct))), 2)
        await save_volatile_prices()


async def get_stock_prices() -> dict:
    prices = {}
    headers = {"User-Agent": "Mozilla/5.0"}
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for ticker in STOCKS:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as r:
                    data = await r.json(content_type=None)
                    prices[ticker] = round(data["chart"]["result"][0]["meta"]["regularMarketPrice"], 2)
            except Exception:
                prices[ticker] = 0.0
    prices.update(volatile_prices)
    return prices


async def get_portfolio(user_id: int) -> dict:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT ticker, shares FROM portfolios WHERE user_id=? AND shares > 0", (user_id,)
        )
        rows = await cursor.fetchall()
    return {ticker: shares for ticker, shares in rows}


# -------------------- БАЗА ДАННЫХ --------------------

async def init_db():
    os.makedirs("data", exist_ok=True)
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cursor = await db.execute("PRAGMA table_info(users)")
            cols = [col[1] for col in await cursor.fetchall()]
            if cols and "chat_id" in cols:
                for tbl in ("users", "user_chats", "fights", "portfolios", "bank", "volatile_stocks"):
                    await db.execute(f"DROP TABLE IF EXISTS {tbl}")
                await db.commit()
        except Exception:
            pass

        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id   INTEGER PRIMARY KEY,
            size      INTEGER DEFAULT 0,
            last_grow TEXT,
            wins      INTEGER DEFAULT 0,
            loses     INTEGER DEFAULT 0,
            max_size  INTEGER DEFAULT 0,
            name      TEXT,
            loan      INTEGER DEFAULT 0,
            loan_date TEXT,
            last_box  TEXT,
            last_tax  TEXT
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_chats (
            user_id INTEGER,
            chat_id INTEGER,
            PRIMARY KEY (user_id, chat_id)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS fights (
            chat_id    INTEGER PRIMARY KEY,
            challenger INTEGER,
            amount     INTEGER
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS portfolios (
            user_id INTEGER,
            ticker  TEXT,
            shares  REAL DEFAULT 0,
            PRIMARY KEY (user_id, ticker)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS bank (
            id      INTEGER PRIMARY KEY,
            capital INTEGER DEFAULT 0
        )""")
        await db.execute("INSERT OR IGNORE INTO bank (id, capital) VALUES (1, 0)")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS volatile_stocks (
            ticker TEXT PRIMARY KEY,
            price  REAL NOT NULL
        )""")
        for ticker, price in VOLATILE_INITIAL_PRICES.items():
            await db.execute(
                "INSERT OR IGNORE INTO volatile_stocks (ticker, price) VALUES (?, ?)",
                (ticker, price)
            )
        await db.execute("""
        CREATE TABLE IF NOT EXISTS clans (
            clan_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT UNIQUE NOT NULL,
            owner_id     INTEGER NOT NULL,
            logo_file_id TEXT,
            created_at   TEXT
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS clan_members (
            user_id   INTEGER PRIMARY KEY,
            clan_id   INTEGER NOT NULL,
            role      TEXT DEFAULT 'Участник',
            joined_at TEXT
        )""")
        await db.commit()


# -------------------- УТИЛИТЫ --------------------

def mention(user):
    name = (user.full_name or "Игрок").replace("<", "").replace(">", "")
    return f"<a href='tg://user?id={user.id}'>{name}</a>"


async def save_user_chat(user_id: int, chat_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO user_chats (user_id, chat_id) VALUES (?, ?)",
            (user_id, chat_id)
        )
        await db.commit()


async def get_user(user_id: int, name: str = None):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, last_grow FROM users WHERE user_id=?", (user_id,)
        )
        row = await cursor.fetchone()
        if not row:
            await db.execute(
                "INSERT INTO users (user_id, size, name) VALUES (?, 0, ?)", (user_id, name)
            )
            await db.commit()
            return 0, None
        if name:
            await db.execute("UPDATE users SET name=? WHERE user_id=?", (name, user_id))
            await db.commit()
        return row


async def update_size(user_id: int, size: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT max_size FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
        max_size = row[0] if row and row[0] else 0
        if size > max_size:
            max_size = size
        await db.execute(
            "UPDATE users SET size=?, max_size=? WHERE user_id=?", (size, max_size, user_id)
        )
        await db.commit()


async def update_last_grow(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET last_grow=? WHERE user_id=?", (now_msk().isoformat(), user_id)
        )
        await db.commit()


# -------------------- НАЛОГ --------------------

async def apply_tax(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, last_tax FROM users WHERE user_id=?", (user_id,)
        )
        row = await cursor.fetchone()
    if not row or row[0] < 1000:
        return None
    size, last_tax = row
    now = datetime.now()
    if last_tax:
        last_time = datetime.fromisoformat(last_tax)
        if now - last_time < timedelta(days=1):
            return None
    k = size // 1000
    tax_amount = 30 * k
    size = max(0, size - tax_amount)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET size=?, last_tax=? WHERE user_id=?", (size, now.isoformat(), user_id)
        )
        await db.commit()
    await add_to_bank(tax_amount)
    return size


# -------------------- БАНК --------------------

async def add_to_bank(amount: int):
    if amount <= 0:
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE bank SET capital = capital + ? WHERE id = 1", (amount,))
        await db.commit()


async def get_bank_capital() -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT capital FROM bank WHERE id = 1")
        row = await cursor.fetchone()
        return row[0] if row else 0


async def apply_loan_interest(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT loan, loan_date FROM users WHERE user_id=?", (user_id,)
        )
        row = await cursor.fetchone()
    if not row or not row[0] or row[0] <= 0:
        return 0, 0
    loan, loan_date = row
    if not loan_date:
        return loan, 0
    now = now_msk()
    last_time = datetime.fromisoformat(loan_date)
    if last_time.tzinfo is None:
        last_time = MSK.localize(last_time)
    days_passed = int((now - last_time).total_seconds() // 86400)
    if days_passed <= 0:
        return loan, 0
    interest_total = 0
    new_loan = loan
    for _ in range(days_passed):
        interest = max(1, int(new_loan * 0.30))
        interest_total += interest
        new_loan += interest
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET loan=?, loan_date=? WHERE user_id=?",
            (new_loan, now.isoformat(), user_id)
        )
        await db.commit()
    await add_to_bank(interest_total)
    return new_loan, interest_total


# -------------------- КЛАНЫ (ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ) --------------------

async def get_clan_member(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT clan_id, role FROM clan_members WHERE user_id=?", (user_id,)
        )
        return await cursor.fetchone()


async def get_clan(clan_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT clan_id, name, owner_id, logo_file_id FROM clans WHERE clan_id=?", (clan_id,)
        )
        return await cursor.fetchone()


async def get_clan_members_list(clan_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """SELECT cm.user_id, cm.role, u.name
               FROM clan_members cm
               JOIN users u ON cm.user_id = u.user_id
               WHERE cm.clan_id=?
               ORDER BY cm.user_id""",
            (clan_id,)
        )
        return await cursor.fetchall()


async def _show_clan_menu(message: Message, user_id: int):
    member = await get_clan_member(user_id)
    if not member:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚔️ Создать клан (5000 см)", callback_data="clan_create")],
        ])
        await message.answer(
            "🛡 <b>Кланы</b>\n\nТы не состоишь ни в каком клане.\n"
            "Создание клана стоит <b>5000 см</b>.",
            reply_markup=kb
        )
        return
    clan_id, role = member
    clan = await get_clan(clan_id)
    if not clan:
        await message.answer("❌ Ошибка: клан не найден.")
        return
    _, name, owner_id, logo_file_id = clan
    members = await get_clan_members_list(clan_id)
    text = (
        f"🛡 <b>{name}</b>\n"
        f"👥 Участников: {len(members)}\n"
        f"🎖 Твоя роль: <b>{role}</b>"
    )
    buttons = [
        [InlineKeyboardButton(text="👥 Участники", callback_data=f"clan_members_{clan_id}")],
    ]
    if user_id == owner_id:
        buttons.append([InlineKeyboardButton(text="🔗 Ссылка-приглашение", callback_data=f"clan_invite_{clan_id}")])
        buttons.append([InlineKeyboardButton(text="🎖 Назначить роль", callback_data=f"clan_setrole_{clan_id}")])
        buttons.append([InlineKeyboardButton(text="🖼 Изменить логотип", callback_data=f"clan_setlogo_{clan_id}")])
        buttons.append([InlineKeyboardButton(text="🗑 Расформировать клан", callback_data=f"clan_disband_{clan_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="🚪 Покинуть клан", callback_data=f"clan_leave_{clan_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    if logo_file_id:
        await message.answer_photo(logo_file_id, caption=text, reply_markup=kb)
    else:
        await message.answer(text, reply_markup=kb)


async def _finish_clan_creation(msg: Message, state: FSMContext, user_id: int, name: str, logo_file_id):
    size, _ = await get_user(user_id)
    if size < 5000:
        await msg.answer("❌ Недостаточно см для создания клана!")
        await state.clear()
        return
    await update_size(user_id, size - 5000)
    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "INSERT INTO clans (name, owner_id, logo_file_id, created_at) VALUES (?, ?, ?, ?)",
            (name, user_id, logo_file_id, now.isoformat())
        )
        clan_id = cursor.lastrowid
        await db.execute(
            "INSERT INTO clan_members (user_id, clan_id, role, joined_at) VALUES (?, ?, '👑 Лидер', ?)",
            (user_id, clan_id, now.isoformat())
        )
        await db.commit()
    await state.clear()
    await msg.answer(
        f"✅ Клан <b>{name}</b> создан!\n"
        f"💸 Списано: 5000 см\n"
        f"🎖 Твоя роль: 👑 Лидер\n\n"
        f"Нажми 🛡 Клан для управления."
    )


async def handle_join_clan(message: Message, clan_id: int):
    user_id = message.from_user.id
    clan = await get_clan(clan_id)
    if not clan:
        await message.answer("❌ Клан не найден или был расформирован.")
        return
    existing = await get_clan_member(user_id)
    if existing:
        await message.answer("❌ Ты уже состоишь в клане! Сначала покинь текущий.")
        return
    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO clan_members (user_id, clan_id, role, joined_at) VALUES (?, ?, 'Участник', ?)",
            (user_id, clan_id, now.isoformat())
        )
        await db.commit()
    await get_user(user_id, message.from_user.full_name)
    is_group = message.chat.type != "private"
    await message.answer(
        f"✅ Ты вступил в клан <b>{clan[1]}</b>!\n🎖 Роль: Участник",
        reply_markup=main_menu_reply_kb(is_group)
    )


# -------------------- МЕНЮ --------------------

BUTTON_TEXTS = frozenset({
    "🌱 Вырастить", "📊 Статистика", "🎁 Лутбокс", "🎰 Слоты",
    "📈 Рынок акций", "💼 Forbes", "🏦 Банк", "🛒 Магазин",
    "🏆 Топ чата", "⚔️ Бой", "🛡 Клан", "📩 Поддержка",
})


def main_menu_reply_kb(is_group: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🌱 Вырастить"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🎁 Лутбокс"), KeyboardButton(text="🎰 Слоты")],
        [KeyboardButton(text="📈 Рынок акций"), KeyboardButton(text="💼 Forbes")],
        [KeyboardButton(text="🏦 Банк"), KeyboardButton(text="🛒 Магазин")],
    ]
    if is_group:
        rows.append([KeyboardButton(text="🏆 Топ чата"), KeyboardButton(text="⚔️ Бой")])
        rows.append([KeyboardButton(text="🛡 Клан")])
    else:
        rows.append([KeyboardButton(text="🛡 Клан"), KeyboardButton(text="📩 Поддержка")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def menu_kb():
    return None


# -------------------- СОСТОЯНИЯ --------------------

class BroadcastState(StatesGroup):
    waiting_message = State()


class MarketState(StatesGroup):
    waiting_buy = State()
    waiting_sell = State()


class AdminCmState(StatesGroup):
    add_cm = State()
    sub_cm = State()


class LoanState(StatesGroup):
    waiting_amount = State()


class TaskState(StatesGroup):
    question = State()
    answer = State()
    reward = State()


class SlotsState(StatesGroup):
    waiting_amount = State()


class FightState(StatesGroup):
    waiting_amount = State()


class ClanCreateState(StatesGroup):
    waiting_name = State()
    waiting_logo = State()


class ClanRoleState(StatesGroup):
    waiting_role = State()


class SupportState(StatesGroup):
    waiting_message = State()


# -------------------- АДМИН-ПАНЕЛЬ --------------------

@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Управление игроками", callback_data="admin_users")],
        [InlineKeyboardButton(text="🧠 Создать задачу", callback_data="admin_task")],
    ])
    await message.answer("⚙️ Админ-панель", reply_markup=kb)


@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Управление игроками", callback_data="admin_users")],
        [InlineKeyboardButton(text="🧠 Создать задачу", callback_data="admin_task")],
    ])
    await callback.message.edit_text("⚙️ Админ-панель", reply_markup=kb)
    await callback.answer()


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
        except Exception:
            failed += 1
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("DELETE FROM user_chats WHERE chat_id=?", (chat_id,))
                await db.commit()
    await message.answer(f"📢 Рассылка завершена\n\n✅ Успешно: {success}\n❌ Ошибки: {failed}")
    await state.clear()


@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT user_id, name, size FROM users ORDER BY size DESC LIMIT 30"
        )
        users = await cursor.fetchall()
    if not users:
        await callback.message.edit_text("❌ Нет игроков", reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]]
        ))
        await callback.answer()
        return
    rows = []
    for user_id, name, size in users:
        label = (name or "Игрок")[:22].replace("<", "").replace(">", "")
        rows.append([InlineKeyboardButton(
            text=f"👤 {label} — {size} см",
            callback_data=f"admin_usr_{user_id}"
        )])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    await callback.message.edit_text(
        "👥 Выбери игрока (топ-30 по размеру):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_usr_"))
async def admin_user_actions(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data[10:])
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT size, name FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Игрок не найден", show_alert=True)
        return
    size, name = row
    label = (name or "Игрок").replace("<", "").replace(">", "")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить см", callback_data=f"admin_add_{user_id}"),
            InlineKeyboardButton(text="➖ Убрать см", callback_data=f"admin_sub_{user_id}"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users")]
    ])
    await callback.message.edit_text(
        f"👤 <b>{label}</b>\n🥒 Огурец: <b>{size} см</b>\n\nЧто сделать?",
        reply_markup=kb
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_add_"))
async def admin_add_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data[10:])
    await state.set_state(AdminCmState.add_cm)
    await state.update_data(target_user=user_id)
    await callback.message.answer("➕ Сколько см добавить?")
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_sub_"))
async def admin_sub_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data[10:])
    await state.set_state(AdminCmState.sub_cm)
    await state.update_data(target_user=user_id)
    await callback.message.answer("➖ Сколько см убрать?")
    await callback.answer()


@dp.message(AdminCmState.add_cm)
async def admin_add_cm(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи целое число больше 0")
        return
    data = await state.get_data()
    user_id = data["target_user"]
    size, _ = await get_user(user_id)
    await update_size(user_id, size + amount)
    await message.answer(f"✅ Добавлено {amount} см. Теперь: {size + amount} см")
    await state.clear()


@dp.message(AdminCmState.sub_cm)
async def admin_sub_cm(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи целое число больше 0")
        return
    data = await state.get_data()
    user_id = data["target_user"]
    size, _ = await get_user(user_id)
    new_size = max(0, size - amount)
    await update_size(user_id, new_size)
    await message.answer(f"✅ Убрано {amount} см. Теперь: {new_size} см")
    await state.clear()


# -------------------- ЗАДАЧИ --------------------

ACTIVE_TASK: dict = {}


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
async def task_answer_handler(message: Message, state: FSMContext):
    await state.update_data(answer=message.text.lower())
    await message.answer("💰 Введи награду (см):")
    await state.set_state(TaskState.reward)


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
                f"🧠 ЗАДАЧА!\n\n{data['question']}\n\n💰 Награда: {reward} см\n"
                f"✍️ Напиши ответ в чат\n"
                f"‼️ Если отправляешь правильный ответ, но не получаешь награду — задача уже решена в другой группе."
            )
            ACTIVE_TASK[chat_id] = {"answer": data["answer"], "reward": reward, "active": True}
        except Exception:
            pass
    await message.answer("✅ Задача отправлена!")
    await state.clear()


@dp.message(F.text & ~F.text.startswith("/") & ~F.text.in_(BUTTON_TEXTS), StateFilter(None))
async def check_answer(message: Message):
    chat_id = message.chat.id
    task = ACTIVE_TASK.get(chat_id)
    if not task or not task.get("active"):
        return
    if message.text.lower().strip() == task["answer"].lower().strip():
        user_id = message.from_user.id
        size, _ = await get_user(user_id, message.from_user.full_name)
        size += task["reward"]
        await update_size(user_id, size)
        task["active"] = False
        await message.answer(
            f"🎉 {mention(message.from_user)} решил задачу!\n"
            f"+{task['reward']} см\n📏 Теперь: {size} см"
        )


# -------------------- ЛОГИКА ДЕЙСТВИЙ --------------------

async def _do_grow(user, chat_id: int, is_private: bool) -> tuple[str, InlineKeyboardMarkup]:
    user_id = user.id
    if not is_private:
        await save_user_chat(user_id, chat_id)
    size, last_grow = await get_user(user_id, user.full_name)
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
            return (
                f"⏳ {mention(user)}\nТы уже выращивал сегодня!\n\n"
                f"🕐 Снова можно в: {next_time.strftime('%H:%M:%S')} (МСК)\n"
                f"⏱ Осталось: {hours}ч {minutes}м {seconds}с"
            ), menu_kb()
    growth = random.randint(1, 50)
    size += growth
    await update_size(user_id, size)
    await update_last_grow(user_id)
    tax = int(growth * 20 / 100)
    new_size = size - tax
    await add_to_bank(tax)
    wealth_tax_result = await apply_tax(user_id)
    wealth_tax_note = ""
    if wealth_tax_result is not None:
        k = new_size // 1000
        wt = 30 * k
        wealth_tax_note = f"\n🏦 Налог на богатство: -{wt} см"
        new_size = wealth_tax_result
    await update_size(user_id, new_size)
    return (
        f"🌱 {mention(user)}\n"
        f"+{growth} см\nТеперь: {size} см\n"
        f"💸 Налог 20% от дохода: -{tax} см"
        f"{wealth_tax_note}\n"
        f"🥒 Итого: {new_size} см"
    ), menu_kb()


async def _do_stats(user, chat_id: int, is_private: bool) -> tuple[str, InlineKeyboardMarkup]:
    user_id = user.id
    if not is_private:
        await save_user_chat(user_id, chat_id)
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, wins, loses, max_size FROM users WHERE user_id=?", (user_id,)
        )
        row = await cursor.fetchone()
    if not row:
        return "❌ Нет данных. Используй /grow чтобы начать!", menu_kb()
    size, wins, loses, max_size = row
    wins = wins or 0
    loses = loses or 0
    max_size = max_size or size
    total_battles = wins + loses
    winrate = int((wins / total_battles) * 100) if total_battles > 0 else 0
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
    prices = await get_stock_prices()
    portfolio = await get_portfolio(user_id)
    portfolio_lines = ""
    portfolio_total = 0
    for ticker, shares in portfolio.items():
        price = prices.get(ticker, 0)
        value = int(shares * price)
        portfolio_total += value
        portfolio_lines += f"  {ALL_STOCKS.get(ticker, ticker)}: {shares:g} шт × {price:.0f} = <b>{value} см</b>\n"
    portfolio_block = ""
    if portfolio_lines:
        portfolio_block = (
            f"\n\n📈 <b>Портфель акций:</b>\n{portfolio_lines}"
            f"  💼 Стоимость: <b>{portfolio_total} см</b>"
        )
    clan_block = ""
    clan_row = await get_clan_member(user_id)
    if clan_row:
        clan_data = await get_clan(clan_row[0])
        if clan_data:
            clan_block = f"\n🛡 Клан: <b>{clan_data[1]}</b> | 🎖 {clan_row[1]}"

    text = (
        f"📊 {mention(user)}\n"
        f"📏 Огурец: {size} см\n📈 Макс: {max_size} см\n"
        f"🏆 Победы: {wins} / Поражения: {loses}\n"
        f"💯 Winrate: {winrate}%\n🎭 Роль: {role}"
        f"{clan_block}"
        f"{portfolio_block}"
    )
    return text, menu_kb()


async def _do_box(user, chat_id: int, is_private: bool) -> tuple[str, InlineKeyboardMarkup]:
    user_id = user.id
    if not is_private:
        await save_user_chat(user_id, chat_id)
    await get_user(user_id, user.full_name)
    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT last_box FROM users WHERE user_id=?", (user_id,))
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
            return (
                f"⏳ {mention(user)}\nТы уже открывал лутбокс!\n\n"
                f"🕐 Снова можно в: {next_time.strftime('%H:%M:%S')} (МСК)\n"
                f"⏱ Осталось: {hours}ч {minutes}м {seconds}с"
            ), menu_kb()
    roll = random.randint(1, 100)
    if roll <= 40:
        reward, rarity = random.randint(1, 3), "💩 Мусор"
    elif roll <= 70:
        reward, rarity = random.randint(4, 8), "🟢 Обычный"
    elif roll <= 90:
        reward, rarity = random.randint(9, 15), "🔵 Редкий"
    elif roll <= 99:
        reward, rarity = random.randint(16, 30), "🟣 Эпик"
    else:
        reward, rarity = random.randint(50, 100), "🟡 Легендарный"
    size, _ = await get_user(user_id)
    size += reward
    await update_size(user_id, size)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET last_box=? WHERE user_id=?", (now.isoformat(), user_id))
        await db.commit()
    return (
        f"🎁 {mention(user)} открыл лутбокс!\n\n"
        f"✨ Редкость: {rarity}\n💰 Награда: +{reward} см\n\n📏 Теперь: {size} см"
    ), menu_kb()


async def _do_top(chat_id: int) -> tuple[str, InlineKeyboardMarkup]:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """SELECT u.user_id, u.size, u.name
               FROM users u
               INNER JOIN user_chats uc ON u.user_id = uc.user_id
               WHERE uc.chat_id = ?
               ORDER BY u.size DESC LIMIT 10""",
            (chat_id,)
        )
        rows = await cursor.fetchall()
    if not rows:
        return "😢 Нет игроков", menu_kb()
    medals = ["🥇", "🥈", "🥉"]
    text = "🏆 Топ огурцов:\n\n"
    for i, (user_id, size, name) in enumerate(rows, 1):
        display_name = (name or "Игрок").replace("<", "").replace(">", "")
        prefix = medals[i - 1] if i <= 3 else f"{i}."
        text += f"{prefix} <a href='tg://user?id={user_id}'>{display_name}</a> — {size} см\n"
    return text, menu_kb()


async def _do_forbes() -> tuple[str, InlineKeyboardMarkup]:
    prices = await get_stock_prices()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT user_id, size, name FROM users")
        rows = await cursor.fetchall()
    rich_list = []
    for user_id, size, name in rows:
        portfolio = await get_portfolio(user_id)
        port_val = int(sum(shares * prices.get(t, 0) for t, shares in portfolio.items()))
        cucumber = size or 0
        rich_list.append((user_id, cucumber + port_val, cucumber, port_val, name))
    rich_list.sort(key=lambda x: x[1], reverse=True)
    rich_list = rich_list[:5]
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    lines = ""
    for i, (uid, total, cucumber, port_val, name) in enumerate(rich_list):
        display = (name or "Игрок").replace("<", "").replace(">", "")
        lines += (
            f"{medals[i]} <a href='tg://user?id={uid}'>{display}</a>\n"
            f"   🥒 {cucumber} см + 📈 {port_val} см = <b>{total} см</b>\n\n"
        )
    return f"💼 <b>Forbes — Топ 5 богатейших игроков</b>\n\n{lines}", menu_kb()


async def _do_slots(user, chat_id: int, is_private: bool, amount: int, answer_fn):
    user_id = user.id
    if not is_private:
        await save_user_chat(user_id, chat_id)
    size, _ = await get_user(user_id, user.full_name)
    if size < amount:
        await answer_fn(
            f"❌ Недостаточно см!\nУ тебя: {size} см, ставка: {amount} см",
            reply_markup=menu_kb()
        )
        return
    await update_size(user_id, size - amount)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎰 Крутить!", callback_data=f"slot_{user_id}_{amount}")]
    ])
    await answer_fn(
        f"🎰 {mention(user)} ставит {amount} см на автомат!\nНажми кнопку, чтобы крутить!",
        reply_markup=kb
    )


# -------------------- /start --------------------

@dp.message(Command("start"))
async def start(message: Message):
    parts = message.text.split(maxsplit=1)
    payload = parts[1] if len(parts) > 1 else ""
    if payload.startswith("joinclan_"):
        try:
            clan_id = int(payload[9:])
            await handle_join_clan(message, clan_id)
            return
        except ValueError:
            pass
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    is_group = message.chat.type != "private"
    await message.answer(
        "🥒 <b>Огуречный бот</b>\nВыбери действие:",
        reply_markup=main_menu_reply_kb(is_group)
    )


@dp.callback_query(F.data == "cmd_menu")
async def cmd_menu_cb(callback: CallbackQuery):
    is_group = callback.message.chat.type != "private"
    await callback.message.answer(
        "🥒 <b>Огуречный бот</b>\nВыбери действие:",
        reply_markup=main_menu_reply_kb(is_group)
    )
    await callback.answer()


# -------------------- ОБРАБОТЧИКИ КНОПОК МЕНЮ --------------------

@dp.message(F.text == "🌱 Вырастить", StateFilter(None))
async def btn_grow(message: Message):
    text, kb = await _do_grow(message.from_user, message.chat.id, message.chat.type == "private")
    await message.answer(text, reply_markup=kb)


@dp.message(F.text == "📊 Статистика", StateFilter(None))
async def btn_stats(message: Message):
    text, kb = await _do_stats(message.from_user, message.chat.id, message.chat.type == "private")
    await message.answer(text, reply_markup=kb)


@dp.message(F.text == "🎁 Лутбокс", StateFilter(None))
async def btn_box(message: Message):
    text, kb = await _do_box(message.from_user, message.chat.id, message.chat.type == "private")
    await message.answer(text, reply_markup=kb)


@dp.message(F.text == "🎰 Слоты", StateFilter(None))
async def btn_slots(message: Message, state: FSMContext):
    await state.set_state(SlotsState.waiting_amount)
    await message.answer("🎰 Введи ставку (в см):")


@dp.message(F.text == "📈 Рынок акций", StateFilter(None))
async def btn_market(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await message.answer("⏳ Получаем котировки...")
    await _send_market(message.answer)


@dp.message(F.text == "💼 Forbes", StateFilter(None))
async def btn_forbes(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await message.answer("⏳ Считаем состояния...")
    text, kb = await _do_forbes()
    await message.answer(text, reply_markup=kb)


@dp.message(F.text == "🏦 Банк", StateFilter(None))
async def btn_bank(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await _send_bank(message.from_user.id, message.answer)


@dp.message(F.text == "🛒 Магазин", StateFilter(None))
async def btn_shop(message: Message):
    await message.answer(f"🛒 {mention(message.from_user)}\nВыбери покупку:", reply_markup=shop_kb())


@dp.message(F.text == "🏆 Топ чата", StateFilter(None))
async def btn_top(message: Message):
    if message.chat.type == "private":
        await message.answer("⚠️ Кнопка доступна только в группах!")
        return
    await save_user_chat(message.from_user.id, message.chat.id)
    text, kb = await _do_top(message.chat.id)
    await message.answer(text, reply_markup=kb)


@dp.message(F.text == "⚔️ Бой", StateFilter(None))
async def btn_fight(message: Message, state: FSMContext):
    if message.chat.type == "private":
        await message.answer("⚠️ Кнопка доступна только в группах!")
        return
    await state.set_state(FightState.waiting_amount)
    await state.update_data(chat_id=message.chat.id)
    await message.answer("⚔️ Введи ставку (в см):")


@dp.message(F.text == "🛡 Клан", StateFilter(None))
async def btn_clan(message: Message):
    await _show_clan_menu(message, message.from_user.id)


@dp.message(F.text == "📩 Поддержка", StateFilter(None))
async def btn_support(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("⚠️ Поддержка доступна только в личных сообщениях!")
        return
    await state.set_state(SupportState.waiting_message)
    await message.answer(
        "📩 <b>Связь с поддержкой</b>\n\n"
        "Опиши свою проблему или вопрос — текст, скриншот или видео.\n"
        "Сообщение будет передано администратору.\n\n"
        "/cancel — отменить"
    )


@dp.message(SupportState.waiting_message)
async def support_msg_handler(message: Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    header = (
        f"📩 <b>Обращение в поддержку</b>\n"
        f"👤 {mention(user)} (ID: <code>{user.id}</code>)"
    )
    try:
        await bot.send_message(ADMIN_ID, header)
        await message.forward(ADMIN_ID)
    except Exception:
        pass
    await message.answer("✅ Сообщение отправлено! Мы свяжемся с тобой в ближайшее время.")


@dp.message(Command("cancel"))
async def cancel_cmd(message: Message, state: FSMContext):
    if await state.get_state() is None:
        return
    await state.clear()
    await message.answer("✅ Действие отменено.")


# -------------------- КЛАНЫ (ОБРАБОТЧИКИ) --------------------

@dp.callback_query(F.data == "clan_create")
async def clan_create_cb(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    existing = await get_clan_member(user_id)
    if existing:
        await callback.answer("❌ Ты уже в клане!", show_alert=True)
        return
    size, _ = await get_user(user_id)
    if size < 5000:
        await callback.answer(f"❌ Нужно 5000 см, у тебя {size}", show_alert=True)
        return
    await state.update_data(user_id=user_id)
    await state.set_state(ClanCreateState.waiting_name)
    await callback.message.answer(
        "🛡 <b>Создание клана</b>\n\nПридумай название (2–30 символов):\n\n/cancel — отменить"
    )
    await callback.answer()


@dp.message(ClanCreateState.waiting_name)
async def clan_name_input(message: Message, state: FSMContext):
    name = message.text.strip() if message.text else ""
    if len(name) < 2 or len(name) > 30:
        await message.answer("❗ Название должно быть от 2 до 30 символов")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT clan_id FROM clans WHERE name=?", (name,))
        if await cursor.fetchone():
            await message.answer("❌ Клан с таким названием уже существует. Придумай другое:")
            return
    await state.update_data(clan_name=name)
    await state.set_state(ClanCreateState.waiting_logo)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="clan_skip_logo")]
    ])
    await message.answer(
        "🖼 Отправь логотип клана (фото) или нажми <b>Пропустить</b>:",
        reply_markup=kb
    )


@dp.message(ClanCreateState.waiting_logo, F.photo)
async def clan_logo_photo_input(message: Message, state: FSMContext):
    logo_file_id = message.photo[-1].file_id
    data = await state.get_data()
    if "clan_id_update" in data:
        clan_id = data["clan_id_update"]
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE clans SET logo_file_id=? WHERE clan_id=?", (logo_file_id, clan_id)
            )
            await db.commit()
        await state.clear()
        await message.answer("✅ Логотип клана обновлён!")
    else:
        await _finish_clan_creation(message, state, data["user_id"], data["clan_name"], logo_file_id)


@dp.message(ClanCreateState.waiting_logo, ~F.photo)
async def clan_logo_wrong(message: Message):
    if message.text and message.text.startswith("/"):
        return
    await message.answer("❗ Отправь фото или нажми <b>Пропустить</b>")


@dp.callback_query(F.data == "clan_skip_logo")
async def clan_skip_logo_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "clan_id_update" in data:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE clans SET logo_file_id=NULL WHERE clan_id=?", (data["clan_id_update"],)
            )
            await db.commit()
        await state.clear()
        await callback.message.answer("✅ Логотип удалён.")
    else:
        await _finish_clan_creation(callback.message, state, data["user_id"], data["clan_name"], None)
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_invite_"))
async def clan_invite_cb(callback: CallbackQuery):
    clan_id = int(callback.data[12:])
    user_id = callback.from_user.id
    clan = await get_clan(clan_id)
    if not clan or clan[2] != user_id:
        await callback.answer("❌ Нет прав!", show_alert=True)
        return
    link = f"https://t.me/{BOT_USERNAME}?start=joinclan_{clan_id}"
    await callback.message.answer(
        f"🔗 <b>Ссылка-приглашение в клан «{clan[1]}»:</b>\n\n"
        f"<code>{link}</code>\n\n"
        f"Поделись ссылкой — по ней можно вступить в клан через ЛС бота."
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_members_"))
async def clan_members_cb(callback: CallbackQuery):
    clan_id = int(callback.data[13:])
    clan = await get_clan(clan_id)
    members = await get_clan_members_list(clan_id)
    if not clan or not members:
        await callback.answer("Клан не найден", show_alert=True)
        return
    lines = f"👥 <b>Участники клана «{clan[1]}»:</b>\n\n"
    for uid, role, name in members:
        display = (name or "Игрок").replace("<", "").replace(">", "")
        crown = "👑 " if uid == clan[2] else ""
        lines += f"{crown}<a href='tg://user?id={uid}'>{display}</a> — {role}\n"
    await callback.message.answer(lines)
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_setrole_"))
async def clan_setrole_cb(callback: CallbackQuery):
    clan_id = int(callback.data[13:])
    user_id = callback.from_user.id
    clan = await get_clan(clan_id)
    if not clan or clan[2] != user_id:
        await callback.answer("❌ Нет прав!", show_alert=True)
        return
    members = await get_clan_members_list(clan_id)
    rows = []
    for m_uid, m_role, m_name in members:
        if m_uid == user_id:
            continue
        label = (m_name or "Игрок")[:25].replace("<", "").replace(">", "")
        rows.append([InlineKeyboardButton(
            text=f"👤 {label} — {m_role}",
            callback_data=f"clan_pick_member_{m_uid}"
        )])
    if not rows:
        await callback.answer("В клане нет других участников", show_alert=True)
        return
    await callback.message.answer(
        "🎖 Выбери участника для назначения роли:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_pick_member_"))
async def clan_pick_member_cb(callback: CallbackQuery, state: FSMContext):
    target_uid = int(callback.data[17:])
    await state.set_state(ClanRoleState.waiting_role)
    await state.update_data(target_user_id=target_uid)
    await callback.message.answer(
        "✏️ Введи новую роль (до 20 символов):\n\n/cancel — отменить"
    )
    await callback.answer()


@dp.message(ClanRoleState.waiting_role)
async def clan_role_input(message: Message, state: FSMContext):
    role = (message.text or "").strip()
    if not role or len(role) > 20:
        await message.answer("❗ Роль должна быть от 1 до 20 символов")
        return
    data = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE clan_members SET role=? WHERE user_id=?", (role, data["target_user_id"])
        )
        await db.commit()
    await state.clear()
    await message.answer(f"✅ Роль назначена: <b>{role}</b>")


@dp.callback_query(F.data.startswith("clan_setlogo_"))
async def clan_setlogo_cb(callback: CallbackQuery, state: FSMContext):
    clan_id = int(callback.data[13:])
    user_id = callback.from_user.id
    clan = await get_clan(clan_id)
    if not clan or clan[2] != user_id:
        await callback.answer("❌ Нет прав!", show_alert=True)
        return
    await state.update_data(user_id=user_id, clan_name=clan[1], clan_id_update=clan_id)
    await state.set_state(ClanCreateState.waiting_logo)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить логотип", callback_data="clan_skip_logo")]
    ])
    await callback.message.answer(
        "🖼 Отправь новый логотип (фото) или нажми <b>Удалить логотип</b>:",
        reply_markup=kb
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_disband_confirm_"))
async def clan_disband_confirm_cb(callback: CallbackQuery):
    clan_id = int(callback.data[21:])
    user_id = callback.from_user.id
    clan = await get_clan(clan_id)
    if not clan or clan[2] != user_id:
        await callback.answer("❌ Нет прав!", show_alert=True)
        return
    name = clan[1]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM clan_members WHERE clan_id=?", (clan_id,))
        await db.execute("DELETE FROM clans WHERE clan_id=?", (clan_id,))
        await db.commit()
    await callback.message.answer(f"💔 Клан <b>{name}</b> расформирован.")
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_disband_"))
async def clan_disband_cb(callback: CallbackQuery):
    if callback.data.startswith("clan_disband_confirm_"):
        return
    clan_id = int(callback.data[13:])
    user_id = callback.from_user.id
    clan = await get_clan(clan_id)
    if not clan or clan[2] != user_id:
        await callback.answer("❌ Нет прав!", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, расформировать", callback_data=f"clan_disband_confirm_{clan_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="clan_disband_cancel"),
        ]
    ])
    await callback.message.answer(
        f"⚠️ Расформировать клан <b>{clan[1]}</b>?\nВсе участники будут исключены.",
        reply_markup=kb
    )
    await callback.answer()


@dp.callback_query(F.data == "clan_disband_cancel")
async def clan_disband_cancel_cb(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("Отменено")


@dp.callback_query(F.data.startswith("clan_leave_"))
async def clan_leave_cb(callback: CallbackQuery):
    clan_id = int(callback.data[11:])
    user_id = callback.from_user.id
    clan = await get_clan(clan_id)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "DELETE FROM clan_members WHERE user_id=? AND clan_id=?", (user_id, clan_id)
        )
        await db.commit()
    name = clan[1] if clan else "клан"
    await callback.message.answer(f"🚪 Ты покинул клан <b>{name}</b>.")
    await callback.answer()


# -------------------- GROW --------------------

@dp.message(Command("grow"))
async def grow(message: Message):
    text, kb = await _do_grow(message.from_user, message.chat.id, message.chat.type == "private")
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "cmd_grow")
async def cmd_grow_cb(callback: CallbackQuery):
    text, kb = await _do_grow(
        callback.from_user, callback.message.chat.id, callback.message.chat.type == "private"
    )
    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


# -------------------- STATS --------------------

@dp.message(Command("stats"))
async def stats(message: Message):
    text, kb = await _do_stats(message.from_user, message.chat.id, message.chat.type == "private")
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "cmd_stats")
async def cmd_stats_cb(callback: CallbackQuery):
    text, kb = await _do_stats(
        callback.from_user, callback.message.chat.id, callback.message.chat.type == "private"
    )
    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


# -------------------- BOX --------------------

@dp.message(Command("box"))
async def open_box(message: Message):
    text, kb = await _do_box(message.from_user, message.chat.id, message.chat.type == "private")
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "cmd_box")
async def cmd_box_cb(callback: CallbackQuery):
    text, kb = await _do_box(
        callback.from_user, callback.message.chat.id, callback.message.chat.type == "private"
    )
    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


# -------------------- SHOP --------------------

def shop_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 10 см — ⭐10", callback_data="buy_10")],
        [InlineKeyboardButton(text="💰 50 см — ⭐45", callback_data="buy_50")],
        [InlineKeyboardButton(text="💰 100 см — ⭐80", callback_data="buy_100")],
    ])


@dp.message(Command("shop"))
async def shop(message: Message):
    await message.answer(f"🛒 {mention(message.from_user)}\nВыбери покупку:", reply_markup=shop_kb())


@dp.callback_query(F.data == "cmd_shop")
async def cmd_shop_cb(callback: CallbackQuery):
    await callback.message.answer(
        f"🛒 {mention(callback.from_user)}\nВыбери покупку:", reply_markup=shop_kb()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_"))
async def buy_handler(call: CallbackQuery):
    amounts = {"buy_10": (10, 10), "buy_50": (50, 45), "buy_100": (100, 80)}
    if call.data not in amounts:
        await call.answer("Ошибка")
        return
    amount, price = amounts[call.data]
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="Покупка огурца 🥒",
        description=f"Ты покупаешь {amount} см",
        payload=f"cucumber_{amount}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=f"{amount} см", amount=price)],
        start_parameter="cucumber-shop"
    )
    await call.answer("Проверь личные сообщения. Если ничего нет — нажми START в ЛС бота.")


@dp.pre_checkout_query()
async def pre_checkout(pre_checkout_q: PreCheckoutQuery):
    await pre_checkout_q.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: Message):
    amount = int(message.successful_payment.invoice_payload.split("_")[1])
    user_id = message.from_user.id
    size, _ = await get_user(user_id, message.from_user.full_name)
    size += amount
    await update_size(user_id, size)
    await message.answer(
        f"💰 Покупка успешна!\n+{amount} см\n🥒 Огурец: {size} см",
        reply_markup=menu_kb()
    )
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT DISTINCT chat_id FROM user_chats WHERE user_id=?", (user_id,)
        )
        chats = await cursor.fetchall()
    for (chat_id,) in chats:
        try:
            await bot.send_message(
                chat_id, f"💰 {mention(message.from_user)} купил {amount} см!\nТеперь: {size} см"
            )
        except Exception:
            pass


# -------------------- TOP (только в группах) --------------------

@dp.message(Command("top"))
async def top(message: Message):
    if message.chat.type == "private":
        await message.answer("⚠️ Команда /top доступна только в группах!", reply_markup=menu_kb())
        return
    await save_user_chat(message.from_user.id, message.chat.id)
    text, kb = await _do_top(message.chat.id)
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "cmd_top")
async def cmd_top_cb(callback: CallbackQuery):
    if callback.message.chat.type == "private":
        await callback.answer("⚠️ Только в группах!", show_alert=True)
        return
    await save_user_chat(callback.from_user.id, callback.message.chat.id)
    text, kb = await _do_top(callback.message.chat.id)
    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


# -------------------- FIGHT (только в группах) --------------------

@dp.message(Command("fight"))
async def fight(message: Message):
    if message.chat.type == "private":
        await message.answer("⚠️ Команда /fight доступна только в группах!", reply_markup=menu_kb())
        return
    try:
        amount = int(message.text.split()[1])
        if amount <= 0:
            raise ValueError
    except (IndexError, ValueError):
        await message.answer("❗ Пример: /fight 10")
        return
    await _create_fight(message.from_user, message.chat.id, amount, message.answer)


@dp.callback_query(F.data == "cmd_fight_menu")
async def cmd_fight_menu_cb(callback: CallbackQuery, state: FSMContext):
    if callback.message.chat.type == "private":
        await callback.answer("⚠️ Только в группах!", show_alert=True)
        return
    await state.set_state(FightState.waiting_amount)
    await state.update_data(chat_id=callback.message.chat.id)
    await callback.message.answer("⚔️ Введи ставку (в см):")
    await callback.answer()


@dp.message(FightState.waiting_amount)
async def fight_amount_input(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи целое число больше 0")
        return
    data = await state.get_data()
    chat_id = data.get("chat_id", message.chat.id)
    await state.clear()
    await _create_fight(message.from_user, chat_id, amount, message.answer)


async def _create_fight(user, chat_id: int, amount: int, answer_fn):
    await save_user_chat(user.id, chat_id)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM fights WHERE chat_id=?", (chat_id,))
        await db.execute(
            "INSERT INTO fights (chat_id, challenger, amount) VALUES (?, ?, ?)",
            (chat_id, user.id, amount)
        )
        await db.commit()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚔️ Сражаться", callback_data="fight")],
    ])
    await answer_fn(
        f"⚔️ {mention(user)} ищет соперника!\nСтавка: {amount} см",
        reply_markup=kb
    )


@dp.callback_query(F.data == "fight")
async def fight_callback(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT challenger, amount FROM fights WHERE chat_id=?", (chat_id,)
        )
        fight_row = await cursor.fetchone()
    if not fight_row:
        await callback.answer("Бой уже завершён")
        return
    challenger, amount = fight_row
    if user_id == challenger:
        await callback.answer("Нельзя драться с собой 😅")
        return
    c_size, _ = await get_user(challenger)
    u_size, _ = await get_user(user_id)
    if c_size < amount or u_size < amount:
        await callback.answer("У кого-то не хватает см 😢")
        return
    winner = random.choice([challenger, user_id])
    loser = challenger if winner == user_id else user_id
    w_size, _ = await get_user(winner)
    l_size, _ = await get_user(loser)
    await update_size(winner, w_size + amount)
    await update_size(loser, max(0, l_size - amount))
    challenger_user = (await bot.get_chat_member(chat_id, challenger)).user
    opponent_user = callback.from_user
    winner_user = challenger_user if winner == challenger else opponent_user
    loser_user = opponent_user if winner == challenger else challenger_user
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM fights WHERE chat_id=?", (chat_id,))
        await db.execute(
            "UPDATE users SET wins = COALESCE(wins,0)+1 WHERE user_id=?", (winner,)
        )
        await db.execute(
            "UPDATE users SET loses = COALESCE(loses,0)+1 WHERE user_id=?", (loser,)
        )
        await db.commit()
    await callback.message.edit_text(
        f"⚔️ Бой состоялся!\n\n"
        f"🏆 Победитель: {mention(winner_user)}\n"
        f"💀 Проигравший: {mention(loser_user)}\n"
        f"Ставка: {amount} см",
        reply_markup=menu_kb()
    )
    await callback.answer("Бой завершён!")


# -------------------- FORBES --------------------

@dp.message(Command("forbes"))
async def forbes(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await message.answer("⏳ Считаем состояния...")
    text, kb = await _do_forbes()
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "cmd_forbes")
async def cmd_forbes_cb(callback: CallbackQuery):
    if callback.message.chat.type != "private":
        await save_user_chat(callback.from_user.id, callback.message.chat.id)
    await callback.message.answer("⏳ Считаем состояния...")
    text, kb = await _do_forbes()
    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


# -------------------- MARKET --------------------

async def _send_market(answer_fn):
    prices = await get_stock_prices()
    lines = "📊 <b>Реальные акции</b> (1$ = 1 см):\n"
    for ticker, name in STOCKS.items():
        price = prices.get(ticker, 0)
        lines += f"{name} (<code>{ticker}</code>): <b>{price:.0f} см/акция</b>\n"
    lines += "\n🎲 <b>Крипто-токены</b> (меняются каждые 10 мин):\n"
    for ticker, name in VOLATILE_STOCKS.items():
        price = prices.get(ticker, 0)
        lines += f"{name} (<code>{ticker}</code>): <b>{price:.0f} см/акция</b>\n"
    real_buttons = [
        [
            InlineKeyboardButton(text=f"🛒 {name.split()[1]}", callback_data=f"mkt_buy_{ticker}"),
            InlineKeyboardButton(text=f"💸 {ticker}", callback_data=f"mkt_sell_{ticker}"),
        ]
        for ticker, name in STOCKS.items()
    ]
    volatile_buttons = [
        [
            InlineKeyboardButton(text=f"🛒 {ticker}", callback_data=f"mkt_buy_{ticker}"),
            InlineKeyboardButton(text=f"💸 {ticker}", callback_data=f"mkt_sell_{ticker}"),
        ]
        for ticker in VOLATILE_STOCKS
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=real_buttons + volatile_buttons)
    await answer_fn(f"📈 <b>Рынок акций</b>\n\n{lines}", reply_markup=kb)


@dp.message(Command("market"))
async def market(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await message.answer("⏳ Получаем котировки...")
    await _send_market(message.answer)


@dp.callback_query(F.data == "cmd_market")
async def cmd_market_cb(callback: CallbackQuery):
    if callback.message.chat.type != "private":
        await save_user_chat(callback.from_user.id, callback.message.chat.id)
    await callback.message.answer("⏳ Получаем котировки...")
    await _send_market(callback.message.answer)
    await callback.answer()


@dp.callback_query(F.data.startswith("mkt_buy_"))
async def market_buy_cb(callback: CallbackQuery, state: FSMContext):
    ticker = callback.data[8:]
    name = ALL_STOCKS.get(ticker, ticker)
    prices = await get_stock_prices()
    price = prices.get(ticker, 0)
    await state.set_state(MarketState.waiting_buy)
    await state.update_data(ticker=ticker)
    await callback.message.answer(
        f"🛒 Покупка {name}\n"
        f"📊 Текущая цена: <b>{price:.0f} см/акция</b>\n\n"
        f"Сколько <b>акций</b> купить?"
    )
    await callback.answer()


@dp.message(MarketState.waiting_buy)
async def market_buy_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    ticker = data["ticker"]
    user_id = message.from_user.id
    try:
        shares_to_buy = float(message.text.strip().replace(",", "."))
        if shares_to_buy <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи число акций больше 0 (например: 1 или 0.5)")
        return
    prices = await get_stock_prices()
    price = prices.get(ticker, 0)
    if price <= 0:
        await message.answer("❌ Не удалось получить цену акции, попробуй позже")
        await state.clear()
        return
    cost = int(shares_to_buy * price)
    if cost <= 0:
        await message.answer("❗ Слишком малое количество акций")
        await state.clear()
        return
    size, _ = await get_user(user_id)
    if size < cost:
        await message.answer(
            f"❌ Недостаточно см!\nНужно: {cost} см, у тебя: {size} см",
            reply_markup=menu_kb()
        )
        await state.clear()
        return
    await update_size(user_id, size - cost)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """INSERT INTO portfolios (user_id, ticker, shares) VALUES (?, ?, ?)
               ON CONFLICT(user_id, ticker) DO UPDATE SET shares = shares + excluded.shares""",
            (user_id, ticker, shares_to_buy)
        )
        await db.commit()
    await message.answer(
        f"✅ Куплено <b>{shares_to_buy:g}</b> акций {ALL_STOCKS.get(ticker, ticker)}\n"
        f"💸 Потрачено: {cost} см\n📊 Цена покупки: {price:.0f} см/акция",
        reply_markup=menu_kb()
    )
    await state.clear()


@dp.callback_query(F.data.startswith("mkt_sell_"))
async def market_sell_cb(callback: CallbackQuery, state: FSMContext):
    ticker = callback.data[9:]
    user_id = callback.from_user.id
    portfolio = await get_portfolio(user_id)
    shares = portfolio.get(ticker, 0)
    if shares <= 0:
        await callback.answer("У тебя нет этих акций 📭", show_alert=True)
        return
    name = ALL_STOCKS.get(ticker, ticker)
    await state.set_state(MarketState.waiting_sell)
    await state.update_data(ticker=ticker)
    await callback.message.answer(
        f"💸 Продажа {name}\n"
        f"У тебя: <b>{shares:.4f}</b> акций\n"
        f"Сколько продать? (или напиши <b>все</b>)"
    )
    await callback.answer()


@dp.message(MarketState.waiting_sell)
async def market_sell_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    ticker = data["ticker"]
    user_id = message.from_user.id
    portfolio = await get_portfolio(user_id)
    owned = portfolio.get(ticker, 0)
    text = message.text.strip().lower()
    if text in ("все", "всё", "all"):
        sell_shares = owned
    else:
        try:
            sell_shares = float(text.replace(",", "."))
            if sell_shares <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❗ Введи число или напиши <b>все</b>")
            return
    if sell_shares > owned + 1e-9:
        await message.answer(f"❌ У тебя только {owned:.4f} акций")
        await state.clear()
        return
    prices = await get_stock_prices()
    price = prices.get(ticker, 0)
    if price <= 0:
        await message.answer("❌ Не удалось получить цену акции, попробуй позже")
        await state.clear()
        return
    earned = int(sell_shares * price)
    new_shares = owned - sell_shares
    async with aiosqlite.connect(DB_NAME) as db:
        if new_shares < 1e-6:
            await db.execute(
                "DELETE FROM portfolios WHERE user_id=? AND ticker=?", (user_id, ticker)
            )
        else:
            await db.execute(
                "UPDATE portfolios SET shares=? WHERE user_id=? AND ticker=?",
                (new_shares, user_id, ticker)
            )
        await db.commit()
    size, _ = await get_user(user_id)
    await update_size(user_id, size + earned)
    await message.answer(
        f"✅ Продано <b>{sell_shares:.4f}</b> акций {ALL_STOCKS.get(ticker, ticker)}\n"
        f"💰 Получено: +{earned} см\n🥒 Огурец: {size + earned} см",
        reply_markup=menu_kb()
    )
    await state.clear()


# -------------------- SLOTS --------------------

SLOT_SYMBOLS = ["🍋", "🍇", "💎", "7️⃣"]
SLOT_WEIGHTS = [40, 35, 20, 5]


def spin_reels():
    return [random.choices(SLOT_SYMBOLS, weights=SLOT_WEIGHTS)[0] for _ in range(3)]


@dp.message(Command("slots"))
async def slots_command(message: Message):
    try:
        amount = int(message.text.split()[1])
        if amount <= 0:
            raise ValueError
    except (IndexError, ValueError):
        await message.answer("❗ Пример: /slots 10\nСделай ставку в сантиметрах и крути автомат!")
        return
    await _do_slots(
        message.from_user, message.chat.id, message.chat.type == "private", amount, message.answer
    )


@dp.callback_query(F.data == "cmd_slots_menu")
async def cmd_slots_menu_cb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SlotsState.waiting_amount)
    await callback.message.answer("🎰 Введи ставку (в см):")
    await callback.answer()


@dp.message(SlotsState.waiting_amount)
async def slots_amount_input(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи целое число больше 0")
        return
    await state.clear()
    await _do_slots(
        message.from_user, message.chat.id, message.chat.type == "private", amount, message.answer
    )


@dp.callback_query(F.data.startswith("slot_"))
async def slot_spin_callback(callback: CallbackQuery):
    parts = callback.data.split("_")
    original_user_id = int(parts[1])
    amount = int(parts[2])
    if callback.from_user.id != original_user_id:
        await callback.answer("Это не твой автомат! 😅")
        return
    user_id = callback.from_user.id
    await callback.message.edit_reply_markup(reply_markup=None)
    reels = spin_reels()
    result_line = " | ".join(reels)
    is_jackpot = reels[0] == reels[1] == reels[2] == "7️⃣"
    is_three_same = reels[0] == reels[1] == reels[2] and not is_jackpot
    size, _ = await get_user(user_id)
    header = (
        "╔═══🎰 CASINO 🎰═══╗\n"
        f"║  {result_line}              ║\n"
        "╚════════════════╝"
    )
    if is_jackpot:
        winnings = amount * 10
        await update_size(user_id, size + winnings)
        await callback.message.answer(
            f"{header}\n\n🏆 <b>ДЖЕКПОТ! 7️⃣7️⃣7️⃣</b>\n━━━━━━━━━━━━━━━\n"
            f"👤 {mention(callback.from_user)}\n"
            f"💰 Выигрыш: <b>+{winnings} см</b>\n🥒 Огурец: <b>{size + winnings} см</b>",
            reply_markup=menu_kb()
        )
    elif is_three_same:
        winnings = int(amount * 5)
        await update_size(user_id, size + winnings)
        await callback.message.answer(
            f"{header}\n\n🎊 <b>Три одинаковых!</b>\n━━━━━━━━━━━━━━━\n"
            f"👤 {mention(callback.from_user)}\n"
            f"💰 Выигрыш: <b>+{winnings} см</b>\n🥒 Огурец: <b>{size + winnings} см</b>",
            reply_markup=menu_kb()
        )
    else:
        await add_to_bank(amount)
        await callback.message.answer(
            f"{header}\n\n😢 <b>Не повезло...</b>\n━━━━━━━━━━━━━━━\n"
            f"👤 {mention(callback.from_user)}\n"
            f"📉 Ставка: <b>-{amount} см</b>\n🥒 Огурец: <b>{size} см</b>\n"
            f"🏦 Cucumber Bank пополнен на {amount} см",
            reply_markup=menu_kb()
        )
    await callback.answer()


# -------------------- BANK --------------------

async def _send_bank(user_id: int, answer_fn):
    loan, interest = await apply_loan_interest(user_id)
    capital = await get_bank_capital()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT loan FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
    loan = row[0] if row and row[0] else 0
    text = (
        f"🏦 <b>Cucumber Bank</b>\n━━━━━━━━━━━━━━━\n"
        f"💰 Капитал банка: <b>{capital} см</b>\n━━━━━━━━━━━━━━━\n"
    )
    buttons = []
    if loan > 0:
        if interest > 0:
            text += f"📈 Начислены проценты: <b>+{interest} см к долгу</b>\n"
        text += (
            f"💳 Твой долг: <b>{loan} см</b>\n"
            f"⚠️ Ежедневно +30% на остаток долга\n"
        )
        buttons.append([InlineKeyboardButton(
            text=f"💳 Погасить кредит ({loan} см)", callback_data="repay_loan"
        )])
    else:
        text += "📋 <i>Кредиты под 30% в день</i>\n"
        buttons.append([InlineKeyboardButton(text="💵 Взять кредит", callback_data="take_loan")])
    await answer_fn(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@dp.message(Command("bank"))
async def bank_command(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await _send_bank(message.from_user.id, message.answer)


@dp.callback_query(F.data == "cmd_bank")
async def cmd_bank_cb(callback: CallbackQuery):
    if callback.message.chat.type != "private":
        await save_user_chat(callback.from_user.id, callback.message.chat.id)
    await _send_bank(callback.from_user.id, callback.message.answer)
    await callback.answer()


@dp.callback_query(F.data == "take_loan")
async def take_loan_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT loan FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
    if row and row[0] and row[0] > 0:
        await callback.answer("❌ У тебя уже есть непогашенный кредит!", show_alert=True)
        return
    capital = await get_bank_capital()
    if capital <= 0:
        await callback.answer("❌ В банке нет средств для выдачи кредита!", show_alert=True)
        return
    size, _ = await get_user(user_id)
    max_loan = min(capital, 10000)
    await callback.message.answer(
        f"💵 <b>Кредит от Cucumber Bank</b>\n━━━━━━━━━━━━━━━\n"
        f"⚠️ Ставка: <b>30% в день</b> на остаток долга\n"
        f"🏦 Доступно в банке: <b>{capital} см</b>\n"
        f"🥒 У тебя сейчас: <b>{size} см</b>\n\n"
        f"Введи сумму кредита (максимум {max_loan} см):"
    )
    await state.set_state(LoanState.waiting_amount)
    await callback.answer()


@dp.message(LoanState.waiting_amount)
async def process_loan_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи целое число больше 0!")
        return
    capital = await get_bank_capital()
    max_loan = min(capital, 10000)
    if amount > max_loan:
        if capital <= 0:
            await message.answer("❌ В банке закончились средства. Кредит недоступен.")
            await state.clear()
            return
        await message.answer(f"❗ Максимально доступная сумма: {max_loan} см (в банке {capital} см)")
        return
    now = now_msk()
    size, _ = await get_user(user_id)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET loan=?, loan_date=? WHERE user_id=?",
            (amount, now.isoformat(), user_id)
        )
        await db.execute("UPDATE bank SET capital = capital - ? WHERE id = 1", (amount,))
        await db.commit()
    await update_size(user_id, size + amount)
    await state.clear()
    await message.answer(
        f"✅ <b>Кредит выдан!</b>\n━━━━━━━━━━━━━━━\n"
        f"💵 Получено: <b>+{amount} см</b>\n"
        f"💳 Долг: <b>{amount} см</b>\n"
        f"⚠️ Каждые 24 ч банк начисляет <b>+30%</b> на остаток\n"
        f"🥒 Огурец: <b>{size + amount} см</b>",
        reply_markup=menu_kb()
    )


@dp.callback_query(F.data == "repay_loan")
async def repay_loan_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    await apply_loan_interest(user_id)
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT loan, size FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
    if not row or not row[0] or row[0] <= 0:
        await callback.answer("У тебя нет активного кредита.", show_alert=True)
        return
    loan_amount, size = row
    if size < loan_amount:
        await callback.answer(
            f"❌ Недостаточно средств!\nДолг: {loan_amount} см, у тебя: {size} см",
            show_alert=True
        )
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET loan=0, loan_date=NULL WHERE user_id=?", (user_id,))
        await db.commit()
    await update_size(user_id, size - loan_amount)
    await add_to_bank(loan_amount)
    await callback.message.answer(
        f"✅ <b>Кредит погашен!</b>\n━━━━━━━━━━━━━━━\n"
        f"💳 Выплачено: <b>{loan_amount} см</b>\n"
        f"🥒 Огурец: <b>{size - loan_amount} см</b>\n"
        f"🏦 Cucumber Bank благодарит за своевременную оплату!",
        reply_markup=menu_kb()
    )
    await callback.answer()


# -------------------- КОМАНДЫ --------------------

async def set_commands(bot_: Bot):
    group_commands = [
        BotCommand(command="start", description="Главное меню 🏠"),
        BotCommand(command="grow", description="Вырастить огурец 🌱"),
        BotCommand(command="stats", description="Статистика 📊"),
        BotCommand(command="top", description="Топ игроков 🏆"),
        BotCommand(command="fight", description="Создать бой ⚔️"),
        BotCommand(command="shop", description="Магазин 🛒"),
        BotCommand(command="box", description="Лутбокс 🎁"),
        BotCommand(command="slots", description="Слоты 🎰"),
        BotCommand(command="market", description="Рынок акций 📈"),
        BotCommand(command="forbes", description="Forbes 💼"),
        BotCommand(command="bank", description="Банк 🏦"),
    ]
    private_commands = [
        BotCommand(command="start", description="Главное меню 🏠"),
        BotCommand(command="grow", description="Вырастить огурец 🌱"),
        BotCommand(command="stats", description="Статистика 📊"),
        BotCommand(command="shop", description="Магазин 🛒"),
        BotCommand(command="box", description="Лутбокс 🎁"),
        BotCommand(command="slots", description="Слоты 🎰"),
        BotCommand(command="market", description="Рынок акций 📈"),
        BotCommand(command="forbes", description="Forbes 💼"),
        BotCommand(command="bank", description="Банк 🏦"),
    ]
    await bot_.set_my_commands(group_commands, scope=BotCommandScopeAllGroupChats())
    await bot_.set_my_commands(private_commands, scope=BotCommandScopeAllPrivateChats())


# -------------------- ЗАПУСК --------------------

async def main():
    global BOT_USERNAME
    await init_db()
    await load_volatile_prices()
    await set_commands(bot)
    bot_info = await bot.get_me()
    BOT_USERNAME = bot_info.username
    asyncio.create_task(update_volatile_prices())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
