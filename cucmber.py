import asyncio
import io
import os
import random
import aiosqlite
import aiohttp
from aiohttp import web as aio_web
from datetime import datetime, timedelta
from aiogram.types import LabeledPrice, PreCheckoutQuery, BufferedInputFile
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    CallbackQuery, BotCommand,
    BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats,
    WebAppInfo,
)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False
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

DB_NAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cucumbers.db")
ADMIN_ID = 5971748042
BOT_USERNAME = ""
WEBAPP_URL = os.environ.get("WEBAPP_URL", "")  # set via env: WEBAPP_URL=https://your-ngrok-url.ngrok.io


def now_msk():
    return datetime.now(MSK)


# -------------------- АКЦИИ --------------------

STOCKS = {
    "AAPL":  "🍎 Apple",
    "TSLA":  "⚡ Tesla",
    "NVDA":  "🎮 NVIDIA",
    "AMZN":  "📦 Amazon",
    "GOOGL": "🔍 Google",
    "MSFT":  "🪟 Microsoft",
    "META":  "👓 Meta",
    "NFLX":  "🎬 Netflix",
    "BRKB":  "💼 Berkshire",
    "JPM":   "🏦 JPMorgan",
    "V":     "💳 Visa",
    "JNJ":   "💊 Johnson&Johnson",
    "WMT":   "🛒 Walmart",
    "DIS":   "🏰 Disney",
    "PYPL":  "🔵 PayPal",
    "INTC":  "🔲 Intel",
    "AMD":   "🔴 AMD",
    "ORCL":  "🗄️ Oracle",
    "CRM":   "☁️ Salesforce",
    "COIN":  "🪙 Coinbase",
}

# -------------------- БИЗНЕС — КОНФИГ --------------------

BIZ_TYPES = {
    "farm":    {"emoji": "🌾", "label": "Ферма",        "cost": 300,   "min_emp": 2, "base_out": 10,  "mat_cycle": 15, "salary": 3,  "tax": 0.10, "upg_cost": 150, "prod_h": 4},
    "factory": {"emoji": "🏭", "label": "Завод",        "cost": 800,   "min_emp": 3, "base_out": 15,  "mat_cycle": 20, "salary": 4,  "tax": 0.15, "upg_cost": 300, "prod_h": 4},
    "mine":    {"emoji": "⛏️",  "label": "Шахта",        "cost": 600,   "min_emp": 4, "base_out": 13,  "mat_cycle": 30, "salary": 6,  "tax": 0.18, "upg_cost": 250, "prod_h": 4},
    "brewery": {"emoji": "🍺", "label": "Пивоварня",    "cost": 500,   "min_emp": 2, "base_out": 12,  "mat_cycle": 25, "salary": 4,  "tax": 0.12, "upg_cost": 200, "prod_h": 4},
    "it":      {"emoji": "💻", "label": "IT-компания",  "cost": 1200,  "min_emp": 1, "base_out": 25,  "mat_cycle": 5,  "salary": 8,  "tax": 0.20, "upg_cost": 500, "prod_h": 4},
}

MATERIAL_QUALITY = {
    "low":    {"label": "🟤 Эконом",   "price": 1,  "eff": 0.7},
    "medium": {"label": "🔵 Стандарт", "price": 1,  "eff": 1.2},
    "high":   {"label": "💎 Премиум",  "price": 2,  "eff": 2.0},
}

BIZ_LEVEL_MULT = {1: 1.0, 2: 1.5, 3: 2.0, 4: 2.8, 5: 4.0}


async def get_total_ticker_shares(ticker: str) -> float:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(shares), 0) FROM portfolios WHERE ticker=?", (ticker,)
        )
        row = await cursor.fetchone()
    return row[0] if row else 0.0


async def update_real_stock_prices_loop():
    while True:
        await asyncio.sleep(3600)
        headers = {"User-Agent": "Mozilla/5.0"}
        prices = {}
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                for ticker in STOCKS:
                    try:
                        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
                        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as r:
                            data = await r.json(content_type=None)
                            prices[ticker] = round(data["chart"]["result"][0]["meta"]["regularMarketPrice"], 2)
                    except Exception:
                        pass
        except Exception:
            pass
        if not prices:
            continue
        now_str = now_msk().isoformat()
        async with aiosqlite.connect(DB_NAME) as db:
            for ticker, price in prices.items():
                await db.execute(
                    "INSERT INTO price_history (ticker, price, recorded_at) VALUES (?, ?, ?)",
                    (ticker, price, now_str)
                )
            for ticker in STOCKS:
                await db.execute(
                    """DELETE FROM price_history WHERE ticker=? AND id NOT IN (
                       SELECT id FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 72
                    )""", (ticker, ticker)
                )
            await db.commit()


async def luxury_tax_loop():
    while True:
        now = now_msk()
        target = now.replace(hour=8, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute("SELECT user_id, size FROM users WHERE size >= 1000")
            users = await cursor.fetchall()
        total_tax = 0
        for uid, sz in users:
            k = sz // 1000
            tax = 30 * k
            new_sz = max(0, sz - tax)
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_sz, uid))
                await db.commit()
            total_tax += tax
            try:
                await bot.send_message(
                    uid,
                    f"🏦 <b>Налог на роскошь — 08:00 МСК</b>\n"
                    f"📉 Списано: <b>{tax} см</b> (×{k} тыс.)\n"
                    f"🥒 Остаток: <b>{new_sz} см</b>"
                )
            except Exception:
                pass
        if total_tax > 0:
            await add_to_bank(total_tax)


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
    os.makedirs(os.path.dirname(DB_NAME), exist_ok=True)
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
        await db.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            price       REAL NOT NULL,
            recorded_at TEXT NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            deposit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            amount     INTEGER NOT NULL,
            rate       REAL NOT NULL,
            days       INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            matures_at TEXT NOT NULL,
            claimed    INTEGER DEFAULT 0
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            biz_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id   INTEGER NOT NULL,
            biz_type   TEXT NOT NULL,
            name       TEXT NOT NULL,
            level      INTEGER DEFAULT 1,
            employees  INTEGER DEFAULT 0,
            materials  INTEGER DEFAULT 0,
            mat_qual   TEXT DEFAULT 'low',
            goods      INTEGER DEFAULT 0,
            last_prod  TEXT,
            created_at TEXT
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS backgammon_games (
            game_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            player1_id INTEGER NOT NULL,
            player2_id INTEGER,
            bet        INTEGER NOT NULL DEFAULT 0,
            state_json TEXT,
            status     TEXT DEFAULT 'waiting',
            winner_id  INTEGER,
            created_at TEXT
        )""")
        try:
            await db.execute("ALTER TABLE clans ADD COLUMN treasury INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE businesses ADD COLUMN created_at TEXT")
        except Exception:
            pass
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
                "INSERT OR IGNORE INTO users (user_id, size, name) VALUES (?, 0, ?)", (user_id, name)
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


async def get_inflation_rate() -> float:
    """Returns inflation rate as a decimal 0.0–0.5 based on how many 'rich' users exist (size >= 5000)."""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE size >= 5000")
        row = await cursor.fetchone()
    rich_count = row[0] if row else 0
    return min(0.50, rich_count * 0.03)


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


async def auto_repay_loan(user_id: int, gained: int) -> tuple[int, int]:
    """Deduct gained cm toward loan. Returns (kept, repaid)."""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT loan FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
    if not row or not row[0] or row[0] <= 0:
        return gained, 0
    loan = row[0]
    repay = min(loan, gained)
    new_loan = loan - repay
    async with aiosqlite.connect(DB_NAME) as db:
        if new_loan > 0:
            await db.execute("UPDATE users SET loan=? WHERE user_id=?", (new_loan, user_id))
        else:
            await db.execute("UPDATE users SET loan=0, loan_date=NULL WHERE user_id=?", (user_id,))
        await db.commit()
    await add_to_bank(repay)
    return gained - repay, repay


def _render_chart_sync(ticker: str, name: str, rows: list) -> io.BytesIO:
    prices = [r[0] for r in rows]
    xs = list(range(len(prices)))
    pct = ((prices[-1] - prices[0]) / prices[0]) * 100 if prices[0] else 0
    color = "#00c853" if pct >= 0 else "#d50000"
    arrow = "+" if pct >= 0 else ""
    fig, ax = plt.subplots(figsize=(8, 4), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")
    ax.plot(xs, prices, color=color, linewidth=2.5)
    ax.fill_between(xs, prices, min(prices) * 0.95, alpha=0.2, color=color)
    ax.set_title(f"{name} ({ticker})   {arrow}{pct:.1f}%", fontsize=13, color=color, pad=10)
    ax.set_xlabel("Price updates", color="#aaaaaa", fontsize=9)
    ax.set_ylabel("Price (cm)", color="#aaaaaa", fontsize=9)
    ax.tick_params(colors="#aaaaaa")
    for spine in ax.spines.values():
        spine.set_edgecolor("#333355")
    ax.grid(True, alpha=0.2, color="#444466")
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


async def generate_price_chart(ticker: str) -> io.BytesIO | None:
    if not CHARTS_AVAILABLE:
        return None
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT price, recorded_at FROM price_history WHERE ticker=? ORDER BY id ASC LIMIT 72",
            (ticker,)
        )
        rows = await cursor.fetchall()
    if len(rows) < 2:
        return None
    name = STOCKS.get(ticker, ticker)
    try:
        buf = await asyncio.to_thread(_render_chart_sync, ticker, name, rows)
    except Exception:
        return None
    return buf


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
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT treasury FROM clans WHERE clan_id=?", (clan_id,))
        trow = await cursor.fetchone()
    treasury = trow[0] if trow and trow[0] else 0
    text = (
        f"🛡 <b>{name}</b>\n"
        f"👥 Участников: {len(members)}\n"
        f"🎖 Твоя роль: <b>{role}</b>\n"
        f"💰 Общак: <b>{treasury} см</b>"
    )
    buttons = [
        [InlineKeyboardButton(text="👥 Участники", callback_data=f"clan_members_{clan_id}")],
        [InlineKeyboardButton(text="💰 Общак", callback_data=f"clan_treasury_{clan_id}")],
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
    "🏆 Топ чата", "⚔️ Бой", "🛡 Клан", "📩 Поддержка", "💸 Перевод",
    "🏗️ Бизнес",
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
        rows.append([KeyboardButton(text="🛡 Клан"), KeyboardButton(text="💸 Перевод")])
        rows.append([KeyboardButton(text="🏗️ Бизнес")])
        rows.append([KeyboardButton(text="📩 Поддержка")])
    if WEBAPP_URL:
        rows.append([KeyboardButton(text="🌐 Веб-приложение", web_app=WebAppInfo(url=WEBAPP_URL))])
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


class TransferState(StatesGroup):
    waiting_target = State()
    waiting_amount = State()
    waiting_confirm = State()


class DepositState(StatesGroup):
    waiting_amount = State()


class AdminReplyState(StatesGroup):
    waiting_reply = State()


class ClanTreasuryState(StatesGroup):
    waiting_contribute = State()
    waiting_withdraw = State()


class BizCreateState(StatesGroup):
    waiting_name = State()

class BizMatState(StatesGroup):
    waiting_qty = State()

class BizHireState(StatesGroup):
    waiting_count = State()


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
        [InlineKeyboardButton(text="📊 Статистика игрока", callback_data=f"admin_stats_{user_id}")],
        [InlineKeyboardButton(text="✉️ Написать игроку", callback_data=f"admin_write_{user_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users")],
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
    await update_size(user_id, new_size)
    net_gain = growth - tax
    repay_note = ""
    if net_gain > 0:
        _, repaid = await auto_repay_loan(user_id, net_gain)
        if repaid > 0:
            repay_note = f"\n💳 Автопогашение долга: -{repaid} см"
            new_size = max(0, new_size - repaid)
            await update_size(user_id, new_size)
    return (
        f"🌱 {mention(user)}\n"
        f"+{growth} см\nТеперь: {size} см\n"
        f"💸 Налог 20%: -{tax} см"
        f"{repay_note}\n"
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
    elif size < 20000:
        role = "💍 Муж Марички"
    else:
        role = 'Покоритель сочных одиннадцатикласниц'
    prices = await get_stock_prices()
    portfolio = await get_portfolio(user_id)
    portfolio_lines = ""
    portfolio_total = 0
    for ticker, shares in portfolio.items():
        price = prices.get(ticker, 0)
        value = int(shares * price)
        portfolio_total += value
        portfolio_lines += f"  {STOCKS.get(ticker, ticker)}: {shares:g} шт × {price:.0f} = <b>{value} см</b>\n"
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
    repay_note = ""
    _, repaid = await auto_repay_loan(user_id, reward)
    if repaid > 0:
        repay_note = f"\n💳 Автопогашение долга: -{repaid} см"
        size = max(0, size - repaid)
        await update_size(user_id, size)
    return (
        f"🎁 {mention(user)} открыл лутбокс!\n\n"
        f"✨ Редкость: {rarity}\n💰 Награда: +{reward} см"
        f"{repay_note}\n\n📏 Теперь: {size} см"
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
    reply_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✉️ Ответить пользователю", callback_data=f"admin_reply_{user.id}")
    ]])
    try:
        await bot.send_message(ADMIN_ID, header, reply_markup=reply_kb)
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


@dp.callback_query(F.data.startswith("clan_treasury_"))
async def clan_treasury_cb(callback: CallbackQuery):
    clan_id = int(callback.data[14:])
    user_id = callback.from_user.id
    member = await get_clan_member(user_id)
    if not member or member[0] != clan_id:
        await callback.answer("❌ Ты не в этом клане", show_alert=True)
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT name, owner_id, treasury FROM clans WHERE clan_id=?", (clan_id,)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Клан не найден", show_alert=True)
        return
    clan_name, owner_id, treasury = row
    treasury = treasury or 0
    size, _ = await get_user(user_id)
    btns = [
        [InlineKeyboardButton(text="💸 Положить в общак", callback_data=f"clan_contrib_{clan_id}")],
    ]
    if user_id == owner_id and treasury > 0:
        btns.append([InlineKeyboardButton(
            text=f"📤 Вывести из общака ({treasury} см)", callback_data=f"clan_withdraw_{clan_id}"
        )])
    await callback.message.answer(
        f"💰 <b>Общак клана «{clan_name}»</b>\n━━━━━━━━━━━━━━━\n"
        f"💵 В общаке: <b>{treasury} см</b>\n"
        f"🥒 У тебя: <b>{size} см</b>\n\n"
        f"Любой участник может положить сантиметры в общак.\n"
        f"Вывести может только лидер клана.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("clan_contrib_"))
async def clan_contrib_cb(callback: CallbackQuery, state: FSMContext):
    clan_id = int(callback.data[13:])
    user_id = callback.from_user.id
    member = await get_clan_member(user_id)
    if not member or member[0] != clan_id:
        await callback.answer("❌ Ты не в этом клане", show_alert=True)
        return
    size, _ = await get_user(user_id)
    await state.set_state(ClanTreasuryState.waiting_contribute)
    await state.update_data(clan_id=clan_id)
    await callback.message.answer(
        f"💸 Сколько см положить в общак?\n"
        f"💰 Твой баланс: {size} см\n/cancel — отмена"
    )
    await callback.answer()


@dp.message(ClanTreasuryState.waiting_contribute)
async def clan_contrib_input(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return
    user_id = message.from_user.id
    size, _ = await get_user(user_id)
    if size < amount:
        await message.answer(f"❌ Недостаточно см: у тебя {size} см.")
        return
    data = await state.get_data()
    clan_id = data["clan_id"]
    member = await get_clan_member(user_id)
    if not member or member[0] != clan_id:
        await state.clear()
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE clans SET treasury = treasury + ? WHERE clan_id=?", (amount, clan_id)
        )
        await db.commit()
    await update_size(user_id, size - amount)
    await state.clear()
    await message.answer(
        f"✅ <b>+{amount} см</b> добавлено в общак клана!\n"
        f"🥒 Твой остаток: {size - amount} см"
    )


@dp.callback_query(F.data.startswith("clan_withdraw_"))
async def clan_withdraw_cb(callback: CallbackQuery, state: FSMContext):
    clan_id = int(callback.data[14:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT owner_id, treasury FROM clans WHERE clan_id=?", (clan_id,)
        )
        row = await cursor.fetchone()
    if not row or row[0] != user_id:
        await callback.answer("❌ Только лидер может выводить средства", show_alert=True)
        return
    treasury = row[1] or 0
    if treasury <= 0:
        await callback.answer("Общак пуст", show_alert=True)
        return
    await state.set_state(ClanTreasuryState.waiting_withdraw)
    await state.update_data(clan_id=clan_id)
    await callback.message.answer(
        f"📤 Вывод из общака\n"
        f"💵 Доступно: <b>{treasury} см</b>\n"
        f"Сколько вывести?\n/cancel — отмена"
    )
    await callback.answer()


@dp.message(ClanTreasuryState.waiting_withdraw)
async def clan_withdraw_input(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return
    user_id = message.from_user.id
    data = await state.get_data()
    clan_id = data["clan_id"]
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT owner_id, treasury FROM clans WHERE clan_id=?", (clan_id,)
        )
        row = await cursor.fetchone()
    if not row or row[0] != user_id:
        await state.clear()
        return
    treasury = row[1] or 0
    if amount > treasury:
        await message.answer(f"❌ В общаке только {treasury} см.")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE clans SET treasury = treasury - ? WHERE clan_id=?", (amount, clan_id)
        )
        await db.commit()
    size, _ = await get_user(user_id)
    await update_size(user_id, size + amount)
    await state.clear()
    await message.answer(
        f"✅ Выведено <b>{amount} см</b> из общака!\n"
        f"🥒 Твой баланс: {size + amount} см"
    )


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
    lines = "📈 <b>Рынок акций</b> (1$ = 1 см):\n\n"
    for ticker, name in STOCKS.items():
        price = prices.get(ticker, 0)
        lines += f"{name} (<code>{ticker}</code>): <b>{price:.0f} см</b>\n"
    buttons = []
    for ticker, name in STOCKS.items():
        nm = name.split(' ', 1)[1] if ' ' in name else name
        buttons.append([
            InlineKeyboardButton(text=f"🛒 {nm}", callback_data=f"mkt_buy_{ticker}"),
            InlineKeyboardButton(text=f"💸 Продать", callback_data=f"mkt_sell_{ticker}"),
            InlineKeyboardButton(text="📈 График", callback_data=f"mkt_chart_{ticker}"),
        ])
    await answer_fn(lines, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@dp.message(Command("market"))
async def market(message: Message):
    if message.chat.type != "private":
        await save_user_chat(message.from_user.id, message.chat.id)
    await _send_market(message.answer)


@dp.callback_query(F.data == "cmd_market")
async def cmd_market_cb(callback: CallbackQuery):
    if callback.message.chat.type != "private":
        await save_user_chat(callback.from_user.id, callback.message.chat.id)
    await _send_market(callback.message.answer)
    await callback.answer()


@dp.callback_query(F.data == "mkt_tab_stocks")
async def mkt_tab_stocks_cb(callback: CallbackQuery):
    await callback.message.answer("⏳ Получаем котировки...")
    await _send_market(callback.message.answer)
    await callback.answer()


@dp.callback_query(F.data.startswith("mkt_chart_"))
async def mkt_chart_cb(callback: CallbackQuery):
    ticker = callback.data[10:]
    if ticker not in STOCKS:
        await callback.answer("Неверный тикер", show_alert=True)
        return
    await callback.answer("⏳ Генерируем график...")
    chart_buf = await generate_price_chart(ticker)
    if chart_buf is None:
        await callback.message.answer(
            f"❌ Данных для графика {ticker} ещё нет.\n"
            f"График появится через ≈1–2 ч (акции сохраняются раз в час)."
        )
        return
    name = STOCKS.get(ticker, ticker)
    await callback.message.answer_photo(
        BufferedInputFile(chart_buf.read(), filename=f"{ticker}.png"),
        caption=f"📈 <b>График {name} ({ticker})</b>"
    )


@dp.callback_query(F.data.startswith("mkt_buy_"))
async def market_buy_cb(callback: CallbackQuery, state: FSMContext):
    ticker = callback.data[8:]
    name = STOCKS.get(ticker, ticker)
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
        f"✅ Куплено <b>{shares_to_buy:g}</b> акций {STOCKS.get(ticker, ticker)}\n"
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
    name = STOCKS.get(ticker, ticker)
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
    gross = int(sell_shares * price)
    commission = max(1, int(gross * 0.05))
    earned = gross - commission
    await add_to_bank(commission)
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
        f"✅ Продано <b>{sell_shares:.4f}</b> акций {STOCKS.get(ticker, ticker)}\n"
        f"💰 Сумма: {gross} см  •  Комиссия 5%: -{commission} см\n"
        f"📥 Получено: <b>+{earned} см</b>\n🥒 Огурец: {size + earned} см",
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
        final_size = size + winnings
        await update_size(user_id, final_size)
        _, repaid = await auto_repay_loan(user_id, winnings)
        if repaid:
            final_size = max(0, final_size - repaid)
            await update_size(user_id, final_size)
        repay_note = f"\n💳 Автопогашение: -{repaid} см" if repaid else ""
        await callback.message.answer(
            f"{header}\n\n🏆 <b>ДЖЕКПОТ! 7️⃣7️⃣7️⃣</b>\n━━━━━━━━━━━━━━━\n"
            f"👤 {mention(callback.from_user)}\n"
            f"💰 Выигрыш: <b>+{winnings} см</b>{repay_note}\n🥒 Огурец: <b>{final_size} см</b>",
            reply_markup=menu_kb()
        )
    elif is_three_same:
        winnings = int(amount * 5)
        final_size = size + winnings
        await update_size(user_id, final_size)
        _, repaid = await auto_repay_loan(user_id, winnings)
        if repaid:
            final_size = max(0, final_size - repaid)
            await update_size(user_id, final_size)
        repay_note = f"\n💳 Автопогашение: -{repaid} см" if repaid else ""
        await callback.message.answer(
            f"{header}\n\n🎊 <b>Три одинаковых!</b>\n━━━━━━━━━━━━━━━\n"
            f"👤 {mention(callback.from_user)}\n"
            f"💰 Выигрыш: <b>+{winnings} см</b>{repay_note}\n🥒 Огурец: <b>{final_size} см</b>",
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
    inflation = await get_inflation_rate()
    inflation_pct = inflation * 100
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT loan FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()
    loan = row[0] if row and row[0] else 0
    if inflation_pct < 5:
        inf_emoji = "🟢"
    elif inflation_pct < 20:
        inf_emoji = "🟡"
    else:
        inf_emoji = "🔴"
    text = (
        f"🏦 <b>Cucumber Bank</b>\n━━━━━━━━━━━━━━━\n"
        f"💰 Капитал банка: <b>{capital} см</b>\n"
        f"{inf_emoji} Инфляция: <b>{inflation_pct:.1f}%</b> "
        f"<i>(богатых игроков: {int(inflation_pct / 3)})</i>\n━━━━━━━━━━━━━━━\n"
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
    buttons.append([
        InlineKeyboardButton(text="💰 Открыть вклад", callback_data="deposit_new"),
        InlineKeyboardButton(text="📋 Мои вклады", callback_data="deposit_list"),
    ])
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
    max_loan = min(capital, 100000)
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
    max_loan = min(capital, 100000)
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


# -------------------- ВКЛАДЫ --------------------

@dp.callback_query(F.data == "deposit_new")
async def deposit_new_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    size, _ = await get_user(user_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 1 день (+5%)", callback_data="deposit_days_1")],
        [InlineKeyboardButton(text="📅 2 дня (+12%)", callback_data="deposit_days_2")],
        [InlineKeyboardButton(text="📅 3 дня (+20%)", callback_data="deposit_days_3")],
    ])
    await callback.message.answer(
        f"💰 <b>Банковский вклад</b>\n\nУ тебя: <b>{size} см</b>\n\n"
        f"Выбери срок — снять досрочно <b>нельзя</b>:",
        reply_markup=kb
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("deposit_days_"))
async def deposit_days_cb(callback: CallbackQuery, state: FSMContext):
    days = int(callback.data[13:])
    rates = {1: 0.05, 2: 0.12, 3: 0.20}
    rate = rates[days]
    await state.set_state(DepositState.waiting_amount)
    await state.update_data(days=days, rate=rate)
    await callback.message.answer(
        f"💰 Вклад на {days} {'день' if days == 1 else 'дня'} под {int(rate * 100)}%\n"
        f"Введи сумму (в см):\n\n/cancel — отменить"
    )
    await callback.answer()


@dp.message(DepositState.waiting_amount)
async def deposit_amount_input(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введи целое число больше 0")
        return
    user_id = message.from_user.id
    size, _ = await get_user(user_id)
    if size < amount:
        await message.answer(f"❌ Недостаточно см! У тебя {size} см")
        return
    data = await state.get_data()
    days, rate = data["days"], data["rate"]
    now = now_msk()
    matures_at = (now + timedelta(days=days)).isoformat()
    await update_size(user_id, size - amount)
    await add_to_bank(amount)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO deposits (user_id, amount, rate, days, created_at, matures_at) VALUES (?,?,?,?,?,?)",
            (user_id, amount, rate, days, now.isoformat(), matures_at)
        )
        await db.commit()
    await state.clear()
    interest = int(amount * rate)
    await message.answer(
        f"✅ <b>Вклад открыт!</b>\n"
        f"💰 Сумма: {amount} см\n"
        f"📈 Процент: {int(rate * 100)}%\n"
        f"💵 Получишь: <b>{amount + interest} см</b>\n"
        f"📅 Доступно: {(now + timedelta(days=days)).strftime('%d.%m %H:%M')} (МСК)"
    )


@dp.callback_query(F.data == "deposit_list")
async def deposit_list_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT deposit_id, amount, rate, days, matures_at FROM deposits "
            "WHERE user_id=? AND claimed=0 ORDER BY matures_at",
            (user_id,)
        )
        rows = await cursor.fetchall()
    if not rows:
        await callback.message.answer("📋 У тебя нет активных вкладов.")
        await callback.answer()
        return
    text = "📋 <b>Твои вклады:</b>\n\n"
    buttons = []
    for did, amount, rate, days, matures_at in rows:
        matures = datetime.fromisoformat(matures_at)
        if matures.tzinfo is None:
            matures = MSK.localize(matures)
        interest = int(amount * rate)
        total = amount + interest
        if now >= matures:
            status = "✅ Готов"
            buttons.append([InlineKeyboardButton(
                text=f"💵 Получить {total} см", callback_data=f"deposit_claim_{did}"
            )])
        else:
            rem = matures - now
            h, r = divmod(int(rem.total_seconds()), 3600)
            status = f"⏳ {h}ч {r // 60}м"
        text += f"💰 {amount} → {total} см ({int(rate*100)}%) — {status}\n"
    await callback.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("deposit_claim_"))
async def deposit_claim_cb(callback: CallbackQuery):
    deposit_id = int(callback.data[14:])
    user_id = callback.from_user.id
    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT amount, rate, matures_at, claimed FROM deposits WHERE deposit_id=? AND user_id=?",
            (deposit_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Вклад не найден", show_alert=True)
        return
    amount, rate, matures_at, claimed = row
    if claimed:
        await callback.answer("Вклад уже получен", show_alert=True)
        return
    matures = datetime.fromisoformat(matures_at)
    if matures.tzinfo is None:
        matures = MSK.localize(matures)
    if now < matures:
        rem = matures - now
        h, r = divmod(int(rem.total_seconds()), 3600)
        await callback.answer(f"Ещё не созрел! Через {h}ч {r // 60}м", show_alert=True)
        return
    interest = int(amount * rate)
    payout = amount + interest
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE deposits SET claimed=1 WHERE deposit_id=?", (deposit_id,))
        await db.execute("UPDATE bank SET capital = MAX(0, capital - ?) WHERE id=1", (interest,))
        await db.commit()
    size, _ = await get_user(user_id)
    await update_size(user_id, size + payout)
    await callback.message.answer(
        f"💵 <b>Вклад получен!</b>\n"
        f"💰 Тело: {amount} см\n"
        f"📈 Проценты: +{interest} см\n"
        f"🥒 Итого получено: <b>{payout} см</b>"
    )
    await callback.answer()


# -------------------- БИЗНЕС --------------------

async def _send_biz_panel(biz_id: int, answer_fn):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT biz_id, owner_id, biz_type, name, level, employees, materials, mat_qual, goods, last_prod "
            "FROM businesses WHERE biz_id=?", (biz_id,)
        )
        row = await cursor.fetchone()
    if not row:
        await answer_fn("❌ Предприятие не найдено.")
        return
    bid, owner_id, biz_type, name, level, employees, materials, mat_qual, goods, last_prod = row
    cfg = BIZ_TYPES[biz_type]
    qual = MATERIAL_QUALITY[mat_qual]

    can_produce = False
    cd_str = ""
    if last_prod:
        last_dt = datetime.fromisoformat(last_prod)
        if last_dt.tzinfo is None:
            last_dt = MSK.localize(last_dt)
        next_dt = last_dt + timedelta(hours=cfg["prod_h"])
        now = now_msk()
        if now >= next_dt:
            can_produce = True
        else:
            rem = next_dt - now
            h, r = divmod(int(rem.total_seconds()), 3600)
            cd_str = f"{h}ч {r // 60}м"
    else:
        can_produce = True

    ready = employees >= cfg["min_emp"] and materials >= cfg["mat_cycle"]
    goods_gain = int(cfg["base_out"] * employees * qual["eff"] * BIZ_LEVEL_MULT[level]) if ready else 0
    salary = employees * cfg["salary"] if ready else 0

    lines = [
        f"{cfg['emoji']} <b>{name}</b>   Ур.{level}/5",
        "━━━━━━━━━━━━━━━",
        f"👷 Сотрудников: <b>{employees}</b> (мин. {cfg['min_emp']}, макс. {10 * level})",
        f"📦 Сырьё: <b>{materials} ед.</b> ({qual['label']})",
        f"💼 Товары на складе: <b>{goods} см</b>",
        "━━━━━━━━━━━━━━━",
    ]
    if ready:
        lines += [
            f"📊 1 цикл ({cfg['prod_h']}ч):",
            f"  +{goods_gain} см товаров",
            f"  −{salary} см зарплата",
            f"  −{cfg['mat_cycle']} ед. сырья",
            f"  Налог при продаже: {int(cfg['tax'] * 100)}%",
        ]
        if cd_str:
            lines.append(f"⏳ Кулдаун: {cd_str}")
    else:
        issues = []
        if employees < cfg["min_emp"]:
            issues.append(f"нужно ≥{cfg['min_emp']} сотрудников")
        if materials < cfg["mat_cycle"]:
            issues.append(f"нужно ≥{cfg['mat_cycle']} ед. сырья")
        lines.append("⚠️ Не работает: " + ", ".join(issues))

    btns = []
    if can_produce and ready:
        btns.append([InlineKeyboardButton(text="⚙️ Произвести", callback_data=f"biz_prod_{bid}")])
    btns.append([
        InlineKeyboardButton(text="📦 Купить сырьё", callback_data=f"biz_mat_{bid}"),
        InlineKeyboardButton(text="👷 Нанять", callback_data=f"biz_hire_{bid}"),
    ])
    row2 = []
    if employees > 0:
        row2.append(InlineKeyboardButton(text="👋 Уволить всех", callback_data=f"biz_fire_{bid}"))
    if goods > 0:
        row2.append(InlineKeyboardButton(text="💰 Продать всё", callback_data=f"biz_sell_{bid}"))
    if row2:
        btns.append(row2)
    if level < 5:
        upg = cfg["upg_cost"] * level
        btns.append([InlineKeyboardButton(
            text=f"⬆️ Улучшить до Ур.{level + 1} ({upg} см)",
            callback_data=f"biz_upg_{bid}"
        )])
    btns.append([InlineKeyboardButton(text="◀️ Мои предприятия", callback_data="biz_list")])
    await answer_fn("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


async def _show_biz_list(user_id: int, answer_fn):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT biz_id, biz_type, name, level, goods FROM businesses WHERE owner_id=?",
            (user_id,)
        )
        rows = await cursor.fetchall()
    btns = []
    if rows:
        text = "🏗️ <b>Твои предприятия</b>\n━━━━━━━━━━━━━━━\n"
        for bid, biz_type, name, level, goods in rows:
            cfg = BIZ_TYPES[biz_type]
            text += f"{cfg['emoji']} <b>{name}</b>  Ур.{level}  💼{goods}см\n"
            btns.append([InlineKeyboardButton(
                text=f"{cfg['emoji']} {name}", callback_data=f"biz_view_{bid}"
            )])
    else:
        text = "🏗️ <b>Предприятия</b>\n\nУ тебя пока нет предприятий."
    if len(rows) < 3:
        btns.append([InlineKeyboardButton(text="🏪 Купить предприятие", callback_data="biz_buy_menu")])
    await answer_fn(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@dp.message(F.text == "🏗️ Бизнес", StateFilter(None))
async def btn_biz(message: Message):
    if message.chat.type != "private":
        await message.answer("🏗️ Предприятия доступны только в личных сообщениях.")
        return
    await _show_biz_list(message.from_user.id, message.answer)


@dp.callback_query(F.data == "biz_list")
async def biz_list_cb(callback: CallbackQuery):
    await _show_biz_list(callback.from_user.id, callback.message.answer)
    await callback.answer()


@dp.callback_query(F.data == "biz_buy_menu")
async def biz_buy_menu_cb(callback: CallbackQuery):
    lines = ["🏪 <b>Типы предприятий</b>\n━━━━━━━━━━━━━━━"]
    btns = []
    for key, cfg in BIZ_TYPES.items():
        lines.append(
            f"{cfg['emoji']} <b>{cfg['label']}</b> — {cfg['cost']} см\n"
            f"  мин. {cfg['min_emp']} сотр. | налог {int(cfg['tax'] * 100)}% | цикл {cfg['prod_h']}ч"
        )
        btns.append([InlineKeyboardButton(
            text=f"{cfg['emoji']} {cfg['label']} ({cfg['cost']} см)",
            callback_data=f"biz_pick_{key}"
        )])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="biz_list")])
    await callback.message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await callback.answer()


@dp.callback_query(F.data.startswith("biz_pick_"))
async def biz_pick_type_cb(callback: CallbackQuery, state: FSMContext):
    biz_type = callback.data[9:]
    if biz_type not in BIZ_TYPES:
        await callback.answer("Неверный тип", show_alert=True)
        return
    user_id = callback.from_user.id
    cfg = BIZ_TYPES[biz_type]
    size, _ = await get_user(user_id)
    if size < cfg["cost"]:
        await callback.answer(f"❌ Нужно {cfg['cost']} см, у тебя {size} см", show_alert=True)
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM businesses WHERE owner_id=?", (user_id,))
        cnt = (await cursor.fetchone())[0]
    if cnt >= 3:
        await callback.answer("❌ Максимум 3 предприятия на одного игрока", show_alert=True)
        return
    await state.set_state(BizCreateState.waiting_name)
    await state.update_data(biz_type=biz_type)
    await callback.message.answer(
        f"🏪 Открываешь {cfg['emoji']} <b>{cfg['label']}</b>\n"
        f"Придумай название предприятию (2–40 символов):\n/cancel — отмена"
    )
    await callback.answer()


@dp.message(BizCreateState.waiting_name)
async def biz_name_input(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2 or len(name) > 40:
        await message.answer("❌ Название должно быть от 2 до 40 символов.")
        return
    data = await state.get_data()
    biz_type = data["biz_type"]
    cfg = BIZ_TYPES[biz_type]
    user_id = message.from_user.id
    size, _ = await get_user(user_id)
    if size < cfg["cost"]:
        await state.clear()
        await message.answer(f"❌ Недостаточно средств: нужно {cfg['cost']} см.")
        return
    await update_size(user_id, size - cfg["cost"])
    now_str = now_msk().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "INSERT INTO businesses (owner_id, biz_type, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, biz_type, name, now_str)
        )
        biz_id = cursor.lastrowid
        await db.commit()
    await state.clear()
    await message.answer(
        f"✅ <b>«{name}»</b> открыто!\n"
        f"💸 Потрачено: {cfg['cost']} см  •  Остаток: {size - cfg['cost']} см\n\n"
        f"Теперь найми сотрудников и купи сырьё для запуска производства."
    )
    await _send_biz_panel(biz_id, message.answer)


@dp.callback_query(F.data.startswith("biz_view_"))
async def biz_view_cb(callback: CallbackQuery):
    biz_id = int(callback.data[9:])
    await _send_biz_panel(biz_id, callback.message.answer)
    await callback.answer()


@dp.callback_query(F.data.startswith("biz_prod_"))
async def biz_prod_cb(callback: CallbackQuery):
    biz_id = int(callback.data[9:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT biz_type, name, level, employees, materials, mat_qual, goods, last_prod "
            "FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("❌ Не найдено", show_alert=True)
        return
    biz_type, name, level, employees, materials, mat_qual, goods, last_prod = row
    cfg = BIZ_TYPES[biz_type]
    qual = MATERIAL_QUALITY[mat_qual]
    if last_prod:
        last_dt = datetime.fromisoformat(last_prod)
        if last_dt.tzinfo is None:
            last_dt = MSK.localize(last_dt)
        if now_msk() < last_dt + timedelta(hours=cfg["prod_h"]):
            await callback.answer("⏳ Цикл ещё не завершён!", show_alert=True)
            return
    if employees < cfg["min_emp"]:
        await callback.answer(f"❌ Нужно минимум {cfg['min_emp']} сотрудников", show_alert=True)
        return
    if materials < cfg["mat_cycle"]:
        await callback.answer(f"❌ Нужно минимум {cfg['mat_cycle']} ед. сырья", show_alert=True)
        return
    salary = employees * cfg["salary"]
    size, _ = await get_user(user_id)
    if size < salary:
        await callback.answer(f"❌ Не хватает на зарплату: {salary} см", show_alert=True)
        return
    goods_gain = int(cfg["base_out"] * employees * qual["eff"] * BIZ_LEVEL_MULT[level])
    now_str = now_msk().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE businesses SET materials=materials-?, goods=goods+?, last_prod=? WHERE biz_id=?",
            (cfg["mat_cycle"], goods_gain, now_str, biz_id)
        )
        await db.commit()
    await update_size(user_id, size - salary)
    await callback.message.answer(
        f"⚙️ <b>Цикл производства завершён!</b>\n"
        f"{cfg['emoji']} {name}\n━━━━━━━━━━━━━━━\n"
        f"📦 Произведено: <b>+{goods_gain} см</b>\n"
        f"💸 Зарплата: <b>−{salary} см</b>\n"
        f"📉 Сырьё: <b>−{cfg['mat_cycle']} ед.</b>\n"
        f"💼 На складе: <b>{goods + goods_gain} см</b>"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("biz_mat_"))
async def biz_mat_cb(callback: CallbackQuery):
    biz_id = int(callback.data[8:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT name FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    name = row[0]
    btns = []
    for qual_key, qual in MATERIAL_QUALITY.items():
        btns.append([InlineKeyboardButton(
            text=f"{qual['label']} — {qual['price']} см/ед.  (эфф. ×{qual['eff']})",
            callback_data=f"biz_mq_{biz_id}_{qual_key}"
        )])
    await callback.message.answer(
        f"📦 <b>Сырьё для «{name}»</b>\nВыбери качество:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("biz_mq_"))
async def biz_mat_qual_cb(callback: CallbackQuery, state: FSMContext):
    parts = callback.data[7:].split("_", 1)
    biz_id = int(parts[0])
    qual_key = parts[1]
    if qual_key not in MATERIAL_QUALITY:
        await callback.answer("Неверное качество", show_alert=True)
        return
    user_id = callback.from_user.id
    size, _ = await get_user(user_id)
    qual = MATERIAL_QUALITY[qual_key]
    await state.set_state(BizMatState.waiting_qty)
    await state.update_data(biz_id=biz_id, qual_key=qual_key)
    await callback.message.answer(
        f"📦 {qual['label']} — {qual['price']} см/ед.\n"
        f"💰 Баланс: {size} см\n"
        f"Сколько единиц купить?\n/cancel — отмена"
    )
    await callback.answer()


@dp.message(BizMatState.waiting_qty)
async def biz_mat_qty_input(message: Message, state: FSMContext):
    try:
        qty = int(message.text.strip())
        if qty < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return
    data = await state.get_data()
    biz_id = data["biz_id"]
    qual_key = data["qual_key"]
    qual = MATERIAL_QUALITY[qual_key]
    user_id = message.from_user.id
    total_cost = qty * qual["price"]
    size, _ = await get_user(user_id)
    if size < total_cost:
        await message.answer(f"❌ Нужно {total_cost} см, у тебя {size} см.")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT materials FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id)
        )
        row = await cursor.fetchone()
        if not row:
            await state.clear()
            return
        new_mats = row[0] + qty
        await db.execute(
            "UPDATE businesses SET materials=?, mat_qual=? WHERE biz_id=?",
            (new_mats, qual_key, biz_id)
        )
        await db.commit()
    await update_size(user_id, size - total_cost)
    await state.clear()
    await message.answer(
        f"✅ Куплено <b>{qty} ед.</b> сырья ({qual['label']})\n"
        f"💸 Потрачено: {total_cost} см\n"
        f"📦 На складе: {new_mats} ед."
    )


@dp.callback_query(F.data.startswith("biz_hire_"))
async def biz_hire_cb(callback: CallbackQuery, state: FSMContext):
    biz_id = int(callback.data[9:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT biz_type, name, employees, level FROM businesses WHERE biz_id=? AND owner_id=?",
            (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    biz_type, name, employees, level = row
    cfg = BIZ_TYPES[biz_type]
    max_emp = 10 * level
    recruit_fee = cfg["salary"] * 5
    size, _ = await get_user(user_id)
    await state.set_state(BizHireState.waiting_count)
    await state.update_data(biz_id=biz_id, biz_type=biz_type)
    await callback.message.answer(
        f"👷 <b>Найм в «{name}»</b>\n"
        f"Сотрудников: {employees}/{max_emp}\n"
        f"Единовременный взнос: <b>{recruit_fee} см/чел.</b>\n"
        f"Зарплата за цикл: <b>{cfg['salary']} см/чел.</b>\n"
        f"💰 Баланс: {size} см\n"
        f"Сколько нанять?\n/cancel — отмена"
    )
    await callback.answer()


@dp.message(BizHireState.waiting_count)
async def biz_hire_input(message: Message, state: FSMContext):
    try:
        count = int(message.text.strip())
        if count < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return
    data = await state.get_data()
    biz_id = data["biz_id"]
    biz_type = data["biz_type"]
    cfg = BIZ_TYPES[biz_type]
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT employees, level FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await state.clear()
        return
    employees, level = row
    max_emp = 10 * level
    if employees + count > max_emp:
        await message.answer(f"❌ Максимум {max_emp} сотрудников на уровне {level}.")
        return
    recruit_fee = cfg["salary"] * 5
    total_cost = recruit_fee * count
    size, _ = await get_user(user_id)
    if size < total_cost:
        await message.answer(f"❌ Нужно {total_cost} см, у тебя {size} см.")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE businesses SET employees=employees+? WHERE biz_id=?", (count, biz_id)
        )
        await db.commit()
    await update_size(user_id, size - total_cost)
    await state.clear()
    await message.answer(
        f"✅ Нанято <b>{count}</b> сотрудников!\n"
        f"💸 Потрачено: {total_cost} см\n"
        f"👷 Всего: {employees + count}"
    )


@dp.callback_query(F.data.startswith("biz_fire_"))
async def biz_fire_cb(callback: CallbackQuery):
    biz_id = int(callback.data[9:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT employees, name FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row or row[0] == 0:
        await callback.answer("Нечего увольнять", show_alert=True)
        return
    employees, name = row
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE businesses SET employees=0 WHERE biz_id=?", (biz_id,))
        await db.commit()
    await callback.message.answer(f"👋 Уволено {employees} сотрудников из «{name}».")
    await callback.answer()


@dp.callback_query(F.data.startswith("biz_sell_"))
async def biz_sell_cb(callback: CallbackQuery):
    biz_id = int(callback.data[9:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT biz_type, name, goods FROM businesses WHERE biz_id=? AND owner_id=?",
            (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    biz_type, name, goods = row
    if goods <= 0:
        await callback.answer("Нет товаров для продажи", show_alert=True)
        return
    cfg = BIZ_TYPES[biz_type]
    tax = int(goods * cfg["tax"])
    received = goods - tax
    await add_to_bank(tax)
    size, _ = await get_user(user_id)
    await update_size(user_id, size + received)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE businesses SET goods=0 WHERE biz_id=?", (biz_id,))
        await db.commit()
    await callback.message.answer(
        f"💰 <b>Товары проданы!</b>\n"
        f"{cfg['emoji']} {name}\n━━━━━━━━━━━━━━━\n"
        f"💼 Продано: <b>{goods} см</b>\n"
        f"🏦 Налог ({int(cfg['tax'] * 100)}%): <b>−{tax} см</b>\n"
        f"✅ Получено: <b>+{received} см</b>\n"
        f"🥒 Баланс: <b>{size + received} см</b>"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("biz_upg_"))
async def biz_upg_cb(callback: CallbackQuery):
    biz_id = int(callback.data[8:])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT biz_type, name, level FROM businesses WHERE biz_id=? AND owner_id=?",
            (biz_id, user_id)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    biz_type, name, level = row
    if level >= 5:
        await callback.answer("Максимальный уровень!", show_alert=True)
        return
    cfg = BIZ_TYPES[biz_type]
    upg_cost = cfg["upg_cost"] * level
    size, _ = await get_user(user_id)
    if size < upg_cost:
        await callback.answer(f"❌ Нужно {upg_cost} см, у тебя {size} см", show_alert=True)
        return
    new_level = level + 1
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE businesses SET level=? WHERE biz_id=?", (new_level, biz_id))
        await db.commit()
    await update_size(user_id, size - upg_cost)
    await callback.message.answer(
        f"⬆️ <b>Улучшение успешно!</b>\n"
        f"{cfg['emoji']} {name}  Ур.{level} → Ур.{new_level}\n"
        f"💸 Потрачено: {upg_cost} см\n"
        f"📈 Производительность: ×{BIZ_LEVEL_MULT[new_level]}"
    )
    await callback.answer()


# -------------------- ПЕРЕВОДЫ --------------------

@dp.message(F.text == "💸 Перевод", StateFilter(None))
async def btn_transfer(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("⚠️ Переводы доступны только в личных сообщениях!")
        return
    await state.set_state(TransferState.waiting_target)
    await message.answer(
        "💸 <b>Перевод см</b>\n\n"
        "Введи <b>числовой ID</b> получателя или перешли любое его сообщение:\n\n"
        "💡 Свой ID показывает команда /stats\n\n"
        "/cancel — отменить"
    )


@dp.message(TransferState.waiting_target)
async def transfer_target_input(message: Message, state: FSMContext):
    target_id = None
    target_name = None
    if message.forward_from:
        target_id = message.forward_from.id
        target_name = message.forward_from.full_name
    elif message.text and message.text.strip().lstrip("-").isdigit():
        target_id = int(message.text.strip())
    else:
        await message.answer("❗ Введи числовой ID или перешли сообщение от пользователя")
        return
    if target_id == message.from_user.id:
        await message.answer("❗ Нельзя переводить себе!")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT name FROM users WHERE user_id=?", (target_id,))
        row = await cursor.fetchone()
    if not row:
        await message.answer("❌ Пользователь не найден. Он должен хотя бы раз запустить бота.")
        return
    name = target_name or row[0] or "Игрок"
    await state.update_data(target_id=target_id, target_name=name)
    await state.set_state(TransferState.waiting_amount)
    size, _ = await get_user(message.from_user.id)
    await message.answer(
        f"💸 Получатель: <b>{name}</b> (ID: <code>{target_id}</code>)\n"
        f"У тебя: <b>{size} см</b>\n\n"
        f"Введи сумму (мин. 10 см):\n🏦 Комиссия банка: 5%"
    )


@dp.message(TransferState.waiting_amount)
async def transfer_amount_input(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount < 10:
            raise ValueError
    except ValueError:
        await message.answer("❗ Минимум 10 см")
        return
    user_id = message.from_user.id
    size, _ = await get_user(user_id)
    commission = max(1, int(amount * 0.05))
    total_cost = amount + commission
    if size < total_cost:
        await message.answer(
            f"❌ Недостаточно!\n{amount} + {commission} (комиссия) = {total_cost} см\nУ тебя: {size} см"
        )
        return
    data = await state.get_data()
    await state.update_data(amount=amount, commission=commission)
    await state.set_state(TransferState.waiting_confirm)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data="transfer_confirm"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="transfer_cancel"),
        ]
    ])
    await message.answer(
        f"💸 <b>Подтверди перевод:</b>\n\n"
        f"👤 {data['target_name']}\n"
        f"💰 Сумма: {amount} см\n"
        f"🏦 Комиссия (5%): {commission} см\n"
        f"💸 Итого спишется: {total_cost} см",
        reply_markup=kb
    )


@dp.callback_query(F.data == "transfer_confirm")
async def transfer_confirm_cb(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state != TransferState.waiting_confirm.state:
        await callback.answer("Устаревший запрос", show_alert=True)
        return
    data = await state.get_data()
    user_id = callback.from_user.id
    target_id = data["target_id"]
    amount = data["amount"]
    commission = data["commission"]
    total_cost = amount + commission
    size, _ = await get_user(user_id)
    if size < total_cost:
        await callback.answer("❌ Недостаточно см!", show_alert=True)
        await state.clear()
        return
    await update_size(user_id, size - total_cost)
    target_size, _ = await get_user(target_id)
    await update_size(target_id, target_size + amount)
    await add_to_bank(commission)
    await state.clear()
    await callback.message.edit_text(
        f"✅ <b>Перевод выполнен!</b>\n\n"
        f"💰 Отправлено: {amount} см\n"
        f"🏦 Комиссия: {commission} см\n"
        f"🥒 Твой огурец: {size - total_cost} см"
    )
    try:
        await bot.send_message(
            target_id,
            f"💸 <b>Входящий перевод!</b>\n"
            f"👤 От: {callback.from_user.full_name or 'Игрок'}\n"
            f"💰 Сумма: <b>+{amount} см</b>"
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data == "transfer_cancel")
async def transfer_cancel_cb(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Перевод отменён.")
    await callback.answer()


# -------------------- ПОДДЕРЖКА: ОТВЕТЫ АДМИНА --------------------

@dp.callback_query(F.data.startswith("admin_reply_"))
async def admin_reply_cb(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    target_id = int(callback.data[12:])
    await state.set_state(AdminReplyState.waiting_reply)
    await state.update_data(target_id=target_id)
    await callback.message.answer(
        f"✉️ Введи ответ пользователю {target_id}:\n\n/cancel — отменить"
    )
    await callback.answer()


@dp.message(AdminReplyState.waiting_reply)
async def admin_reply_input(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    target_id = data["target_id"]
    await state.clear()
    try:
        await bot.send_message(
            target_id,
            f"📬 <b>Ответ от поддержки:</b>\n\n{message.text or '[медиа]'}"
        )
        if message.text is None:
            await message.copy_to(target_id)
        await message.answer(f"✅ Ответ отправлен пользователю {target_id}")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}")


@dp.callback_query(F.data.startswith("admin_write_"))
async def admin_write_cb(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    target_id = int(callback.data[12:])
    await state.set_state(AdminReplyState.waiting_reply)
    await state.update_data(target_id=target_id)
    await callback.message.answer(
        f"✉️ Введи сообщение для пользователя {target_id}:\n\n/cancel — отменить"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_"))
async def admin_stats_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    user_id = int(callback.data[12:])
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT size, wins, loses, max_size, loan, name FROM users WHERE user_id=?", (user_id,)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Игрок не найден", show_alert=True)
        return
    size, wins, loses, max_size, loan, name = row
    wins = wins or 0
    loses = loses or 0
    loan = loan or 0
    prices = await get_stock_prices()
    portfolio = await get_portfolio(user_id)
    port_val = int(sum(s * prices.get(t, 0) for t, s in portfolio.items()))
    clan_info = ""
    member = await get_clan_member(user_id)
    if member:
        clan = await get_clan(member[0])
        if clan:
            clan_info = f"\n🛡 Клан: {clan[1]} | {member[1]}"
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount + CAST(amount * rate AS INTEGER)), 0) "
            "FROM deposits WHERE user_id=? AND claimed=0", (user_id,)
        )
        dep_row = await cursor.fetchone()
    dep_count, dep_total = dep_row if dep_row else (0, 0)
    text = (
        f"📊 <b>Игрок:</b> {name or 'Нет имени'} (ID: <code>{user_id}</code>)\n\n"
        f"🥒 Огурец: <b>{size} см</b>\n"
        f"📈 Максимум: {max_size or 0} см\n"
        f"🏆 Победы/поражения: {wins}/{loses}\n"
        f"💳 Долг: {loan} см\n"
        f"📈 Портфель: {port_val} см\n"
        f"💰 Вкладов: {dep_count} (ожидается {dep_total} см)"
        f"{clan_info}"
    )
    await callback.message.answer(text)
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


# -------------------- ВЕБ-СЕРВЕР (встроенный) --------------------

WEBAPP_PORT = int(os.environ.get("WEBAPP_PORT", "8080"))
_WEBAPP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp")
_WEBAPP_CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json(data, status=200):
    import json
    return aio_web.Response(
        text=json.dumps(data, ensure_ascii=False),
        status=status, content_type="application/json",
        headers=_WEBAPP_CORS,
    )


async def _wa_index(request):
    return aio_web.FileResponse(os.path.join(_WEBAPP_DIR, "index.html"))


async def _wa_options(request):
    return aio_web.Response(status=204, headers=_WEBAPP_CORS)


async def _wa_user(request):
    try:
        uid = int(request.match_info["user_id"])
    except Exception:
        return _json({"error": "invalid"}, 400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT size,wins,loses,max_size,name,loan,last_grow FROM users WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            # Создаём пользователя если открыл webapp первым делом
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, size, name) VALUES (?, 0, ?)",
                (uid, "Игрок"))
            await db.commit()
            row = (0, 0, 0, 0, "Игрок", 0, None)
    size, wins, loses, max_size, name, loan, last_grow = row
    cd = 0
    if last_grow:
        try:
            lt = datetime.fromisoformat(last_grow)
            if lt.tzinfo is None:
                lt = MSK.localize(lt)
            cd = max(0, int(86400 - (now_msk() - lt).total_seconds()))
        except Exception:
            pass
    return _json({"user_id": uid, "size": size or 0, "wins": wins or 0,
                  "loses": loses or 0, "max_size": max_size or 0,
                  "name": name or "Игрок", "loan": loan or 0, "cooldown_remaining": cd})


async def _wa_grow(request):
    try:
        uid = int(request.match_info["user_id"])
    except Exception:
        return _json({"error": "invalid"}, 400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size,last_grow,loan FROM users WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            return _json({"error": "not found"}, 404)
        size, last_grow, loan = row
        size = size or 0
        loan = loan or 0
        now = now_msk()
        if last_grow:
            try:
                lt = datetime.fromisoformat(last_grow)
                if lt.tzinfo is None:
                    lt = MSK.localize(lt)
                elapsed = (now - lt).total_seconds()
                if elapsed < 86400:
                    return _json({"error": "cooldown", "cooldown_remaining": int(86400 - elapsed)}, 429)
            except Exception:
                pass
        gain = random.randint(1, 15)
        kept, repaid = gain, 0
        if loan > 0:
            repay = min(loan, gain)
            kept, repaid = gain - repay, repay
            new_loan = loan - repay
            if new_loan > 0:
                await db.execute("UPDATE users SET loan=?,loan_date=? WHERE user_id=?",
                                 (new_loan, now.isoformat(), uid))
            else:
                await db.execute("UPDATE users SET loan=0,loan_date=NULL WHERE user_id=?", (uid,))
        new_size = size + kept
        cur2 = await db.execute("SELECT max_size FROM users WHERE user_id=?", (uid,))
        mr = await cur2.fetchone()
        new_max = max(mr[0] or 0, new_size) if mr else new_size
        await db.execute("UPDATE users SET size=?,max_size=?,last_grow=? WHERE user_id=?",
                         (new_size, new_max, now.isoformat(), uid))
        await db.commit()
    return _json({"success": True, "gain": gain, "kept": kept, "repaid": repaid,
                  "new_size": new_size, "cooldown_remaining": 86400})


async def _wa_stocks(request):
    real_p, prev_r = {}, {}
    async with aiosqlite.connect(DB_NAME) as db:
        for tk in STOCKS:
            cur = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 2", (tk,))
            rows = await cur.fetchall()
            real_p[tk] = rows[0][0] if rows else 0.0
            prev_r[tk] = rows[1][0] if len(rows) >= 2 else real_p[tk]
    result = []
    for tk, nm in STOCKS.items():
        pr = real_p.get(tk, 0.0)
        pv = prev_r.get(tk, pr)
        ch = round((pr - pv) / pv * 100, 2) if pv else 0.0
        result.append({"ticker": tk, "name": nm, "price": pr, "change": ch, "type": "stock"})
    return _json(result)


async def _wa_history(request):
    tk = request.match_info.get("ticker", "").upper()
    if tk not in STOCKS:
        return _json({"error": "unknown"}, 404)
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT price,recorded_at FROM price_history WHERE ticker=? ORDER BY id ASC LIMIT 72", (tk,))
        rows = await cur.fetchall()
    return _json({"ticker": tk, "name": STOCKS.get(tk, tk),
                  "data": [{"price": r[0], "time": r[1]} for r in rows]})


async def _wa_top(request):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT user_id,name,size,wins,loses FROM users ORDER BY size DESC LIMIT 20")
        rows = await cur.fetchall()
    return _json([{"rank": i+1, "user_id": r[0], "name": r[1] or "Игрок",
                   "size": r[2] or 0, "wins": r[3] or 0, "loses": r[4] or 0}
                  for i, r in enumerate(rows)])


async def _wa_portfolio(request):
    try:
        uid = int(request.match_info["user_id"])
    except Exception:
        return _json({"error": "invalid"}, 400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT ticker,shares FROM portfolios WHERE user_id=? AND shares>0", (uid,))
        port = await cur.fetchall()
        rp = {}
        for tk in STOCKS:
            cur3 = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 1", (tk,))
            r = await cur3.fetchone()
            rp[tk] = r[0] if r else 0.0
    result, total = [], 0.0
    for tk, sh in port:
        if tk not in STOCKS:
            continue  # skip old/removed tickers
        pr = rp.get(tk, 0.0)
        val = round(sh * pr, 2)
        total += val
        result.append({"ticker": tk, "name": STOCKS.get(tk, tk),
                       "shares": sh, "price": round(pr, 2), "value": val})
    result.sort(key=lambda x: x["value"], reverse=True)
    return _json({"portfolio": result, "total_value": round(total, 2)})


async def _wa_bank(request):
    async with aiosqlite.connect(DB_NAME) as db:
        cap = (await (await db.execute("SELECT capital FROM bank WHERE id=1")).fetchone() or [0])[0]
        uc = (await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0]
        rc = (await (await db.execute("SELECT COUNT(*) FROM users WHERE size>=5000")).fetchone())[0]
        dt = (await (await db.execute(
            "SELECT COALESCE(SUM(amount),0) FROM deposits WHERE claimed=0")).fetchone())[0]
    return _json({"capital": cap, "users_count": uc, "rich_count": rc,
                  "inflation_rate": round(min(50.0, rc * 3.0), 1), "deposits_total": dt})


_SLOT_SYMS = ["🍒", "🍋", "🍊", "🍇", "💎", "7️⃣", "🥒"]
_SLOT_W = [30, 25, 20, 15, 5, 3, 2]
_SLOT_PAY = {"🥒🥒🥒": 50, "7️⃣7️⃣7️⃣": 25, "💎💎💎": 15,
             "🍇🍇🍇": 6, "🍊🍊🍊": 5, "🍋🍋🍋": 4, "🍒🍒🍒": 3}


async def _wa_slots(request):
    try:
        uid = int(request.match_info["user_id"])
        body = await request.json()
        bet = max(1, int(body.get("bet", 10)))
    except Exception:
        return _json({"error": "invalid"}, 400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size FROM users WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            return _json({"error": "not found"}, 404)
        sz = row[0] or 0
        if sz < bet:
            return _json({"error": "insufficient", "size": sz}, 400)
        reels = random.choices(_SLOT_SYMS, weights=_SLOT_W, k=3)
        key = "".join(reels)
        mult = _SLOT_PAY.get(key, 0)
        if mult > 0:
            net = bet * mult - bet
        elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
            net = 0
        else:
            net = -bet
        new_sz = sz + net
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_sz, uid))
        await db.commit()
    return _json({"reels": reels, "won": net > 0, "push": net == 0,
                  "multiplier": mult, "net": net, "new_size": new_sz})


async def _wa_business(request):
    try:
        uid = int(request.match_info["user_id"])
    except Exception:
        return _json({"error": "invalid"}, 400)
    _biz = {"farm": ("🌾","Ферма"), "factory": ("🏭","Завод"), "mine": ("⛏️","Шахта"),
            "brewery": ("🍺","Пивоварня"), "it": ("💻","IT-компания")}
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cur = await db.execute(
                "SELECT biz_id,biz_type,name,level,employees,materials,mat_qual,goods,last_prod "
                "FROM businesses WHERE owner_id=? ORDER BY biz_id", (uid,))
            rows = await cur.fetchall()
        except Exception:
            rows = []
    result = []
    for r in rows:
        em, lb = _biz.get(r[1], ("🏢", r[1]))
        result.append({"biz_id": r[0], "type": r[1], "emoji": em, "label": lb,
                        "name": r[2], "level": r[3] or 1, "employees": r[4] or 0,
                        "materials": r[5] or 0, "mat_qual": r[6] or "low", "goods": r[7] or 0})
    return _json(result)


async def _wa_clan(request):
    try:
        uid = int(request.match_info["user_id"])
    except Exception:
        return _json({"error": "invalid"}, 400)
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cur = await db.execute("SELECT clan_id,role FROM clan_members WHERE user_id=?", (uid,))
            member = await cur.fetchone()
            if not member:
                return _json({"clan": None, "members": []})
            cid, role = member
            cur2 = await db.execute("SELECT name,owner_id,treasury FROM clans WHERE clan_id=?", (cid,))
            clan = await cur2.fetchone()
            if not clan:
                return _json({"clan": None, "members": []})
            cur3 = await db.execute(
                "SELECT cm.user_id,cm.role,u.name,u.size FROM clan_members cm "
                "LEFT JOIN users u ON cm.user_id=u.user_id WHERE cm.clan_id=? ORDER BY u.size DESC", (cid,))
            members = await cur3.fetchall()
        except Exception:
            return _json({"clan": None, "members": []})
    return _json({"clan": {"id": cid, "name": clan[0], "owner_id": clan[1],
                           "treasury": clan[2] or 0, "my_role": role},
                  "members": [{"user_id": m[0], "role": m[1], "name": m[2] or "Игрок", "size": m[3] or 0}
                               for m in members]})


# ── Нарды: игровая логика ─────────────────────────────────────────────────────

import copy, json as _json_mod

def _bg_new_state():
    board = [0]*24
    board[23]=2; board[12]=5; board[7]=3; board[5]=5
    board[0]=-2; board[11]=-5; board[16]=-3; board[18]=-5
    d1,d2 = random.randint(1,6), random.randint(1,6)
    while d1==d2:
        d1,d2 = random.randint(1,6), random.randint(1,6)
    first = 1 if d1>d2 else 2
    return {"board":board,"bar":[0,0],"off":[0,0],
            "dice":[d1,d2],"moves_left":[d1,d2],"current":first}

def _bg_all_home(st, p):
    b=st["board"]; bar=st["bar"]
    if bar[p-1]>0: return False
    if p==1: return all(v<=0 for v in b[6:])
    return all(v>=0 for v in b[:18])

def _bg_bear_ok(st, p, idx, die):
    b=st["board"]
    if not _bg_all_home(st,p): return False
    if p==1:
        if idx-die<0: return all(b[i]<=0 for i in range(idx+1,6))
    else:
        if idx+die>23: return all(b[i]>=0 for i in range(18,idx))
    return False

def _bg_valid(st):
    board=st["board"]; bar=st["bar"]; ml=st["moves_left"]; p=st["current"]
    ud=list(set(ml)); res=[]
    if bar[p-1]>0:
        for d in ud:
            tgt=(24-d) if p==1 else (d-1)
            if 0<=tgt<=23:
                tv=board[tgt]
                if (p==1 and tv>=-1) or (p==2 and tv<=1):
                    res.append((-1,d))
        return res
    for i in range(24):
        v=board[i]
        if not((p==1 and v>0) or (p==2 and v<0)): continue
        for d in ud:
            if p==1:
                t=i-d
                if t>=0 and board[t]>=-1: res.append((i,d))
                elif t<0 and _bg_bear_ok(st,p,i,d): res.append((i,d))
            else:
                t=i+d
                if t<=23 and board[t]<=1: res.append((i,d))
                elif t>23 and _bg_bear_ok(st,p,i,d): res.append((i,d))
    return list(set(res))

def _bg_move(st, frm, die):
    st=copy.deepcopy(st)
    b=st["board"]; bar=st["bar"]; off=st["off"]; p=st["current"]
    if frm==-1:
        if bar[p-1]<=0: return None
        tgt=(24-die) if p==1 else (die-1)
        if not(0<=tgt<=23): return None
        tv=b[tgt]
        if p==1:
            if tv<-1: return None
            if tv==-1: b[tgt]=1; bar[1]+=1
            else: b[tgt]+=1
        else:
            if tv>1: return None
            if tv==1: b[tgt]=-1; bar[0]+=1
            else: b[tgt]-=1
        bar[p-1]-=1
    else:
        if p==1:
            if b[frm]<=0: return None
            tgt=frm-die
            if tgt<0:
                if not _bg_bear_ok(st,p,frm,die): return None
                b[frm]-=1; off[0]+=1
            else:
                tv=b[tgt]
                if tv<-1: return None
                if tv==-1: b[tgt]=1; bar[1]+=1
                else: b[tgt]+=1
                b[frm]-=1
        else:
            if b[frm]>=0: return None
            tgt=frm+die
            if tgt>23:
                if not _bg_bear_ok(st,p,frm,die): return None
                b[frm]+=1; off[1]+=1
            else:
                tv=b[tgt]
                if tv>1: return None
                if tv==1: b[tgt]=-1; bar[0]+=1
                else: b[tgt]-=1
                b[frm]+=1
    ml=list(st["moves_left"])
    if die in ml: ml.remove(die)
    st["moves_left"]=ml
    if off[0]>=15: st["winner"]=1
    elif off[1]>=15: st["winner"]=2
    if not ml and "winner" not in st:
        np_=3-p
        d1,d2=random.randint(1,6),random.randint(1,6)
        st["dice"]=[d1,d2]
        st["moves_left"]=[d1,d2,d1,d2] if d1==d2 else [d1,d2]
        st["current"]=np_
        if not _bg_valid(st):
            d1,d2=random.randint(1,6),random.randint(1,6)
            st["dice"]=[d1,d2]
            st["moves_left"]=[d1,d2,d1,d2] if d1==d2 else [d1,d2]
            st["current"]=p
    return st

# ── Нарды: API ────────────────────────────────────────────────────────────────

async def _wa_bg_lobby(request):
    try: uid=int(request.match_info["user_id"])
    except: return _json({"error":"invalid"},400)
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            cur=await db.execute("SELECT game_id,player1_id,bet,created_at FROM backgammon_games WHERE status='waiting' ORDER BY game_id DESC LIMIT 20")
            waiting=await cur.fetchall()
            cur2=await db.execute("SELECT game_id,player1_id,player2_id,bet,status FROM backgammon_games WHERE (player1_id=? OR player2_id=?) AND status IN ('waiting','active')",(uid,uid))
            mine=await cur2.fetchone()
        games=[{"game_id":r[0],"player1_id":r[1],"bet":r[2],"created_at":r[3]} for r in waiting]
        return _json({"lobby":games,"my_game":{"game_id":mine[0],"status":mine[3]} if mine else None})
    except Exception as e:
        return _json({"error":"db_error","detail":str(e)},500)

async def _wa_bg_create(request):
    try:
        uid=int(request.match_info["user_id"])
        body=await request.json(); bet=max(0,int(body.get("bet",0)))
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        if bet>0 and (row[0] or 0)<bet: return _json({"error":"insufficient"},400)
        cur2=await db.execute("SELECT game_id FROM backgammon_games WHERE player1_id=? AND status IN ('waiting','active')",(uid,))
        if await cur2.fetchone(): return _json({"error":"already_in_game"},400)
        if bet>0:
            await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(bet,uid))
        cur3=await db.execute("INSERT INTO backgammon_games (player1_id,bet,status,created_at) VALUES (?,?,?,?)",
                              (uid,bet,'waiting',now_msk().isoformat()))
        gid=cur3.lastrowid
        await db.commit()
    return _json({"success":True,"game_id":gid})

async def _wa_bg_join(request):
    try:
        uid=int(request.match_info["user_id"])
        gid=int(request.match_info["game_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT player1_id,player2_id,bet,status FROM backgammon_games WHERE game_id=?",(gid,))
        g=await cur.fetchone()
        if not g: return _json({"error":"not found"},404)
        p1,p2,bet,status=g
        if status!='waiting': return _json({"error":"game_started"},400)
        if p1==uid: return _json({"error":"same_player"},400)
        if bet>0:
            cur2=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
            row=await cur2.fetchone()
            if not row or (row[0] or 0)<bet: return _json({"error":"insufficient"},400)
            await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(bet,uid))
        state=_bg_new_state()
        await db.execute("UPDATE backgammon_games SET player2_id=?,status='active',state_json=? WHERE game_id=?",
                         (uid,_json_mod.dumps(state),gid))
        await db.commit()
    return _json({"success":True,"game_id":gid})

async def _wa_bg_state(request):
    try:
        uid=int(request.match_info["user_id"])
        gid=int(request.match_info["game_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT player1_id,player2_id,bet,status,state_json,winner_id FROM backgammon_games WHERE game_id=?",(gid,))
        g=await cur.fetchone()
        if not g: return _json({"error":"not found"},404)
        p1,p2,bet,status,sj,winner_id=g
        cur2=await db.execute("SELECT name FROM users WHERE user_id=?",(p1,))
        n1r=await cur2.fetchone(); n1=n1r[0] if n1r else "Игрок 1"
        n2="Ожидание..."
        if p2:
            cur3=await db.execute("SELECT name FROM users WHERE user_id=?",(p2,))
            n2r=await cur3.fetchone(); n2=n2r[0] if n2r else "Игрок 2"
    my_player=1 if uid==p1 else (2 if uid==p2 else 0)
    state=_json_mod.loads(sj) if sj else None
    valid=[]
    if state and status=='active' and state.get("current")==my_player:
        valid=_bg_valid(state)
    return _json({"game_id":gid,"player1_id":p1,"player2_id":p2,"player1_name":n1,"player2_name":n2,
                  "bet":bet,"status":status,"winner_id":winner_id,"my_player":my_player,
                  "state":state,"valid_moves":valid})

async def _wa_bg_move(request):
    try:
        uid=int(request.match_info["user_id"])
        gid=int(request.match_info["game_id"])
        body=await request.json()
        frm=int(body.get("from_pt",-1)); die=int(body.get("die",0))
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT player1_id,player2_id,bet,status,state_json FROM backgammon_games WHERE game_id=?",(gid,))
        g=await cur.fetchone()
        if not g: return _json({"error":"not found"},404)
        p1,p2,bet,status,sj=g
        if status!='active': return _json({"error":"not_active"},400)
        my_player=1 if uid==p1 else (2 if uid==p2 else 0)
        if not my_player: return _json({"error":"not_player"},403)
        state=_json_mod.loads(sj)
        if state.get("current")!=my_player: return _json({"error":"not_your_turn"},400)
        new_state=_bg_move(state,frm,die)
        if new_state is None: return _json({"error":"invalid_move"},400)
        winner=new_state.get("winner")
        if winner:
            winner_id=p1 if winner==1 else p2
            loser_id=p2 if winner==1 else p1
            prize=bet*2
            if prize>0:
                await db.execute("UPDATE users SET size=size+? WHERE user_id=?",(prize,winner_id))
            await db.execute("UPDATE backgammon_games SET state_json=?,status='finished',winner_id=? WHERE game_id=?",
                             (_json_mod.dumps(new_state),winner_id,gid))
        else:
            await db.execute("UPDATE backgammon_games SET state_json=? WHERE game_id=?",
                             (_json_mod.dumps(new_state),gid))
        await db.commit()
    valid=[] if winner else _bg_valid(new_state)
    return _json({"success":True,"state":new_state,"valid_moves":valid,
                  "winner":winner,"winner_id":winner_id if winner else None})

async def _wa_bg_resign(request):
    try:
        uid=int(request.match_info["user_id"])
        gid=int(request.match_info["game_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT player1_id,player2_id,bet,status FROM backgammon_games WHERE game_id=?",(gid,))
        g=await cur.fetchone()
        if not g: return _json({"error":"not found"},404)
        p1,p2,bet,status=g
        if status=='waiting':
            if bet>0: await db.execute("UPDATE users SET size=size+? WHERE user_id=?",(bet,uid))
            await db.execute("DELETE FROM backgammon_games WHERE game_id=?",(gid,))
        elif status=='active':
            winner_id=p2 if uid==p1 else p1
            if winner_id and bet>0:
                await db.execute("UPDATE users SET size=size+? WHERE user_id=?",(bet*2,winner_id))
            await db.execute("UPDATE backgammon_games SET status='finished',winner_id=? WHERE game_id=?",(winner_id,gid))
        await db.commit()
    return _json({"success":True})

# ── Бизнес: операции ──────────────────────────────────────────────────────────

async def _wa_biz_types(request):
    result=[{"type":k,"emoji":v["emoji"],"label":v["label"],"cost":v["cost"],
             "min_emp":v["min_emp"],"base_out":v["base_out"],"upg_cost":v["upg_cost"]}
            for k,v in BIZ_TYPES.items()]
    return _json(result)

async def _wa_biz_create(request):
    try:
        uid=int(request.match_info["user_id"])
        body=await request.json(); btype=body.get("biz_type",""); name=str(body.get("name","")).strip()[:32]
    except: return _json({"error":"invalid"},400)
    if btype not in BIZ_TYPES or not name: return _json({"error":"invalid"},400)
    cfg=BIZ_TYPES[btype]; cost=cfg["cost"]
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        row=await cur.fetchone()
        if not row or (row[0] or 0)<cost: return _json({"error":"insufficient","need":cost},400)
        await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(cost,uid))
        await db.execute("INSERT INTO businesses (owner_id,biz_type,name,level,created_at) VALUES (?,?,?,1,?)",
                         (uid,btype,name,now_msk().isoformat()))
        await db.commit()
    return _json({"success":True})

async def _wa_biz_produce(request):
    try:
        uid=int(request.match_info["user_id"]); bid=int(request.match_info["biz_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT biz_type,level,employees,materials,mat_qual,goods,last_prod FROM businesses WHERE biz_id=? AND owner_id=?",(bid,uid))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        btype,level,emp,mat,qual,goods,last_prod=row
        cfg=BIZ_TYPES.get(btype,{}); mn=cfg.get("min_emp",1); ph=cfg.get("prod_h",4)
        if emp<mn: return _json({"error":"need_employees","min":mn},400)
        if mat<cfg.get("mat_cycle",1): return _json({"error":"need_materials"},400)
        now=now_msk()
        if last_prod:
            try:
                lt=datetime.fromisoformat(last_prod)
                if lt.tzinfo is None: lt=MSK.localize(lt)
                if (now-lt).total_seconds()<ph*3600:
                    cd=int(ph*3600-(now-lt).total_seconds())
                    return _json({"error":"cooldown","cooldown_remaining":cd},429)
            except: pass
        eff=MATERIAL_QUALITY.get(qual,{}).get("eff",1.0)
        mult=BIZ_LEVEL_MULT.get(level,1.0)
        gain=int(cfg.get("base_out",10)*eff*mult)
        tax=int(gain*cfg.get("tax",0.1)); net=gain-tax
        salary=cfg.get("salary",0)*emp; paid=min(net,salary); net=max(0,net-paid)
        new_mat=max(0,mat-cfg.get("mat_cycle",1))
        await db.execute("UPDATE businesses SET goods=goods+?,materials=?,last_prod=? WHERE biz_id=?",(net,new_mat,now.isoformat(),bid))
        await db.commit()
    return _json({"success":True,"produced":net,"tax":tax,"salary":paid,"goods":goods+net,"cd_sec":ph*3600})

async def _wa_biz_buy_materials(request):
    try:
        uid=int(request.match_info["user_id"]); bid=int(request.match_info["biz_id"])
        body=await request.json(); qual=body.get("qual","low"); qty=max(1,int(body.get("qty",1)))
    except: return _json({"error":"invalid"},400)
    if qual not in MATERIAL_QUALITY: return _json({"error":"invalid qual"},400)
    price=MATERIAL_QUALITY[qual]["price"]*qty
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        row=await cur.fetchone()
        if not row or (row[0] or 0)<price: return _json({"error":"insufficient","need":price},400)
        cur2=await db.execute("SELECT biz_id FROM businesses WHERE biz_id=? AND owner_id=?",(bid,uid))
        if not await cur2.fetchone(): return _json({"error":"not found"},404)
        await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(price,uid))
        await db.execute("UPDATE businesses SET materials=materials+?,mat_qual=? WHERE biz_id=?",(qty,qual,bid))
        await db.commit()
    return _json({"success":True,"spent":price,"qty":qty})

async def _wa_biz_hire(request):
    try:
        uid=int(request.match_info["user_id"]); bid=int(request.match_info["biz_id"])
        body=await request.json(); cnt=max(1,int(body.get("count",1)))
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT biz_type,employees FROM businesses WHERE biz_id=? AND owner_id=?",(bid,uid))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        btype,emp=row; cost=BIZ_TYPES.get(btype,{}).get("salary",3)*cnt*2
        cur2=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        sz=await cur2.fetchone()
        if not sz or (sz[0] or 0)<cost: return _json({"error":"insufficient","need":cost},400)
        await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(cost,uid))
        await db.execute("UPDATE businesses SET employees=employees+? WHERE biz_id=?",(cnt,bid))
        await db.commit()
    return _json({"success":True,"hired":cnt,"employees":emp+cnt,"spent":cost})

async def _wa_biz_fire(request):
    try:
        uid=int(request.match_info["user_id"]); bid=int(request.match_info["biz_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT employees FROM businesses WHERE biz_id=? AND owner_id=?",(bid,uid))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        if not row[0]: return _json({"error":"no employees"},400)
        await db.execute("UPDATE businesses SET employees=employees-1 WHERE biz_id=?",(bid,))
        await db.commit()
    return _json({"success":True,"employees":row[0]-1})

async def _wa_biz_sell(request):
    try:
        uid=int(request.match_info["user_id"]); bid=int(request.match_info["biz_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT goods FROM businesses WHERE biz_id=? AND owner_id=?",(bid,uid))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        goods=row[0] or 0
        if not goods: return _json({"error":"no goods"},400)
        await db.execute("UPDATE users SET size=size+? WHERE user_id=?",(goods,uid))
        await db.execute("UPDATE businesses SET goods=0 WHERE biz_id=?",(bid,))
        await db.commit()
    return _json({"success":True,"sold":goods})

async def _wa_biz_upgrade(request):
    try:
        uid=int(request.match_info["user_id"]); bid=int(request.match_info["biz_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT biz_type,level FROM businesses WHERE biz_id=? AND owner_id=?",(bid,uid))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        btype,level=row
        if level>=5: return _json({"error":"max_level"},400)
        cost=BIZ_TYPES.get(btype,{}).get("upg_cost",100)*level
        cur2=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        sz=await cur2.fetchone()
        if not sz or (sz[0] or 0)<cost: return _json({"error":"insufficient","need":cost},400)
        await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(cost,uid))
        await db.execute("UPDATE businesses SET level=level+1 WHERE biz_id=?",(bid,))
        await db.commit()
    return _json({"success":True,"new_level":level+1,"spent":cost})

# ── Клан: казна ───────────────────────────────────────────────────────────────

async def _wa_clan_contribute(request):
    try:
        uid=int(request.match_info["user_id"]); body=await request.json(); amt=max(1,int(body.get("amount",0)))
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT clan_id FROM clan_members WHERE user_id=?",(uid,))
        m=await cur.fetchone()
        if not m: return _json({"error":"no clan"},400)
        cid=m[0]
        cur2=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        sz=await cur2.fetchone()
        if not sz or (sz[0] or 0)<amt: return _json({"error":"insufficient"},400)
        await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(amt,uid))
        await db.execute("UPDATE clans SET treasury=COALESCE(treasury,0)+? WHERE clan_id=?",(amt,cid))
        await db.commit()
        cur3=await db.execute("SELECT treasury FROM clans WHERE clan_id=?",(cid,))
        tr=await cur3.fetchone()
    return _json({"success":True,"treasury":tr[0] if tr else 0})

async def _wa_clan_withdraw(request):
    try:
        uid=int(request.match_info["user_id"]); body=await request.json(); amt=max(1,int(body.get("amount",0)))
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT clan_id FROM clan_members WHERE user_id=?",(uid,))
        m=await cur.fetchone()
        if not m: return _json({"error":"no clan"},400)
        cid=m[0]
        cur2=await db.execute("SELECT owner_id,treasury FROM clans WHERE clan_id=?",(cid,))
        cl=await cur2.fetchone()
        if not cl: return _json({"error":"clan error"},400)
        if cl[0]!=uid: return _json({"error":"not owner"},403)
        tr=cl[1] or 0
        if tr<amt: return _json({"error":"insufficient","treasury":tr},400)
        await db.execute("UPDATE clans SET treasury=treasury-? WHERE clan_id=?",(amt,cid))
        await db.execute("UPDATE users SET size=size+? WHERE user_id=?",(amt,uid))
        await db.commit()
    return _json({"success":True,"treasury":tr-amt})

# ── Акции: покупка/продажа ────────────────────────────────────────────────────

async def _wa_stock_buy(request):
    try:
        uid=int(request.match_info["user_id"]); body=await request.json()
        ticker=str(body.get("ticker","")).upper(); qty=float(body.get("qty",1))
    except: return _json({"error":"invalid"},400)
    if ticker not in STOCKS or qty<=0: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 1",(ticker,))
        r=await cur.fetchone()
        if not r or r[0]<=0: return _json({"error":"no price"},400)
        price=r[0]; cost=round(price*qty,2); cost_int=max(1,int(cost))
        cur2=await db.execute("SELECT size FROM users WHERE user_id=?",(uid,))
        sz=await cur2.fetchone()
        if not sz or (sz[0] or 0)<cost_int: return _json({"error":"insufficient","need":cost_int},400)
        await db.execute("UPDATE users SET size=size-? WHERE user_id=?",(cost_int,uid))
        await db.execute("INSERT INTO portfolios (user_id,ticker,shares) VALUES (?,?,?) ON CONFLICT(user_id,ticker) DO UPDATE SET shares=shares+?",(uid,ticker,qty,qty))
        await db.commit()
    return _json({"success":True,"ticker":ticker,"qty":qty,"cost":cost_int})

async def _wa_stock_sell(request):
    try:
        uid=int(request.match_info["user_id"]); body=await request.json()
        ticker=str(body.get("ticker","")).upper(); qty=float(body.get("qty",0))
    except: return _json({"error":"invalid"},400)
    if ticker not in STOCKS or qty<=0: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT shares FROM portfolios WHERE user_id=? AND ticker=?",(uid,ticker))
        r=await cur.fetchone()
        if not r or (r[0] or 0)<qty: return _json({"error":"insufficient_shares"},400)
        cur2=await db.execute("SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 1",(ticker,))
        pr=await cur2.fetchone()
        if not pr or pr[0]<=0: return _json({"error":"no price"},400)
        gross=round(pr[0]*qty,2); commission=int(gross*0.05); earned=max(0,int(gross)-commission)
        new_shares=round(r[0]-qty,6)
        if new_shares<=0:
            await db.execute("DELETE FROM portfolios WHERE user_id=? AND ticker=?",(uid,ticker))
        else:
            await db.execute("UPDATE portfolios SET shares=? WHERE user_id=? AND ticker=?",(new_shares,uid,ticker))
        await db.execute("UPDATE users SET size=size+? WHERE user_id=?",(earned,uid))
        await db.commit()
    return _json({"success":True,"ticker":ticker,"qty":qty,"earned":earned,"commission":commission})

# ── Лутбокс ───────────────────────────────────────────────────────────────────

_BOX_CD = 3600

async def _wa_lootbox_info(request):
    try: uid=int(request.match_info["user_id"])
    except: return _json({"error":"invalid"},400)
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT last_box FROM users WHERE user_id=?",(uid,))
        row=await cur.fetchone()
    if not row: return _json({"cooldown_remaining":0})
    lb=row[0]
    cd=0
    if lb:
        try:
            lt=datetime.fromisoformat(lb)
            if lt.tzinfo is None: lt=MSK.localize(lt)
            cd=max(0,int(_BOX_CD-(now_msk()-lt).total_seconds()))
        except: pass
    return _json({"cooldown_remaining":cd})

async def _wa_lootbox_open(request):
    try: uid=int(request.match_info["user_id"])
    except: return _json({"error":"invalid"},400)
    now=now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cur=await db.execute("SELECT size,last_box,loan FROM users WHERE user_id=?",(uid,))
        row=await cur.fetchone()
        if not row: return _json({"error":"not found"},404)
        size,lb,loan=row; size=size or 0; loan=loan or 0
        if lb:
            try:
                lt=datetime.fromisoformat(lb)
                if lt.tzinfo is None: lt=MSK.localize(lt)
                if (now-lt).total_seconds()<_BOX_CD:
                    cd=int(_BOX_CD-(now-lt).total_seconds())
                    return _json({"error":"cooldown","cooldown_remaining":cd},429)
            except: pass
        roll=random.randint(1,100)
        if roll<=40: reward,rarity=random.randint(1,3),"💩 Мусор"
        elif roll<=70: reward,rarity=random.randint(4,8),"🟢 Обычный"
        elif roll<=90: reward,rarity=random.randint(9,15),"🔵 Редкий"
        elif roll<=99: reward,rarity=random.randint(16,30),"🟣 Эпик"
        else: reward,rarity=random.randint(50,100),"🟡 Легендарный"
        repaid=0
        if loan>0:
            repaid=min(loan,reward); new_loan=loan-repaid
            if new_loan>0:
                await db.execute("UPDATE users SET loan=? WHERE user_id=?",(new_loan,uid))
            else:
                await db.execute("UPDATE users SET loan=0,loan_date=NULL WHERE user_id=?",(uid,))
        new_size=size+reward-repaid
        await db.execute("UPDATE users SET size=?,last_box=? WHERE user_id=?",(new_size,now.isoformat(),uid))
        await db.commit()
    return _json({"success":True,"reward":reward,"rarity":rarity,"repaid":repaid,
                  "new_size":new_size,"cooldown_remaining":_BOX_CD})


async def _run_webapp_safe():
    """Запускает веб-сервер в фоне. Любые ошибки логируются, бот продолжает работать."""
    try:
        await start_webapp()
    except OSError as e:
        print(f"⚠️ Веб-сервер не запустился (порт {WEBAPP_PORT}): {e}")
    except Exception as e:
        print(f"⚠️ Веб-сервер ошибка: {e}")


async def start_webapp():
    """Запускает встроенный веб-сервер. Бот работает независимо от этой функции."""
    app = aio_web.Application()
    app.router.add_get("/", _wa_index)
    app.router.add_get("/api/user/{user_id}", _wa_user)
    app.router.add_post("/api/grow/{user_id}", _wa_grow)
    app.router.add_get("/api/stocks", _wa_stocks)
    app.router.add_get("/api/history/{ticker}", _wa_history)
    app.router.add_get("/api/top", _wa_top)
    app.router.add_get("/api/portfolio/{user_id}", _wa_portfolio)
    app.router.add_get("/api/bank", _wa_bank)
    app.router.add_post("/api/slots/{user_id}", _wa_slots)
    app.router.add_get("/api/business/types", _wa_biz_types)
    app.router.add_get("/api/business/{user_id}", _wa_business)
    app.router.add_post("/api/business/{user_id}/create", _wa_biz_create)
    app.router.add_get("/api/clan/{user_id}", _wa_clan)
    app.router.add_post("/api/business/{user_id}/{biz_id}/produce", _wa_biz_produce)
    app.router.add_post("/api/business/{user_id}/{biz_id}/buy-materials", _wa_biz_buy_materials)
    app.router.add_post("/api/business/{user_id}/{biz_id}/hire", _wa_biz_hire)
    app.router.add_post("/api/business/{user_id}/{biz_id}/fire", _wa_biz_fire)
    app.router.add_post("/api/business/{user_id}/{biz_id}/sell", _wa_biz_sell)
    app.router.add_post("/api/business/{user_id}/{biz_id}/upgrade", _wa_biz_upgrade)
    app.router.add_post("/api/clan/{user_id}/contribute", _wa_clan_contribute)
    app.router.add_post("/api/clan/{user_id}/withdraw", _wa_clan_withdraw)
    app.router.add_post("/api/stocks/buy/{user_id}", _wa_stock_buy)
    app.router.add_post("/api/stocks/sell/{user_id}", _wa_stock_sell)
    app.router.add_get("/api/lootbox/{user_id}", _wa_lootbox_info)
    app.router.add_post("/api/lootbox/{user_id}", _wa_lootbox_open)
    app.router.add_get("/api/backgammon/lobby/{user_id}", _wa_bg_lobby)
    app.router.add_post("/api/backgammon/create/{user_id}", _wa_bg_create)
    app.router.add_post("/api/backgammon/join/{user_id}/{game_id}", _wa_bg_join)
    app.router.add_get("/api/backgammon/state/{game_id}/{user_id}", _wa_bg_state)
    app.router.add_post("/api/backgammon/move/{game_id}/{user_id}", _wa_bg_move)
    app.router.add_post("/api/backgammon/resign/{game_id}/{user_id}", _wa_bg_resign)
    app.router.add_route("OPTIONS", "/{path_info:.*}", _wa_options)
    runner = aio_web.AppRunner(app)
    await runner.setup()
    site = aio_web.TCPSite(runner, "0.0.0.0", WEBAPP_PORT)
    await site.start()
    print(f"✅ Веб-сервер запущен: http://0.0.0.0:{WEBAPP_PORT}")


# -------------------- ЗАПУСК --------------------

async def main():
    global BOT_USERNAME
    await init_db()
    await set_commands(bot)
    bot_info = await bot.get_me()
    BOT_USERNAME = bot_info.username
    asyncio.create_task(luxury_tax_loop())
    asyncio.create_task(update_real_stock_prices_loop())
    asyncio.create_task(_run_webapp_safe())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
