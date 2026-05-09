import asyncio
import json
import os
import random
from datetime import datetime, timedelta

import aiosqlite
import pytz
from aiohttp import web

MSK = pytz.timezone("Europe/Moscow")
DB_NAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cucumbers.db")
PORT = 8080
GROW_COOLDOWN = 86400  # 24h in seconds

STOCKS = {
    "AAPL": "🍎 Apple", "TSLA": "⚡ Tesla", "NVDA": "🎮 NVIDIA",
    "AMZN": "📦 Amazon", "GOOGL": "🔍 Google",
}
VOLATILE_STOCKS = {
    "PEPE": "🐸 PepeToken", "DOGE": "🐕 DogeCoin", "SHIB": "💀 ShibaInu",
    "MOON": "🌙 MoonCoin", "PUMP": "🚀 PumpToken", "CUKE": "🥒 CukeCoin",
    "MEME": "😂 MemeCoin", "CHAD": "💪 ChadCoin", "REKT": "💸 RektCoin",
    "BONK": "🔨 BonkToken",
}
VOLATILE_INITIAL_PRICES = {
    "PEPE": 50.0, "DOGE": 120.0, "SHIB": 30.0, "MOON": 80.0, "PUMP": 200.0,
    "CUKE": 100.0, "MEME": 60.0, "CHAD": 150.0, "REKT": 40.0, "BONK": 75.0,
}
ALL_STOCKS = {**STOCKS, **VOLATILE_STOCKS}

BIZ_TYPES = {
    "farm":    {"emoji": "🌾", "label": "Ферма",       "cost": 300_000,   "min_emp": 2, "base_out": 10_000, "mat_cycle": 15, "salary": 2_500, "tax": 0.10, "upg_cost": 150_000, "prod_h": 4},
    "factory": {"emoji": "🏭", "label": "Завод",       "cost": 800_000,   "min_emp": 3, "base_out": 15_000, "mat_cycle": 20, "salary": 4_000, "tax": 0.15, "upg_cost": 300_000, "prod_h": 4},
    "mine":    {"emoji": "⛏️",  "label": "Шахта",       "cost": 600_000,   "min_emp": 4, "base_out": 13_000, "mat_cycle": 30, "salary": 5_500, "tax": 0.18, "upg_cost": 250_000, "prod_h": 4},
    "brewery": {"emoji": "🍺", "label": "Пивоварня",   "cost": 500_000,   "min_emp": 2, "base_out": 12_000, "mat_cycle": 25, "salary": 3_500, "tax": 0.12, "upg_cost": 200_000, "prod_h": 4},
    "it":      {"emoji": "💻", "label": "IT-компания", "cost": 1_200_000, "min_emp": 1, "base_out": 25_000, "mat_cycle": 5,  "salary": 8_000, "tax": 0.20, "upg_cost": 500_000, "prod_h": 4},
}
MATERIAL_QUALITY = {
    "low":    {"label": "🟤 Эконом",   "price": 300,  "eff": 0.7},
    "medium": {"label": "🔵 Стандарт", "price": 900,  "eff": 1.2},
    "high":   {"label": "💎 Премиум",  "price": 2000, "eff": 2.0},
}
BIZ_LEVEL_MULT = {1: 1.0, 2: 1.5, 3: 2.0, 4: 2.8, 5: 4.0}
DEPOSIT_RATES = {1: 0.05, 2: 0.12, 3: 0.20}

SLOT_SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "💎", "7️⃣", "🥒"]
SLOT_WEIGHTS = [30, 25, 20, 15, 5, 3, 2]
SLOT_PAYOUTS = {"🥒🥒🥒": 50, "7️⃣7️⃣7️⃣": 25, "💎💎💎": 15,
                "🍇🍇🍇": 6, "🍊🍊🍊": 5, "🍋🍋🍋": 4, "🍒🍒🍒": 3}

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def json_response(data, status=200):
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        status=status, content_type="application/json", headers=CORS,
    )


async def options_handler(request):
    return web.Response(status=204, headers=CORS)


def now_msk():
    return datetime.now(MSK)


# ── INIT DB ──────────────────────────────────────────────────────────────────

async def init_db():
    os.makedirs(os.path.dirname(DB_NAME), exist_ok=True)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, size INTEGER DEFAULT 0,
            last_grow TEXT, wins INTEGER DEFAULT 0, loses INTEGER DEFAULT 0,
            max_size INTEGER DEFAULT 0, name TEXT, loan INTEGER DEFAULT 0,
            loan_date TEXT, last_box TEXT, last_tax TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS portfolios (
            user_id INTEGER, ticker TEXT, shares REAL DEFAULT 0,
            PRIMARY KEY (user_id, ticker))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS bank (
            id INTEGER PRIMARY KEY, capital INTEGER DEFAULT 0)""")
        await db.execute("INSERT OR IGNORE INTO bank (id, capital) VALUES (1, 0)")
        await db.execute("""CREATE TABLE IF NOT EXISTS volatile_stocks (
            ticker TEXT PRIMARY KEY, price REAL NOT NULL)""")
        for ticker, price in VOLATILE_INITIAL_PRICES.items():
            await db.execute("INSERT OR IGNORE INTO volatile_stocks (ticker, price) VALUES (?, ?)", (ticker, price))
        await db.execute("""CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
            price REAL NOT NULL, recorded_at TEXT NOT NULL)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS deposits (
            deposit_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL, rate REAL NOT NULL, days INTEGER NOT NULL,
            created_at TEXT NOT NULL, matures_at TEXT NOT NULL, claimed INTEGER DEFAULT 0)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS clans (
            clan_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
            owner_id INTEGER NOT NULL, logo_file_id TEXT, created_at TEXT,
            treasury INTEGER DEFAULT 0)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS clan_members (
            user_id INTEGER PRIMARY KEY, clan_id INTEGER NOT NULL,
            role TEXT DEFAULT 'Участник', joined_at TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS crypto_bets (
            bet_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            ticker TEXT NOT NULL, amount INTEGER NOT NULL, cycle INTEGER NOT NULL,
            settled INTEGER DEFAULT 0, won INTEGER DEFAULT 0)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS businesses (
            biz_id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL,
            biz_type TEXT NOT NULL, name TEXT NOT NULL, level INTEGER DEFAULT 1,
            employees INTEGER DEFAULT 0, materials INTEGER DEFAULT 0,
            mat_qual TEXT DEFAULT 'low', goods INTEGER DEFAULT 0,
            last_prod TEXT, created_at TEXT NOT NULL)""")
        try:
            await db.execute("ALTER TABLE clans ADD COLUMN treasury INTEGER DEFAULT 0")
        except Exception:
            pass
        await db.commit()
    print(f"✅ БД готова: {DB_NAME}")


# ── HELPERS ───────────────────────────────────────────────────────────────────

async def _add_to_bank(db, amount: int):
    if amount > 0:
        await db.execute("UPDATE bank SET capital = capital + ? WHERE id = 1", (amount,))


async def _get_user_size(db, user_id: int):
    cur = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
    row = await cur.fetchone()
    return row[0] if row and row[0] else 0


# ── USER ──────────────────────────────────────────────────────────────────────

async def api_user(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT size, wins, loses, max_size, name, loan, last_grow FROM users WHERE user_id=?",
            (user_id,))
        row = await cur.fetchone()

    if not row:
        return json_response({"error": "not found"}, 404)

    size, wins, loses, max_size, name, loan, last_grow = row
    cooldown_remaining = 0
    if last_grow:
        try:
            last_time = datetime.fromisoformat(last_grow)
            if last_time.tzinfo is None:
                last_time = MSK.localize(last_time)
            elapsed = (now_msk() - last_time).total_seconds()
            cooldown_remaining = max(0, int(GROW_COOLDOWN - elapsed))
        except Exception:
            pass

    return json_response({
        "user_id": user_id, "size": size or 0, "wins": wins or 0,
        "loses": loses or 0, "max_size": max_size or 0,
        "name": name or "Игрок", "loan": loan or 0,
        "cooldown_remaining": cooldown_remaining,
    })


# ── GROW ──────────────────────────────────────────────────────────────────────

async def api_grow(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size, last_grow, loan FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)

        size, last_grow, loan = row
        size = size or 0
        loan = loan or 0
        now = now_msk()

        if last_grow:
            try:
                last_time = datetime.fromisoformat(last_grow)
                if last_time.tzinfo is None:
                    last_time = MSK.localize(last_time)
                elapsed = (now - last_time).total_seconds()
                if elapsed < GROW_COOLDOWN:
                    return json_response({"error": "cooldown", "cooldown_remaining": int(GROW_COOLDOWN - elapsed)}, 429)
            except Exception:
                pass

        gain = random.randint(1, 50)
        tax = int(gain * 0.20)
        kept = gain - tax
        repaid = 0
        if loan > 0:
            repay = min(loan, kept)
            kept -= repay
            repaid = repay
            new_loan = loan - repay
            if new_loan > 0:
                await db.execute("UPDATE users SET loan=?, loan_date=? WHERE user_id=?",
                                 (new_loan, now.isoformat(), user_id))
            else:
                await db.execute("UPDATE users SET loan=0, loan_date=NULL WHERE user_id=?", (user_id,))

        new_size = size + kept
        cur2 = await db.execute("SELECT max_size FROM users WHERE user_id=?", (user_id,))
        max_row = await cur2.fetchone()
        new_max = max(max_row[0] or 0, new_size) if max_row else new_size
        await db.execute("UPDATE users SET size=?, max_size=?, last_grow=? WHERE user_id=?",
                         (new_size, new_max, now.isoformat(), user_id))
        await _add_to_bank(db, tax)
        await db.commit()

    return json_response({
        "success": True, "gain": gain, "tax": tax, "kept": kept, "repaid": repaid,
        "new_size": new_size, "cooldown_remaining": GROW_COOLDOWN,
    })


# ── STOCKS ────────────────────────────────────────────────────────────────────

async def api_stocks(request):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT ticker, price FROM volatile_stocks")
        volatile = {t: p for t, p in await cur.fetchall()}
        if not volatile:
            volatile = dict(VOLATILE_INITIAL_PRICES)

        prev_volatile = {}
        for ticker in VOLATILE_STOCKS:
            cur2 = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 2", (ticker,))
            rows = await cur2.fetchall()
            prev_volatile[ticker] = rows[1][0] if len(rows) >= 2 else volatile.get(ticker, 0.0)

        real_prices, prev_real = {}, {}
        for ticker in STOCKS:
            cur3 = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 2", (ticker,))
            rows = await cur3.fetchall()
            real_prices[ticker] = rows[0][0] if rows else 0.0
            prev_real[ticker] = rows[1][0] if len(rows) >= 2 else real_prices[ticker]

    result = []
    for ticker, name in STOCKS.items():
        price = real_prices.get(ticker, 0.0)
        prev = prev_real.get(ticker, price)
        change = round((price - prev) / prev * 100, 2) if prev else 0.0
        result.append({"ticker": ticker, "name": name, "price": price, "change": change, "type": "stock"})

    for ticker, name in VOLATILE_STOCKS.items():
        price = volatile.get(ticker, VOLATILE_INITIAL_PRICES.get(ticker, 0.0))
        prev = prev_volatile.get(ticker, price)
        change = round((price - prev) / prev * 100, 2) if prev else 0.0
        result.append({"ticker": ticker, "name": name, "price": round(price, 2), "change": change, "type": "crypto"})

    return json_response(result)


# ── HISTORY ───────────────────────────────────────────────────────────────────

async def api_history(request):
    ticker = request.match_info.get("ticker", "").upper()
    if ticker not in ALL_STOCKS:
        return json_response({"error": "unknown ticker"}, 404)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT price, recorded_at FROM price_history WHERE ticker=? ORDER BY id ASC LIMIT 72", (ticker,))
        rows = await cur.fetchall()
        if not rows and ticker in VOLATILE_STOCKS:
            cur2 = await db.execute("SELECT price FROM volatile_stocks WHERE ticker=?", (ticker,))
            r = await cur2.fetchone()
            if r:
                rows = [(r[0], now_msk().isoformat())]

    return json_response({"ticker": ticker, "name": ALL_STOCKS.get(ticker, ticker),
                          "data": [{"price": r[0], "time": r[1]} for r in rows]})


# ── TOP ───────────────────────────────────────────────────────────────────────

async def api_top(request):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT user_id, name, size, wins, loses FROM users ORDER BY size DESC LIMIT 20")
        rows = await cur.fetchall()
    return json_response([
        {"rank": i + 1, "user_id": r[0], "name": r[1] or "Игрок",
         "size": r[2] or 0, "wins": r[3] or 0, "loses": r[4] or 0}
        for i, r in enumerate(rows)
    ])


# ── PORTFOLIO ─────────────────────────────────────────────────────────────────

async def api_portfolio(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT ticker, shares FROM portfolios WHERE user_id=? AND shares > 0", (user_id,))
        portfolio_rows = await cur.fetchall()
        cur2 = await db.execute("SELECT ticker, price FROM volatile_stocks")
        volatile = {t: p for t, p in await cur2.fetchall()}
        real_prices = {}
        for ticker in STOCKS:
            cur3 = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 1", (ticker,))
            r = await cur3.fetchone()
            real_prices[ticker] = r[0] if r else 0.0

    result, total = [], 0.0
    for ticker, shares in portfolio_rows:
        price = volatile.get(ticker, real_prices.get(ticker, 0.0))
        value = round(shares * price, 2)
        total += value
        result.append({"ticker": ticker, "name": ALL_STOCKS.get(ticker, ticker),
                       "shares": shares, "price": round(price, 2), "value": value})
    result.sort(key=lambda x: x["value"], reverse=True)
    return json_response({"portfolio": result, "total_value": round(total, 2)})


# ── BANK (public) ─────────────────────────────────────────────────────────────

async def api_bank(request):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT capital FROM bank WHERE id=1")
        row = await cur.fetchone()
        capital = row[0] if row else 0
        cur2 = await db.execute("SELECT COUNT(*) FROM users")
        users_count = (await cur2.fetchone())[0]
        cur3 = await db.execute("SELECT COUNT(*) FROM users WHERE size >= 5000")
        rich_count = (await cur3.fetchone())[0]
        cur4 = await db.execute("SELECT COALESCE(SUM(amount),0) FROM deposits WHERE claimed=0")
        deposits_total = (await cur4.fetchone())[0]

    inflation = min(50.0, rich_count * 3.0)
    return json_response({
        "capital": capital, "users_count": users_count,
        "rich_count": rich_count, "inflation_rate": round(inflation, 1),
        "deposits_total": deposits_total,
    })


# ── BANK (user) ───────────────────────────────────────────────────────────────

async def api_bank_user(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size, loan, loan_date FROM users WHERE user_id=?", (user_id,))
        urow = await cur.fetchone()
        if not urow:
            return json_response({"error": "not found"}, 404)
        size, loan, loan_date = urow
        size = size or 0
        loan = loan or 0

        cur2 = await db.execute("SELECT capital FROM bank WHERE id=1")
        capital = (await cur2.fetchone())[0] or 0

        cur3 = await db.execute(
            "SELECT deposit_id, amount, rate, days, created_at, matures_at FROM deposits "
            "WHERE user_id=? AND claimed=0 ORDER BY deposit_id", (user_id,))
        dep_rows = await cur3.fetchall()

    deposits = []
    for dep_id, amount, rate, days, created_at, matures_at in dep_rows:
        try:
            mat_dt = datetime.fromisoformat(matures_at)
            if mat_dt.tzinfo is None:
                mat_dt = MSK.localize(mat_dt)
            is_mature = now >= mat_dt
            remaining_sec = max(0, int((mat_dt - now).total_seconds())) if not is_mature else 0
        except Exception:
            is_mature = False
            remaining_sec = 0
        payout = int(amount * (1 + rate))
        deposits.append({
            "deposit_id": dep_id, "amount": amount, "rate": round(rate * 100),
            "days": days, "matures_at": matures_at,
            "is_mature": is_mature, "payout": payout,
            "remaining_sec": remaining_sec,
        })

    return json_response({
        "size": size, "loan": loan,
        "max_loan": min(capital, 100000),
        "capital": capital,
        "deposits": deposits,
    })


async def api_bank_loan(request):
    try:
        user_id = int(request.match_info["user_id"])
        body = await request.json()
        amount = int(body.get("amount", 0))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if amount <= 0:
        return json_response({"error": "amount must be > 0"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size, loan FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        size, existing_loan = row
        if existing_loan and existing_loan > 0:
            return json_response({"error": "already_has_loan"}, 400)
        cur2 = await db.execute("SELECT capital FROM bank WHERE id=1")
        capital = (await cur2.fetchone())[0] or 0
        max_loan = min(capital, 100000)
        if amount > max_loan:
            return json_response({"error": "exceeds_limit", "max_loan": max_loan}, 400)
        if capital < amount:
            return json_response({"error": "insufficient_bank"}, 400)

        now = now_msk()
        new_size = (size or 0) + amount
        await db.execute("UPDATE users SET size=?, loan=?, loan_date=? WHERE user_id=?",
                         (new_size, amount, now.isoformat(), user_id))
        await db.execute("UPDATE bank SET capital = capital - ? WHERE id = 1", (amount,))
        await db.commit()

    return json_response({"success": True, "loan": amount, "new_size": new_size})


async def api_bank_repay(request):
    try:
        user_id = int(request.match_info["user_id"])
        body = await request.json()
        amount = int(body.get("amount", 0))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if amount <= 0:
        return json_response({"error": "amount must be > 0"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size, loan FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        size, loan = row
        size = size or 0
        loan = loan or 0
        if loan <= 0:
            return json_response({"error": "no_loan"}, 400)
        repay = min(amount, loan, size)
        if repay <= 0:
            return json_response({"error": "insufficient"}, 400)
        new_loan = loan - repay
        new_size = size - repay
        if new_loan > 0:
            await db.execute("UPDATE users SET size=?, loan=? WHERE user_id=?", (new_size, new_loan, user_id))
        else:
            await db.execute("UPDATE users SET size=?, loan=0, loan_date=NULL WHERE user_id=?", (new_size, user_id))
        await _add_to_bank(db, repay)
        await db.commit()

    return json_response({"success": True, "repaid": repay, "loan_left": new_loan, "new_size": new_size})


async def api_bank_deposit_create(request):
    try:
        user_id = int(request.match_info["user_id"])
        body = await request.json()
        amount = int(body.get("amount", 0))
        days = int(body.get("days", 1))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if amount <= 0:
        return json_response({"error": "amount must be > 0"}, 400)
    if days not in DEPOSIT_RATES:
        return json_response({"error": "days must be 1, 2 or 3"}, 400)

    rate = DEPOSIT_RATES[days]
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        size = row[0] or 0
        if size < amount:
            return json_response({"error": "insufficient", "size": size}, 400)

        now = now_msk()
        matures_at = now + timedelta(days=days)
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (size - amount, user_id))
        await db.execute(
            "INSERT INTO deposits (user_id, amount, rate, days, created_at, matures_at) VALUES (?,?,?,?,?,?)",
            (user_id, amount, rate, days, now.isoformat(), matures_at.isoformat()))
        await _add_to_bank(db, amount)
        await db.commit()

    return json_response({
        "success": True, "amount": amount, "rate_pct": round(rate * 100),
        "days": days, "payout": int(amount * (1 + rate)),
        "matures_at": matures_at.isoformat(),
    })


async def api_bank_deposit_claim(request):
    try:
        user_id = int(request.match_info["user_id"])
        deposit_id = int(request.match_info["deposit_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid input"}, 400)

    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT amount, rate, matures_at, claimed FROM deposits WHERE deposit_id=? AND user_id=?",
            (deposit_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        amount, rate, matures_at, claimed = row
        if claimed:
            return json_response({"error": "already_claimed"}, 400)
        try:
            mat_dt = datetime.fromisoformat(matures_at)
            if mat_dt.tzinfo is None:
                mat_dt = MSK.localize(mat_dt)
        except Exception:
            return json_response({"error": "invalid date"}, 500)
        if now < mat_dt:
            return json_response({"error": "not_mature", "remaining_sec": int((mat_dt - now).total_seconds())}, 400)

        payout = int(amount * (1 + rate))
        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        urow = await cur2.fetchone()
        new_size = (urow[0] or 0) + payout
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute("UPDATE deposits SET claimed=1 WHERE deposit_id=?", (deposit_id,))
        await db.commit()

    return json_response({"success": True, "payout": payout, "new_size": new_size})


# ── SLOTS ─────────────────────────────────────────────────────────────────────

async def api_slots(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)
    try:
        body = await request.json()
        bet = max(1, int(body.get("bet", 10)))
    except Exception:
        return json_response({"error": "invalid body"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        if (row[0] or 0) < bet:
            return json_response({"error": "insufficient", "size": row[0] or 0}, 400)

        reels = random.choices(SLOT_SYMBOLS, weights=SLOT_WEIGHTS, k=3)
        key = "".join(reels)
        multiplier = SLOT_PAYOUTS.get(key, 0)
        if multiplier > 0:
            net = bet * multiplier - bet
        elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
            net = 0
        else:
            net = -bet

        new_size = (row[0] or 0) + net
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        if net < 0:
            await _add_to_bank(db, abs(net))
        await db.commit()

    return json_response({"reels": reels, "won": net > 0, "push": net == 0,
                          "multiplier": multiplier, "net": net, "new_size": new_size})


# ── BUSINESS ─────────────────────────────────────────────────────────────────

async def api_business(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cur = await db.execute(
                "SELECT biz_id, biz_type, name, level, employees, materials, mat_qual, goods, last_prod "
                "FROM businesses WHERE owner_id=? ORDER BY biz_id", (user_id,))
            rows = await cur.fetchall()
        except Exception:
            rows = []

    result = []
    for row in rows:
        biz_id, biz_type, name, level, employees, materials, mat_qual, goods, last_prod = row
        cfg = BIZ_TYPES.get(biz_type, {"emoji": "🏢", "label": biz_type,
                                        "min_emp": 1, "mat_cycle": 10, "salary": 100,
                                        "base_out": 1000, "tax": 0.15, "upg_cost": 100000, "prod_h": 4})
        qual = MATERIAL_QUALITY.get(mat_qual or "low", MATERIAL_QUALITY["low"])
        lv = level or 1
        emp = employees or 0
        mats = materials or 0

        can_produce = False
        cd_sec = 0
        if last_prod:
            try:
                last_dt = datetime.fromisoformat(last_prod)
                if last_dt.tzinfo is None:
                    last_dt = MSK.localize(last_dt)
                next_dt = last_dt + timedelta(hours=cfg["prod_h"])
                if now >= next_dt:
                    can_produce = True
                else:
                    cd_sec = int((next_dt - now).total_seconds())
            except Exception:
                can_produce = True
        else:
            can_produce = True

        ready = emp >= cfg["min_emp"] and mats >= cfg["mat_cycle"]
        goods_gain = int(cfg["base_out"] * emp * qual["eff"] * BIZ_LEVEL_MULT.get(lv, 1.0)) if ready else 0

        result.append({
            "biz_id": biz_id, "type": biz_type,
            "emoji": cfg["emoji"], "label": cfg["label"],
            "name": name, "level": lv,
            "employees": emp, "materials": mats,
            "mat_qual": mat_qual or "low", "mat_label": qual["label"],
            "goods": goods or 0, "last_prod": last_prod,
            "can_produce": can_produce and ready,
            "cd_sec": cd_sec, "ready": ready,
            "goods_gain": goods_gain,
            "salary": emp * cfg["salary"],
            "min_emp": cfg["min_emp"],
            "mat_cycle": cfg["mat_cycle"],
            "tax_pct": int(cfg["tax"] * 100),
            "max_emp": lv * 10,
            "prod_h": cfg["prod_h"],
            "upg_cost": cfg["upg_cost"] * lv if lv < 5 else None,
            "upg_mult": BIZ_LEVEL_MULT.get(lv + 1) if lv < 5 else None,
        })
    return json_response(result)


async def api_biz_types(request):
    result = []
    for key, cfg in BIZ_TYPES.items():
        result.append({
            "type": key, "emoji": cfg["emoji"], "label": cfg["label"],
            "cost": cfg["cost"], "min_emp": cfg["min_emp"],
            "tax_pct": int(cfg["tax"] * 100), "prod_h": cfg["prod_h"],
            "base_out": cfg["base_out"],
        })
    return json_response(result)


async def api_biz_create(request):
    try:
        user_id = int(request.match_info["user_id"])
        body = await request.json()
        biz_type = str(body.get("biz_type", ""))
        name = str(body.get("name", "")).strip()
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if biz_type not in BIZ_TYPES:
        return json_response({"error": "unknown type"}, 400)
    if len(name) < 2 or len(name) > 40:
        return json_response({"error": "name 2–40 chars"}, 400)

    cfg = BIZ_TYPES[biz_type]
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        size = row[0] or 0
        if size < cfg["cost"]:
            return json_response({"error": "insufficient", "need": cfg["cost"], "size": size}, 400)
        cur2 = await db.execute("SELECT COUNT(*) FROM businesses WHERE owner_id=?", (user_id,))
        cnt = (await cur2.fetchone())[0]
        if cnt >= 3:
            return json_response({"error": "max_businesses"}, 400)
        now_str = now_msk().isoformat()
        cur3 = await db.execute(
            "INSERT INTO businesses (owner_id, biz_type, name, created_at) VALUES (?,?,?,?)",
            (user_id, biz_type, name, now_str))
        biz_id = cur3.lastrowid
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (size - cfg["cost"], user_id))
        await db.commit()

    return json_response({"success": True, "biz_id": biz_id, "new_size": size - cfg["cost"]})


async def api_biz_produce(request):
    try:
        user_id = int(request.match_info["user_id"])
        biz_id = int(request.match_info["biz_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid input"}, 400)

    now = now_msk()
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT biz_type, level, employees, materials, mat_qual, goods, last_prod "
            "FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        biz_type, level, employees, materials, mat_qual, goods, last_prod = row
        cfg = BIZ_TYPES.get(biz_type)
        if not cfg:
            return json_response({"error": "unknown type"}, 400)

        if last_prod:
            try:
                last_dt = datetime.fromisoformat(last_prod)
                if last_dt.tzinfo is None:
                    last_dt = MSK.localize(last_dt)
                if now < last_dt + timedelta(hours=cfg["prod_h"]):
                    cd = int((last_dt + timedelta(hours=cfg["prod_h"]) - now).total_seconds())
                    return json_response({"error": "cooldown", "cd_sec": cd}, 429)
            except Exception:
                pass

        emp = employees or 0
        mats = materials or 0
        if emp < cfg["min_emp"]:
            return json_response({"error": "not_enough_employees", "need": cfg["min_emp"]}, 400)
        if mats < cfg["mat_cycle"]:
            return json_response({"error": "not_enough_materials", "need": cfg["mat_cycle"]}, 400)

        qual = MATERIAL_QUALITY.get(mat_qual or "low", MATERIAL_QUALITY["low"])
        lv = level or 1
        salary = emp * cfg["salary"]

        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        urow = await cur2.fetchone()
        size = urow[0] or 0
        if size < salary:
            return json_response({"error": "cannot_pay_salary", "need": salary, "size": size}, 400)

        goods_gain = int(cfg["base_out"] * emp * qual["eff"] * BIZ_LEVEL_MULT.get(lv, 1.0))
        new_size = size - salary
        new_mats = mats - cfg["mat_cycle"]
        new_goods = (goods or 0) + goods_gain

        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute(
            "UPDATE businesses SET materials=?, goods=?, last_prod=? WHERE biz_id=?",
            (new_mats, new_goods, now.isoformat(), biz_id))
        await db.commit()

    return json_response({
        "success": True, "goods_gain": goods_gain, "salary_paid": salary,
        "materials_left": new_mats, "goods_total": new_goods, "new_size": new_size,
    })


async def api_biz_buy_materials(request):
    try:
        user_id = int(request.match_info["user_id"])
        biz_id = int(request.match_info["biz_id"])
        body = await request.json()
        qual_key = str(body.get("qual", "low"))
        qty = int(body.get("qty", 0))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if qual_key not in MATERIAL_QUALITY:
        return json_response({"error": "unknown quality"}, 400)
    if qty < 1:
        return json_response({"error": "qty must be >= 1"}, 400)

    qual = MATERIAL_QUALITY[qual_key]
    total_cost = qty * qual["price"]

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT materials FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        urow = await cur2.fetchone()
        size = urow[0] or 0
        if size < total_cost:
            return json_response({"error": "insufficient", "need": total_cost, "size": size}, 400)
        new_mats = (row[0] or 0) + qty
        new_size = size - total_cost
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute("UPDATE businesses SET materials=?, mat_qual=? WHERE biz_id=?",
                         (new_mats, qual_key, biz_id))
        await db.commit()

    return json_response({"success": True, "qty": qty, "cost": total_cost,
                          "materials_total": new_mats, "new_size": new_size})


async def api_biz_hire(request):
    try:
        user_id = int(request.match_info["user_id"])
        biz_id = int(request.match_info["biz_id"])
        body = await request.json()
        count = int(body.get("count", 0))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if count < 1:
        return json_response({"error": "count must be >= 1"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT biz_type, employees, level FROM businesses WHERE biz_id=? AND owner_id=?",
            (biz_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        biz_type, employees, level = row
        cfg = BIZ_TYPES.get(biz_type, {})
        lv = level or 1
        max_emp = lv * 10
        emp = employees or 0
        if emp + count > max_emp:
            return json_response({"error": "exceeds_max", "max_emp": max_emp}, 400)

        salary = cfg.get("salary", 1000)
        recruit_cost = salary * 5 * count
        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        urow = await cur2.fetchone()
        size = urow[0] or 0
        if size < recruit_cost:
            return json_response({"error": "insufficient", "need": recruit_cost, "size": size}, 400)

        new_size = size - recruit_cost
        new_emp = emp + count
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute("UPDATE businesses SET employees=? WHERE biz_id=?", (new_emp, biz_id))
        await db.commit()

    return json_response({"success": True, "hired": count, "employees": new_emp,
                          "cost": recruit_cost, "new_size": new_size})


async def api_biz_fire(request):
    try:
        user_id = int(request.match_info["user_id"])
        biz_id = int(request.match_info["biz_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid input"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT employees FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        fired = row[0] or 0
        await db.execute("UPDATE businesses SET employees=0 WHERE biz_id=?", (biz_id,))
        await db.commit()

    return json_response({"success": True, "fired": fired})


async def api_biz_sell(request):
    try:
        user_id = int(request.match_info["user_id"])
        biz_id = int(request.match_info["biz_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid input"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT biz_type, goods FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        biz_type, goods = row
        goods = goods or 0
        if goods <= 0:
            return json_response({"error": "no_goods"}, 400)

        cfg = BIZ_TYPES.get(biz_type, {"tax": 0.15})
        tax = int(goods * cfg["tax"])
        received = goods - tax

        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        size = (await cur2.fetchone())[0] or 0
        new_size = size + received

        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute("UPDATE businesses SET goods=0 WHERE biz_id=?", (biz_id,))
        await _add_to_bank(db, tax)
        await db.commit()

    return json_response({"success": True, "goods": goods, "tax": tax,
                          "received": received, "new_size": new_size})


async def api_biz_upgrade(request):
    try:
        user_id = int(request.match_info["user_id"])
        biz_id = int(request.match_info["biz_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid input"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT biz_type, level FROM businesses WHERE biz_id=? AND owner_id=?", (biz_id, user_id))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)
        biz_type, level = row
        lv = level or 1
        if lv >= 5:
            return json_response({"error": "max_level"}, 400)

        cfg = BIZ_TYPES.get(biz_type, {"upg_cost": 1000000})
        upg_cost = cfg["upg_cost"] * lv
        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        size = (await cur2.fetchone())[0] or 0
        if size < upg_cost:
            return json_response({"error": "insufficient", "need": upg_cost, "size": size}, 400)

        new_level = lv + 1
        new_size = size - upg_cost
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute("UPDATE businesses SET level=? WHERE biz_id=?", (new_level, biz_id))
        await db.commit()

    return json_response({"success": True, "level": new_level,
                          "mult": BIZ_LEVEL_MULT.get(new_level), "new_size": new_size})


# ── CLAN ─────────────────────────────────────────────────────────────────────

async def api_clan(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cur = await db.execute("SELECT clan_id, role FROM clan_members WHERE user_id=?", (user_id,))
            member = await cur.fetchone()
        except Exception:
            member = None
        if not member:
            return json_response({"clan": None, "members": []})

        clan_id, role = member
        try:
            cur2 = await db.execute(
                "SELECT name, owner_id, treasury FROM clans WHERE clan_id=?", (clan_id,))
            clan = await cur2.fetchone()
        except Exception:
            clan = None
        if not clan:
            return json_response({"clan": None, "members": []})

        try:
            cur3 = await db.execute(
                """SELECT cm.user_id, cm.role, u.name, u.size
                   FROM clan_members cm LEFT JOIN users u ON cm.user_id=u.user_id
                   WHERE cm.clan_id=? ORDER BY u.size DESC""", (clan_id,))
            members = await cur3.fetchall()
        except Exception:
            members = []

    return json_response({
        "clan": {"id": clan_id, "name": clan[0], "owner_id": clan[1],
                 "treasury": clan[2] or 0, "my_role": role},
        "members": [{"user_id": m[0], "role": m[1], "name": m[2] or "Игрок", "size": m[3] or 0}
                    for m in members],
    })


async def api_clan_contribute(request):
    try:
        user_id = int(request.match_info["user_id"])
        body = await request.json()
        amount = int(body.get("amount", 0))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if amount <= 0:
        return json_response({"error": "amount must be > 0"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT clan_id FROM clan_members WHERE user_id=?", (user_id,))
        mrow = await cur.fetchone()
        if not mrow:
            return json_response({"error": "not_in_clan"}, 400)
        clan_id = mrow[0]
        cur2 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        urow = await cur2.fetchone()
        size = urow[0] or 0
        if size < amount:
            return json_response({"error": "insufficient", "size": size}, 400)
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (size - amount, user_id))
        await db.execute("UPDATE clans SET treasury = treasury + ? WHERE clan_id=?", (amount, clan_id))
        cur3 = await db.execute("SELECT treasury FROM clans WHERE clan_id=?", (clan_id,))
        new_treasury = (await cur3.fetchone())[0]
        await db.commit()

    return json_response({"success": True, "contributed": amount,
                          "treasury": new_treasury, "new_size": size - amount})


async def api_clan_withdraw(request):
    try:
        user_id = int(request.match_info["user_id"])
        body = await request.json()
        amount = int(body.get("amount", 0))
    except Exception:
        return json_response({"error": "invalid input"}, 400)
    if amount <= 0:
        return json_response({"error": "amount must be > 0"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT clan_id FROM clan_members WHERE user_id=?", (user_id,))
        mrow = await cur.fetchone()
        if not mrow:
            return json_response({"error": "not_in_clan"}, 400)
        clan_id = mrow[0]
        cur2 = await db.execute("SELECT owner_id, treasury FROM clans WHERE clan_id=?", (clan_id,))
        crow = await cur2.fetchone()
        if not crow or crow[0] != user_id:
            return json_response({"error": "not_owner"}, 403)
        treasury = crow[1] or 0
        if amount > treasury:
            return json_response({"error": "insufficient_treasury", "treasury": treasury}, 400)
        cur3 = await db.execute("SELECT size FROM users WHERE user_id=?", (user_id,))
        size = (await cur3.fetchone())[0] or 0
        new_size = size + amount
        new_treasury = treasury - amount
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.execute("UPDATE clans SET treasury=? WHERE clan_id=?", (new_treasury, clan_id))
        await db.commit()

    return json_response({"success": True, "withdrawn": amount,
                          "treasury": new_treasury, "new_size": new_size})


# ── STATIC ────────────────────────────────────────────────────────────────────

async def serve_webapp(request):
    path = os.path.join(os.path.dirname(__file__), "webapp", "index.html")
    return web.FileResponse(path)


def create_app():
    app = web.Application()
    app.router.add_get("/", serve_webapp)
    # Public
    app.router.add_get("/api/stocks", api_stocks)
    app.router.add_get("/api/history/{ticker}", api_history)
    app.router.add_get("/api/top", api_top)
    app.router.add_get("/api/bank", api_bank)
    app.router.add_get("/api/business/types", api_biz_types)
    # User
    app.router.add_get("/api/user/{user_id}", api_user)
    app.router.add_post("/api/grow/{user_id}", api_grow)
    app.router.add_get("/api/portfolio/{user_id}", api_portfolio)
    app.router.add_post("/api/slots/{user_id}", api_slots)
    # Bank user
    app.router.add_get("/api/bank/{user_id}", api_bank_user)
    app.router.add_post("/api/bank/{user_id}/loan", api_bank_loan)
    app.router.add_post("/api/bank/{user_id}/repay", api_bank_repay)
    app.router.add_post("/api/bank/{user_id}/deposit/create", api_bank_deposit_create)
    app.router.add_post("/api/bank/{user_id}/deposit/{deposit_id}/claim", api_bank_deposit_claim)
    # Business
    app.router.add_get("/api/business/{user_id}", api_business)
    app.router.add_post("/api/business/{user_id}/create", api_biz_create)
    app.router.add_post("/api/business/{user_id}/{biz_id}/produce", api_biz_produce)
    app.router.add_post("/api/business/{user_id}/{biz_id}/buy-materials", api_biz_buy_materials)
    app.router.add_post("/api/business/{user_id}/{biz_id}/hire", api_biz_hire)
    app.router.add_post("/api/business/{user_id}/{biz_id}/fire", api_biz_fire)
    app.router.add_post("/api/business/{user_id}/{biz_id}/sell", api_biz_sell)
    app.router.add_post("/api/business/{user_id}/{biz_id}/upgrade", api_biz_upgrade)
    # Clan
    app.router.add_get("/api/clan/{user_id}", api_clan)
    app.router.add_post("/api/clan/{user_id}/contribute", api_clan_contribute)
    app.router.add_post("/api/clan/{user_id}/withdraw", api_clan_withdraw)
    # CORS preflight
    app.router.add_route("OPTIONS", "/{path_info:.*}", options_handler)
    return app


async def main():
    await init_db()
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Сервер: http://0.0.0.0:{PORT}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
