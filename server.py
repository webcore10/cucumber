import asyncio
import json
import os
import random
from datetime import datetime

import aiosqlite
import pytz
from aiohttp import web

MSK = pytz.timezone("Europe/Moscow")
DB_NAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cucumbers.db")
PORT = 8080
GROW_COOLDOWN = 3600

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

BIZ_TYPES = {
    "farm":    {"emoji": "🌾", "label": "Ферма"},
    "factory": {"emoji": "🏭", "label": "Завод"},
    "mine":    {"emoji": "⛏️",  "label": "Шахта"},
    "brewery": {"emoji": "🍺", "label": "Пивоварня"},
    "it":      {"emoji": "💻", "label": "IT-компания"},
}

SLOT_SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "💎", "7️⃣", "🥒"]
SLOT_WEIGHTS = [30, 25, 20, 15, 5, 3, 2]
SLOT_PAYOUTS = {
    "🥒🥒🥒": 50,
    "7️⃣7️⃣7️⃣": 25,
    "💎💎💎": 15,
    "🍇🍇🍇": 6,
    "🍊🍊🍊": 5,
    "🍋🍋🍋": 4,
    "🍒🍒🍒": 3,
}

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def json_response(data, status=200):
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        status=status,
        content_type="application/json",
        headers=CORS,
    )


async def options_handler(request):
    return web.Response(status=204, headers=CORS)


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
            await db.execute(
                "INSERT OR IGNORE INTO volatile_stocks (ticker, price) VALUES (?, ?)",
                (ticker, price))
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
        await db.commit()
    print(f"✅ БД готова: {DB_NAME}")


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
            elapsed = (datetime.now(MSK) - last_time).total_seconds()
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
        cur = await db.execute(
            "SELECT size, last_grow, loan FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return json_response({"error": "not found"}, 404)

        size, last_grow, loan = row
        size = size or 0
        loan = loan or 0
        now = datetime.now(MSK)

        if last_grow:
            try:
                last_time = datetime.fromisoformat(last_grow)
                if last_time.tzinfo is None:
                    last_time = MSK.localize(last_time)
                elapsed = (now - last_time).total_seconds()
                if elapsed < GROW_COOLDOWN:
                    return json_response({
                        "error": "cooldown",
                        "cooldown_remaining": int(GROW_COOLDOWN - elapsed),
                    }, 429)
            except Exception:
                pass

        gain = random.randint(1, 15)
        kept, repaid = gain, 0
        if loan > 0:
            repay = min(loan, gain)
            kept, repaid = gain - repay, repay
            new_loan = loan - repay
            if new_loan > 0:
                await db.execute("UPDATE users SET loan=?, loan_date=? WHERE user_id=?",
                                 (new_loan, now.isoformat(), user_id))
            else:
                await db.execute("UPDATE users SET loan=0, loan_date=NULL WHERE user_id=?",
                                 (user_id,))

        new_size = size + kept
        cur2 = await db.execute("SELECT max_size FROM users WHERE user_id=?", (user_id,))
        max_row = await cur2.fetchone()
        new_max = max(max_row[0] or 0, new_size) if max_row else new_size
        await db.execute("UPDATE users SET size=?, max_size=?, last_grow=? WHERE user_id=?",
                         (new_size, new_max, now.isoformat(), user_id))
        await db.commit()

    return json_response({
        "success": True, "gain": gain, "kept": kept, "repaid": repaid,
        "new_size": new_size, "cooldown_remaining": GROW_COOLDOWN,
    })


# ── STOCKS ────────────────────────────────────────────────────────────────────

async def api_stocks(request):
    async with aiosqlite.connect(DB_NAME) as db:
        # volatile: current price from volatile_stocks table
        cur = await db.execute("SELECT ticker, price FROM volatile_stocks")
        volatile = {t: p for t, p in await cur.fetchall()}
        if not volatile:
            volatile = dict(VOLATILE_INITIAL_PRICES)

        # volatile change: compare with 2nd-to-last price_history entry
        prev_volatile = {}
        for ticker in VOLATILE_STOCKS:
            cur2 = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 2",
                (ticker,))
            rows = await cur2.fetchall()
            prev_volatile[ticker] = rows[1][0] if len(rows) >= 2 else volatile.get(ticker, 0.0)

        # real stocks: latest from price_history
        real_prices, prev_real = {}, {}
        for ticker in STOCKS:
            cur3 = await db.execute(
                "SELECT price FROM price_history WHERE ticker=? ORDER BY id DESC LIMIT 2",
                (ticker,))
            rows = await cur3.fetchall()
            real_prices[ticker] = rows[0][0] if rows else 0.0
            prev_real[ticker] = rows[1][0] if len(rows) >= 2 else real_prices[ticker]

    result = []
    for ticker, name in STOCKS.items():
        price = real_prices.get(ticker, 0.0)
        prev = prev_real.get(ticker, price)
        change = round((price - prev) / prev * 100, 2) if prev else 0.0
        result.append({"ticker": ticker, "name": name, "price": price,
                       "change": change, "type": "stock"})

    for ticker, name in VOLATILE_STOCKS.items():
        price = volatile.get(ticker, VOLATILE_INITIAL_PRICES.get(ticker, 0.0))
        prev = prev_volatile.get(ticker, price)
        change = round((price - prev) / prev * 100, 2) if prev else 0.0
        result.append({"ticker": ticker, "name": name, "price": round(price, 2),
                       "change": change, "type": "crypto"})

    return json_response(result)


# ── HISTORY ───────────────────────────────────────────────────────────────────

async def api_history(request):
    ticker = request.match_info.get("ticker", "").upper()
    if ticker not in ALL_STOCKS:
        return json_response({"error": "unknown ticker"}, 404)

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT price, recorded_at FROM price_history WHERE ticker=? ORDER BY id ASC LIMIT 72",
            (ticker,))
        rows = await cur.fetchall()

        # fallback: if no history, return current volatile price as single point
        if not rows and ticker in VOLATILE_STOCKS:
            cur2 = await db.execute("SELECT price FROM volatile_stocks WHERE ticker=?", (ticker,))
            r = await cur2.fetchone()
            if r:
                rows = [(r[0], datetime.now(MSK).isoformat())]

    data = [{"price": r[0], "time": r[1]} for r in rows]
    return json_response({"ticker": ticker, "name": ALL_STOCKS.get(ticker, ticker), "data": data})


# ── TOP / FORBES ──────────────────────────────────────────────────────────────

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


# ── BANK ──────────────────────────────────────────────────────────────────────

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
            net = 0  # two-of-a-kind → push, return bet
        else:
            net = -bet

        new_size = (row[0] or 0) + net
        await db.execute("UPDATE users SET size=? WHERE user_id=?", (new_size, user_id))
        await db.commit()

    return json_response({
        "reels": reels, "won": net > 0, "push": net == 0,
        "multiplier": multiplier, "net": net, "new_size": new_size,
    })


# ── BUSINESS ─────────────────────────────────────────────────────────────────

async def api_business(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cur = await db.execute(
                """SELECT biz_id, biz_type, name, level, employees, materials,
                          mat_qual, goods, last_prod
                   FROM businesses WHERE owner_id=? ORDER BY biz_id""",
                (user_id,))
            rows = await cur.fetchall()
        except Exception:
            rows = []

    result = []
    for row in rows:
        biz_id, biz_type, name, level, employees, materials, mat_qual, goods, last_prod = row
        info = BIZ_TYPES.get(biz_type, {"emoji": "🏢", "label": biz_type})
        result.append({
            "biz_id": biz_id, "type": biz_type,
            "emoji": info["emoji"], "label": info["label"],
            "name": name, "level": level or 1,
            "employees": employees or 0, "materials": materials or 0,
            "mat_qual": mat_qual or "low", "goods": goods or 0,
            "last_prod": last_prod,
        })
    return json_response(result)


# ── CLAN ─────────────────────────────────────────────────────────────────────

async def api_clan(request):
    try:
        user_id = int(request.match_info["user_id"])
    except (ValueError, KeyError):
        return json_response({"error": "invalid user_id"}, 400)

    async with aiosqlite.connect(DB_NAME) as db:
        try:
            cur = await db.execute(
                "SELECT clan_id, role FROM clan_members WHERE user_id=?", (user_id,))
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
                   WHERE cm.clan_id=? ORDER BY u.size DESC""",
                (clan_id,))
            members = await cur3.fetchall()
        except Exception:
            members = []

    return json_response({
        "clan": {
            "id": clan_id, "name": clan[0],
            "owner_id": clan[1], "treasury": clan[2] or 0,
            "my_role": role,
        },
        "members": [
            {"user_id": m[0], "role": m[1], "name": m[2] or "Игрок", "size": m[3] or 0}
            for m in members
        ],
    })


# ── STATIC ────────────────────────────────────────────────────────────────────

async def serve_webapp(request):
    path = os.path.join(os.path.dirname(__file__), "webapp", "index.html")
    return web.FileResponse(path)


def create_app():
    app = web.Application()
    app.router.add_get("/", serve_webapp)
    app.router.add_get("/api/user/{user_id}", api_user)
    app.router.add_post("/api/grow/{user_id}", api_grow)
    app.router.add_get("/api/stocks", api_stocks)
    app.router.add_get("/api/history/{ticker}", api_history)
    app.router.add_get("/api/top", api_top)
    app.router.add_get("/api/portfolio/{user_id}", api_portfolio)
    app.router.add_get("/api/bank", api_bank)
    app.router.add_post("/api/slots/{user_id}", api_slots)
    app.router.add_get("/api/business/{user_id}", api_business)
    app.router.add_get("/api/clan/{user_id}", api_clan)
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
