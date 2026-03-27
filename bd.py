import asyncpg

DB_CONFIG = {
    "user": "postgres",
    "password": "password",
    "database": "cucumbers",
    "host": "localhost"
}

pool = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(**DB_CONFIG)

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT,
            chat_id BIGINT,
            size INTEGER DEFAULT 0,
            last_grow TIMESTAMP,
            last_box TIMESTAMP,
            last_tax TIMESTAMP,
            name TEXT,
            PRIMARY KEY (user_id, chat_id)
        )
        """)


async def get_user(user_id, chat_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT size, last_grow FROM users WHERE user_id=$1 AND chat_id=$2",
            user_id, chat_id
        )

        if row:
            return row["size"], row["last_grow"]

        await conn.execute(
            "INSERT INTO users (user_id, chat_id) VALUES ($1, $2)",
            user_id, chat_id
        )

        return 0, None


async def update_size(user_id, chat_id, size):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET size=$1 WHERE user_id=$2 AND chat_id=$3",
            size, user_id, chat_id
        )


async def update_last_box(user_id, chat_id):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_box=NOW() WHERE user_id=$1 AND chat_id=$2",
            user_id, chat_id
        )


async def save_user_name(user_id, chat_id, name):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET name=$1 WHERE user_id=$2 AND chat_id=$3",
            name, user_id, chat_id
        )


async def get_top(chat_id):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT user_id, size, name FROM users WHERE chat_id=$1 ORDER BY size DESC LIMIT 10",
            chat_id
        )