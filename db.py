DB_CONFIG = {
    "user": "postgres",
    "password": "qwerty",
    "database": "lots",
    "host": "localhost",
    "port": 5432
}

TABLE_NAME = "lots"

async def save_to_db(pool, lots):
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                title TEXT,
                price TEXT
            );
        """)
        await conn.executemany(
            f"INSERT INTO {TABLE_NAME} (title, price) VALUES ($1, $2)",
            [(lot["title"], lot["price"]) for lot in lots]
        )