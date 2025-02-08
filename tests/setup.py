import asyncpg
from conn import DatabaseConnection


async def create_test_dbs(dsn: str):
    conn = await asyncpg.connect(dsn)

    await conn.execute("DROP DATABASE IF EXISTS source;")
    await conn.execute("DROP DATABASE IF EXISTS target;")
    await conn.execute("CREATE DATABASE source;")
    await conn.execute("CREATE DATABASE target;")

    await conn.close()
    print("Test databases created.")


async def setup_source_db(source_db: DatabaseConnection):
    await source_db.connect()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL
    );
    """
    await source_db.execute(create_table_query)
    
    insert_data_query = """
    INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com')
    ON CONFLICT DO NOTHING;
    """
    await source_db.execute(insert_data_query)

    create_new_table_query = """
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL
    );
    """
    await source_db.execute(create_new_table_query)

    await source_db.close()
    print("Source database set up with test data.")


async def setup_target_db(target_db: DatabaseConnection):
    await target_db.connect()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL
    );
    """
    await target_db.execute(create_table_query)

    insert_data_query = """
    INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com')
    ON CONFLICT DO NOTHING;
    """
    await target_db.execute(insert_data_query)

    await target_db.close()
    print("Target database set up.")
