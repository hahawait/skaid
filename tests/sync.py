from conn import DatabaseConnection
from sync import DBSynchronizer


async def sync(
    synchronizer: DBSynchronizer,
    source_db: DatabaseConnection,
    target_db: DatabaseConnection,
):

    insert_data_query = """
    INSERT INTO users (name, email) VALUES
    ('David', 'david@example.com')
    ON CONFLICT DO NOTHING;
    """
    await source_db.execute(insert_data_query)

    insert_data_query = """
    INSERT INTO orders (name, email) VALUES
    ('Igor', 'igor@example.com')
    ON CONFLICT DO NOTHING;
    """
    await source_db.execute(insert_data_query)

    await synchronizer.synchronize()

    source_tables = await synchronizer.get_tables(source_db)
    target_tables = await synchronizer.get_tables(target_db)
    assert len(target_tables) == len(source_tables)

    select_query = "SELECT * FROM users;"
    source_data = await source_db.fetch(select_query)
    target_data = await target_db.fetch(select_query)
    assert len(source_data) == len(target_data)
    assert source_data == target_data

    select_query = "SELECT * FROM orders;"
    source_data = await source_db.fetch(select_query)
    target_data = await target_db.fetch(select_query)
    assert len(source_data) == len(target_data)
    assert source_data == target_data

    print("SYNC SCHEMAS TEST PASSED\n")