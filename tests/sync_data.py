from conn import DatabaseConnection
from sync import DBSynchronizer


async def sync_data(
    synchronizer: DBSynchronizer,
    source_db: DatabaseConnection,
    target_db: DatabaseConnection,
):
    source_tables = await synchronizer.get_tables(source_db)
    for table in source_tables:
        await synchronizer.sync_data(table)

    select_query = "SELECT * FROM users;"
    source_data = await source_db.fetch(select_query)
    target_data = await target_db.fetch(select_query)
    assert len(source_data) == len(target_data)
    print("SYNC DATA TEST PASSED\n")