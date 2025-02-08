from conn import DatabaseConnection
from sync import DBSynchronizer


async def sync_schemas(
    synchronizer: DBSynchronizer,
    source_db: DatabaseConnection,
    target_db: DatabaseConnection,
):
    await synchronizer.sync_schemas()

    source_tables = await synchronizer.get_tables(source_db)
    target_tables = await synchronizer.get_tables(target_db)
    assert len(target_tables) == len(source_tables)
    print("SYNC SCHEMAS TEST PASSED\n")