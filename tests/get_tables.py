from conn import DatabaseConnection
from sync import DBSynchronizer


async def get_tables(
    synchronizer: DBSynchronizer,
    source_db: DatabaseConnection,
    target_db: DatabaseConnection,
):
    source_tables = await synchronizer.get_tables(source_db)
    assert len(source_tables) == 2

    target_tables = await synchronizer.get_tables(target_db)
    assert len(target_tables) == 1
    print("GET TABLES TEST PASSED\n")