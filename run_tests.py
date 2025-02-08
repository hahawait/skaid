import asyncio
from tests.get_tables import get_tables
from tests.setup import create_test_dbs, setup_source_db, setup_target_db
from tests.sync import sync
from tests.sync_data import sync_data
from tests.sync_schemas import sync_schemas
from conn import DatabaseConnection
from sync import DBSynchronizer


async def main():
    dsn = "postgres://analitic_tg_user:4f8b61rk@localhost/postgres"
    source_dsn = "postgres://analitic_tg_user:4f8b61rk@localhost/source"
    target_dsn = "postgres://analitic_tg_user:4f8b61rk@localhost/target"

    source_db = DatabaseConnection(source_dsn)
    target_db = DatabaseConnection(target_dsn)

    synchronizer = DBSynchronizer(source_db, target_db)

    await create_test_dbs(dsn)
    await setup_source_db(source_db)
    await setup_target_db(target_db)
    print("SETUP OK.\n")

    await get_tables(synchronizer, source_db, target_db)
    await sync_schemas(synchronizer, source_db, target_db)
    await sync_data(synchronizer, source_db, target_db)
    await sync(synchronizer, source_db, target_db)
    print("ALL TESTS PASSED.")


if __name__ == "__main__":
    asyncio.run(main())
