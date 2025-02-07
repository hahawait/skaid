import asyncio
from conn import DatabaseConnection
from sync import DBSynchronizer


async def main():
    source_db = DatabaseConnection("postgresql://postgres:postgres@localhost:5432/source")
    target_db = DatabaseConnection("postgresql://postgres:postgres@localhost:5432/target")
    synchronizer = DBSynchronizer(source_db, target_db)
    await synchronizer.synchronize()

if __name__=="__main__":
    asyncio.run(main())
