import asyncpg

class DatabaseConnection:
    def __init__(self, dsn: str) -> None:
        self.dsn: str = dsn
        self.conn: asyncpg.Connection | None = None

    async def connect(self) -> None:
        if not self.conn:
            self.conn = await asyncpg.connect(self.dsn)

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def fetch(self, query: str, *args) -> list:
        await self.connect()
        return await self.conn.fetch(query, *args)

    async def execute(self, query: str, *args) -> str:
        await self.connect()
        return await self.conn.execute(query, *args)

    async def executemany(self, query: str, *args) -> None:
        await self.connect()
        return await self.conn.executemany(query, *args)
