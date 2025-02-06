from conn import DatabaseConnection


class DBSynchronizer:
    def __init__(self, source_db: DatabaseConnection, target_db: DatabaseConnection):
        self.source_db = source_db
        self.target_db = target_db

    async def get_tables(self, db: DatabaseConnection):
        query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public';
        """
        rows = await db.fetch(query)
        return {row["table_name"] for row in rows}

    async def sync_schemas(self):
        """
        Синхронизирует структуру таблиц между базами данных.
        Создаёт таблицы в целевой БД, если их нет, но они есть в исходной БД.
        """
        source_tables = await self.get_tables(self.source_db)
        target_tables = await self.get_tables(self.target_db)

        missing_tables = source_tables - target_tables
        for table in missing_tables:
            # Получаем информацию о столбцах таблицы из исходной БД
            schema_query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table}';
            """
            columns = await self.source_db.fetch(schema_query)

            # Строка определения столбцов для создания таблицы
            columns_def = ", ".join(f"{col['column_name']} {col['data_type']}" for col in columns)

            create_query = f"CREATE TABLE {table} ({columns_def});"
            await self.target_db.execute(create_query)

            print(f"Created table {table}")


    async def sync_data(self, table):
        """
        Синхронизирует данные в таблицах между базами данных.
        Добавляет недостающие записи в целевую БД, но не изменяет существующие.
        """
        source_data = await self.source_db.fetch(f"SELECT * FROM {table};")
        target_data = await self.target_db.fetch(f"SELECT * FROM {table};")

        source_set = {tuple(row.values()) for row in source_data}
        target_set = {tuple(row.values()) for row in target_data}

        missing_rows = source_set - target_set

        for row in missing_rows:
            # Строка значений для вставки
            values = ", ".join(f"'{v}'" for v in row)

            query = f"INSERT INTO {table} VALUES ({values}) ON CONFLICT DO NOTHING;"
            await self.target_db.execute(query)

            print(f"Inserted into {table}: {values}")

    async def synchronize(self):
        await self.source_db.connect()
        await self.target_db.connect()

        if not self.target_db.conn:
            print("Failed to connect to target database.")
            return

        try:
            async with self.target_db.conn.transaction() as tx:
                try:
                    await self.sync_schemas()

                    tables = await self.get_tables(self.source_db)
                    for table in tables:
                        await self.sync_data(table)

                    await tx.commit()
                    print("Synchronization completed successfully.")
                except Exception as e:
                    print(f"Error during synchronization: {e}")
                    await tx.rollback()
        except AttributeError as e:
            print(f"Transaction error: {e}")
        finally:
            await self.source_db.close()
            await self.target_db.close()
