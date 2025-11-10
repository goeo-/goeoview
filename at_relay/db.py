import clickhouse_connect

instance = None


def _db_singleton(_cls):
    def get_instance(*args, **kwargs):
        global instance
        if instance is None:
            instance = _cls(*args, **kwargs)
        return instance

    return get_instance


@_db_singleton
class DB:
    def __init__(self, host, database) -> None:
        self.host = host
        self.database = database
        self.db = None

    async def start(self):
        db = await clickhouse_connect.create_async_client(
            host=self.host, database=self.database
        )

        self.db = db
