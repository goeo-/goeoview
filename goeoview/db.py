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
    def __init__(self, host="127.0.0.1", database="goeoview", port=8123) -> None:
        self.host = host
        self.database = database
        self.port = port
        self.db = None

    async def start(self):
        if self.db is not None:
            return
        db = await clickhouse_connect.create_async_client(
            host=self.host, port=self.port, database=self.database
        )

        self.db = db
