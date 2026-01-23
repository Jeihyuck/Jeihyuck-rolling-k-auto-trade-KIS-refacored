from sqlalchemy import Engine, create_engine, event

from . import config


def make_engine(database_url: str | None = None) -> Engine:
    url = database_url or config.get_database_url()
    kwargs = {}
    if not config.is_sqlite_url(url):
        kwargs["pool_pre_ping"] = True
        kwargs["pool_size"] = 5
        kwargs["max_overflow"] = 10
    engine = create_engine(url, echo=config.get_db_echo(), **kwargs)
    if config.is_sqlite_url(url):
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _connection_record) -> None:
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON;")
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA busy_timeout=5000;")
            cursor.close()
    return engine
