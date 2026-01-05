from sqlalchemy import Engine, create_engine

from . import config


def make_engine(database_url: str | None = None) -> Engine:
    url = database_url or config.get_database_url()
    kwargs = {}
    if not config.is_sqlite_url(url):
        kwargs["pool_pre_ping"] = True
    engine = create_engine(url, echo=config.get_db_echo(), **kwargs)
    return engine
