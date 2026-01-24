import sqlalchemy as sa

LOCK_KEY = 912345678


def acquire_advisory_lock(conn, key: int = LOCK_KEY) -> bool:
    return bool(conn.execute(sa.text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar())


def release_advisory_lock(conn, key: int = LOCK_KEY) -> None:
    conn.execute(sa.text("SELECT pg_advisory_unlock(:k)"), {"k": key})
