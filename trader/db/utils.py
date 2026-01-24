def is_postgres_url(url: str) -> bool:
    normalized = (url or "").strip().lower()
    return normalized.startswith("postgres://") or normalized.startswith("postgresql://")
