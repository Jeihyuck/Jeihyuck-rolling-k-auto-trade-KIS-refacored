import re
from uuid import uuid4


UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')


def new_run_id() -> str:
    return str(uuid4())


def assert_uuid(run_id: str) -> None:
    if not UUID_PATTERN.match(run_id):
        raise ValueError(f"run_id must be a valid UUID string, got: {run_id}")