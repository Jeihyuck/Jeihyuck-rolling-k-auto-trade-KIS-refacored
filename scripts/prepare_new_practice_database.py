from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from trader.db.engine import get_db_url
from trader.db.practice_database_generation import (
    prepare_new_practice_database,
    target_url_from_source,
    write_generation_manifest,
)

logger = logging.getLogger(__name__)


def _manifest_dir() -> Path:
    raw = os.getenv("PRACTICE_DB_GENERATION_MANIFEST_DIR") or "runtime/private/practice_db_generations"
    return Path(raw)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    source_url = get_db_url()
    target_url = str(os.getenv("PBCORE_NEW_DB_URL") or "").strip() or None
    target_name = str(os.getenv("NEW_PRACTICE_DB_NAME") or "").strip() or None

    result = prepare_new_practice_database(
        source_url=source_url,
        target_database_name=target_name,
        target_url=target_url,
    )
    manifest = write_generation_manifest(result, _manifest_dir())

    # Never print credentials. Only database identities and the manifest path.
    output = {
        "status": result["status"],
        "source": result["source"],
        "target": result["target"],
        "source_preserved": result["source_preserved"],
        "history_copied_to_new_db": result["history_copied_to_new_db"],
        "target_state_row_counts": result["target_state_row_counts"],
        "target_migration_count": result["target_migration_count"],
        "target_latest_migration": result["target_latest_migration"],
        "manifest_path": str(manifest),
        "next_step": (
            "After KIS practice reset and cutover verification, change PBCORE_DB_URL "
            "to the new database URL on every runtime host."
        ),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))

    if target_url is None and target_name:
        # This message intentionally contains only the DB name, not credentials.
        logger.info(
            "[PRACTICE_DB_GENERATION][TARGET_CREATED] database=%s source_unchanged=1",
            target_name,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
