from __future__ import annotations

from typing import Any

from scripts.reset_practice_account_state import (
    build_account_key,
    execute_practice_account_state_reset,
    main,
    resolve_capital_krw,
    resolve_reset_env,
)


def execute_practice_account_reset(*, engine=None, kis=None) -> dict[str, Any]:
    return execute_practice_account_state_reset(engine=engine, kis=kis)


if __name__ == "__main__":
    raise SystemExit(main())