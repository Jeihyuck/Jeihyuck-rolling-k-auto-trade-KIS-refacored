from __future__ import annotations

import argparse
import logging
import os

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo
from trader.universe.mode import resolve_strategy_mode

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DB universe precheck")
    parser.add_argument("--env", required=True, help="Environment key (practice/real)")
    parser.add_argument("--strategy", required=True, help="Strategy key")
    parser.add_argument("--mode", help="Strategy mode (LIVE/DIAG). Defaults to env")
    return parser.parse_args()


def _write_env_flag(flag: str, value: str) -> None:
    github_env = os.getenv("GITHUB_ENV")
    if not github_env:
        return
    try:
        with open(github_env, "a", encoding="utf-8") as f:
            f.write(f"{flag}={value}\n")
    except Exception:
        logger.exception("[UNIVERSE][DB_CHECK][ENV_FAIL] flag=%s value=%s", flag, value)


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    mode = (args.mode or resolve_strategy_mode() or "LIVE").upper()
    engine = make_engine()
    run_migrations(engine)
    
    # DB schema validation
    with engine.connect() as conn:
        sql = """
            select table_name, column_name, data_type, is_nullable, column_default
            from information_schema.columns
            where table_name in ('ledger_events','positions','runs')
              and column_name in ('ok','tp1_done','tp2_done','dry_run')
            order by table_name, column_name;
        """
        result = conn.exec_driver_sql(sql)
        expected = {
            ('ledger_events', 'ok'): ('boolean', 'NO', 'true'),
            ('runs', 'dry_run'): ('boolean', 'NO', 'false'),
            ('positions', 'tp1_done'): ('boolean', 'NO', 'false'),
            ('positions', 'tp2_done'): ('boolean', 'NO', 'false'),
        }
        for row in result:
            table_name, column_name, data_type, is_nullable, column_default = row
            key = (table_name, column_name)
            if key in expected:
                exp_data_type, exp_nullable, exp_default = expected[key]
                if data_type != exp_data_type or is_nullable != exp_nullable or column_default != exp_default:
                    logger.error(
                        "[UNIVERSE][DB_CHECK][SCHEMA_FAIL] table=%s column=%s expected=(%s,%s,%s) actual=(%s,%s,%s)",
                        table_name, column_name, exp_data_type, exp_nullable, exp_default,
                        data_type, is_nullable, column_default
                    )
                    return 1
        logger.info("[UNIVERSE][DB_CHECK][SCHEMA_OK] boolean columns validated")
    
    repo = UniverseRepo(engine)
    snapshot = repo.get_current_universe_snapshot(args.env, args.strategy)
    if not snapshot or not snapshot.get("members"):
        logger.error(
            "[UNIVERSE][DB_CHECK][MISSING] env=%s strategy=%s mode=%s",
            args.env,
            args.strategy,
            mode,
        )
        if mode == "LIVE":
            logger.warning("[UNIVERSE][DB_CHECK][NO_TRADE] mode=LIVE -> skipping trade")
            _write_env_flag("NO_TRADE", "1")
            return 0
        return 1
    logger.info(
        "[UNIVERSE][DB_CHECK][OK] env=%s strategy=%s mode=%s run_id=%s as_of=%s members=%s sample=%s",
        args.env,
        args.strategy,
        mode,
        snapshot.get("run_id"),
        snapshot.get("as_of"),
        snapshot.get("members_count"),
        snapshot.get("sample_codes"),
    )
    _write_env_flag("NO_TRADE", "0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
