"""Read-only emergency-seed market-verification drill on the WSL PREP host.

Uses FinanceDataReader KRX listing; does not touch Supabase, KIS orders,
positions or live PREP. Fail closed if listing source is unavailable.
Run with repo cwd: .venv/bin/python scripts/kr_emergency_venue_readonly_probe.py
"""
from __future__ import annotations

import json
import sys
import time

from trader.universe.build import (
    _load_seed_rows,
    _load_krx_listing_market_map,
    _load_emergency_seed,
)


def build_report(rows: list[dict], venue_map: dict[str, str], *,
                 elapsed_s: float, emergency: dict | None) -> dict:
    codes = [str(row.get("code") or "").strip().zfill(6) for row in rows]
    verified = [c for c in codes if venue_map.get(c) in {"KOSPI", "KOSDAQ"}]
    unknown = [c for c in codes if c not in verified]
    declared_conflicts = [
        c for c, row in zip(codes, rows)
        if row.get("market") in {"KOSPI", "KOSDAQ"}
        and venue_map.get(c) != row["market"]
    ]
    coverage = len(verified) / len(codes) if codes else 0.0
    markets = {"KOSPI": sum(venue_map.get(c) == "KOSPI" for c in verified),
               "KOSDAQ": sum(venue_map.get(c) == "KOSDAQ" for c in verified)}
    accepted = bool(
        rows and coverage >= 0.95 and not declared_conflicts
        and emergency is not None
        and emergency.get("params", {}).get("market_source") in {
            "krx_listing_verified", "explicit_seed_csv"
        }
    )
    return {
        "status": "VERIFIED_INPUTS_ONLY" if accepted else "INCONCLUSIVE",
        "read_only": True,
        "fdr_elapsed_s": round(elapsed_s, 2),
        "legacy_seed_size": len(codes),
        "listing_mapping_size": len(venue_map),
        "coverage": round(coverage, 5),
        "verified_by_market": markets,
        "unknown_count": len(unknown),
        "unknown_sample": unknown[:10],
        "declared_conflicts": declared_conflicts[:10],
        "emergency_member_count": len(emergency.get("members") or []) if emergency else 0,
        "historical_orders_changed": False,
    }


def main() -> int:
    import os
    from pathlib import Path

    if os.environ.get("KIS_ENV", "practice").lower() != "practice":
        print(json.dumps({"status": "INCONCLUSIVE", "reason": "practice_only"}))
        return 2
    os.environ["KIS_ENV"] = "practice"
    os.environ["US_KIS_ORDER_ALLOWED"] = "0"
    os.environ["ALLOW_REAL_ORDER"] = "0"
    os.environ["DRY_RUN"] = "1"
    os.environ["UNIVERSE_EMERGENCY_VERIFY_KRX"] = "1"
    seed = _load_seed_rows(Path("data") / "universe_seed.csv")
    started = time.monotonic()
    try:
        market_map = _load_krx_listing_market_map()
        # Reuse one verified listing snapshot: do not double HTTP-fetch KRX or
        # obtain inconsistent venues during the same read-only probe.
        from unittest.mock import patch
        with patch("trader.universe.build._load_krx_listing_market_map",
                   return_value=market_map):
            emergency = _load_emergency_seed()
        report = build_report(
            seed, market_map, elapsed_s=time.monotonic() - started,
            emergency=emergency,
        )
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0 if report["status"] == "VERIFIED_INPUTS_ONLY" else 2
    except Exception as exc:
        # Avoid emitting anything potentially contained in vendor error bodies.
        print(json.dumps({
            "status": "INCONCLUSIVE", "reason_type": type(exc).__name__,
            "elapsed_s": round(time.monotonic() - started, 2),
        }))
        return 2


if __name__ == "__main__":
    sys.exit(main())
