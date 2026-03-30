#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


def _latest_manifest(repo_root: Path) -> Path:
    prep_root = repo_root / "runtime" / "prep"
    if not prep_root.exists():
        raise SystemExit("[PREP][VERIFY][FAIL] runtime/prep missing")
    manifests = sorted(prep_root.glob("*/prep_manifest.json"))
    if not manifests:
        raise SystemExit("[PREP][VERIFY][FAIL] prep_manifest.json missing")
    return manifests[-1]


def main() -> int:
    repo_root = Path.cwd()
    path = _latest_manifest(repo_root)
    payload = json.loads(path.read_text(encoding="utf-8"))

    quality_ok = bool(payload.get("final30_quality_ok", False))
    flow_failed_ratio = payload.get("flow_failed_ratio")
    if flow_failed_ratio is None:
        raise SystemExit("[PREP][VERIFY][FAIL] flow_failed_ratio missing")

    required = ["final30_quality", "final30_count", "flow_total_symbols", "flow_success_symbols", "flow_failed_symbols"]
    missing = [k for k in required if k not in payload]
    if missing:
        raise SystemExit(f"[PREP][VERIFY][FAIL] missing_required={missing}")

    if int(payload.get("final30_count") or 0) != 30:
        raise SystemExit(f"[PREP][VERIFY][FAIL] final30_count_invalid={payload.get('final30_count')}")

    print(f"[PREP][VERIFY][SUMMARY] manifest={path}")
    print(f"[PREP][VERIFY][SUMMARY] quality_ok={int(quality_ok)} flow_failed_ratio={float(flow_failed_ratio):.3f} final30_count={payload.get('final30_count')}")

    soft_reasons = list((payload.get("final30_quality") or {}).get("soft_fail_reasons") or [])
    if soft_reasons:
        print(f"[PREP][VERIFY][SOFT_FAIL] reasons={soft_reasons}")

    if not quality_ok:
        raise SystemExit("[PREP][VERIFY][FAIL] final30_quality_ok=0")

    print("[PREP][VERIFY][OK] quality contract satisfied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
