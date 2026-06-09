#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--fallback-key", default="")
    parser.add_argument("--default", default="")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(args.default)
        return 0

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        print(args.default)
        return 0

    value = payload.get(args.key)
    if value is None and args.fallback_key:
        value = payload.get(args.fallback_key)
    if value is None:
        value = args.default

    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
