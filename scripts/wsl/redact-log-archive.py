#!/usr/bin/env python3
"""Redact staged logs without ever applying regex substitutions to JSON syntax."""
from __future__ import annotations
import json, re, sys
from pathlib import Path

SENSITIVE = {x.lower() for x in ("CANO ACNT_PRDT_CD APP_KEY APP_SECRET KIS_APP_KEY KIS_APP_SECRET ACCESS_TOKEN SMTP_PASS DATABASE_URL DB_URL authorization token password account recipient message_id").split()}
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SECRET = re.compile(r"(?i)((?:APP_KEY|APP_SECRET|KIS_APP_KEY|KIS_APP_SECRET|ACCESS_TOKEN|SMTP_PASS|DATABASE_URL|DB_URL|CANO|ACNT_PRDT_CD|token|password|account)\s*[=:]\s*)[^\s,\"]+")
ACCOUNT = re.compile(r"(?<![\d.])(\d{4})[- ]?\d{4}[- ]?(\d{2,6})(?![\d.])")

def redact_json(value):
    if isinstance(value, dict):
        return {k: "[REDACTED]" if str(k).lower() in SENSITIVE else redact_json(v) for k, v in value.items()}
    if isinstance(value, list): return [redact_json(v) for v in value]
    return value

def redact_tree(root: Path) -> None:
    for path in root.rglob("*"):
        if not path.is_file(): continue
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            path.write_text(json.dumps(redact_json(data), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        elif path.suffix.lower() in {".log", ".txt", ".md"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            text = EMAIL.sub("[REDACTED_EMAIL]", SECRET.sub(r"\1[REDACTED]", text))
            path.write_text(ACCOUNT.sub(r"\1-****-\2", text), encoding="utf-8")

if __name__ == "__main__": redact_tree(Path(sys.argv[1]))
