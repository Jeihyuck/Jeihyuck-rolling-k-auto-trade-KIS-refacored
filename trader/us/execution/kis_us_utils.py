# -*- coding: utf-8 -*-
"""Small pure helpers shared by the US KIS client."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def endpoint_label(method: str, path: str) -> str:
    tail = str(path or "").strip().split("/")[-1]
    if tail == "order":
        return "POST_order" if method.upper() == "POST" else "GET_order"
    return f"{method.upper()}_{tail}"


def resolve_us_dailyprice_bymd(as_of_date: str | None = None) -> str:
    """KIS dailyprice BYMD 파라미터를 결정한다."""
    if as_of_date:
        return str(as_of_date).replace("-", "")[:8]
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")


def extract_input_field_name(error_msg: str) -> str:
    """Extract the missing field name from a KIS INPUT_FIELD_NAME error."""
    marker = "INPUT_FIELD_NAME"
    if marker not in error_msg:
        return ""
    tail = error_msg.split(marker, 1)[-1]
    cleaned = tail.replace("'", "").replace('"', "").replace(":", "").strip()
    tokens = cleaned.split()
    return tokens[0] if tokens else ""
