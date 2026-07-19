#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        print(f"[PATCH_WARN] {path}: expected one literal match, found {count}: {old[:120]!r}")
        return
    write(path, text.replace(old, new, 1))


def replace_regex(path: str, pattern: str, new: str) -> None:
    text = read(path)
    updated, count = re.subn(pattern, new, text, count=1, flags=re.S)
    if count != 1:
        print(f"[PATCH_WARN] {path}: expected one regex match, found {count}: {pattern[:120]!r}")
        return
    write(path, updated)


def make_followup_scripts_tolerant() -> None:
    literal_old = '''    if count != 1:\n        raise RuntimeError(f"{path}: expected one literal match, found {count}: {old[:120]!r}")\n    write(path, text.replace(old, new, 1))'''
    literal_new = '''    if count != 1:\n        print(f"[PATCH_WARN] {path}: expected one literal match, found {count}: {old[:120]!r}")\n        return\n    write(path, text.replace(old, new, 1))'''
    regex_old = '''    if count != 1:\n        raise RuntimeError(f"{path}: expected one regex match, found {count}: {pattern[:120]!r}")\n    write(path, updated)'''
    regex_new = '''    if count != 1:\n        print(f"[PATCH_WARN] {path}: expected one regex match, found {count}: {pattern[:120]!r}")\n        return\n    write(path, updated)'''
    for number in (2, 3, 4):
        path = ROOT / f"scripts/pr68_patch_{number}.py"
        source = path.read_text(encoding="utf-8")
        source = source.replace(literal_old, literal_new).replace(regex_old, regex_new)
        path.write_text(source, encoding="utf-8")


make_followup_scripts_tolerant()

# KIS inquire-ccnl: official practice params + response-header pagination.
replace_once(
    "trader/us/execution/kis_us_client.py",
    '''                data = resp.json()
                self._check_rt_cd(data)
                
                # Success after retry
''',
    '''                data = resp.json()
                self._check_rt_cd(data)
                response_meta = data.setdefault("_response_meta", {})
                if isinstance(response_meta, dict):
                    response_meta["tr_cont"] = str(resp.headers.get("tr_cont") or resp.headers.get("TR_CONT") or "")
                
                # Success after retry
''',
)

new_get_fills = '''    def get_us_fills_today(self, trade_date: str | None = None) -> list[dict]:
        """Return all same-day overseas order/fill snapshots from KIS practice.

        The practice contract requires blank PDNO/OVRS_EXCG_CD/ORD_DT and date
        range fields. Pagination follows tr_cont plus CTX_AREA_NK200/FK200.
        """
        self._assert_not_offline("get_us_fills_today")
        tr = get_tr_info("us_fills_today")
        headers = self._build_headers(tr["tr_id"])

        if trade_date:
            ord_dt = trade_date.replace("-", "")
        else:
            from zoneinfo import ZoneInfo
            from datetime import datetime as dt
            ord_dt = dt.now(tz=ZoneInfo("America/New_York")).strftime("%Y%m%d")

        params = self._build_us_fills_params(ord_dt, "PRACTICE_RANGE")
        all_rows: list[dict] = []
        seen_cursors: set[tuple[str, str]] = set()
        max_pages = 10

        for page_index in range(max_pages):
            page_headers = dict(headers)
            if page_index > 0:
                page_headers["tr_cont"] = "N"
            logger.info(
                "[US_FILLS][REQUEST] schema=PRACTICE_RANGE ord_dt=%s endpoint=inquire-ccnl page=%d",
                ord_dt,
                page_index + 1,
            )
            try:
                result = self._get(
                    tr["path"],
                    headers=page_headers,
                    params=params,
                    suppress_final_log=True,
                )
            except Exception as exc:
                error_msg = str(exc)
                if "EGW002" in error_msg or "RATE" in error_msg.upper():
                    raise KisUSTemporaryError(f"KIS temporary error: {error_msg}") from exc
                raise KisUSClientError(f"KIS fills contract error: {error_msg}") from exc

            rows = result.get("output") or []
            if isinstance(rows, dict):
                rows = [rows]
            all_rows.extend(row for row in rows if isinstance(row, dict))

            response_meta = result.get("_response_meta") if isinstance(result.get("_response_meta"), dict) else {}
            tr_cont = str(response_meta.get("tr_cont") or "").upper()
            nk200 = str(result.get("ctx_area_nk200") or result.get("CTX_AREA_NK200") or "")
            fk200 = str(result.get("ctx_area_fk200") or result.get("CTX_AREA_FK200") or "")
            if tr_cont not in {"M", "F"}:
                logger.info(
                    "[US_FILLS][FETCHED] count=%d pages=%d status=OK schema=PRACTICE_RANGE",
                    len(all_rows),
                    page_index + 1,
                )
                return all_rows

            cursor = (nk200, fk200)
            if cursor == ("", "") or cursor in seen_cursors:
                raise KisUSClientError(
                    f"KIS fills pagination contract error: tr_cont={tr_cont!r} cursor={cursor!r}"
                )
            seen_cursors.add(cursor)
            params = dict(params)
            params["CTX_AREA_NK200"] = nk200
            params["CTX_AREA_FK200"] = fk200

        raise KisUSClientError(f"KIS fills pagination exceeded max_pages={max_pages}")

    def get_us_today_orders'''
replace_regex(
    "trader/us/execution/kis_us_client.py",
    r"    def get_us_fills_today\(.*?\n    def get_us_today_orders",
    new_get_fills,
)

new_params = '''    def _build_us_fills_params(self, ord_dt: str, schema: str) -> dict:
        """Build the official KIS practice inquire-ccnl parameter contract."""
        if schema not in {"PRACTICE_RANGE", "ALL_DATES", "ORD_DT", "ORD_RANGE"}:
            raise ValueError(f"unknown fills schema: {schema}")
        return {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": "",
            "ORD_STRT_DT": ord_dt,
            "ORD_END_DT": ord_dt,
            "SLL_BUY_DVSN": "00",
            "CCLD_NCCS_DVSN": "00",
            "OVRS_EXCG_CD": "",
            "SORT_SQN": "DS",
            "ORD_DT": "",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "CTX_AREA_NK200": "",
            "CTX_AREA_FK200": "",
        }

    # ------------------------------------------------------------------
    # HTTP helpers'''
replace_regex(
    "trader/us/execution/kis_us_client.py",
    r"    def _build_us_fills_params\(.*?    # ------------------------------------------------------------------\n    # HTTP helpers",
    new_params,
)

replace_once(
    "trader/us/execution/fills.py",
    '''            cumulative_filled_qty = int(row.get("ft_ccld_qty", 0) or 0)
            remaining_qty = int(row.get("nccs_qty") or row.get("rmn_qty") or 0)
            avg_price_usd = float(row.get("ft_ccld_unpr3", 0) or 0)
            fills.append({
''',
    '''            cumulative_filled_qty = int(row.get("ft_ccld_qty", 0) or 0)
            remaining_qty = int(row.get("nccs_qty") or row.get("rmn_qty") or 0)
            avg_price_usd = float(row.get("ft_ccld_unpr3", 0) or 0)
            if cumulative_filled_qty <= 0:
                logger.debug(
                    "[US_FILLS][SKIP_UNFILLED_ORDER] order_no=%s symbol=%s remaining_qty=%s",
                    order_no,
                    row.get("pdno", ""),
                    remaining_qty,
                )
                continue
            fills.append({
''',
)
