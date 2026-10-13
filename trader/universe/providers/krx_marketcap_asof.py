"""Strict, date-addressable KRX market-cap universe for KR PREP.

Unlike the KIS ranking endpoint (one 30-row current ranking page), this source
requests the last completed KRX session explicitly. Never roll backward to
another session, infer venues from ticker order, or convert failed data into a
success snapshot. Actual publication availability remains a runtime condition.
"""
from __future__ import annotations

import logging
import re
from datetime import date

import pandas as pd
from pykrx.stock import get_market_ticker_list

from trader.universe.providers.krx_provider import fetch_with_rollback

logger = logging.getLogger(__name__)
_CODE = re.compile(r"^[0-9]{6}$")
_CAP_COLUMNS = ("시가총액", "시가 총액", "MKT_CAP")
_NAME_COLUMNS = ("종목명", "Name", "name")


class KRXAsOfMarketcapProvider:
    """Return top-N from a dated KRX query, with zero date rollback."""

    def get_marketcap_top_with_meta(self, market: str, as_of_date: str, n: int) -> list[dict]:
        venue = str(market).strip().upper()
        if venue not in {"KOSPI", "KOSDAQ"}:
            raise ValueError(f"krx_asof_invalid_market:{market}")
        requested = date.fromisoformat(str(as_of_date))
        if int(n) < 1:
            raise ValueError("krx_asof_invalid_target")

        # max_rollback_days=0 is essential: a past session cannot silently
        # be stored under the requested date on PREP.
        df, used_date, reason = fetch_with_rollback(
            venue, requested, max_rollback_days=0,
        )
        if used_date != requested or reason != "ok":
            raise RuntimeError(
                f"krx_asof_date_mismatch requested={requested} actual={used_date} reason={reason}"
            )
        if df is None or df.empty:
            raise RuntimeError(f"krx_asof_empty market={venue} as_of={requested}")
        # Some adapters provide an explicit data date. If present, it must
        # agree with the requested session; never silently override it.
        published_date = (
            df.attrs.get("actual_as_of") or df.attrs.get("as_of") or df.attrs.get("date")
        )
        if published_date is not None:
            try:
                normalized_date = date.fromisoformat(str(published_date).replace("/", "-")[:10])
            except ValueError as exc:
                raise RuntimeError("krx_asof_unparseable_provider_date") from exc
            if normalized_date != requested:
                raise RuntimeError(
                    f"krx_asof_provider_date_mismatch requested={requested} actual={normalized_date}"
                )

        # Confirm venue membership from the same *dated* market listing API,
        # not from the order of an unversioned local seed or the request label.
        listed_codes = {
            str(code).strip().zfill(6)
            for code in get_market_ticker_list(requested.strftime("%Y%m%d"), market=venue)
        }
        if len(listed_codes) < int(n):
            raise RuntimeError(
                f"krx_asof_venue_listing_incomplete market={venue} "
                f"listed={len(listed_codes)} required={n}"
            )

        cap_col = next((c for c in _CAP_COLUMNS if c in df.columns), None)
        if cap_col is None:
            raise RuntimeError(f"krx_asof_missing_market_cap market={venue}")
        name_col = next((c for c in _NAME_COLUMNS if c in df.columns), None)
        caps = pd.to_numeric(df[cap_col], errors="coerce")
        rows: list[dict] = []
        seen: set[str] = set()
        # pykrx uses 6-digit stock codes as the DataFrame index.
        for code_raw, cap, idx in zip(df.index, caps, range(len(df))):
            code = str(code_raw).strip()
            if (not _CODE.fullmatch(code) or code not in listed_codes
                    or code in seen or pd.isna(cap) or float(cap) <= 0):
                continue
            seen.add(code)
            name = str(df.iloc[idx][name_col]).strip() if name_col else ""
            rows.append({
                "code": code, "name": name or None, "market": venue,
                "market_cap": float(cap), "actual_as_of": requested.isoformat(),
            })
        rows.sort(key=lambda item: item["market_cap"], reverse=True)
        if len(rows) < int(n):
            raise RuntimeError(
                f"krx_asof_insufficient market={venue} as_of={requested} "
                f"valid={len(rows)} required={n}"
            )
        logger.info(
            "[KRX][MKTCAP][ASOF_VERIFIED] market=%s requested=%s "
            "used=%s provider_date=%s rows=%s selected=%s",
            venue, requested, used_date, published_date, len(rows), n,
        )
        return rows[:int(n)]
