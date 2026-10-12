from __future__ import annotations

import logging
import os
from typing import Iterable

from settings import API_BASE_URL
from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)


class KISMarketcapTopProvider:
    DEFAULT_ENDPOINT = "/uapi/domestic-stock/v1/ranking/market-cap"
    DEFAULT_TR_IDS = {
        "practice": os.getenv("KIS_TR_ID_MARKETCAP_TOP", "FHPST01740000"),
        "real": os.getenv("KIS_TR_ID_MARKETCAP_TOP_REAL", "FHPST01740000"),
    }
    # KIS domestic market-cap rank [091] (FHPST01740000) request contract.
    # FID_RANK_SORT_CLS_CODE belongs to a *different* ranking API, namely
    # fluctuation FHPST01700000. Never mix their query contracts.
    # Blank filters mean no extra filter; existing selection policy unchanged.
    DEFAULT_PARAMS = {
        "FID_COND_SCR_DIV_CODE": os.getenv("KIS_MKTCAP_SCREEN_CODE", "20174"),
        "FID_DIV_CLS_CODE": os.getenv("KIS_MKTCAP_DIV_CLS_CODE", "0"),
        # Selected explicitly by market in _build_params; never submit 0000 for both.
        "FID_TRGT_CLS_CODE": os.getenv("KIS_MKTCAP_TARGET_CLS_CODE", "0"),
        "FID_TRGT_EXLS_CLS_CODE": os.getenv("KIS_MKTCAP_TARGET_EXCLUDE_CLS_CODE", "0"),
        "FID_INPUT_PRICE_1": os.getenv("KIS_MKTCAP_INPUT_PRICE_1", ""),
        "FID_INPUT_PRICE_2": os.getenv("KIS_MKTCAP_INPUT_PRICE_2", ""),
        "FID_VOL_CNT": os.getenv("KIS_MKTCAP_VOL_CNT", ""),
    }
    MARKET_CODE_MAP = {"KOSPI": "0001", "KOSDAQ": "1001"}

    def __init__(
        self,
        *,
        kis: KisAPI | None = None,
        env: str | None = None,
        endpoint: str | None = None,
        tr_ids: dict[str, str] | None = None,
    ) -> None:
        self.kis = kis or KisAPI()
        self.env = (env or self.kis.env or "practice").lower()
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.tr_ids = tr_ids or dict(self.DEFAULT_TR_IDS)
        self.params = dict(self.DEFAULT_PARAMS)

    def _pick_tr_id(self) -> str | None:
        return self.tr_ids.get(self.env) or self.tr_ids.get("practice")

    def _market_code(self, market: str) -> str:
        # The API's market division remains J for both KOSPI and KOSDAQ.
        return "J"

    def _build_params(self, market: str, n: int) -> dict:
        # The market-cap ranking API has no FID_INPUT_CNT_1 field.  The provider
        # limits the normalized response locally after the broker request.
        return {
            **self.params,
            "FID_COND_MRKT_DIV_CODE": self._market_code(market),
            "FID_INPUT_ISCD": self.MARKET_CODE_MAP[market.upper()],
        }

    def validate_params(self, market: str, n: int) -> None:
        if market.upper() not in self.MARKET_CODE_MAP:
            raise ValueError(f"unsupported market: {market}")
        if n <= 0:
            raise ValueError(f"n must be positive: {n}")
        if not self._pick_tr_id():
            raise ValueError(f"TR id missing for env={self.env}")

    @staticmethod
    def _normalize_rows(rows: Iterable[dict]) -> list[dict]:
        normalized: list[dict] = []
        for row in rows:
            code = None
            for key in ("stck_shrn_iscd", "mksc_shrn_iscd", "pdno", "code", "CODE"):
                if key in row and row.get(key):
                    code = str(row.get(key)).strip()
                    break
            if not code:
                continue
            name = row.get("hts_kor_isnm") or row.get("mksc_kor_isnm") or row.get("hname") or row.get("prdt_name") or row.get("name")
            normalized.append({"code": code.zfill(6), "name": str(name).strip() if name else None})
        # preserve order while removing duplicates
        seen: set[str] = set()
        uniq: list[dict] = []
        for row in normalized:
            code = row["code"]
            if code in seen:
                continue
            seen.add(code)
            uniq.append(row)
        return uniq

    @staticmethod
    def _normalize_codes(rows: Iterable[dict]) -> list[str]:
        codes: list[str] = []
        for row in rows:
            code = None
            for key in ("stck_shrn_iscd", "mksc_shrn_iscd", "pdno", "code", "CODE"):
                if key in row and row.get(key):
                    code = str(row.get(key)).strip()
                    break
            if not code:
                continue
            codes.append(code.zfill(6))
        # preserve order while removing duplicates
        seen: set[str] = set()
        uniq: list[str] = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                uniq.append(c)
        return uniq

    def _extract_rows(self, data: dict) -> list[dict]:
        buckets = []
        for key in ("output", "output1", "output2"):
            out = data.get(key)
            if isinstance(out, list):
                buckets.extend(out)
            elif isinstance(out, dict):
                buckets.append(out)
        return buckets

    def _fetch_rank_rows(self, market: str, n: int) -> list[dict]:
        """Use the [091] API with bounded tr_cont pagination.

        Always query the correct market sector; reject nonzero broker rt_cd.
        """
        tr_id = self._pick_tr_id()
        params = self._build_params(market, n)
        base_headers = self.kis._headers(tr_id)  # type: ignore[attr-defined]
        url = f"{API_BASE_URL}{self.endpoint}"
        all_rows: list[dict] = []
        seen: set[str] = set()
        tr_cont = ""
        for page in range(12):
            headers = dict(base_headers)
            if tr_cont:
                headers["tr_cont"] = tr_cont
            self.kis._limiter.wait("marketcap-top")  # type: ignore[attr-defined]
            resp = self.kis._safe_request(  # type: ignore[attr-defined]
                "GET", url, headers=headers, params=params, timeout=(3.0, 7.0),
            )
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"marketcap_top invalid_json market={market} page={page}")
            rt_cd = str(data.get("rt_cd") or "").strip()
            if rt_cd and rt_cd != "0":
                raise RuntimeError(
                    f"marketcap_top broker_reject market={market} page={page} "
                    f"rt_cd={rt_cd} msg={data.get('msg1')}"
                )
            before = len(seen)
            for row in self._extract_rows(data):
                if not isinstance(row, dict):
                    continue
                code = str(row.get("mksc_shrn_iscd") or row.get("stck_shrn_iscd")
                           or row.get("pdno") or row.get("code") or "").strip()
                if code and code not in seen:
                    seen.add(code)
                    all_rows.append(row)
            response_headers = getattr(resp, "headers", {}) or {}
            continue_flag = str(response_headers.get("tr_cont") or "").strip().upper()
            logger.info(
                "[KIS][MKTCAP][PAGE] market=%s page=%s tr_id=%s fresh=%s total=%s next=%s",
                market, page + 1, tr_id, len(seen) - before, len(all_rows), continue_flag,
            )
            if len(all_rows) >= n or continue_flag != "M":
                break
            if len(seen) == before:
                raise RuntimeError(f"marketcap_top pagination_stalled market={market} page={page}")
            tr_cont = "N"
        else:
            raise RuntimeError(f"marketcap_top page_limit_reached market={market} n={n}")
        return all_rows


    def get_marketcap_top(self, market: str, n: int) -> list[str]:
        """
        KIS "국내주식 시가총액 상위" 호출로 시장별 TopN 종목코드 반환.
        실패 시 예외 대신 빈 리스트를 반환하고 WARN 로그만 남긴다.
        """
        try:
            self.validate_params(market, n)
        except Exception as exc:
            logger.warning("[KIS][MKTCAP][VALIDATION_FAIL] env=%s market=%s err=%s", self.env, market, exc)
            return []

        try:
            output_rows = self._fetch_rank_rows(market, n)
        except Exception as exc:
            logger.warning("[KIS][MKTCAP][FAIL] env=%s market=%s err=%s", self.env, market, exc)
            return []

        codes = self._normalize_codes(output_rows)
        if not codes:
            logger.warning(
                "[KIS][MKTCAP][EMPTY] env=%s market=%s rt_cd=%s msg=%s",
                self.env,
                market,
                "0",
                "NO_RANK_ROWS",
            )
            return []
        return codes[: max(0, int(n))]

    def get_marketcap_top_with_meta(self, market: str, n: int) -> list[dict]:
        """
        get_marketcap_top과 동일하지만 이름 메타를 포함한다.
        실패/빈 응답 시 예외를 던져 상위 호출자가 사유를 명확히 기록하도록 한다.
        """
        self.validate_params(market, n)
        output_rows = self._fetch_rank_rows(market, n)
        normalized = self._normalize_rows(output_rows)
        if not normalized:
            raise RuntimeError(
                f"marketcap_top empty market={market} rt_cd=0 msg=NO_RANK_ROWS"
            )
        limit = max(0, int(n))
        return normalized[:limit]
