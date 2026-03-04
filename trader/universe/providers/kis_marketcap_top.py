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
        "practice": os.getenv("KIS_TR_ID_MARKETCAP_TOP", "FHPST01700000"),
        "real": os.getenv("KIS_TR_ID_MARKETCAP_TOP_REAL", "FHPST01700000"),
    }
    DEFAULT_PARAMS = {
        # 조건 스크린 코드/정렬 기준은 KIS 포털의 "국내주식 시가총액 상위" 기본값을 사용한다.
        # 필요 시 환경 변수 혹은 코드 한 곳만 수정하면 되도록 상수로 모아둔다.
        "fid_rank_sort_cls_code": os.getenv("KIS_MKTCAP_SORT_CODE", "1"),
        "fid_cond_scr_div_code": os.getenv("KIS_MKTCAP_SCREEN_CODE", "20171"),
        "fid_input_iscd": os.getenv("KIS_MKTCAP_INPUT_ISCD", "0000"),
    }
    MARKET_CODE_MAP = {"KOSPI": "J", "KOSDAQ": "Q"}

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
        return self.MARKET_CODE_MAP.get(market.upper(), "J")

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

        tr_id = self._pick_tr_id()
        market_code = self._market_code(market)
        # [FIX] FID_INPUT_CNT_1 필수 파라미터 추가 (시가총액 상위 조회 개수)
        params = {
            **self.params,
            "fid_cond_mrkt_div_code": market_code,
            "fid_input_cnt_1": str(n),
        }

        try:
            headers = self.kis._headers(tr_id)  # type: ignore[attr-defined]
            url = f"{API_BASE_URL}{self.endpoint}"
            self.kis._limiter.wait("marketcap-top")  # type: ignore[attr-defined]
            resp = self.kis._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 7.0))  # type: ignore[attr-defined]
            data = resp.json()
        except Exception as exc:
            logger.warning("[KIS][MKTCAP][FAIL] env=%s market=%s err=%s", self.env, market, exc)
            return []

        output_rows = self._extract_rows(data if isinstance(data, dict) else {})
        codes = self._normalize_codes(output_rows)
        if not codes:
            logger.warning(
                "[KIS][MKTCAP][EMPTY] env=%s market=%s rt_cd=%s msg=%s",
                self.env,
                market,
                data.get("rt_cd") if isinstance(data, dict) else None,
                data.get("msg1") if isinstance(data, dict) else None,
            )
            return []
        return codes[: max(0, int(n))]

    def get_marketcap_top_with_meta(self, market: str, n: int) -> list[dict]:
        """
        get_marketcap_top과 동일하지만 이름 메타를 포함한다.
        실패/빈 응답 시 예외를 던져 상위 호출자가 사유를 명확히 기록하도록 한다.
        """
        self.validate_params(market, n)
        tr_id = self._pick_tr_id()
        market_code = self._market_code(market)
        params = {**self.params, "fid_cond_mrkt_div_code": market_code}

        headers = self.kis._headers(tr_id)  # type: ignore[attr-defined]
        url = f"{API_BASE_URL}{self.endpoint}"
        self.kis._limiter.wait("marketcap-top")  # type: ignore[attr-defined]
        resp = self.kis._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 7.0))  # type: ignore[attr-defined]
        data = resp.json()
        output_rows = self._extract_rows(data if isinstance(data, dict) else {})
        normalized = self._normalize_rows(output_rows)
        if not normalized:
            raise RuntimeError(
                f"marketcap_top empty market={market} rt_cd={data.get('rt_cd') if isinstance(data, dict) else None} msg={data.get('msg1') if isinstance(data, dict) else None}"
            )
        limit = max(0, int(n))
        return normalized[:limit]
