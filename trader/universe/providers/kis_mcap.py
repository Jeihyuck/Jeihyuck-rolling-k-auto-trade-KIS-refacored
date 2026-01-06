from __future__ import annotations

import logging
import os
from typing import Iterable

from settings import API_BASE_URL
from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)


class KISMcapProvider:
    """KIS 랭킹 API 기반 시가총액 상위 종목 제공자."""

    DEFAULT_ENDPOINT = "/uapi/domestic-stock/v1/ranking/market-cap"
    DEFAULT_TR_IDS = {
        "practice": os.getenv("KIS_TR_ID_MARKETCAP_TOP", "FHPST01700000"),
        "real": os.getenv("KIS_TR_ID_MARKETCAP_TOP_REAL", "FHPST01700000"),
    }
    DEFAULT_PARAMS = {
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

    @staticmethod
    def _normalize_rows(rows: Iterable[dict]) -> list[dict]:
        normalized: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            code = None
            for key in ("stck_shrn_iscd", "mksc_shrn_iscd", "pdno", "code", "CODE"):
                if key in row and row.get(key):
                    code = str(row.get(key)).strip()
                    break
            if not code:
                continue
            code = code.zfill(6)
            if code in seen:
                continue
            seen.add(code)
            name = row.get("hts_kor_isnm") or row.get("mksc_kor_isnm") or row.get("hname") or row.get("prdt_name") or row.get("name")
            normalized.append({"code": code, "name": str(name).strip() if name else None})
        return normalized

    @staticmethod
    def _extract_rows(data: dict) -> list[dict]:
        buckets = []
        for key in ("output", "output1", "output2"):
            out = data.get(key)
            if isinstance(out, list):
                buckets.extend(out)
            elif isinstance(out, dict):
                buckets.append(out)
        return buckets

    def get_top_with_meta(self, market: str, n: int) -> list[dict]:
        """
        시장별 시가총액 상위 목록을 코드/이름 메타와 함께 반환한다.
        실패 시 빈 리스트를 반환하고 WARN 로그만 남긴다.
        """
        tr_id = self._pick_tr_id()
        if not tr_id:
            logger.warning("[KIS][MCAP][TR_MISSING] env=%s market=%s", self.env, market)
            return []

        market_code = self._market_code(market)
        params = {**self.params, "fid_cond_mrkt_div_code": market_code}

        try:
            headers = self.kis._headers(tr_id)  # type: ignore[attr-defined]
            url = f"{API_BASE_URL}{self.endpoint}"
            self.kis._limiter.wait("marketcap-top")  # type: ignore[attr-defined]
            resp = self.kis._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 7.0))  # type: ignore[attr-defined]
            data = resp.json()
        except Exception as exc:
            logger.warning("[KIS][MCAP][FAIL] env=%s market=%s err=%s", self.env, market, exc)
            return []

        output_rows = self._extract_rows(data if isinstance(data, dict) else {})
        normalized = self._normalize_rows(output_rows)
        if not normalized:
            logger.warning(
                "[KIS][MCAP][EMPTY] env=%s market=%s rt_cd=%s msg=%s",
                self.env,
                market,
                data.get("rt_cd") if isinstance(data, dict) else None,
                data.get("msg1") if isinstance(data, dict) else None,
            )
            return []
        limit = max(0, int(n))
        return normalized[:limit]

    def get_top_codes(self, market: str, n: int) -> list[str]:
        return [row["code"] for row in self.get_top_with_meta(market, n)]


def get_kospi_top_mcap(n: int = 100, *, kis: KisAPI | None = None, env: str | None = None) -> list[str]:
    provider = KISMcapProvider(kis=kis, env=env)
    return provider.get_top_codes("KOSPI", n)


def get_kosdaq_top_mcap(n: int = 100, *, kis: KisAPI | None = None, env: str | None = None) -> list[str]:
    provider = KISMcapProvider(kis=kis, env=env)
    return provider.get_top_codes("KOSDAQ", n)
