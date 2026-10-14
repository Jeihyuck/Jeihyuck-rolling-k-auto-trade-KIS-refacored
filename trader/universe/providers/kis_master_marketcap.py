"""KIS official daily KOSPI/KOSDAQ master-file market-cap universe.

Based on Korea Investment & Securities official open-trading-api stock master
parsers: `stocks_info/kis_kospi_code_mst.py` and `kis_kosdaq_code_mst.py`.
The cap is a nine-character numeric field immediately before the final 3+1+1+1
bytes in both fixed-width tails. The KOSDAQ field is labelled previous-day
market capitalisation (100M KRW). KOSPI labels its field market cap.

These are DAILY SNAPSHOTS, not a historical data API. Strict PREP can use
only a new, independently date-stamped morning publication for yesterday's
completed session. Otherwise fail closed; a download's Date header does not
prove its underlying content is current.
"""
from __future__ import annotations

from datetime import date, datetime, time
from email.utils import parsedate_to_datetime
import hashlib
from io import BytesIO
import logging
import re
from zipfile import ZipFile

import requests
from zoneinfo import ZoneInfo

from trader.time_utils import now_kst, resolve_prev_krx_trading_day

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
MASTER_BASE = "https://new.real.download.dws.co.kr/common/master"
LAYOUT = {
    "KOSPI": ("kospi_code.mst", 228),
    "KOSDAQ": ("kosdaq_code.mst", 222),
}
CODE_RE = re.compile(r"^[0-9]{6}$")


def parse_master(raw: bytes, market: str) -> list[dict]:
    """Parse the official CP949 master by BYTE offsets; no pandas coercion."""
    name, tail_len = LAYOUT[market]
    rows: list[dict] = []
    seen: set[str] = set()
    if not raw or len(raw) > 80 * 1024 * 1024:
        raise RuntimeError("kis_master_invalid_uncompressed_size")
    # KIS's reference parser opens the WHOLE file with encoding=cp949 first,
    # then slices 228/222 characters from each decoded line. A bytewise split
    # can sever a Korean multibyte name (observed on the live KOSPI master).
    try:
        decoded = raw.decode("cp949")
    except UnicodeError as exc:
        raise RuntimeError(f"kis_master_encoding_error market={market}") from exc
    for line_no, line in enumerate(decoded.splitlines(), start=1):
        if not line.strip():
            continue
        if len(line) <= 21 + tail_len:
            raise RuntimeError(f"kis_master_truncated market={market} line={line_no}")
        prefix, tail = line[:-tail_len], line[-tail_len:]
        short = prefix[:9].strip()
        code = short[1:] if len(short) == 7 and short[0] == "A" else short
        name_kr = prefix[21:].strip()
        cap_string = tail[-15:-6].strip()
        if not CODE_RE.fullmatch(code):
            continue
        if code in seen:
            raise RuntimeError(f"kis_master_duplicate_code market={market} code={code}")
        seen.add(code)
        if not cap_string.isdigit():
            continue
        market_cap = int(cap_string)
        if market_cap <= 0:
            continue
        rows.append({"code": code, "name": name_kr or None,
                     "market": market, "market_cap": market_cap})
    if len(rows) < 150:
        raise RuntimeError(f"kis_master_insufficient_valid market={market} rows={len(rows)}")
    rows.sort(key=lambda row: (-row["market_cap"], row["code"]))
    return rows


class KISMasterMarketcapProvider:
    """Official KIS master downloads, no KIS quote/ranking rate-budget."""
    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()

    def _get_market(self, market: str) -> tuple[list[dict], date, str]:
        if market not in LAYOUT:
            raise ValueError(f"kis_master_invalid_market:{market}")
        file_name, _ = LAYOUT[market]
        url = f"{MASTER_BASE}/{file_name}.zip"
        response = self.session.get(url, timeout=(5, 25),
                                    headers={"User-Agent": "nullim-kis-master/1.0"})
        response.raise_for_status()
        content = response.content
        if not content or len(content) > 25 * 1024 * 1024:
            raise RuntimeError(f"kis_master_zip_size_invalid market={market}")
        last_modified = response.headers.get("Last-Modified")
        if not last_modified:
            raise RuntimeError(f"kis_master_last_modified_missing market={market}")
        try:
            modified = parsedate_to_datetime(last_modified)
            if modified.tzinfo is None:
                raise ValueError("naive Last-Modified")
            published_on = modified.astimezone(KST).date()
        except (ValueError, TypeError) as exc:
            raise RuntimeError(f"kis_master_last_modified_invalid market={market}") from exc
        with ZipFile(BytesIO(content)) as zf:
            if file_name not in zf.namelist():
                raise RuntimeError(f"kis_master_zip_member_missing market={market}")
            info = zf.getinfo(file_name)
            if info.file_size > 80 * 1024 * 1024:
                raise RuntimeError(f"kis_master_zip_uncompressed_oversize market={market}")
            raw = zf.read(file_name)
        rows = parse_master(raw, market)
        digest = hashlib.sha256(content).hexdigest()
        logger.info("[KIS][MASTER][LOADED] market=%s updated_kst=%s codes=%s zip_sha256=%s",
                    market, published_on, len(rows), digest)
        return rows, published_on, digest

    def get_marketcap_top_with_meta(self, market: str, as_of_date: str,
                                    n: int, *, readonly_probe: bool = False) -> list[dict]:
        market = str(market).upper()
        if market not in LAYOUT or int(n) <= 0:
            raise ValueError(f"kis_master_invalid_request:{market}:{n}")
        requested = date.fromisoformat(str(as_of_date))
        now = now_kst()
        # A file fetched after market open/close can contain new intraday data;
        # it cannot safely be relabelled as yesterday's EOD snapshot.
        if not readonly_probe:
            if not (time(5, 0) <= now.time() < time(9, 0)):
                raise RuntimeError("kis_master_prep_morning_window_required")
            if requested != resolve_prev_krx_trading_day(now.date()):
                raise RuntimeError(
                    f"kis_master_asof_mismatch requested={requested} "
                    f"previous_session={resolve_prev_krx_trading_day(now.date())}"
                )
        rows, published_on, digest = self._get_market(market)
        if not readonly_probe and published_on != now.date():
            raise RuntimeError(
                f"kis_master_stale_publication market={market} "
                f"modified={published_on} expected={now.date()}"
            )
        if len(rows) < int(n):
            raise RuntimeError(f"kis_master_insufficient market={market} count={len(rows)} required={n}")
        # The official daily KIS master has no explicit as-of date per record.
        # The prior-session association is a morning-publication rule,
        # NOT a claim that a historical KIS endpoint was queried.
        return [{**row, "actual_as_of": (None if readonly_probe else requested.isoformat()),
                 "source_published_on": published_on.isoformat(),
                 "source_sha256": digest}
                for row in rows[:int(n)]]
