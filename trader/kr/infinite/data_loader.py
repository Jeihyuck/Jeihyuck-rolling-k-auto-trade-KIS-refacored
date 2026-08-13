from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import pandas as pd
from sqlalchemy import text


@dataclass(frozen=True)
class DataQualityReport:
    source: str; start: str; end: str; rows: int; checksum: str
    adjusted_status: str; corporate_action_status: str


def load_and_validate_csv(path: str | Path, *, source: str, adjusted_status: str = "UNVERIFIED",
                          corporate_action_status: str = "UNVERIFIED") -> tuple[pd.DataFrame, DataQualityReport]:
    raw = Path(path).read_bytes(); frame = pd.read_csv(path)
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(frame): raise ValueError("missing OHLCV columns")
    frame["date"] = pd.to_datetime(frame["date"])
    if not frame["date"].is_monotonic_increasing or frame["date"].duplicated().any(): raise ValueError("dates invalid")
    if ((frame[["open","high","low","close"]] <= 0).any().any() or (frame.volume < 0).any()
            or (frame.high < frame[["open","low","close"]].max(axis=1)).any()
            or (frame.low > frame[["open","high","close"]].min(axis=1)).any()): raise ValueError("OHLCV logic invalid")
    report = DataQualityReport(source, str(frame.date.iloc[0].date()), str(frame.date.iloc[-1].date()), len(frame),
                               hashlib.sha256(raw).hexdigest(), adjusted_status, corporate_action_status)
    return frame, report


def load_price_history(*, engine=None, kis_provider=None, csv_path=None, start="2010-02-22", end=None,
                       adjusted_status="UNVERIFIED", corporate_action_status="UNVERIFIED"):
    """Load 122630 in the mandated source order, paginating the KIS fallback."""
    if engine is not None:
        with engine.connect() as conn:
            rows=conn.execute(text("""SELECT date, open, high, low, close, volume FROM price_daily
              WHERE code='122630' AND date>=:start AND (:end IS NULL OR date<=CAST(:end AS DATE)) ORDER BY date"""),
              {"start":start,"end":end}).mappings().all()
        if rows:
            return validate_frame(pd.DataFrame(rows),source="price_daily",adjusted_status=adjusted_status,
                                  corporate_action_status=corporate_action_status)
    if kis_provider is not None:
        # Reuse the repository's OHLCVProvider contract. KISOHLCVProvider itself
        # performs DB-first loading and KIS period backfill; do not invent a cursor API.
        result=kis_provider.get_ohlcv("122630",6000,purpose="kr_infinite_replay",
                                      usage_context="validation",allow_long_fetch=True)
        frame=getattr(result,"df",result)
        if isinstance(frame,pd.DataFrame) and not frame.empty:
            return validate_frame(frame,source=str((getattr(result,"meta",{}) or {}).get("source") or "KIS_OHLCVProvider"),adjusted_status=adjusted_status,
                                  corporate_action_status=corporate_action_status)
    if csv_path:
        return load_and_validate_csv(csv_path,source="verified_user_csv",adjusted_status=adjusted_status,
                                     corporate_action_status=corporate_action_status)
    raise RuntimeError("no authoritative 122630 price source available")


def validate_frame(frame: pd.DataFrame, *, source: str, adjusted_status: str,
                   corporate_action_status: str):
    temp=frame.copy(); temp["date"]=pd.to_datetime(temp["date"])
    required={"date","open","high","low","close","volume"}
    if not required.issubset(temp): raise ValueError("missing OHLCV columns")
    if not temp.date.is_monotonic_increasing or temp.date.duplicated().any(): raise ValueError("dates invalid")
    if ((temp[["open","high","low","close"]]<=0).any().any() or (temp.volume<0).any()
        or (temp.high<temp[["open","low","close"]].max(axis=1)).any()
        or (temp.low>temp[["open","high","close"]].min(axis=1)).any()): raise ValueError("OHLCV logic invalid")
    canonical=temp[list(sorted(required))].to_csv(index=False).encode()
    report=DataQualityReport(source,str(temp.date.iloc[0].date()),str(temp.date.iloc[-1].date()),len(temp),
        hashlib.sha256(canonical).hexdigest(),adjusted_status,corporate_action_status)
    return temp,report
