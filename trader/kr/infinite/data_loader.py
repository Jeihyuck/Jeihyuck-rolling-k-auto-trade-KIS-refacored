from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import pandas as pd


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
