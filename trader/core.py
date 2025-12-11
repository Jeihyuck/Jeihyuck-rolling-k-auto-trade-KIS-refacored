# -*- coding: utf-8 -*-
"""trader 모듈의 공용 로직 집합을 모듈 단위로 재구성."""
from __future__ import annotations

from typing import Dict, Optional

from rolling_k_auto_trade_api.best_k_meta_strategy import get_kosdaq_top_n

from . import config
from .kis_wrapper import DataEmptyError, DataShortError, KisAPI, NetTemporaryError, append_fill
from .metrics import vwap_guard
from .report_ceo import ceo_report

from . import core_constants
from . import core_utils
from . import signals
from . import execution
from .core_constants import *  # noqa: F401,F403
from .core_utils import *  # noqa: F401,F403
from .execution import *  # noqa: F401,F403
from .signals import *  # noqa: F401,F403

try:
    from .rkmax_utils import blend_k, recent_features
except Exception:
    def blend_k(k_month: float, day: int, atr20: Optional[float], atr60: Optional[float]) -> float:  # type: ignore[misc]
        return float(k_month) if k_month is not None else 0.5

    def recent_features(kis, code: str) -> Dict[str, Optional[float]]:  # type: ignore[misc]
        return {"atr20": None, "atr60": None}


__all__ = (
    [
        "KisAPI",
        "NetTemporaryError",
        "DataEmptyError",
        "DataShortError",
        "append_fill",
        "blend_k",
        "recent_features",
        "ceo_report",
        "get_kosdaq_top_n",
        "vwap_guard",
    ]
    + core_constants.__all__
    + core_utils.__all__
    + signals.__all__
    + execution.__all__
)

