"""
Multi-Strategy Signal Generation
헤지펀드 수준의 멀티 전략 신호 생성
"""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from trader.signals.pullback_signal import pullback_signal
from trader.signals.breakout_signal import breakout_signal
from trader.signals.momentum_signal import momentum_signal


def _load_legacy_signals_module():
    legacy_path = Path(__file__).resolve().parents[1] / "signals.py"
    spec = spec_from_file_location("trader._legacy_signals_module", legacy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load legacy signals module from {legacy_path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_legacy_signals = _load_legacy_signals_module()

_safe_get_price = _legacy_signals._safe_get_price
_fetch_balances = _legacy_signals._fetch_balances
_get_effective_ord_cash = _legacy_signals._get_effective_ord_cash
_get_daily_candles_cached = _legacy_signals._get_daily_candles_cached
_detect_pullback_reversal = _legacy_signals._detect_pullback_reversal
_classify_champion_grade = _legacy_signals._classify_champion_grade
_compute_daily_entry_context = _legacy_signals._compute_daily_entry_context
_compute_intraday_entry_context = _legacy_signals._compute_intraday_entry_context
is_bad_entry = _legacy_signals.is_bad_entry
is_good_entry = _legacy_signals.is_good_entry
evaluate_setup_gate = _legacy_signals.evaluate_setup_gate
evaluate_trigger_gate = _legacy_signals.evaluate_trigger_gate
_get_intraday_1min = _legacy_signals._get_intraday_1min
_compute_vwap_from_1min = _legacy_signals._compute_vwap_from_1min
_compute_intraday_momentum = _legacy_signals._compute_intraday_momentum
is_strong_momentum_vwap = _legacy_signals.is_strong_momentum_vwap
get_20d_return_pct = _legacy_signals.get_20d_return_pct
is_strong_momentum = _legacy_signals.is_strong_momentum
_percentile_rank = _legacy_signals._percentile_rank
_has_bullish_trend_structure = _legacy_signals._has_bullish_trend_structure
_weight_to_qty = _legacy_signals._weight_to_qty
_notional_to_qty = _legacy_signals._notional_to_qty
_get_atr = _legacy_signals._get_atr

__all__ = [
    "pullback_signal",
    "breakout_signal",
    "momentum_signal",
    "_safe_get_price",
    "_fetch_balances",
    "_get_effective_ord_cash",
    "_get_daily_candles_cached",
    "_detect_pullback_reversal",
    "_classify_champion_grade",
    "_compute_daily_entry_context",
    "_compute_intraday_entry_context",
    "is_bad_entry",
    "is_good_entry",
    "evaluate_setup_gate",
    "evaluate_trigger_gate",
    "_get_intraday_1min",
    "_compute_vwap_from_1min",
    "_compute_intraday_momentum",
    "is_strong_momentum_vwap",
    "get_20d_return_pct",
    "is_strong_momentum",
    "_percentile_rank",
    "_has_bullish_trend_structure",
    "_weight_to_qty",
    "_notional_to_qty",
    "_get_atr",
]
