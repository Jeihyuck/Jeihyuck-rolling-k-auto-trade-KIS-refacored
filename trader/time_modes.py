import logging
from dataclasses import dataclass
from datetime import datetime, time as dtime


@dataclass
class TimeModeState:
    """Legacy time-mode flags used by the orchestrator loop.

    The names mirror the original inline definition inside ``trader.py`` so
    existing conditionals (intraday entry gates, close-bet scanning/entry
    checks) keep working without attribute changes after the module split.
    """

    mode: str
    allow_intraday_entries: bool
    allow_close_bet_scan: bool
    allow_close_bet_entry: bool


class TimeModeController:
    def __init__(
        self,
        active_start: dtime,
        full_active_end: dtime,
        close_bet_prep: dtime,
        close_bet_entry: dtime,
        cutoff: dtime,
        market_close: dtime,
    ) -> None:
        self.active_start = active_start
        self.full_active_end = full_active_end
        self.close_bet_prep = close_bet_prep
        self.close_bet_entry = close_bet_entry
        self.cutoff = cutoff
        self.market_close = market_close
        self._last_mode: str = ""

    def evaluate(self, now_dt: datetime) -> TimeModeState:
        now_time = now_dt.time()
        if now_time >= self.market_close:
            mode = "shutdown"
            return TimeModeState(mode, False, False, False)
        if now_time >= self.cutoff:
            mode = "cutoff"
            return TimeModeState(mode, False, False, True)
        if now_time >= self.close_bet_entry:
            mode = "close_bet_entry"
            return TimeModeState(mode, False, False, True)
        if now_time >= self.close_bet_prep:
            mode = "light_active"
            return TimeModeState(mode, False, True, False)
        if now_time >= self.active_start:
            mode = "full_active"
            return TimeModeState(mode, True, False, False)
        mode = "pre_open"
        return TimeModeState(mode, False, False, False)

    def log_if_changed(self, logger: logging.Logger, state: TimeModeState) -> None:
        if state.mode != self._last_mode:
            self._last_mode = state.mode
            if state.mode == "full_active":
                logger.info(
                    f"[TIME-MODE] FULL_ACTIVE ({self.active_start.strftime('%H:%M')}~{self.full_active_end.strftime('%H:%M')})"
                )
            elif state.mode == "light_active":
                logger.info(
                    f"[TIME-MODE] LIGHT_ACTIVE ({self.close_bet_prep.strftime('%H:%M')}~{self.close_bet_entry.strftime('%H:%M')}) 신규 진입 제한, 종가 베팅 준비"
                )
            elif state.mode == "close_bet_entry":
                logger.info(
                    f"[TIME-MODE] CLOSE_BET_ENTRY ({self.close_bet_entry.strftime('%H:%M')}~{self.cutoff.strftime('%H:%M')})"
                )
            elif state.mode == "shutdown":
                logger.info(
                    f"[TIME-MODE] SHUTDOWN 준비 (커트오프 {self.cutoff.strftime('%H:%M')} 도달 예정)"
                )
            else:
                logger.info(
                    "[TIME-MODE] PRE_OPEN (장 시작 전 준비)"
                )
