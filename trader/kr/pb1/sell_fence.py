from __future__ import annotations


class NoSellableStickyFence:
    def __init__(self) -> None:
        self._blocked: dict[tuple[str, str, str], str] = {}

    def mark(self, *, trade_date: str, symbol: str, snapshot_version: str) -> None:
        self._blocked[(trade_date, str(symbol).zfill(6), "SELL")] = str(snapshot_version)

    def blocked(self, *, trade_date: str, symbol: str, snapshot_version: str,
                orderable_qty: int) -> bool:
        key = (trade_date, str(symbol).zfill(6), "SELL")
        old_version = self._blocked.get(key)
        if orderable_qty > 0 and old_version != str(snapshot_version):
            self._blocked.pop(key, None)
            return False
        return old_version == str(snapshot_version)


NO_SELLABLE_STICKY = NoSellableStickyFence()
