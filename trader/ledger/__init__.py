# Make trader.ledger a regular package (avoid namespace ambiguity)
from .event_types import (
    LedgerEvent,
    new_order_intent,
    new_order_ack,
    new_fill,
    new_exit_intent,
    new_error,
    new_unfilled,
)
from .store import LedgerStore

# Backward-compat shim: if legacy module existed, keep old API accessible.
try:
    from trader.ledger_legacy import *  # noqa: F401,F403
except Exception:
    pass
