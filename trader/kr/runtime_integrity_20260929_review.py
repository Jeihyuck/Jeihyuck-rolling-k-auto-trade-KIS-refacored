"""Final review fences for the Sep-29 KR broker-truth layer.

This module deliberately installs after ``runtime_integrity_20260929``.  The
inner capture wrapper may observe individual KIS daily-ccld pages; this outer
fence guarantees that only a root request whose response itself proves the
query complete may replace the authoritative negative-proof snapshot.
"""
from __future__ import annotations

import functools
from typing import Any, Callable

from trader.kis_wrapper import KisAPI
from trader.kr.runtime_integrity_20260929 import _daily_payload_complete

_INSTALLED = False
_MISSING = object()


def _build_daily_ccld_authority_fence(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, *args: Any, **kwargs: Any):
        previous = getattr(self, "_kr_20260929_daily_ccld_snapshot", _MISSING)
        result = original(self, *args, **kwargs)

        # A continuation-page response is not the complete query result even if
        # KIS marks that page D/E.  The Sep-29 negative-proof cache must therefore
        # only be replaced by a root (empty input cursor), authoritative, complete
        # response.  Otherwise restore the last independently proven snapshot.
        input_fk = str(kwargs.get("ctx_area_fk100") or "").strip()
        input_nk = str(kwargs.get("ctx_area_nk100") or "").strip()
        authoritative_root_complete = (
            not input_fk
            and not input_nk
            and _daily_payload_complete(result)
        )
        if not authoritative_root_complete:
            if previous is _MISSING:
                try:
                    delattr(self, "_kr_20260929_daily_ccld_snapshot")
                except AttributeError:
                    pass
            else:
                setattr(self, "_kr_20260929_daily_ccld_snapshot", previous)
        return result

    return guarded


def install_kr_20260929_review_guards() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    if not getattr(KisAPI, "_kr_p0_20260929_daily_ccld_authority_fence_installed", False):
        KisAPI.inquire_daily_ccld = _build_daily_ccld_authority_fence(KisAPI.inquire_daily_ccld)
        KisAPI._kr_p0_20260929_daily_ccld_authority_fence_installed = True
    _INSTALLED = True
