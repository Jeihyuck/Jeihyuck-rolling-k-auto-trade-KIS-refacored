# -*- coding: utf-8 -*-
"""US Exit Position Resolver.

미국장 전체 보유 종목의 exit input을 표준화한다.

exit 평가에 들어가는 모든 position은 반드시 entry_price를 가져야 한다.
entry_price를 만들 수 없으면 pnl_input_ok=False, entry_price_source="missing"으로 표시한다.

절대 금지:
- 한국장 DB repo import 금지 (us.db.repos 만 사용)
- us_* 이외 테이블 접근 금지
- 특정 종목 심볼 하드코딩 금지
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_float(value: Any, default: float | None = None) -> float | None:
    """안전한 float 변환. 변환 불가 시 default 반환."""
    if value is None:
        return default
    try:
        f = float(value)
        if f != f:  # NaN
            return default
        return f
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """안전한 int 변환."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _normalize_symbol(symbol: Any) -> str:
    """심볼 대문자 정규화."""
    return str(symbol or "").strip().upper()


# ---------------------------------------------------------------------------
# Entry price resolution helpers
# ---------------------------------------------------------------------------
def _resolve_entry_price_from_position(pos: dict) -> tuple[float | None, str]:
    """position dict에서 직접 entry_price 추출.

    우선순위:
    1. entry_price
    2. avg_price_usd
    3. avg_cost
    4. average_price
    5. avg_buy_price

    Returns:
        (price, source_name) or (None, "")
    """
    for field, source in [
        ("entry_price", "entry_price_field"),
        ("avg_price_usd", "kis_avg_price_usd"),
        ("avg_cost", "avg_cost"),
        ("average_price", "average_price"),
        ("avg_buy_price", "avg_buy_price"),
    ]:
        v = _safe_float(pos.get(field))
        if v is not None and v > 0:
            return v, source
    return None, ""


def _resolve_entry_price_from_buy_amount(pos: dict) -> tuple[float | None, str]:
    """buy_amount_usd / qty 계산.

    Returns:
        (price, "kis_buy_amount_usd") or (None, "")
    """
    qty = _safe_int(pos.get("qty"))
    buy_amount = _safe_float(pos.get("buy_amount_usd"))
    if qty > 0 and buy_amount is not None and buy_amount > 0:
        return round(buy_amount / qty, 6), "kis_buy_amount_usd"
    return None, ""


def _resolve_entry_price_from_pnl_rate(pos: dict) -> tuple[float | None, str]:
    """pnl_rate와 current_price로 entry_price 역산.

    공식: entry_price = current_price / (1 + rate)
    pnl_rate가 절대값 1 이하면 소수점 비율, 초과면 퍼센트로 해석.

    Returns:
        (price, "kis_pnl_rate_fallback") or (None, "")
    """
    # current_price 후보
    current_price: float | None = None
    for field in ("current_price_usd", "current_price", "current_px"):
        v = _safe_float(pos.get(field))
        if v is not None and v > 0:
            current_price = v
            break

    if current_price is None or current_price <= 0:
        return None, ""

    # pnl_rate 후보
    pnl_rate: float | None = None
    for field in ("pnl_rate", "unrealized_pnl_pct", "evlu_pfls_rt"):
        v = _safe_float(pos.get(field))
        if v is not None and v != 0.0:
            pnl_rate = v
            break

    if pnl_rate is None:
        return None, ""

    # 단위 정규화: |rate| > 1 이면 퍼센트
    rate = pnl_rate / 100.0 if abs(pnl_rate) > 1 else pnl_rate

    # -100% 이하이면 무의미 (포지션이 이미 0)
    if rate <= -0.99:
        return None, ""

    entry_price = current_price / (1.0 + rate)
    if entry_price <= 0:
        return None, ""

    return round(entry_price, 6), "kis_pnl_rate_fallback"


def _position_identity(pos: dict, *, env: str) -> dict[str, str]:
    meta = pos.get("meta") if isinstance(pos.get("meta"), dict) else {}
    contract = _metadata(meta.get("entry_exit_contract"))

    def value(*keys: str) -> str:
        for key in keys:
            raw = pos.get(key) if pos.get(key) not in (None, "") else meta.get(key)
            if raw not in (None, ""):
                return str(raw).strip()
        return ""

    position_env = value("env", "trading_env") or str(env or "").strip().lower()
    account_id = value("account_id", "account_key")
    if not account_id:
        try:
            from trader.account_state import get_account_key
            account_id = get_account_key(env=position_env)
        except Exception:
            account_id = ""
    trading_epoch_id = value("trading_epoch_id")
    strategy_owner = (
        value("strategy_owner", "sleeve_id")
        or str(contract.get("strategy_owner") or contract.get("sleeve_id") or "").strip()
    )
    lifecycle_id = value(
        "position_lifecycle_id", "position_cycle_id", "lifecycle_id", "cycle_id",
    )
    return {
        "env": position_env.lower(),
        "account_id": account_id,
        "trading_epoch_id": trading_epoch_id,
        "strategy_owner": strategy_owner.upper(),
        "position_lifecycle_id": lifecycle_id,
    }


def _identity_is_complete(identity: dict[str, str]) -> bool:
    return all(identity.get(key) for key in (
        "env", "account_id", "trading_epoch_id", "strategy_owner",
        "position_lifecycle_id",
    ))


def _identity_is_current(identity: dict[str, str]) -> bool:
    if not _identity_is_complete(identity):
        return False
    try:
        from trader.account_state import get_account_key, resolve_env_name
        from trader.us.db.repos import _active_us_epoch

        return bool(
            resolve_env_name() == identity["env"]
            and get_account_key(env=identity["env"]) == identity["account_id"]
            and str(_active_us_epoch() or "") == identity["trading_epoch_id"]
        )
    except Exception:
        return False


def _metadata(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            import json
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _history_row_matches_identity(row: dict, identity: dict[str, str]) -> bool:
    meta = _metadata(row.get("meta"))
    contract = _metadata(row.get("entry_exit_contract") or meta.get("entry_exit_contract"))
    state = _metadata(row.get("state"))
    lifecycle = _metadata(state.get("lifecycle"))
    row_epoch = row.get("trading_epoch_id") or meta.get("trading_epoch_id")
    row_owner = (
        row.get("strategy_owner") or meta.get("strategy_owner")
        or row.get("sleeve_id") or meta.get("sleeve_id")
        or contract.get("strategy_owner") or lifecycle.get("strategy_owner")
        or lifecycle.get("sleeve_id")
    )
    row_lifecycle = (
        row.get("position_lifecycle_id") or meta.get("position_lifecycle_id")
        or row.get("position_cycle_id") or meta.get("position_cycle_id")
        or row.get("lifecycle_id") or meta.get("lifecycle_id")
        or lifecycle.get("lifecycle_id")
    )
    row_env = (
        row.get("env") or meta.get("env") or meta.get("trading_env")
        or lifecycle.get("env") or lifecycle.get("trading_env")
    )
    row_account = (
        row.get("account_id") or row.get("account_key")
        or meta.get("account_id") or meta.get("account_key")
        or lifecycle.get("account_id") or lifecycle.get("account_key")
    )
    return bool(
        row_epoch and str(row_epoch) == identity["trading_epoch_id"]
        and row_owner and str(row_owner).strip().upper() == identity["strategy_owner"]
        and row_lifecycle and str(row_lifecycle).strip() == identity["position_lifecycle_id"]
        and row_env and str(row_env).strip().lower() == identity["env"]
        and row_account and str(row_account).strip() == identity["account_id"]
    )


def _latest_exact_history_candidate(
    candidates: list[dict], identity: dict[str, str], date_field: str,
) -> dict:
    matches = [row for row in candidates if _history_row_matches_identity(row, identity)]
    if not matches:
        return {}
    latest_date = max(str(row.get(date_field) or "") for row in matches)
    latest = [row for row in matches if str(row.get(date_field) or "") == latest_date]
    if len(latest) > 1:
        signatures = {
            repr(sorted((key, repr(value)) for key, value in row.items() if key != "updated_at"))
            for row in latest
        }
        if len(signatures) > 1:
            return {}
    return latest[0]


def _lifecycle_matches_identity(
    risk_state: dict, lifecycle: dict, identity: dict[str, str],
) -> bool:
    if not _history_row_matches_identity(risk_state, identity):
        return False
    lifecycle_identity = {
        "env": lifecycle.get("env") or lifecycle.get("trading_env"),
        "trading_epoch_id": lifecycle.get("trading_epoch_id"),
        "strategy_owner": lifecycle.get("strategy_owner") or lifecycle.get("sleeve_id"),
        "position_lifecycle_id": lifecycle.get("lifecycle_id"),
    }
    return bool(
        all(
        lifecycle_identity.get(key) is not None
        and str(lifecycle_identity[key]).strip().lower() == identity[key].lower()
        for key in lifecycle_identity
        )
        and (
            str(lifecycle.get("account_id") or lifecycle.get("account_key") or "").strip()
            == identity["account_id"]
        )
    )


def _position_lifecycle_view(
    position: dict,
    identity: dict[str, str],
    lifecycle: dict,
    *,
    identity_known: bool,
) -> dict:
    meta = _metadata(position.get("meta"))
    contract = _metadata(position.get("entry_exit_contract") or meta.get("entry_exit_contract"))

    def value(*names: str) -> Any:
        for name in names:
            candidate = position.get(name)
            if candidate is None:
                candidate = meta.get(name)
            if candidate is not None:
                return candidate
        return None

    try:
        from trader.us.entry_exit_contract import us_entry_exit_contract_integrity_state
        contract_state = us_entry_exit_contract_integrity_state(position, meta)
    except Exception:
        contract_state = "INVALID" if contract else "NONE"

    tp_quantities: dict[str, dict[str, Any]] = {}
    tp_inconsistent = False
    for stage in ("tp1", "tp2", "tp3"):
        filled = value(f"{stage}_filled_qty", f"{stage}_actual_filled_qty")
        target = value(f"{stage}_target_qty", f"{stage}_qty")
        remaining = value(f"{stage}_remaining_qty", f"{stage}_pending_qty")
        if filled is not None and target is not None and remaining is not None:
            try:
                tp_inconsistent = tp_inconsistent or int(target) != int(filled) + int(remaining)
            except (TypeError, ValueError):
                tp_inconsistent = True
        tp_quantities[stage] = {
            "actual_filled_qty": filled,
            "target_qty": target,
            "pending_or_remaining_qty": remaining,
            "done": value(f"{stage}_done"),
            "pending": value(f"{stage}_pending"),
        }

    qty = _safe_int(position.get("qty"))
    lifecycle_open = lifecycle.get("is_open") if lifecycle else value("lifecycle_is_open", "is_open")
    pending_state = any(bool(tp_quantities[stage]["pending"]) for stage in tp_quantities)
    closed_with_open_state = lifecycle_open is False and (qty > 0 or pending_state)
    integrity = {
        "missing_or_ambiguous_lifecycle_identity": not identity_known,
        "broker_held_position_hidden_by_epoch": bool(
            qty > 0 and (
                not identity.get("trading_epoch_id")
                or position.get("epoch_visibility_status") == "STALE_EPOCH_VISIBLE_PROTECTIVE"
            )
        ),
        "frozen_contract_hash_version_mismatch": contract_state == "INVALID",
        "tp_quantity_inconsistency": tp_inconsistent,
        "closed_lifecycle_with_open_or_pending_state": closed_with_open_state,
    }
    return {
        "env": identity.get("env") or None,
        "account_id": identity.get("account_id") or None,
        "market": str(value("market") or "US").upper(),
        "trading_epoch_id": identity.get("trading_epoch_id") or None,
        "strategy_owner": identity.get("strategy_owner") or None,
        "sleeve_id": value("sleeve_id") or identity.get("strategy_owner") or None,
        "lifecycle_id": lifecycle.get("lifecycle_id") or identity.get("position_lifecycle_id") or None,
        "entry_exit_contract": contract or None,
        "entry_exit_contract_version": (
            contract.get("version") or value("entry_exit_contract_version")
        ),
        "entry_exit_contract_sha256": (
            contract.get("sha256") or value("entry_exit_contract_sha256")
        ),
        "opened_at": lifecycle.get("opened_at") or value("opened_at"),
        "opened_trade_date": lifecycle.get("opened_trade_date") or value("opened_trade_date"),
        "opened_at_source": lifecycle.get("opened_at_source") or value("opened_at_source"),
        "entry_price": position.get("entry_price"),
        "entry_price_provenance": position.get("entry_price_source"),
        "holding_qty": value("holding_qty") if value("holding_qty") is not None else qty,
        "orderable_qty": value("orderable_qty", "sellable_qty"),
        "broker_snapshot_source": value("balance_source"),
        "broker_snapshot_as_of": value("balance_snapshot_asof", "as_of", "updated_at"),
        "broker_snapshot_complete": value("authoritative_positions") is True,
        "cumulative_buy_filled_qty": value("lifecycle_buy_filled_qty", "cumulative_buy_filled_qty"),
        "cumulative_sell_filled_qty": value("lifecycle_sell_filled_qty", "cumulative_sell_filled_qty"),
        "tp_stages": tp_quantities,
        "is_open": lifecycle_open,
        "closed_at": lifecycle.get("closed_at") or value("closed_at"),
        "identity_status": (
            "IDENTIFIED" if identity_known
            else position.get("lifecycle_identity_status") or "MISSING_OR_AMBIGUOUS"
        ),
        "contract_integrity": contract_state,
        "integrity": integrity,
    }


# ---------------------------------------------------------------------------
# DB-backed fallbacks (us_positions / us_fills)
# ---------------------------------------------------------------------------
def _resolve_from_us_positions_db(
    symbol: str,
    as_of: str | None,
    identity: dict[str, str],
    identity_is_current: bool,
) -> tuple[float | None, str]:
    """us_positions DB에서 avg_cost 조회."""
    if not identity_is_current:
        return None, ""
    try:
        from trader.us.db.repos import load_us_position_history_candidates
        rows = load_us_position_history_candidates(symbol, as_of=as_of)
        row = _latest_exact_history_candidate(rows, identity, "as_of")
        if row:
            v = _safe_float(row.get("avg_cost"))
            if v is not None and v > 0:
                return v, "us_positions_avg_cost"
    except Exception as exc:
        logger.debug("[US_EXIT_RESOLVER][DB_POS_FAIL] symbol=%s err=%s", symbol, exc)
    return None, ""


def _resolve_from_us_fills_db(
    symbol: str,
    trade_date: str | None,
    identity: dict[str, str],
    identity_is_current: bool,
) -> tuple[float | None, str]:
    """us_fills DB에서 최신 BUY price_usd 조회."""
    if not identity_is_current:
        return None, ""
    try:
        from trader.us.db.repos import load_us_buy_fill_history_candidates
        rows = load_us_buy_fill_history_candidates(symbol, trade_date=trade_date)
        row = _latest_exact_history_candidate(rows, identity, "trade_date")
        if row:
            v = _safe_float(row.get("price_usd"))
            if v is not None and v > 0:
                return v, "us_fills_latest_buy"
    except Exception as exc:
        logger.debug("[US_EXIT_RESOLVER][DB_FILL_FAIL] symbol=%s err=%s", symbol, exc)
    return None, ""


# ---------------------------------------------------------------------------
# Single position enrichment
# ---------------------------------------------------------------------------
def _enrich_single_position(
    pos: dict,
    *,
    trade_date: str | None,
    env: str,
) -> dict:
    """단일 position의 entry_price를 표준 contract로 채운다.

    이미 유효한 entry_price가 있으면 그대로 유지하되 lifecycle timing은
    항상 복원한다. 없으면 순서대로 fallback을 시도한다.
    """
    symbol = _normalize_symbol(pos.get("symbol", ""))
    exchange = str(pos.get("exchange") or "NASDAQ").strip() or "NASDAQ"
    qty = _safe_int(pos.get("qty"))
    identity = _position_identity(pos, env=env)
    identity_is_current = _identity_is_current(identity)

    # qty <= 0 은 exit 대상 아님
    if qty <= 0:
        return {
            **pos,
            "symbol": symbol,
            "exchange": exchange,
            "qty": qty,
            "pnl_input_ok": False,
            "entry_price_source": "qty_zero",
        }

    # 기존 entry price/source가 있어도 여기서 return하지 않는다. DB position
    # fast path 역시 cross-day lifecycle timing 복원을 반드시 거쳐야 한다.
    existing_ep = _safe_float(pos.get("entry_price"))
    existing_src = pos.get("entry_price_source") or ""
    ep: float | None = existing_ep if existing_ep is not None and existing_ep > 0 and existing_src else None
    src: str = str(existing_src) if ep is not None else ""

    # --- Resolution chain ---
    # 1-5: position 필드 직접
    if ep is None:
        ep, src = _resolve_entry_price_from_position(pos)

    # 6: buy_amount_usd / qty
    if ep is None:
        ep, src = _resolve_entry_price_from_buy_amount(pos)

    # 7: pnl_rate 역산
    if ep is None:
        ep, src = _resolve_entry_price_from_pnl_rate(pos)

    # 8: us_positions DB
    if ep is None:
        ep, src = _resolve_from_us_positions_db(
            symbol, as_of=trade_date, identity=identity,
            identity_is_current=identity_is_current,
        )

    # 9: us_fills DB
    if ep is None:
        ep, src = _resolve_from_us_fills_db(
            symbol, trade_date=trade_date, identity=identity,
            identity_is_current=identity_is_current,
        )

    enriched = {
        **pos,
        "symbol": symbol,
        "exchange": exchange,
        "qty": qty,
    }

    if ep is not None and ep > 0:
        enriched["entry_price"] = ep
        enriched["entry_price_source"] = src
        enriched["pnl_input_ok"] = True
    else:
        enriched["entry_price"] = 0.0
        enriched["entry_price_source"] = "missing"
        enriched["pnl_input_ok"] = False
        logger.warning(
            "[US_EXIT_RESOLVER][PNL_MISSING] symbol=%s qty=%s "
            "entry_price=%s avg_price_usd=%s avg_cost=%s buy_amount_usd=%s pnl_rate=%s",
            symbol,
            qty,
            pos.get("entry_price"),
            pos.get("avg_price_usd"),
            pos.get("avg_cost"),
            pos.get("buy_amount_usd"),
            pos.get("pnl_rate"),
        )

    # Persisted lifecycle timing and high-watermark are independent authorities.
    # Restore timing even when a historical row lacks a high-watermark value.
    lifecycle: dict = {}
    latest: dict = {}
    try:
        from trader.us.db.repos import load_us_position_risk_state_candidates
        candidates = (
            load_us_position_risk_state_candidates(symbol, trade_date)
            if trade_date and identity_is_current else {}
        )
        latest = _latest_exact_history_candidate(candidates, identity, "trade_date")
        lifecycle = ((latest.get("state") or {}).get("lifecycle") or {}) if isinstance(latest.get("state"), dict) else {}
        if lifecycle and _lifecycle_matches_identity(latest, lifecycle, identity):
            enriched["position_lifecycle_id"] = lifecycle.get("lifecycle_id") or enriched.get("position_lifecycle_id")
            enriched["opened_trade_date"] = lifecycle.get("opened_trade_date") or enriched.get("opened_trade_date")
            enriched["opened_at"] = lifecycle.get("opened_at") or enriched.get("opened_at")
            enriched["opened_at_source"] = lifecycle.get("opened_at_source") or enriched.get("opened_at_source")
            enriched["holding_trade_days"] = lifecycle.get("holding_trade_days") or enriched.get("holding_trade_days")
            entry_policy = _metadata(lifecycle.get("entry_policy"))
            if not _metadata(enriched.get("entry_exit_contract")):
                frozen_contract = _metadata(entry_policy.get("entry_exit_contract"))
                if frozen_contract:
                    enriched["entry_exit_contract"] = frozen_contract
                    enriched["entry_exit_contract_sha256"] = (
                        entry_policy.get("entry_exit_contract_sha256") or frozen_contract.get("sha256")
                    )
                    enriched["entry_exit_contract_version"] = (
                        entry_policy.get("entry_exit_contract_version") or frozen_contract.get("version")
                    )
                    enriched_meta = _metadata(enriched.get("meta"))
                    enriched_meta.update({
                        "entry_exit_contract": frozen_contract,
                        "entry_exit_contract_sha256": enriched["entry_exit_contract_sha256"],
                        "entry_exit_contract_version": enriched["entry_exit_contract_version"],
                    })
                    enriched["meta"] = enriched_meta
            # `us_positions.created_at` is a durable row timestamp, not the BUY
            # lifecycle start. The exit router checks `entry_time` first, so bind
            # it to the authoritative lifecycle/fill timestamp.
            if lifecycle.get("opened_at"):
                enriched["entry_time"] = lifecycle.get("opened_at")
                enriched["entry_time_source"] = lifecycle.get("opened_at_source") or "us_position_risk_state"
        elif lifecycle:
            enriched["lifecycle_identity_status"] = "AMBIGUOUS_OR_MISMATCHED"
            lifecycle = {}
        else:
            enriched["lifecycle_identity_status"] = (
                "IDENTIFIED" if identity_is_current
                else pos.get("lifecycle_identity_status") or "MISSING"
            )
        hwm = _safe_float(lifecycle.get("high_watermark"))
        if (
            hwm is not None and hwm > 0
            and _lifecycle_matches_identity(latest, lifecycle, identity)
        ):
            enriched["high_watermark"] = hwm
            enriched["max_price"] = hwm
            enriched["high_watermark_source"] = "us_position_risk_state"
    except Exception as exc:
        logger.debug("[US_EXIT_RESOLVER][HWM_FAIL] symbol=%s err=%s", symbol, exc)
    if not enriched.get("max_price") and not enriched.get("high_watermark"):
        current_price_v = _safe_float(pos.get("current_price_usd") or pos.get("current_price") or pos.get("current_px"))
        base = enriched.get("entry_price") or 0.0
        meta = pos.get("meta") if isinstance(pos.get("meta"), dict) else {}
        source_lifecycle_id = (
            pos.get("high_watermark_lifecycle_id")
            or meta.get("high_watermark_lifecycle_id")
            or pos.get("position_lifecycle_id")
            or meta.get("position_lifecycle_id")
        )
        hwm = (
            _safe_float(pos.get("high_watermark") or pos.get("max_price"))
            if source_lifecycle_id and str(source_lifecycle_id) == identity["position_lifecycle_id"]
            else None
        )
        if hwm is not None and hwm > 0:
            enriched["high_watermark"] = hwm; enriched["max_price"] = hwm
            enriched["high_watermark_source"] = pos.get("high_watermark_source") or "position_field"
        elif current_price_v and current_price_v > 0:
            enriched["max_price"] = max(base, current_price_v)
            enriched["high_watermark"] = enriched["max_price"]
            enriched["high_watermark_source"] = "fallback_current_or_entry"
        elif base > 0:
            enriched["max_price"] = base
            enriched["high_watermark"] = base
            enriched["high_watermark_source"] = "fallback_current_or_entry"

    enriched["position_lifecycle_view"] = _position_lifecycle_view(
        enriched, identity, lifecycle, identity_known=identity_is_current,
    )
    return enriched


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def enrich_us_positions_for_exit(
    positions: list[dict],
    *,
    trade_date: str,
    env: str = "practice",
    provider: Any = None,
) -> tuple[list[dict], dict]:
    """미국장 전체 보유 종목의 exit input을 표준화한다.

    Parameters
    ----------
    positions : list[dict]
        KIS reconcile 또는 DB에서 온 raw position 목록
    trade_date : str
        거래일 (YYYY-MM-DD)
    env : str
        실행 환경 (practice / real / live)
    provider : optional
        USDataProvider (현재 미사용 — 향후 real-time price 보강용)

    Returns
    -------
    (enriched_positions, meta)
        enriched_positions: entry_price가 채워진 position 목록 (qty <= 0 제외)
        meta: resolution 통계 dict
    """
    if not positions:
        return [], {
            "total": 0, "ok": 0, "missing": 0,
            "sources": {}, "missing_symbols": [], "integrity": {},
        }

    enriched_list: list[dict] = []
    source_counts: dict[str, int] = {}
    missing_symbols: list[str] = []
    integrity_counts: dict[str, int] = {}
    ok_count = 0

    for pos in positions:
        ep = _enrich_single_position(pos, trade_date=trade_date, env=env)
        qty = ep.get("qty", 0)

        # qty <= 0 제외
        if qty <= 0:
            continue

        enriched_list.append(ep)

        src = ep.get("entry_price_source", "missing")
        source_counts[src] = source_counts.get(src, 0) + 1

        if ep.get("pnl_input_ok"):
            ok_count += 1
        else:
            missing_symbols.append(ep.get("symbol", "?"))
        for key, value in (ep.get("position_lifecycle_view") or {}).get("integrity", {}).items():
            if value:
                integrity_counts[key] = integrity_counts.get(key, 0) + 1

    total = len(enriched_list)
    missing_count = total - ok_count

    meta = {
        "total": total,
        "ok": ok_count,
        "missing": missing_count,
        "sources": source_counts,
        "missing_symbols": missing_symbols,
        "integrity": integrity_counts,
    }

    logger.info(
        "[US_EXIT_RESOLVER][DONE] total=%d ok=%d missing=%d sources=%s missing_symbols=%s",
        total,
        ok_count,
        missing_count,
        source_counts,
        missing_symbols,
    )

    return enriched_list, meta
