"""Original KIS H0STCNT0 index contract: full native date/VWAP + 47-field batching."""
import asyncio
import time

from trader.marketdata.kis_ws_price import KisWebSocketPriceService


def _record(code="219130", *, day="20261113", clock="122910", width=47):
    fields = ["0"] * width
    fields[0] = code
    fields[1] = clock
    fields[2] = "10000"
    fields[6] = "9995.50"  # official WGHN_AVRG_STCK_PRC
    fields[10] = "10005"
    fields[11] = "10000"
    fields[13] = "23600"  # ACML_VOL, not one-execution CNTG_VOL
    fields[33] = day
    return fields


def test_full_native_record_preserves_vwap_date_time_and_cumulative(monkeypatch):
    svc = KisWebSocketPriceService()
    monkeypatch.setattr(svc, "network_allowed", lambda market: True)
    parsed = svc.parse_kr_trade("^".join(_record()))
    assert parsed["wghn_avrg_stck_prc"] == 9995.5
    assert parsed["stck_bsop_date"] == "20261113"
    assert parsed["stck_cntg_hour"] == "122910"
    assert parsed["acml_vol"] == 23600
    svc.put_quote(**parsed, received_at=time.time())
    q = svc.get_fresh_quote("KR", "219130", max_age_sec=5)
    assert q["source"] == "KIS_WEBSOCKET"
    assert q["symbol"] == "219130"
    assert q["stck_bsop_date"] == "20261113"
    assert q["wghn_avrg_stck_prc"] == 9995.5
    assert q["stck_cntg_hour"] == "122910"


def test_multi_record_47_field_ws_frame_keeps_second_symbol_aligned(monkeypatch):
    svc = KisWebSocketPriceService()
    monkeypatch.setattr(svc, "network_allowed", lambda market: True)
    a = _record("219130")
    b = _record("251970", clock="122911")
    text = "0|H0STCNT0|2|" + "^".join(a + b)

    class DummySocket:
        pass

    asyncio.run(svc._handle_message(DummySocket(), text))
    qa = svc.get_fresh_quote("KR", "219130", max_age_sec=5)
    qb = svc.get_fresh_quote("KR", "251970", max_age_sec=5)
    assert qa["symbol"] == "219130"
    assert qb["symbol"] == "251970"
    assert qb["stck_cntg_hour"] == "122911"
    assert qb["stck_bsop_date"] == "20261113"
    assert qa["wghn_avrg_stck_prc"] == qb["wghn_avrg_stck_prc"]


def test_short_price_only_frame_does_not_fabricate_volume_day_or_vwap():
    parsed = KisWebSocketPriceService.parse_kr_trade(
        "^".join(_record()[:12])
    )
    assert "stck_cntg_hour" not in parsed
    assert "acml_vol" not in parsed
    assert "stck_bsop_date" not in parsed
    assert "wghn_avrg_stck_prc" not in parsed


def test_native_date_not_invented_from_invalid_wire_value():
    parsed = KisWebSocketPriceService.parse_kr_trade(
        "^".join(_record(day="00000000"))
    )
    # An eight-digit but impossible date must not enter a VERIFIED quote.
    # The proof gate rejects it as a mismatched native session date.
    assert parsed.get("stck_bsop_date") == "00000000"
