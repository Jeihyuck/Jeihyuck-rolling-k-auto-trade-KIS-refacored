"""KR final30 market-data fetch parallelism may not alter entry selection.

This tests the exact data IO primitive reused in PB1Engine._compute_candidates.
No concurrent signal policy, risk sizing or order submits are permitted.
"""
from inspect import getsource

from trader.kr.exit_io_pipeline import ordered_exit_ohlcv
from trader.pb1_engine import PB1Engine


def test_all_thirty_candidate_rows_preserve_original_order_and_derived_skips():
    members = [{"code": str(i).zfill(6)} for i in range(1,31)]
    derived = {str(i).zfill(6) for i in range(1,31) if i % 3 != 0}
    reads = []
    for parallel in (True,False):
        reads.clear()
        results=list(ordered_exit_ohlcv(
            members,
            needs_history=lambda member: member["code"] in derived,
            fetch=lambda m: reads.append(m["code"]) or {"ohlcv_code":m["code"]},
            empty=lambda: {"source":"derived_contract_missing"},
            enabled=parallel,
        ))
        assert [x["code"] for x,_ in results]==[x["code"] for x in members]
        assert reads == [m["code"] for m in members if m["code"] in derived]
        assert all(((x["code"] in derived) == ("ohlcv_code" in data)) for x,data in results)


def test_entry_prefetch_only_wraps_data_not_signal_or_broker_decisions():
    code=getsource(PB1Engine._compute_candidates)
    assert 'KR_ENTRY_ORDERED_OHLCV_READAHEAD' in code
    assert 'ordered_exit_ohlcv(' in code
    assert code.index('for m, (df, meta) in _candidate_history') < code.index('features = compute_features(df)')
    assert 'self._fetch_daily(_code, days=need_days)' in code
    assert 'logger.warning(' in code
