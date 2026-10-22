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


def test_three_worker_market_reads_preserve_thirty_ordered_final30():
    import time
    from threading import Lock
    from trader.kr.exit_io_pipeline import ordered_bounded_entry_reads
    lock=Lock()
    currently=0
    peak=0
    order=list(range(30))

    def fetch(i):
        nonlocal currently,peak
        with lock:
            currently+=1
            peak=max(peak,currently)
        try:
            time.sleep(0.005 if i % 2 else 0.01)
            return i * 10
        finally:
            with lock:
                currently-=1

    actual=list(ordered_bounded_entry_reads(
        order,needs_history=lambda _: True,fetch=fetch,
        empty=lambda: None,max_workers=3,
    ))
    assert actual==[(i,i*10) for i in order]
    assert 1 < peak <=3


def test_three_workers_propagate_deadline_but_never_share_policy_state():
    import time
    from trader.kis_wrapper import (
        set_kr_tick_deadline,get_kr_tick_deadline,clear_kr_tick_deadline,
    )
    from trader.kr.exit_io_pipeline import ordered_bounded_entry_reads
    expected=time.monotonic()+8
    set_kr_tick_deadline(expected)
    try:
        results=list(ordered_bounded_entry_reads(
            ["A","B","C","D"],needs_history=lambda _: True,
            fetch=lambda code: (code,get_kr_tick_deadline()),
            empty=lambda: None,max_workers=3,
        ))
        assert [p[1][0] for p in results]==["A","B","C","D"]
        assert all(p[1][1]==expected for p in results)
        assert get_kr_tick_deadline()==expected
    finally:
        clear_kr_tick_deadline()
