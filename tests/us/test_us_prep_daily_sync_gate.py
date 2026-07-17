from trader.us.prep_contract import build_us_prep_contract

def test_51_of_92_blocks_entry_only():
    c=build_us_prep_contract(trade_date='2026-07-16',env='practice',status='OK',dynamic_universe_result={},candidate_pool_result={},watchlist_result={'final30_count':30,'final30_scored_count':30},validation={'ok':True,'score_nonzero_count':30},paths={},daily_sync_summary={'sync_target_count':92,'sync_ok_count':51,'sync_failed_count':41})
    assert c['daily_sync_success_ratio']==51/92 and c['entry_can_proceed']==0 and c['exit_can_proceed']==1 and c['close_can_proceed']==1
