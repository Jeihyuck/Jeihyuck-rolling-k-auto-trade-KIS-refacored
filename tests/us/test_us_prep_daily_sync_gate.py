from trader.us.prep_contract import build_us_prep_contract

def test_51_of_92_blocks_entry_only():
    c=build_us_prep_contract(trade_date='2026-07-16',env='practice',status='OK',dynamic_universe_result={},candidate_pool_result={},watchlist_result={'final30_count':30,'final30_scored_count':30},validation={'ok':True,'score_nonzero_count':30},paths={},daily_sync_summary={'sync_target_count':92,'sync_ok_count':51,'sync_failed_count':41})
    assert c['daily_sync_success_ratio']==51/92 and c['entry_can_proceed']==0 and c['exit_can_proceed']==1 and c['close_can_proceed']==1


def test_exchange_and_price_failures_are_separate_contract_fields():
    c = build_us_prep_contract(
        trade_date='2026-07-16', env='practice', status='OK',
        dynamic_universe_result={}, candidate_pool_result={},
        watchlist_result={'final30_count': 30, 'final30_scored_count': 30},
        validation={'ok': True, 'score_nonzero_count': 30}, paths={},
        daily_sync_summary={
            'sync_target_count': 93, 'sync_ok_count': 91, 'sync_failed_count': 2,
            'sync_failed_symbols': ['ZZZZ', 'BAD'],
            'exchange_resolution_failed_symbols': ['ZZZZ'],
            'price_data_failed_symbols': ['BAD'],
        },
    )
    assert c['exchange_resolution_failed_symbols'] == ['ZZZZ']
    assert c['price_data_failed_symbols'] == ['BAD']


def test_all_ok_daily_sync_does_not_trigger_data_quality_block():
    c = build_us_prep_contract(
        trade_date='2026-07-16', env='practice', status='OK',
        dynamic_universe_result={}, candidate_pool_result={},
        watchlist_result={'final30_count': 30, 'final30_scored_count': 30},
        validation={'ok': True, 'score_nonzero_count': 30}, paths={},
        daily_sync_summary={'sync_target_count': 93, 'sync_ok_count': 93, 'sync_failed_count': 0},
    )
    assert c['daily_sync_success_ratio'] == 1.0
    assert c['trade_block_reason'] != 'data_sync_quality_entry_block'
