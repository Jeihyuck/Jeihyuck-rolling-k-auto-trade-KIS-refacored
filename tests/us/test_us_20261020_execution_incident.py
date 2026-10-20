"""Oct 19 production-shaped regressions; all broker and DB I/O is fake."""
import pytest
from trader.us.execution import order_journal as journal
from trader.us import config
from trader.us.execution.order_permissions import resolve_us_order_permissions


def test_practice_real_stop_matches_startup_at_native_order_guard(monkeypatch):
    for key, value in dict(TRADING_REGION='US', KIS_ENV='practice',
                           US_PAPER_TRADING_ENABLED=True, US_LIVE_TRADING_ENABLED=True,
                           DISABLE_REAL_TRADING=True, DRY_RUN=False).items():
        monkeypatch.setattr(config, key, value)
    flags = dict(DRY_RUN='0', DISABLE_LIVE_TRADING='0', DISABLE_REAL_TRADING='1',
                 LIVE_TRADING_ENABLED='1', US_LIVE_TRADING_ENABLED='1', US_ORDER_ARMED='1')
    assert resolve_us_order_permissions('am', 'practice', 'TRADE', flags).allowed
    config.assert_us_paper_order_allowed()
    assert config.is_paper_order_allowed()


@pytest.mark.parametrize('env', ['real', 'live'])
def test_real_account_stays_blocked_by_native_paper_guard(monkeypatch, env):
    monkeypatch.setattr(config, 'KIS_ENV', env)
    with pytest.raises(RuntimeError, match='kis_env_not_practice'):
        config.assert_us_paper_order_allowed()


def _events():
    submit = dict(event_id='s', event_type='BROKER_SUBMIT_STARTED',
                  trade_date='2026-10-19', client_order_key='meta-exit',
                  submit_attempt_id='pre-io-1', symbol='META', side='SELL', qty=2)
    return [submit, {**submit, 'event_id': 'abort',
                     'event_type': 'BROKER_SUBMIT_ABORTED_PRE_IO'}]


def test_pre_io_abort_is_not_a_sent_or_unknown_order(monkeypatch):
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: _events())
    result = journal.aggregate_order_events('2026-10-19')
    assert result['orders_sent_total'] == 0
    assert result['unresolved_submit_attempt_count'] == 0


def test_pre_io_abort_replay_never_queries_broker(monkeypatch):
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: _events())
    class NoBroker:
        def get_balance(self, **kwargs):
            pytest.fail('known unsent attempts must not trigger broker I/O')
    result = journal.replay_order_journal('2026-10-19', provider=NoBroker())
    assert result['unresolved_count'] == 0
    assert result['broker_unknown_count'] == 0


def test_abort_followed_by_ack_is_not_discarded(monkeypatch):
    events = _events()
    events.append({**events[0], 'event_id': 'ack', 'event_type': 'BROKER_ACK_RECEIVED',
                   'broker_order_no': '0001'})
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: events)
    result = journal.aggregate_order_events('2026-10-19')
    assert result['orders_sent_total'] == 1
    assert result['orders_ack_total'] == 1


def test_exhausted_replay_budget_defers_every_action_without_releasing_fences(monkeypatch):
    events = [_events()[0]]
    events += [{**events[0], 'event_id': 'other', 'client_order_key': 'net-exit',
                'submit_attempt_id': 'net-1', 'symbol': 'NET'}]
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: events)
    monkeypatch.setattr(journal.time, 'monotonic', lambda: 20.0)
    monkeypatch.setattr(journal, 'append_order_event', lambda *a, **k: pytest.fail('no mutation on defer'))
    class NoBroker:
        def get_balance(self, **kwargs):
            pytest.fail('expired stage must not start a broker request')
    result = journal.replay_order_journal('2026-10-19', provider=NoBroker(), deadline_monotonic=10)
    assert result['status'] == 'UNRESOLVED'
    assert result['deferred_count'] == result['unresolved_count'] == 2
    assert result['unresolved_symbol_sides'] == [['META', 'SELL'], ['NET', 'SELL']]


def test_exact_run_sources_reject_missing_run_and_require_matching_completion(monkeypatch):
    from trader.us import path_contract, score_columns, prep_effective
    from trader.us.db import repos
    rows = [{'symbol': f'S{i}', 'run_id': 'prep-19'} for i in range(30)]
    contract = dict(trade_date='2026-10-19', status='OK_WITH_WARNINGS',
                    contract_ok=True, trade_can_proceed=1, exit_can_proceed=1,
                    entry_can_proceed=0, final30_scored_count=30, score_nonzero_count=30)
    monkeypatch.setattr(path_contract, 'load_us_prep_contract', lambda day: contract)
    monkeypatch.setattr(path_contract, 'load_us_final30_scored', lambda day: rows)
    looked_up = []
    def completion(day, run):
        looked_up.append((day, run))
        return dict(run_id=run, status='OK_WITH_WARNINGS')
    monkeypatch.setattr(repos, 'load_us_prep_completion_for_run', completion)
    monkeypatch.setattr(repos, 'load_locked_us_watchlist', lambda **kw: rows)
    monkeypatch.setattr(score_columns, 'validate_us_entry_provenance_contract', lambda rows: {'ok': True})
    assert prep_effective.check_effective_prep_sources('2026-10-19')['reason'] == 'artifact_run_id_missing'
    assert looked_up == []
    contract['run_id'] = 'prep-19'
    assert prep_effective.check_effective_prep_sources('2026-10-19')['ok']
    assert contract['entry_can_proceed'] == 0
    rows[-1]['run_id'] = 'older-run'
    assert not prep_effective.check_effective_prep_sources('2026-10-19')['ok']


def test_bounded_replay_rotates_and_preserves_full_close_recovery(monkeypatch):
    """Same 20 source-day requests: 100s full vs 10s bounded (virtual clock)."""
    events = [{**_events()[0], 'event_id': str(i), 'client_order_key': f'exit-{i}',
               'submit_attempt_id': f'attempt-{i}', 'trade_date': f'2026-10-{i+1:02d}',
               'symbol': f'S{i}'} for i in range(20)]
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: events)
    monkeypatch.setattr(journal, 'append_order_event', lambda *a, **k: {})
    clock = [0.0]
    monkeypatch.setattr(journal.time, 'monotonic', lambda: clock[0])
    class Provider:
        def __init__(self):
            self.days = []
        def get_balance(self, **kwargs):
            return {'positions': []}
        def get_today_orders(self, *, trade_date):
            clock[0] += 5.0
            self.days.append(trade_date)
            return []
    full = Provider()
    result = journal.replay_order_journal('2026-10-19', provider=full)
    assert clock[0] == 100.0 and len(full.days) == 20
    assert result['unresolved_count'] == 20
    clock[0] = 0.0
    bounded = Provider()
    result = journal.replay_order_journal('2026-10-19', provider=bounded,
                                         deadline_monotonic=10, active_claim_offset=5)
    assert clock[0] == 10.0 and bounded.days == ['2026-10-06', '2026-10-07']
    assert result['deferred_count'] == 18
    assert result['unresolved_count'] == 20
    assert len(result['unresolved_symbol_sides']) == 20


def test_previous_replay_noise_does_not_keep_proven_unsent_health_failed(monkeypatch):
    from trader.us.db import repos
    events = _events() + [{**_events()[0], 'event_id': 'replay-noise',
                           'event_type': 'JOURNAL_REPLAY_UNRESOLVED'}]
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: events)
    monkeypatch.setattr(repos, 'load_today_fills', lambda *a, **k: [])
    monkeypatch.setattr(repos, 'load_us_daily_orders_for_report', lambda *a, **k: [])
    result = repos.load_broker_recovery_health('2026-10-19')
    assert result['broker_fill_rebound_failure_count'] == 0


@pytest.mark.parametrize('saved_count,finish_ok,expected_stage', [
    (30, True, None), (0, True, 'locked_watchlist_save'), (30, False, 'prep_run_finish'),
])
def test_prep_pipeline_binds_artifact_and_locked_rows_and_requires_db_completion(
    monkeypatch, tmp_path, saved_count, finish_ok, expected_stage,
):
    from trader.us.runner import prep_runner
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('trader.us.run_manifest.precheck_prep_revision', lambda *a, **k: {'ok': True, 'reason': 'test'})
    monkeypatch.setattr('trader.us.prep_contract._current_git_commit_sha', lambda: 'test-sha')
    monkeypatch.setattr('trader.us.path_contract.load_us_prep_contract', lambda *a: {})
    monkeypatch.setattr(prep_runner, 'save_us_prep_run', lambda **kw: 'prep-run-19')
    monkeypatch.setattr(prep_runner, 'finish_us_prep_run', lambda *a, **kw: finish_ok)
    monkeypatch.setattr(prep_runner, '_save_json_file', lambda *a: None)
    monkeypatch.setattr('trader.us.universe.load_universe', lambda **kw: None)
    monkeypatch.setattr('trader.us.universe.get_all_tickers', lambda: [])
    class Provider:
        def __init__(self, **kw):
            pass
        def get_completed_daily_prices_result(self, *a, **kw):
            return {'quality': 'OK'}
    monkeypatch.setattr('trader.us.data_provider.USDataProvider', Provider)
    monkeypatch.setattr('trader.us.universe_builder.build_us_dynamic_universe',
                        lambda **kw: {'status': 'OK', 'filtered_count': 30, 'symbols': []})
    monkeypatch.setattr('trader.us.db.repos.load_positions', lambda **kw: [])
    monkeypatch.setattr('trader.us.db.repos.load_latest_open_us_position_lifecycles', lambda *a: {})
    monkeypatch.setattr('trader.us.symbols.resolve_exchange', lambda *a: 'NASDAQ')
    monkeypatch.setattr('trader.us.exchange_registry.prepare_exchange_registry',
                        lambda **kw: {'failed_symbols': [], 'failures': []})
    monkeypatch.setattr('trader.us.candidate_pool_builder.build_us_candidate_pool',
                        lambda **kw: {'status': 'OK', 'selected_count': 50, 'rows': []})
    monkeypatch.setattr('trader.us.watchlist_builder._build_rotation_context', lambda *a: {})
    monkeypatch.setattr('trader.us.market_state_overlay.evaluate_us_market_state', lambda **kw: {})
    rows = [{'symbol': f'S{i}', 'score_final': 80, 'close': 10} for i in range(30)]
    monkeypatch.setattr('trader.us.watchlist_builder.build_us_watchlist',
                        lambda **kw: {'final30_scored': rows, 'final30_scored_count': 30,
                                      'top50_scored': rows, 'top50_count': 30})
    monkeypatch.setattr('trader.us.final30_quality.verify_us_final30_scored_rows',
                        lambda *a, **kw: {'ok': True, 'score_nonzero_count': 30})
    monkeypatch.setattr(prep_runner, '_evaluate_benchmark_daily_gate', lambda **kw: {})
    monkeypatch.setattr('trader.us.prep_contract.build_us_prep_contract',
                        lambda **kw: {'status': 'OK', 'trade_date': '2026-10-19',
                                      'trade_can_proceed': 1, 'score_nonzero_count': 30})
    captured = {}
    def save_contract(contract):
        captured['contract'] = dict(contract)
        return {'ok': True, 'saved_paths': []}
    def save_locked(**kw):
        captured['locked'] = kw
        return {'saved_count': saved_count, 'unique_count': saved_count}
    monkeypatch.setattr('trader.us.prep_contract.save_us_prep_contract', save_contract)
    monkeypatch.setattr('trader.us.prep_contract.save_us_prep_summary', lambda *a: None)
    monkeypatch.setattr(prep_runner, 'clear_and_save_locked_us_watchlist', save_locked)
    result = prep_runner.run_prep(offline=True, force_now='2026-10-19T08:00:00-04:00')
    assert captured['contract']['run_id'] == captured['locked']['run_id'] == 'prep-run-19'
    if expected_stage:
        assert result['status'] == 'ERROR' and result['stage'] == expected_stage
    else:
        assert result['status'] == 'OK', result
        assert result['run_id'] == 'prep-run-19'


def test_failed_pre_io_release_remains_visible_and_fenced(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(journal, 'load_order_events', lambda *a, **k: _events())
    monkeypatch.setattr(repos, 'load_active_execution_claim_attempts', lambda: [
        {'trade_date': '2026-10-19', 'client_order_key': 'meta-exit',
         'active_attempt_id': 'pre-io-1'}])
    result = journal.replay_order_journal('2026-10-19', include_active_claims=True,
                                         active_claims_only=True)
    assert result['status'] == 'ERROR'
    assert result['unresolved_count'] == 1
    assert result['unresolved_symbol_sides'] == [['META', 'SELL']]
