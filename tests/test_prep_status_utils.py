from trader.prep_runner import _resolve_prep_final_status
from trader.prep_status_utils import resolve_prep_final_status


def test_prep_status_utils_matches_wrapper():
    prep_core = {"core_ok": 1, "reasons": []}
    aux_failures = [{"stage": "watchlist_load_verify", "reason": "timeout"}]
    assert _resolve_prep_final_status(prep_core=prep_core, aux_failures=aux_failures) == resolve_prep_final_status(prep_core=prep_core, aux_failures=aux_failures)
