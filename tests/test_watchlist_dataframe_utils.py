from trader.watchlist_builder import _as_dataframe
from trader.watchlist_dataframe_utils import as_dataframe


def test_as_dataframe_wrapper_matches_module():
    data = [{"code": "000001", "value": 1}, {"code": "000002", "value": 2}]
    assert _as_dataframe(data).equals(as_dataframe(data))
    assert _as_dataframe(("a", "b")).equals(as_dataframe(("a", "b")))
    assert _as_dataframe(None).equals(as_dataframe(None))
