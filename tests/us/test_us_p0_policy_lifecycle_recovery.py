from trader.us.db import repos


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Conn:
    def __init__(self, position_rows=None, buy_rows=None):
        self.position_rows = position_rows or []
        self.buy_rows = buy_rows or []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM us_positions" in sql:
            return _Result(self.position_rows)
        if "FROM us_orders" in sql:
            return _Result(self.buy_rows)
        raise AssertionError(sql)


def _policy(book, horizon, exit_policy, lifecycle):
    return {
        "book": book,
        "horizon": horizon,
        "exit_policy": exit_policy,
        "entry_strategy": "us_pb1",
        "position_lifecycle_id": lifecycle,
    }


def test_closed_prior_cycle_prefers_confirmed_buy_from_current_lifecycle():
    conn = _Conn(
        position_rows=[{
            "as_of": "2026-09-10",
            "qty": 0,
            "meta": _policy("OLD_BOOK", "OLD_HORIZON", "OLD_EXIT", "life-old"),
        }],
        buy_rows=[
            {
                "trade_date": "2026-09-12",
                "created_at": "2026-09-12T14:00:00Z",
                "meta": _policy("NEW_BOOK", "NEW_HORIZON", "NEW_EXIT", "life-new"),
            },
            {
                "trade_date": "2026-09-09",
                "created_at": "2026-09-09T14:00:00Z",
                "meta": _policy("OLD_BOOK", "OLD_HORIZON", "OLD_EXIT", "life-old"),
            },
        ],
    )

    policy = repos._load_us_entry_policy_for_position(
        conn,
        symbol="AMD",
        trade_date="2026-09-14",
        position_lifecycle_id="life-new",
    )

    assert policy["book"] == "NEW_BOOK"
    assert policy["horizon"] == "NEW_HORIZON"
    assert policy["exit_policy"] == "NEW_EXIT"
    assert policy["entry_policy_source"] == "confirmed_buy_order"


def test_closed_boundary_never_restores_buy_from_before_full_exit():
    conn = _Conn(
        position_rows=[{
            "as_of": "2026-09-10",
            "qty": 0,
            "meta": _policy("OLD_BOOK", "OLD_HORIZON", "OLD_EXIT", "life-old"),
        }],
        buy_rows=[{
            "trade_date": "2026-09-09",
            "created_at": "2026-09-09T14:00:00Z",
            "meta": _policy("OLD_BOOK", "OLD_HORIZON", "OLD_EXIT", "life-old"),
        }],
    )

    assert repos._load_us_entry_policy_for_position(
        conn,
        symbol="AMD",
        trade_date="2026-09-14",
    ) == {}


def test_statement_capture_connection_returning_none_is_tolerated():
    class _NoneConn:
        def execute(self, statement, params=None):
            return None

    assert repos._load_us_entry_policy_for_position(
        _NoneConn(),
        symbol="AMD",
        trade_date="2026-09-14",
    ) == {}
