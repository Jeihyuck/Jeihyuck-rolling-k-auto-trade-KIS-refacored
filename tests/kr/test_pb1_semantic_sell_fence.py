from trader.kr.pb1.semantic_sell_fence import same_day_semantic_sell_blocked


class _Repo:
    def __init__(self, rows=None, exc: Exception | None = None):
        self.rows = rows or []
        self.exc = exc
        self.calls = []

    def list_today_orders(self, env, *, side, code, status_exclude):
        self.calls.append((env, side, code, status_exclude))
        if self.exc is not None:
            raise self.exc
        return self.rows


class _Logger:
    def __init__(self):
        self.messages = []

    def warning(self, msg, *args):
        self.messages.append((msg, args))


def test_same_day_semantic_sell_blocked_on_matching_row():
    repo = _Repo(rows=[{
        "code": "035720",
        "strategy_owner": "KR_STANDARD",
        "status": "ACKED",
        "reason_family": "PROFIT_CAPTURE",
        "position_lifecycle_id": "life-1",
    }])

    assert same_day_semantic_sell_blocked(
        orders_repo=repo,
        env="practice",
        code="035720",
        strategy_owner="KR_STANDARD",
        reason_family="PROFIT_CAPTURE",
        lifecycle_id="life-1",
    )
    assert repo.calls == [("practice", "SELL", "035720", ())]


def test_same_day_semantic_sell_blocked_fails_open_on_lookup_error():
    repo = _Repo(exc=RuntimeError("boom"))
    logger = _Logger()

    assert not same_day_semantic_sell_blocked(
        orders_repo=repo,
        env="practice",
        code="035720",
        strategy_owner="KR_STANDARD",
        reason_family="PROFIT_CAPTURE",
        lifecycle_id="life-1",
        logger=logger,
    )
    assert repo.calls == [("practice", "SELL", "035720", ())]
    assert logger.messages
