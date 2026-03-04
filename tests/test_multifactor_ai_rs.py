from trader.factors.multifactor import compute_ai_rs_scores


def test_ai_rs_scores_returns_ranked_scale():
    rows = [
        {
            "code": "000001",
            "rs63": 0.1,
            "rs126": 0.2,
            "rs252": 0.3,
            "rs_score": 20.0,
            "volume_trend": 0.1,
            "volatility": 0.02,
            "momentum": 0.08,
        },
        {
            "code": "000002",
            "rs63": 0.2,
            "rs126": 0.25,
            "rs252": 0.35,
            "rs_score": 30.0,
            "volume_trend": 0.2,
            "volatility": 0.03,
            "momentum": 0.12,
        },
        {
            "code": "000003",
            "rs63": -0.05,
            "rs126": 0.0,
            "rs252": 0.05,
            "rs_score": 5.0,
            "volume_trend": -0.1,
            "volatility": 0.05,
            "momentum": -0.01,
        },
    ]

    scores = compute_ai_rs_scores(rows)

    assert set(scores.keys()) == {"000001", "000002", "000003"}
    for value in scores.values():
        assert 0.0 <= float(value) <= 100.0
    assert scores["000002"] >= scores["000001"] >= scores["000003"]
