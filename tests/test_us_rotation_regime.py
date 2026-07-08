from trader.us.rotation import (
    AI_CAP_CLUSTERS,
    apply_cap_flags,
    classify_rotation_regime,
    compute_cluster_exposure,
    select_bucket_champions,
    theme_cluster_for,
)


def _returns(spy=.01, qqq=.0, smh=-.01, dia=.015, rsp=.014, xli=.015, xlf=.016, xlv=.012):
    base = {s: {1: 0.0, 3: spy, 5: spy, 20: 0.0} for s in ["SPY", "QQQ", "SMH", "DIA", "IWM", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"]}
    base["QQQ"][3] = qqq; base["SMH"][3] = smh; base["DIA"][3] = dia; base["RSP"][3] = rsp
    base["XLI"][3] = xli; base["XLF"][3] = xlf; base["XLV"][3] = xlv
    return base


def _row(sym, score, cluster=None):
    return {"symbol": sym, "score_final": score, "theme_cluster": cluster or theme_cluster_for(sym)}


def test_ai_off_rotation_classifier_and_relative_fields():
    ctx = classify_rotation_regime(_returns(), ai_basket_3d=-0.01)
    assert ctx["rotation_regime"] == "AI_OFF_ROTATION"
    assert ctx["qqq_vs_spy_3d"] < 0
    assert ctx["smh_vs_spy_3d"] < -0.01
    assert "INDUSTRIAL" in ctx["strongest_sectors"] or "FINANCIAL" in ctx["strongest_sectors"]


def test_ai_off_final30_ai_cap_and_non_tech_minimum():
    ai = [_row(f"AI{i}", 1 - i * .001, "AI_SEMI") for i in range(20)]
    non = []
    clusters = ["INDUSTRIAL", "FINANCIAL", "HEALTHCARE", "CONSUMER_STAPLES", "DEFENSIVE_UTILITY", "ENERGY_MATERIALS", "ETF_INDEX"]
    for i in range(40):
        non.append(_row(f"N{i}", .7 - i * .001, clusters[i % len(clusters)]))
    selected, meta = select_bucket_champions(ai + non, 30, "AI_OFF_ROTATION")
    ai_count = sum(1 for r in selected if r["theme_cluster"] in AI_CAP_CLUSTERS)
    nontech = sum(1 for r in selected if r["theme_cluster"] not in AI_CAP_CLUSTERS)
    assert ai_count <= 8  # 25~30% cap band for 30 names
    assert nontech >= 15
    assert meta["selected_by_bucket_champion"] is True


def test_ai_on_allows_larger_ai_weight():
    rows = [_row(f"AI{i}", 1 - i * .001, "AI_SEMI") for i in range(30)] + [_row(f"H{i}", .6 - i * .001, "HEALTHCARE") for i in range(10)]
    selected, _ = select_bucket_champions(rows, 30, "AI_ON")
    assert sum(1 for r in selected if r["theme_cluster"] in AI_CAP_CLUSTERS) > 8


def test_risk_off_defensive_quota_increases():
    rows = []
    for c in ["AI_SEMI", "HEALTHCARE", "CONSUMER_STAPLES", "DEFENSIVE_UTILITY", "FINANCIAL", "INDUSTRIAL"]:
        rows += [_row(f"{c}{i}", 1 - i * .001, c) for i in range(12)]
    selected, _ = select_bucket_champions(rows, 30, "RISK_OFF")
    defensive = sum(1 for r in selected if r["theme_cluster"] in {"HEALTHCARE", "CONSUMER_STAPLES", "DEFENSIVE_UTILITY"})
    ai = sum(1 for r in selected if r["theme_cluster"] in AI_CAP_CLUSTERS)
    assert defensive >= 18
    assert ai <= 4


def test_existing_ai_over_cap_blocks_new_ai():
    exposure = compute_cluster_exposure([{"symbol": "NVDA", "market_value_usd": 60_000}], equity=100_000)
    flagged = apply_cap_flags(exposure, "AI_OFF_ROTATION")
    blocked = {c for c, v in flagged.items() if v["over_cap"]}
    rows = [_row("NVDA2", 1.0, "AI_SEMI"), _row("JPM", .8, "FINANCIAL"), _row("CAT", .7, "INDUSTRIAL")]
    selected, _ = select_bucket_champions(rows, 2, "AI_OFF_ROTATION", blocked_clusters=blocked)
    assert "AI_SEMI" in blocked
    assert all(r["theme_cluster"] != "AI_SEMI" for r in selected)


def test_bucket_champion_not_simple_top_score():
    rows = [_row(f"AI{i}", 1 - i * .001, "AI_SEMI") for i in range(30)] + [_row(f"J{i}", .5 - i * .001, "FINANCIAL") for i in range(10)]
    selected, _ = select_bucket_champions(rows, 30, "AI_OFF_ROTATION")
    simple_top = rows[:30]
    assert [r["symbol"] for r in selected] != [r["symbol"] for r in simple_top]
    assert any(r["theme_cluster"] == "FINANCIAL" for r in selected)
