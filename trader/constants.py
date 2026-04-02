from __future__ import annotations

REQUIRED_FINAL30_SCORED_COLS = [
    "score_final",
    "tech_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "rs_percentile",
    "vcp_score",
    "entry_style_selected",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
]

FLOW_OPTIONAL_COLS = [
    "investor_flow",
    "foreign_net_buy",
    "institutional_net_buy",
    "program_trade",
    "flow_score",
    "flow_rank",
    "flow_reason",
]

OPTIONAL_FLOW_COLS = FLOW_OPTIONAL_COLS

FINAL30_SCORED_IDENTITY_COLS = [
    "as_of",
    "code",
    "name",
    "score",
]

CRITICAL_SCORED_COLS = [
    *REQUIRED_FINAL30_SCORED_COLS,
    "close",
]

FINAL30_SCORED_PERSIST_COLS = [
    *FINAL30_SCORED_IDENTITY_COLS,
    *REQUIRED_FINAL30_SCORED_COLS,
    "rank",
    "rank_pool120",
    "rank_top50",
    "rank_final30",
    "score_flow",
    "score_liq",
    "score_tech",
    "flow_score",
    "final_score",
    "entry_component",
    "rs_pctile",
    "rs_score",
    "trend_score",
    "pullback_pct",
    "foreign_20_ratio",
    "inst_20_ratio",
    "liq_avg",
    "last_close",
    "close",
    "volume",
    "volume_avg20",
    "rows",
    "meta",
    "scores",
    "reasons",
    "reject_reasons",
    "filters_passed",
    "filters_failed",
]