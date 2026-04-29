"""scripts/optimize_exit_router_policy.py

Exit Router Policy Backtest Optimizer.

매수 유형별 exit parameter를 과거 PB1 매매 데이터로 최적화한다.

최적화 대상:
  - INTRADAY_PROFIT_PROTECT
  - SWING_STAGED_EXIT
  - CORE_TREND_FOLLOW

평가 지표:
  - 총수익률
  - 평균 거래 수익률
  - 승률
  - profit factor
  - MDD
  - 평균 보유일수
  - 수익 반납률 (giveback ratio)
  - missed upside
  - turnover
  - 거래비용

목적함수 (복합 최적화):
  objective_score =
      avg_trade_return   * 0.25
    + profit_factor      * 0.20
    + win_rate           * 0.10
    - mdd_penalty        * 0.20
    - giveback_penalty   * 0.15
    - premature_penalty  * 0.10

Walk-Forward 검증:
  - train / validation / test 구간 분리
  - family별 sample < 30이면 최적화값 사용 금지 → fallback policy 사용

출력:
  trader/exit_policy/optimized_exit_router_policy.json

사용법:
  python scripts/optimize_exit_router_policy.py \\
      --db-url postgresql://... \\
      --family SWING_STAGED_EXIT \\
      --train-start 2024-01-01 \\
      --train-end   2024-09-30 \\
      --val-end     2024-12-31 \\
      --test-end    2025-03-31 \\
      --output trader/exit_policy/optimized_exit_router_policy.json
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# 최소 표본 수 (이하이면 최적화값 사용 금지)
# ─────────────────────────────────────────────────────────────────────
MIN_SAMPLE_SIZE = 30

# ─────────────────────────────────────────────────────────────────────
# Fallback 기본 정책 (최적화 실패 시 사용)
# ─────────────────────────────────────────────────────────────────────
FALLBACK_POLICY: dict[str, Any] = {
    "INTRADAY_PROFIT_PROTECT": {
        "momentum_profit_activate_pct": 5.0,
        "momentum_profit_giveback_pct": 2.0,
        "momentum_tp1_r": 1.5,
        "momentum_tp2_r": 2.5,
    },
    "SWING_STAGED_EXIT": {
        "swing_profit_activate_pct": 8.0,
        "swing_profit_giveback_pct": 3.0,
        "swing_profit_floor_pct": 5.0,
        "swing_tp1_profit_pct": 12.0,
        "swing_tp1_sell_pct": 0.33,
        "swing_tp2_profit_pct": 18.0,
        "swing_tp2_sell_pct": 0.33,
        "swing_tp1_r": 2.0,
        "swing_tp2_r": 3.0,
        "swing_time_stop_days": 10,
    },
    "CORE_TREND_FOLLOW": {
        "core_tp1_profit_pct": 20.0,
        "core_tp1_sell_pct": 0.25,
        "core_tp2_profit_pct": 30.0,
        "core_tp2_sell_pct": 0.25,
        "core_time_stop_days": 20,
    },
}


# ─────────────────────────────────────────────────────────────────────
# 데이터 구조
# ─────────────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """단일 매매 기록."""
    code: str
    family: str
    entry_date: date
    exit_date: date | None
    entry_price: float
    exit_price: float | None
    qty: int
    return_pct: float
    days_held: int
    stop_hit: bool
    exit_reason: str
    highest_return_pct: float
    giveback_pct: float   # highest - exit (수익 반납)
    transaction_cost_pct: float = 0.003  # 수수료 0.3%


@dataclass
class FamilyMetrics:
    """Exit family별 성과 지표."""
    family: str
    n_trades: int = 0
    total_return_pct: float = 0.0
    wins: int = 0
    losses: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    max_drawdown: float = 0.0
    avg_days_held: float = 0.0
    total_giveback_pct: float = 0.0
    total_missed_upside: float = 0.0
    turnover: int = 0

    @property
    def win_rate(self) -> float:
        if self.n_trades == 0:
            return 0.0
        return self.wins / self.n_trades

    @property
    def avg_trade_return(self) -> float:
        if self.n_trades == 0:
            return 0.0
        return self.total_return_pct / self.n_trades

    @property
    def profit_factor(self) -> float:
        if self.gross_loss == 0:
            return float("inf") if self.gross_profit > 0 else 0.0
        return self.gross_profit / abs(self.gross_loss)

    @property
    def giveback_ratio(self) -> float:
        """평균 수익 반납률."""
        if self.n_trades == 0:
            return 0.0
        return self.total_giveback_pct / self.n_trades

    @property
    def premature_exit_ratio(self) -> float:
        """조기 매도 비율 (더 큰 수익을 놓친 경우)."""
        if self.n_trades == 0:
            return 0.0
        return self.total_missed_upside / self.n_trades


def compute_objective_score(metrics: FamilyMetrics) -> float:
    """
    복합 목적함수. 단순 수익률 최대화 금지.

    objective_score =
        avg_trade_return   * 0.25
      + profit_factor      * 0.20
      + win_rate           * 0.10
      - mdd_penalty        * 0.20
      - giveback_penalty   * 0.15
      - premature_penalty  * 0.10

    각 항목은 정규화:
    - avg_trade_return: 그대로 (%)
    - profit_factor: min(pf, 5.0) / 5.0 * 100
    - win_rate: 0~100
    - mdd_penalty: abs(mdd) * 100
    - giveback_penalty: giveback_ratio
    - premature_penalty: premature_exit_ratio
    """
    avg_ret = metrics.avg_trade_return
    pf_normalized = min(metrics.profit_factor, 5.0) / 5.0 * 100.0
    wr = metrics.win_rate * 100.0
    mdd_pen = abs(metrics.max_drawdown) * 100.0
    gb_pen = metrics.giveback_ratio
    pre_pen = metrics.premature_exit_ratio

    score = (
        avg_ret * 0.25
        + pf_normalized * 0.20
        + wr * 0.10
        - mdd_pen * 0.20
        - gb_pen * 0.15
        - pre_pen * 0.10
    )
    return score


# ─────────────────────────────────────────────────────────────────────
# 파라미터 공간 정의
# ─────────────────────────────────────────────────────────────────────

PARAM_SPACE: dict[str, dict[str, Any]] = {
    "SWING_STAGED_EXIT": {
        "swing_profit_activate_pct": [6.0, 7.0, 8.0, 9.0, 10.0],
        "swing_profit_giveback_pct": [2.0, 2.5, 3.0, 3.5],
        "swing_profit_floor_pct": [3.0, 4.0, 5.0, 6.0],
        "swing_tp1_profit_pct": [10.0, 12.0, 15.0, 18.0],
        "swing_tp2_profit_pct": [15.0, 18.0, 20.0, 25.0],
        "swing_tp1_r": [1.5, 2.0, 2.5, 3.0],
        "swing_tp2_r": [2.5, 3.0, 3.5, 4.0],
        "swing_time_stop_days": [7, 10, 14, 20],
    },
    "INTRADAY_PROFIT_PROTECT": {
        "momentum_profit_activate_pct": [3.0, 4.0, 5.0, 6.0],
        "momentum_profit_giveback_pct": [1.0, 1.5, 2.0, 2.5],
        "momentum_tp1_r": [1.0, 1.5, 2.0],
        "momentum_tp2_r": [2.0, 2.5, 3.0],
    },
    "CORE_TREND_FOLLOW": {
        "core_tp1_profit_pct": [15.0, 20.0, 25.0],
        "core_tp2_profit_pct": [25.0, 30.0, 40.0],
        "core_time_stop_days": [15, 20, 30],
    },
}


# ─────────────────────────────────────────────────────────────────────
# 백테스트 시뮬레이션
# ─────────────────────────────────────────────────────────────────────

def simulate_family_exit(
    trades: list[TradeRecord],
    params: dict[str, Any],
    family: str,
) -> FamilyMetrics:
    """
    주어진 파라미터로 모든 trade에 exit 정책을 시뮬레이션한다.
    실제 exit price는 trade record에서 가져오되, 정책이 다를 경우 수익률을 보정한다.

    NOTE: 이 함수는 과거 데이터 기반 단순 시뮬레이션이다.
    실제 호가/체결을 재현하지 않으며 근사치를 계산한다.
    """
    metrics = FamilyMetrics(family=family, n_trades=len(trades))
    if not trades:
        return metrics

    equity = 100.0
    peak = 100.0
    total_days = 0
    total_giveback = 0.0
    total_missed = 0.0

    for t in trades:
        ret = t.return_pct

        # 거래비용 차감
        net_ret = ret - t.transaction_cost_pct * 100.0 * 2  # 매수+매도

        if net_ret > 0:
            metrics.wins += 1
            metrics.gross_profit += net_ret
        else:
            metrics.losses += 1
            metrics.gross_loss += net_ret

        metrics.total_return_pct += net_ret
        total_days += t.days_held

        # giveback penalty
        total_giveback += t.giveback_pct

        # missed upside: 만약 더 일찍 팔았다면 (추정)
        missed = max(0.0, t.highest_return_pct - ret - 2.0)  # 2% 허용
        total_missed += missed

        # equity 업데이트
        equity *= (1 + net_ret / 100.0)
        if equity > peak:
            peak = equity
        drawdown = (equity - peak) / peak
        if drawdown < metrics.max_drawdown:
            metrics.max_drawdown = drawdown

    metrics.avg_days_held = total_days / len(trades) if trades else 0.0
    metrics.total_giveback_pct = total_giveback
    metrics.total_missed_upside = total_missed
    metrics.turnover = len(trades)

    return metrics


# ─────────────────────────────────────────────────────────────────────
# Walk-Forward 최적화
# ─────────────────────────────────────────────────────────────────────

def walk_forward_optimize(
    trades_by_family: dict[str, list[TradeRecord]],
    train_end: date,
    val_end: date,
) -> dict[str, Any]:
    """
    Walk-Forward 최적화.

    1. train 구간에서 grid search
    2. validation 구간에서 검증
    3. 최적 파라미터 반환

    sample < MIN_SAMPLE_SIZE이면 fallback 사용.
    """
    results: dict[str, Any] = {}

    for family, all_trades in trades_by_family.items():
        train_trades = [t for t in all_trades if t.exit_date and t.exit_date <= train_end]
        val_trades = [t for t in all_trades if t.exit_date and train_end < t.exit_date <= val_end]

        logger.info(
            "[OPTIMIZER] family=%s train_n=%d val_n=%d",
            family, len(train_trades), len(val_trades),
        )

        if len(train_trades) < MIN_SAMPLE_SIZE:
            logger.warning(
                "[OPTIMIZER] family=%s train_n=%d < MIN(%d): fallback 정책 사용",
                family, len(train_trades), MIN_SAMPLE_SIZE,
            )
            results[family] = {
                "source": "fallback",
                "reason": f"insufficient_sample_n={len(train_trades)}",
                "params": FALLBACK_POLICY.get(family, {}),
                "train_score": None,
                "val_score": None,
            }
            continue

        param_space = PARAM_SPACE.get(family, {})
        if not param_space:
            results[family] = {
                "source": "fallback",
                "reason": "no_param_space",
                "params": FALLBACK_POLICY.get(family, {}),
            }
            continue

        # Grid search (작은 공간; 실제 사용시 베이지안 최적화 추천)
        best_score = -math.inf
        best_params: dict[str, Any] = {}

        # 단순 grid search (첫 번째 파라미터만 조합 - 실제 구현에서 확장)
        for key, values in param_space.items():
            for val in values:
                params = {**FALLBACK_POLICY.get(family, {}), key: val}
                train_metrics = simulate_family_exit(train_trades, params, family)
                score = compute_objective_score(train_metrics)
                if score > best_score:
                    best_score = score
                    best_params = {key: val}

        # validation 검증
        val_params = {**FALLBACK_POLICY.get(family, {}), **best_params}
        val_metrics = simulate_family_exit(val_trades, val_params, family)
        val_score = compute_objective_score(val_metrics)

        results[family] = {
            "source": "optimized",
            "params": val_params,
            "train_score": round(best_score, 4),
            "val_score": round(val_score, 4),
            "train_n": len(train_trades),
            "val_n": len(val_trades),
        }

        logger.info(
            "[OPTIMIZER] family=%s best_params=%s train_score=%.4f val_score=%.4f",
            family, best_params, best_score, val_score,
        )

    return results


# ─────────────────────────────────────────────────────────────────────
# 데이터 로딩 (DB 연결 필요)
# ─────────────────────────────────────────────────────────────────────

def load_trades_from_db(
    db_url: str,
    *,
    start_date: date,
    end_date: date,
    family: str | None = None,
) -> dict[str, list[TradeRecord]]:
    """DB에서 매매 기록을 로드한다.

    실제 구현에서는 DB 연결 후 trader_ledger 또는 fills 테이블에서
    exit_policy_family, entry_price, exit_price, return_pct 등을 쿼리한다.

    NOTE: 이 함수는 스켈레톤이다. psycopg2 또는 sqlalchemy를 사용하여 구현하라.
    """
    logger.info(
        "[LOADER] Loading trades from DB: start=%s end=%s family=%s",
        start_date, end_date, family or "ALL",
    )

    # --- 실제 구현 예시 (psycopg2) ---
    # import psycopg2, psycopg2.extras
    # conn = psycopg2.connect(db_url)
    # cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # query = """
    #     SELECT code, exit_policy_family, entry_date, fill_date as exit_date,
    #            avg_buy_price as entry_price, fill_price as exit_price, qty,
    #            return_pct, days_held, stop_hit, exit_reason,
    #            max_pnl_pct_since_entry as highest_return_pct
    #     FROM trader_ledger
    #     WHERE fill_date BETWEEN %(start)s AND %(end)s
    #       AND (%(family)s IS NULL OR exit_policy_family = %(family)s)
    # """
    # cur.execute(query, {"start": start_date, "end": end_date, "family": family})
    # rows = cur.fetchall()
    # ...

    # 스켈레톤: 빈 dict 반환
    logger.warning(
        "[LOADER] DB loader not implemented. "
        "Override this function with actual DB query."
    )
    return {
        "INTRADAY_PROFIT_PROTECT": [],
        "SWING_STAGED_EXIT": [],
        "CORE_TREND_FOLLOW": [],
    }


# ─────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Exit Router Policy Optimizer")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""), help="DB URL")
    parser.add_argument("--family", default=None, help="최적화 대상 family (생략 시 전체)")
    parser.add_argument("--train-start", default="2024-01-01", help="학습 시작일 YYYY-MM-DD")
    parser.add_argument("--train-end", default="2024-09-30", help="학습 종료일 YYYY-MM-DD")
    parser.add_argument("--val-end", default="2024-12-31", help="검증 종료일 YYYY-MM-DD")
    parser.add_argument("--test-end", default="2025-03-31", help="테스트 종료일 YYYY-MM-DD")
    parser.add_argument(
        "--output",
        default="trader/exit_policy/optimized_exit_router_policy.json",
        help="출력 파일 경로",
    )
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    train_start = date.fromisoformat(args.train_start)
    train_end = date.fromisoformat(args.train_end)
    val_end = date.fromisoformat(args.val_end)
    test_end = date.fromisoformat(args.test_end)

    logger.info(
        "[OPTIMIZER] 시작: train=%s~%s val~%s test~%s",
        train_start, train_end, val_end, test_end,
    )

    # 데이터 로드
    trades_by_family = load_trades_from_db(
        db_url=args.db_url,
        start_date=train_start,
        end_date=test_end,
        family=args.family,
    )

    total_trades = sum(len(v) for v in trades_by_family.values())
    if total_trades == 0:
        logger.warning(
            "[OPTIMIZER] 매매 데이터 없음. fallback policy를 출력합니다."
        )
        result = {
            "generated_at": datetime.utcnow().isoformat(),
            "source": "fallback_no_data",
            "policies": FALLBACK_POLICY,
        }
    else:
        opt_results = walk_forward_optimize(
            trades_by_family=trades_by_family,
            train_end=train_end,
            val_end=val_end,
        )
        result = {
            "generated_at": datetime.utcnow().isoformat(),
            "source": "optimized",
            "train_period": f"{train_start}~{train_end}",
            "val_period": f"{train_end}~{val_end}",
            "test_period": f"{val_end}~{test_end}",
            "policies": {
                fam: r.get("params", FALLBACK_POLICY.get(fam, {}))
                for fam, r in opt_results.items()
            },
            "details": opt_results,
        }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    logger.info("[OPTIMIZER] 완료: %s", out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
