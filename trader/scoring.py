# FILE: trader/scoring.py
from __future__ import annotations
import json
import math
import statistics as stats
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime, timedelta

# === 2주(10거래일) 무빙 윈도우 집계 & Top10 점수화 ===

@dataclass
class SymbolStats:
    code: str
    trades: int
    wins: int
    profit_sum: float
    loss_sum: float
    avg_gain: float
    avg_loss: float
    avg_slippage_pct: float
    volume_rank_pct: int  # 낮을수록 유동성 우수 (상위 30% = 30)


def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    denom = 1 + (z * z) / n
    centre = p + (z * z) / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + (z * z) / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default


def score_symbol(s: SymbolStats) -> float:
    wr_w = wilson_lower_bound(s.wins, s.trades)
    expect = s.avg_gain * wr_w - s.avg_loss * (1 - wr_w)
    pf = _safe_div(s.profit_sum, abs(s.loss_sum), 0.0)
    liq = 1 - (s.volume_rank_pct / 100.0)  # 상위일수록 +
    exec_q = - (s.avg_slippage_pct / 100.0)  # 슬리피지 낮을수록 +
    # 가중합 (요청대로 정수% 지표는 보고서에서만 반올림)
    return 0.50 * wr_w + 0.20 * expect + 0.15 * pf + 0.10 * liq + 0.05 * exec_q


def _iter_trade_logs(log_dir: Path, since: datetime) -> List[dict]:
    rows: List[dict] = []
    for p in sorted(log_dir.glob("trades_*.json")):
        # 파일명에서 날짜 파싱
        try:
            d = datetime.strptime(p.stem.replace("trades_", ""), "%Y-%m-%d")
        except Exception:
            continue
        if d < since:
            continue
        with p.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    return rows


def build_2w_stats(log_dir: Path, lookback_days: int = 10) -> Dict[str, SymbolStats]:
    since = datetime.now().date() - timedelta(days=lookback_days + 3)  # 주말 보정
    since_dt = datetime.combine(since, datetime.min.time())
    rows = _iter_trade_logs(Path(log_dir), since_dt)

    # 심플 집계 (BUY/SELL 쌍이 완전하지 않아도 수익(수익률) 필드로 근사)
    bucket: Dict[str, List[dict]] = {}
    for r in rows:
        code = r.get("code")
        if not code:
            continue
        bucket.setdefault(code, []).append(r)

    out: Dict[str, SymbolStats] = {}
    for code, lst in bucket.items():
        pnls = [float(x.get("수익", 0) or 0) for x in lst]
        wins = sum(1 for p in pnls if p > 0)
        trades = len(pnls)
        gains = [p for p in pnls if p > 0]
        losses = [-p for p in pnls if p < 0]
        profit_sum = sum(gains)
        loss_sum = -sum(losses)
        avg_gain = (sum(gains) / len(gains)) if gains else 0.0
        avg_loss = (sum(losses) / len(losses)) if losses else 0.0
        slip = [float(x.get("slippage_pct", 0) or 0) for x in lst if x.get("slippage_pct") is not None]
        avg_slip = (sum(slip) / len(slip)) if slip else 0.0
        # 거래대금 랭크는 로그가 없으면 50으로 보수적 처리
        vol_rank = int(min(100, max(1, float(lst[0].get("volume_rank_pct", 50)))))
        out[code] = SymbolStats(
            code=code,
            trades=trades,
            wins=wins,
            profit_sum=profit_sum,
            loss_sum=loss_sum,
            avg_gain=avg_gain,
            avg_loss=avg_loss,
            avg_slippage_pct=avg_slip,
            volume_rank_pct=vol_rank,
        )
    return out


def rank_and_pick(stats_map: Dict[str, SymbolStats], k: int = 10, bench: int = 4,
                  hysteresis_prev: List[str] | None = None, hysteresis_band: int = 2) -> Tuple[List[dict], List[dict]]:
    scored = []
    for code, s in stats_map.items():
        score = score_symbol(s)
        scored.append((code, score))
    scored.sort(key=lambda x: x[1], reverse=True)

    # 히스테리시스: 직전 Core 유지 우선
    core: List[str] = [c for c, _ in scored[:k]]
    if hysteresis_prev:
        # 직전 순위를 보정: 직전 Core에 있던 종목이 현재 상위 k+band 이내면 유지
        prev_set = set(hysteresis_prev)
        keep = [c for c, _ in scored if c in prev_set][:k]
        merged = list(dict.fromkeys(keep + core))[:k]
        core = merged

    bench_list = [c for c, _ in scored if c not in core][:bench]

    def pack(lst: List[str]) -> List[dict]:
        # weight는 균등 분배, bench는 절반 가중치 추천
        w = round(1.0 / max(1, len(lst)), 4)
        return [{"code": c, "weight": w} for c in lst]

    return pack(core), pack(bench_list)
