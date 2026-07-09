"""Korean market sector classification helpers for PR49."""
from __future__ import annotations

from collections import Counter
from typing import Any

KR_HIGH_BETA_CLUSTERS = {"BIO_HEALTHCARE", "SECONDARY_BATTERY", "AI_SOFTWARE", "ENTERTAINMENT_GAME", "HIGH_BETA_GROWTH"}
KR_DEFENSIVE_CLUSTERS = {"DEFENSIVE_CONSUMER", "TELECOM_UTILITY", "LOW_BETA_DEFENSIVE", "INDEX_CORE"}
KR_CYCLICAL_VALUE_CLUSTERS = {"AUTO", "FINANCIAL", "SHIPBUILDING_MACHINERY", "ENERGY_MATERIALS"}
KR_CORE_GROWTH_CLUSTERS = {"SEMICONDUCTOR", "AI_SOFTWARE"}
KR_SECTOR_CLUSTERS = KR_HIGH_BETA_CLUSTERS | KR_DEFENSIVE_CLUSTERS | KR_CYCLICAL_VALUE_CLUSTERS | KR_CORE_GROWTH_CLUSTERS | {"UNKNOWN"}

_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("SEMICONDUCTOR", ("반도체", "hbm", "파운드리", "장비", "소재", "삼성전자", "sk하이닉스")),
    ("AI_SOFTWARE", ("ai", "인공지능", "소프트웨어", "클라우드", "로봇")),
    ("BIO_HEALTHCARE", ("바이오", "제약", "헬스케어", "신약", "cdmo", "셀트리온")),
    ("SECONDARY_BATTERY", ("2차전지", "이차전지", "배터리", "양극재", "음극재", "전해액", "분리막")),
    ("AUTO", ("자동차", "현대차", "기아", "타이어", "전장", "부품")),
    ("FINANCIAL", ("금융", "은행", "보험", "증권", "금융지주", "카드")),
    ("SHIPBUILDING_MACHINERY", ("조선", "기계", "방산", "중공업", "중공")),
    ("DEFENSIVE_CONSUMER", ("음식료", "식품", "생활소비재", "필수소비재", "유통")),
    ("TELECOM_UTILITY", ("통신", "전력", "가스", "유틸리티", "전기")),
    ("ENERGY_MATERIALS", ("화학", "정유", "에너지", "철강", "소재")),
    ("ENTERTAINMENT_GAME", ("게임", "엔터", "미디어", "콘텐츠", "웹툰")),
    ("INDEX_CORE", ("kospi200", "kosdaq150", "코스피200", "코스닥150", "지수")),
]

def _text(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    return " ".join(str(row.get(k) or "") for k in keys).strip().lower()

def classify_kr_sector(row: dict) -> dict:
    row = row or {}
    explicit = str(row.get("sector_cluster") or row.get("cluster") or "").strip().upper()
    if explicit in KR_SECTOR_CLUSTERS:
        cluster, source, conf = explicit, "explicit_column", 1.0
    else:
        cluster, source, conf = "UNKNOWN", "unknown", 0.0
        for src, keys, score in (("industry", ("industry", "sector", "업종"), 0.85), ("theme", ("theme", "themes", "테마"), 0.75), ("name_keyword", ("name", "code_name", "종목명"), 0.65)):
            blob = _text(row, keys)
            for candidate, words in _KEYWORDS:
                if blob and any(w.lower() in blob for w in words):
                    cluster, source, conf = candidate, src, score
                    break
            if cluster != "UNKNOWN":
                break
    return {"sector_cluster": cluster, "sector_source": source, "sector_confidence": conf, "is_high_beta": cluster in KR_HIGH_BETA_CLUSTERS, "is_defensive": cluster in KR_DEFENSIVE_CLUSTERS}

def summarize_final30_clusters(rows: list[dict] | None) -> dict:
    rows = rows or []
    counts = Counter(classify_kr_sector(r)["sector_cluster"] for r in rows)
    total = max(1, len(rows))
    high_beta = sum(counts.get(c, 0) for c in KR_HIGH_BETA_CLUSTERS)
    unknown = counts.get("UNKNOWN", 0)
    return {"counts": dict(counts), "unknown_ratio": unknown / total, "high_beta_ratio": high_beta / total, "sector_context_suspect": unknown / total >= 0.40}
