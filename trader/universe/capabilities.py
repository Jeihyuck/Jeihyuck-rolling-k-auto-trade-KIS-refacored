from __future__ import annotations

ALLOWED_PROVIDER_CHAIN = {
    # FDR 제거 - KIS와 seed_static만 사용 (FDR KRX scraping 실패 문제 해결)
    "practice": ["kis_marketcap_top", "seed_static"],
    "real": ["kis_marketcap_top", "seed_static"],
}


def providers_for_env(env: str) -> list[str]:
    normalized = (env or "practice").lower()
    return list(ALLOWED_PROVIDER_CHAIN.get(normalized, ALLOWED_PROVIDER_CHAIN["practice"]))
