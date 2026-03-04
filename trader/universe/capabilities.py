from __future__ import annotations

ALLOWED_PROVIDER_CHAIN = {
    # KIS provider 우선 사용 (FDR KRX LOGOUT 문제 회피)
    "practice": ["kis_marketcap_top", "seed_static", "fdr_kospi100_kosdaq100", "fdr_marketcap_top"],
    "real": ["kis_marketcap_top", "fdr_kospi100_kosdaq100", "fdr_marketcap_top", "seed_static"],
}


def providers_for_env(env: str) -> list[str]:
    normalized = (env or "practice").lower()
    return list(ALLOWED_PROVIDER_CHAIN.get(normalized, ALLOWED_PROVIDER_CHAIN["practice"]))
