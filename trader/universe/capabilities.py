from __future__ import annotations

ALLOWED_PROVIDER_CHAIN = {
    "practice": ["seed_static", "kis_marketcap_top", "emergency_seed"],
    "real": ["kis_marketcap_top", "seed_static", "emergency_seed"],
}


def providers_for_env(env: str) -> list[str]:
    normalized = (env or "practice").lower()
    return list(ALLOWED_PROVIDER_CHAIN.get(normalized, ALLOWED_PROVIDER_CHAIN["practice"]))
