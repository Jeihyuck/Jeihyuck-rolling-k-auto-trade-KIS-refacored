from __future__ import annotations

# Canonical provider order. Runtime build._prefer_provider_order(), contract
# tests, and this capability declaration must describe the same source order.
# Live KIS market-cap data is the primary source; seed_static is a fallback;
# emergency_seed is last-resort only.
ALLOWED_PROVIDER_CHAIN = {
    "practice": ["kis_marketcap_top", "seed_static", "emergency_seed"],
    "real": ["kis_marketcap_top", "seed_static", "emergency_seed"],
}


def providers_for_env(env: str) -> list[str]:
    normalized = (env or "practice").lower()
    return list(ALLOWED_PROVIDER_CHAIN.get(normalized, ALLOWED_PROVIDER_CHAIN["practice"]))
