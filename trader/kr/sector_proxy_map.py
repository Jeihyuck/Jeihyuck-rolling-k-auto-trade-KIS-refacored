"""KR sector proxy map and return resolver for PR49."""
from __future__ import annotations
import copy, json, logging, os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

KR_SECTOR_PROXY_MAP = {
 "SEMICONDUCTOR":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["반도체","HBM","장비","소재","파운드리"]},
 "BIO_HEALTHCARE":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["바이오","제약","헬스케어","신약","CDMO"]},
 "SECONDARY_BATTERY":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["2차전지","배터리","양극재","음극재","전해액","분리막"]},
 "FINANCIAL":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["은행","보험","증권","금융지주"]},
 "AUTO":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["자동차","부품","타이어","전장"]},
 "SHIPBUILDING_MACHINERY":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["조선","기계","방산","중공업"]},
 "DEFENSIVE_CONSUMER":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["음식료","생활소비재","필수소비재"]},
 "TELECOM_UTILITY":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["통신","전력","가스","유틸리티"]},
 "ENTERTAINMENT_GAME":{"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":["게임","엔터","미디어","콘텐츠"]},
}

def load_kr_sector_proxy_map(path: str | None = None) -> dict:
    cfg = copy.deepcopy(KR_SECTOR_PROXY_MAP)
    p = path or os.getenv("KR_SECTOR_PROXY_CONFIG_PATH")
    if p and Path(p).exists():
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        for sector, override in data.items():
            base = cfg.setdefault(sector, {"primary_index":None,"etf_proxies":[],"basket_symbols":[],"fallback_keywords":[]})
            if isinstance(override, dict): base.update(override)
    return cfg

def _ret(provider: Any, symbol: str, trade_date: str, lb: int) -> float | None:
    for name in ("get_return", "get_index_return", "return_pct"):
        fn = getattr(provider, name, None)
        if callable(fn):
            try: return float(fn(symbol=symbol, trade_date=trade_date, lookback=lb))
            except TypeError:
                try: return float(fn(symbol, trade_date, lb))
                except Exception: pass
            except Exception: pass
    fn = getattr(provider, "_kr_return_from_daily", None)
    if callable(fn):
        try: return float(fn(symbol, lb))
        except TypeError:
            try: return float(fn(symbol=symbol, lookback=lb))
            except Exception: pass
        except Exception: pass
    data = getattr(provider, "returns", None) or getattr(provider, "data", None) or {}
    for key in ((symbol, lb), (symbol, f"return_{lb}d"), f"{symbol}_{lb}d", symbol):
        if key in data:
            val = data[key]
            if isinstance(val, dict): val = val.get(f"return_{lb}d") or val.get(lb)
            try: return float(val)
            except Exception: return None
    return None

def compute_sector_proxy_return(*, sector: str, provider, trade_date: str, lookbacks: tuple[int, ...]=(1,3,5,20), index_context: dict | None=None) -> dict:
    cfg = load_kr_sector_proxy_map().get(sector, {})
    source, quality, symbols = "keyword_only", "low", []
    if cfg.get("primary_index"):
        source, quality, symbols = "krx_index", "high", [cfg["primary_index"]]
    elif cfg.get("etf_proxies"):
        source, quality, symbols = "etf_proxy", "high", list(cfg["etf_proxies"])
    elif cfg.get("basket_symbols") and os.getenv("KR_SECTOR_PROXY_ALLOW_BASKET_FALLBACK", "1") != "0":
        source, quality, symbols = "basket", "medium", list(cfg["basket_symbols"])
    out = {"sector": sector, "source": source, "source_quality": quality}
    for lb in lookbacks:
        vals = [_ret(provider, s, trade_date, lb) for s in symbols]
        vals = [v for v in vals if v is not None]
        out[f"return_{lb}d"] = (sum(vals)/len(vals)) if vals else None
    idx = index_context or {}
    r3 = out.get("return_3d")
    for idx_name, key in (("kospi","kospi_3d_return"),("kosdaq","kosdaq_3d_return"),("kospi200","kospi200_3d_return"),("kosdaq150","kosdaq150_3d_return")):
        base = idx.get(key)
        out[f"vs_{idx_name}_3d"] = round(r3 - float(base), 10) if r3 is not None and base is not None else None
    logger.info(
        "[KR_SECTOR_PROXY][RETURN] sector=%s source=%s source_quality=%s return_1d=%s return_3d=%s return_5d=%s vs_kospi_3d=%s vs_kosdaq_3d=%s vs_kospi200_3d=%s vs_kosdaq150_3d=%s",
        sector, out.get("source"), out.get("source_quality"), out.get("return_1d"), out.get("return_3d"), out.get("return_5d"), out.get("vs_kospi_3d"), out.get("vs_kosdaq_3d"), out.get("vs_kospi200_3d"), out.get("vs_kosdaq150_3d"),
    )
    return out
