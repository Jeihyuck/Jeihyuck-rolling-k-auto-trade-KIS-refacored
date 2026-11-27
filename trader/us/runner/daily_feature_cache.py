"""Exact completed daily MA/RS features for the US position trend fallback.

Only a verified prefix of the same day, account, owner and formula may advance.
No persisted trend state, quote, stop, risk decision or position authority is
cached. Preserve the original sum order and six-digit rounding; a rolling
subtraction/addition would introduce different floating point boundary values.
"""
import math
import os


def _metrics(tail, count, asof):
    def ma(n):
        return round(sum(tail[-n:]) / n, 6) if count >= n else None
    def rs(n):
        return round((tail[-1] - tail[-1-n]) / tail[-1-n], 6) if count > n and tail[-1-n] > 0 else None
    ma200 = ma(200)
    ma200_prev = round(sum(tail[-220:-20]) / 200, 6) if count >= 220 else None
    slope = round((ma200 - ma200_prev) / ma200_prev, 6) if ma200 and ma200_prev and ma200_prev > 0 else None
    return {"ma20":ma(20),"ma50":ma(50),"ma150":ma(150),"ma200":ma200,
            "ma200_slope":slope,"rs_20d":rs(20),"rs_60d":rs(60),"rs_120d":rs(120),
            "daily_bar_count":count,"daily_metrics_as_of":asof}


def reuse_daily_metrics(rows, trade_date, *, position, row_date, row_close, original):
    if (os.getenv("WSL_RUN_MARKET","").upper() != "US"
            or os.getenv("KIS_ENV","practice").lower() != "practice"
            or os.getenv("US_SHARED_NATIVE_FEATURES_ENABLED","1") != "1"
            or position.get("strategy_owner") != "US_STANDARD"):
        return original(rows,trade_date)
    fields = ("broker_account_hash","trading_epoch_id","symbol","exchange","strategy_owner")
    scope = tuple(str(position.get(k) or "") for k in fields)
    if not all(scope):
        return original(rows,trade_date)
    # Input comes from the existing completed price_daily repository. Do not
    # silently normalize a malformed sequence into incremental authority.
    from trader.marketdata.feature_reuse import shared_native_features
    def build(all_rows):
        value = original(all_rows,trade_date)
        completed = sorted((r for r in all_rows if isinstance(r,dict) and row_date(r)
                            and row_date(r) < str(trade_date)),key=row_date)
        closes = [c for r in completed if (c := row_close(r)) is not None]
        dates = [row_date(r) for r in completed]
        strict = (len(completed)==len(all_rows)==len(closes)
                  and all(a<b for a,b in zip(dates,dates[1:]))
                  and all(math.isfinite(c) and c>0 for c in closes))
        return {"metrics":value,"strict":strict,"tail":closes[-220:],
                "count":len(closes),"asof":value["daily_metrics_as_of"]}
    def advance(prior, all_rows, n):
        if not isinstance(prior,dict) or prior.get("strict") is not True or prior.get("count")!=n:
            return None
        tail = list(prior["tail"])
        if len(tail)!=min(n,220) or n<60 or not all(math.isfinite(v) and v>0 for v in tail):
            return None
        asof = prior["asof"]
        if row_date(all_rows[n-1])!=asof:
            return None
        for row in all_rows[n:]:
            if not isinstance(row,dict):
                return None
            at, close = row_date(row), row_close(row)
            if not at or not asof<at<str(trade_date) or close is None or not math.isfinite(close) or close<=0:
                return None
            tail.append(close)
            asof=at
        tail=tail[-220:]
        count=len(all_rows)
        return {"metrics":_metrics(tail,count,asof),"strict":True,
                "tail":tail,"count":count,"asof":asof}
    result = shared_native_features(
        market="US",scope=scope,trade_date=str(trade_date),
        settings=("US_COMPLETED_TREND_MA_RS_V1",20,50,150,200,220,6),
        rows=rows,compute=build,advance=advance,base="runtime/private/native-features")
    return result["metrics"]
