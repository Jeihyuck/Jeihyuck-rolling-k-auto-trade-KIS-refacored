"""US canonical price snapshots only. No order/balance/fill or native proof cache."""
from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import tempfile
import time

from trader.marketdata.rest_singleflight import read_or_fetch_scoped

def _valid(row, now):
    if not isinstance(row, dict) or str(row.get("rt_cd")) != "0":
        return False
    try:
        price = float(row["output"]["last"])
        at = float(row["_quote_asof_epoch"])
        return (row.get("_quote_source") == "KIS_LIVE"
                and row.get("_quote_quality") == "FRESH"
                and math.isfinite(price) and price>0 and math.isfinite(at)
                and 0<=now-at<=5)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False

def _paths(scope, symbol, exchange):
    if not scope or not symbol or not exchange:
        raise ValueError("US_REST_SHARED_SCOPE")
    root=Path("/tmp")/f"us-rest-price-{os.getuid()}"
    root.mkdir(mode=0o700,exist_ok=True)
    st=root.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid!=os.getuid() or stat.S_IMODE(st.st_mode)&0o077:
        raise ValueError("US_REST_SHARED_DIR")
    key=hashlib.sha256(json.dumps(["US","practice",scope,"/uapi/overseas-price/v1/quotations/price",
                                  symbol,exchange],separators=(",",":")).encode()).hexdigest()
    return root/(key+".lock"),root/(key+".json")

def _open_private(path,flags):
    fd=os.open(path,flags|os.O_NOFOLLOW,0o600)
    try:
        st=os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid() or stat.S_IMODE(st.st_mode)&0o077:
            raise ValueError("US_REST_SHARED_FILE")
        return fd
    except BaseException:
        os.close(fd)
        raise

def _read(path):
    try:
        with os.fdopen(_open_private(path,os.O_RDONLY),"r",encoding="utf-8") as stream:
            raw=stream.read(65537)
        if len(raw)>65536:
            return None
        row=json.loads(raw)
        now=time.time()
        if _valid(row,now):
            return {**row,"_quote_source":"KIS_CACHE","_quote_quality":"FRESH_CACHE",
                    "_quote_age_sec":now-float(row["_quote_asof_epoch"]),
                    "_shared_transport":"US_REST_SINGLEFLIGHT"}
    except (OSError,ValueError,TypeError):
        pass
    return None

def _publish(path,row):
    if not _valid(row,time.time()):
        return
    tmp=None
    try:
        data=json.dumps(row,allow_nan=False,separators=(",",":"))
        if len(data)>65536:
            return
        with tempfile.NamedTemporaryFile(mode="w",encoding="utf-8",dir=path.parent,delete=False) as f:
            tmp=Path(f.name)
            f.write(data)
        os.replace(tmp,path)
    except (OSError,ValueError,TypeError):
        pass
    finally:
        if tmp:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

def read_price(client, symbol, exchange):
    # Only canonical provider reads enter here. Client direct calls used by
    # final execution/proof paths are unchanged. Busy may use its original
    # bounded fallback; no fabricated receipt or successful empty response.
    if getattr(client,"_env",None)!="practice" or os.getenv("US_REST_SHARED_QUOTE_ENABLED","1")!="1":
        return "DISABLED",client.get_us_price(symbol,exchange)
    appkey=getattr(client,"_app_key",None)
    if not appkey:
        return "UNAVAILABLE",client.get_us_price(symbol,exchange)
    symbol=str(symbol).upper().strip()
    exchange=client._resolve_quote_excd(exchange)
    from trader.us.utils.session_guard import _child_fence_scope
    try:
        account=_child_fence_scope("practice")["account_digest"]
    except (KeyError,ValueError,TypeError):
        return "UNAVAILABLE",client.get_us_price(symbol,exchange)
    scope=hashlib.sha256((appkey+"\0"+account).encode()).hexdigest()
    status,row=read_or_fetch_scoped(paths=lambda:_paths(scope,symbol,exchange),
        open_private=_open_private,read=_read,publish=_publish,
        fetch=lambda:client.get_us_price(symbol,exchange))
    if status in {"BUSY","UNAVAILABLE"}:
        return status,client.get_us_price(symbol,exchange)
    return status,row
