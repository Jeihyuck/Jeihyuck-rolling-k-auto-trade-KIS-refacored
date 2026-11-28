"""Practice quote IPC two-process acceptance with no native KIS connection."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import threading
import uuid

from trader.marketdata import kr_ws_shared

KST = timezone(timedelta(hours=9))

CLIENT = r"""
import json,sys
from trader.marketdata.kr_ws_shared import read_owner_quote
class Client:
    def _credentials(self):
        return sys.argv[1], "fixture_secret"
    def network_allowed(self,market):
        return market=="KR"
used, quote=read_owner_quote(Client(),"005930",max_age_sec=5,
    timeout_sec=.18,required_fields=("last","bid","ask"))
print(json.dumps({"used":used,"quote":quote},default=str))
"""


def test_existing_ws_owner_supplies_separate_process_without_second_kis_connection(monkeypatch):
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("KR_WS_SHARED_QUOTE_ENABLED","1")
    appkey = "fixture-" + uuid.uuid4().hex
    calls = []
    class Owner:
        def __init__(self):
            self._connected = threading.Event()
            self._connected.set()
            self.ask = 10010
            self.day_override = None
        def _credentials(self):
            return appkey, "fixture_secret"
        def network_allowed(self,market):
            return market == "KR"
        def subscribe_kr(self, symbol):
            calls.append(("subscribe", symbol))
        def get_fresh_quote(self, market, code, *, max_age_sec):
            import time
            now = datetime.now(KST)
            calls.append(("quote",market,code))
            return {"source":"KIS_WEBSOCKET",
                    "source_identity":"KIS_WS_PARSED_SYMBOL",
                    "market":"KR","symbol":code,"requested_symbol":code,
                    "last":10000,"bid":9990,"ask":self.ask,
                    "received_at":time.time(),
                    "stck_bsop_date":self.day_override or now.strftime("%Y%m%d"),
                    "stck_cntg_hour":now.strftime("%H%M%S")}

    async def run():
        owner=Owner()
        handle=await kr_ws_shared.start_owner_server(owner)
        path=handle[1]
        assert path.exists()
        assert (path.stat().st_mode & 0o077) == 0
        async def client(key):
            proc=await asyncio.create_subprocess_exec(
                sys.executable,"-c",CLIENT,key,
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,
            )
            stdout,stderr=await asyncio.wait_for(proc.communicate(),timeout=5)
            assert proc.returncode == 0, stderr.decode()[:500]
            return json.loads(stdout.decode())
        try:
            received=await client(appkey)
            assert received["used"] is True
            q=received["quote"]
            assert q["shared_transport"] == "KR_OWNER_UNIX_SOCKET"
            assert q["source"] == "KIS_WEBSOCKET"
            assert q["symbol"] == "005930"
            assert q["last"] == 10000 and q["bid"] == 9990 and q["ask"] == 10010
            assert q["shared_owner_pid"] == os.getpid()
            assert q["shared_generation"] == handle[3]
            assert calls == [("subscribe","005930"),("quote","KR","005930")]
            # Credential isolation: different broker account cannot access
            # the owner's shared socket or request a duplicate subscription.
            other=await client(appkey+"-other")
            assert other == {"used":False,"quote":None}
            assert len(calls)==2
            owner.ask=None
            partial=await client(appkey)
            assert partial == {"used":True,"quote":None}
            owner.day_override=(datetime.now(KST)-timedelta(days=1)).strftime("%Y%m%d")
            stale=await client(appkey)
            assert stale == {"used":True,"quote":None}
        finally:
            await kr_ws_shared.close_owner_server(handle)
            assert not path.exists()
        gone=await client(appkey)
        assert gone == {"used":False,"quote":None}
    asyncio.run(run())
