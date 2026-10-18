"""Exercise the actual async socket loop with a broker double, never KIS."""
import asyncio
import json

import pytest
import websockets

from trader.marketdata.kis_ws_price import KisWebSocketPriceService


@pytest.mark.parametrize('cap,holdings_count', [(2, 4), (40, 80)])
@pytest.mark.parametrize('unsubscribe_reply', ['ack', 'reject', 'timeout'])
def test_rebalance_observes_broker_slot_lifetime_and_recovers(monkeypatch, cap, holdings_count, unsubscribe_reply):
    service = KisWebSocketPriceService()
    monkeypatch.setattr(service, 'network_allowed', lambda _: True)
    monkeypatch.setattr(service, '_ensure_started', lambda: None)
    monkeypatch.setattr(service, '_approval_key', lambda: 'offline-key')
    monkeypatch.setenv('KIS_WS_MAX_SUBSCRIPTIONS', str(cap))
    monkeypatch.setenv('KIS_WS_SUBSCRIBE_INTERVAL_SEC', '0')
    monkeypatch.setenv('KIS_WS_UNSUBSCRIBE_ACK_TIMEOUT_SEC', '0.01')
    monkeypatch.setenv('KIS_WS_RECONNECT_MAX_SEC', '0.001')
    initial = [f'{10000+i:06}' for i in range(cap)]
    owners = [f'{20000+i:06}' for i in range(holdings_count)]
    for symbol in initial:
        service.subscribe_kr(symbol)
    connections = []
    events = []

    class Socket:
        def __init__(self):
            self.active = set()
            self.messages = []
            self.changed = False
            connections.append(self)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            events.append(('disconnect', len(self.active)))
            self.active.clear()
        async def send(self, raw):
            payload = json.loads(raw)
            key = payload['body']['input']['tr_key']
            action = payload['header']['tr_type']
            header = {'tr_id': service.KR_TR_ID, 'tr_key': key}
            if action == '1':
                self.active.add(key)
                assert len(self.active) <= cap, 'replacement sent before broker freed a slot'
                self.messages.append((header, '0', None))
                events.append(('subscribe', key))
                if self.active == set(initial) and not self.changed:
                    self.changed = True
                    service.prioritize_kr_symbols(holdings=owners, unresolved_orders=[], final30=[])
                if self.active == set(owners[:cap]):
                    service._stop.set()
            else:
                events.append(('unsubscribe_sent', key))
                # Broker still owns the slot until ACK is read.
                assert key in self.active
                if unsubscribe_reply == 'ack':
                    self.messages.append((header, '0', key))
                elif unsubscribe_reply == 'reject':
                    self.messages.append((header, '1', None))
        async def recv(self):
            if not self.messages:
                await asyncio.sleep(.002)
                raise asyncio.TimeoutError
            header, rt_cd, removed = self.messages.pop(0)
            if removed:
                self.active.remove(removed)
                events.append(('unsubscribe_ack', removed))
            return json.dumps({'header': header, 'body': {'rt_cd': rt_cd}})

    monkeypatch.setattr(websockets, 'connect', lambda *a, **k: Socket())
    asyncio.run(asyncio.wait_for(service._run_forever(), timeout=8))
    assert service._stop.is_set()
    assert len(service._desired) == cap + holdings_count
    # Overflow remains desired but is never subscribed beyond the 40-slot cap.
    assert not any(kind == 'subscribe' and symbol in owners[cap:] for kind, symbol in events)
    assert ('unsubscribe_sent', next(v for k,v in events if k == 'unsubscribe_sent')) in events
    if unsubscribe_reply == 'ack':
        assert len(connections) == 1
        assert sum(k == 'unsubscribe_ack' for k,_ in events) == cap
    else:
        assert len(connections) >= 2
        first_owner = next(i for i,(kind,key) in enumerate(events) if kind == 'subscribe' and key in owners)
        assert any(kind == 'disconnect' for kind,_ in events[:first_owner])
    assert service._pending_unsubscribes == set()


def test_failed_unsubscribe_does_not_create_retry_or_false_subscription_ack():
    service = KisWebSocketPriceService()
    sub = (service.KR_TR_ID, '005930')
    service._pending_unsubscribes.add(sub)
    service._broker_subscriptions_acked.add(sub)
    service.put_quote(market='KR', symbol='005930', last=70000)
    rejected = json.dumps({'header': {'tr_id': sub[0], 'tr_key': sub[1]}, 'body': {'rt_cd': '1'}})
    with pytest.raises(RuntimeError, match='UNSUBSCRIBE_REJECTED'):
        asyncio.run(service._handle_message(None, rejected))
    assert sub in service._pending_unsubscribes
    assert sub not in service._retry_subscriptions


def test_80_owners_keep_40_ws_and_40_real_rest_fallback_paths(monkeypatch):
    from types import SimpleNamespace
    import trader.kis_wrapper as wrapper
    service = KisWebSocketPriceService()
    monkeypatch.setattr(service, 'network_allowed', lambda _: True)
    monkeypatch.setattr(service, '_ensure_started', lambda: None)
    monkeypatch.setenv('KIS_WS_MAX_SUBSCRIPTIONS', '40')
    owners = [f'{30000+i:06}' for i in range(80)]
    service.prioritize_kr_symbols(holdings=owners, unresolved_orders=[], final30=[])
    for code in owners[:40]:
        service.put_quote(market='KR', symbol=code, last=70000)
    monkeypatch.setattr(wrapper, 'get_kis_ws_price_service', lambda: service)
    monkeypatch.setattr(wrapper, '_price_cache', wrapper._PriceCache(15, 30))
    monkeypatch.setattr(wrapper, '_price_rl', SimpleNamespace(acquire=lambda: 0))
    monkeypatch.setattr(wrapper, '_PRICE_JITTER_MAX_SEC', 0)
    api = wrapper.KisAPI.__new__(wrapper.KisAPI)
    rest_calls = []
    def rest(code, **kw):
        rest_calls.append(code)
        return {'last': 70000, 'rt_cd': '0'}
    api.get_price_quote = rest
    assert all(api.get_price_snapshot(code)['last'] == 70000 for code in owners)
    assert rest_calls == owners[40:]


def test_rest_overflow_does_not_sleep_past_shared_tick_budget(monkeypatch):
    from types import SimpleNamespace
    import trader.kis_wrapper as wrapper
    service = KisWebSocketPriceService()
    monkeypatch.setattr(service, 'subscribe_kr', lambda code: None)
    monkeypatch.setattr(wrapper, 'get_kis_ws_price_service', lambda: service)
    monkeypatch.setattr(wrapper, '_price_cache', wrapper._PriceCache(15, 30))
    monkeypatch.setattr(wrapper, '_price_rl', SimpleNamespace(acquire=lambda: 5))
    monkeypatch.setattr(wrapper, 'kr_tick_remaining_sec', lambda *args: .1)
    monkeypatch.setattr(wrapper.time, 'sleep', lambda delay: pytest.fail('sleep exceeds tick'))
    api = wrapper.KisAPI.__new__(wrapper.KisAPI)
    api.get_price_quote = lambda *a, **k: pytest.fail('REST must not execute after rejected wait')
    assert api.get_price_snapshot('005930') == {}
    assert not wrapper._price_cache.inflight


def test_inflight_join_respects_shared_remaining_budget(monkeypatch):
    from types import SimpleNamespace
    import trader.kis_wrapper as wrapper
    monkeypatch.setattr(wrapper, 'kr_tick_remaining_sec', lambda: .1)
    timeouts = []
    event = SimpleNamespace(wait=lambda **kw: timeouts.append(kw['timeout']) or False)
    assert wrapper._PriceCache(15, 30).wait_inflight(('J','005930'), event) == {}
    assert timeouts == [pytest.approx(.05)]
