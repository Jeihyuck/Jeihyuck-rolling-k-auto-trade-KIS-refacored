# KODEX 레버리지 무한매수 V2 검증 보고서

## Production policy

The isolated sleeve owns only `122630`, with KRW 15,000,000 split into 40 KRW
375,000 units and no more than one filled unit per trading day. Canonical regime
caps are 0/0/8/12/20/28/36/40 units in `STATE_ORDER`; defense states block routine
buying, caution requires recovery confirmation, the net-cost target baseline is a
10% full exit, and a broken long trend in defense reduces 50%. Runtime and replay
share the same decision engine and checksum.

## Data/replay status

No authoritative `price_daily` database, KIS credentials, or verified user CSV was
available in this workspace. Consequently no genuine listing-to-present data set,
2018/2020/2022/OOS/walk-forward result, candidate matrix, or objective final target
selection is claimed. The 10% target remains an explicitly provisional baseline.

Run on Ubuntu with database credentials and authoritative data:

```bash
python -m trader.kr.infinite.data_loader /path/to/verified-122630.csv
pytest -q tests/kr/infinite
```

Before readiness, record source, adjustment/corporate-action status, period, rows,
SHA-256, all requested matrix metrics, practice endpoint order/fill reconciliation,
and PostgreSQL two-process advisory-lock evidence. Keep the PR in Draft meanwhile.

## Runtime configuration

```dotenv
KR_INFINITE_ENABLED=1
KR_INFINITE_SYMBOL=122630
KR_INFINITE_CAPITAL_KRW=15000000
KR_INFINITE_UNITS=40
KR_INFINITE_UNIT_KRW=375000
KR_INFINITE_ALLOW_BUY=1
KR_INFINITE_ALLOW_SELL=1
KR_INFINITE_ORDER_MODE=INHERIT
```

Orders additionally require `LIVE_TRADING_ENABLED=1`,
`KR_LIVE_TRADING_ENABLED=1`, `KR_ORDER_ARMED=1`, `STRATEGY_MODE=LIVE`, and
all of `DRY_RUN`, `DISABLE_LIVE_TRADING`, and `FORCE_BLOCK_LIVE` to be zero.

## Remaining integration blockers

The canonical response parser, fill attribution, durable repository operations,
PB1 ownership boundaries, and multi-day replay core are implemented. A fully
authoritative runner invocation still requires exposing the already-open PB1 KIS,
database, balance, quote, and regime objects through a stable runner context; the
current runner does not return those objects. No empty/default evidence adapter is
used as a substitute. Staged/trailing matrix policies and PostgreSQL/KIS integration
evidence also remain incomplete, so this change must remain Draft.
