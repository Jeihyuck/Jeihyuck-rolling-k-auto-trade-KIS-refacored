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
