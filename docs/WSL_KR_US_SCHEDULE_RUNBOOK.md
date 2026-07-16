# WSL KR/US Schedule Runbook

## 운영 원칙

- Windows Task Scheduler is the only automatic scheduler for WSL trading.
- WSL cron must not run trading sessions under WSL.
- GitHub Actions workflows are manual diagnostic/smoke/report workflows only.
- `active_scheduler=windows_task_scheduler`를 schedule health/report에 기록합니다.
- `runtime/cron/us-prep.log`는 legacy cron 로그이며 신규 판단 기준은 `runtime/wsl-us-prep.log`입니다.
- GitHub Actions schedule은 한국장/미국장 주문 가능 workflow에서 모두 제거되었습니다.
- GitHub Actions는 `workflow_dispatch` 수동 실행만 가능합니다.
- GitHub Actions 수동 실행은 기본 `INTENT_ONLY`/`DRY_RUN` 안전모드입니다.
- 실제 모의투자 자동매매는 Ubuntu WSL에서 실행합니다.
- `.env`는 GitHub에 commit하지 않습니다.

## Windows 작업 스케줄러 → WSL 실행

- 한국장: Windows 작업 스케줄러가 prep/am/afternoon/close 전용 스크립트를 호출합니다.
- 미국장: Windows 작업 스케줄러가 세션별로 아래 스크립트를 각각 호출합니다.
  - Prep Prewarm EDT: 19:30 KST `scripts/wsl/run-us-prep.sh`
  - Prep Prewarm EST: 20:30 KST `scripts/wsl/run-us-prep.sh`
  - Prep Regular: 21:30 KST `scripts/wsl/run-us-prep.sh`
  - Prep Recovery: 22:10 KST `scripts/wsl/run-us-prep-recovery.sh`
  - AM Preflight: 22:20 KST `scripts/wsl/check-us-prep-before-am.sh`
  - AM: 22:30 KST `scripts/wsl/run-us-am.sh`
  - Afternoon: `scripts/wsl/run-us-afternoon.sh`
  - Close: `scripts/wsl/run-us-close.sh`
  - Mail: 07:00 KST `scripts/wsl/send-market-log-mail.sh us`
  - Health: 07:10 KST `scripts/wsl/check-nullim-day-health.sh us`
- `scripts/wsl/run-us-trader.sh`는 ET 현재 시간 또는 첫 번째 인자(`prep`, `am`, `afternoon`, `close`)로 위 세션별 스크립트를 호출하는 dispatcher wrapper입니다.
- 모든 WSL 스크립트는 `/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored/.env`를 읽고 `runtime/`에 로그를 남깁니다.

## 검증 순서

1. `.env.example`을 참고해 WSL 서버에 `.env`를 생성합니다.
2. 먼저 `scripts/wsl/run-kr-dryrun.sh`와 `scripts/wsl/run-us-dryrun.sh`로 dispatcher dry-run을 검증합니다.
3. 미국장은 세션별 스크립트도 dry-run 환경값(`DRY_RUN=1`, `US_LIVE_TRADING_ENABLED=0`)에서 개별 검증합니다.
4. 로그 파일을 확인합니다.
   - 한국장: `runtime/wsl-kr-trader.log`
   - 미국장 dispatcher: `runtime/wsl-us-trader.log`
   - 미국장 prep: `runtime/wsl-us-prep.log`
   - 미국장 AM: `runtime/wsl-us-am.log`
   - 미국장 afternoon: `runtime/wsl-us-afternoon.log`
   - 미국장 close: `runtime/wsl-us-close.log`
5. 이후 practice 주문 모드는 `.env`에서 아래 값으로 전환합니다.

```env
DRY_RUN=0
DISABLE_LIVE_TRADING=0
LIVE_TRADING_ENABLED=1
STRATEGY_MODE=LIVE
FORCE_STRATEGY_MODE=LIVE
```

## 중복 주문 방지 체크리스트

- GitHub Actions의 주문 가능 workflow에는 `on.schedule`이 없어야 합니다.
- Windows 작업 스케줄러의 KR/US 작업만 활성화합니다.
- GitHub Actions 수동 실행은 진단, 백업, 리포트, CI 용도로만 사용합니다.

## 06:30 ET prewarm 처리

GitHub dispatcher의 06:30 ET phase resolver는 `workflow_dispatch` 수동 진단용입니다. 자동매매 prewarm은 Windows Task Scheduler의 `PB1 US Prep Prewarm EDT WSL`(19:30 KST) 및 `PB1 US Prep Prewarm EST WSL`(20:30 KST) 작업으로 이관합니다. 같은 NY trade_date에 정상 prep contract가 있으면 prewarm은 no-op로 취급하고, 재생성은 `US_FORCE_REBUILD_PREP=1`에서만 허용합니다.

## Prep missing safe mode

AM preflight는 same-day prep contract, DB prep status, locked watchlist count를 확인하고 누락 시 recovery를 1회 시도합니다. Recovery cutoff(22:25 KST) 이후 또는 recovery 실패 시 `runtime/health/us-prep-missing-{trade_date}.json` marker를 남기며 AM/afternoon은 신규매수 금지(`entry_can_proceed=0`)와 exit/close 허용(`exit_can_proceed=1`, `close_can_proceed=1`)으로 실행되어 기존 보유 종목 감시를 유지합니다.
