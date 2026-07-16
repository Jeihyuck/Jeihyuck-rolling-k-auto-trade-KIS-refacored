# WSL KR/US Schedule Runbook

## 운영 원칙

## 단일 자동 스케줄러 원칙

- Windows Task Scheduler is the only automatic scheduler for KR/US WSL trading.
- WSL cron must not run trading sessions under WSL; `scripts/wsl/install-nullim-cron.sh` removes legacy NULLIM cron blocks.
- GitHub Actions workflows are manual diagnostic/smoke/report workflows only.
- US prep/market schedule (KST): 19:30/20:30 prewarm, 21:30 regular prep, 22:10 recovery, 22:20 AM preflight, 22:30 AM, 02:00 afternoon, 05:05 close, 07:00 mail, 07:10 health.
- If same-day US prep is missing, entry is blocked but exit monitoring and close/reconcile continue.
- Verify Windows tasks with `powershell -ExecutionPolicy Bypass -File scripts/windows/verify-scheduler.ps1`.
- `runtime/cron` logs are legacy; use `runtime/wsl-us-*.log` and `reports/us_schedule_health/*.json` for current status.


- GitHub Actions schedule은 한국장/미국장 주문 가능 workflow에서 모두 제거되었습니다.
- GitHub Actions는 `workflow_dispatch` 수동 실행만 가능합니다.
- GitHub Actions 수동 실행은 기본 `INTENT_ONLY`/`DRY_RUN` 안전모드입니다.
- 실제 모의투자 자동매매는 Ubuntu WSL에서 실행합니다.
- `.env`는 GitHub에 commit하지 않습니다.

## Windows 작업 스케줄러 → WSL 실행

- 한국장: Windows 작업 스케줄러가 `scripts/wsl/run-kr-trader.sh`를 호출합니다.
- 미국장: Windows 작업 스케줄러가 세션별로 아래 스크립트를 각각 호출합니다.
  - Prep: `scripts/wsl/run-us-prep.sh`
  - AM: `scripts/wsl/run-us-am.sh`
  - Afternoon: `scripts/wsl/run-us-afternoon.sh`
  - Close: `scripts/wsl/run-us-close.sh`
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
