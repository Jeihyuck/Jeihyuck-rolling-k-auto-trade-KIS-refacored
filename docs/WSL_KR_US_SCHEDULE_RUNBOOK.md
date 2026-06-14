# WSL KR/US Schedule Runbook

## 운영 원칙

- GitHub Actions schedule은 한국장/미국장 주문 가능 workflow에서 모두 제거되었습니다.
- GitHub Actions는 `workflow_dispatch` 수동 실행만 가능합니다.
- GitHub Actions 수동 실행은 기본 `INTENT_ONLY`/`DRY_RUN` 안전모드입니다.
- 실제 모의투자 자동매매는 Ubuntu WSL에서 실행합니다.
- `.env`는 GitHub에 commit하지 않습니다.

## Windows 작업 스케줄러 → WSL 실행

- 한국장: Windows 작업 스케줄러가 `scripts/wsl/run-kr-trader.sh`를 호출합니다.
- 미국장: Windows 작업 스케줄러가 `scripts/wsl/run-us-trader.sh`를 호출합니다.
- 두 스크립트는 `/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored/.env`를 읽고 `runtime/`에 로그를 남깁니다.

## 검증 순서

1. `.env.example`을 참고해 WSL 서버에 `.env`를 생성합니다.
2. 먼저 `scripts/wsl/run-kr-dryrun.sh`와 `scripts/wsl/run-us-dryrun.sh`로 검증합니다.
3. 로그 파일을 확인합니다.
   - 한국장: `runtime/wsl-kr-trader.log`
   - 미국장: `runtime/wsl-us-trader.log`
4. 이후 practice 주문 모드는 `.env`에서 아래 값으로 전환합니다.

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
