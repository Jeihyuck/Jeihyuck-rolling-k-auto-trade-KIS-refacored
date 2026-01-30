#!/bin/bash
# DIAG 모드 KIS HTTP 차단 검증 스크립트

set -euo pipefail

echo "=========================================="
echo "DIAG 모드 KIS HTTP 차단 검증"
echo "=========================================="
echo ""

# 환경 변수 설정
export STRATEGY_MODE=DIAG
export KIS_HTTP_ENABLED=0
export PB1_DIAG_FULL_EXEC=1
export DRY_RUN=1

echo "✅ 환경 변수 설정:"
echo "   STRATEGY_MODE=${STRATEGY_MODE}"
echo "   KIS_HTTP_ENABLED=${KIS_HTTP_ENABLED}"
echo "   PB1_DIAG_FULL_EXEC=${PB1_DIAG_FULL_EXEC}"
echo "   DRY_RUN=${DRY_RUN}"
echo ""

echo "=========================================="
echo "1. diag_utils.is_diag_mode() 테스트"
echo "=========================================="
python -c "
from trader.diag_utils import is_diag_mode
result = is_diag_mode()
print(f'is_diag_mode() = {result}')
if result:
    print('✅ PASS: DIAG 모드 정상 감지')
else:
    print('❌ FAIL: DIAG 모드 미감지')
    exit(1)
"
echo ""

echo "=========================================="
echo "2. ohlcv_provider KIS 차단 테스트"
echo "=========================================="
python << 'PY'
import os
os.environ["STRATEGY_MODE"] = "DIAG"
os.environ["KIS_HTTP_ENABLED"] = "0"

from trader.diag_utils import is_diag_mode

if not is_diag_mode():
    print("❌ FAIL: DIAG 모드가 아닙니다")
    exit(1)

print("✅ PASS: DIAG 모드 확인됨")
print("")
print("ohlcv_provider 테스트를 위해서는 실제 pb1_runner를 실행해야 합니다.")
print("다음 명령으로 전체 파이프라인을 테스트하세요:")
print("")
print("  python -m trader.pb1_runner --window day --phase entry")
print("")
print("예상 로그:")
print("  [OHLCV][DIAG][KIS_BLOCKED] symbol=XXX days=XXX -> returning empty (no KIS in DIAG)")
print("  [PB1][DIAG_FULL_EXEC] override entry gate reason=entry_cutoff -> allow entry pipeline")
PY

echo ""
echo "=========================================="
echo "검증 완료"
echo "=========================================="
echo ""
echo "다음 단계:"
echo "1. 전체 파이프라인 테스트:"
echo "   python -m trader.pb1_runner --window day --phase entry"
echo ""
echo "2. 로그에서 다음 패턴 확인:"
echo "   - ✅ [OHLCV][DIAG][KIS_BLOCKED]"
echo "   - ✅ [PB1][DIAG_FULL_EXEC]"
echo "   - ❌ inquire-daily-itemchartprice (이 로그가 나오면 실패)"
echo ""
echo "3. GitHub Actions 테스트:"
echo "   - workflow_dispatch로 수동 실행"
echo "   - DIAG_FULL_REHEARSAL=1"
echo "   - DIAG_REHEARSAL_KIS_CALLS=0"
echo ""
