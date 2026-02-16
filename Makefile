# Makefile

PYTHON ?= python3
PIP    ?= $(PYTHON) -m pip
PKG    ?= rolling_k_auto_trade_api

.DEFAULT_GOAL := help

help: ## 사용 가능한 타겟 목록 출력
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "\033[36m%-12s\033[0m %s\n",$${1},$${2}}'

install: requirements.txt ## 의존성 설치 (.venv 생성 및 패키지 설치)
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && \
	$(PIP) install -U pip && \
	$(PIP) install -r requirements.txt

run: ## 개발 서버 실행 (FastAPI + Uvicorn)
	. .venv/bin/activate && \
	uvicorn $(PKG).main:app --reload --host 0.0.0.0 --port 8000

verify-candidate-trade: ## candidate→trade 원클릭 검증 (AS_OF/STRATEGY_ENV/EXPECTED_POOL/EXPECTED_FINAL override 가능)
	AS_OF="$(AS_OF)" \
	STRATEGY_ENV="$(STRATEGY_ENV)" \
	EXPECTED_POOL="$(EXPECTED_POOL)" \
	EXPECTED_FINAL="$(EXPECTED_FINAL)" \
	./scripts/pbcore_candidate_trade_verify.sh
