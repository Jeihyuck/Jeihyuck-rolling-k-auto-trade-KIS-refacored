PYTHON ?= python
PIP    ?= \$(PYTHON) -m pip
PKG    ?= rolling_k_auto_trade_api

.DEFAULT_GOAL := help

help: ## 리스트
	@grep -E '^[a-zA-Z_-]+:.*?## ' \$(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "\033[36m%-12s\033[0m %s\n",\$$1,\$$2}'

install: requirements.txt ## 의존성 설치
	\$(PYTHON) -m venv .venv
	. .venv/bin/activate && \$(PIP) install -U pip && \$(PIP) install -r requirements.txt

run: ## 개발 서버
	. .venv/bin/activate && \
	uvicorn \$(PKG).main:app --reload --host 0.0.0.0 --port 8000
