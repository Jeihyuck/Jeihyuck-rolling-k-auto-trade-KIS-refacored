#run:
#	PYTHONPATH=rolling-k-auto-trade-KIS-main \
#	python -m uvicorn rolling_k_auto_trade_api.main:app \
#	--reload --reload-dir=rolling-k-auto-trade-KIS-main/rolling_k_auto_trade_api \
#	--host 0.0.0.0 --port 8000

SHELL := /bin/bash

.PHONY: init install run clean

init:
	python3 -m venv .venv && \
	. .venv/bin/activate && \
	pip install --upgrade pip && \
	pip install -r requirements.txt && \
	echo "✅ .venv 생성 및 의존성 설치 완료"

install:
	. .venv/bin/activate && pip install -r requirements.txt

#run:
#	PYTHONPATH=rolling-k-auto-trade-KIS-main \
#	python -m uvicorn rolling_k_auto_trade_api.main:app \
##	--host 0.0.0.0 --port 8000

run:
	. .venv/bin/activate && \
	PYTHONPATH=rolling-k-auto-trade-KIS-main \
	python -m uvicorn rolling_k_auto_trade_api.main:app \
	--reload --reload-dir=rolling-k-auto-trade-KIS-main/rolling_k_auto_trade_api \
	--host 0.0.0.0 --port 8000
	
clean:
	rm -rf .venv __pycache__ .pytest_cache .mypy_cache
