#!/bin/bash
python -m uvicorn rolling_k_auto_trade_api.main:app --reload --port 8000
