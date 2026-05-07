#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test US entry engine does not have local import shadowing issues.

UnboundLocalError가 발생하지 않도록 함수 내부에서 module을 재import하지 않아야 한다.
특히 os와 같이 파일 상단에서 이미 import하고 함수 내부에서도 사용하는 경우.
"""
import os


def test_generate_entry_intents_no_unbound_local_error():
    """generate_entry_intents()가 빈 입력에서도 UnboundLocalError 없이 실행되어야 한다."""
    # Setup minimal environment
    os.environ["US_MAX_NEW_ENTRIES_PER_TICK"] = "3"
    os.environ["US_MIN_CASH_BUFFER_USD"] = "200"
    
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    
    # Mock provider
    class MockProvider:
        def get_daily_prices(self, symbol, exchange):
            return []
        
        def get_current_price(self, symbol, exchange):
            return {"price": 100.0}
    
    provider = MockProvider()
    
    # Call with empty tickers - 이전에는 UnboundLocalError 발생
    result = generate_entry_intents(
        tickers=[],
        provider=provider,
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=34482.76,
        max_new_entries=3,
        watchlist_entries=None,
    )
    
    # 빈 입력이므로 빈 결과가 정상
    assert result == []


def test_generate_entry_intents_with_watchlist_entries():
    """watchlist_entries를 넣고 호출해도 UnboundLocalError가 발생하지 않아야 한다."""
    os.environ["US_MAX_NEW_ENTRIES_PER_TICK"] = "3"
    os.environ["US_MIN_CASH_BUFFER_USD"] = "200"
    
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    
    class MockProvider:
        def get_daily_prices(self, symbol, exchange):
            # Minimal daily data
            return [{"close": 100.0, "volume": 1000000}]
        
        def get_current_price(self, symbol, exchange):
            return {"price": 101.0}
    
    provider = MockProvider()
    
    watchlist_entries = [
        {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "score": 0.85,
            "strategy": "us_pb1",
        }
    ]
    
    # 이전에는 score diagnostics artifact 저장 시점에서 UnboundLocalError 발생 가능
    result = generate_entry_intents(
        tickers=None,
        provider=provider,
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=34482.76,
        max_new_entries=3,
        watchlist_entries=watchlist_entries,
    )
    
    # 결과는 빈 리스트거나 intent가 있을 수 있음 (MockProvider가 충분한 데이터를 제공하지 않으므로 skip될 수 있음)
    # 중요한 것은 UnboundLocalError가 발생하지 않는 것
    assert isinstance(result, list)


def test_entry_engine_file_has_no_function_local_os_import():
    """trader/us/pb1/us_entry_engine.py 파일에서 함수 내부 'import os'가 없어야 한다.
    
    파일 상단의 module-level import os는 있어야 하지만,
    함수 내부에서 재import하면 local variable shadowing 문제가 발생한다.
    """
    from pathlib import Path
    
    path = Path("trader/us/pb1/us_entry_engine.py")
    text = path.read_text(encoding="utf-8")
    
    lines = text.split("\n")
    
    # Module-level import os가 있는지 확인 (파일 상단)
    module_level_import_found = False
    for i, line in enumerate(lines[:50]):  # 상단 50줄 내에서 확인
        if line.strip().startswith("import os"):
            module_level_import_found = True
            break
    
    assert module_level_import_found, "Module-level 'import os' must exist at file top"
    
    # 함수 내부에서 'import os'가 있는지 확인 (들여쓰기된 import os)
    function_local_os_imports = []
    in_function = False
    
    for i, line in enumerate(lines, start=1):
        # def로 시작하면 함수 시작
        if line.strip().startswith("def "):
            in_function = True
        
        # 함수 내부에서 들여쓰기된 'import os' 발견
        if in_function and line.startswith("    ") and "import os" in line:
            # 하지만 주석은 제외
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if stripped.startswith("import os") or stripped == "import os":
                function_local_os_imports.append((i, line))
    
    # 함수 내부 import os가 없어야 함
    assert len(function_local_os_imports) == 0, (
        f"Found function-local 'import os' at lines: {function_local_os_imports}. "
        "This causes UnboundLocalError when os is used before the import statement."
    )
