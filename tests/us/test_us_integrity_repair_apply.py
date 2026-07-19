import pytest
from scripts.repair_us_trade_integrity import audit

def test_repair_plan_identifies_cross_symbol_order():
    rows=[{'id':1,'trade_date':'2026-07-16','client_order_key':'a','order_no':'1','symbol':'AMZN','side':'SELL','meta':{}},{'id':2,'trade_date':'2026-07-16','client_order_key':'b','order_no':'1','symbol':'GOOGL','side':'SELL','meta':{}}]
    assert audit(rows,[])['issue_count']==2
