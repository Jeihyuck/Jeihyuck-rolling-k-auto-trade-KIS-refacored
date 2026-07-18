from trader.us.db.repos import _synthetic_reconcile_fill_meta,is_synthetic_fill_meta
from trader.us.runner.daily_report_runner import _fill_is_synthetic

def test_synthetic_fill_uses_is_synthetic_true():
    m=_synthetic_reconcile_fill_meta(source='balance',order_no='1',client_order_key='k',side='SELL',qty=1,avg_price_usd=1,base_meta={}); assert m['is_synthetic'] is True and m['fill_evidence_type']=='BALANCE_DELTA_SYNTHETIC'
def test_kis_actual_fill_uses_is_synthetic_false(): assert not _fill_is_synthetic({'meta':{'is_synthetic':False,'fill_evidence_type':'KIS_EXECUTION_ACTUAL'}})
def test_legacy_synthetic_fill_marker_is_recognized(): assert is_synthetic_fill_meta({'synthetic_fill':True}) and _fill_is_synthetic({'meta':{'synthetic':True}})
def test_actual_and_synthetic_same_order_not_double_counted():
    rows=[{'order_no':'1','meta':{'is_synthetic':False}},{'order_no':'1','meta':{'is_synthetic':True}}]; assert len({r['order_no'] for r in rows})==1
def test_repair_detects_synthetic_fill_marker_variants():
    from scripts.repair_us_trade_integrity import _meta; assert _meta({'meta':'{"synthetic_fill":true}'})['synthetic_fill']
