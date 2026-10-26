from types import SimpleNamespace
import sqlalchemy as sa
from trader.kr import epoch_scoped_health_20261026 as h

def test_health_current_epoch_only(monkeypatch):
    e=sa.create_engine("sqlite:///:memory:")
    m=sa.MetaData()
    t=sa.Table("positions",m,sa.Column("position_id",sa.String,primary_key=True),
        sa.Column("code",sa.String),sa.Column("env",sa.String),
        sa.Column("strategy",sa.String),sa.Column("status",sa.String),
        sa.Column("qty",sa.Integer),sa.Column("trading_epoch_id",sa.String))
    m.create_all(e)
    with e.begin() as c:
        c.execute(sa.insert(t),[
          dict(position_id="a",code="293490",env="practice",strategy="pb1_pullback_close",status="OPEN",qty=153,trading_epoch_id="active"),
          dict(position_id="b",code="293490",env="practice",strategy="pb1_pullback_close",status="OPEN",qty=36,trading_epoch_id="old"),
        ])
    monkeypatch.setattr(h,"schema_for_engine",lambda _:SimpleNamespace(positions=t))
    monkeypatch.setattr(h,"active_trading_epoch_id",lambda *a,**kw:"active")
    monkeypatch.setattr(h,"get_account_key",lambda **kw:"practice:new")
    monkeypatch.setattr(h,"trading_epoch_enforced",lambda:True)
    monkeypatch.setattr(h,"OrdersRepo",lambda _:SimpleNamespace(get_open_orders=lambda *a,**kw:[]))
    a=h.health_after_reconcile(engine=e,env="practice",strategy="pb1_pullback_close",holdings_rows=[{"pdno":"293490","hldg_qty":"153"}])
    assert a["qty_mismatch_count"] == 0
    assert a["duplicate_open_lifecycle_count"] == 0
