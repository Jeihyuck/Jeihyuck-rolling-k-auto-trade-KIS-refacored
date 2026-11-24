"""Manual offline 2026-11-23 exact-snapshot acceptance.

No production data belongs in Git, CI logs, or public artifacts.
Export on the authorized operator host, then run the explicit pytest target
against a dedicated localhost us_ci PostgreSQL service. Only the fake broker
receives orders. Do not run against the production database.
"""
import json
import os
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import pytest
from test_us_postgres_integration_real import amgn_pg_engine  # noqa: F401

SYMBOLS = ("ABBV", "CME", "COST", "ED", "ICE", "LLY", "MCK", "MDT", "SHW", "U")
AS_OF = "2026-11-23"

# Return the full original rows, rather than rebuilding or re-signing contracts.
EXPORT_SQL = """
WITH held AS (
 SELECT p.* FROM us_positions p
 JOIN trading_epochs e ON e.trading_epoch_id=p.trading_epoch_id
 WHERE e.env='practice' AND e.status='ACTIVE' AND p.as_of=:as_of
   AND p.qty>0 AND p.symbol=ANY(:symbols)
)
SELECT jsonb_build_object(
 'position',to_jsonb(p), 'epoch',to_jsonb(e), 'portfolio',to_jsonb(pe),
 'risk',(SELECT to_jsonb(r) FROM us_position_risk_state r
         WHERE r.trading_epoch_id=p.trading_epoch_id AND r.symbol=p.symbol
           AND r.trade_date=p.as_of LIMIT 1),
 'intents',(SELECT coalesce(jsonb_agg(to_jsonb(i)),'[]'::jsonb)
         FROM us_order_intents i WHERE i.trading_epoch_id=p.trading_epoch_id
           AND i.symbol=p.symbol AND i.account_id=p.account_id
           AND i.portfolio_epoch_id=p.portfolio_epoch_id
           AND i.position_cycle_id=p.position_cycle_id),
 'orders',(SELECT coalesce(jsonb_agg(to_jsonb(o)),'[]'::jsonb)
         FROM us_orders o WHERE o.trading_epoch_id=p.trading_epoch_id
           AND o.symbol=p.symbol AND o.account_id=p.account_id
           AND o.portfolio_epoch_id=p.portfolio_epoch_id
           AND o.position_cycle_id=p.position_cycle_id),
 'fills',(SELECT coalesce(jsonb_agg(to_jsonb(f)),'[]'::jsonb)
         FROM us_fills f WHERE f.trading_epoch_id=p.trading_epoch_id
           AND f.symbol=p.symbol AND f.account_id=p.account_id
           AND f.portfolio_epoch_id=p.portfolio_epoch_id
           AND f.position_cycle_id=p.position_cycle_id),
 'claims',(SELECT coalesce(jsonb_agg(to_jsonb(c)),'[]'::jsonb)
         FROM us_execution_claims c WHERE c.trading_epoch_id=p.trading_epoch_id
           AND c.account_id=p.account_id AND c.position_cycle_id=p.position_cycle_id
           AND c.portfolio_epoch_id=p.portfolio_epoch_id),
 'attempts',(SELECT coalesce(jsonb_agg(to_jsonb(a)),'[]'::jsonb)
         FROM us_execution_attempts a JOIN us_execution_claims c USING(action_key)
         WHERE c.trading_epoch_id=p.trading_epoch_id
           AND c.account_id=p.account_id AND c.position_cycle_id=p.position_cycle_id
           AND c.portfolio_epoch_id=p.portfolio_epoch_id),
 'stages',(SELECT coalesce(jsonb_agg(to_jsonb(s)),'[]'::jsonb)
         FROM us_profit_capture_lifecycle s WHERE s.trading_epoch_id=p.trading_epoch_id
           AND s.symbol=p.symbol AND s.position_lifecycle_id=p.position_cycle_id)
)::text AS payload
FROM held p JOIN trading_epochs e ON e.trading_epoch_id=p.trading_epoch_id
JOIN portfolio_epochs pe ON pe.portfolio_epoch_id::text=p.portfolio_epoch_id
 AND pe.trading_epoch_id::text=p.trading_epoch_id
 AND pe.env='practice' AND pe.status='ACTIVE' AND pe.account_id=e.account_id
 AND pe.strategy='US_STANDARD'
ORDER BY p.symbol
"""


def export_source(destination):
    """Separate read-only process; do not print raw rows or source URI."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url
    source = make_url(os.environ["US_SST_REPLAY_SOURCE_URL"])
    assert source.drivername.startswith("postgresql"), "SOURCE_NOT_POSTGRESQL"
    engine = create_engine(source, future=True, echo=False)
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            assert conn.execute(text("SHOW transaction_read_only")).scalar_one() == "on"
            rows = conn.execute(text(EXPORT_SQL),
                                {"as_of":AS_OF, "symbols":list(SYMBOLS)}).scalars().all()
            conn.rollback()
        parsed = [json.loads(row) for row in rows]
        assert sorted(r["position"]["symbol"] for r in parsed) == list(SYMBOLS), "NOT_EXACT_TEN"
        assert all(r["risk"] and r["intents"] and r["orders"] and r["fills"] for r in parsed)
        target = Path(destination).expanduser().resolve()
        # Explicitly prohibit writing into the repository or a Git worktree.
        assert not any((parent / ".git").exists() for parent in target.parents)
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write('{"as_of":' + json.dumps(AS_OF) + ',"rows":[' + ",".join(rows) + "]}")
        print("SOURCE_EXPORT_PASS 10/10; read-only; private local file")
    finally:
        engine.dispose()


TABLES = {
    "trading_epochs", "portfolio_epochs", "us_order_intents", "us_orders",
    "us_fills", "us_positions", "us_position_risk_state", "us_execution_claims",
    "us_execution_attempts", "us_profit_capture_lifecycle",
}


def copy_row(conn, table, data):
    from sqlalchemy import inspect, text
    assert table in TABLES
    # The CI fixture intentionally omits unrelated schema columns. Preserve
    # every column it consumes, including the *full* original JSON payload.
    cols = [c["name"] for c in inspect(conn).get_columns(table)
            if c["name"] in data and c["name"] != "id"]
    assert cols and ("meta" not in data or "meta" in cols)
    names = ",".join('"' + c + '"' for c in cols)
    conn.execute(text(
        f'INSERT INTO "{table}" ({names}) SELECT {names} '
        f'FROM jsonb_populate_record(NULL::"{table}",CAST(:row AS jsonb))'
    ), {"row":json.dumps(data,ensure_ascii=False,allow_nan=False)})


def read_snapshot(symbol):
    path = Path(os.environ["US_SST_REPLAY_INPUT"]).expanduser().resolve()
    assert not any((parent / ".git").exists() for parent in path.parents)
    assert (path.stat().st_mode & 0o077) == 0, "PRIVATE_INPUT_PERMISSIONS_REQUIRED"
    bundle = json.loads(path.read_text(encoding="utf-8"))
    assert bundle["as_of"] == AS_OF
    assert sorted(r["position"]["symbol"] for r in bundle["rows"]) == list(SYMBOLS)
    return next(r for r in bundle["rows"] if r["position"]["symbol"] == symbol)


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_exact_original_position_sell_replay(symbol, amgn_pg_engine, monkeypatch, tmp_path):
    """Real PG, real exit producer/router/repository, simulated broker only.

    Tests hard-stop full exit, cumulative partial fill for qty>1, duplicate
    evidence replay, verified residual holding and lifecycle closure. It does
    not simulate terminal cancellation/reissue or native KIS HTTP.
    """
    import hashlib
    import requests
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.db.schema import schema_for_engine
    from trader.us.execution import order_router
    from trader.us.execution.order_router import route_order
    from trader.us.execution.recovery_identity import USRecoveryScope
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract
    from trader.us.original_buy_lineage import prove_open_standard_buy_source
    from trader.us.decision_evidence_5w1h import verify_exit_entry_link
    from trader.us.pb1.us_exit_position_resolver import enrich_us_positions_for_exit
    from trader.us.pb1.us_exit_engine import generate_exit_intents
    from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import us_execution_claims, us_execution_attempts
    import trader.account_state as account_state

    assert os.getenv("US_SST_REPLAY_ALLOW_LOCAL_TEST") == "1"
    engine = amgn_pg_engine
    assert engine.url.host in ("localhost", "127.0.0.1")
    assert engine.url.database == "us_ci_amgn_original_buy_proof"
    # Export and replay are distinct processes. Never inherit a remote URI.
    assert not os.getenv("US_SST_REPLAY_SOURCE_URL")
    for name in ("PBCORE_DB_URL","DATABASE_URL"):
        assert not os.getenv(name), "UNSET_REMOTE_DB_URL_BEFORE_REPLAY"
        monkeypatch.setenv(name, engine.url.render_as_string(hide_password=False))
    for name in ("KIS_APP_KEY","KIS_APP_SECRET","CANO","ACNT_PRDT_CD"):
        assert not os.getenv(name), "UNSET_BROKER_CREDENTIALS_BEFORE_REPLAY"
    def no_http(*args, **kwargs):
        raise AssertionError("OUTBOUND_HTTP_FORBIDDEN_IN_REPLAY")
    monkeypatch.setattr(requests.sessions.Session,"request",no_http)
    for name,value in {
        "KIS_ENV":"practice","WSL_RUN_MARKET":"US",
        "US_SWING_EXIT_MODE":"V3_ACTIVE_PRACTICE",
        "US_SWING_V3_DEPLOY_APPROVED":"1",
        "US_ACCOUNT_RESERVATION_MODE":"OFF",
        "US_ATOMIC_CLAIM_OUTBOX_ENABLED":"0",
        "US_RECONCILE_WORKER_MODE":"LEGACY",
        "US_ORDER_JOURNAL_DIR":str(tmp_path/"journal"),
        "KIS_HTTP_BLOCK":"1","US_OFFLINE_MODE":"1",
        "OFFLINE_MODE":"1","US_ORDER_ARMED":"0",
        "US_KIS_ORDER_ALLOWED":"0","LIVE_TRADING_ENABLED":"0",
        "US_LIVE_TRADING_ENABLED":"0","ALLOW_REAL_ORDER":"0",
        "DISABLE_REAL_TRADING":"1","DISABLE_LIVE_TRADING":"1",
        "DRY_RUN":"1",
    }.items():
        monkeypatch.setenv(name,value)
    # The only simulated broker is passed below. Native construction is banned.
    def no_native(*args, **kwargs):
        raise AssertionError("NATIVE_KIS_CLIENT_FORBIDDEN")
    # Keep the class symbol intact for isinstance checks; forbid construction.
    monkeypatch.setattr(order_router.NativeKisUSClient,"__init__",no_native)
    raw = read_snapshot(symbol)
    pos, e, pe = raw["position"], raw["epoch"], raw["portfolio"]
    qty, avg = int(pos["qty"]),float(pos["avg_cost"])
    exchange = pos["exchange"]
    epoch,portfolio,cycle = pos["trading_epoch_id"],pos["portfolio_epoch_id"],pos["position_cycle_id"]
    account = e["account_id"]
    hashed = hashlib.sha256(account.encode()).hexdigest()
    assert pos["account_id"] == hashed
    assert qty > 0 and avg > 0 and e["status"] == "ACTIVE" and e["env"] == "practice"
    contract = pos["meta"]["entry_exit_contract"]
    assert verify_us_entry_exit_contract(contract)
    entry = contract["entry_provenance"]["entry_decision_evidence_5w1h"]
    assert all(r["side"] == "BUY" for r in raw["orders"] + raw["fills"])
    original = [r for r in raw["orders"] if r["side"]=="BUY" and r["status"]=="FILLED"]
    assert len(original) == 1
    original = original[0]
    assert int(original["qty_filled"]) == qty
    key = original["client_order_key"]
    original_intent = next(r for r in raw["intents"] if r["client_order_key"] == key)
    original_fill = next(r for r in raw["fills"] if r["client_order_key"] == key)
    assert all(r["meta"]["entry_exit_contract"] == contract for r in (original,original_intent,original_fill))
    schema = schema_for_engine(engine)
    schema.trading_epochs.create(engine,checkfirst=True)
    schema.portfolio_epochs.create(engine,checkfirst=True)
    with engine.begin() as conn:
        repos._ensure_us_position_risk_state_table(conn)
        for field in ("account_id","portfolio_epoch_id","position_cycle_id"):
            conn.exec_driver_sql(f"ALTER TABLE us_position_risk_state ADD COLUMN IF NOT EXISTS {field} TEXT")
        copy_row(conn,"trading_epochs",e)
        copy_row(conn,"portfolio_epochs",pe)
        for name,table in (("intents","us_order_intents"),("orders","us_orders"),
                           ("fills","us_fills"),("claims","us_execution_claims"),
                           ("attempts","us_execution_attempts"),("stages","us_profit_capture_lifecycle")):
            for row in raw[name]:
                copy_row(conn,table,row)
        copy_row(conn,"us_positions",pos)
        copy_row(conn,"us_position_risk_state",raw["risk"])
    monkeypatch.setattr(account_state,"get_account_key",lambda **kw:account)
    monkeypatch.setattr(account_state,"resolve_env_name",lambda *a,**kw:"practice")
    monkeypatch.setattr(repos,"_active_us_epoch",lambda *a,**kw:epoch)
    monkeypatch.setattr(repos,"_us_state_epoch_id",lambda *a,**kw:epoch)
    monkeypatch.setattr(order_router,"_SENT_ORDER_KEYS",set())
    monkeypatch.setattr(order_router,"_BLOCKED_INTENT_KEYS",set())
    # Exactly the existing offline PostgreSQL test adapter convention:
    # no native client/HTTP; only the in-process TestBroker sees the order.
    monkeypatch.setattr(order_router,"resolve_dry_run_for_us_order",lambda:False)
    claims=DurableExecutionClaimRepo(engine,us_execution_claims,us_execution_attempts)
    monkeypatch.setattr(repos,"_execution_claim_repo",lambda:claims)
    monkeypatch.setattr(repos,"claim_execution_action",claims.acquire)
    monkeypatch.setattr(repos,"record_execution_action_observation",claims.record_observation)
    monkeypatch.setattr(repos,"release_execution_action_before_submit",claims.release_before_submit)
    scope=USRecoveryScope("practice",hashed,epoch,portfolio,cycle,"US_STANDARD")
    identity={**scope.as_dict(),"account_id":account,"position_lifecycle_id":cycle}
    with engine.connect() as conn:
        proof=prove_open_standard_buy_source(conn=conn,symbol=symbol,trade_date=AS_OF,identity=identity)
    binding=proof["original_buy_decision_binding"]
    assert binding and binding["source_buy_client_order_key"] == key
    assert binding["source_entry_decision_sha256"] == entry["sha256"]

    day=(date.fromisoformat(AS_OF)+timedelta(days=1)).isoformat()
    later=(date.fromisoformat(day)+timedelta(days=1)).isoformat()
    now=datetime.fromisoformat(day+"T18:00:00+00:00")
    # Deliberately synthetic test price: stress the frozen hard-stop policy.
    # Never interpret this as a market price, trading signal, or recommendation.
    px=round(avg*0.80,2)
    def snapshot(day, held_qty):
        balance=[{"symbol":symbol,"exchange":exchange,"qty":held_qty,"avg_cost":avg,
                  "current_px":px,"balance_source":"kis_balance_authoritative","meta":{}}] if held_qty else []
        return repos.save_position_snapshot(
            balance,trade_date=day,balance_fetch_status="OK",balance_parse_status="OK",
            authoritative_positions=True,preserve_previous_positions=False)
    assert snapshot(day,qty) == 1
    loaded = repos.load_us_positions_by_symbols([symbol],as_of=day)
    resolved,_=enrich_us_positions_for_exit(list(loaded.values()),trade_date=day,env="practice")
    assert len(resolved)==1
    held=resolved[0]
    held.update(resolved_current_price=px,orderable_qty=qty,holding_qty=qty,sellable_qty=qty)
    held["entry_exit_contract"]=held["meta"]["entry_exit_contract"]
    assert held["entry_exit_contract"] == contract
    exits=generate_exit_intents([],prepared_snapshots=[held],now=now,include_trend_time=False)
    sells=[r for r in exits if r.get("side")=="SELL"]
    assert len(sells)==1,"MISSING_OR_DUPLICATE_EXIT_INTENT"
    sell=sells[0]
    assert sell["source_entry_contract_sha256"] == contract["sha256"]
    assert sell["position_lifecycle_id"] == cycle
    sell["trade_date"]=day
    for field,value in (("exchange",exchange),("holding_qty",qty),("orderable_qty",qty),
                        ("sellable_qty",qty),("available_qty",qty)):
        sell.setdefault(field,value)
    class TestBroker:
        def __init__(self):
            self.posts=[]
        def get_balance(self,**kw):
            return {"positions":[{"symbol":symbol,"exchange":exchange,"qty":qty,
                      "holding_qty":qty,"orderable_qty":qty,"avg_price":avg}]}
        def get_orderable_cash(self,**kw):
            return 100000.0
        def place_us_sell_order(self,sym,ex,n,price):
            assert (sym,ex,n)==(symbol,exchange,qty)
            self.posts.append(n)
            return {"ok":True,"order_no":"OFFLINE-SST-"+symbol}
    broker=TestBroker()
    route=route_order(sell,kis_client=broker,current_position_symbols={symbol},
                      current_position_count=1,total_portfolio_usd=max(20000,qty*avg*2),
                      available_cash_usd=10000,now=now)
    assert route["status"]=="ACK","OFFLINE_SELL_ROUTER_FAILED"
    assert broker.posts==[qty],"WRONG_OR_DUPLICATE_SIMULATED_SELL"
    sellkey=route["intent"]["client_order_key"]
    orderno="OFFLINE-SST-"+symbol
    with engine.connect() as conn:
        persisted=conn.execute(text("""
            SELECT i.meta AS im,o.meta AS om,o.status,o.qty_filled
            FROM us_order_intents i JOIN us_orders o
            ON i.client_order_key=o.client_order_key AND i.trade_date=o.trade_date
            WHERE o.order_no=:n
        """),{"n":orderno}).mappings().one()
    assert persisted["status"]=="ACK" and persisted["qty_filled"]==0
    def check_audit(meta):
        assert meta["source_entry_reason"] == contract["entry_provenance"]["entry_reason"]
        assert meta["source_entry_decision_sha256"] == entry["sha256"]
        assert meta["us_original_buy_decision_binding_v1"] == binding
        assert meta["us_original_buy_sst_v1"]["source_buy_client_order_key"] == key
        assert verify_exit_entry_link(meta["exit_decision_evidence_5w1h"],entry,original_buy_binding=binding)
    check_audit(persisted["im"])
    check_audit(persisted["om"])
    # A native-format fill observer provides scope and economic facts only.
    # Never manufacture the original decision audit in the observation.
    audit_fields={"source_entry_reason","source_entry_decision_sha256",
                  "us_original_buy_decision_binding_v1","exit_decision_evidence_5w1h",
                  "exit_decision_evidence_status","us_original_buy_sst_v1"}
    fill_base={k:v for k,v in persisted["om"].items() if k not in audit_fields}
    def fill(cumulative):
        result=repos.mark_order_filled_by_reconcile(
            order_no=orderno,client_order_key=sellkey,symbol=symbol,side="SELL",
            filled_qty=cumulative,requested_qty=qty,cumulative_filled_qty=cumulative,
            avg_price_usd=px,trade_date=day,evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
            source="fills_by_order_no",meta={**fill_base,"cumulative_filled_qty":cumulative})
        assert result["status"]=="OK","OFFLINE_FILL_RECONCILIATION_FAILED"
        with engine.connect() as conn:
            state=conn.execute(text("""
                SELECT o.status,o.qty_filled,o.position_cycle_id AS oc,
                       f.qty AS fq,f.position_cycle_id AS fc,o.meta AS om,f.meta AS fm
                FROM us_orders o JOIN us_fills f
                ON o.order_no=f.order_no AND o.client_order_key=f.client_order_key
                WHERE o.order_no=:n
            """),{"n":orderno}).mappings().one()
        assert state["qty_filled"]==state["fq"]==cumulative
        assert state["oc"]==state["fc"]==cycle
        check_audit(state["om"])
        check_audit(state["fm"])
        return state
    last_snapshot_day=day
    if qty>1:
        part=qty//2
        assert fill(part)["status"]=="PARTIALLY_FILLED"
        assert snapshot(later,qty-part)==1
        remaining=repos.load_us_positions_by_symbols([symbol],as_of=later)[symbol]
        assert remaining["qty"]==qty-part
        assert remaining["meta"]["entry_exit_contract"]==contract
        with engine.connect() as conn:
            after=prove_open_standard_buy_source(conn=conn,symbol=symbol,trade_date=later,identity=identity)
        assert after["original_buy_decision_binding"]==binding
        last_snapshot_day=later
    assert fill(qty)["status"]=="FILLED"
    assert fill(qty)["status"]=="FILLED"
    assert snapshot(last_snapshot_day,0)==0
    closed=reconcile_us_position_lifecycles(
        positions=[],trade_date=last_snapshot_day,
        now=datetime.fromisoformat(last_snapshot_day+"T20:00:00+00:00"),
        authoritative=True,env="practice",managed_owners={"US_STANDARD"})[symbol]
    assert closed["is_open"] is False and closed["lifecycle_id"]==cycle
    assert not repos.load_us_positions_by_symbols([symbol],as_of=last_snapshot_day)
    with engine.connect() as conn:
        for table,source in (("us_order_intents",original_intent),("us_orders",original),("us_fills",original_fill)):
            stored=conn.execute(text(f"SELECT meta FROM {table} WHERE client_order_key=:k"),{"k":key}).scalar_one()
            assert stored == source["meta"],"ORIGINAL_BUY_META_MUTATED"
        assert conn.execute(text("SELECT count(*) FROM us_fills WHERE order_no=:n"),{"n":orderno}).scalar_one()==1
    assert broker.posts==[qty]


if __name__=="__main__":
    import sys
    if len(sys.argv)!=3 or sys.argv[1]!="export":
        raise SystemExit("Usage: python tests/us/manual_us_sst_exact_snapshot_replay.py export /private/path.json")
    try:
        export_source(sys.argv[2])
    except Exception as exc:
        # Do not print source credentials, original account data or traceback.
        print("SOURCE_EXPORT_FAILED",type(exc).__name__,file=sys.stderr)
        raise SystemExit(1)
