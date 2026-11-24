"""US_STANDARD native broker boundary must retain one original BUY proof.

These static checks supplement (not replace) the real PostgreSQL route replay.
"""
import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROUTER = ROOT / "trader/us/execution/order_router.py"
CLIENT = ROOT / "trader/us/execution/kis_us_client.py"


def _call_name(call):
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def test_no_native_us_buy_sell_call_outside_canonical_router():
    """Reject new direct submit paths in the automated US runtime package."""
    methods = {"place_us_buy_order", "place_us_sell_order"}
    bypasses = []
    for path in (ROOT / "trader/us").rglob("*.py"):
        if path in {ROUTER, CLIENT}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            direct = _call_name(node) in methods
            dynamic = (_call_name(node) == "getattr" and len(node.args) >= 2
                       and isinstance(node.args[1], ast.Constant)
                       and node.args[1].value in methods)
            if direct or dynamic:
                bypasses.append((str(path.relative_to(ROOT)), node.lineno))
    assert not bypasses, bypasses


def test_native_router_original_buy_source_before_intent_and_claim():
    tree = ast.parse(ROUTER.read_text(encoding="utf-8-sig"))
    route = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == "route_order")
    calls = [node for node in ast.walk(route) if isinstance(node, ast.Call)]
    checks = sorted(node.lineno for node in calls
                    if _call_name(node) == "bind_existing_standard_order_buy_source")
    assert len(checks) == 2
    # The first check is immediately after the existing portfolio/cycle binder;
    # the second is within the preclaim connection, before the claim checks.
    scopes = sorted(node.lineno for node in calls
                    if _call_name(node) == "bind_original_us_order_scope")
    claim_checks = sorted(node.lineno for node in calls
                          if _call_name(node) == "assert_preexisting_us_claim_scope")
    assert len(scopes) == 1 and len(claim_checks) >= 1
    assert scopes[0] < checks[0] < checks[1] < claim_checks[0]
    # Guard against moving the verification behind any real claim acquisition.
    claims = sorted(node.lineno for node in calls
                    if _call_name(node) == "claim_execution_action")
    assert claims and checks[1] < claims[0]
