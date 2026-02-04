#!/usr/bin/env python3
"""
DRY_RUN parsing test script.
Tests that env_bool correctly parses all variations of DRY_RUN values.
"""
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trader.utils.env import env_bool


def test_env_bool():
    """Test env_bool with various DRY_RUN values."""
    print("=" * 60)
    print("Testing env_bool with DRY_RUN environment variable")
    print("=" * 60)
    
    test_cases = [
        # (env_value, expected_result, description)
        ("0", False, "String '0' should be False"),
        ("1", True, "String '1' should be True"),
        ("false", False, "String 'false' should be False"),
        ("true", True, "String 'true' should be True"),
        ("False", False, "String 'False' should be False"),
        ("True", True, "String 'True' should be True"),
        ("no", False, "String 'no' should be False"),
        ("yes", True, "String 'yes' should be True"),
        ("off", False, "String 'off' should be False"),
        ("on", True, "String 'on' should be True"),
        ("", False, "Empty string should use default (False)"),
        (None, False, "None (unset) should use default (False)"),
        (None, True, "None (unset) with default=True should be True"),
    ]
    
    passed = 0
    failed = 0
    
    for env_value, expected, description in test_cases:
        # Handle None case separately for default test
        if env_value is None and expected is True:
            if "DRY_RUN" in os.environ:
                del os.environ["DRY_RUN"]
            result = env_bool("DRY_RUN", default=True)
            default_used = True
        else:
            if env_value is None:
                if "DRY_RUN" in os.environ:
                    del os.environ["DRY_RUN"]
                default_used = False
            else:
                os.environ["DRY_RUN"] = env_value
                default_used = False
            
            result = env_bool("DRY_RUN", default=False if not default_used else True)
        
        status = "✅ PASS" if result == expected else "❌ FAIL"
        if result == expected:
            passed += 1
        else:
            failed += 1
        
        env_display = f"'{env_value}'" if env_value is not None else "None"
        print(f"{status}: {description}")
        print(f"  env={env_display} → result={result} (expected={expected}, type={type(result).__name__})")
        print()
    
    print("=" * 60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0


def test_workflow_scenarios():
    """Test real workflow scenarios from GitHub Actions."""
    print("\n" + "=" * 60)
    print("Testing Real Workflow Scenarios")
    print("=" * 60)
    
    scenarios = [
        {
            "name": "Schedule LIVE (weekday paper trading)",
            "env": {"DRY_RUN": "0", "LIVE_TRADING_ENABLED": "1", "DISABLE_LIVE_TRADING": "0"},
            "expected": {"dry_run": False, "live_enabled": True, "disable_live": False},
        },
        {
            "name": "Manual DIAG test",
            "env": {"DRY_RUN": "1", "LIVE_TRADING_ENABLED": "0", "DISABLE_LIVE_TRADING": "1"},
            "expected": {"dry_run": True, "live_enabled": False, "disable_live": True},
        },
        {
            "name": "Weekend candidate build",
            "env": {"DRY_RUN": "1", "LIVE_TRADING_ENABLED": "0", "DISABLE_LIVE_TRADING": "1"},
            "expected": {"dry_run": True, "live_enabled": False, "disable_live": True},
        },
    ]
    
    passed = 0
    failed = 0
    
    for scenario in scenarios:
        print(f"\n🧪 Scenario: {scenario['name']}")
        print(f"   Environment: {scenario['env']}")
        
        # Set environment
        for key, value in scenario["env"].items():
            os.environ[key] = value
        
        # Parse
        dry_run = env_bool("DRY_RUN", default=True)
        live_enabled = env_bool("LIVE_TRADING_ENABLED", default=False)
        disable_live = env_bool("DISABLE_LIVE_TRADING", default=True)
        
        # Check
        results = {
            "dry_run": dry_run,
            "live_enabled": live_enabled,
            "disable_live": disable_live,
        }
        
        scenario_passed = results == scenario["expected"]
        if scenario_passed:
            passed += 1
            print(f"   ✅ PASS")
        else:
            failed += 1
            print(f"   ❌ FAIL")
            print(f"   Expected: {scenario['expected']}")
            print(f"   Got:      {results}")
    
    print("\n" + "=" * 60)
    print(f"Scenario Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    print("DRY_RUN Parsing Test Suite\n")
    
    test1_pass = test_env_bool()
    test2_pass = test_workflow_scenarios()
    
    if test1_pass and test2_pass:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
