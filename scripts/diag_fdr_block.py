#!/usr/bin/env python3
"""
Diagnostic script to detect FinanceDataReader HTTP blocking issues.

Exit codes:
  0: All tests OK, no block signals detected
  1: Other failures (exceptions, empty data, etc.)
  2: Block suspected (403/429 status, HTML response, captcha keywords)
"""

import sys
import time
from typing import List, Dict, Any

# Monkeypatch requests before any other imports that might use it
import requests

original_request = requests.Session.request
http_captures: List[Dict[str, Any]] = []


def patched_request(self, method, url, **kwargs):
    """Capture HTTP request details."""
    start_time = time.time()
    exception_info = None
    response = None
    
    try:
        response = original_request(self, method, url, **kwargs)
        elapsed_ms = int((time.time() - start_time) * 1000)
        
        # Get response body preview
        body_preview = ""
        try:
            text = response.text if hasattr(response, 'text') else ""
            # Normalize newlines and take first 300 chars
            body_preview = text.replace('\n', '\\n').replace('\r', '')[:300]
        except Exception:
            body_preview = "<unable to read response text>"
        
        http_captures.append({
            'method': method,
            'url': url,
            'status_code': response.status_code,
            'content_type': response.headers.get('Content-Type', ''),
            'elapsed_ms': elapsed_ms,
            'body_prefix': body_preview,
            'exception': None
        })
        
        return response
        
    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        exception_info = f"{type(e).__name__}: {str(e)}"
        
        http_captures.append({
            'method': method,
            'url': url,
            'status_code': None,
            'content_type': '',
            'elapsed_ms': elapsed_ms,
            'body_prefix': '',
            'exception': exception_info
        })
        
        raise


# Apply the monkeypatch
requests.Session.request = patched_request


# Now import FinanceDataReader after patching
try:
    import FinanceDataReader as fdr
except ImportError:
    print("ERROR: FinanceDataReader not installed")
    print("Install with: pip install finance-datareader")
    sys.exit(1)


def run_test(test_name: str, test_func) -> tuple[bool, str, Any]:
    """
    Run a single test and return (success, message, result_data).
    """
    try:
        result = test_func()
        if result is None or (hasattr(result, 'empty') and result.empty):
            return False, "Empty result", None
        if hasattr(result, 'shape'):
            return True, f"OK - shape: {result.shape}", result
        return True, "OK", result
    except Exception as e:
        return False, f"FAIL - {type(e).__name__}: {str(e)}", None


def detect_block_signals() -> tuple[bool, List[str]]:
    """
    Analyze HTTP captures for blocking signals.
    Returns (is_blocked, reasons).
    """
    reasons = []
    
    for capture in http_captures:
        status = capture.get('status_code')
        content_type = capture.get('content_type', '').lower()
        body_prefix = capture.get('body_prefix', '').lower()
        
        # Check for blocking status codes
        if status in {401, 403, 429}:
            reasons.append(f"HTTP {status} detected on {capture['url']}")
        
        # Check for 400 with session/auth rejection (e.g., LOGOUT)
        if status == 400:
            session_keywords = ['logout', 'login', 'session']
            for keyword in session_keywords:
                if keyword in body_prefix:
                    reasons.append(f"HTTP 400 with '{keyword}' on {capture['url']} (session/auth rejection)")
                    break
        
        # Check for HTML response when we expect data
        if status == 200 and 'text/html' in content_type:
            reasons.append(f"HTML response on {capture['url']} (expected data)")
        
        # Check for blocking keywords in response body
        block_keywords = [
            'captcha', 'access denied', 'forbidden', 
            'too many requests', 'blocked', 'rate limit',
            'cloudflare', 'suspicious activity', 'robot'
        ]
        
        for keyword in block_keywords:
            if keyword in body_prefix:
                reasons.append(f"Keyword '{keyword}' found in response from {capture['url']}")
                break
    
    return len(reasons) > 0, reasons


def main():
    print("=" * 80)
    print("FinanceDataReader Block Diagnostic")
    print("=" * 80)
    print()
    
    # Define tests
    tests = [
        ("StockListing(KOSPI)", lambda: fdr.StockListing("KOSPI")),
        ("StockListing(KOSDAQ)", lambda: fdr.StockListing("KOSDAQ")),
        ("DataReader(005930)", lambda: fdr.DataReader("005930")),
    ]
    
    # Run tests sequentially
    test_results = []
    for test_name, test_func in tests:
        print(f"Running: {test_name}")
        success, message, data = run_test(test_name, test_func)
        test_results.append((test_name, success, message))
        print(f"  {message}")
        print()
    
    # Print HTTP capture summary
    print("-" * 80)
    print("HTTP CAPTURE SUMMARY")
    print("-" * 80)
    
    if not http_captures:
        print("No HTTP calls captured")
    else:
        for i, capture in enumerate(http_captures, 1):
            print(f"\n[{i}] {capture['method']} {capture['url']}")
            
            if capture['exception']:
                print(f"    Exception: {capture['exception']}")
            else:
                print(f"    Status: {capture['status_code']}")
                print(f"    Content-Type: {capture['content_type']}")
                print(f"    Elapsed: {capture['elapsed_ms']}ms")
                
                if capture['body_prefix']:
                    print(f"    Body prefix: {capture['body_prefix'][:150]}...")
    
    print()
    print("-" * 80)
    print("VERDICT")
    print("-" * 80)
    
    # Analyze results
    all_tests_passed = all(success for _, success, _ in test_results)
    is_blocked, block_reasons = detect_block_signals()
    
    if is_blocked:
        print("STATUS: BLOCK_SUSPECTED")
        print("\nReasons:")
        for reason in block_reasons:
            print(f"  - {reason}")
        print("\nAction: FinanceDataReader may be blocked. Check network, VPN, or rate limits.")
        return 2
    
    elif all_tests_passed:
        print("STATUS: HEALTHY")
        print("\nAll tests passed, no blocking signals detected.")
        return 0
    
    else:
        print("STATUS: OTHER_FAILURE")
        print("\nSome tests failed but no blocking signals detected.")
        print("Failed tests:")
        for test_name, success, message in test_results:
            if not success:
                print(f"  - {test_name}: {message}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
