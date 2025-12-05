#!/usr/bin/env python3
"""
Script to run pytest tests individually and detect all tests with "no longer implemented" panics.
Collects all failing tests in one pass by running each test separately with a timeout.
"""

import subprocess
import sys
import re
import signal
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

TEST_FILE = "python/tests/test_remote_functions.py"
TIMEOUT_SECONDS = 30  # Timeout per test


def get_test_list():
    """Get list of all test names in the file."""
    result = subprocess.run(
        ["pytest", TEST_FILE, "--collect-only", "-q"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    tests = []
    for line in result.stdout.splitlines():
        if "::" in line and "test_" in line:
            # Extract just the test name part
            tests.append(line.strip())
    return tests


def run_single_test(test_name):
    """Run a single test and check if it panics with 'no longer implemented'."""
    try:
        process = subprocess.Popen(
            ["pytest", test_name, "-v", "-s", "-x"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        output_lines = []
        try:
            # Read output with timeout
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                output_lines.append(line)

                # Detect the panic
                if "no longer implemented" in line.lower():
                    process.kill()
                    process.wait()
                    return {"test": test_name, "status": "panic", "output": output_lines}

            process.wait(timeout=TIMEOUT_SECONDS)

            if process.returncode == 0:
                return {"test": test_name, "status": "passed"}
            else:
                return {
                    "test": test_name,
                    "status": "failed",
                    "output": output_lines,
                }

        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            return {"test": test_name, "status": "timeout", "output": output_lines}

    except Exception as e:
        return {"test": test_name, "status": "error", "error": str(e)}


def main():
    print("Collecting tests...")
    tests = get_test_list()
    print(f"Found {len(tests)} tests")

    panic_tests = []
    passed_tests = []
    failed_tests = []
    timeout_tests = []

    for i, test in enumerate(tests):
        # Extract just the test method name for display
        test_short = test.split("::")[-1] if "::" in test else test
        print(f"[{i+1}/{len(tests)}] Running {test_short}...", end=" ", flush=True)

        result = run_single_test(test)

        if result["status"] == "panic":
            print("PANIC - no longer implemented")
            panic_tests.append(test)
        elif result["status"] == "passed":
            print("PASSED")
            passed_tests.append(test)
        elif result["status"] == "timeout":
            print("TIMEOUT")
            timeout_tests.append(test)
        else:
            print(f"FAILED ({result['status']})")
            failed_tests.append(test)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Passed: {len(passed_tests)}")
    print(f"Failed: {len(failed_tests)}")
    print(f"Timeout: {len(timeout_tests)}")
    print(f"Panic (no longer implemented): {len(panic_tests)}")

    if panic_tests:
        print("\n" + "=" * 60)
        print("TESTS TO DELETE (no longer implemented):")
        print("=" * 60)
        for test in panic_tests:
            # Extract just the test method name
            test_method = test.split("::")[-1] if "::" in test else test
            print(f"  - {test_method}")

    if timeout_tests:
        print("\n" + "=" * 60)
        print("TESTS THAT TIMED OUT (may also need deletion):")
        print("=" * 60)
        for test in timeout_tests:
            test_method = test.split("::")[-1] if "::" in test else test
            print(f"  - {test_method}")

    return panic_tests


if __name__ == "__main__":
    panic_tests = main()
    if panic_tests:
        sys.exit(1)
    sys.exit(0)
