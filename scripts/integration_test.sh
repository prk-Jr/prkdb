#!/bin/bash
# Integration test runner for PrkDB
# Run all integration tests with proper logging and error handling

set -e  # Exit on error

echo "====================================="
echo "PrkDB Integration Test Suite"
echo "====================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to run a test suite
run_test_suite() {
    local test_name=$1
    echo -e "${YELLOW}Running: $test_name${NC}"
    
    if cargo test --test "$test_name" -- --nocapture 2>&1 | tee "/tmp/prkdb_test_$test_name.log"; then
        echo -e "${GREEN}✓ $test_name PASSED${NC}"
        ((PASSED_TESTS++))
    else
        echo -e "${RED}✗ $test_name FAILED${NC}"
        echo "  Log: /tmp/prkdb_test_$test_name.log"
        ((FAILED_TESTS++))
    fi
    ((TOTAL_TESTS++))
    echo ""
}

# Clean previous test artifacts
echo "Cleaning previous test artifacts..."
cargo clean -p prkdb --release 2>/dev/null || true
rm -f /tmp/prkdb_test_*.log
echo ""

# Run unit tests first
echo -e "${YELLOW}Running unit tests...${NC}"
if cargo test --lib; then
    echo -e "${GREEN}✓ Unit tests PASSED${NC}"
    ((PASSED_TESTS++))
else
    echo -e "${RED}✗ Unit tests FAILED${NC}"
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))
echo ""

# Run integration test suites
echo "Running integration test suites..."
echo ""

run_test_suite "consumer_tests"
run_test_suite "streaming_tests"
run_test_suite "retention_tests"
run_test_suite "integration_tests"
run_test_suite "outbox_cdc_tests"
run_test_suite "stateful_compute_tests"

# Run doc tests
echo -e "${YELLOW}Running doc tests...${NC}"
if cargo test --doc; then
    echo -e "${GREEN}✓ Doc tests PASSED${NC}"
    ((PASSED_TESTS++))
else
    echo -e "${RED}✗ Doc tests FAILED${NC}"
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))
echo ""

# Summary
echo "====================================="
echo "Test Summary"
echo "====================================="
echo "Total test suites: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
if [ $FAILED_TESTS -gt 0 ]; then
    echo -e "${RED}Failed: $FAILED_TESTS${NC}"
else
    echo "Failed: 0"
fi
echo ""

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed. Check logs in /tmp/prkdb_test_*.log${NC}"
    exit 1
fi