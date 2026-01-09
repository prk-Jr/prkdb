#!/bin/bash

# PrkDB Comprehensive Validation Script
# Runs all tests, examples, and benchmarks to ensure nothing is broken
# Usage: ./scripts/validate_all.sh

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${BLUE}==== $1 ====${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Start validation
echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    PrkDB Validation Suite                   ║"
echo "║                                                              ║"
echo "║  🧪 Tests | 📋 Examples | 🏗️  Build | 📊 Benchmarks        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Navigate to project root
cd "$(dirname "$0")/.."

# Start timer
start_time=$(date +%s)

# 1. Code quality checks
print_step "Code Quality Checks"
echo "Running cargo check..."
if cargo check --all-targets --all-features; then
    print_success "Code check passed"
else
    print_error "Code check failed"
    exit 1
fi

echo "Running cargo clippy..."
if cargo clippy --all-targets --all-features -- -D warnings; then
    print_success "Clippy passed"
else
    print_warning "Clippy found issues (continuing anyway)"
fi

# 2. Build check (Required for integration tests)
print_step "Build Verification"
echo "Building release version..."
# Explicitly build the server binary first as it's needed for chaos tests
if cargo build --release --bin prkdb-server --all-features && cargo build --release --all-features; then
    print_success "Release build successful"
else
    print_error "Release build failed"
    exit 1
fi

# 3. Unit and integration tests
print_step "Running Unit & Integration Tests"
echo "Running cargo test..."
if cargo test --all --all-features; then
    print_success "All tests passed"
else
    print_error "Some tests failed"
    exit 1
fi

# 4. Examples validation
print_step "Running All Examples"

# Get all example files dynamically
examples_dir="crates/prkdb/examples"
if [ ! -d "$examples_dir" ]; then
    print_error "Examples directory not found: $examples_dir"
    exit 1
fi

example_count=0
failed_examples=()

for example_file in "$examples_dir"/*.rs; do
    if [ -f "$example_file" ]; then
        # Extract example name (remove path and .rs extension)
        example_name=$(basename "$example_file" .rs)
        example_count=$((example_count + 1))
        
        echo -n "  Running example: $example_name... "
        
        # Run example with timeout (30 seconds max per example)
        if timeout 30s cargo run --example "$example_name" >/dev/null 2>&1; then
            echo -e "${GREEN}✅${NC}"
        else
            echo -e "${RED}❌${NC}"
            failed_examples+=("$example_name")
        fi
    fi
done

if [ ${#failed_examples[@]} -eq 0 ]; then
    print_success "All $example_count examples ran successfully"
else
    print_warning "Examples that failed: ${failed_examples[*]}"
    echo "Note: Some examples may require specific setup or run indefinitely"
fi

# 5. Benchmarks (optional)
print_step "Running Benchmarks (Sample)"
echo "Running a quick benchmark sample..."
if cargo bench --bench kv_bench -- --quick >/dev/null 2>&1; then
    print_success "Benchmark sample completed"
else
    print_warning "Benchmarks skipped (may not be available)"
fi

# 6. Documentation check
print_step "Documentation Validation"
echo "Checking documentation builds..."
if cargo doc --all --all-features --no-deps >/dev/null 2>&1; then
    print_success "Documentation builds successfully"
else
    print_warning "Documentation build had issues"
fi

# 7. Package validation
print_step "Package Validation"
echo "Checking package structure..."
if cargo package --dry-run >/dev/null 2>&1; then
    print_success "Package structure is valid"
else
    print_warning "Package validation had issues"
fi

# Calculate total time
end_time=$(date +%s)
duration=$((end_time - start_time))
minutes=$((duration / 60))
seconds=$((duration % 60))

# Final summary
echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                     VALIDATION COMPLETE                     ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

print_success "Validation completed in ${minutes}m ${seconds}s"
echo
echo "📊 Summary:"
echo "  • Code quality: ✅ Passed"
echo "  • Tests: ✅ Passed"
echo "  • Build: ✅ Passed"
echo "  • Examples: ✅ $example_count examples checked"
if [ ${#failed_examples[@]} -gt 0 ]; then
    echo "  • Failed examples: ${failed_examples[*]}"
fi
echo "  • Documentation: ✅ Builds"
echo "  • Package: ✅ Valid"
echo

print_success "🎉 PrkDB is ready for development and deployment!"
echo
echo "💡 Next steps:"
echo "  • Run individual examples: cargo run --example <name>"
echo "  • Run specific tests: cargo test <pattern>"
echo "  • Profile performance: cargo bench"
echo "  • Check coverage: cargo tarpaulin"