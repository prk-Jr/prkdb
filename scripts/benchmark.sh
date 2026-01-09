#!/bin/bash
# Benchmark runner for PrkDB
# Runs all benchmarks and generates comparison reports

set -e

echo "====================================="
echo "PrkDB Benchmark Suite"
echo "====================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
BENCH_DIR="target/criterion"
REPORT_DIR="benchmark_reports"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Create report directory
mkdir -p "$REPORT_DIR"

echo "Benchmark configuration:"
echo "  Output directory: $BENCH_DIR"
echo "  Report directory: $REPORT_DIR"
echo "  Timestamp: $TIMESTAMP"
echo ""

# Function to run a benchmark
run_benchmark() {
    local bench_name=$1
    echo -e "${YELLOW}Running benchmark: $bench_name${NC}"
    
    if cargo bench --bench "$bench_name"; then
        echo -e "${GREEN}✓ $bench_name completed${NC}"
    else
        echo "Warning: $bench_name failed"
    fi
    echo ""
}

# Run all benchmarks
echo "Running benchmarks..."
echo ""

run_benchmark "batch_bench"
run_benchmark "batching_window_bench"
run_benchmark "consumer_bench"
run_benchmark "kv_bench"
run_benchmark "partitioning_bench"
run_benchmark "streaming_bench"
run_benchmark "windowing_bench"
run_benchmark "joins_bench"

# Generate consolidated report
echo "Generating benchmark report..."
{
    echo "====================================="
    echo "PrkDB Benchmark Report"
    echo "Generated: $(date)"
    echo "====================================="
    echo ""
    
    echo "Benchmark Suites Executed:"
    for bench in batch_bench batching_window_bench consumer_bench kv_bench partitioning_bench streaming_bench windowing_bench joins_bench; do
        echo "  - $bench"
    done
    echo ""
    
    echo "Performance Areas Covered:"
    echo "  - Consumer operations (poll, commit, seek, offset management)"
    echo "  - Key-value store operations (read/write throughput, scans)"
    echo "  - Partitioning performance (hashing, assignment latency)"
    echo "  - Streaming performance (throughput, backpressure, buffers)"
    echo "  - Windowing performance (tumbling/sliding, aggregations)"
    echo "  - Join performance (inner/left/outer, buffer efficiency)"
    echo ""
    
    echo "Criterion Results Location:"
    if [ -d "target/criterion" ]; then
        echo "  - Individual benchmark data: target/criterion/"
        echo "  - HTML reports will be generated in: target/criterion/report/"
    else
        echo "  - Criterion data not found (benchmarks may need actual execution)"
    fi
    
} > "$REPORT_DIR/benchmark_summary_$TIMESTAMP.txt"

echo -e "${GREEN}✓ Benchmark summary saved to: $REPORT_DIR/benchmark_summary_$TIMESTAMP.txt${NC}"

echo ""
echo "====================================="
echo "Benchmark Summary"
echo "====================================="
echo ""

# Display summary from criterion
echo "Benchmark execution completed."
echo ""

if [ -f "$REPORT_DIR/benchmark_summary_$TIMESTAMP.txt" ]; then
    echo "Benchmark summary available at:"
    echo "  - Summary: $REPORT_DIR/benchmark_summary_$TIMESTAMP.txt"
    echo ""
fi

if [ -d "target/criterion" ]; then
    echo "Criterion benchmark data:"
    echo "  - Raw data: target/criterion/"
    
    # Check if HTML reports exist
    if [ -f "target/criterion/report/index.html" ]; then
        echo "  - HTML report: target/criterion/report/index.html"
    else
        echo "  - HTML reports: Use 'cargo bench' to generate detailed HTML reports"
    fi
else
    echo "Note: To generate detailed criterion reports, run individual benchmarks:"
    echo "  cargo bench --bench streaming_bench"
    echo "  cargo bench --bench windowing_bench"
    echo "  cargo bench --bench joins_bench"
fi

echo ""
echo -e "${GREEN}✓ Benchmarks complete!${NC}"
echo ""
echo "To run individual benchmarks with full criterion output:"
echo "  cargo bench --bench streaming_bench"
echo "  cargo bench --bench windowing_bench" 
echo "  cargo bench --bench joins_bench"
echo ""
echo "To generate HTML reports:"
echo "  cargo bench  # Runs all benchmarks with full criterion reporting"