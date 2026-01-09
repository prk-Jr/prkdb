#!/bin/bash
# Benchmark script to measure Raft proposal batching performance

set -e

echo "🔬 PrkDB Raft Batching Benchmark"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REQUESTS=${1:-10000}
CONCURRENCY=${2:-100}

echo -e "${BLUE}Configuration:${NC}"
echo "  Total requests: $REQUESTS"
echo "  Concurrent clients: $CONCURRENCY"
echo ""

# Build in release mode
echo -e "${BLUE}Building in release mode...${NC}"
cargo build --release --example smart_client_benchmark 2>&1 | grep -E "(Finished|Compiling prkdb)" || true
echo ""

# Run benchmark
echo -e "${BLUE}Running benchmark...${NC}"
echo ""

# Start time
START_TIME=$(date +%s)

# Run the benchmark (if it exists, otherwise we'll create a simple one)
if [ -f "target/release/examples/smart_client_benchmark" ]; then
    ./target/release/examples/smart_client_benchmark \
        --requests $REQUESTS \
        --concurrency $CONCURRENCY 2>&1 | tee benchmark_output.txt
else
    echo -e "${YELLOW}smart_client_benchmark not found, using alternative...${NC}"
    # Use distributed_benchmark instead
    cargo run --release --example distributed_benchmark 2>&1 | tee benchmark_output.txt
fi

# End time
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo -e "${GREEN}✅ Benchmark complete in ${DURATION}s${NC}"
echo ""

# Extract metrics
echo -e "${BLUE}Metrics Summary:${NC}"
echo "=================================="

# Check if metrics are available in the output
if grep -q "ops/sec" benchmark_output.txt 2>/dev/null; then
    grep "ops/sec" benchmark_output.txt | tail -n 5
fi

if grep -q "Batch processed" benchmark_output.txt 2>/dev/null; then
    echo ""
    echo "Recent batches:"
    grep "Batch processed" benchmark_output.txt | tail -n 5
fi

echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Check Grafana for detailed metrics"
echo "2. Look at logs for batch sizes"
echo "3. Analyze prometheus metrics at http://localhost:9090"
echo ""
echo "Prometheus queries to try:"
echo "  - rate(raft_proposal_batches_total[1m])"
echo "  - histogram_quantile(0.99, raft_proposal_batch_size)"
echo "  - histogram_quantile(0.99, raft_proposal_latency_seconds)"
