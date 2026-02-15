#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# Cleanup function
cleanup() {
    echo -e "${RED}Stopping cluster...${NC}"
    kill $(jobs -p) 2>/dev/null || true
    rm -rf /tmp/prkdb_cluster_*
}
trap cleanup EXIT

echo -e "${GREEN}Building PrkDB (Release)...${NC}"
cargo build --release -p prkdb-cli --bin prkdb-server --bin prkdb-cli

PRKDB_SERVER=./target/release/prkdb-server
PRKDB_CLI=./target/release/prkdb-cli

# Set admin token for security
export PRKDB_ADMIN_TOKEN=benchmark_secret

# Setup cluster directories
mkdir -p /tmp/prkdb_cluster_1
mkdir -p /tmp/prkdb_cluster_2
mkdir -p /tmp/prkdb_cluster_3

echo -e "${GREEN}Starting 3-node cluster...${NC}"

# Node 1 (Bootstrap leader)
RUST_LOG=debug,raft=debug,prkdb=debug,openraft=debug $PRKDB_SERVER --verbose serve \
    --id 1 \
    --port 8081 \
    --grpc-port 50051 \
    --peers "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053" \
    --num-partitions 3 \
    > /tmp/prkdb_node_1.log 2>&1 &
PID1=$!

sleep 2

# Node 2
RUST_LOG=debug,raft=debug,prkdb=debug,openraft=debug $PRKDB_SERVER --verbose serve \
    --id 2 \
    --port 8082 \
    --grpc-port 50052 \
    --peers "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053" \
    > /tmp/prkdb_node_2.log 2>&1 &
PID2=$!

# Node 3
RUST_LOG=debug,raft=debug,prkdb=debug,openraft=debug $PRKDB_SERVER --verbose serve \
    --id 3 \
    --port 8083 \
    --grpc-port 50053 \
    --peers "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053" \
    > /tmp/prkdb_node_3.log 2>&1 &
PID3=$!

echo "Waiting for cluster to stabilize (15s)..."
sleep 15

echo -e "${GREEN}Creating partitioned collection 'benchmark'...${NC}"

# Retry loop for collection creation to handle leadership election timing
# With 3 partitions x 3 nodes = 9 Raft groups, election can take a while
success=false
for round in 1 2 3 4 5; do
    echo "--- Attempt round $round ---"
    for port in 50051 50052 50053; do
        echo "  Trying port $port..."
        if $PRKDB_CLI --server http://127.0.0.1:$port collection create benchmark --partitions 3 --replication-factor 3; then
            echo -e "${GREEN}✓ Collection created successfully on port $port${NC}"
            success=true
            break 2
        fi
    done
    echo "  Round $round failed, waiting 3s before next round..."
    sleep 3
done

if [ "$success" = false ]; then
    echo -e "${RED}Failed to create collection after 5 rounds.${NC}"
    echo "--- Node 1 last 20 lines ---"
    tail -n 20 /tmp/prkdb_node_1.log || true
    exit 1
fi

echo -e "${GREEN}Generating Python Client...${NC}"
$PRKDB_CLI codegen --server http://127.0.0.1:50051 --lang python --out ./client_py --force

echo -e "${GREEN}Running Python Benchmark...${NC}"
# Use existing python benchmark, pointing to one node
# TODO: Update python benchmark to support multiple nodes or handle redirects better?
# For now, simplistic approach
if [ -f "benches/bench_python.py" ]; then
    python3 benches/bench_python.py --server http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 --records 1000
else
    echo -e "${RED}Benchmark script not found!${NC}"
fi

echo -e "${GREEN}Benchmark Completed!${NC}"
