#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Build
echo -e "${GREEN}Building PrkDB...${NC}"
cargo build --release -p prkdb-cli --bin prkdb-server --bin prkdb-cli 2>/dev/null

PRKDB_SERVER=./target/release/prkdb-server
PRKDB_CLI=./target/release/prkdb-cli

# Cleanup
rm -rf /tmp/prkdb_debug
mkdir -p /tmp/prkdb_debug

# Start Server
echo -e "${GREEN}Starting Server on 50051...${NC}"
RUST_LOG=debug,tonic=debug,hyper=debug,h2=debug $PRKDB_SERVER --verbose serve \
    --id 1 \
    --port 8081 \
    --grpc-port 50051 \
    --host 127.0.0.1 \
    --peers "1=127.0.0.1:50051" \
    > /tmp/prkdb_debug.log 2>&1 &
PID=$!

echo "Waiting for server (5s)..."
sleep 5

# Check if running
if ! ps -p $PID > /dev/null; then
    echo -e "${RED}Server failed to start!${NC}"
    cat /tmp/prkdb_debug.log
    exit 1
fi

echo -e "${GREEN}Testing gRPC connectivity...${NC}"
# Try collection create
RUST_LOG=debug,tonic=debug,hyper=debug,h2=debug $PRKDB_CLI --verbose --server http://127.0.0.1:50051 collection create debug_collection --partitions 1 --replication-factor 1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Success!${NC}"
else
    echo -e "${RED}Failed!${NC}"
    echo "Server Logs:"
    cat /tmp/prkdb_debug.log
fi

kill $PID
rm -rf /tmp/prkdb_debug
