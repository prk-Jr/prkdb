
import sys
import os
import json
import random
import string
import argparse
import asyncio
import time

# Add generated client to path
sys.path.append(os.path.join(os.getcwd(), "client_py"))

try:
    from prkdb_client import PrkDbClient
except ImportError as e:
    print(f"❌ Error: PrkDB Client not found or dependency missing: {e}")
    sys.exit(1)

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def main():
    parser = argparse.ArgumentParser(description="PrkDB Python Benchmark")
    parser.add_argument("--server", default="http://127.0.0.1:8081", help="PrkDB Server URL(s), comma-separated")
    parser.add_argument("--records", type=int, default=10000, help="Number of records")
    args = parser.parse_args()

    servers = args.server.split(",")
    try:
        asyncio.run(run_benchmark(servers, args.records))
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted")

async def run_benchmark(servers, num_records):
    print(f"🚀 Connecting to {servers}...")
    
    # Create a pool of clients
    clients = [PrkDbClient(host=s) for s in servers]
    
    print(f"  📤 Starting Producer: {num_records} records...")
    
    produce_start = time.time()
    success_count = 0
    
    # Batch concurrent requests to simulate load
    BATCH_SIZE = 100
    
    for i in range(0, num_records, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE, num_records)
        current_batch = []
        
        for j in range(i, batch_end):
            data = {
                "id": f"bench_{j}",
                "payload": random_string(100),
                "timestamp": int(time.time() * 1000)
            }
            # Round-robin initial client
            client = clients[j % len(clients)]
            current_batch.append(put_with_retry(clients, "benchmark", data))
            
        # Wait for batch
        results = await asyncio.gather(*current_batch, return_exceptions=True)
        for res in results:
            if not isinstance(res, Exception):
                success_count += 1
            else:
                print(f"Error: {res}")

    # Close all clients
    for c in clients:
        await c.close()

    duration = time.time() - produce_start
    mbps = (num_records * 100) / duration / 1024 / 1024
    
    print(f"✅ Producer Finished: {success_count}/{num_records} records")
    print(f"⏱️  Duration: {duration:.2f}s")
    print(f"📈 Throughput: {mbps:.2f} MB/s")

async def put_with_retry(clients, collection, data):
    # Try clients in order (starting from random or just round-robin)
    # Simple failover: try all clients
    last_err = None
    for client in clients:
        try:
            await client.put(collection, data)
            return
        except Exception as e:
            last_err = e
            # Logic to detect "Not Leader"?
            # Serve.rs returns 500 for forwarding error.
            # We just try next node.
            continue
    raise last_err

if __name__ == "__main__":
    main()
