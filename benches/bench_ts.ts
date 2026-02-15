
import { PrkDbClient } from './client_ts/benchmark'; // Assumes client is generated here
import { performance } from 'perf_hooks';

// Polyfill for fetch if running in older Node.js environments (though Node 18+ has it native)
if (!globalThis.fetch) {
    console.error("❌ Error: fetch API not found. Please use Node.js 18+");
    process.exit(1);
}

const SERVER_URL = process.env.PRKDB_SERVER || "http://127.0.0.1:50051";
const NUM_RECORDS = parseInt(process.env.NUM_RECORDS || "10000");
const BATCH_SIZE = 100;

function randomString(length: number): string {
    let result = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

async function runBenchmark() {
    console.log(`🚀 Connecting to ${SERVER_URL}...`);
    const client = new PrkDbClient(SERVER_URL);

    console.log(`  📤 Starting Producer: ${NUM_RECORDS} records...`);
    const start = performance.now();
    let successCount = 0;

    // We use a semaphore-like pattern to limit concurrency if needed, 
    // but for max throughput we can just fire promises in batches.
    for (let i = 0; i < NUM_RECORDS; i += BATCH_SIZE) {
        const batchPromises: Promise<void>[] = [];
        const limit = Math.min(i + BATCH_SIZE, NUM_RECORDS);

        for (let j = i; j < limit; j++) {
            const data = {
                id: `bench_ts_${j}`,
                payload: randomString(100),
                timestamp: Date.now()
            };

            // Fire request
            batchPromises.push(
                client.put('benchmark', data)
                    .then(() => { successCount++; })
                    .catch(e => console.error(`Error: ${e.message}`))
            );
        }

        await Promise.all(batchPromises);
    }

    const duration = (performance.now() - start) / 1000; // seconds
    const mbps = (NUM_RECORDS * 100) / duration / 1024 / 1024;

    console.log(`✅ Producer Finished: ${successCount}/${NUM_RECORDS} records`);
    console.log(`⏱️  Duration: ${duration.toFixed(2)}s`);
    console.log(`📈 Throughput: ${mbps.toFixed(2)} MB/s`);
}

runBenchmark().catch(console.error);
