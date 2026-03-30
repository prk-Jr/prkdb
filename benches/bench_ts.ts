import { performance } from 'perf_hooks';
import { PrkDbClient } from './client_ts/benchmark';

if (!globalThis.fetch) {
  console.error('❌ Error: fetch API not found. Please use Node.js 18+');
  process.exit(1);
}

const BATCH_SIZE = 100;
const PAYLOAD = 'x'.repeat(100);

function getEnv(name: string, fallback: string): string {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  if (value.trim() === '') {
    throw new Error(`${name} must not be empty`);
  }
  return value;
}

function getPositiveIntEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    throw new Error(`${name} must be an integer, got ${JSON.stringify(value)}`);
  }
  if (parsed <= 0) {
    throw new Error(`${name} must be greater than 0, got ${parsed}`);
  }

  return parsed;
}

function getConfig() {
  return {
    serverUrl: getEnv('PRKDB_SERVER', 'http://127.0.0.1:8080'),
    numRecords: getPositiveIntEnv('NUM_RECORDS', 10000),
    collection: getEnv('PRKDB_COLLECTION', 'benchmark'),
    idPrefix: getEnv('PRKDB_ID_PREFIX', 'bench_ts'),
  };
}

function buildRecord(index: number, idPrefix: string) {
  return {
    id: `${idPrefix}_${index}`,
    payload: PAYLOAD,
    timestamp: Date.now(),
  };
}

async function runBenchmark() {
  const config = getConfig();

  console.log(`🚀 Connecting to ${config.serverUrl}...`);
  const client = new PrkDbClient(config.serverUrl);

  console.log(`  📤 Starting Producer: ${config.numRecords} records...`);
  const start = performance.now();
  let successCount = 0;
  let failureCount = 0;

  for (let batchStart = 0; batchStart < config.numRecords; batchStart += BATCH_SIZE) {
    const batchPromises: Promise<void>[] = [];
    const batchEnd = Math.min(batchStart + BATCH_SIZE, config.numRecords);

    for (let index = batchStart; index < batchEnd; index += 1) {
      batchPromises.push(
        client
          .put(config.collection, buildRecord(index, config.idPrefix))
          .then(() => {
            successCount += 1;
          })
          .catch((error: Error) => {
            failureCount += 1;
            console.error(`Error: ${error.message}`);
          })
      );
    }

    await Promise.all(batchPromises);
  }

  const duration = (performance.now() - start) / 1000;
  const mbps = (successCount * PAYLOAD.length) / duration / 1024 / 1024;

  console.log(`✅ Producer Finished: ${successCount}/${config.numRecords} records`);
  if (failureCount > 0) {
    console.log(`❌ Failed Writes: ${failureCount}`);
  }
  console.log(`⏱️  Duration: ${duration.toFixed(2)}s`);
  console.log(`📈 Throughput: ${mbps.toFixed(2)} MB/s`);

  if (failureCount > 0) {
    throw new Error(`benchmark failed with ${failureCount} write errors`);
  }
}

runBenchmark().catch((error: Error) => {
  console.error(`❌ ${error.message}`);
  process.exit(1);
});
