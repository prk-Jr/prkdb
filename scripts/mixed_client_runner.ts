import { pathToFileURL } from 'url';
import path from 'path';

type Mode = 'write' | 'read';

interface RunnerArgs {
  mode: Mode;
  server: string;
  collection: string;
  records: number;
  idPrefix: string;
  clientDir: string;
  sampleIds: string[];
}

const DEFAULT_SERVER = 'http://127.0.0.1:8080';
const DEFAULT_COLLECTION = 'benchmark';
const DEFAULT_RECORDS = 1000;
const DEFAULT_ID_PREFIX = 'ts';

function envOrDefault(name: string, fallback: string): string {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  if (value.trim() === '') {
    throw new Error(`${name} must not be empty`);
  }
  return value;
}

function envInt(name: string, fallback: number): number {
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

function parseArgs(argv: string[]): RunnerArgs {
  const args: Partial<RunnerArgs> & { sampleIds?: string[] } = {
    mode: (envOrDefault('PRKDB_MODE', 'write') as Mode),
    server: envOrDefault('PRKDB_SERVER', DEFAULT_SERVER),
    collection: envOrDefault('PRKDB_COLLECTION', DEFAULT_COLLECTION),
    records: envInt('NUM_RECORDS', DEFAULT_RECORDS),
    idPrefix: envOrDefault('PRKDB_ID_PREFIX', DEFAULT_ID_PREFIX),
    clientDir: envOrDefault('PRKDB_CLIENT_DIR', '.'),
    sampleIds: [],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    switch (token) {
      case '--mode':
        args.mode = (argv[++index] as Mode) ?? args.mode;
        break;
      case '--server':
        args.server = argv[++index] ?? args.server;
        break;
      case '--collection':
        args.collection = argv[++index] ?? args.collection;
        break;
      case '--records': {
        const parsed = Number.parseInt(argv[++index] ?? '', 10);
        if (!Number.isFinite(parsed) || Number.isNaN(parsed) || parsed <= 0) {
          throw new Error('--records must be a positive integer');
        }
        args.records = parsed;
        break;
      }
      case '--id-prefix':
        args.idPrefix = argv[++index] ?? args.idPrefix;
        break;
      case '--client-dir':
        args.clientDir = argv[++index] ?? args.clientDir;
        break;
      case '--sample-id':
        args.sampleIds!.push(argv[++index] ?? '');
        break;
      default:
        throw new Error(`unknown argument: ${token}`);
    }
  }

  if (!args.mode || (args.mode !== 'write' && args.mode !== 'read')) {
    throw new Error('--mode must be write or read');
  }
  if (!args.server) {
    throw new Error('--server must not be empty');
  }
  if (!args.collection) {
    throw new Error('--collection must not be empty');
  }
  if (!args.idPrefix) {
    throw new Error('--id-prefix must not be empty');
  }
  if (!args.clientDir) {
    throw new Error('--client-dir must not be empty');
  }
  if (args.mode === 'read' && (!args.sampleIds || args.sampleIds.length === 0)) {
    throw new Error('at least one --sample-id is required');
  }

  return args as RunnerArgs;
}

function buildRecord(index: number, idPrefix: string) {
  const recordId = `${idPrefix}-${String(index + 1).padStart(6, '0')}`;
  return {
    id: recordId,
    payload: recordId,
    timestamp: Date.now(),
  };
}

function buildClientModuleUrl(clientDir: string, collection: string): string {
  return pathToFileURL(path.join(clientDir, `${collection}.ts`)).href;
}

function normalizeRows(rows: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(rows)) {
    return [];
  }
  return rows.filter((row): row is Record<string, unknown> => row !== null && typeof row === 'object');
}

async function loadClientClass(clientDir: string, collection: string) {
  const moduleUrl = buildClientModuleUrl(clientDir, collection);
  const generatedModule = await import(moduleUrl);
  const clientClass = generatedModule.PrkDbClient ?? generatedModule.default?.PrkDbClient ?? generatedModule.default;

  if (!clientClass) {
    throw new Error(`generated module at ${moduleUrl} did not export PrkDbClient`);
  }

  return clientClass;
}

async function runWrite(args: RunnerArgs) {
  const PrkDbClient = await loadClientClass(args.clientDir, args.collection);
  const client = new PrkDbClient(args.server, process.env.PRKDB_CREDENTIAL || undefined);

  for (let index = 0; index < args.records; index += 1) {
    await client.put(args.collection, buildRecord(index, args.idPrefix));
  }

  console.log(
    `✅ TypeScript mixed-client write: collection=${args.collection} records=${args.records} ` +
      `range=${args.idPrefix}-000001..${args.idPrefix}-${String(args.records).padStart(6, '0')}`,
  );
}

async function runRead(args: RunnerArgs) {
  const PrkDbClient = await loadClientClass(args.clientDir, args.collection);
  const client = new PrkDbClient(args.server, process.env.PRKDB_CREDENTIAL || undefined);
  const rows = normalizeRows(await client.list(args.collection, { limit: 10000 }));
  const rowsById = new Map<string, Record<string, unknown>>();

  for (const row of rows) {
    const id = row.id;
    if (typeof id === 'string') {
      rowsById.set(id, row);
    }
  }

  for (const sampleId of args.sampleIds) {
    const item = await client.get(args.collection, sampleId);
    if (!item || !rowsById.has(sampleId)) {
      throw new Error(`missing expected sample id: ${sampleId}`);
    }
  }

  console.log(
    `✅ TypeScript mixed-client read: collection=${args.collection} sample_ids=${args.sampleIds.length} ` +
      `fetched_rows=${rowsById.size}`,
  );
}

async function main() {
  try {
    const args = parseArgs(process.argv.slice(2));
    if (args.mode === 'write') {
      await runWrite(args);
    } else {
      if (args.sampleIds.length === 0) {
        throw new Error('at least one --sample-id is required');
      }
      await runRead(args);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`❌ ${message}`);
    process.exit(1);
  }
}

void main();
