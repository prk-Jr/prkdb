# Mixed-Client Integration And Cross-Language Benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a correctness-first mixed-client integration flow that proves Python, TypeScript, and Go generated clients can write to and read from one live PrkDB server, while also expanding the cross-language benchmark category to include Go and cleaning up benchmark script drift.

**Architecture:** Keep the existing per-language client-feature jobs intact for failure isolation, and add one new shell-orchestrated mixed-client flow that starts a single server, generates all three clients, runs concurrent writes through dedicated language runners, performs per-language cross-read checks, and then finishes with one Python aggregate verifier over HTTP. Separately, keep the current cross-language benchmark job as telemetry, but normalize its scripts, add a Go benchmark runner, and make the local benchmark script mirror the CI shape closely enough for debugging.

**Tech Stack:** Bash orchestration, Python (`httpx`, `asyncio`, generated Python client), TypeScript (`tsx`/`ts-node`, generated TypeScript client), Go (`go run`, generated Go client), GitHub Actions, existing `prkdb-cli` schema/codegen flows, Markdown docs

---

## Current Repo State

- The repo already has isolated client-feature integration scripts:
  - `scripts/test_client_features.sh`
  - `scripts/test_client_features_ts.sh`
  - `scripts/test_client_features_go.sh`
- The repo already has benchmark runners for Python and TypeScript:
  - `benches/bench_python.py`
  - `benches/bench_ts.ts`
- There is no Go benchmark runner today.
- The `cross-lang-benchmark` workflow in `.github/workflows/ci.yml` only generates and runs Python and TypeScript clients.
- `scripts/run_benchmarks_local.sh` mirrors the old benchmark shape and also only handles Python and TypeScript.
- The repo does not have a root `go.mod`, so any Go runner that imports generated Go client code must execute from a temporary Go module, not directly from the repo root.
- `README.md` already carries the benchmark caveat, but it does not describe the mixed-client integration coverage or the expanded cross-language benchmark category.
- `docs/.gitignore` ignores `docs/superpowers`, so plan/spec files live there for workflow purposes but are not added to version control unless explicitly force-added.

## File Structure Map

- Create: `benches/bench_go.go`
  Purpose: Go benchmark runner template copied into a temporary Go module alongside the generated Go client.
- Modify: `benches/bench_python.py`
  Purpose: normalize benchmark configuration, deterministic IDs, generated-client path handling, and output shape.
- Modify: `benches/bench_ts.ts`
  Purpose: normalize benchmark configuration, deterministic IDs, and output shape.
- Create: `scripts/mixed_client_runner.py`
  Purpose: generated-Python-client runner that performs write phase plus cross-language read validation for the mixed-client integration flow.
- Create: `scripts/mixed_client_runner.ts`
  Purpose: generated-TypeScript-client runner that performs write phase plus cross-language read validation for the mixed-client integration flow.
- Create: `scripts/mixed_client_runner.go`
  Purpose: Go mixed-client runner template copied into a temporary Go module alongside the generated Go client.
- Create: `scripts/verify_mixed_client_results.py`
  Purpose: aggregate verifier that pages the collection over HTTP, counts IDs by prefix, and checks deterministic sample IDs.
- Create: `scripts/test_mixed_client_integration.sh`
  Purpose: orchestrate one live server, schema registration, client generation, concurrent runner execution, aggregate verification, and cleanup.
- Modify: `scripts/run_benchmarks_local.sh`
  Purpose: generate the Go client, run the Go benchmark, and align local benchmark behavior with CI.
- Modify: `.github/workflows/ci.yml`
  Purpose: add the mixed-client integration job and extend the existing cross-language benchmark job to generate and run Go.
- Modify: `README.md`
  Purpose: describe the mixed-client integration coverage and the expanded cross-language benchmark category without weakening the benchmark caveat.

## Task 1: Normalize Benchmark Runners And Add Go Coverage

**Files:**
- Create: `benches/bench_go.go`
- Modify: `benches/bench_python.py`
- Modify: `benches/bench_ts.ts`
- Test: `scripts/run_benchmarks_local.sh`

- [ ] **Step 1: Capture the current benchmark gap as the failing baseline**

Run the existing local benchmark flow:

```bash
./scripts/run_benchmarks_local.sh
```

Expected before changes:
- Python and TypeScript benchmark output appear.
- There is no Go benchmark section.
- The script still assumes the old Python/TypeScript generated-client layout and comments.

- [ ] **Step 2: Make the Python benchmark runner deterministic and layout-safe**

Update `benches/bench_python.py` so it:
- accepts a single `--server`
- accepts `--records`, `--collection`, `--id-prefix`, and `--client-dir`
- imports the generated client from the configured output directory instead of hardcoding `client_py`
- emits deterministic IDs

Target shape:

```python
parser.add_argument("--server", default=os.environ.get("PRKDB_SERVER", "http://127.0.0.1:8080"))
parser.add_argument("--records", type=int, default=int(os.environ.get("NUM_RECORDS", "10000")))
parser.add_argument("--collection", default=os.environ.get("PRKDB_COLLECTION", "benchmark"))
parser.add_argument("--id-prefix", default=os.environ.get("PRKDB_ID_PREFIX", "py-bench"))
parser.add_argument("--client-dir", default=os.environ.get("PRKDB_CLIENT_DIR", "benches/client_py"))
```

- [ ] **Step 3: Make the TypeScript benchmark runner deterministic and layout-safe**

Update `benches/bench_ts.ts` so it:
- reads `PRKDB_SERVER`, `NUM_RECORDS`, `PRKDB_COLLECTION`, `PRKDB_ID_PREFIX`
- keeps importing the generated collection file for `benchmark`
- emits deterministic IDs and the same output fields as Python

Target shape:

```ts
const SERVER_URL = process.env.PRKDB_SERVER || "http://127.0.0.1:8080";
const COLLECTION = process.env.PRKDB_COLLECTION || "benchmark";
const ID_PREFIX = process.env.PRKDB_ID_PREFIX || "ts-bench";
```

- [ ] **Step 4: Add the Go benchmark runner**

Create `benches/bench_go.go` as a template that will be copied into a temporary Go module
with the generated `client_go` directory. The file should assume the temp module name
`benchclient` and import the generated package through that module path.

Target shape:

```go
package main

import models "benchclient/client_go"

func main() {
    server := envOr("PRKDB_SERVER", "http://127.0.0.1:8080")
    collection := envOr("PRKDB_COLLECTION", "benchmark")
    idPrefix := envOr("PRKDB_ID_PREFIX", "go-bench")
    numRecords := mustAtoi(envOr("NUM_RECORDS", "10000"))

    client := models.NewPrkDbClient(server)
    // write deterministic IDs like fmt.Sprintf("%s-%06d", idPrefix, i)
    // print duration and MB/s
}
```

The local script and CI workflow must create a temp directory like:

```bash
GO_BENCH_DIR="$(mktemp -d)"
cp benches/bench_go.go "$GO_BENCH_DIR/main.go"
cp -R benches/client_go "$GO_BENCH_DIR/client_go"
(
  cd "$GO_BENCH_DIR"
  go mod init benchclient
  go run main.go
)
```

- [ ] **Step 5: Re-run the local benchmark flow**

Run:

```bash
./scripts/run_benchmarks_local.sh
```

Expected after these runner changes, once orchestration is updated in Task 2:
- Python, TypeScript, and Go benchmark sections all appear.
- IDs are deterministic by language prefix.
- Output fields are consistent enough to compare internal telemetry.

- [ ] **Step 6: Commit the benchmark runner changes**

```bash
git add benches/bench_python.py benches/bench_ts.ts benches/bench_go.go
git commit -m "feat: add go cross-language benchmark runner"
```

## Task 2: Update Local And CI Benchmark Orchestration

**Files:**
- Modify: `scripts/run_benchmarks_local.sh`
- Modify: `.github/workflows/ci.yml`
- Test: `scripts/run_benchmarks_local.sh`

- [ ] **Step 1: Extend the local benchmark script to generate and run Go**

Modify `scripts/run_benchmarks_local.sh` so it:
- generates `benches/client_go`
- cleans up `benches/client_go`
- runs the Go benchmark from a temporary Go module
- uses the normalized env vars for all three languages

Key additions:

```bash
mkdir -p benches/client_py benches/client_ts benches/client_go
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang go --out benches/client_go --collection benchmark

export PRKDB_SERVER="$SERVER_HTTP_URL"
export PRKDB_COLLECTION=benchmark

GO_BENCH_DIR="$(mktemp -d)"
cp benches/bench_go.go "$GO_BENCH_DIR/main.go"
cp -R benches/client_go "$GO_BENCH_DIR/client_go"
(
  cd "$GO_BENCH_DIR"
  go mod init benchclient
  go run main.go
)
```

- [ ] **Step 2: Re-run the local benchmark script to verify the old gap is closed**

Run:

```bash
./scripts/run_benchmarks_local.sh
```

Expected:
- the script exits successfully
- Python, TypeScript, and Go each report benchmark output
- cleanup removes `benches/client_py`, `benches/client_ts`, `benches/client_go`, `bench.proto`, and `bench.desc`

- [ ] **Step 3: Extend the CI cross-language benchmark job**

Update `.github/workflows/ci.yml` in the `cross-lang-benchmark` job to:
- set up Go with `actions/setup-go`
- generate the Go client in `benches/client_go`
- run the Go benchmark after Python and TypeScript
- remove stale comments that imply ad hoc import-path fixes are still required

Target workflow fragment:

```yaml
      - name: Setup Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Generate Clients
        run: |
          ./target/release/prkdb-cli codegen --server http://127.0.0.1:50051 --lang python --out benches/client_py --collection benchmark
          ./target/release/prkdb-cli codegen --server http://127.0.0.1:50051 --lang typescript --out benches/client_ts --collection benchmark
          ./target/release/prkdb-cli codegen --server http://127.0.0.1:50051 --lang go --out benches/client_go --collection benchmark

      - name: "📗 Run Go Benchmark"
        run: |
          GO_BENCH_DIR="$(mktemp -d)"
          cp benches/bench_go.go "$GO_BENCH_DIR/main.go"
          cp -R benches/client_go "$GO_BENCH_DIR/client_go"
          (
            cd "$GO_BENCH_DIR"
            go mod init benchclient
            go run main.go
          )
        env:
          PRKDB_SERVER: http://127.0.0.1:8080
          PRKDB_COLLECTION: benchmark
          PRKDB_ID_PREFIX: go-bench
          NUM_RECORDS: 10000
```

- [ ] **Step 4: Dry-run the changed local benchmark workflow one more time**

Run:

```bash
./scripts/run_benchmarks_local.sh
```

Expected:
- the local script still passes after the workflow-oriented cleanup
- the log output remains readable enough to diagnose which language failed if a future regression appears

- [ ] **Step 5: Commit the orchestration updates**

```bash
git add scripts/run_benchmarks_local.sh .github/workflows/ci.yml
git commit -m "feat: expand cross-language benchmark coverage"
```

## Task 3: Add Mixed-Client Language Runners

**Files:**
- Create: `scripts/mixed_client_runner.py`
- Create: `scripts/mixed_client_runner.ts`
- Create: `scripts/mixed_client_runner.go`
- Test: `scripts/test_mixed_client_integration.sh`

- [ ] **Step 1: Write the runner contract first**

Use the same CLI/env contract for all three runner files:

```text
--mode write|read
--server http://127.0.0.1:<port>
--collection benchmark
--records <n>
--id-prefix py|ts|go
--client-dir <generated-client-dir>
--sample-id py-000001 --sample-id py-000700 ...
```

Each runner must:
- write deterministic IDs during `write`
- read and validate exact sample IDs during `read`
- exit non-zero on any failed write or read check

- [ ] **Step 2: Implement the Python mixed-client runner**

Create `scripts/mixed_client_runner.py` using the generated Python client:

```python
async def run_write(client, collection: str, id_prefix: str, records: int) -> None:
    for index in range(1, records + 1):
        record_id = f"{id_prefix}-{index:06d}"
        await client.put(collection, {"id": record_id, "payload": record_id, "timestamp": int(time.time() * 1000)})

async def run_read(client, collection: str, sample_ids: list[str]) -> None:
    rows = await client.list(collection, limit=10000)
    found = {row["id"] for row in rows}
    missing = [sample_id for sample_id in sample_ids if sample_id not in found]
    if missing:
        raise AssertionError(f"missing ids: {missing}")
```

- [ ] **Step 3: Implement the TypeScript mixed-client runner**

Create `scripts/mixed_client_runner.ts` so it dynamically imports the generated
TypeScript collection module from `--client-dir` at runtime. Do not hardcode a repo-local
relative import, because the mixed-client integration flow generates the TypeScript client
inside the temp work directory.

```ts
const moduleUrl = pathToFileURL(path.join(clientDir, `${collection}.ts`)).href;
const { PrkDbClient } = await import(moduleUrl);

async function runWrite(client: PrkDbClient, collection: string, idPrefix: string, records: number) {
  for (let index = 1; index <= records; index += 1) {
    const id = `${idPrefix}-${index.toString().padStart(6, "0")}`;
    await client.put(collection, { id, payload: id, timestamp: Date.now() });
  }
}

async function runRead(client: PrkDbClient, collection: string, sampleIds: string[]) {
  const rows = await client.list<any>(collection, { limit: 10000 });
  const found = new Set(rows.map(row => row.id));
  const missing = sampleIds.filter(id => !found.has(id));
  if (missing.length > 0) throw new Error(`missing ids: ${missing.join(", ")}`);
}
```

- [ ] **Step 4: Implement the Go mixed-client runner**

Create `scripts/mixed_client_runner.go` as a template run from a temporary Go module with
module name `mixedclient` and a copied `client_go` directory:

```go
package main

import models "mixedclient/client_go"

func runWrite(client *models.PrkDbClient, collection, idPrefix string, records int) error {
    for index := 1; index <= records; index++ {
        id := fmt.Sprintf("%s-%06d", idPrefix, index)
        row := map[string]interface{}{"id": id, "payload": id, "timestamp": time.Now().UnixMilli()}
        if err := client.Put(collection, row); err != nil {
            return err
        }
    }
    return nil
}
```

Use `ListRaw` for the read phase, checking the returned IDs exactly as the Python and TypeScript runners do.

- [ ] **Step 5: Smoke-check each runner through the mixed-client orchestrator once it exists**

Run after Task 4 wiring is in place:

```bash
./scripts/test_mixed_client_integration.sh
```

Expected:
- all three runners complete their write phase
- all three runners complete their cross-language read phase

- [ ] **Step 6: Commit the runner scripts**

```bash
git add scripts/mixed_client_runner.py scripts/mixed_client_runner.ts scripts/mixed_client_runner.go
git commit -m "feat: add mixed-client language runners"
```

## Task 4: Add The Mixed-Client Aggregate Verifier And Orchestrator

**Files:**
- Create: `scripts/verify_mixed_client_results.py`
- Create: `scripts/test_mixed_client_integration.sh`
- Test: `scripts/test_mixed_client_integration.sh`

- [ ] **Step 1: Write the aggregate verifier first**

Create `scripts/verify_mixed_client_results.py` so it:
- reads `--server`, `--collection`, and expected counts for `py`, `ts`, and `go`
- pages `/collections/<collection>/data`
- counts rows by `id` prefix
- verifies deterministic sample IDs

Target shape:

```python
async def fetch_all_rows(base_url: str, collection: str) -> list[dict]:
    rows = []
    offset = 0
    while True:
        response = await client.get(f"{base_url}/collections/{collection}/data", params={"limit": 500, "offset": offset})
        batch = response.json()["data"]["data"]
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
    return rows
```

- [ ] **Step 2: Run the verifier before the orchestrator exists to capture the failing baseline**

Run:

```bash
python3 scripts/verify_mixed_client_results.py --server http://127.0.0.1:9999 --collection benchmark --expect-py 1 --expect-ts 1 --expect-go 1
```

Expected:
- FAIL because there is no server and no orchestration flow yet.

- [ ] **Step 3: Implement the mixed-client orchestration script**

Create `scripts/test_mixed_client_integration.sh` that:
- creates a temp work directory
- reserves ports
- starts `prkdb-cli serve`
- writes `bench.proto` and `bench.desc`
- registers the `benchmark` collection schema
- generates `client_py`, `client_ts`, and `client_go` inside the temp work directory
- launches the three language runners concurrently in `write` mode
- waits for all three write phases to succeed
- launches the three language runners in `read` mode with deterministic sample IDs from all prefixes
- runs `scripts/verify_mixed_client_results.py`
- prints server logs on failure
- cleans up on exit

Target structure:

```bash
PY_COUNT=700
TS_COUNT=1000
GO_COUNT=1300

python3 scripts/mixed_client_runner.py --mode write --server "$SERVER_HTTP_URL" --collection benchmark --records "$PY_COUNT" --id-prefix py --client-dir "$WORK_DIR/client_py" &
PY_PID=$!

npx tsx scripts/mixed_client_runner.ts --mode write --server "$SERVER_HTTP_URL" --collection benchmark --records "$TS_COUNT" --id-prefix ts --client-dir "$WORK_DIR/client_ts" &
TS_PID=$!

GO_RUN_DIR="$WORK_DIR/go_runner"
mkdir -p "$GO_RUN_DIR"
cp scripts/mixed_client_runner.go "$GO_RUN_DIR/main.go"
cp -R "$WORK_DIR/client_go" "$GO_RUN_DIR/client_go"
(
  cd "$GO_RUN_DIR"
  go mod init mixedclient
  go run main.go --mode write --server "$SERVER_HTTP_URL" --collection benchmark --records "$GO_COUNT" --id-prefix go --client-dir "$WORK_DIR/client_go"
) &
GO_PID=$!

wait "$PY_PID" "$TS_PID" "$GO_PID"

python3 scripts/mixed_client_runner.py --mode read --server "$SERVER_HTTP_URL" --collection benchmark --client-dir "$WORK_DIR/client_py" --sample-id py-000001 --sample-id ts-000001 --sample-id go-000001
npx tsx scripts/mixed_client_runner.ts --mode read --server "$SERVER_HTTP_URL" --collection benchmark --client-dir "$WORK_DIR/client_ts" --sample-id py-000001 --sample-id ts-000001 --sample-id go-000001
(
  cd "$GO_RUN_DIR"
  go run main.go --mode read --server "$SERVER_HTTP_URL" --collection benchmark --client-dir "$WORK_DIR/client_go" --sample-id py-000001 --sample-id ts-000001 --sample-id go-000001
)

python3 scripts/verify_mixed_client_results.py --server "$SERVER_HTTP_URL" --collection benchmark --expect-py "$PY_COUNT" --expect-ts "$TS_COUNT" --expect-go "$GO_COUNT" --sample-id py-000001 --sample-id ts-000001 --sample-id go-000001
```

- [ ] **Step 4: Run the mixed-client integration script**

Run:

```bash
./scripts/test_mixed_client_integration.sh
```

Expected:
- PASS
- Python, TypeScript, and Go each report successful writes
- Python, TypeScript, and Go each report successful read validation against all prefixes
- the aggregate verifier reports the exact expected total and per-prefix counts

- [ ] **Step 5: Commit the mixed-client orchestration flow**

```bash
git add scripts/verify_mixed_client_results.py scripts/test_mixed_client_integration.sh
git commit -m "feat: add mixed-client integration flow"
```

## Task 5: Wire Mixed-Client Integration Into CI And Update Docs

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`
- Test: `./scripts/test_mixed_client_integration.sh`
- Test: `./scripts/run_benchmarks_local.sh`

- [ ] **Step 1: Add the new mixed-client integration CI job**

Modify `.github/workflows/ci.yml` to add a job alongside the existing client-feature jobs with:
- `actions/setup-python`
- `actions/setup-node`
- `actions/setup-go`
- `protobuf-compiler`
- `cargo build -p prkdb-cli --bin prkdb-cli`
- `pip install httpx`
- `npm install -g tsx typescript`
- `./scripts/test_mixed_client_integration.sh`

Target workflow fragment:

```yaml
  mixed-client-integration:
    name: Mixed Client Integration
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Protoc
        run: sudo apt-get install -y protobuf-compiler
      - uses: dtolnay/rust-toolchain@stable
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - name: Install Python dependencies
        run: pip install httpx
      - name: Install TypeScript runner
        run: npm install -g tsx typescript
      - name: Build PrkDB CLI
        run: cargo build -p prkdb-cli --bin prkdb-cli
      - name: Run Mixed Client Integration
        run: ./scripts/test_mixed_client_integration.sh
        env:
          PRKDB_BIN: ./target/debug/prkdb-cli
          SKIP_BUILD: "1"
```

- [ ] **Step 2: Update `README.md` to stay truthful**

Add concise wording that:
- CI includes isolated client-feature checks per language
- CI now also includes a mixed-client integration flow across Python, TypeScript, and Go
- the cross-language benchmark category now covers the three generated clients
- benchmark caveats remain intact

Suggested insert near the benchmark section:

```md
CI also runs generated-client coverage in two modes:
- isolated per-language client feature integration for Python, TypeScript, and Go
- mixed-client integration where all three generated clients write to and read from one live PrkDB server
```

- [ ] **Step 3: Run the final local verification set**

Run:

```bash
./scripts/test_mixed_client_integration.sh
./scripts/run_benchmarks_local.sh
```

Expected:
- both commands pass
- the mixed-client flow proves write and read interoperability across all three generated clients
- the local benchmark flow prints Python, TypeScript, and Go telemetry

- [ ] **Step 4: Commit the CI and doc updates**

```bash
git add .github/workflows/ci.yml README.md
git commit -m "feat: add mixed-client CI coverage"
```

## Final Verification Checklist

- [ ] Run `./scripts/test_mixed_client_integration.sh`
- [ ] Run `./scripts/run_benchmarks_local.sh`
- [ ] If a language runner fails, inspect the per-language output plus server log before changing code.
- [ ] If the aggregate verifier fails, compare expected counts against actual `id` prefixes before changing code.
- [ ] Confirm `.github/workflows/ci.yml` now contains:
  - isolated Python, TypeScript, and Go client-feature jobs
  - a new mixed-client integration job
  - an expanded cross-language benchmark job with Go
- [ ] Confirm `README.md` still states that benchmark numbers are trend-tracking telemetry, not apples-to-apples product comparisons.
