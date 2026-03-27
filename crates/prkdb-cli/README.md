# prkdb-cli

`prkdb-cli` provides:

- remote gRPC data commands
- admin commands for collections, partitions, consumers, replication, and schema registry
- an optional HTTP server for browsing data, metrics, and WebSocket streams

## Build

```bash
cargo build --release -p prkdb-cli
```

The binary is available as:

```bash
./target/release/prkdb-cli
```

If you copy it into your `PATH`, the examples below use `prkdb`.

## Connection Defaults

- Collection, consumer, partition, and replication commands use the global `--server` flag and `PRKDB_SERVER`.
- `get`, `put`, `delete`, `batch-put`, `schema`, and `codegen` have their own `--server` flags.
- `--local` switches supported admin commands such as `consumer`, `partition`, and `replication` to the embedded database instead of remote gRPC.
- When `prkdb serve` binds to `0.0.0.0`, set `--advertised-grpc-address` or `PRKDB_ADVERTISED_GRPC_ADDR` so smart clients receive a dialable address.

## Common Commands

### Data

```bash
prkdb put my-key "hello" --server http://127.0.0.1:8080
prkdb get my-key --server http://127.0.0.1:8080
prkdb delete my-key --server http://127.0.0.1:8080
prkdb batch-put ./data.csv --server http://127.0.0.1:8080
```

### Collections

```bash
export PRKDB_ADMIN_TOKEN=change-me

prkdb collection create users
prkdb collection list
prkdb collection describe users
prkdb collection count users
prkdb collection sample users --limit 10
prkdb collection data users --limit 20 --offset 0 --filter "name=Alice"
prkdb collection drop users
```

### Consumers

```bash
export PRKDB_ADMIN_TOKEN=change-me

prkdb consumer list
prkdb consumer describe my-group
prkdb consumer lag --group my-group
prkdb consumer reset my-group --latest
```

### Partitions

```bash
export PRKDB_ADMIN_TOKEN=change-me

prkdb partition list
prkdb partition list --collection users
prkdb partition describe users 0
prkdb partition assignment --group my-group
prkdb partition rebalance my-group
```

### Replication

```bash
export PRKDB_ADMIN_TOKEN=change-me

prkdb replication status
prkdb replication nodes
prkdb replication lag
prkdb replication start ./replication.toml
```

### Metrics

```bash
prkdb metrics show
prkdb metrics partition --collection users
prkdb metrics consumer --group my-group
prkdb metrics reset
```

### Database

```bash
prkdb --database ./prkdb.db database info
prkdb --database ./prkdb.db database health
prkdb --database ./prkdb.db database compact
prkdb --database ./prkdb.db database backup ./backup.tar.gz
```

### Schema Registry

```bash
export PRKDB_ADMIN_TOKEN=change-me

prkdb schema register --server http://127.0.0.1:8080 --collection users --proto ./schemas/users.binpb
prkdb schema get --server http://127.0.0.1:8080 --collection users
prkdb schema list --server http://127.0.0.1:8080
prkdb schema check --server http://127.0.0.1:8080 --collection users --proto ./schemas/users-v2.binpb
```

## HTTP Server

Start the HTTP server with:

```bash
prkdb serve --host 127.0.0.1 --port 8080 --grpc-port 50051 --prometheus --websockets

# Required if clients connect through a different public address
prkdb serve \
  --host 0.0.0.0 \
  --port 8080 \
  --grpc-port 50051 \
  --advertised-grpc-address http://db.example.com:50051
```

The remote CLI defaults assume `prkdb-server` on `http://127.0.0.1:8080`. When you point admin or data commands at the local gRPC endpoint exposed by `prkdb serve`, use `--server http://127.0.0.1:50051`.

With `--cors`, the server enables a restricted CORS policy. You can override the allowed origins with `PRKDB_CORS_ORIGINS`.

If `PRKDB_WS_TOKEN` is set, `/ws/collections/:name` requires `Authorization: Bearer <token>`.

## HTTP Endpoints

- `GET /`
- `GET /health`
- `GET /collections`
- `GET /collections/:name`
- `GET /collections/:name/data`
- `PUT /collections/:name/data`
- `GET /collections/:name/data/:id`
- `DELETE /collections/:name/data/:id`
- `GET /collections/:name/count`
- `GET /collections/:name/schema`
- `GET /metrics` when `--prometheus` is enabled
- `WS /ws/collections/:name` when `--websockets` is enabled

## Notes

- Admin RPCs use `PRKDB_ADMIN_TOKEN`.
- Remote data commands talk to the gRPC API.
- Generated TypeScript and Python SDKs talk to the HTTP API.
