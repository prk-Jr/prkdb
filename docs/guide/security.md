# Authorization, TLS, and Operations

Everything on this page is verified against the shipped binaries. Where a flag or endpoint
is named, `prkdb-cli serve --help` accepts it and a test exercises it.

## The server refuses to start unauthorized

Neither binary starts without either a credential or an explicit opt-out:

```
No principals are configured. Set PRKDB_BOOTSTRAP_TOKEN to create an admin
principal, or pass --allow-anonymous to serve without authorization
(development only).
```

| | `prkdb-cli serve` | `prkdb-server` |
|---|---|---|
| create an admin | `PRKDB_BOOTSTRAP_TOKEN=…` | `PRKDB_BOOTSTRAP_TOKEN=…` |
| opt out | `--allow-anonymous` | `PRKDB_ALLOW_ANONYMOUS=1` |

Refusing is the point. Every `/collections` route reads and writes your data; a server
that starts unauthenticated by default is one that reaches production that way by
accident.

`--allow-anonymous` warns at startup and means exactly what it says: every collection is
readable and writable by anyone who can reach the port.

## Principals, roles, and grants

A **principal** is an identity holding one credential and a set of **grants**. A grant is a
collection pattern plus a permission, ordered `Read < Write < Admin`. Grants union — any
one of them granting sufficient authority is enough — and there is no deny rule, so
ordering cannot matter.

Only the SHA-256 of a credential is stored. Comparison is constant-time, so a wrong
credential does not leak its correct prefix through response timing.

### Bootstrapping

`PRKDB_BOOTSTRAP_TOKEN` mints a single principal with `Admin` on `*` on first start. It is
**ignored once any principal exists**, so a restart cannot quietly create a second way in.

### Managing principals at runtime

```bash
# List (never returns credentials or their digests)
curl -H "Authorization: Bearer $ADMIN" http://127.0.0.1:8080/admin/principals

# Create or replace
curl -X PUT -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"reporting","credential":"…","grants":[{"collection":"events","permission":"read"}]}' \
  http://127.0.0.1:8080/admin/principals

# Revoke
curl -X DELETE -H "Authorization: Bearer $ADMIN" \
  http://127.0.0.1:8080/admin/principals/reporting
```

All three require `Admin`. **The last remaining admin cannot be revoked** — a cluster with
no administrator cannot be administered again without stopping it and editing storage by
hand, which you would be doing during an incident.

Every mutation is written to the audit log with the acting principal, the operation, the
target, and the outcome — including refusals. Credentials and digests are never logged.

### On a cluster

Principal changes go through Raft, proposed to **partition 0**, which owns the
authorization keyspace. Applying one updates the durable copy *and* the in-memory map that
authentication reads, on every node, in log order.

If partition 0 has no leader, the change is **refused** rather than written locally. A
local write would succeed on one node and leave the others disagreeing about who may do
what — and a revoke reported as done while the credential still works elsewhere is worse
than an error.

Single-node deployments (no `--peers`) have no Raft and write locally. That is the only
path there, not a fallback.

## Clients

Every client sends the credential as `Authorization: Bearer <credential>` and distinguishes
`401` (unknown or missing) from `403` (known but insufficient).

```bash
# CLI — --credential, or PRKDB_CREDENTIAL
prkdb-cli --credential "$TOKEN" schema --server http://127.0.0.1:50051 list

# --admin-token also works, since an admin token is Admin on *
prkdb-cli --admin-token "$TOKEN" collection list
```

The credential must be supplied **at connection time**, not after. Client construction
fetches cluster metadata, and metadata requires `Read`, so a credential applied later
arrives after the call that needed it and the connection fails while reporting a network
problem.

Generated Python, TypeScript, and Go clients take the credential in their constructor.

## TLS

```bash
prkdb-cli serve --port 8080 --grpc-port 50051 \
  --tls-cert server.pem --tls-key server-key.pem \
  --tls-client-ca cluster-ca.pem
```

| flag | effect |
|---|---|
| `--tls-cert` + `--tls-key` | HTTPS and gRPC-over-TLS. Both required together. |
| `--tls-client-ca` | requires clients to present a certificate signed by it (mTLS) |

`--tls-client-ca` is also how Raft peers authenticate to each other. A multi-node cluster
refuses to start without either it or `PRKDB_CLUSTER_SECRET`, because Raft RPCs can rewrite
the log — `--allow-unauthenticated-peers` overrides that, for development only.

Peers dial each other with the same scheme they listen on, so a TLS cluster forms over TLS
rather than failing the handshake.

## Operations

### Probes

| endpoint | meaning |
|---|---|
| `/livez` | the process is up. Touches nothing. |
| `/readyz` | WAL replay finished, a leader is known, **and the write path is confirming writes**. Names the unmet condition on `503`. |
| `/health` | legacy combined check; public so orchestrators can probe before any credential exists. `503` when the write path is unhealthy. |

Kubernetes needs both: without `/readyz`, traffic routes to nodes that will fail.

#### Write-path health

The WAL writer publishes queued writes in the background. If it exits or stops making
progress, writes are accepted and never confirmed — the process stays up and answers HTTP
while every client write hangs. `/livez` will keep saying the process is alive, correctly,
which is why liveness alone is not enough to route on.

`/readyz` returns `503` in that state. It is a *stop routing here* condition, not a restart
condition: restarting loses the queued writes.

```json
// GET /readyz — 503
{
  "status": "not_ready",
  "reason": "the storage write path is not confirming writes",
  "detail": "WAL writer stalled: 3 write(s) queued, oldest unpublished for 800ms with no publication progress (threshold 800ms)",
  "queue_depth": 3,
  "oldest_unpublished_age_ms": 800
}
```

`/health` carries the same facts as a `write_path` block on every response, healthy or not,
so it can be scraped for a trend rather than only alerted on:

```json
// GET /health — 200 when healthy, 503 when not
{
  "status": "healthy",
  "write_path": {
    "healthy": true,
    "reason": null,
    "queue_depth": 0,
    "oldest_unpublished_age_ms": 0,
    "last_publish_age_ms": 12
  }
}
```

A rising `queue_depth` with a rising `oldest_unpublished_age_ms` is a writer falling behind.
A `last_publish_age_ms` that keeps growing while `queue_depth` is non-zero is a writer that
has stopped publishing altogether — that is what trips `healthy: false`.

The stall threshold derives from the configured flush interval, so a deployment that
batches less aggressively is not flagged for it.

These counters are **not** on `/metrics`. `/metrics` exports Raft, partition, and consumer
series; storage write-path health is only on the probe endpoints today. Alert on `/health`
returning `503`, not on a Prometheus series that does not exist.

### Rate limiting

```bash
prkdb-cli serve --rate-limit 1000
```

Sheds excess requests on both surfaces — HTTP with `429` and `Retry-After`, gRPC with
`ResourceExhausted`. Two exemptions:

- **probe endpoints and gRPC `Health`** — shedding a liveness check gets the node killed
  under exactly the load where it most needs to stay up
- **Raft peer RPCs** — rate-limiting `AppendEntries` makes a busy leader look unreachable
  to its followers, which is self-inflicted failover

### Backups

```bash
prkdb-cli backup --output snapshot.bin
prkdb-cli restore --input snapshot.bin --data-dir ./restored
```

`backup` writes a sidecar manifest recording length, SHA-256, entry count, and format
version. `restore` verifies both **before writing anything** — a corrupt archive restored
into an empty directory and reported as success is the worst outcome a backup tool has,
because it is discovered only when the backup is needed.

A missing manifest is a warning (archives predating manifests still restore). A manifest
that disagrees with its archive is fatal. `--skip-verify` exists for salvage.

## The deprecated `admin_token`

`PRKDB_ADMIN_TOKEN` predates this model. It is still accepted for one release and is
treated as `Admin` on `*`. Prefer `PRKDB_BOOTSTRAP_TOKEN` plus per-principal grants: a
single shared secret cannot be scoped, attributed in an audit log, or revoked for one
consumer.

## Complete `serve` reference

Checked against the binary by `scripts/check_docs_cover_cli.sh`, which fails the build if a
flag exists here and not in `serve --help`, or the reverse.

| flag | default | purpose |
|---|---|---|
| `--host <HOST>` | `127.0.0.1` | interface to bind |
| `--port <PORT>` | `8080` | HTTP port |
| `--grpc-port <PORT>` | `50051` | gRPC port, for clients and Raft peers |
| `--allow-anonymous` | off | serve without authorization. Development only. |
| `--allow-unauthenticated-peers` | off | form a multi-node cluster without peer authentication. Development only — Raft RPCs can rewrite the log. |
| `--rate-limit <N>` | none | shed above N requests/second; probes exempt |
| `--tls-cert <PATH>` | none | PEM certificate; enables TLS on both surfaces. Requires `--tls-key`. |
| `--tls-key <PATH>` | none | PEM private key matching `--tls-cert` |
| `--tls-client-ca <PATH>` | none | require client certificates signed by this CA (mTLS); also how Raft peers authenticate |
| `--prometheus` | off | expose `/metrics` in Prometheus format |
| `--cors` | off | permit cross-origin requests, for browser dashboards |
| `--websockets` | off | enable `/ws/collections/:name` for live updates |
| `--id <N>` | `1` | this node's Raft id |
| `--peers <LIST>` | none | `id=host:port,…`. **Presence of this flag is what makes the node clustered** — without it there is no Raft and writes are local. |
| `--num-partitions <N>` | `16` | Raft partitions to create |
| `--advertised-grpc-address <ADDR>` | listen address | address given to clients and peers, when it differs from what this node binds (NAT, containers) |

Global flags, before the subcommand:

| flag | environment | purpose |
|---|---|---|
| `--database <PATH>` | | data directory |
| `--credential <TOKEN>` | `PRKDB_CREDENTIAL` | bearer credential sent with every request |
| `--admin-token <TOKEN>` | `PRKDB_ADMIN_TOKEN` | deprecated; treated as `Admin` on `*` |
| `--server <URL>` | `PRKDB_SERVER` | gRPC address to connect to |
| `--format <FMT>` | | `table`, `json`, or `yaml` |

## HTTP endpoints

| route | auth | notes |
|---|---|---|
| `GET /health` | public | orchestrators probe before any credential exists; `503` when the write path is unhealthy |
| `GET /livez` | public | process liveness only |
| `GET /readyz` | public | WAL replayed, leader known, write path confirming; `503` names the unmet condition |
| `GET /metrics` | credential | Prometheus format, with `--prometheus` |
| `GET /collections` | `Read` | narrowed to what the caller's grants permit |
| `GET /collections/:name` | `Read` | |
| `GET /collections/:name/data` | `Read` | |
| `GET /collections/:name/count` | `Read` | |
| `GET /collections/:name/schema` | `Read` | |
| `PUT /collections/:name/data` | `Write` | |
| `DELETE /collections/:name/data/:id` | `Write` | |
| `GET /admin/principals` | `Admin` | never returns credentials or digests |
| `PUT /admin/principals` | `Admin` | |
| `DELETE /admin/principals/:name` | `Admin` | refuses to remove the last admin |
| `GET /ws/collections/:name` | credential | with `--websockets` |
