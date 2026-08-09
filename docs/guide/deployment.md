# PrkDB Deployment Guide

This guide reflects the current `prkdb-server` binary and the current `prkdb-cli` verification workflow.

## Architecture

- Inter-node Raft traffic uses the addresses in `CLUSTER_NODES`.
- Client gRPC traffic is multiplexed on the same address and port as the local node entry in `CLUSTER_NODES`.
- `GRPC_PORT` is optional. When set, it must match the local node's `CLUSTER_NODES` port.
- Use `PRKDB_ADVERTISED_GRPC_ADDR` when the bind address differs from the dialable client address.
- Use `PRKDB_ADVERTISED_NODE_ADDRS` when peer nodes also need explicit dialable client addresses in metadata, for example `2=http://db-2.example.com:8081,3=http://db-3.example.com:8082`.
- Metrics bind to `127.0.0.1:(9090 + NODE_ID)` by default. Set `PRKDB_METRICS_ADDR` to override or `PRKDB_DISABLE_METRICS=1` to disable them.
- Schema registry data is persisted under `${STORAGE_PATH}/schemas`.

## Build

```bash
cargo build --release --bin prkdb-server --bin prkdb-cli
cp target/release/prkdb-server /usr/local/bin/
cp target/release/prkdb-cli /usr/local/bin/prkdb
```

## Example 3-Node Cluster

### Node addresses

- Node 1 address: `10.0.0.1:8080`
- Node 2 address: `10.0.0.2:8081`
- Node 3 address: `10.0.0.3:8082`

### Systemd unit

Create `/etc/systemd/system/prkdb.service` on each node.

```ini
[Unit]
Description=PrkDB Server
After=network.target

[Service]
Type=simple
User=prkdb
WorkingDirectory=/var/lib/prkdb
Environment=NODE_ID=1
Environment=CLUSTER_NODES=1@10.0.0.1:8080,2@10.0.0.2:8081,3@10.0.0.3:8082
Environment=STORAGE_PATH=/var/lib/prkdb/node1
Environment=PRKDB_ADMIN_TOKEN=change-me
Environment=PRKDB_ADVERTISED_GRPC_ADDR=http://db-1.example.com:8080
Environment=PRKDB_ADVERTISED_NODE_ADDRS=2=http://db-2.example.com:8081,3=http://db-3.example.com:8082
ExecStart=/usr/local/bin/prkdb-server
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

For node 2 and node 3, change:

- `NODE_ID`
- `STORAGE_PATH`
- the local address inside `CLUSTER_NODES`

## Start the Cluster

```bash
sudo systemctl daemon-reload
sudo systemctl enable prkdb
sudo systemctl start prkdb
```

## Verify the Deployment

### Check metrics

On node 1:

```bash
curl http://127.0.0.1:9091/metrics | grep prkdb_up
```

On node 2:

```bash
curl http://127.0.0.1:9092/metrics | grep prkdb_up
```

### Check the gRPC API

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb --server http://127.0.0.1:8080 collection list
```

### Check schema registry persistence

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb schema list --server http://127.0.0.1:8080
```

## Backup and Restore

`prkdb-cli backup` operates **offline**, directly on a data directory — it does not talk to
a running server. Point it at a stopped node, or at a copy of the directory.

```bash
prkdb-cli --database /var/lib/prkdb backup --output /backups/prkdb-$(date +%F).bin
```

This writes two files: the archive, and a `.manifest` sidecar recording the archive's
length, SHA-256, entry count, and format version.

```bash
prkdb-cli restore --input /backups/prkdb-2026-08-09.bin --data-dir /var/lib/prkdb-restored
```

`restore` verifies the archive against its manifest **before writing anything**, so a
corrupt archive fails without leaving a half-populated target. Keep the two files
together; a missing manifest downgrades to a warning so older archives still restore, but
then nothing is checked.

- `--force` — restore into a non-empty directory. Without it, restore refuses rather than
  merging two databases.
- `--skip-verify` — restore despite a manifest mismatch. This is for salvaging what is
  readable from a damaged archive, not for silencing the check. A mismatch means the
  archive is not the one that was backed up.

### Verify the backup, not just the exit code

A backup nobody has restored is not a backup. Restore into a scratch directory on a
schedule and check a known key:

```bash
prkdb-cli restore --input "$ARCHIVE" --data-dir /tmp/verify-$$ && rm -rf /tmp/verify-$$
```

### Scheduling

Scheduling belongs to the operator, not to the database — PrkDB deliberately ships no
internal scheduler, so backups follow the same operational controls as everything else you
run (alerting on failure, retention, offsite copies).

A systemd timer, for a node whose data directory is `/var/lib/prkdb`:

```ini
# /etc/systemd/system/prkdb-backup.service
[Unit]
Description=PrkDB backup
# Back up a stopped node or a snapshot of its directory. Backing up a directory that is
# being written to captures an inconsistent point in time.

[Service]
Type=oneshot
ExecStart=/usr/local/bin/prkdb-cli --database /var/lib/prkdb \
    backup --output /backups/prkdb-%%i.bin
```

```ini
# /etc/systemd/system/prkdb-backup.timer
[Unit]
Description=Nightly PrkDB backup

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

The cron equivalent, with retention:

```cron
# Nightly at 02:30; keep 14 days.
30 2 * * * prkdb-cli --database /var/lib/prkdb backup \
    --output /backups/prkdb-$(date +\%%F).bin >> /var/log/prkdb-backup.log 2>&1
15 3 * * * find /backups -name 'prkdb-*.bin*' -mtime +14 -delete
```

Note the `*` in the retention glob: it removes each archive's `.manifest` alongside it.
Deleting archives while leaving manifests behind accumulates files that describe nothing.

## Operational Notes

- `CLUSTER_NODES` should contain every node in the cluster, including the local node.
- Smart clients consume the addresses returned by metadata. Do not advertise `0.0.0.0`; set `PRKDB_ADVERTISED_GRPC_ADDR` if clients connect through DNS or a load balancer.
- If peer nodes have different bind and public addresses, configure `PRKDB_ADVERTISED_NODE_ADDRS` so metadata never falls back to an internal-only socket.
- `PRKDB_ADMIN_TOKEN` protects admin RPCs such as collection management and schema registration.
- If you expose the HTTP server from `prkdb-cli serve`, restrict CORS origins explicitly with `PRKDB_CORS_ORIGINS`.
- WebSocket auth is header-based. Set `PRKDB_WS_TOKEN` when you want bearer-token enforcement for `/ws/collections/:name`.

## Security Checklist

- Run the cluster behind TLS termination or a private network boundary.
- Keep `PRKDB_ADMIN_TOKEN` and `PRKDB_WS_TOKEN` out of shell history and process listings where possible.
- Persist `STORAGE_PATH` on durable local disks.
- Scrape metrics from the node-local metrics bind address instead of exposing it publicly.
