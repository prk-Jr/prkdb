# Production feasibility requirements: maintainer decisions

Status: proposed concrete requirements; no performance or shipment approval.
The maintainer selected BOTH streams and indexed data with independent gates.
Existing evaluation8eab751 is retained and concretized, not restarted.

Current source9b88229 cannot qualify broad production profiles: KEY-02,
TXN-01..04 and relevant consumer/offset findings/prerequisites remain unresolved.
Eligibility must precede timing. Correctness work has its own existing claims and
STOP decisions; this task does not take them over or waive their checks.

| Decision | Concrete proposal | Status |
| --- | --- | --- |
| Supported lanes | Streams AND indexed, independently assessed | approved |
| Source eligibility | Assembled regression/gate-qualified source; all shipping critical/high issues resolved; capabilities explicit | required, currently BLOCKED |
| Latency | Fast1/64KiB p99 10/20ms,p99.9 25/50ms; Durable25/50ms,75/150ms; actual maintenance has same bounds | pending maintainer |
| Read/lag | warm1KiB5/20ms,warm64KiB20/50ms,cold64KiB50/150ms; consumer lag from mode-specific acknowledgement100/250ms; durable-horizon timing separately observed | pending maintainer |
| Throughput | exact per-mode/value/batch stream and indexed-write minima in design; no competitive claim | pending maintainer |
| Host | dedicated x86_64 Linux; engine4CPUs/client4CPUs;16GiB engine RAM/localNVMe; exact model/device pinned | pending maintainer/availability |
| Memory/storage | engineRSS8GiB cap,<=256MiB last30min growth; indexed disk<=4xlive+2segments after maintenance | pending maintainer |
| Client/drain | stream append_timeout: proposed250msFast/750msDurable; indexed end-to-end profile BLOCKED pending reviewed API; original settlement bounds retained; maintenance drain30s | pending maintainer/source capability |
| Runtime | <=10h initial screens/calibration; <=125h capacity verification/sustained; <=2h bounded failure/recovery; stop lane on first failure | pending maintainer |
| Sustained/recovery | finite22-cell screen first; 12 ordinary cells, frozen100% capacity verification then hour tiers70/90/110%,3 repetitions,>=50k samples per ordinary class/repetition plus confidence; cold1/4/16GiB ready5/20/80s | pending maintainer |

All proposals need explicit review before measuring; absence of approval is not
consent. Numbers are requirements proposals, not measured ability. Old floors/
Fast loss guarantees remain unchanged. Any decision to change a requirement is a
new versioned product decision, never a reclassification of old failed rows.

Seven completed clean worktrees retired without force. Branches/exact heads,
report snapshots/notes/restore commands and original benchmark evidence retained
in main .agents/verification/worktree-cleanup-2026-10-13/. Paused/in-progress
worktrees preserved. No production/CI/benchmark change or new native run.
