# wal_write_path_spike (fixture: fabricated numbers for scripts/wal_fast_rule.py --self-test)
#
# This is NOT a captured bench run. It is a synthetic, minimal raw-output fixture that
# matches `wal_write_path_spike.rs`'s `print_header`/`print_row` table shape closely
# enough for `scripts/wal_fast_rule.py` to parse, built so that:
#   - the 1-writer cell is a clean PASS (fast loses ~9%, under the 15% rule), and
#   - the 8-writer cell is a deliberate LOSS (fast loses ~25%, over the 15% rule),
# so the self-test exercises both verdict branches and the parser's column handling
# (extra trailing columns, an unrelated `current_mmap_wal` kind that must not be
# paired). Real Linux probe output goes in the decision record (§8), not here.
- warm-up 1000 ms, measure 3000 ms, reps 1, tokio worker threads = 8
- load average at start: 0.50 0.40 0.30

| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | avg batch | max batch | writer idle % | writer write % | writer sync % | writer CPU % | load1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| single_log_fast/1w/1k | 1 | 1 KiB | 910 | 0.9 | 100.0 | 150.0 | 200.0 | 1.0 | 1 | 50 | 40 | 0 | 40 | 0.50 0.40 0.30 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 1000 | 1.0 | 90.0 | 140.0 | 190.0 | — | — | — | — | — | — | 0.50 0.40 0.30 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 995 | 1.0 | 89.0 | 138.0 | 188.0 | — | — | — | — | — | — | 0.50 0.40 0.30 |
| single_log_fast/8w/1k | 8 | 1 KiB | 1275 | 1.3 | 250.0 | 400.0 | 900.0 | 3.0 | 8 | 10 | 60 | 0 | 60 | 0.60 0.45 0.32 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 1700 | 1.7 | 200.0 | 350.0 | 800.0 | — | — | — | — | — | — | 0.60 0.45 0.32 |
