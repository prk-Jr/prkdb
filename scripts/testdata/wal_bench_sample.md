# wal_fast_rule.py self-test fixture: raw wal_write_path_spike rows. Expected verdicts are asserted by --self-test.
- warm-up 1000 ms, measure 3000 ms, reps 2, tokio worker threads = 8
- load average at start: 2.85 2.10 1.90
- disk ceiling, pwrite 1 MiB, no sync: 1555 MB/s (1483 writes/s)
| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | avg batch | max batch | writer idle % | writer write % | writer sync % | writer CPU % | load1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| wal_fast/1w/1k | 1 | 1 KiB | 80000 | 81.9 | 10.4 | 17.3 | 853.6 | 1.0 | 1 | 52 | 39 | 0 | 39 | 7.28 6.10 5.02 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 90000 | 92.2 | 11.3 | 5067.5 | 7758.0 |  |  |  |  |  |  | 7.28 6.10 5.02 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 1062 | 1.1 | 16.3 | 5443.3 | 12118.0 |  |  |  |  |  |  | 6.94 6.00 5.00 |
| wal_fast/8w/1k | 8 | 1 KiB | 284392 | 291.2 | 15.8 | 49.2 | 2800.1 | 3.0 | 8 | 15 | 73 | 0 | 51 | 5.92 5.50 5.00 |
| current_mmap_wal/8w/1k | 8 | 1 KiB | 1605 | 1.6 | 5012.2 | 8890.7 | 16516.8 |  |  |  |  |  |  | 5.53 5.40 5.00 |
| wal_fast/1w/64k | 1 | 64 KiB | 400 | 26.2 | 152.7 | 2405.8 | 3224.4 | 1.0 | 1 | 66 | 31 | 0 | 8 | 3.44 3.40 3.30 |
| current_mmap_wal/1w/64k | 1 | 64 KiB | 522 | 34.2 | 1905.1 | 4106.3 | 4487.0 |  |  |  |  |  |  | 3.33 3.30 3.20 |
| wal_fast/1w/1k | 1 | 1 KiB | 76000 | 77.8 | 10.6 | 18.0 | 900.0 | 1.0 | 1 | 50 | 40 | 0 | 40 | 3.10 3.00 2.90 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 90000 | 92.2 | 11.2 | 5000.0 | 7700.0 |  |  |  |  |  |  | 3.10 3.00 2.90 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 1062 | 1.1 | 16.0 | 5400.0 | 12000.0 |  |  |  |  |  |  | 3.00 3.00 2.90 |
