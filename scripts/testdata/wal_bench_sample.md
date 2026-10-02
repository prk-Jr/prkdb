# wal_fast_rule.py self-test fixture: raw wal_write_path rows (2 reps, every cell of the grid). Expected verdicts are asserted by --self-test; the failure cases are derived from it there.
- warm-up 1000 ms, measure 3000 ms, reps 2, tokio worker threads = 4
- load average at start: 2.85 2.10 1.90
- disk ceiling, pwrite 1 MiB, no sync: 1555 MB/s (1483 writes/s)
| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | load1 |
|---|---|---|---|---|---|---|---|---|
| adapter_put/1w/1k | 1 | 1 KiB | 27200 | 27.9 | 20.3 | 61.0 | 91.5 | 1.50 1.40 1.30 |
- compaction_concurrent_put/1w/1k: 12 compaction runs during the cell
| compaction_concurrent_put/1w/1k | 1 | 1 KiB | 15000 | 15.4 | 966.7 | 2900.0 | 4350.0 | 1.50 1.40 1.30 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 26000 | 26.6 | 23.3 | 70.0 | 105.0 | 1.50 1.40 1.30 |
| wal_fast/1w/1k | 1 | 1 KiB | 80000 | 81.9 | 5.8 | 17.3 | 26.0 | 1.50 1.40 1.30 |
| adapter_put/8w/1k | 8 | 1 KiB | 182632 | 187.0 | 225.4 | 676.2 | 1014.3 | 1.50 1.40 1.30 |
- compaction_concurrent_put/8w/1k: 12 compaction runs during the cell
| compaction_concurrent_put/8w/1k | 8 | 1 KiB | 120000 | 122.9 | 1366.7 | 4100.0 | 6150.0 | 1.50 1.40 1.30 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 180000 | 184.3 | 233.3 | 700.0 | 1050.0 | 1.50 1.40 1.30 |
| wal_fast/8w/1k | 8 | 1 KiB | 284392 | 291.2 | 16.4 | 49.2 | 73.8 | 1.50 1.40 1.30 |
| adapter_put/64w/1k | 64 | 1 KiB | 256900 | 263.1 | 521.3 | 1563.9 | 2345.9 | 1.50 1.40 1.30 |
- compaction_concurrent_put/64w/1k: 12 compaction runs during the cell
| compaction_concurrent_put/64w/1k | 64 | 1 KiB | 200000 | 204.8 | 3266.7 | 9800.0 | 14700.0 | 1.50 1.40 1.30 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 250000 | 256.0 | 533.3 | 1600.0 | 2400.0 | 1.50 1.40 1.30 |
| wal_fast/64w/1k | 64 | 1 KiB | 400000 | 409.6 | 53.3 | 160.0 | 240.0 | 1.50 1.40 1.30 |
| adapter_put/1w/64k | 1 | 64 KiB | 3940 | 258.2 | 547.6 | 1642.9 | 2464.4 | 1.50 1.40 1.30 |
- compaction_concurrent_put/1w/64k: 12 compaction runs during the cell
| compaction_concurrent_put/1w/64k | 1 | 64 KiB | 3000 | 196.6 | 1733.3 | 5200.0 | 7800.0 | 1.50 1.40 1.30 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 3900 | 255.6 | 566.7 | 1700.0 | 2550.0 | 1.50 1.40 1.30 |
| wal_fast/1w/64k | 1 | 64 KiB | 400 | 26.2 | 801.9 | 2405.8 | 3608.7 | 1.50 1.40 1.30 |
| adapter_put/8w/64k | 8 | 64 KiB | 6053 | 396.7 | 926.7 | 2780.0 | 4170.0 | 1.50 1.40 1.30 |
- compaction_concurrent_put/8w/64k: 12 compaction runs during the cell
| compaction_concurrent_put/8w/64k | 8 | 64 KiB | 5000 | 327.7 | 2933.3 | 8800.0 | 13200.0 | 1.50 1.40 1.30 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 6000 | 393.2 | 933.3 | 2800.0 | 4200.0 | 1.50 1.40 1.30 |
| wal_fast/8w/64k | 8 | 64 KiB | 6300 | 412.9 | 8000.0 | 24000.0 | 36000.0 | 1.50 1.40 1.30 |
| adapter_put/64w/64k | 64 | 64 KiB | 6054 | 396.8 | 12118.3 | 36354.9 | 54532.4 | 1.50 1.40 1.30 |
- compaction_concurrent_put/64w/64k: 12 compaction runs during the cell
| compaction_concurrent_put/64w/64k | 64 | 64 KiB | 5000 | 327.7 | 30000.0 | 90000.0 | 135000.0 | 1.50 1.40 1.30 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 6000 | 393.2 | 13333.3 | 40000.0 | 60000.0 | 1.50 1.40 1.30 |
| wal_fast/64w/64k | 64 | 64 KiB | 6300 | 412.9 | 17333.3 | 52000.0 | 78000.0 | 1.50 1.40 1.30 |
| adapter_put/1w/1k | 1 | 1 KiB | 27000 | 27.6 | 20.8 | 62.3 | 93.4 | 1.50 1.40 1.30 |
- compaction_concurrent_put/1w/1k: 12 compaction runs during the cell
| compaction_concurrent_put/1w/1k | 1 | 1 KiB | 15500 | 15.9 | 1016.7 | 3050.0 | 4575.0 | 1.50 1.40 1.30 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 26500 | 27.1 | 23.7 | 71.0 | 106.5 | 1.50 1.40 1.30 |
| wal_fast/1w/1k | 1 | 1 KiB | 76000 | 77.8 | 6.0 | 18.0 | 27.0 | 1.50 1.40 1.30 |
| adapter_put/8w/1k | 8 | 1 KiB | 189466 | 194.0 | 221.3 | 663.8 | 995.7 | 1.50 1.40 1.30 |
- compaction_concurrent_put/8w/1k: 12 compaction runs during the cell
| compaction_concurrent_put/8w/1k | 8 | 1 KiB | 121000 | 123.9 | 1433.3 | 4300.0 | 6450.0 | 1.50 1.40 1.30 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 181000 | 185.3 | 230.0 | 690.0 | 1035.0 | 1.50 1.40 1.30 |
| wal_fast/8w/1k | 8 | 1 KiB | 284392 | 291.2 | 16.7 | 50.0 | 75.0 | 1.50 1.40 1.30 |
| adapter_put/64w/1k | 64 | 1 KiB | 258278 | 264.5 | 556.4 | 1669.2 | 2503.8 | 1.50 1.40 1.30 |
- compaction_concurrent_put/64w/1k: 12 compaction runs during the cell
| compaction_concurrent_put/64w/1k | 64 | 1 KiB | 201000 | 205.8 | 3300.0 | 9900.0 | 14850.0 | 1.50 1.40 1.30 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 251000 | 257.0 | 550.0 | 1650.0 | 2475.0 | 1.50 1.40 1.30 |
| wal_fast/64w/1k | 64 | 1 KiB | 400000 | 409.6 | 52.7 | 158.0 | 237.0 | 1.50 1.40 1.30 |
| adapter_put/1w/64k | 1 | 64 KiB | 3975 | 260.5 | 533.8 | 1601.4 | 2402.1 | 1.50 1.40 1.30 |
- compaction_concurrent_put/1w/64k: 12 compaction runs during the cell
| compaction_concurrent_put/1w/64k | 1 | 64 KiB | 3100 | 203.2 | 1800.0 | 5400.0 | 8100.0 | 1.50 1.40 1.30 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 3950 | 258.9 | 550.0 | 1650.0 | 2475.0 | 1.50 1.40 1.30 |
| wal_fast/1w/64k | 1 | 64 KiB | 400 | 26.2 | 800.0 | 2400.0 | 3600.0 | 1.50 1.40 1.30 |
| adapter_put/8w/64k | 8 | 64 KiB | 6081 | 398.5 | 897.4 | 2692.3 | 4038.5 | 1.50 1.40 1.30 |
- compaction_concurrent_put/8w/64k: 12 compaction runs during the cell
| compaction_concurrent_put/8w/64k | 8 | 64 KiB | 5050 | 331.0 | 3033.3 | 9100.0 | 13650.0 | 1.50 1.40 1.30 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 6050 | 396.5 | 916.7 | 2750.0 | 4125.0 | 1.50 1.40 1.30 |
| wal_fast/8w/64k | 8 | 64 KiB | 6300 | 412.9 | 8166.7 | 24500.0 | 36750.0 | 1.50 1.40 1.30 |
| adapter_put/64w/64k | 64 | 64 KiB | 6061 | 397.2 | 15449.3 | 46347.9 | 69521.9 | 1.50 1.40 1.30 |
- compaction_concurrent_put/64w/64k: 12 compaction runs during the cell
| compaction_concurrent_put/64w/64k | 64 | 64 KiB | 5010 | 328.3 | 31666.7 | 95000.0 | 142500.0 | 1.50 1.40 1.30 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 6010 | 393.9 | 13666.7 | 41000.0 | 61500.0 | 1.50 1.40 1.30 |
| wal_fast/64w/64k | 64 | 64 KiB | 6300 | 412.9 | 17000.0 | 51000.0 | 76500.0 | 1.50 1.40 1.30 |
