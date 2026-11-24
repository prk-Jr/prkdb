# Offline perf fixtures

Byte-for-byte recorder-only files from public diagnostic artifacts, not database
performance runs. `provenance.json` records their exact original hashes, sizes,
workflow IDs and independent native/sample identities. Linux6.17 expected semantic
summaries are projections of the previously accepted trace-quality records.

Linux7's dynamic write payload includes literal newlines; it is deliberately
unmodified. Tests alter only in-memory copies for malformed/adversarial cases.
No historical commit object, GitHub access, Linux perf installation or capture is
required to run these fixtures on a clean checkout.
