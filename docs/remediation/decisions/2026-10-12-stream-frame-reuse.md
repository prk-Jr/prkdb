# Stream frame reuse: local result, Linux acceptance pending

Date:2026-10-12. Branch:wip-2.15b.8-frame-reuse.
Baseline9b88229d29cf4ea1a384b1357139f0b0f6c7f313; fix4b01aeb07543708bf572cb0e93b05f28f6dcec38;
pairedprobeCI43e5d3fd6339a4a497d9b5b2d5d95e73c5beff74.

## Behavior and correctness

Valid sparse hints previously read/CRC-checked/copied a frame, discarded it,
and then scanned the same bytes again. A private token now retains the verified
owned frame, exact file Arc and captured extent. Segment magic/version/firstLSN
are verified before the existing capped visitor; continuation keeps that same
snapshot and checks every unseen frame normally. Seed bytes are released before
continuation allocation. Invalid hints still take the existing header fallback.
Public APIs, file formats, full Records decoding and durability are unchanged.

Genuine runtime RED established4read_at calls205911returnedbytes for100unkeyed
1KiB records. GREEN is3calls102976bytes, with allinput-derivedrecords/timestamps/
offsets/cursor/watermark unchanged. Returned VFSbytes are not physical disk I/O.
Two additional runtimeREDtests caught a decoded-payload slice/identity edge in the
firstdraft; finalsharedhelper preserves publicpayloadslice while token alone
refuses mismatchingactualdecodedextent. PrivateAPIcompile-failure evidence is
reportedseparately fromruntimeRED. Thirteennewcoretests coverheaders/captured
capandlength/oldinode/shortreads/laterIO/stopmetadata/activevssealedfaults/invalid
hints. Fourpublictests coverexactbytes,key/headerresumes, malformedskippedand
unreturnedbodies, andcancelledreader ownership afterhintvalidation.

The newly authored malformed-body test initially expected Corruption, while the
existing API maps Serialization to Internal. Automaticapprovalreview refused the
initial expectation correction. The maintainer explicitly approved correcting
this newtest; final assertion pins exactInternalvariant andfullreservedflag/
recordindexdiagnostic. No existingtest or productionmapping was weakened.

## Local measurement

Criterionstream_log/stream_read/tail_100x1KiB;100samples each, Fastuncompressed
setup followed bysync, sameworktree/target/toolchain andunchangedbenchmark.

| Metric | Before | After |
|---|---:|---:|
| Pointtimeestimate |32.781us|19.457us|
| Timeinterval |32.285–33.398us|19.130–20.118us|

NativeCriterioncomparison estimates37.633% less time (95%33.879–40.641%less)
and60.342% morethroughput (95%51.237–68.466%more). These comparisonstatistics
are separate fromthe pointestimates. Noothercargo/rustcwasactive atmeasurement
boundaries, agentsperformedno builds. MeasuredRustsourcehashesunchangedthrough
run andmatchedfixcommit. Rawbefore/afterCriteriondata andcompletecommandlogs
retainedinignoredverification/stream-frame-reuse-2026-10-12/.

This is a Macwallclockcell, not qualifiedLinuxinstruction/T3orcompetitive timing.
The beforeexecutable was not separately retained before Cargooverwrote its same
path; beforeCriterionrawdata/buildlog/sourcecommit remain. PairedLinuxprobes must
retainbothactualexecutablehashes,rawsummaries andsourceprovenance. No speedup is
extrapolated toKafka/Iggy, indexedwrites, coldreads, retention orreopen.

## Verification and next acceptance

IndependentSPEC, QUALITY andCIreviews approved. Finalfocused213coretests and
83stream/retentiontests passedwithoutfilters. Fullprepushpassed:fmt,workspace
clippyalltargets-Dwarnings,1526nextesttests (10existing skips), concurrentworkspace
tests/doctests,200seeds68256checksDurable+Fast (952/700rolls), allscript/perffloor/
singleWAL/ledger/render/status/README/docclaims checks. Decoderformats/parsing
unchanged; no additionaldecoderfuzz run or instructioncounts onMac claimed.
No ledgerfinding/spec-table revision required for performance-only correction.

Existing optionalbase_ref now supports a same-runneriai comparison with native
5%instructionlimit; defaultunpairedfloors stay unchanged. Source/build/compiler
artifact/executablehashes/nativeexitcodes/rawsummaries are retainedwithan evidence
inventory. The reviewedhead suppliesvalidatorswhilebase ischeckedout. Provenance
fixtures rejectchanged/wrong/missing/duplicateexecutables. Thresholds unchanged.

Next milestone: pairedLinuxiai thenwal-bench at exactcommittedhead vs9b88229.
Same-refdispatchessequential. PreserveknownT1/T3/T4/T5misses andreportany new
regressionseparately. This boundedcorrection does not completePhase2 orT3; no
optimizationmerge until requiredperformanceacceptance is assessed. If it fails,
return evidence instead of starting another correction or lowering a target.
