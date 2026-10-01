//! Checkpoints for fast WAL recovery.
//!
//! Empty until Task 2.14 writes an index snapshot here. The JSON checkpoint that lived here
//! stored per-segment offsets and let recovery skip records below them, which is what lost
//! every key written before a checkpoint (STO-01); it stopped being written or read in Task
//! 2.8a and was deleted in Task 2.8c. Until 2.14, recovery replays the whole log.
