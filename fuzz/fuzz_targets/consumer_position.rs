#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| prkdb_verify::fuzz_entry::consumer_position(data));
