#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| prkdb_verify::fuzz_entry::frame_decode(data));
