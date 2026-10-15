#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| ruvio::json_fuzz_ops(data));
