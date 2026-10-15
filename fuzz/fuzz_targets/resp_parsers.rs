#![no_main]

use libfuzzer_sys::fuzz_target;
use ruvio::protocol::{parse_command, parse_hot_command};


fuzz_target!(|data: &[u8]| {
    let _ = parse_hot_command(data);
    let _ = parse_command(data);
});
