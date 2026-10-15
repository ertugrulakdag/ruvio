//! License texts linked into the release binary.

pub const THIRD_PARTY_NOTICES: &str = include_str!("../licenses/THIRD_PARTY_NOTICES");

#[cfg(test)]
mod tests {
    use super::THIRD_PARTY_NOTICES;

    #[test]
    fn the_binary_notice_keeps_ruvio_and_vendored_hashbrown() {
        assert!(THIRD_PARTY_NOTICES.contains("Copyright (c) 2026 Salih Cantekin"));
        assert!(THIRD_PARTY_NOTICES.contains("GNU AFFERO GENERAL PUBLIC LICENSE"));
        assert!(THIRD_PARTY_NOTICES.contains("vendored src/hashbrown"));
        assert!(THIRD_PARTY_NOTICES.contains("Apache License"));
        assert!(THIRD_PARTY_NOTICES.contains("Permission is hereby granted"));
    }
}
