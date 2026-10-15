use crate::protocol::HotCommand;

/// Default count of numbered logical databases, as in Redis `databases` and Garnet `MaxDatabases`.
pub const DEFAULT_DATABASES: usize = 16;

/// Database numbers travel as one byte in WAL records and snapshot entries.
pub const MAX_DATABASES: usize = 256;

/// True when the hot GET/SET/INCR parser may serve the command. Hot keys live in database 0.
pub fn hot_path_allowed(db: u8, _command: &HotCommand<'_>) -> bool {
    db == 0
}

/// Side-table identity of a key: the database byte followed by the user key.
/// Stored keys never carry it; only waiter and tracking tables do.
pub fn tagged(database: u8, key: &[u8]) -> Vec<u8> {
    let mut tagged = Vec::with_capacity(1 + key.len());

    tagged.push(database);
    tagged.extend_from_slice(key);
    tagged
}

/// Database and user key of a [`tagged`] identity.
pub fn untagged(tagged: &[u8]) -> (u8, &[u8]) {
    match tagged.split_first() {
        Some((database, key)) => (*database, key),
        None => (0, tagged),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_identities_round_trip_and_keep_databases_apart() {
        let zero = tagged(0, b"k");
        let one = tagged(1, b"k");

        assert_ne!(zero, one);
        assert_eq!(untagged(&zero), (0, &b"k"[..]));
        assert_eq!(untagged(&one), (1, &b"k"[..]));
        assert_eq!(untagged(&tagged(0, &[1, b'k'])), (0, &[1, b'k'][..]));
    }
}
