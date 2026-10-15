//! Shard side of the JSON field indexes (see `crate::json_index`): registry, hooks that keep
//! the postings in step with the keyspace, the bounded build scan and the lookup.

use super::*;
use crate::json_index::{Definition, IndexState, JsonIndexes, Scalar};

/// Index definitions live in database 0 under this prefix, so the WAL, snapshots and
/// replication carry them like any other key.
pub(crate) const DEFINITION_PREFIX: &[u8] = b"\0ruvio\0jsonidx\0";

pub(crate) fn definition_key(name: &str) -> Vec<u8> {
    let mut key = DEFINITION_PREFIX.to_vec();

    key.extend_from_slice(name.as_bytes());
    key
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FindError {
    NoSuchIndex,
    Building,
    BadValue,
}

impl<C: Clock, I: StorageIndex> KvShard<C, I> {
    /// Registers an index. A shard with no keys has nothing to build.
    pub(crate) fn json_index_create(&mut self, definition: Definition) -> bool {
        let empty = self.databases_are_empty();
        let indexes = self.json_indexes.get_or_insert_with(Box::default);

        if empty {
            indexes.create_ready(definition)
        } else {
            indexes.create(definition)
        }
    }

    /// A full copy of the named index, so a rolled-back `DROP` brings it back ready.
    pub(crate) fn json_index_copy(&self, name: &str) -> Option<crate::json_index::Index> {
        self.json_indexes.as_ref()?.copy_of(name)
    }

    pub(crate) fn json_index_restore(&mut self, index: crate::json_index::Index) -> bool {
        self.json_indexes
            .get_or_insert_with(Box::default)
            .restore(index)
    }

    pub(crate) fn json_index_drop(&mut self, name: &str) -> bool {
        let Some(indexes) = self.json_indexes.as_mut() else {
            return false;
        };
        let dropped = indexes.drop_index(name);

        if indexes.is_empty() {
            self.json_indexes = None;
        }

        dropped
    }

    pub(crate) fn json_index_list(&self) -> Vec<(Definition, IndexState, usize, usize)> {
        self.json_indexes.as_ref().map_or_else(Vec::new, |indexes| {
            indexes
                .definitions()
                .map(|(definition, state, entries, bytes)| {
                    (definition.clone(), state, entries, bytes)
                })
                .collect()
        })
    }

    fn databases_are_empty(&self) -> bool {
        self.database_ids().iter().all(|database| {
            if *database == self.active {
                self.entries.len() == 0
            } else {
                self.databases
                    .get(usize::from(*database))
                    .and_then(Option::as_ref)
                    .is_none_or(|map| map.len() == 0)
            }
        })
    }

    /// Reads the field at `path` of the document stored under `key` in the active database.
    fn indexed_field(&self, key: &[u8], path: &[u8]) -> Option<Vec<u8>> {
        if let Some(tree) = self.json_tree(key) {
            return tree.get(path).ok().flatten();
        }

        match self.json_view(key) {
            Ok(Some(document)) => crate::json::get(document, path).ok().flatten(),
            _ => None,
        }
    }

    /// Makes the postings of `key` (active database) agree with what it holds now. Called
    /// after any change to the key; absent, expired or non-JSON keys leave the index.
    pub(crate) fn json_index_refresh(&mut self, key: &[u8]) {
        let Some(mut indexes) = self.json_indexes.take() else {
            return;
        };

        {
            let reader = |path: &[u8]| self.indexed_field(key, path);

            indexes.reindex(self.active, key, Some(&reader));
        }

        self.json_indexes = Some(indexes);
    }

    pub(crate) fn json_index_database_flushed(&mut self, database: u8) {
        if let Some(indexes) = self.json_indexes.as_mut() {
            indexes.clear_database(database);
        }
    }

    pub(crate) fn json_index_all_flushed(&mut self) {
        self.json_indexes = None;
    }

    pub(crate) fn json_index_databases_swapped(&mut self, left: u8, right: u8) {
        if let Some(indexes) = self.json_indexes.as_mut() {
            indexes.swap_databases(left, right);
        }
    }

    /// Keys of the active database that hold `value` in index `name`, ascending after
    /// `after`, at most `count`. Every candidate is read again before it is returned.
    pub(crate) fn json_index_find(
        &mut self,
        name: &str,
        value: &[u8],
        after: Option<&[u8]>,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, FindError> {
        let (field, path, scalar, state) = {
            let index = self
                .json_indexes
                .as_ref()
                .and_then(|indexes| indexes.get(name))
                .ok_or(FindError::NoSuchIndex)?;
            let field = index.definition.field;

            (
                field,
                index.definition.path.clone(),
                Scalar::from_argument(value, field).ok_or(FindError::BadValue)?,
                index.state,
            )
        };

        if state != IndexState::Ready {
            return Err(FindError::Building);
        }

        let mut found = Vec::with_capacity(count.min(64));
        let mut cursor = after.map(<[u8]>::to_vec);

        // Stale candidates are dropped and the page refilled, a bounded number of times.
        for _ in 0..4 {
            let candidates = {
                let index = self
                    .json_indexes
                    .as_ref()
                    .and_then(|indexes| indexes.get(name))
                    .ok_or(FindError::NoSuchIndex)?;

                JsonIndexes::candidates(
                    index,
                    self.active,
                    &scalar,
                    cursor.as_deref(),
                    count - found.len(),
                )
            };

            if candidates.is_empty() {
                break;
            }

            for key in candidates {
                cursor = Some(key.clone());

                let live = self
                    .indexed_field(&key, &path)
                    .and_then(|json| Scalar::from_json(&json, field))
                    .is_some_and(|current| current == scalar);

                if live {
                    found.push(key);
                } else if let Some(indexes) = self.json_indexes.as_mut() {
                    indexes.drop_candidate(name, self.active, &key);
                }
            }

            if found.len() >= count {
                break;
            }
        }

        Ok(found)
    }

    /// Reads at most `budget` slots into the building index. Returns true while work remains.
    pub(crate) fn json_index_build_step(&mut self, budget: usize) -> bool {
        let Some(name) = self
            .json_indexes
            .as_ref()
            .and_then(|indexes| indexes.building())
            .map(|index| index.definition.name.clone())
        else {
            return false;
        };

        let (database, cursor) = {
            let index = self
                .json_indexes
                .as_ref()
                .and_then(|i| i.get(&name))
                .unwrap();

            (index.build_database, index.build_cursor)
        };
        let databases = self.database_ids();
        let Some(position) = databases.iter().position(|id| *id >= database) else {
            self.finish_json_index_build(&name);
            return self
                .json_indexes
                .as_ref()
                .is_some_and(|i| i.building().is_some());
        };
        let current = databases[position];
        let previous = self.active;

        self.enter(current);

        let (next, keys) = {
            let (next, keys) = self.scan(cursor, budget, None, Some(ValueKind::Json));

            (
                next,
                keys.into_iter().map(<[u8]>::to_vec).collect::<Vec<_>>(),
            )
        };

        for key in &keys {
            if let Some(mut indexes) = self.json_indexes.take() {
                let reader = |path: &[u8]| self.indexed_field(key, path);

                if let Some(index) = indexes.get_mut(&name) {
                    index.build_one(current, key, &reader);
                }

                self.json_indexes = Some(indexes);
            }
        }

        self.enter(previous);

        if let Some(index) = self.json_indexes.as_mut().and_then(|i| i.get_mut(&name)) {
            if next == 0 {
                index.build_database = current.saturating_add(1);
                index.build_cursor = 0;

                if databases.last().is_none_or(|last| *last <= current) {
                    index.state = IndexState::Ready;
                }
            } else {
                index.build_cursor = next;
            }
        }

        self.json_indexes
            .as_ref()
            .is_some_and(|i| i.building().is_some())
    }

    fn finish_json_index_build(&mut self, name: &str) {
        if let Some(indexes) = self.json_indexes.as_mut() {
            indexes.finish_build(name);
        }
    }

    /// Keeps the registry in step with replayed definition keys (replicas and recovery).
    pub(crate) fn json_index_replayed(&mut self, mutation: &crate::OwnedMutation) {
        use crate::OwnedMutation;

        let name_of = |key: &[u8]| {
            key.strip_prefix(DEFINITION_PREFIX)
                .and_then(|name| std::str::from_utf8(name).ok())
                .map(str::to_owned)
        };

        match mutation {
            OwnedMutation::SetBytes { key, value } => {
                if let Some(name) = name_of(key)
                    && let Some(definition) = Definition::decode(&name, value)
                {
                    self.json_index_create(definition);
                }
            }
            OwnedMutation::Delete { keys } => {
                for name in keys.iter().filter_map(|key| name_of(key)) {
                    self.json_index_drop(&name);
                }
            }
            // A `MULTI` or script: its definition writes count in order, and a flush in
            // it drops what came before, as it did on the primary.
            OwnedMutation::Transaction { mutations } => {
                for inner in mutations {
                    self.json_index_replayed(inner);
                }
            }
            OwnedMutation::FlushDatabase | OwnedMutation::FlushAll => self.json_index_all_flushed(),
            _ => {}
        }
    }

    /// Re-creates the registry from the definition keys of database 0 (after recovery).
    pub(crate) fn json_index_load_definitions(&mut self) {
        let previous = self.active;

        self.enter(0);

        let mut found = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (next, keys) = self.scan(cursor, 1024, None, Some(ValueKind::Bytes));

            for key in keys {
                if let Some(name) = key.strip_prefix(DEFINITION_PREFIX)
                    && let Ok(name) = std::str::from_utf8(name)
                    && let Ok(Some(value)) = self.get_bytes(key).map(|v| v.map(|v| v.to_vec()))
                    && let Some(definition) = Definition::decode(name, &value)
                {
                    found.push(definition);
                }
            }

            if next == 0 {
                break;
            }

            cursor = next;
        }

        self.enter(previous);

        for definition in found {
            self.json_index_create(definition);
        }
    }
}
