//! Field indexes over JSON documents (J10): exact equality on one scalar path.
//!
//! An index is a key prefix, one `$.path` and a scalar type. The shard keeps, per index,
//! `value -> sorted keys` plus `key -> value`, so removing a key never needs its old
//! document. Everything here is absent (`Option::None` on the shard) until the first
//! `JSON.INDEX CREATE`, so keys outside an index pay one `is_none` test and nothing else.
//!
//! The postings are a cache of the keyspace and are never persisted: definitions are, and
//! the postings are rebuilt from the documents (`Building` state, a bounded number of keys
//! per shard tick). `JSON.FIND` re-reads every returned key, so a stale entry (a missed
//! delete, an eviction that raced) can cost a lookup but never produces a wrong answer.

use std::collections::{BTreeSet, HashMap};

/// What the indexed field must be. Documents whose field has another type are not indexed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FieldType {
    String,
    Number,
    Bool,
}

impl FieldType {
    pub(crate) fn parse(name: &[u8]) -> Option<Self> {
        match name.to_ascii_lowercase().as_slice() {
            b"string" => Some(Self::String),
            b"number" => Some(Self::Number),
            b"bool" | b"boolean" => Some(Self::Bool),
            _ => None,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Bool => "bool",
        }
    }
}

/// One indexed scalar, compared exactly. Numbers compare by their `f64` value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Scalar {
    String(Box<str>),
    Number(u64),
    Bool(bool),
}

impl Scalar {
    /// The scalar a JSON value is, if it has the wanted type.
    pub(crate) fn from_json(json: &[u8], wanted: FieldType) -> Option<Self> {
        match (wanted, json.first()?) {
            (FieldType::String, b'"') => serde_json::from_slice::<String>(json)
                .ok()
                .map(|text| Self::String(text.into_boxed_str())),
            (FieldType::Bool, b't') if json == b"true" => Some(Self::Bool(true)),
            (FieldType::Bool, b'f') if json == b"false" => Some(Self::Bool(false)),
            (FieldType::Number, b'-' | b'0'..=b'9') => serde_json::from_slice::<f64>(json)
                .ok()
                .map(|number| Self::Number(canonical_bits(number))),
            _ => None,
        }
    }

    /// The scalar a `JSON.FIND ... VALUE` argument names: JSON text, or for strings the
    /// bare text when it is not valid JSON.
    pub(crate) fn from_argument(argument: &[u8], wanted: FieldType) -> Option<Self> {
        Self::from_json(argument, wanted).or_else(|| match wanted {
            FieldType::String => std::str::from_utf8(argument)
                .ok()
                .map(|text| Self::String(text.into())),
            _ => None,
        })
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::String(text) => text.len(),
            _ => 0,
        }
    }
}

/// `0.0` and `-0.0` are one number.
fn canonical_bits(number: f64) -> u64 {
    if number == 0.0 { 0 } else { number.to_bits() }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Definition {
    pub name: String,
    pub prefix: Vec<u8>,
    /// A `$.field...` path accepted by `crate::json`.
    pub path: Vec<u8>,
    pub field: FieldType,
}

impl Definition {
    /// The definition as stored in the keyspace: `prefix \0 path \0 type`.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.prefix.len() + self.path.len() + 8);

        bytes.extend_from_slice(&self.prefix);
        bytes.push(0);
        bytes.extend_from_slice(&self.path);
        bytes.push(0);
        bytes.extend_from_slice(self.field.name().as_bytes());
        bytes
    }

    pub(crate) fn decode(name: &str, bytes: &[u8]) -> Option<Self> {
        let mut parts = bytes.splitn(3, |byte| *byte == 0);
        let prefix = parts.next()?;
        let path = parts.next()?;
        let field = FieldType::parse(parts.next()?)?;

        Some(Self {
            name: name.to_owned(),
            prefix: prefix.to_vec(),
            path: path.to_vec(),
            field,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexState {
    /// Existing documents are still being read in; `JSON.FIND` refuses.
    Building,
    Ready,
}

type DbKey = (u8, Vec<u8>);

#[derive(Debug, Clone)]
pub(crate) struct Index {
    pub definition: Definition,
    pub state: IndexState,
    /// Where the build scan stands (`SCAN` cursor) and the database it is in.
    pub build_database: u8,
    pub build_cursor: u64,
    postings: HashMap<(u8, Scalar), BTreeSet<Vec<u8>>>,
    current: HashMap<DbKey, Scalar>,
}

impl Index {
    fn new(definition: Definition) -> Self {
        Self {
            definition,
            state: IndexState::Building,
            build_database: 0,
            build_cursor: 0,
            postings: HashMap::new(),
            current: HashMap::new(),
        }
    }

    /// Build scan: indexes one existing key of `database`.
    pub(crate) fn build_one(
        &mut self,
        database: u8,
        key: &[u8],
        field: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
    ) {
        if !self.applies_to(key) {
            return;
        }

        self.forget(database, key);

        if let Some(scalar) = field(&self.definition.path)
            .and_then(|json| Scalar::from_json(&json, self.definition.field))
        {
            self.remember(database, key, scalar);
        }
    }

    fn applies_to(&self, key: &[u8]) -> bool {
        key.starts_with(&self.definition.prefix)
    }

    pub(crate) fn len(&self) -> usize {
        self.current.len()
    }

    fn forget(&mut self, database: u8, key: &[u8]) {
        let slot = (database, key.to_vec());
        let Some(old) = self.current.remove(&slot) else {
            return;
        };
        let posting = (database, old);

        if let Some(keys) = self.postings.get_mut(&posting) {
            keys.remove(key);

            if keys.is_empty() {
                self.postings.remove(&posting);
            }
        }
    }

    fn remember(&mut self, database: u8, key: &[u8], value: Scalar) {
        self.current.insert((database, key.to_vec()), value.clone());
        self.postings
            .entry((database, value))
            .or_default()
            .insert(key.to_vec());
    }

    fn clear_database(&mut self, database: u8) {
        self.postings.retain(|(db, _), _| *db != database);
        self.current.retain(|(db, _), _| *db != database);
    }

    fn swap_databases(&mut self, left: u8, right: u8) {
        let swap = |db: &mut u8| {
            if *db == left {
                *db = right;
            } else if *db == right {
                *db = left;
            }
        };

        self.postings = std::mem::take(&mut self.postings)
            .into_iter()
            .map(|((mut db, value), keys)| {
                swap(&mut db);
                ((db, value), keys)
            })
            .collect();
        self.current = std::mem::take(&mut self.current)
            .into_iter()
            .map(|((mut db, key), value)| {
                swap(&mut db);
                ((db, key), value)
            })
            .collect();
    }

    fn memory_bytes(&self) -> usize {
        let entry = std::mem::size_of::<(DbKey, Scalar)>();
        let posting = std::mem::size_of::<((u8, Scalar), BTreeSet<Vec<u8>>)>();

        self.current
            .iter()
            .map(|((_, key), value)| entry + key.len() * 2 + value.heap_bytes() * 2 + 48)
            .sum::<usize>()
            + self.postings.len() * posting
    }
}

/// Reads the indexed field of a stored document.
pub(crate) type FieldReader<'a> = dyn Fn(&[u8]) -> Option<Vec<u8>> + 'a;

/// One database's postings of every index, by index name.
#[allow(clippy::type_complexity)]
pub(crate) struct DetachedPostings(
    Vec<(
        String,
        Vec<((u8, Scalar), BTreeSet<Vec<u8>>)>,
        Vec<(DbKey, Scalar)>,
    )>,
);

#[derive(Default, Debug)]
pub(crate) struct JsonIndexes {
    indexes: Vec<Index>,
}

impl JsonIndexes {
    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub(crate) fn definitions(
        &self,
    ) -> impl Iterator<Item = (&Definition, IndexState, usize, usize)> {
        self.indexes.iter().map(|index| {
            (
                &index.definition,
                index.state,
                index.len(),
                index.memory_bytes(),
            )
        })
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Index> {
        self.indexes
            .iter()
            .find(|index| index.definition.name == name)
    }

    pub(crate) fn get_mut(&mut self, name: &str) -> Option<&mut Index> {
        self.indexes
            .iter_mut()
            .find(|index| index.definition.name == name)
    }

    /// Adds an index in the `Building` state. False when the name is taken.
    pub(crate) fn create(&mut self, definition: Definition) -> bool {
        if self.get(&definition.name).is_some() {
            return false;
        }

        self.indexes.push(Index::new(definition));
        true
    }

    /// Adds an index that is already complete (an empty keyspace has nothing to build).
    pub(crate) fn create_ready(&mut self, definition: Definition) -> bool {
        if !self.create(definition) {
            return false;
        }

        if let Some(index) = self.indexes.last_mut() {
            index.state = IndexState::Ready;
        }

        true
    }

    /// A full copy of the index named `name`, postings and build position included.
    pub(crate) fn copy_of(&self, name: &str) -> Option<Index> {
        self.get(name).cloned()
    }

    /// Puts back an index dropped earlier, as it was; false when the name is taken again.
    pub(crate) fn restore(&mut self, index: Index) -> bool {
        if self.get(&index.definition.name).is_some() {
            return false;
        }

        self.indexes.push(index);
        true
    }

    pub(crate) fn drop_index(&mut self, name: &str) -> bool {
        let before = self.indexes.len();

        self.indexes.retain(|index| index.definition.name != name);
        self.indexes.len() != before
    }

    /// Makes `key` in `database` agree with its document. `field` reads the scalar at an
    /// index path out of whatever representation the document has; `None` means the key no
    /// longer holds a JSON document.
    pub(crate) fn reindex(&mut self, database: u8, key: &[u8], field: Option<&FieldReader<'_>>) {
        for index in &mut self.indexes {
            if !index.applies_to(key) {
                continue;
            }

            index.forget(database, key);

            let scalar = field
                .and_then(|read| read(&index.definition.path))
                .and_then(|json| Scalar::from_json(&json, index.definition.field));

            if let Some(scalar) = scalar {
                index.remember(database, key, scalar);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn forget(&mut self, database: u8, key: &[u8]) {
        for index in &mut self.indexes {
            if index.applies_to(key) {
                index.forget(database, key);
            }
        }
    }

    pub(crate) fn clear_database(&mut self, database: u8) {
        for index in &mut self.indexes {
            index.clear_database(database);
        }
    }

    /// Lifts one database's postings out, for a flush inside `MULTI` that may be refused.
    pub(crate) fn detach_database(&mut self, database: u8) -> DetachedPostings {
        DetachedPostings(
            self.indexes
                .iter_mut()
                .map(|index| {
                    (
                        index.definition.name.clone(),
                        index
                            .postings
                            .extract_if(|(db, _), _| *db == database)
                            .collect(),
                        index
                            .current
                            .extract_if(|(db, _), _| *db == database)
                            .collect(),
                    )
                })
                .collect(),
        )
    }

    /// Puts back what `detach_database` lifted, in place of what the database holds now.
    pub(crate) fn restore_database(&mut self, database: u8, detached: DetachedPostings) {
        self.clear_database(database);
        for (name, postings, current) in detached.0 {
            if let Some(index) = self.get_mut(&name) {
                index.postings.extend(postings);
                index.current.extend(current);
            }
        }
    }

    pub(crate) fn swap_databases(&mut self, left: u8, right: u8) {
        for index in &mut self.indexes {
            index.swap_databases(left, right);
        }
    }

    /// Keys of `database` whose indexed value is `value`, ascending, after `after`.
    pub(crate) fn candidates(
        index: &Index,
        database: u8,
        value: &Scalar,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Vec<Vec<u8>> {
        let Some(keys) = index.postings.get(&(database, value.clone())) else {
            return Vec::new();
        };

        match after {
            Some(after) => keys
                .range::<[u8], _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .take(limit)
                .cloned()
                .collect(),
            None => keys.iter().take(limit).cloned().collect(),
        }
    }

    pub(crate) fn drop_candidate(&mut self, name: &str, database: u8, key: &[u8]) {
        if let Some(index) = self.get_mut(name) {
            index.forget(database, key);
        }
    }

    pub(crate) fn finish_build(&mut self, name: &str) {
        if let Some(index) = self.get_mut(name) {
            index.state = IndexState::Ready;
        }
    }

    pub(crate) fn building(&self) -> Option<&Index> {
        self.indexes
            .iter()
            .find(|index| index.state == IndexState::Building)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read<'a>(document: &'a [u8]) -> impl Fn(&[u8]) -> Option<Vec<u8>> + 'a {
        move |path| crate::json::get(document, path).ok().flatten()
    }

    fn definition(name: &str, prefix: &str, path: &str, field: FieldType) -> Definition {
        Definition {
            name: name.into(),
            prefix: prefix.into(),
            path: path.into(),
            field,
        }
    }

    #[test]
    fn definitions_round_trip_through_their_stored_form() {
        let original = definition("by_age", "user:", "$.profile.age", FieldType::Number);

        assert_eq!(
            Definition::decode("by_age", &original.encode()),
            Some(original)
        );
        assert_eq!(Definition::decode("x", b"user:\0$.a\0nonsense"), None);
    }

    #[test]
    fn reindex_follows_the_document_and_ignores_other_types() {
        let mut indexes = JsonIndexes::default();

        indexes.create_ready(definition("n", "u:", "$.name", FieldType::String));
        let first = br#"{"name":"ada","age":36}"#;
        let first_read = read(first);

        indexes.reindex(0, b"u:1", Some(&first_read));
        indexes.reindex(0, b"other", Some(&first_read));
        indexes.reindex(1, b"u:1", Some(&first_read));

        let index = indexes.get("n").unwrap();
        let ada = Scalar::from_argument(b"ada", FieldType::String).unwrap();

        assert_eq!(
            JsonIndexes::candidates(index, 0, &ada, None, 10),
            vec![b"u:1".to_vec()]
        );
        assert_eq!(
            index.len(),
            2,
            "databases are separate, other prefixes skipped"
        );

        let renamed = br#"{"name":"grace"}"#;
        let renamed_read = read(renamed);

        indexes.reindex(0, b"u:1", Some(&renamed_read));
        let index = indexes.get("n").unwrap();

        assert!(JsonIndexes::candidates(index, 0, &ada, None, 10).is_empty());

        let wrong_type = br#"{"name":7}"#;
        let wrong_type_read = read(wrong_type);

        indexes.reindex(0, b"u:1", Some(&wrong_type_read));
        assert_eq!(indexes.get("n").unwrap().len(), 1);

        indexes.reindex(0, b"u:1", None);
        indexes.forget(1, b"u:1");
        assert_eq!(indexes.get("n").unwrap().len(), 0);
    }

    #[test]
    fn find_pages_keys_in_order_and_numbers_compare_by_value() {
        let mut indexes = JsonIndexes::default();

        indexes.create_ready(definition("a", "", "$.n", FieldType::Number));

        for key in ["k3", "k1", "k2", "k4"] {
            let document: &[u8] = if key == "k4" {
                b"{\"n\":2}"
            } else {
                b"{\"n\":1.0}"
            };
            let reader = read(document);

            indexes.reindex(0, key.as_bytes(), Some(&reader));
        }

        let index = indexes.get("a").unwrap();
        let one = Scalar::from_argument(b"1", FieldType::Number).unwrap();

        assert_eq!(
            JsonIndexes::candidates(index, 0, &one, None, 2),
            vec![b"k1".to_vec(), b"k2".to_vec()]
        );
        assert_eq!(
            JsonIndexes::candidates(index, 0, &one, Some(b"k2"), 5),
            vec![b"k3".to_vec()]
        );
    }

    #[test]
    fn database_swap_and_clear_move_postings_with_their_database() {
        let mut indexes = JsonIndexes::default();

        indexes.create_ready(definition("a", "", "$.v", FieldType::Bool));
        let document = br#"{"v":true}"#;
        let reader = read(document);

        indexes.reindex(0, b"k", Some(&reader));
        indexes.swap_databases(0, 3);

        let index = indexes.get("a").unwrap();
        let yes = Scalar::from_argument(b"true", FieldType::Bool).unwrap();

        assert!(JsonIndexes::candidates(index, 0, &yes, None, 5).is_empty());
        assert_eq!(JsonIndexes::candidates(index, 3, &yes, None, 5).len(), 1);

        indexes.clear_database(3);
        assert_eq!(indexes.get("a").unwrap().len(), 0);
    }
}
