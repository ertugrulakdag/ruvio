use std::ops::Range;

use serde_json::{Value, value::RawValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonError {
    Syntax,
    Path,
    MissingRoot,
}

#[derive(Clone, Copy)]
enum Segment<'a> {
    Field(&'a str),
    Index(usize),
}

fn parse_path(path: &[u8]) -> Result<Vec<Segment<'_>>, JsonError> {
    let path = std::str::from_utf8(path).map_err(|_| JsonError::Path)?;
    let bytes = path.as_bytes();
    if bytes.first() != Some(&b'$') {
        return Err(JsonError::Path);
    }

    let mut segments = Vec::new();
    let mut pos = 1;
    while pos < bytes.len() {
        match bytes[pos] {
            b'.' => {
                pos += 1;
                let start = pos;
                while pos < bytes.len() && bytes[pos] != b'.' && bytes[pos] != b'[' {
                    pos += 1;
                }
                let field = &path[start..pos];
                if field.is_empty()
                    || field
                        .chars()
                        .any(|c| c.is_whitespace() || matches!(c, '*' | '?' | ']' | '"' | '\''))
                {
                    return Err(JsonError::Path);
                }
                segments.push(Segment::Field(field));
            }
            b'[' => {
                pos += 1;
                let start = pos;
                let mut index = 0usize;
                while pos < bytes.len() && bytes[pos].is_ascii_digit() {
                    index = index
                        .checked_mul(10)
                        .and_then(|n| n.checked_add(usize::from(bytes[pos] - b'0')))
                        .ok_or(JsonError::Path)?;
                    pos += 1;
                }
                if pos == start || bytes.get(pos) != Some(&b']') {
                    return Err(JsonError::Path);
                }
                pos += 1;
                segments.push(Segment::Index(index));
            }
            _ => return Err(JsonError::Path),
        }
    }
    Ok(segments)
}

pub(crate) fn validate_path(path: &[u8]) -> Result<(), JsonError> {
    parse_path(path).map(|_| ())
}

fn child_mut<'a>(value: &'a mut Value, segment: Segment<'_>) -> Option<&'a mut Value> {
    match segment {
        Segment::Field(field) => value.as_object_mut()?.get_mut(field),
        Segment::Index(index) => value.as_array_mut()?.get_mut(index),
    }
}

fn parse_document(document: &[u8]) -> Result<Value, JsonError> {
    serde_json::from_slice(document).map_err(|_| JsonError::Syntax)
}

fn encode(value: &Value) -> Result<Vec<u8>, JsonError> {
    serde_json::to_vec(value).map_err(|_| JsonError::Syntax)
}

/// Byte range of the value at `segments`. Stored documents are valid JSON, so this walks
/// the bytes directly (one pass, no tokenizing); a malformed document yields `Syntax`.
fn select_raw(
    document: &[u8],
    segments: &[Segment<'_>],
) -> Result<Option<std::ops::Range<usize>>, JsonError> {
    let mut at = skip_space(document, 0);
    for segment in segments {
        match (*segment, document.get(at)) {
            (Segment::Field(name), Some(b'{')) => {
                at += 1;
                loop {
                    at = skip_space(document, at);
                    match document.get(at) {
                        Some(b'"') => {}
                        Some(b'}') => return Ok(None),
                        _ => return Err(JsonError::Syntax),
                    }
                    let key_end = skip_string(document, at).ok_or(JsonError::Syntax)?;
                    let key = &document[at + 1..key_end - 1];
                    let matches = if key.contains(&b'\\') {
                        serde_json::from_slice::<String>(&document[at..key_end])
                            .map_err(|_| JsonError::Syntax)?
                            == name
                    } else {
                        key == name.as_bytes()
                    };
                    at = skip_space(document, key_end);
                    if document.get(at) != Some(&b':') {
                        return Err(JsonError::Syntax);
                    }
                    at = skip_space(document, at + 1);
                    if matches {
                        break;
                    }
                    at = skip_space(document, skip_value(document, at)?);
                    match document.get(at) {
                        Some(b',') => at += 1,
                        Some(b'}') => return Ok(None),
                        _ => return Err(JsonError::Syntax),
                    }
                }
            }
            (Segment::Index(index), Some(b'[')) => {
                at = skip_space(document, at + 1);
                if document.get(at) == Some(&b']') {
                    return Ok(None);
                }
                for _ in 0..index {
                    at = skip_space(document, skip_value(document, at)?);
                    match document.get(at) {
                        Some(b',') => at = skip_space(document, at + 1),
                        Some(b']') => return Ok(None),
                        _ => return Err(JsonError::Syntax),
                    }
                }
            }
            (_, Some(_)) => return Ok(None),
            (_, None) => return Err(JsonError::Syntax),
        }
    }
    let end = skip_value(document, at)?;
    Ok(Some(at..end))
}

fn skip_space(document: &[u8], mut at: usize) -> usize {
    while matches!(document.get(at), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        at += 1;
    }
    at
}

/// Index just past the string that starts at `at` (a `"`).
fn skip_string(document: &[u8], at: usize) -> Option<usize> {
    let mut index = at + 1;
    while let Some(&byte) = document.get(index) {
        match byte {
            b'"' => return Some(index + 1),
            b'\\' => index += 2,
            _ => index += 1,
        }
    }
    None
}

/// Index just past the value that starts at `at`.
fn skip_value(document: &[u8], at: usize) -> Result<usize, JsonError> {
    match document.get(at) {
        Some(b'"') => skip_string(document, at).ok_or(JsonError::Syntax),
        Some(b'{' | b'[') => {
            let mut depth = 0usize;
            let mut index = at;
            while let Some(&byte) = document.get(index) {
                match byte {
                    b'"' => {
                        index = skip_string(document, index).ok_or(JsonError::Syntax)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Ok(index + 1);
                        }
                    }
                    _ => {}
                }
                index += 1;
            }
            Err(JsonError::Syntax)
        }
        Some(_) => {
            let mut index = at;
            while !matches!(
                document.get(index),
                None | Some(b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r')
            ) {
                index += 1;
            }
            Ok(index)
        }
        None => Err(JsonError::Syntax),
    }
}

// Why splice: parsing and re-encoding the whole document per write is O(document); a located byte range
// is replaced in place instead, and serde only parses the small part being written.
// Stored documents and replacements are compact and key-sorted before this path runs.
fn splice(document: &[u8], selected: std::ops::Range<usize>, replacement: &[u8]) -> Vec<u8> {
    let (start, end) = (selected.start, selected.end);
    let mut result = Vec::with_capacity(document.len() - (end - start) + replacement.len());
    result.extend_from_slice(&document[..start]);
    result.extend_from_slice(replacement);
    result.extend_from_slice(&document[end..]);
    result
}

fn merge_patch(target: Option<Value>, patch: Value) -> Value {
    let Value::Object(patch) = patch else {
        return patch;
    };
    let mut target = match target {
        Some(Value::Object(target)) => target,
        _ => serde_json::Map::new(),
    };
    for (name, value) in patch {
        if value.is_null() {
            target.shift_remove(&name);
        } else {
            // An existing member keeps its position; a new one goes last.
            let previous = target.get_mut(&name).map(std::mem::take);
            target.insert(name, merge_patch(previous, value));
        }
    }
    Value::Object(target)
}

/// Applies RFC 7396 to the selected root or simple path.
/// Only a final object field can be created; absent intermediate nodes are unchanged.
pub(crate) fn merge(
    document: Option<&[u8]>,
    path: &[u8],
    patch: &[u8],
) -> Result<Option<Vec<u8>>, JsonError> {
    let segments = parse_path(path)?;
    let patch = parse_document(patch)?;
    let Some((last, parents)) = segments.split_last() else {
        let document = document.map(parse_document).transpose()?;
        return encode(&merge_patch(document, patch)).map(Some);
    };
    let Some(document) = document else {
        return Err(JsonError::MissingRoot);
    };
    if let Some(target) = select_raw(document, &segments)? {
        let updated = merge_patch(Some(parse_document(&document[target.clone()])?), patch);
        return Ok(Some(splice(document, target, &encode(&updated)?)));
    }
    let mut document = parse_document(document)?;
    let mut parent = &mut document;
    for &segment in parents {
        parent = match child_mut(parent, segment) {
            Some(value) => value,
            None => return Ok(None),
        };
    }
    match *last {
        Segment::Field(field) => {
            let Some(object) = parent.as_object_mut() else {
                return Ok(None);
            };
            let previous = object.get_mut(field).map(std::mem::take);
            object.insert(field.to_owned(), merge_patch(previous, patch));
        }
        Segment::Index(index) => {
            let Some(target) = parent.as_array_mut().and_then(|array| array.get_mut(index)) else {
                return Ok(None);
            };
            let previous = std::mem::replace(target, Value::Null);
            *target = merge_patch(Some(previous), patch);
        }
    }
    encode(&document).map(Some)
}

/// Replaces a root whose existing value has already been validated on insertion.
pub(crate) fn set_root(value: &[u8]) -> Result<Vec<u8>, JsonError> {
    encode(&parse_document(value)?)
}

/// `set_root` that also decides tree eligibility from the value it already parsed, so a big
/// document is parsed once instead of again by `TreeDocument::from_bytes`.
pub(crate) fn set_root_tree(value: &[u8]) -> Result<(Vec<u8>, Option<TreeDocument>), JsonError> {
    let root = parse_document(value)?;
    let canonical = encode(&root)?;
    let tree = TreeDocument::from_parsed(&canonical, root);
    Ok((canonical, tree))
}

/// True when some string token is at least `TREE_THRESHOLD / 2` bytes long (quotes included):
/// the only documents `IndexedDocument` accepts. One byte-wise pass, no allocation.
fn has_large_string(bytes: &[u8]) -> bool {
    let limit = TREE_THRESHOLD / 2;
    let (mut start, mut in_string, mut escaped) = (0usize, false, false);

    for (at, byte) in bytes.iter().enumerate() {
        if !in_string {
            if *byte == b'"' {
                in_string = true;
                start = at;
            }
        } else if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            in_string = false;
            if at + 1 - start >= limit {
                return true;
            }
        }
    }

    false
}

/// Selects from a compact stored document without materializing unrelated fields.
pub(crate) fn get(document: &[u8], path: &[u8]) -> Result<Option<Vec<u8>>, JsonError> {
    let segments = parse_path(path)?;
    if segments.is_empty() {
        return encode(&parse_document(document)?).map(Some);
    }
    Ok(select_raw(document, &segments)?.map(|range| document[range].to_vec()))
}

/// `get` for a document read back from the store, which is already canonical (compact,
/// key-sorted): the root is copied instead of parsed and encoded again.
pub(crate) fn get_stored(document: &[u8], path: &[u8]) -> Result<Option<Vec<u8>>, JsonError> {
    let segments = parse_path(path)?;
    if segments.is_empty() {
        return Ok(Some(document.to_vec()));
    }
    Ok(select_raw(document, &segments)?.map(|range| document[range].to_vec()))
}

/// Replaces the root or an existing target; a final object field may be created.
/// Missing intermediate nodes and out-of-range array indices return `None`.
pub(crate) fn set(
    document: Option<&[u8]>,
    path: &[u8],
    value: &[u8],
) -> Result<Option<Vec<u8>>, JsonError> {
    set_with(document, path, value, false)
}

/// `set`; with `trusted` the replacement is canonical JSON the engine built from stored bytes,
/// so it is spliced in as is.
pub(crate) fn set_with(
    document: Option<&[u8]>,
    path: &[u8],
    value: &[u8],
    trusted: bool,
) -> Result<Option<Vec<u8>>, JsonError> {
    let segments = parse_path(path)?;
    // A trusted value is canonical already; only the tree fallback below needs it parsed.
    let replacement = if trusted {
        None
    } else {
        Some(parse_document(value)?)
    };
    let canonical = |replacement: &Option<Value>| match replacement {
        Some(parsed) => encode(parsed),
        None => Ok(value.to_vec()),
    };
    let Some(document) = document else {
        return if segments.is_empty() {
            canonical(&replacement).map(Some)
        } else {
            Err(JsonError::MissingRoot)
        };
    };
    let Some((last, parents)) = segments.split_last() else {
        parse_document(document)?;
        return canonical(&replacement).map(Some);
    };
    // Writing at index == length appends; the bytes are spliced in without parsing the array.
    // The parent is looked up first so an append costs one pass over the document, not two.
    if let Segment::Index(index) = *last
        && let Some(array) = select_raw(document, parents)?
        && document[array.start] == b'['
        && count_items(&document[array.clone()]) == index
    {
        let end = array.end - 1;
        let item = canonical(&replacement)?;
        let mut result = Vec::with_capacity(document.len() + item.len() + 1);
        result.extend_from_slice(&document[..end]);
        if index > 0 {
            result.push(b',');
        }
        result.extend_from_slice(&item);
        result.extend_from_slice(&document[end..]);
        return Ok(Some(result));
    }
    if let Some(target) = select_raw(document, &segments)? {
        return Ok(Some(splice(document, target, &canonical(&replacement)?)));
    }
    let replacement = match replacement {
        Some(parsed) => parsed,
        None => parse_document(value)?,
    };
    let mut document = parse_document(document)?;
    let mut parent = &mut document;
    for &segment in parents {
        parent = match child_mut(parent, segment) {
            Some(value) => value,
            None => return Ok(None),
        };
    }
    match *last {
        Segment::Field(field) => {
            let Some(object) = parent.as_object_mut() else {
                return Ok(None);
            };
            object.insert(field.to_owned(), replacement);
        }
        Segment::Index(index) => {
            let Some(array) = parent.as_array_mut() else {
                return Ok(None);
            };
            if index < array.len() {
                array[index] = replacement;
            } else if index == array.len() {
                array.push(replacement);
            } else {
                return Ok(None);
            }
        }
    }
    encode(&document).map(Some)
}

/// Number of elements of a compact array (`bytes` starts with `[`), by one byte scan.
fn count_items(bytes: &[u8]) -> usize {
    let inner = &bytes[1..bytes.len() - 1];
    if inner.iter().all(u8::is_ascii_whitespace) {
        return 0;
    }
    let (mut depth, mut commas, mut quoted, mut escaped) = (0usize, 0usize, false, false);
    for &byte in inner {
        if quoted {
            match (escaped, byte) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => quoted = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b'[' | b'{' => depth += 1,
            b']' | b'}' => depth -= 1,
            b',' if depth == 0 => commas += 1,
            _ => {}
        }
    }
    commas + 1
}

/// Elements of a stored array (`bytes` starts with `[`) as byte ranges, one forward scan and no
/// allocation. Stored documents are valid JSON, so the scan only tracks strings and nesting.
pub(crate) struct ArrayItems<'a> {
    bytes: &'a [u8],
    at: usize,
}

pub(crate) fn array_items(bytes: &[u8]) -> ArrayItems<'_> {
    ArrayItems { bytes, at: 1 }
}

impl Iterator for ArrayItems<'_> {
    type Item = std::ops::Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.at = skip_space(self.bytes, self.at);

            match self.bytes.get(self.at) {
                Some(b',') => self.at += 1,
                Some(b']') | None => return None,
                Some(_) => {
                    let end = skip_value(self.bytes, self.at).ok()?;
                    let item = self.at..end;

                    self.at = end;
                    return Some(item);
                }
            }
        }
    }
}

/// Members of a stored object (`bytes` starts with `{`): the key including its quotes, then the value.
pub(crate) struct ObjectMembers<'a> {
    bytes: &'a [u8],
    at: usize,
}

pub(crate) fn object_members(bytes: &[u8]) -> ObjectMembers<'_> {
    ObjectMembers { bytes, at: 1 }
}

impl Iterator for ObjectMembers<'_> {
    type Item = (std::ops::Range<usize>, std::ops::Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.at = skip_space(self.bytes, self.at);

            match self.bytes.get(self.at) {
                Some(b',') => self.at += 1,
                Some(b'"') => {
                    let key_end = skip_string(self.bytes, self.at)?;
                    let key = self.at..key_end;
                    let colon = skip_space(self.bytes, key_end);

                    if self.bytes.get(colon) != Some(&b':') {
                        return None;
                    }

                    let start = skip_space(self.bytes, colon + 1);
                    let end = skip_value(self.bytes, start).ok()?;

                    self.at = end;
                    return Some((key, start..end));
                }
                _ => return None,
            }
        }
    }
}

/// Number of elements of a stored array.
pub(crate) fn count_array_items(bytes: &[u8]) -> usize {
    count_items(bytes)
}

pub(crate) enum ArrayLen {
    Missing,
    NotArray,
    Len(usize),
}

/// Element count of the array at `path` in a compact document.
pub(crate) fn array_len(document: &[u8], path: &[u8]) -> Result<ArrayLen, JsonError> {
    let segments = parse_path(path)?;
    Ok(match select_raw(document, &segments)? {
        None => ArrayLen::Missing,
        Some(raw) if document[raw.start] == b'[' => ArrayLen::Len(count_items(&document[raw])),
        Some(_) => ArrayLen::NotArray,
    })
}

/// Removes a selected field or array element. Both missing paths and root deletion
/// return `None`; callers distinguish root deletion using `path == b"$"`.
pub(crate) fn del(document: &[u8], path: &[u8]) -> Result<Option<Vec<u8>>, JsonError> {
    let segments = parse_path(path)?;
    // An array element is cut out of the bytes: the neighbouring comma goes with it.
    if let Some((Segment::Index(index), parents)) = segments.split_last()
        && let Some(array) = select_raw(document, parents)?
        && document[array.start] == b'['
    {
        let bytes = &document[array.clone()];
        let mut items = array_items(bytes);
        let mut previous_end = None;

        for _ in 0..*index {
            match items.next() {
                Some(item) => previous_end = Some(item.end),
                None => return Ok(None),
            }
        }
        let Some(item) = items.next() else {
            return Ok(None);
        };
        let cut = match (previous_end, items.next()) {
            (Some(end), _) => end..item.end,
            (None, Some(next)) => item.start..next.start,
            (None, None) => item.start..item.end,
        };
        let base = array.start;
        let mut result = Vec::with_capacity(document.len());

        result.extend_from_slice(&document[..base + cut.start]);
        result.extend_from_slice(&document[base + cut.end..]);
        return Ok(Some(result));
    }
    let mut document = parse_document(document)?;
    let Some((last, parents)) = segments.split_last() else {
        return Ok(None);
    };
    let mut parent = &mut document;
    for &segment in parents {
        parent = match child_mut(parent, segment) {
            Some(value) => value,
            None => return Ok(None),
        };
    }
    let removed = match *last {
        Segment::Field(field) => parent
            .as_object_mut()
            .and_then(|object| object.shift_remove(field))
            .is_some(),
        Segment::Index(index) => parent
            .as_array_mut()
            .and_then(|array| (index < array.len()).then(|| array.remove(index)))
            .is_some(),
    };
    if !removed {
        return Ok(None);
    }
    encode(&document).map(Some)
}

/// Large documents use indexed compact storage where possible, otherwise a tree
/// when its estimated ownership is bounded. Small documents remain compact.
pub(crate) const TREE_THRESHOLD: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct TreeDocument {
    root: Option<Value>,
    indexed: Option<IndexedDocument>,
    len: usize,
    charge: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct IndexedDocument {
    base: Vec<u8>,
    paths: Vec<(Vec<u8>, Range<usize>)>,
    patches: Vec<(usize, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq)]
enum IndexedEdit {
    Replace {
        start: usize,
        replacement: Vec<u8>,
    },
    /// A small indexed object or array replaced as a whole; indexed descendants are dropped.
    Splice {
        start: usize,
        end: usize,
        replacement: Vec<u8>,
    },
    Rebuild(Box<TreeDocument>),
}

#[derive(Clone)]
enum OwnedSegment {
    Field(String),
    Index(usize),
}

pub(crate) struct TreeEdit {
    path: Vec<OwnedSegment>,
    replacement: Option<Value>,
    indexed: Option<IndexedEdit>,
    len: usize,
    charge: usize,
}

fn tree_charge(value: &Value) -> usize {
    use std::mem::size_of;
    let mut charge = size_of::<Value>();
    match value {
        Value::String(string) => charge += string.capacity(),
        Value::Array(array) => {
            charge += array.capacity() * size_of::<Value>();
            charge += array.iter().map(tree_charge).sum::<usize>();
        }
        Value::Object(object) => {
            charge += object.len() * (size_of::<String>() + 3 * size_of::<usize>());
            charge += object
                .iter()
                .map(|(name, value)| name.capacity() + tree_charge(value))
                .sum::<usize>();
        }
        _ => {}
    }
    charge
}

impl IndexedDocument {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < TREE_THRESHOLD || bytes.first() != Some(&b'{') {
            return None;
        }
        if !has_large_string(bytes) {
            return None;
        }
        let mut paths = Vec::new();
        let mut large_string = false;
        Self::index_children(bytes, bytes, b"$", &mut paths, &mut large_string)?;
        if !large_string || paths.len() > 128 {
            return None;
        }
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        Some(Self {
            base: bytes.to_vec(),
            paths,
            patches: Vec::new(),
        })
    }

    fn index_children(
        base: &[u8],
        bytes: &[u8],
        parent: &[u8],
        paths: &mut Vec<(Vec<u8>, Range<usize>)>,
        large_string: &mut bool,
    ) -> Option<()> {
        if paths.len() > 128 {
            return None;
        }
        if bytes.first() == Some(&b'{') {
            let entries: std::collections::BTreeMap<String, &RawValue> =
                serde_json::from_slice(bytes).ok()?;
            for (field, raw) in entries {
                let mut path = parent.to_vec();
                path.push(b'.');
                path.extend_from_slice(field.as_bytes());
                if validate_path(&path).is_err() {
                    continue;
                }
                Self::index_raw(base, raw, path, paths, large_string)?;
            }
        } else if bytes.first() == Some(&b'[') {
            let entries: Vec<&RawValue> = serde_json::from_slice(bytes).ok()?;
            for (index, raw) in entries.into_iter().enumerate() {
                let mut path = parent.to_vec();
                path.extend_from_slice(format!("[{index}]").as_bytes());
                Self::index_raw(base, raw, path, paths, large_string)?;
            }
        }
        Some(())
    }

    fn index_raw(
        base: &[u8],
        raw: &RawValue,
        path: Vec<u8>,
        paths: &mut Vec<(Vec<u8>, Range<usize>)>,
        large_string: &mut bool,
    ) -> Option<()> {
        let bytes = raw.get().as_bytes();
        let start = bytes.as_ptr() as usize - base.as_ptr() as usize;
        if start.checked_add(bytes.len())? > base.len() {
            return None;
        }
        if bytes.first() == Some(&b'"') && bytes.len() >= TREE_THRESHOLD / 2 {
            *large_string = true;
        }
        paths.push((path.clone(), start..start + bytes.len()));
        Self::index_children(base, bytes, &path, paths, large_string)
    }

    fn charge(&self) -> usize {
        self.base.capacity()
            + self.paths.capacity() * std::mem::size_of::<(Vec<u8>, Range<usize>)>()
            + self
                .paths
                .iter()
                .map(|(path, _)| path.capacity())
                .sum::<usize>()
            + self.patches.capacity() * std::mem::size_of::<(usize, Vec<u8>)>()
            + self
                .patches
                .iter()
                .map(|(_, value)| value.capacity())
                .sum::<usize>()
    }

    fn range(&self, path: &[u8]) -> Option<&Range<usize>> {
        self.paths
            .binary_search_by(|(key, _)| key.as_slice().cmp(path))
            .ok()
            .map(|index| &self.paths[index].1)
    }

    fn write_range(
        &self,
        writer: &mut impl std::io::Write,
        range: Range<usize>,
    ) -> std::io::Result<()> {
        let mut pos = range.start;
        for (start, patch) in &self.patches {
            if *start >= range.end {
                break;
            }
            let end = self
                .paths
                .iter()
                .find(|(_, span)| span.start == *start)
                .unwrap()
                .1
                .end;
            if end <= range.start {
                continue;
            }
            writer.write_all(&self.base[pos..*start])?;
            writer.write_all(patch)?;
            pos = end;
        }
        writer.write_all(&self.base[pos..range.end])
    }

    fn to_vec(&self, range: Range<usize>) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(range.len());
        self.write_range(&mut bytes, range)
            .expect("Vec writes cannot fail");
        bytes
    }
}

impl TreeDocument {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < TREE_THRESHOLD {
            return None;
        }
        if let Some(indexed) = IndexedDocument::from_bytes(bytes) {
            let charge = indexed.charge();
            if charge <= bytes.len().saturating_mul(2) {
                return Some(Self {
                    root: None,
                    indexed: Some(indexed),
                    len: bytes.len(),
                    charge,
                });
            }
        }
        let root: Value = serde_json::from_slice(bytes).ok()?;
        let len = encode(&root).ok()?.len();
        Self::from_root(root, len)
    }

    /// Eligibility for a document the caller already parsed (`canonical` is its compact encoding).
    pub(crate) fn from_parsed(canonical: &[u8], root: Value) -> Option<Self> {
        if canonical.len() < TREE_THRESHOLD {
            return None;
        }
        if let Some(indexed) = IndexedDocument::from_bytes(canonical) {
            let charge = indexed.charge();
            if charge <= canonical.len().saturating_mul(2) {
                return Some(Self {
                    root: None,
                    indexed: Some(indexed),
                    len: canonical.len(),
                    charge,
                });
            }
        }
        Self::from_root(root, canonical.len())
    }

    fn from_root(root: Value, len: usize) -> Option<Self> {
        let charge = tree_charge(&root);
        if len < TREE_THRESHOLD || charge > len.saturating_mul(2) {
            return None;
        }
        Some(Self {
            root: Some(root),
            indexed: None,
            len,
            charge,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn charge(&self) -> usize {
        self.charge
    }

    pub(crate) fn to_vec(&self) -> Vec<u8> {
        if let Some(indexed) = &self.indexed {
            let mut bytes = Vec::with_capacity(self.len);
            indexed
                .write_range(&mut bytes, 0..indexed.base.len())
                .expect("Vec writes cannot fail");
            return bytes;
        }
        serde_json::to_vec(self.root.as_ref().unwrap()).expect("stored JSON is serializable")
    }

    pub(crate) fn write_to(&self, mut writer: impl std::io::Write) -> std::io::Result<()> {
        if let Some(indexed) = &self.indexed {
            return indexed.write_range(&mut writer, 0..indexed.base.len());
        }
        serde_json::to_writer(writer, self.root.as_ref().unwrap()).map_err(std::io::Error::other)
    }

    pub(crate) fn compact_view(&self) -> Option<&[u8]> {
        self.indexed.as_ref().and_then(|indexed| {
            indexed
                .patches
                .is_empty()
                .then_some(indexed.base.as_slice())
        })
    }

    pub(crate) fn array_len(&self, path: &[u8]) -> Result<ArrayLen, JsonError> {
        let segments = parse_path(path)?;
        let Some(root) = self.root.as_ref() else {
            return Ok(match self.get(path) {
                Ok(Some(bytes)) if bytes.first() == Some(&b'[') => {
                    ArrayLen::Len(count_items(&bytes))
                }
                Ok(Some(_)) => ArrayLen::NotArray,
                Ok(None) | Err(JsonError::MissingRoot) => ArrayLen::Missing,
                Err(error) => return Err(error),
            });
        };
        let mut node = root;
        for segment in segments {
            let next = match segment {
                Segment::Field(field) => node.as_object().and_then(|map| map.get(field)),
                Segment::Index(index) => node.as_array().and_then(|array| array.get(index)),
            };
            let Some(next) = next else {
                return Ok(ArrayLen::Missing);
            };
            node = next;
        }
        Ok(node
            .as_array()
            .map_or(ArrayLen::NotArray, |array| ArrayLen::Len(array.len())))
    }

    pub(crate) fn get(&self, path: &[u8]) -> Result<Option<Vec<u8>>, JsonError> {
        let segments = parse_path(path)?;
        if let Some(indexed) = &self.indexed {
            if let Some(span) = indexed.range(path) {
                return Ok(Some(indexed.to_vec(span.clone())));
            }
            return get(&self.to_vec(), path)?
                .ok_or(JsonError::MissingRoot)
                .map(Some);
        }
        let mut node = self.root.as_ref().unwrap();
        for segment in segments {
            node = match segment {
                Segment::Field(field) => node.as_object().and_then(|map| map.get(field)),
                Segment::Index(index) => node.as_array().and_then(|array| array.get(index)),
            }
            .ok_or(JsonError::MissingRoot)?;
        }
        encode(node).map(Some)
    }

    /// Structural edit confined to the nearest small indexed ancestor: only that span is
    /// parsed and rewritten, the rest of the body is untouched. `None` means the caller
    /// must use the general rebuild (no small indexed ancestor, patch cap reached, or the
    /// edit does not reach an existing target).
    fn prepare_splice(
        &self,
        indexed: &IndexedDocument,
        path: &[u8],
        value: Option<&[u8]>,
        parsed: Option<&Value>,
        merge: bool,
    ) -> Result<Option<TreeEdit>, JsonError> {
        const SPLICE_LIMIT: usize = 8192;
        if indexed.patches.len() >= 64 {
            return Ok(None);
        }
        // Delete needs the parent of the target; set and merge may use the target itself.
        let cuts = path
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, byte)| matches!(byte, b'.' | b'['))
            .map(|(index, _)| index);
        let own = value.is_some().then_some(path.len());
        let Some((prefix, span)) = own
            .into_iter()
            .chain(cuts)
            .find_map(|cut| indexed.range(&path[..cut]).map(|span| (cut, span.clone())))
        else {
            return Ok(None);
        };
        let old = indexed.to_vec(span.clone());
        if old.len() > SPLICE_LIMIT
            || !matches!(old.first(), Some(b'{' | b'[')) && prefix != path.len()
        {
            return Ok(None);
        }
        // A scalar target is the scalar patch path's job; this one is for structure.
        if prefix == path.len() && !matches!(old.first(), Some(b'{' | b'[')) {
            return Ok(None);
        }
        let mut relative = Vec::with_capacity(1 + path.len() - prefix);
        relative.push(b'$');
        relative.extend_from_slice(&path[prefix..]);
        let updated = match value {
            Some(value) if merge => crate::json::merge(Some(&old), &relative, value)?,
            Some(_) if prefix == path.len() => Some(encode(parsed.expect("parsed value"))?),
            Some(value) => set(Some(&old), &relative, value)?,
            None => del(&old, &relative)?,
        };
        let Some(replacement) = updated else {
            return Ok(None);
        };
        if replacement.len() > SPLICE_LIMIT {
            return Ok(None);
        }
        let len = self.len - old.len() + replacement.len();
        let charge = self.charge + replacement.capacity() + std::mem::size_of::<(usize, Vec<u8>)>();
        Ok(Some(TreeEdit {
            path: Vec::new(),
            replacement: None,
            indexed: Some(IndexedEdit::Splice {
                start: span.start,
                end: span.end,
                replacement,
            }),
            len,
            charge,
        }))
    }

    /// Prepare only the selected subtree; the document is not changed before WAL admission.
    pub(crate) fn prepare(
        &self,
        path: &[u8],
        value: Option<&[u8]>,
        merge: bool,
    ) -> Result<Option<TreeEdit>, JsonError> {
        let segments = parse_path(path)?;
        let parsed = value.map(parse_document).transpose()?;
        if let Some(indexed) = &self.indexed {
            if let (Some(span), Some(parsed)) = (indexed.range(path), parsed.as_ref()) {
                let old = &indexed.base[span.clone()];
                if !matches!(old.first(), Some(b'{' | b'['))
                    && !matches!(parsed, Value::Object(_) | Value::Array(_))
                {
                    let replacement = encode(parsed)?;
                    if replacement.len() <= 8192 && indexed.patches.len() < 64 {
                        let existing = indexed
                            .patches
                            .iter()
                            .find(|(start, _)| *start == span.start);
                        let previous_len = existing.map_or(span.len(), |(_, bytes)| bytes.len());
                        let len = self.len - previous_len + replacement.len();
                        let charge = self.charge
                            - existing.map_or(0, |(_, bytes)| bytes.capacity())
                            + replacement.capacity()
                            + if existing.is_none() {
                                std::mem::size_of::<(usize, Vec<u8>)>()
                            } else {
                                0
                            };
                        return Ok(Some(TreeEdit {
                            path: Vec::new(),
                            replacement: None,
                            indexed: Some(IndexedEdit::Replace {
                                start: span.start,
                                replacement,
                            }),
                            len,
                            charge,
                        }));
                    }
                }
            }
            if let Some(edit) = self.prepare_splice(indexed, path, value, parsed.as_ref(), merge)? {
                return Ok(Some(edit));
            }
            let compact = self.to_vec();
            let result = match value {
                Some(value) if merge => crate::json::merge(Some(&compact), path, value)?,
                Some(value) => set(Some(&compact), path, value)?,
                None => del(&compact, path)?,
            };
            let Some(result) = result else {
                let parent_end = path
                    .iter()
                    .rposition(|byte| matches!(byte, b'.' | b'['))
                    .unwrap_or(1);
                if get(&compact, &path[..parent_end])?.is_none() {
                    return Err(JsonError::MissingRoot);
                }
                return Ok(None);
            };
            let rebuilt = Self::from_bytes(&result).unwrap_or_else(|| {
                let root = parse_document(&result).expect("validated JSON");
                let charge = tree_charge(&root);
                Self {
                    root: Some(root),
                    indexed: None,
                    len: result.len(),
                    charge,
                }
            });
            return Ok(Some(TreeEdit {
                path: Vec::new(),
                replacement: None,
                len: rebuilt.len,
                charge: rebuilt.charge,
                indexed: Some(IndexedEdit::Rebuild(Box::new(rebuilt))),
            }));
        }
        let Some((last, parents)) = segments.split_last() else {
            return Err(JsonError::Path);
        };
        let mut parent = self.root.as_ref().unwrap();
        for &segment in parents {
            parent = match segment {
                Segment::Field(field) => parent.as_object().and_then(|map| map.get(field)),
                Segment::Index(index) => parent.as_array().and_then(|array| array.get(index)),
            }
            .ok_or(JsonError::MissingRoot)?;
        }
        let old = match last {
            Segment::Field(field) => parent.as_object().and_then(|map| map.get(*field)),
            Segment::Index(index) => parent.as_array().and_then(|array| array.get(*index)),
        };
        // A missing intermediate node, or a missing array index, is a no-op.
        if (matches!(last, Segment::Field(_)) && !parent.is_object())
            || (matches!(last, Segment::Index(index) if old.is_none()
                && !(value.is_some() && !merge && parent.as_array().is_some_and(|a| a.len() == *index))))
            || (value.is_none() && old.is_none())
        {
            return Ok(None);
        }
        let replacement = parsed.map(|parsed| {
            if merge {
                merge_patch(old.cloned(), parsed)
            } else {
                parsed
            }
        });
        let old_len = old.map_or(0, |value| encode(value).expect("stored JSON").len());
        let new_len = replacement
            .as_ref()
            .map_or(0, |value| encode(value).expect("stored JSON").len());
        let old_charge = old.map_or(0, tree_charge);
        let new_charge = replacement.as_ref().map_or(0, tree_charge);
        let (separator_len, field_charge) = match last {
            Segment::Field(field) => {
                let field_len = serde_json::to_string(field).expect("valid field").len() + 1;
                let field_charge =
                    field.len() + std::mem::size_of::<String>() + 3 * std::mem::size_of::<usize>();
                (field_len, field_charge)
            }
            Segment::Index(_) => (0, 0),
        };
        let (len, charge) = match (old, replacement.as_ref()) {
            (Some(_), Some(_)) => (
                self.len - old_len + new_len,
                self.charge.saturating_sub(old_charge) + new_charge,
            ),
            (None, Some(_)) => (
                self.len
                    + new_len
                    + separator_len
                    + usize::from(
                        parent.as_object().is_some_and(|m| !m.is_empty())
                            || parent.as_array().is_some_and(|a| !a.is_empty()),
                    ),
                self.charge + new_charge + field_charge,
            ),
            (Some(_), None) => (
                self.len
                    - old_len
                    - usize::from(match last {
                        Segment::Field(_) => parent.as_object().is_some_and(|m| m.len() > 1),
                        Segment::Index(_) => parent.as_array().is_some_and(|a| a.len() > 1),
                    })
                    - if matches!(last, Segment::Field(_)) {
                        separator_len
                    } else {
                        0
                    },
                self.charge.saturating_sub(
                    old_charge
                        + if matches!(last, Segment::Field(_)) {
                            field_charge
                        } else {
                            0
                        },
                ),
            ),
            (None, None) => unreachable!(),
        };
        let path = segments
            .iter()
            .map(|segment| match segment {
                Segment::Field(name) => OwnedSegment::Field((*name).to_owned()),
                Segment::Index(index) => OwnedSegment::Index(*index),
            })
            .collect();
        Ok(Some(TreeEdit {
            path,
            replacement,
            indexed: None,
            len,
            charge,
        }))
    }

    pub(crate) fn apply(&mut self, edit: TreeEdit) {
        if let Some(indexed_edit) = edit.indexed {
            match indexed_edit {
                IndexedEdit::Replace { start, replacement } => {
                    let indexed = self.indexed.as_mut().expect("indexed edit");
                    if let Some((_, bytes)) =
                        indexed.patches.iter_mut().find(|(pos, _)| *pos == start)
                    {
                        *bytes = replacement;
                    } else {
                        indexed.patches.reserve_exact(1);
                        let index = indexed.patches.partition_point(|(pos, _)| *pos < start);
                        indexed.patches.insert(index, (start, replacement));
                    }
                    self.len = edit.len;
                    self.charge = indexed.charge();
                }
                IndexedEdit::Splice {
                    start,
                    end,
                    replacement,
                } => {
                    let indexed = self.indexed.as_mut().expect("indexed edit");
                    indexed
                        .patches
                        .retain(|(pos, _)| *pos < start || *pos >= end);
                    indexed
                        .paths
                        .retain(|(_, span)| span.start <= start || span.start >= end);
                    let index = indexed.patches.partition_point(|(pos, _)| *pos < start);
                    indexed.patches.insert(index, (start, replacement));
                    self.len = edit.len;
                    self.charge = indexed.charge();
                }
                IndexedEdit::Rebuild(rebuilt) => *self = *rebuilt,
            }
            return;
        }
        let (last, parents) = edit.path.split_last().expect("non-root edit");
        let mut parent = self.root.as_mut().unwrap();
        for segment in parents {
            parent = match segment {
                OwnedSegment::Field(name) => parent.as_object_mut().unwrap().get_mut(name).unwrap(),
                OwnedSegment::Index(index) => {
                    parent.as_array_mut().unwrap().get_mut(*index).unwrap()
                }
            };
        }
        match last {
            OwnedSegment::Field(name) => {
                let map = parent.as_object_mut().unwrap();
                if let Some(value) = edit.replacement {
                    map.insert(name.clone(), value);
                } else {
                    map.shift_remove(name);
                }
            }
            OwnedSegment::Index(index) => {
                let array = parent.as_array_mut().unwrap();
                if let Some(value) = edit.replacement {
                    if *index == array.len() {
                        array.push(value);
                    } else {
                        array[*index] = value;
                    }
                } else {
                    array.remove(*index);
                }
            }
        }
        self.len = edit.len;
        self.charge = edit.charge;
    }
}

impl TreeEdit {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn charge(&self) -> usize {
        self.charge
    }
}

/// Fuzz entry: applies arbitrary path/value operations to a plain compact document and
/// to an indexed one built from the same bytes; both must agree and stay valid JSON.
#[doc(hidden)]
pub fn fuzz_ops(data: &[u8]) {
    let template = {
        let mut doc = br#"{"bulk":""#.to_vec();
        doc.resize(doc.len() + TREE_THRESHOLD, b'x');
        doc.extend_from_slice(br#"","meta":{"o":{"k":1},"tags":[1,2,3],"value":"a"},"n":5}"#);
        doc
    };
    let mut reference = template.clone();
    let Some(mut tree) = TreeDocument::from_bytes(&template) else {
        return;
    };
    for chunk in data.split(|byte| *byte == b'\n').take(64) {
        let Some((&op, rest)) = chunk.split_first() else {
            continue;
        };
        let (path, value) = match rest.iter().position(|byte| *byte == b' ') {
            Some(at) => (&rest[..at], &rest[at + 1..]),
            None => (rest, &b"null"[..]),
        };
        // The engine handles root writes itself and never asks the tree for them.
        if path == b"$" {
            continue;
        }
        let (expected, edit) = match op % 3 {
            0 => (
                set(Some(&reference), path, value),
                tree.prepare(path, Some(value), false),
            ),
            1 => (
                merge(Some(&reference), path, value),
                tree.prepare(path, Some(value), true),
            ),
            _ => (del(&reference, path), tree.prepare(path, None, false)),
        };
        match (expected, edit) {
            (Ok(Some(next)), Ok(Some(edit))) => {
                assert_eq!(edit.len(), next.len());
                tree.apply(edit);
                assert!(serde_json::from_slice::<Value>(&next).is_ok());
                reference = next;
                assert!(tree.to_vec() == reference);
            }
            (Ok(Some(_)), edit) => panic!("tree rejected a valid edit: {:?}", edit.map(|_| ())),
            (Ok(None), Ok(Some(_))) => panic!("tree accepted an unreachable edit"),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IndexedEdit, JsonError, TREE_THRESHOLD, TreeDocument, del, encode, get, has_large_string,
        merge, parse_document, set, set_root, set_root_tree,
    };

    #[test]
    fn set_root_tree_decides_eligibility_like_from_bytes() {
        let many = {
            let items: Vec<String> = (0..4000)
                .map(|i| format!("{{\"id\":{i},\"name\":\"item-{i}\",\"tags\":[\"a\",\"b\"]}}"))
                .collect();
            format!("{{\"items\":[{}]}}", items.join(","))
        };
        let big_string = format!(
            "{{\"bulk\":\"{}\",\"small\":1}}",
            "x".repeat(TREE_THRESHOLD)
        );
        let escaped = format!("{{\"bulk\":\"{}\"}}", "\\\"".repeat(TREE_THRESHOLD));
        let spaced = format!(
            "{{ \"bulk\" : \"{}\" , \"n\" : [1, 2, 3] }}",
            "y".repeat(TREE_THRESHOLD)
        );
        for document in [many, big_string, escaped, spaced, "{\"a\":1}".to_string()] {
            let (canonical, tree) = set_root_tree(document.as_bytes()).unwrap();
            assert_eq!(canonical, set_root(document.as_bytes()).unwrap());
            let expected = TreeDocument::from_bytes(&canonical);
            assert_eq!(
                tree.is_some(),
                expected.is_some(),
                "{}",
                &document[..40.min(document.len())]
            );
            if let (Some(tree), Some(expected)) = (tree, expected) {
                assert_eq!(tree.len(), expected.len());
                assert_eq!(tree.charge(), expected.charge());
                assert_eq!(tree.to_vec(), expected.to_vec());
            }
        }
        assert!(has_large_string(
            format!("[\"{}\"]", "z".repeat(TREE_THRESHOLD)).as_bytes()
        ));
        assert!(!has_large_string(b"[\"short\",\"strings\"]"));
    }

    fn result(value: Result<Option<Vec<u8>>, JsonError>) -> Option<String> {
        value
            .unwrap()
            .map(|bytes| String::from_utf8(bytes).unwrap())
    }

    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[test]
    fn indexed_structural_edits_match_the_compact_reference() {
        let paths: [&[u8]; 14] = [
            b"$.meta",
            b"$.meta.value",
            b"$.meta.tags",
            b"$.meta.tags[1]",
            b"$.meta.o",
            b"$.meta.o.k",
            b"$.meta.fresh",
            b"$.arr",
            b"$.arr[0]",
            b"$.arr[1].n",
            b"$.arr[5]",
            b"$.nope.x",
            b"$.n",
            b"$.top",
        ];
        let values: [&[u8]; 8] = [
            br#"1"#,
            br#""text""#,
            br#"{"a":1,"b":{"c":[1,2]}}"#,
            br#"[1,2,3]"#,
            br#"null"#,
            br#"{"k":null,"z":2}"#,
            br#"true"#,
            br#"{}"#,
        ];
        let mut splices = 0;
        for seed in [0x9e37_79b9_u64, 0x1234_5678_9abc_def1, 42] {
            let mut state = seed;
            let initial = serde_json::to_vec(&serde_json::json!({
                "bulk": "x".repeat(TREE_THRESHOLD),
                "meta": {"value": "a", "tags": [1, 2, 3], "o": {"k": 1}},
                "arr": [{"n": 1}, {"n": 2}],
                "n": 5,
            }))
            .unwrap();
            let mut reference = encode(&parse_document(&initial).unwrap()).unwrap();
            let mut tree = TreeDocument::from_bytes(&reference).expect("indexed document");
            assert!(tree.indexed.is_some());
            for _ in 0..if cfg!(miri) { 12 } else { 150 } {
                let path = paths[next(&mut state) as usize % paths.len()];
                let value = values[next(&mut state) as usize % values.len()];
                let op = next(&mut state) % 3;
                let expected = match op {
                    0 => set(Some(&reference), path, value),
                    1 => merge(Some(&reference), path, value),
                    _ => del(&reference, path),
                }
                .unwrap();
                let edit = match op {
                    0 => tree.prepare(path, Some(value), false),
                    1 => tree.prepare(path, Some(value), true),
                    _ => tree.prepare(path, None, false),
                };
                match (expected, edit) {
                    (Some(next_doc), Ok(Some(edit))) => {
                        if matches!(edit.indexed, Some(IndexedEdit::Splice { .. })) {
                            splices += 1;
                        }
                        assert_eq!(edit.len(), next_doc.len());
                        tree.apply(edit);
                        reference = next_doc;
                    }
                    (None, Ok(None) | Err(JsonError::MissingRoot)) => {}
                    (expected, edit) => panic!(
                        "op {op} {} {}: reference {:?}, tree {:?}",
                        String::from_utf8_lossy(path),
                        String::from_utf8_lossy(value),
                        expected.map(|doc| doc.len()),
                        edit.map(|edit| edit.map(|edit| edit.len()))
                    ),
                }
                assert!(tree.to_vec() == reference, "bytes diverged");
                assert_eq!(tree.len(), reference.len());
                for probe in paths {
                    assert_eq!(
                        tree.get(probe).ok().flatten(),
                        get(&reference, probe).ok().flatten(),
                        "read {}",
                        String::from_utf8_lossy(probe)
                    );
                }
            }
        }
        assert!(
            cfg!(miri) || splices > 30,
            "splice path barely exercised: {splices}"
        );
    }

    /// Component timing, run with `cargo test --profile release-fast --lib -- --ignored --nocapture json_structural`.
    #[test]
    #[ignore]
    fn json_structural_edit_component_timing() {
        let initial = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(1 << 20), "meta": {"value": "a"}
        }))
        .unwrap();
        let n = 2000u32;
        let mut tree = TreeDocument::from_bytes(&initial).unwrap();
        let start = std::time::Instant::now();
        for i in 0..n {
            let patch = format!(r#"{{"value":"{i:012}"}}"#);
            let edit = tree
                .prepare(b"$.meta", Some(patch.as_bytes()), true)
                .unwrap()
                .unwrap();
            tree.apply(edit);
        }
        let spliced = start.elapsed();
        let start = std::time::Instant::now();
        let mut doc = initial.clone();
        for i in 0..n / 10 {
            let patch = format!(r#"{{"value":"{i:012}"}}"#);
            doc = merge(Some(&doc), b"$.meta", patch.as_bytes())
                .unwrap()
                .unwrap();
            let _ = TreeDocument::from_bytes(&doc).unwrap();
        }
        let rebuilt = start.elapsed() * 10;
        println!(
            "1 MiB MERGE into $.meta: splice {:.0}/s, previous rebuild {:.0}/s",
            f64::from(n) / spliced.as_secs_f64(),
            f64::from(n) / rebuilt.as_secs_f64()
        );
    }

    #[test]
    fn fuzz_entry_accepts_arbitrary_operation_lines() {
        super::fuzz_ops(b"0$.meta.value \"z\"\n1$.meta {\"o\":null,\"x\":[1]}\n2$.meta.tags[1]\n0$.top 1\n9$[ \n\xff\xfe $");
        super::fuzz_ops(b"");
        let pool: [&[u8]; 10] = [
            b"$.meta",
            b"$.meta.o",
            b"$.meta.tags[0]",
            b"$.n",
            b"$.zz",
            b"$",
            b"$.meta.o.k",
            b"$[0]",
            b"$.bulk",
            b"$.meta.value",
        ];
        let mut state = 0xdead_beef_u64;
        for _ in 0..150 {
            let mut data = Vec::new();
            for _ in 0..(next(&mut state) % 20) {
                data.push(b'0' + (next(&mut state) % 3) as u8);
                data.extend_from_slice(pool[next(&mut state) as usize % pool.len()]);
                data.push(b' ');
                for _ in 0..(next(&mut state) % 12) {
                    data.push(b"{}[]\":,0aln-1 "[next(&mut state) as usize % 14]);
                }
                data.push(b'\n');
            }
            super::fuzz_ops(&data);
        }
    }

    #[test]
    fn documents_keep_the_member_order_they_were_given() {
        let source = br#"{"z":1,"a":{"y":2,"b":3},"m":[{"q":1,"c":2}]}"#;
        let stored = set_root(source).unwrap();
        assert_eq!(stored, source.to_vec());
        assert_eq!(
            result(set(Some(&stored), b"$.new", b"1")),
            Some(r#"{"z":1,"a":{"y":2,"b":3},"m":[{"q":1,"c":2}],"new":1}"#.into())
        );
        assert_eq!(
            result(merge(Some(&stored), b"$.a", br#"{"y":9,"x":8}"#)),
            Some(r#"{"z":1,"a":{"y":9,"b":3,"x":8},"m":[{"q":1,"c":2}]}"#.into())
        );
        assert_eq!(
            result(del(&stored, b"$.a.y")),
            Some(r#"{"z":1,"a":{"b":3},"m":[{"q":1,"c":2}]}"#.into())
        );
        assert_eq!(
            result(merge(None, b"$", br#"{"k":1,"b":2}"#)),
            Some(r#"{"k":1,"b":2}"#.into())
        );
    }

    #[test]
    fn structured_edits_match_compact_json_and_track_exact_lengths() {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(super::TREE_THRESHOLD),
            "nested": {"keep": true, "discard": 1},
            "array": [1, 2, 3]
        }))
        .unwrap();
        let mut tree = super::TreeDocument::from_bytes(&bytes).unwrap();
        let mut compact = bytes;
        for (path, value, is_merge) in [
            (
                b"$.nested.keep".as_slice(),
                Some(b"false".as_slice()),
                false,
            ),
            (
                b"$.nested.new".as_slice(),
                Some(br#"{"x":1}"#.as_slice()),
                false,
            ),
            (
                b"$.nested".as_slice(),
                Some(br#"{"discard":null,"new":{"y":2}}"#.as_slice()),
                true,
            ),
            (b"$.array[0]".as_slice(), None, false),
            (b"$.nested.new".as_slice(), None, false),
        ] {
            let edit = tree.prepare(path, value, is_merge).unwrap().unwrap();
            compact = match value {
                Some(value) if is_merge => merge(Some(&compact), path, value).unwrap().unwrap(),
                Some(value) => set(Some(&compact), path, value).unwrap().unwrap(),
                None => del(&compact, path).unwrap().unwrap(),
            };
            assert_eq!(edit.len(), compact.len());
            tree.apply(edit);
            assert_eq!(tree.to_vec(), compact);
            assert_eq!(
                tree.get(b"$.nested").unwrap(),
                get(&compact, b"$.nested").unwrap()
            );
        }
        assert!(matches!(
            tree.prepare(b"$.missing.deep", Some(b"1"), false),
            Err(JsonError::MissingRoot)
        ));
        assert_eq!(
            tree.prepare(b"$.missing.deep", Some(b"oops"), false).err(),
            Some(JsonError::Syntax)
        );
        assert_eq!(tree.get(b"$.missing"), Err(JsonError::MissingRoot));
    }

    #[test]
    fn indexed_document_preserves_root_and_parent_after_repeated_scalar_edits() {
        let original = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(super::TREE_THRESHOLD),
            "nested": {"value": 1, "other": "snow 雪"},
        }))
        .unwrap();
        let mut document = super::TreeDocument::from_bytes(&original).unwrap();
        assert_eq!(document.compact_view(), Some(original.as_slice()));
        assert!(document.charge() < original.len() * 2);
        for replacement in ["\"changed\\n雪\"".as_bytes(), b"true", b"0"] {
            let before = document.to_vec();
            let edit = document
                .prepare(b"$.nested.value", Some(replacement), false)
                .unwrap()
                .unwrap();
            assert_eq!(document.to_vec(), before);
            document.apply(edit);
            assert!(document.compact_view().is_none());
            assert_eq!(document.to_vec().len(), document.len());
            let parent = document.get(b"$.nested").unwrap().unwrap();
            let expected: serde_json::Value = serde_json::from_slice(&document.to_vec()).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&parent).unwrap(),
                expected["nested"]
            );
            assert_eq!(
                document.get(b"$.nested.value").unwrap().unwrap(),
                replacement
            );
        }
        let edit = document
            .prepare(b"$.nested.other", Some(b"\"changed\""), false)
            .unwrap()
            .unwrap();
        document.apply(edit);
        let updated: serde_json::Value = serde_json::from_slice(&document.to_vec()).unwrap();
        assert_eq!(updated["nested"]["other"], "changed");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &document.get(b"$.nested").unwrap().unwrap()
            )
            .unwrap(),
            updated["nested"]
        );
        let mut wire = Vec::new();
        document.write_to(&mut wire).unwrap();
        assert_eq!(wire, document.to_vec());
        let edit = document
            .prepare(b"$.nested.added", Some(b"9"), false)
            .unwrap()
            .unwrap();
        document.apply(edit);
        assert_eq!(
            document.get(b"$.nested.added").unwrap(),
            Some(b"9".to_vec())
        );
    }

    #[test]
    fn root_and_escaped_values() {
        let source = br#" { "text": "a\n\"b\"", "nested": [true, null] } "#;
        assert_eq!(
            result(get(source, b"$")),
            Some(r#"{"text":"a\n\"b\"","nested":[true,null]}"#.into())
        );
        assert_eq!(result(get(source, b"$.text")), Some(r#""a\n\"b\"""#.into()));
        assert_eq!(
            result(set(None, b"$", br#" { "x": 1 } "#)),
            Some(r#"{"x":1}"#.into())
        );
        assert_eq!(
            result(set(Some(b"null"), b"$", b"false")),
            Some("false".into())
        );
        assert_eq!(result(get(b"null", b"$")), Some("null".into()));
        assert_eq!(result(del(b"null", b"$")), None);
    }

    #[test]
    fn path_reads_match_materialized_json_for_compact_documents() {
        let value = serde_json::json!({
            "a\\b": {"nested": [false, {"value": "é\\n\""}]},
            "items": [0, null, {"name": "ok", "list": [1, 2]}],
            "other": {"large": "x".repeat(32768)},
            "null": null,
        });
        let document = serde_json::to_vec(&value).unwrap();
        let cases = [
            (
                br#"$.a\b.nested[1].value"#.as_slice(),
                Some(serde_json::json!("é\\n\"")),
            ),
            (
                b"$.items[2]",
                Some(serde_json::json!({"name": "ok", "list": [1, 2]})),
            ),
            (b"$.items[2].list[0]", Some(serde_json::json!(1))),
            (b"$.items[1]", Some(serde_json::Value::Null)),
            (b"$.items[5]", None),
            (b"$.items[0].name", None),
            (b"$.null.x", None),
            (
                b"$.other",
                Some(serde_json::json!({"large": "x".repeat(32768)})),
            ),
        ];
        for (path, expected) in cases {
            let actual = get(&document, path).unwrap();
            assert_eq!(
                actual.as_deref(),
                expected
                    .as_ref()
                    .map(|v| serde_json::to_vec(v).unwrap())
                    .as_deref(),
                "path: {}",
                String::from_utf8_lossy(path)
            );
        }
        // Stored documents are canonical, so a key never repeats and the first match is the one.
    }

    #[test]
    fn root_replacement_only_parses_the_new_document() {
        assert_eq!(set_root(br#" { "ok": true } "#).unwrap(), br#"{"ok":true}"#);
        assert_eq!(set_root(b"invalid"), Err(JsonError::Syntax));
    }

    #[test]
    fn nested_arrays_and_object_fields() {
        let document = r#"{"rows":[{"0":"old","label":"é"},[1,2,3]]}"#.as_bytes();
        assert_eq!(
            result(get(document, b"$.rows[0].0")),
            Some(r#""old""#.into())
        );
        assert_eq!(
            result(get(document, b"$.rows[0].label")),
            Some(r#""é""#.into())
        );
        assert_eq!(
            result(set(Some(document), b"$.rows[0].0", br#""new""#)),
            Some(r#"{"rows":[{"0":"new","label":"é"},[1,2,3]]}"#.into())
        );
        assert_eq!(
            result(set(Some(document), b"$.rows[0].added", b"null")),
            Some(r#"{"rows":[{"0":"old","label":"é","added":null},[1,2,3]]}"#.into())
        );
        assert_eq!(
            result(set(Some(document), b"$.rows[1][1]", b"42")),
            Some(r#"{"rows":[{"0":"old","label":"é"},[1,42,3]]}"#.into())
        );
        assert_eq!(
            result(del(document, b"$.rows[1][0]")),
            Some(r#"{"rows":[{"0":"old","label":"é"},[2,3]]}"#.into())
        );
        assert_eq!(
            result(del(document, b"$.rows[0].0")),
            Some(r#"{"rows":[{"label":"é"},[1,2,3]]}"#.into())
        );
    }

    #[test]
    fn unicode_fields_and_missing_targets() {
        let document = "{\"şehir\":{\"値\":5}}".as_bytes();
        assert_eq!(
            result(get(document, "$.şehir.値".as_bytes())),
            Some("5".into())
        );
        assert_eq!(
            result(set(Some(document), "$.şehir.新".as_bytes(), b"true")),
            Some(r#"{"şehir":{"値":5,"新":true}}"#.into())
        );
        assert_eq!(result(get(document, b"$.missing")), None);
        assert_eq!(result(del(document, b"$.missing")), None);
        assert_eq!(result(set(Some(document), b"$.missing.next", b"1")), None);
        assert_eq!(result(set(Some(b"null"), b"$.field", b"1")), None);
        assert_eq!(result(get(b"null", b"$.field")), None);
        assert_eq!(result(del(b"null", b"$.field")), None);
        // Writing at index == length appends; further out is a miss.
        assert_eq!(
            result(set(Some(b"[1]"), b"$[1]", b"2")),
            Some("[1,2]".into())
        );
        assert_eq!(result(set(Some(b"[]"), b"$[0]", b"2")), Some("[2]".into()));
        assert_eq!(result(set(Some(b"[1]"), b"$[2]", b"2")), None);
        assert_eq!(
            result(set(
                Some(br#"{"a":["x,]\"",[1,2]],"b":1}"#),
                b"$.a[2]",
                b"{}"
            )),
            Some(r#"{"a":["x,]\"",[1,2],{}],"b":1}"#.into())
        );
        assert_eq!(result(del(b"[1]", b"$[1]")), None);
        assert_eq!(result(get(b"[1]", b"$[1]")), None);
        assert_eq!(result(set(Some(b"[1]"), b"$[0].x", b"2")), None);
        assert_eq!(set(None, b"$.x", b"1"), Err(JsonError::MissingRoot));
    }

    #[test]
    fn existing_path_updates_keep_the_large_document_canonical() {
        let source = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(65536),
            "nested": {"array": [1, {"leaf": 2}], "drop": 1},
        }))
        .unwrap();
        let replaced = set(
            Some(&source),
            b"$.nested.array[1].leaf",
            br#""quoted\\n\"""#,
        )
        .unwrap()
        .unwrap();
        let expected = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(65536),
            "nested": {"array": [1, {"leaf": "quoted\\n\""}], "drop": 1},
        }))
        .unwrap();
        assert_eq!(replaced, expected);
        let merged = merge(
            Some(&replaced),
            b"$.nested",
            br#"{"drop":null,"array":[null,{"leaf":true}]}"#,
        )
        .unwrap()
        .unwrap();
        let expected = serde_json::to_vec(&serde_json::json!({
            "bulk": "x".repeat(65536),
            "nested": {"array": [null, {"leaf": true}]},
        }))
        .unwrap();
        assert_eq!(merged, expected);
    }

    #[test]
    fn merge_matches_rfc_7396_examples() {
        let cases = [
            (r#"{"a":"b"}"#, r#"{"a":"c"}"#, r#"{"a":"c"}"#),
            (r#"{"a":"b"}"#, r#"{"b":"c"}"#, r#"{"a":"b","b":"c"}"#),
            (r#"{"a":"b"}"#, r#"{"a":null}"#, r#"{}"#),
            (r#"{"a":["b"]}"#, r#"{"a":"c"}"#, r#"{"a":"c"}"#),
            (
                r#"{"a":{"b":"c"}}"#,
                r#"{"a":{"b":"d","c":null}}"#,
                r#"{"a":{"b":"d"}}"#,
            ),
            (r#"{"a":[{"b":"c"}]}"#, r#"{"a":[1]}"#, r#"{"a":[1]}"#),
            (r#"["a","b"]"#, r#"["c","d"]"#, r#"["c","d"]"#),
            (r#"{"a":"foo"}"#, "null", "null"),
            (r#"{"a":"foo"}"#, r#""bar""#, r#""bar""#),
            (r#"{"e":null}"#, r#"{"a":1}"#, r#"{"e":null,"a":1}"#),
            (r#"[1,2]"#, r#"{"a":"b","c":null}"#, r#"{"a":"b"}"#),
            (r#"{}"#, r#"{"a":{"b":{"c":null}}}"#, r#"{"a":{"b":{}}}"#),
        ];
        for (target, patch, expected) in cases {
            let actual = merge(Some(target.as_bytes()), b"$", patch.as_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&actual).unwrap(),
                serde_json::from_str::<serde_json::Value>(expected).unwrap(),
                "target={target}, patch={patch}"
            );
        }
    }

    #[test]
    fn merge_simple_paths_and_missing_root() {
        let source = br#"{"a":{"drop":2,"keep":1},"rows":[1,2]}"#;
        assert_eq!(
            result(merge(Some(source), b"$.a", br#"{"drop":null,"add":3}"#)),
            Some(r#"{"a":{"keep":1,"add":3},"rows":[1,2]}"#.into())
        );
        assert_eq!(
            result(merge(Some(source), b"$.rows[0]", br#"{"x":1}"#)),
            Some(r#"{"a":{"drop":2,"keep":1},"rows":[{"x":1},2]}"#.into())
        );
        assert_eq!(
            result(merge(Some(source), b"$.added", b"null")),
            Some(r#"{"a":{"drop":2,"keep":1},"rows":[1,2],"added":null}"#.into())
        );
        assert_eq!(result(merge(Some(source), b"$.missing.child", b"1")), None);
        assert_eq!(result(merge(Some(source), b"$.rows[2]", b"1")), None);
        assert_eq!(merge(None, b"$.a", b"1"), Err(JsonError::MissingRoot));
        assert_eq!(
            result(merge(None, b"$", br#"{"a":null,"b":1}"#)),
            Some(r#"{"b":1}"#.into())
        );
        assert_eq!(result(merge(None, b"$", b"null")), Some("null".into()));
        assert_eq!(
            result(merge(Some(b"5"), b"$", br#"{"a":null}"#)),
            Some("{}".into())
        );
        assert_eq!(
            merge(Some(source), b"$.a", b"invalid"),
            Err(JsonError::Syntax)
        );
        assert_eq!(merge(Some(source), b"$.a]", b"1"), Err(JsonError::Path));
        assert_eq!(merge(Some(b"invalid"), b"$", b"1"), Err(JsonError::Syntax));
    }

    #[test]
    fn invalid_paths_are_rejected_in_full() {
        for path in [
            &b""[..],
            b"x",
            b"$x",
            b"$.",
            b"$..x",
            b"$.x.",
            b"$[",
            b"$[]",
            b"$[-1]",
            b"$[+1]",
            b"$[1",
            b"$[1]tail",
            b"$[0:1]",
            b"$[9999999999999999999999999999999999]",
            b"$['key']",
            b"$[\"key\"]",
            b"$[*]",
            b"$.*",
            b"$..field",
            b"$.a?b",
            b"$.a]",
            b"$.a b",
            b"$.a[0]bad",
            b"$.a[0][?x]",
            b"$.a\xff",
        ] {
            assert_eq!(get(b"{}", path), Err(JsonError::Path), "{path:?}");
            assert_eq!(
                set(Some(b"{}"), path, b"1"),
                Err(JsonError::Path),
                "{path:?}"
            );
            assert_eq!(del(b"{}", path), Err(JsonError::Path), "{path:?}");
        }
    }

    #[test]
    fn invalid_json_is_rejected_even_for_missing_paths() {
        for invalid in [&b""[..], b"{", b"1 2", b"[1,]", b"NaN"] {
            // Reads walk the bytes of documents that were validated when stored, so only the
            // truncated inputs are noticed here; every write parses the whole document.
            if invalid.is_empty() || invalid == b"{" {
                assert_eq!(get(invalid, b"$.missing"), Err(JsonError::Syntax));
            }
            assert_eq!(set(Some(invalid), b"$", b"1"), Err(JsonError::Syntax));
            assert_eq!(del(invalid, b"$"), Err(JsonError::Syntax));
        }
        assert_eq!(set(None, b"$", b"{invalid"), Err(JsonError::Syntax));
        assert_eq!(set(Some(b"{}"), b"$.x", b"1 2"), Err(JsonError::Syntax));
    }
}
