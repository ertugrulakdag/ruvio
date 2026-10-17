//! Lazy, cursor-driven iterators over `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN`.
//!
//! Each iterator holds one page at a time and asks for the next page only when the current
//! one is used up, so walking a large keyspace keeps memory flat. Like the commands, a key
//! that changes during the walk may be returned twice or missed; filter duplicates if that
//! matters.

use crate::client::Client;
use crate::error::Error;
use crate::extra::{entries_from_flat, pair_up};
use crate::models::SortedSetEntry;

/// Turns one page of raw strings into items.
type Decode<T> = fn(Vec<String>) -> Result<Vec<T>, Error>;

/// A lazy iterator over `SCAN`, `HSCAN`, `SSCAN` or `ZSCAN`, created by
/// [`Client::scan_iter`], [`Client::hscan_iter`], [`Client::sscan_iter`] or
/// [`Client::zscan_iter`].
///
/// It holds one page at a time and asks the server for the next page only when the current
/// one is used up, so walking a large keyspace uses flat memory. It borrows the client
/// mutably, so nothing else can use the client until the iterator is dropped. Each item is a
/// `Result`; after the first `Err` the iterator ends.
///
/// As with Redis, an element that changes during the walk may be returned twice or missed,
/// and the same element can repeat after a table rebuild, so de-duplicate if that matters.
/// An element present for the whole walk is returned at least once.
pub struct ScanIter<'a, T> {
    client: &'a mut Client,
    command: &'static str,
    key: Option<String>,
    pattern: Option<String>,
    count: Option<u64>,
    cursor: u64,
    finished: bool,
    page: std::vec::IntoIter<T>,
    decode: Decode<T>,
}

impl<'a, T> ScanIter<'a, T> {
    fn new(
        client: &'a mut Client,
        command: &'static str,
        key: Option<&str>,
        pattern: Option<&str>,
        count: Option<u64>,
        decode: Decode<T>,
    ) -> Self {
        Self {
            client,
            command,
            key: key.map(str::to_owned),
            pattern: pattern.map(str::to_owned),
            count,
            cursor: 0,
            finished: false,
            page: Vec::new().into_iter(),
            decode,
        }
    }

    /// The cursor the next page will be requested with. `0` before the first page is fetched
    /// and again after the last one. Cursors are opaque; on a sharded server the client
    /// combines the shard number into it.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
}

impl<T> Iterator for ScanIter<'_, T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.page.next() {
                return Some(Ok(item));
            }

            if self.finished {
                return None;
            }

            let fetched = self.client.scan_page(
                self.command,
                self.key.as_deref(),
                self.cursor,
                self.pattern.as_deref(),
                self.count,
            );

            match fetched.and_then(|(cursor, raw)| Ok((cursor, (self.decode)(raw)?))) {
                Ok((cursor, items)) => {
                    self.cursor = cursor;
                    self.finished = cursor == 0;
                    self.page = items.into_iter();
                }
                Err(error) => {
                    self.finished = true;

                    return Some(Err(error));
                }
            }
        }
    }
}

fn keep(raw: Vec<String>) -> Result<Vec<String>, Error> {
    Ok(raw)
}

fn pairs(raw: Vec<String>) -> Result<Vec<(String, String)>, Error> {
    Ok(pair_up(raw))
}

impl Client {
    /// Walks every key of the selected database with `SCAN`, one page at a time.
    ///
    /// On a sharded server the iterator visits each shard in turn, so it covers the whole
    /// keyspace. On a client opened with `discover_cluster: false` it sees only the shard it is
    /// connected to.
    ///
    /// # Arguments
    /// * `pattern` - optional glob (`*`, `?`, `[a-z]`) applied to key names (`MATCH`). `None`
    ///   returns every key. The pattern is applied after the page is read, so a page may be
    ///   empty while the walk continues.
    /// * `count` - optional hint for how many index slots to examine per page (`COUNT`), not a
    ///   limit on results. The server default is 10.
    ///
    /// # Returns
    /// An iterator of `Result<String, Error>` (key names).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// for key in client.scan_iter(Some("session:*"), Some(500)) {
    ///     println!("{}", key?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan_iter(&mut self, pattern: Option<&str>, count: Option<u64>) -> ScanIter<'_, String> {
        ScanIter::new(self, "SCAN", None, pattern, count, keep)
    }

    /// Walks one hash with `HSCAN`, yielding `(field, value)` pairs.
    ///
    /// # Arguments
    /// * `key` - the hash. A missing key yields nothing.
    /// * `pattern` - optional glob on field names. `count` - page size hint, as for
    ///   [`Client::scan_iter`].
    ///
    /// # Errors
    /// Items are `Err(Error::Server)` with `WRONGTYPE` when `key` is not a hash.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// for pair in client.hscan_iter("user:1", None, None) {
    ///     let (field, value) = pair?;
    ///     println!("{field}={value}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn hscan_iter(
        &mut self,
        key: &str,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> ScanIter<'_, (String, String)> {
        ScanIter::new(self, "HSCAN", Some(key), pattern, count, pairs)
    }

    /// Walks one set with `SSCAN`, yielding members. Arguments as for [`Client::hscan_iter`];
    /// `pattern` matches member names.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// for member in client.sscan_iter("tags", Some("rust*"), None) {
    ///     println!("{}", member?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn sscan_iter(
        &mut self,
        key: &str,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> ScanIter<'_, String> {
        ScanIter::new(self, "SSCAN", Some(key), pattern, count, keep)
    }

    /// Walks one sorted set with `ZSCAN`, yielding a [`SortedSetEntry`] (member and score) per
    /// element. Arguments as for [`Client::hscan_iter`]; `pattern` matches member names.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// for entry in client.zscan_iter("leaderboard", None, Some(100)) {
    ///     let entry = entry?;
    ///     println!("{} {}", entry.member, entry.score);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn zscan_iter(
        &mut self,
        key: &str,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> ScanIter<'_, SortedSetEntry> {
        ScanIter::new(self, "ZSCAN", Some(key), pattern, count, entries_from_flat)
    }
}
