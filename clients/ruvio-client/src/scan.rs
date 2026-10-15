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

/// An iterator over a `*SCAN` command. Yields `Err` once and stops if a page fails.
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

    /// The cursor the next page will be requested with. 0 after the last page.
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
    /// Walks every key of the database with `SCAN`, one page at a time.
    ///
    /// ```no_run
    /// # let mut db = ruvio_client::Client::connect("127.0.0.1", 6379)?;
    /// for key in db.scan_iter(Some("session:*"), Some(500)) {
    ///     println!("{}", key?);
    /// }
    /// # Ok::<(), ruvio_client::Error>(())
    /// ```
    pub fn scan_iter(&mut self, pattern: Option<&str>, count: Option<u64>) -> ScanIter<'_, String> {
        ScanIter::new(self, "SCAN", None, pattern, count, keep)
    }

    /// Walks a hash with `HSCAN`, yielding `(field, value)`.
    pub fn hscan_iter(
        &mut self,
        key: &str,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> ScanIter<'_, (String, String)> {
        ScanIter::new(self, "HSCAN", Some(key), pattern, count, pairs)
    }

    /// Walks a set with `SSCAN`.
    pub fn sscan_iter(
        &mut self,
        key: &str,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> ScanIter<'_, String> {
        ScanIter::new(self, "SSCAN", Some(key), pattern, count, keep)
    }

    /// Walks a sorted set with `ZSCAN`, yielding members with scores.
    pub fn zscan_iter(
        &mut self,
        key: &str,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> ScanIter<'_, SortedSetEntry> {
        ScanIter::new(self, "ZSCAN", Some(key), pattern, count, entries_from_flat)
    }
}
