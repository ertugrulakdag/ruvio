//! Convenience helpers built on Ruvio's native commands: a distributed lock with an RAII
//! guard, fenced leases, semaphore permits, rate limiting, stampede-safe cache-aside,
//! run-once (idempotency), capped "recent items" lists and Bloom "seen before" checks.
//!
//! Every helper is a thin layer over the typed methods; nothing here keeps state on the
//! server that the plain commands would not.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::client::{Client, SetMode};
use crate::error::Error;
use crate::models::{FillOutcome, IdemState, RateLimit};

/// First and largest wait between polls of a busy lock or cache fill.
const POLL_FIRST: Duration = Duration::from_millis(2);
const POLL_MAX: Duration = Duration::from_millis(50);

/// A random 128-bit lock token as 32 hex characters. Unique per call (seeded from the
/// standard library's per-process random hash keys, the clock, the process id and a counter);
/// it identifies the holder, it is not a secret.
pub fn new_token() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let mut halves = [0_u64; 2];

    for (salt, half) in halves.iter_mut().enumerate() {
        let mut hasher = RandomState::new().build_hasher();

        hasher.write_u64(count);
        hasher.write_u128(nanos);
        hasher.write_u32(std::process::id());
        hasher.write_usize(salt);
        *half = hasher.finish();
    }

    format!("{:016x}{:016x}", halves[0], halves[1])
}

/// A held distributed lock. Derefs to the [`Client`], so commands can run while it is held.
/// Dropping it releases the lock with `DELEX key IFEQ token`, which never deletes a lock that
/// expired and was taken by someone else. Call [`LockGuard::release`] to see the result.
pub struct LockGuard<'a> {
    client: &'a mut Client,
    key: String,
    token: String,
    released: bool,
}

impl LockGuard<'_> {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// This holder's token.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Extends the TTL with `SET key token IFEQ token PX ttl`. False when the lock was lost.
    pub fn renew(&mut self, ttl: Duration) -> Result<bool, Error> {
        self.client
            .set_if_eq(&self.key, &self.token, &self.token, Some(ttl))
    }

    /// Releases now. False when the lock had already expired (or was taken by another holder).
    pub fn release(mut self) -> Result<bool, Error> {
        self.released = true;
        self.client.delex(&self.key, &self.token)
    }
}

impl Deref for LockGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        self.client
    }
}

impl DerefMut for LockGuard<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Best effort: the TTL frees the lock anyway if this cannot reach the server.
            let _ = self.client.delex(&self.key, &self.token);
        }
    }
}

/// A `LEASE` grant. Derefs to the [`Client`]. Dropping it sends `RELEASE key token`.
/// Pass [`LeaseGuard::fence`] to the resource you protect so it can refuse stale holders.
pub struct LeaseGuard<'a> {
    client: &'a mut Client,
    key: String,
    token: i64,
    released: bool,
}

impl LeaseGuard<'_> {
    /// The fencing token: strictly increasing per key across grants.
    pub fn fence(&self) -> i64 {
        self.token
    }

    /// Releases now. False when the lease had expired or moved to another holder.
    pub fn release(mut self) -> Result<bool, Error> {
        self.released = true;
        Ok(self.client.release(&self.key, self.token as u64)? > 0)
    }
}

impl Deref for LeaseGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        self.client
    }
}

impl DerefMut for LeaseGuard<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
    }
}

impl Drop for LeaseGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.client.release(&self.key, self.token as u64);
        }
    }
}

/// One `SEMAPHORE` slot. Derefs to the [`Client`]. Dropping it sends `RELEASE key token`.
pub struct SemaphorePermit<'a> {
    client: &'a mut Client,
    key: String,
    token: i64,
    released: bool,
}

impl SemaphorePermit<'_> {
    /// The holder token.
    pub fn token(&self) -> i64 {
        self.token
    }

    /// Frees the slot now. False when the slot had already expired.
    pub fn release(mut self) -> Result<bool, Error> {
        self.released = true;
        Ok(self.client.release(&self.key, self.token as u64)? > 0)
    }
}

impl Deref for SemaphorePermit<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        self.client
    }
}

impl DerefMut for SemaphorePermit<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.client.release(&self.key, self.token as u64);
        }
    }
}

/// [`Client::get_or_create_with`] settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheOptions {
    /// Fresh lifetime of the stored value.
    pub ttl: Duration,
    /// How long the fill marker lives: the longest the factory may take before another caller
    /// takes over.
    pub fill_timeout: Duration,
    /// Serve the old value this long past `ttl` while one caller refreshes (`FILLSET ... SWR`).
    pub stale: Option<Duration>,
    /// How long a caller that sees `BUSY` keeps polling before giving up with a timeout.
    pub max_wait: Duration,
}

impl CacheOptions {
    /// `ttl` with a 10 s fill timeout, no stale window and a 10 s wait.
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            fill_timeout: Duration::from_secs(10),
            stale: None,
            max_wait: Duration::from_secs(10),
        }
    }
}

/// What [`Client::idempotent`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdemOutcome {
    /// This call ran the work and published its result.
    Executed(Vec<u8>),
    /// The work had already finished; this is the stored result.
    Replayed(Vec<u8>),
    /// Another owner is running it now. The work was not run.
    Pending,
    /// The key holds a different fingerprint. The work was not run.
    Conflict,
}

impl Client {
    // ---- distributed lock ------------------------------------------------------------------

    /// `SET key token NX PX ttl`: takes the lock for `token`. False when someone holds it.
    pub fn acquire_lock(&mut self, key: &str, token: &str, ttl: Duration) -> Result<bool, Error> {
        self.set_with(key, token, Some(ttl), SetMode::IfNotExists)
    }

    /// `SET key token IFEQ token PX ttl`: extends the lock only while `token` still holds it.
    pub fn renew_lock(&mut self, key: &str, token: &str, ttl: Duration) -> Result<bool, Error> {
        self.set_if_eq(key, token, token, Some(ttl))
    }

    /// `DELEX key IFEQ token`: releases only the caller's own lock.
    pub fn release_lock(&mut self, key: &str, token: &str) -> Result<bool, Error> {
        self.delex(key, token)
    }

    /// Tries once to take `key` with a fresh token. `None` when it is held.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # let mut db = ruvio_client::Client::connect("127.0.0.1", 6379)?;
    /// if let Some(mut lock) = db.try_lock("lock:report", Duration::from_secs(30))? {
    ///     lock.set("report:status", "running")?; // the guard derefs to the client
    /// } // released here with DELEX IFEQ
    /// # Ok::<(), ruvio_client::Error>(())
    /// ```
    pub fn try_lock(&mut self, key: &str, ttl: Duration) -> Result<Option<LockGuard<'_>>, Error> {
        let token = new_token();

        if !self.acquire_lock(key, &token, ttl)? {
            return Ok(None);
        }

        Ok(Some(LockGuard {
            client: self,
            key: key.to_owned(),
            token,
            released: false,
        }))
    }

    /// Like [`Self::try_lock`], polling with a short backoff (2 ms doubling to 50 ms) for up to
    /// `wait`. `None` when the lock stayed held.
    pub fn lock(
        &mut self,
        key: &str,
        ttl: Duration,
        wait: Duration,
    ) -> Result<Option<LockGuard<'_>>, Error> {
        let token = new_token();
        let deadline = Instant::now() + wait;
        let mut delay = POLL_FIRST;

        loop {
            if self.acquire_lock(key, &token, ttl)? {
                return Ok(Some(LockGuard {
                    client: self,
                    key: key.to_owned(),
                    token,
                    released: false,
                }));
            }

            let now = Instant::now();

            if now >= deadline {
                return Ok(None);
            }

            std::thread::sleep(delay.min(deadline - now));
            delay = (delay * 2).min(POLL_MAX);
        }
    }

    /// Runs `work` under the lock and releases it afterwards. `None` when the lock was held.
    pub fn with_lock<T, E>(
        &mut self,
        key: &str,
        ttl: Duration,
        work: impl FnOnce(&mut Client) -> Result<T, E>,
    ) -> Result<Option<T>, E>
    where
        E: From<Error>,
    {
        let Some(mut guard) = self.try_lock(key, ttl)? else {
            return Ok(None);
        };

        let result = work(&mut guard)?;

        guard.release()?;

        Ok(Some(result))
    }

    // ---- leases and semaphores -------------------------------------------------------------

    /// `LEASE key ttl`: a fenced lease, released on drop. `None` while another holder has it.
    pub fn lease_guard(
        &mut self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseGuard<'_>>, Error> {
        let Some(token) = self.lease(key, crate::client::duration_millis(ttl) as u64)? else {
            return Ok(None);
        };

        Ok(Some(LeaseGuard {
            client: self,
            key: key.to_owned(),
            token,
            released: false,
        }))
    }

    /// `SEMAPHORE key max ttl`: one of `max` slots, released on drop. `None` when all are taken.
    pub fn semaphore_permit(
        &mut self,
        key: &str,
        max: u64,
        ttl: Duration,
    ) -> Result<Option<SemaphorePermit<'_>>, Error> {
        let Some(token) = self.semaphore(key, max, crate::client::duration_millis(ttl) as u64)?
        else {
            return Ok(None);
        };

        Ok(Some(SemaphorePermit {
            client: self,
            key: key.to_owned(),
            token,
            released: false,
        }))
    }

    // ---- rate limit ------------------------------------------------------------------------

    /// `LIMIT key max window`: a fixed-window rate limit. A refused call is not counted.
    pub fn rate_limit(
        &mut self,
        key: &str,
        max: u64,
        window: Duration,
    ) -> Result<RateLimit, Error> {
        Ok(
            match self.limit(key, max, crate::client::duration_millis(window) as u64)? {
                Some(remaining) => RateLimit {
                    allowed: true,
                    remaining,
                },
                None => RateLimit {
                    allowed: false,
                    remaining: 0,
                },
            },
        )
    }

    // ---- cache-aside -----------------------------------------------------------------------

    /// Cache-aside with stampede protection: returns the cached string, or lets exactly one
    /// caller run `factory` and store the result for `ttl` while the rest wait.
    /// See [`Self::get_or_create_with`].
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # let mut db = ruvio_client::Client::connect("127.0.0.1", 6379)?;
    /// let page = db.get_or_create("page:home", Duration::from_secs(60), || {
    ///     Ok::<_, ruvio_client::Error>("<html>...</html>".to_owned())
    /// })?;
    /// # Ok::<(), ruvio_client::Error>(())
    /// ```
    pub fn get_or_create<F, E>(&mut self, key: &str, ttl: Duration, factory: F) -> Result<String, E>
    where
        F: FnOnce() -> Result<String, E>,
        E: From<Error>,
    {
        self.get_or_create_with(key, CacheOptions::new(ttl), factory)
    }

    /// [`Self::get_or_create`] with every setting. Built on `FILLGET` / `FILLSET`:
    /// `HIT` returns the value; `FILL` (or `STALE` with a token) runs `factory` and stores it
    /// with the token; `STALE` without a token returns the old value; `BUSY` polls plain
    /// `FILLGET` with a 2–50 ms backoff, because `FILLGET ... WAIT` would park the connection.
    /// Gives up with an [`std::io::ErrorKind::TimedOut`] error after `max_wait`.
    /// If `factory` fails, nothing is stored and the marker lapses after `fill_timeout`.
    pub fn get_or_create_with<F, E>(
        &mut self,
        key: &str,
        options: CacheOptions,
        factory: F,
    ) -> Result<String, E>
    where
        F: FnOnce() -> Result<String, E>,
        E: From<Error>,
    {
        let deadline = Instant::now() + options.max_wait;
        let mut delay = POLL_FIRST;

        loop {
            match self.fill_get(key, options.fill_timeout)? {
                FillOutcome::Hit(value) | FillOutcome::Stale { value, token: None } => {
                    return Ok(value);
                }
                FillOutcome::Fill(token)
                | FillOutcome::Stale {
                    token: Some(token), ..
                } => {
                    let value = factory()?;

                    // False means the marker lapsed and another caller took over; this value is
                    // still correct for the caller.
                    self.fill_set(key, &value, options.ttl, &token, options.stale)?;

                    return Ok(value);
                }
                FillOutcome::Busy => {
                    let now = Instant::now();

                    if now >= deadline {
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "another caller is still filling the key",
                        ))
                        .into());
                    }

                    std::thread::sleep(delay.min(deadline - now));
                    delay = (delay * 2).min(POLL_MAX);
                }
            }
        }
    }

    // ---- idempotency -----------------------------------------------------------------------

    /// Runs `work` at most once per `key` and `fingerprint` (within `ttl`) using native `IDEM`:
    /// the first owner runs it and publishes the result; later calls get the stored result;
    /// a call while another owner runs it gets `Pending` and does not run `work`. If `work`
    /// fails, the reservation stays pending until `ttl` passes. Not exactly-once for external
    /// side effects (see docs/IDEM.md).
    pub fn idempotent<F, E>(
        &mut self,
        key: &str,
        fingerprint: &str,
        owner: &str,
        ttl: Duration,
        work: F,
    ) -> Result<IdemOutcome, E>
    where
        F: FnOnce() -> Result<Vec<u8>, E>,
        E: From<Error>,
    {
        match self.idem_begin(key, fingerprint, owner, ttl)? {
            IdemState::Acquired => {
                let result = work()?;

                self.idem_complete(key, fingerprint, owner, &result)?;

                Ok(IdemOutcome::Executed(result))
            }
            IdemState::Completed(result) => Ok(IdemOutcome::Replayed(result)),
            IdemState::Pending => Ok(IdemOutcome::Pending),
            IdemState::Conflict => Ok(IdemOutcome::Conflict),
        }
    }

    // ---- recent items and Bloom ------------------------------------------------------------

    /// Pushes `item` to the front of a list capped at `cap` items (`LPUSHCAP`); returns the length.
    pub fn push_recent(&mut self, key: &str, cap: u64, item: &str) -> Result<i64, Error> {
        self.lpushcap(key, cap, &[item])
    }

    /// The newest `count` items pushed with [`Self::push_recent`], newest first.
    pub fn recent(&mut self, key: &str, count: u64) -> Result<Vec<String>, Error> {
        if count == 0 {
            return Ok(Vec::new());
        }

        self.lrange(key, 0, count as i64 - 1)
    }

    /// Records `items` in a Bloom filter (`BF.MADD`) and reports, per item, whether it was
    /// probably seen before (true) or certainly new (false). A missing key becomes a default
    /// scalable filter.
    pub fn seen_before(&mut self, key: &str, items: &[&str]) -> Result<Vec<bool>, Error> {
        Ok(self
            .bf_madd(key, items)?
            .into_iter()
            .map(|added| !added)
            .collect())
    }

    /// Checks `items` without recording them (`BF.MEXISTS`): true means "maybe seen".
    pub fn maybe_seen(&mut self, key: &str, items: &[&str]) -> Result<Vec<bool>, Error> {
        self.bf_mexists(key, items)
    }
}

#[cfg(test)]
mod tests {
    use super::new_token;

    #[test]
    fn tokens_are_32_hex_characters_and_unique() {
        let first = new_token();
        let second = new_token();

        assert_eq!(first.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }
}
