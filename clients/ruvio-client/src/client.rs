use std::net::{IpAddr, SocketAddr};

use std::sync::{Arc, Mutex};

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::args::{Args, Num};
use crate::cluster::{ClusterRouter, Connection, Discovery, throw_error};

use crate::error::Error;
use crate::value::RespValue;

/// How `SET` treats a key that already exists or is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetMode {
    /// Write the key either way.
    #[default]
    Always,
    /// Write only when the key is absent (`NX`).
    IfNotExists,
    /// Write only when the key exists (`XX`).
    IfExists,
}

/// Where to connect, and an optional password sent as `AUTH`.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Server host. Defaults to loopback.
    pub host: String,
    /// RESP port. Defaults to 6379.
    pub port: u16,
    /// ACL user. When set together with [`Self::password`], the client sends `AUTH user password`.
    pub username: Option<String>,
    /// When set, the client sends `AUTH` before [`Client::connect_with`] returns.
    pub password: Option<String>,
    /// Logical database. When not 0, the client sends `SELECT` after `AUTH`.
    pub database: u32,
    /// How long TCP connect may take.
    pub connect_timeout: Duration,
    /// When true (the default), connect sends `CLUSTER SLOTS`. A slot map turns
    /// the client into a router; a disabled-cluster error keeps one socket.
    pub discover_cluster: bool,
    /// When true (the default), a broken socket is opened again before the next command.
    /// The command that discovers the drop still fails. Set this to false and call
    /// [`Client::reconnect`] on your own schedule.
    pub reconnect: bool,
    /// How many connect attempts a reconnect makes before giving up. One is a single try.
    pub max_reconnect_attempts: u32,
    /// First automatic wait after a failed reconnect attempt.
    pub reconnect_base_delay: Duration,
    /// Ceiling for the automatic exponential wait.
    pub reconnect_max_delay: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 6379,
            username: None,
            password: None,
            database: 0,
            connect_timeout: Duration::from_secs(5),
            discover_cluster: true,
            reconnect: true,
            max_reconnect_attempts: 8,
            reconnect_base_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(2),
        }
    }
}

/// The shared slot for an `on_connection_lost` / `on_connection_restored` callback.
pub(crate) type NoticeHook = Arc<Mutex<Option<Arc<dyn Fn(ConnectionNotice) + Send + Sync>>>>;

/// A command or subscriber socket dropped, or came back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionNotice {
    pub host: String,
    pub port: u16,
    /// True when the socket is the dedicated Pub/Sub connection.
    pub subscriber: bool,
}

/// A Ruvio client. Against a single server it is one TCP connection. Against a
/// sharded server it opens one connection per shard and sends each command to
/// the shard that owns its key.
///
/// Subscribe commands open a second TCP socket, the same way `Ruvio.Client`
/// does. `GET`, `SET`, `PING`, and `PUBLISH` stay on the command sockets.
///
/// The client speaks RESP2 and does not send `CLIENT SETINFO` or `HELLO`.
pub struct Client {
    transport: Transport,
    database: u32,
    options: ClientOptions,
    dedicated_subscriber: bool,
    pubsub: Option<Box<Client>>,
    on_lost: NoticeHook,
    on_restored: NoticeHook,
    reconnect_delay: Option<Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync>>,
    subscriptions: Vec<Vec<String>>,
    read_timeout: Option<Option<Duration>>,
}

enum Transport {
    Single(Connection),
    Cluster(Box<ClusterRouter>),
}

impl Client {
    /// Opens a connection. Sends `CLUSTER SLOTS` so a sharded server is routed automatically.
    pub fn connect(host: impl Into<String>, port: u16) -> Result<Self, Error> {
        Self::connect_database(host, port, 0)
    }

    /// Opens a connection bound to one logical database. Sends `SELECT` when `database` is not 0.
    ///
    /// The returned client is the database handle: every command on it runs in `database`.
    pub fn connect_database(
        host: impl Into<String>,
        port: u16,
        database: u32,
    ) -> Result<Self, Error> {
        Self::connect_with(ClientOptions {
            host: host.into(),
            port,
            database,
            ..ClientOptions::default()
        })
    }

    /// Opens a connection to an IP address and port. Sends `SELECT` when `database` is not 0.
    pub fn connect_ip(ip: IpAddr, port: u16, database: u32) -> Result<Self, Error> {
        Self::connect_addr(SocketAddr::new(ip, port), database)
    }

    /// Opens a connection to a socket address. Sends `SELECT` when `database` is not 0.
    pub fn connect_addr(address: SocketAddr, database: u32) -> Result<Self, Error> {
        Self::connect_database(address.ip().to_string(), address.port(), database)
    }

    /// Opens a connection. When [`ClientOptions::password`] is set, sends `AUTH`.
    /// When [`ClientOptions::discover_cluster`] is on, sends `CLUSTER SLOTS`.
    /// When [`ClientOptions::database`] is not 0, a standalone server then gets `SELECT`.
    pub fn connect_with(options: ClientOptions) -> Result<Self, Error> {
        if options.host.is_empty() {
            return Err(Error::Protocol("host is required".to_owned()));
        }

        let seed = Connection::open(&options.host, options.port, &options)?;

        if !options.discover_cluster {
            return finish_standalone(seed, options);
        }

        match ClusterRouter::discover(seed, options.clone())? {
            Discovery::Cluster(_) if options.database != 0 => Err(Error::Protocol(
                "SELECT is not supported in cluster mode".to_owned(),
            )),
            Discovery::Cluster(mut router) => {
                let on_lost = Arc::new(Mutex::new(None));
                let on_restored = Arc::new(Mutex::new(None));

                router.share_hooks(Arc::clone(&on_lost), Arc::clone(&on_restored));

                Ok(Self {
                    transport: Transport::Cluster(router),
                    database: 0,
                    options,
                    dedicated_subscriber: false,
                    pubsub: None,
                    read_timeout: None,
                    on_lost,
                    on_restored,
                    reconnect_delay: None,
                    subscriptions: Vec::new(),
                })
            }

            Discovery::Standalone(seed) => finish_standalone(seed, options),
        }
    }

    /// True when the server reported a slot map and commands are routed by slot.
    pub fn is_cluster(&self) -> bool {
        matches!(self.transport, Transport::Cluster(_))
    }

    /// How many shards the client routes to. 1 for a single server.
    pub fn shard_count(&self) -> usize {
        match &self.transport {
            Transport::Single(_) => 1,
            Transport::Cluster(router) => router.node_count(),
        }
    }

    /// Sends `PING` and returns the simple-string reply.
    pub fn ping(&mut self) -> Result<String, Error> {
        read_ping(self.run(["PING"])?)
    }

    /// Sends `PING message` and returns the echoed message.
    pub fn ping_message(&mut self, message: &str) -> Result<String, Error> {
        read_ping(self.run(["PING", message])?)
    }

    /// The logical database this connection last selected.
    pub fn database(&self) -> u32 {
        self.database
    }

    /// Opens a second socket for Pub/Sub. `subscribe` on this client already
    /// does that automatically; use this when you want the socket in hand.
    pub fn subscriber(&self) -> Result<Self, Error> {
        self.open_pubsub(&["SUBSCRIBE"])
    }

    /// A separate Pub/Sub socket for sharded channels: on a cluster it connects to the shard that
    /// owns `channel`'s slot, so `ssubscribe` of channels in that slot (one hash tag) works there.
    /// A [`Self::subscriber`] socket reaches one shard only.
    pub fn sharded_subscriber(&self, channel: &str) -> Result<Self, Error> {
        self.open_pubsub(&["SSUBSCRIBE", channel])
    }

    /// Sends `SELECT`. Indexes run from 0 to the server's `databases` setting minus one (16 by default).
    pub fn select(&mut self, database: u32) -> Result<(), Error> {
        let index = Num::uint(u64::from(database));

        self.run(["SELECT", index.as_str()])?;
        self.database = database;

        Ok(())
    }

    /// Sends `SELECT` and returns the same connection.
    ///
    /// This is not a second TCP socket: every later command on the client uses the new database.
    pub fn into_database(mut self, database: u32) -> Result<Self, Error> {
        self.select(database)?;

        Ok(self)
    }

    /// Sets how long a read may block. `None` waits forever.
    ///
    /// A timed-out read returns [`Error::Io`], and the reply may still arrive later,
    /// so drop the client unless it was only waiting in [`Self::read_message`].
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        self.read_timeout = Some(timeout);

        match &mut self.transport {
            Transport::Single(connection) => connection.set_read_timeout(timeout)?,
            Transport::Cluster(router) => router.set_read_timeout(timeout)?,
        }

        if let Some(pubsub) = &mut self.pubsub {
            pubsub.set_read_timeout(timeout)?;
        }

        Ok(())
    }

    /// Sends `GET` and returns the raw bulk, or `None` when the key is missing.
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.execute(&["GET", key])? {
            RespValue::Null => Ok(None),
            RespValue::Bulk(bytes) => Ok(Some(bytes)),
            other => Err(Error::Protocol(format!("GET returned {other:?}"))),
        }
    }

    /// Sends `GET` and decodes the bulk as UTF-8. `None` when the key is missing.
    pub fn get_string(&mut self, key: &str) -> Result<Option<String>, Error> {
        match self.get(key)? {
            None => Ok(None),
            Some(bytes) => RespValue::Bulk(bytes).into_string(),
        }
    }

    /// Sends `SET`. Returns false when `NX` or `XX` skips the write.
    pub fn set(&mut self, key: &str, value: &str) -> Result<bool, Error> {
        self.set_with(key, value, None, SetMode::Always)
    }

    /// Sends `SET` with an optional millisecond expiry and `NX` or `XX`.
    pub fn set_with(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<Duration>,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let mut digits = [0_u8; 40];

        match expiry {
            Some(ttl) => self.set_fixed(
                key,
                value,
                Some(("PX", decimal(duration_millis(ttl), &mut digits))),
                mode,
            ),
            None => self.set_fixed(key, value, None, mode),
        }
    }

    /// Sends `SET ... PXAT`: the key expires at `expires_at`.
    pub fn set_expires_at(
        &mut self,
        key: &str,
        value: &str,
        expires_at: SystemTime,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let mut digits = [0_u8; 40];

        self.set_fixed(
            key,
            value,
            Some(("PXAT", decimal(unix_millis(expires_at)?, &mut digits))),
            mode,
        )
    }

    /// Sends `SET ... KEEPTTL`: the key keeps its current expiry.
    pub fn set_keep_ttl(&mut self, key: &str, value: &str, mode: SetMode) -> Result<bool, Error> {
        self.set_fixed(key, value, Some(("KEEPTTL", "")), mode)
    }

    /// `SET` built on the stack: no argument vector or owned strings. A `("KEEPTTL", "")` expiry
    /// sends the keyword alone.
    fn set_fixed(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<(&str, &str)>,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let mut arguments = ["SET", key, value, "", "", ""];
        let mut count = 3;

        if let Some((keyword, amount)) = expiry {
            arguments[count] = keyword;
            count += 1;

            if !amount.is_empty() {
                arguments[count] = amount;
                count += 1;
            }
        }

        match mode {
            SetMode::Always => {}
            SetMode::IfNotExists => {
                arguments[count] = "NX";
                count += 1;
            }
            SetMode::IfExists => {
                arguments[count] = "XX";
                count += 1;
            }
        }

        Ok(!matches!(
            self.execute(&arguments[..count])?,
            RespValue::Null
        ))
    }

    /// Sends `SET ... GET` and returns the previous string, or `None` when the key was missing.
    /// With `NX` or `XX` the previous value is returned even when the write is skipped.
    pub fn set_and_get(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<Duration>,
        mode: SetMode,
    ) -> Result<Option<String>, Error> {
        let millis = expiry.map(|ttl| Num::wide(duration_millis(ttl)));
        let mut arguments = ["SET", key, value, "", "", "", ""];
        let mut count = 3;

        if let Some(millis) = &millis {
            arguments[3] = "PX";
            arguments[4] = millis.as_str();
            count = 5;
        }

        if let Some(flag) = mode_flag(mode) {
            arguments[count] = flag;
            count += 1;
        }

        arguments[count] = "GET";
        self.bulk_or_null(&arguments[..=count])
    }

    /// Sends `SET key value IFEQ expected [PX ms]` (Ruvio extension): writes only when the
    /// current string equals `expected`. False when it did not match or the key is missing.
    /// `None` for `ttl` clears any TTL, as a plain `SET` does.
    pub fn set_if_eq(
        &mut self,
        key: &str,
        value: &str,
        expected: &str,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        let millis = ttl.map(|ttl| Num::wide(duration_millis(ttl)));

        let reply = match &millis {
            Some(millis) => {
                self.execute(&["SET", key, value, "IFEQ", expected, "PX", millis.as_str()])?
            }
            None => self.execute(&["SET", key, value, "IFEQ", expected])?,
        };

        Ok(!reply.is_null())
    }

    /// Sends `SET key value IFEQ expected KEEPTTL`: compare-and-set that keeps the key's TTL.
    pub fn set_if_eq_keep_ttl(
        &mut self,
        key: &str,
        value: &str,
        expected: &str,
    ) -> Result<bool, Error> {
        Ok(!self
            .execute(&["SET", key, value, "IFEQ", expected, "KEEPTTL"])?
            .is_null())
    }

    /// Sends `SET key value EX seconds`.
    pub fn set_ex(&mut self, key: &str, value: &str, seconds: u64) -> Result<bool, Error> {
        let seconds = Num::uint(seconds);

        self.set_fixed(key, value, Some(("EX", seconds.as_str())), SetMode::Always)
    }

    /// Sends `SET key value EXAT unix_seconds`.
    pub fn set_exat(
        &mut self,
        key: &str,
        value: &str,
        unix_seconds: u64,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let seconds = Num::uint(unix_seconds);

        self.set_fixed(key, value, Some(("EXAT", seconds.as_str())), mode)
    }

    /// Binary-safe `SET key value [PX ms]`. Use [`Self::get`] to read the bytes back.
    pub fn set_bytes(
        &mut self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), Error> {
        let millis = ttl.map(|ttl| Num::wide(duration_millis(ttl)));

        match &millis {
            Some(millis) => self.execute_binary(&[
                b"SET",
                key.as_bytes(),
                value,
                b"PX",
                millis.as_str().as_bytes(),
            ])?,
            None => self.execute_binary(&[b"SET", key.as_bytes(), value])?,
        };

        Ok(())
    }

    /// Sends `INCR`.
    pub fn incr(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["INCR", key])
    }

    /// Sends `EXPIRE`, or `PEXPIRE` when `ttl` is not whole seconds. False when the key does not exist.
    pub fn expire(&mut self, key: &str, ttl: Duration) -> Result<bool, Error> {
        if ttl.subsec_nanos() == 0 {
            let seconds = Num::uint(duration_secs(ttl));

            return self.flag(["EXPIRE", key, seconds.as_str()]);
        }

        let millis = Num::wide(duration_millis(ttl));

        self.flag(["PEXPIRE", key, millis.as_str()])
    }

    /// Sends `DEL` for one key.
    pub fn del_key(&mut self, key: &str) -> Result<i64, Error> {
        self.del(&[key])
    }

    /// Sends `DEL` and returns how many keys were removed.
    pub fn del(&mut self, keys: &[&str]) -> Result<i64, Error> {
        if keys.is_empty() {
            return Err(Error::Protocol("DEL needs at least one key".to_owned()));
        }

        self.integer(join("DEL", keys))
    }

    /// Reads the next reply without sending a command. Use this after a subscribe
    /// for `message`, `pmessage`, and `smessage` pushes.
    pub fn read_message(&mut self) -> Result<RespValue, Error> {
        if let Some(pubsub) = &mut self.pubsub {
            return pubsub.read_message();
        }

        self.ensure_open()?;

        let reply = match &mut self.transport {
            Transport::Single(connection) => connection.read()?,
            Transport::Cluster(router) => router.read_message()?,
        };

        throw_error(reply)
    }

    /// Called when a command or subscriber socket drops.
    pub fn on_connection_lost<F>(&mut self, hook: F)
    where
        F: Fn(ConnectionNotice) + Send + Sync + 'static,
    {
        *self.on_lost.lock().expect("connection hook") = Some(Arc::new(hook));
    }

    /// Called after a dropped socket is open again.
    pub fn on_connection_restored<F>(&mut self, hook: F)
    where
        F: Fn(ConnectionNotice) + Send + Sync + 'static,
    {
        *self.on_restored.lock().expect("connection hook") = Some(Arc::new(hook));
    }

    /// Replaces the built-in exponential wait. The attempt starts at 1 after the first failure.
    /// Return `None` to stop. When set, `max_reconnect_attempts` is not applied.
    pub fn set_reconnect_delay<F>(&mut self, delay: F)
    where
        F: Fn(u32) -> Option<Duration> + Send + Sync + 'static,
    {
        self.reconnect_delay = Some(Arc::new(delay));

        if let Transport::Cluster(router) = &mut self.transport {
            router.set_reconnect_delay(Arc::clone(self.reconnect_delay.as_ref().unwrap()));
        }
    }

    /// Opens a dropped command socket again. With [`ClientOptions::reconnect`] on, the next
    /// command does this itself.
    pub fn reconnect(&mut self) -> Result<(), Error> {
        if let Transport::Cluster(router) = &mut self.transport {
            return router.reconnect_broken();
        }

        if !self.transport_is_broken() {
            return Ok(());
        }

        self.reopen()
    }

    /// Sends one command. A RESP error becomes [`Error::Server`] and the connection stays open.
    pub fn execute(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        self.ensure_open()?;

        if is_subscription_command(arguments) && !self.dedicated_subscriber {
            return self.ensure_pubsub(arguments)?.execute(arguments);
        }

        let reply = match &mut self.transport {
            Transport::Single(connection) => connection.send(arguments)?,
            Transport::Cluster(router) => router.execute(arguments)?,
        };

        let reply = throw_error(reply)?;

        self.remember_subscription(arguments);

        Ok(reply)
    }

    /// Binary-safe [`Self::execute`]: every argument is raw bytes, so values need not be UTF-8
    /// (`RESTORE` payloads, images, encrypted blobs). On a cluster it routes by the second
    /// argument; multi-key commands are not split on this path.
    pub fn execute_binary(&mut self, arguments: &[&[u8]]) -> Result<RespValue, Error> {
        self.ensure_open()?;

        let reply = match &mut self.transport {
            Transport::Single(connection) => connection.send_bytes(arguments)?,
            Transport::Cluster(router) => router.execute_binary(arguments)?,
        };

        throw_error(reply)
    }

    /// Writes every command, then reads one reply each. Error replies stay as [`RespValue::Error`].
    /// On a cluster the commands are grouped by shard; replies keep command order.
    pub fn execute_many(&mut self, commands: &[&[&str]]) -> Result<Vec<RespValue>, Error> {
        if commands.is_empty() {
            return Err(Error::Protocol(
                "a pipeline needs at least one command".to_owned(),
            ));
        }

        match &mut self.transport {
            Transport::Single(connection) => connection.send_many(commands),
            Transport::Cluster(router) => router.execute_many(commands),
        }
    }

    /// Writes one command and reads `replies` replies. A subscribe confirms each channel separately.
    pub(crate) fn run_replies<A: Args>(
        &mut self,
        arguments: A,
        replies: usize,
    ) -> Result<Vec<RespValue>, Error> {
        arguments.with_slice(|borrowed| self.run_replies_slice(borrowed, replies))
    }

    fn run_replies_slice(
        &mut self,
        borrowed: &[&str],
        replies: usize,
    ) -> Result<Vec<RespValue>, Error> {
        if is_subscription_command(borrowed) && !self.dedicated_subscriber {
            return self
                .ensure_pubsub(borrowed)?
                .run_replies_slice(borrowed, replies);
        }

        self.ensure_open()?;

        let values = match &mut self.transport {
            Transport::Single(connection) => connection.send_replies(borrowed, replies)?,
            Transport::Cluster(router) => router.run_replies(borrowed, replies)?,
        };

        if values.len() < replies {
            return Err(Error::Protocol("missing subscribe confirmation".to_owned()));
        }

        for value in &values {
            if let RespValue::Error(message) = value {
                return Err(Error::Server(message.clone()));
            }
        }

        self.remember_subscription(borrowed);

        Ok(values)
    }

    /// Sends one command built from any argument list; see [`Args`].
    #[inline]
    pub(crate) fn run<A: Args>(&mut self, arguments: A) -> Result<RespValue, Error> {
        arguments.with_slice(|borrowed| self.execute(borrowed))
    }

    /// Sends a command and hands the reply plus the command name (for error text) to `read`.
    #[inline]
    pub(crate) fn run_with<A: Args, T>(
        &mut self,
        arguments: A,
        read: impl FnOnce(&str, RespValue) -> Result<T, Error>,
    ) -> Result<T, Error> {
        arguments.with_slice(|borrowed| {
            let reply = self.execute(borrowed)?;

            read(borrowed.first().copied().unwrap_or_default(), reply)
        })
    }

    fn ensure_pubsub(&mut self, arguments: &[&str]) -> Result<&mut Client, Error> {
        if self.pubsub.is_none() {
            self.pubsub = Some(Box::new(self.open_pubsub(arguments)?));
        }

        Ok(self.pubsub.as_mut().unwrap())
    }
    fn open_pubsub(&self, arguments: &[&str]) -> Result<Client, Error> {
        let mut options = self.options.clone();

        options.discover_cluster = false;

        if let Transport::Cluster(router) = &self.transport {
            let (host, port) = router.subscription_endpoint(arguments);

            options.host = host;
            options.port = port;
        }

        let mut client = Client::connect_with(options)?;

        client.dedicated_subscriber = true;
        client.on_lost = Arc::clone(&self.on_lost);
        client.on_restored = Arc::clone(&self.on_restored);
        client.reconnect_delay = self.reconnect_delay.clone();

        if let Some(timeout) = self.read_timeout {
            client.set_read_timeout(timeout)?;
        }

        Ok(client)
    }

    fn transport_is_broken(&self) -> bool {
        match &self.transport {
            Transport::Single(connection) => connection.is_broken(),
            Transport::Cluster(_) => false,
        }
    }

    // Only the socket is reopened; the command that hit the drop is not replayed, because its
    // outcome is unknown and it may be a write (clients/README.md limits C# retries to reads).
    fn ensure_open(&mut self) -> Result<(), Error> {
        let broken = self.transport_is_broken();

        if !broken {
            return Ok(());
        }

        if !self.options.reconnect {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "connection is closed",
            )));
        }

        self.reopen()
    }

    fn reopen(&mut self) -> Result<(), Error> {
        let host = self.options.host.clone();
        let port = self.options.port;

        self.notify_lost(host.clone(), port);

        let mut attempt = 0_u32;

        let mut opened = loop {
            attempt += 1;

            match crate::cluster::Connection::open(&host, port, &self.options) {
                Ok(connection) => break connection,
                Err(error) => match self.next_delay(attempt) {
                    Some(delay) => std::thread::sleep(delay),
                    None => return Err(error),
                },
            }
        };

        if self.database != 0 {
            let index = self.database.to_string();

            throw_error(opened.send(&["SELECT", &index])?)?;
        }

        for command in &self.subscriptions {
            let borrowed: Vec<&str> = command.iter().map(String::as_str).collect();

            throw_error(opened.send(&borrowed)?)?;
        }

        self.transport = Transport::Single(opened);
        self.notify_restored(host, port);

        Ok(())
    }

    // Doubles per attempt and is capped at reconnect_max_delay, so a server that is still
    // restarting is not retried at a fixed rate.
    fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if !self.options.reconnect {
            return None;
        }

        if let Some(delay) = &self.reconnect_delay {
            return delay(attempt);
        }

        if attempt >= self.options.max_reconnect_attempts {
            return None;
        }

        let multiplier = 2_u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.options.reconnect_base_delay.saturating_mul(multiplier);

        Some(delay.min(self.options.reconnect_max_delay))
    }

    fn notify_lost(&self, host: String, port: u16) {
        let notice = ConnectionNotice {
            host,
            port,
            subscriber: self.dedicated_subscriber,
        };

        if let Some(hook) = self.on_lost.lock().expect("connection hook").as_ref() {
            hook(notice);
        }
    }

    fn notify_restored(&self, host: String, port: u16) {
        let notice = ConnectionNotice {
            host,
            port,
            subscriber: self.dedicated_subscriber,
        };

        if let Some(hook) = self.on_restored.lock().expect("connection hook").as_ref() {
            hook(notice);
        }
    }

    fn remember_subscription(&mut self, arguments: &[&str]) {
        if !self.dedicated_subscriber || arguments.is_empty() {
            return;
        }

        let command = arguments[0].to_ascii_uppercase();
        let (subscribe, dropping) = match command.as_str() {
            "SUBSCRIBE" => ("SUBSCRIBE", false),
            "UNSUBSCRIBE" => ("SUBSCRIBE", true),
            "PSUBSCRIBE" => ("PSUBSCRIBE", false),
            "PUNSUBSCRIBE" => ("PSUBSCRIBE", true),
            "SSUBSCRIBE" => ("SSUBSCRIBE", false),
            "SUNSUBSCRIBE" => ("SSUBSCRIBE", true),
            _ => return,
        };

        if dropping && arguments.len() <= 1 {
            self.subscriptions
                .retain(|item| item.first().map(String::as_str) != Some(subscribe));

            return;
        }

        if dropping {
            for channel in &arguments[1..] {
                self.subscriptions.retain(|item| {
                    item.first().map(String::as_str) != Some(subscribe)
                        || item.get(1).map(String::as_str) != Some(*channel)
                });
            }

            return;
        }

        for channel in &arguments[1..] {
            let entry = vec![subscribe.to_owned(), (*channel).to_owned()];

            if !self.subscriptions.iter().any(|item| item == &entry) {
                self.subscriptions.push(entry);
            }
        }
    }

    pub(crate) fn integer<A: Args>(&mut self, arguments: A) -> Result<i64, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Integer(value) => Ok(value),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn flag<A: Args>(&mut self, arguments: A) -> Result<bool, Error> {
        Ok(self.integer(arguments)? > 0)
    }

    pub(crate) fn ok<A: Args>(&mut self, arguments: A) -> Result<(), Error> {
        self.run(arguments)?;

        Ok(())
    }

    pub(crate) fn text<A: Args>(&mut self, arguments: A) -> Result<String, Error> {
        self.run_with(arguments, |command, reply| {
            reply
                .into_string()?
                .ok_or_else(|| Error::Protocol(format!("{command} returned a null reply")))
        })
    }

    pub(crate) fn bulk_or_null<A: Args>(&mut self, arguments: A) -> Result<Option<String>, Error> {
        self.run(arguments)?.into_string()
    }

    pub(crate) fn bytes_or_null<A: Args>(
        &mut self,
        arguments: A,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(None),
            RespValue::Bulk(bytes) => Ok(Some(bytes)),
            RespValue::Simple(text) => Ok(Some(text.into_bytes())),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn strings<A: Args>(&mut self, arguments: A) -> Result<Vec<String>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(Vec::new()),
            RespValue::Array(items) => items
                .into_iter()
                .map(|item| {
                    item.into_string()?
                        .ok_or_else(|| Error::Protocol(format!("{command} returned a null bulk")))
                })
                .collect(),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn optional_strings<A: Args>(
        &mut self,
        arguments: A,
    ) -> Result<Vec<Option<String>>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(Vec::new()),
            RespValue::Array(items) => items.into_iter().map(RespValue::into_string).collect(),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn optional_integer<A: Args>(&mut self, arguments: A) -> Result<Option<i64>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(None),
            RespValue::Integer(value) => Ok(Some(value)),
            other => Err(unexpected(command, &other)),
        })
    }
}

fn read_ping(reply: RespValue) -> Result<String, Error> {
    if let Some(items) = reply.as_array() {
        if let Some(first) = items.first() {
            if first
                .as_string()?
                .is_some_and(|kind| kind.eq_ignore_ascii_case("pong"))
            {
                if items.len() > 1 {
                    return Ok(items[1].as_string()?.unwrap_or_default());
                }

                return Ok("PONG".to_owned());
            }
        }
    }

    reply
        .into_string()?
        .ok_or_else(|| Error::Protocol("PING returned a null reply".to_owned()))
}

fn finish_standalone(mut seed: Connection, options: ClientOptions) -> Result<Client, Error> {
    if options.database != 0 {
        let index = options.database.to_string();

        throw_error(seed.send(&["SELECT", &index])?)?;
    }

    Ok(Client {
        transport: Transport::Single(seed),
        database: options.database,
        options,
        dedicated_subscriber: false,
        pubsub: None,
        read_timeout: None,
        on_lost: Arc::new(Mutex::new(None)),
        on_restored: Arc::new(Mutex::new(None)),
        reconnect_delay: None,
        subscriptions: Vec::new(),
    })
}

fn is_subscription_command(arguments: &[&str]) -> bool {
    const NAMES: [&str; 6] = [
        "SUBSCRIBE",
        "UNSUBSCRIBE",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
        "SSUBSCRIBE",
        "SUNSUBSCRIBE",
    ];

    arguments
        .first()
        .is_some_and(|name| NAMES.iter().any(|known| name.eq_ignore_ascii_case(known)))
}

pub(crate) fn unexpected(command: &str, reply: &RespValue) -> Error {
    Error::Protocol(format!("{command} returned {reply:?}"))
}

pub(crate) fn join(command: &str, arguments: &[&str]) -> Vec<String> {
    let mut values = Vec::with_capacity(arguments.len() + 1);

    values.push(command.to_owned());
    values.extend(arguments.iter().map(|argument| (*argument).to_owned()));

    values
}

fn mode_flag(mode: SetMode) -> Option<&'static str> {
    match mode {
        SetMode::Always => None,
        SetMode::IfNotExists => Some("NX"),
        SetMode::IfExists => Some("XX"),
    }
}

/// Decimal text of `value` in `buffer`, without allocating.
fn decimal(mut value: u128, buffer: &mut [u8; 40]) -> &str {
    let mut start = buffer.len();

    loop {
        start -= 1;
        buffer[start] = b'0' + (value % 10) as u8;
        value /= 10;

        if value == 0 {
            break;
        }
    }

    std::str::from_utf8(&buffer[start..]).expect("digits are ASCII")
}

pub(crate) fn unix_millis(time: SystemTime) -> Result<u128, Error> {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .map_err(|_| Error::Protocol("time is before the Unix epoch".to_owned()))
}

pub(crate) fn duration_millis(ttl: Duration) -> u128 {
    let millis = ttl.as_millis();

    if ttl.subsec_nanos().is_multiple_of(1_000_000) {
        return millis;
    }

    millis.saturating_add(1)
}

pub(crate) fn duration_secs(ttl: Duration) -> u64 {
    let seconds = ttl.as_secs();

    if ttl.subsec_nanos() == 0 {
        return seconds;
    }

    seconds.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use std::net::{TcpListener, TcpStream};

    use std::thread;
    use std::time::Duration;

    use super::{Client, ClientOptions, RespValue, read_ping};

    fn bound(stream: TcpStream) -> TcpStream {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        stream
    }

    fn reply_standalone_cluster(server: &mut TcpStream) {
        let expected = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
        let mut got = vec![0_u8; expected.len()];

        server.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        server
            .write_all(b"-ERR cluster mode is disabled\r\n")
            .unwrap();
    }

    #[test]
    fn connect_asks_for_cluster_slots_then_is_quiet() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut server = accepted.join().unwrap();

        reply_standalone_cluster(&mut server);

        let mut client = connecting.join().unwrap();

        assert!(!client.is_cluster());
        thread::sleep(Duration::from_millis(50));
        server.set_nonblocking(true).unwrap();

        let mut peeked = [0_u8; 1];

        assert!(server.peek(&mut peeked).is_err());
        server.set_nonblocking(false).unwrap();

        let ping = thread::spawn(move || client.ping().unwrap());
        let mut got = [0_u8; 14];

        server.read_exact(&mut got).unwrap();

        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        server.write_all(b"+PONG\r\n").unwrap();

        assert_eq!(ping.join().unwrap(), "PONG");
    }

    #[test]
    fn discover_cluster_off_sends_nothing_until_the_first_command() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let mut client = Client::connect_with(ClientOptions {
            host: "127.0.0.1".to_owned(),
            port,
            discover_cluster: false,
            ..ClientOptions::default()
        })
        .unwrap();
        let mut server = accepted.join().unwrap();

        thread::sleep(Duration::from_millis(50));
        server.set_nonblocking(true).unwrap();

        let mut peeked = [0_u8; 1];

        assert!(server.peek(&mut peeked).is_err());
        server.set_nonblocking(false).unwrap();

        let ping = thread::spawn(move || client.ping().unwrap());
        let mut got = [0_u8; 14];

        server.read_exact(&mut got).unwrap();

        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        server.write_all(b"+PONG\r\n").unwrap();

        assert_eq!(ping.join().unwrap(), "PONG");
    }

    #[test]
    fn a_server_error_leaves_the_next_command_usable() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut server = accepted.join().unwrap();

        reply_standalone_cluster(&mut server);

        let mut client = connecting.join().unwrap();
        let error = thread::spawn(move || {
            let error = client.get_string("missing").unwrap_err();
            let pong = client.ping().unwrap();

            (error.to_string(), pong)
        });

        read_some(&mut server);
        server.write_all(b"-ERR no such key\r\n").unwrap();
        read_some(&mut server);
        server.write_all(b"+PONG\r\n").unwrap();

        let (message, pong) = error.join().unwrap();

        assert_eq!(message, "ERR no such key");
        assert_eq!(pong, "PONG");
    }

    #[test]
    fn password_is_sent_as_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                host: "127.0.0.1".to_owned(),
                port,
                password: Some("secret".to_owned()),
                ..ClientOptions::default()
            })
            .unwrap()
        });

        let mut server = accepted.join().unwrap();
        let expected = b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n";
        let mut got = vec![0_u8; expected.len()];

        server.read_exact(&mut got).unwrap();

        assert_eq!(got, expected);
        server.write_all(b"+OK\r\n").unwrap();
        reply_standalone_cluster(&mut server);
        connecting.join().unwrap();
    }

    #[test]
    fn database_is_selected_after_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                host: "127.0.0.1".to_owned(),
                port,
                password: Some("secret".to_owned()),
                database: 3,
                ..ClientOptions::default()
            })
            .unwrap()
        });

        let mut server = accepted.join().unwrap();
        let auth = b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n";
        let select = b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n";
        let mut got = vec![0_u8; auth.len()];

        server.read_exact(&mut got).unwrap();
        assert_eq!(got, auth);
        server.write_all(b"+OK\r\n").unwrap();
        reply_standalone_cluster(&mut server);
        got = vec![0_u8; select.len()];
        server.read_exact(&mut got).unwrap();
        assert_eq!(got, select);
        server.write_all(b"+OK\r\n").unwrap();
        assert_eq!(connecting.join().unwrap().database(), 3);
    }

    #[test]
    fn subscribe_opens_a_second_socket_and_leaves_ping_on_the_first() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut command = bound(crate::test_support::accept(&listener).0);

        reply_standalone_cluster(&mut command);

        let mut client = connecting.join().unwrap();
        let working = thread::spawn(move || {
            let confirms = client.subscribe(&["news"]).unwrap();
            let pong = client.ping().unwrap();

            (confirms.len(), pong)
        });

        let mut subscriber = bound(crate::test_support::accept(&listener).0);
        let expected = b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n";
        let mut got = vec![0_u8; expected.len()];

        subscriber.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        subscriber
            .write_all(b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")
            .unwrap();

        let mut ping = [0_u8; 14];

        command.read_exact(&mut ping).unwrap();
        assert_eq!(&ping, b"*1\r\n$4\r\nPING\r\n");
        command.write_all(b"+PONG\r\n").unwrap();

        let (confirms, pong) = working.join().unwrap();

        assert_eq!(confirms, 1);
        assert_eq!(pong, "PONG");
    }

    #[test]
    fn explicit_subscriber_uses_one_extra_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut command = bound(crate::test_support::accept(&listener).0);

        reply_standalone_cluster(&mut command);

        let client = connecting.join().unwrap();
        let working = thread::spawn(move || {
            let mut subscriber = client.subscriber().unwrap();

            subscriber.subscribe(&["news"]).unwrap();
        });

        let mut subscriber = bound(crate::test_support::accept(&listener).0);
        let expected = b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n";
        let mut got = vec![0_u8; expected.len()];

        subscriber.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        subscriber
            .write_all(b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")
            .unwrap();
        working.join().unwrap();
        listener.set_nonblocking(true).unwrap();

        assert!(listener.accept().is_err());
    }

    #[test]
    fn the_command_after_a_dropped_socket_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                host: "127.0.0.1".to_owned(),
                port,
                discover_cluster: false,
                reconnect_base_delay: std::time::Duration::ZERO,
                reconnect_max_delay: std::time::Duration::ZERO,
                ..ClientOptions::default()
            })
            .unwrap()
        });

        let accepted = bound(crate::test_support::accept(&listener).0);

        drop(accepted);

        let mut client = connecting.join().unwrap();
        let failed = thread::spawn(move || {
            assert!(client.ping().is_err());

            client.ping().unwrap()
        });

        let mut again = bound(crate::test_support::accept(&listener).0);
        let mut got = [0_u8; 14];

        again.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        again.write_all(b"+PONG\r\n").unwrap();

        assert_eq!(failed.join().unwrap(), "PONG");
    }

    #[test]
    fn ping_reads_a_pubsub_array() {
        let reply = RespValue::Array(vec![
            RespValue::Bulk(b"pong".to_vec()),
            RespValue::Bulk(b"hello".to_vec()),
        ]);

        assert_eq!(read_ping(reply).unwrap(), "hello");
    }

    fn read_some(stream: &mut TcpStream) {
        let mut buffer = [0_u8; 64];

        let read = stream.read(&mut buffer).unwrap();

        assert!(read > 0);
    }
}
