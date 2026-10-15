use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::ffi::c_int;

use std::rc::Rc;

use ahash::AHashMap;
use mlua::{Function, Lua, LuaOptions, Result as LuaResult, StdLib, Table, Value, ffi};

use sha1::{Digest, Sha1};

use crate::{
    Clock, KvShard, StorageIndex,
    config::ScriptLimits,
    engine::{ListWaiterSink, Response, execute_in_database},
    lua_library::{guarded_insert, guarded_remove, guarded_sort, refuse_views, wrap_c_function},
    mutation::{Mutation, MutationSink, OwnedMutation},
    protocol::{Bytes, Command, ProtocolError, parse_arguments},
    routing::command_keys,
};

#[derive(Default)]
pub struct ScriptCache {
    bodies: AHashMap<[u8; 20], Rc<[u8]>>,
    /// The same scripts keyed by body, so EVAL of a cached body skips SHA-1.
    digests: AHashMap<Rc<[u8]>, [u8; 20]>,
    functions: AHashMap<Box<[u8]>, Box<[u8]>>,
    vm: Option<ScriptVm>,
    /// Rules of the user who runs the next script; taken by that script.
    acl: Option<std::sync::Arc<crate::acl::AclRules>>,
    /// Key and value byte limits a `ruvio.call` command meets, as a client's would.
    payload: crate::protocol::ProtocolLimits,
}

impl std::fmt::Debug for ScriptCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScriptCache")
            .field("scripts", &self.bodies.len())
            .field("functions", &self.functions.len())
            .finish()
    }
}

impl ScriptCache {
    /// A cache whose scripts' commands meet `payload`, the limits clients' commands meet.
    pub fn with_payload_limits(payload: crate::protocol::ProtocolLimits) -> Self {
        Self {
            payload,
            ..Self::default()
        }
    }

    /// Limits what the next script may call to what `rules` allow. `None` is unrestricted.
    pub fn restrict_next_script(&mut self, rules: Option<std::sync::Arc<crate::acl::AclRules>>) {
        self.acl = rules;
    }

    pub fn load(&mut self, body: &[u8], limits: ScriptLimits) -> Result<[u8; 20], &'static [u8]> {
        let sha = script_sha1(body);

        self.load_digest(body, sha, limits).map(|()| sha)
    }

    /// Caches `body` under its digest `sha`, computed once by the caller. A body already
    /// cached is not copied again.
    fn load_digest(
        &mut self,
        body: &[u8],
        sha: [u8; 20],
        limits: ScriptLimits,
    ) -> Result<(), &'static [u8]> {
        if body.len() > limits.max_bytes {
            return Err(b"ERR script is too large");
        }

        if self.bodies.contains_key(&sha) {
            return Ok(());
        }

        if self.bodies.len() >= limits.max_cached {
            return Err(b"ERR script cache is full");
        }

        let body: Rc<[u8]> = Rc::from(body);

        self.digests.insert(Rc::clone(&body), sha);
        self.bodies.insert(sha, body);
        Ok(())
    }

    /// Digest of `body`, looked up when the body is cached.
    fn digest(&self, body: &[u8]) -> [u8; 20] {
        self.digests
            .get(body)
            .copied()
            .unwrap_or_else(|| script_sha1(body))
    }

    pub fn exists(&self, sha: &[u8; 20]) -> bool {
        self.bodies.contains_key(sha)
    }

    pub fn flush(&mut self) {
        self.bodies.clear();
        self.digests.clear();
        self.vm = None;
    }

    pub fn load_function(
        &mut self,
        name: &[u8],
        body: &[u8],
        limits: ScriptLimits,
    ) -> Result<(), &'static [u8]> {
        if name.is_empty() {
            return Err(b"ERR function name is empty");
        }

        if body.len() > limits.max_bytes {
            return Err(b"ERR function is too large");
        }

        if !self.functions.contains_key(name) && self.functions.len() >= limits.max_cached {
            return Err(b"ERR function library is full");
        }

        self.functions.insert(
            name.to_vec().into_boxed_slice(),
            body.to_vec().into_boxed_slice(),
        );
        Ok(())
    }

    pub fn function(&self, name: &[u8]) -> Option<&[u8]> {
        self.functions.get(name).map(|body| &body[..])
    }

    pub fn delete_function(&mut self, name: &[u8]) -> bool {
        self.functions.remove(name).is_some()
    }

    pub fn function_names(&self) -> Vec<&[u8]> {
        let mut names: Vec<&[u8]> = self.functions.keys().map(|name| &name[..]).collect();

        names.sort_unstable();
        names
    }
}

pub fn script_sha1(body: &[u8]) -> [u8; 20] {
    Sha1::digest(body).into()
}

pub fn hex_sha(sha: &[u8; 20]) -> String {
    let mut out = String::with_capacity(40);

    for byte in sha {
        out.push_str(&format!("{byte:02x}"));
    }

    out
}

pub fn parse_sha(bytes: &[u8]) -> Option<[u8; 20]> {
    if bytes.len() != 40 {
        return None;
    }

    let mut sha = [0u8; 20];

    for (index, slot) in sha.iter_mut().enumerate() {
        let start = index * 2;

        *slot = u8::from_str_radix(std::str::from_utf8(&bytes[start..start + 2]).ok()?, 16).ok()?;
    }

    Some(sha)
}

#[derive(Default)]
struct BufferingSink {
    mutations: Vec<OwnedMutation>,
}

impl MutationSink for BufferingSink {
    type Error = std::convert::Infallible;

    fn record(&mut self, mutation: Mutation<'_>) -> Result<(), Self::Error> {
        // The batch becomes one WAL transaction, which cannot nest: inline a command's own.
        match mutation {
            Mutation::Transaction { mutations } => self.mutations.extend_from_slice(mutations),
            mutation => self.mutations.push(mutation.to_owned()),
        }
        Ok(())
    }

    fn switches_databases(&self) -> bool {
        false
    }
}

struct EvalHost<'a, C: Clock, I: StorageIndex, W: ListWaiterSink> {
    shard: &'a mut KvShard<C, I>,
    collected: BufferingSink,
    waiters: &'a mut W,
    allowed: &'a [Bytes<'a>],
    /// Declared keys as they were before the script first wrote them, restored when the
    /// script fails or its batch is refused.
    undo: Vec<ScriptUndo>,
    calls: usize,
    limits: ScriptLimits,
    payload: crate::protocol::ProtocolLimits,
    database: u8,
    acl: Option<std::sync::Arc<crate::acl::AclRules>>,
    /// A `ruvio.call` hit the memory limit: the whole script fails and rolls back, even
    /// when the script caught the error, so no command it ran stays applied.
    aborted: bool,
}

struct ScriptUndo {
    key: Box<[u8]>,
    entry: Option<crate::Entry>,
    deadline: Option<crate::time::UnixMillis>,
}

#[allow(clippy::too_many_arguments)]
pub fn execute_eval<C, I, S, W>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    cache: &mut ScriptCache,
    limits: ScriptLimits,
    body: &[u8],
    keys: &[Bytes<'_>],
    args: &[Bytes<'_>],
    database: u8,
) -> Result<Response<'static>, S::Error>
where
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
{
    let sha = cache.digest(body);

    eval_digest(
        shard, sink, waiters, cache, limits, body, sha, keys, args, database,
    )
}

/// `execute_eval` with the body's digest already known, so it is hashed once per call.
#[allow(clippy::too_many_arguments)]
fn eval_digest<C, I, S, W>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    cache: &mut ScriptCache,
    limits: ScriptLimits,
    body: &[u8],
    sha: [u8; 20],
    keys: &[Bytes<'_>],
    args: &[Bytes<'_>],
    database: u8,
) -> Result<Response<'static>, S::Error>
where
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
{
    if body.len() > limits.max_bytes {
        return Ok(Response::Error(b"ERR script is too large"));
    }

    if !keys.is_empty() && crate::routing::common_slot(keys.iter().map(AsRef::as_ref)).is_err() {
        return Ok(Response::Error(
            b"CROSSSLOT Keys in request do not hash to the same slot",
        ));
    }

    let keep = cache.load_digest(body, sha, limits).is_ok();

    shard.enter(database);

    // Blocked clients served by the script get their data once its writes are recorded.
    let before = sink.written_sequence();

    waiters.hold_deliveries();
    let host = RefCell::new(EvalHost {
        shard,
        collected: BufferingSink::default(),
        waiters,
        allowed: keys,
        undo: Vec::new(),
        calls: 0,
        limits,
        payload: cache.payload,
        database,
        acl: cache.acl.take(),
        aborted: false,
    });

    let result = run_lua(cache, &host, body, sha, keep, keys, args);
    let mut host = host.into_inner();
    let result = if host.aborted {
        Err(Response::Error(MEMORY_LIMIT_ERROR))
    } else {
        result
    };
    let undo = std::mem::take(&mut host.undo);

    match result {
        Ok(response) => {
            sink.select_database(database);

            if !host.collected.mutations.is_empty()
                && let Err(error) = sink.record_now(Mutation::Transaction {
                    mutations: &host.collected.mutations,
                })
            {
                restore(host.shard, database, undo);
                host.shard.settle_held_removals(false);
                host.waiters.release_deliveries(false);

                return Err(error);
            }

            host.shard.settle_held_removals(true);
            host.waiters.served_by(crate::engine::written_since(
                before,
                sink.written_sequence(),
            ));
            host.waiters.release_deliveries(true);
            Ok(response)
        }

        Err(response) => {
            restore(host.shard, database, undo);
            host.shard.settle_held_removals(false);
            host.waiters.release_deliveries(false);
            Ok(response)
        }
    }
}

pub fn execute_script_command<C, I, S, W>(
    shard: &mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    cache: &mut ScriptCache,
    limits: ScriptLimits,
    command: Command<'_>,
    database: u8,
) -> Result<Response<'static>, S::Error>
where
    C: Clock,
    I: StorageIndex,
    S: MutationSink,
    W: ListWaiterSink,
{
    match command {
        Command::ScriptLoad(body) => match cache.load(body.as_ref(), limits) {
            Ok(sha) => Ok(Response::Bulk(Some(std::borrow::Cow::Owned(
                hex_sha(&sha).into_bytes(),
            )))),
            Err(message) => Ok(Response::Error(message)),
        },
        Command::ScriptExists(shas) => {
            let mut replies = Vec::with_capacity(shas.len());

            for sha in shas {
                replies.push(Response::Integer(i64::from(
                    parse_sha(sha.as_ref()).is_some_and(|sha| cache.exists(&sha)),
                )));
            }

            Ok(Response::Exec(replies))
        }

        Command::ScriptFlush => {
            cache.flush();
            Ok(Response::Simple(b"OK"))
        }

        Command::ScriptKill => Ok(Response::Error(
            b"NOTBUSY No scripts in execution right now.",
        )),
        Command::Eval { script, keys, args } => execute_eval(
            shard,
            sink,
            waiters,
            cache,
            limits,
            script.as_ref(),
            &keys,
            &args,
            database,
        ),
        Command::EvalSha { sha, keys, args } => {
            let Some(digest) = parse_sha(sha.as_ref()) else {
                return Ok(Response::Error(
                    b"NOSCRIPT No matching script. Please use EVAL.",
                ));
            };

            let Some(body) = cache.bodies.get(&digest).map(Rc::clone) else {
                return Ok(Response::Error(
                    b"NOSCRIPT No matching script. Please use EVAL.",
                ));
            };

            eval_digest(
                shard, sink, waiters, cache, limits, &body, digest, &keys, &args, database,
            )
        }

        Command::FunctionLoad { name, body } => {
            match cache.load_function(name.as_ref(), body.as_ref(), limits) {
                Ok(()) => Ok(Response::Bulk(Some(std::borrow::Cow::Owned(
                    name.as_ref().to_vec(),
                )))),
                Err(message) => Ok(Response::Error(message)),
            }
        }

        Command::FunctionDelete(name) => Ok(Response::Integer(i64::from(
            cache.delete_function(name.as_ref()),
        ))),
        Command::FunctionList => Ok(Response::Exec(
            cache
                .function_names()
                .into_iter()
                .map(|name| Response::Bulk(Some(std::borrow::Cow::Owned(name.to_vec()))))
                .collect(),
        )),
        Command::FCall { name, keys, args } => {
            let Some(body) = cache.function(name.as_ref()).map(<[u8]>::to_vec) else {
                return Ok(Response::Error(b"ERR function not found"));
            };

            execute_eval(
                shard, sink, waiters, cache, limits, &body, &keys, &args, database,
            )
        }

        _ => Ok(Response::Error(b"ERR not a script command")),
    }
}

fn restore<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    database: u8,
    undo: Vec<ScriptUndo>,
) {
    shard.enter(database);

    for saved in undo.into_iter().rev() {
        shard.restore_entry(&saved.key, saved.entry);

        if let Some(deadline) = saved.deadline {
            shard.expire_at(&saved.key, deadline);
        }
    }
}

struct ScriptVm {
    compiled: AHashMap<[u8; 20], Function>,
    /// What scripts see as their globals: an empty read-only view of the real globals.
    env: Table,
    /// `KEYS` and `ARGV`, the globals set before each script.
    names: [mlua::LuaString; 2],
    lua: Lua,
}

/// The live script host. `ruvio.call` is installed once and reads this slot,
/// the same way Redis keeps `server.lua_client` for `redis.call`.
#[derive(Clone, Copy)]
struct DriverSlot {
    ptr: *mut (),
    lua: *const Lua,
    /// Runs one `ruvio.call` on the given stack: `Some(n)` leaves `n` results on top,
    /// `None` leaves the error to raise.
    drive: fn(*mut (), *const Lua, *mut ffi::lua_State, bool) -> Option<c_int>,
}

#[cfg(test)]
thread_local! {
    /// Most Lua memory seen right after a `ruvio.call` built its reply.
    static PEAK_CALL_MEMORY: Cell<usize> = const { Cell::new(0) };
}

thread_local! {
    static SCRIPT_DRIVER: Cell<DriverSlot> = const {
        Cell::new(DriverSlot {
            ptr: std::ptr::null_mut(),
            lua: std::ptr::null(),
            drive: missing_driver,
        })
    };

    /// A panic caught inside `ruvio.call`, resumed once the script has unwound.
    static SCRIPT_PANIC: Cell<Option<Box<dyn std::any::Any + Send>>> = const { Cell::new(None) };
}

#[allow(unsafe_code)]
fn missing_driver(
    _ptr: *mut (),
    _lua: *const Lua,
    state: *mut ffi::lua_State,
    _raise: bool,
) -> Option<c_int> {
    // SAFETY: Lua called `ruvio.call` with this state; a C function has LUA_MINSTACK free
    // slots. Nothing here has a destructor, so an allocation error skips none.
    unsafe { push_bytes(state, b"ERR ruvio.call outside a script") };
    None
}

/// `ruvio.call`, a plain C function: arguments are read where they sit on the Lua stack
/// and the reply is pushed straight onto it, with no mlua handle per value.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn script_call(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state.
    unsafe { enter_script_call(state, true) }
}

/// `ruvio.pcall`: a failed command comes back as `{err = message}` instead of raising.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn script_pcall(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state.
    unsafe { enter_script_call(state, false) }
}

#[allow(unsafe_code)]
unsafe fn enter_script_call(state: *mut ffi::lua_State, raise: bool) -> c_int {
    let slot = SCRIPT_DRIVER.with(Cell::get);
    // After a panic the shard may be half-changed: no further command runs, even if the
    // script caught the error.
    let panicked = SCRIPT_PANIC.with(|panic| {
        let payload = panic.take();
        let panicked = payload.is_some();

        panic.set(payload);
        panicked
    });

    if panicked {
        // SAFETY: Lua's own state; nothing with a destructor is alive here.
        unsafe {
            ffi::lua_settop(state, 0);
            push_bytes(state, b"ERR ruvio.call failed");
            ffi::lua_error(state)
        }
    }

    // A command can take long, and few instructions run between two calls.
    if let Err(error) = crate::lua_library::check_deadline() {
        // SAFETY: Lua's own state; nothing with a destructor is alive here.
        unsafe {
            ffi::lua_settop(state, 0);
            push_bytes(state, error.as_bytes());
            ffi::lua_error(state)
        }
    }

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (slot.drive)(slot.ptr, slot.lua, state, raise)
    }));

    match outcome {
        Ok(Some(results)) => {
            // SAFETY: the running thread; the reply may be a large value.
            unsafe { crate::lua_library::adapt_hook(state) };
            return results;
        }
        Ok(None) => {}
        Err(payload) => {
            SCRIPT_PANIC.with(|slot| slot.set(Some(payload)));
            // SAFETY: Lua's own state; emptying the frame leaves room for the message.
            unsafe {
                ffi::lua_settop(state, 0);
                push_bytes(state, b"ERR ruvio.call failed");
            }
        }
    }

    // SAFETY: the error value is on top of the stack. Every Rust value of this call has
    // been dropped (`slot` is `Copy`), so the longjmp back into Lua skips no destructor.
    unsafe { ffi::lua_error(state) }
}

#[allow(unsafe_code)]
fn drive_active<C, I, W>(
    ptr: *mut (),
    lua: *const Lua,
    state: *mut ffi::lua_State,
    raise: bool,
) -> Option<c_int>
where
    C: Clock,
    I: StorageIndex,
    W: ListWaiterSink,
{
    // SAFETY: `DriverGuard` stored these pointers together with `drive_active::<C, I, W>`;
    // the host and the Lua state stay on this thread's stack until the guard drops.
    // `ruvio.call` cannot start another script, so the slot is not replaced underneath.
    let (host, lua) = unsafe { (&*(ptr as *const RefCell<EvalHost<'_, C, I, W>>), &*lua) };
    let _lifted = LiftedLimit::new(lua);
    let mut budget = LuaBudget::new(lua, state);
    // SAFETY: Lua called `ruvio.call` with `state` and the limit is lifted.
    let outcome = unsafe { dispatch_call(host, state, &mut budget) };

    #[cfg(test)]
    PEAK_CALL_MEMORY.set(PEAK_CALL_MEMORY.get().max(lua.used_memory()));
    let failure = match outcome {
        // The reply was charged as it was built; this catches what the estimate missed.
        Ok(()) if !budget.over_limit() => return Some(1),
        Ok(()) => CallFailure::Memory,
        Err(_) if budget.over_limit() => CallFailure::Memory,
        Err(failure) => failure,
    };

    if matches!(failure, CallFailure::Memory) {
        host.borrow_mut().aborted = true;
    }

    // SAFETY: as above; three slots are checked before pushing the error.
    unsafe {
        if ffi::lua_checkstack(state, 3) == 0 {
            ffi::lua_settop(state, 0);
        }

        match failure {
            CallFailure::Command(message) if !raise => {
                push_status(state, c"err", &message);
                Some(1)
            }

            CallFailure::Command(message) | CallFailure::Fatal(message) => {
                push_bytes(state, &message);
                None
            }

            CallFailure::Memory => {
                push_bytes(state, MEMORY_LIMIT_ERROR);
                None
            }
        }
    }
}

/// Why one `ruvio.call` failed.
enum CallFailure {
    /// The command failed or was refused: `call` raises it, `pcall` returns `{err = ...}`.
    Command(Cow<'static, [u8]>),
    /// The script itself is at fault (bad arguments, quotas, limits): always raised.
    Fatal(Cow<'static, [u8]>),
    /// The script's memory limit was reached: raised, and the whole script fails.
    Memory,
}

const MEMORY_LIMIT_ERROR: &[u8] = b"ERR Lua script exceeded the memory limit";

/// What the Lua state may still allocate for one `ruvio.call` reply. Each value is charged
/// its estimated Lua size before it is created, so a huge reply is refused once it would
/// pass the limit instead of being built in full with the limit lifted.
struct LuaBudget<'a> {
    lua: &'a Lua,
    state: *mut ffi::lua_State,
    left: usize,
    collected: bool,
}

/// Estimated Lua heap cost of what [`push_response`] creates: a string's header, its
/// terminator and its share of the string table; a table's header; one array slot; one
/// hash node.
const LUA_STRING_COST: usize = 48;
const LUA_TABLE_COST: usize = 64;
const LUA_SLOT_COST: usize = 16;
const LUA_NODE_COST: usize = 40;

impl<'a> LuaBudget<'a> {
    fn new(lua: &'a Lua, state: *mut ffi::lua_State) -> Self {
        Self {
            lua,
            state,
            left: SCRIPT_MEMORY_LIMIT.saturating_sub(lua.used_memory()),
            collected: false,
        }
    }

    /// Takes `bytes` from the budget. Out of budget, garbage is collected once and the
    /// budget measured again before the charge is refused.
    #[inline]
    fn charge(&mut self, bytes: usize) -> Result<(), PushFailure> {
        match self.left.checked_sub(bytes) {
            Some(left) => {
                self.left = left;
                Ok(())
            }
            None => self.charge_after_collecting(bytes),
        }
    }

    #[cold]
    fn charge_after_collecting(&mut self, bytes: usize) -> Result<(), PushFailure> {
        if self.collected {
            return Err(PushFailure::Memory);
        }

        self.collect();
        self.left = SCRIPT_MEMORY_LIMIT.saturating_sub(self.lua.used_memory());
        self.left = self.left.checked_sub(bytes).ok_or(PushFailure::Memory)?;
        Ok(())
    }

    /// Charges `count` values of `each` bytes.
    #[inline]
    fn charge_many(&mut self, count: usize, each: usize) -> Result<(), PushFailure> {
        self.charge(count.saturating_mul(each))
    }

    /// Whether the state holds more than the limit, garbage collected first.
    fn over_limit(&mut self) -> bool {
        if self.lua.used_memory() <= SCRIPT_MEMORY_LIMIT {
            return false;
        }

        self.collect();
        self.lua.used_memory() > SCRIPT_MEMORY_LIMIT
    }

    #[cold]
    #[allow(unsafe_code)]
    fn collect(&mut self) {
        self.collected = true;
        // SAFETY: the running state, inside `ruvio.call` with the limit lifted. Everything
        // the call still uses sits on its stack; scripts cannot set finalizers.
        unsafe { ffi::lua_gc(self.state, ffi::LUA_GCCOLLECT, 0) };
    }
}

/// Why a reply could not be pushed onto the Lua stack.
#[derive(Debug, PartialEq)]
enum PushFailure {
    Memory,
    Refused(&'static [u8]),
}

struct DriverGuard(DriverSlot);

impl DriverGuard {
    fn install<C, I, W>(host: &RefCell<EvalHost<'_, C, I, W>>, lua: &Lua) -> Self
    where
        C: Clock,
        I: StorageIndex,
        W: ListWaiterSink,
    {
        let previous = SCRIPT_DRIVER.with(|slot| {
            slot.replace(DriverSlot {
                ptr: std::ptr::from_ref(host).cast_mut().cast(),
                lua: std::ptr::from_ref(lua),
                drive: drive_active::<C, I, W>,
            })
        });

        Self(previous)
    }
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        SCRIPT_DRIVER.with(|slot| slot.set(self.0));
    }
}

/// Memory one shard's Lua state may hold: scripts, their cached chunks and their data.
const SCRIPT_MEMORY_LIMIT: usize = 128 * 1024 * 1024;

/// Lifts the Lua memory limit while Rust code builds or reads Lua values; the limit is
/// back in force when the guard drops. Under a limit mlua wraps every string and table
/// it creates in a protected call (an allocation failure must not unwind through Rust
/// frames), which made each `ruvio.call` and each script's `KEYS`/`ARGV` several times
/// dearer. A `ruvio.call` reply is charged against the limit by [`LuaBudget`] as it is
/// built instead; the script's own code, such as `string.rep`, still runs under it.
struct LiftedLimit<'a>(&'a Lua);

impl<'a> LiftedLimit<'a> {
    fn new(lua: &'a Lua) -> Self {
        let _ = lua.set_memory_limit(0);
        Self(lua)
    }
}

impl Drop for LiftedLimit<'_> {
    fn drop(&mut self) {
        let _ = self.0.set_memory_limit(SCRIPT_MEMORY_LIMIT);
    }
}

impl ScriptVm {
    #[allow(unsafe_code)]
    fn open() -> Result<Self, Response<'static>> {
        // Reduced library set: io, os, package and debug stay out so a script cannot reach
        // the host or its files.
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )
        .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;

        sanitize_globals(&lua).map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        // One C call such as `string.rep` can ask for gigabytes between instruction hooks.
        lua.set_memory_limit(SCRIPT_MEMORY_LIMIT)
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        // A raw count hook rather than mlua's: Lua 5.1 copies it to every coroutine, and
        // it can shorten its own interval on a thread whose heap holds large values.
        lua.exec_raw_lua(|raw| {
            // SAFETY: the main state, outside any call; the hook keeps Lua's rules.
            unsafe {
                ffi::lua_sethook(
                    raw.state(),
                    Some(crate::lua_library::instruction_hook),
                    ffi::LUA_MASKCOUNT,
                    crate::lua_library::HOOK_INTERVAL as c_int,
                );
            }
        });
        // SAFETY: both are `lua_CFunction`s that keep Lua's stack discipline and raise
        // only once no Rust value is left on their frames.
        let (call, pcall) = unsafe {
            (
                lua.create_c_function(script_call),
                lua.create_c_function(script_pcall),
            )
        };
        let call = call.map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        let pcall = pcall.map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        let status_reply = lua
            .create_function(|lua, message: mlua::LuaString| status_table(lua, &message))
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        let error_reply = lua
            .create_function(|lua, message: mlua::LuaString| error_table(lua, &message))
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        let commands = lua
            .create_table()
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;

        commands
            .set("call", call)
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        commands
            .set("pcall", pcall)
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        commands
            .set("status_reply", status_reply)
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        commands
            .set("error_reply", error_reply)
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;
        let env = seal_globals(&lua, commands)
            .map_err(|_| Response::Error(b"ERR failed to start Lua"))?;

        let names = [lua.create_string("KEYS"), lua.create_string("ARGV")];
        let [Ok(keys), Ok(args)] = names else {
            return Err(Response::Error(b"ERR failed to start Lua"));
        };

        Ok(Self {
            compiled: AHashMap::new(),
            env,
            names: [keys, args],
            lua,
        })
    }

    fn function_for(
        &mut self,
        body: &[u8],
        sha: [u8; 20],
        keep: bool,
    ) -> Result<Function, Response<'static>> {
        if keep && let Some(function) = self.compiled.get(&sha) {
            return Ok(function.clone());
        }

        // Compiled bytecode is never run: only source text is accepted.
        if body.first() == Some(&0x1b) {
            return Err(Response::Error(b"ERR Lua bytecode is not allowed"));
        }

        let function = self
            .lua
            .load(body)
            .set_name("=user_script")
            .set_environment(self.env.clone())
            .into_function()
            .map_err(|error| lua_error_response(&error))?;

        if keep {
            self.compiled.insert(sha, function.clone());
        }

        Ok(function)
    }
}

fn run_lua<C, I, W>(
    cache: &mut ScriptCache,
    host: &RefCell<EvalHost<'_, C, I, W>>,
    body: &[u8],
    sha: [u8; 20],
    keep: bool,
    keys: &[Bytes<'_>],
    args: &[Bytes<'_>],
) -> Result<Response<'static>, Response<'static>>
where
    C: Clock,
    I: StorageIndex,
    W: ListWaiterSink,
{
    // Before the state may be opened: its setup runs Lua code under the hook.
    let limits = host.borrow().limits;

    crate::lua_library::start_script(
        u64::try_from(limits.max_instructions).unwrap_or(u64::MAX),
        std::time::Duration::from_millis(u64::try_from(limits.max_time_ms).unwrap_or(u64::MAX)),
    );
    let vm = match cache.vm {
        Some(ref mut vm) => vm,
        None => {
            cache.vm = Some(ScriptVm::open()?);

            cache.vm.as_mut().expect("lua state was just opened")
        }
    };

    let function = vm.function_for(body, sha, keep)?;
    let lua = &vm.lua;
    let lifted = LiftedLimit::new(lua);

    set_globals(&lifted, &vm.names, keys, args).map_err(|error| lua_error_response(&error))?;
    // `KEYS` and `ARGV` may be large, and the heap holds what earlier scripts left.
    crate::lua_library::adapt_main_hook(lua);
    let _driver = DriverGuard::install(host, lua);
    // The script's own code runs under the limit.
    drop(lifted);
    let result = function.call::<Value>(());

    if let Some(payload) = SCRIPT_PANIC.with(Cell::take) {
        std::panic::resume_unwind(payload);
    }

    let _lifted = LiftedLimit::new(lua);
    let response = match result {
        Ok(value) => {
            let mut budget = MAX_SCRIPT_REPLY_BYTES;

            lua_to_response(value, 0, &mut budget).map_err(Response::Error)
        }
        Err(error) => Err(lua_error_response(&error)),
    };

    // Lua 5.1 does not collect before failing an allocation, so garbage a script left behind
    // (often from hitting the limit) would fail the next script. Collect once it is large.
    if lua.used_memory() > SCRIPT_MEMORY_LIMIT / 4 {
        let _ = lua.gc_collect();
    }

    response
}

fn sanitize_globals(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();

    for name in [
        "dofile",
        "loadfile",
        // `loadstring` and `load` must stay removed: the vendored Lua 5.1 parser lacks the
        // CVE-2025-49844 fix, so compiling script-supplied source at run time is unsafe.
        "loadstring",
        "load",
        "print",
        "module",
        "require",
        "collectgarbage",
        // The only way Lua 5.1 code can get a `__gc` finalizer, which would run (and
        // could raise) inside a garbage-collection step of a raw C API call.
        "newproxy",
        // Reach and replace the real globals behind a script's read-only view.
        "getfenv",
        "setfenv",
    ] {
        globals.set(name, Value::Nil)?;
    }

    // No hidden RNG: recovery replays WAL mutations and never runs Lua, so a script's
    // result must depend only on its inputs.
    if let Ok(math) = globals.get::<Table>("math") {
        math.set("random", Value::Nil)?;
        math.set("randomseed", Value::Nil)?;
    }

    crate::lua_library::install(lua)
}

/// Makes the globals read-only, as Redis 7 does: scripts run in an empty view of them
/// that raises on new globals, and every table in them (`ruvio`, `string`, `table`, ...)
/// is replaced by such a view too. One script therefore cannot change what the next one
/// runs, such as wrapping `ruvio.call` to run commands under another user's ACL. `KEYS`
/// and `ARGV` are still set in the real globals before each script. The functions that
/// write a table raw (`rawset`, `table.insert`, `table.remove`, `table.sort`) refuse the
/// views. Returns the view scripts use as their environment.
fn seal_globals(lua: &Lua, commands: Table) -> LuaResult<Table> {
    // A Lua function, so the script gets a string error it can catch and print.
    let refuse: Function = lua
        .load("local error = error return function() error('Attempt to modify a readonly table', 2) end")
        .set_name("=sandbox")
        .eval()?;
    let view = |table: &Table| -> LuaResult<Table> {
        let proxy = lua.create_table()?;
        let meta = lua.create_table()?;

        meta.raw_set("__index", table)?;
        meta.raw_set("__newindex", refuse.clone())?;
        // `getmetatable` returns this instead, and `setmetatable` refuses the table.
        meta.raw_set("__metatable", false)?;
        proxy.set_metatable(Some(meta))?;
        Ok(proxy)
    };
    let globals = lua.globals();

    globals.raw_set("ruvio", commands)?;
    let libraries: Vec<(Value, Table)> = globals
        .pairs::<Value, Value>()
        .filter_map(|pair| match pair {
            Ok((name, Value::Table(table))) if table != globals => Some(Ok((name, table))),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<LuaResult<_>>()?;
    let mut views = Vec::with_capacity(libraries.len() + 1);

    for (name, library) in &libraries {
        let proxy = view(library)?;

        globals.raw_set(name, &proxy)?;
        views.push(proxy);
    }

    let env = view(&globals)?;

    globals.raw_set("_G", &env)?;
    views.push(env.clone());
    let table_library = libraries
        .iter()
        .find(
            |(name, _)| matches!(name, Value::String(name) if name.as_bytes().as_ref() == b"table"),
        )
        .map(|(_, table)| table.clone())
        .ok_or_else(|| mlua::Error::runtime("no table library"))?;
    let _lifted = LiftedLimit::new(lua);

    let upvalues: Vec<Value> = views.into_iter().map(Value::Table).collect();

    for (library, name, wrapper) in [
        (&globals, c"rawset", refuse_views as ffi::lua_CFunction),
        (&table_library, c"insert", guarded_insert),
        (&table_library, c"remove", guarded_remove),
        (&table_library, c"sort", guarded_sort),
    ] {
        wrap_c_function(lua, library, name, wrapper, &upvalues)?;
    }

    Ok(env)
}

/// Sets `KEYS` and `ARGV` straight on the stack: building them through mlua handles costs
/// a registry slot and two stack moves per string, and a protected call costs a closure
/// and an error handler per script. They go in the real globals, which scripts read
/// through their read-only view.
#[allow(unsafe_code)]
fn set_globals(
    lifted: &LiftedLimit<'_>,
    names: &[mlua::LuaString; 2],
    keys: &[Bytes<'_>],
    args: &[Bytes<'_>],
) -> LuaResult<()> {
    let too_many = |values: &[Bytes<'_>]| c_int::try_from(values.len()).is_err();

    if too_many(keys) || too_many(args) {
        return Err(mlua::Error::runtime("too many arguments"));
    }

    lifted.0.exec_raw_lua(|raw| {
        let state = raw.state();

        // SAFETY: these calls run unprotected, as mlua runs its own without a memory limit.
        // `lifted` proves the limit is off, so the allocator aborts rather than fail and no
        // allocation raises a Lua error. A GC step may run finalizers, but scripts cannot
        // set any (`newproxy` is removed and there is no debug library), and mlua's own do
        // not raise. Raw sets of string keys raise nothing and run no metamethod. The stack
        // is checked for three slots and left as it was found. Lengths fit `c_int` (checked
        // above).
        unsafe {
            if ffi::lua_checkstack(state, 3) == 0 {
                return Err(mlua::Error::runtime("Lua stack is full"));
            }

            let top = ffi::lua_gettop(state);

            // The names are kept as Lua strings: interning them anew costs a hash and a
            // string-table probe each.
            raw.push(&names[0])?;
            push_global_strings(state, keys);
            raw.push(&names[1])?;
            push_global_strings(state, args);
            debug_assert_eq!(ffi::lua_gettop(state), top);
        }

        Ok(())
    })
}

/// Sets the global named by the string on top of the stack to an array of `values`.
///
/// # Safety
/// Same contract as the calls in [`set_globals`]: limit lifted, the name on top and two
/// free stack slots above it, `values.len()` fits `c_int`.
#[allow(unsafe_code)]
unsafe fn push_global_strings(state: *mut ffi::lua_State, values: &[Bytes<'_>]) {
    // SAFETY: guaranteed by the caller.
    unsafe {
        ffi::lua_createtable(state, values.len() as c_int, 0);

        for (index, value) in values.iter().enumerate() {
            let value = value.as_ref();

            ffi::lua_pushlstring_(state, value.as_ptr().cast(), value.len());
            ffi::lua_rawseti_(state, -2, index as c_int + 1);
        }

        ffi::lua_rawset(state, ffi::LUA_GLOBALSINDEX);
    }
}

fn status_table(lua: &Lua, message: &mlua::LuaString) -> LuaResult<Table> {
    let table = lua.create_table()?;

    table.set("ok", lua.create_string(message.as_bytes())?)?;
    Ok(table)
}

fn error_table(lua: &Lua, message: &mlua::LuaString) -> LuaResult<Table> {
    let table = lua.create_table()?;

    table.set("err", lua.create_string(message.as_bytes())?)?;
    Ok(table)
}

/// Runs the command whose arguments are the whole Lua stack and pushes its reply.
///
/// # Safety
/// `state` is the stack `ruvio.call` was called with, and the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn dispatch_call<C, I, W>(
    host: &RefCell<EvalHost<'_, C, I, W>>,
    state: *mut ffi::lua_State,
    budget: &mut LuaBudget<'_>,
) -> Result<(), CallFailure>
where
    C: Clock,
    I: StorageIndex,
    W: ListWaiterSink,
{
    let mut host = host.borrow_mut();

    // Decided before the command runs: a command that ran is never reported as failed
    // for memory and then kept.
    if host.aborted || (budget.left == 0 && budget.over_limit()) {
        return Err(CallFailure::Memory);
    }

    host.calls += 1;

    if host.calls > host.limits.max_calls {
        return Err(CallFailure::Fatal(Cow::Borrowed(
            b"ERR Lua ruvio.call quota exceeded",
        )));
    }

    const BAD_ARGUMENT: &[u8] = b"ERR Lua ruvio.call arguments must be strings or integers";
    // SAFETY: the caller's stack.
    let count = unsafe { ffi::lua_gettop(state) };

    if count == 0 {
        return Err(CallFailure::Fatal(Cow::Borrowed(BAD_ARGUMENT)));
    }

    let mut tokens = Vec::with_capacity(count as usize);

    for index in 1..=count {
        // SAFETY: `index` is on the stack, and the arguments stay there, below anything
        // this call pushes, until it returns; `tokens` and the command die before that.
        match unsafe { stack_argument(state, index) } {
            Some(token) => tokens.push(token),
            None => return Err(CallFailure::Fatal(Cow::Borrowed(BAD_ARGUMENT))),
        }
    }

    let command = match parse_arguments(tokens) {
        Ok(command) => command,
        Err(ProtocolError::Invalid(message)) => {
            return Err(CallFailure::Command(Cow::Owned(
                format!("ERR {message}").into_bytes(),
            )));
        }

        Err(_) => {
            return Err(CallFailure::Command(Cow::Borrowed(
                b"ERR invalid ruvio.call arguments",
            )));
        }
    };

    match crate::protocol::validate_payloads(&command, host.payload) {
        Ok(()) => {}
        Err(ProtocolError::LimitExceeded(message) | ProtocolError::Invalid(message)) => {
            return Err(CallFailure::Command(Cow::Owned(
                format!("ERR {message}").into_bytes(),
            )));
        }
        Err(_) => {
            return Err(CallFailure::Command(Cow::Borrowed(
                b"ERR invalid ruvio.call arguments",
            )));
        }
    }

    if !scriptable(&command) {
        return Err(CallFailure::Command(Cow::Borrowed(
            b"ERR command not allowed from script source",
        )));
    }

    if let Some(rules) = &host.acl
        && let Err(deny) = rules.authorize(&command)
    {
        return Err(CallFailure::Command(
            match crate::acl::noperm_response(deny) {
                Response::OwnedError(message) => Cow::Owned(message),
                Response::Error(message) => Cow::Borrowed(message),
                _ => Cow::Borrowed(b"NOPERM script command refused"),
            },
        ));
    }

    if !keys_allowed(host.allowed, &command) {
        return Err(CallFailure::Command(Cow::Borrowed(
            b"ERR script attempted to access undeclared keys",
        )));
    }

    let EvalHost {
        shard,
        collected,
        waiters,
        database,
        undo,
        ..
    } = &mut *host;

    if crate::protocol::command_mutates_store(&command) {
        save_before_write(shard, undo, *database, &command);
    }

    let response = match execute_in_database(*shard, command, collected, *waiters, *database) {
        Ok(response) => response,
        Err(error) => match error {},
    };

    match response {
        Response::Error(message) => Err(CallFailure::Command(Cow::Borrowed(message))),
        Response::OwnedError(message) => Err(CallFailure::Command(Cow::Owned(message))),
        response => {
            // SAFETY: `state` is the running script's stack, and the memory limit is lifted
            // around `ruvio.call` (see `set_globals`).
            unsafe { push_reply(state, response, budget) }.map_err(|failure| match failure {
                PushFailure::Memory => CallFailure::Memory,
                PushFailure::Refused(message) => CallFailure::Fatal(Cow::Borrowed(message)),
            })
        }
    }
}

/// The `ruvio.call` argument at `index`: a string as it sits in Lua, or a number in
/// the decimal form `tostring` gives an integer.
///
/// # Safety
/// `index` is a valid index of `state`; the slice lives while that value stays there.
#[allow(unsafe_code)]
unsafe fn stack_argument<'s>(state: *mut ffi::lua_State, index: c_int) -> Option<Bytes<'s>> {
    // SAFETY: guaranteed by the caller. `lua_tolstring` on a string neither converts nor
    // allocates, and Lua strings never move while referenced from the stack.
    unsafe {
        match ffi::lua_type(state, index) {
            ffi::LUA_TSTRING => {
                let mut len = 0;
                let ptr = ffi::lua_tolstring(state, index, &mut len);

                Some(Cow::Borrowed(std::slice::from_raw_parts(ptr.cast(), len)))
            }

            ffi::LUA_TNUMBER => {
                let value = ffi::lua_tonumber(state, index);
                let text = if value.fract() == 0.0 && value.abs() <= i64::MAX as f64 {
                    (value as i64).to_string()
                } else {
                    value.to_string()
                };

                Some(Cow::Owned(text.into_bytes()))
            }

            _ => None,
        }
    }
}

/// Copies each key `command` may write the first time the script writes it, so keys the
/// script only reads are never copied.
fn save_before_write<C: Clock, I: StorageIndex>(
    shard: &mut KvShard<C, I>,
    undo: &mut Vec<ScriptUndo>,
    database: u8,
    command: &Command<'_>,
) {
    shard.enter(database);

    for key in crate::engine::written_keys(command) {
        if undo.iter().any(|saved| saved.key.as_ref() == key) {
            continue;
        }

        #[cfg(test)]
        crate::engine::UNDO_COPIES.set(crate::engine::UNDO_COPIES.get() + 1);
        undo.push(ScriptUndo {
            key: key.into(),
            entry: shard.clone_entry(key),
            deadline: shard.deadline(database, key),
        });
    }
}

fn scriptable(command: &Command<'_>) -> bool {
    !crate::protocol::command_may_block(command)
        && !matches!(
            command,
            Command::Wait { .. }
                | Command::Save
                | Command::Bgsave
                | Command::Hello { .. }
                | Command::ClientGetName
                | Command::ClientSetName(_)
                | Command::ClientTracking(_)
                | Command::ShardPublish { .. }
                | Command::ShardSubscribe(_)
                | Command::ShardUnsubscribe(_)
                | Command::Publish { .. }
                | Command::Subscribe(_)
                | Command::Unsubscribe(_)
                | Command::PSubscribe(_)
                | Command::PUnsubscribe(_)
                | Command::Auth { .. }
                | Command::Acl(_)
                | Command::ClusterSlots
                | Command::ClusterNodes
                | Command::ClusterInfo
                | Command::ClusterShards
                | Command::ClusterMyId
                | Command::ClusterKeySlot(_)
                | Command::ReadOnly
                | Command::ReadWrite
                | Command::ConfigGet(_)
                | Command::Info(_)
                | Command::Multi
                | Command::Exec
                | Command::Discard
                | Command::Watch(_)
                | Command::Unwatch
                | Command::Eval { .. }
                | Command::EvalSha { .. }
                | Command::ScriptLoad(_)
                | Command::ScriptExists(_)
                | Command::ScriptFlush
                | Command::ScriptKill
                | Command::Select(_)
                | Command::FlushDb
                | Command::FlushAll
                | Command::SwapDb(_, _)
                | Command::Move { .. }
                | Command::FCall { .. }
                | Command::FunctionLoad { .. }
                | Command::FunctionList
                | Command::FunctionDelete(_)
                | Command::ClientId
                | Command::ClientInfo
                | Command::ClientList { .. }
                | Command::ClientKill { .. }
                | Command::ChangesSubscribe(_)
                | Command::JsonIndex { .. }
        )
}

fn keys_allowed(allowed: &[Bytes<'_>], command: &Command<'_>) -> bool {
    let keys = command_keys(command);

    if allowed.is_empty() {
        return keys.is_empty();
    }

    keys.iter()
        .all(|key| allowed.iter().any(|allowed| allowed.as_ref() == *key))
}

/// Deepest table nesting converted between Lua tables and replies, either way. Both
/// conversions recurse once per level, and a table can even contain itself. Lua 5.1's own
/// parser allows the same nesting of table constructors.
const MAX_REPLY_DEPTH: usize = 200;

const STACK_LIMIT_ERROR: &[u8] = b"ERR reached lua stack limit";

/// Largest reply a script may return, estimated as it is converted. A table reachable many
/// times over from the result would otherwise expand without bound: a few lines of Lua can
/// repeat one 1 MiB string hundreds of times, and the conversion work is not covered by the
/// script's instruction quota or deadline, so the cap also bounds its time.
const MAX_SCRIPT_REPLY_BYTES: usize = 64 * 1024 * 1024;

/// Pushes `response` as the Lua value a script gets for it, as Redis converts replies.
/// On failure the stack is left as it was found.
///
/// # Safety
/// `state` is a running Lua stack and the memory limit is lifted (see [`set_globals`]).
#[allow(unsafe_code)]
unsafe fn push_reply(
    state: *mut ffi::lua_State,
    response: Response<'_>,
    budget: &mut LuaBudget<'_>,
) -> Result<(), PushFailure> {
    // SAFETY: guaranteed by the caller.
    unsafe {
        let top = ffi::lua_gettop(state);
        let pushed = push_response(state, response, budget, 0);

        if pushed.is_err() {
            ffi::lua_settop(state, top);
        }

        pushed
    }
}

/// # Safety
/// As [`push_reply`]; on failure the stack may hold a partly built value.
#[allow(unsafe_code)]
unsafe fn push_response(
    state: *mut ffi::lua_State,
    response: Response<'_>,
    budget: &mut LuaBudget<'_>,
    depth: usize,
) -> Result<(), PushFailure> {
    // SAFETY: guaranteed by the caller; each level checks its own slots, and indexes fit
    // `c_int` because each table's length did. Every value is charged before it is made.
    unsafe {
        if ffi::lua_checkstack(state, 4) == 0 {
            return Err(PushFailure::Refused(b"ERR Lua stack is full"));
        }

        match response {
            Response::Simple(value) => push_status_charged(state, budget, c"ok", value)?,
            Response::OwnedSimple(value) => push_status_charged(state, budget, c"ok", &value)?,
            Response::Error(message) => push_status_charged(state, budget, c"err", message)?,
            Response::OwnedError(message) => {
                push_status_charged(state, budget, c"err", &message)?;
            }
            Response::Bulk(Some(value)) => push_bytes_charged(state, budget, &value)?,
            Response::JsonTree(tree) => push_bytes_charged(state, budget, &tree.to_vec())?,
            Response::Bulk(None) | Response::NullArray => ffi::lua_pushboolean(state, 0),
            Response::Integer(value) => ffi::lua_pushnumber(state, value as ffi::lua_Number),
            Response::Bool(value) => ffi::lua_pushnumber(state, f64::from(u8::from(value))),
            Response::Double(value) => {
                push_bytes_charged(state, budget, &crate::engine::format_score(value))?;
            }
            Response::OneInteger(value) => {
                push_table(state, budget, 1)?;

                match value {
                    Some(value) => ffi::lua_pushnumber(state, value as ffi::lua_Number),
                    None => ffi::lua_pushboolean(state, 0),
                }

                ffi::lua_rawseti_(state, -2, 1);
            }

            Response::Array(values) => {
                push_table(state, budget, values.len())?;

                for (index, value) in values.iter().enumerate() {
                    match value {
                        Some(value) => push_item(state, budget, value, index)?,
                        None => {
                            ffi::lua_pushboolean(state, 0);
                            ffi::lua_rawseti_(state, -2, index as c_int + 1);
                        }
                    }
                }
            }

            Response::Set(members) => push_strings(state, budget, &members)?,
            // Streamed replies go straight from the store into Lua, with no owned copy.
            Response::Members(stream) => {
                push_table(state, budget, stream.len())?;
                let mut index = 0;

                stream.visit(|member| {
                    push_item(state, budget, member, index)?;
                    index += 1;
                    Ok(())
                })?;
            }

            Response::ListSlice(stream) => {
                push_table(state, budget, stream.len())?;
                let mut index = 0;

                stream.visit(|item| {
                    push_item(state, budget, item, index)?;
                    index += 1;
                    Ok(())
                })?;
            }

            Response::Scored(entries, with_scores) => {
                let per_entry = 1 + usize::from(with_scores);

                push_table(state, budget, entries.len().saturating_mul(per_entry))?;

                for (position, (member, score)) in entries.into_iter().enumerate() {
                    let index = position * per_entry;

                    push_item(state, budget, member, index)?;

                    if with_scores {
                        push_item(
                            state,
                            budget,
                            &crate::engine::format_score(score),
                            index + 1,
                        )?;
                    }
                }
            }

            Response::Popped(key, entries) => {
                let first = usize::from(key.is_some());

                push_table(state, budget, first + entries.len().saturating_mul(2))?;

                if let Some(key) = key {
                    push_item(state, budget, &key, 0)?;
                }

                for (position, (member, score)) in entries.iter().enumerate() {
                    let index = first + position * 2;

                    push_item(state, budget, member, index)?;
                    push_item(
                        state,
                        budget,
                        &crate::engine::format_score(score),
                        index + 1,
                    )?;
                }
            }

            Response::HashEntries(stream, kind) => {
                use crate::store::HashStreamKind;

                let per_entry = if matches!(kind, HashStreamKind::Pairs) {
                    2
                } else {
                    1
                };
                let mut index = 0;

                push_table(state, budget, stream.len().saturating_mul(per_entry))?;
                stream.visit(|field, value| {
                    match kind {
                        HashStreamKind::Pairs => {
                            push_item(state, budget, field, index)?;
                            push_item(state, budget, value, index + 1)?;
                        }
                        HashStreamKind::Fields => push_item(state, budget, field, index)?,
                        HashStreamKind::Values => push_item(state, budget, value, index)?,
                    }

                    index += per_entry;
                    Ok(())
                })?;
            }

            Response::Scan { cursor, keys } => {
                push_table(state, budget, 2)?;
                push_item(state, budget, cursor.to_string().as_bytes(), 0)?;
                push_strings(state, budget, &keys)?;
                ffi::lua_rawseti_(state, -2, 2);
            }

            Response::Exec(values) => {
                if depth >= MAX_REPLY_DEPTH {
                    return Err(PushFailure::Refused(STACK_LIMIT_ERROR));
                }

                push_table(state, budget, values.len())?;

                for (index, value) in values.into_iter().enumerate() {
                    push_response(state, value, budget, depth + 1)?;
                    ffi::lua_rawseti_(state, -2, index as c_int + 1);
                }
            }

            Response::Map(pairs) => {
                if depth >= MAX_REPLY_DEPTH {
                    return Err(PushFailure::Refused(STACK_LIMIT_ERROR));
                }

                push_table(state, budget, pairs.len().saturating_mul(2))?;

                for (index, (key, value)) in pairs.into_iter().enumerate() {
                    let slot = index * 2;

                    push_item(state, budget, &key, slot)?;
                    push_response(state, value, budget, depth + 1)?;
                    ffi::lua_rawseti_(state, -2, slot as c_int + 2);
                }
            }

            Response::BlockList { .. }
            | Response::BlockStream { .. }
            | Response::BlockGroup { .. }
            | Response::BlockRetry { .. } => {
                return Err(PushFailure::Refused(
                    b"ERR command not allowed from script source",
                ));
            }
        }
    }

    Ok(())
}

/// Pushes an empty table sized for `count` array items, charged first.
///
/// # Safety
/// One free stack slot; the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn push_table(
    state: *mut ffi::lua_State,
    budget: &mut LuaBudget<'_>,
    count: usize,
) -> Result<(), PushFailure> {
    let Ok(slots) = c_int::try_from(count) else {
        return Err(PushFailure::Refused(b"ERR Lua reply is too large"));
    };

    budget.charge(LUA_TABLE_COST)?;
    budget.charge_many(count, LUA_SLOT_COST)?;
    // SAFETY: guaranteed by the caller.
    unsafe { ffi::lua_createtable(state, slots, 0) };
    Ok(())
}

/// Sets item `index` (from 0) of the table on top to the string `value`, charged first.
/// `index` fits `c_int`: the table was created with room for it.
///
/// # Safety
/// A table on top, one free slot above it; the memory limit is lifted.
#[allow(unsafe_code)]
#[inline]
unsafe fn push_item(
    state: *mut ffi::lua_State,
    budget: &mut LuaBudget<'_>,
    value: &[u8],
    index: usize,
) -> Result<(), PushFailure> {
    budget.charge(value.len().saturating_add(LUA_STRING_COST))?;
    // SAFETY: guaranteed by the caller.
    unsafe {
        push_bytes(state, value);
        ffi::lua_rawseti_(state, -2, index as c_int + 1);
    }
    Ok(())
}

/// # Safety
/// One free stack slot; the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn push_bytes_charged(
    state: *mut ffi::lua_State,
    budget: &mut LuaBudget<'_>,
    value: &[u8],
) -> Result<(), PushFailure> {
    budget.charge(value.len().saturating_add(LUA_STRING_COST))?;
    // SAFETY: guaranteed by the caller.
    unsafe { push_bytes(state, value) };
    Ok(())
}

/// # Safety
/// Three free stack slots; the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn push_status_charged(
    state: *mut ffi::lua_State,
    budget: &mut LuaBudget<'_>,
    field: &std::ffi::CStr,
    value: &[u8],
) -> Result<(), PushFailure> {
    budget.charge(
        value
            .len()
            .saturating_add(LUA_TABLE_COST + LUA_NODE_COST + 2 * LUA_STRING_COST),
    )?;
    // SAFETY: guaranteed by the caller.
    unsafe { push_status(state, field, value) };
    Ok(())
}

/// # Safety
/// One free stack slot; the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn push_bytes(state: *mut ffi::lua_State, value: &[u8]) {
    // SAFETY: guaranteed by the caller.
    unsafe { ffi::lua_pushlstring_(state, value.as_ptr().cast(), value.len()) }
}

/// Pushes `{[field] = value}`, the table Redis uses for status and error replies.
///
/// # Safety
/// Three free stack slots; the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn push_status(state: *mut ffi::lua_State, field: &std::ffi::CStr, value: &[u8]) {
    // SAFETY: guaranteed by the caller.
    unsafe {
        ffi::lua_createtable(state, 0, 1);
        push_bytes(state, field.to_bytes());
        push_bytes(state, value);
        ffi::lua_rawset(state, -3);
    }
}

/// Pushes an array of strings.
///
/// # Safety
/// Two free stack slots; the memory limit is lifted.
#[allow(unsafe_code)]
unsafe fn push_strings(
    state: *mut ffi::lua_State,
    budget: &mut LuaBudget<'_>,
    values: &[Cow<'_, [u8]>],
) -> Result<(), PushFailure> {
    // SAFETY: guaranteed by the caller.
    unsafe {
        push_table(state, budget, values.len())?;

        for (index, value) in values.iter().enumerate() {
            push_item(state, budget, value, index)?;
        }
    }

    Ok(())
}

/// Converts a script's result into its reply. `depth` is the table nesting so far and
/// `budget` the reply bytes still allowed.
fn lua_to_response(
    value: Value,
    depth: usize,
    budget: &mut usize,
) -> Result<Response<'static>, &'static [u8]> {
    const TOO_LARGE: &[u8] = b"ERR Lua reply is too large";
    let mut charge = |bytes: usize| {
        *budget = budget
            .checked_sub(bytes.saturating_add(std::mem::size_of::<Response<'static>>()))
            .ok_or(TOO_LARGE)?;
        Ok::<(), &'static [u8]>(())
    };

    Ok(match value {
        Value::Nil | Value::Boolean(false) => Response::Bulk(None),
        Value::Boolean(true) => Response::Integer(1),
        Value::Integer(value) => Response::Integer(value),
        Value::Number(value) if value.fract() == 0.0 && value.abs() <= i64::MAX as f64 => {
            Response::Integer(value as i64)
        }

        Value::Number(value) => {
            let text = value.to_string().into_bytes();

            charge(text.len())?;
            Response::Bulk(Some(std::borrow::Cow::Owned(text)))
        }

        Value::String(value) => {
            let bytes = value.as_bytes();

            charge(bytes.len())?;
            Response::Bulk(Some(std::borrow::Cow::Owned(bytes.to_vec())))
        }

        Value::Table(table) => {
            if depth >= MAX_REPLY_DEPTH {
                return Err(STACK_LIMIT_ERROR);
            }

            // Raw reads: a returned table's `__index` must not run Lua code here.
            if let Ok(err) = table.raw_get::<mlua::LuaString>("err") {
                charge(err.as_bytes().len())?;
                return Ok(owned_error(&err));
            }

            if let Ok(ok) = table.raw_get::<mlua::LuaString>("ok") {
                charge(ok.as_bytes().len())?;
                return Ok(owned_simple(&ok));
            }

            let len = table.raw_len();

            charge(len.saturating_mul(std::mem::size_of::<Response<'static>>()))?;
            let mut items = Vec::with_capacity(len);

            for index in 1..=len {
                let nested = table
                    .raw_get::<Value>(index)
                    .map_err(|_| UNSUPPORTED_VALUE)?;

                items.push(lua_to_response(nested, depth + 1, budget)?);
            }

            Response::Exec(items)
        }

        _ => return Err(UNSUPPORTED_VALUE),
    })
}

const UNSUPPORTED_VALUE: &[u8] = b"ERR Lua ruvio.call returned an unsupported value";

/// A status or error line is one line of the protocol: a line break inside it would end it
/// early, and whatever followed would be read by the client as further replies.
fn single_line(message: &[u8]) -> Vec<u8> {
    message
        .iter()
        .map(|&byte| {
            if byte == b'\r' || byte == b'\n' {
                b' '
            } else {
                byte
            }
        })
        .collect()
}

fn owned_error(message: &mlua::LuaString) -> Response<'static> {
    Response::OwnedError(single_line(&message.as_bytes()))
}

fn owned_simple(message: &mlua::LuaString) -> Response<'static> {
    let bytes = message.as_bytes();

    if bytes.as_ref() == b"OK" {
        Response::Simple(b"OK")
    } else if bytes.as_ref() == b"PONG" {
        Response::Simple(b"PONG")
    } else {
        Response::OwnedSimple(single_line(&bytes))
    }
}

/// [`crate::engine::OOM_ERROR`] as text, found in the Lua error a refused `ruvio.call` raises.
const OOM_TEXT: &str = match std::str::from_utf8(crate::engine::OOM_ERROR) {
    Ok(text) => text,
    Err(_) => panic!("the OOM error is ASCII"),
};

fn lua_error_response(error: &mlua::Error) -> Response<'static> {
    let message = error.to_string();

    if matches!(error, mlua::Error::MemoryError(_))
        || message.contains("not enough memory")
        || message.contains("exceeded the memory limit")
    {
        Response::Error(b"ERR Lua script exceeded the memory limit")
    } else if message.contains("readonly table") {
        Response::Error(b"ERR Attempt to modify a readonly table")
    } else if message.contains("pattern too complex") {
        Response::Error(b"ERR pattern too complex")
    } else if message.contains(OOM_TEXT) {
        // A command the script called was refused under `maxmemory`: the client sees that
        // error, as in Redis, not a generic script failure.
        Response::Error(crate::engine::OOM_ERROR)
    } else if message.contains("instruction quota") {
        Response::Error(b"ERR Lua instruction quota exceeded")
    } else if message.contains("script time limit") {
        Response::Error(b"ERR Lua script time limit exceeded")
    } else if message.contains("call quota") {
        Response::Error(b"ERR Lua ruvio.call quota exceeded")
    } else if message.contains("undeclared keys") {
        Response::Error(b"ERR script attempted to access undeclared keys")
    } else if message.contains("not allowed from script") {
        Response::Error(b"ERR command not allowed from script source")
    } else if message.contains("no permissions to run") {
        Response::Error(b"NOPERM this user has no permissions to run a command the script called")
    } else if message.contains("no permissions to access") {
        Response::Error(b"NOPERM this user has no permissions to access one or more keys")
    } else if message.contains("WRONGTYPE") {
        Response::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    } else if message.contains("key is too large") {
        // A `ruvio.call` command over the protocol limits, as a client's would be.
        Response::Error(b"ERR key is too large")
    } else if message.contains("value is too large") {
        Response::Error(b"ERR value is too large")
    } else if message.contains("too large") {
        Response::Error(b"ERR script is too large")
    } else {
        Response::Error(b"ERR Error running script")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keyless commands a script may call. Every other keyless command is refused, and
    /// each one the parser builds is in exactly one of the two lists: a new keyless command
    /// fails here until someone decides. Redis refuses `CLIENT`, `FUNCTION` and `FCALL` in
    /// scripts; `JSON.INDEX` and `CHANGES SUBSCRIBE` act beyond the script's shard and call.
    const KEYLESS_IN_SCRIPTS: &[&str] = &[
        "Changes",
        "CommandMeta",
        "DbSize",
        "Echo",
        "Keys",
        "LatencyLatest",
        "Ping",
        "RandomKey",
        "Ruvio",
        "Scan",
        "Slowlog",
        "StreamGroupRead",
    ];
    const KEYLESS_REFUSED_IN_SCRIPTS: &[&str] = &[
        "Acl",
        "Auth",
        "Bgsave",
        "ChangesSubscribe",
        "ClientGetName",
        "ClientId",
        "ClientInfo",
        "ClientKill",
        "ClientList",
        "ClientSetName",
        "ClientTracking",
        "ClusterInfo",
        "ClusterKeySlot",
        "ClusterMyId",
        "ClusterNodes",
        "ClusterShards",
        "ClusterSlots",
        "ConfigGet",
        "Discard",
        "Eval",
        "EvalSha",
        "Exec",
        "FCall",
        "FlushAll",
        "FlushDb",
        "FunctionDelete",
        "FunctionList",
        "FunctionLoad",
        "Hello",
        "Info",
        "JsonIndex",
        "Multi",
        "PSubscribe",
        "PUnsubscribe",
        "Publish",
        "ReadOnly",
        "ReadWrite",
        "Save",
        "ScriptExists",
        "ScriptFlush",
        "ScriptKill",
        "ScriptLoad",
        "Select",
        "ShardPublish",
        "ShardSubscribe",
        "ShardUnsubscribe",
        "Subscribe",
        "SwapDb",
        "Unsubscribe",
        "Unwatch",
        "Wait",
    ];

    #[test]
    fn every_keyless_command_is_decided_for_scripts() {
        let mut undecided = Vec::new();
        let mut wrong = Vec::new();

        for (variant, command) in crate::protocol::consistency::keyless_forms() {
            let allowed = KEYLESS_IN_SCRIPTS.contains(&variant.as_str());

            if !allowed && !KEYLESS_REFUSED_IN_SCRIPTS.contains(&variant.as_str()) {
                undecided.push(variant);
            } else if scriptable(&command) != allowed {
                wrong.push(variant);
            }
        }

        assert!(
            undecided.is_empty(),
            "keyless commands with no script decision: {undecided:?}"
        );
        assert!(
            wrong.is_empty(),
            "scriptable disagrees with the lists: {wrong:?}"
        );
    }
    use crate::{KvShard, mutation::NoopMutationSink};

    fn owned(value: &[u8]) -> Bytes<'static> {
        Cow::Owned(value.to_vec())
    }

    struct RefusingSink;

    impl MutationSink for RefusingSink {
        type Error = ();

        fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
            Err(())
        }
    }

    /// `ruvio.call` meets the key and value byte limits a client's command meets.
    #[test]
    fn a_script_cannot_write_a_value_over_the_value_limit() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::with_payload_limits(crate::protocol::ProtocolLimits {
            max_value_bytes: 16,
            ..crate::protocol::ProtocolLimits::default()
        });
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            roomy_limits(),
            b"return ruvio.call('SET', KEYS[1], string.rep('x', 100))",
            &[owned(b"big")],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(response, Response::Error(b"ERR value is too large"));
        assert_eq!(shard.get(b"big"), None);
    }

    #[test]
    fn a_script_hands_blocked_clients_nothing_unless_its_writes_are_recorded() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut waiters = crate::list_waiters::ListWaiters::new(4);
        let slot = crate::list_waiters::EncodedReplySlot::default();
        waiters
            .register(
                vec![crate::database::tagged(0, b"q")],
                None,
                slot.clone(),
                Vec::new(),
            )
            .unwrap();
        let body = b"ruvio.call('RPUSH', KEYS[1], 'a', 'b'); return 1";
        let refused = execute_eval(
            &mut shard,
            &mut RefusingSink,
            &mut waiters,
            &mut cache,
            ScriptLimits::default(),
            body,
            &[owned(b"q")],
            &[],
            0,
        );

        assert!(refused.is_err());
        assert_eq!(slot.take(), None);
        assert_eq!(waiters.len(), 1, "the client is still parked");

        let failed = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut waiters,
            &mut cache,
            ScriptLimits::default(),
            b"ruvio.call('RPUSH', KEYS[1], 'a'); return ruvio.call('NOSUCHCOMMAND')",
            &[owned(b"q")],
            &[],
            0,
        )
        .unwrap();

        assert!(matches!(
            failed,
            Response::Error(_) | Response::OwnedError(_)
        ));
        assert_eq!(slot.take(), None, "a script that failed hands out nothing");
        assert_eq!(waiters.len(), 1);
    }

    #[test]
    fn a_repeated_script_keeps_one_compiled_function() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let body = b"local n = 0 for i = 1, 2 do n = n + i end return n";

        for expected in [3, 3] {
            let response = execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                &[],
                &[],
                0,
            )
            .unwrap();

            assert_eq!(response, Response::Integer(expected));
        }

        assert_eq!(cache.vm.as_ref().unwrap().compiled.len(), 1);
        let first = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"return KEYS[1]",
            &[owned(b"a")],
            &[],
            0,
        )
        .unwrap();
        let second = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"return KEYS[1]",
            &[owned(b"b")],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(first, Response::Bulk(Some(Cow::Borrowed(b"a"))));
        assert_eq!(second, Response::Bulk(Some(Cow::Borrowed(b"b"))));
    }

    #[test]
    fn a_script_returning_a_cyclic_or_deeply_nested_table_fails_without_crashing() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut run = |body: &[u8]| {
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                &[],
                &[],
                0,
            )
            .unwrap()
        };

        for body in [
            b"local t = {} t[1] = t return t" as &[u8],
            b"local t = {} for i = 1, 100 do t[i] = t end return t",
            b"local t = {} for i = 1, 100000 do t = {t} end return t",
            b"local t = {} t[1] = {err = t} t[2] = t return t",
        ] {
            assert_eq!(
                run(body),
                Response::Error(STACK_LIMIT_ERROR),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        // Shared, not cyclic: 2^40 leaves from 41 tables.
        assert_eq!(
            run(b"local t = {'leaf'} for i = 1, 40 do t = {t, t} end return t"),
            Response::Error(b"ERR Lua reply is too large")
        );

        // Nesting up to the limit is still a reply, and it can be encoded.
        let nest = |tables: usize| {
            format!("local t = {{7}} for i = 2, {tables} do t = {{t}} end return t").into_bytes()
        };

        assert_eq!(
            run(&nest(MAX_REPLY_DEPTH + 1)),
            Response::Error(STACK_LIMIT_ERROR)
        );
        let deep = run(&nest(MAX_REPLY_DEPTH));
        let mut tables = 0;
        let mut level = &deep;

        while let Response::Exec(items) = level {
            level = &items[0];
            tables += 1;
        }

        assert_eq!((tables, level), (MAX_REPLY_DEPTH, &Response::Integer(7)));
        let mut wire = Vec::new();
        crate::engine::write_response(&mut wire, deep).unwrap();
        assert!(wire.ends_with(b":7\r\n"));
        assert_eq!(
            run(b"return 'still running'"),
            Response::Bulk(Some(owned(b"still running")))
        );
    }

    #[test]
    fn a_deeply_nested_reply_is_refused_when_pushed_to_lua() {
        // Dropping 20 000 nested replies recurses too; give it room.
        std::thread::Builder::new()
            .stack_size(256 << 20)
            .spawn(|| {
                let lua = Lua::new();
                let mut nested = Response::Integer(1);

                for _ in 0..20_000 {
                    nested = Response::Exec(vec![nested]);
                }

                let mut shallow = Response::Integer(1);

                for _ in 0..MAX_REPLY_DEPTH {
                    shallow = Response::Map(vec![(owned(b"k"), shallow)]);
                }

                // SAFETY: no memory limit is set on this state.
                #[allow(unsafe_code)]
                lua.exec_raw_lua(|raw| unsafe {
                    let state = raw.state();
                    let top = ffi::lua_gettop(state);
                    let mut budget = LuaBudget::new(&lua, state);

                    assert_eq!(
                        push_reply(state, nested, &mut budget),
                        Err(PushFailure::Refused(STACK_LIMIT_ERROR))
                    );
                    assert_eq!(ffi::lua_gettop(state), top, "the partial reply is popped");
                    assert_eq!(push_reply(state, shallow, &mut budget), Ok(()));
                    ffi::lua_settop(state, top);
                });
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn scripts_cannot_change_what_later_scripts_run() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut run = |body: &str| {
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body.as_bytes(),
                &[owned(b"{t}k")],
                &[owned(b"arg")],
                0,
            )
            .unwrap()
        };
        let readonly = Response::Error(b"ERR Attempt to modify a readonly table");
        let victim = "return ruvio.call('PING')";

        // Compiled and cached before the attempts, so the cached function is checked too.
        assert_eq!(run(victim), Response::Simple(b"PONG"));

        for attack in [
            "ruvio.call = function() return 'hijacked' end",
            "ruvio = {call = function() return 'hijacked' end}",
            "_G.ruvio = {call = function() return 'hijacked' end}",
            "hijacked = true",
            "KEYS = {'evil'}",
            "string.rep = function() return 'hijacked' end",
            "table.insert = nil",
            "math.floor = nil",
            "_G = {}",
        ] {
            // Twice: the second run takes the cached function.
            for _ in 0..2 {
                assert_eq!(run(attack), readonly, "{attack}");
            }
        }

        for attack in [
            "setmetatable(_G, nil)",
            "setmetatable(ruvio, {})",
            "getmetatable('').__index.rep = nil",
            "setfenv(0, {})",
            "getfenv(0).ruvio = {}",
        ] {
            assert!(
                matches!(run(attack), Response::Error(_)),
                "{attack} succeeded"
            );
        }

        // Raw writes are refused too; on a script's own tables they still work.
        for attack in [
            "rawset(ruvio, 'call', function() return 'hijacked' end)",
            "rawset(_G, 'KEYS', {'evil'})",
            "table.insert(_G, 1)",
            "table.insert(string, 1, 'x')",
            "table.remove(ruvio)",
            "table.sort(_G)",
        ] {
            assert_eq!(run(attack), readonly, "{attack}");
        }

        assert_eq!(
            run(
                "local t = {3, 1} rawset(t, 'k', 'v') table.insert(t, 2) table.insert(t, 1, 0) \
                 table.remove(t, 1) table.sort(t) return {t[1], t[2], t[3], t.k}"
            ),
            Response::Exec(vec![
                Response::Integer(1),
                Response::Integer(2),
                Response::Integer(3),
                Response::Bulk(Some(owned(b"v"))),
            ])
        );

        assert_eq!(run(victim), Response::Simple(b"PONG"));
        assert_eq!(
            run("return {KEYS[1], ARGV[1], type(hijacked), string.rep('ab', 2), #_G}"),
            Response::Exec(vec![
                Response::Bulk(Some(owned(b"{t}k"))),
                Response::Bulk(Some(owned(b"arg"))),
                Response::Bulk(Some(owned(b"nil"))),
                Response::Bulk(Some(owned(b"abab"))),
                Response::Integer(0),
            ])
        );
        // Ordinary scripts: locals, their own tables and metatables, library calls.
        assert_eq!(
            run(
                "local t = setmetatable({}, {__index = function(_, k) return k .. '!' end}) \
                 t.x = 1 local ok = pcall(error, 'e') \
                 return {t.x, t.y, tostring(ok), ('abc'):upper(), table.concat({1, 2}, ','), \
                 math.floor(2.5), string.format('%d', 3), select('#', unpack({1, 2}))}"
            ),
            Response::Exec(vec![
                Response::Integer(1),
                Response::Bulk(Some(owned(b"y!"))),
                Response::Bulk(Some(owned(b"false"))),
                Response::Bulk(Some(owned(b"ABC"))),
                Response::Bulk(Some(owned(b"1,2"))),
                Response::Integer(2),
                Response::Bulk(Some(owned(b"3"))),
                Response::Integer(2),
            ])
        );
        assert_eq!(
            run("local ok, e = pcall(function() undeclared = 1 end) \
                 return string.find(e, 'Attempt to modify a readonly table', 1, true) ~= nil"),
            Response::Integer(1)
        );
    }

    #[test]
    fn a_wrapper_left_by_one_user_cannot_run_under_the_next_users_acl() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut rules = crate::acl::AclRules::none();

        rules.commands.insert("set");
        rules.key_patterns.push(b"*".to_vec());
        let mut run = |body: &[u8], acl: Option<crate::acl::AclRules>, shard: &mut KvShard| {
            cache.restrict_next_script(acl.map(std::sync::Arc::new));
            execute_eval(
                shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                &[owned(b"{t}k")],
                &[],
                0,
            )
            .unwrap()
        };

        // A restricted user tries to leave a wrapper that writes a second key when the
        // next, unrestricted script calls `ruvio.call`.
        run(
            b"local call = ruvio.call \
              ruvio.call = function(...) call('SET', KEYS[1], 'planted') return call(...) end",
            Some(rules.clone()),
            &mut shard,
        );
        assert_eq!(
            run(b"return ruvio.call('GET', KEYS[1])", None, &mut shard),
            Response::Bulk(None)
        );
        assert_eq!(shard.get(b"{t}k"), None);
    }

    #[test]
    fn a_long_built_pattern_is_refused_instead_of_overflowing_the_stack() {
        let run = |body: &[u8]| eval_body(body, ScriptLimits::default());

        assert_eq!(
            run(b"return #string.match(string.rep('a', 200000), string.rep('a?', 200000))"),
            Response::Error(b"ERR pattern too complex")
        );
        assert_eq!(
            run(b"return #(string.rep('a', 200000)):gsub(string.rep('a-', 100000), '')"),
            Response::Error(b"ERR pattern too complex")
        );
        // Short patterns, long literal ones and plain finds are untouched.
        assert_eq!(
            run(b"local s = string.rep('ab', 1000) \
                  return {string.match('hello', 'h(.-)o'), string.find(s, s, 1, true), \
                  string.find(s, string.rep('ab', 500) .. '$'), select(2, string.gsub(s, 'a', 'c'))}"),
            Response::Exec(vec![
                Response::Bulk(Some(owned(b"ell"))),
                Response::Integer(1),
                Response::Integer(1001),
                Response::Integer(1000),
            ])
        );
    }

    #[test]
    fn a_huge_call_reply_is_refused_while_it_is_built() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        // Distinct items: Lua 5.1 interns every string, so equal ones would share memory.
        let items: Vec<Vec<u8>> = (0..20u8).map(|i| vec![b'a' + i; 2 << 20]).collect();

        shard.list_push_back(b"{t}list", &items).unwrap();
        PEAK_CALL_MEMORY.set(0);
        // 100 MiB of the script's own leaves about 28 MiB for a 40 MiB reply.
        let refused = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            roomy_limits(),
            b"local pad = {} for i = 1, 100 do pad[i] = string.rep('x', 2^20 - 64) .. i end \
              local ok, items = pcall(ruvio.call, 'LRANGE', KEYS[1], 0, -1) \
              return {tostring(ok), #pad}",
            &[owned(b"{t}list")],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(refused, Response::Error(MEMORY_LIMIT_ERROR));
        assert!(PEAK_CALL_MEMORY.get() > 100 << 20, "the call never ran");
        assert!(
            PEAK_CALL_MEMORY.get() <= SCRIPT_MEMORY_LIMIT,
            "the reply was built past the limit: {} bytes",
            PEAK_CALL_MEMORY.get()
        );
        assert_eq!(
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                roomy_limits(),
                b"return #ruvio.call('LRANGE', KEYS[1], 0, -1)",
                &[owned(b"{t}list")],
                &[],
                0,
            )
            .unwrap(),
            Response::Integer(20)
        );
    }

    #[test]
    fn a_command_whose_reply_passes_the_memory_limit_is_not_kept() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        // Distinct items: Lua 5.1 interns every string, so equal ones would share memory.
        let items: Vec<Vec<u8>> = (0..20u8).map(|i| vec![b'a' + i; 2 << 20]).collect();

        shard.list_push_back(b"{t}list", &items).unwrap();
        // The pop runs, its 40 MiB reply does not fit: the script fails as a whole even
        // though it caught the error, so the pop is undone rather than kept unreported.
        let reply = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            roomy_limits(),
            b"local pad = {} for i = 1, 100 do pad[i] = string.rep('x', 2^20 - 64) .. i end \
              pcall(ruvio.call, 'LPOP', KEYS[1], 20) \
              ruvio.pcall('SET', KEYS[2], 'after') \
              return 'survived'",
            &[owned(b"{t}list"), owned(b"{t}other")],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(reply, Response::Error(MEMORY_LIMIT_ERROR));
        assert_eq!(shard.list_len(b"{t}list").unwrap(), 20);
        assert_eq!(shard.get(b"{t}other"), None);
    }

    #[test]
    fn lua_global_is_named_ruvio() {
        assert_eq!(
            eval_body(b"return type(ruvio.call)", ScriptLimits::default()),
            Response::Bulk(Some(Cow::Borrowed(b"function")))
        );
        assert_eq!(
            eval_body(b"return type(redis)", ScriptLimits::default()),
            Response::Bulk(Some(Cow::Borrowed(b"nil")))
        );
    }

    #[test]
    fn large_json_root_reply_is_a_lua_string() {
        let lua = Lua::new();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "padding": "x".repeat(crate::json::TREE_THRESHOLD),
            "value": "line\n\"quoted\"",
        }))
        .unwrap();
        let tree = crate::json::TreeDocument::from_bytes(&bytes).unwrap();
        // SAFETY: no memory limit is set on this state.
        #[allow(unsafe_code)]
        let value = lua.exec_raw_lua(|raw| unsafe {
            let mut budget = LuaBudget::new(&lua, raw.state());

            push_reply(raw.state(), Response::JsonTree(&tree), &mut budget).unwrap();
            raw.pop::<Value>().unwrap()
        });
        let Value::String(result) = value else {
            panic!("expected Lua string");
        };
        let parsed: serde_json::Value = serde_json::from_slice(result.as_bytes().as_ref()).unwrap();
        assert_eq!(parsed["value"], "line\n\"quoted\"");
    }

    #[test]
    fn a_script_past_the_memory_limit_fails_and_the_vm_keeps_working() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut run = |body: &[u8]| {
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                roomy_limits(),
                body,
                &[],
                &[],
                0,
            )
            .unwrap()
        };

        assert_eq!(
            run(b"return #string.rep('x', 2^30)"),
            Response::Error(b"ERR Lua script exceeded the memory limit")
        );
        // Doubling a string takes few instructions but exponential memory.
        assert_eq!(
            run(b"local s = 'x' for i = 1, 40 do s = s .. s end return #s"),
            Response::Error(b"ERR Lua script exceeded the memory limit")
        );
        assert_eq!(
            run(b"local t = {} for i = 1, 200 do t[i] = string.rep('y', 2^20) .. i end return #t"),
            Response::Error(b"ERR Lua script exceeded the memory limit")
        );
        assert_eq!(
            run(b"return #string.rep('x', 2^20)"),
            Response::Integer(1 << 20)
        );
    }

    #[test]
    fn replies_a_script_keeps_count_toward_the_memory_limit() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let keys: Vec<Bytes<'_>> = (0..10)
            .map(|i| owned(format!("{{b}}big{i}").as_bytes()))
            .collect();

        for (i, key) in keys.iter().enumerate() {
            shard.set_bytes(key, &vec![b'a' + i as u8; 15 << 20]);
        }

        // Ten distinct 15 MiB replies held in locals: the script allocates nothing itself.
        let mut body = String::new();
        for i in 1..=10 {
            body.push_str(&format!("local v{i} = ruvio.call('GET', KEYS[{i}]) "));
        }
        body.push_str("return 1");
        let mut run = |body: &[u8], keys: &[Bytes<'_>]| {
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                keys,
                &[],
                0,
            )
            .unwrap()
        };

        assert_eq!(
            run(body.as_bytes(), &keys),
            Response::Error(b"ERR Lua script exceeded the memory limit")
        );
        assert_eq!(
            run(b"return #ruvio.call('GET', KEYS[1])", &keys[..1]),
            Response::Integer(15 << 20)
        );
        // No finalizers for scripts: one could raise inside the host's own calls.
        assert_eq!(
            run(b"return type(newproxy)", &[]),
            Response::Bulk(Some(owned(b"nil")))
        );
    }

    #[test]
    fn scripts_obey_the_acl_of_the_user_running_them() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut rules = crate::acl::AclRules::none();

        rules.commands.insert("get");
        rules.key_patterns.push(b"app:*".to_vec());

        let run = |body: &[u8], key: &[u8], cache: &mut ScriptCache, shard: &mut KvShard<_, _>| {
            cache.restrict_next_script(Some(std::sync::Arc::new(rules.clone())));
            execute_eval(
                shard,
                &mut NoopMutationSink,
                &mut (),
                cache,
                ScriptLimits::default(),
                body,
                &[Cow::Owned(key.to_vec())],
                &[],
                0,
            )
            .unwrap()
        };

        let denied = run(
            b"return ruvio.call('SET', KEYS[1], 'v')",
            b"app:1",
            &mut cache,
            &mut shard,
        );
        assert!(
            matches!(&denied, Response::Error(m) if m.starts_with(b"NOPERM"))
                || matches!(&denied, Response::OwnedError(m) if m.starts_with(b"NOPERM")),
            "{denied:?}"
        );
        assert!(
            shard.get(b"app:1").is_none(),
            "a refused SET writes nothing"
        );

        let wrong_key = run(
            b"return ruvio.call('GET', KEYS[1])",
            b"other:1",
            &mut cache,
            &mut shard,
        );
        assert!(
            matches!(&wrong_key, Response::Error(m) if m.starts_with(b"NOPERM"))
                || matches!(&wrong_key, Response::OwnedError(m) if m.starts_with(b"NOPERM")),
            "{wrong_key:?}"
        );

        let allowed = run(
            b"return ruvio.call('GET', KEYS[1])",
            b"app:1",
            &mut cache,
            &mut shard,
        );
        assert_eq!(allowed, Response::Bulk(None));
    }

    #[test]
    fn pcall_returns_every_refusal_as_an_error_table_and_call_raises_it() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut rules = crate::acl::AclRules::none();

        rules.commands.insert("get");
        rules.key_patterns.push(b"app:*".to_vec());
        shard.set_bytes(b"app:1", b"v");
        shard.set_bytes(b"other:1", b"v");

        let mut run = |body: &str, key: &[u8]| {
            cache.restrict_next_script(Some(std::sync::Arc::new(rules.clone())));
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body.as_bytes(),
                &[Cow::Owned(key.to_vec())],
                &[],
                0,
            )
            .unwrap()
        };
        // The table's `err` field, read inside the script; `nil` when pcall did not return one.
        let probe = |call: &str| {
            format!(
                "local r = ruvio.pcall({call}); if type(r) == 'table' and r.err then return 'err:' .. r.err end; return type(r)"
            )
        };

        for (call, key, expected) in [
            ("'SET', KEYS[1], 'x'", &b"app:1"[..], "err:NOPERM"),
            ("'GET', KEYS[1]", b"other:1", "err:NOPERM"),
            (
                "'GET', 'app:2'",
                b"app:1",
                "err:ERR script attempted to access undeclared keys",
            ),
            (
                "'BLPOP', KEYS[1], '0'",
                b"app:1",
                "err:ERR command not allowed from script source",
            ),
            ("'GET'", b"app:1", "err:ERR"),
            ("'LPUSH', KEYS[1], 'x'", b"app:1", "err:NOPERM"),
        ] {
            let reply = run(&probe(call), key);
            let text = match &reply {
                Response::Bulk(Some(text)) => String::from_utf8_lossy(text).into_owned(),
                other => format!("{other:?}"),
            };

            assert!(text.starts_with(expected), "pcall({call}) gave {text}");

            // `call` raises the same refusal: the script fails with an error reply.
            let raised = run(&format!("return ruvio.call({call})"), key);
            assert!(
                matches!(raised, Response::Error(_) | Response::OwnedError(_)),
                "call({call}) did not raise: {raised:?}"
            );
        }

        // An error table returned as the script's result is the client's error reply.
        let returned = run("return ruvio.pcall('SET', KEYS[1], 'x')", b"app:1");
        assert!(
            matches!(&returned, Response::Error(m) if m.starts_with(b"NOPERM"))
                || matches!(&returned, Response::OwnedError(m) if m.starts_with(b"NOPERM")),
            "{returned:?}"
        );
        assert_eq!(shard.get(b"app:1"), Some(&b"v"[..]), "a refused SET wrote");
    }

    #[test]
    fn a_call_refused_under_maxmemory_fails_the_script_with_the_oom_error() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();

        shard.set_memory_limit(Some(48));
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"return ruvio.call('SET', KEYS[1], ARGV[1])",
            &[Cow::Borrowed(b"k".as_slice())],
            &[Cow::Owned(vec![7; 128])],
            0,
        )
        .unwrap();

        assert_eq!(response, Response::Error(crate::engine::OOM_ERROR));
        assert!(shard.is_empty());
    }

    #[test]
    fn a_script_on_a_full_noeviction_store_can_delete_but_not_grow() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();

        shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
        shard.set_bytes(b"old", &[1; 256]);
        shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() - 1));
        let mut eval = |shard: &mut KvShard, body: &[u8], key: &'static [u8]| {
            execute_eval(
                shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                &[Cow::Borrowed(key)],
                &[Cow::Owned(vec![7; 256])],
                0,
            )
            .unwrap()
            .into_owned()
        };

        assert_eq!(
            eval(
                &mut shard,
                b"return ruvio.call('SET', KEYS[1], ARGV[1])",
                b"new"
            ),
            Response::Error(crate::engine::OOM_ERROR)
        );
        assert_eq!(
            eval(&mut shard, b"return ruvio.call('DEL', KEYS[1])", b"old"),
            Response::Integer(1)
        );
        assert!(shard.is_empty());
    }

    #[test]
    fn eval_runs_ruvio_call_and_caches_the_sha() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let body = b"return ruvio.call('SET', KEYS[1], ARGV[1])";
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[owned(b"{t}k")],
            &[owned(b"v")],
            0,
        )
        .unwrap();

        assert_eq!(response, Response::Simple(b"OK"));
        assert_eq!(shard.get(b"{t}k"), Some(&b"v"[..]));
        let sha = hex_sha(&script_sha1(body));

        assert_eq!(
            execute_script_command(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                Command::EvalSha {
                    sha: owned(sha.as_bytes()),
                    keys: vec![owned(b"{t}k")],
                    args: vec![owned(b"v2")],
                },
                0
            )
            .unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(shard.get(b"{t}k"), Some(&b"v2"[..]));
    }

    #[test]
    fn eval_sees_a_sorted_set_pop_as_member_score_pairs() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();

        shard
            .sorted_set_add_pairs(b"z", &[(2.5, b"b"), (1.0, b"a"), (3.0, b"c")])
            .unwrap();

        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"return ruvio.call('ZPOPMIN', KEYS[1], 2)",
            &[owned(b"z")],
            &[],
            0,
        )
        .unwrap();
        let bulk = |value: &[u8]| Response::Bulk(Some(Cow::Owned(value.to_vec())));

        assert_eq!(
            response,
            Response::Exec(vec![bulk(b"a"), bulk(b"1"), bulk(b"b"), bulk(b"2.5")])
        );
        assert_eq!(shard.sorted_set_card(b"z"), Ok(1));
    }

    #[test]
    fn eval_without_keys_runs_and_cross_slot_keys_are_refused() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut eval = |keys: &[Bytes<'static>]| {
            execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                b"return 7",
                keys,
                &[],
                0,
            )
            .unwrap()
        };

        assert_eq!(eval(&[]), Response::Integer(7));
        assert_eq!(
            eval(&[owned(b"{a}1"), owned(b"{b}2")]),
            Response::Error(b"CROSSSLOT Keys in request do not hash to the same slot")
        );
    }

    #[test]
    fn eval_sees_user_keys_and_writes_to_the_selected_database() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"ruvio.call('SET', KEYS[1], ARGV[1]); return {KEYS[1], ruvio.call('DBSIZE')}",
            &[owned(b"k")],
            &[owned(b"v")],
            2,
        )
        .unwrap();

        assert_eq!(
            response,
            Response::Exec(vec![
                Response::Bulk(Some(Cow::Owned(b"k".to_vec()))),
                Response::Integer(1),
            ])
        );
        shard.enter(0);
        assert_eq!(shard.get(b"k"), None);
        shard.enter(2);
        assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
    }

    #[test]
    fn eval_rejects_undeclared_keys_and_rolls_back_failed_scripts() {
        let mut shard = KvShard::new();

        shard.set_bytes(b"{t}k", b"old");
        let mut cache = ScriptCache::default();
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"ruvio.call('SET', KEYS[1], 'new'); return ruvio.call('GET', 'other')",
            &[owned(b"{t}k")],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(
            response,
            Response::Error(b"ERR script attempted to access undeclared keys")
        );
        assert_eq!(shard.get(b"{t}k"), Some(&b"old"[..]));
    }

    #[test]
    fn script_load_exists_flush_and_missing_sha() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let loaded = execute_script_command(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            Command::ScriptLoad(owned(b"return 1")),
            0,
        )
        .unwrap();
        let Response::Bulk(Some(sha)) = loaded else {
            panic!("expected SHA bulk");
        };

        assert_eq!(
            execute_script_command(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                Command::ScriptExists(vec![sha.clone(), owned(&[b'0'; 40])]),
                0
            )
            .unwrap(),
            Response::Exec(vec![Response::Integer(1), Response::Integer(0)])
        );
        execute_script_command(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            Command::ScriptFlush,
            0,
        )
        .unwrap();
        assert_eq!(
            execute_script_command(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                Command::EvalSha {
                    sha,
                    keys: vec![],
                    args: vec![],
                },
                0
            )
            .unwrap(),
            Response::Error(b"NOSCRIPT No matching script. Please use EVAL.")
        );
    }

    #[test]
    fn persist_failure_restores_script_keys() {
        let mut shard = KvShard::new();

        shard.set_bytes(b"{t}k", b"old");
        let mut cache = ScriptCache::default();

        struct FailingSink;
        impl MutationSink for FailingSink {
            type Error = &'static str;
            fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
                Err("persistence unavailable")
            }
        }

        assert_eq!(
            execute_eval(
                &mut shard,
                &mut FailingSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                b"return ruvio.call('SET', KEYS[1], ARGV[1])",
                &[owned(b"{t}k")],
                &[owned(b"new")],
                0
            ),
            Err("persistence unavailable")
        );
        assert_eq!(shard.get(b"{t}k"), Some(&b"old"[..]));
    }

    #[test]
    fn rollback_preserves_absolute_deadlines_in_the_selected_database() {
        use crate::time::{TestClock, UnixMillis};

        struct FailingSink;
        impl MutationSink for FailingSink {
            type Error = &'static str;
            fn record(&mut self, _: Mutation<'_>) -> Result<(), Self::Error> {
                Err("persistence unavailable")
            }
        }

        for script_error in [false, true] {
            let mut shard = KvShard::with_clock(TestClock::new(1_000));

            shard.enter(2);
            shard.set_bytes(b"{t}expires", b"old");
            shard.expire_at(b"{t}expires", UnixMillis::new(5_000));
            shard.set_bytes(b"{t}persistent", b"old");
            shard.set_bytes(b"{t}expired", b"old");
            shard.expire_at(b"{t}expired", UnixMillis::new(999));
            let body = if script_error {
                b"ruvio.call('SET', KEYS[1], 'new'); ruvio.call('PEXPIRE', KEYS[1], 9000); ruvio.call('PEXPIRE', KEYS[2], 9000); ruvio.call('SET', KEYS[3], 'new', 'PX', 9000); error('abort')" as &[u8]
            } else {
                b"ruvio.call('SET', KEYS[1], 'new'); ruvio.call('PEXPIRE', KEYS[1], 9000); ruvio.call('PEXPIRE', KEYS[2], 9000); return ruvio.call('SET', KEYS[3], 'new', 'PX', 9000)"
            };

            let response = execute_eval(
                &mut shard,
                &mut FailingSink,
                &mut (),
                &mut ScriptCache::default(),
                ScriptLimits::default(),
                body,
                &[
                    owned(b"{t}expires"),
                    owned(b"{t}persistent"),
                    owned(b"{t}missing"),
                    owned(b"{t}expired"),
                ],
                &[],
                2,
            );

            if script_error {
                assert!(matches!(
                    response,
                    Ok(Response::Error(_) | Response::OwnedError(_))
                ));
            } else {
                assert_eq!(response, Err("persistence unavailable"));
            }

            assert_eq!(shard.get(b"{t}expires"), Some(&b"old"[..]));
            assert_eq!(
                shard.expires_at(b"{t}expires"),
                Some(UnixMillis::new(5_000))
            );
            assert_eq!(shard.pttl(b"{t}expires"), 4_000);
            assert_eq!(shard.pttl(b"{t}persistent"), -1);
            assert_eq!(shard.get(b"{t}missing"), None);
            assert_eq!(shard.get(b"{t}expired"), None);
            shard.enter(0);
            assert_eq!(shard.get(b"{t}expires"), None);
        }
    }

    /// Limits for tests that build large values on purpose: they reach the memory limit
    /// before the quota or the deadline.
    fn roomy_limits() -> ScriptLimits {
        ScriptLimits {
            max_instructions: 100_000_000,
            max_time_ms: 60_000,
            ..ScriptLimits::default()
        }
    }

    fn eval_body(body: &[u8], limits: ScriptLimits) -> Response<'static> {
        execute_eval(
            &mut KvShard::new(),
            &mut NoopMutationSink,
            &mut (),
            &mut ScriptCache::default(),
            limits,
            body,
            &[owned(b"{t}k")],
            &[],
            0,
        )
        .unwrap()
    }

    #[test]
    fn sandbox_rejects_host_escape_and_admin_calls() {
        for body in [
            b"return os.execute('id')" as &[u8],
            b"return io.open('/etc/passwd')",
            b"return loadfile('/etc/passwd')",
            b"return require('os')",
            b"return ruvio.call('AUTH', 'x')",
            b"return ruvio.call('SAVE')",
            b"return ruvio.call('CONFIG', 'GET', '*')",
            b"return ruvio.call('ACL', 'WHOAMI')",
            b"return ruvio.call('CLUSTER', 'SLOTS')",
        ] {
            let response = eval_body(body, ScriptLimits::default());

            assert!(
                matches!(response, Response::Error(_) | Response::OwnedError(_)),
                "script {body:?} should not succeed: {response:?}"
            );

            if body.starts_with(b"return ruvio.call") {
                assert_eq!(
                    response,
                    Response::Error(b"ERR command not allowed from script source")
                );
            }
        }
    }

    /// Runs `body` on its own thread and fails the test if it has not finished within
    /// `seconds`, instead of hanging the suite.
    fn eval_within(seconds: u64, body: &'static [u8], limits: ScriptLimits) -> Response<'static> {
        let (sender, receiver) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let _ = sender.send(eval_body(body, limits));
        });

        match receiver.recv_timeout(std::time::Duration::from_secs(seconds)) {
            Ok(response) => response,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
                "script still running after {seconds}s: {}",
                String::from_utf8_lossy(body)
            ),
            Err(error) => panic!("script thread failed: {error}"),
        }
    }

    /// Lua 5.1 computes the count in `int`. Compiled with optimization, as in release
    /// builds, its overflow check is folded away and these bounds crashed the process; an
    /// unoptimized build happens to catch them.
    #[test]
    fn unpack_with_extreme_bounds_fails_instead_of_crashing() {
        for body in [
            b"return {pcall(unpack, {}, -2147483648, -1)}" as &'static [u8],
            b"return {pcall(unpack, {}, -1073741824, 1073741824)}",
            b"return {pcall(unpack, {}, -1e10, 1e10)}",
            b"return {pcall(unpack, {}, 1, 1e6)}",
        ] {
            let response = eval_within(30, body, ScriptLimits::default());
            let Response::Exec(items) = &response else {
                panic!("{response:?}");
            };

            assert_eq!(items.first(), Some(&Response::Bulk(None)), "{response:?}");
            assert!(
                matches!(items.get(1), Some(Response::Bulk(Some(message)))
                    if message.ends_with(b"too many results to unpack")),
                "{response:?}"
            );
        }

        // Ordinary ranges keep Lua 5.1's results.
        assert_eq!(
            eval_within(
                30,
                b"local t = {1, 2, 3} \
                  return {select('#', unpack(t)), select('#', unpack(t, -1, 1)), \
                  select('#', unpack(t, 3, 1)), unpack(t, 2), \
                  select('#', unpack({}, 2147483647, 2147483647))}",
                ScriptLimits::default(),
            ),
            Response::Exec(vec![
                Response::Integer(3),
                Response::Integer(3),
                Response::Integer(0),
                Response::Integer(2),
                Response::Integer(1),
            ])
        );
    }

    #[test]
    fn an_endless_loop_inside_a_coroutine_or_pcall_hits_the_quota() {
        let limits = ScriptLimits {
            max_instructions: 100_000,
            ..ScriptLimits::default()
        };

        for body in [
            b"coroutine.wrap(function() while true do end end)()" as &'static [u8],
            b"local co = coroutine.create(function() while true do end end) \
              return coroutine.resume(co)",
            b"while true do pcall(function() while true do end end) end",
            b"while true do pcall(coroutine.wrap(function() while true do end end)) end",
            b"while true do xpcall(function() while true do end end, function(e) return e end) end",
            b"local co = coroutine.wrap(function() \
                  while true do coroutine.resume(coroutine.create(function() \
                      while true do end end)) end end) \
              while true do pcall(co) end",
        ] {
            assert_eq!(
                eval_within(30, body, limits),
                Response::Error(b"ERR Lua instruction quota exceeded"),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        // The next script gets a fresh quota, and catching ordinary errors still works.
        assert_eq!(
            eval_within(30, b"return {pcall(error, 'x', 0)}", limits),
            Response::Exec(vec![
                Response::Bulk(None),
                Response::Bulk(Some(owned(b"x")))
            ])
        );
    }

    #[test]
    fn a_backtracking_pattern_hits_the_quota_instead_of_holding_the_shard() {
        // The default quota, with time enough that a loaded debug build hits it first.
        let quota_only = ScriptLimits {
            max_time_ms: 60_000,
            ..ScriptLimits::default()
        };
        for body in [
            b"return string.find(string.rep('a', 40), string.rep('a*', 20) .. 'b')" as &'static [u8],
            b"return string.match(string.rep('a', 100000), '.-.-b')",
            b"return string.gsub(string.rep('a', 100000), '.-b', '')",
            b"for m in string.gmatch(string.rep('a', 100000), '.-b') do end",
            b"while true do pcall(string.find, string.rep('a', 40), string.rep('a*', 20) .. 'b') end",
        ] {
            assert_eq!(
                eval_within(60, body, quota_only),
                Response::Error(b"ERR Lua instruction quota exceeded"),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        // Ordinary matching over a large subject fits in the default budget.
        assert_eq!(
            eval_within(
                60,
                b"return select(2, string.gsub(string.rep('a b ', 262144), '%s+', ' '))",
                quota_only
            ),
            Response::Integer(524_288)
        );

        // A smaller quota gives fewer steps.
        let tight = ScriptLimits {
            max_instructions: 1_000,
            ..ScriptLimits::default()
        };

        assert_eq!(
            eval_within(
                30,
                b"return string.find(string.rep('a', 1000), '.-b')",
                tight
            ),
            Response::Error(b"ERR Lua instruction quota exceeded")
        );
        assert_eq!(
            eval_within(
                30,
                b"return string.find(string.rep('a', 100), '.-b')",
                tight
            ),
            Response::Bulk(None)
        );
    }

    #[test]
    fn library_functions_that_loop_in_c_return_promptly() {
        let run = |body: &'static [u8]| eval_within(10, body, ScriptLimits::default());

        // C appends the empty string two billion times.
        assert_eq!(
            run(b"local n = 0 for i = 1, 20 do n = n + #string.rep('', 2^31 - 1) end return n"),
            Response::Integer(0)
        );
        assert_eq!(
            run(b"return {string.rep('ab', 3), string.rep('x', 0), string.rep('x', -1)}"),
            Response::Exec(vec![
                Response::Bulk(Some(owned(b"ababab"))),
                Response::Bulk(Some(owned(b""))),
                Response::Bulk(Some(owned(b""))),
            ])
        );
        // C moves every slot from the end of the table down to the position.
        assert_eq!(
            run(b"local ok, e = pcall(table.insert, {}, -2^31 + 2, 1) return {tostring(ok), e}"),
            Response::Exec(vec![
                Response::Bulk(Some(owned(b"false"))),
                Response::Bulk(Some(owned(
                    b"bad argument #2 to '?' (position out of bounds)"
                ))),
            ])
        );
        assert_eq!(
            run(b"local t = {1} table.insert(t, 0, 'z') return t[0]"),
            Response::Bulk(Some(owned(b"z")))
        );
        assert_eq!(
            run(b"local t = {1, 3} table.insert(t, 2, 2) table.insert(t, 4) return t"),
            Response::Exec(vec![
                Response::Integer(1),
                Response::Integer(2),
                Response::Integer(3),
                Response::Integer(4),
            ])
        );
        // C's plain search compares the pattern at every position: quadratic.
        assert_eq!(
            run(b"local s = string.rep('a', 1000000) \
                  return {string.find(s, string.rep('a', 500000) .. 'b', 1, true), \
                  string.find(s .. 'b', string.rep('a', 500000) .. 'b', 1, true)}"),
            Response::Exec(vec![
                Response::Bulk(None),
                Response::Integer(500001),
                Response::Integer(1000001),
            ])
        );
    }

    #[test]
    fn coroutines_are_charged_for_the_instructions_the_hook_misses() {
        let limits = ScriptLimits {
            max_instructions: 100_000,
            ..ScriptLimits::default()
        };

        // About a million instructions, nearly all in the first thousand of fresh
        // coroutines, which the hook never counts.
        assert_eq!(
            eval_within(
                30,
                b"for i = 1, 1000 do coroutine.wrap(function() for j = 1, 450 do end end)() end \
                  return 'done'",
                limits
            ),
            Response::Error(b"ERR Lua instruction quota exceeded")
        );
        assert_eq!(
            eval_within(
                30,
                b"local n = 0 for i = 1, 20 do n = n + coroutine.wrap(function() return i end)() end \
                  return n",
                limits
            ),
            Response::Integer(210)
        );
    }

    #[test]
    fn work_that_grows_with_data_is_stopped_by_the_quota_or_the_deadline() {
        let limits = ScriptLimits {
            max_time_ms: 200,
            ..ScriptLimits::default()
        };

        // Each costs a few instructions and up to 16 MiB of copying.
        for body in [
            b"local s = string.rep('x', 2^20) while true do local t = s .. s end"
                as &'static [u8],
            b"local s = string.rep('x', 2^20) \
              while true do pcall(function() local t = s .. s end) end",
        ] {
            assert_eq!(
                eval_within(30, body, limits),
                Response::Error(b"ERR Lua script time limit exceeded"),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        // Library functions charge the bytes they build or scan: the quota stops these
        // even with time to spare.
        let slow = ScriptLimits {
            max_time_ms: 60_000,
            ..ScriptLimits::default()
        };

        for body in [
            b"for i = 1, 200 do local t = string.rep('x', 2^24) end" as &'static [u8],
            b"local s = string.rep('x', 2^24) for i = 1, 100 do local u = s:upper() end",
            b"local s = string.rep('x', 2^24) for i = 1, 100 do local u = s:reverse() end",
            b"local t = {} for i = 1, 2^16 do t[i] = 'x' end \
              while true do local s = table.concat(t) end",
            b"local t = {} for i = 1, 50000 do t[i] = i end while true do table.sort(t) end",
            b"local t = {} for i = 1, 2^16 do t[i] = i end \
              while true do table.remove(t, 1) table.insert(t, 1, 0) end",
            b"local s = string.rep('x', 2^24) while true do local u = s:sub(2) end",
            b"local s = string.rep('x', 2^24) while true do local u = string.format('%s', s) end",
            b"local s = string.rep('x', 2^24) while true do local u = s:find('y', 1, true) end",
        ] {
            assert_eq!(
                eval_within(30, body, slow),
                Response::Error(b"ERR Lua instruction quota exceeded"),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        assert_eq!(
            eval_within(
                30,
                b"return #string.rep('ab', 2^20) + #string.upper(string.rep('x', 2^20))",
                limits
            ),
            Response::Integer(3 * 1024 * 1024)
        );
    }

    #[test]
    fn an_instruction_quota_above_four_billion_is_not_truncated() {
        let limits = ScriptLimits {
            max_instructions: (1 << 32) + 1_000,
            ..ScriptLimits::default()
        };

        assert_eq!(
            eval_within(30, b"for i = 1, 5000 do end return 'done'", limits),
            Response::Bulk(Some(owned(b"done")))
        );
    }

    /// Runs `body` with a 200 ms deadline and checks that one of the limits stopped it
    /// within a few seconds: one VM instruction or library call on a large value must not
    /// run on long past the deadline.
    fn assert_stopped_promptly(body: &'static [u8]) {
        assert_stopped_within(body, ScriptLimits::default().max_instructions, 4);
    }

    /// Asserts `body` fails with a quota or time error within `bound` seconds, with a
    /// 200 ms deadline and `max_instructions`.
    fn assert_stopped_within(body: &'static [u8], max_instructions: usize, bound: u64) {
        let limits = ScriptLimits {
            max_instructions,
            max_time_ms: 200,
            ..ScriptLimits::default()
        };
        let started = std::time::Instant::now();
        let response = eval_within(30, body, limits);
        let elapsed = started.elapsed();

        assert!(
            matches!(
                response,
                Response::Error(
                    b"ERR Lua instruction quota exceeded"
                        | b"ERR Lua script time limit exceeded"
                        | b"ERR Lua ruvio.call quota exceeded"
                )
            ),
            "{}: {response:?}",
            String::from_utf8_lossy(body)
        );
        assert!(
            elapsed < std::time::Duration::from_secs(bound),
            "{} ran {elapsed:?}",
            String::from_utf8_lossy(body)
        );
    }

    #[test]
    fn an_xpcall_handler_cannot_outlive_the_quota() {
        for body in [
            b"return xpcall(function() while true do end end, function(e) while true do end end)"
                as &'static [u8],
            b"return xpcall(function() error('x') end, function(e) while true do end end)",
            b"return xpcall(function() local s = string.rep('x', 2^30) end, \
              function(e) while true do end end)",
            b"while true do xpcall(function() while true do end end, function(e) return e end) end",
        ] {
            assert_stopped_promptly(body);
        }

        // An ordinary handler still runs.
        assert_eq!(
            eval_within(
                30,
                b"return {xpcall(function() error('x', 0) end, function(e) return 'handled ' .. e end)}",
                ScriptLimits::default()
            ),
            Response::Exec(vec![Response::Bulk(None), Response::Bulk(Some(owned(b"handled x")))])
        );
    }

    #[test]
    fn a_table_length_far_past_its_entries_is_charged_before_it_is_read() {
        // About 30 entries whose length, found by binary search, is 671,088,640.
        for body in [
            b"local t = {} t[1] = 1 t[2] = 1 t[4] = 1 local j = 5 \
              while j < 2^30 do t[j] = 1 j = j * 2 end table.sort(t)"
                as &'static [u8],
            b"local t = {} t[1] = 1 t[2] = 1 t[4] = 1 local j = 5 \
              while j < 2^30 do t[j] = 1 j = j * 2 end \
              return xpcall(function() error(t) end, table.sort)",
        ] {
            // The default quota is spent before the scan starts.
            let started = std::time::Instant::now();
            assert_eq!(
                eval_within(30, body, ScriptLimits::default()),
                Response::Error(b"ERR Lua instruction quota exceeded"),
                "{}",
                String::from_utf8_lossy(body)
            );
            assert!(started.elapsed() < std::time::Duration::from_secs(10));
            // With quota to spare the deadline stops the scan, which unchecked took
            // about 12 s in a release build.
            assert_stopped_within(body, usize::MAX, 10);
        }
    }

    #[test]
    fn large_values_cannot_hold_the_shard_past_the_deadline() {
        for body in [
            // The replacement is read once per match.
            b"local s = string.rep('y', 2^20) return #s:gsub('', string.rep('%0', 2^19))"
                as &'static [u8],
            // Each comparison reads both strings.
            b"local s = string.rep('x', 2^25) local t = {} for i = 1, 500 do t[i] = s end \
              table.sort(t) return #t",
            // One VM instruction each.
            b"local s = string.rep('x', 2^25) local n = 0 while true do if s < s then n = n + 1 end end",
            b"local s = string.rep('x', 2^24) while true do local t = s .. s end",
            // Library calls that read a large argument.
            b"local s = string.rep('1', 2^25) local n = 0 \
              while true do n = n + (tonumber(s) and 1 or 0) end",
            b"local sep = string.rep('x', 2^24) while true do local u = table.concat({'', ''}, sep) end",
            b"local t = {} for i = 1, 100000 do t['k' .. i] = i end \
              while true do local m = table.maxn(t) end",
            // A command per call, few instructions between.
            b"for b = 1, 20 do local t = {} for i = 1, 5000 do t[i] = i end \
              ruvio.call('RPUSH', KEYS[1], unpack(t)) end \
              while true do local r = ruvio.call('LRANGE', KEYS[1], 0, -1) end",
        ] {
            assert_stopped_promptly(body);
        }

        assert_eq!(
            eval_within(
                30,
                b"local t = {} for i = 1, 100 do t[i] = i end t[1000] = 1 t.x = 5 \
                  return {table.maxn(t), table.maxn({}), table.concat({1, 'a', 2}, '-')}",
                ScriptLimits::default()
            ),
            Response::Exec(vec![
                Response::Integer(1000),
                Response::Integer(0),
                Response::Bulk(Some(owned(b"1-a-2"))),
            ])
        );
    }

    #[test]
    fn string_dump_is_not_available() {
        assert_eq!(
            eval_body(b"return type(string.dump)", ScriptLimits::default()),
            Response::Bulk(Some(owned(b"nil")))
        );
    }

    #[test]
    fn eval_enforces_instruction_and_call_quotas() {
        let tight = ScriptLimits {
            max_instructions: 1_000,
            max_calls: 2,
            ..ScriptLimits::default()
        };

        assert_eq!(
            eval_body(b"while true do end", tight),
            Response::Error(b"ERR Lua instruction quota exceeded")
        );
        assert_eq!(
            eval_body(
                b"ruvio.call('PING'); ruvio.call('PING'); return ruvio.call('PING')",
                tight
            ),
            Response::Error(b"ERR Lua ruvio.call quota exceeded")
        );
    }

    #[test]
    fn function_load_fcall_list_and_delete_stay_out_of_the_wal() {
        // FUNCTION LOAD, FUNCTION LIST, FUNCTION DELETE, and FCALL stay in memory.
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let mut sink = NoopMutationSink;
        let limits = ScriptLimits::default();
        let loaded = execute_script_command(
            &mut shard,
            &mut sink,
            &mut (),
            &mut cache,
            limits,
            Command::FunctionLoad {
                name: owned(b"put"),
                body: owned(b"return ruvio.call('SET', KEYS[1], ARGV[1])"),
            },
            0,
        )
        .unwrap();

        assert_eq!(
            loaded,
            Response::Bulk(Some(std::borrow::Cow::Owned(b"put".to_vec())))
        );
        assert_eq!(
            execute_script_command(
                &mut shard,
                &mut sink,
                &mut (),
                &mut cache,
                limits,
                Command::FCall {
                    name: owned(b"put"),
                    keys: vec![owned(b"{t}k")],
                    args: vec![owned(b"v")],
                },
                0
            )
            .unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(shard.get(b"{t}k"), Some(&b"v"[..]));
        let listed = execute_script_command(
            &mut shard,
            &mut sink,
            &mut (),
            &mut cache,
            limits,
            Command::FunctionList,
            0,
        )
        .unwrap();

        assert_eq!(
            listed,
            Response::Exec(vec![Response::Bulk(Some(std::borrow::Cow::Owned(
                b"put".to_vec()
            )))])
        );
        assert_eq!(
            execute_script_command(
                &mut shard,
                &mut sink,
                &mut (),
                &mut cache,
                limits,
                Command::FunctionDelete(owned(b"put")),
                0
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            execute_script_command(
                &mut shard,
                &mut sink,
                &mut (),
                &mut cache,
                limits,
                Command::FCall {
                    name: owned(b"put"),
                    keys: vec![owned(b"{t}k")],
                    args: vec![owned(b"v")],
                },
                0
            )
            .unwrap(),
            Response::Error(b"ERR function not found")
        );
    }

    fn eval_with<S: MutationSink>(
        shard: &mut KvShard,
        sink: &mut S,
        body: &[u8],
        keys: &[&[u8]],
    ) -> Result<Response<'static>, S::Error> {
        let keys: Vec<_> = keys.iter().map(|key| owned(key)).collect();

        execute_eval(
            shard,
            sink,
            &mut (),
            &mut ScriptCache::default(),
            ScriptLimits::default(),
            body,
            &keys,
            &[],
            0,
        )
    }

    fn big_sorted_set(shard: &mut KvShard) {
        let members: Vec<_> = (0..2_000u32)
            .map(|index| (f64::from(index), format!("m{index}").into_bytes()))
            .collect();

        shard.sorted_set_add_pairs(b"{t}z", &members).unwrap();
    }

    #[test]
    fn a_reading_script_copies_no_key() {
        let mut shard = KvShard::new();

        big_sorted_set(&mut shard);
        crate::engine::UNDO_COPIES.set(0);
        let reply = eval_with(
            &mut shard,
            &mut NoopMutationSink,
            b"return ruvio.call('ZSCORE', KEYS[1], 'm7')",
            &[b"{t}z"],
        )
        .unwrap();

        assert_eq!(reply, Response::Bulk(Some(Cow::Borrowed(b"7"))));
        assert_eq!(crate::engine::UNDO_COPIES.get(), 0);
    }

    #[test]
    fn a_failing_script_restores_only_what_it_wrote() {
        let mut shard = KvShard::new();

        big_sorted_set(&mut shard);
        shard.set_bytes(b"{t}a", b"old");
        let before = shard.snapshot_entries();
        crate::engine::UNDO_COPIES.set(0);

        let reply = eval_with(
            &mut shard,
            &mut NoopMutationSink,
            b"ruvio.call('ZCARD', KEYS[1]); ruvio.call('GET', KEYS[2]); \
              ruvio.call('SET', KEYS[2], 'new'); ruvio.call('APPEND', KEYS[2], 'x'); \
              ruvio.call('SET', KEYS[3], 'created'); error('abort')",
            &[b"{t}z", b"{t}a", b"{t}created"],
        )
        .unwrap();

        assert!(matches!(
            reply,
            Response::Error(_) | Response::OwnedError(_)
        ));
        assert_eq!(
            crate::engine::UNDO_COPIES.get(),
            2,
            "the read zset is not copied"
        );
        assert_eq!(shard.snapshot_entries(), before);
    }

    #[test]
    fn a_refused_script_batch_restores_a_key_written_after_reads() {
        let mut shard = KvShard::new();

        big_sorted_set(&mut shard);
        let before = shard.snapshot_entries();

        assert_eq!(
            eval_with(
                &mut shard,
                &mut RefusingSink,
                b"ruvio.call('ZSCORE', KEYS[1], 'm1'); ruvio.call('ZADD', KEYS[1], 5, 'new'); \
                  return ruvio.call('ZREM', KEYS[1], 'm2')",
                &[b"{t}z"],
            ),
            Err(())
        );
        assert_eq!(shard.snapshot_entries(), before);
    }

    #[test]
    fn a_script_refused_for_memory_leaves_its_keys_in_a_running_snapshot() {
        let mut shard = KvShard::new();

        shard.set_bytes(b"{t}a", b"old");
        shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() + 16));
        shard.begin_snapshot(4_096).unwrap();

        // The refused SET changes nothing, but the failed script still restores the key.
        let reply = eval_with(
            &mut shard,
            &mut NoopMutationSink,
            b"return ruvio.call('SET', KEYS[1], string.rep('x', 4096))",
            &[b"{t}a"],
        )
        .unwrap();

        assert!(matches!(
            reply,
            Response::Error(_) | Response::OwnedError(_)
        ));
        assert_eq!(shard.get(b"{t}a"), Some(&b"old"[..]));
        let snapshot = shard.finish_snapshot();

        assert!(
            snapshot.iter().any(|entry| entry.key == b"{t}a"
                && entry.value == crate::store::SnapshotValue::Bytes(b"old".to_vec())),
            "the snapshot lost a key the script did not change: {snapshot:?}"
        );
    }

    #[test]
    fn a_rolled_back_timed_hash_still_expires_its_fields() {
        use crate::time::TestClock;

        let clock = TestClock::new(1_000);
        let mut shard = KvShard::with_clock(clock.clone());
        let mut cache = ScriptCache::default();
        let mut eval = |shard: &mut KvShard<TestClock>, body: &[u8]| {
            execute_eval(
                shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                &[owned(b"{t}h")],
                &[],
                0,
            )
            .unwrap()
        };

        eval(
            &mut shard,
            b"ruvio.call('HSET', KEYS[1], 'short', 'a', 'long', 'b'); \
              return ruvio.call('HPEXPIRE', KEYS[1], 100, 'FIELDS', 1, 'short')",
        );
        shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() + 16));

        // Refused for memory: the script fails and puts the hash back.
        let refused = eval(
            &mut shard,
            b"return ruvio.call('HSET', KEYS[1], 'big', string.rep('x', 4096))",
        );

        assert!(matches!(
            refused,
            Response::Error(_) | Response::OwnedError(_)
        ));
        clock.set(2_000);
        assert_eq!(
            eval(&mut shard, b"return ruvio.call('HKEYS', KEYS[1])[1]"),
            Response::Bulk(Some(owned(b"long")))
        );
        assert_eq!(
            eval(&mut shard, b"return ruvio.call('HLEN', KEYS[1])"),
            Response::Integer(1)
        );
    }

    #[test]
    fn status_and_error_replies_stay_on_one_line() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();

        for (body, error) in [
            (&br"return {err='bad\r\n+FORGED'}"[..], true),
            (&br"return {ok='fine\r\n+FORGED'}"[..], false),
            (&br"return ruvio.error_reply('bad\r\n:1')"[..], true),
            (&br"return {1, {err='bad\n:1'}}"[..], true),
        ] {
            let response = execute_eval(
                &mut shard,
                &mut NoopMutationSink,
                &mut (),
                &mut cache,
                ScriptLimits::default(),
                body,
                &[],
                &[],
                0,
            )
            .unwrap();
            let text = match &response {
                Response::OwnedError(text) | Response::OwnedSimple(text) if error || !error => {
                    text.clone()
                }
                Response::Exec(items) => match &items[1] {
                    Response::OwnedError(text) => text.clone(),
                    other => panic!("unexpected {other:?}"),
                },
                other => panic!("unexpected {other:?}"),
            };

            assert!(!text.contains(&b'\r') && !text.contains(&b'\n'));
        }
    }

    #[test]
    fn compiled_bytecode_is_refused() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"\x1bLua",
            &[],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(
            response,
            Response::Error(b"ERR Lua bytecode is not allowed")
        );
    }

    #[test]
    fn a_reply_repeating_one_large_string_is_refused() {
        let mut shard = KvShard::new();
        let mut cache = ScriptCache::default();
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"local s = string.rep('x', 1024 * 1024) local t = {} for i = 1, 100 do t[i] = s end return t",
            &[],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(response, Response::Error(b"ERR Lua reply is too large"));
    }
}
