//! The parts of Lua 5.1's standard library that scripts get in a bounded form.
//!
//! Each function here replaces one whose C code could crash the server or run unbounded
//! between instruction hooks: pattern matching (backtracking, unbounded recursion),
//! `unpack` (an `int` overflow that writes past the Lua stack), `string.rep` of an empty
//! string (billions of empty appends) and `table.insert` at a position far below the
//! table (billions of moves). The rest keep their Lua 5.1 results.
//!
//! It also keeps each script's budget: the instruction quota, which the instruction hook
//! and the functions here charge, and a wall-clock deadline for work whose cost grows
//! with the data rather than the instruction count (`s .. s`, `table.sort`, `upper`).
//!
//! These are C functions: an error unwinds them with `longjmp`, so none of them holds a
//! value with a destructor when it calls into Lua.

use std::cell::Cell;
use std::ffi::{CStr, c_int};
use std::mem::MaybeUninit;
use std::time::{Duration, Instant};

use mlua::{Function, Lua, Result as LuaResult, Table, Value, ffi};

use crate::lua_pattern::{self, Anchor, Capture, Found, PatternError};

/// Pattern-matching steps a script may take for each instruction of its quota. A step
/// costs about as much as an instruction, so a script that spends its whole budget on
/// matching runs at most this many times as long as one that loops to its quota, and a
/// 1 MiB subject still fits in the default budget.
pub(crate) const PATTERN_STEPS_PER_INSTRUCTION: u64 = 8;

/// Bytes of a string a library function builds or scans for each instruction it is
/// charged: about the cost of one instruction for a plain copy, less for `upper`.
const BYTES_PER_INSTRUCTION: u64 = 64;

/// Table slots `table.insert` and `table.remove` move for each instruction charged.
const MOVES_PER_INSTRUCTION: u64 = 16;

/// Table elements `table.concat` and `table.maxn` visit for each instruction charged.
const ELEMENTS_PER_INSTRUCTION: u64 = 4;

/// What a coroutine costs when it is created: Lua 5.1 starts each new thread's hook
/// count afresh, so up to this many of its first instructions are never seen by the hook.
pub(crate) const COROUTINE_COST: u64 = 1_000;

pub(crate) const QUOTA_ERROR: &str = "ERR Lua instruction quota exceeded";
pub(crate) const TIME_ERROR: &str = "ERR Lua script time limit exceeded";

thread_local! {
    /// Instructions the running script has been charged.
    static USED: Cell<u64> = const { Cell::new(0) };
    /// `USED` when the clock was last read.
    static CLOCK_CHECKED: Cell<u64> = const { Cell::new(0) };
    /// Bytes charged that did not yet make up an instruction.
    static PENDING_BYTES: Cell<u64> = const { Cell::new(0) };
    /// The running script's instruction quota.
    static LIMIT: Cell<u64> = const { Cell::new(u64::MAX) };
    /// When the running script must stop.
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    /// Pattern-matching steps left to the running script.
    static PATTERN_STEPS: Cell<u64> = const { Cell::new(8_000_000) };
    /// Why the running script was stopped, once it was: from then on `pcall`, `xpcall`
    /// and `coroutine.resume` pass that error on instead of catching it.
    static STOPPED: Cell<Option<&'static str>> = const { Cell::new(None) };
}

/// Instructions between two checks of the clock by charged library calls, as between
/// two calls of the instruction hook on a small heap.
const CLOCK_INTERVAL: u64 = 1_000;

/// The instruction hook's interval while the Lua heap is at most this many KiB. Past it
/// the interval shrinks in proportion: one VM instruction such as `..` or `<` can then
/// work through strings of many MiB, and fewer of them may run between two checks.
const SMALL_HEAP_KIB: u64 = 1_024;

/// A charge this large (64 KiB of bytes) may leave a large value on the heap.
const LARGE_CHARGE: u64 = 1_024;

/// The hook's shortest interval, on a heap near the 128 MiB limit.
const MIN_HOOK_INTERVAL: u64 = 10;

/// Starts the next script with a quota of `instructions` and `time` to run.
pub(crate) fn start_script(instructions: u64, time: Duration) {
    USED.set(0);
    CLOCK_CHECKED.set(0);
    PENDING_BYTES.set(0);
    LIMIT.set(instructions);
    DEADLINE.set(Instant::now().checked_add(time));
    PATTERN_STEPS.set(instructions.saturating_mul(PATTERN_STEPS_PER_INSTRUCTION));
    STOPPED.set(None);
}

/// Charges `instructions` and checks the deadline: the instruction hook's work. Returns
/// the error to stop the script with.
fn tick(instructions: u64) -> Result<(), &'static str> {
    charge(instructions)?;
    check_deadline()
}

/// Checks the deadline, and returns the error the script was stopped with if it was.
pub(crate) fn check_deadline() -> Result<(), &'static str> {
    if let Some(error) = STOPPED.get() {
        return Err(error);
    }

    CLOCK_CHECKED.set(USED.get());
    match DEADLINE.get() {
        Some(deadline) if Instant::now() >= deadline => Err(stop(TIME_ERROR)),
        _ => Ok(()),
    }
}

/// Charges `instructions` to the running script, or fails with the error it was stopped
/// with.
fn charge(instructions: u64) -> Result<(), &'static str> {
    if let Some(error) = STOPPED.get() {
        return Err(error);
    }

    let used = USED.get().saturating_add(instructions);

    USED.set(used);
    if used > LIMIT.get() {
        Err(stop(QUOTA_ERROR))
    } else {
        Ok(())
    }
}

/// Stops the running script with `error`.
fn stop(error: &'static str) -> &'static str {
    STOPPED.set(Some(error));
    error
}

/// The instruction hook, on every thread: charges the instructions since its last call
/// and checks the deadline, then sets the interval to its next call from the size of
/// the Lua heap.
///
/// # Safety
/// Lua calls it as a count hook.
#[allow(unsafe_code)]
pub(crate) unsafe extern "C-unwind" fn instruction_hook(
    state: *mut ffi::lua_State,
    _debug: *mut ffi::lua_Debug,
) {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        let interval = ffi::lua_gethookcount(state).max(1) as u64;

        if let Err(error) = tick(interval) {
            raise(state, 0, &[error.as_bytes()]);
        }

        adapt_hook(state);
    }
}

/// [`adapt_hook`] for the main thread of `lua`, outside any call.
#[allow(unsafe_code)]
pub(crate) fn adapt_main_hook(lua: &Lua) {
    lua.exec_raw_lua(|raw| {
        // SAFETY: the main state, which runs nothing while this is called.
        unsafe { adapt_hook(raw.state()) }
    });
}

/// Sets the running thread's hook interval from the size of the Lua heap. Called by the
/// hook, and wherever the heap may have grown by a large value at once: the script's
/// start, a large charged call, a `ruvio.call` reply.
///
/// # Safety
/// `state` is the running Lua thread.
#[allow(unsafe_code)]
pub(crate) unsafe fn adapt_hook(state: *mut ffi::lua_State) {
    // SAFETY: guaranteed by the caller.
    unsafe { adapt_hook_for(state, 0) }
}

/// [`adapt_hook`] for a heap `extra_kib` larger than now: a charged call is about to
/// make a value that large.
///
/// # Safety
/// As [`adapt_hook`].
#[allow(unsafe_code)]
unsafe fn adapt_hook_for(state: *mut ffi::lua_State, extra_kib: u64) {
    // SAFETY: guaranteed by the caller; neither call raises.
    unsafe {
        let heap =
            (ffi::lua_gc(state, ffi::LUA_GCCOUNT, 0).max(1) as u64).saturating_add(extra_kib);
        let next = (HOOK_INTERVAL * SMALL_HEAP_KIB / heap).clamp(MIN_HOOK_INTERVAL, HOOK_INTERVAL);

        // Setting the hook restarts its count, so the instructions run since the last
        // call go uncharged: at most one interval (1,000) per change, and the interval
        // changes only when the heap crosses a size that gives a new one.
        if next != ffi::lua_gethookcount(state) as u64 {
            ffi::lua_sethook(
                state,
                Some(instruction_hook),
                ffi::LUA_MASKCOUNT,
                next as c_int,
            );
        }
    }
}

/// The hook's interval on a small heap.
pub(crate) const HOOK_INTERVAL: u64 = 1_000;

/// Charges `instructions` of work that grows with the data, before it is done, and
/// raises if the script is out of quota or, checked every [`CLOCK_INTERVAL`] charged
/// instructions, out of time.
///
/// # Safety
/// Lua called a function with `state`; nothing with a destructor is alive in the frames
/// an error unwinds.
#[allow(unsafe_code)]
unsafe fn charge_work(state: *mut ffi::lua_State, instructions: u64) {
    if instructions == 0 {
        return;
    }

    // The call may be about to make a value as large as the work charged: check more
    // often from now on.
    if instructions >= LARGE_CHARGE {
        // SAFETY: guaranteed by the caller.
        unsafe {
            adapt_hook_for(
                state,
                instructions.saturating_mul(BYTES_PER_INSTRUCTION) / 1024,
            )
        };
    }

    let outcome = charge(instructions).and_then(|()| {
        if USED.get() - CLOCK_CHECKED.get() >= CLOCK_INTERVAL {
            check_deadline()
        } else {
            Ok(())
        }
    });

    if let Err(error) = outcome {
        // SAFETY: guaranteed by the caller.
        unsafe { raise(state, 1, &[error.as_bytes()]) }
    }
}

/// Charges work that copies or scans `bytes`. Bytes short of an instruction are kept for
/// the next charge, so many small calls add up.
///
/// # Safety
/// As [`charge_work`].
#[allow(unsafe_code)]
unsafe fn charge_bytes(state: *mut ffi::lua_State, bytes: u64) {
    let total = PENDING_BYTES.get().saturating_add(bytes);

    PENDING_BYTES.set(total % BYTES_PER_INSTRUCTION);
    // SAFETY: guaranteed by the caller.
    unsafe { charge_work(state, total / BYTES_PER_INSTRUCTION) }
}

/// Replaces the unbounded functions of `lua`'s standard library. Runs before the globals
/// are sealed and before the memory limit is set.
#[allow(unsafe_code)]
pub(crate) fn install(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();
    let string: Table = globals.get("string")?;
    // SAFETY: each is a `lua_CFunction` that keeps Lua's stack discipline and raises only
    // when no Rust value with a destructor is alive on its frame.
    let (find, matches, gmatch, next_match, unpack) = unsafe {
        (
            lua.create_c_function(string_find)?,
            lua.create_c_function(string_match)?,
            lua.create_c_function(string_gmatch)?,
            lua.create_c_function(pattern_next)?,
            lua.create_c_function(table_unpack)?,
        )
    };
    let replace_with: Function = lua
        .load(REPLACE_WITH)
        .set_name("=sandbox")
        .call(next_match)?;

    string.raw_set("find", find)?;
    string.raw_set("match", matches)?;
    string.raw_set("gmatch", &gmatch)?;
    string.raw_set("gfind", gmatch)?;
    // Its bytecode is no use without `loadstring`, and it exposes function internals.
    string.raw_set("dump", Value::Nil)?;
    globals.raw_set("unpack", unpack)?;
    wrap_c_function(
        lua,
        &string,
        c"gsub",
        string_gsub,
        &[Value::Function(replace_with)],
    )?;
    wrap_c_function(lua, &string, c"rep", string_rep, &[])?;
    for name in [c"upper", c"lower", c"reverse"] {
        wrap_c_function(lua, &string, name, charge_length, &[])?;
    }

    wrap_c_function(lua, &string, c"sub", charge_substring, &[])?;
    wrap_c_function(lua, &string, c"format", charge_arguments, &[])?;

    let table: Table = globals.get("table")?;

    wrap_c_function(lua, &table, c"concat", charge_concat, &[])?;
    // SAFETY: a `lua_CFunction` that keeps Lua's stack discipline.
    table.raw_set("maxn", unsafe { lua.create_c_function(table_maxn)? })?;
    wrap_c_function(lua, &globals, c"tonumber", charge_string_argument, &[])?;

    let coroutine: Table = globals.get("coroutine")?;

    // A loop that catches the quota error and starts over would never end.
    wrap_c_function(lua, &globals, c"pcall", rethrow_quota, &[])?;
    wrap_c_function(lua, &globals, c"xpcall", guarded_xpcall, &[])?;
    wrap_c_function(lua, &coroutine, c"resume", rethrow_quota, &[])?;
    wrap_c_function(lua, &coroutine, c"create", charge_coroutine, &[])?;
    wrap_c_function(lua, &coroutine, c"wrap", charge_coroutine, &[])?;
    // `getmetatable('')` must not reach the real `string` table.
    lua.load("getmetatable('').__metatable = false")
        .set_name("=sandbox")
        .exec()
}

/// `string.gsub` with a function or table replacement, which Lua code calls; the
/// matching runs in `next_match`. [`string_gsub`] calls it with checked arguments, so
/// errors here name the script's position three or four frames up, as C's name the
/// caller of `gsub`.
const REPLACE_WITH: &str = r#"
local next_match = ...
local type, error, concat, sub = type, error, table.concat, string.sub

return function(s, pattern, replacement, limit, by_table)
    local len = #s
    local anchored = sub(pattern, 1, 1) == '^'
    local pieces, count, n, at = {}, 0, 0, 0
    -- A table is indexed by the first capture alone; the others are not checked.
    local mode = by_table and 2 or 1
    local function take(start, finish, ...)
        if start == nil then return nil end
        n = n + 1
        count = count + 1
        pieces[count] = sub(s, at + 1, start)
        local value
        if by_table then value = replacement[(...)] else value = replacement(...) end
        if not value then
            value = sub(s, start + 1, finish)
        elseif type(value) ~= 'string' and type(value) ~= 'number' then
            error('invalid replacement value (a ' .. type(value) .. ')', 4)
        end
        count = count + 1
        pieces[count] = value
        if finish > start then return finish end
        if start < len then
            count = count + 1
            pieces[count] = sub(s, start + 1, start + 1)
            return start + 1
        end
        return len + 1
    end
    while n < limit and at <= len do
        local next_at = take(next_match(s, pattern, at, mode, 3))
        if next_at == nil then break end
        at = next_at
        if anchored then break end
    end
    count = count + 1
    pieces[count] = sub(s, at + 1)
    return concat(pieces), n
end
"#;

/// Replaces C function `library[name]` with C closure `wrapper`: upvalue 1 is the
/// original, then `extra`.
#[allow(unsafe_code)]
pub(crate) fn wrap_c_function(
    lua: &Lua,
    library: &Table,
    name: &CStr,
    wrapper: ffi::lua_CFunction,
    extra: &[Value],
) -> LuaResult<()> {
    let upvalues = c_int::try_from(extra.len() + 1).map_err(mlua::Error::external)?;

    lua.exec_raw_lua(|raw| {
        let state = raw.state();

        // SAFETY: no memory limit is in force while the library is set up (the caller
        // runs before it is set, or holds a `LiftedLimit`), so nothing here raises; the
        // stack is checked for every value pushed and left as it was found.
        unsafe {
            if ffi::lua_checkstack(state, upvalues + 2) == 0 {
                return Err(mlua::Error::runtime("Lua stack is full"));
            }

            let top = ffi::lua_gettop(state);

            raw.push(library)?;
            ffi::lua_getfield(state, -1, name.as_ptr());

            if ffi::lua_tocfunction(state, -1).is_none() {
                ffi::lua_settop(state, top);
                return Err(mlua::Error::runtime("not a C function"));
            }

            for value in extra {
                raw.push(value)?;
            }

            ffi::lua_pushcclosure(state, wrapper, upvalues);
            ffi::lua_setfield(state, -2, name.as_ptr());
            ffi::lua_settop(state, top);
        }

        Ok(())
    })
}

/// Calls the original function of a [`wrap_c_function`] closure on this call's frame:
/// its arguments and results are the same, and the originals use no upvalues of their own.
///
/// # Safety
/// Lua called the closure with `state`.
#[allow(unsafe_code)]
unsafe fn call_original(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: guaranteed by the caller.
    unsafe {
        match ffi::lua_tocfunction(state, ffi::lua_upvalueindex(1)) {
            Some(original) => original(state),
            None => 0,
        }
    }
}

/// Raises if argument 1 is one of the read-only views a [`wrap_c_function`] closure
/// holds as upvalues 2 and up.
///
/// # Safety
/// Lua called such a closure with `state`.
#[allow(unsafe_code)]
unsafe fn refuse_view_argument(state: *mut ffi::lua_State) {
    // SAFETY: guaranteed by the caller; upvalue indexes past the last one read as none,
    // and `lua_rawequal` with an absent argument is false.
    unsafe {
        let mut index = 2;

        while ffi::lua_type(state, ffi::lua_upvalueindex(index)) != ffi::LUA_TNONE {
            if ffi::lua_rawequal(state, 1, ffi::lua_upvalueindex(index)) != 0 {
                raise(state, 0, &[b"Attempt to modify a readonly table"]);
            }

            index += 1;
        }
    }
}

/// Length of table argument 1 (0 for anything else), as `luaL_getn` reads it, without
/// raising: the original function checks its arguments itself.
///
/// # Safety
/// Lua called a function with `state`.
#[allow(unsafe_code)]
unsafe fn table_length(state: *mut ffi::lua_State) -> i64 {
    // SAFETY: guaranteed by the caller.
    unsafe {
        if ffi::lua_type(state, 1) == ffi::LUA_TTABLE {
            ffi::lua_objlen(state, 1) as i64
        } else {
            0
        }
    }
}

/// Integer argument `index` as `luaL_optint` reads it, or `default`, without raising.
///
/// # Safety
/// Lua called a function with `state`.
#[allow(unsafe_code)]
unsafe fn int_argument(state: *mut ffi::lua_State, index: c_int, default: i64) -> i64 {
    // SAFETY: guaranteed by the caller.
    unsafe {
        if ffi::lua_type(state, index) <= ffi::LUA_TNIL {
            default
        } else {
            i64::from(ffi::lua_tointeger_(state, index) as c_int)
        }
    }
}

/// A function that writes its first argument raw (`rawset`), wrapped by
/// [`wrap_c_function`] with the read-only views it must not write.
#[allow(unsafe_code)]
pub(crate) unsafe extern "C-unwind" fn refuse_views(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state.
    unsafe {
        refuse_view_argument(state);
        call_original(state)
    }
}

/// `table.insert`, wrapped as [`refuse_views`], that also refuses a negative position:
/// Lua 5.1 moves every slot from the end of the table down to it, one by one, up to two
/// billion moves between instruction hooks. An insert at a position charges the moves.
#[allow(unsafe_code)]
pub(crate) unsafe extern "C-unwind" fn guarded_insert(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        refuse_view_argument(state);
        if ffi::lua_gettop(state) == 3 {
            // C reads the position with `luaL_checkint`, keeping the low 32 bits.
            let position = ffi::luaL_checkinteger(state, 2) as c_int;

            if position < 0 {
                ffi::luaL_argerror(state, 2, c"position out of bounds".as_ptr());
            }

            let moves = table_length(state) + 1 - i64::from(position);

            charge_work(state, moves.max(0) as u64 / MOVES_PER_INSTRUCTION);
        }

        call_original(state)
    }
}

/// `table.remove`, wrapped as [`refuse_views`], charging the moves it makes.
#[allow(unsafe_code)]
pub(crate) unsafe extern "C-unwind" fn guarded_remove(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        refuse_view_argument(state);
        let len = table_length(state);
        let moves = len - int_argument(state, 2, len);

        charge_work(state, moves.clamp(0, len) as u64 / MOVES_PER_INSTRUCTION);
        call_original(state)
    }
}

/// `table.sort`, wrapped as [`refuse_views`], charging an instruction per element and,
/// without a comparator, the string comparisons: C compares strings with `strcoll`,
/// which reads both, and a sort of n elements makes about n log n comparisons.
#[allow(unsafe_code)]
pub(crate) unsafe extern "C-unwind" fn guarded_sort(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor, and
    // each element read is popped before the next.
    unsafe {
        refuse_view_argument(state);
        let len = table_length(state);
        let n = len.max(1) as u64;
        let mut bytes = 0u64;

        // The length can be far larger than the table (a border found by binary search
        // over a sparse hash part), so the elements are charged before they are read.
        charge_work(state, n);
        if ffi::lua_type(state, 2) <= ffi::LUA_TNIL {
            for index in 1..=len {
                ffi::lua_rawgeti_(state, 1, index as c_int);
                if ffi::lua_type(state, -1) == ffi::LUA_TSTRING {
                    bytes = bytes.saturating_add(ffi::lua_objlen(state, -1) as u64);
                }

                ffi::lua_settop(state, -2);
                charge_visit(state, index as u64);
            }
        }

        let comparisons = n.saturating_mul(u64::from(u64::BITS - n.leading_zeros()));
        let per_comparison = bytes / n / BYTES_PER_INSTRUCTION;

        charge_work(state, comparisons.saturating_mul(per_comparison));
        call_original(state)
    }
}

/// `table.concat(t, sep, i, j)`, wrapped by [`wrap_c_function`] to charge for the
/// elements it visits and the bytes it copies, separators included.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn charge_concat(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor, and
    // each element read is popped before the next.
    unsafe {
        if ffi::lua_type(state, 1) == ffi::LUA_TTABLE {
            let first = int_argument(state, 3, 1);
            let last = int_argument(state, 4, table_length(state));
            let separator = if ffi::lua_type(state, 2) == ffi::LUA_TSTRING {
                ffi::lua_objlen(state, 2) as u64
            } else {
                0
            };
            let mut count = 0u64;
            let mut bytes = 0u64;

            // C stops at the first element that is not a string or number.
            for index in first..=last {
                ffi::lua_rawgeti_(state, 1, index as c_int);
                let kind = ffi::lua_type(state, -1);

                if kind == ffi::LUA_TSTRING {
                    bytes = bytes.saturating_add(ffi::lua_objlen(state, -1) as u64);
                }

                ffi::lua_settop(state, -2);
                if kind != ffi::LUA_TSTRING && kind != ffi::LUA_TNUMBER {
                    break;
                }

                count += 1;
            }

            let separators = separator.saturating_mul(count.saturating_sub(1));

            charge_work(state, count.div_ceil(ELEMENTS_PER_INSTRUCTION));
            charge_bytes(state, bytes.saturating_add(separators));
        }

        call_original(state)
    }
}

/// Charges a loop over table entries every 1,024 entries `visited`, so the quota or the
/// deadline stops a long one.
///
/// # Safety
/// As [`charge_work`].
#[allow(unsafe_code)]
unsafe fn charge_visit(state: *mut ffi::lua_State, visited: u64) {
    if visited.is_multiple_of(1_024) {
        // SAFETY: guaranteed by the caller.
        unsafe { charge_work(state, 1_024 / ELEMENTS_PER_INSTRUCTION) };
    }
}

/// `table.maxn(t)`: the largest positive numeric key, 0 if none. C's walks the whole
/// table between hooks; this one charges as it goes.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn table_maxn(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor, and
    // `lua_next` leaves the key on the stack for the next step.
    unsafe {
        ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);
        let mut max: ffi::lua_Number = 0.0;
        let mut visited = 0u64;

        ffi::lua_pushnil(state);
        while ffi::lua_next(state, 1) != 0 {
            ffi::lua_settop(state, -2);
            if ffi::lua_type(state, -1) == ffi::LUA_TNUMBER {
                max = max.max(ffi::lua_tonumber(state, -1));
            }

            visited += 1;
            charge_visit(state, visited);
        }

        charge_work(state, (visited % 1_024).div_ceil(ELEMENTS_PER_INSTRUCTION));
        ffi::lua_pushnumber(state, max);
        1
    }
}

/// `tonumber(s)` on a string reads all of it.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn charge_string_argument(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        if ffi::lua_type(state, 1) == ffi::LUA_TSTRING {
            charge_bytes(state, ffi::lua_objlen(state, 1) as u64);
        }

        call_original(state)
    }
}

/// `xpcall(f, handler)`, wrapped by [`wrap_c_function`]: the handler runs through
/// [`guarded_handler`], and the error that stopped the script is raised again after.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn guarded_xpcall(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        ffi::lua_settop(state, 2);
        ffi::lua_pushcclosure(state, guarded_handler, 1);
        rethrow_quota(state)
    }
}

/// An `xpcall` handler, the script's as upvalue 1. Lua runs a handler before it unwinds,
/// with hooks off when the error came from the instruction hook, so the script's handler
/// would run unbounded: once the script is stopped it is skipped and the error kept.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn guarded_handler(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state and the error as argument 1.
    unsafe {
        ffi::lua_settop(state, 1);
        if check_deadline().is_err() {
            return 1;
        }

        ffi::lua_pushvalue(state, ffi::lua_upvalueindex(1));
        ffi::lua_insert(state, 1);
        ffi::lua_call(state, 1, 1);
        1
    }
}

/// A function that catches errors (`pcall`, `xpcall`, `coroutine.resume`), wrapped by
/// [`wrap_c_function`] to raise the error that stopped the script again.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn rethrow_quota(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        let results = call_original(state);

        if let Some(error) = STOPPED.get() {
            raise(state, 1, &[error.as_bytes()]);
        }

        results
    }
}

/// `coroutine.create` and `coroutine.wrap`, wrapped by [`wrap_c_function`] to charge
/// [`COROUTINE_COST`] first.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn charge_coroutine(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        if let Err(error) = tick(COROUTINE_COST) {
            raise(state, 1, &[error.as_bytes()]);
        }

        call_original(state)
    }
}

/// A function whose work grows with the length of its string argument (`upper`,
/// `lower`, `reverse`), wrapped by [`wrap_c_function`] to charge it first.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn charge_length(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        let mut len = 0;

        ffi::luaL_checklstring(state, 1, &mut len);
        charge_bytes(state, len as u64);
        call_original(state)
    }
}

/// `string.sub`, wrapped by [`wrap_c_function`] to charge the bytes it copies.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn charge_substring(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        // A string shorter than one instruction's worth costs nothing: skip the sums.
        if ffi::lua_type(state, 1) == ffi::LUA_TSTRING
            && ffi::lua_objlen(state, 1) as u64 >= BYTES_PER_INSTRUCTION
        {
            let len = ffi::lua_objlen(state, 1) as i64;
            // C's `posrelat`, then the clamps of `str_sub`.
            let relative = |position: i64| {
                if position < 0 {
                    len + position + 1
                } else {
                    position
                }
            };
            let start = relative(int_argument(state, 2, 1)).max(1);
            let end = relative(int_argument(state, 3, -1)).min(len);

            charge_bytes(state, (end - start + 1).max(0) as u64);
        }

        call_original(state)
    }
}

/// `string.format`, wrapped by [`wrap_c_function`] to charge the bytes of its string
/// arguments, which `%s` and `%q` copy.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn charge_arguments(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        let mut bytes = 0u64;

        for index in 2..=ffi::lua_gettop(state) {
            if ffi::lua_type(state, index) == ffi::LUA_TSTRING {
                bytes = bytes.saturating_add(ffi::lua_objlen(state, index) as u64);
            }
        }

        charge_bytes(state, bytes);
        call_original(state)
    }
}

/// `string.rep` that returns at once when the result is empty: C appends the empty
/// string `n` times, up to two billion appends between instruction hooks.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn string_rep(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        let mut len = 0;

        ffi::luaL_checklstring(state, 1, &mut len);
        let count = ffi::luaL_checkinteger(state, 2) as c_int;

        if len == 0 || count <= 0 {
            ffi::lua_pushlstring_(state, c"".as_ptr(), 0);
            return 1;
        }

        charge_bytes(state, (len as u64).saturating_mul(count as u64));
        call_original(state)
    }
}

/// `unpack(t, i, j)` with the count computed without overflow. Lua 5.1 computes
/// `j - i + 1` in `int`; past `INT_MAX` it wraps, passes the stack check and writes past
/// the end of the Lua stack.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn table_unpack(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor, and
    // each index pushed has a stack slot from `lua_checkstack`.
    unsafe {
        ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);
        // C reads both bounds with `luaL_checkint`, keeping the low 32 bits.
        let first = ffi::luaL_optinteger(state, 2, 1) as c_int;
        let last = if ffi::lua_type(state, 3) <= ffi::LUA_TNIL {
            ffi::lua_objlen(state, 1) as c_int
        } else {
            ffi::luaL_checkinteger(state, 3) as c_int
        };

        if first > last {
            return 0;
        }

        let count = i64::from(last) - i64::from(first) + 1;
        let Ok(slots) = c_int::try_from(count) else {
            raise(state, 1, &[b"too many results to unpack"]);
        };

        if ffi::lua_checkstack(state, slots) == 0 {
            raise(state, 1, &[b"too many results to unpack"]);
        }

        for index in first..=last {
            ffi::lua_rawgeti_(state, 1, index);
        }

        slots
    }
}

/// `string.find(s, pattern, init, plain)`.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn string_find(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state.
    unsafe { find_aux(state, true) }
}

/// `string.match(s, pattern, init)`.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn string_match(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state.
    unsafe { find_aux(state, false) }
}

/// C's `str_find_aux`.
///
/// # Safety
/// Lua called a function with `state`.
#[allow(unsafe_code)]
unsafe fn find_aux(state: *mut ffi::lua_State, find: bool) -> c_int {
    // SAFETY: guaranteed by the caller; the strings stay on the stack while used.
    unsafe {
        let subject = string_argument(state, 1);
        let pattern = string_argument(state, 2);
        let init = start_position(ffi::luaL_optinteger(state, 3, 1), subject.len());
        let mut found = Found::new();

        if find && (ffi::lua_toboolean(state, 4) != 0 || !lua_pattern::has_specials(pattern)) {
            // A plain search, in linear time: C's naive search is quadratic.
            charge_bytes(state, (subject.len() - init) as u64);
            if let Some(at) = memchr::memmem::find(&subject[init..], pattern) {
                push_position(state, init + at + 1);
                push_position(state, init + at + pattern.len());
                return 2;
            }
        } else if search(
            state,
            1,
            subject,
            pattern,
            init,
            Anchor::Leading,
            &mut found,
        ) {
            let mut pushed = 0;

            if find {
                push_position(state, found.start + 1);
                push_position(state, found.end);
                pushed = 2;
            }

            let count = if find { found.count() } else { found.results() };

            return pushed + push_captures(state, 1, subject, &found, count);
        }

        ffi::lua_pushnil(state);
        1
    }
}

/// `next_match(s, pattern, at, mode, level)`, the primitive under `gmatch` (mode 0: `^`
/// is literal) and `gsub` (mode 1, or 2 for the first capture alone: a leading `^`
/// anchors at `at`). Returns the match's 0-based start and end and its captures (the
/// whole match when it has none), or nothing. Errors name the position of the function
/// `level` frames up.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn pattern_next(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; the strings stay on the stack.
    unsafe {
        let subject = string_argument(state, 1);
        let pattern = string_argument(state, 2);
        let at = usize::try_from(ffi::luaL_checkinteger(state, 3)).unwrap_or(0);
        let mode = ffi::luaL_checkinteger(state, 4);
        let anchor = if mode == 0 {
            Anchor::Literal
        } else {
            Anchor::Leading
        };
        let level = ffi::luaL_checkinteger(state, 5) as c_int;

        // Past the end after an empty match there: the iteration is over.
        if at > subject.len() {
            return 0;
        }

        let mut found = Found::new();

        if !search(state, level, subject, pattern, at, anchor, &mut found) {
            return 0;
        }

        push_position(state, found.start);
        push_position(state, found.end);
        let count = if mode == 2 { 1 } else { found.results() };

        2 + push_captures(state, level, subject, &found, count)
    }
}

/// `string.gmatch(s, pattern)`: an iterator over C closure [`gmatch_next`].
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn string_gmatch(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; nothing here has a destructor.
    unsafe {
        string_argument(state, 1);
        string_argument(state, 2);
        ffi::lua_settop(state, 2);
        ffi::lua_pushinteger(state, 0);
        ffi::lua_pushcclosure(state, gmatch_next, 3);
        1
    }
}

/// C's `gmatch_aux`: upvalues are the subject, the pattern (`^` is literal) and the
/// 0-based position to search from.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn gmatch_next(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; the upvalues are two strings and an
    // integer, and stay alive while the slices are used.
    unsafe {
        let subject = upvalue_string(state, 1);
        let pattern = upvalue_string(state, 2);
        let at = usize::try_from(ffi::lua_tointeger_(state, ffi::lua_upvalueindex(3))).unwrap_or(0);
        let mut found = Found::new();

        if at > subject.len()
            || !search(state, 1, subject, pattern, at, Anchor::Literal, &mut found)
        {
            return 0;
        }

        // An empty match moves on one byte.
        let next = if found.end == found.start {
            found.end + 1
        } else {
            found.end
        };

        push_position(state, next);
        ffi::lua_replace(state, ffi::lua_upvalueindex(3));
        push_captures(state, 1, subject, &found, found.results())
    }
}

/// `string.gsub(s, pattern, replacement, n)`, wrapped by [`wrap_c_function`] with the
/// Lua function that handles a function or table replacement as upvalue 2.
#[allow(unsafe_code)]
unsafe extern "C-unwind" fn string_gsub(state: *mut ffi::lua_State) -> c_int {
    // SAFETY: Lua calls this with its running state; the strings stay on the stack while
    // used, and nothing with a destructor is alive when `lua_call` may raise.
    unsafe {
        let subject = string_argument(state, 1);
        let pattern = string_argument(state, 2);
        let kind = ffi::lua_type(state, 3);
        // C reads it with `luaL_optint`, keeping the low 32 bits.
        let limit = ffi::luaL_optinteger(state, 4, subject.len() as ffi::lua_Integer + 1) as c_int;

        match kind {
            ffi::LUA_TNUMBER | ffi::LUA_TSTRING => {
                let replacement = string_argument(state, 3);

                gsub_text(state, subject, pattern, replacement, limit)
            }
            ffi::LUA_TFUNCTION | ffi::LUA_TTABLE => {
                ffi::lua_pushvalue(state, ffi::lua_upvalueindex(2));
                ffi::lua_pushvalue(state, 1);
                ffi::lua_pushvalue(state, 2);
                ffi::lua_pushvalue(state, 3);
                ffi::lua_pushinteger(state, ffi::lua_Integer::from(limit));
                ffi::lua_pushboolean(state, c_int::from(kind == ffi::LUA_TTABLE));
                ffi::lua_call(state, 5, 2);
                2
            }
            _ => ffi::luaL_argerror(state, 3, c"string/function/table expected".as_ptr()),
        }
    }
}

/// `string.gsub` with a string replacement, as C's `str_gsub`.
///
/// # Safety
/// Lua called [`string_gsub`] with `state`; the slices are its arguments.
#[allow(unsafe_code)]
unsafe fn gsub_text(
    state: *mut ffi::lua_State,
    subject: &[u8],
    pattern: &[u8],
    replacement: &[u8],
    limit: c_int,
) -> c_int {
    // SAFETY: guaranteed by the caller; the buffer stays in this frame, unmoved, from
    // `luaL_buffinit` on.
    unsafe {
        let anchored = pattern.first() == Some(&b'^');
        let mut buffer = MaybeUninit::<ffi::luaL_Buffer>::uninit();
        let buffer = buffer.as_mut_ptr();
        let mut at = 0;
        let mut count: c_int = 0;
        let mut found = Found::new();

        ffi::luaL_buffinit(state, buffer);
        while count < limit {
            if !search(state, 1, subject, pattern, at, Anchor::Leading, &mut found) {
                break;
            }

            count += 1;
            // The replacement is read once per match, the gap copied once.
            charge_bytes(state, (replacement.len() + found.start - at) as u64);
            add_bytes(buffer, &subject[at..found.start]);
            add_replacement(state, buffer, subject, replacement, &found);

            if found.end > found.start {
                at = found.end;
            } else if found.start < subject.len() {
                add_bytes(buffer, &subject[found.start..found.start + 1]);
                at = found.start + 1;
            } else {
                at = subject.len();
                break;
            }

            if anchored {
                break;
            }
        }

        add_bytes(buffer, &subject[at..]);
        ffi::luaL_pushresult(buffer);
        ffi::lua_pushinteger(state, ffi::lua_Integer::from(count));
        2
    }
}

/// C's `add_s`: `%0`-`%9` are captures, `%` before anything else is that character.
///
/// # Safety
/// `buffer` is initialized on `state`; the slices stay on its stack.
#[allow(unsafe_code)]
unsafe fn add_replacement(
    state: *mut ffi::lua_State,
    buffer: *mut ffi::luaL_Buffer,
    subject: &[u8],
    replacement: &[u8],
    found: &Found,
) {
    let mut rest = replacement;

    // SAFETY: guaranteed by the caller.
    unsafe {
        while let Some(escape) = memchr::memchr(b'%', rest) {
            add_bytes(buffer, &rest[..escape]);
            // `%` at the end reads C's terminating NUL.
            let next = rest.get(escape + 1).copied().unwrap_or(0);

            rest = rest.get(escape + 2..).unwrap_or_default();
            if !next.is_ascii_digit() {
                add_bytes(buffer, &[next]);
            } else if next == b'0' {
                charge_bytes(state, (found.end - found.start) as u64);
                add_bytes(buffer, &subject[found.start..found.end]);
            } else {
                match found.capture(usize::from(next - b'1')) {
                    Ok(Capture::Text(start, end)) => {
                        charge_bytes(state, (end - start) as u64);
                        add_bytes(buffer, &subject[start..end]);
                    }
                    Ok(Capture::Position(position)) => {
                        let mut digits = [0; 20];

                        add_bytes(buffer, decimal(position, &mut digits));
                    }
                    Err(error) => raise_pattern(state, 1, error),
                }
            }
        }

        add_bytes(buffer, rest);
    }
}

/// `value` in decimal, written to the end of `digits`: no allocation, which an error
/// unwinding past it would leak.
fn decimal(mut value: usize, digits: &mut [u8; 20]) -> &[u8] {
    let mut start = digits.len();

    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &digits[start..];
        }
    }
}

/// # Safety
/// `buffer` is initialized.
#[allow(unsafe_code)]
unsafe fn add_bytes(buffer: *mut ffi::luaL_Buffer, bytes: &[u8]) {
    // SAFETY: guaranteed by the caller.
    unsafe { ffi::luaL_addlstring(buffer, bytes.as_ptr().cast(), bytes.len()) }
}

/// Runs [`lua_pattern::search`] against the script's step budget; raises its error.
///
/// # Safety
/// Lua called a function with `state`.
#[allow(unsafe_code)]
unsafe fn search(
    state: *mut ffi::lua_State,
    level: c_int,
    subject: &[u8],
    pattern: &[u8],
    init: usize,
    anchor: Anchor,
    found: &mut Found,
) -> bool {
    let mut steps = PATTERN_STEPS.get();
    let outcome = lua_pattern::search(subject, pattern, init, anchor, &mut steps, found);

    PATTERN_STEPS.set(steps);
    match outcome {
        Ok(matched) => matched,
        // SAFETY: guaranteed by the caller.
        Err(error) => unsafe { raise_pattern(state, level, error) },
    }
}

/// Pushes the first `count` captures of `found`.
///
/// # Safety
/// Lua called a function with `state`; `subject` stays on its stack.
#[allow(unsafe_code)]
unsafe fn push_captures(
    state: *mut ffi::lua_State,
    level: c_int,
    subject: &[u8],
    found: &Found,
    count: usize,
) -> c_int {
    // At most `MAX_CAPTURES`.
    let pushed = count as c_int;

    // SAFETY: guaranteed by the caller.
    unsafe {
        ffi::luaL_checkstack(state, pushed, c"too many captures".as_ptr());
        for index in 0..count {
            match found.capture(index) {
                Ok(Capture::Text(start, end)) => {
                    let text = &subject[start..end];

                    ffi::lua_pushlstring_(state, text.as_ptr().cast(), text.len());
                }
                Ok(Capture::Position(position)) => push_position(state, position),
                Err(error) => raise_pattern(state, level, error),
            }
        }
    }

    pushed
}

/// # Safety
/// One free stack slot.
#[allow(unsafe_code)]
unsafe fn push_position(state: *mut ffi::lua_State, position: usize) {
    // SAFETY: guaranteed by the caller. Positions are string offsets, far below 2^53.
    unsafe { ffi::lua_pushinteger(state, position as ffi::lua_Integer) }
}

/// String argument `index`, numbers converted in place as C's `luaL_checklstring` does.
///
/// # Safety
/// Lua called a function with `state`; the slice lives while the argument stays there.
#[allow(unsafe_code)]
unsafe fn string_argument<'a>(state: *mut ffi::lua_State, index: c_int) -> &'a [u8] {
    // SAFETY: guaranteed by the caller.
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_checklstring(state, index, &mut len);

        std::slice::from_raw_parts(ptr.cast(), len)
    }
}

/// String upvalue `index` of the running C closure.
///
/// # Safety
/// Lua called a C closure with `state` whose upvalue `index` is a string; the slice
/// lives while the closure does.
#[allow(unsafe_code)]
unsafe fn upvalue_string<'a>(state: *mut ffi::lua_State, index: c_int) -> &'a [u8] {
    // SAFETY: guaranteed by the caller.
    unsafe {
        let mut len = 0;
        let ptr = ffi::lua_tolstring(state, ffi::lua_upvalueindex(index), &mut len);

        std::slice::from_raw_parts(ptr.cast(), len)
    }
}

/// C's `posrelat` and clamping of `init`: a 0-based start within `0..=len`.
fn start_position(init: ffi::lua_Integer, len: usize) -> usize {
    let len_signed = len as i64;
    let relative = if init < 0 {
        init.saturating_add(len_signed + 1)
    } else {
        init
    };

    usize::try_from(relative.saturating_sub(1)).map_or(0, |start| start.min(len))
}

/// Raises the error a pattern search stopped with.
///
/// # Safety
/// Lua called a function with `state`; nothing with a destructor is alive in the frames
/// this unwinds.
#[allow(unsafe_code)]
unsafe fn raise_pattern(state: *mut ffi::lua_State, level: c_int, error: PatternError) -> ! {
    let message: &[u8] = match error {
        PatternError::Invalid(message) => message.as_bytes(),
        PatternError::TooComplex => b"pattern too complex",
        PatternError::Quota => {
            stop(QUOTA_ERROR);
            b"ERR Lua instruction quota exceeded (pattern matching)"
        }
    };

    // SAFETY: guaranteed by the caller.
    unsafe { raise(state, level, &[message]) }
}

/// Raises `parts` joined, after the position of the function `level` frames up, as
/// `luaL_error` does with level 1 (level 0, a C function, gives none).
///
/// # Safety
/// As [`raise_pattern`].
#[allow(unsafe_code)]
unsafe fn raise(state: *mut ffi::lua_State, level: c_int, parts: &[&[u8]]) -> ! {
    // At most a handful of parts.
    let count = parts.len() as c_int;

    // SAFETY: guaranteed by the caller; the position and the parts get their slots.
    unsafe {
        ffi::luaL_checkstack(state, count + 1, c"error message".as_ptr());
        ffi::luaL_where(state, level);
        for part in parts {
            ffi::lua_pushlstring_(state, part.as_ptr().cast(), part.len());
        }

        ffi::lua_concat(state, count + 1);
        ffi::lua_error(state)
    }
}

#[cfg(test)]
mod tests {
    use mlua::{Lua, LuaOptions, StdLib};

    use super::*;

    const SERIALIZE: &str = r#"
        local function show(...)
            local out = {}
            for i = 1, select('#', ...) do
                local v = select(i, ...)
                out[i] = type(v) .. ':' .. tostring(v)
            end
            return table.concat(out, ',')
        end
        local function collect(s, p)
            local out = {}
            for a, b, c in string.gmatch(s, p) do
                out[#out + 1] = show(a, b, c)
                if #out > 50 then break end
            end
            return table.concat(out, ';')
        end
        local s, p, init = ...
        local lookup = setmetatable({}, {__index = function(_, k) return k == 'b' and '#' or nil end})
        return table.concat({
            show(pcall(string.find, s, p)),
            show(pcall(string.find, s, p, init)),
            show(pcall(string.find, s, p, init, true)),
            show(pcall(string.match, s, p)),
            show(pcall(string.match, s, p, init)),
            show(pcall(string.gsub, s, p, '<%0|%1>')),
            show(pcall(string.gsub, s, p, '%%', init)),
            show(pcall(string.gsub, s, p, lookup)),
            show(pcall(string.gsub, s, p, function(a, b) return b and a .. b or nil end)),
            show(pcall(collect, s, p)),
        }, '\n')
    "#;

    fn state() -> Lua {
        Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )
        .unwrap()
    }

    #[test]
    fn pattern_functions_give_lua_5_1_results() {
        let original = state();
        let bounded = state();

        install(&bounded).unwrap();
        let tokens: &[&str] = &[
            "a", "b", ".", "%a", "%d", "%s", "%A", "[ab]", "[^a]", "[a-c]", "[%d)]", "*", "+", "-",
            "?", "(", ")", "()", "^", "$", "%b()", "%f[a]", "%f[%s]", "%1", "%2", "%", "[", "]",
            "%.", "\0", "%z", "%W",
        ];
        let letters = b"aab()1 b.^$%\0x";
        // A fixed linear congruential generator: the same corpus every run.
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = |bound: usize| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) as usize % bound
        };
        let mut checked = 0;

        for case in 0..4_000 {
            let pattern: String = (0..next(7)).map(|_| tokens[next(tokens.len())]).collect();
            let subject: Vec<u8> = (0..next(12))
                .map(|_| letters[next(letters.len())])
                .collect();
            let init = next(16) as i64 - 8;
            let run = |lua: &Lua| -> String {
                start_script(1_000_000, Duration::from_secs(60));
                let subject = lua.create_string(&subject).unwrap();
                let pattern = lua.create_string(&pattern).unwrap();

                lua.load(SERIALIZE)
                    .call::<mlua::LuaString>((subject, pattern, init))
                    .unwrap()
                    .to_string_lossy()
            };

            assert_eq!(
                run(&bounded),
                run(&original),
                "case {case}: subject {subject:?}, pattern {pattern:?}, init {init}"
            );
            checked += 1;
        }

        assert_eq!(checked, 4_000);
    }

    #[test]
    fn argument_errors_and_edge_cases_match_lua_5_1() {
        let original = state();
        let bounded = state();

        install(&bounded).unwrap();
        for expression in [
            "string.gmatch()",
            "string.gmatch('a')",
            "string.gmatch(12, 2)()",
            "string.gsub()",
            "string.gsub('a', 'a')",
            "string.gsub('a', 'a', true)",
            "string.gsub('a', 'a', 'b', 'x')",
            "string.gsub('aaa', 'a', 'b', 2.7)",
            "string.gsub('aaa', 'a', 'b', -1)",
            "string.gsub('aaa', 'a', 'b', '2')",
            "string.gsub('aaa', 'a', 'b', 2^32 + 1)",
            "string.gsub('abc', '%w', {a = 1, b = true})",
            "string.gsub('abc', '%w', {a = {}})",
            "string.gsub('abc', '(%w)', function(c) return {} end)",
            "string.gsub('abc', '(%w)', function(c) return c:upper() end, 2)",
            "(function() for m in string.gmatch('abc', '%') do end end)()",
            "(function() local t = {} for a, b in string.gmatch('k=v, x=y', '(%w+)=(%w+)') do t[#t + 1] = a .. b end return table.concat(t) end)()",
            "string.find('a', '(()')",
            "string.find('abc', 'b', 10)",
            "string.find('abc', '', 10)",
            "string.match('  x', '()x()')",
            "string.gsub('abc', '', '-')",
            "string.gsub('abc', 'b*', '-')",
            "string.gsub('hello world', '(%w+)', '%1 %1')",
            "string.gsub('hello', '(l)(l)', '%2%1%0%%')",
            "string.gsub('abc', '%w', '%2')",
            "string.gsub('abc', '%w', '%')",
            "string.gsub('abc', '^a', 'x')",
            "string.gsub('abc', '^b', 'x')",
            "string.gsub('^a^a', '^a', {['^a'] = 'y'})",
            "string.gsub(123, 2, 9)",
            "string.gsub('THE (quick) fox', '%f[%a]%a+', function(w) return w:lower() end)",
            "string.gsub('x(a(b)c)y', '%b()', '')",
            "string.rep('ab', 3, 'x')",
            "unpack({1, 2, 3}, '2', '3')",
            "unpack({1, 2, 3}, 2.9)",
            "unpack({1, 2, 3}, 2, nil)",
            "unpack()",
            "table.insert({}, 1, 2, 3)",
            "(function() local t = {1, 2} table.insert(t, 0, 0) return #t end)()",
        ] {
            let chunk = format!(
                "local function show(...) local out = {{}} for i = 1, select('#', ...) do \
                 local v = select(i, ...) out[i] = type(v) .. ':' .. tostring(v) end \
                 return table.concat(out, ',') end \
                 return show(pcall(function() return show({expression}) end)) .. ' / ' .. \
                 show(pcall(function() return {expression} end))"
            );
            let run = |lua: &Lua| -> String {
                start_script(1_000_000, Duration::from_secs(60));
                lua.load(&chunk).set_name("=case").eval::<String>().unwrap()
            };

            assert_eq!(run(&bounded), run(&original), "{expression}");
        }
    }

    #[test]
    fn deep_patterns_are_refused_and_long_scans_are_charged() {
        let mut steps = u64::MAX;

        assert_eq!(
            lua_pattern::search(
                &[b'a'; 300],
                "a?".repeat(300).as_bytes(),
                0,
                Anchor::Leading,
                &mut steps,
                &mut Found::new(),
            )
            .unwrap_err(),
            PatternError::TooComplex
        );

        let mut steps = 1_000;

        assert_eq!(
            lua_pattern::search(
                &[b'a'; 1_000],
                b".-b",
                0,
                Anchor::Leading,
                &mut steps,
                &mut Found::new(),
            )
            .unwrap_err(),
            PatternError::Quota
        );
        assert_eq!(steps, 0);
    }
}
