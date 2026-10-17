// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
)

// Pipeline buffers commands and sends them all in one write; the replies come back in
// order, so many commands cost one round trip. Queue commands with the typed methods
// below (Set, Get ...) or with Do for any other command; each returns the index of the
// command in the Exec result. Nothing is sent until Exec.
//
// A Pipeline is not safe for concurrent use. It can be reused after Exec or Discard. A
// pipeline is not a transaction: other clients' commands can run in between (use
// Client.Transaction for that), and a failing command does not stop the following ones.
//
// In cluster mode a pipeline is grouped per shard and the replies are returned in the
// original order. A command whose keys span slots is split when it is MGET, MSET, DEL,
// UNLINK, EXISTS or TOUCH; any other command with keys in several slots makes the whole
// pipeline fail with ErrCrossSlot before anything is sent.
//
//	p := c.Pipeline()
//	iSet := p.Set("a", "1")
//	iGet := p.Get("a")
//	vals, err := p.Exec(ctx)
//	fmt.Println(vals[iGet].Str(), iSet)
type Pipeline struct {
	ex  executor
	r   *request
	n   int
	err error
}

// Pipeline returns an empty pipeline that runs on the shared connection (or the cluster
// router).
func (c *Client) Pipeline() *Pipeline { return &Pipeline{ex: c} }

// Pipeline returns an empty pipeline that runs on this dedicated connection.
func (d *Conn) Pipeline() *Pipeline { return &Pipeline{ex: d} }

// Pipelined queues commands in fn and executes them, a shortcut for Pipeline, fn and
// Exec. It returns the replies in order. See Pipeline.Exec for the error rules.
//
//	vals, err := c.Pipelined(ctx, func(p *ruvio.Pipeline) {
//	    p.Incr("hits")
//	    p.PExpire("hits", 60000)
//	})
func (c *Client) Pipelined(ctx context.Context, fn func(p *Pipeline)) ([]Value, error) {
	p := c.Pipeline()
	fn(p)
	return p.Exec(ctx)
}

func (p *Pipeline) req() *request {
	if p.r == nil {
		p.r = getReq()
	}
	p.n++
	return p.r
}

// Do queues any command and returns its index in the Exec result. Arguments are encoded
// like Client.Do. If an argument has an unsupported type the command is not queued, Do
// returns -1 and Exec later returns that error without sending anything.
func (p *Pipeline) Do(args ...any) int {
	r := p.req()
	mark := len(r.buf)
	var err error
	if r.buf, err = AppendCommand(r.buf, args...); err != nil {
		r.buf = r.buf[:mark]
		p.n--
		if p.err == nil {
			p.err = err
		}
		return -1
	}
	return p.n - 1
}

// DoBytes queues a command whose arguments are raw bytes and returns its index in the
// Exec result.
func (p *Pipeline) DoBytes(args ...[]byte) int {
	r := p.req()
	r.buf = appendArrayHeader(r.buf, len(args))
	for _, a := range args {
		r.b(a)
	}
	return p.n - 1
}

func (p *Pipeline) cmd(n int, name []byte) *request {
	r := p.req()
	r.buf = appendArrayHeader(r.buf, n)
	r.buf = append(r.buf, name...)
	return r
}

// Set queues SET key value and returns the command's index. The reply is "OK".
func (p *Pipeline) Set(key, value string) int { p.cmd(3, cSET).s(key).s(value); return p.n - 1 }

// SetPX queues SET key value PX ttlMs (value with a TTL in milliseconds) and returns the
// command's index.
func (p *Pipeline) SetPX(key, value string, ttlMs int64) int {
	p.cmd(5, cSET).s(key).s(value).t(tPX).i(ttlMs)
	return p.n - 1
}

// Get queues GET key and returns the command's index. The reply is a bulk string, or a
// nil Value (IsNil) for a missing key.
func (p *Pipeline) Get(key string) int { p.cmd(2, cGET).s(key); return p.n - 1 }

// Del queues DEL with the keys and returns the command's index. The reply is an integer,
// the number of keys that existed.
func (p *Pipeline) Del(keys ...string) int { p.cmd(1+len(keys), cDEL).ss(keys); return p.n - 1 }

// Incr queues INCR key and returns the command's index. The reply is the new integer
// value.
func (p *Pipeline) Incr(key string) int { p.cmd(2, cINCR).s(key); return p.n - 1 }

// IncrBy queues INCRBY key delta and returns the command's index. The reply is the new
// integer value.
func (p *Pipeline) IncrBy(key string, delta int64) int {
	p.cmd(3, cINCRBY).s(key).i(delta)
	return p.n - 1
}

// PExpire queues PEXPIRE key ms and returns the command's index. The reply is 1 when the
// TTL was set and 0 for a missing key.
func (p *Pipeline) PExpire(key string, ms int64) int { p.cmd(3, cPEXPIRE).s(key).i(ms); return p.n - 1 }

// HSet queues HSET key with field, value pairs and returns the command's index. The
// reply is the number of new fields.
func (p *Pipeline) HSet(key string, fieldValues ...string) int {
	p.cmd(2+len(fieldValues), cHSET).s(key).ss(fieldValues)
	return p.n - 1
}

// HGet queues HGET key field and returns the command's index. The reply is a bulk
// string, or nil for a missing field.
func (p *Pipeline) HGet(key, field string) int { p.cmd(3, cHGET).s(key).s(field); return p.n - 1 }

// LPush queues LPUSH key values and returns the command's index. The reply is the new
// list length.
func (p *Pipeline) LPush(key string, values ...string) int {
	p.cmd(2+len(values), cLPUSH).s(key).ss(values)
	return p.n - 1
}

// RPush queues RPUSH key values and returns the command's index. The reply is the new
// list length.
func (p *Pipeline) RPush(key string, values ...string) int {
	p.cmd(2+len(values), cRPUSH).s(key).ss(values)
	return p.n - 1
}

// SAdd queues SADD key members and returns the command's index. The reply is the number
// of new members.
func (p *Pipeline) SAdd(key string, members ...string) int {
	p.cmd(2+len(members), cSADD).s(key).ss(members)
	return p.n - 1
}

// ZAdd queues ZADD key with the members and scores and returns the command's index. The
// reply is the number of new members.
func (p *Pipeline) ZAdd(key string, members ...Z) int {
	r := p.cmd(2+2*len(members), cZADD).s(key)
	for _, m := range members {
		r.f(m.Score).s(m.Member)
	}
	return p.n - 1
}

// Publish queues PUBLISH channel message and returns the command's index. The reply is
// the receiver count on the local shard.
func (p *Pipeline) Publish(channel, message string) int {
	p.cmd(3, cPUBLISH).s(channel).s(message)
	return p.n - 1
}

// Len returns how many commands are queued.
func (p *Pipeline) Len() int { return p.n }

// Discard drops the queued commands without sending them and clears any pending encoding
// error.
func (p *Pipeline) Discard() {
	if p.r != nil {
		putReq(p.r)
	}
	p.r, p.n, p.err = nil, 0, nil
}

// Exec sends every queued command in one write and returns all the replies in order,
// then empties the pipeline. Calling it on an empty pipeline returns nil, nil.
//
// Server errors stay in place in the result as Values of Kind KindError, so one failed
// command does not hide the others; the returned error is the first of them (an *Error).
// A transport failure or a cancelled context returns that error instead, and the result
// may be shorter than expected. If Do rejected an argument, that error is returned and
// nothing is sent. In cluster mode see the Pipeline type for ErrCrossSlot.
func (p *Pipeline) Exec(ctx context.Context) ([]Value, error) {
	if p.err != nil {
		err := p.err
		p.Discard()
		return nil, err
	}
	if p.n == 0 {
		return nil, nil
	}
	r, n := p.r, p.n
	p.r, p.n = nil, 0
	vals, err := p.ex.execMany(ctx, r, n)
	if err != nil {
		return vals, err
	}
	return vals, firstErr(vals)
}

func firstErr(vals []Value) error {
	for _, v := range vals {
		if v.Kind == KindError {
			return &Error{Msg: b2s(v.Bytes)}
		}
	}
	return nil
}

// ErrTxAborted is returned by Tx.Exec, Tx.ExecQueued and Client.Transaction when EXEC
// found a WATCHed key modified by another connection (or by this one) and ran nothing.
// Re-read the data and run the transaction again.
var ErrTxAborted = errors.New("ruvio: transaction aborted (watched key changed)")

// Tx is a MULTI/EXEC transaction on a dedicated connection. The typed methods of the
// embedded Conn (Get, HGet ...) run immediately, so use them to read values after Watch.
// Queue the writes of the transaction with Exec and its pipeline, or with Multi, Queue
// and ExecQueued. Always Close a Tx.
//
// The commands run one after another without other commands in between and are written
// to the log as one record, but this is not an ACID transaction: there is no rollback of
// the effects of the commands that succeeded when a later one fails at run time (errors
// appear in the result). A transaction is local to one shard: all its keys must hash to
// the same slot even on a single-shard server, so use hash tags for several keys. The
// server allows at most 1024 queued commands and 128 watched keys, and refuses to queue
// BLPOP/BRPOP, SAVE/BGSAVE, AUTH, scripts and cluster discovery commands.
//
//	// optimistic increment with retry
//	tx, err := c.Tx(ctx)
//	if err != nil {
//	    return err
//	}
//	defer tx.Close()
//	for {
//	    if err := tx.Watch(ctx, "balance"); err != nil {
//	        return err
//	    }
//	    n, _, _ := tx.Get(ctx, "balance")
//	    _, err := tx.Exec(ctx, func(p *ruvio.Pipeline) {
//	        p.Set("balance", next(n))
//	    })
//	    if errors.Is(err, ruvio.ErrTxAborted) {
//	        continue // somebody changed it; try again
//	    }
//	    return err
//	}
type Tx struct {
	*Conn
	watching bool
	inMulti  bool
	queued   int
}

// Tx takes a dedicated connection for a transaction. Close it when done. In cluster mode
// the connection comes from the shard of the first key used and all keys must share a
// slot; use TxFor to pick the shard from a key up front.
func (c *Client) Tx(ctx context.Context) (*Tx, error) {
	d, err := c.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{Conn: d}, nil
}

// Transaction runs the commands queued by fn atomically (MULTI ... EXEC) on a dedicated
// connection and returns the EXEC results, one Value per queued command. There is no
// WATCH, so it cannot abort because of other writers; use Tx for optimistic locking. A
// command rejected while queueing (wrong arity, unknown command) makes the whole EXEC
// fail with that *Error. A command that fails at run time shows up as a KindError Value
// in the result and as the returned error (the first one), while the other commands
// still ran. In cluster mode all keys must share a slot.
//
//	res, err := c.Transaction(ctx, func(p *ruvio.Pipeline) {
//	    p.Incr("{acct:1}:balance")
//	    p.RPush("{acct:1}:log", "deposit")
//	})
func (c *Client) Transaction(ctx context.Context, fn func(p *Pipeline)) ([]Value, error) {
	tx, err := c.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	return tx.Exec(ctx, fn)
}

// Watch marks keys to watch (WATCH). If any of them is modified by another connection
// (or this one) before Exec, Exec runs nothing and returns ErrTxAborted. WATCH must come
// before Multi/Exec. At least one key is required; the server allows 128 watched keys
// per connection. All keys must share a slot in cluster mode.
func (t *Tx) Watch(ctx context.Context, keys ...string) error {
	err := t.ok(ctx, newCmd(1+len(keys), cWATCH).ss(keys))
	if err == nil {
		t.watching = true
	}
	return err
}

// Unwatch forgets all watched keys (UNWATCH).
func (t *Tx) Unwatch(ctx context.Context) error {
	err := t.ok(ctx, newCmd(1, cUNWATCH))
	if err == nil {
		t.watching = false
	}
	return err
}

// Multi starts queueing explicitly (MULTI). Send commands with Queue, then finish with
// ExecQueued or Discard. Exec does MULTI for you; use Multi only when commands must be
// queued one by one. In cluster mode the MULTI is held back until the first keyed
// command picks the shard.
func (t *Tx) Multi(ctx context.Context) error {
	err := t.ok(ctx, newCmd(1, cMULTI))
	if err == nil {
		t.inMulti, t.queued = true, 0
	}
	return err
}

// Queue sends one command while in MULTI and returns nil when the server answered
// QUEUED. A command the server rejects (wrong arity, unknown) returns that error and
// EXEC will then fail. Arguments are encoded like Client.Do.
func (t *Tx) Queue(ctx context.Context, args ...any) error {
	_, err := t.Do(ctx, args...)
	if err == nil {
		t.queued++
	}
	return err
}

// Discard drops the commands queued since Multi (DISCARD). Watched keys stay watched;
// call Unwatch to drop them.
func (t *Tx) Discard(ctx context.Context) error {
	err := t.ok(ctx, newCmd(1, cDISCARD))
	t.inMulti = false
	return err
}

// ExecQueued sends EXEC after Multi and Queue and returns one Value per queued command.
// It returns ErrTxAborted when a watched key changed. A runtime error of one command
// appears as a KindError Value, and the first of them is also returned as the error.
// Watches are cleared afterwards.
func (t *Tx) ExecQueued(ctx context.Context) ([]Value, error) {
	v, err := t.value(ctx, newCmd(1, cEXEC))
	t.inMulti, t.watching = false, false
	return execReply(v, err)
}

func execReply(v Value, err error) ([]Value, error) {
	if err != nil {
		return nil, err
	}
	if v.Kind == KindNil {
		return nil, ErrTxAborted
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	return v.Array, firstErr(v.Array)
}

// Exec sends MULTI, the commands queued by fn and EXEC in one write, and returns one
// Value per queued command. It returns ErrTxAborted when a watched key changed (nothing
// ran), or the server error when a command was rejected while queueing (nothing ran).
// Runtime errors of single commands stay in the result as KindError Values, and the
// first is returned as the error. Watches are cleared afterwards, whatever the outcome.
// The Pipeline passed to fn is only valid inside fn.
func (t *Tx) Exec(ctx context.Context, fn func(p *Pipeline)) ([]Value, error) {
	p := &Pipeline{ex: t.Conn}
	p.cmd(1, cMULTI)
	fn(p)
	if p.err != nil {
		err := p.err
		p.Discard()
		return nil, err
	}
	p.cmd(1, cEXEC)
	r, n := p.r, p.n
	p.r, p.n = nil, 0
	vals, err := t.Conn.execMany(ctx, r, n)
	t.watching = false
	if err != nil {
		return nil, err
	}
	// MULTI -> OK, each command -> QUEUED (or an error), EXEC -> results.
	for _, v := range vals[:n-1] {
		if v.Kind == KindError {
			if last := vals[n-1]; last.Kind == KindError {
				return nil, &Error{Msg: b2s(last.Bytes)}
			}
			return nil, &Error{Msg: b2s(v.Bytes)}
		}
	}
	last := vals[n-1]
	if last.Kind == KindError {
		return nil, &Error{Msg: b2s(last.Bytes)}
	}
	return execReply(last, nil)
}

// Close discards an open MULTI, drops watches and returns the connection to the client.
// If cleanup fails the connection is closed rather than reused. Safe to call more than
// once.
func (t *Tx) Close() error {
	if t.Conn == nil || t.cn == nil {
		return nil
	}
	ctx := context.Background()
	if t.inMulti {
		if t.Discard(ctx) != nil {
			t.dirty = true
		}
	}
	if t.watching {
		if t.Unwatch(ctx) != nil {
			t.dirty = true
		}
	}
	return t.Conn.Close()
}
