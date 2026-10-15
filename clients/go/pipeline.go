// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
)

// Pipeline buffers commands and sends them in one write; replies come back in order.
// A Pipeline is not safe for concurrent use. It can be reused after Exec.
type Pipeline struct {
	ex  executor
	r   *request
	n   int
	err error
}

// Pipeline returns an empty pipeline on the shared connection.
func (c *Client) Pipeline() *Pipeline { return &Pipeline{ex: c} }

// Pipeline returns an empty pipeline on this dedicated connection.
func (d *Conn) Pipeline() *Pipeline { return &Pipeline{ex: d} }

// Pipelined queues commands in fn and executes them.
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

// Do queues any command and returns its index in the Exec result.
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

// DoBytes queues a binary command.
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

// Set queues SET key value.
func (p *Pipeline) Set(key, value string) int { p.cmd(3, cSET).s(key).s(value); return p.n - 1 }

// SetPX queues SET key value PX ttl.
func (p *Pipeline) SetPX(key, value string, ttlMs int64) int {
	p.cmd(5, cSET).s(key).s(value).t(tPX).i(ttlMs)
	return p.n - 1
}

// Get queues GET key.
func (p *Pipeline) Get(key string) int { p.cmd(2, cGET).s(key); return p.n - 1 }

// Del queues DEL keys.
func (p *Pipeline) Del(keys ...string) int { p.cmd(1+len(keys), cDEL).ss(keys); return p.n - 1 }

// Incr queues INCR key.
func (p *Pipeline) Incr(key string) int { p.cmd(2, cINCR).s(key); return p.n - 1 }

// IncrBy queues INCRBY key delta.
func (p *Pipeline) IncrBy(key string, delta int64) int {
	p.cmd(3, cINCRBY).s(key).i(delta)
	return p.n - 1
}

// PExpire queues PEXPIRE key ms.
func (p *Pipeline) PExpire(key string, ms int64) int { p.cmd(3, cPEXPIRE).s(key).i(ms); return p.n - 1 }

// HSet queues HSET key field value...
func (p *Pipeline) HSet(key string, fieldValues ...string) int {
	p.cmd(2+len(fieldValues), cHSET).s(key).ss(fieldValues)
	return p.n - 1
}

// HGet queues HGET key field.
func (p *Pipeline) HGet(key, field string) int { p.cmd(3, cHGET).s(key).s(field); return p.n - 1 }

// LPush queues LPUSH.
func (p *Pipeline) LPush(key string, values ...string) int {
	p.cmd(2+len(values), cLPUSH).s(key).ss(values)
	return p.n - 1
}

// RPush queues RPUSH.
func (p *Pipeline) RPush(key string, values ...string) int {
	p.cmd(2+len(values), cRPUSH).s(key).ss(values)
	return p.n - 1
}

// SAdd queues SADD.
func (p *Pipeline) SAdd(key string, members ...string) int {
	p.cmd(2+len(members), cSADD).s(key).ss(members)
	return p.n - 1
}

// ZAdd queues ZADD.
func (p *Pipeline) ZAdd(key string, members ...Z) int {
	r := p.cmd(2+2*len(members), cZADD).s(key)
	for _, m := range members {
		r.f(m.Score).s(m.Member)
	}
	return p.n - 1
}

// Publish queues PUBLISH.
func (p *Pipeline) Publish(channel, message string) int {
	p.cmd(3, cPUBLISH).s(channel).s(message)
	return p.n - 1
}

// Len is the number of queued commands.
func (p *Pipeline) Len() int { return p.n }

// Discard drops queued commands.
func (p *Pipeline) Discard() {
	if p.r != nil {
		putReq(p.r)
	}
	p.r, p.n, p.err = nil, 0, nil
}

// Exec sends every queued command in one write and returns all replies in order.
// Server errors stay in place as KindError values; the returned error is the first
// of them (as *Error), or a transport/context error.
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

// ErrTxAborted is returned by EXEC when a WATCHed key changed.
var ErrTxAborted = errors.New("ruvio: transaction aborted (watched key changed)")

// Tx is a MULTI/EXEC transaction on a dedicated connection. Typed methods on the
// embedded Conn run immediately (use them for reads after Watch); queue the
// transaction body with Exec's pipeline or with Multi + Queue + ExecQueued.
type Tx struct {
	*Conn
	watching bool
	inMulti  bool
	queued   int
}

// Tx takes a dedicated connection for a transaction. Close it when done.
func (c *Client) Tx(ctx context.Context) (*Tx, error) {
	d, err := c.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{Conn: d}, nil
}

// Transaction runs the commands queued by fn atomically (MULTI ... EXEC) on a
// dedicated connection and returns the EXEC results.
func (c *Client) Transaction(ctx context.Context, fn func(p *Pipeline)) ([]Value, error) {
	tx, err := c.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	return tx.Exec(ctx, fn)
}

// Watch watches keys for the next EXEC.
func (t *Tx) Watch(ctx context.Context, keys ...string) error {
	err := t.ok(ctx, newCmd(1+len(keys), cWATCH).ss(keys))
	if err == nil {
		t.watching = true
	}
	return err
}

// Unwatch forgets watched keys.
func (t *Tx) Unwatch(ctx context.Context) error {
	err := t.ok(ctx, newCmd(1, cUNWATCH))
	if err == nil {
		t.watching = false
	}
	return err
}

// Multi starts queueing explicitly; follow with Queue and ExecQueued or Discard.
func (t *Tx) Multi(ctx context.Context) error {
	err := t.ok(ctx, newCmd(1, cMULTI))
	if err == nil {
		t.inMulti, t.queued = true, 0
	}
	return err
}

// Queue sends one command inside MULTI (the server replies QUEUED).
func (t *Tx) Queue(ctx context.Context, args ...any) error {
	_, err := t.Do(ctx, args...)
	if err == nil {
		t.queued++
	}
	return err
}

// Discard drops the queued commands.
func (t *Tx) Discard(ctx context.Context) error {
	err := t.ok(ctx, newCmd(1, cDISCARD))
	t.inMulti = false
	return err
}

// ExecQueued sends EXEC after Multi/Queue.
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

// Exec sends MULTI, the commands queued by fn and EXEC in one write. It returns the
// EXEC results, or ErrTxAborted when a watched key changed.
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

// Close resets transaction state and returns the connection to the client.
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
