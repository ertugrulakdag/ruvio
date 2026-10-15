// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
)

// bind prepares a cluster-mode dedicated connection for the n commands in r: it checks
// that their keys share one slot (and the slot the connection is already bound to) and
// takes the connection from the owning node on first use. done=true means the call is
// finished (r recycled) and v/err are its result; done=false means send r on d.cn.
//
// A MULTI sent before any key is known is deferred and goes out in front of the first
// keyed command, so Tx.Multi followed by Queue works.
func (d *Conn) bind(ctx context.Context, r *request, n int) (v Value, done bool, err error) {
	fail := func(err error) (Value, bool, error) {
		putReq(r)
		return Value{}, true, err
	}
	if d.closed {
		return fail(ErrClosed)
	}
	slot, off := -1, 0
	for i := 0; i < n; i++ {
		_, s, _, err := routeFrame(r.buf[off:])
		if err != nil {
			return fail(err)
		}
		end, ok := frameEnd(r.buf[off:])
		if !ok {
			return fail(ErrProtocol)
		}
		if s == slotCross {
			return fail(crossSlotErr(r.buf[off : off+end]))
		}
		if s >= 0 {
			if slot >= 0 && s != slot {
				return fail(fmt.Errorf("%w (transaction or connection)", ErrCrossSlot))
			}
			slot = s
		}
		off += end
	}
	t := d.cl.t.Load()
	if t == nil {
		return fail(ErrClosed)
	}
	if d.cn == nil {
		if n == 1 && slot < 0 {
			name := cmdName(r.buf)
			switch {
			case upperEq(name, "MULTI"):
				d.deferMulti = true
				putReq(r)
				return Value{Kind: KindSimple, Bytes: []byte("OK")}, true, nil
			case d.deferMulti && upperEq(name, "EXEC"):
				d.deferMulti = false
				putReq(r)
				return Value{Kind: KindArray}, true, nil
			case d.deferMulti && upperEq(name, "DISCARD"):
				d.deferMulti = false
				putReq(r)
				return Value{Kind: KindSimple, Bytes: []byte("OK")}, true, nil
			}
		}
		node := d.cl.seed
		if slot >= 0 {
			node = t.nodeAt(slot, d.cl.seed)
		}
		cn, err := node.acquire(ctx)
		if err != nil {
			return fail(err)
		}
		d.c, d.cn, d.slot = node, cn, slot
		if d.deferMulti {
			d.deferMulti = false
			if _, err := cn.exec(ctx, newCmd(1, cMULTI)); err != nil {
				return fail(err)
			}
		}
		return Value{}, false, nil
	}
	if slot >= 0 {
		if d.slot >= 0 && slot != d.slot {
			return fail(fmt.Errorf("%w (connection is bound to slot %d)", ErrCrossSlot, d.slot))
		}
		if d.slot < 0 {
			if t.nodeAt(slot, d.cl.seed) != d.c {
				return fail(fmt.Errorf("%w (connection is bound to node %s)", ErrCrossSlot, d.c.opts.Addr))
			}
			d.slot = slot
		}
	}
	return Value{}, false, nil
}

// ConnFor takes a dedicated connection to the node that owns key (cluster mode). On a
// standalone server it is Conn.
func (c *Client) ConnFor(ctx context.Context, key string) (*Conn, error) {
	d, err := c.Conn(ctx)
	if err != nil || d.cl == nil {
		return d, err
	}
	r := newCmd(2, cEXISTS).s(key) // route only; nothing is sent
	_, done, err := d.bind(ctx, r, 1)
	if done {
		return nil, err
	}
	putReq(r)
	return d, nil
}

// TxFor is Tx on the node that owns key (cluster mode); every key used in the
// transaction must share key's slot.
func (c *Client) TxFor(ctx context.Context, key string) (*Tx, error) {
	d, err := c.ConnFor(ctx, key)
	if err != nil {
		return nil, err
	}
	return &Tx{Conn: d}, nil
}
