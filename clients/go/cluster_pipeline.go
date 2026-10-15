// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"sync"
)

// ---- pipelines ---------------------------------------------------------------------------------------

func (c *Client) execManyLocal(ctx context.Context, r *request, n int) ([]Value, error) {
	cn, err := c.shared(ctx)
	if err != nil {
		putReq(r)
		return nil, err
	}
	return cn.execMany(ctx, r, n)
}

// rtMany sends a pipeline of n commands; see rt for the ownership rules.
func (c *Client) rtMany(ctx context.Context, r *request, n int) (bool, error) {
	r.expect = n
	r.vals = make([]Value, 0, n)
	return c.rt(ctx, r, false)
}

func (cl *cluster) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	t, err := cl.topo(ctx)
	if err != nil {
		putReq(r)
		return nil, err
	}
	if t == nil {
		return cl.seed.execManyLocal(ctx, r, n)
	}
	// pass 1: where does each command go? Allocation-free when they share one node.
	first, uniform, sawMulti, differ, firstSlot := -1, true, false, false, -1
	off := 0
	for i := 0; i < n; i++ {
		_, slot, _, err := routeFrame(r.buf[off:])
		if err != nil {
			putReq(r)
			return nil, err
		}
		end, ok := frameEnd(r.buf[off:])
		if !ok {
			putReq(r)
			return nil, ErrProtocol
		}
		off += end
		if upperEq(cmdName(r.buf[off-end:]), "MULTI") {
			sawMulti = true
		}
		if slot == slotCross {
			putReq(r)
			return nil, crossSlotErr(r.buf[off-end:])
		}
		if slot < 0 {
			continue
		}
		if firstSlot < 0 {
			firstSlot = slot
		} else if slot != firstSlot {
			differ = true
		}
		ni := int(t.owner[slot])
		if first < 0 {
			first = ni
		} else if ni != first {
			uniform = false
		}
	}
	if sawMulti && differ {
		putReq(r)
		return nil, fmt.Errorf("%w (MULTI)", ErrCrossSlot)
	}
	if sawMulti { // a transaction travels as one unit to the node of its slot
		uniform = true
	}
	if uniform {
		node := cl.seed
		if firstSlot >= 0 && first != noOwner {
			node = t.nodes[first]
		}
		return cl.pipeOne(ctx, t, node, r, n)
	}
	return cl.pipeMany(ctx, t, r, n)
}

func cmdName(b []byte) []byte {
	var st [1][]byte
	a, _, _, ok := frameArgs(b, 1, st[:0])
	if !ok || len(a) == 0 {
		return nil
	}
	return a[0]
}

// frameAt returns the i-th command frame of a pipeline buffer.
func frameAt(buf []byte, i int) []byte {
	off := 0
	for ; i > 0; i-- {
		end, _ := frameEnd(buf[off:])
		off += end
	}
	end, _ := frameEnd(buf[off:])
	return buf[off : off+end]
}

// pipeOne sends the whole pipeline to one node and repairs -MOVED replies.
func (cl *cluster) pipeOne(ctx context.Context, t *topology, node *Client, r *request, n int) ([]Value, error) {
	ok, err := node.rtMany(ctx, r, n)
	if !ok {
		return nil, err
	}
	if r.err != nil {
		err := r.err
		putReq(r)
		return nil, err
	}
	vals := r.vals
	for i := range vals {
		if vals[i].Kind == KindError {
			if _, _, isMoved := parseMoved(vals[i].Bytes); isMoved {
				cl.redo(ctx, t, frameAt(r.buf, i), &vals[i])
			}
		}
	}
	putReq(r)
	return vals, nil
}

// redo sends one command again after -MOVED (the table is updated first) and stores the
// outcome in dst.
func (cl *cluster) redo(ctx context.Context, t *topology, frame []byte, dst *Value) {
	req := getReq()
	req.buf = append(req.buf, frame...)
	if ms, addr, ok := parseMoved(dst.Bytes); ok {
		if nn, err := cl.applyMoved(ctx, ms, addr); err == nil {
			ok2, err2 := nn.rt(ctx, req, false)
			if !ok2 {
				*dst = Value{Kind: KindError, Bytes: []byte(err2.Error())}
				return
			}
			if req.err != nil {
				*dst = Value{Kind: KindError, Bytes: []byte(req.err.Error())}
			} else {
				*dst = req.val
			}
			putReq(req)
			return
		}
	}
	putReq(req)
}

// pipeMany groups the commands by node, sends each group as one pipelined write
// (concurrently) and puts the replies back in the caller's order.
func (cl *cluster) pipeMany(ctx context.Context, t *topology, r *request, n int) ([]Value, error) {
	offs := make([]int, n+1)
	target := make([]int, n)
	cur := -1
	off := 0
	for i := 0; i < n; i++ {
		_, slot, _, _ := routeFrame(r.buf[off:])
		end, _ := frameEnd(r.buf[off:])
		offs[i] = off
		off += end
		if slot >= 0 {
			cur = int(t.owner[slot])
		}
		target[i] = cur // keyless commands follow the previous keyed one
	}
	offs[n] = off
	for i := 0; i < n && target[i] < 0; i++ { // leading keyless commands join the first keyed group
		for _, tg := range target {
			if tg >= 0 {
				target[i] = tg
				break
			}
		}
	}
	type group struct {
		node int
		idx  []int
	}
	var groups []group
	pos := map[int]int{}
	for i, tg := range target {
		gi, ok := pos[tg]
		if !ok {
			gi = len(groups)
			pos[tg] = gi
			groups = append(groups, group{node: tg})
		}
		groups[gi].idx = append(groups[gi].idx, i)
	}
	out := make([]Value, n)
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	do := func(gi int) {
		g := groups[gi]
		node := cl.seed
		if g.node >= 0 && g.node != noOwner {
			node = t.nodes[g.node]
		}
		sub := getReq()
		for _, i := range g.idx {
			sub.buf = append(sub.buf, r.buf[offs[i]:offs[i+1]]...)
		}
		ok, err := node.rtMany(ctx, sub, len(g.idx))
		if !ok {
			errs[gi] = err
			return
		}
		if sub.err != nil {
			errs[gi] = sub.err
			putReq(sub)
			return
		}
		for j, i := range g.idx {
			out[i] = sub.vals[j]
		}
		putReq(sub)
	}
	for gi := 1; gi < len(groups); gi++ {
		wg.Add(1)
		go func(gi int) { defer wg.Done(); do(gi) }(gi)
	}
	do(0)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			putReq(r)
			return nil, err
		}
	}
	for i := range out {
		if out[i].Kind == KindError {
			if _, _, isMoved := parseMoved(out[i].Bytes); isMoved {
				cl.redo(ctx, t, r.buf[offs[i]:offs[i+1]], &out[i])
			}
		}
	}
	putReq(r)
	return out, nil
}
