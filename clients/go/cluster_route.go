// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

func (cl *cluster) exec(ctx context.Context, r *request) (Value, error) {
	return cl.route(ctx, r, false)
}

func (cl *cluster) execBlocking(ctx context.Context, r *request) (Value, error) {
	return cl.route(ctx, r, true)
}

func (cl *cluster) route(ctx context.Context, r *request, blocking bool) (Value, error) {
	t, err := cl.topo(ctx)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	if t == nil {
		if blocking {
			return cl.seed.execBlockingLocal(ctx, r)
		}
		return cl.seed.execLocal(ctx, r)
	}
	spec, slot, _, err := routeFrame(r.buf)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	switch {
	case spec.scan:
		return cl.scan(ctx, t, r)
	case spec.fan != fanNone && fansOut(spec, r.buf):
		return cl.fanOut(ctx, t, spec, r)
	case slot == slotCross:
		if spec.split {
			return cl.split(ctx, t, spec, r)
		}
		err := crossSlotErr(r.buf)
		putReq(r)
		return Value{}, err
	}
	return cl.run(ctx, t, slot, r, blocking)
}

func crossSlotErr(buf []byte) error {
	var st [2][]byte
	if a, _, _, ok := frameArgs(buf, 1, st[:0]); ok && len(a) == 1 {
		return fmt.Errorf("%w (%s)", ErrCrossSlot, strings.ToUpper(string(a[0])))
	}
	return ErrCrossSlot
}

// run sends r to the node owning slot (the seed for slot < 0) and retries once on -MOVED.
// It recycles r.
func (cl *cluster) run(ctx context.Context, t *topology, slot int, r *request, blocking bool) (Value, error) {
	node := cl.seed
	if slot >= 0 {
		node = t.nodeAt(slot, cl.seed)
	}
	for attempt := 0; ; attempt++ {
		ok, err := node.rt(ctx, r, blocking)
		if !ok {
			return Value{}, err
		}
		if r.err != nil {
			err := r.err
			putReq(r)
			return Value{}, err
		}
		v := r.val
		if v.Kind == KindError {
			if attempt == 0 {
				if ms, addr, isMoved := parseMoved(v.Bytes); isMoved {
					nn, err := cl.applyMoved(ctx, ms, addr)
					if err != nil {
						putReq(r)
						return Value{}, err
					}
					node = nn
					r.val, r.vals, r.err = Value{}, nil, nil // the command was not executed: send it again
					r.state.Store(stateWaiting)
					continue
				}
			}
			putReq(r)
			return v, &Error{Msg: b2s(v.Bytes)}
		}
		putReq(r)
		return v, nil
	}
}

// routeFrame reads the command at the start of b and reports how to route it. slot is the
// hash slot of its keys, -1 for a keyless command, or slotCross when the keys of one
// command span several slots.
func routeFrame(b []byte) (spec cmdSpec, slot, nargs int, err error) {
	var stack [2][]byte
	args, n, _, ok := frameArgs(b, 2, stack[:0])
	if !ok || n == 0 {
		return spec, -1, 0, ErrProtocol
	}
	spec, _ = specOf(args[0])
	switch r := spec.keys; {
	case r.kind == ruleNone:
		return spec, -1, n, nil
	case r.kind == ruleRange && r.a == 1 && r.b == 1: // the key is argument 1 (the common case)
		if n < 2 {
			return spec, -1, n, nil
		}
		return spec, hashSlot(args[1]), n, nil
	}
	var all [16][]byte
	full, _, _, ok := frameArgs(b, 0, all[:0])
	if !ok {
		return spec, -1, n, ErrProtocol
	}
	var ks [16][]byte
	keys := keysOf(spec.keys, full, ks[:0])
	if len(keys) == 0 {
		return spec, -1, n, nil
	}
	slot = hashSlot(keys[0])
	for _, k := range keys[1:] {
		if hashSlot(k) != slot {
			return spec, slotCross, n, nil
		}
	}
	return spec, slot, n, nil
}

// fansOut reports whether the command really goes to every node (SCRIPT/FUNCTION only for
// their state-changing subcommands).
func fansOut(spec cmdSpec, buf []byte) bool {
	if spec.fan != fanSub {
		return true
	}
	var st [2][]byte
	args, _, _, ok := frameArgs(buf, 2, st[:0])
	if !ok || len(args) < 2 {
		return false
	}
	s := args[1]
	return upperEq(s, "LOAD") || upperEq(s, "FLUSH") || upperEq(s, "DELETE") || upperEq(s, "RESTORE")
}

// ---- multi-key split ---------------------------------------------------------------------

type slotPart struct {
	slot int
	pos  []int // key (or pair) positions in the caller's order
}

// split runs MGET, MSET, DEL, UNLINK, EXISTS and TOUCH whose keys span slots: one command
// per slot, all concurrent, replies re-assembled in the caller's order. Parts are not
// atomic with respect to each other.
func (cl *cluster) split(ctx context.Context, t *topology, spec cmdSpec, r *request) (Value, error) {
	args, _, _, ok := frameArgs(r.buf, 0, nil)
	if !ok {
		putReq(r)
		return Value{}, ErrProtocol
	}
	name := args[0]
	pairs := spec.keys.c == 2 // MSET: key value key value ...
	var keys [][]byte
	if pairs {
		for i := 1; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
	} else {
		keys = args[1:]
	}
	index := make(map[int]int, 4)
	var parts []slotPart
	for i, k := range keys {
		s := hashSlot(k)
		pi, seen := index[s]
		if !seen {
			pi = len(parts)
			index[s] = pi
			parts = append(parts, slotPart{slot: s})
		}
		parts[pi].pos = append(parts[pi].pos, i)
	}
	replies := make([]Value, len(parts))
	errs := make([]error, len(parts))
	isMGet, nkeys := upperEq(name, "MGET"), len(keys)
	canonical := preEncBytes(name)
	per := 1
	if pairs {
		per = 2
	}
	appendPart := func(dst *request, p slotPart) {
		dst.buf = appendArrayHeader(dst.buf, 1+per*len(p.pos))
		dst.buf = append(dst.buf, canonical...)
		for _, i := range p.pos {
			if pairs {
				dst.b(args[1+2*i]).b(args[2+2*i])
			} else {
				dst.b(keys[i])
			}
		}
	}
	// One pipelined write per node (the server wants a single slot per command, so a node
	// gets one command per slot it owns); the nodes run concurrently.
	type nodeGroup struct{ parts []int }
	var order []int
	groups := map[int]*nodeGroup{}
	for pi, p := range parts {
		ni := int(t.owner[p.slot])
		g := groups[ni]
		if g == nil {
			g = &nodeGroup{}
			groups[ni] = g
			order = append(order, ni)
		}
		g.parts = append(g.parts, pi)
	}
	var wg sync.WaitGroup
	do := func(ni int) {
		g := groups[ni]
		node := cl.seed
		if ni != noOwner {
			node = t.nodes[ni]
		}
		sub := getReq()
		for _, pi := range g.parts {
			appendPart(sub, parts[pi])
		}
		ok, err := node.rtMany(ctx, sub, len(g.parts))
		if !ok || sub.err != nil {
			if ok {
				err = sub.err
				putReq(sub)
			}
			for _, pi := range g.parts {
				errs[pi] = err
			}
			return
		}
		for j, pi := range g.parts {
			v := sub.vals[j]
			if v.Kind == KindError {
				if _, _, isMoved := parseMoved(v.Bytes); isMoved { // repair this part alone
					again := getReq()
					appendPart(again, parts[pi])
					v, errs[pi] = cl.run(ctx, t, parts[pi].slot, again, false)
					replies[pi] = v
					continue
				}
				errs[pi] = &Error{Msg: b2s(v.Bytes)}
				continue
			}
			replies[pi] = v
		}
		putReq(sub)
	}
	for _, ni := range order[1:] {
		wg.Add(1)
		go func(ni int) { defer wg.Done(); do(ni) }(ni)
	}
	do(order[0])
	wg.Wait()
	putReq(r)
	for _, err := range errs {
		if err != nil {
			return Value{}, err
		}
	}
	switch {
	case isMGet:
		out := make([]Value, nkeys)
		for pi, p := range parts {
			if replies[pi].Kind != KindArray || len(replies[pi].Array) != len(p.pos) {
				return Value{}, unexpected(replies[pi])
			}
			for j, i := range p.pos {
				out[i] = replies[pi].Array[j]
			}
		}
		return Value{Kind: KindArray, Array: out}, nil
	case pairs:
		return Value{Kind: KindSimple, Bytes: []byte("OK")}, nil
	}
	var sum int64
	for _, v := range replies {
		n, err := v.AsInt64()
		if err != nil {
			return Value{}, err
		}
		sum += n
	}
	return Value{Kind: KindInt, Int: sum}, nil
}

func preEncBytes(name []byte) []byte {
	// name is an argument; upper-case it so the frame is canonical.
	return preEnc(strings.ToUpper(string(name)))
}

// ---- fan-out (DBSIZE, FLUSHDB, KEYS, SCRIPT LOAD ...) ------------------------------------------

func (cl *cluster) fanOut(ctx context.Context, t *topology, spec cmdSpec, r *request) (Value, error) {
	nodes := t.nodes
	replies := make([]Value, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	do := func(i int) {
		sub := getReq()
		sub.buf = append(sub.buf, r.buf...)
		replies[i], errs[i] = nodes[i].execLocal(ctx, sub)
	}
	for i := 1; i < len(nodes); i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); do(i) }(i)
	}
	do(0)
	wg.Wait()
	putReq(r)
	for _, err := range errs {
		if err != nil {
			return Value{}, err
		}
	}
	switch spec.fan {
	case fanSum:
		var sum int64
		for _, v := range replies {
			n, err := v.AsInt64()
			if err != nil {
				return Value{}, err
			}
			sum += n
		}
		return Value{Kind: KindInt, Int: sum}, nil
	case fanConcat:
		var out []Value
		for _, v := range replies {
			if v.Kind != KindArray {
				return Value{}, unexpected(v)
			}
			out = append(out, v.Array...)
		}
		return Value{Kind: KindArray, Array: out}, nil
	}
	return replies[0], nil
}

// ---- SCAN across nodes -----------------------------------------------------------------------------

// scan walks the nodes one after the other. The cursor handed to the caller encodes the
// node: client = server*nodes + nodeIndex; "0" ends the walk after the last node.
func (cl *cluster) scan(ctx context.Context, t *topology, r *request) (Value, error) {
	args, _, _, ok := frameArgs(r.buf, 0, nil)
	if !ok || len(args) < 2 {
		putReq(r)
		return Value{}, &Error{Msg: "ERR wrong number of arguments for 'scan' command"}
	}
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		putReq(r)
		return Value{}, &Error{Msg: "ERR invalid cursor"}
	}
	nn := uint64(len(t.nodes))
	node := int(cursor % nn)
	sub := getReq()
	sub.buf = appendArrayHeader(sub.buf, len(args))
	sub.buf = append(sub.buf, cSCAN...)
	sub.u(cursor / nn)
	for _, a := range args[2:] {
		sub.b(a)
	}
	putReq(r)
	v, err := t.nodes[node].execLocal(ctx, sub)
	if err != nil {
		return v, err
	}
	if v.Kind != KindArray || len(v.Array) < 2 {
		return Value{}, unexpected(v)
	}
	next, err := v.Array[0].AsInt64()
	if err != nil {
		return Value{}, err
	}
	var out uint64
	switch {
	case next != 0:
		if uint64(next) > (1<<63-1)/nn-1 {
			return Value{}, errors.New("ruvio: scan cursor overflow")
		}
		out = uint64(next)*nn + uint64(node)
	case uint64(node)+1 < nn:
		out = uint64(node) + 1
	}
	return Value{Kind: KindArray, Array: []Value{
		{Kind: KindBulk, Bytes: strconv.AppendUint(nil, out, 10)}, v.Array[1],
	}}, nil
}
