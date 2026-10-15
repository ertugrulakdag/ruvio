// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrCrossSlot is returned (without sending anything) for a multi-key command, pipeline
// or transaction whose keys do not share one hash slot and which the client cannot split.
var ErrCrossSlot = errors.New("ruvio: CROSSSLOT keys in request don't hash to the same slot")

const (
	noOwner   = 0xFFFF
	slotCross = -2 // routeFrame: the keys of one command span several slots
)

// topology is an immutable slot table; updates copy it.
type topology struct {
	owner [slotCount]uint16 // index into nodes, noOwner when the slot is unassigned
	nodes []*Client
	addrs []string
}

func (t *topology) nodeAt(slot int, seed *Client) *Client {
	if i := t.owner[slot]; i != noOwner {
		return t.nodes[i]
	}
	return seed
}

func (t *topology) indexOf(addr string) int {
	for i, a := range t.addrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// cluster routes commands over one multiplexed Client per node. The seed Client (the one
// the caller created) doubles as the node it points at; the other nodes are internal
// Clients with cluster mode off, so every per-node fast path is the standalone one.
type cluster struct {
	seed       *Client
	t          atomic.Pointer[topology]
	standalone atomic.Bool // the seed answered "cluster mode is disabled"
	moved      atomic.Int32

	mu    sync.Mutex // guards nodes, refresh and slot patches
	nodes map[string]*Client
}

func newCluster(seed *Client) *cluster {
	return &cluster{seed: seed, nodes: map[string]*Client{seed.opts.Addr: seed}}
}

// node returns the Client for addr, creating it (no connection yet). Caller holds cl.mu.
func (cl *cluster) node(addr string) *Client {
	if n := cl.nodes[addr]; n != nil {
		return n
	}
	o := cl.seed.opts
	o.Addr = addr
	o.Cluster = false
	n := NewClient(o)
	cl.nodes[addr] = n
	return n
}

func (cl *cluster) close() {
	cl.mu.Lock()
	nodes := make([]*Client, 0, len(cl.nodes))
	for _, n := range cl.nodes {
		if n != cl.seed {
			nodes = append(nodes, n)
		}
	}
	cl.mu.Unlock()
	for _, n := range nodes {
		n.Close()
	}
}

// topo returns the slot table, discovering it on first use. A nil table means the seed
// is a standalone server: callers fall back to the plain single-connection path.
func (cl *cluster) topo(ctx context.Context) (*topology, error) {
	if t := cl.t.Load(); t != nil {
		return t, nil
	}
	if cl.standalone.Load() {
		return nil, nil
	}
	return cl.refresh(ctx, true)
}

func isClusterDisabled(err error) bool {
	var e *Error
	return errors.As(err, &e) && (strings.Contains(e.Msg, "cluster mode is disabled") ||
		strings.Contains(e.Msg, "unknown command") || strings.Contains(e.Msg, "unknown subcommand"))
}

// refresh reloads the slot table with CLUSTER SLOTS. first is the discovery call: a
// "cluster mode is disabled" reply switches the client to standalone for good.
func (cl *cluster) refresh(ctx context.Context, first bool) (*topology, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if first {
		if t := cl.t.Load(); t != nil {
			return t, nil
		}
		if cl.standalone.Load() {
			return nil, nil
		}
	}
	candidates := []*Client{cl.seed}
	if t := cl.t.Load(); t != nil {
		for _, n := range t.nodes {
			if n != cl.seed {
				candidates = append(candidates, n)
			}
		}
	}
	var lastErr error
	for _, n := range candidates {
		v, err := n.execLocal(ctx, newCmd(2, cCLUSTER).t(tSLOTS))
		if err != nil {
			if first && isClusterDisabled(err) {
				cl.standalone.Store(true)
				return nil, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				break
			}
			continue
		}
		t, err := cl.build(v)
		if err != nil {
			return nil, err
		}
		cl.t.Store(t)
		cl.moved.Store(0)
		return t, nil
	}
	return nil, lastErr
}

// build turns a CLUSTER SLOTS reply into a topology. Caller holds cl.mu.
func (cl *cluster) build(v Value) (*topology, error) {
	if v.Kind != KindArray || len(v.Array) == 0 {
		return nil, fmt.Errorf("ruvio: invalid CLUSTER SLOTS reply: %s", v.String())
	}
	ranges := append([]Value(nil), v.Array...)
	sort.SliceStable(ranges, func(i, j int) bool {
		a, _ := ranges[i].Array[0].AsInt64()
		b, _ := ranges[j].Array[0].AsInt64()
		return a < b
	})
	seedHost, _, _ := net.SplitHostPort(cl.seed.opts.Addr)
	t := &topology{}
	for i := range t.owner {
		t.owner[i] = noOwner
	}
	for _, rg := range ranges {
		if rg.Kind != KindArray || len(rg.Array) < 3 || rg.Array[2].Kind != KindArray || len(rg.Array[2].Array) < 2 {
			return nil, fmt.Errorf("ruvio: invalid CLUSTER SLOTS range: %s", rg.String())
		}
		lo, e1 := rg.Array[0].AsInt64()
		hi, e2 := rg.Array[1].AsInt64()
		port, e3 := rg.Array[2].Array[1].AsInt64()
		if e1 != nil || e2 != nil || e3 != nil || lo < 0 || hi < lo || hi >= slotCount {
			return nil, fmt.Errorf("ruvio: invalid CLUSTER SLOTS range: %s", rg.String())
		}
		host := rg.Array[2].Array[0].Str()
		if host == "" {
			host = seedHost
		}
		addr := net.JoinHostPort(host, strconv.FormatInt(port, 10))
		idx := t.indexOf(addr)
		if idx < 0 {
			idx = len(t.addrs)
			t.addrs = append(t.addrs, addr)
			t.nodes = append(t.nodes, cl.node(addr))
		}
		for s := lo; s <= hi; s++ {
			t.owner[s] = uint16(idx)
		}
	}
	return t, nil
}

// parseMoved decodes "MOVED <slot> host:port".
func parseMoved(msg []byte) (slot int, addr string, ok bool) {
	if len(msg) < 8 || string(msg[:6]) != "MOVED " {
		return 0, "", false
	}
	rest := msg[6:]
	sp := -1
	for i, c := range rest {
		if c == ' ' {
			sp = i
			break
		}
	}
	if sp <= 0 {
		return 0, "", false
	}
	n, ok := atoiBytes(rest[:sp])
	if !ok || n >= slotCount || sp+1 >= len(rest) {
		return 0, "", false
	}
	return n, string(rest[sp+1:]), true
}

// applyMoved updates the table after -MOVED and returns the node to retry on. The slot
// is patched at once; the table is reloaded when the target is unknown or when more than
// one slot has moved since the last reload.
func (cl *cluster) applyMoved(ctx context.Context, slot int, addr string) (*Client, error) {
	cl.mu.Lock()
	t := cl.t.Load()
	n := cl.node(addr)
	idx := -1
	if t != nil {
		idx = t.indexOf(addr)
	}
	if idx >= 0 {
		nt := *t
		nt.owner[slot] = uint16(idx)
		cl.t.Store(&nt)
	}
	cl.mu.Unlock()
	if idx < 0 || cl.moved.Add(1) > 1 {
		if nt, err := cl.refresh(ctx, false); err == nil && nt != nil {
			return nt.nodeAt(slot, n), nil
		} else if err != nil && idx < 0 {
			return nil, err
		}
	}
	return n, nil
}

// rt sends r on c and waits. ok=false: r was handed back to the reader (or recycled); the
// caller must not touch it. ok=true: r is the caller's, its reply is in r.val / r.err.
func (c *Client) rt(ctx context.Context, r *request, blocking bool) (bool, error) {
	if !blocking {
		cn, err := c.shared(ctx)
		if err != nil {
			putReq(r)
			return false, err
		}
		return cn.roundTrip(ctx, r)
	}
	cn, err := c.acquire(ctx)
	if err != nil {
		putReq(r)
		return false, err
	}
	ok, err := cn.roundTrip(ctx, r)
	if !ok || r.err != nil {
		cn.close() // a reply may still be in flight: never reuse this connection
	} else {
		c.release(cn)
	}
	return ok, err
}

// stats sums the counters of every node connection.
func (cl *cluster) stats() Stats {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	var s Stats
	for _, n := range cl.nodes {
		if cn := n.cur.Load(); cn != nil {
			s.Writes += cn.writes.Load()
			s.BytesWritten += cn.bytesWritten.Load()
		}
	}
	return s
}

// Nodes returns the addresses of the cluster nodes, in slot order (nil for a standalone
// server or before the first command).
func (c *Client) Nodes() []string {
	if c.cl == nil {
		return nil
	}
	if t := c.cl.t.Load(); t != nil {
		return append([]string(nil), t.addrs...)
	}
	return nil
}

// IsCluster reports whether the client discovered a cluster (false for a standalone
// server, or before the first command).
func (c *Client) IsCluster() bool { return c.cl != nil && c.cl.t.Load() != nil }

// RefreshSlots reloads the slot table now (CLUSTER SLOTS).
func (c *Client) RefreshSlots(ctx context.Context) error {
	if c.cl == nil {
		return nil
	}
	_, err := c.cl.refresh(ctx, c.cl.t.Load() == nil)
	return err
}
