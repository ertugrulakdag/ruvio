// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"sync"
)

// Message is a Pub/Sub delivery. Kind is "message", "pmessage" or "smessage";
// Pattern is set for pmessage.
type Message struct {
	Kind    string
	Channel string
	Pattern string
	Payload string
}

// PubSub is a subscription on its own dedicated connection. Messages arrive on
// Channel(); it is closed when the connection closes. Methods are safe for
// concurrent use.
type PubSub struct {
	cl      *cluster // cluster mode: shard channels live on the node owning their slot
	cn      *conn    // the seed node (SUBSCRIBE, PSUBSCRIBE, and every channel in standalone mode)
	extra   map[string]*conn
	xmu     sync.Mutex
	msgs    chan Message
	confirm chan Value
	quit    chan struct{}
	once    sync.Once
	mu      sync.Mutex // serializes subscribe/unsubscribe round-trips
	chans   map[string]struct{}
	pats    map[string]struct{}
	shards  map[string]struct{}
	err     error
}

// PubSub opens a dedicated subscription connection. buffer sizes the message
// channel (0 = 1024); a slow reader applies backpressure to this connection only.
func (c *Client) PubSub(ctx context.Context, buffer int) (*PubSub, error) {
	if buffer <= 0 {
		buffer = 1024
	}
	cn, err := c.dial(ctx, nil)
	if err != nil {
		return nil, err
	}
	ps := &PubSub{
		cl:      c.cl,
		cn:      cn,
		msgs:    make(chan Message, buffer),
		confirm: make(chan Value, 256),
		quit:    make(chan struct{}),
		chans:   map[string]struct{}{},
		pats:    map[string]struct{}{},
		shards:  map[string]struct{}{},
	}
	cn.sink.Store(&sinkFuncs{value: ps.onValue, fail: ps.onFail})
	cn.keepReading()
	return ps, nil
}

// Channel returns the message stream.
func (ps *PubSub) Channel() <-chan Message { return ps.msgs }

func (ps *PubSub) onValue(v Value) {
	if !v.isAgg() || len(v.Array) < 2 { // +PONG before subscribing, or an error
		select {
		case ps.confirm <- v:
		case <-ps.quit:
		}
		return
	}
	kind := v.Array[0].Str()
	var m Message
	switch kind {
	case "message", "smessage":
		if len(v.Array) != 3 {
			return
		}
		m = Message{Kind: kind, Channel: v.Array[1].Str(), Payload: v.Array[2].Str()}
	case "pmessage":
		if len(v.Array) != 4 {
			return
		}
		m = Message{Kind: kind, Pattern: v.Array[1].Str(), Channel: v.Array[2].Str(), Payload: v.Array[3].Str()}
	default: // subscribe/unsubscribe confirmations and pong
		select {
		case ps.confirm <- v:
		case <-ps.quit:
		}
		return
	}
	select {
	case ps.msgs <- m:
	case <-ps.quit:
	}
}

func (ps *PubSub) onFail(err error) {
	ps.once.Do(func() {
		ps.err = err
		close(ps.quit)
		close(ps.msgs)
	})
}

// Close closes the subscription connection and the message channel.
func (ps *PubSub) Close() error {
	ps.cn.close()
	ps.xmu.Lock()
	for _, cn := range ps.extra {
		cn.close()
	}
	ps.xmu.Unlock()
	return nil
}

func (ps *PubSub) roundTrip(ctx context.Context, name []byte, args []string, expect int, set map[string]struct{}, add bool) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.roundTripOn(ctx, ps.cn, name, args, expect, set, add)
}

// roundTripOn is roundTrip on one connection; the caller holds ps.mu.
func (ps *PubSub) roundTripOn(ctx context.Context, cn *conn, name []byte, args []string, expect int, set map[string]struct{}, add bool) error {
	r := newCmd(1+len(args), name).ss(args)
	err := cn.sendRaw(r.buf)
	putReq(r)
	if err != nil {
		return err
	}
	if expect == 0 { // unsubscribe-all: one reply per current entry, at least one
		expect = len(set)
		if expect == 0 {
			expect = 1
		}
	}
	for i := 0; i < expect; i++ {
		select {
		case v := <-ps.confirm:
			if v.Kind == KindError {
				return &Error{Msg: b2s(v.Bytes)}
			}
			if len(v.Array) >= 2 && v.Array[1].Kind != KindNil {
				if add {
					set[v.Array[1].Str()] = struct{}{}
				} else {
					delete(set, v.Array[1].Str())
				}
			}
		case <-ps.quit:
			if ps.err != nil {
				return ps.err
			}
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Subscribe subscribes to exact channels and waits for the confirmations.
func (ps *PubSub) Subscribe(ctx context.Context, channels ...string) error {
	return ps.roundTrip(ctx, cSUBSCRIBE, channels, len(channels), ps.chans, true)
}

// Unsubscribe drops channels (none = all exact channels).
func (ps *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	return ps.roundTrip(ctx, cUNSUBSCRIBE, channels, len(channels), ps.chans, false)
}

// PSubscribe subscribes to glob patterns (* and ?).
func (ps *PubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	return ps.roundTrip(ctx, cPSUBSCRIBE, patterns, len(patterns), ps.pats, true)
}

// PUnsubscribe drops patterns (none = all).
func (ps *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	return ps.roundTrip(ctx, cPUNSUBSCRIBE, patterns, len(patterns), ps.pats, false)
}

// SSubscribe subscribes to shard channels. In cluster mode each channel is subscribed
// on the node owning its slot (one extra connection per node, opened on demand); a lost
// node connection closes the whole subscription.
func (ps *PubSub) SSubscribe(ctx context.Context, channels ...string) error {
	if ps.cl == nil {
		return ps.roundTrip(ctx, cSSUBSCRIBE, channels, len(channels), ps.shards, true)
	}
	return ps.shardOp(ctx, cSSUBSCRIBE, channels, true)
}

// SUnsubscribe drops shard channels (none = all).
func (ps *PubSub) SUnsubscribe(ctx context.Context, channels ...string) error {
	if ps.cl == nil {
		return ps.roundTrip(ctx, cSUNSUBSCRIBE, channels, len(channels), ps.shards, false)
	}
	return ps.shardOp(ctx, cSUNSUBSCRIBE, channels, false)
}

func (ps *PubSub) shardOp(ctx context.Context, name []byte, channels []string, add bool) error {
	t, err := ps.cl.topo(ctx)
	if err != nil {
		return err
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if t == nil {
		return ps.roundTripOn(ctx, ps.cn, name, channels, len(channels), ps.shards, add)
	}
	if len(channels) == 0 && !add { // unsubscribe from every shard channel we know
		for c := range ps.shards {
			channels = append(channels, c)
		}
		if len(channels) == 0 {
			return nil
		}
	}
	// one command per slot: the server refuses channels of different slots in one call
	var order []int
	bySlot := map[int][]string{}
	for _, c := range channels {
		s := KeySlot(c)
		if _, ok := bySlot[s]; !ok {
			order = append(order, s)
		}
		bySlot[s] = append(bySlot[s], c)
	}
	for _, s := range order {
		cn, err := ps.connFor(ctx, t.nodeAt(s, ps.cl.seed))
		if err != nil {
			return err
		}
		chs := bySlot[s]
		if err := ps.roundTripOn(ctx, cn, name, chs, len(chs), ps.shards, add); err != nil {
			return err
		}
	}
	return nil
}

// connFor returns the subscription connection to node, opening it on first use.
func (ps *PubSub) connFor(ctx context.Context, node *Client) (*conn, error) {
	if node == ps.cl.seed {
		return ps.cn, nil
	}
	ps.xmu.Lock()
	defer ps.xmu.Unlock()
	if cn := ps.extra[node.opts.Addr]; cn != nil && !cn.dead.Load() {
		return cn, nil
	}
	cn, err := node.dial(ctx, nil)
	if err != nil {
		return nil, err
	}
	cn.sink.Store(&sinkFuncs{value: ps.onValue, fail: ps.onFail})
	cn.keepReading()
	if ps.extra == nil {
		ps.extra = map[string]*conn{}
	}
	ps.extra[node.opts.Addr] = cn
	return cn, nil
}

// Ping checks the subscription connection.
func (ps *PubSub) Ping(ctx context.Context) error {
	return ps.roundTrip(ctx, cPING, nil, 1, nil, false)
}
