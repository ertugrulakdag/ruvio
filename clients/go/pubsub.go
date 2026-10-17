// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"sync"
)

// Message is one Pub/Sub delivery, received from PubSub.Channel.
//
// Kind is "message" for a SUBSCRIBE channel, "pmessage" for a pattern subscription and
// "smessage" for a sharded channel (SSUBSCRIBE). Channel is the channel the message was
// published to. Pattern is the matching pattern and is set only for "pmessage". Payload
// is the message body.
type Message struct {
	// Kind is "message", "pmessage" or "smessage".
	Kind string
	// Channel is the channel the message was published to.
	Channel string
	// Pattern is the matching pattern; set only for "pmessage".
	Pattern string
	// Payload is the message body.
	Payload string
}

// PubSub is a subscription session on its own dedicated connection. Create one with
// Client.PubSub, add channels with Subscribe, PSubscribe or SSubscribe, and read
// deliveries from Channel.
//
// You must keep reading Channel: when its buffer is full the connection reader blocks,
// so a slow reader delays only this subscription but also delays subscribe
// confirmations. Channel is closed when the connection closes (Close, a network error or
// a server shutdown). There is no automatic resubscribe; open a new PubSub and subscribe
// again if that happens.
//
// All methods are safe for concurrent use. In cluster mode, shard channels are
// subscribed on the node that owns their slot (one connection per node); classic
// channels and patterns use the seed node, and a classic PUBLISH reaches every shard.
//
//	ps, err := c.PubSub(ctx, 0)
//	if err != nil { return err }
//	defer ps.Close()
//	_ = ps.Subscribe(ctx, "news")
//	for m := range ps.Channel() {
//	    fmt.Println(m.Channel, m.Payload)
//	}
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

// PubSub opens a dedicated subscription connection (it does not use the shared
// connection).
//
// buffer is the capacity of the message channel; 0 or a negative value means 1024.
// Returns an error if the connection cannot be opened or the handshake (HELLO, AUTH,
// SELECT) fails. No channels are subscribed yet.
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

// Channel returns the stream of incoming messages. It is closed when the subscription
// connection closes, so ranging over it ends cleanly. The same channel is returned on
// every call.
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

// Close closes the subscription connection(s) and, through that, the Channel. It always
// returns nil and is safe to call more than once.
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

// Subscribe sends SUBSCRIBE and waits for the server to confirm every channel. Channels
// are exact names. Messages published afterwards arrive on Channel with Kind "message".
// Subscribing to a channel twice is harmless. Returns the server error, a connection
// error or the context error.
func (ps *PubSub) Subscribe(ctx context.Context, channels ...string) error {
	return ps.roundTrip(ctx, cSUBSCRIBE, channels, len(channels), ps.chans, true)
}

// Unsubscribe sends UNSUBSCRIBE for the given channels. With no arguments it leaves
// every classic channel this PubSub has subscribed to. It waits for the confirmations.
func (ps *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	return ps.roundTrip(ctx, cUNSUBSCRIBE, channels, len(channels), ps.chans, false)
}

// PSubscribe sends PSUBSCRIBE and waits for the confirmations. Patterns use glob style:
// * matches any run of characters and ? matches one character. Matching deliveries
// arrive with Kind "pmessage" and Pattern set.
//
//	_ = ps.PSubscribe(ctx, "user:*", "log.?")
func (ps *PubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	return ps.roundTrip(ctx, cPSUBSCRIBE, patterns, len(patterns), ps.pats, true)
}

// PUnsubscribe sends PUNSUBSCRIBE for the given patterns. With no arguments it leaves
// every pattern this PubSub has subscribed to.
func (ps *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	return ps.roundTrip(ctx, cPUNSUBSCRIBE, patterns, len(patterns), ps.pats, false)
}

// SSubscribe sends SSUBSCRIBE (sharded Pub/Sub). Messages published with SPUBLISH arrive
// with Kind "smessage". In cluster mode each channel is subscribed on the node that owns
// its slot. Keyspace and keyevent notifications are delivered this way too, on channels
// named like __keyspace@<db>__:<key> and __keyevent@<db>__:<event>.
func (ps *PubSub) SSubscribe(ctx context.Context, channels ...string) error {
	if ps.cl == nil {
		return ps.roundTrip(ctx, cSSUBSCRIBE, channels, len(channels), ps.shards, true)
	}
	return ps.shardOp(ctx, cSSUBSCRIBE, channels, true)
}

// SUnsubscribe sends SUNSUBSCRIBE for the given shard channels. With no arguments it
// leaves every shard channel this PubSub has subscribed to.
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

// Ping sends PING on the subscription connection and waits for the reply. It is a cheap
// way to check that the connection is alive. Returns an error if the connection is
// closed or ctx ends first.
func (ps *PubSub) Ping(ctx context.Context) error {
	return ps.roundTrip(ctx, cPING, nil, 1, nil, false)
}
