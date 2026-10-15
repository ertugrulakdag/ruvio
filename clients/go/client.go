// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

// Package ruvio is a fast, allocation-light client for the Ruvio server (RESP2, RESP3
// via HELLO 3).
//
// One Client multiplexes every concurrent call over a single TCP connection: frames
// queued while a write is in flight are coalesced into one write, and replies are
// matched to callers in FIFO order. Blocking commands, transactions and Pub/Sub use
// dedicated connections so they never stall the shared one.
package ruvio

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures a Client.
type Options struct {
	// Addr is host:port. Default 127.0.0.1:6379.
	Addr string
	// Username and Password send AUTH on every new connection when Password is set.
	Username string
	Password string
	// DB is the logical database selected on every new connection.
	DB int
	// ClientName sends CLIENT SETNAME on every new connection.
	ClientName string
	// Protocol 3 sends HELLO 3 on connect; 0 or 2 stays on RESP2 (nothing is sent).
	Protocol int
	// DialTimeout bounds connecting and the handshake when the context has no deadline.
	DialTimeout time.Duration
	// Dial overrides the dialer (for TLS, unix sockets, tests).
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// MaxIdleDedicated is how many dedicated connections (blocking commands,
	// transactions) are kept for reuse. Default 4.
	MaxIdleDedicated int
	// OnPush receives RESP3 push frames (CLIENT TRACKING invalidations) on the
	// shared connection. It runs on the reader goroutine and must not block.
	OnPush func(Value)
	// Cluster turns on cluster routing. On first use the client asks the seed (Addr) for
	// CLUSTER SLOTS, builds the slot table and keeps one multiplexed connection per node
	// (connected lazily). A seed that answers "cluster mode is disabled" is a standalone
	// server: the client then behaves exactly as without this option.
	Cluster bool
}

// Client is safe for concurrent use. Its zero value is not usable; call NewClient.
type Client struct {
	cmds
	opts Options

	cur    atomic.Pointer[conn]
	dialMu sync.Mutex

	idleMu sync.Mutex
	idle   []*conn
	closed atomic.Bool
	db     atomic.Int64

	cl *cluster // non-nil when Options.Cluster is set
}

// NewClient returns a client. It connects lazily; call Ping to connect eagerly.
func NewClient(opts Options) *Client {
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:6379"
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.MaxIdleDedicated == 0 {
		opts.MaxIdleDedicated = 4
	}
	c := &Client{opts: opts}
	c.db.Store(int64(opts.DB))
	c.cmds.ex = c
	if opts.Cluster {
		c.cl = newCluster(c)
	}
	return c
}

// Connect is NewClient followed by PING.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	c := NewClient(opts)
	if err := c.Ping(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Close closes the shared and idle dedicated connections. Pending calls fail.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	if cn := c.cur.Swap(nil); cn != nil {
		cn.close()
	}
	if c.cl != nil {
		c.cl.close()
	}
	c.idleMu.Lock()
	for _, cn := range c.idle {
		cn.close()
	}
	c.idle = nil
	c.idleMu.Unlock()
	return nil
}

func (c *Client) shared(ctx context.Context) (*conn, error) {
	if cn := c.cur.Load(); cn != nil && !cn.dead.Load() {
		return cn, nil
	}
	c.dialMu.Lock()
	defer c.dialMu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if cn := c.cur.Load(); cn != nil && !cn.dead.Load() {
		return cn, nil
	}
	cn, err := c.dial(ctx, c.opts.OnPush)
	if err != nil {
		return nil, err
	}
	c.cur.Store(cn)
	return cn, nil
}

func (c *Client) dial(ctx context.Context, onPush func(Value)) (*conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.DialTimeout)
		defer cancel()
	}
	var nc net.Conn
	var err error
	if c.opts.Dial != nil {
		nc, err = c.opts.Dial(ctx, "tcp", c.opts.Addr)
	} else {
		var d net.Dialer
		nc, err = d.DialContext(ctx, "tcp", c.opts.Addr)
	}
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	cn := newConn(nc, onPush)
	if onPush != nil || c.opts.Protocol == 3 {
		cn.keepReading() // push frames can arrive with no call pending
	}
	if err := c.handshake(ctx, cn); err != nil {
		cn.close()
		return nil, err
	}
	return cn, nil
}

func (c *Client) handshake(ctx context.Context, cn *conn) error {
	o := c.opts
	if o.Protocol == 3 {
		n := 2
		if o.Password != "" {
			n += 3
		}
		r := newCmd(n, cHELLO).s("3")
		if o.Password != "" {
			user := o.Username
			if user == "" {
				user = "default"
			}
			r.t(tAUTH).s(user).s(o.Password)
		}
		if _, err := cn.exec(ctx, r); err != nil {
			return err
		}
	} else if o.Password != "" {
		var r *request
		if o.Username != "" {
			r = newCmd(3, cAUTH).s(o.Username).s(o.Password)
		} else {
			r = newCmd(2, cAUTH).s(o.Password)
		}
		if _, err := cn.exec(ctx, r); err != nil {
			return err
		}
	}
	if db := c.db.Load(); db != 0 {
		if _, err := cn.exec(ctx, newCmd(2, cSELECT).i(db)); err != nil {
			return err
		}
	}
	if o.ClientName != "" {
		if _, err := cn.exec(ctx, newCmd(3, cCLIENT).t(tSETNAME).s(o.ClientName)); err != nil {
			return err
		}
	}
	return nil
}

// exec implements executor: the shared connection, or the owning node in cluster mode.
func (c *Client) exec(ctx context.Context, r *request) (Value, error) {
	if c.cl != nil {
		return c.cl.exec(ctx, r)
	}
	return c.execLocal(ctx, r)
}

func (c *Client) execLocal(ctx context.Context, r *request) (Value, error) {
	cn, err := c.shared(ctx)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	return cn.exec(ctx, r)
}

func (c *Client) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	if c.cl != nil {
		return c.cl.execMany(ctx, r, n)
	}
	return c.execManyLocal(ctx, r, n)
}

// execBlocking runs a command that may block the server-side connection on a
// dedicated connection, so the shared one keeps flowing.
func (c *Client) execBlocking(ctx context.Context, r *request) (Value, error) {
	if c.cl != nil {
		return c.cl.execBlocking(ctx, r)
	}
	return c.execBlockingLocal(ctx, r)
}

func (c *Client) execBlockingLocal(ctx context.Context, r *request) (Value, error) {
	cn, err := c.acquire(ctx)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	v, err := cn.exec(ctx, r)
	if err != nil && !isServerErr(err) {
		cn.close() // the reply may still be in flight: never reuse this connection
	} else {
		c.release(cn)
	}
	return v, err
}

func isServerErr(err error) bool {
	var e *Error
	return errors.As(err, &e)
}

func (c *Client) acquire(ctx context.Context) (*conn, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	c.idleMu.Lock()
	for len(c.idle) > 0 {
		cn := c.idle[len(c.idle)-1]
		c.idle = c.idle[:len(c.idle)-1]
		if !cn.dead.Load() {
			c.idleMu.Unlock()
			return cn, nil
		}
	}
	c.idleMu.Unlock()
	return c.dial(ctx, nil)
}

func (c *Client) release(cn *conn) {
	if cn.dead.Load() || c.closed.Load() {
		cn.close()
		return
	}
	c.idleMu.Lock()
	if len(c.idle) < c.opts.MaxIdleDedicated {
		c.idle = append(c.idle, cn)
		cn = nil
	}
	c.idleMu.Unlock()
	if cn != nil {
		cn.close()
	}
}

// Do sends any command (the escape hatch) on the shared connection. Arguments may be
// string, []byte, ints, uints, floats or bool. Server errors are returned as *Error.
func (c *Client) Do(ctx context.Context, args ...any) (Value, error) {
	return c.cmds.Do(ctx, args...)
}

// Stats reports transport counters of the shared connection (writes coalesce frames).
type Stats struct {
	Writes       uint64
	BytesWritten uint64
}

// Stats returns counters for the current shared connection.
func (c *Client) Stats() Stats {
	if c.cl != nil {
		return c.cl.stats()
	}
	cn := c.cur.Load()
	if cn == nil {
		return Stats{}
	}
	return Stats{Writes: cn.writes.Load(), BytesWritten: cn.bytesWritten.Load()}
}

// Conn is a dedicated connection taken from the client. Use it for commands that
// change connection state (WATCH, CLIENT TRACKING, SELECT for one caller). Not safe
// for concurrent use by design; call Close to return it to the client.
type Conn struct {
	cmds
	c     *Client
	cn    *conn
	dirty bool

	// cluster mode: the connection is taken from the node owning the first key it sees
	// and every later key must hash to the same slot.
	cl         *cluster
	slot       int  // bound slot, -1 when not known yet
	deferMulti bool // MULTI sent before the node was known; it goes out with the first keyed command
	closed     bool
}

// Conn takes a dedicated connection.
func (c *Client) Conn(ctx context.Context) (*Conn, error) {
	if c.cl != nil {
		t, err := c.cl.topo(ctx)
		if err != nil {
			return nil, err
		}
		if t != nil {
			d := &Conn{cl: c.cl, slot: -1}
			d.cmds.ex = d
			return d, nil
		}
	}
	cn, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	d := &Conn{c: c, cn: cn}
	d.cmds.ex = d
	return d, nil
}

// Close returns the connection to the client. A connection whose database or
// protocol was changed is closed instead of being reused.
func (d *Conn) Close() error {
	d.closed = true
	if d.cn == nil {
		return nil
	}
	if d.dirty {
		d.cn.close()
	} else {
		d.c.release(d.cn)
	}
	d.cn = nil
	return nil
}

func (d *Conn) exec(ctx context.Context, r *request) (Value, error) {
	if d.cl != nil {
		if v, done, err := d.bind(ctx, r, 1); done {
			return v, err
		}
	}
	if d.cn == nil {
		putReq(r)
		return Value{}, ErrClosed
	}
	return d.cn.exec(ctx, r)
}

func (d *Conn) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	if d.cl != nil {
		if _, done, err := d.bind(ctx, r, n); done {
			return nil, err
		}
	}
	if d.cn == nil {
		putReq(r)
		return nil, ErrClosed
	}
	return d.cn.execMany(ctx, r, n)
}

func (d *Conn) execBlocking(ctx context.Context, r *request) (Value, error) { return d.exec(ctx, r) }

func (d *Conn) selected(db int64) { d.dirty = true }
func (c *Client) selected(db int64) {
	c.db.Store(db)
	c.idleMu.Lock()
	idle := c.idle
	c.idle = nil
	c.idleMu.Unlock()
	for _, cn := range idle {
		cn.close()
	}
}
