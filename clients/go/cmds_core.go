// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

type executor interface {
	exec(ctx context.Context, r *request) (Value, error)
	execMany(ctx context.Context, r *request, n int) ([]Value, error)
	execBlocking(ctx context.Context, r *request) (Value, error)
	selected(db int64)
}

// cmds holds the typed command methods shared by Client and Conn.
type cmds struct{ ex executor }

// NullString is a nullable bulk string element (MGET, HMGET, ...).
type NullString struct {
	Val   string
	Valid bool
}

// NullFloat is a nullable score (ZMSCORE).
type NullFloat struct {
	Val   float64
	Valid bool
}

// Do sends any command. Arguments may be string, []byte, ints, uints, floats or bool.
func (c *cmds) Do(ctx context.Context, args ...any) (Value, error) {
	r := getReq()
	var err error
	if r.buf, err = AppendCommand(r.buf, args...); err != nil {
		putReq(r)
		return Value{}, err
	}
	return c.ex.exec(ctx, r)
}

// DoBytes is Do for raw binary arguments.
func (c *cmds) DoBytes(ctx context.Context, args ...[]byte) (Value, error) {
	r := getReq()
	r.buf = appendArrayHeader(r.buf, len(args))
	for _, a := range args {
		r.b(a)
	}
	return c.ex.exec(ctx, r)
}

// ---- reply helpers -------------------------------------------------------

func (c *cmds) value(ctx context.Context, r *request) (Value, error) { return c.ex.exec(ctx, r) }

func (c *cmds) ok(ctx context.Context, r *request) error {
	_, err := c.ex.exec(ctx, r)
	return err
}

func (c *cmds) intR(ctx context.Context, r *request) (int64, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return 0, err
	}
	if v.Kind == KindInt {
		return v.Int, nil
	}
	return v.AsInt64()
}

func (c *cmds) intNil(ctx context.Context, r *request) (int64, bool, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil || v.Kind == KindNil {
		return 0, false, err
	}
	if v.Kind == KindInt {
		return v.Int, true, nil
	}
	n, err := v.AsInt64()
	return n, err == nil, err
}

func (c *cmds) boolR(ctx context.Context, r *request) (bool, error) {
	n, err := c.intR(ctx, r)
	return n == 1, err
}

func (c *cmds) strR(ctx context.Context, r *request) (string, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return "", err
	}
	return v.Str(), nil
}

func (c *cmds) strNil(ctx context.Context, r *request) (string, bool, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil || v.Kind == KindNil {
		return "", false, err
	}
	return v.Str(), true, nil
}

func (c *cmds) bytesNil(ctx context.Context, r *request) ([]byte, bool, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil || v.Kind == KindNil {
		return nil, false, err
	}
	return v.Bytes, true, nil
}

func (c *cmds) floatR(ctx context.Context, r *request) (float64, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return 0, err
	}
	return v.AsFloat64()
}

func (c *cmds) floatNil(ctx context.Context, r *request) (float64, bool, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil || v.Kind == KindNil {
		return 0, false, err
	}
	f, err := v.AsFloat64()
	return f, err == nil, err
}

func (c *cmds) strs(ctx context.Context, r *request) ([]string, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return nil, err
	}
	return v.Strings()
}

func (c *cmds) nullStrs(ctx context.Context, r *request) ([]NullString, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return nil, err
	}
	return toNullStrings(v)
}

func toNullStrings(v Value) ([]NullString, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]NullString, len(v.Array))
	for i, e := range v.Array {
		if e.Kind != KindNil {
			out[i] = NullString{Val: e.Str(), Valid: true}
		}
	}
	return out, nil
}

func (c *cmds) ints(ctx context.Context, r *request) ([]int64, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return nil, err
	}
	return toInts(v)
}

func toInts(v Value) ([]int64, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]int64, len(v.Array))
	for i, e := range v.Array {
		if e.Kind == KindNil {
			continue
		}
		n, err := e.AsInt64()
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

func (c *cmds) bools(ctx context.Context, r *request) ([]bool, error) {
	n, err := c.ints(ctx, r)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(n))
	for i, x := range n {
		out[i] = x == 1
	}
	return out, nil
}

// pairs converts a flat [k, v, k, v] array (or a RESP3 map) to a map.
func (c *cmds) strMap(ctx context.Context, r *request) (map[string]string, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return nil, err
	}
	return toStrMap(v)
}

func toStrMap(v Value) (map[string]string, error) {
	if v.Kind == KindNil {
		return map[string]string{}, nil
	}
	if !v.isAgg() || len(v.Array)%2 != 0 {
		return nil, unexpected(v)
	}
	m := make(map[string]string, len(v.Array)/2)
	for i := 0; i+1 < len(v.Array); i += 2 {
		m[v.Array[i].Str()] = v.Array[i+1].Str()
	}
	return m, nil
}

// Z is a sorted-set member with its score.
type Z struct {
	Member string
	Score  float64
}

func toZs(v Value) ([]Z, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	// RESP3 may nest [member, score] pairs.
	if len(v.Array) > 0 && v.Array[0].isAgg() {
		out := make([]Z, len(v.Array))
		for i, p := range v.Array {
			if len(p.Array) != 2 {
				return nil, unexpected(v)
			}
			f, err := p.Array[1].AsFloat64()
			if err != nil {
				return nil, err
			}
			out[i] = Z{p.Array[0].Str(), f}
		}
		return out, nil
	}
	if len(v.Array)%2 != 0 {
		return nil, unexpected(v)
	}
	out := make([]Z, len(v.Array)/2)
	for i := range out {
		f, err := v.Array[2*i+1].AsFloat64()
		if err != nil {
			return nil, err
		}
		out[i] = Z{v.Array[2*i].Str(), f}
	}
	return out, nil
}

func (c *cmds) zs(ctx context.Context, r *request) ([]Z, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return nil, err
	}
	return toZs(v)
}

func ms(d time.Duration) int64 {
	m := d.Milliseconds()
	if m == 0 && d > 0 {
		m = 1
	}
	return m
}

// secs converts a timeout to the decimal-seconds argument used by blocking commands.
func secs(d time.Duration) float64 { return d.Seconds() }
