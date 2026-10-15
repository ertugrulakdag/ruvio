// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrIdemPending means another owner is still running the operation.
var ErrIdemPending = errors.New("ruvio: idempotent operation pending")

// ErrIdemConflict means the key was used with a different fingerprint.
var ErrIdemConflict = errors.New("ruvio: idempotency key reused with a different fingerprint")

// ---- cache-aside (FILLGET / FILLSET) ----------------------------------------------

// GetOrCreateOptions tune GetOrCreate. Zero values pick defaults.
type GetOrCreateOptions struct {
	// FillTimeout is how long the fill marker protects a filler (default 5s).
	FillTimeout time.Duration
	// Stale keeps the value readable this long after ttl (SWR); stale readers get the
	// old value and one of them refreshes in the background.
	Stale time.Duration
	// PollMin and PollMax bound the BUSY backoff (default 2ms .. 50ms).
	PollMin, PollMax time.Duration
}

// GetOrCreate returns the cached value or computes it once across all callers
// (stampede protection). While another caller fills, it polls plain FILLGET with a
// short backoff: FILLGET ... WAIT would park the shared connection.
func (c *cmds) GetOrCreate(ctx context.Context, key string, ttl time.Duration, factory func(ctx context.Context) (string, error), o ...GetOrCreateOptions) (string, error) {
	var opt GetOrCreateOptions
	if len(o) > 0 {
		opt = o[0]
	}
	if opt.FillTimeout <= 0 {
		opt.FillTimeout = 5 * time.Second
	}
	if opt.PollMin <= 0 {
		opt.PollMin = 2 * time.Millisecond
	}
	if opt.PollMax <= 0 {
		opt.PollMax = 50 * time.Millisecond
	}
	wait := opt.PollMin
	for {
		r, err := c.FillGet(ctx, key, opt.FillTimeout)
		if err != nil {
			return "", err
		}
		switch r.State {
		case FillHit:
			return r.Value, nil
		case FillStale:
			if r.Token != "" {
				bg := context.WithoutCancel(ctx)
				go func() {
					if v, err := factory(bg); err == nil {
						c.FillSet(bg, key, v, ttl, r.Token, opt.Stale)
					}
				}()
			}
			return r.Value, nil
		case FillFill:
			v, err := factory(ctx)
			if err != nil {
				return "", err // the marker lapses after FillTimeout
			}
			if _, err := c.FillSet(ctx, key, v, ttl, r.Token, opt.Stale); err != nil {
				return v, err
			}
			return v, nil
		}
		if err := sleep(ctx, wait); err != nil {
			return "", err
		}
		if wait *= 2; wait > opt.PollMax {
			wait = opt.PollMax
		}
	}
}

// GetOrCreateJSON is GetOrCreate for values encoded with encoding/json.
func GetOrCreateJSON[T any](ctx context.Context, c *Client, key string, ttl time.Duration, factory func(ctx context.Context) (T, error), o ...GetOrCreateOptions) (T, error) {
	var out T
	s, err := c.GetOrCreate(ctx, key, ttl, func(ctx context.Context) (string, error) {
		v, err := factory(ctx)
		if err != nil {
			return "", err
		}
		b, err := json.Marshal(v)
		return string(b), err
	}, o...)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal([]byte(s), &out)
	return out, err
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Idempotent runs fn at most once per (key, fingerprint) within ttl and returns the
// stored result to later callers. It returns ErrIdemPending while another caller runs
// fn and ErrIdemConflict for a different fingerprint. Not exactly-once for external
// effects: if fn succeeds and COMPLETE fails, a retry after ttl runs fn again.
func (c *cmds) Idempotent(ctx context.Context, key, fingerprint string, ttl time.Duration, fn func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	owner := randomToken()
	res, err := c.IdemBegin(ctx, key, fingerprint, owner, ttl)
	if err != nil {
		return nil, err
	}
	switch res.State {
	case IdemCompleted:
		return res.Result, nil
	case IdemPending:
		return nil, ErrIdemPending
	case IdemConflict:
		return nil, ErrIdemConflict
	}
	out, err := fn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := c.IdemComplete(ctx, key, fingerprint, owner, out); err != nil {
		return out, err
	}
	return out, nil
}
