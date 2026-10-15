// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// ErrNotAcquired is returned by the scoped helpers when the resource is held.
var ErrNotAcquired = errors.New("ruvio: not acquired")

func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Lock is a held distributed lock (SET key token NX PX). Unlock and Refresh only act
// while the stored token is still ours, so they never touch someone else's lock.
type Lock struct {
	c     *cmds
	Key   string
	Token string
}

// AcquireLock tries once to take key for ttl. ok is false when another holder has it.
func (c *cmds) AcquireLock(ctx context.Context, key string, ttl time.Duration) (*Lock, bool, error) {
	token := randomToken()
	ok, err := c.SetArgs(ctx, key, token, SetOptions{TTL: ttl, NX: true})
	if err != nil || !ok {
		return nil, false, err
	}
	return &Lock{c: c, Key: key, Token: token}, true, nil
}

// AcquireLockWait retries every `every` until the lock is taken or ctx ends.
func (c *cmds) AcquireLockWait(ctx context.Context, key string, ttl, every time.Duration) (*Lock, error) {
	for {
		l, ok, err := c.AcquireLock(ctx, key, ttl)
		if err != nil || ok {
			return l, err
		}
		if err := sleep(ctx, every); err != nil {
			return nil, err
		}
	}
}

// Refresh extends the lock to ttl when it is still ours (SET key token IFEQ token PX).
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) (bool, error) {
	return l.c.SetIfEq(ctx, l.Key, l.Token, l.Token, SetOptions{TTL: ttl})
}

// Unlock releases the lock when it is still ours (DELEX key IFEQ token).
func (l *Lock) Unlock(ctx context.Context) (bool, error) {
	return l.c.DelIfEq(ctx, l.Key, l.Token)
}

// WithLock runs fn while holding key (one attempt) and always releases it afterwards.
// It returns ErrNotAcquired when the lock is held elsewhere.
func (c *cmds) WithLock(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	l, ok, err := c.AcquireLock(ctx, key, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotAcquired
	}
	defer l.Unlock(context.WithoutCancel(ctx))
	return fn(ctx)
}

// ---- fenced lease and semaphore ---------------------------------------------------

// LeaseHandle is a held fenced lease; Token increases with every grant.
type LeaseHandle struct {
	c     *cmds
	Key   string
	Token int64
}

// AcquireLease takes a fenced lease. ok is false while held elsewhere.
func (c *cmds) AcquireLease(ctx context.Context, key string, ttl time.Duration) (*LeaseHandle, bool, error) {
	tok, ok, err := c.Lease(ctx, key, ttl)
	if err != nil || !ok {
		return nil, false, err
	}
	return &LeaseHandle{c: c, Key: key, Token: tok}, true, nil
}

// Release frees the lease when this token is still the holder.
func (l *LeaseHandle) Release(ctx context.Context) (bool, error) {
	return l.c.Release(ctx, l.Key, l.Token)
}

// WithLease runs fn with a fenced token and releases the lease afterwards.
func (c *cmds) WithLease(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context, token int64) error) error {
	l, ok, err := c.AcquireLease(ctx, key, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotAcquired
	}
	defer l.Release(context.WithoutCancel(ctx))
	return fn(ctx, l.Token)
}

// Permit is a held semaphore slot.
type Permit struct {
	c     *cmds
	Key   string
	Token int64
}

// AcquirePermit takes one of max slots for ttl. ok is false when full.
func (c *cmds) AcquirePermit(ctx context.Context, key string, max int64, ttl time.Duration) (*Permit, bool, error) {
	tok, ok, err := c.Semaphore(ctx, key, max, ttl)
	if err != nil || !ok {
		return nil, false, err
	}
	return &Permit{c: c, Key: key, Token: tok}, true, nil
}

// Release frees the slot.
func (p *Permit) Release(ctx context.Context) (bool, error) {
	return p.c.Release(ctx, p.Key, p.Token)
}

// WithPermit runs fn holding a semaphore slot and releases it afterwards.
func (c *cmds) WithPermit(ctx context.Context, key string, max int64, ttl time.Duration, fn func(ctx context.Context) error) error {
	p, ok, err := c.AcquirePermit(ctx, key, max, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotAcquired
	}
	defer p.Release(context.WithoutCancel(ctx))
	return fn(ctx)
}

// ---- rate limit -----------------------------------------------------------------

// RateLimit counts one call against max per window (fixed window on LIMIT).
// remaining is the allowance left after this call (0 when rejected).
func (c *cmds) RateLimit(ctx context.Context, key string, max int64, window time.Duration) (allowed bool, remaining int64, err error) {
	remaining, allowed, err = c.Limit(ctx, key, max, window)
	return allowed, remaining, err
}
