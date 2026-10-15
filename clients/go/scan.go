// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
)

// ---- scan iterators ----------------------------------------------------------------------

// Scanner walks a SCAN-family cursor lazily, one page per round trip.
//
//	it := c.ScanIter(ruvio.ScanOptions{Match: "user:*"})
//	for it.Next(ctx) { use(it.Val()) }
//	if err := it.Err(); err != nil { ... }
//
// HSCAN and ZSCAN yield field/member and value/score as consecutive items.
type Scanner struct {
	page   func(ctx context.Context, cursor string) (string, []string, error)
	cursor string
	items  []string
	pos    int
	done   bool
	err    error
	val    string
}

// Next advances; false at the end or on error.
func (s *Scanner) Next(ctx context.Context) bool {
	for s.pos >= len(s.items) {
		if s.done || s.err != nil {
			return false
		}
		next, items, err := s.page(ctx, s.cursor)
		if err != nil {
			s.err = err
			return false
		}
		s.cursor, s.items, s.pos = next, items, 0
		if next == "0" {
			s.done = true
		}
	}
	s.val = s.items[s.pos]
	s.pos++
	return true
}

// Val is the current item.
func (s *Scanner) Val() string { return s.val }

// Err is the first error.
func (s *Scanner) Err() error { return s.err }

func newScanner(page func(ctx context.Context, cursor string) (string, []string, error)) *Scanner {
	return &Scanner{page: page, cursor: "0"}
}

// ScanIter iterates keys.
func (c *cmds) ScanIter(o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.Scan(ctx, cur, o) })
}

// HScanIter iterates field, value, field, value...
func (c *cmds) HScanIter(key string, o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.HScan(ctx, key, cur, o) })
}

// SScanIter iterates set members.
func (c *cmds) SScanIter(key string, o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.SScan(ctx, key, cur, o) })
}

// ZScanIter iterates member, score, member, score...
func (c *cmds) ZScanIter(key string, o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.ZScan(ctx, key, cur, o) })
}
