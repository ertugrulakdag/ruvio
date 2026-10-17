// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
)

// ---- scan iterators ----------------------------------------------------------------------

// Scanner walks a SCAN-family cursor lazily, fetching one page per round trip. Create
// one with ScanIter, HScanIter, SScanIter or ZScanIter. It is not safe for concurrent
// use.
//
//	it := c.ScanIter(ruvio.ScanOptions{Match: "user:*"})
//	for it.Next(ctx) { use(it.Val()) }
//	if err := it.Err(); err != nil { ... }
//
// HSCAN and ZSCAN yield field/member and value/score as consecutive items. As with SCAN,
// items can repeat and elements changed during the walk may be missed.
type Scanner struct {
	page   func(ctx context.Context, cursor string) (string, []string, error)
	cursor string
	items  []string
	pos    int
	done   bool
	err    error
	val    string
}

// Next advances to the next item, fetching a new page when needed. It returns false at
// the end of the iteration or after an error (check Err).
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

// Val returns the current item. It is valid after Next returned true.
func (s *Scanner) Val() string { return s.val }

// Err returns the first error that stopped the iteration, or nil.
func (s *Scanner) Err() error { return s.err }

func newScanner(page func(ctx context.Context, cursor string) (string, []string, error)) *Scanner {
	return &Scanner{page: page, cursor: "0"}
}

// ScanIter iterates over the keys of the keyspace (SCAN), using the Match, Count and
// Type options in o. In cluster mode it walks every node.
func (c *cmds) ScanIter(o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.Scan(ctx, cur, o) })
}

// HScanIter iterates over a hash as field, value, field, value ... (HSCAN).
func (c *cmds) HScanIter(key string, o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.HScan(ctx, key, cur, o) })
}

// SScanIter iterates over the members of a set (SSCAN).
func (c *cmds) SScanIter(key string, o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.SScan(ctx, key, cur, o) })
}

// ZScanIter iterates over a sorted set as member, score, member, score ... (ZSCAN).
// Scores are returned as strings.
func (c *cmds) ZScanIter(key string, o ScanOptions) *Scanner {
	return newScanner(func(ctx context.Context, cur string) (string, []string, error) { return c.ZScan(ctx, key, cur, o) })
}
