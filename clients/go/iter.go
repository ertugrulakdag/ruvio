// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

//go:build go1.23

package ruvio

import (
	"context"
	"iter"
)

// All adapts a Scanner to a range-over-func sequence (Go 1.23+):
//
//	for key, err := range c.ScanIter(opts).All(ctx) { ... }
//
// The error is yielded once, as the last element, when a page fails.
func (s *Scanner) All(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for s.Next(ctx) {
			if !yield(s.val, nil) {
				return
			}
		}
		if s.err != nil {
			yield("", s.err)
		}
	}
}
