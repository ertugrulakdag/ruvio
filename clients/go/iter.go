// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

//go:build go1.23

package ruvio

import (
	"context"
	"iter"
)

// All adapts the Scanner to a range-over-func sequence (Go 1.23+). The error, if any, is
// yielded once as the last element.
//
//	for key, err := range c.ScanIter(opts).All(ctx) { ... }
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
