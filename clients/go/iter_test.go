// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

//go:build go1.23

package ruvio

import "testing"

func TestLiveScanRangeFunc(t *testing.T) {
	c, ctx := live(t)
	for i := 0; i < 12; i++ {
		noErr(t, c.Set(ctx, "it:"+string(rune('a'+i)), "x"))
	}
	n := 0
	for key, err := range c.ScanIter(ScanOptions{Match: "it:*", Count: 5}).All(ctx) {
		noErr(t, err)
		if key == "" {
			t.Fatal("empty key")
		}
		n++
	}
	eq(t, n, 12)
	// early break stops paging
	for range c.ScanIter(ScanOptions{Match: "it:*", Count: 1}).All(ctx) {
		break
	}
}
