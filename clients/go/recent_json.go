// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"encoding/json"
)

// ---- capped list and Bloom ------------------------------------------------------------

// PushRecent pushes items at the head of a list and trims it to at most max entries in
// one step (LPUSHCAP). It returns the new list length. Use it for "latest N" feeds.
// Returns WRONGTYPE if key is not a list.
func (c *cmds) PushRecent(ctx context.Context, key string, max int64, items ...string) (int64, error) {
	return c.LPushCap(ctx, key, max, items...)
}

// Recent returns the newest n items of a list written with PushRecent (LRANGE key 0
// n-1), newest first. n should be positive: 0 would return the whole list. A missing key
// gives an empty slice.
func (c *cmds) Recent(ctx context.Context, key string, n int64) ([]string, error) {
	return c.LRange(ctx, key, 0, n-1)
}

// SeenBefore adds items to a Bloom filter and reports, for each item, whether it was
// probably seen before (the inverse of BF.MADD, which reports "newly added"). One round
// trip. A true can be a false positive; a
// false is always correct.
func (c *cmds) SeenBefore(ctx context.Context, key string, items ...string) ([]bool, error) {
	added, err := c.BFMAdd(ctx, key, items...)
	if err != nil {
		return nil, err
	}
	for i := range added {
		added[i] = !added[i]
	}
	return added, nil
}

// MaybeSeen checks items against a Bloom filter without adding them (BF.MEXISTS). true
// means probably present, false means definitely absent. A missing key gives all false.
func (c *cmds) MaybeSeen(ctx context.Context, key string, items ...string) ([]bool, error) {
	return c.BFMExists(ctx, key, items...)
}

// ---- typed JSON ------------------------------------------------------------------------

// JSONSetValue marshals v with encoding/json and stores it at path (JSON.SET). Use "$"
// for the whole document. It returns an error if v cannot be encoded or the command
// fails.
func (c *cmds) JSONSetValue(ctx context.Context, key, path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.ok(ctx, newCmd(4, cJSON_SET).s(key).s(path).b(b))
}

// JSONGetValue reads path (JSON.GET) and decodes it into dst. It returns false, with dst
// untouched, when the key or path is missing (null reply), and an error if decoding
// fails.
func (c *cmds) JSONGetValue(ctx context.Context, key, path string, dst any) (bool, error) {
	v, err := c.value(ctx, pathReq(cJSON_GET, key, path))
	if err != nil || v.Kind == KindNil {
		return false, err
	}
	return true, json.Unmarshal(v.Bytes, dst)
}

// JSONGetAs is the generic form of JSONGetValue: it returns the decoded T and whether it
// was found.
//
//	u, ok, err := ruvio.JSONGetAs[User](ctx, c, "user:1", "$")
func JSONGetAs[T any](ctx context.Context, c *Client, key, path string) (T, bool, error) {
	var out T
	ok, err := c.JSONGetValue(ctx, key, path, &out)
	return out, ok, err
}
