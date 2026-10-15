// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"encoding/json"
)

// ---- capped list and Bloom ------------------------------------------------------------

// PushRecent records items at the head of a list capped at max (LPUSHCAP).
func (c *cmds) PushRecent(ctx context.Context, key string, max int64, items ...string) (int64, error) {
	return c.LPushCap(ctx, key, max, items...)
}

// Recent returns the newest n items pushed with PushRecent.
func (c *cmds) Recent(ctx context.Context, key string, n int64) ([]string, error) {
	return c.LRange(ctx, key, 0, n-1)
}

// SeenBefore adds items to a Bloom filter and reports, per item, whether it was
// (probably) seen before. One round trip (BF.MADD).
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

// MaybeSeen checks items without adding them (BF.MEXISTS).
func (c *cmds) MaybeSeen(ctx context.Context, key string, items ...string) ([]bool, error) {
	return c.BFMExists(ctx, key, items...)
}

// ---- typed JSON ------------------------------------------------------------------------

// JSONSetValue marshals v with encoding/json and stores it at path.
func (c *cmds) JSONSetValue(ctx context.Context, key, path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.ok(ctx, newCmd(4, cJSON_SET).s(key).s(path).b(b))
}

// JSONGetValue reads path and unmarshals it into dst; false when the key is missing.
func (c *cmds) JSONGetValue(ctx context.Context, key, path string, dst any) (bool, error) {
	v, err := c.value(ctx, pathReq(cJSON_GET, key, path))
	if err != nil || v.Kind == KindNil {
		return false, err
	}
	return true, json.Unmarshal(v.Bytes, dst)
}

// JSONGetAs is the generic form of JSONGetValue.
func JSONGetAs[T any](ctx context.Context, c *Client, key, path string) (T, bool, error) {
	var out T
	ok, err := c.JSONGetValue(ctx, key, path, &out)
	return out, ok, err
}
