// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import "context"

// first unwraps a single-element array reply (RedisJSON "$" paths) to its element.
func first(v Value) Value {
	if v.Kind == KindArray && len(v.Array) == 1 {
		return v.Array[0]
	}
	return v
}

func (c *cmds) jsonInt(ctx context.Context, r *request) (int64, bool, error) {
	v, err := c.value(ctx, r)
	if err != nil {
		return 0, false, err
	}
	v = first(v)
	if v.Kind == KindNil {
		return 0, false, nil
	}
	n, err := v.AsInt64()
	return n, err == nil, err
}

func pathReq(name []byte, key, path string) *request {
	if path == "" {
		return newCmd(2, name).s(key)
	}
	return newCmd(3, name).s(key).s(path)
}

// JSONGet returns the JSON text at the paths (no path = "$").
func (c *cmds) JSONGet(ctx context.Context, key string, paths ...string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2+len(paths), cJSON_GET).s(key).ss(paths))
}

// JSONSet writes json at path.
func (c *cmds) JSONSet(ctx context.Context, key, path, json string) error {
	return c.ok(ctx, newCmd(4, cJSON_SET).s(key).s(path).s(json))
}

// JSONSetMode writes with "NX" (path missing) or "XX" (path present); false when skipped.
func (c *cmds) JSONSetMode(ctx context.Context, key, path, json, mode string) (bool, error) {
	v, err := c.value(ctx, newCmd(5, cJSON_SET).s(key).s(path).s(json).s(mode))
	return err == nil && v.Kind != KindNil, err
}

// JSONDel removes the value at path ("" = root).
func (c *cmds) JSONDel(ctx context.Context, key, path string) (int64, error) {
	return c.intR(ctx, pathReq(cJSON_DEL, key, path))
}

// JSONForget is the JSON.DEL alias.
func (c *cmds) JSONForget(ctx context.Context, key, path string) (int64, error) {
	return c.intR(ctx, pathReq(cJSON_FORGET, key, path))
}

// JSONMGet reads one path from several keys.
func (c *cmds) JSONMGet(ctx context.Context, path string, keys ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(2+len(keys), cJSON_MGET).ss(keys).s(path))
}

// JSONMerge applies an RFC 7396 merge patch at path.
func (c *cmds) JSONMerge(ctx context.Context, key, path, patch string) error {
	return c.ok(ctx, newCmd(4, cJSON_MERGE).s(key).s(path).s(patch))
}

// JMerge is the JSON.MERGE alias.
func (c *cmds) JMerge(ctx context.Context, key, path, patch string) error {
	return c.ok(ctx, newCmd(4, cJMERGE).s(key).s(path).s(patch))
}

// JSONType returns the type name at path.
func (c *cmds) JSONType(ctx context.Context, key, path string) (string, bool, error) {
	v, err := c.value(ctx, pathReq(cJSON_TYPE, key, path))
	if err != nil {
		return "", false, err
	}
	v = first(v)
	return v.Str(), v.Kind != KindNil, nil
}

// JSONStrLen returns a string value's byte length.
func (c *cmds) JSONStrLen(ctx context.Context, key, path string) (int64, bool, error) {
	return c.jsonInt(ctx, pathReq(cJSON_STRLEN, key, path))
}

// JSONArrLen returns an array's length.
func (c *cmds) JSONArrLen(ctx context.Context, key, path string) (int64, bool, error) {
	return c.jsonInt(ctx, pathReq(cJSON_ARRLEN, key, path))
}

// JSONObjLen returns an object's member count.
func (c *cmds) JSONObjLen(ctx context.Context, key, path string) (int64, bool, error) {
	return c.jsonInt(ctx, pathReq(cJSON_OBJLEN, key, path))
}

// JSONObjKeys returns object member names.
func (c *cmds) JSONObjKeys(ctx context.Context, key, path string) ([]string, error) {
	return c.strs(ctx, pathReq(cJSON_OBJKEYS, key, path))
}

// JSONNumIncrBy adds to a number and returns the new value as JSON text.
func (c *cmds) JSONNumIncrBy(ctx context.Context, key, path string, delta float64) (string, error) {
	v, err := c.value(ctx, newCmd(4, cJSON_NUMINCRBY).s(key).s(path).f(delta))
	return first(v).Str(), err
}

// JSONNumMultBy multiplies a number and returns the new value as JSON text.
func (c *cmds) JSONNumMultBy(ctx context.Context, key, path string, factor float64) (string, error) {
	v, err := c.value(ctx, newCmd(4, cJSON_NUMMULTBY).s(key).s(path).f(factor))
	return first(v).Str(), err
}

// JSONToggle flips a boolean and returns the new value.
func (c *cmds) JSONToggle(ctx context.Context, key, path string) (bool, error) {
	n, _, err := c.jsonInt(ctx, newCmd(3, cJSON_TOGGLE).s(key).s(path))
	return n == 1, err
}

// JSONArrAppend appends JSON values and returns the new length.
func (c *cmds) JSONArrAppend(ctx context.Context, key, path string, values ...string) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(3+len(values), cJSON_ARRAPPEND).s(key).s(path).ss(values))
	return n, err
}

// JSONArrInsert inserts JSON values before index.
func (c *cmds) JSONArrInsert(ctx context.Context, key, path string, index int64, values ...string) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(4+len(values), cJSON_ARRINSERT).s(key).s(path).i(index).ss(values))
	return n, err
}

// JSONArrPop removes and returns the element at index (-1 = last) as JSON text.
func (c *cmds) JSONArrPop(ctx context.Context, key, path string, index int64) (string, bool, error) {
	v, err := c.value(ctx, newCmd(4, cJSON_ARRPOP).s(key).s(path).i(index))
	if err != nil {
		return "", false, err
	}
	v = first(v)
	return v.Str(), v.Kind != KindNil, nil
}

// JSONArrTrim keeps [start, stop] and returns the new length.
func (c *cmds) JSONArrTrim(ctx context.Context, key, path string, start, stop int64) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(5, cJSON_ARRTRIM).s(key).s(path).i(start).i(stop))
	return n, err
}

// JSONArrIndex finds a JSON scalar (optionally within start [stop]); -1 when absent.
func (c *cmds) JSONArrIndex(ctx context.Context, key, path, scalar string, startStop ...int64) (int64, error) {
	r := newCmd(4+len(startStop), cJSON_ARRINDEX).s(key).s(path).s(scalar)
	for _, x := range startStop {
		r.i(x)
	}
	n, _, err := c.jsonInt(ctx, r)
	return n, err
}

// JSONClear empties containers / zeroes numbers and returns how many changed.
func (c *cmds) JSONClear(ctx context.Context, key, path string) (int64, error) {
	n, _, err := c.jsonInt(ctx, pathReq(cJSON_CLEAR, key, path))
	return n, err
}

// JSONStrAppend appends a JSON string literal and returns the new length.
func (c *cmds) JSONStrAppend(ctx context.Context, key, path, jsonString string) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(4, cJSON_STRAPPEND).s(key).s(path).s(jsonString))
	return n, err
}

// JSONIndexCreate creates an equality index (database 0 only). typ is "", "string",
// "number" or "bool".
func (c *cmds) JSONIndexCreate(ctx context.Context, name, prefix, path, typ string) error {
	if typ == "" {
		return c.ok(ctx, newCmd(7, cJSON_INDEX).t(tCREATE).s(name).t(tPREFIX).s(prefix).t(tPATH).s(path))
	}
	return c.ok(ctx, newCmd(9, cJSON_INDEX).t(tCREATE).s(name).t(tPREFIX).s(prefix).t(tPATH).s(path).t(tTYPE).s(typ))
}

// JSONIndexDrop drops an index.
func (c *cmds) JSONIndexDrop(ctx context.Context, name string) error {
	return c.ok(ctx, newCmd(3, cJSON_INDEX).t(tDROP).s(name))
}

// JSONIndexList lists index definitions.
func (c *cmds) JSONIndexList(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cJSON_INDEX).t(tLIST))
}

// JSONFind returns keys whose indexed field equals value, ascending (count 0 = server
// default; after "" = from the start).
func (c *cmds) JSONFind(ctx context.Context, index, value string, count int64, after string) ([]string, error) {
	n := 4
	if count > 0 {
		n += 2
	}
	if after != "" {
		n += 2
	}
	r := newCmd(n, cJSON_FIND).s(index).t(tVALUE).s(value)
	if count > 0 {
		r.t(tCOUNT).i(count)
	}
	if after != "" {
		r.t(tAFTER).s(after)
	}
	return c.strs(ctx, r)
}

// ---- Bloom filters ---------------------------------------------------------------

// BFReserve creates a filter. expansion 0 omits it; nonScaling sends NONSCALING.
func (c *cmds) BFReserve(ctx context.Context, key string, errorRate float64, capacity, expansion int64, nonScaling bool) error {
	switch {
	case nonScaling:
		return c.ok(ctx, newCmd(5, cBF_RESERVE).s(key).f(errorRate).i(capacity).t(tNONSCALING))
	case expansion > 0:
		return c.ok(ctx, newCmd(6, cBF_RESERVE).s(key).f(errorRate).i(capacity).t(tEXPANSION).i(expansion))
	}
	return c.ok(ctx, newCmd(4, cBF_RESERVE).s(key).f(errorRate).i(capacity))
}

// BFAdd adds an item; true when it was new.
func (c *cmds) BFAdd(ctx context.Context, key, item string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cBF_ADD).s(key).s(item))
}

// BFMAdd adds items.
func (c *cmds) BFMAdd(ctx context.Context, key string, items ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(items), cBF_MADD).s(key).ss(items))
}

// BFExists tests an item.
func (c *cmds) BFExists(ctx context.Context, key, item string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cBF_EXISTS).s(key).s(item))
}

// BFMExists tests items.
func (c *cmds) BFMExists(ctx context.Context, key string, items ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(items), cBF_MEXISTS).s(key).ss(items))
}

// BFInsertOptions apply only when the filter is created.
type BFInsertOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NoCreate   bool
	NonScaling bool
}

// BFInsert adds items, creating the filter with options when missing.
func (c *cmds) BFInsert(ctx context.Context, key string, o BFInsertOptions, items ...string) ([]bool, error) {
	n := 3 + len(items)
	if o.Capacity > 0 {
		n += 2
	}
	if o.Error > 0 {
		n += 2
	}
	if o.Expansion > 0 {
		n += 2
	}
	if o.NoCreate {
		n++
	}
	if o.NonScaling {
		n++
	}
	r := newCmd(n, cBF_INSERT).s(key)
	if o.Capacity > 0 {
		r.t(tCAPACITY).i(o.Capacity)
	}
	if o.Error > 0 {
		r.t(tERROR).f(o.Error)
	}
	if o.Expansion > 0 {
		r.t(tEXPANSION).i(o.Expansion)
	}
	if o.NoCreate {
		r.t(tNOCREATE)
	}
	if o.NonScaling {
		r.t(tNONSCALING)
	}
	r.t(tITEMS).ss(items)
	return c.bools(ctx, r)
}

// BFInfo returns the filter fields (Capacity, Size, Number of filters, ...).
func (c *cmds) BFInfo(ctx context.Context, key string) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(2, cBF_INFO).s(key))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

// BFCard returns the number of items added.
func (c *cmds) BFCard(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cBF_CARD).s(key))
}
