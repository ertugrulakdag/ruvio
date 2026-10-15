// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"os"
	"testing"
	"time"
)

func TestLiveJSON(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)

	noErr(t, c.JSONSet(ctx, "j", "$", `{"s":"ab","n":1,"b":true,"a":[1,2,3],"o":{"x":1}}`))
	doc, ok, err := c.JSONGet(ctx, "j")
	noErr(t, err)
	if !ok || doc == "" {
		t.Fatal("json.get")
	}
	v, _, _ := c.JSONGet(ctx, "j", ".a")
	eq(t, v, "[1,2,3]")
	_, ok, _ = c.JSONGet(ctx, "nope")
	eq(t, ok, false)
	eq(t, must[bool](t)(c.JSONSetMode(ctx, "j", ".s", `"zz"`, "NX")), false)
	eq(t, must[bool](t)(c.JSONSetMode(ctx, "j", ".new", `0`, "NX")), true)
	eq(t, must[bool](t)(c.JSONSetMode(ctx, "j", ".new", `5`, "XX")), true)

	typ, ok, _ := c.JSONType(ctx, "j", ".a")
	eq(t, []any{typ, ok}, []any{"array", true})
	n, ok, _ := c.JSONStrLen(ctx, "j", ".s")
	eq(t, []any{n, ok}, []any{int64(2), true})
	n, _, _ = c.JSONArrLen(ctx, "j", ".a")
	eq(t, n, int64(3))
	n, _, _ = c.JSONObjLen(ctx, "j", ".o")
	eq(t, n, int64(1))
	eq(t, must[[]string](t)(c.JSONObjKeys(ctx, "j", ".o")), []string{"x"})
	eq(t, must[string](t)(c.JSONNumIncrBy(ctx, "j", ".n", 2)), "3")
	eq(t, must[string](t)(c.JSONNumMultBy(ctx, "j", ".n", 2)), "6")
	eq(t, must[bool](t)(c.JSONToggle(ctx, "j", ".b")), false)
	eq(t, mi(c.JSONArrAppend(ctx, "j", ".a", "4", "5")), int64(5))
	eq(t, mi(c.JSONArrInsert(ctx, "j", ".a", 0, "0")), int64(6))
	p, ok, _ := c.JSONArrPop(ctx, "j", ".a", -1)
	eq(t, []any{p, ok}, []any{"5", true})
	eq(t, mi(c.JSONArrIndex(ctx, "j", ".a", "3")), int64(3))
	eq(t, mi(c.JSONArrIndex(ctx, "j", ".a", "3", 0, 2)), int64(-1))
	eq(t, mi(c.JSONArrTrim(ctx, "j", ".a", 1, 2)), int64(2))
	eq(t, mi(c.JSONStrAppend(ctx, "j", ".s", `"cd"`)), int64(4))
	noErr(t, c.JSONMerge(ctx, "j", ".o", `{"y":2}`))
	noErr(t, c.JMerge(ctx, "j", ".o", `{"x":null}`))
	eq(t, must[[]string](t)(c.JSONObjKeys(ctx, "j", ".o")), []string{"y"})
	eq(t, mi(c.JSONClear(ctx, "j", ".o")), int64(1))
	eq(t, mi(c.JSONDel(ctx, "j", ".new")), int64(1))
	eq(t, mi(c.JSONForget(ctx, "j", ".b")), int64(1))
	noErr(t, c.JSONSet(ctx, "j2", "$", `{"s":"other"}`))
	eq(t, must[[]NullString](t)(c.JSONMGet(ctx, ".s", "j", "j2", "nope")), []NullString{{`"abcd"`, true}, {`"other"`, true}, {}})
	eq(t, mi(c.JSONDel(ctx, "j2", "")), int64(1))

	// typed JSON helpers
	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	noErr(t, c.JSONSetValue(ctx, "u", "$", user{"ann", 30}))
	got, ok, err := JSONGetAs[user](ctx, c, "u", "")
	noErr(t, err)
	eq(t, []any{got, ok}, []any{user{"ann", 30}, true})
	var age int
	ok, err = c.JSONGetValue(ctx, "u", ".age", &age)
	noErr(t, err)
	eq(t, []any{age, ok}, []any{30, true})
}

// JSON.INDEX runs only in database 0; this test uses its own prefix there and
// removes what it created.
func TestLiveJSONIndex(t *testing.T) {
	addr := os.Getenv("RUVIO_TEST_ADDR")
	if addr == "" {
		t.Skip("RUVIO_TEST_ADDR not set")
	}
	c0, ctx := live(t)
	c0.Close()
	c, err := Connect(ctx, Options{Addr: addr})
	noErr(t, err)
	defer c.Close()
	c.JSONIndexDrop(ctx, "goidx")
	noErr(t, c.JSONIndexCreate(ctx, "goidx", "gotest:", "$.city", "string"))
	defer c.JSONIndexDrop(ctx, "goidx")
	defer c.Del(ctx, "gotest:1", "gotest:2", "gotest:3")
	noErr(t, c.JSONSet(ctx, "gotest:1", "$", `{"city":"Izmir"}`))
	noErr(t, c.JSONSet(ctx, "gotest:2", "$", `{"city":"Ankara"}`))
	noErr(t, c.JSONSet(ctx, "gotest:3", "$", `{"city":"Izmir"}`))
	var keys []string
	for i := 0; i < 50; i++ { // the first build is asynchronous
		keys, err = c.JSONFind(ctx, "goidx", "Izmir", 0, "")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	noErr(t, err)
	eq(t, keys, []string{"gotest:1", "gotest:3"})
	eq(t, must[[]string](t)(c.JSONFind(ctx, "goidx", "Izmir", 1, "gotest:1")), []string{"gotest:3"})
	list, err := c.JSONIndexList(ctx)
	noErr(t, err)
	if len(list.Array) == 0 {
		t.Fatal("index list empty")
	}
	noErr(t, c.JSONIndexDrop(ctx, "goidx"))
}

func TestLiveBloom(t *testing.T) {
	c, ctx := live(t)
	mb := must[bool](t)
	mbs := must[[]bool](t)

	noErr(t, c.BFReserve(ctx, "bf", 0.01, 1000, 0, false))
	noErr(t, c.BFReserve(ctx, "bf2", 0.01, 1000, 4, false))
	noErr(t, c.BFReserve(ctx, "bf3", 0.01, 2, 0, true))
	eq(t, mb(c.BFAdd(ctx, "bf", "a")), true)
	eq(t, mb(c.BFAdd(ctx, "bf", "a")), false)
	eq(t, mbs(c.BFMAdd(ctx, "bf", "a", "b")), []bool{false, true})
	eq(t, mb(c.BFExists(ctx, "bf", "b")), true)
	eq(t, mbs(c.BFMExists(ctx, "bf", "a", "zzz")), []bool{true, false})
	eq(t, mbs(c.BFInsert(ctx, "bfi", BFInsertOptions{Capacity: 100, Error: 0.001, Expansion: 2}, "x", "y")), []bool{true, true})
	if _, err := c.BFInsert(ctx, "nope", BFInsertOptions{NoCreate: true}, "x"); err == nil {
		t.Fatal("NOCREATE on a missing key must fail")
	}
	eq(t, mbs(c.BFInsert(ctx, "bfn", BFInsertOptions{NonScaling: true}, "x")), []bool{true})
	info, err := c.BFInfo(ctx, "bf")
	noErr(t, err)
	if n, _ := info["Capacity"].AsInt64(); n != 1000 {
		t.Fatalf("bf.info %v", info)
	}
	eq(t, must[int64](t)(c.BFCard(ctx, "bf")), int64(2))

	// helpers
	eq(t, mbs(c.SeenBefore(ctx, "seen", "u1", "u2")), []bool{false, false})
	eq(t, mbs(c.SeenBefore(ctx, "seen", "u1", "u3")), []bool{true, false})
	eq(t, mbs(c.MaybeSeen(ctx, "seen", "u2", "u9")), []bool{true, false})
}
