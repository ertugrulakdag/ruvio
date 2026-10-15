// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import "testing"

func vb(s string) Value    { return Value{Kind: KindBulk, Bytes: []byte(s)} }
func vi(n int64) Value     { return Value{Kind: KindInt, Int: n} }
func va(vs ...Value) Value { return Value{Kind: KindArray, Array: vs} }
func vm(vs ...Value) Value { return Value{Kind: KindMap, Array: vs} }
func vnil() Value          { return Value{Kind: KindNil} }
func mustErr(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error", what)
	}
}

func TestToZsShapes(t *testing.T) {
	if z, err := toZs(vnil()); err != nil || z != nil {
		t.Fatalf("nil reply: %v %v", z, err)
	}
	_, err := toZs(vb("x"))
	mustErr(t, err, "scalar reply")

	flat, err := toZs(va(vb("a"), vb("1.5"), vb("b"), vb("-2")))
	if err != nil || len(flat) != 2 || flat[0] != (Z{"a", 1.5}) || flat[1] != (Z{"b", -2}) {
		t.Fatalf("flat: %v %v", flat, err)
	}
	nested, err := toZs(va(va(vb("a"), vb("3")), va(vb("b"), vb("4"))))
	if err != nil || len(nested) != 2 || nested[1] != (Z{"b", 4}) {
		t.Fatalf("nested: %v %v", nested, err)
	}
	if empty, err := toZs(va()); err != nil || len(empty) != 0 {
		t.Fatalf("empty: %v %v", empty, err)
	}
	_, err = toZs(va(vb("a"), vb("1"), vb("b")))
	mustErr(t, err, "odd flat length")
	_, err = toZs(va(va(vb("a"))))
	mustErr(t, err, "nested pair of wrong size")
	_, err = toZs(va(va(vb("a"), vb("notanumber"))))
	mustErr(t, err, "nested bad score")
	_, err = toZs(va(vb("a"), vb("notanumber")))
	mustErr(t, err, "flat bad score")
}

func TestToIntsAndStrMap(t *testing.T) {
	if n, err := toInts(vnil()); err != nil || n != nil {
		t.Fatalf("nil: %v %v", n, err)
	}
	_, err := toInts(vi(1))
	mustErr(t, err, "scalar")
	n, err := toInts(va(vi(1), vnil(), vb("7")))
	if err != nil || len(n) != 3 || n[0] != 1 || n[1] != 0 || n[2] != 7 {
		t.Fatalf("mixed: %v %v", n, err)
	}
	_, err = toInts(va(vb("zz")))
	mustErr(t, err, "non numeric element")

	m, err := toStrMap(vnil())
	if err != nil || m == nil || len(m) != 0 {
		t.Fatalf("nil map: %v %v", m, err)
	}
	m, err = toStrMap(va(vb("k"), vb("v"), vb("k2"), vi(2)))
	if err != nil || m["k"] != "v" || m["k2"] != "2" {
		t.Fatalf("map: %v %v", m, err)
	}
	m, err = toStrMap(vm(vb("a"), vb("b")))
	if err != nil || m["a"] != "b" {
		t.Fatalf("resp3 map: %v %v", m, err)
	}
	_, err = toStrMap(va(vb("k")))
	mustErr(t, err, "odd length")
	_, err = toStrMap(vb("x"))
	mustErr(t, err, "scalar")
}

func TestToXMessagesShapes(t *testing.T) {
	if m, err := toXMessages(vnil()); err != nil || m != nil {
		t.Fatalf("nil: %v %v", m, err)
	}
	_, err := toXMessages(vi(3))
	mustErr(t, err, "scalar")

	// Normal reply: [[id, [f, v]], ...]
	msgs, err := toXMessages(va(va(vb("1-0"), va(vb("f"), vb("v"))), va(vb("2-0"), va(vb("g"), vb("w")))))
	if err != nil || len(msgs) != 2 || msgs[0].ID != "1-0" || msgs[1].Values[0] != "g" {
		t.Fatalf("normal: %v %v", msgs, err)
	}
	// Single unwrapped entry shape from a woken blocked XREAD.
	msgs, err = toXMessages(va(vb("9-0"), va(vb("f"), vb("v"))))
	if err != nil || len(msgs) != 1 || msgs[0].ID != "9-0" || msgs[0].Values[1] != "v" {
		t.Fatalf("unwrapped: %v %v", msgs, err)
	}
	// Entry without fields (id only) and entries that are skipped.
	msgs, err = toXMessages(va(va(vb("5-0")), vb("6-0"), vnil(), va()))
	if err != nil || len(msgs) != 2 || msgs[0].ID != "5-0" || msgs[1].ID != "6-0" {
		t.Fatalf("id only: %v %v", msgs, err)
	}
	// Fields that are not a string array.
	_, err = toXMessages(va(va(vb("1-0"), Value{Kind: KindError, Bytes: []byte("x")}), vb("a")))
	_ = err // Strings() on a non-aggregate reports an error
	_, err = toXMessages(va(va(vb("1-0"), vi(5)), va(vb("2-0"), vi(5))))
	mustErr(t, err, "fields not an array")
}

func TestToXStreamsShapes(t *testing.T) {
	if s, err := toXStreams(vnil()); err != nil || s != nil {
		t.Fatalf("nil: %v %v", s, err)
	}
	_, err := toXStreams(vb("x"))
	mustErr(t, err, "scalar")

	entry := va(va(vb("1-0"), va(vb("f"), vb("v"))))
	resp2, err := toXStreams(va(va(vb("s1"), entry), va(vb("s2"), entry)))
	if err != nil || len(resp2) != 2 || resp2[1].Stream != "s2" || resp2[0].Messages[0].ID != "1-0" {
		t.Fatalf("resp2: %v %v", resp2, err)
	}
	resp3, err := toXStreams(vm(vb("s1"), entry))
	if err != nil || len(resp3) != 1 || resp3[0].Stream != "s1" {
		t.Fatalf("resp3: %v %v", resp3, err)
	}
	_, err = toXStreams(va(va(vb("s1"))))
	mustErr(t, err, "stream pair of wrong size")
	_, err = toXStreams(va(va(vb("s1"), vi(1))))
	mustErr(t, err, "messages not an array")
	_, err = toXStreams(vm(vb("s1"), vi(1)))
	mustErr(t, err, "resp3 messages not an array")
}
