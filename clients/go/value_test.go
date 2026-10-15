// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"errors"
	"math"
	"testing"
)

func TestValueConversions(t *testing.T) {
	dbl, _, err := ParseReply([]byte(",2.5\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	bad := []Value{vnil(), va(), {Kind: KindError, Bytes: []byte("ERR x")}}

	ints := []struct {
		v    Value
		want int64
	}{
		{vi(-9), -9}, {Value{Kind: KindBool, Int: 1}, 1}, {vb("123"), 123}, {vb("-7"), -7},
		{Value{Kind: KindSimple, Bytes: []byte("5")}, 5}, {Value{Kind: KindBigNum, Bytes: []byte("77")}, 77}, {dbl, 2},
	}
	for _, c := range ints {
		got, err := c.v.AsInt64()
		if err != nil || got != c.want {
			t.Fatalf("AsInt64(%v) = %d, %v; want %d", c.v, got, err, c.want)
		}
	}
	for _, b := range bad {
		if _, err := b.AsInt64(); err == nil {
			t.Fatalf("AsInt64(%v) should fail", b)
		}
		if _, err := b.AsFloat64(); err == nil {
			t.Fatalf("AsFloat64(%v) should fail", b)
		}
	}
	// Numeric strings that do not parse, and boundaries of the fast integer path.
	for _, s := range []string{"", "-", "12a", "9223372036854775808", "-9223372036854775809", "123456789012345678901"} {
		if _, err := vb(s).AsInt64(); err == nil {
			t.Fatalf("AsInt64(%q) should fail", s)
		}
	}
	for s, want := range map[string]int64{"9223372036854775807": math.MaxInt64, "-9223372036854775808": math.MinInt64, "0": 0, "-0": 0, "1000000000000000000": 1e18} {
		got, err := vb(s).AsInt64()
		if err != nil || got != want {
			t.Fatalf("AsInt64(%q) = %d, %v", s, got, err)
		}
	}

	floats := map[string]float64{"1.5": 1.5, "-3": -3, "1e3": 1000, "inf": math.Inf(1), "+inf": math.Inf(1), "Inf": math.Inf(1), "+Inf": math.Inf(1), "-inf": math.Inf(-1), "-Inf": math.Inf(-1)}
	for s, want := range floats {
		got, err := vb(s).AsFloat64()
		if err != nil || got != want {
			t.Fatalf("AsFloat64(%q) = %v, %v", s, got, err)
		}
	}
	if f, err := vi(4).AsFloat64(); err != nil || f != 4 {
		t.Fatalf("int as float: %v %v", f, err)
	}
	if f, err := dbl.AsFloat64(); err != nil || f != 2.5 {
		t.Fatalf("double as float: %v %v", f, err)
	}
	if _, err := vb("abc").AsFloat64(); err == nil {
		t.Fatal("non numeric float should fail")
	}
}

func TestValueStringsAndRendering(t *testing.T) {
	if s, err := vnil().Strings(); err != nil || s != nil {
		t.Fatalf("nil: %v %v", s, err)
	}
	if _, err := vb("x").Strings(); err == nil {
		t.Fatal("scalar Strings should fail")
	}
	s, err := va(vb("a"), vnil(), vi(3)).Strings()
	if err != nil || len(s) != 3 || s[0] != "a" || s[1] != "" || s[2] != "3" {
		t.Fatalf("strings: %v %v", s, err)
	}
	if got := (Value{Kind: KindError, Bytes: []byte("ERR boom")}).String(); got != "(error) ERR boom" {
		t.Fatal(got)
	}
	if got := vnil().String(); got != "(nil)" {
		t.Fatal(got)
	}
	if got := va(vb("a"), va(vi(1))).String(); got != "[a [1]]" {
		t.Fatal(got)
	}
	if got := (Value{Kind: KindDouble, Int: int64(math.Float64bits(0.25))}).Str(); got != "0.25" {
		t.Fatal(got)
	}
	if got := va().Str(); got != "" {
		t.Fatalf("aggregate Str should be empty, got %q", got)
	}
}

func TestErrorTypes(t *testing.T) {
	for msg, prefix := range map[string]string{"WRONGTYPE bad kind": "WRONGTYPE", "ERR": "ERR", "": "", "MOVED 3999 127.0.0.1:7000": "MOVED"} {
		e := &Error{Msg: msg}
		if e.Prefix() != prefix || e.Error() != msg {
			t.Fatalf("%q: prefix %q error %q", msg, e.Prefix(), e.Error())
		}
	}
	err := unexpected(Value{Kind: KindError, Bytes: []byte("ERR x")})
	var se *Error
	if !errors.As(err, &se) || se.Msg != "ERR x" {
		t.Fatalf("error reply should surface as *Error, got %T", err)
	}
	err = unexpected(vi(1))
	var ue *UnexpectedReplyError
	if !errors.As(err, &ue) || ue.Error() != "ruvio: unexpected reply 1" {
		t.Fatalf("got %v", err)
	}
}

func TestParseMalformedAndIncomplete(t *testing.T) {
	protocol := []string{"*x\r\n", ":\r\n", ",abc\r\n", "$2\r\nabcd\r\n"}
	for _, in := range protocol {
		_, _, err := ParseReply([]byte(in))
		if err == nil {
			t.Fatalf("%q: expected an error", in)
		}
	}
	for _, in := range []string{"", "+OK", "+OK\r", "$3\r\nab", "*2\r\n:1\r\n", "%1\r\n+a\r\n", "|1\r\n+k\r\n+v\r\n"} {
		if _, _, err := ParseReply([]byte(in)); err != errIncomplete {
			t.Fatalf("%q: want incomplete, got %v", in, err)
		}
	}
	// Nested aggregates deeper than a few levels still parse.
	deep := ""
	for i := 0; i < 20; i++ {
		deep += "*1\r\n"
	}
	deep += ":1\r\n"
	if v, n, err := ParseReply([]byte(deep)); err != nil || n != len(deep) || v.Kind != KindArray {
		t.Fatalf("deep: %v %v", n, err)
	}
	// Two replies in one buffer: only the first is consumed.
	_, n, err := ParseReply([]byte("+A\r\n+B\r\n"))
	if err != nil || n != 4 {
		t.Fatalf("pipelined: %d %v", n, err)
	}
}
