package ruvio

import (
	"bytes"
	"testing"
)

func TestKeywordTokensMatchStringPath(t *testing.T) {
	type kw interface {
		token() []byte
	}
	cases := []struct {
		tok kw
		raw string
	}{
		{GeoMeters, "m"}, {GeoKilometers, "km"}, {GeoFeet, "ft"}, {GeoMiles, "mi"},
		{GeoAsc, "ASC"}, {GeoDesc, "DESC"},
		{ListLeft, "LEFT"}, {ListRight, "RIGHT"},
		{InsertBefore, "BEFORE"}, {InsertAfter, "AFTER"},
		{BitAnd, "AND"}, {BitOr, "OR"}, {BitXor, "XOR"}, {BitNot, "NOT"},
		{BitUnitByte, "BYTE"}, {BitUnitBit, "BIT"},
		{AggSum, "SUM"}, {AggMin, "MIN"}, {AggMax, "MAX"},
		{BitFieldGet, "GET"}, {BitFieldSet, "SET"}, {BitFieldIncrBy, "INCRBY"}, {BitFieldOverflow, "OVERFLOW"},
		{ScanString, "string"}, {ScanJSON, "json"}, {ScanList, "list"}, {ScanSet, "set"},
		{ScanHash, "hash"}, {ScanZSet, "zset"}, {ScanBloomFilter, "bloomfilter"},
		{ScanStream, "stream"}, {ScanLease, "lease"}, {ScanLimit, "limit"},
		{ScanSemaphore, "semaphore"}, {ScanOnce, "once"},
	}
	for _, c := range cases {
		got := newCmd(1, cGET).kw(c.tok.token(), c.raw).buf
		want := newCmd(1, cGET).s(c.raw).buf
		if !bytes.Equal(got, want) {
			t.Errorf("%q: %q != %q", c.raw, got, want)
		}
	}
}

func TestKeywordUnknownFallsBack(t *testing.T) {
	got := newCmd(1, cGET).kw(GeoUnit("yd").token(), "yd").buf
	want := newCmd(1, cGET).s("yd").buf
	if !bytes.Equal(got, want) {
		t.Fatalf("%q != %q", got, want)
	}
}

func TestKeywordRequests(t *testing.T) {
	q := GeoSearchQuery{Longitude: 1, Latitude: 2, Radius: 3, Unit: GeoKilometers, Sort: GeoDesc}
	r := newCmd(2+q.n(false), cGEOSEARCH).s("k")
	q.append(r, false)
	q2 := GeoSearchQuery{Longitude: 1, Latitude: 2, Radius: 3, Unit: "km", Sort: "DESC"}
	r2 := newCmd(2+q2.n(false), cGEOSEARCH).s("k")
	q2.append(r2, false)
	if !bytes.Equal(r.buf, r2.buf) {
		t.Fatal("geosearch typed != literal")
	}
	if !bytes.Contains(r.buf, preEnc("km")) || !bytes.Contains(r.buf, preEnc("DESC")) {
		t.Fatalf("missing tokens: %q", r.buf)
	}
	bf := bitfieldReq(cBITFIELD, "k", []BitFieldOp{
		{Op: BitFieldOverflow, Type: BitOverflowSat},
		{Op: BitFieldIncrBy, Type: BitSigned(5), Offset: "#1", Value: 2},
	}).buf
	for _, w := range []string{"OVERFLOW", "SAT", "INCRBY", "i5"} {
		if !bytes.Contains(bf, preEnc(w)) {
			t.Errorf("bitfield missing %s: %q", w, bf)
		}
	}
	if BitUnsigned(63) != "u63" {
		t.Fatal("BitUnsigned")
	}
}
