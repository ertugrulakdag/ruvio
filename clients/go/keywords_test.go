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

func TestWave2WireBytesMatchStringPath(t *testing.T) {
	old := newCmd(5, cXTRIM).s("s").s("MAXLEN").t(tTILDE).s("10").buf
	if got := newCmd(5, cXTRIM).s("s").trim(TrimMaxLen.Of(10).WithMode(TrimApproximate)).buf; !bytes.Equal(got, old) {
		t.Fatalf("trim ~: %q vs %q", got, old)
	}
	old = newCmd(5, cXTRIM).s("s").s("MINID").s("=").s("1-0").buf
	if got := newCmd(5, cXTRIM).s("s").trim(TrimMinID.OfID("1-0").WithMode(TrimExact)).buf; !bytes.Equal(got, old) {
		t.Fatalf("trim =: %q vs %q", got, old)
	}
	for _, s := range []InfoSection{InfoServer, InfoStats, InfoBuffers, InfoMemory, InfoMemoryBreakdown,
		InfoKeyspace, InfoSnapshot, InfoCluster, InfoReplication, InfoCommandCounters,
		InfoLatencyStats, InfoLogs, InfoSlabs, InfoAll, InfoDefault, InfoPrometheus} {
		if !bytes.Equal(newCmd(2, cINFO).kw(s.token(), string(s)).buf, newCmd(2, cINFO).s(string(s)).buf) {
			t.Fatalf("info %s", s)
		}
	}
	for _, c := range []ClientType{ClientNormal, ClientMaster, ClientReplica, ClientPubSub} {
		if !bytes.Equal(newCmd(1, cGET).kw(c.token(), string(c)).buf, newCmd(1, cGET).s(string(c)).buf) {
			t.Fatalf("client type %s", c)
		}
	}
	if ScoreExclusive(5) != "(5" || ScoreExclusive(1.5) != "(1.5" || LexInclusive("a") != "[a" || LexExclusive("a") != "(a" {
		t.Fatal("bounds")
	}
}
