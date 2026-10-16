// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
)

// respVector is one entry of clients/testdata/resp.json. Bytes holds UTF-8 wire bytes;
// BytesHex replaces it when they are not valid UTF-8.
type respVector struct {
	Name     string          `json:"name"`
	Args     []string        `json:"args"`
	ArgsHex  []string        `json:"args_hex"`
	Bytes    *string         `json:"bytes"`
	BytesHex string          `json:"bytes_hex"`
	Value    json.RawMessage `json:"value"`
}

func (v respVector) wire(t *testing.T) []byte {
	if v.Bytes != nil {
		return []byte(*v.Bytes)
	}
	return mustHex(t, v.BytesHex)
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func loadRespVectors(t *testing.T) (encode, decode, incomplete []respVector) {
	t.Helper()
	data, err := os.ReadFile("../testdata/resp.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Encode     []respVector `json:"encode"`
		Decode     []respVector `json:"decode"`
		Incomplete []respVector `json:"incomplete"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Encode) == 0 || len(file.Decode) == 0 || len(file.Incomplete) == 0 {
		t.Fatal("no vectors")
	}
	return file.Encode, file.Decode, file.Incomplete
}

// expectedValue maps the neutral JSON value shape onto a Value.
func expectedValue(t *testing.T, raw json.RawMessage) Value {
	t.Helper()
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil || len(shape) != 1 {
		t.Fatalf("bad value %s", raw)
	}
	text := func(r json.RawMessage) string {
		var s string
		if err := json.Unmarshal(r, &s); err != nil {
			t.Fatalf("bad string %s", r)
		}
		return s
	}
	for kind, inner := range shape {
		switch kind {
		case "simple":
			return Value{Kind: KindSimple, Bytes: []byte(text(inner))}
		case "error":
			return Value{Kind: KindError, Bytes: []byte(text(inner))}
		case "int":
			digits := string(inner)
			if inner[0] == '"' {
				digits = text(inner)
			}
			n, err := strconv.ParseInt(digits, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return Value{Kind: KindInt, Int: n}
		case "bulk":
			return Value{Kind: KindBulk, Bytes: []byte(text(inner))}
		case "bulk_hex":
			return Value{Kind: KindBulk, Bytes: mustHex(t, text(inner))}
		case "null":
			return Value{Kind: KindNil}
		case "array":
			var items []json.RawMessage
			if err := json.Unmarshal(inner, &items); err != nil {
				t.Fatal(err)
			}
			out := Value{Kind: KindArray, Array: []Value{}}
			for _, item := range items {
				out.Array = append(out.Array, expectedValue(t, item))
			}
			return out
		default:
			t.Fatalf("unknown value kind %q", kind)
		}
	}
	return Value{}
}

func sameValue(a, b Value) bool {
	if a.Kind != b.Kind || a.Int != b.Int || !bytes.Equal(a.Bytes, b.Bytes) || len(a.Array) != len(b.Array) {
		return false
	}
	if (a.Array == nil) != (b.Array == nil) {
		return false
	}
	for i := range a.Array {
		if !sameValue(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}

// The RESP wire vectors in clients/testdata/resp.json are shared by every client, so the six
// encoders and parsers cannot drift apart.
func TestRespVectorsEncode(t *testing.T) {
	encode, _, _ := loadRespVectors(t)
	for _, v := range encode {
		want := v.wire(t)
		var binary []any
		if v.Args != nil {
			text := make([]any, len(v.Args))
			for i, a := range v.Args {
				text[i] = a
				binary = append(binary, []byte(a))
			}
			got, err := AppendCommand(nil, text...)
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("%s: string args encode to %q (%v), want %q", v.Name, got, err, want)
			}
		} else {
			for _, a := range v.ArgsHex {
				binary = append(binary, mustHex(t, a))
			}
		}
		got, err := AppendCommand(nil, binary...)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: []byte args encode to %q (%v), want %q", v.Name, got, err, want)
		}
	}
}

func TestRespVectorsDecode(t *testing.T) {
	_, decode, _ := loadRespVectors(t)
	for _, v := range decode {
		wire := v.wire(t)
		got, n, err := ParseReply(wire)
		if err != nil {
			t.Errorf("%s: %v", v.Name, err)
			continue
		}
		if want := expectedValue(t, v.Value); !sameValue(got, want) {
			t.Errorf("%s: got %v, want %v", v.Name, got, want)
		}
		if n != len(wire) {
			t.Errorf("%s: consumed %d of %d bytes", v.Name, n, len(wire))
		}
	}
}

func TestRespVectorsIncomplete(t *testing.T) {
	_, _, incomplete := loadRespVectors(t)
	for _, v := range incomplete {
		if got, _, err := ParseReply(v.wire(t)); !errors.Is(err, errIncomplete) {
			t.Errorf("%s: got %v, %v; want a request for more data", v.Name, got, err)
		}
	}
}
