// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
)

// The command to keys vectors in clients/testdata/key_vectors.json come from the server
// parser; the router must find exactly the keys the server routes each command by.
// SPUBLISH, SSUBSCRIBE and SUNSUBSCRIBE are skipped: the router sends them by channel,
// while the file lists no keys for them.
func TestCommandKeysMatchSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../testdata/key_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Vectors []struct {
			Args []string `json:"args"`
			Keys []string `json:"keys"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	checked := 0
	for _, v := range vectors.Vectors {
		switch strings.ToUpper(v.Args[0]) {
		case "SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE":
			continue
		}
		got := CommandKeys(v.Args...)
		slices.Sort(got)
		if !slices.Equal(got, v.Keys) {
			t.Errorf("%q: keys %q, want %q", v.Args, got, v.Keys)
		}
		checked++
	}
	t.Logf("checked %d vectors", checked)
}
