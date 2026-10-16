// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"encoding/json"
	"os"
	"testing"
)

// The key to hash slot vectors in clients/testdata/slots.json are shared by the server and
// every client.
func TestKeySlotMatchesSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../testdata/slots.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Keys []struct {
			Key  string `json:"key"`
			Slot int    `json:"slot"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Keys) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range vectors.Keys {
		if got := KeySlot(v.Key); got != v.Slot {
			t.Errorf("KeySlot(%q) = %d, want %d", v.Key, got, v.Slot)
		}
	}
}
