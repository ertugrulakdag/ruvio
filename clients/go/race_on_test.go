// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

//go:build race

package ruvio

// raceEnabled is true under -race, where sync.Pool drops a share of Puts on purpose, so a
// pool-backed path allocates now and then.
const raceEnabled = true
