// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkRouteFrameSet(b *testing.B) {
	buf := newCmd(3, cSET).s("user:1000").s("some value of moderate size").buf
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		routeFrame(buf)
	}
}

func BenchmarkHashSlot(b *testing.B) {
	k := []byte("user:1000:profile")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		hashSlot(k)
	}
}

const benchKeys = 256

func benchKeyNames() []string {
	ks := make([]string, benchKeys)
	for i := range ks {
		ks[i] = fmt.Sprintf("bench:%d", i)
	}
	return ks
}

// benchClient returns a cluster client (cluster=true, RUVIO_CLUSTER_ADDR) or a plain
// client on RUVIO_TEST_ADDR (cluster=false) with the bench keys written.
func benchClient(b *testing.B, cluster bool) (*Client, context.Context, []string) {
	b.Helper()
	env, opts := "RUVIO_TEST_ADDR", Options{DB: testDB}
	if cluster {
		env, opts = "RUVIO_CLUSTER_ADDR", Options{Cluster: true}
	}
	addr := os.Getenv(env)
	if addr == "" {
		b.Skip(env + " not set")
	}
	opts.Addr = addr
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	b.Cleanup(cancel)
	c, err := Connect(ctx, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { c.Close() })
	keys := benchKeyNames()
	v := strings.Repeat("v", 64)
	for _, k := range keys {
		if err := c.Set(ctx, k, v); err != nil {
			b.Fatal(err)
		}
	}
	return c, ctx, keys
}

func benchParallel(b *testing.B, cluster, write bool) {
	c, ctx, keys := benchClient(b, cluster)
	v := strings.Repeat("v", 64)
	var n atomic.Uint64
	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(n.Add(1)) * 7919
		for pb.Next() {
			i++
			k := keys[i%benchKeys]
			var err error
			if write {
				err = c.Set(ctx, k, v)
			} else {
				_, _, err = c.Get(ctx, k)
			}
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkStandaloneGetParallel(b *testing.B) { benchParallel(b, false, false) }
func BenchmarkStandaloneSetParallel(b *testing.B) { benchParallel(b, false, true) }
func BenchmarkClusterGetParallel(b *testing.B)    { benchParallel(b, true, false) }
func BenchmarkClusterSetParallel(b *testing.B)    { benchParallel(b, true, true) }

// Standalone server, cluster option on: the fallback path.
func BenchmarkFallbackGetParallel(b *testing.B) {
	addr := os.Getenv("RUVIO_TEST_ADDR")
	if addr == "" {
		b.Skip("RUVIO_TEST_ADDR not set")
	}
	ctx := context.Background()
	c, err := Connect(ctx, Options{Addr: addr, DB: testDB, Cluster: true})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	c.Set(ctx, "bench", strings.Repeat("v", 64))
	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := c.Get(ctx, "bench"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkClusterGetSerial(b *testing.B) {
	c, ctx, keys := benchClient(b, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.Get(ctx, keys[i%benchKeys]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterMGet16Split(b *testing.B) {
	c, ctx, keys := benchClient(b, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.MGet(ctx, keys[:16]...); err != nil {
			b.Fatal(err)
		}
	}
}
