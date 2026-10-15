// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"strconv"
	"strings"
	"testing"
)

var sinkValue Value

func BenchmarkEncodeSetTyped(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := newCmd(5, cSET).s("user:1000").s("some value of moderate size").t(tPX).i(60000)
		putReq(r)
	}
}

func BenchmarkEncodeDoGeneric(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, 0, 256)
	for i := 0; i < b.N; i++ {
		buf, _ = AppendCommand(buf[:0], "SET", "user:1000", "some value of moderate size", "PX", 60000)
	}
}

func benchParse(b *testing.B, in []byte) {
	b.ReportAllocs()
	b.SetBytes(int64(len(in)))
	var p parser
	for i := 0; i < b.N; i++ {
		v, _, _, _ := p.next(in)
		sinkValue = v
	}
}

func BenchmarkParseInt(b *testing.B) { benchParse(b, []byte(":1234567\r\n")) }
func BenchmarkParseOK(b *testing.B)  { benchParse(b, []byte("+OK\r\n")) }
func BenchmarkParseBulk64(b *testing.B) {
	benchParse(b, []byte("$64\r\n"+strings.Repeat("v", 64)+"\r\n"))
}
func BenchmarkParseArray10(b *testing.B) {
	s := "*10\r\n"
	for i := 0; i < 10; i++ {
		s += "$6\r\nmember\r\n"
	}
	benchParse(b, []byte(s))
}

func BenchmarkLiveGetSerial(b *testing.B) {
	c, ctx := live(b)
	c.Set(ctx, "bench", strings.Repeat("v", 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.Get(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLiveSetSerial(b *testing.B) {
	c, ctx := live(b)
	v := strings.Repeat("v", 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(ctx, "bench", v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLiveGetParallel(b *testing.B) {
	c, ctx := live(b)
	c.Set(ctx, "bench", strings.Repeat("v", 64))
	b.ReportAllocs()
	b.SetParallelism(16) // 16 x GOMAXPROCS goroutines share one connection
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := c.Get(ctx, "bench"); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	st := c.Stats()
	b.ReportMetric(float64(st.Writes)/float64(b.N), "writes/op")
}

func BenchmarkLiveSetParallel(b *testing.B) {
	c, ctx := live(b)
	v := strings.Repeat("v", 64)
	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Set(ctx, "bench", v); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	st := c.Stats()
	b.ReportMetric(float64(st.Writes)/float64(b.N), "writes/op")
}

func BenchmarkLivePipelineGet16(b *testing.B) {
	c, ctx := live(b)
	c.Set(ctx, "bench", strings.Repeat("v", 64))
	keys := make([]string, 16)
	for i := range keys {
		keys[i] = "bench" + strconv.Itoa(i%1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	p := c.Pipeline()
	for i := 0; i < b.N; i += 16 {
		for _, k := range keys {
			p.Get(k)
		}
		if _, err := p.Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
