package main

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"
)

type nopHandler struct{}

func (nopHandler) Process(ctx context.Context, b Batch) error {
	// Consume bytes to avoid being optimized away
	var sum int
	for _, rec := range b.Records {
		sum += len(rec)
	}
	_ = sum
	return nil
}

func BenchmarkProcessFile(b *testing.B) {
	path := "tests/big.log"

	info, err := os.Stat(path)
	if err != nil {
		b.Fatalf("stat: %v", err)
	}
	size := info.Size()
	b.SetBytes(size)

	cfg := Config{
		Workers:           runtime.NumCPU(),
		Shards:            1,       // start with single producer; try 2..8 on SSD/NVMe
		BatchBytes:        2 << 20, // 2 MiB
		ReaderBufferBytes: 2 << 20, // 2 MiB
		GzipAutoDetect:    false,   // file is plain text
		FlushInterval:     0,       // pure throughput
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := ProcessFile(ctx, path, nopHandler{}, cfg); err != nil {
			b.Fatalf("ProcessFile: %v", err)
		}
		_ = time.Since(start)
	}
}
