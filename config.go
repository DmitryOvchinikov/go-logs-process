package main

import "time"

type Config struct {
	// Workers is the number of parallel processing workers.
	// Typical: num CPU cores or 2x cores if the handler is I/O bound.
	Workers int

	// BatchBytes is the target size in bytes for a batch arena.
	// Lines are appended until BatchBytes or BatchCount triggers a flush.
	// Default: 1 << 20 (1 MiB).
	BatchBytes int

	// BatchCount is the max number of lines per batch (0 means no limit).
	// Default: 0 (no limit, only BatchBytes is considered).
	BatchCount int

	// ReaderBufferBytes controls the size of the bufio reader.
	// Default: 1 << 20 (1 MiB).
	ReaderBufferBytes int

	// MaxLineBytes is the max-permitted line length; oversized lines are truncated
	// or cause error depending on TruncateLongLines flag.
	// Default: 1 << 20 (1 MiB).
	MaxLineBytes int

	// TruncateLongLines controls the behavior for lines exceeding MaxLineBytes.
	// If true, the line is truncated to MaxLineBytes; if false, it returns an error.
	TruncateLongLines bool

	// GzipAutoDetect toggles auto-detection of gzip input.
	// If true, reads first bytes and wraps a gzip reader when needed.
	GzipAutoDetect bool

	// InputHasNewlineAtEOF indicates whether the input is guaranteed to end with a newline.
	// If false, the final partial line (if any) will be emitted as a record.
	InputHasNewlineAtEOF bool

	// ChannelDepth controls queued batches between reader and workers.
	// Default: Workers * 2 (min 2).
	ChannelDepth int

	// If the handler is slow, you can set FlushInterval to force periodic
	// flushing of partial batches to improve latency (optional).
	FlushInterval time.Duration

	// Shards controls how many concurrent input producers read the file in parallel.
	// When Shards <= 1, a single producer reads sequentially.
	// Only applicable for plain (uncompressed) files.
	Shards int
}

func (c *Config) withDefaults() Config {
	cc := *c
	if cc.Workers <= 0 {
		cc.Workers = 4
	}
	if cc.BatchBytes <= 0 {
		cc.BatchBytes = 1 << 20 // 1 MiB
	}
	if cc.ReaderBufferBytes <= 0 {
		cc.ReaderBufferBytes = 1 << 20
	}
	if cc.MaxLineBytes <= 0 {
		cc.MaxLineBytes = 1 << 20
	}
	if cc.ChannelDepth <= 0 {
		d := cc.Workers * 2
		if d < 2 {
			d = 2
		}
		cc.ChannelDepth = d
	}
	// Default to single-producer if not specified.
	if cc.Shards < 0 {
		cc.Shards = 1
	}
	if cc.Shards == 0 {
		cc.Shards = 1
	}
	return cc
}
