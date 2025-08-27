package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Record is a single log line (no trailing newline), backed by a batch-local arena.
// Safe to read during Handle call; invalid after Handle returns.
type Record = []byte

// Batch is a collection of records sharing the same backing arena.
// All records become invalid after Handler.Process returns.
type Batch struct {
	Records []Record
	// arena is the backing storage; kept to manage lifetime and pooling.
	arena []byte
}

// Pool arenas to avoid per-batch 2 MiB allocations.
var arenaPool sync.Pool

// helper to get an arena with the configured cap
func getArena(capacity int) []byte {
	if v := arenaPool.Get(); v != nil {
		if b, ok := v.([]byte); ok && cap(b) >= capacity {
			return b[:0]
		}
	}
	return make([]byte, 0, capacity)
}

// Introduce a package-level job type so it's used consistently across functions.
type job struct {
	batch Batch
}

// Handler processes batches. It must NOT retain references to Batch.Records after returning.
type Handler interface {
	Process(ctx context.Context, b Batch) error
}

// HandlerFunc adapter.
type HandlerFunc func(ctx context.Context, b Batch) error

func (f HandlerFunc) Process(ctx context.Context, b Batch) error { return f(ctx, b) }

// WrapWithSubstringMatcher decorates a Handler to signal when any of the provided patterns
// occur in a record. It calls onHit for each match, then forwards the batch to the wrapped handler.
// Note: Records are only valid during the call; copy if you need to retain them.
func WrapWithSubstringMatcher(
	h Handler,
	patterns []string,
	caseInsensitive bool,
	onHit func(ctx context.Context, rec []byte, pattern string),
) Handler {
	pats := make([][]byte, len(patterns))
	if caseInsensitive {
		for i, p := range patterns {
			pats[i] = bytes.ToLower([]byte(p))
		}
	} else {
		for i, p := range patterns {
			pats[i] = []byte(p)
		}
	}

	return HandlerFunc(func(ctx context.Context, b Batch) error {
		for _, rec := range b.Records {
			var hay []byte
			if caseInsensitive {
				// Lowercase copy for case-insensitive search
				hay = bytes.ToLower(rec)
			} else {
				hay = rec
			}
			for i, pat := range pats {
				if len(pat) == 0 {
					continue
				}
				if bytes.Contains(hay, pat) {
					onHit(ctx, rec, patterns[i])
				}
			}
		}
		return h.Process(ctx, b)
	})
}

// ProcessFile opens a file and streams it into batches for the handler.
func ProcessFile(ctx context.Context, path string, h Handler, cfg Config) error {
	cc := cfg.withDefaults()
	if cc.Shards > 1 {
		// Use newline-aligned sharded reading for plain files.
		return processFileSharded(ctx, path, h, cc)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return ProcessReader(ctx, f, h, cc)
}

// ProcessReader streams an io.Reader using batching semantics.
// If GzipAutoDetect is enabled and the stream looks like gzip, it will be wrapped accordingly.
func ProcessReader(ctx context.Context, r io.Reader, h Handler, cfg Config) error {
	cc := cfg.withDefaults()

	// Optional gzip auto-detect: peek first 2 bytes
	if cc.GzipAutoDetect {
		br := bufio.NewReaderSize(r, 2*1024)
		hdr, err := br.Peek(2)
		if err == nil && len(hdr) >= 2 && hdr[0] == 0x1f && hdr[1] == 0x8b {
			gzr, gzErr := gzip.NewReader(br)
			if gzErr != nil {
				return gzErr
			}
			defer gzr.Close()
			r = gzr
		} else {
			r = br
		}
	}

	reader := bufio.NewReaderSize(r, cc.ReaderBufferBytes)

	// REMOVE the local 'type job struct' if present; use the package-level 'job' type.
	jobs := make(chan job, cc.ChannelDepth)
	errCh := make(chan error, cc.Workers+1)

	// Workers (errgroup-based)
	var g errgroup.Group
	for i := 0; i < cc.Workers; i++ {
		g.Go(func() error {
			for j := range jobs {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := h.Process(ctx, j.batch); err != nil {
					return err
				}
				// return arena to pool after processing
				if j.batch.arena != nil {
					arenaPool.Put(j.batch.arena[:0])
				}
			}
			return nil
		})
	}

	// Reader/producer
	readErr := readAndDispatch(ctx, reader, jobs, errCh, cc)

	// Close jobs and wait
	close(jobs)
	workersErr := g.Wait()

	// Prefer first processing error if any
	select {
	case perr := <-errCh:
		return perr
	default:
	}
	if workersErr != nil {
		return workersErr
	}
	return readErr
}

var errLineTooLong = errors.New("line exceeds MaxLineBytes")

// readAndDispatch reads lines and sends batches into jobs channel.
func readAndDispatch(
	ctx context.Context,
	r *bufio.Reader,
	jobs chan<- job, // <- changed: use named job type instead of anonymous struct
	errCh chan<- error,
	cfg Config,
) error {
	batchBytesCap := cfg.BatchBytes
	if batchBytesCap < 256 {
		batchBytesCap = 256
	}
	var (
		arena        = getArena(batchBytesCap) // <- use pool
		records      = make([][]byte, 0, 1024)
		inBatchBytes int
		lastFlush    time.Time
	)

	flush := func(force bool) error {
		if len(records) == 0 {
			return nil
		}
		b := Batch{
			Records: records,
			arena:   arena,
		}
		select {
		case jobs <- job{batch: b}:
			// obtain a new arena for next batch (pooled)
			arena = getArena(batchBytesCap)
			records = records[:0]
			inBatchBytes = 0
			lastFlush = time.Now()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			// backpressure: block until a slot is available
		}
		select {
		case jobs <- job{batch: b}:
			arena = getArena(batchBytesCap)
			records = records[:0]
			inBatchBytes = 0
			lastFlush = time.Now()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Helper that appends a line to current batch arena without extra allocs.
	appendLine := func(line []byte) {
		// Ensure room for line
		need := len(line)
		oldLen := len(arena)
		if cap(arena)-oldLen < need {
			// grow capacity geometrically to reduce copies
			newCap := cap(arena)*2 + need
			if newCap < oldLen+need {
				newCap = oldLen + need
			}
			tmp := make([]byte, oldLen, newCap)
			copy(tmp, arena)
			arena = tmp
		}
		arena = arena[:oldLen+need]
		copy(arena[oldLen:], line)
		records = append(records, arena[oldLen:oldLen+need])
		inBatchBytes += need
	}

	// Read loop
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Periodic flush (latency control)
		if cfg.FlushInterval > 0 && !lastFlush.IsZero() && time.Since(lastFlush) >= cfg.FlushInterval {
			if err := flush(true); err != nil {
				return err
			}
		}

		// Read until newline or EOF. We avoid bufio.Scanner to bypass token limits.
		line, err := readLine(r, cfg.MaxLineBytes, cfg.TruncateLongLines)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(line) > 0 && !cfg.InputHasNewlineAtEOF {
					appendLine(line)
				}
				// Final flush
				return flush(true)
			}
			if errors.Is(err, errLineTooLong) {
				select {
				case errCh <- err:
				default:
				}
				return err
			}
			return err
		}

		// Append line to batch
		appendLine(line)

		// Should we flush?
		shouldFlush := false
		if cfg.BatchBytes > 0 && inBatchBytes >= cfg.BatchBytes {
			shouldFlush = true
		}
		if !shouldFlush && cfg.BatchCount > 0 && len(records) >= cfg.BatchCount {
			shouldFlush = true
		}
		if shouldFlush {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
}

// readLine reads a single line without the trailing newline using ReadSlice to avoid
// per-line allocations from ReadBytes. It handles LF/CRLF and enforces MaxLineBytes.
// It may allocate only when a line exceeds the internal read buffer (spans multiple slices).
func readLine(r *bufio.Reader, max int, truncate bool) ([]byte, error) {
	var (
		buf   []byte // grows only if the line spans multiple slices
		isEOF bool
		total int
	)

	for {
		// Read a slice up to and including '\n' (no alloc when found within buffer)
		part, err := r.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, bufio.ErrBufferFull) {
				// No newline yet; part is a fragment. Append to buf and continue.
				buf = append(buf, part...)
				total += len(part)
				// Max check while accumulating
				if max > 0 && total > max && !truncate {
					return nil, errLineTooLong
				}
				continue
			}
			if errors.Is(err, io.EOF) {
				isEOF = true
				// Append any remaining bytes
				if len(part) > 0 {
					buf = append(buf, part...)
					total += len(part)
				}
				break
			}
			return nil, err
		}

		// We got a line (with '\n' at the end)
		if len(buf) == 0 {
			// Fast path: whole line is in 'part'; trim newline/CR without extra allocs
			line := part
			// Trim trailing '\n'
			if n := len(line); n > 0 && line[n-1] == '\n' {
				line = line[:n-1]
				// Trim optional '\r'
				if n := len(line); n > 0 && line[n-1] == '\r' {
					line = line[:n-1]
				}
			}
			// Enforce max
			if max > 0 && len(line) > max {
				if truncate {
					return line[:max], nil
				}
				return nil, errLineTooLong
			}
			return line, nil
		}

		// Slow path: line spanned multiple buffer loads; accumulate and finalize
		buf = append(buf, part...)
		total += len(part)
		break
	}

	// Finalize accumulated buffer: trim newline/CR if present
	if len(buf) > 0 && buf[len(buf)-1] == '\n' {
		buf = buf[:len(buf)-1]
		if len(buf) > 0 && buf[len(buf)-1] == '\r' {
			buf = buf[:len(buf)-1]
		}
	}

	// Enforce max
	if max > 0 && len(buf) > max {
		if truncate {
			buf = buf[:max]
		} else {
			return nil, errLineTooLong
		}
	}

	// EOF semantics
	if isEOF {
		if len(buf) == 0 {
			return nil, io.EOF
		}
		return buf, io.EOF
	}
	return buf, nil
}

// Sharded reading for plain text files (newline-aligned shards).
type shard struct {
	start int64
	end   int64
}

func shardFileByNewlines(path string, shards int) ([]shard, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if shards <= 1 || size == 0 {
		return []shard{{start: 0, end: size}}, nil
	}

	// Initial equal-size splits
	parts := make([]shard, shards)
	chunk := size / int64(shards)
	var s int64
	for i := 0; i < shards; i++ {
		e := s + chunk
		if i == shards-1 {
			e = size
		}
		parts[i] = shard{start: s, end: e}
		s = e
	}

	// Adjust boundaries to newline (except first shard)
	buf := make([]byte, 64*1024)
	for i := 1; i < len(parts); i++ {
		pos := parts[i].start
		for {
			if pos >= size {
				parts[i-1].end = size
				parts[i].start = size
				break
			}
			n, _ := f.ReadAt(buf, pos)
			if n == 0 {
				parts[i-1].end = size
				parts[i].start = size
				break
			}
			found := -1
			for j := 0; j < n; j++ {
				if buf[j] == '\n' {
					found = j
					break
				}
			}
			if found >= 0 {
				ns := pos + int64(found) + 1
				parts[i-1].end = ns
				parts[i].start = ns
				break
			}
			pos += int64(n)
		}
	}
	return parts, nil
}

func processFileSharded(ctx context.Context, path string, h Handler, cfg Config) error {
	parts, err := shardFileByNewlines(path, cfg.Shards)
	if err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	jobs := make(chan job, cfg.ChannelDepth)

	// Workers (errgroup-based)
	var gWorkers errgroup.Group
	for i := 0; i < cfg.Workers; i++ {
		gWorkers.Go(func() error {
			for j := range jobs {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := h.Process(ctx, j.batch); err != nil {
					return err
				}
				// return arena to pool after processing
				if j.batch.arena != nil {
					arenaPool.Put(j.batch.arena[:0])
				}
			}
			return nil
		})
	}

	// Producers: one goroutine per shard
	var gProd errgroup.Group
	for _, p := range parts {
		p := p
		gProd.Go(func() error {
			section := io.NewSectionReader(f, p.start, p.end-p.start)
			br := bufio.NewReaderSize(section, cfg.ReaderBufferBytes)

			batchBytesCap := cfg.BatchBytes
			if batchBytesCap < 256 {
				batchBytesCap = 256
			}
			arena := getArena(batchBytesCap) // <- use pool
			records := make([][]byte, 0, 1024)
			inBatchBytes := 0
			lastFlush := time.Time{}

			flush := func(force bool) error {
				if len(records) == 0 {
					return nil
				}
				b := Batch{Records: records, arena: arena}
				// send (with optional blocking)...
				if force {
					select {
					case jobs <- job{batch: b}:
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					select {
					case jobs <- job{batch: b}:
					case <-ctx.Done():
						return ctx.Err()
					default:
						select {
						case jobs <- job{batch: b}:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				// get a fresh (pooled) arena for next batch
				arena = getArena(batchBytesCap)
				records = records[:0]
				inBatchBytes = 0
				lastFlush = time.Now()
				return nil
			}

			appendLine := func(line []byte) {
				need := len(line)
				oldLen := len(arena)
				if cap(arena)-oldLen < need {
					newCap := cap(arena)*2 + need
					if newCap < oldLen+need {
						newCap = oldLen + need
					}
					tmp := make([]byte, oldLen, newCap)
					copy(tmp, arena)
					arena = tmp
				}
				arena = arena[:oldLen+need]
				copy(arena[oldLen:], line)
				records = append(records, arena[oldLen:oldLen+need])
				inBatchBytes += need
			}

			for {
				if cfg.FlushInterval > 0 && !lastFlush.IsZero() && time.Since(lastFlush) >= cfg.FlushInterval {
					if err := flush(true); err != nil {
						return err
					}
				}
				line, err := br.ReadBytes('\n')
				if len(line) > 0 {
					n := len(line)
					if n > 0 && line[n-1] == '\n' {
						n--
						if n > 0 && line[n-1] == '\r' {
							n--
						}
					}
					if cfg.MaxLineBytes > 0 && n > cfg.MaxLineBytes {
						if cfg.TruncateLongLines {
							n = cfg.MaxLineBytes
						} else {
							return errLineTooLong
						}
					}
					appendLine(line[:n])
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				shouldFlush := false
				if cfg.BatchBytes > 0 && inBatchBytes >= cfg.BatchBytes {
					shouldFlush = true
				}
				if !shouldFlush && cfg.BatchCount > 0 && len(records) >= cfg.BatchCount {
					shouldFlush = true
				}
				if shouldFlush {
					if err := flush(false); err != nil {
						return err
					}
				}
			}
			return flush(true)
		})
	}

	// Wait for producers, then close jobs and wait for workers
	if perr := gProd.Wait(); perr != nil {
		// Producers failed: ensure workers can exit and return the error
		close(jobs)
		_ = gWorkers.Wait()
		return perr
	}
	close(jobs)
	if werr := gWorkers.Wait(); werr != nil {
		return werr
	}
	return nil
}
