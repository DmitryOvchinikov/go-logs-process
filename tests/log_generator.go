package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

func main() {
	var (
		outPath   string
		sizeStr   string
		errProb   float64
		debugProb float64
		warnProb  float64
		bufSize   int
		seed      int64
	)
	flag.StringVar(&outPath, "out", "big.log", "output file path")
	flag.StringVar(&sizeStr, "size", "10gb", "target size (e.g. 1gb, 512mb, 10gb)")
	flag.Float64Var(&errProb, "error-prob", 0.001, "probability of ERROR lines (e.g., 0.001 = 0.1%)")
	flag.Float64Var(&debugProb, "debug-prob", 0.10, "probability of DEBUG lines")
	flag.Float64Var(&warnProb, "warn-prob", 0.01, "probability of WARN lines")
	flag.IntVar(&bufSize, "buf", 4<<20, "buffer size for writer")
	flag.Int64Var(&seed, "seed", time.Now().UnixNano(), "PRNG seed")
	flag.Parse()

	targetBytes, err := parseSize(sizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid size: %v\n", err)
		os.Exit(1)
	}
	if errProb < 0 || errProb > 1 || debugProb < 0 || debugProb > 1 || warnProb < 0 || warnProb > 1 {
		fmt.Fprintln(os.Stderr, "probabilities must be in [0,1]")
		os.Exit(1)
	}
	if errProb+warnProb+debugProb > 0.99 {
		fmt.Fprintln(os.Stderr, "sum of probabilities for ERROR+WARN+DEBUG should be < 1 (leave room for INFO)")
		os.Exit(1)
	}

	rand.Seed(seed)

	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	words := []string{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
		"golf", "hotel", "india", "juliet", "kilo", "lima", "mike",
		"november", "oscar", "papa", "quebec", "romeo", "sierra",
		"tango", "uniform", "victor", "whiskey", "xray", "yankee", "zulu",
		"request", "response", "timeout", "latency", "cache", "database",
		"connection", "retry", "token", "session", "auth", "pipeline",
		"queue", "stream", "buffer", "index", "shard", "replica",
	}

	users := []string{"john.doe", "jane.smith", "admin", "svc-reporter", "ops", "alice", "bob", "charlie"}

	start := time.Now()
	var written int64
	var i int64

	for written < targetBytes {
		// Timestamp that advances with each line (just for variety)
		ts := start.Add(time.Duration(i) * time.Millisecond).Format("2006-01-02 15:04:05")

		// Pick level by probabilities
		r := rand.Float64()
		var level string
		switch {
		case r < errProb:
			level = "ERROR"
		case r < errProb+warnProb:
			level = "WARN"
		case r < errProb+warnProb+debugProb:
			level = "DEBUG"
		default:
			level = "INFO"
		}

		// Random-ish components
		user := users[rand.Intn(len(users))]
		ip := fmt.Sprintf("%d.%d.%d.%d", 10+rand.Intn(200), rand.Intn(255), rand.Intn(255), rand.Intn(255))
		msg := randomMessage(level, words)

		line := fmt.Sprintf("%s %s user=%q ip=%s %s\n", ts, level, user, ip, msg)

		n, err := w.WriteString(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			os.Exit(1)
		}
		written += int64(n)
		i++

		// Occasionally flush to keep buffers bounded during very long runs
		if i%200000 == 0 {
			if err := w.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "flush: %v\n", err)
				os.Exit(1)
			}
		}
	}

	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "final flush: %v\n", err)
		os.Exit(1)
	}
	fi, _ := f.Stat()
	fmt.Printf("Wrote %d bytes to %s (requested ~%d bytes)\n", fi.Size(), outPath, targetBytes)
}

func randomMessage(level string, words []string) string {
	// Vary length 8..40 words
	n := 8 + rand.Intn(33)
	var b strings.Builder
	switch level {
	case "ERROR":
		b.WriteString("msg=\"")
		b.WriteString("operation failed\" cause=\"")
		b.WriteString(words[rand.Intn(len(words))])
		b.WriteString(" ")
		b.WriteString(words[rand.Intn(len(words))])
		b.WriteString("\" ")
	case "WARN":
		b.WriteString("msg=\"potential issue detected\" ")
	case "DEBUG":
		b.WriteString("ctx=\"trace\" ")
	default:
		b.WriteString("msg=\"ok\" ")
	}
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[rand.Intn(len(words))])
	}
	// Append some ids/codes to diversify lengths
	b.WriteString(fmt.Sprintf(" id=%d code=%04d", rand.Intn(10_000_000), rand.Intn(10000)))
	return b.String()
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	multipliers := map[string]int64{
		"kb": 1 << 10, "k": 1 << 10,
		"mb": 1 << 20, "m": 1 << 20,
		"gb": 1 << 30, "g": 1 << 30,
		"tb": 1 << 40, "t": 1 << 40,
		"b": 1,
	}
	for suf, mul := range multipliers {
		if strings.HasSuffix(s, suf) {
			num := strings.TrimSuffix(s, suf)
			var v float64
			_, err := fmt.Sscanf(num, "%f", &v)
			if err != nil {
				return 0, err
			}
			return int64(v * float64(mul)), nil
		}
	}
	// No suffix: assume bytes
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
