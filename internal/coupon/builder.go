package coupon

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Build reduces the raw coupon corpus (N gzipped code-per-line files) to the
// exact set of codes present in at least two distinct files, and writes it as
// an index file. Two passes, flat memory regardless of corpus size:
//
//	pass 1  stream each file → Normalize → spill the code itself as a
//	        fixed-width NUL-padded record into a per-(file,partition) spill
//	        file on disk, routed by the top byte of the code's hash. Equal
//	        codes hash equally, so all copies of a code — from any file —
//	        land in the same partition.
//	pass 2  per partition (concurrently): sort the records from all files,
//	        OR a per-file bit for each run of equal codes; popcount ≥ 2 is
//	        the "found in at least two files" rule. Emit the code string.
//
// Because the records ARE the codes (not hashes of them), the result is
// exact by construction — there is no collision case to reason about and no
// verification pass. The hash is used only to route records to partitions;
// a collision there merely co-locates two different codes in one partition,
// where the byte-wise sort still tells them apart.
//
// (Design note: v1 spilled 8-byte hashes instead of 10-byte codes to save
// ~600 MB of scratch disk, which forced a third full corpus pass to resolve
// candidate hashes back to exact strings. Spilling the code itself deletes
// that pass — and its 12.7 s of gzip decompression — for 25% more temp disk.
// See docs/DESIGN.md for the evolution and measurements.)
//
// ANY read error — truncated gzip, CRC failure, I/O fault — aborts the whole
// build. A partial corpus must never produce a plausible-looking index.
type BuildOptions struct {
	Sources    []string // paths to .gz inputs (2..8 files)
	OutPath    string   // index file to write atomically
	TempDir    string   // scratch dir for spill files; "" = os.MkdirTemp
	Partitions int      // 0 = 256
	// Workers bounds how many partitions are counted concurrently in pass 2.
	// 0 = runtime.NumCPU(). Peak memory is roughly Workers × (corpus/Partitions),
	// so lower it on memory-constrained machines (raise Partitions instead to
	// shrink each chunk).
	Workers int
	Log     func(format string, args ...any)
}

// maxSources bounds the per-file bitmask (uint8) used in pass 2.
const maxSources = 8

// spillFlushRecords is how many codes buffer in RAM per (file,partition)
// before appending to the spill file: 16Ki × 10 B × 256 parts ≈ 40 MiB/file.
const spillFlushRecords = 16 * 1024

func Build(ctx context.Context, opts BuildOptions) (*IndexMeta, error) {
	if len(opts.Sources) < 2 {
		return nil, fmt.Errorf("build: need at least 2 source files, got %d", len(opts.Sources))
	}
	if len(opts.Sources) > maxSources {
		return nil, fmt.Errorf("build: at most %d source files supported", maxSources)
	}
	parts := opts.Partitions
	if parts <= 0 {
		parts = 256
	}
	if parts > 256 {
		return nil, fmt.Errorf("build: partitions must be ≤256 (top-byte routing)")
	}
	logf := opts.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}

	tempDir := opts.TempDir
	if tempDir == "" {
		var err error
		tempDir, err = os.MkdirTemp("", "couponidx-*")
		if err != nil {
			return nil, err
		}
	}
	defer os.RemoveAll(tempDir)

	// ---- pass 1: scatter codes into partition spill files ----------------
	start := time.Now()
	sources := make([]SourceMeta, len(opts.Sources))
	if err := forEachSource(ctx, opts.Sources, func(i int, path string) error {
		meta, err := scatterFile(ctx, path, i, parts, tempDir)
		if err != nil {
			return fmt.Errorf("pass 1 (%s): %w", filepath.Base(path), err)
		}
		sources[i] = *meta
		logf("pass1 %s: %d lines, %d kept, %d rejected, sha256=%s",
			meta.Name, meta.Lines, meta.KeptLines, meta.RejectedLines, meta.SHA256[:12])
		return nil
	}); err != nil {
		return nil, err
	}
	logf("pass1 done in %s", time.Since(start).Round(time.Millisecond))

	// ---- pass 2: count per partition, emit exact codes -------------------
	// Partitions are independent by construction (equal codes always land in
	// the same one), so they are counted concurrently. Results are collected
	// per partition and merged in index order, keeping output deterministic.
	p2start := time.Now()
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	perPartition := make([][]string, parts)
	if err := forEachPartition(ctx, parts, workers, func(p int) error {
		found, err := countPartition(len(opts.Sources), tempDir, p)
		if err != nil {
			return fmt.Errorf("pass 2 partition %d: %w", p, err)
		}
		perPartition[p] = found
		return nil
	}); err != nil {
		return nil, err
	}
	var final []string
	for _, part := range perPartition {
		final = append(final, part...)
	}
	sort.Strings(final)
	logf("pass2 done in %s (%d workers): %d exact valid codes",
		time.Since(p2start).Round(time.Millisecond), workers, len(final))

	meta := IndexMeta{
		BuiltAt:     time.Now().UTC(),
		ToolVersion: "indexer/2.0",
		Sources:     sources,
		CodeCount:   len(final),
	}
	if err := WriteIndex(opts.OutPath, meta, final); err != nil {
		return nil, fmt.Errorf("writing index: %w", err)
	}
	logf("index written to %s (%d codes) in %s total", opts.OutPath, len(final), time.Since(start).Round(time.Millisecond))
	return &meta, nil
}

// forEachSource runs fn concurrently for every source and returns the first error.
func forEachSource(ctx context.Context, sources []string, fn func(i int, path string) error) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i, path := range sources {
		wg.Add(1)
		go func(i int, path string) {
			defer wg.Done()
			if err := fn(i, path); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i, path)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// forEachPartition runs fn for every partition across a bounded worker pool,
// returning the first error. Workers pull the next partition index off a
// shared counter, so uneven partitions self-balance.
func forEachPartition(ctx context.Context, parts, workers int, fn func(p int) error) error {
	if workers > parts {
		workers = parts
	}
	if workers < 1 {
		workers = 1
	}
	var (
		next     atomic.Int64
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				p := int(next.Add(1)) - 1
				if p >= parts || ctx.Err() != nil {
					return
				}
				if err := fn(p); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// countPartition loads one partition's spilled records from every source and
// returns the codes present in at least two distinct sources — exact strings,
// decided by byte-wise comparison, so hash quality never affects the result.
//
// Records are packed as (hi uint64, lo uint16) big-endian so the sort runs on
// two integer comparisons instead of a bytes.Compare; big-endian preserves
// byte-wise ordering, and NUL padding round-trips (a code can never contain
// NUL — Normalize enforces the alphabet).
//
// The per-file bitmask is where the "found in ≥2 files" rule lives: OR-ing
// the same source bit repeatedly is idempotent, so a code repeated within a
// single file still counts once — per-file dedup costs nothing.
func countPartition(numSources int, tempDir string, p int) ([]string, error) {
	type rec struct {
		hi  uint64
		lo  uint16
		src uint8
	}
	var recs []rec
	for src := 0; src < numSources; src++ {
		b, err := readSpill(spillPath(tempDir, src, p))
		if err != nil {
			return nil, err
		}
		for i := 0; i+recordSize <= len(b); i += recordSize {
			recs = append(recs, rec{
				hi:  binary.BigEndian.Uint64(b[i : i+8]),
				lo:  binary.BigEndian.Uint16(b[i+8 : i+10]),
				src: uint8(src),
			})
		}
	}
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].hi != recs[j].hi {
			return recs[i].hi < recs[j].hi
		}
		return recs[i].lo < recs[j].lo
	})

	var out []string
	for i := 0; i < len(recs); {
		j, mask := i, uint8(0)
		for j < len(recs) && recs[j].hi == recs[i].hi && recs[j].lo == recs[i].lo {
			mask |= 1 << recs[j].src
			j++
		}
		if bits.OnesCount8(mask) >= 2 {
			var code [recordSize]byte
			binary.BigEndian.PutUint64(code[:8], recs[i].hi)
			binary.BigEndian.PutUint16(code[8:10], recs[i].lo)
			out = append(out, string(bytes.TrimRight(code[:], "\x00")))
		}
		i = j
	}
	return out, nil
}

func spillPath(dir string, src, part int) string {
	return filepath.Join(dir, fmt.Sprintf("s%d_p%03d.bin", src, part))
}

// scatterFile streams one gzipped source, spilling every normalized code as
// a fixed-width record into partition files, while fingerprinting the
// compressed bytes (sha256).
func scatterFile(ctx context.Context, path string, src, parts int, tempDir string) (*SourceMeta, error) {
	meta := &SourceMeta{Name: filepath.Base(path)}

	shaw := sha256.New()
	bufs := make([][]byte, parts)
	for i := range bufs {
		bufs[i] = make([]byte, 0, spillFlushRecords*recordSize)
	}
	flush := func(p int) error {
		if len(bufs[p]) == 0 {
			return nil
		}
		if err := appendSpill(spillPath(tempDir, src, p), bufs[p]); err != nil {
			return err
		}
		bufs[p] = bufs[p][:0]
		return nil
	}

	err := streamLines(ctx, path, shaw, func(line []byte) error {
		meta.Lines++
		code, ok := Normalize(string(line))
		if !ok {
			meta.RejectedLines++
			return nil
		}
		meta.KeptLines++
		p := int(fnv1a64(code)>>56) % parts
		var rec [recordSize]byte
		copy(rec[:], code) // remaining bytes stay 0x00 — same padding as the index
		bufs[p] = append(bufs[p], rec[:]...)
		if len(bufs[p]) >= spillFlushRecords*recordSize {
			return flush(p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for p := range bufs {
		if err := flush(p); err != nil {
			return nil, err
		}
	}

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	meta.Bytes = fi.Size()
	meta.SHA256 = hex.EncodeToString(shaw.Sum(nil))
	return meta, nil
}

// streamLines feeds every newline-delimited line of a gzipped file to fn,
// teeing the compressed bytes into sink (for fingerprinting).
//
// Failure contract: gzip CRC errors, unexpected EOF (truncation), and I/O
// errors are returned as errors — never swallowed. An overlong line (>64 KiB,
// impossible for a real code) is consumed and surfaced to fn as an empty
// line, so it is counted as one rejected line instead of aborting the build.
func streamLines(ctx context.Context, path string, sink io.Writer, fn func(line []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tee := io.TeeReader(bufio.NewReaderSize(f, 1<<20), sink)
	gz, err := gzip.NewReader(tee)
	if err != nil {
		return fmt.Errorf("gzip open: %w", err)
	}
	defer gz.Close()

	br := bufio.NewReaderSize(gz, 1<<16)
	lineNo := 0
	for {
		if lineNo%(1<<20) == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		lineNo++
		line, err := br.ReadSlice('\n')
		switch err {
		case nil:
			if e := fn(line[:len(line)-1]); e != nil {
				return e
			}
		case bufio.ErrBufferFull:
			// Pathologically long line: consume the remainder, count as one
			// (rejected) line rather than aborting the whole build.
			for err == bufio.ErrBufferFull {
				_, err = br.ReadSlice('\n')
			}
			if err != nil && err != io.EOF {
				return fmt.Errorf("read: %w", err)
			}
			if e := fn(nil); e != nil { // empty line → rejected by Normalize
				return e
			}
			if err == io.EOF {
				return drainTail(gz, tee)
			}
		case io.EOF:
			if len(line) > 0 { // final line without trailing newline
				if e := fn(line); e != nil {
					return e
				}
			}
			return drainTail(gz, tee)
		default:
			// Includes gzip checksum failures and io.ErrUnexpectedEOF from a
			// truncated download — the silent-index killer. Hard fail.
			return fmt.Errorf("read: %w", err)
		}
	}
}

// drainTail ensures the gzip stream verified its trailer (CRC) and the tee
// consumed every compressed byte, so the recorded sha256 covers the whole file.
func drainTail(gz *gzip.Reader, tee io.Reader) error {
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return fmt.Errorf("gzip trailer: %w", err)
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return fmt.Errorf("draining file: %w", err)
	}
	return nil
}

func appendSpill(path string, buf []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func readSpill(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil // partition never received a record — legitimately empty
	}
	if err != nil {
		return nil, err
	}
	if len(b)%recordSize != 0 {
		return nil, fmt.Errorf("spill %s: corrupt length %d", path, len(b))
	}
	return b, nil
}
