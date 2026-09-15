package coupon

import (
	"bufio"
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
// an index file. Three passes, flat memory regardless of corpus size:
//
//	pass 1  stream each file → Normalize → FNV-1a 64 → scatter hashes into
//	        per-(file,partition) spill files on disk (top byte = partition)
//	pass 2  per partition: dedup hashes per file, keep hashes seen in ≥2
//	        files → candidate hashes (a strict superset of the truth)
//	pass 3  re-stream files, resolve candidate hashes to actual strings,
//	        keep strings truly present in ≥2 files → exact final set
//
// Hash collisions can only ever ADD candidates in pass 2; pass 3 decides on
// exact strings, so the output is provably exact, not probabilistic.
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

// maxSources bounds the per-file bitmask (uint8) used in passes 2 and 3.
const maxSources = 8

// spillFlushCodes is how many hashes buffer in RAM per (file,partition)
// before appending to the spill file: 32Ki × 8 B × 256 parts ≈ 64 MiB/file.
const spillFlushCodes = 32 * 1024

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

	// ---- pass 1: scatter ------------------------------------------------
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

	// ---- pass 2: count per partition ------------------------------------
	// Partitions are independent by construction (equal hashes always land in
	// the same one), so they are counted concurrently. Results are collected
	// per partition and merged in index order, keeping output deterministic.
	p2start := time.Now()
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	perPartition := make([][]uint64, parts)
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
	var candidates []uint64
	for _, part := range perPartition {
		candidates = append(candidates, part...)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	logf("pass2 done in %s (%d workers): %d candidate hashes",
		time.Since(p2start).Round(time.Millisecond), workers, len(candidates))

	// ---- pass 3: verify with exact strings -------------------------------
	p3start := time.Now()
	perSource := make([]map[string]struct{}, len(opts.Sources))
	if err := forEachSource(ctx, opts.Sources, func(i int, path string) error {
		found, err := resolveCandidates(ctx, path, candidates)
		if err != nil {
			return fmt.Errorf("pass 3 (%s): %w", filepath.Base(path), err)
		}
		perSource[i] = found
		return nil
	}); err != nil {
		return nil, err
	}

	membership := map[string]uint8{}
	for src, set := range perSource {
		for code := range set {
			membership[code] |= 1 << uint8(src)
		}
	}
	var final []string
	for code, mask := range membership {
		if bits.OnesCount8(mask) >= 2 {
			final = append(final, code)
		}
	}
	sort.Strings(final)
	logf("pass3 done in %s: %d exact valid codes", time.Since(p3start).Round(time.Millisecond), len(final))

	meta := IndexMeta{
		BuiltAt:     time.Now().UTC(),
		ToolVersion: "indexer/1.0",
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

// countPartition loads one partition's spilled hashes from every source and
// returns the hashes present in at least two distinct sources.
//
// The per-file bitmask is where the "found in ≥2 files" rule lives: OR-ing the
// same source bit repeatedly is idempotent, so a code repeated within a single
// file still counts once — per-file dedup costs nothing.
func countPartition(numSources int, tempDir string, p int) ([]uint64, error) {
	type entry struct {
		h   uint64
		src uint8
	}
	var entries []entry
	for src := 0; src < numSources; src++ {
		hashes, err := readSpill(spillPath(tempDir, src, p))
		if err != nil {
			return nil, err
		}
		for _, h := range hashes {
			entries = append(entries, entry{h, uint8(src)})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].h < entries[j].h })

	var out []uint64
	for i := 0; i < len(entries); {
		j, mask := i, uint8(0)
		for j < len(entries) && entries[j].h == entries[i].h {
			mask |= 1 << entries[j].src
			j++
		}
		if bits.OnesCount8(mask) >= 2 {
			out = append(out, entries[i].h)
		}
		i = j
	}
	return out, nil
}

func spillPath(dir string, src, part int) string {
	return filepath.Join(dir, fmt.Sprintf("s%d_p%03d.bin", src, part))
}

// scatterFile streams one gzipped source, hashing every normalized code into
// partition spill files, while fingerprinting the compressed bytes (sha256).
func scatterFile(ctx context.Context, path string, src, parts int, tempDir string) (*SourceMeta, error) {
	meta := &SourceMeta{Name: filepath.Base(path)}

	shaw := sha256.New()
	bufs := make([][]uint64, parts)
	for i := range bufs {
		bufs[i] = make([]uint64, 0, spillFlushCodes)
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
		h := fnv1a64(code)
		p := int(h>>56) % parts
		bufs[p] = append(bufs[p], h)
		if len(bufs[p]) >= spillFlushCodes {
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

// resolveCandidates re-streams one source and returns the set of normalized
// codes whose hash is in the sorted candidates slice.
func resolveCandidates(ctx context.Context, path string, candidates []uint64) (map[string]struct{}, error) {
	found := map[string]struct{}{}
	err := streamLines(ctx, path, io.Discard, func(line []byte) error {
		code, ok := Normalize(string(line))
		if !ok {
			return nil
		}
		h := fnv1a64(code)
		i := sort.Search(len(candidates), func(i int) bool { return candidates[i] >= h })
		if i < len(candidates) && candidates[i] == h {
			found[code] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
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

func appendSpill(path string, hashes []uint64) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	buf := make([]byte, len(hashes)*8)
	for i, h := range hashes {
		binary.LittleEndian.PutUint64(buf[i*8:], h)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func readSpill(path string) ([]uint64, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil // partition never received a hash — legitimately empty
	}
	if err != nil {
		return nil, err
	}
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("spill %s: corrupt length %d", path, len(b))
	}
	out := make([]uint64, len(b)/8)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return out, nil
}
