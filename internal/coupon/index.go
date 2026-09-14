package coupon

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Index file format ("KUPN1"):
//
//	magic    [8]byte  "KUPNIDX1"
//	count    uint32   number of records
//	metaLen  uint32   length of the JSON metadata blob
//	crc      uint32   CRC-32 (IEEE) of the records section
//	meta     []byte   JSON (IndexMeta)
//	records  count × 10 bytes, NUL-padded codes, globally sorted bytewise
//
// Records are fixed-width so lookups are a zero-parse binary search over a
// flat byte slice. Padding is 0x00 — never ASCII '0', which is a legal code
// character and would make the encoding ambiguous (valid "OVER9000" vs the
// invalid 10-char input "OVER900000").
const (
	indexMagic = "KUPNIDX1"
	recordSize = MaxLen
)

// IndexMeta records the provenance of an index so a running server can always
// answer "which coupon truth am I serving?".
type IndexMeta struct {
	BuiltAt     time.Time    `json:"built_at"`
	ToolVersion string       `json:"tool_version"`
	Sources     []SourceMeta `json:"sources"`
	CodeCount   int          `json:"code_count"`
}

// SourceMeta describes one input file that contributed to the index.
type SourceMeta struct {
	Name          string `json:"name"`
	SHA256        string `json:"sha256"`
	Bytes         int64  `json:"bytes"`
	Lines         int64  `json:"lines"`
	KeptLines     int64  `json:"kept_lines"`     // lines that passed Normalize
	RejectedLines int64  `json:"rejected_lines"` // lines filtered out
}

// Index is an immutable, fully-loaded coupon index. Safe for concurrent use:
// after Load it is never mutated.
type Index struct {
	Meta    IndexMeta
	records []byte // Count() * recordSize, sorted
}

// LoadIndex reads and verifies an index file. Any structural problem —
// wrong magic, truncation, CRC mismatch — is a hard error: a server must
// refuse to serve coupon answers it cannot trust.
func LoadIndex(path string) (*Index, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("coupon index: %w", err)
	}
	const headerLen = 8 + 4 + 4 + 4
	if len(raw) < headerLen || string(raw[:8]) != indexMagic {
		return nil, fmt.Errorf("coupon index %s: not a %s file", path, indexMagic)
	}
	count := binary.LittleEndian.Uint32(raw[8:12])
	metaLen := binary.LittleEndian.Uint32(raw[12:16])
	crc := binary.LittleEndian.Uint32(raw[16:20])

	if uint64(len(raw)) != uint64(headerLen)+uint64(metaLen)+uint64(count)*recordSize {
		return nil, fmt.Errorf("coupon index %s: truncated or corrupt (size mismatch)", path)
	}
	meta := raw[headerLen : headerLen+int(metaLen)]
	records := raw[headerLen+int(metaLen):]
	if crc32.ChecksumIEEE(records) != crc {
		return nil, fmt.Errorf("coupon index %s: CRC mismatch — file is corrupt", path)
	}

	idx := &Index{records: records}
	if err := json.Unmarshal(meta, &idx.Meta); err != nil {
		return nil, fmt.Errorf("coupon index %s: bad metadata: %w", path, err)
	}
	// Enforce sortedness once at load so Contains can trust binary search.
	for i := recordSize; i < len(records); i += recordSize {
		if bytes.Compare(records[i-recordSize:i], records[i:i+recordSize]) > 0 {
			return nil, fmt.Errorf("coupon index %s: records not sorted", path)
		}
	}
	return idx, nil
}

// Count returns the number of codes in the index.
func (ix *Index) Count() int { return len(ix.records) / recordSize }

// Contains reports whether code (already normalized) is in the index.
func (ix *Index) Contains(code string) bool {
	if len(code) < MinLen || len(code) > MaxLen {
		return false
	}
	var key [recordSize]byte
	copy(key[:], code) // remaining bytes stay 0x00, matching record padding

	n := ix.Count()
	i := sort.Search(n, func(i int) bool {
		return bytes.Compare(ix.records[i*recordSize:(i+1)*recordSize], key[:]) >= 0
	})
	return i < n && bytes.Equal(ix.records[i*recordSize:(i+1)*recordSize], key[:])
}

// Codes returns all codes in the index (sorted). Intended for tooling
// (e.g. the Redis seeder); the hot path never needs it.
func (ix *Index) Codes() []string {
	out := make([]string, 0, ix.Count())
	for i := 0; i < len(ix.records); i += recordSize {
		rec := ix.records[i : i+recordSize]
		out = append(out, string(bytes.TrimRight(rec, "\x00")))
	}
	return out
}

// WriteIndex atomically writes an index file: temp file in the destination
// directory, fsync, rename, fsync directory. A crash at any point leaves
// either the old file or the new file — never a torn one.
func WriteIndex(path string, meta IndexMeta, codes []string) error {
	sorted := make([]string, len(codes))
	copy(sorted, codes)
	// Global bytewise sort of padded records == sort of the padded strings.
	sort.Slice(sorted, func(i, j int) bool {
		var a, b [recordSize]byte
		copy(a[:], sorted[i])
		copy(b[:], sorted[j])
		return bytes.Compare(a[:], b[:]) < 0
	})

	records := make([]byte, 0, len(sorted)*recordSize)
	for _, c := range sorted {
		if _, ok := Normalize(c); !ok {
			return fmt.Errorf("refusing to write malformed code %q to index", c)
		}
		var rec [recordSize]byte
		copy(rec[:], c)
		records = append(records, rec[:]...)
	}

	meta.CodeCount = len(sorted)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	buf.WriteString(indexMagic)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(sorted)))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(metaJSON)))
	_ = binary.Write(buf, binary.LittleEndian, crc32.ChecksumIEEE(records))
	buf.Write(metaJSON)
	buf.Write(records)

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".coupons-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename

	if _, err := tmp.Write(buf.Bytes()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// CreateTemp yields 0600; the index is a public read-only artifact and
	// must be readable by non-root runtime users (distroless, k8s).
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
