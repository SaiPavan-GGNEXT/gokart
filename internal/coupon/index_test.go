package coupon

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestIndex(t *testing.T, codes ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "coupons.idx")
	meta := IndexMeta{BuiltAt: time.Now().UTC(), ToolVersion: "test"}
	if err := WriteIndex(path, meta, codes); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}
	return path
}

func TestIndexRoundtrip(t *testing.T) {
	path := writeTestIndex(t, "HAPPYHRS", "FIFTYOFF", "OVER9000", "ABCDEFGH12")
	idx, err := LoadIndex(path)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	if idx.Count() != 4 {
		t.Fatalf("Count = %d, want 4", idx.Count())
	}
	for _, c := range []string{"HAPPYHRS", "FIFTYOFF", "OVER9000", "ABCDEFGH12"} {
		if !idx.Contains(c) {
			t.Errorf("Contains(%q) = false, want true", c)
		}
	}
	for _, c := range []string{"SUPER100", "MOODYHRS", "NOPE1234"} {
		if idx.Contains(c) {
			t.Errorf("Contains(%q) = true, want false", c)
		}
	}
}

// The padding trap: with NUL padding, the invalid 10-char input "OVER900000"
// must never match the valid 8-char record "OVER9000" (which it would with
// ASCII '0' padding — a false accept that hands out discounts).
func TestIndexPaddingUnambiguous(t *testing.T) {
	idx, err := LoadIndex(writeTestIndex(t, "OVER9000"))
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Contains("OVER9000") {
		t.Fatal("valid code rejected")
	}
	for _, evil := range []string{"OVER90000", "OVER900000"} {
		if idx.Contains(evil) {
			t.Fatalf("padding ambiguity: %q accepted", evil)
		}
	}
}

func TestIndexRejectsCorruption(t *testing.T) {
	path := writeTestIndex(t, "HAPPYHRS", "FIFTYOFF")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "trunc.idx")
		if err := os.WriteFile(p, raw[:len(raw)-5], 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadIndex(p); err == nil {
			t.Fatal("truncated index loaded without error")
		}
	})

	t.Run("bit-flip in records", func(t *testing.T) {
		mut := append([]byte(nil), raw...)
		mut[len(mut)-1] ^= 0xFF // flip a byte inside the records section
		p := filepath.Join(t.TempDir(), "flip.idx")
		if err := os.WriteFile(p, mut, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadIndex(p); err == nil {
			t.Fatal("corrupted index loaded without error (CRC must catch this)")
		}
	})

	t.Run("wrong magic", func(t *testing.T) {
		mut := append([]byte(nil), raw...)
		copy(mut, "NOTANIDX")
		p := filepath.Join(t.TempDir(), "magic.idx")
		if err := os.WriteFile(p, mut, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadIndex(p); err == nil {
			t.Fatal("foreign file loaded as index")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadIndex(filepath.Join(t.TempDir(), "absent.idx")); err == nil {
			t.Fatal("missing index must be an error (fail-fast startup depends on it)")
		}
	})
}

func TestWriteIndexRefusesMalformedCodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.idx")
	err := WriteIndex(path, IndexMeta{}, []string{"lowercase"})
	if err == nil {
		t.Fatal("malformed code written to index")
	}
}

func BenchmarkIndexContains(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.idx")
	if err := WriteIndex(path, IndexMeta{}, []string{
		"BIRTHDAY", "BUYGETON", "FIFTYOFF", "FREEZAAA",
		"GNULINUX", "HAPPYHRS", "OVER9000", "SIXTYOFF",
	}); err != nil {
		b.Fatal(err)
	}
	idx, err := LoadIndex(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !idx.Contains("HAPPYHRS") {
			b.Fatal("miss")
		}
		if idx.Contains("SUPER100") {
			b.Fatal("false hit")
		}
	}
}
