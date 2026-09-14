package coupon

import (
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// writeGz writes lines as a gzipped file and returns its path.
func writeGz(t *testing.T, dir, name string, lines []string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	for _, l := range lines {
		if _, err := gz.Write([]byte(l + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestBuildEndToEnd replicates the real corpus's trap structure in miniature:
// per-file dominant lengths, valid codes planted at EOF, a code only in one
// file (SUPER100 analog), a near-miss (MOODYHRS analog), a duplicate within
// a single file (must count once), and junk lines that Normalize rejects.
func TestBuildEndToEnd(t *testing.T) {
	dir := t.TempDir()

	f1 := writeGz(t, dir, "f1.gz", []string{
		"SUPER100", // only in this file → invalid
		"AAAA1111", "BBBB2222", "CCCC3333",
		"DUPEDUPE", "DUPEDUPE", // twice in the SAME file → still one file
		"trash", "", "toolongtobeacode123456", "lower999",
		"BUYGETON", "HAPPYHRS", "FIFTYOFF", // planted at EOF
	})
	f2 := writeGz(t, dir, "f2.gz", []string{
		"DDDDD4444", "EEEEE5555",
		"MOODYHRS",             // near-miss: this file only → invalid
		"BUYGETON", "FIFTYOFF", // planted at EOF (no HAPPYHRS here)
	})
	f3 := writeGz(t, dir, "f3.gz", []string{
		"FFFFFF6666", "GGGGGG7777",
		"DUPEDUPE",             // second FILE for DUPEDUPE → now valid
		"HAPPYHRS", "FIFTYOFF", // planted at EOF
	})

	out := filepath.Join(dir, "out.idx")
	meta, err := Build(context.Background(), BuildOptions{
		Sources: []string{f1, f2, f3},
		OutPath: out,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	idx, err := LoadIndex(out)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"BUYGETON", "DUPEDUPE", "FIFTYOFF", "HAPPYHRS"}
	if got := idx.Codes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("valid set = %v, want %v", got, want)
	}
	for _, invalid := range []string{"SUPER100", "MOODYHRS", "AAAA1111", "DDDDD4444"} {
		if idx.Contains(invalid) {
			t.Errorf("%q must be invalid (present in <2 files)", invalid)
		}
	}

	// Stats: f1 has 13 lines, 4 of them junk (trash/empty/toolong/lower).
	if meta.Sources[0].Lines != 13 || meta.Sources[0].RejectedLines != 4 {
		t.Errorf("f1 stats = %+v, want 13 lines / 4 rejected", meta.Sources[0])
	}
	if meta.CodeCount != 4 {
		t.Errorf("CodeCount = %d, want 4", meta.CodeCount)
	}
}

// TestBuildFailsOnTruncatedGzip is the red-team's critical scenario: every
// real valid code lives in the LAST lines of its file, so a truncated
// download must abort the build — never emit a plausible-but-wrong index.
func TestBuildFailsOnTruncatedGzip(t *testing.T) {
	dir := t.TempDir()
	f1 := writeGz(t, dir, "f1.gz", []string{"AAAA1111", "HAPPYHRS"})
	f2 := writeGz(t, dir, "f2.gz", []string{"BBBB2222", "HAPPYHRS"})

	// Truncate f2 mid-stream.
	raw, err := os.ReadFile(f2)
	if err != nil {
		t.Fatal(err)
	}
	trunc := filepath.Join(dir, "trunc.gz")
	if err := os.WriteFile(trunc, raw[:len(raw)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out.idx")
	_, err = Build(context.Background(), BuildOptions{
		Sources: []string{f1, trunc},
		OutPath: out,
	})
	if err == nil {
		t.Fatal("Build succeeded on a truncated input — this must be a hard failure")
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatal("a partial index file was written despite the failure")
	}
}

func TestBuildRequiresTwoSources(t *testing.T) {
	dir := t.TempDir()
	f1 := writeGz(t, dir, "f1.gz", []string{"AAAA1111"})
	_, err := Build(context.Background(), BuildOptions{
		Sources: []string{f1},
		OutPath: filepath.Join(dir, "out.idx"),
	})
	if err == nil {
		t.Fatal("single-source build must fail (≥2 files rule is undefined otherwise)")
	}
}

// TestBuildOverlongLineSurvives: a pathological multi-megabyte line must be
// skipped (counted as rejected), not crash or abort the build.
func TestBuildOverlongLineSurvives(t *testing.T) {
	dir := t.TempDir()
	long := make([]byte, 200*1024)
	for i := range long {
		long[i] = 'A'
	}
	f1 := writeGz(t, dir, "f1.gz", []string{"AAAA1111", string(long), "HAPPYHRS"})
	f2 := writeGz(t, dir, "f2.gz", []string{"AAAA1111", "HAPPYHRS"})

	out := filepath.Join(dir, "out.idx")
	meta, err := Build(context.Background(), BuildOptions{
		Sources: []string{f1, f2},
		OutPath: out,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if meta.CodeCount != 2 {
		t.Fatalf("CodeCount = %d, want 2 (AAAA1111, HAPPYHRS)", meta.CodeCount)
	}
	if meta.Sources[0].RejectedLines == 0 {
		t.Fatal("overlong line was not counted as rejected")
	}
}
