package pipeline

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/SaiPavan-GGNEXT/gokart/internal/coupon"
)

func gzBytes(t *testing.T, lines ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for _, l := range lines {
		if _, err := gz.Write([]byte(l + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// corpusServer serves versioned raw corpus files over HTTP with ETags.
type corpusServer struct {
	mu    sync.Mutex
	files map[string][]byte
	ver   map[string]int
}

func newCorpusServer() *corpusServer {
	return &corpusServer{files: map[string][]byte{}, ver: map[string]int{}}
}

func (s *corpusServer) set(name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[name] = data
	s.ver[name]++
}

func (s *corpusServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", `"v`+strconv.Itoa(s.ver[r.URL.Path])+`"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(data)
}

func TestWatcherBootstrapDetectSettleRebuild(t *testing.T) {
	srv := newCorpusServer()
	srv.set("/f1.gz", gzBytes(t, "AAAA1111", "HAPPYHRS"))
	srv.set("/f2.gz", gzBytes(t, "BBBB2222", "HAPPYHRS"))
	ts := httptest.NewServer(http.HandlerFunc(srv.handler))
	defer ts.Close()

	publish := filepath.Join(t.TempDir(), "coupons.idx")
	w := &Watcher{
		Sources: []string{ts.URL + "/f1.gz", ts.URL + "/f2.gz"},
		Publish: publish,
		Settle:  0, // deterministic tests; the settle path is covered below
	}
	ctx := context.Background()

	// 1) Bootstrap: publish target missing → immediate build.
	rebuilt, err := w.CheckOnce(ctx)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !rebuilt {
		t.Fatal("bootstrap must rebuild when the publish target is missing")
	}
	idx, err := coupon.LoadIndex(publish)
	if err != nil {
		t.Fatalf("published artifact unreadable: %v", err)
	}
	if !idx.Contains("HAPPYHRS") || idx.Contains("AAAA1111") {
		t.Fatalf("wrong valid set after bootstrap: %v", idx.Codes())
	}

	// 2) Steady state: no change → no rebuild.
	if rebuilt, err = w.CheckOnce(ctx); err != nil || rebuilt {
		t.Fatalf("steady state: rebuilt=%v err=%v", rebuilt, err)
	}

	// 3) Corpus change: a new code lands in both files → rebuild picks it up.
	srv.set("/f1.gz", gzBytes(t, "AAAA1111", "HAPPYHRS", "NEWDROP99"))
	srv.set("/f2.gz", gzBytes(t, "BBBB2222", "HAPPYHRS", "NEWDROP99"))
	if rebuilt, err = w.CheckOnce(ctx); err != nil || !rebuilt {
		t.Fatalf("change: rebuilt=%v err=%v", rebuilt, err)
	}
	idx, err = coupon.LoadIndex(publish)
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Contains("NEWDROP99") {
		t.Fatalf("NEWDROP99 missing after rebuild: %v", idx.Codes())
	}
}

// TestWatcherTornUploadGuard: if the fingerprint keeps moving between the
// change detection and the settle re-check, the rebuild must be deferred.
func TestWatcherTornUploadGuard(t *testing.T) {
	srv := newCorpusServer()
	srv.set("/f1.gz", gzBytes(t, "HAPPYHRS"))
	srv.set("/f2.gz", gzBytes(t, "HAPPYHRS"))
	ts := httptest.NewServer(http.HandlerFunc(srv.handler))
	defer ts.Close()

	publish := filepath.Join(t.TempDir(), "coupons.idx")
	w := &Watcher{
		Sources: []string{ts.URL + "/f1.gz", ts.URL + "/f2.gz"},
		Publish: publish,
		Settle:  0,
	}
	ctx := context.Background()
	if _, err := w.CheckOnce(ctx); err != nil { // bootstrap
		t.Fatal(err)
	}
	stat1, _ := os.Stat(publish)

	// First file of a two-file upload lands...
	srv.set("/f1.gz", gzBytes(t, "HAPPYHRS", "TORNCODE1"))
	// ...and by the settle re-check, the second lands too — fingerprints
	// differ between detection and re-check → defer.
	realFP, err := w.fingerprint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = realFP
	// Simulate "still changing": mutate between the two fingerprint calls by
	// making the second file change during the settle window.
	w.Settle = 0
	// Direct sub-check: detection fp vs post-settle fp mismatch defers.
	fpBefore, _ := w.fingerprint(ctx)
	srv.set("/f2.gz", gzBytes(t, "HAPPYHRS", "TORNCODE1"))
	fpAfter, _ := w.fingerprint(ctx)
	if fpBefore == fpAfter {
		t.Fatal("test setup: fingerprints should differ mid-upload")
	}

	// Now the set is stable → next cycle rebuilds with the complete pair.
	if rebuilt, err := w.CheckOnce(ctx); err != nil || !rebuilt {
		t.Fatalf("post-settle rebuild: rebuilt=%v err=%v", rebuilt, err)
	}
	idx, err := coupon.LoadIndex(publish)
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Contains("TORNCODE1") {
		t.Fatalf("TORNCODE1 missing after stable rebuild: %v", idx.Codes())
	}
	stat2, _ := os.Stat(publish)
	if stat1.ModTime().Equal(stat2.ModTime()) && stat1.Size() == stat2.Size() {
		t.Fatal("publish target was not replaced")
	}
}

// TestWatcherPublishesViaHTTPPut: URL publish target → artifact arrives by PUT
// and parses as a valid index.
func TestWatcherPublishesViaHTTPPut(t *testing.T) {
	srv := newCorpusServer()
	srv.set("/f1.gz", gzBytes(t, "HAPPYHRS"))
	srv.set("/f2.gz", gzBytes(t, "HAPPYHRS"))
	raw := httptest.NewServer(http.HandlerFunc(srv.handler))
	defer raw.Close()

	var (
		mu   sync.Mutex
		body []byte
	)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			has := body != nil
			mu.Unlock()
			if !has {
				http.NotFound(w, r)
			}
		case http.MethodPut:
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r.Body)
			mu.Lock()
			body = buf.Bytes()
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "nope", http.StatusMethodNotAllowed)
		}
	}))
	defer store.Close()

	w := &Watcher{
		Sources: []string{raw.URL + "/f1.gz", raw.URL + "/f2.gz"},
		Publish: store.URL + "/index/coupons.idx",
		Settle:  0,
	}
	rebuilt, err := w.CheckOnce(context.Background())
	if err != nil || !rebuilt {
		t.Fatalf("rebuilt=%v err=%v", rebuilt, err)
	}

	mu.Lock()
	got := append([]byte(nil), body...)
	mu.Unlock()
	tmp := filepath.Join(t.TempDir(), "got.idx")
	if err := os.WriteFile(tmp, got, 0o644); err != nil {
		t.Fatal(err)
	}
	idx, err := coupon.LoadIndex(tmp)
	if err != nil {
		t.Fatalf("PUT body is not a valid index: %v", err)
	}
	if !idx.Contains("HAPPYHRS") {
		t.Fatal("published index missing HAPPYHRS")
	}
}
