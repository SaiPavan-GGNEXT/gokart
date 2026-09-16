package coupon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// indexBytes builds an index file in a temp dir and returns its raw bytes.
func indexBytes(t *testing.T, codes ...string) []byte {
	t.Helper()
	p := filepath.Join(t.TempDir(), "i.idx")
	if err := WriteIndex(p, IndexMeta{BuiltAt: time.Now().UTC()}, codes); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// swappableServer serves whatever artifact is currently set, with an ETag.
type swappableServer struct {
	mu   sync.Mutex
	body []byte
	etag string
	hits int
}

func (s *swappableServer) set(body []byte, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body, s.etag = body, etag
}

func (s *swappableServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits++
	if r.Header.Get("If-None-Match") == s.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", s.etag)
	_, _ = w.Write(s.body)
}

func TestRemoteValidatorFetchAndHotSwap(t *testing.T) {
	srv := &swappableServer{}
	srv.set(indexBytes(t, "HAPPYHRS", "FIFTYOFF"), `"v1"`)
	ts := httptest.NewServer(http.HandlerFunc(srv.handler))
	defer ts.Close()

	ctx := context.Background()
	v, err := NewRemoteValidator(ctx, ts.URL)
	if err != nil {
		t.Fatalf("initial fetch: %v", err)
	}

	if ok, _ := v.Validate(ctx, "HAPPYHRS"); !ok {
		t.Fatal("HAPPYHRS must validate against v1")
	}
	if ok, _ := v.Validate(ctx, "NEWCODE99"); ok {
		t.Fatal("NEWCODE99 must not validate against v1")
	}

	// Unchanged artifact → 304 path, index object identical.
	before := v.idx.Load()
	if err := v.reload(ctx); err != nil {
		t.Fatalf("reload (unchanged): %v", err)
	}
	if v.idx.Load() != before {
		t.Fatal("index must not be replaced on 304")
	}

	// Publish v2 → reload must hot-swap.
	srv.set(indexBytes(t, "HAPPYHRS", "NEWCODE99"), `"v2"`)
	if err := v.reload(ctx); err != nil {
		t.Fatalf("reload (v2): %v", err)
	}
	if ok, _ := v.Validate(ctx, "NEWCODE99"); !ok {
		t.Fatal("NEWCODE99 must validate after hot-swap")
	}
	if ok, _ := v.Validate(ctx, "FIFTYOFF"); ok {
		t.Fatal("FIFTYOFF was removed in v2 and must no longer validate")
	}
}

// TestRemoteValidatorFailStatic: a corrupt update must be rejected and the
// previous index must keep serving.
func TestRemoteValidatorFailStatic(t *testing.T) {
	srv := &swappableServer{}
	srv.set(indexBytes(t, "HAPPYHRS"), `"v1"`)
	ts := httptest.NewServer(http.HandlerFunc(srv.handler))
	defer ts.Close()

	ctx := context.Background()
	v, err := NewRemoteValidator(ctx, ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Publish garbage under a new ETag.
	srv.set([]byte("this is not an index"), `"v2"`)
	if err := v.reload(ctx); err == nil {
		t.Fatal("reload must report the corrupt artifact")
	}
	// The old truth must still serve.
	if ok, _ := v.Validate(ctx, "HAPPYHRS"); !ok {
		t.Fatal("previous index must keep serving after a failed reload")
	}
}

// TestRemoteValidatorRejectsBadInitial: fail-fast when the very first fetch
// is corrupt — same policy as a corrupt local file.
func TestRemoteValidatorRejectsBadInitial(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("garbage"))
	}))
	defer ts.Close()

	if _, err := NewRemoteValidator(context.Background(), ts.URL); err == nil {
		t.Fatal("initial fetch of a corrupt artifact must fail startup")
	}
}
