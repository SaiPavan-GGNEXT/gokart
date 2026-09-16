package coupon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// RemoteValidator serves lookups from an index fetched over HTTP(S) — e.g. a
// (public or presigned) S3 object URL — and optionally polls it for changes,
// hot-swapping the in-memory index with zero downtime.
//
// This is the serving half of the live-update pipeline (docs/DESIGN.md,
// "Updating the corpus"): raw .gz files land in object storage whenever the
// business likes; an indexer job reduces them to a fresh coupons.idx; every
// server instance picks the new artifact up within one poll interval.
//
// Update semantics:
//   - initial fetch is fail-fast: no verified index, no server — same
//     policy as the local file path.
//   - reloads are fail-STATIC: if a poll or a fetched artifact is bad
//     (network error, CRC mismatch, non-200), the current index keeps
//     serving and the problem is logged. Yesterday's verified truth beats
//     an unverifiable update.
//   - swaps are atomic (atomic.Pointer): requests see the old index or the
//     new one, never a mix, with no locking on the lookup path.
type RemoteValidator struct {
	url    string
	client *http.Client

	idx  atomic.Pointer[Index]
	mu   sync.Mutex // guards etag + fetchedAt (reload path only)
	etag string

	fetchedAt atomic.Pointer[time.Time]
}

// NewRemoteValidator fetches and verifies the index at url. The context
// bounds the initial fetch only.
func NewRemoteValidator(ctx context.Context, url string) (*RemoteValidator, error) {
	v := &RemoteValidator{
		url:    url,
		client: &http.Client{Timeout: 30 * time.Second},
	}
	if err := v.reload(ctx); err != nil {
		return nil, fmt.Errorf("remote coupon index: initial fetch: %w", err)
	}
	return v, nil
}

// StartPolling re-checks the URL every interval until ctx is cancelled.
// Uses If-None-Match so unchanged artifacts cost one 304 round trip.
func (v *RemoteValidator) StartPolling(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := v.reload(ctx); err != nil {
					slog.Warn("coupon index reload failed; continuing to serve current index",
						"url", v.url, "error", err)
				}
			}
		}
	}()
}

// reload fetches the artifact if it changed, verifies it, and swaps it in.
func (v *RemoteValidator) reload(ctx context.Context) error {
	v.mu.Lock()
	etag := v.etag
	v.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil // current index is still the latest
	case http.StatusOK:
		// fall through
	default:
		return fmt.Errorf("GET %s: HTTP %d", v.url, resp.StatusCode)
	}

	// Write to a temp file so LoadIndex applies the full verification suite
	// (magic, size, CRC, sortedness) before anything is swapped in.
	tmp, err := os.CreateTemp("", "coupons-remote-*.idx")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, io.LimitReader(resp.Body, 64<<20)); err != nil {
		tmp.Close()
		return fmt.Errorf("downloading index: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	idx, err := LoadIndex(tmpName)
	if err != nil {
		return fmt.Errorf("fetched artifact failed verification: %w", err)
	}

	old := v.idx.Load()
	v.idx.Store(idx)
	now := time.Now().UTC()
	v.fetchedAt.Store(&now)
	v.mu.Lock()
	v.etag = resp.Header.Get("ETag")
	v.mu.Unlock()

	if old == nil {
		slog.Info("coupon index loaded from URL",
			"url", v.url, "codes", idx.Count(), "built_at", idx.Meta.BuiltAt)
	} else {
		slog.Info("coupon index hot-swapped",
			"url", v.url, "codes", idx.Count(),
			"old_built_at", old.Meta.BuiltAt, "new_built_at", idx.Meta.BuiltAt)
	}
	return nil
}

func (v *RemoteValidator) Validate(_ context.Context, raw string) (bool, error) {
	code, ok := Normalize(raw)
	if !ok {
		return false, nil
	}
	return v.idx.Load().Contains(code), nil
}

func (v *RemoteValidator) Info() map[string]any {
	idx := v.idx.Load()
	info := map[string]any{
		"mode":       "remote-index",
		"url":        v.url,
		"code_count": idx.Count(),
		"built_at":   idx.Meta.BuiltAt,
		"sources":    idx.Meta.Sources,
	}
	if t := v.fetchedAt.Load(); t != nil {
		info["fetched_at"] = *t
	}
	return info
}

func (v *RemoteValidator) Healthy(context.Context) error {
	if v.idx.Load() == nil {
		return fmt.Errorf("remote coupon index not loaded")
	}
	return nil
}
