// Package pipeline contains the producing half of the live coupon-update
// story: watching the raw corpus for changes and republishing the index.
//
// The watcher runs OUTSIDE the serving process, deliberately (docs/DESIGN.md,
// "Updating the corpus"): serving needs megabytes and nanoseconds; rebuilds
// need gigabytes and seconds. Fusing them would size every API replica for
// the batch job and put live traffic inside the batch blast radius. As a
// sidecar/companion, the watcher gives "listen and trigger" automation while
// the API keeps its own listener at the artifact layer (RemoteValidator).
package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SaiPavan-GGNEXT/gokart/internal/coupon"
)

// Watcher polls raw corpus sources and republishes the index when they change.
type Watcher struct {
	Sources  []string      // raw .gz corpus: local paths or http(s) URLs
	Publish  string        // built index destination: local path or http(s) URL (PUT)
	Interval time.Duration // poll cadence
	// Settle is the stability window: after a change is detected, the
	// fingerprint must be IDENTICAL again after this delay before a rebuild
	// runs. This is the torn-upload guard — a multi-file corpus update is
	// only consistent as a set, and uploads are not atomic across files.
	Settle  time.Duration
	Workers int // passed through to coupon.Build (0 = NumCPU)
	Client  *http.Client

	lastFP string
}

func (w *Watcher) client() *http.Client {
	if w.Client != nil {
		return w.Client
	}
	return http.DefaultClient
}

// Run polls until ctx is cancelled. If the publish target does not exist yet,
// an initial build runs immediately (bootstrap).
func (w *Watcher) Run(ctx context.Context) error {
	if _, err := w.CheckOnce(ctx); err != nil {
		// First check failing is not fatal: sources may be mid-upload.
		slog.Warn("watcher: initial check failed; will retry", "error", err)
	}
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := w.CheckOnce(ctx); err != nil {
				slog.Warn("watcher: check failed; corpus unchanged as far as consumers are concerned",
					"error", err)
			}
		}
	}
}

// CheckOnce performs one poll cycle and reports whether a rebuild happened.
//
// Failure semantics mirror the serving side: any error leaves the previously
// published artifact untouched (fail-static) and the change is retried on the
// next cycle because lastFP only advances after a successful publish.
func (w *Watcher) CheckOnce(ctx context.Context) (rebuilt bool, err error) {
	fp, err := w.fingerprint(ctx)
	if err != nil {
		return false, fmt.Errorf("fingerprinting sources: %w", err)
	}

	if w.lastFP == "" { // first observation
		exists, err := w.publishTargetExists(ctx)
		if err != nil {
			return false, err
		}
		if exists {
			w.lastFP = fp
			slog.Info("watcher: publish target present; watching for changes",
				"sources", len(w.Sources))
			return false, nil
		}
		slog.Info("watcher: publish target missing; bootstrapping initial build")
		if err := w.rebuild(ctx); err != nil {
			return false, err
		}
		w.lastFP = fp
		return true, nil
	}

	if fp == w.lastFP {
		return false, nil // steady state
	}

	// Change detected — wait for the set to stop moving (torn-upload guard).
	slog.Info("watcher: corpus change detected; waiting for upload to settle",
		"settle", w.Settle.String())
	if w.Settle > 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(w.Settle):
		}
	}
	fp2, err := w.fingerprint(ctx)
	if err != nil {
		return false, fmt.Errorf("re-fingerprinting after settle: %w", err)
	}
	if fp2 != fp {
		slog.Info("watcher: corpus still changing; deferring rebuild to next cycle")
		return false, nil // lastFP unchanged → retried next tick
	}

	if err := w.rebuild(ctx); err != nil {
		return false, err
	}
	w.lastFP = fp2
	return true, nil
}

// fingerprint combines a cheap identity probe of every source: HEAD
// (ETag, Last-Modified, Content-Length) for URLs, size+mtime for files.
// The raw bytes are never read here — a no-change poll costs three HEADs.
func (w *Watcher) fingerprint(ctx context.Context) (string, error) {
	var b strings.Builder
	for _, src := range w.Sources {
		fp, err := fingerprintSource(ctx, w.client(), src)
		if err != nil {
			return "", fmt.Errorf("%s: %w", src, err)
		}
		b.WriteString(fp)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func fingerprintSource(ctx context.Context, client *http.Client, src string) (string, error) {
	if !isURL(src) {
		fi, err := os.Stat(src)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("file|%d|%d", fi.Size(), fi.ModTime().UnixNano()), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, src, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HEAD: HTTP %d", resp.StatusCode)
	}
	return fmt.Sprintf("url|%s|%s|%d",
		resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), resp.ContentLength), nil
}

// rebuild materializes the sources, runs the exact 2-pass build, and
// publishes the fresh artifact.
func (w *Watcher) rebuild(ctx context.Context) error {
	start := time.Now()
	workDir, err := os.MkdirTemp("", "coupon-watch-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workDir)

	local := make([]string, len(w.Sources))
	for i, src := range w.Sources {
		p, err := materialize(ctx, w.client(), src, workDir, i)
		if err != nil {
			return fmt.Errorf("fetching %s: %w", src, err)
		}
		local[i] = p
	}

	out := filepath.Join(workDir, "coupons.idx")
	meta, err := coupon.Build(ctx, coupon.BuildOptions{
		Sources: local,
		OutPath: out,
		Workers: w.Workers,
		Log:     func(f string, a ...any) { slog.Info("watcher: " + fmt.Sprintf(f, a...)) },
	})
	if err != nil {
		return fmt.Errorf("index build: %w", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		return err
	}
	if err := publish(ctx, w.client(), w.Publish, data); err != nil {
		return fmt.Errorf("publishing index: %w", err)
	}
	slog.Info("watcher: index rebuilt and published",
		"codes", meta.CodeCount, "bytes", len(data),
		"target", w.Publish, "took", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// materialize returns a local path for a source: local files pass through,
// URLs are streamed to the work dir.
func materialize(ctx context.Context, client *http.Client, src, dir string, i int) (string, error) {
	if !isURL(src) {
		return src, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET: HTTP %d", resp.StatusCode)
	}
	path := filepath.Join(dir, fmt.Sprintf("src%d.gz", i))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return "", err
	}
	return path, f.Close()
}

func (w *Watcher) publishTargetExists(ctx context.Context) (bool, error) {
	if !isURL(w.Publish) {
		_, err := os.Stat(w.Publish)
		if os.IsNotExist(err) {
			return false, nil
		}
		return err == nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, w.Publish, nil)
	if err != nil {
		return false, err
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return false, err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

// publish writes the artifact to a local path (temp + rename, atomic) or
// PUTs it to a URL (S3-compatible stores replace objects atomically).
func publish(ctx context.Context, client *http.Client, target string, data []byte) error {
	if !isURL(target) {
		dir := filepath.Dir(target)
		tmp, err := os.CreateTemp(dir, ".idx-*")
		if err != nil {
			return err
		}
		name := tmp.Name()
		defer os.Remove(name)
		if _, err := tmp.Write(data); err != nil {
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
		if err := os.Chmod(name, 0o644); err != nil {
			return err
		}
		return os.Rename(name, target)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("PUT %s: HTTP %d", target, resp.StatusCode)
	}
	return nil
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
