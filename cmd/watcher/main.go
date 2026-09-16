// Command watcher automates the corpus → index pipeline: it fingerprints the
// raw coupon files (local paths or URLs), and when they change — after a
// settle window guarding against half-finished multi-file uploads — rebuilds
// the index with the exact 2-pass builder and publishes it (local path or
// HTTP PUT, e.g. to S3/MinIO). Serving instances configured with
// COUPON_INDEX=<published URL> pick the new artifact up on their next poll.
//
//	watcher -publish http://minio:9000/coupons/index/coupons.idx \
//	        -interval 60s data/couponbase1.gz data/couponbase2.gz data/couponbase3.gz
//
// -once runs a single check-and-maybe-rebuild cycle and exits: the same
// binary serves as the CronJob payload (deploy/k8s) or an S3-event handler.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SaiPavan-GGNEXT/gokart/internal/pipeline"
)

func main() {
	publish := flag.String("publish", "", "destination for the built index: local path or http(s) URL (required)")
	interval := flag.Duration("interval", time.Minute, "poll cadence for source changes")
	settle := flag.Duration("settle", 10*time.Second, "stability window after a detected change (torn-upload guard)")
	workers := flag.Int("workers", 0, "pass 2 concurrency for rebuilds (0 = NumCPU)")
	once := flag.Bool("once", false, "run a single check cycle and exit (cron / event-handler mode)")
	flag.Parse()

	sources := flag.Args()
	if *publish == "" || len(sources) < 2 {
		fmt.Fprintln(os.Stderr, "usage: watcher -publish <path|url> [flags] <source1> <source2> [more...]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := &pipeline.Watcher{
		Sources:  sources,
		Publish:  *publish,
		Interval: *interval,
		Settle:   *settle,
		Workers:  *workers,
	}

	if *once {
		rebuilt, err := w.CheckOnce(ctx)
		if err != nil {
			slog.Error("watcher: check failed", "error", err)
			os.Exit(1)
		}
		slog.Info("watcher: single cycle complete", "rebuilt", rebuilt)
		return
	}

	slog.Info("watcher: starting",
		"sources", sources, "publish", *publish,
		"interval", interval.String(), "settle", settle.String())
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("watcher: exited", "error", err)
		os.Exit(1)
	}
}
