// Command indexer builds the coupon index from the raw gzipped corpus.
//
//	indexer -out data/coupons.idx data/couponbase1.gz data/couponbase2.gz data/couponbase3.gz
//
// This is the offline half of the coupon system: it runs once per corpus
// delivery (never per request) and reduces hundreds of millions of lines to
// the exact set of codes present in ≥2 files. See docs/DESIGN.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/SaiPavan-GGNEXT/gokart/internal/coupon"
)

func main() {
	out := flag.String("out", "data/coupons.idx", "output index path")
	tmp := flag.String("tmp", "", "scratch directory for spill files (default: system temp)")
	printCodes := flag.Bool("print", false, "print the final valid codes to stdout")
	flag.Parse()

	sources := flag.Args()
	if len(sources) < 2 {
		fmt.Fprintln(os.Stderr, "usage: indexer -out <index> <file1.gz> <file2.gz> [more.gz...]")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := log.New(os.Stderr, "indexer: ", log.LstdFlags)
	meta, err := coupon.Build(ctx, coupon.BuildOptions{
		Sources: sources,
		OutPath: *out,
		TempDir: *tmp,
		Log:     logger.Printf,
	})
	if err != nil {
		logger.Fatalf("BUILD FAILED (no index written): %v", err)
	}

	fmt.Printf("built %s: %d valid codes from %d sources\n", *out, meta.CodeCount, len(meta.Sources))
	for _, s := range meta.Sources {
		fmt.Printf("  %s  lines=%d kept=%d rejected=%d sha256=%s\n",
			s.Name, s.Lines, s.KeptLines, s.RejectedLines, s.SHA256)
	}
	if *printCodes {
		idx, err := coupon.LoadIndex(*out)
		if err != nil {
			logger.Fatalf("re-reading index: %v", err)
		}
		for _, c := range idx.Codes() {
			fmt.Println(c)
		}
	}
}
