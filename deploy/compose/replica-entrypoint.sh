#!/bin/sh
# Replica bootstrap: clone the primary once with pg_basebackup (-R writes
# standby.signal + primary_conninfo), then hand off to the stock entrypoint,
# which starts postgres as a hot standby (read-only).
set -e
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "replica: cloning primary via pg_basebackup..."
  until PGPASSWORD=repl pg_basebackup -h db-primary -U repl -D "$PGDATA" -R -X stream; do
    echo "replica: primary not ready yet; retrying"
    rm -rf "$PGDATA"/* 2>/dev/null || true
    sleep 2
  done
  chmod 700 "$PGDATA" || true
fi
exec docker-entrypoint.sh postgres
