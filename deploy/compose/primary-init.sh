#!/bin/sh
# Runs once at primary initdb: create the replication role and allow
# replication connections from the compose network.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD 'repl';"
echo "host replication repl all scram-sha-256" >> "$PGDATA/pg_hba.conf"
