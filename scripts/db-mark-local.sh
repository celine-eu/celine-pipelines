#!/bin/sh
# Mark the datasets database as the `local` environment, for tools that copy data
# between environments and refuse an unmarked or mismatched database.
#
# Runs inside the datasets-db container (`task db:mark-local`), as the database owner,
# with the container's own POSTGRES_USER and POSTGRES_DB.
#
#   no comment                 -> set it
#   celine:env=local           -> nothing to do
#   any other comment          -> refuse: this may not be a local database
set -eu

# The image's own defaults: user postgres, database named after the user.
user="${POSTGRES_USER:-postgres}"
db="${POSTGRES_DB:-$user}"
marker="celine:env=local"

current=$(psql -v ON_ERROR_STOP=1 -U "$user" -d "$db" -tAX -c \
  "SELECT coalesce(pg_catalog.shobj_description(d.oid, 'pg_database'), '')
     FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()")

case "$current" in
  "$marker")
    echo "database $db is already marked $marker"
    exit 0
    ;;
  "")
    psql -v ON_ERROR_STOP=1 -U "$user" -d "$db" -q -c "COMMENT ON DATABASE \"$db\" IS '$marker'"
    echo "database $db marked $marker"
    ;;
  celine:env=*)
    echo "refusing: database $db is marked '$current', not local; it is not this machine's database" >&2
    exit 1
    ;;
  *)
    echo "refusing: database $db already has a comment that is not an environment marker; check it and clear it by hand first" >&2
    exit 1
    ;;
esac
