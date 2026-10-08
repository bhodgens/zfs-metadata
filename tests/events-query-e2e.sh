#!/usr/bin/env bash
# SPDX-License-Identifier: CDDL-1.0
#
# This file and its contents are supplied under the terms of the
# Common Development and Distribution License ("CDDL"), version 1.0.
# You may only use this file in accordance with the terms of version
# 1.0 of the CDDL.
#
# A full copy of the text of the CDDL should have accompanied this
# source.  A copy of the CDDL is also available via the Internet at
# https://opensource.org/license/CDDL-1.0.
#
# This suite runs under bash; [ ] tests are intentional style here.
# shellcheck disable=SC2292
#
# tests/events-query-e2e.sh - zmetad --query incremental cursor e2e
# (issue #17, SCHEMA.md section 8.5).
#
# Pure userspace: no pool, no sudo, no kernel ZFS.  The suite
# synthesizes SQLite databases with the SCHEMA.md section 2 layout
# (all six tables; meta stamped db_schema_version=8 and
# events_schema_version=3, matching what zmetad_db_open_readonly
# expects to find in a real database: the readonly opener validates
# nothing, it just opens the existing file), then drives the one-shot
# `zmetad --query <dataset> [--since-id N] [--max-events N]` mode
# against them and checks the cursor contract: dataset-scoped,
# strictly-ascending id order, exclusive --since-id resume, LIMIT
# truncation with the stderr resume hint at exit 0, NDJSON output
# with the Section 2.1 column names plus leading "id", and NULL ->
# null / escaped TEXT rendering.
#
# Usage: bash tests/events-query-e2e.sh
# Exit 0 = every case passed; exit 1 = first failure.
#
# Environment overrides: ZMETAD (binary under test; default is the
# libtool wrapper ./zmetad at the repo root).

set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ZMETAD="${ZMETAD:-$REPO/zmetad}"
WD="$(mktemp -d /var/tmp/zmetad-query-e2e.XXXXXX)" ||
	{ echo "FAIL: mktemp -d" >&2; exit 1; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

pass() {
	echo "PASS $*"
}

# cleanup runs from the EXIT trap; shellcheck cannot see that path.
# shellcheck disable=SC2317
cleanup() {
	rm -rf "$WD"
}
trap cleanup EXIT
# TERM/INT must clean up too: an EXIT-only trap leaves the workdir
# behind when the suite is killed between steps.  Routing the signal
# through exit runs the EXIT trap above.
trap 'exit 143' TERM
trap 'exit 130' INT

command -v sqlite3 >/dev/null 2>&1 ||
	fail "sqlite3 CLI not found in PATH (needed to synthesize databases)"
command -v python3 >/dev/null 2>&1 ||
	fail "python3 not found in PATH (needed for the JSON spot checks)"
[ -x "$ZMETAD" ] ||
	fail "binary under test not executable: $ZMETAD (set ZMETAD=...)"
"$ZMETAD" --help 2>&1 | grep -q -- '--query' ||
	fail "$ZMETAD does not support --query; build the issue #17 tree first"

# make_schema is inline below: `schema_sql_fresh | sqlite3 <db>` lays
# down the empty SCHEMA.md section 2 layout.

schema_sql_fresh() {
	cat <<'SQL'
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    txg INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    captured_at INTEGER,
    object_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    path TEXT,
    old_path TEXT,
    uid INTEGER,
    gid INTEGER,
    mode INTEGER,
    size INTEGER,
    io_offset INTEGER,
    io_bytes INTEGER,
    parent INTEGER,
    old_parent INTEGER,
    target TEXT,
    old_size INTEGER,
    attrs INTEGER,
    full_path TEXT,
    old_full_path TEXT,
    principal INTEGER,
    UNIQUE(dataset, txg, object_id, event_type, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_events_dataset_time
    ON events(dataset, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_object
    ON events(dataset, object_id);
CREATE INDEX IF NOT EXISTS idx_events_path
    ON events(dataset, path);
CREATE TABLE IF NOT EXISTS objmap (
    dataset TEXT NOT NULL,
    object_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    parent INTEGER,
    PRIMARY KEY (dataset, object_id)
);
CREATE TABLE IF NOT EXISTS sync_state (
    dataset TEXT PRIMARY KEY,
    last_offset INTEGER NOT NULL,
    last_sync INTEGER NOT NULL,
    ring_guid INTEGER,
    last_lost INTEGER,
    root_id INTEGER
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    detected INTEGER NOT NULL,
    from_offset INTEGER,
    to_offset INTEGER,
    lost INTEGER NOT NULL
);
INSERT INTO meta (key, value) VALUES ('db_schema_version', '8');
INSERT INTO meta (key, value) VALUES ('events_schema_version', '3');
SQL
}

# seed_rows <sqlfile> -- emit explicit-id inserts: dataset 'ds' takes
# ids 1..100, dataset 'other' ids 501..550, so cursor windows,
# isolation, and resume points have literal expected values.
seed_rows() {
	_sqlf="$1"
	: > "$_sqlf"
	for ((i = 1; i <= 100; i++)); do
		printf "INSERT INTO events (id, dataset, txg, timestamp, captured_at, object_id, event_type) VALUES (%d, 'ds', %d, %d, 1700000000, %d, 'CREATE');\n" \
		    "$i" "$i" "$i" "$i" >> "$_sqlf"
	done
	for ((i = 501; i <= 550; i++)); do
		printf "INSERT INTO events (id, dataset, txg, timestamp, captured_at, object_id, event_type) VALUES (%d, 'other', %d, %d, 1700000000, %d, 'REMOVE');\n" \
		    "$i" "$i" "$i" "$i" >> "$_sqlf"
	done
	# A gaps row exists so the query's loss probe runs against real
	# data (ring OFFSETS, a different keyspace from events.id per
	# SCHEMA.md 8.5); it must not disturb any query result.
	printf "INSERT INTO gaps (dataset, detected, from_offset, to_offset, lost) VALUES ('other', 1700000000, 512, 1024, 7);\n" \
	    >> "$_sqlf"
}

# leading_id <ndjson-line> -> echoes the leading "id" value; returns
# nonzero (does not exit: callers run in command substitution) when
# the line does not open with a numeric "id" field.
leading_id() {
	_line="$1"
	_id="$(printf '%s\n' "$_line" |
	    sed -n 's/^{"id":\([0-9][0-9]*\),.*/\1/p')"
	[ -n "$_id" ] || return 1
	printf '%s\n' "$_id"
}

# validate_ndjson <file> <dataset>
# Every line must parse as JSON with exactly the SCHEMA.md 2.1 column
# names plus leading "id" (the emitter's field order), and carry the
# requested dataset.  No jq dependency.  Returns nonzero (with a
# diagnostic on stderr) on the first bad line; callers || fail.
validate_ndjson() {
	_file="$1"
	_ds="$2"
	python3 - "$_file" "$_ds" <<'PYEOF'
import json
import sys

path, ds = sys.argv[1], sys.argv[2]
expected_keys = [
    "id", "dataset", "txg", "timestamp", "captured_at", "object_id",
    "event_type", "path", "old_path", "uid", "gid", "mode", "size",
    "io_offset", "io_bytes", "parent", "old_parent", "target",
    "old_size", "attrs", "full_path", "old_full_path", "principal",
]
with open(path, "r", encoding="utf-8") as f:
    lines = f.read().splitlines()
if not lines:
    sys.exit("no output lines to validate")
for n, line in enumerate(lines, 1):
    if not line.startswith('{"id":'):
        sys.exit("line %d: id is not the leading field: %s"
                 % (n, line[:60]))
    try:
        obj = json.loads(line)
    except ValueError as exc:
        sys.exit("line %d: not valid JSON: %s" % (n, exc))
    if sorted(obj.keys()) != sorted(expected_keys):
        sys.exit("line %d: key set mismatch: %s"
                 % (n, sorted(obj.keys())))
    if obj["dataset"] != ds:
        sys.exit("line %d: dataset %r, expected %r"
                 % (n, obj["dataset"], ds))
    if not isinstance(obj["id"], int) or not isinstance(obj["txg"], int):
        sys.exit("line %d: id/txg are not JSON integers" % n)
print("json-ok")
PYEOF
}

OUT="$WD/out"
ERR="$WD/err"
SQLF="$WD/seed.sql"

# --- Case (a): empty events table -> empty stdout, exit 0 ---------
EMPTY_DB="$WD/empty.db"
schema_sql_fresh | sqlite3 "$EMPTY_DB" || fail "build $EMPTY_DB"
if ! "$ZMETAD" -d "$EMPTY_DB" --query ds >"$OUT" 2>"$ERR"; then
	fail "case a: --query on empty db exited nonzero: $(cat "$ERR")"
fi
[ -s "$OUT" ] && fail "case a: expected empty stdout, got: $(head -3 "$OUT")"
pass "empty db: no rows, exit 0"

# --- Main database: 100 ds rows (ids 1..100) + 50 other rows ------
MAIN_DB="$WD/main.db"
schema_sql_fresh | sqlite3 "$MAIN_DB" || fail "build $MAIN_DB"
seed_rows "$SQLF"
sqlite3 "$MAIN_DB" <"$SQLF" || fail "seed $MAIN_DB"

# --- Case (b): cursor window + truncation hint --------------------
# ids > 50 for 'ds' are 51..100; --max-events 10 must return exactly
# 51..60 ascending and hint the resume id 60 on stderr at exit 0.
if ! "$ZMETAD" -d "$MAIN_DB" --query ds --since-id 50 --max-events 10 \
    >"$OUT" 2>"$ERR"
then
	fail "case b: query exited nonzero: $(cat "$ERR")"
fi
_nlines="$(wc -l <"$OUT")"
[ "$_nlines" -eq 10 ] ||
	fail "case b: expected 10 rows, got $_nlines"
_expected=(51 52 53 54 55 56 57 58 59 60)
_i=0
while IFS= read -r _line; do
	_id="$(leading_id "$_line")" ||
		fail "case b: line lacks a leading numeric \"id\": $_line"
	[ "$_id" -eq "${_expected[$_i]}" ] ||
		fail "case b: row $_i has id $_id, expected ${_expected[$_i]}"
	_i=$((_i + 1))
done <"$OUT"
grep -Fq 'truncated: resume with --since-id 60' "$ERR" ||
	fail "case b: stderr lacks resume hint: $(cat "$ERR")"
pass "cursor window: ids 51..60 ascending, truncated hint at 60, exit 0"

# --- Case (c): --since-id beyond the last row ----------------------
if ! "$ZMETAD" -d "$MAIN_DB" --query ds --since-id 200 >"$OUT" 2>"$ERR"
then
	fail "case c: query exited nonzero: $(cat "$ERR")"
fi
[ -s "$OUT" ] && fail "case c: expected empty stdout, got: $(head -3 "$OUT")"
pass "since-id beyond last row: empty, exit 0"

# --- Case (d): no --since-id starts from id 1 ----------------------
if ! "$ZMETAD" -d "$MAIN_DB" --query ds >"$OUT" 2>"$ERR"
then
	fail "case d: query exited nonzero: $(cat "$ERR")"
fi
_nlines="$(wc -l <"$OUT")"
[ "$_nlines" -eq 100 ] ||
	fail "case d: expected 100 rows (default max 1000), got $_nlines"
_first="$(leading_id "$(head -1 "$OUT")")" ||
	fail "case d: first line lacks a leading numeric \"id\""
[ "$_first" -eq 1 ] || fail "case d: first id is $_first, expected 1"
validate_ndjson "$OUT" ds ||
	fail "case d: NDJSON validation failed"
pass "no --since-id: starts at id 1, 100 rows, valid NDJSON"

# --- Case (e): dataset isolation -----------------------------------
# 'other' rows (ids 501..550, including a gaps row) never leak into
# the 'ds' stream and vice versa.
grep -q '"dataset":"other"' "$OUT" &&
	fail "case e: 'other' rows leaked into --query ds output"
if ! "$ZMETAD" -d "$MAIN_DB" --query other >"$OUT" 2>"$ERR"
then
	fail "case e: --query other exited nonzero: $(cat "$ERR")"
fi
_nlines="$(wc -l <"$OUT")"
[ "$_nlines" -eq 50 ] || fail "case e: expected 50 'other' rows, got $_nlines"
_first="$(leading_id "$(head -1 "$OUT")")" ||
	fail "case e: first line lacks a leading numeric \"id\""
[ "$_first" -eq 501 ] || fail "case e: first 'other' id is $_first, expected 501"
validate_ndjson "$OUT" other ||
	fail "case e: NDJSON validation failed"
pass "dataset isolation: ds/other streams fully scoped"

# --- Cases (f)/(g)/(h): argument validation ------------------------
"$ZMETAD" -d "$MAIN_DB" --query ds --since-id garbage >"$OUT" 2>"$ERR"
_rc=$?
[ "$_rc" -ne 0 ] || fail "case f: non-numeric --since-id was accepted"
[ -s "$ERR" ] || fail "case f: rejection printed no error"
pass "since-id garbage: rejected with an error"

"$ZMETAD" -d "$MAIN_DB" --query ds --max-events 0 >"$OUT" 2>"$ERR"
_rc=$?
[ "$_rc" -ne 0 ] || fail "case g: --max-events 0 was accepted"
[ -s "$ERR" ] || fail "case g: rejection printed no error"
pass "max-events 0: rejected with an error"

"$ZMETAD" -d "$MAIN_DB" --since-id 5 >"$OUT" 2>"$ERR"
_rc=$?
[ "$_rc" -ne 0 ] || fail "case h: --since-id without --query was accepted"
[ -s "$ERR" ] || fail "case h: rejection printed no error"
pass "since-id without --query: rejected with an error"

# --- Case (i): NULL -> null and TEXT escaping ----------------------
# Two rows exercising the rendering contract: a TRUNCATE row whose
# optional columns are all NULL (must render as JSON null), and a
# CREATE row whose path carries a double quote and a backslash (must
# survive JSON escaping and round-trip through json.loads).
JSON_DB="$WD/json.db"
schema_sql_fresh | sqlite3 "$JSON_DB" || fail "build $JSON_DB"
sqlite3 "$JSON_DB" >/dev/null <<'SQL' || fail "seed $JSON_DB"
INSERT INTO events (id, dataset, txg, timestamp, object_id, event_type,
    old_size, size)
VALUES (1, 'ds', 42, 123456789, 128, 'TRUNCATE', 100, 0);
INSERT INTO events (id, dataset, txg, timestamp, captured_at, object_id,
    event_type, path, uid, gid)
VALUES (2, 'ds', 43, 123456790, 1700000123, 129, 'CREATE', 'we"ird\name',
    1000, 1000);
SQL
if ! "$ZMETAD" -d "$JSON_DB" --query ds >"$OUT" 2>"$ERR"
then
	fail "case i: query exited nonzero: $(cat "$ERR")"
fi
[ "$(wc -l <"$OUT")" -eq 2 ] || fail "case i: expected 2 rows"
python3 - "$OUT" <<'PYEOF' || fail "case i: JSON null/escape check failed"
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    rows = [json.loads(line) for line in f.read().splitlines()]
if len(rows) != 2:
    sys.exit("expected 2 rows, got %d" % len(rows))
r1, r2 = rows
# TRUNCATE populates only old_size/size beyond the always-present
# four (SCHEMA.md 2.1); every other optional column must be null.
null_columns = (
    "captured_at", "path", "old_path", "uid", "gid", "mode",
    "io_offset", "io_bytes", "parent", "old_parent", "target",
    "attrs", "full_path", "old_full_path", "principal",
)
for k in null_columns:
    if r1[k] is not None:
        sys.exit("NULL column %s rendered as %r, expected null"
                 % (k, r1[k]))
if r1["id"] != 1 or r1["object_id"] != 128:
    sys.exit("row 1 id/object_id wrong: %r" % (r1,))
if r1["event_type"] != "TRUNCATE":
    sys.exit("row 1 event_type wrong: %r" % (r1["event_type"],))
if r1["old_size"] != 100 or r1["size"] != 0:
    sys.exit("row 1 sizes rendered wrong: %r" % (r1,))
if r2["path"] != 'we"ird\\name':
    sys.exit("TEXT escape mismatch: %r" % (r2["path"],))
if r2["captured_at"] != 1700000123 or r2["uid"] != 1000:
    sys.exit("row 2 integers rendered wrong: %r" % (r2,))
print("json-null-escape-ok")
PYEOF

pass "JSON rendering: nulls preserved, TEXT escaping round-trips"

echo "E2E: ALL PASS"
exit 0
