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
# tests/events-tags-e2e.sh - zmetad object tag CLI e2e (issue #13,
# SCHEMA.md section 2.7).
#
# Pure userspace: no pool, no sudo, no kernel ZFS, no ZDB.  Unlike
# tests/events-query-e2e.sh no database is synthesized: the database
# is created (and migrated to layout 9) by zmetad_db_open on the
# FIRST --tag-set, exactly as a fresh consumer deployment would see
# it.  The suite drives the one-shot tag modes
#
#	zmetad --tag-set <dataset> --tag-object <id> --tag k=v [...]
#	zmetad --tag-get <dataset> --tag-object <id>
#	zmetad --tag-clear <dataset> --tag-object <id>
#
# and checks the pinned contract: a set REPLACES the object's whole
# set (never merges), the S3 limits (<= 10 tags/object, key <= 128
# characters, value <= 256 bytes) reject with the stored set left
# untouched, get of an untagged object is an empty result at exit 0,
# clear is idempotent, --tag splits on the FIRST '=', the tag modes
# are mutually exclusive with each other and reject malformed
# arguments, tags persist across separate CLI invocations, and tags
# are dataset-scoped (same object id, different dataset, no leak).
# Every CLI call is a fresh process, so the persistence case needs
# no daemon: each get that follows a set IS a new invocation reading
# the committed database.
#
# Usage: bash tests/events-tags-e2e.sh
# Exit 0 = every case passed; exit 1 = first failure.
#
# Environment overrides: ZMETAD (binary under test; default is the
# libtool wrapper ./zmetad at the repo root).

set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ZMETAD="${ZMETAD:-$REPO/zmetad}"
WD="$(mktemp -d /var/tmp/zmetad-tags-e2e.XXXXXX)" ||
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

[ -x "$ZMETAD" ] ||
	fail "binary under test not executable: $ZMETAD (set ZMETAD=...)"
"$ZMETAD" --help 2>&1 | grep -q -- '--tag-set' ||
	fail "$ZMETAD does not support --tag-set; build the issue #13 phase-2 tree first"

DB="$WD/zmetad.db"
OUT="$WD/out"
ERR="$WD/err"
EXPECT="$WD/expect"

# expected_get <dataset> <id> <tag>... - run --tag-get as a fresh
# process, require exit 0, sort the key=value lines (get order is
# not contractual), and compare against the expected lines given as
# arguments (empty argument list = empty result).
expected_get() {
	_ds="$1"
	_oid="$2"
	shift 2
	if ! "$ZMETAD" -d "$DB" --tag-get "$_ds" --tag-object "$_oid" \
	    >"$OUT" 2>"$ERR"
	then
		fail "tag get $_ds/$_oid exited nonzero: $(cat "$ERR")"
	fi
	LC_ALL=C sort -o "$OUT" "$OUT"
	if [ "$#" -eq 0 ]; then
		: >"$EXPECT"
	else
		printf '%s\n' "$@" | LC_ALL=C sort >"$EXPECT"
	fi
	if ! diff -u "$EXPECT" "$OUT" >"$ERR".diff; then
		fail "tag get $_ds/$_oid mismatch: $(head -20 "$ERR".diff)"
	fi
}

# --- Case (a): set 2 tags, get returns exactly those 2 lines -------
# The FIRST --tag-set creates the database (layout 9); k2's value
# contains '=' to pin the first-'=' split rule end to end.
if ! "$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    --tag k1=v1 --tag k2=a=b >"$OUT" 2>"$ERR"
then
	fail "case a: first --tag-set (db creation) failed: $(cat "$ERR")"
fi
expected_get tank/a 128 'k1=v1' 'k2=a=b'
pass "set 2 tags: get returns exactly k1=v1 and k2=a=b"

# --- Case (b): replace, not merge ----------------------------------
# A second set with one different key must leave ONLY the new set.
if ! "$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    --tag r1=newval >"$OUT" 2>"$ERR"
then
	fail "case b: replace --tag-set failed: $(cat "$ERR")"
fi
expected_get tank/a 128 'r1=newval'
pass "replace: second set drops k1/k2, only r1=newval remains"

# --- Case (c): clear is empty-at-exit-0 and idempotent --------------
if ! "$ZMETAD" -d "$DB" --tag-clear tank/a --tag-object 128 \
    >"$OUT" 2>"$ERR"
then
	fail "case c: --tag-clear failed: $(cat "$ERR")"
fi
expected_get tank/a 128
pass "clear: get is empty at exit 0"
if ! "$ZMETAD" -d "$DB" --tag-clear tank/a --tag-object 128 \
    >"$OUT" 2>"$ERR"
then
	fail "case c: second --tag-clear failed: $(cat "$ERR")"
fi
expected_get tank/a 128
pass "clear again: still exit 0, still empty"

# --- Case (d): 11 pairs rejected, stored set untouched ---------------
# Re-seed a known set first: case (c) cleared the object, and a
# rejection leaving "untouched" is only provable against a non-empty
# stored set.
if ! "$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    --tag r1=newval >"$OUT" 2>"$ERR"
then
	fail "case d: re-seed --tag-set failed: $(cat "$ERR")"
fi
for _i in {1..11}; do
	_L11+=("k$_i=v$_i")
done
# Direct invocation (not inside an if): $? must be read right here.
"$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    "${_L11[@]}" >"$OUT" 2>"$ERR"
_rc=$?
[ "$_rc" -eq 1 ] ||
	fail "case d: 11-tag set exited $_rc, expected 1"
[ -s "$ERR" ] || fail "case d: rejection printed no error"
expected_get tank/a 128 'r1=newval'
pass "11 tags: exit 1, stored set untouched"

# --- Case (e): key/value size limits ---------------------------------
# key <= 128 characters and value <= 256 bytes are allowed; one more
# of either must be rejected with the stored set left untouched.
# The probes keep the OTHER field short so exactly one limit is
# exercised per invocation, and the sizes are asserted so the case
# cannot silently degenerate into a wrong-length probe.
_key129="$(printf 'k%.0s' {1..129})"
_val257="$(printf 'v%.0s' {1..257})"
[ "${#_key129}" -eq 129 ] || fail "case e: key probe is not 129 chars"
[ "${#_val257}" -eq 257 ] || fail "case e: value probe is not 257 bytes"
"$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    --tag "$_key129=x" >"$OUT" 2>"$ERR"
_rc=$?
[ "$_rc" -eq 1 ] || fail "case e: 129-char key exited $_rc, expected 1"
[ -s "$ERR" ] || fail "case e: 129-char key rejection printed no error"
"$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    --tag k="$_val257" >"$OUT" 2>"$ERR"
_rc=$?
[ "$_rc" -eq 1 ] || fail "case e: 257-byte value exited $_rc, expected 1"
[ -s "$ERR" ] || fail "case e: 257-byte value rejection printed no error"
expected_get tank/a 128 'r1=newval'
pass "129-char key and 257-byte value: exit 1, set untouched"

# --- Case (f): argument validation -----------------------------------
# Every malformed invocation must be rejected (nonzero exit, error
# on stderr) and must not disturb the stored set.
tag_reject() {
	_label="$1"
	shift
	"$ZMETAD" -d "$DB" "$@" >"$OUT" 2>"$ERR"
	_rc=$?
	[ "$_rc" -ne 0 ] ||
		fail "case f ($_label): malformed invocation was accepted"
	[ -s "$ERR" ] ||
		fail "case f ($_label): rejection printed no error"
}
tag_reject "--tag without =" \
	--tag-set tank/a --tag-object 128 --tag noequals
tag_reject "--tag-set with no --tag" \
	--tag-set tank/a --tag-object 128
tag_reject "--tag-get and --tag-set together" \
	--tag-get tank/a --tag-set tank/a --tag-object 128 --tag k=v
tag_reject "--tag-object missing" \
	--tag-set tank/a --tag k1=v1
tag_reject "garbage --tag-object" \
	--tag-get tank/a --tag-object garbage
expected_get tank/a 128 'r1=newval'
pass "malformed invocations rejected, set untouched"

# --- Case (g): persistence across fresh invocations ------------------
# Every CLI call above is already its own process against the same
# database; this case states the contract explicitly: a set followed
# by a get IN A NEW PROCESS still sees the committed tags.
if ! "$ZMETAD" -d "$DB" --tag-set tank/a --tag-object 128 \
    --tag env=prod >"$OUT" 2>"$ERR"
then
	fail "case g: --tag-set failed: $(cat "$ERR")"
fi
expected_get tank/a 128 'env=prod'
pass "persistence: new-process get sees the committed set"

# --- Case (h): dataset isolation at the same object id ---------------
# Tags key on (dataset, object_id): tank/b's tags for object 128 are
# invisible to tank/a's get for the same id, and neither set bleeds.
if ! "$ZMETAD" -d "$DB" --tag-set tank/b --tag-object 128 \
    --tag iso=1 >"$OUT" 2>"$ERR"
then
	fail "case h: --tag-set tank/b failed: $(cat "$ERR")"
fi
expected_get tank/b 128 'iso=1'
expected_get tank/a 128 'env=prod'
pass "dataset isolation: tank/a and tank/b scoped at object 128"

echo "E2E: ALL PASS"
exit 0
