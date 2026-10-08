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
# tests/events-live-e2e.sh - LIVE end-to-end validation of the query
# cursor (issue #17) and object tags (issue #13) contracts against a
# real kernel event ring, unlike the pool-free suites which exercise
# only the CLI/DB layers on synthetic databases.
#
# Chain under test: real file operations (create/rename/remove) on an
# events=on dataset -> kernel event ring -> zmetad poll ingest into
# the real SQLite DB -> `zmetad --query` cursor semantics over
# genuinely kernel-produced records -> --tag-set/get/clear on the
# same DB -> REMOVE-event tag coupling against a real remove -> purge
# of a real dataset.
#
# Usage: bash tests/events-live-e2e.sh
# REQUIRES: root or passwordless sudo; a pool with feature@events
# enabled; the dataset must NOT exist (it is created and destroyed).
# Environment overrides: ZMETAD (default /usr/local/sbin/zmetad),
# ZFS (default /usr/local/sbin/zfs), ZPOOL, TESTPOOL (default
# testpool), WORKDS (default e2e-live-$$), ZMETAD_DB (default
# /var/lib/zfs/zmetad.db).
#
# Exit 0 = whole chain verified; nonzero = first failed assertion.
# Idempotent: fresh dataset per run; EXIT trap destroys it.

set -u

ZMETAD="${ZMETAD:-/usr/local/sbin/zmetad}"
ZFS="${ZFS:-/usr/local/sbin/zfs}"
ZPOOL="${ZPOOL:-/usr/local/sbin/zpool}"
TESTPOOL="${TESTPOOL:-testpool}"
WORKDS="${WORKDS:-e2e-live-$$}"
DS="$TESTPOOL/$WORKDS"
MNT=""
DB="${ZMETAD_DB:-/var/lib/zfs/zmetad.db}"
SUDO=()
[ "$(id -u)" = "0" ] || SUDO=(sudo)
fail() {
	echo "FAIL: $*"
	exit 1
}

# shellcheck disable=SC2317  # EXIT/INT trap, invoked indirectly
cleanup() {
	[ -n "$MNT" ] && "${SUDO[@]}" "$ZFS" destroy -R "$DS" >/dev/null 2>&1
	[ -n "$STOP_ZMETAD" ] && "${SUDO[@]}" pkill -f "$ZMETAD -i 1" \
	    >/dev/null 2>&1
}
trap cleanup EXIT INT TERM

[ -e "$ZMETAD" ] || fail "zmetad not found at $ZMETAD (set ZMETAD=)"
[ -e "$ZFS" ] || fail "zfs not found at $ZFS (set ZFS=)"

"${SUDO[@]}" "$ZPOOL" get -H -o value feature@events "$TESTPOOL" \
    2>/dev/null | grep -q enabled \
    || fail "$TESTPOOL does not have feature@events enabled"

"${SUDO[@]}" "$ZFS" create -o events=on -o mountpoint=/tmp/"$WORKDS" \
    "$DS" || fail "cannot create $DS"
MNT="$DS"
"${SUDO[@]}" "$ZFS" set mountpoint=/tmp/"$WORKDS" "$DS" \
    2>/dev/null || true
MNT="/tmp/$WORKDS"
[ -d "$MNT" ] || fail "$DS mounted nowhere ($MNT missing)"

#
# The DB must not carry pre-existing rows for this dataset name
# (parallel runs reuse $WORKDS only within one PID, but the daemon
# may have history).  Purge both DB state and any stale dataset.
"${SUDO[@]}" "$ZMETAD" --purge "$DS" >/dev/null 2>&1

#
# 1. Generate REAL file events: 3 creates + 1 rename + 1 remove.
#
"${SUDO[@]}" touch "$MNT/f1" "$MNT/f2" "$MNT/f3"
"${SUDO[@]}" mv "$MNT/f2" "$MNT/f2r"
"${SUDO[@]}" rm "$MNT/f3"
sync

#
# 2. Ingest: run a one-poll daemon against the real ring.
#    zmetad has no run-once flag, so run it with a 1s interval in the
#    background, give it two cycles, then stop it.
#
"${SUDO[@]}" "$ZMETAD" -i 1 -f -d "$DB" >/dev/null 2>&1 &
ZPID=$!
STOP_ZMETAD=1
sleep 3
kill "$ZPID" 2>/dev/null
wait "$ZPID" 2>/dev/null

#
# 3. Cursor contract over REAL kernel records.
#
OUT=$("${SUDO[@]}" "$ZMETAD" --query "$DS" -d "$DB")
N=$(printf '%s\n' "$OUT" | grep -c .)
[ "$N" -ge 5 ] || fail "expected >=5 events for $DS, got $N"
printf '%s\n' "$OUT" | head -1 | python3 -c '
import json, sys
json.loads(sys.stdin.readline())
' || fail "first live event row is not valid JSON"

# Strictly ascending ids from the live ring.
IDS=$(printf '%s\n' "$OUT" | python3 -c '
import json, sys
ids = [json.loads(l)["id"] for l in sys.stdin if l.strip()]
print(" ".join(str(i) for i in ids))
')
ASC=$(printf '%s\n' "$IDS" | tr ' ' '\n' | sort -n | tr '\n' ' ')
WANT=$(printf '%s\n' "$IDS" | tr ' ' '\n' | tr '\n' ' ')
[ "$ASC" = "$WANT" ] || fail "live ids not ascending: $IDS"

# Truncation honesty over live data.
LAST1=$(printf '%s\n' "$IDS" | tr ' ' '\n' | sort -n | tail -1)
ERR=$("${SUDO[@]}" "$ZMETAD" --query "$DS" --since-id 0 \
    --max-events 3 -d "$DB" 2>&1 >/dev/null)
echo "$ERR" | grep -q "truncated: resume with --since-id [0-9]" \
    || fail "live truncation hint missing: $ERR"

# Resume from the hinted cursor: no overlap, continues upward.
HINT=$(printf '%s\n' "$ERR" | grep -o '[0-9]*$')
RESUME=$("${SUDO[@]}" "$ZMETAD" --query "$DS" --since-id "$HINT" \
    -d "$DB")
FIRST_R=$(printf '%s\n' "$RESUME" | head -1 | python3 -c '
import json, sys
print(json.loads(sys.stdin.readline())["id"])
')
[ "$FIRST_R" -gt "$HINT" ] || fail "resume id $FIRST_R <= hint $HINT"

# Create something new AFTER a cursor was taken: since-id sees only
# the increment (the true incremental-consumption pattern).
"${SUDO[@]}" touch "$MNT/after-cursor"
sync
"${SUDO[@]}" "$ZMETAD" -i 1 -f -d "$DB" >/dev/null 2>&1 &
ZPID=$!
sleep 2
kill "$ZPID" 2>/dev/null
wait "$ZPID" 2>/dev/null
DELTA=$("${SUDO[@]}" "$ZMETAD" --query "$DS" --since-id "$LAST1" -d "$DB" \
    | grep -c .)
[ "$DELTA" -ge 1 ] || fail "no new events visible after cursor $LAST1"

echo "PASS cursor: $N live events, ascending, truncate+resume at $HINT, incremental delta $DELTA"

#
# 4. Tags contract on the live DB.
#
TAGOBJ=$("${SUDO[@]}" "$ZMETAD" --query "$DS" -d "$DB" | head -1 \
    | python3 -c '
import json, sys
print(json.loads(sys.stdin.readline())["object_id"])
')
[ -n "$TAGOBJ" ] || fail "could not read an object_id from live events"

"${SUDO[@]}" "$ZMETAD" --tag-set "$DS" --tag-object "$TAGOBJ" \
    --tag team=storage --tag env=prod -d "$DB" \
    | grep -q "2 tag(s) set" || fail "tag-set confirmation missing"
GOT=$("${SUDO[@]}" "$ZMETAD" --tag-get "$DS" --tag-object "$TAGOBJ" -d "$DB")
printf '%s\n' "$GOT" | grep -q '^team=storage$' \
    || fail "tag get missing team: $GOT"
printf '%s\n' "$GOT" | grep -q '^env=prod$' \
    || fail "tag get missing env: $GOT"

# Replace semantics on the live DB.
"${SUDO[@]}" "$ZMETAD" --tag-set "$DS" --tag-object "$TAGOBJ" \
    --tag tier=gold -d "$DB" >/dev/null || fail "re-tag failed"
GOT=$("${SUDO[@]}" "$ZMETAD" --tag-get "$DS" --tag-object "$TAGOBJ" -d "$DB")
printf '%s\n' "$GOT" | grep -q '^tier=gold$' || fail "replace lost new tag"
printf '%s\n' "$GOT" | grep -q '^team=storage$' \
    && fail "replace kept stale tag"

echo "PASS tags: set/get/replace on live DB for object $TAGOBJ"

#
# 5. REMOVE coupling: remove a DIFFERENT tagged file via the VFS;
#    after ingest its tags must be gone.
#
"${SUDO[@]}" touch "$MNT/tagged"
sync
"${SUDO[@]}" "$ZMETAD" -i 1 -f -d "$DB" >/dev/null 2>&1 &
ZPID=$!
sleep 2
kill "$ZPID" 2>/dev/null
wait "$ZPID" 2>/dev/null
TOBJ=$("${SUDO[@]}" "$ZMETAD" --query "$DS" -d "$DB" \
    | python3 -c '
import json, sys
name_id = {}
for line in sys.stdin:
    if not line.strip():
        continue
    r = json.loads(line)
    if r.get("path") == "tagged" and r.get("event_type") == "CREATE":
        name_id["obj"] = r["object_id"]
print(name_id.get("obj", ""))
')
[ -n "$TOBJ" ] || fail "could not find CREATE event for tagged file"

"${SUDO[@]}" "$ZMETAD" --tag-set "$DS" --tag-object "$TOBJ" \
    --tag keep=me -d "$DB" >/dev/null || fail "tagging tagged-file failed"
"${SUDO[@]}" rm "$MNT/tagged"
sync
"${SUDO[@]}" "$ZMETAD" -i 1 -f -d "$DB" >/dev/null 2>&1 &
ZPID=$!
sleep 2
kill "$ZPID" 2>/dev/null
wait "$ZPID" 2>/dev/null

REMOVED=$("${SUDO[@]}" "$ZMETAD" --query "$DS" -d "$DB" \
    | python3 -c '
import json, sys
found = False
for line in sys.stdin:
    if not line.strip():
        continue
    r = json.loads(line)
    if r.get("event_type") == "REMOVE" and r.get("object_id"):
        print(r["object_id"])
        found = True
print("none" if not found else "")
')
echo "$REMOVED" | grep -q "$TOBJ" \
    || fail "REMOVE for object $TOBJ not in live log: got [$REMOVED]"
LEFT=$("${SUDO[@]}" "$ZMETAD" --tag-get "$DS" --tag-object "$TOBJ" -d "$DB")
[ -z "$LEFT" ] || fail "tags survived REMOVE: $LEFT"

echo "PASS remove-coupling: object $TOBJ removed via VFS, tags gone"

#
# 6. Purge a real dataset clears both events and tags.
#
"${SUDO[@]}" "$ZMETAD" --tag-set "$DS" --tag-object "$TAGOBJ" \
    --tag purge=me -d "$DB" >/dev/null || fail "pre-purge tagging failed"
"${SUDO[@]}" "$ZMETAD" --purge "$DS" -d "$DB" >/dev/null \
    || fail "purge failed"
LEFTQ=$("${SUDO[@]}" "$ZMETAD" --query "$DS" -d "$DB" | grep -c .)
[ "$LEFTQ" = "0" ] || fail "purge left events behind ($LEFTQ rows)"
LEFTT=$("${SUDO[@]}" "$ZMETAD" --tag-get "$DS" --tag-object "$TAGOBJ" -d "$DB")
[ -z "$LEFTT" ] || fail "purge left tags behind: $LEFTT"

echo "PASS purge: dataset rows and tags both cleared"
echo "LIVE E2E: ALL PASS"
exit 0
