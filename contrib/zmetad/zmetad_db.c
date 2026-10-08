// SPDX-License-Identifier: CDDL-1.0
/*
 * This file and its contents are supplied under the terms of the
 * Common Development and Distribution License ("CDDL"), version 1.0.
 * You may only use this file in accordance with the terms of version
 * 1.0 of the CDDL.
 *
 * A full copy of the text of the CDDL should have accompanied this
 * source.  A copy of the CDDL is also available via the Internet at
 * https://opensource.org/license/CDDL-1.0.
 */

/*
 * zmetad database operations - SQLite backend
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>

#include <sqlite3.h>
#include <libnvpair.h>

#include "zmetad.h"
#include "zmetad_schema.h"

#define	ZMETAD_DB_SCHEMA_VERSION	9

struct zmetad_db {
	sqlite3		*sqlite;
	sqlite3_stmt	*insert_event_stmt;
	sqlite3_stmt	*get_last_offset_stmt;
	sqlite3_stmt	*set_last_offset_stmt;
	sqlite3_stmt	*get_ring_guid_stmt;
	sqlite3_stmt	*objmap_get_stmt;
	sqlite3_stmt	*objmap_put_stmt;
	sqlite3_stmt	*objmap_del_stmt;
	sqlite3_stmt	*objmap_any_stmt;
	sqlite3_stmt	*tag_del_obj_stmt;
	sqlite3_stmt	*prune_datasets_stmt;
	sqlite3_stmt	*get_last_lost_stmt;
	sqlite3_stmt	*set_last_lost_stmt;
	const zmetad_schema_t *schema;
	/*
	 * Runtime warning sink (set by the daemon via
	 * zmetad_db_set_warn so warnings survive daemonization);
	 * NULL falls back to stderr.
	 */
	void		(*warn)(const char *msg);
};

void
zmetad_db_set_warn(zmetad_db_t *db, void (*cb)(const char *msg))
{
	if (db != NULL)
		db->warn = cb;
}

/*
 * Emit a runtime warning through the installed sink, or stderr when
 * none was installed (one-shot CLI modes, early open).
 */
static void
db_warn(zmetad_db_t *db, const char *fmt, ...)
{
	char buf[1024];
	va_list ap;

	va_start(ap, fmt);
	(void) vsnprintf(buf, sizeof (buf), fmt, ap);
	va_end(ap);

	if (db->warn != NULL)
		db->warn(buf);
	else
		(void) fputs(buf, stderr);
}

/*
 * One ALTER TABLE ADD COLUMN migration step: a table plus the
 * columns it gains at that layout version.
 */
typedef struct {
	const char	*name;
	const char	*type;
} db_column_t;

/*
 * Columns added by database layout version 2.  Used to upgrade a
 * version 1 database in place.
 */
static const db_column_t db_v2_columns[] = {
	{ "parent",	"INTEGER" },
	{ "old_parent",	"INTEGER" },
	{ "target",	"TEXT" },
	{ "old_size",	"INTEGER" },
	{ "attrs",	"INTEGER" },
};
/*
 * Columns added by database layout version 3.  Used to upgrade a
 * version 2 database in place.
 */
static const db_column_t db_v3_columns[] = {
	{ "ring_guid",	"INTEGER" },
};

#define	NDBCOLS(a)	(sizeof (a) / sizeof ((a)[0]))

/*
 * The gaps table records event-log losses observed while polling:
 * ring-wrap overwrites reported as records_lost deltas and watermark
 * regressions from lost sync_state state or a cleared/recreated ring.
 *
 * gaps.lost semantics (permanent completeness record; rows are
 * never rewritten or deleted by retention cleanup -- only
 * zmetad --purge removes them, by dataset):
 *   > 0   that many records lost (collector lag / queue overflow)
 *   0     watermark regression detected; count unknown
 *   -1    ring replaced (identity swap; count unknown)
 */
static const char *gaps_sql =
	"CREATE TABLE IF NOT EXISTS gaps ("
	"    id INTEGER PRIMARY KEY AUTOINCREMENT,"
	"    dataset TEXT NOT NULL,"
	"    detected INTEGER NOT NULL,"
	"    from_offset INTEGER,"
	"    to_offset INTEGER,"
	"    lost INTEGER NOT NULL"
		");";

static const char *insert_gap_sql =
	"INSERT INTO gaps (dataset, detected, from_offset, to_offset, lost) "
	"VALUES (?, ?, ?, ?, ?)";
/*
 * The datasets table maps each events-enabled dataset to its
 * mountpoint, recorded at collect time so path-keyed consumers
 * can resolve path -> dataset with one query.  last_seen is the
 * wall-clock second of the most recent collect that saw the
 * dataset; rows not refreshed within a poll cycle are pruned
 * (zmetad_db_prune_stale_datasets).
 *
 * last_seen is added via ALTER (db_add_column, duplicate-tolerant)
 * on every open rather than a layout-version bump: adding a column
 * with CREATE TABLE IF NOT EXISTS alone would leave existing tables
 * without it, and the version gates in db_check_layout() run after
 * this point -- a tolerant ALTER keeps new and old databases
 * converged with no version churn.
 */
static const char *datasets_sql =
	"CREATE TABLE IF NOT EXISTS datasets ("
	"    dataset TEXT PRIMARY KEY,"
	"    mountpoint TEXT NOT NULL"
	");";

static const char *upsert_mountpoint_sql =
	"INSERT OR REPLACE INTO datasets (dataset, mountpoint, last_seen) "
	"VALUES (?, ?, ?)";

static const char *prune_stale_datasets_sql =
	"DELETE FROM datasets WHERE last_seen IS NULL OR last_seen < ?";

static const char *schema_sql =
	"CREATE TABLE IF NOT EXISTS events ("
	"    id INTEGER PRIMARY KEY AUTOINCREMENT,"
	"    dataset TEXT NOT NULL,"
	"    txg INTEGER NOT NULL,"
	"    timestamp INTEGER NOT NULL,"
	"    captured_at INTEGER,"
	"    object_id INTEGER NOT NULL,"
	"    event_type TEXT NOT NULL,"
	"    path TEXT,"
	"    old_path TEXT,"
	"    uid INTEGER,"
	"    gid INTEGER,"
	"    mode INTEGER,"
	"    size INTEGER,"
	"    io_offset INTEGER,"
	"    io_bytes INTEGER,"
	"    parent INTEGER,"
	"    old_parent INTEGER,"
	"    target TEXT,"
	"    old_size INTEGER,"
	"    attrs INTEGER,"
	"    full_path TEXT,"
	"    old_full_path TEXT,"
	"    principal INTEGER,"
	"    UNIQUE(dataset, txg, object_id, event_type, timestamp)"
		");"
	"CREATE INDEX IF NOT EXISTS idx_events_dataset_time "
	"    ON events(dataset, timestamp);"
	"CREATE INDEX IF NOT EXISTS idx_events_object "
	"    ON events(dataset, object_id);"
	"CREATE INDEX IF NOT EXISTS idx_events_path "
	"    ON events(dataset, path);"
	"CREATE TABLE IF NOT EXISTS objmap ("
	"    dataset TEXT NOT NULL,"
	"    object_id INTEGER NOT NULL,"
	"    name TEXT NOT NULL,"
	"    parent INTEGER,"
	"    PRIMARY KEY (dataset, object_id)"
			");"
	"CREATE TABLE IF NOT EXISTS sync_state ("
	"    dataset TEXT PRIMARY KEY,"
	"    last_offset INTEGER NOT NULL,"
	"    last_sync INTEGER NOT NULL,"
	"    ring_guid INTEGER,"
	"    last_lost INTEGER,"
	"    root_id INTEGER"
			");"
	"CREATE TABLE IF NOT EXISTS meta ("
	"    key TEXT PRIMARY KEY,"
	"    value TEXT NOT NULL"
	");";

static const char *insert_event_sql =
	"INSERT OR IGNORE INTO events "
	"(dataset, txg, timestamp, object_id, event_type, path, old_path, "
	"uid, gid, mode, size, io_offset, io_bytes, parent, old_parent, "
	"target, old_size, attrs, captured_at, full_path, old_full_path, "
	"principal) "
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
	"?, ?, ?)";

/*
 * The objmap table is the objid -> (name, parent) graph the
 * full-path resolver walks.  It accumulates the complete graph
 * forever (unlike the kernel ring, where an ancestor's CREATE can
 * wrap away), which is what makes insert-time resolution O(depth)
 * instead of a full-table scan.
 */
static const char *objmap_get_sql =
	"SELECT name, parent FROM objmap "
	"WHERE dataset = ? AND object_id = ?";

static const char *objmap_put_sql =
	"INSERT OR REPLACE INTO objmap "
	"(dataset, object_id, name, parent) VALUES (?, ?, ?, ?)";

static const char *objmap_del_sql =
	"DELETE FROM objmap WHERE dataset = ? AND object_id = ?";

static const char *objmap_any_sql =
	"SELECT 1 FROM objmap WHERE dataset = ? LIMIT 1";

/*
 * Object tags (DB layout 9, issue #13): S3-style string:string
 * key/value pairs keyed on (dataset, object_id, key).  The PRIMARY
 * KEY gives replace-set semantics (INSERT OR REPLACE) and lets
 * clear-all be one DELETE per object.  S3 limits (<= 10 tags per
 * object, key <= 128 characters, value <= 256 bytes) are enforced
 * in zmetad_db_tag_set(), the single write path.
 *
 * captured_at is unix seconds, the events.captured_at convention.
 *
 * Lifetime: tags are removed when a REMOVE event for the object is
 * ingested (zmetad_db_insert_event) and by zmetad --purge.  They do
 * NOT age out by time -- S3 tags live until object deletion or an
 * explicit clear -- so retention (zmetad_db_cleanup) never touches
 * this table.  RENAME needs no action: object_id is stable across
 * renames, and tags key on (dataset, object_id), not on the name.
 */
static const char *tags_sql =
	"CREATE TABLE IF NOT EXISTS tags ("
	"    dataset TEXT NOT NULL,"
	"    object_id INTEGER NOT NULL,"
	"    key TEXT NOT NULL,"
	"    value TEXT NOT NULL,"
	"    captured_at INTEGER NOT NULL,"
	"    PRIMARY KEY (dataset, object_id, key)"
			");";

static const char *tag_put_sql =
	"INSERT OR REPLACE INTO tags "
	"(dataset, object_id, key, value, captured_at) "
	"VALUES (?, ?, ?, ?, ?)";

static const char *tag_get_sql =
	"SELECT key, value FROM tags "
	"WHERE dataset = ? AND object_id = ?";

static const char *tag_del_obj_sql =
	"DELETE FROM tags WHERE dataset = ? AND object_id = ?";

static const char *get_last_offset_sql =
	"SELECT last_offset FROM sync_state WHERE dataset = ?";

static const char *get_ring_guid_sql =
	"SELECT ring_guid FROM sync_state WHERE dataset = ?";

/*
 * Single watermark write: last_offset + ring_guid together, so a
 * ring swap persists the reset offset and the new identity in one
 * statement.  ring_guid binding of NULL stores "identity unknown"
 * WITHOUT erasing a previously stored identity: the DO UPDATE keeps
 * the stored guid when the incoming one is NULL (a legacy reply
 * binds NULL upstream), so mixed-version operation cannot lose swap
 * detection.  Only a non-NULL (nonzero) incoming guid replaces it.
 * The DO UPDATE touches exactly these three columns; last_lost and
 * root_id are owned by their own writers and survive this upsert.
 */
static const char *set_last_offset_sql =
	"INSERT INTO sync_state "
	"(dataset, last_offset, last_sync, ring_guid) VALUES (?, ?, ?, ?) "
	"ON CONFLICT(dataset) DO UPDATE SET "
	"last_offset = excluded.last_offset, "
	"last_sync = excluded.last_sync, "
	"ring_guid = CASE WHEN excluded.ring_guid IS NULL "
	"THEN sync_state.ring_guid ELSE excluded.ring_guid END";

/*
 * last_lost is written on its own (each poll that observed the
 * records_lost counter) and must survive the per-poll
 * set_last_offset upsert above, so the offset writer uses
 * DO UPDATE on exactly its own columns.
 */
static const char *get_last_lost_sql =
	"SELECT last_lost FROM sync_state WHERE dataset = ?";

static const char *set_last_lost_sql =
	"INSERT INTO sync_state (dataset, last_offset, last_sync, last_lost) "
	"VALUES (?, 0, 0, ?) "
	"ON CONFLICT(dataset) DO UPDATE SET last_lost = excluded.last_lost";

/*
 * Persisted dataset root object id (DB layout 7): the full-path
 * resolver's fallback when a poll reply carries no "root_objid"
 * (legacy kernel).  Own upsert, like last_lost: the offset writer
 * must not touch this column and this writer must not move the
 * watermark (insert path uses the NOT NULL defaults 0/0, which a
 * conflicting row's DO UPDATE never observes).
 */
static const char *set_root_id_sql =
	"INSERT INTO sync_state "
	"(dataset, last_offset, last_sync, root_id) VALUES (?, 0, 0, ?) "
	"ON CONFLICT(dataset) DO UPDATE SET root_id = excluded.root_id";

static const char *get_root_id_sql =
	"SELECT root_id FROM sync_state WHERE dataset = ?";

/*
 * Per-dataset purge epoch (meta key 'purge_epoch:<dataset>').  The
 * bump is ONE atomic upsert statement: the old read-then-write pair
 * could lose an increment when a --purge raced another --purge.
 * Per-dataset (not one global key) so purging dataset A does not
 * re-arm datasets B and C's loss state for a poll.
 */
static const char *get_purge_epoch_sql =
	"SELECT CAST(value AS INTEGER) FROM meta WHERE key = ?";

static const char *bump_purge_epoch_sql =
	"INSERT INTO meta (key, value) VALUES (?, '1') "
	"ON CONFLICT(key) DO UPDATE SET value = "
	"CAST(CAST(value AS INTEGER) + 1 AS TEXT)";

static const char *purge_dataset_sql[] = {
	"DELETE FROM events WHERE dataset = ?",
	"DELETE FROM gaps WHERE dataset = ?",
	"DELETE FROM sync_state WHERE dataset = ?",
	"DELETE FROM objmap WHERE dataset = ?",
	"DELETE FROM tags WHERE dataset = ?",
};

static const char *get_meta_sql =
	"SELECT value FROM meta WHERE key = ?";

static const char *set_meta_sql =
	"INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)";

static const char *cleanup_sql =
	"DELETE FROM events WHERE captured_at IS NOT NULL AND "
	"captured_at < ?";

/*
 * Incremental consumer query (one-shot --query; SCHEMA.md Section
 * 8.5).  dataset-filtered, id-cursored, id-ordered, LIMIT-clamped;
 * the +1 probe row detects truncation without buffering the result.
 */
static const char *query_events_sql =
	"SELECT id, dataset, txg, timestamp, captured_at, object_id, "
	"event_type, path, old_path, uid, gid, mode, size, io_offset, "
	"io_bytes, parent, old_parent, target, old_size, attrs, "
	"full_path, old_full_path, principal "
	"FROM events WHERE dataset = ? AND id > ? ORDER BY id ASC "
	"LIMIT ?";

static const char *query_loss_sql =
	"SELECT 1 FROM gaps WHERE dataset = ? LIMIT 1";

/*
 * Explicit transaction wrappers.  BEGIN IMMEDIATE takes the write
 * lock up front so a concurrent writer hits the busy timeout
 * instead of deadlocking on a deferred lock upgrade.
 */
int
zmetad_db_begin(zmetad_db_t *db)
{
	char *errmsg = NULL;
	int rc;

	rc = sqlite3_exec(db->sqlite, "BEGIN IMMEDIATE", NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		db_warn(db, "BEGIN error: %s\n",
		    errmsg != NULL ? errmsg : sqlite3_errmsg(db->sqlite));
		sqlite3_free(errmsg);
		return (EIO);
	}
	return (0);
}

int
zmetad_db_commit(zmetad_db_t *db)
{
	char *errmsg = NULL;
	int rc;

	rc = sqlite3_exec(db->sqlite, "COMMIT", NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		db_warn(db, "COMMIT error: %s\n",
		    errmsg != NULL ? errmsg : sqlite3_errmsg(db->sqlite));
		sqlite3_free(errmsg);
		return (EIO);
	}
	return (0);
}

int
zmetad_db_rollback(zmetad_db_t *db)
{
	char *errmsg = NULL;
	int rc;

	rc = sqlite3_exec(db->sqlite, "ROLLBACK", NULL, NULL, &errmsg);
	if (rc != SQLITE_OK && rc != SQLITE_ERROR) {
		/*
		 * SQLITE_ERROR here usually means "no transaction is
		 * active" (already rolled back); not an error worth
		 * propagating.
		 */
		db_warn(db, "ROLLBACK error: %s\n",
		    errmsg != NULL ? errmsg : sqlite3_errmsg(db->sqlite));
		sqlite3_free(errmsg);
		return (EIO);
	}
	sqlite3_free(errmsg);
	return (0);
}

/*
 * Read a value from the meta table.  Returns 0 and sets *out (caller
 * frees) when the key exists; ENOENT when absent.
 */
static int
db_get_meta(zmetad_db_t *db, const char *key, char **out)
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	*out = NULL;

	rc = sqlite3_prepare_v2(db->sqlite, get_meta_sql, -1, &stmt, NULL);
	if (rc != SQLITE_OK)
		return (EIO);

	rc = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		(void) sqlite3_finalize(stmt);
		return (EIO);
	}
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		const unsigned char *val = sqlite3_column_text(stmt, 0);

		*out = strdup(val != NULL ? (const char *)val : "");
		(void) sqlite3_finalize(stmt);
		if (*out == NULL)
			return (ENOMEM);
		return (0);
	}
	(void) sqlite3_finalize(stmt);
	return (rc == SQLITE_DONE ? ENOENT : EIO);
}

static int
db_set_meta(zmetad_db_t *db, const char *key, const char *value)
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	rc = sqlite3_prepare_v2(db->sqlite, set_meta_sql, -1, &stmt, NULL);
	if (rc != SQLITE_OK)
		return (EIO);

	rc = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		(void) sqlite3_finalize(stmt);
		return (EIO);
	}
	rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	return (rc == SQLITE_DONE ? 0 : EIO);
}

/*
 * Read a numeric meta value.  0 and *outp = value when present,
 * ENOENT when absent, EIO on query failure; a non-numeric stored
 * value is EINVAL (corrupt key).
 */
static int
db_get_meta_u64(zmetad_db_t *db, const char *key, uint64_t *outp)
{
	char *str = NULL;
	char *endptr = NULL;
	uint64_t v;
	int rc;

	rc = db_get_meta(db, key, &str);
	if (rc != 0)
		return (rc);

	v = strtoull(str, &endptr, 10);
	if (str[0] == '\0' || endptr == str ||
	    (endptr != NULL && *endptr != '\0'))
		rc = EINVAL;
	else {
		*outp = v;
		rc = 0;
	}
	free(str);
	return (rc);
}

/*
 * ALTER TABLE ADD COLUMN helper: succeeds, and reports *added =
 * B_FALSE, when the column already exists ("duplicate column
 * name"), so a migration that died after applying some columns
 * recovers on retry.  Any other error is reported and returned.
 */
static int
db_add_column(zmetad_db_t *db, const char *table, const char *name,
    const char *type, boolean_t *added)
{
	char sql[128];
	char *errmsg = NULL;
	int rc;

	*added = B_FALSE;

	(void) snprintf(sql, sizeof (sql),
	    "ALTER TABLE %s ADD COLUMN %s %s", table, name, type);
	rc = sqlite3_exec(db->sqlite, sql, NULL, NULL, &errmsg);
	if (rc == SQLITE_OK) {
		*added = B_TRUE;
		return (0);
	}
	if (errmsg != NULL &&
	    strstr(errmsg, "duplicate column name") != NULL) {
		sqlite3_free(errmsg);
		return (0);
	}
	db_warn(db, "migration error adding %s.%s: %s\n", table, name,
	    errmsg != NULL ? errmsg : "unknown");
	sqlite3_free(errmsg);
	return (EIO);
}

/*
 * Apply one migration stage atomically: BEGIN IMMEDIATE, run the
 * stage's ALTERs, record the stage's OWN version in meta, COMMIT.
 * Recording the stage version (not the final one) inside the same
 * transaction means a crash can never leave a database that claims
 * a version whose columns are missing; a crash mid-transaction
 * rolls the ALTERs back and the retry re-applies them (the
 * duplicate-column tolerance covers any leftover).
 *
 * The stamp never moves BACKWARDS: a stage also runs to repair a
 * column set an aborted older migration left incomplete (see
 * db_check_layout), and such a database can already be stamped at or
 * above this stage.  Recording the lower version there would make
 * the stamp disagree with the columns it just repaired, so take the
 * maximum of the stage version and whatever is already stored.
 */
static int
db_migrate_stage(zmetad_db_t *db, unsigned long stage_version,
    const char *table, const db_column_t *cols, size_t ncols)
{
	char version_str[16];
	char *stored = NULL;
	unsigned long record_version = stage_version;
	boolean_t added_any = B_FALSE;
	boolean_t added;
	int rc;

	rc = zmetad_db_begin(db);
	if (rc != 0)
		return (rc);

	for (size_t i = 0; i < ncols; i++) {
		rc = db_add_column(db, table, cols[i].name, cols[i].type,
		    &added);
		if (rc != 0) {
			(void) zmetad_db_rollback(db);
			return (rc);
		}
		added_any = added_any || added;
	}

	if (db_get_meta(db, "db_schema_version", &stored) == 0 &&
	    stored != NULL) {
		char *end = NULL;
		unsigned long cur = strtoul(stored, &end, 10);

		if (end != stored && *end == '\0' &&
		    cur > record_version)
			record_version = cur;
	}
	free(stored);

	(void) snprintf(version_str, sizeof (version_str), "%lu",
	    record_version);
	rc = db_set_meta(db, "db_schema_version", version_str);
	if (rc != 0) {
		db_warn(db, "Failed to record database "
		    "layout version %lu\n", record_version);
		(void) zmetad_db_rollback(db);
		return (rc);
	}

	rc = zmetad_db_commit(db);
	if (rc != 0) {
		(void) zmetad_db_rollback(db);
		return (rc);
	}

	/*
	 * A stage can run with zero ALTERs applied: a fresh database
	 * already has every column (schema_sql) and only lacks the
	 * version stamp, as does one recovering from a stamp-only
	 * failure.  Announcing "upgraded" there is noise.
	 */
	if (added_any)
		db_warn(db, "upgraded database to layout version %lu\n",
		    stage_version);
	return (0);
}

/*
 * Column-probe for one migration stage: returns B_TRUE when EVERY
 * column of the stage is already present on the table.  A probe
 * prepare failure for a missing column means the stage still has
 * work to do; any other failure (corrupt database) is reported as
 * "not applied" and the stage's duplicate-tolerant ALTER loop will
 * surface the real error.
 */
static boolean_t
db_columns_present(zmetad_db_t *db, const char *table,
    const db_column_t *cols, size_t ncols)
{
	for (size_t i = 0; i < ncols; i++) {
		char sql[128];
		sqlite3_stmt *probe = NULL;
		int rc;

		(void) snprintf(sql, sizeof (sql),
		    "SELECT %s FROM %s LIMIT 1", cols[i].name, table);
		rc = sqlite3_prepare_v2(db->sqlite, sql, -1, &probe, NULL);
		if (rc == SQLITE_OK)
			(void) sqlite3_finalize(probe);
		if (rc != SQLITE_OK)
			return (B_FALSE);
	}
	return (B_TRUE);
}

/*
 * Ensure the database layout version matches this build.  Layouts
 * evolve additively via ALTER TABLE ADD COLUMN stages:
 *   2: events graph columns   3: sync_state.ring_guid
 *   4: events.captured_at     5: events full_path + objmap
 *   6: sync_state.last_lost   7: sync_state.root_id
 *   8: events.principal       9: tags table (created by
 * schema_sql on every open, so the stage only stamps the version)
 * The added columns are NULL for old rows, which is the correct
 * representation for fields absent from those records.  Each stage
 * runs in one transaction with its own version stamp, so schema and
 * version can never diverge after a crash.  A database written by a
 * NEWER layout is refused.
 *
 * A stage runs when its version is due OR when its columns are
 * actually missing, so a database stamped by an aborted older
 * migration is repaired instead of being advanced past its missing
 * columns forever.
 *
 * Handle ownership: this function NEVER closes db->sqlite --
 * zmetad_db_open() owns the handle lifecycle and closes it on every
 * error return.
 */
static int
db_check_layout(zmetad_db_t *db)
{
	char *stored_version = NULL;
	char *endptr = NULL;
	unsigned long v;
	int rc;

	rc = db_get_meta(db, "db_schema_version", &stored_version);
	if (rc == ENOENT) {
		/*
		 * No key: a fresh database (schema_sql already created
		 * every current column) or a pre-versioning layout.
		 * Start the chain at 1 and let the per-stage column
		 * probes below decide which stages actually run.
		 */
		v = 1;
	} else if (rc != 0) {
		fprintf(stderr, "cannot read db_schema_version\n");
		return (rc);
	} else {
		v = strtoul(stored_version, &endptr, 10);
		if (stored_version[0] == '\0' || endptr == stored_version ||
		    (endptr != NULL && *endptr != '\0')) {
			fprintf(stderr, "invalid db_schema_version '%s'\n",
			    stored_version);
			free(stored_version);
			return (EINVAL);
		}
		free(stored_version);
		if (v > ZMETAD_DB_SCHEMA_VERSION) {
			fprintf(stderr, "database layout version mismatch: "
			    "stored=%lu loaded=%u; recreate the database or "
			    "run an older zmetad\n", v,
			    ZMETAD_DB_SCHEMA_VERSION);
			return (EINVAL);
		}
	}

	/*
	 * Every stage runs when the stored version says it is due OR when
	 * the stage's columns are actually missing.  The version test
	 * alone is not enough: builds up to 276fe5085 applied the v2
	 * ALTERs outside a transaction and stamped '2' unconditionally,
	 * so a crash mid-ALTER left a '2'-stamped database missing
	 * columns that every later open advanced past, then failed
	 * forever at prepare ("no column named parent").  Probing the
	 * live column set repairs such a database in one open: the
	 * stages are duplicate-tolerant and db_migrate_stage never moves
	 * the stamp backwards, so repair and forward migration share the
	 * same path.
	 */
	if (v < 2 || !db_columns_present(db, "events", db_v2_columns,
	    NDBCOLS(db_v2_columns))) {
		rc = db_migrate_stage(db, 2, "events", db_v2_columns,
		    NDBCOLS(db_v2_columns));
		if (rc != 0)
			return (rc);
		v = (v < 2) ? 2 : v;
	}

	/*
	 * Version 1/2 -> 3: sync_state gains the ring_guid column.
	 * The stage records version 3 (its own), not the final
	 * version, and runs ALTER+stamp in one transaction.
	 */
	if (v < 3 || !db_columns_present(db, "sync_state", db_v3_columns,
	    NDBCOLS(db_v3_columns))) {
		rc = db_migrate_stage(db, 3, "sync_state",
		    db_v3_columns, NDBCOLS(db_v3_columns));
		if (rc != 0)
			return (rc);
		v = (v < 3) ? 3 : v;
	}

	/*
	 * Version 3 -> 4: events gains captured_at (unix seconds at
	 * ingest).  events.timestamp holds the kernel wire 'time'
	 * value, which is gethrtime() nanoseconds since boot - not a
	 * wall clock - so retention could never compare it against a
	 * time(NULL) cutoff.  Pre-v4 rows get NULL: retention leaves
	 * them (they are either recent or the operator purges).
	 */
	{
		static const db_column_t v4_columns[] = {
			{ "captured_at", "INTEGER" },
		};

		if (v < 4 || !db_columns_present(db, "events", v4_columns,
		    NDBCOLS(v4_columns))) {
			rc = db_migrate_stage(db, 4, "events",
			    v4_columns, NDBCOLS(v4_columns));
			if (rc != 0)
				return (rc);
			v = (v < 4) ? 4 : v;
		}
	}

	/*
	 * Version 4 -> 5: events gains full_path/old_full_path (the
	 * dataset-relative path resolved at insert time; NULL when the
	 * ancestor chain is unresolvable), and the objmap table carries
	 * the objid -> (name, parent) graph the resolver walks.  Pre-v5
	 * rows keep NULL full_path and remain PARTIAL under the
	 * documented conservative-match rules.
	 */
	{
		static const db_column_t v5_columns[] = {
			{ "full_path",		"TEXT" },
			{ "old_full_path",	"TEXT" },
		};

		if (v < 5 || !db_columns_present(db, "events", v5_columns,
		    NDBCOLS(v5_columns))) {
			rc = db_migrate_stage(db, 5, "events",
			    v5_columns, NDBCOLS(v5_columns));
			if (rc != 0)
				return (rc);
			v = (v < 5) ? 5 : v;
		}
	}

	/*
	 * Version 5 -> 6: sync_state gains last_lost, the previous
	 * poll's cumulative records_lost.  NULL on old rows means "no
	 * baseline", so the first poll after an upgrade does not
	 * invent a gap for history that predates the column; a wrap
	 * that happens while the daemon is down becomes a delta
	 * against the stored counter instead of a silent re-arm.
	 */
	{
		static const db_column_t v6_columns[] = {
			{ "last_lost",		"INTEGER" },
		};

		if (v < 6 || !db_columns_present(db, "sync_state", v6_columns,
		    NDBCOLS(v6_columns))) {
			rc = db_migrate_stage(db, 6, "sync_state",
			    v6_columns, NDBCOLS(v6_columns));
			if (rc != 0)
				return (rc);
			v = (v < 6) ? 6 : v;
		}
	}

	/*
	 * Version 6 -> 7: sync_state gains root_id, the persisted
	 * dataset root object id.  Legacy kernels (GET_EVENTS without
	 * "root_objid") previously degraded root-level records to
	 * PARTIAL permanently once the objmap graph was non-empty;
	 * with the root id persisted on first sighting, later polls
	 * fall back to the STORED id when the reply lacks one.
	 * Pre-v7 rows get NULL: 0/NULL means "no stored root id --
	 * legacy kernel + heuristic fallback" (see SCHEMA.md).
	 */
	{
		static const db_column_t v7_columns[] = {
			{ "root_id",		"INTEGER" },
		};

		if (v < 7 || !db_columns_present(db, "sync_state", v7_columns,
		    NDBCOLS(v7_columns))) {
			rc = db_migrate_stage(db, 7, "sync_state",
			    v7_columns, NDBCOLS(v7_columns));
			if (rc != 0)
				return (rc);
			v = (v < 7) ? 7 : v;
		}
	}

	/*
	 * Version 7 -> 8: events gains principal, the opaque
	 * application principal tag carried by ZFS_EV_PRINCIPAL
	 * (gh zeta-object#7).  It is supplied by the writing
	 * application through userspace and is NOT verified by the
	 * kernel -- stored verbatim as a claim, never evidence.
	 * Unregistered writers send no key: pre-v8 rows get NULL,
	 * which is also the correct on-disk representation for
	 * every record whose writer never registered a principal.
	 */
	{
		static const db_column_t v8_columns[] = {
			{ "principal",		"INTEGER" },
		};

		if (v < 8 || !db_columns_present(db, "events", v8_columns,
		    NDBCOLS(v8_columns))) {
			rc = db_migrate_stage(db, 8, "events",
			    v8_columns, NDBCOLS(v8_columns));
			if (rc != 0)
				return (rc);
			v = (v < 8) ? 8 : v;
		}
	}

	/*
	 * Version 8 -> 9: the tags table (issue #13).  Like gaps, it
	 * is created by schema_sql on EVERY open (CREATE TABLE IF NOT
	 * EXISTS), so this stage carries no ALTERs: it only stamps
	 * the version so a pre-9 database announces layout 9 once the
	 * table exists.  Old rows are impossible by construction; a
	 * migration that died before committing simply re-runs here.
	 */
	if (v < 9) {
		rc = db_migrate_stage(db, 9, "tags", NULL, 0);
		if (rc != 0)
			return (rc);
		v = 9;
	}

	return (0);
}

int
zmetad_db_open(zmetad_db_t **dbp, const char *path,
    const zmetad_schema_t *zs)
{
	zmetad_db_t *db;
	char *stored_version = NULL;
	char version_str[32];
	int rc;

	db = calloc(1, sizeof (*db));
	if (db == NULL) {
		return (ENOMEM);
	}

	rc = sqlite3_open(path, &db->sqlite);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQLite open error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	/*
	 * A one-shot --purge runs while a live daemon may hold the
	 * write lock (its poll writes are frequent).  Without a busy
	 * handler the open-time schema-version write fails immediately
	 * with SQLITE_BUSY and purge reports a database error.  Wait
	 * up to 5s for the lock instead.  Set BEFORE the WAL pragma:
	 * the mode switch itself can contend with another connection.
	 */
	rc = sqlite3_busy_timeout(db->sqlite, 5000);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQLite busy timeout error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	/*
	 * Enable WAL mode for better concurrency, and VERIFY it took:
	 * a silent fallback to the rollback journal (e.g. the database
	 * is on a filesystem without mmap support) would break the
	 * documented concurrent-reader contract, so warn loudly.
	 */
	{
		sqlite3_stmt *pragma = NULL;
		boolean_t is_wal = B_FALSE;

		rc = sqlite3_prepare_v2(db->sqlite,
		    "PRAGMA journal_mode=WAL", -1, &pragma, NULL);
		if (rc == SQLITE_OK && sqlite3_step(pragma) == SQLITE_ROW) {
			const unsigned char *mode =
			    sqlite3_column_text(pragma, 0);

			is_wal = (mode != NULL &&
			    strcasecmp((const char *)mode, "wal") == 0);
		}
		if (pragma != NULL)
			(void) sqlite3_finalize(pragma);
		if (!is_wal) {
			fprintf(stderr, "warning: database %s is not in "
			    "WAL mode; concurrent readers may block\n",
			    path);
		}
	}

	/* Create schema */
	char *errmsg = NULL;
	rc = sqlite3_exec(db->sqlite, schema_sql, NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Schema creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_exec(db->sqlite, gaps_sql, NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Gaps table creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_exec(db->sqlite, datasets_sql, NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Datasets table creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_exec(db->sqlite, tags_sql, NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Tags table creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	/*
	 * Dataset-scoped lookups (tag get, --purge, phase-2 CLI
	 * listings); the PRIMARY KEY already serves point lookups
	 * by (dataset, object_id, key).
	 */
	rc = sqlite3_exec(db->sqlite,
	    "CREATE INDEX IF NOT EXISTS idx_tags_dataset "
	    "ON tags(dataset)", NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Tags index creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	/*
	 * Negotiate the STORED event record schema version BEFORE any
	 * layout mutation.  Reading it after db_check_layout() is too
	 * late: the layout check stamps db_schema_version forward in its
	 * own transaction, so a database written by a previous build
	 * would be advanced and then refused -- leaving a database the
	 * previous build no longer recognises as its own ("stored
	 * version is newer") and this build refuses too, bricking it.
	 *
	 * The stored value is the wire record schema version the previous
	 * run wrote with.  Record schema versions evolve ADDITIVELY only
	 * (a schema_version bump adds fields or op enum values; see
	 * zmetad_schema_check_version), so a database written with an
	 * OLDER schema is fully readable by this build: field decode is
	 * driven by the loaded schema, absent fields surface as ENOENT and
	 * stay NULL, and unknown op values decode as "UNKNOWN".  ACCEPT a
	 * stored version <= ours; REFUSE only a stored version NEWER than
	 * ours (records this build may not decode faithfully) or a
	 * non-numeric stamp (a value this code cannot order).  A missing
	 * key is a fresh or pre-versioning database and is accepted.
	 * db->schema and version_str are established here, with the read.
	 */
	db->schema = zs;
	if (zs != NULL) {
		unsigned long long stored_numeric, loaded_numeric;
		char *vend = NULL;

		loaded_numeric =
		    (unsigned long long)zmetad_schema_version(zs);
		(void) snprintf(version_str, sizeof (version_str), "%llu",
		    loaded_numeric);

		rc = db_get_meta(db, "events_schema_version",
		    &stored_version);
		if (rc == 0) {
			errno = 0;
			stored_numeric = strtoull(stored_version, &vend, 10);
			if (stored_version[0] == '\0' ||
			    vend == stored_version || *vend != '\0' ||
			    errno != 0) {
				fprintf(stderr, "database event schema "
				    "version mismatch: stored='%s' is not a "
				    "numeric version; loaded=%llu.  "
				    "Recreate the database or move it "
				    "aside.\n", stored_version,
				    loaded_numeric);
				free(stored_version);
				sqlite3_close(db->sqlite);
				free(db);
				return (EINVAL);
			}
			free(stored_version);
			if (stored_numeric > loaded_numeric) {
				fprintf(stderr, "database event schema "
				    "version mismatch: stored=%llu "
				    "loaded=%llu; this database was written "
				    "by a NEWER zmetad.  Recreate it or run "
				    "that newer build.\n", stored_numeric,
				    loaded_numeric);
				sqlite3_close(db->sqlite);
				free(db);
				return (EINVAL);
			}
			/* stored_numeric <= loaded_numeric: accept. */
		} else if (rc != ENOENT) {
			fprintf(stderr,
			    "cannot read events_schema_version\n");
			sqlite3_close(db->sqlite);
			free(db);
			return (rc);
		}
	}

	/*
	 * datasets.last_seen for databases created before the
	 * stale-row prune existed.  Duplicate-tolerant; no layout
	 * version bump (see the comment above datasets_sql: a
	 * tolerant ALTER keeps old and new databases converged
	 * without version churn).
	 */
	{
		boolean_t added;

		rc = db_add_column(db, "datasets", "last_seen", "INTEGER",
		    &added);
		if (rc != 0) {
			sqlite3_close(db->sqlite);
			free(db);
			return (rc);
		}
	}

	rc = db_check_layout(db);
	if (rc != 0) {
		sqlite3_close(db->sqlite);
		free(db);
		return (rc);
	}

	/*
	 * captured_at index for the retention DELETE and VACUUM, which
	 * would otherwise full-scan the largest table under the 5s
	 * busy timeout.  Executed AFTER the layout check (which may
	 * ALTER the column in) and on every open, so existing
	 * databases gain it too.  Adding an index needs no
	 * layout-version bump: it changes no column set, so any build
	 * tolerates its presence or absence.
	 */
	rc = sqlite3_exec(db->sqlite,
	    "CREATE INDEX IF NOT EXISTS idx_events_captured_at "
	    "ON events(captured_at)", NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Index creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	/*
	 * full_path index: same ordering rule as captured_at above -
	 * schema_sql cannot carry it because on an existing pre-v5
	 * database the events table gains full_path only in the
	 * layout migration, which runs after schema_sql.
	 */
	rc = sqlite3_exec(db->sqlite,
	    "CREATE INDEX IF NOT EXISTS idx_events_full_path "
	    "ON events(dataset, full_path)", NULL, NULL, &errmsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Index creation error: %s\n", errmsg);
		sqlite3_free(errmsg);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	/*
	 * Stamp the schema version this database now holds.  The stored
	 * version was validated above (accepted or refused), and the
	 * layout migration has completed, so the stamp never advertises
	 * columns or a record format the database does not actually have.
	 * A database written by an older build is accepted and advanced
	 * to ours in this one open.
	 */
	if (zs != NULL) {
		rc = db_set_meta(db, "events_schema_version", version_str);
		if (rc != 0) {
			fprintf(stderr, "Failed to record schema version\n");
			sqlite3_close(db->sqlite);
			free(db);
			return (rc);
		}
	}

	/* Prepare statements */
	rc = sqlite3_prepare_v2(db->sqlite, insert_event_sql, -1,
	    &db->insert_event_stmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Prepare insert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_prepare_v2(db->sqlite, get_last_offset_sql, -1,
	    &db->get_last_offset_stmt, NULL);
	if (rc != SQLITE_OK) {
		(void) sqlite3_finalize(db->insert_event_stmt);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_prepare_v2(db->sqlite, set_last_offset_sql, -1,
	    &db->set_last_offset_stmt, NULL);
	if (rc != SQLITE_OK) {
		(void) sqlite3_finalize(db->insert_event_stmt);
		(void) sqlite3_finalize(db->get_last_offset_stmt);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_prepare_v2(db->sqlite, get_ring_guid_sql, -1,
	    &db->get_ring_guid_stmt, NULL);
	if (rc != SQLITE_OK) {
		(void) sqlite3_finalize(db->insert_event_stmt);
		(void) sqlite3_finalize(db->get_last_offset_stmt);
		(void) sqlite3_finalize(db->set_last_offset_stmt);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_prepare_v2(db->sqlite, prune_stale_datasets_sql, -1,
	    &db->prune_datasets_stmt, NULL);
	if (rc != SQLITE_OK) {
		(void) sqlite3_finalize(db->insert_event_stmt);
		(void) sqlite3_finalize(db->get_last_offset_stmt);
		(void) sqlite3_finalize(db->set_last_offset_stmt);
		(void) sqlite3_finalize(db->get_ring_guid_stmt);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	rc = sqlite3_prepare_v2(db->sqlite, objmap_get_sql, -1,
	    &db->objmap_get_stmt, NULL);
	if (rc == SQLITE_OK)
		rc = sqlite3_prepare_v2(db->sqlite, objmap_put_sql, -1,
		    &db->objmap_put_stmt, NULL);
	if (rc == SQLITE_OK)
		rc = sqlite3_prepare_v2(db->sqlite, objmap_del_sql, -1,
		    &db->objmap_del_stmt, NULL);
	if (rc == SQLITE_OK)
		rc = sqlite3_prepare_v2(db->sqlite, objmap_any_sql, -1,
		    &db->objmap_any_stmt, NULL);
	if (rc == SQLITE_OK)
		rc = sqlite3_prepare_v2(db->sqlite, tag_del_obj_sql, -1,
		    &db->tag_del_obj_stmt, NULL);
	if (rc == SQLITE_OK)
		rc = sqlite3_prepare_v2(db->sqlite, get_last_lost_sql, -1,
		    &db->get_last_lost_stmt, NULL);
	if (rc == SQLITE_OK)
		rc = sqlite3_prepare_v2(db->sqlite, set_last_lost_sql, -1,
		    &db->set_last_lost_stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(db->insert_event_stmt);
		sqlite3_finalize(db->get_last_offset_stmt);
		sqlite3_finalize(db->set_last_offset_stmt);
		sqlite3_finalize(db->get_ring_guid_stmt);
		sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	*dbp = db;
	return (0);
}

void
zmetad_db_close(zmetad_db_t *db)
{
	if (db == NULL)
		return;

	if (db->insert_event_stmt)
		(void) sqlite3_finalize(db->insert_event_stmt);
	if (db->get_last_offset_stmt)
		(void) sqlite3_finalize(db->get_last_offset_stmt);
	if (db->set_last_offset_stmt)
		(void) sqlite3_finalize(db->set_last_offset_stmt);
	if (db->get_ring_guid_stmt)
		(void) sqlite3_finalize(db->get_ring_guid_stmt);
	if (db->objmap_get_stmt)
		(void) sqlite3_finalize(db->objmap_get_stmt);
	if (db->objmap_put_stmt)
		(void) sqlite3_finalize(db->objmap_put_stmt);
	if (db->objmap_del_stmt)
		(void) sqlite3_finalize(db->objmap_del_stmt);
	if (db->objmap_any_stmt)
		(void) sqlite3_finalize(db->objmap_any_stmt);
	if (db->tag_del_obj_stmt)
		(void) sqlite3_finalize(db->tag_del_obj_stmt);
	if (db->get_last_lost_stmt)
		(void) sqlite3_finalize(db->get_last_lost_stmt);
	if (db->set_last_lost_stmt)
		(void) sqlite3_finalize(db->set_last_lost_stmt);
	if (db->prune_datasets_stmt)
		(void) sqlite3_finalize(db->prune_datasets_stmt);
	if (db->sqlite)
		sqlite3_close(db->sqlite);

	free(db);
}

/*
 * One-shot --query opener: open an existing database WITHOUT any of
 * zmetad_db_open()'s write-side effects (schema creation, ALTER
 * migrations, version stamping, WAL switch).  A query must not mutate
 * a database a live daemon owns, and must not "create" a database out
 * of a typo'd path.  READWRITE (not pure READONLY) because SQLite
 * cannot read a WAL database through a read-only connection unless a
 * -shm exists and the reader can create it; the SQL this connection
 * runs is all SELECTs, so the daemon's writer role is never contested.
 * Busy timeout as in zmetad_db_open: a --query racing the daemon's
 * poll write waits instead of failing.
 */
int
zmetad_db_open_readonly(zmetad_db_t **dbp, const char *path)
{
	zmetad_db_t *db;
	int rc;

	/*
	 * Existence check BEFORE the open: sqlite3_open_v2 without
	 * CREATE reports generic SQLITE_CANTOPEN, not ENOENT, so a
	 * typo'd path would read as a permissions problem.
	 */
	if (access(path, F_OK) != 0) {
		fprintf(stderr, "cannot open database %s: %s (--query "
		    "reads an existing database; it does not create "
		    "one)\n", path, strerror(errno));
		return (errno == ENOENT ? ENOENT : EIO);
	}

	db = calloc(1, sizeof (*db));
	if (db == NULL)
		return (ENOMEM);

	rc = sqlite3_open_v2(path, &db->sqlite,
	    SQLITE_OPEN_READWRITE, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "cannot open database %s: %s\n", path,
		    db->sqlite != NULL ?
		    sqlite3_errmsg(db->sqlite) : "out of memory");
		if (db->sqlite != NULL)
			sqlite3_close(db->sqlite);
		free(db);
		return (EIO);
	}

	(void) sqlite3_busy_timeout(db->sqlite, 5000);

	*dbp = db;
	return (0);
}

/*
 * Resolve a dataset-relative full path for an object by walking the
 * objmap graph from its parent chain to the root (parent NULL/0).
 * Returns 0 with a malloc'd path in *pathp, ENOENT when any ancestor
 * is unknown (the caller leaves full_path NULL: the row stays PARTIAL
 * under the SCHEMA.md conservative-match rules), or another error.
 * A depth cap guards against corrupt-graph cycles.
 */
#define	ZMETAD_PATH_MAX_DEPTH	64

/*
 * Probe whether the dataset's graph has any rows: 1 when it does, 0
 * when it does not, EIO when the query itself failed.  A query error
 * must not be conflated with "the graph has rows": that turns a
 * transient read failure into a fabricated "broken ancestor chain",
 * and the record is then stored with a permanently NULL full_path.
 */
static int
objmap_graph_has_rows(zmetad_db_t *db, const char *dataset)
{
	sqlite3_stmt *stmt = db->objmap_any_stmt;
	int rc;

	sqlite3_reset(stmt);
	sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	rc = sqlite3_step(stmt);
	sqlite3_reset(stmt);
	if (rc == SQLITE_ROW)
		return (1);
	if (rc == SQLITE_DONE)
		return (0);
	return (EIO);
}

static int objmap_resolve_root(zmetad_db_t *db, const char *dataset,
    uint64_t parent, uint64_t root_id, const char *name, char **pathp);

/*
 * Learned dataset root object ids (GET_EVENTS "root_objid").  The
 * resolver needs the exact root to distinguish "ancestor is the
 * dataset root" (terminus) from "ancestor lost" (PARTIAL): the objmap
 * never maps the root, and root ids vary per dataset, so graph
 * emptiness alone cannot decide once any row exists.
 *
 * Each learned id is also persisted in sync_state.root_id (DB layout
 * 7) so a legacy kernel reply -- no "root_objid" key at all -- can
 * fall back to the STORED id instead of degrading every root-level
 * record to PARTIAL forever.  NULL/0 in the column means "never
 * learned": legacy kernel + heuristic fallback (SCHEMA.md Section 6).
 */
struct root_id_entry {
	char	*dataset;
	uint64_t id;
	struct root_id_entry *next;
};

static struct root_id_entry *g_root_ids;

static void
root_id_cache_put(const char *dataset, uint64_t id)
{
	struct root_id_entry *e;

	for (e = g_root_ids; e != NULL; e = e->next) {
		if (strcmp(e->dataset, dataset) == 0) {
			e->id = id;
			return;
		}
	}

	e = calloc(1, sizeof (*e));
	if (e == NULL)
		return;
	e->dataset = strdup(dataset);
	if (e->dataset == NULL) {
		free(e);
		return;
	}
	e->id = id;
	e->next = g_root_ids;
	g_root_ids = e;
}

static uint64_t
root_id_cache_get(const char *dataset)
{
	struct root_id_entry *e;

	for (e = g_root_ids; e != NULL; e = e->next) {
		if (strcmp(e->dataset, dataset) == 0)
			return (e->id);
	}
	return (0);
}

void
zmetad_db_set_root_id(zmetad_db_t *db, const char *dataset, uint64_t id)
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	/* Cache first: the resolver must see this id this poll. */
	root_id_cache_put(dataset, id);

	rc = sqlite3_prepare_v2(db->sqlite, set_root_id_sql, -1,
	    &stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare root_id upsert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return;
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)id);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE) {
		/*
		 * Non-fatal: this poll still resolves from the cache;
		 * the next sighting retries the persist.
		 */
		db_warn(db, "root_id persist failed for %s: %s\n", dataset,
		    sqlite3_errmsg(db->sqlite));
	}
}

/*
 * The dataset's root object id: the in-memory cache (seeded this
 * poll), else the value persisted in sync_state (DB layout 7; the
 * legacy-kernel fallback), else 0 (never learned).  Returns 0 on
 * success with *idp set, EIO when the lookup itself failed: an error
 * must not masquerade as the "never learned" id 0, or a transient
 * read failure would silently degrade the resolver to the heuristic
 * and store a NULL full_path with no warning.
 */
static int
objmap_root_id(zmetad_db_t *db, const char *dataset, uint64_t *idp)
{
	sqlite3_stmt *stmt = NULL;
	uint64_t id;
	int rc;

	*idp = 0;

	id = root_id_cache_get(dataset);
	if (id != 0) {
		*idp = id;
		return (0);
	}

	rc = sqlite3_prepare_v2(db->sqlite, get_root_id_sql, -1,
	    &stmt, NULL);
	if (rc != SQLITE_OK)
		return (EIO);
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW &&
	    sqlite3_column_type(stmt, 0) != SQLITE_NULL)
		id = (uint64_t)sqlite3_column_int64(stmt, 0);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE)
		return (EIO);

	/*
	 * Seed the cache so repeated resolutions within this poll do
	 * not re-query.
	 */
	if (id != 0)
		root_id_cache_put(dataset, id);
	*idp = id;
	return (0);
}

static int
objmap_resolve(zmetad_db_t *db, const char *dataset, uint64_t parent,
    const char *name, char **pathp)
{
	uint64_t root_id;
	int rc;

	rc = objmap_root_id(db, dataset, &root_id);
	if (rc != 0)
		return (rc);
	return (objmap_resolve_root(db, dataset, parent, root_id, name,
	    pathp));
}

/*
 * root_id: the dataset root object id as reported by the kernel
 * (GET_EVENTS "root_objid"). Nonzero makes "ancestor == root" exact;
 * zero (legacy kernel) falls back to the empty-graph heuristic.
 */
static int
objmap_resolve_root(zmetad_db_t *db, const char *dataset, uint64_t parent,
    uint64_t root_id, const char *name, char **pathp)
{
	char segs[ZMETAD_PATH_MAX_DEPTH][256];
	size_t lens[ZMETAD_PATH_MAX_DEPTH];
	size_t total = 0, off = 0;
	sqlite3_stmt *stmt = db->objmap_get_stmt;
	char *path;
	int depth = 0, rc;

	*pathp = NULL;
	if (name == NULL)
		return (ENOENT);
	lens[0] = strlen(name);
	if (lens[0] >= sizeof (segs[0]))
		return (ENOENT);
	memcpy(segs[0], name, lens[0] + 1);
	total = lens[0];
	if (parent == 0 || parent == UINT64_MAX ||
	    (root_id != 0 && parent == root_id)) {
		/* Already at the dataset root: only the name. */
		goto build;
	}

	for (depth = 0; parent != 0 && parent != UINT64_MAX; depth++) {
		uint64_t next_parent;
		const unsigned char *nm;

		if (depth >= ZMETAD_PATH_MAX_DEPTH - 1)
			return (ENOENT);	/* cycle / corrupt graph */

		if (parent == root_id) {
			/* Dataset root (kernel-reported): terminus. */
			goto build;
		}

		sqlite3_reset(stmt);
		sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
		sqlite3_bind_int64(stmt, 2, (sqlite3_int64)parent);
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE) {
			/*
			 * No objmap row for this ancestor.  With an
			 * otherwise empty graph that ancestor is the
			 * dataset root (never mapped): the chain's
			 * terminus.  Once any row exists, a missing
			 * ancestor is a broken chain -- the CREATE that
			 * would have mapped it was lost -- and the honest
			 * answer is ENOENT: the row keeps NULL full_path
			 * and stays PARTIAL instead of getting a
			 * fabricated shorter path.
			 */
			sqlite3_reset(stmt);
			switch (objmap_graph_has_rows(db, dataset)) {
			case 0:
				goto build;
			case 1:
				return (ENOENT);
			default:
				/* Query error: not a broken chain. */
				return (EIO);
			}
		}
		if (rc != SQLITE_ROW) {
			sqlite3_reset(stmt);
			return (EIO);
		}
		nm = sqlite3_column_text(stmt, 0);
		if (nm != NULL && nm[0] == '\0') {
			/* Root sentinel: terminus, no segment. */
			sqlite3_reset(stmt);
			goto build;
		}
		if (nm == NULL || strlen((const char *)nm) >=
		    sizeof (segs[0])) {
			sqlite3_reset(stmt);
			return (ENOENT);
		}
		lens[depth + 1] = strlen((const char *)nm);
		memcpy(segs[depth + 1], nm, lens[depth + 1] + 1);
		total += lens[depth + 1] + 1;
		next_parent = (uint64_t)sqlite3_column_int64(stmt, 1);
		sqlite3_reset(stmt);
		parent = next_parent;
	}

build:
	path = malloc(total + 1);
	if (path == NULL)
		return (ENOMEM);

	/* Walk segments root-first: they were collected child-first. */
	for (int i = depth; i >= 0; i--) {
		if (i != depth) {
			path[off] = '/';
			off++;
		}
		memcpy(path + off, segs[i], lens[i]);
		off += lens[i];
	}
	path[off] = '\0';
	*pathp = path;
	return (0);
}

/*
 * Update the objmap graph from one decoded record.  CREATE/LINK/
 * SYMLINK/RENAME map the object's new location; REMOVE drops it
 * (its subtree's entries become stale but are only consulted via
 * the parent chain, which now dead-ends -> ENOENT, i.e. PARTIAL).
 * WRITE/READ/SETATTR/TRUNCATE change no graph edges.
 * Returns 0 on success or when the op needs no graph update.
 */
static int
objmap_update(zmetad_db_t *db, const char *dataset, uint64_t op,
    boolean_t have_op, uint64_t object, boolean_t have_object,
    const char *name, boolean_t have_name, uint64_t parent,
    boolean_t have_parent)
{
	sqlite3_stmt *stmt;

	if (!have_op || !have_object || !have_name)
		return (0);

	switch (op) {
	case 1:	/* CREATE */
	case 3:	/* RENAME */
	case 4:	/* LINK */
	case 5:	/* SYMLINK */
		/*
		 * No eager root seeding here: the resolver treats a
		 * missing ancestor as the root while the graph is
		 * empty (objmap_graph_empty), which covers the fresh
		 * dataset without guessing wrong on upgraded
		 * databases (empty graph, deep tree).
		 */
		stmt = db->objmap_put_stmt;
		sqlite3_reset(stmt);
		sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
		sqlite3_bind_int64(stmt, 2, (sqlite3_int64)object);
		sqlite3_bind_text(stmt, 3, name, -1, SQLITE_TRANSIENT);
		sqlite3_bind_int64(stmt, 4, have_parent ?
		    (sqlite3_int64)parent : (sqlite3_int64)0);
		if (sqlite3_step(stmt) != SQLITE_DONE) {
			sqlite3_reset(stmt);
			return (EIO);
		}
		sqlite3_reset(stmt);
		return (0);
	case 2:	/* REMOVE */
		stmt = db->objmap_del_stmt;
		sqlite3_reset(stmt);
		sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
		sqlite3_bind_int64(stmt, 2, (sqlite3_int64)object);
		if (sqlite3_step(stmt) != SQLITE_DONE) {
			sqlite3_reset(stmt);
			return (EIO);
		}
		sqlite3_reset(stmt);
		return (0);
	default:
		return (0);
	}
}

int
zmetad_db_insert_event(zmetad_db_t *db, const char *dataset, nvlist_t *event)
{
	sqlite3_stmt *stmt = db->insert_event_stmt;
	const zmetad_schema_t *zs = db->schema;
	/*
	 * One decoded-value buffer valid for either type the schema can
	 * declare: a uint64_t or a string pointer.  zmetad_schema_field()
	 * decodes into it according to the field's DECLARED type and
	 * reports that type back via dtype, so the binder never has to
	 * guess a field's type up front (see the loop below).
	 */
	union {
		uint64_t	n;
		const char	*s;
	} val;
	uint64_t op = 0;
	boolean_t have_op = B_FALSE;
	uint64_t object = 0;
	boolean_t have_object = B_FALSE;
	uint64_t parent = 0;
	boolean_t have_parent = B_FALSE;
	uint64_t old_parent = 0;
	boolean_t have_old_parent = B_FALSE;
	const char *rec_name = NULL;
	boolean_t have_name = B_FALSE;
	const char *rec_old_name = NULL;
	boolean_t have_old_name = B_FALSE;
	uint_t nelem;
	data_type_t dtype;
	int rc;

	if (zs == NULL)
		return (EINVAL);

	/*
	 * Decode the record schema-driven: every known field is looked
	 * up via zmetad_schema_field() (ENOENT = absent, normal) and
	 * bound to the matching SQL column by field name.  Absent
	 * fields stay NULL.
	 *
	 * The decoded value goes into one union sized for either
	 * representation, and the SQL binding is chosen by the type the
	 * schema actually DECLARED (reported in dtype).  A
	 * hand-maintained list of string field names used to be the only
	 * thing that selected the string representation, so a newly
	 * declared string field decoded as a number and tripped the old
	 * wire-type guard, failing every insert and rolling the whole
	 * batch back forever -- an ingestion livelock until zmetad was
	 * rebuilt.  Reading the type from the schema removes that
	 * duplicated knowledge.
	 */
	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Insert reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_clear_bindings(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Clear bindings error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	for (uint_t i = 0; i < zmetad_schema_nfields(zs); i++) {
		const char *name = zmetad_schema_field_name(zs, i);
		int col = 0;

		/*
		 * Decode with a buffer valid for either declared type;
		 * the schema reports which member it wrote via dtype.
		 */
		val.n = 0;
		val.s = NULL;
		nelem = 0;
		dtype = DATA_TYPE_UNKNOWN;

		rc = zmetad_schema_field(zs, name, event, &val, &nelem,
		    &dtype);
		if (rc == ENOENT)
			continue;
		if (rc != 0) {
			db_warn(db, "Field %s: %s\n", name,
			    strerror(rc));
			return (rc);
		}

		/*
		 * Map the record field to its fixed events column and
		 * cache the cross-field values the resolver needs below.
		 * The bind FUNCTION is chosen from the type the schema
		 * actually DECLARED (dtype), not from a hand-maintained
		 * list of field names: a newly declared string field
		 * arrives in val.s and is bound as text without touching
		 * this switch, and a numeric field in val.n as an int.
		 * "op" has no column of its own (it becomes event_type
		 * further down), and an unknown field maps to no column
		 * and is ignored (forward compatibility).
		 */
		if (strcmp(name, "txg") == 0) {
			col = 2;
		} else if (strcmp(name, "time") == 0) {
			col = 3;
		} else if (strcmp(name, "object") == 0) {
			col = 4;
			object = val.n;
			have_object = B_TRUE;
		} else if (strcmp(name, "op") == 0) {
			op = val.n;
			have_op = B_TRUE;
		} else if (strcmp(name, "name") == 0) {
			col = 6;
			rec_name = val.s;
			have_name = B_TRUE;
		} else if (strcmp(name, "old_name") == 0) {
			col = 7;
			rec_old_name = val.s;
			have_old_name = B_TRUE;
		} else if (strcmp(name, "uid") == 0) {
			col = 8;
		} else if (strcmp(name, "gid") == 0) {
			col = 9;
		} else if (strcmp(name, "mode") == 0) {
			col = 10;
		} else if (strcmp(name, "new_size") == 0) {
			col = 11;
		} else if (strcmp(name, "io_offset") == 0) {
			col = 12;
		} else if (strcmp(name, "io_bytes") == 0) {
			col = 13;
		} else if (strcmp(name, "parent") == 0) {
			col = 14;
			parent = val.n;
			have_parent = B_TRUE;
		} else if (strcmp(name, "old_parent") == 0) {
			col = 15;
			/* RENAME: source directory of the old name. */
			old_parent = val.n;
			have_old_parent = B_TRUE;
		} else if (strcmp(name, "target") == 0) {
			col = 16;
		} else if (strcmp(name, "old_size") == 0) {
			col = 17;
		} else if (strcmp(name, "attrs") == 0) {
			col = 18;
		} else if (strcmp(name, "principal") == 0) {
			/*
			 * Opaque application principal tag
			 * (ZFS_EV_PRINCIPAL): supplied by the writing
			 * application, NOT verified by the kernel.
			 * Stored verbatim as a claim, never evidence.
			 * A record without the key never reaches this
			 * branch, so the column stays NULL (unregistered
			 * writer) instead of a fabricated value.
			 */
			col = 22;
		}

		if (col == 0)
			continue;

		if (dtype == DATA_TYPE_STRING)
			rc = sqlite3_bind_text(stmt, col, val.s, nelem,
			    SQLITE_TRANSIENT);
		else
			rc = sqlite3_bind_int64(stmt, col,
			    (sqlite3_int64)val.n);

		if (rc != SQLITE_OK) {
			db_warn(db, "Bind error for field %s: %s\n",
			    name, sqlite3_errmsg(db->sqlite));
			(void) sqlite3_reset(stmt);
			return (EIO);
		}
	}

	/*
	 * The event_type column stores the schema enum name; an op
	 * outside the enum decodes as UNKNOWN.
	 */
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		db_warn(db, "Bind error for dataset: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_reset(stmt);
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 5, zmetad_schema_op_name(zs,
	    have_op ? op : 0), -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		db_warn(db, "Bind error for event_type: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_reset(stmt);
		return (EIO);
	}

	/*
	 * captured_at (19) is ingest wall time (unix seconds): the
	 * wire 'time' bound to timestamp is gethrtime() nanoseconds
	 * since boot, which retention cannot compare a wall-clock
	 * cutoff against.
	 */
	rc = sqlite3_bind_int64(stmt, 19, (sqlite3_int64)time(NULL));
	if (rc != SQLITE_OK) {
		db_warn(db, "Bind error for captured_at: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_reset(stmt);
		return (EIO);
	}

	/*
	 * full_path (20) / old_full_path (21): resolve the record's
	 * place in the dataset tree at insert time by walking the
	 * objmap graph.  A name-bearing record also updates the graph
	 * (the insert and the graph write are both idempotent against
	 * the UNIQUE key / PRIMARY KEY, so a redelivered record is
	 * harmless).  Unresolvable chains (ENOENT) leave the columns
	 * NULL and the row stays PARTIAL per the conservative-match
	 * contract.  Resolution failure never fails the insert:
	 * path/parent stay as ground truth for later re-resolution, but
	 * a real query error (EIO) is warned about rather than silently
	 * recorded as an unresolvable chain.
	 */
	(void) objmap_update(db, dataset, op, have_op, object,
	    have_object, rec_name, have_name, parent, have_parent);

	/*
	 * REMOVE lifetime coupling (issue #13): the object's tags die
	 * with it.  This runs INSIDE the caller's batch transaction
	 * (zmetad.c wraps each poll's inserts in begin/commit), so the
	 * event insert and the tag delete commit atomically or both
	 * roll back.  RENAME needs no such action here: object_id is
	 * stable across renames and tags key on (dataset, object_id),
	 * not on the name.
	 */
	if (have_op && have_object &&
	    zmetad_schema_op_name(zs, op) != NULL &&
	    strcmp(zmetad_schema_op_name(zs, op), "REMOVE") == 0) {
		sqlite3_stmt *tdel = db->tag_del_obj_stmt;

		rc = sqlite3_reset(tdel);
		if (rc == SQLITE_OK)
			rc = sqlite3_bind_text(tdel, 1, dataset, -1,
			    SQLITE_STATIC);
		if (rc == SQLITE_OK)
			rc = sqlite3_bind_int64(tdel, 2,
			    (sqlite3_int64)object);
		if (rc == SQLITE_OK)
			rc = sqlite3_step(tdel);
		(void) sqlite3_reset(tdel);
		if (rc != SQLITE_DONE) {
			db_warn(db, "tag remove for %s object %llu failed: "
			    "%s\n", dataset, (u_longlong_t)object,
			    sqlite3_errmsg(db->sqlite));
			(void) sqlite3_reset(stmt);
			return (EIO);
		}
	}

	if (have_object && have_name) {
		char *fp = NULL;
		int rrc;

		rrc = objmap_resolve(db, dataset, have_parent ? parent : 0,
		    rec_name, &fp);
		if (rrc == 0 && fp != NULL) {
			sqlite3_bind_text(stmt, 20, fp, -1, SQLITE_TRANSIENT);
			free(fp);
		} else if (rrc != ENOENT) {
			db_warn(db, "full_path resolve error for %s: %s\n",
			    dataset, strerror(rrc));
		}
		if (have_old_name && rec_old_name != NULL) {
			char *ofp = NULL;

			/*
			 * Resolve the before-path against old_parent (the
			 * source directory), not parent (the destination):
			 * a cross-directory rename would otherwise record
			 * the destination chain as the before-path.
			 */
			rrc = objmap_resolve(db, dataset,
			    have_old_parent ? old_parent : parent,
			    rec_old_name, &ofp);
			if (rrc == 0 && ofp != NULL) {
				sqlite3_bind_text(stmt, 21, ofp, -1,
				    SQLITE_TRANSIENT);
				free(ofp);
			} else if (rrc != ENOENT) {
				db_warn(db, "old_full_path resolve error for "
				    "%s: %s\n", dataset, strerror(rrc));
			}
		}
	}

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE && rc != SQLITE_CONSTRAINT) {
		db_warn(db, "Insert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_reset(stmt);
		return (EIO);
	}

	return (0);
}

/*
 * Stored watermark for a dataset: 0 on success (row exists), ENOENT
 * with *offset = 0 when the dataset has no sync_state row yet
 * (first poll: not a regression), EIO on query failure so the
 * caller never mistakes an error for offset 0.
 */
int
zmetad_db_get_last_offset(zmetad_db_t *db, const char *dataset,
    uint64_t *offset)
{
	sqlite3_stmt *stmt = db->get_last_offset_stmt;
	int rc;

	*offset = 0;

	/*
	 * sqlite3_reset() reports the error code of the statement's
	 * PREVIOUS run: surface it here so a failed step from an
	 * earlier poll is not silently swallowed.
	 */
	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Get last_offset reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		db_warn(db, "Get last_offset bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		*offset = (uint64_t)sqlite3_column_int64(stmt, 0);
	}
	/*
	 * Always reset before returning: an unrestarted statement
	 * keeps its read transaction open, pinning the WAL snapshot
	 * and making later writes on this same connection fail with
	 * SQLITE_BUSY (the busy handler never fires against the
	 * connection's own active statements).
	 */
	(void) sqlite3_reset(stmt);
	if (rc == SQLITE_ROW)
		return (0);
	if (rc == SQLITE_DONE) {
		/* No row: never synced; distinct from a query error. */
		return (ENOENT);
	}

	db_warn(db, "Get last_offset error for %s: %s\n", dataset,
	    sqlite3_errmsg(db->sqlite));
	return (EIO);
}

/*
 * Stored ring identity for a dataset: 0 = unknown (no sync_state row,
 * a pre-v3 row, or a legacy reply never recorded one).
 */
uint64_t
zmetad_db_get_ring_guid(zmetad_db_t *db, const char *dataset)
{
	sqlite3_stmt *stmt = db->get_ring_guid_stmt;
	uint64_t guid = 0;
	int rc;

	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Get ring_guid reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (0);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		db_warn(db, "Get ring_guid bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (0);
	}

	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW &&
	    sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
		guid = (uint64_t)sqlite3_column_int64(stmt, 0);
	}
	/*
	 * Always reset: an unrestarted statement after SQLITE_ROW
	 * keeps a read transaction open on this connection and makes
	 * later writes fail with SQLITE_BUSY (see
	 * zmetad_db_get_last_offset).
	 */
	(void) sqlite3_reset(stmt);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
		db_warn(db, "Get ring_guid error for %s: %s\n", dataset,
		    sqlite3_errmsg(db->sqlite));
	}

	return (guid);
}

/*
 * Persist the watermark and (optionally) the ring identity in one
 * write.  ring_guid 0 stores NULL: the identity is unknown, which is
 * the correct on-disk representation for legacy replies and fresh
 * datasets alike.
 */
int
zmetad_db_set_last_offset(zmetad_db_t *db, const char *dataset,
    uint64_t offset, uint64_t ring_guid)
{
	sqlite3_stmt *stmt = db->set_last_offset_stmt;
	int rc;

	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Set last_offset reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK)
		goto bind_err;
	rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)offset);
	if (rc != SQLITE_OK)
		goto bind_err;
	rc = sqlite3_bind_int64(stmt, 3, (sqlite3_int64)time(NULL));
	if (rc != SQLITE_OK)
		goto bind_err;
	if (ring_guid != 0) {
		rc = sqlite3_bind_int64(stmt, 4, (sqlite3_int64)ring_guid);
	} else {
		rc = sqlite3_bind_null(stmt, 4);
	}
	if (rc != SQLITE_OK)
		goto bind_err;

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		db_warn(db, "Set last_offset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);

bind_err:
	db_warn(db, "Set last_offset bind error: %s\n",
	    sqlite3_errmsg(db->sqlite));
	return (EIO);
}

/*
 * Cumulative records_lost as of the previous poll.  ENOENT means "no
 * baseline" (no row yet, or a NULL column): the caller treats it as a
 * fresh ring and invents no gap for pre-existing history.  A step
 * ERROR (SQLITE_BUSY, IOERR, CORRUPT...) returns EIO so the caller
 * SKIPS loss detection this poll instead of silently re-arming and
 * swallowing loss accumulated since the last persisted baseline.
 */
int
zmetad_db_get_last_lost(zmetad_db_t *db, const char *dataset,
    boolean_t *havep, uint64_t *lostp)
{
	sqlite3_stmt *stmt = db->get_last_lost_stmt;
	int rc;

	*havep = B_FALSE;
	*lostp = 0;

	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Get last_lost reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		db_warn(db, "Get last_lost bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_reset(stmt);
		return (EIO);
	}
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW &&
	    sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
		*lostp = (uint64_t)sqlite3_column_int64(stmt, 0);
		*havep = B_TRUE;
		rc = 0;
	} else if (rc == SQLITE_DONE ||
	    (rc == SQLITE_ROW &&
	    sqlite3_column_type(stmt, 0) == SQLITE_NULL)) {
		/* No row / no baseline: distinct from a query error. */
		rc = ENOENT;
	} else {
		db_warn(db, "Get last_lost error for %s: %s\n", dataset,
		    sqlite3_errmsg(db->sqlite));
		rc = EIO;
	}
	/*
	 * Always reset before returning: an unrestarted statement
	 * keeps its read transaction open, pinning the WAL snapshot
	 * and making later writes on this same connection fail with
	 * SQLITE_BUSY (see zmetad_db_get_last_offset).
	 */
	(void) sqlite3_reset(stmt);
	return (rc);
}

int
zmetad_db_set_last_lost(zmetad_db_t *db, const char *dataset,
    boolean_t have, uint64_t lost)
{
	sqlite3_stmt *stmt = db->set_last_lost_stmt;
	int rc;

	sqlite3_reset(stmt);
	sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (have)
		rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)lost);
	else
		rc = sqlite3_bind_null(stmt, 2);
	if (rc != SQLITE_OK)
		goto out;
	rc = sqlite3_step(stmt);
out:
	sqlite3_reset(stmt);
	return (rc == SQLITE_DONE ? 0 : EIO);
}

/*
 * Per-dataset purge epoch (DB layout 7; meta key
 * 'purge_epoch:<dataset>'): bumped by every successful
 * `zmetad --purge <dataset>`; a running daemon compares ONLY its own
 * dataset's epoch per poll, re-arming that dataset's in-memory
 * watermark and loss baseline.  Per-dataset keys keep purging
 * dataset A from re-arming B and C.
 *
 * Migration continuity: the legacy GLOBAL 'purge_epoch' key is read
 * as the initial baseline for a dataset that has no per-dataset key
 * yet, so epochs bumped by a pre-layout-7 zmetad are not forgotten
 * (a daemon holding the old global value would otherwise see 0 < N
 * and re-arm once, harmlessly; reading it once here is cleaner).
 */
int
zmetad_db_get_purge_epoch(zmetad_db_t *db, const char *dataset,
    uint64_t *epochp)
{
	sqlite3_stmt *stmt;
	char key[512];
	int rc;

	*epochp = 0;

	rc = snprintf(key, sizeof (key), "purge_epoch:%s", dataset);
	if (rc < 0 || (size_t)rc >= sizeof (key))
		return (ENAMETOOLONG);

	rc = sqlite3_prepare_v2(db->sqlite, get_purge_epoch_sql, -1,
	    &stmt, NULL);
	if (rc != SQLITE_OK)
		return (EIO);
	rc = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(stmt);
	if (rc == SQLITE_DONE) {
		/* No per-dataset key yet: fall back to the legacy global. */
		(void) sqlite3_finalize(stmt);
		return (db_get_meta_u64(db, "purge_epoch", epochp));
	}
	if (rc == SQLITE_ROW) {
		*epochp = (uint64_t)sqlite3_column_int64(stmt, 0);
		rc = 0;
	} else {
		rc = EIO;
	}
	sqlite3_finalize(stmt);
	return (rc);
}

/*
 * Atomic, per-dataset epoch bump: INSERT with the initial value 1 or
 * ON CONFLICT an in-SQL increment, so two concurrent --purge runs
 * cannot lose an increment (the old get-then-set could).
 */
int
zmetad_db_bump_purge_epoch(zmetad_db_t *db, const char *dataset)
{
	sqlite3_stmt *stmt;
	char key[512];
	int rc;

	rc = snprintf(key, sizeof (key), "purge_epoch:%s", dataset);
	if (rc < 0 || (size_t)rc >= sizeof (key))
		return (ENAMETOOLONG);

	rc = sqlite3_prepare_v2(db->sqlite, bump_purge_epoch_sql, -1,
	    &stmt, NULL);
	if (rc != SQLITE_OK)
		return (EIO);
	rc = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(stmt);
	sqlite3_finalize(stmt);
	return (rc == SQLITE_DONE ? 0 : EIO);
}

/*
 * S3 object-tagging limits (the pinned contract): at most 10 tags
 * per object, keys at most 128 characters, values at most 256
 * bytes.  Enforced here, in the single tags write path.
 */
#define	ZMETAD_TAG_MAX_TAGS	10
#define	ZMETAD_TAG_MAX_KEY	128	/* characters */
#define	ZMETAD_TAG_MAX_VALUE	256	/* bytes */

/*
 * Set an object's tag set to exactly "tags" (nvlist of string
 * pairs) in ONE transaction: BEGIN IMMEDIATE, INSERT OR REPLACE
 * every pair, DELETE the keys the new set drops, COMMIT.  Rejects
 * an empty set, more than 10 tags, a key over 128 characters, a
 * value over 256 bytes, a NULL/empty key or a non-string pair with
 * EINVAL and a named reason, and writes nothing in that case (the
 * limits are checked BEFORE the transaction opens).
 *
 * captured_at is unix seconds, the events.captured_at convention.
 *
 * RENAME needs no lifetime action here: object_id is stable across
 * renames and tags key on (dataset, object_id), not on the name.
 */
int
zmetad_db_tag_set(zmetad_db_t *db, const char *dataset,
    unsigned long long object_id, const nvlist_t *tags)
{
	sqlite3_stmt *stmt;
	nvpair_t *pair = NULL;
	uint_t ntags = 0;
	int rc;

	if (db == NULL || dataset == NULL || dataset[0] == '\0' ||
	    tags == NULL)
		return (EINVAL);

	/*
	 * Validate the whole set up front: a rejection must leave the
	 * stored set untouched, so the limits cannot be enforced
	 * mid-transaction.
	 */
	while ((pair = nvlist_next_nvpair((nvlist_t *)tags, pair)) !=
	    NULL) {
		const char *val;

		if (nvpair_type(pair) != DATA_TYPE_STRING) {
			db_warn(db, "tag '%s': value is not a string\n",
			    nvpair_name(pair));
			return (EINVAL);
		}
		if (strlen(nvpair_name(pair)) == 0) {
			db_warn(db, "tag key is empty\n");
			return (EINVAL);
		}
		if (strlen(nvpair_name(pair)) > ZMETAD_TAG_MAX_KEY) {
			db_warn(db, "tag key longer than %d characters: "

			    "%s\n", ZMETAD_TAG_MAX_KEY, nvpair_name(pair));
			return (EINVAL);
		}
		rc = nvpair_value_string(pair, &val);
		if (rc != 0 || val == NULL) {
			db_warn(db, "tag '%s': cannot read value\n",
			    nvpair_name(pair));
			return (EINVAL);
		}
		if (strlen(val) > ZMETAD_TAG_MAX_VALUE) {
			db_warn(db, "tag '%s': value longer than %d "

			    "bytes\n", nvpair_name(pair),
			    ZMETAD_TAG_MAX_VALUE);
			return (EINVAL);
		}
		ntags++;
	}
	if (ntags == 0) {
		db_warn(db, "tag set is empty; use tag clear to remove "

		    "all tags\n");
		return (EINVAL);
	}
	if (ntags > ZMETAD_TAG_MAX_TAGS) {
		db_warn(db, "tag set has %u tags; at most %d are "

		    "allowed\n", ntags, ZMETAD_TAG_MAX_TAGS);
		return (EINVAL);
	}

	rc = zmetad_db_begin(db);
	if (rc != 0)
		return (rc);

	/*
	 * Replace the whole set: drop every stored row for the object
	 * first, then insert the new keys, all inside the one
	 * transaction.  Deleting per-key AFTER the inserts would be
	 * wrong twice over -- it would remove the rows just written
	 * (the delete matches the new keys) and would leave stale keys
	 * from the old set in place.
	 */
	rc = sqlite3_prepare_v2(db->sqlite, tag_del_obj_sql, -1, &stmt,
	    NULL);
	if (rc != SQLITE_OK)
		goto err;
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)object_id);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE)
		goto err;

	pair = NULL;
	while ((pair = nvlist_next_nvpair((nvlist_t *)tags, pair))
	    != NULL) {
		const char *val = NULL;

		(void) nvpair_value_string(pair, &val);
		rc = sqlite3_prepare_v2(db->sqlite, tag_put_sql, -1,
		    &stmt, NULL);
		if (rc != SQLITE_OK)
			goto err;
		rc = sqlite3_bind_text(stmt, 1, dataset, -1,
		    SQLITE_STATIC);
		if (rc == SQLITE_OK)
			rc = sqlite3_bind_int64(stmt, 2,
			    (sqlite3_int64)object_id);
		if (rc == SQLITE_OK)
			rc = sqlite3_bind_text(stmt, 3,
			    nvpair_name(pair), -1, SQLITE_STATIC);
		if (rc == SQLITE_OK)
			rc = sqlite3_bind_text(stmt, 4, val, -1,
			    SQLITE_TRANSIENT);
		if (rc == SQLITE_OK)
			rc = sqlite3_bind_int64(stmt, 5,
			    (sqlite3_int64)time(NULL));
		if (rc == SQLITE_OK)
			rc = sqlite3_step(stmt);
		(void) sqlite3_finalize(stmt);
		if (rc != SQLITE_DONE)
			goto err;
	}

	rc = zmetad_db_commit(db);
	if (rc != 0) {
		(void) zmetad_db_rollback(db);
		return (rc);
	}
	return (0);

err:
	db_warn(db, "tag set for %s object %llu: %s\n", dataset,
	    object_id, sqlite3_errmsg(db->sqlite));
	(void) zmetad_db_rollback(db);
	return (EIO);
}

/*
 * All tag pairs for (dataset, object_id) as a string:string nvlist.
 * Returns 0 with an EMPTY nvlist when the object has no tags (not
 * an error); *out is NULL only on error.  Caller frees with
 * fnvlist_free().
 */
int
zmetad_db_tag_get(zmetad_db_t *db, const char *dataset,
    unsigned long long object_id, nvlist_t **out)
{
	sqlite3_stmt *stmt = NULL;
	nvlist_t *result;
	int rc;

	*out = NULL;

	if (db == NULL || dataset == NULL || dataset[0] == '\0')
		return (EINVAL);

	rc = sqlite3_prepare_v2(db->sqlite, tag_get_sql, -1, &stmt,
	    NULL);
	if (rc != SQLITE_OK)
		return (EIO);

	result = fnvlist_alloc();
	if (result == NULL) {
		(void) sqlite3_finalize(stmt);
		return (ENOMEM);
	}

	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_int64(stmt, 2,
		    (sqlite3_int64)object_id);
	if (rc == SQLITE_OK) {
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			const unsigned char *key =
			    sqlite3_column_text(stmt, 0);
			const unsigned char *val =
			    sqlite3_column_text(stmt, 1);

			if (key == NULL || val == NULL)
				continue;
			/*
			 * Keys are validated at tag-set time (<= 128
			 * chars, unique), so fnvlist_add_string cannot
			 * collide here.
			 */
			fnvlist_add_string(result,
			    (const char *)key, (const char *)val);
		}
	}
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE) {
		fnvlist_free(result);
		return (EIO);
	}

	*out = result;
	return (0);
}

/*
 * Delete every tag row for (dataset, object_id).  Idempotent like
 * the S3 clear-all: 0 even when none existed.
 */
int
zmetad_db_tag_clear(zmetad_db_t *db, const char *dataset,
    unsigned long long object_id)
{
	sqlite3_stmt *stmt = db->tag_del_obj_stmt;
	int rc;

	if (db == NULL || dataset == NULL || dataset[0] == '\0')
		return (EINVAL);

	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Tag clear reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_int64(stmt, 2,
		    (sqlite3_int64)object_id);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(stmt);
	(void) sqlite3_reset(stmt);
	if (rc != SQLITE_DONE) {
		db_warn(db, "Tag clear error for %s: %s\n", dataset,
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	return (0);
}

/*
 * Delete every row belonging to "dataset" from the events, gaps,
 * sync_state, objmap and tags tables in ONE transaction: a midway
 * failure rolls all five deletes back instead of leaving events
 * gone while gaps/sync_state/objmap/tags survive.  Row counts are
 * returned through the caller's array (events, gaps, sync_state,
 * objmap order; the tags delete is not one of the reported counts).
 */
int
zmetad_db_purge_dataset(zmetad_db_t *db, const char *dataset,
    long long counts[4])
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	for (int i = 0; i < 4; i++)
		counts[i] = 0;

	rc = zmetad_db_begin(db);
	if (rc != 0)
		return (rc);

	for (int i = 0; i < (int)(sizeof (purge_dataset_sql) /
	    sizeof (purge_dataset_sql[0])); i++) {
		rc = sqlite3_prepare_v2(db->sqlite, purge_dataset_sql[i],
		    -1, &stmt, NULL);
		if (rc != SQLITE_OK) {
			db_warn(db, "Prepare purge error: %s\n",
			    sqlite3_errmsg(db->sqlite));
			(void) zmetad_db_rollback(db);
			return (EIO);
		}
		rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
		if (rc != SQLITE_OK) {
			db_warn(db, "Purge bind error: %s\n",
			    sqlite3_errmsg(db->sqlite));
			(void) sqlite3_finalize(stmt);
			(void) zmetad_db_rollback(db);
			return (EIO);
		}
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
			db_warn(db, "Purge error: %s\n",
			    sqlite3_errmsg(db->sqlite));
			(void) sqlite3_finalize(stmt);
			(void) zmetad_db_rollback(db);
			return (EIO);
		}
		if (i < 4)
			counts[i] = sqlite3_changes(db->sqlite);
		(void) sqlite3_finalize(stmt);
		stmt = NULL;
	}
	rc = zmetad_db_commit(db);
	if (rc != 0) {
		(void) zmetad_db_rollback(db);
		for (int i = 0; i < 4; i++)
			counts[i] = 0;
		return (rc);
	}

	return (0);
}

/*
 * Record an event-log gap: a loss observed at poll time, either a
 * records_lost delta (ring wrap or queue overflow) or a watermark
 * regression (lost sync_state or a cleared/recreated ring).  "lost"
 * is the count of records lost since the previous poll; from_offset
 * may be unknown (pass 0) when no range applies.
 */
int
zmetad_db_insert_gap(zmetad_db_t *db, const char *dataset,
    uint64_t from_offset, uint64_t to_offset, uint64_t lost)
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	rc = sqlite3_prepare_v2(db->sqlite, insert_gap_sql, -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare gap insert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK)
		goto bind_err;
	rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)time(NULL));
	if (rc != SQLITE_OK)
		goto bind_err;
	if (from_offset != 0) {
		rc = sqlite3_bind_int64(stmt, 3,
		    (sqlite3_int64)from_offset);
		if (rc != SQLITE_OK)
			goto bind_err;
	}
	if (to_offset != 0) {
		rc = sqlite3_bind_int64(stmt, 4, (sqlite3_int64)to_offset);
		if (rc != SQLITE_OK)
			goto bind_err;
	}
	rc = sqlite3_bind_int64(stmt, 5, (sqlite3_int64)lost);
	if (rc != SQLITE_OK)
		goto bind_err;

	rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE) {
		db_warn(db, "Gap insert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);

bind_err:
	db_warn(db, "Gap insert bind error: %s\n",
	    sqlite3_errmsg(db->sqlite));
	(void) sqlite3_finalize(stmt);
	return (EIO);
}

/*
 * Record (or refresh) a dataset's mountpoint and stamp last_seen
 * with the current wall-clock second.  INSERT OR REPLACE keeps one
 * row per dataset; called every collect so mountpoint changes
 * self-heal and prune_stale_datasets can tell which datasets the
 * current cycle actually saw.  Table is tiny: prepare per call like
 * zmetad_db_insert_gap().
 */
int
zmetad_db_upsert_mountpoint(zmetad_db_t *db, const char *dataset,
    const char *mountpoint)
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	rc = sqlite3_prepare_v2(db->sqlite, upsert_mountpoint_sql, -1,
	    &stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare mountpoint upsert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK)
		goto bind_err;
	rc = sqlite3_bind_text(stmt, 2, mountpoint, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK)
		goto bind_err;
	rc = sqlite3_bind_int64(stmt, 3, (sqlite3_int64)time(NULL));
	if (rc != SQLITE_OK)
		goto bind_err;

	rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE) {
		db_warn(db, "Mountpoint upsert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);

bind_err:
	db_warn(db, "Mountpoint upsert bind error: %s\n",
	    sqlite3_errmsg(db->sqlite));
	(void) sqlite3_finalize(stmt);
	return (EIO);
}

/*
 * One statement per poll cycle: drop datasets rows whose last_seen
 * predates this cycle's start (dataset destroyed, or events
 * disabled), so a stale mountpoint can never win a longest-prefix
 * match.  Parameterized; NULL last_seen (pre-prune rows) also goes.
 */
int
zmetad_db_prune_stale_datasets(zmetad_db_t *db, int64_t cycle_start)
{
	sqlite3_stmt *stmt = db->prune_datasets_stmt;
	int rc;

	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prune datasets reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_int64(stmt, 1, (sqlite3_int64)cycle_start);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prune datasets bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		db_warn(db, "Prune datasets error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);
}

int
zmetad_db_gap_stats(zmetad_db_t *db, const char *dataset, long long counts[3])
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	counts[0] = counts[1] = counts[2] = 0;

	rc = sqlite3_prepare_v2(db->sqlite,
	    "SELECT"
	    " SUM(CASE WHEN lost < 0 THEN 1 ELSE 0 END),"
	    " SUM(CASE WHEN lost = 0 THEN 1 ELSE 0 END),"
	    " SUM(CASE WHEN lost > 0 THEN 1 ELSE 0 END)"
	    " FROM gaps WHERE dataset = ?", -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare gap stats error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		db_warn(db, "Gap stats bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_finalize(stmt);
		return (EIO);
	}

	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		/* SUM() over an empty set yields NULL; map to 0. */
		counts[0] = (sqlite3_column_type(stmt, 0) == SQLITE_NULL) ?
		    0 : sqlite3_column_int64(stmt, 0);
		counts[1] = (sqlite3_column_type(stmt, 1) == SQLITE_NULL) ?
		    0 : sqlite3_column_int64(stmt, 1);
		counts[2] = (sqlite3_column_type(stmt, 2) == SQLITE_NULL) ?
		    0 : sqlite3_column_int64(stmt, 2);
	}
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_ROW) {
		db_warn(db, "Gap stats error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);
}

/*
 * Retention applies to events only.  gaps rows are the permanent
 * completeness record (see SCHEMA.md): consumers compute lifetime
 * loss as SUM(lost) WHERE lost > 0 and ring swaps as COUNT(*)
 * WHERE lost = -1.  Removing gaps rows happens exclusively via
 * zmetad --purge, which deletes by dataset.
 *
 * The cutoff is computed in 64-bit with a defensive range check
 * (option parsing already rejects days <= 0 and absurd values) and
 * BOUND as a parameter, never interpolated into SQL text.
 */
int
zmetad_db_cleanup(zmetad_db_t *db, int retention_days)
{
	sqlite3_stmt *stmt = NULL;
	long long span;
	long long cutoff;
	int rc;

	if (retention_days <= 0 ||
	    retention_days > ZMETAD_MAX_RETENTION_DAYS) {
		db_warn(db, "refusing cleanup: retention_days %d out "
		    "of range (1..%d)\n", retention_days,
		    ZMETAD_MAX_RETENTION_DAYS);
		return (EINVAL);
	}
	span = (long long)retention_days * 86400LL;
	if (span / 86400LL != retention_days) {
		/* Unreachable given the bound above; belt and braces. */
		db_warn(db, "refusing cleanup: retention span "
		    "overflow\n");
		return (EINVAL);
	}
	cutoff = (long long)time(NULL) - span;

	rc = sqlite3_prepare_v2(db->sqlite, cleanup_sql, -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare cleanup error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_int64(stmt, 1, (sqlite3_int64)cutoff);
	if (rc != SQLITE_OK) {
		db_warn(db, "Cleanup bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_finalize(stmt);
		return (EIO);
	}

	rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE) {
		db_warn(db, "Cleanup error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	/* Vacuum to reclaim space; failure is not fatal. */
	{
		char *errmsg = NULL;

		rc = sqlite3_exec(db->sqlite, "VACUUM", NULL, NULL, &errmsg);
		if (rc != SQLITE_OK) {
			db_warn(db, "VACUUM error: %s\n",
			    errmsg != NULL ? errmsg : "unknown");
			sqlite3_free(errmsg);
		}
	}

	return (0);
}

/*
 * Append "s" to fp as a quoted, JSON-escaped string: the same escape
 * set the spool uses (\", \\, \n, \r, \t, \b, \f, \uXXXX for the
 * remaining control bytes); non-ASCII UTF-8 passes through verbatim,
 * which any JSON parser accepts.  Returns 0, or EIO on a write error.
 */
static int
query_json_string(FILE *fp, const char *s)
{
	if (fputc('"', fp) == EOF)
		return (EIO);
	for (size_t i = 0; s[i] != '\0'; i++) {
		unsigned char c = (unsigned char)s[i];
		const char *esc = NULL;

		switch (c) {
		case '"':
			esc = "\\\"";
			break;
		case '\\':
			esc = "\\\\";
			break;
		case '\n':
			esc = "\\n";
			break;
		case '\r':
			esc = "\\r";
			break;
		case '\t':
			esc = "\t";
			break;
		case '\b':
			esc = "\\b";
			break;
		case '\f':
			esc = "\\f";
			break;
		default:
			if (c < 0x20) {
				if (fprintf(fp, "\\u%04x",
				    (unsigned)c) < 0)
					return (EIO);
				continue;
			}
			if (fputc(c, fp) == EOF)
				return (EIO);
			continue;
		}
		if (fputs(esc, fp) == EOF)
			return (EIO);
	}
	if (fputc('"', fp) == EOF)
		return (EIO);
	return (0);
}

/*
 * Render one events row as a compact NDJSON object from the row's
 * own columns.  Field names are the SCHEMA.md Section 2.1 column
 * names plus "id"; NULL column -> JSON null, TEXT column -> JSON
 * string, INTEGER column -> JSON number.  The spool envelope is NOT
 * reused: its wire names differ from the column names (Section 10
 * vs Section 2.1).
 */
static int
query_emit_row(FILE *fp, sqlite3_stmt *stmt, unsigned long long *idp)
{
	static const char *query_fields[] = {
		"id", "dataset", "txg", "timestamp", "captured_at",
		"object_id", "event_type", "path", "old_path",
		"uid", "gid", "mode", "size", "io_offset", "io_bytes",
		"parent", "old_parent", "target", "old_size", "attrs",
		"full_path", "old_full_path", "principal",
	};
	const int nfields = (int)(sizeof (query_fields) /
	    sizeof (query_fields[0]));
	int rc = 0;

	/*
	 * Each row is one JSON object: the leading and trailing
	 * braces make the NDJSON line self-contained and parseable.
	 */
	if (fputc('{', fp) == EOF)
		return (EIO);

	for (int i = 0; i < nfields && rc == 0; i++) {
		if (i > 0 && fputc(',', fp) == EOF) {
			rc = EIO;
			break;
		}
		if (fprintf(fp, "\"%s\":", query_fields[i]) < 0) {
			rc = EIO;
			break;
		}
		if (sqlite3_column_type(stmt, i) == SQLITE_NULL) {
			rc = (fputs("null", fp) == EOF) ? EIO : 0;
			continue;
		}
		if (sqlite3_column_type(stmt, i) == SQLITE_TEXT) {
			const unsigned char *val =
			    sqlite3_column_text(stmt, i);

			if (val == NULL) {
				rc = (fputs("null", fp) == EOF) ?
				    EIO : 0;
				continue;
			}
			rc = query_json_string(fp, (const char *)val);
			continue;
		}
		rc = (fprintf(fp, "%lld",
		    (long long)sqlite3_column_int64(stmt, i)) < 0) ?
		    EIO : 0;
	}
	if (rc == 0 && fputc('}', fp) == EOF)
		rc = EIO;
	if (rc == 0 && fputc('\n', fp) == EOF)
		rc = EIO;
	if (rc == 0)
		*idp = (unsigned long long)
		    sqlite3_column_int64(stmt, 0);
	return (rc);
}

/*
 * Incremental consumer query (SCHEMA.md Section 8.5): stream the
 * dataset's event rows with id > since_id to fp in ascending id
 * order, at most max_events rows.  LIMIT max_events + 1 detects
 * truncation: the extra probe row proves more matching rows exist
 * without buffering the result.  loss_seen is ANY gaps row for the
 * dataset -- gaps offsets are kernel ring offsets, not ids (a
 * different keyspace), so no per-range intersection warning is
 * possible; the full-rescan decision is the consumer's.  Read-only:
 * only SELECTs touch the database.
 */
int
zmetad_db_query_events(zmetad_db_t *db, const char *dataset,
    unsigned long long since_id, unsigned long long max_events,
    FILE *fp, unsigned long long *returnedp,
    unsigned long long *last_idp, boolean_t *truncatedp,
    boolean_t *loss_seenp)
{
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *loss_stmt = NULL;
	unsigned long long returned = 0;
	unsigned long long last_id = 0;
	boolean_t truncated = B_FALSE;
	boolean_t loss_seen = B_FALSE;
	int rc;

	if (dataset == NULL || dataset[0] == '\0' || max_events == 0 ||
	    max_events > ZMETAD_MAX_QUERY_EVENTS)
		return (EINVAL);

	*returnedp = 0;
	*last_idp = 0;
	*truncatedp = B_FALSE;
	*loss_seenp = B_FALSE;

	rc = sqlite3_prepare_v2(db->sqlite, query_events_sql, -1,
	    &stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare query error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_int64(stmt, 2,
		    (sqlite3_int64)since_id);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_int64(stmt, 3,
		    (sqlite3_int64)(max_events + 1));
	if (rc != SQLITE_OK) {
		db_warn(db, "Query bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_finalize(stmt);
		return (EIO);
	}

	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (returned == max_events) {
			/* The +1 probe row: matching rows continue. */
			truncated = B_TRUE;
			break;
		}
		rc = query_emit_row(fp, stmt, &last_id);
		if (rc != 0) {
			db_warn(db, "Query output error: %s\n",
			    strerror(rc));
			(void) sqlite3_finalize(stmt);
			return (rc);
		}
		returned++;
	}
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
		db_warn(db, "Query error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	if (fflush(fp) != 0 || ferror(fp))
		return (EIO);

	rc = sqlite3_prepare_v2(db->sqlite, query_loss_sql, -1,
	    &loss_stmt, NULL);
	if (rc != SQLITE_OK) {
		db_warn(db, "Prepare loss probe error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_text(loss_stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_step(loss_stmt);
	if (rc == SQLITE_ROW)
		loss_seen = B_TRUE;
	(void) sqlite3_finalize(loss_stmt);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
		db_warn(db, "Loss probe error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	*returnedp = returned;
	*last_idp = last_id;
	*truncatedp = truncated;
	*loss_seenp = loss_seen;
	return (0);
}
