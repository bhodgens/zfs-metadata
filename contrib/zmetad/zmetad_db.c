// SPDX-License-Identifier: CDDL-1.0
/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or https://opensource.org/licenses/CDDL-1.0.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * CDDL HEADER END
 */

/*
 * zmetad database operations - SQLite backend
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <time.h>

#include <sqlite3.h>
#include <libnvpair.h>

#include "zmetad.h"
#include "zmetad_schema.h"

#define	ZMETAD_DB_SCHEMA_VERSION	5

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
	sqlite3_stmt	*objmap_root_stmt;
	sqlite3_stmt	*prune_datasets_stmt;
	const zmetad_schema_t *schema;
};

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
	"    ring_guid INTEGER"
		");"
	"CREATE TABLE IF NOT EXISTS meta ("
	"    key TEXT PRIMARY KEY,"
	"    value TEXT NOT NULL"
	");";

static const char *insert_event_sql =
	"INSERT OR IGNORE INTO events "
	"(dataset, txg, timestamp, object_id, event_type, path, old_path, "
	"uid, gid, mode, size, io_offset, io_bytes, parent, old_parent, "
	"target, old_size, attrs, captured_at, full_path, old_full_path) "
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
	"?, ?)";

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

static const char *objmap_root_sql =
	"INSERT OR IGNORE INTO objmap "
	"(dataset, object_id, name, parent) VALUES (?, ?, '', 0)";

static const char *get_last_offset_sql =
	"SELECT last_offset FROM sync_state WHERE dataset = ?";

static const char *get_ring_guid_sql =
	"SELECT ring_guid FROM sync_state WHERE dataset = ?";

/*
 * Single watermark write: last_offset + ring_guid together, so a
 * ring swap persists the reset offset and the new identity in one
 * statement.  ring_guid binding of NULL stores an unknown identity.
 */
static const char *set_last_offset_sql =
	"INSERT OR REPLACE INTO sync_state "
	"(dataset, last_offset, last_sync, ring_guid) VALUES (?, ?, ?, ?)";

static const char *purge_dataset_sql[] = {
	"DELETE FROM events WHERE dataset = ?",
	"DELETE FROM gaps WHERE dataset = ?",
	"DELETE FROM sync_state WHERE dataset = ?",
	"DELETE FROM objmap WHERE dataset = ?",
};

static const char *get_meta_sql =
	"SELECT value FROM meta WHERE key = ?";

static const char *set_meta_sql =
	"INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)";

static const char *cleanup_sql =
	"DELETE FROM events WHERE captured_at IS NOT NULL AND "
	"captured_at < ?";

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
		fprintf(stderr, "BEGIN error: %s\n",
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
		fprintf(stderr, "COMMIT error: %s\n",
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
		fprintf(stderr, "ROLLBACK error: %s\n",
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
	fprintf(stderr, "migration error adding %s.%s: %s\n", table, name,
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
 */
static int
db_migrate_stage(zmetad_db_t *db, unsigned long stage_version,
    const char *table, const db_column_t *cols, size_t ncols)
{
	char version_str[16];
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
	}

	(void) snprintf(version_str, sizeof (version_str), "%lu",
	    stage_version);
	rc = db_set_meta(db, "db_schema_version", version_str);
	if (rc != 0) {
		fprintf(stderr, "Failed to record database "
		    "layout version %lu\n", stage_version);
		(void) zmetad_db_rollback(db);
		return (rc);
	}

	rc = zmetad_db_commit(db);
	if (rc != 0) {
		(void) zmetad_db_rollback(db);
		return (rc);
	}

	fprintf(stderr, "upgraded database to layout version %lu\n",
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
 * Ensure the database layout version matches this build.  Version 1
 * (pre-gap-tracking), version 2 (pre-ring-identity) and version 3
 * (pre-captured_at) databases are upgraded in place with ALTER TABLE
 * ADD COLUMN; the added columns are NULL for old rows, which is the
 * correct representation for fields absent from those records.
 * Each stage runs in one transaction with its own version stamp, so
 * schema and version can never diverge after a crash.  A database
 * written by a NEWER layout is refused.
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
		 * No key: either a fresh database (created empty by
		 * schema_sql above, which already contains every v2+
		 * column) or a version 1 database.  Probe the v2
		 * column set on the events table: when all five are
		 * present the table is current and only the version
		 * stamp is missing; otherwise run the v1->v2 stage
		 * (duplicate-tolerant, so a partially applied prior
		 * attempt recovers).  Either way v ends up 1 and
		 * falls through the stage blocks below.
		 */
		v = 1;
		if (!db_columns_present(db, "events", db_v2_columns,
		    NDBCOLS(db_v2_columns))) {
			rc = db_migrate_stage(db, 2, "events",
			    db_v2_columns, NDBCOLS(db_v2_columns));
			if (rc != 0)
				return (rc);
		}
		/*
		 * v stays 1 (a missing key means at most a v1 layout;
		 * when the columns were already present the table is
		 * newer or fresh and the duplicate-tolerant stages
		 * below are no-ops).  Fall through: v < 3 and v < 4
		 * run the later stages, each stamping its own version
		 * in the same transaction as its ALTERs.
		 */
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
	 * Version 1/2 -> 3: sync_state gains the ring_guid column.
	 * The stage records version 3 (its own), not the final
	 * version, and runs ALTER+stamp in one transaction.
	 */
	if (v < 3) {
		rc = db_migrate_stage(db, 3, "sync_state",
		    db_v3_columns, NDBCOLS(db_v3_columns));
		if (rc != 0)
			return (rc);
		v = 3;
	}

	/*
	 * Version 3 -> 4: events gains captured_at (unix seconds at
	 * ingest).  events.timestamp holds the kernel wire 'time'
	 * value, which is gethrtime() nanoseconds since boot - not a
	 * wall clock - so retention could never compare it against a
	 * time(NULL) cutoff.  Pre-v4 rows get NULL: retention leaves
	 * them (they are either recent or the operator purges).
	 */
	if (v < 4) {
		static const db_column_t v4_columns[] = {
			{ "captured_at", "INTEGER" },
		};

		rc = db_migrate_stage(db, 4, "events",
		    v4_columns, NDBCOLS(v4_columns));
		if (rc != 0)
			return (rc);
	}

	/*
	 * Version 4 -> 5: events gains full_path/old_full_path (the
	 * dataset-relative path resolved at insert time; NULL when the
	 * ancestor chain is unresolvable), and the objmap table carries
	 * the objid -> (name, parent) graph the resolver walks.  Pre-v5
	 * rows keep NULL full_path and remain PARTIAL under the
	 * documented conservative-match rules.
	 */
	if (v < 5) {
		static const db_column_t v5_columns[] = {
			{ "full_path",		"TEXT" },
			{ "old_full_path",	"TEXT" },
		};

		rc = db_migrate_stage(db, 5, "events",
		    v5_columns, NDBCOLS(v5_columns));
		if (rc != 0)
			return (rc);
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

	/*
	 * datasets.last_seen for databases created before the
	 * stale-row prune existed.  Duplicate-tolerant; no layout
	 * version bump (see comment at datasets_v2_sql).
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
	 * Record the event schema version this database was written with.
	 * Refuse to open when a previous run used a different version:
	 * silently mixing record layouts would corrupt the event table.
	 */
	db->schema = zs;
	if (zs != NULL) {
		(void) snprintf(version_str, sizeof (version_str), "%llu",
		    (unsigned long long)zmetad_schema_version(zs));

		rc = db_get_meta(db, "events_schema_version",
		    &stored_version);
		if (rc == 0 && strcmp(stored_version, version_str) != 0) {
			fprintf(stderr, "database schema version mismatch: "
			    "stored=%s loaded=%s\n",
			    stored_version, version_str);
			free(stored_version);
			sqlite3_close(db->sqlite);
			free(db);
			return (EINVAL);
		}
		free(stored_version);

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
		rc = sqlite3_prepare_v2(db->sqlite, objmap_root_sql, -1,
		    &db->objmap_root_stmt, NULL);
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
	if (db->objmap_root_stmt)
		(void) sqlite3_finalize(db->objmap_root_stmt);
	if (db->prune_datasets_stmt)
		(void) sqlite3_finalize(db->prune_datasets_stmt);
	if (db->sqlite)
		sqlite3_close(db->sqlite);

	free(db);
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

static int
objmap_resolve(zmetad_db_t *db, const char *dataset, uint64_t parent,
    const char *name, char **pathp)
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
	if (parent == 0 || parent == UINT64_MAX) {
		/* Already at the dataset root: only the name. */
		goto build;
	}

	for (depth = 0; parent != 0 && parent != UINT64_MAX; depth++) {
		uint64_t next_parent;
		const unsigned char *nm;

		if (depth >= ZMETAD_PATH_MAX_DEPTH - 1)
			return (ENOENT);	/* cycle / corrupt graph */

		sqlite3_reset(stmt);
		sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
		sqlite3_bind_int64(stmt, 2, (sqlite3_int64)parent);
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_ROW) {
			sqlite3_reset(stmt);
			return (ENOENT);
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
	int rc;

	if (!have_op || !have_object || !have_name)
		return (0);

	switch (op) {
	case 1:	/* CREATE */
	case 3:	/* RENAME */
	case 4:	/* LINK */
	case 5:	/* SYMLINK */
		/*
		 * First name-bearing record of a dataset with an empty
		 * graph: its parent IS the dataset root directory (an
		 * empty dataset's first object is created in the root;
		 * a cleared ring keeps objmap, so this cannot misfire
		 * on a wrapped ring).  Seed it as the sentinel root so
		 * chains terminate cleanly instead of every row in a
		 * fresh dataset resolving as PARTIAL.
		 */
		sqlite3_reset(db->objmap_any_stmt);
		sqlite3_bind_text(db->objmap_any_stmt, 1, dataset, -1,
		    SQLITE_STATIC);
		rc = sqlite3_step(db->objmap_any_stmt);
		sqlite3_reset(db->objmap_any_stmt);
		if (rc == SQLITE_DONE && have_parent && parent != 0) {
			sqlite3_reset(db->objmap_root_stmt);
			sqlite3_bind_text(db->objmap_root_stmt, 1, dataset,
			    -1, SQLITE_STATIC);
			sqlite3_bind_int64(db->objmap_root_stmt, 2,
			    (sqlite3_int64)parent);
			(void) sqlite3_step(db->objmap_root_stmt);
			sqlite3_reset(db->objmap_root_stmt);
		}

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
	uint64_t u64val = 0;
	const char *strval = NULL;
	void *outp;
	uint64_t op = 0;
	boolean_t have_op = B_FALSE;
	uint64_t object = 0;
	boolean_t have_object = B_FALSE;
	uint64_t parent = 0;
	boolean_t have_parent = B_FALSE;
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
	 * Numeric and string values are stored in separate locals and
	 * the address passed to zmetad_schema_field() is chosen by
	 * the field's known type: no type-punning through a union
	 * (strict-aliasing UB on non-LP64 layouts).
	 */
	rc = sqlite3_reset(stmt);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Insert reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_clear_bindings(stmt);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Clear bindings error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	for (uint_t i = 0; i < zmetad_schema_nfields(zs); i++) {
		const char *name = zmetad_schema_field_name(zs, i);

		u64val = 0;
		strval = NULL;
		nelem = 0;
		dtype = DATA_TYPE_UNKNOWN;

		/*
		 * No union type-punning: string fields (the schema's
		 * ZST_STRING set: name, old_name, target) decode into
		 * a const char * lvalue, numerics into a uint64_t,
		 * and zmetad_schema_field() gets the address of the
		 * correctly typed storage.
		 */
		boolean_t str_field =
		    (strcmp(name, "name") == 0 ||
		    strcmp(name, "old_name") == 0 ||
		    strcmp(name, "target") == 0);
		outp = str_field ? (void *)&strval : (void *)&u64val;

		rc = zmetad_schema_field(zs, name, event, outp, &nelem,
		    &dtype);
		if (rc == ENOENT)
			continue;
		if (rc != 0) {
			fprintf(stderr, "Field %s: %s\n", name,
			    strerror(rc));
			return (rc);
		}
		/* Guard: the record's type must match what we bound. */
		if (str_field != (dtype == DATA_TYPE_STRING)) {
			fprintf(stderr, "Field %s: unexpected wire type\n",
			    name);
			return (EINVAL);
		}

		if (strcmp(name, "txg") == 0) {
			rc = sqlite3_bind_int64(stmt, 2,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "time") == 0) {
			rc = sqlite3_bind_int64(stmt, 3,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "object") == 0) {
			rc = sqlite3_bind_int64(stmt, 4,
			    (sqlite3_int64)u64val);
			object = u64val;
			have_object = B_TRUE;
		} else if (strcmp(name, "op") == 0) {
			op = u64val;
			have_op = B_TRUE;
			rc = SQLITE_OK;
		} else if (strcmp(name, "name") == 0) {
			rc = sqlite3_bind_text(stmt, 6, strval, nelem,
			    SQLITE_TRANSIENT);
			rec_name = strval;
			have_name = B_TRUE;
		} else if (strcmp(name, "old_name") == 0) {
			rc = sqlite3_bind_text(stmt, 7, strval, nelem,
			    SQLITE_TRANSIENT);
			rec_old_name = strval;
			have_old_name = B_TRUE;
		} else if (strcmp(name, "uid") == 0) {
			rc = sqlite3_bind_int64(stmt, 8,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "gid") == 0) {
			rc = sqlite3_bind_int64(stmt, 9,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "mode") == 0) {
			rc = sqlite3_bind_int64(stmt, 10,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "new_size") == 0) {
			rc = sqlite3_bind_int64(stmt, 11,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "io_offset") == 0) {
			rc = sqlite3_bind_int64(stmt, 12,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "io_bytes") == 0) {
			rc = sqlite3_bind_int64(stmt, 13,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "parent") == 0) {
			rc = sqlite3_bind_int64(stmt, 14,
			    (sqlite3_int64)u64val);
			parent = u64val;
			have_parent = B_TRUE;
		} else if (strcmp(name, "old_parent") == 0) {
			rc = sqlite3_bind_int64(stmt, 15,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "target") == 0) {
			rc = sqlite3_bind_text(stmt, 16, strval, nelem,
			    SQLITE_TRANSIENT);
		} else if (strcmp(name, "old_size") == 0) {
			rc = sqlite3_bind_int64(stmt, 17,
			    (sqlite3_int64)u64val);
		} else if (strcmp(name, "attrs") == 0) {
			rc = sqlite3_bind_int64(stmt, 18,
			    (sqlite3_int64)u64val);
		} else {
			rc = SQLITE_OK;
		}
		if (rc != SQLITE_OK) {
			fprintf(stderr, "Bind error for field %s: %s\n",
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
		fprintf(stderr, "Bind error for dataset: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_reset(stmt);
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 5, zmetad_schema_op_name(zs,
	    have_op ? op : 0), -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Bind error for event_type: %s\n",
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
		fprintf(stderr, "Bind error for captured_at: %s\n",
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
	 * harmless).  Unresolvable chains leave the columns NULL and
	 * the row stays PARTIAL per the conservative-match contract.
	 * Resolution failure never fails the insert: path/parent stay
	 * as ground truth for later re-resolution.
	 */
	(void) objmap_update(db, dataset, op, have_op, object,
	    have_object, rec_name, have_name, parent, have_parent);

	if (have_object && have_name) {
		char *fp = NULL;

		if (objmap_resolve(db, dataset, have_parent ? parent : 0,
		    rec_name, &fp) == 0 && fp != NULL) {
			sqlite3_bind_text(stmt, 20, fp, -1, SQLITE_TRANSIENT);
			free(fp);
		}
		if (have_old_name && rec_old_name != NULL) {
			char *ofp = NULL;

			if (objmap_resolve(db, dataset,
			    parent, rec_old_name, &ofp) == 0 && ofp != NULL) {
				sqlite3_bind_text(stmt, 21, ofp, -1,
				    SQLITE_TRANSIENT);
				free(ofp);
			}
		}
	}

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE && rc != SQLITE_CONSTRAINT) {
		fprintf(stderr, "Insert error: %s\n",
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
		fprintf(stderr, "Get last_offset reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Get last_offset bind error: %s\n",
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

	fprintf(stderr, "Get last_offset error for %s: %s\n", dataset,
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
		fprintf(stderr, "Get ring_guid reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (0);
	}
	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Get ring_guid bind error: %s\n",
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
		fprintf(stderr, "Get ring_guid error for %s: %s\n", dataset,
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
		fprintf(stderr, "Set last_offset reset error: %s\n",
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
		fprintf(stderr, "Set last_offset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);

bind_err:
	fprintf(stderr, "Set last_offset bind error: %s\n",
	    sqlite3_errmsg(db->sqlite));
	return (EIO);
}

/*
 * Delete every row belonging to "dataset" from the events, gaps and
 * sync_state tables in ONE transaction: a midway failure rolls all
 * three deletes back instead of leaving events gone while
 * gaps/sync_state survive.  Row counts are returned through the
 * caller's array (events, gaps, sync_state order).
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

	for (int i = 0; i < 4; i++) {
		rc = sqlite3_prepare_v2(db->sqlite, purge_dataset_sql[i],
		    -1, &stmt, NULL);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "Prepare purge error: %s\n",
			    sqlite3_errmsg(db->sqlite));
			(void) zmetad_db_rollback(db);
			return (EIO);
		}
		rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "Purge bind error: %s\n",
			    sqlite3_errmsg(db->sqlite));
			(void) sqlite3_finalize(stmt);
			(void) zmetad_db_rollback(db);
			return (EIO);
		}
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "Purge error: %s\n",
			    sqlite3_errmsg(db->sqlite));
			(void) sqlite3_finalize(stmt);
			(void) zmetad_db_rollback(db);
			return (EIO);
		}
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
		fprintf(stderr, "Prepare gap insert error: %s\n",
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
		fprintf(stderr, "Gap insert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);

bind_err:
	fprintf(stderr, "Gap insert bind error: %s\n",
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
		fprintf(stderr, "Prepare mountpoint upsert error: %s\n",
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
		fprintf(stderr, "Mountpoint upsert error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);

bind_err:
	fprintf(stderr, "Mountpoint upsert bind error: %s\n",
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
		fprintf(stderr, "Prune datasets reset error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_bind_int64(stmt, 1, (sqlite3_int64)cycle_start);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Prune datasets bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Prune datasets error: %s\n",
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
		fprintf(stderr, "Prepare gap stats error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_text(stmt, 1, dataset, -1, SQLITE_STATIC);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Gap stats bind error: %s\n",
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
		fprintf(stderr, "Gap stats error: %s\n",
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
		fprintf(stderr, "refusing cleanup: retention_days %d out "
		    "of range (1..%d)\n", retention_days,
		    ZMETAD_MAX_RETENTION_DAYS);
		return (EINVAL);
	}
	span = (long long)retention_days * 86400LL;
	if (span / 86400LL != retention_days) {
		/* Unreachable given the bound above; belt and braces. */
		fprintf(stderr, "refusing cleanup: retention span "
		    "overflow\n");
		return (EINVAL);
	}
	cutoff = (long long)time(NULL) - span;

	rc = sqlite3_prepare_v2(db->sqlite, cleanup_sql, -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Prepare cleanup error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	rc = sqlite3_bind_int64(stmt, 1, (sqlite3_int64)cutoff);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Cleanup bind error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		(void) sqlite3_finalize(stmt);
		return (EIO);
	}

	rc = sqlite3_step(stmt);
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Cleanup error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	/* Vacuum to reclaim space; failure is not fatal. */
	{
		char *errmsg = NULL;

		rc = sqlite3_exec(db->sqlite, "VACUUM", NULL, NULL, &errmsg);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "VACUUM error: %s\n",
			    errmsg != NULL ? errmsg : "unknown");
			sqlite3_free(errmsg);
		}
	}

	return (0);
}

int
zmetad_db_stats(zmetad_db_t *db, uint64_t *event_count, uint64_t *db_size)
{
	sqlite3_stmt *stmt = NULL;
	int rc;

	*event_count = 0;
	*db_size = 0;

	/* Get event count */
	rc = sqlite3_prepare_v2(db->sqlite, "SELECT COUNT(*) FROM events",
	    -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Prepare stats error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		*event_count = (uint64_t)sqlite3_column_int64(stmt, 0);
	}
	(void) sqlite3_finalize(stmt);
	stmt = NULL;
	if (rc != SQLITE_ROW) {
		fprintf(stderr, "Event count error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	/* Get database page count * page size */
	rc = sqlite3_prepare_v2(db->sqlite,
	    "SELECT page_count * page_size "
	    "FROM pragma_page_count, pragma_page_size",
	    -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		*event_count = 0;
		fprintf(stderr, "Prepare db size error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		*db_size = (uint64_t)sqlite3_column_int64(stmt, 0);
	}
	(void) sqlite3_finalize(stmt);
	if (rc != SQLITE_ROW) {
		*event_count = 0;
		*db_size = 0;
		fprintf(stderr, "Db size error: %s\n",
		    sqlite3_errmsg(db->sqlite));
		return (EIO);
	}

	return (0);
}
