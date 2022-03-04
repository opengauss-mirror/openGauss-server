/* -------------------------------------------------------------------------
 *
 * dbcommands.cpp
 *		Database management commands (create/drop database).
 *
 * Note: database creation/destruction commands use exclusive locks on
 * the database objects (as expressed by LockSharedObject()) to avoid
 * stepping on each others' toes.  Formerly we used table-level locks
 * on pg_database, but that's too coarse-grained.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/dbcommands.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>

#include "executor/executor.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/multixact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_job.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_uid_fn.h"
#include "catalog/pgxc_slice.h"
#include "catalog/storage_xlog.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "replication/slot.h"
#include "commands/sec_rls_cmds.h"
#include "commands/seclabel.h"
#include "commands/tablespace.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "postmaster/rbcleaner.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "storage/copydir.h"
#include "storage/lmgr.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/smgr/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "utils/timestamp.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "access/gtm.h"
#include "pgxc/poolutils.h"
#include "tcop/utility.h"
#endif
#include "storage/dfs/dfs_connector.h"
#include "storage/smgr/segment.h"

typedef struct {
    Oid src_dboid;  /* source (template) DB */
    Oid dest_dboid; /* DB we are trying to create */
} createdb_failure_params;

typedef struct {
    Oid dest_dboid; /* DB we are trying to move */
    Oid dest_tsoid; /* tablespace we are trying to move to */
#ifdef PGXC
    Oid src_tsoid; /* tablespace we are trying to move from */
#endif
} movedb_failure_params;

/* non-export function prototypes */
static void createdb_failure_callback(int code, Datum arg);
static void movedb(const char* dbname, const char* tblspcname);
static void movedb_failure_callback(int code, Datum arg);
static bool get_db_info(const char* name, LOCKMODE lockmode, Oid* dbIdP, Oid* ownerIdP, int* encodingP,
    bool* dbIsTemplateP, bool* dbAllowConnP, Oid* dbLastSysOidP, TransactionId* dbFrozenXidP, MultiXactId *dbMinMultiP,
    Oid* dbTablespace, char** dbCollate, char** dbCtype, char** src_compatibility = NULL);
static void remove_dbtablespaces(Oid db_id);
static bool check_db_file_conflict(Oid db_id);
static void createdb_xact_callback(bool isCommit, const void* arg);
static void movedb_xact_callback(bool isCommit, const void* arg);
static void movedb_success_callback(Oid db_id, Oid tblspcoid);
static void AlterDatabasePrivateObject(const Form_pg_database fbform, Oid dbid, bool enablePrivateObject);

/*
 * Check if user is a logic cluster user
 */
bool userbindlc(Oid rolid)
{
    if (!in_logic_cluster())
        return false;

    bool isNull = true;

    HeapTuple tup = SearchSysCache1(AUTHOID, rolid);

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for role with oid %u", rolid)));

    Datum datum = SysCacheGetAttr(AUTHOID, tup, Anum_pg_authid_rolnodegroup, &isNull);
    if (!isNull) {
        Oid groupoid = DatumGetObjectId(datum);
        if (OidIsValid(groupoid))
            return true;
    }

    ReleaseSysCache(tup);
    return false;
}

/*
 * CREATE DATABASE
 */
void createdb(const CreatedbStmt* stmt)
{
    TableScanDesc scan;
    Relation rel;
    Oid src_dboid;
    Oid src_owner;
    int src_encoding;
    char* src_collate = NULL;
    char* src_ctype = NULL;
    char* src_compatibility = NULL;
    bool src_istemplate = false;
    bool src_allowconn = false;
    Oid src_lastsysoid;
    TransactionId src_frozenxid;
    MultiXactId src_minmxid;
    Oid src_deftablespace;
    volatile Oid dst_deftablespace;
    Relation pg_database_rel;
    HeapTuple tuple;
    Datum new_record[Natts_pg_database];
    bool new_record_nulls[Natts_pg_database];
    Oid dboid;
    Oid datdba;
    ListCell* option = NULL;
    DefElem* dtablespacename = NULL;
    DefElem* downer = NULL;
    DefElem* dtemplate = NULL;
    DefElem* dencoding = NULL;
    DefElem* dcollate = NULL;
    DefElem* dctype = NULL;
    DefElem* dcompatibility = NULL;
    DefElem* dconnlimit = NULL;
    char* dbname = stmt->dbname;
    char* dbowner = NULL;
    const char* dbtemplate = NULL;
    char* dbcollate = NULL;
    char* dbctype = NULL;
    char* dbcompatibility = NULL;
    char* canonname = NULL;
    int encoding = -1;
    int dbconnlimit = -1;
    int notherbackends;
    int npreparedxacts;
    createdb_failure_params fparms;
    Snapshot snapshot;

    /* Extract options from the statement node tree */
    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "tablespace") == 0) {
            if (dtablespacename != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dtablespacename = defel;
        } else if (strcmp(defel->defname, "owner") == 0) {
            if (downer != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            downer = defel;
        } else if (strcmp(defel->defname, "template") == 0) {
            if (dtemplate != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dtemplate = defel;
        } else if (strcmp(defel->defname, "encoding") == 0) {
            if (dencoding != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dencoding = defel;
        } else if (strcmp(defel->defname, "lc_collate") == 0) {
            if (dcollate != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dcollate = defel;
        } else if (strcmp(defel->defname, "lc_ctype") == 0) {
            if (dctype != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dctype = defel;
        } else if (strcmp(defel->defname, "connectionlimit") == 0) {
            if (dconnlimit != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dconnlimit = defel;
        } else if (strcmp(defel->defname, "dbcompatibility") == 0) {
            if (dcompatibility != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dcompatibility = defel;
        } else if (strcmp(defel->defname, "location") == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("LOCATION is not supported anymore"),
                    errhint("Consider using tablespaces instead.")));
        } else
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
    }

    if (downer != NULL && downer->arg != NULL)
        dbowner = strVal(downer->arg);
    if (dtemplate != NULL && dtemplate->arg != NULL) {
        dbtemplate = strVal(dtemplate->arg);
        /* make sure template is from template0 */
        if (strcmp(dbtemplate, "template0") != 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s is not supported for using here, just support template0", dbtemplate)));
        }
    }
    if (dencoding != NULL && dencoding->arg != NULL) {
        const char* encoding_name = NULL;

        if (IsA(dencoding->arg, Integer)) {
            encoding = intVal(dencoding->arg);
            encoding_name = pg_encoding_to_char(encoding);
            if (strcmp(encoding_name, "") == 0 || pg_valid_server_encoding(encoding_name) < 0)
                ereport(
                    ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%d is not a valid encoding code", encoding)));
        } else if (IsA(dencoding->arg, String)) {
            encoding_name = strVal(dencoding->arg);
            encoding = pg_valid_server_encoding(encoding_name);
            if (encoding < 0)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%s is not a valid encoding name", encoding_name)));
        } else
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", nodeTag(dencoding->arg))));
    }
    if (dcollate != NULL && dcollate->arg != NULL)
        dbcollate = strVal(dcollate->arg);
    if (dctype != NULL && dctype->arg != NULL)
        dbctype = strVal(dctype->arg);
    if (dcompatibility != NULL && dcompatibility->arg != NULL) {
        dbcompatibility = strVal(dcompatibility->arg);
    }

    if (dconnlimit != NULL && dconnlimit->arg != NULL) {
        dbconnlimit = intVal(dconnlimit->arg);
        if (dbconnlimit < -1)
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid connection limit: %d", dbconnlimit)));
    }

    /* obtain OID of proposed owner */
    if (dbowner != NULL)
        datdba = get_role_oid(dbowner, false);
    else
        datdba = GetUserId();

    /*
     * To create a database, must have createdb privilege and must be able to
     * become the target role (this does not imply that the target role itself
     * must have createdb privilege).  The latter provision guards against
     * "giveaway" attacks.	Note that a superuser will always have both of
     * these privileges a fortiori.
     */
    if (!have_createdb_privilege())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to create database")));

    check_is_member_of_role(GetUserId(), datdba);

    if (userbindlc(datdba))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("database cannot be owned by logic cluster user.")));

    /*
     * Lookup database (template) to be cloned, and obtain share lock on it.
     * ShareLock allows two CREATE DATABASEs to work from the same template
     * concurrently, while ensuring no one is busy dropping it in parallel
     * (which would be Very Bad since we'd likely get an incomplete copy
     * without knowing it).  This also prevents any new connections from being
     * made to the source until we finish copying it, so we can be sure it
     * won't change underneath us.
     */
    if (dbtemplate == NULL) {
        if (IsBootstrapProcessingMode() || !IsUnderPostmaster) {
            /* When initdb called make sure template0 and postgres can be created by template1 successfully */
            dbtemplate = "template1";
        } else {
            /* Default template database name */
            dbtemplate = "template0";
        }
    }

    if (!get_db_info(
        dbtemplate,
        ShareLock,
        &src_dboid,
        &src_owner,
        &src_encoding,
        &src_istemplate,
        &src_allowconn,
        &src_lastsysoid,
        &src_frozenxid,
        &src_minmxid,
        &src_deftablespace,
        &src_collate,
        &src_ctype,
        &src_compatibility)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("template database \"%s\" does not exist", dbtemplate)));
    }

    /*
     * Permission check: to copy a DB that's not marked datistemplate, you
     * must be superuser or the owner thereof.
     */
    if (!src_istemplate) {
        if (!pg_database_ownercheck(src_dboid, GetUserId()))
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("permission denied to copy database \"%s\"", dbtemplate)));
    }

    RbCltPurgeDatabase(src_dboid);

    /* If encoding or locales are defaulted, use source's setting */
    if (encoding < 0) {
        encoding = src_encoding;
    }
    if (dbcollate == NULL)
        dbcollate = src_collate;
    if (dbctype == NULL)
        dbctype = src_ctype;
    if (dbcompatibility == NULL)
        dbcompatibility = src_compatibility;

    /* Some encodings are client only */
    if (!PG_VALID_BE_ENCODING(encoding))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("invalid server encoding %d", encoding)));

    /* Check that the chosen locales are valid, and get canonical spellings */
    if (!check_locale(LC_COLLATE, dbcollate, &canonname))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("invalid locale name: \"%s\"", dbcollate)));
    dbcollate = canonname;
    if (!check_locale(LC_CTYPE, dbctype, &canonname))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("invalid locale name: \"%s\"", dbctype)));
    dbctype = canonname;

    check_encoding_locale_matches(encoding, dbcollate, dbctype);

    /*
     * Check that the new encoding and locale settings match the source
     * database.  We insist on this because we simply copy the source data ---
     * any non-ASCII data would be wrongly encoded, and any indexes sorted
     * according to the source locale would be wrong.
     *
     * However, we assume that template0 doesn't contain any non-ASCII data
     * nor any indexes that depend on collation or ctype, so template0 can be
     * used as template for creating a database with any encoding or locale.
     */
    if (strcmp(dbtemplate, "template0") != 0) {
        if (encoding != src_encoding)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("new encoding (%s) is incompatible with the encoding of the template database (%s)",
                        pg_encoding_to_char(encoding),
                        pg_encoding_to_char(src_encoding)),
                    errhint("Use the same encoding as in the template database, or use template0 as template.")));

        if (strcmp(dbcollate, src_collate) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("new collation (%s) is incompatible with the collation of the template database (%s)",
                        dbcollate,
                        src_collate),
                    errhint("Use the same collation as in the template database, or use template0 as template.")));

        if (strcmp(dbctype, src_ctype) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("new LC_CTYPE (%s) is incompatible with the LC_CTYPE of the template database (%s)",
                        dbctype,
                        src_ctype),
                    errhint("Use the same LC_CTYPE as in the template database, or use template0 as template.")));
    }

    /* Resolve default tablespace for new database */
    if (dtablespacename != NULL && dtablespacename->arg != NULL) {
        char* tablespacename = NULL;
        AclResult aclresult;

        tablespacename = strVal(dtablespacename->arg);
        dst_deftablespace = get_tablespace_oid(tablespacename, false);
        /* check permissions */
        aclresult = pg_tablespace_aclcheck(dst_deftablespace, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, tablespacename);

        /* pg_global must never be the default tablespace */
        if (dst_deftablespace == GLOBALTABLESPACE_OID)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("pg_global cannot be used as default tablespace")));

        if (IsSpecifiedTblspc(dst_deftablespace, FILESYSTEM_HDFS)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("DFS tablespace can not be used as default tablespace.")));
        }

        /*
         * If we are trying to change the default tablespace of the template,
         * we require that the template not have any files in the new default
         * tablespace.	This is necessary because otherwise the copied
         * database would contain pg_class rows that refer to its default
         * tablespace both explicitly (by OID) and implicitly (as zero), which
         * would cause problems.  For example another CREATE DATABASE using
         * the copied database as template, and trying to change its default
         * tablespace again, would yield outright incorrect results (it would
         * improperly move tables to the new default tablespace that should
         * stay in the same tablespace).
         */
        if (dst_deftablespace != src_deftablespace) {
            char* srcpath = NULL;
            struct stat st;

            srcpath = GetDatabasePath(src_dboid, dst_deftablespace);

            if (stat(srcpath, &st) == 0 && S_ISDIR(st.st_mode) && !directory_is_empty(srcpath))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot assign new default tablespace \"%s\"", tablespacename),
                        errdetail(
                            "There is a conflict because database \"%s\" already has some tables in this tablespace.",
                            dbtemplate)));
            pfree_ext(srcpath);
        }
    } else {
        /* Use template database's default tablespace */
        dst_deftablespace = src_deftablespace;
        /* Note there is no additional permission check in this path */
    }

    /*
     * Check for db name conflict.	This is just to give a more friendly error
     * message than "unique index violation".  There's a race condition but
     * we're willing to accept the less friendly message in that case.
     */
    if (OidIsValid(get_database_oid(dbname, true)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_DATABASE), errmsg("database \"%s\" already exists", dbname)));

    /*
     * The source DB can't have any active backends, except this one
     * (exception is to allow CREATE DB while connected to template1).
     * Otherwise we might copy inconsistent data.
     *
     * This should be last among the basic error checks, because it involves
     * potential waiting; we may as well throw an error first if we're gonna
     * throw one.
     */

    LockDatabaseObject(DatabaseRelationId, src_dboid, 0, AccessExclusiveLock);
    if (CountOtherDBBackends(src_dboid, &notherbackends, &npreparedxacts))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("source database \"%s\" is being accessed by other users", dbtemplate),
                errdetail_busy_db(notherbackends, npreparedxacts)));

    /*
     * Select an OID for the new database, checking that it doesn't have a
     * filename conflict with anything already existing in the tablespace
     * directories.
     */
    pg_database_rel = heap_open(DatabaseRelationId, RowExclusiveLock);

    do {
        dboid = GetNewOid(pg_database_rel);
    } while (check_db_file_conflict(dboid));

    /*
     * Insert a new tuple into pg_database.  This establishes our ownership of
     * the new database name (anyone else trying to insert the same name will
     * block on the unique index, and fail after we commit).
     */
    /* Form tuple */
    errno_t rc = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(rc, "", "");
    rc = memset_s(new_record_nulls, sizeof(new_record_nulls), 0, sizeof(new_record_nulls));
    securec_check(rc, "", "");

    new_record[Anum_pg_database_datname - 1] = DirectFunctionCall1(namein, CStringGetDatum(dbname));
    new_record[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(datdba);
    new_record[Anum_pg_database_encoding - 1] = Int32GetDatum(encoding);
    new_record[Anum_pg_database_datcollate - 1] = DirectFunctionCall1(namein, CStringGetDatum(dbcollate));
    new_record[Anum_pg_database_datctype - 1] = DirectFunctionCall1(namein, CStringGetDatum(dbctype));
    new_record[Anum_pg_database_datistemplate - 1] = BoolGetDatum(false);
    new_record[Anum_pg_database_datallowconn - 1] = BoolGetDatum(true);
    new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(dbconnlimit);
    new_record[Anum_pg_database_datlastsysoid - 1] = ObjectIdGetDatum(src_lastsysoid);
    new_record[Anum_pg_database_datfrozenxid - 1] = ShortTransactionIdGetDatum(src_frozenxid);
    new_record[Anum_pg_database_dattablespace - 1] = ObjectIdGetDatum(dst_deftablespace);
    new_record[Anum_pg_database_compatibility - 1] = DirectFunctionCall1(namein, CStringGetDatum(dbcompatibility));

    /*
     * We deliberately set datacl to default (NULL), rather than copying it
     * from the template database.	Copying it would be a bad idea when the
     * owner is not the same as the template's owner.
     */
    new_record_nulls[Anum_pg_database_datacl - 1] = true;
    new_record[Anum_pg_database_datfrozenxid64 - 1] = TransactionIdGetDatum(src_frozenxid);
#ifndef ENABLE_MULTIPLE_NODES
    new_record[Anum_pg_database_datminmxid - 1] = TransactionIdGetDatum(src_minmxid);
#endif
    tuple = heap_form_tuple(RelationGetDescr(pg_database_rel), new_record, new_record_nulls);

    HeapTupleSetOid(tuple, dboid);

    (void)simple_heap_insert(pg_database_rel, tuple);

    /* Update indexes */
    CatalogUpdateIndexes(pg_database_rel, tuple);

    /*
     * Now generate additional catalog entries associated with the new DB
     */
    /* Register owner dependency */
    recordDependencyOnOwner(DatabaseRelationId, dboid, datdba);

    /* Create pg_shdepend entries for objects within database */
    copyTemplateDependencies(src_dboid, dboid);

    /* Post creation hook for new database */
    InvokeObjectAccessHook(OAT_POST_CREATE, DatabaseRelationId, dboid, 0, NULL);

    /*
     * Force a checkpoint before starting the copy. This will force dirty
     * buffers out to disk, to ensure source database is up-to-date on disk
     * for the copy. FlushDatabaseBuffers() would suffice for that, but we
     * also want to process any pending unlink requests. Otherwise, if a
     * checkpoint happened while we're copying files, a file might be deleted
     * just when we're about to copy it, causing the lstat() call in copydir()
     * to fail with ENOENT.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * Take an MVCC snapshot to use while scanning through pg_tablespace.  For
     * safety, register the snapshot (this prevents it from changing if
     * something else were to request a snapshot during the loop).
     *
     * Traversing pg_tablespace with an MVCC snapshot is necessary to provide
     * us with a consistent view of the tablespaces that exist.  Using
     * SnapshotNow here would risk seeing the same tablespace multiple times,
     * or worse not seeing a tablespace at all, if its tuple is moved around
     * by a concurrent update (eg an ACL change).
     *
     * Inconsistency of this sort is inherent to all SnapshotNow scans, unless
     * some lock is held to prevent concurrent updates of the rows being
     * sought.	There should be a generic fix for that, but in the meantime
     * it's worth fixing this case in particular because we are doing very
     * heavyweight operations within the scan, so that the elapsed time for
     * the scan is vastly longer than for most other catalog scans.  That
     * means there's a much wider window for concurrent updates to cause
     * trouble here than anywhere else.  XXX this code should be changed
     * whenever a generic fix is implemented.
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());

    /*
     * Once we start copying subdirectories, we need to be able to clean 'em
     * up if we fail.  Use an ENSURE block to make sure this happens.  (This
     * is not a 100% solution, because of the possibility of failure during
     * transaction commit after we leave this routine, but it should handle
     * most scenarios.)
     */
    fparms.src_dboid = src_dboid;
    fparms.dest_dboid = dboid;
    PG_ENSURE_ERROR_CLEANUP(createdb_failure_callback, PointerGetDatum(&fparms));
    {
        /*
         * Iterate through all tablespaces of the template database, and copy
         * each one to the new database.
         */
        rel = heap_open(TableSpaceRelationId, AccessShareLock);
        scan = tableam_scan_begin(rel, snapshot, 0, NULL);
        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            Oid srctablespace = HeapTupleGetOid(tuple);
            Oid dsttablespace;
            char* srcpath = NULL;
            char* dstpath = NULL;
            struct stat st;

            /* No need to copy global tablespace */
            if (srctablespace == GLOBALTABLESPACE_OID)
                continue;

            srcpath = GetDatabasePath(src_dboid, srctablespace);

            if (stat(srcpath, &st) < 0 || !S_ISDIR(st.st_mode) || directory_is_empty(srcpath)) {
                /* Assume we can ignore it */
                pfree_ext(srcpath);
                continue;
            }

            if (srctablespace == src_deftablespace)
                dsttablespace = dst_deftablespace;
            else
                dsttablespace = srctablespace;
            
            dstpath = GetDatabasePath(dboid, dsttablespace);

            /*
             * Copy this subdirectory to the new location
             *
             * We don't need to copy subdirectories
             */
            (void)copydir(srcpath, dstpath, false, ERROR);

            /* Record the filesystem change in XLOG */
            {
                xl_dbase_create_rec xlrec;

                xlrec.db_id = dboid;
                xlrec.tablespace_id = dsttablespace;
                xlrec.src_db_id = src_dboid;
                xlrec.src_tablespace_id = srctablespace;

                XLogBeginInsert();
                XLogRegisterData((char*)&xlrec, sizeof(xl_dbase_create_rec));

                (void)XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE | XLR_SPECIAL_REL_UPDATE);
            }
        }
        tableam_scan_end(scan);
        heap_close(rel, AccessShareLock);

        /*
         * We force a checkpoint before committing.  This effectively means
         * that committed XLOG_DBASE_CREATE operations will never need to be
         * replayed (at least not in ordinary crash recovery; we still have to
         * make the XLOG entry for the benefit of PITR operations). This
         * avoids two nasty scenarios:
         *
         * #1: When PITR is off, we don't XLOG the contents of newly created
         * indexes; therefore the drop-and-recreate-whole-directory behavior
         * of DBASE_CREATE replay would lose such indexes.
         *
         * #2: Since we have to recopy the source database during DBASE_CREATE
         * replay, we run the risk of copying changes in it that were
         * committed after the original CREATE DATABASE command but before the
         * system crash that led to the replay.  This is at least unexpected
         * and at worst could lead to inconsistencies, eg duplicate table
         * names.
         *
         * (Both of these were real bugs in releases 8.0 through 8.0.3.)
         *
         * In PITR replay, the first of these isn't an issue, and the second
         * is only a risk if the CREATE DATABASE and subsequent template
         * database change both occur while a base backup is being taken.
         * There doesn't seem to be much we can do about that except document
         * it as a limitation.
         *
         * Perhaps if we ever implement CREATE DATABASE in a less cheesy way,
         * we can avoid this.
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
        /*
         * Wait for last checkpoint sync to standby and then flush the latest lsn to disk;
         */
        WaitCheckpointSync();
        CheckPointReplicationSlots();
        /*
         * Close pg_database, but keep lock till commit.
         */
        heap_close(pg_database_rel, NoLock);

        /*
         * Force synchronous commit, thus minimizing the window between
         * creation of the database files and commital of the transaction. If
         * we crash before committing, we'll have a DB that's taking up disk
         * space but is not in pg_database, which is not good.
         */
        ForceSyncCommit();
    }
    PG_END_ENSURE_ERROR_CLEANUP(createdb_failure_callback, PointerGetDatum(&fparms));
    /* Free our snapshot */
    UnregisterSnapshot(snapshot);
#ifdef PGXC
    /*
     * Even if we are successful, ultimately this transaction can be aborted
     * because some other node failed. So arrange for cleanup on transaction
     * abort.
     * Unregistering snapshot above, will not have any problem when the
     * callback function is executed because the callback register's its
     * own snapshot in function remove_dbtablespaces
     */
    set_dbcleanup_callback(createdb_xact_callback, &fparms.dest_dboid, sizeof(fparms.dest_dboid));
#endif
}

/*
 * Check whether chosen encoding matches chosen locale settings.  This
 * restriction is necessary because libc's locale-specific code usually
 * fails when presented with data in an encoding it's not expecting. We
 * allow mismatch in four cases:
 *
 * 1. locale encoding = SQL_ASCII, which means that the locale is C/POSIX
 * which works with any encoding.
 *
 * 2. locale encoding = -1, which means that we couldn't determine the
 * locale's encoding and have to trust the user to get it right.
 *
 * 3. selected encoding is UTF8 and platform is win32. This is because
 * UTF8 is a pseudo codepage that is supported in all locales since it's
 * converted to UTF16 before being used.
 *
 * 4. selected encoding is SQL_ASCII, but only if you're a superuser. This
 * is risky but we have historically allowed it --- notably, the
 * regression tests require it.
 *
 * Note: if you change this policy, fix initdb to match.
 */
void check_encoding_locale_matches(int encoding, const char* collate, const char* ctype)
{
    int ctype_encoding;
    int collate_encoding;

    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();
    ctype_encoding = pg_get_encoding_from_locale(ctype, true);
    collate_encoding = pg_get_encoding_from_locale(collate, true);
    localeLock.unLock();

    if (!(ctype_encoding == encoding || ctype_encoding == PG_SQL_ASCII || ctype_encoding == -1 ||
#ifdef WIN32
            encoding == PG_UTF8 ||
#endif
            (encoding == PG_SQL_ASCII && superuser())))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("encoding \"%s\" does not match locale \"%s\"", pg_encoding_to_char(encoding), ctype),
                errdetail(
                    "The chosen LC_CTYPE setting requires encoding \"%s\".", pg_encoding_to_char(ctype_encoding))));

    if (!(collate_encoding == encoding || collate_encoding == PG_SQL_ASCII || collate_encoding == -1 ||
#ifdef WIN32
            encoding == PG_UTF8 ||
#endif
            (encoding == PG_SQL_ASCII && superuser())))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("encoding \"%s\" does not match locale \"%s\"", pg_encoding_to_char(encoding), collate),
                errdetail(
                    "The chosen LC_COLLATE setting requires encoding \"%s\".", pg_encoding_to_char(collate_encoding))));
}

#ifdef PGXC
/*
 * Error cleanup callback for createdb. Aftec createdb() succeeds, the
 * transaction can still be aborted due to other nodes. So on abort-transaction,
 * this function is called to do the cleanup. This involves removing directories
 * created after successful completion.
 * Nothing to be done on commit.
 */
static void createdb_xact_callback(bool isCommit, const void* arg)
{
    if (isCommit)
        return;

    /* Throw away any successfully copied subdirectories */
    remove_dbtablespaces(*(Oid*)arg);
}
#endif

/* Error cleanup callback for createdb */
static void createdb_failure_callback(int code, Datum arg)
{
    createdb_failure_params* fparms = (createdb_failure_params*)DatumGetPointer(arg);

    /*
     * Release lock on source database before doing recursive remove. This is
     * not essential but it seems desirable to release the lock as soon as
     * possible.
     */
    UnlockSharedObject(DatabaseRelationId, fparms->src_dboid, 0, ShareLock);

    /* Throw away any successfully copied subdirectories */
    remove_dbtablespaces(fparms->dest_dboid);
}

void ts_dropdb_xact_callback(bool isCommit, const void* arg)
{
    Oid* db_oid = (Oid*)arg;
    if (!isCommit) {
        *db_oid = InvalidOid;
    }
    (void)pg_atomic_sub_fetch_u32(&g_instance.ts_compaction_cxt.drop_db_count, 1);
    ereport(LOG, (errmodule(MOD_TIMESERIES), errcode(ERRCODE_LOG),
        errmsg("drop db callback have drop db session count after subtract %u", 
        pg_atomic_read_u32(&g_instance.ts_compaction_cxt.drop_db_count))));
}

#ifdef ENABLE_MULTIPLE_NODES
void handle_compaction_dropdb(const char* dbname)
{
    /**
     * if timeseries db and compaction working, send sigusr1 to compaction producer
     * signal will block producer switch session.
     */
    if (!IS_PGXC_DATANODE) {
        return;
    }
    if (g_instance.attr.attr_common.enable_tsdb && u_sess->attr.attr_common.enable_ts_compaction) {
        Oid drop_databse_oid = get_database_oid(dbname, false);
        (void)pg_atomic_add_fetch_u32(&g_instance.ts_compaction_cxt.drop_db_count, 1);
        g_instance.ts_compaction_cxt.dropdb_id = drop_databse_oid;
        ereport(LOG, (errmodule(MOD_TIMESERIES), errcode(ERRCODE_LOG),
            errmsg("drop db callback have drop db session count after add %u", 
            pg_atomic_read_u32(&g_instance.ts_compaction_cxt.drop_db_count))));
               
        while (!g_instance.ts_compaction_cxt.compaction_rest && 
            g_instance.ts_compaction_cxt.state == Compaction::COMPACTION_IN_PROGRESS) {
            pg_usleep(100 * USECS_PER_MSEC);
        }
        
        set_dbcleanup_callback(ts_dropdb_xact_callback, &g_instance.ts_compaction_cxt.dropdb_id,
            sizeof(g_instance.ts_compaction_cxt.dropdb_id));
  
    }
}
#endif

typedef struct {
    int nPendingDeletes;
    Oid dbOid;
    Oid pendingDeletes[1];
} DropDbArg;

static void InsertDropDbInfo(Oid dbOid, DropDbArg** dropDbInfo, const List *pendingDeletesList)
{
    int nPendingDeletes = list_length(pendingDeletesList);
    *dropDbInfo = (DropDbArg*)palloc(offsetof(DropDbArg, pendingDeletes) + nPendingDeletes * sizeof(Oid));
    (*dropDbInfo)->nPendingDeletes = nPendingDeletes;
    (*dropDbInfo)->dbOid = dbOid;
    ListCell *lc = NULL;
    int index = 0;
    foreach(lc, pendingDeletesList) {
        Oid dsttablespace = lfirst_oid(lc);
        (*dropDbInfo)->pendingDeletes[index++] = dsttablespace;
    }
}

static void InitDropDbInfo(Oid dbOid, DropDbArg** dropDbInfo)
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    Snapshot snapshot;
    List *pendingDeletesList = NULL;
    /*
     * As in createdb(), we'd better use an MVCC snapshot here, since this
     * scan can run for a long time.  Duplicate visits to tablespaces would be
     * harmless, but missing a tablespace could result in permanently leaked
     * files.
     *
     * XXX change this when a generic fix for SnapshotNow races is implemented
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());
    
    rel = heap_open(TableSpaceRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, snapshot, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Oid dsttablespace = HeapTupleGetOid(tuple);
        char* dstpath = NULL;
        struct stat st;

        /* Don't mess with the global tablespace */
        if (dsttablespace == GLOBALTABLESPACE_OID)
            continue;

        dstpath = GetDatabasePath(dbOid, dsttablespace);

        if (lstat(dstpath, &st) < 0 || !S_ISDIR(st.st_mode)) {
            /* Assume we can ignore it */
            pfree_ext(dstpath);
            continue;
        }
        /* add vaild path oid */
        pendingDeletesList = lappend_oid(pendingDeletesList, dsttablespace);
        pfree_ext(dstpath);
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);
    UnregisterSnapshot(snapshot);

    InsertDropDbInfo(dbOid, dropDbInfo, pendingDeletesList);
    list_free(pendingDeletesList);
}
/* 
 * Use record tablespace oid list instead of using snapshot scan,
 * because we cannot see the information of target db after dropping
 * database success.
 */
static void DropdbXactCallback(bool isCommit, const void* arg)
{
    if (!isCommit) {
        return;
    }
    DropDbArg *dropDbInfo = (DropDbArg *)arg;
    Oid dbOid = dropDbInfo->dbOid;
    int len = dropDbInfo->nPendingDeletes;
    int index;
    for (index = 0; index < len; index++) {
        Oid dsttablespace = dropDbInfo->pendingDeletes[index];
        char* dstpath = NULL;
        struct stat st;

        /* Don't mess with the global tablespace */
        if (dsttablespace == GLOBALTABLESPACE_OID)
            continue;

        dstpath = GetDatabasePath(dbOid, dsttablespace);

        if (lstat(dstpath, &st) < 0 || !S_ISDIR(st.st_mode)) {
            /* Assume we can ignore it */
            pfree_ext(dstpath);
            continue;
        }

        if (!rmtree(dstpath, true))
            ereport(
                WARNING, (errmsg("some useless files may be left behind in old database directory \"%s\"", dstpath)));

        /* Record the filesystem change in XLOG */
        {
            xl_dbase_drop_rec xlrec;

            xlrec.db_id = dbOid;
            xlrec.tablespace_id = dsttablespace;

            XLogBeginInsert();
            XLogRegisterData((char*)&xlrec, sizeof(xl_dbase_drop_rec));

            (void)XLogInsert(RM_DBASE_ID, XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);
        }

        pfree_ext(dstpath);
    }
}

/*
 * DROP DATABASE
 */
void dropdb(const char* dbname, bool missing_ok)
{
    Oid db_id;
    bool db_istemplate = false;
    Relation pgdbrel;
    HeapTuple tup;
    int notherbackends;
    int npreparedxacts;
    int			nsubscriptions;

    /* If we will return before reaching function end, please release this lock */
    LWLockAcquire(DelayDDLLock, LW_SHARED);

    if (!XLogRecPtrIsInvalid(GetDDLDelayStartPtr()))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("could not drop database "
                       "while ddl delay function is enabled")));

#ifdef ENABLE_MULTIPLE_NODES
    handle_compaction_dropdb(dbname);
#endif
    /*
     * Look up the target database's OID, and get exclusive lock on it. We
     * need this to ensure that no new backend starts up in the target
     * database while we are deleting it (see postinit.c), and that no one is
     * using it as a CREATE DATABASE template or trying to delete it for
     * themselves.
     */
    pgdbrel = heap_open(DatabaseRelationId, RowExclusiveLock);

    if (!get_db_info(
            dbname, AccessExclusiveLock, &db_id, NULL, NULL, &db_istemplate,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL)) {
        if (!missing_ok) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", dbname)));
        } else {
            /* Close pg_database, release the lock, since we changed nothing */
            heap_close(pgdbrel, RowExclusiveLock);
            ereport(NOTICE, (errmsg("database \"%s\" does not exist, skipping", dbname)));
            LWLockRelease(DelayDDLLock);
            return;
        }
    }

    gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());

    /*
     * Permission checks
     */
    AclResult aclresult = pg_database_aclcheck(db_id, GetUserId(), ACL_DROP);
    if (aclresult != ACLCHECK_OK && !pg_database_ownercheck(db_id, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_DATABASE, dbname);
    }

    RbCltPurgeDatabase(db_id);

    /* DROP hook for the database being removed */
    if (object_access_hook) {
        ObjectAccessDrop drop_arg;

        errno_t rc = memset_s(&drop_arg, sizeof(ObjectAccessDrop), 0, sizeof(ObjectAccessDrop));
        securec_check(rc, "\0", "\0");
        InvokeObjectAccessHook(OAT_DROP, DatabaseRelationId, db_id, 0, &drop_arg);
    }

    /*
     * Disallow dropping a DB that is marked istemplate.  This is just to
     * prevent people from accidentally dropping template0 or template1; they
     * can do so if they're really determined ...
     */
    if (db_istemplate)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("cannot drop a template database")));

    /* Obviously can't drop my own database */
    if (db_id == u_sess->proc_cxt.MyDatabaseId)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("cannot drop the currently open database")));

    if (0 == strcmp(dbname, DEFAULT_DATABASE))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("cannot drop the default database")));

    /*
     * Check for other backends in the target database.  (Because we hold the
     * database lock, no new ones can start after this.)
     *
     * As in CREATE DATABASE, check this after other error conditions.
     */
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("Database \"%s\" is being accessed by other users. You can stop all connections by command:"
                    " \"clean connection to all force for database XXXX;\" or wait for the sessions to end by querying "
                    "view: \"pg_stat_activity\".", dbname),
                errdetail_busy_db(notherbackends, npreparedxacts)));

    /*
     * Check if there are subscriptions defined in the target database.
     *
     * We can't drop them automatically because they might be holding
     * resources in other databases/instances.
     */
    if ((nsubscriptions = CountDBSubscriptions(db_id)) > 0) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
            errmsg("database \"%s\" is being used by logical replication subscription", dbname),
            errdetail_plural("There is %d subscription.", "There are %d subscriptions.", nsubscriptions,
            nsubscriptions)));
    }

    /* Search need delete use-defined C fun library. */
    prepareDatabaseCFunLibrary(db_id);

    /* Relate to remove all job belong the database. */
    remove_job_by_oid(db_id, DbOid, true);

    /* Search need delete use-defined dictionary. */
    deleteDatabaseTSFile(db_id);

    /*
     * Remove the database's tuple from pg_database.
     */
    tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(db_id));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for database %u", db_id)));

    if (EnableGlobalSysCache()) {
        g_instance.global_sysdbcache.DropDB(db_id, true);
    }
    simple_heap_delete(pgdbrel, &tup->t_self);

    ReleaseSysCache(tup);

    /*
     * Delete any comments or security labels associated with the database.
     */
    DeleteSharedComments(db_id, DatabaseRelationId);
    DeleteSharedSecurityLabel(db_id, DatabaseRelationId);

    /*
     * Remove settings associated with this database
     */
    DropSetting(db_id, InvalidOid);

    /*
     * Remove shared dependency references for the database.
     */
    dropDatabaseDependencies(db_id);
    DeleteDatabaseUidEntry(db_id);

    /*
     * Request an immediate checkpoint to flush all the dirty pages in share buffer
     * before we drop the related buffer pages under the database to keep consistent
     * with the principle of checkpoint.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * Drop pages for this database that are in the shared buffer cache. This
     * is important to ensure that no remaining backend tries to write out a
     * dirty buffer to the dead database later...
     */
    DropDatabaseBuffers(db_id);

    /*
     * Tell the stats collector to forget it immediately, too.
     */
    pgstat_drop_database(db_id);

    /*
     * Tell checkpointer to forget any pending fsync and unlink requests for
     * files in the database; else the fsyncs will fail at next checkpoint, or
     * worse, it will delete files that belong to a newly created database
     * with the same OID.
     */
    ForgetDatabaseSyncRequests(db_id);

    /*
     * Force a checkpoint to make sure the checkpointer has received the
     * message sent by ForgetDatabaseSyncRequests. On Windows, this also
     * ensures that background procs don't hold any open files, which would
     * cause rmdir() to fail.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * Register all pending delete tablespace belonging to the database.
     * Callback function do real work after commit transaction.
     */
    DropDbArg *dropDbInfo = NULL;
    InitDropDbInfo(db_id, &dropDbInfo);
    int dropDbInfoLen = offsetof(DropDbArg, pendingDeletes) + dropDbInfo->nPendingDeletes * sizeof(Oid);
    set_dbcleanup_callback(DropdbXactCallback, dropDbInfo, dropDbInfoLen);
    pfree_ext(dropDbInfo);

    /*
     * Close pg_database, but keep lock till commit.
     */
    heap_close(pgdbrel, NoLock);

    /*
     * Force synchronous commit, thus minimizing the window between removal of
     * the database files and commital of the transaction. If we crash before
     * committing, we'll have a DB that's gone on disk but still there
     * according to pg_database, which is not good.
     */
    ForceSyncCommit();

#ifdef PGXC
    /* Drop sequences on gtm that are on the database dropped. */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        if (DropSequenceGTM(0, dbname))
            ereport(LOG,
                (errcode(ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE),
                    errmsg("Deletion of sequences on database %s not completed", dbname)));
#endif

    LWLockRelease(DelayDDLLock);

    ereport(LOG,(errmsg("drop database \"%s\", id is %u", dbname, db_id)));

    /* need calculate user used space info */
    g_instance.comm_cxt.force_cal_space_info = true;

    /* Drop local connection for dbname */
    if(IS_PGXC_COORDINATOR) {
        DropDBCleanConnection(dbname);
    }
}

/*
 * Must be owner or have alter privilege to alter database
 */
static void AlterDatabasePermissionCheck(Oid dboid, const char* dbname)
{
    AclResult aclresult = pg_database_aclcheck(dboid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_database_ownercheck(dboid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_DATABASE, dbname);
    }
}

/*
 * Rename database
 */
void RenameDatabase(const char* oldname, const char* newname)
{
    Oid db_id;
    HeapTuple newtup;
    Relation rel;
    int notherbackends;
    int npreparedxacts;
    List* existTblSpcList = NIL;
    Relation pg_job_tbl = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;

    /*
     * Look up the target database's OID, and get exclusive lock on it. We
     * need this for the same reasons as DROP DATABASE.
     */
    rel = heap_open(DatabaseRelationId, RowExclusiveLock);

    if (!get_db_info(oldname, AccessExclusiveLock, &db_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", oldname)));

    /* Permission check. */
    AlterDatabasePermissionCheck(db_id, oldname);

    /* must have createdb rights */
    if (!have_createdb_privilege())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to rename database")));

    /*
     * Make sure the new name doesn't exist.  See notes for same error in
     * CREATE DATABASE.
     */
    if (OidIsValid(get_database_oid(newname, true)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_DATABASE), errmsg("database \"%s\" already exists", newname)));

    /*
     * XXX Client applications probably store the current database somewhere,
     * so renaming it could cause confusion.  On the other hand, there may not
     * be an actual problem besides a little confusion, so think about this
     * and decide.
     */
    if (db_id == u_sess->proc_cxt.MyDatabaseId)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("current database cannot be renamed")));

    /*
     * Make sure the database does not have active sessions.  This is the same
     * concern as above, but applied to other sessions.
     *
     * As in CREATE DATABASE, check this after other error conditions.
     */
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("database \"%s\" is being accessed by other users", oldname),
                errdetail_busy_db(notherbackends, npreparedxacts)));

    existTblSpcList = HDFSTablespaceDirExistDatabase(db_id);
    if (existTblSpcList != NIL) {
        StringInfo existTblspc = (StringInfo)linitial(existTblSpcList);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("It is unsupported to rename database \"%s\" on DFS tablespace \"%s\".",
                    oldname,
                    existTblspc->data)));
    }

    /* rename */
    newtup = SearchSysCacheCopy1(DATABASEOID, ObjectIdGetDatum(db_id));
    if (!HeapTupleIsValid(newtup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for database %u", db_id)));
    (void)namestrcpy(&(((Form_pg_database)GETSTRUCT(newtup))->datname), newname);

    /*
     * We have to do GSC DropDb to invalid the GSC content of that database even we do rename on dbName
     * rename db dont change GSC content, but a name swap for two db may cause lsc fake cache hit, which
     * brings about inconsistent data event
     **/
    if (EnableGlobalSysCache()) {
        g_instance.global_sysdbcache.DropDB(db_id, false);
    }

    simple_heap_update(rel, &newtup->t_self, newtup);
    CatalogUpdateIndexes(rel, newtup);

    /* update sequence info */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        RenameSequenceGTM((char*)oldname, (char*)newname, GTM_SEQ_DB_NAME);
    }
    /*
     * Close pg_database, but keep lock till commit.
     */
    heap_close(rel, NoLock);
    /*
     * change the database name in the pg_job. 
     */
    pg_job_tbl = heap_open(PgJobRelationId, ExclusiveLock);
    scan = heap_beginscan(pg_job_tbl, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        Form_pg_job pg_job = (Form_pg_job)GETSTRUCT(tuple);
        if (strcmp(NameStr(pg_job->dbname), oldname) == 0) {
            update_pg_job_dbname(pg_job->job_id, newname);
        }
    }

    heap_endscan(scan);
    heap_close(pg_job_tbl, ExclusiveLock);

}

/*
 * Whether Check existance of a database with given db_id in
 * HDFS tablesacpe or not.
 * @_in_param db_id: The database oid to be checked.
 * @return Return StringInfo list of tablespace name if exists, otherwise return
 * NIL.
 */
List* HDFSTablespaceDirExistDatabase(Oid db_id)
{
    return NIL;
}

#ifdef PGXC
/*
 * IsSetTableSpace:
 * Returns true if it is ALTER DATABASE SET TABLESPACE
 */
bool IsSetTableSpace(AlterDatabaseStmt* stmt)
{
    ListCell* option = NULL;
    /* Handle the SET TABLESPACE option separately */
    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);
        if (strcmp(defel->defname, "tablespace") == 0)
            return true;
    }
    return false;
}

#endif

/*
 * ALTER DATABASE SET TABLESPACE
 */
static void movedb(const char* dbname, const char* tblspcname)
{
    Oid db_id;
    Relation pgdbrel;
    int notherbackends;
    int npreparedxacts;
    HeapTuple oldtuple, newtuple;
    Oid src_tblspcoid, dst_tblspcoid;
    Datum new_record[Natts_pg_database];
    bool new_record_nulls[Natts_pg_database];
    bool new_record_repl[Natts_pg_database];
    ScanKeyData scankey;
    SysScanDesc sysscan;
    AclResult aclresult;
    char* src_dbpath = NULL;
    char* dst_dbpath = NULL;
    DIR* dstdir = NULL;
    struct dirent* xlde = NULL;
    movedb_failure_params fparms;

    /*
     * Look up the target database's OID, and get exclusive lock on it. We
     * need this to ensure that no new backend starts up in the database while
     * we are moving it, and that no one is using it as a CREATE DATABASE
     * template or trying to delete it.
     */
    pgdbrel = heap_open(DatabaseRelationId, RowExclusiveLock);

    if (!get_db_info(
            dbname, AccessExclusiveLock, &db_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &src_tblspcoid, NULL, NULL))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", dbname)));

    /*
     * We actually need a session lock, so that the lock will persist across
     * the commit/restart below.  (We could almost get away with letting the
     * lock be released at commit, except that someone could try to move
     * relations of the DB back into the old directory while we rmtree() it.)
     */
    LockSharedObjectForSession(DatabaseRelationId, db_id, 0, AccessExclusiveLock);

#ifdef PGXC
    /*
     * Now that we have session lock, transaction lock is not necessary.
     * Besides, a PREPARE does not allow both transaction and session lock on the
     * same object
     */
    UnlockSharedObject(DatabaseRelationId, db_id, 0, AccessExclusiveLock);
#endif

    /* Permission check. */
    AlterDatabasePermissionCheck(db_id, dbname);

    /*
     * Obviously can't move the tables of my own database
     */
    if (db_id == u_sess->proc_cxt.MyDatabaseId)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE), errmsg("cannot change the tablespace of the currently open database")));

    /*
     * Get tablespace's oid
     */
    dst_tblspcoid = get_tablespace_oid(tblspcname, false);

    /*
     * Permission checks
     */
    aclresult = pg_tablespace_aclcheck(dst_tblspcoid, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_TABLESPACE, tblspcname);

    /*
     * pg_global must never be the default tablespace
     */
    if (dst_tblspcoid == GLOBALTABLESPACE_OID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("pg_global cannot be used as default tablespace")));

    /*
     * No-op if same tablespace
     */
    if (src_tblspcoid == dst_tblspcoid) {
        heap_close(pgdbrel, NoLock);
        UnlockSharedObjectForSession(DatabaseRelationId, db_id, 0, AccessExclusiveLock);
        return;
    }

    /*
     * Check for other backends in the target database.  (Because we hold the
     * database lock, no new ones can start after this.)
     *
     * As in CREATE DATABASE, check this after other error conditions.
     */
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("Database \"%s\" is being accessed by other users. You can stop all connections by command:"
                    " \"clean connection to all force for database XXXX;\" or wait for the sessions to end by querying "
                    "view: \"pg_stat_activity\".", dbname),
                errdetail_busy_db(notherbackends, npreparedxacts)));

    /*
     * Get old and new database paths
     */
    src_dbpath = GetDatabasePath(db_id, src_tblspcoid);
    dst_dbpath = GetDatabasePath(db_id, dst_tblspcoid);

    /*
     * Force a checkpoint before proceeding. This will force dirty buffers out
     * to disk, to ensure source database is up-to-date on disk for the copy.
     * FlushDatabaseBuffers() would suffice for that, but we also want to
     * process any pending unlink requests. Otherwise, the check for existing
     * files in the target directory might fail unnecessarily, not to mention
     * that the copy might fail due to source files getting deleted under it.
     * On Windows, this also ensures that background procs don't hold any open
     * files, which would cause rmdir() to fail.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * Check for existence of files in the target directory, i.e., objects of
     * this database that are already in the target tablespace.  We can't
     * allow the move in such a case, because we would need to change those
     * relations' pg_class.reltablespace entries to zero, and we don't have
     * access to the DB's pg_class to do so.
     */
    dstdir = AllocateDir(dst_dbpath);
    if (dstdir != NULL) {
        while ((xlde = ReadDir(dstdir, dst_dbpath)) != NULL) {
            if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0)
                continue;

            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("some relations of database \"%s\" are already in tablespace \"%s\"", dbname, tblspcname),
                    errhint(
                        "You must move them back to the database's default tablespace before using this command.")));
        }

        FreeDir(dstdir);

        /*
         * The directory exists but is empty. We must remove it before using
         * the copydir function.
         */
        if (rmdir(dst_dbpath) != 0)
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("could not remove directory \"%s\": %m", dst_dbpath)));
    }

    /*
     * Use an ENSURE block to make sure we remove the debris if the copy fails
     * (eg, due to out-of-disk-space).	This is not a 100% solution, because
     * of the possibility of failure during transaction commit, but it should
     * handle most scenarios.
     */
    fparms.dest_dboid = db_id;
    fparms.dest_tsoid = dst_tblspcoid;
    PG_ENSURE_ERROR_CLEANUP(movedb_failure_callback, PointerGetDatum(&fparms));
    {
        /*
         * Copy files from the old tablespace to the new one
         */
        (void)copydir(src_dbpath, dst_dbpath, false, ERROR);

        /*
         * Record the filesystem change in XLOG
         */
        {
            xl_dbase_create_rec xlrec;

            xlrec.db_id = db_id;
            xlrec.tablespace_id = dst_tblspcoid;
            xlrec.src_db_id = db_id;
            xlrec.src_tablespace_id = src_tblspcoid;

            XLogBeginInsert();
            XLogRegisterData((char*)&xlrec, sizeof(xl_dbase_create_rec));

            (void)XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE | XLR_SPECIAL_REL_UPDATE);
        }

        /*
         * Update the database's pg_database tuple
         */
        ScanKeyInit(&scankey, Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(dbname));
        sysscan = systable_beginscan(pgdbrel, DatabaseNameIndexId, true, NULL, 1, &scankey);
        oldtuple = systable_getnext(sysscan);
        if (!HeapTupleIsValid(oldtuple)) /* shouldn't happen... */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", dbname)));

        errno_t rc = EOK;
        rc = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
        securec_check(rc, "\0", "\0");

        rc = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
        securec_check(rc, "\0", "\0");

        rc = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
        securec_check(rc, "\0", "\0");

        new_record[Anum_pg_database_dattablespace - 1] = ObjectIdGetDatum(dst_tblspcoid);
        new_record_repl[Anum_pg_database_dattablespace - 1] = true;

        newtuple =
            (HeapTuple) tableam_tops_modify_tuple(oldtuple, RelationGetDescr(pgdbrel), new_record, new_record_nulls, new_record_repl);
        if (EnableGlobalSysCache()) {
            g_instance.global_sysdbcache.DropDB(db_id, false);
        }
        simple_heap_update(pgdbrel, &oldtuple->t_self, newtuple);

        /* Update indexes */
        CatalogUpdateIndexes(pgdbrel, newtuple);

        systable_endscan(sysscan);

        /*
         * Force another checkpoint here.  As in CREATE DATABASE, this is to
         * ensure that we don't have to replay a committed XLOG_DBASE_CREATE
         * operation, which would cause us to lose any unlogged operations
         * done in the new DB tablespace before the next checkpoint.
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

        /*
         * Force synchronous commit, thus minimizing the window between
         * copying the database files and commital of the transaction. If we
         * crash before committing, we'll leave an orphaned set of files on
         * disk, which is not fatal but not good either.
         */
        ForceSyncCommit();

        /*
         * Close pg_database, but keep lock till commit.
         */
        heap_close(pgdbrel, NoLock);
    }
    PG_END_ENSURE_ERROR_CLEANUP(movedb_failure_callback, PointerGetDatum(&fparms));
#ifdef PGXC
    /*
     * Even if we are successful, ultimately this transaction may or may not
     * be committed. so arrange for cleanup of source directory or target
     * directory during commit or abort, respectively.
     */
    fparms.src_tsoid = src_tblspcoid;
    set_dbcleanup_callback(movedb_xact_callback, &fparms, sizeof(fparms));
}

/*
 * movedb_success_callback:
 * Cleanup files in the dbpath directory corresponding to db_id and tblspcoid.
 * This function code is actual part of the movedb() operation in PG. We have
 * made a function out of it for PGXC, and it gets called as part of the
 * at-commit xact callback mechanism.
 */
static void movedb_success_callback(Oid db_id, Oid src_tblspcoid)
{
    char* src_dbpath = GetDatabasePath(db_id, src_tblspcoid);
#endif /* PGXC */

    /*
     * Commit the transaction so that the pg_database update is committed. If
     * we crash while removing files, the database won't be corrupt, we'll
     * just leave some orphaned files in the old directory.
     *
     * (This is OK because we know we aren't inside a transaction block.)
     *
     * XXX would it be safe/better to do this inside the ensure block?	Not
     * convinced it's a good idea; consider elog just after the transaction
     * really commits.
     */
#ifdef PGXC
    /*
     * Don't commit the transaction. We don't require the two separate
     * commits since we handle this function as at-commit xact callback.
     */
#else
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Start new transaction for the remaining work; don't need a snapshot */
    StartTransactionCommand();
#endif

    /*
     * Remove files from the old tablespace
     */
    if (!rmtree(src_dbpath, true))
        ereport(
            WARNING, (errmsg("some useless files may be left behind in old database directory \"%s\"", src_dbpath)));

    /*
     * Record the filesystem change in XLOG
     */
    {
        xl_dbase_drop_rec xlrec;

        xlrec.db_id = db_id;
        xlrec.tablespace_id = src_tblspcoid;

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, sizeof(xl_dbase_drop_rec));

        (void)XLogInsert(RM_DBASE_ID, XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);
    }

    /* Now it's safe to release the database lock */
    UnlockSharedObjectForSession(DatabaseRelationId, db_id, 0, AccessExclusiveLock);

#ifdef PGXC
    pfree_ext(src_dbpath);
#endif
}

#ifdef PGXC
/*
 * Error cleanup callback for movedb. Aftec movedb() succeeds, the
 * transaction can still be aborted due to other nodes. So on abort-transaction,
 * this function is called to do the cleanup of target tablespace directory,
 * and on transaction commit, it is called to cleanup source directory.
 */
static void movedb_xact_callback(bool isCommit, const void* arg)
{
    movedb_failure_params* fparms = (movedb_failure_params*)DatumGetPointer(arg);

    if (isCommit)
        movedb_success_callback(fparms->dest_dboid, fparms->src_tsoid);
    else {
        /* Call the same function that is used in ENSURE block for movedb() */
        movedb_failure_callback(XACT_EVENT_ABORT, PointerGetDatum(arg));
    }
}
#endif

/* Error cleanup callback for movedb */
static void movedb_failure_callback(int code, Datum arg)
{
    movedb_failure_params* fparms = (movedb_failure_params*)DatumGetPointer(arg);
    char* dstpath = NULL;

    /* Get rid of anything we managed to copy to the target directory */
    dstpath = GetDatabasePath(fparms->dest_dboid, fparms->dest_tsoid);

    (void)rmtree(dstpath, true);
}

/*
 * ALTER DATABASE name ...
 */
void AlterDatabase(AlterDatabaseStmt* stmt, bool isTopLevel)
{
    Relation rel;
    HeapTuple tuple, newtuple;
    ScanKeyData scankey;
    SysScanDesc scan;
    ListCell* option = NULL;
    int connlimit = -1;
    DefElem* dconnlimit = NULL;
    DefElem* dtablespace = NULL;
    DefElem* privateobject = NULL;
    Datum new_record[Natts_pg_database];
    bool new_record_nulls[Natts_pg_database];
    bool new_record_repl[Natts_pg_database];

    /* Extract options from the statement node tree */
    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "connectionlimit") == 0) {
            if (dconnlimit != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dconnlimit = defel;
        } else if (strcmp(defel->defname, "tablespace") == 0) {
            if (dtablespace != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            dtablespace = defel;
        } else if (strcmp(defel->defname, "privateobject") == 0) {
            if (privateobject != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            privateobject = defel;
        } else
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
    }

    if (dtablespace != NULL) {
        Oid dst_deftablespace = get_tablespace_oid(strVal(dtablespace->arg), false);
        /* currently, can't be specified along with any other options */
        Assert(!dconnlimit);
        if (IsSpecifiedTblspc(dst_deftablespace, FILESYSTEM_HDFS)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("DFS tablespace can not be used as default tablespace.")));
        }
        /* this case isn't allowed within a transaction block */
#ifdef PGXC
        /* Clean connections before alter a database on local node */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            /* clean all connections with dbname on all CNs before db operations */
            PreCleanAndCheckConns(stmt->dbname, false);
        }

        /* ... but we allow it on remote nodes */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
#endif
            PreventTransactionChain(isTopLevel, "ALTER DATABASE SET TABLESPACE");

        movedb(stmt->dbname, strVal(dtablespace->arg));
        return;
    }

    if (dconnlimit != NULL) {
        connlimit = intVal(dconnlimit->arg);
        if (connlimit < -1)
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid connection limit: %d", connlimit)));
    }

    /*
     * Get the old tuple.  We don't need a lock on the database per se,
     * because we're not going to do anything that would mess up incoming
     * connections.
     */
    rel = heap_open(DatabaseRelationId, RowExclusiveLock);
    ScanKeyInit(&scankey, Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(stmt->dbname));
    scan = systable_beginscan(rel, DatabaseNameIndexId, true, NULL, 1, &scankey);
    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", stmt->dbname)));

    /* Permmision Check */
    AlterDatabasePermissionCheck(HeapTupleGetOid(tuple), stmt->dbname);

    /*
     * Build an updated tuple, perusing the information just obtained
     */
    errno_t rc = EOK;
    rc = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(rc, "\0", "\0");

    rc = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
    securec_check(rc, "\0", "\0");

    rc = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(rc, "\0", "\0");

    if (dconnlimit != NULL) {
        new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(connlimit);
        new_record_repl[Anum_pg_database_datconnlimit - 1] = true;
    }

    if (privateobject != NULL) {
        bool enablePrivateObject = (intVal(privateobject->arg) > 0);
        Form_pg_database dbform = (Form_pg_database)GETSTRUCT(tuple);
        AlterDatabasePrivateObject(dbform, HeapTupleGetOid(tuple), enablePrivateObject);
    }

    newtuple = (HeapTuple) tableam_tops_modify_tuple(tuple, RelationGetDescr(rel), new_record, new_record_nulls, new_record_repl);
    if (EnableGlobalSysCache() && privateobject != NULL) {
        g_instance.global_sysdbcache.DropDB(HeapTupleGetOid(tuple), false);
    }
    simple_heap_update(rel, &tuple->t_self, newtuple);

    /* Update indexes */
    CatalogUpdateIndexes(rel, newtuple);

    systable_endscan(scan);

    /* Close pg_database, but keep lock till commit */
    heap_close(rel, NoLock);
}

/*
 * ALTER DATABASE name SET ...
 */
void AlterDatabaseSet(AlterDatabaseSetStmt* stmt)
{
    Oid datid = get_database_oid(stmt->dbname, false);

    /*
     * Obtain a lock on the database and make sure it didn't go away in the
     * meantime.
     */
    shdepLockAndCheckObject(DatabaseRelationId, datid);

    /* Permission check. */
    AlterDatabasePermissionCheck(datid, stmt->dbname);

    AlterSetting(datid, InvalidOid, stmt->setstmt);

#ifdef ENABLE_MULTIPLE_NODES
    printHintInfo(stmt->dbname, NULL);
#endif

    UnlockSharedObject(DatabaseRelationId, datid, 0, AccessShareLock);
}

/*
 * ALTER DATABASE name OWNER TO newowner
 */
void AlterDatabaseOwner(const char* dbname, Oid newOwnerId)
{
    HeapTuple tuple;
    Relation rel;
    ScanKeyData scankey;
    SysScanDesc scan;
    Form_pg_database datForm;

    if (userbindlc(newOwnerId))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("database cannot be owned by logic cluster user.")));

    /*
     * Get the old tuple.  We don't need a lock on the database per se,
     * because we're not going to do anything that would mess up incoming
     * connections.
     */
    rel = heap_open(DatabaseRelationId, RowExclusiveLock);
    ScanKeyInit(&scankey, Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(dbname));
    scan = systable_beginscan(rel, DatabaseNameIndexId, true, NULL, 1, &scankey);
    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", dbname)));

    datForm = (Form_pg_database)GETSTRUCT(tuple);

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is to be consistent with other
     * objects.
     */
    if (datForm->datdba != newOwnerId) {
        Datum repl_val[Natts_pg_database];
        bool repl_null[Natts_pg_database];
        bool repl_repl[Natts_pg_database];
        Acl* newAcl = NULL;
        Datum aclDatum;
        bool isNull = false;
        HeapTuple newtuple;
        errno_t rc;

        /* Otherwise, must be owner of the existing object */
        if (!pg_database_ownercheck(HeapTupleGetOid(tuple), GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE, dbname);

        /* Must be able to become new owner */
        check_is_member_of_role(GetUserId(), newOwnerId);

        /*
         * must have createdb rights
         *
         * NOTE: This is different from other alter-owner checks in that the
         * current user is checked for createdb privileges instead of the
         * destination owner.  This is consistent with the CREATE case for
         * databases.  Because superusers will always have this right, we need
         * no special case for them.
         */
        if (!have_createdb_privilege())
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to change owner of database")));

        rc = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
        securec_check(rc, "\0", "\0");
        rc = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
        securec_check(rc, "\0", "\0");

        repl_repl[Anum_pg_database_datdba - 1] = true;
        repl_val[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = heap_getattr(tuple, Anum_pg_database_datacl, RelationGetDescr(rel), &isNull);
        if (!isNull) {
            newAcl = aclnewowner(DatumGetAclP(aclDatum), datForm->datdba, newOwnerId);
            repl_repl[Anum_pg_database_datacl - 1] = true;
            repl_val[Anum_pg_database_datacl - 1] = PointerGetDatum(newAcl);
        }

        newtuple = (HeapTuple) tableam_tops_modify_tuple(tuple, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
        simple_heap_update(rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);

        tableam_tops_free_tuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(DatabaseRelationId, HeapTupleGetOid(tuple), newOwnerId);
    }

    systable_endscan(scan);

    /* Close pg_database, but keep lock till commit */
    heap_close(rel, NoLock);
}

/*
 * Helper functions
 */
/*
 * Look up info about the database named "name".  If the database exists,
 * obtain the specified lock type on it, fill in any of the remaining
 * parameters that aren't NULL, and return TRUE.  If no such database,
 * return FALSE.
 */
static bool get_db_info(const char* name, LOCKMODE lockmode, Oid* dbIdP, Oid* ownerIdP, int* encodingP,
    bool* dbIsTemplateP, bool* dbAllowConnP, Oid* dbLastSysOidP, TransactionId* dbFrozenXidP, MultiXactId *dbMinMultiP,
    Oid* dbTablespace, char** dbCollate, char** dbCtype, char** dbcompatibility)
{
    bool result = false;
    Relation relation;

    AssertArg(name);

    /* Caller may wish to grab a better lock on pg_database beforehand... */
    relation = heap_open(DatabaseRelationId, AccessShareLock);

    /*
     * Loop covers the rare case where the database is renamed before we can
     * lock it.  We try again just in case we can find a new one of the same
     * name.
     */
    for (;;) {
        ScanKeyData scanKey;
        SysScanDesc scan;
        HeapTuple tuple;
        Oid dbOid;

        /*
         * there's no syscache for database-indexed-by-name, so must do it the
         * hard way
         */
        ScanKeyInit(&scanKey, Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(name));

        scan = systable_beginscan(relation, DatabaseNameIndexId, true, NULL, 1, &scanKey);

        tuple = systable_getnext(scan);

        if (!HeapTupleIsValid(tuple)) {
            /* definitely no database of that name */
            systable_endscan(scan);
            break;
        }

        dbOid = HeapTupleGetOid(tuple);

        systable_endscan(scan);

        /*
         * Now that we have a database OID, we can try to lock the DB.
         */
        if (lockmode != NoLock)
            LockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);

        /*
         * And now, re-fetch the tuple by OID.	If it's still there and still
         * the same name, we win; else, drop the lock and loop back to try
         * again.
         */
        tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbOid));
        if (HeapTupleIsValid(tuple)) {
            Form_pg_database dbform = (Form_pg_database)GETSTRUCT(tuple);

            if (strcmp(name, NameStr(dbform->datname)) == 0) {
                /* oid of the database */
                if (dbIdP != NULL)
                    *dbIdP = dbOid;
                /* oid of the owner */
                if (ownerIdP != NULL)
                    *ownerIdP = dbform->datdba;
                /* character encoding */
                if (encodingP != NULL)
                    *encodingP = dbform->encoding;
                /* allowed as template? */
                if (dbIsTemplateP != NULL)
                    *dbIsTemplateP = dbform->datistemplate;
                /* allowing connections? */
                if (dbAllowConnP != NULL)
                    *dbAllowConnP = dbform->datallowconn;
                /* last system OID used in database */
                if (dbLastSysOidP != NULL)
                    *dbLastSysOidP = dbform->datlastsysoid;
                /* limit of frozen XIDs */
                if (dbFrozenXidP != NULL) {
                    bool isNull = false;
                    TransactionId datfrozenxid;
                    Datum xid64datum =
                        heap_getattr(tuple, Anum_pg_database_datfrozenxid64, RelationGetDescr(relation), &isNull);

                    if (isNull) {
                        datfrozenxid = dbform->datfrozenxid;

                        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, datfrozenxid))
                            datfrozenxid = FirstNormalTransactionId;
                    } else {
                        datfrozenxid = DatumGetTransactionId(xid64datum);
                    }

                    *dbFrozenXidP = datfrozenxid;
                }
#ifndef ENABLE_MULTIPLE_NODES
                /* limit of frozen Multixacts */
                if (dbMinMultiP != NULL) {
                    bool isNull = false;
                    Datum minmxidDatum =
                        heap_getattr(tuple, Anum_pg_database_datminmxid, RelationGetDescr(relation), &isNull);
                    *dbMinMultiP = isNull ? FirstMultiXactId : DatumGetTransactionId(minmxidDatum);
                }
#endif
                /* default tablespace for this database */
                if (dbTablespace != NULL)
                    *dbTablespace = dbform->dattablespace;
                /* default locale settings for this database */
                if (dbCollate != NULL)
                    *dbCollate = pstrdup(NameStr(dbform->datcollate));
                if (dbCtype != NULL)
                    *dbCtype = pstrdup(NameStr(dbform->datctype));
                if (dbcompatibility != NULL)
                    *dbcompatibility = pstrdup(NameStr(dbform->datcompatibility));
                ReleaseSysCache(tuple);
                result = true;
                break;
            }
            /* can only get here if it was just renamed */
            ReleaseSysCache(tuple);
        }

        if (lockmode != NoLock)
            UnlockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);
    }

    heap_close(relation, AccessShareLock);

    return result;
}

/* Check if current user has createdb privileges */
bool have_createdb_privilege(void)
{
    bool result = false;
    HeapTuple utup;

    /* Superusers can always do everything */
    if (superuser())
        return true;

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
    if (HeapTupleIsValid(utup)) {
        result = ((Form_pg_authid)GETSTRUCT(utup))->rolcreatedb;
        ReleaseSysCache(utup);
    }
    return result;
}

/*
 * Remove tablespace directories
 *
 * We don't know what tablespaces db_id is using, so iterate through all
 * tablespaces removing <tablespace>/db_id
 */
static void remove_dbtablespaces(Oid db_id)
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    Snapshot snapshot;

    /*
     * As in createdb(), we'd better use an MVCC snapshot here, since this
     * scan can run for a long time.  Duplicate visits to tablespaces would be
     * harmless, but missing a tablespace could result in permanently leaked
     * files.
     *
     * XXX change this when a generic fix for SnapshotNow races is implemented
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());

    rel = heap_open(TableSpaceRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, snapshot, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Oid dsttablespace = HeapTupleGetOid(tuple);
        char* dstpath = NULL;
        struct stat st;

        /* Don't mess with the global tablespace */
        if (dsttablespace == GLOBALTABLESPACE_OID)
            continue;

        dstpath = GetDatabasePath(db_id, dsttablespace);

        if (lstat(dstpath, &st) < 0 || !S_ISDIR(st.st_mode)) {
            /* Assume we can ignore it */
            pfree_ext(dstpath);
            continue;
        }

        if (!rmtree(dstpath, true))
            ereport(
                WARNING, (errmsg("some useless files may be left behind in old database directory \"%s\"", dstpath)));

        /* Record the filesystem change in XLOG */
        {
            xl_dbase_drop_rec xlrec;

            xlrec.db_id = db_id;
            xlrec.tablespace_id = dsttablespace;

            XLogBeginInsert();
            XLogRegisterData((char*)&xlrec, sizeof(xl_dbase_drop_rec));

            (void)XLogInsert(RM_DBASE_ID, XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);
        }

        pfree_ext(dstpath);
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);
    UnregisterSnapshot(snapshot);
}

/*
 * Check for existing files that conflict with a proposed new DB OID;
 * return TRUE if there are any
 *
 * If there were a subdirectory in any tablespace matching the proposed new
 * OID, we'd get a create failure due to the duplicate name ... and then we'd
 * try to remove that already-existing subdirectory during the cleanup in
 * remove_dbtablespaces.  Nuking existing files seems like a bad idea, so
 * instead we make this extra check before settling on the OID of the new
 * database.  This exactly parallels what GetNewRelFileNode() does for table
 * relfilenode values.
 */
static bool check_db_file_conflict(Oid db_id)
{
    bool result = false;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    Snapshot snapshot;

    /*
     * As in createdb(), we'd better use an MVCC snapshot here; missing a
     * tablespace could result in falsely reporting the OID is unique, with
     * disastrous future consequences per the comment above.
     *
     * XXX change this when a generic fix for SnapshotNow races is implemented
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());

    rel = heap_open(TableSpaceRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, snapshot, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Oid dsttablespace = HeapTupleGetOid(tuple);
        char* dstpath = NULL;
        struct stat st;

        /* Don't mess with the global tablespace */
        if (dsttablespace == GLOBALTABLESPACE_OID)
            continue;

        dstpath = GetDatabasePath(db_id, dsttablespace);

        if (lstat(dstpath, &st) == 0) {
            /* Found a conflicting file (or directory, whatever) */
            pfree_ext(dstpath);
            result = true;
            break;
        }

        pfree_ext(dstpath);
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);
    UnregisterSnapshot(snapshot);

    return result;
}

/*
 * Issue a suitable errdetail message for a busy database
 */
int errdetail_busy_db(int notherbackends, int npreparedxacts)
{
    if (notherbackends > 0 && npreparedxacts > 0)
        /* We don't deal with singular versus plural here, since gettext
         * doesn't support multiple plurals in one string. */
        errdetail("There are %d other session(s) and %d prepared transaction(s) using the database.",
            notherbackends,
            npreparedxacts);
    else if (notherbackends > 0)
        errdetail_plural("There is %d other session using the database.",
            "There are %d other sessions using the database.",
            notherbackends,
            notherbackends);
    else
        errdetail_plural("There is %d prepared transaction using the database.",
            "There are %d prepared transactions using the database.",
            npreparedxacts,
            npreparedxacts);
    return 0; /* just to keep ereport macro happy */
}

/*
 * get_database_oid - given a database name, look up the OID
 *
 * If missing_ok is false, throw an error if database name not found.  If
 * true, just return InvalidOid.
 */
Oid get_database_oid(const char* dbname, bool missing_ok)
{
    Relation pg_database;
    ScanKeyData entry[1];
    SysScanDesc scan;
    HeapTuple dbtuple;
    Oid oid;

    /*
     * There's no syscache for pg_database indexed by name, so we must look
     * the hard way.
     */
    pg_database = heap_open(DatabaseRelationId, AccessShareLock);
    ScanKeyInit(&entry[0], Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(dbname));
    scan = systable_beginscan(pg_database, DatabaseNameIndexId, true, NULL, 1, entry);

    dbtuple = systable_getnext(scan);

    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(dbtuple))
        oid = HeapTupleGetOid(dbtuple);
    else
        oid = InvalidOid;

    systable_endscan(scan);
    heap_close(pg_database, AccessShareLock);

    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", dbname)));

    return oid;
}

/*
 * get_database_name - given a database OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such database.
 */
char* get_database_name(Oid dbid)
{
    HeapTuple dbtuple;
    char* result = NULL;

    dbtuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
    if (HeapTupleIsValid(dbtuple)) {
        result = pstrdup(NameStr(((Form_pg_database)GETSTRUCT(dbtuple))->datname));
        ReleaseSysCache(dbtuple);
    } else
        result = NULL;

    return result;
}

char* get_and_check_db_name(Oid dbid, bool is_ereport)
{
    char* dbname = NULL;

    dbname = get_database_name(dbid);
    if (dbname == NULL) {
        if (is_ereport) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                 errmsg("database with OID %u does not exist", dbid)));
        }
        dbname = pstrdup("invalid database");
    }

    return dbname;
}

/*
 * DATABASE resource manager's routines
 */
void xlog_db_create(Oid dstDbId, Oid dstTbSpcId, Oid srcDbId, Oid srcTbSpcId)
{
    char* src_path = NULL;
    char* dst_path = NULL;
    struct stat st;

    src_path = GetDatabasePath(srcDbId, srcTbSpcId);
    dst_path = GetDatabasePath(dstDbId, dstTbSpcId);

    /*
     * Our theory for replaying a CREATE is to forcibly drop the target
     * subdirectory if present, then re-copy the source data. This may be
     * more work than needed, but it is simple to implement.
     */
    if (stat(dst_path, &st) == 0 && S_ISDIR(st.st_mode) && !IsRoachRestore()) {
        if (!rmtree(dst_path, true))
            /* If this failed, copydir() below is going to error. */
            ereport(WARNING,
                (errmsg("some useless files may be left behind in old database directory \"%s\"", dst_path)));
    }

    /*
     * Force dirty buffers out to disk, to ensure source database is
     * up-to-date for the copy.
     */
    FlushDatabaseBuffers(srcDbId);

    /*
     * Copy this subdirectory to the new location
     *
     * We don't need to copy subdirectories
     */
    bool copyRes = false;
    copyRes = copydir(src_path, dst_path, false, WARNING);

    /*
     * In this scenario, src_path may be droped:
     *
     *  #1. CHECKPOINT;
     *  #2. XLOG create db1 template db2;
     *  #3. XLOG drop db2;
     *  #4. Redo killed;
     *  #5. start again and Redo from #1, when Redo #2, src_path do not exits because it is already drop in Redo #3;
     */
    if (!copyRes) {
        RelFileNode tmp = {srcTbSpcId, srcDbId, 0, InvalidBktId};

        /* forknum and blockno has no meaning */
        log_invalid_page(tmp, MAIN_FORKNUM, 0, NOT_PRESENT, NULL);
    }
}

void do_db_drop(Oid dbId, Oid tbSpcId)
{
    char* dst_path = GetDatabasePath(dbId, tbSpcId);

    if (InHotStandby) {
        /*
         * Lock database while we resolve conflicts to ensure that
         * InitPostgres() cannot fully re-execute concurrently. This
         * avoids backends re-connecting automatically to same database,
         * which can happen in some cases.
         */
        LockSharedObjectForSession(DatabaseRelationId, dbId, 0, AccessExclusiveLock);
        ResolveRecoveryConflictWithDatabase(dbId);
    }

    /* Drop pages for this database that are in the shared buffer cache */
    DropDatabaseBuffers(dbId);

    /* Also, clean out any fsync requests that might be pending in md.c */
    ForgetDatabaseSyncRequests(dbId);
    
    /* Clean out the xlog relcache too */
    XLogDropDatabase(dbId);

    /* And remove the physical files */
    if (!rmtree(dst_path, true)) {
        ereport(WARNING, (errmsg("some useless files may be left behind in old database directory \"%s\"", dst_path)));
    }
    
    if (InHotStandby) {
        /*
         * Release locks prior to commit. XXX There is a race condition
         * here that may allow backends to reconnect, but the window for
         * this is small because the gap between here and commit is mostly
         * fairly small and it is unlikely that people will be dropping
         * databases that we are trying to connect to anyway.
         */
        UnlockSharedObjectForSession(DatabaseRelationId, dbId, 0, AccessExclusiveLock);
    }
}

void xlogRemoveRemainSegsByDropDB(Oid dbId, Oid tablespaceId)
{
    Assert(dbId != InvalidOid || tablespaceId != InvalidOid);
    
    AutoMutexLock remainSegsLock(&g_instance.xlog_cxt.remain_segs_lock);
    remainSegsLock.lock();
    if (t_thrd.xlog_cxt.remain_segs == NULL) {
        t_thrd.xlog_cxt.remain_segs = redo_create_remain_segs_htbl();
    }

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, t_thrd.xlog_cxt.remain_segs);
    ExtentTag *extentTag = NULL;
    while ((extentTag = (ExtentTag *)hash_seq_search(&status)) != NULL) {
        if ((dbId != InvalidOid && extentTag->remainExtentHashTag.rnode.dbNode != dbId) || 
            (tablespaceId != InvalidOid && extentTag->remainExtentHashTag.rnode.spcNode != tablespaceId)) {
            continue;
        }

        if (hash_search(t_thrd.xlog_cxt.remain_segs,
                        (void *)&extentTag->remainExtentHashTag, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("hash table corrupted.")));
        }
    }

    remainSegsLock.unLock();
}

void xlog_db_drop(XLogRecPtr lsn, Oid dbId, Oid tbSpcId)
{
    UpdateMinRecoveryPoint(lsn, false);
    do_db_drop(dbId, tbSpcId);
    xlogRemoveRemainSegsByDropDB(dbId, tbSpcId);
}

void dbase_redo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    Assert(!XLogRecHasAnyBlockRefs(record));
    if (info == XLOG_DBASE_CREATE) {
        xl_dbase_create_rec* xlrec = (xl_dbase_create_rec*)XLogRecGetData(record);
        xlog_db_create(xlrec->db_id, xlrec->tablespace_id, xlrec->src_db_id, xlrec->src_tablespace_id);
    } else if (info == XLOG_DBASE_DROP) {
        xl_dbase_drop_rec* xlrec = (xl_dbase_drop_rec*)XLogRecGetData(record);
        xlog_db_drop(record->EndRecPtr, xlrec->db_id, xlrec->tablespace_id);
    } else
        ereport(PANIC, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("dbase_redo: unknown op code %hhu", info)));

    t_thrd.xlog_cxt.needImmediateCkp = true;
}

/*
 * Get remote nodes prepared xacts, if remote nodes have prepared xacts,
 * we cannot drop database. Otherwise, drop database finally failed in 
 * remote nodes and gs_clean cannot connect to database in CN to clean
 * the in-doubt transactions.
 * The memory palloc in PortalHeapMemory context, it will release after 
 * operation finished, it also can be released in abort transaction if operation
 * failed.
 */
int64 GetRemoteNodePreparedNumDB(const char* dbname)
{
    int64 size = 0;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT count(*) from pg_catalog.pg_prepared_xacts where database = '%s'", dbname);
    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum, true, EXEC_ON_ALL_NODES, true);
    size = state->result;
    FreeParallelFunctionState(state);
    return size;
}

/*
 * the function is just called in 3 cases currently:
 * 1. drop db, 2. rename db, 3. change tablespace of db
 * There are step 1 and 2 in pgxc, it cann't check gsql session to remote CNs;
 * There are step 1 and 4 in gaussdb before, it's appropriate just for 2 CNs,
 * Think about the case below:
 * cn1:                     cn2:               cn3:
 * create database db1
 * create table t1          \c db1
 *                          create table t2    \c db1
 *                                             create table t3
 * There are 2 connections between every 2 CNs, so it is not enough for more
 * than 2 CNs to run step 1 and 4 to clean connections in pooler on all CNs.
 */
void PreCleanAndCheckConns(const char* dbname, bool missing_ok)
{
    char query[256];
    int rc;
    Oid db_id;
    int notherbackends, npreparedxacts;
    int64 nremotepreparedxacts;

    /* 0. check if db exists? */
    db_id = get_database_oid(dbname, missing_ok);
    if (InvalidOid == db_id)
        return;  // just for drop database if exists dbname

    /* 1. clean connections on local pooler */
    DropDBCleanConnection((char*)dbname);

    /* 2. clean connections on pooler of remote CNs */
    rc = sprintf_s(query, sizeof(query), "CLEAN CONNECTION TO ALL FOR DATABASE %s;", quote_identifier(dbname));
    securec_check_ss(rc, "\0", "\0");
    ExecUtilityStmtOnNodes(query, NULL, false, true, EXEC_ON_COORDS, false);

    /* 3. check for other backends in the local CN */
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("Database \"%s\" is being accessed by other users. You can stop all connections by command:"
                    " \"clean connection to all force for database XXXX;\" or wait for the sessions to end by querying "
                    "view: \"pg_stat_activity\".", dbname),
                errdetail_busy_db(notherbackends, npreparedxacts)));

    /* 4. check for other backends in remote CNs */
    rc = sprintf_s(query, sizeof(query), "CLEAN CONNECTION TO ALL CHECK FOR DATABASE %s;", quote_identifier(dbname));
    securec_check_ss(rc, "\0", "\0");
    ExecUtilityStmtOnNodes(query, NULL, false, true, EXEC_ON_COORDS, false);

    /* 5. get and check remote node prepared xacts num */
    nremotepreparedxacts = GetRemoteNodePreparedNumDB(dbname);
    if (nremotepreparedxacts != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("Database \"%s\" is being accessed by other users. You can "
                "call gs_clean to clean prepared transactions", dbname),
                errdetail_busy_db(0, nremotepreparedxacts)));
    }
}

/*
 * AlterDatabasePrivateObject
 *    Alter database private object attribute
 *
 * @param (in) fbform: include all the information for database.
 * @param (in) dbid: database id
 * @param (in) enablePrivateObject: enable database private object or disable database private object
 * @return: void
 */
static void AlterDatabasePrivateObject(const Form_pg_database fbform, Oid dbid, bool enablePrivateObject)
{
    Assert(fbform != NULL);
    /* Check whether need to change db private object on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return;
    }
    /* Check license whether support this feature */
    LicenseSupportRls();

    if (dbid != u_sess->proc_cxt.MyDatabaseId) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support alter cross-database private pbject, please switch to \"%s\" and run this command",
                    NameStr(fbform->datname))));
    }

    Relation rel = NULL;
    Oid rlsPolicyId = InvalidOid;

    /* Create row level security policy on system catalog */
    if (enablePrivateObject) {
        /*
         * step 1: create row level security for pg_class if not exists
         *   CREATE ROW LEVEL SECURITY POLICY pg_class_rls ON pg_class AS PERMISSIVE FOR SELECT TO PUBLIC
         *     USING (has_table_privilege(current_user, oid, 'select'))
         */
        rlsPolicyId = get_rlspolicy_oid(RelationRelationId, "pg_class_rls", true);
        if (false == OidIsValid(rlsPolicyId)) {
            CreateRlsPolicyForSystem("pg_catalog", "pg_class", "pg_class_rls", "has_table_privilege", "oid", "select");
            rel = relation_open(RelationRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /*
         * step 2: create row level security for pg_attribute if not exists
         *   CREATE ROW LEVEL SECURITY POLICY pg_attribute_rls ON pg_attribute AS PERMISSIVE FOR SELECT TO PUBLIC
         *     USING (has_table_privilege(current_user, attrelid, 'select'))
         */
        rlsPolicyId = get_rlspolicy_oid(AttributeRelationId, "pg_attribute_rls", true);
        if (false == OidIsValid(rlsPolicyId)) {
            CreateRlsPolicyForSystem(
                "pg_catalog", "pg_attribute", "pg_attribute_rls", "has_table_privilege", "attrelid", "select");
            rel = relation_open(AttributeRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /*
         * step 3: create row level security for pg_proc if not exists
         *   CREATE ROW LEVEL SECURITY POLICY pg_proc_rls ON pg_proc AS PERMISSIVE FOR SELECT TO PUBLIC
         *     USING (has_function_privilege(current_user, oid, 'select'))
         */
        rlsPolicyId = get_rlspolicy_oid(ProcedureRelationId, "pg_proc_rls", true);
        if (false == OidIsValid(rlsPolicyId)) {
            CreateRlsPolicyForSystem(
                "pg_catalog", "pg_proc", "pg_proc_rls", "has_function_privilege", "oid", "execute");
            rel = relation_open(ProcedureRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /*
         * step 4: create row level security for pg_namespace if not exists
         *   CREATE ROW LEVEL SECURITY POLICY pg_namespace_rls ON pg_namespace AS PERMISSIVE FOR SELECT TO PUBLIC
         *     USING (has_schema_privilege(current_user, nspname, 'select'))
         */
        rlsPolicyId = get_rlspolicy_oid(NamespaceRelationId, "pg_namespace_rls", true);
        if (false == OidIsValid(rlsPolicyId)) {
            CreateRlsPolicyForSystem(
                "pg_catalog", "pg_namespace", "pg_namespace_rls", "has_schema_privilege", "oid", "USAGE");
            rel = relation_open(NamespaceRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /*
         * step 5: create row level security for pgxc_slice if not exists
         *   CREATE ROW LEVEL SECURITY POLICY pgxc_slice_rls ON pgxc_slice AS PERMISSIVE FOR SELECT TO PUBLIC
         *     USING (has_schema_privilege(current_user, nspname, 'select'))
         */
        if (t_thrd.proc->workingVersionNum >= RANGE_LIST_DISTRIBUTION_VERSION_NUM) {
            rlsPolicyId = get_rlspolicy_oid(PgxcSliceRelationId, "pgxc_slice_rls", true);
            if (OidIsValid(rlsPolicyId) == false) {
                CreateRlsPolicyForSystem(
                    "pg_catalog", "pgxc_slice", "pgxc_slice_rls", "has_table_privilege", "relid", "select");
                rel = relation_open(PgxcSliceRelationId, ShareUpdateExclusiveLock);
                ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
                relation_close(rel, ShareUpdateExclusiveLock);
            }
        }
        /*
         * step 6: create row level security for pg_partition if not exists
         *   CREATE ROW LEVEL SECURITY POLICY pg_partition_rls ON pg_partition AS PERMISSIVE FOR SELECT TO PUBLIC
         *     USING (has_schema_privilege(current_user, nspname, 'select'))
         */
        rlsPolicyId = get_rlspolicy_oid(PartitionRelationId, "pg_partition_rls", true);
        if (OidIsValid(rlsPolicyId) == false) {
            CreateRlsPolicyForSystem(
                "pg_catalog", "pg_partition", "pg_partition_rls", "has_table_privilege", "parentid", "select");
            rel = relation_open(PartitionRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
    } else {
        /* Remove row level security policy for system catalog */
        /* step 1: remove row level security policy from pg_class */
        rlsPolicyId = get_rlspolicy_oid(RelationRelationId, "pg_class_rls", true);
        if (OidIsValid(rlsPolicyId)) {
            RemoveRlsPolicyById(rlsPolicyId);
            rel = relation_open(RelationRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_DISABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /* step 2: remove row level security policy from pg_attribute */
        rlsPolicyId = get_rlspolicy_oid(AttributeRelationId, "pg_attribute_rls", true);
        if (OidIsValid(rlsPolicyId)) {
            RemoveRlsPolicyById(rlsPolicyId);
            rel = relation_open(AttributeRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_DISABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /* step 3: remove row level security policy from pg_proc */
        rlsPolicyId = get_rlspolicy_oid(ProcedureRelationId, "pg_proc_rls", true);
        if (OidIsValid(rlsPolicyId)) {
            RemoveRlsPolicyById(rlsPolicyId);
            rel = relation_open(ProcedureRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_DISABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /* step 4: remove row level security policy from pg_namespace */
        rlsPolicyId = get_rlspolicy_oid(NamespaceRelationId, "pg_namespace_rls", true);
        if (OidIsValid(rlsPolicyId)) {
            RemoveRlsPolicyById(rlsPolicyId);
            rel = relation_open(NamespaceRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_DISABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
        /* step 5: remove row level security policy from pgxc_slice */
        if (t_thrd.proc->workingVersionNum >= RANGE_LIST_DISTRIBUTION_VERSION_NUM) {
            rlsPolicyId = get_rlspolicy_oid(PgxcSliceRelationId, "pgxc_slice_rls", true);
            if (OidIsValid(rlsPolicyId)) {
                RemoveRlsPolicyById(rlsPolicyId);
                rel = relation_open(PgxcSliceRelationId, ShareUpdateExclusiveLock);
                ATExecEnableDisableRls(rel, RELATION_RLS_DISABLE, ShareUpdateExclusiveLock);
                relation_close(rel, ShareUpdateExclusiveLock);
            }
        }
        /* step 6: remove row level security policy from pg_partition */
        rlsPolicyId = get_rlspolicy_oid(PartitionRelationId, "pg_partition_rls", true);
        if (OidIsValid(rlsPolicyId)) {
            RemoveRlsPolicyById(rlsPolicyId);
            rel = relation_open(PartitionRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_DISABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
    }
}
