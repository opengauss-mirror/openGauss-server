/* -------------------------------------------------------------------------
 *
 * vacuum.cpp
 *	  The postgres vacuum cleaner.
 *
 * This file now includes only control and dispatch code for VACUUM and
 * ANALYZE commands.  Regular VACUUM is implemented in vacuumlazy.c,
 * ANALYZE in analyze.c, and VACUUM FULL is a variant of CLUSTER, handled
 * in cluster.c.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/vacuum.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h" /* stay here, otherwise compile errors */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "access/clog.h"
#include "access/dfs/dfs_insert.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/namespace.h"
#include "catalog/gs_matview.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pgxc_class.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "commands/cluster.h"
#include "commands/dbcommands.h"
#include "commands/matview.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "executor/node/nodeModifyTable.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rbtree.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/commands_gstrace.h"

#ifdef ENABLE_MOT
#include "foreign/fdwapi.h"
#endif
#include "access/ustore/knl_uheap.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/constant_def.h"
#endif

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "catalog/catalog.h"
#endif

const int ROW_COUNT_SQL_TEMPLATE = 0;
const int MERGE_SQL_TEMPLATE = 1;
const int VACUUM_SQL_TEMPLATE = 2;
const int VACUUM_SQL_TEMPLATE_WITH_SCHEMA = 3;

const char* sql_templates[] = {
    "SELECT count(*) FROM cstore.\"%s\"",

    "START TRANSACTION;"
    "LOCK TABLE \"%s\" IN ACCESS EXCLUSIVE MODE;"
    "SET LOCAL CSTORE_INSERT_MODE=MAIN;"
    "INSERT INTO \"%s\" SELECT * FROM cstore.\"%s\";"
    "TRUNCATE cstore.\"%s\";"
    "COMMIT;", /* no schema */

    "VACUUM cstore.\"%s\"",

    "START TRANSACTION;"
    "LOCK TABLE \"%s\".\"%s\" IN ACCESS EXCLUSIVE MODE;"
    "SET LOCAL CSTORE_INSERT_MODE=MAIN;"
    "INSERT INTO \"%s\".\"%s\" SELECT * FROM cstore.\"%s\";"
    "TRUNCATE cstore.\"%s\";"
    "COMMIT;", /* with schema */
};

typedef struct {
    OidRBTree* invisiblePartOids; /* cache invisible tuple's partOid in global partition index */
    OidRBTree* visiblePartOids;   /* cache visible tuple's partOid in global partition index */
    oidvector* bucketList;
} VacStates;

extern void exec_query_for_merge(const char* query_string);

extern void do_delta_merge(List* infos, VacuumStmt* stmt);

/* release all memory in infos */
static void free_merge_info(List* infos);
static void DropEmptyPartitionDirectories(Oid relid);

/* A few variables that don't seem worth passing around as parameters */
static THR_LOCAL BufferAccessStrategy vac_strategy;
static THR_LOCAL int elevel = -1;

static void UstoreVacuumMainPartitionGPIs(Relation onerel, const VacuumStmt* vacstmt,
    LOCKMODE lockmode, BufferAccessStrategy bstrategy);

static void vac_truncate_clog(TransactionId frozenXID, MultiXactId frozenMulti);
static bool vacuum_rel(Oid relid, VacuumStmt* vacstmt, bool do_toast);
static void GPIVacuumMainPartition(
    Relation onerel, const VacuumStmt* vacstmt, LOCKMODE lockmode, BufferAccessStrategy bstrategy);
static void CBIVacuumMainPartition(
    Relation onerel, const VacuumStmt* vacstmt, LOCKMODE lockmode, BufferAccessStrategy bstrategy);

#define TryOpenCStoreInternalRelation(r, lmode, r1, r2)                   \
    do {                                                                  \
        if (RelationGetCUDescRelId(r) != InvalidOid)                      \
            (r1) = try_relation_open(RelationGetCUDescRelId(r), (lmode)); \
        if (RelationGetDeltaRelId(r) != InvalidOid)                       \
            (r2) = try_relation_open(RelationGetDeltaRelId(r), (lmode));  \
    } while (0)

/// Coordinator needs guarantee the old select statement must finish when
/// VACUUM FULL table runs.
void CNGuardOldQueryForVacuumFull(VacuumStmt* vacstmt, Oid relid)
{
    if (IS_PGXC_COORDINATOR && ((uint32)(vacstmt->options) & VACOPT_FULL)) {
        /// Acquiring this lock will be blocked by any other transaction,
        /// even it's just a SELECT query. Until that transaction finishes,
        /// lock can be gained.
        Relation rel = try_relation_open(relid, AccessExclusiveLock);

        // because this relation maybe dropped by the other transaction,
        // so *rel* may be NULL. be careful this case!
        if (rel != NULL)
            relation_close(rel, AccessExclusiveLock);
    }
}

/*
 * Primary entry point for VACUUM and ANALYZE commands.
 *
 * relid is normally InvalidOid; if it is not, then it provides the relation
 * OID to be processed, and vacstmt->relation is ignored.  (The non-invalid
 * case is currently only used by autovacuum.)
 *
 * do_toast is passed as FALSE by autovacuum, because it processes TOAST
 * tables separately.
 *
 * bstrategy is normally given as NULL, but in autovacuum it can be passed
 * in to use the same buffer strategy object across multiple vacuum() calls.
 *
 * isTopLevel should be passed down from ProcessUtility.
 *
 * It is the caller's responsibility that vacstmt and bstrategy
 * (if given) be allocated in a memory context that won't disappear
 * at transaction commit.
 */
void vacuum(
    VacuumStmt* vacstmt, Oid relid, bool do_toast, BufferAccessStrategy bstrategy, bool isTopLevel)
{
    ereport(ES_LOGLEVEL, (errmsg("[Vacuum] > CN?[%d], [%d]", IS_PGXC_COORDINATOR, u_sess->pgxc_cxt.PGXCNodeId)));

    const char* stmttype = NULL;
    volatile bool in_outer_xact = false;
    volatile bool use_own_xacts = false;
    List* relations = NIL;

    /* sanity checks on options */
    Assert(vacstmt->options & (VACOPT_VACUUM | VACOPT_ANALYZE));
    Assert((vacstmt->options & VACOPT_VACUUM) || !(vacstmt->options & (VACOPT_FULL | VACOPT_FREEZE)));
    Assert((vacstmt->options & VACOPT_ANALYZE) || vacstmt->va_cols == NIL);

    stmttype = (vacstmt->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";

    if (u_sess->attr.attr_storage.enable_show_any_tuples)
        ereport(ERROR,
            (errcode(ERRCODE_QUERY_CANCELED),
                errmsg(
                    "%s cannot be executed when u_sess->attr.attr_storage.enable_show_any_tuples is true.", stmttype)));

    pgstat_set_io_state(IOSTATE_VACUUM);

    /*
     * We cannot run VACUUM inside a user transaction block; if we were inside
     * a transaction, then our commit- and start-transaction-command calls
     * would not have the intended effect!	There are numerous other subtle
     * dependencies on this, too.
     *
     * ANALYZE (without VACUUM) can run either way.
     */
    if (vacstmt->options & VACOPT_VACUUM) {
        PreventTransactionChain(isTopLevel, stmttype);
        in_outer_xact = false;
    } else
        in_outer_xact = IsInTransactionChain(isTopLevel);

    /*
     * Due to static variables vac_context, analyze_context and vac_strategy,
     * vacuum() is not reentrant.  This matters when VACUUM FULL or ANALYZE
     * calls a hostile index expression that itself calls ANALYZE.
     */
    if (t_thrd.vacuum_cxt.in_vacuum)
        ereport(
            ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("%s cannot be executed from VACUUM or ANALYZE", stmttype)));

    /*
     * Send info about dead objects to the statistics collector, unless we are
     * in autovacuum --- autovacuum.c does this for itself.
     */
    if ((vacstmt->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess())
        pgstat_vacuum_stat();

    /*
     * Create special memory context for cross-transaction storage.
     *
     * Since it is a child of t_thrd.mem_cxt.portal_mem_cxt, it will go away eventually even
     * if we suffer an error; there's no need for special abort cleanup logic.
     */
    t_thrd.vacuum_cxt.vac_context = AllocSetContextCreate(t_thrd.mem_cxt.portal_mem_cxt,
        "Vacuum",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * If caller didn't give us a buffer strategy object, make one in the
     * cross-transaction memory context.
     */
    if (bstrategy == NULL) {
        MemoryContext old_context = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);

        bstrategy = GetAccessStrategy(BAS_VACUUM);
        (void)MemoryContextSwitchTo(old_context);
    }
    vac_strategy = bstrategy;

    /*
     * Build list of relations to process, unless caller gave us one. (If we
     * build one, we put it in vac_context for safekeeping.)
     */
    relations = get_rel_oids(relid, vacstmt);

    /*
     * Decide whether we need to start/commit our own transactions.
     *
     * For VACUUM (with or without ANALYZE): always do so, so that we can
     * release locks as soon as possible.  (We could possibly use the outer
     * transaction for a one-table VACUUM, but handling TOAST tables would be
     * problematic.)
     *
     * For ANALYZE (no VACUUM): if inside a transaction block, we cannot
     * start/commit our own transactions.  Also, there's no need to do so if
     * only processing one relation.  For multiple relations when not within a
     * transaction block, and also in an autovacuum worker, use own
     * transactions so we can release locks sooner.
     */
    if (vacstmt->options & VACOPT_VACUUM) {
        use_own_xacts = true;
    } else {
        Assert(vacstmt->options & VACOPT_ANALYZE);
        if (IsAutoVacuumWorkerProcess()) {
            use_own_xacts = true;
        } else if (in_outer_xact) {
            use_own_xacts = false;
        } else if (list_length(relations) > 1) {
            use_own_xacts = true;
        } else {
            use_own_xacts = false;
        }
    }

    /*
     * vacuum_rel expects to be entered with no transaction active; it will
     * start and commit its own transaction.  But we are called by an SQL
     * command, and so we are executing inside a transaction already. We
     * commit the transaction started in PostgresMain() here, and start
     * another one before exiting to match the commit waiting for us back in
     * PostgresMain().
     */
    if (use_own_xacts) {
        /* ActiveSnapshot is not set by autovacuum */
        if (ActiveSnapshotSet())
            PopActiveSnapshot();

        /* matches the StartTransaction in PostgresMain() */
        CommitTransactionCommand();
    }

    /* Turn vacuum cost accounting on or off */
    PG_TRY();
    {
        ListCell* cur = NULL;

        t_thrd.vacuum_cxt.in_vacuum = true;
        t_thrd.vacuum_cxt.VacuumCostActive = (u_sess->attr.attr_storage.VacuumCostDelay > 0);
        t_thrd.vacuum_cxt.VacuumCostBalance = 0;
        t_thrd.vacuum_cxt.VacuumPageHit = 0;
        t_thrd.vacuum_cxt.VacuumPageMiss = 0;
        t_thrd.vacuum_cxt.VacuumPageDirty = 0;

        /*
         * Loop to process each selected relation.
         */
        WaitState oldStatus = pgstat_report_waitstatus(STATE_VACUUM);
        foreach (cur, relations) {
            vacuum_object* vacObj = (vacuum_object*)lfirst(cur);
            Oid relOid = vacObj->tab_oid;
            vacstmt->flags = vacObj->flags;
            vacstmt->onepartrel = NULL;
            vacstmt->onepart = NULL;
            vacstmt->partList = NIL;
            vacstmt->parentpartrel = NULL;
            vacstmt->parentpart = NULL;
            vacstmt->issubpartition = false;

            /*
             * do NOT vacuum partitioned table,
             * as vacuum is an operation related with tuple and storage page reorganization
             */
            if (vacstmt->options & VACOPT_VACUUM) {
                if (vacuumPartition(vacstmt->flags) || vacuumRelation(vacstmt->flags) ||
                    vacuumMainPartition(vacstmt->flags)) {
                    if (!vacuum_rel(relOid, vacstmt, do_toast))
                        continue;
                } else {
                    /* for partitioned table, just report collector that we just vacuumed. */
                    pgstat_report_vacuum(relOid, InvalidOid, false, 0);
                }
            }

            vacstmt->flags = vacObj->flags;
            vacstmt->onepartrel = NULL;
            vacstmt->onepart = NULL;
            vacstmt->parentpartrel = NULL;
            vacstmt->parentpart = NULL;
            vacstmt->issubpartition = false;

            if (vacstmt->options & VACOPT_ANALYZE) {
                /*
                 * we have received user-defined table's stat info from remote coordinator
                 * in function FetchGlobalRelationStatistics, so we skip analyze
                 */
                if (udtRemoteAnalyze(relOid))
                    continue;

                /*
                 * If using separate xacts, start one for analyze. Otherwise,
                 * we can use the outer transaction.
                 */
                if (use_own_xacts) {
                    StartTransactionCommand();
                    /* functions in indexes may want a snapshot set */
                    PushActiveSnapshot(GetTransactionSnapshot());
                    LockSharedObject(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, RowExclusiveLock);
                }

                /*
                 * do NOT analyze partition, as analyze is an operation related with
                 * data redistribution reflect and this is not meaningfull for one
                 * or more partitions, it must be done on basis of table level, either
                 * plain heap or partitioned heap.
                 */
                if (!vacuumPartition(vacstmt->flags)) {
                    pgstat_report_waitstatus_relname(STATE_ANALYZE, get_nsp_relname(relOid));
                    analyze_rel(relOid, vacstmt, vac_strategy);
                }

                if (use_own_xacts) {
                    PopActiveSnapshot();
                    CommitTransactionCommand();
                }
            }
        }
        (void)pgstat_report_waitstatus(oldStatus);
        list_free_deep(relations);
    }
    PG_CATCH();
    {
        t_thrd.vacuum_cxt.in_vacuum = false;
        /* Make sure cost accounting is turned off after error */
        list_free_deep(relations);
        t_thrd.vacuum_cxt.VacuumCostActive = false;
        PG_RE_THROW();
    }
    PG_END_TRY();

    t_thrd.vacuum_cxt.in_vacuum = false;
    /*
     * Reset query cancel signal here to prevent hange 
     * when multiple vacuum triggered (e.g. toast)
     */
    if (t_thrd.int_cxt.QueryCancelPending) {
        t_thrd.int_cxt.QueryCancelPending = false;
    }
    /* Turn off vacuum cost accounting */
    t_thrd.vacuum_cxt.VacuumCostActive = false;

    /*
     * Finish up processing.
     */
    if (use_own_xacts) {
        /* here, we are not in a transaction
         *
         * This matches the CommitTransaction waiting for us in
         * PostgresMain().
         */
        StartTransactionCommand();
        LockSharedObject(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, RowExclusiveLock);
    }

    if (((uint32)(vacstmt->options) & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess()) {
        /*
         * Update pg_database.datfrozenxid, and truncate pg_clog if possible.
         * (autovacuum.c does this for itself.)
         */
        vac_update_datfrozenxid();
    }

    /*
     * Clean up working storage --- note we must do this after
     * StartTransactionCommand, else we might be trying to delete the active
     * context!
     */
    if (t_thrd.vacuum_cxt.vac_context) {
        MemoryContextDelete(t_thrd.vacuum_cxt.vac_context);
        t_thrd.vacuum_cxt.vac_context = NULL;
    }
}

bool CheckRelOrientationByPgClassTuple(HeapTuple tuple, TupleDesc tupdesc, const char* orientation)
{
    bool ret = false;
    bytea* options = NULL;

    options = extractRelOptions(tuple, tupdesc, InvalidOid);
    if (options != NULL) {
        const char* format = ((options) && (((StdRdOptions*)(options))->orientation))
            ? ((char*)(options) + *(int*)&(((StdRdOptions*)(options))->orientation))
            : ORIENTATION_ROW;
        if (pg_strcasecmp(format, orientation) == 0)
            ret = true;

        pfree_ext(options);
    }

    return ret;
}

/*
 * Generate schema.relname string for a relation specified OID when vacuum or analyze.
 */
char* get_nsp_relname(Oid relid)
{
    char* nsp_relname = NULL;
    char* relname = get_rel_name(relid);

    if (relname != NULL) {
        nsp_relname = (char*)palloc(NAMEDATALEN * 2);
        errno_t rc = snprintf_s(nsp_relname,
            NAMEDATALEN * 2,
            NAMEDATALEN * 2 - 1,
            "%s.%s",
            get_namespace_name(get_rel_namespace(relid)),
            relname);
        securec_check_ss(rc, "\0", "\0");

        pfree(relname);
        relname = NULL;
    }

    return nsp_relname;
}

static List *GetVacuumObjectOfSubpartitionTable(const Oid relId)
{
    Relation pgpartition;
    TableScanDesc partScan;
    HeapTuple partTuple;
    ScanKeyData keys[2];
    List *result = NULL;
    MemoryContext oldcontext = NULL;
    vacuum_object *vacObj = NULL;

    /* Process all plain partitions listed in pg_partition */
    ScanKeyInit(&keys[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
                CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

    ScanKeyInit(&keys[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relId));

    pgpartition = heap_open(PartitionRelationId, AccessShareLock);
    partScan = tableam_scan_begin(pgpartition, SnapshotNow, 2, keys);
    while (NULL != (partTuple = (HeapTuple)tableam_scan_getnexttuple(partScan, ForwardScanDirection))) {
        TableScanDesc subPartScan;
        HeapTuple subPartTuple;
        ScanKeyData subPartKeys[2];

        /* Process all plain subpartitions listed in pg_partition */
        ScanKeyInit(&subPartKeys[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
                    CharGetDatum(PART_OBJ_TYPE_TABLE_SUB_PARTITION));

        ScanKeyInit(&subPartKeys[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(HeapTupleGetOid(partTuple)));

        subPartScan = tableam_scan_begin(pgpartition, SnapshotNow, 2, subPartKeys);
        while (NULL != (subPartTuple = (HeapTuple)tableam_scan_getnexttuple(subPartScan, ForwardScanDirection))) {
            if (t_thrd.vacuum_cxt.vac_context) {
                oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
            }
            vacObj = (vacuum_object *)palloc0(sizeof(vacuum_object));
            vacObj->tab_oid = HeapTupleGetOid(subPartTuple);
            vacObj->parent_oid = HeapTupleGetOid(partTuple);
            vacObj->flags = VACFLG_SUB_PARTITION;
            result = lappend(result, vacObj);

            if (t_thrd.vacuum_cxt.vac_context) {
                (void)MemoryContextSwitchTo(oldcontext);
            }
        }
        heap_endscan(subPartScan);
    }

    heap_endscan(partScan);
    heap_close(pgpartition, AccessShareLock);

    /*
     * add partitioned table to list
     */
    if (t_thrd.vacuum_cxt.vac_context) {
        oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
    }
    vacObj = (vacuum_object *)palloc0(sizeof(vacuum_object));
    vacObj->tab_oid = relId;
    vacObj->parent_oid = InvalidOid;
    vacObj->flags = VACFLG_MAIN_PARTITION;
    result = lappend(result, vacObj);

    if (t_thrd.vacuum_cxt.vac_context) {
        (void)MemoryContextSwitchTo(oldcontext);
    }

    return result;
}

static vacuum_object *GetVacuumObjectOfSubpartition(VacuumStmt* vacstmt, Oid relationid)
{
    Assert(PointerIsValid(vacstmt->relation->subpartitionname));

    /*
    * for dfs table, there is no partition table now so just return
    * for dfs special vacuum.
    */
    if (hdfsVcuumAction(vacstmt->options)) {
        return NULL;
    }

    Oid partitionid = InvalidOid;
    Oid subpartitionid = InvalidOid;
    Form_pg_partition subpartitionForm;
    HeapTuple subpartitionTup;
    MemoryContext oldcontext = NULL;
    vacuum_object* vacObj = NULL;

    subpartitionid = partitionNameGetPartitionOid(relationid,
        vacstmt->relation->subpartitionname,
        PART_OBJ_TYPE_TABLE_SUB_PARTITION,
        AccessShareLock,
        true,
        false,
        NULL,
        NULL,
        NoLock,
        &partitionid);
    if (!OidIsValid(subpartitionid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("subpartition \"%s\" of relation \"%s\" does not exist",
                    vacstmt->relation->subpartitionname,
                    vacstmt->relation->relname)));
    }

    subpartitionTup = SearchSysCache1WithLogLevel(PARTRELID, ObjectIdGetDatum(subpartitionid), LOG);
    if (!HeapTupleIsValid(subpartitionTup)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for subpartition %u", subpartitionid)));
    }
    subpartitionForm = (Form_pg_partition)GETSTRUCT(subpartitionTup);
    Assert(subpartitionForm->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION);

    if (t_thrd.vacuum_cxt.vac_context) {
        oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
    }

    vacObj = (vacuum_object *)palloc0(sizeof(vacuum_object));
    vacObj->tab_oid = subpartitionid;
    vacObj->parent_oid = partitionid;
    vacObj->flags = VACFLG_SUB_PARTITION;

    if (t_thrd.vacuum_cxt.vac_context) {
        (void)MemoryContextSwitchTo(oldcontext);
    }

    ReleaseSysCache(subpartitionTup);
    return vacObj;
}

/*
 * Build a list of Oids for each relation to be processed
 *
 * The list is built in vac_context so that it will survive across our
 * per-relation transactions.
 */
List* get_rel_oids(Oid relid, VacuumStmt* vacstmt)
{
    List* oid_list = NIL;
    vacuum_object* vacObj = NULL;
    MemoryContext oldcontext = NULL;
    TupleDesc pgclassdesc = GetDefaultPgClassDesc();

    /*
     * OID supplied by VACUUM's caller?
     * Matview need to be processed, it can not be FULL vacuum because ctid
     * is used by refresh.
     * 1. if the relid is valid, this function is called by autovacuum, it
     *    is not a FULL vacuum.
     * 2. if the stmt->relation is set, it is be called by 'VACUUM rel', user
     *    can set the FULL options, we must check it and make an error report.
     * 3. if there are no relations are setted, it will scan all relations to
     *    vacuum, we simply skip the matview when FULL option is setted.
     */
    if (OidIsValid(relid)) {
        /* check for safety */
        if (is_incremental_matview(relid) && (vacstmt->options & VACOPT_FULL)) {
            ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("relation with OID %u cannot do the FULL vacuum", relid)));
        }

        if (OidIsValid(find_matview_mlog_table(relid)) && (vacstmt->options & VACOPT_FULL)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("relation with OID %u has one or more incremental materialized view associated, and cannot be FULL vacuumed",
                                   relid)));
        }

        if (t_thrd.vacuum_cxt.vac_context) {
            oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
        }
        vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
        vacObj->tab_oid = relid;
        vacObj->flags = vacstmt->flags;
        oid_list = lappend(oid_list, vacObj);

        if (!IsAutoVacuumWorkerProcess()) {
            TrForbidAccessRbObject(RelationRelationId, relid);
        }

        if (t_thrd.vacuum_cxt.vac_context) {
            (void)MemoryContextSwitchTo(oldcontext);
        }
    } else if (vacstmt->relation) {
        /* Process a specific relation */
        Oid relationid;
        Oid partitionid;

        Form_pg_partition partitionForm;
        HeapTuple partitionTup;
        Form_pg_class classForm;
        HeapTuple classTup;

        /*
         * Since we don't take a lock here, the relation might be gone, or the
         * RangeVar might no longer refer to the OID we look up here.  In the
         * former case, VACUUM will do nothing; in the latter case, it will
         * process the OID we looked up here, rather than the new one.
         * Neither is ideal, but there's little practical alternative, since
         * we're going to commit this transaction and begin a new one between
         * now and then.
         */
        relationid = RangeVarGetRelidExtended(vacstmt->relation, NoLock, false, false, false, true, NULL, NULL);

        if (is_incremental_matview(relationid) && (vacstmt->options & VACOPT_FULL)) {
            ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("relation with OID %u cannot do the FULL vacuum", relationid)));
        }

        /*
         * We can't do full vacuum on a table if it's associated with some incremental materialized views.
         *
         * The reason is, we keep the association of tuple tids between base tables and mvs, via matviewmaps,
         * and the tid of base table may be changed after a full vacuum, which can cause data inconsistency.
         *
         * For example, if we do the following operations on the base table and mv:
         *      Insert tid1, Insert tid2, Insert tid3, Refresh mv, Delete tid1, Vacuum full, Delete tid1, Refresh mv
         *
         * the second 'Delete tid1' will remove one tuple(originally tid2 before vacuum) from the base table, but will
         * do nothing on mv. On the other hand, some tuples will exist in mv forever after this operation sequence.
         */
        if (OidIsValid(find_matview_mlog_table(relationid)) && (vacstmt->options & VACOPT_FULL)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("relation with OID %u has one or more incremental materialized view associated, and cannot be FULL vacuumed",
                                   relationid)));
        }
        if (!IsAutoVacuumWorkerProcess()) {
            TrForbidAccessRbObject(RelationRelationId, relationid, vacstmt->relation->relname);
        }

        /* 1.a partition */
        if (PointerIsValid(vacstmt->relation->partitionname)) {
            /*
             * for dfs table, there is no partition table now so just return
             * for dfs special vacuum.
             */
            if (hdfsVcuumAction(vacstmt->options)) {
                return NIL;
            }

            partitionid = partitionNameGetPartitionOid(relationid,
                vacstmt->relation->partitionname,
                PART_OBJ_TYPE_TABLE_PARTITION,
                AccessShareLock,
                true,
                false,
                NULL,
                NULL,
                NoLock);
            if (!OidIsValid(partitionid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("partition \"%s\" of relation \"%s\" does not exist",
                            vacstmt->relation->partitionname,
                            vacstmt->relation->relname)));
            }

            partitionTup = SearchSysCache1WithLogLevel(PARTRELID, ObjectIdGetDatum(partitionid), LOG);

            if (!HeapTupleIsValid(partitionTup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for partition %u", partitionid)));
            }

            partitionForm = (Form_pg_partition)GETSTRUCT(partitionTup);

            if (partitionForm->parttype == PART_OBJ_TYPE_TABLE_PARTITION) {
                if (partitionForm->relfilenode != InvalidOid) {
                    /* it is a partition of partition table. */
                    if (t_thrd.vacuum_cxt.vac_context) {
                        oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
                    }
                    vacObj = (vacuum_object *)palloc0(sizeof(vacuum_object));
                    vacObj->tab_oid = partitionid;
                    vacObj->parent_oid = relationid;
                    vacObj->flags = VACFLG_SUB_PARTITION;
                    oid_list = lappend(oid_list, vacObj);

                    if (t_thrd.vacuum_cxt.vac_context) {
                        (void)MemoryContextSwitchTo(oldcontext);
                    }
                } else {
                    /* it is a partition of subpartition table. */
                    Relation pgpartition;
                    TableScanDesc subPartScan;
                    HeapTuple subPartTuple;
                    ScanKeyData subPartKeys[2];

                    /* Process all plain subpartitions listed in pg_partition */
                    ScanKeyInit(&subPartKeys[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
                                CharGetDatum(PART_OBJ_TYPE_TABLE_SUB_PARTITION));

                    ScanKeyInit(&subPartKeys[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ,
                                ObjectIdGetDatum(HeapTupleGetOid(partitionTup)));
                    pgpartition = heap_open(PartitionRelationId, AccessShareLock);
                    subPartScan = tableam_scan_begin(pgpartition, SnapshotNow, 2, subPartKeys);
                    while (NULL !=
                           (subPartTuple = (HeapTuple)tableam_scan_getnexttuple(subPartScan, ForwardScanDirection))) {
                        if (t_thrd.vacuum_cxt.vac_context) {
                            oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
                        }
                        vacObj = (vacuum_object *)palloc0(sizeof(vacuum_object));
                        vacObj->tab_oid = HeapTupleGetOid(subPartTuple);
                        vacObj->parent_oid = HeapTupleGetOid(partitionTup);
                        vacObj->flags = VACFLG_SUB_PARTITION;
                        oid_list = lappend(oid_list, vacObj);

                        if (t_thrd.vacuum_cxt.vac_context) {
                            (void)MemoryContextSwitchTo(oldcontext);
                        }
                    }
                    heap_endscan(subPartScan);
                    heap_close(pgpartition, AccessShareLock);
                }
            }

            ReleaseSysCache(partitionTup);
        } else if (PointerIsValid(vacstmt->relation->subpartitionname)) {
            /* 2. a subpartition */
            vacObj = GetVacuumObjectOfSubpartition(vacstmt, relationid);
            if (PointerIsValid(vacObj)) {
                oid_list = lappend(oid_list, vacObj);
            }
        } else {
            /* 3.a relation */
            classTup = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(relationid), LOG);

            if (HeapTupleIsValid(classTup)) {
                /* for dfs special vacuum, just skip for non-dfs table. */
                if ((((vacstmt->options & VACOPT_HDFSDIRECTORY) || (vacstmt->options & VACOPT_COMPACT)) &&
                        !CheckRelOrientationByPgClassTuple(classTup, pgclassdesc, ORIENTATION_ORC)) ||
                    ((vacstmt->options & VACOPT_MERGE) &&
                        !(CheckRelOrientationByPgClassTuple(classTup, pgclassdesc, ORIENTATION_COLUMN) ||
                        CheckRelOrientationByPgClassTuple(classTup, pgclassdesc, ORIENTATION_ORC) ||
                        CheckRelOrientationByPgClassTuple(classTup, pgclassdesc, ORIENTATION_TIMESERIES)))) {
                    ReleaseSysCache(classTup);

                    return NIL;
                }

                classForm = (Form_pg_class)GETSTRUCT(classTup);

                /* Partitioned table, get all the partitions */
                if (classForm->parttype == PARTTYPE_PARTITIONED_RELATION && classForm->relkind == RELKIND_RELATION &&
                    ((vacstmt->options & VACOPT_FULL) || (vacstmt->options & VACOPT_VACUUM))) {
                    Relation pgpartition;
                    TableScanDesc scan;
                    HeapTuple tuple;
                    ScanKeyData keys[2];

                    /* Process all partitions of this partitiond table */
                    ScanKeyInit(&keys[0],
                        Anum_pg_partition_parttype,
                        BTEqualStrategyNumber,
                        F_CHAREQ,
                        CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

                    ScanKeyInit(&keys[1],
                        Anum_pg_partition_parentid,
                        BTEqualStrategyNumber,
                        F_OIDEQ,
                        ObjectIdGetDatum(relationid));

                    pgpartition = heap_open(PartitionRelationId, AccessShareLock);
                    scan = tableam_scan_begin(pgpartition, SnapshotNow, 2, keys);

                    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
                        if (t_thrd.vacuum_cxt.vac_context) {
                            oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
                        }    
                        vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                        vacObj->tab_oid = HeapTupleGetOid(tuple);
                        vacObj->parent_oid = relationid;
                        vacObj->flags = VACFLG_SUB_PARTITION;
                        oid_list = lappend(oid_list, vacObj);

                        if (t_thrd.vacuum_cxt.vac_context) {
                            (void)MemoryContextSwitchTo(oldcontext);
                        }
                    }

                    heap_endscan(scan);
                    heap_close(pgpartition, AccessShareLock);

                    /*
                     * add partitioned table to list
                     */
                    if (t_thrd.vacuum_cxt.vac_context) {
                        oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
                    }
                    vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                    vacObj->tab_oid = relationid;
                    vacObj->parent_oid = InvalidOid;
                    vacObj->flags = VACFLG_MAIN_PARTITION;
                    oid_list = lappend(oid_list, vacObj);

                    if (t_thrd.vacuum_cxt.vac_context) {
                        (void)MemoryContextSwitchTo(oldcontext);
                    }
                } else if (classForm->parttype == PARTTYPE_SUBPARTITIONED_RELATION &&
                           classForm->relkind == RELKIND_RELATION &&
                           ((vacstmt->options & VACOPT_FULL) || (vacstmt->options & VACOPT_VACUUM))) {
                    oid_list = list_concat(oid_list, GetVacuumObjectOfSubpartitionTable(relationid));
                } else {
                    /*
                     * non-partitioned table
                     * forbit vacuum full/vacuum/analyze cstore.xxxxx direct on datanode
                     * except for timeseries index tables.
                     */
                    if (classForm->relnamespace == CSTORE_NAMESPACE) {
                        if (memcmp(vacstmt->relation->relname, "pg_delta", 8)
#ifdef ENABLE_MULTIPLE_NODES
                            && memcmp(vacstmt->relation->relname, TsConf::TAG_TABLE_NAME_PREFIX,
                                      strlen(TsConf::TAG_TABLE_NAME_PREFIX))
#endif
                        ) {
                            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                                            errmsg("cstore.%s is a internal table", vacstmt->relation->relname)));
                        }
                    }

                    if (t_thrd.vacuum_cxt.vac_context) {
                        oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);                    
                    }
                    vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                    vacObj->tab_oid = relationid;
                    vacObj->parent_oid = InvalidOid;
                    vacObj->flags = VACFLG_SIMPLE_HEAP;
                    vacObj->is_tsdb_deltamerge = (classForm->relkind == RELKIND_RELATION &&
                        (vacstmt->options & VACOPT_MERGE) &&
                        CheckRelOrientationByPgClassTuple(classTup, pgclassdesc, ORIENTATION_TIMESERIES));
                    oid_list = lappend(oid_list, vacObj);

                    if (t_thrd.vacuum_cxt.vac_context) {
                        (void)MemoryContextSwitchTo(oldcontext);
                    }
                }
            } else {
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                                errmsg("cache lookup failed for relation %u", relationid)));
            }

            ReleaseSysCache(classTup);
        }
    } else {
        /*
         * The SQL command is just "vacuum" without table specified, so
         * we must find all the plain tables, B-Tree index, partitions
         * and B-Tree index partitions from pg_class and pg_partition.
         */
        Relation pgclass;
        TableScanDesc scan;
        HeapTuple tuple;

        pgclass = heap_open(RelationRelationId, AccessShareLock);

        scan = tableam_scan_begin(pgclass, SnapshotNow, 0, NULL);

        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);

            /* Skip object in recycle bin. */
            if (TrIsRefRbObjectEx(RelationRelationId, HeapTupleGetOid(tuple), NameStr(classForm->relname))) {
                continue;
            }

            if (classForm->relkind != RELKIND_RELATION && classForm->relkind != RELKIND_MATVIEW)
                continue;

            /* skip the incremental matview */
            if ((is_incremental_matview(HeapTupleGetOid(tuple)) ||
                    (OidIsValid(find_matview_mlog_table(HeapTupleGetOid(tuple))))) &&
                (vacstmt->options & VACOPT_FULL)) {
                continue;
            }

            /* for dfs special vacuum, just skip for non-dfs table. */
            if ((((vacstmt->options & VACOPT_HDFSDIRECTORY) || (vacstmt->options & VACOPT_COMPACT)) &&
                    !CheckRelOrientationByPgClassTuple(tuple, pgclassdesc, ORIENTATION_ORC)) ||
                ((vacstmt->options & VACOPT_MERGE) &&
                !CheckRelOrientationByPgClassTuple(tuple, pgclassdesc, ORIENTATION_COLUMN) &&
                !CheckRelOrientationByPgClassTuple(tuple, pgclassdesc, ORIENTATION_ORC)))
                continue;

            /* Plain relation and valued partition relation */
            if (classForm->parttype == PARTTYPE_NON_PARTITIONED_RELATION ||
                classForm->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION ||
                ((vacstmt->options & VACOPT_MERGE) &&
                    CheckRelOrientationByPgClassTuple(tuple, pgclassdesc, ORIENTATION_COLUMN))) {
                /*
                 * when vacuum/vacuum full/analyze the total database,
                 * we skip some collect of relations, for example in CSTORE namespace.
                 */
                if (classForm->relnamespace == CSTORE_NAMESPACE)
                    continue;

                if (t_thrd.vacuum_cxt.vac_context) {
                    oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
                }
                vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                vacObj->tab_oid = HeapTupleGetOid(tuple);
                vacObj->parent_oid = InvalidOid;
                vacObj->flags = VACFLG_SIMPLE_HEAP;
                oid_list = lappend(oid_list, vacObj);

                if (t_thrd.vacuum_cxt.vac_context) {
                    (void)MemoryContextSwitchTo(oldcontext);
                }
            } else if (classForm->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
                oid_list = list_concat(oid_list, GetVacuumObjectOfSubpartitionTable(HeapTupleGetOid(tuple)));
            } else {
                /* 
                 * Partitioned table 
                 * It is a partitioned table, so find all the partitions in pg_partition
                 */
                Relation pgpartition;
                TableScanDesc partScan;
                HeapTuple partTuple;
                ScanKeyData keys[2];

                /* Process all plain partitions listed in pg_partition */
                ScanKeyInit(&keys[0],
                    Anum_pg_partition_parttype,
                    BTEqualStrategyNumber,
                    F_CHAREQ,
                    CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

                ScanKeyInit(&keys[1],
                    Anum_pg_partition_parentid,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(HeapTupleGetOid(tuple)));

                pgpartition = heap_open(PartitionRelationId, AccessShareLock);
                partScan = tableam_scan_begin(pgpartition, SnapshotNow, 2, keys);
                while (NULL != (partTuple = (HeapTuple) tableam_scan_getnexttuple(partScan, ForwardScanDirection))) {
                    if (t_thrd.vacuum_cxt.vac_context) {
                        oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);                    
                    }
                    vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                    vacObj->tab_oid = HeapTupleGetOid(partTuple);
                    vacObj->parent_oid = HeapTupleGetOid(tuple);
                    vacObj->flags = VACFLG_SUB_PARTITION;
                    oid_list = lappend(oid_list, vacObj);

                    if (t_thrd.vacuum_cxt.vac_context) {
                        (void)MemoryContextSwitchTo(oldcontext);                    
                    }
                }

                heap_endscan(partScan);
                heap_close(pgpartition, AccessShareLock);

                /*
                 * add partitioned table to list
                 */
                if (t_thrd.vacuum_cxt.vac_context) {
                    oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);                
                }
                vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                vacObj->tab_oid = HeapTupleGetOid(tuple);
                vacObj->parent_oid = InvalidOid;
                vacObj->flags = VACFLG_MAIN_PARTITION;
                oid_list = lappend(oid_list, vacObj);

                if (t_thrd.vacuum_cxt.vac_context) {
                    (void)MemoryContextSwitchTo(oldcontext);                
                }
            }
        }

        heap_endscan(scan);
        heap_close(pgclass, AccessShareLock);
    }

    return oid_list;
}

/*
 * vacuum_set_xid_limits() -- compute oldest-Xmin and freeze cutoff points
 */
void vacuum_set_xid_limits(Relation rel, int64 freeze_min_age, int64 freeze_table_age, TransactionId* oldestXmin,
    TransactionId* freezeLimit, TransactionId* freezeTableLimit, MultiXactId* multiXactFrzLimit)
{
    int64 freezemin;
    TransactionId limit;
    TransactionId safeLimit;
    TransactionId nextXid;

    /*
     * We can always ignore processes running lazy vacuum.	This is because we
     * use these values only for deciding which tuples we must keep in the
     * tables.	Since lazy vacuum doesn't write its XID anywhere, it's safe to
     * ignore it.  In theory it could be problematic to ignore lazy vacuums in
     * a full vacuum, but keep in mind that only one vacuum process can be
     * working on a particular table at any time, and that each vacuum is
     * always an independent transaction.
     */
    *oldestXmin = GetOldestXmin(rel);
    if (IsCatalogRelation(rel) || RelationIsAccessibleInLogicalDecoding(rel)) {
        TransactionId CatalogXmin = GetReplicationSlotCatalogXmin();
        if (TransactionIdIsNormal(CatalogXmin) && TransactionIdPrecedes(CatalogXmin, *oldestXmin)) {
            *oldestXmin = CatalogXmin;
        }
    }

    Assert(TransactionIdIsNormal(*oldestXmin));

    /*
     * Determine the minimum freeze age to use: as specified by the caller, or
     * vacuum_freeze_min_age, but in any case not more than half
     * autovacuum_freeze_max_age, so that autovacuums to prevent excessive
     * clog won't occur too frequently.
     */
    freezemin = freeze_min_age;
    if (freezemin < 0)
        freezemin = u_sess->attr.attr_storage.vacuum_freeze_min_age;
    freezemin = Min(freezemin, g_instance.attr.attr_storage.autovacuum_freeze_max_age / 2);
    Assert(freezemin >= 0);

    /*
     * Compute the cutoff XID, being careful not to generate a "permanent" XID
     */
    limit = *oldestXmin;
    if (limit > FirstNormalTransactionId + (uint64)freezemin)
        limit -= (uint64)freezemin;
    else
        limit = FirstNormalTransactionId;

    /*
     * If oldestXmin is very far back (in practice, more than
     * g_instance.attr.attr_storage.autovacuum_freeze_max_age / 2 XIDs old), complain and force a minimum
     * freeze age of zero.
     */
    nextXid = ReadNewTransactionId();
    if (nextXid > FirstNormalTransactionId + (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age)
        safeLimit = nextXid - (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age;
    else
        safeLimit = FirstNormalTransactionId;

    if (TransactionIdPrecedes(limit, safeLimit)) {
        ereport(WARNING,
            (errmsg("relation: %s, current xid: %lu, oldestXmin: %lu, oldest xmin is far in the past",
                 rel ? RelationGetRelationName(rel) : "NULL",
                 t_thrd.xact_cxt.ShmemVariableCache->nextXid,
                 *oldestXmin)));
        limit = *oldestXmin;
    }

    *freezeLimit = limit;

    if (freezeTableLimit != NULL) {
        int64 freezetable;

        /*
         * Determine the table freeze age to use: as specified by the caller,
         * or vacuum_freeze_table_age, but in any case not more than
         * autovacuum_freeze_max_age * 0.95, so that if you have e.g nightly
         * VACUUM schedule, the nightly VACUUM gets a chance to freeze tuples
         * before freeze autovacuum is launched.
         */
        freezetable = freeze_table_age;
        if (freezetable < 0)
            freezetable = u_sess->attr.attr_storage.vacuum_freeze_table_age;
        freezetable = Min(freezetable, g_instance.attr.attr_storage.autovacuum_freeze_max_age * 0.95);
        Assert(freezetable >= 0);

        /*
         * Compute the cutoff XID, being careful not to generate a "permanent"
         * XID.
         */
        limit = ReadNewTransactionId();
        if (limit > FirstNormalTransactionId + (uint64)freezetable)
            limit -= (uint64)freezetable;
        else
            limit = FirstNormalTransactionId;

        *freezeTableLimit = limit;
    }

    if (multiXactFrzLimit != NULL) {
#ifndef ENABLE_MULTIPLE_NODES
        MultiXactId mxLimit;

        /*
         * simplistic multixactid freezing: use the same freezing policy as
         * for Xids
         */
        mxLimit = GetOldestMultiXactId();
        if (mxLimit > FirstMultiXactId + (uint64)freezemin)
            mxLimit -= (uint64)freezemin;
        else
            mxLimit = FirstMultiXactId;

        *multiXactFrzLimit = mxLimit;
#else
        *multiXactFrzLimit = InvalidMultiXactId;
#endif
    }


}

/*
 * vac_estimate_reltuples() -- estimate the new value for pg_class.reltuples
 *
 *		If we scanned the whole relation then we should just use the count of
 *		live tuples seen; but if we did not, we should not blindly extrapolate
 *		from that number, since VACUUM may have scanned a quite nonrandom
 *		subset of the table.  When we have only partial information, we take
 *		the old value of pg_class.reltuples as a measurement of the
 *		tuple density in the unscanned pages.
 */
double vac_estimate_reltuples(
    Relation relation, BlockNumber total_pages, BlockNumber scanned_pages, double scanned_tuples)
{
    double old_rel_pages = relation->rd_rel->relpages;
    double old_rel_tuples = relation->rd_rel->reltuples;
    double old_density;
    double unscanned_pages;
    double total_tuples;

    if (RelationIsBucket(relation)) {
        return 0;
    }

    /* If we did scan the whole table, just use the count as-is */
    if (scanned_pages >= total_pages)
        return scanned_tuples;

    /*
     * If scanned_pages is zero but total_pages isn't, keep the existing value
     * of reltuples.  (Note: callers should avoid updating the pg_class
     * statistics in this situation, since no new information has been
     * provided.)
     */
    if (scanned_pages == 0)
        return old_rel_tuples;

    /*
     * If old value of relpages is zero, old density is indeterminate; we
     * can't do much except scale up scanned_tuples to match total_pages.
     */
    if (old_rel_pages == 0)
        return floor((scanned_tuples / scanned_pages) * total_pages + 0.5);

    /*
     * Okay, we've covered the corner cases.  The normal calculation is to
     * convert the old measurement to a density (tuples per page), then
     * estimate the number of tuples in the unscanned pages using that figure,
     * and finally add on the number of tuples in the scanned pages.
     */
    old_density = old_rel_tuples / old_rel_pages;
    unscanned_pages = (double)total_pages - (double)scanned_pages;
    total_tuples = old_density * unscanned_pages + scanned_tuples;
    return floor(total_tuples + 0.5);
}

static void debug_print_rows_and_pages(Relation relation, Form_pg_class pgcform)
{
    if (!module_logging_is_on(MOD_AUTOVAC)) {
        return;
    }
    ereport(DEBUG2, (errmodule(MOD_AUTOVAC),
                    (errmsg("Update relstats for relation %s: "
                            "old totalrows = %lf, relpages = %lf; "
                            "new totalrows = %lf, relpages = %lf.",
                            NameStr(relation->rd_rel->relname),
                            relation->rd_rel->reltuples, relation->rd_rel->relpages,
                            pgcform->reltuples, pgcform->relpages))));
}

/*
 *	vac_update_relstats() -- update statistics for one relation
 *
 *		Update the whole-relation statistics that are kept in its pg_class
 *		row.  There are additional stats that will be updated if we are
 *		doing ANALYZE, but we always update these stats.  This routine works
 *		for both index and heap relation entries in pg_class.
 *
 *		We violate transaction semantics here by overwriting the rel's
 *		existing pg_class tuple with the new values.  This is reasonably
 *		safe since the new values are correct whether or not this transaction
 *		commits.  The reason for this is that if we updated these tuples in
 *		the usual way, vacuuming pg_class itself wouldn't work very well ---
 *		by the time we got done with a vacuum cycle, most of the tuples in
 *		pg_class would've been obsoleted.  Of course, this only works for
 *		fixed-size never-null columns, but these are.
 *
 *		Note another assumption: that two VACUUMs/ANALYZEs on a table can't
 *		run in parallel, nor can VACUUM/ANALYZE run in parallel with a
 *		schema alteration such as adding an index, rule, or trigger.  Otherwise
 *		our updates of relhasindex etc might overwrite uncommitted updates.
 *
 *		Another reason for doing it this way is that when we are in a lazy
 *		VACUUM and have PROC_IN_VACUUM set, we mustn't do any updates ---
 *		somebody vacuuming pg_class might think they could delete a tuple
 *		marked with xmin = our xid.
 *		isdirty - identify the data of the relation have changed or not.
 *
 *		This routine is shared by VACUUM and ANALYZE.
 */
void vac_update_relstats(Relation relation, Relation classRel, RelPageType num_pages, double num_tuples,
    BlockNumber num_all_visible_pages, bool hasindex, TransactionId frozenxid, MultiXactId minmulti)
{
    Oid relid = RelationGetRelid(relation);
    HeapTuple ctup;
    HeapTuple nctup = NULL;
    Form_pg_class pgcform;
    bool dirty = false;
    bool isNull = false;
    TransactionId relfrozenxid;
    Datum xid64datum;
    bool isGtt = false;
    MultiXactId relminmxid;

    /* global temp table remember relstats to localhash and rel->rd_rel, not catalog */
    if (RELATION_IS_GLOBAL_TEMP(relation)) {
        isGtt = true;
        up_gtt_relstats(relation,
                        static_cast<unsigned int>(num_pages), num_tuples,
                        num_all_visible_pages,
                        frozenxid);
    }

    /* Fetch a copy of the tuple to scribble on */
    ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(ctup))
        ereport(ERROR,
            (errcode(ERRCODE_NO_DATA_FOUND), errmsg("pg_class entry for relid %u vanished during vacuuming", relid)));
    pgcform = (Form_pg_class)GETSTRUCT(ctup);

    /* Apply required updates, if any, to copied tuple */
    dirty = false;
#ifdef PGXC
    // frozenxid == BootstrapTransactionId means it was invoked by execRemote.cpp:ReceivePageAndTuple()
    if (IS_PGXC_DATANODE || (frozenxid == BootstrapTransactionId) || IsSystemRelation(relation)) {
#endif
        if (isGtt) {
            relation->rd_rel->relpages = (int32) num_pages;
            relation->rd_rel->reltuples = (float4) num_tuples;
            relation->rd_rel->relallvisible = (int32) num_all_visible_pages;
        } else {
            if (pgcform->relpages - num_pages != 0) {
                pgcform->relpages = num_pages;
                dirty = true;
            }
            if (pgcform->reltuples - num_tuples != 0) {
                pgcform->reltuples = num_tuples;
                dirty = true;
            }
            if (pgcform->relallvisible != (int32)num_all_visible_pages) {
                pgcform->relallvisible = (int32)num_all_visible_pages;
                dirty = true;
            }
        }
        debug_print_rows_and_pages(relation, pgcform);
#ifdef PGXC
    }
#endif
    if (pgcform->relhasindex != hasindex) {
        pgcform->relhasindex = hasindex;
        dirty = true;
    }

    /*
     * If we have discovered that there are no indexes, then there's no
     * primary key either.	This could be done more thoroughly...
     */
    if (pgcform->relhaspkey && !hasindex) {
        pgcform->relhaspkey = false;
        dirty = true;
    }

    /* We also clear relhasrules and relhastriggers if needed */
    if (pgcform->relhasrules && relation->rd_rules == NULL) {
        pgcform->relhasrules = false;
        dirty = true;
    }
    if (pgcform->relhastriggers && relation->trigdesc == NULL) {
        pgcform->relhastriggers = false;
        dirty = true;
    }

    /*
     * relfrozenxid should never go backward, except in PGXC, when xid has gone
     * out-of-sync w.r.t. gxid and we want to correct it using standalone
     * backend.
     * Caller can pass InvalidTransactionId if it has no new data.
     */
    xid64datum = tableam_tops_tuple_getattr(ctup, Anum_pg_class_relfrozenxid64, RelationGetDescr(classRel), &isNull);

    if (isNull) {
        relfrozenxid = pgcform->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

#ifndef ENABLE_MULTIPLE_NODES
    Datum minmxidDatum = tableam_tops_tuple_getattr(ctup,
        Anum_pg_class_relminmxid,
        RelationGetDescr(classRel),
        &isNull);
    relminmxid = isNull ? InvalidMultiXactId : DatumGetTransactionId(minmxidDatum);
#endif

    if ((TransactionIdIsNormal(frozenxid) && (TransactionIdPrecedes(relfrozenxid, frozenxid)
                                                || !IsPostmasterEnvironment)) ||
        (MultiXactIdIsValid(minmulti) && (MultiXactIdPrecedes(relminmxid, minmulti)
                                                || !IsPostmasterEnvironment)) || isNull
    ) {

        Datum values[Natts_pg_class];
        bool nulls[Natts_pg_class];
        bool replaces[Natts_pg_class];
        errno_t rc;

        pgcform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "", "");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "", "");

        if (TransactionIdIsNormal(frozenxid) && TransactionIdPrecedes(relfrozenxid, frozenxid)) {
            replaces[Anum_pg_class_relfrozenxid64 - 1] = true;
            values[Anum_pg_class_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenxid);
        }

#ifndef ENABLE_MULTIPLE_NODES
        if ((MultiXactIdIsValid(minmulti) && MultiXactIdPrecedes(relminmxid, minmulti)) || isNull) {
            replaces[Anum_pg_class_relminmxid - 1] = true;
            values[Anum_pg_class_relminmxid - 1] = TransactionIdGetDatum(minmulti);
        }
#endif

        nctup = (HeapTuple) tableam_tops_modify_tuple(ctup, RelationGetDescr(classRel), values, nulls, replaces);
        ctup = nctup;
        dirty = true;
    }

    /* If anything changed, write out the tuple. */
    if (dirty) {
        if (isNull && nctup) {
            simple_heap_update(classRel, &ctup->t_self, ctup);
            CatalogUpdateIndexes(classRel, ctup);
        } else
            heap_inplace_update(classRel, ctup);

        if (nctup)
            heap_freetuple(nctup);
    }
}

/*
 *	vac_update_datfrozenxid() -- update pg_database.datfrozenxid for our DB
 *
 *		Update pg_database's datfrozenxid entry for our database to be the
 *		minimum of the pg_class.relfrozenxid values.
 *
 *		Similarly, update our datfrozenmulti to be the minimum of the
 *		pg_class.relfrozenmulti values.
 *
 *		If we are able to advance either pg_database value, also try to
 *		truncate pg_clog and pg_multixact.
 *
 *		We violate transaction semantics here by overwriting the database's
 *		existing pg_database tuple with the new value.	This is reasonably
 *		safe since the new value is correct whether or not this transaction
 *		commits.  As with vac_update_relstats, this avoids leaving dead tuples
 *		behind after a VACUUM.
 */
void vac_update_datfrozenxid(void)
{
    HeapTuple tuple;
    HeapTuple newtuple;
    Form_pg_database dbform;
    Relation relation;
    SysScanDesc scan;
    HeapTuple classTup;
    TransactionId newFrozenXid;
    TransactionId lastSaneFrozenXid;
    bool dirty = false;
    bool bogus = false;
    bool isNull = false;
    TransactionId datfrozenxid;
    TransactionId relfrozenxid;
    Datum xid64datum;
    MultiXactId	newFrozenMulti = InvalidMultiXactId;
#ifndef ENABLE_MULTIPLE_NODES
    Datum minmxidDatum;
    MultiXactId relminmxid = InvalidMultiXactId;
#endif
    MultiXactId	datminmxid = InvalidMultiXactId;

    /* Don't update datfrozenxid when cluser is resizing */
    if (ClusterResizingInProgress()) {
        return;
    }
    /*
     * Initialize the "min" calculation with GetOldestXmin, which is a
     * reasonable approximation to the minimum relfrozenxid for not-yet-
     * committed pg_class entries for new tables; see AddNewRelationTuple().
     * So we cannot produce a wrong minimum by starting with this.
     */
    newFrozenXid = GetOldestXmin(NULL);

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * Similarly, initialize the MultiXact "min" with the value that would
     * be used on pg_class for new tables.  See AddNewRelationTuple().
     */
    newFrozenMulti = GetOldestMultiXactId();
#endif

    lastSaneFrozenXid = ReadNewTransactionId();

    /*
     * We must seqscan pg_class to find the minimum Xid, because there is no
     * index that can help us here.
     */
    relation = heap_open(RelationRelationId, AccessShareLock);

    scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    while ((classTup = systable_getnext(scan)) != NULL) {
        Form_pg_class classForm = (Form_pg_class)GETSTRUCT(classTup);

        /*
         * Only consider heap and TOAST tables (anything else should have
         * InvalidTransactionId in relfrozenxid anyway.)
         */
        if (classForm->relkind != RELKIND_RELATION &&
            classForm->relkind != RELKIND_MATVIEW &&
            classForm->relkind != RELKIND_TOASTVALUE)
            continue;

        /* global temp table relstats not in pg_class */
        if (classForm->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            continue;
        }

        xid64datum = tableam_tops_tuple_getattr(classTup, Anum_pg_class_relfrozenxid64, RelationGetDescr(relation), &isNull);

        if (isNull) {
            relfrozenxid = classForm->relfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                !TransactionIdIsNormal(relfrozenxid))
                relfrozenxid = FirstNormalTransactionId;
        } else
            relfrozenxid = DatumGetTransactionId(xid64datum);

        Assert(TransactionIdIsNormal(relfrozenxid));

        /*
         * If things are working properly, no relation should have a
         * relfrozenxid that is "in the future" in pg. But in cluster environment,
         * it might be the node didn't attend transactions for a long time.
         * If this vacuum thread sleep a long time after get lastSaneFronzen,
         * another vacuum change the relfrozenxid, the following might occur.
         */
        if (TransactionIdPrecedes(lastSaneFrozenXid, relfrozenxid)) {
            ereport(LOG,
                (errmsg("Relfrozenxid %lu is larger than nextXid " XID_FMT ". newFrozenXid:" XID_FMT
                        ", Current xid: " XID_FMT ", pid: %lu, RecentXmin:" XID_FMT ", RecentGlobalXmin:" XID_FMT
                        ", OldestXmin:" XID_FMT ", FreezeLimit: %lu, useLocalSnapshot:%d",
                     relfrozenxid,
                     lastSaneFrozenXid,
                     newFrozenXid,
                     t_thrd.pgxact->xid,
                     t_thrd.proc->pid,
                     u_sess->utils_cxt.RecentXmin,
                     u_sess->utils_cxt.RecentGlobalXmin,
                     u_sess->cmd_cxt.OldestXmin,
                     u_sess->cmd_cxt.FreezeLimit,
                     t_thrd.xact_cxt.useLocalSnapshot),
                    errdetail("Don't do update datfrozenxid to avoid data missing.")));
            bogus = true;
            break;
        }

        if (TransactionIdPrecedes(relfrozenxid, newFrozenXid))
            newFrozenXid = relfrozenxid;

#ifndef ENABLE_MULTIPLE_NODES
        minmxidDatum = tableam_tops_tuple_getattr(
            classTup, Anum_pg_class_relminmxid, RelationGetDescr(relation), &isNull);
        relminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(minmxidDatum);

        if (MultiXactIdIsValid(relminmxid) && MultiXactIdPrecedes(relminmxid, newFrozenMulti))
            newFrozenMulti = relminmxid;
#endif
    }

    /* we're done with pg_class */
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    /* chicken out if bogus data found */
    if (bogus) {
        return;
    }

    Assert(TransactionIdIsNormal(newFrozenXid));
    /*
    * Global temp table get frozenxid from MyProc
    * to avoid the vacuum truncate clog that gtt need.
    */
    if (u_sess->attr.attr_storage.max_active_gtt > 0) {
        TransactionId safeAge;
        TransactionId oldestGttFrozenxid = InvalidTransactionId;
        if (ENABLE_THREAD_POOL) {
            ThreadPoolSessControl* sessCtl = g_threadPoolControler->GetSessionCtrl();
            oldestGttFrozenxid = sessCtl->ListAllSessionGttFrozenxids(0, NULL, NULL, NULL);
        } else {
            oldestGttFrozenxid = ListAllThreadGttFrozenxids(0, NULL, NULL, NULL);
        }

        if (TransactionIdIsNormal(oldestGttFrozenxid)) {
            safeAge =
                oldestGttFrozenxid + static_cast<TransactionId>(u_sess->attr.attr_storage.vacuum_gtt_defer_check_age);
            if (safeAge < FirstNormalTransactionId) {
                safeAge += FirstNormalTransactionId;
            }

            /*
            * We tolerate that the minimum age of gtt is less than
            * the minimum age of conventional tables, otherwise it will
            * throw warning message.
            */
            if (TransactionIdIsNormal(safeAge) &&
                TransactionIdPrecedes(safeAge, newFrozenXid)) {
                ereport(WARNING, 
                    (errmsg(
                        "global temp table oldest relfrozenxid %lu is the oldest in the entire db", oldestGttFrozenxid),
                     errdetail("The oldest relfrozenxid in pg_class is %lu", newFrozenXid),
                     errhint("If they differ greatly, please consider cleaning up the data in global temp table.")));
            }

            if (TransactionIdPrecedes(oldestGttFrozenxid, newFrozenXid)) {
                newFrozenXid = oldestGttFrozenxid;
            }
        }
    }

    /* Now fetch the pg_database tuple we need to update. */
    relation = heap_open(DatabaseRelationId, RowExclusiveLock);

    /* Fetch a copy of the tuple to scribble on */
    tuple = SearchSysCacheCopy1(DATABASEOID, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_NO_DATA_FOUND),
                errmsg("could not find tuple for database %u", u_sess->proc_cxt.MyDatabaseId)));
    dbform = (Form_pg_database)GETSTRUCT(tuple);

    /*
     * Don't allow datfrozenxid to go backward, unless in PGXC when it's a
     * standalone backend and we want to bring the datfrozenxid in sync with gxid.
     * Also detect the common case where it doesn't go forward either.
     */
    xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_database_datfrozenxid64, RelationGetDescr(relation), &isNull);

    if (isNull) {
        datfrozenxid = dbform->datfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, datfrozenxid))
            datfrozenxid = FirstNormalTransactionId;
    } else {
        datfrozenxid = DatumGetTransactionId(xid64datum);
    }

    /* Consider frozenxid of objects in recyclebin. */
    TrAdjustFrozenXid64(u_sess->proc_cxt.MyDatabaseId, &newFrozenXid);

#ifndef ENABLE_MULTIPLE_NODES
    minmxidDatum = tableam_tops_tuple_getattr(tuple, Anum_pg_database_datminmxid, RelationGetDescr(relation), &isNull);
    datminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(minmxidDatum);
#endif

    if ((TransactionIdPrecedes(datfrozenxid, newFrozenXid)) || (MultiXactIdPrecedes(datminmxid, newFrozenMulti))
#ifdef PGXC
        || !IsPostmasterEnvironment
#endif
    ) {
        Datum values[Natts_pg_database];
        bool nulls[Natts_pg_database];
        bool replaces[Natts_pg_database];
        errno_t rc;

        dbform->datfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "", "");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "", "");

        if (TransactionIdPrecedes(datfrozenxid, newFrozenXid)) {
            replaces[Anum_pg_database_datfrozenxid64 - 1] = true;
            values[Anum_pg_database_datfrozenxid64 - 1] = TransactionIdGetDatum(newFrozenXid);
        }
#ifndef ENABLE_MULTIPLE_NODES
        if (MultiXactIdPrecedes(datminmxid, newFrozenMulti)) {
            replaces[Anum_pg_database_datminmxid - 1] = true;
            values[Anum_pg_database_datminmxid - 1] = TransactionIdGetDatum(newFrozenMulti);
        }
#endif
        newtuple = (HeapTuple) tableam_tops_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
        dirty = true;
    }

    if (dirty) {
        if (isNull) {
            simple_heap_update(relation, &newtuple->t_self, newtuple);
            CatalogUpdateIndexes(relation, newtuple);
        } else
            heap_inplace_update(relation, newtuple);

        heap_freetuple(newtuple);
    }

    heap_freetuple(tuple);
    heap_close(relation, RowExclusiveLock);

    /*
     * If we were able to advance datfrozenxid, see if we can truncate
     * pg_clog. Also do it if the shared XID-wrap-limit info is stale, since
     * this action will update that too.
     */
    if (dirty || ForceTransactionIdLimitUpdate()) {
        vac_truncate_clog(newFrozenXid, newFrozenMulti);
    }
}

/*
 *	vac_truncate_clog() -- attempt to truncate the commit log
 *
 *		Scan pg_database to determine the system-wide oldest datfrozenxid,
 *		and use it to truncate the transaction commit log (pg_clog).
 *
 *		The passed XID is simply the one I just wrote into my pg_database
 *		entry.	It's used to initialize the "min" calculation.
 *
 *		This routine is only invoked when we've managed to change our
 *		DB's datfrozenxid entry.
 */
static void vac_truncate_clog(TransactionId frozenXID, MultiXactId frozenMulti)
{
    Relation relation;
    TableScanDesc scan;
    HeapTuple tuple;
    Oid oldestxid_datoid;
    Oid oldestmulti_datoid;

    /* init oldest datoids to sync with my frozen values */
    oldestxid_datoid = u_sess->proc_cxt.MyDatabaseId;
    oldestmulti_datoid = u_sess->proc_cxt.MyDatabaseId;

    /*
     * Scan pg_database to compute the minimum datfrozenxid
     *
     * Since vac_update_datfrozenxid updates datfrozenxid in-place,
     * the values could change while we look at them.  Fetch each one just
     * once to ensure sane behavior of the comparison logic.  (Here, as in
     * many other places, we assume that fetching or updating an XID in shared
     * storage is atomic.)
     *
     * Note: we need not worry about a race condition with new entries being
     * inserted by CREATE DATABASE.  Any such entry will have a copy of some
     * existing DB's datfrozenxid, and that source DB cannot be ours because
     * of the interlock against copying a DB containing an active backend.
     * Hence the new entry will not reduce the minimum.  Also, if two VACUUMs
     * concurrently modify the datfrozenxid's of different databases, the
     * worst possible outcome is that pg_clog is not truncated as aggressively
     * as it could be.
     */
    relation = heap_open(DatabaseRelationId, AccessShareLock);

    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        volatile FormData_pg_database* dbform = (Form_pg_database)GETSTRUCT(tuple);

        bool isNull = false;
        TransactionId datfrozenxid;
        Datum xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_database_datfrozenxid64, RelationGetDescr(relation), &isNull);

        if (isNull) {
            datfrozenxid = dbform->datfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, datfrozenxid))
                datfrozenxid = FirstNormalTransactionId;
        } else
            datfrozenxid = DatumGetTransactionId(xid64datum);

        Assert(TransactionIdIsNormal(datfrozenxid));

        if (TransactionIdPrecedes(datfrozenxid, frozenXID)) {
            frozenXID = datfrozenxid;
            oldestxid_datoid = HeapTupleGetOid(tuple);
        }

#ifndef ENABLE_MULTIPLE_NODES
        Datum minmxidDatum = tableam_tops_tuple_getattr(tuple, Anum_pg_database_datminmxid,
            RelationGetDescr(relation), &isNull);
        MultiXactId datminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(minmxidDatum);
        Assert(MultiXactIdIsValid(datminmxid));

        if (MultiXactIdPrecedes(datminmxid, frozenMulti)) {
            frozenMulti = datminmxid;
            oldestmulti_datoid = HeapTupleGetOid(tuple);
        }
#endif
    }

    heap_endscan(scan);

    heap_close(relation, AccessShareLock);

    /* Truncate CLOG to the oldest frozenxid */
    TruncateCLOG(frozenXID);
#ifndef ENABLE_MULTIPLE_NODES
    TruncateMultiXact(frozenMulti);
#endif

    /*
     * Update the wrap limit for GetNewTransactionId and creation of new
     * MultiXactIds.  Note: these functions will also signal the postmaster for
     * an(other) autovac cycle if needed.   XXX should we avoid possibly
     * signalling twice?
     */
    SetTransactionIdLimit(frozenXID, oldestxid_datoid);
    MultiXactAdvanceOldest(frozenMulti, oldestmulti_datoid);

    ereport(LOG,
        (errmsg("In truncate clog: frozenXID:" XID_FMT ", oldestxid_datoid: %u, xid:" XID_FMT
                ", pid: %lu, ShmemVariableCache: nextOid:%u, oldestXid:" XID_FMT ","
                "nextXid:" XID_FMT ", xidVacLimit:%lu, oldestXidDB:%u, RecentXmin:" XID_FMT
                ", RecentGlobalXmin:" XID_FMT ", OldestXmin:" XID_FMT ", FreezeLimit:%lu, useLocalSnapshot:%d.",
            frozenXID,
            oldestxid_datoid,
            t_thrd.pgxact->xid,
            t_thrd.proc->pid,
            t_thrd.xact_cxt.ShmemVariableCache->nextOid,
            t_thrd.xact_cxt.ShmemVariableCache->oldestXid,
            t_thrd.xact_cxt.ShmemVariableCache->nextXid,
            t_thrd.xact_cxt.ShmemVariableCache->xidVacLimit,
            t_thrd.xact_cxt.ShmemVariableCache->oldestXidDB,
            u_sess->utils_cxt.RecentXmin,
            u_sess->utils_cxt.RecentGlobalXmin,
            u_sess->cmd_cxt.OldestXmin,
            u_sess->cmd_cxt.FreezeLimit,
            t_thrd.xact_cxt.useLocalSnapshot)));
}

static inline void proc_snapshot_and_transaction()
{
    PopActiveSnapshot();
    CommitTransactionCommand();
    gstrace_exit(GS_TRC_ID_vacuum_rel);
    return;
}

static inline void
TableRelationVacuum(Relation rel, VacuumStmt *vacstmt, BufferAccessStrategy vacStrategy)
{
    if (RelationIsUstoreFormat(rel)) {
        LazyVacuumUHeapRel(rel, vacstmt, vacStrategy);
    } else {
        lazy_vacuum_rel(rel, vacstmt, vacStrategy);
    }
}

/*
 *	vacuum_rel() -- vacuum one heap relation
 *
 *		Doing one heap at a time incurs extra overhead, since we need to
 *		check that the heap exists again just before we vacuum it.	The
 *		reason that we do this is so that vacuuming can be spread across
 *		many small transactions.  Otherwise, two-phase locking would require
 *		us to lock the entire database during one pass of the vacuum cleaner.
 *
 *		At entry and exit, we are not inside a transaction.
 */
static bool vacuum_rel(Oid relid, VacuumStmt* vacstmt, bool do_toast)
{
    LOCKMODE lmode;
    Relation onerel = NULL;
    LockRelId onerelid;
    Oid toast_relid;
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;

    Partition onepart = NULL;
    Relation onepartrel = NULL;
    Oid relationid = InvalidOid;
    bool GetLock = false;
    LOCKMODE lmodePartTable = NoLock;
    PartitionIdentifier* partIdentifier = NULL;
    PartitionIdentifier partIdtf;
    bool isFakeRelation = false;
    LockRelId partLockRelId;
    Relation cudescrel = NULL;
    Relation deltarel = NULL;
    LockRelId cudescLockRelid = InvalidLockRelId;
    LockRelId deltaLockRelid = InvalidLockRelId;

    /*
     * we use this identify whether the partition is a subpartition
     * subparentid is valid means it's a subpartition, then relationid saves the grandparentid
     */
    Oid subparentid = InvalidOid;
    Partition onesubpart = NULL;
    Relation onesubpartrel = NULL;
    PartitionIdentifier* subpartIdentifier = NULL;
    PartitionIdentifier subpartIdtf;

    /* Vacuum map/log table obeys the same rule as toast, only triggered by vacuum, ignored by autovacuum */
    bool doMapLog = do_toast;
    Oid maplogOid;

    int messageLevel = -1;
    /* Begin a transaction for vacuuming this relation */
    gstrace_entry(GS_TRC_ID_vacuum_rel);
    StartTransactionCommand();

    /* vacuum must hold RowExclusiveLock on db for a new transaction */
    LockSharedObject(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, RowExclusiveLock);
    /* check database valid */
    get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true);

    if (!(vacstmt->options & VACOPT_FULL)) {
        /*
         * In lazy vacuum, we can set the PROC_IN_VACUUM flag, which lets
         * other concurrent VACUUMs know that they can ignore this one while
         * determining their OldestXmin.  (The reason we don't set it during a
         * full VACUUM is exactly that we may have to run user-defined
         * functions for functional indexes, and we want to make sure that if
         * they use the snapshot set above, any tuples it requires can't get
         * removed from other tables.  An index function that depends on the
         * contents of other tables is arguably broken, but we won't break it
         * here by violating transaction semantics.)
         *
         * We also set the VACUUM_FOR_WRAPAROUND flag, which is passed down by
         * autovacuum; it's used to avoid canceling a vacuum that was invoked
         * in an emergency.
         *
         * Note: these flags remain set until CommitTransaction or
         * AbortTransaction.  We don't want to clear them until we reset
         * MyPgXact->xid/xmin, else OldestXmin might appear to go backwards,
         * which is probably Not Good.
         */
        (void)LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        t_thrd.pgxact->vacuumFlags |= PROC_IN_VACUUM;

        if (IS_PGXC_DATANODE && !IS_SINGLE_NODE) {
            t_thrd.pgxact->vacuumFlags |= PROC_IS_AUTOVACUUM;
        }

        LWLockRelease(ProcArrayLock);
    }

#ifdef PGXC
    ereport(DEBUG1, (errmodule(MOD_AUTOVAC), (errmsg("Starting autovacuum"))));
    /* Now that flags have been set, we can take a snapshot correctly */
    PushActiveSnapshot(GetTransactionSnapshot());
    ereport(DEBUG1, (errmodule(MOD_AUTOVAC), (errmsg("Started autovacuum"))));
#endif
    /*
     * Check for user-requested abort.	Note we want this to be inside a
     * transaction, so xact.c doesn't issue useless WARNING.
     */
    CHECK_FOR_INTERRUPTS();

    /* Get the partitioned table's oid */
    if (vacuumPartition(vacstmt->flags)) {
        relationid = partid_get_parentid(relid);
        if (!OidIsValid(relationid)) {
            proc_snapshot_and_transaction();
            return false;
        }
        Oid grandparentid = partid_get_parentid(relationid);
        if (OidIsValid(grandparentid)) {
            subparentid = relationid;
            relationid = grandparentid;
        }
    }

    /*
     * Determine the type of lock we want --- hard exclusive lock for a FULL
     * vacuum, but just ShareUpdateExclusiveLock for concurrent vacuum. Either
     * way, we can be sure that no other backend is vacuuming the same table.
     */
    // we need to lock low mode on partiton table
    if (vacstmt->options & VACOPT_FULL) {
        lmode = ExclusiveLock;
        lmodePartTable = ShareUpdateExclusiveLock;
    } else {
        lmode = ShareUpdateExclusiveLock;
        lmodePartTable = ShareUpdateExclusiveLock;
    }

#ifdef ENABLE_MOT
    if (vacuumRelation(vacstmt->flags) && vacstmt->isMOTForeignTable) {
        if (IS_PGXC_COORDINATOR) {
            proc_snapshot_and_transaction();
            return false;
        }
        lmode = AccessExclusiveLock;
    }
#endif

    if ((!OidIsValid(relationid)) && (vacstmt->options & VACOPT_FULL) && is_sys_table(relid))
        lmode = AccessExclusiveLock;

    /*
     * Open the relation and get the appropriate lock on it.
     *
     * There's a race condition here: the rel may have gone away since the
     * last time we saw it.  If so, we don't need to vacuum it.
     *
     * If we've been asked not to wait for the relation lock, acquire it first
     * in non-blocking mode, before calling try_relation_open().
     */
    if (vacuumRelation(vacstmt->flags) && !(vacstmt->options & VACOPT_NOWAIT)) {

        if (relid > FirstNormalObjectId && !checkGroup(relid, true)) {
            proc_snapshot_and_transaction();
            return false;
        }

        /* check the permissions to vacuum full system table before getting high level lock */
        if (relid < FirstNormalObjectId && (vacstmt->options & VACOPT_FULL)) {
            Relation rel = try_relation_open(relid, AccessShareLock);
            if (rel != NULL && rel->rd_rel->relkind == RELKIND_RELATION) {
                AclResult aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_VACUUM);
                if (aclresult != ACLCHECK_OK && !(pg_class_ownercheck(relid, GetUserId()) ||
                        (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) &&
                            !rel->rd_rel->relisshared) ||
                                (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
                    if (vacstmt->options & VACOPT_VERBOSE)
                        messageLevel = VERBOSEMESSAGE;
                    else
                        messageLevel = WARNING;

                    if (rel->rd_rel->relisshared)
                        ereport(messageLevel,
                            (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                                errmsg("skipping \"%s\" --- only system admin can vacuum it",
                                    RelationGetRelationName(rel))));
                    else if (rel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
                        ereport(messageLevel,
                            (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                                errmsg("skipping \"%s\" --- only system admin or database owner can vacuum it",
                                    RelationGetRelationName(rel))));
                    else
                        ereport(messageLevel,
                            (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                                errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
                                    RelationGetRelationName(rel))));
                    relation_close(rel, AccessShareLock);
                    proc_snapshot_and_transaction();
                    return false;
                }

                if (rel != NULL && relid == PartitionRelationId && PartitionMetadataDisabledClean(rel)) {
                    if (vacstmt->options & VACOPT_VERBOSE) {
                        messageLevel = VERBOSEMESSAGE;
                    } else {
                        messageLevel = WARNING;
                    }
                    ereport(messageLevel,
                        (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                            errmsg("skipping \"%s\" --- only table or database can vacuum it",
                                RelationGetRelationName(rel))));
                    relation_close(rel, AccessShareLock);
                    proc_snapshot_and_transaction();
                    return false;
                }
            }

            /* VACUUM FULL on hard-coded catalogs are only executed under maintenance mode */
            if (rel != NULL && relid < FirstBootstrapObjectId &&
                !u_sess->attr.attr_common.xc_maintenance_mode && !IsInitdb) {
                ereport(NOTICE,
                        (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                         errmsg("skipping \"%s\" --- use xc_maintenance_mode to VACUUM FULL it",
                             RelationGetRelationName(rel))));
                relation_close(rel, AccessShareLock);
                proc_snapshot_and_transaction();
                return false;
            }

            /* Forbid vacuum full on shared relation during upgrade, to protect global/pg_filenode.map not changed */
            if (u_sess->attr.attr_common.upgrade_mode != 0 && rel != NULL &&
                relid < FirstBootstrapObjectId && rel->rd_rel->relisshared &&
                t_thrd.proc->workingVersionNum < RELMAP_4K_VERSION_NUM) {
                ereport(NOTICE,
                        (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                         errmsg("skipping \"%s\" --- VACUUM FULL on shared relation is not allowed during upgrade",
                             RelationGetRelationName(rel))));
                relation_close(rel, AccessShareLock);
                proc_snapshot_and_transaction();
                return false;
            }

            if (rel != NULL)
                relation_close(rel, AccessShareLock);
        }

        /*
         * Coordinator needs guarantee the old select statement must finish when
         * run vacuum full table
         */
        CNGuardOldQueryForVacuumFull(vacstmt, relid);

        onerel = try_relation_open(relid, lmode);
        if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
        } else if (onerel != NULL && onerel->rd_rel != NULL && 
            onerel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !validateTempNamespace(onerel->rd_rel->relnamespace)) {
            relation_close(onerel, lmode);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                    errmsg("Temp table's data is invalid because datanode %s restart. "
                       "Quit your session to clean invalid temp tables.", 
                       g_instance.attr.attr_common.PGXCNodeName)));
        }
        GetLock = true;
        /*
         * We block vacuum operation while the target table is in redistribution
         * read only mode. For redistribution IUD mode, we will block vacuum before
         * coming to this point.
         */
        if (!u_sess->attr.attr_sql.enable_cluster_resize && onerel != NULL &&
            RelationInClusterResizingReadOnly(onerel)) {
            ereport(ERROR,
                (errcode(ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                    errmsg("%s is redistributing, please retry later.", onerel->rd_rel->relname.data)));
        }
    } else if (vacuumPartition(vacstmt->flags) && !(vacstmt->options & VACOPT_NOWAIT)) {
        Assert(OidIsValid(relationid));

        /*
         * Coordinator needs guarantee that old select statement must finish when
         * run vacuum full table.
         */
        CNGuardOldQueryForVacuumFull(vacstmt, relationid);

        /* Open the partitioned table */
        onepartrel = try_relation_open(relationid, lmodePartTable);

        if (onepartrel) {
            if (!OidIsValid(subparentid)) { /* for a partition */
                /* Open the partition */
                onepart = tryPartitionOpen(onepartrel, relid, lmode);

                if (onepart) {
                    /* Get a Relation from a Partition */
                    onerel = partitionGetRelation(onepartrel, onepart);
                }
            } else { /* for a subpartition */
                onepart = tryPartitionOpen(onepartrel, subparentid, lmode);
                if (onepart) {
                    onesubpartrel = partitionGetRelation(onepartrel, onepart);
                    onesubpart = tryPartitionOpen(onesubpartrel, relid, lmode);
                    if (onesubpartrel && onesubpart) {
                        onerel = partitionGetRelation(onesubpartrel, onesubpart);
                    }
                }
            }
        }

        GetLock = true;
    } else if (vacuumMainPartition(vacstmt->flags) && !(vacstmt->options & VACOPT_NOWAIT)) {
        /*
         * Coordinator needs guarantee the old select statement must finish when
         * run vacuum full table
         */
        CNGuardOldQueryForVacuumFull(vacstmt, relid);

        onerel = try_relation_open(relid, lmode);
        GetLock = true;
        /*
         * We block vacuum operation while the target table is in redistribution
         * read only mode. For redistribution IUD mode, we will block vacuum before
         * coming to this point.
         */
        if (!u_sess->attr.attr_sql.enable_cluster_resize && onerel != NULL &&
            RelationInClusterResizingReadOnly(onerel)) {
            ereport(ERROR,
                (errcode(ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                    errmsg("%s is redistributing, please retry later.", onerel->rd_rel->relname.data)));
        }
    } else if (vacuumRelation(vacstmt->flags) && ConditionalLockRelationOid(relid, lmode)) {
        if (relid > FirstNormalObjectId && !checkGroup(relid, true)) {
            proc_snapshot_and_transaction();
            return false;
        }
        Assert(!(vacstmt->options & VACOPT_FULL));
        onerel = try_relation_open(relid, NoLock);
        GetLock = true;
    } else if (vacuumPartition(vacstmt->flags) && ConditionalLockRelationOid(relationid, lmodePartTable)) {
        PartitionIdentifier* partID = NULL;

        Assert(!(vacstmt->options & VACOPT_FULL));

        /* Open the partitioned table */
        onepartrel = try_relation_open(relationid, NoLock);

        if (onepartrel) {
            if (!OidIsValid(subparentid)) { /* for a partition */
                if (onepartrel->rd_rel->relkind == RELKIND_RELATION) {
                    partID = partOidGetPartID(onepartrel, relid);
                    if (PART_AREA_RANGE == partID->partArea ||
                        partID->partArea == PART_AREA_INTERVAL ||
                        PART_AREA_LIST == partID->partArea ||
                        PART_AREA_HASH == partID->partArea) {
                        if (ConditionalLockPartition(onepartrel->rd_id, relid, lmode, PARTITION_LOCK)) {
                            GetLock = true;
                        }
                    }
                    // remember to free partID
                    pfree_ext(partID);
                } else if (onepartrel->rd_rel->relkind == RELKIND_INDEX) {
                    if (ConditionalLockPartition(onepartrel->rd_id, relid, lmode, PARTITION_LOCK)) {
                        GetLock = true;
                    }
                }

                if (GetLock) {
                    /* Open the partition */
                    onepart = tryPartitionOpen(onepartrel, relid, NoLock);

                    /* Get a Relation from a Partition */
                    if (onepart)
                        onerel = partitionGetRelation(onepartrel, onepart);
                }
            } else { /* for a subpartition */
                PartitionIdentifier* subpartID = NULL;

                partID = partOidGetPartID(onepartrel, subparentid);
                if (ConditionalLockPartition(onepartrel->rd_id, subparentid, lmode, PARTITION_LOCK)) {
                    onepart = tryPartitionOpen(onepartrel, subparentid, NoLock);
                    if (onepart) {
                        onesubpartrel = partitionGetRelation(onepartrel, onepart);
                        subpartID = partOidGetPartID(onesubpartrel, relid);
                        if (ConditionalLockPartition(onesubpartrel->rd_id, relid, lmode, PARTITION_LOCK)) {
                            GetLock = true;
                        }
                    }
                }

                if (GetLock) {
                    onesubpart = tryPartitionOpen(onesubpartrel, relid, NoLock);
                    if (onesubpart)
                        onerel = partitionGetRelation(onesubpartrel, onesubpart);
                }
            }
        } else
            GetLock = true;
    } else if (vacuumMainPartition(vacstmt->flags) && ConditionalLockRelationOid(relationid, lmodePartTable)) {
        Assert(!(vacstmt->options & VACOPT_FULL));
        onerel = try_relation_open(relid, NoLock);
        GetLock = true;
    }

    if (!GetLock) {
        onerel = NULL;

        if (vacstmt->options & VACOPT_VERBOSE)
            messageLevel = VERBOSEMESSAGE;
        else
            messageLevel = LOG;

        if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0)
            ereport(messageLevel,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                    errmsg("skipping vacuum of \"%s\" --- lock not available", vacstmt->relation->relname)));
    }

    if (!onerel) {
        if (onesubpart != NULL) {
            partitionClose(onesubpartrel, onesubpart, lmode);
        }
        if (onesubpartrel != NULL) {
            releaseDummyRelation(&onesubpartrel);
        }
        if (onepart != NULL)
            partitionClose(onepartrel, onepart, lmode);
        if (onepartrel != NULL)
            relation_close(onepartrel, lmodePartTable);

        proc_snapshot_and_transaction();
        return false;
    }

    /*
     * Since vacuum full starts one new transaction for each to-be-vacuumed
     * relation, we should reacquire the DDL lock each time.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsNormalProcessingMode() && (vacstmt->options & VACOPT_FULL))
        pgxc_lock_for_utility_stmt(NULL, RelationIsLocalTemp(onerel));

    // Try to open CUDescRel and DeltaRel if needed
    if (!(vacstmt->options & VACOPT_NOWAIT))
        TryOpenCStoreInternalRelation(onerel, lmode, cudescrel, deltarel);
    else {
        Oid cudescrelid = RelationGetCUDescRelId(onerel);
        Oid deltarelid = RelationGetDeltaRelId(onerel);
        if (cudescrelid != InvalidOid && ConditionalLockRelationOid(cudescrelid, lmode))
            cudescrel = try_relation_open(cudescrelid, NoLock);
        if (deltarelid != InvalidOid && ConditionalLockRelationOid(deltarelid, lmode))
            deltarel = try_relation_open(deltarelid, NoLock);
    }

#define CloseAllRelationsBeforeReturnFalse()                      \
    do {                                                          \
        if (OidIsValid(subparentid)) {                            \
            releaseDummyRelation(&onerel);                        \
            if (onesubpart != NULL) {                             \
                partitionClose(onesubpartrel, onesubpart, lmode); \
            }                                                     \
            if (onesubpartrel != NULL) {                          \
                releaseDummyRelation(&onesubpartrel);             \
            }                                                     \
            if (onepart != NULL) {                                \
                partitionClose(onepartrel, onepart, lmode);       \
            }                                                     \
            if (onepartrel != NULL) {                             \
                relation_close(onepartrel, lmodePartTable);       \
            }                                                     \
        } else if (RelationIsPartition(onerel)) {                 \
            releaseDummyRelation(&onerel);                        \
            if (onepart != NULL) {                                \
                partitionClose(onepartrel, onepart, lmode);       \
            }                                                     \
            if (onepartrel != NULL) {                             \
                relation_close(onepartrel, lmodePartTable);       \
            }                                                     \
        } else {                                                  \
            relation_close(onerel, lmode);                        \
        }                                                         \
        if (cudescrel != NULL)                                    \
            relation_close(cudescrel, lmode);                     \
        if (deltarel != NULL)                                     \
            relation_close(deltarel, lmode);                      \
    } while (0)

    // We don't do vaccum when the table is in redistribution.
    // For read only redistribution case, even if the target table
    // will not be changed during this redistribution period,
    // it might have performance impact on data movement so we block
    // it here.
    if (RelationInClusterResizing(onerel) || RelationInClusterResizingReadOnly(onerel)) {
        CloseAllRelationsBeforeReturnFalse();
        proc_snapshot_and_transaction();
        return false;
    }

    /* 
     * We don't do vaccum on temp table create by pg_redis,
     * It will be deleted after redistribution.
     */
    Oid redis_ns = get_namespace_oid("data_redis", true);
    if (OidIsValid(redis_ns) && onerel->rd_rel->relnamespace == redis_ns) {
        ereport(LOG, (errmsg("skip vacuum for temp table : %s create by pg_redis", onerel->rd_rel->relname.data)));
        CloseAllRelationsBeforeReturnFalse();
        proc_snapshot_and_transaction();
        return false;
    }

    // If the internal relation of CStore relation open failed, we should return false
    //
    if ((RelationGetCUDescRelId(onerel) != InvalidOid && (cudescrel == NULL)) ||
        (RelationGetDeltaRelId(onerel) != InvalidOid && (deltarel == NULL))) {
        CloseAllRelationsBeforeReturnFalse();
        proc_snapshot_and_transaction();
        return false;
    }

    /*
     * Check permissions.
     *
     * We allow the user to vacuum a table if he is superuser, the table
     * owner, or the database owner (but in the latter case, only if it's not
     * a shared relation).	pg_class_ownercheck includes the superuser case.
     *
     * Note we choose to treat permissions failure as a WARNING and keep
     * trying to vacuum the rest of the DB --- is this appropriate?
     */
    AclResult aclresult;
    if (OidIsValid(subparentid)) {
        aclresult = pg_class_aclcheck(onerel->grandparentId, GetUserId(), ACL_VACUUM);
    } else {
        aclresult = pg_class_aclcheck(RelationGetPgClassOid(onerel, (onepart != NULL)), GetUserId(), ACL_VACUUM);
    }
    if (aclresult != ACLCHECK_OK &&
        !(pg_class_ownercheck(RelationGetPgClassOid(onerel, (onepart != NULL)), GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !onerel->rd_rel->relisshared) ||
                (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        if (vacstmt->options & VACOPT_VERBOSE)
            messageLevel = VERBOSEMESSAGE;
        else
            messageLevel = WARNING;

        if (onerel->rd_rel->relisshared)
            ereport(messageLevel,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("skipping \"%s\" --- only system admin can vacuum it", RelationGetRelationName(onerel))));
        else if (onerel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
            ereport(messageLevel,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("skipping \"%s\" --- only system admin or database owner can vacuum it",
                        RelationGetRelationName(onerel))));
        else
            ereport(messageLevel,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
                        RelationGetRelationName(onerel))));

        CloseAllRelationsBeforeReturnFalse();

        proc_snapshot_and_transaction();
        return false;
    }

    /*
     * Check that it's a vacuumable relation; we used to do this in
     * get_rel_oids() but seems safer to check after we've locked the
     * relation.
     */

    bool isPartitionedUHeap = RelationIsUstoreFormat(onerel) && RelationIsPartitioned(onerel);
    if (onerel->rd_rel->relkind != RELKIND_RELATION &&
#ifdef ENABLE_MOT
        !(RelationIsForeignTable(onerel) && isMOTFromTblOid(onerel->rd_id)) &&
#endif
        onerel->rd_rel->relkind != RELKIND_MATVIEW &&
        onerel->rd_rel->relkind != RELKIND_TOASTVALUE && !isPartitionedUHeap) {

        if (vacstmt->options & VACOPT_VERBOSE)
            messageLevel = VERBOSEMESSAGE;
        else
            messageLevel = WARNING;

        ereport(messageLevel,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("skipping \"%s\" --- cannot vacuum non-tables or special system tables",
                    RelationGetRelationName(onerel))));

        CloseAllRelationsBeforeReturnFalse();

        proc_snapshot_and_transaction();
        return false;
    }

    /*
     * Silently ignore tables that are temp tables of other backends ---
     * trying to vacuum these will lead to great unhappiness, since their
     * contents are probably not up-to-date on disk.  (We don't throw a
     * warning here; it would just lead to chatter during a database-wide
     * VACUUM.)
     */
    if (RELATION_IS_OTHER_TEMP(onerel)) {
        CloseAllRelationsBeforeReturnFalse();

        proc_snapshot_and_transaction();
        return false;
    }

    if (RELATION_IS_GLOBAL_TEMP(onerel) &&
        !gtt_storage_attached(RelationGetRelid(onerel))) {
        CloseAllRelationsBeforeReturnFalse();
        proc_snapshot_and_transaction();
        return false;
    }

    /*
     * Get a session-level lock too. This will protect our access to the
     * relation across multiple transactions, so that we can vacuum the
     * relation's TOAST table (if any) secure in the knowledge that no one is
     * deleting the parent relation.
     *
     * NOTE: this cannot block, even if someone else is waiting for access,
     * because the lock manager knows that both lock requests are from the
     * same process.
     */
    onerelid = onerel->rd_lockInfo.lockRelId;

    if (OidIsValid(subparentid)) {
        isFakeRelation = true;
        partLockRelId = onepartrel->rd_lockInfo.lockRelId;
        LockRelationIdForSession(&partLockRelId, lmodePartTable);

        Assert(onerelid.relId == relid);
        Assert(OidIsValid(relationid));

        /* first lock the subpartition */
        subpartIdentifier = partOidGetPartID(onesubpartrel, relid);
        subpartIdtf = *subpartIdentifier;
        LockPartitionVacuumForSession(subpartIdentifier, subparentid, relid, lmode);

        /* then lock the partition */
        partIdentifier = partOidGetPartID(onepartrel, subparentid);
        LockPartitionVacuumForSession(partIdentifier, relationid, subparentid, lmode);
        partIdtf = *partIdentifier;

        pfree_ext(subpartIdentifier);
        pfree_ext(partIdentifier);
    } else if (RelationIsPartition(onerel)) {
        isFakeRelation = true;
        partLockRelId = onepartrel->rd_lockInfo.lockRelId;
        LockRelationIdForSession(&partLockRelId, lmodePartTable);

        Assert(onerelid.relId == relid);
        Assert(OidIsValid(relationid));
        partIdentifier = partOidGetPartID(onepartrel, relid);
        partIdtf = *partIdentifier;
        LockPartitionVacuumForSession(partIdentifier, relationid, relid, lmode);
        pfree_ext(partIdentifier);
    } else {
        LockRelationIdForSession(&onerelid, lmode);
    }

    if (cudescrel) {
        cudescLockRelid = cudescrel->rd_lockInfo.lockRelId;
        LockRelationIdForSession(&cudescLockRelid, lmode);
    }

    if (deltarel) {
        deltaLockRelid = deltarel->rd_lockInfo.lockRelId;
        LockRelationIdForSession(&deltaLockRelid, lmode);
    }

    /*
     * Remember the relation's TOAST relation for later, if the caller asked
     * us to process it.  In VACUUM FULL, though, the toast table is
     * automatically rebuilt by cluster_rel so we shouldn't recurse to it.
     */
    if (do_toast && !(vacstmt->options & VACOPT_FULL)) {
        if (OidIsValid(subparentid)) {
            toast_relid = onesubpart->pd_part->reltoastrelid;
        } else if (RelationIsPartition(onerel)) {
            toast_relid = onepart->pd_part->reltoastrelid;
        } else {
            toast_relid = onerel->rd_rel->reltoastrelid;
        }
    } else {
        toast_relid = InvalidOid;
    }

    /* Matview and basetable don't support vacuum full, and matview can never be created on partition table */
    maplogOid = InvalidOid;
    if (doMapLog && !(vacstmt->options & VACOPT_FULL) && !RelationIsPartition(onerel)) {
        if (is_incremental_matview(relid)) {
            maplogOid = DatumGetObjectId(get_matview_mapid(relid));
        } else {
            maplogOid = onerel->rd_mlogoid;
        }
    }


    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command. (This is
     * unnecessary, but harmless, for lazy VACUUM.)
     */
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(onerel->rd_rel->relowner, save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();

    /* workload client manager */
    if (IS_PGXC_COORDINATOR && ENABLE_WORKLOAD_CONTROL && !IsAnyAutoVacuumProcess()) {
        AdaptMem* memUsage = &vacstmt->memUsage;

        /* if operatorMem is already set, the mem check is already done */
        if (memUsage->work_mem == 0) {
            UtilityDesc desc;
            errno_t rc = memset_s(&desc, sizeof(UtilityDesc), 0, sizeof(UtilityDesc));
            Relation rel = onerel;
            securec_check(rc, "\0", "\0");

            if (vacstmt->options & VACOPT_FULL) {
                if (vacuumPartition(vacstmt->flags)) {
                    if (OidIsValid(subparentid)) {
                        rel = onesubpartrel;
                    } else {
                        rel = onepartrel;
                    }
                }

                if (rel->rd_rel->relhasclusterkey) {
                    EstIdxMemInfo(rel, NULL, &desc, NULL, NULL);
                }

                /* for vacuum database or rel has index, we give a rough estimation */
                List* indexIds = RelationGetIndexList(rel);
                if (vacstmt->relation == NULL || indexIds != NIL) {
                    bool use_tenant = false;
                    int available_mem = 0;
                    int max_mem = 0;
                    dywlm_client_get_memory_info(&max_mem, &available_mem, &use_tenant);

                    if (desc.assigned_mem == 0) {
                        desc.assigned_mem = max_mem;
                    }
                    desc.cost = g_instance.cost_cxt.disable_cost;
                    desc.query_mem[0] = Max(STATEMENT_MIN_MEM * 1024, desc.query_mem[0]);
                    /* We use assigned variable if query_mem is set */
                    if (VALID_QUERY_MEM()) {
                        desc.query_mem[0] = Max(desc.query_mem[0], max_mem);
                        desc.query_mem[1] = Max(desc.query_mem[1], max_mem);
                    }
                }
                if (indexIds != NIL)
                    list_free(indexIds);
            }
            WLMInitQueryPlan((QueryDesc*)&desc, false);
            dywlm_client_manager((QueryDesc*)&desc, false);
            AdjustIdxMemInfo(memUsage, &desc);
        }
    }

    /*
     * Do the actual work --- either FULL or "lazy" vacuum
     */
    if (OidIsValid(subparentid)) {
        vacstmt->parentpartrel = onepartrel;
        vacstmt->parentpart = onepart;
        vacstmt->onepartrel = onesubpartrel;
        vacstmt->onepart = onesubpart;
        vacstmt->issubpartition = true;
    } else if (RelationIsPartition(onerel)) {
        vacstmt->onepartrel = onepartrel;
        vacstmt->onepart = onepart;
        vacstmt->parentpartrel = NULL;
        vacstmt->parentpart = NULL;
        vacstmt->issubpartition = false;
    }

    WaitState oldStatus = pgstat_report_waitstatus_relname(STATE_VACUUM, get_nsp_relname(relid));
#ifdef ENABLE_MOT
    if (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
        FdwRoutine* fdwroutine =  GetFdwRoutineForRelation(onerel, false);

        if (fdwroutine != NULL && fdwroutine->VacuumForeignTable != NULL) {
            fdwroutine->VacuumForeignTable(vacstmt, onerel);
        } else {
            if (vacstmt->options & VACOPT_VERBOSE)
                messageLevel = VERBOSEMESSAGE;
            else
                messageLevel = WARNING;

            ereport(messageLevel, (errmsg("skipping \"%s\" --- foreign table does not support vacuum",
                    RelationGetRelationName(onerel))));
        }
    } else if ((vacstmt->options & VACOPT_FULL) && (vacstmt->flags & VACFLG_SIMPLE_HEAP)) {
#else
    if ((vacstmt->options & VACOPT_FULL) && (vacstmt->flags & VACFLG_SIMPLE_HEAP)) {
#endif
        bool is_hdfs_rel = RelationIsPAXFormat(onerel);
        if (is_hdfs_rel) {
            ereport(LOG, (errmsg("vacuum full for DFS table: %s", onerel->rd_rel->relname.data)));
        }

        /* close relation before vacuuming, but hold lock until commit */
        if (cudescrel != NULL) {
            relation_close(cudescrel, NoLock);
            cudescrel = NULL;
        }
        if (deltarel != NULL) {
            relation_close(deltarel, NoLock);
            deltarel = NULL;
        }
        relation_close(onerel, NoLock);
        onerel = NULL;
        pgstat_report_waitstatus_relname(STATE_VACUUM_FULL, get_nsp_relname(relid));

        if (is_hdfs_rel) {
            DfsVacuumFull(relid, vacstmt);
        } else {
            /* VACUUM FULL is now a variant of CLUSTER; see cluster.c */
            cluster_rel(relid,
                InvalidOid,
                InvalidOid,
                false,
                (vacstmt->options & VACOPT_VERBOSE) != 0,
                vacstmt->freeze_min_age,
                vacstmt->freeze_table_age,
                &vacstmt->memUsage,
                vacstmt->relation != NULL);
            /* Record changecsn when VACUUM FULL occur */
            Relation rel = RelationIdGetRelation(relid);
            UpdatePgObjectChangecsn(relid, rel->rd_rel->relkind);
            RelationClose(rel);
        }
    } else if ((vacstmt->options & VACOPT_FULL) && (vacstmt->flags & VACFLG_SUB_PARTITION)) {
        if (OidIsValid(subparentid)) {
            releaseDummyRelation(&onerel);
            partitionClose(onesubpartrel, onesubpart, NoLock);
            releaseDummyRelation(&onesubpartrel);
            partitionClose(onepartrel, onepart, NoLock);
            relation_close(onepartrel, NoLock);
        } else {
            releaseDummyRelation(&onerel);
            partitionClose(onepartrel, onepart, NoLock);
            relation_close(onepartrel, NoLock);
        }
        onerel = NULL;
        onepartrel = NULL;
        onepart = NULL;
        onesubpartrel = NULL;
        onesubpart = NULL;

        if (cudescrel != NULL) {
            relation_close(cudescrel, NoLock);
            cudescrel = NULL;
        }
        if (deltarel != NULL) {
            relation_close(deltarel, NoLock);
            deltarel = NULL;
        }

        /* VACUUM FULL is now a variant of CLUSTER; see cluster.c */
        pgstat_report_waitstatus_relname(STATE_VACUUM_FULL, get_nsp_relname(relid));
        vacuumFullPart(relid, vacstmt, vacstmt->freeze_min_age, vacstmt->freeze_table_age);
    } else if ((vacstmt->options & VACOPT_FULL) && (vacstmt->flags & VACFLG_MAIN_PARTITION)) {
        if (cudescrel != NULL) {
            relation_close(cudescrel, NoLock);
            cudescrel = NULL;
        }
        if (deltarel != NULL) {
            relation_close(deltarel, NoLock);
            deltarel = NULL;
        }
        relation_close(onerel, NoLock);
        onerel = NULL;

        pgstat_report_waitstatus_relname(STATE_VACUUM_FULL, get_nsp_relname(relid));
        GpiVacuumFullMainPartiton(relid);
        CBIVacuumFullMainPartiton(relid);
        pgstat_report_vacuum(relid, InvalidOid, false, 0);
    } else if (!(vacstmt->options & VACOPT_FULL)) {
        /* clean hdfs empty directories of value partition just on main CN */
        if (vacstmt->options & VACOPT_HDFSDIRECTORY) {
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                DropEmptyPartitionDirectories(relid);
            }
        } else if (vacuumMainPartition((uint32)(vacstmt->flags))) {
            pgstat_report_waitstatus_relname(STATE_VACUUM, get_nsp_relname(relid));
            if (isPartitionedUHeap) {
                UstoreVacuumMainPartitionGPIs(onerel, vacstmt, lmode, vac_strategy);
            } else {
                GPIVacuumMainPartition(onerel, vacstmt, lmode, vac_strategy);
                CBIVacuumMainPartition(onerel, vacstmt, lmode, vac_strategy);
            }
            pgstat_report_vacuum(relid, InvalidOid, false, 0);
        } else {
            pgstat_report_waitstatus_relname(STATE_VACUUM, get_nsp_relname(relid));
            TableRelationVacuum(onerel, vacstmt, vac_strategy);
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* all done with this class, but hold lock until commit */
    if (onerel) {
        if (RelationIsPartition(onerel)) {
            if (OidIsValid(subparentid)) {
                releaseDummyRelation(&onerel);
                partitionClose(onesubpartrel, onesubpart, NoLock);
                releaseDummyRelation(&onesubpartrel);
                partitionClose(onepartrel, onepart, NoLock);
                relation_close(onepartrel, NoLock);
            } else {
                releaseDummyRelation(&onerel);
                partitionClose(onepartrel, onepart, NoLock);
                relation_close(onepartrel, NoLock);
            }
        } else {
            relation_close(onerel, NoLock);
        }
    }

    if (cudescrel != NULL)
        relation_close(cudescrel, NoLock);
    if (deltarel != NULL)
        relation_close(deltarel, NoLock);

    /*
     * Complete the transaction and free all temporary memory used.
     */
    PopActiveSnapshot();
    CommitTransactionCommand();

    /*
     * If the relation has a secondary toast rel, vacuum that too while we
     * still hold the session lock on the master table.  Note however that
     * "analyze" will not get done on the toast table.	This is good, because
     * the toaster always uses hardcoded index access and statistics are
     * totally unimportant for toast relations.
     */
    if (toast_relid != InvalidOid) {
        vacstmt->flags = VACFLG_SIMPLE_HEAP;
        vacstmt->onepartrel = NULL;
        vacstmt->onepart = NULL;
        vacstmt->parentpartrel = NULL;
        vacstmt->parentpart = NULL;
        vacstmt->issubpartition = false;
        (void)vacuum_rel(toast_relid, vacstmt, false);
    }

    if (maplogOid != InvalidOid) {
        vacstmt->flags = VACFLG_SIMPLE_HEAP;
        vacstmt->onepartrel = NULL;
        vacstmt->onepart = NULL;
        vacstmt->parentpartrel = NULL;
        vacstmt->parentpart = NULL;
        vacstmt->issubpartition = false;
        (void)vacuum_rel(maplogOid, vacstmt, false);
    }

    /*
     * Now release the session-level lock on the master table.
     */
    if (!LockRelIdIsInvalid(cudescLockRelid))
        UnlockRelationIdForSession(&cudescLockRelid, lmode);

    if (!LockRelIdIsInvalid(deltaLockRelid))
        UnlockRelationIdForSession(&deltaLockRelid, lmode);

    if (isFakeRelation) {
        Assert(onerelid.relId == relid && OidIsValid(relationid));
        if (OidIsValid(subparentid)) {
            UnLockPartitionVacuumForSession(&subpartIdtf, subparentid, relid, lmode);

            UnLockPartitionVacuumForSession(&partIdtf, relationid, subparentid, lmode);

            UnlockRelationIdForSession(&partLockRelId, lmodePartTable);
        } else {
            UnLockPartitionVacuumForSession(&partIdtf, relationid, relid, lmode);

            UnlockRelationIdForSession(&partLockRelId, lmodePartTable);
        }
    } else {
        UnlockRelationIdForSession(&onerelid, lmode);
    }

    /* Report that we really did it. */
    gstrace_exit(GS_TRC_ID_vacuum_rel);
    return true;
}

/*
 * Open all the vacuumable indexes of the given relation, obtaining the
 * specified kind of lock on each.	Return an array of Relation pointers for
 * the indexes into *Irel, and the number of indexes into *nindexes.
 *
 * We consider an index vacuumable if it is marked insertable (IndexIsReady).
 * If it isn't, probably a CREATE INDEX CONCURRENTLY command failed early in
 * execution, and what we have is too corrupt to be processable.  We will
 * vacuum even if the index isn't indisvalid; this is important because in a
 * unique index, uniqueness checks will be performed anyway and had better not
 * hit dangling index pointers.
 */
void vac_open_indexes(Relation relation, LOCKMODE lockmode, int* nindexes, Relation** Irel)
{
    List* indexoidlist = NIL;
    ListCell* indexoidscan = NULL;
    int i;

    Assert(lockmode != NoLock);

    indexoidlist = RelationGetIndexList(relation);

    /* allocate enough memory for all indexes */
    i = list_length(indexoidlist);
    
    if (i > 0)
        *Irel = (Relation*)palloc(i * sizeof(Relation));
    else
        *Irel = NULL;

    /* collect just the ready indexes */
    i = 0;
    foreach (indexoidscan, indexoidlist) {
        Oid indexoid = lfirst_oid(indexoidscan);
        Relation indrel;

        indrel = index_open(indexoid, lockmode);
        if (IndexIsReady(indrel->rd_index) && IndexIsUsable(indrel->rd_index))
            (*Irel)[i++] = indrel;
        else
            index_close(indrel, lockmode);
    }

    *nindexes = i;

    list_free(indexoidlist);
}

/*
 * Release the resources acquired by vac_open_indexes.	Optionally release
 * the locks (say NoLock to keep 'em).
 */
void vac_close_indexes(int nindexes, Relation* Irel, LOCKMODE lockmode)
{
    if (Irel == NULL)
        return;

    while (nindexes--) {
        Relation ind = Irel[nindexes];

        index_close(ind, lockmode);
    }
    pfree_ext(Irel);
}

/*
 * vacuum_delay_point --- check for interrupts and cost-based delay.
 *
 * This should be called in each major loop of VACUUM processing,
 * typically once per page processed.
 */
void vacuum_delay_point(void)
{
    /* Always check for interrupts */
    CHECK_FOR_INTERRUPTS();

    /* Nap if appropriate */
    if (t_thrd.vacuum_cxt.VacuumCostActive && !InterruptPending &&
        t_thrd.vacuum_cxt.VacuumCostBalance >= u_sess->attr.attr_storage.VacuumCostLimit) {
        int msec;

        msec = u_sess->attr.attr_storage.VacuumCostDelay * t_thrd.vacuum_cxt.VacuumCostBalance /
               u_sess->attr.attr_storage.VacuumCostLimit;
        if (msec > u_sess->attr.attr_storage.VacuumCostDelay * 4)
            msec = u_sess->attr.attr_storage.VacuumCostDelay * 4;

        pg_usleep(msec * 1000L);

        t_thrd.vacuum_cxt.VacuumCostBalance = 0;

        /* update balance values for workers */
        AutoVacuumUpdateDelay();

        /* Might have gotten an interrupt while sleeping */
        CHECK_FOR_INTERRUPTS();
    }
}

void vac_update_partstats(Partition part, BlockNumber num_pages, double num_tuples, BlockNumber num_all_visible_pages,
    TransactionId frozenxid, MultiXactId minmulti)
{
    Oid partid = PartitionGetPartid(part);
    Relation rd;
    HeapTuple parttup;
    HeapTuple nparttup = NULL;
    Form_pg_partition partform;
    bool dirty = false;
    bool isNull = false;
    TransactionId relfrozenxid;
    Datum xid64datum;
    MultiXactId relminmxid = MaxMultiXactId;

    rd = heap_open(PartitionRelationId, RowExclusiveLock);

    /* Fetch a copy of the tuple to scribble on */
    parttup = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partid));
    if (!HeapTupleIsValid(parttup)) {
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("pg_partition entry for partid %u vanished during vacuuming", partid)));
    }
    partform = (Form_pg_partition)GETSTRUCT(parttup);

    /* Apply required updates, if any, to copied tuple */
    dirty = false;
#ifdef PGXC
    // frozenxid == BootstrapTransactionId means it was invoked by execRemote.cpp:ReceivePageAndTuple()
    if (IS_PGXC_DATANODE || (frozenxid == BootstrapTransactionId)) {
#endif
        if (partform->relpages - (float8)num_pages != 0) {
            partform->relpages = (float8)num_pages;
            dirty = true;
        }
        if (partform->reltuples - (float8)num_tuples != 0) {
            partform->reltuples = (float8)num_tuples;
            dirty = true;
        }
        if (partform->relallvisible != (int32)num_all_visible_pages) {
            partform->relallvisible = (int32)num_all_visible_pages;
            dirty = true;
        }
#ifdef PGXC
    }
#endif
    /*
     * relfrozenxid should never go backward.  Caller can pass
     * InvalidTransactionId if it has no new data.
     */
    xid64datum = tableam_tops_tuple_getattr(parttup, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rd), &isNull);

    if (isNull) {
        relfrozenxid = partform->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

#ifndef ENABLE_MULTIPLE_NODES
    Datum minmxidDatum = tableam_tops_tuple_getattr(parttup, Anum_pg_partition_relminmxid,
        RelationGetDescr(rd), &isNull);
    relminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(minmxidDatum);
#endif

    if ((TransactionIdIsNormal(frozenxid) && TransactionIdPrecedes(relfrozenxid, frozenxid)) ||
        (MultiXactIdIsValid(minmulti) && MultiXactIdPrecedes(relminmxid, minmulti))) {
        Datum values[Natts_pg_partition];
        bool nulls[Natts_pg_partition];
        bool replaces[Natts_pg_partition];
        errno_t rc;

        partform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "", "");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "", "");

        if (TransactionIdIsNormal(frozenxid) && TransactionIdPrecedes(relfrozenxid, frozenxid)) {
            replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
            values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenxid);
        }

#ifndef ENABLE_MULTIPLE_NODES
        if (MultiXactIdIsValid(minmulti) && MultiXactIdPrecedes(relminmxid, minmulti)) {
            replaces[Anum_pg_partition_relminmxid - 1] = true;
            values[Anum_pg_partition_relminmxid - 1] = TransactionIdGetDatum(minmulti);
        }
#endif

        nparttup = (HeapTuple) tableam_tops_modify_tuple(parttup, RelationGetDescr(rd), values, nulls, replaces);
        parttup = nparttup;
        dirty = true;
    }

    /* If anything changed, write out the tuple. */
    if (dirty) {
        if (isNull && nparttup) {
            simple_heap_update(rd, &parttup->t_self, parttup);
            CatalogUpdateIndexes(rd, parttup);
        } else
            heap_inplace_update(rd, parttup);

        if (nparttup)
            heap_freetuple(nparttup);
    }
    heap_close(rd, RowExclusiveLock);
}

static void vac_open_global_indexs(
    List* globIndOidList, LOCKMODE lockmode, int* nindexes, int* nGlobalIndexes, Relation** Irel)
{
    ListCell* globIndCell = NULL;
    Relation indrel = NULL;
    int j = 0;
    int i = *nindexes;

    // collect ready global partion indexes
    foreach (globIndCell, globIndOidList) {
        Oid globIndOid = lfirst_oid(globIndCell);
        indrel = index_open(globIndOid, lockmode);
        Assert(indrel != NULL);
        if (IndexIsReady(indrel->rd_index) && IndexIsUsable(indrel->rd_index)) {
            (*Irel)[i + j] = indrel;
            j++;
        } else {
            index_close(indrel, lockmode);
        }
    }
    *nGlobalIndexes = j;
    *nindexes += j;
}

static void vac_open_local_indexs(List* localIndOidList, LOCKMODE lockmode, int* nindexes, Relation** Irel,
    Relation** indexrel, Partition** indexpart)
{
    int i = 0;
    ListCell* localIndCell = NULL;
    Relation indrel = NULL;

    foreach (localIndCell, localIndOidList) {
        Oid localIndOid = lfirst_oid(localIndCell);
        /* Get the index partition's parent oid */
        Oid indexParentid = partid_get_parentid(localIndOid);
        indrel = relation_open(indexParentid, lockmode);
        Assert(indrel != NULL);

        /* Open the partition */
        Partition indpart = partitionOpen(indrel, localIndOid, lockmode);
        Relation indexPartRel = partitionGetRelation(indrel, indpart);

        if (IndexIsReady(indrel->rd_index) && IndexIsReady(indexPartRel->rd_index) && IndexIsUsable(indrel->rd_index) &&
            indpart->pd_part->indisusable) {
            (*indexrel)[i] = indrel;
            (*indexpart)[i] = indpart;
            (*Irel)[i] = indexPartRel;
            ++i;
        } else {
            releaseDummyRelation(&indexPartRel);
            partitionClose(indrel, indpart, lockmode);
            relation_close(indrel, lockmode);
        }
    }
    *nindexes = i;
}

void vac_open_part_indexes(VacuumStmt* vacstmt, LOCKMODE lockmode, int* nindexes, int* nindexesGlobal, Relation** Irel,
    Relation** indexrel, Partition** indexpart)
{
    List* localIndOidList = NIL;
    List* globIndOidList = NIL;
    int localIndNums;
    int globIndNums;
    long tolIndNums;

    Assert(lockmode != NoLock);
    Assert(vacstmt->onepart != NULL);
    Assert(vacstmt->onepartrel != NULL);

    // get local partition indexes
    localIndOidList = PartitionGetPartIndexList(vacstmt->onepart);
    localIndNums = list_length(localIndOidList);
    // get global partition indexes
    if (!vacstmt->parentpartrel) {
        globIndOidList = RelationGetSpecificKindIndexList(vacstmt->onepartrel, true);
    } else {
        globIndOidList = RelationGetSpecificKindIndexList(vacstmt->parentpartrel, true);
    }
    globIndNums = list_length(globIndOidList);

    tolIndNums = (long)localIndNums + (long)globIndNums;
    if (tolIndNums > 0) {
        *Irel = (Relation*)palloc(tolIndNums * sizeof(Relation));
    } else {
        *Irel = NULL;
    }
    if (localIndNums > 0) {
        *indexrel = (Relation*)palloc(localIndNums * sizeof(Relation));
        *indexpart = (Partition*)palloc(localIndNums * sizeof(Partition));
    } else {
        *indexrel = NULL;
        *indexpart = NULL;
    }

    // collect ready local partition indexes
    vac_open_local_indexs(localIndOidList, lockmode, nindexes, Irel, indexrel, indexpart);

    // collect ready global partion indexes
    vac_open_global_indexs(globIndOidList, lockmode, nindexes, nindexesGlobal, Irel);

    list_free_ext(localIndOidList);
    list_free_ext(globIndOidList);
}

void vac_close_part_indexes(
    int nindexes, int nindexesGlobal, Relation* Irel, Relation* indexrel, Partition* indexpart, LOCKMODE lockmode)
{
    if (Irel == NULL) {
        return;
    }

    int nindexes_local = nindexes - nindexesGlobal;
    while (nindexes--) {
        Relation rel = Irel[nindexes];
        if (nindexes < nindexes_local) {
            // close local partition indexes
            Relation ind = indexrel[nindexes];
            Partition part = indexpart[nindexes];
            releaseDummyRelation(&rel);
            partitionClose(ind, part, lockmode);
            relation_close(ind, lockmode);
        } else {
            // close global partition indexes
            index_close(rel, lockmode);
        }
    }
    pfree_ext(indexrel);
    pfree_ext(indexpart);
    pfree_ext(Irel);
}

/* Scan pg_partition to get all the partitions of the partitioned table,
 * calculate all the pages, tuples, and the min frozenXid, multiXid
 */
void CalculatePartitionedRelStats(_in_ Relation partitionRel, _in_ Relation partRel, _out_ BlockNumber* totalPages,
    _out_ BlockNumber* totalVisiblePages, _out_ double* totalTuples, _out_ TransactionId* minFrozenXid,
    _out_ MultiXactId* minMultiXid)
{
    ScanKeyData partKey[2];
    BlockNumber pages = 0;
    BlockNumber allVisiblePages = 0;
    double tuples = 0;
    TransactionId frozenXid;
    MultiXactId multiXid = InvalidMultiXactId;
    Form_pg_partition partForm;

    Assert(partitionRel->rd_rel->parttype == PARTTYPE_PARTITIONED_RELATION ||
           partitionRel->rd_rel->parttype == PARTTYPE_SUBPARTITIONED_RELATION);

    if (partitionRel->rd_rel->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
        ScanKeyInit(&partKey[0],
            Anum_pg_partition_parttype,
            BTEqualStrategyNumber,
            F_CHAREQ,
            CharGetDatum(PART_OBJ_TYPE_TABLE_SUB_PARTITION));
    } else if (partitionRel->rd_rel->relkind == RELKIND_RELATION) {
        ScanKeyInit(&partKey[0],
            Anum_pg_partition_parttype,
            BTEqualStrategyNumber,
            F_CHAREQ,
            CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));
    } else if (partitionRel->rd_rel->relkind == RELKIND_INDEX) {
        ScanKeyInit(&partKey[0],
            Anum_pg_partition_parttype,
            BTEqualStrategyNumber,
            F_CHAREQ,
            CharGetDatum(PART_OBJ_TYPE_INDEX_PARTITION));
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unexpected relkind!")));
    }

    ScanKeyInit(&partKey[1],
        Anum_pg_partition_parentid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(partitionRel)));

    SysScanDesc partScan = systable_beginscan(partRel, PartitionParentOidIndexId, true, NULL, 2, partKey);

    HeapTuple partTuple = NULL;
    /* compute all pages, tuples and the minimum frozenXid, multiXid */
    partTuple = systable_getnext(partScan);
    if (partTuple != NULL) {
        partForm = (Form_pg_partition)GETSTRUCT(partTuple);

        bool isNull = false;
        TransactionId relfrozenxid;
        Relation rel = heap_open(PartitionRelationId, AccessShareLock);
        Datum xid64datum = tableam_tops_tuple_getattr(partTuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);

        if (isNull) {
            relfrozenxid = partForm->relfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                !TransactionIdIsNormal(relfrozenxid))
                relfrozenxid = FirstNormalTransactionId;
        } else {
            relfrozenxid = DatumGetTransactionId(xid64datum);
        }

        frozenXid = relfrozenxid;

#ifndef ENABLE_MULTIPLE_NODES
        xid64datum = tableam_tops_tuple_getattr(partTuple, Anum_pg_partition_relminmxid,
            RelationGetDescr(rel), &isNull);
        multiXid = isNull ? FirstMultiXactId : DatumGetTransactionId(xid64datum);
#endif

        do {
            partForm = (Form_pg_partition)GETSTRUCT(partTuple);

            /* calculate pages and tuples from all the partitioned. */
            pages += (uint32) partForm->relpages;
            allVisiblePages += partForm->relallvisible;
            tuples += partForm->reltuples;

            /* update the relfrozenxid */
            xid64datum = tableam_tops_tuple_getattr(partTuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);

            if (isNull) {
                relfrozenxid = partForm->relfrozenxid;

                if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                    !TransactionIdIsNormal(relfrozenxid))
                    relfrozenxid = FirstNormalTransactionId;
            } else
                relfrozenxid = DatumGetTransactionId(xid64datum);

            if (TransactionIdPrecedes(relfrozenxid, frozenXid)) {
                frozenXid = relfrozenxid;
            }

#ifndef ENABLE_MULTIPLE_NODES
            xid64datum = tableam_tops_tuple_getattr(partTuple, Anum_pg_partition_relminmxid,
                RelationGetDescr(rel), &isNull);
            MultiXactId relminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(xid64datum);

            if (TransactionIdPrecedes(relminmxid, multiXid)) {
                multiXid = relminmxid;
            }
#endif
        } while ((partTuple = systable_getnext(partScan)) != NULL);

        heap_close(rel, AccessShareLock);
    } else {
        frozenXid = InvalidTransactionId;
        multiXid = InvalidMultiXactId;
    }

    systable_endscan(partScan);

    if (totalPages != NULL)
        *totalPages = pages;
    if (totalVisiblePages != NULL)
        *totalVisiblePages = allVisiblePages;
    if (totalTuples != NULL)
        *totalTuples = tuples;
    if (minFrozenXid != NULL)
        *minFrozenXid = frozenXid;
    if (minMultiXid != NULL)
        *minMultiXid = multiXid;
}

/*
 * After VACUUM or ANALYZE, update pg_class for the partitioned tables.
 */
void vac_update_pgclass_partitioned_table(Relation partitionRel, bool hasIndex, TransactionId newFrozenXid,
                                          MultiXactId newMultiXid)
{
    BlockNumber pages = 0;
    BlockNumber allVisiblePages = 0;
    double tuples = 0;
    TransactionId frozenXid = newFrozenXid;
    MultiXactId multiXid = newMultiXid;

    Assert(partitionRel->rd_rel->parttype == PARTTYPE_PARTITIONED_RELATION ||
           partitionRel->rd_rel->parttype == PARTTYPE_SUBPARTITIONED_RELATION);

    /*
     * We should make CalculatePartitionedRelStats and vac_udpate_relstats in whole,
     * Use ShareUpdateExclusiveLock lock to block another vacuum thread to update pgclass partition table in parallel
     * Keep lock RelatitionRelation first, and then PartitionRelation to avoid dead lock
     */
    Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
    Relation partRel = heap_open(PartitionRelationId, ShareUpdateExclusiveLock);
    CalculatePartitionedRelStats(partitionRel, partRel, &pages, &allVisiblePages, &tuples, &frozenXid, &multiXid);
    vac_update_relstats(partitionRel, classRel, pages, tuples, allVisiblePages, hasIndex, frozenXid, multiXid);
    heap_close(partRel, ShareUpdateExclusiveLock);
    heap_close(classRel, RowExclusiveLock);
}

/* After VACUUM or ANALYZE, update pg_class for the partitioned cstore-tables. */
void CStoreVacUpdatePartitionRelStats(Relation partitionRel, TransactionId newFrozenXid)
{
    Assert(partitionRel->rd_rel->parttype == PARTTYPE_PARTITIONED_RELATION);

    TransactionId frozenXid = newFrozenXid;

    /*
     * We should make CalculatePartitionedRelStats and CStoreVacUpdateNormalRelStats in whole,
     * Use ShareUpdateExclusiveLock lock to block another vacuum thread to run CStoreVacUpdatePartitonRelStats in
     * parallel Keep lock RelatitionRelation first, and then PartitionRelation to avoid dead lock
     */
    Relation pgclassRel = heap_open(RelationRelationId, RowExclusiveLock);
    Relation pgPartitionRel = heap_open(PartitionRelationId, ShareUpdateExclusiveLock);
    CalculatePartitionedRelStats(partitionRel, pgPartitionRel, NULL, NULL, NULL, &frozenXid, NULL);
    CStoreVacUpdateNormalRelStats(RelationGetRelid(partitionRel), frozenXid, pgclassRel);
    heap_close(pgPartitionRel, ShareUpdateExclusiveLock);
    heap_close(pgclassRel, RowExclusiveLock);
}

void CStoreVacUpdateNormalRelStats(Oid relid, TransactionId frozenxid, Relation pgclassRel)
{

    /* Fetch a copy of the tuple to scribble on. */
    HeapTuple ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    HeapTuple nctup;
    if (!HeapTupleIsValid(ctup))
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("pg_class entry for relid %u vanished during vacuuming", relid)));

    Form_pg_class pgcform = (Form_pg_class)GETSTRUCT(ctup);

    /* relfrozenxid should never go backward, except in PGXC, when xid has gone
     * out-of-sync w.r.t. gxid and we want to correct it using standalone backend.
     * Caller can pass InvalidTransactionId if it has no new data.
     */
    bool isNull = false;
    TransactionId relfrozenxid;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    Datum xid64datum = tableam_tops_tuple_getattr(ctup, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
    heap_close(rel, AccessShareLock);

    if (isNull) {
        relfrozenxid = pgcform->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

    if (TransactionIdIsNormal(frozenxid) &&
        (TransactionIdPrecedes(relfrozenxid, frozenxid) || !IsPostmasterEnvironment)) {
        Datum values[Natts_pg_class];
        bool nulls[Natts_pg_class];
        bool replaces[Natts_pg_class];
        errno_t rc;

        pgcform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "", "");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "", "");

        replaces[Anum_pg_class_relfrozenxid64 - 1] = true;
        values[Anum_pg_class_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenxid);

        nctup = (HeapTuple) tableam_tops_modify_tuple(ctup, RelationGetDescr(pgclassRel), values, nulls, replaces);
        if (isNull) {
            simple_heap_update(pgclassRel, &nctup->t_self, nctup);
            CatalogUpdateIndexes(pgclassRel, nctup);
        } else {
            heap_inplace_update(pgclassRel, nctup);
        }

        heap_freetuple(nctup);
    }
}

void CStoreVacUpdatePartitionStats(Oid relid, TransactionId frozenxid)
{
    Relation rd = heap_open(PartitionRelationId, RowExclusiveLock);

    /* Fetch a copy of the tuple to scribble on */
    HeapTuple ctup = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(relid));
    HeapTuple ntup;
    if (!HeapTupleIsValid(ctup))
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("pg_class entry for relid %u vanished during vacuuming", relid)));

    Form_pg_partition pgcform = (Form_pg_partition)GETSTRUCT(ctup);

    /* relfrozenxid should never go backward, except in PGXC, when xid has gone
     * out-of-sync w.r.t. gxid and we want to correct it using standalone backend.
     * Caller can pass InvalidTransactionId if it has no new data.
     */
    bool isNull = false;
    TransactionId relfrozenxid;
    Datum xid64datum = tableam_tops_tuple_getattr(ctup, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rd), &isNull);

    if (isNull) {
        relfrozenxid = pgcform->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

    if (TransactionIdIsNormal(frozenxid) && TransactionIdPrecedes(relfrozenxid, frozenxid)) {
        Datum values[Natts_pg_partition];
        bool nulls[Natts_pg_partition];
        bool replaces[Natts_pg_partition];
        errno_t rc;

        pgcform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
        values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenxid);

        ntup = (HeapTuple) tableam_tops_modify_tuple(ctup, RelationGetDescr(rd), values, nulls, replaces);
        if (isNull) {
            simple_heap_update(rd, &ntup->t_self, ntup);
            CatalogUpdateIndexes(rd, ntup);
        } else {
            heap_inplace_update(rd, ntup);
        }

        heap_freetuple(ntup);
    }

    heap_close(rd, RowExclusiveLock);
}

/* get all partitions oid */
static List* get_part_oid(Oid relid)
{
    List* oid_list = NIL;
    Relation pgpartition;
    TableScanDesc scan;
    HeapTuple tuple;
    ScanKeyData keys[2];
    MemoryContext oldcontext = NULL;

    /* Process all partitions of this partitiond table */
    ScanKeyInit(&keys[0],
        Anum_pg_partition_parttype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

    ScanKeyInit(&keys[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    pgpartition = heap_open(PartitionRelationId, AccessShareLock);
    scan = tableam_scan_begin(pgpartition, SnapshotNow, 2, keys);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        if (t_thrd.vacuum_cxt.vac_context) {
            oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);        
        }
        oid_list = lappend_oid(oid_list, HeapTupleGetOid(tuple));
        if (t_thrd.vacuum_cxt.vac_context) {
            (void)MemoryContextSwitchTo(oldcontext);        
        }
    }
    heap_endscan(scan);
    heap_close(pgpartition, AccessShareLock);

    return oid_list;
}

/* read source tuples and insert into dest */
static void getTuplesAndInsert(Relation source, Oid dest_oid)
{
    /* open dest relation */
    Relation dest = relation_open(dest_oid, AccessExclusiveLock);
    TableScanDesc scan = tableam_scan_begin(source, SnapshotNow, 0, NULL);
    HeapTuple tuple = NULL;

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        HeapTuple copyTuple = NULL;

        copyTuple = (HeapTuple) tableam_tops_copy_tuple(tuple);

        (void)simple_heap_insert(dest, copyTuple);

        heap_freetuple(copyTuple);
    }

    tableam_scan_end(scan);
    /* close dest relation */
    if (dest != NULL)
        relation_close(dest, NoLock);
}

void merge_cu_relation(void* _info, VacuumStmt* stmt)
{
    MergeInfo* info = (MergeInfo*)_info;
    /* only use table oid */
    Oid rel_oid = info->oid;
    Relation rel = NULL;

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    CHECK_FOR_INTERRUPTS();

    /* vac_context is needed by getting parition oid */
    t_thrd.vacuum_cxt.vac_context = AllocSetContextCreate(t_thrd.mem_cxt.portal_mem_cxt,
        "Vacuum Deltamerge",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* open cstore relation with AccessExclusiveLock lock */
    rel = relation_open(rel_oid, AccessExclusiveLock);

    /* Silently ignore tables that are temp tables of other backends */
    if (RELATION_IS_OTHER_TEMP(rel)) {
        /* just close relation */
        if (NULL != rel)
            relation_close(rel, NoLock);
        PopActiveSnapshot();
        CommitTransactionCommand();
    } else if (RELATION_IS_PARTITIONED(rel)) {
        /* deal with patitioned table */
        List* oid_list = get_part_oid(rel_oid);
        if (rel != NULL)
            relation_close(rel, NoLock);
        PopActiveSnapshot();
        CommitTransactionCommand();

        ListCell* cell = NULL;
        foreach (cell, oid_list) {
            Oid part_oid = lfirst_oid(cell);
            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());
            CHECK_FOR_INTERRUPTS();
            Relation main_rel = NULL;
            Partition partrel = NULL;
            Relation onerel = NULL;
            main_rel = try_relation_open(rel_oid, AccessExclusiveLock); /* need to check */
            if (main_rel) {
                /* Open the partition */
                partrel = tryPartitionOpen(main_rel, part_oid, AccessExclusiveLock);
                if (partrel) {
                    /* Get a Relation from a Partition */
                    onerel = partitionGetRelation(main_rel, partrel);
                }
            }
            if (onerel != NULL) {
                stmt->onepartrel = main_rel;
                stmt->onepart = partrel;
                lazy_vacuum_rel(onerel, stmt, GetAccessStrategy(BAS_VACUUM));
                CommandCounterIncrement();

                /* get delta oid  */
                Oid deltaOid = partrel->pd_part->reldeltarelid;
                Relation delta_rel = relation_open(deltaOid, AccessExclusiveLock);
                if (delta_rel != NULL) {
                    Oid OIDNewHeap = make_new_heap(deltaOid, delta_rel->rd_rel->reltablespace, AccessExclusiveLock);

                    /* copy delta data to new heap */
                    getTuplesAndInsert(delta_rel, OIDNewHeap);

                    /* swap relfile node */
                    finish_heap_swap(deltaOid, OIDNewHeap, false, false, false, u_sess->utils_cxt.RecentGlobalXmin,
                                     InvalidMultiXactId);

                    /* close relation */
                    relation_close(delta_rel, NoLock);
                }

                releaseDummyRelation(&onerel);
                partitionClose(main_rel, partrel, NoLock);
                relation_close(main_rel, NoLock);
            } else {
                if (partrel != NULL && main_rel != NULL)
                    partitionClose(main_rel, partrel, NoLock);
                if (main_rel != NULL)
                    relation_close(main_rel, NoLock);
            }

            /*
             * Complete the transaction and free all temporary memory used.
             */
            PopActiveSnapshot();
            CommitTransactionCommand();
        }

        /* free oid_list */
        list_free(oid_list);
    } else {
        /* non-partition table */
        stmt->onepartrel = NULL;
        stmt->onepart = NULL;
        stmt->parentpartrel = NULL;
        stmt->parentpart = NULL;
        stmt->issubpartition = false;
        lazy_vacuum_rel(rel, stmt, GetAccessStrategy(BAS_VACUUM));
        CommandCounterIncrement();

        /* open delta realtion */
        Oid deltaOid = rel->rd_rel->reldeltarelid;
        Relation delta_rel = relation_open(deltaOid, AccessExclusiveLock);

        Oid OIDNewHeap = make_new_heap(deltaOid, delta_rel->rd_rel->reltablespace, AccessExclusiveLock);

        /* copy delta data to new heap */
        getTuplesAndInsert(delta_rel, OIDNewHeap);

        /* swap relfile node */
        finish_heap_swap(deltaOid, OIDNewHeap, false, false, false, u_sess->utils_cxt.RecentGlobalXmin,
                         InvalidMultiXactId);

        /* close relation */
        if (delta_rel != NULL)
            relation_close(delta_rel, NoLock);

        if (rel != NULL)
            relation_close(rel, NoLock);
        /*
         * Complete the transaction and free all temporary memory used.
         */
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    if (t_thrd.vacuum_cxt.vac_context) {
        MemoryContextDelete(t_thrd.vacuum_cxt.vac_context);
        t_thrd.vacuum_cxt.vac_context = NULL;
    }
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/*
 * call dfs::EstimateBufferRows to get upper limit of row count of delta merge
 */
static uint64 get_max_row(Oid relid)
{
    return DefaultFullCUSize;
}

/*
 * "vacuum deltamerge t1" will be splited into three SQLs in DN.
 */
static void make_real_queries(MergeInfo* info)
{
    StringInfo deltaName = makeStringInfo();
    Relation rel;
    Oid deltaOid;

    Assert(info);

    ereport(DEBUG1, (errmsg("deltamerge: %s()", __FUNCTION__)));

    rel = heap_open(info->oid, AccessShareLock);
    deltaOid = RelationGetDeltaRelId(rel);
    Relation deltaRel = relation_open(deltaOid, AccessShareLock);
    appendStringInfo(deltaName, "%s", RelationGetRelationName(deltaRel));

    relation_close(deltaRel, NoLock);
    heap_close(rel, NoLock);

    info->row_count_sql = makeStringInfo();
    info->merge_sql = makeStringInfo();
    info->vacuum_sql = makeStringInfo();

    appendStringInfo(info->row_count_sql, sql_templates[ROW_COUNT_SQL_TEMPLATE], deltaName->data);
    appendStringInfo(info->vacuum_sql, sql_templates[VACUUM_SQL_TEMPLATE], deltaName->data);

    if (info->schemaname == NULL)
        appendStringInfo(info->merge_sql,
            sql_templates[MERGE_SQL_TEMPLATE],
            info->relname->data,
            info->relname->data,
            deltaName->data,
            deltaName->data);
    else
        appendStringInfo(info->merge_sql,
            sql_templates[VACUUM_SQL_TEMPLATE_WITH_SCHEMA],
            info->schemaname->data,
            info->relname->data,
            info->schemaname->data,
            info->relname->data,
            deltaName->data,
            deltaName->data);

    pfree_ext(deltaName->data);
    pfree_ext(deltaName);
}

/*
 * Scan pg_class to determine which tables to merge.
 */
static List* get_tables_to_merge()
{
    HeapTuple tuple;
    TableScanDesc relScan;

    ScanKeyData key[1];
    Form_pg_class form;

    List* infos = NIL;

    ereport(DEBUG1, (errmsg("deltamerge: %s()", __FUNCTION__)));

    Relation classRel = heap_open(RelationRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_RELATION));

    relScan = tableam_scan_begin(classRel, SnapshotNow, 1, key);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(relScan, ForwardScanDirection)) != NULL) {
        form = (Form_pg_class)GETSTRUCT(tuple);
        Oid relid = HeapTupleGetOid(tuple);

        if (form->relnamespace == CSTORE_NAMESPACE)
            continue;

        ereport(DEBUG1,
            (errmsg("deltamerge, candidate relation: relid: %u, relname: %s, schemaid: %u",
                relid,
                form->relname.data,
                form->relnamespace)));

        bool is_hdfs = RelationIsPaxFormatByOid(relid);
        if (is_hdfs || RelationIsCUFormatByOid(relid)) {
            MergeInfo* info = (MergeInfo*)palloc0(sizeof(MergeInfo));

            info->oid = relid;
            info->max_row = get_max_row(info->oid);
            info->relname = makeStringInfo();
            info->is_hdfs = is_hdfs;
            appendStringInfo(info->relname, "%s", form->relname.data);

            char* schema_name = get_namespace_name(form->relnamespace);
            if (NULL == schema_name) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_SCHEMA_NAME), errmsg("Invalid schema oid: %u", form->relnamespace)));
            }
            info->schemaname = makeStringInfo();
            appendStringInfo(info->schemaname, "%s", schema_name);
            pfree_ext(schema_name);

            if (info->is_hdfs) {
                make_real_queries(info);
            }
            infos = lappend(infos, info);

            ereport(DEBUG1,
                (errmsg("deltamerge, chosen relation: relid: %u, relname: %s, schemaid: %u, max_row: %lu",
                    relid,
                    form->relname.data,
                    form->relnamespace,
                    info->max_row)));
        }

    } /* end while */

    heap_endscan(relScan);
    heap_close(classRel, AccessShareLock);

    return infos;
}

/*
 * merge_one_relation() will call exec_query_for_merge() for delta merge sql,
 */
void merge_one_relation(void* _info)
{
    uint64 row_count;
    CommandDest old_dest = (CommandDest)t_thrd.postgres_cxt.whereToSendOutput;

    MergeInfo* info = (MergeInfo*)_info;

    /*
     * select count(*) from pg_delta_xxx
     */
    PG_TRY();
    {
        t_thrd.postgres_cxt.whereToSendOutput = DestNone;
        exec_query_for_merge(info->row_count_sql->data);
    }
    PG_CATCH();
    {
        /* recover to old output config as early as possible */
        t_thrd.postgres_cxt.whereToSendOutput = old_dest;
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* recover to old output config as early as possible */
    t_thrd.postgres_cxt.whereToSendOutput = old_dest;

    row_count = (uint64)DatumGetInt64(t_thrd.postgres_cxt.query_result);

    ereport(DEBUG1,
        (errmsg("deltamerge, count sql: %s, result: %lu, max_row: %lu",
            info->row_count_sql->data,
            row_count,
            info->max_row)));

    /*
     * if data size of delta table is less than 60000 and cstore_insert_mode is
     * not main, ignore delta merge.
     */
    if (row_count == 0 || (row_count < info->max_row && u_sess->attr.attr_storage.cstore_insert_mode != TO_MAIN))
        return;

    ereport(DEBUG1, (errmsg("deltamerge, merge sql: %s", info->merge_sql->data)));

    /*
     * do real merge, move data from delta table to main table
     */
    exec_query_for_merge(info->merge_sql->data);
}

/*
 * main entry of "vacuum deltamerge [table_name]" on DN
 */
void begin_delta_merge(VacuumStmt* stmt)
{
    List* infos = NIL;
    RangeVar* var = stmt->relation;

    ereport(DEBUG1, (errmsg("deltamerge: %s()", __FUNCTION__)));
    ereport(DEBUG1, (errmsg("Oid of the current schema: %u", getCurrentNamespace())));

    /* "deltamerge" on other remote CNs do nothing except locking table. */
    if (IS_PGXC_COORDINATOR && IsConnFromCoord() && var) {
        Oid relid = RangeVarGetRelid(var, NoLock, false);
        if (InvalidOid == relid) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", var->relname)));
        }

        Relation rel = heap_open(relid, AccessExclusiveLock);
        heap_close(rel, NoLock);
        return;
    }

    if (var != NULL) {
        /*
         * merge ONE table
         */
        Oid relid = RangeVarGetRelid(var, NoLock, false);
        if (InvalidOid == relid) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", var->relname)));
        }

        bool is_hdfs = RelationIsPaxFormatByOid(relid);
        bool is_cstore = RelationIsCUFormatByOid(relid);

        if (false == is_hdfs && false == is_cstore) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("deltamerge: This relation doesn't support vacuum deltamerge operation")));
        }

        MergeInfo* info = (MergeInfo*)palloc0(sizeof(MergeInfo));

        info->oid = relid;
        info->max_row = get_max_row(relid);
        info->relname = makeStringInfo();
        info->is_hdfs = is_hdfs;
        appendStringInfo(info->relname, "%s", var->relname);

        info->schemaname = NULL;
        if (var->schemaname) {
            info->schemaname = makeStringInfo();
            appendStringInfo(info->schemaname, "%s", var->schemaname);
        }

        if (info->is_hdfs)
            make_real_queries(info);

        infos = lappend(infos, info);

    } else {
        /*
         * get all merge sqls
         */
        infos = get_tables_to_merge();
    }

    do_delta_merge(infos, stmt);

    /* free memory */
    free_merge_info(infos);
}

/*
 * release all memory in infos
 */
static void free_merge_info(List* infos)
{
    ListCell* lc = NULL;
    foreach (lc, infos) {
        MergeInfo* info = (MergeInfo*)lfirst(lc);
        if (info->row_count_sql) {
            pfree_ext(info->row_count_sql->data);
            pfree_ext(info->row_count_sql);
        }

        if (info->merge_sql) {
            pfree_ext(info->merge_sql->data);
            pfree_ext(info->merge_sql);
        }

        if (info->vacuum_sql) {
            pfree_ext(info->vacuum_sql->data);
            pfree_ext(info->vacuum_sql);
        }

        if (info->relname) {
            pfree_ext(info->relname->data);
            pfree_ext(info->relname);
        }

        if (info->schemaname) {
            pfree_ext(info->schemaname->data);
            pfree_ext(info->schemaname);
        }

        pfree_ext(info);
    }
}

bool equal_string(const void* _str1, const void* _str2)
{
    StringInfo str1 = (StringInfo)_str1;
    StringInfo str2 = (StringInfo)_str2;
    if (pg_strcasecmp(str1->data, str2->data))
        return false;
    else
        return true;
}

bool IsMember(const List* list, const void* datum, EqualFunc fn)
{
    const ListCell* cell = NULL;

    foreach (cell, list) {
        if ((*fn)(lfirst(cell), datum))
            return true;
    }

    return false;
}

/*
 * get all items which is in list1 and not in list2
 */
List* GetDifference(const List* list1, const List* list2, EqualFunc fn)
{
    List* result = NIL;

    if (list2 == NIL)
        return list_copy(list1);

    const ListCell* cell = NULL;
    foreach (cell, list1) {
        if (!IsMember(list2, lfirst(cell), fn))
            result = lappend(result, lfirst(cell));
    }

    return result;
}

/*
 * remove garbase files in the directory of the relation on hdfs, the garbase
 * files can be made because of other operation, such as the interrupt of insert.
 */
void RemoveGarbageFiles(Relation rel, DFSDescHandler* handler)
{
    ListCell* lc = NULL;

    /*
     * get connection object and will be used in doPendingDfsDelete()
     */
    StringInfo store_path = getDfsStorePath(rel);
    DfsSrvOptions* dfsoptions = GetDfsSrvOptions(rel->rd_rel->reltablespace);
    dfs::DFSConnector* conn = dfs::createConnector(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), dfsoptions, rel->rd_rel->reltablespace);

    /*
     * get files in desc table
     */
    List* descs = handler->GetAllDescs(SnapshotNow);
    List* files_in_desc = NIL;
    foreach (lc, descs) {
        DFSDesc* desc = (DFSDesc*)lfirst(lc);

        StringInfo fpath = makeStringInfo();
        appendStringInfo(fpath, "%s", desc->GetFileName());

        files_in_desc = lappend(files_in_desc, fpath);
        desc->Destroy();
        pfree_ext(desc);
    }
    list_free(descs);

    /*
     * get files in dir and remove garbage files
     */
    StringInfo fullpath = makeStringInfo();
    List* files_in_dir = getDataFiles(rel->rd_id);

    List* deleted = GetDifference(files_in_dir, files_in_desc, equal_string);
    foreach (lc, deleted) {
        resetStringInfo(fullpath);

        StringInfo file = (StringInfo)lfirst(lc);
        appendStringInfo(fullpath, "%s/%s", store_path->data, file->data);

        ereport(DEBUG1,
            (errmsg("remove_garbage_file: nodename: %s, delete file: %s",
                g_instance.attr.attr_common.PGXCNodeName ? g_instance.attr.attr_common.PGXCNodeName : "",
                fullpath->data)));

        conn->deleteFile(fullpath->data, false);
    }

    foreach (lc, files_in_desc) {
        StringInfo file = (StringInfo)lfirst(lc);
        pfree_ext(file->data);
        pfree_ext(file);
    }
    list_free(files_in_desc);

    delete (conn);
}

/*
 * main entry of "VACUUM FULL" for DFS table
 *
 * @_in_param relid: Oid of DFS table
 * @_in_param vacstmt: statment for the DFS table with relid
 */
void DfsVacuumFull(Oid relid, VacuumStmt* vacstmt)
{
    /*
     * vacuum full just run on DN
     */
    if (IS_PGXC_COORDINATOR)
        return;

    /*
     * "vacuum full" is default behavior, not "vacuum full compact"
     */
    t_thrd.vacuum_cxt.vacuum_full_compact = false;
    if ((uint32)(vacstmt->options) & VACOPT_COMPACT)
        t_thrd.vacuum_cxt.vacuum_full_compact = true;

    /*
     * reset pendingDfsDelete
     */
    ResetPendingDfsDelete();

    /*
     * block INSERT, DELETE and UPDATE operation, just SELECT can be run
     * with "VACUUM FULL [ COMPACT ]" parallel.
     */
    Relation rel = heap_open(relid, ExclusiveLock);

    /*
     * create handler for desc table
     */
    DFSDescHandler* handler = New(CurrentMemoryContext) DFSDescHandler(MAX_LOADED_DFSDESC, rel->rd_att->natts, rel);

    /*
     * remove garbage files in store path
     */
    RemoveGarbageFiles(rel, handler);
    delete (handler);

    heap_close(rel, NoLock);

    /*
     * do all the dirty work
     */
    cluster_rel(relid,
        InvalidOid,
        InvalidOid,
        false,
        false,
        vacstmt->freeze_min_age,
        vacstmt->freeze_table_age,
        &vacstmt->memUsage,
        vacstmt->relation != NULL);
}

/*
 * drop all empty directories including root_path itself.
 *
 * @_in_param root_path: starting point
 */
static void RemoveEmptyDir(char* root_path, dfs::DFSConnector* conn)
{
    bool file_exist = false;
    List* sub_paths = conn->listDirectory(root_path, false);

    ListCell* lc = NULL;
    foreach (lc, sub_paths) {
        char* sub_path = (char*)lfirst(lc);

        if (!conn->isDfsFile(sub_path, false)) {
            RemoveEmptyDir(sub_path, conn);
        } else {
            file_exist = true;
        }
    }

    list_free(sub_paths);

    if (file_exist) {
        return;
    }

    /*
     * drop the empty directory if no any staff in the root_path.
     */
    sub_paths = conn->listDirectory(root_path, false);
    if (sub_paths == NIL) {
        ereport(DEBUG1, (errmsg("vacuum_hdfsdir: remove hdfs dir: %s", root_path)));

        if (conn->pathExists(root_path) && conn->deleteFile(root_path, 0) == -1) {
            ereport(DEBUG1, (errmsg("vacuum_hdfsdir: Failed to remove hdfs dir: %s", root_path)));
        }
    }

    list_free(sub_paths);
}

/*
 * drop all empty value partition directories which are belong to dfs table with
 * relid.
 *
 * @_in_param relid: Oid of dfs table
 */
static void DropEmptyPartitionDirectories(Oid relid)
{
    Assert(relid);

    /*
     * vacuum full just run on main CN
     */
    if (IS_PGXC_DATANODE)
        return;

    /*
     * open with ExclusiveLock, only select can be run on this relation
     */
    Relation rel = heap_open(relid, ExclusiveLock);
    StringInfo store_path = getDfsStorePath(rel);

    ereport(DEBUG1,
        (errmsg("vacuum_hdfsdir: relname: %s, relid: %u, schemaid: %u, root_path: %s",
            rel->rd_rel->relname.data,
            relid,
            rel->rd_rel->relnamespace,
            store_path->data)));

    /*
     * get connection object and will be used in RemoveEmptyDir()
     */
    DfsSrvOptions* options = GetDfsSrvOptions(rel->rd_rel->reltablespace);
    dfs::DFSConnector* conn = dfs::createConnector(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), options, rel->rd_rel->reltablespace);

    /*
     * walk the root path of dfs table recursively
     */
    ListCell* lc = NULL;
    List* sub_paths = conn->listDirectory(store_path->data, false);

    foreach (lc, sub_paths) {
        char* sub_path = (char*)lfirst(lc);

        if (!conn->isDfsFile(sub_path, false)) {
            RemoveEmptyDir(sub_path, conn);
        }
    }

    list_free(sub_paths);

    delete (conn);

    heap_close(rel, NoLock);
}

void updateTotalRows(Oid relid, double num_tuples)
{
    Relation classRel;
    HeapTuple ctup;
    Form_pg_class pgcform;

    classRel = heap_open(RelationRelationId, RowExclusiveLock);

    /* Fetch a copy of the tuple to scribble on */
    ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(ctup))
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("pg_class entry for relid %u vanished during updating TotalRows", relid)));

    pgcform = (Form_pg_class)GETSTRUCT(ctup);
    pgcform->reltuples = num_tuples;
    heap_inplace_update(classRel, ctup);
    heap_close(classRel, RowExclusiveLock);
}

// call back func to check current partOid is invisible or visible
static bool GPIIsInvisibleTuple(ItemPointer itemptr, void* state, Oid partOid, int2 bktId)
{
    VacStates* pvacStates = (VacStates*)state;
    Assert(pvacStates != NULL);

    if (partOid == InvalidOid) {
        ereport(elevel,
            (errmsg("global index tuple's oid invalid. partOid = %u", partOid)));
        return false;
    }
    
    /* check bucket id of crossbucket index tuple */
    if (pvacStates->bucketList != NULL && lookupHBucketid(pvacStates->bucketList, 0, bktId) == -1) {
        return true;
    }
    
    // check partition oid of global partition index tuple
    if (OidRBTreeMemberOid(pvacStates->invisiblePartOids, partOid)) {
        return true;
    }
    if (OidRBTreeMemberOid(pvacStates->visiblePartOids, partOid)) {
        return false;
    }
    PartStatus partStat = PartitionGetMetadataStatus(partOid, true);
    if (partStat == PART_METADATA_INVISIBLE) {
        (void)OidRBTreeMemberOid(pvacStates->invisiblePartOids, partOid);
        return true;
    } else {
        // visible include EXIST and NOEXIST
        (void)OidRBTreeMemberOid(pvacStates->visiblePartOids, partOid);
        return false;
    }
}

// clean invisible tuples for global partition index
static void GPICleanInvisibleIndex(Relation indrel, IndexBulkDeleteResult **stats, OidRBTree **cleanedParts,
    OidRBTree *invisibleParts)
{
    IndexVacuumInfo ivinfo;
    PGRUsage ru0;

    gstrace_entry(GS_TRC_ID_lazy_vacuum_index);
    pg_rusage_init(&ru0);

    ivinfo.index = indrel;
    ivinfo.analyze_only = false;
    ivinfo.estimated_count = true;
    ivinfo.message_level = elevel;
    ivinfo.num_heap_tuples = indrel->rd_rel->reltuples;
    ivinfo.strategy = vac_strategy;
    ivinfo.invisibleParts = NULL;

    VacStates* pvacStates = (VacStates*)palloc0(sizeof(VacStates));
    pvacStates->invisiblePartOids = invisibleParts;
    pvacStates->visiblePartOids = CreateOidRBTree();
    pvacStates->bucketList = NULL;
    if (RELATION_OWN_BUCKET(indrel) && RelationIsCrossBucketIndex(indrel)) {
        pvacStates->bucketList = searchHashBucketByOid(indrel->rd_bucketoid);
    }
    /* Do bulk deletion */
    *stats = index_bulk_delete(&ivinfo, *stats, GPIIsInvisibleTuple, (void*)pvacStates);
    Assert(!OidRBTreeHasIntersection(pvacStates->invisiblePartOids, pvacStates->visiblePartOids));

    OidRBTreeUnionOids(*cleanedParts, pvacStates->invisiblePartOids);

    DestroyOidRBTree(&pvacStates->visiblePartOids);
    pfree_ext(pvacStates);
    ereport(elevel,
        (errmsg("scanned index \"%s\" to remove %lf invisible rows",
             RelationGetRelationName(indrel),
             (*stats)->tuples_removed),
            errdetail("%s.", pg_rusage_show(&ru0))));
    gstrace_exit(GS_TRC_ID_lazy_vacuum_index);
}

static void GPIOpenGlobalIndexes(Relation onerel, LOCKMODE lockmode, int* nindexes, Relation** iRel)
{
    List* globIndOidList = NIL;
    ListCell* globIndCell = NULL;
    int globIndNums;

    Assert(lockmode != NoLock);
    Assert(onerel != NULL);

    // get global partition indexes
    globIndOidList = RelationGetSpecificKindIndexList(onerel, true);
    globIndNums = list_length(globIndOidList);
    if (globIndNums > 0) {
        *iRel = (Relation*)palloc((long)(globIndNums) * sizeof(Relation));
    } else {
        *iRel = NULL;
    }

    // collect ready global partion indexes
    int i = 0;
    foreach (globIndCell, globIndOidList) {
        Oid globIndOid = lfirst_oid(globIndCell);
        Relation indrel = index_open(globIndOid, lockmode);
        if (IndexIsReady(indrel->rd_index) && IndexIsUsable(indrel->rd_index)) {
            (*iRel)[i] = indrel;
            i++;
        } else {
            index_close(indrel, lockmode);
        }
    }
    *nindexes = i;

    list_free(globIndOidList);
}


static void UstoreVacuumMainPartitionGPIs(Relation onerel, const VacuumStmt* vacstmt,
    LOCKMODE lockmode, BufferAccessStrategy bstrategy)
{
    OidRBTree* invisibleParts = CreateOidRBTree();
    Oid parentOid = RelationGetRelid(onerel);

    if (vacstmt->options & VACOPT_VERBOSE) {
        elevel = VERBOSEMESSAGE;
    } else {
        elevel = DEBUG2;
    }

    /* Get invisable parts */
    if (ConditionalLockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK)) {
        PartitionGetAllInvisibleParts(parentOid, &invisibleParts);
        UnlockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK);
    }
    
    /* In rbtree, rb_leftmost will return NULL if rbtree is empty. */
    if (rb_leftmost(invisibleParts) == NULL) {
        DestroyOidRBTree(&invisibleParts);
        return;
    }

    vac_strategy = bstrategy;

    /* Open all global indexes of the main partition */
    Relation* iRel = NULL;
    int nIndexes;
    GPIOpenGlobalIndexes(onerel, lockmode, &nIndexes, &iRel);
    Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
    for (int i = 0; i < nIndexes; i++) {
        /* Start the cleanup process and collect the info needed */
        IndexVacuumInfo ivinfo;
        ivinfo.index = iRel[i];
        ivinfo.analyze_only = false;
        ivinfo.estimated_count = false;
        ivinfo.message_level = elevel;
        ivinfo.num_heap_tuples = -1;
        ivinfo.strategy = vac_strategy;
        ivinfo.invisibleParts = invisibleParts;

        /* Cleanup process of index */
        index_bulk_delete(&ivinfo, NULL, NULL, NULL);
        
        index_close(iRel[i], lockmode);
    }
    heap_close(classRel, RowExclusiveLock);

    /*
     * Before clearing the global partition index of a partition table,
     * acquire a AccessShareLock on ADD_PARTITION_ACTION, and make sure that the interval partition
     * creation process will not be performed concurrently.
     */
    OidRBTree* cleanedParts = CreateOidRBTree();
    OidRBTreeUnionOids(cleanedParts, invisibleParts);
    if (ConditionalLockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK)) {
        PartitionSetEnabledClean(parentOid, cleanedParts, invisibleParts, true);
        UnlockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK);
    } else {
        /* Updates reloptions of cleanedParts in pg_partition after GPI vacuum is executed */
        PartitionSetEnabledClean(parentOid, cleanedParts, invisibleParts, false);
    }

    DestroyOidRBTree(&invisibleParts);
    DestroyOidRBTree(&cleanedParts);
    pfree_ext(iRel);
}

// vacuum main partition table to delete invisible tuple in global partition index
static void GPIVacuumMainPartition(
    Relation onerel, const VacuumStmt* vacstmt, LOCKMODE lockmode, BufferAccessStrategy bstrategy)
{
    Relation* iRel = NULL;
    int nindexes;
    OidRBTree* cleanedParts = CreateOidRBTree();
    OidRBTree* invisibleParts = CreateOidRBTree();
    Oid parentOid = RelationGetRelid(onerel);

    if (vacstmt->options & VACOPT_VERBOSE) {
        elevel = VERBOSEMESSAGE;
    } else {
        elevel = DEBUG2;
    }

    /*
     * Before clearing the global partition index of a partition table,
     * acquire a AccessShareLock on ADD_PARTITION_ACTION, and make sure that the interval partition
     * creation process will not be performed concurrently.
     */
    if (ConditionalLockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK)) {
        PartitionGetAllInvisibleParts(parentOid, &invisibleParts);
        UnlockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK);
    }

    vac_strategy = bstrategy;
    // Open all global indexes of the main partition
    GPIOpenGlobalIndexes(onerel, lockmode, &nindexes, &iRel);
    IndexBulkDeleteResult** indstats = (IndexBulkDeleteResult**)palloc0(nindexes * sizeof(IndexBulkDeleteResult*));
    Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
    for (int i = 0; i < nindexes; i++) {
        GPICleanInvisibleIndex(iRel[i], &indstats[i], &cleanedParts, invisibleParts);
        if (IndexEnableWaitCleanCbi(iRel[i])) {
            cbi_set_enable_clean(iRel[i]);
        }
        vac_update_relstats(
            iRel[i], classRel, indstats[i]->num_pages, indstats[i]->num_index_tuples, 0, false, InvalidTransactionId,
            InvalidMultiXactId);
        pfree_ext(indstats[i]);
        index_close(iRel[i], lockmode);
    }
    heap_close(classRel, RowExclusiveLock);

    /*
     * Before clearing the global partition index of a partition table,
     * acquire a AccessShareLock on ADD_PARTITION_ACTION, and make sure that the interval partition
     * creation process will not be performed concurrently.
     */
    if (ConditionalLockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK)) {
        PartitionSetEnabledClean(parentOid, cleanedParts, invisibleParts, true);
        UnlockPartition(parentOid, ADD_PARTITION_ACTION, AccessShareLock, PARTITION_SEQUENCE_LOCK);
    } else {
        /* Updates reloptions of cleanedParts in pg_partition after gpi lazy_vacuum is executed */
        PartitionSetEnabledClean(parentOid, cleanedParts, invisibleParts, false);
    }

    DestroyOidRBTree(&cleanedParts);
    DestroyOidRBTree(&invisibleParts);
    pfree_ext(indstats);
    pfree_ext(iRel);
}

void CBIOpenLocalCrossbucketIndex(Relation onerel, LOCKMODE lockmode, int* nindexes, Relation** iRel)
{
    List* indOidList = NIL;
    ListCell* indCell = NULL;
    int indNums;
    Assert(lockmode != NoLock);
    Assert(onerel != NULL);
    indOidList = RelationGetLocalCbiList(onerel);
    indNums = list_length(indOidList);
    if (indNums > 0) {
        *iRel = (Relation*)palloc((long)(indNums) * sizeof(Relation));
    } else {
        *iRel = NULL;
    }
    int i = 0;
    foreach (indCell, indOidList) {
        Oid indOid = lfirst_oid(indCell);
        Relation indrel = index_open(indOid, lockmode);
        if (IndexIsReady(indrel->rd_index) && IndexIsUsable(indrel->rd_index)) {
            (*iRel)[i] = indrel;
            i++;
        } else {
            index_close(indrel, lockmode);
        }
    }
    *nindexes = i;
    list_free(indOidList);
}

static void CBIVacuumMainPartition(
    Relation onerel, const VacuumStmt* vacstmt, LOCKMODE lockmode, BufferAccessStrategy bstrategy)
{
    Relation* iRel = NULL;
    int nindexes;
    if (vacstmt->options & VACOPT_VERBOSE) {
        elevel = VERBOSEMESSAGE;
    } else {
        elevel = DEBUG2;
    }
    vac_strategy = bstrategy;
    CBIOpenLocalCrossbucketIndex(onerel, lockmode, &nindexes, &iRel);
    for (int i = 0; i < nindexes; i++) {
        if (IndexEnableWaitCleanCbi(iRel[i])) {
            cbi_set_enable_clean(iRel[i]);
        }
        index_close(iRel[i], lockmode);
    }

    pfree_ext(iRel);
}

