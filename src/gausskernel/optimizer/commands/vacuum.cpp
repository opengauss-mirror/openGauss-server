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
#include "catalog/dfsstore_ctlg.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pgxc_class.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "commands/cluster.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "executor/nodeModifyTable.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/commands_gstrace.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "catalog/catalog.h"
#include "foreign/fdwapi.h"
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

extern void exec_query_for_merge(const char* query_string);

extern void do_delta_merge(List* infos, VacuumStmt* stmt);

/* release all memory in infos */
static void free_merge_info(List* infos);
static void DropEmptyPartitionDirectories(Oid relid);

static THR_LOCAL BufferAccessStrategy vac_strategy;

static void vac_truncate_clog(TransactionId frozenXID);
static bool vacuum_rel(Oid relid, VacuumStmt* vacstmt, bool do_toast);

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
    if (IS_PGXC_COORDINATOR && (vacstmt->options & VACOPT_FULL)) {
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

            /*
             * do NOT vacuum partitioned table,
             * as vacuum is an operation related with tuple and storage page reorganization
             */
            if (!vacuumMainPartition(vacstmt->flags) && (vacstmt->options & VACOPT_VACUUM)) {
                if (vacuumPartition(vacstmt->flags) || vacuumRelation(vacstmt->flags)) {
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
    }

    if ((vacstmt->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess()) {
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

bool rel_is_pax_format(HeapTuple tuple, TupleDesc tupdesc)
{
    bool ret = false;
    bytea* options = NULL;

    options = extractRelOptions(tuple, tupdesc, InvalidOid);
    if (options != NULL) {
        const char* format = ((options) && (((StdRdOptions*)(options))->orientation))
            ? ((char*)(options) + *(int*)&(((StdRdOptions*)(options))->orientation))
            : ORIENTATION_ROW;
        if (pg_strcasecmp(format, ORIENTATION_ORC) == 0)
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
            get_namespace_name(get_rel_namespace(relid), true),
            relname);
        securec_check_ss(rc, "\0", "\0");

        pfree(relname);
        relname = NULL;
    }

    return nsp_relname;
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

    /* OID supplied by VACUUM's caller? */
    if (OidIsValid(relid)) {
        if (t_thrd.vacuum_cxt.vac_context) {
            oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
        }
        vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
        vacObj->tab_oid = relid;
        vacObj->flags = vacstmt->flags;
        oid_list = lappend(oid_list, vacObj);

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
                /* It must be a partition */
                if (t_thrd.vacuum_cxt.vac_context) {
                    oldcontext = MemoryContextSwitchTo(t_thrd.vacuum_cxt.vac_context);
                }    
                vacObj = (vacuum_object*)palloc0(sizeof(vacuum_object));
                vacObj->tab_oid = partitionid;
                vacObj->parent_oid = relationid;
                vacObj->flags = VACFLG_SUB_PARTITION;
                oid_list = lappend(oid_list, vacObj);

                if (t_thrd.vacuum_cxt.vac_context) {
                    (void)MemoryContextSwitchTo(oldcontext);
                }   
            }

            ReleaseSysCache(partitionTup);
        } else {
            /* 2.a relation */
            classTup = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(relationid), LOG);

            if (HeapTupleIsValid(classTup)) {
                /* for dfs special vacuum, just skip for non-dfs table. */
                if ((((vacstmt->options & VACOPT_HDFSDIRECTORY) || (vacstmt->options & VACOPT_COMPACT)) &&
                        !rel_is_pax_format(classTup, pgclassdesc)) ||
                    ((vacstmt->options & VACOPT_MERGE) && !rel_is_CU_format(classTup, pgclassdesc) &&
                        !rel_is_pax_format(classTup, pgclassdesc))) {
                    ReleaseSysCache(classTup);

                    return NIL;
                }

                classForm = (Form_pg_class)GETSTRUCT(classTup);

                /* Partitioned table, get all the partitions */
                if (classForm->parttype  == PARTTYPE_PARTITIONED_RELATION && classForm->relkind == RELKIND_RELATION &&
                    ((vacstmt->options & VACOPT_FULL) || (vacstmt->options & VACOPT_VACUUM))) {
                    Relation pgpartition;
                    HeapScanDesc scan;
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
                    scan = heap_beginscan(pgpartition, SnapshotNow, 2, keys);

                    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
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
                } else {
                    /* 
                     * non-partitioned table 
                     * forbit vacuum full/vacuum/analyze cstore.xxxxx direct on datanode 
                     */
                    if (classForm->relnamespace == CSTORE_NAMESPACE) {
                        if (memcmp(vacstmt->relation->relname, "pg_delta", 8)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
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
                    oid_list = lappend(oid_list, vacObj);

                    if (t_thrd.vacuum_cxt.vac_context) {
                        (void)MemoryContextSwitchTo(oldcontext);
                    }
                }
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relationid)));
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
        HeapScanDesc scan;
        HeapTuple tuple;

        /* Process all plain relations listed in pg_class */
        pgclass = heap_open(RelationRelationId, AccessShareLock);

        scan = heap_beginscan(pgclass, SnapshotNow, 0, NULL);

        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
            /* for dfs special vacuum, just skip for non-dfs table. */
            if ((((vacstmt->options & VACOPT_HDFSDIRECTORY) || (vacstmt->options & VACOPT_COMPACT)) &&
                    !rel_is_pax_format(tuple, pgclassdesc)) ||
                ((vacstmt->options & VACOPT_MERGE) && !rel_is_CU_format(tuple, pgclassdesc) &&
                    !rel_is_pax_format(tuple, pgclassdesc)))
                continue;

            Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tuple);

            /* Only vacuum table and materialized view */
            if (classForm->relkind != RELKIND_RELATION && classForm->relkind != RELKIND_MATVIEW) {
                continue;
            }

            /* Plain relation and valued partition relation */
            if (classForm->parttype == PARTTYPE_NON_PARTITIONED_RELATION ||
                classForm->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION ||
                ((vacstmt->options & VACOPT_MERGE) && rel_is_CU_format(tuple, pgclassdesc))) {
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
            } else {
                /* 
                 * Partitioned table 
                 * It is a partitioned table, so find all the partitions in pg_partition
                 */
                Relation pgpartition;
                HeapScanDesc partScan;
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
                partScan = heap_beginscan(pgpartition, SnapshotNow, 2, keys);
                while (NULL != (partTuple = heap_getnext(partScan, ForwardScanDirection))) {
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
    TransactionId* freezeLimit, TransactionId* freezeTableLimit)
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
    if (limit > FirstNormalTransactionId + freezemin)
        limit -= freezemin;
    else
        limit = FirstNormalTransactionId;

    /*
     * If oldestXmin is very far back (in practice, more than
     * g_instance.attr.attr_storage.autovacuum_freeze_max_age / 2 XIDs old), complain and force a minimum
     * freeze age of zero.
     */
    nextXid = ReadNewTransactionId();
    if (nextXid > FirstNormalTransactionId + g_instance.attr.attr_storage.autovacuum_freeze_max_age)
        safeLimit = nextXid - g_instance.attr.attr_storage.autovacuum_freeze_max_age;
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
        freezetable = Min((double) freezetable, g_instance.attr.attr_storage.autovacuum_freeze_max_age * 0.95);
        Assert(freezetable >= 0);

        /*
         * Compute the cutoff XID, being careful not to generate a "permanent"
         * XID.
         */
        limit = ReadNewTransactionId();
        if (limit > FirstNormalTransactionId + freezetable)
            limit -= freezetable;
        else
            limit = FirstNormalTransactionId;

        *freezeTableLimit = limit;
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
    BlockNumber num_all_visible_pages, bool hasindex, TransactionId frozenxid)
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
    xid64datum = heap_getattr(ctup, Anum_pg_class_relfrozenxid64, RelationGetDescr(classRel), &isNull);

    if (isNull) {
        relfrozenxid = pgcform->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

    if (TransactionIdIsNormal(frozenxid) && (TransactionIdPrecedes(relfrozenxid, frozenxid)
#ifdef PGXC
                                                || !IsPostmasterEnvironment)
#endif
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

        replaces[Anum_pg_class_relfrozenxid64 - 1] = true;
        values[Anum_pg_class_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenxid);

        nctup = heap_modify_tuple(ctup, RelationGetDescr(classRel), values, nulls, replaces);
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
 *		minimum of the pg_class.relfrozenxid values.  If we are able to
 *		advance pg_database.datfrozenxid, also try to truncate pg_clog.
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
    /*
     * Initialize the "min" calculation with GetOldestXmin, which is a
     * reasonable approximation to the minimum relfrozenxid for not-yet-
     * committed pg_class entries for new tables; see AddNewRelationTuple().
     * Se we cannot produce a wrong minimum by starting with this.
     */
    newFrozenXid = GetOldestXmin(NULL);

    lastSaneFrozenXid = ReadNewTransactionId();

    /*
     * We must seqscan pg_class to find the minimum Xid, because there is no
     * index that can help us here.
     */
    relation = heap_open(RelationRelationId, AccessShareLock);

    scan = systable_beginscan(relation, InvalidOid, false, SnapshotNow, 0, NULL);

    while ((classTup = systable_getnext(scan)) != NULL) {
        Form_pg_class classForm = (Form_pg_class)GETSTRUCT(classTup);

        /*
         * Only consider relations able to hold unfrozen XIDs (anything else
         * should have InvalidTransactionId in relfrozenxid anyway.)
         */
        if (classForm->relkind != RELKIND_RELATION && classForm->relkind != RELKIND_MATVIEW && 
                classForm->relkind != RELKIND_TOASTVALUE)
            continue;

        /* global temp table relstats not in pg_class */
        if (classForm->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            continue;
        }

        xid64datum = heap_getattr(classTup, Anum_pg_class_relfrozenxid64, RelationGetDescr(relation), &isNull);

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
        TransactionId oldestGttFrozenxid = ENABLE_THREAD_POOL ? 
                                            ListAllSessionGttFrozenxids(0, NULL, NULL, NULL) :
                                            ListAllThreadGttFrozenxids(0, NULL, NULL, NULL);

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
    xid64datum = heap_getattr(tuple, Anum_pg_database_datfrozenxid64, RelationGetDescr(relation), &isNull);

    if (isNull) {
        datfrozenxid = dbform->datfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, datfrozenxid))
            datfrozenxid = FirstNormalTransactionId;
    } else {
        datfrozenxid = DatumGetTransactionId(xid64datum);
    }

    if ((TransactionIdPrecedes(datfrozenxid, newFrozenXid))
#ifdef PGXC
        || !IsPostmasterEnvironment)
#endif
    {
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

        replaces[Anum_pg_database_datfrozenxid64 - 1] = true;
        values[Anum_pg_database_datfrozenxid64 - 1] = TransactionIdGetDatum(newFrozenXid);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
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
        vac_truncate_clog(newFrozenXid);
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
static void vac_truncate_clog(TransactionId frozenXID)
{
    Relation relation;
    HeapScanDesc scan;
    HeapTuple tuple;
    Oid oldest_datoid;
    /* init oldest_datoid to sync with my frozenXID */
    oldest_datoid = u_sess->proc_cxt.MyDatabaseId;

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

    scan = heap_beginscan(relation, SnapshotNow, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        volatile FormData_pg_database* dbform = (Form_pg_database)GETSTRUCT(tuple);
        bool isNull = false;
        TransactionId datfrozenxid;
        Datum xid64datum = heap_getattr(tuple, Anum_pg_database_datfrozenxid64, RelationGetDescr(relation), &isNull);

        if (isNull) {
            datfrozenxid = dbform->datfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, datfrozenxid))
                datfrozenxid = FirstNormalTransactionId;
        } else
            datfrozenxid = DatumGetTransactionId(xid64datum);

        Assert(TransactionIdIsNormal(datfrozenxid));

        if (TransactionIdPrecedes(datfrozenxid, frozenXID)) {
            frozenXID = datfrozenxid;
            oldest_datoid = HeapTupleGetOid(tuple);
        }
    }

    heap_endscan(scan);

    heap_close(relation, AccessShareLock);

    /* Truncate CLOG to the oldest frozenxid */
    TruncateCLOG(frozenXID);

    /*
     * Update the wrap limit for GetNewTransactionId.  Note: this function
     * will also signal the postmaster for an(other) autovac cycle if needed.
     */
    SetTransactionIdLimit(frozenXID, oldest_datoid);

    ereport(LOG,
        (errmsg("In truncate clog: frozenXID:" XID_FMT ", oldest_datoid: %u, xid:" XID_FMT
                ", pid: %lu, ShmemVariableCache: nextOid:%u, oldestXid:" XID_FMT ","
                "nextXid:" XID_FMT ", xidVacLimit:%lu, oldestXidDB:%u, RecentXmin:" XID_FMT
                ", RecentGlobalXmin:" XID_FMT ", OldestXmin:" XID_FMT ", FreezeLimit:%lu, useLocalSnapshot:%d.",
            frozenXID,
            oldest_datoid,
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

    int messageLevel = -1;
    /* Begin a transaction for vacuuming this relation */
    gstrace_entry(GS_TRC_ID_vacuum_rel);
    StartTransactionCommand();

    Assert(!vacuumMainPartition(vacstmt->flags));

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
    ereport(DEBUG1, (errmsg("Starting autovacuum")));
    /* Now that flags have been set, we can take a snapshot correctly */
    PushActiveSnapshot(GetTransactionSnapshot());
    ereport(DEBUG1, (errmsg("Started autovacuum")));
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

    if (vacuumRelation(vacstmt->flags) && vacstmt->isMOTForeignTable) {
        if (IS_PGXC_COORDINATOR) {
            proc_snapshot_and_transaction();
            return false;
        }
        lmode = AccessExclusiveLock;
    }

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
                if (!(pg_class_ownercheck(relid, GetUserId()) ||
                        (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) &&
                            !rel->rd_rel->relisshared))) {
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
            /* Open the partition */
            onepart = tryPartitionOpen(onepartrel, relid, lmode);

            if (onepart) {
                /* Get a Relation from a Partition */
                onerel = partitionGetRelation(onepartrel, onepart);
            }
        }

        GetLock = true;
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
            if (onepartrel->rd_rel->relkind == RELKIND_RELATION) {
                partID = partOidGetPartID(onepartrel, relid);
                if (partID->partArea == PART_AREA_RANGE || partID->partArea == PART_AREA_INTERVAL) {
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
        } else
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
    //
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

#define CloseAllRelationsBeforeReturnFalse()            \
    do {                                                \
        if (RelationIsPartition(onerel)) {              \
            releaseDummyRelation(&onerel);              \
            partitionClose(onepartrel, onepart, lmode); \
            relation_close(onepartrel, lmodePartTable); \
        } else {                                        \
            relation_close(onerel, lmode);              \
        }                                               \
        if (cudescrel != NULL)                          \
            relation_close(cudescrel, lmode);           \
        if (deltarel != NULL)                           \
            relation_close(deltarel, lmode);            \
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
    if (!(pg_class_ownercheck(RelationGetPgClassOid(onerel, (onepart != NULL)), GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !onerel->rd_rel->relisshared))) {
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
    if (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE && isMOTFromTblOid(onerel->rd_id)) {
        ;
    } else if (onerel->rd_rel->relkind != RELKIND_RELATION && onerel->rd_rel->relkind != RELKIND_MATVIEW && 
        onerel->rd_rel->relkind != RELKIND_TOASTVALUE) {

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

    if (RelationIsPartition(onerel)) {
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
        if (RelationIsPartition(onerel)) {
            toast_relid = onepart->pd_part->reltoastrelid;
        } else {
            toast_relid = onerel->rd_rel->reltoastrelid;
        }
    } else {
        toast_relid = InvalidOid;
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
                if (vacuumPartition(vacstmt->flags))
                    rel = onepartrel;

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
    if (RelationIsPartition(onerel)) {
        vacstmt->onepartrel = onepartrel;
        vacstmt->onepart = onepart;
    }

    WaitState oldStatus = pgstat_report_waitstatus_relname(STATE_VACUUM, get_nsp_relname(relid));
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
        }
    } else if ((vacstmt->options & VACOPT_FULL) && (vacstmt->flags & VACFLG_SUB_PARTITION)) {
        releaseDummyRelation(&onerel);
        partitionClose(onepartrel, onepart, NoLock);
        relation_close(onepartrel, NoLock);
        onerel = NULL;
        onepartrel = NULL;
        onepart = NULL;

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
    } else if (!(vacstmt->options & VACOPT_FULL)) {
        /* clean hdfs empty directories of value partition just on main CN */
        if (vacstmt->options & VACOPT_HDFSDIRECTORY) {
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                DropEmptyPartitionDirectories(relid);
            }
        } else {
            pgstat_report_waitstatus_relname(STATE_VACUUM, get_nsp_relname(relid));
            lazy_vacuum_rel(onerel, vacstmt, vac_strategy);
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
            releaseDummyRelation(&onerel);
            partitionClose(onepartrel, onepart, NoLock);
            relation_close(onepartrel, NoLock);
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
        (void)vacuum_rel(toast_relid, vacstmt, false);
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
        UnLockPartitionVacuumForSession(&partIdtf, relationid, relid, lmode);

        UnlockRelationIdForSession(&partLockRelId, lmodePartTable);
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
    TransactionId frozenxid)
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
    xid64datum = heap_getattr(parttup, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rd), &isNull);

    if (isNull) {
        relfrozenxid = partform->relfrozenxid;

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

        partform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "", "");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "", "");

        replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
        values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenxid);

        nparttup = heap_modify_tuple(parttup, RelationGetDescr(rd), values, nulls, replaces);
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

void vac_open_part_indexes(
    VacuumStmt* vacstmt, LOCKMODE lockmode, int* nindexes, Relation** Irel, Relation** indexrel, Partition** indexpart)
{
    List* indexoidlist = NIL;
    ListCell* indexoidscan = NULL;
    int i;
    Relation indrel;

    Assert(lockmode != NoLock);
    Assert(vacstmt->onepart != NULL);
    Assert(vacstmt->onepartrel != NULL);

    indexoidlist = PartitionGetPartIndexList(vacstmt->onepart);
    i = list_length(indexoidlist);
    if (i > 0) {
        *Irel = (Relation*)palloc((long)(i) * sizeof(Relation));
        *indexrel = (Relation*)palloc((long)(i) * sizeof(Relation));
        *indexpart = (Partition*)palloc((long)(i) * sizeof(Partition));
    } else {
        *Irel = NULL;
        *indexrel = NULL;
        *indexpart = NULL;
    }

    i = 0;
    foreach (indexoidscan, indexoidlist) {
        Oid indexParentid;
        Oid indexoid = lfirst_oid(indexoidscan);
        Partition indpart;
        Relation indexPartRel = NULL;

        /* Get the index partition's parent oid */
        indexParentid = partid_get_parentid(indexoid);

        indrel = relation_open(indexParentid, lockmode);
        Assert(indrel != NULL);

        /* Open the partition */
        indpart = partitionOpen(indrel, indexoid, lockmode);
        indexPartRel = partitionGetRelation(indrel, indpart);

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

    list_free(indexoidlist);
}

void vac_close_part_indexes(int nindexes, Relation* Irel, Relation* indexrel, Partition* indexpart, LOCKMODE lockmode)
{
    if (Irel == NULL || indexpart == NULL || indexrel == NULL) {
        return;
    }

    while (nindexes--) {
        Relation rel = Irel[nindexes];
        Relation ind = indexrel[nindexes];
        Partition part = indexpart[nindexes];

        releaseDummyRelation(&rel);
        partitionClose(ind, part, lockmode);
        relation_close(ind, lockmode);
    }
    pfree_ext(indexrel);
    pfree_ext(indexpart);
    pfree_ext(Irel);
}

/* Scan pg_partition to get all the partitions of the partitioned table,
 * calculate all the pages, tuples, and the min frozenXid
 */
void CalculatePartitionedRelStats(_in_ Relation partitionRel, _in_ Relation partRel, _out_ BlockNumber* totalPages,
    _out_ BlockNumber* totalVisiblePages, _out_ double* totalTuples, _out_ TransactionId* minFrozenXid)
{
    ScanKeyData partKey[2];
    BlockNumber pages = 0;
    BlockNumber allVisiblePages = 0;
    double tuples = 0;
    TransactionId frozenXid;
    Form_pg_partition partForm;

    Assert(partitionRel->rd_rel->parttype == PARTTYPE_PARTITIONED_RELATION);

    if (partitionRel->rd_rel->relkind == RELKIND_RELATION) {
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

    SysScanDesc partScan = systable_beginscan(partRel, PartitionParentOidIndexId, true, SnapshotNow, 2, partKey);

    HeapTuple partTuple = NULL;
    /* compute all pages, tuples and the minimum frozenXid */
    partTuple = systable_getnext(partScan);
    if (partTuple != NULL) {
        partForm = (Form_pg_partition)GETSTRUCT(partTuple);

        bool isNull = false;
        TransactionId relfrozenxid;
        Relation rel = heap_open(PartitionRelationId, AccessShareLock);
        Datum xid64datum = heap_getattr(partTuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);

        if (isNull) {
            relfrozenxid = partForm->relfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                !TransactionIdIsNormal(relfrozenxid))
                relfrozenxid = FirstNormalTransactionId;
        } else {
            relfrozenxid = DatumGetTransactionId(xid64datum);
        }

        frozenXid = relfrozenxid;

        do {
            partForm = (Form_pg_partition)GETSTRUCT(partTuple);

            /* calculate pages and tuples from all the partitioned. */
            pages += (uint32) partForm->relpages;
            allVisiblePages += (uint32) partForm->relallvisible;
            tuples += partForm->reltuples;

            /* update the relfrozenxid */
            xid64datum = heap_getattr(partTuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);

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
        } while ((partTuple = systable_getnext(partScan)) != NULL);

        heap_close(rel, AccessShareLock);
    } else {
        frozenXid = InvalidTransactionId;
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
}

/*
 * After VACUUM or ANALYZE, update pg_class for the partitioned tables.
 */
void vac_update_pgclass_partitioned_table(Relation partitionRel, bool hasIndex, TransactionId newFrozenXid)
{
    BlockNumber pages = 0;
    BlockNumber allVisiblePages = 0;
    double tuples = 0;
    TransactionId frozenXid = newFrozenXid;

    Assert(partitionRel->rd_rel->parttype == PARTTYPE_PARTITIONED_RELATION);

    /*
     * We should make CalculatePartitionedRelStats and vac_udpate_relstats in whole,
     * Use ShareUpdateExclusiveLock lock to block another vacuum thread to update pgclass partition table in parallel
     * Keep lock RelatitionRelation first, and then PartitionRelation to avoid dead lock
     */
    Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
    Relation partRel = heap_open(PartitionRelationId, ShareUpdateExclusiveLock);
    CalculatePartitionedRelStats(partitionRel, partRel, &pages, &allVisiblePages, &tuples, &frozenXid);
    vac_update_relstats(partitionRel, classRel, pages, tuples, allVisiblePages, hasIndex, frozenXid);
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
    CalculatePartitionedRelStats(partitionRel, pgPartitionRel, NULL, NULL, NULL, &frozenXid);
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
    Datum xid64datum = heap_getattr(ctup, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
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

        nctup = heap_modify_tuple(ctup, RelationGetDescr(pgclassRel), values, nulls, replaces);
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
    Datum xid64datum = heap_getattr(ctup, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rd), &isNull);

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

        ntup = heap_modify_tuple(ctup, RelationGetDescr(rd), values, nulls, replaces);
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
    HeapScanDesc scan;
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
    scan = heap_beginscan(pgpartition, SnapshotNow, 2, keys);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
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
    HeapScanDesc scan = heap_beginscan(source, SnapshotNow, 0, NULL);
    HeapTuple tuple = NULL;

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        HeapTuple copyTuple = NULL;

        copyTuple = heap_copytuple(tuple);

        (void)simple_heap_insert(dest, copyTuple);

        heap_freetuple(copyTuple);
    }

    heap_endscan(scan);
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
        relation_close(rel, NoLock);
        PopActiveSnapshot();
        CommitTransactionCommand();
    } else if (RELATION_IS_PARTITIONED(rel)) {
        /* deal with patitioned table */
        List* oid_list = get_part_oid(rel_oid);
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
                    finish_heap_swap(deltaOid, OIDNewHeap, false, false, false, u_sess->utils_cxt.RecentGlobalXmin);

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
        lazy_vacuum_rel(rel, stmt, GetAccessStrategy(BAS_VACUUM));
        CommandCounterIncrement();

        /* open delta realtion */
        Oid deltaOid = rel->rd_rel->reldeltarelid;
        Relation delta_rel = relation_open(deltaOid, AccessExclusiveLock);

        Oid OIDNewHeap = make_new_heap(deltaOid, delta_rel->rd_rel->reltablespace, AccessExclusiveLock);

        /* copy delta data to new heap */
        getTuplesAndInsert(delta_rel, OIDNewHeap);

        /* swap relfile node */
        finish_heap_swap(deltaOid, OIDNewHeap, false, false, false, u_sess->utils_cxt.RecentGlobalXmin);

        /* close relation */
        relation_close(delta_rel, NoLock);
        
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
    HeapScanDesc relScan;

    ScanKeyData key[1];
    Form_pg_class form;

    List* infos = NIL;

    ereport(DEBUG1, (errmsg("deltamerge: %s()", __FUNCTION__)));

    Relation classRel = heap_open(RelationRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_RELATION));

    relScan = heap_beginscan(classRel, SnapshotNow, 1, key);
    while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL) {
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

            make_real_queries(info);

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
    dfs::DFSConnector* conn = dfs::createConnector(t_thrd.top_mem_cxt, dfsoptions, rel->rd_rel->reltablespace);

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
    if (vacstmt->options & VACOPT_COMPACT)
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
    dfs::DFSConnector* conn = dfs::createConnector(t_thrd.top_mem_cxt, options, rel->rd_rel->reltablespace);

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
