/* -------------------------------------------------------------------------
 *
 * cluster.cpp
 *    CLUSTER a table on an index.	This is now also used for VACUUM FULL.
 *
 * There is hardly anything left of Paul Brown's original implementation...
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/commands/cluster.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h" /* stay here, otherwise compile errors */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_delta.h"
#include "access/dfs/dfs_insert.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pgxc_slice.h"
#include "catalog/storage.h"
#include "catalog/toasting.h"
#include "catalog/storage_gtt.h"
#include "catalog/index.h"
#include "catalog/pg_constraint.h"
#include "commands/cluster.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr/smgr.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "access/cstore_am.h"
#include "access/cstore_insert.h"
#include "catalog/cstore_ctlg.h"
#include "pgxc/pgxc.h"
#include "pgxc/groupmgr.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/pg_hashbucket_fn.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/commands_gstrace.h"
#include "parser/parse_utilcmd.h"
#include "access/multixact.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/storage/part_merge.h"
#include "tsdb/utils/ts_relcache.h"
#include "tsdb/cache/tags_cachemgr.h"
#endif

#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/knl_uscan.h"
#include "access/ustore/knl_uheap.h"
#include "commands/tablespace.h"
#include "storage/smgr/segment.h"

/*
 * This struct is used to pass around the information on tables to be
 * clustered. We need this so we can make a list of them when invoked without
 * a specific table/index pair.
 */
typedef struct {
    Oid tableOid;
    Oid indexOid;
} RelToCluster;

#define SQL_STR_LEN 1024

#define MAX_REDIS_SWITCH_EXEC_CMD 2

#define BUCKET_SWITCH_FLAG 0x1
#define TRANSFER_SWITCH_FLAG 0x2

typedef enum {
    REDIS_SWITCH_EXEC_NORMAL,
    REDIS_SWITCH_EXEC_MOVE,
    REDIS_SWITCH_EXEC_DROP,
    REDIS_SWITCH_EXEC_TRANSFER
} RedisSwitchType;

typedef struct {
    RedisSwitchType type;
    ExecNodes* nodes;
} RedisSwitchNode;

extern List* GetDifference(const List* list1, const List* list2, EqualFunc fn);
extern void InsertNewFileToDfsPending(const char* filename, Oid ownerid, uint64 filesize);
extern void InsertIntoPendingDfsDelete(const char* filename, bool atCommit, Oid ownerid, uint64 filesize);
extern DfsSrvOptions* GetDfsSrvOptions(Oid spcNode);

static void swap_relation_names(Oid r1, Oid r2);

static void swapCascadeHeapTables(
    Oid relId1, Oid relId2, Oid tempTableOid, bool swapByContent, TransactionId frozenXid,
    MultiXactId multiXid, Oid* mappedTables);

static void SwapCStoreTables(Oid relId1, Oid relId2, Oid parentOid, Oid tempTableOid);

static void rebuild_relation(
    Relation OldHeap, Oid indexOid, int freeze_min_age, int freeze_table_age, bool verbose, AdaptMem* mem_info);

static void rebuildPartitionedTable(
    Relation partTableRel, Oid indexOid, int freezeMinAge, int freezeTableAge, bool verbose, AdaptMem* mem_info);
static void rebuildPartition(Relation partTableRel, Oid partitionOid, Oid indexOid, int freezeMinAge,
    int freezeTableAge, bool verbose, AdaptMem* mem_info);

static void copyPartitionHeapData(Relation newHeap, Relation oldHeap, Oid indexOid, PlannerInfo* root,
    RelOptInfo* relOptInfo, int freezeMinAge, int freezeTableAge, bool verbose, bool* pSwapToastByContent,
    TransactionId* pFreezeXid, MultiXactId* pFreezeMulti, AdaptMem* mem_info, double* ptrDeleteTupleNum = NULL);
static void CopyCStoreData(Relation oldRel, Relation newRel, int freeze_min_age, int freeze_table_age, bool verbose,
    bool* pSwapToastByContent, TransactionId* pFreezeXid, AdaptMem* mem_info);
static void DoCopyPaxFormatData(Relation oldRel, Relation newRel);
static void CopyOldDeltaToNewRel(Oid oldRel, Oid newRel);
static void DoCopyCUFormatData(Relation oldRel, Relation newRel, TupleDesc oldTupDesc, AdaptMem* mem_info);
static List* FindMergedDescs(Relation oldRel, Relation newRel);
extern ValuePartitionMap* buildValuePartitionMap(Relation relation, Relation pg_partition, HeapTuple partitioned_tuple);
static void copy_heap_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex, int freeze_min_age, int freeze_table_age,
    bool verbose, bool* pSwapToastByContent, TransactionId* pFreezeXid, MultiXactId *pFreezeMulti,
    double* ptrDeleteTupleNum, AdaptMem* mem_info);
static List* get_tables_to_cluster(MemoryContext cluster_context);
static void reform_and_rewrite_tuple(HeapTuple tuple, TupleDesc oldTupDesc, TupleDesc newTupDesc, Datum* values,
    bool* isnull, bool newRelHasOids, RewriteState rwstate);
static void ReformAndRewriteUTuple(UHeapTuple tuple, TupleDesc oldTupDesc, TupleDesc newTupDesc, Datum* values,
    bool* isnull, bool newRelHasOids, RewriteState rwstate);

static void rebuildPartVacFull(
    Relation oldHeap, Oid partOid, int freezeMinAge, int freezeTableAge, VacuumStmt* vacstmt);
static void RebuildCStoreRelation(
    Relation OldHeap, Oid indexOid, int freeze_min_age, int freeze_table_age, bool verbose, AdaptMem* mem_info);

extern void Start_Prefetch(TableScanDesc scan, SeqScanAccessor* pAccessor, ScanDirection dir);
extern void SeqScan_Init(TableScanDesc Scan, SeqScanAccessor* pAccessor, Relation relation);

static void swap_partition_relfilenode(Oid partitionOid1, Oid partitionOid2, bool swapToastByContent,
    TransactionId frozenXid, MultiXactId multiXid, Oid* mappedTables);
static void partition_relfilenode_swap(Oid OIDOldHeap, Oid OIDNewHeap, uint8 needSwitch);
static void relfilenode_swap(Oid OIDOldHeap, Oid OIDNewHeap, uint8 needSwitch, TransactionId relfrozenxid,
    MultiXactId relMultiXid);
#ifdef ENABLE_MULTIPLE_NODES
static Datum pgxc_parallel_execution(const char* query, ExecNodes* exec_nodes);
static int switch_relfilenode_execnode(Oid relOid1, Oid relOid2, bool isbucket, RedisSwitchNode* rsn);
namespace Tsdb {
static void VacFullCompaction(Relation oldHeap, Oid partOid);
}
#endif
void swapRelationIndicesRelfileNode(Relation rel1, Relation rel2, uint8 needSwitch);
static void GttSwapRelationFiles(Oid r1, Oid r2);
static void HbktModifyPartIndexRelnode(Relation indexRel, Partition indexPart, DataTransferType transferType,
    Oid bucketOid);

/* ---------------------------------------------------------------------------
 * This cluster code allows for clustering multiple tables at once. Because
 * of this, we cannot just run everything on a single transaction, or we
 * would be forced to acquire exclusive locks on all the tables being
 * clustered, simultaneously --- very likely leading to deadlock.
 *
 * To solve this we follow a similar strategy to VACUUM code,
 * clustering each relation in a separate transaction. For this to work,
 * we need to:
 *	- provide a separate memory context so that we can pass information in
 *	  a way that survives across transactions
 *	- start a new transaction every time a new relation is clustered
 *	- check for validity of the information on to-be-clustered relations,
 *	  as someone might have deleted a relation behind our back, or
 *	  clustered one on a different index
 *	- end the transaction
 *
 * The single-relation case does not have any such overhead.
 *
 * We also allow a relation to be specified without index.	In that case,
 * the indisclustered bit will be looked up, and an ERROR will be thrown
 * if there is no index with the bit set.
 *---------------------------------------------------------------------------
 */
void cluster(ClusterStmt* stmt, bool isTopLevel)
{
    /*
     * We cannot run this form of CLUSTER inside a user transaction block;
     * we'd be holding locks way too long.
     */
    PreventTransactionChain(isTopLevel, "CLUSTER");

    if (stmt->relation != NULL) {
        /* This is the single-relation case. */
        Oid tableOid;
        Oid indexOid = InvalidOid;
        Relation rel;
        LOCKMODE lockMode = NoLock;
        Oid partOid = InvalidOid;

        /* Find, lock, and check permissions on the table */
        if (stmt->relation->partitionname == NULL) {
            lockMode = ExclusiveLock;
        } else {
            lockMode = AccessShareLock;
        }

        tableOid = RangeVarGetRelidExtended(
            stmt->relation, lockMode, false, false, false, false, RangeVarCallbackOwnsTable, NULL);
        rel = heap_open(tableOid, NoLock);

        TrForbidAccessRbObject(RelationRelationId, tableOid, stmt->relation->relname);

        /* cluster a specific partition */
        if (stmt->relation->partitionname != NULL) {
            if (!RelationIsPartitioned(rel)) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("table is not partitioned")));
            }

            partOid = partitionNameGetPartitionOid(tableOid,
                stmt->relation->partitionname,
                PART_OBJ_TYPE_TABLE_PARTITION,
                ExclusiveLock,
                false,
                false,
                NULL,
                NULL,
                NoLock);
        }

        /*
         * Reject clustering a remote temp table ... their local buffer
         * manager is not going to cope.
         */
        if (RELATION_IS_OTHER_TEMP(rel))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot cluster temporary tables of other sessions")));

        if (stmt->indexname == NULL) {
            ListCell* index = NULL;

            /* We need to find the index that has indisclustered set. */
            foreach (index, RelationGetIndexList(rel)) {
                HeapTuple idxtuple;
                Form_pg_index indexForm;

                indexOid = lfirst_oid(index);
                idxtuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
                if (!HeapTupleIsValid(idxtuple))
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", indexOid)));

                indexForm = (Form_pg_index)GETSTRUCT(idxtuple);
                if (indexForm->indisclustered) {
                    ReleaseSysCache(idxtuple);
                    break;
                }
                ReleaseSysCache(idxtuple);
                indexOid = InvalidOid;
            }

            if (!OidIsValid(indexOid))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("there is no previously clustered index for table \"%s\"", stmt->relation->relname)));
        } else {
            /*
             * The index is expected to be in the same namespace as the
             * relation.
             */
            indexOid = get_relname_relid(stmt->indexname, rel->rd_rel->relnamespace);
            if (!OidIsValid(indexOid))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg(
                            "index \"%s\" for table \"%s\" does not exist", stmt->indexname, stmt->relation->relname)));
        }

        /* close relation, keep lock till commit */
        heap_close(rel, NoLock);

        /* Do the job */
        cluster_rel(tableOid, partOid, indexOid, false, stmt->verbose, -1, -1, &stmt->memUsage, true);
    } else {
        /*
         * This is the "multi relation" case. We need to cluster all tables
         * that have some index with indisclustered set.
         */
        MemoryContext cluster_context;
        List* rvs = NIL;
        ListCell* rv = NULL;

        /*
         * Create special memory context for cross-transaction storage.
         *
         * Since it is a child of t_thrd.mem_cxt.portal_mem_cxt, it will go away even in case
         * of error.
         */
        cluster_context = AllocSetContextCreate(t_thrd.mem_cxt.portal_mem_cxt,
            "Cluster",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        /*
         * Build the list of relations to cluster.	Note that this lives in
         * cluster_context.
         */
        rvs = get_tables_to_cluster(cluster_context);

        /* Commit to get out of starting transaction */
        PopActiveSnapshot();
        CommitTransactionCommand();

        /* Ok, now that we've got them all, cluster them one by one */
        foreach (rv, rvs) {
            RelToCluster* rvtc = (RelToCluster*)lfirst(rv);

            /* Start a new transaction for each relation. */
            StartTransactionCommand();
            /* functions in indexes may want a snapshot set */
            PushActiveSnapshot(GetTransactionSnapshot());
            cluster_rel(
                rvtc->tableOid, InvalidOid, rvtc->indexOid, true, stmt->verbose, -1, -1, &stmt->memUsage, false);
            PopActiveSnapshot();
            CommitTransactionCommand();
        }

        /* Start a new transaction for the cleanup work. */
        StartTransactionCommand();

        /* Clean up working storage */
        MemoryContextDelete(cluster_context);
    }
}

/*
 * cluster_rel
 *
 * This clusters the table by creating a new, clustered table and
 * swapping the relfilenodes of the new table and the old table, so
 * the OID of the original table is preserved.	Thus we do not lose
 * GRANT, inheritance nor references to this table (this was a bug
 * in releases thru 7.3).
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new table, it's better to create the indexes afterwards than to fill
 * them incrementally while we load the table.
 *
 * If indexOid is InvalidOid, the table will be rewritten in physical order
 * instead of index order.	This is the new implementation of VACUUM FULL,
 * and error messages should refer to the operation as VACUUM not CLUSTER.
 */
void cluster_rel(Oid tableOid, Oid partitionOid, Oid indexOid, bool recheck, bool verbose, int freeze_min_age,
    int freeze_table_age, void* mem_info, bool onerel)
{
    Relation OldHeap;
    LOCKMODE lockMode = NoLock;
    Oid amid = InvalidOid;
    AdaptMem* memUsage = (AdaptMem*)mem_info;

    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();

    /* cluster on hard-coded catalogs are only executed under maintenance mode */
    if (tableOid < FirstBootstrapObjectId && !u_sess->attr.attr_common.xc_maintenance_mode && !IsInitdb) {
        ereport(NOTICE,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                 errmsg("skipping system catalog %u --- use xc_maintenance_mode to CLUSTER it", tableOid)));
        return;
    }

    gstrace_entry(GS_TRC_ID_cluster_rel);
    /*
     * We grab exclusive access to the target rel and index for the duration
     * of the transaction.	(This is redundant for the single-transaction
     * case, since cluster() already did it.)  The index lock is taken inside
     * check_index_is_clusterable.
     */
    if (!OidIsValid(partitionOid)) {
        lockMode = ExclusiveLock;
    } else {
        lockMode = ShareUpdateExclusiveLock;
    }

    if (is_sys_table(tableOid))
        lockMode = AccessExclusiveLock;

    OldHeap = try_relation_open(tableOid, lockMode);

    /* If the table has gone away, we can skip processing it */
    if (!OldHeap) {
        gstrace_exit(GS_TRC_ID_cluster_rel);
        return;
    }

    /* Forbid cluster on shared relation during upgrade, to protect global/pg_filenode.map not changed */
    if (u_sess->attr.attr_common.upgrade_mode != 0 &&
        tableOid < FirstBootstrapObjectId && OldHeap->rd_rel->relisshared &&
        t_thrd.proc->workingVersionNum < RELMAP_4K_VERSION_NUM) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot cluster shared relation during upgrade")));
    }

    /*
     * Since we may open a new transaction for each relation, we have to check
     * that the relation still is what we think it is.
     *
     * If this is a single-transaction CLUSTER, we can skip these tests. We
     * *must* skip the one on indisclustered since it would reject an attempt
     * to cluster a not-previously-clustered index.
     */
    if (recheck) {
        HeapTuple tuple;
        Form_pg_index indexForm;

        /* Check that the user still owns the relation */
        if (!pg_class_ownercheck(tableOid, GetUserId())) {
            relation_close(OldHeap, lockMode);
            gstrace_exit(GS_TRC_ID_cluster_rel);
            return;
        }

        /*
         * Silently skip a temp table for a remote session.  Only doing this
         * check in the "recheck" case is appropriate (which currently means
         * somebody is executing a database-wide CLUSTER), because there is
         * another check in cluster() which will stop any attempt to cluster
         * remote temp tables by name.	There is another check in cluster_rel
         * which is redundant, but we leave it for extra safety.
         */
        if (RELATION_IS_OTHER_TEMP(OldHeap)) {
            relation_close(OldHeap, lockMode);
            gstrace_exit(GS_TRC_ID_cluster_rel);
            return;
        }

        if (OidIsValid(indexOid)) {
            /*
             * Check that the index still exists
             */
            if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(indexOid))) {
                relation_close(OldHeap, lockMode);
                gstrace_exit(GS_TRC_ID_cluster_rel);
                return;
            }

            /*
             * Check that the index is still the one with indisclustered set.
             */
            tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
            if (!HeapTupleIsValid(tuple)) {
                /* probably can't happen */
                relation_close(OldHeap, lockMode);
                gstrace_exit(GS_TRC_ID_cluster_rel);
                return;
            }
            indexForm = (Form_pg_index)GETSTRUCT(tuple);
            if (!indexForm->indisclustered) {
                ReleaseSysCache(tuple);
                relation_close(OldHeap, lockMode);
                gstrace_exit(GS_TRC_ID_cluster_rel);
                return;
            }
            ReleaseSysCache(tuple);
        }
    }

    /*
     * We allow VACUUM FULL, but not CLUSTER, on shared catalogs.  CLUSTER
     * would work in most respects, but the index would only get marked as
     * indisclustered in the current database, leading to unexpected behavior
     * if CLUSTER were later invoked in another database.
     */
    if (OidIsValid(indexOid) && OldHeap->rd_rel->relisshared)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot cluster a shared catalog")));

    /*
     * Don't process temp tables of other backends ... their local buffer
     * manager is not going to cope.
     */
    if (RELATION_IS_OTHER_TEMP(OldHeap)) {
        if (OidIsValid(indexOid))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot cluster temporary tables of other sessions")));
        else
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot vacuum temporary tables of other sessions")));
    }

    if (RelationIsSubPartitioned(OldHeap)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot cluster a subpartition table")));
    }

    if (RELATION_IS_GLOBAL_TEMP(OldHeap) && !gtt_storage_attached(RelationGetRelid(OldHeap))) {
        relation_close(OldHeap, lockMode);
        gstrace_exit(GS_TRC_ID_cluster_rel);
        return;
    }

    if (OldHeap->storage_type == SEGMENT_PAGE) {
        ereport(INFO, (errmsg("skipping segment table \"%s\" --- please use gs_space_shrink "
            "to recycle segment space.", RelationGetRelationName(OldHeap))));
        relation_close(OldHeap, lockMode);
        return;
    }

    /*
     * Also check for active uses of the relation in the current transaction,
     * including open scans and pending AFTER trigger events.
     */
    // for relation, check AccessExclusiveLocked status
    // for partition, call CheckPartitionNotInUse()
    if (!OidIsValid(partitionOid)) {
        CheckTableNotInUse(OldHeap, OidIsValid(indexOid) ? "CLUSTER" : "VACUUM");
    }
    /* Check heap and index are valid to cluster on */
    if (OidIsValid(indexOid))
        check_index_is_clusterable(OldHeap, indexOid, recheck, lockMode, &amid);

    /*
     * There is no data on Coordinator except system tables, it is no sense to rewrite a relation
     * on Coordinator.so we can skip to vacuum full user-define tables
     */
    if (IS_PGXC_COORDINATOR && tableOid >= FirstNormalObjectId) {
        Oid relid = tableOid;
        Oid parentid = InvalidOid;
        Relation* iRel = NULL;
        int nindexes;

        /* Mark the correct index as clustered */
        if (OidIsValid(indexOid)) {
            mark_index_clustered(OldHeap, indexOid);

            /* workload client manager, only btree is need for sort during cluster */
            if (ENABLE_WORKLOAD_CONTROL && OID_IS_BTREE(amid)) {
                /* if operatorMem is already set, the mem check is already done */
                if (memUsage->work_mem == 0) {
                    UtilityDesc desc;
                    errno_t rc = memset_s(&desc, sizeof(UtilityDesc), 0, sizeof(UtilityDesc));
                    securec_check(rc, "\0", "\0");

                    EstIdxMemInfo(OldHeap, NULL, &desc, NULL, NULL);
                    if (!onerel) {
                        desc.cost = g_instance.cost_cxt.disable_cost;
                        desc.query_mem[0] = Max(STATEMENT_MIN_MEM * 1024, desc.query_mem[0]);
                    }
                    WLMInitQueryPlan((QueryDesc*)&desc, false);
                    dywlm_client_manager((QueryDesc*)&desc, false);
                    AdjustIdxMemInfo(memUsage, &desc);
                }
            }
        }
        CBIOpenLocalCrossbucketIndex(OldHeap, RowExclusiveLock, &nindexes, &iRel);
        for (int i = 0; i < nindexes; i++) {
            if (IndexEnableWaitCleanCbi(iRel[i])) {
                cbi_set_enable_clean(iRel[i]);
            }
            index_close(iRel[i], RowExclusiveLock);
        }
        pfree_ext(iRel);

        relation_close(OldHeap, lockMode);

        if (partitionOid) {
            parentid = tableOid;
            relid = partitionOid;
        }

        pgstat_report_vacuum(relid, parentid, false, 0);
        gstrace_exit(GS_TRC_ID_cluster_rel);
        return;
    }

    /*
     * Quietly ignore the request if the a materialized view is not scannable.
     * No harm is done because there is nothing no data to deal with, and we
     * don't want to throw an error if this is part of a multi-relation
     * request -- for example, CLUSTER was run on the entire database.
     */
    if (OldHeap->rd_rel->relkind == RELKIND_MATVIEW &&
        !OldHeap->rd_isscannable)
    {
        relation_close(OldHeap, AccessExclusiveLock);
        return;
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (OldHeap->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        set_stream_off();
    }
#endif

    /*
     * All predicate locks on the tuples or pages are about to be made
     * invalid, because we move tuples around.	Promote them to relation
     * locks.  Predicate locks on indexes will be promoted when they are
     * reindexed.
     */
    TransferPredicateLocksToHeapRelation(OldHeap);

    /* rebuild_relation does all the dirty work */
    if (!RELATION_IS_PARTITIONED(OldHeap)) {
        /*
         * for non partitioned table
         * future Get reloptions
         */
        if (RelationIsColStore(OldHeap))
            RebuildCStoreRelation(OldHeap, indexOid, freeze_min_age, freeze_table_age, verbose, memUsage);
        else
            rebuild_relation(OldHeap, indexOid, freeze_min_age, freeze_table_age, verbose, memUsage);

        /* NB: rebuild_relation does heap_close() on OldHeap */
    } else if (!OidIsValid(partitionOid)) {
        /* for partitioned table */
        rebuildPartitionedTable(OldHeap, indexOid, freeze_min_age, freeze_table_age, verbose, memUsage);
    } else {
        /* for a specific partition */
        rebuildPartition(OldHeap, partitionOid, indexOid, freeze_min_age, freeze_table_age, verbose, memUsage);
    }
    gstrace_exit(GS_TRC_ID_cluster_rel);
}

/*
 * Verify that the specified heap and index are valid to cluster on
 *
 * Side effect: obtains exclusive lock on the index.  The caller should
 * already have exclusive lock on the table, so the index lock is likely
 * redundant, but it seems best to grab it anyway to ensure the index
 * definition can't change under us.
 */
void check_index_is_clusterable(Relation OldHeap, Oid indexOid, bool recheck, LOCKMODE lockmode, Oid* amid)
{
    Relation OldIndex;

    OldIndex = index_open(indexOid, lockmode);

    /*
     * Check that index is in fact an index on the given relation
     */
    if (OldIndex->rd_index == NULL || OldIndex->rd_index->indrelid != RelationGetRelid(OldHeap))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not an index for table \"%s\"",
                    RelationGetRelationName(OldIndex),
                    RelationGetRelationName(OldHeap))));

    /* Index AM must allow clustering */
    if (!OldIndex->rd_am->amclusterable)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot cluster on index \"%s\" because access method does not support clustering",
                    RelationGetRelationName(OldIndex))));

    /*
     * Disallow clustering on incomplete indexes (those that might not index
     * every row of the relation).	We could relax this by making a separate
     * seqscan pass over the table to copy the missing rows, but that seems
     * expensive and tedious.
     */
	if (!tableam_tops_tuple_attisnull(OldIndex->rd_indextuple, Anum_pg_index_indpred, NULL))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot cluster on partial index \"%s\"", RelationGetRelationName(OldIndex))));

    /*
     * Disallow if index is left over from a failed CREATE INDEX CONCURRENTLY;
     * it might well not contain entries for every heap row, or might not even
     * be internally consistent.  (But note that we don't check indcheckxmin;
     * the worst consequence of following broken HOT chains would be that we
     * might put recently-dead tuples out-of-order in the new table, and there
     * is little harm in that.)
     */
    if (!IndexIsValid(OldIndex->rd_index))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot cluster on invalid index \"%s\"", RelationGetRelationName(OldIndex))));

    if (amid != NULL)
        *amid = OldIndex->rd_rel->relam;

    /* Drop relcache refcnt on OldIndex, but keep lock */
    index_close(OldIndex, NoLock);
}

/*
 * mark_index_clustered: mark the specified index as the one clustered on
 *
 * With indexOid == InvalidOid, will mark all indexes of rel not-clustered.
 *
 * Note: we do transactional updates of the pg_index rows, which are unsafe
 * against concurrent SnapshotNow scans of pg_index.  Therefore this is unsafe
 * to execute with less than full exclusive lock on the parent table;
 * otherwise concurrent executions of RelationGetIndexList could miss indexes.
 */
void mark_index_clustered(Relation rel, Oid indexOid)
{
    HeapTuple indexTuple;
    Form_pg_index indexForm;
    Relation pg_index;
    ListCell* index = NULL;

    /*
     * If the index is already marked clustered, no need to do anything.
     */
    if (OidIsValid(indexOid)) {
        indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
        if (!HeapTupleIsValid(indexTuple))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", indexOid)));
        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        if (indexForm->indisclustered) {
            ReleaseSysCache(indexTuple);
            return;
        }

        ReleaseSysCache(indexTuple);
    }

    /*
     * Check each index of the relation and set/clear the bit as needed.
     */
    pg_index = heap_open(IndexRelationId, RowExclusiveLock);

    foreach (index, RelationGetIndexList(rel, true)) {
        Oid thisIndexOid = lfirst_oid(index);

        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(thisIndexOid));
        if (!HeapTupleIsValid(indexTuple))
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", thisIndexOid)));
        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        /*
         * Unset the bit if set.  We know it's wrong because we checked this
         * earlier.
         */
        if (indexForm->indisclustered) {
            indexForm->indisclustered = false;
            simple_heap_update(pg_index, &indexTuple->t_self, indexTuple);
            CatalogUpdateIndexes(pg_index, indexTuple);
        } else if (thisIndexOid == indexOid) {
            /* this was checked earlier, but let's be real sure */
            if (!IndexIsValid(indexForm))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("cannot cluster on invalid index %u", indexOid)));
            indexForm->indisclustered = true;
            simple_heap_update(pg_index, &indexTuple->t_self, indexTuple);
            CatalogUpdateIndexes(pg_index, indexTuple);
        }
        heap_freetuple(indexTuple);
    }

    heap_close(pg_index, RowExclusiveLock);
}

/*
 * rebuild_relation: rebuild an existing relation in index or physical order
 *
 * OldHeap: table to rebuild --- must be opened and exclusive-locked!
 * indexOid: index to cluster by, or InvalidOid to rewrite in physical order.
 *
 * NB: this routine closes OldHeap at the right time; caller should not.
 */
static void rebuild_relation(
    Relation OldHeap, Oid indexOid, int freeze_min_age, int freeze_table_age, bool verbose, AdaptMem* memUsage)
{
    Oid tableOid = RelationGetRelid(OldHeap);
    Oid tableSpace = OldHeap->rd_rel->reltablespace;
    Oid OIDNewHeap;
    bool is_system_catalog = false;
    bool swap_toast_by_content = false;
    TransactionId frozenXid = InvalidTransactionId;
    double deleteTupleNum = 0;
    bool is_shared = OldHeap->rd_rel->relisshared;
    MultiXactId multiXid;

    /* Mark the correct index as clustered */
    if (OidIsValid(indexOid))
        mark_index_clustered(OldHeap, indexOid);

    /* Remember if it's a system catalog */
    is_system_catalog = IsSystemRelation(OldHeap);

    /* Close relcache entry, but keep lock until transaction commit */
    heap_close(OldHeap, NoLock);

    /* Create the transient table that will receive the re-ordered data */
    OIDNewHeap = make_new_heap(tableOid, tableSpace, ExclusiveLock);

    /* Copy the heap data into the new table in the desired order */
    copy_heap_data(OIDNewHeap,
        tableOid,
        indexOid,
        freeze_min_age,
        freeze_table_age,
        verbose,
        &swap_toast_by_content,
        &frozenXid,
        &multiXid,
        &deleteTupleNum,
        memUsage);

    /*
     * We must hold AccessExclusiveLock before finish_heap_swap in order to block
     * select statement until transaction commit. Because vacumm full have done
     * lots of work by here, so we enlarge deadlock-check time for vacuum full thread
     * to avoid vacuum full/cluster table failed.
     */
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    LockRelationOid(tableOid, AccessExclusiveLock);

    /*
     * Swap the physical files of the target and transient tables, then
     * rebuild the target's indexes and throw away the transient table.
     */
    finish_heap_swap(tableOid, OIDNewHeap, is_system_catalog, swap_toast_by_content,
                     false, frozenXid, multiXid, memUsage);

    /* report vacuum full stat to PgStatCollector */
    pgstat_report_vacuum(tableOid, InvalidOid, is_shared, deleteTupleNum);
    /* clear all attrinitdefval for alter-table-instantly feature */
    clearAttrInitDefVal(tableOid);
}

void getPartitionRelxids(Relation ordTableRel, TransactionId* frozenXid, MultiXactId* multiXid)
{
    Relation rel = heap_open(PartitionRelationId, AccessShareLock);
    HeapTuple tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(RelationGetRelid(ordTableRel)));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(ordTableRel))));
    }
    bool isNull = true;
    Datum xid64datum =
        tableam_tops_tuple_getattr(tuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);

    if (isNull) {
        *frozenXid = ordTableRel->rd_rel->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, *frozenXid) ||
            !TransactionIdIsNormal(*frozenXid))
            *frozenXid = FirstNormalTransactionId;
    } else {
        *frozenXid = DatumGetTransactionId(xid64datum);
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (multiXid != NULL) {
        xid64datum =
            tableam_tops_tuple_getattr(tuple, Anum_pg_partition_relminmxid, RelationGetDescr(rel), &isNull);
        *multiXid = isNull ? InvalidMultiXactId : DatumGetTransactionId(xid64datum);
    }
#endif

    heap_close(rel, AccessShareLock);
    heap_freetuple(tuple);
}

void getRelationRelxids(Relation ordTableRel, TransactionId* frozenXid, MultiXactId* multiXid)
{
    bool isNull = true;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(ordTableRel)));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(ordTableRel))));
    }
    Datum xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);

    if (isNull) {
        *frozenXid = ordTableRel->rd_rel->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, *frozenXid) ||
            !TransactionIdIsNormal(*frozenXid))
            *frozenXid = FirstNormalTransactionId;
    } else {
        *frozenXid = DatumGetTransactionId(xid64datum);
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (multiXid != NULL) {
        xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_class_relminmxid, RelationGetDescr(rel), &isNull);
        *multiXid = isNull ? InvalidMultiXactId : DatumGetTransactionId(xid64datum);
    }
#endif

    heap_close(rel, AccessShareLock);
    heap_freetuple(tuple);
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        : rebuild an existing relation in index or physical order
 * Description  :
 * Notes        :
 * Input        :
 * Output       : NA
 */
static void rebuildPartitionedTable(
    Relation partTableRel, Oid indexOid, int freezeMinAge, int freezeTableAge, bool verbose, AdaptMem* memUsage)
{
    Oid partTableOid = RelationGetRelid(partTableRel);
    Oid OIDNewHeap = InvalidOid;
    bool swapToastByContent = false;
    TransactionId* frozenXid = NULL;
    MultiXactId* multiXid = NULL;

    TupleDesc partTabHeapDesc;
    HeapTuple tuple = NULL;
    Datum partTabRelOptions = 0;
    bool isNull = false;
    int reindexFlags = 0;
    bool isCStore = RelationIsColStore(partTableRel);

    PlannerInfo* root = NULL;
    Query* query = NULL;
    PlannerGlobal* glob = NULL;
    RangeTblEntry* rte = NULL;
    RelOptInfo* relOptInfo = NULL;

    List* partitions = NIL;
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation partRel = NULL;
    Relation newHeap = NULL;
    double deleteTuplesNum = -1;
    double taotaldeleteTuples = 0;

    /* remember all the new partition heap oid. */
    Oid* OIDNewHeapArray = NULL;
    int OIDNewHeapArrayLen = 0;
    int pos = 0;
    int loc = 0;
    int temp = 0;

    /* Mark the correct index as clustered */
    if (OidIsValid(indexOid)) {
        mark_index_clustered(partTableRel, indexOid);
    }

    /* get desc of partitioned table */
    partTabHeapDesc = RelationGetDescr(partTableRel);

    tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(partTableOid), LOG);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", partTableOid)));
    }

    /* get RelOptInfo of partitioned table */
    partTabRelOptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
    if (isNull) {
        partTabRelOptions = (Datum)0;
    }

    /* Set up mostly-dummy planner state */
    query = makeNode(Query);
    query->commandType = CMD_SELECT;

    glob = makeNode(PlannerGlobal);

    root = makeNode(PlannerInfo);
    root->parse = query;
    root->glob = glob;
    root->query_level = 1;
    root->planner_cxt = CurrentMemoryContext;
    root->wt_param_id = -1;

    /* Build a minimal RTE for the RelOptInfo */
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = partTableOid;
    rte->ispartrel = true;
    rte->relkind = RELKIND_RELATION;
    rte->inh = false;
    rte->inFromCl = true;
    query->rtable = list_make1(rte);

    /* Set up RTE/RelOptInfo arrays */
    setup_simple_rel_arrays(root);

    /* Build RelOptInfo */
    relOptInfo = build_simple_rel(root, 1, RELOPT_BASEREL);

    /* 3. plan cluster on every partition */
    partitions = relationGetPartitionList(partTableRel, ExclusiveLock);
    OIDNewHeapArrayLen = list_length(partitions);
    OIDNewHeapArray = (Oid*)palloc(sizeof(Oid) * OIDNewHeapArrayLen);
    frozenXid = (TransactionId*)palloc(sizeof(TransactionId) * OIDNewHeapArrayLen);
    multiXid = (MultiXactId*)palloc(sizeof(MultiXactId) * OIDNewHeapArrayLen);
    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        partRel = partitionGetRelation(partTableRel, partition);

        /* we need to transfre locks here. */
        TransferPredicateLocksToHeapRelation(partRel);

        /* get pages and tuples of partition */
        estimatePartitionSize(partTableRel,
            PartitionGetPartid(partition),
            relOptInfo->attr_widths - relOptInfo->min_attr,
            &(relOptInfo->pages),
            &(relOptInfo->tuples),
            &(relOptInfo->allvisfrac));

        /*
         * Rather than doing all the pushups that would be needed to use
         * set_baserel_size_estimates, just do a quick hack for rows and width.
         */
        relOptInfo->rows = relOptInfo->tuples;
        relOptInfo->width = getPartitionDataWidth(partRel, NULL);
        root->total_table_pages = relOptInfo->pages;

        /* make a temp table for swapping partition */
        OIDNewHeap = makePartitionNewHeap(partTableRel,
            partTabHeapDesc,
            partTabRelOptions,
            partRel->rd_id,
            partRel->rd_rel->reltoastrelid,
            partRel->rd_rel->reltablespace,
            isCStore);

        /* remember each Oid of new partition heap. */
        OIDNewHeapArray[pos++] = OIDNewHeap;

        /* Copy the heap data into the new table in the desired order */
        newHeap = heap_open(OIDNewHeap, AccessExclusiveLock);
        if (isCStore)
            CopyCStoreData(partRel,
                newHeap,
                freezeMinAge,
                freezeTableAge,
                verbose,
                &swapToastByContent,
                &frozenXid[loc++],
                memUsage);
        else {
            temp = loc++;
            copyPartitionHeapData(newHeap,
                partRel,
                indexOid,
                root,
                relOptInfo,
                freezeMinAge,
                freezeTableAge,
                verbose,
                &swapToastByContent,
                &frozenXid[temp],
                &multiXid[temp],
                memUsage,
                &deleteTuplesNum);
        }

        heap_close(newHeap, NoLock);
        releaseDummyRelation(&partRel);

        taotaldeleteTuples += deleteTuplesNum;
        pgstat_report_vacuum(PartitionGetPartid(partition), partTableOid, false, deleteTuplesNum);
    }
    Assert(pos == OIDNewHeapArrayLen);

    // We must hold AccessExluviseLock before swap relfile node in order to prevent
    // from select statement.  Because vacumm full have done lots of work by here,
    // so we delay dead lock check for vacuum full thread to avoid vacuum full/cluster
    // table failed.
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    LockRelation(partTableRel, AccessExclusiveLock);

    // Swap relation file node after holding AccessExclusiveLock on
    // logical parent relation
    pos = 0;
    loc = 0;
    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        partRel = partitionGetRelation(partTableRel, partition);
        OIDNewHeap = OIDNewHeapArray[pos++];

        temp = loc++;
        /* swap the temp table and partition */
        finishPartitionHeapSwap(partRel->rd_id, OIDNewHeap, swapToastByContent, frozenXid[temp],
                                isCStore ? InvalidMultiXactId : multiXid[temp]);

        /* release this partition relation. */
        releaseDummyRelation(&partRel);
    }
    Assert(pos == OIDNewHeapArrayLen);

    ReleaseSysCache(tuple);

    releasePartitionList(partTableRel, &partitions, ExclusiveLock);
    heap_close(partTableRel, NoLock);

    if (!isCStore) {
        /* clear all attrinitdefval for alter-table-instantly feature */
        clearAttrInitDefVal(RelationGetRelid(partTableRel));
        pgstat_report_vacuum(partTableOid, InvalidOid, false, taotaldeleteTuples);
    } else {
        pgstat_report_vacuum(partTableOid, InvalidOid, false, -1);
    }

    /* rebuild index of partitioned table */
    reindexFlags = REINDEX_REL_SUPPRESS_INDEX_USE;
    (void)ReindexRelation(partTableOid, reindexFlags, REINDEX_ALL_INDEX, NULL);

    /* drop the temp tables for swapping */
    for (int i = 0; i < OIDNewHeapArrayLen; ++i) {
        ObjectAddress object;

        object.classId = RelationRelationId;
        object.objectId = OIDNewHeapArray[i];
        object.objectSubId = 0;

        performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }
    pfree_ext(OIDNewHeapArray);
    pfree_ext(frozenXid);
}

static void rebuildPartition(Relation partTableRel, Oid partitionOid, Oid indexOid, int freezeMinAge,
    int freezeTableAge, bool verbose, AdaptMem* memUsage)
{
    Oid partTableOid = RelationGetRelid(partTableRel);
    Oid OIDNewHeap = InvalidOid;
    bool swapToastByContent = false;
    TransactionId frozenXid = InvalidTransactionId;
    MultiXactId multiXid = InvalidMultiXactId;
    bool isCStore = RelationIsColStore(partTableRel);

    TupleDesc partTabHeapDesc;
    HeapTuple tuple = NULL;
    Datum partTabRelOptions = 0;
    bool isNull = false;
    int reindexFlags = 0;

    PlannerInfo* root = NULL;
    Query* query = NULL;
    PlannerGlobal* glob = NULL;
    RangeTblEntry* rte = NULL;
    RelOptInfo* relOptInfo = NULL;

    Partition partition = NULL;
    Relation partRel = NULL;
    Relation newHeap = NULL;
    ObjectAddress object;
    const char* stmt = OidIsValid(indexOid) ? "CLUSTER" : "VACUUM";
    double deleteTuplesNum = -1;

    /* Mark the correct index as clustered */
    if (OidIsValid(indexOid)) {
        mark_index_clustered(partTableRel, indexOid);
    }

    /* 1. get desc of partitioned table */
    partTabHeapDesc = RelationGetDescr(partTableRel);

    tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(partTableOid), LOG);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", partTableOid)));
    }
    partTabRelOptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
    if (isNull) {
        partTabRelOptions = (Datum)0;
    }

    /* 2. get RelOptInfo of partitioned table */
    /* Set up mostly-dummy planner state */
    query = makeNode(Query);
    query->commandType = CMD_SELECT;

    glob = makeNode(PlannerGlobal);

    root = makeNode(PlannerInfo);
    root->parse = query;
    root->glob = glob;
    root->query_level = 1;
    root->planner_cxt = CurrentMemoryContext;
    root->wt_param_id = -1;

    /* Build a minimal RTE for the RelOptInfo */
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = partTableOid;
    rte->ispartrel = true;
    rte->relkind = RELKIND_RELATION;
    rte->inh = false;
    rte->inFromCl = true;
    query->rtable = list_make1(rte);

    /* Set up RTE/RelOptInfo arrays */
    setup_simple_rel_arrays(root);

    /* Build RelOptInfo */
    relOptInfo = build_simple_rel(root, 1, RELOPT_BASEREL);

    /*
     * 3. plan cluster on the specific partition
     * 3.1 copy data from old partition file to new relation file
     */
    partition = partitionOpen(partTableRel, partitionOid, ExclusiveLock);
    partRel = partitionGetRelation(partTableRel, partition);

    /* get pages and tuples of partition */
    estimatePartitionSize(partTableRel,
        PartitionGetPartid(partition),
        relOptInfo->attr_widths - relOptInfo->min_attr,
        &(relOptInfo->pages),
        &(relOptInfo->tuples),
        &(relOptInfo->allvisfrac));

    /*
     * Rather than doing all the pushups that would be needed to use
     * set_baserel_size_estimates, just do a quick hack for rows and width.
     */
    relOptInfo->rows = relOptInfo->tuples;
    relOptInfo->width = getPartitionDataWidth(partRel, NULL);
    root->total_table_pages = relOptInfo->pages;

    /* make a temp table for swapping partition */
    OIDNewHeap = makePartitionNewHeap(partTableRel,
        partTabHeapDesc,
        partTabRelOptions,
        partRel->rd_id,
        partRel->rd_rel->reltoastrelid,
        partRel->rd_rel->reltablespace,
        isCStore);
    object.classId = RelationRelationId;
    object.objectId = OIDNewHeap;
    object.objectSubId = 0;

    /* Copy the heap data into the new table in the desired order */
    newHeap = heap_open(OIDNewHeap, AccessExclusiveLock);
    if (isCStore) {
        CopyCStoreData(
            partRel, newHeap, freezeMinAge, freezeTableAge, verbose, &swapToastByContent, &frozenXid, memUsage);

        /*
         * If this is a colstore partition table, we must hold AccessExclusiveLock on
         * logical parent relation before swap file node. Because vacumm full have done
         * lots of work by here, so we delay dead lock check for vacuum full thread
         * to avoid vacuum full/cluster table failed.
         */
        t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
        LockRelation(partTableRel, AccessExclusiveLock);
    } else {
        copyPartitionHeapData(newHeap,
            partRel,
            indexOid,
            root,
            relOptInfo,
            freezeMinAge,
            freezeTableAge,
            verbose,
            &swapToastByContent,
            &frozenXid,
            &multiXid,
            memUsage,
            &deleteTuplesNum);
    }

    heap_close(newHeap, NoLock);
    partitionClose(partTableRel, partition, NoLock);
    releaseDummyRelation(&partRel);

    /* 3.2 Swap refilenode, this op need a AccessExclusiveLock */
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    partition = partitionOpenWithRetry(partTableRel, partitionOid, AccessExclusiveLock, stmt);

    if (!partition) {
        /* 4.last step, clean up */
        ReleaseSysCache(tuple);
        heap_close(partTableRel, NoLock);

        /* drop the temp table for swapping */
        performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("could not acquire AccessExclusiveLock on dest table partition \"%s\", %s failed",
                    getPartitionName(partitionOid, false),
                    stmt)));
    } else {
        CheckPartitionNotInUse(partition, stmt);
        partRel = partitionGetRelation(partTableRel, partition);
        /*
         * we need to transfre locks here.
         */
        TransferPredicateLocksToHeapRelation(partRel);
        /* swap the temp table and partition */
        finishPartitionHeapSwap(partRel->rd_id, OIDNewHeap, swapToastByContent, frozenXid, multiXid);
        /* rebuild index of partition table */
        reindexFlags = REINDEX_REL_SUPPRESS_INDEX_USE;
        (void)reindexPartition(RelationGetRelid(partTableRel), partitionOid, reindexFlags, REINDEX_ALL_INDEX);

        /* close partition */
        partitionClose(partTableRel, partition, NoLock);
        releaseDummyRelation(&partRel);

        /* 4.last step, clean up */
        ReleaseSysCache(tuple);
        heap_close(partTableRel, NoLock);

        /* drop the temp table for swapping */
        performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }

    pgstat_report_vacuum(partitionOid, partTableOid, false, deleteTuplesNum);
}

/*
 * @Description: add Partial Cluster Key for new realtion.
 * @Param[IN] OIDNewHeap: new heap relation oid
 * @Param[IN] tupleDesc:  includes PCK info
 * @See also:
 */
static void CopyPartialClusterKeyToNewRelation(Oid OIDNewHeap, TupleDesc tupleDesc)
{
    TupleConstr* constr = tupleDesc->constr;

    if (tupledesc_have_pck(constr)) {
        Relation newRel = NULL;
        Constraint* pck = makeNode(Constraint);
        int const pckNum = constr->clusterKeyNum;
        int pckCnt = 0;

        /* get attribute name list for PCK */
        for (pckCnt = 0; pckCnt < pckNum; ++pckCnt) {
            AttrNumber attrNum = constr->clusterKeys[pckCnt];
            Form_pg_attribute attribute = tupleDesc->attrs[attrNum - 1];
            char* attrName = NameStr(attribute->attname);

            pck->contype = CONSTR_CLUSTER;
            pck->location = -1;
            pck->keys = lappend(pck->keys, makeString(attrName));
        }

        /* add PCK for new relation.
         * it's ok to use AccessExclusiveLock during creating relation.
         */
        newRel = relation_open(OIDNewHeap, AccessExclusiveLock);
        AddRelClusterConstraints(newRel, list_make1(pck));
        relation_close(newRel, NoLock);

        pfree_ext(pck);

        /*
         * Advance command counter so that the newly-created relation's catalog
         * tuples will be visible to heap_open/relation_open.
         */
        CommandCounterIncrement();
    }
}

/*
 * Create the transient table that will be filled with new data during
 * CLUSTER, ALTER TABLE, and similar operations.  The transient table
 * duplicates the logical structure of the OldHeap, but is placed in
 * NewTableSpace which might be different from OldHeap's.
 *
 * After this, the caller should load the new heap with transferred/modified
 * data, then call finish_heap_swap to complete the operation.
 */
Oid make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, int lockMode)
{
    TupleDesc OldHeapDesc;
    char NewHeapName[NAMEDATALEN];
    Oid OIDNewHeap;
    Oid toastid;
    Relation OldHeap;
    HeapTuple tuple;
    Datum reloptions;
    bool isNull = false;
    int ss_c = 0;
    HashBucketInfo bucketinfo;

    OldHeap = heap_open(OIDOldHeap, lockMode);
    OldHeapDesc = RelationGetDescr(OldHeap);

    /*
     * Note that the NewHeap will not receive any of the defaults or
     * constraints associated with the OldHeap; we don't need 'em, and there's
     * no reason to spend cycles inserting them into the catalogs only to
     * delete them.
     */
    /*
     * But we do want to use reloptions of the old heap for new heap.
     */
    tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(OIDOldHeap), LOG);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", OIDOldHeap)));
    reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
    if (isNull)
        reloptions = (Datum)0;

    /*
     * Create the new heap, using a temporary name in the same namespace as
     * the existing table.	NOTE: there is some risk of collision with user
     * relnames.  Working around this seems more trouble than it's worth; in
     * particular, we can't create the new heap in a different namespace from
     * the old, or we will have problems with the TEMP status of temp tables.
     *
     * Note: the new heap is not a shared relation, even if we are rebuilding
     * a shared rel.  However, we do make the new heap mapped if the source is
     * mapped.	This simplifies swap_relation_files, and is absolutely
     * necessary for rebuilding pg_class, for reasons explained there.
     */
    ss_c = snprintf_s(NewHeapName, sizeof(NewHeapName), sizeof(NewHeapName) - 1, "pg_temp_%u", OIDOldHeap);
    securec_check_ss_c(ss_c, "\0", "\0");

    bucketinfo.bucketOid = RelationGetBucketOid(OldHeap);
    OIDNewHeap = heap_create_with_catalog(NewHeapName,
        RelationGetNamespace(OldHeap),
        NewTableSpace,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        OldHeap->rd_rel->relowner,
        OldHeapDesc,
        NIL,
        OldHeap->rd_rel->relkind,
        OldHeap->rd_rel->relpersistence,
        false,
        RelationIsMapped(OldHeap),
        true,
        0,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        NULL,
        RELATION_GET_CMPRS_ATTR(OldHeap),
        RELATION_CREATE_BUCKET(OldHeap) ? &bucketinfo : NULL,
        true,
        NULL,
        RelationGetStorageType(OldHeap));
    Assert(OIDNewHeap != InvalidOid);

    ReleaseSysCache(tuple);

    /*
     * Advance command counter so that the newly-created relation's catalog
     * tuples will be visible to heap_open.
     */
    CommandCounterIncrement();

    /* remember PCK info of columar relation */
    if (RelationIsColStore(OldHeap)) {
        CopyPartialClusterKeyToNewRelation(OIDNewHeap, OldHeapDesc);
    }

    ss_c = snprintf_s(NewHeapName, sizeof(NewHeapName), sizeof(NewHeapName) - 1, "pg_temp_%u", OIDNewHeap);
    securec_check_ss_c(ss_c, "\0", "\0");

    updateRelationName(OIDNewHeap, false, NewHeapName);

    /*
     * If necessary, create a TOAST table for the new relation.
     *
     * If the relation doesn't have a TOAST table already, we can't need one
     * for the new relation.  The other way around is possible though: if some
     * wide columns have been dropped, AlterTableCreateToastTable can decide
     * that no TOAST table is needed for the new table.
     *
     * Note that AlterTableCreateToastTable ends with CommandCounterIncrement,
     * so that the TOAST table will be visible for insertion.
     */
    toastid = OldHeap->rd_rel->reltoastrelid;
    if (OidIsValid(toastid)) {
        /* keep the existing toast table's reloptions, if any */
        tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(toastid), LOG);
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", toastid)));
        reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
        if (isNull)
            reloptions = (Datum)0;

        AlterTableCreateToastTable(OIDNewHeap, reloptions);

        ReleaseSysCache(tuple);
    }

    if (RelationIsColStore(OldHeap)) {
        if (RelationIsCUFormat(OldHeap))
            AlterCStoreCreateTables(OIDNewHeap, 0, NULL);
        else
            AlterDfsCreateTables(OIDNewHeap, 0, NULL);
    }
    heap_close(OldHeap, NoLock);

    return OIDNewHeap;
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        : scan or rewrite one partitioned table
 * Description  :
 * Notes        :
 * Input        :
 * Output       : oid of new heap
 */
Oid makePartitionNewHeap(Relation partitionedTableRel, TupleDesc partTabHeapDesc, Datum partTabRelOptions,
    Oid oldPartOid, Oid partToastOid, Oid NewTableSpace, bool isCStore, Oid subpartFilenode)
{
    char NewHeapName[NAMEDATALEN];
    Oid OIDNewHeap = InvalidOid;
    HeapTuple tuple = NULL;
    Datum reloptions = 0;
    bool isNull = false;
    int ss_c = 0;
    HashBucketInfo bucketinfo;

    /*
     * Create the new heap, using a temporary name in the same namespace as
     * the existing table.
     */
    ss_c = snprintf_s(NewHeapName, sizeof(NewHeapName), sizeof(NewHeapName) - 1, "pg_temp_%u", oldPartOid);
    securec_check_ss(ss_c, "\0", "\0");
    bucketinfo.bucketOid = RelationGetBucketOid(partitionedTableRel);
    OIDNewHeap = heap_create_with_catalog(NewHeapName,
        RelationGetNamespace(partitionedTableRel),
        NewTableSpace,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        partitionedTableRel->rd_rel->relowner,
        partTabHeapDesc,
        NIL,
        partitionedTableRel->rd_rel->relkind,
        partitionedTableRel->rd_rel->relpersistence,
        false,
        subpartFilenode == InvalidOid ? RelationIsMapped(partitionedTableRel) : false,
        true,
        0,
        ONCOMMIT_NOOP,
        partTabRelOptions,
        false,
        true,
        NULL,
        RELATION_GET_CMPRS_ATTR(partitionedTableRel),
        RELATION_OWN_BUCKETKEY(partitionedTableRel) ? &bucketinfo : NULL,
        true,
        NULL,
        RelationGetStorageType(partitionedTableRel));
    Assert(OIDNewHeap != InvalidOid);
    /*
     * Advance command counter so that the newly-created relation's catalog
     * tuples will be visible to heap_open.
     */
    CommandCounterIncrement();

    ss_c = snprintf_s(NewHeapName, sizeof(NewHeapName), sizeof(NewHeapName) - 1, "pg_temp_%u", OIDNewHeap);
    securec_check_ss(ss_c, "\0", "\0");
    updateRelationName(OIDNewHeap, false, NewHeapName);

    /* remember PCK info of columar relation partition */
    if (isCStore) {
        CopyPartialClusterKeyToNewRelation(OIDNewHeap, partTabHeapDesc);
    }

    /*
     * If necessary, create a TOAST table for the new relation.
     */
    if (OidIsValid(partToastOid)) {
        /* keep the existing toast table's reloptions, if any */
        tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(partToastOid), LOG);
        if (!HeapTupleIsValid(tuple)) {
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", partToastOid)));
        }
        reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
        if (isNull) {
            reloptions = (Datum)0;
        }

        AlterTableCreateToastTable(OIDNewHeap, reloptions);

        ReleaseSysCache(tuple);
    }

    if (isCStore)
        AlterCStoreCreateTables(OIDNewHeap, (Datum)0, NULL);
    return OIDNewHeap;
}

/*
 * @@GaussDB@@
 * Target       : log
 * Brief        : Log what we're doing about clustering.
 * Description  :
 * Notes        :
 * Input        :
 * Output       :
 */
static void ClusterRunMsg(
    Relation tblRelation, Relation indexRelation, IndexScanDesc indexScan, Tuplesortstate* tuplesort, bool verbose)
{
    int elevel = verbose ? VERBOSEMESSAGE : DEBUG2;
    if (indexScan != NULL) {
        ereport(elevel,
            (errcode(ERRCODE_LOG),
                errmsg("clustering \"%s.%s\" using index scan on \"%s\"",
                    get_namespace_name(RelationGetNamespace(tblRelation)),
                    RelationGetRelationName(tblRelation),
                    RelationGetRelationName(indexRelation))));
    } else if (tuplesort != NULL) {
        ereport(elevel,
            (errcode(ERRCODE_LOG),
                errmsg("clustering \"%s.%s\" using sequential scan and sort",
                    get_namespace_name(RelationGetNamespace(tblRelation)),
                    RelationGetRelationName(tblRelation))));
    } else {
        ereport(elevel,
            (errcode(ERRCODE_LOG),
                errmsg("vacuuming \"%s.%s\"",
                    get_namespace_name(RelationGetNamespace(tblRelation)),
                    RelationGetRelationName(tblRelation))));
    }
}

double CopyUHeapDataInternal(Relation oldHeap, Relation oldIndex, Relation newHeap, TransactionId oldestXmin,
    TransactionId freezeXid, bool verbose, bool useSort, const AdaptMem *memUsage)
{
    TupleDesc oldTupDesc;
    TupleDesc newTupDesc;
    Relation heapRelation = NULL;
    int natts;
    Datum *values = NULL;
    bool *isnull = NULL;
    IndexScanDesc indexScan;
    UHeapScanDesc heapScan;
    bool useWal = XLogIsNeeded() && RelationNeedsWAL(newHeap);
    RewriteState rwstate;
    Tuplesortstate *tuplesort = NULL;
    double num_tuples = 0;
    double tups_vacuumed = 0;
    double tups_recently_dead = 0;
    int elevel = verbose ? VERBOSEMESSAGE : DEBUG2;
    int messageLevel = -1;
    PGRUsage ru0;
    SeqScanAccessor scanaccessor;
    TupleTableSlot *slot = NULL;

    pg_rusage_init(&ru0);

    /* use_wal off requires smgr_targblock be initially invalid */
    Assert(RelationGetTargetBlock(newHeap) == InvalidBlockNumber);

    /*
     * Their tuple descriptors should be exactly alike, but here we only need
     * assume that they have the same number of columns.
     */
    oldTupDesc = RelationGetDescr(oldHeap);
    newTupDesc = RelationGetDescr(newHeap);
    Assert(newTupDesc->natts == oldTupDesc->natts);

    /* Preallocate values/isnull arrays */
    natts = newTupDesc->natts;
    values = (Datum *)palloc(natts * sizeof(Datum));
    isnull = (bool *)palloc(natts * sizeof(bool));

    slot = MakeSingleTupleTableSlot(oldTupDesc, false, oldTupDesc->tdTableAmType);

    /* Initialize the rewrite operation */
    rwstate = begin_heap_rewrite(oldHeap, newHeap, oldestXmin, freezeXid, useWal);

    /* Set up sorting if wanted */
    if (useSort) {
        int workMem = (memUsage->work_mem > 0) ? memUsage->work_mem : u_sess->attr.attr_memory.maintenance_work_mem;
        int maxMem = memUsage->max_mem;
        tuplesort = tuplesort_begin_cluster(oldTupDesc, oldIndex, workMem, false, maxMem, true);
    } else {
        tuplesort = NULL;
    }

    /*
     * Prepare to scan the oldHeap.  To ensure we see recently-dead tuples
     * that still need to be copied, we scan with SnapshotAny and use
     * HeapTupleSatisfiesVacuum for the visibility test.
     */
    if (oldIndex != NULL && !useSort) {
        heapScan = NULL;
        if (RelationIsGlobalIndex(oldIndex)) {
            /* Open the parent heap relation. */
            Oid heapId = IndexGetRelation(RelationGetRelid(oldIndex), false);
            heapRelation = heap_open(heapId, NoLock);
            indexScan = index_beginscan(heapRelation, oldIndex, SnapshotAny, 0, 0);
        } else {
            indexScan = (IndexScanDesc)index_beginscan(oldHeap, oldIndex, SnapshotAny, 0, 0);
        }
        index_rescan(indexScan, NULL, 0, NULL, 0);
    } else {
        heapScan = (UHeapScanDesc)UHeapBeginScan(oldHeap, SnapshotAny, 0);
        indexScan = NULL;
        ADIO_RUN()
        {
            SeqScan_Init((TableScanDesc)heapScan, &scanaccessor, oldHeap);
        }
        ADIO_END();
    }

    /* Log what we're doing */
    if (indexScan != NULL)
        ereport(elevel, (errcode(ERRCODE_LOG),
            errmsg("clustering \"%s.%s\" using index scan on \"%s\"", get_namespace_name(RelationGetNamespace(oldHeap)),
                RelationGetRelationName(oldHeap), RelationGetRelationName(oldIndex))));
    else if (tuplesort != NULL)
        ereport(elevel, (errcode(ERRCODE_LOG), errmsg("clustering \"%s.%s\" using sequential scan and sort",
            get_namespace_name(RelationGetNamespace(oldHeap)), RelationGetRelationName(oldHeap))));
    else
        ereport(elevel, (errcode(ERRCODE_LOG), errmsg("vacuuming \"%s.%s\"",
            get_namespace_name(RelationGetNamespace(oldHeap)), RelationGetRelationName(oldHeap))));

    if (verbose)
        messageLevel = VERBOSEMESSAGE;
    else
        messageLevel = WARNING;

    /*
     * Scan through the oldHeap, either in oldIndex order or sequentially;
     * copy each tuple into the newHeap, or transiently to the tuplesort
     * module.	Note that we don't bother sorting dead tuples (they won't get
     * to the new table anyway).
     */
    for (;;) {
        UHeapTuple utuple;
        Buffer buf;
        bool isdead = false;
        Page page;

        CHECK_FOR_INTERRUPTS();

        /* IO collector and IO scheduler for vacuum full -- for read */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);

        if (indexScan != NULL) {
            if (IndexGetnextSlot(indexScan, ForwardScanDirection, slot))
                utuple = ExecGetUHeapTupleFromSlot(slot);
            else
                break;

            if (utuple == NULL)
                break;

            if (RelationGetRelid(oldHeap) != utuple->table_oid) {
                continue;
            }

            /* Since we used no scan keys, should never need to recheck */
            if (indexScan->xs_recheck)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("CLUSTER does not support lossy index conditions")));

            buf = indexScan->xs_cbuf;
        } else {
            if ((UHeapGetNextSlotGuts((TableScanDesc)heapScan, ForwardScanDirection, slot)) != NULL)
                utuple = ExecGetUHeapTupleFromSlot(slot);
            else
                break;

            buf = heapScan->rs_base.rs_cbuf;
            ADIO_RUN()
            {
            }
            ADIO_END();
        }

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        if (u_sess->attr.attr_storage.enable_debug_vacuum)
            t_thrd.utils_cxt.pRelatedRel = oldHeap;

        TransactionId xwait;
        SubTransactionId subxidXwait = InvalidSubTransactionId;

        switch (UHeapTupleSatisfiesOldestXmin(utuple, oldestXmin, buf, true, &utuple, &xwait, &subxidXwait)) {
            case UHEAPTUPLE_DEAD:
                /* Definitely dead */
                isdead = true;
                break;
            case UHEAPTUPLE_RECENTLY_DEAD:
                /* Treat as dead since we are not transfering undologs in this version */
                isdead = true;
                break;
            case UHEAPTUPLE_LIVE:
                /* Live or recently dead, must copy it */
                isdead = false;
                break;
            case UHEAPTUPLE_INSERT_IN_PROGRESS:

                /*
                 * Since we hold exclusive lock on the relation, normally the
                 * only way to see this is if it was inserted earlier in our
                 * own transaction.
                 */
                if (!TransactionIdIsCurrentTransactionId(xwait)) {
                    ereport(messageLevel, (errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("concurrent insert in progress within table \"%s\"", RelationGetRelationName(oldHeap))));
                }
                /* treat as live */
                isdead = false;
                break;
            case UHEAPTUPLE_DELETE_IN_PROGRESS:

                /*
                 * Similar situation to INSERT_IN_PROGRESS case.
                 */
                if (!TransactionIdIsCurrentTransactionId(xwait)) {
                    ereport(messageLevel, (errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("concurrent delete in progress within table \"%s\"", RelationGetRelationName(oldHeap))));
                }
                /* treat as recently dead */
                isdead = true;
                break;
            default:
                isdead = false; /* keep compiler quiet */
                ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                break;
        }

        if (u_sess->attr.attr_storage.enable_debug_vacuum)
            t_thrd.utils_cxt.pRelatedRel = NULL;

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        /* IO collector and IO scheduler for vacuum full -- for write */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);

        if (isdead) {
            tups_vacuumed += 1;
            continue;
        }

        num_tuples += 1;
        if (tuplesort != NULL)
            TuplesortPutheaptuple(tuplesort, (HeapTuple)utuple);
        else
            ReformAndRewriteUTuple(utuple, oldTupDesc, newTupDesc, values, isnull, newHeap->rd_rel->relhasoids,
                rwstate);
    }

    if (indexScan != NULL)
        index_endscan(indexScan);
    if (heapScan != NULL)
        UHeapEndScan((TableScanDesc)heapScan);

    if (RelationIsValid(heapRelation)) {
        Assert(RelationIsGlobalIndex(oldIndex));
        heap_close(heapRelation, NoLock);
    }

    /*
     * In scan-and-sort mode, complete the sort, then read out all live tuples
     * from the tuplestore and write them to the new relation.
     */
    if (tuplesort != NULL) {
        tuplesort_performsort(tuplesort);

        for (;;) {
            UHeapTuple utuple;
            bool shouldfree = false;

            CHECK_FOR_INTERRUPTS();

            utuple = (UHeapTuple)tuplesort_getheaptuple(tuplesort, true, &shouldfree);
            if (utuple == NULL)
                break;

            Assert(utuple->tupTableType == UHEAP_TUPLE);

            ReformAndRewriteUTuple(utuple, oldTupDesc, newTupDesc, values, isnull, newHeap->rd_rel->relhasoids,
                rwstate);

            if (shouldfree)
                UHeapFreeTuple(utuple);
        }

        tuplesort_end(tuplesort);
    }

    /* Write out any remaining tuples, and fsync if needed */
    end_heap_rewrite(rwstate);

    /* Log what we did */
    ereport(elevel, (errcode(ERRCODE_LOG),
        errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
            RelationGetRelationName(oldHeap), tups_vacuumed, num_tuples, RelationGetNumberOfBlocks(oldHeap)),
            errdetail("%.0f dead row versions cannot be removed yet.\n""%s.",
                tups_recently_dead, pg_rusage_show(&ru0))));
    /* Clean up */
    pfree_ext(values);
    pfree_ext(isnull);
    ExecDropSingleTupleTableSlot(slot);

    return tups_vacuumed;
}

static inline bool tuple_invisible_not_hotupdate(HeapTuple tuple, Relation relation)
{
    if (HeapKeepInvisibleTuple(tuple, RelationGetDescr(relation)) && !HeapTupleIsHotUpdated(tuple)) {
        return false;
    } else {
        return true;
    }
}

double copy_heap_data_internal(Relation OldHeap, Relation OldIndex, Relation NewHeap, TransactionId OldestXmin,
    TransactionId FreezeXid, bool verbose, bool use_sort, AdaptMem* memUsage)
{

    TupleDesc oldTupDesc;
    TupleDesc newTupDesc;
    Relation heapRelation = NULL;
    int natts;
    Datum* values = NULL;
    bool* isnull = NULL;
    IndexScanDesc indexScan = NULL;
    TableScanDesc heapScan = NULL;
    bool useWal = XLogIsNeeded() && RelationNeedsWAL(NewHeap);
    bool is_system_catalog = IsSystemRelation(OldHeap);
    RewriteState rwstate;
    Tuplesortstate* tuplesort = NULL;
    double num_tuples = 0;
    double tups_vacuumed = 0;
    double tups_recently_dead = 0;
    int elevel = verbose ? VERBOSEMESSAGE : DEBUG2;
    int messageLevel = -1;
    PGRUsage ru0;
    SeqScanAccessor scanaccessor;

    pg_rusage_init(&ru0);

    /* use_wal off requires smgr_targblock be initially invalid */
    Assert(RelationGetTargetBlock(NewHeap) == InvalidBlockNumber);

    /*
     * Their tuple descriptors should be exactly alike, but here we only need
     * assume that they have the same number of columns.
     */
    oldTupDesc = RelationGetDescr(OldHeap);
    newTupDesc = RelationGetDescr(NewHeap);
    Assert(newTupDesc->natts == oldTupDesc->natts);

    /* Preallocate values/isnull arrays */
    natts = newTupDesc->natts;
    values = (Datum*)palloc(natts * sizeof(Datum));
    isnull = (bool*)palloc(natts * sizeof(bool));

    /* Initialize the rewrite operation */
    rwstate = begin_heap_rewrite(OldHeap, NewHeap, OldestXmin, FreezeXid, useWal);

    /* Set up sorting if wanted */
    if (use_sort) {
        int workMem = (memUsage->work_mem > 0) ? memUsage->work_mem : u_sess->attr.attr_memory.maintenance_work_mem;
        int maxMem = memUsage->max_mem;
        tuplesort = tuplesort_begin_cluster(oldTupDesc, OldIndex, workMem, false, maxMem, false);
    } else {
        tuplesort = NULL;
    }

    /*
     * Prepare to scan the OldHeap.  To ensure we see recently-dead tuples
     * that still need to be copied, we scan with SnapshotAny and use
     * HeapTupleSatisfiesVacuum for the visibility test.
     * If index is global index, we will use indexScan to copy tuples.
     */
    if (OldIndex != NULL && !use_sort) {
        heapRelation = OldHeap;
        if (RelationIsCrossBucketIndex(OldIndex)) {
            heapRelation = OldHeap->parent;
        }
        if (RelationIsGlobalIndex(OldIndex)) {
            Oid heapId = IndexGetRelation(RelationGetRelid(OldIndex), false);
            heapRelation = heap_open(heapId, NoLock);
        }
        indexScan = scan_handler_idx_beginscan(heapRelation, OldIndex, SnapshotAny, 0, 0);
        scan_handler_idx_rescan_local(indexScan, NULL, 0, NULL, 0);
    } else {
        heapScan = tableam_scan_begin(OldHeap, SnapshotAny, 0, (ScanKey)NULL);
        ADIO_RUN()
        {
            SeqScan_Init(heapScan, &scanaccessor, OldHeap);
        }
        ADIO_END();
    }

    /* Log what we're doing */
    ClusterRunMsg(OldHeap, OldIndex, indexScan, tuplesort, verbose);

    if (verbose)
        messageLevel = VERBOSEMESSAGE;
    else
        messageLevel = WARNING;

    if (OldHeap->rd_rel->relkind == RELKIND_MATVIEW) {
    /* Make sure the heap looks good even if no rows are written. */
        SetRelationIsScannable(NewHeap);
    }

    /*
     * Scan through the OldHeap, either in OldIndex order or sequentially;
     * copy each tuple into the NewHeap, or transiently to the tuplesort
     * module.	Note that we don't bother sorting dead tuples (they won't get
     * to the new table anyway).
     */
    for (;;) {
        HeapTuple tuple;
        Buffer buf;
        bool isdead = false;
        Page page;

        CHECK_FOR_INTERRUPTS();

        /* IO collector and IO scheduler for vacuum full -- for read */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);

        if (indexScan != NULL) {
            tuple = (HeapTuple)scan_handler_idx_getnext(indexScan, ForwardScanDirection);
            if (tuple == NULL)
                break;
            if (RelationGetRelid(OldHeap) != tuple->t_tableOid || RelationGetBktid(OldHeap) != tuple->t_bucketId) 
                continue;
            Assert(TUPLE_IS_HEAP_TUPLE(tuple));
            /* Since we used no scan keys, should never need to recheck */
            bool xs_recheck = RelationIsCrossBucketIndex(OldIndex) ? 
                ((HBktIdxScanDesc)indexScan)->currBktIdxScan->xs_recheck : indexScan->xs_recheck;
            if (xs_recheck)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("CLUSTER does not support lossy index conditions")));

            buf = RelationIsCrossBucketIndex(OldIndex) ? 
                ((HBktIdxScanDesc)indexScan)->currBktIdxScan->xs_cbuf : indexScan->xs_cbuf;
        } else {
            tuple =  (HeapTuple) tableam_scan_getnexttuple(heapScan, ForwardScanDirection);
            if (tuple == NULL)
                break;

            Assert(TUPLE_IS_HEAP_TUPLE(tuple));

            buf = heapScan->rs_cbuf;
            ADIO_RUN()
            {
                Start_Prefetch(heapScan, &scanaccessor, ForwardScanDirection);
            }
            ADIO_END();
        }

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        if (u_sess->attr.attr_storage.enable_debug_vacuum)
            t_thrd.utils_cxt.pRelatedRel = OldHeap;

        switch (HeapTupleSatisfiesVacuum(tuple, OldestXmin, buf)) {
            case HEAPTUPLE_DEAD:
                /* Definitely dead */
                isdead = tuple_invisible_not_hotupdate(tuple, OldHeap);
                break;
            case HEAPTUPLE_RECENTLY_DEAD:
                tups_recently_dead += 1;
                /* fall through */
            case HEAPTUPLE_LIVE:
                /* Live or recently dead, must copy it */
                isdead = false;
                break;
            case HEAPTUPLE_INSERT_IN_PROGRESS:

                /*
                 * Since we hold exclusive lock on the relation, normally the
                 * only way to see this is if it was inserted earlier in our
                 * own transaction.  However, it can happen in system
                 * catalogs, since we tend to release write lock before commit
                 * there.  Give a warning if neither case applies; but in any
                 * case we had better copy it.
                 */
                if (!is_system_catalog &&
                    !TransactionIdIsCurrentTransactionId(HeapTupleGetUpdateXid(tuple)))
                    ereport(messageLevel, (errcode(ERRCODE_OBJECT_IN_USE),
                            errmsg("concurrent insert in progress within table \"%s\"",
                                RelationGetRelationName(OldHeap))));
                /* treat as live */
                isdead = false;
                break;
            case HEAPTUPLE_DELETE_IN_PROGRESS:

                /*
                 * Similar situation to INSERT_IN_PROGRESS case.
                 */
                Assert(!(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
                if (!is_system_catalog &&
                    !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple->t_data)))
                    ereport(messageLevel, (errcode(ERRCODE_OBJECT_IN_USE),
                            errmsg("concurrent delete in progress within table \"%s\"",
                                RelationGetRelationName(OldHeap))));
                /* treat as recently dead */
                tups_recently_dead += 1;
                isdead = false;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                        errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                isdead = false; /* keep compiler quiet */
                break;
        }

        if (u_sess->attr.attr_storage.enable_debug_vacuum)
            t_thrd.utils_cxt.pRelatedRel = NULL;

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        /* IO collector and IO scheduler for vacuum full -- for write */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);

        if (isdead) {
            if (u_sess->attr.attr_storage.enable_debug_vacuum)
                elogVacuumInfo(OldHeap, tuple, "copy heap data", OldestXmin);
            tups_vacuumed += 1;
            /* heap rewrite module still needs to see it... */
            /*
             * If we are vacuuming system_catalog, another transaction may abort after we scan system_catalog A tuple,
             * which is actually still alive. In this situation, system catalog  A is HEAPTUPLE_DELETE_IN_PROGRESS
             * and B is dead, but A's xmax finally abort, so we cannot delete it.
             */
            if (!is_system_catalog && rewrite_heap_dead_tuple(rwstate, tuple)) {
                /* A previous recently-dead tuple is now known dead */
                tups_vacuumed += 1;
                tups_recently_dead -= 1;
            }
            continue;
        }

        num_tuples += 1;
        if (tuplesort != NULL)
            TuplesortPutheaptuple(tuplesort, tuple);
        else
            reform_and_rewrite_tuple(
                tuple, oldTupDesc, newTupDesc, values, isnull, NewHeap->rd_rel->relhasoids, rwstate);
    }

    if (indexScan != NULL) {
        scan_handler_idx_endscan(indexScan);
        if (RelationIsGlobalIndex(OldIndex)) {
            heap_close(heapRelation, NoLock);
        }
    }

    if (heapScan != NULL)
        tableam_scan_end(heapScan);

    /*
     * In scan-and-sort mode, complete the sort, then read out all live tuples
     * from the tuplestore and write them to the new relation.
     */
    if (tuplesort != NULL) {
        tuplesort_performsort(tuplesort);

        for (;;) {
            HeapTuple tuple;
            bool shouldfree = false;

            CHECK_FOR_INTERRUPTS();

            tuple = (HeapTuple)tuplesort_getheaptuple(tuplesort, true, &shouldfree);
            if (tuple == NULL)
                break;

            reform_and_rewrite_tuple(
                tuple, oldTupDesc, newTupDesc, values, isnull, NewHeap->rd_rel->relhasoids, rwstate);

            if (shouldfree)
                heap_freetuple(tuple);
        }

        tuplesort_end(tuplesort);
    }

    /* Write out any remaining tuples, and fsync if needed */
    end_heap_rewrite(rwstate);

    /* Log what we did */
    ereport(elevel,
        (errcode(ERRCODE_LOG),
            errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
                RelationGetRelationName(OldHeap),
                tups_vacuumed, num_tuples,
                RelationGetNumberOfBlocks(OldHeap)),
            errdetail("%.0f dead row versions cannot be removed yet.\n"
                      "%s.",
                tups_recently_dead,
                pg_rusage_show(&ru0))));
    /* Clean up */
    pfree_ext(values);
    pfree_ext(isnull);

    return tups_vacuumed;
}

/*
 * Do the physical copying of heap data.
 *
 * There are two output parameters:
 * *pSwapToastByContent is set true if toast tables must be swapped by content.
 * *pFreezeXid receives the TransactionId used as freeze cutoff point.
 */
static void copy_heap_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex, int freeze_min_age, int freeze_table_age,
    bool verbose, bool* pSwapToastByContent, TransactionId* pFreezeXid, MultiXactId *pFreezeMulti,
    double* ptrDeleteTupleNum, AdaptMem* memUsage)
{
    Relation NewHeap, OldHeap, OldIndex;
    TransactionId OldestXmin;
    TransactionId FreezeXid;
    MultiXactId	MultiXactFrzLimit;
    bool use_sort = false;
    double tups_vacuumed = 0;
    bool isGtt = false;
    TransactionId gttRelfrozenxid = 0;

    /*
     * Open the relations we need.
     */
    NewHeap = heap_open(OIDNewHeap, ExclusiveLock);
    OldHeap = heap_open(OIDOldHeap, ExclusiveLock);

    if (OidIsValid(OIDOldIndex))
        OldIndex = index_open(OIDOldIndex, ExclusiveLock);
    else
        OldIndex = NULL;

    if (RELATION_IS_GLOBAL_TEMP(OldHeap)) {
        isGtt = true;
    }

    /*
     * If the OldHeap has a toast table, get lock on the toast table to keep
     * it from being vacuumed.	This is needed because autovacuum processes
     * toast tables independently of their main tables, with no lock on the
     * latter.	If an autovacuum were to start on the toast table after we
     * compute our OldestXmin below, it would use a later OldestXmin, and then
     * possibly remove as DEAD toast tuples belonging to main tuples we think
     * are only RECENTLY_DEAD.	Then we'd fail while trying to copy those
     * tuples.
     *
     * We don't need to open the toast relation here, just lock it.  The lock
     * will be held till end of transaction.
     */
    if (OldHeap->rd_rel->reltoastrelid)
        LockRelationOid(OldHeap->rd_rel->reltoastrelid, ExclusiveLock);

    /*
     * If both tables have TOAST tables, perform toast swap by content.  It is
     * possible that the old table has a toast table but the new one doesn't,
     * if toastable columns have been dropped.	In that case we have to do
     * swap by links.  This is okay because swap by content is only essential
     * for system catalogs, and we don't support schema changes for them.
     */
    if (OldHeap->rd_rel->reltoastrelid && NewHeap->rd_rel->reltoastrelid) {
        *pSwapToastByContent = true;

        /* When doing swap by content, any toast pointers written into NewHeap
         * must use the old toast table's OID, because that's where the toast
         * data will eventually be found.  Set this up by setting rd_toastoid.
         * This also tells toast_save_datum() to preserve the toast value
         * OIDs, which we want so as not to invalidate toast pointers in
         * system catalog caches, and to avoid making multiple copies of a
         * single toast value.
         *
         * Note that we must hold NewHeap open until we are done writing data,
         * since the relcache will not guarantee to remember this setting once
         * the relation is closed.	Also, this technique depends on the fact
         * that no one will try to read from the NewHeap until after we've
         * finished writing it and swapping the rels --- otherwise they could
         * follow the toast pointers to the wrong place.  (It would actually
         * work for values copied over from the old toast table, but not for
         * any values that we toast which were previously not toasted.)
         */

        NewHeap->rd_toastoid = OldHeap->rd_rel->reltoastrelid;
    } else
        *pSwapToastByContent = false;

    if (!RelationIsUstoreFormat(OldHeap)) {    
        /*
         * compute xids used to freeze and weed out dead tuples.  We use -1
         * freeze_min_age to avoid having CLUSTER freeze tuples earlier than a
         * plain VACUUM would.
         */

        vacuum_set_xid_limits(OldHeap, 0, freeze_table_age, &OldestXmin, &FreezeXid, NULL, &MultiXactFrzLimit);

        /*
         * FreezeXid will become the table's new relfrozenxid, and that mustn't go
         * backwards, so take the max.
         */
        if (isGtt) {
            (void)get_gtt_relstats(OIDOldHeap, NULL, NULL, NULL, &gttRelfrozenxid);
            if (TransactionIdIsValid(gttRelfrozenxid) && TransactionIdPrecedes(FreezeXid, gttRelfrozenxid))
                FreezeXid = gttRelfrozenxid;
        } else {
            bool isNull = false;
            TransactionId relfrozenxid;
#ifndef ENABLE_MULTIPLE_NODES
            MultiXactId relminmxid;
#endif
            Relation rel = heap_open(RelationRelationId, AccessShareLock);
            HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDOldHeap));
            if (!HeapTupleIsValid(tuple)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("cache lookup failed for relation %u", RelationGetRelid(OldHeap))));
            }
            Datum xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);

            if (isNull) {
                relfrozenxid = OldHeap->rd_rel->relfrozenxid;
                if (OldHeap->rd_tam_type == TAM_USTORE) {
                    elog(ERROR, "cluster not supported in ustore");
                }

                if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                    !TransactionIdIsNormal(relfrozenxid)) {
                    relfrozenxid = FirstNormalTransactionId;
                    }
            } else {
                relfrozenxid = DatumGetTransactionId(xid64datum);
            }

            if (TransactionIdPrecedes(FreezeXid, relfrozenxid)) {
                FreezeXid = relfrozenxid;
            }
#ifndef ENABLE_MULTIPLE_NODES
            Datum minmxidDatum = tableam_tops_tuple_getattr(tuple, Anum_pg_class_relminmxid, RelationGetDescr(rel), &isNull);
            relminmxid = isNull ? InvalidMultiXactId : DatumGetTransactionId(minmxidDatum);

            if (MultiXactIdPrecedes(MultiXactFrzLimit, relminmxid)) {
                MultiXactFrzLimit = relminmxid;
            }
#endif
            heap_close(rel, AccessShareLock);
            heap_freetuple(tuple);
        }
    } else {
        /* We will eventually freeze all tuples of ustore tables here.
         * Hence freeze xid should be CurrentTransactionId
         */
        FreezeXid = GetCurrentTransactionId();
    }

    /* return selected value to caller */
    *pFreezeXid = FreezeXid;
    *pFreezeMulti = MultiXactFrzLimit;

    /*
     * Decide whether to use an indexscan or seqscan-and-optional-sort to scan
     * the OldHeap.  We know how to use a sort to duplicate the ordering of a
     * btree index, and will use seqscan-and-sort for that case if the planner
     * tells us it's cheaper.  Otherwise, always indexscan if an index is
     * provided, else plain seqscan.
     */
    if (OldIndex != NULL && OID_IS_BTREE(OldIndex->rd_rel->relam)) {
        use_sort = plan_cluster_use_sort(OIDOldHeap, OIDOldIndex);
    } else {
        use_sort = false;
    }

    if (RELATION_CREATE_BUCKET(OldHeap)) {
        oidvector* bucketlist = searchHashBucketByOid(OldHeap->rd_bucketoid);

        for (int i = 0; i < bucketlist->dim1; i++) {
            Relation OldBucketHeap = bucketGetRelation(OldHeap, NULL, bucketlist->values[i]);
            Relation NewBucketHeap = bucketGetRelation(NewHeap, NULL, bucketlist->values[i]);
            Relation OldBucketIndex = NULL;
            if (OldIndex != NULL) {
                OldBucketIndex = RelationIsCrossBucketIndex(OldIndex) ? OldIndex :
                    bucketGetRelation(OldIndex, NULL, bucketlist->values[i]);
            }
            tups_vacuumed += tableam_relation_copy_for_cluster(
                OldBucketHeap, OldBucketIndex, NewBucketHeap, OldestXmin, FreezeXid, verbose, use_sort, memUsage);
            bucketCloseRelation(OldBucketHeap);
            bucketCloseRelation(NewBucketHeap);
            if (OldBucketIndex != NULL && !RelationIsCrossBucketIndex(OldIndex))
                bucketCloseRelation(OldBucketIndex);
        }

        /* If the rel is WAL-logged, must fsync before commit.    We use heap_sync
        * to ensure that the toast table gets fsync'd too.
        *
        * It's obvious that we must do this when not WAL-logging. It's less
        * obvious that we have to do it even if we did WAL-log the pages. The
        * reason is the same as in tablecmds.c's copy_relation_data(): we're
        * writing data that's not in shared buffers, and so a CHECKPOINT
        * occurring during the rewriteheap operation won't have fsync'd data we
        * wrote before the checkpoint.
        */
        if (RelationNeedsWAL(NewHeap)) {
            heap_sync(NewHeap);
        }
    } else {
        tups_vacuumed =
            tableam_relation_copy_for_cluster(OldHeap, OldIndex, NewHeap, OldestXmin, FreezeXid, verbose, use_sort, memUsage);
    }
    /* Reset rd_toastoid just to be tidy --- it shouldn't be looked at again */
    NewHeap->rd_toastoid = InvalidOid;

    /* record vacuumed tuple for reporting stat to PgStatCollector */
    *ptrDeleteTupleNum = tups_vacuumed;

    if (OldIndex != NULL)
        index_close(OldIndex, NoLock);
    heap_close(OldHeap, NoLock);
    heap_close(NewHeap, NoLock);
}

static Relation GetPartitionIndexRel(
    const Relation oldHeap, Oid indexOid, Relation* partTabIndexRel, Partition* partIndexRel)
{
    Relation oldIndex = NULL;

    if (OidIsValid(indexOid)) {
        Oid partIndexOid = InvalidOid;
        *partTabIndexRel = index_open(indexOid, NoLock);
        if (RelationIsGlobalIndex(*partTabIndexRel)) {
            oldIndex = *partTabIndexRel;
        } else {
            partIndexOid = getPartitionIndexOid(indexOid, RelationGetRelid(oldHeap));
            *partIndexRel = partitionOpen(*partTabIndexRel, partIndexOid, ExclusiveLock);
            if (!(*partIndexRel)->pd_part->indisusable) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("can not cluster partition %s using %s bacause of unusable local index",
                            getPartitionName(oldHeap->rd_id, false),
                            get_rel_name(indexOid))));
            }
            oldIndex = partitionGetRelation(*partTabIndexRel, *partIndexRel);
        }
    } else {
        oldIndex = NULL;
    }

    return oldIndex;
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        : Do the physical copying of partition heap data.
 * Description  :
 * Notes        : There are two output parameters:
 * 				  pSwapToastByContent is set true if toast tables must be swapped by content.
 * 				  pFreezeXid receives the TransactionId used as freeze cutoff point.
 * Input        :
 * Output       : NA
 */
static void copyPartitionHeapData(Relation newHeap, Relation oldHeap, Oid indexOid, PlannerInfo* root,
    RelOptInfo* relOptInfo, int freezeMinAge, int freezeTableAge, bool verbose, bool* pSwapToastByContent,
    TransactionId* pFreezeXid, MultiXactId* pFreezeMulti, AdaptMem* memUsage, double* ptrDeleteTupleNum)
{
    Relation oldIndex = NULL;
    TransactionId oldestXmin = 0;
    TransactionId freezeXid = 0;
    MultiXactId freezeMulti = 0;
    bool useSort = false;
    Relation partTabIndexRel = NULL;
    Partition partIndexRel = NULL;
    TransactionId relfrozenxid = InvalidTransactionId;
    MultiXactId relfrozenmxid = InvalidMultiXactId;
    double tups_vacuumed = 0;
    *pSwapToastByContent = false;

    oldIndex = GetPartitionIndexRel(oldHeap, indexOid, &partTabIndexRel, &partIndexRel);

    /*
     * If the OldHeap has a toast table, get lock on the toast table to keep
     * it from being vacuumed.
     */
    if (oldHeap->rd_rel->reltoastrelid) {
        LockRelationOid(oldHeap->rd_rel->reltoastrelid, ExclusiveLock);

        /*
         * If both tables have TOAST tables, perform toast swap by content.
         */
        if (newHeap->rd_rel->reltoastrelid) {
            *pSwapToastByContent = true;
            newHeap->rd_toastoid = oldHeap->rd_rel->reltoastrelid;
        }
    }

    if (!RelationIsUstoreFormat(oldHeap)) {
        /*
         * compute xids used to freeze and weed out dead tuples.
         */
        vacuum_set_xid_limits(oldHeap, 0, freezeTableAge, &oldestXmin, &freezeXid, NULL, &freezeMulti);

        /*
         * FreezeXid will become the table's new relfrozenxid, and that mustn't go
         * backwards, so take the max.
         */
        getPartitionRelxids(oldHeap, &relfrozenxid, &relfrozenmxid);

        if (TransactionIdPrecedes(freezeXid, relfrozenxid))
            freezeXid = relfrozenxid;

        if (MultiXactIdPrecedes(freezeMulti, relfrozenmxid))
            freezeMulti = relfrozenmxid;
    } else {
        /* We will eventually freeze all tuples of ustore tables here.
         * Hence freeze xid should be CurrentTransactionId
         */
        freezeXid = GetCurrentTransactionId();
        freezeMulti = GetOldestMultiXactId();
    }

    /* return selected value to caller */
    *pFreezeXid = freezeXid;
    *pFreezeMulti = freezeMulti;

    /*
     * Decide whether to use an indexscan or seqscan-and-optional-sort to scan
     * the OldHeap.
     */
    if (oldIndex != NULL && OID_IS_BTREE(oldIndex->rd_rel->relam)) {
        useSort = planClusterPartitionUseSort(oldHeap, indexOid, root, relOptInfo);
    }

    if (RELATION_CREATE_BUCKET(oldHeap)) {
        oidvector* bucketlist = searchHashBucketByOid(oldHeap->rd_bucketoid);

        for (int i = 0; i < bucketlist->dim1; i++) {
            Relation OldBucketHeap = bucketGetRelation(oldHeap, NULL, bucketlist->values[i]);
            Relation NewBucketHeap = bucketGetRelation(newHeap, NULL, bucketlist->values[i]);
            Relation OldBucketIndex = NULL;
            if (oldIndex != NULL)
                OldBucketIndex = RelationIsCrossBucketIndex(oldIndex) ? 
                    oldIndex : bucketGetRelation(oldIndex, NULL, bucketlist->values[i]);

            /* */
            tups_vacuumed += tableam_relation_copy_for_cluster(
                OldBucketHeap, OldBucketIndex, NewBucketHeap, oldestXmin, freezeXid, verbose, useSort, memUsage);
            bucketCloseRelation(OldBucketHeap);
            bucketCloseRelation(NewBucketHeap);
            if (OldBucketIndex != NULL && !RelationIsCrossBucketIndex(oldIndex))
                bucketCloseRelation(OldBucketIndex);
        }        
        if (RelationNeedsWAL(newHeap)) {
            heap_sync(newHeap);
        }
    } else {
        tups_vacuumed =
            tableam_relation_copy_for_cluster(oldHeap, oldIndex, newHeap, oldestXmin, freezeXid, verbose, useSort, memUsage);
    }

    /* Reset rd_toastoid just to be tidy --- it shouldn't be looked at again */
    newHeap->rd_toastoid = InvalidOid;

    /* record vacuumed tuple for reporting stat to PgStatCollector */
    if (ptrDeleteTupleNum != NULL)
        *ptrDeleteTupleNum = tups_vacuumed;
    if (RelationIsValid(partTabIndexRel) && RelationIsGlobalIndex(partTabIndexRel)) {
        index_close(partTabIndexRel, NoLock);
        return;
    }

    if (oldIndex != NULL) {
        releaseDummyRelation(&oldIndex);
        partitionClose(partTabIndexRel, partIndexRel, NoLock);
        index_close(partTabIndexRel, NoLock);
    }
}

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace and relfilenode) while
 * keeping the same logical identities of the two relations.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries.  The former is needed
 * to manage rewrites of shared catalogs (where we cannot change the pg_class
 * links) while the latter is the only way to handle cases in which a toast
 * table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 * Lastly, if r2 and its toast table and toast index (if any) are mapped,
 * their OIDs are emitted into mapped_tables[].  This is hacky but beats
 * having to look the information up again later in finish_heap_swap.
 */
static void swap_relation_files(
    Oid r1, Oid r2, bool target_is_pg_class, bool swap_toast_by_content, TransactionId frozenXid,
    MultiXactId frozenMulti, Oid* mapped_tables)
{
    Relation relRelation;
    HeapTuple reltup1, reltup2;
    HeapTuple nctup;
    Form_pg_class relform1, relform2;
    Oid relfilenode1, relfilenode2;
    Oid swaptemp;
    CatalogIndexState indstate;

    /* We need writable copies of both pg_class tuples. */
    relRelation = heap_open(RelationRelationId, RowExclusiveLock);

    reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
    if (!HeapTupleIsValid(reltup1))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", r1)));
    relform1 = (Form_pg_class)GETSTRUCT(reltup1);

    reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
    if (!HeapTupleIsValid(reltup2))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", r2)));
    relform2 = (Form_pg_class)GETSTRUCT(reltup2);

    relfilenode1 = relform1->relfilenode;
    relfilenode2 = relform2->relfilenode;

    if (OidIsValid(relfilenode1) && OidIsValid(relfilenode2)) {
        /* Normal non-mapped relations: swap relfilenodes and reltablespaces */
        Assert(!target_is_pg_class);

        ereport(LOG,
            (errmsg("Relation %s(%u) [%u/%u/%u] Swap files with Relation %u [%u/%u/%u] xid %lu",
                NameStr(relform1->relname),
                r1,
                relform1->reltablespace,
                u_sess->proc_cxt.MyDatabaseId,
                relform1->relfilenode,
                r2,
                relform2->reltablespace,
                u_sess->proc_cxt.MyDatabaseId,
                relform2->relfilenode,
                GetCurrentTransactionIdIfAny())));

        swaptemp = relform1->relfilenode;
        relform1->relfilenode = relform2->relfilenode;
        relform2->relfilenode = swaptemp;

        swaptemp = relform1->reltablespace;
        relform1->reltablespace = relform2->reltablespace;
        relform2->reltablespace = swaptemp;

        // Any way, we should swapping cudesc,delta by links
        //
        swaptemp = relform1->relcudescrelid;
        relform1->relcudescrelid = relform2->relcudescrelid;
        relform2->relcudescrelid = swaptemp;

        swaptemp = relform1->reldeltarelid;
        relform1->reldeltarelid = relform2->reldeltarelid;
        relform2->reldeltarelid = swaptemp;

        /* Also swap toast links, if we're swapping by links */
        if (!swap_toast_by_content) {
            swaptemp = relform1->reltoastrelid;
            relform1->reltoastrelid = relform2->reltoastrelid;
            relform2->reltoastrelid = swaptemp;
        }
    } else {
        /*
         * Mapped-relation case.  Here we have to swap the relation mappings
         * instead of modifying the pg_class columns.  Both must be mapped.
         */
        if (OidIsValid(relfilenode1) || OidIsValid(relfilenode2))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("cannot swap mapped relation \"%s\" with non-mapped relation", NameStr(relform1->relname))));

        /*
         * We can't change the tablespace of a mapped rel, and we can't handle
         * toast link swapping for one either, because we must not apply any
         * critical changes to its pg_class row.  These cases should be
         * prevented by upstream permissions tests, so this check is a
         * non-user-facing emergency backstop.
         */
        if (relform1->reltablespace != relform2->reltablespace)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("cannot change tablespace of mapped relation \"%s\"", NameStr(relform1->relname))));
        if (!swap_toast_by_content && (relform1->reltoastrelid || relform2->reltoastrelid))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("cannot swap toast by links for mapped relation \"%s\"", NameStr(relform1->relname))));

        /*
         * Fetch the mappings --- shouldn't fail, but be paranoid
         */
        relfilenode1 = RelationMapOidToFilenode(r1, relform1->relisshared);
        if (!OidIsValid(relfilenode1))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("could not find relation mapping for relation \"%s\", OID %u",
                        NameStr(relform1->relname),
                        r1)));
        relfilenode2 = RelationMapOidToFilenode(r2, relform2->relisshared);
        if (!OidIsValid(relfilenode2))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("could not find relation mapping for relation \"%s\", OID %u",
                        NameStr(relform2->relname),
                        r2)));

        /*
         * Send replacement mappings to relmapper.	Note these won't actually
         * take effect until CommandCounterIncrement.
         */
        RelationMapUpdateMap(r1, relfilenode2, relform1->relisshared, false);
        RelationMapUpdateMap(r2, relfilenode1, relform2->relisshared, false);

        /* Pass OIDs of mapped r2 tables back to caller */
        *mapped_tables++ = r2;
    }

    /*
     * In the case of a shared catalog, these next few steps will only affect
     * our own database's pg_class row; but that's okay, because they are all
     * noncritical updates.  That's also an important fact for the case of a
     * mapped catalog, because it's possible that we'll commit the map change
     * and then fail to commit the pg_class update.
     *
     * set rel1's frozen Xid and minimum MultiXid
     */
    nctup = NULL;
    if (relform1->relkind != RELKIND_INDEX && relform1->relkind != RELKIND_GLOBAL_INDEX) {
        Datum values[Natts_pg_class];
        bool nulls[Natts_pg_class];
        bool replaces[Natts_pg_class];
        errno_t rc;
        HeapTuple tmp;

        relform1->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_class_relfrozenxid64 - 1] = true;
        values[Anum_pg_class_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenXid);

#ifndef ENABLE_MULTIPLE_NODES
        replaces[Anum_pg_class_relminmxid - 1] = true;
        values[Anum_pg_class_relminmxid - 1] = TransactionIdGetDatum(frozenMulti);
#endif

        nctup = (HeapTuple) tableam_tops_modify_tuple(reltup1, RelationGetDescr(relRelation), values, nulls, replaces);

        relform1 = (Form_pg_class)GETSTRUCT(nctup);

        tmp = nctup;
        nctup = reltup1;
        reltup1 = tmp;
    }

    /* swap size statistics too, since new rel has freshly-updated stats */
    if (!IS_PGXC_COORDINATOR) {
        float8 swap_pages;
        float4 swap_tuples;
        int4 swap_allvisible;

        swap_pages = relform1->relpages;
        relform1->relpages = relform2->relpages;
        relform2->relpages = swap_pages;

        swap_tuples = relform1->reltuples;
        relform1->reltuples = relform2->reltuples;
        relform2->reltuples = swap_tuples;

        swap_allvisible = relform1->relallvisible;
        relform1->relallvisible = relform2->relallvisible;
        relform2->relallvisible = swap_allvisible;
    }

    /*
     * Update the tuples in pg_class --- unless the target relation of the
     * swap is pg_class itself.  In that case, there is zero point in making
     * changes because we'd be updating the old data that we're about to throw
     * away.  Because the real work being done here for a mapped relation is
     * just to change the relation map settings, it's all right to not update
     * the pg_class rows in this case.
     */
    if (!target_is_pg_class) {
        simple_heap_update(relRelation, &reltup1->t_self, reltup1);
        simple_heap_update(relRelation, &reltup2->t_self, reltup2);

        /* Keep system catalogs current */
        indstate = CatalogOpenIndexes(relRelation);
        CatalogIndexInsert(indstate, reltup1);
        CatalogIndexInsert(indstate, reltup2);
        CatalogCloseIndexes(indstate);
    } else {
        /* no update ... but we do still need relcache inval */
        CacheInvalidateRelcacheByTuple(reltup1);
        CacheInvalidateRelcacheByTuple(reltup2);
    }

    /*
     * If we have toast tables associated with the relations being swapped,
     * deal with them too.
     */
    if (relform1->reltoastrelid || relform2->reltoastrelid) {
        if (swap_toast_by_content) {
            if (relform1->reltoastrelid && relform2->reltoastrelid) {
                /* Recursively swap the contents of the toast tables */
                swap_relation_files(relform1->reltoastrelid,
                    relform2->reltoastrelid,
                    target_is_pg_class,
                    swap_toast_by_content,
                    frozenXid,
                    frozenMulti,
                    mapped_tables);
            } else {
                /* caller messed up */
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                        errmsg("cannot swap toast files by content when there's only one")));
            }
        } else {
            /*
             * We swapped the ownership links, so we need to change dependency
             * data to match.
             *
             * NOTE: it is possible that only one table has a toast table.
             *
             * NOTE: at present, a TOAST table's only dependency is the one on
             * its owning table.  If more are ever created, we'd need to use
             * something more selective than deleteDependencyRecordsFor() to
             * get rid of just the link we want.
             */
            ObjectAddress baseobject, toastobject;
            long count;

            /*
             * We disallow this case for system catalogs, to avoid the
             * possibility that the catalog we're rebuilding is one of the
             * ones the dependency changes would change.  It's too late to be
             * making any data changes to the target catalog.
             */
            if (IsSystemClass(relform1))
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                        errmsg("cannot swap toast files by links for system catalogs")));

            /* Delete old dependencies */
            if (relform1->reltoastrelid) {
                count = deleteDependencyRecordsFor(RelationRelationId, relform1->reltoastrelid, false);
                if (count != 1)
                    ereport(ERROR,
                        (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                            errmsg("expected one dependency record for TOAST table, found %ld", count)));
            }
            if (relform2->reltoastrelid) {
                count = deleteDependencyRecordsFor(RelationRelationId, relform2->reltoastrelid, false);
                if (count != 1)
                    ereport(ERROR,
                        (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                            errmsg("expected one dependency record for TOAST table, found %ld", count)));
            }

            /* Register new dependencies */
            baseobject.classId = RelationRelationId;
            baseobject.objectSubId = 0;
            toastobject.classId = RelationRelationId;
            toastobject.objectSubId = 0;

            if (relform1->reltoastrelid) {
                baseobject.objectId = r1;
                toastobject.objectId = relform1->reltoastrelid;
                recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
            }

            if (relform2->reltoastrelid) {
                baseobject.objectId = r2;
                toastobject.objectId = relform2->reltoastrelid;
                recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
            }
        }
    }

    /*
     * If we have delta tables or CUDesc tables associated with the relations being swapped,
     * deal with them too
     * */
    SwapCStoreTables(relform1->relcudescrelid, relform2->relcudescrelid, r1, r2);
    SwapCStoreTables(relform1->reldeltarelid, relform2->reldeltarelid, r1, r2);

    /* data redistribution for DFS table. */
    swap_relation_names(relform1->relcudescrelid, relform2->relcudescrelid);
    swap_relation_names(relform1->reldeltarelid, relform2->reldeltarelid);

    /*
     * If we're swapping two toast tables by content, do the same for their
     * indexes.
     */
    if (swap_toast_by_content && relform1->reltoastidxid && relform2->reltoastidxid)
        swap_relation_files(relform1->reltoastidxid,
            relform2->reltoastidxid,
            target_is_pg_class,
            swap_toast_by_content,
            InvalidTransactionId,
            InvalidMultiXactId,
            mapped_tables);
    /* Clean up. */
    if (nctup)
        heap_freetuple(nctup);
    heap_freetuple(reltup1);
    heap_freetuple(reltup2);

    heap_close(relRelation, RowExclusiveLock);

    /*
     * Close both relcache entries' smgr links.  We need this kluge because
     * both links will be invalidated during upcoming CommandCounterIncrement.
     * Whichever of the rels is the second to be cleared will have a dangling
     * reference to the other's smgr entry.  Rather than trying to avoid this
     * by ordering operations just so, it's easiest to close the links first.
     * (Fortunately, since one of the entries is local in our transaction,
     * it's sufficient to clear out our own relcache this way; the problem
     * cannot arise for other backends when they see our update on the
     * non-transient relation.)
     *
     * Caution: the placement of this step interacts with the decision to
     * handle toast rels by recursion.	When we are trying to rebuild pg_class
     * itself, the smgr close on pg_class must happen after all accesses in
     * this function.
     */
    RelationCloseSmgrByOid(r1);
    RelationCloseSmgrByOid(r2);
}

static void swap_relation_names(Oid r1, Oid r2)
{
    if (!OidIsValid(r1) || !OidIsValid(r2))
        return;

    Relation relation_r1, relation_r2;
    Oid toastidx_r1, toastidx_r2;
    char newName_tmp[NAMEDATALEN], newName_r1[NAMEDATALEN], newName_r2[NAMEDATALEN];

    relation_r1 = relation_open(r1, AccessShareLock);
    relation_r2 = relation_open(r2, AccessShareLock);

    errno_t rc = EOK;
    rc = snprintf_s(newName_tmp, NAMEDATALEN, NAMEDATALEN - 1, "pg_temp_%u_%u", r1, r2);
    securec_check_ss(rc, "\0", "\0");

    rc = strncpy_s(newName_r1, NAMEDATALEN, RelationGetRelationName(relation_r2), NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    rc = strncpy_s(newName_r2, NAMEDATALEN, RelationGetRelationName(relation_r1), NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    // also swap names of toast indexes.
    if (IsToastRelation(relation_r1) && IsToastRelation(relation_r2)) {
        toastidx_r1 = relation_r1->rd_rel->reltoastidxid;
        toastidx_r2 = relation_r2->rd_rel->reltoastidxid;
        swap_relation_names(toastidx_r1, toastidx_r2);
    }
    relation_close(relation_r1, AccessShareLock);
    relation_close(relation_r2, AccessShareLock);

    RenameRelationInternal(r2, newName_tmp);
    CommandCounterIncrement();
    RenameRelationInternal(r1, newName_r1);
    CommandCounterIncrement();
    RenameRelationInternal(r2, newName_r2);
    CommandCounterIncrement();
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        : scan or rewrite one partitioned table
 * Description  : Swap the physical files of two given relations.
 * Notes        :
 * Input        :
 * Output       : NA
 */
static void swapPartitionfiles(
    Oid partitionOid, Oid tempTableOid, bool swapToastByContent, TransactionId frozenXid,
    MultiXactId multiXid, Oid* mappedTables)
{
    Relation relRelation1 = NULL;
    Relation relRelation2 = NULL;
    HeapTuple reltup1 = NULL;
    HeapTuple reltup2 = NULL;
    HeapTuple ntup = NULL;
    Form_pg_partition relform1 = NULL;
    Form_pg_class relform2 = NULL;
    Oid relfilenode1 = InvalidOid;
    Oid relfilenode2 = InvalidOid;
    Oid swaptemp = InvalidOid;
    CatalogIndexState indstate1 = NULL;
    CatalogIndexState indstate2 = NULL;

    /* We need writable copies of both pg_class tuples. */
    relRelation2 = heap_open(RelationRelationId, RowExclusiveLock);
    relRelation1 = heap_open(PartitionRelationId, RowExclusiveLock);

    reltup1 = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partitionOid));
    if (!HeapTupleIsValid(reltup1)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", partitionOid)));
    }
    relform1 = (Form_pg_partition)GETSTRUCT(reltup1);

    reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(tempTableOid));
    if (!HeapTupleIsValid(reltup2)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", tempTableOid)));
    }
    relform2 = (Form_pg_class)GETSTRUCT(reltup2);

    relfilenode1 = relform1->relfilenode;
    relfilenode2 = relform2->relfilenode;

    if (OidIsValid(relfilenode1) && OidIsValid(relfilenode2)) {
        swaptemp = relform1->relfilenode;
        relform1->relfilenode = relform2->relfilenode;
        relform2->relfilenode = swaptemp;

        swaptemp = relform1->reltablespace;
        relform1->reltablespace = relform2->reltablespace;
        relform2->reltablespace = swaptemp;

        /* Also swap toast/cudesc/delta links, if we're swapping by links */
        if (!swapToastByContent) {
            swaptemp = relform1->reltoastrelid;
            relform1->reltoastrelid = relform2->reltoastrelid;
            relform2->reltoastrelid = swaptemp;
        }

        // Any way, we should swap cudesc,delta by links
        //
        swaptemp = relform1->relcudescrelid;
        relform1->relcudescrelid = relform2->relcudescrelid;
        relform2->relcudescrelid = swaptemp;

        swaptemp = relform1->reldeltarelid;
        relform1->reldeltarelid = relform2->reldeltarelid;
        relform2->reldeltarelid = swaptemp;
    }

    /* set rel1's frozen Xid */
    if (relform1->parttype != PART_OBJ_TYPE_INDEX_PARTITION) {
        Datum values[Natts_pg_partition];
        bool nulls[Natts_pg_partition];
        bool replaces[Natts_pg_partition];
        errno_t rc;
        HeapTuple tmp;

        relform1->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
        values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenXid);

#ifndef ENABLE_MULTIPLE_NODES
        replaces[Anum_pg_partition_relminmxid - 1] = true;
        values[Anum_pg_partition_relminmxid - 1] = TransactionIdGetDatum(multiXid);
#endif

        ntup = (HeapTuple) tableam_tops_modify_tuple(reltup1, RelationGetDescr(relRelation1), values, nulls, replaces);

        relform1 = (Form_pg_partition)GETSTRUCT(ntup);

        tmp = ntup;
        ntup = reltup1;
        reltup1 = tmp;
    }

    /* swap size statistics too, since new rel has freshly-updated stats */
    {
        float8 swap_pages;
        float4 swap_tuples;
        int4 swap_allvisible;

        swap_pages = relform1->relpages;
        relform1->relpages = relform2->relpages;
        relform2->relpages = swap_pages;

        swap_tuples = relform1->reltuples;
        relform1->reltuples = relform2->reltuples;
        relform2->reltuples = swap_tuples;

        swap_allvisible = relform1->relallvisible;
        relform1->relallvisible = relform2->relallvisible;
        relform2->relallvisible = swap_allvisible;
    }

    /*
     * Update the tuples in pg_class and pg_partiton
     */
    simple_heap_update(relRelation1, &reltup1->t_self, reltup1);
    simple_heap_update(relRelation2, &reltup2->t_self, reltup2);

    /* Keep system catalogs current */
    indstate1 = CatalogOpenIndexes(relRelation1);
    indstate2 = CatalogOpenIndexes(relRelation2);
    CatalogIndexInsert(indstate1, reltup1);
    CatalogIndexInsert(indstate2, reltup2);
    CatalogCloseIndexes(indstate1);
    CatalogCloseIndexes(indstate2);

    /*
     * If we have toast tables associated with the relations being swapped,
     * deal with them too.
     */
    swapCascadeHeapTables(
        relform1->reltoastrelid, relform2->reltoastrelid, tempTableOid, swapToastByContent, frozenXid,
        multiXid, mappedTables);
    SwapCStoreTables(relform1->relcudescrelid, relform2->relcudescrelid, InvalidOid, tempTableOid);
    SwapCStoreTables(relform1->reldeltarelid, relform2->reldeltarelid, InvalidOid, tempTableOid);

    /*
     * If we're swapping two toast tables by content, do the same for their
     * indexes.
     */
    if (swapToastByContent && relform1->reltoastidxid && relform2->reltoastidxid)
        swap_relation_files(relform1->reltoastidxid,
            relform2->reltoastidxid,
            false,
            swapToastByContent,
            InvalidTransactionId,
            InvalidMultiXactId,
            mappedTables);

    /* Clean up. */
    if (ntup)
        heap_freetuple(ntup);
    heap_freetuple(reltup1);
    heap_freetuple(reltup2);

    heap_close(relRelation1, RowExclusiveLock);
    heap_close(relRelation2, RowExclusiveLock);

    /*
     * Close both relcache entries' smgr links.
     */
    PartitionCloseSmgrByOid(partitionOid);
    RelationCloseSmgrByOid(tempTableOid);
}

static void swapCascadeHeapTables(
    Oid relId1, Oid relId2, Oid tempTableOid, bool swapByContent, TransactionId frozenXid,
    MultiXactId multiXid, Oid* mappedTables)
{
    if (relId1 || relId2) {
        if (swapByContent) {
            if (relId1 && relId2) {
                /* Recursively swap the contents of the toast tables */
                swap_relation_files(relId1, relId2, false, swapByContent, frozenXid, multiXid, mappedTables);
            } else {
                /* caller messed up */
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                        errmsg("cannot swap toast files by content when there's only one")));
            }
        } else {
            /*
             * We swapped the ownership links, so we need to change dependency
             * data to match.
             */
            ObjectAddress baseobject, heapobject;
            long count;

            /* Delete old dependencies */
            if (relId1) {
                count = deleteDependencyRecordsFor(RelationRelationId, relId1, false);
                if (count != 1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                            errmsg("expected one dependency record for TOAST table, found %ld", count)));
                }
            }

            if (relId2) {
                count = deleteDependencyRecordsFor(RelationRelationId, relId2, false);
                if (count != 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                            errmsg("expected none dependency record for partiton's TOAST table, found %ld", count)));
                }
            }

            /* Register new dependencies */
            baseobject.classId = RelationRelationId;
            baseobject.objectSubId = 0;
            heapobject.classId = RelationRelationId;
            heapobject.objectSubId = 0;

            if (relId2) {
                baseobject.objectId = tempTableOid;
                heapobject.objectId = relId2;
                recordDependencyOn(&heapobject, &baseobject, DEPENDENCY_INTERNAL);
            }

            // if swap toast table by link, we also need to swap the names of relId1 and relId2
            if (relId1 && relId2)
                swap_relation_names(relId1, relId2);
        }
    }
}

// if parentOid != InvalidOid, it's not a partition table.
// else it's partition table
//
static void SwapCStoreTables(Oid relId1, Oid relId2, Oid parentOid, Oid tempTableOid)
{
    if (relId1 && relId2) {
        /*
         * We swapped the ownership links, so we need to change dependency
         * data to match.
         */
        ObjectAddress baseobject, heapobject;
        long count;

        /* Delete old dependencies */
        if (relId1) {
            count = deleteDependencyRecordsFor(RelationRelationId, relId1, false);
            // if partition table, count should be 0
            // else if not a partition table, count should be 1
            //
            if (!tempTableOid && count != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                        errmsg("expected none dependency record for partition's CUDesc/Delta table, found %ld", count)));
            }
        }

        if (relId2) {
            count = deleteDependencyRecordsFor(RelationRelationId, relId2, false);
            // if partition table, count should be 0
            // else if not a partition table, count should be 1
            //
            if (!parentOid && count != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                        errmsg("expected none dependency record for partition's CUDesc/Delta table, found %ld", count)));
            } else if (parentOid && count != 1)
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                        errmsg("expected one dependency record for CUDesc/Delta table, found %ld", count)));
        }

        /* Register new dependencies */
        baseobject.classId = RelationRelationId;
        baseobject.objectSubId = 0;
        heapobject.classId = RelationRelationId;
        heapobject.objectSubId = 0;

        // if not a partition table, add dependency to parent table,
        // else skip it.
        //
        if (relId1 && parentOid) {
            baseobject.objectId = parentOid;
            heapobject.objectId = relId1;
            recordDependencyOn(&heapobject, &baseobject, DEPENDENCY_INTERNAL);
        }

        if (relId2 && tempTableOid) {
            Assert(OidIsValid(tempTableOid));
            baseobject.objectId = tempTableOid;
            heapobject.objectId = relId2;
            recordDependencyOn(&heapobject, &baseobject, DEPENDENCY_INTERNAL);
        }
    }
}

/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 */
void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap, bool is_system_catalog, bool swap_toast_by_content,
    bool checkConstraints, TransactionId frozenXid, MultiXactId frozenMulti, AdaptMem* memInfo)
{
    ObjectAddress object;
    Oid mapped_tables[4];
    int reindex_flags;
    int i;
    errno_t rc = EOK;

    /* Zero out possible results from swapped_relation_files */
    rc = memset_s(mapped_tables, sizeof(mapped_tables), 0, sizeof(mapped_tables));
    securec_check(rc, "\0", "\0");

    /*
     * Swap the contents of the heap relations (including any toast tables).
     * Also set old heap's relfrozenxid to frozenXid.
     */
    if (get_rel_persistence(OIDOldHeap) == RELPERSISTENCE_GLOBAL_TEMP) {
        Assert(!is_system_catalog);
        GttSwapRelationFiles(OIDOldHeap, OIDNewHeap);
    } else {
        swap_relation_files(OIDOldHeap, OIDNewHeap, (OIDOldHeap == RelationRelationId), 
            swap_toast_by_content, frozenXid, frozenMulti, mapped_tables);
    }

    /*
     * If it's a system catalog, queue an sinval message to flush all
     * catcaches on the catalog when we reach CommandCounterIncrement.
     */
    if (is_system_catalog)
        CacheInvalidateCatalog(OIDOldHeap);

    /*
     * Rebuild each index on the relation (but not the toast table, which is
     * all-new at this point).	It is important to do this before the DROP
     * step because if we are processing a system catalog that will be used
     * during DROP, we want to have its indexes available.	There is no
     * advantage to the other order anyway because this is all transactional,
     * so no chance to reclaim disk space before commit.  We do not need a
     * final CommandCounterIncrement() because ReindexRelation does it.
     *
     * Note: because index_build is called via ReindexRelation, it will never
     * set indcheckxmin true for the indexes.  This is OK even though in some
     * sense we are building new indexes rather than rebuilding existing ones,
     * because the new heap won't contain any HOT chains at all, let alone
     * broken ones, so it can't be necessary to set indcheckxmin.
     */
    reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
    if (checkConstraints)
        reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;
    ReindexRelation(OIDOldHeap, reindex_flags, REINDEX_ALL_INDEX, NULL, memInfo);

#ifndef ENABLE_MULTIPLE_NODES
    if (RelationIsCUFormatByOid(OIDOldHeap)) {
        /*
         * Each delta table of OIDNewHeap and OIDHeap will not be swapped.
         * We will build index of new delta table. After swap_relation_files,
         * OIDNewHeap has the old relfilenode, OIDOldHeap has the new relfilenode.
         */
        BuildIndexOnNewDeltaTable(OIDNewHeap, OIDOldHeap);
    }
#endif

    /* Destroy new heap with old filenode */
    object.classId = RelationRelationId;
    object.objectId = OIDNewHeap;
    object.objectSubId = 0;

    /*
     * The new relation is local to our transaction and we know nothing
     * depends on it, so DROP_RESTRICT should be OK.
     *
     * performDeletion does CommandCounterIncrement at end
     */
    performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    /*
     * Now we must remove any relation mapping entries that we set up for the
     * transient table, as well as its toast table and toast index if any. If
     * we fail to do this before commit, the relmapper will complain about new
     * permanent map entries being added post-bootstrap.
     */
    for (i = 0; OidIsValid(mapped_tables[i]); i++)
        RelationMapRemoveMapping(mapped_tables[i]);

    /*
     * At this point, everything is kosher except that, if we did toast swap
     * by links, the toast table's name corresponds to the transient table.
     * The name is irrelevant to the backend because it's referenced by OID,
     * but users looking at the catalogs could be confused.  Rename it to
     * prevent this problem.
     *
     * Note no lock required on the relation, because we already hold an
     * exclusive lock on it.
     */
    if (!swap_toast_by_content) {
        Relation newrel;

        newrel = heap_open(OIDOldHeap, NoLock);
        if (OidIsValid(newrel->rd_rel->reltoastrelid)) {
            Relation toastrel;
            Oid toastidx;
            char NewToastName[NAMEDATALEN];

            toastrel = relation_open(newrel->rd_rel->reltoastrelid, AccessShareLock);
            toastidx = toastrel->rd_rel->reltoastidxid;
            relation_close(toastrel, AccessShareLock);

            /* rename the toast table ... */
            rc = snprintf_s(NewToastName, NAMEDATALEN, NAMEDATALEN - 1, "pg_toast_%u", OIDOldHeap);
            securec_check_ss(rc, "\0", "\0");
            RenameRelationInternal(newrel->rd_rel->reltoastrelid, NewToastName);

            /* ... and its index too */
            rc = snprintf_s(NewToastName, NAMEDATALEN, NAMEDATALEN - 1, "pg_toast_%u_index", OIDOldHeap);
            securec_check_ss(rc, "\0", "\0");
            RenameRelationInternal(toastidx, NewToastName);
        }
        relation_close(newrel, NoLock);
    }
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        : scan or rewrite one partitioned table
 * Description  : Remove the transient table that was built by make_new_heap, and finish
 *                cleaning up (including rebuilding all indexes on the old heap).
 * Notes        :
 * Input        :
 * Output       : NA
 */
void finishPartitionHeapSwap(
    Oid partitionOid, Oid tempTableOid, bool swapToastByContent, TransactionId frozenXid,
    MultiXactId multiXid, bool tempTableIsPartition)
{
    Oid mapped_tables[4];
    int i = 0;
    errno_t rc = EOK;

    /* Zero out possible results from swapped_relation_files */
    rc = memset_s(mapped_tables, sizeof(mapped_tables), 0, sizeof(mapped_tables));
    securec_check(rc, "\0", "\0");

    /*
     * Swap the contents of the heap relations (including any toast tables).
     * Also set old heap's relfrozenxid to frozenXid.
     */
    if (tempTableIsPartition) {
        /* For redistribution, exchange meta info between two partitions */
        swap_partition_relfilenode(partitionOid, tempTableOid, swapToastByContent, frozenXid, multiXid, mapped_tables);
    } else {
        /* For alter table exchange, between partition and a normal table */
        swapPartitionfiles(partitionOid, tempTableOid, swapToastByContent, frozenXid, multiXid, mapped_tables);
    }

    /*
     * Now we must remove any relation mapping entries that we set up for the
     * transient table, as well as its toast table and toast index if any. If
     * we fail to do this before commit, the relmapper will complain about new
     * permanent map entries being added post-bootstrap.
     */
    for (i = 0; OidIsValid(mapped_tables[i]); i++) {
        RelationMapRemoveMapping(mapped_tables[i]);
    }
}

/*
 * Get a list of tables that the current user owns and
 * have indisclustered set.  Return the list in a List * of rvsToCluster
 * with the tableOid and the indexOid on which the table is already
 * clustered.
 */
static List* get_tables_to_cluster(MemoryContext cluster_context)
{
    Relation indRelation;
    TableScanDesc scan;
    ScanKeyData entry;
    HeapTuple indexTuple;
    Form_pg_index index;
    MemoryContext old_context;
    RelToCluster* rvtc = NULL;
    List* rvs = NIL;

    /*
     * Get all indexes that have indisclustered set and are owned by
     * appropriate user. System relations or nailed-in relations cannot ever
     * have indisclustered set, because CLUSTER will refuse to set it when
     * called with one of them as argument.
     */
    indRelation = heap_open(IndexRelationId, AccessShareLock);
    ScanKeyInit(&entry, Anum_pg_index_indisclustered, BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));
    scan = tableam_scan_begin(indRelation, SnapshotNow, 1, &entry);
    while ((indexTuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Assert(TUPLE_IS_HEAP_TUPLE(indexTuple));
        index = (Form_pg_index)GETSTRUCT(indexTuple);

        /* Skip object in recycle bin. */
        if (TrIsRefRbObjectEx(RelationRelationId, index->indrelid)) {
            continue;
        }

        if (!pg_class_ownercheck(index->indrelid, GetUserId()))
            continue;

        /*
         * We have to build the list in a different memory context so it will
         * survive the cross-transaction processing
         */
        old_context = MemoryContextSwitchTo(cluster_context);

        rvtc = (RelToCluster*)palloc(sizeof(RelToCluster));
        rvtc->tableOid = index->indrelid;
        rvtc->indexOid = index->indexrelid;
        rvs = lcons(rvtc, rvs);

        MemoryContextSwitchTo(old_context);
    }
    tableam_scan_end(scan);

    relation_close(indRelation, AccessShareLock);

    return rvs;
}

static void GttSwapRelationFiles(Oid r1, Oid r2)
{
    Oid         relfilenode1;
    Oid         relfilenode2;
    Relation    rel1;
    Relation    rel2;

    rel1 = relation_open(r1, AccessExclusiveLock);
    rel2 = relation_open(r2, AccessExclusiveLock);

    relfilenode1 = gtt_fetch_current_relfilenode(r1);
    relfilenode2 = gtt_fetch_current_relfilenode(r2);

    Assert(OidIsValid(relfilenode1) && OidIsValid(relfilenode2));
    gtt_switch_rel_relfilenode(r1, relfilenode1, r2, relfilenode2, true);

    CacheInvalidateRelcache(rel1);
    CacheInvalidateRelcache(rel2);

    if (rel1->rd_rel->reltoastrelid && rel2->rd_rel->reltoastrelid) {
        GttSwapRelationFiles(rel1->rd_rel->reltoastrelid, rel2->rd_rel->reltoastrelid);
    }

    if (rel1->rd_rel->relkind == RELKIND_TOASTVALUE && rel2->rd_rel->relkind == RELKIND_TOASTVALUE) {
        GttSwapRelationFiles(rel1->rd_rel->reltoastidxid, rel2->rd_rel->reltoastidxid);
    }

    relation_close(rel1, NoLock);
    relation_close(rel2, NoLock);

    RelationCloseSmgrByOid(r1);
    RelationCloseSmgrByOid(r2);

    CommandCounterIncrement();
}

/*
 * Reconstruct and rewrite the given tuple
 * We cannot simply copy the tuple as-is, for several reasons
 * 1. We'd like to squeeze out the values of any dropped columns, both
 * to save space and to ensure we have no corner-case failures. (It's
 * possible for example that the new table hasn't got a TOAST table
 * and so is unable to store any large values of dropped cols.)
 * 2. The tuple might not even be legal for the new table; this is
 * currently only known to happen as an after-effect of ALTER TABLE
 * SET WITHOUT OIDS.
 * So, we must reconstruct the tuple from component Datums.
 */
static void reform_and_rewrite_tuple(HeapTuple tuple, TupleDesc oldTupDesc, TupleDesc newTupDesc, Datum* values,
    bool* isnull, bool newRelHasOids, RewriteState rwstate)
{
    HeapTuple copiedTuple;
    int i;
    MemoryContext oldMemCxt = NULL;

    tableam_tops_deform_tuple(tuple, oldTupDesc, values, isnull);

    /* Be sure to null out any dropped columns */
    for (i = 0; i < newTupDesc->natts; i++) {
        if (newTupDesc->attrs[i]->attisdropped)
            isnull[i] = true;
    }

    bool usePrivateMemcxt = use_heap_rewrite_memcxt(rwstate);
    if (usePrivateMemcxt)
        oldMemCxt = MemoryContextSwitchTo(get_heap_rewrite_memcxt(rwstate));
    copiedTuple = (HeapTuple)heap_form_tuple(newTupDesc, values, isnull);

    /* Preserve OID, if any */
    if (newRelHasOids)
        HeapTupleSetOid(copiedTuple, HeapTupleGetOid(tuple));

    /* The heap rewrite module does the rest */
    if (usePrivateMemcxt) {
        RewriteAndCompressTup(rwstate, tuple, copiedTuple);
        (void)MemoryContextSwitchTo(oldMemCxt);
    } else {
        rewrite_heap_tuple(rwstate, tuple, copiedTuple);
        tableam_tops_free_tuple(copiedTuple);
    }
}

/*
 * GpiVacuumFullMainPartiton
 *
 * Clean up global partition index finally for the vacuum full.
 */
void GpiVacuumFullMainPartiton(Oid parentOid)
{
    Relation parentHeap = NULL;

    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();

    // to promote the concurrency of vacuum full on partitions in mppdb version,
    // degrade lockmode from AccessExclusiveLock to AccessShareLock.
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    parentHeap = try_relation_open(parentOid, AccessExclusiveLock);

    /* If the table has gone away, we can skip processing it */
    if (!parentHeap)
        return;

    /*
     * Don't process temp tables of other backends ... their local buffer
     * manager is not going to cope.
     */
    if (RELATION_IS_OTHER_TEMP(parentHeap)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot vacuum temporary tables of other sessions")));
    }

    /*
     * Also check for active uses of the relation in the current transaction,
     * including open scans and pending AFTER trigger events.
     */
    CheckTableNotInUse(parentHeap, "VACUUM");

    /* Update this partition's system catalog tuple in pg_partiton to make it can be cleaned up */
    PartitionSetAllEnabledClean(RelationGetRelid(parentHeap));
    heap_close(parentHeap, NoLock);
}

void CBIVacuumFullMainPartiton(Oid parentOid)
{
    Relation parentHeap = NULL;
    Relation* iRel = NULL;
    int nindexes;
    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();
    // to promote the concurrency of vacuum full on partitions in mppdb version,
    // degrade lockmode from AccessExclusiveLock to AccessShareLock.
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    parentHeap = try_relation_open(parentOid, AccessExclusiveLock);
    /* If the table has gone away, we can skip processing it */
    if (!parentHeap)
        return;
    /*
     * Don't process temp tables of other backends ... their local buffer
     * manager is not going to cope.
     */
    if (RELATION_IS_OTHER_TEMP(parentHeap)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot vacuum temporary tables of other sessions")));
    }

    /* If vacuum full partitioned segment table, give hint here */
    if (parentHeap->storage_type == SEGMENT_PAGE) {
        ereport(INFO, (errmsg("skipping segment table \"%s\" --- please use gs_space_shrink "
            "to recycle segment space.", RelationGetRelationName(parentHeap))));
    }

    /*
     * Also check for active uses of the relation in the current transaction,
     * including open scans and pending AFTER trigger events.
     */
    CheckTableNotInUse(parentHeap, "VACUUM");

    CBIOpenLocalCrossbucketIndex(parentHeap, RowExclusiveLock, &nindexes, &iRel);
    for (int i = 0; i < nindexes; i++) {
        if (IndexEnableWaitCleanCbi(iRel[i])) {
            cbi_set_enable_clean(iRel[i]);
        }
        index_close(iRel[i], NoLock);
    }
    heap_close(parentHeap, NoLock);
    pfree_ext(iRel);
}

/*
 * Reconstruct and rewrite the given utuple
 *
 * We cannot simply copy the tuple as-is, for several reasons:
 *
 * 1. We'd like to squeeze out the values of any dropped columns, both
 * to save space and to ensure we have no corner-case failures. (It's
 * possible for example that the new table hasn't got a TOAST table
 * and so is unable to store any large values of dropped cols.)
 *
 * 2. The tuple might not even be legal for the new table; this is
 * currently only known to happen as an after-effect of ALTER TABLE
 * SET WITHOUT OIDS.
 *
 * So, we must reconstruct the tuple from component Datums.
 */
static void
ReformAndRewriteUTuple(UHeapTuple tuple,
                                                 TupleDesc oldTupDesc, TupleDesc newTupDesc,
                                                 Datum *values, bool *isnull,
                                                 bool newRelHasOids, RewriteState rwstate)
{
        Tuple        copiedTuple;
        int                     i;
        MemoryContext oldMemCxt = NULL;

        // EB:
        tableam_tops_deform_tuple(tuple, oldTupDesc, values, isnull);

        /* Be sure to null out any dropped columns */
        for (i = 0; i < newTupDesc->natts; i++)
        {
                if (newTupDesc->attrs[i]->attisdropped)
                        isnull[i] = true;
        }

        bool usePrivateMemcxt = use_heap_rewrite_memcxt(rwstate);
        if (usePrivateMemcxt)
                oldMemCxt = MemoryContextSwitchTo(get_heap_rewrite_memcxt(rwstate));

        copiedTuple = tableam_tops_form_tuple(newTupDesc, values, isnull, UHEAP_TUPLE);

        {
                RewriteUHeapTuple(rwstate, tuple, (UHeapTuple)copiedTuple);
                UHeapFreeTuple(copiedTuple);
        }
}

/*
 * vacuumFullPart
 *
 * This vacuum the table by creating a new, clustered table and
 * swapping the relfilenodes of the new table and the old table, so
 * the OID of the original table is preserved.	Thus we do not lose
 * GRANT, inheritance nor references to this table (this was a bug
 * in releases thru 7.3).
 */
void vacuumFullPart(Oid partOid, VacuumStmt* vacstmt, int freeze_min_age, int freeze_table_age)
{
    Relation oldHeap = NULL;
    Oid oldRelOid;

    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();

    /*
     * We grab exclusive access to the target rel and index for the duration
     * of the transaction.	(This is redundant for the single-transaction
     * case, since cluster() already did it.)  The index lock is taken inside
     * check_index_is_clusterable.
     */
    if (!vacstmt->issubpartition) {
        oldRelOid = partid_get_parentid(partOid);
    } else {
        Oid subparentid = partid_get_parentid(partOid);
        oldRelOid = partid_get_parentid(subparentid);
    }

    /// to promote the concurrency of vacuum full on partitions in mppdb version,
    /// degrade lockmode from AccessExclusiveLock to AccessShareLock.
    oldHeap = try_relation_open(oldRelOid, AccessShareLock);

    /* If the table has gone away, we can skip processing it */
    if (!oldHeap)
        return;

    /*
     * Don't process temp tables of other backends ... their local buffer
     * manager is not going to cope.
     */
    if (RELATION_IS_OTHER_TEMP(oldHeap)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot vacuum temporary tables of other sessions")));
    }

    if (oldHeap->storage_type == SEGMENT_PAGE) {
        relation_close(oldHeap, AccessShareLock);
        return;
    }

    /*
     * Also check for active uses of the relation in the current transaction,
     * including open scans and pending AFTER trigger events.
     */
    CheckTableNotInUse(oldHeap, "VACUUM");

#ifdef ENABLE_MULTIPLE_NODES
    if (unlikely(RelationIsTsStore(oldHeap)) && g_instance.attr.attr_common.enable_tsdb) {
        Tsdb::VacFullCompaction(oldHeap, partOid);
    } else {
        rebuildPartVacFull(oldHeap, partOid, freeze_min_age, freeze_table_age, vacstmt);
    }
#else  // ENABLE_MULTIPLE_NODES
    rebuildPartVacFull(oldHeap, partOid, freeze_min_age, freeze_table_age, vacstmt);
#endif  // ENABLE_MULTIPLE_NODES

    /* NB: rebuildPartVacFull does heap_close() on OldHeap */
}

#ifdef ENABLE_MULTIPLE_NODES
namespace Tsdb {
/**
 * Used in tsdb. Execute VACUUM FULL in one partition.
 * This function first find all cudesc tables in the partition. Then, it calls MergeUtils::merge_parts() to
 * do the compaction work. After that, it drops old parts(cudesc tables, cu data files, timestamp files).
 * Parameters:
 *  - oldHeap: opened ts store table
 *  - partOid: oid of the partition to be compacted
 */
static void VacFullCompaction(Relation oldHeap, Oid partOid)
{
    if (u_sess->attr.attr_common.enable_ts_compaction) {
        ereport(WARNING, (errcode(MOD_TIMESERIES), errmsg("Ts compaction is on, please disable ts compaction first.")));
        return;
    }

    Partition part = partitionOpen(oldHeap, partOid, NoLock);
    LockRelationOid(partOid, AccessExclusiveLock);
    List *cudesc_oids = NIL;
    cudesc_oids = search_cudesc(partOid, false);
    /* It is unnecessary to do compaction if there is only one part in the partition */
    if (list_length(cudesc_oids) > 1) {
        List* target_cudesc = NIL;
        List* target_cudesc_oids = NIL;
        Relation tmp_cudesc_rel = NULL;
        ListCell *cudesc_cell = NULL;
        foreach(cudesc_cell, cudesc_oids) {
            Oid cudesc_oid = lfirst_oid(cudesc_cell);
            tmp_cudesc_rel = heap_open(cudesc_oid, AccessExclusiveLock);
            if (target_cudesc == NIL) {
                target_cudesc = list_make1(tmp_cudesc_rel);
                target_cudesc_oids = list_make1_oid(cudesc_oid);
            } else {
                lappend(target_cudesc, tmp_cudesc_rel);
                lappend_oid(target_cudesc_oids, cudesc_oid);
            }
        }
        Oid new_desc_oid = Tsdb::MergeUtils::merge_parts(oldHeap, partOid, target_cudesc);
        ListCell* cell = NULL;
        foreach (cell, target_cudesc) {
            tmp_cudesc_rel = (Relation)lfirst(cell);
            heap_close(tmp_cudesc_rel, NoLock);
        }
        Tsdb::PartCacheMgr::GetInstance().refresh_part_item_cache(partOid, new_desc_oid, target_cudesc_oids);
        Tsdb::DropPartStorage(
             partOid, &(part->pd_node), oldHeap->rd_backend, oldHeap->rd_rel->relowner, target_cudesc_oids);

        list_free_ext(target_cudesc);
        if (list_length(target_cudesc_oids) > 1) {
            list_free_ext(target_cudesc_oids);
        }
    }

    partitionClose(oldHeap, part, NoLock);
    UnlockRelationOid(partOid, AccessExclusiveLock);
    heap_close(oldHeap, NoLock);
    list_free_ext(cudesc_oids);
}
}
#endif  // ENABLE_MULTIPLE_NODES

static void rebuildPartVacFull(Relation oldHeap, Oid partOid, int freezeMinAge, int freezeTableAge, VacuumStmt* vacstmt)
{
    Oid tableOid = RelationGetRelid(oldHeap);
    uint32 statFlag = RelationIsSubPartitioned(oldHeap) ? partid_get_parentid(partOid) : tableOid;
    Oid OIDNewHeap = InvalidOid;
    bool swapToastByContent = false;
    TransactionId frozenXid = InvalidTransactionId;
    MultiXactId multiXid = InvalidMultiXactId;
    TupleDesc partTabHeapDesc;
    HeapTuple tuple = NULL;
    Datum partTabRelOptions = 0;
    bool isNull = false;
    Partition partition = NULL;
    Relation partRel = NULL;
    Partition parentpartition = NULL;
    Relation parentpartRel = NULL;
    Oid subparentid = InvalidOid;
    Relation newHeap = NULL;
    int reindexFlags = 0;
    ObjectAddress object;
    Relation partTable = NULL;
    bool isCStore = RelationIsColStore(oldHeap);
    bool verbose = (vacstmt->options & VACOPT_VERBOSE) != 0;
    double deleteTupleNum = 0;
    bool is_shared = false;
    /* Get desc of partitioned table */
    partTabHeapDesc = RelationGetDescr(oldHeap);

    tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(tableOid), LOG);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", tableOid)));
    }

    partTabRelOptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
    if (isNull) {
        partTabRelOptions = (Datum)0;
    }

    if (!vacstmt->issubpartition) {
        partition = partitionOpen(oldHeap, partOid, ExclusiveLock);
        partRel = partitionGetRelation(oldHeap, partition);
    } else {
        subparentid = partid_get_parentid(partOid);
        parentpartition = partitionOpen(oldHeap, subparentid, ExclusiveLock);
        parentpartRel = partitionGetRelation(oldHeap, parentpartition);
        partition = partitionOpen(parentpartRel, partOid, ExclusiveLock);
        partRel = partitionGetRelation(parentpartRel, partition);
    }
    is_shared = partRel->rd_rel->relisshared;

    /*
     * we need to transfer predicate lock here
     */
    TransferPredicateLocksToHeapRelation(partRel);

    /* Create the transient table that will receive the re-ordered data */
    OIDNewHeap = makePartitionNewHeap(oldHeap,
        partTabHeapDesc,
        partTabRelOptions,
        partRel->rd_id,
        partRel->rd_rel->reltoastrelid,
        partRel->rd_rel->reltablespace,
        isCStore);

    /* release until pointer to attr options within tuple is not used */
    ReleaseSysCache(tuple);

    /* Copy the heap data into the new table in the desired order */
    newHeap = heap_open(OIDNewHeap, AccessExclusiveLock);
    if (isCStore) {
        CopyCStoreData(partRel,
            newHeap,
            freezeMinAge,
            freezeTableAge,
            verbose,
            &swapToastByContent,
            &frozenXid,
            &vacstmt->memUsage);
        /*
         * We must hold AccessExclusiveLock on logical parent table
         * if relation is a ColStore relation and only vacuum full one partition.
         *
         * we need hold AccessExclusiveLock on partition before swap relfile node.
         */
        t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
        LockRelation(oldHeap, AccessExclusiveLock);
    } else {
        copyPartitionHeapData(newHeap,
            partRel,
            InvalidOid,
            NULL,
            NULL,
            freezeMinAge,
            freezeTableAge,
            verbose,
            &swapToastByContent,
            &frozenXid,
            &multiXid,
            &vacstmt->memUsage,
            &deleteTupleNum);
    }
    heap_close(newHeap, NoLock);

    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    List* indexGPIRelList = LockAllGlobalIndexes(oldHeap, AccessExclusiveLock);

    if (!vacstmt->issubpartition) {
        LockPartition(oldHeap->rd_id, partOid, AccessExclusiveLock, PARTITION_LOCK);
    } else {
        LockPartition(parentpartRel->rd_id, partOid, AccessExclusiveLock, PARTITION_LOCK);
        LockPartition(oldHeap->rd_id, subparentid, AccessExclusiveLock, PARTITION_LOCK);
    }

    /*
     * Swap the physical files of the target and transient tables, then
     * rebuild the target's indexes and throw away the transient table.
     */
    finishPartitionHeapSwap(partRel->rd_id, OIDNewHeap, swapToastByContent, frozenXid, multiXid);

    /* Close relcache entry, but keep lock until transaction commit */
    if (!vacstmt->issubpartition) {
        releaseDummyRelation(&partRel);
        partitionClose(oldHeap, partition, NoLock);
    } else {
        releaseDummyRelation(&partRel);
        partitionClose(parentpartRel, partition, NoLock);
        releaseDummyRelation(&parentpartRel);
        partitionClose(oldHeap, parentpartition, NoLock);
    }
    ReleaseLockAllGlobalIndexes(&indexGPIRelList, NoLock);
    heap_close(oldHeap, NoLock);

    /* Rebuild index of partitioned table */
    reindexFlags = REINDEX_REL_SUPPRESS_INDEX_USE;
    (void)ReindexRelation(tableOid, reindexFlags, REINDEX_ALL_INDEX, NULL, NULL, false, GLOBAL_INDEX);
    (void)reindexPartition(tableOid, partOid, reindexFlags, REINDEX_ALL_INDEX);

#ifndef ENABLE_MULTIPLE_NODES
    if (RelationIsCUFormatByOid(tableOid)) {
        /*
         * After partition heap swap, OIDNewHeap has the old partition relfilenode
         * and partOid has the new partition relfilenode.
         */
        BuildIndexOnNewDeltaTable(OIDNewHeap, partOid, tableOid);
    }
#endif

    /* Drop the temp tables for swapping */
    object.classId = RelationRelationId;
    object.objectId = OIDNewHeap;
    object.objectSubId = 0;

    performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    /* here this relation has hold AccessShareLock, don't worry about *partTable*
     * is NULL or not.
     */
    partTable = try_relation_open(tableOid, AccessShareLock);
    /* Update reltuples and relpages in pg_class for partitioned table. */
    vac_update_pgclass_partitioned_table(partTable, partTable->rd_rel->relhasindex, frozenXid, multiXid);
    /*
     * report vacuum full stat to PgStatCollector.
     * For CStore table, we delete all invisible tuple, so dead tuple should be 0; and
     * we use -1 to identify Cstore table and let PgStatCollector set deadtuple to 0.
     * For row table, we use oldestxmin to delete tuple, some dead tuples are not
     * deleted. So we send deleteTupleNum to PgStatCollector.
     */
    if (isCStore)
        pgstat_report_vacuum(partOid, statFlag, is_shared, -1);
    else
        pgstat_report_vacuum(partOid, statFlag, is_shared, deleteTupleNum);
    heap_close(partTable, NoLock);
}

static void CopyCStoreData(Relation oldRel, Relation newRel, int freeze_min_age, int freeze_table_age, bool verbose,
    bool* pSwapToastByContent, TransactionId* pFreezeXid, AdaptMem* mem_info)
{
    TupleDesc oldTupDesc;
    TransactionId FreezeXid;
    TransactionId OldestXmin;

    // Their tuple descriptors should be exactly alike, but here we only need
    // assume that they have the same number of columns.
    //
    oldTupDesc = RelationGetDescr(oldRel);
    Assert(oldTupDesc->natts == RelationGetDescr(newRel)->natts);

    /*
     * If the OldHeap, CUDesc and Delta have a toast table, get lock on the toast table to keep
     * it from being vacuumed.	This is needed because autovacuum processes
     * toast tables independently of their main tables, with no lock on the
     * latter.	If an autovacuum were to start on the toast table after we
     * compute our OldestXmin below, it would use a later OldestXmin, and then
     * possibly remove as DEAD toast tuples belonging to main tuples we think
     * are only RECENTLY_DEAD.	Then we'd fail while trying to copy those
     * tuples.
     *
     * We don't need to open the toast relation here, just lock it.  The lock
     * will be held till end of transaction.
     */
    if (oldRel->rd_rel->reltoastrelid)
        LockRelationOid(oldRel->rd_rel->reltoastrelid, ExclusiveLock);

    // we also lock the internal relation of CStore relation
    //
    if (oldRel->rd_rel->relcudescrelid) {
        Relation cudescrel = relation_open(oldRel->rd_rel->relcudescrelid, NoLock);
        if (cudescrel->rd_rel->reltoastrelid)
            LockRelationOid(cudescrel->rd_rel->reltoastrelid, ExclusiveLock);
        relation_close(cudescrel, NoLock);
    }
    if (oldRel->rd_rel->reldeltarelid) {
        Relation deltarel = relation_open(oldRel->rd_rel->reldeltarelid, NoLock);
        if (deltarel->rd_rel->reltoastrelid)
            LockRelationOid(deltarel->rd_rel->reltoastrelid, ExclusiveLock);
        relation_close(deltarel, NoLock);
    }

    /*
     * If both tables have TOAST tables, perform toast swap by content.  It is
     * possible that the old table has a toast table but the new one doesn't,
     * if toastable columns have been dropped.	In that case we have to do
     * swap by links.  This is okay because swap by content is only essential
     * for system catalogs, and we don't support schema changes for them.
     */
    if (oldRel->rd_rel->reltoastrelid && newRel->rd_rel->reltoastrelid) {
        *pSwapToastByContent = true;

        /* When doing swap by content, any toast pointers written into NewHeap
         * must use the old toast table's OID, because that's where the toast
         * data will eventually be found.  Set this up by setting rd_toastoid.
         * This also tells toast_save_datum() to preserve the toast value
         * OIDs, which we want so as not to invalidate toast pointers in
         * system catalog caches, and to avoid making multiple copies of a
         * single toast value.
         *
         * Note that we must hold NewHeap open until we are done writing data,
         * since the relcache will not guarantee to remember this setting once
         * the relation is closed.	Also, this technique depends on the fact
         * that no one will try to read from the NewHeap until after we've
         * finished writing it and swapping the rels --- otherwise they could
         * follow the toast pointers to the wrong place.  (It would actually
         * work for values copied over from the old toast table, but not for
         * any values that we toast which were previously not toasted.)
         */
        newRel->rd_toastoid = oldRel->rd_rel->reltoastrelid;
    } else
        *pSwapToastByContent = false;
    /*
     * compute xids used to freeze and weed out dead tuples.  We use -1
     * freeze_min_age to avoid having CLUSTER freeze tuples earlier than a
     * plain VACUUM would.
     **/
    vacuum_set_xid_limits(oldRel, freeze_min_age, freeze_table_age, &OldestXmin, &FreezeXid, NULL, NULL);
    bool isNull = false;
    TransactionId relfrozenxid = InvalidTransactionId;
    Relation rel;
    HeapTuple tuple;
    Datum xid64datum;

    if (RelationIsPartition(oldRel)) {
        rel = heap_open(PartitionRelationId, AccessShareLock);
        tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(oldRel->rd_id));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("cache lookup failed for relation %u", RelationGetRelid(oldRel))));
        }
        xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);
    } else {
        rel = heap_open(RelationRelationId, AccessShareLock);
        tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(oldRel->rd_id));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("cache lookup failed for relation %u", RelationGetRelid(oldRel))));
        }
        xid64datum = tableam_tops_tuple_getattr(tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
    }

    heap_close(rel, AccessShareLock);
    heap_freetuple(tuple);

    if (isNull) {
        relfrozenxid = oldRel->rd_rel->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

    if (TransactionIdPrecedes(FreezeXid, relfrozenxid))
        FreezeXid = relfrozenxid;

    *pFreezeXid = FreezeXid;

    if (RelationIsCUFormat(oldRel)) {
        /* for col store table */
        DoCopyCUFormatData(oldRel, newRel, oldTupDesc, mem_info);
    } else {
        /*
         * for data on hdfs
         *
         * make_new_heap() can not copy partiton info from old dfs table to new one,
         * so do it here, it's a little tricky.
         *
         * NB: new heap is temp heap and just visible in this transaction, and will
         *     be droped whatever transaction commit or abort, so any changes of the
         *     newRel is safe here.
         */
        char parttype;
        PartitionMap* partmap = NULL;
        if (RelationIsValuePartitioned(oldRel)) {
            /* new value for parttype */
            parttype = newRel->rd_rel->parttype;
            newRel->rd_rel->parttype = PARTTYPE_VALUE_PARTITIONED_RELATION;

            /* new value for partmap */
            Relation pg_relation = heap_open(PartitionRelationId, AccessShareLock);

            HeapTuple partitioned_tuple = searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, oldRel->rd_id);
            if (partitioned_tuple == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmodule(MOD_CACHE),
                        errmsg("Failed on finding partitioned tuple!\n")));
            }
            PartitionMap* copy_oldRel_partmap =
                (PartitionMap*)buildValuePartitionMap(oldRel, pg_relation, partitioned_tuple);

            heap_close(pg_relation, AccessShareLock);

            partmap = newRel->partMap;
            newRel->partMap = copy_oldRel_partmap;
        }

        PG_TRY();
        {
            DoCopyPaxFormatData(oldRel, newRel);
            CopyOldDeltaToNewRel(oldRel->rd_id, newRel->rd_id);
        }
        PG_CATCH();
        {
            /* restore old value for newRel */
            if (RelationIsValuePartitioned(oldRel)) {
                newRel->rd_rel->parttype = parttype;
                newRel->partMap = partmap;
            }
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* restore old value for newRel */
        if (RelationIsValuePartitioned(oldRel)) {
            newRel->rd_rel->parttype = parttype;
            newRel->partMap = partmap;
        }
    }
}

/**
 * filter droped column
 * give an error when we see a droped cloumn have none 0 ScalarValue in batch 
 */
static void filter_batch(const TupleDesc oldTupDesc, const VectorBatch* pbatch) 
{
    for (int i = 0; i < oldTupDesc->natts; i++) {
        if (!oldTupDesc->attrs[i]->attisdropped) {
            continue;
        }
        
        for (int j = 0; j < pbatch->m_arr[i].m_rows; j++) {
            if (pbatch->m_arr[i].m_vals[j] != 0) {
                pbatch->m_arr[i].m_vals[j] = 0;
                ereport(LOG, (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("droped column %d have not null scalar value in batch in row %d", i, j)));
            }
        }
    }
}

/*
 * copy the data of old col table to new col table
 */
static void DoCopyCUFormatData(Relation oldRel, Relation newRel, TupleDesc oldTupDesc, AdaptMem* mem_info)
{
    CStoreInsert* cstoreOpt = NULL;
    CStoreScanDesc scan = NULL;
    int16* colIdx = NULL;
    Form_pg_attribute* oldAttrs = NULL;
    MemInfoArg memInfo;

    // Init CStore insertion.
    //
    InsertArg args;
    CStoreInsert::InitInsertArg(newRel, NULL, true, args);
    args.sortType = BATCH_SORT;
    memInfo.canSpreadmaxMem = mem_info->max_mem;
    memInfo.MemSort = mem_info->work_mem;
    memInfo.partitionNum = 1;
    cstoreOpt = (CStoreInsert*)New(CurrentMemoryContext) CStoreInsert(newRel, args, false, NULL, &memInfo);

    /*
     * Init CStore scan.
     * */
    colIdx = (int16*)palloc0(sizeof(int16) * oldTupDesc->natts);
    oldAttrs = oldTupDesc->attrs;
    for (int i = 0; i < oldTupDesc->natts; i++)
        colIdx[i] = oldAttrs[i]->attnum;
    scan = CStoreBeginScan(oldRel, oldTupDesc->natts, colIdx, SnapshotNow, true);

    /*
     * Scan through the OldRel, sequentially;
     * Copy each batch into the NewRel.
     */
    VectorBatch* batch = NULL;
    do {
        CHECK_FOR_INTERRUPTS();

        batch = CStoreGetNextBatch(scan);
        if (!BatchIsNull(batch)) {
            filter_batch(oldTupDesc, batch);
            cstoreOpt->BatchInsert(batch, TABLE_INSERT_FROZEN);
        }
    } while (!CStoreIsEndScan(scan));
    cstoreOpt->SetEndFlag();
    cstoreOpt->BatchInsert((VectorBatch*)NULL, TABLE_INSERT_FROZEN);
    DELETE_EX(cstoreOpt);

    pfree_ext(colIdx);
    CStoreEndScan(scan);
    CStoreInsert::DeInitInsertArg(args);
}

/*
 * equal helper function for dfs table
 */
bool equal_dfsdesc(const void* _data1, const void* _data2)
{
    DFSDesc* desc1 = (DFSDesc*)_data1;
    DFSDesc* desc2 = (DFSDesc*)_data2;

    return desc1->GetDescId() == desc2->GetDescId();
}

void InsertNewFileToDfsPending(const char* filename, Oid ownerid, uint64 filesize)
{
    InsertIntoPendingDfsDelete(filename, false, ownerid, filesize);
}

/*
 * find all desc tuples needed to be merged from desc table.
 */
static List* FindMergedDescs(Relation oldRel, Relation newRel)
{
    List* merged_descs = NIL;
    List* all_descs = NIL;

    DFSDescHandler* old_handler =
        New(CurrentMemoryContext) DFSDescHandler(MAX_LOADED_DFSDESC, oldRel->rd_att->natts, oldRel);

    DFSDescHandler* new_handler =
        New(CurrentMemoryContext) DFSDescHandler(MAX_LOADED_DFSDESC, newRel->rd_att->natts, newRel);

    /*
     * decide the set of the desc tuples whether COMPACT is enabled
     */
    all_descs = old_handler->GetAllDescs(SnapshotNow);
    if (t_thrd.vacuum_cxt.vacuum_full_compact) {
        merged_descs = old_handler->GetDescsToBeMerged(SnapshotNow);

        /*
         * move desc tuple which no invalid data to the desc table of new dfs
         * table if <<COMPACT is enabled>>. This action must be done before
         * any data is inserted into the new one.
         */
        List* to_newrel_desc = GetDifference(all_descs, merged_descs, equal_dfsdesc);

        ListCell* lc = NULL;
        foreach (lc, to_newrel_desc)
            new_handler->Add((DFSDesc*)lfirst(lc), 1, GetCurrentCommandId(true), TABLE_INSERT_FROZEN);
    }

    return t_thrd.vacuum_cxt.vacuum_full_compact ? merged_descs : all_descs;
}

static void CopyOldDeltaToNewRel(Oid OIDOldHeap, Oid OIDNewHeap)
{
    Relation NewHeap, OldHeap;
    TupleDesc oldTupDesc;
    int natts;
    Datum* values = NULL;
    bool* isnull = NULL;

    TableScanDesc heapScan;
    HeapTuple tuple;

    /*
     * Open the relations we need.
     */
    NewHeap = heap_open(OIDNewHeap, ExclusiveLock);
    OldHeap = heap_open(OIDOldHeap, ExclusiveLock);

    Relation OldDeltaHeap = heap_open(OldHeap->rd_rel->reldeltarelid, ExclusiveLock);

    /* Preallocate values/isnull arrays */
    oldTupDesc = OldHeap->rd_att;
    natts = oldTupDesc->natts;
    values = (Datum*)palloc(natts * sizeof(Datum));
    isnull = (bool*)palloc(natts * sizeof(bool));

    DfsInsertInter* insert = NULL;
    insert = (DfsInsert*)CreateDfsInsert(NewHeap, false, OldHeap);
    insert->BeginBatchInsert(TUPLE_SORT);
    insert->RegisterInsertPendingFunc(InsertNewFileToDfsPending);

    heapScan = tableam_scan_begin(OldDeltaHeap, SnapshotNow, 0, (ScanKey)NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(heapScan, ForwardScanDirection)) != NULL) {
        tableam_tops_deform_tuple(tuple, oldTupDesc, values, isnull);
        insert->TupleInsert(values, isnull, TABLE_INSERT_FROZEN);
    }

    tableam_scan_end(heapScan);

    insert->SetEndFlag();
    insert->TupleInsert(NULL, NULL, TABLE_INSERT_FROZEN);
    DELETE_EX(insert);

    /* Clean up */
    pfree_ext(values);
    pfree_ext(isnull);

    heap_close(OldDeltaHeap, NoLock);
    heap_close(OldHeap, NoLock);
    heap_close(NewHeap, NoLock);
}

/*
 * copy the data of old dfs table to new dfs table
 */
static void DoCopyPaxFormatData(Relation oldRel, Relation newRel)
{
    /*
     * if no files to be merged, return directly
     */
    List* todo_descs = FindMergedDescs(oldRel, newRel);
    if (todo_descs == NULL)
        return;

    /*
     * save path for relation, and this path will be used in doPendingDfsDelete()
     */
    StringInfo store_path = getDfsStorePath(oldRel);
    MemoryContext oldcontext =
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    u_sess->catalog_cxt.vf_store_root = makeStringInfo();
    MemoryContextSwitchTo(oldcontext);

    appendStringInfo(u_sess->catalog_cxt.vf_store_root, "%s", store_path->data);

    /*
     * get connection object and will be used in doPendingDfsDelete()
     */
    DfsSrvOptions* dfsoptions = GetDfsSrvOptions(oldRel->rd_rel->reltablespace);
    dfs::DFSConnector* conn = dfs::createConnector(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), dfsoptions, oldRel->rd_rel->reltablespace);
    u_sess->catalog_cxt.delete_conn = conn;

    /*
     * init dfs insert object
     */
    DfsInsertInter* insert = NULL;

    insert = (DfsInsert*)CreateDfsInsert(newRel, false, oldRel);
    insert->BeginBatchInsert(BATCH_SORT);
    insert->RegisterInsertPendingFunc(InsertNewFileToDfsPending);

    StringInfo rootDir = getDfsStorePath(oldRel);
    ListCell* lc = NULL;
    List* splitList = NIL;
    foreach (lc, todo_descs) {
        DFSDesc* desc = (DFSDesc*)lfirst(lc);

        StringInfo filePath = makeStringInfo();
        appendStringInfo(filePath, "%s/%s", rootDir->data, desc->GetFileName());

        SplitInfo* split = InitFileSplit(filePath->data, NULL, desc->GetFileSize());
        splitList = lappend(splitList, split);

        InsertIntoPendingDfsDelete(desc->GetFileName(), true, newRel->rd_rel->relowner, (uint64)desc->GetFileSize());
    }

    DfsScanState* scan = dfs::reader::DFSBeginScan(oldRel, splitList, 0, NULL, SnapshotNow);

    /*
     * compact all files which contain invalid data.
     */
    VectorBatch* batch = NULL;
    do {
        CHECK_FOR_INTERRUPTS();

        batch = dfs::reader::DFSGetNextBatch(scan);
        if (BatchIsNull(batch)) {
            insert->SetEndFlag();
            insert->BatchInsert((VectorBatch*)NULL, TABLE_INSERT_FROZEN);
        } else {
            insert->BatchInsert(batch, TABLE_INSERT_FROZEN);
        }

        if (BatchIsNull(batch))
            break;

    } while (true);

    DELETE_EX(insert);

    dfs::reader::DFSEndScan(scan);
}

// now this function servers VACUUM FULL cstore tables
// excluding CLUSTER clause.
static void RebuildCStoreRelation(
    Relation oldHeap, Oid indexOid, int freeze_min_age, int freeze_table_age, bool verbose, AdaptMem* mem_info)
{
    Oid tableOid = RelationGetRelid(oldHeap);
    Oid tableSpace = oldHeap->rd_rel->reltablespace;
    Oid oidNewHeap;
    TransactionId frozenXid = InvalidTransactionId;
    Relation newRel, oldRel;
    bool swapToastByContent = false;
    bool is_shared = oldHeap->rd_rel->relisshared;
    Assert(!IsSystemRelation(oldHeap));

    heap_close(oldHeap, NoLock);

    oidNewHeap = make_new_heap(tableOid, tableSpace, ExclusiveLock);

    /* reopen relations */
    newRel = heap_open(oidNewHeap, AccessExclusiveLock);
    oldRel = heap_open(tableOid, ExclusiveLock);

    CopyCStoreData(
        oldHeap, newRel, freeze_min_age, freeze_table_age, verbose, &swapToastByContent, &frozenXid, mem_info);

    heap_close(newRel, NoLock);
    heap_close(oldRel, NoLock);

    // We must hold AccessExclusiveLock before finish_heap_swap
    // in order to block select statement until transaction commit
    // Because vacumm full have done lots of work by here, so we delay
    // dead lock check for vacuum full thread to avoid vacuum full failed
    //
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = true;
    LockRelationOid(tableOid, AccessExclusiveLock);

    /* swap relation files */
    finish_heap_swap(tableOid, oidNewHeap, false, swapToastByContent, false, frozenXid, InvalidMultiXactId, mem_info);

    /*
     * Report vacuum full stat to PgStatCollector.
     * We use -1 to identify Cstore table and let PgStatCollector set deadtuple to 0.
     * Also see comments of pgstat_report_vacuum in rebuildPartVacFull.
     */
    pgstat_report_vacuum(tableOid, InvalidOid, is_shared, -1);
}

/*
 * Brief        : Update the relation name.
 * Input        : relOid, the relation Oid.
 *                isParttion, whether or not the relation is a parttition table.
 *                relNewName, the new relation name.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
void updateRelationName(Oid relOid, bool isPartition, const char* relNewName)
{
    int catalogRelId = 0;
    int catalogIndex = 0;
    char* relName = NULL;

    if (!isPartition) {
        catalogRelId = RelationRelationId;
        catalogIndex = RELOID;
    } else {
        catalogRelId = PartitionRelationId;
        catalogIndex = PARTRELID;
    }

    Relation classRel = heap_open(catalogRelId, RowExclusiveLock);

    HeapTuple reltup = SearchSysCacheCopy1(catalogIndex, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(reltup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("Cache lookup failed for relation %u.", relOid)));
    }

    if (!isPartition) {
        relName = NameStr(((Form_pg_class)GETSTRUCT(reltup))->relname);
    } else {
        relName = NameStr(((Form_pg_partition)GETSTRUCT(reltup))->relname);
    }

    errno_t rc = 0;
    rc = strncpy_s(relName, NAMEDATALEN, relNewName, strlen(relNewName) + 1);
    securec_check(rc, "\0", "\0");
    relName[strlen(relNewName)] = '\0';

    if (!IsBootstrapProcessingMode()) {
        /* normal case, use a transactional update */
        simple_heap_update(classRel, &reltup->t_self, reltup);

        /* Keep catalog indexes current */
        CatalogUpdateIndexes(classRel, reltup);
    } else {
        /* While bootstrapping, we cannot UPDATE, so overwrite in-place */
        heap_inplace_update(classRel, reltup);
    }

    heap_freetuple(reltup);

    heap_close(classRel, RowExclusiveLock);

    /*
     * Make changes visible
     */
    CommandCounterIncrement();
}

#ifdef ENABLE_MULTIPLE_NODES
static bool IsNeedToTransfer(Oid relOid1, Oid relOid2)
{
    Assert(IS_PGXC_COORDINATOR);
    bool result = false;
    Relation rel1 = relation_open(relOid1, NoLock);
    Relation rel2 = relation_open(relOid2, NoLock);
    if (rel1->storage_type != rel2->storage_type) {
        result = true;
    } else {
        if (rel1->storage_type == SEGMENT_PAGE && rel1->rd_bucketoid != rel2->rd_bucketoid && 
            (!RELATION_HAS_BUCKET(rel1) || !RELATION_HAS_BUCKET(rel2))) {
            result = true;
        } else if (RELATION_HAS_BUCKET(rel1) && RELATION_HAS_BUCKET(rel2) &&
            rel1->rd_bucketmapsize != rel2->rd_bucketmapsize) {
            /*
             * 1.hashbucket transfer to hashbucket will be return
             * 2.hashbucket expand, new dn(rel1->rd_bucketmapsize=0)
             * */
            result = true;
        }
    }
    relation_close(rel2, NoLock);
    relation_close(rel1, NoLock);
    return result;
}

/*
 * Get exec nodes based on two relations.
 * For scale out: exclude the group members in relOid1(old table) from relOid2(tmp table).
 * For scale in: return NULL, since it should get exec nodes based on relOid2.
 * For resize/scale up: return NULL, since it should get exec nodes based on relOid2.
 */
static int switch_relfilenode_execnode(Oid relOid1, Oid relOid2, bool isbucket, RedisSwitchNode* rsn)
{
    List* nodeNameList1 = NIL;
    List* nodeNameList2 = NIL;
    List* sameList = NIL;
    List* diffList = NIL;
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    Oid* members1 = NULL;
    Oid* members2 = NULL;

    /* If wang to transfer table type. */
    if (IsNeedToTransfer(relOid1, relOid2)) {
        rsn->nodes = NULL;
        rsn->type = REDIS_SWITCH_EXEC_TRANSFER;
        return 1;
    }

    /* no bucket or no group change then just do normal switch */
    if (!isbucket || (get_pgxc_class_groupoid(relOid1) == get_pgxc_class_groupoid(relOid2))) {
        rsn->nodes = NULL;
        rsn->type = REDIS_SWITCH_EXEC_NORMAL;
        return 1;
    }

    /* Get group memebers by relation oid */
    int nmembers1 = get_pgxc_classnodes(relOid1, &members1);
    int nmembers2 = get_pgxc_classnodes(relOid2, &members2);

    /*
     * Same datanode may have different oid in different groups(because of the primary node may changed when
     * create a group). So we need to get datanode name to do the exclude operation.
     */
    for (int i = 0; i < nmembers1; i++) {
        nodeNameList1 = lappend(nodeNameList1, get_pgxc_nodename(members1[i], NULL));
    }

    for (int i = 0; i < nmembers2; i++) {
        nodeNameList2 = lappend(nodeNameList2, get_pgxc_nodename(members2[i], NULL));
    }

    /* make sure list2 is the longer one */
    if (nmembers1 > nmembers2) {
        List* tmp = nodeNameList1;
        nodeNameList1 = nodeNameList2;
        nodeNameList2 = tmp;
    }
    /* compare the members, nodeNameList2 must be the longger one */
    foreach (cell2, nodeNameList2) {
        char* nodeName2 = (char*)lfirst(cell2);
        bool isSame = false;
        int nodeId;

        foreach (cell1, nodeNameList1) {
            char* nodeName1 = (char*)lfirst(cell1);
            if (strcmp(nodeName1, nodeName2) == 0) {
                isSame = true;
                break;
            }
        }
        nodeId = PGXCNodeGetNodeIdFromName(nodeName2, PGXC_NODE_DATANODE);
        if (nodeId < 0) {
            ereport(ERROR, (errcode(ERRCODE_NODE_ID_MISSMATCH), errmsg("invalid nodeId: %s(%d)", nodeName2, nodeId)));
        }

        /* Add to the final node list */
        if (isSame == true) {
            sameList = lappend_int(sameList, nodeId);
        } else {
            diffList = lappend_int(diffList, nodeId);
        }
    }

    /* No intersection set bwtween these two relations -> resize */
    if (sameList == NULL) {
        rsn->nodes = NULL;
        rsn->type = REDIS_SWITCH_EXEC_NORMAL;
        return 1;
    }

    if (list_length(sameList) * 2 + list_length(diffList) != nmembers1 + nmembers2) {
        ereport(ERROR,
            (errcode(ERRCODE_NODE_ID_MISSMATCH),
                errmsg("Invalid node group member found while doing table switch (%u,%u)", relOid1, relOid2)));
    }
    /* Same node members just do normal switch */
    if (diffList == NULL) {
        rsn->nodes = NULL;
        rsn->type = REDIS_SWITCH_EXEC_NORMAL;
        return 1;
    }

    /* Clean up. */
    list_free_deep(nodeNameList1);
    list_free_deep(nodeNameList2);
    pfree_ext(members1);
    pfree_ext(members2);

    /* Must be scale in case, just set nodes to NULL */
    if (nmembers1 > nmembers2) {
        rsn->nodes = NULL;
        rsn->type = REDIS_SWITCH_EXEC_NORMAL;
        return 1;
    }

    /* Scale out, construct the exec nodes */
    /* first add switch node */
    ExecNodes* execNodes = makeNode(ExecNodes);
    execNodes->nodeList = sameList;
    Distribution* distribution = ng_convert_to_distribution(execNodes->nodeList);
    ng_set_distribution(&execNodes->distribution, distribution);
    rsn->nodes = execNodes;
    rsn->type = REDIS_SWITCH_EXEC_DROP;
    rsn++;
    /* add drop bucket node */
    execNodes = makeNode(ExecNodes);
    execNodes->nodeList = diffList;
    distribution = ng_convert_to_distribution(execNodes->nodeList);
    ng_set_distribution(&execNodes->distribution, distribution);
    rsn->nodes = execNodes;
    rsn->type = REDIS_SWITCH_EXEC_NORMAL;

    return 2;
}
#endif

static bool PartitionIndexGetIndIsUsable (Oid indexOid)
{
    Datum datum;
    bool  isnull = false;

    Relation pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);
    HeapTuple reltup = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(indexOid));
    if (!HeapTupleIsValid(reltup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", indexOid)));
    }
    datum = heap_getattr(reltup, Anum_pg_partition_indisusable, RelationGetDescr(pg_partition), &isnull);
    Assert(!isnull);

    heap_freetuple(reltup);
    heap_close(pg_partition, RowExclusiveLock);
    return BoolGetDatum(datum);
}

void HbktModifyPartitionIndexRelNode(Relation indexRel, DataTransferType transferType, Oid bucketOid)
{
    List* partitions = NIL;
    partitions = GetIndexPartitionListByOrder(indexRel, ExclusiveLock);
    ListCell* cell = NULL;
    foreach(cell, partitions) {
        Partition indexPartition = (Partition)lfirst(cell);
        if (!PartitionIndexGetIndIsUsable(indexPartition->pd_id)) {
            HbktModifyPartIndexRelnode(indexRel, indexPartition, transferType, bucketOid);
        }
    }
    releasePartitionList(indexRel, &partitions, ExclusiveLock);
}

void HbktTransferDeletePgAttributeTablebucketid(Oid relid, bool cboffIndex)
{
    Relation attrel;
    SysScanDesc scan;
    ScanKeyData key[2];
    HeapTuple atttup;
    int attrnum = 0;
    int i = 0;

    if (cboffIndex)
        return;

    Relation rel1 = relation_open(relid, NoLock);

    for (i = 0; i < rel1->rd_att->natts; i++) {
        char *attr_name = pstrdup(NameStr(rel1->rd_att->attrs[i]->attname));
        if (strcmp(attr_name, "tablebucketid") == 0) {
            attrnum = rel1->rd_att->attrs[i]->attnum;
            break;
        }
    }
    if (i == rel1->rd_att->natts) {
        Assert(0);
    }

    relation_close(rel1, NoLock);

    /* Grab an appropriate lock on the pg_attribute relation */
    attrel = heap_open(AttributeRelationId, RowExclusiveLock);
    /* Use the index to scan only system attributes of the target relation */
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_attribute_attnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attrnum));
    scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, NULL, 2, key);

    /* Delete all the matching tuples */
    while ((atttup = systable_getnext(scan)) != NULL)
        simple_heap_delete(attrel, &atttup->t_self);

    /* Clean up after the scan */
    systable_endscan(scan);
    heap_close(attrel, RowExclusiveLock);
}

void HbktTransferModifyPgAttributeTablebucketid(Oid indexOid)
{
    Relation rel1 = relation_open(indexOid, NoLock);
    FormData_pg_attribute attr = {0,
    {"tablebucketid"},
    INT2OID,
    0,
    sizeof(int2),
    0,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

    FormData_pg_attribute attStruct;
    errno_t rc = memcpy_s(&attStruct, sizeof(FormData_pg_attribute), (char*)&attr,
        sizeof(FormData_pg_attribute));
    securec_check(rc, "\0", "\0");
    attStruct.attrelid = indexOid;
    attStruct.attnum = rel1->rd_att->natts + 1;

    relation_close(rel1, NoLock);

    Relation attrel;
    attrel = heap_open(AttributeRelationId, RowExclusiveLock);
    CatalogIndexState indstate;
    indstate = CatalogOpenIndexes(attrel);

    InsertPgAttributeTuple(attrel, &attStruct, indstate);

    CatalogCloseIndexes(indstate);
    heap_close(attrel, RowExclusiveLock);
}

DataTransferType GetTransferType(Relation rel1, Relation rel2)
{
    if (rel1->storage_type != SEGMENT_PAGE) {
        if (RELATION_HAS_BUCKET(rel2)) {
            return NORMAL_TO_HASHBUCKET;
        } else {
            return NORMAL_TO_SEGMENT;
        }
    } else {
        if (!RELATION_HAS_BUCKET(rel1)) {
            if (rel2->storage_type != SEGMENT_PAGE) {
                return SEGMENT_TO_NORMAL;
            } else {
                return SEGMENT_TO_HASHBUCKET;
            }
        } else {
            if (rel2->storage_type != SEGMENT_PAGE) {
                return HASHBUCKET_TO_NORMAL;
            } else if (RELATION_HAS_BUCKET(rel2)) {
                return HASHBUCKET_TO_HASHBUCKET;
            } else {
                return HASHBUCKET_TO_SEGMENT;
            }
        }
    }

}

void HbktTransferModifyPgAttribute(DataTransferType transferType, Oid attributeOid, bool cboffIndex)
{
    Relation rel1 = relation_open(attributeOid, NoLock);
    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET) {
        HbktTransferModifyPgAttributeTablebucketid(attributeOid);
    } else {
        HbktTransferDeletePgAttributeTablebucketid(attributeOid, cboffIndex);
    }
    relation_close(rel1, NoLock);
}

int IndexGetindisusable(Oid indexOid)
{
    HeapTuple       reltup1;
    Relation relRelation;
    Datum datum;
    bool  isnull = false;

    relRelation = heap_open(IndexRelationId, RowExclusiveLock);
    reltup1 = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexOid));
    if (!HeapTupleIsValid(reltup1))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("cache lookup failed for relation %u", indexOid)));

    datum = heap_getattr(reltup1, Anum_pg_index_indisusable, GetDefaultPgIndexDesc(), &isnull);
    Assert(!isnull);

    /* Clean up. */
    heap_freetuple(reltup1);
    heap_close(relRelation, RowExclusiveLock);
    return BoolGetDatum(datum);

}

void HbktTransferModifyPgClass(DataTransferType transferType, Oid indexOid, bool cboffIndex,
    Oid rel2bucketid)
{
    Relation indexrel = relation_open(indexOid, AccessExclusiveLock);
    Assert(PointerIsValid(indexrel));

    HeapTuple reltup1;
    Relation relRelation = heap_open(RelationRelationId, RowExclusiveLock);
    reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(indexOid));
    if (!HeapTupleIsValid(reltup1))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("cache lookup failed for relation %u", indexOid)));

    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET ||
        transferType == HASHBUCKET_TO_NORMAL || transferType == HASHBUCKET_TO_SEGMENT) {
        reltup1 = HbktTransferModifyRelationRelnatts(reltup1, transferType, cboffIndex);
    }

    bool indexIsUse = IndexGetindisusable(indexOid);
    reltup1 = HbktTransferModifyPgClassRelbucket(reltup1, transferType, rel2bucketid, indexIsUse);
    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET ||
        transferType == HASHBUCKET_TO_NORMAL || transferType == HASHBUCKET_TO_SEGMENT) {
        reltup1 = HbktTtransferModifyRelationReloptions(reltup1, transferType, indexIsUse);
    }
    //relfilenode
    if (RelationIsPartitioned(indexrel)) {
        HbktModifyPartitionIndexRelNode(indexrel, transferType, rel2bucketid);
    } else {
        reltup1 = HbktModifyRelationRelfilenode(reltup1, transferType, indexrel, indexIsUse, rel2bucketid);
    }
    relation_close(indexrel, NoLock);
    simple_heap_update(relRelation, &reltup1->t_self, reltup1);
    CatalogUpdateIndexes(relRelation, reltup1);

    heap_freetuple(reltup1);
    heap_close(relRelation, RowExclusiveLock);

}

void HbktTransferModifyPgIndex(DataTransferType transferType, Oid indexOid, bool *cboffIndex)
{
    HeapTuple       reltup1;
    Relation relRelation;

    relRelation = heap_open(IndexRelationId, RowExclusiveLock);
    reltup1 = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexOid));
    if (!HeapTupleIsValid(reltup1))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("cache lookup failed for relation %u", indexOid)));

    HbktTransferModifyPgIndexNattsAndIndkey(transferType, &reltup1, cboffIndex);
    /* update catalog */
    simple_heap_update(relRelation, &reltup1->t_self, reltup1);
    CatalogUpdateIndexes(relRelation, reltup1);

    /* Clean up. */
    heap_freetuple(reltup1);
    heap_close(relRelation, RowExclusiveLock);

}

ArrayType* GetNewConincludingArray(DataTransferType transferType, HeapTuple tuple)
{
    ArrayType* conincludingArray = NULL;
    Datum* conincluding = NULL;
    int conNum = 0;
    ArrayType* arr = NULL;
    int16* attnums = NULL;
    int numkeys;
    int j = 0;

    bool isnull = true;
    Datum val = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_conincluding, &isnull);
    if (isnull) {
        numkeys = 0;
    } else {
        arr = DatumGetArrayTypeP(val);
        numkeys = ARR_DIMS(arr)[0];
        if (ARR_NDIM(arr) != 1 || numkeys < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID)
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("conkey is not a 1-D smallint array")));
        attnums = (int16*)ARR_DATA_PTR(arr);
    }

    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET) {
        conNum = numkeys + 1;
        conincluding = (Datum*)palloc(conNum * sizeof(Datum));
        if (!isnull) {
            for (int i = 0; i < numkeys; i++) {
                conincluding[j++] = UInt16GetDatum(attnums[i]);
            }
        }
        conincluding[j] = UInt16GetDatum(BucketIdAttributeNumber);
        conincludingArray = construct_array(conincluding, conNum, INT2OID, 2, true, 's');
    } else {
        if (numkeys > 1) {
            conNum = numkeys - 1;
            conincluding = (Datum*)palloc(conNum * sizeof(Datum));
            for (int i = 0; i < numkeys; i++) {
                if (attnums[i] == BucketIdAttributeNumber) {
                    continue;
                } else {
                    conincluding[j++] = UInt16GetDatum(attnums[i]);
                }
            }
            conincludingArray = construct_array(conincluding, conNum, INT2OID, 2, true, 's');
        }
    }
    pfree_ext(conincluding);
    return conincludingArray;
}
void HbktTransferModifyPgConstraint(DataTransferType transferType, Oid conindid, bool cboffIndex)
{
    Oid constraintId = get_index_constraint(conindid);
    if (!OidIsValid(constraintId)) {
        return;
    }

    Relation conDesc = NULL;
    HeapTuple tuple = NULL;
    ArrayType* conincludingArray = NULL;

    conDesc = heap_open(ConstraintRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(CONSTROID, ObjectIdGetDatum(constraintId));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for constraint %u", constraintId)));
    }
    if ((transferType == HASHBUCKET_TO_NORMAL || transferType == HASHBUCKET_TO_SEGMENT) && cboffIndex) {
        goto free;
    }
    conincludingArray = GetNewConincludingArray(transferType, tuple);

    bool nulls[Natts_pg_constraint];
    Datum values[Natts_pg_constraint];
    bool replaces[Natts_pg_constraint];
    errno_t rc;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    if (conincludingArray) {
        values[Anum_pg_constraint_conincluding - 1] = PointerGetDatum(conincludingArray);
    } else {
        nulls[Anum_pg_constraint_conincluding - 1] = true;
    }
    replaces[Anum_pg_constraint_conincluding - 1] = true;
    tuple = heap_modify_tuple(tuple, RelationGetDescr(conDesc), values, nulls, replaces);
    simple_heap_update(conDesc, &tuple->t_self, tuple);
    CatalogUpdateIndexes(conDesc, tuple);

free:
    heap_freetuple_ext(tuple);
    heap_close(conDesc, RowExclusiveLock);
}

void HbktTransferModifyAttr(Relation rel1, Relation rel2)
{
    DataTransferType transferType = NORMAL_TO_SEGMENT;
    transferType = GetTransferType(rel1, rel2);
    List* indicesList = RelationGetIndexList(rel1, true);
    ListCell* cell = NULL;
    Oid indexOid;
    bool cboffIndex = false;
    foreach (cell, indicesList) {
        indexOid = lfirst_oid(cell);
        switch (transferType) { 
            case NORMAL_TO_HASHBUCKET:
            case SEGMENT_TO_HASHBUCKET:
            case HASHBUCKET_TO_NORMAL:
            case HASHBUCKET_TO_SEGMENT:
                HbktTransferModifyPgIndex(transferType, indexOid, &cboffIndex);
                HbktTransferModifyPgAttribute(transferType, indexOid, cboffIndex);
                HbktTransferModifyPgConstraint(transferType, indexOid, cboffIndex);
                /* fall through */
            case NORMAL_TO_SEGMENT:
            case SEGMENT_TO_NORMAL:
            case HASHBUCKET_TO_HASHBUCKET:
                HbktTransferModifyPgClass(transferType, indexOid, cboffIndex, rel2->rd_bucketoid);
                break;
            default:
                Assert(0);
                break;
        }
    }

    list_free_ext(indicesList);

}

void HbktExpandModifyRelIndexRelNode(Relation indexRel, Oid bucketOid)
{
    HeapTuple reltup;
    if (IndexGetindisusable(indexRel->rd_id)) {
        return;
    }
    Relation pg_class = heap_open(RelationRelationId, RowExclusiveLock);
    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(indexRel->rd_id));
    if (!HeapTupleIsValid(reltup)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", indexRel->rd_id)));
    }
    reltup = HbktModifyRelationRelfilenode(reltup, TRANSFER_IS_INVALID, indexRel, false, bucketOid);
    simple_heap_update(pg_class, &reltup->t_self, reltup);
    CatalogUpdateIndexes(pg_class, reltup);
    heap_freetuple(reltup);
    heap_close(pg_class, RowExclusiveLock);
}

void HbktExpandModifyUnusableIndexRelNode(Relation rel, Oid bucketOid)
{
    List* indicesList = RelationGetIndexList(rel, true);
    ListCell* cell = NULL;
    Oid indexOid;
    foreach(cell, indicesList) {
        indexOid = lfirst_oid(cell);
        if (!IndexGetindisusable(indexOid)) {
            Relation pg_class = heap_open(RelationRelationId, RowExclusiveLock);
            HeapTuple indexRel = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(indexOid));
            if (!HeapTupleIsValid(indexRel)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", indexOid)));
            }
            indexRel = HbktTransferModifyPgClassRelbucket(indexRel, TRANSFER_IS_INVALID, bucketOid, false);
            simple_heap_update(pg_class, &indexRel->t_self, indexRel);
            CatalogUpdateIndexes(pg_class, indexRel);
            heap_freetuple(indexRel);
            heap_close(pg_class, RowExclusiveLock);
        }
        Relation indexRel = relation_open(indexOid, AccessExclusiveLock);
        if (!RelationIsCrossBucketIndex(indexRel)) {
            if (RelationIsPartitioned(indexRel)) {
                HbktModifyPartitionIndexRelNode(indexRel, TRANSFER_IS_INVALID, bucketOid);
            } else {
                CommandCounterIncrement();
                HbktExpandModifyRelIndexRelNode(indexRel, bucketOid);
            }
        }
        relation_close(indexRel, NoLock);
    }
    list_free_ext(indicesList);
}

/*
 * @Description : Exchange relfilenode between table relOId1 and relOid2.
 * @in         	: relOId1, the relation Oid.
 * @in          : relOId2, the relation Oid.
 * @out         : None
 * @return      : 1 if success
 */
static int64 execute_relfilenode_swap(Oid relOid1, Oid relOid2, uint8 needSwitch)
{
    char* relname1 = get_rel_name(relOid1);
    char* relname2 = get_rel_name(relOid2);
    Relation rel = NULL;

    ereport(LOG,
        (errcode(ERRCODE_LOG),
            errmsg("swap relfilenode: %s(%u)<->%s(%u) on node %s",
                relname1,
                relOid1,
                relname2,
                relOid2,
                g_instance.attr.attr_common.PGXCNodeName)));

    rel = try_relation_open(relOid1, NoLock);

    if (rel == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("could not open relation %s(%u) on node %s.",
                    relname1,
                    relOid1,
                    g_instance.attr.attr_common.PGXCNodeName)));

    if (RelationIsPartitioned(rel)) {
        /* Partition table */
        partition_relfilenode_swap(relOid1, relOid2, needSwitch);
    } else {
        TransactionId relfrozenxid;
        MultiXactId relMultiXid;
        getRelationRelxids(rel, &relfrozenxid, &relMultiXid);
        /* Ordinary table */
        relfilenode_swap(relOid1, relOid2, needSwitch, relfrozenxid, relMultiXid);
    }

    /* swap bucket info while doing data redis */
    if (needSwitch) {
        CommandCounterIncrement();
        relation_swap_bucket(relOid1, relOid2, needSwitch & TRANSFER_SWITCH_FLAG);
        Relation rel1 = relation_open(relOid1, NoLock);
        Relation rel2 = relation_open(relOid2, NoLock);
        if (RelationIsRelation(rel1)) {
            if (needSwitch & TRANSFER_SWITCH_FLAG) {
                HbktTransferModifyAttr(rel1, rel2);
            } else {
                HbktExpandModifyUnusableIndexRelNode(rel1, rel2->rd_bucketoid);
            }
        }
        relation_close(rel2, NoLock);
        relation_close(rel1, NoLock);
    }

    relation_close(rel, NoLock);
    return 1;
}

#ifdef ENABLE_MULTIPLE_NODES
void RouteSwitchQuery2CN(char* relName1, char* relName2)
{
    ParallelFunctionState* state = NULL;
    StringInfoData buf;
    ExecNodes* exec_nodes = NULL;
    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->nodeList = GetAllCoordNodes();
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.gs_switch_relfilenode('%s','%s', %d)",
        relName1, relName2, REDIS_SWITCH_EXEC_TRANSFER);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, false, EXEC_ON_COORDS, true, true);
    FreeParallelFunctionState(state);
    pfree_ext(buf.data);
}
#endif

/*
 * @Description : Parallel exchange relfilenode.
 * @in         	: relOId1, the relation Oid.
 * @in          : relOId2, the relation Oid.
 * @out         : None
 * @return      : DN counts, how many datanodes executed the exchange function successfully.
 */
Datum pg_switch_relfilenode_name(PG_FUNCTION_ARGS)
{

#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported proc in single node mode.")));

    PG_RETURN_NULL();
#else
    Oid relOid1 = PG_GETARG_OID(0);
    Oid relOid2 = PG_GETARG_OID(1);
    int32 switchtype = PG_GETARG_INT32(2);
    char* relName1 = NULL;
    char* relName2 = NULL;
    bool rel1Isbucket = false;
    bool ispart = false;
    bool swap_bucket = false;
    int64 size = 0;

    if (!u_sess->attr.attr_sql.enable_cluster_resize)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("gs_switch_relfilenode can be only used by redistribution tool.")));

    Relation rel1 = relation_open(relOid1, NoLock);
    Relation rel2 = relation_open(relOid2, NoLock);

    ispart = RELATION_IS_PARTITIONED(rel1);
    rel1Isbucket = RELATION_HAS_BUCKET(rel1);

    relName1 =
        quote_qualified_identifier(get_namespace_name(rel1->rd_rel->relnamespace), RelationGetRelationName(rel1));
    relName1 = repairObjectName(relName1);

    relName2 =
        quote_qualified_identifier(get_namespace_name(rel2->rd_rel->relnamespace), RelationGetRelationName(rel2));
    relName2 = repairObjectName(relName2);

    swap_bucket = rel1Isbucket && (rel1->rd_bucketoid != rel2->rd_bucketoid);
    relation_close(rel2, NoLock);
    relation_close(rel1, NoLock);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        ExecNodes* exec_nodes = NULL;
        char sqlStr[SQL_STR_LEN];
        RedisSwitchNode rsn[2];
        int64 ret = 0;
        int cnt = switch_relfilenode_execnode(relOid1, relOid2, rel1Isbucket, rsn);
        Assert(cnt <= MAX_REDIS_SWITCH_EXEC_CMD);
        for (int i = 0; i < cnt; i++) {
            errno_t rc = snprintf_s(sqlStr,
                SQL_STR_LEN,
                SQL_STR_LEN - 1,
                "SELECT pg_catalog.gs_switch_relfilenode('%s','%s',%d)",
                relName1,
                relName2,
                rsn[i].type);
            securec_check_ss_c(rc, "\0", "\0");

            if (rsn[i].nodes == NULL) {
                if (rsn->type == REDIS_SWITCH_EXEC_TRANSFER) {
                    RouteSwitchQuery2CN(relName1, relName2);
                } 
                exec_nodes = RelidGetExecNodes(relOid2, false);
            } else {
                exec_nodes = rsn[i].nodes;
            }
            ret += DatumGetInt64(pgxc_parallel_execution(sqlStr, exec_nodes));
        }
        if (rsn->type != REDIS_SWITCH_EXEC_TRANSFER) {
            PG_RETURN_INT64(ret);
        }
        switchtype = REDIS_SWITCH_EXEC_TRANSFER;
    }
#endif
    /* not run on cn */
    if (switchtype != REDIS_SWITCH_EXEC_TRANSFER && IS_PGXC_COORDINATOR) {
        Assert(false);
    }

    uint8 needSwitch = swap_bucket ? BUCKET_SWITCH_FLAG : 0;

    switch (switchtype) {
        case REDIS_SWITCH_EXEC_NORMAL:
            size = execute_relfilenode_swap(relOid1, relOid2, needSwitch);
            break;
        case REDIS_SWITCH_EXEC_DROP:
            size = execute_drop_bucketlist(relOid1, relOid2, ispart);
            break;
        case REDIS_SWITCH_EXEC_MOVE:
            size = execute_move_bucketlist(relOid1, relOid2, ispart);
            break;
        case REDIS_SWITCH_EXEC_TRANSFER:
            size = execute_relfilenode_swap(relOid1, relOid2, needSwitch | TRANSFER_SWITCH_FLAG);
            break;
        default:
            Assert(false);
    }

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
#endif
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @Description : Sending query using parallel execution framework.
 * @in          : query, the query string.
 * @in          : exec_nodes, the nodes of relation.
 * @out         : None
 * @return      : DN counts, how many datanodes executed the query successfully.
 */
static Datum pgxc_parallel_execution(const char* query, ExecNodes* exec_nodes)
{
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 size;

    initStringInfo(&buf);
    appendStringInfoString(&buf, query);

    state = RemoteFunctionResultHandler(buf.data, exec_nodes, StrategyFuncSum, false);

    size = state->result;
    FreeParallelFunctionState(state);

    PG_RETURN_INT64(size);
}
#endif

/*
 * @Description : Exchange relfilenodes of two partitions
 * @in         	: partitionOid1, the partition oid.
 * @in          : partitionOid2, the partition oid.
 * @in          : swap toast table by content but links.
 * @in          : frozenXid, frozen transaction Id
 * @out         : None
 * @return      : None
 */
static void swap_partition_relfilenode(
    Oid partitionOid1, Oid partitionOid2, bool swapToastByContent, TransactionId frozenXid,
    MultiXactId multiXid, Oid* mappedTables)
{
    Relation relRelation = NULL;
    HeapTuple reltup1 = NULL;
    HeapTuple reltup2 = NULL;
    HeapTuple ntup = NULL;
    Form_pg_partition relform1 = NULL;
    Form_pg_partition relform2 = NULL;  // pg_partition relform
    Oid relfilenode1 = InvalidOid;
    Oid relfilenode2 = InvalidOid;
    Oid swaptemp = InvalidOid;
    CatalogIndexState indstate1 = NULL;
    CatalogIndexState indstate2 = NULL;

    /* We need writable copies of both pg_class tuples. */
    relRelation = heap_open(PartitionRelationId, RowExclusiveLock);
    reltup1 = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partitionOid1));
    if (!HeapTupleIsValid(reltup1)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", partitionOid1)));
    }
    relform1 = (Form_pg_partition)GETSTRUCT(reltup1);

    reltup2 = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partitionOid2));
    if (!HeapTupleIsValid(reltup2)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", partitionOid2)));
    }
    relform2 = (Form_pg_partition)GETSTRUCT(reltup2);

    relfilenode1 = relform1->relfilenode;
    relfilenode2 = relform2->relfilenode;

    if (OidIsValid(relfilenode1) && OidIsValid(relfilenode2)) {
        swaptemp = relform1->relfilenode;
        relform1->relfilenode = relform2->relfilenode;
        relform2->relfilenode = swaptemp;

        swaptemp = relform1->reltablespace;
        relform1->reltablespace = relform2->reltablespace;
        relform2->reltablespace = swaptemp;

        /* Also swap toast/cudesc/delta links, if we're swapping by links */
        if (!swapToastByContent) {
            swaptemp = relform1->reltoastrelid;
            relform1->reltoastrelid = relform2->reltoastrelid;
            relform2->reltoastrelid = swaptemp;
        }

        // Any way, we should swap cudesc,delta by links
        //
        swaptemp = relform1->relcudescrelid;
        relform1->relcudescrelid = relform2->relcudescrelid;
        relform2->relcudescrelid = swaptemp;

        swaptemp = relform1->reldeltarelid;
        relform1->reldeltarelid = relform2->reldeltarelid;
        relform2->reldeltarelid = swaptemp;
    }

    /* set rel1's frozen Xid */
    if (relform1->parttype != PART_OBJ_TYPE_INDEX_PARTITION) {
        Datum values[Natts_pg_partition];
        bool nulls[Natts_pg_partition];
        bool replaces[Natts_pg_partition];
        errno_t rc;
        HeapTuple tmp;

        relform1->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
        values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(frozenXid);

#ifndef ENABLE_MULTIPLE_NODES
        replaces[Anum_pg_partition_relminmxid - 1] = true;
        values[Anum_pg_partition_relminmxid - 1] = TransactionIdGetDatum(multiXid);
#endif

        ntup = (HeapTuple) tableam_tops_modify_tuple(reltup1, RelationGetDescr(relRelation), values, nulls, replaces);

        relform1 = (Form_pg_partition)GETSTRUCT(ntup);

        tmp = ntup;
        ntup = reltup1;
        reltup1 = tmp;
    }

    /* swap size statistics too, since new rel has freshly-updated stats */
    float8 swap_pages;
    float4 swap_tuples;
    int4 swap_allvisible;

    swap_pages = relform1->relpages;
    relform1->relpages = relform2->relpages;
    relform2->relpages = swap_pages;

    swap_tuples = relform1->reltuples;
    relform1->reltuples = relform2->reltuples;
    relform2->reltuples = swap_tuples;

    swap_allvisible = relform1->relallvisible;
    relform1->relallvisible = relform2->relallvisible;
    relform2->relallvisible = swap_allvisible;

    /*
     * Update the tuples in pg_class and pg_partiton
     */
    simple_heap_update(relRelation, &reltup1->t_self, reltup1);
    simple_heap_update(relRelation, &reltup2->t_self, reltup2);

    /* Keep system catalogs current */
    indstate1 = CatalogOpenIndexes(relRelation);
    indstate2 = CatalogOpenIndexes(relRelation);
    CatalogIndexInsert(indstate1, reltup1);
    CatalogIndexInsert(indstate2, reltup2);
    CatalogCloseIndexes(indstate1);
    CatalogCloseIndexes(indstate2);

    /*
     * If we have delta tables or CUDesc tables associated with the relations being swapped,
     * deal with them too
     * */
    SwapCStoreTables(relform1->relcudescrelid, relform2->relcudescrelid, InvalidOid, InvalidOid);
    SwapCStoreTables(relform1->reldeltarelid, relform2->reldeltarelid, InvalidOid, InvalidOid);

    /* data redistribution for DFS table. */
    swap_relation_names(relform1->relcudescrelid, relform2->relcudescrelid);
    swap_relation_names(relform1->reldeltarelid, relform2->reldeltarelid);
    /*
     * If we're swapping two toast tables by content, do the same for their
     * indexes.
     */
    if (swapToastByContent && relform1->reltoastidxid && relform2->reltoastidxid)
        swap_relation_files(relform1->reltoastidxid,
            relform2->reltoastidxid,
            false,
            swapToastByContent,
            InvalidTransactionId,
            InvalidMultiXactId,
            mappedTables);

    /* Clean up. */
    if (ntup)
        heap_freetuple(ntup);
    heap_freetuple(reltup1);
    heap_freetuple(reltup2);

    heap_close(relRelation, RowExclusiveLock);

    /*
     * Close both relcache entries' smgr links.
     */
    PartitionCloseSmgrByOid(partitionOid1);
    RelationCloseSmgrByOid(partitionOid2);
}

/*
 * For index partition, order by heap partition oid.
 */
List* GetIndexPartitionListByOrder(Relation indexRelation, LOCKMODE lockmode)
{
    Oid relation_oid = IndexGetRelation(indexRelation->rd_id, false);
    Relation rel = relation_open(relation_oid, NoLock);
    List* relation_oid_list = relationGetPartitionOidList(rel);
    List* old_partitions = indexGetPartitionList(indexRelation, lockmode);
    List* indexPartitionList = NIL;
    ListCell* cell = NULL;

    foreach (cell, relation_oid_list) {
        Oid relid = lfirst_oid(cell);
        ListCell* parCell = NULL;
        bool found = false;
        foreach (parCell, old_partitions) {
            Partition indexPartition = (Partition)lfirst(parCell);
            if (relid == indexPartition->pd_part->indextblid) {
                indexPartitionList = lappend(indexPartitionList, indexPartition);
                found = true;
                break;
            }
        }
        if (!found) {
            indexPartitionList = lappend(indexPartitionList, NULL);
        }
    }
    list_free_ext(relation_oid_list);
    list_free_ext(old_partitions);
    relation_close(rel, NoLock);
    return indexPartitionList;
}

static void update_part_index(const List* partidxlist, Oid newidxtblid) 
{
    Relation partition_relation;
    HeapTuple part_tuple;
    Form_pg_partition part_form;
    ListCell* idx_cell = NULL;

    partition_relation = relation_open(PartitionRelationId, RowExclusiveLock);
    foreach(idx_cell, partidxlist) {
        Oid idx_oid = lfirst_oid(idx_cell);
        part_tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(idx_oid));
        if (!HeapTupleIsValid(part_tuple)) {
            continue;
        }
        part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
        part_form->indextblid = newidxtblid;
        simple_heap_update(partition_relation, &(part_tuple->t_self), part_tuple);
        CatalogUpdateIndexes(partition_relation, part_tuple);
        tableam_tops_free_tuple(part_tuple);
    }
    relation_close(partition_relation, RowExclusiveLock);
}

/*
 * exchange relname,parentid,reltablespace for each partition.
 */
static void swap_partition_info(Oid partOid1, Oid partOid2) 
{
    Relation partitionRelation;
    HeapTuple partTuple1;
    HeapTuple partTuple2;
    Form_pg_partition partForm1;
    Form_pg_partition partForm2;
    Oid parentOid1;
    Oid parentOid2;
    NameData tmpNameData;
    if (!OidIsValid(partOid1) || !OidIsValid(partOid2)) {
        return;
    }
    partitionRelation = relation_open(PartitionRelationId, RowExclusiveLock);
    partTuple1 = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partOid1));
    partTuple2 = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partOid2));
    if (!HeapTupleIsValid(partTuple1) || !HeapTupleIsValid(partTuple2)) {
        return;
    }
    partForm1 = (Form_pg_partition)GETSTRUCT(partTuple1);
    partForm2 = (Form_pg_partition)GETSTRUCT(partTuple2);

    namestrcpy(&tmpNameData, partForm1->relname.data);
    namestrcpy(&(partForm1->relname), partForm2->relname.data);
    namestrcpy(&(partForm2->relname), tmpNameData.data);

    parentOid1 = partForm1->parentid;
    parentOid2 = partForm2->parentid;
    partForm1->parentid = parentOid2;
    partForm2->parentid = parentOid1;

    simple_heap_update(partitionRelation, &(partTuple1->t_self), partTuple1);
    simple_heap_update(partitionRelation, &(partTuple2->t_self), partTuple2);

    CatalogUpdateIndexes(partitionRelation, partTuple1);
    CatalogUpdateIndexes(partitionRelation, partTuple2);

    tableam_tops_free_tuple(partTuple1);
    tableam_tops_free_tuple(partTuple2);

    relation_close(partitionRelation, RowExclusiveLock);
    
    CacheInvalidateRelcacheByRelid(parentOid1);
    CacheInvalidateRelcacheByRelid(parentOid2);
}

static void HbktModifyPartIndexRelnode(Relation indexRel, Partition indexPart, DataTransferType transferType,
    Oid bucketOid)
{
    errno_t rc;
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    bool replaces[Natts_pg_partition];
    Oid newrelfilenode;
    int4 bucketNode;

    bool isBucket = transferType == TRANSFER_IS_INVALID ? true :
        (transferType != HASHBUCKET_TO_HASHBUCKET ? false : !RelationIsCrossBucketIndex(indexRel));

    /* In new node, partition index's relfilenode copys from CN, need to update it. */
    ereport(LOG, (errmsg("The partition's index(%u) of relation(%u) is unusable, will create new relfilenode, "
                         "transferType is %d, isBucket is %d.",
        indexPart->pd_id, indexRel->rd_id, transferType, isBucket)));

    if (transferType == HASHBUCKET_TO_NORMAL || transferType == SEGMENT_TO_NORMAL) {
        newrelfilenode = GetNewRelFileNode(indexPart->pd_part->reltablespace, NULL, indexRel->rd_rel->relpersistence);
        bucketNode = InvalidBktId;
    } else {
        newrelfilenode = seg_alloc_segment(ConvertToRelfilenodeTblspcOid(indexPart->pd_part->reltablespace),
            u_sess->proc_cxt.MyDatabaseId, isBucket, InvalidBlockNumber);
        bucketNode = SegmentBktId;
    }

    RelFileNode newrnode = indexPart->pd_node;
    newrnode.relNode = newrelfilenode;
    newrnode.bucketNode = bucketNode;
    RelationCreateStorage(newrnode, indexRel->rd_rel->relpersistence, indexRel->rd_rel->relowner,
        isBucket ? bucketOid  : InvalidOid);

    InsertStorageIntoPendingList(&indexPart->pd_node, InvalidAttrNumber, indexRel->rd_backend,
        indexRel->rd_rel->relowner, true);

    Relation pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);
    HeapTuple reltup = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(indexPart->pd_id));
    if (!HeapTupleIsValid(reltup)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u", indexPart->pd_id)));
    }
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");
    replaces[Anum_pg_partition_relfilenode - 1] = true;
    values[Anum_pg_partition_relfilenode - 1] = ObjectIdGetDatum(newrelfilenode);
    HeapTuple ntup =
        (HeapTuple)tableam_tops_modify_tuple(reltup, RelationGetDescr(pg_partition), values, nulls, replaces);
    simple_heap_update(pg_partition, &ntup->t_self, ntup);
    CatalogUpdateIndexes(pg_partition, ntup);
    heap_freetuple(reltup);
    heap_freetuple(ntup);
    heap_close(pg_partition, RowExclusiveLock);
}

/*
 * For partition table, exchange meta information for each partition.
 */
static void PartitionRelfilenodeSwap(
    Relation oldHeap, const List* oldPartitions, Relation newHeap, const List* newPartitions)
{
    ListCell* old_cell = NULL;
    ListCell* new_cell = NULL;
    int count = 0;

    forboth(old_cell, oldPartitions, new_cell, newPartitions)
    {
        Partition old_partition = (Partition)lfirst(old_cell);
        Partition new_partition = (Partition)lfirst(new_cell);
        count++;
        if (RelationIsIndex(oldHeap)) {
            if (new_partition == NULL) {
                if (old_partition->pd_part->indisusable) {
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("The %dth partition index of relation(%u) is not exists.", count, newHeap->rd_id)));
                }
                continue;
            }
        }
        Relation old_partRel = partitionGetRelation(oldHeap, old_partition);
        Relation new_partRel = partitionGetRelation(newHeap, new_partition);
        TransactionId relfrozenxid = InvalidTransactionId;
        MultiXactId relminmxid = InvalidMultiXactId;
        getPartitionRelxids(old_partRel, &relfrozenxid, &relminmxid);

        /* Exchange two partition's meta information */
        if (RelationIsIndex(oldHeap)) {
            finishPartitionHeapSwap(old_partRel->rd_id, new_partRel->rd_id, false, relfrozenxid, relminmxid, true);
        } else {
            List* old_part_idx_list = PartitionGetPartIndexList(old_partition, true);
            List* new_part_idx_list = PartitionGetPartIndexList(new_partition, true);
            update_part_index(old_part_idx_list, new_partRel->rd_id);
            update_part_index(new_part_idx_list, old_partRel->rd_id);
            list_free_ext(old_part_idx_list);
            list_free_ext(new_part_idx_list);
            swap_partition_info(old_partRel->rd_id, new_partRel->rd_id);
        }

        /* Release partition relations. */
        releaseDummyRelation(&old_partRel);
        releaseDummyRelation(&new_partRel);
    }
}

/*
 * @Description : For partition table, exchange meta information for each partition
 * @in         	: OIDOldHeap, the relation oid.
 * @in          : OIDNewHeap, the relation oid.
 * @out         : None
 * @return      : None
 */
void partition_relfilenode_swap(Oid OIDOldHeap, Oid OIDNewHeap, uint8 needSwitch)
{
    List* old_partitions = NIL;
    List* new_partitions = NIL;

    Relation OldHeap = relation_open(OIDOldHeap, ExclusiveLock);
    Relation NewHeap = relation_open(OIDNewHeap, ExclusiveLock);

    if (RelationIsIndex(OldHeap)) {
        old_partitions = GetIndexPartitionListByOrder(OldHeap, ExclusiveLock);
        new_partitions = GetIndexPartitionListByOrder(NewHeap, ExclusiveLock);
    } else {
        old_partitions = relationGetPartitionList(OldHeap, ExclusiveLock);
        new_partitions = relationGetPartitionList(NewHeap, ExclusiveLock);
    }
    Assert(list_length(old_partitions) == list_length(new_partitions));
    PartitionRelfilenodeSwap(OldHeap, old_partitions, NewHeap, new_partitions);

    releasePartitionList(NewHeap, &new_partitions, ExclusiveLock, false);
    releasePartitionList(OldHeap, &old_partitions, ExclusiveLock);

    /* Swap all indices relfilenode on this relation expect col store. */
    if (RelationIsRelation(OldHeap) && !RelationIsColStore(OldHeap)) {
        CommandCounterIncrement();
        swapRelationIndicesRelfileNode(OldHeap, NewHeap, needSwitch);
    }

    heap_close(NewHeap, NoLock);
    heap_close(OldHeap, NoLock);
}

/*
 * @Description : Exchange all indices relfilenode between table rel1 and rel2.
 * @in          : rel1, the old relation.
 * @in          : rel2, the tmp relation.
 * @out         : None
 * @return      : void
 */
void swapRelationIndicesRelfileNode(Relation rel1, Relation rel2, uint8 needSwitch)
{
    Assert(PointerIsValid(rel1));
    Assert(PointerIsValid(rel2));
    List* indicesList = RelationGetIndexList(rel1);
    ListCell* cell = NULL;
    char* srcIdxName = NULL;
    char* tmpIdxName = NULL;
    char* srcSchema = NULL;
    Oid tmpIdxOid;
    Oid indexOid;

    foreach (cell, indicesList) {
        indexOid = lfirst_oid(cell);
        /* Get src index name by oid */
        srcIdxName = get_rel_name(indexOid);
        if (!PointerIsValid(srcIdxName)) {
            continue;
        }
        srcSchema = get_namespace_name(rel1->rd_rel->relnamespace);
        tmpIdxName = getTmptableIndexName(srcSchema, srcIdxName);
        /*
         * The tmp index name is same as src index name, check generateClonedIndexStmt.
         * Get namespace from tmp table, the index of tmp table must have same namespace with tmp table
         */
        tmpIdxOid = get_relname_relid(tmpIdxName, RelationGetNamespace(rel2));
        Assert(OidIsValid(tmpIdxOid));

        /* Swap index relfilenode */
        execute_relfilenode_swap(indexOid, tmpIdxOid, needSwitch);
        pfree_ext(srcIdxName);
        pfree_ext(tmpIdxName);
        pfree_ext(srcSchema);
    }

    list_free_ext(indicesList);
}

/*
 * @Description : Exchange relfilenode for ordinary table
 * @in         	: OIDOldHeap, the relation oid.
 * @in          : OIDNewHeap, the relation oid.
 * @out         : None
 * @return      : None
 */
void relfilenode_swap(Oid OIDOldHeap, Oid OIDNewHeap, uint8 needSwitch, TransactionId relfrozenxid,
    MultiXactId relMultiXid)
{
    Oid mapped_tables[4];
    int i;
    errno_t rc = 0;

    /* Zero out possible results from swapped_relation_files */
    rc = memset_s(mapped_tables, sizeof(mapped_tables), 0, sizeof(mapped_tables));
    securec_check_c(rc, "\0", "\0");

    /*
     * Swap the contents of the heap relations (including any toast tables).
     * Also set old heap's relfrozenxid to RecentGlobalXmin.
     */
    swap_relation_files(OIDOldHeap,
        OIDNewHeap,
        (OIDOldHeap == RelationRelationId),
        false,
        relfrozenxid,
        relMultiXid,
        mapped_tables);
    /*
     * Now we must remove any relation mapping entries that we set up for the
     * transient table, as well as its toast table and toast index if any. If
     * we fail to do this before commit, the relmapper will complain about new
     * permanent map entries being added post-bootstrap.
     */
    for (i = 0; OidIsValid(mapped_tables[i]); i++)
        RelationMapRemoveMapping(mapped_tables[i]);

    /* Swap all indices relfilenode on this relation expect col store. */
    Relation oldHeap = relation_open(OIDOldHeap, AccessShareLock);
    Relation newHeap = relation_open(OIDNewHeap, AccessShareLock);
    if (RelationIsRelation(oldHeap) && !RelationIsColStore(oldHeap)) {
        swapRelationIndicesRelfileNode(oldHeap, newHeap, needSwitch);
    }
    relation_close(oldHeap, AccessShareLock);
    relation_close(newHeap, AccessShareLock);
}
