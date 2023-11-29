/* -------------------------------------------------------------------------
 *
 * plancat.cpp
 *	   routines for accessing the system catalogs
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/plancat.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/heap.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/storage_gtt.h"
#include "commands/dbcommands.h"
#include "executor/node/nodeModifyTable.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/autoanalyzer.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "optimizer/predtest.h"
#include "optimizer/prep.h"
#include "optimizer/streamplan.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#ifdef USE_SPQ
#include "catalog/pg_inherits_fn.h"
#endif

#define ESTIMATE_PARTITION_NUMBER 10
#define ESTIMATE_PARTITION_NUMBER_THRESHOLD 5
#define ESTIMATE_PARTPAGES_THRESHOLD 50
#define DEFAULT_PAGES_NUM (u_sess->attr.attr_sql.enable_global_stats ? 10 * u_sess->pgxc_cxt.NumDataNodes : 10)
#define DEFAULT_TUPLES_NUM DEFAULT_PAGES_NUM

#define ESTIMATE_SUBPARTITION_NUMBER 100

/* Hook for plugins to get control in get_relation_info() */
THR_LOCAL get_relation_info_hook_type get_relation_info_hook = NULL;

extern void AcceptInvalidationMessages(void);
extern bool check_relation_analyzed(Oid relid);

static int32 get_rel_data_width(Relation rel, int32* attr_widths, bool vectorized = false);
static List* get_relation_constraints(PlannerInfo* root, Oid relationObjectId, RelOptInfo* rel, bool include_notnull);
List* build_index_tlist(PlannerInfo* root, IndexOptInfo* index, Relation heapRelation);
static void setRelStoreInfo(RelOptInfo* relOptInfo, Relation relation);

static BlockNumber ComputeTheTotalPages(Relation relation, int nonzeroPartitionNumber, int notAvailPartitionCnt,
    int totalPartitionNumber, BlockNumber partPages)
{
    BlockNumber totalPages = 0;
    int pnumber = RelationIsSubPartitioned(relation) ? ESTIMATE_SUBPARTITION_NUMBER : ESTIMATE_PARTITION_NUMBER;

    if (nonzeroPartitionNumber > 0 && nonzeroPartitionNumber < pnumber) {
        if (notAvailPartitionCnt != 0) {
            totalPages = partPages * ((nonzeroPartitionNumber + notAvailPartitionCnt) / (double)nonzeroPartitionNumber);
        } else {
            totalPages = partPages;
        }
    } else if (nonzeroPartitionNumber == pnumber) {
        totalPages = partPages * (totalPartitionNumber / (double)nonzeroPartitionNumber);
    } else if (nonzeroPartitionNumber == 0) {
        totalPages = relation->rd_rel->relpages;
    }

    return totalPages;
}

/*
 * A modified method for acquireSamplesForPartitionedRelation
 * We use sampling stratification to get 10 partitions, instead of using the top 10 partitions, meanwhile the relpages
 * in pg_partition (calculated by lazy vacuum) are used as weight.
 * And if the valid partitions is not enough (less than or equal to 5), we use the relpages in pg_class (calculated by
 * analyze or lazy vacuum) as a result.
 */
static void acquireSamplesForPartitionedRelationModify(
    Relation relation, LOCKMODE lmode, RelPageType* samplePages, List** sampledPartitionOids)
{
    if (!(RelationIsPartitioned(relation) && RelationIsRelation(relation))) {
        return;
    }

    List* partoidlist = relationGetPartitionOidList(relation);
    int totalPartitionNumber = list_length(partoidlist);
    int nonzeroPartitionNumber = 0;
    BlockNumber partPages = 0;
    BlockNumber currentPartPages = 0;

    BlockNumber relPages = relation->rd_rel->relpages; /* analyzed pages, stored in pg_class->relpages */
    BlockNumber relpartPages = 0; /* analyzed sample pages, stored in pg_partition->relpages */

    int stepNum =
        totalPartitionNumber > ESTIMATE_PARTITION_NUMBER ? totalPartitionNumber / ESTIMATE_PARTITION_NUMBER : 1;
    int iterations = 0;
    ListCell* cell = NULL;
    foreach(cell, partoidlist) {
        iterations++;
        if (iterations % stepNum != 0) {
            continue;
        }

        Oid partitionOid = DatumGetObjectId(lfirst(cell));
        if (!OidIsValid(partitionOid)) {
            continue;
        }

        if (!ConditionalLockPartition(relation->rd_id, partitionOid, lmode, PARTITION_LOCK)) {
            continue;
        }

        /* we've already got partition lock in ConditionalLockPartition, so just NoLock here */
        /* the partition may have already been dropped, so should check here */
        Partition part = tryPartitionOpen(relation, partitionOid, NoLock);
        if (part == NULL) {
            continue;
        }
        currentPartPages = PartitionGetNumberOfBlocksInFork(relation, part, MAIN_FORKNUM, true);
        /* for empty heap, PartitionGetNumberOfBlocks() return 0 */
        if (currentPartPages > 0) {
            if (sampledPartitionOids != NULL)
                *sampledPartitionOids = lappend_oid(*sampledPartitionOids, partitionOid);

            partPages += currentPartPages;
            relpartPages += part->pd_part->relpages;
            nonzeroPartitionNumber++;
        }
        partitionClose(relation, part, lmode);
    }

    if (relpartPages > 0 && nonzeroPartitionNumber > ESTIMATE_PARTITION_NUMBER_THRESHOLD) {
        *samplePages = partPages * (relPages / (double)relpartPages);
    } else {
        *samplePages = relPages;
    }
}

int GetTotalPartitionNumber(Relation relation)
{
    int totalPartitionNumber = 0;
    if (relation->partMap->type == PART_TYPE_LIST) {
        totalPartitionNumber = getNumberOfListPartitions(relation);
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        totalPartitionNumber = getNumberOfHashPartitions(relation);
    } else {
        totalPartitionNumber = getNumberOfRangePartitions(relation);
    }
    return totalPartitionNumber;
}

static void acquireSamplesForSubPartitionedRelation(Relation relation, LOCKMODE lmode, RelPageType *samplePages,
    List **sampledPartitionOids)
{
    Assert(RelationIsSubPartitioned(relation));

    /* we set estimate flag here to avoid wait on lock when another session still holds the lock */
    List *subpartidlist = RelationGetSubPartitionOidList(relation, AccessShareLock, true);
    int totalSubPartitionNumber = list_length(subpartidlist);
    int nonzeroSubPartitionNumber = 0;
    int notAvailSubPartitionNumber = 0;
    BlockNumber partPages = 0;
    BlockNumber currentPartPages = 0;
    Oid relid = RelationGetRelid(relation);

    int iterations = 0;
    ListCell *cell = NULL;

    int stepNum = totalSubPartitionNumber > ESTIMATE_SUBPARTITION_NUMBER ?
        totalSubPartitionNumber / ESTIMATE_SUBPARTITION_NUMBER :
        1;
    foreach (cell, subpartidlist) {
        iterations++;
        if (iterations % stepNum != 0) {
            continue;
        }

        Oid subpartid = DatumGetObjectId(lfirst(cell));
        Oid partid = partid_get_parentid(subpartid);
        if (!OidIsValid(partid)) {
            notAvailSubPartitionNumber++;
            continue;
        }

        if (!ConditionalLockPartition(relid, partid, lmode, PARTITION_LOCK)) {
            notAvailSubPartitionNumber++;
            continue;
        }
        if (!ConditionalLockPartition(partid, subpartid, lmode, PARTITION_LOCK)) {
            if (lmode != NoLock) {
                UnlockPartition(relid, partid, lmode, PARTITION_LOCK);
            }
            notAvailSubPartitionNumber++;
            continue;
        }

        /*
         * we've already got partition lock in ConditionalLockPartition, so just NoLock here
         * and noticed that nolock is applied when getting subpartidlist, so the partition may have already been dropped
         */
        Partition part = tryPartitionOpen(relation, partid, NoLock);
        if (part == NULL) {
            totalSubPartitionNumber--;
            continue;
        }
        Relation partrel = partitionGetRelation(relation, part);
        /* the subpartition may be dropped */
        Partition subpart = tryPartitionOpen(partrel, subpartid, NoLock);
        if (subpart == NULL) {
            releaseDummyRelation(&partrel);
            partitionClose(relation, part, lmode);
            totalSubPartitionNumber--;
            continue;
        }
        currentPartPages = PartitionGetNumberOfBlocksInFork(partrel, subpart, MAIN_FORKNUM, true);
        /* for empty heap, PartitionGetNumberOfBlocks() return 0 */
        if (currentPartPages > 0) {
            if (sampledPartitionOids != NULL)
                *sampledPartitionOids = lappend_oid(*sampledPartitionOids, subpartid);

            partPages += currentPartPages;
            nonzeroSubPartitionNumber++;
        }
        partitionClose(partrel, subpart, lmode);
        releaseDummyRelation(&partrel);
        partitionClose(relation, part, lmode);

        if (nonzeroSubPartitionNumber == ESTIMATE_SUBPARTITION_NUMBER) {
            break;
        }
    }

    *samplePages = ComputeTheTotalPages(relation, nonzeroSubPartitionNumber, notAvailSubPartitionNumber,
        totalSubPartitionNumber, partPages);
}

static void acquireSamplesForPartitionedRelation(
    Relation relation, LOCKMODE lmode, RelPageType* samplePages, List** sampledPartitionOids)
{
    if (RelationIsSubPartitioned(relation)) {
        acquireSamplesForSubPartitionedRelation(relation, lmode, samplePages, sampledPartitionOids);
        return;
    }

    if (RelationIsPartitioned(relation)) {
        if (relation->rd_rel->relkind == RELKIND_RELATION) {  
            int totalRangePartitionNumber = GetTotalPartitionNumber(relation);
            int totalPartitionNumber = totalRangePartitionNumber;
            int nonzeroPartitionNumber = 0;
            BlockNumber partPages = 0;
            BlockNumber currentPartPages = 0;
            Partition part = NULL;
            int notAvailPartitionCnt = 0;

            List *partidlist = relationGetPartitionOidList(relation);
            ListCell *cell = NULL;
            foreach (cell, partidlist) {
                Oid partitionOid = DatumGetObjectId(lfirst(cell));

                if (!OidIsValid(partitionOid))
                    continue;

                if (!ConditionalLockPartition(relation->rd_id, partitionOid, lmode, PARTITION_LOCK)) {
                    notAvailPartitionCnt++;
                    continue;
                }

                /* we've already got partition lock in ConditionalLockPartition, so just NoLock here */
                /* the partition may have already been dropped, so should check here */
                part = tryPartitionOpen(relation, partitionOid, NoLock);
                if (part == NULL) {
                    notAvailPartitionCnt++;
                    continue;
                }
                currentPartPages = PartitionGetNumberOfBlocksInFork(relation, part, MAIN_FORKNUM, true);
                partitionClose(relation, part, lmode);

                /* for empty heap, PartitionGetNumberOfBlocks() return 0 */
                if (currentPartPages > 0) {
                    if (sampledPartitionOids != NULL)
                        *sampledPartitionOids = lappend_oid(*sampledPartitionOids, partitionOid);

                    partPages += currentPartPages;
                    if (++nonzeroPartitionNumber == ESTIMATE_PARTITION_NUMBER) {
                        break;
                    }
                }
            }

            *samplePages = ComputeTheTotalPages(relation, nonzeroPartitionNumber, notAvailPartitionCnt,
                                                totalPartitionNumber, partPages);

            if (nonzeroPartitionNumber == ESTIMATE_PARTITION_NUMBER &&
                *samplePages < relation->rd_rel->relpages / ESTIMATE_PARTPAGES_THRESHOLD) {
                if (sampledPartitionOids != NULL)
                    list_free_ext(*sampledPartitionOids);
                acquireSamplesForPartitionedRelationModify(relation, lmode, samplePages, sampledPartitionOids);
            }
            pfree_ext(partidlist);
        }
    }
}

static RelPageType EstimatePartitionIndexPages(Relation relation, Relation indexRelation, List *sampledPartitionIds)
{
    /* normal partitioned index, including crossbucket index */
    if (sampledPartitionIds == NIL) {
        return indexRelation->rd_rel->relpages;
    }

    ListCell *cell = NULL;
    BlockNumber indexPages = 0;
    BlockNumber partIndexPages = 0;
    BlockNumber indexrelPartPages = 0; /* analyzed pages, stored in pg_partition->relpages */
    int partitionNum = getNumberOfPartitions(relation);
    int samplePartitions = 0;
    foreach (cell, sampledPartitionIds) {
        Oid partOid = lfirst_oid(cell);
        Oid partIndexOid = getPartitionIndexOid(indexRelation->rd_id, partOid);
        if (!ConditionalLockPartition(indexRelation->rd_id, partIndexOid, AccessShareLock, PARTITION_LOCK)) {
            continue;
        }
        samplePartitions++;
        Partition partIndex = partitionOpen(indexRelation, partIndexOid, NoLock);
        partIndexPages += PartitionGetNumberOfBlocks(indexRelation, partIndex);
        indexrelPartPages += partIndex->pd_part->relpages;
        partitionClose(indexRelation, partIndex, AccessShareLock);
    }

    /* if all partition is locked, just use relpages in index relation */
    if (samplePartitions == 0) {
        return indexRelation->rd_rel->relpages;
    }

    indexPages = partIndexPages * (partitionNum / samplePartitions);

    if (!RelationIsSubPartitioned(relation)) {
        if (indexrelPartPages > 0 && partitionNum > ESTIMATE_PARTITION_NUMBER &&
            partIndexPages < indexRelation->rd_rel->relpages / ESTIMATE_PARTPAGES_THRESHOLD) {
            if (samplePartitions  > ESTIMATE_PARTITION_NUMBER_THRESHOLD) {
                indexPages = partIndexPages * (indexRelation->rd_rel->relpages / (double)indexrelPartPages);
            } else {
                indexPages = indexRelation->rd_rel->relpages;
            }
        }
    }

    return indexPages;
}

/*
 * get_relation_info -
 *	  Retrieves catalog information for a given relation.
 *
 * Given the Oid of the relation, return the following info into fields
 * of the RelOptInfo struct:
 *
 *	min_attr	lowest valid AttrNumber
 *	max_attr	highest valid AttrNumber
 *	indexlist	list of IndexOptInfos for relation's indexes
 *	fdwroutine	if it's a foreign table, the FDW function pointers
 *	pages		number of pages
 *	tuples		number of tuples
 *
 * Also, initialize the attr_needed[] and attr_widths[] arrays.  In most
 * cases these are left as zeroes, but sometimes we need to compute attr
 * widths here, and we may as well cache the results for costsize.c.
 *
 * If inhparent is true, all we need to do is set up the attr arrays:
 * the RelOptInfo actually represents the appendrel formed by an inheritance
 * tree, and so the parent rel's physical size and index information isn't
 * important for it.
 */
void get_relation_info(PlannerInfo* root, Oid relationObjectId, bool inhparent, RelOptInfo* rel)
{
    Index varno = rel->relid;
    bool hasindex = false;
    List* indexinfos = NIL;
    List* sampledPartitionIds = NIL;

    /*
     * We need not lock the relation since it was already locked, either by
     * the rewriter or when expand_inherited_rtentry() added it to the query's
     * rangetable.
     */
    Relation relation = heap_open(relationObjectId, NoLock);
    /* Temporary and unlogged relations are inaccessible during recovery. */
    if (!RelationNeedsWAL(relation) && RecoveryInProgress())
        ereport(ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot access temporary or unlogged relations during recovery"))));

    rel->min_attr = FirstLowInvalidHeapAttributeNumber + 1;
    rel->max_attr = RelationGetNumberOfAttributes(relation);
    rel->reltablespace = RelationGetForm(relation)->reltablespace;

    AssertEreport(rel->max_attr >= rel->min_attr, MOD_OPT, "Max attribute no is less than the min attribute number.");
    rel->attr_needed = (Relids*)palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(Relids));
    rel->attr_widths = (int32*)palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(int32));

    /*
     * Estimate relation size --- unless it's an inheritance parent, in which
     * case the size will be computed later in set_append_rel_pathlist, and we
     * must leave it zero for now to avoid bollixing the total_table_pages
     * calculation.
     */
    if (!inhparent)
        estimate_rel_size(relation,
            rel->attr_widths - rel->min_attr,
            &rel->pages,
            &rel->tuples,
            &rel->allvisfrac,
            &sampledPartitionIds);

    /*
     * Make list of indexes.  Ignore indexes on system catalogs if told to.
     * Don't bother with indexes for an inheritance parent, either.
     */
    if (inhparent || (u_sess->attr.attr_common.IgnoreSystemIndexes && IsSystemClass(relation->rd_rel)))
        hasindex = false;
    else
        hasindex = relation->rd_rel->relhasindex;

    if (hasindex) {
        ListCell* l = NULL;
        LOCKMODE lmode;

        List* indexoidlist = RelationGetIndexList(relation);

        /*
         * For each index, we get the same type of lock that the executor will
         * need, and do not release it.  This saves a couple of trips to the
         * shared lock manager while not creating any real loss of
         * concurrency, because no schema changes could be happening on the
         * index while we hold lock on the parent rel, and neither lock type
         * blocks any other kind of index operation.
         */
        lmode = (rel->relid == (unsigned int)linitial2_int(root->parse->resultRelations)) ?
            RowExclusiveLock : AccessShareLock;

        StringInfoData gttEmptyIndex;
        initStringInfo(&gttEmptyIndex);

        foreach (l, indexoidlist) {
            Oid indexoid = lfirst_oid(l);
            int i, ncolumns, nkeycolumns;

            /*
             * Extract info from the relation descriptor for the index.
             */
            Relation indexRelation = index_open(indexoid, lmode);
            Form_pg_index index = indexRelation->rd_index;

            /*
             * Ignore invalid indexes, since they can't safely be used for
             * queries.  Note that this is OK because the data structure we
             * are constructing is only used by the planner --- the executor
             * still needs to insert into "invalid" indexes, if they're marked
             * IndexIsReady.
             */
            if (!IndexIsValid(index) || !GetIndexVisibleStateByTuple(indexRelation->rd_indextuple)) {
                index_close(indexRelation, NoLock);
                continue;
            }

            /* Ignore empty index for global temp table */
            if (RELATION_IS_GLOBAL_TEMP(indexRelation) && !gtt_storage_attached(RelationGetRelid(indexRelation))) {
                if (u_sess->attr.attr_sql.under_explain) {
                    char* emptyIndexName = get_rel_name(indexoid);
                    appendStringInfo(&gttEmptyIndex, "%s ", emptyIndexName);
                    pfree(emptyIndexName);
                }
                index_close(indexRelation, NoLock);
                continue;
            }

            /*
             * If the index is valid, but cannot yet be used, ignore it; but
             * mark the plan we are generating as transient. See
             * src/backend/access/heap/README.HOT for discussion.
             */
            if (index->indcheckxmin) {
                TransactionId xmin = HeapTupleGetRawXmin(indexRelation->rd_indextuple);
                if (RelationIsUBTree(indexRelation) && TransactionIdIsCurrentTransactionId(xmin) &&
                    !IsolationUsesXactSnapshot()) {
                    /* new created ubtree index of the current Read Committed transaction is still valid */
                } else if (!TransactionIdPrecedes(xmin, u_sess->utils_cxt.TransactionXmin)) {
                    /*
                     * Since the TransactionXmin won't advance immediately(see CalculateLocalLatestSnapshot),
                     * we need to check CSN for the visibility.
                     */
                    CommitSeqNo csn = TransactionIdGetCommitSeqNo(xmin, false, true, false, NULL);
                    if (!COMMITSEQNO_IS_COMMITTED(csn) || csn >= u_sess->utils_cxt.CurrentSnapshot->snapshotcsn) {
                        root->glob->transientPlan = true;
                        index_close(indexRelation, NoLock);
                        continue;
                    }
                }
            }

            IndexOptInfo* info = makeNode(IndexOptInfo);

            info->indexoid = index->indexrelid;
            info->reltablespace = RelationGetForm(indexRelation)->reltablespace;
            info->rel = rel;
            info->ncolumns = ncolumns = index->indnatts;
            info->nkeycolumns = nkeycolumns = IndexRelationGetNumberOfKeyAttributes(indexRelation);
            info->indexkeys = (int*)palloc(sizeof(int) * ncolumns);
            info->indexcollations = (Oid*)palloc(sizeof(Oid) * nkeycolumns);
            info->opfamily = (Oid*)palloc(sizeof(Oid) * nkeycolumns);
            info->opcintype = (Oid*)palloc(sizeof(Oid) * nkeycolumns);

            /* index kinds */
            info->isGlobal = RelationIsGlobalIndex(indexRelation);
            info->crossbucket = RelationIsCrossBucketIndex(indexRelation);

#ifndef ENABLE_MULTIPLE_NODES
            /* IUD to global partition index do not support stream */
            if (IS_STREAM && root->parse->commandType != CMD_SELECT && info->isGlobal) {
                mark_stream_unsupport();
            }
#endif

            for (i = 0; i < ncolumns; i++) {
                info->indexkeys[i] = index->indkey.values[i];
            }

            for (i = 0; i < nkeycolumns; i++) {
                info->opfamily[i] = indexRelation->rd_opfamily[i];
                info->opcintype[i] = indexRelation->rd_opcintype[i];
                info->indexcollations[i] = indexRelation->rd_indcollation[i];
            }

            info->relam = indexRelation->rd_rel->relam;
            info->amcostestimate = indexRelation->rd_am->amcostestimate;
            info->canreturn = index_can_return(indexRelation);
            info->amcanorderbyop = indexRelation->rd_am->amcanorderbyop;
            info->amoptionalkey = indexRelation->rd_am->amoptionalkey;
            info->amsearcharray = indexRelation->rd_am->amsearcharray;
            info->amsearchnulls = indexRelation->rd_am->amsearchnulls;
            info->amhasgettuple = OidIsValid(indexRelation->rd_am->amgettuple);
            info->amhasgetbitmap = OidIsValid(indexRelation->rd_am->amgetbitmap);

            /*
             * Fetch the ordering information for the index, if any.
             */
            if (OID_IS_BTREE(info->relam)) {
                /*
                 * If it's a btree index, we can use its opfamily OIDs
                 * directly as the sort ordering opfamily OIDs.
                 */
                AssertEreport(indexRelation->rd_am->amcanorder, MOD_OPT, "amcanorder is NULL.");

                info->sortopfamily = info->opfamily;
                info->reverse_sort = (bool*)palloc(sizeof(bool) * nkeycolumns);
                info->nulls_first = (bool*)palloc(sizeof(bool) * nkeycolumns);

                for (i = 0; i < nkeycolumns; i++) {
                    int16 opt = indexRelation->rd_indoption[i];

                    info->reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
                    info->nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;
                }
            } else if (indexRelation->rd_am->amcanorder) {
                /*
                 * Otherwise, identify the corresponding btree opfamilies by
                 * trying to map this index's "<" operators into btree.  Since
                 * "<" uniquely defines the behavior of a sort order, this is
                 * a sufficient test.
                 *
                 * XXX This method is rather slow and also requires the
                 * undesirable assumption that the other index AM numbers its
                 * strategies the same as btree.  It'd be better to have a way
                 * to explicitly declare the corresponding btree opfamily for
                 * each opfamily of the other index type.  But given the lack
                 * of current or foreseeable amcanorder index types, it's not
                 * worth expending more effort on now.
                 */
                info->sortopfamily = (Oid*)palloc(sizeof(Oid) * nkeycolumns);
                info->reverse_sort = (bool*)palloc(sizeof(bool) * nkeycolumns);
                info->nulls_first = (bool*)palloc(sizeof(bool) * nkeycolumns);

                for (i = 0; i < nkeycolumns; i++) {
                    int16 opt = indexRelation->rd_indoption[i];
                    int16 btstrategy;
                    Oid btopfamily, btopcintype;

                    info->reverse_sort[i] = (((unsigned int)opt) & INDOPTION_DESC) != 0;
                    info->nulls_first[i] = (((unsigned int)opt) & INDOPTION_NULLS_FIRST) != 0;

                    Oid ltopr = get_opfamily_member(
                        info->opfamily[i], info->opcintype[i], info->opcintype[i], BTLessStrategyNumber);
                    if (OidIsValid(ltopr) &&
                        get_ordering_op_properties(ltopr, &btopfamily, &btopcintype, &btstrategy) &&
                        btopcintype == info->opcintype[i] && btstrategy == BTLessStrategyNumber) {
                        /* Successful mapping */
                        info->sortopfamily[i] = btopfamily;
                    } else {
                        /* Fail ... quietly treat index as unordered */
                        info->sortopfamily = NULL;
                        info->reverse_sort = NULL;
                        info->nulls_first = NULL;
                        break;
                    }
                }
            } else {
                info->sortopfamily = NULL;
                info->reverse_sort = NULL;
                info->nulls_first = NULL;
            }

            /*
             * Fetch the index expressions and predicate, if any.  We must
             * modify the copies we obtain from the relcache to have the
             * correct varno for the parent relation, so that they match up
             * correctly against qual clauses.
             */
            info->indexprs = RelationGetIndexExpressions(indexRelation);
            info->indpred = RelationGetIndexPredicate(indexRelation);
            if (info->indexprs && varno != 1)
                ChangeVarNodes((Node*)info->indexprs, 1, varno, 0);
            if (info->indpred && varno != 1)
                ChangeVarNodes((Node*)info->indpred, 1, varno, 0);

            /* Build targetlist using the completed indexprs data */
            info->indextlist = build_index_tlist(root, info, relation);

            info->predOK = false; /* set later in indxpath.c */
            info->unique = index->indisunique;
            info->immediate = index->indimmediate;
            info->hypothetical = false;

            /*
             * Estimate the index size.  If it's not a partial index, we lock
             * the number-of-tuples estimate to equal the parent table; if it
             * is partial then we have to use the same methods as we would for
             * a table, except we can be sure that the index is not larger
             * than the table.
             */
            if (info->indpred == NIL) {
#ifdef ENABLE_MULTIPLE_NODES
                /*
                 * If parent relation is distributed the local storage manager
                 * does not have actual information about index size.
                 * We have to get relation statistics instead.
                 */
                if (IS_PGXC_COORDINATOR && relation->rd_locator_info != NULL) {
                    info->pages = indexRelation->rd_rel->relpages;
                } else {
#endif
                    if (!RelationIsPartitioned(indexRelation)) { /* non-partitioned index */
                        if (RELATION_CREATE_BUCKET(indexRelation)) {
                            info->pages = RelationGetNumberOfBlocksInFork(indexRelation, MAIN_FORKNUM, true); 
                        } else {
                            info->pages = RelationGetNumberOfBlocks(indexRelation);
                        }
                    } else { /* partitioned index */
                        if (RelationIsGlobalIndex(indexRelation)) {
                            /* global partitioned index */
                            info->pages = RelationGetNumberOfBlocks(indexRelation);
                        } else {
                            info->pages = EstimatePartitionIndexPages(relation, indexRelation, sampledPartitionIds);
                        }
                    }
#ifdef ENABLE_MULTIPLE_NODES
                }
#endif
                info->tuples = rel->tuples;
            } else {
                double allvisfrac; /* dummy */

                estimate_rel_size(indexRelation, NULL, &info->pages, &info->tuples, &allvisfrac, NULL);
                if (info->tuples > rel->tuples)
                    info->tuples = rel->tuples;
            }

            info->ispartitionedindex = RelationIsPartitioned(relation);
            info->partitionindex = InvalidOid;

            index_close(indexRelation, NoLock);

            indexinfos = lcons(info, indexinfos);
        }
        if (gttEmptyIndex.data[0] != '\0') {
            ereport(WARNING,
                (errmsg("The following uninitialized GTT indexes are ignored: %s", gttEmptyIndex.data)));
        }
        pfree_ext(gttEmptyIndex.data);

        list_free_ext(indexoidlist);
    }

    rel->indexlist = indexinfos;
    setRelStoreInfo(rel, relation);

    /* Grab the fdwroutine info using the relcache, while we have it */
    if (relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE || relation->rd_rel->relkind == RELKIND_STREAM) {
        rel->serverid = GetForeignServerIdByRelId(RelationGetRelid(relation));
        rel->fdwroutine = GetFdwRoutineForRelation(relation, true);
    } else {
        rel->serverid = InvalidOid;
        rel->fdwroutine = NULL;
    }

    heap_close(relation, NoLock);

    /*
    * Allow a plugin to editorialize on the info we obtained from the
    * catalogs.  Actions might include altering the assumed relation size,
    * removing an index, or adding a hypothetical index to the indexlist.
    */
    if (get_relation_info_hook)
        (*get_relation_info_hook) (root, relationObjectId, inhparent, rel);
}

/*
 * estimate_rel_size - estimate # pages and # tuples in a table or index
 *
 * We also estimate the fraction of the pages that are marked all-visible in
 * the visibility map, for use in estimation of index-only scans.
 *
 * If attr_widths isn't NULL, it points to the zero-index entry of the
 * relation's attr_widths[] cache; we fill this in if we have need to compute
 * the attribute widths for estimation purposes.
 */
void estimate_rel_size(Relation rel, int32* attr_widths, RelPageType* pages, double* tuples, double* allvisfrac,
    List** sampledPartitionIds)
{
    RelPageType curpages = 0;
    RelPageType relpages;
    double reltuples;
    BlockNumber relallvisible;
    double density;

    switch (rel->rd_rel->relkind) {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
#ifdef ENABLE_MULTIPLE_NODES
            /*
             * This is a remote table... we have no idea how many pages/rows
             * we may get from a scan of this table. However, we should set the
             * costs in such a manner that cheapest paths should pick up the
             * ones involving these remote rels
             *
             * These allow for maximum query shipping to the remote
             * side later during the planning phase
             *
             * This has to be set on a remote Coordinator only
             * as it hugely penalizes performance on backend Nodes.
             *
             * Override the estimates only for remote tables (currently
             * identified by non-NULL rd_locator_info)
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && rel->rd_locator_info) {
                *pages = (RelPageType)DEFAULT_PAGES_NUM;
                *tuples = (double)DEFAULT_TUPLES_NUM;
                *tuples = clamp_row_est(*tuples);
#ifdef STREAMPLAN
                if (IS_PGXC_COORDINATOR) {
                    if ((rel->rd_rel->relpages > 0) || (rel->rd_rel->reltuples > 0)) {
                        *pages = rel->rd_rel->relpages;
                        *tuples = (double)rel->rd_rel->reltuples;
                        *tuples = clamp_row_est(*tuples);
                    } else if (rel->rd_id >= FirstNormalObjectId) {
                        if (u_sess->analyze_cxt.need_autoanalyze && !check_relation_analyzed(rel->rd_id)) {
                            bool is_analyzed = AutoAnaProcess::runAutoAnalyze(rel);
                            if (is_analyzed) {
                                /* refresh the statistic info */
                                AcceptInvalidationMessages();

                                /* keep the default value for empty table */
                                if ((rel->rd_rel->relpages > 0) || (rel->rd_rel->reltuples > 0)) {
                                    *pages = rel->rd_rel->relpages;
                                    *tuples = (double)rel->rd_rel->reltuples;
                                    *tuples = clamp_row_est(*tuples);
                                }
                            } else {
                                elog(LOG,
                                    "[AUTO-ANALYZE] fail to do autoanalyze on table \"%s.%s\"",
                                    get_namespace_name(RelationGetNamespace(rel)),
                                    RelationGetRelationName(rel));

                                set_noanalyze_rellist(rel->rd_id, 0);
                            }
                        } else
                            set_noanalyze_rellist(rel->rd_id, 0);
                    }
                }
#endif
                break;
            }
#endif
        /* fall through */
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
        /* fall through */
        case RELKIND_TOASTVALUE:
            /*
             * 1. if it has storage, ok to call the smgr;(for  non-partitioned relation)
             * 2. if it doesnot have storage(for partitioned relation),
             *	get the curpages of average first
             *	ESTIMATE_PARTITION_NUMBER non-zero-pages partitions
             *	multiply total number of partitions
             */
            if ((RelationIsPartitioned(rel) || RelationIsSubPartitioned(rel)) && !RelationIsColStore(rel) &&
                !RelationIsGlobalIndex(rel)) {
                curpages = rel->rd_rel->relpages;
                if (curpages == 0) {
                    acquireSamplesForPartitionedRelation(rel, AccessShareLock, &curpages, sampledPartitionIds);
                }
            } else if (RelationIsValuePartitioned(rel)) {
                /*
                 * For value partitioned rels, the effort of getting curpages might be
                 * very consuming(high uniqueness of partKey), as we have to recursively
                 * get HDFS file sizes under each partition directories, so for this
                 * case we'd prefer to look at relpages in pg_class to get a rough estimation
                 * as workaround.
                 *
                 * Please note, as comparing to none-partitioned HDFS table, partitioned
                 * HDFS's execution plan is more heavily depending on STATS so we are
                 * strongly recommanding to run ANALYZE command before submitting a
                 * query with partitioned HDFS tables.
                 */
                curpages = rel->rd_rel->relpages;
            } else {
#ifdef ENABLE_MULTIPLE_NODES
                if (RELATION_CREATE_BUCKET(rel)) {
                    curpages = RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM, true);
                } else {
#endif
                    curpages = RelationGetNumberOfBlocks(rel);
#ifdef ENABLE_MULTIPLE_NODES
                }
#endif
            }

            /*
             * HACK: if the relation has never yet been vacuumed, use a
             * minimum size estimate of 10 pages.  The idea here is to avoid
             * assuming a newly-created table is really small, even if it
             * currently is, because that may not be true once some data gets
             * loaded into it.	Once a vacuum or analyze cycle has been done
             * on it, it's more reasonable to believe the size is somewhat
             * stable.
             *
             * (Note that this is only an issue if the plan gets cached and
             * used again after the table has been filled.	What we're trying
             * to avoid is using a nestloop-type plan on a table that has
             * grown substantially since the plan was made.  Normally,
             * autovacuum/autoanalyze will occur once enough inserts have
             * happened and cause cached-plan invalidation; but that doesn't
             * happen instantaneously, and it won't happen at all for cases
             * such as temporary tables.)
             *
             * We approximate "never vacuumed" by "has relpages = 0", which
             * means this will also fire on genuinely empty relations.	Not
             * great, but fortunately that's a seldom-seen case in the real
             * world, and it shouldn't degrade the quality of the plan too
             * much anyway to err in this direction.
             *
             * There are two exceptions wherein we don't apply this heuristic.
             * One is if the table has inheritance children.  Totally empty
             * parent tables are quite common, so we should be willing to
             * believe that they are empty.  Also, we don't apply the 10-page
             * minimum to indexes.
             */
            if (curpages < 10 && rel->rd_rel->relpages == 0 && !rel->rd_rel->relhassubclass &&
                rel->rd_rel->relkind != RELKIND_INDEX)
                curpages = 10;

            /* report estimated # pages */
            *pages = curpages;
            /* quick exit if rel is clearly empty */
            if (curpages == 0) {
                *tuples = 0;
                *allvisfrac = 0;
                break;
            }
            /* coerce values in pg_class to more desirable types */
            relpages = rel->rd_rel->relpages;
            reltuples = (double)rel->rd_rel->reltuples;
            relallvisible = (BlockNumber)rel->rd_rel->relallvisible;

            /*
             * If it's an index, discount the metapage while estimating the
             * number of tuples.  This is a kluge because it assumes more than
             * it ought to about index structure.  Currently it's OK for
             * btree, hash, and GIN indexes but suspect for GiST indexes.
             */
            if (rel->rd_rel->relkind == RELKIND_INDEX && relpages > 0) {
                curpages--;
                relpages--;
            }

            /* estimate number of tuples from previous tuple density */
            if (relpages > 0)
                density = reltuples / (double)relpages;
            else {
                /*
                 * When we have no data because the relation was truncated,
                 * estimate tuple width from attribute datatypes.  We assume
                 * here that the pages are completely full, which is OK for
                 * tables (since they've presumably not been VACUUMed yet) but
                 * is probably an overestimate for indexes.  Fortunately
                 * get_relation_info() can clamp the overestimate to the
                 * parent table's size.
                 *
                 * Note: this code intentionally disregards alignment
                 * considerations, because (a) that would be gilding the lily
                 * considering how crude the estimate is, and (b) it creates
                 * platform dependencies in the default plans which are kind
                 * of a headache for regression testing.
                 */
                int32 tuple_width;

                tuple_width = get_rel_data_width(rel, attr_widths);
                tuple_width += sizeof(HeapTupleHeaderData);
                tuple_width += sizeof(ItemPointerData);
                /* note: integer division is intentional here */
                density = (BLCKSZ - SizeOfPageHeaderData) / (double)tuple_width;
            }
            *tuples = rint(density * curpages);
            *tuples = clamp_row_est(*tuples);

            /*
             * We use relallvisible as-is, rather than scaling it up like we
             * do for the pages and tuples counts, on the theory that any
             * pages added since the last VACUUM are most likely not marked
             * all-visible.  But costsize.c wants it converted to a fraction.
             */
            if (relallvisible == 0 || curpages <= 0)
                *allvisfrac = 0;
            else if ((double)relallvisible >= curpages)
                *allvisfrac = 1;
            else
                *allvisfrac = (double)relallvisible / curpages;
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            /* Sequences always have a known size */
            *pages = 1;
            *tuples = 1;
            *allvisfrac = 0;
            break;
        case RELKIND_STREAM:
        case RELKIND_FOREIGN_TABLE:
            /* Just use whatever's in pg_class */
            *pages = rel->rd_rel->relpages;
            *tuples = rel->rd_rel->reltuples;
            *tuples = clamp_row_est(*tuples);
            *allvisfrac = 0;
            /*
             * Append no analyze relation to g_NoAnalyzeRelNameList
             * in order to print warning and output to log for hdfs foreign table.
             */
            if ((*pages == 0) && (*tuples == 0) && (rel->rd_id >= FirstNormalObjectId))
                set_noanalyze_rellist(rel->rd_id, 0);
            break;
        default:
            /* else it has no disk storage; probably shouldn't get here? */
            *pages = 0;
            *tuples = 0;
            *allvisfrac = 0;
            break;
    }
}

/*
 * get_rel_data_width
 *
 * Estimate the average width of (the data part of) the relation's tuples.
 *
 * If attr_widths isn't NULL, it points to the zero-index entry of the
 * relation's attr_widths[] cache; use and update that cache as appropriate.
 *
 * Currently we ignore dropped columns.  Ideally those should be included
 * in the result, but we haven't got any way to get info about them; and
 * since they might be mostly NULLs, treating them as zero-width is not
 * necessarily the wrong thing anyway.
 */
static int32 get_rel_data_width(Relation rel, int32* attr_widths, bool vectorized)
{
    int32 tuple_width = 0;
    int i;
    bool isPartition = RelationIsPartition(rel);
    bool hasencoded = false;

    for (i = 1; i <= RelationGetNumberOfAttributes(rel); i++) {
        Form_pg_attribute att = &rel->rd_att->attrs[i - 1];
        int32 item_width;
        int4 att_typmod = att->atttypmod;
        Oid att_typid = att->atttypid;

        if (att->attisdropped)
            continue;

        /* use previously cached data, if any */
        if (attr_widths != NULL && attr_widths[i] > 0) {
            tuple_width += attr_widths[i];
            continue;
        }

        /* This should match set_rel_width() in costsize.c */
        item_width = get_attavgwidth(RelationGetRelid(rel), i, isPartition);
        if (item_width <= 0) {
            item_width = get_typavgwidth(att_typid, att_typmod);
            if (unlikely(item_width <= 0)) {
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("Expected positive width estimation.")));
            }
        }
        if (attr_widths != NULL)
            attr_widths[i] = item_width;

        if (vectorized) {
            if (COL_IS_ENCODE((int)att_typid)) {
                hasencoded = true;
                tuple_width += alloc_trunk_size(item_width);
            }
        } else
            tuple_width += item_width;
    }

    if (vectorized)
        tuple_width += sizeof(Datum) * (i + (hasencoded ? 1 : 0));

    return tuple_width;
}

/*
 * get_relation_data_width
 *
 * External API for get_rel_data_width: same behavior except we have to
 * open the relcache entry.
 */
int32 get_relation_data_width(Oid relid, Oid partid, int32* attr_widths, bool vectorized)
{
    int32 result;
    Relation relation;
    Partition partition = NULL;
    Relation targetrel = NULL;

    /* As above, assume relation is already locked */
    relation = heap_open(relid, NoLock);

    if (OidIsValid(partid)) {
        partition = partitionOpen(relation, partid, NoLock);
        targetrel = partitionGetRelation(relation, partition);
    } else {
        targetrel = relation;
    }

    result = get_rel_data_width(targetrel, attr_widths, vectorized);

    if (OidIsValid(partid)) {
        releaseDummyRelation(&targetrel);
        partitionClose(relation, partition, NoLock);
    }

    heap_close(relation, NoLock);

    return result;
}

int32 getPartitionDataWidth(Relation partRel, int32* attr_widths)
{
    return get_rel_data_width(partRel, attr_widths);
}

#define REL_GET_ITH_ATT(rel, i) (rel->rd_att->attrs[i])

int32 getIdxDataWidth(Relation rel, IndexInfo* info, bool vectorized)
{
    int32 width = 0;
    bool isPartition = RelationIsPartition(rel);
    bool hasencoded = false;
    int i = 0;
    int expr_i = 0;

    for (i = 0; i < info->ii_NumIndexAttrs; i++) {
        AttrNumber attnum = info->ii_KeyAttrNumbers[i];
        Oid typid = InvalidOid;
        int32 item_width = 0;

        if (attnum > 0) {
            /* This should match set_rel_width() in costsize.c */
            item_width = get_attavgwidth(RelationGetRelid(rel), attnum, isPartition);
            if (item_width <= 0) {
                item_width = get_typavgwidth(
                    REL_GET_ITH_ATT(rel, attnum - 1).atttypid, REL_GET_ITH_ATT(rel, attnum - 1).atttypmod);
                AssertEreport(item_width > 0, MOD_OPT, "");
            }
            typid = REL_GET_ITH_ATT(rel, attnum - 1).atttypid;
        } else if (expr_i < list_length(info->ii_Expressions)) {
            Node* expr = (Node*)list_nth(info->ii_Expressions, expr_i);
            typid = exprType(expr);
            item_width = get_typavgwidth(typid, exprTypmod(expr));
            expr_i++;
        }

        if (vectorized) {
            if (COL_IS_ENCODE(typid)) {
                hasencoded = true;
                width += alloc_trunk_size(item_width);
            }
        } else
            width += item_width;
    }

    if (vectorized) {
        /* we include index columns, index ctid, and sort column for encoded column */
        int numCol = info->ii_NumIndexAttrs + 1 + (hasencoded ? 1 : 0);
        width += alloc_trunk_size(sizeof(Datum) * numCol) + alloc_trunk_size(sizeof(uint8) * numCol);
    }

    return width;
}

/*
 * get_relation_constraints
 *
 * Retrieve the validated CHECK constraint expressions of the given relation.
 *
 * Returns a List (possibly empty) of constraint expressions.  Each one
 * has been canonicalized, and its Vars are changed to have the varno
 * indicated by rel->relid.  This allows the expressions to be easily
 * compared to expressions taken from WHERE.
 *
 * If include_notnull is true, "col IS NOT NULL" expressions are generated
 * and added to the result for each column that's marked attnotnull.
 *
 * Note: at present this is invoked at most once per relation per planner
 * run, and in many cases it won't be invoked at all, so there seems no
 * point in caching the data in RelOptInfo.
 */
static List* get_relation_constraints(PlannerInfo* root, Oid relationObjectId, RelOptInfo* rel, bool include_notnull)
{
    List* result = NIL;
    Index varno = rel->relid;
    Relation relation;
    TupleConstr* constr = NULL;

    /*
     * We assume the relation has already been safely locked.
     */
    relation = heap_open(relationObjectId, NoLock);

    constr = relation->rd_att->constr;
    if (constr != NULL) {
        int num_check = constr->num_check;
        int i;

        for (i = 0; i < num_check; i++) {
            Node* cexpr = NULL;

            /*
             * If this constraint hasn't been fully validated yet, we must
             * ignore it here.
             */
            if (!constr->check[i].ccvalid)
                continue;

            cexpr = (Node*)stringToNode(constr->check[i].ccbin);

            /*
             * Run each expression through const-simplification and
             * canonicalization.  This is not just an optimization, but is
             * necessary, because we will be comparing it to
             * similarly-processed qual clauses, and may fail to detect valid
             * matches without this.  This must match the processing done to
             * qual clauses in preprocess_expression()!  (We can skip the
             * stuff involving subqueries, however, since we don't allow any
             * in check constraints.)
             */
            cexpr = eval_const_expressions(root, cexpr);

            cexpr = (Node*)canonicalize_qual((Expr*)cexpr, true);

            /* Fix Vars to have the desired varno */
            if (varno != 1)
                ChangeVarNodes(cexpr, 1, varno, 0);

            /*
             * Finally, convert to implicit-AND format (that is, a List) and
             * append the resulting item(s) to our output list.
             */
            result = list_concat(result, make_ands_implicit((Expr*)cexpr));
        }

        /* Add NOT NULL constraints in expression form, if requested */
        if (include_notnull && constr->has_not_null) {
            int natts = relation->rd_att->natts;

            for (i = 1; i <= natts; i++) {
                Form_pg_attribute att = &relation->rd_att->attrs[i - 1];

                if (att->attnotnull && !att->attisdropped) {
                    NullTest* ntest = makeNode(NullTest);

                    ntest->arg = (Expr*)makeVar(varno, i, att->atttypid, att->atttypmod, att->attcollation, 0);
                    ntest->nulltesttype = IS_NOT_NULL;
                    
                    /*
                     * argisrow=false is correct even for a composite column,
                     * because attnotnull does not represent a SQL-spec IS NOT
                     * NULL test in such a case, just IS DISTINCT FROM NULL.
                     */
                    ntest->argisrow = false;
                    result = lappend(result, ntest);
                }
            }
        }
    }

    heap_close(relation, NoLock);

    return result;
}

/*
 * relation_excluded_by_constraints
 *
 * Detect whether the relation need not be scanned because it has either
 * self-inconsistent restrictions, or restrictions inconsistent with the
 * relation's validated CHECK constraints.
 *
 * Note: this examines only rel->relid, rel->reloptkind, and
 * rel->baserestrictinfo; therefore it can be called before filling in
 * other fields of the RelOptInfo.
 */
bool relation_excluded_by_constraints(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    List* safe_restrictions = NIL;
    List* constraint_pred = NIL;
    List* safe_constraints = NIL;
    ListCell* lc = NULL;

    /* Skip the test if constraint exclusion is disabled for the rel */
    if (u_sess->attr.attr_sql.constraint_exclusion == CONSTRAINT_EXCLUSION_OFF ||
        (u_sess->attr.attr_sql.constraint_exclusion == CONSTRAINT_EXCLUSION_PARTITION &&
            !(rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
                (root->hasInheritedTarget && rel->reloptkind == RELOPT_BASEREL &&
                    rel->relid == (unsigned int)linitial2_int(root->parse->resultRelations)))))
        return false;

    /*
     * Check for self-contradictory restriction clauses.  We dare not make
     * deductions with non-immutable functions, but any immutable clauses that
     * are self-contradictory allow us to conclude the scan is unnecessary.
     *
     * Note: strip off RestrictInfo because predicate_refuted_by() isn't
     * expecting to see any in its predicate argument.
     */
    foreach (lc, rel->baserestrictinfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (!contain_mutable_functions((Node*)rinfo->clause))
            safe_restrictions = lappend(safe_restrictions, rinfo->clause);
    }

    if (predicate_refuted_by(safe_restrictions, safe_restrictions, false))
        return true;

    /* Only plain relations have constraints */
    if (rte->rtekind != RTE_RELATION || rte->inh)
        return false;

    /*
     * OK to fetch the constraint expressions.	Include "col IS NOT NULL"
     * expressions for attnotnull columns, in case we can refute those.
     */
    constraint_pred = get_relation_constraints(root, rte->relid, rel, true);

    /*
     * We do not currently enforce that CHECK constraints contain only
     * immutable functions, so it's necessary to check here. We daren't draw
     * conclusions from plan-time evaluation of non-immutable functions. Since
     * they're ANDed, we can just ignore any mutable constraints in the list,
     * and reason about the rest.
     */
    safe_constraints = NIL;
    foreach (lc, constraint_pred) {
        Node* pred = (Node*)lfirst(lc);

        if (!contain_mutable_functions(pred))
            safe_constraints = lappend(safe_constraints, pred);
    }

    /*
     * The constraints are effectively ANDed together, so we can just try to
     * refute the entire collection at once.  This may allow us to make proofs
     * that would fail if we took them individually.
     *
     * Note: we use rel->baserestrictinfo, not safe_restrictions as might seem
     * an obvious optimization.  Some of the clauses might be OR clauses that
     * have volatile and nonvolatile subclauses, and it's OK to make
     * deductions with the nonvolatile parts.
     */
    if (predicate_refuted_by(safe_constraints, rel->baserestrictinfo, false))
        return true;

    return false;
}

/*
 * build_physical_tlist
 *
 * Build a targetlist consisting of exactly the relation's user attributes,
 * in order.  The executor can special-case such tlists to avoid a projection
 * step at runtime, so we use such tlists preferentially for scan nodes.
 *
 * Exception: if there are any dropped columns, we punt and return NIL.
 * Ideally we would like to handle the dropped-column case too.  However this
 * creates problems for ExecTypeFromTL, which may be asked to build a tupdesc
 * for a tlist that includes vars of no-longer-existent types.	In theory we
 * could dig out the required info from the pg_attribute entries of the
 * relation, but that data is not readily available to ExecTypeFromTL.
 * For now, we don't apply the physical-tlist optimization when there are
 * dropped cols.
 *
 * We also support building a "physical" tlist for subqueries, functions,
 * values lists, and CTEs, since the same optimization can occur in
 * SubqueryScan, FunctionScan, ValuesScan, CteScan, and WorkTableScan nodes.
 */
List* build_physical_tlist(PlannerInfo* root, RelOptInfo* rel)
{
    List* tlist = NIL;
    Index varno = rel->relid;
    RangeTblEntry* rte = planner_rt_fetch(varno, root);
    Relation relation;
    Query* subquery = NULL;
    Var* var = NULL;
    ListCell* l = NULL;
    int attrno, numattrs;
    List* colvars = NIL;

    switch (rte->rtekind) {
        case RTE_RELATION:
            /* Assume we already have adequate lock */
            relation = heap_open(rte->relid, NoLock);

            numattrs = RelationGetNumberOfAttributes(relation);
            for (attrno = 1; attrno <= numattrs; attrno++) {
                Form_pg_attribute att_tup = &relation->rd_att->attrs[attrno - 1];

                if (att_tup->attisdropped) {
                    /* found a dropped col, so punt */
                    tlist = NIL;
                    break;
                }

                var = makeVar(varno, attrno, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);

                tlist = lappend(tlist, makeTargetEntry((Expr*)var, attrno, NULL, false));
            }

            heap_close(relation, NoLock);
            break;

        case RTE_SUBQUERY:
            subquery = rte->subquery;
            foreach (l, subquery->targetList) {
                TargetEntry* tle = (TargetEntry*)lfirst(l);

                /*
                 * A resjunk column of the subquery can be reflected as
                 * resjunk in the physical tlist; we need not punt.
                 */
                var = makeVarFromTargetEntry(varno, tle);

                tlist = lappend(tlist, makeTargetEntry((Expr*)var, tle->resno, NULL, tle->resjunk));
            }
            break;

        case RTE_FUNCTION:
        case RTE_VALUES:
        case RTE_CTE:
        case RTE_RESULT:
            /* incase of CTE, we need check and set swConverted fields */
            if (rte->rtekind == RTE_CTE && !rte->swConverted) {
                rte->swConverted = IsRteForStartWith(root, rte);
            }

            /* Not all of these can have dropped cols, but share code anyway */
            expandRTE(rte, varno, 0, -1, true /* include dropped */, NULL, &colvars);
            foreach (l, colvars) {
                var = (Var*)lfirst(l);
                /*
                 * A non-Var in expandRTE's output means a dropped column;
                 * must punt.
                 */
                if (!IsA(var, Var)) {
                    tlist = NIL;
                    break;
                }

                tlist = lappend(tlist, makeTargetEntry((Expr*)var, var->varattno, NULL, false));
            }

            /* in case sw-converted CTE, we need fix the tlist */
            if (rte->rtekind == RTE_CTE && rte->swConverted) {
                tlist = FixSwTargetlistResname(root, rte, tlist);
            }

            break;

        default:
            /* caller error */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unsupported RTE kind %d in build_physical_tlist", (int)rte->rtekind)));
            break;
    }

    return tlist;
}

/*
 * @Brief: check it the given rte is the rte for StartWith
 */
bool IsRteForStartWith(PlannerInfo *root, RangeTblEntry *rte)
{
    PlannerInfo *cteroot = NULL;
    bool result = false;

    /* return false if not a CTE entry */
    if (rte->rtekind != RTE_CTE) {
        return false;
    }

    /* Return true */
    if (rte->swConverted) {
        return true;
    }

    /* check and set fromStartWithConverted */
    ListCell *lc = NULL;
    int levelsup = rte->ctelevelsup;
    cteroot = get_cte_root(root, levelsup, rte->ctename);

    /* search CTE expr and check if it is sw converted case */
    foreach (lc, cteroot->parse->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);
        if (!cte->cterecursive) {
            continue;
        }

        if (strcmp(cte->ctename, rte->ctename) == 0 && cte->swoptions != NULL) {
            result = true;
            break;
        }
    }

    return result;
}

/*
 * mark_index_col
 *     mark real index column in g_index_vars.
 *     vars with type conversion in g_index_vars are from parser.
 */
static void mark_index_col(Oid relid, AttrNumber attno, Oid indexoid)
{
    if (g_index_vars == NULL || attno < 0)
        return;

    ListCell* lc = NULL;
    foreach (lc, g_index_vars) {
        IndexVar* var = (IndexVar*)lfirst(lc);

        if (var->relid == relid && var->attno == attno) {
            var->indexcol = true;
            var->indexoids = lappend_oid(var->indexoids, indexoid);
            break;
        }
    }
}

/*
 * build_index_tlist
 *
 * Build a targetlist representing the columns of the specified index.
 * Each column is represented by a Var for the corresponding base-relation
 * column, or an expression in base-relation Vars, as appropriate.
 *
 * There are never any dropped columns in indexes, so unlike
 * build_physical_tlist, we need no failure case.
 */
List* build_index_tlist(PlannerInfo* root, IndexOptInfo* index, Relation heapRelation)
{
    List* tlist = NIL;
    Index varno = index->rel->relid;
    ListCell* indexpr_item = NULL;
    int i;

    indexpr_item = list_head(index->indexprs);
    for (i = 0; i < index->ncolumns; i++) {
        int indexkey = index->indexkeys[i];
        Expr* indexvar = NULL;

        if (indexkey != 0) {
            /* simple column */
            Form_pg_attribute att_tup;

            if (indexkey < 0) {
                att_tup = SystemAttributeDefinition(indexkey, heapRelation->rd_rel->relhasoids, 
                    RELATION_HAS_BUCKET(heapRelation), RELATION_HAS_UIDS(heapRelation));
            } else {
                att_tup = &heapRelation->rd_att->attrs[indexkey - 1];
            }

            indexvar = (Expr*)makeVar(varno, indexkey, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);

            if (enable_check_implicit_cast()) {
                mark_index_col(heapRelation->rd_id, (AttrNumber)indexkey, index->indexoid);
            }
        } else {
            /* expression column */
            if (indexpr_item == NULL) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("wrong number of index expressions")));
            }
            indexvar = (Expr*)lfirst(indexpr_item);
            indexpr_item = lnext(indexpr_item);
        }

        tlist = lappend(tlist, makeTargetEntry(indexvar, i + 1, NULL, false));
    }
    if (indexpr_item != NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("wrong number of index expressions")));
    }

    return tlist;
}

static void FixWildcardSymbol(char** src)
{
    size_t lenSrc = strlen(*src);
    /* Resize to 2len + 1 when all symbol are wildcards that needs an extra '\' sign, 1 is for the trailing ending. */
    size_t lenResize = 2 * lenSrc + 1;
    errno_t rc = EOK;
    const char* symbols = "_%\\";
    size_t loc = 0;
    size_t cnt = 0;
    *src = (char*)repalloc(*src, sizeof(char) * lenResize);
    rc = memset_s(*src + lenSrc, lenResize - lenSrc, '\0', lenResize - lenSrc);
    securec_check(rc, "", "");
    while ((*src)[loc] != '\0') {
        if (strchr(symbols, (*src)[loc]) != NULL) {
            rc = memmove_s((*src) + loc + 1, lenResize - loc - 1, (*src) + loc, lenSrc - loc + cnt);
            securec_check(rc, "", "");
            (*src)[loc] = '\\';
            cnt++;
            loc++;
        }
        loc++;
    }
}

/*
 * generate a substituted function args for pattern match selectivity.
 * caller should free the returned list after use.
 */
static List* GetInstrPatternArgs(const char* fmt, Node* func)
{
    Node* funcArg1 = (Node*)linitial(((FuncExpr*)func)->args);
    char* instrConstArg = text_to_cstring(DatumGetTextPP(((Const*)lsecond(((FuncExpr*)func)->args))->constvalue));
    FixWildcardSymbol(&instrConstArg);
    const int extraDigits = 3;
    size_t lenPatternlikeConstArg = strlen(instrConstArg) + extraDigits;
    char* patternlikeConstArg = (char*)palloc0(lenPatternlikeConstArg);
    errno_t rc = sprintf_s(patternlikeConstArg, lenPatternlikeConstArg, fmt, instrConstArg);
    securec_check_ss(rc, "", "");
    Const* constArg = (Const*)copyObject(lsecond(((FuncExpr*)func)->args));
    Pointer p = DatumGetPointer(constArg->constvalue);
    pfree_ext(p);
    constArg->constvalue = PointerGetDatum(cstring_to_text(patternlikeConstArg));
    pfree_ext(instrConstArg);
    if (IsA(funcArg1, Var) || IsA(funcArg1, RelabelType)) {
        return list_make2(linitial(((FuncExpr*)func)->args), (Node*)constArg);
    } else { /* CheckInstrRICanOpt ensures that first arg can only be var(bpchar/name)::text */
        return list_make2(linitial(((FuncExpr*)funcArg1)->args), (Node*)constArg);
    }
}


static Oid GetInstrPatternOprcode(const Node* func, bool like)
{
    Node* funcArg1 = (Node*)linitial(((FuncExpr*)func)->args);
    if (IsA(funcArg1, Var) || IsA(funcArg1, RelabelType)) {
        return like ? OID_TEXT_LIKE_OP : TEXTNOTLIKEOID;
    } else if (((FuncExpr*)funcArg1)->funcid == RTRIM1FUNCOID) {
        return like ? OID_BPCHAR_LIKE_OP : OID_BPCHAR_NOT_LIKE_OP;
    } else if (((FuncExpr*)funcArg1)->funcid == NAME2TEXTFUNCOID) {
        return like ? OID_NAME_LIKE_OP : OID_NAME_NOT_LIKE_OP;
    } else { /* should not be here */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("invalid instr argument")));
        return InvalidOid; /* to keep compiler quiet */
    }
}

static bool CheckInstrRICanOpt(const Node* funcSide, const Node* constSide)
{
    /* need to be instr() <opr> const or const <opr> instr() */
    if (!IsA(funcSide, FuncExpr) || ((FuncExpr*)funcSide)->funcid != INSTR2FUNCOID || !IsA(constSide, Const)) {
        return false;
    }

    /* only handle int operend */
    if (((Const*)constSide)->consttype != INT4OID) {
        return false;
    }

    Node* funcArg1 = (Node*)linitial(((FuncExpr*)funcSide)->args);
    Node* funcArg2 = (Node*)lsecond(((FuncExpr*)funcSide)->args);

    /* do not handle instr(var1, var2) or instr('xxx', var) */
    if (!(IsA(funcArg1, Var) || IsA(funcArg1, RelabelType) || IsA(funcArg1, FuncExpr)) || !IsA(funcArg2, Const)) {
        return false;
    }

    /* if instr's first arg is not var, only accept bpchar and name to text typecast */
    if (IsA(funcArg1, FuncExpr) &&
        ((((FuncExpr*)funcArg1)->funcid != RTRIM1FUNCOID && ((FuncExpr*)funcArg1)->funcid != NAME2TEXTFUNCOID) ||
        !(IsA(linitial(((FuncExpr*)funcArg1)->args), Var)))) {
        return false;
    }

    return true;
}

enum { EXPR_LT_CONST, EXPR_LE_CONST, EXPR_EQ_CONST, EXPR_GE_CONST, EXPR_GT_CONST, INSTR_CASE_INVALID };

static unsigned int GetInstrRestrictPattern(Oid operatorid, bool varOnLeft)
{
    const unsigned int varOnLeftMirror = EXPR_GT_CONST;
    unsigned int res = INSTR_CASE_INVALID;
    switch (operatorid) {
        case INT4LTOID:
            res = EXPR_LT_CONST;
            break;
        case INT4LEOID:
            res = EXPR_LE_CONST;
            break;
        case INT4EQOID:
            res = EXPR_EQ_CONST;
            break;
        case INT4GEOID:
            res = EXPR_GE_CONST;
            break;
        case INT4GTOID:
            res = EXPR_GT_CONST;
            break;
        default:
            return INSTR_CASE_INVALID;
    }
    if (!varOnLeft) {
        res = varOnLeftMirror - res;
    }
    return res;
}

/*
 * check if restriction contains function instr(var, '<pattern>') and handle accordingly.
 * valid cases are:
 *  +------------------------+----------------------+
 *  | instr                  | pattern match        |
 *  +------------------------+----------------------+
 *  | instr(var, 'xxx') = 0  | var not like '%xxx%' |
 *  +------------------------+----------------------+
 *  | instr(var, 'xxx') <= 0 | var not like '%xxx%' |
 *  +------------------------+----------------------+
 *  | instr(var, 'xxx') > 0  | var like '%xxx%'     |
 *  +------------------------+----------------------+
 *  | instr(var, 'xxx') < 1  | var not like '%xxx%' |
 *  +------------------------+----------------------+
 *  | instr(var, 'xxx') = 1  | var like 'xxx%'      |
 *  +------------------------+----------------------+
 */
static bool InstrAsPatternMatch(Oid* operatorid, List** args)
{
    if (list_length(*args) != 2) {
        return false;
    }
    bool valid = false;
    bool varOnLeft = IsA((Node*)lsecond(*args), Const);
    Node* constSide = varOnLeft ? (Node*)lsecond(*args) : (Node*)linitial(*args);
    Node* funcSide = varOnLeft ? (Node*)linitial(*args) : (Node*)lsecond(*args);

    if (!CheckInstrRICanOpt(funcSide, constSide)) {
        return false;
    }

    List* patternselArgs = NIL;
    Oid patternselOprcode = InvalidOid;
    int instrCond = DatumGetInt32(((Const*)constSide)->constvalue);

    unsigned int pattern = GetInstrRestrictPattern(*operatorid, varOnLeft);
    if (pattern == INSTR_CASE_INVALID) {
        return false;
    }

    if (instrCond == 0) {
        if (pattern == EXPR_EQ_CONST || pattern == EXPR_LE_CONST) {
            /* estimate instr(var, 'XXX') = 0 as var not like 'XXX' */
            patternselArgs = GetInstrPatternArgs("%%%s%%", funcSide);
            patternselOprcode = GetInstrPatternOprcode(funcSide, false);
            valid = true;
        } else if (pattern == EXPR_GT_CONST) {
            /* estimate instr(var, 'XXX') > 0 as var like '%XXX%' */
            patternselArgs = GetInstrPatternArgs("%%%s%%", funcSide);
            patternselOprcode = GetInstrPatternOprcode(funcSide, true);
            valid = true;
        }
    } else if (instrCond == 1) {
        if (pattern == EXPR_EQ_CONST) {
            /* estimate instr(var, 'XXX') = 1 as var like 'XXX%' */
            patternselArgs = GetInstrPatternArgs("%s%%", funcSide);
            patternselOprcode = GetInstrPatternOprcode(funcSide, true);
            valid = true;
        } else if (pattern == EXPR_LT_CONST) {
            /* estimate instr(var, 'XXX') < 1 as var not like '%XXX%' */
            patternselArgs = GetInstrPatternArgs("%%%s%%", funcSide);
            patternselOprcode = GetInstrPatternOprcode(funcSide, false);
            valid = true;
        }
    }
    if (valid) {
        *operatorid = patternselOprcode;
        *args = patternselArgs;
    }
    return valid;
}

/*
 * restriction_selectivity
 *
 * Returns the selectivity of a specified restriction operator clause.
 * This code executes registered procedures stored in the
 * operator relation, by calling the function manager.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 */
Selectivity restriction_selectivity(PlannerInfo* root, Oid operatorid, List* args, Oid inputcollid, int varRelid)
{
    float8 result;
    int tmp_encoding = PG_INVALID_ENCODING;
    int db_encoding = PG_INVALID_ENCODING;
    bool useInstrOpt = false;
    if (ENABLE_SQL_BETA_FEATURE(SEL_EXPR_INSTR)) {
        useInstrOpt = InstrAsPatternMatch(&operatorid, &args);
    }

    RegProcedure oprrest = get_oprrest(operatorid);
    /*
     * if the oprrest procedure is missing for whatever reason, use a
     * selectivity of 0.5
     */
    if (!oprrest)
        return (Selectivity)0.5;
    if (oprrest == F_SCALARLTSEL) {
        result = scalarltsel_internal(root, operatorid, args, varRelid);
    } else {
        result = DatumGetFloat8(OidFunctionCall4Coll(oprrest, inputcollid, PointerGetDatum(root),
                                                     ObjectIdGetDatum(operatorid), PointerGetDatum(args),
                                                     Int32GetDatum(varRelid)));
    }

    if (DB_IS_CMPT(B_FORMAT)) {
        tmp_encoding = get_valid_charset_by_collation(inputcollid);
        db_encoding = GetDatabaseEncoding();
    }

    if (tmp_encoding != db_encoding) {
        DB_ENCODING_SWITCH_TO(tmp_encoding);
        result = DatumGetFloat8(OidFunctionCall4Coll(oprrest,
            inputcollid,
            PointerGetDatum(root),
            ObjectIdGetDatum(operatorid),
            PointerGetDatum(args),
            Int32GetDatum(varRelid)));
        DB_ENCODING_SWITCH_BACK(db_encoding);
    } else {
        result = DatumGetFloat8(OidFunctionCall4Coll(oprrest,
            inputcollid,
            PointerGetDatum(root),
            ObjectIdGetDatum(operatorid),
            PointerGetDatum(args),
            Int32GetDatum(varRelid)));
    }

    if (useInstrOpt) {
        list_free_ext(args);
    }

    if (result < 0.0 || result > 1.0)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("invalid restriction selectivity: %f", result)));

    return (Selectivity)result;
}

/*
 * join_selectivity
 *
 * Returns the selectivity of a specified join operator clause.
 * This code executes registered procedures stored in the
 * operator relation, by calling the function manager.
 */
Selectivity join_selectivity(
    PlannerInfo* root, Oid operatorid, List* args, Oid inputcollid, JoinType jointype, SpecialJoinInfo* sjinfo)
{
    RegProcedure oprjoin = get_oprjoin(operatorid);
    float8 result;

    /*
     * if the oprjoin procedure is missing for whatever reason, use a
     * selectivity of 0.5
     */
    if (!oprjoin)
        return (Selectivity)0.5;

    result = DatumGetFloat8(OidFunctionCall5Coll(oprjoin,
        inputcollid,
        PointerGetDatum(root),
        ObjectIdGetDatum(operatorid),
        PointerGetDatum(args),
        Int16GetDatum(jointype),
        PointerGetDatum(sjinfo)));
    if (result < 0.0 || result > 1.0)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("invalid join selectivity: %f", result)));

    return (Selectivity)result;
}

/*
 * has_unique_index
 *
 * Detect whether there is a unique index on the specified attribute
 * of the specified relation, thus allowing us to conclude that all
 * the (non-null) values of the attribute are distinct.
 *
 * This function does not check the index's indimmediate property, which
 * means that uniqueness may transiently fail to hold intra-transaction.
 * That's appropriate when we are making statistical estimates, but beware
 * of using this for any correctness proofs.
 */
bool has_unique_index(RelOptInfo* rel, AttrNumber attno)
{
    ListCell* ilist = NULL;

    foreach (ilist, rel->indexlist) {
        IndexOptInfo* index = (IndexOptInfo*)lfirst(ilist);

        /*
         * Note: ignore partial indexes, since they don't allow us to conclude
         * that all attr values are distinct, *unless* they are marked predOK
         * which means we know the index's predicate is satisfied by the
         * query. We don't take any interest in expressional indexes either.
         * Also, a multicolumn unique index doesn't allow us to conclude that
         * just the specified attr is unique.
         */
        if (index->unique && index->nkeycolumns == 1 && index->indexkeys[0] == attno &&
            (index->indpred == NIL || index->predOK))
            return true;
    }
    return false;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: estimate # pages and # tuples in a partition, and get the
 *			: tablespace that the partition locates
 * Description	:
 * Notes		: simulate the caculation of the ordinary table
 */
void estimatePartitionSize(
    Relation relation, Oid partitionid, int32* attr_widths, RelPageType* pages, double* tuples, double* allvisfrac)
{
    BlockNumber curpages = 0;
    BlockNumber partitionpages = 0;
    double partitiontuples = 0;
    BlockNumber partitionallvisible = 0;
    double density = 0.0;
    // partition already locked by caller
    Partition partition = partitionOpen(relation, partitionid, NoLock);

    /* calculate the number of blocks in the partition */
    curpages = PartitionGetNumberOfBlocks(relation, partition);
    /*
     * use a minimum size estimate of 10 pages.  The idea here is to avoid assuming
     *  a newly-created table is really small, even if it currently is, because that may
     * not be true once some data gets loaded into it.	Once a vacuum or analyze
     * cycle has been done on it, it's more reasonable to believe the size is somewhat
     * stable.
     */
    if (curpages < 10 && !partition->pd_part->relpages && PartitionIsTablePartition(partition)) {
        curpages = 10;
    }

    /* report estimated # pages */
    *pages = curpages;

    /* quick exit if partition is clearly empty */
    if (curpages == 0) {
        *tuples = 0;
        *allvisfrac = 0;
        partitionClose(relation, partition, NoLock);
        return;
    }

    /* coerce values in pg_partition to more desirable types */
    partitionpages = (BlockNumber)partition->pd_part->relpages;
    partitiontuples = (double)partition->pd_part->reltuples;
    partitionallvisible = (BlockNumber)partition->pd_part->relallvisible;

    /*
     * If it's an index, discount the metapage while estimating the number of tuples.
     * This is a kluge because it assumes more than it ought to about index structure.
     * Currently it's OK for btree, hash, and GIN indexes but suspect for GiST indexes.
     */
    if (partitionpages > 0 && PartitionIsIndexPartition(partition)) {
        curpages--;
        partitionpages--;
    }

    /* estimate number of tuples from previous tuple density */
    if (partitionpages > 0) {
        density = partitiontuples / (double)partitionpages;
    } else {
        /*
         * When we have no data because the partition was truncated,
         * estimate tuple width from attribute datatypes.  We assume
         * here that the pages are completely full, which is OK for
         * tables (since they've presumably not been VACUUMed yet) but
         * is probably an overestimate for indexes.  Fortunately
         * get_relation_info() can clamp the overestimate to the
         * parent table's size.
         *
         * Note: this code intentionally disregards alignment
         * considerations, because (a) that would be gilding the lily
         * considering how crude the estimate is, and (b) it creates
         * platform dependencies in the default plans which are kind
         * of a headache for regression testing.
         */
        int32 tuple_width;
        Relation fakerel = partitionGetRelation(relation, partition);

        tuple_width = get_rel_data_width(fakerel, attr_widths);
        tuple_width += sizeof(HeapTupleHeaderData);
        tuple_width += sizeof(ItemPointerData);

        density = (BLCKSZ - SizeOfPageHeaderData) / (double)tuple_width;
        releaseDummyRelation(&fakerel);
    }

    *tuples = rint(density * (double)curpages);

    /*
     * We use relallvisible as-is, rather than scaling it up like we
     * do for the pages and tuples counts, on the theory that any
     * pages added since the last VACUUM are most likely not marked
     * all-visible.  But costsize.c wants it converted to a fraction.
     */
    if (partitionallvisible == 0 || curpages == 0) {
        *allvisfrac = 0;
    } else if ((double)partitionallvisible >= curpages) {
        *allvisfrac = 1;
    } else {
        *allvisfrac = (double)partitionallvisible / curpages;
    }

    partitionClose(relation, partition, NoLock);
}

/*
 * Brief        : Set the relation store information.
 * Input        : relOptInfo, the RelOptInfo stuct.
 *                relaiton, the relation to be seted.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void setRelStoreInfo(RelOptInfo* relOptInfo, Relation relation)
{
    AssertEreport(relation != NULL, MOD_OPT, "Relation is null.");

    relOptInfo->is_ustore = RelationIsUstoreFormat(relation);

    if (RelationIsColStore(relation)) {
        /*
         * This is a column store table.
         */
        /*
         * Set store location type.
         */
        relOptInfo->relStoreLocation = LOCAL_STORE;

        /*
         * Set store format type.
         */
        if (RelationIsPAXFormat(relation)) {
            relOptInfo->orientation = REL_PAX_ORIENTED;
        } else {
            AssertEreport(RelationIsCUFormat(relation), MOD_OPT, "Unexpected relation store format.");
            relOptInfo->orientation = REL_COL_ORIENTED;
        }
#ifdef ENABLE_MULTIPLE_NODES
    } else if(RelationIsTsStore(relation)) {
        relOptInfo->orientation = REL_TIMESERIES_ORIENTED;
        relOptInfo->relStoreLocation = LOCAL_STORE;
#endif
    } else {
        /*
         *  This is a row store table.
         */
        relOptInfo->orientation = REL_ROW_ORIENTED;
        relOptInfo->relStoreLocation = LOCAL_STORE;
    }
}

PlannerInfo *get_cte_root(PlannerInfo *root, int levelsup, char *ctename)
{
    PlannerInfo *cteroot = root;
    while (levelsup-- > 0) {
        cteroot = cteroot->parent_root;
        if (cteroot == NULL) { /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("bad levelsup for CTE \"%s\"", ctename)));
        }
    }
    return cteroot;
}

#ifdef USE_SPQ
double spq_estimate_partitioned_numtuples(Relation rel)
{
    List *inheritors;
    ListCell *lc;
    double totaltuples;
 
    if (rel->rd_rel->reltuples > 0)
        return rel->rd_rel->reltuples;
 
    inheritors = find_all_inheritors(RelationGetRelid(rel), AccessShareLock, NULL);
    totaltuples = 0;
    foreach (lc, inheritors) {
        Oid childid = lfirst_oid(lc);
        Relation childrel;
        double childtuples;
 
        if (childid != RelationGetRelid(rel))
            childrel = try_table_open(childid, NoLock);
        else
            childrel = rel;
 
        childtuples = childrel->rd_rel->reltuples;
 
        if (childtuples == 0 && rel_is_external_table(RelationGetRelid(childrel))) {
#define DEFAULT_EXTERNAL_TABLE_TUPLES 1000000
            childtuples = DEFAULT_EXTERNAL_TABLE_TUPLES;
        }
        totaltuples += childtuples;
 
        if (childrel != rel)
            heap_close(childrel, NoLock);
    }
    return totaltuples;
}
 
#endif
