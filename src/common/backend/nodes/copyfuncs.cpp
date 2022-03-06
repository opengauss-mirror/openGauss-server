/* -------------------------------------------------------------------------
 *
 * copyfuncs.cpp
 *     Copy functions for openGauss tree nodes.
 *
 * NOTE: we currently support copying all node types found in parse and
 * plan trees. We do not support copying executor state trees; there
 * is no need for that, and no point in maintaining all the code that
 * would be needed.  We also do not support copying Path trees, mainly
 * because the circular linkages between RelOptInfo and Path nodes can't
 * be handled easily in a simple depth-first traversal.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *     src/common/backend/nodes/copyfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifdef FRONTEND_PARSER
#include "nodes/parsenodes_common.h"
#include "utils/elog.h"
typedef struct OpMemInfo {
    double  opMem;                                  /* op_work_mem in path phase */
    double  minMem;                      /* ideal memory usage with maximum disk */
    double  maxMem;                           /* ideal memory usage without disk */
    double  regressCost; /* performance degression ratio between min and max Mem */
} OpMemInfo;
#else
#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#ifdef PGXC
#include "parser/parse_hint.h"
#include "pgxc/execRemote.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgFdwRemote.h"
#include "optimizer/dataskew.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pgxcplan.h"
#endif
#include "utils/datum.h"
#include "utils/hotkey.h"
#include "optimizer/streamplan.h"
#include "bulkload/dist_fdw.h"
#include "storage/tcap.h"
#endif /* FRONTEND_PARSER */

/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos. Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname)      (newnode->fldname = from->fldname)

#define COPY_ARRAY_FIELD(fldname)                                                                      \
do {                                                                                                   \
    errno_t rc;                                                                                        \
    rc = memcpy_s(&newnode->fldname, sizeof(newnode->fldname), &from->fldname, sizeof(from->fldname)); \
    securec_check(rc, "", "");                                                                         \
} while (0)


/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname)                                              \
    do {                                                                      \
        void* ptr = copyObject(from->fldname);                                \
        errno_t rc;                                                           \
        rc = memcpy_s(&newnode->fldname, sizeof(void*), &ptr, sizeof(void*)); \
        securec_check(rc, "", "");                                            \
    } while (0)

/* Copy a field that is a pointer to a Bitmapset */
#define COPY_BITMAPSET_FIELD(fldname) (newnode->fldname = bms_copy(from->fldname))

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) (newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char*)NULL)

/* Copy a field that is a pointer to a simple palloc'd object of size sz */
#define COPY_POINTER_FIELD(fldname, sz)                                       \
    do {                                                                      \
        Size _size = (sz);                                                    \
        void* ptr = palloc(_size);                                            \
        errno_t rc;                                                           \
        rc = memcpy_s(&newnode->fldname, sizeof(void*), &ptr, sizeof(void*)); \
        securec_check(rc, "", "");                                            \
        rc = memcpy_s(newnode->fldname, _size, from->fldname, _size);         \
        securec_check(rc, "", "");                                            \
    } while (0)

/* Copy a parse location field (for Copy, this is same as scalar case) */
#define COPY_LOCATION_FIELD(fldname) (newnode->fldname = from->fldname)

#ifndef FRONTEND_PARSER
static void CopyMemInfoFields(const OpMemInfo* from, OpMemInfo* newnode);
static ReplicaIdentityStmt* _copyReplicaIdentityStmt(const ReplicaIdentityStmt* from);
#ifndef ENABLE_MULTIPLE_NODES
static AlterSystemStmt* _copyAlterSystemStmt(const AlterSystemStmt* from);
#endif
static void CopyCursorFields(const Cursor_Data* from, Cursor_Data* newnode);
/* ****************************************************************
 *                      plannodes.h copy functions
 * ****************************************************************
 */

/*
 * _copyPlannedStmt
 */
static PlannedStmt* _copyPlannedStmt(const PlannedStmt* from)
{
    PlannedStmt* newnode = makeNode(PlannedStmt);

    COPY_SCALAR_FIELD(commandType);
    COPY_SCALAR_FIELD(queryId);
    COPY_SCALAR_FIELD(hasReturning);
    COPY_SCALAR_FIELD(hasModifyingCTE);
    COPY_SCALAR_FIELD(canSetTag);
    COPY_SCALAR_FIELD(transientPlan);
    COPY_SCALAR_FIELD(dependsOnRole);
    COPY_NODE_FIELD(planTree);
    COPY_NODE_FIELD(rtable);
    COPY_NODE_FIELD(resultRelations);
    COPY_NODE_FIELD(utilityStmt);
    COPY_NODE_FIELD(subplans);
    COPY_BITMAPSET_FIELD(rewindPlanIDs);
    COPY_NODE_FIELD(rowMarks);
    COPY_NODE_FIELD(relationOids);
    COPY_NODE_FIELD(invalItems);
    COPY_SCALAR_FIELD(nParamExec);
    COPY_SCALAR_FIELD(num_streams);
    COPY_SCALAR_FIELD(num_nodes);
    if (from->nodesDefinition != NULL) {
        COPY_POINTER_FIELD(nodesDefinition, from->num_nodes * sizeof(*from->nodesDefinition));
    }
    COPY_SCALAR_FIELD(instrument_option);
    COPY_SCALAR_FIELD(num_plannodes);
    COPY_SCALAR_FIELD(query_mem[0]);
    COPY_SCALAR_FIELD(query_mem[1]);
    COPY_SCALAR_FIELD(assigned_query_mem[0]);
    COPY_SCALAR_FIELD(assigned_query_mem[1]);
    COPY_SCALAR_FIELD(num_bucketmaps);

    for (int i = 0; i < newnode->num_bucketmaps; i++) {
        if (from->bucketMap[i]) {
            COPY_POINTER_FIELD(bucketMap[i], sizeof(uint2) * from->bucketCnt[i]);
        } else {
            newnode->bucketMap[i] = NULL;
        }
        COPY_SCALAR_FIELD(bucketCnt[i]);
    }

    COPY_STRING_FIELD(query_string);
    COPY_NODE_FIELD(subplan_ids);
    COPY_NODE_FIELD(initPlan);
    /* data redistribution for DFS table. */
    COPY_SCALAR_FIELD(dataDestRelIndex);
    COPY_SCALAR_FIELD(query_dop);
    COPY_SCALAR_FIELD(MaxBloomFilterNum);
    COPY_SCALAR_FIELD(in_compute_pool);
    COPY_SCALAR_FIELD(has_obsrel);
    COPY_NODE_FIELD(noanalyze_rellist);
    COPY_SCALAR_FIELD(ng_use_planA);
    COPY_SCALAR_FIELD(gather_count);
    COPY_SCALAR_FIELD(isRowTriggerShippable);
    COPY_SCALAR_FIELD(is_stream_plan);
    COPY_SCALAR_FIELD(multi_node_hint);
    COPY_SCALAR_FIELD(uniqueSQLId);
    /*
     * Not copy ng_queryMem to avoid memory leak in CachedPlan context,
     * and dywlm_client_manager always calls CalculateQueryMemMain to generate it.
     */
    return newnode;
}

/*
 * CopyPlanFields
 *
 *		This function copies the fields of the Plan node.  It is used by
 *		all the copy functions for classes which inherit from Plan.
 */
static void CopyPlanFields(const Plan* from, Plan* newnode)
{
    COPY_SCALAR_FIELD(plan_node_id);
    COPY_SCALAR_FIELD(parent_node_id);
    COPY_SCALAR_FIELD(exec_type);
    COPY_SCALAR_FIELD(startup_cost);
    COPY_SCALAR_FIELD(total_cost);
    COPY_SCALAR_FIELD(plan_rows);
    COPY_SCALAR_FIELD(multiple);
    COPY_SCALAR_FIELD(plan_width);
    COPY_SCALAR_FIELD(dop);
    COPY_NODE_FIELD(targetlist);
    COPY_NODE_FIELD(qual);
    COPY_NODE_FIELD(lefttree);
    COPY_NODE_FIELD(righttree);
    COPY_SCALAR_FIELD(ispwj);
    COPY_SCALAR_FIELD(paramno);
    COPY_SCALAR_FIELD(subparamno);
    COPY_NODE_FIELD(initPlan);
    COPY_NODE_FIELD(distributed_keys);
    COPY_NODE_FIELD(exec_nodes);
    COPY_BITMAPSET_FIELD(extParam);
    COPY_BITMAPSET_FIELD(allParam);
    COPY_SCALAR_FIELD(vec_output);
    COPY_SCALAR_FIELD(hasUniqueResults);
    COPY_SCALAR_FIELD(isDeltaTable);
    COPY_SCALAR_FIELD(operatorMemKB[0]);
    COPY_SCALAR_FIELD(operatorMemKB[1]);
    COPY_SCALAR_FIELD(operatorMaxMem);
    COPY_SCALAR_FIELD(parallel_enabled);
    COPY_SCALAR_FIELD(hasHashFilter);
    COPY_SCALAR_FIELD(outerdistinct);
    COPY_SCALAR_FIELD(innerdistinct);
    COPY_NODE_FIELD(var_list);
    COPY_NODE_FIELD(filterIndexList);
    COPY_SCALAR_FIELD(dop);
    COPY_SCALAR_FIELD(recursive_union_plan_nodeid);
    COPY_SCALAR_FIELD(recursive_union_controller);
    COPY_SCALAR_FIELD(control_plan_nodeid);
    COPY_SCALAR_FIELD(is_sync_plannode);
    COPY_SCALAR_FIELD(pred_rows);
    COPY_SCALAR_FIELD(pred_startup_time);
    COPY_SCALAR_FIELD(pred_total_time);
    COPY_SCALAR_FIELD(pred_max_memory);
}

/*
 * _copyPlan
 */
static Plan* _copyPlan(const Plan* from)
{
    Plan* newnode = makeNode(Plan);

    /*
     * copy node superclass fields
     */
    CopyPlanFields(from, newnode);

    return newnode;
}

/*
 * _copyResult
 */
static BaseResult* _copyResult(const BaseResult* from)
{
    BaseResult* newnode = makeNode(BaseResult);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(resconstantqual);

    return newnode;
}

/*
 * _copyModifyTable
 */
static ModifyTable* _copyModifyTable(const ModifyTable* from)
{
    ModifyTable* newnode = makeNode(ModifyTable);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(operation);
    COPY_SCALAR_FIELD(canSetTag);
    COPY_NODE_FIELD(resultRelations);
    COPY_SCALAR_FIELD(resultRelIndex);
    COPY_NODE_FIELD(plans);
    COPY_NODE_FIELD(returningLists);
    COPY_NODE_FIELD(fdwPrivLists);
    COPY_NODE_FIELD(rowMarks);
    COPY_SCALAR_FIELD(epqParam);
    COPY_SCALAR_FIELD(partKeyUpdated);
#ifdef PGXC
    COPY_NODE_FIELD(remote_plans);
    COPY_NODE_FIELD(remote_insert_plans);
    COPY_NODE_FIELD(remote_update_plans);
    COPY_NODE_FIELD(remote_delete_plans);
#endif
    COPY_SCALAR_FIELD(is_dist_insertselect);
    COPY_NODE_FIELD(cacheEnt);

    COPY_SCALAR_FIELD(mergeTargetRelation);
    COPY_NODE_FIELD(mergeSourceTargetList);
    COPY_NODE_FIELD(mergeActionList);

    COPY_SCALAR_FIELD(upsertAction);
    COPY_NODE_FIELD(updateTlist);
    COPY_NODE_FIELD(exclRelTlist);
    COPY_SCALAR_FIELD(exclRelRTIndex);
    COPY_NODE_FIELD(upsertWhere);

    return newnode;
}

/*
 * _copyAppend
 */
static Append* _copyAppend(const Append* from)
{
    Append* newnode = makeNode(Append);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(appendplans);

    return newnode;
}

/*
 * _copyMergeAppend
 */
static MergeAppend* _copyMergeAppend(const MergeAppend* from)
{
    MergeAppend* newnode = makeNode(MergeAppend);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(mergeplans);
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(collations, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
    }

    return newnode;
}

/*
 * _copyPartIterator
 */
static PartIterator* _copyPartIterator(const PartIterator* from)
{
    PartIterator* newnode = makeNode(PartIterator);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(partType);
    COPY_SCALAR_FIELD(itrs);
    COPY_SCALAR_FIELD(direction);
    COPY_NODE_FIELD(param);

    return newnode;
}
/*
 * _copyPartIteratorParam
 */
static PartIteratorParam* _copyPartIteratorParam(const PartIteratorParam* from)
{
    PartIteratorParam* newnode = makeNode(PartIteratorParam);

    COPY_SCALAR_FIELD(paramno);
    COPY_SCALAR_FIELD(subPartParamno);
    return newnode;
}

/*
 * _copyRecursiveUnion
 */
static RecursiveUnion* _copyRecursiveUnion(const RecursiveUnion* from)
{
    RecursiveUnion* newnode = makeNode(RecursiveUnion);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(wtParam);
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(dupColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(dupOperators, from->numCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(numGroups);
    COPY_SCALAR_FIELD(has_inner_stream);
    COPY_SCALAR_FIELD(has_outer_stream);
    COPY_SCALAR_FIELD(is_used);
    COPY_SCALAR_FIELD(is_correlated);

    /* start with support */
    COPY_NODE_FIELD(internalEntryList);

    return newnode;
}

static StartWithOp *_copyStartWithOp(const StartWithOp *from)
{
    StartWithOp *newnode = makeNode(StartWithOp);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(cteplan);
    COPY_NODE_FIELD(ruplan);
    COPY_NODE_FIELD(keyEntryList);
    COPY_NODE_FIELD(colEntryList);
    COPY_NODE_FIELD(internalEntryList);
    COPY_NODE_FIELD(fullEntryList);

    COPY_NODE_FIELD(swoptions);

    COPY_SCALAR_FIELD(swExecOptions);
    COPY_NODE_FIELD(prcTargetEntryList);

    return newnode;
}

/*
 * _copyBitmapAnd
 */
static BitmapAnd* _copyBitmapAnd(const BitmapAnd* from)
{
    BitmapAnd* newnode = makeNode(BitmapAnd);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(bitmapplans);
    COPY_SCALAR_FIELD(is_ustore);

    return newnode;
}

/*
 * _copyBitmapOr
 */
static BitmapOr* _copyBitmapOr(const BitmapOr* from)
{
    BitmapOr* newnode = makeNode(BitmapOr);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(bitmapplans);
    COPY_SCALAR_FIELD(is_ustore);

    return newnode;
}

/*
 * _copyCStoreIndexAnd
 */
static CStoreIndexAnd* _copyCStoreIndexAnd(const CStoreIndexAnd* from)
{
    CStoreIndexAnd* newnode = makeNode(CStoreIndexAnd);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(bitmapplans);

    return newnode;
}

/*
 * _copyCStoreIndexOr
 */
static CStoreIndexOr* _copyCStoreIndexOr(const CStoreIndexOr* from)
{
    CStoreIndexOr* newnode = makeNode(CStoreIndexOr);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(bitmapplans);

    return newnode;
}

/*
 * _copyBucketInfo
 */
static BucketInfo* _copyBucketInfo(const BucketInfo* from)
{
    BucketInfo* newnode = makeNode(BucketInfo);

    COPY_NODE_FIELD(buckets);

    return newnode;
}

/*
 * CopyScanFields
 *
 *		This function copies the fields of the Scan node.  It is used by
 *		all the copy functions for classes which inherit from Scan.
 */
static void CopyScanFields(const Scan* from, Scan* newnode)
{
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(scanrelid);
    COPY_SCALAR_FIELD(isPartTbl);
    COPY_SCALAR_FIELD(itrs);
    COPY_SCALAR_FIELD(partScanDirection);

    newnode->pruningInfo = copyPruningResult(from->pruningInfo);
    COPY_SCALAR_FIELD(scan_qual_optimized);
    COPY_SCALAR_FIELD(predicate_pushdown_optimized);
    COPY_SCALAR_FIELD(scanBatchMode);
    COPY_SCALAR_FIELD(tableRows);

    /* partition infos */
    COPY_NODE_FIELD(bucketInfo);
    /* copy remainder of node.*/
    COPY_NODE_FIELD(tablesample);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);
    COPY_SCALAR_FIELD(is_inplace);
}

/*
 * _copyScan
 */
static Scan* _copyScan(const Scan* from)
{
    Scan* newnode = makeNode(Scan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    return newnode;
}

/*
 * _copySeqScan
 */
static SeqScan* _copySeqScan(const SeqScan* from)
{
    SeqScan* newnode = makeNode(SeqScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    return newnode;
}

/*
 * _copyIndexScan
 */
static IndexScan* _copyIndexScan(const IndexScan* from)
{
    IndexScan* newnode = makeNode(IndexScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(indexid);
    COPY_NODE_FIELD(indexqual);
    COPY_NODE_FIELD(indexqualorig);
    COPY_NODE_FIELD(indexorderby);
    COPY_NODE_FIELD(indexorderbyorig);
    COPY_SCALAR_FIELD(indexorderdir);
    COPY_SCALAR_FIELD(is_ustore);

    return newnode;
}

/*
 * _copyCStoreIndexScan
 */
static CStoreIndexScan* _copyCStoreIndexScan(const CStoreIndexScan* from)
{
    CStoreIndexScan* newnode = makeNode(CStoreIndexScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(indexid);
    COPY_NODE_FIELD(indexqual);
    COPY_NODE_FIELD(indexqualorig);
    COPY_NODE_FIELD(indexorderby);
    COPY_NODE_FIELD(indexorderbyorig);
    COPY_SCALAR_FIELD(indexorderdir);

    COPY_NODE_FIELD(baserelcstorequal);
    COPY_NODE_FIELD(cstorequal);
    COPY_NODE_FIELD(indextlist);
    COPY_SCALAR_FIELD(relStoreLocation);
    COPY_SCALAR_FIELD(indexonly);

    return newnode;
}

/*
 * _copyCStoreIndexScan
 */
static DfsIndexScan* _copyDfsIndexScan(const DfsIndexScan* from)
{
    DfsIndexScan* newnode = makeNode(DfsIndexScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(indexid);
    COPY_NODE_FIELD(indextlist);
    COPY_NODE_FIELD(indexqual);
    COPY_NODE_FIELD(indexqualorig);
    COPY_NODE_FIELD(indexorderby);
    COPY_NODE_FIELD(indexorderbyorig);
    COPY_SCALAR_FIELD(indexorderdir);
    COPY_SCALAR_FIELD(relStoreLocation);
    COPY_NODE_FIELD(cstorequal);
    COPY_NODE_FIELD(indexScantlist);
    COPY_NODE_FIELD(dfsScan);
    COPY_SCALAR_FIELD(indexonly);

    return newnode;
}

/*
 * _copyIndexOnlyScan
 */
static IndexOnlyScan* _copyIndexOnlyScan(const IndexOnlyScan* from)
{
    IndexOnlyScan* newnode = makeNode(IndexOnlyScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(indexid);
    COPY_NODE_FIELD(indexqual);
    COPY_NODE_FIELD(indexorderby);
    COPY_NODE_FIELD(indextlist);
    COPY_SCALAR_FIELD(indexorderdir);

    return newnode;
}

/*
 * _copyBitmapIndexScan
 */
static BitmapIndexScan* _copyBitmapIndexScan(const BitmapIndexScan* from)
{
    BitmapIndexScan* newnode = makeNode(BitmapIndexScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(indexid);
    COPY_NODE_FIELD(indexqual);
    COPY_NODE_FIELD(indexqualorig);
    COPY_SCALAR_FIELD(is_ustore);

    return newnode;
}

/*
 * _copyBitmapHeapScan
 */
static BitmapHeapScan* _copyBitmapHeapScan(const BitmapHeapScan* from)
{
    BitmapHeapScan* newnode = makeNode(BitmapHeapScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(bitmapqualorig);

    return newnode;
}

/*
 * _copyCStoreIndexCtidScan
 */
static CStoreIndexCtidScan* _copyCStoreIndexCtidScan(const CStoreIndexCtidScan* from)
{
    CStoreIndexCtidScan* newnode = makeNode(CStoreIndexCtidScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(indexid);
    COPY_NODE_FIELD(indexqual);
    COPY_NODE_FIELD(indexqualorig);
    COPY_NODE_FIELD(indextlist);
    COPY_NODE_FIELD(cstorequal);
    return newnode;
}

/*
 * _copyCStoreIndexHeapScan
 */
static CStoreIndexHeapScan* _copyCStoreIndexHeapScan(const CStoreIndexHeapScan* from)
{
    CStoreIndexHeapScan* newnode = makeNode(CStoreIndexHeapScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(bitmapqualorig);

    return newnode;
}

/*
 * _copyTidScan
 */
static TidScan* _copyTidScan(const TidScan* from)
{
    TidScan* newnode = makeNode(TidScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(tidquals);

    return newnode;
}

/*
 * _copySubqueryScan
 */
static SubqueryScan* _copySubqueryScan(const SubqueryScan* from)
{
    SubqueryScan* newnode = makeNode(SubqueryScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(subplan);

    return newnode;
}

/*
 * _copyFunctionScan
 */
static FunctionScan* _copyFunctionScan(const FunctionScan* from)
{
    FunctionScan* newnode = makeNode(FunctionScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(funcexpr);
    COPY_NODE_FIELD(funccolnames);
    COPY_NODE_FIELD(funccoltypes);
    COPY_NODE_FIELD(funccoltypmods);
    COPY_NODE_FIELD(funccolcollations);

    return newnode;
}

/*
 * _copyValuesScan
 */
static ValuesScan* _copyValuesScan(const ValuesScan* from)
{
    ValuesScan* newnode = makeNode(ValuesScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(values_lists);

    return newnode;
}

/*
 * _copyCteScan
 */
static CteScan* _copyCteScan(const CteScan* from)
{
    CteScan* newnode = makeNode(CteScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(ctePlanId);
    COPY_SCALAR_FIELD(cteParam);

    /* start with support */
    COPY_NODE_FIELD(cteRef);
    COPY_NODE_FIELD(internalEntryList);
    return newnode;
}

/*
 * _copyWorkTableScan
 */
static WorkTableScan* _copyWorkTableScan(const WorkTableScan* from)
{
    WorkTableScan* newnode = makeNode(WorkTableScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(wtParam);
    COPY_SCALAR_FIELD(forStartWith);

    return newnode;
}

/*
 * _copyForeignScan
 */
static ForeignScan* _copyForeignScan(const ForeignScan* from)
{
    ForeignScan* newnode = makeNode(ForeignScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(scan_relid);
    COPY_NODE_FIELD(fdw_exprs);
    COPY_NODE_FIELD(fdw_private);
    COPY_SCALAR_FIELD(fsSystemCol);
    COPY_SCALAR_FIELD(needSaveError);
    COPY_NODE_FIELD(errCache);
    COPY_NODE_FIELD(prunningResult);
    COPY_NODE_FIELD(rel);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(objectNum);
    COPY_SCALAR_FIELD(bfNum);
    COPY_SCALAR_FIELD(in_compute_pool);

    if (from->bfNum) {
        newnode->bloomFilterSet = (BloomFilterSet**)palloc0(sizeof(BloomFilterSet*) * from->bfNum);
        for (int cell = 0; cell < from->bfNum; cell++) {
            COPY_NODE_FIELD(bloomFilterSet[cell]);
        }
    }

    return newnode;
}

static RelationMetaData* _copyRelationMetaData(const RelationMetaData* from)
{
    RelationMetaData* newnode = makeNode(RelationMetaData);

    COPY_SCALAR_FIELD(rd_id);
    COPY_SCALAR_FIELD(spcNode);
    COPY_SCALAR_FIELD(dbNode);
    COPY_SCALAR_FIELD(relNode);
    COPY_SCALAR_FIELD(bucketNode);
    COPY_STRING_FIELD(relname);
    COPY_SCALAR_FIELD(relkind);
    COPY_SCALAR_FIELD(parttype);
    COPY_SCALAR_FIELD(natts);
    COPY_NODE_FIELD(attrs);

    return newnode;
}

static AttrMetaData* _copyAttrMetaData(const AttrMetaData* from)
{
    AttrMetaData* newnode = makeNode(AttrMetaData);

    COPY_STRING_FIELD(attname);
    COPY_SCALAR_FIELD(atttypid);
    COPY_SCALAR_FIELD(attlen);
    COPY_SCALAR_FIELD(attnum);
    COPY_SCALAR_FIELD(atttypmod);
    COPY_SCALAR_FIELD(attbyval);
    COPY_SCALAR_FIELD(attstorage);
    COPY_SCALAR_FIELD(attalign);
    COPY_SCALAR_FIELD(attnotnull);
    COPY_SCALAR_FIELD(atthasdef);
    COPY_SCALAR_FIELD(attisdropped);
    COPY_SCALAR_FIELD(attislocal);
    COPY_SCALAR_FIELD(attkvtype);
    COPY_SCALAR_FIELD(attcmprmode);
    COPY_SCALAR_FIELD(attinhcount);
    COPY_SCALAR_FIELD(attcollation);

    return newnode;
}

static ForeignOptions* _copyForeignOptions(const ForeignOptions* from)
{
    ForeignOptions* newnode = makeNode(ForeignOptions);

    COPY_SCALAR_FIELD(stype);
    COPY_NODE_FIELD(fOptions);

    return newnode;
}

static DistFdwDataNodeTask* _copyDistFdwDataNodeTask(const DistFdwDataNodeTask* from)
{
    DistFdwDataNodeTask* newnode = makeNode(DistFdwDataNodeTask);

    COPY_STRING_FIELD(dnName);
    COPY_NODE_FIELD(task);

    return newnode;
}

static DistFdwFileSegment* _copyDistFdwFileSegment(const DistFdwFileSegment* from)
{
    DistFdwFileSegment* newnode = makeNode(DistFdwFileSegment);

    COPY_STRING_FIELD(filename);
    COPY_SCALAR_FIELD(begin);
    COPY_SCALAR_FIELD(end);
    COPY_SCALAR_FIELD(ObjectSize);

    return newnode;
}

static SplitInfo* _copySplitInfo(const SplitInfo* from)
{
    SplitInfo* newnode = makeNode(SplitInfo);

    COPY_STRING_FIELD(filePath);
    COPY_STRING_FIELD(fileName);
    COPY_NODE_FIELD(partContentList);
    COPY_SCALAR_FIELD(ObjectSize);
    COPY_STRING_FIELD(eTag);
    COPY_SCALAR_FIELD(prefixSlashNum);
    return newnode;
}

static SplitMap* _copySplitMap(const SplitMap* from)
{
    SplitMap* newnode = makeNode(SplitMap);

    COPY_SCALAR_FIELD(nodeId);
    COPY_SCALAR_FIELD(locatorType);
    COPY_SCALAR_FIELD(totalSize);
    COPY_SCALAR_FIELD(fileNums);
    COPY_STRING_FIELD(downDiskFilePath);
    COPY_NODE_FIELD(lengths);
    COPY_NODE_FIELD(splits);
    return newnode;
}

/* @hdfs */
static DfsPrivateItem* _copyDfsPrivateItem(const DfsPrivateItem* from)
{
    DfsPrivateItem* newnode = makeNode(DfsPrivateItem);

    COPY_NODE_FIELD(columnList);
    COPY_NODE_FIELD(targetList);
    COPY_NODE_FIELD(restrictColList);
    COPY_NODE_FIELD(partList);
    COPY_NODE_FIELD(opExpressionList);
    COPY_NODE_FIELD(dnTask);
    COPY_NODE_FIELD(hdfsQual);
    COPY_SCALAR_FIELD(colNum);
    if (from->colNum > 0) {
        COPY_POINTER_FIELD(selectivity, sizeof(double) * from->colNum);
    }

    return newnode;
}

/*
 * _copyExtensiblePlan
 */
static ExtensiblePlan* _copyExtensiblePlan(const ExtensiblePlan* from)
{
    ExtensiblePlan* newnode = makeNode(ExtensiblePlan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(flags);
    COPY_NODE_FIELD(extensible_plans);
    COPY_NODE_FIELD(extensible_exprs);
    COPY_NODE_FIELD(extensible_private);
    COPY_NODE_FIELD(extensible_plan_tlist);
    COPY_BITMAPSET_FIELD(extensible_relids);

    /*
     * NOTE: The method field of ExtensiblePlan is required to be a pointer to a
     * static table of callback functions.  So we don't copy the table itself,
     * just reference the original one.
     */
    COPY_POINTER_FIELD(methods, sizeof(ExtensiblePlanMethods));

    return newnode;
}

/*
 * CopyJoinFields
 *
 *		This function copies the fields of the Join node.  It is used by
 *		all the copy functions for classes which inherit from Join.
 */
static void CopyJoinFields(const Join* from, Join* newnode)
{
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(jointype);
    COPY_NODE_FIELD(joinqual);
    COPY_SCALAR_FIELD(optimizable);
    COPY_NODE_FIELD(nulleqqual);
    COPY_SCALAR_FIELD(skewoptimize);
}

/*
 * _copyJoin
 */
static Join* _copyJoin(const Join* from)
{
    Join* newnode = makeNode(Join);

    /*
     * copy node superclass fields
     */
    CopyJoinFields(from, newnode);

    return newnode;
}

/*
 * _copyNestLoop
 */
static NestLoop* _copyNestLoop(const NestLoop* from)
{
    NestLoop* newnode = makeNode(NestLoop);

    /*
     * copy node superclass fields
     */
    CopyJoinFields((const Join*)from, (Join*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(nestParams);
    COPY_SCALAR_FIELD(materialAll);

    return newnode;
}

/*
 * _copyVecNestLoop
 */
static VecNestLoop* _copyVecNestLoop(const VecNestLoop* from)
{
    VecNestLoop* newnode = makeNode(VecNestLoop);

    /*
     * copy node superclass fields
     */
    CopyJoinFields((const Join*)from, (Join*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(nestParams);
    COPY_SCALAR_FIELD(materialAll);

    return newnode;
}

/*
 * _copyMergeJoin
 */
static MergeJoin* _copyMergeJoin(const MergeJoin* from)
{
    MergeJoin* newnode = makeNode(MergeJoin);
    int numCols;

    /*
     * copy node superclass fields
     */
    CopyJoinFields((const Join*)from, (Join*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(mergeclauses);
    numCols = list_length(from->mergeclauses);
    if (numCols > 0) {
        COPY_POINTER_FIELD(mergeFamilies, numCols * sizeof(Oid));
        COPY_POINTER_FIELD(mergeCollations, numCols * sizeof(Oid));
        COPY_POINTER_FIELD(mergeStrategies, numCols * sizeof(int));
        COPY_POINTER_FIELD(mergeNullsFirst, numCols * sizeof(bool));
    }

    return newnode;
}

static MergeJoin* _copyVecMergeJoin(const VecMergeJoin* from)
{
    VecMergeJoin* newnode = makeNode(VecMergeJoin);
    int numCols;

    /*
     * copy node superclass fields
     */
    CopyJoinFields((const Join*)from, (Join*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(mergeclauses);
    numCols = list_length(from->mergeclauses);
    if (numCols > 0) {
        COPY_POINTER_FIELD(mergeFamilies, numCols * sizeof(Oid));
        COPY_POINTER_FIELD(mergeCollations, numCols * sizeof(Oid));
        COPY_POINTER_FIELD(mergeStrategies, numCols * sizeof(int));
        COPY_POINTER_FIELD(mergeNullsFirst, numCols * sizeof(bool));
    }

    return newnode;
}
/*
 * _copyHashJoin
 */
static HashJoin* _copyHashJoin(const HashJoin* from)
{
    HashJoin* newnode = makeNode(HashJoin);

    /*
     * copy node superclass fields
     */
    CopyJoinFields((const Join*)from, (Join*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(hashclauses);
    COPY_SCALAR_FIELD(streamBothSides);
    COPY_SCALAR_FIELD(transferFilterFlag);
    COPY_SCALAR_FIELD(rebuildHashTable);
    COPY_SCALAR_FIELD(isSonicHash);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);
    COPY_SCALAR_FIELD(joinRows);

    return newnode;
}

/*
 * _copyMaterial
 */
static Material* _copyMaterial(const Material* from)
{
    Material* newnode = makeNode(Material);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);
    COPY_SCALAR_FIELD(materialize_all);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

/*
 * _copySort
 */
static Sort* _copySort(const Sort* from)
{
    Sort* newnode = makeNode(Sort);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(collations, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
    }
#ifdef PGXC
    COPY_SCALAR_FIELD(srt_start_merge);
#endif

    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

/*
 * _copyGroup
 */
static Group* _copyGroup(const Group* from)
{
    Group* newnode = makeNode(Group);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(grpOperators, from->numCols * sizeof(Oid));
    }

    return newnode;
}

/*
 * _copyAgg
 */
static Agg* _copyAgg(const Agg* from)
{
    Agg* newnode = makeNode(Agg);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(aggstrategy);
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(grpOperators, from->numCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(numGroups);
    COPY_NODE_FIELD(groupingSets);
    COPY_NODE_FIELD(chain);
#ifdef PGXC
    COPY_SCALAR_FIELD(is_final);
    COPY_SCALAR_FIELD(single_node);

#endif /* PGXC */
    COPY_BITMAPSET_FIELD(aggParams);

    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    COPY_SCALAR_FIELD(is_sonichash);
    COPY_SCALAR_FIELD(is_dummy);
    COPY_SCALAR_FIELD(skew_optimize);
    COPY_SCALAR_FIELD(unique_check);
    return newnode;
}

/*
 * _copyWindowAgg
 */
static WindowAgg* _copyWindowAgg(const WindowAgg* from)
{
    WindowAgg* newnode = makeNode(WindowAgg);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(winref);
    COPY_SCALAR_FIELD(partNumCols);
    if (from->partNumCols > 0) {
        COPY_POINTER_FIELD(partColIdx, from->partNumCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(partOperators, from->partNumCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(ordNumCols);
    if (from->ordNumCols > 0) {
        COPY_POINTER_FIELD(ordColIdx, from->ordNumCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(ordOperators, from->ordNumCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(frameOptions);
    COPY_NODE_FIELD(startOffset);
    COPY_NODE_FIELD(endOffset);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

/*
 * _copyUnique
 */
static Unique* _copyUnique(const Unique* from)
{
    Unique* newnode = makeNode(Unique);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(uniqColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(uniqOperators, from->numCols * sizeof(Oid));
    }

    return newnode;
}

/*
 * _copyHash
 */
static Hash* _copyHash(const Hash* from)
{
    Hash* newnode = makeNode(Hash);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(skewTable);
    COPY_SCALAR_FIELD(skewColumn);
    COPY_SCALAR_FIELD(skewInherit);
    COPY_SCALAR_FIELD(skewColType);
    COPY_SCALAR_FIELD(skewColTypmod);

    return newnode;
}

/*
 * _copySetOp
 */
static SetOp* _copySetOp(const SetOp* from)
{
    SetOp* newnode = makeNode(SetOp);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(cmd);
    COPY_SCALAR_FIELD(strategy);
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(dupColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(dupOperators, from->numCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(flagColIdx);
    COPY_SCALAR_FIELD(firstFlag);
    COPY_SCALAR_FIELD(numGroups);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

/*
 * _copyLockRows
 */
static LockRows* _copyLockRows(const LockRows* from)
{
    LockRows* newnode = makeNode(LockRows);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(rowMarks);
    COPY_SCALAR_FIELD(epqParam);

    return newnode;
}

/*
 * _copyLimit
 */
static Limit* _copyLimit(const Limit* from)
{
    Limit* newnode = makeNode(Limit);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(limitOffset);
    COPY_NODE_FIELD(limitCount);

    return newnode;
}

/*
 * _copyNestLoopParam
 */
static NestLoopParam* _copyNestLoopParam(const NestLoopParam* from)
{
    NestLoopParam* newnode = makeNode(NestLoopParam);

    COPY_SCALAR_FIELD(paramno);
    COPY_NODE_FIELD(paramval);

    return newnode;
}

/*
 * _copyPlanRowMark
 */
static PlanRowMark* _copyPlanRowMark(const PlanRowMark* from)
{
    PlanRowMark* newnode = makeNode(PlanRowMark);

    COPY_SCALAR_FIELD(rti);
    COPY_SCALAR_FIELD(prti);
    COPY_SCALAR_FIELD(rowmarkId);
    COPY_SCALAR_FIELD(markType);
    COPY_SCALAR_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COPY_SCALAR_FIELD(waitSec);
    }
    COPY_SCALAR_FIELD(isParent);
    COPY_SCALAR_FIELD(numAttrs);
    COPY_BITMAPSET_FIELD(bms_nodeids);

    return newnode;
}

/*
 * _copyPlanInvalItem
 */
static PlanInvalItem* _copyPlanInvalItem(const PlanInvalItem* from)
{
    PlanInvalItem* newnode = makeNode(PlanInvalItem);

    COPY_SCALAR_FIELD(cacheId);
    COPY_SCALAR_FIELD(hashValue);

    return newnode;
}

static FuncInvalItem* _copyFuncInvalItem(const FuncInvalItem* from)
{
    FuncInvalItem* newnode = makeNode(FuncInvalItem);

    COPY_SCALAR_FIELD(cacheId);
    COPY_SCALAR_FIELD(objId);

    return newnode;
}

static VecToRow* _copyVecToRow(const VecToRow* from)
{
    VecToRow* newnode = makeNode(VecToRow);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    return newnode;
}
static RowToVec* _copyRowToVec(const RowToVec* from)
{
    RowToVec* newnode = makeNode(RowToVec);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    return newnode;
}
static VecSort* _copyVecSort(const VecSort* from)
{
    VecSort* newnode = makeNode(VecSort);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(collations, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
    }
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}
static VecResult* _copyVecResult(const VecResult* from)
{
    VecResult* newnode = makeNode(VecResult);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(resconstantqual);

    return newnode;
}
static CStoreScan* _copyCStoreScan(const CStoreScan* from)
{
    CStoreScan* newnode = makeNode(CStoreScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    COPY_LOCATION_FIELD(selectionRatio);
    COPY_NODE_FIELD(cstorequal);
    COPY_NODE_FIELD(minMaxInfo);
    COPY_LOCATION_FIELD(relStoreLocation);
    COPY_SCALAR_FIELD(is_replica_table);

    return newnode;
}

static DfsScan* _copyDfsScan(const DfsScan* from)
{
    DfsScan* newnode = makeNode(DfsScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);
    COPY_SCALAR_FIELD(relStoreLocation);
    COPY_STRING_FIELD(storeFormat);
    COPY_NODE_FIELD(privateData);
    return newnode;
}

#ifdef ENABLE_MULTIPLE_NODES
static TsStoreScan*
_copyTsStoreScan(const TsStoreScan * from)
{
    TsStoreScan* newnode = makeNode(TsStoreScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    COPY_LOCATION_FIELD(selectionRatio);
    COPY_NODE_FIELD(tsstorequal);
    COPY_NODE_FIELD(minMaxInfo);
    COPY_LOCATION_FIELD(relStoreLocation);
    COPY_SCALAR_FIELD(is_replica_table);
    COPY_SCALAR_FIELD(sort_by_time_colidx);
    COPY_SCALAR_FIELD(limit);
    COPY_SCALAR_FIELD(is_simple_scan);
    COPY_SCALAR_FIELD(has_sort);
    COPY_SCALAR_FIELD(series_func_calls);
    COPY_SCALAR_FIELD(top_key_func_arg);
    return newnode;
}
#endif   /* ENABLE_MULTIPLE_NODES */

static VecSubqueryScan* _copyVecSubqueryScan(const VecSubqueryScan* from)
{
    VecSubqueryScan* newnode = makeNode(VecSubqueryScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(subplan);

    return newnode;
}

static VecHashJoin* _copyVecHashJoin(const VecHashJoin* from)
{
    VecHashJoin* newnode = makeNode(VecHashJoin);

    /*
     * copy node superclass fields
     */
    CopyJoinFields((const Join*)from, (Join*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(hashclauses);
    COPY_SCALAR_FIELD(streamBothSides);
    COPY_SCALAR_FIELD(transferFilterFlag);
    COPY_SCALAR_FIELD(rebuildHashTable);
    COPY_SCALAR_FIELD(isSonicHash);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

static VecAgg* _copyVecAgg(const VecAgg* from)
{
    VecAgg* newnode = makeNode(VecAgg);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(aggstrategy);
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(grpOperators, from->numCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(numGroups);
    COPY_NODE_FIELD(groupingSets);
    COPY_NODE_FIELD(chain);
#ifdef PGXC
    COPY_SCALAR_FIELD(is_final);
    COPY_SCALAR_FIELD(single_node);

#endif /* PGXC */
    COPY_BITMAPSET_FIELD(aggParams);
    COPY_SCALAR_FIELD(is_sonichash);
    COPY_SCALAR_FIELD(skew_optimize);
    COPY_SCALAR_FIELD(unique_check);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

static VecPartIterator* _copyVecPartIterator(const VecPartIterator* from)
{
    VecPartIterator* newnode = makeNode(VecPartIterator);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(partType);
    COPY_SCALAR_FIELD(itrs);
    COPY_SCALAR_FIELD(direction);
    COPY_NODE_FIELD(param);

    return newnode;
}

static VecAppend* _copyVecAppend(const VecAppend* from)
{
    VecAppend* newnode = makeNode(VecAppend);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(appendplans);

    return newnode;
}

static VecSetOp* _copyVecSetOp(const VecSetOp* from)
{
    VecSetOp* newnode = makeNode(VecSetOp);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(cmd);
    COPY_SCALAR_FIELD(strategy);
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(dupColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(dupOperators, from->numCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(flagColIdx);
    COPY_SCALAR_FIELD(firstFlag);
    COPY_SCALAR_FIELD(numGroups);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

static VecForeignScan* _copyVecForeignScan(const VecForeignScan* from)
{
    VecForeignScan* newnode = makeNode(VecForeignScan);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(scan_relid);
    COPY_NODE_FIELD(fdw_exprs);
    COPY_NODE_FIELD(fdw_private);
    COPY_SCALAR_FIELD(fsSystemCol);
    COPY_SCALAR_FIELD(needSaveError);
    COPY_NODE_FIELD(errCache);
    COPY_NODE_FIELD(prunningResult);
    COPY_NODE_FIELD(rel);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(objectNum);
    COPY_SCALAR_FIELD(bfNum);

    COPY_SCALAR_FIELD(in_compute_pool);

    if (from->bfNum) {
        newnode->bloomFilterSet = (BloomFilterSet**)palloc0(sizeof(BloomFilterSet*) * from->bfNum);
        for (int cell = 0; cell < from->bfNum; cell++) {
            COPY_NODE_FIELD(bloomFilterSet[cell]);
        }
    }

    return newnode;
}

static VecModifyTable* _copyVecModifyTable(const VecModifyTable* from)
{
    VecModifyTable* newnode = makeNode(VecModifyTable);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(operation);
    COPY_SCALAR_FIELD(canSetTag);
    COPY_NODE_FIELD(resultRelations);
    COPY_SCALAR_FIELD(resultRelIndex);
    COPY_NODE_FIELD(plans);
    COPY_NODE_FIELD(returningLists);
    COPY_NODE_FIELD(rowMarks);
    COPY_SCALAR_FIELD(epqParam);
    COPY_SCALAR_FIELD(partKeyUpdated);
#ifdef PGXC
    COPY_NODE_FIELD(remote_plans);
#endif
    COPY_NODE_FIELD(cacheEnt);

    COPY_SCALAR_FIELD(mergeTargetRelation);
    COPY_NODE_FIELD(mergeSourceTargetList);
    COPY_NODE_FIELD(mergeActionList);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

/*
 * _copyVecGroup
 */
static VecGroup* _copyVecGroup(const VecGroup* from)
{
    VecGroup* newnode = makeNode(VecGroup);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(grpOperators, from->numCols * sizeof(Oid));
    }

    return newnode;
}

/*
 * _copyVecUnique
 */
static VecUnique* _copyVecUnique(const VecUnique* from)
{
    VecUnique* newnode = makeNode(VecUnique);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(uniqColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(uniqOperators, from->numCols * sizeof(Oid));
    }

    return newnode;
}

static VecLimit* _copyVecLimit(const VecLimit* from)
{
    VecLimit* newnode = makeNode(VecLimit);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_NODE_FIELD(limitOffset);
    COPY_NODE_FIELD(limitCount);

    return newnode;
}

static VecMaterial* _copyVecMaterial(const VecMaterial* from)
{
    VecMaterial* newnode = makeNode(VecMaterial);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);
    COPY_SCALAR_FIELD(materialize_all);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

#ifdef PGXC

static VecStream* _copyVecStream(const VecStream* from)
{
    VecStream* newnode = makeNode(VecStream);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(type);
    COPY_STRING_FIELD(plan_statement);
    COPY_NODE_FIELD(consumer_nodes);
    COPY_NODE_FIELD(distribute_keys);
    COPY_SCALAR_FIELD(is_sorted);
    COPY_NODE_FIELD(sort);
    COPY_SCALAR_FIELD(is_dummy);
    COPY_SCALAR_FIELD(smpDesc.consumerDop);
    COPY_SCALAR_FIELD(smpDesc.producerDop);
    COPY_SCALAR_FIELD(smpDesc.distriType);
    COPY_NODE_FIELD(skew_list);
    COPY_SCALAR_FIELD(stream_level);
    COPY_NODE_FIELD(origin_consumer_nodes);
    COPY_SCALAR_FIELD(is_recursive_local);

    return newnode;
}

/*
 * _copyWindowAgg
 */
static VecWindowAgg* _copyVecWindowAgg(const VecWindowAgg* from)
{
    VecWindowAgg* newnode = makeNode(VecWindowAgg);

    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    COPY_SCALAR_FIELD(winref);
    COPY_SCALAR_FIELD(partNumCols);
    if (from->partNumCols > 0) {
        COPY_POINTER_FIELD(partColIdx, from->partNumCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(partOperators, from->partNumCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(ordNumCols);
    if (from->ordNumCols > 0) {
        COPY_POINTER_FIELD(ordColIdx, from->ordNumCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(ordOperators, from->ordNumCols * sizeof(Oid));
    }
    COPY_SCALAR_FIELD(frameOptions);
    COPY_NODE_FIELD(startOffset);
    COPY_NODE_FIELD(endOffset);
    CopyMemInfoFields(&from->mem_info, &newnode->mem_info);

    return newnode;
}

/*
 * _copyvecRemoteQuery
 */
static VecRemoteQuery* _copyVecRemoteQuery(const VecRemoteQuery* from)
{
    VecRemoteQuery* newnode = makeNode(VecRemoteQuery);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(exec_direct_type);
    COPY_STRING_FIELD(sql_statement);
    COPY_NODE_FIELD(exec_nodes);
    COPY_SCALAR_FIELD(combine_type);
    COPY_SCALAR_FIELD(read_only);
    COPY_SCALAR_FIELD(force_autocommit);
    COPY_STRING_FIELD(statement);
    COPY_STRING_FIELD(cursor);
    COPY_SCALAR_FIELD(rq_num_params);
    if (from->rq_param_types) {
        COPY_POINTER_FIELD(rq_param_types, sizeof(from->rq_param_types[0]) * from->rq_num_params);
    } else {
        newnode->rq_param_types = NULL;
    }
    COPY_SCALAR_FIELD(rq_params_internal);
    COPY_SCALAR_FIELD(exec_type);
    COPY_SCALAR_FIELD(is_temp);
    COPY_SCALAR_FIELD(rq_finalise_aggs);
    COPY_SCALAR_FIELD(rq_sortgroup_colno);
    COPY_NODE_FIELD(remote_query);
    COPY_NODE_FIELD(base_tlist);
    COPY_NODE_FIELD(coord_var_tlist);
    COPY_NODE_FIELD(query_var_tlist);
    COPY_SCALAR_FIELD(has_row_marks);
    COPY_SCALAR_FIELD(rq_save_command_id);
    COPY_SCALAR_FIELD(is_simple);
    COPY_SCALAR_FIELD(rq_need_proj);
    COPY_SCALAR_FIELD(mergesort_required);
    COPY_SCALAR_FIELD(spool_no_data);
    COPY_SCALAR_FIELD(poll_multi_channel);
    COPY_SCALAR_FIELD(num_stream);
    COPY_SCALAR_FIELD(num_gather);
    COPY_NODE_FIELD(sort);
    COPY_NODE_FIELD(rte_ref);
    COPY_SCALAR_FIELD(position);
    COPY_SCALAR_FIELD(is_remote_function_query);
    return newnode;
}

/*
 * _copyExecDirect
 */
static ExecDirectStmt* _copyExecDirect(const ExecDirectStmt* from)
{
    ExecDirectStmt* newnode = makeNode(ExecDirectStmt);

    COPY_NODE_FIELD(node_names);
    COPY_STRING_FIELD(query);

    return newnode;
}

static Stream* _copyStream(const Stream* from)
{
    Stream* newnode = makeNode(Stream);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(type);
    COPY_STRING_FIELD(plan_statement);
    COPY_NODE_FIELD(consumer_nodes);
    COPY_NODE_FIELD(distribute_keys);
    COPY_SCALAR_FIELD(is_sorted);
    COPY_NODE_FIELD(sort);
    COPY_SCALAR_FIELD(is_dummy);
    COPY_SCALAR_FIELD(smpDesc.consumerDop);
    COPY_SCALAR_FIELD(smpDesc.producerDop);
    COPY_SCALAR_FIELD(smpDesc.distriType);
    COPY_NODE_FIELD(skew_list);
    COPY_SCALAR_FIELD(stream_level);
    COPY_NODE_FIELD(origin_consumer_nodes);
    COPY_SCALAR_FIELD(is_recursive_local);

    return newnode;
}

/*
 * _copyRemoteQuery
 */
static RemoteQuery* _copyRemoteQuery(const RemoteQuery* from)
{
    RemoteQuery* newnode = makeNode(RemoteQuery);

    /*
     * copy node superclass fields
     */
    CopyScanFields((const Scan*)from, (Scan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(exec_direct_type);
    COPY_STRING_FIELD(sql_statement);
    COPY_NODE_FIELD(exec_nodes);
    COPY_SCALAR_FIELD(combine_type);
    COPY_SCALAR_FIELD(read_only);
    COPY_SCALAR_FIELD(force_autocommit);
    COPY_STRING_FIELD(statement);
    COPY_STRING_FIELD(cursor);
    COPY_SCALAR_FIELD(rq_num_params);
    if (from->rq_param_types) {
        COPY_POINTER_FIELD(rq_param_types, sizeof(from->rq_param_types[0]) * from->rq_num_params);
    } else {
        newnode->rq_param_types = NULL;
    }
    COPY_SCALAR_FIELD(rq_params_internal);
    COPY_SCALAR_FIELD(exec_type);
    COPY_SCALAR_FIELD(is_temp);
    COPY_SCALAR_FIELD(rq_finalise_aggs);
    COPY_SCALAR_FIELD(rq_sortgroup_colno);
    COPY_NODE_FIELD(remote_query);
    COPY_NODE_FIELD(base_tlist);
    COPY_NODE_FIELD(coord_var_tlist);
    COPY_NODE_FIELD(query_var_tlist);
    COPY_SCALAR_FIELD(has_row_marks);
    COPY_SCALAR_FIELD(rq_save_command_id);
    COPY_SCALAR_FIELD(is_simple);
    COPY_SCALAR_FIELD(rq_need_proj);
    COPY_SCALAR_FIELD(mergesort_required);
    COPY_SCALAR_FIELD(spool_no_data);
    COPY_SCALAR_FIELD(poll_multi_channel);
    COPY_SCALAR_FIELD(num_stream);
    COPY_SCALAR_FIELD(num_gather);
    COPY_NODE_FIELD(sort);
    COPY_NODE_FIELD(rte_ref);
    COPY_SCALAR_FIELD(position);
    COPY_SCALAR_FIELD(is_remote_function_query);
    newnode->isCustomPlan = from->isCustomPlan;
    newnode->isFQS = from->isFQS;
    COPY_NODE_FIELD(relationOids);
    return newnode;
}

/*
 * _copySliceBoundary
 */
static SliceBoundary* _copySliceBoundary(const SliceBoundary* from)
{
    SliceBoundary* newnode = makeNode(SliceBoundary);
    COPY_SCALAR_FIELD(nodeIdx);
    COPY_SCALAR_FIELD(len);

    for (int i = 0; i < from->len; i++) {
        newnode->boundary[i] = (Const*)copyObject(from->boundary[i]);
    }
    return newnode;
}

/*
 * _copyExecBoundary
 */
static ExecBoundary* _copyExecBoundary(const ExecBoundary* from)
{
    ExecBoundary* newnode = makeNode(ExecBoundary);
    COPY_SCALAR_FIELD(locatorType);
    COPY_SCALAR_FIELD(count);

    if (from->count > 0) {
        SliceBoundary** bdArray = (SliceBoundary**)palloc0(sizeof(SliceBoundary*) * newnode->count);
        for (int i = 0; i < newnode->count; i++) {
            bdArray[i] = (SliceBoundary*)copyObject(from->eles[i]);
        }
        newnode->eles = bdArray;
    }

    return newnode;
}


/*
 * _copyExecNodes
 */
static ExecNodes* _copyExecNodes(const ExecNodes* from)
{
    ExecNodes* newnode = makeNode(ExecNodes);

    COPY_NODE_FIELD(primarynodelist);
    COPY_NODE_FIELD(nodeList);
    COPY_SCALAR_FIELD(distribution.group_oid);
    COPY_BITMAPSET_FIELD(distribution.bms_data_nodeids);
    COPY_SCALAR_FIELD(baselocatortype);
    COPY_NODE_FIELD(en_expr);
    COPY_SCALAR_FIELD(en_relid);
    COPY_SCALAR_FIELD(rangelistOid);
    COPY_SCALAR_FIELD(need_range_prune);
    COPY_SCALAR_FIELD(en_varno);
    COPY_NODE_FIELD(boundaries);
    COPY_SCALAR_FIELD(accesstype);
    COPY_NODE_FIELD(en_dist_vars);
    COPY_SCALAR_FIELD(bucketmapIdx);
    COPY_SCALAR_FIELD(nodelist_is_nil);
    COPY_NODE_FIELD(original_nodeList);
    COPY_NODE_FIELD(dynamic_en_expr);
    COPY_SCALAR_FIELD(bucketid);
    COPY_NODE_FIELD(bucketexpr);
    COPY_SCALAR_FIELD(bucketrelid);
    newnode->hotkeys = CopyHotKeys(from->hotkeys);

    return newnode;
}

/*
 * _copySimpleSort
 */
static SimpleSort* _copySimpleSort(const SimpleSort* from)
{
    SimpleSort* newnode = makeNode(SimpleSort);

    COPY_SCALAR_FIELD(numCols);
    if (from->numCols > 0) {
        COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
        COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(sortCollations, from->numCols * sizeof(Oid));
        COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
    }
    COPY_SCALAR_FIELD(sortToStore);
    return newnode;
}
#endif

/* ****************************************************************
 *					   primnodes.h copy functions
 * ****************************************************************
 */

/*
 * _copyAlias
 */
static Alias* _copyAlias(const Alias* from)
{
    Alias* newnode = makeNode(Alias);

    COPY_STRING_FIELD(aliasname);
    COPY_NODE_FIELD(colnames);

    return newnode;
}

/*
 * _copyRangeVar
 */
static RangeVar* _copyRangeVar(const RangeVar* from)
{
    RangeVar* newnode = makeNode(RangeVar);

    COPY_STRING_FIELD(catalogname);
    COPY_STRING_FIELD(schemaname);
    COPY_STRING_FIELD(relname);
    COPY_STRING_FIELD(partitionname);
    COPY_STRING_FIELD(subpartitionname);
    COPY_SCALAR_FIELD(inhOpt);
    COPY_SCALAR_FIELD(relpersistence);
    COPY_NODE_FIELD(alias);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(ispartition);
    COPY_SCALAR_FIELD(issubpartition);
    COPY_NODE_FIELD(partitionKeyValuesList);
    COPY_SCALAR_FIELD(isbucket);
    COPY_NODE_FIELD(buckets);
#ifdef ENABLE_MOT
    COPY_SCALAR_FIELD(foreignOid);
#endif
    COPY_SCALAR_FIELD(withVerExpr);

    return newnode;
}

/*
 * _copyIntoClause
 */
static IntoClause* _copyIntoClause(const IntoClause* from)
{
    IntoClause* newnode = makeNode(IntoClause);

    COPY_NODE_FIELD(rel);
    COPY_NODE_FIELD(colNames);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(onCommit);
    COPY_SCALAR_FIELD(row_compress);
    COPY_STRING_FIELD(tableSpaceName);
    COPY_SCALAR_FIELD(skipData);
    COPY_SCALAR_FIELD(ivm);
    COPY_SCALAR_FIELD(relkind);
#ifdef PGXC
    COPY_NODE_FIELD(distributeby);
    COPY_NODE_FIELD(subcluster);
#endif

    return newnode;
}

/*
 * _copyStartWithClause
 */
static StartWithClause* _copyStartWithClause(const StartWithClause* from)
{
    StartWithClause* newnode = makeNode(StartWithClause);

    COPY_NODE_FIELD(startWithExpr);
    COPY_NODE_FIELD(connectByExpr);
    COPY_NODE_FIELD(siblingsOrderBy);

    COPY_SCALAR_FIELD(priorDirection);
    COPY_SCALAR_FIELD(nocycle);
    COPY_SCALAR_FIELD(opt);

    return newnode;
}

/*
 * We don't need a _copyExpr because Expr is an abstract supertype which
 * should never actually get instantiated.	Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out copying the common fields...
 */

/*
 * _copyVar
 */
static Var* _copyVar(const Var* from)
{
    Var* newnode = makeNode(Var);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(varno);
    COPY_SCALAR_FIELD(varattno);
    COPY_SCALAR_FIELD(vartype);
    COPY_SCALAR_FIELD(vartypmod);
    COPY_SCALAR_FIELD(varcollid);
    COPY_SCALAR_FIELD(varlevelsup);
    COPY_SCALAR_FIELD(varnoold);
    COPY_SCALAR_FIELD(varoattno);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyConst
 */
static Const* _copyConst(const Const* from)
{
    Const* newnode = makeNode(Const);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(consttype);
    COPY_SCALAR_FIELD(consttypmod);
    COPY_SCALAR_FIELD(constcollid);
    COPY_SCALAR_FIELD(constlen);

    if (from->constbyval || from->constisnull) {
        /*
         * passed by value so just copy the datum. Also, don't try to copy
         * struct when value is null!
         */
        newnode->constvalue = from->constvalue;
    } else {
        /*
         * passed by reference.  We need a palloc'd copy.
         */
        newnode->constvalue = datumCopy(from->constvalue, from->constbyval, from->constlen);
    }

    COPY_SCALAR_FIELD(constisnull);
    COPY_SCALAR_FIELD(constbyval);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(ismaxvalue);
    CopyCursorFields(&from->cursor_data, &newnode->cursor_data);

    return newnode;
}

/*
 * _copyParam
 */
static Param* _copyParam(const Param* from)
{
    Param* newnode = makeNode(Param);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(paramkind);
    COPY_SCALAR_FIELD(paramid);
    COPY_SCALAR_FIELD(paramtype);
    COPY_SCALAR_FIELD(paramtypmod);
    COPY_SCALAR_FIELD(paramcollid);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(tableOfIndexType);
    COPY_SCALAR_FIELD(recordVarTypOid);

    return newnode;
}

/*
 * _copyRownum
 */
static Rownum* _copyRownum(const Rownum* from)
{
    Rownum* newnode = (Rownum*)makeNode(Rownum);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(rownumcollid);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyAggref
 */
static Aggref* _copyAggref(const Aggref* from)
{
    Aggref* newnode = makeNode(Aggref);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(aggfnoid);
    COPY_SCALAR_FIELD(aggtype);
#ifdef PGXC
    COPY_SCALAR_FIELD(aggtrantype);
    COPY_SCALAR_FIELD(agghas_collectfn);
    COPY_SCALAR_FIELD(aggstage);
#endif /* PGXC */
    COPY_SCALAR_FIELD(aggcollid);
    COPY_SCALAR_FIELD(inputcollid);
    COPY_NODE_FIELD(aggdirectargs);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(aggorder);
    COPY_NODE_FIELD(aggdistinct);
    COPY_SCALAR_FIELD(aggstar);
    COPY_SCALAR_FIELD(aggvariadic);
    COPY_SCALAR_FIELD(aggkind);
    COPY_SCALAR_FIELD(agglevelsup);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyGroupingFunc
 */
static GroupingFunc* _copyGroupingFunc(const GroupingFunc* from)
{
    GroupingFunc* newnode = makeNode(GroupingFunc);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(refs);
    COPY_NODE_FIELD(cols);
    COPY_SCALAR_FIELD(agglevelsup);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static GroupingSet* _copyGroupingSet(const GroupingSet* from)
{
    GroupingSet* newnode = makeNode(GroupingSet);

    COPY_SCALAR_FIELD(kind);
    COPY_NODE_FIELD(content);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyWindowFunc
 */
static WindowFunc* _copyWindowFunc(const WindowFunc* from)
{
    WindowFunc* newnode = makeNode(WindowFunc);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(winfnoid);
    COPY_SCALAR_FIELD(wintype);
    COPY_SCALAR_FIELD(wincollid);
    COPY_SCALAR_FIELD(inputcollid);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(winref);
    COPY_SCALAR_FIELD(winstar);
    COPY_SCALAR_FIELD(winagg);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyArrayRef
 */
static ArrayRef* _copyArrayRef(const ArrayRef* from)
{
    ArrayRef* newnode = makeNode(ArrayRef);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(refarraytype);
    COPY_SCALAR_FIELD(refelemtype);
    COPY_SCALAR_FIELD(reftypmod);
    COPY_SCALAR_FIELD(refcollid);
    COPY_NODE_FIELD(refupperindexpr);
    COPY_NODE_FIELD(reflowerindexpr);
    COPY_NODE_FIELD(refexpr);
    COPY_NODE_FIELD(refassgnexpr);

    return newnode;
}

/*
 * _copyFuncExpr
 */
static FuncExpr* _copyFuncExpr(const FuncExpr* from)
{
    FuncExpr* newnode = makeNode(FuncExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(funcid);
    COPY_SCALAR_FIELD(funcresulttype);
    COPY_SCALAR_FIELD(funcresulttype_orig);
    COPY_SCALAR_FIELD(funcretset);
    COPY_SCALAR_FIELD(funcvariadic);
    COPY_SCALAR_FIELD(funcformat);
    COPY_SCALAR_FIELD(funccollid);
    COPY_SCALAR_FIELD(inputcollid);
    COPY_NODE_FIELD(args);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(refSynOid);

    return newnode;
}

/*
 * _copyNamedArgExpr *
 */
static NamedArgExpr* _copyNamedArgExpr(const NamedArgExpr* from)
{
    NamedArgExpr* newnode = makeNode(NamedArgExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_STRING_FIELD(name);
    COPY_SCALAR_FIELD(argnumber);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

template <typename T>
static void _copyCommonOpExprPart(const T* from, T* newnode)
{
    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(opno);
    COPY_SCALAR_FIELD(opfuncid);
    COPY_SCALAR_FIELD(opresulttype);
    COPY_SCALAR_FIELD(opretset);
    COPY_SCALAR_FIELD(opcollid);
    COPY_SCALAR_FIELD(inputcollid);
    COPY_NODE_FIELD(args);
    COPY_LOCATION_FIELD(location);
}
/*
 * _copyOpExpr
 */
static OpExpr* _copyOpExpr(const OpExpr* from)
{
    OpExpr* newnode = makeNode(OpExpr);
    _copyCommonOpExprPart<OpExpr>(from, newnode);
    return newnode;
}

/*
 * _copyDistinctExpr (same as OpExpr)
 */
static DistinctExpr* _copyDistinctExpr(const DistinctExpr* from)
{
    DistinctExpr* newnode = makeNode(DistinctExpr);
    _copyCommonOpExprPart<DistinctExpr>(from, newnode);
    return newnode;
}

/*
 * _copyNullIfExpr (same as OpExpr)
 */
static NullIfExpr* _copyNullIfExpr(const NullIfExpr* from)
{
    NullIfExpr* newnode = makeNode(NullIfExpr);
    _copyCommonOpExprPart<NullIfExpr>(from, newnode);
    return newnode;
}

/*
 * _copyScalarArrayOpExpr
 */
static ScalarArrayOpExpr* _copyScalarArrayOpExpr(const ScalarArrayOpExpr* from)
{
    ScalarArrayOpExpr* newnode = makeNode(ScalarArrayOpExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(opno);
    COPY_SCALAR_FIELD(opfuncid);
    COPY_SCALAR_FIELD(useOr);
    COPY_SCALAR_FIELD(inputcollid);
    COPY_NODE_FIELD(args);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyBoolExpr
 */
static BoolExpr* _copyBoolExpr(const BoolExpr* from)
{
    BoolExpr* newnode = makeNode(BoolExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(boolop);
    COPY_NODE_FIELD(args);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copySubLink
 */
static SubLink* _copySubLink(const SubLink* from)
{
    SubLink* newnode = makeNode(SubLink);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(subLinkType);
    COPY_NODE_FIELD(testexpr);
    COPY_NODE_FIELD(operName);
    COPY_NODE_FIELD(subselect);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copySubPlan
 */
static SubPlan* _copySubPlan(const SubPlan* from)
{
    SubPlan* newnode = makeNode(SubPlan);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(subLinkType);
    COPY_NODE_FIELD(testexpr);
    COPY_NODE_FIELD(paramIds);
    COPY_SCALAR_FIELD(plan_id);
    COPY_STRING_FIELD(plan_name);
    COPY_SCALAR_FIELD(firstColType);
    COPY_SCALAR_FIELD(firstColTypmod);
    COPY_SCALAR_FIELD(firstColCollation);
    COPY_SCALAR_FIELD(useHashTable);
    COPY_SCALAR_FIELD(unknownEqFalse);
    COPY_NODE_FIELD(setParam);
    COPY_NODE_FIELD(parParam);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(startup_cost);
    COPY_SCALAR_FIELD(per_call_cost);

    return newnode;
}

/*
 * _copyAlternativeSubPlan
 */
static AlternativeSubPlan* _copyAlternativeSubPlan(const AlternativeSubPlan* from)
{
    AlternativeSubPlan* newnode = makeNode(AlternativeSubPlan);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(subplans);

    return newnode;
}

/*
 * _copyFieldSelect
 */
static FieldSelect* _copyFieldSelect(const FieldSelect* from)
{
    FieldSelect* newnode = makeNode(FieldSelect);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(fieldnum);
    COPY_SCALAR_FIELD(resulttype);
    COPY_SCALAR_FIELD(resulttypmod);
    COPY_SCALAR_FIELD(resultcollid);

    return newnode;
}

/*
 * _copyFieldStore
 */
static FieldStore* _copyFieldStore(const FieldStore* from)
{
    FieldStore* newnode = makeNode(FieldStore);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_NODE_FIELD(newvals);
    COPY_NODE_FIELD(fieldnums);
    COPY_SCALAR_FIELD(resulttype);

    return newnode;
}

/*
 * _copyRelabelType
 */
static RelabelType* _copyRelabelType(const RelabelType* from)
{
    RelabelType* newnode = makeNode(RelabelType);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(resulttype);
    COPY_SCALAR_FIELD(resulttypmod);
    COPY_SCALAR_FIELD(resultcollid);
    COPY_SCALAR_FIELD(relabelformat);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCoerceViaIO
 */
static CoerceViaIO* _copyCoerceViaIO(const CoerceViaIO* from)
{
    CoerceViaIO* newnode = makeNode(CoerceViaIO);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(resulttype);
    COPY_SCALAR_FIELD(resultcollid);
    COPY_SCALAR_FIELD(coerceformat);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyArrayCoerceExpr
 */
static ArrayCoerceExpr* _copyArrayCoerceExpr(const ArrayCoerceExpr* from)
{
    ArrayCoerceExpr* newnode = makeNode(ArrayCoerceExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(elemfuncid);
    COPY_SCALAR_FIELD(resulttype);
    COPY_SCALAR_FIELD(resulttypmod);
    COPY_SCALAR_FIELD(resultcollid);
    COPY_SCALAR_FIELD(isExplicit);
    COPY_SCALAR_FIELD(coerceformat);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyConvertRowtypeExpr
 */
static ConvertRowtypeExpr* _copyConvertRowtypeExpr(const ConvertRowtypeExpr* from)
{
    ConvertRowtypeExpr* newnode = makeNode(ConvertRowtypeExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(resulttype);
    COPY_SCALAR_FIELD(convertformat);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCollateExpr
 */
static CollateExpr* _copyCollateExpr(const CollateExpr* from)
{
    CollateExpr* newnode = makeNode(CollateExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(collOid);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCaseExpr
 */
static CaseExpr* _copyCaseExpr(const CaseExpr* from)
{
    CaseExpr* newnode = makeNode(CaseExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(casetype);
    COPY_SCALAR_FIELD(casecollid);
    COPY_NODE_FIELD(arg);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(defresult);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCaseWhen
 */
static CaseWhen* _copyCaseWhen(const CaseWhen* from)
{
    CaseWhen* newnode = makeNode(CaseWhen);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(expr);
    COPY_NODE_FIELD(result);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCaseTestExpr
 */
static CaseTestExpr* _copyCaseTestExpr(const CaseTestExpr* from)
{
    CaseTestExpr* newnode = makeNode(CaseTestExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(typeId);
    COPY_SCALAR_FIELD(typeMod);
    COPY_SCALAR_FIELD(collation);

    return newnode;
}

/*
 * _copyArrayExpr
 */
static ArrayExpr* _copyArrayExpr(const ArrayExpr* from)
{
    ArrayExpr* newnode = makeNode(ArrayExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(array_typeid);
    COPY_SCALAR_FIELD(array_collid);
    COPY_SCALAR_FIELD(element_typeid);
    COPY_NODE_FIELD(elements);
    COPY_SCALAR_FIELD(multidims);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyRowExpr
 */
static RowExpr* _copyRowExpr(const RowExpr* from)
{
    RowExpr* newnode = makeNode(RowExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(row_typeid);
    COPY_SCALAR_FIELD(row_format);
    COPY_NODE_FIELD(colnames);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyRowCompareExpr
 */
static RowCompareExpr* _copyRowCompareExpr(const RowCompareExpr* from)
{
    RowCompareExpr* newnode = makeNode(RowCompareExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(rctype);
    COPY_NODE_FIELD(opnos);
    COPY_NODE_FIELD(opfamilies);
    COPY_NODE_FIELD(inputcollids);
    COPY_NODE_FIELD(largs);
    COPY_NODE_FIELD(rargs);

    return newnode;
}

/*
 * _copyCoalesceExpr
 */
static CoalesceExpr* _copyCoalesceExpr(const CoalesceExpr* from)
{
    CoalesceExpr* newnode = makeNode(CoalesceExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(coalescetype);
    COPY_SCALAR_FIELD(coalescecollid);
    COPY_NODE_FIELD(args);
    COPY_LOCATION_FIELD(location);
    // modify NVL display to A db's style "NVL" instead of "COALESCE"
    COPY_SCALAR_FIELD(isnvl);

    return newnode;
}

/*
 * _copyMinMaxExpr
 */
static MinMaxExpr* _copyMinMaxExpr(const MinMaxExpr* from)
{
    MinMaxExpr* newnode = makeNode(MinMaxExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(minmaxtype);
    COPY_SCALAR_FIELD(minmaxcollid);
    COPY_SCALAR_FIELD(inputcollid);
    COPY_SCALAR_FIELD(op);
    COPY_NODE_FIELD(args);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyXmlExpr
 */
static XmlExpr* _copyXmlExpr(const XmlExpr* from)
{
    XmlExpr* newnode = makeNode(XmlExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(op);
    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(named_args);
    COPY_NODE_FIELD(arg_names);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(xmloption);
    COPY_SCALAR_FIELD(type);
    COPY_SCALAR_FIELD(typmod);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyNullTest
 */
static NullTest* _copyNullTest(const NullTest* from)
{
    NullTest* newnode = makeNode(NullTest);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(nulltesttype);
    COPY_SCALAR_FIELD(argisrow);

    return newnode;
}

/*
 * _copyHashFilter
 */
static HashFilter* _copyHashFilter(const HashFilter* from)
{
    HashFilter* newnode = makeNode(HashFilter);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_NODE_FIELD(typeOids);
    COPY_NODE_FIELD(nodeList);

    return newnode;
}

/*
 * _copyBooleanTest
 */
static BooleanTest* _copyBooleanTest(const BooleanTest* from)
{
    BooleanTest* newnode = makeNode(BooleanTest);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(booltesttype);

    return newnode;
}

/*
 * _copyCoerceToDomain
 */
static CoerceToDomain* _copyCoerceToDomain(const CoerceToDomain* from)
{
    CoerceToDomain* newnode = makeNode(CoerceToDomain);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(resulttype);
    COPY_SCALAR_FIELD(resulttypmod);
    COPY_SCALAR_FIELD(resultcollid);
    COPY_SCALAR_FIELD(coercionformat);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCoerceToDomainValue
 */
static CoerceToDomainValue* _copyCoerceToDomainValue(const CoerceToDomainValue* from)
{
    CoerceToDomainValue* newnode = makeNode(CoerceToDomainValue);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(typeId);
    COPY_SCALAR_FIELD(typeMod);
    COPY_SCALAR_FIELD(collation);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copySetToDefault
 */
static SetToDefault* _copySetToDefault(const SetToDefault* from)
{
    SetToDefault* newnode = makeNode(SetToDefault);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(typeId);
    COPY_SCALAR_FIELD(typeMod);
    COPY_SCALAR_FIELD(collation);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * _copyCurrentOfExpr
 */
static CurrentOfExpr* _copyCurrentOfExpr(const CurrentOfExpr* from)
{
    CurrentOfExpr* newnode = makeNode(CurrentOfExpr);

    COPY_SCALAR_FIELD(xpr.selec);
    COPY_SCALAR_FIELD(cvarno);
    COPY_STRING_FIELD(cursor_name);
    COPY_SCALAR_FIELD(cursor_param);

    return newnode;
}

/*
 * _copyTargetEntry
 */
static TargetEntry* _copyTargetEntry(const TargetEntry* from)
{
    TargetEntry* newnode = makeNode(TargetEntry);

    COPY_NODE_FIELD(expr);
    COPY_SCALAR_FIELD(resno);
    COPY_STRING_FIELD(resname);
    COPY_SCALAR_FIELD(ressortgroupref);
    COPY_SCALAR_FIELD(resorigtbl);
    COPY_SCALAR_FIELD(resorigcol);
    COPY_SCALAR_FIELD(resjunk);

    return newnode;
}


/*
 * _copyPseudoTargetEntry
 */
static PseudoTargetEntry* _copyPseudoTargetEntry(const PseudoTargetEntry* from)
{
    PseudoTargetEntry* newnode = makeNode(PseudoTargetEntry);

    COPY_NODE_FIELD(tle);
    COPY_NODE_FIELD(srctle);

    return newnode;
}

/*
 * _copyRangeTblRef
 */
static RangeTblRef* _copyRangeTblRef(const RangeTblRef* from)
{
    RangeTblRef* newnode = makeNode(RangeTblRef);

    COPY_SCALAR_FIELD(rtindex);

    return newnode;
}

/*
 * _copyJoinExpr
 */
static JoinExpr* _copyJoinExpr(const JoinExpr* from)
{
    JoinExpr* newnode = makeNode(JoinExpr);

    COPY_SCALAR_FIELD(jointype);
    COPY_SCALAR_FIELD(isNatural);
    COPY_NODE_FIELD(larg);
    COPY_NODE_FIELD(rarg);
    COPY_NODE_FIELD(usingClause);
    COPY_NODE_FIELD(quals);
    COPY_NODE_FIELD(alias);
    COPY_SCALAR_FIELD(rtindex);

    return newnode;
}

/*
 * _copyFromExpr
 */
static FromExpr* _copyFromExpr(const FromExpr* from)
{
    FromExpr* newnode = makeNode(FromExpr);

    COPY_NODE_FIELD(fromlist);
    COPY_NODE_FIELD(quals);

    return newnode;
}

static PartitionState* _copyPartitionState(const PartitionState* from)
{
    PartitionState* newnode = makeNode(PartitionState);

    COPY_SCALAR_FIELD(partitionStrategy);
    COPY_NODE_FIELD(intervalPartDef);
    COPY_NODE_FIELD(partitionKey);
    COPY_NODE_FIELD(partitionList);
    COPY_SCALAR_FIELD(rowMovement);
    COPY_NODE_FIELD(subPartitionState);
    COPY_NODE_FIELD(partitionNameList);

    return newnode;
}

static RangePartitionDefState* _copyRangePartitionDefState(const RangePartitionDefState* from)
{
    RangePartitionDefState* newnode = makeNode(RangePartitionDefState);

    COPY_STRING_FIELD(partitionName);
    COPY_NODE_FIELD(boundary);
    COPY_STRING_FIELD(tablespacename);
    COPY_SCALAR_FIELD(curStartVal);
    COPY_STRING_FIELD(partitionInitName);
    COPY_NODE_FIELD(subPartitionDefState);

    return newnode;
}

static HashPartitionDefState* _copyHashPartitionDefState(const HashPartitionDefState* from)
{
    HashPartitionDefState* newnode = makeNode(HashPartitionDefState);

    COPY_STRING_FIELD(partitionName);
    COPY_NODE_FIELD(boundary);
    COPY_STRING_FIELD(tablespacename);
    COPY_NODE_FIELD(subPartitionDefState);

    return newnode;
}

static ListPartitionDefState* _copyListPartitionDefState(const ListPartitionDefState* from)
{
    ListPartitionDefState* newnode = makeNode(ListPartitionDefState);

    COPY_STRING_FIELD(partitionName);
    COPY_NODE_FIELD(boundary);
    COPY_STRING_FIELD(tablespacename);
    COPY_NODE_FIELD(subPartitionDefState);

    return newnode;
}

static IntervalPartitionDefState* _copyIntervalPartitionDefState(const IntervalPartitionDefState* from)
{
    IntervalPartitionDefState* newnode = makeNode(IntervalPartitionDefState);

    COPY_NODE_FIELD(partInterval);
    COPY_NODE_FIELD(intervalTablespaces);

    return newnode;
}
static SplitPartitionState* _copySplitPartitionState(const SplitPartitionState* from)
{
    SplitPartitionState* newnode = makeNode(SplitPartitionState);

    COPY_SCALAR_FIELD(splitType);
    COPY_STRING_FIELD(src_partition_name);
    COPY_NODE_FIELD(partition_for_values);
    COPY_NODE_FIELD(split_point);
    COPY_NODE_FIELD(dest_partition_define_list);
    COPY_NODE_FIELD(newListSubPartitionBoundry);

    return newnode;
}

static AddPartitionState* _copyAddPartitionState(const AddPartitionState* from)
{
    AddPartitionState* newnode = makeNode(AddPartitionState);

    COPY_NODE_FIELD(partitionList);
    COPY_SCALAR_FIELD(isStartEnd);

    return newnode;
}

static AddSubPartitionState* _copyAddSubPartitionState(const AddSubPartitionState* from)
{
    AddSubPartitionState* newnode = makeNode(AddSubPartitionState);

    COPY_STRING_FIELD(partitionName);
    COPY_NODE_FIELD(subPartitionList);

    return newnode;
}


static RangePartitionStartEndDefState* _copyRangePartitionStartEndDefState(const RangePartitionStartEndDefState* from)
{
    RangePartitionStartEndDefState* newnode = makeNode(RangePartitionStartEndDefState);

    COPY_STRING_FIELD(partitionName);
    COPY_NODE_FIELD(startValue);
    COPY_NODE_FIELD(endValue);
    COPY_NODE_FIELD(everyValue);
    COPY_STRING_FIELD(tableSpaceName);

    return newnode;
}

static AddTableIntoCBIState* _copyAddTblIntoCBIState(const AddTableIntoCBIState* from)
{
    AddTableIntoCBIState* newnode = makeNode(AddTableIntoCBIState);
    COPY_NODE_FIELD(relation);
    return newnode;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
static RangePartitionindexDefState* _copyRangePartitionindexDefState(const RangePartitionindexDefState* from)
{
    RangePartitionindexDefState* newnode = makeNode(RangePartitionindexDefState);

    COPY_STRING_FIELD(name);
    COPY_STRING_FIELD(tablespace);
    COPY_NODE_FIELD(sublist);

    return newnode;
}

/*
 * @MERGE INTO
 * _copyMergeAction
 */
static MergeAction* _copyMergeAction(const MergeAction* from)
{
    MergeAction* newnode = makeNode(MergeAction);

    COPY_SCALAR_FIELD(matched);
    COPY_NODE_FIELD(qual);
    COPY_SCALAR_FIELD(commandType);
    COPY_NODE_FIELD(targetList);
    COPY_NODE_FIELD(pulluped_targetList);

    return newnode;
}

/* ****************************************************************
 *						relation.h copy functions
 *
 * We don't support copying RelOptInfo, IndexOptInfo, or Path nodes.
 * There are some subsidiary structs that are useful to copy, though.
 * ****************************************************************
 */

/*
 * _copyPathKey
 */
static PathKey* _copyPathKey(const PathKey* from)
{
    PathKey* newnode = makeNode(PathKey);

    /* EquivalenceClasses are never moved, so just shallow-copy the pointer */
    COPY_SCALAR_FIELD(pk_eclass);
    COPY_SCALAR_FIELD(pk_opfamily);
    COPY_SCALAR_FIELD(pk_strategy);
    COPY_SCALAR_FIELD(pk_nulls_first);

    return newnode;
}

/*
 * _copyRestrictInfo
 */
static RestrictInfo* _copyRestrictInfo(const RestrictInfo* from)
{
    RestrictInfo* newnode = makeNode(RestrictInfo);

    COPY_NODE_FIELD(clause);
    COPY_SCALAR_FIELD(is_pushed_down);
    COPY_SCALAR_FIELD(outerjoin_delayed);
    COPY_SCALAR_FIELD(can_join);
    COPY_SCALAR_FIELD(pseudoconstant);
    COPY_SCALAR_FIELD(leakproof);
    COPY_SCALAR_FIELD(security_level);
    COPY_BITMAPSET_FIELD(clause_relids);
    COPY_BITMAPSET_FIELD(required_relids);
    COPY_BITMAPSET_FIELD(outer_relids);
    COPY_BITMAPSET_FIELD(nullable_relids);
    COPY_BITMAPSET_FIELD(left_relids);
    COPY_BITMAPSET_FIELD(right_relids);
    COPY_NODE_FIELD(orclause);
    /* EquivalenceClasses are never copied, so shallow-copy the pointers */
    COPY_SCALAR_FIELD(parent_ec);
    COPY_SCALAR_FIELD(eval_cost);
    COPY_SCALAR_FIELD(norm_selec);
    COPY_SCALAR_FIELD(outer_selec);
    COPY_NODE_FIELD(mergeopfamilies);
    /* EquivalenceClasses are never copied, so shallow-copy the pointers */
    COPY_SCALAR_FIELD(left_ec);
    COPY_SCALAR_FIELD(right_ec);
    COPY_SCALAR_FIELD(left_em);
    COPY_SCALAR_FIELD(right_em);
    /* MergeScanSelCache isn't a Node, so hard to copy; just reset cache */
    newnode->scansel_cache = NIL;
    COPY_SCALAR_FIELD(outer_is_left);
    COPY_SCALAR_FIELD(hashjoinoperator);
    COPY_SCALAR_FIELD(left_bucketsize);
    COPY_SCALAR_FIELD(right_bucketsize);
    return newnode;
}

/*
 * _copyPlaceHolderVar
 */
static PlaceHolderVar* _copyPlaceHolderVar(const PlaceHolderVar* from)
{
    PlaceHolderVar* newnode = makeNode(PlaceHolderVar);

    COPY_NODE_FIELD(phexpr);
    COPY_BITMAPSET_FIELD(phrels);
    COPY_SCALAR_FIELD(phid);
    COPY_SCALAR_FIELD(phlevelsup);

    return newnode;
}

/*
 * _copySpecialJoinInfo
 */
static SpecialJoinInfo* _copySpecialJoinInfo(const SpecialJoinInfo* from)
{
    SpecialJoinInfo* newnode = makeNode(SpecialJoinInfo);

    COPY_BITMAPSET_FIELD(min_lefthand);
    COPY_BITMAPSET_FIELD(min_righthand);
    COPY_BITMAPSET_FIELD(syn_lefthand);
    COPY_BITMAPSET_FIELD(syn_righthand);
    COPY_SCALAR_FIELD(jointype);
    COPY_SCALAR_FIELD(lhs_strict);
    COPY_SCALAR_FIELD(delay_upper_joins);
    COPY_NODE_FIELD(join_quals);

    return newnode;
}

/*
 * _copyLateralJoinInfo
 */
static LateralJoinInfo *
_copyLateralJoinInfo(const LateralJoinInfo *from)
{
   LateralJoinInfo *newnode = makeNode(LateralJoinInfo);

   COPY_SCALAR_FIELD(lateral_rhs);
   COPY_BITMAPSET_FIELD(lateral_lhs);

   return newnode;
}

/*
 * _copyAppendRelInfo
 */
static AppendRelInfo* _copyAppendRelInfo(const AppendRelInfo* from)
{
    AppendRelInfo* newnode = makeNode(AppendRelInfo);

    COPY_SCALAR_FIELD(parent_relid);
    COPY_SCALAR_FIELD(child_relid);
    COPY_SCALAR_FIELD(parent_reltype);
    COPY_SCALAR_FIELD(child_reltype);
    COPY_NODE_FIELD(translated_vars);
    COPY_SCALAR_FIELD(parent_reloid);

    return newnode;
}

/*
 * _copyPlaceHolderInfo
 */
static PlaceHolderInfo* _copyPlaceHolderInfo(const PlaceHolderInfo* from)
{
    PlaceHolderInfo* newnode = makeNode(PlaceHolderInfo);

    COPY_SCALAR_FIELD(phid);
    COPY_NODE_FIELD(ph_var);
    COPY_BITMAPSET_FIELD(ph_eval_at);
    COPY_BITMAPSET_FIELD(ph_needed);
    COPY_SCALAR_FIELD(ph_width);

    return newnode;
}

/* ****************************************************************
 *					parsenodes.h copy functions
 * ****************************************************************
 */

static RangeTblEntry* _copyRangeTblEntry(const RangeTblEntry* from)
{
    RangeTblEntry* newnode = makeNode(RangeTblEntry);

    COPY_SCALAR_FIELD(rtekind);

#ifdef PGXC
    COPY_STRING_FIELD(relname);
    COPY_NODE_FIELD(partAttrNum);
#endif

    COPY_SCALAR_FIELD(relid);
    COPY_SCALAR_FIELD(partitionOid);
    COPY_SCALAR_FIELD(isContainPartition);
    COPY_SCALAR_FIELD(subpartitionOid);
    COPY_SCALAR_FIELD(isContainSubPartition);
    COPY_SCALAR_FIELD(refSynOid);
    COPY_NODE_FIELD(partid_list);
    COPY_SCALAR_FIELD(relkind);
    COPY_SCALAR_FIELD(isResultRel);
    COPY_NODE_FIELD(tablesample);
    COPY_NODE_FIELD(timecapsule);
    COPY_SCALAR_FIELD(ispartrel);
    COPY_SCALAR_FIELD(ignoreResetRelid);
    COPY_NODE_FIELD(subquery);
    COPY_SCALAR_FIELD(security_barrier);
    COPY_SCALAR_FIELD(jointype);
    COPY_NODE_FIELD(joinaliasvars);
    COPY_NODE_FIELD(funcexpr);
    COPY_NODE_FIELD(funccoltypes);
    COPY_NODE_FIELD(funccoltypmods);
    COPY_NODE_FIELD(funccolcollations);
    COPY_NODE_FIELD(values_lists);
    COPY_NODE_FIELD(values_collations);
    COPY_STRING_FIELD(ctename);
    COPY_SCALAR_FIELD(ctelevelsup);
    COPY_SCALAR_FIELD(self_reference);
    COPY_SCALAR_FIELD(cterecursive);
    COPY_NODE_FIELD(ctecoltypes);
    COPY_NODE_FIELD(ctecoltypmods);
    COPY_NODE_FIELD(ctecolcollations);
    COPY_SCALAR_FIELD(swConverted);
    COPY_NODE_FIELD(origin_index);
    COPY_SCALAR_FIELD(swAborted);
    COPY_SCALAR_FIELD(swSubExist);
    COPY_SCALAR_FIELD(locator_type);
    COPY_NODE_FIELD(alias);
    COPY_NODE_FIELD(eref);
    COPY_NODE_FIELD(pname);
    COPY_NODE_FIELD(plist);
    COPY_SCALAR_FIELD(lateral);
    COPY_SCALAR_FIELD(inh);
    COPY_SCALAR_FIELD(inFromCl);
    COPY_SCALAR_FIELD(requiredPerms);
    COPY_SCALAR_FIELD(checkAsUser);
    COPY_BITMAPSET_FIELD(selectedCols);
    COPY_BITMAPSET_FIELD(modifiedCols);
    COPY_BITMAPSET_FIELD(insertedCols);
    COPY_BITMAPSET_FIELD(updatedCols);
    COPY_BITMAPSET_FIELD(extraUpdatedCols);
    COPY_SCALAR_FIELD(orientation);
    COPY_STRING_FIELD(mainRelName);
    COPY_STRING_FIELD(mainRelNameSpace);
    COPY_NODE_FIELD(securityQuals);
    COPY_SCALAR_FIELD(subquery_pull_up);
    COPY_SCALAR_FIELD(correlated_with_recursive_cte);
    COPY_SCALAR_FIELD(relhasbucket);
    COPY_SCALAR_FIELD(isbucket);
    COPY_SCALAR_FIELD(bucketmapsize);
    COPY_NODE_FIELD(buckets);
    COPY_SCALAR_FIELD(isexcluded);
    COPY_SCALAR_FIELD(sublink_pull_up);
    COPY_SCALAR_FIELD(is_ustore);
    COPY_SCALAR_FIELD(pulled_from_subquery);

    return newnode;
}

/*
 * Description: Copy TableSampleClause node
 *
 * Parameters:
 *	@in from: source TableSampleClause node
 *
 * Return: TableSampleClause*
 */
static TableSampleClause* _copyTableSampleClause(const TableSampleClause* from)
{
    TableSampleClause* newnode = makeNode(TableSampleClause);

    COPY_SCALAR_FIELD(sampleType);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(repeatable);

    return newnode;
}

/*
 * Description: Copy TimeCapsuleClause node
 *
 * Parameters:
 *     @in from: source TimeCapsuleClause node
 *
 * Return: TimeCapsuleClause*
 */
static TimeCapsuleClause* CopyTimeCapsuleClause(const TimeCapsuleClause* from)
{
    TimeCapsuleClause* newnode = makeNode(TimeCapsuleClause);

    COPY_SCALAR_FIELD(tvtype);
    COPY_NODE_FIELD(tvver);

    return newnode;
}

static SortGroupClause* _copySortGroupClause(const SortGroupClause* from)
{
    SortGroupClause* newnode = makeNode(SortGroupClause);

    COPY_SCALAR_FIELD(tleSortGroupRef);
    COPY_SCALAR_FIELD(eqop);
    COPY_SCALAR_FIELD(sortop);
    COPY_SCALAR_FIELD(nulls_first);
    COPY_SCALAR_FIELD(hashable);
    COPY_SCALAR_FIELD(groupSet);

    return newnode;
}

static WindowClause* _copyWindowClause(const WindowClause* from)
{
    WindowClause* newnode = makeNode(WindowClause);

    COPY_STRING_FIELD(name);
    COPY_STRING_FIELD(refname);
    COPY_NODE_FIELD(partitionClause);
    COPY_NODE_FIELD(orderClause);
    COPY_SCALAR_FIELD(frameOptions);
    COPY_NODE_FIELD(startOffset);
    COPY_NODE_FIELD(endOffset);
    COPY_SCALAR_FIELD(winref);
    COPY_SCALAR_FIELD(copiedOrder);

    return newnode;
}

static RowMarkClause* _copyRowMarkClause(const RowMarkClause* from)
{
    RowMarkClause* newnode = makeNode(RowMarkClause);

    COPY_SCALAR_FIELD(rti);
    COPY_SCALAR_FIELD(forUpdate);
    COPY_SCALAR_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COPY_SCALAR_FIELD(waitSec);
    }
    COPY_SCALAR_FIELD(pushedDown);
    if (t_thrd.proc->workingVersionNum >= ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        COPY_SCALAR_FIELD(strength);
    }

    return newnode;
}

static WithClause* _copyWithClause(const WithClause* from)
{
    WithClause* newnode = makeNode(WithClause);

    COPY_NODE_FIELD(ctes);
    COPY_SCALAR_FIELD(recursive);
    COPY_LOCATION_FIELD(location);

    COPY_NODE_FIELD(sw_clause);

    return newnode;
}

static UpsertClause* _copyUpsertClause(const UpsertClause* from)
{
    UpsertClause* newnode = makeNode(UpsertClause);

    COPY_NODE_FIELD(targetList);
    COPY_LOCATION_FIELD(location);
    COPY_NODE_FIELD(whereClause);

    return newnode;
}

static UpsertExpr* _copyUpsertExpr(const UpsertExpr* from)
{
    UpsertExpr* newnode = makeNode(UpsertExpr);

    COPY_SCALAR_FIELD(upsertAction);
    COPY_NODE_FIELD(updateTlist);
    COPY_NODE_FIELD(exclRelTlist);
    COPY_SCALAR_FIELD(exclRelIndex);
    COPY_NODE_FIELD(upsertWhere);

    return newnode;
}

static StartWithTargetRelInfo* _copyStartWithTargetRelInfo(const StartWithTargetRelInfo* from)
{
    StartWithTargetRelInfo* newnode = makeNode(StartWithTargetRelInfo);

    COPY_STRING_FIELD(relname);
    COPY_STRING_FIELD(aliasname);
    COPY_STRING_FIELD(ctename);
    COPY_NODE_FIELD(columns);
    COPY_NODE_FIELD(tblstmt);

    COPY_SCALAR_FIELD(rtekind);
    COPY_NODE_FIELD(rte);
    COPY_NODE_FIELD(rtr);

    return newnode;
}

static CommonTableExpr* _copyCommonTableExpr(const CommonTableExpr* from)
{
    CommonTableExpr* newnode = makeNode(CommonTableExpr);

    COPY_STRING_FIELD(ctename);
    COPY_NODE_FIELD(aliascolnames);
    COPY_SCALAR_FIELD(ctematerialized);
    COPY_NODE_FIELD(ctequery);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(cterecursive);
    COPY_SCALAR_FIELD(cterefcount);
    COPY_NODE_FIELD(ctecolnames);
    COPY_NODE_FIELD(ctecoltypes);
    COPY_NODE_FIELD(ctecoltypmods);
    COPY_NODE_FIELD(ctecolcollations);
    COPY_SCALAR_FIELD(locator_type);
    COPY_SCALAR_FIELD(self_reference);
    COPY_SCALAR_FIELD(referenced_by_subquery);

    /* copy start-with specific part */
    COPY_NODE_FIELD(swoptions);

    return newnode;
}

static StartWithOptions *_copyStartWithOptions(const StartWithOptions* from)
{
    StartWithOptions* newnode = makeNode(StartWithOptions);

    COPY_NODE_FIELD(siblings_orderby_clause);
    COPY_NODE_FIELD(prior_key_index);
    COPY_SCALAR_FIELD(connect_by_type);
    COPY_NODE_FIELD(connect_by_level_quals);
    COPY_NODE_FIELD(connect_by_other_quals);
    COPY_SCALAR_FIELD(nocycle);

    return newnode;
}

static A_Expr* _copyAExpr(const A_Expr* from)
{
    A_Expr* newnode = makeNode(A_Expr);

    COPY_SCALAR_FIELD(kind);
    COPY_NODE_FIELD(name);
    COPY_NODE_FIELD(lexpr);
    COPY_NODE_FIELD(rexpr);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static ColumnRef* _copyColumnRef(const ColumnRef* from)
{
    ColumnRef* newnode = makeNode(ColumnRef);

    COPY_NODE_FIELD(fields);
    COPY_SCALAR_FIELD(prior);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static ParamRef* _copyParamRef(const ParamRef* from)
{
    ParamRef* newnode = makeNode(ParamRef);

    COPY_SCALAR_FIELD(number);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static A_Const* _copyAConst(const A_Const* from)
{
    A_Const* newnode = makeNode(A_Const);

    /* This part must duplicate _copyValue */
    COPY_SCALAR_FIELD(val.type);
    switch (from->val.type) {
        case T_Integer:
            COPY_SCALAR_FIELD(val.val.ival);
            break;
        case T_Float:
        case T_String:
        case T_BitString:
            COPY_STRING_FIELD(val.val.str);
            break;
        case T_Null:
            /* nothing to do */
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)from->val.type)));
            break;
    }

    COPY_LOCATION_FIELD(location);

    return newnode;
}

static FuncCall* _copyFuncCall(const FuncCall* from)
{
    FuncCall* newnode = makeNode(FuncCall);

    COPY_NODE_FIELD(funcname);
    COPY_STRING_FIELD(colname);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(agg_order);
    COPY_SCALAR_FIELD(agg_star);
    COPY_SCALAR_FIELD(agg_distinct);
    COPY_SCALAR_FIELD(func_variadic);
    COPY_SCALAR_FIELD(agg_within_group);
    COPY_NODE_FIELD(over);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(call_func);

    return newnode;
}

static A_Star* _copyAStar(const A_Star* from)
{
    A_Star* newnode = makeNode(A_Star);

    return newnode;
}

static A_Indices* _copyAIndices(const A_Indices* from)
{
    A_Indices* newnode = makeNode(A_Indices);

    COPY_NODE_FIELD(lidx);
    COPY_NODE_FIELD(uidx);

    return newnode;
}

static A_Indirection* _copyA_Indirection(const A_Indirection* from)
{
    A_Indirection* newnode = makeNode(A_Indirection);

    COPY_NODE_FIELD(arg);
    COPY_NODE_FIELD(indirection);

    return newnode;
}

static A_ArrayExpr* _copyA_ArrayExpr(const A_ArrayExpr* from)
{
    A_ArrayExpr* newnode = makeNode(A_ArrayExpr);

    COPY_NODE_FIELD(elements);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static ResTarget* _copyResTarget(const ResTarget* from)
{
    ResTarget* newnode = makeNode(ResTarget);

    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(indirection);
    COPY_NODE_FIELD(val);
    COPY_LOCATION_FIELD(location);

    return newnode;
}
#endif
static TypeName* _copyTypeName(const TypeName* from)
{
    TypeName* newnode = makeNode(TypeName);

    COPY_NODE_FIELD(names);
    COPY_SCALAR_FIELD(typeOid);
    COPY_SCALAR_FIELD(setof);
    COPY_SCALAR_FIELD(pct_type);
    COPY_NODE_FIELD(typmods);
    COPY_SCALAR_FIELD(typemod);
    COPY_NODE_FIELD(arrayBounds);
    COPY_LOCATION_FIELD(location);
    COPY_LOCATION_FIELD(end_location);
    COPY_SCALAR_FIELD(pct_rowtype);

    return newnode;
}
#ifndef FRONTEND_PARSER
static SortBy* _copySortBy(const SortBy* from)
{
    SortBy* newnode = makeNode(SortBy);

    COPY_NODE_FIELD(node);
    COPY_SCALAR_FIELD(sortby_dir);
    COPY_SCALAR_FIELD(sortby_nulls);
    COPY_NODE_FIELD(useOp);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static WindowDef* _copyWindowDef(const WindowDef* from)
{
    WindowDef* newnode = makeNode(WindowDef);

    COPY_STRING_FIELD(name);
    COPY_STRING_FIELD(refname);
    COPY_NODE_FIELD(partitionClause);
    COPY_NODE_FIELD(orderClause);
    COPY_SCALAR_FIELD(frameOptions);
    COPY_NODE_FIELD(startOffset);
    COPY_NODE_FIELD(endOffset);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static RangeSubselect* _copyRangeSubselect(const RangeSubselect* from)
{
    RangeSubselect* newnode = makeNode(RangeSubselect);

    COPY_SCALAR_FIELD(lateral);
    COPY_NODE_FIELD(subquery);
    COPY_NODE_FIELD(alias);

    return newnode;
}

static RangeFunction* _copyRangeFunction(const RangeFunction* from)
{
    RangeFunction* newnode = makeNode(RangeFunction);

    COPY_SCALAR_FIELD(lateral);
    COPY_NODE_FIELD(funccallnode);
    COPY_NODE_FIELD(alias);
    COPY_NODE_FIELD(coldeflist);

    return newnode;
}

/*
 * Description: Copy RangeTableSample node
 *
 * Parameters:
 *	@in from: source RangeTableSample node
 *
 * Return: RangeTableSample*
 */
static RangeTableSample* _copyRangeTableSample(const RangeTableSample* from)
{
    RangeTableSample* newnode = makeNode(RangeTableSample);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(method);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(repeatable);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * Description: Copy RangeTimeCapsule node
 *
 * Parameters:
 *    @in from: source RangeTimeCapsule node
 *
 * Return: RangeTimeCapsule*
 */
static RangeTimeCapsule* CopyRangeTimeCapsule(const RangeTimeCapsule* from)
{
    RangeTimeCapsule* newnode = makeNode(RangeTimeCapsule);

    COPY_NODE_FIELD(relation);
    COPY_SCALAR_FIELD(tvtype);
    COPY_NODE_FIELD(tvver);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static TypeCast* _copyTypeCast(const TypeCast* from)
{
    TypeCast* newnode = makeNode(TypeCast);

    COPY_NODE_FIELD(arg);
    COPY_NODE_FIELD(typname);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static CollateClause* _copyCollateClause(const CollateClause* from)
{
    CollateClause* newnode = makeNode(CollateClause);

    COPY_NODE_FIELD(arg);
    COPY_NODE_FIELD(collname);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static ClientLogicColumnParam* _copyColumnParam (const ClientLogicColumnParam* from)
{
    ClientLogicColumnParam* newnode = makeNode(ClientLogicColumnParam);
    COPY_SCALAR_FIELD(key);
    COPY_STRING_FIELD(value);
    COPY_NODE_FIELD(qualname);
    COPY_SCALAR_FIELD(len);
    COPY_LOCATION_FIELD(location);
    return newnode;
}

static ClientLogicGlobalParam* _copyGlobalParam (const ClientLogicGlobalParam* from)
{
    ClientLogicGlobalParam* newnode = makeNode(ClientLogicGlobalParam);
    COPY_SCALAR_FIELD(key);
    COPY_STRING_FIELD(value);
    COPY_SCALAR_FIELD(len);
    COPY_LOCATION_FIELD(location);
    return newnode;
}

static CreateClientLogicGlobal* _copyGlobalSetting (const CreateClientLogicGlobal* from)
{
    CreateClientLogicGlobal *newnode = makeNode(CreateClientLogicGlobal);
    COPY_NODE_FIELD(global_key_name);
    COPY_NODE_FIELD(global_setting_params);
    return newnode;
}
static CreateClientLogicColumn* _copyColumnSetting (const CreateClientLogicColumn* from)
{
    CreateClientLogicColumn* newnode = makeNode(CreateClientLogicColumn);
    COPY_NODE_FIELD(column_key_name);
    COPY_NODE_FIELD(column_setting_params);
    return newnode;
}

static ClientLogicColumnRef* _copyEncryptedColumn(const ClientLogicColumnRef *from)
{
    ClientLogicColumnRef* newnode = makeNode(ClientLogicColumnRef);

    COPY_NODE_FIELD(column_key_name);
    COPY_SCALAR_FIELD(columnEncryptionAlgorithmType);
    COPY_NODE_FIELD(orig_typname);
    COPY_NODE_FIELD(dest_typname);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

static IndexElem* _copyIndexElem(const IndexElem* from)
{
    IndexElem* newnode = makeNode(IndexElem);

    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(expr);
    COPY_STRING_FIELD(indexcolname);
    COPY_NODE_FIELD(collation);
    COPY_NODE_FIELD(opclass);
    COPY_SCALAR_FIELD(ordering);
    COPY_SCALAR_FIELD(nulls_ordering);

    return newnode;
}

static ColumnDef* _copyColumnDef(const ColumnDef* from)
{
    ColumnDef* newnode = makeNode(ColumnDef);

    COPY_STRING_FIELD(colname);
    COPY_NODE_FIELD(typname);
    COPY_SCALAR_FIELD(kvtype);
    COPY_SCALAR_FIELD(inhcount);
    COPY_SCALAR_FIELD(is_local);
    COPY_SCALAR_FIELD(is_not_null);
    COPY_SCALAR_FIELD(is_from_type);
    COPY_SCALAR_FIELD(is_serial);
    COPY_SCALAR_FIELD(storage);
    COPY_SCALAR_FIELD(cmprs_mode);
    COPY_NODE_FIELD(raw_default);
    COPY_SCALAR_FIELD(generatedCol);
    COPY_NODE_FIELD(cooked_default);
    COPY_NODE_FIELD(collClause);
    COPY_SCALAR_FIELD(collOid);
    COPY_NODE_FIELD(constraints);
    COPY_NODE_FIELD(fdwoptions);
    COPY_NODE_FIELD(clientLogicColumnRef);
    COPY_NODE_FIELD(position);

    return newnode;
}

static Position* _copyPosition(const Position* from)
{
    Position* newnode = makeNode(Position);

    COPY_STRING_FIELD(colname);
    COPY_SCALAR_FIELD(fixedlen);
    COPY_SCALAR_FIELD(position);
    return newnode;
}

static Constraint* _copyConstraint(const Constraint* from)
{
    Constraint* newnode = makeNode(Constraint);

    COPY_SCALAR_FIELD(contype);
    COPY_STRING_FIELD(conname);
    COPY_SCALAR_FIELD(deferrable);
    COPY_SCALAR_FIELD(initdeferred);
    COPY_LOCATION_FIELD(location);
    COPY_SCALAR_FIELD(is_no_inherit);
    COPY_NODE_FIELD(raw_expr);
    COPY_STRING_FIELD(cooked_expr);
    COPY_NODE_FIELD(keys);
    COPY_NODE_FIELD(including);
    COPY_NODE_FIELD(exclusions);
    COPY_NODE_FIELD(options);
    COPY_STRING_FIELD(indexname);
    COPY_STRING_FIELD(indexspace);
    COPY_STRING_FIELD(access_method);
    COPY_NODE_FIELD(where_clause);
    COPY_NODE_FIELD(pktable);
    COPY_NODE_FIELD(fk_attrs);
    COPY_NODE_FIELD(pk_attrs);
    COPY_SCALAR_FIELD(fk_matchtype);
    COPY_SCALAR_FIELD(fk_upd_action);
    COPY_SCALAR_FIELD(fk_del_action);
    COPY_NODE_FIELD(old_conpfeqop);
    COPY_SCALAR_FIELD(old_pktable_oid);
    COPY_SCALAR_FIELD(skip_validation);
    COPY_SCALAR_FIELD(initially_valid);
    COPY_NODE_FIELD(inforConstraint);

    return newnode;
}

static DefElem* _copyDefElem(const DefElem* from)
{
    DefElem* newnode = makeNode(DefElem);

    COPY_STRING_FIELD(defnamespace);
    COPY_STRING_FIELD(defname);
    COPY_NODE_FIELD(arg);
    COPY_SCALAR_FIELD(defaction);
    COPY_SCALAR_FIELD(begin_location);
    COPY_SCALAR_FIELD(end_location);

    return newnode;
}

static LockingClause* _copyLockingClause(const LockingClause* from)
{
    LockingClause* newnode = makeNode(LockingClause);

    COPY_NODE_FIELD(lockedRels);
    COPY_SCALAR_FIELD(forUpdate);
    COPY_SCALAR_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        COPY_SCALAR_FIELD(strength);
    }
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COPY_SCALAR_FIELD(waitSec);
    }

    return newnode;
}

static XmlSerialize* _copyXmlSerialize(const XmlSerialize* from)
{
    XmlSerialize* newnode = makeNode(XmlSerialize);

    COPY_SCALAR_FIELD(xmloption);
    COPY_NODE_FIELD(expr);
    COPY_NODE_FIELD(typname);
    COPY_LOCATION_FIELD(location);

    return newnode;
}

/*
 * @Description: Copy hint filelds.
 * @in from: Source hint.
 * @out newnode: Target hint.
 */
static void CopyBaseHintFilelds(const Hint* from, Hint* newnode)
{
    COPY_NODE_FIELD(relnames);
    COPY_SCALAR_FIELD(hint_keyword);
    COPY_SCALAR_FIELD(state);
}

static NoExpandHint* _copyNoExpandHint(const NoExpandHint* from)
{
    NoExpandHint* newnode = makeNode(NoExpandHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    return newnode;
}

static SetHint* _copySetHint(const SetHint* from)
{
    SetHint* newnode = makeNode(SetHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_STRING_FIELD(name);
    COPY_STRING_FIELD(value);
    return newnode;
}

static PlanCacheHint* _copyPlanCacheHint(const PlanCacheHint* from)
{
    PlanCacheHint* newnode = makeNode(PlanCacheHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_SCALAR_FIELD(chooseCustomPlan);
    return newnode;
}

static NoGPCHint* _copyNoGPCHint(const NoGPCHint* from)
{
    NoGPCHint* newnode = makeNode(NoGPCHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    return newnode;
}

/*
 * @Description: Copy hint filelds.
 * @in from: Source hint.
 * @out newnode: Target hint.
 */
static PredpushHint* _copyPredpushHint(const PredpushHint* from)
{
    PredpushHint* newnode = makeNode(PredpushHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_SCALAR_FIELD(negative);
    COPY_STRING_FIELD(dest_name);
    COPY_SCALAR_FIELD(dest_id);
    COPY_BITMAPSET_FIELD(candidates);

    return newnode;
}

/*
 * @Description: Copy hint filelds.
 * @in from: Source hint.
 * @out newnode: Target hint.
 */
static PredpushSameLevelHint* _copyPredpushSameLevelHint(const PredpushSameLevelHint* from)
{
    PredpushSameLevelHint* newnode = makeNode(PredpushSameLevelHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_SCALAR_FIELD(negative);
    COPY_STRING_FIELD(dest_name);
    COPY_SCALAR_FIELD(dest_id);
    COPY_BITMAPSET_FIELD(candidates);

    return newnode;
}

/*
 * @Description: Copy hint filelds.
 * @in from: Source hint.
 * @out newnode: Target hint.
 */
static RewriteHint* _copyRewriteHint(const RewriteHint* from)
{
    RewriteHint* newnode = makeNode(RewriteHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_NODE_FIELD(param_names);
    COPY_SCALAR_FIELD(param_bits);
    return newnode;
}

/*
 * @Description: Copy hint filelds.
 * @in from: Source hint.
 * @out newnode: Target hint.
 */
static GatherHint* _copyGatherHint(const GatherHint* from)
{
    GatherHint* newnode = makeNode(GatherHint);
    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_SCALAR_FIELD(source);
    return newnode;
}

/*
 * @Description: Copy join hint struct.
 * @in from: Source join hint.
 * @return: JoinMethodHint struct.
 */
static JoinMethodHint* _copyJoinHint(const JoinMethodHint* from)
{
    JoinMethodHint* newnode = makeNode(JoinMethodHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);

    COPY_SCALAR_FIELD(negative);
    COPY_BITMAPSET_FIELD(joinrelids);
    COPY_BITMAPSET_FIELD(inner_joinrelids);

    return newnode;
}

/*
 * @Description: Copy rows hint struct.
 * @in from: Source rows hint.
 * @return: RowsHint struct.
 */
static RowsHint* _copyRowsHint(const RowsHint* from)
{
    RowsHint* newnode = makeNode(RowsHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_BITMAPSET_FIELD(joinrelids);
    COPY_STRING_FIELD(rows_str);
    COPY_SCALAR_FIELD(value_type);
    COPY_SCALAR_FIELD(rows);

    return newnode;
}

/*
 * @Description: Copy stream hint struct.
 * @in from: Source stream hint.
 * @return: StreamHint struct.
 */
static StreamHint* _copyStreamHint(const StreamHint* from)
{
    StreamHint* newnode = makeNode(StreamHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_SCALAR_FIELD(negative);
    COPY_BITMAPSET_FIELD(joinrelids);
    COPY_SCALAR_FIELD(stream_type);

    return newnode;
}

static BlockNameHint* _copyBlockNameHint(const BlockNameHint* from)
{
    BlockNameHint* newnode = makeNode(BlockNameHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);

    return newnode;
}

static ScanMethodHint* _copyScanMethodHint(const ScanMethodHint* from)
{
    ScanMethodHint* newnode = makeNode(ScanMethodHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_SCALAR_FIELD(negative);
    COPY_BITMAPSET_FIELD(relid);
    COPY_NODE_FIELD(indexlist);

    return newnode;
}

static PgFdwRemoteInfo* _copyPgFdwRemoteInfo(const PgFdwRemoteInfo* from)
{
    PgFdwRemoteInfo* newnode = makeNode(PgFdwRemoteInfo);

    COPY_SCALAR_FIELD(reltype);
    COPY_SCALAR_FIELD(datanodenum);
    COPY_SCALAR_FIELD(snapsize);

    newnode->snapshot = (Snapshot)palloc0(newnode->snapsize + 1);
    errno_t rc = memcpy_s((char*)newnode->snapshot, newnode->snapsize, (char*)from->snapshot, newnode->snapsize);
    securec_check_c(rc, "\0", "\0");

    return newnode;
}

static SkewHint* _copySkewHint(const SkewHint* from)
{
    SkewHint* newnode = makeNode(SkewHint);

    COPY_BITMAPSET_FIELD(relid);
    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);
    COPY_NODE_FIELD(column_list);
    COPY_NODE_FIELD(value_list);

    return newnode;
}

static SkewRelInfo* _copySkewRelInfo(const SkewRelInfo* from)
{
    SkewRelInfo* newnode = makeNode(SkewRelInfo);

    COPY_STRING_FIELD(relation_name);
    COPY_SCALAR_FIELD(relation_oid);
    COPY_NODE_FIELD(rte);
    COPY_NODE_FIELD(parent_rte);

    return newnode;
}

static SkewColumnInfo* _copySkewColumnInfo(const SkewColumnInfo* from)
{
    SkewColumnInfo* newnode = makeNode(SkewColumnInfo);

    COPY_SCALAR_FIELD(relation_Oid);
    COPY_STRING_FIELD(column_name);
    COPY_SCALAR_FIELD(attnum);
    COPY_SCALAR_FIELD(column_typid);
    COPY_NODE_FIELD(expr);

    return newnode;
}

static SkewValueInfo* _copySkewValueInfo(const SkewValueInfo* from)
{
    SkewValueInfo* newnode = makeNode(SkewValueInfo);

    COPY_SCALAR_FIELD(support_redis);
    COPY_NODE_FIELD(const_value);

    return newnode;
}

static CopyColExpr* _copyCopyColExpr(const CopyColExpr* from)
{
    CopyColExpr* newnode = makeNode(CopyColExpr);

    COPY_STRING_FIELD(colname);
    COPY_NODE_FIELD(typname);
    COPY_NODE_FIELD(colexpr);

    return newnode;
}

static SkewHintTransf* _copySkewHintTransf(const SkewHintTransf* from)
{
    SkewHintTransf* newnode = makeNode(SkewHintTransf);

    COPY_NODE_FIELD(before);
    COPY_NODE_FIELD(rel_info_list);
    COPY_NODE_FIELD(column_info_list);
    COPY_NODE_FIELD(value_info_list);

    return newnode;
}

/*
 * @Descripton: Copy leading hint.
 * @in from: Source hint.
 * @return: Leading hint.
 */
static LeadingHint* _copyLeadingHint(const LeadingHint* from)
{
    LeadingHint* newnode = makeNode(LeadingHint);

    CopyBaseHintFilelds((const Hint*)from, (Hint*)newnode);

    COPY_SCALAR_FIELD(join_order_hint);

    return newnode;
}

/*
 * @Description: Copy Hintstate.
 * @in from: HintState source.
 * @return: Hintstate struct.
 */
static HintState* _copyHintState(const HintState* from)
{
    HintState* newnode = makeNode(HintState);

    COPY_SCALAR_FIELD(nall_hints);

    COPY_NODE_FIELD(join_hint);
    COPY_NODE_FIELD(leading_hint);
    COPY_NODE_FIELD(row_hint);
    COPY_NODE_FIELD(stream_hint);
    COPY_NODE_FIELD(block_name_hint);
    COPY_NODE_FIELD(scan_hint);
    COPY_NODE_FIELD(skew_hint);
    COPY_SCALAR_FIELD(multi_node_hint);
    COPY_NODE_FIELD(hint_warning);
    COPY_NODE_FIELD(predpush_hint);
    COPY_NODE_FIELD(rewrite_hint);
    COPY_NODE_FIELD(gather_hint);
    COPY_NODE_FIELD(no_expand_hint);
    COPY_NODE_FIELD(set_hint);
    COPY_NODE_FIELD(cache_plan_hint);
    COPY_NODE_FIELD(no_gpc_hint);
    COPY_NODE_FIELD(predpush_same_level_hint);

    return newnode;
}

static QualSkewInfo* _copyQualSkewInfo(const QualSkewInfo* from)
{
    QualSkewInfo* newnode = makeNode(QualSkewInfo);

    COPY_SCALAR_FIELD(skew_stream_type);
    COPY_NODE_FIELD(skew_quals);
    COPY_SCALAR_FIELD(qual_cost.startup);
    COPY_SCALAR_FIELD(qual_cost.per_tuple);
    COPY_SCALAR_FIELD(broadcast_ratio);

    return newnode;
}

static Query* _copyQuery(const Query* from)
{
    Query* newnode = makeNode(Query);

    COPY_SCALAR_FIELD(commandType);
    COPY_SCALAR_FIELD(querySource);
    COPY_SCALAR_FIELD(queryId);
    COPY_SCALAR_FIELD(canSetTag);
    COPY_NODE_FIELD(utilityStmt);
    COPY_SCALAR_FIELD(resultRelation);
    COPY_SCALAR_FIELD(hasAggs);
    COPY_SCALAR_FIELD(hasWindowFuncs);
    COPY_SCALAR_FIELD(hasSubLinks);
    COPY_SCALAR_FIELD(hasDistinctOn);
    COPY_SCALAR_FIELD(hasRecursive);
    COPY_SCALAR_FIELD(hasModifyingCTE);
    COPY_SCALAR_FIELD(hasForUpdate);
    COPY_SCALAR_FIELD(hasRowSecurity);
    COPY_SCALAR_FIELD(hasSynonyms);
    COPY_NODE_FIELD(cteList);
    COPY_NODE_FIELD(rtable);
    COPY_NODE_FIELD(jointree);
    COPY_NODE_FIELD(targetList);
    COPY_NODE_FIELD(starStart);
    COPY_NODE_FIELD(starEnd);
    COPY_NODE_FIELD(starOnly);
    COPY_NODE_FIELD(returningList);
    COPY_NODE_FIELD(groupClause);
    COPY_NODE_FIELD(groupingSets);
    COPY_NODE_FIELD(havingQual);
    COPY_NODE_FIELD(windowClause);
    COPY_NODE_FIELD(distinctClause);
    COPY_NODE_FIELD(sortClause);
    COPY_NODE_FIELD(limitOffset);
    COPY_NODE_FIELD(limitCount);
    COPY_NODE_FIELD(rowMarks);
    COPY_NODE_FIELD(setOperations);
    COPY_NODE_FIELD(constraintDeps);
    COPY_NODE_FIELD(hintState);
#ifdef PGXC
    COPY_STRING_FIELD(sql_statement);
    COPY_SCALAR_FIELD(is_local);
    COPY_SCALAR_FIELD(has_to_save_cmd_id);
    COPY_SCALAR_FIELD(vec_output);
    COPY_SCALAR_FIELD(tdTruncCastStatus);
    COPY_NODE_FIELD(equalVars);
#endif
    COPY_SCALAR_FIELD(mergeTarget_relation);
    COPY_NODE_FIELD(mergeSourceTargetList);
    COPY_NODE_FIELD(mergeActionList);
    COPY_NODE_FIELD(upsertQuery);
    COPY_NODE_FIELD(upsertClause);
    COPY_SCALAR_FIELD(isRowTriggerShippable);
    COPY_SCALAR_FIELD(use_star_targets);
    COPY_SCALAR_FIELD(is_from_full_join_rewrite);
    COPY_SCALAR_FIELD(uniqueSQLId);
#ifndef ENABLE_MULTIPLE_NODES
    COPY_STRING_FIELD(unique_sql_text);
#endif
    COPY_SCALAR_FIELD(can_push);
    COPY_SCALAR_FIELD(unique_check);

    return newnode;
}

static InsertStmt* _copyInsertStmt(const InsertStmt* from)
{
    InsertStmt* newnode = makeNode(InsertStmt);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(cols);
    COPY_NODE_FIELD(selectStmt);
    COPY_NODE_FIELD(returningList);
    COPY_NODE_FIELD(withClause);
    COPY_NODE_FIELD(upsertClause);
    COPY_NODE_FIELD(hintState);
    COPY_SCALAR_FIELD(isRewritten);
    return newnode;
}

static DeleteStmt* _copyDeleteStmt(const DeleteStmt* from)
{
    DeleteStmt* newnode = makeNode(DeleteStmt);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(usingClause);
    COPY_NODE_FIELD(whereClause);
    COPY_NODE_FIELD(returningList);
    COPY_NODE_FIELD(withClause);
    COPY_NODE_FIELD(limitClause);
    COPY_NODE_FIELD(hintState);

    return newnode;
}

static UpdateStmt* _copyUpdateStmt(const UpdateStmt* from)
{
    UpdateStmt* newnode = makeNode(UpdateStmt);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(targetList);
    COPY_NODE_FIELD(whereClause);
    COPY_NODE_FIELD(fromClause);
    COPY_NODE_FIELD(returningList);
    COPY_NODE_FIELD(withClause);
    COPY_NODE_FIELD(hintState);

    return newnode;
}

static MergeStmt* _copyMergeStmt(const MergeStmt* from)
{
    MergeStmt* newnode = makeNode(MergeStmt);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(source_relation);
    COPY_NODE_FIELD(join_condition);
    COPY_NODE_FIELD(mergeWhenClauses);
    COPY_SCALAR_FIELD(is_insert_update);
    COPY_NODE_FIELD(insert_stmt);
    COPY_NODE_FIELD(hintState);

    return newnode;
}

static MergeWhenClause* _copyMergeWhenClause(const MergeWhenClause* from)
{
    MergeWhenClause* newnode = makeNode(MergeWhenClause);

    COPY_SCALAR_FIELD(matched);
    COPY_SCALAR_FIELD(commandType);
    COPY_NODE_FIELD(condition);
    COPY_NODE_FIELD(targetList);
    COPY_NODE_FIELD(cols);
    COPY_NODE_FIELD(values);

    return newnode;
}

static SelectStmt* _copySelectStmt(const SelectStmt* from)
{
    SelectStmt* newnode = makeNode(SelectStmt);

    COPY_NODE_FIELD(distinctClause);
    COPY_NODE_FIELD(intoClause);
    COPY_NODE_FIELD(targetList);
    COPY_NODE_FIELD(fromClause);
    COPY_NODE_FIELD(startWithClause);
    COPY_NODE_FIELD(whereClause);
    COPY_NODE_FIELD(groupClause);
    COPY_NODE_FIELD(havingClause);
    COPY_NODE_FIELD(windowClause);
    COPY_NODE_FIELD(withClause);
    COPY_NODE_FIELD(valuesLists);
    COPY_NODE_FIELD(sortClause);
    COPY_NODE_FIELD(limitOffset);
    COPY_NODE_FIELD(limitCount);
    COPY_NODE_FIELD(lockingClause);
    COPY_NODE_FIELD(hintState);
    COPY_SCALAR_FIELD(op);
    COPY_SCALAR_FIELD(all);
    COPY_NODE_FIELD(larg);
    COPY_NODE_FIELD(rarg);
    COPY_SCALAR_FIELD(hasPlus);

    return newnode;
}

static SetOperationStmt* _copySetOperationStmt(const SetOperationStmt* from)
{
    SetOperationStmt* newnode = makeNode(SetOperationStmt);

    COPY_SCALAR_FIELD(op);
    COPY_SCALAR_FIELD(all);
    COPY_NODE_FIELD(larg);
    COPY_NODE_FIELD(rarg);
    COPY_NODE_FIELD(colTypes);
    COPY_NODE_FIELD(colTypmods);
    COPY_NODE_FIELD(colCollations);
    COPY_NODE_FIELD(groupClauses);

    return newnode;
}

static AlterTableStmt* _copyAlterTableStmt(const AlterTableStmt* from)
{
    AlterTableStmt* newnode = makeNode(AlterTableStmt);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(cmds);
    COPY_SCALAR_FIELD(relkind);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_SCALAR_FIELD(fromCreate);

    return newnode;
}

static AlterTableCmd* _copyAlterTableCmd(const AlterTableCmd* from)
{
    AlterTableCmd* newnode = makeNode(AlterTableCmd);

    COPY_SCALAR_FIELD(subtype);
    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(def);
    COPY_SCALAR_FIELD(behavior);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_NODE_FIELD(exchange_with_rel);
    COPY_SCALAR_FIELD(check_validation);
    COPY_SCALAR_FIELD(exchange_verbose);
    COPY_STRING_FIELD(target_partition_tablespace);
    COPY_SCALAR_FIELD(additional_property);
    COPY_NODE_FIELD(bucket_list);
    COPY_SCALAR_FIELD(alterGPI);

    return newnode;
}

static AlterDomainStmt* _copyAlterDomainStmt(const AlterDomainStmt* from)
{
    AlterDomainStmt* newnode = makeNode(AlterDomainStmt);

    COPY_SCALAR_FIELD(subtype);
    COPY_NODE_FIELD(typname);
    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(def);
    COPY_SCALAR_FIELD(behavior);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static GrantStmt* _copyGrantStmt(const GrantStmt* from)
{
    GrantStmt* newnode = makeNode(GrantStmt);

    COPY_SCALAR_FIELD(is_grant);
    COPY_SCALAR_FIELD(targtype);
    COPY_SCALAR_FIELD(objtype);
    COPY_NODE_FIELD(objects);
    COPY_NODE_FIELD(privileges);
    COPY_NODE_FIELD(grantees);
    COPY_SCALAR_FIELD(grant_option);
    COPY_SCALAR_FIELD(behavior);

    return newnode;
}

static PrivGrantee* _copyPrivGrantee(const PrivGrantee* from)
{
    PrivGrantee* newnode = makeNode(PrivGrantee);

    COPY_STRING_FIELD(rolname);

    return newnode;
}

static FuncWithArgs* _copyFuncWithArgs(const FuncWithArgs* from)
{
    FuncWithArgs* newnode = makeNode(FuncWithArgs);

    COPY_NODE_FIELD(funcname);
    COPY_NODE_FIELD(funcargs);

    return newnode;
}

static AccessPriv* _copyAccessPriv(const AccessPriv* from)
{
    AccessPriv* newnode = makeNode(AccessPriv);

    COPY_STRING_FIELD(priv_name);
    COPY_NODE_FIELD(cols);

    return newnode;
}

static GrantRoleStmt* _copyGrantRoleStmt(const GrantRoleStmt* from)
{
    GrantRoleStmt* newnode = makeNode(GrantRoleStmt);

    COPY_NODE_FIELD(granted_roles);
    COPY_NODE_FIELD(grantee_roles);
    COPY_SCALAR_FIELD(is_grant);
    COPY_SCALAR_FIELD(admin_opt);
    COPY_STRING_FIELD(grantor);
    COPY_SCALAR_FIELD(behavior);

    return newnode;
}

static DbPriv* _copyDbPriv(const DbPriv* from)
{
    DbPriv* newnode = makeNode(DbPriv);

    COPY_STRING_FIELD(db_priv_name);

    return newnode;
}

static GrantDbStmt* _copyGrantDbStmt(const GrantDbStmt* from)
{
    GrantDbStmt* newnode = makeNode(GrantDbStmt);

    COPY_SCALAR_FIELD(is_grant);
    COPY_NODE_FIELD(privileges);
    COPY_NODE_FIELD(grantees);
    COPY_SCALAR_FIELD(admin_opt);

    return newnode;
}

static AlterDefaultPrivilegesStmt* _copyAlterDefaultPrivilegesStmt(const AlterDefaultPrivilegesStmt* from)
{
    AlterDefaultPrivilegesStmt* newnode = makeNode(AlterDefaultPrivilegesStmt);

    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(action);

    return newnode;
}

static DeclareCursorStmt* _copyDeclareCursorStmt(const DeclareCursorStmt* from)
{
    DeclareCursorStmt* newnode = makeNode(DeclareCursorStmt);

    COPY_STRING_FIELD(portalname);
    COPY_SCALAR_FIELD(options);
    COPY_NODE_FIELD(query);

    return newnode;
}

static ClosePortalStmt* _copyClosePortalStmt(const ClosePortalStmt* from)
{
    ClosePortalStmt* newnode = makeNode(ClosePortalStmt);

    COPY_STRING_FIELD(portalname);

    return newnode;
}

static ClusterStmt* _copyClusterStmt(const ClusterStmt* from)
{
    ClusterStmt* newnode = makeNode(ClusterStmt);

    COPY_NODE_FIELD(relation);
    COPY_STRING_FIELD(indexname);
    COPY_SCALAR_FIELD(verbose);
    COPY_SCALAR_FIELD(memUsage.work_mem);
    COPY_SCALAR_FIELD(memUsage.max_mem);

    return newnode;
}

static CopyStmt* _copyCopyStmt(const CopyStmt* from)
{
    CopyStmt* newnode = makeNode(CopyStmt);

    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(query);
    COPY_NODE_FIELD(attlist);
    COPY_SCALAR_FIELD(is_from);
    COPY_STRING_FIELD(filename);
    COPY_NODE_FIELD(options);

    return newnode;
}

#ifdef PGXC
static DistributeBy* _copyDistributeBy(const DistributeBy* from)
{
    DistributeBy* newnode = makeNode(DistributeBy);

    COPY_SCALAR_FIELD(disttype);
    COPY_NODE_FIELD(colname);
    COPY_NODE_FIELD(distState);
    COPY_SCALAR_FIELD(referenceoid);

    return newnode;
}

static DistState* _copyDistState(const DistState* from)
{
    DistState* newnode = makeNode(DistState);
    COPY_SCALAR_FIELD(strategy);
    COPY_NODE_FIELD(sliceList);
    COPY_STRING_FIELD(refTableName);

    return newnode;
}

static ListSliceDefState* _copyListSliceDefState(const ListSliceDefState* from)
{
    ListSliceDefState* newnode = makeNode(ListSliceDefState);
    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(boundaries);
    COPY_STRING_FIELD(datanode_name);

    return newnode;
}

static PGXCSubCluster* _copyPGXCSubCluster(const PGXCSubCluster* from)
{
    PGXCSubCluster* newnode = makeNode(PGXCSubCluster);

    COPY_SCALAR_FIELD(clustertype);
    COPY_NODE_FIELD(members);

    return newnode;
}
#endif

/*
 * CopyCreateStmtFields
 *
 *		This function copies the fields of the CreateStmt node.  It is used by
 *		copy functions for classes which inherit from CreateStmt.
 */
static void CopyCreateStmtFields(const CreateStmt* from, CreateStmt* newnode)
{
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(tableElts);
    COPY_NODE_FIELD(inhRelations);
    COPY_NODE_FIELD(ofTypename);
    COPY_NODE_FIELD(constraints);
    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(clusterKeys);
    COPY_SCALAR_FIELD(oncommit);
    COPY_STRING_FIELD(tablespacename);
    COPY_SCALAR_FIELD(if_not_exists);
    COPY_SCALAR_FIELD(ivm);
    COPY_SCALAR_FIELD(row_compress);
    COPY_NODE_FIELD(partTableState);
#ifdef PGXC
    COPY_NODE_FIELD(distributeby);
    COPY_NODE_FIELD(subcluster);
#endif
    COPY_STRING_FIELD(internalData);
    COPY_NODE_FIELD(uuids);
    COPY_SCALAR_FIELD(relkind);
}

static CreateStmt* _copyCreateStmt(const CreateStmt* from)
{
    CreateStmt* newnode = makeNode(CreateStmt);

    CopyCreateStmtFields(from, newnode);

    return newnode;
}

static TableLikeClause* _copyTableLikeClause(const TableLikeClause* from)
{
    TableLikeClause* newnode = makeNode(TableLikeClause);

    COPY_NODE_FIELD(relation);
    COPY_SCALAR_FIELD(options);

    return newnode;
}

static DefineStmt* _copyDefineStmt(const DefineStmt* from)
{
    DefineStmt* newnode = makeNode(DefineStmt);

    COPY_SCALAR_FIELD(kind);
    COPY_SCALAR_FIELD(oldstyle);
    COPY_NODE_FIELD(defnames);
    COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(definition);

    return newnode;
}

static DropStmt* _copyDropStmt(const DropStmt* from)
{
    DropStmt* newnode = makeNode(DropStmt);

    COPY_NODE_FIELD(objects);
    COPY_NODE_FIELD(arguments);
    COPY_SCALAR_FIELD(removeType);
    COPY_SCALAR_FIELD(behavior);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_SCALAR_FIELD(concurrent);

    return newnode;
}

static TruncateStmt* _copyTruncateStmt(const TruncateStmt* from)
{
    TruncateStmt* newnode = makeNode(TruncateStmt);

    COPY_NODE_FIELD(relations);
    COPY_SCALAR_FIELD(restart_seqs);
    COPY_SCALAR_FIELD(behavior);
    COPY_SCALAR_FIELD(purge);

    return newnode;
}

static PurgeStmt* CopyPurgeStmt(const PurgeStmt* from)
{
    PurgeStmt* newnode = makeNode(PurgeStmt);

    COPY_SCALAR_FIELD(purtype);
    COPY_NODE_FIELD(purobj);

    return newnode;
}

static TimeCapsuleStmt* CopyTimeCapsuleStmt(const TimeCapsuleStmt* from)
{
    TimeCapsuleStmt* newnode = makeNode(TimeCapsuleStmt);

    COPY_SCALAR_FIELD(tcaptype);
    COPY_NODE_FIELD(relation);
    COPY_STRING_FIELD(new_relname);

    COPY_NODE_FIELD(tvver);
    COPY_SCALAR_FIELD(tvtype);

    return newnode;
}

static CommentStmt* _copyCommentStmt(const CommentStmt* from)
{
    CommentStmt* newnode = makeNode(CommentStmt);

    COPY_SCALAR_FIELD(objtype);
    COPY_NODE_FIELD(objname);
    COPY_NODE_FIELD(objargs);
    COPY_STRING_FIELD(comment);

    return newnode;
}

static SecLabelStmt* _copySecLabelStmt(const SecLabelStmt* from)
{
    SecLabelStmt* newnode = makeNode(SecLabelStmt);

    COPY_SCALAR_FIELD(objtype);
    COPY_NODE_FIELD(objname);
    COPY_NODE_FIELD(objargs);
    COPY_STRING_FIELD(provider);
    COPY_STRING_FIELD(label);

    return newnode;
}

static FetchStmt* _copyFetchStmt(const FetchStmt* from)
{
    FetchStmt* newnode = makeNode(FetchStmt);

    COPY_SCALAR_FIELD(direction);
    COPY_SCALAR_FIELD(howMany);
    COPY_STRING_FIELD(portalname);
    COPY_SCALAR_FIELD(ismove);

    return newnode;
}

static IndexStmt* _copyIndexStmt(const IndexStmt* from)
{
    IndexStmt* newnode = makeNode(IndexStmt);

    COPY_STRING_FIELD(schemaname);
    COPY_STRING_FIELD(idxname);
    COPY_NODE_FIELD(relation);
    COPY_STRING_FIELD(accessMethod);
    COPY_STRING_FIELD(tableSpace);
    COPY_NODE_FIELD(indexParams);
    COPY_NODE_FIELD(indexIncludingParams);
    COPY_SCALAR_FIELD(isGlobal);
    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(whereClause);
    COPY_NODE_FIELD(excludeOpNames);
    COPY_STRING_FIELD(idxcomment);
    COPY_SCALAR_FIELD(indexOid);
    COPY_SCALAR_FIELD(oldNode);
    COPY_NODE_FIELD(partClause);
    COPY_SCALAR_FIELD(partIndexUsable);
    COPY_SCALAR_FIELD(isPartitioned);
    COPY_SCALAR_FIELD(unique);
    COPY_SCALAR_FIELD(primary);
    COPY_SCALAR_FIELD(isconstraint);
    COPY_SCALAR_FIELD(deferrable);
    COPY_SCALAR_FIELD(initdeferred);
    COPY_SCALAR_FIELD(concurrent);
    COPY_NODE_FIELD(inforConstraint);
    COPY_SCALAR_FIELD(internal_flag);
    COPY_SCALAR_FIELD(skip_mem_check);
    COPY_SCALAR_FIELD(memUsage.work_mem);
    COPY_SCALAR_FIELD(memUsage.max_mem);

    return newnode;
}

static CreateFunctionStmt* _copyCreateFunctionStmt(const CreateFunctionStmt* from)
{
    CreateFunctionStmt* newnode = makeNode(CreateFunctionStmt);

    COPY_SCALAR_FIELD(replace);
    COPY_SCALAR_FIELD(isOraStyle);
    COPY_SCALAR_FIELD(isProcedure);
    COPY_NODE_FIELD(funcname);
    COPY_NODE_FIELD(parameters);
    COPY_NODE_FIELD(returnType);
    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(withClause);
    COPY_STRING_FIELD(inputHeaderSrc);

    return newnode;
}

static CreatePackageStmt* _copyCreatePackageStmt(const CreatePackageStmt* from)
{
    CreatePackageStmt* newnode = makeNode(CreatePackageStmt);

    COPY_SCALAR_FIELD(replace);
    COPY_NODE_FIELD(pkgname);
    COPY_SCALAR_FIELD(pkgsecdef);
    COPY_STRING_FIELD(pkgspec);

    return newnode;
}

static CreatePackageBodyStmt* _copyCreatePackageBodyStmt(const CreatePackageBodyStmt* from)
{
    CreatePackageBodyStmt* newnode = makeNode(CreatePackageBodyStmt);

    COPY_SCALAR_FIELD(replace);
    COPY_NODE_FIELD(pkgname);
    COPY_STRING_FIELD(pkgbody);
    COPY_STRING_FIELD(pkginit); 
    COPY_SCALAR_FIELD(pkgsecdef);
    return newnode;
}

static FunctionParameter* _copyFunctionParameter(const FunctionParameter* from)
{
    FunctionParameter* newnode = makeNode(FunctionParameter);

    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(argType);
    COPY_SCALAR_FIELD(mode);
    COPY_NODE_FIELD(defexpr);

    return newnode;
}

static AlterFunctionStmt* _copyAlterFunctionStmt(const AlterFunctionStmt* from)
{
    AlterFunctionStmt* newnode = makeNode(AlterFunctionStmt);

    COPY_NODE_FIELD(func);
    COPY_NODE_FIELD(actions);

    return newnode;
}

static DoStmt* _copyDoStmt(const DoStmt* from)
{
    DoStmt* newnode = makeNode(DoStmt);

    COPY_NODE_FIELD(args);

    return newnode;
}

static RenameStmt* _copyRenameStmt(const RenameStmt* from)
{
    RenameStmt* newnode = makeNode(RenameStmt);

    COPY_SCALAR_FIELD(renameType);
    COPY_SCALAR_FIELD(relationType);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(object);
    COPY_NODE_FIELD(objarg);
    COPY_STRING_FIELD(subname);
    COPY_STRING_FIELD(newname);
    COPY_SCALAR_FIELD(behavior);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static AlterObjectSchemaStmt* _copyAlterObjectSchemaStmt(const AlterObjectSchemaStmt* from)
{
    AlterObjectSchemaStmt* newnode = makeNode(AlterObjectSchemaStmt);

    COPY_SCALAR_FIELD(objectType);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(object);
    COPY_NODE_FIELD(objarg);
    COPY_STRING_FIELD(addname);
    COPY_STRING_FIELD(newschema);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static AlterOwnerStmt* _copyAlterOwnerStmt(const AlterOwnerStmt* from)
{
    AlterOwnerStmt* newnode = makeNode(AlterOwnerStmt);

    COPY_SCALAR_FIELD(objectType);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(object);
    COPY_NODE_FIELD(objarg);
    COPY_STRING_FIELD(addname);
    COPY_STRING_FIELD(newowner);

    return newnode;
}

static RuleStmt* _copyRuleStmt(const RuleStmt* from)
{
    RuleStmt* newnode = makeNode(RuleStmt);

    COPY_NODE_FIELD(relation);
    COPY_STRING_FIELD(rulename);
    COPY_NODE_FIELD(whereClause);
    COPY_SCALAR_FIELD(event);
    COPY_SCALAR_FIELD(instead);
    COPY_NODE_FIELD(actions);
    COPY_SCALAR_FIELD(replace);

    return newnode;
}

static NotifyStmt* _copyNotifyStmt(const NotifyStmt* from)
{
    NotifyStmt* newnode = makeNode(NotifyStmt);

    COPY_STRING_FIELD(conditionname);
    COPY_STRING_FIELD(payload);

    return newnode;
}

static ListenStmt* _copyListenStmt(const ListenStmt* from)
{
    ListenStmt* newnode = makeNode(ListenStmt);

    COPY_STRING_FIELD(conditionname);

    return newnode;
}

static UnlistenStmt* _copyUnlistenStmt(const UnlistenStmt* from)
{
    UnlistenStmt* newnode = makeNode(UnlistenStmt);

    COPY_STRING_FIELD(conditionname);

    return newnode;
}

static TransactionStmt* _copyTransactionStmt(const TransactionStmt* from)
{
    TransactionStmt* newnode = makeNode(TransactionStmt);

    COPY_SCALAR_FIELD(kind);
    COPY_NODE_FIELD(options);
    COPY_STRING_FIELD(gid);
    COPY_SCALAR_FIELD(csn);

    return newnode;
}

static CompositeTypeStmt* _copyCompositeTypeStmt(const CompositeTypeStmt* from)
{
    CompositeTypeStmt* newnode = makeNode(CompositeTypeStmt);

    COPY_NODE_FIELD(typevar);
    COPY_NODE_FIELD(coldeflist);

    return newnode;
}

static TableOfTypeStmt* _copyTableOfTypeStmt(const TableOfTypeStmt* from)
{
    TableOfTypeStmt* newnode = makeNode(TableOfTypeStmt);

    COPY_NODE_FIELD(typname);
    COPY_NODE_FIELD(reftypname);

    return newnode;
}

static CreateEnumStmt* _copyCreateEnumStmt(const CreateEnumStmt* from)
{
    CreateEnumStmt* newnode = makeNode(CreateEnumStmt);

    COPY_NODE_FIELD(typname);
    COPY_NODE_FIELD(vals);

    return newnode;
}

static CreateRangeStmt* _copyCreateRangeStmt(const CreateRangeStmt* from)
{
    CreateRangeStmt* newnode = makeNode(CreateRangeStmt);

    COPY_NODE_FIELD(typname);
    COPY_NODE_FIELD(params);

    return newnode;
}

static AlterEnumStmt* _copyAlterEnumStmt(const AlterEnumStmt* from)
{
    AlterEnumStmt* newnode = makeNode(AlterEnumStmt);

    COPY_NODE_FIELD(typname);
    COPY_STRING_FIELD(oldVal);
    COPY_STRING_FIELD(newVal);
    COPY_STRING_FIELD(newValNeighbor);
    COPY_SCALAR_FIELD(newValIsAfter);
    COPY_SCALAR_FIELD(skipIfNewValExists);

    return newnode;
}

static ViewStmt* _copyViewStmt(const ViewStmt* from)
{
    ViewStmt* newnode = makeNode(ViewStmt);

    COPY_NODE_FIELD(view);
    COPY_NODE_FIELD(aliases);
    COPY_NODE_FIELD(query);
    COPY_SCALAR_FIELD(replace);
    COPY_SCALAR_FIELD(ivm);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(relkind);

    return newnode;
}

static LoadStmt* _copyLoadStmt(const LoadStmt* from)
{
    LoadStmt* newnode = makeNode(LoadStmt);

    COPY_STRING_FIELD(filename);

    COPY_NODE_FIELD(pre_load_options);
    COPY_SCALAR_FIELD(is_load_data);
    COPY_SCALAR_FIELD(is_only_special_filed);
    COPY_NODE_FIELD(load_options);
    COPY_SCALAR_FIELD(load_type);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(rel_options);

    return newnode;
}

static CreateDomainStmt* _copyCreateDomainStmt(const CreateDomainStmt* from)
{
    CreateDomainStmt* newnode = makeNode(CreateDomainStmt);

    COPY_NODE_FIELD(domainname);
    COPY_NODE_FIELD(typname);
    COPY_NODE_FIELD(collClause);
    COPY_NODE_FIELD(constraints);

    return newnode;
}

static CreateOpClassStmt* _copyCreateOpClassStmt(const CreateOpClassStmt* from)
{
    CreateOpClassStmt* newnode = makeNode(CreateOpClassStmt);

    COPY_NODE_FIELD(opclassname);
    COPY_NODE_FIELD(opfamilyname);
    COPY_STRING_FIELD(amname);
    COPY_NODE_FIELD(datatype);
    COPY_NODE_FIELD(items);
    COPY_SCALAR_FIELD(isDefault);

    return newnode;
}

static CreateOpClassItem* _copyCreateOpClassItem(const CreateOpClassItem* from)
{
    CreateOpClassItem* newnode = makeNode(CreateOpClassItem);

    COPY_SCALAR_FIELD(itemtype);
    COPY_NODE_FIELD(name);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(number);
    COPY_NODE_FIELD(order_family);
    COPY_NODE_FIELD(class_args);
    COPY_NODE_FIELD(storedtype);

    return newnode;
}

static CreateOpFamilyStmt* _copyCreateOpFamilyStmt(const CreateOpFamilyStmt* from)
{
    CreateOpFamilyStmt* newnode = makeNode(CreateOpFamilyStmt);

    COPY_NODE_FIELD(opfamilyname);
    COPY_STRING_FIELD(amname);

    return newnode;
}

static AlterOpFamilyStmt* _copyAlterOpFamilyStmt(const AlterOpFamilyStmt* from)
{
    AlterOpFamilyStmt* newnode = makeNode(AlterOpFamilyStmt);

    COPY_NODE_FIELD(opfamilyname);
    COPY_STRING_FIELD(amname);
    COPY_SCALAR_FIELD(isDrop);
    COPY_NODE_FIELD(items);

    return newnode;
}

static CreatedbStmt* _copyCreatedbStmt(const CreatedbStmt* from)
{
    CreatedbStmt* newnode = makeNode(CreatedbStmt);

    COPY_STRING_FIELD(dbname);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterDatabaseStmt* _copyAlterDatabaseStmt(const AlterDatabaseStmt* from)
{
    AlterDatabaseStmt* newnode = makeNode(AlterDatabaseStmt);

    COPY_STRING_FIELD(dbname);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterDatabaseSetStmt* _copyAlterDatabaseSetStmt(const AlterDatabaseSetStmt* from)
{
    AlterDatabaseSetStmt* newnode = makeNode(AlterDatabaseSetStmt);

    COPY_STRING_FIELD(dbname);
    COPY_NODE_FIELD(setstmt);

    return newnode;
}

static DropdbStmt* _copyDropdbStmt(const DropdbStmt* from)
{
    DropdbStmt* newnode = makeNode(DropdbStmt);

    COPY_STRING_FIELD(dbname);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static VacuumStmt* _copyVacuumStmt(const VacuumStmt* from)
{
    VacuumStmt* newnode = makeNode(VacuumStmt);

    COPY_SCALAR_FIELD(options);
    COPY_SCALAR_FIELD(flags);
    COPY_SCALAR_FIELD(rely_oid);
    COPY_SCALAR_FIELD(freeze_min_age);
    COPY_SCALAR_FIELD(freeze_table_age);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(va_cols);
    COPY_SCALAR_FIELD(onepartrel);
    COPY_NODE_FIELD(onepart);
    COPY_NODE_FIELD(partList);
    COPY_SCALAR_FIELD(isForeignTables);
    COPY_SCALAR_FIELD(totalFileCnt);
    COPY_SCALAR_FIELD(nodeNo);
    COPY_SCALAR_FIELD(DnCnt);
    // for global stats
    COPY_NODE_FIELD(dest);
    COPY_SCALAR_FIELD(num_samples);
    COPY_NODE_FIELD(sampleRows);
    COPY_SCALAR_FIELD(tableidx);
    COPY_ARRAY_FIELD(pstGlobalStatEx);
    COPY_SCALAR_FIELD(memUsage.work_mem);
    COPY_SCALAR_FIELD(memUsage.max_mem);

    return newnode;
}

static ExplainStmt* _copyExplainStmt(const ExplainStmt* from)
{
    ExplainStmt* newnode = makeNode(ExplainStmt);

    COPY_NODE_FIELD(statement);
    COPY_NODE_FIELD(query);
    COPY_NODE_FIELD(options);

    return newnode;
}

static CreateTableAsStmt* _copyCreateTableAsStmt(const CreateTableAsStmt* from)
{
    CreateTableAsStmt* newnode = makeNode(CreateTableAsStmt);

    COPY_NODE_FIELD(query);
    COPY_NODE_FIELD(into);
    COPY_SCALAR_FIELD(relkind);
    COPY_SCALAR_FIELD(is_select_into);

    return newnode;
}

static RefreshMatViewStmt *
_copyRefreshMatViewStmt(const RefreshMatViewStmt *from)
{
   RefreshMatViewStmt *newnode = makeNode(RefreshMatViewStmt);

   COPY_SCALAR_FIELD(skipData);
   COPY_SCALAR_FIELD(incremental);
   COPY_NODE_FIELD(relation);

   return newnode;
}

static CreateSeqStmt* _copyCreateSeqStmt(const CreateSeqStmt* from)
{
    CreateSeqStmt* newnode = makeNode(CreateSeqStmt);

    COPY_NODE_FIELD(sequence);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(ownerId);
#ifdef PGXC
    COPY_SCALAR_FIELD(is_serial);
#endif
    COPY_SCALAR_FIELD(uuid);
    COPY_SCALAR_FIELD(canCreateTempSeq);
    COPY_SCALAR_FIELD(is_large);

    return newnode;
}

static AlterSeqStmt* _copyAlterSeqStmt(const AlterSeqStmt* from)
{
    AlterSeqStmt* newnode = makeNode(AlterSeqStmt);

    COPY_NODE_FIELD(sequence);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_SCALAR_FIELD(is_large);

    return newnode;
}

static VariableSetStmt* _copyVariableSetStmt(const VariableSetStmt* from)
{
    VariableSetStmt* newnode = makeNode(VariableSetStmt);

    COPY_SCALAR_FIELD(kind);
    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(is_local);

    return newnode;
}

static VariableShowStmt* _copyVariableShowStmt(const VariableShowStmt* from)
{
    VariableShowStmt* newnode = makeNode(VariableShowStmt);

    COPY_STRING_FIELD(name);
    COPY_STRING_FIELD(likename);

    return newnode;
}

static DiscardStmt* _copyDiscardStmt(const DiscardStmt* from)
{
    DiscardStmt* newnode = makeNode(DiscardStmt);

    COPY_SCALAR_FIELD(target);

    return newnode;
}

static CreateTableSpaceStmt* _copyCreateTableSpaceStmt(const CreateTableSpaceStmt* from)
{
    CreateTableSpaceStmt* newnode = makeNode(CreateTableSpaceStmt);

    COPY_STRING_FIELD(tablespacename);
    COPY_STRING_FIELD(owner);
    COPY_STRING_FIELD(location);
    COPY_STRING_FIELD(maxsize);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(relative);

    return newnode;
}

static DropTableSpaceStmt* _copyDropTableSpaceStmt(const DropTableSpaceStmt* from)
{
    DropTableSpaceStmt* newnode = makeNode(DropTableSpaceStmt);

    COPY_STRING_FIELD(tablespacename);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static AlterTableSpaceOptionsStmt* _copyAlterTableSpaceOptionsStmt(const AlterTableSpaceOptionsStmt* from)
{
    AlterTableSpaceOptionsStmt* newnode = makeNode(AlterTableSpaceOptionsStmt);

    COPY_STRING_FIELD(tablespacename);
    COPY_STRING_FIELD(maxsize);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(isReset);

    return newnode;
}

static CreateExtensionStmt* _copyCreateExtensionStmt(const CreateExtensionStmt* from)
{
    CreateExtensionStmt* newnode = makeNode(CreateExtensionStmt);

    COPY_STRING_FIELD(extname);
    COPY_SCALAR_FIELD(if_not_exists);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterExtensionStmt* _copyAlterExtensionStmt(const AlterExtensionStmt* from)
{
    AlterExtensionStmt* newnode = makeNode(AlterExtensionStmt);

    COPY_STRING_FIELD(extname);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterExtensionContentsStmt* _copyAlterExtensionContentsStmt(const AlterExtensionContentsStmt* from)
{
    AlterExtensionContentsStmt* newnode = makeNode(AlterExtensionContentsStmt);

    COPY_STRING_FIELD(extname);
    COPY_SCALAR_FIELD(action);
    COPY_SCALAR_FIELD(objtype);
    COPY_NODE_FIELD(objname);
    COPY_NODE_FIELD(objargs);

    return newnode;
}

static CreateFdwStmt* _copyCreateFdwStmt(const CreateFdwStmt* from)
{
    CreateFdwStmt* newnode = makeNode(CreateFdwStmt);

    COPY_STRING_FIELD(fdwname);
    COPY_NODE_FIELD(func_options);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterFdwStmt* _copyAlterFdwStmt(const AlterFdwStmt* from)
{
    AlterFdwStmt* newnode = makeNode(AlterFdwStmt);

    COPY_STRING_FIELD(fdwname);
    COPY_NODE_FIELD(func_options);
    COPY_NODE_FIELD(options);

    return newnode;
}

static CreateForeignServerStmt* _copyCreateForeignServerStmt(const CreateForeignServerStmt* from)
{
    CreateForeignServerStmt* newnode = makeNode(CreateForeignServerStmt);

    COPY_STRING_FIELD(servername);
    COPY_STRING_FIELD(servertype);
    COPY_STRING_FIELD(version);
    COPY_STRING_FIELD(fdwname);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterForeignServerStmt* _copyAlterForeignServerStmt(const AlterForeignServerStmt* from)
{
    AlterForeignServerStmt* newnode = makeNode(AlterForeignServerStmt);

    COPY_STRING_FIELD(servername);
    COPY_STRING_FIELD(version);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(has_version);

    return newnode;
}

static CreateUserMappingStmt* _copyCreateUserMappingStmt(const CreateUserMappingStmt* from)
{
    CreateUserMappingStmt* newnode = makeNode(CreateUserMappingStmt);

    COPY_STRING_FIELD(username);
    COPY_STRING_FIELD(servername);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterUserMappingStmt* _copyAlterUserMappingStmt(const AlterUserMappingStmt* from)
{
    AlterUserMappingStmt* newnode = makeNode(AlterUserMappingStmt);

    COPY_STRING_FIELD(username);
    COPY_STRING_FIELD(servername);
    COPY_NODE_FIELD(options);

    return newnode;
}

static DropUserMappingStmt* _copyDropUserMappingStmt(const DropUserMappingStmt* from)
{
    DropUserMappingStmt* newnode = makeNode(DropUserMappingStmt);

    COPY_STRING_FIELD(username);
    COPY_STRING_FIELD(servername);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static ForeignPartState* _copyForeignPartState(const ForeignPartState* from)
{
    ForeignPartState* newnode = makeNode(ForeignPartState);
    COPY_NODE_FIELD(partitionKey);
    return newnode;
}

static CreateForeignTableStmt* _copyCreateForeignTableStmt(const CreateForeignTableStmt* from)
{
    CreateForeignTableStmt* newnode = makeNode(CreateForeignTableStmt);

    CopyCreateStmtFields((const CreateStmt*)from, (CreateStmt*)newnode);

    COPY_STRING_FIELD(servername);
    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(extOptions);
    COPY_NODE_FIELD(error_relation);
    COPY_LOCATION_FIELD(write_only);
    COPY_NODE_FIELD(part_state);

    return newnode;
}

static CreateDataSourceStmt* _copyCreateDataSourceStmt(const CreateDataSourceStmt* from)
{
    CreateDataSourceStmt* newnode = makeNode(CreateDataSourceStmt);

    COPY_STRING_FIELD(srcname);
    COPY_STRING_FIELD(srctype);
    COPY_STRING_FIELD(version);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterSchemaStmt* _copyAlterSchemaStmt(const AlterSchemaStmt* from)
{
    AlterSchemaStmt* newnode = makeNode(AlterSchemaStmt);

    COPY_STRING_FIELD(schemaname);
    COPY_STRING_FIELD(authid);
    COPY_SCALAR_FIELD(hasBlockChain);

    return newnode;
}

static AlterDataSourceStmt* _copyAlterDataSourceStmt(const AlterDataSourceStmt* from)
{
    AlterDataSourceStmt* newnode = makeNode(AlterDataSourceStmt);

    COPY_STRING_FIELD(srcname);
    COPY_STRING_FIELD(srctype);
    COPY_STRING_FIELD(version);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(has_version);

    return newnode;
}

static CreatePolicyLabelStmt* _copyCreatePolicyLabelStmt(const CreatePolicyLabelStmt* from)
{
    CreatePolicyLabelStmt* newnode = makeNode(CreatePolicyLabelStmt);
    COPY_SCALAR_FIELD(if_not_exists);
    COPY_STRING_FIELD(label_type);
    COPY_STRING_FIELD(label_name);
    COPY_NODE_FIELD(label_items);
    return newnode;
}

static AlterPolicyLabelStmt* _copyAlterPolicyLabelStmt(const AlterPolicyLabelStmt* from)
{
    AlterPolicyLabelStmt* newnode = makeNode(AlterPolicyLabelStmt);
    COPY_STRING_FIELD(stmt_type);
    COPY_STRING_FIELD(label_name);
    COPY_NODE_FIELD(label_items);
    return newnode;
}

static DropPolicyLabelStmt* _copyDropPolicyLabelStmt(const DropPolicyLabelStmt* from)
{
    DropPolicyLabelStmt* newnode = makeNode(DropPolicyLabelStmt);
    COPY_SCALAR_FIELD(if_exists);
    COPY_NODE_FIELD(label_names);
    return newnode;
}

static CreateAuditPolicyStmt* _copyCreateAuditPolicyStmt(const CreateAuditPolicyStmt* from)
{
    CreateAuditPolicyStmt* newnode = makeNode(CreateAuditPolicyStmt);
    COPY_SCALAR_FIELD(if_not_exists);
    COPY_STRING_FIELD(policy_type);
    COPY_STRING_FIELD(policy_name);
    COPY_NODE_FIELD(policy_targets);
    COPY_NODE_FIELD(policy_filters);
    COPY_SCALAR_FIELD(policy_enabled);
    return newnode;
}

static AlterAuditPolicyStmt* _copyAlterAuditPolicyStmt(const AlterAuditPolicyStmt* from)
{
    AlterAuditPolicyStmt* newnode = makeNode(AlterAuditPolicyStmt);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_STRING_FIELD(policy_name);
    COPY_STRING_FIELD(policy_action);
    COPY_STRING_FIELD(policy_type);
    COPY_NODE_FIELD(policy_items);
    COPY_NODE_FIELD(policy_filters);
    COPY_STRING_FIELD(policy_comments);
    COPY_NODE_FIELD(policy_enabled);
    return newnode;
}

static DropAuditPolicyStmt* _copyDropAuditPolicyStmt(const DropAuditPolicyStmt* from)
{
    DropAuditPolicyStmt* newnode = makeNode(DropAuditPolicyStmt);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_NODE_FIELD(policy_names);
    return newnode;
}

static CreateMaskingPolicyStmt* _copyCreateMaskingPolicyStmt(const CreateMaskingPolicyStmt* from)
{
    CreateMaskingPolicyStmt* newnode = makeNode(CreateMaskingPolicyStmt);
    COPY_SCALAR_FIELD(if_not_exists);
    COPY_STRING_FIELD(policy_name);
    COPY_NODE_FIELD(policy_data);
    COPY_NODE_FIELD(policy_condition);
    COPY_NODE_FIELD(policy_filters);
    COPY_SCALAR_FIELD(policy_enabled);
    return newnode;
}

static AlterMaskingPolicyStmt* _copyAlterMaskingPolicyStmt(const AlterMaskingPolicyStmt* from)
{
    AlterMaskingPolicyStmt* newnode = makeNode(AlterMaskingPolicyStmt);
    COPY_STRING_FIELD(policy_name);
    COPY_STRING_FIELD(policy_action);
    COPY_NODE_FIELD(policy_items);
    COPY_NODE_FIELD(policy_condition);
    COPY_NODE_FIELD(policy_filters);
    COPY_STRING_FIELD(policy_comments);
    COPY_NODE_FIELD(policy_enabled);
    return newnode;
}

static DropMaskingPolicyStmt* _copyDropMaskingPolicyStmt(const DropMaskingPolicyStmt* from)
{
    DropMaskingPolicyStmt* newnode = makeNode(DropMaskingPolicyStmt);
    COPY_SCALAR_FIELD(if_exists);
    COPY_NODE_FIELD(policy_names);
    return newnode;
}

static MaskingPolicyCondition* _copyMaskingPolicyCondition(const MaskingPolicyCondition* from)
{
    MaskingPolicyCondition* newnode = makeNode(MaskingPolicyCondition);
    COPY_NODE_FIELD(fqdn);
    COPY_STRING_FIELD(_operator);
    COPY_NODE_FIELD(arg);
    return newnode;
}

static PolicyFilterNode* _copyPolicyFilterNode(const PolicyFilterNode* from)
{
    PolicyFilterNode* newnode = makeNode(PolicyFilterNode);
    COPY_STRING_FIELD(node_type);
    COPY_STRING_FIELD(op_value);
    COPY_STRING_FIELD(filter_type);
    COPY_NODE_FIELD(values);
    COPY_SCALAR_FIELD(has_not_operator);
    COPY_NODE_FIELD(left);
    COPY_NODE_FIELD(right);
    return newnode;
}

static CreateWeakPasswordDictionaryStmt* _copyCreateWeakPasswordDictionaryStmt(const CreateWeakPasswordDictionaryStmt* from)
{
    CreateWeakPasswordDictionaryStmt* newnode = makeNode(CreateWeakPasswordDictionaryStmt);

    COPY_NODE_FIELD(weak_password_string_list);

    return newnode;
}

static DropWeakPasswordDictionaryStmt* _copyDropWeakPasswordDictionaryStmt(const DropWeakPasswordDictionaryStmt* from)
{
    DropWeakPasswordDictionaryStmt* newnode = makeNode(DropWeakPasswordDictionaryStmt);

    return newnode;
}

static CreateRlsPolicyStmt* _copyCreateRlsPolicyStmt(const CreateRlsPolicyStmt* from)
{
    CreateRlsPolicyStmt* newnode = makeNode(CreateRlsPolicyStmt);

    COPY_STRING_FIELD(policyName);
    COPY_NODE_FIELD(relation);
    COPY_STRING_FIELD(cmdName);
    COPY_SCALAR_FIELD(isPermissive);
    COPY_SCALAR_FIELD(fromExternal);
    COPY_NODE_FIELD(roleList);
    COPY_NODE_FIELD(usingQual);

    return newnode;
}

static AlterRlsPolicyStmt* _copyAlterRlsPolicyStmt(const AlterRlsPolicyStmt* from)
{
    AlterRlsPolicyStmt* newnode = makeNode(AlterRlsPolicyStmt);

    COPY_STRING_FIELD(policyName);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(roleList);
    COPY_NODE_FIELD(usingQual);

    return newnode;
}

static CreateTrigStmt* _copyCreateTrigStmt(const CreateTrigStmt* from)
{
    CreateTrigStmt* newnode = makeNode(CreateTrigStmt);

    COPY_STRING_FIELD(trigname);
    COPY_NODE_FIELD(relation);
    COPY_NODE_FIELD(funcname);
    COPY_NODE_FIELD(args);
    COPY_SCALAR_FIELD(row);
    COPY_SCALAR_FIELD(timing);
    COPY_SCALAR_FIELD(events);
    COPY_NODE_FIELD(columns);
    COPY_NODE_FIELD(whenClause);
    COPY_SCALAR_FIELD(isconstraint);
    COPY_SCALAR_FIELD(deferrable);
    COPY_SCALAR_FIELD(initdeferred);
    COPY_NODE_FIELD(constrrel);

    return newnode;
}

static CreatePLangStmt* _copyCreatePLangStmt(const CreatePLangStmt* from)
{
    CreatePLangStmt* newnode = makeNode(CreatePLangStmt);

    COPY_SCALAR_FIELD(replace);
    COPY_STRING_FIELD(plname);
    COPY_NODE_FIELD(plhandler);
    COPY_NODE_FIELD(plinline);
    COPY_NODE_FIELD(plvalidator);
    COPY_SCALAR_FIELD(pltrusted);

    return newnode;
}

static CreateRoleStmt* _copyCreateRoleStmt(const CreateRoleStmt* from)
{
    CreateRoleStmt* newnode = makeNode(CreateRoleStmt);

    COPY_SCALAR_FIELD(stmt_type);
    COPY_STRING_FIELD(role);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterRoleStmt* _copyAlterRoleStmt(const AlterRoleStmt* from)
{
    AlterRoleStmt* newnode = makeNode(AlterRoleStmt);

    COPY_STRING_FIELD(role);
    COPY_NODE_FIELD(options);
    COPY_SCALAR_FIELD(action);
    COPY_SCALAR_FIELD(lockstatus);

    return newnode;
}

static AlterRoleSetStmt* _copyAlterRoleSetStmt(const AlterRoleSetStmt* from)
{
    AlterRoleSetStmt* newnode = makeNode(AlterRoleSetStmt);

    COPY_STRING_FIELD(role);
    COPY_STRING_FIELD(database);
    COPY_NODE_FIELD(setstmt);

    return newnode;
}

static DropRoleStmt* _copyDropRoleStmt(const DropRoleStmt* from)
{
    DropRoleStmt* newnode = makeNode(DropRoleStmt);

    COPY_NODE_FIELD(roles);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_SCALAR_FIELD(is_user);
    COPY_SCALAR_FIELD(behavior);
    return newnode;
}

static LockStmt* _copyLockStmt(const LockStmt* from)
{
    LockStmt* newnode = makeNode(LockStmt);

    COPY_NODE_FIELD(relations);
    COPY_SCALAR_FIELD(mode);
    COPY_SCALAR_FIELD(nowait);
    COPY_SCALAR_FIELD(cancelable);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COPY_SCALAR_FIELD(waitSec);
    }

    return newnode;
}

static ConstraintsSetStmt* _copyConstraintsSetStmt(const ConstraintsSetStmt* from)
{
    ConstraintsSetStmt* newnode = makeNode(ConstraintsSetStmt);

    COPY_NODE_FIELD(constraints);
    COPY_SCALAR_FIELD(deferred);

    return newnode;
}

static ReindexStmt* _copyReindexStmt(const ReindexStmt* from)
{
    ReindexStmt* newnode = makeNode(ReindexStmt);

    COPY_SCALAR_FIELD(kind);
    COPY_NODE_FIELD(relation);
    COPY_STRING_FIELD(name);
    COPY_SCALAR_FIELD(do_system);
    COPY_SCALAR_FIELD(do_user);
    COPY_SCALAR_FIELD(memUsage.work_mem);
    COPY_SCALAR_FIELD(memUsage.max_mem);

    return newnode;
}

static CreateSchemaStmt* _copyCreateSchemaStmt(const CreateSchemaStmt* from)
{
    CreateSchemaStmt* newnode = makeNode(CreateSchemaStmt);

    COPY_STRING_FIELD(schemaname);
    COPY_STRING_FIELD(authid);
    COPY_SCALAR_FIELD(hasBlockChain);
    COPY_NODE_FIELD(schemaElts);
    COPY_NODE_FIELD(uuids);
    return newnode;
}

static CreateConversionStmt* _copyCreateConversionStmt(const CreateConversionStmt* from)
{
    CreateConversionStmt* newnode = makeNode(CreateConversionStmt);

    COPY_NODE_FIELD(conversion_name);
    COPY_STRING_FIELD(for_encoding_name);
    COPY_STRING_FIELD(to_encoding_name);
    COPY_NODE_FIELD(func_name);
    COPY_SCALAR_FIELD(def);

    return newnode;
}

static CreateCastStmt* _copyCreateCastStmt(const CreateCastStmt* from)
{
    CreateCastStmt* newnode = makeNode(CreateCastStmt);

    COPY_NODE_FIELD(sourcetype);
    COPY_NODE_FIELD(targettype);
    COPY_NODE_FIELD(func);
    COPY_SCALAR_FIELD(context);
    COPY_SCALAR_FIELD(inout);

    return newnode;
}

static PrepareStmt* _copyPrepareStmt(const PrepareStmt* from)
{
    PrepareStmt* newnode = makeNode(PrepareStmt);

    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(argtypes);
    COPY_NODE_FIELD(query);

    return newnode;
}

static ExecuteStmt* _copyExecuteStmt(const ExecuteStmt* from)
{
    ExecuteStmt* newnode = makeNode(ExecuteStmt);

    COPY_STRING_FIELD(name);
    COPY_NODE_FIELD(params);

    return newnode;
}

static DeallocateStmt* _copyDeallocateStmt(const DeallocateStmt* from)
{
    DeallocateStmt* newnode = makeNode(DeallocateStmt);

    COPY_STRING_FIELD(name);

    return newnode;
}

static DropOwnedStmt* _copyDropOwnedStmt(const DropOwnedStmt* from)
{
    DropOwnedStmt* newnode = makeNode(DropOwnedStmt);

    COPY_NODE_FIELD(roles);
    COPY_SCALAR_FIELD(behavior);

    return newnode;
}

static ReassignOwnedStmt* _copyReassignOwnedStmt(const ReassignOwnedStmt* from)
{
    ReassignOwnedStmt* newnode = makeNode(ReassignOwnedStmt);

    COPY_NODE_FIELD(roles);
    COPY_STRING_FIELD(newrole);

    return newnode;
}

static AlterTSDictionaryStmt* _copyAlterTSDictionaryStmt(const AlterTSDictionaryStmt* from)
{
    AlterTSDictionaryStmt* newnode = makeNode(AlterTSDictionaryStmt);

    COPY_NODE_FIELD(dictname);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterTSConfigurationStmt* _copyAlterTSConfigurationStmt(const AlterTSConfigurationStmt* from)
{
    AlterTSConfigurationStmt* newnode = makeNode(AlterTSConfigurationStmt);

    COPY_NODE_FIELD(cfgname);
    COPY_NODE_FIELD(tokentype);
    COPY_NODE_FIELD(dicts);
    COPY_SCALAR_FIELD(override);
    COPY_SCALAR_FIELD(replace);
    COPY_SCALAR_FIELD(missing_ok);

    return newnode;
}

static CreateDirectoryStmt* _copyCreateDirectoryStmt(const CreateDirectoryStmt* from)
{
    CreateDirectoryStmt* newnode = makeNode(CreateDirectoryStmt);

    COPY_SCALAR_FIELD(replace);
    COPY_STRING_FIELD(directoryname);
    COPY_STRING_FIELD(owner);
    COPY_STRING_FIELD(location);

    return newnode;
}

static DropDirectoryStmt* _copyDropDirectoryStmt(const DropDirectoryStmt* from)
{
    DropDirectoryStmt* newnode = makeNode(DropDirectoryStmt);

    COPY_STRING_FIELD(directoryname);

    return newnode;
}

static CreateSynonymStmt* _copyCreateSynonymStmt(const CreateSynonymStmt* from)
{
    CreateSynonymStmt* newnode = makeNode(CreateSynonymStmt);

    COPY_SCALAR_FIELD(replace);
    COPY_NODE_FIELD(synName);
    COPY_NODE_FIELD(objName);

    return newnode;
}

static DropSynonymStmt* _copyDropSynonymStmt(const DropSynonymStmt* from)
{
    DropSynonymStmt* newnode = makeNode(DropSynonymStmt);

    COPY_NODE_FIELD(synName);
    COPY_SCALAR_FIELD(behavior);
    COPY_SCALAR_FIELD(missing);

    return newnode;
}

// DB4AI
static CreateModelStmt* _copyCreateModelStmt(const CreateModelStmt* from){
    CreateModelStmt* newnode = makeNode(CreateModelStmt);
    COPY_STRING_FIELD(model);
    COPY_STRING_FIELD(architecture);
    COPY_NODE_FIELD(hyperparameters);
    COPY_NODE_FIELD(select_query);
    COPY_NODE_FIELD(model_features);
    COPY_NODE_FIELD(model_target);
    COPY_SCALAR_FIELD(algorithm);

    return newnode;
}

static CreatePublicationStmt *_copyCreatePublicationStmt(const CreatePublicationStmt *from)
{
    CreatePublicationStmt *newnode = makeNode(CreatePublicationStmt);

    COPY_STRING_FIELD(pubname);
    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(tables);
    COPY_SCALAR_FIELD(for_all_tables);

    return newnode;
}

static AlterPublicationStmt *_copyAlterPublicationStmt(const AlterPublicationStmt *from)
{
    AlterPublicationStmt *newnode = makeNode(AlterPublicationStmt);

    COPY_STRING_FIELD(pubname);
    COPY_NODE_FIELD(options);
    COPY_NODE_FIELD(tables);
    COPY_SCALAR_FIELD(for_all_tables);
    COPY_SCALAR_FIELD(tableAction);

    return newnode;
}

static CreateSubscriptionStmt *_copyCreateSubscriptionStmt(const CreateSubscriptionStmt *from)
{
    CreateSubscriptionStmt *newnode = makeNode(CreateSubscriptionStmt);

    COPY_STRING_FIELD(subname);
    COPY_STRING_FIELD(conninfo);
    COPY_NODE_FIELD(publication);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterSubscriptionStmt *_copyAlterSubscriptionStmt(const AlterSubscriptionStmt *from)
{
    AlterSubscriptionStmt *newnode = makeNode(AlterSubscriptionStmt);

    COPY_STRING_FIELD(subname);
    COPY_NODE_FIELD(options);

    return newnode;
}

static DropSubscriptionStmt *_copyDropSubscriptionStmt(const DropSubscriptionStmt *from)
{
    DropSubscriptionStmt *newnode = makeNode(DropSubscriptionStmt);

    COPY_STRING_FIELD(subname);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_SCALAR_FIELD(behavior);

    return newnode;
}

static PredictByFunction *_copyPredictByFunctionStmt(const PredictByFunction *from)
{
    PredictByFunction* newnode = makeNode(PredictByFunction);
    COPY_STRING_FIELD(model_name);
    COPY_SCALAR_FIELD(model_name_location);
    COPY_NODE_FIELD(model_args);
    COPY_SCALAR_FIELD(model_args_location);
    return newnode;
}

/* ****************************************************************
 *					pg_list.h copy functions
 * ****************************************************************
 */

/*
 * Perform a deep copy of the specified list, using copyObject(). The
 * list MUST be of type T_List; T_IntList and T_OidList nodes don't
 * need deep copies, so they should be copied via list_copy()
 */
#define COPY_NODE_CELL(newm, old)                 \
    (newm) = (ListCell*)palloc(sizeof(ListCell)); \
    lfirst(newm) = copyObject(lfirst(old));

static List* _copyList(const List* from)
{
    List* newm = NIL;
    ListCell* curr_old = NULL;
    ListCell* prev_new = NULL;

    Assert(list_length(from) >= 1);

    newm = makeNode(List);
    newm->length = from->length;

    COPY_NODE_CELL(newm->head, from->head);
    prev_new = newm->head;
    curr_old = lnext(from->head);

    while (curr_old != NULL) {
        COPY_NODE_CELL(prev_new->next, curr_old);
        prev_new = prev_new->next;
        curr_old = curr_old->next;
    }
    prev_new->next = NULL;
    newm->tail = prev_new;

    return newm;
}

/* ****************************************************************
 *                  value.h copy functions
 * ****************************************************************
 */
static Value* _copyValue(const Value* from)
{
    Value* newnode = makeNode(Value);

    /* See also _copyAConst when changing this code! */
    COPY_SCALAR_FIELD(type);
    switch (from->type) {
        case T_Integer:
            COPY_SCALAR_FIELD(val.ival);
            break;
        case T_Float:
        case T_String:
        case T_BitString:
            COPY_STRING_FIELD(val.str);
            break;
        case T_Null:
            /* nothing to do */
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)from->type)));
            break;
    }
    return newnode;
}

#ifdef PGXC
/* ****************************************************************
 *                  barrier.h copy functions
 * ****************************************************************
 */
static BarrierStmt* _copyBarrierStmt(const BarrierStmt* from)
{
    BarrierStmt* newnode = makeNode(BarrierStmt);

    COPY_STRING_FIELD(id);

    return newnode;
}

/* ****************************************************************
 *                  nodemgr.h copy functions
 * ****************************************************************
 */
static AlterNodeStmt* _copyAlterNodeStmt(const AlterNodeStmt* from)
{
    AlterNodeStmt* newnode = makeNode(AlterNodeStmt);

    COPY_STRING_FIELD(node_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static CreateNodeStmt* _copyCreateNodeStmt(const CreateNodeStmt* from)
{
    CreateNodeStmt* newnode = makeNode(CreateNodeStmt);

    COPY_STRING_FIELD(node_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static DropNodeStmt* _copyDropNodeStmt(const DropNodeStmt* from)
{
    DropNodeStmt* newnode = makeNode(DropNodeStmt);

    COPY_STRING_FIELD(node_name);
    COPY_SCALAR_FIELD(missing_ok);
    COPY_NODE_FIELD(remote_nodes);

    return newnode;
}

/* ****************************************************************
 *                  groupmgr.h copy functions
 * ****************************************************************
 */
static CreateGroupStmt* _copyCreateGroupStmt(const CreateGroupStmt* from)
{
    CreateGroupStmt* newnode = makeNode(CreateGroupStmt);

    COPY_STRING_FIELD(group_name);
    COPY_STRING_FIELD(group_parent);
    COPY_STRING_FIELD(src_group_name);
    COPY_NODE_FIELD(nodes);
    COPY_NODE_FIELD(buckets);
    COPY_SCALAR_FIELD(bucketcnt);
    COPY_SCALAR_FIELD(vcgroup);

    return newnode;
}

static AlterGroupStmt* _copyAlterGroupStmt(const AlterGroupStmt* from)
{
    AlterGroupStmt* newnode = makeNode(AlterGroupStmt);

    COPY_STRING_FIELD(group_name);
    COPY_STRING_FIELD(install_name);
    COPY_SCALAR_FIELD(alter_type);
    COPY_NODE_FIELD(nodes);

    return newnode;
}

static DropGroupStmt* _copyDropGroupStmt(const DropGroupStmt* from)
{
    DropGroupStmt* newnode = makeNode(DropGroupStmt);

    COPY_STRING_FIELD(group_name);
    COPY_STRING_FIELD(src_group_name);
    COPY_SCALAR_FIELD(to_elastic_group);

    return newnode;
}

/* ****************************************************************
 *                  workload.h copy functions
 * ****************************************************************
 */
static CreateResourcePoolStmt* _copyCreateResourcePoolStmt(const CreateResourcePoolStmt* from)
{
    CreateResourcePoolStmt* newnode = makeNode(CreateResourcePoolStmt);

    COPY_STRING_FIELD(pool_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterResourcePoolStmt* _copyAlterResourcePoolStmt(const AlterResourcePoolStmt* from)
{
    AlterResourcePoolStmt* newnode = makeNode(AlterResourcePoolStmt);

    COPY_STRING_FIELD(pool_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static DropResourcePoolStmt* _copyDropResourcePoolStmt(const DropResourcePoolStmt* from)
{
    DropResourcePoolStmt* newnode = makeNode(DropResourcePoolStmt);

    COPY_SCALAR_FIELD(missing_ok);
    COPY_STRING_FIELD(pool_name);

    return newnode;
}

static AlterGlobalConfigStmt* _copyAlterGlobalConfigStmt(const AlterGlobalConfigStmt* from)
{
    AlterGlobalConfigStmt* newnode = makeNode(AlterGlobalConfigStmt);

    COPY_NODE_FIELD(options);

    return newnode;
}

static DropGlobalConfigStmt* _copyDropGlobalConfigStmt(const DropGlobalConfigStmt* from)
{
    DropGlobalConfigStmt* newnode = makeNode(DropGlobalConfigStmt);

    COPY_NODE_FIELD(options);

    return newnode;
}

static CreateWorkloadGroupStmt* _copyCreateWorkloadGroupStmt(const CreateWorkloadGroupStmt* from)
{
    CreateWorkloadGroupStmt* newnode = makeNode(CreateWorkloadGroupStmt);

    COPY_STRING_FIELD(group_name);
    COPY_STRING_FIELD(pool_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterWorkloadGroupStmt* _copyAlterWorkloadGroupStmt(const AlterWorkloadGroupStmt* from)
{
    AlterWorkloadGroupStmt* newnode = makeNode(AlterWorkloadGroupStmt);

    COPY_STRING_FIELD(group_name);
    COPY_STRING_FIELD(pool_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static DropWorkloadGroupStmt* _copyDropWorkloadGroupStmt(const DropWorkloadGroupStmt* from)
{
    DropWorkloadGroupStmt* newnode = makeNode(DropWorkloadGroupStmt);

    COPY_SCALAR_FIELD(missing_ok);
    COPY_STRING_FIELD(group_name);

    return newnode;
}

static CreateAppWorkloadGroupMappingStmt* _copyCreateAppWorkloadGroupMappingStmt(
    const CreateAppWorkloadGroupMappingStmt* from)
{
    CreateAppWorkloadGroupMappingStmt* newnode = makeNode(CreateAppWorkloadGroupMappingStmt);

    COPY_STRING_FIELD(app_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static AlterAppWorkloadGroupMappingStmt* _copyAlterAppWorkloadGroupMappingStmt(
    const AlterAppWorkloadGroupMappingStmt* from)
{
    AlterAppWorkloadGroupMappingStmt* newnode = makeNode(AlterAppWorkloadGroupMappingStmt);

    COPY_STRING_FIELD(app_name);
    COPY_NODE_FIELD(options);

    return newnode;
}

static DropAppWorkloadGroupMappingStmt* _copyDropAppWorkloadGroupMappingStmt(
    const DropAppWorkloadGroupMappingStmt* from)
{
    DropAppWorkloadGroupMappingStmt* newnode = makeNode(DropAppWorkloadGroupMappingStmt);

    COPY_SCALAR_FIELD(missing_ok);
    COPY_STRING_FIELD(app_name);

    return newnode;
}

/* ****************************************************************
 *                  poolutils.h copy functions
 * ****************************************************************
 */
static CleanConnStmt* _copyCleanConnStmt(const CleanConnStmt* from)
{
    CleanConnStmt* newnode = makeNode(CleanConnStmt);

    COPY_NODE_FIELD(nodes);
    COPY_STRING_FIELD(dbname);
    COPY_STRING_FIELD(username);
    COPY_SCALAR_FIELD(is_coord);
    COPY_SCALAR_FIELD(is_force);
    COPY_SCALAR_FIELD(is_check);

    return newnode;
}

#endif

static ErrorCacheEntry* _copyErrorCacheEntry(const ErrorCacheEntry* from)
{
    ErrorCacheEntry* newnode = makeNode(ErrorCacheEntry);
    COPY_NODE_FIELD(rte);
    COPY_STRING_FIELD(filename);
    return newnode;
}

static InformationalConstraint* _copyInformationalConstraint(const InformationalConstraint* from)
{
    InformationalConstraint* newnode = makeNode(InformationalConstraint);

    COPY_STRING_FIELD(constrname);
    COPY_SCALAR_FIELD(contype);
    COPY_SCALAR_FIELD(nonforced);
    COPY_SCALAR_FIELD(enableOpt);

    return newnode;
}

static GroupingId* _copyGroupingId(const GroupingId* from)
{
    GroupingId* newnode = makeNode(GroupingId);
    return newnode;
}

static BloomFilterSet* _copyBloomFilterSet(const BloomFilterSet* from)
{
    BloomFilterSet* newnode = makeNode(BloomFilterSet);

    /* copy uint64 array
     * first palloc memory for array 
     */
    COPY_POINTER_FIELD(data, sizeof(uint64) * from->length);
    COPY_SCALAR_FIELD(length);
    COPY_SCALAR_FIELD(numBits);
    COPY_SCALAR_FIELD(numHashFunctions);
    COPY_SCALAR_FIELD(numValues);
    COPY_SCALAR_FIELD(maxNumValues);
    COPY_SCALAR_FIELD(startupEntries);

    /* test the data value. */
    elog(DEBUG1, "_copyBloomFilterSet:: data");
    StringInfo str = makeStringInfo();
    appendStringInfo(str, "\n");

    for (uint64 i = 0; i < from->length; i++) {
        Assert(from->data[i] == newnode->data[i]);
        if (from->data[i] != 0) {
            appendStringInfo(str, "not zore value in position %lu , value %lu,\n", i, newnode->data[i]);
        }
    }

    elog(DEBUG1, "%s", str->data);
    resetStringInfo(str);
    elog(DEBUG1, "_copyBloomFilterSet:: valuePositions");
    appendStringInfo(str, "\n");

    if (from->startupEntries > 0) {
        newnode->valuePositions = (ValueBit*)palloc0(from->startupEntries * sizeof(ValueBit));
        for (uint64 cell = 0; cell < from->startupEntries; cell++) {
            COPY_ARRAY_FIELD(valuePositions[cell].position);

            /* test valuePositions value */
            for (int i = 0; i < 4; i++) {
                Assert(from->valuePositions[cell].position[i] == newnode->valuePositions[cell].position[i]);
                appendStringInfo(str, "%d, ", from->valuePositions[cell].position[i]);
            }
            appendStringInfo(str, "\n");
        }

        elog(DEBUG1, "%s", str->data);
    }

    COPY_SCALAR_FIELD(minIntValue);
    COPY_SCALAR_FIELD(minFloatValue);
    COPY_STRING_FIELD(minStringValue);
    COPY_SCALAR_FIELD(maxIntValue);
    COPY_SCALAR_FIELD(maxFloatValue);
    COPY_STRING_FIELD(maxStringValue);

    COPY_SCALAR_FIELD(addMinMax);
    COPY_SCALAR_FIELD(hasMM);
    COPY_SCALAR_FIELD(bfType);
    COPY_SCALAR_FIELD(dataType);
    COPY_SCALAR_FIELD(typeMod);
    COPY_SCALAR_FIELD(collation);

    pfree_ext(str->data);

    return newnode;
}

static ShutdownStmt* _copyShutdownStmt(const ShutdownStmt* from)
{
    ShutdownStmt* newnode = makeNode(ShutdownStmt);
    COPY_STRING_FIELD(mode);
    return newnode;
}

static SubPartitionPruningResult *_copySubPartitionPruningResult(const SubPartitionPruningResult *from)
{
    SubPartitionPruningResult* newnode = makeNode(SubPartitionPruningResult);
    COPY_SCALAR_FIELD(partSeq);
    COPY_BITMAPSET_FIELD(bm_selectedSubPartitions);
    COPY_NODE_FIELD(ls_selectedSubPartitions);
    return newnode;
}

/*
 * copyObject
 *
 * Create a copy of a Node tree or list.  This is a "deep" copy: all
 * substructure is copied too, recursively.
 */
void* copyObject(const void* from)
{
    void* retval = NULL;

    if (from == NULL) {
        return NULL;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(from)) {
            /*
             * PLAN NODES
             */
        case T_PlannedStmt:
            retval = _copyPlannedStmt((PlannedStmt*)from);
            break;
        case T_Plan:
            retval = _copyPlan((Plan*)from);
            break;
        case T_BaseResult:
            retval = _copyResult((BaseResult*)from);
            break;
        case T_ModifyTable:
            retval = _copyModifyTable((ModifyTable*)from);
            break;
        case T_Append:
            retval = _copyAppend((Append*)from);
            break;
        case T_MergeAppend:
            retval = _copyMergeAppend((MergeAppend*)from);
            break;
        case T_PartIterator:
            retval = _copyPartIterator((PartIterator*)from);
            break;
        case T_RecursiveUnion:
            retval = _copyRecursiveUnion((RecursiveUnion*)from);
            break;
        case T_StartWithOp:
            retval = _copyStartWithOp((StartWithOp*)from);
            break;
        case T_BitmapAnd:
            retval = _copyBitmapAnd((BitmapAnd*)from);
            break;
        case T_BitmapOr:
            retval = _copyBitmapOr((BitmapOr*)from);
            break;
        case T_Scan:
            retval = _copyScan((Scan*)from);
            break;
        case T_BucketInfo:
            retval = _copyBucketInfo((BucketInfo*)from);
            break;
        case T_SeqScan:
            retval = _copySeqScan((SeqScan*)from);
            break;
        case T_IndexScan:
            retval = _copyIndexScan((IndexScan*)from);
            break;
        case T_IndexOnlyScan:
            retval = _copyIndexOnlyScan((IndexOnlyScan*)from);
            break;
        case T_DfsIndexScan:
            retval = _copyDfsIndexScan((DfsIndexScan*)from);
            break;
        case T_CStoreIndexScan:
            retval = _copyCStoreIndexScan((CStoreIndexScan*)from);
            break;
        case T_CStoreIndexCtidScan:
            retval = _copyCStoreIndexCtidScan((CStoreIndexCtidScan*)from);
            break;
        case T_CStoreIndexHeapScan:
            retval = _copyCStoreIndexHeapScan((CStoreIndexHeapScan*)from);
            break;
        case T_CStoreIndexAnd:
            retval = _copyCStoreIndexAnd((CStoreIndexAnd*)from);
            break;
        case T_CStoreIndexOr:
            retval = _copyCStoreIndexOr((CStoreIndexOr*)from);
            break;
        case T_BitmapIndexScan:
            retval = _copyBitmapIndexScan((BitmapIndexScan*)from);
            break;
        case T_BitmapHeapScan:
            retval = _copyBitmapHeapScan((BitmapHeapScan*)from);
            break;
        case T_TidScan:
            retval = _copyTidScan((TidScan*)from);
            break;
        case T_SubqueryScan:
            retval = _copySubqueryScan((SubqueryScan*)from);
            break;
        case T_FunctionScan:
            retval = _copyFunctionScan((FunctionScan*)from);
            break;
        case T_ValuesScan:
            retval = _copyValuesScan((ValuesScan*)from);
            break;
        case T_CteScan:
            retval = _copyCteScan((CteScan*)from);
            break;
        case T_WorkTableScan:
            retval = _copyWorkTableScan((WorkTableScan*)from);
            break;
        case T_ForeignScan:
            retval = _copyForeignScan((ForeignScan*)from);
            break;
        case T_ExtensiblePlan:
            retval = _copyExtensiblePlan((ExtensiblePlan*)from);
            break;
        case T_RelationMetaData:
            retval = _copyRelationMetaData((RelationMetaData*)from);
            break;
        case T_AttrMetaData:
            retval = _copyAttrMetaData((AttrMetaData*)from);
            break;
        case T_ForeignOptions:
            retval = _copyForeignOptions((ForeignOptions*)from);
            break;
        case T_Join:
            retval = _copyJoin((Join*)from);
            break;
        case T_NestLoop:
            retval = _copyNestLoop((NestLoop*)from);
            break;
        case T_VecNestLoop:
            retval = _copyVecNestLoop((VecNestLoop*)from);
            break;
        case T_MergeJoin:
            retval = _copyMergeJoin((MergeJoin*)from);
            break;
        case T_VecMergeJoin:
            retval = _copyVecMergeJoin((VecMergeJoin*)from);
            break;
        case T_HashJoin:
            retval = _copyHashJoin((HashJoin*)from);
            break;
        case T_Material:
            retval = _copyMaterial((Material*)from);
            break;
        case T_Sort:
            retval = _copySort((Sort*)from);
            break;
        case T_Group:
            retval = _copyGroup((Group*)from);
            break;
        case T_Agg:
            retval = _copyAgg((Agg*)from);
            break;
        case T_WindowAgg:
            retval = _copyWindowAgg((WindowAgg*)from);
            break;
        case T_Unique:
            retval = _copyUnique((Unique*)from);
            break;
        case T_Hash:
            retval = _copyHash((Hash*)from);
            break;
        case T_SetOp:
            retval = _copySetOp((SetOp*)from);
            break;
        case T_LockRows:
            retval = _copyLockRows((LockRows*)from);
            break;
        case T_Limit:
            retval = _copyLimit((Limit*)from);
            break;
        case T_NestLoopParam:
            retval = _copyNestLoopParam((NestLoopParam*)from);
            break;
        case T_PartIteratorParam:
            retval = _copyPartIteratorParam((PartIteratorParam*)from);
            break;
        case T_PlanRowMark:
            retval = _copyPlanRowMark((PlanRowMark*)from);
            break;
        case T_PlanInvalItem:
            retval = _copyPlanInvalItem((PlanInvalItem*)from);
            break;
        case T_FuncInvalItem:
            retval = _copyFuncInvalItem((FuncInvalItem*)from);
            break;
        case T_VecToRow:
            retval = _copyVecToRow((VecToRow*)from);
            break;
        case T_RowToVec:
            retval = _copyRowToVec((RowToVec*)from);
            break;
        case T_VecSort:
            retval = _copyVecSort((VecSort*)from);
            break;
        case T_VecResult:
            retval = _copyVecResult((VecResult*)from);
            break;
        case T_CStoreScan:
            retval = _copyCStoreScan((CStoreScan*)from);
            break;
        case T_DfsScan:
            retval = _copyDfsScan((DfsScan*)from);
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            retval = _copyTsStoreScan((TsStoreScan *)from);
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_VecSubqueryScan:
            retval = _copyVecSubqueryScan((VecSubqueryScan*)from);
            break;
        case T_VecHashJoin:
            retval = _copyVecHashJoin((VecHashJoin*)from);
            break;
        case T_VecAgg:
            retval = _copyVecAgg((VecAgg*)from);
            break;
        case T_VecPartIterator:
            retval = _copyVecPartIterator((VecPartIterator*)from);
            break;
        case T_VecAppend:
            retval = _copyVecAppend((VecAppend*)from);
            break;
        case T_VecSetOp:
            retval = _copyVecSetOp((VecSetOp*)from);
            break;
        case T_VecForeignScan:
            retval = _copyVecForeignScan((VecForeignScan*)from);
            break;
        case T_VecModifyTable:
            retval = _copyVecModifyTable((VecModifyTable*)from);
            break;
        case T_VecGroup:
            retval = _copyVecGroup((VecGroup*)from);
            break;
        case T_VecUnique:
            retval = _copyVecUnique((VecUnique*)from);
            break;
        case T_VecLimit:
            retval = _copyVecLimit((VecLimit*)from);
            break;
        case T_VecMaterial:
            retval = _copyVecMaterial((VecMaterial*)from);
            break;
        case T_VecWindowAgg:
            retval = _copyVecWindowAgg((VecWindowAgg*)from);
            break;

#ifdef PGXC
        case T_VecStream:
            retval = _copyVecStream((VecStream*)from);
            break;
        case T_VecRemoteQuery:
            retval = _copyVecRemoteQuery((VecRemoteQuery*)from);
            break;
            /*
             * PGXC SPECIFIC NODES
             */
        case T_ExecDirectStmt:
            retval = _copyExecDirect((ExecDirectStmt*)from);
            break;
        case T_RemoteQuery:
            retval = _copyRemoteQuery((RemoteQuery*)from);
            break;
        case T_ExecNodes:
            retval = _copyExecNodes((ExecNodes*)from);
            break;
        case T_SliceBoundary:
            retval = _copySliceBoundary((SliceBoundary*)from);
            break;
        case T_ExecBoundary:
            retval = _copyExecBoundary((ExecBoundary*)from);
            break;
        case T_SimpleSort:
            retval = _copySimpleSort((SimpleSort*)from);
            break;
#endif
        case T_Stream:
            retval = _copyStream((Stream*)from);
            break;
            /*
             * PRIMITIVE NODES
             */
        case T_Alias:
            retval = _copyAlias((Alias*)from);
            break;
        case T_RangeVar:
            retval = _copyRangeVar((RangeVar*)from);
            break;
        case T_IntoClause:
            retval = _copyIntoClause((IntoClause*)from);
            break;
        case T_StartWithClause:
            retval = _copyStartWithClause((StartWithClause*)from);
            break;
        case T_Var:
            retval = _copyVar((Var*)from);
            break;
        case T_Const:
            retval = _copyConst((Const*)from);
            break;
        case T_Param:
            retval = _copyParam((Param*)from);
            break;
        case T_Rownum:
            retval = _copyRownum((Rownum*)from);
            break;
        case T_Aggref:
            retval = _copyAggref((Aggref*)from);
            break;
        case T_GroupingFunc:
            retval = _copyGroupingFunc((GroupingFunc*)from);
            break;
        case T_WindowFunc:
            retval = _copyWindowFunc((WindowFunc*)from);
            break;
        case T_ArrayRef:
            retval = _copyArrayRef((ArrayRef*)from);
            break;
        case T_FuncExpr:
            retval = _copyFuncExpr((FuncExpr*)from);
            break;
        case T_NamedArgExpr:
            retval = _copyNamedArgExpr((NamedArgExpr*)from);
            break;
        case T_OpExpr:
            retval = _copyOpExpr((OpExpr*)from);
            break;
        case T_DistinctExpr:
            retval = _copyDistinctExpr((DistinctExpr*)from);
            break;
        case T_NullIfExpr:
            retval = _copyNullIfExpr((NullIfExpr*)from);
            break;
        case T_ScalarArrayOpExpr:
            retval = _copyScalarArrayOpExpr((ScalarArrayOpExpr*)from);
            break;
        case T_BoolExpr:
            retval = _copyBoolExpr((BoolExpr*)from);
            break;
        case T_SubLink:
            retval = _copySubLink((SubLink*)from);
            break;
        case T_SubPlan:
            retval = _copySubPlan((SubPlan*)from);
            break;
        case T_AlternativeSubPlan:
            retval = _copyAlternativeSubPlan((AlternativeSubPlan*)from);
            break;
        case T_FieldSelect:
            retval = _copyFieldSelect((FieldSelect*)from);
            break;
        case T_FieldStore:
            retval = _copyFieldStore((FieldStore*)from);
            break;
        case T_RelabelType:
            retval = _copyRelabelType((RelabelType*)from);
            break;
        case T_CoerceViaIO:
            retval = _copyCoerceViaIO((CoerceViaIO*)from);
            break;
        case T_ArrayCoerceExpr:
            retval = _copyArrayCoerceExpr((ArrayCoerceExpr*)from);
            break;
        case T_ConvertRowtypeExpr:
            retval = _copyConvertRowtypeExpr((ConvertRowtypeExpr*)from);
            break;
        case T_CollateExpr:
            retval = _copyCollateExpr((CollateExpr*)from);
            break;
        case T_CaseExpr:
            retval = _copyCaseExpr((CaseExpr*)from);
            break;
        case T_CaseWhen:
            retval = _copyCaseWhen((CaseWhen*)from);
            break;
        case T_CaseTestExpr:
            retval = _copyCaseTestExpr((CaseTestExpr*)from);
            break;
        case T_ArrayExpr:
            retval = _copyArrayExpr((ArrayExpr*)from);
            break;
        case T_RowExpr:
            retval = _copyRowExpr((RowExpr*)from);
            break;
        case T_RowCompareExpr:
            retval = _copyRowCompareExpr((RowCompareExpr*)from);
            break;
        case T_CoalesceExpr:
            retval = _copyCoalesceExpr((CoalesceExpr*)from);
            break;
        case T_MinMaxExpr:
            retval = _copyMinMaxExpr((MinMaxExpr*)from);
            break;
        case T_XmlExpr:
            retval = _copyXmlExpr((XmlExpr*)from);
            break;
        case T_NullTest:
            retval = _copyNullTest((NullTest*)from);
            break;
        case T_HashFilter:
            retval = _copyHashFilter((HashFilter*)from);
            break;
        case T_BooleanTest:
            retval = _copyBooleanTest((BooleanTest*)from);
            break;
        case T_CoerceToDomain:
            retval = _copyCoerceToDomain((CoerceToDomain*)from);
            break;
        case T_CoerceToDomainValue:
            retval = _copyCoerceToDomainValue((CoerceToDomainValue*)from);
            break;
        case T_SetToDefault:
            retval = _copySetToDefault((SetToDefault*)from);
            break;
        case T_CurrentOfExpr:
            retval = _copyCurrentOfExpr((CurrentOfExpr*)from);
            break;
        case T_TargetEntry:
            retval = _copyTargetEntry((TargetEntry*)from);
            break;
        case T_PseudoTargetEntry:
            retval = _copyPseudoTargetEntry((PseudoTargetEntry*)from);
            break;
        case T_RangeTblRef:
            retval = _copyRangeTblRef((RangeTblRef*)from);
            break;
        case T_JoinExpr:
            retval = _copyJoinExpr((JoinExpr*)from);
            break;
        case T_FromExpr:
            retval = _copyFromExpr((FromExpr*)from);
            break;
        case T_UpsertExpr:
            retval = _copyUpsertExpr((UpsertExpr *)from);
            break;
        case T_PartitionState:
            retval = _copyPartitionState((PartitionState*)from);
            break;
        case T_RangePartitionDefState:
            retval = _copyRangePartitionDefState((RangePartitionDefState*)from);
            break;
        case T_ListPartitionDefState:
            retval = _copyListPartitionDefState((ListPartitionDefState*)from);
            break;
        case T_HashPartitionDefState:
            retval = _copyHashPartitionDefState((HashPartitionDefState*)from);
            break;
        case T_IntervalPartitionDefState:
            retval = _copyIntervalPartitionDefState((IntervalPartitionDefState*)from);
            break;
        case T_RangePartitionindexDefState:
            retval = _copyRangePartitionindexDefState((RangePartitionindexDefState*)from);
            break;
        case T_SplitPartitionState:
            retval = _copySplitPartitionState((SplitPartitionState*)from);
            break;
        case T_AddPartitionState:
            retval = _copyAddPartitionState((AddPartitionState*)from);
            break;
        case T_AddSubPartitionState:
            retval = _copyAddSubPartitionState((AddSubPartitionState*)from);
            break;
        case T_RangePartitionStartEndDefState:
            retval = _copyRangePartitionStartEndDefState((RangePartitionStartEndDefState*)from);
            break;
        case T_MergeAction:
            retval = _copyMergeAction((MergeAction*)from);
            break;
            /*
             * RELATION NODES
             */
        case T_PathKey:
            retval = _copyPathKey((PathKey*)from);
            break;
        case T_RestrictInfo:
            retval = _copyRestrictInfo((RestrictInfo*)from);
            break;
        case T_PlaceHolderVar:
            retval = _copyPlaceHolderVar((PlaceHolderVar*)from);
            break;
        case T_SpecialJoinInfo:
            retval = _copySpecialJoinInfo((SpecialJoinInfo*)from);
            break;
        case T_LateralJoinInfo:
            retval = _copyLateralJoinInfo((LateralJoinInfo *)from);
            break;
        case T_AppendRelInfo:
            retval = _copyAppendRelInfo((AppendRelInfo*)from);
            break;
        case T_PlaceHolderInfo:
            retval = _copyPlaceHolderInfo((PlaceHolderInfo*)from);
            break;

            /*
             * VALUE NODES
             */
        case T_Integer:
        case T_Float:
        case T_String:
        case T_BitString:
        case T_Null:
            retval = _copyValue((Value*)from);
            break;

            /*
             * LIST NODES
             */
        case T_List:
            retval = _copyList((List*)from);
            break;

            /*
             * Lists of integers and OIDs don't need to be deep-copied, so we
             * perform a shallow copy via list_copy()
             */
        case T_IntList:
        case T_OidList:
            retval = list_copy((List*)from);
            break;

            /*
             * PARSE NODES
             */
        case T_Query:
            retval = _copyQuery((Query*)from);
            break;
        case T_InsertStmt:
            retval = _copyInsertStmt((InsertStmt*)from);
            break;
        case T_DeleteStmt:
            retval = _copyDeleteStmt((DeleteStmt*)from);
            break;
        case T_UpdateStmt:
            retval = _copyUpdateStmt((UpdateStmt*)from);
            break;
        case T_MergeStmt:
            retval = _copyMergeStmt((MergeStmt*)from);
            break;
        case T_MergeWhenClause:
            retval = _copyMergeWhenClause((MergeWhenClause*)from);
            break;
        case T_SelectStmt:
            retval = _copySelectStmt((SelectStmt*)from);
            break;
        case T_SetOperationStmt:
            retval = _copySetOperationStmt((SetOperationStmt*)from);
            break;
        case T_AlterTableStmt:
            retval = _copyAlterTableStmt((AlterTableStmt*)from);
            break;
        case T_AlterTableCmd:
            retval = _copyAlterTableCmd((AlterTableCmd*)from);
            break;
        case T_AlterDomainStmt:
            retval = _copyAlterDomainStmt((AlterDomainStmt*)from);
            break;
        case T_GrantStmt:
            retval = _copyGrantStmt((GrantStmt*)from);
            break;
        case T_GrantRoleStmt:
            retval = _copyGrantRoleStmt((GrantRoleStmt*)from);
            break;
        case T_GrantDbStmt:
            retval = _copyGrantDbStmt((GrantDbStmt*)from);
            break;
        case T_AlterDefaultPrivilegesStmt:
            retval = _copyAlterDefaultPrivilegesStmt((AlterDefaultPrivilegesStmt*)from);
            break;
        case T_DeclareCursorStmt:
            retval = _copyDeclareCursorStmt((DeclareCursorStmt*)from);
            break;
        case T_ClosePortalStmt:
            retval = _copyClosePortalStmt((ClosePortalStmt*)from);
            break;
        case T_ClusterStmt:
            retval = _copyClusterStmt((ClusterStmt*)from);
            break;
        case T_CopyStmt:
            retval = _copyCopyStmt((CopyStmt*)from);
            break;
        case T_CreateStmt:
            retval = _copyCreateStmt((CreateStmt*)from);
            break;
        case T_TableLikeClause:
            retval = _copyTableLikeClause((const TableLikeClause*)from);
            break;
        case T_DefineStmt:
            retval = _copyDefineStmt((DefineStmt*)from);
            break;
        case T_DropStmt:
            retval = _copyDropStmt((DropStmt*)from);
            break;
        case T_TruncateStmt:
            retval = _copyTruncateStmt((TruncateStmt*)from);
            break;
        case T_PurgeStmt:
            retval = CopyPurgeStmt((PurgeStmt*)from);
            break;
        case T_TimeCapsuleStmt:
            retval = CopyTimeCapsuleStmt((TimeCapsuleStmt*)from);
            break;
        case T_CommentStmt:
            retval = _copyCommentStmt((CommentStmt*)from);
            break;
        case T_SecLabelStmt:
            retval = _copySecLabelStmt((SecLabelStmt*)from);
            break;
        case T_FetchStmt:
            retval = _copyFetchStmt((FetchStmt*)from);
            break;
        case T_IndexStmt:
            retval = _copyIndexStmt((IndexStmt*)from);
            break;
        case T_CreateFunctionStmt:
            retval = _copyCreateFunctionStmt((CreateFunctionStmt*)from);
            break;
        case T_FunctionParameter:
            retval = _copyFunctionParameter((FunctionParameter*)from);
            break;
        case T_CreatePackageStmt:
            retval = _copyCreatePackageStmt((CreatePackageStmt*)from);
            break;
        case T_CreatePackageBodyStmt:
            retval = _copyCreatePackageBodyStmt((CreatePackageBodyStmt*)from);
            break;
        case T_AlterFunctionStmt:
            retval = _copyAlterFunctionStmt((AlterFunctionStmt*)from);
            break;
        case T_DoStmt:
            retval = _copyDoStmt((const DoStmt*)from);
            break;
        case T_RenameStmt:
            retval = _copyRenameStmt((RenameStmt*)from);
            break;
        case T_AlterObjectSchemaStmt:
            retval = _copyAlterObjectSchemaStmt((AlterObjectSchemaStmt*)from);
            break;
        case T_AlterOwnerStmt:
            retval = _copyAlterOwnerStmt((AlterOwnerStmt*)from);
            break;
        case T_RuleStmt:
            retval = _copyRuleStmt((RuleStmt*)from);
            break;
        case T_NotifyStmt:
            retval = _copyNotifyStmt((NotifyStmt*)from);
            break;
        case T_ListenStmt:
            retval = _copyListenStmt((ListenStmt*)from);
            break;
        case T_UnlistenStmt:
            retval = _copyUnlistenStmt((UnlistenStmt*)from);
            break;
        case T_TransactionStmt:
            retval = _copyTransactionStmt((TransactionStmt*)from);
            break;
        case T_CompositeTypeStmt:
            retval = _copyCompositeTypeStmt((CompositeTypeStmt*)from);
            break;
        case T_TableOfTypeStmt:
            retval = _copyTableOfTypeStmt((TableOfTypeStmt*)from);
            break;
        case T_CreateEnumStmt:
            retval = _copyCreateEnumStmt((CreateEnumStmt*)from);
            break;
        case T_CreateRangeStmt:
            retval = _copyCreateRangeStmt((const CreateRangeStmt*)from);
            break;
        case T_AlterEnumStmt:
            retval = _copyAlterEnumStmt((AlterEnumStmt*)from);
            break;
        case T_ViewStmt:
            retval = _copyViewStmt((ViewStmt*)from);
            break;
        case T_LoadStmt:
            retval = _copyLoadStmt((LoadStmt*)from);
            break;
        case T_CreateDomainStmt:
            retval = _copyCreateDomainStmt((CreateDomainStmt*)from);
            break;
        case T_CreateOpClassStmt:
            retval = _copyCreateOpClassStmt((CreateOpClassStmt*)from);
            break;
        case T_CreateOpClassItem:
            retval = _copyCreateOpClassItem((CreateOpClassItem*)from);
            break;
        case T_CreateOpFamilyStmt:
            retval = _copyCreateOpFamilyStmt((CreateOpFamilyStmt*)from);
            break;
        case T_AlterOpFamilyStmt:
            retval = _copyAlterOpFamilyStmt((AlterOpFamilyStmt*)from);
            break;
        case T_CreatedbStmt:
            retval = _copyCreatedbStmt((CreatedbStmt*)from);
            break;
        case T_AlterDatabaseStmt:
            retval = _copyAlterDatabaseStmt((AlterDatabaseStmt*)from);
            break;
        case T_AlterDatabaseSetStmt:
            retval = _copyAlterDatabaseSetStmt((AlterDatabaseSetStmt*)from);
            break;
        case T_DropdbStmt:
            retval = _copyDropdbStmt((DropdbStmt*)from);
            break;
        case T_VacuumStmt:
            retval = _copyVacuumStmt((VacuumStmt*)from);
            break;
        case T_ExplainStmt:
            retval = _copyExplainStmt((ExplainStmt*)from);
            break;
        case T_CreateTableAsStmt:
            retval = _copyCreateTableAsStmt((CreateTableAsStmt*)from);
            break;
        case T_RefreshMatViewStmt:
            retval = _copyRefreshMatViewStmt((RefreshMatViewStmt*)from);
            break;
        case T_ReplicaIdentityStmt:
            retval = _copyReplicaIdentityStmt((ReplicaIdentityStmt*)from);
            break;
#ifndef ENABLE_MULTIPLE_NODES
        case T_AlterSystemStmt:
            retval = _copyAlterSystemStmt((AlterSystemStmt*)from);
            break;
#endif
        case T_CreateSeqStmt:
            retval = _copyCreateSeqStmt((CreateSeqStmt*)from);
            break;
        case T_AlterSeqStmt:
            retval = _copyAlterSeqStmt((AlterSeqStmt*)from);
            break;
        case T_VariableSetStmt:
            retval = _copyVariableSetStmt((VariableSetStmt*)from);
            break;
        case T_VariableShowStmt:
            retval = _copyVariableShowStmt((VariableShowStmt*)from);
            break;
        case T_DiscardStmt:
            retval = _copyDiscardStmt((DiscardStmt*)from);
            break;
        case T_CreateTableSpaceStmt:
            retval = _copyCreateTableSpaceStmt((CreateTableSpaceStmt*)from);
            break;
        case T_DropTableSpaceStmt:
            retval = _copyDropTableSpaceStmt((DropTableSpaceStmt*)from);
            break;
        case T_AlterTableSpaceOptionsStmt:
            retval = _copyAlterTableSpaceOptionsStmt((AlterTableSpaceOptionsStmt*)from);
            break;
        case T_CreateExtensionStmt:
            retval = _copyCreateExtensionStmt((CreateExtensionStmt*)from);
            break;
        case T_AlterExtensionStmt:
            retval = _copyAlterExtensionStmt((AlterExtensionStmt*)from);
            break;
        case T_AlterExtensionContentsStmt:
            retval = _copyAlterExtensionContentsStmt((AlterExtensionContentsStmt*)from);
            break;
        case T_CreateFdwStmt:
            retval = _copyCreateFdwStmt((CreateFdwStmt*)from);
            break;
        case T_AlterFdwStmt:
            retval = _copyAlterFdwStmt((AlterFdwStmt*)from);
            break;
        case T_CreateForeignServerStmt:
            retval = _copyCreateForeignServerStmt((CreateForeignServerStmt*)from);
            break;
        case T_AlterForeignServerStmt:
            retval = _copyAlterForeignServerStmt((AlterForeignServerStmt*)from);
            break;
        case T_CreateUserMappingStmt:
            retval = _copyCreateUserMappingStmt((CreateUserMappingStmt*)from);
            break;
        case T_AlterUserMappingStmt:
            retval = _copyAlterUserMappingStmt((AlterUserMappingStmt*)from);
            break;
        case T_DropUserMappingStmt:
            retval = _copyDropUserMappingStmt((DropUserMappingStmt*)from);
            break;
        case T_ForeignPartState:
            retval = _copyForeignPartState((ForeignPartState*)from);
            break;
        case T_CreateForeignTableStmt:
            retval = _copyCreateForeignTableStmt((CreateForeignTableStmt*)from);
            break;
        case T_CreateDataSourceStmt:
            retval = _copyCreateDataSourceStmt((CreateDataSourceStmt*)from);
            break;
        case T_AlterSchemaStmt:
            retval = _copyAlterSchemaStmt((AlterSchemaStmt*)from);
            break;
        case T_AlterDataSourceStmt:
            retval = _copyAlterDataSourceStmt((AlterDataSourceStmt*)from);
            break;
        case T_CreateRlsPolicyStmt:
            retval = _copyCreateRlsPolicyStmt((CreateRlsPolicyStmt*)from);
            break;
        case T_AlterRlsPolicyStmt:
            retval = _copyAlterRlsPolicyStmt((AlterPolicyStmt*)from);
            break;
        case T_CreateTrigStmt:
            retval = _copyCreateTrigStmt((CreateTrigStmt*)from);
            break;
        case T_CreatePLangStmt:
            retval = _copyCreatePLangStmt((CreatePLangStmt*)from);
            break;
        case T_CreateRoleStmt:
            retval = _copyCreateRoleStmt((CreateRoleStmt*)from);
            break;
        case T_AlterRoleStmt:
            retval = _copyAlterRoleStmt((AlterRoleStmt*)from);
            break;
        case T_AlterRoleSetStmt:
            retval = _copyAlterRoleSetStmt((AlterRoleSetStmt*)from);
            break;
        case T_DropRoleStmt:
            retval = _copyDropRoleStmt((DropRoleStmt*)from);
            break;
        case T_LockStmt:
            retval = _copyLockStmt((LockStmt*)from);
            break;
        case T_ConstraintsSetStmt:
            retval = _copyConstraintsSetStmt((ConstraintsSetStmt*)from);
            break;
        case T_ReindexStmt:
            retval = _copyReindexStmt((ReindexStmt*)from);
            break;
        case T_CreatePolicyLabelStmt:
            retval = _copyCreatePolicyLabelStmt((CreatePolicyLabelStmt*)from);
            break;
        case T_AlterPolicyLabelStmt:
            retval = _copyAlterPolicyLabelStmt((AlterPolicyLabelStmt*)from);
            break;
        case T_DropPolicyLabelStmt:
            retval = _copyDropPolicyLabelStmt((DropPolicyLabelStmt*)from);
            break;
        case T_CreateAuditPolicyStmt:
            retval = _copyCreateAuditPolicyStmt((CreateAuditPolicyStmt*)from);
            break;
        case T_AlterAuditPolicyStmt:
            retval = _copyAlterAuditPolicyStmt((AlterAuditPolicyStmt*)from);
            break;
        case T_DropAuditPolicyStmt:
            retval = _copyDropAuditPolicyStmt((DropAuditPolicyStmt*)from);
            break;
        case T_CreateMaskingPolicyStmt:
            retval = _copyCreateMaskingPolicyStmt((CreateMaskingPolicyStmt*)from);
            break;
        case T_AlterMaskingPolicyStmt:
            retval = _copyAlterMaskingPolicyStmt((AlterMaskingPolicyStmt*)from);
            break;
        case T_DropMaskingPolicyStmt:
            retval = _copyDropMaskingPolicyStmt((DropMaskingPolicyStmt*)from);
            break;
        case T_MaskingPolicyCondition:
            retval = _copyMaskingPolicyCondition((MaskingPolicyCondition*)from);
            break;
        case T_PolicyFilterNode:
            retval = _copyPolicyFilterNode((PolicyFilterNode*)from);
            break;
        case T_CreateWeakPasswordDictionaryStmt:
            retval = _copyCreateWeakPasswordDictionaryStmt((CreateWeakPasswordDictionaryStmt*)from);
            break;
        case T_DropWeakPasswordDictionaryStmt:
            retval = _copyDropWeakPasswordDictionaryStmt((DropWeakPasswordDictionaryStmt*)from);
            break;
        case T_CheckPointStmt:
            retval = (void*)makeNode(CheckPointStmt);
            break;
#ifdef PGXC
        case T_BarrierStmt:
            retval = _copyBarrierStmt((BarrierStmt*)from);
            break;
        case T_AlterNodeStmt:
            retval = _copyAlterNodeStmt((AlterNodeStmt*)from);
            break;
        case T_CreateNodeStmt:
            retval = _copyCreateNodeStmt((CreateNodeStmt*)from);
            break;
        case T_DropNodeStmt:
            retval = _copyDropNodeStmt((DropNodeStmt*)from);
            break;
        case T_CreateGroupStmt:
            retval = _copyCreateGroupStmt((CreateGroupStmt*)from);
            break;
        case T_AlterGroupStmt:
            retval = _copyAlterGroupStmt((AlterGroupStmt*)from);
            break;
        case T_DropGroupStmt:
            retval = _copyDropGroupStmt((DropGroupStmt*)from);
            break;
        case T_CleanConnStmt:
            retval = _copyCleanConnStmt((CleanConnStmt*)from);
            break;
        case T_CreateResourcePoolStmt:
            retval = _copyCreateResourcePoolStmt((CreateResourcePoolStmt*)from);
            break;
        case T_AlterResourcePoolStmt:
            retval = _copyAlterResourcePoolStmt((AlterResourcePoolStmt*)from);
            break;
        case T_DropResourcePoolStmt:
            retval = _copyDropResourcePoolStmt((DropResourcePoolStmt*)from);
            break;
        case T_AlterGlobalConfigStmt:
            retval = _copyAlterGlobalConfigStmt((AlterGlobalConfigStmt*)from);
            break;
        case T_DropGlobalConfigStmt:
            retval = _copyDropGlobalConfigStmt((DropGlobalConfigStmt*)from);
            break;
        case T_CreateWorkloadGroupStmt:
            retval = _copyCreateWorkloadGroupStmt((CreateWorkloadGroupStmt*)from);
            break;
        case T_AlterWorkloadGroupStmt:
            retval = _copyAlterWorkloadGroupStmt((AlterWorkloadGroupStmt*)from);
            break;
        case T_DropWorkloadGroupStmt:
            retval = _copyDropWorkloadGroupStmt((DropWorkloadGroupStmt*)from);
            break;
        case T_CreateAppWorkloadGroupMappingStmt:
            retval = _copyCreateAppWorkloadGroupMappingStmt((CreateAppWorkloadGroupMappingStmt*)from);
            break;
        case T_AlterAppWorkloadGroupMappingStmt:
            retval = _copyAlterAppWorkloadGroupMappingStmt((AlterAppWorkloadGroupMappingStmt*)from);
            break;
        case T_DropAppWorkloadGroupMappingStmt:
            retval = _copyDropAppWorkloadGroupMappingStmt((DropAppWorkloadGroupMappingStmt*)from);
            break;
#endif
        case T_CreateSchemaStmt:
            retval = _copyCreateSchemaStmt((CreateSchemaStmt*)from);
            break;
        case T_CreateConversionStmt:
            retval = _copyCreateConversionStmt((CreateConversionStmt*)from);
            break;
        case T_CreateCastStmt:
            retval = _copyCreateCastStmt((CreateCastStmt*)from);
            break;
        case T_PrepareStmt:
            retval = _copyPrepareStmt((PrepareStmt*)from);
            break;
        case T_ExecuteStmt:
            retval = _copyExecuteStmt((ExecuteStmt*)from);
            break;
        case T_DeallocateStmt:
            retval = _copyDeallocateStmt((DeallocateStmt*)from);
            break;
        case T_DropOwnedStmt:
            retval = _copyDropOwnedStmt((DropOwnedStmt*)from);
            break;
        case T_ReassignOwnedStmt:
            retval = _copyReassignOwnedStmt((ReassignOwnedStmt*)from);
            break;
        case T_AlterTSDictionaryStmt:
            retval = _copyAlterTSDictionaryStmt((AlterTSDictionaryStmt*)from);
            break;
        case T_AlterTSConfigurationStmt:
            retval = _copyAlterTSConfigurationStmt((AlterTSConfigurationStmt*)from);
            break;
        case T_CreateSynonymStmt:
            retval = _copyCreateSynonymStmt((CreateSynonymStmt*)from);
            break;
        case T_DropSynonymStmt:
            retval = _copyDropSynonymStmt((DropSynonymStmt*)from);
            break;
        case T_CreateModelStmt: // DB4AI
            retval = _copyCreateModelStmt((CreateModelStmt*) from);
            break;
        case T_A_Expr:
            retval = _copyAExpr((A_Expr*)from);
            break;
        case T_ColumnRef:
            retval = _copyColumnRef((ColumnRef*)from);
            break;
        case T_ParamRef:
            retval = _copyParamRef((ParamRef*)from);
            break;
        case T_A_Const:
            retval = _copyAConst((A_Const*)from);
            break;
        case T_FuncCall:
            retval = _copyFuncCall((FuncCall*)from);
            break;
        case T_A_Star:
            retval = _copyAStar((A_Star*)from);
            break;
        case T_A_Indices:
            retval = _copyAIndices((A_Indices*)from);
            break;
        case T_A_Indirection:
            retval = _copyA_Indirection((A_Indirection*)from);
            break;
        case T_A_ArrayExpr:
            retval = _copyA_ArrayExpr((A_ArrayExpr*)from);
            break;
        case T_ResTarget:
            retval = _copyResTarget((ResTarget*)from);
            break;
        case T_TypeCast:
            retval = _copyTypeCast((TypeCast*)from);
            break;
        case T_CollateClause:
            retval = _copyCollateClause((CollateClause*)from);
            break;
        case T_ClientLogicGlobalParam:
            retval = _copyGlobalParam((ClientLogicGlobalParam*)from);
            break;
        case T_ClientLogicColumnParam:
            retval = _copyColumnParam((ClientLogicColumnParam*)from);
            break;
        case T_CreateClientLogicGlobal:
            retval = _copyGlobalSetting((CreateClientLogicGlobal*)from);
            break;
        case T_CreateClientLogicColumn:
            retval = _copyColumnSetting((CreateClientLogicColumn*)from);
            break;
        case T_ClientLogicColumnRef:
            retval = _copyEncryptedColumn((ClientLogicColumnRef*)from);
            break;
        case T_SortBy:
            retval = _copySortBy((SortBy*)from);
            break;
        case T_WindowDef:
            retval = _copyWindowDef((WindowDef*)from);
            break;
        case T_RangeSubselect:
            retval = _copyRangeSubselect((RangeSubselect*)from);
            break;
        case T_RangeFunction:
            retval = _copyRangeFunction((RangeFunction*)from);
            break;
        case T_RangeTableSample:
            retval = _copyRangeTableSample((RangeTableSample*)from);
            break;
        case T_RangeTimeCapsule:
            retval = CopyRangeTimeCapsule((RangeTimeCapsule*)from);
            break;
        case T_TypeName:
            retval = _copyTypeName((TypeName*)from);
            break;
        case T_IndexElem:
            retval = _copyIndexElem((IndexElem*)from);
            break;
        case T_ColumnDef:
            retval = _copyColumnDef((ColumnDef*)from);
            break;
        case T_Constraint:
            retval = _copyConstraint((Constraint*)from);
            break;
        case T_DefElem:
            retval = _copyDefElem((DefElem*)from);
            break;
        case T_LockingClause:
            retval = _copyLockingClause((LockingClause*)from);
            break;
        case T_RangeTblEntry:
            retval = _copyRangeTblEntry((RangeTblEntry*)from);
            break;
        case T_TableSampleClause:
            retval = _copyTableSampleClause((TableSampleClause*)from);
            break;
        case T_TimeCapsuleClause:
            retval = CopyTimeCapsuleClause((TimeCapsuleClause*)from);
            break;
        case T_SortGroupClause:
            retval = _copySortGroupClause((SortGroupClause*)from);
            break;
        case T_GroupingSet:
            retval = _copyGroupingSet((GroupingSet*)from);
            break;
        case T_WindowClause:
            retval = _copyWindowClause((WindowClause*)from);
            break;
        case T_RowMarkClause:
            retval = _copyRowMarkClause((RowMarkClause*)from);
            break;
        case T_WithClause:
            retval = _copyWithClause((WithClause*)from);
            break;
        case T_UpsertClause:
            retval = _copyUpsertClause((UpsertClause *)from);
            break;
        case T_CommonTableExpr:
            retval = _copyCommonTableExpr((CommonTableExpr*)from);
            break;
        case T_StartWithTargetRelInfo:
            retval = _copyStartWithTargetRelInfo((StartWithTargetRelInfo*)from);
            break;
        case T_StartWithOptions:
            retval = _copyStartWithOptions((StartWithOptions*)from);
            break;
        case T_Position:
            retval = _copyPosition((Position*)from);
            break;
        case T_PrivGrantee:
            retval = _copyPrivGrantee((PrivGrantee*)from);
            break;
        case T_FuncWithArgs:
            retval = _copyFuncWithArgs((FuncWithArgs*)from);
            break;
        case T_AccessPriv:
            retval = _copyAccessPriv((AccessPriv*)from);
            break;
        case T_DbPriv:
            retval = _copyDbPriv((DbPriv*)from);
            break;
        case T_XmlSerialize:
            retval = _copyXmlSerialize((XmlSerialize*)from);
            break;
#ifdef PGXC
        case T_DistributeBy:
            retval = _copyDistributeBy((DistributeBy*)from);
            break;
        case T_PGXCSubCluster:
            retval = _copyPGXCSubCluster((PGXCSubCluster*)from);
            break;
        case T_DistState:
            retval = _copyDistState((DistState*)from);
            break;
        case T_ListSliceDefState:
            retval = _copyListSliceDefState((ListSliceDefState*)from);
            break;
#endif
        case T_DistFdwDataNodeTask:
            retval = _copyDistFdwDataNodeTask((DistFdwDataNodeTask*)from);
            break;
        case T_DistFdwFileSegment:
            retval = _copyDistFdwFileSegment((DistFdwFileSegment*)from);
            break;
        case T_SplitInfo:
            retval = _copySplitInfo((SplitInfo*)from);
            break;
        case T_SplitMap:
            retval = _copySplitMap((SplitMap*)from);
            break;
        case T_DfsPrivateItem:
            retval = _copyDfsPrivateItem((DfsPrivateItem*)from);
            break;

        case T_ErrorCacheEntry:
            retval = _copyErrorCacheEntry((ErrorCacheEntry*)from);
            break;
        case T_InformationalConstraint:
            retval = _copyInformationalConstraint((InformationalConstraint*)from);
            break;
        case T_GroupingId:
            retval = _copyGroupingId((GroupingId*)from);
            break;
        case T_BloomFilterSet:
            retval = _copyBloomFilterSet((BloomFilterSet*)from);
            break;
        case T_AddTableIntoCBIState:
            retval = _copyAddTblIntoCBIState((AddTableIntoCBIState*)from);
            break;
            /*
             * PLAN HINT NODES
             */
        case T_HintState:
            retval = _copyHintState((HintState*)from);
            break;
        case T_JoinMethodHint:
            retval = _copyJoinHint((JoinMethodHint*)from);
            break;
        case T_RowsHint:
            retval = _copyRowsHint((RowsHint*)from);
            break;
        case T_StreamHint:
            retval = _copyStreamHint((StreamHint*)from);
            break;
        case T_LeadingHint:
            retval = _copyLeadingHint((LeadingHint*)from);
            break;
        case T_BlockNameHint:
            retval = _copyBlockNameHint((BlockNameHint*)from);
            break;
        case T_ScanMethodHint:
            retval = _copyScanMethodHint((ScanMethodHint*)from);
            break;
        case T_PgFdwRemoteInfo:
            retval = _copyPgFdwRemoteInfo((PgFdwRemoteInfo*)from);
            break;
        case T_SkewHint:
            retval = _copySkewHint((SkewHint*)from);
            break;
        case T_SkewHintTransf:
            retval = _copySkewHintTransf((SkewHintTransf*)from);
            break;
        case T_SkewRelInfo:
            retval = _copySkewRelInfo((SkewRelInfo*)from);
            break;
        case T_SkewColumnInfo:
            retval = _copySkewColumnInfo((SkewColumnInfo*)from);
            break;
        case T_SkewValueInfo:
            retval = _copySkewValueInfo((SkewValueInfo*)from);
            break;
        case T_CopyColExpr:
            retval = _copyCopyColExpr((CopyColExpr*)from);
            break;

            /* skew info */
        case T_QualSkewInfo:
            retval = _copyQualSkewInfo((QualSkewInfo*)from);
            break;
        case T_CreateDirectoryStmt:
            retval = _copyCreateDirectoryStmt((CreateDirectoryStmt*)from);
            break;
        case T_DropDirectoryStmt:
            retval = _copyDropDirectoryStmt((DropDirectoryStmt*)from);
            break;
        case T_PredpushHint:
            retval = _copyPredpushHint((PredpushHint*)from);
            break;
        case T_PredpushSameLevelHint:
            retval = _copyPredpushSameLevelHint((PredpushSameLevelHint*)from);
            break;
        case T_RewriteHint:
            retval = _copyRewriteHint((RewriteHint*)from);
            break;
        case T_GatherHint:
            retval = _copyGatherHint((GatherHint*)from);
            break;
        case T_NoExpandHint:
            retval = _copyNoExpandHint((NoExpandHint*)from);
            break;
        case T_SetHint:
            retval = _copySetHint((SetHint*)from);
            break;
        case T_PlanCacheHint:
            retval = _copyPlanCacheHint((PlanCacheHint*)from);
            break;
        case T_NoGPCHint:
            retval = _copyNoGPCHint((NoGPCHint*)from);
            break;

           /* shutdown */
        case T_ShutdownStmt:
            retval = _copyShutdownStmt((ShutdownStmt*)from);
            break;
        case T_SubPartitionPruningResult:
            retval = _copySubPartitionPruningResult((SubPartitionPruningResult*)from);
            break;

        case T_CreatePublicationStmt:
            retval = _copyCreatePublicationStmt((CreatePublicationStmt *)from);
            break;
        case T_AlterPublicationStmt:
            retval = _copyAlterPublicationStmt((AlterPublicationStmt *)from);
            break;
        case T_CreateSubscriptionStmt:
            retval = _copyCreateSubscriptionStmt((CreateSubscriptionStmt *)from);
            break;
        case T_AlterSubscriptionStmt:
            retval = _copyAlterSubscriptionStmt((AlterSubscriptionStmt *)from);
            break;
        case T_PredictByFunction:
            retval = _copyPredictByFunctionStmt((PredictByFunction *)from);
            break;
        case T_DropSubscriptionStmt:
            retval = _copyDropSubscriptionStmt((DropSubscriptionStmt *)from);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(from))));
            retval = 0; /* keep compiler quiet */
            break;
    }

    return retval;
}

/*
 * CopyMemInfoFields -
 *	   copy OpMemInfo structure from "from" node to "newnode"
 */
static void CopyMemInfoFields(const OpMemInfo* from, OpMemInfo* newnode)
{
    COPY_SCALAR_FIELD(opMem);
    COPY_SCALAR_FIELD(minMem);
    COPY_SCALAR_FIELD(maxMem);
    COPY_SCALAR_FIELD(regressCost);
}

static ReplicaIdentityStmt* _copyReplicaIdentityStmt(const ReplicaIdentityStmt* from)
{
    ReplicaIdentityStmt* newnode = makeNode(ReplicaIdentityStmt);

    COPY_SCALAR_FIELD(identity_type);
    COPY_STRING_FIELD(name);

    return newnode;
}

#ifndef ENABLE_MULTIPLE_NODES
static AlterSystemStmt* _copyAlterSystemStmt(const AlterSystemStmt* from)
{
    AlterSystemStmt* newnode = makeNode(AlterSystemStmt);
    COPY_NODE_FIELD(setstmt);
    return newnode;
}
#endif

/*
 * CopyCursorFields -
 *	   copy Cursor_Data structure from "from" node to "newnode"
 */
static void CopyCursorFields(const Cursor_Data* from, Cursor_Data* newnode)
{
    COPY_SCALAR_FIELD(row_count);
    COPY_SCALAR_FIELD(cur_dno);
    COPY_SCALAR_FIELD(is_open);
    COPY_SCALAR_FIELD(found);
    COPY_SCALAR_FIELD(not_found);
    COPY_SCALAR_FIELD(null_open);
    COPY_SCALAR_FIELD(null_fetch);
}

#else /* FRONTEND_PARSER */

/*
 * copyObject
 *
 * Create a copy of a Node tree or list.  This is a "deep" copy: all
 * substructure is copied too, recursively.
 */
void *copyObject(const void *from)
{
    void *retval = NULL;

    if (from == NULL) {
        return NULL;
    }
    /* Guard against stack overflow due to overly complex expressions */
    switch (nodeTag(from)) {
        case T_TypeName:
            retval = _copyTypeName((TypeName*)from);
            break;
        default:
            retval = 0; /* keep compiler quiet */
            break;
    }
    return retval;
}
#endif /* FRONTEND_PARSER */
