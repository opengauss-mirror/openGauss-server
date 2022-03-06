/* -------------------------------------------------------------------------
 *
 * costsize.cpp
 *	  Routines to compute (and set) relation sizes and path costs
 *
 * Path costs are measured in arbitrary units established by these basic
 * parameters:
 *
 *	seq_page_cost		Cost of a sequential page fetch
 *	random_page_cost	Cost of a non-sequential page fetch
 *	cpu_tuple_cost		Cost of typical CPU time to process a tuple
 *	cpu_index_tuple_cost  Cost of typical CPU time to process an index tuple
 *	cpu_operator_cost	Cost of CPU time to execute an operator or function
 *
 * We expect that the kernel will typically do some amount of read-ahead
 * optimization; this in conjunction with seek costs means that seq_page_cost
 * is normally considerably less than random_page_cost.  (However, if the
 * database is fully cached in RAM, it is reasonable to set them equal.)
 *
 * We also use a rough estimate "g_instance.cost_cxt.effective_cache_size" of the number of
 * disk pages in openGauss + OS-level disk cache.  (We can't simply use
 * NBuffers for this purpose because that would ignore the effects of
 * the kernel's disk cache.)
 *
 * Obviously, taking constants for these values is an oversimplification,
 * but it's tough enough to get any useful estimates even at this level of
 * detail.	Note that all of these parameters are user-settable, in case
 * the default values are drastically off for a particular platform.
 *
 * seq_page_cost and random_page_cost can also be overridden for an individual
 * tablespace, in case some data is on a fast disk and other data is on a slow
 * disk.  Per-tablespace overrides never apply to temporary work files such as
 * an external sort or a materialize node that overflows work_mem.
 *
 * We compute two separate costs for each path:
 *		total_cost: total estimated cost to fetch all tuples
 *		startup_cost: cost that is expended before first tuple is fetched
 * In some scenarios, such as when there is a LIMIT or we are implementing
 * an EXISTS(...) sub-select, it is not necessary to fetch all tuples of the
 * path's result.  A caller can estimate the cost of fetching a partial
 * result by interpolating between startup_cost and total_cost.  In detail:
 *		actual_cost = startup_cost +
 *			(total_cost - startup_cost) * tuples_to_fetch / path->rows;
 * Note that a base relation's rows count (and, by extension, plan_rows for
 * plan nodes below the LIMIT node) are set without regard to any LIMIT, so
 * that this equation works properly.  (Also, these routines guarantee not to
 * set the rows count to zero, so there will be no zero divide.)  The LIMIT is
 * applied as a top-level plan node.
 *
 * For largely historical reasons, most of the routines in this module use
 * the passed result Path only to store their results (rows, startup_cost and
 * total_cost) into.  All the input data they need is passed as separate
 * parameters, even though much of it could be extracted from the Path.
 * An exception is made for the cost_XXXjoin() routines, which expect all
 * the other fields of the passed XXXPath to be filled in, and similarly
 * cost_index() assumes the passed IndexPath is valid except for its output
 * values.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/costsize.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/node/nodeHash.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/bucketpruning.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "utils/hll_mpp.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "utils/tuplesort.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "vectorsonic/vsonichash.h"
#include "pgxc/pgxc.h"

/* The default value of the column row width */
#define COL_TUPLE_WIDTH 30

typedef struct {
    PlannerInfo* root;
    QualCost total;
} cost_qual_eval_context;

/* Identify the max global rows of joinrel if have over estimate. */
#define JOINREL_MAX_GLOBAL_ROWS (double)(1.0e11)

static bool cost_qual_eval_walker(Node* node, cost_qual_eval_context* context);
static void get_restriction_qual_cost(
    PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info, QualCost* qpqual_cost);
static double calc_joinrel_size_estimate(PlannerInfo* root, double outer_rows, double inner_rows,
    SpecialJoinInfo* sjinfo, List* restrictlist, bool varratio_cached);
static int calc_distributekey_width(Path* path, int* width, bool vectorized, bool aligned);
static Cost get_subqueryscan_stream_cost(Plan* subplan);
static bool is_predpush_dest(PlannerInfo* root, Relids indexes);
static bool enable_parametrized_path(PlannerInfo* root, RelOptInfo* baserel, Path* path);

extern bool isExprSonicEnable(Expr* node);
extern bool isAggrefSonicEnable(Oid aggfnoid);

/*
 * init_plan_cost
 *     used to initialize a plan's cost related field
 *
 * @param (in) plan:
 *     the plan to be handled
 *
 * @return: void
 */
void init_plan_cost(Plan* plan)
{
    plan->startup_cost = 0.0;
    plan->total_cost = 0.0;
    plan->multiple = 1.0;
    plan->plan_rows = 0.0;
    plan->plan_width = 0;
    plan->innerdistinct = 1.0;
    plan->outerdistinct = 1.0;
    plan->pred_rows = -1.0;
    plan->pred_startup_time = -1.0;
    plan->pred_total_time = -1.0;
    plan->pred_max_memory = -1;
}

static inline void get_info_from_rel(
    Relation relation, int* maxBatchRow, bool* isPartTable, bool* isValuePartTable, int* partialClusterRows)
{
    *maxBatchRow = RelationGetMaxBatchRows(relation);
    *isPartTable = RELATION_IS_PARTITIONED(relation);
    *isValuePartTable = RELATION_IS_VALUE_PARTITIONED(relation);
    *partialClusterRows = RelationGetPartialClusterRows(relation);
}

/*
 * Description: estimation the memory info for cstoreinsert and dfsinsert .
 *
 * For dfs insert : maxMem = insert memory + sort memory(if has pck). If the table is value partition,
 * we set 2G memory for all dynamic partitions to use. If the table is not partition table,we shoule used
 * 128MB to promise write disk.
 *
 * For cstore insert : maxMem = insert memory + sort memory(if has pck).  insetMem = maxBatchrow * column *3.
 * the means of 3 is that the insert will need three part memory for batchinsert\beforecompress\aftercompress.
 * sortMem =  partialClusterRows(tuples) * column. Default partialClusterRows is 420w. if tuples number is less than
 * 420w, the true values(tuples number) is used to caculate the memory. 1) pck+partition: insetMem is set to 2g and sort
 * memory is estimated to 4g. 2) partition: insetMem is maxBatchrow * column *3. insetMem<=2g. Default maxBatchrow is 6w.
 * 3) pck+ table: insertMem is maxBatchrow * column*3. sortMem is partialClusterRows(tuples) * column.
 * 4) table: insertMem is the total memory, is maxBatchrow * column*3
 *
 * Parameters:
 *	@in path: the path for insert.
 *	@in root: plannerinfo struct for current query level.
 *	@in input_cost: is the total cost for reading the input data.
 *   @in tuples: is the number of tuples in the relation.
 *	@in width: is the average tuple width in bytes.
 *	@in comparison_cost: is the extra cost per comparison, if any.
 *	@in modify_mem: is the number of kilobytes of work memory allowed for the sort.
 *   @in dop: set the dop.
 *	@in resultRelOid: the relation to be scanned.
 *   @in isDfsStore: judge whether table is dfs table.
 *	@in mem_info: is operator max and min info used by memory control module.
 *   Return: void
 */
void cost_insert(Path* path, bool vectorized, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int modify_mem, int dop, Oid resultRelOid, bool isDfsStore, OpMemInfo* mem_info)
{
    Cost startup_cost = input_cost;
    Cost run_cost = 0;
    double input_bytes = relation_byte_size(tuples, width, vectorized) / SET_DOP(dop);
    double output_bytes = 0;
    double output_bytes_insert = 0;
    double output_bytes_pck = 0;
    long modify_mem_bytes = modify_mem * 1024L / SET_DOP(dop);
    /* isPartTable is judge whether is range partition. isValuePartTable is judge whether is value partition */
    bool isPartTable = false;
    bool isValuePartTable = false;
    bool hasPck = false;
    Relation relation;
    int maxBatchRow = MAX_BATCH_ROWS;
    int partialClusterRows = PARTIAL_CLUSTER_ROWS;
    double sortRows = 1;

    /* We should compute the table's partition num and maxBatchRow and pck and index information. */
    if (resultRelOid) {
        relation = relation_open(resultRelOid, AccessShareLock);
        get_info_from_rel(relation, &maxBatchRow, &isPartTable, &isValuePartTable, &partialClusterRows);
        if (relation->rd_rel->relhasclusterkey) {
            hasPck = true;
        }
        relation_close(relation, NoLock);
    }
    /*
     * We want to be sure the cost of a sort is never estimated as zero, even
     * if passed-in tuple count is zero.  Besides, mustn't do log(0)...
     */
    if (tuples < 2.0) {
        tuples = 2.0;
    }

    /* Include the default cost-per-comparison */
    comparison_cost += 2.0 * u_sess->attr.attr_sql.cpu_operator_cost;

    /* if dfs table for insert, the memory is 128MB. If cstore table for insert, the memory is maxBatchRow*width. */
    if (isDfsStore)
        output_bytes_insert = DFS_MIN_MEM_SIZE * MEM_KB;
    else
        output_bytes_insert = relation_byte_size(maxBatchRow, width, vectorized) * 3;

    if (output_bytes_insert > modify_mem_bytes) {
        /* CPU costs : Assume about N log2 N comparisons */
        startup_cost += comparison_cost * tuples * LOG2(tuples);
        /* Disk costs */
        startup_cost += compute_sort_disk_cost(input_bytes, modify_mem_bytes);
    } else {
        if (tuples > 2 * maxBatchRow || input_bytes > modify_mem_bytes) {
            /*
             * We'll use a bounded heap-sort keeping just K tuples in memory, for
             * a total number of tuple comparisons of N log2 K; but the constant
             * factor is a bit higher than for quicksort.  Tweak it so that the
             * cost curve is continuous at the crossover point.
             */
            startup_cost += comparison_cost * tuples * LOG2(2.0 * maxBatchRow);
        } else {
            /* We'll use plain quicksort on all the input tuples */
            startup_cost += comparison_cost * tuples * LOG2(tuples);
        }
    }

    if (isDfsStore) {
        sortRows = tuples > partialClusterRows ? partialClusterRows : tuples;
        if (mem_info != NULL) {
            mem_info->opMem = modify_mem;
            if (hasPck && isValuePartTable) {
                output_bytes_insert = PARTITION_MAX_SIZE * MEM_KB;
                output_bytes_pck = relation_byte_size(sortRows, width, vectorized);
                mem_info->maxMem = output_bytes_pck / MEM_KB + output_bytes_insert / MEM_KB;
                mem_info->minMem = mem_info->maxMem;
            } else if (!hasPck && isValuePartTable) {
                output_bytes = PARTITION_MAX_SIZE * MEM_KB;
                mem_info->maxMem = output_bytes / MEM_KB;
                mem_info->minMem = mem_info->maxMem;
            } else if (hasPck && !isValuePartTable) {
                output_bytes_pck = relation_byte_size(sortRows, width, vectorized);
                mem_info->maxMem = (output_bytes_insert + output_bytes_pck) / MEM_KB;
                mem_info->minMem = output_bytes_insert / MEM_KB + output_bytes_pck / MEM_KB / SORT_MAX_DISK_SIZE;
            } else {
                mem_info->maxMem = output_bytes_insert / MEM_KB;
                mem_info->minMem = output_bytes_insert / MEM_KB;
            }
            mem_info->regressCost = compute_sort_disk_cost(input_bytes, mem_info->minMem);
            MEMCTL_LOG(DEBUG2,
                "DFS INSERT:The opMem is: %lfKB, the maxMem is: %lfKB, the minMem is: %lfKB",
                mem_info->opMem,
                mem_info->maxMem,
                mem_info->minMem);
        }
    } else {
        /* calucate the mem_info for partition\ partition_pck,cstoretable\cstoretable_pck.*/
        if (mem_info != NULL) {
            mem_info->opMem = modify_mem;
            if (hasPck && isPartTable) {
                /* we will need 2g memory to insert ,4g memory to sort. NOTICE : PARTITION_MAX_SIZE is KB */
                output_bytes_pck = PARTITION_MAX_SIZE * MEM_KB * 2;
                output_bytes_insert = PARTITION_MAX_SIZE * MEM_KB;
                mem_info->maxMem = (output_bytes_pck + output_bytes_insert) / MEM_KB;
            } else if (!hasPck && isPartTable) {
                output_bytes = output_bytes_insert;
                double output_k_bytes = output_bytes / MEM_KB;
                mem_info->maxMem = (output_k_bytes > PARTITION_MAX_SIZE) ? output_k_bytes : PARTITION_MAX_SIZE;
            } else if (hasPck && !isPartTable) {
                output_bytes_pck = relation_byte_size(partialClusterRows, width, vectorized);
                output_bytes = output_bytes_pck + output_bytes_insert;
                mem_info->maxMem = output_bytes / MEM_KB;
            } else {
                output_bytes = output_bytes_insert;
                mem_info->maxMem = output_bytes / MEM_KB;
            }

            mem_info->minMem = mem_info->maxMem / SORT_MAX_DISK_SIZE;
            mem_info->regressCost = compute_sort_disk_cost(input_bytes, mem_info->minMem);
            MEMCTL_LOG(DEBUG2,
                "CSTORE INSERT:The opMem is: %lfKB, the maxMem is: %lfKB, the minMem is: %lfKB",
                mem_info->opMem,
                mem_info->maxMem,
                mem_info->minMem);
        }
    }

    /*
     * Also charge a small amount (arbitrarily set equal to operator cost) per
     * extracted tuple.  We don't charge cpu_tuple_cost because a Sort node
     * doesn't do qual-checking or projection, so it has less overhead than
     * most plan nodes.  Note it's correct to use tuples not output_tuples
     * here --- the upper LIMIT will pro-rate the run cost so we'd be double
     * counting the LIMIT otherwise.
     */
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * tuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}

/*
 * Description: estimation the memory info for cstoredelete and dfsdelete .
 *
 * For delete : maxMem = delete mem(for sort). It is same between cstoredelete and dfsdelete .
 * sortMem =  partialClusterRows(tuples) * column. If the mem is less than 16MB,maxMem >=16MB.
 * Default partialClusterRows is 420w. if tuples number is less than 420w, the true values(tuples number)
 * is used to caculate the memory.
 *
 * Parameters:
 *	@in path: the path for delete.
 *	@in root: plannerinfo struct for current query level.
 *	@in input_cost: is the total cost for reading the input data.
 *   @in tuples: is the number of tuples in the relation.
 *	@in width: is the average tuple width in bytes.
 *	@in comparison_cost: is the extra cost per comparison, if any.
 *	@in modify_mem: is the number of kilobytes of work memory allowed for the sort.
 *   @in dop: set the dop.
 *	@in resultRelOid: the relation to be scanned.
 *   @in isDfsStore: judge whether table is dfs table.
 *	@in mem_info: is operator max and min info used by memory control module.
 *   Return: void
 */
void cost_delete(Path* path, bool vectorized, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int modify_mem, int dop, Oid resultRelOid, bool isDfsStore, OpMemInfo* mem_info)
{
    Cost startup_cost = input_cost;
    Cost run_cost = 0;
    double input_bytes = relation_byte_size(tuples, width, vectorized) / SET_DOP(dop);
    double output_bytes = 0;
    double output_tuples = 0;
    long modify_mem_bytes = modify_mem * 1024L / SET_DOP(dop);
    Relation relation;
    int partialClusterRows = PARTIAL_CLUSTER_ROWS;

    if (resultRelOid) {
        relation = relation_open(resultRelOid, AccessShareLock);
        partialClusterRows = RelationGetPartialClusterRows(relation);
        relation_close(relation, NoLock);
    }

    /*
     * We want to be sure the cost of a sort is never estimated as zero, even
     * if passed-in tuple count is zero.  Besides, mustn't do log(0)...
     */
    if (tuples < 2.0) {
        tuples = 2.0;
    }

    /* Include the default cost-per-comparison */
    comparison_cost += 2.0 * u_sess->attr.attr_sql.cpu_operator_cost;
    output_tuples = tuples > partialClusterRows ? partialClusterRows : tuples;
    output_bytes = relation_byte_size(output_tuples, width, vectorized);
    if (output_bytes > modify_mem_bytes) {
        /* CPU costs : Assume about N log2 N comparisons */
        startup_cost += comparison_cost * tuples * LOG2(tuples);
        /* Disk costs */
        startup_cost += compute_sort_disk_cost(input_bytes, modify_mem_bytes);
    } else {
        if (tuples > 2 * output_tuples || input_bytes > modify_mem_bytes) {
            /*
             * We'll use a bounded heap-sort keeping just K tuples in memory, for
             * a total number of tuple comparisons of N log2 K; but the constant
             * factor is a bit higher than for quicksort.  Tweak it so that the
             * cost curve is continuous at the crossover point.
             */
            startup_cost += comparison_cost * tuples * LOG2(2.0 * output_tuples);
        } else {
            /* We'll use plain quicksort on all the input tuples */
            startup_cost += comparison_cost * tuples * LOG2(tuples);
        }
    }

    /*
     * calucate the mem_info for cstore table or dfs table.
     */
    if (mem_info != NULL) {
        mem_info->opMem = modify_mem;
        mem_info->maxMem = output_bytes / MEM_KB > SORT_MIM_MEM ? output_bytes / MEM_KB : SORT_MIM_MEM;
        mem_info->minMem = mem_info->maxMem / SORT_MAX_DISK_SIZE;
        mem_info->regressCost = compute_sort_disk_cost(input_bytes, mem_info->minMem);
        MEMCTL_LOG(DEBUG2,
            "MEMORY DELETE:The opMem is : %lfKB, the maxMem is :%lfKB, the minMem is :%lfKB",
            mem_info->opMem,
            mem_info->maxMem,
            mem_info->minMem);
    }

    /*
     * Also charge a small amount (arbitrarily set equal to operator cost) per
     * extracted tuple.  We don't charge cpu_tuple_cost because a Sort node
     * doesn't do qual-checking or projection, so it has less overhead than
     * most plan nodes.  Note it's correct to use tuples not output_tuples
     * here --- the upper LIMIT will pro-rate the run cost so we'd be double
     * counting the LIMIT otherwise.
     */
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * tuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}

/*
 * Description: estimation the memory info for cstoreupdate and dfsupdate . update = delete +insert(insert + sort).
 * So, we should caculate the delete mem(sort mem) and insert mem(insert and sort). Here, the deleted
 * memory can be reused for the pck sort memory when insert, so regardless of whether there is a pck,
 * we need to calculate the memory required by sort. So, update mem = sortMem(delete) + insertMem.
 *
 * For dfs update : maxMem = insert memory + sort memory. For insertMem: If the table is value partition,
 * we set 2G memory for all dynamic partitions to use. If the table is not partition table,we shoule used
 * 128MB to promise write disk.  For sortMem: partialClusterRows(tuples) * column. It is important to
 * note that the minimum value needs to >= 128MB.
 *
 * For cstore update : maxMem = insert memory + sort memory.  insetMem = maxBatchrow * column *3.
 * the means of 3 is that the insert will need three part memory for batchinsert\beforecompress\aftercompress.
 * sortMem =  partialClusterRows(tuples) * column. Default partialClusterRows is 420w. if tuples number is less than
 * 420w, the true values(tuples number) is used to caculate the memory. 1) partition: insetMem is set to 2g and sort
 * memory is estimated to 4g. 2) table: insertMem is maxBatchrow * column*3. sortMem is partialClusterRows(tuples) *
 * column.
 *
 * Parameters:
 *	@in path: the path for update.
 *	@in root: plannerinfo struct for current query level.
 *	@in input_cost: is the total cost for reading the input data.
 *   @in tuples: is the number of tuples in the relation.
 *	@in width: is the average tuple width in bytes.
 *	@in comparison_cost: is the extra cost per comparison, if any.
 *	@in modify_mem: is the number of kilobytes of work memory allowed for the sort.
 *   @in dop: set the dop.
 *	@in resultRelOid: the relation to be scanned.
 *   @in isDfsStore: judge whether table is dfs table.
 *	@in mem_info: is operator max and min info used by memory control module.
 *   Return: void
 */
void cost_update(Path* path, bool vectorized, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int modify_mem, int dop, Oid resultRelOid, bool isDfsStore, OpMemInfo* mem_info)
{
    Cost startup_cost = input_cost;
    Cost run_cost = 0;
    double input_bytes = relation_byte_size(tuples, width, vectorized) / SET_DOP(dop);
    double output_bytes = 0;
    double output_bytes_insert = 0;
    double output_bytes_pck = 0;
    long modify_mem_bytes = modify_mem * 1024L / SET_DOP(dop);
    bool isPartTable = false;
    bool isValuePartTable = false;
    bool hasPck = false;
    Relation relation;
    int maxBatchRow = MAX_BATCH_ROWS;
    int partialClusterRows = PARTIAL_CLUSTER_ROWS;

    /* We should compute the table's partition num and maxBatchRow and pck and index information. */
    if (resultRelOid) {
        relation = relation_open(resultRelOid, AccessShareLock);
        get_info_from_rel(relation, &maxBatchRow, &isPartTable, &isValuePartTable, &partialClusterRows);
        if (relation->rd_rel->relhasclusterkey) {
            hasPck = true;
        }
        relation_close(relation, NoLock);
    }

    /*
     * We want to be sure the cost of a sort is never estimated as zero, even
     * if passed-in tuple count is zero.  Besides, mustn't do log(0)...
     */
    if (tuples < 2.0) {
        tuples = 2.0;
    }

    /* Include the default cost-per-comparison */
    comparison_cost += 2.0 * u_sess->attr.attr_sql.cpu_operator_cost;

    /* If dfs table for insert, the memory is 128MB.
     * If dfs part table for insert, the memory is 2GB.
     * If cstore table for insert, the memory is maxBatchRow*width.
     */
    if (isDfsStore)
        output_bytes_insert = isValuePartTable ? PARTITION_MAX_SIZE * MEM_KB : DFS_MIN_MEM_SIZE * MEM_KB;
    else
        output_bytes_insert = relation_byte_size(maxBatchRow, width, vectorized) * 3;

    if (output_bytes_insert > modify_mem_bytes) {
        /* CPU costs : Assume about N log2 N comparisons */
        startup_cost += comparison_cost * tuples * LOG2(tuples);
        /* Disk costs */
        startup_cost += compute_sort_disk_cost(input_bytes, modify_mem_bytes);
    } else {
        if (tuples > 2 * maxBatchRow || input_bytes > modify_mem_bytes) {
            /*
             * We'll use a bounded heap-sort keeping just K tuples in memory, for
             * a total number of tuple comparisons of N log2 K; but the constant
             * factor is a bit higher than for quicksort.  Tweak it so that the
             * cost curve is continuous at the crossover point.
             */
            startup_cost += comparison_cost * tuples * LOG2(2.0 * maxBatchRow);
        } else {
            /* We'll use plain quicksort on all the input tuples */
            startup_cost += comparison_cost * tuples * LOG2(tuples);
        }
    }

    /*
     * calucate the mem_info for partition\ cstoretable. delete memory + insert memory.
     * delete sort will be reused to insert sort.
     */
    if (isDfsStore) {
        if (mem_info != NULL) {
            mem_info->opMem = modify_mem;
            output_bytes_pck = relation_byte_size(partialClusterRows, width, vectorized);
            output_bytes = output_bytes_pck + output_bytes_insert;
            mem_info->maxMem = output_bytes / MEM_KB;
            mem_info->minMem = output_bytes_insert / MEM_KB + output_bytes_pck / SORT_MAX_DISK_SIZE / MEM_KB;
            mem_info->regressCost = compute_sort_disk_cost(input_bytes, mem_info->minMem);
            MEMCTL_LOG(DEBUG2,
                "DFS UPDATE:The opMem is: %lfKB, the maxMem is: %lfKB, the minMem is: %lfKB",
                mem_info->opMem,
                mem_info->maxMem,
                mem_info->minMem);
        }
    } else {
        if (mem_info != NULL) {
            mem_info->opMem = modify_mem;
            if (isPartTable) {
                /* We will need 2g memory to insert ,4g memory to sort.*/
                output_bytes_pck = PARTITION_MAX_SIZE * MEM_KB * 2;
                output_bytes_insert = PARTITION_MAX_SIZE * MEM_KB;
                mem_info->maxMem = (output_bytes_pck + output_bytes_insert) / MEM_KB;
                mem_info->minMem = output_bytes_insert / MEM_KB + output_bytes_pck / SORT_MAX_DISK_SIZE / MEM_KB;
                MEMCTL_LOG(DEBUG2,
                    "CSTORE PART TABLE UPDATE:The opMem is: %lfKB, the maxMem is: %lfKB, the minMem is: %lfKB",
                    mem_info->opMem,
                    mem_info->maxMem,
                    mem_info->minMem);
            } else {
                output_bytes_pck = relation_byte_size(partialClusterRows, width, vectorized);
                output_bytes = output_bytes_pck + output_bytes_insert;
                mem_info->maxMem = output_bytes / MEM_KB;
                mem_info->minMem = output_bytes_insert / MEM_KB + output_bytes_pck / SORT_MAX_DISK_SIZE / MEM_KB;
                MEMCTL_LOG(DEBUG2,
                    "CSTORE TABLE UPDATE:The opMem is: %lfKB, the maxMem is: %lfKB, the minMem is: %lfKB",
                    mem_info->opMem,
                    mem_info->maxMem,
                    mem_info->minMem);
            }

            mem_info->regressCost = compute_sort_disk_cost(input_bytes, mem_info->minMem);
        }
    }

    /*
     * Also charge a small amount (arbitrarily set equal to operator cost) per
     * extracted tuple.  We don't charge cpu_tuple_cost because a Sort node
     * doesn't do qual-checking or projection, so it has less overhead than
     * most plan nodes.  Note it's correct to use tuples not output_tuples
     * here --- the upper LIMIT will pro-rate the run cost so we'd be double
     * counting the LIMIT otherwise.
     */
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * tuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}

/*
 * clamp_row_est
 *		Force a row-count estimate to a sane value.
 */
double clamp_row_est(double nrows)
{
    /*
     * Force estimate to be at least one row, to make explain output look
     * better and to avoid possible divide-by-zero when interpolating costs.
     * Make it an integer, too.
     */
    if (nrows <= 1.0) {
        nrows = 1.0;
    } else {
        nrows = rint(nrows);
    }

    return nrows;
}

/* Set local and global rows for sort/agg/group by/material/windowfun path for lower path. */
void set_path_rows(Path* path, double rows, double multiple)
{
    path->rows = rows;
    path->multiple = multiple;
}

/* Set local and global rows for baserel or joinrel path. */
void set_rel_path_rows(Path* path, RelOptInfo* rel, ParamPathInfo* param_info)
{
    /* Mark the path with the correct row estimate */
    if (param_info != NULL)
        set_path_rows(path, param_info->ppi_rows);
    else
        set_path_rows(path, rel->rows, rel->multiple);
}

/*
 * Set the real path rows after parallel.
 *
 * @in_param path: the path need to be corrected.
 */
static void set_parallel_path_rows(Path* path)
{
    int dop = SET_DOP(path->dop);

    /*
     * When we parallel replicate path, the return rows in one node
     * and global will both increase. This can be useful when we parallel
     * join inner path.
     */
    if (is_replicated_path(path)) {
        path->rows *= dop;
    }
}

/*
 * cost_resultscan
 *       Determines and returns the cost of scanning an RTE_RESULT relation.
 */
void cost_resultscan(Path *path, PlannerInfo *root,
    RelOptInfo *baserel, ParamPathInfo *param_info)
{
    Cost            startup_cost = 0;
    Cost            run_cost = 0;
    QualCost        qpqual_cost;
    Cost            cpu_per_tuple;

    /* Should only be applied to RTE_RESULT base relations */
    Assert(baserel->relid > 0);
    Assert(baserel->rtekind == RTE_RESULT);

    /* Mark the path with the correct row estimate */
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    /* We charge qual cost plus cpu_tuple_cost */
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = DEFAULT_CPU_TUPLE_COST + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * baserel->tuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}

/*
 * cost_seqscan
 *	  Determines and returns the cost of scanning a relation sequentially.
 *	  The pruning ration for Partitioned table will be considered in set_plain_rel_size().
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
void cost_seqscan(Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double spc_seq_page_cost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple = 0.0;
    int dop = SET_DOP(path->dop);
    bool disable_path = enable_parametrized_path(root, baserel, (Path*)path);

    /* Should only be applied to base relations */
    Assert(baserel->relid > 0);
    Assert(baserel->rtekind == RTE_RELATION);

    /* Mark the path with the correct row estimate */
    set_rel_path_rows(path, baserel, param_info);
    set_parallel_path_rows(path);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(baserel->reltablespace, NULL, &spc_seq_page_cost);
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);

    startup_cost += qpqual_cost.startup;
    if (!u_sess->attr.attr_sql.enable_seqscan || disable_path)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /*
     * When we parallel the scan node, then the disk costs and cpu costs
     * wiil be equal division to all parallelism thread.
     */
    run_cost += u_sess->opt_cxt.smp_thread_cost * (dop - 1);
    run_cost += spc_seq_page_cost * baserel->pages / dop;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples) / dop;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (!u_sess->attr.attr_sql.enable_seqscan || disable_path)
        path->total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
}

/*
 * Description: Determines and returns the cost of scanning a relation using sampling.
 *
 * Parameters:
 *	@in path: seqscan or cstorescan path.
 *	@in root: plannerinfo struct for current query level.
 *	@in baserel: the relation to be scanned.
 *	@in param_info: the ParamPathInfo if this is a parameterized path, else NULL
 *
 * Return: void
 */
void cost_samplescan(Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    RangeTblEntry* rte = NULL;
    TableSampleClause* tsc = NULL;
    double spc_seq_page_cost, spc_random_page_cost, spc_page_cost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple = 0.0;

    /* Should only be applied to base relations with tablesample clauses */
    AssertEreport(baserel->relid > 0,
        MOD_OPT,
        "The relid is invalid when determining the cost of scanning a relation using sampling.");
    rte = planner_rt_fetch(baserel->relid, root);
    AssertEreport(rte->rtekind == RTE_RELATION,
        MOD_OPT,
        "Only base relation can be supported when determining the cost of scanning a relation using sampling.");
    tsc = rte->tablesample;
    AssertEreport(tsc != NULL,
        MOD_OPT,
        "Samling method and parameters is null when determining the cost of scanning a relation using sampling.");

    set_rel_path_rows(path, baserel, param_info);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(baserel->reltablespace, &spc_random_page_cost, &spc_seq_page_cost);

    /* if sampleType is SYSTEM_SAMPLE, assume random access, else sequential */
    spc_page_cost = (tsc->sampleType == BERNOULLI_SAMPLE) ? spc_seq_page_cost : spc_random_page_cost;

    /*
     * disk costs (recall that baserel->pages has already been set to the
     * number of pages the sampling method will visit)
     */
    run_cost += spc_page_cost * baserel->pages;

    /*
     * CPU costs (recall that baserel->tuples has already been set to the
     * number of tuples the sampling method will select).  Note that we ignore
     * execution cost of the TABLESAMPLE parameter expressions; they will be
     * evaluated only once per scan, and in most usages they'll likely be
     * simple constants anyway.  We also don't charge anything for the
     * calculations the sampling method might do internally.
     */
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);

    startup_cost += qpqual_cost.startup;

    if (baserel->orientation == REL_COL_ORIENTED) {
        cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost / COL_TUPLE_COST_MULTIPLIER + qpqual_cost.per_tuple;
    } else if (baserel->orientation == REL_TIMESERIES_ORIENTED) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Unsupported Using Index FOR TIMESERIES.")));
    } else {
        cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qpqual_cost.per_tuple;
    }

    run_cost += cpu_per_tuple * baserel->tuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;
}

/*
 * cost_cstorescan
 *	  Determines and returns the cost of scanning a column store.
 *	  The pruning ration for Partitioned table will be considered in set_plain_rel_size().
 */
void cost_cstorescan(Path* path, PlannerInfo* root, RelOptInfo* baserel)
{
    double spc_seq_page_cost;
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost cpu_per_tuple = 0.0;
    int dop = SET_DOP(path->dop);

    /* Should only be applied to base relations */
    Assert(baserel->relid > 0 && baserel->rtekind == RTE_RELATION);

    set_rel_path_rows(path, baserel, NULL);
    set_parallel_path_rows(path);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(baserel->reltablespace, NULL, &spc_seq_page_cost);

    startup_cost += baserel->baserestrictcost.startup;

    if (!u_sess->attr.attr_sql.enable_seqscan)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /*
     * When we parallel the scan node, then the disk costs and cpu costs
     * wiil be equal division to all parallelism thread.
     */
    run_cost += u_sess->opt_cxt.smp_thread_cost * (dop - 1);
    run_cost += spc_seq_page_cost * baserel->pages / dop;
    cpu_per_tuple =
        u_sess->attr.attr_sql.cpu_tuple_cost / COL_TUPLE_COST_MULTIPLIER + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples) / dop;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (!u_sess->attr.attr_sql.enable_seqscan)
        path->total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
}

/*
 * Determines and returns the cost of scanning a DFS relation.
 * path:    The scan path.
 * root:    The PlannerInfo struct.
 * baserel: The relation to be scanned.
 */
void cost_dfsscan(Path* path, PlannerInfo* root, RelOptInfo* baserel)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double spc_seq_page_cost;
    Cost cpu_per_tuple = 0.0;
    int dop = SET_DOP(path->dop);

    /*
     * Should only be applied to base relations.
     */
    AssertEreport(
        baserel->relid > 0, MOD_OPT, "The relid is invalid when determining the cost of scanning a DFS relation.");
    AssertEreport(baserel->rtekind == RTE_RELATION,
        MOD_OPT,
        "Only base relation can be supported when determining the cost of scanning a DFS relation.");

    set_rel_path_rows(path, baserel, NULL);
    set_parallel_path_rows(path);

    /*
     * Fetch estimated page cost for tablespace containing table.
     */
    get_tablespace_page_costs(baserel->reltablespace, NULL, &spc_seq_page_cost);

    startup_cost = baserel->baserestrictcost.startup;
    /* Parallelize start up cost. */
    if (!u_sess->attr.attr_sql.enable_seqscan)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /*
     * When we parallel the scan node, then the disk costs and cpu costs
     * wiil be equal division to all parallelism thread.
     */
    run_cost += u_sess->opt_cxt.smp_thread_cost * (dop - 1);
    cpu_per_tuple =
        u_sess->attr.attr_sql.cpu_tuple_cost / COL_TUPLE_COST_MULTIPLIER + baserel->baserestrictcost.per_tuple;

    run_cost += (cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples)) / dop +
                (spc_seq_page_cost * baserel->pages) / dop;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (!u_sess->attr.attr_sql.enable_seqscan)
        path->total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);

    /*
     * data redistribution for DFS table.
     */
    if (true == u_sess->attr.attr_sql.enable_cluster_resize && root->query_level == 1 &&
        root->parse->commandType == CMD_INSERT) {
        root->dataDestRelIndex = baserel->relid;
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * cost_tsstorescan
 *    Determines and returns the cost of scanning a time series store.
 */
void cost_tsstorescan(Path *path, PlannerInfo *root, RelOptInfo *baserel)
{
    double spc_seq_page_cost;
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost cpu_per_tuple = 0.0;
    int dop = SET_DOP(path->dop);

    /* Should only be applied to base relations */
    Assert(baserel->relid > 0 && baserel->rtekind == RTE_RELATION);

    set_rel_path_rows(path, baserel, NULL);
    set_parallel_path_rows(path);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(baserel->reltablespace, NULL, &spc_seq_page_cost);

    startup_cost += baserel->baserestrictcost.startup;

    if (!u_sess->attr.attr_sql.enable_seqscan) {
        startup_cost += g_instance.cost_cxt.disable_cost;
    }

    /* 
     * When we parallel the scan node, then the disk costs and cpu costs 
     * wiil be equal division to all parallelism thread.
     */
    run_cost += u_sess->opt_cxt.smp_thread_cost * (dop - 1);
    run_cost += spc_seq_page_cost * baserel->pages / dop;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost / COL_TUPLE_COST_MULTIPLIER
                    + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples) / dop;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (!u_sess->attr.attr_sql.enable_seqscan) {
        path->total_cost *= 
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
    }
}
#endif   /* ENABLE_MULTIPLE_NODES */

/*
 * apply_random_page_cost_mod
 *     Apply a logistic filter on the resulting MAX/MIN costs;
 * This mod is effective when dealing with small tables with small amount of random accesses.
 *
 * Note1: random_page_cost measures how efficient to do random accesses on drives, we assume it is somewhat 
 * related to the number of pages just for now.
 *
 * Note2: We do want to evaluate the costs dynamically base on the number of pages because for smaller tables,
 *     random accesses and sequencial accesses doesn't make such a difference, thus the costs should be
 *     tuned down in those situations.
 *
 * This mod will help the planner promotes index scan paths better.
 */
double apply_random_page_cost_mod(double rand_page_cost, double seq_page_cost, double num_of_page)
{
    /*
     * Smooth out at num_of_page = 1000
     * We get this number base on random tests. Try not to make the number too big that
     * the index scan will probably dominates in most of the plans.
     */
#define COST_SLOPE_FACTOR 0.005
#define COST_PAGE_THRESHOLD 1000
    double slope_factor = COST_SLOPE_FACTOR;

    /* Logistic function */
    double new_page_cost = (seq_page_cost >= rand_page_cost) ? rand_page_cost : \
        LOGISTIC_FUNC(num_of_page, COST_PAGE_THRESHOLD, rand_page_cost, seq_page_cost, slope_factor);

    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            (errmsg("Estimating random page cost = %lf with sql_beta_feature = RAND_COST_OPT.", new_page_cost))));

    return new_page_cost;
}

/*
 * is_predpush_dest
 *    check if the predpush dest are part of the indexes.
 * @param current-level root, relids from baserel.
 * @return true if matched.
 */
static bool is_predpush_dest(PlannerInfo* root, Relids indexes)
{
    HintState *hstate = root->parse->hintState;
    if (hstate == NULL) {
        return false;
    }

    if (hstate->predpush_hint == NULL) {
        return false;
    }

    ListCell *lc = NULL;
    foreach (lc, hstate->predpush_hint) {
        PredpushHint *predpushHint = (PredpushHint*)lfirst(lc);
        if (predpushHint->dest_id != 0 && predpushHint->candidates != NULL && \
            bms_is_member(predpushHint->dest_id, indexes)) {
            return true;
        }
    }
    return false;
}

/*
 * enable_parametrized_path
 * If enabled, disable all unmatched paths to get the parametrized path we need.
 */
static bool enable_parametrized_path(PlannerInfo* root, RelOptInfo* baserel, Path* path)
{
    Assert(path != NULL);

    if (!ENABLE_SQL_BETA_FEATURE(PREDPUSH_SAME_LEVEL)) {
        /* sql beta feature is necessary */
        return false;
    }

    if (ENABLE_PRED_PUSH_FORCE(root) && is_predpush_dest(root, baserel->relids)) {
        if (path->param_info) {
            return !bms_is_subset(path->param_info->ppi_req_outer, predpush_candidates_same_level(root));
        } else {
            return true;
        }
    }

    return false;
}

#define HEAP_PAGES_FETCHED(isUstore, pages_fetched, allvisfrac) \
        (isUstore) ? 0.0 : ceil((pages_fetched) * (1.0 - (allvisfrac)))

/*
 * cost_index
 *	  Determines and returns the cost of scanning a relation using an index.
 *
 * 'path' describes the indexscan under consideration, and is complete
 *		except for the fields to be set by this routine
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior
 *
 * In addition to rows, startup_cost and total_cost, cost_index() sets the
 * path's indextotalcost and indexselectivity fields.  These values will be
 * needed if the IndexPath is used in a BitmapIndexScan.
 *
 * NOTE: path->indexquals must contain only clauses usable as index
 * restrictions.  Any additional quals evaluated as qpquals may reduce the
 * number of returned tuples, but they won't reduce the number of tuples
 * we have to fetch from the table, so they don't reduce the scan cost.
 */
void cost_index(IndexPath* path, PlannerInfo* root, double loop_count)
{
    IndexOptInfo* index = path->indexinfo;
    RelOptInfo* baserel = index->rel;
    bool isUstore = baserel->is_ustore;
    bool indexonly = (path->path.pathtype == T_IndexOnlyScan);
    List* allclauses = NIL;
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost indexStartupCost;
    Cost indexTotalCost;
    Selectivity indexSelectivity;
    double indexCorrelation, csquared;
    double spc_seq_page_cost, spc_random_page_cost;
    Cost min_IO_cost, max_IO_cost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple = 0.0;
    double tuples_fetched;
    double pages_fetched;
    bool ispartitionedindex = path->indexinfo->rel->isPartitionedTable;
    bool disable_path = enable_parametrized_path(root, baserel, (Path*)path) || \
        (!u_sess->attr.attr_sql.enable_indexscan);

    /* Should only be applied to base relations */
    AssertEreport(IsA(baserel, RelOptInfo) && IsA(index, IndexOptInfo),
        MOD_OPT,
        "The nodeTag of baserel is not T_RelOptInfo, or the nodeTag of index is not T_IndexOptInfo"
        "when determining the cost of scanning a relation using an index.");
    AssertEreport(baserel->relid > 0,
        MOD_OPT,
        "The relid is invalid when determining the cost of scanning a relation using an index.");
    AssertEreport(baserel->rtekind == RTE_RELATION,
        MOD_OPT,
        "Only base relation can be supported when determining the cost of scanning a relation using an index.");

    set_rel_path_rows(&path->path, baserel, path->path.param_info);

    /* Mark the path with the correct row estimate */
    if (path->path.param_info) {
        /* also get the set of clauses that should be enforced by the scan */
        allclauses = list_concat(list_copy(path->path.param_info->ppi_clauses), baserel->baserestrictinfo);
    } else {
        /* allclauses should just be the rel's restriction clauses */
        allclauses = baserel->baserestrictinfo;
    }

    if (disable_path)
        startup_cost += g_instance.cost_cxt.disable_cost;
    /* we don't need to check enable_indexonlyscan; indxpath.c does that */
    /*
     * Call index-access-method-specific code to estimate the processing cost
     * for scanning the index, as well as the selectivity of the index (ie,
     * the fraction of main-table tuples we will have to retrieve) and its
     * correlation to the main-table tuple order.
     */
    OidFunctionCall7(index->amcostestimate,
        PointerGetDatum(root),
        PointerGetDatum(path),
        Float8GetDatum(loop_count),
        PointerGetDatum(&indexStartupCost),
        PointerGetDatum(&indexTotalCost),
        PointerGetDatum(&indexSelectivity),
        PointerGetDatum(&indexCorrelation));

    /*
     * Save amcostestimate's results for possible use in bitmap scan planning.
     * We don't bother to save indexStartupCost or indexCorrelation, because a
     * bitmap scan doesn't care about either.
     */
    path->indextotalcost = indexTotalCost;
    path->indexselectivity = indexSelectivity;

    /* all costs for touching index itself included here */
    startup_cost += indexStartupCost;
    run_cost += indexTotalCost - indexStartupCost;

    /* estimate number of main-table tuples fetched */
    tuples_fetched = clamp_row_est(indexSelectivity * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples));

    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            errmsg("Computing IndexScanCost: tuples_fetched: %lf, indexSelectivity: %lf, indexTotalCost: %lf",
                tuples_fetched, indexSelectivity, indexTotalCost)));

    /* fetch estimated page costs for tablespace containing table */
    get_tablespace_page_costs(baserel->reltablespace, &spc_random_page_cost, &spc_seq_page_cost);

    /* ----------
     * Estimate number of main-table pages fetched, and compute I/O cost.
     *
     * When the index ordering is uncorrelated with the table ordering,
     * we use an approximation proposed by Mackert and Lohman (see
     * index_pages_fetched() for details) to compute the number of pages
     * fetched, and then charge spc_random_page_cost per page fetched.
     *
     * When the index ordering is exactly correlated with the table ordering
     * (just after a CLUSTER, for example), the number of pages fetched should
     * be exactly selectivity * table_size.  What's more, all but the first
     * will be sequential fetches, not the random fetches that occur in the
     * uncorrelated case.  So if the number of pages is more than 1, we
     * ought to charge
     *		spc_random_page_cost + (pages_fetched - 1) * spc_seq_page_cost
     * For partially-correlated indexes, we ought to charge somewhere between
     * these two estimates.  We currently interpolate linearly between the
     * estimates based on the correlation squared (XXX is that appropriate?).
     *
     * If it's an index-only scan, then we will not need to fetch any heap
     * pages for which the visibility map shows all tuples are visible.
     * Hence, reduce the estimated number of heap fetches accordingly.
     * We use the measured fraction of the entire heap that is all-visible,
     * which might not be particularly relevant to the subset of the heap
     * that this query will fetch; but it's not clear how to do better.
     * For ustore, there is visibility info in index so we will not need to
     * fetch any heap pages for index-only scan.
     * ----------
     */

    /* Cost mod flag */
    double old_random_page_cost = spc_random_page_cost;
    bool use_modded_cost = ENABLE_SQL_BETA_FEATURE(RAND_COST_OPT);

    if (loop_count > 1) {
        /*
         * For repeated indexscans, the appropriate estimate for the
         * uncorrelated case is to scale up the number of tuples fetched in
         * the Mackert and Lohman formula by the number of scans, so that we
         * estimate the number of pages fetched by all the scans; then
         * pro-rate the costs for one scan.  In this case we assume all the
         * fetches are random accesses.
         */
        pages_fetched = index_pages_fetched(
            tuples_fetched * loop_count, (BlockNumber)baserel->pages, (double)index->pages, root, ispartitionedindex);

        if (indexonly)
            pages_fetched = HEAP_PAGES_FETCHED(isUstore, pages_fetched, baserel->allvisfrac);

        /* Apply cost mod */
        spc_random_page_cost = RANDOM_PAGE_COST(use_modded_cost, old_random_page_cost, \
                                    spc_seq_page_cost, pages_fetched);

        max_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;

        ereport(DEBUG2,
            (errmodule(MOD_OPT),
                errmsg("Computing IndexScanCost(loop_count > 1): max_pages_fetched: %lf, max_IO_cost: %lf",
                    pages_fetched, max_IO_cost)));

        /*
         * In the perfectly correlated case, the number of pages touched by
         * each scan is selectivity * table_size, and we can use the Mackert
         * and Lohman formula at the page level to estimate how much work is
         * saved by caching across scans.  We still assume all the fetches are
         * random, though, which is an overestimate that's hard to correct for
         * without double-counting the cache effects.  (But in most cases
         * where such a plan is actually interesting, only one page would get
         * fetched per scan anyway, so it shouldn't matter much.)
         */
        pages_fetched = ceil(indexSelectivity * (double)baserel->pages);

        pages_fetched = index_pages_fetched(
            pages_fetched * loop_count, (BlockNumber)baserel->pages, (double)index->pages, root, ispartitionedindex);

        if (indexonly)
            pages_fetched = HEAP_PAGES_FETCHED(isUstore, pages_fetched, baserel->allvisfrac);

        /* Apply cost mod after new pages fetched */
        spc_random_page_cost = RANDOM_PAGE_COST(use_modded_cost, old_random_page_cost, \
                                    spc_seq_page_cost, pages_fetched);

        min_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;

        ereport(DEBUG2,
            (errmodule(MOD_OPT),
                errmsg("Computing IndexScanCost(loop_count > 1): min_pages_fetched: %lf, min_IO_cost: %lf",
                    pages_fetched, min_IO_cost)));
    } else {
        /*
         * Normal case: apply the Mackert and Lohman formula, and then
         * interpolate between that and the correlation-derived result.
         */
        pages_fetched = index_pages_fetched(
            tuples_fetched, (BlockNumber)baserel->pages, (double)index->pages, root, ispartitionedindex);

        if (indexonly)
            pages_fetched = HEAP_PAGES_FETCHED(isUstore, pages_fetched, baserel->allvisfrac);

        /* Apply cost mod */
        spc_random_page_cost = RANDOM_PAGE_COST(use_modded_cost, old_random_page_cost, \
                                    spc_seq_page_cost, pages_fetched);

        /* max_IO_cost is for the perfectly uncorrelated case (csquared=0) */
        max_IO_cost = pages_fetched * spc_random_page_cost;

        ereport(DEBUG2,
            (errmodule(MOD_OPT),
                errmsg("Computing IndexScanCost(loop_count = 1): max_pages_fetched: %lf, max_IO_cost: %lf",
                    pages_fetched, max_IO_cost)));

        /* min_IO_cost is for the perfectly correlated case (csquared=1) */
        pages_fetched = ceil(indexSelectivity * (double)baserel->pages);

        if (indexonly)
            pages_fetched = HEAP_PAGES_FETCHED(isUstore, pages_fetched, baserel->allvisfrac);

        if (pages_fetched > 0) {
            /* Apply cost mod after new pages fetched */
            spc_random_page_cost = RANDOM_PAGE_COST(use_modded_cost, old_random_page_cost, \
                                    spc_seq_page_cost, pages_fetched);

            min_IO_cost = spc_random_page_cost;
            if (pages_fetched > 1)
                min_IO_cost += (pages_fetched - 1) * spc_seq_page_cost;
        } else {
            min_IO_cost = 0;
        }
        
        /*
         * When database keep running without vacuum, the number of relpages may inflate quickly 
         * and finally cause min_IO_cost overestimated. So, adjust min_IO_cost to ensure 
         * min_IO_cost < max_IO_cost.
         */
        min_IO_cost = Min(min_IO_cost, max_IO_cost);

        ereport(DEBUG2,
            (errmodule(MOD_OPT),
                errmsg("Computing IndexScanCost(loop_count = 1): min_pages_fetched: %lf, min_IO_cost: %lf",
                    pages_fetched, min_IO_cost)));
    }

    min_IO_cost = Min(min_IO_cost, max_IO_cost);

    /*
     * Now interpolate based on estimated index order correlation to get total
     * disk I/O cost for main table accesses.
     */
    csquared = indexCorrelation * indexCorrelation;

    run_cost += max_IO_cost + csquared * (min_IO_cost - max_IO_cost);

    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            errmsg("Computing IndexScanCost: max_IO_cost: %lf, min_IO_cost: %lf, csquared: %lf, IO_run_cost: %lf",
                max_IO_cost, min_IO_cost, csquared, max_IO_cost + csquared * (min_IO_cost - max_IO_cost))));

    /*
     * Estimate CPU costs per tuple.
     *
     * What we want here is cpu_tuple_cost plus the evaluation costs of any
     * qual clauses that we have to evaluate as qpquals.  We approximate that
     * list as allclauses minus any clauses appearing in indexquals.  (We
     * assume that pointer equality is enough to recognize duplicate
     * RestrictInfos.)	This method neglects some considerations such as
     * clauses that needn't be checked because they are implied by a partial
     * index's predicate.  It does not seem worth the cycles to try to factor
     * those things in at this stage, even though createplan.c will take pains
     * to remove such unnecessary clauses from the qpquals list if this path
     * is selected for use.
     */
    cost_qual_eval(&qpqual_cost, list_difference_ptr(allclauses, path->indexquals), root);

    startup_cost += qpqual_cost.startup;

    if (path->path.parent->orientation == REL_COL_ORIENTED)
        cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost / 10 + qpqual_cost.per_tuple;
    else if (path->path.parent->orientation == REL_TIMESERIES_ORIENTED)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Unsupported Using Index FOR TIMESERIES.")));
    else
        cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qpqual_cost.per_tuple;

    run_cost += cpu_per_tuple * tuples_fetched;

    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            errmsg("Computing IndexScanCost: cpu_per_tuple: %lf, tuples_fetched: %lf, cpu_run_cost: %lf",
                cpu_per_tuple, tuples_fetched, cpu_per_tuple * tuples_fetched)));

    path->path.startup_cost = startup_cost;
    path->path.total_cost = startup_cost + run_cost;
    path->path.stream_cost = 0;

    if (disable_path)
        path->path.total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
    /* clamp weighted cost below 1e30 for double overflow */
    double weight = u_sess->attr.attr_sql.cost_weight_index;
    path->path.startup_cost = (1e30f / weight > path->path.startup_cost) ? (path->path.startup_cost * weight) : (1e30);
    path->path.total_cost = (1e30f / weight > path->path.total_cost) ? (path->path.total_cost * weight) : (1e30);
    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            errmsg("IndexScan Cost startup_cost: %lf, total_cost: %lf, pages_fetched: %lf",
                path->path.startup_cost, path->path.total_cost, pages_fetched)));
}

/*
 * index_pages_fetched
 *	  Estimate the number of pages actually fetched after accounting for
 *	  cache effects.
 *
 * We use an approximation proposed by Mackert and Lohman, "Index Scans
 * Using a Finite LRU Buffer: A Validated I/O Model", ACM Transactions
 * on Database Systems, Vol. 14, No. 3, September 1989, Pages 401-424.
 * The Mackert and Lohman approximation is that the number of pages
 * fetched is
 *	PF =
 *		min(2TNs/(2T+Ns), T)			when T <= b
 *		2TNs/(2T+Ns)					when T > b and Ns <= 2Tb/(2T-b)
 *		b + (Ns - 2Tb/(2T-b))*(T-b)/T	when T > b and Ns > 2Tb/(2T-b)
 * where
 *		T = # pages in table
 *		N = # tuples in table
 *		s = selectivity = fraction of table to be scanned
 *		b = # buffer pages available (we include kernel space here)
 *
 * We assume that g_instance.cost_cxt.effective_cache_size is the total number of buffer pages
 * available for the whole query, and pro-rate that space across all the
 * tables in the query and the index currently under consideration.  (This
 * ignores space needed for other indexes used by the query, but since we
 * don't know which indexes will get used, we can't estimate that very well;
 * and in any case counting all the tables may well be an overestimate, since
 * depending on the join plan not all the tables may be scanned concurrently.)
 *
 * The product Ns is the number of tuples fetched; we pass in that
 * product rather than calculating it here.  "pages" is the number of pages
 * in the object under consideration (either an index or a table).
 * "index_pages" is the amount to add to the total table space, which was
 * computed for us by query_planner.
 *
 * Caller is expected to have ensured that tuples_fetched is greater than zero
 * and rounded to integer (see clamp_row_est).	The result will likewise be
 * greater than zero and integral.
 *
 * add an input parameter to indicate if it is for partitioned index. because the
 * method of calculating the base stat info for partitioned index cannot fulfill
 * the logic for ordinary table., so we have to deal with it specially.
 */
double index_pages_fetched(
    double tuples_fetched, BlockNumber pages, double index_pages, PlannerInfo* root, bool ispartitionedindex)
{
    double pages_fetched;
    double total_pages;
    double T, b;

    /* T is # pages in table, but don't allow it to be zero */
    T = (pages > 1) ? (double)pages : 1.0;

    /* Compute number of pages assumed to be competing for cache space */
    total_pages = root->total_table_pages + index_pages;
    total_pages = Max(total_pages, 1.0);

    /*
     * it is special to estimate the number of pages actually fetched.
     * it look likes illegal, but we have to do this because the method
     * of calculating the base stat info for partitioned index cannot
     * fulfill the logic for ordinary table
     */
    if (ispartitionedindex) {
        T = (T > total_pages ? T : total_pages);
    } else {
        AssertEreport(total_pages >= T,
            MOD_OPT,
            "The number of pages in table is larger than total_pages"
            "when estimating the number of pages actually fetched.");
    }

    /* b is pro-rated share of u_sess->attr.attr_sql.effective_cache_size */
    b = (double)u_sess->attr.attr_sql.effective_cache_size * T / total_pages;

    /* force it positive and integral */
    if (b <= 1.0) {
        b = 1.0;
    } else {
        b = ceil(b);
    }

    /* This part is the Mackert and Lohman formula */
    if (T <= b) {
        pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
        if (pages_fetched >= T) {
            pages_fetched = T;
        } else {
            pages_fetched = ceil(pages_fetched);
        }
    } else {
        double lim;

        lim = (2.0 * T * b) / (2.0 * T - b);
        if (tuples_fetched <= lim) {
            pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
        } else {
            pages_fetched = b + (tuples_fetched - lim) * (T - b) / T;
        }
        pages_fetched = ceil(pages_fetched);
    }
    return pages_fetched;
}

/*
 * get_indexpath_pages
 *		Determine the total size of the indexes used in a bitmap index path.
 *
 * Note: if the same index is used more than once in a bitmap tree, we will
 * count it multiple times, which perhaps is the wrong thing ... but it's
 * not completely clear, and detecting duplicates is difficult, so ignore it
 * for now.
 */
static double get_indexpath_pages(Path* bitmapqual)
{
    double result = 0;
    ListCell* l = NULL;

    if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;

        foreach (l, apath->bitmapquals) {
            result += get_indexpath_pages((Path*)lfirst(l));
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;

        foreach (l, opath->bitmapquals) {
            result += get_indexpath_pages((Path*)lfirst(l));
        }
    } else if (IsA(bitmapqual, IndexPath)) {
        IndexPath* ipath = (IndexPath*)bitmapqual;

        result = (double)ipath->indexinfo->pages;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type of a bitmap index path when get pages: %d", nodeTag(bitmapqual))));
    }

    return result;
}

/*
 * cost_bitmap_heap_scan
 *	  Determines and returns the cost of scanning a relation using a bitmap
 *	  index-then-heap plan.
 *
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 * 'bitmapqual' is a tree of IndexPaths, BitmapAndPaths, and BitmapOrPaths
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior
 *
 * Note: the component IndexPaths in bitmapqual should have been costed
 * using the same loop_count.
 */
void cost_bitmap_heap_scan(
    Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info, Path* bitmapqual, double loop_count)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost indexTotalCost;
    Selectivity indexSelectivity;
    QualCost qpqual_cost;
    Cost cpu_per_tuple = 0.0;
    Cost cost_per_page;
    double tuples_fetched;
    double pages_fetched;
    double spc_seq_page_cost, spc_random_page_cost;
    double T;
    bool disable_path = (!u_sess->attr.attr_sql.enable_bitmapscan) ||
        enable_parametrized_path(root, baserel, (Path*)path);
    bool canCrossBucket = (baserel->bucketInfo == NULL);

    /* Should only be applied to base relations */
    AssertEreport(IsA(baserel, RelOptInfo),
        MOD_OPT,
        "The nodeTag of baserel is not T_RelOptInfo"
        "when determining the cost of scanning a relation using a bitmap index-then-heap plan.");
    AssertEreport(baserel->relid > 0,
        MOD_OPT,
        "The relid is invalid when determining the cost of scanning a relation"
        "using a bitmap index-then-heap plan.");
    AssertEreport(baserel->rtekind == RTE_RELATION,
        MOD_OPT,
        "Only base relation can be supported when determining the cost of scanning a relation"
        "using a bitmap index-then-heap plan.");

    /* Mark the path with the correct row estimate */
    set_rel_path_rows(path, baserel, param_info);

    /*
     * Support partiton index unusable.
     * Here not support bitmap index unusable.If the bitmap path contains unusable index paths, set enable_bitmapscan to
     * off. So it will go partition full/partial unusable index scan ,if index path is selected.
     */
    if (path->parent->isPartitionedTable) {
        if (!check_bitmap_heap_path_index_unusable(bitmapqual, baserel)) {
            disable_path = true;
        }
        
        /* If the bitmap path contains both global and local partition index, set enable_bitmapscan to off */
        if (CheckBitmapHeapPathContainGlobalOrLocal(bitmapqual)) {
            disable_path = true;
        }
    }

    /* If the bitmap path contains both crossbucket and non-crossbucket index, set enable_bitmapscan to off */
    if (baserel->bucketInfo != NULL && CheckBitmapHeapPathIsCrossbucket(bitmapqual)) {
        canCrossBucket = true;
    }
    disable_path |= (!canCrossBucket);  /* Disable path if cannot crossbucket */

    if (disable_path) {
        startup_cost += g_instance.cost_cxt.disable_cost;
    }

    /*
     * Fetch total cost of obtaining the bitmap, as well as its total
     * selectivity.
     */
    cost_bitmap_tree_node(bitmapqual, &indexTotalCost, &indexSelectivity);

    startup_cost += indexTotalCost;

    /* Fetch estimated page costs for tablespace containing table. */
    get_tablespace_page_costs(baserel->reltablespace, &spc_random_page_cost, &spc_seq_page_cost);

    /*
     * Estimate number of main-table pages fetched.
     */
    tuples_fetched = clamp_row_est(indexSelectivity * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples));

    T = (baserel->pages > 1) ? (double)baserel->pages : 1.0;

    if (loop_count > 1) {
        /*
         * For repeated bitmap scans, scale up the number of tuples fetched in
         * the Mackert and Lohman formula by the number of scans, so that we
         * estimate the number of pages fetched by all the scans. Then
         * pro-rate for one scan.
         */
        pages_fetched = index_pages_fetched(tuples_fetched * loop_count,
            (BlockNumber)baserel->pages,
            get_indexpath_pages(bitmapqual),
            root,
            path->parent->isPartitionedTable);

        pages_fetched /= loop_count;
    } else {
        /*
         * For a single scan, the number of heap pages that need to be fetched
         * is the same as the Mackert and Lohman formula for the case T <= b
         * (ie, no re-reads needed).
         */
        pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
    }

    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            errmsg("Computing IndexScanCost: pagesFetched: %lf, tuples_fetched: %lf, indexSelectivity: %lf,"
                   " indexTotalCost: %lf, loopCount: %lf, T: %lf",
                pages_fetched, tuples_fetched, indexSelectivity, indexTotalCost, loop_count, T)));

    if (pages_fetched >= T) {
        pages_fetched = T;
    } else {
        pages_fetched = ceil(pages_fetched);
    }

    /*
     * For small numbers of pages we should charge spc_random_page_cost
     * apiece, while if nearly all the table's pages are being read, it's more
     * appropriate to charge spc_seq_page_cost apiece.	The effect is
     * nonlinear, too. For lack of a better idea, interpolate like this to
     * determine the cost per page.
     */
    if (pages_fetched >= 2.0)
        cost_per_page = spc_random_page_cost - (spc_random_page_cost - spc_seq_page_cost) * sqrt(pages_fetched / T);
    else
        cost_per_page = spc_random_page_cost;

    run_cost += pages_fetched * cost_per_page;

    /*
     * Estimate CPU costs per tuple.
     *
     * Often the indexquals don't need to be rechecked at each tuple ... but
     * not always, especially not if there are enough tuples involved that the
     * bitmaps become lossy.  For the moment, just assume they will be
     * rechecked always.  This means we charge the full freight for all the
     * scan clauses.
     */
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qpqual_cost.per_tuple;

    run_cost += cpu_per_tuple * tuples_fetched;
    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            errmsg("Computing IndexScanCost: startupCost: %lf, runCost: %lf",
                startup_cost, run_cost)));

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (disable_path) {
        path->total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
    }
}

/*
 * cost_bitmap_tree_node
 *		Extract cost and selectivity from a bitmap tree node (index/and/or)
 */
void cost_bitmap_tree_node(Path* path, Cost* cost, Selectivity* selec)
{
    if (IsA(path, IndexPath)) {
        *cost = ((IndexPath*)path)->indextotalcost;
        *selec = ((IndexPath*)path)->indexselectivity;

        /*
         * Charge a small amount per retrieved tuple to reflect the costs of
         * manipulating the bitmap.  This is mostly to make sure that a bitmap
         * scan doesn't look to be the same cost as an indexscan to retrieve a
         * single tuple.
         */
        *cost += 0.1 * u_sess->attr.attr_sql.cpu_operator_cost * PATH_LOCAL_ROWS(path);
    } else if (IsA(path, BitmapAndPath)) {
        *cost = path->total_cost;
        *selec = ((BitmapAndPath*)path)->bitmapselectivity;
    } else if (IsA(path, BitmapOrPath)) {
        *cost = path->total_cost;
        *selec = ((BitmapOrPath*)path)->bitmapselectivity;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type when extract cost and selectivity from a bitmap tree node: %d",
                    nodeTag(path))));
        *cost = *selec = 0; /* keep compiler quiet */
    }
}

/*
 * cost_bitmap_and_node
 *		Estimate the cost of a BitmapAnd node
 *
 * Note that this considers only the costs of index scanning and bitmap
 * creation, not the eventual heap access.	In that sense the object isn't
 * truly a Path, but it has enough path-like properties (costs in particular)
 * to warrant treating it as one.  We don't bother to set the path rows field,
 * however.
 */
void cost_bitmap_and_node(BitmapAndPath* path, PlannerInfo* root)
{
    Cost totalCost;
    Selectivity selec;
    ListCell* l = NULL;

    /*
     * We estimate AND selectivity on the assumption that the inputs are
     * independent.  This is probably often wrong, but we don't have the info
     * to do better.
     *
     * The runtime cost of the BitmapAnd itself is estimated at 100x
     * cpu_operator_cost for each tbm_intersect needed.  Probably too small,
     * definitely too simplistic?
     */
    totalCost = 0.0;
    selec = 1.0;
    foreach (l, path->bitmapquals) {
        Path* subpath = (Path*)lfirst(l);
        Cost subCost;
        Selectivity subselec;

        cost_bitmap_tree_node(subpath, &subCost, &subselec);

        selec *= subselec;

        totalCost += subCost;
        if (l != list_head(path->bitmapquals))
            totalCost += 100.0 * u_sess->attr.attr_sql.cpu_operator_cost;
    }
    path->bitmapselectivity = selec;
    set_path_rows(&path->path, 0); /* per above, not used */
    path->path.startup_cost = totalCost;
    path->path.total_cost = totalCost;
    path->path.stream_cost = 0;
}

/*
 * cost_bitmap_or_node
 *		Estimate the cost of a BitmapOr node
 *
 * See comments for cost_bitmap_and_node.
 */
void cost_bitmap_or_node(BitmapOrPath* path, PlannerInfo* root)
{
    Cost totalCost;
    Selectivity selec;
    ListCell* l = NULL;

    /*
     * We estimate OR selectivity on the assumption that the inputs are
     * non-overlapping, since that's often the case in "x IN (list)" type
     * situations.	Of course, we clamp to 1.0 at the end.
     *
     * The runtime cost of the BitmapOr itself is estimated at 100x
     * cpu_operator_cost for each tbm_union needed.  Probably too small,
     * definitely too simplistic?  We are aware that the tbm_unions are
     * optimized out when the inputs are BitmapIndexScans.
     */
    totalCost = 0.0;
    selec = 0.0;
    foreach (l, path->bitmapquals) {
        Path* subpath = (Path*)lfirst(l);
        Cost subCost;
        Selectivity subselec;

        cost_bitmap_tree_node(subpath, &subCost, &subselec);

        selec += subselec;

        totalCost += subCost;
        if (l != list_head(path->bitmapquals) && !IsA(subpath, IndexPath))
            totalCost += 100.0 * u_sess->attr.attr_sql.cpu_operator_cost;
    }
    path->bitmapselectivity = Min(selec, 1.0);
    set_path_rows(&path->path, 0); /* per above, not used */
    path->path.startup_cost = totalCost;
    path->path.total_cost = totalCost;
    path->path.stream_cost = 0;
}

/*
 * cost_tidscan
 *	  Determines and returns the cost of scanning a relation using TIDs.
 */
void cost_tidscan(Path* path, PlannerInfo* root, RelOptInfo* baserel, List* tidquals)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    bool isCurrentOf = false;
    Cost cpu_per_tuple = 0.0;
    QualCost tid_qual_cost;
    int ntuples;
    ListCell* l = NULL;
    double spc_random_page_cost;

    /* Should only be applied to base relations */
    AssertEreport(baserel->relid > 0,
        MOD_OPT,
        "The relid is invalid when determining the cost of scanning a relation using TIDs.");
    AssertEreport(baserel->rtekind == RTE_RELATION,
        MOD_OPT,
        "Only base relation can be supported"
        "when determining the cost of scanning a relation using TIDs.");

    /* For now, tidscans are never parameterized */
    set_rel_path_rows(path, baserel, NULL);

    /* Count how many tuples we expect to retrieve */
    ntuples = 0;
    foreach (l, tidquals) {
        if (IsA(lfirst(l), ScalarArrayOpExpr)) {
            /* Each element of the array yields 1 tuple */
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)lfirst(l);
            Node* arraynode = (Node*)lsecond(saop->args);

            ntuples += estimate_array_length(arraynode);
        } else if (IsA(lfirst(l), CurrentOfExpr)) {
            /* CURRENT OF yields 1 tuple */
            isCurrentOf = true;
            ntuples++;
        } else {
            /* It's just CTID = something, count 1 tuple */
            ntuples++;
        }
    }

    /*
     * We must force TID scan for WHERE CURRENT OF, because only nodeTidscan.c
     * understands how to do it correctly.	Therefore, honor u_sess->attr.attr_sql.enable_tidscan
     * only when CURRENT OF isn't present.  Also note that cost_qual_eval
     * counts a CurrentOfExpr as having startup cost g_instance.cost_cxt.disable_cost, which we
     * subtract off here; that's to prevent other plan types such as seqscan
     * from winning.
     */
    if (isCurrentOf) {
        AssertEreport(baserel->baserestrictcost.startup >= g_instance.cost_cxt.disable_cost,
            MOD_OPT,
            "The one-time cost of base relation is less than disable_cost"
            "when determining the cost of scanning a relation using TIDs.");
        startup_cost -= g_instance.cost_cxt.disable_cost;
    } else if (!u_sess->attr.attr_sql.enable_tidscan)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /*
     * The TID qual expressions will be computed once, any other baserestrict
     * quals once per retrieved tuple.
     */
    cost_qual_eval(&tid_qual_cost, tidquals, root);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(baserel->reltablespace, &spc_random_page_cost, NULL);

    /* disk costs --- assume each tuple on a different page */
    run_cost += spc_random_page_cost * ntuples;

    /* CPU costs */
    startup_cost += baserel->baserestrictcost.startup + tid_qual_cost.per_tuple;
    cpu_per_tuple =
        u_sess->attr.attr_sql.cpu_tuple_cost + baserel->baserestrictcost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (!u_sess->attr.attr_sql.enable_tidscan && !isCurrentOf)
        path->total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
}

/*
 * cost_subqueryscan
 *	  Determines and returns the cost of scanning a subquery RTE.
 *
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
void cost_subqueryscan(Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info)
{
    Cost startup_cost;
    Cost run_cost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple = 0.0;

    /* Should only be applied to base relations that are subqueries */
    AssertEreport(
        baserel->relid > 0, MOD_OPT, "The relid is invalid when determining the cost of scanning a subquery RTE.");
    AssertEreport(baserel->rtekind == RTE_SUBQUERY,
        MOD_OPT,
        "Only subquery in FROM clause can be supported"
        "when determining the cost of scanning a subquery RTE.");

    /* Mark the path with the correct row estimate */
    set_rel_path_rows(path, baserel, param_info);

    /*
     * Cost of path is cost of evaluating the subplan, plus cost of evaluating
     * any restriction clauses that will be attached to the SubqueryScan node,
     * plus cpu_tuple_cost to account for selection and projection overhead.
     */
    path->startup_cost = baserel->subplan->startup_cost;
    path->total_cost = baserel->subplan->total_cost;

    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);

    startup_cost = qpqual_cost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost = cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples);

    path->startup_cost += startup_cost;
    path->total_cost += startup_cost + run_cost;
    path->stream_cost = get_subqueryscan_stream_cost(baserel->subplan);
    ereport(DEBUG2,
        (errmodule(MOD_OPT_SUBPLAN),
            errmsg("subqueryscan stream_cost: %lf, startup_cost: %lf, total_cost: %lf",
                path->stream_cost,
                path->startup_cost,
                path->total_cost)));
}

/*
 * cost_functionscan
 *	  Determines and returns the cost of scanning a function RTE.
 */
void cost_functionscan(Path* path, PlannerInfo* root, RelOptInfo* baserel)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost cpu_per_tuple = 0.0;
    RangeTblEntry* rte = NULL;
    QualCost exprcost;

    /* Should only be applied to base relations that are functions */
    AssertEreport(
        baserel->relid > 0, MOD_OPT, "The relid is invalid when determining the cost of scanning a function RTE.");
    rte = planner_rt_fetch(baserel->relid, root);
    AssertEreport(rte->rtekind == RTE_FUNCTION,
        MOD_OPT,
        "Only function in FROM clause can be supported"
        "when determining the cost of scanning a function RTE.");

    /* functionscans are never parameterized */
    set_rel_path_rows(path, baserel, NULL);

    /*
     * Estimate costs of executing the function expression.
     *
     * Currently, nodeFunctionscan.c always executes the function to
     * completion before returning any rows, and caches the results in a
     * tuplestore.	So the function eval cost is all startup cost, and per-row
     * costs are minimal.
     *
     * XXX in principle we ought to charge tuplestore spill costs if the
     * number of rows is large.  However, given how phony our rowcount
     * estimates for functions tend to be, there's not a lot of point in that
     * refinement right now.
     */
    cost_qual_eval_node(&exprcost, rte->funcexpr, root);

    startup_cost += exprcost.startup + exprcost.per_tuple;

    /* Add scanning CPU costs */
    startup_cost += baserel->baserestrictcost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples);

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;
}

/*
 * cost_valuesscan
 *	  Determines and returns the cost of scanning a VALUES RTE.
 */
void cost_valuesscan(Path* path, PlannerInfo* root, RelOptInfo* baserel)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost cpu_per_tuple = 0.0;

    /* Should only be applied to base relations that are values lists */
    AssertEreport(
        baserel->relid > 0, MOD_OPT, "The relid is invalid when determining the cost of scanning a VALUES RTE.");
    AssertEreport(baserel->rtekind == RTE_VALUES,
        MOD_OPT,
        "Only VALUES list can be supported when determining the cost of scanning a VALUES RTE.");

    /* valuesscans are never parameterized */
    set_rel_path_rows(path, baserel, NULL);

    /*
     * For now, estimate list evaluation cost at one operator eval per list
     * (probably pretty bogus, but is it worth being smarter?)
     */
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_operator_cost;

    /* Add scanning CPU costs */
    startup_cost += baserel->baserestrictcost.startup;
    cpu_per_tuple += u_sess->attr.attr_sql.cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples);

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;
}

/*
 * cost_ctescan
 *	  Determines and returns the cost of scanning a CTE RTE.
 *
 * Note: this is used for both self-reference and regular CTEs; the
 * possible cost differences are below the threshold of what we could
 * estimate accurately anyway.	Note that the costs of evaluating the
 * referenced CTE query are added into the final plan as initplan costs,
 * and should NOT be counted here.
 */
void cost_ctescan(Path* path, PlannerInfo* root, RelOptInfo* baserel)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost cpu_per_tuple = 0.0;

    /* Should only be applied to base relations that are CTEs */
    AssertEreport(baserel->relid > 0, MOD_OPT, "The relid is invalid when determining the cost of scanning a CTE RTE.");
    AssertEreport(baserel->rtekind == RTE_CTE,
        MOD_OPT,
        "Only common table expr can be supported when determining the cost of scanning a CTE RTE.");

    /* ctescans are never parameterized */
    set_rel_path_rows(path, baserel, NULL);

    /* Charge one CPU tuple cost per row for tuplestore manipulation */
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost;

    /* Add scanning CPU costs */
    startup_cost += baserel->baserestrictcost.startup;
    cpu_per_tuple += u_sess->attr.attr_sql.cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples);

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;
}

/*
 * cost_recursive_union
 *	  Determines and returns the cost of performing a recursive union,
 *	  and also the estimated output size.
 *
 * We are given Plans for the nonrecursive and recursive terms.
 *
 * Note that the arguments and output are Plans, not Paths as in most of
 * the rest of this module.  That's because we don't bother setting up a
 * Path representation for recursive union --- we have only one way to do it.
 */
void cost_recursive_union(Plan* runion, Plan* nrterm, Plan* rterm)
{
    Cost startup_cost;
    Cost total_cost;
    double total_rows;
    double total_global_rows;

    /* We probably have decent estimates for the non-recursive term */
    startup_cost = nrterm->startup_cost;
    total_cost = nrterm->total_cost;
    total_rows = PLAN_LOCAL_ROWS(nrterm);
    total_global_rows = nrterm->plan_rows;

    /*
     * We arbitrarily assume that about 10 recursive iterations will be
     * needed, and that we've managed to get a good fix on the cost and output
     * size of each one of them.  These are mighty shaky assumptions but it's
     * hard to see how to do better.
     */
    total_cost += 10 * rterm->total_cost;
    total_rows += 10 * PLAN_LOCAL_ROWS(rterm);
    total_global_rows += 10 * rterm->plan_rows;

    /*
     * Also charge cpu_tuple_cost per row to account for the costs of
     * manipulating the tuplestores.  (We don't worry about possible
     * spill-to-disk costs.)
     */
    total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * total_rows;

    runion->startup_cost = startup_cost;
    runion->total_cost = total_cost;
    set_plan_rows(runion, total_global_rows, nrterm->multiple);
    runion->plan_width = Max(nrterm->plan_width, rterm->plan_width);
}

/*
 * cost_sort
 *	  Determines and returns the cost of sorting a relation, including
 *	  the cost of reading the input data.
 *
 * If the total volume of data to sort is less than sort_mem, we will do
 * an in-memory sort, which requires no I/O and about t*log2(t) tuple
 * comparisons for t tuples.
 *
 * If the total volume exceeds sort_mem, we switch to a tape-style merge
 * algorithm.  There will still be about t*log2(t) tuple comparisons in
 * total, but we will also need to write and read each tuple once per
 * merge pass.	We expect about ceil(logM(r)) merge passes where r is the
 * number of initial runs formed and M is the merge order used by tuplesort.c.
 * Since the average initial run should be about twice sort_mem, we have
 *      disk traffic = 2 * relsize * ceil(logM(p / (2*sort_mem)))
 *      cpu = comparison_cost * t * log2(t)
 *
 * If the sort is bounded (i.e., only the first k result tuples are needed)
 * and k tuples can fit into sort_mem, we use a heap method that keeps only
 * k tuples in the heap; this will require about t*log2(k) tuple comparisons.
 *
 * The disk traffic is assumed to be 3/4ths sequential and 1/4th random
 * accesses (XXX can't we refine that guess?)
 *
 * By default, we charge two operator evals per tuple comparison, which should
 * be in the right ballpark in most cases.	The caller can tweak this by
 * specifying nonzero comparison_cost; typically that's used for any extra
 * work that has to be done to prepare the inputs to the comparison operators.
 *
 * 'pathkeys' is a list of sort keys
 * 'input_cost' is the total cost for reading the input data
 * 'tuples' is the number of tuples in the relation
 * 'width' is the average tuple width in bytes
 * 'comparison_cost' is the extra cost per comparison, if any
 * 'sort_mem' is the number of kilobytes of work memory allowed for the sort
 * 'limit_tuples' is the bound on the number of output tuples; -1 if no bound
 * 'mem_info' is operator max and min info used by memory control module
 *
 * NOTE: some callers currently pass NIL for pathkeys because they
 * can't conveniently supply the sort keys.  Since this routine doesn't
 * currently do anything with pathkeys anyway, that doesn't matter...
 * but if it ever does, it should react gracefully to lack of key data.
 * (Actually, the thing we'd most likely be interested in is just the number
 * of sort keys, which all callers *could* supply.)
 */
void cost_sort(Path* path, List* pathkeys, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int sort_mem, double limit_tuples, bool col_store, int dop, OpMemInfo* mem_info, bool index_sort)
{
    Cost startup_cost = input_cost;
    Cost run_cost = 0;
    double input_bytes = relation_byte_size(tuples, width, col_store, true, true, index_sort) / SET_DOP(dop);
    double output_bytes;
    double output_tuples;
    long sort_mem_bytes = sort_mem * 1024L / SET_DOP(dop);

    dop = SET_DOP(dop);

    if (!u_sess->attr.attr_sql.enable_sort)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /*
     * We want to be sure the cost of a sort is never estimated as zero, even
     * if passed-in tuple count is zero.  Besides, mustn't do log(0)...
     */
    if (tuples < 2.0) {
        tuples = 2.0;
    }

    /* Include the default cost-per-comparison */
    comparison_cost += 2.0 * u_sess->attr.attr_sql.cpu_operator_cost;

    /* Do we have a useful LIMIT? */
    if (limit_tuples > 0 && limit_tuples < tuples) {
        output_tuples = limit_tuples;
        output_bytes = relation_byte_size(output_tuples, width, col_store, true, true, index_sort);
    } else {
        output_tuples = tuples;
        output_bytes = input_bytes;
    }

    if (output_bytes > sort_mem_bytes) {
        /*
         * CPU costs
         *
         * Assume about N log2 N comparisons
         */
        startup_cost += comparison_cost * tuples * LOG2(tuples);

        /* Disk costs */
        startup_cost += compute_sort_disk_cost(input_bytes, sort_mem_bytes);
    } else {
        if (tuples > 2 * output_tuples || input_bytes > sort_mem_bytes) {
            /*
             * We'll use a bounded heap-sort keeping just K tuples in memory, for
             * a total number of tuple comparisons of N log2 K; but the constant
             * factor is a bit higher than for quicksort.  Tweak it so that the
             * cost curve is continuous at the crossover point.
             */
            startup_cost += comparison_cost * tuples * LOG2(2.0 * output_tuples);
        } else {
            /* We'll use plain quicksort on all the input tuples */
            startup_cost += comparison_cost * tuples * LOG2(tuples);
        }
    }

    if (mem_info != NULL) {
        mem_info->opMem = u_sess->opt_cxt.op_work_mem;
        mem_info->maxMem = output_bytes / 1024L * dop;
        mem_info->minMem = mem_info->maxMem / SORT_MAX_DISK_SIZE;
        mem_info->regressCost = compute_sort_disk_cost(input_bytes, mem_info->minMem);
        /* Special case if array larger than 1G, so we must spill to disk */
        if (output_tuples > (MaxAllocSize / TUPLE_OVERHEAD(true) * dop)) {
            mem_info->maxMem = STATEMENT_MIN_MEM * 1024L * dop;
            mem_info->minMem = Min(mem_info->maxMem, mem_info->minMem);
        }
    }

    /*
     * Also charge a small amount (arbitrarily set equal to operator cost) per
     * extracted tuple.  We don't charge cpu_tuple_cost because a Sort node
     * doesn't do qual-checking or projection, so it has less overhead than
     * most plan nodes.  Note it's correct to use tuples not output_tuples
     * here --- the upper LIMIT will pro-rate the run cost so we'd be double
     * counting the LIMIT otherwise.
     */
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * tuples;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
    path->stream_cost = 0;

    if (!u_sess->attr.attr_sql.enable_sort)
        path->total_cost *=
            (g_instance.cost_cxt.disable_cost_enlarge_factor * g_instance.cost_cxt.disable_cost_enlarge_factor);
}

/*
 * compute_sort_disk_cost
 *	compute disk spill cost of sort operator
 *
 * Parameters:
 *	@in input_bytes: bytes of input relation
 *	@in sort_mem_bytes: work mem of sort operator
 *
 * Returns: estimated disk cost
 */
double compute_sort_disk_cost(double input_bytes, double sort_mem_bytes)
{
    /*
     * We'll have to use a disk-based sort of all the tuples
     */
    double npages = ceil(input_bytes / BLCKSZ);
    double nruns = (input_bytes / sort_mem_bytes) * 0.5;
    double mergeorder = tuplesort_merge_order(sort_mem_bytes);
    double log_runs;
    double npageaccesses;

    /* Compute logM(r) as log(r) / log(M) */
    if (nruns > mergeorder) {
        log_runs = ceil(log(nruns) / log(mergeorder));
    } else {
        log_runs = 1.0;
    }
    npageaccesses = 2.0 * npages * log_runs;
    /* Assume 3/4ths of accesses are sequential, 1/4th are not */
    return npageaccesses * (u_sess->attr.attr_sql.seq_page_cost * 0.75 + u_sess->attr.attr_sql.random_page_cost * 0.25);
}

/*
 * cost_merge_append
 *	  Determines and returns the cost of a MergeAppend node.
 *
 * MergeAppend merges several pre-sorted input streams, using a heap that
 * at any given instant holds the next tuple from each stream.	If there
 * are N streams, we need about N*log2(N) tuple comparisons to construct
 * the heap at startup, and then for each output tuple, about log2(N)
 * comparisons to delete the top heap entry and another log2(N) comparisons
 * to insert its successor from the same stream.
 *
 * (The effective value of N will drop once some of the input streams are
 * exhausted, but it seems unlikely to be worth trying to account for that.)
 *
 * The heap is never spilled to disk, since we assume N is not very large.
 * So this is much simpler than cost_sort.
 *
 * As in cost_sort, we charge two operator evals per tuple comparison.
 *
 * 'pathkeys' is a list of sort keys
 * 'n_streams' is the number of input streams
 * 'input_startup_cost' is the sum of the input streams' startup costs
 * 'input_total_cost' is the sum of the input streams' total costs
 * 'tuples' is the number of tuples in all the streams
 */
void cost_merge_append(Path* path, PlannerInfo* root, List* pathkeys, int n_streams, Cost input_startup_cost,
    Cost input_total_cost, double tuples)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost comparison_cost;
    double N;
    double logN;

    /*
     * Avoid log(0)...
     */
    N = (n_streams < 2) ? 2.0 : (double)n_streams;
    logN = LOG2(N);

    /* Assumed cost per tuple comparison */
    comparison_cost = 2.0 * u_sess->attr.attr_sql.cpu_operator_cost;

    /* Heap creation cost */
    startup_cost += comparison_cost * N * logN;

    /* Per-tuple heap maintenance cost */
    run_cost += tuples * comparison_cost * 2.0 * logN;

    /*
     * Also charge a small amount (arbitrarily set equal to operator cost) per
     * extracted tuple.  We don't charge cpu_tuple_cost because a MergeAppend
     * node doesn't do qual-checking or projection, so it has less overhead
     * than most plan nodes.
     */
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * tuples;

    path->startup_cost = startup_cost + input_startup_cost;
    path->total_cost = startup_cost + run_cost + input_total_cost;
}

/*
 * cost_material
 *	  Determines and returns the cost of materializing a relation, including
 *	  the cost of reading the input data.
 *
 * If the total volume of data to materialize exceeds work_mem, we will need
 * to write it to disk, so the cost is much higher in that case.
 *
 * Note that here we are estimating the costs for the first scan of the
 * relation, so the materialization is all overhead --- any savings will
 * occur only on rescan, which is estimated in cost_rescan.
 */
void cost_material(Path* path, Cost input_startup_cost, Cost input_total_cost, double tuples, int width)
{
    Cost startup_cost = input_startup_cost;
    Cost run_cost = input_total_cost - input_startup_cost;
    int dop = SET_DOP(path->dop);
    if (dop > 1) {
        tuples = tuples / dop;
    }

    double nbytes = relation_byte_size(tuples, width, false, true, false);
    long work_mem_bytes = u_sess->opt_cxt.op_work_mem * 1024L / dop;

    /*
     * Whether spilling or not, charge 2x cpu_operator_cost per tuple to
     * reflect bookkeeping overhead.  (This rate must be more than what
     * cost_rescan charges for materialize, ie, cpu_operator_cost per tuple;
     * if it is exactly the same then there will be a cost tie between
     * nestloop with A outer, materialized B inner and nestloop with B outer,
     * materialized A inner.  The extra cost ensures we'll prefer
     * materializing the smaller rel.)	Note that this is normally a good deal
     * less than cpu_tuple_cost; which is OK because a Material plan node
     * doesn't do qual-checking or projection, so it's got less overhead than
     * most plan nodes.
     */
    run_cost += 2 * u_sess->attr.attr_sql.cpu_operator_cost * tuples;

    /*
     * If we will spill to disk, charge at the rate of seq_page_cost per page.
     * This cost is assumed to be evenly spread through the plan run phase,
     * which isn't exactly accurate but our cost model doesn't allow for
     * nonuniform costs within the run phase.
     */
    if (nbytes > work_mem_bytes) {
        double npages = ceil(nbytes / BLCKSZ);

        run_cost += u_sess->attr.attr_sql.seq_page_cost * npages;
    }

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}

/*
 * Decide sonic hashagg routine or not.
 * Similar with the same function in vsonichashagg.cpp
 */
bool isSonicHashAggPlanEnable(PlannerInfo* root, AggStrategy aggstrategy, int numGroupCols)
{
    if (!(root->glob->vectorized) || aggstrategy != AGG_HASHED || !u_sess->attr.attr_sql.enable_sonic_hashagg)
        return false;

    /*
     * Get the target list.
     * Including all the corresponding columns in group by clause, whether or not it appears in target list.
     */
    List* tleList = root->parse->targetList;

    /* Get hash key in group by clause. */
    List* sgClauses = root->parse->groupClause;
    ListCell* lc = NULL;

    Oid hashtype = 0;
    foreach (lc, sgClauses) {
        SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc);
        TargetEntry* tle = get_sortgroupclause_tle(sortcl, tleList, false);
        if (tle == NULL)
            return false;

        switch (nodeTag(tle->expr)) {
            case T_Var: {
                Var* var = (Var*)(tle->expr);
                hashtype = var->vartype;
            } break;
            case T_FuncExpr: {
                FuncExpr* funcexpr = (FuncExpr*)(tle->expr);
                hashtype = funcexpr->funcresulttype;
            } break;
            default:
                break;
        }

        switch (hashtype) {
            case CHAROID:
            case BPCHAROID:
            case INT1OID:
            case INT2OID:
            case INT4OID:
            case INT8OID:
            case FLOAT4OID:
            case FLOAT8OID:
            case TIMESTAMPOID:
            case DATEOID:
            case TEXTOID:
            case VARCHAROID:
                break;
            default:
                return false;
        }
    }

    /*
     * Aggregate function only support sum(), avg() function for int4, int8 and numeric tyep.
     * Loop over all the targetlist and check aggref.
     */
    foreach (lc, tleList) {
        TargetEntry* tre = (TargetEntry*)lfirst(lc);
        switch (nodeTag(tre->expr)) {
            case T_Aggref: {
                Aggref* aggref = (Aggref*)tre->expr;

                if (!isAggrefSonicEnable(aggref->aggfnoid))
                    return false;

                /* count(*) has no args */
                if (aggref->aggfnoid == COUNTOID || aggref->aggfnoid == ANYCOUNTOID)
                    continue;

                Expr* refexpr = (Expr*)linitial(aggref->args);
                /* We only support simple expression cases */
                if (!isExprSonicEnable(refexpr))
                    return false;
            } break;
            case T_FuncExpr: {
                return false;
            }
            case T_Var:
            case T_Const:
                break;
            default:
                return false;
        }
    }
    return true;
}

/*
 * For operator HashAgg, compute appropriate size for hashtable given the estimated size of the
 * columns to be hashed (number of rows).
 */
double estimate_hashagg_size(Path* path, PlannerInfo* root, AggStrategy aggstrategy, int numGroupCols, double numGroups,
    double input_tuples, int input_width, int hash_entry_size)
{
    double hash_table_size;
    if (hash_entry_size == 0)
        hash_entry_size = alloc_trunk_size(input_width) + HASH_ENTRY_OVERHEAD;
    hash_table_size = (double)(hash_entry_size * numGroups) / 1024L;
    return hash_table_size;
}

/*
 * cost_agg
 *		Determines and returns the cost of performing an Agg plan node,
 *		including the cost of its input.
 *
 * aggcosts can be NULL when there are no actual aggregate functions (i.e.,
 * we are using a hashed Agg node just to do grouping).
 *
 * Note: when aggstrategy == AGG_SORTED, caller must ensure that input costs
 * are for appropriately-sorted input.
 */
void cost_agg(Path* path, PlannerInfo* root, AggStrategy aggstrategy, const AggClauseCosts* aggcosts, int numGroupCols,
    double numGroups, Cost input_startup_cost, Cost input_total_cost, double input_tuples, int input_width,
    int hash_entry_size, int dop, OpMemInfo* mem_info)
{
    double output_tuples;
    Cost startup_cost;
    Cost total_cost;
    AggClauseCosts dummy_aggcosts;
    double hllagg_size = 0;

    /* Use all-zero per-aggregate costs if NULL is passed */
    if (aggcosts == NULL) {
        AssertEreport(aggstrategy == AGG_HASHED,
            MOD_OPT,
            "Only support Hashed aggstrategy"
            "when determining the cost of performing an Agg plan node.");

        errno_t errorno = EOK;
        errorno = memset_s(&dummy_aggcosts, sizeof(AggClauseCosts), 0, sizeof(AggClauseCosts));
        securec_check(errorno, "\0", "\0");
        aggcosts = &dummy_aggcosts;
    }

    hllagg_size = estimate_hllagg_size(numGroups, root->parse->targetList);
    dop = SET_DOP(dop);
    /*
     * The transCost.per_tuple component of aggcosts should be charged once
     * per input tuple, corresponding to the costs of evaluating the aggregate
     * transfns and their input expressions (with any startup cost of course
     * charged but once).  The finalCost component is charged once per output
     * tuple, corresponding to the costs of evaluating the finalfns.
     *
     * If we are grouping, we charge an additional u_sess->attr.attr_sql.cpu_operator_cost per
     * grouping column per input tuple for grouping comparisons.
     *
     * We will produce a single output tuple if not grouping, and a tuple per
     * group otherwise.  We charge u_sess->attr.attr_sql.cpu_tuple_cost for each output tuple.
     *
     * Note: in this cost model, AGG_SORTED and AGG_HASHED have exactly the
     * same total CPU cost, but AGG_SORTED has lower startup cost.	If the
     * input path is already sorted appropriately, AGG_SORTED should be
     * preferred (since it has no risk of memory overflow).  This will happen
     * as long as the computed total costs are indeed exactly equal --- but if
     * there's roundoff error we might do the wrong thing.  So be sure that
     * the computations below form the same intermediate values in the same
     * order.
     */
    if (aggstrategy == AGG_PLAIN) {
        startup_cost = input_total_cost;
        startup_cost += aggcosts->transCost.startup;
        startup_cost += aggcosts->transCost.per_tuple * input_tuples;
        startup_cost += aggcosts->finalCost;
        /* we aren't grouping */
        total_cost = startup_cost + u_sess->attr.attr_sql.cpu_tuple_cost;
        output_tuples = 1;
    } else if (aggstrategy == AGG_SORTED) {
        /* Here we are able to deliver output on-the-fly */
        startup_cost = input_startup_cost;
        total_cost = input_total_cost;
        /* calcs phrased this way to match HASHED case, see note above */
        total_cost += aggcosts->transCost.startup;
        total_cost += aggcosts->transCost.per_tuple * input_tuples;
        total_cost += (u_sess->attr.attr_sql.cpu_operator_cost * numGroupCols) * input_tuples;
        total_cost += aggcosts->finalCost * numGroups;
        total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * numGroups;
        output_tuples = numGroups;
    } else {
        bool spill_disk = false;
        double hash_table_size;

        /* must be AGG_HASHED */
        startup_cost = input_total_cost;
        startup_cost += aggcosts->transCost.startup;
        startup_cost += aggcosts->transCost.per_tuple * input_tuples / dop;
        startup_cost += (u_sess->attr.attr_sql.cpu_operator_cost * numGroupCols) * input_tuples / dop;
        total_cost = startup_cost;
        total_cost += aggcosts->finalCost * numGroups / dop;
        total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * numGroups / dop;
        output_tuples = numGroups;

        /* Get hash table size and estimate mem_info. */
        hash_table_size = estimate_hashagg_size(
            path, root, aggstrategy, numGroupCols, numGroups, input_tuples, input_width, hash_entry_size);

        /* one more step estimate for hll */
        hash_table_size += hllagg_size;
        if (hash_table_size < 0)
            hash_table_size = (double)LONG_MAX;
        spill_disk = (hash_table_size > (double)u_sess->opt_cxt.op_work_mem);

        if (spill_disk) {
            const double disk_ratio = 1 - u_sess->opt_cxt.op_work_mem / hash_table_size;
            double disk_pages = ceil(page_size(input_tuples, input_width) * disk_ratio);
            double one_disk_io_cost = u_sess->attr.attr_sql.seq_page_cost * disk_pages;
            double disk_hash_cost = u_sess->attr.attr_sql.cpu_operator_cost * numGroupCols * input_tuples * disk_ratio;

            startup_cost += disk_hash_cost + one_disk_io_cost;   /* hash, write cost counted */
            total_cost += disk_hash_cost + 2 * one_disk_io_cost; /* hash, write, read cost counted */
        }
        if (mem_info != NULL) {
            double disk_pages = ceil(page_size(input_tuples, input_width));
            double one_disk_io_cost = u_sess->attr.attr_sql.seq_page_cost * disk_pages;
            double disk_hash_cost = u_sess->attr.attr_sql.cpu_operator_cost * numGroupCols * input_tuples;

            mem_info->opMem = u_sess->opt_cxt.op_work_mem;
            mem_info->maxMem = hash_table_size;
            mem_info->minMem = mem_info->maxMem / HASH_MAX_DISK_SIZE;
            mem_info->regressCost = (disk_hash_cost + 2 * one_disk_io_cost);
        }
    }

    path->rows = get_global_rows(output_tuples, 1.0, ng_get_dest_num_data_nodes(path));
    path->multiple = 1.0;
    path->startup_cost = startup_cost;
    path->total_cost = total_cost;
}

/*
 * cost_windowagg
 *		Determines and returns the cost of performing a WindowAgg plan node,
 *		including the cost of its input.
 *
 * Input is assumed already properly sorted.
 */
void cost_windowagg(Path* path, PlannerInfo* root, List* windowFuncs, int numPartCols, int numOrderCols,
    Cost input_startup_cost, Cost input_total_cost, double input_tuples)
{
    Cost startup_cost;
    Cost total_cost;
    ListCell* lc = NULL;

    startup_cost = input_startup_cost;
    total_cost = input_total_cost;

    /*
     * Window functions are assumed to cost their stated execution cost, plus
     * the cost of evaluating their input expressions, per tuple.  Since they
     * may in fact evaluate their inputs at multiple rows during each cycle,
     * this could be a drastic underestimate; but without a way to know how
     * many rows the window function will fetch, it's hard to do better.  In
     * any case, it's a good estimate for all the built-in window functions,
     * so we'll just do this for now.
     */
    foreach (lc, windowFuncs) {
        WindowFunc* wfunc = (WindowFunc*)lfirst(lc);
        Cost wfunccost;
        QualCost argcosts;

        AssertEreport(IsA(wfunc, WindowFunc),
            MOD_OPT,
            "The nodeTag of wfunc is not T_WindowFunc"
            "when determining the cost of performing a WindowAgg plan node.");
        wfunccost = get_func_cost(wfunc->winfnoid) * u_sess->attr.attr_sql.cpu_operator_cost;

        /* also add the input expressions' cost to per-input-row costs */
        cost_qual_eval_node(&argcosts, (Node*)wfunc->args, root);
        startup_cost += argcosts.startup;
        wfunccost += argcosts.per_tuple;

        total_cost += wfunccost * input_tuples;
    }

    /*
     * We also charge cpu_operator_cost per grouping column per tuple for
     * grouping comparisons, plus cpu_tuple_cost per tuple for general
     * overhead.
     *
     * XXX this neglects costs of spooling the data to disk when it overflows
     * work_mem.  Sooner or later that should get accounted for.
     */
    total_cost += u_sess->attr.attr_sql.cpu_operator_cost * (numPartCols + numOrderCols) * input_tuples;
    total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * input_tuples;

    path->startup_cost = startup_cost;
    path->total_cost = total_cost;
}

/*
 * cost_group
 *		Determines and returns the cost of performing a Group plan node,
 *		including the cost of its input.
 *
 * Note: caller must ensure that input costs are for appropriately-sorted
 * input.
 */
void cost_group(Path* path, PlannerInfo* root, int numGroupCols, double numGroups, Cost input_startup_cost,
    Cost input_total_cost, double input_tuples)
{
    Cost startup_cost;
    Cost total_cost;

    startup_cost = input_startup_cost;
    total_cost = input_total_cost;

    /*
     * Charge one cpu_operator_cost per comparison per input tuple. We assume
     * all columns get compared at most of the tuples.
     */
    total_cost += u_sess->attr.attr_sql.cpu_operator_cost * input_tuples * numGroupCols;

    path->rows = get_global_rows(numGroups, 1.0, ng_get_dest_num_data_nodes(path));
    path->multiple = 1.0;
    path->startup_cost = startup_cost;
    path->total_cost = total_cost;
}

double adjust_limit_row_count(double lefttree_rows)
{
    if (u_sess->attr.attr_sql.default_limit_rows < 0) { /* use percentage adjustment */
        return -(lefttree_rows * u_sess->attr.attr_sql.default_limit_rows / 100);
    } else { /* use direct adjustment */
        return Min(u_sess->attr.attr_sql.default_limit_rows, lefttree_rows);
    }
}

/*
 * @Description: Calculate cost and adjust output rows for limit node.
 *
 * @param[IN] plan: limit plan
 * @param[IN] lefttree: subplan
 * @return void
 */
void cost_limit(Plan* plan, Plan* lefttree, int64 offset_est, int64 count_est)
{
    /*
     * Adjust the output rows count and costs according to the offset/limit.
     * This is only a cosmetic issue if we are at top level, but if we are
     * building a subquery then it's important to report correct info to the
     * outer planner.
     *
     * When the offset or count couldn't be estimated, use default_limit_rows.
     * Percentage adjustment is used When default_limit_rows is negative:
     *  e.g. 10% of the estimated number of rows emitted from the subplan is
     *       used if default_limit_rows is -0.1.
     * Direct adjustment when positive:
     *  e.g. 100 is used if default_limit_rows is 100.
     */
    if (offset_est != 0) {
        double offset_rows;

        if (offset_est > 0) {
            offset_rows = (double)offset_est;
            if (is_replicated_plan(lefttree) && is_execute_on_datanodes(lefttree)) {
                offset_rows *= ng_get_dest_num_data_nodes(lefttree);
            }
        } else
            offset_rows = clamp_row_est(lefttree->plan_rows * 0.10);
        if (offset_rows > lefttree->plan_rows)
            offset_rows = lefttree->plan_rows;
        if (plan->plan_rows > 0)
            plan->startup_cost += (plan->total_cost - plan->startup_cost) * offset_rows / plan->plan_rows;
        plan->plan_rows -= offset_rows;
        if (plan->plan_rows < 1)
            plan->plan_rows = 1;
    }

    if (count_est != 0) {
        double count_rows;

        if (count_est > 0) {
            count_rows = (double)count_est;
            if (is_execute_on_datanodes(lefttree)) {
                count_rows *= ng_get_dest_num_data_nodes(lefttree);
            }
        } else
            count_rows = clamp_row_est(adjust_limit_row_count(lefttree->plan_rows));
        if (count_rows > plan->plan_rows)
            count_rows = plan->plan_rows;
        if (plan->plan_rows > 0)
            plan->total_cost =
                plan->startup_cost + (plan->total_cost - plan->startup_cost) * count_rows / plan->plan_rows;
        plan->plan_rows = count_rows;
        if (plan->plan_rows < 1)
            plan->plan_rows = 1;
    }
}

/*
 * initial_cost_nestloop
 *	  Preliminary estimate of the cost of a nestloop join path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.  If we are unable to eliminate the proposed path from
 * consideration using the lower bounds, final_cost_nestloop will be called
 * to obtain the final estimates.
 *
 * The exact division of labor between this function and final_cost_nestloop
 * is private to them, and represents a tradeoff between speed of the initial
 * estimate and getting a tight lower bound.  We choose to not examine the
 * join quals here, since that's by far the most expensive part of the
 * calculations.  The end result is that CPU-cost considerations must be
 * left for the second phase.
 *
 * 'workspace' is to be filled with startup_cost, total_cost, and perhaps
 *		other data to be used by final_cost_nestloop
 * 'jointype' is the type of join to be performed
 * 'outer_path' is the outer input to the join
 * 'inner_path' is the inner input to the join
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 */
void initial_cost_nestloop(PlannerInfo* root, JoinCostWorkspace* workspace, JoinType jointype, Path* outer_path,
    Path* inner_path, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, int dop)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double outer_path_rows = PATH_LOCAL_ROWS(outer_path) / dop;
    Cost inner_rescan_start_cost;
    Cost inner_rescan_total_cost;
    Cost inner_run_cost;
    Cost inner_rescan_run_cost;
    errno_t rc = 0;

    rc = memset_s(&workspace->inner_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    /* estimate costs to rescan the inner relation */
    cost_rescan(root, inner_path, &inner_rescan_start_cost, &inner_rescan_total_cost, &workspace->inner_mem_info);

    /* cost of source data */
    /*
     * NOTE: clearly, we must pay both outer and inner paths' startup_cost
     * before we can start returning tuples, so the join's startup cost is
     * their sum.  We'll also pay the inner path's rescan startup cost
     * multiple times.
     */
    startup_cost += outer_path->startup_cost + inner_path->startup_cost;
    run_cost += outer_path->total_cost - outer_path->startup_cost;
    if (outer_path_rows > 1)
        run_cost += (outer_path_rows - 1) * inner_rescan_start_cost;

    inner_run_cost = inner_path->total_cost - inner_path->startup_cost;
    inner_rescan_run_cost = inner_rescan_total_cost - inner_rescan_start_cost;

    if (jointype == JOIN_SEMI || jointype == JOIN_ANTI) {
        double outer_matched_rows;
        Selectivity inner_scan_frac;

        /*
         * SEMI or ANTI join: executor will stop after first match.
         *
         * For an outer-rel row that has at least one match, we can expect the
         * inner scan to stop after a fraction 1/(match_count+1) of the inner
         * rows, if the matches are evenly distributed.  Since they probably
         * aren't quite evenly distributed, we apply a fuzz factor of 2.0 to
         * that fraction.  (If we used a larger fuzz factor, we'd have to
         * clamp inner_scan_frac to at most 1.0; but since match_count is at
         * least 1, no such clamp is needed now.)
         *
         * A complicating factor is that rescans may be cheaper than first
         * scans.  If we never scan all the way to the end of the inner rel,
         * it might be (depending on the plan type) that we'd never pay the
         * whole inner first-scan run cost.  However it is difficult to
         * estimate whether that will happen, so be conservative and always
         * charge the whole first-scan cost once.
         */
        run_cost += inner_run_cost;

        outer_matched_rows = rint(outer_path_rows * semifactors->outer_match_frac);
        inner_scan_frac = 2.0 / (semifactors->match_count + 1.0);

        /* Add inner run cost for additional outer tuples having matches */
        if (outer_matched_rows > 1)
            run_cost += (outer_matched_rows - 1) * inner_rescan_run_cost * inner_scan_frac;

        /*
         * The cost of processing unmatched rows varies depending on the
         * details of the joinclauses, so we leave that part for later.
         */
        /* Save private data for final_cost_nestloop */
        workspace->outer_matched_rows = outer_matched_rows;
        workspace->inner_scan_frac = inner_scan_frac;
        workspace->inner_mem_info.regressCost *= Max(outer_matched_rows, 1.0);
    } else {
        /* Normal case; we'll scan whole input rel for each outer row */
        run_cost += inner_run_cost;
        if (outer_path_rows > 1)
            run_cost += (outer_path_rows - 1) * inner_rescan_run_cost;
        workspace->inner_mem_info.regressCost *= Max(outer_path_rows, 1.0);
    }

    /* CPU costs left for later */
    /* Public result fields */
    workspace->startup_cost = startup_cost;
    workspace->total_cost = startup_cost + run_cost;
    /* Save private data for final_cost_nestloop */
    workspace->run_cost = run_cost;
    workspace->inner_rescan_run_cost = inner_rescan_run_cost;
    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Initial nestloop cost: startup_cost: %lf, total_cost: %lf",
                workspace->startup_cost,
                workspace->total_cost)));
}

/*
 * final_cost_nestloop
 *	  Final estimate of the cost and result size of a nestloop join path.
 *
 * 'path' is already filled in except for the rows and cost fields
 * 'workspace' is the result from initial_cost_nestloop
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if path->jointype is SEMI or ANTI
 */
void final_cost_nestloop(PlannerInfo* root, NestPath* path, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo,
    SemiAntiJoinFactors* semifactors, bool hasalternative, int dop)
{
    Path* outer_path = path->outerjoinpath;
    Path* inner_path = path->innerjoinpath;
    double outer_path_rows = PATH_LOCAL_ROWS(outer_path) / dop;
    double inner_path_rows = PATH_LOCAL_ROWS(inner_path) / dop;
    Cost startup_cost = workspace->startup_cost;
    Cost run_cost = workspace->run_cost;
    Cost inner_rescan_run_cost = workspace->inner_rescan_run_cost;
    Cost cpu_per_tuple = 0.0;
    QualCost restrict_qual_cost;
    double ntuples;
    bool ppi_used = false;
    bool method_enabled = true;

    /* Mark the path with the correct row estimate */
    set_rel_path_rows(&path->path, path->path.parent, path->path.param_info);

    /*
     * If inner_path or outer_path is EC functioinScan without stream,
     * we should set the multiple particularly.
     */
    set_joinpath_multiple_for_EC(root, &path->path, outer_path, inner_path);

    /*
     * We could include g_instance.cost_cxt.disable_cost in the preliminary estimate, but that
     * would amount to optimizing for the case where the join method is
     * disabled, which doesn't seem like the way to bet.
     */
    if (inner_path->param_info != NULL && outer_path->parent != NULL)
        ppi_used = bms_overlap(inner_path->param_info->ppi_req_outer, outer_path->parent->relids);
    /* If ppi is used, we use enable_index_nestloop to judge whether use this path */
    method_enabled = ppi_used ? u_sess->attr.attr_sql.enable_index_nestloop
                              : (u_sess->attr.attr_sql.enable_nestloop || !hasalternative);
    if (!method_enabled)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /* cost of source data */
    if (path->jointype == JOIN_SEMI || path->jointype == JOIN_ANTI) {
        double outer_matched_rows = workspace->outer_matched_rows;
        Selectivity inner_scan_frac = workspace->inner_scan_frac;

        /*
         * SEMI or ANTI join: executor will stop after first match.
         */
        /* Compute number of tuples processed (not number emitted!) */
        ntuples = outer_matched_rows * inner_path_rows * inner_scan_frac;

        /*
         * For unmatched outer-rel rows, there are two cases.  If the inner
         * path is an indexscan using all the joinquals as indexquals, then an
         * unmatched row results in an indexscan returning no rows, which is
         * probably quite cheap.  We estimate this case as the same cost to
         * return the first tuple of a nonempty scan.  Otherwise, the executor
         * will have to scan the whole inner rel; not so cheap.
         */
        if (has_indexed_join_quals(path)) {
            run_cost += (outer_path_rows - outer_matched_rows) * inner_rescan_run_cost / inner_path_rows;

            /*
             * We won't be evaluating any quals at all for these rows, so
             * don't add them to ntuples.
             */
        } else {
            run_cost += (outer_path_rows - outer_matched_rows) * inner_rescan_run_cost;
            ntuples += (outer_path_rows - outer_matched_rows) * inner_path_rows;
        }
    } else {
        /* Normal-case source costs were included in preliminary estimate */
        /* Compute number of tuples processed (not number emitted!) */
        ntuples = outer_path_rows * inner_path_rows;
    }

    /* CPU costs */
    cost_qual_eval(&restrict_qual_cost, path->joinrestrictinfo, root);
    startup_cost += restrict_qual_cost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + restrict_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    path->path.startup_cost = startup_cost;
    path->path.total_cost = startup_cost + run_cost;
    path->path.stream_cost = outer_path->stream_cost;

    if (!method_enabled)
        path->path.total_cost *= g_instance.cost_cxt.disable_cost_enlarge_factor;

    if (path->innerjoinpath->pathtype == T_Material)
        copy_mem_info(&((MaterialPath*)path->innerjoinpath)->mem_info, &workspace->inner_mem_info);

    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("final cost nest loop: stream_cost: %lf, startup_cost: %lf, total_cost: %lf",
                path->path.stream_cost,
                path->path.startup_cost,
                path->path.total_cost)));
}

/*
 * initial_cost_mergejoin
 *	  Preliminary estimate of the cost of a mergejoin path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.  If we are unable to eliminate the proposed path from
 * consideration using the lower bounds, final_cost_mergejoin will be called
 * to obtain the final estimates.
 *
 * The exact division of labor between this function and final_cost_mergejoin
 * is private to them, and represents a tradeoff between speed of the initial
 * estimate and getting a tight lower bound.  We choose to not examine the
 * join quals here, except for obtaining the scan selectivity estimate which
 * is really essential (but fortunately, use of caching keeps the cost of
 * getting that down to something reasonable).
 * We also assume that cost_sort is cheap enough to use here.
 *
 * 'workspace' is to be filled with startup_cost, total_cost, and perhaps
 *		other data to be used by final_cost_mergejoin
 * 'jointype' is the type of join to be performed
 * 'mergeclauses' is the list of joinclauses to be used as merge clauses
 * 'outer_path' is the outer input to the join
 * 'inner_path' is the inner input to the join
 * 'outersortkeys' is the list of sort keys for the outer path
 * 'innersortkeys' is the list of sort keys for the inner path
 * 'sjinfo' is extra info about the join for selectivity estimation
 *
 * Note: outersortkeys and innersortkeys should be NIL if no explicit
 * sort is needed because the respective source path is already ordered.
 */
void initial_cost_mergejoin(PlannerInfo* root, JoinCostWorkspace* workspace, JoinType jointype, List* mergeclauses,
    Path* outer_path, Path* inner_path, List* outersortkeys, List* innersortkeys, SpecialJoinInfo* sjinfo)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double outer_path_rows = PATH_LOCAL_ROWS(outer_path);
    double inner_path_rows = PATH_LOCAL_ROWS(inner_path);
    Cost inner_run_cost;
    double outer_rows, inner_rows, outer_skip_rows, inner_skip_rows;
    Selectivity outerstartsel, outerendsel, innerstartsel, innerendsel;
    Path sort_path; /* dummy for result of cost_sort */
    errno_t rc = 0;

    rc = memset_s(&workspace->outer_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&workspace->inner_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    /* Protect some assumptions below that rowcounts aren't zero or NaN */
    if (outer_path_rows <= 0 || isnan(outer_path_rows))
        outer_path_rows = 1;
    if (inner_path_rows <= 0 || isnan(inner_path_rows))
        inner_path_rows = 1;

    /*
     * A merge join will stop as soon as it exhausts either input stream
     * (unless it's an outer join, in which case the outer side has to be
     * scanned all the way anyway).  Estimate fraction of the left and right
     * inputs that will actually need to be scanned.  Likewise, we can
     * estimate the number of rows that will be skipped before the first join
     * pair is found, which should be factored into startup cost. We use only
     * the first (most significant) merge clause for this purpose. Since
     * mergejoinscansel() is a fairly expensive computation, we cache the
     * results in the merge clause RestrictInfo.
     */
    if (mergeclauses != NIL && jointype != JOIN_FULL) {
        RestrictInfo* firstclause = (RestrictInfo*)linitial(mergeclauses);
        List* opathkeys = NIL;
        List* ipathkeys = NIL;
        PathKey* opathkey = NULL;
        PathKey* ipathkey = NULL;
        MergeScanSelCache* cache = NULL;

        /* Get the input pathkeys to determine the sort-order details */
        opathkeys = outersortkeys ? outersortkeys : outer_path->pathkeys;
        ipathkeys = innersortkeys ? innersortkeys : inner_path->pathkeys;
        AssertEreport(opathkeys != NIL,
            MOD_OPT,
            "The outer pathkeys is null when determining the cost of performing a mergejoin path.");
        AssertEreport(ipathkeys != NIL,
            MOD_OPT,
            "The inner pathkeys is null when determining the cost of performing a mergejoin path.");

        opathkey = (PathKey*)linitial(opathkeys);
        ipathkey = (PathKey*)linitial(ipathkeys);
        /* debugging check */
        if (!OpFamilyEquals(opathkey->pk_opfamily, ipathkey->pk_opfamily) ||
            opathkey->pk_eclass->ec_collation != ipathkey->pk_eclass->ec_collation ||
            opathkey->pk_strategy != ipathkey->pk_strategy || opathkey->pk_nulls_first != ipathkey->pk_nulls_first)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("left and right pathkeys do not match in mergejoin when initlize cost")));

        /* Get the selectivity with caching */
        cache = cached_scansel(root, firstclause, opathkey);

        if (bms_is_subset(firstclause->left_relids, outer_path->parent->relids)) {
            /* left side of clause is outer */
            outerstartsel = cache->leftstartsel;
            outerendsel = cache->leftendsel;
            innerstartsel = cache->rightstartsel;
            innerendsel = cache->rightendsel;
        } else {
            /* left side of clause is inner */
            outerstartsel = cache->rightstartsel;
            outerendsel = cache->rightendsel;
            innerstartsel = cache->leftstartsel;
            innerendsel = cache->leftendsel;
        }
        if (jointype == JOIN_LEFT || jointype == JOIN_LEFT_ANTI_FULL || jointype == JOIN_ANTI) {
            outerstartsel = 0.0;
            outerendsel = 1.0;
        } else if (jointype == JOIN_RIGHT) {
            innerstartsel = 0.0;
            innerendsel = 1.0;
        }
        /* jointype should not be JOIN_RIGHT_ANTI_FULL,
         * because JOIN_RIGHT_ANTI_FULL can not create a mergejoin plan.
         */
        AssertEreport(jointype != JOIN_RIGHT_ANTI_FULL,
            MOD_OPT,
            "The mergejoin plan with JOIN_RIGHT_ANTI_FULL is not allowed."
            "when determining the cost of performing a mergejoin path.");
    } else {
        /* cope with clauseless or full mergejoin */
        outerstartsel = innerstartsel = 0.0;
        outerendsel = innerendsel = 1.0;
    }

    /*
     * Convert selectivities to row counts.  We force outer_rows and
     * inner_rows to be at least 1, but the skip_rows estimates can be zero.
     */
    outer_skip_rows = rint(outer_path_rows * outerstartsel);
    inner_skip_rows = rint(inner_path_rows * innerstartsel);
    outer_rows = clamp_row_est(outer_path_rows * outerendsel);
    inner_rows = clamp_row_est(inner_path_rows * innerendsel);

    AssertEreport(outer_skip_rows <= outer_rows,
        MOD_OPT,
        "The estimated skip rows is larger than rounding outer rows which avoid possible divide-by-zero "
        "when determining the cost of performing a mergejoin path.");
    AssertEreport(inner_skip_rows <= inner_rows,
        MOD_OPT,
        "The estimated skip rows is larger than rounding inner rows which avoid possible divide-by-zero"
        "when determining the cost of performing a mergejoin path.");

    /*
     * Readjust scan selectivities to account for above rounding.  This is
     * normally an insignificant effect, but when there are only a few rows in
     * the inputs, failing to do this makes for a large percentage error.
     */
    outerstartsel = outer_skip_rows / outer_path_rows;
    innerstartsel = inner_skip_rows / inner_path_rows;
    outerendsel = outer_rows / outer_path_rows;
    innerendsel = inner_rows / inner_path_rows;

    AssertEreport(outerstartsel <= outerendsel,
        MOD_OPT,
        "The selectivities corresponding to estimated skip rows is larger than that of above rounding outer rows"
        "when determining the cost of performing a mergejoin path.");
    AssertEreport(innerstartsel <= innerendsel,
        MOD_OPT,
        "The selectivities corresponding to estimated skip rows is larger than that of above rounding inner rows"
        "when determining the cost of performing a mergejoin path.");

    /* cost of source data */
    if (outersortkeys || IsA(outer_path, StreamPath)) { /* do we need to sort outer? */
        int outer_width = get_path_actual_total_width(outer_path, root->glob->vectorized, OP_SORT);

        cost_sort(&sort_path,
            outersortkeys,
            outer_path->total_cost,
            outer_path_rows,
            outer_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized,
            1,
            &workspace->outer_mem_info);
        startup_cost += sort_path.startup_cost;
        startup_cost += (sort_path.total_cost - sort_path.startup_cost) * outerstartsel;
        run_cost += (sort_path.total_cost - sort_path.startup_cost) * (outerendsel - outerstartsel);
    } else {
        startup_cost += outer_path->startup_cost;
        startup_cost += (outer_path->total_cost - outer_path->startup_cost) * outerstartsel;
        run_cost += (outer_path->total_cost - outer_path->startup_cost) * (outerendsel - outerstartsel);
    }

    if (innersortkeys || IsA(inner_path, StreamPath)) { /* do we need to sort inner? */       
        int inner_width = get_path_actual_total_width(inner_path, root->glob->vectorized, OP_SORT);

        cost_sort(&sort_path,
            innersortkeys,
            inner_path->total_cost,
            inner_path_rows,
            inner_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized,
            1,
            &workspace->inner_mem_info);
        startup_cost += sort_path.startup_cost;
        startup_cost += (sort_path.total_cost - sort_path.startup_cost) * innerstartsel;
        inner_run_cost = (sort_path.total_cost - sort_path.startup_cost) * (innerendsel - innerstartsel);
    } else {
        startup_cost += inner_path->startup_cost;
        startup_cost += (inner_path->total_cost - inner_path->startup_cost) * innerstartsel;
        inner_run_cost = (inner_path->total_cost - inner_path->startup_cost) * (innerendsel - innerstartsel);
    }

    /*
     * We can't yet determine whether rescanning occurs, or whether
     * materialization of the inner input should be done.  The minimum
     * possible inner input cost, regardless of rescan and materialization
     * considerations, is inner_run_cost.  We include that in
     * workspace->total_cost, but not yet in run_cost.
     */
    /* CPU costs left for later */
    /* Public result fields */
    workspace->startup_cost = startup_cost;
    workspace->total_cost = startup_cost + run_cost + inner_run_cost;
    /* Save private data for final_cost_mergejoin */
    workspace->run_cost = run_cost;
    workspace->inner_run_cost = inner_run_cost;
    workspace->outer_rows = outer_rows;
    workspace->inner_rows = inner_rows;
    workspace->outer_skip_rows = outer_skip_rows;
    workspace->inner_skip_rows = inner_skip_rows;
    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Initial mergejoin cost: startup_cost: %lf, total_cost: %lf",
                workspace->startup_cost,
                workspace->total_cost)));
}

/*
 * final_cost_mergejoin
 *	  Final estimate of the cost and result size of a mergejoin path.
 *
 * Unlike other costsize functions, this routine makes one actual decision:
 * whether we should materialize the inner path.  We do that either because
 * the inner path can't support mark/restore, or because it's cheaper to
 * use an interposed Material node to handle mark/restore.	When the decision
 * is cost-based it would be logically cleaner to build and cost two separate
 * paths with and without that flag set; but that would require repeating most
 * of the cost calculations, which are not all that cheap.	Since the choice
 * will not affect output pathkeys or startup cost, only total cost, there is
 * no possibility of wanting to keep both paths.  So it seems best to make
 * the decision here and record it in the path's materialize_inner field.
 *
 * 'path' is already filled in except for the rows and cost fields and
 *		materialize_inner
 * 'workspace' is the result from initial_cost_mergejoin
 * 'sjinfo' is extra info about the join for selectivity estimation
 */
void final_cost_mergejoin(
    PlannerInfo* root, MergePath* path, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, bool hasalternative)
{
    Path* outer_path = path->jpath.outerjoinpath;
    Path* inner_path = path->jpath.innerjoinpath;
    double inner_path_rows = PATH_LOCAL_ROWS(inner_path);
    List* mergeclauses = path->path_mergeclauses;
    List* innersortkeys = path->innersortkeys;
    Cost startup_cost = workspace->startup_cost;
    Cost run_cost = workspace->run_cost;
    Cost inner_run_cost = workspace->inner_run_cost;
    double outer_rows = workspace->outer_rows;
    double inner_rows = workspace->inner_rows;
    double outer_skip_rows = workspace->outer_skip_rows;
    double inner_skip_rows = workspace->inner_skip_rows;
    Cost cpu_per_tuple, bare_inner_cost, mat_inner_cost;
    QualCost merge_qual_cost;
    QualCost qp_qual_cost;
    double mergejointuples, rescannedtuples;
    double rescanratio;

    /* Protect some assumptions below that rowcounts aren't zero or NaN */
    if (inner_path_rows <= 0 || isnan(inner_path_rows))
        inner_path_rows = 1;

    /* Mark the path with the correct row estimate */
    set_rel_path_rows(&path->jpath.path, path->jpath.path.parent, path->jpath.path.param_info);

    /*
     * If inner_path or outer_path is EC functioinScan without stream,
     * we should set the multiple particularly.
     */
    set_joinpath_multiple_for_EC(root, &path->jpath.path, outer_path, inner_path);

    /*
     * We could include g_instance.cost_cxt.disable_cost in the preliminary estimate, but that
     * would amount to optimizing for the case where the join method is
     * disabled, which doesn't seem like the way to bet.
     */
    if (!u_sess->attr.attr_sql.enable_mergejoin && hasalternative)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /*
     * Compute cost of the mergequals and qpquals (other restriction clauses)
     * separately.
     */
    cost_qual_eval(&merge_qual_cost, mergeclauses, root);
    cost_qual_eval(&qp_qual_cost, path->jpath.joinrestrictinfo, root);
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    /*
     * Get approx # tuples passing the mergequals.	We use approx_tuple_count
     * here because we need an estimate done with JOIN_INNER semantics.
     */
    mergejointuples = approx_tuple_count(root, &path->jpath, mergeclauses);

    /*
     * When there are equal merge keys in the outer relation, the mergejoin
     * must rescan any matching tuples in the inner relation. This means
     * re-fetching inner tuples; we have to estimate how often that happens.
     *
     * For regular inner and outer joins, the number of re-fetches can be
     * estimated approximately as size of merge join output minus size of
     * inner relation. Assume that the distinct key values are 1, 2, ..., and
     * denote the number of values of each key in the outer relation as m1,
     * m2, ...; in the inner relation, n1, n2, ...	Then we have
     *
     * size of join = m1 * n1 + m2 * n2 + ...
     *
     * number of rescanned tuples = (m1 - 1) * n1 + (m2 - 1) * n2 + ... = m1 *
     * n1 + m2 * n2 + ... - (n1 + n2 + ...) = size of join - size of inner
     * relation
     *
     * This equation works correctly for outer tuples having no inner match
     * (nk = 0), but not for inner tuples having no outer match (mk = 0); we
     * are effectively subtracting those from the number of rescanned tuples,
     * when we should not.	Can we do better without expensive selectivity
     * computations?
     *
     * The whole issue is moot if we are working from a unique-ified outer
     * input.
     */
    if (IsA(outer_path, UniquePath))
        rescannedtuples = 0;
    else {
        rescannedtuples = mergejointuples - inner_path_rows;
        /* Must clamp because of possible underestimate */
        if (rescannedtuples < 0) {
            rescannedtuples = 0;
        }
    }
    /* We'll inflate various costs this much to account for rescanning */
    rescanratio = 1.0 + (rescannedtuples / inner_path_rows);

    /*
     * Decide whether we want to materialize the inner input to shield it from
     * mark/restore and performing re-fetches.	Our cost model for regular
     * re-fetches is that a re-fetch costs the same as an original fetch,
     * which is probably an overestimate; but on the other hand we ignore the
     * bookkeeping costs of mark/restore.  Not clear if it's worth developing
     * a more refined model.  So we just need to inflate the inner run cost by
     * rescanratio.
     */
    bare_inner_cost = inner_run_cost * rescanratio;

    /*
     * When we interpose a Material node the re-fetch cost is assumed to be
     * just cpu_operator_cost per tuple, independently of the underlying
     * plan's cost; and we charge an extra cpu_operator_cost per original
     * fetch as well.  Note that we're assuming the materialize node will
     * never spill to disk, since it only has to remember tuples back to the
     * last mark.  (If there are a huge number of duplicates, our other cost
     * factors will make the path so expensive that it probably won't get
     * chosen anyway.)	So we don't use cost_rescan here.
     *
     * Note: keep this estimate in sync with create_mergejoin_plan's labeling
     * of the generated Material node.
     */
    mat_inner_cost = inner_run_cost + u_sess->attr.attr_sql.cpu_operator_cost * inner_path_rows * rescanratio;

    int inner_width = get_path_actual_total_width(inner_path, root->glob->vectorized, OP_MATERIAL);
    double inner_rel_size = relation_byte_size(inner_path_rows, inner_width, root->glob->vectorized, true, false);
    bool mat_spill_disk = (inner_rel_size > (u_sess->opt_cxt.op_work_mem * 1024L));
    /*
     * Prefer materializing if it looks cheaper, unless the user has asked to
     * suppress materialization.
     */
    if (u_sess->attr.attr_sql.enable_material && mat_inner_cost < bare_inner_cost)
        path->materialize_inner = true;

    /*
     * Even if materializing doesn't look cheaper, we *must* do it if the
     * inner path is to be used directly (without sorting) and it doesn't
     * support mark/restore.
     *
     * Since the inner side must be ordered, and only Sorts and IndexScans can
     * create order to begin with, and they both support mark/restore, you
     * might think there's no problem --- but you'd be wrong.  Nestloop and
     * merge joins can *preserve* the order of their inputs, so they can be
     * selected as the input of a mergejoin, and they don't support
     * mark/restore at present.
     *
     * We don't test the value of enable_material here, because
     * materialization is required for correctness in this case, and turning
     * it off does not entitle us to deliver an invalid plan.
     */
    else if (innersortkeys == NIL && !ExecSupportsMarkRestore(inner_path))
        path->materialize_inner = true;

    /*
     * Also, force materializing if the inner path is to be sorted and the
     * sort is expected to spill to disk.  This is because the final merge
     * pass can be done on-the-fly if it doesn't have to support mark/restore.
     * We don't try to adjust the cost estimates for this consideration,
     * though.
     *
     * Since materialization is a performance optimization in this case,
     * rather than necessary for correctness, we skip it if enable_material is
     * off.
     */
    else if (u_sess->attr.attr_sql.enable_material && innersortkeys != NIL && mat_spill_disk)
        path->materialize_inner = true;
    else
        path->materialize_inner = false;

    /* Charge the right incremental cost for the chosen case */
    if (path->materialize_inner) {
        path->mat_mem_info.opMem = u_sess->opt_cxt.op_work_mem;
        path->mat_mem_info.maxMem = inner_rel_size / 1024L;
        path->mat_mem_info.minMem = path->mat_mem_info.maxMem / SORT_MAX_DISK_SIZE;
        path->mat_mem_info.regressCost =
            ceil(inner_rel_size / BLCKSZ) / SET_DOP(path->jpath.path.dop) * u_sess->attr.attr_sql.seq_page_cost * 2.0;
        run_cost += mat_inner_cost;
    } else
        run_cost += bare_inner_cost;

    /* CPU costs */
    /*
     * The number of tuple comparisons needed is approximately number of outer
     * rows plus number of inner rows plus number of rescanned tuples (can we
     * refine this?).  At each one, we need to evaluate the mergejoin quals.
     */
    startup_cost += merge_qual_cost.startup;
    startup_cost += merge_qual_cost.per_tuple * (outer_skip_rows + inner_skip_rows * rescanratio);
    run_cost +=
        merge_qual_cost.per_tuple * ((outer_rows - outer_skip_rows) + (inner_rows - inner_skip_rows) * rescanratio);

    /*
     * For each tuple that gets through the mergejoin proper, we charge
     * cpu_tuple_cost plus the cost of evaluating additional restriction
     * clauses that are to be applied at the join.	(This is pessimistic since
     * not all of the quals may get evaluated at each tuple.)
     *
     * Note: we could adjust for SEMI/ANTI joins skipping some qual
     * evaluations here, but it's probably not worth the trouble.
     */
    startup_cost += qp_qual_cost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * mergejointuples;

    copy_mem_info(&path->outer_mem_info, &workspace->outer_mem_info);
    copy_mem_info(&path->inner_mem_info, &workspace->inner_mem_info);

    path->jpath.path.startup_cost = startup_cost;
    path->jpath.path.total_cost = startup_cost + run_cost;
    path->jpath.path.stream_cost = outer_path->stream_cost;

    if (!u_sess->attr.attr_sql.enable_mergejoin && hasalternative)
        path->jpath.path.total_cost *= g_instance.cost_cxt.disable_cost_enlarge_factor;

    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("final cost merge join: stream_cost: %lf, startup_cost: %lf, total_cost: %lf",
                path->jpath.path.stream_cost,
                path->jpath.path.startup_cost,
                path->jpath.path.total_cost)));
}

/*
 * run mergejoinscansel() with caching
 */
MergeScanSelCache* cached_scansel(PlannerInfo* root, RestrictInfo* rinfo, PathKey* pathkey)
{
    MergeScanSelCache* cache = NULL;
    ListCell* lc = NULL;
    Selectivity leftstartsel, leftendsel, rightstartsel, rightendsel;
    MemoryContext oldcontext;

    /* Do we have this result already? */
    foreach (lc, rinfo->scansel_cache) {
        cache = (MergeScanSelCache*)lfirst(lc);
        if (OpFamilyEquals(cache->opfamily, pathkey->pk_opfamily) &&
            cache->collation == pathkey->pk_eclass->ec_collation &&
            cache->strategy == pathkey->pk_strategy && cache->nulls_first == pathkey->pk_nulls_first)
            return cache;
    }

    /* Nope, do the computation */
    mergejoinscansel(root,
        (Node*)rinfo->clause,
        pathkey->pk_opfamily,
        pathkey->pk_strategy,
        pathkey->pk_nulls_first,
        &leftstartsel,
        &leftendsel,
        &rightstartsel,
        &rightendsel);

    /* Cache the result in suitably long-lived workspace */
    oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    cache = (MergeScanSelCache*)palloc(sizeof(MergeScanSelCache));
    cache->opfamily = pathkey->pk_opfamily;
    cache->collation = pathkey->pk_eclass->ec_collation;
    cache->strategy = pathkey->pk_strategy;
    cache->nulls_first = pathkey->pk_nulls_first;
    cache->leftstartsel = leftstartsel;
    cache->leftendsel = leftendsel;
    cache->rightstartsel = rightstartsel;
    cache->rightendsel = rightendsel;

    rinfo->scansel_cache = lappend(rinfo->scansel_cache, cache);

    MemoryContextSwitchTo(oldcontext);

    return cache;
}

/*
 * initial_cost_hashjoin
 *	  Preliminary estimate of the cost of a hashjoin path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.  If we are unable to eliminate the proposed path from
 * consideration using the lower bounds, final_cost_hashjoin will be called
 * to obtain the final estimates.
 *
 * The exact division of labor between this function and final_cost_hashjoin
 * is private to them, and represents a tradeoff between speed of the initial
 * estimate and getting a tight lower bound.  We choose to not examine the
 * join quals here (other than by counting the number of hash clauses),
 * so we can't do much with CPU costs.  We do assume that
 * ExecChooseHashTableSize is cheap enough to use here.
 *
 * 'workspace' is to be filled with startup_cost, total_cost, and perhaps
 *		other data to be used by final_cost_hashjoin
 * 'jointype' is the type of join to be performed
 * 'hashclauses' is the list of joinclauses to be used as hash clauses
 * 'outer_path' is the outer input to the join
 * 'inner_path' is the inner input to the join
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 */
void initial_cost_hashjoin(PlannerInfo* root, JoinCostWorkspace* workspace, JoinType jointype, List* hashclauses,
    Path* outer_path, Path* inner_path, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, int dop)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    double outer_path_rows = PATH_LOCAL_ROWS(outer_path) / dop;
    double inner_path_rows = PATH_LOCAL_ROWS(inner_path) / dop;
    int num_hashclauses = list_length(hashclauses);
    int numbuckets;
    int numbatches;
    int num_skew_mcvs;
    int inner_width; /* width of inner rel */
    int outer_width; /* width of outer rel */
    double outerpages;
    double innerpages;

    errno_t rc = 0;

    rc = memset_s(&workspace->inner_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    ereport(
        DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("outer: %lf, %lf", outer_path->startup_cost, outer_path->total_cost)));
    ereport(
        DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("inner: %lf, %lf", inner_path->startup_cost, inner_path->total_cost)));

    /* cost of source data */
    startup_cost += outer_path->startup_cost;
    run_cost += outer_path->total_cost - outer_path->startup_cost;
    /*
     * Sometimes, we suffers the case that small table with large cost join
     * with a large table. In such case, the cost mainly comes from large cost
     * of small table, and join cost become similar (within 1% gap). Since 1%
     * gap is tolerated during add_path(), we should temporarily remove large
     * cost for small table to ensure the small table be the inner side for better
     * performance, and then restore the cost back before do the final decision.
     */
    if (!u_sess->attr.attr_sql.enable_change_hjcost)
        startup_cost += inner_path->total_cost;
    else {
        startup_cost += inner_path->startup_cost;
        run_cost += inner_path->total_cost - inner_path->startup_cost;
    }

    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Source data cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));

    /* save startup cost for later stream cut off */
    Cost startup_cost_origin = startup_cost;

    /* sours
     * Cost of computing hash function: must do it once per input tuple. We
     * charge one cpu_operator_cost for each column's hash function.  Also,
     * tack on one cpu_tuple_cost per inner row, to model the costs of
     * inserting the row into the hashtable.
     *
     * XXX when a hashclause is more complex than a single operator, we really
     * should charge the extra eval costs of the left or right side, as
     * appropriate, here.  This seems more work than it's worth at the moment.
     */
    startup_cost += (u_sess->attr.attr_sql.cpu_operator_cost * num_hashclauses + u_sess->attr.attr_sql.cpu_tuple_cost +
                        u_sess->attr.attr_sql.allocate_mem_cost) *
                    inner_path_rows;
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * num_hashclauses * outer_path_rows;

    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Add hash function cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));

    bool isComplicateHashKey = has_complicate_hashkey(hashclauses, inner_path->parent->relids);
    int newcolnum = isComplicateHashKey ? 1 : 0;
    /* for vectorized right join or right anti join, we should add more column to flag match or not */
    if (root->glob->vectorized && (jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI ||
                                      jointype == JOIN_RIGHT_SEMI || jointype == JOIN_RIGHT_ANTI_FULL))
        newcolnum++;

    /*
     * Get hash table size that executor would use for inner relation.
     *
     * XXX for the moment, always assume that skew optimization will be
     * performed.  As long as SKEW_WORK_MEM_PERCENT is small, it's not worth
     * trying to determine that for sure.
     *
     * XXX at some point it might be interesting to try to account for skew
     * optimization in the cost estimate, but for now, we don't.
     */
    inner_width = get_path_actual_total_width(inner_path, root->glob->vectorized, OP_HASHJOIN, newcolnum);
    outer_width = get_path_actual_total_width(outer_path, root->glob->vectorized, OP_HASHJOIN);
    ExecChooseHashTableSize(inner_path_rows,
        inner_width,
        true,
        &numbuckets,
        &numbatches,
        &num_skew_mcvs,
        u_sess->opt_cxt.op_work_mem / dop,
        root->glob->vectorized,
        &workspace->inner_mem_info);

    innerpages = page_size(PATH_LOCAL_ROWS(inner_path), inner_width) / dop;
    outerpages = page_size(PATH_LOCAL_ROWS(outer_path), outer_width) / dop;

    /*
     * If inner relation is too big then we will need to "batch" the join,
     * which implies writing and reading most of the tuples to disk an extra
     * time.  Charge seq_page_cost per page, since the I/O should be nice and
     * sequential.	Writing the inner rel counts as startup cost, all the rest
     * as run cost.
     */
    double startuppagecost = u_sess->attr.attr_sql.seq_page_cost * innerpages;
    double runpagecost = u_sess->attr.attr_sql.seq_page_cost * (innerpages + 2 * outerpages);

    if (numbatches > 1) {
        startup_cost += startuppagecost;
        run_cost += runpagecost;
        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("Add seq page cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));
    }

    /* cut stream_cost */
    if (numbatches <= 1 /* don't cut stream cost if inner rel spill to disk */
        && outer_path->stream_cost > STREAM_COST_THRESHOLD && outer_path->stream_cost > 0.25 * inner_path->total_cost) {
        Cost cut_cost = 0.75 * Min(outer_path->stream_cost, inner_path->total_cost);

        if (cut_cost <= startup_cost_origin)
            startup_cost -= cut_cost;
        else {
            ereport(DEBUG2,
                (errmodule(MOD_OPT_JOIN),
                    errmsg("Abnormal case: outer_stream_cost: %lf >  current startup_cost: %lf",
                        outer_path->stream_cost,
                        startup_cost)));
        }

        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("outer_stream_cost: %lf, after cut-off: startup_cost: %lf, run_cost: %lf, cut_off_cost: %lf",
                    outer_path->stream_cost,
                    startup_cost,
                    run_cost,
                    cut_cost)));
    }

    /* Set mem info for hash join path */
    workspace->inner_mem_info.maxMem *= dop;
    workspace->inner_mem_info.minMem = workspace->inner_mem_info.maxMem / HASH_MAX_DISK_SIZE;
    workspace->inner_mem_info.opMem = u_sess->opt_cxt.op_work_mem;
    workspace->inner_mem_info.regressCost = (startuppagecost + runpagecost);

    /* CPU costs left for later */
    /* Public result fields */
    workspace->startup_cost = startup_cost;
    workspace->total_cost = startup_cost + run_cost;
    /* Save private data for final_cost_hashjoin */
    workspace->run_cost = run_cost;
    workspace->numbuckets = numbuckets;
    workspace->numbatches = numbatches;

    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Initial hashjoin cost: startup_cost: %lf, total_cost: %lf, work_mem: %d, esti_work_mem: %d",
                workspace->startup_cost,
                workspace->total_cost,
                u_sess->opt_cxt.op_work_mem,
                root->glob->estiopmem)));
}

/*
 * compute_bucket_size
 *
 *	acquire hashjoin comparison selectivity within one bucket. Here are some key points:
 *	1. Since the stat should be acquired from system catalog, we should cache the final selectivity to
 *	decrease the times to fetch system cache, as pg does.
 *	2. In distribute environment, selectivity differs a lot for non-stream, redistribute and broadcast
 *	case, so we should cache them respectively.
 *	3. Since the minimum selectivity is limited by virtualbucket size, so we should also cache virtual
 *	bucket size, and recalculate the selectivity when virtualbucket size changes.
 *
 *	sjinfo: identify join info include lefthand/righthand in order to judge if can use possion to estimate distinct.
 */
Selectivity compute_bucket_size(PlannerInfo* root, RestrictInfo* restrictinfo, double virtualbuckets, Path* inner_path,
    bool left, SpecialJoinInfo* sjinfo, double* ndistinct)
{
    Path* inner = NULL;
    Selectivity thisbucketsize = -1;
    BucketSize* bucket = left ? &restrictinfo->left_bucketsize : &restrictinfo->right_bucketsize;

    if (!IsA(inner_path, StreamPath)) {
        /* for replicate path, we should also adjust to global distinct value */
        if (IsA(inner_path, HashPath) || IsLocatorReplicated(inner_path->locator_type)) {
            inner = inner_path;
        } else if (bucket->normal.nbuckets == virtualbuckets) {
            /* Get inner bucketsize from cache which has saved before  */
            thisbucketsize = bucket->normal.bucket_size;
            *ndistinct = bucket->normal.ndistinct;
        }
    } else {
        inner = inner_path;
        if (((StreamPath*)inner)->type == STREAM_REDISTRIBUTE) {
            if (bucket->redistribute.nbuckets == virtualbuckets) {
                thisbucketsize = bucket->redistribute.bucket_size;
                *ndistinct = bucket->redistribute.ndistinct;
            }
        } else if (((StreamPath*)inner)->type == STREAM_BROADCAST) {
            if (bucket->broadcast.nbuckets == virtualbuckets) {
                thisbucketsize = bucket->broadcast.bucket_size;
                *ndistinct = bucket->broadcast.ndistinct;
            }
        }
    }

    /* Now we don't have cache for smp, should calculate every time */
    if (inner_path->dop > 1) {
        thisbucketsize = estimate_hash_bucketsize(root,
            left ? get_leftop(restrictinfo->clause) : get_rightop(restrictinfo->clause),
            virtualbuckets,
            inner,
            sjinfo,
            ndistinct);
    } else if (thisbucketsize < 0) {
        /* not cached yet */
        thisbucketsize = estimate_hash_bucketsize(root,
            left ? get_leftop(restrictinfo->clause) : get_rightop(restrictinfo->clause),
            virtualbuckets,
            inner,
            sjinfo,
            ndistinct);
        if (!IsA(inner_path, StreamPath)) {
            if (!IsA(inner_path, HashPath)) {
                bucket->normal.nbuckets = virtualbuckets;
                bucket->normal.bucket_size = thisbucketsize;
                bucket->normal.ndistinct = *ndistinct;
            }
        } else {
            if (((StreamPath*)inner)->type == STREAM_REDISTRIBUTE) {
                bucket->redistribute.nbuckets = virtualbuckets;
                bucket->redistribute.bucket_size = thisbucketsize;
                bucket->redistribute.ndistinct = *ndistinct;
            } else if (((StreamPath*)inner)->type == STREAM_BROADCAST) {
                bucket->broadcast.nbuckets = virtualbuckets;
                bucket->broadcast.bucket_size = thisbucketsize;
                bucket->broadcast.ndistinct = *ndistinct;
            }
        }
    }

    return thisbucketsize;
}

/*
 * final_cost_hashjoin
 *	  Final estimate of the cost and result size of a hashjoin path.
 *
 * Note: the numbatches estimate is also saved into 'path' for use later
 *
 * 'path' is already filled in except for the rows and cost fields and
 *		num_batches
 * 'workspace' is the result from initial_cost_hashjoin
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if path->jointype is SEMI or ANTI
 */
void final_cost_hashjoin(PlannerInfo* root, HashPath* path, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo,
    SemiAntiJoinFactors* semifactors, bool hasalternative, int dop)
{
    Path* outer_path = path->jpath.outerjoinpath;
    Path* inner_path = path->jpath.innerjoinpath;
    double outer_path_rows = PATH_LOCAL_ROWS(outer_path) / dop;
    double inner_path_rows = PATH_LOCAL_ROWS(inner_path) / dop;
    List* hashclauses = path->path_hashclauses;
    Cost startup_cost = workspace->startup_cost;
    Cost run_cost = workspace->run_cost;
    int numbuckets = workspace->numbuckets;
    int numbatches = workspace->numbatches;
    Cost cpu_per_tuple = 0.0;
    QualCost hash_qual_cost;
    QualCost qp_qual_cost;
    double hashjointuples;
    double virtualbuckets;
    Selectivity innerbucketsize;
    Selectivity outer_scan_ratio = 0.0;
    ListCell* hcl = NULL;
    ES_SELECTIVITY* es = NULL;
    MemoryContext ExtendedStat = NULL;
    MemoryContext oldcontext;
    List* clauselist = hashclauses;
    double innerdistinct = 1.0;
    double tmp_distinct = 1.0;
    double outerdistinct = 1.0;
    /* Mark the path with the correct row estimate */
    set_rel_path_rows(&path->jpath.path, path->jpath.path.parent, path->jpath.path.param_info);

    /*
     * If inner_path or outer_path is EC functioinScan without stream,
     * we should set the multiple particularly.
     */
    set_joinpath_multiple_for_EC(root, &path->jpath.path, outer_path, inner_path);

    /*
     * We could include g_instance.cost_cxt.disable_cost in the preliminary estimate, but that
     * would amount to optimizing for the case where the join method is
     * disabled, which doesn't seem like the way to bet.
     */
    if (!u_sess->attr.attr_sql.enable_hashjoin && hasalternative)
        startup_cost += g_instance.cost_cxt.disable_cost;

    /* mark the path with estimated # of batches */
    path->num_batches = numbatches;

    /* and compute the number of "virtual" buckets in the whole join */
    virtualbuckets = (double)numbuckets * (double)numbatches;

    /*
     * Determine bucketsize fraction for inner relation.  We use the smallest
     * bucketsize estimated for any individual hashclause; this is undoubtedly
     * conservative.
     *
     * BUT: if inner relation has been unique-ified, we can assume it's good
     * for hashing.  This is important both because it's the right answer, and
     * because we avoid contaminating the cache with a value that's wrong for
     * non-unique-ified paths.
     */
    if (IsA(inner_path, UniquePath))
        innerbucketsize = 1.0 / virtualbuckets;
    else {
        innerbucketsize = 1.0;
        /* use extended statistics to calculate innerbucket size and outerbucketsize */
        if (list_length(hashclauses) >= 2) {
            ExtendedStat = AllocSetContextCreate(CurrentMemoryContext,
                "ExtendedStat",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);
            oldcontext = MemoryContextSwitchTo(ExtendedStat);
            es = New(ExtendedStat) ES_SELECTIVITY();
            AssertEreport(root != NULL,
                MOD_OPT,
                "The NULL PlannerInfo is not allowed."
                "when estimating the cost and result size of a hashjoin path.");
            (void)es->calculate_selectivity(
                root, hashclauses, sjinfo, path->jpath.jointype, &path->jpath, ES_COMPUTEBUCKETSIZE);
            clauselist = es->unmatched_clause_group;
            (void)MemoryContextSwitchTo(oldcontext);

            ListCell* bcl = NULL;
            Selectivity outerbucketsize = 1.0;
            foreach (bcl, es->bucketsize_list) {
                es_bucketsize* es_bucket = (es_bucketsize*)lfirst(bcl);
                if (bms_is_subset(es_bucket->left_relids, inner_path->parent->relids)) {
                    innerbucketsize *=
                        es->estimate_hash_bucketsize(es_bucket, &tmp_distinct, true, inner_path, virtualbuckets);
                    innerdistinct *= tmp_distinct;
                    outerbucketsize *=
                        es->estimate_hash_bucketsize(es_bucket, &tmp_distinct, false, inner_path, virtualbuckets);
                    outerdistinct *= tmp_distinct;
                } else {
                    AssertEreport(bms_is_subset(es_bucket->right_relids, inner_path->parent->relids),
                        MOD_OPT,
                        "The right relids is not subset of the relids of inner path's parent"
                        "when estimating the cost and result size of a hashjoin path.");

                    innerbucketsize *=
                        es->estimate_hash_bucketsize(es_bucket, &tmp_distinct, false, inner_path, virtualbuckets);
                    innerdistinct *= tmp_distinct;
                    outerbucketsize *=
                        es->estimate_hash_bucketsize(es_bucket, &tmp_distinct, true, inner_path, virtualbuckets);
                    outerdistinct *= tmp_distinct;
                }
                ereport(DEBUG2,
                    (errmodule(MOD_OPT_JOIN),
                        errmsg("[ES]innerbucketsize: %e, innerdistinct: %.0f", innerbucketsize, innerdistinct)));
                ereport(DEBUG2,
                    (errmodule(MOD_OPT_JOIN),
                        errmsg("[ES]outerbucketsize: %e, outerdistinct: %.0f", outerbucketsize, outerdistinct)));
            }

            if (innerdistinct < MIN_HASH_BUCKET_SIZE) {
                /* cut bucket size to min size, unless there are few distinct value in outer path */
                outer_scan_ratio = Max(Min(innerdistinct / outerdistinct, 1.0), outer_scan_ratio);

                ereport(DEBUG2,
                    (errmodule(MOD_OPT_JOIN),
                        errmsg("[ES]outerdistinct: %f, outer_scan_ratio: %e", outerdistinct, outer_scan_ratio)));
            } else
                outer_scan_ratio = 1.0;
        }

        int number_of_joinrels = 0;
        Bitmapset* join_relids = NULL;
        foreach (hcl, clauselist) {
            RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(hcl);
            Selectivity thisbucketsize;
            Node* outerkey = NULL;
            innerdistinct = 1.0;

            AssertEreport(IsA(restrictinfo, RestrictInfo),
                MOD_OPT,
                "The nodeTag of restrictinfo is not T_RestrictInfo"
                "when estimating the cost and result size of a hashjoin path.");

            /*
             * First we have to figure out which side of the hashjoin clause
             * is the inner side.
             *
             * Since we tend to visit the same clauses over and over when
             * planning a large query, we cache the bucketsize estimate in the
             * RestrictInfo node to avoid repeated lookups of statistics.
             */
            if (bms_is_subset(restrictinfo->right_relids, inner_path->parent->relids)) {
                thisbucketsize =
                    compute_bucket_size(root, restrictinfo, virtualbuckets, inner_path, false, sjinfo, &innerdistinct);
                outerkey = get_leftop(restrictinfo->clause);
            } else {
                AssertEreport(bms_is_subset(restrictinfo->left_relids, inner_path->parent->relids),
                    MOD_OPT,
                    "The left relids is not subset of the relids of inner path's parent"
                    "when estimating the cost and result size of a hashjoin path.");
                thisbucketsize =
                    compute_bucket_size(root, restrictinfo, virtualbuckets, inner_path, true, sjinfo, &innerdistinct);
                outerkey = get_rightop(restrictinfo->clause);
            }

            ereport(DEBUG2,
                (errmodule(MOD_OPT_JOIN),
                    errmsg("thisbucketsize: %e, innerdistinct: %.0f", thisbucketsize, innerdistinct)));

            /*
             * Adjust outer scan ratio if bucket size is too big, since we have at least 32768 buckets.
             * If not all 32768 buckets are filled, the outer tuples can meet empty bucket and return
             * immediately
             */
            if (innerdistinct < MIN_HASH_BUCKET_SIZE) {
                outerdistinct = DEFAULT_NUM_DISTINCT;

                /*
                 * First estimate distinct value of outer path, will get derived stats for
                 * replicate path, hashjoin path or stream path
                 */
                if (IsA(outer_path, StreamPath) || IsA(outer_path, HashPath) ||
                    IsLocatorReplicated(outer_path->locator_type)) {
                    Selectivity outerbucketsize;

                    /* When calculating outerdistinct we have to take skew into consideration */
                    outerbucketsize =
                        estimate_hash_bucketsize(root, outerkey, virtualbuckets, outer_path, sjinfo, NULL);

                    /*
                     * Restrict outerdistinct less than MIN_HASH_BUCKET_SIZE
                     * otherwize outer_scan_ratio = innerdistinct / outerdistinct will be extremly small
                     * if outer rel have large amount of tuples and the cost will be estimated lower
                     * than it should be
                     */
                    outerdistinct = Min(1 / outerbucketsize, MIN_HASH_BUCKET_SIZE);
                }

                /* cut bucket size to min size, unless there are few distinct value in outer path */
                outer_scan_ratio = Max(Min(innerdistinct / outerdistinct, 1.0), outer_scan_ratio);

                ereport(DEBUG2,
                    (errmodule(MOD_OPT_JOIN),
                        errmsg("outerdistinct: %f, outer_scan_ratio: %e", outerdistinct, outer_scan_ratio)));
            } else
                outer_scan_ratio = 1.0;

            ereport(DEBUG2, (errmodule(MOD_OPT_JOIN), errmsg("outer_scan_ratio: %e", outer_scan_ratio)));

            /*
             * Previously, we select the smallest innerbucketsize because there could be correlationship and the
             * innerbucketsize could be too small if just multiply. For now, we have multi-column statistics and
             * will calculate innerbucketsize first with multi-column statistics, code above.
             */
            join_relids = bms_add_members(join_relids, restrictinfo->right_relids);
            join_relids = bms_add_members(join_relids, restrictinfo->left_relids);
            if (number_of_joinrels > 0 && number_of_joinrels == bms_num_members(join_relids)) {
                /* There is no new rel added to the join_relids, which mean there could be some correlationship between
                 * clauses */
                Selectivity tmp_bucketsize = innerbucketsize * thisbucketsize;
                innerbucketsize = Min(innerbucketsize * 0.75, thisbucketsize * 0.75);
                innerbucketsize = Max(innerbucketsize, tmp_bucketsize);
                ereport(DEBUG2,
                    (errmodule(MOD_OPT_JOIN),
                        errmsg("using fudge factor to fix innerbucket size: %e, tmp_bucketsize:%e",
                            innerbucketsize,
                            tmp_bucketsize)));
            } else {
                innerbucketsize *= thisbucketsize;
                ereport(DEBUG2, (errmodule(MOD_OPT_JOIN), errmsg("multiplying innerbucket size: %e", innerbucketsize)));
            }

            number_of_joinrels = bms_num_members(join_relids);
        }

        if (join_relids != NULL) {
            bms_free_ext(join_relids);
        }
    }
    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("innerbucketsize: %e, outer_scan_ratio:%e", innerbucketsize, outer_scan_ratio)));

    /*
     * add some restrition for innerbucketsize:
     * (1) innerbucketsize should not be smaller than 1.0e-7 as same as what we do in estimate_hash_bucketsize()
     * (2) innerbucketsize * inner_path_rows should be more than 1.0 as there couldn't be that much distincts in single
     * DN
     */
    if (innerbucketsize * inner_path_rows < 1.0) {
        innerbucketsize = 1 / clamp_row_est(inner_path_rows);
        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("innerbucketsize: %e, inner_path_rows:%e", innerbucketsize, inner_path_rows)));
    }

    if (innerbucketsize < 1.0e-7) {
        innerbucketsize = 1.0e-7;
        ereport(DEBUG2, (errmodule(MOD_OPT_JOIN), errmsg("innerbucketsize: %e", innerbucketsize)));
    }
    path->jpath.path.innerdistinct = innerdistinct;
    path->jpath.path.outerdistinct = outerdistinct;
    /*
     * Compute cost of the hashquals and qpquals (other restriction clauses)
     * separately.
     */
    cost_qual_eval(&hash_qual_cost, hashclauses, root);
    cost_qual_eval(&qp_qual_cost, path->jpath.joinrestrictinfo, root);
    qp_qual_cost.startup -= hash_qual_cost.startup;
    qp_qual_cost.per_tuple -= hash_qual_cost.per_tuple;

    /* CPU costs */
    if (path->jpath.jointype == JOIN_SEMI || path->jpath.jointype == JOIN_ANTI) {
        double outer_matched_rows;
        Selectivity inner_scan_frac;

        /*
         * SEMI or ANTI join: executor will stop after first match.
         *
         * For an outer-rel row that has at least one match, we can expect the
         * bucket scan to stop after a fraction 1/(match_count+1) of the
         * bucket's rows, if the matches are evenly distributed.  Since they
         * probably aren't quite evenly distributed, we apply a fuzz factor of
         * 2.0 to that fraction.  (If we used a larger fuzz factor, we'd have
         * to clamp inner_scan_frac to at most 1.0; but since match_count is
         * at least 1, no such clamp is needed now.)
         */
        outer_matched_rows = rint(outer_path_rows * semifactors->outer_match_frac);
        inner_scan_frac = 2.0 / (semifactors->match_count + 1.0);

        startup_cost += hash_qual_cost.startup;
        double matching_cost = hash_qual_cost.per_tuple * clamp_row_est(outer_matched_rows * outer_scan_ratio) *
                               clamp_row_est(inner_path_rows * innerbucketsize * inner_scan_frac) * 0.5;
        run_cost += matching_cost;
        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("matching_cost: %e, hash_qual_cost.per_tuple: %e, outer_matched_rows: %e, inner_scan_frac:%e",
                    matching_cost,
                    hash_qual_cost.per_tuple,
                    outer_matched_rows,
                    inner_scan_frac)));

        /*
         * For unmatched outer-rel rows, the picture is quite a lot different.
         * In the first place, there is no reason to assume that these rows
         * preferentially hit heavily-populated buckets; instead assume they
         * are uncorrelated with the inner distribution and so they see an
         * average bucket size of inner_path_rows / virtualbuckets.  In the
         * second place, it seems likely that they will have few if any exact
         * hash-code matches and so very few of the tuples in the bucket will
         * actually require eval of the hash quals.  We don't have any good
         * way to estimate how many will, but for the moment assume that the
         * effective cost per bucket entry is one-tenth what it is for
         * matchable tuples.
         */
        run_cost += hash_qual_cost.per_tuple *
                    clamp_row_est((outer_path_rows - outer_matched_rows) * outer_scan_ratio) *
                    clamp_row_est(inner_path_rows / virtualbuckets) * 0.05;

        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("Add cpu cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));

        /* Get # of tuples that will pass the basic join */
        if (path->jpath.jointype == JOIN_SEMI)
            hashjointuples = outer_matched_rows;
        else
            hashjointuples = outer_path_rows - outer_matched_rows;

        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                errmsg("hashjointuples=%.0f, outer_tuples=%.0f, inner_tuples=%.0f, outer_global_tuples=%.0f, "
                       "inner_global_tuples=%.0f, outer_matched_rows=%.10f",
                    hashjointuples,
                    outer_path_rows,
                    inner_path_rows,
                    outer_path->rows,
                    inner_path->rows,
                    outer_matched_rows)));
    } else if (path->jpath.jointype == JOIN_RIGHT_SEMI || path->jpath.jointype == JOIN_RIGHT_ANTI) {
        double outer_matched_rows, inner_matched_rows;

        /*
         * RIGHT_SEMI or RIGHT_ANTI join:  hash table is the side to be returned
         *
         * For cost of this Join, we should be careful that the second member (i.e. match_count)
         * of structure SemiAntiJoinFactors means the fraction of the inner tuples that are
         * expected to have at least one match in outer side.  For this new meaning, we
         * have two selectivites:
         *    outer_match_frac:  percent of outer-rel rows which have at least one match
         *    inner_match_frac: percent of inner-rel rows which have at least one match
         *
         * Note:
         *   1. the caller should carry semifactors with the new meaning for this 'RIGHT' join.
         *   2. outer_match_rows should be constrained to a limit by distinct value:
         *       if: outer rows: N, with distinct d2;    inner rows: with distinct d1
         *           outer rows which have matches: N'
         *       then,
         *                N'
         *               --- * d2 <= d1,    (averagely, without data skew)
         *                N
         *       Hence, N' should be corrected, e.g.   N' = min (N', N * d1/d2)
         */
        outer_matched_rows = rint(outer_path_rows * semifactors->outer_match_frac);
        outer_matched_rows = Min(outer_matched_rows, outer_scan_ratio * outer_path_rows);
        inner_matched_rows = rint(inner_path_rows * semifactors->match_count);

        startup_cost += hash_qual_cost.startup;

        /*
         * For matched outer-rel rows, the idea is:
         * Let
         * 	N=outer_path_rows, N'=outer_matched_rows
         * 	n=inner_path_rows, n'=inner_matched_rows.
         * Since N' rows of outer-rel match n' rows of inner-rel, 1 row of outer-rel roughly
         * matches n'/N' rows of inner-rel. Suppose that the row in Hash Table is deleted
         * once it is matched with outer-rel. In this case, 1st match row of outer-rel will
         * see a Hash Table holding n rows, 2nd match row of outer-rel will see n - n'/N'
         * rows in the Hash Table, the 3td will see n - 2 * n'/N' rows, and so on.
         * Recall that, during probe stage, one outer row do not stop even if it matches
         * an inner row in Hash Table, therefore, these outer-rel rows (can match sth in
         * Hash Table) shall do averagely
         * 	innerbucketsize * ( n + (n - n'/N') + (n - 2n'/N') + ... + (n - (N'-1)n'/N') )
         * 		= innerbucketsize * ( n*N' - n'*(N'-1)/2 )
         * comparisons totaly.
         */
        run_cost += hash_qual_cost.per_tuple *
                    clamp_row_est(innerbucketsize * (inner_path_rows * outer_matched_rows -
                                                        0.5 * inner_matched_rows * (outer_matched_rows - 1.0)));

        /* cost of moving pointer which used to delete cells in Hash table */
        run_cost += u_sess->attr.attr_sql.cpu_operator_cost * 0.1 * outer_matched_rows *
                    clamp_row_est((inner_path_rows - inner_matched_rows * 0.5) / virtualbuckets);

        /*
         * For unmatched outer-rel rows:
         * By statistical average, each unmatched row will see (n - n'/2) rows and compare
         * an bucket with size being of '(n - n'/2)/virtualbuckets' if it could have exact hash-
         * code matches. But, it is most likely that very few(say 1/10) of these unmatched
         * outer rows can exactly match inner rows. That is only these '1/10' will actually
         * require evaluation, while others '9/10' match empty buckets.
         */
        run_cost += hash_qual_cost.per_tuple * (outer_path_rows - outer_matched_rows) *
                    clamp_row_est((inner_path_rows - inner_matched_rows * 0.5) / virtualbuckets) * 0.05;

        /* cost of moving pointer which used to delete cells in Hash table: assume 1/10 needs compare */
        run_cost += u_sess->attr.attr_sql.cpu_operator_cost * 0.1 * (outer_path_rows - outer_matched_rows) * 0.05 *
                    clamp_row_est((inner_path_rows - inner_matched_rows * 0.5) / virtualbuckets);

        if (path->jpath.jointype == JOIN_RIGHT_SEMI)
            hashjointuples = inner_matched_rows;
        else {
            /* hashjointuples are those unmatched tuples in hash table */
            hashjointuples = inner_path_rows - inner_matched_rows;
        }

        /* log messages for debug */
        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("Add cpu cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));

        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                errmsg("hashjointuples=%.0f, outer_tuples=%.0f, inner_tuples=%.0f, outer_global_tuples=%.0f, "
                       "inner_global_tuples=%.0f, outer_matched_rows=%.10f, inner_matched_rows=%.10f",
                    hashjointuples,
                    outer_path_rows,
                    inner_path_rows,
                    outer_path->rows,
                    inner_path->rows,
                    outer_matched_rows,
                    inner_matched_rows)));
    } else {
        /*
         * The number of tuple comparisons needed is the number of outer
         * tuples times the typical number of tuples in a hash bucket, which
         * is the inner relation size times its bucketsize fraction.  At each
         * one, we need to evaluate the hashjoin quals.  But actually,
         * charging the full qual eval cost at each tuple is pessimistic,
         * since we don't evaluate the quals unless the hash values match
         * exactly.  For lack of a better idea, halve the cost estimate to
         * allow for that.
         */
        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple * clamp_row_est(outer_path_rows * outer_scan_ratio) *
                    clamp_row_est(inner_path_rows * innerbucketsize) * 0.5;

        ereport(DEBUG2,
            (errmodule(MOD_OPT_JOIN),
                errmsg("Add cpu cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));

        /*
         * Get approx # tuples passing the hashquals.  We use
         * approx_tuple_count here because we need an estimate done with
         * JOIN_INNER semantics.
         */
        hashjointuples = approx_tuple_count(root, &path->jpath, hashclauses);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                errmsg("outer_path_rows=%.0f, inner_path_rows=%.0f, hashjointuples=%.0f",
                    outer_path_rows,
                    inner_path_rows,
                    hashjointuples)));
    }

    /*
     * For each tuple that gets through the hashjoin proper, we charge
     * u_sess->attr.attr_sql.cpu_tuple_cost plus the cost of evaluating additional restriction
     * clauses that are to be applied at the join.	(This is pessimistic since
     * not all of the quals may get evaluated at each tuple.)
     */
    startup_cost += qp_qual_cost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * hashjointuples;

    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Add restriction clauses cost: startup_cost: %lf, run_cost: %lf", startup_cost, run_cost)));

    path->jpath.path.startup_cost = startup_cost;
    path->jpath.path.total_cost = startup_cost + run_cost;
    path->jpath.path.stream_cost = inner_path->stream_cost;
    path->joinRows = hashjointuples;

    if (!u_sess->attr.attr_sql.enable_hashjoin && hasalternative)
        path->jpath.path.total_cost *= g_instance.cost_cxt.disable_cost_enlarge_factor;

    ereport(DEBUG2,
        (errmodule(MOD_OPT_JOIN),
            errmsg("Add cpu cost: stream_cost: %lf, startup_cost: %lf, total_cost: %lf",
                path->jpath.path.stream_cost,
                path->jpath.path.startup_cost,
                path->jpath.path.total_cost)));

    copy_mem_info(&path->mem_info, &workspace->inner_mem_info);

    debug_print_hashjoin_detail(
        root, path, virtualbuckets, innerbucketsize, outer_scan_ratio, startup_cost, startup_cost + run_cost);

    /* free space used by extended statistic */
    if (es != NULL) {
        clauselist = NIL;
        list_free_ext(es->unmatched_clause_group);
        delete es;
        MemoryContextDelete(ExtendedStat);
    }
}

/*
 * cost_subplan
 *		Figure the costs for a SubPlan (or initplan).
 *
 * Note: we could dig the subplan's Plan out of the root list, but in practice
 * all callers have it handy already, so we make them pass it.
 */
void cost_subplan(PlannerInfo* root, SubPlan* subplan, Plan* plan)
{
    QualCost sp_cost;

    /* Figure any cost for evaluating the testexpr */
    cost_qual_eval(&sp_cost, make_ands_implicit((Expr*)subplan->testexpr), root);

    if (subplan->useHashTable) {
        /*
         * If we are using a hash table for the subquery outputs, then the
         * cost of evaluating the query is a one-time cost.  We charge one
         * u_sess->attr.attr_sql.cpu_operator_cost per tuple for the work of loading the hashtable,
         * too.
         */
        sp_cost.startup += plan->total_cost + u_sess->attr.attr_sql.cpu_operator_cost * PLAN_LOCAL_ROWS(plan);

        /*
         * The per-tuple costs include the cost of evaluating the lefthand
         * expressions, plus the cost of probing the hashtable.  We already
         * accounted for the lefthand expressions as part of the testexpr, and
         * will also have counted one u_sess->attr.attr_sql.cpu_operator_cost for each comparison
         * operator.  That is probably too low for the probing cost, but it's
         * hard to make a better estimate, so live with it for now.
         */
    } else {
        /*
         * Otherwise we will be rescanning the subplan output on each
         * evaluation.	We need to estimate how much of the output we will
         * actually need to scan.  NOTE: this logic should agree with the
         * tuple_fraction estimates used by make_subplan() in
         * plan/subselect.c.
         */
        Cost plan_run_cost = plan->total_cost - plan->startup_cost;

        if (subplan->subLinkType == EXISTS_SUBLINK) {
            /* we only need to fetch 1 tuple */
            sp_cost.per_tuple += plan_run_cost / PLAN_LOCAL_ROWS(plan);
        } else if (subplan->subLinkType == ALL_SUBLINK || subplan->subLinkType == ANY_SUBLINK) {
            /* assume we need 50% of the tuples */
            sp_cost.per_tuple += 0.50 * plan_run_cost;
            /* also charge a u_sess->attr.attr_sql.cpu_operator_cost per row examined */
            sp_cost.per_tuple += 0.50 * PLAN_LOCAL_ROWS(plan) * u_sess->attr.attr_sql.cpu_operator_cost;
        } else {
            /* assume we need all tuples */
            sp_cost.per_tuple += plan_run_cost;
        }

        /*
         * Also account for subplan's startup cost. If the subplan is
         * uncorrelated or undirect correlated, AND its topmost node is one
         * that materializes its output, assume that we'll only need to pay
         * its startup cost once; otherwise assume we pay the startup cost
         * every time.
         */
        if (subplan->parParam == NIL && ExecMaterializesOutput(nodeTag(plan)))
            sp_cost.startup += plan->startup_cost;
        else
            sp_cost.per_tuple += plan->startup_cost;
    }

    subplan->startup_cost = sp_cost.startup;
    subplan->per_call_cost = sp_cost.per_tuple;
}

/*
 * cost_rescan
 *		Given a finished Path, estimate the costs of rescanning it after
 *		having done so the first time.	For some Path types a rescan is
 *		cheaper than an original scan (if no parameters change), and this
 *		function embodies knowledge about that.  The default is to return
 *		the same costs stored in the Path.	(Note that the cost estimates
 *		actually stored in Paths are always for first scans.)
 *
 * This function is not currently intended to model effects such as rescans
 * being cheaper due to disk block caching; what we are concerned with is
 * plan types wherein the executor caches results explicitly, or doesn't
 * redo startup calculations, etc.
 */
void cost_rescan(PlannerInfo* root, Path* path, Cost* rescan_startup_cost, /* output parameters */
    Cost* rescan_total_cost, OpMemInfo* mem_info)
{
    int dop = SET_DOP(path->dop);

    switch (path->pathtype) {
        case T_FunctionScan:

            /*
             * Currently, nodeFunctionscan.c always executes the function to
             * completion before returning any rows, and caches the results in
             * a tuplestore.  So the function eval cost is all startup cost
             * and isn't paid over again on rescans. However, all run costs
             * will be paid over again.
             */
            *rescan_startup_cost = 0;
            *rescan_total_cost = path->total_cost - path->startup_cost;
            break;
        case T_HashJoin:

            /*
             * Assume that all of the startup cost represents hash table
             * building, which we won't have to do over.
             */
            *rescan_startup_cost = 0;
            *rescan_total_cost = path->total_cost - path->startup_cost;
            break;
        case T_CteScan:
        case T_WorkTableScan: {
            /*
             * These plan types materialize their final result in a
             * tuplestore or tuplesort object.	So the rescan cost is only
             * u_sess->attr.attr_sql.cpu_tuple_cost per tuple, unless the result is large enough
             * to spill to disk.
             */
            double rows = PATH_LOCAL_ROWS(path);
            Cost run_cost = u_sess->attr.attr_sql.cpu_tuple_cost * rows;
            double nbytes = relation_byte_size(rows, path->parent->width, false, true, false);
            long work_mem_bytes = u_sess->opt_cxt.op_work_mem * 1024L;

            if (nbytes > work_mem_bytes) {
                /* It will spill, so account for re-read cost */
                double npages = ceil(nbytes / BLCKSZ);

                run_cost += u_sess->attr.attr_sql.seq_page_cost * npages;
            }
            *rescan_startup_cost = 0;
            *rescan_total_cost = run_cost;
        } break;
        case T_Material:
        case T_Sort: {
            /*
             * These plan types not only materialize their results, but do
             * not implement qual filtering or projection.	So they are
             * even cheaper to rescan than the ones above.	We charge only
             * cpu_operator_cost per tuple.  (Note: keep that in sync with
             * the run_cost charge in cost_sort, and also see comments in
             * cost_material before you change it.)
             */
            double rows = PATH_LOCAL_ROWS(path);
            *rescan_startup_cost = 0;
            *rescan_total_cost = cost_rescan_material(rows,
                get_path_actual_total_width(path, root->glob->vectorized, OP_MATERIAL),
                mem_info,
                root->glob->vectorized,
                dop);
        } break;
        default:
            *rescan_startup_cost = path->startup_cost;
            *rescan_total_cost = path->total_cost;
            break;
    }
}

/*
 * cost_rescan_material
 *	Calculate rescan cost of the materialize node
 *
 * Parameters:
 *	@in rows: number of rows of input relation
 *	@in width: width of input relation
 *	@out mem_info: mem info to record memory usage of materialize node
 *	@in vectorized: if path to be vectorized plan
 *
 * Returns: calculated cost
 */
Cost cost_rescan_material(double rows, int width, OpMemInfo* mem_info, bool vectorized, int dop)
{
    /*
     * These plan types not only materialize their results, but do
     * not implement qual filtering or projection.	So they are
     * even cheaper to rescan than the ones above.	We charge only
     * cpu_operator_cost per tuple.  (Note: keep that in sync with
     * the run_cost charge in cost_sort, and also see comments in
     * cost_material before you change it.)
     */
    double local_rows = rows / dop;
    Cost run_cost = u_sess->attr.attr_sql.cpu_operator_cost * local_rows;
    double nbytes = relation_byte_size(local_rows, width, vectorized, true, false);
    long work_mem_bytes = u_sess->opt_cxt.op_work_mem * 1024L / dop;
    /* It will spill, so account for re-read cost */
    double npages = ceil(nbytes / BLCKSZ);
    double disk_cost = u_sess->attr.attr_sql.seq_page_cost * npages;

    if (nbytes > work_mem_bytes) {
        run_cost += disk_cost;
    }
    if (mem_info != NULL) {
        mem_info->opMem = u_sess->opt_cxt.op_work_mem;
        mem_info->maxMem = nbytes / 1024L * dop;
        mem_info->minMem = mem_info->maxMem / SORT_MAX_DISK_SIZE;
        mem_info->regressCost = disk_cost;
    }

    return run_cost;
}

#ifdef PGXC
/*
 * cost_remotequery
 * As of now the function just sets the costs to 0 to make this path the
 * cheapest.
 * NOTICE: Ideally, we should estimate the costs of network transfer from
 * datanodes and any datanode costs involved.
 */
void cost_remotequery(RemoteQueryPath* rqpath, PlannerInfo* root, RelOptInfo* rel)
{
    rqpath->path.startup_cost = 0;
    rqpath->path.total_cost = 0;
    set_rel_path_rows(&rqpath->path, rel, NULL);
}
#endif /* PGXC */

/*
 * cost_qual_eval
 *		Estimate the CPU costs of evaluating a WHERE clause.
 *		The input can be either an implicitly-ANDed list of boolean
 *		expressions, or a list of RestrictInfo nodes.  (The latter is
 *		preferred since it allows caching of the results.)
 *		The result includes both a one-time (startup) component,
 *		and a per-evaluation component.
 */
void cost_qual_eval(QualCost* cost, List* quals, PlannerInfo* root)
{
    cost_qual_eval_context context;
    ListCell* l = NULL;

    context.root = root;
    context.total.startup = 0;
    context.total.per_tuple = 0;

    /* We don't charge any cost for the implicit ANDing at top level ... */
    foreach (l, quals) {
        Node* qual = (Node*)lfirst(l);

        (void)cost_qual_eval_walker(qual, &context);
    }

    *cost = context.total;
}

/*
 * cost_qual_eval_node
 *		As above, for a single RestrictInfo or expression.
 */
void cost_qual_eval_node(QualCost* cost, Node* qual, PlannerInfo* root)
{
    cost_qual_eval_context context;

    context.root = root;
    context.total.startup = 0;
    context.total.per_tuple = 0;

    (void)cost_qual_eval_walker(qual, &context);

    *cost = context.total;
}

static bool cost_qual_eval_walker(Node* node, cost_qual_eval_context* context)
{
    if (node == NULL)
        return false;

    /*
     * RestrictInfo nodes contain an eval_cost field reserved for this
     * routine's use, so that it's not necessary to evaluate the qual clause's
     * cost more than once.  If the clause's cost hasn't been computed yet,
     * the field's startup value will contain -1.
     */
    if (IsA(node, RestrictInfo)) {
        RestrictInfo* rinfo = (RestrictInfo*)node;

        if (rinfo->eval_cost.startup < 0) {
            cost_qual_eval_context locContext;

            locContext.root = context->root;
            locContext.total.startup = 0;
            locContext.total.per_tuple = 0;

            /*
             * For an OR clause, recurse into the marked-up tree so that we
             * set the eval_cost for contained RestrictInfos too.
             */
            if (rinfo->orclause)
                (void)cost_qual_eval_walker((Node*)rinfo->orclause, &locContext);
            else
                (void)cost_qual_eval_walker((Node*)rinfo->clause, &locContext);

            /*
             * If the RestrictInfo is marked pseudoconstant, it will be tested
             * only once, so treat its cost as all startup cost.
             */
            if (rinfo->pseudoconstant) {
                /* count one execution during startup */
                locContext.total.startup += locContext.total.per_tuple;
                locContext.total.per_tuple = 0;
            }
            rinfo->eval_cost = locContext.total;
        }
        context->total.startup += rinfo->eval_cost.startup;
        context->total.per_tuple += rinfo->eval_cost.per_tuple;
        /* do NOT recurse into children */
        return false;
    }

    /*
     * For each operator or function node in the given tree, we charge the
     * estimated execution cost given by pg_proc.procost (remember to multiply
     * this by cpu_operator_cost).
     *
     * Vars and Consts are charged zero, and so are boolean operators (AND,
     * OR, NOT). Simplistic, but a lot better than no model at all.
     *
     * Should we try to account for the possibility of short-circuit
     * evaluation of AND/OR?  Probably *not*, because that would make the
     * results depend on the clause ordering, and we are not in any position
     * to expect that the current ordering of the clauses is the one that's
     * going to end up being used.	The above per-RestrictInfo caching would
     * not mix well with trying to re-order clauses anyway.
     *
     * Another issue that is entirely ignored here is that if a set-returning
     * function is below top level in the tree, the functions/operators above
     * it will need to be evaluated multiple times.  In practical use, such
     * cases arise so seldom as to not be worth the added complexity needed;
     * moreover, since our rowcount estimates for functions tend to be pretty
     * phony, the results would also be pretty phony.
     */
    if (IsA(node, FuncExpr)) {
        context->total.per_tuple += get_func_cost(((FuncExpr*)node)->funcid) * u_sess->attr.attr_sql.cpu_operator_cost;
    } else if (IsA(node, OpExpr) || IsA(node, DistinctExpr) || IsA(node, NullIfExpr)) {
        /* rely on struct equivalence to treat these all alike */
        set_opfuncid((OpExpr*)node);
        context->total.per_tuple += get_func_cost(((OpExpr*)node)->opfuncid) * u_sess->attr.attr_sql.cpu_operator_cost;
    } else if (IsA(node, ScalarArrayOpExpr)) {
        /*
         * Estimate that the operator will be applied to about half of the
         * array elements before the answer is determined.
         */
        ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)node;
        Node* arraynode = (Node*)lsecond(saop->args);

        set_sa_opfuncid(saop);
        context->total.per_tuple += get_func_cost(saop->opfuncid) * u_sess->attr.attr_sql.cpu_operator_cost *
                                    estimate_array_length(arraynode) * 0.5;
    } else if (IsA(node, Aggref) || IsA(node, WindowFunc)) {
        /*
         * Aggref and WindowFunc nodes are (and should be) treated like Vars,
         * ie, zero execution cost in the current model, because they behave
         * essentially like Vars in execQual.c.  We disregard the costs of
         * their input expressions for the same reason.  The actual execution
         * costs of the aggregate/window functions and their arguments have to
         * be factored into plan-node-specific costing of the Agg or WindowAgg
         * plan node.
         */
        return false; /* don't recurse into children */
    } else if (IsA(node, CoerceViaIO)) {
        CoerceViaIO* iocoerce = (CoerceViaIO*)node;
        Oid iofunc;
        Oid typioparam;
        bool typisvarlena = false;

        /* check the result type's input function */
        getTypeInputInfo(iocoerce->resulttype, &iofunc, &typioparam);
        context->total.per_tuple += get_func_cost(iofunc) * u_sess->attr.attr_sql.cpu_operator_cost;
        /* check the input type's output function */
        getTypeOutputInfo(exprType((Node*)iocoerce->arg), &iofunc, &typisvarlena);
        context->total.per_tuple += get_func_cost(iofunc) * u_sess->attr.attr_sql.cpu_operator_cost;
    } else if (IsA(node, ArrayCoerceExpr)) {
        ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
        Node* arraynode = (Node*)acoerce->arg;

        if (OidIsValid(acoerce->elemfuncid))
            context->total.per_tuple += get_func_cost(acoerce->elemfuncid) * u_sess->attr.attr_sql.cpu_operator_cost *
                                        estimate_array_length(arraynode);
    } else if (IsA(node, RowCompareExpr)) {
        /* Conservatively assume we will check all the columns */
        RowCompareExpr* rcexpr = (RowCompareExpr*)node;
        ListCell* lc = NULL;

        foreach (lc, rcexpr->opnos) {
            Oid opid = lfirst_oid(lc);

            context->total.per_tuple += get_func_cost(get_opcode(opid)) * u_sess->attr.attr_sql.cpu_operator_cost;
        }
    } else if (IsA(node, CurrentOfExpr)) {
        /* Report high cost to prevent selection of anything but TID scan */
        context->total.startup += g_instance.cost_cxt.disable_cost;
    } else if (IsA(node, SubLink)) {
        /* This routine should not be applied to un-planned expressions */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("cannot handle unplanned sub-select when costing quals")));
    } else if (IsA(node, SubPlan)) {
        /*
         * A subplan node in an expression typically indicates that the
         * subplan will be executed on each evaluation, so charge accordingly.
         * (Sub-selects that can be executed as InitPlans have already been
         * removed from the expression.)
         */
        SubPlan* subplan = (SubPlan*)node;

        context->total.startup += subplan->startup_cost;
        context->total.per_tuple += subplan->per_call_cost;

        /*
         * We don't want to recurse into the testexpr, because it was already
         * counted in the SubPlan node's costs.  So we're done.
         */
        return false;
    } else if (IsA(node, AlternativeSubPlan)) {
        /*
         * Arbitrarily use the first alternative plan for costing.	(We should
         * certainly only include one alternative, and we don't yet have
         * enough information to know which one the executor is most likely to
         * use.)
         */
        AlternativeSubPlan* asplan = (AlternativeSubPlan*)node;

        return cost_qual_eval_walker((Node*)linitial(asplan->subplans), context);
    }

    /* recurse into children */
    return expression_tree_walker(node, (bool (*)())cost_qual_eval_walker, (void*)context);
}

/*
 * get_restriction_qual_cost
 *	  Compute evaluation costs of a baserel's restriction quals, plus any
 *	  movable join quals that have been pushed down to the scan.
 *	  Results are returned into *qpqual_cost.
 *
 * This is a convenience subroutine that works for seqscans and other cases
 * where all the given quals will be evaluated the hard way.  It's not useful
 * for cost_index(), for example, where the index machinery takes care of
 * some of the quals.  We assume baserestrictcost was previously set
 * by set_baserel_size_estimates().
 */
static void get_restriction_qual_cost(
    PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info, QualCost* qpqual_cost)
{
    if (param_info != NULL) {
        /* Include costs of pushed-down clauses */
        cost_qual_eval(qpqual_cost, param_info->ppi_clauses, root);

        qpqual_cost->startup += baserel->baserestrictcost.startup;
        qpqual_cost->per_tuple += baserel->baserestrictcost.per_tuple;
    } else
        *qpqual_cost = baserel->baserestrictcost;
}

/*
 * compute_semi_anti_join_factors
 *	  Estimate how much of the inner input a SEMI or ANTI join
 *	  can be expected to scan.
 *
 * In a hash or nestloop SEMI/ANTI join, the executor will stop scanning
 * inner rows as soon as it finds a match to the current outer row.
 * We should therefore adjust some of the cost components for this effect.
 * This function computes some estimates needed for these adjustments.
 * These estimates will be the same regardless of the particular paths used
 * for the outer and inner relation, so we compute these once and then pass
 * them to all the join cost estimation functions.
 *
 * Input parameters:
 *	outerrel: outer relation under consideration
 *	innerrel: inner relation under consideration
 *	jointype: must be JOIN_SEMI or JOIN_ANTI
 *	sjinfo: SpecialJoinInfo relevant to this join
 *	restrictlist: join quals
 * Output parameters:
 *	*semifactors is filled in (see relation.h for field definitions)
 */
void compute_semi_anti_join_factors(PlannerInfo* root, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype,
    SpecialJoinInfo* sjinfo, List* restrictlist, SemiAntiJoinFactors* semifactors)
{
    Selectivity jselec;
    Selectivity nselec;
    Selectivity avgmatch;
    SpecialJoinInfo norm_sjinfo;
    List* joinquals = NIL;
    ListCell* l = NULL;

    /* Should only be called in these cases */
    AssertEreport(jointype == JOIN_SEMI || jointype == JOIN_ANTI,
        MOD_OPT,
        "Only JOIN_SEMI or JOIN_ANTI can be supported"
        "when estimating how much of the inner input a SEMI or ANTI join can be expected to scan.");

    /*
     * In an ANTI join, we must ignore clauses that are "pushed down", since
     * those won't affect the match logic.  In a SEMI join, we do not
     * distinguish joinquals from "pushed down" quals, so just use the whole
     * restrictinfo list.
     */
    if (jointype == JOIN_ANTI) {
        joinquals = NIL;
        foreach (l, restrictlist) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

            AssertEreport(IsA(rinfo, RestrictInfo),
                MOD_OPT,
                "The nodeTag of rinfo is T_RestrictInfo"
                "when estimating how much of the inner input a SEMI or ANTI join can be expected to scan.");
            if (!rinfo->is_pushed_down)
                joinquals = lappend(joinquals, rinfo);
        }
    } else
        joinquals = restrictlist;

    /*
     * Get the JOIN_SEMI or JOIN_ANTI selectivity of the join clauses.
     */
    jselec = clauselist_selectivity(root, joinquals, 0, jointype, sjinfo);

    /*
     * Also get the normal inner-join selectivity of the join clauses.
     */
    norm_sjinfo.type = T_SpecialJoinInfo;
    norm_sjinfo.min_lefthand = outerrel->relids;
    norm_sjinfo.min_righthand = innerrel->relids;
    norm_sjinfo.syn_lefthand = outerrel->relids;
    norm_sjinfo.syn_righthand = innerrel->relids;
    norm_sjinfo.jointype = JOIN_INNER;
    /* we don't bother trying to make the remaining fields valid */
    norm_sjinfo.lhs_strict = false;
    norm_sjinfo.delay_upper_joins = false;
    norm_sjinfo.join_quals = NIL;
    /* we don't cache nselec because it is only used to compute matched cout for inner. */
    norm_sjinfo.varratio_cached = false;

    nselec = clauselist_selectivity(root, joinquals, 0, JOIN_INNER, &norm_sjinfo);

    /* Avoid leaking a lot of ListCells */
    if (jointype == JOIN_ANTI)
        list_free_ext(joinquals);

    /*
     * jselec can be interpreted as the fraction of outer-rel rows that have
     * any matches (this is true for both SEMI and ANTI cases).  And nselec is
     * the fraction of the Cartesian product that matches.	So, the average
     * number of matches for each outer-rel row that has at least one match is
     * nselec * inner_rows / jselec.
     *
     * Note: it is correct to use the inner rel's "rows" count here, even
     * though we might later be considering a parameterized inner path with
     * fewer rows.	This is because we have included all the join clauses in
     * the selectivity estimate.
     */
    if (jselec > 0) { /* protect against zero divide */
        avgmatch = nselec * RELOPTINFO_LOCAL_FIELD(root, innerrel, rows) / jselec;
        /* Clamp to sane range */
        avgmatch = Max(1.0, avgmatch);
    } else {
        avgmatch = 1.0;
    }
    semifactors->outer_match_frac = jselec;
    semifactors->match_count = avgmatch;
}

/*
 * has_indexed_join_quals
 *	  Check whether all the joinquals of a nestloop join are used as
 *	  inner index quals.
 *
 * If the inner path of a SEMI/ANTI join is an indexscan (including bitmap
 * indexscan) that uses all the joinquals as indexquals, we can assume that an
 * unmatched outer tuple is cheap to process, whereas otherwise it's probably
 * expensive.
 */
bool has_indexed_join_quals(NestPath* joinpath)
{
    Relids joinrelids = joinpath->path.parent->relids;
    Path* innerpath = joinpath->innerjoinpath;
    List* indexclauses = NIL;
    bool found_one = false;
    ListCell* lc = NULL;

    /* If join still has quals to evaluate, it's not fast */
    if (joinpath->joinrestrictinfo != NIL)
        return false;
    /* Nor if the inner path isn't parameterized at all */
    if (innerpath->param_info == NULL)
        return false;

    /* Find the indexclauses list for the inner scan */
    switch (innerpath->pathtype) {
        case T_IndexScan:
        case T_IndexOnlyScan:
            indexclauses = ((IndexPath*)innerpath)->indexclauses;
            break;
        case T_BitmapHeapScan: {
            /* Accept only a simple bitmap scan, not AND/OR cases */
            Path* bmqual = ((BitmapHeapPath*)innerpath)->bitmapqual;

            if (IsA(bmqual, IndexPath))
                indexclauses = ((IndexPath*)bmqual)->indexclauses;
            else
                return false;
            break;
        }
        default:

            /*
             * If it's not a simple indexscan, it probably doesn't run quickly
             * for zero rows out, even if it's a parameterized path using all
             * the joinquals.
             */
            return false;
    }

    /*
     * Examine the inner path's param clauses.  Any that are from the outer
     * path must be found in the indexclauses list, either exactly or in an
     * equivalent form generated by equivclass.c.  Also, we must find at least
     * one such clause, else it's a clauseless join which isn't fast.
     */
    found_one = false;
    foreach (lc, innerpath->param_info->ppi_clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (join_clause_is_movable_into(rinfo, innerpath->parent->relids, joinrelids)) {
            if (!(list_member_ptr(indexclauses, rinfo) || is_redundant_derived_clause(rinfo, indexclauses)))
                return false;
            found_one = true;
        }
    }
    return found_one;
}

/*
 * approx_tuple_count
 *		Quick-and-dirty estimation of the number of join rows passing
 *		a set of qual conditions.
 *
 * The quals can be either an implicitly-ANDed list of boolean expressions,
 * or a list of RestrictInfo nodes (typically the latter).
 *
 * We intentionally compute the selectivity under JOIN_INNER rules, even
 * if it's some type of outer join.  This is appropriate because we are
 * trying to figure out how many tuples pass the initial merge or hash
 * join step.
 *
 * This is quick-and-dirty because we bypass clauselist_selectivity, and
 * simply multiply the independent clause selectivities together.  Now
 * clauselist_selectivity often can't do any better than that anyhow, but
 * for some situations (such as range constraints) it is smarter.  However,
 * we can't effectively cache the results of clauselist_selectivity, whereas
 * the individual clause selectivities can be and are cached.
 *
 * Since we are only using the results to estimate how many potential
 * output tuples are generated and passed through qpqual checking, it
 * seems OK to live with the approximation.
 */
double approx_tuple_count(PlannerInfo* root, JoinPath* path, List* quals)
{
    double tuples;
    double outer_global_tuples = path->outerjoinpath->rows;
    double inner_global_tuples = path->innerjoinpath->rows;
    double outer_tuples = PATH_LOCAL_ROWS(path->outerjoinpath);
    double inner_tuples = PATH_LOCAL_ROWS(path->innerjoinpath);
    SpecialJoinInfo sjinfo;
    Selectivity selec = 1.0;
    ListCell* l = NULL;
    int dop = SET_DOP(path->path.dop);
    List* qual_list = quals;
    ES_SELECTIVITY* es = NULL;
    MemoryContext ExtendedStat = NULL;
    MemoryContext oldcontext;

    /*
     * Make up a SpecialJoinInfo for JOIN_INNER semantics.
     */
    sjinfo.type = T_SpecialJoinInfo;
    sjinfo.min_lefthand = path->outerjoinpath->parent->relids;
    sjinfo.min_righthand = path->innerjoinpath->parent->relids;
    sjinfo.syn_lefthand = path->outerjoinpath->parent->relids;
    sjinfo.syn_righthand = path->innerjoinpath->parent->relids;
    sjinfo.jointype = JOIN_INNER;
    /* we don't bother trying to make the remaining fields valid */
    sjinfo.lhs_strict = false;
    sjinfo.delay_upper_joins = false;
    sjinfo.join_quals = NIL;
    sjinfo.varratio_cached = true;

    /* initialize es_selectivity class */
    if (list_length(qual_list) >= 2) {
        ExtendedStat = AllocSetContextCreate(CurrentMemoryContext,
            "ExtendedStat",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        oldcontext = MemoryContextSwitchTo(ExtendedStat);
        es = New(ExtendedStat) ES_SELECTIVITY();
        AssertEreport(root != NULL,
            MOD_OPT,
            "The NULL PlannerInfo is not allowed "
            "when estimation the number of join rows passing a set of qual conditions approximately.");

        selec = es->calculate_selectivity(root, qual_list, &sjinfo, JOIN_INNER, path, ES_EQJOINSEL);
        qual_list = es->unmatched_clause_group;
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* Get the approximate selectivity */
    foreach (l, qual_list) {
        Node* qual = (Node*)lfirst(l);

        /* Note that clause_selectivity will be able to cache its result */
        selec *= clause_selectivity(root, qual, 0, JOIN_INNER, &sjinfo);
    }

    /* We should use global tuples if has stream. */
    if (IS_STREAM_PLAN) {
        /*
         * if outerpath or innerpath is redistribute, we should use global tuples for both.
         * if outerpath or innerpath is broadcast, we should use local tuples for the side of broadcast.
         * if outerpath or innerpath is not stream, we should use global tuples for both.
         * When parallel broadcast or local broadcast, the inner_tuples refer to tuples in a DN, so we divide dop.
         */
        if (IsA(path->outerjoinpath, StreamPath) && (STREAM_BROADCAST == ((StreamPath*)path->outerjoinpath)->type)) {
            outer_global_tuples = outer_tuples / dop;
        } else if (IsA(path->innerjoinpath, StreamPath) &&
                   (((StreamPath*)path->innerjoinpath)->type == STREAM_BROADCAST)) {
            inner_global_tuples = inner_tuples / dop;
        } else if (IsA(path->outerjoinpath, StreamPath) && ((StreamPath*)path->outerjoinpath)->smpDesc &&
                   ((StreamPath*)path->outerjoinpath)->smpDesc->distriType == LOCAL_BROADCAST) {
            outer_global_tuples = outer_global_tuples / dop;
        } else if (IsA(path->innerjoinpath, StreamPath) && ((StreamPath*)path->innerjoinpath)->smpDesc &&
                   ((StreamPath*)path->innerjoinpath)->smpDesc->distriType == LOCAL_BROADCAST) {
            inner_global_tuples = inner_global_tuples / dop;
        }

        tuples = selec * outer_global_tuples * inner_global_tuples;
        /* estimate local relation sizes. */
        tuples = get_local_rows(tuples,
                                path->path.multiple,
                                IsLocatorReplicated(path->path.locator_type),
                                ng_get_dest_num_data_nodes(&path->path)) / 
                 dop;
    } else
        tuples = selec * outer_tuples * inner_tuples;

    /* free space used by extended statistic */
    if (es != NULL) {
        qual_list = NIL;
        list_free_ext(es->unmatched_clause_group);
        delete es;
        MemoryContextDelete(ExtendedStat);
    }

    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("hashjointuples=%.0f, outer_tuples=%.0f, inner_tuples=%.0f, outer_global_tuples=%.0f, "
                   "inner_global_tuples=%.0f, selec=%.10f, multiple=%.0f,",
                tuples,
                outer_tuples,
                inner_tuples,
                outer_global_tuples,
                inner_global_tuples,
                selec,
                path->path.multiple)));
    return clamp_row_est(tuples);
}

/*
 * set_baserel_size_estimates
 *		Set the size estimates for the given base relation.
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already, and rel->tuples must be set.
 *
 * We set the following fields of the rel node:
 *	rows: the estimated number of output tuples (after applying
 *		  restriction clauses).
 *	width: the estimated average output tuple width in bytes.
 *	baserestrictcost: estimated cost of evaluating baserestrictinfo clauses.
 */
void set_baserel_size_estimates(PlannerInfo* root, RelOptInfo* rel)
{
    double nrows;

    /* Should only be applied to base relations */
    AssertEreport(
        rel->relid > 0, MOD_OPT, "The relid is invalid when set the size estimates for the given base relation.");

    nrows = rel->tuples * clauselist_selectivity(root, rel->baserestrictinfo, 0, JOIN_INNER, NULL);

    rel->rows = clamp_row_est(nrows);

    cost_qual_eval(&rel->baserestrictcost, rel->baserestrictinfo, root);

    set_rel_width(root, rel);
}

/*
 * set_result_size_estimates
 *             Set the size estimates for an RTE_RESULT base relation
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already.
 *
 * We set the same fields as set_baserel_size_estimates.
 */
void set_result_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
    /* Should only be applied to RTE_RESULT base relations */
    Assert(rel->relid > 0);
    Assert(planner_rt_fetch(rel->relid, root)->rtekind == RTE_RESULT);

    /* RTE_RESULT always generates a single row, natively */
    rel->tuples = 1;

    /* Now estimate number of output rows, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * get_parameterized_baserel_size
 *		Make a size estimate for a parameterized scan of a base relation.
 *
 * 'param_clauses' lists the additional join clauses to be used.
 *
 * set_baserel_size_estimates must have been applied already.
 */
double get_parameterized_baserel_size(PlannerInfo* root, RelOptInfo* rel, List* param_clauses)
{
    List* allclauses = NIL;
    double nrows;

    /*
     * Estimate the number of rows returned by the parameterized scan, knowing
     * that it will apply all the extra join clauses as well as the rel's own
     * restriction clauses.  Note that we force the clauses to be treated as
     * non-join clauses during selectivity estimation.
     */
    allclauses = list_concat(list_copy(param_clauses), rel->baserestrictinfo);
    allclauses = list_concat(list_copy(param_clauses), rel->subplanrestrictinfo);
    nrows = rel->tuples * clauselist_selectivity(root,
                                                 allclauses,
                                                 rel->relid, /* do not use 0! */
                                                 JOIN_INNER,
                                                 NULL,
                                                 false);
    nrows = clamp_row_est(nrows);
    /* For safety, make sure result is not more than the base estimate */
    if (nrows > rel->rows)
        nrows = rel->rows;
    return nrows;
}

/*
 * set_joinrel_size_estimates
 *		Set the size estimates for the given join relation.
 *
 * The rel's targetlist must have been constructed already, and a
 * restriction clause list that matches the given component rels must
 * be provided.
 *
 * Since there is more than one way to make a joinrel for more than two
 * base relations, the results we get here could depend on which component
 * rel pair is provided.  In theory we should get the same answers no matter
 * which pair is provided; in practice, since the selectivity estimation
 * routines don't handle all cases equally well, we might not.  But there's
 * not much to be done about it.  (Would it make sense to repeat the
 * calculations for each pair of input rels that's encountered, and somehow
 * average the results?  Probably way more trouble than it's worth, and
 * anyway we must keep the rowcount estimate the same for all paths for the
 * joinrel.)
 *
 * We set only the rows field here.  The width field was already set by
 * build_joinrel_tlist, and baserestrictcost is not used for join rels.
 */
void set_joinrel_size_estimates(PlannerInfo* root, RelOptInfo* rel, RelOptInfo* outer_rel, RelOptInfo* inner_rel,
    SpecialJoinInfo* sjinfo, List* restrictlist)
{
    rel->rows = calc_joinrel_size_estimate(root, outer_rel->rows, inner_rel->rows, sjinfo, restrictlist, true);

    /*
     * We should adjust joinrel's rows as max between outer_rel and inner_rel
     * when the join rel's global rows is over estimate(more than 1.0e11).
     */
    if (((unsigned int)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_CONJUNCT) &&
        (rel->rows >= JOINREL_MAX_GLOBAL_ROWS) && (1 < list_length(restrictlist))) {
        rel->rows = clamp_row_est(Min(Min(Max(outer_rel->rows, inner_rel->rows), rel->rows), JOINREL_MAX_GLOBAL_ROWS));
    }

    /* If global rows is less more, we should adjust it, unless either one is replicate */
    if (IS_STREAM_PLAN && (rel->rows == 1) &&
        !(IsLocatorReplicated(inner_rel->locator_type) || IsLocatorReplicated(outer_rel->locator_type))) {
        Distribution* distribution = ng_get_default_computing_group_distribution();
        rel->rows = bms_num_members(distribution->bms_data_nodeids);
    }
    rel->tuples = rel->rows;
}

/*
 * set_joinpath_multiple_for_EC
 * 		Set multiple for joinpath when the inner path or outer path contains
 * 		EC function without following by StreamPath
 *
 * Because EC function only execute on one single datanode, so we should set
 * the multiple particularly. When there is no StreamPath after the EC FunctionScan,
 * the join result very likely be skew, we set joinpath multiple in this scene.
 */
void set_joinpath_multiple_for_EC(PlannerInfo* root, Path* path, Path* outer_path, Path* inner_path)
{
    if (path == NULL || outer_path == NULL || inner_path == NULL || root == NULL) {
        return;
    }

    RangeTblEntry* rte = NULL;

    if (outer_path->pathtype == T_FunctionScan) {
        rte = planner_rt_fetch(outer_path->parent->relid, root);
        if (IS_EC_FUNC(rte)) {
            path->multiple = outer_path->parent->multiple;
        }
    } else if (inner_path->pathtype == T_FunctionScan) {
        rte = planner_rt_fetch(inner_path->parent->relid, root);
        if (IS_EC_FUNC(rte)) {
            path->multiple = inner_path->parent->multiple;
        }
    }

    return;
}

/*
 * get_parameterized_joinrel_size
 *		Make a size estimate for a parameterized scan of a join relation.
 *
 * 'rel' is the joinrel under consideration.
 * 'outer_rows', 'inner_rows' are the sizes of the (probably also
 *		parameterized) join inputs under consideration.
 * 'sjinfo' is any SpecialJoinInfo relevant to this join.
 * 'restrict_clauses' lists the join clauses that need to be applied at the
 * join node (including any movable clauses that were moved down to this join,
 * and not including any movable clauses that were pushed down into the
 * child paths).
 *
 * set_joinrel_size_estimates must have been applied already.
 */
double get_parameterized_joinrel_size(PlannerInfo* root, RelOptInfo* rel, double outer_rows, double inner_rows,
    SpecialJoinInfo* sjinfo, List* restrict_clauses)
{
    double nrows;

    /*
     * Estimate the number of rows returned by the parameterized join as the
     * sizes of the input paths times the selectivity of the clauses that have
     * ended up at this join node.
     *
     * As with set_joinrel_size_estimates, the rowcount estimate could depend
     * on the pair of input paths provided, though ideally we'd get the same
     * estimate for any pair with the same parameterization.
     */
    nrows = calc_joinrel_size_estimate(root, outer_rows, inner_rows, sjinfo, restrict_clauses, false);
    /* For safety, make sure result is not more than the base estimate */
    if (nrows > rel->rows)
        nrows = rel->rows;
    return nrows;
}

/*
 * calc_joinrel_size_estimate
 *		Workhorse for set_joinrel_size_estimates and
 *		get_parameterized_joinrel_size.
 */
static double calc_joinrel_size_estimate(PlannerInfo* root, double outer_rows, double inner_rows,
    SpecialJoinInfo* sjinfo, List* restrictlist, bool varratio_cached)
{
    JoinType jointype = sjinfo->jointype;
    Selectivity jselec;
    Selectivity pselec;
    double nrows;

    /*
     * Compute joinclause selectivity.	Note that we are only considering
     * clauses that become restriction clauses at this join level; we are not
     * double-counting them because they were not considered in estimating the
     * sizes of the component rels.
     *
     * For an outer join, we have to distinguish the selectivity of the join's
     * own clauses (JOIN/ON conditions) from any clauses that were "pushed
     * down".  For inner joins we just count them all as joinclauses.
     */
    if (IS_OUTER_JOIN((uint32)jointype)) {
        List* joinquals = NIL;
        List* pushedquals = NIL;
        ListCell* l = NULL;

        /* Grovel through the clauses to separate into two lists */
        foreach (l, restrictlist) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

            AssertEreport(IsA(rinfo, RestrictInfo),
                MOD_OPT,
                "The nodeTag of rinfo is T_RestrictInfo"
                "when calculating the joinrel size.");
            if (rinfo->is_pushed_down)
                pushedquals = lappend(pushedquals, rinfo);
            else
                joinquals = lappend(joinquals, rinfo);
        }

        /* Get the separate selectivities */
        jselec = clauselist_selectivity(root, joinquals, 0, jointype, sjinfo, varratio_cached);
        pselec = clauselist_selectivity(root, pushedquals, 0, jointype, sjinfo, varratio_cached);

        /* Avoid leaking a lot of ListCells */
        list_free_ext(joinquals);
        list_free_ext(pushedquals);
    } else {
        jselec = clauselist_selectivity(root, restrictlist, 0, jointype, sjinfo, varratio_cached);
        pselec = 0.0; /* not used, keep compiler quiet */
    }

    /*
     * Basically, we multiply size of Cartesian product by selectivity.
     *
     * If we are doing an outer join, take that into account: the joinqual
     * selectivity has to be clamped using the knowledge that the output must
     * be at least as large as the non-nullable input.	However, any
     * pushed-down quals are applied after the outer join, so their
     * selectivity applies fully.
     *
     * For JOIN_SEMI and JOIN_ANTI, the selectivity is defined as the fraction
     * of LHS rows that have matches, and we apply that straightforwardly.
     */
    switch (jointype) {
        case JOIN_INNER:
            nrows = outer_rows * inner_rows * jselec;
            break;
        case JOIN_LEFT:
            nrows = outer_rows * inner_rows * jselec;
            if (nrows < outer_rows)
                nrows = outer_rows;
            nrows *= pselec;
            break;
        case JOIN_FULL:
            nrows = outer_rows * inner_rows * jselec;
            if (nrows < outer_rows)
                nrows = outer_rows;
            if (nrows < inner_rows)
                nrows = inner_rows;
            nrows *= pselec;
            break;
        case JOIN_SEMI:
            nrows = outer_rows * jselec;
            /* pselec not used */
            break;
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            nrows = outer_rows * (1.0 - jselec);
            nrows *= pselec;
            break;
        default: {
            /* other values not expected here */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized join type when calculate joinrel size estimate: %d", (int)jointype)));
            nrows = 0; /* keep compiler quiet */
        } break;
    }

    return clamp_row_est(nrows);
}

/*
 * set_subquery_size_estimates
 *		Set the size estimates for a base relation that is a subquery.
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already, and the plan for the subquery must have been completed.
 * We look at the subquery's plan and PlannerInfo to extract data.
 *
 * We set the same fields as set_baserel_size_estimates.
 */
void set_subquery_size_estimates(PlannerInfo* root, RelOptInfo* rel)
{
    PlannerInfo* subroot = rel->subroot;
    RangeTblEntry PG_USED_FOR_ASSERTS_ONLY* rte = NULL;
    ListCell* lc = NULL;

    /* Should only be applied to base relations that are subqueries */
    AssertEreport(rel->relid > 0,
        MOD_OPT,
        "The relid is invalid when set the size estimates for a base relation that is a subquery.");
    rte = planner_rt_fetch(rel->relid, root);
    AssertEreport(rte->rtekind == RTE_SUBQUERY,
        MOD_OPT,
        "Only subquery in FROM clause can be supported"
        "when set the size estimates for a base relation that is a subquery.");

    /* Copy raw number of output rows from subplan */
    if (rel->subplan->exec_nodes != NULL)
        rel->locator_type = rel->subplan->exec_nodes->baselocatortype;
    if (rel->locator_type != LOCATOR_TYPE_REPLICATED)
        rel->tuples = rel->subplan->plan_rows;
    else
        rel->tuples = PLAN_LOCAL_ROWS(rel->subplan);
    set_local_rel_size(root, rel);

    /*
     * Compute per-output-column width estimates by examining the subquery's
     * targetlist.	For any output that is a plain Var, get the width estimate
     * that was made while planning the subquery.  Otherwise, we leave it to
     * set_rel_width to fill in a datatype-based default estimate.
     */
    foreach (lc, subroot->parse->targetList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        Node* texpr = (Node*)te->expr;
        int32 item_width = 0;

        AssertEreport(IsA(te, TargetEntry),
            MOD_OPT,
            "The nodeTag of te is T_TargetEntry"
            "when set the size estimates for a base relation that is a subquery.");
        /* junk columns aren't visible to upper query */
        if (te->resjunk)
            continue;

        /*
         * The subquery could be an expansion of a view that's had columns
         * added to it since the current query was parsed, so that there are
         * non-junk tlist columns in it that don't correspond to any column
         * visible at our query level.  Ignore such columns.
         */
        if (te->resno < rel->min_attr || te->resno > rel->max_attr)
            continue;

        /*
         * XXX This currently doesn't work for subqueries containing set
         * operations, because the Vars in their tlists are bogus references
         * to the first leaf subquery, which wouldn't give the right answer
         * even if we could still get to its PlannerInfo.
         *
         * Also, the subquery could be an appendrel for which all branches are
         * known empty due to constraint exclusion, in which case
         * set_append_rel_pathlist will have left the attr_widths set to zero.
         *
         * In either case, we just leave the width estimate zero until
         * set_rel_width fixes it.
         */
        if (IsA(texpr, Var) && subroot->parse->setOperations == NULL) {
            Var* var = (Var*)texpr;
            RelOptInfo* subrel = find_base_rel(subroot, var->varno);

            item_width = subrel->attr_widths[var->varattno - subrel->min_attr];
        }
        rel->attr_widths[te->resno - rel->min_attr] = item_width;
    }

    /* Now estimate number of output rows, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * set_function_size_estimates
 *		Set the size estimates for a base relation that is a function call.
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already.
 *
 * We set the same fields as set_baserel_size_estimates.
 */
void set_function_size_estimates(PlannerInfo* root, RelOptInfo* rel)
{
    RangeTblEntry* rte = NULL;

    /* Should only be applied to base relations that are functions */
    AssertEreport(rel->relid > 0,
        MOD_OPT,
        "The relid is invalid when set the size estimates for a base relation that is a function call.");
    rte = planner_rt_fetch(rel->relid, root);
    AssertEreport(rte->rtekind == RTE_FUNCTION,
        MOD_OPT,
        "Only function in FROM clause can be supported"
        "when set the size estimates for a base relation that is a subquery.");

    /* Estimate number of rows the function itself will return */
    rel->tuples = expression_returns_set_rows(rte->funcexpr);
    set_local_rel_size(root, rel);

    /* Now estimate number of output rows, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * set_values_size_estimates
 *		Set the size estimates for a base relation that is a values list.
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already.
 *
 * We set the same fields as set_baserel_size_estimates.
 */
void set_values_size_estimates(PlannerInfo* root, RelOptInfo* rel)
{
    RangeTblEntry* rte = NULL;

    /* Should only be applied to base relations that are values lists */
    AssertEreport(rel->relid > 0,
        MOD_OPT,
        "The relid is invalid when set the size estimates for a base relation that is a values list.");
    rte = planner_rt_fetch(rel->relid, root);
    AssertEreport(rte->rtekind == RTE_VALUES,
        MOD_OPT,
        "Only VALUES list can be supported"
        "when set the size estimates for a base relation that is a values list.");

    /*
     * Estimate number of rows the values list will return. We know this
     * precisely based on the list length (well, barring set-returning
     * functions in list items, but that's a refinement not catered for
     * anywhere else either).
     */
    rel->tuples = list_length(rte->values_lists);
    set_local_rel_size(root, rel);

    /* Now estimate number of output rows, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * set_cte_size_estimates
 *		Set the size estimates for a base relation that is a CTE reference.
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already, and we need the completed plan for the CTE (if a regular CTE)
 * or the non-recursive term (if a self-reference).
 *
 * We set the same fields as set_baserel_size_estimates.
 */
void set_cte_size_estimates(PlannerInfo* root, RelOptInfo* rel, Plan* cteplan)
{
    RangeTblEntry* rte = NULL;

    /* Should only be applied to base relations that are CTE references */
    AssertEreport(rel->relid > 0,
        MOD_OPT,
        "The relid is invalid when set the size estimates for a base relation that is a CTE reference.");
    rte = planner_rt_fetch(rel->relid, root);
    AssertEreport(rte->rtekind == RTE_CTE,
        MOD_OPT,
        "Only common table expr can be supported"
        "when set the size estimates for a base relation that is a CTE reference.");

    if (rte->self_reference) {
        /*
         * In a self-reference, arbitrarily assume the average worktable size
         * is about 10 times the nonrecursive term's size.
         */
        rel->tuples = 10 * cteplan->plan_rows;
    } else {
        /* Otherwise just believe the CTE plan's output estimate */
        rel->tuples = cteplan->plan_rows;
    }
    set_local_rel_size(root, rel);

    /* Now estimate number of output rows, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * set_foreign_size_estimates
 *		Set the size estimates for a base relation that is a foreign table.
 *
 * There is not a whole lot that we can do here; the foreign-data wrapper
 * is responsible for producing useful estimates.  We can do a decent job
 * of estimating baserestrictcost, so we set that, and we also set up width
 * using what will be purely datatype-driven estimates from the targetlist.
 * There is no way to do anything sane with the rows value, so we just put
 * a default estimate and hope that the wrapper can improve on it.	The
 * wrapper's GetForeignRelSize function will be called momentarily.
 *
 * The rel's targetlist and restrictinfo list must have been constructed
 * already.
 */
void set_foreign_size_estimates(PlannerInfo* root, RelOptInfo* rel)
{
    /* Should only be applied to base relations */
    AssertEreport(rel->relid > 0,
        MOD_OPT,
        "The relid is invalid when set the size estimates for a base relation that is a foreign table.");

    rel->rows = 1000; /* entirely bogus default estimate */

    cost_qual_eval(&rel->baserestrictcost, rel->baserestrictinfo, root);

    set_rel_width(root, rel);
}

inline void set_rel_encode_info_if_vectorized(PlannerInfo *root, RelOptInfo *rel, int typid, int width)
{
    if (root->glob->vectorized) {
        rel->encodedwidth += columnar_get_col_width(typid, width);
        rel->encodednum++;
    }
}

/*
 * set_rel_width
 *		Set the estimated output width of a base relation.
 *
 * The estimated output width is the sum of the per-attribute width estimates
 * for the actually-referenced columns, plus any PHVs or other expressions
 * that have to be calculated at this relation.  This is the amount of data
 * we'd need to pass upwards in case of a sort, hash, etc.
 *
 * NB: this works best on plain relations because it prefers to look at
 * real Vars.  For subqueries, set_subquery_size_estimates will already have
 * copied up whatever per-column estimates were made within the subquery,
 * and for other types of rels there isn't much we can do anyway.  We fall
 * back on (fairly stupid) datatype-based width estimates if we can't get
 * any better number.
 *
 * The per-attribute width estimates are cached for possible re-use while
 * building join relations.
 */
void set_rel_width(PlannerInfo* root, RelOptInfo* rel)
{
    Oid reloid = planner_rt_fetch(rel->relid, root)->relid;
    int32 tuple_width = 0;
    bool have_wholerow_var = false;
    ListCell* lc = NULL;

    foreach (lc, rel->reltargetlist) {
        Node* node = (Node*)lfirst(lc);

        if (IsA(node, Var)) {
            Var* var = (Var*)node;
            int ndx;
            int32 item_width;

            AssertEreport(var->varno == rel->relid,
                MOD_OPT,
                "The varno does not match to relid when setting the estimated output width of a base relation.");
            AssertEreport(var->varattno >= rel->min_attr,
                MOD_OPT,
                "The varattno is less than min_attr when setting the estimated output width of a base relation.");
            AssertEreport(var->varattno <= rel->max_attr,
                MOD_OPT,
                "The varattno is larger than max_attr when setting the estimated output width of a base relation.");

            ndx = var->varattno - rel->min_attr;

            /*
             * If it's a whole-row Var, we'll deal with it below after we have
             * already cached as many attr widths as possible.
             */
            if (var->varattno == 0) {
                have_wholerow_var = true;
                continue;
            }

            /*
             * The width may have been cached already (especially if it's a
             * subquery), so don't duplicate effort.
             */
            if (rel->attr_widths[ndx] > 0) {
                tuple_width += rel->attr_widths[ndx];
                set_rel_encode_info_if_vectorized(root, rel, var->vartype, rel->attr_widths[ndx]);
                continue;
            }

            /* Try to get column width from statistics */
            if (reloid != InvalidOid && var->varattno > 0) {
                Oid targetid = reloid;
                bool ispartition = false;
                RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);

                if (rte->isContainPartition) {
                    AssertEreport(OidIsValid(rte->partitionOid),
                        MOD_OPT,
                        "The partitionOid is invalid when setting the estimated output width of a base relation.");
                    targetid = rte->partitionOid;
                    ispartition = true;
                }

                if (rte->isContainSubPartition) {
                    Assert(OidIsValid(rte->partitionOid));
                    targetid = rte->subpartitionOid;
                    ispartition = true;
                }

                item_width = get_attavgwidth(targetid, var->varattno, ispartition);
                if (item_width > 0) {
                    rel->attr_widths[ndx] = item_width;
                    tuple_width += item_width;
                    set_rel_encode_info_if_vectorized(root, rel, var->vartype, item_width);
                    continue;
                }
            }

            /*
             * Not a plain relation, or can't find statistics for it. Estimate
             * using just the type info.
             */
            item_width = get_typavgwidth(var->vartype, var->vartypmod);
            AssertEreport(item_width > 0,
                MOD_OPT,
                "The estimated average width of values of the type is not larger than 0"
                "when setting the estimated output width of a base relation.");
            rel->attr_widths[ndx] = item_width;
            tuple_width += item_width;
            set_rel_encode_info_if_vectorized(root, rel, var->vartype, item_width);
        } else if (IsA(node, PlaceHolderVar)) {
            PlaceHolderVar* phv = (PlaceHolderVar*)node;
            PlaceHolderInfo* phinfo = find_placeholder_info(root, phv, false);

            tuple_width += phinfo->ph_width;
            set_rel_encode_info_if_vectorized(root, rel, exprType((Node*)phv->phexpr), phinfo->ph_width);
        } else {
            /*
             * We could be looking at an expression pulled up from a subquery,
             * or a ROW() representing a whole-row child Var, etc.	Do what we
             * can using the expression type information.
             */
            int32 item_width;

            item_width = get_typavgwidth(exprType(node), exprTypmod(node));
            AssertEreport(item_width > 0,
                MOD_OPT,
                "The estimated average width of values of the type is not larger than 0"
                "when setting the estimated output width of a base relation.");

            tuple_width += item_width;
            set_rel_encode_info_if_vectorized(root, rel, exprType(node), item_width);
        }
    }

    /*
     * If we have a whole-row reference, estimate its width as the sum of
     * per-column widths plus sizeof(HeapTupleHeaderData).
     */
    if (have_wholerow_var) {
        int32 wholerow_width = sizeof(HeapTupleHeaderData);

        if (reloid != InvalidOid) {
            Oid partid = InvalidOid;
            RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);

            if (rte->isContainPartition) {
                AssertEreport(OidIsValid(rte->partitionOid),
                    MOD_OPT,
                    "The partitionOid is invalid when setting the estimated output width of a base relation.");
                partid = rte->partitionOid;
            }

            if (rte->isContainSubPartition) {
                Assert(OidIsValid(rte->partitionOid));
                partid = rte->subpartitionOid;
            }

            /* Real relation, so estimate true tuple width */
            wholerow_width += get_relation_data_width(reloid, partid, rel->attr_widths - rel->min_attr);
        } else {
            /* Do what we can with info for a phony rel */
            AttrNumber i;

            for (i = 1; i <= rel->max_attr; i++)
                wholerow_width += rel->attr_widths[i - rel->min_attr];
        }

        rel->attr_widths[0 - rel->min_attr] = wholerow_width;

        /*
         * Include the whole-row Var as part of the output tuple.  Yes, that
         * really is what happens at runtime.
         */
        tuple_width += wholerow_width;
    }

    AssertEreport(tuple_width >= 0,
        MOD_OPT,
        "The estimated width of tuple is less than 0"
        "when setting the estimated output width of a base relation.");
    rel->width = tuple_width;
}

/*
 * relation_byte_size
 *	  Estimate the storage space in bytes for a given number of tuples
 *	  of a given width (size in bytes).
 *
 * Parameters:
 *	@in tuples: number of rows of input relation
 *	@in width: width of input relation
 *	@in vectorized: if rel is vectorized stored
 *	@in aligned: should be calculated as stored aligned. For disk storage,
 *		it's false, but in execution, the memory should be aligned
 *	@in issort: if the path is sort. Since the function will be used by
 *		sort and materialize, and their way to calculate width is different
 *	@in index_sort: if index header should be used instead of heap
 *
 * Returns: total size of the input relation
 */
double relation_byte_size(double tuples, int width, bool vectorized, bool aligned, bool issort, bool indexsort)
{
    Assert(width >= 0);
    size_t header_size = (issort && indexsort) ? sizeof(IndexTupleData) : sizeof(HeapTupleHeaderData);
    if (aligned) {
        if (vectorized)
            return tuples * (TUPLE_OVERHEAD(true) + width);
        else
            return tuples *
                   (TUPLE_OVERHEAD(issort) + alloc_trunk_size(MAXALIGN((uintptr_t)width) + MAXALIGN(header_size)));
    } else {
        return tuples * (MAXALIGN((uintptr_t)width) + MAXALIGN(header_size));
    }
}

/*
 * page_size
 *	  Returns an estimate of the number of pages covered by a given
 *	  number of tuples of a given width (size in bytes).
 */
double page_size(double tuples, int width)
{
    return ceil(relation_byte_size(tuples, width, false) / BLCKSZ);
}

/* it used to compute page_size in createplan.cpp */
double cost_page_size(double tuples, int width)
{
    return page_size(tuples, width);
}

/*
 * restore_hashjoin_cost
 *	With u_sess->attr.attr_sql.enable_change_hjcost on, we lessen startup cost to do the internal cost comparison.
 *	Before final judgement, we should restore it back.
 */
void restore_hashjoin_cost(Path* path)
{
    if (u_sess->attr.attr_sql.enable_change_hjcost && IsA(path, HashPath)) {
        Path* innerpath = ((HashPath*)path)->jpath.innerjoinpath;
        path->startup_cost += innerpath->total_cost - innerpath->startup_cost;
    }
}

/*
 * finalize_dml_cost:
 *	calculate insert/update/delete cost of the modifytable plan
 * Param:
 *	@in plan: target modifytable plan
 */
void finalize_dml_cost(ModifyTable* plan)
{
    CmdType type = plan->operation;

    /* For insert and update, we should insert the tuples, so add the insertion cost */
    if (type == CMD_INSERT || type == CMD_UPDATE) {
        double size = page_size(PLAN_LOCAL_ROWS(&plan->plan), plan->plan.plan_width);

        plan->plan.total_cost +=
            (u_sess->attr.attr_sql.cpu_tuple_cost + u_sess->attr.attr_sql.seq_page_cost * 2) * size;
    }

    /* For delete and update we should scan the tuples by ctid, so add random page cost */
    if (type == CMD_DELETE || type == CMD_UPDATE) {
        plan->plan.total_cost += u_sess->attr.attr_sql.random_page_cost * PLAN_LOCAL_ROWS(&plan->plan);
    }
}

/*
 * Description: Get sample fraction.
 *
 * Parameters:
 *	@in pctnode: node of percent args.
 *
 * Return: float4
 */
static float4 get_samplefract(Node* pctnode)
{
    float4 samplefract;

    if (IsA(pctnode, Const) && !((Const*)pctnode)->constisnull) {
        samplefract = DatumGetFloat4(((Const*)pctnode)->constvalue);
        if (samplefract >= 0.0 && samplefract <= 100.0 && !isnan(samplefract)) {
            samplefract /= 100.0f;
        } else {
            /* Default samplefract if the value is bogus */
            samplefract = 0.1f;
        }
    } else {
        /* Default samplefract if we didn't obtain a non-null Const */
        samplefract = 0.1f;
    }

    return samplefract;
}

/*
 * Description: Sample size estimation.
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level.
 *	@in baserel: the relation to be scanned.
 *	@in paramexprs: the sample percentage info.
 *
 * Return: void
 */
void system_samplescangetsamplesize(PlannerInfo* root, RelOptInfo* baserel, List* paramexprs)
{
    Node* pctnode = NULL;
    float4 samplefract;

    /* Try to extract an estimate for the sample percentage */
    pctnode = (Node*)linitial(paramexprs);
    pctnode = estimate_expression_value(root, pctnode);
    samplefract = get_samplefract(pctnode);

    /* We'll visit a sample of the pages ... */
    baserel->pages = clamp_row_est(baserel->pages * samplefract);

    /* ... and hopefully get a representative number of tuples from them */
    baserel->tuples = clamp_row_est(baserel->tuples * samplefract);
}

/*
 * Description: Sample size estimation.
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level.
 *	@in baserel: the relation to be scanned.
 *	@in paramexprs: the sample percentage info.
 *
 * Return: void
 */
void bernoulli_samplescangetsamplesize(PlannerInfo* root, RelOptInfo* baserel, List* paramexprs)
{
    Node* pctnode = NULL;
    float4 samplefract;

    /* Try to extract an estimate for the sample percentage */
    pctnode = (Node*)linitial(paramexprs);
    pctnode = estimate_expression_value(root, pctnode);
    samplefract = get_samplefract(pctnode);

    /* We'll visit all pages of the baserel, so pages is the same  */
    baserel->tuples = clamp_row_est(baserel->tuples * samplefract);
}

/*
 * Description: Sample size estimation.
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level.
 *	@in baserel: the relation to be scanned.
 *	@in paramexprs: the sample percentage info.
 *
 * Return: void
 */
void hybrid_samplescangetsamplesize(PlannerInfo* root, RelOptInfo* baserel, List* paramexprs)
{
    ListCell* lc = NULL;
    uint16 i = 0;

    AssertEreport(SAMPLEARGSNUM == list_length(paramexprs),
        MOD_OPT,
        "The number of sample percentage info does not equal 2"
        "when setting the estimated output width of a base relation.");
    foreach (lc, paramexprs) {
        Node* paramnode = (Node*)lfirst(lc);
        Node* pctnode = estimate_expression_value(root, paramnode);
        float4 samplefract = 0.0;
        if (likely(pctnode)) {
            samplefract = get_samplefract(pctnode);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("Fail to estimate expression value.")));
        }

        if (i == SYSTEM_SAMPLE) {
            /* We'll visit a sample of the pages ... */
            baserel->pages = clamp_row_est(baserel->pages * samplefract);
        }

        /* ... and hopefully get a representative number of tuples from them */
        baserel->tuples = clamp_row_est(baserel->tuples * samplefract);

        i++;
    }
}

/*
 * copy_mem_info
 *	copy OpMemInfo structure from source to dest
 *
 * Parameters:
 *	@in dest: dest structure
 *	@in src: source structure
 *
 * Returns: void
 */
void copy_mem_info(OpMemInfo* dest, OpMemInfo* src)
{
    errno_t rc = 0;

    rc = memcpy_s(dest, sizeof(OpMemInfo), src, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");
}

/*
 * columnar_get_col_width
 *	Calculate additional space besides datum for a columnar column,
 *	only happens on length-varying columns
 *
 * Parameters:
 *	@in typid: column type oid
 *	@in width: column raw width
 *	@in aligned: if return value is aligned by memory aset module
 *
 * Returns: additional width for the column, or 0 for fixed-length col
 */
int columnar_get_col_width(int typid, int width, bool aligned)
{
    if (COL_IS_ENCODE(typid)) {
        if (aligned) {
            return alloc_trunk_size(width);
        } else {
            return width;
        }
    } else
        return 0;
}

/*
 * has_complicate_hashkey
 *	Judge if has complicate hash key, if so, vector engine will use another
 *	8 bytes to store hash value to avoid duplicate calculation. Only simple
 *	var is not complicate hash key
 *
 * Parameters:
 *	@in hashclauses: equal clauses of hash join
 *	@in inner_relids: relids of hashjoin inner table
 *
 * Returns: If there's complicate hash key in the hash condition
 */
bool has_complicate_hashkey(List* hashclauses, Relids inner_relids)
{
    ListCell* lc = NULL;

    foreach (lc, hashclauses) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(lc);
        Node* innerkey = NULL;

        AssertEreport(IsA(restrictinfo, RestrictInfo),
            MOD_OPT,
            "The nodeTag of restrictinfo is not T_RestrictInfo"
            "when setting the estimated output width of a base relation.");

        /*
         * First we have to figure out which side of the hashjoin clause
         * is the inner side.
         */
        if (bms_is_subset(restrictinfo->right_relids, inner_relids)) {
            innerkey = get_rightop(restrictinfo->clause);
        } else {
            AssertEreport(bms_is_subset(restrictinfo->left_relids, inner_relids),
                MOD_OPT,
                "The left relids is not subset of the relids of inner side"
                "when setting the estimated output width of a base relation.");
            innerkey = get_leftop(restrictinfo->clause);
        }

        /* Judge if the inner key is simple var */
        if (innerkey != NULL && !IsA(innerkey, Var) &&
            !(IsA(innerkey, RelabelType) && IsA(((RelabelType*)innerkey)->arg, Var)))
            return true;
    }

    return false;
}

/*
 * calc_distributekey_width
 *	Optimizer will add distribute key in the targetlist if not found in plan
 *	phase, so added width should be considerred here
 *
 * Parameters:
 *	@in path: the path that should estimate width
 *	@out width: return total width of added distribute keys
 *	@in vectorized: if the path will be vectorized plan
 *	@in aligned: if the width should be aligned by memory aset module
 *
 * Returns: number of distribute keys added to targetlist
 */
static int calc_distributekey_width(Path* path, int* width, bool vectorized, bool aligned)
{
    int num = 0;
    ListCell* lc = NULL;

    /* Only do this for redistribute stream, since only redistribute has distribute keys */
    if (IsA(path, StreamPath) && ((StreamPath*)path)->type == STREAM_REDISTRIBUTE) {
        foreach (lc, path->distribute_keys) {
            Node* node = (Node*)lfirst(lc);
            if (!list_member(path->parent->reltargetlist, node)) {
                num++;
                int32 item_width = get_typavgwidth(exprType(node), exprTypmod(node));
                AssertEreport(item_width > 0,
                    MOD_OPT,
                    "The item width is not larger than 0 when setting the estimated output width of a base relation.");
                if (vectorized)
                    *width += columnar_get_col_width(exprType(node), item_width, aligned);
                else
                    *width += item_width;
            }
        }
    }

    return num;
}

/*
 * get_path_actual_total_width
 *	In PG optimizer, only width of row engine is estimated, and it has
 *	big difference with vector engine, so this function is used to estimate
 *	width of a path with vector engine though row width
 *
 * Parameters:
 *	@in path: the path that should estimate width
 *	@in vectorized: if the path will be vectorized
 *	@in type: the type to calculate the width, only considerring hashjoin,
 *			hashagg, sort and material
 *	@in newcol: for some case like add distribute column, and vectorized
 *			abbreviate sort, new columns will be added, so should record
 *			the number of new col to impact the width
 *
 * Returns: estimated width with row-engine and vector-engine
 */
int get_path_actual_total_width(Path* path, bool vectorized, OpType type, int newcol)
{
    int num_new_col = 0;
    int width = 0;
    bool aligned = (type >= OP_SORT);

    if (path->parent == NULL) {
        return COL_TUPLE_WIDTH;
    }

    /* For redistribute, we will add unmatched distribute key into targetlist, so count this */
    num_new_col = calc_distributekey_width(path, &width, vectorized, aligned);

    if (vectorized) {
        switch (type) {
            case OP_HASHJOIN:
                width += path->parent->encodedwidth +
                         SIZE_COL_VALUE * (list_length(path->parent->reltargetlist) + num_new_col + newcol);
                break;
            case OP_HASHAGG:
                width += path->parent->encodedwidth + TUPLE_OVERHEAD(true) + sizeof(void*) * 2 +
                         SIZE_COL_VALUE * (list_length(path->parent->reltargetlist) + num_new_col + newcol);
                break;
            case OP_SORT:
                if (width != 0 || path->parent->encodednum != 0)
                    newcol += 1;
                /* No need break here. */
            case OP_MATERIAL:
                /* don't know encoded width of each column, just average them for a rough estimation */
                if (path->parent->encodednum > 0)
                    width += path->parent->encodednum *
                             alloc_trunk_size(path->parent->encodedwidth / path->parent->encodednum);
                width += sizeof(Datum) * (list_length(path->parent->reltargetlist) + num_new_col + newcol);
                break;
            default:
                break;
        }
    } else {
        width += path->parent->width;
    }

    return width;
}

/*
 * get_subqueryscan_stream_cost
 * 	get stream_cost of a subquery
 */
static Cost get_subqueryscan_stream_cost(Plan* subplan)
{
    Cost stream_cost = 0;

    if (subplan == NULL)
        return stream_cost;

    switch (nodeTag(subplan)) {
        case T_HashJoin:
        case T_VecHashJoin:
            stream_cost = get_subqueryscan_stream_cost(subplan->lefttree);
            break;

        case T_NestLoop:
        case T_VecNestLoop:
            stream_cost = get_subqueryscan_stream_cost(subplan->righttree);
            break;

        case T_MergeJoin:
        case T_VecMergeJoin:
            stream_cost = get_subqueryscan_stream_cost(subplan->righttree);
            break;

        case T_Stream:
        case T_VecStream:
            stream_cost = subplan->startup_cost;
            break;

        default:
            stream_cost = get_subqueryscan_stream_cost(subplan->lefttree);
            break;
    }

    return stream_cost;
}
