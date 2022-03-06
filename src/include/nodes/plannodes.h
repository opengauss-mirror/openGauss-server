/* -------------------------------------------------------------------------
 *
 * plannodes.h
 *	  definitions for query plan nodes
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/plannodes.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLANNODES_H
#define PLANNODES_H

#include "access/sdir.h"
#include "foreign/foreign.h"
#include "nodes/bitmapset.h"
#include "nodes/primnodes.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "optimizer/pruning.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "bulkload/dist_fdw.h"
#include "utils/bloom_filter.h"

#define MAX_SPECIAL_BUCKETMAP_NUM    20
#define BUCKETMAP_DEFAULT_INDEX_BIT (1 << 31)

/*
 * Determines the position where the RemoteQuery node will run.
 */
typedef enum { GATHER, PLAN_ROUTER, SCAN_GATHER } RemoteQueryType;

/*
 * @hdfs
 * Determines the optimization mode base on informational constraint.
 * Currently, join Scan and foreign scan about on HDFS foreign table
 * would be optimized.
 */
typedef enum { INVALID_MODE, SCAN_OPTIMIZE_MODE, JOIN_OPTIMIZE_MODE } OptimizedMode;
/* ----------------------------------------------------------------
 *						node definitions
 * ----------------------------------------------------------------
 */

typedef struct NodeGroupQueryMem {
    Oid ng_oid;
    char nodegroup[NAMEDATALEN];

    int query_mem[2]; /* memory in kb */
} NodeGroupQueryMem;

/* ----------------
 *		PlannedStmt node
 *
 * The output of the planner is a Plan tree headed by a PlannedStmt node.
 * PlannedStmt holds the "one time" information needed by the executor.
 * ----------------
 */
typedef struct PlannedStmt {
    NodeTag type;

    CmdType commandType; /* select|insert|update|delete */

    uint64 queryId; /* query identifier,  uniquely indicate this plan in Runtime (copied from Query) */

    bool hasReturning; /* is it insert|update|delete RETURNING? */

    bool hasModifyingCTE; /* has insert|update|delete in WITH? */

    bool canSetTag; /* do I set the command result tag? */

    bool transientPlan; /* redo plan when TransactionXmin changes? */

    bool dependsOnRole; /* is plan specific to current role? */

    Plan* planTree; /* tree of Plan nodes */

    List* rtable; /* list of RangeTblEntry nodes */

    /* rtable indexes of target relations for INSERT/UPDATE/DELETE */
    List* resultRelations; /* integer list of RT indexes, or NIL */

    Node* utilityStmt; /* non-null if this is DECLARE CURSOR */

    List* subplans; /* Plan trees for SubPlan expressions */

    Bitmapset* rewindPlanIDs; /* indices of subplans that require REWIND */

    List* rowMarks; /* a list of PlanRowMark's */

    /*
     * Notice: be careful to use relationOids as it may contain non-table OID
     * in some scenarios, e.g. assignment of relationOids in fix_expr_common.
     */
    List* relationOids; /* contain OIDs of relations the plan depends on */

    List* invalItems; /* other dependencies, as PlanInvalItems */

    int nParamExec; /* number of PARAM_EXEC Params used */

    int num_streams; /* number of stream exist in plan tree	*/

    int max_push_sql_num; /* number of split sql want push DN execute */

    int gather_count; /* gather_count in query */

    int num_nodes; /* number of data nodes */

    NodeDefinition* nodesDefinition; /* all data nodes' defination */

    int instrument_option; /* used for collect instrument data */

    int num_plannodes; /* how many plan node in this planstmt */

    int query_mem[2]; /* how many memory the query can use ,  memory in kb  */

    int assigned_query_mem[2]; /* how many memory the query is assigned   */

    bool is_dynmaic_smp;

    int dynsmp_max_cpu; /* max avaliable cpu for this dn */

    int dynsmp_avail_cpu; /* max avaliable cpu for this dn */

    int dynsmp_cpu_util;

    int dynsmp_active_statement;

    double dynsmp_query_estimate_cpu_usge;

    int dynsmp_plan_optimal_dop; /* the final optimized dop for the plan */

    int dynsmp_plan_original_dop;

    int dynsmp_dop_mem_limit; /* memory will put a limit on dop */

    int dynsmp_min_non_spill_dop; /* optimal dop cannot greater than this */

    int num_bucketmaps; /* Num of special-bucketmap stored in plannedstmt */

    uint2* bucketMap[MAX_SPECIAL_BUCKETMAP_NUM]; /* the map information need to be get */

    int    bucketCnt[MAX_SPECIAL_BUCKETMAP_NUM]; /* the map bucket count */

    char* query_string; /* convey the query string to backend/stream thread of DataNode for debug purpose */

    List* subplan_ids; /* in which plan id subplan should be inited */

    List* initPlan; /* initplan in top plan node */
    /* data redistribution for DFS table.
     * dataDestRelIndex is index into the range table. This variable
     * will take effect on data redistribution state.
     */
    Index dataDestRelIndex;

    int MaxBloomFilterNum;

    int query_dop; /* Dop of current query. */

    double plannertime; /* planner execute time */

    /* set true in do_query_for_planrouter() for PlannedStmt sent to
     * the compute pool
     */
    bool in_compute_pool;

    /* true if there is/are ForeignScan node(s) of OBS foreign table
     * in plantree.
     */
    bool has_obsrel;

    List* plan_hint_warning; /* hint warning during plan generation, only used in CN */

    List* noanalyze_rellist; /* relations and attributes that have no statistics, only used in CN */

    int ng_num;                     /* nodegroup number */
    NodeGroupQueryMem* ng_queryMem; /* each nodegroup's query mem */
    bool ng_use_planA;              /* true means I am a planA, default false */

    bool isRowTriggerShippable; /* true if all row triggers are shippable. */
    bool is_stream_plan;
    bool multi_node_hint;

    uint64 uniqueSQLId;
} PlannedStmt;

typedef struct NodeGroupInfoContext {
    Oid groupOids[MAX_SPECIAL_BUCKETMAP_NUM];
    uint2* bucketMap[MAX_SPECIAL_BUCKETMAP_NUM];
    int    bucketCnt[MAX_SPECIAL_BUCKETMAP_NUM];
    int num_bucketmaps;
} NodeGroupInfoContext;

/*
 * Determine if this plan step needs excution on current dn
 * RestoreMode is always considered as 'have to' execute
 */
#define NeedExecute(plan)                                                               \
    (isRestoreMode ? true                                                               \
                   : (plan->exec_nodes == NULL || plan->exec_nodes->nodeList == NULL || \
                         list_member_int((plan)->exec_nodes->nodeList, u_sess->pgxc_cxt.PGXCNodeId)))

/* macro for fetching the Plan associated with a SubPlan node */
#define exec_subplan_get_plan(plannedstmt, subplan) ((Plan*)list_nth((plannedstmt)->subplans, (subplan)->plan_id - 1))

/* ----------------
 *		Plan node
 *
 * All plan nodes "derive" from the Plan structure by having the
 * Plan structure as the first field.  This ensures that everything works
 * when nodes are cast to Plan's.  (node pointers are frequently cast to Plan*
 * when passed around generically in the executor)
 *
 * We never actually instantiate any Plan nodes; this is just the common
 * abstract superclass for all Plan-type nodes.
 * ----------------
 */
typedef struct Plan {
    NodeTag type;

    int plan_node_id;   /* node id */
    int parent_node_id; /* parent node id */
    RemoteQueryExecType exec_type;

    /*
     * estimated execution costs for plan (see costsize.c for more info)
     */
    Cost startup_cost; /* cost expended before fetching any tuples */
    Cost total_cost;   /* total cost (assuming all tuples fetched) */

    /*
     * planner's estimate of result size of this plan step
     */
    double plan_rows; /* number of global rows plan is expected to emit */
    double multiple;
    int plan_width; /* average row width in bytes */
    int dop;        /* degree of parallelism of current plan */

    /*
     * machine learning model estimations
     */
    double pred_rows;
    double pred_startup_time;
    double pred_total_time;
    long pred_max_memory;
    /*
     * MPPDB Recursive-Union Support
     *
     * - @recursive_union_plan_nodeid
     *		Pointing to its belonging RecursiveUnion's plan node id to indate if we are
     *      under RecursiveUnion
     *
     * - @recursive_union_controller
     *      Indicate if current Plan node is controller node in recursive-union steps
     *
     * - @control_plan_nodeid
     *      Normally, set on the top-plan node of a producer thread, to indicate which
     *      control-plan we need syn-up with
     *
     * - @is_sync_planode
     *      Indicate the current producer thread is the sync-up thread in recursive union,
     *      normally set on produer's top plan node
     *
     * Please note the above four variables is meaningless if a plan node is not under
     * recursive-union's recursive part
     */
    /*
     * plan node id of RecursiveUnion node where current plan node belongs to, 0 for
     * not under recursive-union
     */
    int recursive_union_plan_nodeid;

    /* flag to indicate if it is controller plan node */
    bool recursive_union_controller;

    /* plan node id of Controller plan node, 0 for not in control */
    int control_plan_nodeid;

    /* flag indicate if the current plan node is the sync node (for multi-stream case) */
    bool is_sync_plannode;

    /*
     * Common structural data for all Plan types.
     */
    List* targetlist;      /* target list to be computed at this node */
    List* qual;            /* implicitly-ANDed qual conditions */
    struct Plan* lefttree; /* input plan tree(s) */
    struct Plan* righttree;

    bool ispwj;  /* is it special for partitionwisejoin? */
    int paramno; /* the partition'sn that it is scaning */
    int subparamno; /* the subpartition'sn that it is scaning */

    List* initPlan;    /* Init Plan nodes (un-correlated expr
                        * subselects) */

    List* distributed_keys; /* distributed on which key */
    ExecNodes* exec_nodes;  /* 	List of Datanodes where to execute this plan	*/

    /*
     * Information for management of parameter-change-driven rescanning
     *
     * extParam includes the paramIDs of all external PARAM_EXEC params
     * affecting this plan node or its children.  setParam params from the
     * node's initPlans are not included, but their extParams are.
     *
     * allParam includes all the extParam paramIDs, plus the IDs of local
     * params that affect the node (i.e., the setParams of its initplans).
     * These are _all_ the PARAM_EXEC params that affect this node.
     */
    Bitmapset* extParam;
    Bitmapset* allParam;

    // For vectorized engine, plan produce vector output
    //
    bool vec_output;
    /*
     * @hdfs
     * Mark the foreign scan whether has unique results on one of its
     * output columns.
     */
    bool hasUniqueResults;
    /*
     * Mark the plan whether includes delta table or not.
     */
    bool isDeltaTable;

    /* used to replace work_mem, maxmem in [0], and minmem in [1] */
    int operatorMemKB[2];
    /* allowed max mem after spread */
    int operatorMaxMem;

    bool parallel_enabled; /* Is it run in parallel? */
    bool hasHashFilter;    /* true for this plan has a hashfilter */

    List* var_list;        /* Need bloom filter var list. */
    List* filterIndexList; /* Need used bloomfilter array index. */

    /* used to replace work_mem */
    int** ng_operatorMemKBArray; /* for multiple logic cluster */
    int ng_num;
    double innerdistinct; /* join inner rel distinct estimation value */
    double outerdistinct; /* join outer rel distinct estimation value */

    /* used for ustore partial seq scan */
    List* flatList = NULL; /* flattened targetlist representing columns in query */
} Plan;

/* ----------------
 *	these are defined to avoid confusion problems with "left"
 *	and "right" and "inner" and "outer".  The convention is that
 *	the "left" plan is the "outer" plan and the "right" plan is
 *	the inner plan, but these make the code more readable.
 * ----------------
 */
#define innerPlan(node) (((Plan*)(node))->righttree)
#define outerPlan(node) (((Plan*)(node))->lefttree)

/* ----------------
 *	 Result node -
 *		If no outer plan, evaluate a variable-free targetlist.
 *		If outer plan, return tuples from outer plan (after a level of
 *		projection as shown by targetlist).
 *
 * If resconstantqual isn't NULL, it represents a one-time qualification
 * test (i.e., one that doesn't depend on any variables from the outer plan,
 * so needs to be evaluated only once).
 * ----------------
 */
typedef struct BaseResult {
    Plan plan;
    Node* resconstantqual;
} BaseResult;

typedef struct VecResult : public BaseResult {
} VecResult;

/* ----------------
 *	 ModifyTable node -
 *		Apply rows produced by subplan(s) to result table(s),
 *		by inserting, updating, or deleting.
 *
 * Note that rowMarks and epqParam are presumed to be valid for all the
 * subplan(s); they can't contain any info that varies across subplans.
 * ----------------
 */
typedef struct ModifyTable {
    Plan plan;
    CmdType operation;     /* INSERT, UPDATE, or DELETE */
    bool canSetTag;        /* do we set the command tag/es_processed? */
    List* resultRelations; /* integer list of RT indexes */
    int resultRelIndex;    /* index of first resultRel in plan's list */
    List* plans;           /* plan(s) producing source data */
    List* returningLists;  /* per-target-table RETURNING tlists */
    List* fdwPrivLists;    /* per-target-table FDW private data lists */
    List* rowMarks;        /* PlanRowMarks (non-locking only) */
    int epqParam;          /* ID of Param for EvalPlanQual re-eval */
    bool partKeyUpdated;   /* when update on a partitioned table,true: part key  column
                              is to be updated,false: no part key column is to updated */
#ifdef PGXC
    List* remote_plans; /* per-target-table remote node */
    List* remote_insert_plans;
    List* remote_update_plans;
    List* remote_delete_plans;
#endif
    bool is_dist_insertselect;

    ErrorCacheEntry* cacheEnt; /* Error record cache */

    Index mergeTargetRelation; /* RT index of the merge target */
    List* mergeSourceTargetList;
    List* mergeActionList; /* actions for MERGE */

    UpsertAction upsertAction; /* DUPLICATE KEY UPDATE action */
    List* updateTlist;			/* List of UPDATE target */
    List* exclRelTlist;		   /* target list of the EXECLUDED pseudo relation */
    Index exclRelRTIndex;			 /* RTI of the EXCLUDED pseudo relation */
    Node* upsertWhere;          /* Qualifiers for upsert's update clause to check */

    OpMemInfo mem_info;    /*  Memory info for modify node */
} ModifyTable;

/* ----------------
 *	 Append node -
 *		Generate the concatenation of the results of sub-plans.
 * ----------------
 */
typedef struct Append {
    Plan plan;
    List* appendplans;
} Append;

typedef struct VecAppend : public Append {
} VecAppend;
/* ----------------
 *	 MergeAppend node -
 *		Merge the results of pre-sorted sub-plans to preserve the ordering.
 * ----------------
 */
typedef struct MergeAppend {
    Plan plan;
    List* mergeplans;
    /* remaining fields are just like the sort-key info in struct Sort */
    int numCols;            /* number of sort-key columns */
    AttrNumber* sortColIdx; /* their indexes in the target list */
    Oid* sortOperators;     /* OIDs of operators to sort them by */
    Oid* collations;        /* OIDs of collations */
    bool* nullsFirst;       /* NULLS FIRST/LAST directions */
} MergeAppend;

/* ----------------
 *	RecursiveUnion node -
 *		Generate a recursive union of two subplans.
 *
 * The "outer" subplan is always the non-recursive term, and the "inner"
 * subplan is the recursive term.
 * ----------------
 */
typedef struct RecursiveUnion {
    Plan plan;
    int wtParam; /* ID of Param representing work table */
    /* Remaining fields are zero/null in UNION ALL case */
    int numCols;           /* number of columns to check for
                            * duplicate-ness */
    AttrNumber* dupColIdx; /* their indexes in the target list */
    Oid* dupOperators;     /* equality operators to compare with */
    long numGroups;        /* estimated number of groups in input */
    bool has_inner_stream; /* indicate the underlaying plan node has stream operator (on recursive-term) */
    bool has_outer_stream; /* indicate the underlaying plan node has stream operator (on none-recursive side) */
    bool is_used;
    bool is_correlated;    /* indicate if the recursive union contains correlated term,
                            * in case of correlated term involved, we need broadcast data
                            * to one datanode to execute the recursive CTE in one-DN mode */

    /*
     * StartWith Support containt the pseudo target entry, also not-null indicates
     * a start-with converted recursive union
     *  1. RUITR
     *  2. array_key
     *  3. array_col_nn
     *  4. array_col_nn
     *   ...
     */
    List *internalEntryList;
} RecursiveUnion;

/* ----------------
 *	 StartWithOp node -
 *		Generate the start with connect by operator
 *
 * xxxxxxxxxxxx
 * ----------------
 */

struct CteScan;
typedef struct StartWithOp
{
    Plan plan;
    
    /* other ref attributes */
    CteScan        *cteplan;
    RecursiveUnion *ruplan;

    List *keyEntryList;
    List *colEntryList;
    List *internalEntryList;    /* RUITR, array_key, array_col */
    List *fullEntryList;        /* level, isleaf, iscycle, RUITR, array_key, array_col */

    /*
     * swoptions, normally store some static information that derived from SQL parsing
     * stage, e.g. nocycle, connect_by_type, sibling clause
     */
    StartWithOptions  *swoptions;

    /*
     * swExecOptions, exeuction options for StartWithOp operator
     *
     * store some hint-bit level information supports SWCB runing efficiently, currently
     * we only use last 4-bits to indicate if we need skip some pseudo return column
     * computation
     */
    uint16      swExecOptions;

    List *prcTargetEntryList;
} StartWithOp;

/* ----------------
 *	 BitmapAnd node -
 *		Generate the intersection of the results of sub-plans.
 *
 * The subplans must be of types that yield tuple bitmaps.	The targetlist
 * and qual fields of the plan are unused and are always NIL.
 * ----------------
 */
typedef struct BitmapAnd {
    Plan plan;
    List* bitmapplans;
    bool is_ustore;
} BitmapAnd;

/* ----------------
 *	 BitmapOr node -
 *		Generate the union of the results of sub-plans.
 *
 * The subplans must be of types that yield tuple bitmaps.	The targetlist
 * and qual fields of the plan are unused and are always NIL.
 * ----------------
 */
typedef struct BitmapOr {
    Plan plan;
    List* bitmapplans;
    bool is_ustore;
} BitmapOr;

/*
 * ==========
 * Scan nodes
 * ==========
 */
typedef struct Scan {
    Plan plan;
    Index scanrelid;                 /* relid is index into the range table */
    bool isPartTbl;                  /* Does it scan a partitioned table */
    int itrs;                        /* table partition's number for scan */
    PruningResult* pruningInfo;      /* pruning result for where-clause */
    BucketInfo* bucketInfo;  /* pruning result for buckets */
    ScanDirection partScanDirection; /* specifies the scan ordering */
    /*
     * @hdfs
     * If we use the informational constarint, the following variables will be seted as true.
     * If scan_qual_optimized is true, it means that foreign scan will be optimized by using
     * scan qual in executor phase.
     * If predicate_pushdown_optimized is true, it means that predicate is pushed down and
     * foreign scan will be optimized in the executor phase by using the predicate.
     */
    bool scan_qual_optimized;
    bool predicate_pushdown_optimized;

    /* use struct pointer to avoid including parsenodes.h here */
    TableSampleClause* tablesample;

    /*  Memory info for scan node, now it just used on indexscan, indexonlyscan, bitmapscan, dfsindexscan */
    OpMemInfo mem_info;
    bool is_inplace;
    bool scanBatchMode;
    double tableRows;
} Scan;

/* ----------------
 *		sequential scan node
 * ----------------
 */
typedef Scan SeqScan;

/*
 * ==========
 * Column Store Scan nodes
 * ==========
 */
typedef struct CStoreScan : public Scan {
    /* Some optimization information */
    double selectionRatio;         /* row output / rows scanned */
    List* cstorequal;              /* push predicate down to cstorescan */
    List* minMaxInfo;              /* min/max information, mark get this column min or max value. */
    RelstoreType relStoreLocation; /* The store position information. */
    bool is_replica_table;         /* Is a replication table? */
} CStoreScan;

/*
 * ==========
 * Dfs Store Scan nodes. When the relation is CU format, we use CstoreScan
 * to scan data.
 * ==========
 */
typedef struct DfsScan : public Scan {
    RelstoreType relStoreLocation;
    char* storeFormat; /* The store format, the ORC format only is supported for dfsScan. */
    List* privateData; /* Private data. */
} DfsScan;

/*
 * ==========
 * Time Series Store Scan nodes
 * ==========
 */
typedef struct TsStoreScan: public Scan {
    /* Some optimization information */
    double      selectionRatio;     /* row output / rows scanned */
    List        *tsstorequal;       /* push predicate down to tsstorescan */
    List        *minMaxInfo;        /* min/max information, mark get this column min or max value.*/
    RelstoreType relStoreLocation;  /* The store position information. */
    bool        is_replica_table;   /* Is a replication table? */
    AttrNumber   sort_by_time_colidx;  /* If is sort by tstime limit n */
    int          limit; /* If is limit n */
    bool         is_simple_scan; /* If is sort by tstime limit n */
    bool         has_sort; /* If is have sort node */
    int          series_func_calls; /* series function calls time  */
    int          top_key_func_arg; /* second arg of top_key function */
} TsStoreScan;

/* ----------------
 *		index scan node
 *
 * indexqualorig is an implicitly-ANDed list of index qual expressions, each
 * in the same form it appeared in the query WHERE condition.  Each should
 * be of the form (indexkey OP comparisonval) or (comparisonval OP indexkey).
 * The indexkey is a Var or expression referencing column(s) of the index's
 * base table.	The comparisonval might be any expression, but it won't use
 * any columns of the base table.  The expressions are ordered by index
 * column position (but items referencing the same index column can appear
 * in any order).  indexqualorig is used at runtime only if we have to recheck
 * a lossy indexqual.
 *
 * indexqual has the same form, but the expressions have been commuted if
 * necessary to put the indexkeys on the left, and the indexkeys are replaced
 * by Var nodes identifying the index columns (their varno is INDEX_VAR and
 * their varattno is the index column number).
 *
 * indexorderbyorig is similarly the original form of any ORDER BY expressions
 * that are being implemented by the index, while indexorderby is modified to
 * have index column Vars on the left-hand side.  Here, multiple expressions
 * must appear in exactly the ORDER BY order, and this is not necessarily the
 * index column order.	Only the expressions are provided, not the auxiliary
 * sort-order information from the ORDER BY SortGroupClauses; it's assumed
 * that the sort ordering is fully determinable from the top-level operators.
 * indexorderbyorig is unused at run time, but is needed for EXPLAIN.
 * (Note these fields are used for amcanorderbyop cases, not amcanorder cases.)
 *
 * indexorderdir specifies the scan ordering, for indexscans on amcanorder
 * indexes (for other indexes it should be "don't care").
 * ----------------
 */
typedef struct IndexScan {
    Scan scan;
    Oid indexid;                 /* OID of index to scan */
    char* indexname;             /* Index name of index to scan	*/
    List* indexqual;             /* list of index quals (usually OpExprs) */
    List* indexqualorig;         /* the same in original form */
    List* indexorderby;          /* list of index ORDER BY exprs */
    List* indexorderbyorig;      /* the same in original form */
    ScanDirection indexorderdir; /* forward or backward or don't care */
    bool usecstoreindex;         /* mark the column store index */
    Index indexscan_relid;       /* Hack for column store index, treat the index as normal relation */
    List* idx_cstorequal;        /* For column store, this contains only quals pushdownable to
                                    storage engine */
    List* cstorequal;            /* quals that can be pushdown to cstore base table */
    List* targetlist;            /* Hack for column store index, target list to be computed at this node */
    bool index_only_scan;
    bool is_ustore;
} IndexScan;

/* ----------------
 *		index-only scan node
 *
 * IndexOnlyScan is very similar to IndexScan, but it specifies an
 * index-only scan, in which the data comes from the index not the heap.
 * Because of this, *all* Vars in the plan node's targetlist, qual, and
 * index expressions reference index columns and have varno = INDEX_VAR.
 * Hence we do not need separate indexqualorig and indexorderbyorig lists,
 * since their contents would be equivalent to indexqual and indexorderby.
 *
 * To help EXPLAIN interpret the index Vars for display, we provide
 * indextlist, which represents the contents of the index as a targetlist
 * with one TLE per index column.  Vars appearing in this list reference
 * the base table, and this is the only field in the plan node that may
 * contain such Vars.
 * ----------------
 */
typedef struct IndexOnlyScan {
    Scan scan;
    Oid indexid;                 /* OID of index to scan */
    List* indexqual;             /* list of index quals (usually OpExprs) */
    List* indexorderby;          /* list of index ORDER BY exprs */
    List* indextlist;            /* TargetEntry list describing index's cols */
    ScanDirection indexorderdir; /* forward or backward or don't care */
} IndexOnlyScan;

/* ----------------
 *		bitmap index scan node
 *
 * BitmapIndexScan delivers a bitmap of potential tuple locations;
 * it does not access the heap itself.	The bitmap is used by an
 * ancestor BitmapHeapScan node, possibly after passing through
 * intermediate BitmapAnd and/or BitmapOr nodes to combine it with
 * the results of other BitmapIndexScans.
 *
 * The fields have the same meanings as for IndexScan, except we don't
 * store a direction flag because direction is uninteresting.
 *
 * In a BitmapIndexScan plan node, the targetlist and qual fields are
 * not used and are always NIL.  The indexqualorig field is unused at
 * run time too, but is saved for the benefit of EXPLAIN.
 * ----------------
 */
typedef struct BitmapIndexScan {
    Scan scan;
    Oid indexid;         /* OID of index to scan */
    char* indexname;     /*	name of index to scan */
    List* indexqual;     /* list of index quals (OpExprs) */
    List* indexqualorig; /* the same in original form */
    bool is_ustore;
} BitmapIndexScan;

/* ----------------
 *		bitmap sequential scan node
 *
 * This needs a copy of the qual conditions being used by the input index
 * scans because there are various cases where we need to recheck the quals;
 * for example, when the bitmap is lossy about the specific rows on a page
 * that meet the index condition.
 * ----------------
 */
typedef struct BitmapHeapScan {
    Scan scan;
    List* bitmapqualorig; /* index quals, in standard expr form */
} BitmapHeapScan;

/* ----------------
 *		Column Store index scan node
 *
 * ----------------
 */
typedef struct CStoreIndexScan {
    Scan scan;
    Oid indexid;                   /* OID of index to scan */
    List* indexqual;               /* list of index quals (usually OpExprs) */
    List* indexqualorig;           /* the same in original form */
    List* indexorderby;            /* list of index ORDER BY exprs */
    List* indexorderbyorig;        /* the same in original form */
    ScanDirection indexorderdir;   /* forward or backward or don't care */

    List* baserelcstorequal;       /* for base relation of index */
    List* cstorequal;              /* quals that can be pushdown to cstore base table */
    List* indextlist;
    RelstoreType relStoreLocation; /* The store position information. */
    bool indexonly;                /* flag indicates index only scan */
} CStoreIndexScan;

typedef struct CStoreIndexCtidScan : public BitmapIndexScan {
    List* indextlist;
    List* cstorequal;
} CStoreIndexCtidScan;

typedef struct CStoreIndexHeapScan : public BitmapHeapScan {
} CStoreIndexHeapScan;

typedef struct CStoreIndexAnd : public BitmapAnd {
} CStoreIndexAnd;

typedef struct CStoreIndexOr : public BitmapOr {
} CStoreIndexOr;

/* ----------------
 *		DFS Store index scan node
 */
typedef struct DfsIndexScan {
    Scan scan;
    Oid indexid;                   /* OID of index to scan */
    List* indextlist;              /* list of index target entry which represents the column of base-relation */
    List* indexqual;               /* list of index quals (usually OpExprs) */
    List* indexqualorig;           /* the same in original form */
    List* indexorderby;            /* list of index ORDER BY exprs */
    List* indexorderbyorig;        /* the same in original form */
    ScanDirection indexorderdir;   /* forward or backward or don't care */
    RelstoreType relStoreLocation; /* The store position information. */
    List* cstorequal;              /* quals that can be pushdown to cstore base table */
    List* indexScantlist;          /* list of target column for scanning on index table */
    DfsScan* dfsScan;              /* the inner object for scanning the base-relation */
    bool indexonly;                /* flag indicates index only scan */
} DfsIndexScan;

/* ----------------
 *		tid scan node
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant" or "CTID = ANY(pseudoconstant_array)".
 * ----------------
 */
typedef struct TidScan {
    Scan scan;
    List* tidquals; /* qual(s) involving CTID = something */
} TidScan;

/* ----------------
 *		subquery scan node
 *
 * SubqueryScan is for scanning the output of a sub-query in the range table.
 * We often need an extra plan node above the sub-query's plan to perform
 * expression evaluations (which we can't push into the sub-query without
 * risking changing its semantics).  Although we are not scanning a physical
 * relation, we make this a descendant of Scan anyway for code-sharing
 * purposes.
 *
 * Note: we store the sub-plan in the type-specific subplan field, not in
 * the generic lefttree field as you might expect.	This is because we do
 * not want plan-tree-traversal routines to recurse into the subplan without
 * knowing that they are changing Query contexts.
 * ----------------
 */
typedef struct SubqueryScan {
    Scan scan;
    Plan* subplan;
} SubqueryScan;

typedef struct VecSubqueryScan : public SubqueryScan {
} VecSubqueryScan;

/* ----------------
 *		FunctionScan node
 * ----------------
 */
typedef struct FunctionScan {
    Scan scan;
    Node* funcexpr;          /* expression tree for func call */
    List* funccolnames;      /* output column names (string Value nodes) */
    List* funccoltypes;      /* OID list of column type OIDs */
    List* funccoltypmods;    /* integer list of column typmods */
    List* funccolcollations; /* OID list of column collation OIDs */
} FunctionScan;

/* ----------------
 *		ValuesScan node
 * ----------------
 */
typedef struct ValuesScan {
    Scan scan;
    List* values_lists; /* list of expression lists */
} ValuesScan;

/* ----------------
 *		CteScan node
 * ----------------
 */
typedef struct CteScan {
    Scan scan;
    int ctePlanId;           /* ID of init SubPlan for CTE */
    int cteParam;            /* ID of Param representing CTE output */
    RecursiveUnion* subplan; /* subplan of CteScan, must be RecursiveUnion */

    CommonTableExpr *cteRef; /* Reference of curernt CteScan node's expr */

    /* These fields are only valid for Hierarchical Query(start with) only */

    /*
     * - pseudoReturnTargetEntryList
     *
     * Hold the TargetEntry reference for PRC a.w.k. "pseudo return columns"
     * [1]. level
     * [2]. connect_by_isleaf
     * [3]. connect_by_iscycle
     * [4]. rownum
     */
    List *prcTargetEntryList;

    /*
     * - internalEntryList
     *
     * Hold the internal TargetEntry for Hierarchical Query execution (not visible)
     *  1. RUITR
     *  2. array_column1
     *  3. array_column2
     *   ...
     */
    List *internalEntryList;
} CteScan;

/* ----------------
 *		WorkTableScan node
 * ----------------
 */
typedef struct WorkTableScan {
    Scan scan;
    int wtParam; /* ID of Param representing work table */

    /* indicate it is workable from start-with */
    bool forStartWith;
} WorkTableScan;

/* ----------------
 *		ForeignScan node
 *
 * fdw_exprs and fdw_private are both under the control of the foreign-data
 * wrapper, but fdw_exprs is presumed to contain expression trees and will
 * be post-processed accordingly by the planner; fdw_private won't be.
 * Note that everything in both lists must be copiable by copyObject().
 * One way to store an arbitrary blob of bytes is to represent it as a bytea
 * Const.  Usually, though, you'll be better off choosing a representation
 * that can be dumped usefully by nodeToString().
 * ----------------
 */
typedef struct ForeignScan {
    Scan scan;

    Oid scan_relid;    /* Oid of the scan relation */
    List* fdw_exprs;   /* expressions that FDW may evaluate */
    List* fdw_private; /* private data for FDW */
    bool fsSystemCol;  /* true if any "system column" is needed */

    bool needSaveError;
    ErrorCacheEntry* errCache; /* Error record cache */

    /* This is used in hdfs foreign scan to store prunning information. */
    List* prunningResult;

    RelationMetaData* rel;   /* the meta data of the foreign table */
    ForeignOptions* options; /* the configuration options */

    /* number of files(objects) to be scanned. just valid for planner */
    int64 objectNum;
    BloomFilterSet** bloomFilterSet;
    int bfNum; /* the number of bloomfilter object. */

    /* set true in do_query_for_planrouter() for ForeignScan sent to
     * the compute pool
     */
    bool in_compute_pool;
    bool not_use_bloomfilter; /* set true in ExecInitXXXX() of planrouter node */
} ForeignScan;

/* ----------------
 *	   ExtensiblePlan node
 *
 * The comments for ForeignScan's fdw_exprs, fdw_private, fdw_scan_tlist,
 * and fs_relids fields apply equally to ExtensiblePlan's extensible_exprs,
 * extensible_data, extensible_plan_tlist, and extensible_relids fields.  The
 * convention of setting scan.scanrelid to zero for joins applies as well.
 *
 * Note that since Plan trees can be copied, extensible scan providers *must*
 * fit all plan data they need into those fields; embedding ExtensiblePlan in
 * a larger struct will not work.
 * ----------------
 */
struct ExtensiblePlan;

typedef struct ExtensiblePlanMethods {
    char* ExtensibleName;

    /* Create execution state (ExtensiblePlanState) from a ExtensiblePlan plan node */
    Node* (*CreateExtensiblePlanState)(struct ExtensiblePlan* cscan);
} ExtensiblePlanMethods;

typedef struct ExtensiblePlan {
    Scan scan;

    uint32 flags;                  /* mask of EXTENSIBLEPATH_* flags, see relation.h */

    List* extensible_plans;        /* list of Plan nodes, if any */

    List* extensible_exprs;        /* expressions that extensible code may evaluate */

    List* extensible_private;      /* private data for extensible code */

    List* extensible_plan_tlist;   /* optional tlist describing scan
                                    * tuple */
    Bitmapset* extensible_relids;  /* RTIs generated by this scan */

    ExtensiblePlanMethods* methods;
} ExtensiblePlan;
/*
 * ==========
 * Join nodes
 * ==========
 */

/* ----------------
 *		Join node
 *
 * jointype:	rule for joining tuples from left and right subtrees
 * joinqual:	qual conditions that came from JOIN/ON or JOIN/USING
 *				(plan.qual contains conditions that came from WHERE)
 *
 * When jointype is INNER, joinqual and plan.qual are semantically
 * interchangeable.  For OUTER jointypes, the two are *not* interchangeable;
 * only joinqual is used to determine whether a match has been found for
 * the purpose of deciding whether to generate null-extended tuples.
 * (But plan.qual is still applied before actually returning a tuple.)
 * For an outer join, only joinquals are allowed to be used as the merge
 * or hash condition of a merge or hash join.
 * ----------------
 */
typedef struct Join {
    Plan plan;
    JoinType jointype;
    List* joinqual; /* JOIN quals (in addition to plan.qual) */
    /*
     * @hdfs
     * This flag will be set as true if we use informational constraint
     * in order to optimize join plan.
     */
    bool optimizable;
    List* nulleqqual;

    uint32 skewoptimize;
} Join;

/* ----------------
 *		nest loop join node
 *
 * The nestParams list identifies any executor Params that must be passed
 * into execution of the inner subplan carrying values from the current row
 * of the outer subplan.  Currently we restrict these values to be simple
 * Vars, but perhaps someday that'd be worth relaxing.  (Note: during plan
 * creation, the paramval can actually be a PlaceHolderVar expression; but it
 * must be a Var with varno OUTER_VAR by the time it gets to the executor.)
 * ----------------
 */
typedef struct NestLoop {
    Join join;
    List* nestParams; /* list of NestLoopParam nodes */
    bool materialAll;
} NestLoop;

typedef struct VecNestLoop : public NestLoop {
} VecNestLoop;

typedef struct NestLoopParam {
    NodeTag type;
    int paramno;   /* number of the PARAM_EXEC Param to set */
    Var* paramval; /* outer-relation Var to assign to Param */
} NestLoopParam;

/* ----------------
 *		merge join node
 *
 * The expected ordering of each mergeable column is described by a btree
 * opfamily OID, a collation OID, a direction (BTLessStrategyNumber or
 * BTGreaterStrategyNumber) and a nulls-first flag.  Note that the two sides
 * of each mergeclause may be of different datatypes, but they are ordered the
 * same way according to the common opfamily and collation.  The operator in
 * each mergeclause must be an equality operator of the indicated opfamily.
 * ----------------
 */
typedef struct MergeJoin {
    Join join;
    List* mergeclauses; /* mergeclauses as expression trees */
    /* these are arrays, but have the same length as the mergeclauses list: */
    Oid* mergeFamilies;    /* per-clause OIDs of btree opfamilies */
    Oid* mergeCollations;  /* per-clause OIDs of collations */
    int* mergeStrategies;  /* per-clause ordering (ASC or DESC) */
    bool* mergeNullsFirst; /* per-clause nulls ordering */
} MergeJoin;

typedef struct VecMergeJoin : public MergeJoin {
} VecMergeJoin;
/* ----------------
 *		hash join node
 * ----------------
 */
typedef struct HashJoin {
    Join join;
    List* hashclauses;
    bool streamBothSides;
    bool transferFilterFlag;
    bool rebuildHashTable;
    bool isSonicHash;
    OpMemInfo mem_info; /* Memory info for inner hash table */
    double joinRows;
} HashJoin;

/* ----------------
 *		materialization node
 * ----------------
 */
typedef struct Material {
    Plan plan;
    bool materialize_all; /* if all data should be materialized at the first time */
    OpMemInfo mem_info;   /* Memory info for material */
} Material;

typedef struct VecMaterial : public Material {
} VecMaterial;

/* ----------------
 *		sort node
 * ----------------
 */
typedef struct Sort {
    Plan plan;
    int numCols;            /* number of sort-key columns */
    AttrNumber* sortColIdx; /* their indexes in the target list */
    Oid* sortOperators;     /* OIDs of operators to sort them by */
    Oid* collations;        /* OIDs of collations */
    bool* nullsFirst;       /* NULLS FIRST/LAST directions */
#ifdef PGXC
    bool srt_start_merge;  /* No need to create the sorted runs. The
                            * underlying plan provides those runs. Merge
                            * them.
                            */
#endif                     /* PGXC */
    OpMemInfo mem_info;    /* Memory info for sort */
} Sort;

typedef struct VecSort : public Sort {
} VecSort;

/* ---------------
 *	 group node -
 *		Used for queries with GROUP BY (but no aggregates) specified.
 *		The input must be presorted according to the grouping columns.
 * ---------------
 */
typedef struct Group {
    Plan plan;
    int numCols;           /* number of grouping columns */
    AttrNumber* grpColIdx; /* their indexes in the target list */
    Oid* grpOperators;     /* equality operators to compare with */
} Group;

typedef struct VecGroup : public Group {
} VecGroup;
/* ---------------
 *		aggregate node
 *
 * An Agg node implements plain or grouped aggregation.  For grouped
 * aggregation, we can work with presorted input or unsorted input;
 * the latter strategy uses an internal hashtable.
 *
 * Notice the lack of any direct info about the aggregate functions to be
 * computed.  They are found by scanning the node's tlist and quals during
 * executor startup.  (It is possible that there are no aggregate functions;
 * this could happen if they get optimized away by constant-folding, or if
 * we are using the Agg node to implement hash-based grouping.)
 * ---------------
 */
typedef enum AggStrategy {
    AGG_PLAIN,  /* simple agg across all input rows */
    AGG_SORTED, /* grouped agg, input must be sorted */
    AGG_HASHED  /* grouped agg, use internal hashtable */
} AggStrategy;

#ifdef STREAMPLAN
typedef enum SAggMethod {
    OPTIMAL_AGG,                         /* 0. chose the optimal hash agg plan according to costs. */
    DN_AGG_CN_AGG,                       /* 1. */
    DN_REDISTRIBUTE_AGG,                 /* 2. */
    DN_AGG_REDISTRIBUTE_AGG,             /* 3. */
    DN_REDISTRIBUTE_AGG_CN_AGG,          /* 1+. according to DN_AGG_CN_AGG */
    DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG /* 3+. according to DN_AGG_REDISTRIBUTE_AGG */
} SAggMethod;

/* flags bits for SAggMethod choose */
#define ALLOW_ALL_AGG 0X00   /* Any AggMethod may be selected */
#define DISALLOW_CN_AGG 0x01 /* disallow DN_AGG_CN_AGG, DN_REDISTRIBUTE_AGG_CN_AGG */
#define FOREC_SLVL_AGG 0x02  /* force single level agg, actually disallow CN_AGG meanwhile */

#endif

typedef struct Agg {
    Plan plan;
    AggStrategy aggstrategy;
    int numCols;           /* number of grouping columns */
    AttrNumber* grpColIdx; /* their indexes in the target list */
    Oid* grpOperators;     /* equality operators to compare with */
    long numGroups;        /* estimated number of groups in input */
    List* groupingSets;    /* grouping sets to use */
    List* chain;           /* chained Agg/Sort nodes */
#ifdef PGXC
    bool is_final;        /* apply final agg directly on the data received from remote Datanodes */
    bool single_node;     /* We can finalise the aggregates on the datanode/s  */
#endif                    /* PGXC */
    Bitmapset* aggParams; /* IDs of Params used in Aggref inputs */
    OpMemInfo mem_info;   /* Memory info for hashagg */
    bool is_sonichash;    /* allowed to use sonic hash routine or not */
    bool is_dummy;        /* just for coop analysis, if true, agg node does nothing */
    uint32 skew_optimize; /* skew optimize method for agg */
    bool   unique_check;  /* we will report an error when meet duplicate in unique check mode */
} Agg;

/* ----------------
 *		window aggregate node
 * ----------------
 */
typedef struct WindowAgg {
    Plan plan;
    Index winref;           /* ID referenced by window functions */
    int partNumCols;        /* number of columns in partition clause */
    AttrNumber* partColIdx; /* their indexes in the target list */
    Oid* partOperators;     /* equality operators for partition columns */
    int ordNumCols;         /* number of columns in ordering clause */
    AttrNumber* ordColIdx;  /* their indexes in the target list */
    Oid* ordOperators;      /* equality operators for ordering columns */
    int frameOptions;       /* frame_clause options, see WindowDef */
    Node* startOffset;      /* expression for starting bound, if any */
    Node* endOffset;        /* expression for ending bound, if any */
    OpMemInfo mem_info;     /* Memory info for window agg with agg func */
} WindowAgg;

typedef struct VecWindowAgg : public WindowAgg {
} VecWindowAgg;
/* ----------------
 *		unique node
 * ----------------
 */
typedef struct Unique {
    Plan plan;
    int numCols;            /* number of columns to check for uniqueness */
    AttrNumber* uniqColIdx; /* their indexes in the target list */
    Oid* uniqOperators;     /* equality operators to compare with */
} Unique;

/* ----------------
 *		hash build node
 *
 * If the executor is supposed to try to apply skew join optimization, then
 * skewTable/skewColumn/skewInherit identify the outer relation's join key
 * column, from which the relevant MCV statistics can be fetched.  Also, its
 * type information is provided to save a lookup.
 * ----------------
 */
typedef struct Hash {
    Plan plan;
    Oid skewTable;         /* outer join key's table OID, or InvalidOid */
    AttrNumber skewColumn; /* outer join key's column #, or zero */
    bool skewInherit;      /* is outer join rel an inheritance tree? */
    Oid skewColType;       /* datatype of the outer key column */
    int32 skewColTypmod;   /* typmod of the outer key column */
                           /* all other info is in the parent HashJoin node */
} Hash;

/* ----------------
 *		setop node
 * ----------------
 */
typedef enum SetOpCmd { SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL, SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL } SetOpCmd;

typedef enum SetOpStrategy {
    SETOP_SORTED, /* input must be sorted */
    SETOP_HASHED  /* use internal hashtable */
} SetOpStrategy;

typedef struct SetOp {
    Plan plan;
    SetOpCmd cmd;           /* what to do */
    SetOpStrategy strategy; /* how to do it */
    int numCols;            /* number of columns to check for
                             * duplicate-ness */
    AttrNumber* dupColIdx;  /* their indexes in the target list */
    Oid* dupOperators;      /* equality operators to compare with */
    AttrNumber flagColIdx;  /* where is the flag column, if any */
    int firstFlag;          /* flag value for first input relation */
    long numGroups;         /* estimated number of groups in input */
    OpMemInfo mem_info;     /* Memory info for hashagg set op */
} SetOp;

/* ----------------
 *		lock-rows node
 *
 * rowMarks identifies the rels to be locked by this node; it should be
 * a subset of the rowMarks listed in the top-level PlannedStmt.
 * epqParam is a Param that all scan nodes below this one must depend on.
 * It is used to force re-evaluation of the plan during EvalPlanQual.
 * ----------------
 */
typedef struct LockRows {
    Plan plan;
    List* rowMarks; /* a list of PlanRowMark's */
    int epqParam;   /* ID of Param for EvalPlanQual re-eval */
} LockRows;

/* ----------------
 *		limit node
 *
 * Note: as of Postgres 8.2, the offset and count expressions are expected
 * to yield int8, rather than int4 as before.
 * ----------------
 */
typedef struct Limit {
    Plan plan;
    Node* limitOffset; /* OFFSET parameter, or NULL if none */
    Node* limitCount;  /* COUNT parameter, or NULL if none */
} Limit;

typedef struct VecLimit : public Limit {
} VecLimit;

/*
 * RowMarkType -
 *	  enums for types of row-marking operations
 *
 * When doing UPDATE, DELETE, or SELECT FOR [KEY] UPDATE/SHARE, we have to uniquely
 * identify all the source rows, not only those from the target relations, so
 * that we can perform EvalPlanQual rechecking at need.  For plain tables we
 * can just fetch the TID, the same as for a target relation.  Otherwise (for
 * example for VALUES or FUNCTION scans) we have to copy the whole row value.
 * The latter is pretty inefficient but fortunately the case is not
 * performance-critical in practice.
 */
typedef enum RowMarkType {
    ROW_MARK_EXCLUSIVE,      /* obtain exclusive tuple lock */
    ROW_MARK_NOKEYEXCLUSIVE, /* obtain no-key exclusive tuple lock */
    ROW_MARK_SHARE,          /* obtain shared tuple lock */
    ROW_MARK_KEYSHARE,       /* obtain keyshare tuple lock */
    ROW_MARK_REFERENCE,      /* just fetch the TID */
    ROW_MARK_COPY,           /* physically copy the row value */
    ROW_MARK_COPY_DATUM      /* physically copy the datum of every row column */
} RowMarkType;

#define RowMarkRequiresRowShareLock(marktype) ((marktype) <= ROW_MARK_KEYSHARE)

/*
 * PlanRowMark -
 *	   plan-time representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * When doing UPDATE, DELETE, or SELECT FOR [KEY] UPDATE/SHARE, we create a separate
 * PlanRowMark node for each non-target relation in the query.	Relations that
 * are not specified as FOR [KEY] UPDATE/SHARE are marked ROW_MARK_REFERENCE (if
 * real tables) or ROW_MARK_COPY (if not).
 *
 * Initially all PlanRowMarks have rti == prti and isParent == false.
 * When the planner discovers that a relation is the root of an inheritance
 * tree, it sets isParent true, and adds an additional PlanRowMark to the
 * list for each child relation (including the target rel itself in its role
 * as a child).  The child entries have rti == child rel's RT index and
 * prti == parent's RT index, and can therefore be recognized as children by
 * the fact that prti != rti.
 *
 * The planner also adds resjunk output columns to the plan that carry
 * information sufficient to identify the locked or fetched rows.  For
 * tables (markType != ROW_MARK_COPY), these columns are named
 *		tableoid%u			OID of table
 *		ctid%u				TID of row
 * The tableoid column is only present for an inheritance hierarchy.
 * When markType == ROW_MARK_COPY, there is instead a single column named
 *		wholerow%u			whole-row value of relation
 * In all three cases, %u represents the rowmark ID number (rowmarkId).
 * This number is unique within a plan tree, except that child relation
 * entries copy their parent's rowmarkId.  (Assigning unique numbers
 * means we needn't renumber rowmarkIds when flattening subqueries, which
 * would require finding and renaming the resjunk columns as well.)
 * Note this means that all tables in an inheritance hierarchy share the
 * same resjunk column names.  However, in an inherited UPDATE/DELETE the
 * columns could have different physical column numbers in each subplan.
 */
typedef struct PlanRowMark {
    NodeTag type;
    Index rti;            /* range table index of markable relation */
    Index prti;           /* range table index of parent relation */
    Index rowmarkId;      /* unique identifier for resjunk columns */
    RowMarkType markType; /* see enum above */
    bool noWait;          /* NOWAIT option */
    int waitSec;      /* WAIT time Sec */
    bool isParent;        /* true if this is a "dummy" parent entry */
    int numAttrs;         /* number of attributes in subplan */
    Bitmapset* bms_nodeids;
} PlanRowMark;

/*
 * Plan invalidation info
 *
 * We track the objects on which a PlannedStmt depends in two ways:
 * relations are recorded as a simple list of OIDs, and everything else
 * is represented as a list of PlanInvalItems.	A PlanInvalItem is designed
 * to be used with the syscache invalidation mechanism, so it identifies a
 * system catalog entry by cache ID and hash value.
 */
typedef struct PlanInvalItem {
    NodeTag type;
    int cacheId;      /* a syscache ID, see utils/syscache.h */
    uint32 hashValue; /* hash value of object's cache lookup key */
} PlanInvalItem;
/*
 * Target	: data partition
 * Brief	: structure definition about partition iteration
 */
typedef struct PartIteratorParam {
    NodeTag type;
    int paramno;
    int subPartParamno;
} PartIteratorParam;

typedef struct PartIterator {
    Plan plan;
    PartitionType partType; /* partition type, range or interval? */
    int itrs;               /* the number of the partitions */
    ScanDirection direction;
    PartIteratorParam* param;
    /*
     * Below three variables are used to record starting partition id, ending partition id and number of
     * partitions.
     */
    int startPartitionId;   /* Used in parallel execution to record smp worker starting partition id. */
    int endPartitionId;     /* Used in parallel execution to record smp worker ending partition id.  */
} PartIterator;
typedef struct GlobalPartIterator {
    int curItrs;
    PruningResult* pruningResult;
} GlobalPartIterator;

typedef struct VecPartIterator : public PartIterator {
} VecPartIterator;
/*
 * Vector Plan Nodes.
 *
 */

/* ----------------
 *	vector hash join node
 * ----------------
 */

// comment to  avoid g++ warning
// vector aggregation.

typedef struct HashJoin VecHashJoin;
typedef struct Agg VecAgg;
typedef struct SetOp VecSetOp;
typedef struct Unique VecUnique;

typedef struct RowToVec {
    Plan plan;
} RowToVec;

typedef struct VecToRow {
    Plan plan;
} VecToRow;

inline bool IsVecOutput(Plan* p)
{
    return p && p->vec_output;
}

typedef struct VecForeignScan : public ForeignScan {
} VecForeignScan;

typedef struct VecModifyTable : public ModifyTable {
} VecModifyTable;

static inline bool IsJoinPlan(Node* node)
{
    return IsA(node, Join) || IsA(node, NestLoop) || IsA(node, VecNestLoop) || IsA(node, MergeJoin) ||
        IsA(node, VecMergeJoin) || IsA(node, HashJoin) || IsA(node, VecHashJoin);
}

/*
 * DB4AI
 */
 
// Training model node
struct ModelHyperparameters;
typedef struct TrainModel {
    Plan        plan;
    AlgorithmML algorithm;
    int         configurations;     // 1..N configurations for HPO
    const ModelHyperparameters **hyperparameters;  // one for each configuration
    MemoryContext cxt;              // to store models
} TrainModel;

#endif /* PLANNODES_H */

