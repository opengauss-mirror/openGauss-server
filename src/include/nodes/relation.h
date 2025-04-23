/* -------------------------------------------------------------------------
 *
 * relation.h
 *	  Definitions for planner's internal data structures.
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/relation.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RELATION_H
#define RELATION_H

#include "access/sdir.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "storage/buf/block.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"

#include "optimizer/bucketinfo.h"

#ifdef USE_SPQ
/*
 * ApplyShareInputContext is used in different stages of ShareInputScan
 * processing. This is mostly used as working area during the stages, but
 * some information is also carried through multiple stages.
 */
typedef struct ApplyShareInputContextPerShare {
    int producer_slice_id;
    Bitmapset *participant_slices;
} ApplyShareInputContextPerShare;
 
struct PlanSlice;
struct Plan;
 
typedef struct ApplyShareInputContext {
    /* curr_rtable is used by all stages when traversing into subqueries */
    List *curr_rtable;
 
    /*
     * Populated in dag_to_tree() (or collect_shareinput_producers() for ORCA),
     * used in replace_shareinput_targetlists()
     */
    Plan **shared_plans;
    int shared_input_count;
 
    /*
     * State for replace_shareinput_targetlists()
     */
    int *share_refcounts;
    int share_refcounts_sz; /* allocated sized of 'share_refcounts' */
 
    /*
     * State for apply_sharinput_xslice() walkers.
     */
    PlanSlice *slices;                             /* root->glob->slices */
    List *motStack;                                /* stack of motionIds leading to current node */
    ApplyShareInputContextPerShare *shared_inputs; /* one for each share */
    Bitmapset *qdShares;                           /* share_ids that are referenced from QD slices */
} ApplyShareInputContext;
#endif

/*
 * Determines if query has to be launched
 * on Coordinators only (SEQUENCE DDL),
 * on Datanodes (normal Remote Queries),
 * or on all openGauss nodes (Utilities and DDL).
 */
typedef enum
{
    EXEC_ON_DATANODES,
    EXEC_ON_COORDS,
    EXEC_ON_ALL_NODES,
    EXEC_ON_NONE
} RemoteQueryExecType;

#define EXEC_CONTAIN_COORDINATOR(exec_type) \
    ((exec_type) == EXEC_ON_ALL_NODES || (exec_type) == EXEC_ON_COORDS)

#define EXEC_CONTAIN_DATANODE(exec_type) \
    ((exec_type) == EXEC_ON_ALL_NODES || (exec_type) == EXEC_ON_DATANODES)

/*
 * When looking for a "cheapest path", this enum specifies whether we want
 * cheapest startup cost or cheapest total cost.
 */
typedef enum CostSelector { STARTUP_COST, TOTAL_COST } CostSelector;

/* Different rules are used for path generation */
typedef enum {
    NO_PATH_GEN_RULE = 0,
    BTREE_INDEX_CONTAIN_UNIQUE_COLS = 1 /* an equivalence constraint btree index scan contains unique cols */
} RulesForPathGen;

/*
 * The cost estimate produced by cost_qual_eval() includes both a one-time
 * (startup) cost, and a per-tuple cost.
 */
typedef struct QualCost {
    Cost startup;   /* one-time cost */
    Cost per_tuple; /* per-evaluation cost */
} QualCost;

/*
 * Costing aggregate function execution requires these statistics about
 * the aggregates to be executed by a given Agg node.  Note that transCost
 * includes the execution costs of the aggregates' input expressions.
 */
typedef struct AggClauseCosts {
    int numAggs;             /* total number of aggregate functions */
    int numOrderedAggs;      /* number that use ORDER BY */
    List* exprAggs;          /* expression that use DISTINCT */
    QualCost transCost;      /* total per-input-row execution costs */
    Cost finalCost;          /* total costs of agg final functions */
    Size transitionSpace;    /* space for pass-by-ref transition data */
    bool hasdctDnAggs;       /* has agg-once functions in distinct expr */
    bool hasDnAggs;          /* has agg-once functions not in distinct expr */
    bool unhashable;         /* if distinct node is not hashable */
    bool hasPolymorphicType; /* if aggregate expressions have polymorphic pseudotype */
    int aggWidth;            /* total width of agg function */
} AggClauseCosts;

/*
 * This enum identifies the different types of "upper" (post-scan/join)
 * relations that we might deal with during planning.
 */
typedef enum UpperRelationKind {
    UPPERREL_INIT,              /* is a base rel */
    UPPERREL_SETOP,             /* result of UNION/INTERSECT/EXCEPT, if any */
    UPPERREL_GROUP_AGG,         /* result of grouping/aggregation, if any */
    UPPERREL_WINDOW,            /* result of window functions, if any */
    UPPERREL_DISTINCT,          /* result of "SELECT DISTINCT", if any */
    UPPERREL_ORDERED,           /* result of ORDER BY, if any */
    UPPERREL_ROWMARKS,          /* result of ROMARKS, if any */
    UPPERREL_LIMIT,             /* result of limit offset, if any */
    UPPERREL_FINAL              /* result of any remaining top-level actions */
                                /* NB: UPPERREL_FINAL must be last enum entry; it's used to size arrays */
} UpperRelationKind;

/*
 * For global path optimization, we should keep all paths with interesting distribute
 * keys. There are two kinds of such keys: super set (taking effect for intermediate
 * relation and before agg) and exact match (taking effect for intermediate resultset
 * with all referenced tables (no group by or subset of group by), or final result set
 * after agg). Super set key is used for aggregation redistribution optimization, and
 * matching key is used for insert/ delete/ update redistribution optimization.
 * Also, we should keep corresponding positions for each interesting key, in order to
 * redistribute in positions in sub level, to avoid redistribute in current level
 */
typedef struct ItstDisKey {
    List* superset_keys; /* list of superset keys list, several members possible */
    List* matching_keys; /* list of exact matching keys,  */
} ItstDisKey;

typedef struct {
    int bloomfilter_index; /* Current bloomfilter Num */
    bool add_index;        /* If bloomfilter_index add 1. To eqClass equal member, it's filter index is alike. */
} bloomfilter_context;

typedef struct PlannerContext {
    MemoryContext plannerMemContext;
    MemoryContext dataSkewMemContext;
    MemoryContext tempMemCxt;
    int refCounter; /* tempMemCxt invoked Times */
} PlannerContext;

/*
 * For query mem-based optimization, we should record current memory usage,
 * memory usage with no disk, with maximum disk possible (no severe influence
 * to operator. We allow hashjoin and hashagg to use at most 32 disk files, and
 * sort 256 files. For materialize, we don't want it to spill to disk unless it exceeds
 * memory allowed), and the performance degression ratio between them.
 * (Assume it's linear)
 */
typedef struct OpMemInfo {
    double opMem;       /* u_sess->opt_cxt.op_work_mem in path phase */
    double minMem;      /* ideal memory usage with maximum disk */
    double maxMem;      /* ideal memory usage without disk */
    double regressCost; /* performance degression ratio between min and max Mem */
} OpMemInfo;

#define HASH_MAX_DISK_SIZE 32
#define SORT_MAX_DISK_SIZE 256
#define DFS_MIN_MEM_SIZE 128 * 1024           /* 128MB, the unit is kb */
#define PARTITION_MAX_SIZE (2 * 1024 * 1024L) /* 2GB, the unit is kb */
#define MAX_BATCH_ROWS 60000                  /* default max number of max batch rows, the value can be changed. */
#define PARTIAL_CLUSTER_ROWS 4200000          /* default vaule for max partialClusterRows, the value can be changed. */

/* the min memory for sort is 16MB,  the unit is kb. Don't sort in memory when we don't have engough memory. */
#define SORT_MIM_MEM 16 * 1024
#define MEM_KB 1024L              /* 1024kb for caculating mem info */

/* PSORT_SPREAD_MAXMEM_RATIO can increase 20% for partition table's one part on extended limit. */
#define PSORT_SPREAD_MAXMEM_RATIO 1.2
/* ----------
 * PlannerGlobal
 *		Global information for planning/optimization
 *
 * PlannerGlobal holds state for an entire planner invocation; this state
 * is shared across all levels of sub-Queries that exist in the command being
 * planned.
 * ----------
 */
typedef struct PlannerGlobal {
    NodeTag type;

    ParamListInfo boundParams; /* Param values provided to planner() */

    List* paramlist; /* unused, will be removed in 9.3 */

    List* subplans; /* Plans for SubPlan nodes */

    List* subroots; /* PlannerInfos for SubPlan nodes */

    Bitmapset* rewindPlanIDs; /* indices of subplans that require REWIND */

    List* finalrtable; /* "flat" rangetable for executor */

    List* finalrowmarks; /* "flat" list of PlanRowMarks */

    List* resultRelations; /* "flat" list of integer RT indexes */

    /*
     * Notice: be careful to use relationOids as it may contain non-table OID
     * in some scenarios, e.g. assignment of relationOids in fix_expr_common.
     */
    List* relationOids; /* contain OIDs of relations the plan depends on */

    List* invalItems; /* other dependencies, as PlanInvalItems */

    Index lastPHId; /* highest PlaceHolderVar ID assigned */

    Index lastRowMarkId; /* highest PlanRowMark ID assigned */

    bool transientPlan; /* redo plan when TransactionXmin changes? */

    bool dependsOnRole; /* is plan specific to current role? */

    /* Added post-release, will be in a saner place in 9.3: */
    int nParamExec;       /* number of PARAM_EXEC Params used */
    bool insideRecursion; /* For sql on hdfs, internal flag. */

    bloomfilter_context bloomfilter; /* Bloom filter context. */
    bool vectorized;                 /* whether vector plan be generated, used in join planning phase */
    int minopmem;                    /* min work mem if query mem is used for planning */

    int estiopmem; /* estimation of operator mem, used to revise u_sess->opt_cxt.op_work_mem */

    Cost IOTotalCost; /* total cost */

    List* hint_warning; /* hint warning list */

    PlannerContext* plannerContext;

    /* There is a counter attempt to get name for sublinks */
    int sublink_counter;
#ifdef USE_SPQ
    ApplyShareInputContext share;       /* workspace for GPDB plan sharing */
#endif
} PlannerGlobal;

/* macro for fetching the Plan associated with a SubPlan node */
#define planner_subplan_get_plan(root, subplan) ((Plan*)list_nth((root)->glob->subplans, (subplan)->plan_id - 1))

/* we have to distinguish the different type of subquery */
#define SUBQUERY_NORMAL  0x1
#define SUBQUERY_PARAM   0x2
#define SUBQUERY_RESULT  0x3
#define SUBQUERY_TYPE_BITMAP 0x3
#define SUBQUERY_SUBLINK 0x4

#define SUBQUERY_IS_NORMAL(pr) (((pr->subquery_type & SUBQUERY_TYPE_BITMAP) == SUBQUERY_NORMAL))
#define SUBQUERY_IS_PARAM(pr) (((pr->subquery_type & SUBQUERY_TYPE_BITMAP) == SUBQUERY_PARAM))
#define SUBQUERY_IS_RESULT(pr) (((pr->subquery_type & SUBQUERY_TYPE_BITMAP) == SUBQUERY_RESULT))

#define SUBQUERY_IS_SUBLINK(pr) (((pr->subquery_type & SUBQUERY_SUBLINK) == SUBQUERY_SUBLINK))

#define SUBQUERY_PREDPUSH(pr) ((SUBQUERY_IS_RESULT(pr)) || (SUBQUERY_IS_PARAM(pr)))

#define WITHIN_SUBQUERY(root, rte) (IS_STREAM_PLAN && root->is_correlated && \
            (GetLocatorType(rte->relid) != LOCATOR_TYPE_REPLICATED || ng_is_multiple_nodegroup_scenario()))

struct PlannerTargets;

/* ----------
 * PlannerInfo
 *		Per-query information for planning/optimization
 *
 * This struct is conventionally called "root" in all the planner routines.
 * It holds links to all of the planner's working state, in addition to the
 * original Query.	Note that at present the planner extensively modifies
 * the passed-in Query data structure; someday that should stop.
 * ----------
 */
typedef struct PlannerInfo {
    NodeTag type;

    Query* parse; /* the Query being planned */

    PlannerGlobal* glob; /* global info for current planner run */

    Index query_level; /* 1 at the outermost Query */

    struct PlannerInfo* parent_root; /* NULL at outermost Query */

    /*
     * simple_rel_array holds pointers to "base rels" and "other rels" (see
     * comments for RelOptInfo for more info).	It is indexed by rangetable
     * index (so entry 0 is always wasted).  Entries can be NULL when an RTE
     * does not correspond to a base relation, such as a join RTE or an
     * unreferenced view RTE; or if the RelOptInfo hasn't been made yet.
     */
    struct RelOptInfo** simple_rel_array; /* All 1-rel RelOptInfos */
    int simple_rel_array_size;            /* allocated size of array */

    /*
     * List of changed var that mutated during cost-based rewrite optimization, the
     * element in the list is "struct RewriteVarMapping", for example:
     * - inlist2join
     * - pushjoin2union (will implemented)
     * _ ...
     *
     */
    List* var_mappings;
    Relids var_mapping_rels; /* all the relations that related to inlist2join */

    /*
     * simple_rte_array is the same length as simple_rel_array and holds
     * pointers to the associated rangetable entries.  This lets us avoid
     * rt_fetch(), which can be a bit slow once large inheritance sets have
     * been expanded.
     */
    RangeTblEntry** simple_rte_array; /* rangetable as an array */

    /*
     * append_rel_array is the same length as the above arrays, and holds
     * pointers to the corresponding AppendRelInfo entry indexed by
     * child_relid, or NULL if the rel is not an appendrel child.  The array
     * itself is not allocated if append_rel_list is empty.
     */
    struct AppendRelInfo **append_rel_array;

    /*
     * all_baserels is a Relids set of all base relids (but not "other"
     * relids) in the query; that is, the Relids identifier of the final join
     * we need to form.
     */
    Relids all_baserels;

    /*
     * join_rel_list is a list of all join-relation RelOptInfos we have
     * considered in this planning run.  For small problems we just scan the
     * list to do lookups, but when there are many join relations we build a
     * hash table for faster lookups.  The hash table is present and valid
     * when join_rel_hash is not NULL.	Note that we still maintain the list
     * even when using the hash table for lookups; this simplifies life for
     * GEQO.
     */
    List* join_rel_list;        /* list of join-relation RelOptInfos */
    struct HTAB* join_rel_hash; /* optional hashtable for join relations */

    /*
     * When doing a dynamic-programming-style join search, join_rel_level[k]
     * is a list of all join-relation RelOptInfos of level k, and
     * join_cur_level is the current level.  New join-relation RelOptInfos are
     * automatically added to the join_rel_level[join_cur_level] list.
     * join_rel_level is NULL if not in use.
     */
    List** join_rel_level; /* lists of join-relation RelOptInfos */
    int join_cur_level;    /* index of list being extended */

    List* init_plans; /* init SubPlans for query */

    List* cte_plan_ids; /* per-CTE-item list of subplan IDs */

    List* eq_classes; /* list of active EquivalenceClasses */

    List* canon_pathkeys; /* list of "canonical" PathKeys */

    List* left_join_clauses; /* list of RestrictInfos for
                              * mergejoinable outer join clauses
                              * w/nonnullable var on left */

    List* right_join_clauses; /* list of RestrictInfos for
                               * mergejoinable outer join clauses
                               * w/nonnullable var on right */

    List* full_join_clauses; /* list of RestrictInfos for
                              * mergejoinable full join clauses */

    List* join_info_list; /* list of SpecialJoinInfos */

    List* lateral_info_list;  /* list of LateralJoinInfos */

    List* append_rel_list; /* list of AppendRelInfos */

    List* rowMarks; /* list of PlanRowMarks */

    List* placeholder_list; /* list of PlaceHolderInfos */

    List* query_pathkeys; /* desired pathkeys for query_planner(), and
                           * actual pathkeys afterwards */

    List* group_pathkeys;    /* groupClause pathkeys, if any */
    List* window_pathkeys;   /* pathkeys of bottom window, if any */
    List* distinct_pathkeys; /* distinctClause pathkeys, if any */
    List* sort_pathkeys;     /* sortClause pathkeys, if any */

    /* Use fetch_upper_rel() to get any particular upper rel */
    List *upper_rels[UPPERREL_FINAL + 1]; /* upper-rel RelOptInfos */

    /* Result tlists chosen by grouping_planner for upper-stage processing */
    struct PathTarget *upper_targets[UPPERREL_FINAL + 1];
    
    List* minmax_aggs; /* List of MinMaxAggInfos */

    List* initial_rels; /* RelOptInfos we are now trying to join */

    MemoryContext planner_cxt; /* context holding PlannerInfo */

    double total_table_pages; /* # of pages in all tables of query */

    double tuple_fraction; /* tuple_fraction passed to query_planner */
    double limit_tuples;   /* limit_tuples passed to query_planner */

    bool hasInheritedTarget;     /* true if parse->resultRelation is an
                                  * inheritance child rel */
    bool hasJoinRTEs;            /* true if any RTEs are RTE_JOIN kind */
    bool hasLateralRTEs;         /* true if any RTEs are marked LATERAL */
    bool hasHavingQual;          /* true if havingQual was non-null */
    bool hasPseudoConstantQuals; /* true if any RestrictInfo has
                                  * pseudoconstant = true */
    bool hasRecursion;           /* true if planning a recursive WITH item */
    bool consider_sortgroup_agg;  /*ture if consider to use SORT GROUP agg */

    /* Note: qualSecurityLevel is zero if there are no securityQuals */
    Index qualSecurityLevel; /* minimum security_level for quals */

#ifdef PGXC
    /* This field is used only when RemoteScan nodes are involved */
    int rs_alias_index; /* used to build the alias reference */

    /*
     * In openGauss Coordinators are supposed to skip the handling of
     * row marks of type ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE.
     * In order to do that we simply remove such type
     * of row marks from the list rowMarks. Instead they are saved
     * in xc_rowMarks list that is then handeled to add
     * FOR UPDATE/SHARE in the remote query
     */
    List* xc_rowMarks; /* list of PlanRowMarks of type ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE */
#endif

    /* These fields are used only when hasRecursion is true: */
    int wt_param_id;                 /* PARAM_EXEC ID for the work table */
    struct Plan* non_recursive_plan; /* plan for non-recursive term */

    /* These fields are workspace for createplan.c */
    Relids curOuterRels;  /* outer rels above current node */
    List* curOuterParams; /* not-yet-assigned NestLoopParams */

    Index curIteratorParamIndex;
    bool isPartIteratorPruning;
    Index curSubPartIteratorParamIndex;
    bool isPartIteratorPlanning;
    int curItrs;
    List* subqueryRestrictInfo; /* Subquery RestrictInfo, which only be used in wondows agg. */

    /* optional private data for join_search_hook, e.g., GEQO */
    void* join_search_private;

    /* Added post-release, will be in a saner place in 9.3: */
    List* plan_params; /* list of PlannerParamItems, see below */

    /* For count_distinct, save null info for group by clause */
    List* join_null_info;
    /*
     * grouping_planner passes back its final processed targetlist here, for
     * use in relabeling the topmost tlist of the finished Plan.
     */
    List *processed_tlist;
    /* for GroupingFunc fixup in setrefs */
    AttrNumber* grouping_map;

    /* If current query level is correlated with upper level */
    bool is_correlated;

    /* data redistribution for DFS table.
     * dataDestRelIndex is index into the range table. This variable
     * will take effect on data redistribution state.
     * The effective value must be greater than 0.
     */
    Index dataDestRelIndex;

    /* interesting keys of current query level */
    ItstDisKey dis_keys;

    /*
     * indicate if the subquery planning root (PlannerInfo) is under or rooted from
     * recursive-cte planning.
     */
    bool is_under_recursive_cte;

    /*
     * indicate if the subquery planning root (PlannerInfo) is under recursive-cte's
     * recursive branch
     */
    bool is_under_recursive_tree;
    bool has_recursive_correlated_rte; /* true if any RTE correlated with recursive cte */

    int subquery_type;
    Bitmapset *param_upper;
	
	bool hasRownumQual;
    bool hasRownumCheck;
    List *origin_tlist;
    struct PlannerTargets *planner_targets;
} PlannerInfo;

/*
 * In places where it's known that simple_rte_array[] must have been prepared
 * already, we just index into it to fetch RTEs.  In code that might be
 * executed before or after entering query_planner(), use this macro.
 */
#define planner_rt_fetch(rti, root) \
    ((root)->simple_rte_array ? (root)->simple_rte_array[rti] : rt_fetch(rti, (root)->parse->rtable))

/* ----------
 * RelOptInfo
 *		Per-relation information for planning/optimization
 *
 * For planning purposes, a "base rel" is either a plain relation (a table)
 * or the output of a sub-SELECT or function that appears in the range table.
 * In either case it is uniquely identified by an RT index.  A "joinrel"
 * is the joining of two or more base rels.  A joinrel is identified by
 * the set of RT indexes for its component baserels.  We create RelOptInfo
 * nodes for each baserel and joinrel, and store them in the PlannerInfo's
 * simple_rel_array and join_rel_list respectively.
 *
 * Note that there is only one joinrel for any given set of component
 * baserels, no matter what order we assemble them in; so an unordered
 * set is the right datatype to identify it with.
 *
 * We also have "other rels", which are like base rels in that they refer to
 * single RT indexes; but they are not part of the join tree, and are given
 * a different RelOptKind to identify them.  
 * There is also a RelOptKind for "upper" relations, which are RelOptInfos
 * that describe post-scan/join processing steps, such as aggregation.
 * Many of the fields in these RelOptInfos are meaningless, but their Path
 * fields always hold Paths showing ways to do that processing step, currently
 * this kind is only used for fdw to search path.
 * Lastly, there is a RelOptKind for "dead" relations, which are base rels
 * that we have proven we don't need to join after all.
 *
 * Currently the only kind of otherrels are those made for member relations
 * of an "append relation", that is an inheritance set or UNION ALL subquery.
 * An append relation has a parent RTE that is a base rel, which represents
 * the entire append relation.	The member RTEs are otherrels.	The parent
 * is present in the query join tree but the members are not.  The member
 * RTEs and otherrels are used to plan the scans of the individual tables or
 * subqueries of the append set; then the parent baserel is given Append
 * and/or MergeAppend paths comprising the best paths for the individual
 * member rels.  (See comments for AppendRelInfo for more information.)
 *
 * At one time we also made otherrels to represent join RTEs, for use in
 * handling join alias Vars.  Currently this is not needed because all join
 * alias Vars are expanded to non-aliased form during preprocess_expression.
 *
 * Parts of this data structure are specific to various scan and join
 * mechanisms.	It didn't seem worth creating new node types for them.
 *
 *		relids - Set of base-relation identifiers; it is a base relation
 *				if there is just one, a join relation if more than one
 *		rows - estimated number of tuples in the relation after restriction
 *			   clauses have been applied (ie, output rows of a plan for it)
 *		reltarget - Default Path output tlist for this rel; normally contains
 *					Var and PlaceHolderVar nodes for the values we need to
 *					output from this relation.
 *					List is in no particular order, but all rels of an
 *					appendrel set must use corresponding orders.
 *					NOTE: in an appendrel child relation, may contain
 *					arbitrary expressions pulled up from a subquery!
 *		pathlist - List of Path nodes, one for each potentially useful
 *				   method of generating the relation
 *		ppilist - ParamPathInfo nodes for parameterized Paths, if any
 *		cheapest_startup_path - the pathlist member with lowest startup cost
 *								(regardless of its ordering; but must be
 *								 unparameterized)
 *		cheapest_total_path - the pathlist member with lowest total cost
 *							  (regardless of its ordering; but must be
 *							   unparameterized)
 *		cheapest_unique_path - for caching cheapest path to produce unique
 *							   (no duplicates) output from relation
 *		cheapest_parameterized_paths - paths with cheapest total costs for
 *								 their parameterizations; always includes
 *								 cheapest_total_path
 *
 * If the relation is a base relation it will have these fields set:
 *
 *		relid - RTE index (this is redundant with the relids field, but
 *				is provided for convenience of access)
 *		rtekind - distinguishes plain relation, subquery, or function RTE
 *		min_attr, max_attr - range of valid AttrNumbers for rel
 *		attr_needed - array of bitmapsets indicating the highest joinrel
 *				in which each attribute is needed; if bit 0 is set then
 *				the attribute is needed as part of final targetlist
 *		attr_widths - cache space for per-attribute width estimates;
 *					  zero means not computed yet
 *		indexlist - list of IndexOptInfo nodes for relation's indexes
 *					(always NIL if it's not a table)
 *		pages - number of disk pages in relation (zero if not a table)
 *		tuples - number of tuples in relation (not considering restrictions)
 *		allvisfrac - fraction of disk pages that are marked all-visible
 *		subplan - plan for subquery (NULL if it's not a subquery)
 *		subroot - PlannerInfo for subquery (NULL if it's not a subquery)
 *
 *		Note: for a subquery, tuples, subplan, subroot are not set immediately
 *		upon creation of the RelOptInfo object; they are filled in when
 *		set_subquery_pathlist processes the object.  Likewise, fdwroutine
 *		and fdw_private are filled during initial path creation.
 *
 *		For otherrels that are appendrel members, these fields are filled
 *		in just as for a baserel.
 * If the relation is either a foreign table or a join of foreign tables that
 * all belong to the same foreign server and are assigned to the same user to
 * check access permissions as (cf checkAsUser), these fields will be set:
 *
 *		serverid - OID of foreign server, if foreign table (else InvalidOid)
 *		userid - OID of user to check access as (InvalidOid means current user)
 *		useridiscurrent - we've assumed that userid equals current user
 *		fdwroutine - function hooks for FDW, if foreign table (else NULL)
 *		fdw_private - private state for FDW, if foreign table (else NULL)
 *
 * The presence of the remaining fields depends on the restrictions
 * and joins that the relation participates in:
 *
 *		baserestrictinfo - List of RestrictInfo nodes, containing info about
 *					each non-join qualification clause in which this relation
 *					participates (only used for base rels)
 *		baserestrictcost - Estimated cost of evaluating the baserestrictinfo
 *					clauses at a single tuple (only used for base rels)
 *		baserestrict_min_security - Smallest security_level found among
 *					clauses in baserestrictinfo
 *		joininfo  - List of RestrictInfo nodes, containing info about each
 *					join clause in which this relation participates (but
 *					note this excludes clauses that might be derivable from
 *					EquivalenceClasses)
 *		has_eclass_joins - flag that EquivalenceClass joins are possible
 *
 * Note: Keeping a restrictinfo list in the RelOptInfo is useful only for
 * base rels, because for a join rel the set of clauses that are treated as
 * restrict clauses varies depending on which sub-relations we choose to join.
 * (For example, in a 3-base-rel join, a clause relating rels 1 and 2 must be
 * treated as a restrictclause if we join {1} and {2 3} to make {1 2 3}; but
 * if we join {1 2} and {3} then that clause will be a restrictclause in {1 2}
 * and should not be processed again at the level of {1 2 3}.)	Therefore,
 * the restrictinfo list in the join case appears in individual JoinPaths
 * (field joinrestrictinfo), not in the parent relation.  But it's OK for
 * the RelOptInfo to store the joininfo list, because that is the same
 * for a given rel no matter how we form it.
 *
 * We store baserestrictcost in the RelOptInfo (for base relations) because
 * we know we will need it at least once (to price the sequential scan)
 * and may need it multiple times to price index scans.
 * ----------
 */
typedef enum RelOptKind { RELOPT_BASEREL, RELOPT_JOINREL, RELOPT_OTHER_MEMBER_REL, RELOPT_UPPER_REL, RELOPT_DEADREL } RelOptKind;

typedef enum PartitionFlag { PARTITION_NONE, PARTITION_REQURIED, PARTITION_ANCESOR } PartitionFlag;

/*
 * Is the given relation a simple relation i.e a base or "other" member
 * relation?
 */
#define IS_SIMPLE_REL(rel) ((rel)->reloptkind == RELOPT_BASEREL || (rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)

/* Is the given relation a join relation? */
#define IS_JOIN_REL(rel) ((rel)->reloptkind == RELOPT_JOINREL)

/* Is the given relation an upper relation? */
#define IS_UPPER_REL(rel) ((rel)->reloptkind == RELOPT_UPPER_REL)

/*
 * PathTarget
 *
 * This struct contains what we need to know during planning about the
 * targetlist (output columns) that a Path will compute.  Each RelOptInfo
 * includes a default PathTarget, which its individual Paths may simply
 * reference.  However, in some cases a Path may compute outputs different
 * from other Paths, and in that case we make a custom PathTarget for it.
 * For example, an indexscan might return index expressions that would
 * otherwise need to be explicitly calculated.  (Note also that "upper"
 * relations generally don't have useful default PathTargets.)
 *
 * exprs contains bare expressions; they do not have TargetEntry nodes on top,
 * though those will appear in finished Plans.
 *
 * sortgrouprefs[] is an array of the same length as exprs, containing the
 * corresponding sort/group refnos, or zeroes for expressions not referenced
 * by sort/group clauses.  If sortgrouprefs is NULL (which it generally is in
 * RelOptInfo.reltarget targets; only upper-level Paths contain this info),
 * we have not identified sort/group columns in this tlist.  This allows us to
 * deal with sort/group refnos when needed with less expense than including
 * TargetEntry nodes in the exprs list.
 */
typedef struct PathTarget {
    NodeTag type;
    List *exprs;          /* list of expressions to be computed */
    Index *sortgrouprefs; /* corresponding sort/group refnos, or 0 */
    QualCost cost;        /* cost of evaluating the expressions */
    int width;            /* estimated avg width of result tuples */
} PathTarget;


typedef struct PlannerTargets {
    /*final*/
    bool final_contains_srfs;
    PathTarget *final_target;
    List *final_targets;
    List *final_targets_contain_srfs;

    /*sort input*/
    bool sort_input_contains_srfs;
    PathTarget *sort_input_target;
    List *sort_input_targets;
    List *sort_input_targets_contain_srfs;
    bool have_postponed_srfs = false;

    /*grouping*/
    bool grouping_contains_srfs;
    PathTarget *grouping_target;
    List *grouping_targets;
    List *grouping_targets_contain_srfs;

    /*scan join*/
    bool scanjoin_contains_srfs;
    PathTarget *scanjoin_target;
    List *scanjoin_targets;
    List *scanjoin_targets_contain_srfs;
    
} PlannerTargets;

/* Convenience macro to get a sort/group refno from a PathTarget */
#define get_pathtarget_sortgroupref(target, colno) ((target)->sortgrouprefs ? (target)->sortgrouprefs[colno] : (Index)0)

typedef struct RelOptInfo {
    NodeTag type;

    RelOptKind reloptkind;

    /* all relations included in this RelOptInfo */
    Relids relids; /* set of base relids (rangetable indexes) */

    bool isPartitionedTable; /* is it a partitioned table? it is meaningless unless
                         it is a 'baserel' (reloptkind = RELOPT_BASEREL) */
    PartitionFlag partflag;

    /* size estimates generated by planner */
    double rows;           /* estimated number of global result tuples */
    int encodedwidth;      /* estimated avg width of encoded columns in result tuples */
    AttrNumber encodednum; /* number of encoded column */

    /* default result targetlist for Paths scanning this relation */
    struct PathTarget *reltarget; /* list of Vars/Exprs, cost, width */

    /* materialization information */
    List* distribute_keys; /* distribute key */
    List* pathlist;        /* Path structures */
    List* ppilist;         /* ParamPathInfos used in pathlist */
    struct Path* cheapest_gather_path;
    struct Path* cheapest_startup_path;
    List* cheapest_total_path; /* contain all cheapest total paths from different distribute key */
    struct Path* cheapest_unique_path;
    List* cheapest_parameterized_paths;

    /* information about a base rel (not set for join rels!) */
    Index relid;
    Oid reltablespace;   /* containing tablespace */
    RTEKind rtekind;     /* RELATION, SUBQUERY, or FUNCTION */
    AttrNumber min_attr; /* smallest attrno of rel (often <0) */
    AttrNumber max_attr; /* largest attrno of rel */
    Relids* attr_needed; /* array indexed [min_attr .. max_attr] */
    int32* attr_widths;  /* array indexed [min_attr .. max_attr] */
    List* lateral_vars;  /* LATERAL Vars and PHVs referenced by rel */
    Relids lateral_relids; /* minimum parameterization of rel */
    List* indexlist;     /* list of IndexOptInfo */

#ifndef ENABLE_MULTIPLE_NODES
    List* statlist;      /* list of ExtendedStats */
#endif

    RelPageType pages;   /* local size estimates derived from pg_class */
    double tuples;       /* global size estimates derived from pg_class */
    double multiple;     /* how many dn skewed and biased be influenced by distinct. */
    double allvisfrac;

    struct PruningResult* pruning_result; /* pruning result for partitioned table with
                                    baserestrictinfo,it is meaningless unless it
                                    is a 'baserel' (reloptkind = RELOPT_BASEREL) */
    int partItrs;                         /* the number of the partitions in pruning_result */
    struct PruningResult* pruning_result_for_index_usable;
    int partItrs_for_index_usable; /* the number of the partitions in pruning_result_for_seqscan */
    struct PruningResult* pruning_result_for_index_unusable;
    int partItrs_for_index_unusable; /* the number of the partitions in pruning_result_for_seqscan */
    /* information about a partitioned table */
    BucketInfo *bucketInfo;

    /* use "struct Plan" to avoid including plannodes.h here */
    struct Plan* subplan; /* if subquery */
    PlannerInfo* subroot; /* if subquery */
    List *subplan_params; /* if subquery */

    /* Information about foreign tables and foreign joins */
    Oid serverid;         /* identifies server for the table or join */
    Oid userid;           /* identifies user to check access as */
    bool useridiscurrent; /* join is only valid for current user */
    /* use "struct FdwRoutine" to avoid including fdwapi.h here */
    struct FdwRoutine* fdwroutine; /* if foreign table */
    void* fdw_private;             /* if foreign table */

    /* cache space for remembering if we have proven this relation unique */
    List *unique_for_rels;	/* known unique for these other relid set(s) */
    List *non_unique_for_rels;	/* known not unique for these set(s) */

    /* used by various scans and joins: */
    List* baserestrictinfo;          /* RestrictInfo structures (if base
                                      * rel) */
    QualCost baserestrictcost;       /* cost of evaluating the above */
    Index baserestrict_min_security; /* min security_level found in
                                      * baserestrictinfo */
    List* joininfo;                  /* RestrictInfo structures for join clauses
                                      * involving this rel */
    bool has_eclass_joins;           /* T means joininfo is incomplete */
    Relids top_parent_relids;        /* Relids of topmost parents (if "other"* rel) */
    RelOrientation orientation;      /* the store type of base rel */
    RelstoreType relStoreLocation;   /* the relation store location. */
    char locator_type;               /* the location type of base rel */
    Oid rangelistOid;                /* oid of list/range distributed table, InvalidOid if not list/range table */
    List* subplanrestrictinfo;       /* table filter with correlated column involved */
    ItstDisKey rel_dis_keys;         /* interesting key info for current relation */
    List* varratio;                  /* rel tuples ratio after join to different relation */
    List* varEqRatio;

    bool is_ustore;

    /*
     * The alternative rel for cost-based query rewrite
     *
     * Note: Only base rel have valid pointer of this fields, set to NULL for alternative rel
     */
    List* alternatives;

    /*
     * Rel opinter to base rel that in plannerinfo->simple_rel_array[x].
     *
     * Note: Only alternative rels has valid pointer of this field, set to NULL for the
     * origin rel.
     */
    RelOptInfo* base_rel;

    unsigned int num_data_nodes = 0; //number of distributing data nodes

    List* partial_pathlist;   /* partial Paths */
    int cursorDop;
} RelOptInfo;

/*
 * IndexOptInfo
 *		Per-index information for planning/optimization
 *
 *		indexkeys[], indexcollations[], opfamily[], and opcintype[]
 *		each have ncolumns entries.
 *
 *		sortopfamily[], reverse_sort[], and nulls_first[] likewise have
 *		ncolumns entries, if the index is ordered; but if it is unordered,
 *		those pointers are NULL.
 *
 *		Zeroes in the indexkeys[] array indicate index columns that are
 *		expressions; there is one element in indexprs for each such column.
 *
 *		For an ordered index, reverse_sort[] and nulls_first[] describe the
 *		sort ordering of a forward indexscan; we can also consider a backward
 *		indexscan, which will generate the reverse ordering.
 *
 *		The indexprs and indpred expressions have been run through
 *		prepqual.c and eval_const_expressions() for ease of matching to
 *		WHERE clauses. indpred is in implicit-AND form.
 *
 *		indextlist is a TargetEntry list representing the index columns.
 *		It provides an equivalent base-relation Var for each simple column,
 *		and links to the matching indexprs element for each expression column.
 */
typedef struct IndexOptInfo {
    NodeTag type;

    Oid indexoid;            /* OID of the index relation */
    bool ispartitionedindex; /* it is an partitioned index */
    Oid partitionindex;      /* the partition index oid for current partition */
    Oid reltablespace;       /* tablespace of index (not table) */
    RelOptInfo* rel;         /* back-link to index's table */

    /* statistics from pg_class */
    RelPageType pages; /* number of disk pages in index */
    double tuples;     /* number of global index tuples in index */

    /* index descriptor information */
    int ncolumns;         /* number of columns in index */
    int nkeycolumns;      /* number of key columns in index */
    int* indexkeys;       /* column numbers of index's keys, or 0 */
    Oid* indexcollations; /* OIDs of collations of index columns */
    Oid* opfamily;        /* OIDs of operator families for columns */
    Oid* opcintype;       /* OIDs of opclass declared input data types */
    Oid* sortopfamily;    /* OIDs of btree opfamilies, if orderable */
    bool* reverse_sort;   /* is sort order descending? */
    bool* nulls_first;    /* do NULLs come first in the sort order? */
    Oid relam;            /* OID of the access method (in pg_am) */

    RegProcedure amcostestimate; /* OID of the access method's cost fcn */

    List* indexprs; /* expressions for non-simple index columns */
    List* indpred;  /* predicate if a partial index, else NIL */

    List* indextlist; /* targetlist representing index columns */

    bool isGlobal;       /* true if index is global partition index */
    bool isAnnIndex;     /* true if index is vector index */
    bool crossbucket;    /* true if index is crossbucket */
    bool predOK;         /* true if predicate matches query */
    bool unique;         /* true if a unique index */
    bool immediate;      /* is uniqueness enforced immediately? */
    bool hypothetical;   /* true if index doesn't really exist */
    bool canreturn;      /* can index return IndexTuples? */
    bool amcanorderbyop; /* does AM support order by operator result? */
    bool amoptionalkey;  /* can query omit key for the first column? */
    bool amsearcharray;  /* can AM handle ScalarArrayOpExpr quals? */
    bool amsearchnulls;  /* can AM search for NULL/NOT NULL entries? */
    bool amhasgettuple;  /* does AM have amgettuple interface? */
    bool amhasgetbitmap; /* does AM have amgetbitmap interface? */
    List* indrestrictinfo;/* parent relation's baserestrictinfo list */
} IndexOptInfo;

/*
 * EquivalenceClasses
 *
 * Whenever we can determine that a mergejoinable equality clause A = B is
 * not delayed by any outer join, we create an EquivalenceClass containing
 * the expressions A and B to record this knowledge.  If we later find another
 * equivalence B = C, we add C to the existing EquivalenceClass; this may
 * require merging two existing EquivalenceClasses.  At the end of the qual
 * distribution process, we have sets of values that are known all transitively
 * equal to each other, where "equal" is according to the rules of the btree
 * operator family(s) shown in ec_opfamilies, as well as the collation shown
 * by ec_collation.  (We restrict an EC to contain only equalities whose
 * operators belong to the same set of opfamilies.	This could probably be
 * relaxed, but for now it's not worth the trouble, since nearly all equality
 * operators belong to only one btree opclass anyway.  Similarly, we suppose
 * that all or none of the input datatypes are collatable, so that a single
 * collation value is sufficient.)
 *
 * We also use EquivalenceClasses as the base structure for PathKeys, letting
 * us represent knowledge about different sort orderings being equivalent.
 * Since every PathKey must reference an EquivalenceClass, we will end up
 * with single-member EquivalenceClasses whenever a sort key expression has
 * not been equivalenced to anything else.	It is also possible that such an
 * EquivalenceClass will contain a volatile expression ("ORDER BY random()"),
 * which is a case that can't arise otherwise since clauses containing
 * volatile functions are never considered mergejoinable.  We mark such
 * EquivalenceClasses specially to prevent them from being merged with
 * ordinary EquivalenceClasses.  Also, for volatile expressions we have
 * to be careful to match the EquivalenceClass to the correct targetlist
 * entry: consider SELECT random() AS a, random() AS b ... ORDER BY b,a.
 * So we record the SortGroupRef of the originating sort clause.
 *
 * We allow equality clauses appearing below the nullable side of an outer join
 * to form EquivalenceClasses, but these have a slightly different meaning:
 * the included values might be all NULL rather than all the same non-null
 * values.	See src/backend/optimizer/README for more on that point.
 *
 * NB: if ec_merged isn't NULL, this class has been merged into another, and
 * should be ignored in favor of using the pointed-to class.
 */
typedef struct EquivalenceClass {
    NodeTag type;

    List* ec_opfamilies;                /* btree operator family OIDs */
    Oid ec_collation;                   /* collation, if datatypes are collatable */
    List* ec_members;                   /* list of EquivalenceMembers */
    List* ec_sources;                   /* list of generating RestrictInfos */
    List* ec_derives;                   /* list of derived RestrictInfos */
    Relids ec_relids;                   /* all relids appearing in ec_members */
    bool ec_has_const;                  /* any pseudoconstants in ec_members? */
    bool ec_has_volatile;               /* the (sole) member is a volatile expr */
    bool ec_below_outer_join;           /* equivalence applies below an OJ */
    bool ec_group_set;                  /* if take part in group */
    bool ec_broken;                     /* failed to generate needed clauses? */
    Index ec_sortref;                   /* originating sortclause label, or 0 */
    Index ec_min_security;              /* minimum security_level in ec_sources */
    Index ec_max_security;              /* maximum security_level in ec_sources */
    struct EquivalenceClass* ec_merged; /* set if merged into another EC */
} EquivalenceClass;

/*
 * If an EC contains a const and isn't below-outer-join, any PathKey depending
 * on it must be redundant, since there's only one possible value of the key.
 */
#define EC_MUST_BE_REDUNDANT(eclass) ((eclass)->ec_has_const && !(eclass)->ec_below_outer_join)

#define IS_EC_FUNC(rte)                                                                           \
    (rte->rtekind == RTE_FUNCTION && (((FuncExpr*)rte->funcexpr)->funcid == ECEXTENSIONFUNCOID || \
                                         ((FuncExpr*)rte->funcexpr)->funcid == ECHADOOPFUNCOID))

/*
 * EquivalenceMember - one member expression of an EquivalenceClass
 *
 * em_is_child signifies that this element was built by transposing a member
 * for an appendrel parent relation to represent the corresponding expression
 * for an appendrel child.	These members are used for determining the
 * pathkeys of scans on the child relation and for explicitly sorting the
 * child when necessary to build a MergeAppend path for the whole appendrel
 * tree.  An em_is_child member has no impact on the properties of the EC as a
 * whole; in particular the EC's ec_relids field does NOT include the child
 * relation.  An em_is_child member should never be marked em_is_const nor
 * cause ec_has_const or ec_has_volatile to be set, either.  Thus, em_is_child
 * members are not really full-fledged members of the EC, but just reflections
 * or doppelgangers of real members.  Most operations on EquivalenceClasses
 * should ignore em_is_child members, and those that don't should test
 * em_relids to make sure they only consider relevant members.
 *
 * em_datatype is usually the same as exprType(em_expr), but can be
 * different when dealing with a binary-compatible opfamily; in particular
 * anyarray_ops would never work without this.	Use em_datatype when
 * looking up a specific btree operator to work with this expression.
 */
typedef struct EquivalenceMember {
    NodeTag type;

    Expr* em_expr;             /* the expression represented */
    Relids em_relids;          /* all relids appearing in em_expr */
    Relids em_nullable_relids; /* nullable by lower outer joins */
    bool em_is_const;          /* expression is pseudoconstant? */
    bool em_is_child;          /* derived version for a child relation? */
    Oid em_datatype;           /* the "nominal type" used by the opfamily */
} EquivalenceMember;

/*
 * PathKeys
 *
 * The sort ordering of a path is represented by a list of PathKey nodes.
 * An empty list implies no known ordering.  Otherwise the first item
 * represents the primary sort key, the second the first secondary sort key,
 * etc.  The value being sorted is represented by linking to an
 * EquivalenceClass containing that value and including pk_opfamily among its
 * ec_opfamilies.  The EquivalenceClass tells which collation to use, too.
 * This is a convenient method because it makes it trivial to detect
 * equivalent and closely-related orderings. (See optimizer/README for more
 * information.)
 *
 * Note: pk_strategy is either BTLessStrategyNumber (for ASC) or
 * BTGreaterStrategyNumber (for DESC).	We assume that all ordering-capable
 * index types will use btree-compatible strategy numbers.
 */
typedef struct PathKey {
    NodeTag type;

    EquivalenceClass* pk_eclass; /* the value that is ordered */
    Oid pk_opfamily;             /* btree opfamily defining the ordering */
    int pk_strategy;             /* sort direction (ASC or DESC) */
    bool pk_nulls_first;         /* do NULLs come before normal values? */
} PathKey;

/*
 * ParamPathInfo
 *
 * All parameterized paths for a given relation with given required outer rels
 * link to a single ParamPathInfo, which stores common information such as
 * the estimated rowcount for this parameterization.  We do this partly to
 * avoid recalculations, but mostly to ensure that the estimated rowcount
 * is in fact the same for every such path.
 *
 * Note: ppi_clauses is only used in ParamPathInfos for base relation paths;
 * in join cases it's NIL because the set of relevant clauses varies depending
 * on how the join is formed.  The relevant clauses will appear in each
 * parameterized join path's joinrestrictinfo list, instead.
 */
typedef struct ParamPathInfo {
    NodeTag type;

    Relids ppi_req_outer; /* rels supplying parameters used by path */
    double ppi_rows;      /* estimated global number of result tuples */
    List* ppi_clauses;    /* join clauses available from outer rels */
    Bitmapset* ppi_req_upper; /* param IDs*/
} ParamPathInfo;

/*
 * Type "Path" is used as-is for sequential-scan paths, as well as some other
 * simple plan types that we don't need any extra information in the path for.
 * For other path types it is the first component of a larger struct.
 *
 * "pathtype" is the NodeTag of the Plan node we could build from this Path.
 * It is partially redundant with the Path's NodeTag, but allows us to use
 * the same Path type for multiple Plan types when there is no need to
 * distinguish the Plan type during path processing.
 *
 * "parent" identifies the relation this Path scans, and "pathtarget"
 * describes the precise set of output columns the Path would compute.
 * In simple cases all Paths for a given rel share the same targetlist,
 * which we represent by having path->pathtarget point to parent->reltarget.
 *
 * "param_info", if not NULL, links to a ParamPathInfo that identifies outer
 * relation(s) that provide parameter values to each scan of this path.
 * That means this path can only be joined to those rels by means of nestloop
 * joins with this path on the inside.	Also note that a parameterized path
 * is responsible for testing all "movable" joinclauses involving this rel
 * and the specified outer rel(s).
 *
 * "rows" is the same as parent->rows in simple paths, but in parameterized
 * paths and UniquePaths it can be less than parent->rows, reflecting the
 * fact that we've filtered by extra join conditions or removed duplicates.
 *
 * "pathkeys" is a List of PathKey nodes (see above), describing the sort
 * ordering of the path's output rows.
 */
typedef struct Path {
    NodeTag type;

    NodeTag pathtype; /* tag identifying scan/join method */

    RelOptInfo* parent;        /* the relation this path can build */
    PathTarget *pathtarget;    /* list of Vars/Exprs, cost, width */
    ParamPathInfo* param_info; /* parameterization info, or NULL if none */

    /* estimated size/costs for path (see costsize.c for more info) */
    double rows; /* estimated number of global result tuples */
    double multiple;
    Cost startup_cost; /* cost expended before fetching any tuples */
    Cost total_cost;   /* total cost (assuming all tuples fetched) */
    Cost stream_cost;  /* cost of actions invoked by stream but can't be parallelled in this path */

    List* pathkeys;        /* sort ordering of path's output */
    List* distribute_keys; /* distribute key, Var list */
    char locator_type;
    RemoteQueryExecType exec_type;
    Oid rangelistOid;
    int dop; /* degree of parallelism */
    /* pathkeys is a List of PathKey nodes; see above */
    Distribution distribution;
    int hint_value;       /* Mark this path if be hinted, and hint kind. */
    double innerdistinct; /* join inner rel distinct estimation value */
    double outerdistinct; /* join outer rel distinct estimation value */
} Path;

/* Macro for extracting a path's parameterization relids; beware double eval */
#define PATH_REQ_OUTER(path) ((path)->param_info ? (path)->param_info->ppi_req_outer : (Relids)NULL)
#define PATH_REQ_UPPER(path) ((path)->param_info ? (path)->param_info->ppi_req_upper : (Relids)NULL)

/* ----------
 * IndexPath represents an index scan over a single index.
 *
 * This struct is used for both regular indexscans and index-only scans;
 * path.pathtype is T_IndexScan or T_IndexOnlyScan to show which is meant.
 *
 * 'indexinfo' is the index to be scanned.
 *
 * 'indexclauses' is a list of index qualification clauses, with implicit
 * AND semantics across the list.  Each clause is a RestrictInfo node from
 * the query's WHERE or JOIN conditions.  An empty list implies a full
 * index scan.
 *
 * 'indexquals' has the same structure as 'indexclauses', but it contains
 * the actual index qual conditions that can be used with the index.
 * In simple cases this is identical to 'indexclauses', but when special
 * indexable operators appear in 'indexclauses', they are replaced by the
 * derived indexscannable conditions in 'indexquals'.
 *
 * 'indexqualcols' is an integer list of index column numbers (zero-based)
 * of the same length as 'indexquals', showing which index column each qual
 * is meant to be used with.  'indexquals' is required to be ordered by
 * index column, so 'indexqualcols' must form a nondecreasing sequence.
 * (The order of multiple quals for the same index column is unspecified.)
 *
 * 'indexorderbys', if not NIL, is a list of ORDER BY expressions that have
 * been found to be usable as ordering operators for an amcanorderbyop index.
 * The list must match the path's pathkeys, ie, one expression per pathkey
 * in the same order.  These are not RestrictInfos, just bare expressions,
 * since they generally won't yield booleans.  Also, unlike the case for
 * quals, it's guaranteed that each expression has the index key on the left
 * side of the operator.
 *
 * 'indexorderbycols' is an integer list of index column numbers (zero-based)
 * of the same length as 'indexorderbys', showing which index column each
 * ORDER BY expression is meant to be used with.  (There is no restriction
 * on which index column each ORDER BY can be used with.)
 * 
 * 'rulesforindexgen' is a bitmapset. It is used for recording some rules which 
 * are satisfied in current index path. These recorded rules will be used for 
 * filtering paths. We can consider it as the supplement of CBO (cost based optimize).
 *
 * 'indexscandir' is one of:
 *		ForwardScanDirection: forward scan of an ordered index
 *		BackwardScanDirection: backward scan of an ordered index
 *		NoMovementScanDirection: scan of an unordered index, or don't care
 * (The executor doesn't care whether it gets ForwardScanDirection or
 * NoMovementScanDirection for an indexscan, but the planner wants to
 * distinguish ordered from unordered indexes for building pathkeys.)
 *
 * 'indextotalcost' and 'indexselectivity' are saved in the IndexPath so that
 * we need not recompute them when considering using the same index in a
 * bitmap index/heap scan (see BitmapHeapPath).  The costs of the IndexPath
 * itself represent the costs of an IndexScan or IndexOnlyScan plan type.
 * ----------
 */
typedef struct IndexPath {
    Path path;
    IndexOptInfo* indexinfo;
    List* indexclauses;
    List* indexquals;
    List* indexqualcols;
    List* indexorderbys;
    List* indexorderbycols;
    int rulesforindexgen = NO_PATH_GEN_RULE;
    ScanDirection indexscandir;
    Cost indextotalcost;
    Selectivity indexselectivity;
    bool isAnnIndex;
    List* annQuals;
    List* annQualCols;
    Cost annQualTotalCost;
    Selectivity annQualSelectivity;
    Cost allcost; // index cost + qual cost
    bool is_ustore;
} IndexPath;

typedef struct PartIteratorPath {
    Path path;
    PartitionType partType;
    Path* subPath;
    int itrs;
    ScanDirection direction;
    bool ispwj;
    /* the upper boundary list for the partitions in pruning_result, it is meanless unless it is a partitionwise join */
    List* upperboundary;
    /* the lower boundary list for the partitions in pruning_result, it is meanless unless it is a partitionwise join */
    List* lowerboundary;
    bool needSortNode; /* for min/max Optimization, need to add sort node. */
} PartIteratorPath;

/*
 * BitmapHeapPath represents one or more indexscans that generate TID bitmaps
 * instead of directly accessing the heap, followed by AND/OR combinations
 * to produce a single bitmap, followed by a heap scan that uses the bitmap.
 * Note that the output is always considered unordered, since it will come
 * out in physical heap order no matter what the underlying indexes did.
 *
 * The individual indexscans are represented by IndexPath nodes, and any
 * logic on top of them is represented by a tree of BitmapAndPath and
 * BitmapOrPath nodes.	Notice that we can use the same IndexPath node both
 * to represent a regular (or index-only) index scan plan, and as the child
 * of a BitmapHeapPath that represents scanning the same index using a
 * BitmapIndexScan.  The startup_cost and total_cost figures of an IndexPath
 * always represent the costs to use it as a regular (or index-only)
 * IndexScan.  The costs of a BitmapIndexScan can be computed using the
 * IndexPath's indextotalcost and indexselectivity.
 */
typedef struct BitmapHeapPath {
    Path path;
    Path* bitmapqual; /* IndexPath, BitmapAndPath, BitmapOrPath */
} BitmapHeapPath;

/*
 * BitmapAndPath represents a BitmapAnd plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
typedef struct BitmapAndPath {
    Path path;
    List* bitmapquals; /* IndexPaths and BitmapOrPaths */
    Selectivity bitmapselectivity;
    bool is_ustore;
} BitmapAndPath;

/*
 * BitmapOrPath represents a BitmapOr plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
typedef struct BitmapOrPath {
    Path path;
    List* bitmapquals; /* IndexPaths and BitmapAndPaths */
    Selectivity bitmapselectivity;
    bool is_ustore;
} BitmapOrPath;

/*
 * TidPath represents a scan by TID
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant" or "CTID = ANY(pseudoconstant_array)".
 * Note they are bare expressions, not RestrictInfos.
 */
typedef struct TidPath {
    Path path;
    List* tidquals; /* qual(s) involving CTID = something */
} TidPath;

/*
 * SubqueryScanPath represents a scan of an unflattened subquery-in-FROM
 *
 * Note that the subpath comes from a different planning domain; for example
 * RTE indexes within it mean something different from those known to the
 * SubqueryScanPath.  path.parent->subroot is the planning context needed to
 * interpret the subpath.
 * NOTE: GaussDB keep an subplan other than the sub-path
 */
typedef struct SubqueryScanPath
{
    Path        path;
    List        *subplan_params;
    PlannerInfo *subroot;
    struct Plan *subplan;        /* path representing subquery execution */
} SubqueryScanPath;

/*
 * ForeignPath represents a potential scan of a foreign table
 *
 * fdw_private stores FDW private data about the scan.	While fdw_private is
 * not actually touched by the core code during normal operations, it's
 * generally a good idea to use a representation that can be dumped by
 * nodeToString(), so that you can examine the structure during debugging
 * with tools like pprint().
 */
typedef struct ForeignPath {
    Path path;
    Path* fdw_outerpath;
    List* fdw_private;
} ForeignPath;

/*
 * ExtensiblePath represents a table scan done by some out-of-core extension.
 *
 * We provide a set of hooks here - which the provider must take care to set
 * up correctly - to allow extensions to supply their own methods of scanning
 * a relation.  For example, a provider might provide GPU acceleration, a
 * cache-based scan, or some other kind of logic we haven't dreamed up yet.
 *
 * ExtensiblePaths can be injected into the planning process for a relation by
 * set_rel_pathlist_hook functions.
 *
 * Core code must avoid assuming that the ExtensiblePath is only as large as
 * the structure declared here; providers are allowed to make it the first
 * element in a larger structure.  (Since the planner never copies Paths,
 * this doesn't add any complication.)  However, for consistency with the
 * FDW case, we provide a "extensible_private" field in ExtensiblePath; providers
 * may prefer to use that rather than define another struct type.
 */
struct ExtensiblePath;

typedef struct ExtensiblePathMethods {
    const char* ExtensibleName;

    /* Convert Path to a Plan */
    struct Plan* (*PlanExtensiblePath)(PlannerInfo* root, RelOptInfo* rel, struct ExtensiblePath* best_path,
        List* tlist, List* clauses, List* extensible_plans);
} ExtensiblePathMethods;

typedef struct ExtensiblePath {
    Path path;
    uint32 flags;           /* mask of EXTENSIBLEPATH_* flags */
    List* extensible_paths; /* list of child Path nodes, if any */
    List* extensible_private;
    const struct ExtensiblePathMethods* methods;
} ExtensiblePath;
/*
 * AppendPath represents an Append plan, ie, successive execution of
 * several member plans.
 *
 * Note: it is possible for "subpaths" to contain only one, or even no,
 * elements.  These cases are optimized during create_append_plan.
 * In particular, an AppendPath with no subpaths is a "dummy" path that
 * is created to represent the case that a relation is provably empty.
 */
typedef struct AppendPath {
    Path path;
    List* subpaths; /* list of component Paths */
} AppendPath;

#define IS_DUMMY_PATH(p) (IsA((p), AppendPath) && ((AppendPath*)(p))->subpaths == NIL)

/* A relation that's been proven empty will have one path that is dummy */
#define IS_DUMMY_REL(r) ((r)->cheapest_total_path != NIL && IS_DUMMY_PATH(linitial((r)->cheapest_total_path)))

/*
 * MergeAppendPath represents a MergeAppend plan, ie, the merging of sorted
 * results from several member plans to produce similarly-sorted output.
 */
typedef struct MergeAppendPath {
    Path path;
    List* subpaths;      /* list of component Paths */
    double limit_tuples; /* hard limit on output tuples, or -1 */
    OpMemInfo* mem_info;
} MergeAppendPath;

/*
 * ResultPath represents use of a Result plan node to compute a variable-free
 * targetlist with no underlying tables (a "SELECT expressions" query).
 * The query could have a WHERE clause, too, represented by "quals".
 *
 * Note that quals is a list of bare clauses, not RestrictInfos.
 */
typedef struct ResultPath {
    Path path;
    List* quals;
    Path* subpath;
    List* pathqual;
    bool ispulledupqual;  // qual is pulled up from lower path
} ResultPath;

/*
 * MaterialPath represents use of a Material plan node, i.e., caching of
 * the output of its subpath.  This is used when the subpath is expensive
 * and needs to be scanned repeatedly, or when we need mark/restore ability
 * and the subpath doesn't have it.
 */
typedef struct MaterialPath {
    Path path;
    Path* subpath;
    bool materialize_all; /* true for materialize above streamed subplan */
    OpMemInfo mem_info;   /* Memory info for materialize */
} MaterialPath;

/*
 * UniquePath represents elimination of distinct rows from the output of
 * its subpath.
 *
 * This is unlike the other Path nodes in that it can actually generate
 * different plans: either hash-based or sort-based implementation, or a
 * no-op if the input path can be proven distinct already.	The decision
 * is sufficiently localized that it's not worth having separate Path node
 * types.  (Note: in the no-op case, we could eliminate the UniquePath node
 * entirely and just return the subpath; but it's convenient to have a
 * UniquePath in the path tree to signal upper-level routines that the input
 * is known distinct.)
 */
typedef enum {
    UNIQUE_PATH_NOOP, /* input is known unique already */
    UNIQUE_PATH_HASH, /* use hashing */
    UNIQUE_PATH_SORT  /* use sorting */
} UniquePathMethod;

typedef struct UniquePath {
    Path path;
    Path* subpath;
    UniquePathMethod umethod;
    List* in_operators; /* equality operators of the IN clause */
    List* uniq_exprs;   /* expressions to be made unique */
    bool both_method;
    bool hold_tlist;
    OpMemInfo mem_info; /* Memory info for hashagg or sort */
} UniquePath;

/*
 * All join-type paths share these fields.
 */
typedef struct JoinPath {
    Path path;

    JoinType jointype;
    bool inner_unique; /* each outer tuple provably matches no more
                               * than one inner tuple */

    Path* outerjoinpath; /* path for the outer side of the join */
    Path* innerjoinpath; /* path for the inner side of the join */

    List* joinrestrictinfo; /* RestrictInfos to apply to join */

    /*
     * See the notes for RelOptInfo and ParamPathInfo to understand why
     * joinrestrictinfo is needed in JoinPath, and can't be merged into the
     * parent RelOptInfo.
     */
    int skewoptimize;
} JoinPath;

/*
 * A nested-loop path needs no special fields.
 */
typedef JoinPath NestPath;

/*
 * ProjectionPath represents a projection (that is, targetlist computation)
 *
 * Nominally, this path node represents using a Result plan node to do a
 * projection step.  However, if the input plan node supports projection,
 * we can just modify its output targetlist to do the required calculations
 * directly, and not need a Result.  In some places in the planner we can just
 * jam the desired PathTarget into the input path node (and adjust its cost
 * accordingly), so we don't need a ProjectionPath.  But in other places
 * it's necessary to not modify the input path node, so we need a separate
 * ProjectionPath node, which is marked dummy to indicate that we intend to
 * assign the work to the input plan node.  The estimated cost for the
 * ProjectionPath node will account for whether a Result will be used or not.
 */
typedef struct ProjectionPath {
    Path path;
    Path *subpath; /* path representing input source */
    bool dummypp;  /* true if no separate Result is needed */
} ProjectionPath;

/*
 * ProjectSetPath represents evaluation of a targetlist that includes
 * set-returning function(s), which will need to be implemented by a
 * ProjectSet plan node.
 */
typedef struct ProjectSetPath {
    Path path;
    Path *subpath; /* path representing input source */
} ProjectSetPath;

/*
 * A mergejoin path has these fields.
 *
 * Unlike other path types, a MergePath node doesn't represent just a single
 * run-time plan node: it can represent up to four.  Aside from the MergeJoin
 * node itself, there can be a Sort node for the outer input, a Sort node
 * for the inner input, and/or a Material node for the inner input.  We could
 * represent these nodes by separate path nodes, but considering how many
 * different merge paths are investigated during a complex join problem,
 * it seems better to avoid unnecessary palloc overhead.
 *
 * path_mergeclauses lists the clauses (in the form of RestrictInfos)
 * that will be used in the merge.
 *
 * Note that the mergeclauses are a subset of the parent relation's
 * restriction-clause list.  Any join clauses that are not mergejoinable
 * appear only in the parent's restrict list, and must be checked by a
 * qpqual at execution time.
 *
 * outersortkeys (resp. innersortkeys) is NIL if the outer path
 * (resp. inner path) is already ordered appropriately for the
 * mergejoin.  If it is not NIL then it is a PathKeys list describing
 * the ordering that must be created by an explicit Sort node.
 *
 * materialize_inner is TRUE if a Material node should be placed atop the
 * inner input.  This may appear with or without an inner Sort step.
 */
typedef struct MergePath {
    JoinPath jpath;
    List* path_mergeclauses;  /* join clauses to be used for merge */
    List* outersortkeys;      /* keys for explicit sort, if any */
    List* innersortkeys;      /* keys for explicit sort, if any */
    bool skip_mark_restore;		/* can executor skip mark/restore? */
    bool materialize_inner;   /* add Materialize to inner? */
    OpMemInfo outer_mem_info; /* Mem info for outer explicit sort */
    OpMemInfo inner_mem_info; /* Mem info for inner explicit sort */
    OpMemInfo mat_mem_info;   /* Mem info for materialization of inner */
} MergePath;

/*
 * A hashjoin path has these fields.
 *
 * The remarks above for mergeclauses apply for hashclauses as well.
 *
 * Hashjoin does not care what order its inputs appear in, so we have
 * no need for sortkeys.
 */
typedef struct HashPath {
    JoinPath jpath;
    List* path_hashclauses; /* join clauses used for hashing */
    int num_batches;        /* number of batches expected */
    OpMemInfo mem_info;     /* Mem info for hash table */
    double joinRows;
} HashPath;

#ifdef PGXC
/*
 * A remotequery path represents the queries to be sent to the datanode/s
 *
 * When RemoteQuery plan is created from RemoteQueryPath, we build the query to
 * be executed at the datanode. For building such a query, it's important to get
 * the RHS relation and LHS relation of the JOIN clause. So, instead of storing
 * the outer and inner paths, we find out the RHS and LHS paths and store those
 * here.
 */
typedef struct RemoteQueryPath {
    Path path;
    ExecNodes* rqpath_en; /* List of datanodes to execute the query on */
    /*
     * If the path represents a JOIN rel, leftpath and rightpath represent the
     * RemoteQuery paths for left (outer) and right (inner) side of the JOIN
     * resp. jointype and join_restrictlist pertains to such JOINs.
     */
    struct RemoteQueryPath* leftpath;
    struct RemoteQueryPath* rightpath;
    JoinType jointype;
    List* join_restrictlist;      /* restrict list corresponding to JOINs,
                                   * only considered if rest of
                                   * the JOIN information is
                                   * available
                                   */
    bool rqhas_unshippable_qual;  /* TRUE if there is at least
                                   * one qual which can not be
                                   * shipped to the datanodes
                                   */
    bool rqhas_temp_rel;          /* TRUE if one of the base relations
                                   * involved in this path is a temporary
                                   * table.
                                   */
    bool rqhas_unshippable_tlist; /* TRUE if there is at least one
                                   * targetlist entry which is
                                   * not completely shippable.
                                   */
} RemoteQueryPath;
#endif /* PGXC */

/*
 * Cached bucket selectivity for hashjoin.
 *
 * Since bucket selectivity is limited by hashjoin bucket size, so we should only use the cache
 * when bucket size is the same.
 */
typedef struct BucketSelectivity {
    double nbuckets;
    Selectivity bucket_size;
    double ndistinct;
} BucketSelectivity;

/*
 * Cached bucket selectivity for one side of restrictinfo.
 *
 * Since bucket selectivity differs among different data georgraphy, so we should cache three
 * stream cases for one side of restrictinfo: non-stream, broadcast, redistribute.
 */
typedef struct BucketSize {
    BucketSelectivity normal;
    BucketSelectivity broadcast;
    BucketSelectivity redistribute;
} BucketSize;

/*
 * Restriction clause info.
 *
 * We create one of these for each AND sub-clause of a restriction condition
 * (WHERE or JOIN/ON clause).  Since the restriction clauses are logically
 * ANDed, we can use any one of them or any subset of them to filter out
 * tuples, without having to evaluate the rest.  The RestrictInfo node itself
 * stores data used by the optimizer while choosing the best query plan.
 *
 * If a restriction clause references a single base relation, it will appear
 * in the baserestrictinfo list of the RelOptInfo for that base rel.
 *
 * If a restriction clause references more than one base rel, it will
 * appear in the joininfo list of every RelOptInfo that describes a strict
 * subset of the base rels mentioned in the clause.  The joininfo lists are
 * used to drive join tree building by selecting plausible join candidates.
 * The clause cannot actually be applied until we have built a join rel
 * containing all the base rels it references, however.
 *
 * When we construct a join rel that includes all the base rels referenced
 * in a multi-relation restriction clause, we place that clause into the
 * joinrestrictinfo lists of paths for the join rel, if neither left nor
 * right sub-path includes all base rels referenced in the clause.	The clause
 * will be applied at that join level, and will not propagate any further up
 * the join tree.  (Note: the "predicate migration" code was once intended to
 * push restriction clauses up and down the plan tree based on evaluation
 * costs, but it's dead code and is unlikely to be resurrected in the
 * foreseeable future.)
 *
 * Note that in the presence of more than two rels, a multi-rel restriction
 * might reach different heights in the join tree depending on the join
 * sequence we use.  So, these clauses cannot be associated directly with
 * the join RelOptInfo, but must be kept track of on a per-join-path basis.
 *
 * RestrictInfos that represent equivalence conditions (i.e., mergejoinable
 * equalities that are not outerjoin-delayed) are handled a bit differently.
 * Initially we attach them to the EquivalenceClasses that are derived from
 * them.  When we construct a scan or join path, we look through all the
 * EquivalenceClasses and generate derived RestrictInfos representing the
 * minimal set of conditions that need to be checked for this particular scan
 * or join to enforce that all members of each EquivalenceClass are in fact
 * equal in all rows emitted by the scan or join.
 *
 * When dealing with outer joins we have to be very careful about pushing qual
 * clauses up and down the tree.  An outer join's own JOIN/ON conditions must
 * be evaluated exactly at that join node, unless they are "degenerate"
 * conditions that reference only Vars from the nullable side of the join.
 * Quals appearing in WHERE or in a JOIN above the outer join cannot be pushed
 * down below the outer join, if they reference any nullable Vars.
 * RestrictInfo nodes contain a flag to indicate whether a qual has been
 * pushed down to a lower level than its original syntactic placement in the
 * join tree would suggest.  If an outer join prevents us from pushing a qual
 * down to its "natural" semantic level (the level associated with just the
 * base rels used in the qual) then we mark the qual with a "required_relids"
 * value including more than just the base rels it actually uses.  By
 * pretending that the qual references all the rels required to form the outer
 * join, we prevent it from being evaluated below the outer join's joinrel.
 * When we do form the outer join's joinrel, we still need to distinguish
 * those quals that are actually in that join's JOIN/ON condition from those
 * that appeared elsewhere in the tree and were pushed down to the join rel
 * because they used no other rels.  That's what the is_pushed_down flag is
 * for; it tells us that a qual is not an OUTER JOIN qual for the set of base
 * rels listed in required_relids.	A clause that originally came from WHERE
 * or an INNER JOIN condition will *always* have its is_pushed_down flag set.
 * It's possible for an OUTER JOIN clause to be marked is_pushed_down too,
 * if we decide that it can be pushed down into the nullable side of the join.
 * In that case it acts as a plain filter qual for wherever it gets evaluated.
 * (In short, is_pushed_down is only false for non-degenerate outer join
 * conditions.	Possibly we should rename it to reflect that meaning?)
 *
 * RestrictInfo nodes also contain an outerjoin_delayed flag, which is true
 * if the clause's applicability must be delayed due to any outer joins
 * appearing below it (ie, it has to be postponed to some join level higher
 * than the set of relations it actually references).
 *
 * There is also an outer_relids field, which is NULL except for outer join
 * clauses; for those, it is the set of relids on the outer side of the
 * clause's outer join.  (These are rels that the clause cannot be applied to
 * in parameterized scans, since pushing it into the join's outer side would
 * lead to wrong answers.)
 *
 * There is also a nullable_relids field, which is the set of rels the clause
 * references that can be forced null by some outer join below the clause.
 *
 * outerjoin_delayed = true is subtly different from nullable_relids != NULL:
 * a clause might reference some nullable rels and yet not be
 * outerjoin_delayed because it also references all the other rels of the
 * outer join(s). A clause that is not outerjoin_delayed can be enforced
 * anywhere it is computable.
 *
 * To handle security-barrier conditions efficiently, we mark RestrictInfo
 * nodes with a security_level field, in which higher values identify clauses
 * coming from less-trusted sources.  The exact semantics are that a clause
 * cannot be evaluated before another clause with a lower security_level value
 * unless the first clause is leakproof.  As with outer-join clauses, this
 * creates a reason for clauses to sometimes need to be evaluated higher in
 * the join tree than their contents would suggest; and even at a single plan
 * node, this rule constrains the order of application of clauses.
 *
 * In general, the referenced clause might be arbitrarily complex.	The
 * kinds of clauses we can handle as indexscan quals, mergejoin clauses,
 * or hashjoin clauses are limited (e.g., no volatile functions).  The code
 * for each kind of path is responsible for identifying the restrict clauses
 * it can use and ignoring the rest.  Clauses not implemented by an indexscan,
 * mergejoin, or hashjoin will be placed in the plan qual or joinqual field
 * of the finished Plan node, where they will be enforced by general-purpose
 * qual-expression-evaluation code.  (But we are still entitled to count
 * their selectivity when estimating the result tuple count, if we
 * can guess what it is...)
 *
 * When the referenced clause is an OR clause, we generate a modified copy
 * in which additional RestrictInfo nodes are inserted below the top-level
 * OR/AND structure.  This is a convenience for OR indexscan processing:
 * indexquals taken from either the top level or an OR subclause will have
 * associated RestrictInfo nodes.
 *
 * The can_join flag is set true if the clause looks potentially useful as
 * a merge or hash join clause, that is if it is a binary opclause with
 * nonoverlapping sets of relids referenced in the left and right sides.
 * (Whether the operator is actually merge or hash joinable isn't checked,
 * however.)
 *
 * The pseudoconstant flag is set true if the clause contains no Vars of
 * the current query level and no volatile functions.  Such a clause can be
 * pulled out and used as a one-time qual in a gating Result node.	We keep
 * pseudoconstant clauses in the same lists as other RestrictInfos so that
 * the regular clause-pushing machinery can assign them to the correct join
 * level, but they need to be treated specially for cost and selectivity
 * estimates.  Note that a pseudoconstant clause can never be an indexqual
 * or merge or hash join clause, so it's of no interest to large parts of
 * the planner.
 *
 * When join clauses are generated from EquivalenceClasses, there may be
 * several equally valid ways to enforce join equivalence, of which we need
 * apply only one.	We mark clauses of this kind by setting parent_ec to
 * point to the generating EquivalenceClass.  Multiple clauses with the same
 * parent_ec in the same join are redundant.
 */
typedef struct RestrictInfo {
    NodeTag type;

    Expr* clause; /* the represented clause of WHERE or JOIN */

    bool is_pushed_down; /* TRUE if clause was pushed down in level */

    bool outerjoin_delayed; /* TRUE if delayed by lower outer join */

    bool can_join; /* see comment above */

    bool pseudoconstant; /* see comment above */

    bool leakproof; /* TRUE if known to contain no leaked Vars */

    Index security_level; /* Mark RestrictInfo nodes with a security_level */

    /* The set of relids (varnos) actually referenced in the clause: */
    Relids clause_relids;

    /* The set of relids required to evaluate the clause: */
    Relids required_relids;

    /* If an outer-join clause, the outer-side relations, else NULL: */
    Relids outer_relids;

    /* The relids used in the clause that are nullable by lower outer joins: */
    Relids nullable_relids;

    /* These fields are set for any binary opclause: */
    Relids left_relids;  /* relids in left side of clause */
    Relids right_relids; /* relids in right side of clause */

    /* This field is NULL unless clause is an OR clause: */
    Expr* orclause; /* modified clause with RestrictInfos */

    /* This field is NULL unless clause is potentially redundant: */
    EquivalenceClass* parent_ec; /* generating EquivalenceClass */

    /* cache space for cost and selectivity */
    QualCost eval_cost;      /* eval cost of clause; -1 if not yet set */
    Selectivity norm_selec;  /* selectivity for "normal" (JOIN_INNER)
                              * semantics; -1 if not yet set; >1 means a
                              * redundant clause */
    Selectivity outer_selec; /* selectivity for outer join semantics; -1 if
                              * not yet set */

    /* valid if clause is mergejoinable, else NIL */
    List* mergeopfamilies; /* opfamilies containing clause operator */

    /* cache space for mergeclause processing; NULL if not yet set */
    EquivalenceClass* left_ec;   /* EquivalenceClass containing lefthand */
    EquivalenceClass* right_ec;  /* EquivalenceClass containing righthand */
    EquivalenceMember* left_em;  /* EquivalenceMember for lefthand */
    EquivalenceMember* right_em; /* EquivalenceMember for righthand */
    List* scansel_cache;         /* list of MergeScanSelCache structs */

    /* transient workspace for use while considering a specific join path */
    bool outer_is_left; /* T = outer var on left, F = on right */

    /* valid if clause is hashjoinable, else InvalidOid: */
    Oid hashjoinoperator; /* copy of clause operator */

    /* cache space for hashclause processing; -1 if not yet set */
    BucketSize left_bucketsize;  /* avg bucketsize of left side */
    BucketSize right_bucketsize; /* avg bucketsize of right side */
} RestrictInfo;

/*
 * Since mergejoinscansel() is a relatively expensive function, and would
 * otherwise be invoked many times while planning a large join tree,
 * we go out of our way to cache its results.  Each mergejoinable
 * RestrictInfo carries a list of the specific sort orderings that have
 * been considered for use with it, and the resulting selectivities.
 */
typedef struct MergeScanSelCache {
    /* Ordering details (cache lookup key) */
    Oid opfamily;     /* btree opfamily defining the ordering */
    Oid collation;    /* collation for the ordering */
    int strategy;     /* sort direction (ASC or DESC) */
    bool nulls_first; /* do NULLs come before normal values? */
    /* Results */
    Selectivity leftstartsel;  /* first-join fraction for clause left side */
    Selectivity leftendsel;    /* last-join fraction for clause left side */
    Selectivity rightstartsel; /* first-join fraction for clause right side */
    Selectivity rightendsel;   /* last-join fraction for clause right side */
} MergeScanSelCache;

/*
 * Placeholder node for an expression to be evaluated below the top level
 * of a plan tree.	This is used during planning to represent the contained
 * expression.	At the end of the planning process it is replaced by either
 * the contained expression or a Var referring to a lower-level evaluation of
 * the contained expression.  Typically the evaluation occurs below an outer
 * join, and Var references above the outer join might thereby yield NULL
 * instead of the expression value.
 *
 * Although the planner treats this as an expression node type, it is not
 * recognized by the parser or executor, so we declare it here rather than
 * in primnodes.h.
 */
typedef struct PlaceHolderVar {
    Expr xpr;
    Expr* phexpr;     /* the represented expression */
    Relids phrels;    /* base relids syntactically within expr src */
    Index phid;       /* ID for PHV (unique within planner run) */
    Index phlevelsup; /* > 0 if PHV belongs to outer query */
} PlaceHolderVar;

/*
 * "Special join" info.
 *
 * One-sided outer joins constrain the order of joining partially but not
 * completely.	We flatten such joins into the planner's top-level list of
 * relations to join, but record information about each outer join in a
 * SpecialJoinInfo struct.	These structs are kept in the PlannerInfo node's
 * join_info_list.
 *
 * Similarly, semijoins and antijoins created by flattening IN (subselect)
 * and EXISTS(subselect) clauses create partial constraints on join order.
 * These are likewise recorded in SpecialJoinInfo structs.
 *
 * We make SpecialJoinInfos for FULL JOINs even though there is no flexibility
 * of planning for them, because this simplifies make_join_rel()'s API.
 *
 * min_lefthand and min_righthand are the sets of base relids that must be
 * available on each side when performing the special join.  lhs_strict is
 * true if the special join's condition cannot succeed when the LHS variables
 * are all NULL (this means that an outer join can commute with upper-level
 * outer joins even if it appears in their RHS).  We don't bother to set
 * lhs_strict for FULL JOINs, however.
 *
 * It is not valid for either min_lefthand or min_righthand to be empty sets;
 * if they were, this would break the logic that enforces join order.
 *
 * syn_lefthand and syn_righthand are the sets of base relids that are
 * syntactically below this special join.  (These are needed to help compute
 * min_lefthand and min_righthand for higher joins.)
 *
 * delay_upper_joins is set TRUE if we detect a pushed-down clause that has
 * to be evaluated after this join is formed (because it references the RHS).
 * Any outer joins that have such a clause and this join in their RHS cannot
 * commute with this join, because that would leave noplace to check the
 * pushed-down clause.	(We don't track this for FULL JOINs, either.)
 *
 * join_quals is an implicit-AND list of the quals syntactically associated
 * with the join (they may or may not end up being applied at the join level).
 * This is just a side list and does not drive actual application of quals.
 * For JOIN_SEMI joins, this is cleared to NIL in create_unique_path() if
 * the join is found not to be suitable for a uniqueify-the-RHS plan.
 *
 * jointype is never JOIN_RIGHT; a RIGHT JOIN is handled by switching
 * the inputs to make it a LEFT JOIN.  So the allowed values of jointype
 * in a join_info_list member are only LEFT, FULL, SEMI, or ANTI.
 *
 * For purposes of join selectivity estimation, we create transient
 * SpecialJoinInfo structures for regular inner joins; so it is possible
 * to have jointype == JOIN_INNER in such a structure, even though this is
 * not allowed within join_info_list.  We also create transient
 * SpecialJoinInfos with jointype == JOIN_INNER for outer joins, since for
 * cost estimation purposes it is sometimes useful to know the join size under
 * plain innerjoin semantics.  Note that lhs_strict, delay_upper_joins, and
 * join_quals are not set meaningfully within such structs.
 */
typedef struct SpecialJoinInfo {
    NodeTag type;
    Relids min_lefthand;    /* base relids in minimum LHS for join */
    Relids min_righthand;   /* base relids in minimum RHS for join */
    Relids syn_lefthand;    /* base relids syntactically within LHS */
    Relids syn_righthand;   /* base relids syntactically within RHS */
    JoinType jointype;      /* always INNER, LEFT, FULL, SEMI, or ANTI */
    bool lhs_strict;        /* joinclause is strict for some LHS rel */
    bool delay_upper_joins; /* can't commute with upper RHS */
    List* join_quals;       /* join quals, in implicit-AND list format */
    bool varratio_cached;   /* decide chach selec or not. */
    bool is_straight_join;  /* set true if is straight_join*/
} SpecialJoinInfo;

/*
 * "Lateral join" info.
 *
 * Lateral references in subqueries constrain the join order in a way that's
 * somewhat like outer joins, though different in detail.  We construct one or
 * more LateralJoinInfos for each RTE with lateral references, and add them to
 * the PlannerInfo node's lateral_info_list.
 *
 * lateral_rhs is the relid of a baserel with lateral references, and
 * lateral_lhs is a set of relids of baserels it references, all of which
 * must be present on the LHS to compute a parameter needed by the RHS.
 * Typically, lateral_lhs is a singleton, but it can include multiple rels
 * if the RHS references a PlaceHolderVar with a multi-rel ph_eval_at level.
 * We disallow joining to only part of the LHS in such cases, since that would
 * result in a join tree with no convenient place to compute the PHV.
 *
 * When an appendrel contains lateral references (eg "LATERAL (SELECT x.col1
 * UNION ALL SELECT y.col2)"), the LateralJoinInfos reference the parent
 * baserel not the member otherrels, since it is the parent relid that is
 * considered for joining purposes.
 */
typedef struct LateralJoinInfo
{
   NodeTag     type;
   Index       lateral_rhs;    /* a baserel containing lateral refs */
   Relids      lateral_lhs;    /* some base relids it references */
} LateralJoinInfo;


/*
 * Append-relation info.
 *
 * When we expand an inheritable table or a UNION-ALL subselect into an
 * "append relation" (essentially, a list of child RTEs), we build an
 * AppendRelInfo for each child RTE.  The list of AppendRelInfos indicates
 * which child RTEs must be included when expanding the parent, and each
 * node carries information needed to translate Vars referencing the parent
 * into Vars referencing that child.
 *
 * These structs are kept in the PlannerInfo node's append_rel_list.
 * Note that we just throw all the structs into one list, and scan the
 * whole list when desiring to expand any one parent.  We could have used
 * a more complex data structure (eg, one list per parent), but this would
 * be harder to update during operations such as pulling up subqueries,
 * and not really any easier to scan.  Considering that typical queries
 * will not have many different append parents, it doesn't seem worthwhile
 * to complicate things.
 *
 * Note: after completion of the planner prep phase, any given RTE is an
 * append parent having entries in append_rel_list if and only if its
 * "inh" flag is set.  We clear "inh" for plain tables that turn out not
 * to have inheritance children, and (in an abuse of the original meaning
 * of the flag) we set "inh" for subquery RTEs that turn out to be
 * flattenable UNION ALL queries.  This lets us avoid useless searches
 * of append_rel_list.
 *
 * Note: the data structure assumes that append-rel members are single
 * baserels.  This is OK for inheritance, but it prevents us from pulling
 * up a UNION ALL member subquery if it contains a join.  While that could
 * be fixed with a more complex data structure, at present there's not much
 * point because no improvement in the plan could result.
 */
typedef struct AppendRelInfo {
    NodeTag type;

    /*
     * These fields uniquely identify this append relationship.  There can be
     * (in fact, always should be) multiple AppendRelInfos for the same
     * parent_relid, but never more than one per child_relid, since a given
     * RTE cannot be a child of more than one append parent.
     */
    Index parent_relid; /* RT index of append parent rel */
    Index child_relid;  /* RT index of append child rel */

    /*
     * For an inheritance appendrel, the parent and child are both regular
     * relations, and we store their rowtype OIDs here for use in translating
     * whole-row Vars.	For a UNION-ALL appendrel, the parent and child are
     * both subqueries with no named rowtype, and we store InvalidOid here.
     */
    Oid parent_reltype; /* OID of parent's composite type */
    Oid child_reltype;  /* OID of child's composite type */

    /*
     * The N'th element of this list is a Var or expression representing the
     * child column corresponding to the N'th column of the parent. This is
     * used to translate Vars referencing the parent rel into references to
     * the child.  A list element is NULL if it corresponds to a dropped
     * column of the parent (this is only possible for inheritance cases, not
     * UNION ALL).	The list elements are always simple Vars for inheritance
     * cases, but can be arbitrary expressions in UNION ALL cases.
     *
     * Notice we only store entries for user columns (attno > 0).  Whole-row
     * Vars are special-cased, and system columns (attno < 0) need no special
     * translation since their attnos are the same for all tables.
     *
     * Caution: the Vars have varlevelsup = 0.	Be careful to adjust as needed
     * when copying into a subquery.
     */
    List* translated_vars; /* Expressions in the child's Vars */

    /*
     * We store the parent table's OID here for inheritance, or InvalidOid for
     * UNION ALL.  This is only needed to help in generating error messages if
     * an attempt is made to reference a dropped parent column.
     */
    Oid parent_reloid; /* OID of parent relation */
} AppendRelInfo;

/*
 * For each distinct placeholder expression generated during planning, we
 * store a PlaceHolderInfo node in the PlannerInfo node's placeholder_list.
 * This stores info that is needed centrally rather than in each copy of the
 * PlaceHolderVar.	The phid fields identify which PlaceHolderInfo goes with
 * each PlaceHolderVar.  Note that phid is unique throughout a planner run,
 * not just within a query level --- this is so that we need not reassign ID's
 * when pulling a subquery into its parent.
 *
 * The idea is to evaluate the expression at (only) the ph_eval_at join level,
 * then allow it to bubble up like a Var until the ph_needed join level.
 * ph_needed has the same definition as attr_needed for a regular Var.
 *
 * ph_may_need is an initial estimate of ph_needed, formed using the
 * syntactic locations of references to the PHV.  We need this in order to
 * determine whether the PHV reference forces a join ordering constraint:
 * if the PHV has to be evaluated below the nullable side of an outer join,
 * and then used above that outer join, we must constrain join order to ensure
 * there's a valid place to evaluate the PHV below the join.  The final
 * actual ph_needed level might be lower than ph_may_need, but we can't
 * determine that until later on.  Fortunately this doesn't matter for what
 * we need ph_may_need for: if there's a PHV reference syntactically
 * above the outer join, it's not going to be allowed to drop below the outer
 * join, so we would come to the same conclusions about join order even if
 * we had the final ph_needed value to compare to.
 *
 * We create a PlaceHolderInfo only after determining that the PlaceHolderVar
 * is actually referenced in the plan tree, so that unreferenced placeholders
 * don't result in unnecessary constraints on join order.
 */
typedef struct PlaceHolderInfo {
    NodeTag type;

    Index phid;             /* ID for PH (unique within planner run) */
    PlaceHolderVar* ph_var; /* copy of PlaceHolderVar tree */
    Relids ph_eval_at;      /* lowest level we can evaluate value at */
    Relids ph_needed;       /* highest level the value is needed at */
    int32 ph_width;         /* estimated attribute width */
} PlaceHolderInfo;

/*
 * For each potentially index-optimizable MIN/MAX aggregate function,
 * root->minmax_aggs stores a MinMaxAggInfo describing it.
 */
typedef struct MinMaxAggInfo {
    NodeTag type;

    Oid aggfnoid;         /* pg_proc Oid of the aggregate */
    Oid aggsortop;        /* Oid of its sort operator */
    Expr* target;         /* expression we are aggregating on */
    PlannerInfo* subroot; /* modified "root" for planning the subquery */
    Path* path;           /* access path for subquery */
    Cost pathcost;        /* estimated cost to fetch first row */
    Param* param;         /* param for subplan's output */
    Aggref* aggref;       /* used for construct the final agg in distributed env */
} MinMaxAggInfo;

/*
 * At runtime, PARAM_EXEC slots are used to pass values around from one plan
 * node to another.  They can be used to pass values down into subqueries (for
 * outer references in subqueries), or up out of subqueries (for the results
 * of a subplan), or from a NestLoop plan node into its inner relation (when
 * the inner scan is parameterized with values from the outer relation).
 * The planner is responsible for assigning nonconflicting PARAM_EXEC IDs to
 * the PARAM_EXEC Params it generates.
 *
 * Outer references are managed via root->plan_params, which is a list of
 * PlannerParamItems.  While planning a subquery, each parent query level's
 * plan_params contains the values required from it by the current subquery.
 * During create_plan(), we use plan_params to track values that must be
 * passed from outer to inner sides of NestLoop plan nodes.
 *
 * The item a PlannerParamItem represents can be one of three kinds:
 *
 * A Var: the slot represents a variable of this level that must be passed
 * down because subqueries have outer references to it, or must be passed
 * from a NestLoop node to its inner scan.  The varlevelsup value in the Var
 * will always be zero.
 *
 * A PlaceHolderVar: this works much like the Var case, except that the
 * entry is a PlaceHolderVar node with a contained expression.	The PHV
 * will have phlevelsup = 0, and the contained expression is adjusted
 * to match in level.
 *
 * An Aggref (with an expression tree representing its argument): the slot
 * represents an aggregate expression that is an outer reference for some
 * subquery.  The Aggref itself has agglevelsup = 0, and its argument tree
 * is adjusted to match in level.
 *
 * Note: we detect duplicate Var and PlaceHolderVar parameters and coalesce
 * them into one slot, but we do not bother to do that for Aggrefs.
 * The scope of duplicate-elimination only extends across the set of
 * parameters passed from one query level into a single subquery, or for
 * nestloop parameters across the set of nestloop parameters used in a single
 * query level.  So there is no possibility of a PARAM_EXEC slot being used
 * for conflicting purposes.
 *
 * In addition, PARAM_EXEC slots are assigned for Params representing outputs
 * from subplans (values that are setParam items for those subplans).  These
 * IDs need not be tracked via PlannerParamItems, since we do not need any
 * duplicate-elimination nor later processing of the represented expressions.
 * Instead, we just record the assignment of the slot number by incrementing
 * root->glob->nParamExec.
 */
typedef struct PlannerParamItem {
    NodeTag type;

    Node* item;  /* the Var, PlaceHolderVar, or Aggref */
    int paramId; /* its assigned PARAM_EXEC slot number */
} PlannerParamItem;

/*
 * When making cost estimates for a SEMI/ANTI/inner_unique join, there are
 * some correction factors that are needed in both nestloop and hash joins
 * to account for the fact that the executor can stop scanning inner rows
 * as soon as it finds a match to the current outer row.  These numbers
 * depend only on the selected outer and inner join relations, not on the
 * particular paths used for them, so it's worthwhile to calculate them
 * just once per relation pair not once per considered path.  This struct
 * is filled by compute_semi_anti_join_factors and must be passed along
 * to the join cost estimation functions.
 *
 * outer_match_frac is the fraction of the outer tuples that are
 *		expected to have at least one match.
 * match_count is the average number of matches expected for
 *		outer tuples that have at least one match.
 *
 * Note: For right-semi/anti join, match_count is the fraction of the inner tuples
 * that are expected to have at least one match in outer tuples.
 */
typedef struct SemiAntiJoinFactors {
    Selectivity outer_match_frac;
    Selectivity match_count;
} SemiAntiJoinFactors;

typedef struct JoinPathExtraData
{
    bool inner_unique;
    SpecialJoinInfo *sjinfo;
    SemiAntiJoinFactors semifactors;
} JoinPathExtraData;

/*
 * For speed reasons, cost estimation for join paths is performed in two
 * phases: the first phase tries to quickly derive a lower bound for the
 * join cost, and then we check if that's sufficient to reject the path.
 * If not, we come back for a more refined cost estimate.  The first phase
 * fills a JoinCostWorkspace struct with its preliminary cost estimates
 * and possibly additional intermediate values.  The second phase takes
 * these values as inputs to avoid repeating work.
 *
 * (Ideally we'd declare this in cost.h, but it's also needed in pathnode.h,
 * so seems best to put it here.)
 */
typedef struct JoinCostWorkspace {
    /* Preliminary cost estimates --- must not be larger than final ones! */
    Cost startup_cost; /* cost expended before fetching any tuples */
    Cost total_cost;   /* total cost (assuming all tuples fetched) */

    /* Fields below here should be treated as private to costsize.c */
    Cost run_cost; /* non-startup cost components */

    /* private for cost_nestloop code */
    Cost inner_rescan_run_cost;
    double outer_matched_rows;
    Selectivity inner_scan_frac;

    /* private for cost_mergejoin code */
    Cost inner_run_cost;
    double outer_rows;
    double inner_rows;
    double outer_skip_rows;
    double inner_skip_rows;

    /* private for cost_hashjoin code */
    int numbuckets;
    int numbatches;

    /* Meminfo for joins */
    OpMemInfo outer_mem_info;
    OpMemInfo inner_mem_info;
} JoinCostWorkspace;

#endif /* RELATION_H */
