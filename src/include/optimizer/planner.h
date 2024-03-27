/* -------------------------------------------------------------------------
 *
 * planner.h
 *	  prototypes for planner.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planner.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "executor/exec/execdesc.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/planswcb.h"
#include "nodes/parsenodes.h"
#include "utils/selfuncs.h"

#define INSTALLATION_MODE "installation"

/* Expression kind codes for preprocess_expression */
#define EXPRKIND_QUAL              0
#define EXPRKIND_TARGET            1
#define EXPRKIND_RTFUNC            2
#define EXPRKIND_RTFUNC_LATERAL    3
#define EXPRKIND_VALUES            4
#define EXPRKIND_VALUES_LATERAL    5
#define EXPRKIND_LIMIT             6
#define EXPRKIND_APPINFO           7
#define EXPRKIND_PHV               8
#define EXPRKIND_TABLESAMPLE       9
#define EXPRKIND_TIMECAPSULE       10

/*
 * @hdfs
 * Optimize clause type by using informational constraint.
 */
typedef enum { GROUPBY_CLAUSE_TYPE, DISTINCT_CLAUSE_TYPE, SCAN_QUAL_CLAUSE_TYPE, JOIN_QUAL_CLAUSE_TYPE } ClauseType;

/* Context for mark agg func and denserank in WindowAgg */
typedef struct {
    bool has_agg;
    bool has_denserank;
} DenseRank_context;

extern ExecNodes* getExecNodesByGroupName(const char* gname);
extern PlannedStmt* planner(Query* parse, int cursorOptions, ParamListInfo boundParams);
extern PlannedStmt* standard_planner(Query* parse, int cursorOptions, ParamListInfo boundParams);
extern Plan* grouping_planner(PlannerInfo* root, double tuple_fraction);

typedef PlannedStmt* (*planner_hook_type) (Query* parse, int cursorOptions, ParamListInfo boundParams);
typedef List* (*for_tsdb_hook_type) (PlannerInfo *root,RelOptInfo *rel,
						CmdType operation, bool canSetTag,
						Index nominalRelation,
						List *resultRelations, List *subpaths,
						List *withCheckOptionLists, List *returningLists,
						List *rowMarks, int epqParam,List *tlist);

typedef void (*ndp_pushdown_hook_type) (Query* querytree, PlannedStmt *stmt);
extern THR_LOCAL PGDLLIMPORT ndp_pushdown_hook_type ndp_pushdown_hook;
#ifdef USE_SPQ
typedef PlannedStmt *(*spq_planner_hook_type) (Query* parse, int cursorOptions, ParamListInfo boundParams);
extern THR_LOCAL PGDLLIMPORT spq_planner_hook_type spq_planner_hook;
#endif

typedef Plan* (*grouping_plannerFunc)(PlannerInfo* root, double tuple_fraction);

extern Plan* subquery_planner(PlannerGlobal* glob, Query* parse, PlannerInfo* parent_root, bool hasRecursion,
    double tuple_fraction, PlannerInfo** subroot, int options = SUBQUERY_NORMAL, ItstDisKey* diskeys = NULL,
    List* subqueryRestrictInfo = NIL);

extern void add_tlist_costs_to_plan(PlannerInfo* root, Plan* plan, List* tlist);

extern bool is_dummy_plan(Plan* plan);

extern bool is_single_baseresult_plan(Plan* plan);

extern Expr* expression_planner(Expr* expr);

extern Expr *preprocess_phv_expression(PlannerInfo *root, Expr *expr);

extern bool plan_cluster_use_sort(Oid tableOid, Oid indexOid);

extern bool ContainRecursiveUnionSubplan(PlannedStmt* pstmt);

extern void preprocess_qual_conditions(PlannerInfo* root, Node* jtnode);

extern int apply_set_hint(const Query* parse);

extern void recover_set_hint(int savedNestLevel);

typedef enum {
    /*
     * Disable "inlist2join" rewrite optimization
     */
    QRW_INLIST2JOIN_DISABLE = -1,

    /*
     * Enable "inlist2join" rewrite optimization in cost-base mode, and this is default
     * behavior in optimizer.
     */
    QRW_INLIST2JOIN_CBO = 0,

    /*
     * Enable "inlist2join" rewrite optimization in rule-base mode and without consider
     * the num of elements specified in "In-List" or "= ANY[]" clause.
     *
     * We are considering it as a force mode, more likely it is to help DFX improvement
     * to evaluate Inlist2Join is correct for query result (developer basis)
     */
    QRW_INLIST2JOIN_FORCE = 1,

    /*
     * Other values that greater than 2, inlist2join rewrite optimization runs in extended
     * rule-base mode, where the GUC value plays as the element num of inlist threshold
     * for inlist2join conversion
     */
} QrwInlist2JoinOptMode;

typedef struct RewriteVarMapping {
    Var* old_var;
    Var* new_var;
    bool need_fix; /* the var is needed to fix when create plan */
} RewriteVarMapping;

typedef struct VectorPlanContext {
    bool containRowTable;
    bool forceVectorEngine;
    bool currentExprIsFilter;
    Cost rowCost;
    Cost vecCost;
} VectorPlanContext;

typedef struct VectorExprContext {
    double rows;
    double lefttreeRows;
    VectorPlanContext* planContext;
    List* varList;
} VectorExprContext;

extern MemoryContext SwitchToPlannerTempMemCxt(PlannerInfo *root);
extern MemoryContext ResetPlannerTempMemCxt(PlannerInfo *root, MemoryContext cxt);
extern void fix_vars_plannode(PlannerInfo* root, Plan* plan);
extern void inlist2join_qrw_optimization(PlannerInfo* root, int rti);
extern void find_inlist2join_path(PlannerInfo* root, Path* best_path);

extern bool planClusterPartitionUseSort(Relation partRel, Oid indexOid, PlannerInfo* root, RelOptInfo* relOptInfo);
extern void select_active_windows(PlannerInfo* root, WindowLists* wflists);
extern List* make_pathkeys_for_window(PlannerInfo* root, WindowClause* wc, List* tlist, bool canonicalize);

extern List* get_distributekey_from_tlist(
    PlannerInfo* root, List* tlist, List* groupcls, double rows, double* result_multiple, void* skew_info = NULL);
extern Plan* try_vectorize_plan(Plan* top_plan, Query* parse, bool from_subplan, PlannerInfo* subroot = NULL);
extern bool is_vector_scan(Plan* plan);
extern bool CheckColumnsSuportedByBatchMode(List* targetList, List *qual);

extern bool vector_engine_unsupport_expression_walker(Node* node, VectorPlanContext* planContext = NULL);

extern void adjust_all_pathkeys_by_agg_tlist(PlannerInfo* root, List* tlist, WindowLists* wflists);
extern void get_multiple_from_exprlist(PlannerInfo* root, List* exprList, double rows, bool* useskewmultiple,
    bool usebiasmultiple, double* skew_multiple, double* bias_multiple);
extern double get_bias_from_varlist(PlannerInfo* root, List* varlist, double rows, bool isCoalesceExpr = false);
extern ExecNodes* getDefaultPlannerExecNodes(PlannerInfo* root);
extern ExecNodes* getRelationExecNodes(Oid tableoid);
extern Bitmapset* get_base_rel_indexes(Node* jtnode);
extern Size get_hash_entry_size(int width, int numAggs = 0);

typedef enum QueryIssueType {
    /* Query not plan shipping */
    QueryShipping = 0,

    /* Large relation Broadcast */
    LargeTableBroadCast,

    /* Large relation is the inner relation when deal with Hashjoin */
    LargeTableAsHashJoinInner,

    /* Large relation in nestloop with equal join condition */
    LargeTableWithEqualCondInNestLoop,

    /* Data Skew */
    DataSkew,

    /* Not Collect Statistics */
    StatsNotCollect,

    /* Estimation rows is inaccurate */
    InaccurateEstimationRowNum,

    /* Unsuitable Scan Method */
    UnsuitableScanMethod
} QueryIssueType;

/*
 * Data structure to represent query issues
 */
typedef struct QueryPlanIssueDesc {
    QueryIssueType issue_type;
    const PlanState* issue_plannode;
    StringInfo issue_suggestion;
} QueryPlanIssueDesc;

extern List* PlanAnalyzerQuery(QueryDesc* querydesc);
extern List* PlanAnalyzerOperator(QueryDesc* querydesc, PlanState* planstate);
extern void GetPlanNodePlainText(
    Plan* plan, char** pname, char** sname, char** strategy, char** operation, char** pt_operation, char** pt_options);
extern void RecordQueryPlanIssues(const List* results);
extern bool enable_check_implicit_cast();
extern void check_gtm_free_plan(PlannedStmt *stmt, int elevel);
extern THR_LOCAL List* g_index_vars;
extern bool PreprocessOperator(Node* node, void* context);
extern void check_plan_mergeinto_replicate(PlannedStmt* stmt, int elevel);
extern void check_entry_mergeinto_replicate(Query* parse);
extern List* get_plan_list(Plan* plan);
extern RelOptInfo* build_alternative_rel(const RelOptInfo* origin, RTEKind rtekind);
extern Plan* get_foreign_scan(Plan* plan);
extern uint64 adjust_plsize(Oid relid, uint64 plan_width, uint64 pl_size, uint64* width);
extern bool check_stream_for_loop_fetch(Portal portal);
extern bool IsPlanForPartitionScan(Plan* plan);
extern bool queryIsReadOnly(Query* query);

typedef PlannedStmt* (*plannerFunc)(Query* parse, int cursorOptions, ParamListInfo boundParams);

#ifdef USE_SPQ
extern List* spq_get_distributekey_from_tlist(
    PlannerInfo* root, List* tlist, List* groupcls, double rows, double* result_multiple, void* skew_info = NULL);
#endif

#endif /* PLANNER_H */
