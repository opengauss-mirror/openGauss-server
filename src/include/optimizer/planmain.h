/* -------------------------------------------------------------------------
 *
 * planmain.h
 *	  prototypes for various files in optimizer/plan
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planmain.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLANMAIN_H
#define PLANMAIN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/streamplan.h"
#include "utils/selfuncs.h"

/* GUC parameters */
#define DEFAULT_CURSOR_TUPLE_FRACTION 0.1

typedef enum { OP_HASHJOIN, OP_HASHAGG, OP_SORT, OP_MATERIAL } OpType;

typedef enum AggOrientation {
    AGG_LEVEL_1_INTENT,   /* single level in normal case for hashagg */
    AGG_LEVEL_2_1_INTENT, /* first level in two-level hashagg */
    AGG_LEVEL_2_2_INTENT, /* second level in two-level hashagg */
    DISTINCT_INTENT,      /* distinct case, no nested agg needed */
    UNIQUE_INTENT         /* this is special for SMP to record unique agg type */
} AggOrientation;

/* row threshold to apply having filter */
#define HAVING_THRESHOLD 1e6

/* query_planner callback to compute query_pathkeys */
typedef void (*query_pathkeys_callback) (PlannerInfo *root, void *extra);

/*
 * prototypes for plan/planmain.c
 */
extern RelOptInfo* query_planner(PlannerInfo* root, List* tlist,
                    query_pathkeys_callback qp_callback, void *qp_extra);

extern bool get_number_of_groups(PlannerInfo* root, RelOptInfo* final_rel, double* num_groups, 
    List* rollup_groupclauses = NULL, List* rollup_lists = NULL);

extern void update_tuple_fraction(PlannerInfo* root, RelOptInfo* final_rel, double* numdistinct);

extern void generate_cheapest_and_sorted_path(PlannerInfo* root, RelOptInfo* final_rel, Path** cheapest_path, 
    Path** sorted_path, double* num_groups, bool has_groupby);



/*
 * prototypes for plan/planagg.c
 */
extern void preprocess_minmax_aggregates(PlannerInfo* root, List* tlist);
extern Plan* optimize_minmax_aggregates(
    PlannerInfo* root, List* tlist, const AggClauseCosts* aggcosts, Path* best_path);

/*
 * prototypes for plan/createplan.c
 */
extern void set_plan_rows(Plan* plan, double globalRows, double multiple = 1.0);
extern Plan* create_plan(PlannerInfo* root, Path* best_path);
extern void disuse_physical_tlist(Plan* plan, Path* path);
extern void copy_plan_costsize(Plan* dest, Plan* src);
extern SubqueryScan* make_subqueryscan(List* qptlist, List* qpqual, Index scanrelid, Plan* subplan);
extern ForeignScan* make_foreignscan(List* qptlist, List* qpqual, Index scanrelid, List* fdw_exprs, List* fdw_private,
    List *fdw_scan_tlist, List *fdw_recheck_quals, Plan *outer_plan, RemoteQueryExecType type = EXEC_ON_ALL_NODES);
extern Append* make_append(List* appendplans, List* tlist);
extern RecursiveUnion* make_recursive_union(
    List* tlist, Plan* lefttree, Plan* righttree, int wtParam, List* distinctList, long numGroups);
extern Sort* make_sort_from_pathkeys(
    PlannerInfo* root, Plan* lefttree, List* pathkeys, double limit_tuples, bool can_parallel = false);
extern Sort* make_sort_from_sortclauses(PlannerInfo* root, List* sortcls, Plan* lefttree);
extern Sort* make_sort_from_groupcols(PlannerInfo* root, List* groupcls, AttrNumber* grpColIdx, Plan* lefttree);
extern SortGroup* make_sort_group_from_groupcols(PlannerInfo* root, List* groupcls, AttrNumber* grpColIdx, Plan* lefttree, double dNumGroup);
extern Sort* make_sort_from_targetlist(PlannerInfo* root, Plan* lefttree, double limit_tuples);
extern Sort* make_sort(PlannerInfo* root, Plan* lefttree, int numCols, AttrNumber* sortColIdx, Oid* sortOperators,
    Oid* collations, bool* nullsFirst, double limit_tuples);
extern SortGroup* make_sortgroup(PlannerInfo* root, Plan* lefttree, int numCols, AttrNumber* sortColIdx, Oid* sortOperators,
    Oid* collations, bool* nullsFirst, double dNumGroup);
extern Agg* make_agg(PlannerInfo* root, List* tlist, List* qual, AggStrategy aggstrategy,
    const AggClauseCosts* aggcosts, int numGroupCols, AttrNumber* grpColIdx, Oid* grpOperators, Oid* grp_collations,
    long numGroups, Plan* lefttree, WindowLists* wflists, bool need_stream, bool trans_agg, List* groupingSets = NIL,
    Size hash_entry_size = 0, bool add_width = false, AggOrientation agg_orientation = AGG_LEVEL_1_INTENT,
    bool unique_check = true);
extern WindowAgg* make_windowagg(PlannerInfo* root, List* tlist, List* windowFuncs, Index winref, int partNumCols,
    AttrNumber* partColIdx, Oid* partOperators, int ordNumCols, AttrNumber* ordColIdx, Oid* ordOperators,
    int frameOptions, Node* startOffset, Node* endOffset, Plan* lefttree, Oid *part_collations, Oid *ord_collations);
extern Group* make_group(PlannerInfo* root, List* tlist, List* qual, int numGroupCols, AttrNumber* grpColIdx,
    Oid* grpOperators, double numGroups, Plan* lefttree, Oid* grp_collations);
extern ProjectSet *make_project_set(List *tlist, Plan *subplan);
extern Plan* materialize_finished_plan(Plan* subplan, bool materialize_above_stream = false, bool vectorized = false);
extern bool is_projection_capable_path(Path *path);
extern Unique* make_unique(Plan* lefttree, List* distinctList);
extern LockRows* make_lockrows(PlannerInfo* root, Plan* lefttree);
extern Limit* make_limit(PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est,
    int64 count_est, bool enable_parallel = true);
extern void pick_single_node_plan_for_replication(Plan* plan);
extern Plan* make_stream_limit(PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est,
    int64 count_est, double limit_tuples, bool needs_sort);
extern Plan* make_stream_sort(PlannerInfo* root, Plan* lefttree);
extern SetOp* make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan* lefttree, List* distinctList,
    AttrNumber flagColIdx, int firstFlag, long numGroups, double outputRows, OpMemInfo* memInfo);
#ifdef ENABLE_MULTIPLE_NODES
extern Plan* create_direct_scan(
    PlannerInfo* root, List* tlist, RangeTblEntry* realResultRTE, Index src_idx, Index scanrelid);
extern Plan* create_direct_righttree(
    PlannerInfo* root, Plan* subplan, List* distinctList, List* uniq_exprs, ExecNodes* target_exec_nodes);
extern HashJoin* create_direct_hashjoin(
    PlannerInfo* root, Plan* outerPlan, Plan* innerPlan, List* tlist, List* joinClauses, JoinType joinType);
#endif /* ENABLE_MULTIPLE_NODES */
extern BaseResult* make_result(PlannerInfo* root, List* tlist, Node* resconstantqual, Plan* subplan, List* qual = NIL);
extern Material* make_material(Plan* lefttree, bool materialize_all = false);
extern int find_node_in_targetlist(Node* node, List* targetlist);
extern Node* find_qualify_equal_class(PlannerInfo* root, Node* expr, List* targetlist);
extern List* confirm_distribute_key(PlannerInfo* root, Plan* plan, List* distribute_keys);
extern bool check_dsitribute_key_in_targetlist(PlannerInfo* root, List* distribute_keys, List* targetlist);
extern int get_plan_actual_total_width(Plan* plan, bool vectorized, OpType type, int newcol = 0);
#ifdef STREAMPLAN
extern Plan* make_modifytable(PlannerInfo* root, CmdType operation, bool canSetTag, List* resultRelations,
    List* subplans, List *withCheckOptionLists, List* returningLists, List* rowMarks, int epqParam,
    bool partKeyUpdated, Index mergeTargetRelation, List* mergeSourceTargetList, List* mergeActionList,
    UpsertExpr* upsertClause);
extern Plan* make_modifytables(PlannerInfo* root, CmdType operation, bool canSetTag, List* resultRelations,
    List* subplans, List* returningLists, List* rowMarks, int epqParam, bool partKeyUpdated,
    Index mergeTargetRelation, List* mergeSourceTargetList, List *mergeActionList, UpsertExpr *upsertClause);
extern Plan* make_redistribute_for_agg(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple,
    Distribution* distribution = NULL, bool is_local_redistribute = false);
extern Plan* make_stream_plan(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple,
    Distribution* target_distribution = NULL);
#else
extern ModifyTable* make_modifytable(CmdType operation, bool canSetTag, List* resultRelations,
    List* subplans, List *withCheckOptionLists, List* returningLists, List* rowMarks, int epqParam,
    bool partKeyUpdated, Index mergeTargetRelation, List* mergeSourceTargetList, List* mergeActionList,
    UpsertExpr* upsertClause);
extern ModifyTable* make_modifytables(CmdType operation, bool canSetTag, List* resultRelations,
    List* subplans, List* returningLists, List* rowMarks, int epqParam, bool partKeyUpdated,
    Index mergeTargetRelation, List* mergeSourceTargetList, List* mergeActionList, UpsertExpr* upsertClause);
#endif

extern bool is_projection_capable_plan(Plan* plan);
extern RowToVec* make_rowtovec(Plan* lefttree);
extern VecToRow* make_vectorow(Plan* lefttree);
extern bool isEqualExpr(Node* node);
extern bool check_subplan_in_qual(List* tlist, List* qual);
extern bool check_subplan_exec_datanode(PlannerInfo* root, Node* node);

extern void add_base_rels_to_query(PlannerInfo* root, Node* jtnode);
extern void build_base_rel_tlists(PlannerInfo* root, List* final_tlist);
extern void add_vars_to_targetlist(PlannerInfo* root, List* vars, Relids where_needed, bool create_new_ph);
extern void find_lateral_references(PlannerInfo *root);
extern void create_lateral_join_info(PlannerInfo *root);
extern void add_lateral_info(PlannerInfo *root, Index rhs, Relids lhs);
extern List* deconstruct_jointree(PlannerInfo* root, Relids* non_keypreserved = NULL);
extern void distribute_restrictinfo_to_rels(PlannerInfo* root, RestrictInfo* restrictinfo);
extern void process_security_clause_appendrel(PlannerInfo *root);
extern void process_implied_equality(PlannerInfo* root, Oid opno, Oid collation, Expr* item1, Expr* item2,
    Relids qualscope, Relids nullable_relids, Index security_level, bool below_outer_join, bool both_const);
extern void process_implied_quality(PlannerInfo* root, Node* node, Relids relids, bool below_outer_join);
extern RestrictInfo* build_implied_join_equality(
    Oid opno, Oid collation, Expr* item1, Expr* item2, Relids qualscope, Relids nullable_relids, Index security_level);

extern bool useInformationalConstraint(PlannerInfo* root, List* qualClause, Relids relids);
extern List *build_plan_tlist(PlannerInfo *root, PathTarget *pathtarget);

/*
 * prototypes for plan/analyzejoins.c
 */
extern List* remove_useless_joins(PlannerInfo* root, List* joinlist);
extern bool query_supports_distinctness(Query* query);
extern bool query_is_distinct_for(Query* query, List* colnos, List* opids);
extern bool innerrel_is_unique(PlannerInfo *root, RelOptInfo *outerrel, RelOptInfo *innerrel,
    JoinType jointype, List *restrictlist);

/*
 * prototypes for plan/setrefs.c
 */
extern Plan* set_plan_references(PlannerInfo* root, Plan* plan);
extern void fix_opfuncids(Node* node);
extern void set_opfuncid(OpExpr* opexpr);
extern void set_sa_opfuncid(ScalarArrayOpExpr* opexpr);
extern void record_plan_function_dependency(PlannerInfo* root, Oid funcid);
extern void extract_query_dependencies(
    Node* query, List** relationOids, List** invalItems, bool* hasRowSecurity, bool* hasHdfs);
extern double calc_num_groups(PlannerInfo* root, List* groupClause, double input_rows);

#ifdef PGXC
/*
 * prototypes for plan/pgxcplan.c
 */
extern Plan* create_remotedml_plan(PlannerInfo* root, Plan* topplan, CmdType cmdtyp);
extern Plan* create_remote_mergeinto_plan(PlannerInfo* root, Plan* topplan, CmdType cmdtyp, MergeAction* action);
extern Plan* create_remotegrouping_plan(PlannerInfo* root, Plan* local_plan);
extern Plan* create_remotequery_plan(PlannerInfo* root, RemoteQueryPath* best_path);
extern Plan* create_remotesort_plan(PlannerInfo* root, Plan* local_plan, List* pathkeys = NIL);
extern Plan* create_remotelimit_plan(PlannerInfo* root, Plan* local_plan);
extern List* pgxc_order_qual_clauses(PlannerInfo* root, List* clauses);
extern List* pgxc_build_relation_tlist(RelOptInfo* rel);
extern void pgxc_copy_path_costsize(Plan* dest, Path* src);
extern Plan* pgxc_create_gating_plan(PlannerInfo* root, Plan* plan, List* quals);
#endif
extern void expand_dfs_tables(PlannerInfo* root);
extern void expand_internal_rtentry(PlannerInfo* root, RangeTblEntry* rte, Index rti);
extern List* find_all_internal_tableOids(Oid parentOid);
extern bool check_agg_optimizable(Aggref* aggref, int16* strategy);
extern void check_hashjoinable(RestrictInfo* restrictinfo);

#ifdef USE_SPQ
extern void spq_extract_plan_dependencies(PlannerInfo *root, Plan *plan);
extern List* spq_make_null_eq_clause(List* joinqual, List** otherqual, List* nullinfo);
#endif

#endif /* PLANMAIN_H */
