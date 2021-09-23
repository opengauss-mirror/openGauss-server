/* -------------------------------------------------------------------------
 *
 * pathnode.h
 *	  prototypes for pathnode.c, relnode.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/pathnode.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PATHNODE_H
#define PATHNODE_H

#include "nodes/relation.h"
#include "optimizer/streamplan.h"

/* global path */
#define MAX_PATH_NUM 10 /* the maximum path number of super set distribute key */

/* To keep the plan stability, we should compare path costs with fuzzy factor */
#define FUZZY_FACTOR 1.01
#define SMALL_FUZZY_FACTOR 1.0000000001

typedef enum {
    COSTS_EQUAL,    /* path costs are fuzzily equal */
    COSTS_BETTER1,  /* first path is cheaper than second */
    COSTS_BETTER2,  /* second path is cheaper than first */
    COSTS_DIFFERENT /* neither path dominates the other on cost */
} PathCostComparison;

typedef enum SJoinUniqueMethod {
    REDISTRIBUTE_UNIQUE,
    UNIQUE_REDISTRIBUTE,
    UNIQUE_REDISTRIBUTE_UNIQUE,
    REDISTRIBUTE_UNIQUE_REDISTRIBUTE_UNIQUE
} SJoinUniqueMethod;

/*
 * prototypes for pathnode.c
 */
extern int compare_path_costs(Path* path1, Path* path2, CostSelector criterion);
extern int compare_fractional_path_costs(Path* path1, Path* path2, double fraction);
extern void set_cheapest(RelOptInfo* parent_rel, PlannerInfo* root = NULL);
extern Path* get_cheapest_path(PlannerInfo* root, RelOptInfo* rel, const double* agg_groups, bool has_groupby);
extern Path* find_hinted_path(Path* current_path);
extern void add_path(PlannerInfo* root, RelOptInfo* parent_rel, Path* new_path);
extern bool add_path_precheck(
    RelOptInfo* parent_rel, Cost startup_cost, Cost total_cost, List* pathkeys, Relids required_outer);

extern Path* create_seqscan_path(PlannerInfo* root, RelOptInfo* rel, Relids required_outer, int dop = 1);
extern Path *create_resultscan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer);
extern Path* create_cstorescan_path(PlannerInfo* root, RelOptInfo* rel, int dop = 1);
#ifdef ENABLE_MULTIPLE_NODES
extern Path *create_tsstorescan_path(PlannerInfo * root,RelOptInfo * rel, int dop = 1);
#endif   /* ENABLE_MULTIPLE_NODES */
extern IndexPath* create_index_path(PlannerInfo* root, IndexOptInfo* index, List* indexclauses, List* indexclausecols,
    List* indexorderbys, List* indexorderbycols, List* pathkeys, ScanDirection indexscandir, bool indexonly,
    Relids required_outer, Bitmapset *upper_params, double loop_count);
extern Path* build_seqScanPath_by_indexScanPath(PlannerInfo* root, Path* index_path);
extern bool CheckBitmapQualIsGlobalIndex(Path* bitmapqual);
extern bool CheckBitmapHeapPathContainGlobalOrLocal(Path* bitmapqual);
extern bool CheckBitmapHeapPathIsCrossbucket(Path* bitmapqual);
extern bool check_bitmap_heap_path_index_unusable(Path* bitmapqual, RelOptInfo* baserel);
extern bool is_partitionIndex_Subpath(Path* subpath);
extern bool is_pwj_path(Path* pwjpath);
extern BitmapHeapPath* create_bitmap_heap_path(PlannerInfo* root, RelOptInfo* rel, Path* bitmapqual,
            Relids required_outer, Bitmapset* required_upper, double loop_count);
extern BitmapAndPath* create_bitmap_and_path(PlannerInfo* root, RelOptInfo* rel, List* bitmapquals);
extern BitmapOrPath* create_bitmap_or_path(PlannerInfo* root, RelOptInfo* rel, List* bitmapquals);
extern TidPath* create_tidscan_path(PlannerInfo* root, RelOptInfo* rel, List* tidquals);
extern AppendPath* create_append_path(PlannerInfo* root, RelOptInfo* rel, List* subpaths, Relids required_outer);
extern MergeAppendPath* create_merge_append_path(
    PlannerInfo* root, RelOptInfo* rel, List* subpaths, List* pathkeys, Relids required_outer);
extern ResultPath* create_result_path(PlannerInfo *root, RelOptInfo *rel, List* quals, Path* subpath = NULL, Bitmapset *upper_params = NULL);
extern MaterialPath* create_material_path(Path* subpath, bool materialize_all = false);
extern UniquePath* create_unique_path(PlannerInfo* root, RelOptInfo* rel, Path* subpath, SpecialJoinInfo* sjinfo);
extern Path* create_subqueryscan_path(PlannerInfo* root, RelOptInfo* rel, List* pathkeys, Relids required_outer, List *subplan_params);
extern Path* create_subqueryscan_path_reparam(PlannerInfo* root, RelOptInfo* rel, List* pathkeys, Relids required_outer, List *subplan_params);
extern Path* create_functionscan_path(PlannerInfo* root, RelOptInfo* rel, Relids required_outer);
extern Path* create_valuesscan_path(PlannerInfo* root, RelOptInfo* rel, Relids required_outer);
extern Path* create_ctescan_path(PlannerInfo* root, RelOptInfo* rel);
extern Path* create_worktablescan_path(PlannerInfo* root, RelOptInfo* rel);
extern ForeignPath* create_foreignscan_path(PlannerInfo* root, RelOptInfo* rel, Cost startup_cost, Cost total_cost,
    List* pathkeys, Relids required_outer, List* fdw_private, int dop = 1);
extern Relids calc_nestloop_required_outer(Path* outer_path, Path* inner_path);
extern Relids calc_non_nestloop_required_outer(Path* outer_path, Path* inner_path);

extern NestPath* create_nestloop_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path,
    Path* inner_path, List* restrict_clauses, List* pathkeys, Relids required_outer, int dop = 1);

extern MergePath* create_mergejoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, Path* outer_path, Path* inner_path, List* restrict_clauses,
    List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys, List* innersortkeys);

extern HashPath* create_hashjoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path,
    Path* inner_path, List* restrict_clauses, Relids required_outer, List* hashclauses, int dop = 1);

extern Path* reparameterize_path(PlannerInfo* root, Path* path, Relids required_outer, double loop_count);

/*
 * prototypes for relnode.c
 */
extern void setup_simple_rel_arrays(PlannerInfo* root);
extern RelOptInfo* build_simple_rel(PlannerInfo* root, int relid, RelOptKind reloptkind);
extern RelOptInfo* find_base_rel(PlannerInfo* root, int relid);
extern RelOptInfo* find_join_rel(PlannerInfo* root, Relids relids);
extern void remove_join_rel(PlannerInfo *root, RelOptInfo *rel);
extern RelOptInfo* build_join_rel(PlannerInfo* root, Relids joinrelids, RelOptInfo* outer_rel, RelOptInfo* inner_rel,
    SpecialJoinInfo* sjinfo, List** restrictlist_ptr);
extern AppendRelInfo* find_childrel_appendrelinfo(PlannerInfo* root, RelOptInfo* rel);
extern ParamPathInfo* get_baserel_parampathinfo(PlannerInfo* root, RelOptInfo* baserel, Relids required_outer, Bitmapset *upper_params = NULL);
extern ParamPathInfo* get_subquery_parampathinfo(PlannerInfo* root, RelOptInfo* baserel, Relids required_outer, Bitmapset *upper_params = NULL);
extern ParamPathInfo* get_joinrel_parampathinfo(PlannerInfo* root, RelOptInfo* joinrel, Path* outer_path,
    Path* inner_path, SpecialJoinInfo* sjinfo, Relids required_outer, List** restrict_clauses);
extern ParamPathInfo* get_appendrel_parampathinfo(RelOptInfo* appendrel, Relids required_outer, Bitmapset* upper_params);
extern void get_distribute_keys(PlannerInfo* root, List* joinclauses, Path* outer_path, Path* inner_path,
    double* skew_outer, double* skew_inner, List** distribute_keys_outer, List** distribute_keys_inner,
    List* desired_keys, bool exact_match);
extern bool is_distribute_need_on_joinclauses(PlannerInfo* root, List* side_distkeys, List* joinclauses,
    const RelOptInfo* side_rel, const RelOptInfo* other_rel, List** rrinfo);

extern void add_hashjoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path,
    Path* inner_path, List* restrictlist, Relids required_outer, List* hashclauses, Distribution* target_distribution);

extern void add_nestloop_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path,
    Path* inner_path, List* restrict_clauses, List* pathkeys, Relids required_outer, Distribution* target_distribution);

extern void add_mergejoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, Path* outer_path, Path* inner_path, List* restrict_clauses,
    List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys, List* innersortkeys,
    Distribution* target_distribution);
extern bool equal_distributekey(PlannerInfo* root, List* distribute_key1, List* distribute_key2);
extern bool judge_node_compatible(PlannerInfo* root, Node* n1, Node* n2);
extern List* build_superset_keys_for_rel(
    PlannerInfo* root, RelOptInfo* rel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype);
extern List* remove_duplicate_superset_keys(List* join_dis_key_list);
extern List* generate_join_implied_equalities_normal(
    PlannerInfo* root, EquivalenceClass* ec, Relids join_relids, Relids outer_relids, Relids inner_relids);

#ifdef STREAMPLAN
/*
 * Determine which distributekey can be treated as the joinpath's distributekey
 * 1. outer path's distributekey can be treated as the joinpath's distributekey if LHS_join returns true
 * 2. inner path's distributekey can be treated as the joinpath's distributekey if RHS_join returns true
 */
#define LHS_join(jointype)                                                                                \
    (JOIN_INNER == jointype || JOIN_LEFT == jointype || JOIN_SEMI == jointype || JOIN_ANTI == jointype || \
        JOIN_LEFT_ANTI_FULL == jointype)
#define RHS_join(jointype)                                                                                             \
    (JOIN_INNER == jointype || JOIN_RIGHT_ANTI == jointype || JOIN_RIGHT == jointype || JOIN_RIGHT_SEMI == jointype || \
        JOIN_RIGHT_ANTI_FULL == jointype)

/*
 * Check if we can broadcast side path
 * 1. inner path can be broadcast if can_broadcast_inner returns true
 * 2. outer path can be broadcast if can_broadcast_outer returns true
 */
#define can_broadcast_inner(jointype, save_jointype, replicate_outer, diskeys_outer, path_outer) \
    (LHS_join(jointype) && (save_jointype != JOIN_UNIQUE_OUTER || replicate_outer))
#define can_broadcast_outer(jointype, save_jointype, replicate_inner, diskeys_inner, path_inner) \
    (RHS_join(jointype) && (save_jointype != JOIN_UNIQUE_INNER || replicate_inner))

extern bool find_ec_memeber_for_var(EquivalenceClass* ec, Node* key);
extern double get_skew_ratio(double distinct_value);
extern Path* stream_side_path(PlannerInfo* root, Path* path, JoinType jointype, bool is_replicate,
    StreamType stream_type, List* distribute_key, List* pathkeys, bool is_inner, double skew,
    Distribution* target_distribution, ParallelDesc* smpDesc = NULL);
extern double get_node_mcf(PlannerInfo* root, Node* v, double rows);
extern bool needs_agg_stream(PlannerInfo* root, List* tlist, List* distribute_targetlist, Distribution* distribution = NULL);
extern bool is_replicated_path(Path* path);
extern void adjust_rows_according_to_hint(HintState* hstate, RelOptInfo* rel, Relids subrelids = NULL);

extern bool is_subplan_exec_on_coordinator(Path* path);
extern void set_hint_value(RelOptInfo* join_rel, Path* new_path, HintState* hstate);
extern void debug1_print_new_path(PlannerInfo* root, Path* path, bool small_fuzzy_factor_is_used);
extern void debug1_print_compare_result(PathCostComparison costcmp, PathKeysComparison keyscmp, BMS_Comparison outercmp,
    double rowscmp, PlannerInfo* root, Path* path, bool small_fuzzy_factor_is_used);
extern PathCostComparison compare_path_costs_fuzzily(Path* path1, Path* path2, double fuzz_factor);
extern Node* get_distribute_node(PlannerInfo* root, RestrictInfo* rinfo, RelOptInfo* parent_rel, bool local_left,
    double* skew_multiple, List* desired_keys, Node** exact_match_keys);
extern bool is_exact_match_keys_full(Node** match_keys, int length);
extern List* locate_distribute_key(JoinType jointype, List* outer_distributekey, List* inner_distributekey,
    List* desired_key = NIL, bool exact_match = false);
extern SJoinUniqueMethod get_optimal_join_unique_path(PlannerInfo* root, Path* path, StreamType stream_type,
    List* distribute_key, List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc);
extern Node* join_clause_get_join_key(Node* join_clause, bool is_var_on_left);

extern Path* get_redist_unique(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key,
    List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc, bool cost_only = false);

extern Path* get_unique_redist(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key,
    List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc);

extern Path* get_unique_redist_unique(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key,
    List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc, bool cost_only = false);

extern Path* get_redist_unique_redist_unique(PlannerInfo* root, Path* path, StreamType stream_type,
    List* distribute_key, List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc,
    bool cost_only = false);

extern bool equivalence_class_overlap(PlannerInfo* root, Relids outer_relids, Relids inner_relids);

extern void debug3_print_two_relids(Relids first_relids, Relids second_relids, PlannerInfo* root, StringInfoData* buf);

extern RemoteQueryExecType SetExectypeForJoinPath(Path* inner_path, Path* outer_path);
extern bool CheckJoinExecType(PlannerInfo *root, Path *outer_path, Path *inner_path);
extern bool IsSameJoinExecType(PlannerInfo *root, Path *outer_path, Path *inner_path);

extern bool is_diskey_and_joinkey_compatible(Node* diskey, Node* joinkey);

#endif
#endif /* PATHNODE_H */
