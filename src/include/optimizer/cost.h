/* -------------------------------------------------------------------------
 *
 * cost.h
 *	  prototypes for costsize.c and clausesel.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/cost.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef COST_H
#define COST_H

#include "executor/node/nodeHash.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/planmain.h"
#include "optimizer/aioptimizer.h"
#include "utils/aset.h"
#include "utils/extended_statistics.h"

#define STREAM_COST_THRESHOLD 100000.0

/*
 * For columnar scan, the cpu cost is less than seq scan, so
 * we are definition that the cost of scanning one tuple is 1/10 times.
 */
#define COL_TUPLE_COST_MULTIPLIER 10

/*
 * Estimate the overhead per hashtable entry at 64 bytes (same as in
 * planner.c).
 */
#define HASH_ENTRY_OVERHEAD 64

/* defaults for costsize.c's Cost parameters */
/* NB: cost-estimation code should use the variables, not these constants! */
/* If you change these, update backend/utils/misc/postgresql.sample.conf */
#define DEFAULT_SEQ_PAGE_COST 1.0
#define DEFAULT_RANDOM_PAGE_COST 4.0
#define DEFAULT_CPU_TUPLE_COST 0.01
#define DEFAULT_CPU_INDEX_TUPLE_COST 0.005
#define DEFAULT_CPU_OPERATOR_COST 0.0025
#define DEFAULT_CPU_HASH_COST 0.02
#define DEFAULT_SEND_KDATA_COST 2.0
#define DEFAULT_RECEIVE_KDATA_COST 2.0
#define DEFAULT_ALLOCATE_MEM_COST 0.0
#define LOCAL_SEND_KDATA_COST 1.3    /* The send cost for local stream */
#define LOCAL_RECEIVE_KDATA_COST 1.3 /* The receive cost for local stream */
#define DEFAULT_SMP_THREAD_COST 1000 /* The cost for add a new thread */
#define DEFAULT_STREAM_MULTIPLE 1.0

#define DEFAULT_EFFECTIVE_CACHE_SIZE 16384 /* measured in pages */

#define COST_ALTERNATIVE_NEQ 0x00000001
#define COST_ALTERNATIVE_CONJUNCT 0x00000002
#define COST_ALTERNATIVE_MERGESORT 0x00000004           /* disable mergesort option */
#define COST_ALTERNATIVE_EQUALRANGE_NOTINMCV 0x00000008 /* filter has equal range but the const not in mcv */

#define SIZE_COL_VALUE 16

#define TUPLE_OVERHEAD(colorsort) ((colorsort) ? 24 : 8)

const int64 max_unknown_offset = 10000;

typedef enum {
    CONSTRAINT_EXCLUSION_OFF,      /* do not use c_e */
    CONSTRAINT_EXCLUSION_ON,       /* apply c_e to all rels */
    CONSTRAINT_EXCLUSION_PARTITION /* apply c_e to otherrels only */
} ConstraintExclusionType;

extern void init_plan_cost(Plan* plan);
extern void cost_insert(Path* path, bool vectorized, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int modify_mem, int dop, Oid resultRelOid, OpMemInfo* mem_info);
extern void cost_delete(Path* path, bool vectorized, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int modify_mem, int dop, Oid resultRelOid, OpMemInfo* mem_info);
extern void cost_update(Path* path, bool vectorized, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int modify_mem, int dop, Oid resultRelOid, OpMemInfo* mem_info);

extern double clamp_row_est(double nrows);
extern double index_pages_fetched(
    double tuples_fetched, BlockNumber pages, double index_pages, PlannerInfo* root, bool ispartitionedindex);
extern void cost_seqscan(Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info);
extern void cost_resultscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_samplescan(Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info);
extern void cost_cstorescan(Path* path, PlannerInfo* root, RelOptInfo* baserel);
#ifdef ENABLE_MULTIPLE_NODES
extern void cost_tsstorescan(Path *path, PlannerInfo *root, RelOptInfo *baserel);
#endif   /* ENABLE_MULTIPLE_NODES */
extern void cost_index(IndexPath* path, PlannerInfo* root, double loop_count);
extern void cost_bitmap_heap_scan(
    Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info, Path* bitmapqual, double loop_count);
extern void cost_bitmap_and_node(BitmapAndPath* path, PlannerInfo* root);
extern void cost_bitmap_or_node(BitmapOrPath* path, PlannerInfo* root);
extern void cost_bitmap_tree_node(Path* path, Cost* cost, Selectivity* selec);
extern void cost_tidscan(Path* path, PlannerInfo* root, RelOptInfo* baserel, List* tidquals);
extern void cost_subqueryscan(Path* path, PlannerInfo* root, RelOptInfo* baserel, ParamPathInfo* param_info);
extern void cost_functionscan(Path* path, PlannerInfo* root, RelOptInfo* baserel);
extern void cost_valuesscan(Path* path, PlannerInfo* root, RelOptInfo* baserels);
#ifdef PGXC
extern void cost_remotequery(RemoteQueryPath* rqpath, PlannerInfo* root, RelOptInfo* rel);
#endif
extern void cost_ctescan(Path* path, PlannerInfo* root, RelOptInfo* baserel);
extern void cost_recursive_union(Plan* runion, Plan* nrterm, Plan* rterm);
extern void cost_sort(Path* path, List* pathkeys, Cost input_cost, double tuples, int width, Cost comparison_cost,
    int sort_mem, double limit_tuples, bool col_store, int dop = 1, OpMemInfo* mem_info = NULL,
    bool index_sort = false);
extern void cost_sort_group(Path *path, PlannerInfo *root, Cost input_cost, double tuples, int width,
                            Cost comparison_cost, int sort_mem, double dNumGroups);
extern void cost_merge_append(Path* path, PlannerInfo* root, List* pathkeys, int n_streams, Cost input_startup_cost,
    Cost input_total_cost, double tuples);
extern void cost_material(Path* path, Cost input_startup_cost, Cost input_total_cost, double tuples, int width);
extern void cost_agg(Path* path, PlannerInfo* root, AggStrategy aggstrategy, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, Cost input_startup_cost, Cost input_total_cost, double input_tuples,
    int input_width = 0, int hash_entry_size = 0, int dop = 1, OpMemInfo* mem_info = NULL);
extern void cost_windowagg(Path* path, PlannerInfo* root, List* windowFuncs, int numPartCols, int numOrderCols,
    Cost input_startup_cost, Cost input_total_cost, double input_tuples);
extern void cost_group(Path* path, PlannerInfo* root, int numGroupCols, double numGroups, Cost input_startup_cost,
    Cost input_total_cost, double input_tuples);
extern void cost_limit(Plan* plan, Plan* lefttree, int64 offset_est, int64 count_est);
extern void initial_cost_nestloop(PlannerInfo* root, JoinCostWorkspace* workspace, JoinType jointype, Path* outer_path,
    Path* inner_path, JoinPathExtraData *extra, int dop);
extern void final_cost_nestloop(PlannerInfo* root, NestPath* path, JoinCostWorkspace* workspace,
    JoinPathExtraData *extra, bool hasalternative, int dop);
extern void initial_cost_mergejoin(PlannerInfo* root, JoinCostWorkspace* workspace, JoinType jointype,
    List* mergeclauses, Path* outer_path, Path* inner_path, List* outersortkeys, List* innersortkeys,
    JoinPathExtraData *extra);
extern void final_cost_mergejoin(
    PlannerInfo* root, MergePath* path, JoinCostWorkspace* workspace, JoinPathExtraData *extra, bool hasalternative);
extern void initial_cost_hashjoin(PlannerInfo* root, JoinCostWorkspace* workspace, JoinType jointype, List* hashclauses,
    Path* outer_path, Path* inner_path, JoinPathExtraData *extra, int dop);
extern void final_cost_hashjoin(PlannerInfo* root, HashPath* path, JoinCostWorkspace* workspace,
    JoinPathExtraData *extra, bool hasalternative, int dop);
extern void cost_rescan(PlannerInfo* root, Path* path, Cost* rescan_startup_cost, /* output parameters */
    Cost* rescan_total_cost, OpMemInfo* mem_info);
extern Cost cost_rescan_material(double rows, int width, OpMemInfo* mem_info, bool vectorized, int dop);
extern void cost_subplan(PlannerInfo* root, SubPlan* subplan, Plan* plan);
extern void cost_qual_eval(QualCost* cost, List* quals, PlannerInfo* root);
extern void cost_qual_eval_node(QualCost* cost, Node* qual, PlannerInfo* root);
extern void compute_semi_anti_join_factors(PlannerInfo* root, RelOptInfo* outerrel, RelOptInfo* innerrel,
    JoinType jointype, SpecialJoinInfo* sjinfo, List* restrictlist, SemiAntiJoinFactors* semifactors);
extern void set_baserel_size_estimates(PlannerInfo* root, RelOptInfo* rel);
extern double get_parameterized_baserel_size(PlannerInfo* root, RelOptInfo* rel, List* param_clauses);
extern double get_parameterized_joinrel_size(PlannerInfo* root, RelOptInfo* rel, double outer_rows, double inner_rows,
    SpecialJoinInfo* sjinfo, List* restrict_clauses);
extern void set_joinrel_size_estimates(PlannerInfo* root, RelOptInfo* rel, RelOptInfo* outer_rel, RelOptInfo* inner_rel,
    SpecialJoinInfo* sjinfo, List* restrictlist);
extern void set_joinpath_multiple_for_EC(PlannerInfo* root, Path* path, Path* outer_path, Path* inner_path);
extern void set_subquery_size_estimates(PlannerInfo* root, RelOptInfo* rel);
extern void set_function_size_estimates(PlannerInfo* root, RelOptInfo* rel);
extern void set_values_size_estimates(PlannerInfo* root, RelOptInfo* rel);
extern void set_cte_size_estimates(PlannerInfo* root, RelOptInfo* rel, Plan* cteplan);
extern void set_foreign_size_estimates(PlannerInfo* root, RelOptInfo* rel);
extern void set_result_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern double adjust_limit_row_count(double lefttree_rows);
extern void estimate_limit_offset_count(PlannerInfo* root, int64* offset_est, int64* count_est,
    RelOptInfo* final_rel = NULL, bool* fix_param = NULL);
extern PathTarget *set_pathtarget_cost_width(PlannerInfo *root, PathTarget *target);

/*
 * prototypes for clausesel.c
 *	  routines to compute clause selectivities
 */
extern Selectivity clauselist_selectivity(PlannerInfo* root, List* clauses, int varRelid, JoinType jointype,
    SpecialJoinInfo* sjinfo, bool varratio_cached = true, bool use_poisson = true);
extern Selectivity clause_selectivity(PlannerInfo* root, Node* clause, int varRelid, JoinType jointype,
    SpecialJoinInfo* sjinfo, bool varratio_cached = true, bool check_scalarop = false, bool use_poisson = true);

extern void set_rel_width(PlannerInfo* root, RelOptInfo* rel);
extern void restore_hashjoin_cost(Path* path);
extern void finalize_dml_cost(ModifyTable* plan);
extern void copy_mem_info(OpMemInfo* dest, OpMemInfo* src);
extern int columnar_get_col_width(int typid, int width, bool aligned = false);
extern int get_path_actual_total_width(Path* path, bool vectorized, OpType type, int newcol = 0);

extern void bernoulli_samplescangetsamplesize(PlannerInfo* root, RelOptInfo* baserel, List* paramexprs);
extern void system_samplescangetsamplesize(PlannerInfo* root, RelOptInfo* baserel, List* paramexprs);
extern void hybrid_samplescangetsamplesize(PlannerInfo* root, RelOptInfo* baserel, List* paramexprs);
extern double cost_page_size(double tuples, int width);
extern bool can_use_possion(VariableStatData* vardata, SpecialJoinInfo* sjinfo, double* ratio);
extern void set_equal_varratio(VariableStatData* vardata, Relids other_relids, double ratio, SpecialJoinInfo* sjinfo);
extern double estimate_hash_num_distinct(PlannerInfo* root, List* hashkey, Path* inner_path, VariableStatData* vardata,
    double local_ndistinct, double global_ndistinct, bool* usesinglestats);
extern RelOptInfo* find_join_input_rel(PlannerInfo* root, Relids relids);
extern double compute_sort_disk_cost(double input_bytes, double sort_mem_bytes);

extern double approx_tuple_count(PlannerInfo* root, JoinPath* path, List* quals);
extern void set_rel_path_rows(Path* path, RelOptInfo* rel, ParamPathInfo* param_info);
extern Selectivity compute_bucket_size(PlannerInfo* root, RestrictInfo* restrictinfo, double virtualbuckets,
    Path* inner_path, bool left, SpecialJoinInfo* sjinfo, double* ndistinct);
extern double page_size(double tuples, int width);
extern bool has_complicate_hashkey(List* hashclauses, Relids inner_relids);
extern bool has_indexed_join_quals(NestPath* joinpath);
extern double relation_byte_size(
    double tuples, int width, bool vectorized, bool aligned = true, bool issort = true, bool indexsort = false);
extern MergeScanSelCache* cached_scansel(PlannerInfo* root, RestrictInfo* rinfo, PathKey* pathkey);

extern double apply_random_page_cost_mod(double rand_page_cost, double seq_page_cost, double num_of_page);

#define ES_DEBUG_LEVEL DEBUG2 /* debug level for extended statistic in optimizor */

#define RANDOM_PAGE_COST(use_mod, rand_cost, seq_cost, pages) (use_mod ? \
            apply_random_page_cost_mod(rand_cost, seq_cost, pages) : rand_cost)

/* Logistic function */
#define LOGISTIC_FUNC(variable, threshold, max_func_value, min_func_value, slope_factor) \
            ((variable > threshold && variable > 0) ? max_func_value : \
            2 * (max_func_value - min_func_value) / (1 + exp(-1 * slope_factor * variable)) \
            - (max_func_value - 2 * min_func_value))

enum es_type { ES_EMPTY = 0, ES_EQSEL = 1, ES_EQJOINSEL, ES_GROUPBY, ES_COMPUTEBUCKETSIZE };

struct es_bucketsize {
    Bitmapset* left_relids;
    Bitmapset* right_relids;
    RelOptInfo* left_rel;
    RelOptInfo* right_rel;
    float4 left_distinct;
    float4 right_distinct;
    float4 left_dndistinct;
    float4 right_dndistinct;
    double left_mcvfreq;
    double right_mcvfreq;
    List* left_hashkeys;
    List* right_hashkeys;
};

struct es_candidate {
    es_type tag;
    Bitmapset* relids;
    Bitmapset* left_relids; /* for eqsel and groupby clause, only use left_XXX */
    Bitmapset* right_relids;
    Bitmapset* left_attnums;
    Bitmapset* right_attnums;
    float4 left_stadistinct; /* will be transfer to absolute value */
    float4 right_stadistinct;
    double left_first_mcvfreq; /* frequency of fist mcv, to calculate skew or bucket size */
    double right_first_mcvfreq;
    RelOptInfo* left_rel;
    RelOptInfo* right_rel;
    RangeTblEntry* left_rte;
    RangeTblEntry* right_rte;
    List* clause_group;
    List* clause_map;
    ExtendedStats* left_extended_stats;
    ExtendedStats* right_extended_stats;   /* read from pg_statistic, where distinct value
                                            * could be negative. (-1 means unique.)
                                            */
    List* pseudo_clause_list;              /* save the clause build by equivalence class and
                                            * the original clause.
                                            */
    bool has_null_clause;                  /* if "is null" used in the clauses */
    bool extended_stats_only_ai = false;
};

/*
 * calculate selectivity using extended statistics
 */
class ES_SELECTIVITY : public BaseObject {
public:
    List* es_candidate_list;
    List* es_candidate_saved; /* temporarily save es_candidate_list */
    List* unmatched_clause_group;
    PlannerInfo* root;       /* root from input */
    SpecialJoinInfo* sjinfo; /* sjinfo from input */
    List* origin_clauses;    /* clauselist from input */
    JoinPath* path;          /* path from input */
    List* bucketsize_list;   /* list of bucket size */
    List* statlist;          /* list of ExtendedStats extracted from pg_statistic_ext */

    /* a simplified structure from clause */
    struct es_clause_map {
        int left_attnum;
        int right_attnum;
        Var* left_var;
        Var* right_var;
    };

    ES_SELECTIVITY();
    virtual ~ES_SELECTIVITY();
    Selectivity calculate_selectivity(PlannerInfo* root_input, List* clauses_input, SpecialJoinInfo* sjinfo_input,
        JoinType jointype, JoinPath* path_input, es_type action, STATS_EST_TYPE eType = STATS_TYPE_GLOBAL);
    void clear();
    void print_clauses(List* clauselist) const;
    Selectivity estimate_hash_bucketsize(
        es_bucketsize* es_bucket, double* distinctnum, bool left, Path* inner_path, double nbuckets);

private:
    bool add_attnum(RestrictInfo* clause, es_candidate* temp) const;
    void add_attnum_for_eqsel(es_candidate* temp, int attnum, Node* arg) const;
    void add_clause_map(es_candidate* es, int left_attnum, int right_attnum, Node* left_arg, Node* right_arg) const;
    bool build_es_candidate(RestrictInfo* clause, es_type type);
    bool build_es_candidate_for_eqsel(es_candidate* es, Node* var, int attnum, bool left, RestrictInfo* clause);
    void build_pseudo_varinfo(es_candidate* es, STATS_EST_TYPE eType);
    void cal_bucket_size(es_candidate* es, es_bucketsize* bucket) const;
    Selectivity cal_eqsel(es_candidate* es);
    Selectivity cal_eqjoinsel(es_candidate* es, JoinType jointype);
    Selectivity cal_eqjoinsel_inner(es_candidate* es);
    Selectivity cal_eqjoinsel_semi(es_candidate* es, RelOptInfo* inner_rel, bool inner_on_left);
    void cal_stadistinct_eqsel(es_candidate* es);
    bool cal_stadistinct_eqjoinsel(es_candidate* es);
    void CalSelWithUniqueIndex(Selectivity &result);
    void clear_extended_stats(ExtendedStats* extended_stats) const;
    void clear_extended_stats_list(List* stats_list) const;
    bool ContainIndexCols(const es_candidate* es, const IndexOptInfo* index) const;
    ExtendedStats* copy_stats_ptr(ListCell* l) const;
    void debug_print();
    double estimate_local_numdistinct(es_bucketsize* bucket, bool left, Path* path);
    void group_clauselist(List* clauses);
    void group_clauselist_groupby(List* varinfos);
    void init_candidate(es_candidate* es) const;
    bool IsEsCandidateInEqClass(es_candidate *es, EquivalenceClass *ec);
    void load_eqsel_clause(RestrictInfo* clause);
    void load_eqjoinsel_clause(RestrictInfo* clause);
    Bitmapset* make_attnums_by_clause_map(es_candidate* es, Bitmapset* attnums, bool left) const;
    void match_extended_stats(es_candidate* es, List* stats_list, bool left);
    bool match_pseudo_clauselist(List* clauses, es_candidate* es, List* origin_clause);
    bool MatchUniqueIndex(const es_candidate* es) const;
    void modify_distinct_by_possion_model(es_candidate* es, bool left, SpecialJoinInfo* sjinfo) const;
    char* print_expr(const Node* expr, const List* rtable) const;
    void print_rel(RangeTblEntry* rel) const;
    void print_relids(Bitmapset* relids, const char* str) const;
    int read_attnum(Node* node) const;
    void read_rel_rte(Node* node, RelOptInfo** rel, RangeTblEntry** rte);
    void read_statistic();
    void read_statistic_eqjoinsel(es_candidate* es);
    void read_statistic_eqsel(es_candidate* es);
    void recheck_candidate_list();
    void remove_candidate(es_candidate* es);
    void remove_attnum(es_candidate* es, int dump_attnum);
    void remove_members_without_es_stats(int max_matched, int num_members, es_candidate* es);
    void replace_clause(Datum* old_clause, Datum* new_clause) const;
    void report_no_stats(Oid relid_oid, Bitmapset* attnums) const;
    void save_selectivity(
        es_candidate* es, double left_join_ratio, double right_join_ratio, bool save_semi_join = false);
    void set_up_attnum_order(es_candidate* es, int* attnum_order, bool left) const;
    bool try_equivalence_class(es_candidate* es);
    void setup_es(es_candidate* es, es_type type, RestrictInfo* clause);

/* AI optimizer
 */
    Selectivity cal_eqsel_ai(es_candidate* es);
	Selectivity cal_eqjoinsel_inner_ai(es_candidate* es);
    void set_up_attnum_order_reversed(es_candidate* es, int* attnum_order) const;
};

#endif /* COST_H */
