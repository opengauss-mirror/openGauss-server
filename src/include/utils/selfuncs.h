/* -------------------------------------------------------------------------
 *
 * selfuncs.h
 *	  Selectivity functions and index cost estimation functions for
 *	  standard operators and index access methods.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/selfuncs.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SELFUNCS_H
#define SELFUNCS_H

#include "fmgr.h"
#include "access/htup.h"
#include "nodes/relation.h"
#include "optimizer/nodegroups.h"

/*
 * Note: the default selectivity estimates are not chosen entirely at random.
 * We want them to be small enough to ensure that indexscans will be used if
 * available, for typical table densities of ~100 tuples/page.	Thus, for
 * example, 0.01 is not quite small enough, since that makes it appear that
 * nearly all pages will be hit anyway.  Also, since we sometimes estimate
 * eqsel as 1/num_distinct, we probably want DEFAULT_NUM_DISTINCT to equal
 * 1/DEFAULT_EQ_SEL.
 */

/* default selectivity estimate for equalities such as "A = b" */
#define DEFAULT_EQ_SEL 0.005

/* default selectivity estimate for inequalities such as "A < b" */
#define DEFAULT_INEQ_SEL 0.3333333333333333

/* default selectivity estimate for range inequalities "A > b AND A < c" */
#define DEFAULT_RANGE_INEQ_SEL 0.005

/* default selectivity estimate for pattern-match operators such as LIKE */
#define DEFAULT_MATCH_SEL 0.005

/* default number of distinct values in a table */
#define DEFAULT_NUM_DISTINCT 200

/* default number of rows */
#define DEFAULT_NUM_ROWS 10

/* default number of distinct values and biase for the special expression */
#define DEFAULT_SPECIAL_EXPR_DISTINCT 10
#define DEFAULT_SPECIAL_EXPR_BIASE (pow(u_sess->pgxc_cxt.NumDataNodes, (double)1 / 2) / u_sess->pgxc_cxt.NumDataNodes)

/* default selectivity estimate for boolean and null test nodes */
#define DEFAULT_UNK_SEL 0.005
#define DEFAULT_NOT_UNK_SEL (1.0 - DEFAULT_UNK_SEL)

/* default selectivity estimate for neq anti join */
#define MIN_NEQ_ANTI_SEL 0.05
#define MAX_NEQ_SEMI_SEL (1.0 - MIN_NEQ_ANTI_SEL)

/* If the selectivity is too high, we do not use POISSON to estimate the numbwe of distinct values */
#define SELECTIVITY_THRESHOLD_TO_USE_POISSON 0.95

/* Estimate local distinct of join or agg after filter or join for scattered distribution. */
#define NUM_DISTINCT_SELECTIVITY_FOR_POISSON(distinct, input_rows, selectivity) \
    (double)((distinct) * (1 - exp(-((input_rows) * (selectivity) / (distinct)))))

/* Estimate distinct from global to local for scattered distribution. */
#define NUM_DISTINCT_GTL_FOR_POISSON(gdistinct, input_rows, num_datanodes, dop) \
    (double)(NUM_DISTINCT_SELECTIVITY_FOR_POISSON(gdistinct, input_rows, 1.0 / num_datanodes / (dop)))

/* Estimate thread distinct from dn distinct num. */
#define NUM_PARALLEL_DISTINCT_GTL_FOR_POISSON(dn_distinct, dn_rows, dop) \
    (double)(NUM_DISTINCT_SELECTIVITY_FOR_POISSON(dn_distinct, dn_rows, 1.0 / (dop)))

/*
 * Clamp a computed probability estimate (which may suffer from roundoff or
 * estimation errors) to valid range.  Argument must be a float variable.
 */
#define CLAMP_PROBABILITY(p) \
    do {                     \
        if (p < 0.0)         \
            p = 0.0;         \
        else if (p > 1.0)    \
            p = 1.0;         \
    } while (0)

/* Return data from examine_variable and friends */
typedef struct VariableStatData {
    Node* var;            /* the Var or expression tree */
    RelOptInfo* rel;      /* Relation, or NULL if not identifiable */
    HeapTuple statsTuple; /* pg_statistic tuple, or NULL if none */
    /* NB: if statsTuple!=NULL, it must be freed when caller is done */
    void (*freefunc)(HeapTuple tuple); /* how to free statsTuple */
    Oid vartype;                       /* exposed type of expression */
    Oid atttype;                       /* type to pass to get_attstatsslot */
    int32 atttypmod;                   /* typmod to pass to get_attstatsslot */
    bool isunique;                     /* matches unique index or DISTINCT clause */
    bool enablePossion;                /* indentify we can use possion or not */
    bool acl_ok;                       /* result of ACL check on table or column */
    PlannerInfo *root;                 /* Planner info the var reference */
    double numDistinct[2];             /* estimated numdistinct, 0: means unknown, [0]: local, [1]: global */
    bool isEstimated;                  /* indicate that whether estimation have already been done */
    PlannerInfo *baseRoot;             /* Planner info of the baseVar */
    Node *baseVar;                     /* base Var, owner of the statsTuple */
    RelOptInfo *baseRel;               /* rel of the baseVar */
    bool needAdjust;                   /* true if need adjust on rel */
} VariableStatData;

#define ReleaseVariableStats(vardata)                    \
    do {                                                 \
        if (HeapTupleIsValid((vardata).statsTuple))      \
            (*(vardata).freefunc)((vardata).statsTuple); \
    } while (0)

typedef enum { Pattern_Type_Like, Pattern_Type_Like_IC, Pattern_Type_Regex, Pattern_Type_Regex_IC } Pattern_Type;

typedef enum { Pattern_Prefix_None, Pattern_Prefix_Partial, Pattern_Prefix_Exact } Pattern_Prefix_Status;

typedef enum { STATS_TYPE_GLOBAL, STATS_TYPE_LOCAL } STATS_EST_TYPE;

/*
 * Helper routine for estimate_num_groups: add an item to a list of
 * GroupVarInfos, but only if it's not known equal to any of the existing
 * entries.
 */
typedef struct {
    Node* var;             /* might be an expression, not just a Var */
    RelOptInfo* rel;       /* relation it belongs to */
    double ndistinct;      /* # distinct values */
    bool isdefault;        /* if estimated distinct value is default value */
    bool es_is_used;       /* true if extended statistic is used*/
    Bitmapset* es_attnums; /* number of correlated attributes */
} GroupVarInfo;

extern void set_local_rel_size(PlannerInfo* root, RelOptInfo* rel);
extern double get_join_ratio(VariableStatData* vardata, SpecialJoinInfo* sjinfo);
extern double get_multiple_by_distkey(PlannerInfo* root, List* distkey, double rows);
extern double estimate_agg_num_distinct(PlannerInfo* root, List* group_exprs, Plan* plan, const double* numGroups);
extern double estimate_agg_num_distinct(PlannerInfo* root, List* group_exprs, Path* path, const double* numGroups);
extern void output_noanalyze_rellist_to_log(int lev);
extern void set_noanalyze_rellist(Oid relid, AttrNumber attid);
extern double estimate_local_numdistinct(PlannerInfo* root, Node* hashkey, Path* path, SpecialJoinInfo* sjinfo,
    double* global_distinct, bool* isdefault, VariableStatData* vardata);
extern void get_num_distinct(PlannerInfo* root, List* groupExprs, double local_rows, double global_rows,
    unsigned int num_datanodes, double* numdistinct, List** pgset = NULL);

extern double get_local_rows(double global_rows, double multiple, bool replicate, unsigned int num_data_nodes);
extern double get_global_rows(double local_rows, double multiple, unsigned int num_data_nodes);

#define PATH_LOCAL_ROWS(path) \
    get_local_rows(           \
        (path)->rows, (path)->multiple, IsLocatorReplicated((path)->locator_type), ng_get_dest_num_data_nodes(path))
#define PLAN_LOCAL_ROWS(plan) \
    get_local_rows(           \
        (plan)->plan_rows, (plan)->multiple, (plan)->exec_type != EXEC_ON_DATANODES, ng_get_dest_num_data_nodes(plan))
#define RELOPTINFO_LOCAL_FIELD(root, rel, fldname) \
    get_local_rows((rel)->fldname,                 \
        (rel)->multiple,                           \
        IsLocatorReplicated((rel)->locator_type),  \
        ng_get_dest_num_data_nodes((root), (rel)))
#define IDXOPTINFO_LOCAL_FIELD(root, idx, fldname)     \
    get_local_rows((idx)->fldname,                     \
        (idx)->rel->multiple,                          \
        IsLocatorReplicated((idx)->rel->locator_type), \
        ng_get_dest_num_data_nodes((root), (idx)->rel))

/* Functions in selfuncs.c */

extern void examine_variable(PlannerInfo* root, Node* node, int varRelid, VariableStatData* vardata);
extern bool statistic_proc_security_check(const VariableStatData *vardata, Oid func_oid);
extern bool get_restriction_variable(
    PlannerInfo* root, List* args, int varRelid, VariableStatData* vardata, Node** other, bool* varonleft);
extern void get_join_variables(PlannerInfo* root, List* args, SpecialJoinInfo* sjinfo, VariableStatData* vardata1,
    VariableStatData* vardata2, bool* join_is_reversed);
extern double get_variable_numdistinct(VariableStatData* vardata, bool* isdefault, bool adjust_rows = true,
    double join_ratio = 1.0, SpecialJoinInfo* sjinfo = NULL, STATS_EST_TYPE eType = STATS_TYPE_GLOBAL, 
    bool isJoinVar = false);
extern double mcv_selectivity(VariableStatData* vardata, FmgrInfo* opproc, Datum constval, bool varonleft,
    double* sumcommonp, Oid equaloperator, bool* inmcv, double* lastcommonp = NULL);
extern double histogram_selectivity(VariableStatData* vardata, FmgrInfo* opproc, Datum constval, bool varonleft,
    int min_hist_size, int n_skip, int* hist_size);

extern Pattern_Prefix_Status pattern_fixed_prefix(
    Const* patt, Pattern_Type ptype, Oid collation, Const** prefix, Selectivity* rest_selec);
extern Const* make_greater_string(const Const* str_const, FmgrInfo* ltproc, Oid collation);

extern Datum eqsel(PG_FUNCTION_ARGS);
extern Datum neqsel(PG_FUNCTION_ARGS);
extern Datum scalarltsel(PG_FUNCTION_ARGS);
extern Datum scalargtsel(PG_FUNCTION_ARGS);
extern Datum regexeqsel(PG_FUNCTION_ARGS);
extern Datum icregexeqsel(PG_FUNCTION_ARGS);
extern Datum likesel(PG_FUNCTION_ARGS);
extern Datum iclikesel(PG_FUNCTION_ARGS);
extern Datum regexnesel(PG_FUNCTION_ARGS);
extern Datum icregexnesel(PG_FUNCTION_ARGS);
extern Datum nlikesel(PG_FUNCTION_ARGS);
extern Datum icnlikesel(PG_FUNCTION_ARGS);

extern Datum eqjoinsel(PG_FUNCTION_ARGS);
extern Datum neqjoinsel(PG_FUNCTION_ARGS);
extern Datum scalarltjoinsel(PG_FUNCTION_ARGS);
extern Datum scalargtjoinsel(PG_FUNCTION_ARGS);
extern Datum regexeqjoinsel(PG_FUNCTION_ARGS);
extern Datum icregexeqjoinsel(PG_FUNCTION_ARGS);
extern Datum likejoinsel(PG_FUNCTION_ARGS);
extern Datum iclikejoinsel(PG_FUNCTION_ARGS);
extern Datum regexnejoinsel(PG_FUNCTION_ARGS);
extern Datum icregexnejoinsel(PG_FUNCTION_ARGS);
extern Datum nlikejoinsel(PG_FUNCTION_ARGS);
extern Datum icnlikejoinsel(PG_FUNCTION_ARGS);

extern Selectivity booltestsel(
    PlannerInfo* root, BoolTestType booltesttype, Node* arg, int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo);
extern Selectivity nulltestsel(
    PlannerInfo* root, NullTestType nulltesttype, Node* arg, int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo);
extern Selectivity scalararraysel(PlannerInfo* root, ScalarArrayOpExpr* clause, bool is_join_clause, int varRelid,
    JoinType jointype, SpecialJoinInfo* sjinfo);
extern int estimate_array_length(Node* arrayexpr);
extern Selectivity rowcomparesel(
    PlannerInfo* root, RowCompareExpr* clause, int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo);

extern void mergejoinscansel(PlannerInfo* root, Node* clause, Oid opfamily, int strategy, bool nulls_first,
    Selectivity* leftstart, Selectivity* leftend, Selectivity* rightstart, Selectivity* rightend);

extern double estimate_num_groups(PlannerInfo* root, List* groupExprs, double input_rows, unsigned int num_datanodes,
    STATS_EST_TYPE eType = STATS_TYPE_GLOBAL, List** pgset = NULL);

extern Selectivity estimate_hash_bucketsize(
    PlannerInfo* root, Node* hashkey, double nbuckets, Path* inner_path, SpecialJoinInfo* sjinfo, double* distinctnum);

extern Datum btcostestimate(PG_FUNCTION_ARGS);
extern Datum ubtcostestimate(PG_FUNCTION_ARGS);
extern Datum hashcostestimate(PG_FUNCTION_ARGS);
extern Datum gistcostestimate(PG_FUNCTION_ARGS);
extern Datum spgcostestimate(PG_FUNCTION_ARGS);
extern Datum gincostestimate(PG_FUNCTION_ARGS);
extern Datum psortcostestimate(PG_FUNCTION_ARGS);

/* Functions in array_selfuncs.c */

extern Selectivity scalararraysel_containment(
    PlannerInfo* root, Node* leftop, Node* rightop, Oid elemtype, bool isEquality, bool useOr, int varRelid);
extern Datum arraycontsel(PG_FUNCTION_ARGS);
extern Datum arraycontjoinsel(PG_FUNCTION_ARGS);

/* the type for var data ratio we cached. */
typedef enum { RatioType_Filter, RatioType_Join } RatioType;

/* var ratio structure for one relation after join with other relation or filter by self. */
typedef struct VarRatio {
    RatioType ratiotype; /* filter ratio or join ratio. */
    Node* var;           /* the var of local rel in restriction clause. */
    double ratio;        /* identify joinratio or filterratio if after compute join selectivity,
                              others it means selectivity of filter. */
    Relids joinrelids;   /* the joinrel relids. */
} VarRatio;

typedef struct VarEqRatio {
    Var* var;          /* the var of local rel in restriction clause. */
    double ratio;      /* identify joinratio or filterratio if after compute join selectivity,
                            others it means selectivity of filter. */
    Relids joinrelids; /* the joinrel relids. */
} VarEqRatio;

extern void set_varratio_after_calc_selectivity(
    VariableStatData* vardata, RatioType type, double ratio, SpecialJoinInfo* sjinfo);
extern double get_windowagg_selectivity(PlannerInfo* root, WindowClause* wc, WindowFunc* wfunc, List* partitionExprs,
    int32 constval, double tuples, unsigned int num_datanodes);
extern bool contain_single_col_stat(List* stat_list);
#endif /* SELFUNCS_H */
