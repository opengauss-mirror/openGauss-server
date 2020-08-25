/* -------------------------------------------------------------------------
 *
 * selfuncs.c
 *	  Selectivity functions and index cost estimation functions for
 *	  standard operators and index access methods.
 *
 *	  Selectivity routines are registered in the pg_operator catalog
 *	  in the "oprrest" and "oprjoin" attributes.
 *
 *	  Index cost functions are registered in the pg_am catalog
 *	  in the "amcostestimate" attribute.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/selfuncs.c
 *
 * -------------------------------------------------------------------------
 */

/* ----------
 * Operator selectivity estimation functions are called to estimate the
 * selectivity of WHERE clauses whose top-level operator is their operator.
 * We divide the problem into two cases:
 *		Restriction clause estimation: the clause involves vars of just
 *			one relation.
 *		Join clause estimation: the clause involves vars of multiple rels.
 * Join selectivity estimation is far more difficult and usually less accurate
 * than restriction estimation.
 *
 * When dealing with the inner scan of a nestloop join, we consider the
 * join's joinclauses as restriction clauses for the inner relation, and
 * treat vars of the outer relation as parameters (a/k/a constants of unknown
 * values).  So, restriction estimators need to be able to accept an argument
 * telling which relation is to be treated as the variable.
 *
 * The call convention for a restriction estimator (oprrest function) is
 *
 *		Selectivity oprrest (PlannerInfo *root,
 *							 Oid operator,
 *							 List *args,
 *							 int varRelid);
 *
 * root: general information about the query (rtable and RelOptInfo lists
 * are particularly important for the estimator).
 * operator: OID of the specific operator in question.
 * args: argument list from the operator clause.
 * varRelid: if not zero, the relid (rtable index) of the relation to
 * be treated as the variable relation.  May be zero if the args list
 * is known to contain vars of only one relation.
 *
 * This is represented at the SQL level (in pg_proc) as
 *
 *      float8 oprrest (internal, oid, internal, int4);
 *
 * The result is a selectivity, that is, a fraction (0 to 1) of the rows
 * of the relation that are expected to produce a TRUE result for the
 * given operator.
 *
 * The call convention for a join estimator (oprjoin function) is similar
 * except that varRelid is not needed, and instead join information is
 * supplied:
 *
 *		Selectivity oprjoin (PlannerInfo *root,
 *							 Oid operator,
 *							 List *args,
 *							 JoinType jointype,
 *							 SpecialJoinInfo *sjinfo);
 *
 *		float8 oprjoin (internal, oid, internal, int2, internal);
 *
 * (Before Postgres 8.4, join estimators had only the first four of these
 * parameters.	That signature is still allowed, but deprecated.)  The
 * relationship between jointype and sjinfo is explained in the comments for
 * clause_selectivity() --- the short version is that jointype is usually
 * best ignored in favor of examining sjinfo.
 *
 * Join selectivity for regular inner and outer joins is defined as the
 * fraction (0 to 1) of the cross product of the relations that is expected
 * to produce a TRUE result for the given operator.  For both semi and anti
 * joins, however, the selectivity is defined as the fraction of the left-hand
 * side relation's rows that are expected to have a match (ie, at least one
 * row with a TRUE result) in the right-hand side.
 *
 * For both oprrest and oprjoin functions, the operator's input collation OID
 * (if any) is passed using the standard fmgr mechanism, so that the estimator
 * function can fetch it with PG_GET_COLLATION().  Note, however, that all
 * statistics in pg_statistic are currently built using the database's default
 * collation.  Thus, in most cases where we are looking at statistics, we
 * should ignore the actual operator collation and use DEFAULT_COLLATION_OID.
 * We expect that the error induced by doing this is usually not large enough
 * to justify complicating matters.
 * ----------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <math.h>

#include "access/gin.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/storage_gtt.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "optimizer/var.h"
#include "optimizer/planner.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/nabstime.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "utils/memutils.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/transam.h"
#endif

static double var_eq_const(VariableStatData* vardata, Oid opera, Datum constval, bool constisnull, bool varonleft);
static double var_eq_non_const(VariableStatData* vardata, Oid opera, Node* other, bool varonleft);
static double ineq_histogram_selectivity(
    PlannerInfo* root, VariableStatData* vardata, FmgrInfo* opproc, bool isgt, Datum constval, Oid consttype);
static double eqjoinsel_inner(
    Oid opera, VariableStatData* vardata1, VariableStatData* vardata2, SpecialJoinInfo* sjinfo);
static double eqjoinsel_semi(
    Oid opera, VariableStatData* vardata1, VariableStatData* vardata2, RelOptInfo* inner_rel, SpecialJoinInfo* sjinfo);
static double neqjoinsel_semi(
    Oid opera, VariableStatData* vardata1, VariableStatData* vardata2, RelOptInfo* inner_rel, SpecialJoinInfo* sjinfo);
static bool convert_to_scalar(Datum value, Oid valuetypid, double* scaledvalue, Datum lobound, Datum hibound,
    Oid boundstypid, double* scaledlobound, double* scaledhibound);
static double convert_numeric_to_scalar(Datum value, Oid typid);
static void convert_string_to_scalar(
    const char* value, double* scaledvalue, char* lobound, double* scaledlobound, char* hibound, double* scaledhibound);
static void convert_bytea_to_scalar(
    Datum value, double* scaledvalue, Datum lobound, double* scaledlobound, Datum hibound, double* scaledhibound);
static double convert_one_string_to_scalar(const char* value, int rangelo, int rangehi);
static double convert_one_bytea_to_scalar(unsigned char* value, int valuelen, int rangelo, int rangehi);
static char* convert_string_datum(Datum value, Oid typid);
static double convert_timevalue_to_scalar(Datum value, Oid typid);
static void examine_simple_variable(PlannerInfo* root, Var* var, VariableStatData* vardata);
static bool get_variable_range(PlannerInfo* root, VariableStatData* vardata, Oid sortop, Datum* min, Datum* max);
static bool get_actual_variable_range(PlannerInfo* root, VariableStatData* vardata, Oid sortop, Datum* min, Datum* max);
RelOptInfo* find_join_input_rel(PlannerInfo* root, Relids relids);
static Selectivity prefix_selectivity(
    PlannerInfo* root, VariableStatData* vardata, Oid vartype, Oid opfamily, Const* prefixcon);
static Selectivity like_selectivity(const char* patt, int pattlen, bool case_insensitive);
static Selectivity regex_selectivity(const char* patt, int pattlen, bool case_insensitive, int fixed_prefix_len);
static Datum string_to_datum(const char* str, Oid datatype);
static Const* string_to_const(const char* str, Oid datatype);
static Const* string_to_bytea_const(const char* str, size_t str_len);
static List* specialExpr_group_num(PlannerInfo* root, List* nodeList, double* numdistinct, double rows);
static List* add_unique_group_var(
    PlannerInfo* root, List* varinfos, Node* var, VariableStatData* vardata, STATS_EST_TYPE eType = STATS_TYPE_LOCAL);
static double get_join_ratio(VariableStatData* vardata, SpecialJoinInfo* sjinfo);
bool can_use_possion(VariableStatData* vardata, SpecialJoinInfo* sjinfo, double* ratio);
extern Datum pg_stat_get_last_analyze_time(PG_FUNCTION_ARGS);
extern List* find_skew_join_distribute_keys(Plan* plan);

static Oid distinct_unshippable_func[] = {
    /* substring func */
    TEXTSUBSTRINGFUNCOID,
    TEXTSUBSTRINGNOLENFUNCOID,
    BITSUBSTRINGFUNOID,
    BITSUBSTRINGNOLENFUNCOID,
    BYTEASUBSTRINGFUNCOID,
    BYTEASUBSTRINGNOLENOID,
    TEXTREGEXSUBSTRINGOID,
    SUBSTRINGESCAPEFUNCOID,
    /* substr func */
    SUBSTRFUNCOID,
    SUBSTRBWITHLENFUNCOID,
    SUBSTRBNOLENFUNCOID,
    SUBSTRNOLENFUNCOID,
    SUBSTRINNFUNCOID,
    SUBSTRINNNOLENFUNCOID,
    BYTEASUBSTRFUNCOID,
    BYTEASUBSTRNOLENFUNCOID,
    /* length */
    TEXTLENOID,
    LENGTHFUNCOID,
    BPLENFUNCOID,
    TEXTOCTLENFUNCOID,
    BPOCTLENFUNCOID,
    CHARLENFUNCOID,
    /* datepart */
    TIMESTAMPTZPARTFUNCOID,
    INTERVALPARTFUNCOID,
    TIMESTAMPTZTRUNCFUNCOID,
    INTERVALTRUNCFUNCOID,
    TIMEZPARTFUNCOID,
    DATEPARTFROMABSTIMEFUNCOID,
    DATEPARTFROMRELTIMEFUNCOID,
    DATEPARTFUNCOID,
    TIMEPARTFUNCOID,
    TIMESTAMPPARTFUNCOID,
    /* position */
    POSITIONFUNCOID,
    STRPOSFUNCOID,
    BITPOSITIONFUNCOID,
    BYTEAPOSFUNCOID,
    INSTR2FUNCOID,
    INSTR3FUNCOID,
    INSTR4FUNCOID};

/* Is not different var in the same rel. */
#define VARNEQ(var1, var2)                                                                    \
    (IsA(var1, Var) && IsA(var2, Var)) && (((Var*)(var1))->varno == ((Var*)(var2))->varno) && \
        !(_equalSimpleVar(var1, var2))

#define MINIMAL_SEL 1.0e-5 /* selectivity less than this is counted as approxiamtely 0 */
#define SKEW_RATIO 2       /* the skew ratio under this is considered as even distributed */
#define extrapolation_stats_log(funcname, selec, extrapolationselec)          \
    {                                                                         \
        ereport(DEBUG2,                                                       \
            (errmodule(MOD_OPT),                                              \
                errmsg("Extrapolation stats[%s]: original selectivity: %lf, " \
                       "extrapolated selectivity: %lf",                       \
                    funcname,                                                 \
                    selec,                                                    \
                    extrapolationselec)));                                    \
    }

/*
 *		eqsel			- Selectivity of "=" for any data types.
 *
 * Note: this routine is also used to estimate selectivity for some
 * operators that are not "=" but have comparable selectivity behavior,
 * such as "~=" (geometric approximate-match).	Even for "=", we must
 * keep in mind that the left and right datatypes may differ.
 */
Datum eqsel(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);
    int var_relid = PG_GETARG_INT32(3);
    VariableStatData var_data;
    var_data.statsTuple = NULL;
    var_data.freefunc = NULL;
    var_data.rel = NULL;
    var_data.var = NULL;
    Node* other = NULL;
    bool var_on_left = true;
    double selec;

    /*
     * If expression is not variable = something or something = variable, then
     * punt and return a default estimate.
     */
    if (!get_restriction_variable(root, args, var_relid, &var_data, &other, &var_on_left)) {
        PG_RETURN_FLOAT8(DEFAULT_EQ_SEL);
    }

    /*
     * We can do a lot better if the something is a constant.  (Note: the
     * Const might result from estimation rather than being a simple constant
     * in the query.)
     */
    if (IsA(other, Const)) {
        selec = var_eq_const(&var_data, opera, ((Const*)other)->constvalue, ((Const*)other)->constisnull, var_on_left);
    } else {
        selec = var_eq_non_const(&var_data, opera, other, var_on_left);
    }

    ReleaseVariableStats(var_data);

    PG_RETURN_FLOAT8((float8)selec);
}

/*
 * var_eq_const --- eqsel for var = const case
 *
 * This is split out so that some other estimation functions can use it.
 */
static double var_eq_const(VariableStatData* var_data, Oid opera, Datum const_val, bool const_is_null, bool var_on_left)
{
    double selec;
    bool is_default = false;
    Oid opfuncoid;

    /*
     * If the constant is NULL, assume operator is strict and return zero, ie,
     * operator will never return TRUE.
     */
    if (const_is_null) {
        return 0.0;
    }

    /*
     * If we matched the var to a unique index or DISTINCT clause, assume
     * there is exactly one match regardless of anything else.	(This is
     * slightly bogus, since the index or clause's equality operator might be
     * different from ours, but it's much more likely to be right than
     * ignoring the information.)
     */
    if (var_data->isunique && var_data->rel && var_data->rel->tuples >= 1.0) {
        return 1.0 / var_data->rel->tuples;
    }

    if (HeapTupleIsValid(var_data->statsTuple) &&
        StatisticProcSecurityCheck(var_data, (opfuncoid = get_opcode(opera)))) {
        Form_pg_statistic stats;
        Datum* values = NULL;
        int n_values;
        float4* numbers = NULL;
        int n_numbers;
        bool match = false;
        int i;

        stats = (Form_pg_statistic)GETSTRUCT(var_data->statsTuple);

        /*
         * Is the constant "=" to any of the column's most common values?
         * (Although the given operator may not really be "=", we will assume
         * that seeing whether it returns TRUE is an appropriate test.	If you
         * don't like this, maybe you shouldn't be using eqsel for your
         * operator...)
         */
        if (get_attstatsslot(var_data->statsTuple,
                var_data->atttype,
                var_data->atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                &values,
                &n_values,
                &numbers,
                &n_numbers)) {
            FmgrInfo eqproc;

            fmgr_info(opfuncoid, &eqproc);

            for (i = 0; i < n_values; i++) {
                /* be careful to apply operator right way 'round */
                if (var_on_left) {
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, values[i], const_val));
                } else {
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, const_val, values[i]));
                }
                if (match) {
                    break;
                }
            }
        } else {
            /* no most-common-value info available */
            values = NULL;
            numbers = NULL;
            i = n_values = n_numbers = 0;
        }

        if (match) {
            /*
             * Constant is "=" to this common value.  We know selectivity
             * exactly (or as exactly as ANALYZE could calculate it, anyway).
             */
            selec = numbers[i];
        } else {
            /*
             * Comparison is against a constant that is neither NULL nor any
             * of the common values.  Its selectivity cannot be more than
             * this:
             */
            double sum_common = 0.0;
            double other_distinct;
            double extra_polation_selec = 0.0;

            for (i = 0; i < n_numbers; i++) {
                sum_common += numbers[i];
            }
            selec = 1.0 - sum_common - stats->stanullfrac;
            CLAMP_PROBABILITY(selec);

            if (u_sess->attr.attr_sql.enable_extrapolation_stats && selec <= MINIMAL_SEL &&
                TypeCategory(var_data->vartype) == TYPCATEGORY_DATETIME) {
                /*
                 * Extrapolation stats for all mcv values without histogram
                 * only apply if there's no skew
                 */
                if (n_numbers > 0 && numbers[0] <= numbers[n_numbers - 1] * SKEW_RATIO)
                    extra_polation_selec = numbers[n_numbers - 1];
            }

            /*
             * and in fact it's probably a good deal less. We approximate that
             * all the not-common values share this remaining fraction
             * equally, so we divide by the number of other distinct values.
             */
            other_distinct =
                get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, STATS_TYPE_GLOBAL) - n_numbers;
            if (other_distinct > 1) {
                selec /= other_distinct;
            }

            /*
             * Another cross-check: selectivity shouldn't be estimated as more
             * than the least common "most common value".
             */
            if (n_numbers > 0 && selec > numbers[n_numbers - 1]) {
                selec = numbers[n_numbers - 1];
            }
            if (u_sess->attr.attr_sql.enable_extrapolation_stats) {
                extrapolation_stats_log(__FUNCTION__, selec, extra_polation_selec);
            }

            selec = Max(selec, extra_polation_selec);
        }

        free_attstatsslot(var_data->atttype, values, n_values, numbers, n_numbers);
    } else {
        /*
         * No ANALYZE stats available, so make a guess using estimated number
         * of distinct values and assuming they are equally common. (The guess
         * is unlikely to be very good, but we do know a few special cases.)
         * "FuncExpr = Const" selectivity is underestimated due to lack of stats,
         * so we hack it to 1.0/3.0
         */
        if (IsA(var_data->var, FuncExpr)) {
            if (((FuncExpr*)var_data->var)->funcid == SUBSTRFUNCOID ||
                ((FuncExpr*)var_data->var)->funcid == LENGTHFUNCOID) {
                selec = 1.0 / 3.0;
            } else {
                selec = 1.0 / get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, STATS_TYPE_GLOBAL);
            }
        } else {
            selec = 1.0 / get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, STATS_TYPE_GLOBAL);
        }
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY(selec);

    return selec;
}

/*
 * var_eq_non_const --- eqsel for var = something-other-than-const case
 */
static double var_eq_non_const(VariableStatData* var_data, Oid opera, Node* other, bool var_on_left)
{
    double selec;
    bool is_default = false;

    /*
     * If we matched the var to a unique index or DISTINCT clause, assume
     * there is exactly one match regardless of anything else.	(This is
     * slightly bogus, since the index or clause's equality operator might be
     * different from ours, but it's much more likely to be right than
     * ignoring the information.)
     */
    if (var_data->isunique && var_data->rel && var_data->rel->tuples >= 1.0) {
        return 1.0 / var_data->rel->tuples;
    }

    if (HeapTupleIsValid(var_data->statsTuple)) {
        Form_pg_statistic stats;
        double n_distinct;
        float4* numbers = NULL;
        int n_numbers;

        stats = (Form_pg_statistic)GETSTRUCT(var_data->statsTuple);

        /*
         * Search is for a value that we do not know a priori, but we will
         * assume it is not NULL.  Estimate the selectivity as non-null
         * fraction divided by number of distinct values, so that we get a
         * result averaged over all possible values whether common or
         * uncommon.  (Essentially, we are assuming that the not-yet-known
         * comparison value is equally likely to be any of the possible
         * values, regardless of their frequency in the table.	Is that a good
         * idea?)
         */
        selec = 1.0 - stats->stanullfrac;
        n_distinct = get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, STATS_TYPE_GLOBAL);
        if (n_distinct > 1) {
            selec /= n_distinct;
        }

        /*
         * Cross-check: selectivity should never be estimated as more than the
         * most common value's.
         */
        if (get_attstatsslot(var_data->statsTuple,
                var_data->atttype,
                var_data->atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                NULL,
                NULL,
                &numbers,
                &n_numbers)) {
            if (n_numbers > 0 && selec > numbers[0]) {
                selec = numbers[0];
            }
            free_attstatsslot(var_data->atttype, NULL, 0, numbers, n_numbers);
        }
    } else {
        /*
         * No ANALYZE stats available, so make a guess using estimated number
         * of distinct values and assuming they are equally common. (The guess
         * is unlikely to be very good, but we do know a few special cases.)
         */
        selec = 1.0 / get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, STATS_TYPE_GLOBAL);
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY(selec);

    return selec;
}

/*
 *		neqsel			- Selectivity of "!=" for any data types.
 *
 * This routine is also used for some operators that are not "!="
 * but have comparable selectivity behavior.  See above comments
 * for eqsel().
 */
Datum neqsel(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);
    int var_relid = PG_GETARG_INT32(3);
    Oid eq_op;
    float8 result;

    /*
     * We want 1 - eqsel() - nullfrac where the equality operator is the one associated
     * with this != operator, that is, its negator.
     */
    eq_op = get_negator(opera);
    if (eq_op) {
        result = DatumGetFloat8(DirectFunctionCall4(
            eqsel, PointerGetDatum(root), ObjectIdGetDatum(eq_op), PointerGetDatum(args), Int32GetDatum(var_relid)));

        /*
         * calculate nullfrac of the variable for var <> const or const <> var.
         * First, we should detemine on left or right the variable locates, and
         * then do the fraction calculation of null test
         */
        Node *left = (Node*)linitial(args);
        Node *right = (Node*)lsecond(args);
        Node *node = NULL;
        Relids varno_left = pull_varnos(left);
        Relids varno_right = pull_varnos(right);
        if (varno_left != NULL && varno_right == NULL) {
            node = left;
        } else if (varno_left == NULL && varno_right != NULL) {
            node = right;
        }
        if (node != NULL) {
            result += nulltestsel(root, IS_NULL, node, var_relid, JOIN_INNER, NULL);
        }
        CLAMP_PROBABILITY(result);
        bms_free(varno_left);
        varno_left = NULL;
        bms_free(varno_right);
        varno_right = NULL;
    } else {
        /* Use default selectivity (should we raise an error instead?) */
        result = DEFAULT_EQ_SEL;
    }
    result = 1.0 - result;
    PG_RETURN_FLOAT8(result);
}

/*
 *	scalarineqsel		- Selectivity of "<", "<=", ">", ">=" for scalars.
 *
 * This is the guts of both scalarltsel and scalargtsel.  The caller has
 * commuted the clause, if necessary, so that we can treat the variable as
 * being on the left.  The caller must also make sure that the other side
 * of the clause is a non-null Const, and dissect same into a value and
 * datatype.
 *
 * This routine works for any datatype (or pair of datatypes) known to
 * convert_to_scalar().  If it is applied to some other datatype,
 * it will return a default estimate.
 */
static double scalarineqsel(
    PlannerInfo* root, Oid opera, bool is_gt, VariableStatData* var_data, Datum const_val, Oid const_type)
{
    Form_pg_statistic stats;
    FmgrInfo op_proc;
    double mcv_selec;
    double hist_selec;
    double sum_common = 0.0;
    double last_common = 0.0;
    double selec;
    Oid equal_operator = InvalidOid;
    double equal_selec = 0.0;
    bool in_mcv = false;

    if (!HeapTupleIsValid(var_data->statsTuple)) {
        /* no stats available, so default result */
        return DEFAULT_INEQ_SEL;
    }
    stats = (Form_pg_statistic)GETSTRUCT(var_data->statsTuple);

    fmgr_info(get_opcode(opera), &op_proc);
    equal_operator = get_equal(opera);

    /*
     * If we have most-common-values info, add up the fractions of the MCV
     * entries that satisfy MCV OP CONST.  These fractions contribute directly
     * to the result selectivity.  Also add up the total fraction represented
     * by MCV entries.
     */
    mcv_selec = mcv_selectivity(var_data, &op_proc, const_val, true, &sum_common,
        equal_operator, &in_mcv, &last_common);

    /*
     * If there is a histogram, determine which bin the constant falls in, and
     * compute the resulting contribution to selectivity.
     */
    hist_selec = ineq_histogram_selectivity(root, var_data, &op_proc, is_gt, const_val, const_type);

    /*
     * Now merge the results from the MCV and histogram calculations,
     * realizing that the histogram covers only the non-null values that are
     * not listed in MCV.
     */
    selec = 1.0 - stats->stanullfrac - sum_common;

    if (hist_selec >= 0.0) {
        selec *= hist_selec;
    } else {
        /*
         * If no histogram but there are values not accounted for by MCV,
         * arbitrarily assume half of them will match.
         */
        selec *= 0.5;
    }

    selec += mcv_selec;

    /*
     * The purpose of the following branch is to handle equal range condition
     * but the value is not collected in mcv. If cost_param is turned on, the branch
     * is going to work for broundary equality selectivity consideration.
     * In some cases, the estimated rows would be far away from the actual rows,
     * and cause execution performance degragation.
     * So the equal selectivity will be used to optimize the range selectivity
     * and eliminate the performance degragation.
     */
    if (((uint32)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_EQUALRANGE_NOTINMCV) && !in_mcv &&
        (get_opname(opera) != NULL && (strcmp(get_opname(opera), ">=") == 0 || strcmp(get_opname(opera), "<=") == 0))) {
        equal_selec = var_eq_const(var_data, equal_operator, const_val, false, true);
        /*
         * For ">=" and "<=" selectivity calculation, it is worth noticing that
         * if const_val is not in MCV, the "=" part will be ignored by both
         * hist_selec and mcv_selec. To accommodate for such cases, it is needed to
         * add the equal selectivity.
         */
        selec += equal_selec;
    }

    /* extrapolation stats */
    if (u_sess->attr.attr_sql.enable_extrapolation_stats && is_gt &&
        TypeCategory(var_data->vartype) == TYPCATEGORY_DATETIME) {
        if (hist_selec < 0.0 && sum_common > 0.0) {
            /* no histogram, only mcv, use freq of last mcv as extrapolation selectivity */
            extrapolation_stats_log(__FUNCTION__, selec, last_common);
            selec = Max(last_common, selec);
        } else if (hist_selec >= 0.0 && sum_common == 0.0) {
            /* no mcv, only histogram, use 1/distinct as extrapolation selectivity */
            bool is_default = false;
            double num_distinct = get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, STATS_TYPE_GLOBAL);
            double extra_polation_selec = (num_distinct == 0.0) ? 0.0 : (1.0 - stats->stanullfrac) / num_distinct;
            extrapolation_stats_log(__FUNCTION__, selec, extra_polation_selec);
            selec = Max(selec, extra_polation_selec);
        }
    }
    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY(selec);

    return selec;
}

/*
 *	mcv_selectivity			- Examine the MCV list for selectivity estimates
 *
 * Determine the fraction of the variable's MCV population that satisfies
 * the predicate (VAR OP CONST), or (CONST OP VAR) if !varonleft.  Also
 * compute the fraction of the total column population represented by the MCV
 * list.  This code will work for any boolean-returning predicate operator.
 *
 * The function result is the MCV selectivity, and the fraction of the
 * total population is returned into *sumcommonp.  Zeroes are returned
 * if there is no MCV list.
 */
double mcv_selectivity(VariableStatData* var_data, FmgrInfo* op_proc, Datum const_val, bool var_on_left,
    double* sum_commonp, Oid equal_operator, bool* in_mcv, double* last_commonp)
{
    double mcv_selec = 0.0;
    double sum_common = 0.0;
    Datum* values = NULL;
    int n_values;
    float4* numbers = NULL;
    int n_numbers;
    int i;
    FmgrInfo op_equal;

    *in_mcv = false;
    /*
     * In normal cases, however, when calculating selectivity of a > const or
     * a < const, if a is a not a mcv the selectivity of a = const is split
     * into two parts and is added to the two sides, because of the difference
     * between the histogram's continuity and the discrete nature of data.
     *
     * To resolve this issue, we get rid of half of the equal selectivity to
     * each side.
     * This is only applied when the threshold value is not in MCV.
     */
    if (equal_operator != InvalidOid) {
        fmgr_info(get_opcode(equal_operator), &op_equal);
    }
    if (HeapTupleIsValid(var_data->statsTuple) &&
        StatisticProcSecurityCheck(var_data, op_proc->fn_oid) &&
        get_attstatsslot(var_data->statsTuple,
            var_data->atttype,
            var_data->atttypmod,
            STATISTIC_KIND_MCV,
            InvalidOid,
            NULL,
            &values,
            &n_values,
            &numbers,
            &n_numbers)) {
        for (i = 0; i < n_values; i++) {
            if (op_equal.fn_oid != InvalidOid && equal_operator != InvalidOid) {
                if (var_on_left ?
                    DatumGetBool(FunctionCall2Coll(&op_equal, DEFAULT_COLLATION_OID, values[i], const_val)) :
                    DatumGetBool(FunctionCall2Coll(&op_equal, DEFAULT_COLLATION_OID, const_val, values[i]))) {
                    *in_mcv = true;
                }
            }
            if (var_on_left ?
                DatumGetBool(FunctionCall2Coll(op_proc, DEFAULT_COLLATION_OID, values[i], const_val)) :
                DatumGetBool(FunctionCall2Coll(op_proc, DEFAULT_COLLATION_OID, const_val, values[i]))) {
                mcv_selec += numbers[i];
            }
            sum_common += numbers[i];
        }
        if (n_values > 0 && last_commonp != NULL && numbers[0] <= numbers[n_values - 1] * SKEW_RATIO) {
            *last_commonp = numbers[n_values - 1];
        }
        free_attstatsslot(var_data->atttype, values, n_values, numbers, n_numbers);
    }

    CLAMP_PROBABILITY(sum_common);
    *sum_commonp = sum_common;
    return mcv_selec;
}

/*
 *	histogram_selectivity	- Examine the histogram for selectivity estimates
 *
 * Determine the fraction of the variable's histogram entries that satisfy
 * the predicate (VAR OP CONST), or (CONST OP VAR) if !varonleft.
 *
 * This code will work for any boolean-returning predicate operator, whether
 * or not it has anything to do with the histogram sort operator.  We are
 * essentially using the histogram just as a representative sample.  However,
 * small histograms are unlikely to be all that representative, so the caller
 * should be prepared to fall back on some other estimation approach when the
 * histogram is missing or very small.	It may also be prudent to combine this
 * approach with another one when the histogram is small.
 *
 * If the actual histogram size is not at least min_hist_size, we won't bother
 * to do the calculation at all.  Also, if the n_skip parameter is > 0, we
 * ignore the first and last n_skip histogram elements, on the grounds that
 * they are outliers and hence not very representative.  Typical values for
 * these parameters are 10 and 1.
 *
 * The function result is the selectivity, or -1 if there is no histogram
 * or it's smaller than min_hist_size.
 *
 * The output parameter *hist_size receives the actual histogram size,
 * or zero if no histogram.  Callers may use this number to decide how
 * much faith to put in the function result.
 *
 * Note that the result disregards both the most-common-values (if any) and
 * null entries.  The caller is expected to combine this result with
 * statistics for those portions of the column population.	It may also be
 * prudent to clamp the result range, ie, disbelieve exact 0 or 1 outputs.
 */
double histogram_selectivity(VariableStatData* var_data, FmgrInfo* op_proc, Datum const_val, bool var_on_left,
    int min_hist_size, int n_skip, int* hist_size)
{
    double result;
    Datum* values = NULL;
    int n_values;

    /* check sanity of parameters */
    Assert(n_skip >= 0);
    Assert(min_hist_size > 2 * n_skip);

    if (HeapTupleIsValid(var_data->statsTuple) &&
        StatisticProcSecurityCheck(var_data, op_proc->fn_oid) &&
        get_attstatsslot(var_data->statsTuple,
            var_data->atttype,
            var_data->atttypmod,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            NULL,
            &values,
            &n_values,
            NULL,
            NULL)) {
        *hist_size = n_values;
        if (n_values >= min_hist_size) {
            int n_match = 0;
            int i;

            for (i = n_skip; i < n_values - n_skip; i++) {
                if (var_on_left ?
                    DatumGetBool(FunctionCall2Coll(op_proc, DEFAULT_COLLATION_OID, values[i], const_val)) :
                    DatumGetBool(FunctionCall2Coll(op_proc, DEFAULT_COLLATION_OID, const_val, values[i]))) {
                    n_match++;
                }
            }
            result = ((double)n_match) / ((double)(n_values - 2 * n_skip));
        } else {
            result = -1;
        }
        free_attstatsslot(var_data->atttype, values, n_values, NULL, 0);
    } else {
        *hist_size = 0;
        result = -1;
    }

    return result;
}

/*
 *	ineq_histogram_selectivity	- Examine the histogram for scalarineqsel
 *
 * Determine the fraction of the variable's histogram population that
 * satisfies the inequality condition, ie, VAR < CONST or VAR > CONST.
 *
 * Returns -1 if there is no histogram (valid results will always be >= 0).
 *
 * Note that the result disregards both the most-common-values (if any) and
 * null entries.  The caller is expected to combine this result with
 * statistics for those portions of the column population.
 */
static double ineq_histogram_selectivity(
    PlannerInfo* root, VariableStatData* var_data, FmgrInfo* op_proc, bool is_gt, Datum const_val, Oid const_type)
{
    double hist_selec;
    Oid hist_op;
    Datum* values = NULL;
    int n_values;

    hist_selec = -1.0;

    /*
     * Someday, ANALYZE might store more than one histogram per rel/att,
     * corresponding to more than one possible sort ordering defined for the
     * column type.  However, to make that work we will need to figure out
     * which staop to search for --- it's not necessarily the one we have at
     * hand!  (For example, we might have a '<=' operator rather than the '<'
     * operator that will appear in staop.)  For now, assume that whatever
     * appears in pg_statistic is sorted the same way our operator sorts, or
     * the reverse way if is_gt is TRUE.
     */
    if (HeapTupleIsValid(var_data->statsTuple) &&
        StatisticProcSecurityCheck(var_data, op_proc->fn_oid) &&
        get_attstatsslot(var_data->statsTuple,
            var_data->atttype,
            var_data->atttypmod,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            &hist_op,
            &values,
            &n_values,
            NULL,
            NULL)) {
        if (n_values > 1) {
            /*
             * Use binary search to find proper location, ie, the first slot
             * at which the comparison fails.  (If the given operator isn't
             * actually sort-compatible with the histogram, you'll get garbage
             * results ... but probably not any more garbage-y than you would
             * from the old linear search.)
             *
             * If the binary search accesses the first or last histogram
             * entry, we try to replace that endpoint with the true column min
             * or max as found by get_actual_variable_range().	This
             * ameliorates misestimates when the min or max is moving as a
             * result of changes since the last ANALYZE.  Note that this could
             * result in effectively including MCVs into the histogram that
             * weren't there before, but we don't try to correct for that.
             */
            double hist_frac;
            int lo_bound = 0;       /* first possible slot to search */
            int hi_bound = n_values; /* last+1 slot to search */
            bool have_end = false;

            /*
             * If there are only two histogram entries, we'll want up-to-date
             * values for both.  (If there are more than two, we need at most
             * one of them to be updated, so we deal with that within the
             * loop.)
             */
            if (n_values == 2) {
                have_end = get_actual_variable_range(root, var_data, hist_op, &values[0], &values[1]);
            }

            while (lo_bound < hi_bound) {
                int probe = (lo_bound + hi_bound) / 2;
                bool lt_cmp = false;

                /*
                 * If we find ourselves about to compare to the first or last
                 * histogram entry, first try to replace it with the actual
                 * current min or max (unless we already did so above).
                 */
                if (probe == 0 && n_values > 2) {
                    have_end = get_actual_variable_range(root, var_data, hist_op, &values[0], NULL);
                } else if (probe == n_values - 1 && n_values > 2) {
                    have_end = get_actual_variable_range(root, var_data, hist_op, NULL, &values[probe]);
                }

                lt_cmp = DatumGetBool(FunctionCall2Coll(op_proc, DEFAULT_COLLATION_OID, values[probe], const_val));
                if (is_gt) {
                    lt_cmp = !lt_cmp;
                }
                if (lt_cmp) {
                    lo_bound = probe + 1;
                } else {
                    hi_bound = probe;
                }
            }

            if (lo_bound <= 0) {
                /* Constant is below lower histogram boundary. */
                hist_frac = 0.0;
            } else if (lo_bound >= n_values) {
                /* Constant is above upper histogram boundary. */
                hist_frac = 1.0;
            } else {
                int i = lo_bound;
                double val;
                double high;
                double low;
                double bin_frac;

                /*
                 * We have values[i-1] <= constant <= values[i].
                 *
                 * Convert the constant and the two nearest bin boundary
                 * values to a uniform comparison scale, and do a linear
                 * interpolation within this bin.
                 */
                if (convert_to_scalar(
                        const_val, const_type, &val, values[i - 1], values[i], var_data->vartype, &low, &high)) {
                    if (high <= low) {
                        /* cope if bin boundaries appear identical */
                        bin_frac = 0.5;
                    } else if (val <= low) {
                        bin_frac = 0.0;
                    } else if (val >= high) {
                        bin_frac = 1.0;
                    } else {
                        bin_frac = (val - low) / (high - low);

                        /*
                         * Watch out for the possibility that we got a NaN or
                         * Infinity from the division.	This can happen
                         * despite the previous checks, if for example "low"
                         * is -Infinity.
                         */
                        if (isnan(bin_frac) || bin_frac < 0.0 || bin_frac > 1.0) {
                            bin_frac = 0.5;
                        }
                    }
                } else {
                    /*
                     * Ideally we'd produce an error here, on the grounds that
                     * the given operator shouldn't have scalarXXsel
                     * registered as its selectivity func unless we can deal
                     * with its operand types.	But currently, all manner of
                     * stuff is invoking scalarXXsel, so give a default
                     * estimate until that can be fixed.
                     */
                    bin_frac = 0.5;
                }

                /*
                 * Now, compute the overall selectivity across the values
                 * represented by the histogram.  We have i-1 full bins and
                 * bin_frac partial bin below the constant.
                 */
                hist_frac = (double)(i - 1) + bin_frac;
                hist_frac /= (double)(n_values - 1);
            }

            /*
             * Now hist_frac = fraction of histogram entries below the
             * constant.
             *
             * Account for "<" vs ">"
             */
            hist_selec = is_gt ? (1.0 - hist_frac) : hist_frac;

            /*
             * The histogram boundaries are only approximate to begin with,
             * and may well be out of date anyway.	Therefore, don't believe
             * extremely small or large selectivity estimates --- unless we
             * got actual current endpoint values from the table.
             */
            if (have_end) {
                CLAMP_PROBABILITY(hist_selec);
            } else {
                if (hist_selec < 0.0001) {
                    hist_selec = 0.0001;
                } else if (hist_selec > 0.9999) {
                    hist_selec = 0.9999;
                }
            }
        }

        free_attstatsslot(var_data->atttype, values, n_values, NULL, 0);
    }

    return hist_selec;
}

/*
 *		scalarltsel		- Selectivity of "<" (also "<=") for scalars.
 */
Datum scalarltsel(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);
    int var_relid = PG_GETARG_INT32(3);
    VariableStatData var_data;
    var_data.statsTuple = NULL;
    var_data.freefunc = NULL;
    var_data.var = NULL;
    var_data.rel = NULL;
    Node* other = NULL;
    bool var_on_left = true;
    Datum const_val;
    Oid const_type;
    bool is_gt = false;
    double selec;

    /*
     * If expression is not variable op something or something op variable,
     * then punt and return a default estimate.
     */
    if (!get_restriction_variable(root, args, var_relid, &var_data, &other, &var_on_left)) {
        PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
    }

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    if (!IsA(other, Const)) {
        ReleaseVariableStats(var_data);
        PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
    }

    /*
     * If the constant is NULL, assume operator is strict and return zero, ie,
     * operator will never return TRUE.
     */
    if (((Const*)other)->constisnull) {
        ReleaseVariableStats(var_data);
        PG_RETURN_FLOAT8(0.0);
    }
    const_val = ((Const*)other)->constvalue;
    const_type = ((Const*)other)->consttype;

    /*
     * Force the var to be on the left to simplify logic in scalarineqsel.
     */
    if (var_on_left) {
        /* we have var < other */
        is_gt = false;
    } else {
        /* we have other < var, commute to make var > other */
        opera = get_commutator(opera);
        if (!opera) {
            /* Use default selectivity (should we raise an error instead?) */
            ReleaseVariableStats(var_data);
            PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
        }
        is_gt = true;
    }

    selec = scalarineqsel(root, opera, is_gt, &var_data, const_val, const_type);

    ReleaseVariableStats(var_data);

    PG_RETURN_FLOAT8((float8)selec);
}

/*
 *		scalargtsel		- Selectivity of ">" (also ">=") for integers.
 */
Datum scalargtsel(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);
    int var_relid = PG_GETARG_INT32(3);
    VariableStatData var_data;
    var_data.statsTuple = NULL;
    var_data.freefunc = NULL;
    var_data.rel = NULL;
    var_data.var = NULL;
    Node* other = NULL;
    bool var_on_left = true;
    Datum const_val;
    Oid const_type;
    bool is_gt = false;
    double selec;

    /*
     * If expression is not variable op something or something op variable,
     * then punt and return a default estimate.
     */
    if (!get_restriction_variable(root, args, var_relid, &var_data, &other, &var_on_left)) {
        PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
    }

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    if (!IsA(other, Const)) {
        ReleaseVariableStats(var_data);
        PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
    }

    /*
     * If the constant is NULL, assume operator is strict and return zero, ie,
     * operator will never return TRUE.
     */
    if (((Const*)other)->constisnull) {
        ReleaseVariableStats(var_data);
        PG_RETURN_FLOAT8(0.0);
    }
    const_val = ((Const*)other)->constvalue;
    const_type = ((Const*)other)->consttype;

    /*
     * Force the var to be on the left to simplify logic in scalarineqsel.
     */
    if (var_on_left) {
        /* we have var > other */
        is_gt = true;
    } else {
        /* we have other > var, commute to make var < other */
        opera = get_commutator(opera);
        if (!opera) {
            /* Use default selectivity (should we raise an error instead?) */
            ReleaseVariableStats(var_data);
            PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
        }
        is_gt = false;
    }

    selec = scalarineqsel(root, opera, is_gt, &var_data, const_val, const_type);

    ReleaseVariableStats(var_data);

    PG_RETURN_FLOAT8((float8)selec);
}

/*
 * pattern_sel			- Generic code for pattern-match selectivity.
 */
static double pattern_sel(PG_FUNCTION_ARGS, Pattern_Type ptype, bool negate)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);
    int var_relid = PG_GETARG_INT32(3);
    Oid collation = PG_GET_COLLATION();
    VariableStatData var_data;
    var_data.statsTuple = NULL;
    var_data.freefunc = NULL;
    var_data.var = NULL;
    var_data.rel = NULL;
    Node* other = NULL;
    bool var_on_left = true;
    Datum const_val;
    Oid const_type;
    Oid var_type;
    Oid op_family;
    Pattern_Prefix_Status pstatus;
    Const* patt = NULL;
    Const* prefix = NULL;
    Selectivity rest_selec = 0;
    double result;

    /*
     * If this is for a NOT LIKE or similar operator, get the corresponding
     * positive-match operator and work with that.	Set result to the correct
     * default estimate, too.
     */
    if (negate) {
        opera = get_negator(opera);
        if (!OidIsValid(opera)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                        errmsg("pattern_sel called for operator without a negator"))));
        }

        result = 1.0 - DEFAULT_MATCH_SEL;
    } else {
        result = DEFAULT_MATCH_SEL;
    }

    /*
     * If expression is not variable op constant, then punt and return a
     * default estimate.
     */
    if (!get_restriction_variable(root, args, var_relid, &var_data, &other, &var_on_left)) {
        return result;
    }
    if (!var_on_left || !IsA(other, Const)) {
        ReleaseVariableStats(var_data);
        return result;
    }

    /*
     * If the constant is NULL, assume operator is strict and return zero, ie,
     * operator will never return TRUE.  (It's zero even for a negator op.)
     */
    if (((Const*)other)->constisnull) {
        ReleaseVariableStats(var_data);
        return 0.0;
    }
    const_val = ((Const*)other)->constvalue;
    const_type = ((Const*)other)->consttype;

    /*
     * The right-hand const is type text or bytea for all supported operators.
     * We do not expect to see binary-compatible types here, since
     * const-folding should have relabeled the const to exactly match the
     * operator's declared type.
     */
    if (const_type != TEXTOID && const_type != BYTEAOID) {
        ReleaseVariableStats(var_data);
        return result;
    }

    /*
     * Similarly, the exposed type of the left-hand side should be one of
     * those we know.  (Do not look at var_data.atttype, which might be
     * something binary-compatible but different.)	We can use it to choose
     * the index op_family from which we must draw the comparison operators.
     *
     * NOTE: It would be more correct to use the PATTERN opfamilies than the
     * simple ones, but at the moment ANALYZE will not generate statistics for
     * the PATTERN operators.  But our results are so approximate anyway that
     * it probably hardly matters.
     */
    var_type = var_data.vartype;

    switch (var_type) {
        case TEXTOID:
            op_family = TEXT_BTREE_FAM_OID;
            break;
        case BPCHAROID:
            op_family = BPCHAR_BTREE_FAM_OID;
            break;
        case NAMEOID:
            op_family = NAME_BTREE_FAM_OID;
            break;
        case BYTEAOID:
            op_family = BYTEA_BTREE_FAM_OID;
            break;
        default:
            ReleaseVariableStats(var_data);
            return result;
    }

    /*
     * Pull out any fixed prefix implied by the pattern, and estimate the
     * fractional selectivity of the remainder of the pattern.  Unlike many of
     * the other functions in this file, we use the pattern operator's actual
     * collation for this step.  This is not because we expect the collation
     * to make a big difference in the selectivity estimate (it seldom would),
     * but because we want to be sure we cache compiled regexps under the
     * right cache key, so that they can be re-used at runtime.
     */
    patt = (Const*)other;
    pstatus = pattern_fixed_prefix(patt, ptype, collation, &prefix, &rest_selec);

    /*
     * If necessary, coerce the prefix constant to the right type.
     */
    if ((prefix != NULL) && prefix->consttype != var_type) {
        char* prefixstr = NULL;

        switch (prefix->consttype) {
            case TEXTOID:
                prefixstr = TextDatumGetCString(prefix->constvalue);
                break;
            case BYTEAOID:
                prefixstr = DatumGetCString(DirectFunctionCall1(byteaout, prefix->constvalue));
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized const_type: %u", prefix->consttype))));

                ReleaseVariableStats(var_data);
                return result;
        }
        prefix = string_to_const(prefixstr, var_type);
        pfree_ext(prefixstr);
    }

    if (pstatus == Pattern_Prefix_Exact) {
        /*
         * Pattern specifies an exact match, so pretend operator is '='
         */
        Oid eq_opr = get_opfamily_member(op_family, var_type, var_type, BTEqualStrategyNumber);

        if (eq_opr == InvalidOid) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("no = operator for op_family %u", op_family))));
        }

        result = var_eq_const(&var_data, eq_opr, prefix->constvalue, false, true);
    } else {
        /*
         * Not exact-match pattern.  If we have a sufficiently large
         * histogram, estimate selectivity for the histogram part of the
         * population by counting matches in the histogram.  If not, estimate
         * selectivity of the fixed prefix and remainder of pattern
         * separately, then combine the two to get an estimate of the
         * selectivity for the part of the column population represented by
         * the histogram.  (For small histograms, we combine these
         * approaches.)
         *
         * We then add up data for any most-common-values values; these are
         * not in the histogram population, and we can get exact answers for
         * them by applying the pattern operator, so there's no reason to
         * approximate.  (If the MCVs cover a significant part of the total
         * population, this gives us a big leg up in accuracy.)
         */
        Selectivity selec;
        int hist_size;
        FmgrInfo op_proc;
        double null_frac;
        double mcv_selec;
        double sum_common;
        double last_common;
        bool in_mcv = false;

        /* Try to use the histogram entries to get selectivity */
        fmgr_info(get_opcode(opera), &op_proc);

        selec = histogram_selectivity(&var_data, &op_proc, const_val, true, 10, 1, &hist_size);

        /* If not at least 100 entries, use the heuristic method */
        if (hist_size < 100) {
            Selectivity heur_sel;
            Selectivity prefix_sel;

            if (pstatus == Pattern_Prefix_Partial) {
                prefix_sel = prefix_selectivity(root, &var_data, var_type, op_family, prefix);
            } else {
                prefix_sel = 1.0;
            }
            heur_sel = prefix_sel * rest_selec;

            if (selec < 0) { /* fewer than 10 histogram entries? */
                selec = heur_sel;
            } else {
                /*
                 * For histogram sizes from 10 to 100, we combine the
                 * histogram and heuristic selectivities, putting increasingly
                 * more trust in the histogram for larger sizes.
                 */
                double hist_weight = hist_size / 100.0;

                selec = selec * hist_weight + heur_sel * (1.0 - hist_weight);
            }
        }

        /* In any case, don't believe extremely small or large estimates. */
        if (selec < 0.0001) {
            selec = 0.0001;
        } else if (selec > 0.9999) {
            selec = 0.9999;
        }

        /*
         * If we have most-common-values info, add up the fractions of the MCV
         * entries that satisfy MCV OP PATTERN.  These fractions contribute
         * directly to the result selectivity.	Also add up the total fraction
         * represented by MCV entries.
         */
        mcv_selec = mcv_selectivity(&var_data, &op_proc, const_val, true, &sum_common, InvalidOid,
            &in_mcv, &last_common);

        if (HeapTupleIsValid(var_data.statsTuple)) {
            null_frac = ((Form_pg_statistic)GETSTRUCT(var_data.statsTuple))->stanullfrac;
        } else {
            null_frac = 0.0;
        }

        /*
         * Now merge the results from the MCV and histogram calculations,
         * realizing that the histogram covers only the non-null values that
         * are not listed in MCV.
         */
        selec *= 1.0 - null_frac - sum_common;
        selec += mcv_selec;

        /* result should be in range, but make sure... */
        CLAMP_PROBABILITY(selec);
        result = selec;
    }

    if (prefix != NULL) {
        pfree(DatumGetPointer(prefix->constvalue));
        pfree_ext(prefix);
    }

    ReleaseVariableStats(var_data);

    return negate ? (1.0 - result) : result;
}

/*
 *		regexeqsel		- Selectivity of regular-expression pattern match.
 */
Datum regexeqsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Regex, false));
}

/*
 *		icregexeqsel	- Selectivity of case-insensitive regex match.
 */
Datum icregexeqsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Regex_IC, false));
}

/*
 *		likesel			- Selectivity of LIKE pattern match.
 */
Datum likesel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Like, false));
}

/*
 *		iclikesel			- Selectivity of ILIKE pattern match.
 */
Datum iclikesel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Like_IC, false));
}

/*
 *		regexnesel		- Selectivity of regular-expression pattern non-match.
 */
Datum regexnesel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Regex, true));
}

/*
 *		icregexnesel	- Selectivity of case-insensitive regex non-match.
 */
Datum icregexnesel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Regex_IC, true));
}

/*
 *		nlikesel		- Selectivity of LIKE pattern non-match.
 */
Datum nlikesel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Like, true));
}

/*
 *		icnlikesel		- Selectivity of ILIKE pattern non-match.
 */
Datum icnlikesel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(pattern_sel(fcinfo, Pattern_Type_Like_IC, true));
}

/*
 *		booltestsel		- Selectivity of BooleanTest Node.
 */
Selectivity booltestsel(PlannerInfo* root, BoolTestType bool_test_type, Node* arg, int var_relid, JoinType join_type,
    SpecialJoinInfo* sj_info)
{
    VariableStatData var_data;
    var_data.statsTuple = NULL;
    var_data.freefunc = NULL;
    var_data.rel = NULL;
    var_data.var = NULL;
    double selec;

    examine_variable(root, arg, var_relid, &var_data);

    if (HeapTupleIsValid(var_data.statsTuple)) {
        Form_pg_statistic stats;
        double freq_null;
        Datum* values = NULL;
        int n_values;
        float4* numbers = NULL;
        int n_numbers;

        stats = (Form_pg_statistic)GETSTRUCT(var_data.statsTuple);
        freq_null = stats->stanullfrac;

        if (get_attstatsslot(var_data.statsTuple,
                var_data.atttype,
                var_data.atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                &values,
                &n_values,
                &numbers,
                &n_numbers) &&
            n_numbers > 0) {
            double freq_true;
            double freq_false;

            /*
             * Get first MCV frequency and derive frequency for true.
             */
            if (DatumGetBool(values[0])) {
                freq_true = numbers[0];
            } else {
                freq_true = 1.0 - numbers[0] - freq_null;
            }

            /*
             * Next derive frequency for false. Then use these as appropriate
             * to derive frequency for each case.
             */
            freq_false = 1.0 - freq_true - freq_null;

            switch (bool_test_type) {
                case IS_UNKNOWN:
                    /* select only NULL values */
                    selec = freq_null;
                    break;
                case IS_NOT_UNKNOWN:
                    /* select non-NULL values */
                    selec = 1.0 - freq_null;
                    break;
                case IS_TRUE:
                    /* select only TRUE values */
                    selec = freq_true;
                    break;
                case IS_NOT_TRUE:
                    /* select non-TRUE values */
                    selec = 1.0 - freq_true;
                    break;
                case IS_FALSE:
                    /* select only FALSE values */
                    selec = freq_false;
                    break;
                case IS_NOT_FALSE:
                    /* select non-FALSE values */
                    selec = 1.0 - freq_false;
                    break;
                default:
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized bool_test_type: %d", (int)bool_test_type))));

                    selec = 0.0; /* Keep compiler quiet */
                    break;
            }

            free_attstatsslot(var_data.atttype, values, n_values, numbers, n_numbers);
        } else {
            /*
             * No most-common-value info available. Still have null fraction
             * information, so use it for IS [NOT] UNKNOWN. Otherwise adjust
             * for null fraction and assume an even split for boolean tests.
             */
            switch (bool_test_type) {
                case IS_UNKNOWN:

                    /*
                     * Use freq_null directly.
                     */
                    selec = freq_null;
                    break;
                case IS_NOT_UNKNOWN:

                    /*
                     * Select not unknown (not null) values. Calculate from
                     * freq_null.
                     */
                    selec = 1.0 - freq_null;
                    break;
                case IS_TRUE:
                case IS_NOT_TRUE:
                case IS_FALSE:
                case IS_NOT_FALSE:
                    selec = (1.0 - freq_null) / 2.0;
                    break;
                default:
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized bool_test_type: %d", (int)bool_test_type))));
                    selec = 0.0; /* Keep compiler quiet */
                    break;
            }
        }
    } else {
        /*
         * If we can't get variable statistics for the argument, perhaps
         * clause_selectivity can do something with it.  We ignore the
         * possibility of a NULL value when using clause_selectivity, and just
         * assume the value is either TRUE or FALSE.
         */
        switch (bool_test_type) {
            case IS_UNKNOWN:
                selec = DEFAULT_UNK_SEL;
                break;
            case IS_NOT_UNKNOWN:
                selec = DEFAULT_NOT_UNK_SEL;
                break;
            case IS_TRUE:
            case IS_NOT_FALSE:
                selec = (double)clause_selectivity(root, arg, var_relid, join_type, sj_info, false);
                break;
            case IS_FALSE:
            case IS_NOT_TRUE:
                selec = 1.0 - (double)clause_selectivity(root, arg, var_relid, join_type, sj_info, false);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized bool_test_type: %d", (int)bool_test_type))));
                selec = 0.0; /* Keep compiler quiet */
                break;
        }
    }

    ReleaseVariableStats(var_data);

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY(selec);

    return (Selectivity)selec;
}

/*
 *		nulltestsel		- Selectivity of NullTest Node.
 */
Selectivity nulltestsel(PlannerInfo* root, NullTestType null_test_type, Node* arg, int var_relid, JoinType join_type,
    SpecialJoinInfo* sj_info)
{
    VariableStatData var_data;
    var_data.statsTuple = NULL;
    var_data.freefunc = NULL;
    var_data.rel = NULL;
    var_data.var = NULL;
    double selec;

    examine_variable(root, arg, var_relid, &var_data);

    if (HeapTupleIsValid(var_data.statsTuple)) {
        Form_pg_statistic stats;
        double freq_null;

        stats = (Form_pg_statistic)GETSTRUCT(var_data.statsTuple);
        freq_null = stats->stanullfrac;

        switch (null_test_type) {
            case IS_NULL:

                /*
                 * Use freq_null directly.
                 */
                selec = freq_null;
                break;
            case IS_NOT_NULL:

                /*
                 * Select not unknown (not null) values. Calculate from
                 * freq_null.
                 */
                selec = 1.0 - freq_null;
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized null_test_type: %d", (int)null_test_type))));

                return (Selectivity)0; /* keep compiler quiet */
        }
    } else {
        /*
         * No ANALYZE stats available, so make a guess
         */
        switch (null_test_type) {
            case IS_NULL:
                selec = DEFAULT_UNK_SEL;
                break;
            case IS_NOT_NULL:
                selec = DEFAULT_NOT_UNK_SEL;
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized null_test_type: %d", (int)null_test_type))));
                return (Selectivity)0; /* keep compiler quiet */
        }
    }

    ReleaseVariableStats(var_data);

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY(selec);

    return (Selectivity)selec;
}

/*
 * strip_array_coercion - strip binary-compatible relabeling from an array expr
 *
 * For array values, the parser normally generates ArrayCoerceExpr conversions,
 * but it seems possible that RelabelType might show up.  Also, the planner
 * is not currently tense about collapsing stacked ArrayCoerceExpr nodes,
 * so we need to be ready to deal with more than one level.
 */
static Node* strip_array_coercion(Node* node)
{
    for (;;) {
        if (node && IsA(node, ArrayCoerceExpr) && ((ArrayCoerceExpr*)node)->elemfuncid == InvalidOid) {
            node = (Node*)((ArrayCoerceExpr*)node)->arg;
        } else if (node && IsA(node, RelabelType)) {
            /* We don't really expect this case, but may as well cope */
            node = (Node*)((RelabelType*)node)->arg;
        } else {
            break;
        }
    }
    return node;
}

/*
 *		scalararraysel		- Selectivity of ScalarArrayOpExpr Node.
 */
Selectivity scalararraysel(PlannerInfo* root, ScalarArrayOpExpr* clause, bool is_join_clause, int var_relid,
    JoinType join_type, SpecialJoinInfo* sj_info)
{
    Oid opera = clause->opno;
    bool use_or = clause->useOr;
    bool is_equality = false;
    bool is_in_equality = false;
    Node* left_op = NULL;
    Node* right_op = NULL;
    Oid nominal_element_type;
    Oid nominal_element_collation;
    TypeCacheEntry* type_entry = NULL;
    RegProcedure opr_sel;
    FmgrInfo opr_sel_proc;
    Selectivity s1;
    Selectivity s1_disjoint;

    /* First, deconstruct the expression */
    Assert(list_length(clause->args) == 2);
    left_op = (Node*)linitial(clause->args);
    right_op = (Node*)lsecond(clause->args);

    /* get nominal (after relabeling) element type of right_op */
    nominal_element_type = get_base_element_type(exprType(right_op));
    if (!OidIsValid(nominal_element_type)) {
        return (Selectivity)0.5; /* probably shouldn't happen */
    }
    /* get nominal collation, too, for generating constants */
    nominal_element_collation = exprCollation(right_op);

    /* look through any binary-compatible relabeling of right_op */
    right_op = strip_array_coercion(right_op);

    /*
     * Detect whether the operator is the default equality or inequality
     * operator of the array element type.
     */
    type_entry = lookup_type_cache(nominal_element_type, TYPECACHE_EQ_OPR);
    if (OidIsValid(type_entry->eq_opr)) {
        if (opera == type_entry->eq_opr) {
            is_equality = true;
        }
        else if (get_negator(opera) == type_entry->eq_opr) {
            is_in_equality = true;
        }
    }

    /*
     * If it is equality or inequality, we might be able to estimate this as a
     * form of array containment; for instance "const = ANY(column)" can be
     * treated as "ARRAY[const] <@ column".  scalararraysel_containment tries
     * that, and returns the selectivity estimate if successful, or -1 if not.
     */
    if ((is_equality || is_in_equality) && !is_join_clause) {
        s1 = scalararraysel_containment(root, left_op, right_op, nominal_element_type, is_equality, use_or, var_relid);
        if (s1 >= 0.0) {
            return s1;
        }
    }

    /*
     * Look up the underlying operator's selectivity estimator. Punt if it
     * hasn't got one.
     */
    if (is_join_clause) {
        opr_sel = get_oprjoin(opera);
    } else {
        opr_sel = get_oprrest(opera);
    }
    if (!opr_sel) {
        return (Selectivity)0.5;
    }
    fmgr_info(opr_sel, &opr_sel_proc);

    /*
     * In the array-containment check above, we must only believe that an
     * operator is equality or inequality if it is the default btree equality
     * operator (or its negator) for the element type, since those are the
     * operators that array containment will use.  But in what follows, we can
     * be a little laxer, and also believe that any operators using eqsel() or
     * neqsel() as selectivity estimator act like equality or inequality.
     */
    if (opr_sel == F_EQSEL || opr_sel == F_EQJOINSEL) {
        is_equality = true;
    } else if (opr_sel == F_NEQSEL || opr_sel == F_NEQJOINSEL) {
        is_in_equality = true;
    }

    /*
     * We consider three cases:
     *
     * 1. right_op is an Array constant: deconstruct the array, apply the
     * operator's selectivity function for each array element, and merge the
     * results in the same way that clausesel.c does for AND/OR combinations.
     *
     * 2. right_op is an ARRAY[] construct: apply the operator's selectivity
     * function for each element of the ARRAY[] construct, and merge.
     *
     * 3. otherwise, make a guess ...
     */
    if (right_op && IsA(right_op, Const)) {
        Datum array_datum = ((Const*)right_op)->constvalue;
        bool array_is_null = ((Const*)right_op)->constisnull;
        ArrayType* array_val = NULL;
        int16 elm_len;
        bool elm_by_val = false;
        char elm_align;
        int num_elems;
        Datum* elem_values = NULL;
        bool* elem_nulls = NULL;
        int i;

        if (array_is_null) { /* qual can't succeed if null array */
            return (Selectivity)0.0;
        }
        array_val = DatumGetArrayTypeP(array_datum);
        get_typlenbyvalalign(ARR_ELEMTYPE(array_val), &elm_len, &elm_by_val, &elm_align);
        deconstruct_array(
            array_val, ARR_ELEMTYPE(array_val), elm_len, elm_by_val, elm_align, &elem_values, &elem_nulls, &num_elems);

        /*
         * For generic operators, we assume the probability of success is
         * independent for each array element.	But for "= ANY" or "<> ALL",
         * if the array elements are distinct (which'd typically be the case)
         * then the probabilities are disjoint, and we should just sum them.
         *
         * If we were being really tense we would try to confirm that the
         * elements are all distinct, but that would be expensive and it
         * doesn't seem to be worth the cycles; it would amount to penalizing
         * well-written queries in favor of poorly-written ones.  However, we
         * do protect ourselves a little bit by checking whether the
         * disjointness assumption leads to an impossible (out of range)
         * probability; if so, we fall back to the normal calculation.
         */
        s1 = s1_disjoint = (use_or ? 0.0 : 1.0);

        for (i = 0; i < num_elems; i++) {
            List* args = NIL;
            Selectivity s2;

            args = list_make2(left_op,
                makeConst(nominal_element_type,
                    -1,
                    nominal_element_collation,
                    elm_len,
                    elem_values[i],
                    elem_nulls[i],
                    elm_by_val));
            if (is_join_clause) {
                s2 = DatumGetFloat8(FunctionCall5Coll(&opr_sel_proc,
                    clause->inputcollid,
                    PointerGetDatum(root),
                    ObjectIdGetDatum(opera),
                    PointerGetDatum(args),
                    Int16GetDatum(join_type),
                    PointerGetDatum(sj_info)));
            } else {
                s2 = DatumGetFloat8(FunctionCall4Coll(&opr_sel_proc,
                    clause->inputcollid,
                    PointerGetDatum(root),
                    ObjectIdGetDatum(opera),
                    PointerGetDatum(args),
                    Int32GetDatum(var_relid)));
            }

            if (use_or) {
                s1 = s1 + s2 - s1 * s2;
                if (is_equality) {
                    s1_disjoint += s2;
                }
            } else {
                s1 = s1 * s2;
                if (is_in_equality) {
                    s1_disjoint += s2 - 1.0;
                }
            }
        }

        /* accept disjoint-probability estimate if in range */
        if ((use_or ? is_equality : is_in_equality) && s1_disjoint >= 0.0 && s1_disjoint <= 1.0) {
            s1 = s1_disjoint;
        }

        /* free the unused buffer */
        pfree_ext(elem_nulls);
    } else if (right_op && IsA(right_op, ArrayExpr) && !((ArrayExpr*)right_op)->multidims) {
        ArrayExpr* array_expr = (ArrayExpr*)right_op;
        int16 elm_len;
        bool elm_by_val = false;
        ListCell* l = NULL;

        get_typlenbyval(array_expr->element_typeid, &elm_len, &elm_by_val);

        /*
         * We use the assumption of disjoint probabilities here too, although
         * the odds of equal array elements are rather higher if the elements
         * are not all constants (which they won't be, else constant folding
         * would have reduced the ArrayExpr to a Const).  In this path it's
         * critical to have the sanity check on the s1_disjoint estimate.
         */
        s1 = s1_disjoint = (use_or ? 0.0 : 1.0);

        foreach (l, array_expr->elements) {
            Node* elem = (Node*)lfirst(l);
            List* args = NIL;
            Selectivity s2;

            /*
             * Theoretically, if elem isn't of nominal_element_type we should
             * insert a RelabelType, but it seems unlikely that any operator
             * estimation function would really care ...
             */
            args = list_make2(left_op, elem);
            if (is_join_clause) {
                s2 = DatumGetFloat8(FunctionCall5Coll(&opr_sel_proc,
                    clause->inputcollid,
                    PointerGetDatum(root),
                    ObjectIdGetDatum(opera),
                    PointerGetDatum(args),
                    Int16GetDatum(join_type),
                    PointerGetDatum(sj_info)));
            } else {
                s2 = DatumGetFloat8(FunctionCall4Coll(&opr_sel_proc,
                    clause->inputcollid,
                    PointerGetDatum(root),
                    ObjectIdGetDatum(opera),
                    PointerGetDatum(args),
                    Int32GetDatum(var_relid)));
            }

            if (use_or) {
                s1 = s1 + s2 - s1 * s2;
                if (is_equality) {
                    s1_disjoint += s2;
                }
            } else {
                s1 = s1 * s2;
                if (is_in_equality) {
                    s1_disjoint += s2 - 1.0;
                }
            }
        }

        /* accept disjoint-probability estimate if in range */
        if ((use_or ? is_equality : is_in_equality) && s1_disjoint >= 0.0 && s1_disjoint <= 1.0) {
            s1 = s1_disjoint;
        }
    } else {
        CaseTestExpr* dummy_expr = NULL;
        List* args = NIL;
        Selectivity s2;
        int i;

        /*
         * We need a dummy right_op to pass to the operator selectivity
         * routine.  It can be pretty much anything that doesn't look like a
         * constant; CaseTestExpr is a convenient choice.
         */
        dummy_expr = makeNode(CaseTestExpr);
        dummy_expr->typeId = nominal_element_type;
        dummy_expr->typeMod = -1;
        dummy_expr->collation = clause->inputcollid;
        args = list_make2(left_op, dummy_expr);
        if (is_join_clause) {
            s2 = DatumGetFloat8(FunctionCall5Coll(&opr_sel_proc,
                clause->inputcollid,
                PointerGetDatum(root),
                ObjectIdGetDatum(opera),
                PointerGetDatum(args),
                Int16GetDatum(join_type),
                PointerGetDatum(sj_info)));
        } else {
            s2 = DatumGetFloat8(FunctionCall4Coll(&opr_sel_proc,
                clause->inputcollid,
                PointerGetDatum(root),
                ObjectIdGetDatum(opera),
                PointerGetDatum(args),
                Int32GetDatum(var_relid)));
        }
        s1 = use_or ? 0.0 : 1.0;

        /*
         * Arbitrarily assume 10 elements in the eventual array value (see
         * also estimate_array_length).  We don't risk an assumption of
         * disjoint probabilities here.
         */
        for (i = 0; i < 10; i++) {
            if (use_or) {
                s1 = s1 + s2 - s1 * s2;
            } else {
                s1 = s1 * s2;
            }
        }
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY(s1);

    return s1;
}

/*
 * Estimate number of elements in the array yielded by an expression.
 *
 * It's important that this agree with scalararraysel.
 */
int estimate_array_length(Node* array_expr)
{
    /* look through any binary-compatible relabeling of array_expr */
    array_expr = strip_array_coercion(array_expr);

    if (array_expr && IsA(array_expr, Const)) {
        Datum array_datum = ((Const*)array_expr)->constvalue;
        bool array_is_null = ((Const*)array_expr)->constisnull;
        ArrayType* array_val = NULL;

        if (array_is_null) {
            return 0;
        }
        array_val = DatumGetArrayTypeP(array_datum);
        return ArrayGetNItems(ARR_NDIM(array_val), ARR_DIMS(array_val));
    } else if (array_expr && IsA(array_expr, ArrayExpr) && !((ArrayExpr*)array_expr)->multidims) {
        return list_length(((ArrayExpr*)array_expr)->elements);
    } else {
        /* default guess --- see also scalararraysel */
        return 10;
    }
}

/*
 *		rowcomparesel		- Selectivity of RowCompareExpr Node.
 *
 * We estimate RowCompare selectivity by considering just the first (high
 * order) columns, which makes it equivalent to an ordinary OpExpr.  While
 * this estimate could be refined by considering additional columns, it
 * seems unlikely that we could do a lot better without multi-column
 * statistics.
 */
Selectivity rowcomparesel(
    PlannerInfo* root, RowCompareExpr* clause, int var_relid, JoinType join_type, SpecialJoinInfo* sj_info)
{
    Selectivity s1;
    Oid op_no = linitial_oid(clause->opnos);
    Oid input_collid = linitial_oid(clause->inputcollids);
    List* op_args = NIL;
    bool is_join_clause = false;

    /* Build equivalent arg list for single operator */
    op_args = list_make2(linitial(clause->largs), linitial(clause->rargs));

    /*
     * Decide if it's a join clause.  This should match clausesel.c's
     * treat_as_join_clause(), except that we intentionally consider only the
     * leading columns and not the rest of the clause.
     */
    if (var_relid != 0) {
        /*
         * Caller is forcing restriction mode (eg, because we are examining an
         * inner indexscan qual).
         */
        is_join_clause = false;
    } else if (sj_info == NULL) {
        /*
         * It must be a restriction clause, since it's being evaluated at a
         * scan node.
         */
        is_join_clause = false;
    } else {
        /*
         * Otherwise, it's a join if there's more than one relation used.
         */
        is_join_clause = (NumRelids((Node*)op_args) > 1);
    }

    if (is_join_clause) {
        /* Estimate selectivity for a join clause. */
        s1 = join_selectivity(root, op_no, op_args, input_collid, join_type, sj_info);
    } else {
        /* Estimate selectivity for a restriction clause. */
        s1 = restriction_selectivity(root, op_no, op_args, input_collid, var_relid);
    }

    return s1;
}

/*
 *		eqjoinsel		- Join selectivity of "="
 */
Datum eqjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);

#ifdef NOT_USED
    JoinType join_type = (JoinType)PG_GETARG_INT16(3);
#endif
    SpecialJoinInfo* sj_info = (SpecialJoinInfo*)PG_GETARG_POINTER(4);
    double selec;
    VariableStatData var_data1;
    VariableStatData var_data2;
    bool join_is_reversed = false;
    RelOptInfo* inner_rel = NULL;

    get_join_variables(root, args, sj_info, &var_data1, &var_data2, &join_is_reversed);

    switch (sj_info->jointype) {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_FULL:
            selec = eqjoinsel_inner(opera, &var_data1, &var_data2, sj_info);
            break;
        case JOIN_SEMI:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:

            /*
             * Look up the join's inner relation.  min_righthand is sufficient
             * information because neither SEMI nor ANTI joins permit any
             * reassociation into or out of their RHS, so the righthand will
             * always be exactly that set of rels.
             */
            inner_rel = find_join_input_rel(root, sj_info->min_righthand);

            if (!join_is_reversed) {
                selec = eqjoinsel_semi(opera, &var_data1, &var_data2, inner_rel, sj_info);
            } else {
                selec = eqjoinsel_semi(get_commutator(opera), &var_data2, &var_data1, inner_rel, sj_info);
            }
            break;
        default:
            /* other values not expected here */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized join type: %d", (int)sj_info->jointype))));
            selec = 0; /* keep compiler quiet */
            break;
    }

    ReleaseVariableStats(var_data1);
    ReleaseVariableStats(var_data2);

    CLAMP_PROBABILITY(selec);

    PG_RETURN_FLOAT8((float8)selec);
}

/*
 * @Description: Set equal var ratio which will be append to RelOptInfo's vareqratio.
 * @in vardata: var data info.
 * @in ratio: join ratio according to estimation.
 * @in sjinfo: the join info for current relation join with others.
 */
void set_equal_varratio(VariableStatData* var_data, Relids other_relids, double ratio, SpecialJoinInfo* sj_info)
{
    Node* var = var_data->var;
    MemoryContext cxt = NULL;
    Relids join_relids = NULL;
    VarEqRatio* var_eq_ratio = NULL;

    Assert(IsA(var, Var));

    if (sj_info == NULL || ratio == 0.0) {
        return;
    }

    cxt = MemoryContextSwitchTo(MemoryContextOriginal((char*)var_data->rel));

    join_relids = bms_union(var_data->rel->relids, other_relids);

    ListCell* lc = NULL;
    foreach (lc, var_data->rel->varEqRatio) {
        var_eq_ratio = (VarEqRatio*)lfirst(lc);

        /* we need not save if it have the same joinrel and joinratio, otherwise, we only save the minimum joinratio. */
        if (bms_equal(var_eq_ratio->joinrelids, join_relids) && _equalSimpleVar(var, var_eq_ratio->var)) {
            /* save the minimue ratio. */
            if (ratio < var_eq_ratio->ratio) {
                var_eq_ratio->ratio = ratio;
            }

            bms_free(join_relids);
            join_relids = NULL;
            return;
        }
    }

    var_eq_ratio = (VarEqRatio*)palloc0(sizeof(VarEqRatio));
    var_eq_ratio->var = (Var*)var;
    var_eq_ratio->ratio = ratio;

    var_eq_ratio->joinrelids = join_relids;

    var_data->rel->varEqRatio = lappend(var_data->rel->varEqRatio, var_eq_ratio);
    (void)MemoryContextSwitchTo(cxt);
}

/*
 * eqjoinsel_inner --- eqjoinsel for normal inner join
 *
 * We also use this for LEFT/FULL outer joins; it's not presently clear
 * that it's worth trying to distinguish them here.
 */
static double eqjoinsel_inner(
    Oid opera, VariableStatData* var_data1, VariableStatData* var_data2, SpecialJoinInfo* sj_info)
{
    double selec;
    double nd1;
    double nd2;
    bool is_default1 = false;
    bool is_default2 = false;
    Oid opfuncoid;
    Form_pg_statistic stats1 = NULL;
    Form_pg_statistic stats2 = NULL;
    bool have_mcvs1 = false;
    Datum* values1 = NULL;
    /*
     * 'nvalues1' once set, can not be altered later
     * because 'nvalues1' will be used to free values[] array
     * we add a new varaiable 'nvalues1_frac' so you can do the calculation
     * this is the same for nvalues2 and nvalues2_frac
     */
    int n_values1 = 0;
    double n_values1_frac = 0.0;
    float4* numbers1 = NULL;
    int n_numbers1 = 0;
    bool have_mcvs2 = false;
    Datum* values2 = NULL;
    int n_values2 = 0;
    double nvalues2_frac = 0.0;
    float4* numbers2 = NULL;
    int n_numbers2 = 0;
    double rel_frac1 = 1.0;
    double rel_frac2 = 1.0;
    double rel_frac;

    nd1 = get_variable_numdistinct(
        var_data1, &is_default1, true, get_join_ratio(var_data1, sj_info), sj_info, STATS_TYPE_GLOBAL);
    nd2 = get_variable_numdistinct(
        var_data2, &is_default2, true, get_join_ratio(var_data2, sj_info), sj_info, STATS_TYPE_GLOBAL);

    opfuncoid = get_opcode(opera);

    if (HeapTupleIsValid(var_data1->statsTuple)) {
        /* note we allow use of nullfrac regardless of security check */
        stats1 = (Form_pg_statistic)GETSTRUCT(var_data1->statsTuple);
        if (StatisticProcSecurityCheck(var_data1, opfuncoid)) {
            have_mcvs1 = get_attstatsslot(var_data1->statsTuple,
                var_data1->atttype,
                var_data1->atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                &values1,
                &n_values1,
                &numbers1,
                &n_numbers1);
        }
        if (var_data1->rel != NULL) {
            rel_frac1 = var_data1->rel->rows / var_data1->rel->tuples;
            /* force rel_frac to be within range of [0, 1] */
            CLAMP_PROBABILITY(rel_frac1);
        }
    }

    if (HeapTupleIsValid(var_data2->statsTuple)) {
        /* note we allow use of nullfrac regardless of security check */
        stats2 = (Form_pg_statistic)GETSTRUCT(var_data2->statsTuple);
        if (StatisticProcSecurityCheck(var_data2, opfuncoid)) {
            have_mcvs2 = get_attstatsslot(var_data2->statsTuple,
                var_data2->atttype,
                var_data2->atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                &values2,
                &n_values2,
                &numbers2,
                &n_numbers2);
        }
        if (var_data2->rel != NULL) {
            rel_frac2 = var_data2->rel->rows / var_data2->rel->tuples;
            /* force rel_frac to be within range of [0, 1] */
            CLAMP_PROBABILITY(rel_frac2);
        }
    }
    rel_frac = rel_frac1 * rel_frac2;

    if (have_mcvs1 && have_mcvs2) {
        /*
         * We have most-common-value lists for both relations.	Run through
         * the lists to see which MCVs actually join to each other with the
         * given operator.	This allows us to determine the exact join
         * selectivity for the portion of the relations represented by the MCV
         * lists.  We still have to estimate for the remaining population, but
         * in a skewed distribution this gives us a big leg up in accuracy.
         * For motivation see the analysis in Y. Ioannidis and S.
         * Christodoulakis, "On the propagation of errors in the size of join
         * results", Technical Report 1018, Computer Science Dept., University
         * of Wisconsin, Madison, March 1991 (available from ftp.cs.wisc.edu).
         */
        FmgrInfo eq_proc;
        bool* has_match1 = NULL;
        bool* has_match2 = NULL;
        double null_frac1 = stats1->stanullfrac;
        double null_frac2 = stats2->stanullfrac;
        double match_prod_freq, match_freq1, match_freq2, unmatch_freq1, unmatch_freq2, other_freq1, other_freq2,
            total_sel1, total_sel2;
        int i, n_matches;
        double tmp_nmatches;

        fmgr_info(opfuncoid, &eq_proc);
        has_match1 = (bool*)palloc0(n_values1 * sizeof(bool));
        has_match2 = (bool*)palloc0(n_values2 * sizeof(bool));

        /*
         * Note we assume that each MCV will match at most one member of the
         * other MCV list.	If the operator isn't really equality, there could
         * be multiple matches --- but we don't look for them, both for speed
         * and because the math wouldn't add up...
         */
        match_prod_freq = 0.0;
        n_matches = 0;
        for (i = 0; i < n_values1; i++) {
            int j;

            for (j = 0; j < n_values2; j++) {
                if (has_match2[j]) {
                    continue;
                }
                if (DatumGetBool(FunctionCall2Coll(&eq_proc, DEFAULT_COLLATION_OID, values1[i], values2[j]))) {
                    has_match1[i] = has_match2[j] = true;
                    match_prod_freq += numbers1[i] * numbers2[j];
                    n_matches++;
                    break;
                }
            }
        }
        /* adjust match freq according to relation's filter fraction */
        tmp_nmatches = (double)n_matches * rel_frac;
        if (n_matches != 0) {
            match_prod_freq *= rel_frac;
        }
        CLAMP_PROBABILITY(match_prod_freq);
        /* Sum up frequencies of matched and unmatched MCVs */
        match_freq1 = unmatch_freq1 = 0.0;
        for (i = 0; i < n_values1; i++) {
            if (has_match1[i]) {
                match_freq1 += numbers1[i];
            } else {
                unmatch_freq1 += numbers1[i];
            }
        }
        CLAMP_PROBABILITY(match_freq1);
        CLAMP_PROBABILITY(unmatch_freq1);
        match_freq2 = unmatch_freq2 = 0.0;
        for (i = 0; i < n_values2; i++) {
            if (has_match2[i]) {
                match_freq2 += numbers2[i];
            } else {
                unmatch_freq2 += numbers2[i];
            }
        }
        CLAMP_PROBABILITY(match_freq2);
        CLAMP_PROBABILITY(unmatch_freq2);
        pfree_ext(has_match1);
        pfree_ext(has_match2);

        /*
         * Compute total frequency of non-null values that are not in the MCV
         * lists.
         */
        other_freq1 = 1.0 - null_frac1 - match_freq1 - unmatch_freq1;
        other_freq2 = 1.0 - null_frac2 - match_freq2 - unmatch_freq2;
        CLAMP_PROBABILITY(other_freq1);
        CLAMP_PROBABILITY(other_freq2);

        /*
         * We can estimate the total selectivity from the point of view of
         * relation 1 as: the known selectivity for matched MCVs, plus
         * unmatched MCVs that are assumed to match against random members of
         * relation 2's non-MCV population, plus non-MCV values that are
         * assumed to match against random members of relation 2's unmatched
         * MCVs plus non-MCV values.
         */
        n_values1_frac = (double)n_values1 * rel_frac1;
        nvalues2_frac = (double)n_values2 * rel_frac2;
        total_sel1 = match_prod_freq;
        if (nd2 > nvalues2_frac) {
            total_sel1 += unmatch_freq1 * other_freq2 / (nd2 - nvalues2_frac) * rel_frac;
        }
        if (nd2 > tmp_nmatches) {
            total_sel1 += other_freq1 * (other_freq2 + unmatch_freq2) / (nd2 - tmp_nmatches) * rel_frac;
        }
        /* Same estimate from the point of view of relation 2. */
        total_sel2 = match_prod_freq;
        if (nd1 > n_values1_frac) {
            total_sel2 += unmatch_freq2 * other_freq1 / (nd1 - n_values1_frac) * rel_frac;
        }
        if (nd1 > tmp_nmatches) {
            total_sel2 += other_freq2 * (other_freq1 + unmatch_freq1) / (nd1 - tmp_nmatches) * rel_frac;
        }

        /*
         * Use the smaller of the two estimates.  This can be justified in
         * essentially the same terms as given below for the no-stats case: to
         * a first approximation, we are estimating from the point of view of
         * the relation with smaller nd.
         */
        if (rel_frac == 0) {
            selec = 0;
        } else {
            selec = (total_sel1 < total_sel2) ? (total_sel1 / rel_frac) : (total_sel2 / rel_frac);
        }
        /*
         * calculate join ratio for both two tables, admitting that smaller distinct
         * values will be all joined out
         */
        double join_ratio1 = 0.0;
        double join_ratio2 = 0.0;

        if (n_matches != 0 && rel_frac1 != 0 && rel_frac2 != 0) {
            join_ratio1 = match_freq1 * tmp_nmatches / (n_matches * rel_frac1);
            join_ratio2 = match_freq2 * tmp_nmatches / (n_matches * rel_frac2);
        }

        if (nd1 > nd2) {
            if (nd1 != tmp_nmatches) {
                join_ratio1 += other_freq1 * (nd2 - tmp_nmatches) / (nd1 - tmp_nmatches);
            }
            join_ratio2 += other_freq2;
        } else if (nd1 < nd2) {
            if (nd2 != tmp_nmatches) {
                join_ratio2 += other_freq2 * (nd1 - tmp_nmatches) / (nd2 - tmp_nmatches);
            }
            join_ratio1 += other_freq1;
        }
        CLAMP_PROBABILITY(join_ratio1);
        CLAMP_PROBABILITY(join_ratio2);
        set_varratio_after_calc_selectivity(var_data1, RatioType_Join, join_ratio1, sj_info);
        set_varratio_after_calc_selectivity(var_data2, RatioType_Join, join_ratio2, sj_info);

        /* Bloom filter can be used only when var = var. */
        if (IsA(var_data1->var, Var) && IsA(var_data2->var, Var)) {
            /* Set var's ratio which will be used by bloom filter set. */
            if ((var_data2->rel != NULL) && (var_data1->rel != NULL)) {
                set_equal_varratio(var_data1, var_data2->rel->relids, join_ratio1, sj_info);
                set_equal_varratio(var_data2, var_data1->rel->relids, join_ratio2, sj_info);
            }
        }
    } else {
        /*
         * We do not have MCV lists for both sides.  Estimate the join
         * selectivity as MIN(1/nd1,1/nd2)*(1-nullfrac1)*(1-nullfrac2). This
         * is plausible if we assume that the join operator is strict and the
         * non-null values are about equally distributed: a given non-null
         * tuple of rel1 will join to either zero or N2*(1-nullfrac2)/nd2 rows
         * of rel2, so total join rows are at most
         * N1*(1-nullfrac1)*N2*(1-nullfrac2)/nd2 giving a join selectivity of
         * not more than (1-nullfrac1)*(1-nullfrac2)/nd2. By the same logic it
         * is not more than (1-nullfrac1)*(1-nullfrac2)/nd1, so the expression
         * with MIN() is an upper bound.  Using the MIN() means we estimate
         * from the point of view of the relation with smaller nd (since the
         * larger nd is determining the MIN).  It is reasonable to assume that
         * most tuples in this rel will have join partners, so the bound is
         * probably reasonably tight and should be taken as-is.
         *
         * XXX Can we be smarter if we have an MCV list for just one side? It
         * seems that if we assume equal distribution for the other side, we
         * end up with the same answer anyway.
         */
        double null_frac1 = stats1 ? stats1->stanullfrac : 0.0;
        double null_frac2 = stats2 ? stats2->stanullfrac : 0.0;
        selec = (1.0 - null_frac1) * (1.0 - null_frac2);
        if (nd1 > nd2) {
            selec /= nd1;

            /* we can cache the var ratio only if both var1 and var2 have distinct value. */
            if (!is_default1 && !is_default2) {
                double ratio1 = nd2 / nd1 * (1.0 - null_frac1);

                set_varratio_after_calc_selectivity(var_data1, RatioType_Join, ratio1, sj_info);

                if (IsA(var_data1->var, Var) && IsA(var_data2->var, Var)) {
                    /* Set var's ratio which will be used by bloom filter set. */
                    set_equal_varratio(var_data1, var_data2->rel->relids, ratio1, sj_info);
                }
            }
        } else {
            selec /= nd2;

            /* we can cache the var ratio only if both var1 and var2 have distinct value. */
            if (!is_default1 && !is_default2) {
                double ratio2 = nd1 / nd2 * (1.0 - null_frac2);

                set_varratio_after_calc_selectivity(var_data2, RatioType_Join, ratio2, sj_info);

                if (IsA(var_data1->var, Var) && IsA(var_data2->var, Var)) {
                    set_equal_varratio(var_data2, var_data1->rel->relids, ratio2, sj_info);
                }
            }
        }
    }

    if (have_mcvs1) {
        free_attstatsslot(var_data1->atttype, values1, n_values1, numbers1, n_numbers1);
    }
    if (have_mcvs2) {
        free_attstatsslot(var_data2->atttype, values2, n_values2, numbers2, n_numbers2);
    }

    return selec;
}

/*
 * eqjoinsel_semi --- eqjoinsel for semi join
 *
 * (Also used for anti join, which we are supposed to estimate the same way.)
 * Caller has ensured that vardata1 is the LHS variable.
 * Unlike eqjoinsel_inner, we have to cope with operator being InvalidOid.
 */
static double eqjoinsel_semi(Oid opera, VariableStatData* var_data1, VariableStatData* var_data2, RelOptInfo* inner_rel,
    SpecialJoinInfo* sj_info)
{
    double selec;
    double nd1;
    double nd2;
    bool is_default1 = false;
    bool is_default2 = false;
    Oid opfuncoid;
    Form_pg_statistic stats1 = NULL;
    bool have_mcvs1 = false;
    Datum* values1 = NULL;
    int n_values1 = 0;
    float4* numbers1 = NULL;
    int n_numbers1 = 0;
    bool have_mcvs2 = false;
    Datum* values2 = NULL;
    int n_values2 = 0;
    float4* numbers2 = NULL;
    int n_numbers2 = 0;

    nd1 = get_variable_numdistinct(
        var_data1, &is_default1, true, get_join_ratio(var_data1, sj_info), sj_info, STATS_TYPE_GLOBAL);
    nd2 = get_variable_numdistinct(
        var_data2, &is_default2, true, get_join_ratio(var_data2, sj_info), sj_info, STATS_TYPE_GLOBAL);

    opfuncoid = OidIsValid(opera) ? get_opcode(opera) : InvalidOid;

    /*
     * We clamp nd2 to be not more than what we estimate the inner relation's
     * size to be.	This is intuitively somewhat reasonable since obviously
     * there can't be more than that many distinct values coming from the
     * inner rel.  The reason for the asymmetry (ie, that we don't clamp nd1
     * likewise) is that this is the only pathway by which restriction clauses
     * applied to the inner rel will affect the join result size estimate,
     * since set_joinrel_size_estimates will multiply SEMI/ANTI selectivity by
     * only the outer rel's size.  If we clamped nd1 we'd be double-counting
     * the selectivity of outer-rel restrictions.
     *
     * We can apply this clamping both with respect to the base relation from
     * which the join variable comes (if there is just one), and to the
     * immediate inner input relation of the current join.
     */
    if (var_data2->rel) {
        nd2 = Min(nd2, var_data2->rel->rows);
    }
    nd2 = Min(nd2, inner_rel->rows);

    if (HeapTupleIsValid(var_data1->statsTuple)) {
        stats1 = (Form_pg_statistic)GETSTRUCT(var_data1->statsTuple);
        if (StatisticProcSecurityCheck(var_data1, opfuncoid)) {
            have_mcvs1 = get_attstatsslot(var_data1->statsTuple,
                var_data1->atttype,
                var_data1->atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                &values1,
                &n_values1,
                &numbers1,
                &n_numbers1);
        }
    }

    if (HeapTupleIsValid(var_data2->statsTuple) && StatisticProcSecurityCheck(var_data2, opfuncoid)) {
        have_mcvs2 = get_attstatsslot(var_data2->statsTuple,
            var_data2->atttype,
            var_data2->atttypmod,
            STATISTIC_KIND_MCV,
            InvalidOid,
            NULL,
            &values2,
            &n_values2,
            &numbers2,
            &n_numbers2);
    }

    if (have_mcvs1 && have_mcvs2 && OidIsValid(opera)) {
        /*
         * We have most-common-value lists for both relations.	Run through
         * the lists to see which MCVs actually join to each other with the
         * given operator.	This allows us to determine the exact join
         * selectivity for the portion of the relations represented by the MCV
         * lists.  We still have to estimate for the remaining population, but
         * in a skewed distribution this gives us a big leg up in accuracy.
         */
        FmgrInfo eq_proc;
        bool* has_match1 = NULL;
        bool* has_match2 = NULL;
        double null_frac1 = stats1->stanullfrac;
        double match_freq1;
        double uncertain_frac;
        double uncertain;
        int i;
        int n_matches;
        int clamped_nvalues2;

        /*
         * The clamping above could have resulted in nd2 being less than
         * n_values2; in which case, we assume that precisely the nd2 most
         * common values in the relation will appear in the join input, and so
         * compare to only the first nd2 members of the MCV list.  Of course
         * this is frequently wrong, but it's the best bet we can make.
         */
        clamped_nvalues2 = Min(n_values2, nd2);

        fmgr_info(opfuncoid, &eq_proc);
        has_match1 = (bool*)palloc0(n_values1 * sizeof(bool));
        has_match2 = (bool*)palloc0(clamped_nvalues2 * sizeof(bool));

        /*
         * Note we assume that each MCV will match at most one member of the
         * other MCV list.	If the operator isn't really equality, there could
         * be multiple matches --- but we don't look for them, both for speed
         * and because the math wouldn't add up...
         */
        n_matches = 0;
        for (i = 0; i < n_values1; i++) {
            int j;

            for (j = 0; j < clamped_nvalues2; j++) {
                if (has_match2[j]) {
                    continue;
                }
                if (DatumGetBool(FunctionCall2Coll(&eq_proc, DEFAULT_COLLATION_OID, values1[i], values2[j]))) {
                    has_match1[i] = has_match2[j] = true;
                    n_matches++;
                    break;
                }
            }
        }
        /* Sum up frequencies of matched MCVs */
        match_freq1 = 0.0;
        for (i = 0; i < n_values1; i++) {
            if (has_match1[i]) {
                match_freq1 += numbers1[i];
            }
        }
        CLAMP_PROBABILITY(match_freq1);
        pfree_ext(has_match1);
        pfree_ext(has_match2);

        /*
         * Now we need to estimate the fraction of relation 1 that has at
         * least one join partner.	We know for certain that the matched MCVs
         * do, so that gives us a lower bound, but we're really in the dark
         * about everything else.  Our crude approach is: if nd1 <= nd2 then
         * assume all non-null rel1 rows have join partners, else assume for
         * the uncertain rows that a fraction nd2/nd1 have join partners. We
         * can discount the known-matched MCVs from the distinct-values counts
         * before doing the division.
         *
         * Crude as the above is, it's completely useless if we don't have
         * reliable ndistinct values for both sides.  Hence, if either nd1 or
         * nd2 is default, punt and assume half of the uncertain rows have
         * join partners.
         */
        if (!is_default1 && !is_default2) {
            nd1 -= n_matches;
            nd2 -= n_matches;
            if (nd1 <= nd2 || nd2 < 0) {
                uncertain_frac = 1.0;
            } else {
                uncertain_frac = nd2 / nd1;
            }
        } else {
            uncertain_frac = 0.5;
        }
        uncertain = 1.0 - match_freq1 - null_frac1;
        CLAMP_PROBABILITY(uncertain);
        selec = match_freq1 + uncertain_frac * uncertain;
    } else {
        /*
         * Without MCV lists for both sides, we can only use the heuristic
         * about nd1 vs nd2.
         */
        double null_frac1 = stats1 ? stats1->stanullfrac : 0.0;

        if (!is_default1 && !is_default2) {
            if (nd1 <= nd2 || nd2 < 0) {
                selec = 1.0 - null_frac1;
            } else {
                selec = (nd2 / nd1) * (1.0 - null_frac1);
            }
        } else if (!is_default1 && is_default2 && var_data2->rel) {
            selec = (var_data2->rel->rows / nd1) * (1.0 - null_frac1);
            if (selec > 0.5 * (1.0 - null_frac1)) {
                selec = 0.5 * (1.0 - null_frac1);
            }
        } else {
            selec = 0.5 * (1.0 - null_frac1);
        }
    }

    if (IsA(var_data1->var, Var) && IsA(var_data2->var, Var) && var_data2->rel) {
        /* Set var's ratio which will be used by bloom filter set. */
        set_equal_varratio(var_data1, var_data2->rel->relids, selec, sj_info);
    }

    if (have_mcvs1) {
        free_attstatsslot(var_data1->atttype, values1, n_values1, numbers1, n_numbers1);
    }
    if (have_mcvs2) {
        free_attstatsslot(var_data2->atttype, values2, n_values2, numbers2, n_numbers2);
    }

    return selec;
}

/*
 * neqjoinsel_semi --- neqjoinsel for semi join
 * For semi-joins, if there is more than one distinct value in the RHS
 * relation then every non-null LHS row must find a row to join since
 * it can only be equal to one of them.
 * we could have special cases for empty RHS
 * (selectivity = 0) and single-distinct-value RHS (selectivity =
 * fraction of LHS that has the same value as the single RHS value).
 * (Also used for anti join, which we are supposed to estimate the same way.)
 * Caller has ensured that vardata1 is the LHS variable.
 */
static double neqjoinsel_semi(Oid opera, VariableStatData* var_data1, VariableStatData* var_data2,
    RelOptInfo* inner_rel, SpecialJoinInfo* sj_info)
{
    double nd1;
    double nd2;
    double selec;
    bool is_default1 = false;
    bool is_default2 = false;
    Form_pg_statistic stats1 = NULL;
    double null_frac1;

    if (HeapTupleIsValid(var_data1->statsTuple)) {
        stats1 = (Form_pg_statistic)GETSTRUCT(var_data1->statsTuple);
    }

    null_frac1 = stats1 ? stats1->stanullfrac : 0.0;
    nd1 = get_variable_numdistinct(
        var_data1, &is_default1, true, get_join_ratio(var_data1, sj_info), sj_info, STATS_TYPE_GLOBAL);
    nd2 = get_variable_numdistinct(
        var_data2, &is_default2, true, get_join_ratio(var_data2, sj_info), sj_info, STATS_TYPE_GLOBAL);

    /*
     * When one of the sides contains zero distinct values, the selectivity is zero;
     * when there is more than 1 distinct value in the rhs, it is assumed that all
     * distinct values are selected by neq semi join, a distinct value on lfs
     * can only match at most one value on the rhs, so the exist not equal condition
     * can always be fulfilled;
     * when there is only one distinct value on the rhs and more than one on the lfs,
     * it is assumed that the one distinct value on rhs can filter out 1/nd1 on the
     * lfs;
     * when there is only 1 distinct value on both sides, we use 0.5 as an
     * approximation, it is resonable to assume that there is 50% of chance that the
     * two distinct values one both ends can match.
     * nd less than 1 is considered as 1, which might need further analysis.
     */
    selec = 1 - null_frac1;
    if (nd1 == 0 || nd2 == 0) {
        PG_RETURN_FLOAT8((float8)0);
    } else if (nd2 > 1) {
        PG_RETURN_FLOAT8((float8)selec);
    } else if (nd1 > 1) {
        selec *= (nd1 - 1) / nd1;
    } else {
        selec *= 0.5;
    }
    PG_RETURN_FLOAT8((float8)selec);
}

/*
 *		neqjoinsel		- Join selectivity of "!="
 */
Datum neqjoinsel(PG_FUNCTION_ARGS)
{
    /* Instead of calculate the neq join selectiveity as 1 - eq_join_sel, we can calculate the neq probability
     * for each distinct value and average them out. This yields a more accurate estimation of selectivity,
     * especially in the case of self-join. We want to compare this model with the original model for other situations
     * before we settle on this model
     * Please note that cost_param=1 option used to provide an alternative way for neqjoin selectivity computation,
     * which is deprecated on 2019-02-18. Please go back in time to check if interested.
     */
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    Oid opera = PG_GETARG_OID(1);
    List* args = (List*)PG_GETARG_POINTER(2);

#ifdef NOT_USED
    JoinType join_type = (JoinType)PG_GETARG_INT16(3);
#endif
    SpecialJoinInfo* sj_info = (SpecialJoinInfo*)PG_GETARG_POINTER(4);
    double selec = 0.0;
    VariableStatData var_data1;
    VariableStatData var_data2;
    bool join_is_reversed = false;
    RelOptInfo* inner_rel = NULL;
    Oid eq_op;

    get_join_variables(root, args, sj_info, &var_data1, &var_data2, &join_is_reversed);

    eq_op = get_negator(opera);
    if (eq_op) {
        switch (sj_info->jointype) {
            case JOIN_INNER:
            case JOIN_LEFT:
            case JOIN_FULL:
                /*
                 * We want 1 - eqjoinsel() where the equality operator is the one
                 * associated with this != operator, that is, its negator.
                 */
                eq_op = get_negator(opera);
                if (eq_op) {
                    selec = DatumGetFloat8(DirectFunctionCall5(eqjoinsel,
                        PointerGetDatum(root),
                        ObjectIdGetDatum(eq_op),
                        PointerGetDatum(args),
                        Int16GetDatum(sj_info->jointype),
                        PointerGetDatum(sj_info)));
                    selec = 1 - selec;
                } else {
                    /* Use default selectivity (should we raise an error instead?) */
                    selec = 1 - DEFAULT_EQ_SEL;
                }
                break;
            case JOIN_SEMI:
            case JOIN_ANTI:
            case JOIN_LEFT_ANTI_FULL:
                /*
                 * Look up the join's inner relation.  min_righthand is sufficient
                 * information because neither SEMI nor ANTI joins permit any
                 * reassociation into or out of their RHS, so the righthand will
                 * always be exactly that set of rels.
                 */
                inner_rel = find_join_input_rel(root, sj_info->min_righthand);

                if (!join_is_reversed) {
                    selec = neqjoinsel_semi(eq_op, &var_data1, &var_data2, inner_rel, sj_info);
                } else {
                    selec = neqjoinsel_semi(get_commutator(eq_op), &var_data2, &var_data1, inner_rel, sj_info);
                }
                /*
                 * We clamp the selectivity of neqsemijoin to be not more than 0.995
                 * to avoid destructive underestimation for the corresponded antijoin.
                 */
                if (selec > MAX_NEQ_SEMI_SEL) {
                    selec = MAX_NEQ_SEMI_SEL;
                }
                break;
            default:
                /* other values not expected here */
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized join type: %d", (int)sj_info->jointype))));
                break;
        }
    } else {
        /* Use default selectivity (should we raise an error instead?) */
        selec = 1 - DEFAULT_EQ_SEL;
    }
    ReleaseVariableStats(var_data1);
    ReleaseVariableStats(var_data2);

    CLAMP_PROBABILITY(selec);

    PG_RETURN_FLOAT8((float8)selec);
}

/*
 *		scalarltjoinsel - Join selectivity of "<" and "<=" for scalars
 */
Datum scalarltjoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
}

/*
 *		scalargtjoinsel - Join selectivity of ">" and ">=" for scalars
 */
Datum scalargtjoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
}

/*
 * patternjoinsel		- Generic code for pattern-match join selectivity.
 */
static double patternjoinsel(PG_FUNCTION_ARGS, Pattern_Type ptype, bool negate)
{
    /* For the moment we just punt. */
    return negate ? (1.0 - DEFAULT_MATCH_SEL) : DEFAULT_MATCH_SEL;
}

/*
 *		regexeqjoinsel	- Join selectivity of regular-expression pattern match.
 */
Datum regexeqjoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Regex, false));
}

/*
 *		icregexeqjoinsel	- Join selectivity of case-insensitive regex match.
 */
Datum icregexeqjoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Regex_IC, false));
}

/*
 *		likejoinsel			- Join selectivity of LIKE pattern match.
 */
Datum likejoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Like, false));
}

/*
 *		iclikejoinsel			- Join selectivity of ILIKE pattern match.
 */
Datum iclikejoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Like_IC, false));
}

/*
 *		regexnejoinsel	- Join selectivity of regex non-match.
 */
Datum regexnejoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Regex, true));
}

/*
 *		icregexnejoinsel	- Join selectivity of case-insensitive regex non-match.
 */
Datum icregexnejoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Regex_IC, true));
}

/*
 *		nlikejoinsel		- Join selectivity of LIKE pattern non-match.
 */
Datum nlikejoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Like, true));
}

/*
 *		icnlikejoinsel		- Join selectivity of ILIKE pattern non-match.
 */
Datum icnlikejoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(patternjoinsel(fcinfo, Pattern_Type_Like_IC, true));
}

/*
 * mergejoinscansel			- Scan selectivity of merge join.
 *
 * A merge join will stop as soon as it exhausts either input stream.
 * Therefore, if we can estimate the ranges of both input variables,
 * we can estimate how much of the input will actually be read.  This
 * can have a considerable impact on the cost when using indexscans.
 *
 * Also, we can estimate how much of each input has to be read before the
 * first join pair is found, which will affect the join's startup time.
 *
 * clause should be a clause already known to be mergejoinable.  opfamily,
 * strategy, and nulls_first specify the sort ordering being used.
 *
 * The outputs are:
 *		*leftstart is set to the fraction of the left-hand variable expected
 *		 to be scanned before the first join pair is found (0 to 1).
 *		*leftend is set to the fraction of the left-hand variable expected
 *		 to be scanned before the join terminates (0 to 1).
 *		*rightstart, *rightend similarly for the right-hand variable.
 */
void mergejoinscansel(PlannerInfo* root, Node* clause, Oid opfamily, int strategy, bool nulls_first,
    Selectivity* leftstart, Selectivity* leftend, Selectivity* rightstart, Selectivity* rightend)
{
    Node* left = NULL;
    Node* right = NULL;
    VariableStatData left_var, right_var;
    int op_strategy;
    Oid op_lefttype;
    Oid op_righttype;
    Oid op_no, l_sort_op, r_sort_op, l_stat_op, r_stat_op, l_top, l_eop, r_evl_top, r_evl_eop;
    bool isgt = false;
    Datum left_min;
    Datum left_max;
    Datum right_min;
    Datum right_max;
    double selec;

    /* Set default results if we can't figure anything out. */
    /* XXX should default "start" fraction be a bit more than 0? */
    *leftstart = *rightstart = 0.0;
    *leftend = *rightend = 1.0;

    /* Deconstruct the merge clause */
    if (!is_opclause(clause)) {
        return; /* shouldn't happen */
    }
    op_no = ((OpExpr*)clause)->opno;
    left = get_leftop((Expr*)clause);
    right = get_rightop((Expr*)clause);
    if (right == NULL) {
        return; /* shouldn't happen */
    }

    /* Look for stats for the inputs */
    examine_variable(root, left, 0, &left_var);
    examine_variable(root, right, 0, &right_var);

    /* Extract the operator's declared left/right datatypes */
    get_op_opfamily_properties(op_no, opfamily, false, &op_strategy, &op_lefttype, &op_righttype);
    Assert(op_strategy == BTEqualStrategyNumber);

    /*
     * Look up the various operators we need.  If we don't find them all, it
     * probably means the opfamily is broken, but we just fail silently.
     *
     * Note: we expect that pg_statistic histograms will be sorted by the '<'
     * operator, regardless of which sort direction we are considering.
     */
    switch (strategy) {
        case BTLessStrategyNumber:
            isgt = false;
            if (op_lefttype == op_righttype) {
                /* easy case */
                l_top = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTLessStrategyNumber);
                l_eop = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTLessEqualStrategyNumber);
                l_sort_op = l_top;
                r_sort_op = l_top;
                l_stat_op = l_sort_op;
                r_stat_op = r_sort_op;
                r_evl_top = l_top;
                r_evl_eop = l_eop;
            } else {
                l_top = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTLessStrategyNumber);
                l_eop = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTLessEqualStrategyNumber);
                l_sort_op = get_opfamily_member(opfamily, op_lefttype, op_lefttype, BTLessStrategyNumber);
                r_sort_op = get_opfamily_member(opfamily, op_righttype, op_righttype, BTLessStrategyNumber);
                l_stat_op = l_sort_op;
                r_stat_op = r_sort_op;
                r_evl_top = get_opfamily_member(opfamily, op_righttype, op_lefttype, BTLessStrategyNumber);
                r_evl_eop = get_opfamily_member(opfamily, op_righttype, op_lefttype, BTLessEqualStrategyNumber);
            }
            break;
        case BTGreaterStrategyNumber:
            /* descending-order case */
            isgt = true;
            if (op_lefttype == op_righttype) {
                /* easy case */
                l_top = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTGreaterStrategyNumber);
                l_eop = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTGreaterEqualStrategyNumber);
                l_sort_op = l_top;
                r_sort_op = l_top;
                l_stat_op = get_opfamily_member(opfamily, op_lefttype, op_lefttype, BTLessStrategyNumber);
                r_stat_op = l_stat_op;
                r_evl_top = l_top;
                r_evl_eop = l_eop;
            } else {
                l_top = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTGreaterStrategyNumber);
                l_eop = get_opfamily_member(opfamily, op_lefttype, op_righttype, BTGreaterEqualStrategyNumber);
                l_sort_op = get_opfamily_member(opfamily, op_lefttype, op_lefttype, BTGreaterStrategyNumber);
                r_sort_op = get_opfamily_member(opfamily, op_righttype, op_righttype, BTGreaterStrategyNumber);
                l_stat_op = get_opfamily_member(opfamily, op_lefttype, op_lefttype, BTLessStrategyNumber);
                r_stat_op = get_opfamily_member(opfamily, op_righttype, op_righttype, BTLessStrategyNumber);
                r_evl_top = get_opfamily_member(opfamily, op_righttype, op_lefttype, BTGreaterStrategyNumber);
                r_evl_eop = get_opfamily_member(opfamily, op_righttype, op_lefttype, BTGreaterEqualStrategyNumber);
            }
            break;
        default:
            goto fail; /* shouldn't get here */
    }

    if (!OidIsValid(l_sort_op) || !OidIsValid(r_sort_op) || !OidIsValid(l_stat_op) || !OidIsValid(r_stat_op) ||
        !OidIsValid(l_top) || !OidIsValid(l_eop) || !OidIsValid(r_evl_top) || !OidIsValid(r_evl_eop)) {
        goto fail; /* insufficient info in catalogs */
    }

    /* Try to get ranges of both inputs */
    if (!isgt) {
        if (!get_variable_range(root, &left_var, l_stat_op, &left_min, &left_max)) {
            goto fail; /* no range available from stats */
        }
        if (!get_variable_range(root, &right_var, r_stat_op, &right_min, &right_max)) {
            goto fail; /* no range available from stats */
        }
    } else {
        /* need to swap the max and min */
        if (!get_variable_range(root, &left_var, l_stat_op, &left_max, &left_min)) {
            goto fail; /* no range available from stats */
        }
        if (!get_variable_range(root, &right_var, r_stat_op, &right_max, &right_min)) {
            goto fail; /* no range available from stats */
        }
    }

    /*
     * Now, the fraction of the left variable that will be scanned is the
     * fraction that's <= the right-side maximum value.  But only believe
     * non-default estimates, else stick with our 1.0.
     */
    selec = scalarineqsel(root, l_eop, isgt, &left_var, right_max, op_righttype);
    if (selec != DEFAULT_INEQ_SEL) {
        *leftend = selec;
    }

    /* And similarly for the right variable. */
    selec = scalarineqsel(root, r_evl_eop, isgt, &right_var, left_max, op_lefttype);
    if (selec != DEFAULT_INEQ_SEL) {
        *rightend = selec;
    }

    /*
     * Only one of the two "end" fractions can really be less than 1.0;
     * believe the smaller estimate and reset the other one to exactly 1.0. If
     * we get exactly equal estimates (as can easily happen with self-joins),
     * believe neither.
     */
    if (*leftend > *rightend) {
        *leftend = 1.0;
    } else if (*leftend < *rightend) {
        *rightend = 1.0;
    } else {
        *leftend = *rightend = 1.0;
    }

    /*
     * Also, the fraction of the left variable that will be scanned before the
     * first join pair is found is the fraction that's < the right-side
     * minimum value.  But only believe non-default estimates, else stick with
     * our own default.
     */
    selec = scalarineqsel(root, l_top, isgt, &left_var, right_min, op_righttype);
    if (selec != DEFAULT_INEQ_SEL) {
        *leftstart = selec;
    }

    /* And similarly for the right variable. */
    selec = scalarineqsel(root, r_evl_top, isgt, &right_var, left_min, op_lefttype);
    if (selec != DEFAULT_INEQ_SEL) {
        *rightstart = selec;
    }

    /*
     * Only one of the two "start" fractions can really be more than zero;
     * believe the larger estimate and reset the other one to exactly 0.0. If
     * we get exactly equal estimates (as can easily happen with self-joins),
     * believe neither.
     */
    if (*leftstart < *rightstart) {
        *leftstart = 0.0;
    } else if (*leftstart > *rightstart) {
        *rightstart = 0.0;
    } else {
        *leftstart = *rightstart = 0.0;
    }

    /*
     * If the sort order is nulls-first, we're going to have to skip over any
     * nulls too.  These would not have been counted by scalarineqsel, and we
     * can safely add in this fraction regardless of whether we believe
     * scalarineqsel's results or not.  But be sure to clamp the sum to 1.0!
     */
    if (nulls_first) {
        Form_pg_statistic stats;

        if (HeapTupleIsValid(left_var.statsTuple)) {
            stats = (Form_pg_statistic)GETSTRUCT(left_var.statsTuple);
            *leftstart += stats->stanullfrac;
            CLAMP_PROBABILITY(*leftstart);
            *leftend += stats->stanullfrac;
            CLAMP_PROBABILITY(*leftend);
        }
        if (HeapTupleIsValid(right_var.statsTuple)) {
            stats = (Form_pg_statistic)GETSTRUCT(right_var.statsTuple);
            *rightstart += stats->stanullfrac;
            CLAMP_PROBABILITY(*rightstart);
            *rightend += stats->stanullfrac;
            CLAMP_PROBABILITY(*rightend);
        }
    }

    /* Disbelieve start >= end, just in case that can happen */
    if (*leftstart >= *leftend) {
        *leftstart = 0.0;
        *leftend = 1.0;
    }
    if (*rightstart >= *rightend) {
        *rightstart = 0.0;
        *rightend = 1.0;
    }

fail:
    ReleaseVariableStats(left_var);
    ReleaseVariableStats(right_var);
}

static List* add_unique_group_var(
    PlannerInfo* root, List* var_infos, Node* var, VariableStatData* var_data, STATS_EST_TYPE e_type)
{
    GroupVarInfo* var_info = NULL;
    double n_distinct;
    bool is_default = false;
    ListCell* lc = NULL;

    n_distinct = get_variable_numdistinct(var_data, &is_default, false, 1.0, NULL, e_type);

    /* cannot use foreach here because of possible list_delete */
    lc = list_head(var_infos);
    while (lc != NULL) {
        var_info = (GroupVarInfo*)lfirst(lc);

        /* must advance lc before list_delete possibly pfree's it */
        lc = lnext(lc);

        /* Drop exact duplicates */
        if (equal(var, var_info->var)) {
            return var_infos;
        }

        /*
         * Drop known-equal vars, but only if they belong to different
         * relations (see comments for estimate_num_groups)
         */
        if (var_data->rel != var_info->rel && exprs_known_equal(root, var, var_info->var)) {
            if (var_info->ndistinct <= n_distinct) {
                /* Keep older item, forget new one */
                return var_infos;
            } else {
                /* Delete the older item */
                var_infos = list_delete_ptr(var_infos, var_info);
            }
        }
    }

    var_info = (GroupVarInfo*)palloc(sizeof(GroupVarInfo));

    var_info->var = var;
    var_info->rel = var_data->rel;
    var_info->ndistinct = n_distinct;
    var_info->isdefault = is_default;
    var_info->es_is_used = false;
    var_info->es_attnums = NULL;
    var_infos = lappend(var_infos, var_info);
    return var_infos;
}

/*
 * estimate_num_groups		- Estimate number of groups in a grouped query
 *
 * Given a query having a GROUP BY clause, estimate how many groups there
 * will be --- ie, the number of distinct combinations of the GROUP BY
 * expressions.
 *
 * This routine is also used to estimate the number of rows emitted by
 * a DISTINCT filtering step; that is an isomorphic problem.  (Note:
 * actually, we only use it for DISTINCT when there's no grouping or
 * aggregation ahead of the DISTINCT.)
 *
 * Inputs:
 *	root - the query
 *	groupExprs - list of expressions being grouped by
 *	input_rows - number of rows estimated to arrive at the group/unique
 *		filter step
 *
 * Given the lack of any cross-correlation statistics in the system, it's
 * impossible to do anything really trustworthy with GROUP BY conditions
 * involving multiple Vars.  We should however avoid assuming the worst
 * case (all possible cross-product terms actually appear as groups) since
 * very often the grouped-by Vars are highly correlated.  Our current approach
 * is as follows:
 *	1.	Expressions yielding boolean are assumed to contribute two groups,
 *		independently of their content, and are ignored in the subsequent
 *		steps.	This is mainly because tests like "col IS NULL" break the
 *		heuristic used in step 2 especially badly.
 *	2.	Reduce the given expressions to a list of unique Vars used.  For
 *		example, GROUP BY a, a + b is treated the same as GROUP BY a, b.
 *		It is clearly correct not to count the same Var more than once.
 *		It is also reasonable to treat f(x) the same as x: f() cannot
 *		increase the number of distinct values (unless it is volatile,
 *		which we consider unlikely for grouping), but it probably won't
 *		reduce the number of distinct values much either.
 *		As a special case, if a GROUP BY expression can be matched to an
 *		expressional index for which we have statistics, then we treat the
 *		whole expression as though it were just a Var.
 *	3.	If the list contains Vars of different relations that are known equal
 *		due to equivalence classes, then drop all but one of the Vars from each
 *		known-equal set, keeping the one with smallest estimated # of values
 *		(since the extra values of the others can't appear in joined rows).
 *		Note the reason we only consider Vars of different relations is that
 *		if we considered ones of the same rel, we'd be double-counting the
 *		restriction selectivity of the equality in the next step.
 *	4.	For Vars within a single source rel, we multiply together the numbers
 *		of values, clamp to the number of rows in the rel (divided by 10 if
 *		more than one Var), and then multiply by the selectivity of the
 *		restriction clauses for that rel.  When there's more than one Var,
 *		the initial product is probably too high (it's the worst case) but
 *		clamping to a fraction of the rel's rows seems to be a helpful
 *		heuristic for not letting the estimate get out of hand.  (The factor
 *		of 10 is derived from pre-Postgres-7.4 practice.)  Multiplying
 *		by the restriction selectivity is effectively assuming that the
 *		restriction clauses are independent of the grouping, which is a crummy
 *		assumption, but it's hard to do better.
 *	5.	If there are Vars from multiple rels, we repeat step 4 for each such
 *		rel, and multiply the results together.
 * Note that rels not containing grouped Vars are ignored completely, as are
 * join clauses.  Such rels cannot increase the number of groups, and we
 * assume such clauses do not reduce the number either (somewhat bogus,
 * but we don't have the info to do better).
 */
double estimate_num_groups(PlannerInfo* root, List* group_exprs, double input_rows, unsigned int num_datanodes,
    STATS_EST_TYPE e_type, List** pgset)
{
    List* var_infos = NIL;
    double num_distinct;
    ListCell* l = NULL;
    int i;
    ES_SELECTIVITY* es = NULL;
    MemoryContext extended_stat;
    MemoryContext old_context;

    /*
     * If no grouping columns, there's exactly one group.  (This can't happen
     * for normal cases with GROUP BY or DISTINCT, but it is possible for
     * corner cases with set operations.)
     */
    if (group_exprs == NIL || (pgset && list_length(*pgset) < 1)) {
        return 1.0;
    }

    /*
     * Count groups derived from boolean grouping expressions.	For other
     * expressions, find the unique Vars used, treating an expression as a Var
     * if we can find stats for it.  For each one, record the statistical
     * estimate of number of distinct values (total in its table, without
     * regard for filtering).
     */
    num_distinct = 1.0;
    i = 0;
    foreach (l, group_exprs) {
        Node* groupexpr = (Node*)lfirst(l);
        VariableStatData vardata;
        List* varshere = NIL;
        ListCell* l2 = NULL;

        /* is expression in this grouping set? */
        if (pgset && !list_member_int(*pgset, i++)) {
            continue;
        }

        /* Short-circuit for expressions returning boolean */
        if (exprType(groupexpr) == BOOLOID || IsA(groupexpr, GroupingFunc)) {
            num_distinct *= 2.0;
            continue;
        }

        /*
         * If examine_variable is able to deduce anything about the GROUP BY
         * expression, treat it as a single variable even if it's really more
         * complicated.
         */
        examine_variable(root, groupexpr, 0, &vardata);
        if (HeapTupleIsValid(vardata.statsTuple) || vardata.isunique) {
            /*
             * we don't need possion to estimate distinct because
             * we can't estimate accurate for multiple exprs.
             */
            vardata.enablePossion = false;
            var_infos = add_unique_group_var(root, var_infos, groupexpr, &vardata, e_type);
            ReleaseVariableStats(vardata);
            continue;
        }
        ReleaseVariableStats(vardata);

        /*
         * Else pull out the component Vars.  Handle PlaceHolderVars by
         * recursing into their arguments (effectively assuming that the
         * PlaceHolderVar doesn't change the number of groups, which boils
         * down to ignoring the possible addition of nulls to the result set).
         */
        varshere =
            pull_var_clause(groupexpr, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_INCLUDE_SPECIAL_EXPR);

        if (varshere != NULL) {
            varshere = specialExpr_group_num(root, varshere, &num_distinct, input_rows);
        }
        /*
         * If we find any variable-free GROUP BY item, then either it is a
         * constant (and we can ignore it) or it contains a volatile function;
         * in the latter case we punt and assume that each input row will
         * yield a distinct group.
         */
        if (varshere == NIL) {
            if (contain_volatile_functions(groupexpr)) {
                return clamp_row_est(input_rows);
            }
            continue;
        }

        /*
         * Else add variables to var_infos list
         */
        foreach (l2, varshere) {
            Node* var = (Node*)lfirst(l2);

            examine_variable(root, var, 0, &vardata);
            /*
             * we don't need possion to estimate distinct because
             * we can't estimate accurate for multiple exprs.
             */
            vardata.enablePossion = false;
            var_infos = add_unique_group_var(root, var_infos, var, &vardata, e_type);
            ReleaseVariableStats(vardata);
        }
        list_free_ext(varshere);
    }

    /*
     * If now no Vars, we must have an all-constant or all-boolean GROUP BY
     * list.
     */
    if (var_infos == NIL) {
        /* Guard against out-of-range answers */
        if (num_distinct > input_rows) {
            num_distinct = input_rows;
        }
        return clamp_row_est(num_distinct);
    }

    if (list_length(var_infos) >= 2) {
        /* initialize es_selectivity class */
        extended_stat = AllocSetContextCreate(CurrentMemoryContext,
            "extended_stat",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        old_context = MemoryContextSwitchTo(extended_stat);
        es = New(extended_stat) ES_SELECTIVITY();
        (void)es->calculate_selectivity(root, var_infos, NULL, JOIN_INNER, NULL, ES_GROUPBY, e_type);
        (void)MemoryContextSwitchTo(old_context);
        var_infos = es->unmatched_clause_group;
    }

    List* varinfo_orig = var_infos;
    /*
     * Group Vars by relation and estimate total num_distinct.
     *
     * For each iteration of the outer loop, we process the frontmost Var in
     * var_infos, plus all other Vars in the same relation.	We remove these
     * Vars from the newvarinfos list for the next iteration. This is the
     * easiest way to group Vars of same rel together.
     */
    do {
        GroupVarInfo* var_info1 = (GroupVarInfo*)linitial(var_infos);
        RelOptInfo* rel = var_info1->rel;
        double rel_distinct = var_info1->ndistinct;
        double rel_max_ndistinct = rel_distinct;
        double rel_tuples =
            (e_type == STATS_TYPE_LOCAL)
                ? get_local_rows(rel->tuples, rel->multiple, IsLocatorReplicated(rel->locator_type), num_datanodes)
                : rel->tuples;
        double rel_rows =
            (e_type == STATS_TYPE_LOCAL)
                ? get_local_rows(rel->rows, rel->multiple, IsLocatorReplicated(rel->locator_type), num_datanodes)
                : rel->rows;
        int rel_var_count = 1;
        List* new_var_infos = NIL;
        bool has_default = var_info1->isdefault;

        /*
         * Get the product of num_distinct estimates of the Vars for this rel.
         * Also, construct new var_infos list of remaining Vars.
         */
        for_each_cell(l, lnext(list_head(var_infos))) {
            GroupVarInfo* var_info2 = (GroupVarInfo*)lfirst(l);

            if (var_info2->rel == var_info1->rel) {
                rel_distinct *= var_info2->ndistinct;
                has_default = has_default || var_info2->isdefault;
                if (rel_max_ndistinct < var_info2->ndistinct)
                    rel_max_ndistinct = var_info2->ndistinct;
                rel_var_count++;
            } else {
                /* not time to process var_info2 yet */
                new_var_infos = lcons(var_info2, new_var_infos);
            }
        }

        /*
         * Sanity check --- don't divide by zero if empty relation.
         */
        Assert(rel->reloptkind == RELOPT_BASEREL || rel->reloptkind == RELOPT_OTHER_MEMBER_REL);
        if (rel_tuples > 0) {
            /*
             * Clamp to size of rel, or size of rel / 10 if multiple Vars. The
             * fudge factor is because the Vars are probably correlated but we
             * don't know by how much.  We should never clamp to less than the
             * largest ndistinct value for any of the Vars, though, since
             * there will surely be at least that many groups.
             */
            double clamp = rel_tuples;

            if (rel_var_count > 1) {
                /* if we use default value, so estimate as 1/10 for each dn */
                if (has_default) {
                    clamp *= 0.1;
                    if (e_type == STATS_TYPE_GLOBAL) {
                        clamp *= 1 / (num_datanodes * (1 - 0.1));
                    }
                }
                if (clamp < rel_max_ndistinct) {
                    clamp = rel_max_ndistinct;
                    /* for sanity in case some ndistinct is too large: */
                    if (clamp > rel_tuples) {
                        clamp = rel_tuples;
                    }
                }
            }
            if (rel_distinct > clamp) {
                rel_distinct = clamp;
            }

            /*
             * Multiply by restriction selectivity.
             * If used extended statistc, use possion model to adjust rel_distinct.
             */
            double tmp = rel_distinct;
            if (var_info1->es_is_used && rel_tuples > 0 &&
                rel_rows / rel_tuples < SELECTIVITY_THRESHOLD_TO_USE_POISSON) {
                rel_distinct = NUM_DISTINCT_SELECTIVITY_FOR_POISSON(rel_distinct, rel_tuples, rel_rows / rel_tuples);
                ereport(ES_DEBUG_LEVEL,
                    (errmodule(MOD_OPT),
                        (errmsg(
                            "[ES]The origin distinct value is %f. "
                            "After using possion model with rel_tuples=%f and ration=%e "
                            "The new distinct value is %f",
                            tmp,
                            rel_tuples,
                            rel_rows / rel_tuples,
                            rel_distinct))));
            } else {
                rel_distinct *= rel_rows / rel_tuples;
                ereport(ES_DEBUG_LEVEL,
                    (errmodule(MOD_OPT),
                        (errmsg("The origin distinct value is %f. After multiplying rel_rows(%e)/rel_tuples(%e), "
                            "The new distinct value is %f",
                            tmp,
                            rel_rows,
                            rel_tuples,
                            rel_distinct))));
            }

            /*
             * Update estimate of total distinct groups.
             */
            num_distinct *= rel_distinct;
        }

        var_infos = new_var_infos;
    } while (var_infos != NIL);
    list_free_deep(varinfo_orig);

    /* free space used by extended statistic */
    if (es != NULL) {
        /* notice that: es->unmatched_clause_group has been
         * deleted in list_free_deep(var_infos); So no need to free
         * it again.
         */
        delete es;
        MemoryContextDelete(extended_stat);
    }

    num_distinct = ceil(num_distinct);

    /* Guard against out-of-range answers */
    if (num_distinct > input_rows) {
        num_distinct = input_rows;
    }
    if (num_distinct < 1.0) {
        num_distinct = 1.0;
    }

    return clamp_row_est(num_distinct);
}

/*
 * Estimate hash bucketsize fraction (ie, number of entries in a bucket
 * divided by total tuples in relation) if the specified expression is used
 * as a hash key.
 *
 * XXX This is really pretty bogus since we're effectively assuming that the
 * distribution of hash keys will be the same after applying restriction
 * clauses as it was in the underlying relation.  However, we are not nearly
 * smart enough to figure out how the restrict clauses might change the
 * distribution, so this will have to do for now.
 *
 * We are passed the number of buckets the executor will use for the given
 * input relation.	If the data were perfectly distributed, with the same
 * number of tuples going into each available bucket, then the bucketsize
 * fraction would be 1/nbuckets.  But this happy state of affairs will occur
 * only if (a) there are at least nbuckets distinct data values, and (b)
 * we have a not-too-skewed data distribution.	Otherwise the buckets will
 * be nonuniformly occupied.  If the other relation in the join has a key
 * distribution similar to this one's, then the most-loaded buckets are
 * exactly those that will be probed most often.  Therefore, the "average"
 * bucket size for costing purposes should really be taken as something close
 * to the "worst case" bucket size.  We try to estimate this by adjusting the
 * fraction if there are too few distinct data values, and then scaling up
 * by the ratio of the most common value's frequency to the average frequency.
 *
 * If no statistics are available, use a default estimate of 0.1.  This will
 * discourage use of a hash rather strongly if the inner relation is large,
 * which is what we want.  We do not want to hash unless we know that the
 * inner rel is well-dispersed (or the alternatives seem much worse).
 */
Selectivity estimate_hash_bucketsize(PlannerInfo* root, Node* hash_key, double n_buckets, Path* inner_path,
    SpecialJoinInfo* sj_info, double* distinct_num)
{
    VariableStatData var_data;
    double est_fract;
    double ndistinct;
    double global_ndistinct;
    double mcv_freq;
    double avg_freq;
    bool is_default = false;
    float4* numbers = NULL;
    int n_numbers;

    ndistinct = estimate_local_numdistinct(root, hash_key, inner_path, sj_info,
        &global_ndistinct, &is_default, &var_data);

    /* caller does not need it, we allow for distinct_num == NULL */
    if (distinct_num != NULL) {
        *distinct_num = ndistinct;
    }

    /* If ndistinct isn't real, punt and return 0.1, per comments above */
    if (is_default) {
        ReleaseVariableStats(var_data);
        return (Selectivity)1.0 / DEFAULT_NUM_DISTINCT;
    }

    /* Compute avg freq of all distinct data values in raw relation */
    avg_freq = 1.0 / ndistinct;

    /*
     * Initial estimate of bucketsize fraction is 1/n_buckets as long as the
     * number of buckets is less than the expected number of distinct values;
     * otherwise it is 1/ndistinct.
     */
    if (ndistinct > n_buckets) {
        est_fract = 1.0 / n_buckets;
    } else {
        if (ndistinct < 1.0) {
            ndistinct = 1.0;
        }
        est_fract = 1.0 / ndistinct;
    }

    /*
     * Look up the frequency of the most common value, if available.
     */
    mcv_freq = 0.0;

    if (HeapTupleIsValid(var_data.statsTuple)) {
        if (get_attstatsslot(var_data.statsTuple,
                var_data.atttype,
                var_data.atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                NULL,
                NULL,
                &numbers,
                &n_numbers)) {
            /*
             * The first MCV stat is for the most common value.
             */
            if (n_numbers > 0) {
                if (var_data.rel == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("Unexpected null value for the rel of var_data")));
                }
                /* We should adjust mcv_freq with selectivity because mcv_freq is global. */
                mcv_freq = numbers[0] / (var_data.rel->rows / var_data.rel->tuples);
            }
            free_attstatsslot(var_data.atttype, NULL, 0, numbers, n_numbers);
        }
    }

    /*
     * Adjust estimated bucketsize upward to account for skewed distribution.
     */
    bool check_freq = avg_freq > 0.0 && mcv_freq > avg_freq;
    if (check_freq) {
        /* for distribute key, mcv freq should be multiplied by dn number */
        double multiple = 1.0;
        if (var_data.rel && list_length(var_data.rel->distribute_keys) == 1 &&
            equal(hash_key, linitial(var_data.rel->distribute_keys))) {
            multiple = u_sess->pgxc_cxt.NumDataNodes;
        }

        est_fract *= mcv_freq / avg_freq;
        /* if adjusted selectivity is larger than mcv_freq, then the estimate is too far off,
           take the mcv_freq instead. */
        if (est_fract > mcv_freq * multiple) {
            est_fract = mcv_freq * multiple;
        }
    }

    ereport(DEBUG1,
        (errmodule(MOD_OPT),
            errmsg("ndistinct=%.lf, global_ndistinct=%.lf, avg_freq=%.10f, mcv_freq=%.10f, est_fract=%.10f",
                ndistinct,
                global_ndistinct,
                avg_freq,
                mcv_freq,
                est_fract)));

    if (var_data.rel != NULL) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT),
                errmsg("rows=%.lf, tuples=%.lf, multiple=%.lf",
                    var_data.rel->rows,
                    var_data.rel->tuples,
                    var_data.rel->multiple)));
    }

    /*
     * Clamp bucketsize to sane range (the above adjustment could easily
     * produce an out-of-range result).  We set the lower bound a little above
     * zero, since zero isn't a very sane result.
     * We should adjust the lower bound as 1.0e-7 because the distinct value
     * may be larger than 1000000 as the increasing of work_mem.
     */
    if (est_fract < 1.0e-7) {
        est_fract = 1.0e-7;
    } else if (est_fract >= 1.0) {
        if (mcv_freq > 0.0) {
            est_fract = mcv_freq;
        } else {
            est_fract = 1.0;
        }
    }

    ReleaseVariableStats(var_data);

    return (Selectivity)est_fract;
}

/* -------------------------------------------------------------------------
 *
 * Support routines
 *
 * -------------------------------------------------------------------------
 */

/*
 * convert_to_scalar
 *	  Convert non-NULL values of the indicated types to the comparison
 *	  scale needed by scalarineqsel().
 *	  Returns "true" if successful.
 *
 * XXX this routine is a hack: ideally we should look up the conversion
 * subroutines in pg_type.
 *
 * All numeric datatypes are simply converted to their equivalent
 * "double" values.  (NUMERIC values that are outside the range of "double"
 * are clamped to +/- HUGE_VAL.)
 *
 * String datatypes are converted by convert_string_to_scalar(),
 * which is explained below.  The reason why this routine deals with
 * three values at a time, not just one, is that we need it for strings.
 *
 * The bytea datatype is just enough different from strings that it has
 * to be treated separately.
 *
 * The several datatypes representing absolute times are all converted
 * to Timestamp, which is actually a double, and then we just use that
 * double value.  Note this will give correct results even for the "special"
 * values of Timestamp, since those are chosen to compare correctly;
 * see timestamp_cmp.
 *
 * The several datatypes representing relative times (intervals) are all
 * converted to measurements expressed in seconds.
 */
static bool convert_to_scalar(Datum value, Oid value_typid, double* scaled_value, Datum lo_bound, Datum hi_bound,
    Oid bounds_typid, double* scaled_lo_bound, double* scaled_hi_bound)
{
    /*
     * Both the value_typid and the bounds_typid should exactly match the
     * declared input type(s) of the operator we are invoked for, so we just
     * error out if either is not recognized.
     *
     * XXX The histogram we are interpolating between points of could belong
     * to a column that's only binary-compatible with the declared type. In
     * essence we are assuming that the semantics of binary-compatible types
     * are enough alike that we can use a histogram generated with one type's
     * operators to estimate selectivity for the other's.  This is outright
     * wrong in some cases --- in particular signed versus unsigned
     * interpretation could trip us up.  But it's useful enough in the
     * majority of cases that we do it anyway.	Should think about more
     * rigorous ways to do it.
     */
    switch (value_typid) {
            /*
             * Built-in numeric types
             */
        case BOOLOID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case OIDOID:
        case REGPROCOID:
        case REGPROCEDUREOID:
        case REGOPEROID:
        case REGOPERATOROID:
        case REGCLASSOID:
        case REGTYPEOID:
        case REGCONFIGOID:
        case REGDICTIONARYOID:
            *scaled_value = convert_numeric_to_scalar(value, value_typid);
            *scaled_lo_bound = convert_numeric_to_scalar(lo_bound, bounds_typid);
            *scaled_hi_bound = convert_numeric_to_scalar(hi_bound, bounds_typid);
            return true;

            /*
             * Built-in string types
             */
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        case NAMEOID: {
            char* valstr = convert_string_datum(value, value_typid);
            char* lostr = convert_string_datum(lo_bound, bounds_typid);
            char* histr = convert_string_datum(hi_bound, bounds_typid);

            convert_string_to_scalar(valstr, scaled_value, lostr, scaled_lo_bound, histr, scaled_hi_bound);
            pfree_ext(valstr);
            pfree_ext(lostr);
            pfree_ext(histr);
            return true;
        }

            /*
             * Built-in bytea type
             */
        case BYTEAOID: {
            convert_bytea_to_scalar(value, scaled_value, lo_bound, scaled_lo_bound, hi_bound, scaled_hi_bound);
            return true;
        }

            /*
             * Built-in time types
             */
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case ABSTIMEOID:
        case DATEOID:
        case INTERVALOID:
        case RELTIMEOID:
        case TINTERVALOID:
        case TIMEOID:
        case TIMETZOID:
            *scaled_value = convert_timevalue_to_scalar(value, value_typid);
            *scaled_lo_bound = convert_timevalue_to_scalar(lo_bound, bounds_typid);
            *scaled_hi_bound = convert_timevalue_to_scalar(hi_bound, bounds_typid);
            return true;

            /*
             * Built-in network types
             */
        case INETOID:
        case CIDROID:
        case MACADDROID:
            *scaled_value = convert_network_to_scalar(value, value_typid);
            *scaled_lo_bound = convert_network_to_scalar(lo_bound, bounds_typid);
            *scaled_hi_bound = convert_network_to_scalar(hi_bound, bounds_typid);
            return true;
        default:
            break;
    }
    /* Don't know how to convert */
    *scaled_value = *scaled_lo_bound = *scaled_hi_bound = 0;
    return false;
}

/*
 * Do convert_to_scalar()'s work for any numeric data type.
 */
static double convert_numeric_to_scalar(Datum value, Oid typid)
{
    switch (typid) {
        case BOOLOID:
            return (double)DatumGetBool(value);
        case INT2OID:
            return (double)DatumGetInt16(value);
        case INT4OID:
            return (double)DatumGetInt32(value);
        case INT8OID:
            return (double)DatumGetInt64(value);
        case FLOAT4OID:
            return (double)DatumGetFloat4(value);
        case FLOAT8OID:
            return (double)DatumGetFloat8(value);
        case NUMERICOID:
            /* Note: out-of-range values will be clamped to +-HUGE_VAL */
            return (double)DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, value));
        case OIDOID:
        case REGPROCOID:
        case REGPROCEDUREOID:
        case REGOPEROID:
        case REGOPERATOROID:
        case REGCLASSOID:
        case REGTYPEOID:
        case REGCONFIGOID:
        case REGDICTIONARYOID:
            /* we can treat OIDs as integers... */
            return (double)DatumGetObjectId(value);
        default:
            break;
    }

    /*
     * Can't get here unless someone tries to use scalarltsel/scalargtsel on
     * an operator with one numeric and one non-numeric operand.
     */
    ereport(
        ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unsupported type: %u", typid))));

    return 0;
}

/*
 * Do convert_to_scalar()'s work for any character-string data type.
 *
 * String datatypes are converted to a scale that ranges from 0 to 1,
 * where we visualize the bytes of the string as fractional digits.
 *
 * We do not want the base to be 256, however, since that tends to
 * generate inflated selectivity estimates; few databases will have
 * occurrences of all 256 possible byte values at each position.
 * Instead, use the smallest and largest byte values seen in the bounds
 * as the estimated range for each byte, after some fudging to deal with
 * the fact that we probably aren't going to see the full range that way.
 *
 * An additional refinement is that we discard any common prefix of the
 * three strings before computing the scaled values.  This allows us to
 * "zoom in" when we encounter a narrow data range.  An example is a phone
 * number database where all the values begin with the same area code.
 * (Actually, the bounds will be adjacent histogram-bin-boundary values,
 * so this is more likely to happen than you might think.)
 */
static void convert_string_to_scalar(const char* value, double* scaled_value, char* lo_bound, double* scaled_lo_bound,
    char* hi_bound, double* scaled_hi_bound)
{
    int range_lo;
    int range_hi;
    char* sptr = NULL;

    range_lo = range_hi = (unsigned char)hi_bound[0];
    for (sptr = lo_bound; *sptr; sptr++) {
        if (range_lo > (unsigned char)*sptr) {
            range_lo = (unsigned char)*sptr;
        }
        if (range_hi < (unsigned char)*sptr) {
            range_hi = (unsigned char)*sptr;
        }
    }
    for (sptr = hi_bound; *sptr; sptr++) {
        if (range_lo > (unsigned char)*sptr) {
            range_lo = (unsigned char)*sptr;
        }
        if (range_hi < (unsigned char)*sptr) {
            range_hi = (unsigned char)*sptr;
        }
    }
    /* If range includes any upper-case ASCII chars, make it include all */
    if (range_lo <= 'Z' && range_hi >= 'A') {
        if (range_lo > 'A') {
            range_lo = 'A';
        }
        if (range_hi < 'Z') {
            range_hi = 'Z';
        }
    }
    /* Ditto lower-case */
    if (range_lo <= 'z' && range_hi >= 'a') {
        if (range_lo > 'a') {
            range_lo = 'a';
        }
        if (range_hi < 'z') {
            range_hi = 'z';
        }
    }
    /* Ditto digits */
    if (range_lo <= '9' && range_hi >= '0') {
        if (range_lo > '0') {
            range_lo = '0';
        }
        if (range_hi < '9') {
            range_hi = '9';
        }
    }

    /*
     * If range includes less than 10 chars, assume we have not got enough
     * data, and make it include regular ASCII set.
     */
    if (range_hi - range_lo < 9) {
        range_lo = ' ';
        range_hi = 127;
    }

    /*
     * Now strip any common prefix of the three strings.
     */
    while (*lo_bound) {
        if (*lo_bound != *hi_bound || *lo_bound != *value) {
            break;
        }
        lo_bound++, hi_bound++, value++;
    }

    /*
     * Now we can do the conversions.
     */
    *scaled_value = convert_one_string_to_scalar(value, range_lo, range_hi);
    *scaled_lo_bound = convert_one_string_to_scalar(lo_bound, range_lo, range_hi);
    *scaled_hi_bound = convert_one_string_to_scalar(hi_bound, range_lo, range_hi);
}

static double convert_one_string_to_scalar(const char* value, int range_lo, int range_hi)
{
    int slen = strlen(value);
    double num;
    double denom;
    double base;

    if (slen <= 0) {
        return 0.0; /* empty string has scalar value 0 */
    }

    /*
     * Since base is at least 10, need not consider more than about 20 chars
     */
    if (slen > 20) {
        slen = 20;
    }

    /* Convert initial characters to fraction */
    base = range_hi - range_lo + 1;
    num = 0.0;
    denom = base;
    while (slen-- > 0) {
        int ch = (unsigned char)*value++;

        if (ch < range_lo) {
            ch = range_lo - 1;
        } else if (ch > range_hi) {
            ch = range_hi + 1;
        }
        num += ((double)(ch - range_lo)) / denom;
        denom *= base;
    }

    return num;
}

/*
 * Convert a string-type Datum into a palloc'd, null-terminated string.
 *
 * When using a non-C locale, we must pass the string through strxfrm()
 * before continuing, so as to generate correct locale-specific results.
 */
static char* convert_string_datum(Datum value, Oid typid)
{
    char* val = NULL;

    switch (typid) {
        case CHAROID:
            val = (char*)palloc(2);
            val[0] = DatumGetChar(value);
            val[1] = '\0';
            break;
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
            val = TextDatumGetCString(value);
            break;
        case NAMEOID: {
            NameData* nm = (NameData*)DatumGetPointer(value);

            val = pstrdup(NameStr(*nm));
            break;
        }
        default:

            /*
             * Can't get here unless someone tries to use scalarltsel on an
             * operator with one string and one non-string operand.
             */
            ereport(ERROR,
                (errmodule(MOD_OPT), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unsupported type: %u", typid))));

            return NULL;
    }

    if (!lc_collate_is_c(DEFAULT_COLLATION_OID)) {
        char* xfrm_str = NULL;
        size_t xfrm_len;
        size_t xfrm_len2 PG_USED_FOR_ASSERTS_ONLY;

        /*
         * Note: originally we guessed at a suitable output buffer size, and
         * only needed to call strxfrm twice if our guess was too small.
         * However, it seems that some versions of Solaris have buggy strxfrm
         * that can write past the specified buffer length in that scenario.
         * So, do it the dumb way for portability.
         *
         * Yet other systems (e.g., glibc) sometimes return a smaller value
         * from the second call than the first; thus the Assert must be <= not
         * == as you'd expect.  Can't any of these people program their way
         * out of a paper bag?
         *
         * XXX: strxfrm doesn't support UTF-8 encoding on Win32, it can return
         * bogus data or set an error. This is not really a problem unless it
         * crashes since it will only give an estimation error and nothing
         * fatal.
         */
#if _MSC_VER == 1400 /* VS.Net 2005 */

        /*
         *
         * http://connect.microsoft.com/VisualStudio/feedback/ViewFeedback.aspx?
         * FeedbackID=99694 */
        {
            char x[1];

            xfrm_len = strxfrm(x, val, 0);
        }
#else
        xfrm_len = strxfrm(NULL, val, 0);
#endif
#ifdef WIN32

        /*
         * On Windows, strxfrm returns INT_MAX when an error occurs. Instead
         * of trying to allocate this much memory (and fail), just return the
         * original string unmodified as if we were in the C locale.
         */
        if (xfrm_len == INT_MAX) {
            return val;
        }
#endif
        xfrm_str = (char*)palloc(xfrm_len + 1);
        xfrm_len2 = strxfrm(xfrm_str, val, xfrm_len + 1);
        Assert(xfrm_len2 <= xfrm_len);
        pfree_ext(val);
        val = xfrm_str;
    }

    return val;
}

/*
 * Do convert_to_scalar()'s work for any bytea data type.
 *
 * Very similar to convert_string_to_scalar except we can't assume
 * null-termination and therefore pass explicit lengths around.
 *
 * Also, assumptions about likely "normal" ranges of characters have been
 * removed - a data range of 0..255 is always used, for now.  (Perhaps
 * someday we will add information about actual byte data range to
 * pg_statistic.)
 */
static void convert_bytea_to_scalar(
    Datum value, double* scaled_value, Datum lo_bound, double* scaled_lo_bound, Datum hi_bound, double* scaled_hi_bound)
{
    int range_lo;
    int range_hi;
    int value_len = VARSIZE(DatumGetPointer(value)) - VARHDRSZ;
    int lo_bound_len = VARSIZE(DatumGetPointer(lo_bound)) - VARHDRSZ;
    int hi_bound_len = VARSIZE(DatumGetPointer(hi_bound)) - VARHDRSZ;
    int i;
    int min_len;
    unsigned char *val_str = (unsigned char*)VARDATA(DatumGetPointer(value));
    unsigned char *lo_str = (unsigned char*)VARDATA(DatumGetPointer(lo_bound));
    unsigned char *hi_str = (unsigned char*)VARDATA(DatumGetPointer(hi_bound));

    /*
     * Assume bytea data is uniformly distributed across all byte values.
     */
    range_lo = 0;
    range_hi = 255;

    /*
     * Now strip any common prefix of the three strings.
     */
    min_len = Min(Min(value_len, lo_bound_len), hi_bound_len);
    for (i = 0; i < min_len; i++) {
        if (*lo_str != *hi_str || *lo_str != *val_str) {
            break;
        }
        lo_str++, hi_str++, val_str++;
        lo_bound_len--, hi_bound_len--, value_len--;
    }

    /*
     * Now we can do the conversions.
     */
    *scaled_value = convert_one_bytea_to_scalar(val_str, value_len, range_lo, range_hi);
    *scaled_lo_bound = convert_one_bytea_to_scalar(lo_str, lo_bound_len, range_lo, range_hi);
    *scaled_hi_bound = convert_one_bytea_to_scalar(hi_str, hi_bound_len, range_lo, range_hi);
}

static double convert_one_bytea_to_scalar(unsigned char* value, int value_len, int range_lo, int range_hi)
{
    double num;
    double denom;
    double base;

    if (value_len <= 0) {
        return 0.0; /* empty string has scalar value 0 */
    }

    /*
     * Since base is 256, need not consider more than about 10 chars (even
     * this many seems like overkill)
     */
    if (value_len > 10) {
        value_len = 10;
    }

    /* Convert initial characters to fraction */
    base = range_hi - range_lo + 1;
    num = 0.0;
    denom = base;
    while (value_len-- > 0) {
        int ch = *value++;

        if (ch < range_lo) {
            ch = range_lo - 1;
        } else if (ch > range_hi) {
            ch = range_hi + 1;
        }
        num += ((double)(ch - range_lo)) / denom;
        denom *= base;
    }

    return num;
}

/*
 * Do convert_to_scalar()'s work for any timevalue data type.
 */
static double convert_timevalue_to_scalar(Datum value, Oid typid)
{
    switch (typid) {
        case TIMESTAMPOID:
            return DatumGetTimestamp(value);
        case TIMESTAMPTZOID:
            return DatumGetTimestampTz(value);
        case ABSTIMEOID:
            return DatumGetTimestamp(DirectFunctionCall1(abstime_timestamp, value));
        case DATEOID:
            return date2timestamp_no_overflow(DatumGetDateADT(value));
        case INTERVALOID: {
            Interval* interval = DatumGetIntervalP(value);

            /*
             * Convert the month part of Interval to days using assumed
             * average month length of 365.25/12.0 days.  Not too
             * accurate, but plenty good enough for our purposes.
             */
#ifdef HAVE_INT64_TIMESTAMP
            return interval->time + interval->day * (double)USECS_PER_DAY +
                   interval->month * ((DAYS_PER_YEAR / (double)MONTHS_PER_YEAR) * USECS_PER_DAY);
#else
            return interval->time + interval->day * SECS_PER_DAY +
                   interval->month * ((DAYS_PER_YEAR / (double)MONTHS_PER_YEAR) * (double)SECS_PER_DAY);
#endif
        }
        case RELTIMEOID:
#ifdef HAVE_INT64_TIMESTAMP
            return (DatumGetRelativeTime(value) * 1000000.0);
#else
            return DatumGetRelativeTime(value);
#endif
        case TINTERVALOID: {
            TimeInterval tinterval = DatumGetTimeInterval(value);

#ifdef HAVE_INT64_TIMESTAMP
            if (tinterval->status != 0) {
                return ((tinterval->data[1] - tinterval->data[0]) * 1000000.0);
            }
#else
            if (tinterval->status != 0) {
                return tinterval->data[1] - tinterval->data[0];
            }
#endif
            return 0; /* for lack of a better idea */
        }
        case TIMEOID:
            return DatumGetTimeADT(value);
        case TIMETZOID: {
            TimeTzADT* timetz = DatumGetTimeTzADTP(value);

            /* use GMT-equivalent time */
#ifdef HAVE_INT64_TIMESTAMP
            return (double)(timetz->time + (timetz->zone * 1000000.0));
#else
            return (double)(timetz->time + timetz->zone);
#endif
        }
        default:
            break;
    }

    /*
     * Can't get here unless someone tries to use scalarltsel/scalargtsel on
     * an operator with one timevalue and one non-timevalue operand.
     */
    ereport(
        ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unsupported type: %u", typid))));

    return 0;
}

/*
 * get_restriction_variable
 *		Examine the args of a restriction clause to see if it's of the
 *		form (variable op pseudoconstant) or (pseudoconstant op variable),
 *		where "variable" could be either a Var or an expression in vars of a
 *		single relation.  If so, extract information about the variable,
 *		and also indicate which side it was on and the other argument.
 *
 * Inputs:
 *	root: the planner info
 *	args: clause argument list
 *	varRelid: see specs for restriction selectivity functions
 *
 * Outputs: (these are valid only if TRUE is returned)
 *	*vardata: gets information about variable (see examine_variable)
 *	*other: gets other clause argument, aggressively reduced to a constant
 *	*varonleft: set TRUE if variable is on the left, FALSE if on the right
 *
 * Returns TRUE if a variable is identified, otherwise FALSE.
 *
 * Note: if there are Vars on both sides of the clause, we must fail, because
 * callers are expecting that the other side will act like a pseudoconstant.
 */
bool get_restriction_variable(
    PlannerInfo* root, List* args, int var_relid, VariableStatData* var_data, Node** other, bool* var_on_left)
{
    Node* left = NULL;
    Node* right = NULL;
    VariableStatData r_data;
    r_data.statsTuple = NULL;
    r_data.freefunc = NULL;
    r_data.rel = NULL;
    r_data.var = NULL;

    /* Fail if not a binary opclause (probably shouldn't happen) */
    if (list_length(args) != 2) {
        return false;
    }

    left = (Node*)linitial(args);
    right = (Node*)lsecond(args);

    /*
     * Examine both sides.	Note that when var_relid is nonzero, Vars of other
     * relations will be treated as pseudoconstants.
     */
    examine_variable(root, left, var_relid, var_data);
    examine_variable(root, right, var_relid, &r_data);

    /*
     * If one side is a variable and the other not, we win.
     */
    if (var_data->rel && r_data.rel == NULL) {
        *var_on_left = true;
        *other = estimate_expression_value(root, r_data.var);
        /* Assume we need no ReleaseVariableStats(r_data) here */
        return true;
    }

    if (var_data->rel == NULL && r_data.rel) {
        *var_on_left = false;
        *other = estimate_expression_value(root, var_data->var);
        /* Assume we need no ReleaseVariableStats(*var_data) here */
        *var_data = r_data;
        return true;
    }

    /* Ooops, clause has wrong structure (probably var op var) */
    ReleaseVariableStats(*var_data);
    ReleaseVariableStats(r_data);

    return false;
}

/*
 * get_join_variables
 *		Apply examine_variable() to each side of a join clause.
 *		Also, attempt to identify whether the join clause has the same
 *		or reversed sense compared to the SpecialJoinInfo.
 *
 * We consider the join clause "normal" if it is "lhs_var OP rhs_var",
 * or "reversed" if it is "rhs_var OP lhs_var".  In complicated cases
 * where we can't tell for sure, we default to assuming it's normal.
 */
void get_join_variables(PlannerInfo* root, List* args, SpecialJoinInfo* sj_info, VariableStatData* var_data1,
    VariableStatData* var_data2, bool* join_is_reversed)
{
    Node* left = NULL;
    Node* right = NULL;

    if (list_length(args) != 2) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("join operator should take two arguments"))));
    }

    left = (Node*)linitial(args);
    right = (Node*)lsecond(args);

    examine_variable(root, left, 0, var_data1);
    examine_variable(root, right, 0, var_data2);

    if (var_data1->rel && bms_is_subset(var_data1->rel->relids, sj_info->syn_righthand)) {
        *join_is_reversed = true; /* var1 is on RHS */
    } else if (var_data2->rel && bms_is_subset(var_data2->rel->relids, sj_info->syn_lefthand)) {
        *join_is_reversed = true; /* var2 is on LHS */
    } else {
        *join_is_reversed = false;
    }
}

/*
 * examine_variable
 *		Try to look up statistical data about an expression.
 *		Fill in a VariableStatData struct to describe the expression.
 *
 * Inputs:
 *	root: the planner info
 *	node: the expression tree to examine
 *	varRelid: see specs for restriction selectivity functions
 *
 * Outputs: *vardata is filled as follows:
 *	var: the input expression (with any binary relabeling stripped, if
 *		it is or contains a variable; but otherwise the type is preserved)
 *	rel: RelOptInfo for relation containing variable; NULL if expression
 *		contains no Vars (NOTE this could point to a RelOptInfo of a
 *		subquery, not one in the current query).
 *	statsTuple: the pg_statistic entry for the variable, if one exists;
 *		otherwise NULL.
 *	freefunc: pointer to a function to release statsTuple with.
 *	vartype: exposed type of the expression; this should always match
 *		the declared input type of the operator we are estimating for.
 *	atttype, atttypmod: type data to pass to get_attstatsslot().  This is
 *		commonly the same as the exposed type of the variable argument,
 *		but can be different in binary-compatible-type cases.
 *	isunique: TRUE if we were able to match the var to a unique index or a
 *		single-column DISTINCT clause, implying its values are unique for
 *		this query.  (Caution: this should be trusted for statistical
 *		purposes only, since we do not check indimmediate nor verify that
 *		the exact same definition of equality applies.)
 *  aclOk: TRUE if current user has permission to read the column(s)
 *      underlying the pg_statistic entry. This is consulted
 *      by StatisticProcSecurityCheck.
 *
 * Caller is responsible for doing ReleaseVariableStats() before exiting.
 */
void examine_variable(PlannerInfo* root, Node* node, int var_relid, VariableStatData* var_data)
{
    Node* base_node = NULL;
    Relids varnos;
    RelOptInfo* one_rel = NULL;

    /* Make sure we don't return dangling pointers in var_data */
    errno_t rc = memset_s(var_data, sizeof(VariableStatData), 0, sizeof(VariableStatData));
    securec_check(rc, "\0", "\0");

    /* we enable possion to estimate distinct for default. */
    var_data->enablePossion = true;
    /* Save the exposed type of the expression */
    var_data->vartype = exprType(node);

    /* Look inside any binary-compatible relabeling */
    if (IsA(node, RelabelType)) {
        base_node = (Node*)((RelabelType*)node)->arg;
    } else {
        base_node = node;
    }

    /* Fast path for a simple Var */
    if (IsA(base_node, Var) && (var_relid == 0 || (uint)var_relid == ((Var*)base_node)->varno)) {
        Var* var = (Var*)base_node;

        /* Set up result fields other than the stats tuple */
        var_data->var = base_node; /* return Var without relabeling */
        var_data->rel = find_base_rel(root, var->varno);
        var_data->atttype = var->vartype;
        var_data->atttypmod = var->vartypmod;
        var_data->isunique = has_unique_index(var_data->rel, var->varattno);

        /* Try to locate some stats */
        examine_simple_variable(root, var, var_data);

        return;
    }

    /*
     * Okay, it's a more complicated expression.  Determine variable
     * membership.	Note that when var_relid isn't zero, only vars of that
     * relation are considered "real" vars.
     */
    varnos = pull_varnos(base_node);
    one_rel = NULL;

    switch (bms_membership(varnos)) {
        case BMS_EMPTY_SET:
            /* No Vars at all ... must be pseudo-constant clause */
            break;
        case BMS_SINGLETON:
            if (var_relid == 0 || bms_is_member(var_relid, varnos)) {
                one_rel = find_base_rel(root, (var_relid ? var_relid : bms_singleton_member(varnos)));
                var_data->rel = one_rel;
                node = base_node; /* strip any relabeling */
            }
            /* else treat it as a constant */
            break;
        case BMS_MULTIPLE:
            if (var_relid == 0) {
                /* treat it as a variable of a join relation */
                var_data->rel = find_join_rel(root, varnos);
                node = base_node; /* strip any relabeling */
            } else if (bms_is_member(var_relid, varnos)) {
                /* ignore the vars belonging to other relations */
                var_data->rel = find_base_rel(root, var_relid);
                node = base_node; /* strip any relabeling */
                                 /* note: no point in expressional-index search here */
            }
            /* else treat it as a constant */
            break;
        default:
            break;
    }

    bms_free(varnos);
    varnos = NULL;

    var_data->var = node;
    var_data->atttype = exprType(node);
    var_data->atttypmod = exprTypmod(node);

    if (one_rel != NULL) {
        /*
         * We have an expression in vars of a single relation.	Try to match
         * it to expressional index columns, in hopes of finding some
         * statistics.
         */
        ListCell* ilist = NULL;

        foreach (ilist, one_rel->indexlist) {
            IndexOptInfo* index = (IndexOptInfo*)lfirst(ilist);
            ListCell* indexpr_item = NULL;
            int pos;

            indexpr_item = list_head(index->indexprs);
            if (indexpr_item == NULL) {
                continue; /* no expressions here... */
            }

            for (pos = 0; pos < index->ncolumns; pos++) {
                if (index->indexkeys[pos] == 0) {
                    Node* index_key = NULL;

                    if (indexpr_item == NULL) {
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("too few entries in indexprs list"))));
                    }

                    index_key = (Node*)lfirst(indexpr_item);
                    if (index_key && IsA(index_key, RelabelType))
                        index_key = (Node*)((RelabelType*)index_key)->arg;
                    if (equal(node, index_key)) {
                        /*
                         * Found a match ... is it a unique index? Tests here
                         * should match has_unique_index().
                         */
                        if (index->unique && index->nkeycolumns == 1 && (index->indpred == NIL || index->predOK))
                            var_data->isunique = true;

                        /*
                         * Has it got stats?  We only consider stats for
                         * non-partial indexes, since partial indexes probably
                         * don't reflect whole-relation statistics; the above
                         * check for uniqueness is the only info we take from
                         * a partial index.
                         *
                         * An index stats hook, however, must make its own
                         * decisions about what to do with partial indexes.
                         */
                        if (index->indpred == NIL) {
                            /*
                             * we don't use statistics of partition or index partition
                             * currently, so if the index is index partition, just use
                             * the partition index statistic.
                             */
                            char stakind = STARELKIND_CLASS;
                            Oid indexid = index->indexoid;
                            char relPersistence = get_rel_persistence(index->indexoid);
                            
                            if (u_sess->attr.attr_common.upgrade_mode != 0) {
                                var_data->statsTuple = NULL;
                                var_data->freefunc = ReleaseSysCache;
                            } else if (relPersistence == RELPERSISTENCE_GLOBAL_TEMP) {
                                var_data->statsTuple = get_gtt_att_statistic(index->indexoid,
                                                                             Int16GetDatum(pos + 1));
                                var_data->freefunc = release_gtt_statistic_cache;
                            } else {
                                var_data->statsTuple = SearchSysCache4(STATRELKINDATTINH,
                                    ObjectIdGetDatum(indexid),
                                    CharGetDatum(stakind),
                                    Int16GetDatum(pos + 1),
                                    BoolGetDatum(false));
                                var_data->freefunc = ReleaseSysCache;
                            }
                            if (HeapTupleIsValid(var_data->statsTuple)) {
                                /* Get index's table for permission check */
                                RangeTblEntry *rte;

                                rte = planner_rt_fetch((int)index->rel->relid, root);
                                Assert(rte->rtekind == RTE_RELATION);

                                /*
                                 * For simplicity, we insist on the whole
                                 * table being selectable, rather than trying
                                 * to identify which column(s) the index
                                 * depends on.
                                 */
                                var_data->aclOk =
                                    (pg_class_aclcheck(rte->relid, GetUserId(), ACL_SELECT) == ACLCHECK_OK);
                            } else {
                                /* suppress leakproofness checks later */
                                var_data->aclOk = true;
                            }
                        }
                        if (var_data->statsTuple) {
                            break;
                        }
                    }
                    indexpr_item = lnext(indexpr_item);
                }
            }
            if (var_data->statsTuple) {
                break;
            }
        }

        if (!HeapTupleIsValid(var_data->statsTuple) && !contain_var_unsubstitutable_functions(base_node)) {
            List* vars = NIL;
            vars = pull_var_clause(base_node, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

            if (list_length(vars) == 1) {
                base_node = (Node*)linitial(vars);

                if (!IsA(node, Aggref) && !IsA(node, PlaceHolderVar) && !IsA(node, GroupingFunc) &&
                    !IsA(node, GroupingId)) {
                    Assert(IsA(base_node, Var));
                    if (var_relid == 0 || (uint)var_relid == ((Var*)base_node)->varno) {
                        Var* var = (Var*)base_node;

                        /* Set up result fields other than the stats tuple */
                        var_data->var = base_node; /* return Var without relabeling */
                        var_data->rel = find_base_rel(root, var->varno);
                        var_data->atttype = var->vartype;
                        var_data->atttypmod = var->vartypmod;
                        var_data->isunique = has_unique_index(var_data->rel, var->varattno);

                        /* Try to locate some stats */
                        examine_simple_variable(root, var, var_data);
                    }
                }
            }
        }
    }
}

/*
 * examine_simple_variable
 *		Handle a simple Var for examine_variable
 *
 * This is split out as a subroutine so that we can recurse to deal with
 * Vars referencing subqueries.
 *
 * We already filled in all the fields of *vardata except for the stats tuple.
 */
static void examine_simple_variable(PlannerInfo* root, Var* var, VariableStatData* var_data)
{
    RangeTblEntry* rte = root->simple_rte_array[var->varno];

    Assert(IsA(rte, RangeTblEntry));

    if (rte->rtekind == RTE_RELATION) {
        char sta_kind = STARELKIND_CLASS;
        Oid sta_relid = rte->relid;

        /*
         * get parent table's statistic for each partition if have no statistic, because
         * there is no state info in pg_statistic for each partition, only partition
         * table have state info in pg_statistic.
         *
         * We do not search system cache in upgrading
         */
        char relPersistence = get_rel_persistence(rte->relid);
        if (u_sess->attr.attr_common.upgrade_mode != 0) {
            var_data->statsTuple = NULL;
            var_data->freefunc = ReleaseSysCache;
        } else if (relPersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            var_data->statsTuple = get_gtt_att_statistic(rte->relid, var->varattno);
            var_data->freefunc = release_gtt_statistic_cache;
        } else {
            var_data->statsTuple = SearchSysCache4(STATRELKINDATTINH,
                ObjectIdGetDatum(sta_relid),
                CharGetDatum(sta_kind),
                Int16GetDatum(var->varattno),
                BoolGetDatum(rte->inh));
            var_data->freefunc = ReleaseSysCache;
        }
        
        if (HeapTupleIsValid(var_data->statsTuple)) {
            /* check if user has permission to read this column */
            var_data->aclOk = (pg_class_aclcheck(rte->relid, GetUserId(), ACL_SELECT) == ACLCHECK_OK) ||
                (pg_attribute_aclcheck(rte->relid, var->varattno, GetUserId(), ACL_SELECT) == ACLCHECK_OK);
        } else {
            /* suppress any possible leakproofness checks later */
            var_data->aclOk = true;
        }

#ifdef PGXC
        if ((sta_relid >= FirstNormalObjectId) && (var->varattno > SelfItemPointerAttributeNumber)) {
            if (!HeapTupleIsValid(var_data->statsTuple) && IS_PGXC_COORDINATOR) {
                /* Save no analyzed reloid if is coordinator. */
                set_noanalyze_rellist(rte->relid, var->varattno);
            }
        }
#endif
    } else if (rte->rtekind == RTE_SUBQUERY && !rte->inh) {
        /*
         * Plain subquery (not one that was converted to an appendrel).
         */
        Query* sub_query = rte->subquery;
        RelOptInfo* rel = NULL;
        TargetEntry* ste = NULL;

        /*
         * Punt if it's a whole-row var rather than a plain column reference.
         */
        if (var->varattno == InvalidAttrNumber) {
            return;
        }

        /*
         * Punt if sub_query uses set operations or GROUP BY, as these will
         * mash underlying columns' stats beyond recognition.  (Set ops are
         * particularly nasty; if we forged ahead, we would return stats
         * relevant to only the leftmost subselect...)	DISTINCT is also
         * problematic, but we check that later because there is a possibility
         * of learning something even with it.
         */
        if (sub_query->setOperations || sub_query->groupClause) {
            return;
        }

        /*
         * OK, fetch RelOptInfo for sub_query.  Note that we don't change the
         * rel returned in var_data, since caller expects it to be a rel of the
         * caller's query level.  Because we might already be recursing, we
         * can't use that rel pointer either, but have to look up the Var's
         * rel afresh.
         */
        rel = find_base_rel(root, var->varno);

        /* If the sub_query hasn't been planned yet, we have to punt */
        if (rel->subroot == NULL) {
            return;
        }
        Assert(IsA(rel->subroot, PlannerInfo));

        /*
         * Switch our attention to the sub_query as mangled by the planner. It
         * was okay to look at the pre-planning version for the tests above,
         * but now we need a Var that will refer to the subroot's live
         * RelOptInfos.  For instance, if any sub_query pullup happened during
         * planning, Vars in the targetlist might have gotten replaced, and we
         * need to see the replacement expressions.
         */
        sub_query = rel->subroot->parse;
        Assert(IsA(sub_query, Query));

        /* Get the sub_query output expression referenced by the upper Var */
        ste = get_tle_by_resno(sub_query->targetList, var->varattno);
        if (ste == NULL || ste->resjunk) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_INVALID_ATTRIBUTE),
                        errmsg("sub_query %s does not have attribute %d", rte->eref->aliasname, var->varattno))));
        }

        var = (Var*)ste->expr;

        /*
         * If sub_query uses DISTINCT, we can't make use of any stats for the
         * variable ... but, if it's the only DISTINCT column, we are entitled
         * to consider it unique.  We do the test this way so that it works
         * for cases involving DISTINCT ON.
         */
        if (sub_query->distinctClause) {
            if (list_length(sub_query->distinctClause) == 1 &&
                targetIsInSortList(ste, InvalidOid, sub_query->distinctClause)) {
                var_data->isunique = true;
            }
            /* cannot go further */
            return;
        }

        /*
         * If the sub-query originated from a view with the security_barrier
         * attribute, we must not look at the variable's statistics, though it
         * seems all right to notice the existence of a DISTINCT clause. So
         * stop here.
         *
         * This is probably a harsher restriction than necessary; it's
         * certainly OK for the selectivity estimator (which is a C function,
         * and therefore omnipotent anyway) to look at the statistics.	But
         * many selectivity estimators will happily *invoke the operator
         * function* to try to work out a good estimate - and that's not OK.
         * So for now, don't dig down for stats.
         */
        if (rte->security_barrier) {
            return;
        }

        /* Can only handle a simple Var of sub_query's query level */
        if (var && IsA(var, Var) && var->varlevelsup == 0) {
            /*
             * OK, recurse into the sub_query.  Note that the original setting
             * of var_data->isunique (which will surely be false) is left
             * unchanged in this situation.  That's what we want, since even
             * if the underlying column is unique, the sub_query may have
             * joined to other tables in a way that creates duplicates.
             */
            examine_simple_variable(rel->subroot, var, var_data);
        }
    } else {
        /*
         * Otherwise, the Var comes from a FUNCTION, VALUES, or CTE RTE.  (We
         * won't see RTE_JOIN here because join alias Vars have already been
         * flattened.)	There's not much we can do with function outputs, but
         * maybe someday try to be smarter about VALUES and/or CTEs.
         */
    }
}

/*
 * Check whether it is permitted to call func_oid passing some of the
 * pg_statistic data in vardata.  We allow this either if the user has SELECT
 * privileges on the table or column underlying the pg_statistic data or if
 * the function is marked leak-proof.
 */
bool StatisticProcSecurityCheck(const VariableStatData *vardata, Oid funcOid)
{
    if (vardata->aclOk) {
        return true;
    }

    if (!OidIsValid(funcOid)) {
        return false;
    }

    if (get_func_leakproof(funcOid)) {
        return true;
    }

    ereport(DEBUG2,
        (errmsg_internal("not using statistics because function \"%s\" is not leak-proof", get_func_name(funcOid))));
    return false;
}

/*
 * get_variable_numdistinct
 *	  Estimate the number of distinct values of a variable.
 *
 * vardata: results of examine_variable
 * *isdefault: set to TRUE if the result is a default rather than based on
 * anything meaningful.
 *
 * relid: adjusting rows for rel after joining with other relation
 * all_baserels: identify all baserels include in the local root when sjinfo is null,
 *         we can use it to judge whether we can use possion or not
 *
 * NB: be careful to produce a positive integral result, since callers may
 * compare the result to exact integer counts, or might divide by it.
 */
double get_variable_numdistinct(VariableStatData* var_data, bool* is_default, bool adjust_rows, double join_ratio,
    SpecialJoinInfo* sj_info, STATS_EST_TYPE e_type)
{
    double sta_distinct;
    double sta_null_frac = 0.0;
    double ntuples;
    double adjust_ratio = 1.0;
    double var_ratio = 1.0;

    *is_default = false;

    if (adjust_rows) {
        if (var_data->rel && var_data->rel->rows != 0 && var_data->rel->tuples != 0) {
            /* multiply by selectivity */
            adjust_ratio = var_data->rel->rows * join_ratio / var_data->rel->tuples;
        }
    }

    /*
     * Determine the sta_distinct value to use.	There are cases where we can
     * get an estimate even without a pg_statistic entry, or can get a better
     * value than is in pg_statistic. Grab sta_null_frac too if we can find it
     * (otherwise, assume no nulls, for lack of any better idea).
     */
    if (HeapTupleIsValid(var_data->statsTuple)) {
        /* Use the pg_statistic entry */
        Form_pg_statistic stats;

        stats = (Form_pg_statistic)GETSTRUCT(var_data->statsTuple);

        if (IS_PGXC_COORDINATOR && e_type == STATS_TYPE_LOCAL) {
            sta_distinct = get_attstadndistinct(var_data->statsTuple); /* Local distinct */
        } else {
            sta_distinct = stats->stadistinct;
        }
        sta_null_frac = stats->stanullfrac;
    } else if (var_data->vartype == BOOLOID) {
        /*
         * Special-case boolean columns: presumably, two distinct values.
         *
         * Are there any other datatypes we should wire in special estimates
         * for?
         */
        sta_distinct = 2.0;
        adjust_ratio = 1.0;
    } else {
        /*
         * We don't keep statistics for system columns, but in some cases we
         * can infer distinctness anyway.
         */
        if (var_data->var && IsA(var_data->var, Var)) {
            switch (((Var*)var_data->var)->varattno) {
                case ObjectIdAttributeNumber:
                case SelfItemPointerAttributeNumber:
                    sta_distinct = -1.0; /* unique */
                    break;
                case TableOidAttributeNumber:
                    sta_distinct = 1.0; /* only 1 value */
                    break;
#ifdef PGXC
                case XC_NodeIdAttributeNumber:
                    sta_distinct = 1.0; /* only 1 value */
                    break;
                case BucketIdAttributeNumber:
                    sta_distinct = 1.0; /* only 1 value */
                    break;
#endif
                default:
                    sta_distinct = 0.0; /* means "unknown" */
                    break;
            }
        } else {
            sta_distinct = 0.0; /* means "unknown" */
        }

        /*
         * XXX consider using estimate_num_groups on expressions?
         */
    }

    /*
     * If there is a unique index or DISTINCT clause for the variable, assume
     * it is unique no matter what pg_statistic says; the statistics could be
     * out of date, or we might have found a partial unique index that proves
     * the var is unique for this query. However, we'd better still believe
     * the null-fraction statistic.
     */
    if (var_data->isunique) {
        sta_distinct = -1.0 * (1.0 - sta_null_frac);
    }

    /*
     * Otherwise we need to get the relation size; punt if not available.
     */
    if (var_data->rel == NULL) {
        *is_default = true;
        var_data->enablePossion = false;
        return DEFAULT_NUM_DISTINCT;
    }

    if (e_type == STATS_TYPE_LOCAL) {
        ntuples = get_local_rows(var_data->rel->tuples,
            var_data->rel->multiple,
            IsLocatorReplicated(var_data->rel->locator_type),
            ng_get_dest_num_data_nodes(var_data->rel));
    } else {
        ntuples = var_data->rel->tuples;
    }

    /*
     * If we had an absolute estimate, use that.
     */
    if (sta_distinct > 0.0) {
        /*
         * use possion model if satisify condition which the column of join or filter and var_data->var art different,
         * but they are belong to the same relation.
         * only global stat can use possion because var_ratio computed by global distinct.
         */
        if (var_data->enablePossion && (e_type == STATS_TYPE_GLOBAL) && (ntuples > 0.0) &&
            can_use_possion(var_data, sj_info, &var_ratio)) {
            double tmp = sta_distinct;
            sta_distinct = NUM_DISTINCT_SELECTIVITY_FOR_POISSON(sta_distinct, ntuples, var_ratio);
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("The origin distinct value is %f. After using possion model with ntuples=%f and ration=%f \
                        The new distinct value is %f",
                        tmp,
                        ntuples,
                        var_ratio,
                        sta_distinct))));
        } else {
            /* multiply by selectivity */
            double tmp = sta_distinct;
            sta_distinct = sta_distinct * adjust_ratio;
            var_data->enablePossion = false;
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("The origin distinct value is %f. After multiply by selectivity with adjust_ratio=%f, \
                        the new distinct value is %f",
                        tmp,
                        adjust_ratio,
                        sta_distinct))));
        }
        return clamp_row_est(sta_distinct);
    }

    if (ntuples <= 0.0) {
        *is_default = true;
        var_data->enablePossion = false;
        return DEFAULT_NUM_DISTINCT;
    }

    if (sta_distinct < 0.0) {
        /*
         * use possion model if satisify condition which the column of join or filter and var_data->var art different,
         * but they are belong to the same relation.
         * only global stat can use possion because var_ratio computed by global distinct.
         */
        if (var_data->enablePossion && (e_type == STATS_TYPE_GLOBAL) &&
            can_use_possion(var_data, sj_info, &var_ratio)) {
            sta_distinct = NUM_DISTINCT_SELECTIVITY_FOR_POISSON(-sta_distinct * ntuples, ntuples, var_ratio);
        } else {
            /* multiply by selectivity */
            sta_distinct = (-sta_distinct * ntuples) * adjust_ratio;
            var_data->enablePossion = false;
        }
        return clamp_row_est(sta_distinct);
    }

    /*
     * With no data, estimate ndistinct = ntuples if the table is small, else
     * use default.  We use DEFAULT_NUM_DISTINCT as the cutoff for "small" so
     * that the behavior isn't discontinuous.
     */
    if (ntuples < DEFAULT_NUM_DISTINCT) {
        var_data->enablePossion = false;
        return clamp_row_est(ntuples);
    }

    *is_default = true;
    var_data->enablePossion = false;
    return DEFAULT_NUM_DISTINCT;
}

/*
 * get_variable_range
 *		Estimate the minimum and maximum value of the specified variable.
 *		If successful, store values in *min and *max, and return TRUE.
 *		If no data available, return FALSE.
 *
 * sortop is the "<" comparison operator to use.  This should generally
 * be "<" not ">", as only the former is likely to be found in pg_statistic.
 */
static bool get_variable_range(PlannerInfo* root, VariableStatData* var_data, Oid sort_op, Datum* min, Datum* max)
{
    Datum tmin = 0;
    Datum tmax = 0;
    bool have_data = false;
    int16 typ_len;
    bool typ_by_val = false;
    Oid opfuncoid;
    Datum* values = NULL;
    int n_values;
    int i;

    /*
     * XXX It's very tempting to try to use the actual column min and max, if
     * we can get them relatively-cheaply with an index probe.	However, since
     * this function is called many times during join planning, that could
     * have unpleasant effects on planning speed.  Need more investigation
     * before enabling this.
     */
#ifdef NOT_USED
    if (get_actual_variable_range(root, var_data, sort_op, min, max)) {
        return true;
    }
#endif

    if (!HeapTupleIsValid(var_data->statsTuple)) {
        /* no stats available, so default result */
        return false;
    }

    /*
     * If we can't apply the sortop to the stats data, just fail.  In
     * principle, if there's a histogram and no MCVs, we could return the
     * histogram endpoints without ever applying the sortop ... but it's
     * probably not worth trying, because whatever the caller wants to do with
     * the endpoints would likely fail the security check too.
     */
    if (!StatisticProcSecurityCheck(var_data, (opfuncoid = get_opcode(sort_op)))) {
        return false;
    }

    get_typlenbyval(var_data->atttype, &typ_len, &typ_by_val);

    /*
     * If there is a histogram, grab the first and last values.
     *
     * If there is a histogram that is sorted with some other operator than
     * the one we want, fail --- this suggests that there is data we can't
     * use.
     */
    if (get_attstatsslot(var_data->statsTuple,
            var_data->atttype,
            var_data->atttypmod,
            STATISTIC_KIND_HISTOGRAM,
            sort_op,
            NULL,
            &values,
            &n_values,
            NULL,
            NULL)) {
        if (n_values > 0) {
            tmin = datumCopy(values[0], typ_by_val, typ_len);
            tmax = datumCopy(values[n_values - 1], typ_by_val, typ_len);
            have_data = true;
        }
        free_attstatsslot(var_data->atttype, values, n_values, NULL, 0);
    } else if (get_attstatsslot(var_data->statsTuple,
                   var_data->atttype,
                   var_data->atttypmod,
                   STATISTIC_KIND_HISTOGRAM,
                   InvalidOid,
                   NULL,
                   &values,
                   &n_values,
                   NULL,
                   NULL)) {
        free_attstatsslot(var_data->atttype, values, n_values, NULL, 0);
        return false;
    }

    /*
     * If we have most-common-values info, look for extreme MCVs.  This is
     * needed even if we also have a histogram, since the histogram excludes
     * the MCVs.  However, usually the MCVs will not be the extreme values, so
     * avoid unnecessary data copying.
     */
    if (get_attstatsslot(var_data->statsTuple,
            var_data->atttype,
            var_data->atttypmod,
            STATISTIC_KIND_MCV,
            InvalidOid,
            NULL,
            &values,
            &n_values,
            NULL,
            NULL)) {
        bool tmin_is_mcv = false;
        bool tmax_is_mcv = false;
        FmgrInfo op_proc;

        fmgr_info(opfuncoid, &op_proc);

        for (i = 0; i < n_values; i++) {
            if (!have_data) {
                tmin = tmax = values[i];
                tmin_is_mcv = tmax_is_mcv = have_data = true;
                continue;
            }
            if (DatumGetBool(FunctionCall2Coll(&op_proc, DEFAULT_COLLATION_OID, values[i], tmin))) {
                tmin = values[i];
                tmin_is_mcv = true;
            }
            if (DatumGetBool(FunctionCall2Coll(&op_proc, DEFAULT_COLLATION_OID, tmax, values[i]))) {
                tmax = values[i];
                tmax_is_mcv = true;
            }
        }
        if (tmin_is_mcv) {
            tmin = datumCopy(tmin, typ_by_val, typ_len);
        }
        if (tmax_is_mcv) {
            tmax = datumCopy(tmax, typ_by_val, typ_len);
        }
        free_attstatsslot(var_data->atttype, values, n_values, NULL, 0);
    }

    *min = tmin;
    *max = tmax;
    return have_data;
}

/*
 * get_actual_variable_range
 *		Attempt to identify the current *actual* minimum and/or maximum
 *		of the specified variable, by looking for a suitable btree index
 *		and fetching its low and/or high values.
 *		If successful, store values in *min and *max, and return TRUE.
 *		(Either pointer can be NULL if that endpoint isn't needed.)
 *		If no data available, return FALSE.
 *
 * sortop is the "<" comparison operator to use.
 */
static bool get_actual_variable_range(PlannerInfo* root, VariableStatData* var_data, Oid sort_op,
    Datum* min, Datum* max)
{
    bool have_data = false;
    RelOptInfo* rel = var_data->rel;
    RangeTblEntry* rte = NULL;
    ListCell* lc = NULL;

    /* No hope if no relation or it doesn't have indexes */
    if (rel == NULL || rel->indexlist == NIL) {
        return false;
    }

    if (rel->fdwroutine != NULL)
        return false;

    /* If it has indexes it must be a plain relation */
    rte = root->simple_rte_array[rel->relid];
    Assert(rte->rtekind == RTE_RELATION);

    if (rte->ispartrel || rte->relhasbucket) {
        return false;
    }

    /* Search through the indexes to see if any match our problem */
    foreach (lc, rel->indexlist) {
        IndexOptInfo* index = (IndexOptInfo*)lfirst(lc);
        ScanDirection index_scan_dir;

        /* Ignore non-btree indexes */
        if (index->relam != BTREE_AM_OID) {
            continue;
        }

        /*
         * Ignore partial indexes --- we only want stats that cover the entire
         * relation.
         */
        if (index->indpred != NIL) {
            continue;
        }

        /*
         * The index list might include hypothetical indexes inserted by a
         * get_relation_info hook --- don't try to access them.
         */
        if (index->hypothetical) {
            continue;
        }

        /*
         * The first index column must match the desired variable and sort
         * operator --- but we can use a descending-order index.
         */
        if (!match_index_to_operand(var_data->var, 0, index))
            continue;
        switch (get_op_opfamily_strategy(sort_op, index->sortopfamily[0])) {
            case BTLessStrategyNumber:
                if (index->reverse_sort[0]) {
                    index_scan_dir = BackwardScanDirection;
                } else {
                    index_scan_dir = ForwardScanDirection;
                }
                break;
            case BTGreaterStrategyNumber:
                if (index->reverse_sort[0]) {
                    index_scan_dir = ForwardScanDirection;
                } else {
                    index_scan_dir = BackwardScanDirection;
                }
                break;
            default:
                /* index doesn't match the sort_op */
                continue;
        }

        /*
         * Found a suitable index to extract data from.  We'll need an EState
         * and a bunch of other infrastructure.
         */
        {
            EState* estate = NULL;
            ExprContext* econtext = NULL;
            MemoryContext tmp_context;
            MemoryContext old_context;
            Relation heap_rel;
            Relation index_rel;
            IndexInfo* index_info = NULL;
            TupleTableSlot* slot = NULL;
            int16 typ_len;
            bool typ_by_val = false;
            ScanKeyData scan_keys[1];
            IndexScanDesc index_scan;
            HeapTuple tup;
            Datum values[INDEX_MAX_KEYS];
            bool is_null[INDEX_MAX_KEYS];

            estate = CreateExecutorState();
            econtext = GetPerTupleExprContext(estate);
            /* Make sure any cruft is generated in the econtext's memory */
            tmp_context = econtext->ecxt_per_tuple_memory;
            old_context = MemoryContextSwitchTo(tmp_context);

            /*
             * Open the table and index so we can read from them.  We should
             * already have at least AccessShareLock on the table, but not
             * necessarily on the index.
             */
            heap_rel = heap_open(rte->relid, NoLock);
            index_rel = index_open(index->indexoid, AccessShareLock);

            /* extract index key information from the index's pg_index info */
            index_info = BuildIndexInfo(index_rel);

            /* some other stuff */
            slot = MakeSingleTupleTableSlot(RelationGetDescr(heap_rel));
            econtext->ecxt_scantuple = slot;
            get_typlenbyval(var_data->atttype, &typ_len, &typ_by_val);

            /* set up an IS NOT NULL scan key so that we ignore nulls */
            ScanKeyEntryInitialize(&scan_keys[0],
                SK_ISNULL | SK_SEARCHNOTNULL,
                1,               /* index col to scan */
                InvalidStrategy, /* no strategy */
                InvalidOid,      /* no strategy subtype */
                InvalidOid,      /* no collation */
                InvalidOid,      /* no reg proc for this */
                (Datum)0);       /* constant */

            have_data = true;

            /* If min is requested ... */
            if (min != NULL) {
                index_scan = index_beginscan(heap_rel, index_rel, SnapshotNow, 1, 0);
                index_rescan(index_scan, scan_keys, 1, NULL, 0);

                /* Fetch first tuple in sort_op's direction */
                if ((tup = index_getnext(index_scan, index_scan_dir)) != NULL) {
                    /* Extract the index column values from the heap tuple */
                    (void)ExecStoreTuple(tup, slot, InvalidBuffer, false);
                    FormIndexDatum(index_info, slot, estate, values, is_null);

                    /* Shouldn't have got a null, but be careful */
                    if (is_null[0]) {
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                    errmsg("found unexpected null value in index \"%s\"",
                                        RelationGetRelationName(index_rel)))));
                    }

                    /* Copy the index column value out to caller's context */
                    (void)MemoryContextSwitchTo(old_context);
                    *min = datumCopy(values[0], typ_by_val, typ_len);
                    (void)MemoryContextSwitchTo(tmp_context);
                } else {
                    have_data = false;
                }

                index_endscan(index_scan);
            }

            /* If max is requested, and we didn't find the index is empty */
            if ((max != NULL) && have_data) {
                index_scan = index_beginscan(heap_rel, index_rel, SnapshotNow, 1, 0);
                index_rescan(index_scan, scan_keys, 1, NULL, 0);

                /* Fetch first tuple in reverse direction */
                if ((tup = index_getnext(index_scan, (ScanDirection)-index_scan_dir)) != NULL) {
                    /* Extract the index column values from the heap tuple */
                    (void)ExecStoreTuple(tup, slot, InvalidBuffer, false);
                    FormIndexDatum(index_info, slot, estate, values, is_null);

                    /* Shouldn't have got a null, but be careful */
                    if (is_null[0]) {
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                    errmsg("found unexpected null value in index \"%s\"",
                                        RelationGetRelationName(index_rel)))));
                    }

                    /* Copy the index column value out to caller's context */
                    (void)MemoryContextSwitchTo(old_context);
                    *max = datumCopy(values[0], typ_by_val, typ_len);
                    (void)MemoryContextSwitchTo(tmp_context);
                } else {
                    have_data = false;
                }

                index_endscan(index_scan);
            }

            /* Clean everything up */
            ExecDropSingleTupleTableSlot(slot);

            index_close(index_rel, AccessShareLock);
            heap_close(heap_rel, NoLock);

            (void)MemoryContextSwitchTo(old_context);
            FreeExecutorState(estate);

            /* And we're done */
            break;
        }
    }

    return have_data;
}

/*
 * find_join_input_rel
 *		Look up the input relation for a join.
 *
 * We assume that the input relation's RelOptInfo must have been constructed
 * already.
 */
RelOptInfo* find_join_input_rel(PlannerInfo* root, Relids relids)
{
    RelOptInfo* rel = NULL;

    switch (bms_membership(relids)) {
        case BMS_EMPTY_SET:
            /* should not happen */
            break;
        case BMS_SINGLETON:
            rel = find_base_rel(root, bms_singleton_member(relids));
            break;
        case BMS_MULTIPLE:
            rel = find_join_rel(root, relids);
            break;
        default:
            break;
    }

    if (rel == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("could not find RelOptInfo for given relids"))));
    }

    return rel;
}

/* -------------------------------------------------------------------------
 *
 * Pattern analysis functions
 *
 * These routines support analysis of LIKE and regular-expression patterns
 * by the planner/optimizer.  It's important that they agree with the
 * regular-expression code in backend/regex/ and the LIKE code in
 * backend/utils/adt/like.c.  Also, the computation of the fixed prefix
 * must be conservative: if we report a string longer than the true fixed
 * prefix, the query may produce actually wrong answers, rather than just
 * getting a bad selectivity estimate!
 *
 * Note that the prefix-analysis functions are called from
 * backend/optimizer/path/indxpath.c as well as from routines in this file.
 *
 * -------------------------------------------------------------------------
 */

/*
 * Check whether char is a letter (and, hence, subject to case-folding)
 *
 * In multibyte character sets, we can't use isalpha, and it does not seem
 * worth trying to convert to wchar_t to use iswalpha.	Instead, just assume
 * any multibyte char is potentially case-varying.
 */
static int pattern_char_isalpha(char c, bool is_multibyte, pg_locale_t locale, bool locale_is_c)
{
    if (locale_is_c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    } else if (is_multibyte && IS_HIGHBIT_SET(c)) {
        return true;
#ifdef HAVE_LOCALE_T
    } else if (locale) {
        return isalpha_l((unsigned char)c, locale);
#endif
    } else {
        return isalpha((unsigned char)c);
    }
}

/*
 * Extract the fixed prefix, if any, for a pattern.
 *
 * *prefix is set to a palloc'd prefix string (in the form of a Const node),
 *	or to NULL if no fixed prefix exists for the pattern.
 * If rest_selec is not NULL, *rest_selec is set to an estimate of the
 *	selectivity of the remainder of the pattern (without any fixed prefix).
 * The prefix Const has the same type (TEXT or BYTEA) as the input pattern.
 *
 * The return value distinguishes no fixed prefix, a partial prefix,
 * or an exact-match-only pattern.
 */
static Pattern_Prefix_Status like_fixed_prefix(
    Const* patt_const, bool case_insensitive, Oid collation, Const** prefix_const, Selectivity* rest_selec)
{
    char* match = NULL;
    char* patt = NULL;
    int pattlen;
    Oid typeId = patt_const->consttype;
    int pos, match_pos;
    bool is_multibyte = (pg_database_encoding_max_length() > 1);
    pg_locale_t locale = 0;
    bool locale_is_c = false;

    /* the right-hand const is type text or bytea */
    Assert(typeId == BYTEAOID || typeId == TEXTOID);

    if (case_insensitive) {
        if (typeId == BYTEAOID) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("case insensitive matching not supported on type bytea")));
        }

        /* If case-insensitive, we need locale info */
        if (lc_ctype_is_c(collation)) {
            locale_is_c = true;
        } else if (collation != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collation)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for ILIKE"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
            locale = pg_newlocale_from_collation(collation);
        }
    }

    if (typeId != BYTEAOID) {
        patt = TextDatumGetCString(patt_const->constvalue);
        pattlen = strlen(patt);
    } else {
        bytea* bstr = DatumGetByteaP(patt_const->constvalue);

        pattlen = VARSIZE(bstr) - VARHDRSZ;
        patt = (char*)palloc(pattlen);
        errno_t rc = memcpy_s(patt, pattlen, VARDATA(bstr), pattlen);
        securec_check(rc, "\0", "\0");
        if ((Pointer)bstr != DatumGetPointer(patt_const->constvalue)) {
            pfree_ext(bstr);
        }
    }

    match = (char*)palloc(pattlen + 1);
    match_pos = 0;
    pos = 0;
    while (pos < pattlen) {
        int charlen = pg_mblen(patt + pos);

        /* % and _ are wildcard characters in LIKE */
        if (patt[pos] == '%' || patt[pos] == '_') {
            break;
        }

        /* Backslash escapes the next character */
        if (patt[pos] == '\\') {
            pos++;
            if (pos >= pattlen) {
                break;
            }
        }

        /* Stop if case-varying character (it's sort of a wildcard) */
        if (case_insensitive && pattern_char_isalpha(patt[pos], is_multibyte, locale, locale_is_c)) {
            break;
        }

        for (int i = 0; i < charlen; i++) {
            match[match_pos++] = patt[pos++];
        }
    }

    match[match_pos] = '\0';

    if (typeId != BYTEAOID) {
        *prefix_const = string_to_const(match, typeId);
    } else {
        *prefix_const = string_to_bytea_const(match, match_pos);
    }

    if (rest_selec != NULL) {
        *rest_selec = like_selectivity(&patt[pos], pattlen - pos, case_insensitive);
    }

    pfree_ext(patt);
    pfree_ext(match);

    /* in LIKE, an empty pattern is an exact match! */
    if (pos == pattlen) {
        return Pattern_Prefix_Exact; /* reached end of pattern, so exact */
    }

    if (match_pos > 0) {
        return Pattern_Prefix_Partial;
    }

    return Pattern_Prefix_None;
}

static Pattern_Prefix_Status regex_fixed_prefix(
    Const* patt_const, bool case_insensitive, Oid collation, Const** prefix_const, Selectivity* rest_selec)
{
    Oid typeId = patt_const->consttype;
    char* prefix = NULL;
    bool exact = false;

    /*
     * Should be unnecessary, there are no bytea regex operators defined. As
     * such, it should be noted that the rest of this function has *not* been
     * made safe for binary (possibly NULL containing) strings.
     */
    if (typeId == BYTEAOID) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("regular-expression matching not supported on type bytea")));
    }

    /* Use the regexp machinery to extract the prefix, if any */
    prefix = regexp_fixed_prefix(DatumGetTextPP(patt_const->constvalue), case_insensitive, collation, &exact);
    if (prefix == NULL) {
        *prefix_const = NULL;

        if (rest_selec != NULL) {
            char* patt = TextDatumGetCString(patt_const->constvalue);

            *rest_selec = regex_selectivity(patt, strlen(patt), case_insensitive, 0);
            pfree_ext(patt);
        }

        return Pattern_Prefix_None;
    }

    *prefix_const = string_to_const(prefix, typeId);

    if (rest_selec != NULL) {
        if (exact) {
            /* Exact match, so there's no additional selectivity */
            *rest_selec = 1.0;
        } else {
            char* patt = TextDatumGetCString(patt_const->constvalue);

            *rest_selec = regex_selectivity(patt, strlen(patt), case_insensitive, strlen(prefix));
            pfree_ext(patt);
        }
    }

    pfree_ext(prefix);

    if (exact) {
        return Pattern_Prefix_Exact; /* pattern specifies exact match */
    } else {
        return Pattern_Prefix_Partial;
    }
}

Pattern_Prefix_Status pattern_fixed_prefix(
    Const* patt, Pattern_Type ptype, Oid collation, Const** prefix, Selectivity* rest_selec)
{
    Pattern_Prefix_Status result;

    switch (ptype) {
        case Pattern_Type_Like:
            result = like_fixed_prefix(patt, false, collation, prefix, rest_selec);
            break;
        case Pattern_Type_Like_IC:
            result = like_fixed_prefix(patt, true, collation, prefix, rest_selec);
            break;
        case Pattern_Type_Regex:
            result = regex_fixed_prefix(patt, false, collation, prefix, rest_selec);
            break;
        case Pattern_Type_Regex_IC:
            result = regex_fixed_prefix(patt, true, collation, prefix, rest_selec);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized ptype: %d", (int)ptype))));
            result = Pattern_Prefix_None; /* keep compiler quiet */
            break;
    }
    return result;
}

/*
 * Estimate the selectivity of a fixed prefix for a pattern match.
 *
 * A fixed prefix "foo" is estimated as the selectivity of the expression
 * "variable >= 'foo' AND variable < 'fop'" (see also indxpath.c).
 *
 * The selectivity estimate is with respect to the portion of the column
 * population represented by the histogram --- the caller must fold this
 * together with info about MCVs and NULLs.
 *
 * We use the >= and < operators from the specified btree opfamily to do the
 * estimation.	The given variable and Const must be of the associated
 * datatype.
 *
 * XXX Note: we make use of the upper bound to estimate operator selectivity
 * even if the locale is such that we cannot rely on the upper-bound string.
 * The selectivity only needs to be approximately right anyway, so it seems
 * more useful to use the upper-bound code than not.
 */
static Selectivity prefix_selectivity(
    PlannerInfo* root, VariableStatData* var_data, Oid var_type, Oid op_family, Const* prefix_con)
{
    Selectivity prefix_sel;
    Oid cmp_opr;
    FmgrInfo op_proc;
    Const* greater_str_con = NULL;
    Selectivity eq_sel;

    cmp_opr = get_opfamily_member(op_family, var_type, var_type, BTGreaterEqualStrategyNumber);
    if (cmp_opr == InvalidOid) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("no >= operator for op_family %u", op_family))));
    }

    fmgr_info(get_opcode(cmp_opr), &op_proc);

    prefix_sel = ineq_histogram_selectivity(root, var_data, &op_proc, true,
        prefix_con->constvalue, prefix_con->consttype);

    if (prefix_sel < 0.0) {
        /* No histogram is present ... return a suitable default estimate */
        return DEFAULT_MATCH_SEL;
    }

    /* -------
     * If we can create a string larger than the prefix, say
     *	"x < greaterstr".
     * -------
     */
    cmp_opr = get_opfamily_member(op_family, var_type, var_type, BTLessStrategyNumber);
    if (cmp_opr == InvalidOid) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("no < operator for op_family %u", op_family))));
    }

    fmgr_info(get_opcode(cmp_opr), &op_proc);
    greater_str_con = make_greater_string(prefix_con, &op_proc, DEFAULT_COLLATION_OID);
    if (greater_str_con != NULL) {
        Selectivity topsel;

        topsel = ineq_histogram_selectivity(
            root, var_data, &op_proc, false, greater_str_con->constvalue, greater_str_con->consttype);

        /* ineq_histogram_selectivity worked before, it shouldn't fail now */
        Assert(topsel >= 0.0);

        /*
         * Merge the two selectivities in the same way as for a range query
         * (see clauselist_selectivity()).	Note that we don't need to worry
         * about double-exclusion of nulls, since ineq_histogram_selectivity
         * doesn't count those anyway.
         */
        prefix_sel = topsel + prefix_sel - 1.0;
    }

    /*
     * If the prefix is long then the two bounding values might be too close
     * together for the histogram to distinguish them usefully, resulting in a
     * zero estimate (plus or minus roundoff error). To avoid returning a
     * ridiculously small estimate, compute the estimated selectivity for
     * "variable = 'foo'", and clamp to that. (Obviously, the resultant
     * estimate should be at least that.)
     *
     * We apply this even if we couldn't make a greater string.  That case
     * suggests that the prefix is near the maximum possible, and thus
     * probably off the end of the histogram, and thus we probably got a very
     * small estimate from the >= condition; so we still need to clamp.
     */
    cmp_opr = get_opfamily_member(op_family, var_type, var_type, BTEqualStrategyNumber);
    if (cmp_opr == InvalidOid) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("no = operator for op_family %u", op_family))));
    }

    eq_sel = var_eq_const(var_data, cmp_opr, prefix_con->constvalue, false, true);

    prefix_sel = Max(prefix_sel, eq_sel);

    return prefix_sel;
}

/*
 * Estimate the selectivity of a pattern of the specified type.
 * Note that any fixed prefix of the pattern will have been removed already,
 * so actually we may be looking at just a fragment of the pattern.
 *
 * For now, we use a very simplistic approach: fixed characters reduce the
 * selectivity a good deal, character ranges reduce it a little,
 * wildcards (such as % for LIKE or .* for regex) increase it.
 */
#define FIXED_CHAR_SEL 0.20 /* about 1/5 */
#define CHAR_RANGE_SEL 0.25
#define ANY_CHAR_SEL 0.9 /* not 1, since it won't match end-of-string */
#define FULL_WILDCARD_SEL 5.0
#define PARTIAL_WILDCARD_SEL 2.0

static Selectivity like_selectivity(const char* patt, int patt_len, bool case_insensitive)
{
    Selectivity sel = 1.0;
    int pos = 0;

    /* Skip any leading wildcard; it's already factored into initial sel */
    for (pos = 0; pos < patt_len; pos++) {
        if (patt[pos] != '%' && patt[pos] != '_') {
            break;
        }
    }

    while (pos < patt_len) {
        /* % and _ are wildcard characters in LIKE */
        if (patt[pos] == '%') {
            sel *= FULL_WILDCARD_SEL;
        } else if (patt[pos] == '_') {
            sel *= ANY_CHAR_SEL;
        } else if (patt[pos] == '\\') {
            /* Backslash quotes the next character */
            pos++;
            if (pos >= patt_len) {
                break;
            }
            sel *= FIXED_CHAR_SEL;
        } else {
            sel *= FIXED_CHAR_SEL;
        }
        pos += pg_mblen(patt + pos);
    }
    /* Could get sel > 1 if multiple wildcards */
    if (sel > 1.0) {
        sel = 1.0;
    }
    return sel;
}

static Selectivity regex_selectivity_sub(const char* patt, int patt_len, bool case_insensitive)
{
    Selectivity sel = 1.0;
    int paren_depth = 0;
    int paren_pos = 0; /* dummy init to keep compiler quiet */
    int pos = 0;

    while (pos < patt_len) {
        if (patt[pos] == '(') {
            if (paren_depth == 0) {
                paren_pos = pos; /* remember start of parenthesized item */
            }
            paren_depth++;
        } else if (patt[pos] == ')' && paren_depth > 0) {
            paren_depth--;
            if (paren_depth == 0) {
                sel *= regex_selectivity_sub(patt + (paren_pos + 1), pos - (paren_pos + 1), case_insensitive);
            }
        } else if (patt[pos] == '|' && paren_depth == 0) {
            /*
             * If unquoted | is present at paren level 0 in pattern, we have
             * multiple alternatives; sum their probabilities.
             */
            sel += regex_selectivity_sub(patt + (pos + 1), patt_len - (pos + 1), case_insensitive);
            break; /* rest of pattern is now processed */
        } else if (patt[pos] == '[') {
            bool negclass = false;

            if (patt[++pos] == '^') {
                negclass = true;
                pos++;
            }
            if (patt[pos] == ']') { /* ']' at start of class is not special */
                pos++;
            }
            while (pos < patt_len && patt[pos] != ']') {
                pos++;
            }
            if (paren_depth == 0) {
                sel *= (negclass ? (1.0 - CHAR_RANGE_SEL) : CHAR_RANGE_SEL);
            }
        } else if (patt[pos] == '.') {
            if (paren_depth == 0) {
                sel *= ANY_CHAR_SEL;
            }
        } else if (patt[pos] == '*' || patt[pos] == '?' || patt[pos] == '+') {
            /* Ought to be smarter about quantifiers... */
            if (paren_depth == 0) {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if (patt[pos] == '{') {
            while (pos < patt_len && patt[pos] != '}') {
                pos++;
            }
            if (paren_depth == 0) {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if (patt[pos] == '\\') {
            /* backslash quotes the next character */
            pos++;
            if (pos >= patt_len) {
                break;
            }
            if (paren_depth == 0) {
                sel *= FIXED_CHAR_SEL;
            }
        } else {
            if (paren_depth == 0) {
                sel *= FIXED_CHAR_SEL;
            }
        }
        pos += pg_mblen(patt + pos);
    }
    /* Could get sel > 1 if multiple wildcards */
    if (sel > 1.0) {
        sel = 1.0;
    }
    return sel;
}

static Selectivity regex_selectivity(const char* patt, int patt_len, bool case_insensitive, int fixed_prefix_len)
{
    Selectivity sel;

    /* If patt doesn't end with $, consider it to have a trailing wildcard */
    if (patt_len > 0 && patt[patt_len - 1] == '$' && (patt_len == 1 || patt[patt_len - 2] != '\\')) {
        /* has trailing $ */
        sel = regex_selectivity_sub(patt, patt_len - 1, case_insensitive);
    } else {
        /* no trailing $ */
        sel = regex_selectivity_sub(patt, patt_len, case_insensitive);
        sel *= FULL_WILDCARD_SEL;
    }

    /* If there's a fixed prefix, discount its selectivity */
    if (fixed_prefix_len > 0) {
        sel /= pow(FIXED_CHAR_SEL, fixed_prefix_len);
    }

    /* Make sure result stays in range */
    CLAMP_PROBABILITY(sel);
    return sel;
}

/*
 * For bytea, the increment function need only increment the current byte
 * (there are no multibyte characters to worry about).
 */
static bool byte_increment(unsigned char* ptr, int len)
{
    if (*ptr >= 255) {
        return false;
    }
    (*ptr)++;
    return true;
}

/*
 * Try to generate a string greater than the given string or any
 * string it is a prefix of.  If successful, return a palloc'd string
 * in the form of a Const node; else return NULL.
 *
 * The caller must provide the appropriate "less than" comparison function
 * for testing the strings, along with the collation to use.
 *
 * The key requirement here is that given a prefix string, say "foo",
 * we must be able to generate another string "fop" that is greater than
 * all strings "foobar" starting with "foo".  We can test that we have
 * generated a string greater than the prefix string, but in non-C collations
 * that is not a bulletproof guarantee that an extension of the string might
 * not sort after it; an example is that "foo " is less than "foo!", but it
 * is not clear that a "dictionary" sort ordering will consider "foo!" less
 * than "foo bar".	CAUTION: Therefore, this function should be used only for
 * estimation purposes when working in a non-C collation.
 *
 * To try to catch most cases where an extended string might otherwise sort
 * before the result value, we determine which of the strings "Z", "z", "y",
 * and "9" is seen as largest by the collation, and append that to the given
 * prefix before trying to find a string that compares as larger.
 *
 * To search for a greater string, we repeatedly "increment" the rightmost
 * character, using an encoding-specific character incrementer function.
 * When it's no longer possible to increment the last character, we truncate
 * off that character and start incrementing the next-to-rightmost.
 * For example, if "z" were the last character in the sort order, then we
 * could produce "foo" as a string greater than "fonz".
 *
 * This could be rather slow in the worst case, but in most cases we
 * won't have to try more than one or two strings before succeeding.
 *
 * Note that it's important for the character incrementer not to be too anal
 * about producing every possible character code, since in some cases the only
 * way to get a larger string is to increment a previous character position.
 * So we don't want to spend too much time trying every possible character
 * code at the last position.  A good rule of thumb is to be sure that we
 * don't try more than 256*K values for a K-byte character (and definitely
 * not 256^K, which is what an exhaustive search would approach).
 */
Const* make_greater_string(const Const* str_const, FmgrInfo* lt_proc, Oid collation)
{
    Oid data_type = str_const->consttype;
    char* work_str = NULL;
    int len;
    Datum cmp_str;
    text* cmp_txt = NULL;
    mbcharacter_incrementer char_inc;

    /*
     * Get a modifiable copy of the prefix string in C-string format, and set
     * up the string we will compare to as a Datum.  In C locale this can just
     * be the given prefix string, otherwise we need to add a suffix.  Types
     * NAME and BYTEA sort bytewise so they don't need a suffix either.
     */
    if (data_type == NAMEOID) {
        work_str = DatumGetCString(DirectFunctionCall1(nameout, str_const->constvalue));
        len = strlen(work_str);
        cmp_str = str_const->constvalue;
    } else if (data_type == BYTEAOID) {
        bytea* bstr = DatumGetByteaP(str_const->constvalue);

        len = VARSIZE(bstr) - VARHDRSZ;
        work_str = (char*)palloc(len);
        errno_t rc = memcpy_s(work_str, len, VARDATA(bstr), len);
        securec_check(rc, "\0", "\0");
        if ((Pointer)bstr != DatumGetPointer(str_const->constvalue)) {
            pfree_ext(bstr);
        }
        cmp_str = str_const->constvalue;
    } else {
        work_str = TextDatumGetCString(str_const->constvalue);
        len = strlen(work_str);
        if (lc_collate_is_c(collation) || len == 0) {
            cmp_str = str_const->constvalue;
        } else {
            /* If first time through, determine the suffix to use */
            if (!u_sess->utils_cxt.suffix_char || u_sess->utils_cxt.suffix_collation != collation) {
                char* best = NULL;

                best = "Z";
                if (varstr_cmp(best, 1, "z", 1, collation) < 0) {
                    best = "z";
                }
                if (varstr_cmp(best, 1, "y", 1, collation) < 0) {
                    best = "y";
                }
                if (varstr_cmp(best, 1, "9", 1, collation) < 0) {
                    best = "9";
                }
                u_sess->utils_cxt.suffix_char = *best;
                u_sess->utils_cxt.suffix_collation = collation;
            }

            /* And build the string to compare to */
            cmp_txt = (text*)palloc(VARHDRSZ + len + 1);
            SET_VARSIZE(cmp_txt, VARHDRSZ + len + 1);
            errno_t rc = memcpy_s(VARDATA(cmp_txt), len, work_str, len);
            securec_check(rc, "\0", "\0");
            *(VARDATA(cmp_txt) + len) = u_sess->utils_cxt.suffix_char;
            cmp_str = PointerGetDatum(cmp_txt);
        }
    }

    /* Select appropriate character-incrementer function */
    if (data_type == BYTEAOID) {
        char_inc = byte_increment;
    } else {
        char_inc = pg_database_encoding_character_incrementer();
    }

    /* And search ... */
    while (len > 0) {
        int char_len;
        unsigned char* last_char = NULL;

        /* Identify the last character --- for bytea, just the last byte */
        if (data_type == BYTEAOID) {
            char_len = 1;
        } else {
            char_len = len - pg_mbcliplen(work_str, len, len - 1);
        }
        last_char = (unsigned char*)(work_str + len - char_len);

        /*
         * Try to generate a larger string by incrementing the last character
         * (for BYTEA, we treat each byte as a character).
         *
         * Note: the incrementer function is expected to return true if it's
         * generated a valid-per-the-encoding new character, otherwise false.
         * The contents of the character on false return are unspecified.
         */
        while (char_inc(last_char, char_len)) {
            Const* work_str_const = NULL;

            if (data_type == BYTEAOID) {
                work_str_const = string_to_bytea_const(work_str, len);
            } else {
                work_str_const = string_to_const(work_str, data_type);
            }

            if (DatumGetBool(FunctionCall2Coll(lt_proc, collation, cmp_str, work_str_const->constvalue))) {
                /* Successfully made a string larger than cmp_str */
                if (cmp_txt != NULL) {
                    pfree_ext(cmp_txt);
                }
                pfree_ext(work_str);
                return work_str_const;
            }

            /* No good, release unusable value and try again */
            pfree(DatumGetPointer(work_str_const->constvalue));
            pfree_ext(work_str_const);
        }

        /*
         * No luck here, so truncate off the last character and try to
         * increment the next one.
         */
        len -= char_len;
        work_str[len] = '\0';
    }

    /* Failed... */
    if (cmp_txt != NULL) {
        pfree_ext(cmp_txt);
    }
    pfree_ext(work_str);

    return NULL;
}

/*
 * Generate a Datum of the appropriate type from a C string.
 * Note that all of the supported types are pass-by-ref, so the
 * returned value should be pfree'd if no longer needed.
 */
static Datum string_to_datum(const char* str, Oid data_type)
{
    Assert(str != NULL);

    /*
     * We cheat a little by assuming that CStringGetTextDatum() will do for
     * bpchar and varchar constants too...
     */
    if (data_type == NAMEOID) {
        return DirectFunctionCall1(namein, CStringGetDatum(str));
    } else if (data_type == BYTEAOID) {
        return DirectFunctionCall1(byteain, CStringGetDatum(str));
    } else {
        return CStringGetTextDatum(str);
    }
}

/*
 * Generate a Const node of the appropriate type from a C string.
 */
static Const* string_to_const(const char* str, Oid data_type)
{
    Datum conval = string_to_datum(str, data_type);
    Oid collation;
    int constlen;

    /*
     * We only need to support a few datatypes here, so hard-wire properties
     * instead of incurring the expense of catalog lookups.
     */
    switch (data_type) {
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            collation = DEFAULT_COLLATION_OID;
            constlen = -1;
            break;

        case NAMEOID:
            collation = InvalidOid;
            constlen = NAMEDATALEN;
            break;

        case BYTEAOID:
            collation = InvalidOid;
            constlen = -1;
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("unexpected data_type in string_to_const: %u", data_type))));

            return NULL;
    }

    return makeConst(data_type, -1, collation, constlen, conval, false, false);
}

/*
 * Generate a Const node of bytea type from a binary C string and a length.
 */
static Const* string_to_bytea_const(const char* str, size_t str_len)
{
    bytea* bstr = (bytea*)palloc(VARHDRSZ + str_len);
    Datum con_val;

    errno_t rc = memcpy_s(VARDATA(bstr), VARHDRSZ + str_len, str, str_len);
    securec_check(rc, "", "");

    SET_VARSIZE(bstr, VARHDRSZ + str_len);
    con_val = PointerGetDatum(bstr);

    return makeConst(BYTEAOID, -1, InvalidOid, -1, con_val, false, false);
}

/* -------------------------------------------------------------------------
 *
 * Index cost estimation functions
 *
 * -------------------------------------------------------------------------
 */

/*
 * If the index is partial, add its predicate to the given qual list.
 *
 * ANDing the index predicate with the explicitly given indexquals produces
 * a more accurate idea of the index's selectivity.  However, we need to be
 * careful not to insert redundant clauses, because clauselist_selectivity()
 * is easily fooled into computing a too-low selectivity estimate.	Our
 * approach is to add only the predicate clause(s) that cannot be proven to
 * be implied by the given indexquals.	This successfully handles cases such
 * as a qual "x = 42" used with a partial index "WHERE x >= 40 AND x < 50".
 * There are many other cases where we won't detect redundancy, leading to a
 * too-low selectivity estimate, which will bias the system in favor of using
 * partial indexes where possible.	That is not necessarily bad though.
 *
 * Note that indexQuals contains RestrictInfo nodes while the indpred
 * does not, so the output list will be mixed.	This is OK for both
 * predicate_implied_by() and clauselist_selectivity(), but might be
 * problematic if the result were passed to other things.
 */
static List* add_predicate_to_quals(IndexOptInfo* index, List* index_quals)
{
    List* pred_extra_quals = NIL;
    ListCell* lc = NULL;

    if (index->indpred == NIL) {
        return index_quals;
    }

    foreach (lc, index->indpred) {
        Node* pred_qual = (Node*)lfirst(lc);
        List* one_qual = list_make1(pred_qual);

        if (!predicate_implied_by(one_qual, index_quals)) {
            pred_extra_quals = list_concat(pred_extra_quals, one_qual);
        }
    }
    /* list_concat avoids modifying the passed-in index_quals list */
    return list_concat(pred_extra_quals, index_quals);
}

/*
 * generic_cost_estimate is a general-purpose estimator for use when we
 * don't have any better idea about how to estimate.  Index-type-specific
 * knowledge can be incorporated in the type-specific routines.
 *
 * One bit of index-type-specific knowledge we can relatively easily use
 * in generic_cost_estimate is the estimate of the number of index tuples
 * visited.  If numIndexTuples is not 0 then it is used as the estimate,
 * otherwise we compute a generic estimate.
 */
static void generic_cost_estimate(PlannerInfo* root, IndexPath* path, double loop_count, double num_index_tuples,
    Cost* index_startup_cost, Cost* index_total_cost, Selectivity* index_selectivity, double* index_correlation)
{
    IndexOptInfo* index = path->indexinfo;
    List* index_quals = path->indexquals;
    List* index_orderbys = path->indexorderbys;
    double num_index_pages;
    double num_sa_scans;
    double num_outer_scans;
    double num_scans;
    QualCost index_qual_cost;
    double qual_op_cost;
    double qual_arg_cost;
    double spc_random_page_cost;
    List* selectivity_quals = NIL;
    ListCell* l = NULL;
    bool is_partitioned_index = path->indexinfo->rel->isPartitionedTable;
    double idx_local_tupls = IDXOPTINFO_LOCAL_FIELD(root, index, tuples);
    List* saved_varratios = NIL;

    /*
     * If the index is partial, AND the index predicate with the explicitly
     * given indexquals to produce a more accurate idea of the index
     * selectivity.
     */
    selectivity_quals = add_predicate_to_quals(index, index_quals);

    /*
     * Check for ScalarArrayOpExpr index quals, and estimate the number of
     * index scans that will be performed.
     */
    num_sa_scans = 1;
    foreach (l, index_quals) {
        RestrictInfo* r_info = (RestrictInfo*)lfirst(l);

        if (IsA(r_info->clause, ScalarArrayOpExpr)) {
            ScalarArrayOpExpr* sa_op = (ScalarArrayOpExpr*)r_info->clause;
            int a_length = estimate_array_length((Node*)lsecond(sa_op->args));

            if (a_length > 1) {
                num_sa_scans *= a_length;
            }
        }
    }

    saved_varratios = index->rel->varratio;
    index->rel->varratio = NULL;
    /* Estimate the fraction of main-table tuples that will be visited */
    *index_selectivity = clauselist_selectivity(root, selectivity_quals, index->rel->relid, JOIN_INNER, NULL, false);
    list_free_deep(index->rel->varratio);
    index->rel->varratio = saved_varratios;

    /*
     * If caller didn't give us an estimate, estimate the number of index
     * tuples that will be visited.  We do it in this rather peculiar-looking
     * way in order to get the right answer for partial indexes.
     */
    if (num_index_tuples <= 0.0) {
        num_index_tuples = *index_selectivity * RELOPTINFO_LOCAL_FIELD(root, index->rel, tuples);

        /*
         * The above calculation counts all the tuples visited across all
         * scans induced by ScalarArrayOpExpr nodes.  We want to consider the
         * average per-indexscan number, so adjust.  This is a handy place to
         * round to integer, too.  (If caller supplied tuple estimate, it's
         * responsible for handling these considerations.)
         */
        num_index_tuples = rint(num_index_tuples / num_sa_scans);
    }

    /*
     * We can bound the number of tuples by the index size in any case. Also,
     * always estimate at least one tuple is touched, even when
     * index_selectivity estimate is tiny.
     */
    if (num_index_tuples > idx_local_tupls) {
        num_index_tuples = idx_local_tupls;
    }
    if (num_index_tuples < 1.0) {
        num_index_tuples = 1.0;
    }

    /*
     * Estimate the number of index pages that will be retrieved.
     *
     * We use the simplistic method of taking a pro-rata fraction of the total
     * number of index pages.  In effect, this counts only leaf pages and not
     * any overhead such as index metapage or upper tree levels. In practice
     * this seems a better approximation than charging for access to the upper
     * levels, perhaps because those tend to stay in cache under load.
     */
    if (index->pages > 1 && idx_local_tupls > 1) {
        num_index_pages = ceil(num_index_tuples * index->pages / idx_local_tupls);
    } else {
        num_index_pages = 1.0;
    }

    /* fetch estimated page cost for schema containing index */
    get_tablespace_page_costs(index->reltablespace, &spc_random_page_cost, NULL);

    /*
     * Now compute the disk access costs.
     *
     * The above calculations are all per-index-scan.  However, if we are in a
     * nestloop inner scan, we can expect the scan to be repeated (with
     * different search keys) for each row of the outer relation.  Likewise,
     * ScalarArrayOpExpr quals result in multiple index scans.	This creates
     * the potential for cache effects to reduce the number of disk page
     * fetches needed.	We want to estimate the average per-scan I/O cost in
     * the presence of caching.
     *
     * We use the Mackert-Lohman formula (see costsize.c for details) to
     * estimate the total number of page fetches that occur.  While this
     * wasn't what it was designed for, it seems a reasonable model anyway.
     * Note that we are counting pages not tuples anymore, so we take N = T =
     * index size, as if there were one "tuple" per page.
     */
    num_outer_scans = loop_count;
    num_scans = num_sa_scans * num_outer_scans;

    if (num_scans > 1) {
        double pages_fetched;

        /* total page fetches ignoring cache effects */
        pages_fetched = num_index_pages * num_scans;

        /* use Mackert and Lohman formula to adjust for cache effects */
        pages_fetched =
            index_pages_fetched(pages_fetched, index->pages, (double)index->pages, root, is_partitioned_index);

        /*
         * Now compute the total disk access cost, and then report a pro-rated
         * share for each outer scan.  (Don't pro-rate for ScalarArrayOpExpr,
         * since that's internal to the indexscan.)
         */
        *index_total_cost = (pages_fetched * spc_random_page_cost) / num_outer_scans;
    } else {
        /*
         * For a single index scan, we just charge spc_random_page_cost per
         * page touched.
         */
        *index_total_cost = num_index_pages * spc_random_page_cost;
    }

    /*
     * A difficulty with the leaf-pages-only cost approach is that for small
     * selectivities (eg, single index tuple fetched) all indexes will look
     * equally attractive because we will estimate exactly 1 leaf page to be
     * fetched.  All else being equal, we should prefer physically smaller
     * indexes over larger ones.  (An index might be smaller because it is
     * partial or because it contains fewer columns; presumably the other
     * columns in the larger index aren't useful to the query, or the larger
     * index would have better selectivity.)
     *
     * We can deal with this by adding a very small "fudge factor" that
     * depends on the index size.  The fudge factor used here is one
     * spc_random_page_cost per 100000 index pages, which should be small
     * enough to not alter index-vs-seqscan decisions, but will prevent
     * indexes of different sizes from looking exactly equally attractive.
     */
    *index_total_cost += index->pages * spc_random_page_cost / 100000.0;

    /*
     * CPU cost: any complex expressions in the indexquals will need to be
     * evaluated once at the start of the scan to reduce them to runtime keys
     * to pass to the index AM (see nodeIndexscan.c).  We model the per-tuple
     * CPU costs as cpu_index_tuple_cost plus one cpu_operator_cost per
     * indexqual operator.	Because we have num_index_tuples as a per-scan
     * number, we have to multiply by num_sa_scans to get the correct result
     * for ScalarArrayOpExpr cases.  Similarly add in costs for any index
     * ORDER BY expressions.
     *
     * Note: this neglects the possible costs of rechecking lossy operators
     * and OR-clause expressions.  Detecting that that might be needed seems
     * more expensive than it's worth, though, considering all the other
     * inaccuracies here ...
     */
    cost_qual_eval(&index_qual_cost, index_quals, root);
    qual_arg_cost = index_qual_cost.startup + index_qual_cost.per_tuple;
    cost_qual_eval(&index_qual_cost, index_orderbys, root);
    qual_arg_cost += index_qual_cost.startup + index_qual_cost.per_tuple;
    qual_op_cost = u_sess->attr.attr_sql.cpu_operator_cost * (list_length(index_quals) + list_length(index_orderbys));
    qual_arg_cost -= qual_op_cost;
    if (qual_arg_cost < 0) { /* just in case... */
        qual_arg_cost = 0;
    }

    *index_startup_cost = qual_arg_cost;
    *index_total_cost += qual_arg_cost;
    *index_total_cost += num_index_tuples * num_sa_scans * (u_sess->attr.attr_sql.cpu_index_tuple_cost + qual_op_cost);

    /*
     * We also add a CPU-cost component to represent the general costs of
     * starting an indexscan, such as analysis of btree index keys and initial
     * tree descent.  This is estimated at 100x cpu_operator_cost, which is a
     * bit arbitrary but seems the right order of magnitude. (As noted above,
     * we don't charge any I/O for touching upper tree levels, but charging
     * nothing at all has been found too optimistic.)
     *
     * Although this is startup cost with respect to any one scan, we add it
     * to the "total" cost component because it's only very interesting in the
     * many-ScalarArrayOpExpr-scan case, and there it will be paid over the
     * life of the scan node.
     */
    *index_total_cost += num_sa_scans * 100.0 * u_sess->attr.attr_sql.cpu_operator_cost;
    /*
     * Generic assumption about index correlation: there isn't any.
     */
    *index_correlation = 0.0;
}

Datum btcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* index_startup_cost = (Cost*)PG_GETARG_POINTER(3);
    Cost* index_total_cost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* index_selectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* index_correlation = (double*)PG_GETARG_POINTER(6);
    IndexOptInfo* index = path->indexinfo;
    Oid relid;
    AttrNumber col_num;
    VariableStatData var_data;
    double num_index_tuples;
    List* index_bound_quals = NIL;
    int index_col;
    bool eq_qual_here = false;
    bool found_saop = false;
    bool found_is_null_op = false;
    double num_sa_scans;
    ListCell* lcc = NULL;
    ListCell* lci = NULL;

    /*
     * For a btree scan, only leading '=' quals plus inequality quals for the
     * immediately next attribute contribute to index selectivity (these are
     * the "boundary quals" that determine the starting and stopping points of
     * the index scan).  Additional quals can suppress visits to the heap, so
     * it's OK to count them in index_selectivity, but they should not count
     * for estimating num_index_tuples.  So we must examine the given indexquals
     * to find out which ones count as boundary quals.	We rely on the
     * knowledge that they are given in index column order.
     *
     * For a RowCompareExpr, we consider only the first column, just as
     * rowcomparesel() does.
     *
     * If there's a ScalarArrayOpExpr in the quals, we'll actually perform N
     * index scans not one, but the ScalarArrayOpExpr's operator can be
     * considered to act the same as it normally does.
     */
    index_bound_quals = NIL;
    index_col = 0;
    eq_qual_here = false;
    found_saop = false;
    found_is_null_op = false;
    num_sa_scans = 1;
    forboth(lcc, path->indexquals, lci, path->indexqualcols)
    {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lcc);
        Expr* clause = NULL;
        Node* left_op = NULL;
        Node PG_USED_FOR_ASSERTS_ONLY* right_op = NULL;
        Oid clause_op;
        int op_strategy;
        bool is_null_op = false;

        if (index_col != lfirst_int(lci)) {
            /* Beginning of a new column's quals */
            if (!eq_qual_here) {
                break; /* done if no '=' qual for index_col */
            }
            eq_qual_here = false;
            index_col++;
            if (index_col != lfirst_int(lci)) {
                break; /* no quals at all for index_col */
            }
        }

        Assert(IsA(rinfo, RestrictInfo));
        clause = rinfo->clause;

        if (IsA(clause, OpExpr)) {
            left_op = get_leftop(clause);
            right_op = get_rightop(clause);
            clause_op = ((OpExpr*)clause)->opno;
        } else if (IsA(clause, RowCompareExpr)) {
            RowCompareExpr* rc = (RowCompareExpr*)clause;

            left_op = (Node*)linitial(rc->largs);
            right_op = (Node*)linitial(rc->rargs);
            clause_op = linitial_oid(rc->opnos);
        } else if (IsA(clause, ScalarArrayOpExpr)) {
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)clause;

            left_op = (Node*)linitial(saop->args);
            right_op = (Node*)lsecond(saop->args);
            clause_op = saop->opno;
            found_saop = true;
        } else if (IsA(clause, NullTest)) {
            NullTest* nt = (NullTest*)clause;

            left_op = (Node*)nt->arg;
            right_op = NULL;
            clause_op = InvalidOid;
            if (nt->nulltesttype == IS_NULL) {
                found_is_null_op = true;
                is_null_op = true;
            }
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unsupported indexqual type: %d", (int)nodeTag(clause)))));

            continue; /* keep compiler quiet */
        }

        if (match_index_to_operand(left_op, index_col, index)) {
            /* clause_op is correct */
        } else {
            Assert(match_index_to_operand(right_op, index_col, index));
            /* Must flip operator to get the opfamily member */
            clause_op = get_commutator(clause_op);
        }

        /* check for equality operator */
        if (OidIsValid(clause_op)) {
            op_strategy = get_op_opfamily_strategy(clause_op, index->opfamily[index_col]);
            Assert(op_strategy != 0); /* not a member of opfamily?? */
            if (op_strategy == BTEqualStrategyNumber) {
                eq_qual_here = true;
            }
        } else if (is_null_op) {
            /* IS NULL is like = for purposes of selectivity determination */
            eq_qual_here = true;
        }
        /* count up number of SA scans induced by index_bound_quals only */
        if (IsA(clause, ScalarArrayOpExpr)) {
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)clause;
            int alength = estimate_array_length((Node*)lsecond(saop->args));

            if (alength > 1) {
                num_sa_scans *= alength;
            }
        }
        index_bound_quals = lappend(index_bound_quals, rinfo);
    }

    /*
     * If index is unique and we found an '=' clause for each column, we can
     * just assume num_index_tuples = 1 and skip the expensive
     * clauselist_selectivity calculations.  However, a ScalarArrayOp or
     * NullTest invalidates that theory, even though it sets eq_qual_here.
     */
    if (index->unique && index_col == index->nkeycolumns - 1 && eq_qual_here && !found_saop && !found_is_null_op)
        num_index_tuples = 1.0;
    else {
        List* selectivity_quals = NIL;
        Selectivity btree_selectivity;
        List* saved_varratios = NIL;

        /*
         * If the index is partial, AND the index predicate with the
         * index-bound quals to produce a more accurate idea of the number of
         * rows covered by the bound conditions.
         */
        selectivity_quals = add_predicate_to_quals(index, index_bound_quals);

        saved_varratios = index->rel->varratio;
        index->rel->varratio = NULL;
        btree_selectivity = clauselist_selectivity(root, selectivity_quals, index->rel->relid, JOIN_INNER, NULL, false);
        list_free_deep(index->rel->varratio);
        index->rel->varratio = saved_varratios;
        num_index_tuples = btree_selectivity * RELOPTINFO_LOCAL_FIELD(root, index->rel, tuples);

        /*
         * As in generic_cost_estimate(), we have to adjust for any
         * ScalarArrayOpExpr quals included in index_bound_quals, and then round
         * to integer.
         */
        num_index_tuples = rint(num_index_tuples / num_sa_scans);
    }

    generic_cost_estimate(root, path, loop_count, num_index_tuples, index_startup_cost,
        index_total_cost, index_selectivity, index_correlation);

    /*
     * If we can get an estimate of the first column's ordering correlation C
     * from pg_statistic, estimate the index correlation as C for a
     * single-column index, or C * 0.75 for multiple columns. (The idea here
     * is that multiple columns dilute the importance of the first column's
     * ordering, but don't negate it entirely.  Before 8.0 we divided the
     * correlation by the number of columns, but that seems too strong.)
     */
    errno_t rc = memset_s(&var_data, sizeof(var_data), 0, sizeof(var_data));
    securec_check(rc, "\0", "\0");

    if (index->indexkeys[0] != 0) {
        /* Simple variable --- look to stats for the underlying table */
        RangeTblEntry* rte = planner_rt_fetch(index->rel->relid, root);
        char relPersistence = get_rel_persistence(rte->relid);
        Assert(rte->rtekind == RTE_RELATION);
        relid = rte->relid;
        Assert(relid != InvalidOid);
        col_num = index->indexkeys[0];

        char stakind = STARELKIND_CLASS;
        Oid staoid = relid;

        if (OidIsValid(rte->partitionOid)) {
            Assert(rte->isContainPartition && rte->ispartrel);
            stakind = STARELKIND_PARTITION;
            staoid = rte->partitionOid;
        }

        if (u_sess->attr.attr_common.upgrade_mode != 0) {
            var_data.statsTuple = NULL;
            var_data.freefunc = ReleaseSysCache;
        } else if (relPersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            var_data.statsTuple = get_gtt_att_statistic(rte->relid, col_num);
            var_data.freefunc = release_gtt_statistic_cache;
        } else {
            var_data.statsTuple = SearchSysCache4(STATRELKINDATTINH,
                ObjectIdGetDatum(staoid),
                CharGetDatum(stakind),
                Int16GetDatum(col_num),
                BoolGetDatum(rte->inh));
            var_data.freefunc = ReleaseSysCache;
        }

    } else {
        /* Expression --- maybe there are stats for the index itself */
        char relPersistence = get_rel_persistence(index->indexoid);
        relid = index->indexoid;
        col_num = 1;

        char stakind = STARELKIND_CLASS;
        Oid staoid = relid;

        if (OidIsValid(index->partitionindex)) {
            Assert(index->ispartitionedindex);
            stakind = STARELKIND_PARTITION;
            staoid = index->partitionindex;
        }

        if (u_sess->attr.attr_common.upgrade_mode != 0) {
            var_data.statsTuple = NULL;
            var_data.freefunc = ReleaseSysCache;
        } else if (relPersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            var_data.statsTuple = get_gtt_att_statistic(relid, col_num);
            var_data.freefunc = release_gtt_statistic_cache;
        } else {
            var_data.statsTuple = SearchSysCache4(STATRELKINDATTINH,
                ObjectIdGetDatum(staoid),
                CharGetDatum(stakind),
                Int16GetDatum(col_num),
                BoolGetDatum(false));
            var_data.freefunc = ReleaseSysCache;
        }

    }

    if (HeapTupleIsValid(var_data.statsTuple)) {
        Oid sort_op;
        float4* numbers = NULL;
        int nnumbers;

        sort_op =
            get_opfamily_member(index->opfamily[0], index->opcintype[0], index->opcintype[0], BTLessStrategyNumber);
        if (OidIsValid(sort_op) &&
            get_attstatsslot(var_data.statsTuple,
                InvalidOid,
                0,
                STATISTIC_KIND_CORRELATION,
                sort_op,
                NULL,
                NULL,
                NULL,
                &numbers,
                &nnumbers)) {
            double var_correlation;

            Assert(nnumbers == 1);
            var_correlation = numbers[0];

            if (index->reverse_sort[0]) {
                var_correlation = -var_correlation;
            }

            if (index->nkeycolumns > 1) {
                *index_correlation = var_correlation * 0.75;
            } else {
                *index_correlation = var_correlation;
            }

            free_attstatsslot(InvalidOid, NULL, 0, numbers, nnumbers);
        }
    }

    ReleaseVariableStats(var_data);

    PG_RETURN_VOID();
}

Datum hashcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* index_startup_cost = (Cost*)PG_GETARG_POINTER(3);
    Cost* index_total_cost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* index_selectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* index_correlation = (double*)PG_GETARG_POINTER(6);

    generic_cost_estimate(
        root, path, loop_count, 0.0, index_startup_cost, index_total_cost, index_selectivity, index_correlation);

    PG_RETURN_VOID();
}

Datum gistcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* index_startup_cost = (Cost*)PG_GETARG_POINTER(3);
    Cost* index_total_cost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* index_selectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* index_correlation = (double*)PG_GETARG_POINTER(6);

    generic_cost_estimate(
        root, path, loop_count, 0.0, index_startup_cost, index_total_cost, index_selectivity, index_correlation);

    PG_RETURN_VOID();
}

Datum spgcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* indexStartupCost = (Cost*)PG_GETARG_POINTER(3);
    Cost* indexTotalCost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* indexSelectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* indexCorrelation = (double*)PG_GETARG_POINTER(6);

    generic_cost_estimate(
        root, path, loop_count, 0.0, indexStartupCost, indexTotalCost, indexSelectivity, indexCorrelation);

    PG_RETURN_VOID();
}

#define DFS_INDEX_SELECTIVITY_THRESHOLD 0.001
Datum psortcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* indexStartupCost = (Cost*)PG_GETARG_POINTER(3);
    Cost* indexTotalCost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* indexSelectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* indexCorrelation = (double*)PG_GETARG_POINTER(6);

    IndexOptInfo* index = path->indexinfo;
    RelOptInfo* baserel = index->rel;
    /* selectedTuple - selected tuple in scan, decided by all entry num and selectiviry */
    double selectedTuple = 0.0;
    /* cuNum is the num of cu loaded for scan */
    double cuNum = 0.0;
    /* estimated cost for each tuple */
    Cost cpu_per_tuple = 0.0;
    /* indicate the multiplier cost and the default rows in a cu*/
    const int col_tuple_multiplier_cost = 10;
    int per_cu_itemnum = DefaultFullCUSize;

    generic_cost_estimate(
        root, path, loop_count, 0.0, indexStartupCost, indexTotalCost, indexSelectivity, indexCorrelation);

    /*
     * psort get_tid cost is 10 times as btree, we estimate the psort tid cost based
     * on the former indexTotalCost by generic_cost_estimate
     */
    (*indexTotalCost) *= 100;

    /*
     * calculate the selected tuple num according to the the num of all tuples and
     * the selectivity
     */
    *indexSelectivity = (*indexSelectivity <= 1) ? *indexSelectivity : 1;
    selectedTuple = (*indexSelectivity) * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples);
    selectedTuple = ceil(selectedTuple);

    /*
     * calculate how many cu need to be scanned for cost estimaing,
     * the ratio 0.02 adopted for cu_num estimate, which can be adjusted later
     */
    cuNum = selectedTuple * 0.02;
    /*
     * when selectivity and selectedTuple are very small, set the smallest cu = 1,
     * ensure smallest cost of index scan
     */
    cuNum = ceil(cuNum);

    /*
     * estimate the tuple scan cost in selected cu(cpu + memory), ignore dop
     * the index only scan cost are treated the same as indexscan now
     */
    cpu_per_tuple =
        u_sess->attr.attr_sql.cpu_tuple_cost / col_tuple_multiplier_cost + baserel->baserestrictcost.per_tuple;
    *indexTotalCost += cpu_per_tuple * per_cu_itemnum * cuNum;

    /* For dfs table, index scan is used when the selectivity is smaller than 0.001. */
    elog(DEBUG1, "indexSelectivity: %f.", *indexSelectivity);
    if (path->indexinfo->rel->orientation == REL_PAX_ORIENTED && *indexSelectivity > DFS_INDEX_SELECTIVITY_THRESHOLD) {
        *indexTotalCost += g_instance.cost_cxt.disable_cost * 0.5;
    }

    *indexTotalCost -= 100.0 * u_sess->attr.attr_sql.cpu_operator_cost;

    *indexCorrelation = 1.0;

    PG_RETURN_VOID();
}

Datum cbtreecostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* indexStartupCost = (Cost*)PG_GETARG_POINTER(3);
    Cost* indexTotalCost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* indexSelectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* indexCorrelation = (double*)PG_GETARG_POINTER(6);

    IndexOptInfo* index = path->indexinfo;
    RelOptInfo* baserel = index->rel;
    /* selectedTuple - selected tuple in scan, decided by all entry num and selectiviry */
    double selectedTuple = 0.0;
    /* cuNum is the num of cu loaded for scan */
    double cuNum = 0.0;
    /* estimated cost for each tuple */
    Cost cpu_per_tuple = 0.0;
    /* indicate the multiplier cost and the default rows in a cu*/
    const int col_tuple_multiplier_cost = 10;
    int per_cu_itemnum = DefaultFullCUSize;

    generic_cost_estimate(
        root, path, loop_count, 0.0, indexStartupCost, indexTotalCost, indexSelectivity, indexCorrelation);

    /* estimate the btree tid cost based on the former indexTotalCost */
    (*indexTotalCost) *= 10;

    /*
     * calculate the selected tuple num according to the the num of all tuples and
     * the selectivity
     */
    *indexSelectivity = (*indexSelectivity <= 1) ? *indexSelectivity : 1;
    selectedTuple = (*indexSelectivity) * RELOPTINFO_LOCAL_FIELD(root, baserel, tuples);
    selectedTuple = ceil(selectedTuple);

    /*
     * calculate how many cu need to be scanned for cost estimaing,
     * the ratio 0.002 adopted for cu_num estimate, which can be adjusted later
     */
    cuNum = selectedTuple * 0.002;
    /*
     * when selectivity and selectedTuple are very small, set the smallest cu = 1,
     * ensure smallest cost of index scan
     */
    cuNum = ceil(cuNum);

    /*
     * estimate the tuple scan cost in selected cu(cpu + memory), ignore dop
     * the index only scan cost are treated the same as indexscan now
     */
    cpu_per_tuple =
        u_sess->attr.attr_sql.cpu_tuple_cost / col_tuple_multiplier_cost + baserel->baserestrictcost.per_tuple;
    *indexTotalCost += cpu_per_tuple * per_cu_itemnum * cuNum;

    /* For dfs table, index scan is used when the selectivity is smaller than 0.001. */
    elog(DEBUG1, "indexSelectivity: %f.", *indexSelectivity);
    if (*indexSelectivity > DFS_INDEX_SELECTIVITY_THRESHOLD) {
        *indexTotalCost += g_instance.cost_cxt.disable_cost * 0.5;
    }

    *indexTotalCost -= 100.0 * u_sess->attr.attr_sql.cpu_operator_cost;

    /* cbtree is a bit better than psort for search now. */
    *indexTotalCost *= 0.9;

    *indexCorrelation = 1.0;

    PG_RETURN_VOID();
}

/*
 * Support routines for gincostestimate
 */
typedef struct {
    bool haveFullScan;
    double partialEntries;
    double exactEntries;
    double searchEntries;
    double arrayScans;
} GinQualCounts;

/* Find the index column matching "op"; return its index, or -1 if no match */
static int find_index_column(Node* op, IndexOptInfo* index)
{
    int i;

    for (i = 0; i < index->ncolumns; i++) {
        if (match_index_to_operand(op, i, index))
            return i;
    }

    return -1;
}

/*
 * Estimate the number of index terms that need to be searched for while
 * testing the given GIN query, and increment the counts in *counts
 * appropriately.  If the query is unsatisfiable, return false.
 */
static bool gincost_pattern(IndexOptInfo* index, int indexcol, Oid clause_op, Datum query, GinQualCounts* counts)
{
    Oid extractProcOid;
    int strategy_op;
    Oid left_type;
    Oid right_type;
    int32 nentries = 0;
    bool* partial_matches = NULL;
    Pointer* extra_data = NULL;
    bool* null_flags = NULL;
    int32 search_mode = GIN_SEARCH_MODE_DEFAULT;
    int32 i;

    Assert(indexcol < index->nkeycolumns);
    /*
     * Get the operator's strategy number and declared input data types within
     * the index opfamily.	(We don't need the latter, but we use
     * get_op_opfamily_properties because it will throw error if it fails to
     * find a matching pg_amop entry.)
     */
    get_op_opfamily_properties(clause_op, index->opfamily[indexcol], false, &strategy_op, &left_type, &right_type);

    /*
     * GIN always uses the "default" support functions, which are those with
     * left_type == right_type == the opclass' opcintype (see
     * index_support_initialize in relcache.c).
     */
    extractProcOid = get_opfamily_proc(
        index->opfamily[indexcol], index->opcintype[indexcol], index->opcintype[indexcol], GIN_EXTRACTQUERY_PROC);

    if (!OidIsValid(extractProcOid)) {
        /* should not happen; throw same error as index_getprocinfo */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_NO_FUNCTION_PROVIDED),
                    errmsg("missing support function %d for attribute %d of index \"%s\"",
                        GIN_EXTRACTQUERY_PROC,
                        indexcol + 1,
                        get_rel_name(index->indexoid)))));
    }

    OidFunctionCall7(extractProcOid,
        query,
        PointerGetDatum(&nentries),
        UInt16GetDatum(strategy_op),
        PointerGetDatum(&partial_matches),
        PointerGetDatum(&extra_data),
        PointerGetDatum(&null_flags),
        PointerGetDatum(&search_mode));

    if (nentries <= 0 && search_mode == GIN_SEARCH_MODE_DEFAULT) {
        /* No match is possible */
        return false;
    }

    for (i = 0; i < nentries; i++) {
        /*
         * For partial match we haven't any information to estimate number of
         * matched entries in index, so, we just estimate it as 100
         */
        if ((partial_matches != NULL) && partial_matches[i]) {
            counts->partialEntries += 100;
        } else {
            counts->exactEntries++;
        }

        counts->searchEntries++;
    }

    if (search_mode == GIN_SEARCH_MODE_INCLUDE_EMPTY) {
        /* Treat "include empty" like an exact-match item */
        counts->exactEntries++;
        counts->searchEntries++;
    } else if (search_mode != GIN_SEARCH_MODE_DEFAULT) {
        /* It's GIN_SEARCH_MODE_ALL */
        counts->haveFullScan = true;
    }

    return true;
}

/*
 * Estimate the number of index terms that need to be searched for while
 * testing the given GIN index clause, and increment the counts in *counts
 * appropriately.  If the query is unsatisfiable, return false.
 */
static bool gincost_opexpr(IndexOptInfo* index, OpExpr* clause, GinQualCounts* counts)
{
    Node* leftop = get_leftop((Expr*)clause);
    Node* rightop = get_rightop((Expr*)clause);
    Oid clause_op = clause->opno;
    int index_col;
    Node* operand = NULL;

    /* Locate the operand being compared to the index column */
    if ((index_col = find_index_column(leftop, index)) >= 0) {
        operand = rightop;
        if (operand == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                        errmsg("the right operand should not be null in gincost_opexpr"))));
        }
    } else if ((index_col = find_index_column(rightop, index)) >= 0) {
        operand = leftop;
        clause_op = get_commutator(clause_op);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_INDEX_OPERATOR_MISMATCH), errmsg("could not match index to operand"))));

        operand = NULL; /* keep compiler quiet */
    }

    if (IsA(operand, RelabelType)) {
        operand = (Node*)((RelabelType*)operand)->arg;
    }

    /*
     * It's impossible to call extractQuery method for unknown operand. So
     * unless operand is a Const we can't do much; just assume there will be
     * one ordinary search entry from the operand at runtime.
     */
    if (!IsA(operand, Const)) {
        counts->exactEntries++;
        counts->searchEntries++;
        return true;
    }

    /* If Const is null, there can be no matches */
    if (((Const*)operand)->constisnull) {
        return false;
    }

    /* Otherwise, apply extractQuery and get the actual term counts */
    return gincost_pattern(index, index_col, clause_op, ((Const*)operand)->constvalue, counts);
}

/*
 * Estimate the number of index terms that need to be searched for while
 * testing the given GIN index clause, and increment the counts in *counts
 * appropriately.  If the query is unsatisfiable, return false.
 *
 * A ScalarArrayOpExpr will give rise to N separate indexscans at runtime,
 * each of which involves one value from the RHS array, plus all the
 * non-array quals (if any).  To model this, we average the counts across
 * the RHS elements, and add the averages to the counts in *counts (which
 * correspond to per-indexscan costs).	We also multiply counts->arrayScans
 * by N, causing gincostestimate to scale up its estimates accordingly.
 */
static bool gincost_scalararrayopexpr(
    IndexOptInfo* index, ScalarArrayOpExpr* clause, double num_index_entries, GinQualCounts* counts)
{
    Node* left_op = (Node*)linitial(clause->args);
    Node* right_op = (Node*)lsecond(clause->args);
    Oid clause_op = clause->opno;
    int index_col;
    ArrayType* array_val = NULL;
    int16 elm_len;
    bool elm_by_val = false;
    char elm_align;
    int num_elems;
    Datum* elem_values = NULL;
    bool* elem_nulls = NULL;
    GinQualCounts array_counts;
    int num_possible = 0;
    int i;

    Assert(clause->useOr);

    /* index column must be on the left */
    if ((index_col = find_index_column(left_op, index)) < 0) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_INDEX_OPERATOR_MISMATCH), errmsg("could not match index to operand"))));
    }

    if (IsA(right_op, RelabelType)) {
        right_op = (Node*)((RelabelType*)right_op)->arg;
    }

    /*
     * It's impossible to call extractQuery method for unknown operand. So
     * unless operand is a Const we can't do much; just assume there will be
     * one ordinary search entry from each array entry at runtime, and fall
     * back on a probably-bad estimate of the number of array entries.
     */
    if (!IsA(right_op, Const)) {
        counts->exactEntries++;
        counts->searchEntries++;
        counts->arrayScans *= estimate_array_length(right_op);
        return true;
    }

    /* If Const is null, there can be no matches */
    if (((Const*)right_op)->constisnull) {
        return false;
    }

    /* Otherwise, extract the array elements and iterate over them */
    array_val = DatumGetArrayTypeP(((Const*)right_op)->constvalue);
    get_typlenbyvalalign(ARR_ELEMTYPE(array_val), &elm_len, &elm_by_val, &elm_align);
    deconstruct_array(array_val, ARR_ELEMTYPE(array_val), elm_len, elm_by_val, elm_align,
                      &elem_values, &elem_nulls, &num_elems);

    errno_t rc = memset_s(&array_counts, sizeof(array_counts), 0, sizeof(array_counts));
    securec_check(rc, "\0", "\0");

    for (i = 0; i < num_elems; i++) {
        GinQualCounts elemcounts;

        /* NULL can't match anything, so ignore, as the executor will */
        if (elem_nulls[i]) {
            continue;
        }

        /* Otherwise, apply extractQuery and get the actual term counts */
        errno_t rc = memset_s(&elemcounts, sizeof(elemcounts), 0, sizeof(elemcounts));
        securec_check(rc, "\0", "\0");

        if (gincost_pattern(index, index_col, clause_op, elem_values[i], &elemcounts)) {
            /* We ignore array elements that are unsatisfiable patterns */
            num_possible++;

            if (elemcounts.haveFullScan) {
                /*
                 * Full index scan will be required.  We treat this as if
                 * every key in the index had been listed in the query; is
                 * that reasonable?
                 */
                elemcounts.partialEntries = 0;
                elemcounts.exactEntries = num_index_entries;
                elemcounts.searchEntries = num_index_entries;
            }
            array_counts.partialEntries += elemcounts.partialEntries;
            array_counts.exactEntries += elemcounts.exactEntries;
            array_counts.searchEntries += elemcounts.searchEntries;
        }
    }

    if (num_possible == 0) {
        /* No satisfiable patterns in the array */
        return false;
    }

    /*
     * Now add the averages to the global counts.  This will give us an
     * estimate of the average number of terms searched for in each indexscan,
     * including contributions from both array and non-array quals.
     */
    counts->partialEntries += array_counts.partialEntries / num_possible;
    counts->exactEntries += array_counts.exactEntries / num_possible;
    counts->searchEntries += array_counts.searchEntries / num_possible;

    counts->arrayScans *= num_possible;

    /* free the unused buffer */
    pfree_ext(elem_nulls);

    return true;
}

/*
 * GIN has search behavior completely different from other index types
 */
Datum gincostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loop_count = PG_GETARG_FLOAT8(2);
    Cost* index_startup_cost = (Cost*)PG_GETARG_POINTER(3);
    Cost* index_total_cost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* index_selectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* index_correlation = (double*)PG_GETARG_POINTER(6);
    IndexOptInfo* index = path->indexinfo;
    List* index_quals = path->indexquals;
    List* index_orderbys = path->indexorderbys;
    ListCell* l = NULL;
    List* selectivity_quals = NIL;
    double num_pages = index->pages;
    double num_tuples = IDXOPTINFO_LOCAL_FIELD(root, index, tuples);
    double num_entry_pages;
    double num_data_pages;
    double num_pending_pages;
    double num_entries;
    GinQualCounts counts;
    bool match_possible = false;
    double entry_pages_fetched;
    double data_pages_fetched;
    double data_pages_fetched_by_sel;
    double qual_op_cost;
    double qual_arg_cost;
    double spc_random_page_cost;
    double outer_scans;
    QualCost index_qual_cost;
    Relation index_rel;
    GinStatsData gin_stats;
    bool is_partitioned_index = path->indexinfo->rel->isPartitionedTable;
    List* saved_varratios = NIL;

    /*
     * Obtain statistic information from the meta page
     */
    index_rel = index_open(index->indexoid, AccessShareLock);
    ginGetStats(index_rel, &gin_stats);
    index_close(index_rel, AccessShareLock);

    num_entry_pages = gin_stats.nEntryPages;
    num_data_pages = gin_stats.nDataPages;
    num_pending_pages = gin_stats.nPendingPages;
    num_entries = gin_stats.nEntries;

    /*
     * nPendingPages can be trusted, but the other fields are as of the last
     * VACUUM.	Scale them by the ratio num_pages / nTotalPages to account for
     * growth since then.  If the fields are zero (implying no VACUUM at all,
     * and an index created pre-9.1), assume all pages are entry pages.
     */
    if (gin_stats.nTotalPages == 0 || gin_stats.nEntryPages == 0) {
        num_entry_pages = num_pages;
        num_data_pages = 0;
        num_entries = num_tuples; /* bogus, but no other info available */
    } else {
        double scale = num_pages / gin_stats.nTotalPages;

        num_entry_pages = ceil(num_entry_pages * scale);
        num_data_pages = ceil(num_data_pages * scale);
        num_entries = ceil(num_entries * scale);
        /* ensure we didn't round up too much */
        num_entry_pages = Min(num_entry_pages, num_pages);
        num_data_pages = Min(num_data_pages, num_pages - num_entry_pages);
    }

    /* In an empty index, num_entries could be zero.  Avoid divide-by-zero */
    if (num_entries < 1) {
        num_entries = 1;
    }

    /*
     * Include predicate in selectivity_quals (should match
     * generic_cost_estimate)
     */
    if (index->indpred != NIL) {
        List* predExtraQuals = NIL;

        foreach (l, index->indpred) {
            Node* predQual = (Node*)lfirst(l);
            List* oneQual = list_make1(predQual);

            if (!predicate_implied_by(oneQual, index_quals)) {
                predExtraQuals = list_concat(predExtraQuals, oneQual);
            }
        }
        /* list_concat avoids modifying the passed-in index_quals list */
        selectivity_quals = list_concat(predExtraQuals, index_quals);
    } else {
        selectivity_quals = index_quals;
    }

    saved_varratios = index->rel->varratio;
    index->rel->varratio = NULL;
    /* Estimate the fraction of main-table tuples that will be visited */
    *index_selectivity = clauselist_selectivity(root, selectivity_quals, index->rel->relid, JOIN_INNER, NULL, false);
    list_free_deep(index->rel->varratio);
    index->rel->varratio = saved_varratios;

    /* fetch estimated page cost for schema containing index */
    get_tablespace_page_costs(index->reltablespace, &spc_random_page_cost, NULL);

    /*
     * Generic assumption about index correlation: there isn't any.
     */
    *index_correlation = 0.0;

    /*
     * Examine quals to estimate number of search entries & partial matches
     */
    errno_t rc = memset_s(&counts, sizeof(counts), 0, sizeof(counts));
    securec_check(rc, "", "");
    counts.arrayScans = 1;
    match_possible = true;

    foreach (l, index_quals) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);
        Expr* clause = NULL;

        Assert(IsA(rinfo, RestrictInfo));
        clause = rinfo->clause;
        if (IsA(clause, OpExpr)) {
            match_possible = gincost_opexpr(index, (OpExpr*)clause, &counts);
            if (!match_possible) {
                break;
            }
        } else if (IsA(clause, ScalarArrayOpExpr)) {
            match_possible = gincost_scalararrayopexpr(index, (ScalarArrayOpExpr*)clause, num_entries, &counts);
            if (!match_possible) {
                break;
            }
        } else {
            /* shouldn't be anything else for a GIN index */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unsupported GIN indexqual type: %d", (int)nodeTag(clause)))));
        }
    }

    /* Fall out if there were any provably-unsatisfiable quals */
    if (!match_possible) {
        *index_startup_cost = 0;
        *index_total_cost = 0;
        *index_selectivity = 0;
        PG_RETURN_VOID();
    }

    if (counts.haveFullScan || index_quals == NIL) {
        /*
         * Full index scan will be required.  We treat this as if every key in
         * the index had been listed in the query; is that reasonable?
         */
        counts.partialEntries = 0;
        counts.exactEntries = num_entries;
        counts.searchEntries = num_entries;
    }

    /* Will we have more than one iteration of a nestloop scan? */
    outer_scans = loop_count;

    /*
     * Compute cost to begin scan, first of all, pay attention to pending
     * list.
     */
    entry_pages_fetched = num_pending_pages;

    /*
     * Estimate number of entry pages read.  We need to do
     * counts.searchEntries searches.  Use a power function as it should be,
     * but tuples on leaf pages usually is much greater. Here we include all
     * searches in entry tree, including search of first entry in partial
     * match algorithm
     */
    entry_pages_fetched += ceil(counts.searchEntries * rint(pow(num_entry_pages, 0.15)));

    /*
     * Add an estimate of entry pages read by partial match algorithm. It's a
     * scan over leaf pages in entry tree.	We haven't any useful stats here,
     * so estimate it as proportion.
     */
    entry_pages_fetched += ceil(num_entry_pages * counts.partialEntries / num_entries);

    /*
     * Partial match algorithm reads all data pages before doing actual scan,
     * so it's a startup cost. Again, we haven't any useful stats here, so,
     * estimate it as proportion
     */
    data_pages_fetched = ceil(num_data_pages * counts.partialEntries / num_entries);

    /*
     * Calculate cache effects if more than one scan due to nestloops or array
     * quals.  The result is pro-rated per nestloop scan, but the array qual
     * factor shouldn't be pro-rated (compare generic_cost_estimate).
     */
    if (outer_scans > 1 || counts.arrayScans > 1) {
        entry_pages_fetched *= outer_scans * counts.arrayScans;
        entry_pages_fetched = index_pages_fetched(entry_pages_fetched, (BlockNumber)num_entry_pages, num_entry_pages,
            root, is_partitioned_index);
        entry_pages_fetched /= outer_scans;
        data_pages_fetched *= outer_scans * counts.arrayScans;
        data_pages_fetched = index_pages_fetched(data_pages_fetched, (BlockNumber)num_data_pages, num_data_pages,
            root, is_partitioned_index);
        data_pages_fetched /= outer_scans;
    }

    /*
     * Here we use random page cost because logically-close pages could be far
     * apart on disk.
     */
    *index_startup_cost = (entry_pages_fetched + data_pages_fetched) * spc_random_page_cost;

    /*
     * Now we compute the number of data pages fetched while the scan
     * proceeds.
     */

    /* data pages scanned for each exact (non-partial) matched entry */
    data_pages_fetched = ceil(num_data_pages * counts.exactEntries / num_entries);

    /*
     * Estimate number of data pages read, using selectivity estimation and
     * capacity of data page.
     */
    data_pages_fetched_by_sel = ceil(*index_selectivity * (num_tuples / (BLCKSZ / SizeOfIptrData)));

    if (data_pages_fetched_by_sel > data_pages_fetched) {
        /*
         * At least one of entries is very frequent and, unfortunately, we
         * couldn't get statistic about entries (only tsvector has such
         * statistics). So, we obviously have too small estimation of pages
         * fetched from data tree. Re-estimate it from known capacity of data
         * pages
         */
        data_pages_fetched = data_pages_fetched_by_sel;
    }

    /* Account for cache effects, the same as above */
    if (outer_scans > 1 || counts.arrayScans > 1) {
        data_pages_fetched *= outer_scans * counts.arrayScans;
        data_pages_fetched = index_pages_fetched(data_pages_fetched, (BlockNumber)num_data_pages, num_data_pages,
            root, is_partitioned_index);
        data_pages_fetched /= outer_scans;
    }

    /* And apply random_page_cost as the cost per page */
    *index_total_cost = *index_startup_cost + data_pages_fetched * spc_random_page_cost;

    /*
     * Add on index qual eval costs, much as in generic_cost_estimate
     */
    cost_qual_eval(&index_qual_cost, index_quals, root);
    qual_arg_cost = index_qual_cost.startup + index_qual_cost.per_tuple;
    cost_qual_eval(&index_qual_cost, index_orderbys, root);
    qual_arg_cost += index_qual_cost.startup + index_qual_cost.per_tuple;
    qual_op_cost = u_sess->attr.attr_sql.cpu_operator_cost * (list_length(index_quals) + list_length(index_orderbys));
    qual_arg_cost -= qual_op_cost;
    if (qual_arg_cost < 0) { /* just in case... */
        qual_arg_cost = 0;
    }

    *index_startup_cost += qual_arg_cost;
    *index_total_cost += qual_arg_cost;
    *index_total_cost += ((num_tuples * *index_selectivity) *
        (u_sess->attr.attr_sql.cpu_index_tuple_cost + qual_op_cost));

    PG_RETURN_VOID();
}

bool is_func_distinct_unshippable(Oid funcid)
{
    for (uint i = 0; i < lengthof(distinct_unshippable_func); i++) {
        if (funcid == distinct_unshippable_func[i]) {
            return true;
        }
    }
    return false;
}

static List* specialExpr_group_num(PlannerInfo* root, List* node_list, double* num_distinct, double rows)
{
    List* var_list = NIL;
    List* all_var_list = NIL;
    List* result_list = NIL;
    ListCell* lc = NULL;
    double local_distinct = 1.0;
    double agg_distinct = 1.0;
    double var_distinct;

    /*
     * For case when, substr func or agg, now we can not exact get their estimated value,
     * give them a tiny default value. we may be alter here code when we can
     * exact estimate.
     * With more complicated case, multiple such expressions share the same var. We should
     * distinguish such circumstance and only count expression with non-duplicate vars. Finally,
     * we should also compare the estimation with var estimation to make it better.
     */
    foreach (lc, node_list) {
        Node* expr = (Node*)lfirst(lc);
        if (IsA(expr, Var)) {
            var_list = lappend(var_list, expr);
        }
    }
    foreach (lc, node_list) {
        Node* expr = (Node*)lfirst(lc);
        EstSPNode* sp = (EstSPNode*)expr;
        if (IsA(sp, EstSPNode)) {
            if (IsA(sp->expr, Aggref)) {
                agg_distinct *= DEFAULT_SPECIAL_EXPR_DISTINCT;
            } else {
                /* For no overlap vars, we should count it */
                if ((result_list = list_intersection(var_list, sp->varlist)) == NIL) {
                    if (exprType(sp->expr) == BOOLOID) {
                        local_distinct *= 2;
                    } else {
                        local_distinct *= DEFAULT_SPECIAL_EXPR_DISTINCT;
                    }
                }
                list_free_ext(result_list);
                all_var_list = list_concat_unique(all_var_list, sp->varlist);
            }
            list_free_ext(sp->varlist);
            pfree_ext(sp);
        }
    }
    var_distinct = estimate_num_groups(root, all_var_list, rows, u_sess->pgxc_cxt.NumDataNodes);
    *num_distinct *= Min(Min(local_distinct, var_distinct) * agg_distinct, rows);

    list_free_ext(all_var_list);
    list_free_ext(node_list);
    return var_list;
}

/*
 * set_local_rel_size
 *	Set local size of rel according to the global size
 * Parameters:
 *	root: query info of current query level
 *	rel: rel to calculate
 */
void set_local_rel_size(PlannerInfo* root, RelOptInfo* rel)
{
    if (rel->distribute_keys) {
        /*
         * determine if table is skewed for the distribute key. Since multiple is related to final
         * rows, we should use final tuples to do estimation, as if there's no filter
         */
        if (rel->multiple == 0.0) {
            double saved_rows = rel->rows;
            rel->rows = rel->tuples;
            rel->multiple = get_multiple_by_distkey(root, rel->distribute_keys, rel->tuples);
            rel->rows = saved_rows;
        }
    } else { /* The local is even for joinrel and RROBIN */
        RangeTblEntry* rte = NULL;
        rte = planner_rt_fetch(rel->relid, root);

        if (IS_EC_FUNC(rte)) {
            rel->multiple = (double)u_sess->pgxc_cxt.NumDataNodes;
        } else {
            rel->multiple = 1.0;
        }
    }

    RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);
    if (rte->relkind != RELKIND_FOREIGN_TABLE) {
        rel->pages = RELOPTINFO_LOCAL_FIELD(root, rel, pages);
    }

    /* Set baserel index tuples and pages. */
    if (rel->indexlist) {
        ListCell* lc = NULL;
        foreach (lc, rel->indexlist) {
            IndexOptInfo* idx = (IndexOptInfo*)lfirst(lc);

            idx->pages = IDXOPTINFO_LOCAL_FIELD(root, idx, pages);
        }
    }
}

/* Get hybrd multiple between skew and bias by distribute key. */
double get_multiple_by_distkey(PlannerInfo* root, List* dist_key, double rows)
{
    bool use_skew_multiple = true;
    double result_multiple = 1.0;
    double bias_multiple = 0.0;
    double skew_multiple = 0.0;

    if ((root == NULL) || (dist_key == NULL)) {
        return result_multiple;
    }

    get_multiple_from_exprlist(root, dist_key, rows, &use_skew_multiple, true, &skew_multiple, &bias_multiple);

    if ((skew_multiple == 1) && (bias_multiple < 1)) {
        result_multiple = 1;
    } else {
        result_multiple = Max(bias_multiple, skew_multiple);
    }

    Assert(result_multiple >= 1);

    return result_multiple;
}

/*
 * estimate_agg_num_distinct: Reestimate local distinct value for hashagg.
 *	Before, we estimate local distinct value of group exprs for the final aggregation,
 *	but if it needs a local aggregation sometimes, then we should reestimate it for
 *	the accurate cost estimation of first local aggreation.
 *
 * Parameters:
 *	@In root: PlannerInfo strunct for current query level
 *	@In group_exprs: group exprs whose distinct value needs to be estimated
 *	@In Plan: current plan node to do aggregation
 *	@In numGroups: local and global distinct value already estimated
 * Returns: adjusted local distinct value
 */
double estimate_agg_num_distinct(PlannerInfo* root, List* group_exprs, Plan* plan, const double* num_groups)
{
    if (plan == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Unexpected null value for the plan")));
    }

    int dop = SET_DOP(plan->dop);
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(plan);
    double rel_multiple = (double)num_datanodes * dop;
    bool is_groupexprs_contain_plandistkey = false;
    bool plandistkey_is_rel_diskey = false;
    double local_distinct;
    double plan_rows = PLAN_LOCAL_ROWS(plan);
    List* sub_distribute_keys = NIL;

    /* we should use global distinct for replication or query execute on datanode. */
    if (!IS_PGXC_COORDINATOR || (plan->exec_nodes && IsLocatorReplicated(plan->exec_nodes->baselocatortype))) {
        return num_groups[1];
    }

    sub_distribute_keys = find_skew_join_distribute_keys(plan);
    /*
     * judge if plan's distribute key is base rel's, or we can possibly use local distinct estimation
     * if there's no skew on base rel.
     * we should use local distinct if there's only one rel in group exprs and plan distkey is that rel distkey.
     */
    if (sub_distribute_keys != NIL) {
        Relids group_varnos = pull_varnos((Node*)group_exprs);
        int varno = 0;
        /* if there are multiple rels involved, try to remove const vars */
        if (bms_num_members(group_varnos) > 1) {
            List* group_exprs_copy = list_copy(group_exprs);
            ListCell* lc = NULL;
            ListCell* lc2 = NULL;
            foreach (lc, root->eq_classes) {
                EquivalenceClass* ec = (EquivalenceClass*)lfirst(lc);
                if (ec->ec_has_const) {
                    foreach (lc2, ec->ec_members) {
                        EquivalenceMember* em = (EquivalenceMember*)lfirst(lc2);
                        if (list_member(group_exprs_copy, em->em_expr)) {
                            group_exprs_copy = list_delete(group_exprs_copy, em->em_expr);
                        }
                    }
                }
            }
            bms_free(group_varnos);
            group_varnos = pull_varnos((Node*)group_exprs_copy);
            if (bms_num_members(group_varnos) == 1) {
                varno = bms_first_member(group_varnos);
            }
            list_free_ext(group_exprs_copy);
        } else {
            varno = bms_first_member(group_varnos);
        }

        /* Locate the base rel, and judge if the distribute key is equal */
        if (varno > 0) {
            RelOptInfo* rel = find_base_rel(root, varno);
            if (equal_distributekey(root, sub_distribute_keys, rel->distribute_keys)) {
                rel_multiple = get_multiple_by_distkey(root, rel->distribute_keys, rel->tuples);
                plandistkey_is_rel_diskey = true;
            }
        }
        bms_free(group_varnos);
        group_varnos = NULL;
    }

    is_groupexprs_contain_plandistkey = !(needs_agg_stream(root, group_exprs, sub_distribute_keys));

    /*
     * Estimate local distinct according to global.
     * group_exprs include plan->distribute_keys.
     */
    if (is_groupexprs_contain_plandistkey) {
        /* group_exprs include plan->distribute_keys and plan->distributed_keys include baserel distkey. */
        if (plandistkey_is_rel_diskey) {
            local_distinct =
                (rel_multiple > 1) ? get_local_rows(num_groups[1], 1.0, false, num_datanodes) : num_groups[0];
        } else { /* group_exprs include plan->distribute_keys and plan->distributed_keys not include baserel distkey. */
            local_distinct = get_local_rows(num_groups[1], 1.0, false, num_datanodes);
        }

        /*
         * In agg+local_redistribute + agg situation case,
         * the local distinct value will increase. So, recalculate it.
         */
        if (dop > 1 && is_local_redistribute_needed(plan)) {
            double numLocalGroup = local_distinct;
            local_distinct = NUM_PARALLEL_DISTINCT_GTL_FOR_POISSON(numLocalGroup, plan_rows, dop) * dop;
            local_distinct = Min(clamp_row_est(local_distinct), plan_rows);
            local_distinct = Min(local_distinct, numLocalGroup * dop);
            return local_distinct;
        }
    } else { /* group_exprs not include plan->distribute_keys. */

        double plan_multiple = get_multiple_by_distkey(root, sub_distribute_keys, plan->plan_rows);
        /*
         * plan->distributed_keys include baserel distkey, or
         * plan->distributed_keys not include baserel distkey and group_exprs not include baserel distkey.
         */
        if (plandistkey_is_rel_diskey) {
            if (rel_multiple > 1) {
                local_distinct =
                    (NUM_DISTINCT_GTL_FOR_POISSON(num_groups[1], plan->plan_rows, num_datanodes, dop) * plan_multiple) *
                    dop;
            } else {
                if (dop > 1) {
                    local_distinct = NUM_PARALLEL_DISTINCT_GTL_FOR_POISSON(num_groups[0], plan_rows, dop) * dop;
                } else {
                    local_distinct = num_groups[0];
                }
            }
        } else { /* plan->distributed_keys not include baserel distkey and group_exprs include baserel distkey. */
            local_distinct =
                NUM_DISTINCT_GTL_FOR_POISSON(num_groups[1], plan->plan_rows, num_datanodes, dop) * plan_multiple * dop;
        }
    }

    /*
     * The local_distinct refer to distinct in one DN, when parallel,
     * the local_distinct must be lower than num_groups[1] * dop.
     */
    local_distinct = Min(clamp_row_est(local_distinct), plan_rows);
    local_distinct = Min(local_distinct, num_groups[1] * dop);
    return local_distinct;
}

/*
 * estimate_agg_num_distinct: Reestimate local distinct value for hashagg.
 *	Before, we estimate local distinct value of group exprs for the final aggregation,
 *	but if it needs a local aggregation sometimes, then we should reestimate it for
 *	the accurate cost estimation of first local aggreation.
 *
 * Parameters:
 *	@In root: PlannerInfo strunct for current query level
 *	@In group_exprs: group exprs whose distinct value needs to be estimated
 *	@In path: current path node to do aggregation
 *	@In numGroups: local and global distinct value already estimated
 * Returns: adjusted local distinct value
 */
double estimate_agg_num_distinct(PlannerInfo* root, List* group_exprs, Path* path, const double* num_groups)
{
    if (path == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Unexpected null value for the path")));
    }

    int dop = SET_DOP(path->dop);
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(path);
    double rel_multiple = (double)num_datanodes * dop;
    bool is_groupexprs_contain_plandistkey = false;
    bool plandistkey_is_rel_diskey = false;
    double local_distinct;
    double plan_rows = PATH_LOCAL_ROWS(path);

    /* we should use global distinct for replication or query execute on datanode. */
    if (!IS_PGXC_COORDINATOR || (path && IsLocatorReplicated(path->locator_type))) {
        return num_groups[1];
    }

    /*
     * judge if path's distribute key is base rel's, or we can possibly use local distinct estimation
     * if there's no skew on base rel.
     * we should use local distinct if there's only one rel in group exprs and plan distkey is that rel distkey.
     */
    if (path->distribute_keys != NIL) {
        Relids group_varnos = pull_varnos((Node*)group_exprs);
        int varno = 0;
        /* if there are multiple rels involved, try to remove const vars */
        if (bms_num_members(group_varnos) > 1) {
            List* group_exprs_copy = list_copy(group_exprs);
            ListCell* lc = NULL;
            ListCell* lc2 = NULL;
            foreach (lc, root->eq_classes) {
                EquivalenceClass* ec = (EquivalenceClass*)lfirst(lc);
                if (ec->ec_has_const) {
                    foreach (lc2, ec->ec_members) {
                        EquivalenceMember* em = (EquivalenceMember*)lfirst(lc2);
                        if (list_member(group_exprs_copy, em->em_expr))
                            group_exprs_copy = list_delete(group_exprs_copy, em->em_expr);
                    }
                }
            }
            bms_free(group_varnos);
            group_varnos = pull_varnos((Node*)group_exprs_copy);
            if (bms_num_members(group_varnos) == 1)
                varno = bms_first_member(group_varnos);
            list_free_ext(group_exprs_copy);
        } else {
            varno = bms_first_member(group_varnos);
        }

        /* Locate the base rel, and judge if the distribute key is equal */
        if (varno > 0) {
            RelOptInfo* rel = find_base_rel(root, varno);
            if (equal_distributekey(root, path->distribute_keys, rel->distribute_keys)) {
                rel_multiple = get_multiple_by_distkey(root, rel->distribute_keys, rel->tuples);
                plandistkey_is_rel_diskey = true;
            }
        }
        bms_free(group_varnos);
        group_varnos = NULL;
    }
    is_groupexprs_contain_plandistkey = !(needs_agg_stream(root, group_exprs, path->distribute_keys));

    /*
     * Estimate local distinct according to global.
     * group_exprs include path->distribute_keys.
     */
    if (is_groupexprs_contain_plandistkey) {
        /* group_exprs include plan->distribute_keys and plan->distributed_keys include baserel distkey. */
        if (plandistkey_is_rel_diskey) {
            local_distinct =
                (rel_multiple > 1) ? get_local_rows(num_groups[1], 1.0, false, num_datanodes) : num_groups[0];
        } else { /* group_exprs include plan->distribute_keys and plan->distributed_keys not include baserel distkey. */
            local_distinct = get_local_rows(num_groups[1], 1.0, false, num_datanodes);
        }

        /*
         * In agg+local_redistribute + agg situation case,
         * the local distinct value will increase. So, recalculate it.
         */
        if (dop > 1) {
            double num_local_group = local_distinct;
            local_distinct = NUM_PARALLEL_DISTINCT_GTL_FOR_POISSON(num_local_group, plan_rows, dop) * dop;
            local_distinct = Min(clamp_row_est(local_distinct), plan_rows);
            local_distinct = Min(local_distinct, num_local_group * dop);
            return local_distinct;
        }
    } else {
        double plan_multiple = get_multiple_by_distkey(root, path->distribute_keys, path->rows);
        /*
         * path->distributed_keys include baserel distkey, or
         * path->distributed_keys not include baserel distkey and group_exprs not include baserel distkey.
         */
        if (plandistkey_is_rel_diskey) {
            if (rel_multiple > 1) {
                local_distinct =
                    (NUM_DISTINCT_GTL_FOR_POISSON(num_groups[1], path->rows, num_datanodes, dop) * plan_multiple) * dop;
            } else {
                if (dop > 1) {
                    local_distinct = NUM_PARALLEL_DISTINCT_GTL_FOR_POISSON(num_groups[0], plan_rows, dop) * dop;
                } else {
                    local_distinct = num_groups[0];
                }
            }
        } else { /* plan->distributed_keys not include baserel distkey and group_exprs include baserel distkey. */
            local_distinct =
                NUM_DISTINCT_GTL_FOR_POISSON(num_groups[1], path->rows, num_datanodes, dop) * plan_multiple * dop;
        }
    }
    /*
     * The local_distinct refer to distinct in one DN, when parallel,
     * the local_distinct must be lower than num_groups[1] * dop.
     */
    local_distinct = Min(clamp_row_est(local_distinct), plan_rows);
    local_distinct = Min(local_distinct, num_groups[1] * dop);
    return local_distinct;
}

/*
 * estimate_hash_num_distinct
 *	adjust local dn inner hash distinct value
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level
 *	@in hashclauses: hash join clause of current join
 *	@in hashkey: current hash join node
 *	@in inner_path: inner side path of join
 *	@in vardata: statistic data of hashkey
 *	@in local_ndistinct: calculated local distinct from stats
 *	@in global_ndisitnct: calculated global distinct from stats
 *	@out usesinglestats: if we use single dn stats
 * Returns:
 *	adjusted local inner distinct value
 */
double estimate_hash_num_distinct(PlannerInfo* root, List* hash_key, Path* inner_path, VariableStatData* var_data,
    double local_ndistinct, double global_ndistinct, bool* use_single_stats)
{
    int dop = inner_path->dop > 1 ? inner_path->dop : 1;
    double ndistinct = local_ndistinct;

    if (!IS_PGXC_COORDINATOR) {
        return global_ndistinct;
    }

    Assert(IsA(inner_path, StreamPath) || IsA(inner_path, HashPath) || IsLocatorReplicated(inner_path->locator_type));

    if (IsLocatorReplicated(inner_path->locator_type)) {
        /* Inner is broadcast or replication, we should use global distinct. */
        ndistinct = global_ndistinct;
        *use_single_stats = false;
    } else {
        unsigned int num_datanodes = ng_get_dest_num_data_nodes(inner_path);

        double rel_multiple = (double)num_datanodes * dop;
        double plan_multiple = get_multiple_by_distkey(root, inner_path->distribute_keys, inner_path->rows);
        bool is_join_clauses_contain_inner_key = false;
        bool is_inner_distkey_contain_base_rel_distkey = false;

        /*
         * It means that distkey is not in joinclauses if distribute on joinclauses,
         * Other wise, it means distkey is in joinclauses.
         */
        if (inner_path->distribute_keys) {
            if (list_length(hash_key) >= list_length(inner_path->distribute_keys)) {
                is_join_clauses_contain_inner_key = list_is_subset(inner_path->distribute_keys, hash_key);
            }
            if (var_data->rel->reloptkind == RELOPT_BASEREL &&
                list_length(inner_path->distribute_keys) == list_length(var_data->rel->distribute_keys) &&
                !(needs_agg_stream(root, inner_path->distribute_keys, var_data->rel->distribute_keys))) {
                is_inner_distkey_contain_base_rel_distkey = true;
                rel_multiple = get_multiple_by_distkey(root, var_data->rel->distribute_keys, var_data->rel->tuples);
            }
        }

        if (is_join_clauses_contain_inner_key && is_inner_distkey_contain_base_rel_distkey) {
            ndistinct =
                (rel_multiple > 1) ? get_local_rows(global_ndistinct, 1.0, false, num_datanodes) : local_ndistinct;
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("is_join_clauses_contain_inner_key && is_inner_distkey_contain_base_rel_distkey: \
                            ndistinct (%.0f) = global_ndistinct(%.0f) / num_datanodes(%u) or \
                            = local_ndistinct(%.0f) depending on rel_multiple(%f) > 1 or not",
                        ndistinct,
                        global_ndistinct,
                        num_datanodes,
                        local_ndistinct,
                        rel_multiple))));
        } else if (is_join_clauses_contain_inner_key && !is_inner_distkey_contain_base_rel_distkey) {
            ndistinct = get_local_rows(global_ndistinct, 1.0, false, num_datanodes);
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("is_join_clauses_contain_inner_key && !is_inner_distkey_contain_base_rel_distkey: \
                            ndistinct (%.0f) = global_ndistinct(%.0f) / num_datanodes(%u) ",
                        ndistinct,
                        global_ndistinct,
                        num_datanodes))));
        } else if (!is_join_clauses_contain_inner_key && is_inner_distkey_contain_base_rel_distkey) {
            /*
             * if biase more than 1, we consider all the mcv is even in all dn, so it should use the distinct on dn1.
             * other wise, it should use formular to estimate single dn according to global.
             */
            ndistinct = (rel_multiple > 1)
                            ? NUM_DISTINCT_GTL_FOR_POISSON(global_ndistinct, var_data->rel->rows, num_datanodes, dop) *
                                  plan_multiple
                            : local_ndistinct;
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("!is_join_clauses_contain_inner_key && is_inner_distkey_contain_base_rel_distkey: \
                        ndistinct = %.0f \
                        using poisson model with global_ndistinct(%.0f) , var_data->rel->rows(%e), \
                        num_datanodes(%u), dop(%d), plan_multiple(%f) \
                        or just equals local_ndistinct(%.0f) depending on rel_multiple(%f) > 1 or not",
                        ndistinct,
                        global_ndistinct,
                        var_data->rel->rows,
                        num_datanodes,
                        dop,
                        plan_multiple,
                        local_ndistinct,
                        rel_multiple))));
        } else {
            bool is_join_clauses_contain_rel_distkey = false;
            if (list_length(hash_key) >= list_length(var_data->rel->distribute_keys)) {
                is_join_clauses_contain_rel_distkey = list_is_subset(var_data->rel->distribute_keys, hash_key);
            }

            /* if skew, join contains rel diskey, we should use poisson to calculate local distinct */
            if (is_join_clauses_contain_rel_distkey || (rel_multiple > 1)) {
                ndistinct = NUM_DISTINCT_GTL_FOR_POISSON(global_ndistinct, var_data->rel->rows, num_datanodes, dop) *
                            plan_multiple;
            }
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("other situation: \
                        ndistinct = %.0f \
                        using poisson model with global_ndistinct(%.0f) , var_data->rel->rows(%e), \
                        num_datanodes(%u), dop(%d), plan_multiple(%f) ",
                        ndistinct,
                        global_ndistinct,
                        var_data->rel->rows,
                        num_datanodes,
                        dop,
                        plan_multiple))));
        }
    }

    return ndistinct;
}

/*
 * estimate_local_numdistinct
 *	estimate local dn distinct value of a path
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level
 *	@in hashkey: current hash join node
 *	@in path: the path that hashkey belongs to
 *	@in sjinfo: special join info that path takes part in
 *	@out global_distinct: estimated global_distinct
 *	@out isdefault: if there's no real distinct estimated
 *	@out vardata: statistic data of hashkeyts
 * Returns:
 *	estimated distinct value
 */
double estimate_local_numdistinct(PlannerInfo* root, Node* hash_key, Path* path, SpecialJoinInfo* sj_info,
    double* global_distinct, bool* isdefault, VariableStatData* var_data)
{
    double ndistinct;
    bool use_single_stats = true;

    examine_variable(root, hash_key, 0, var_data);

    /* Get number of distinct values */
    ndistinct = get_variable_numdistinct(var_data, isdefault, false, 1.0, sj_info, STATS_TYPE_LOCAL);
    *global_distinct = get_variable_numdistinct(var_data, isdefault, false, 1.0, sj_info, STATS_TYPE_GLOBAL);

    /* If ndistinct isn't real, punt and return 0.1, per comments above */
    if (*isdefault) {
        ndistinct = DEFAULT_NUM_DISTINCT;
    } else {
        unsigned int num_datanodes = (path != NULL) ? ng_get_dest_num_data_nodes(path) : u_sess->pgxc_cxt.NumDataNodes;

        /* we should adjust local distinct if there is no tuples in dn1 for global stats. */
        if ((ndistinct * num_datanodes) < *global_distinct) {
            if (var_data->rel == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Unexpected null value for the rel of var_data")));
            }
            ndistinct = get_local_rows(*global_distinct, var_data->rel->multiple, false, num_datanodes);
        }

        /* Adjust global distinct values for STREAM_BROADCAST and STREAM_REDISTRIBUTE. */
        if (path != NULL) {
            ereport(DEBUG2,
                (errmodule(MOD_OPT),
                    (errmsg("Adjust global distinct values(%.0f) for STREAM_BROADCAST and STREAM_REDISTRIBUTE.",
                        *global_distinct))));
            List* hash_keys = list_make1(hash_key);
            ndistinct = estimate_hash_num_distinct(root, hash_keys, path, var_data, ndistinct,
                *global_distinct, &use_single_stats);
            list_free_ext(hash_keys);
        }

        /*
         * Adjust ndistinct to account for restriction clauses.  Observe we are
         * assuming that the data distribution is affected uniformly by the
         * restriction clauses!
         *
         * XXX Possibly better way, but much more expensive: multiply by
         * selectivity of rel's restriction clauses that mention the target Var.
         *
         * Only single stat need multiple the ratio as rows/tuples, because
         * possion for global stat.
         * Else if we have use possion, we don't need multiple the ratio.
         */
        if (var_data->rel) {
            if (use_single_stats || !var_data->enablePossion) {
                ndistinct *= var_data->rel->rows / var_data->rel->tuples;
                ereport(DEBUG2,
                    (errmodule(MOD_OPT),
                        (errmsg(
                            "To estimate local numdistinct, \
                            the distinct value is multiplied by relrows(%e)/reltuples(%e) \
                            The new distinct value is %.0f",
                            var_data->rel->rows,
                            var_data->rel->tuples,
                            ndistinct))));
            }
        }
    }

    return ndistinct;
}

/* get_num_distinct
 *	get local and global distinct by local and global rows
 *
 * Parameters:
 *	@in root: plannerinfo struct of current query level
 *	@in groupExprs: group expr to calculate distinct
 *	@in local_rows: single dn rows of current plan
 *	@in global_rows: global dn rows of current plan
 *	@out numdistinct: estimated local and global distinct value
 *	@in pgset: grouping set info
 */
void get_num_distinct(PlannerInfo* root, List* group_exprs, double local_rows, double global_rows,
    unsigned int num_datanodes, double* num_distinct, List** pgset)
{
    /* get local distinct */
    num_distinct[0] = estimate_num_groups(root, group_exprs, local_rows, num_datanodes, STATS_TYPE_LOCAL, pgset);

    /* get global distinct */
    num_distinct[1] = estimate_num_groups(root, group_exprs, global_rows, num_datanodes, STATS_TYPE_GLOBAL, pgset);

    /* we should adjust local distinct if there is no tuples in dn1 for global stats. */
    if ((num_distinct[0] * num_datanodes) < num_distinct[1]) {
        num_distinct[0] = get_local_rows(num_distinct[1], 1.0, false, num_datanodes);
    }

    num_distinct[0] = Min(num_distinct[0], num_distinct[1]);
}

/*
 * Description: Check whether the current relation analyzed or not.
 *
 * Parameters:
 *	@in relid: relation oid
 *
 * Returns: bool
 */
bool check_relation_analyzed(Oid relid)
{
    bool is_analyzed = false;
    PgStat_AnaCheckEntry* tab_entry = NULL;

    /* if set not check, just return false */
    if (!u_sess->attr.attr_sql.enable_analyze_check)
        return false;

    /* load analyzed info from statfile */
    if (u_sess->stat_cxt.analyzeCheckHash == NULL) {
        pgstat_read_analyzed();
    }

    /* load failed */
    if (u_sess->stat_cxt.analyzeCheckHash == NULL)
        return false;

    tab_entry = (PgStat_AnaCheckEntry*)hash_search(u_sess->stat_cxt.analyzeCheckHash, (void*)&relid, HASH_FIND, NULL);
    if (tab_entry != NULL) {
        is_analyzed = tab_entry->is_analyzed;
    }

    return is_analyzed;
}

/*
 * set_noanalyze_rellist
 *	Append no analyze relation or attribute to g_NoAnalyzeRelNameList.
 *
 * Paramters:
 *	@in relid: oid of current relation
 *	@in attid: attnum of current column, or 0 for whole relation
 */
void set_noanalyze_rellist(Oid relid, AttrNumber attid)
{
    /*
     * We should not save the relation to non-analyze list if is under analyzing,
     * because it will create temp table and execute some query, the temp table
     * don't be analyzed when 2% analyzing.
     */
    if (u_sess->analyze_cxt.is_under_analyze) {
        return;
    }

    if (check_relation_analyzed(relid)) {
        return;
    }

    List* tmp_rel_list = NIL;
    List* tmp_att_list = NIL;
    List* tmp_record_list = NIL;
    bool is_obs_ft = false;

    /*
     * As for OBS foreign table, we do not suggest analyze obs forign table,
     * so only check the statistics of foreign table. do not check column statistic.
     */
    Relation rel = relation_open(relid, AccessShareLock);
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE && isSpecifiedSrvTypeFromRelId(relid, OBS_SERVER)) {
        is_obs_ft = true;
    }

    /* as for column statistics of obs foreign table, we will return. */
    if (attid != 0 && is_obs_ft) {
        relation_close(rel, AccessShareLock);
        return;
    }

    /* If the rel is DFS table or gds foreign table, don't add it to list */
    if (RelationIsPAXFormat(rel) || RelationGetNamespace(rel) == CSTORE_NAMESPACE ||
        ((rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) && IsSpecifiedFDWFromRelid(relid, DIST_FDW))) {
        relation_close(rel, AccessShareLock);
        return;
    }
    relation_close(rel, AccessShareLock);

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    if (t_thrd.postgres_cxt.g_NoAnalyzeRelNameList == NIL) {
        tmp_rel_list = lappend_oid(tmp_rel_list, relid);

        /* Add a new rel list into g_NoAnalyzeRelNameList. */
        tmp_record_list = lappend(tmp_record_list, tmp_rel_list);

        /*
         *  If attid = 0, the att list mark the whole rel has no statistics.
         *  Once a new list with  attid = !0 be added, the list with attid = 0 will be deleted.
         */
        tmp_att_list = lappend_int(tmp_att_list, attid);
        tmp_record_list = lappend(tmp_record_list, tmp_att_list);

        t_thrd.postgres_cxt.g_NoAnalyzeRelNameList =
            lappend(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList, tmp_record_list);
    } else {
        ListCell *lc1 = NULL;
        ListCell *lc2 = NULL;
        bool is_rel_exist = false;

        foreach (lc1, t_thrd.postgres_cxt.g_NoAnalyzeRelNameList) {
            List* record = (List*)lfirst(lc1);
            /* Get the reloid */
            Oid tmp_relid = linitial_oid((List*)linitial(record));

            if (relid == tmp_relid) {
                if (attid == 0) {
                    /* The rel has no statistic. */
                    is_rel_exist = true;
                    break;
                } else {
                    bool is_attr_exist = false;

                    /* the 1st list is reloid, skip it */
                    ListCell* prev = list_head(record);
                    if (prev == NULL) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("Unexpected null value for the list record's head")));
                    }
                    lc2 = prev->next;
                    while (lc2 != NULL) {
                        List* rel_or_att = (List*)lfirst(lc2);

                        /* skip muti-col that has no statistic */
                        if (list_length(rel_or_att) > 1) {
                            prev = lc2;
                            lc2 = lnext(lc2);
                            continue;
                        }

                        /* Whether the attrid exist in rellist or not. */
                        if (attid == linitial_int(rel_or_att)) {
                            is_attr_exist = true;
                            break;
                        } else if (0 == linitial_int(rel_or_att)) {
                            /* Delete cell that 'attid = 0'  when new single-col attid will be added */
                            record = list_delete_cell(record, lc2, prev);
                            break;
                        }
                        prev = lc2;
                        lc2 = lnext(lc2);
                    }

                    if (!is_attr_exist) {
                        /* The rel exist and the attr of rel not exist, add the attr in the rellist. */
                        tmp_att_list = lappend_int(tmp_att_list, attid);
                        record = lappend(record, tmp_att_list);
                    } else {
                        break;
                    }
                }
            }
        }

        if (attid == 0 && !is_rel_exist) {
            /* The rel does not exist in g_NoAnalyzeRelNameList, add a new rellist. */
            tmp_rel_list = lappend_oid(tmp_rel_list, relid);
            tmp_record_list = lappend(tmp_record_list, tmp_rel_list);

            tmp_att_list = lappend_int(tmp_att_list, 0);
            tmp_record_list = lappend(tmp_record_list, tmp_att_list);

            t_thrd.postgres_cxt.g_NoAnalyzeRelNameList =
                lappend(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList, tmp_record_list);
        }
    }

    (void)MemoryContextSwitchTo(old_context);
}

/*
 * output_noanalyze_rellist_to_log
 *	set warning that no-analyzed relation name to log
 *
 * @in lev: on which log level should we emit the message
 */
void output_noanalyze_rellist_to_log(int lev)
{
    /*
     * We should not save the relation to non-analyze list if is under analyzing,
     * because it will create temp table and execute some query, the temp table
     * don't be analyzed when 2% analyzing.
     */
    if (u_sess->analyze_cxt.is_under_analyze) {
        return;
    }

    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    StringInfoData buf;
    StringInfoData obs_ft_buf;
    bool need_log = false;
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    if (t_thrd.postgres_cxt.g_NoAnalyzeRelNameList == NIL) {
        (void)MemoryContextSwitchTo(old_context);
        return;
    }

    initStringInfo(&buf);
    initStringInfo(&obs_ft_buf);
    foreach (lc1, t_thrd.postgres_cxt.g_NoAnalyzeRelNameList) {
        List* record = (List*)lfirst(lc1);

        Assert(list_length(record) > 1);

        Oid relid = linitial_oid((List*)linitial(record));
        Relation rel = relation_open(relid, AccessShareLock);

        if ((0 == rel->rd_rel->reltuples && check_relation_analyzed(relid)) ||
            ((RELKIND_FOREIGN_TABLE == rel->rd_rel->relkind) && isMOTFromTblOid(relid))) {
            relation_close(rel, AccessShareLock);
            continue;
        }

        /* the 1st cell is the list of rel , so skip it,  and get the att id */
        lc2 = lnext(list_head(record));

        while (lc2 != NULL) {
            List* tmp_record_list = (List*)lfirst(lc2);
            /* only single-col that has no statistics will be handle */
            if (list_length(tmp_record_list) == 1) {
                int attid = linitial_int(tmp_record_list);
                /* 'attid = 0' means the whole rel has no statistics */
                if (attid == 0) {
                    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE &&
                        isSpecifiedSrvTypeFromRelId(relid, OBS_SERVER)) {
                        if (obs_ft_buf.len > 0) {
                            appendStringInfoString(&obs_ft_buf, ", ");
                        }
                        appendStringInfo(
                            &obs_ft_buf, "%s.%s", get_namespace_name(RelationGetNamespace(rel), true), get_rel_name(relid));
                    } else {
                        if (buf.len > 0) {
                            appendStringInfoString(&buf, ", ");
                        }
                        appendStringInfo(
                            &buf, "%s.%s", get_namespace_name(RelationGetNamespace(rel), true), get_rel_name(relid));
                    }
                    break;
                }

                if (buf.len > 0) {
                    appendStringInfoString(&buf, ", ");
                }
                appendStringInfo(&buf,
                    "%s.%s.%s",
                    get_namespace_name(RelationGetNamespace(rel), true),
                    get_rel_name(relid),
                    (char*)attnumAttName(rel, attid));
            }
            lc2 = lnext(lc2);
        }

        relation_close(rel, AccessShareLock);

        need_log = true;
    }

    /* Need output log/warning if there are rel having no analyze. */
    if (need_log) {
        /* We should output warning if it is scene of cloud. */
        if (isSecurityMode && lev == LOG) {
            lev = WARNING;
        }

        if (buf.len > 0) {
            ereport(lev,
                (errmsg("Statistics in some tables or columns(%s) are not collected.", buf.data),
                    errhint("Do analyze for them in order to generate optimized plan.")));
        }
        if (obs_ft_buf.len > 0) {
            ereport(lev,
                (errmsg("Statistics in some tables(%s) are not collected.", obs_ft_buf.data),
                    errhint("Do set totalrows option for them in order to generate optimized plan.")));
        }
    }

    pfree_ext(buf.data);
    buf.data = NULL;

    /* deep free g_NoAnalyzeRelNameList */
    foreach (lc1, t_thrd.postgres_cxt.g_NoAnalyzeRelNameList) {
        List* record = (List*)lfirst(lc1);
        foreach (lc2, record) {
            List* rel_or_att = (List*)lfirst(lc2);
            list_free_ext(rel_or_att);
        }
        list_free_ext(record);
    }
    list_free_ext(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList);
    t_thrd.postgres_cxt.g_NoAnalyzeRelNameList = NULL;

    (void)MemoryContextSwitchTo(old_context);
}

/*
 * contain_single_col_stat
 * 		if stat_list contains single-col info(including the whole rel), return true, else retrun false.
 * Parameters
 *		@in stat_list: the list will be checked, with the same structure to g_NoAnalyzeRelNameList
 */
bool contain_single_col_stat(List* stat_list)
{
    if (stat_list == NIL) {
        return false;
    }

    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    foreach (lc1, stat_list) {
        List* record = (List*)lfirst(lc1);

        Assert(list_length(record) > 1);

        /* the 1st cell is the list of rel , so skip it,  and get the att id */
        lc2 = lnext(list_head(record));

        while (lc2 != NULL) {
            List* tmp_record_list = (List*)lfirst(lc2);

            if (list_length(tmp_record_list) == 1) {
                return true;
            }
            lc2 = lnext(lc2);
        }
    }
    return false;
}

/*
 * get_global_rows: set global rows from local rows
 *
 * Parameters:
 *	@in local_rows: number of local rows
 *	@in	multiple: skew multiple
 *
 * Returns: global rows
 */
double get_global_rows(double local_rows, double multiple, unsigned int num_data_nodes)
{
    if (IS_STREAM_PLAN) {
        return ((double)clamp_row_est((local_rows / Max(multiple, 1.0) * num_data_nodes)));
    } else {
        return ((double)clamp_row_est(local_rows));
    }
}

/*
 * get_local_rows: set local rows from global rows
 *
 * Parameters:
 *	@in global_rows: number of global rows
 *	@in	multiple: skew multiple
 *
 * Returns: local rows
 */
double get_local_rows(double global_rows, double multiple, bool replicate, unsigned int num_data_nodes)
{
    if (IS_STREAM_PLAN && !replicate) {
        Assert(num_data_nodes > 0);
        return ((double)clamp_row_est((global_rows / num_data_nodes) * Max(multiple, 1.0)));
    } else {
        return ((double)clamp_row_est(global_rows));
    }
}

/*
 * set_varratio_after_calc_selectivity
 *	set the var ratio after join other rel and filter by self
 *    in order to used for join with other rel and estimate distinct use possion.
 *
 * Parameters:
 *	@in vardata: var data info
 *	@in type: joinratio or filterratio
 *	@in ratio: join ratio according to estimation
 * 	@in sjinfo: the join info for current relation join with others
 */
void set_varratio_after_calc_selectivity(
    VariableStatData* var_data, RatioType type, double ratio, SpecialJoinInfo* sj_info)
{
    VarRatio* vr = NULL;
    MemoryContext cxt;
    ListCell* lc = NULL;
    Relids join_relids = NULL;
    Node* var = NULL;

    if (var_data == NULL || var_data->rel == NULL || ratio == 1.0 || ratio <= 0.0) {
        return;
    }

    /*
     * we don't need cache the ratio for the temparary of compute the selectivity
     * of inner join when compute semi and anti join factor.
     */
    if (sj_info && !sj_info->varratio_cached) {
        return;
    }

    var = var_data->var;
    if (!IsA(var_data->var, Var)) {
        List* vars = NIL;
        vars = pull_var_clause(var_data->var, PVC_REJECT_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

        /* we don't process var_data have more var. */
        if (vars == NULL || (list_length(vars) > 1)) {
            list_free_ext(vars);
            return;
        }

        var = (Node*)linitial(vars);
        list_free_ext(vars);
    }

    if (RatioType_Join == type) {
        if (sj_info == NULL) {
            return;
        }

        /* construct relids set that identifies the joinrel. */
        join_relids = bms_union(sj_info->min_lefthand, sj_info->min_righthand);
        foreach (lc, var_data->rel->varratio) {
            vr = (VarRatio*)lfirst(lc);

            if (RatioType_Filter == vr->ratiotype) {
                continue;
            }

            /* we need not save if it have the same joinrel and joinratio, otherwise, we only save the minimum
             * joinratio. */
            if (bms_equal(vr->joinrelids, join_relids) && _equalSimpleVar(var, vr->var)) {
                /* save the minimue ratio. */
                if (ratio < vr->ratio) {
                    vr->ratio = ratio;
                }

                bms_free(join_relids);
                join_relids = NULL;
                return;
            }
        }
        bms_free(join_relids);
        join_relids = NULL;
    }

    cxt = MemoryContextSwitchTo(MemoryContextOriginal((char*)var_data->rel));
    vr = (VarRatio*)palloc0(sizeof(VarRatio));
    vr->var = var;
    vr->ratio = ratio;
    vr->ratiotype = type;

    if (RatioType_Join == type) {
        vr->joinrelids = bms_union(sj_info->min_lefthand, sj_info->min_righthand);
    } else {
        vr->joinrelids = NULL;
    }

    var_data->rel->varratio = lappend(var_data->rel->varratio, vr);
    (void)MemoryContextSwitchTo(cxt);
}

/*
 * get_join_ratio
 *	acquire join ratio when joining to relation with relid
 *
 * Parameters:
 *	@in vardata: var data info
 *	@in sjinfo: the join info for current relation join with others
 *
 * Return:
 *	minimum join ratio with specific relation
 */
static double get_join_ratio(VariableStatData* var_data, SpecialJoinInfo* sj_info)
{
    double ratio = 1.0;
    ListCell* lc = NULL;
    RelOptInfo* rel = var_data->rel;

    if ((rel == NULL) || (sj_info == NULL)) {
        return 1.0;
    }

    foreach (lc, rel->varratio) {
        VarRatio* jr = (VarRatio*)lfirst(lc);

        if (RatioType_Filter == jr->ratiotype) {
            continue;
        }

        /*
         * only get the joinratio with the var.
         * example: we have joinratio(t1 inner join t2 on t1.a=t2.a), we can save the joinratio with
         * t1(t1t2, a, ratio1) and t2(t1t2, a, ratio2).
         * when we compute three table join ratio as (t1 inner join t2 on t1.a=t2.a inner join t3 on t2.b=t3.b),
         * we can use above joinratio ((t1*t2)*t3 (t2.b=t3.b)) with t2 when t1 join with t2 using t2.a.
         */
        if (VARNEQ(var_data->var, jr->var)) {
            continue;
        }

        /*
         * we can use the ratio under two condition:
         * 1. leftrelids include the cached joinrelids if local rel is left in joinrel.
         * 2. rightrelids include the cached joinrelids if local rel is right in joinrel.
         */
        if (((bms_is_subset(rel->relids, sj_info->min_lefthand) && bms_equal(jr->joinrelids, sj_info->min_lefthand)) ||
                (bms_is_subset(rel->relids, sj_info->min_righthand) &&
                    bms_equal(jr->joinrelids, sj_info->min_righthand))) &&
            jr->ratio < ratio) {
            ratio = jr->ratio;
        }
    }

    return ratio;
}

/*
 * can_use_possion
 *	decide whether we can use possion or not according to cached ratio.
 *
 * Parameters:
 *	@in vardata: the var we want to get distinct
 *	@in sjinfo: the join info for current relation join with others
 *	@in all_baserels: identify all baserels include in the local root when sjinfo is null,
 *                              we can use it to judge whether we can use possion or not
 *	@out ratio: the final selectivity include filter and join if we can use possion
 *
 * Return:
 *	whether we can use possion or not
 */
bool can_use_possion(VariableStatData* var_data, SpecialJoinInfo* sj_info, double* ratio)
{
    ListCell* lc = NULL;
    double filter_ratio = 1.0;
    double join_ratio = 1.0;
    bool use_possion = false;

    if ((var_data->rel == NULL) || (var_data->rel->varratio == NULL) || (var_data->var == NULL)) {
        return false;
    }

    /*
     * comput result ratio according to filter and join qual.
     * constrain: the column of join or filter and var_data->var art different,
     * but they are belong to the same relation.
     * thinking: we should get each ratio from varratio list according to ratio type respective
     * method: if ratio type is filter and all the ratio var meat constrain above, we should multiple all the ratio.
     * if ratio type is join ratio, we should find the ratio by get_join_ratio
     */
    foreach (lc, var_data->rel->varratio) {
        VarRatio* vr = (VarRatio*)lfirst(lc);

        if (VARNEQ(var_data->var, vr->var)) {
            if (RatioType_Filter == vr->ratiotype) {
                filter_ratio = Min(filter_ratio, vr->ratio);
                use_possion = true;
            } else {
                /*
                 * we can use the ratio under two condition for have join info:
                 * 1. leftrelids include the cached joinrelids if local rel is left in joinrel.
                 * 2. rightrelids include the cached joinrelids if local rel is right in joinrel.
                 * others, we can use the varratio if the all baserelids is equal to vr->joinrelids.
                 * for example: select t1.a from t1,t2 where t1.b=t2.b;
                 * we can estimate the distinct of t1.a using the joinratio of t1.b join with t2.b.
                 */
                if (sj_info && ((bms_is_subset(var_data->rel->relids, sj_info->min_lefthand) &&
                                   bms_equal(vr->joinrelids, sj_info->min_lefthand)) ||
                                  (bms_is_subset(var_data->rel->relids, sj_info->min_righthand) &&
                                      bms_equal(vr->joinrelids, sj_info->min_righthand)))) {
                    join_ratio = Min(join_ratio, vr->ratio);
                    use_possion = true;
                }
            }
        }
    }

    if ((unsigned int)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_CONJUNCT) {
        *ratio = Min(filter_ratio, join_ratio);
    } else {
        *ratio = filter_ratio * join_ratio;
    }

    return use_possion;
}

/*
 * @Description: Compute less or equal selectivity.
 * @in root: Per-query information for planning/optimization.
 * @in wc: window function clause.
 * @in partitionExprs: partition by expr.
 * @in constval: const value, less than or less equal parameter.
 * @in tuples: plan all tuples number.
 * @return: winfun less than constval selectivity.
 */
double get_windowagg_selectivity(PlannerInfo* root, WindowClause* wc, WindowFunc* wfunc, List* partition_exprs,
    int32 const_val, double tuples, unsigned int num_datanodes)
{
    Assert(wfunc->winfnoid == ROWNUMBERFUNCOID || wfunc->winfnoid == RANKFUNCOID);

    double selec = 0.0;

    if (const_val <= 0) {
        return 0.0;
    }

    /* If runk() partition by and order by expr is same, runk() value must be 1.*/
    if (wfunc->winfnoid == RANKFUNCOID && equal(wc->partitionClause, wc->orderClause) && const_val >= 1) {
        selec = 1.0;
    } else {
        double numdistinct = estimate_num_groups(root, partition_exprs, tuples, num_datanodes, STATS_TYPE_LOCAL);
        /* Estimate less selectivity, group num * c say select rows. */
        selec = Min(numdistinct * const_val, tuples) / tuples;
    }

    return selec;
}
