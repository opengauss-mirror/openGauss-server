/* -------------------------------------------------------------------------
 *
 * dependency.cpp
 *	  Compute functional dependencies
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/dependency/dependency.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/func_dependency.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/attoptcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).
 */
#define WIDTH_THRESHOLD 1024

/* size of the struct header fields (magic, type, ndeps) */
#define SizeOfHeader (3 * sizeof(uint32))

/* size of a serialized dependency (degree, natts, atts) */
Size SizeOfItem(int natts)
{
    return (sizeof(double) + sizeof(AttrNumber) * (1 + (natts)));
}

/*
 * Internal state for DependencyGenerator of dependencies. Dependencies are similar to
 * k-permutations of n elements, except that the order does not matter for the
 * first (k-1) elements. That is, (a,b=>c) and (b,a=>c) are equivalent.
 */
typedef struct DependencyGeneratorData {
    int k; /* size of the dependency */
    int n; /* number of possible attributes */
    int current; /* next dependency to return (index) */
    AttrNumber ndependencies; /* number of dependencies generated */
    AttrNumber *dependencies; /* array of pre-generated dependencies */
} DependencyGeneratorData;

typedef DependencyGeneratorData *DependencyGenerator;

static bool idx_match(const AttrNumber *arr, int target_idx, int idx);
static void generate_dependencies_recurse(const DependencyGenerator state, int index, AttrNumber start,
                                          AttrNumber *current);
static void generate_dependencies(const DependencyGenerator state);
static DependencyGenerator DependencyGenerator_init(int n, int k);
static void DependencyGenerator_free(DependencyGenerator state);
static AttrNumber *DependencyGenerator_next(DependencyGenerator state);
static double dependency_degree(StatsBuildData *data, int k, const AttrNumber *dependency);
static bool dependency_is_fully_matched(const MVDependency *dependency, const Bitmapset *attnums);
static bool fd_clause_processing_opclause(Node **clause, Node **clause_expr);
static bool fd_clause_processing_scalararray(Node **clause, Node **clause_expr);
static bool dependency_is_compatible_clause(Node *clause, Index relid, AttrNumber *attnum);
static MVDependency *find_strongest_dependency(MVDependencies **dependencies, int ndependencies,
                                               const Bitmapset *attnums);
static Selectivity clauselist_apply_dependencies(PlannerInfo *root, const List *clauses, int varRelid,
                                                 JoinType jointype, SpecialJoinInfo *sjinfo,
                                                 MVDependency **dependencies, int ndependencies,
                                                 const AttrNumber *list_attnums, Bitmapset **estimatedclauses);

/* Given an element, check if it is in arr and its idx is less than target_idx */
static bool idx_match(const AttrNumber *arr, int target_idx, int idx)
{
    bool match = false;
    for (int j = 0; j < target_idx; j++) {
        if (arr[j] == (AttrNumber)idx) {
            match = true;
            break;
        }
    }
    return match;
}

/* recurse function to generate functional dependency structures */
static void generate_dependencies_recurse(const DependencyGenerator state, int index, AttrNumber start,
                                          AttrNumber *current)
{
    /*
     * The generator handles the first (k-1) elements differently from the
     * last element.
     */
    if (index < (state->k - 1)) {
        AttrNumber i;

        /*
         * The first (k-1) values have to be in ascending order, which we
         * generate recursively.
         */
        for (i = start; (int)i < state->n; i++) {
            current[index] = i;
            generate_dependencies_recurse(state, (index + 1), (i + 1), current);
        }
    } else {
        int i = 0;

        /*
         * the last element is the implied value, which does not respect the
         * ascending order. We just need to check that the value is not in the
         * first (k-1) elements.
         */
        for (i = 0; i < state->n; i++) {
            bool match = false;
            current[index] = i;
            match = idx_match(current, index, i);
            /*
             * If the value is not found in the first part of the dependency,
             * we're done.
             */
            if (!match) {
                state->dependencies = (AttrNumber *)repalloc(
                    state->dependencies, state->k * (state->ndependencies + 1) * sizeof(AttrNumber));

                errno_t rc = 0;
                rc = memcpy_s(&state->dependencies[(state->k * state->ndependencies)], state->k * sizeof(AttrNumber),
                              current, state->k * sizeof(AttrNumber));
                securec_check_c(rc, "\0", "\0");

                state->ndependencies++;
            }
        }
    }
}

/* generate all dependencies (k-permutations of n elements) */
static void generate_dependencies(const DependencyGenerator state)
{
    AttrNumber *current = (AttrNumber *)palloc0(sizeof(AttrNumber) * state->k);

    generate_dependencies_recurse(state, 0, 0, current);

    pfree_ext(current);
}

/*
 * initialize the DependencyGenerator of variations, and prebuild the variations
 *
 * This pre-builds all the variations. We could also generate them in
 * DependencyGenerator_next(), but this seems simpler.
 */
static DependencyGenerator DependencyGenerator_init(int n, int k)
{
    DependencyGenerator state;

    Assert((n >= k) && (k > 0));

    /* allocate the DependencyGenerator state */
    state = (DependencyGenerator)palloc0(sizeof(DependencyGeneratorData));
    state->dependencies = (AttrNumber *)palloc0(k * sizeof(AttrNumber));

    state->ndependencies = 0;
    state->current = 0;
    state->k = k;
    state->n = n;

    /* now actually pre-generate all the variations */
    generate_dependencies(state);

    return state;
}

/* free the DependencyGenerator state */
static void DependencyGenerator_free(DependencyGenerator state)
{
    pfree_ext(state->dependencies);
    pfree_ext(state);
}

/* generate next combination */
static AttrNumber *DependencyGenerator_next(DependencyGenerator state)
{
    if (state->current == state->ndependencies) {
        return NULL;
    }

    return (&state->dependencies[state->k * state->current++]);
}

/*
 * validates functional dependency on the data
 *
 * An actual work horse of detecting functional dependencies. Given a variation
 * of k attributes, it checks that the first (k-1) are sufficient to determine
 * the last one.
 */
static double dependency_degree(StatsBuildData *data, int k, const AttrNumber *dependency)
{
    int i = 0;
    int nitems = 0;
    MultiSortSupport mss;
    SortItem *items = NULL;
    AttrNumber *attnums_dep = NULL;

    /* counters valid within a group */
    int group_size = 0;
    int n_violations = 0;

    /* total number of rows supporting (consistent with) the dependency */
    int n_supporting_rows = 0;

    /* Make sure we have at least two input attributes. */
    Assert(k > 1);

    /* sort info for all attributes columns */
    mss = multi_sort_init(k);

    /*
     * Translate the array of indexes to regular attnums for the dependency
     * (we will need this to identify the columns in StatsBuildData).
     */
    attnums_dep = (AttrNumber *)palloc(k * sizeof(AttrNumber));
    for (i = 0; i < k; i++) {
        attnums_dep[i] = data->attnums[dependency[i]];
    }

    /*
     * Verify the dependency (a,b,...)->z, using a rather simple algorithm:
     *
     * (a) sort the data lexicographically
     *
     * (b) split the data into groups by first (k-1) columns
     *
     * (c) for each group count different values in the last column
     *
     * We use the column data types' default sort operators and collations;
     * perhaps at some point it'd be worth using column-specific collations?
     */

    /* prepare the sort function for the dimensions */
    for (i = 0; i < k; i++) {
        VacAttrStats *colstat = data->stats;
        TypeCacheEntry *type;

        type = lookup_type_cache(colstat->attrtypid[dependency[i]], TYPECACHE_LT_OPR);
        /* shouldn't happen */
        if (type->lt_opr == InvalidOid) {
            ereport(ERROR, (errdetail("cache lookup failed for ordering operator for type %u", colstat->attrtypid[0])));
        }

        /* prepare the sort function for this dimension */
        multi_sort_add_dimension(mss, i, type->lt_opr, DEFAULT_COLLATION_OID);
    }

    /*
     * build an array of SortItem(s) sorted using the multi-sort support
     *
     * XXX This relies on all stats entries pointing to the same tuple
     * descriptor.  For now that assumption holds, but it might change in the
     * future for example if we support statistics on multiple tables.
     */
    items = build_sorted_items(data, &nitems, mss, k, attnums_dep);

    /*
     * Walk through the sorted array, split it into rows according to the
     * first (k-1) columns. If there's a single value in the last column, we
     * count the group as 'supporting' the functional dependency. Otherwise we
     * count it as contradicting.
     */

    /* start with the first row forming a group */
    group_size = 1;

    /* loop 1 beyond the end of the array so that we count the final group */
    for (i = 1; i <= nitems; i++) {
        /*
         * Check if the group ended, which may be either because we processed
         * all the items (i==nitems), or because the i-th item is not equal to
         * the preceding one.
         */
        if (i == nitems || multi_sort_compare_dims(0, k - FD_MIN_DIMENSIONS, &items[i - 1], &items[i], mss) != 0) {
            /*
             * If no violations were found in the group then track the rows of
             * the group as supporting the functional dependency.
             */
            if (n_violations == 0) {
                n_supporting_rows += group_size;
            }
            /* Reset counters for the new group */
            n_violations = 0;
            group_size = 1;
            continue;
        } else if (multi_sort_compare_dim(k - 1, &items[i - 1], &items[i], mss) != 0) {
            /* first columns match, but the last one does not (so contradicting) */
            n_violations++;
        }
        group_size++;
    }

    if (data->numrows < 1) {
        ereport(ERROR, (errdetail("invalid numrows %d which is expected to be >= 1)", data->numrows)));
    }

    /* Compute the 'degree of validity' as (supporting/total). */
    return (n_supporting_rows * 1.0 / data->numrows);
}

/*
 * detects functional dependencies between groups of columns
 *
 * Generates all possible subsets of columns (variations) and computes
 * the degree of validity for each one. For example when creating statistics
 * on three columns (a,b,c) there are 9 possible dependencies
 *
 *	   two columns			  three columns
 *	   -----------			  -------------
 *	   (a) -> b				  (a,b) -> c
 *	   (a) -> c				  (a,c) -> b
 *	   (b) -> a				  (b,c) -> a
 *	   (b) -> c
 *	   (c) -> a
 *	   (c) -> b
 */
MVDependencies *statext_dependencies_build(StatsBuildData *data)
{
    int i = 0;
    int k = 0;

    /* result */
    MVDependencies *dependencies = NULL;
    MemoryContext cxt;

    Assert(data->nattnums > 1);

    /* tracks memory allocated by dependency_degree calls */
    cxt = AllocSetContextCreate(CurrentMemoryContext, "dependency_degree cxt", ALLOCSET_DEFAULT_SIZES);

    /*
     * We'll try build functional dependencies starting from the smallest ones
     * covering just 2 columns, to the largest ones, covering all columns
     * included in the statistics object.  We start from the smallest ones
     * because we want to be able to skip already implied ones.
     */
    for (k = FD_MIN_DIMENSIONS; k <= data->nattnums; k++) {
        AttrNumber *dependency; /* array with k elements */

        /* prepare a DependencyGenerator of variation */
        DependencyGenerator DependencyGenerator = DependencyGenerator_init(data->nattnums, k);

        /* generate all possible variations of k values (out of n) */
        dependency = DependencyGenerator_next(DependencyGenerator);
        while (dependency) {
            double degree;
            MVDependency *d;
            MemoryContext oldcxt;

            /* release memory used by dependency degree calculation */
            oldcxt = MemoryContextSwitchTo(cxt);

            /* compute how valid the dependency seems */
            degree = dependency_degree(data, k, dependency);

            MemoryContextSwitchTo(oldcxt);
            MemoryContextReset(cxt);

            /*
             * if the dependency seems entirely invalid, don't store it
             */
            if (degree == 0.0) {
                dependency = DependencyGenerator_next(DependencyGenerator);
                continue;
            }

            d = (MVDependency *)palloc0(offsetof(MVDependency, attributes) + k * sizeof(AttrNumber));

            /* copy the dependency (and keep the indexes into stxkeys) */
            d->degree = degree;
            d->nattributes = (AttrNumber)k;
            for (i = 0; i < k; i++) {
                d->attributes[i] = data->attnums[dependency[i]];
            }

            /* initialize the list of dependencies */
            if (dependencies == NULL) {
                dependencies = (MVDependencies *)palloc0(sizeof(MVDependencies));
                dependencies->magic = STATS_DEPS_MAGIC;
                dependencies->type = STATS_DEPS_TYPE_BASIC;
                dependencies->ndeps = 0;
            }

            if (dependencies) {
                dependencies->ndeps++;
                dependencies = (MVDependencies *)repalloc(
                    dependencies, offsetof(MVDependencies, deps) + dependencies->ndeps * sizeof(MVDependency *));
                dependencies->deps[dependencies->ndeps - 1] = d;
            }

            dependency = DependencyGenerator_next(DependencyGenerator);
        }

        /*
         * we're done with variations of k elements, so free the
         * DependencyGenerator
         */
        DependencyGenerator_free(DependencyGenerator);
    }

    MemoryContextDelete(cxt);

    return dependencies;
}

/*
 * Serialize list of dependencies into a bytea value.
 */
bytea *statext_dependencies_serialize(MVDependencies *dependencies)
{
    int i = 0;
    bytea *output = NULL;
    char *tmp = NULL;
    Size len = 0;
    errno_t rc = 0;

    /* we need to store ndeps, with a number of attributes for each one */
    len = VARHDRSZ + SizeOfHeader;

    /* and also include space for the actual attribute numbers and degrees */
    for (i = 0; i < dependencies->ndeps; i++) {
        len += SizeOfItem(dependencies->deps[i]->nattributes);
    }

    output = (bytea *)palloc0(len);
    SET_VARSIZE(output, len);

    tmp = VARDATA(output);

    /* Store the base struct values (magic, type, ndeps) */
    rc = memcpy_s(tmp, sizeof(uint32), &dependencies->magic, sizeof(uint32));
    securec_check_c(rc, "\0", "\0");
    tmp += sizeof(uint32);
    rc = memcpy_s(tmp, sizeof(uint32), &dependencies->type, sizeof(uint32));
    securec_check_c(rc, "\0", "\0");
    tmp += sizeof(uint32);
    rc = memcpy_s(tmp, sizeof(int), &dependencies->ndeps, sizeof(int));
    securec_check_c(rc, "\0", "\0");
    tmp += sizeof(int);

    /* store number of attributes and attribute numbers for each dependency */
    for (i = 0; i < dependencies->ndeps; i++) {
        MVDependency *d = dependencies->deps[i];

        rc = memcpy_s(tmp, sizeof(double), &d->degree, sizeof(double));
        securec_check_c(rc, "\0", "\0");
        tmp += sizeof(double);

        rc = memcpy_s(tmp, sizeof(AttrNumber), &d->nattributes, sizeof(AttrNumber));
        securec_check_c(rc, "\0", "\0");
        tmp += sizeof(AttrNumber);

        rc = memcpy_s(tmp, sizeof(AttrNumber) * d->nattributes, d->attributes, sizeof(AttrNumber) * d->nattributes);
        securec_check_c(rc, "\0", "\0");
        tmp += sizeof(AttrNumber) * d->nattributes;

        /* protect against overflow */
        Assert(tmp <= ((char *)output + len));
    }

    /* make sure we've produced exactly the right amount of data */
    Assert(tmp == ((char *)output + len));

    return output;
}

/*
 * Reads serialized dependencies into MVDependencies structure.
 */
MVDependencies *statext_dependencies_deserialize(bytea *data)
{
    int i = 0;
    Size min_expected_size = 0;
    MVDependencies *dependencies = NULL;
    char *tmp = NULL;
    errno_t rc = 0;

    if (data == NULL) {
        return NULL;
    }

    if (VARSIZE_ANY_EXHDR(data) < SizeOfHeader) {
        ereport(ERROR, (errdetail("invalid MVDependencies size %zd (expected at least %zd)", VARSIZE_ANY_EXHDR(data),
                                  SizeOfHeader)));
    }

    /* read the MVDependencies header */
    dependencies = (MVDependencies *)palloc0(sizeof(MVDependencies));

    /* initialize pointer to the data part (skip the varlena header) */
    tmp = VARDATA_ANY(data);

    /* read the header fields and perform basic sanity checks */
    rc = memcpy_s(&dependencies->magic, sizeof(uint32), tmp, sizeof(uint32));
    securec_check_c(rc, "\0", "\0");
    tmp += sizeof(uint32);
    rc = memcpy_s(&dependencies->type, sizeof(uint32), tmp, sizeof(uint32));
    securec_check_c(rc, "\0", "\0");
    tmp += sizeof(uint32);
    rc = memcpy_s(&dependencies->ndeps, sizeof(int), tmp, sizeof(int));
    securec_check_c(rc, "\0", "\0");
    tmp += sizeof(int);

    if (dependencies->magic != STATS_DEPS_MAGIC) {
        ereport(ERROR, (errdetail("invalid dependency magic %u (expected %u)", dependencies->magic, STATS_DEPS_MAGIC)));
    }

    if (dependencies->type != STATS_DEPS_TYPE_BASIC) {
        ereport(ERROR,
                (errdetail("invalid dependency type %u (expected %d)", dependencies->type, STATS_DEPS_TYPE_BASIC)));
    }

    if (dependencies->ndeps == 0) {
        ereport(ERROR, (errdetail("invalid zero-length item array in MVDependencies")));
    }

    /* what minimum bytea size do we expect for those parameters */
    min_expected_size = SizeOfItem(dependencies->ndeps);
    if (VARSIZE_ANY_EXHDR(data) < min_expected_size) {
        ereport(ERROR, (errdetail("invalid dependencies size %zd (expected at least %zd)", VARSIZE_ANY_EXHDR(data),
                                  min_expected_size)));
    }

    /* allocate space for the dependencies */
    dependencies = (MVDependencies *)repalloc(
        dependencies, offsetof(MVDependencies, deps) + (dependencies->ndeps * sizeof(MVDependency *)));

    for (i = 0; i < dependencies->ndeps; i++) {
        double degree;
        AttrNumber k;
        MVDependency *d;

        /* degree of validity */
        rc = memcpy_s(&degree, sizeof(double), tmp, sizeof(double));
        securec_check_c(rc, "\0", "\0");
        tmp += sizeof(double);

        /* number of attributes */
        rc = memcpy_s(&k, sizeof(AttrNumber), tmp, sizeof(AttrNumber));
        securec_check_c(rc, "\0", "\0");
        tmp += sizeof(AttrNumber);

        /* is the number of attributes valid? */
        Assert((k >= FD_MIN_DIMENSIONS) && (k <= FD_MAX_DIMENSIONS));

        /* now that we know the number of attributes, allocate the dependency */
        d = (MVDependency *)palloc0(offsetof(MVDependency, attributes) + (k * sizeof(AttrNumber)));

        d->degree = degree;
        d->nattributes = k;

        /* copy attribute numbers */
        rc = memcpy_s(d->attributes, sizeof(AttrNumber) * d->nattributes, tmp, sizeof(AttrNumber) * d->nattributes);
        securec_check_c(rc, "\0", "\0");
        tmp += sizeof(AttrNumber) * d->nattributes;

        dependencies->deps[i] = d;

        /* still within the bytea */
        Assert(tmp <= ((char *)data + VARSIZE_ANY(data)));
    }

    /* we should have consumed the whole bytea exactly */
    Assert(tmp == ((char *)data + VARSIZE_ANY(data)));

    return dependencies;
}

/*
 * dependency_is_fully_matched
 *		checks that a functional dependency is fully matched given clauses on
 *		attributes (assuming the clauses are suitable equality clauses)
 */
static bool dependency_is_fully_matched(const MVDependency *dependency, const Bitmapset *attnums)
{
    int j;

    /*
     * Check that the dependency actually is fully covered by clauses. We have
     * to translate all attribute numbers, as those are referenced
     */
    for (j = 0; j < dependency->nattributes; j++) {
        int attnum = dependency->attributes[j];

        if (!bms_is_member(attnum, attnums)) {
            return false;
        }
    }

    return true;
}

/*
 * fd_clause_processing_opclause
 *	    process the clause with type opclause
 *
 *  Determines if this clause is compatible with functional dependencies
 *  If true, modify clause_expr and return true
 *  If false, do noting and return false
 */
static bool fd_clause_processing_opclause(Node **clause, Node **clause_expr)
{
    /* If it's an opclause, check for Var = Const or Const = Var. */
    OpExpr *expr = (OpExpr *)(*clause);

    /* Only expressions with two arguments are candidates. */
    if (list_length(expr->args) != FD_MIN_DIMENSIONS) {
        return false;
    }

    /* Make sure non-selected argument is a pseudoconstant. */
    if (is_pseudo_constant_clause((Node *)lsecond(expr->args))) {
        *clause_expr = (Node *)linitial(expr->args);
    } else if (is_pseudo_constant_clause((Node *)linitial(expr->args))) {
        *clause_expr = (Node *)lsecond(expr->args);
    } else {
        return false;
    }

    /*
     * If it's not an "=" operator, just ignore the clause, as it's not
     * compatible with functional dependencies.
     *
     * This uses the function for estimating selectivity, not the operator
     * directly (a bit awkward, but well ...).
     *
     * XXX this is pretty dubious; probably it'd be better to check btree
     * or hash opclass membership, so as not to be fooled by custom
     * selectivity functions, and to be more consistent with decisions
     * elsewhere in the planner.
     */
    if (get_oprrest(expr->opno) != EQSELRETURNOID) {
        return false;
    }

    return true;
}

/*
 * fd_clause_processing_scalararray
 *	    process the clause with type ScalarArrayOpExpr
 *
 *  Determines if this clause is compatible with functional dependencies
 *  If true, modify clause_expr and return true
 *  If false, do noting and return false
 */
static bool fd_clause_processing_scalararray(Node **clause, Node **clause_expr)
{
    /* If it's an scalar array operator, check for Var IN Const. */
    ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *)(*clause);

    /*
     * Reject ALL() variant, we only care about ANY/IN.
     *
     * XXX Maybe we should check if all the values are the same, and allow
     * ALL in that case? Doesn't seem very practical, though.
     */
    if (!expr->useOr)
        return false;

    /* Only expressions with two arguments are candidates. */
    if (list_length(expr->args) != FD_MIN_DIMENSIONS)
        return false;

    /*
     * We know it's always (Var IN Const), so we assume the var is the
     * first argument, and pseudoconstant is the second one.
     */
    if (!is_pseudo_constant_clause((Node *)lsecond(expr->args)))
        return false;

    *clause_expr = (Node *)linitial(expr->args);

    /*
     * If it's not an "=" operator, just ignore the clause, as it's not
     * compatible with functional dependencies. The operator is identified
     * simply by looking at which function it uses to estimate
     * selectivity. That's a bit strange, but it's what other similar
     * places do.
     */
    if (get_oprrest(expr->opno) != F_EQSEL)
        return false;

    return true;
}

/*
 * dependency_is_compatible_clause
 *		Determines if the clause is compatible with functional dependencies
 *
 * Only clauses that have the form of equality to a pseudoconstant, or can be
 * interpreted that way, are currently accepted.  Furthermore the variable
 * part of the clause must be a simple Var belonging to the specified
 * relation, whose attribute number we return in *attnum on success.
 */
static bool dependency_is_compatible_clause(Node *clause, Index relid, AttrNumber *attnum)
{
    Var *var;
    Node *clause_expr;

    if (IsA(clause, RestrictInfo)) {
        RestrictInfo *rinfo = (RestrictInfo *)clause;

        /* Pseudoconstants are not interesting (they couldn't contain a Var) */
        if (rinfo->pseudoconstant) {
            return false;
        }

        /* Clauses referencing multiple, or no, varnos are incompatible */
        if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON) {
            return false;
        }

        clause = (Node *)rinfo->clause;
    }

    if (is_opclause(clause)) {
        bool processing_opclause = fd_clause_processing_opclause(&clause, &clause_expr);
        if (!processing_opclause) {
            return processing_opclause;
        }
        /* OK to proceed with checking "var" */
    } else if (IsA(clause, ScalarArrayOpExpr)) {
        bool processing_scalararray = fd_clause_processing_scalararray(&clause, &clause_expr);
        if (!processing_scalararray) {
            return processing_scalararray;
        }
        /* OK to proceed with checking "var" */
    } else if (is_orclause(clause)) {
        BoolExpr *bool_expr = (BoolExpr *)clause;
        ListCell *lc;

        /* start with no attribute number */
        *attnum = InvalidAttrNumber;

        foreach (lc, bool_expr->args) {
            AttrNumber clause_attnum;

            /*
             * Had we found incompatible clause in the arguments, treat the
             * whole clause as incompatible.
             */
            if (!dependency_is_compatible_clause((Node *)lfirst(lc), relid, &clause_attnum)) {
                return false;
            }

            if (*attnum == InvalidAttrNumber) {
                *attnum = clause_attnum;
            }

            /* ensure all the variables are the same (same attnum) */
            if (*attnum != clause_attnum) {
                return false;
            }
        }

        /* the Var is already checked by the recursive call */
        return true;
    } else if (is_notclause(clause)) {
        /*
         * "NOT x" can be interpreted as "x = false", so get the argument and
         * proceed with seeing if it's a suitable Var.
         */
        clause_expr = (Node *)get_notclausearg((Expr *)clause);
    } else {
        /*
         * A boolean expression "x" can be interpreted as "x = true", so
         * proceed with seeing if it's a suitable Var.
         */
        clause_expr = (Node *)clause;
    }

    /*
     * We may ignore any RelabelType node above the operand.  (There won't be
     * more than one, since eval_const_expressions has been applied already.)
     */
    if (IsA(clause_expr, RelabelType)) {
        clause_expr = (Node *)((RelabelType *)clause_expr)->arg;
    }

    /* We only support plain Vars for now */
    if (!IsA(clause_expr, Var)) {
        return false;
    }

    /* OK, we know we have a Var */
    var = (Var *)clause_expr;

    /* Ensure Var is from the correct relation */
    if (var->varno != relid) {
        return false;
    }

    /* We also better ensure the Var is from the current level */
    if (var->varlevelsup != 0) {
        return false;
    }

    /* Also ignore system attributes (we don't allow stats on those) */
    if (!AttrNumberIsForUserDefinedAttr(var->varattno)) {
        return false;
    }

    *attnum = var->varattno;
    return true;
}

/*
 * find_strongest_dependency
 *		find the strongest dependency on the attributes
 *
 * When applying functional dependencies, we start with the strongest
 * dependencies. That is, we select the dependency that:
 *
 * (a) has all attributes covered by equality clauses
 *
 * (b) has the most attributes
 *
 * (c) has the highest degree of validity
 *
 * This guarantees that we eliminate the most redundant conditions first
 * (see the comment in dependencies_clauselist_selectivity).
 */
static MVDependency *find_strongest_dependency(MVDependencies **dependencies, int ndependencies,
                                               const Bitmapset *attnums)
{
    int i, j;
    MVDependency *strongest = NULL;

    /* number of attnums in clauses */
    int nattnums = bms_num_members(attnums);

    /*
     * Iterate over the MVDependency items and find the strongest one from the
     * fully-matched dependencies. We do the cheap checks first, before
     * matching it against the attnums.
     */
    for (i = 0; i < ndependencies; i++) {
        for (j = 0; j < (int)(dependencies[i]->ndeps); j++) {
            MVDependency *dependency = dependencies[i]->deps[j];

            /*
             * Skip dependencies referencing more attributes than available
             * clauses, as those can't be fully matched.
             */
            if (dependency->nattributes > nattnums) {
                continue;
            }

            /* skip dependencies on fewer attributes than the strongest. */
            if (strongest && dependency->nattributes < strongest->nattributes) {
                continue;
            }

            /* also skip weaker dependencies when attribute count matches */
            if (strongest && strongest->nattributes == dependency->nattributes &&
                strongest->degree > dependency->degree) {
                continue;
            }

            /*
             * this dependency is stronger, but we must still check that it's
             * fully matched to these attnums. We perform this check last as
             * it's slightly more expensive than the previous checks.
             */
            if (dependency_is_fully_matched(dependency, attnums)) {
                strongest = dependency; /* save new best match */
            }
        }
    }

    return strongest;
}

/*
 * clauselist_apply_dependencies
 *		Apply the specified functional dependencies to a list of clauses and
 *		return the estimated selectivity of the clauses that are compatible
 *		with any of the given dependencies.
 *
 * This will estimate all not-already-estimated clauses that are compatible
 * with functional dependencies, and which have an attribute mentioned by any
 * of the given dependencies (either as an implying or implied attribute).
 *
 * Given (lists of) clauses on attributes (a,b) and a functional dependency
 * (a=>b), the per-column selectivities P(a) and P(b) are notionally combined
 * using the formula
 *
 *		P(a,b) = f * P(a) + (1-f) * P(a) * P(b)
 *
 * where 'f' is the degree of dependency.  This reflects the fact that we
 * expect a fraction f of all rows to be consistent with the dependency
 * (a=>b), and so have a selectivity of P(a), while the remaining rows are
 * treated as independent.
 *
 * In practice, we use a slightly modified version of this formula, which uses
 * a selectivity of Min(P(a), P(b)) for the dependent rows, since the result
 * should obviously not exceed either column's individual selectivity.  I.e.,
 * we actually combine selectivities using the formula
 *
 *		P(a,b) = f * Min(P(a), P(b)) + (1-f) * P(a) * P(b)
 *
 * This can make quite a difference if the specific values matching the
 * clauses are not consistent with the functional dependency.
 */
static Selectivity clauselist_apply_dependencies(PlannerInfo *root, const List *clauses, int varRelid,
                                                 JoinType jointype, SpecialJoinInfo *sjinfo,
                                                 MVDependency **dependencies, int ndependencies,
                                                 const AttrNumber *list_attnums, Bitmapset **estimatedclauses)
{
    Bitmapset *attnums;
    int i;
    int j;
    int nattrs;
    Selectivity *attr_sel;
    int attidx;
    int listidx;
    ListCell *l;
    Selectivity s1;

    /*
     * Extract the attnums of all implying and implied attributes from all the
     * given dependencies.  Each of these attributes is expected to have at
     * least 1 not-already-estimated compatible clause that we will estimate
     * here.
     */
    attnums = NULL;
    for (i = 0; i < ndependencies; i++) {
        for (j = 0; j < dependencies[i]->nattributes; j++) {
            AttrNumber attnum = dependencies[i]->attributes[j];

            attnums = bms_add_member(attnums, attnum);
        }
    }

    /*
     * Compute per-column selectivity estimates for each of these attributes,
     * and mark all the corresponding clauses as estimated.
     */
    nattrs = bms_num_members(attnums);
    attr_sel = (Selectivity *)palloc(sizeof(Selectivity) * nattrs);

    attidx = 0;
    i = -1;
    while ((i = bms_next_member(attnums, i)) >= 0) {
        List *attr_clauses = NIL;
        Selectivity simple_sel;

        listidx = -1;
        foreach (l, clauses) {
            Node *clause = (Node *)lfirst(l);

            listidx++;
            if (list_attnums[listidx] == i) {
                attr_clauses = lappend(attr_clauses, clause);
                *estimatedclauses = bms_add_member(*estimatedclauses, listidx);
            }
        }
        simple_sel = clauselist_selectivity(root, attr_clauses, varRelid, jointype, sjinfo);

        attr_sel[attidx++] = simple_sel;
    }

    /*
     * Now combine these selectivities using the dependency information.  For
     * chains of dependencies such as a -> b -> c, the b -> c dependency will
     * come before the a -> b dependency in the array, so we traverse the
     * array backwards to ensure such chains are computed in the right order.
     *
     * As explained above, pairs of selectivities are combined using the
     * formula
     *
     * P(a,b) = f * Min(P(a), P(b)) + (1-f) * P(a) * P(b)
     *
     * to ensure that the combined selectivity is never greater than either
     * individual selectivity.
     *
     * Where multiple dependencies apply (e.g., a -> b -> c), we use
     * conditional probabilities to compute the overall result as follows:
     *
     * P(a,b,c) = P(c|a,b) * P(a,b) = P(c|a,b) * P(b|a) * P(a)
     *
     * so we replace the selectivities of all implied attributes with
     * conditional probabilities, that are conditional on all their implying
     * attributes.  The selectivities of all other non-implied attributes are
     * left as they are.
     */
    for (i = ndependencies - 1; i >= 0; i--) {
        MVDependency *dependency = dependencies[i];
        AttrNumber attnum;
        Selectivity s2;
        double f;

        /* Selectivity of all the implying attributes */
        s1 = 1.0;
        for (j = 0; j < dependency->nattributes - 1; j++) {
            attnum = dependency->attributes[j];
            attidx = bms_member_index(attnums, attnum);
            s1 *= attr_sel[attidx];
        }

        /* Original selectivity of the implied attribute */
        attnum = dependency->attributes[j];
        attidx = bms_member_index(attnums, attnum);
        s2 = attr_sel[attidx];

        /*
         * Replace s2 with the conditional probability s2 given s1, computed
         * using the formula P(b|a) = P(a,b) / P(a), which simplifies to
         *
         * P(b|a) = f * Min(P(a), P(b)) / P(a) + (1-f) * P(b)
         *
         * where P(a) = s1, the selectivity of the implying attributes, and
         * P(b) = s2, the selectivity of the implied attribute.
         */
        f = dependency->degree;

        if (s1 <= s2) {
            attr_sel[attidx] = f + (1 - f) * s2;
        } else {
            attr_sel[attidx] = f * s2 / s1 + (1 - f) * s2;
        }
    }

    /*
     * The overall selectivity of all the clauses on all these attributes is
     * then the product of all the original (non-implied) probabilities and
     * the new conditional (implied) probabilities.
     */
    s1 = 1.0;
    for (i = 0; i < nattrs; i++) {
        s1 *= attr_sel[i];
    }

    CLAMP_PROBABILITY(s1);

    if (attr_sel) {
        pfree_ext(attr_sel);
    }
    bms_free(attnums);

    return s1;
}

/*
 * dependencies_clauselist_selectivity
 *		Return the estimated selectivity of (a subset of) the given clauses
 *		using functional dependency statistics, or 1.0 if no useful functional
 *		dependency statistic exists.
 *
 * 'estimatedclauses' is an input/output argument that gets a bit set
 * corresponding to the (zero-based) list index of each clause that is included
 * in the estimated selectivity.
 *
 * Given equality clauses on attributes (a,b) we find the strongest dependency
 * between them, i.e. either (a=>b) or (b=>a). Assuming (a=>b) is the selected
 * dependency, we then combine the per-clause selectivities using the formula
 *
 *	   P(a,b) = f * P(a) + (1-f) * P(a) * P(b)
 *
 * where 'f' is the degree of the dependency.  (Actually we use a slightly
 * modified version of this formula -- see clauselist_apply_dependencies()).
 *
 * With clauses on more than two attributes, the dependencies are applied
 * recursively, starting with the widest/strongest dependencies. For example
 * P(a,b,c) is first split like this:
 *
 *	   P(a,b,c) = f * P(a,b) + (1-f) * P(a,b) * P(c)
 *
 * assuming (a,b=>c) is the strongest dependency.
 */
Selectivity dependencies_clauselist_selectivity(PlannerInfo *root, const List *clauses, int varRelid, JoinType jointype,
                                                SpecialJoinInfo *sjinfo, const RelOptInfo *rel,
                                                Bitmapset **estimatedclauses)
{
    Selectivity s1 = 1.0;
    ListCell *l;
    Bitmapset *clauses_attnums = NULL;
    AttrNumber *list_attnums = NULL;
    int listidx;
    MVDependencies **func_dependencies = NULL;
    ;
    int nfunc_dependencies;
    int total_ndeps;
    MVDependency **dependencies = NULL;
    int ndependencies;

    /*
     * Doing nothing if there is no statistics or no clauses. We must return 1.0 so the calling
     * function's selectivity is unaffected.
     */
    if (rel->statlist == NULL || clauses == NULL) {
        return 1.0;
    }

    list_attnums = (AttrNumber *)palloc(sizeof(AttrNumber) * list_length(clauses));

    /*
     * Pre-process the clauses list to extract the attnums seen in each item.
     * We need to determine if there's any clauses which will be useful for
     * dependency selectivity estimations.
     *
     * We also skip clauses that we already estimated using different types of
     * statistics (we treat them as incompatible).
     */
    listidx = 0;
    foreach (l, clauses) {
        Node *clause = (Node *)lfirst(l);
        AttrNumber attnum;

        /* ignore clause by default */
        list_attnums[listidx] = InvalidAttrNumber;

        if (!bms_is_member(listidx, *estimatedclauses) &&
            dependency_is_compatible_clause(clause, rel->relid, &attnum)) {
            Assert(!bms_is_member(attnum, clauses_attnums));
            Assert(attnum > 0 && attnum <= MaxHeapAttributeNumber);
            list_attnums[listidx] = attnum;
            clauses_attnums = bms_add_member(clauses_attnums, attnum);
        }

        listidx++;
    }

    Assert(listidx == list_length(clauses));

    /* If there's not at least two distinct attnums, then reject the whole list of clauses. */
    if (bms_membership(clauses_attnums) != BMS_MULTIPLE) {
        bms_free(clauses_attnums);
        pfree_ext(list_attnums);
        return 1.0;
    }

    /*
     * Extract all functional dependencies matching at least two parameters. We
     * can simply consider all dependencies at once, without having to search
     * for the best statistics object.
     *
     * To not waste cycles and memory, we deserialize dependencies only for
     * statistics that match at least two attributes. The array is allocated
     * with the assumption that all objects match - we could grow the array to
     * make it just the right size, but it's likely wasteful anyway thanks to
     * moving the freed chunks to freelists etc.
     */
    func_dependencies = (MVDependencies **)palloc(sizeof(MVDependencies *) * list_length(rel->statlist));
    nfunc_dependencies = 0;
    total_ndeps = 0;

    foreach (l, rel->statlist) {
        ExtendedStats *stat = (ExtendedStats *)lfirst(l);
        MVDependencies *deps = NULL;

        /* Skip if no functional dependency statistics. */
        if (stat->dependencies == NULL) {
            continue;
        }

        if (bms_num_members(stat->bms_attnum) < FD_MIN_DIMENSIONS) {
            continue;
        }

        deps = stat->dependencies;

        /*
         * It's possible we've removed all dependencies, in which case we
         * don't bother adding it to the list.
         */
        if (deps->ndeps > 0) {
            func_dependencies[nfunc_dependencies] = deps;
            total_ndeps += deps->ndeps;
            nfunc_dependencies++;
        }
    }

    /* if no matching stats could be found then we've nothing to do */
    if (nfunc_dependencies == 0) {
        pfree_ext(func_dependencies);
        bms_free(clauses_attnums);
        pfree_ext(list_attnums);
        return 1.0;
    }

    /*
     * Work out which dependencies we can apply, starting with the
     * widest/strongest ones, and proceeding to smaller/weaker ones.
     */
    dependencies = (MVDependency **)palloc(sizeof(MVDependency *) * total_ndeps);
    ndependencies = 0;

    while (true) {
        MVDependency *dependency = NULL;
        AttrNumber attnum;

        /* the widest/strongest dependency, fully matched by clauses */
        dependency = find_strongest_dependency(func_dependencies, nfunc_dependencies, clauses_attnums);
        if (!dependency) {
            break;
        }

        dependencies[ndependencies++] = dependency;

        /* Ignore dependencies using this implied attribute in later loops */
        attnum = dependency->attributes[dependency->nattributes - 1];
        clauses_attnums = bms_del_member(clauses_attnums, attnum);
    }

    /*
     * If we found applicable dependencies, use them to estimate all
     * compatible clauses on attributes that they refer to.
     */
    if (ndependencies != 0) {
        s1 = clauselist_apply_dependencies(root, clauses, varRelid, jointype, sjinfo, dependencies, ndependencies,
                                           list_attnums, estimatedclauses);
    }

    pfree_ext(dependencies);
    pfree_ext(func_dependencies);
    bms_free(clauses_attnums);
    pfree_ext(list_attnums);

    return s1;
}

/*
 * analyze_compute_dependencies
 *      Compute functional dependency statistics
 *
 * @in onerel: the relation for analyze or vacuum
 * @in slot_idx: index of statistic struct
 * @in tableName: temp table name
 * @in spec: the sample info of special attribute for compute statistic
 * @in stats: statistics for one attribute
 * @Returns: void
 */
void analyze_compute_dependencies(Relation onerel, int *slot_idx, const char *tableName,
                                  AnalyzeSampleTableSpecInfo *spec, VacAttrStats *stats)
{
    /* can't generate functional dependencies if there is no data */
    if (stats->numSamplerows < 1) {
        return;
    }

    /* can't generate functional dependencies if the attribute number is less than FD_MIN_DIMENSIONS */
    if (spec->stats->num_attrs < FD_MIN_DIMENSIONS) {
        return;
    }

    /* can't generate functional dependencies if the attribute number is greater than FD_MAX_DIMENSIONS */
    if (spec->stats->num_attrs > FD_MAX_DIMENSIONS) {
        ereport(ERROR, (errmsg("Please set enable_functional_dependency to OFF to generate extended statistics if "
                               "attribute number is greater than %d.",
                               FD_MAX_DIMENSIONS)));
        return;
    }

    /* check if there is no remaining slot to hold dependency */
    if (*slot_idx >= STATISTIC_NUM_SLOTS) {
        ereport(WARNING, (errmsg("No more extended statistics slots in table %s, so functional dependency statistics "
                                 "will no be generated.",
                                 tableName)));
        return;
    }

    MVDependencies *dependencies = NULL;
    StatsBuildData *data = NULL;
    MemoryContext old_context;
    int natts = 0; /* attribute number of the origin table */

    old_context = MemoryContextSwitchTo(spec->stats->anl_context);

    /* palloc and initialize the data to compute functional dependency statistics */
    data = (StatsBuildData *)palloc0(sizeof(StatsBuildData));
    data->numrows = stats->numSamplerows;
    data->nattnums = spec->stats->num_attrs;
    data->stats = stats;

    /* generate attribute number array */
    data->attnums = (AttrNumber *)palloc0(data->nattnums * sizeof(AttrNumber));
    for (int i = 0; i < data->nattnums; i++) {
        data->attnums[i] = (AttrNumber)(spec->stats->attrs[i]->attnum);
    }

    data->values = (Datum **)palloc(data->nattnums * sizeof(Datum *));
    data->nulls = (bool **)palloc(data->nattnums * sizeof(bool *));
    for (int i = 0; i < data->nattnums; i++) {
        data->values[i] = (Datum *)palloc(data->numrows * sizeof(Datum));
        data->nulls[i] = (bool *)palloc(data->numrows * sizeof(bool));
    }

    /* get attribute number of the origin table */
    natts = HeapTupleHeaderGetNatts(stats->rows[0]->t_data, RelationGetDescr(onerel));

    Datum values[natts];
    bool nulls[natts];
    for (int i = 0; i < data->numrows; i++) {
        /* Given a tuple, extract data into values/isnull arrays */
        heap_deform_tuple(stats->rows[i], RelationGetDescr(onerel), values, nulls);

        /* extract the target value/null from values/isnull arrays */
        for (int j = 0; j < data->nattnums; j++) {
            data->values[j][i] = values[data->attnums[j] - 1];
            data->nulls[j][i] = nulls[data->attnums[j] - 1];
        }
    }

    /* compute functional dependencies */
    dependencies = statext_dependencies_build(data);
    if (dependencies != NULL) {
        Datum *dependencies_values = NULL;
        float4 *dependencies_degrees = NULL;

        /*
         * dependencies_values is an Datum array. However, only the first element dependencies_values[0] is used for
         * maintaining the original structrure of MVDependencies. Seralization and deseraliztion functions for
         * structure MVDependencies are also defined respectively.
         */
        dependencies_values = (Datum *)palloc(sizeof(Datum));
        dependencies_values[0] = PointerGetDatum(statext_dependencies_serialize(dependencies));

        /* dependencies_degrees is alse an array and its lenght is equal to dependencies->ndeps. */
        dependencies_degrees = (float4 *)palloc(dependencies->ndeps * sizeof(float4));
        for (int i = 0; i < (int)(dependencies->ndeps); i++) {
            dependencies_degrees[i] = dependencies->deps[i]->degree;
        }

        /* save dpendencies into slots */
        spec->stats->stakind[*slot_idx] = STATISTIC_EXT_DEPENDENCIES;
        spec->stats->staop[*slot_idx] = 0;
        spec->stats->stanumbers[*slot_idx] = dependencies_degrees;
        spec->stats->numnumbers[*slot_idx] = (int)dependencies->ndeps;
        spec->stats->stavalues[*slot_idx] = dependencies_values;
        spec->stats->stanulls[*slot_idx] = NULL;
        spec->stats->numvalues[*slot_idx] = 1;

        (*slot_idx)++;
    }

    for (int i = 0; i < data->nattnums; i++) {
        if (data->values[i]) {
            pfree_ext(data->values[i]);
        }
        if (data->nulls[i]) {
            pfree_ext(data->nulls[i]);
        }
    }

    pfree_ext(data->values);
    pfree_ext(data->nulls);
    pfree_ext(data->attnums);
    pfree_ext(data);

    (void)MemoryContextSwitchTo(old_context);
}

/* initialize multi-dimensional sort */
MultiSortSupport multi_sort_init(int ndims)
{
    MultiSortSupport mss;

    Assert(ndims > 1);
    mss = (MultiSortSupport)palloc0(offsetof(MultiSortSupportData, ssup) + sizeof(SortSupportData) * ndims);
    mss->ndims = ndims;

    return mss;
}

/*
 * prepare sort support info using the given sort operator
 * at the position 'sortdim'
 */
void multi_sort_add_dimension(MultiSortSupport mss, int sortdim, Oid oper, Oid collation)
{
    SortSupport ssup = &mss->ssup[sortdim];

    ssup->ssup_cxt = CurrentMemoryContext;
    ssup->ssup_collation = collation;
    ssup->ssup_nulls_first = false;

    PrepareSortSupportFromOrderingOp(oper, ssup);
}

/* compare all the dimensions in the selected order */
int multi_sort_compare(const void *arr1, const void *arr2, void *arg)
{
    int i;
    MultiSortSupport mss = (MultiSortSupport)arg;
    SortItem *arr1_sort_item = (SortItem *)arr1;
    SortItem *arr2_sort_item = (SortItem *)arr2;

    for (i = 0; i < mss->ndims; i++) {
        int compare;

        compare = ApplySortComparator(arr1_sort_item->values[i], arr1_sort_item->isnull[i], arr2_sort_item->values[i],
                                      arr2_sort_item->isnull[i], &mss->ssup[i]);
        if (compare) {
            return compare;
        }
    }

    /* equal by default */
    return 0;
}

/* compare selected dimension */
int multi_sort_compare_dim(int dim, const SortItem *arr1, const SortItem *arr2, MultiSortSupport mss)
{
    return ApplySortComparator(arr1->values[dim], arr1->isnull[dim], arr2->values[dim], arr2->isnull[dim],
                               &mss->ssup[dim]);
}

int multi_sort_compare_dims(int start, int end, const SortItem *arr1, const SortItem *arr2, MultiSortSupport mss)
{
    int dim;

    for (dim = start; dim <= end; dim++) {
        int r = ApplySortComparator(arr1->values[dim], arr1->isnull[dim], arr2->values[dim], arr2->isnull[dim],
                                    &mss->ssup[dim]);
        if (r) {
            return r;
        }
    }

    return 0;
}

/* Given en element, find its index form an array. */
int find_index(AttrNumber attnum, const AttrNumber *attnums, int nattnums)
{
    int idx;
    for (idx = 0; idx < nattnums; idx++) {
        if (attnum == attnums[idx]) {
            break;
        }
    }
    return idx;
}

/*
 * build_sorted_items
 *		build a sorted array of SortItem with values from rows
 *
 * Note: All the memory is allocated in a single chunk, so that the caller
 * can simply pfree the return value to release all of it.
 */
SortItem *build_sorted_items(const StatsBuildData *data, int *nitems, MultiSortSupport mss, int numattrs,
                             const AttrNumber *attnums)
{
    int i;
    int j;
    int len;
    int nrows;
    int nvalues;
    SortItem *items = NULL;
    Datum *values = NULL;
    bool *isnull = NULL;
    char *ptr = NULL;
    int *typlen = NULL;

    nvalues = data->numrows * numattrs;

    /* Compute the total amount of memory we need (both items and values). */
    len = data->numrows * sizeof(SortItem) + nvalues * (sizeof(Datum) + sizeof(bool));

    /* Allocate the memory and split it into the pieces. */
    ptr = (char *)palloc0(len);

    /* items to sort */
    items = (SortItem *)ptr;
    ptr += data->numrows * sizeof(SortItem);

    /* values and null flags */
    values = (Datum *)ptr;
    ptr += nvalues * sizeof(Datum);

    isnull = (bool *)ptr;
    ptr += nvalues * sizeof(bool);

    /* make sure we consumed the whole buffer exactly */
    Assert((ptr - (char *)items) == len);

    /* fix the pointers to Datum and bool arrays */
    nrows = 0;
    for (i = 0; i < data->numrows; i++) {
        items[nrows].values = &values[nrows * numattrs];
        items[nrows].isnull = &isnull[nrows * numattrs];

        nrows++;
    }

    /* build a local cache of typlen for all attributes */
    typlen = (int *)palloc(sizeof(int) * data->nattnums);
    for (i = 0; i < data->nattnums; i++)
        typlen[i] = get_typlen(data->stats->attrtypid[i]);

    nrows = 0;
    for (i = 0; i < data->numrows; i++) {
        bool toowide = false;

        /* load the values/null flags from sample rows */
        for (j = 0; (int)j < numattrs; j++) {
            Datum value;
            bool value_isnull = false;
            int attlen = 0;
            int idx = 0;
            AttrNumber attnum = attnums[j];
            int datum_size = 0;

            /* match attnum to the pre-calculated data */
            idx = find_index(attnum, data->attnums, data->nattnums);

            Assert(idx < data->nattnums);

            value = data->values[idx][i];
            value_isnull = data->nulls[idx][i];
            attlen = typlen[idx];

            /*
             * If this is a varlena value, check if it's too wide and if yes
             * then skip the whole item. Otherwise detoast the value.
             *
             * XXX It may happen that we've already detoasted some preceding
             * values for the current item. We don't bother to cleanup those
             * on the assumption that those are small (below WIDTH_THRESHOLD)
             * and will be discarded at the end of analyze.
             */
            if ((!value_isnull) && (attlen == -1)) {
                datum_size = (int)(toast_raw_datum_size(value));
            }
            if ((!value_isnull) && (attlen == -1) && (datum_size > WIDTH_THRESHOLD)) {
                toowide = true;
                break;
            }
            if ((!value_isnull) && (attlen == -1)) {
                value = PointerGetDatum(PG_DETOAST_DATUM(value));
            }

            items[nrows].values[j] = value;
            items[nrows].isnull[j] = value_isnull;
        }

        if (toowide)
            continue;

        nrows++;
    }

    /* store the actual number of items (ignoring the too-wide ones) */
    *nitems = nrows;

    /* all items were too wide */
    if (nrows == 0) {
        /* everything is allocated as a single chunk */
        pfree_ext(items);
        return NULL;
    }

    /* do the sort, using the multi-sort */
    qsort_arg((void *)items, nrows, sizeof(SortItem), multi_sort_compare, mss);

    return items;
}