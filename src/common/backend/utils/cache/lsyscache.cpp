/* -------------------------------------------------------------------------
 *
 * lsyscache.c
 *	  Convenience routines for common queries in the system catalog cache.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/lsyscache.c
 *
 * NOTES
 *	  Eventually, the index information should go through here, too.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "access/transam.h"
#include "bootstrap/bootstrap.h"
#include "catalog/gs_obsscaninfo.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_obsscaninfo.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_range.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "catalog/pg_ts_config.h"
#ifdef PGXC
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_slice.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_workload_group.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#include "catalog/namespace.h"
#endif
#include "catalog/storage_gtt.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "storage/lmgr.h"
#include "storage/sinval.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/partitionkey.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "storage/proc.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "utils/typcache.h"
#include "optimizer/streamplan.h"
#include "pgxc/pgxc.h"
#include "utils/acl.h"
#include "streaming/planner.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/gs_package.h"


/*				---------- AMOP CACHES ----------						 */

/*
 * op_in_opfamily
 *
 *		Return t iff operator 'opno' is in operator family 'opfamily'.
 *
 * This function only considers search operators, not ordering operators.
 */
bool op_in_opfamily(Oid opno, Oid opfamily)
{
    return SearchSysCacheExists3(
        AMOPOPID, ObjectIdGetDatum(opno), CharGetDatum(AMOP_SEARCH), ObjectIdGetDatum(opfamily));
}

/*
 * get_op_opfamily_strategy
 *
 *		Get the operator's strategy number within the specified opfamily,
 *		or 0 if it's not a member of the opfamily.
 *
 * This function only considers search operators, not ordering operators.
 */
int get_op_opfamily_strategy(Oid opno, Oid opfamily)
{
    HeapTuple tp;
    Form_pg_amop amop_tup;
    int result;

    tp = SearchSysCache3(AMOPOPID, ObjectIdGetDatum(opno), CharGetDatum(AMOP_SEARCH), ObjectIdGetDatum(opfamily));
    if (!HeapTupleIsValid(tp)) {
        return 0;
    }
    amop_tup = (Form_pg_amop)GETSTRUCT(tp);
    result = amop_tup->amopstrategy;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_op_opfamily_sortfamily
 *
 *		If the operator is an ordering operator within the specified opfamily,
 *		return its amopsortfamily OID; else return InvalidOid.
 */
Oid get_op_opfamily_sortfamily(Oid opno, Oid opfamily)
{
    HeapTuple tp;
    Form_pg_amop amop_tup;
    Oid result;

    tp = SearchSysCache3(AMOPOPID, ObjectIdGetDatum(opno), CharGetDatum(AMOP_ORDER), ObjectIdGetDatum(opfamily));
    if (!HeapTupleIsValid(tp)) {
        return InvalidOid;
    }
    amop_tup = (Form_pg_amop)GETSTRUCT(tp);
    result = amop_tup->amopsortfamily;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_op_opfamily_properties
 *
 *		Get the operator's strategy number and declared input data types
 *		within the specified opfamily.
 *
 * Caller should already have verified that opno is a member of opfamily,
 * therefore we raise an error if the tuple is not found.
 */
void get_op_opfamily_properties(Oid opno, Oid opfamily, bool ordering_op, int* strategy, Oid* lefttype, Oid* righttype)
{
    HeapTuple tp;
    Form_pg_amop amop_tup;

    tp = SearchSysCache3(AMOPOPID,
        ObjectIdGetDatum(opno),
        CharGetDatum(ordering_op ? AMOP_ORDER : AMOP_SEARCH),
        ObjectIdGetDatum(opfamily));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("operator %u is not a member of opfamily %u", opno, opfamily)));
    amop_tup = (Form_pg_amop)GETSTRUCT(tp);
    *strategy = amop_tup->amopstrategy;
    *lefttype = amop_tup->amoplefttype;
    *righttype = amop_tup->amoprighttype;
    ReleaseSysCache(tp);
}

/*
 * get_opfamily_member
 *		Get the OID of the operator that implements the specified strategy
 *		with the specified datatypes for the specified opfamily.
 *
 * Returns InvalidOid if there is no pg_amop entry for the given keys.
 */
Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy)
{
    HeapTuple tp;
    Form_pg_amop amop_tup;
    Oid result;

    tp = SearchSysCache4(AMOPSTRATEGY,
        ObjectIdGetDatum(opfamily),
        ObjectIdGetDatum(lefttype),
        ObjectIdGetDatum(righttype),
        Int16GetDatum(strategy));
    if (!HeapTupleIsValid(tp)) {
        return InvalidOid;
    }
    amop_tup = (Form_pg_amop)GETSTRUCT(tp);
    result = amop_tup->amopopr;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_ordering_op_properties
 *		Given the OID of an ordering operator (a btree "<" or ">" operator),
 *		determine its opfamily, its declared input datatype, and its
 *		strategy number (BTLessStrategyNumber or BTGreaterStrategyNumber).
 *
 * Returns TRUE if successful, FALSE if no matching pg_amop entry exists.
 * (This indicates that the operator is not a valid ordering operator.)
 *
 * Note: the operator could be registered in multiple families, for example
 * if someone were to build a "reverse sort" opfamily.	This would result in
 * uncertainty as to whether "ORDER BY USING op" would default to NULLS FIRST
 * or NULLS LAST, as well as inefficient planning due to failure to match up
 * pathkeys that should be the same.  So we want a determinate result here.
 * Because of the way the syscache search works, we'll use the interpretation
 * associated with the opfamily with smallest OID, which is probably
 * determinate enough.	Since there is no longer any particularly good reason
 * to build reverse-sort opfamilies, it doesn't seem worth expending any
 * additional effort on ensuring consistency.
 */
bool get_ordering_op_properties(Oid opno, Oid* opfamily, Oid* opcintype, int16* strategy)
{
    bool result = false;
    CatCList* catlist = NULL;
    int i;
    /* ensure outputs are initialized on failure */
    *opfamily = InvalidOid;
    *opcintype = InvalidOid;
    *strategy = 0;

    /*
     * Search pg_amop to see if the target operator is registered as the "<"
     * or ">" operator of any btree opfamily.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);
        /* must be btree */
        if (!OID_IS_BTREE(aform->amopmethod)) {
            continue;
        }
        if (aform->amopstrategy == BTLessStrategyNumber || aform->amopstrategy == BTGreaterStrategyNumber) {
            /* Found it ... should have consistent input types */
            if (aform->amoplefttype == aform->amoprighttype) {
                /* Found a suitable opfamily, return info */
                *opfamily = aform->amopfamily;
                *opcintype = aform->amoplefttype;
                *strategy = aform->amopstrategy;
                result = true;
                break;
            }
        }
    }
    ReleaseSysCacheList(catlist);

    return result;
}

/*
 * get_compare_function_for_ordering_op
 *		Get the OID of the datatype-specific btree comparison function
 *		associated with an ordering operator (a "<" or ">" operator).
 *
 * *cmpfunc receives the comparison function OID.
 * *reverse is set FALSE if the operator is "<", TRUE if it's ">"
 * (indicating the comparison result must be negated before use).
 *
 * Returns TRUE if successful, FALSE if no btree function can be found.
 * (This indicates that the operator is not a valid ordering operator.)
 */
bool get_compare_function_for_ordering_op(Oid opno, Oid* cmpfunc, bool* reverse)
{
    Oid opfamily;
    Oid opcintype;
    int16 strategy;
    /* Find the operator in pg_amop */
    if (get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy)) {
        /* Found a suitable opfamily, get matching support function */
        *cmpfunc = get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC);
        if (!OidIsValid(*cmpfunc)) {
            /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("missing support function %d(%u,%u) in opfamily %u",
                        BTORDER_PROC,
                        opcintype,
                        opcintype,
                        opfamily)));
        }
        *reverse = (strategy == BTGreaterStrategyNumber);
        return true;
    }

    /* ensure outputs are set on failure */
    *cmpfunc = InvalidOid;

    *reverse = false;
    return false;
}

/*
 * get_sort_function_for_ordering_op
 *		Get the OID of the datatype-specific btree sort support function,
 *		or if there is none, the btree comparison function,
 *		associated with an ordering operator (a "<" or ">" operator).
 *
 * *sortfunc receives the support or comparison function OID.
 * *issupport is set TRUE if it's a support func, FALSE if a comparison func.
 * *reverse is set FALSE if the operator is "<", TRUE if it's ">"
 * (indicating that comparison results must be negated before use).
 *
 * Returns TRUE if successful, FALSE if no btree function can be found.
 * (This indicates that the operator is not a valid ordering operator.)
 */
bool get_sort_function_for_ordering_op(Oid opno, Oid* sortfunc, bool* issupport, bool* reverse)
{
    Oid opfamily;
    Oid opcintype;
    int16 strategy;

    /* Find the operator in pg_amop */
    if (get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy)) {
        /* Found a suitable opfamily, get matching support function */
        *sortfunc = get_opfamily_proc(opfamily, opcintype, opcintype, BTSORTSUPPORT_PROC);
        if (OidIsValid(*sortfunc)) {
            *issupport = true;
        } else {
            /* opfamily doesn't provide sort support, get comparison func */
            *sortfunc = get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC);
            if (!OidIsValid(*sortfunc)) {
                /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("missing support function %d(%u,%u) in opfamily %u",
                            BTORDER_PROC,
                            opcintype,
                            opcintype,
                            opfamily)));
            }
            *issupport = false;
        }
        *reverse = (strategy == BTGreaterStrategyNumber);
        return true;
    }

    /* ensure outputs are set on failure */
    *sortfunc = InvalidOid;
    *issupport = false;
    *reverse = false;
    return false;
}

/*
 * get_equality_op_for_ordering_op
 *		Get the OID of the datatype-specific btree equality operator
 *		associated with an ordering operator (a "<" or ">" operator).
 *
 * If "reverse" isn't NULL, also set *reverse to FALSE if the operator is "<",
 * TRUE if it's ">"
 *
 * Returns InvalidOid if no matching equality operator can be found.
 * (This indicates that the operator is not a valid ordering operator.)
 */
Oid get_equality_op_for_ordering_op(Oid opno, bool* reverse)
{
    Oid result = InvalidOid;
    Oid opfamily;
    Oid opcintype;
    int16 strategy;

    /* Find the operator in pg_amop */
    if (get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy)) {
        /* Found a suitable opfamily, get matching equality operator */
        result = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
        if (reverse != NULL) {
            *reverse = (strategy == BTGreaterStrategyNumber);
        }
    }
    return result;
}

/*
 * get_ordering_op_for_equality_op
 *		Get the OID of a datatype-specific btree ordering operator
 *		associated with an equality operator.  (If there are multiple
 *		possibilities, assume any one will do.)
 *
 * This function is used when we have to sort data before unique-ifying,
 * and don't much care which sorting op is used as long as it's compatible
 * with the intended equality operator.  Since we need a sorting operator,
 * it should be single-data-type even if the given operator is cross-type.
 * The caller specifies whether to find an op for the LHS or RHS data type.
 *
 * Returns InvalidOid if no matching ordering operator can be found.
 */
Oid get_ordering_op_for_equality_op(Oid opno, bool use_lhs_type)
{
    Oid result = InvalidOid;
    CatCList* catlist = NULL;
    int i;
    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any btree opfamily.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);
        /* must be btree */
        if (!OID_IS_BTREE(aform->amopmethod)) {
            continue;
        }
        if (aform->amopstrategy == BTEqualStrategyNumber) {
            /* Found a suitable opfamily, get matching ordering operator */
            Oid typid;
            typid = use_lhs_type ? aform->amoplefttype : aform->amoprighttype;
            result = get_opfamily_member(aform->amopfamily, typid, typid, BTLessStrategyNumber);
            if (OidIsValid(result)) {
                break;
            }
            /* failure probably shouldn't happen, but keep looking if so */
        }
    }
    ReleaseSysCacheList(catlist);
    return result;
}

/*
 * get_mergejoin_opfamilies
 *		Given a putatively mergejoinable operator, return a list of the OIDs
 *		of the btree opfamilies in which it represents equality.
 *
 * It is possible (though at present unusual) for an operator to be equality
 * in more than one opfamily, hence the result is a list.  This also lets us
 * return NIL if the operator is not found in any opfamilies.
 *
 * The planner currently uses simple equal() tests to compare the lists
 * returned by this function, which makes the list order relevant, though
 * strictly speaking it should not be.	Because of the way syscache list
 * searches are handled, in normal operation the result will be sorted by OID
 * so everything works fine.  If running with system index usage disabled,
 * the result ordering is unspecified and hence the planner might fail to
 * recognize optimization opportunities ... but that's hardly a scenario in
 * which performance is good anyway, so there's no point in expending code
 * or cycles here to guarantee the ordering in that case.
 */
List* get_mergejoin_opfamilies(Oid opno)
{
    List* result = NIL;
    CatCList* catlist = NULL;
    int i;
    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any btree opfamily.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);
        /* must be btree equality */
        if (OID_IS_BTREE(aform->amopmethod) && aform->amopstrategy == BTEqualStrategyNumber) {
            result = lappend_oid(result, aform->amopfamily);
        }
    }
    ReleaseSysCacheList(catlist);
    return result;
}

/*
 * get_compatible_hash_operators
 *		Get the OID(s) of hash equality operator(s) compatible with the given
 *		operator, but operating on its LHS and/or RHS datatype.
 *
 * An operator for the LHS type is sought and returned into *lhs_opno if
 * lhs_opno isn't NULL.  Similarly, an operator for the RHS type is sought
 * and returned into *rhs_opno if rhs_opno isn't NULL.
 *
 * If the given operator is not cross-type, the results should be the same
 * operator, but in cross-type situations they will be different.
 *
 * Returns true if able to find the requested operator(s), false if not.
 * (This indicates that the operator should not have been marked oprcanhash.)
 */
bool get_compatible_hash_operators(Oid opno, Oid* lhs_opno, Oid* rhs_opno)
{
    bool result = false;
    CatCList* catlist = NULL;
    int i;
    /* Ensure output args are initialized on failure */
    if (lhs_opno != NULL) {
        *lhs_opno = InvalidOid;
    }
    if (rhs_opno != NULL) {
        *rhs_opno = InvalidOid;
    }
    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any hash opfamily.  If the operator is registered in
     * multiple opfamilies, assume we can use any one.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);
        if (aform->amopmethod == HASH_AM_OID && aform->amopstrategy == HTEqualStrategyNumber) {
            /* No extra lookup needed if given operator is single-type */
            if (aform->amoplefttype == aform->amoprighttype) {
                if (lhs_opno != NULL) {
                    *lhs_opno = opno;
                }
                if (rhs_opno != NULL) {
                    *rhs_opno = opno;
                }
                result = true;
                break;
            }
            /*
             * Get the matching single-type operator(s).  Failure probably
             * shouldn't happen --- it implies a bogus opfamily --- but
             * continue looking if so.
             */
            if (lhs_opno != NULL) {
                *lhs_opno = get_opfamily_member(
                    aform->amopfamily, aform->amoplefttype, aform->amoplefttype, HTEqualStrategyNumber);
                if (!OidIsValid(*lhs_opno)) {
                    continue;
                }
                /* Matching LHS found, done if caller doesn't want RHS */
                if (rhs_opno == NULL) {
                    result = true;
                    break;
                }
            }
            if (rhs_opno != NULL) {
                *rhs_opno = get_opfamily_member(
                    aform->amopfamily, aform->amoprighttype, aform->amoprighttype, HTEqualStrategyNumber);
                if (!OidIsValid(*rhs_opno)) {
                    /* Forget any LHS operator from this opfamily */
                    if (lhs_opno != NULL)
                        *lhs_opno = InvalidOid;
                    continue;
                }
                /* Matching RHS found, so done */
                result = true;
                break;
            }
        }
    }
    ReleaseSysCacheList(catlist);
    return result;
}

/*
 * get_op_hash_functions
 *		Get the OID(s) of hash support function(s) compatible with the given
 *		operator, operating on its LHS and/or RHS datatype as required.
 *
 * A function for the LHS type is sought and returned into *lhs_procno if
 * lhs_procno isn't NULL.  Similarly, a function for the RHS type is sought
 * and returned into *rhs_procno if rhs_procno isn't NULL.
 *
 * If the given operator is not cross-type, the results should be the same
 * function, but in cross-type situations they will be different.
 *
 * Returns true if able to find the requested function(s), false if not.
 * (This indicates that the operator should not have been marked oprcanhash.)
 */
bool get_op_hash_functions(Oid opno, RegProcedure* lhs_procno, RegProcedure* rhs_procno)
{
    bool result = false;
    CatCList* catlist = NULL;
    int i;
    /* Ensure output args are initialized on failure */
    if (lhs_procno != NULL) {
        *lhs_procno = InvalidOid;
    }
    if (rhs_procno != NULL) {
        *rhs_procno = InvalidOid;
    }
    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any hash opfamily.  If the operator is registered in
     * multiple opfamilies, assume we can use any one.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);

        if (aform->amopmethod == HASH_AM_OID && aform->amopstrategy == HTEqualStrategyNumber) {
            /*
             * Get the matching support function(s).  Failure probably
             * shouldn't happen --- it implies a bogus opfamily --- but
             * continue looking if so.
             */
            if (lhs_procno != NULL) {
                *lhs_procno = get_opfamily_proc(aform->amopfamily, aform->amoplefttype, aform->amoplefttype, HASHPROC);
                if (!OidIsValid(*lhs_procno)) {
                    continue;
                }
                /* Matching LHS found, done if caller doesn't want RHS */
                if (rhs_procno == NULL) {
                    result = true;
                    break;
                }
                /* Only one lookup needed if given operator is single-type */
                if (aform->amoplefttype == aform->amoprighttype) {
                    *rhs_procno = *lhs_procno;
                    result = true;
                    break;
                }
            }
            if (rhs_procno != NULL) {
                *rhs_procno =
                    get_opfamily_proc(aform->amopfamily, aform->amoprighttype, aform->amoprighttype, HASHPROC);
                if (!OidIsValid(*rhs_procno)) {
                    /* Forget any LHS function from this opfamily */
                    if (lhs_procno != NULL) {
                        *lhs_procno = InvalidOid;
                    }
                    continue;
                }
                /* Matching RHS found, so done */
                result = true;
                break;
            }
        }
    }
    ReleaseSysCacheList(catlist);
    return result;
}

/*
 * get_op_btree_interpretation
 *		Given an operator's OID, find out which btree opfamilies it belongs to,
 *		and what properties it has within each one.  The results are returned
 *		as a palloc'd list of OpBtreeInterpretation structs.
 *
 * In addition to the normal btree operators, we consider a <> operator to be
 * a "member" of an opfamily if its negator is an equality operator of the
 * opfamily.  ROWCOMPARE_NE is returned as the strategy number for this case.
 */
List* get_op_btree_interpretation(Oid opno)
{
    List* result = NIL;
    OpBtreeInterpretation* thisresult = NULL;
    CatCList* catlist = NULL;
    int i;
    /*
     * Find all the pg_amop entries containing the operator.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple op_tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop op_form = (Form_pg_amop)GETSTRUCT(op_tuple);
        StrategyNumber op_strategy;
        /* must be btree */
        if (!OID_IS_BTREE(op_form->amopmethod)) {
            continue;
        }
        /* Get the operator's btree strategy number */
        op_strategy = (StrategyNumber)op_form->amopstrategy;
        Assert(op_strategy >= 1 && op_strategy <= 5);
        thisresult = (OpBtreeInterpretation*)palloc(sizeof(OpBtreeInterpretation));
        thisresult->opfamily_id = op_form->amopfamily;
        thisresult->strategy = op_strategy;
        thisresult->oplefttype = op_form->amoplefttype;
        thisresult->oprighttype = op_form->amoprighttype;
        result = lappend(result, thisresult);
    }
    ReleaseSysCacheList(catlist);
    /*
     * If we didn't find any btree opfamily containing the operator, perhaps
     * it is a <> operator.  See if it has a negator that is in an opfamily.
     */
    if (result == NIL) {
        Oid op_negator = get_negator(opno);
        if (OidIsValid(op_negator)) {
            catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(op_negator));
            for (i = 0; i < catlist->n_members; i++) {
                HeapTuple op_tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
                Form_pg_amop op_form = (Form_pg_amop)GETSTRUCT(op_tuple);
                StrategyNumber op_strategy;
                /* must be btree */
                if (!OID_IS_BTREE(op_form->amopmethod)) {
                    continue;
                }
                /* Get the operator's btree strategy number */
                op_strategy = (StrategyNumber)op_form->amopstrategy;
                Assert(op_strategy >= 1 && op_strategy <= 5);
                /* Only consider negators that are = */
                if (op_strategy != BTEqualStrategyNumber) {
                    continue;
                }
                /* OK, report it with "strategy" ROWCOMPARE_NE */
                thisresult = (OpBtreeInterpretation*)palloc(sizeof(OpBtreeInterpretation));
                thisresult->opfamily_id = op_form->amopfamily;
                thisresult->strategy = ROWCOMPARE_NE;
                thisresult->oplefttype = op_form->amoplefttype;
                thisresult->oprighttype = op_form->amoprighttype;
                result = lappend(result, thisresult);
            }
            ReleaseSysCacheList(catlist);
        }
    }
    return result;
}

/*
 * equality_ops_are_compatible
 *		Return TRUE if the two given equality operators have compatible
 *		semantics.
 *
 * This is trivially true if they are the same operator.  Otherwise,
 * we look to see if they can be found in the same btree or hash opfamily.
 * Either finding allows us to assume that they have compatible notions
 * of equality.  (The reason we need to do these pushups is that one might
 * be a cross-type operator; for instance int24eq vs int4eq.)
 */
bool equality_ops_are_compatible(Oid opno1, Oid opno2)
{
    bool result = false;
    CatCList* catlist = NULL;
    int i;
    /* Easy if they're the same operator */
    if (opno1 == opno2) {
        return true;
    }
    /*
     * We search through all the pg_amop entries for opno1.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno1));
    result = false;
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple op_tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop op_form = (Form_pg_amop)GETSTRUCT(op_tuple);
        /* must be btree or hash */
        if (OID_IS_BTREE(op_form->amopmethod) || op_form->amopmethod == HASH_AM_OID) {
            if (op_in_opfamily(opno2, op_form->amopfamily)) {
                result = true;
                break;
            }
        }
    }
    ReleaseSysCacheList(catlist);
    return result;
}

/*
 * get_opfamily_proc
 *		Get the OID of the specified support function
 *		for the specified opfamily and datatypes.
 *
 * Returns InvalidOid if there is no pg_amproc entry for the given keys.
 */
Oid get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16 procnum)
{
    HeapTuple tp;
    Form_pg_amproc amproc_tup;
    RegProcedure result;
    tp = SearchSysCache4(AMPROCNUM,
        ObjectIdGetDatum(opfamily),
        ObjectIdGetDatum(lefttype),
        ObjectIdGetDatum(righttype),
        Int16GetDatum(procnum));
    if (!HeapTupleIsValid(tp)) {
        return InvalidOid;
    }
    amproc_tup = (Form_pg_amproc)GETSTRUCT(tp);
    result = amproc_tup->amproc;
    ReleaseSysCache(tp);
    return result;
}

/*				---------- ATTRIBUTE CACHES ----------					 */

/*
 * get_attname
 *		Given the relation id and the attribute number,
 *		return the "attname" field from the attribute relation.
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such attribute.
 */
char* get_attname(Oid relid, AttrNumber attnum)
{
    HeapTuple tp;
    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp)) {
        Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
        char* result = NULL;
        result = pstrdup(NameStr(att_tup->attname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return NULL;
    }
}

/*
 * get_kvtype
 *      Given the relation id and the attribute number,
 *      return the field type(ATT_KV_UNDEFINED/ATT_KV_TAG/ATT_KV_FIELD/ATT_KV_TIME/ATT_KV_HIDE)
 *      from the attribute relation.
 *
 * Note: -1 if no such attribute.
 */
int get_kvtype(Oid relid, AttrNumber attnum)
{
    HeapTuple tp;
    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp)) {
        Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
        int kvtype = att_tup->attkvtype;
        ReleaseSysCache(tp);
        return kvtype;
    } else {
        return -1;
    }
}

/*
 * get_relid_attribute_name
 *
 * Same as above routine get_attname(), except that error
 * is handled by elog() instead of returning NULL.
 */
char* get_relid_attribute_name(Oid relid, AttrNumber attnum)
{
    char* attname = NULL;

    attname = get_attname(relid, attnum);
    if (attname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for attribute %d of relation %u", attnum, relid)));
    }
    return attname;
}

/*
 * get_attnum
 *
 *		Given the relation id and the attribute name,
 *		return the "attnum" field from the attribute relation.
 *
 *		Returns InvalidAttrNumber if the attr doesn't exist (or is dropped).
 */
AttrNumber get_attnum(Oid relid, const char* attname)
{
    HeapTuple tp;
    tp = SearchSysCacheAttName(relid, attname);
    if (HeapTupleIsValid(tp)) {
        Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
        AttrNumber result;
        result = att_tup->attnum;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidAttrNumber;
    }
}

/*
 * GetGenerated
 *
 *        Given the relation id and the attribute number,
 *        return the "generated" field from the attrdef relation.
 *
 *        Errors if not found.
 *
 *        Since not generated is represented by '\0', this can also be used as a
 *        Boolean test.
 */
char GetGenerated(Oid relid, AttrNumber attnum)
{
    char        result = '\0';
    Relation    relation;
    TupleDesc   tupdesc;

    /* Assume we already have adequate lock */
    relation = heap_open(relid, NoLock);

    tupdesc = RelationGetDescr(relation);
    result = GetGeneratedCol(tupdesc, attnum - 1);

    heap_close(relation, NoLock);

    return result;
}

/*
 * get_atttype
 *
 *		Given the relation OID and the attribute number with the relation,
 *		return the attribute type OID.
 */
Oid get_atttype(Oid relid, AttrNumber attnum)
{
    HeapTuple tp;
    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp)) {
        Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
        Oid result;
        result = att_tup->atttypid;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_atttypmod
 *
 *		Given the relation id and the attribute number,
 *		return the "atttypmod" field from the attribute relation.
 */
int32 get_atttypmod(Oid relid, AttrNumber attnum)
{
    HeapTuple tp;
    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp)) {
        Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
        int32 result;
        result = att_tup->atttypmod;
        ReleaseSysCache(tp);
        return result;
    } else {
        return -1;
    }
}

/*
 * get_atttypetypmodcoll
 *
 *		A three-fer: given the relation id and the attribute number,
 *		fetch atttypid, atttypmod, and attcollation in a single cache lookup.
 *
 * Unlike the otherwise-similar get_atttype/get_atttypmod, this routine
 * raises an error if it can't obtain the information.
 */
void get_atttypetypmodcoll(Oid relid, AttrNumber attnum, Oid* typid, int32* typmod, Oid* collid)
{
    HeapTuple tp;
    Form_pg_attribute att_tup;
    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for attribute %d of relation %u", attnum, relid)));
    }
    att_tup = (Form_pg_attribute)GETSTRUCT(tp);
    *typid = att_tup->atttypid;
    *typmod = att_tup->atttypmod;
    *collid = att_tup->attcollation;
    ReleaseSysCache(tp);
}

/*
 * get_collation_name
 *		Returns the name of a given pg_collation entry.
 *
 * Returns a palloc'd copy of the string, or NULL if no such constraint.
 *
 * NOTE: since collation name is not unique, be wary of code that uses this
 * for anything except preparing error messages.
 */
char* get_collation_name(Oid colloid)
{
    HeapTuple tp;
    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(colloid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_collation colltup = (Form_pg_collation)GETSTRUCT(tp);
        char* result = NULL;
        result = pstrdup(NameStr(colltup->collname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return NULL;
    }
}

/*
 * get_constraint_name
 *		Returns the name of a given pg_constraint entry.
 *
 * Returns a palloc'd copy of the string, or NULL if no such constraint.
 *
 * NOTE: since constraint name is not unique, be wary of code that uses this
 * for anything except preparing error messages.
 */
char* get_constraint_name(Oid conoid)
{
    HeapTuple tp;

    tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_constraint contup = (Form_pg_constraint)GETSTRUCT(tp);
        char* result = NULL;

        result = pstrdup(NameStr(contup->conname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return NULL;
    }
}

/*
 * get_opclass_family
 *
 *		Returns the OID of the operator family the opclass belongs to.
 */
Oid get_opclass_family(Oid opclass)
{
    HeapTuple tp;
    Form_pg_opclass cla_tup;
    Oid result;
    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for opclass %u", opclass)));
    }
    cla_tup = (Form_pg_opclass)GETSTRUCT(tp);
    result = cla_tup->opcfamily;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_opclass_input_type
 *
 *		Returns the OID of the datatype the opclass indexes.
 */
Oid get_opclass_input_type(Oid opclass)
{
    HeapTuple tp;
    Form_pg_opclass cla_tup;
    Oid result;
    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for opclass %u", opclass)));
    }
    cla_tup = (Form_pg_opclass)GETSTRUCT(tp);
    result = cla_tup->opcintype;
    ReleaseSysCache(tp);
    return result;
}

/*				---------- OPERATOR CACHE ----------					 */

/*
 * get_opcode
 *
 *		Returns the regproc id of the routine used to implement an
 *		operator given the operator oid.
 */
RegProcedure get_opcode(Oid opno)
{
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        RegProcedure result;
        result = optup->oprcode;
        ReleaseSysCache(tp);
        return result;
    } else {
        return (RegProcedure)InvalidOid;
    }
}

/*
 * get_opname
 *	  returns the name of the operator with the given opno
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such operator.
 */
char* get_opname(Oid opno)
{
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        char* result = NULL;
        result = pstrdup(NameStr(optup->oprname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return NULL;
    }
}

/*
 * op_input_types
 *
 *		Returns the left and right input datatypes for an operator
 *		(InvalidOid if not relevant).
 */
void op_input_types(Oid opno, Oid* lefttype, Oid* righttype)
{
    HeapTuple tp;
    Form_pg_operator optup;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (!HeapTupleIsValid(tp)) {
        /* shouldn't happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", opno)));
    }
    optup = (Form_pg_operator)GETSTRUCT(tp);
    *lefttype = optup->oprleft;
    *righttype = optup->oprright;
    ReleaseSysCache(tp);
}

/*
 * op_mergejoinable
 *
 * Returns true if the operator is potentially mergejoinable.  (The planner
 * will fail to find any mergejoin plans unless there are suitable btree
 * opfamily entries for this operator and associated sortops.  The pg_operator
 * flag is just a hint to tell the planner whether to bother looking.)
 *
 * In some cases (currently only array_eq and record_eq), mergejoinability
 * depends on the specific input data type the operator is invoked for, so
 * that must be passed as well. We currently assume that only one input's type
 * is needed to check this --- by convention, pass the left input's data type.
 */
bool op_mergejoinable(Oid opno, Oid inputtype)
{
    bool result = false;
    HeapTuple tp;
    TypeCacheEntry* typentry = NULL;
    /*
     * For array_eq or record_eq, we can sort if the element or field types
     * are all sortable.  We could implement all the checks for that here, but
     * the typcache already does that and caches the results too, so let's
     * rely on the typcache.
     */
    if (opno == ARRAY_EQ_OP) {
        typentry = lookup_type_cache(inputtype, TYPECACHE_CMP_PROC);
        if (typentry->cmp_proc == F_BTARRAYCMP) {
            result = true;
        }
    } else if (opno == RECORD_EQ_OP) {
        typentry = lookup_type_cache(inputtype, TYPECACHE_CMP_PROC);
        if (typentry->cmp_proc == F_BTRECORDCMP) {
            result = true;
        }
    } else {
        /* For all other operators, rely on pg_operator.oprcanmerge */
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if (HeapTupleIsValid(tp)) {
            Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
            result = optup->oprcanmerge;
            ReleaseSysCache(tp);
        }
    }
    return result;
}

/*
 * op_hashjoinable
 *
 * Returns true if the operator is hashjoinable.  (There must be a suitable
 * hash opfamily entry for this operator if it is so marked.)
 *
 * In some cases (currently only array_eq), hashjoinability depends on the
 * specific input data type the operator is invoked for, so that must be
 * passed as well.	We currently assume that only one input's type is needed
 * to check this --- by convention, pass the left input's data type.
 */
bool op_hashjoinable(Oid opno, Oid inputtype)
{
    bool result = false;
    HeapTuple tp;
    TypeCacheEntry* typentry = NULL;
    /* As in op_mergejoinable, let the typcache handle the hard cases */
    /* Eventually we'll need a similar case for record_eq ... */
    if (opno == ARRAY_EQ_OP) {
        typentry = lookup_type_cache(inputtype, TYPECACHE_HASH_PROC);
        if (typentry->hash_proc == F_HASH_ARRAY) {
            result = true;
        }
    } else {
        /* For all other operators, rely on pg_operator.oprcanhash */
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if (HeapTupleIsValid(tp)) {
            Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
            result = optup->oprcanhash;
            ReleaseSysCache(tp);
        }
    }
    return result;
}

/*
 * op_strict
 *
 * Get the proisstrict flag for the operator's underlying function.
 */
bool op_strict(Oid opno)
{
    RegProcedure funcid = get_opcode(opno);
    if (funcid == (RegProcedure)InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("operator %u does not exist", opno)));
    }
    return func_strict((Oid)funcid);
}

/*
 * op_volatile
 *
 * Get the provolatile flag for the operator's underlying function.
 */
char op_volatile(Oid opno)
{
    RegProcedure funcid = get_opcode(opno);
    if (funcid == (RegProcedure)InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("operator %u does not exist", opno)));
    }
    return func_volatile((Oid)funcid);
}

/*
 * get_commutator
 *
 *		Returns the corresponding commutator of an operator.
 */
Oid get_commutator(Oid opno)
{
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        Oid result;
        result = optup->oprcom;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_negator
 *
 *		Returns the corresponding negator of an operator.
 */
Oid get_negator(Oid opno)
{
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        Oid result;
        result = optup->oprnegate;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_equal
 *
 *		Returns the corresponding equal of an operator.
 */
Oid get_equal(Oid opno)
{
    if (opno < FirstNormalObjectId) {
        HeapTuple tp;
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if (HeapTupleIsValid(tp)) {
            Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
            Oid result, operatorNamespace, leftObjectId, rightObjectId;
            bool defined = false;
            operatorNamespace = optup->oprnamespace;
            leftObjectId = optup->oprleft;
            rightObjectId = optup->oprright;
            ReleaseSysCache(tp);
            result = OperatorGet("=", (Oid)operatorNamespace, (Oid)leftObjectId, (Oid)rightObjectId, &defined);
            if (defined) {
                return result;
            }
        }
    }
    return InvalidOid;
}

/*
 * get_oprrest
 *
 *		Returns procedure id for computing selectivity of an operator.
 */
RegProcedure get_oprrest(Oid opno)
{
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        RegProcedure result;
        result = optup->oprrest;
        ReleaseSysCache(tp);
        return result;
    } else {
        return (RegProcedure)InvalidOid;
    }
}

/*
 * get_oprjoin
 *
 *		Returns procedure id for computing selectivity of a join.
 */
RegProcedure get_oprjoin(Oid opno)
{
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        RegProcedure result;
        result = optup->oprjoin;
        ReleaseSysCache(tp);
        return result;
    } else {
        return (RegProcedure)InvalidOid;
    }
}

/*
 * get_func_name
 *	  returns the name of the function with the given funcid
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such function.
 */
char* get_func_name(Oid funcid)
{
    HeapTuple tp;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_proc functup = (Form_pg_proc)GETSTRUCT(tp);
        char* result = NULL;
        result = pstrdup(NameStr(functup->proname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return NULL;
    }
}

/*
 * get_func_namespace
 *
 *		Returns the pg_namespace OID associated with a given function.
 */
Oid get_func_namespace(Oid funcid)
{
    HeapTuple tp;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_proc functup = (Form_pg_proc)GETSTRUCT(tp);
        Oid result;
        result = functup->pronamespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_func_rettype
 *		Given procedure id, return the function's result type.
 */
Oid get_func_rettype(Oid funcid)
{
    HeapTuple tp;
    Oid result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->prorettype;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_nargs
 *		Given procedure id, return the number of arguments.
 */
int get_func_nargs(Oid funcid)
{
    HeapTuple tp;
    int result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->pronargs;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_signature
 *		Given procedure id, return the function's argument and result types.
 *		(The return value is the result type.)
 *
 * The arguments are returned as a palloc'd array.
 */
Oid get_func_signature(Oid funcid, Oid** argtypes, int* nargs)
{
    HeapTuple tp;
    Form_pg_proc procstruct;
    Oid result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    procstruct = (Form_pg_proc)GETSTRUCT(tp);
    result = procstruct->prorettype;
    *nargs = (int)procstruct->pronargs;
    oidvector* proargs = ProcedureGetArgTypes(tp);
    Assert(*nargs == proargs->dim1);
    *argtypes = (Oid*)palloc(*nargs * sizeof(Oid));
    if (*nargs > 0) {
        int rc = memcpy_s(*argtypes, *nargs * sizeof(Oid), proargs->values, *nargs * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_variadictype
 *		Given procedure id, return the function's provariadic field.
 */
Oid get_func_variadictype(Oid funcid)
{
    HeapTuple tp;
    Oid result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->provariadic;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_retset
 *		Given procedure id, return the function's proretset flag.
 */
bool get_func_retset(Oid funcid)
{
    HeapTuple tp;
    bool result = false;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->proretset;
    ReleaseSysCache(tp);
    return result;
}

/*
 * func_strict
 *		Given procedure id, return the function's proisstrict flag.
 */
bool func_strict(Oid funcid)
{
    HeapTuple tp;
    bool result = false;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->proisstrict;
    ReleaseSysCache(tp);
    return result;
}

/*
 * func_volatile
 *		Given procedure id, return the function's provolatile flag.
 */
char func_volatile(Oid funcid)
{
    HeapTuple tp;
    char result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->provolatile;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_langname
 *		Given procedure id, return the function's language.
 *      Caller is responsible to free the returned string.
 */
char* get_func_langname(Oid funcid)
{
    HeapTuple tp;
    Oid langoid;
    bool isNull = true;
    char* result = NULL;
    /* get language oid */
    Relation relation = heap_open(ProcedureRelationId, NoLock);
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    Datum datum = heap_getattr(tp, Anum_pg_proc_prolang, RelationGetDescr(relation), &isNull);
    langoid = DatumGetObjectId(datum);
    heap_close(relation, NoLock);
    ReleaseSysCache(tp);

    /* get language name */
    relation = heap_open(LanguageRelationId, NoLock);
    tp = SearchSysCache1(LANGOID, ObjectIdGetDatum(langoid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for language %u", langoid)));
    }
    datum = heap_getattr(tp, Anum_pg_language_lanname, RelationGetDescr(relation), &isNull);
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for language name %u", langoid)));
    }
    result = pstrdup(NameStr(*DatumGetName(datum)));
    heap_close(relation, NoLock);
    ReleaseSysCache(tp);

    return result;
}

/*
 * get_func_proshippable
 *		Given procedure id, return the function's proshippable flag.
 */
bool get_func_proshippable(Oid funcid)
{
    HeapTuple tp;
    bool result = false;
    bool isNull = true;
    Relation relation = heap_open(ProcedureRelationId, NoLock);
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    Datum datum = heap_getattr(tp, Anum_pg_proc_shippable, RelationGetDescr(relation), &isNull);
    result = DatumGetBool(datum);
    heap_close(relation, NoLock);
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_leakproof
 *	   Given procedure id, return the function's leakproof field.
 */
bool get_func_leakproof(Oid funcid)
{
    HeapTuple tp;
    bool result = false;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->proleakproof;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_cost
 *		Given procedure id, return the function's procost field.
 */
float4 get_func_cost(Oid funcid)
{
    HeapTuple tp;
    float4 result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->procost;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_rows
 *		Given procedure id, return the function's prorows field.
 */
float4 get_func_rows(Oid funcid)
{
    HeapTuple tp;
    float4 result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->prorows;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_lang
 *		Given procedure id, return the function's prolang field.
 */
Oid get_func_lang(Oid funcid)
{
    HeapTuple tp = NULL;
    Oid result;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    result = ((Form_pg_proc)GETSTRUCT(tp))->prolang;
    ReleaseSysCache(tp);
    return result;
}

/*
 * get_func_iswindow
 *		Given procedure id, return the function is window or not.
 */
bool get_func_iswindow(Oid funcid)
{
    HeapTuple tp = NULL;
    bool result = false;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
}
    result = ((Form_pg_proc)GETSTRUCT(tp))->proiswindow;
    ReleaseSysCache(tp);
    return result;
}

char get_func_prokind(const Oid funcid)
{
    HeapTuple tp = NULL;
    char result;
    bool isnull = false;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }
    Datum prokindDatum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prokind, &isnull);
    if (isnull) {
        result = PROKIND_FUNCTION; /* Null prokind items are created when there is no procedure */
    } else {
        result = DatumGetChar(prokindDatum);
    }
    ReleaseSysCache(tp);
    return result;
}

/*				---------- RELATION CACHE ----------					 */

/*
 * get_relname_relid
 *		Given name and namespace of a relation, look up the OID.
 *
 * Returns InvalidOid if there is no such relation.
 */
Oid get_relname_relid(const char* relname, Oid relnamespace)
{
    return GetSysCacheOid2(RELNAMENSP, PointerGetDatum(relname), ObjectIdGetDatum(relnamespace));
}

/* sucks, we don't bother add a header file for it */

/*
 * get_relname_relid_extend
 *		Given name and namespace of a relation, look up the OID and record the details info.
 *
 * Store OID if relation found in search path, else InvalidOid.
 * Returns the details info of supported cases checking.
 */
char* get_relname_relid_extend(
    const char* relname, Oid relnamespace, Oid* relOid, bool isSupportSynonym, Oid* refSynOid)
{
    Oid relid = InvalidOid;
    RangeVar* objVar = NULL;
    Oid objNamespaceOid = InvalidOid;
    StringInfoData detail;
    if (relOid != NULL) {
        *relOid = InvalidOid;
    }
    initStringInfo(&detail);
    relid = get_relname_relid(relname, relnamespace);
    /* additionally, take it as a synonym and search for its referenced object name. */
    if (isSupportSynonym && !OidIsValid(relid)) {
        objVar = SearchReferencedObject(relname, relnamespace);
        /* if found, then fetch the relid of the objVar and check whether or not we has supported already. */
        if (objVar != NULL) {
            objNamespaceOid = get_namespace_oid(objVar->schemaname, true);
            relid = get_relname_relid(objVar->relname, objNamespaceOid);
            appendStringInfo(&detail, _("%s"), CheckReferencedObject(relid, objVar, relname));
            /* store the referenced synonym oid when mapping successfully. */
            if (refSynOid != NULL && OidIsValid(relid)) {
                *refSynOid = GetSynonymOid(relname, relnamespace, true);
            }
        }
    }
    if (relOid != NULL) {
        *relOid = relid;
    }
    /* return checking details. */
    return detail.data;
}

extern bool StreamTopConsumerAmI();

/* same as get_relname_relid except we check for cache invalidation here */
Oid get_valid_relname_relid(const char* relnamespace, const char* relname, bool nsp_missing_ok)
{
    Oid nspid = InvalidOid;
    Oid oldnspid = InvalidOid;
    Oid relid = InvalidOid;
    Oid oldrelid = InvalidOid;
    bool retry = false;
    Assert(relnamespace != NULL);
    Assert(relname != NULL);
    /*
     * DDL operations can change the results of a name lookup.	Since all such
     * operations will generate invalidation messages, we keep track of
     * whether any such messages show up while we're performing the operation,
     * and retry until either (1) no more invalidation messages show up or (2)
     * the answer doesn't change.
     *
     */
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }
        nspid = get_namespace_oid(relnamespace, nsp_missing_ok);
        if (!OidIsValid(nspid)) {
            return InvalidOid;
        }
        relid = get_relname_relid(relname, nspid);
        /*
         * In bootstrap processing mode, we don't bother with locking
         */
        if (IsBootstrapProcessingMode()) {
            break;
        }
        /*
         * If, upon retry, we get back the same OID we did last time, then the
         * invalidation messages we processed did not change the final answer.
         * So we're done.
         *
         * If we got a different OID, we've locked the relation that used to
         * have this name rather than the one that does now.  So release the
         * lock.
         */
        if (retry) {
            /* we get the same relid, nothing has changed, so return it */
            if (relid == oldrelid && nspid == oldnspid) {
                break;
            }
            if (!OidIsValid(nspid)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("namespace %s was invalid after retry", relnamespace)));
            }
            if (!OidIsValid(relid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("relation %s was invalid after retry", relname)));
            }
            /* If creation namespace has changed, give up old lock. */
            if (nspid != oldnspid && OidIsValid(oldnspid)) {
                UnlockDatabaseObject(NamespaceRelationId, oldnspid, 0, AccessShareLock);
            }
            if (relid != oldrelid && OidIsValid(oldrelid)) {
                UnlockRelationOid(oldrelid, AccessShareLock);
            }
        }
        /* Lock relation */
        if (OidIsValid(relid) && StreamTopConsumerAmI()) {
            /*
             * Keep the oid fetching in plan-serialize stage blocked
             * until the former blocking transaction end, we have to
             * do so, since that it could be a vacuum-full operation
             * where the relid gets changed, thus the de-serializing
             * may run into none-matching relid exception.
             */
            LockRelationOid(relid, AccessShareLock);
        }
        /* Lock namespace */
        if (OidIsValid(nspid) && StreamTopConsumerAmI()) {
            LockDatabaseObject(NamespaceRelationId, nspid, 0, AccessShareLock);
        }
        /* If no invalidation message were processed, we're done! */
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                break;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                break;
            }
        }
        /*
         * Let's repeat the name lookup, to make
         * sure this name still references the same relation it did
         * previously.
         */
        retry = true;
        oldrelid = relid;
        oldnspid = nspid;
    }
    return relid;
}

#ifdef PGXC
/*
 * get_relnatts
 *
 *		Returns the number of attributes for a given relation.
 */
int get_relnatts(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        int result;

        CatalogRelationBuildParam catalogParam = GetCatalogParam(relid);
        if (catalogParam.oid != InvalidOid) {
            result = (AttrNumber)catalogParam.natts;
        } else {
            result = reltup->relnatts;
        }

        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidAttrNumber;
    }
}
#endif

/*
 * get_rel_name
 *		Returns the name of a given relation.
 *
 * Returns a palloc'd copy of the string, or NULL if no such relation.
 *
 * NOTE: since relation name is not unique, be wary of code that uses this
 * for anything except preparing error messages.
 */
char* get_rel_name(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        char* result = NULL;
        result = pstrdup(NameStr(reltup->relname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return NULL;
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Returns the name of a given partition
 * Description	: the partition is eihher a table partiton or an index partition
 * Notes		: Returns a palloc'd copy of the string, or NULL if no such relation.
 */
char* getPartitionName(Oid partid, bool missing_ok)
{
    HeapTuple tuple = NULL;
    char* pname = NULL;
    bool found = false;
    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
    if (HeapTupleIsValid(tuple)) {
        Form_pg_partition partition = (Form_pg_partition)GETSTRUCT(tuple);
        /* make sure it is a partition */
        Assert((PART_OBJ_TYPE_TABLE_PARTITION == partition->parttype) ||
               (PART_OBJ_TYPE_TABLE_SUB_PARTITION == partition->parttype) ||
               (PART_OBJ_TYPE_INDEX_PARTITION == partition->parttype));
        /* get partition name */
        pname = pstrdup(NameStr(partition->relname));
        found = true;
        ReleaseSysCache(tuple);
    }

    if (!found && !missing_ok) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("object with oid %u is not a partition object", partid)));
    }
    return pname;
}

/*
 * check_rel_is_partitioned
 *
 *		check whether a given relation is partitioned.
 */
bool check_rel_is_partitioned(Oid relid)
{
    HeapTuple tp = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(relid), LOG);
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        bool result = (reltup->parttype == PARTTYPE_PARTITIONED_RELATION) ||
                      (reltup->parttype == PARTTYPE_SUBPARTITIONED_RELATION);
        ReleaseSysCache(tp);
        return result;
    } else {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
        return false;  // to ease compiler complain
    }
}

/*
 * get_rel_namespace
 *
 *		Returns the pg_namespace OID associated with a given relation.
 */
Oid get_rel_namespace(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        Oid result;
        result = reltup->relnamespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * @Description: check the table's namespace is system catalog or table is not a system table.
 * @in - relid: table oid
 * @out - bool: return true if it satisfies check condition.
 */
bool is_sys_table(Oid relid)
{
    HeapTuple tuple;
    Oid namespaceoid;
    char* schemaname = NULL;
    namespaceoid = get_rel_namespace(relid);
    schemaname = get_namespace_name(namespaceoid);
    /*
     * NodeGroup support
     *
     * In multi-nodegroup environment, we need fetch table's exec_nodes before apply lock,
     * so when concurrent with DROP operation we may get relid but the table has been dropped
     * in another transaction, so we need check if it is not exist here.
     */
    if (schemaname == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("Table object with oid %u does not exists (has been dropped)", relid)));
    }
    if (namespaceoid == PG_CATALOG_NAMESPACE) {
        pfree_ext(schemaname);
        return true;
    }
    if (strncmp(schemaname, "information_schema", sizeof("information_schema")) == 0) {
        pfree_ext(schemaname);
        tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(relid));
        if (HeapTupleIsValid(tuple)) {
            ReleaseSysCache(tuple);
            return false;
        } else {
            return true;
        }
    } else if (IsToastNamespace(namespaceoid) && relid < FirstNormalObjectId) {
        pfree_ext(schemaname);
        /* Bootstrap toast tables are also considered as system tables. */
        return true;
    } else {
        pfree_ext(schemaname);
        return false;
    }
}

/*
 * get_rel_type_id
 *
 *		Returns the pg_type OID associated with a given relation.
 *
 * Note: not all pg_class entries have associated pg_type OIDs; so be
 * careful to check for InvalidOid result.
 */
Oid get_rel_type_id(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        Oid result;
        result = reltup->reltype;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_rel_relkind
 *
 *		Returns the relkind associated with a given relation.
 */
char get_rel_relkind(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        char result;
        result = reltup->relkind;
        ReleaseSysCache(tp);
        return result;
    } else {
        return '\0';
    }
}

/*
 * get_rel_tablespace
 *
 *		Returns the pg_tablespace OID associated with a given relation.
 *
 * Note: InvalidOid might mean either that we couldn't find the relation,
 * or that it is in the database's default tablespace.
 */
Oid get_rel_tablespace(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        Oid result;
        result = reltup->reltablespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_rel_persistence
 *
 *        Returns the relpersistence associated with a given relation.
 */
char get_rel_persistence(Oid relid)
{
    HeapTuple    tp;    
    Form_pg_class reltup;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        reltup = (Form_pg_class) GETSTRUCT(tp);
        char result = reltup->relpersistence;
        ReleaseSysCache(tp);
        return result;
    } else {
        /*
         * for partition table.
         * global temp table does not support partitioning. 
         */
        return '\0';
    }
}

/*				---------- TYPE CACHE ----------						 */

/*
 * get_typisdefined
 *
 *		Given the type OID, determine whether the type is defined
 *		(if not, it's only a shell).
 */
bool get_typisdefined(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        bool result = false;

        result = typtup->typisdefined;
        ReleaseSysCache(tp);
        return result;
    } else {
        return false;
    }
}

/*
 * get_typlen
 *
 *		Given the type OID, return the length of the type.
 */
int16 get_typlen(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        int16 result;
        result = typtup->typlen;
        ReleaseSysCache(tp);
        return result;
    } else {
        return 0;
    }
}

/*
 * get_typbyval
 *
 *		Given the type OID, determine whether the type is returned by value or
 *		not.  Returns true if by value, false if by reference.
 */
bool get_typbyval(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        bool result = false;
        result = typtup->typbyval;
        ReleaseSysCache(tp);
        return result;
    } else {
        return false;
    }
}

/*
 * get_typlenbyval
 *
 *		A two-fer: given the type OID, return both typlen and typbyval.
 *
 *		Since both pieces of info are needed to know how to copy a Datum,
 *		many places need both.	Might as well get them with one cache lookup
 *		instead of two.  Also, this routine raises an error instead of
 *		returning a bogus value when given a bad type OID.
 */
void get_typlenbyval(Oid typid, int16* typlen, bool* typbyval)
{
    HeapTuple tp;
    Form_pg_type typtup;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typtup = (Form_pg_type)GETSTRUCT(tp);
    *typlen = typtup->typlen;
    *typbyval = typtup->typbyval;
    ReleaseSysCache(tp);
}

/*
 * get_typlenbyvalalign
 *
 *		A three-fer: given the type OID, return typlen, typbyval, typalign.
 */
void get_typlenbyvalalign(Oid typid, int16* typlen, bool* typbyval, char* typalign)
{
    HeapTuple tp;
    Form_pg_type typtup;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typtup = (Form_pg_type)GETSTRUCT(tp);
    *typlen = typtup->typlen;
    *typbyval = typtup->typbyval;
    *typalign = typtup->typalign;
    ReleaseSysCache(tp);
}

/*
 * getTypeIOParam
 *		Given a pg_type row, select the type OID to pass to I/O functions
 *
 * Formerly, all I/O functions were passed pg_type.typelem as their second
 * parameter, but we now have a more complex rule about what to pass.
 * This knowledge is intended to be centralized here --- direct references
 * to typelem elsewhere in the code are wrong, if they are associated with
 * I/O calls and not with actual subscripting operations!  (But see
 * bootstrap.c's boot_get_type_io_data() if you need to change this.)
 *
 * As of PostgreSQL 8.1, output functions receive only the value itself
 * and not any auxiliary parameters, so the name of this routine is now
 * a bit of a misnomer ... it should be getTypeInputParam.
 */
Oid getTypeIOParam(HeapTuple typeTuple)
{
    Form_pg_type typeStruct = (Form_pg_type)GETSTRUCT(typeTuple);
    /*
     * Array types get their typelem as parameter; everybody else gets their
     * own type OID as parameter.
     */
    if (OidIsValid(typeStruct->typelem)) {
        return typeStruct->typelem;
    } else {
        return HeapTupleGetOid(typeTuple);
    }
}

/*
 * get_type_io_data
 *
 *		A six-fer:	given the type OID, return typlen, typbyval, typalign,
 *					typdelim, typioparam, and IO function OID. The IO function
 *					returned is controlled by IOFuncSelector
 */
void get_type_io_data(Oid typid, IOFuncSelector which_func, int16* typlen, bool* typbyval, char* typalign,
    char* typdelim, Oid* typioparam, Oid* func)
{
    HeapTuple typeTuple;
    Form_pg_type typeStruct;
    /*
     * In bootstrap mode, pass it off to bootstrap.c.  This hack allows us to
     * use array_in and array_out during bootstrap.
     */
    if (IsBootstrapProcessingMode()) {
        Oid typinput;
        Oid typoutput;
        boot_get_type_io_data(typid, typlen, typbyval, typalign, typdelim, typioparam, &typinput, &typoutput);
        switch (which_func) {
            case IOFunc_input:
                *func = typinput;
                break;
            case IOFunc_output:
                *func = typoutput;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("binary I/O not supported during bootstrap")));
                break;
        }
        return;
    }
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typeStruct = (Form_pg_type)GETSTRUCT(typeTuple);
    *typlen = typeStruct->typlen;
    *typbyval = typeStruct->typbyval;
    *typalign = typeStruct->typalign;
    *typdelim = typeStruct->typdelim;
    *typioparam = getTypeIOParam(typeTuple);
    switch (which_func) {
        case IOFunc_input:
            *func = typeStruct->typinput;
            break;
        case IOFunc_output:
            *func = typeStruct->typoutput;
            break;
        case IOFunc_receive:
            *func = typeStruct->typreceive;
            break;
        case IOFunc_send:
            *func = typeStruct->typsend;
            break;
        default:
            break;
    }
    ReleaseSysCache(typeTuple);
}

#ifdef NOT_USED
char get_typalign(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        char result;
        result = typtup->typalign;
        ReleaseSysCache(tp);
        return result;
    } else {
        return 'i';
    }
}
#endif

char get_typstorage(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        char result;
        result = typtup->typstorage;
        ReleaseSysCache(tp);
        return result;
    } else {
        return 'p';
    }
}

/*
 * get_typdefault
 *	  Given a type OID, return the type's default value, if any.
 *
 *	  The result is a palloc'd expression node tree, or NULL if there
 *	  is no defined default for the datatype.
 *
 * NB: caller should be prepared to coerce result to correct datatype;
 * the returned expression tree might produce something of the wrong type.
 */
Node* get_typdefault(Oid typid)
{
    HeapTuple typeTuple;
    Form_pg_type type;
    Datum datum;
    bool isNull = false;
    Node* expr = NULL;
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    type = (Form_pg_type)GETSTRUCT(typeTuple);
    /*
     * typdefault and typdefaultbin are potentially null, so don't try to
     * access 'em as struct fields. Must do it the hard way with
     * SysCacheGetAttr.
     */
    datum = SysCacheGetAttr(TYPEOID, typeTuple, Anum_pg_type_typdefaultbin, &isNull);
    if (!isNull) {
        /* We have an expression default */
        expr = (Node*)stringToNode(TextDatumGetCString(datum));
    } else {
        /* Perhaps we have a plain literal default */
        datum = SysCacheGetAttr(TYPEOID, typeTuple, Anum_pg_type_typdefault, &isNull);
        if (!isNull) {
            char* strDefaultVal = NULL;
            /* Convert text datum to C string */
            strDefaultVal = TextDatumGetCString(datum);
            /* Convert C string to a value of the given type */
            datum = OidInputFunctionCall(type->typinput, strDefaultVal, getTypeIOParam(typeTuple), -1);
            /* Build a Const node containing the value */
            expr = (Node*)makeConst(typid, -1, type->typcollation, type->typlen, datum, false, type->typbyval);
            pfree_ext(strDefaultVal);
        } else {
            /* No default */
            expr = NULL;
        }
    }
    ReleaseSysCache(typeTuple);
    return expr;
}

/*
 * getBaseType
 *		If the given type is a domain, return its base type;
 *		otherwise return the type's own OID.
 */
Oid getBaseType(Oid typid)
{
    int32 typmod = -1;
    return getBaseTypeAndTypmod(typid, &typmod);
}

/*
 * getBaseTypeAndTypmod
 *		If the given type is a domain, return its base type and typmod;
 *		otherwise return the type's own OID, and leave *typmod unchanged.
 *
 * Note that the "applied typmod" should be -1 for every domain level
 * above the bottommost; therefore, if the passed-in typid is indeed
 * a domain, *typmod should be -1.
 */
Oid getBaseTypeAndTypmod(Oid typid, int32* typmod)
{
    /*
     * We loop to find the bottom base type in a stack of domains.
     */
    for (;;) {
        HeapTuple tup;
        Form_pg_type typTup;
        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if (!HeapTupleIsValid(tup)) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
        }
        typTup = (Form_pg_type)GETSTRUCT(tup);
        if (typTup->typtype != TYPTYPE_DOMAIN) {
            /* Not a domain, so done */
            ReleaseSysCache(tup);
            break;
        }
        Assert(*typmod == -1);
        typid = typTup->typbasetype;
        *typmod = typTup->typtypmod;
        ReleaseSysCache(tup);
    }
    return typid;
}

#ifdef PGXC
/*
 * get_cfgname
 *		Get cfg name for given cfg ID
 */
char* get_cfgname(Oid cfgid)
{
    HeapTuple tuple;
    Form_pg_ts_config cfgform;
    char* result = NULL;
    tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for cfg %u", cfgid)));
    }
    cfgform = (Form_pg_ts_config)GETSTRUCT(tuple);
    result = NameStr(cfgform->cfgname);
    ReleaseSysCache(tuple);
    return result;
}
/*
 * get_typename
 *		Get type name for given type ID
 */
char* get_typename(Oid typid)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    char* result = NULL;
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    result = pstrdup(NameStr(typeForm->typname));
    ReleaseSysCache(tuple);
    return result;
}
/*
 * get_typename
 *		Get enumtype label name for given enumtype labeliD
 */
char* get_enumlabelname(Oid enumlabelid)
{
    HeapTuple tuple;
    Form_pg_enum en;
    char* result = NULL;
    tuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumlabelid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for enumlabelid %u", enumlabelid)));
    }
    en = (Form_pg_enum)GETSTRUCT(tuple);
    result = pstrdup(NameStr(en->enumlabel));
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_typename
 *		Get enumtype name for given enumtype labeliD
 */
char* get_exprtypename(Oid enumlabelid)
{
    HeapTuple tuple;
    Form_pg_enum en;
    Form_pg_type typeForm;
    Oid enumtypid;
    char* result = NULL;
    tuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumlabelid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for enumlabelid %u", enumlabelid)));
    }
    en = (Form_pg_enum)GETSTRUCT(tuple);
    enumtypid = en->enumtypid;
    ReleaseSysCache(tuple);
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(enumtypid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for enumtype %u", enumtypid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    result = pstrdup(NameStr(typeForm->typname));
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_typename
 *		Get type name with namespace for given type ID
 */
char* get_typename_with_namespace(Oid typid)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    char* result = NULL;
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    // Produce a string representing the name of a TypeName. It is always with
    // format namespace.typename, except for "pg_catalog" namespace. This is
    // because most common data types are from it, so we optimize it to avoid
    // extra overhead to lookup for it on both sides.
    //
    // Temp namespace needs special handling here, we can't omit its namespace
    // as it may contains a literally same typname as pg_catalog. Send the name
    // directly won't work as its name can vary on different nodes. We send a
    // special flag instead.
    //
    if (typeForm->typnamespace == PG_CATALOG_NAMESPACE) {
        result = pstrdup(NameStr(typeForm->typname));
    } else {
        StringInfoData typeName;
        char* typeNamespace = "%";
        if (!isTempNamespace(typeForm->typnamespace)) {
            typeNamespace = get_namespace_name(typeForm->typnamespace);
            Assert(typeNamespace[0] != '%');
        }
        initStringInfo(&typeName);
        appendStringInfoString(&typeName, typeNamespace);
        appendStringInfoString(&typeName, ".");
        appendStringInfoString(&typeName, NameStr(typeForm->typname));
        result = pstrdup(typeName.data);
        if (typeNamespace[0] != '%') {
            pfree_ext(typeNamespace);
        }
        pfree_ext(typeName.data);
    }
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_typeoid_with_namespace
 *		Get type oid with namespace for given type name. This actually works as
 * 		a "reversal" version of get_typename_with_namespace().
 */
Oid get_typeoid_with_namespace(const char* typname)
{
    Oid typid;
    Oid namespaceId = InvalidOid;
    int elen;
    List* elemlist = NULL;
    char* nspname = NULL;
    char* atpname = NULL;
    // Separate namespace and typename from given string
    //
    char* namestr = pstrdup(typname);
    if (!SplitIdentifierString(namestr, '.', &elemlist, false)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid syntax for type: \"%s\"", namestr)));
    }
    elen = list_length(elemlist);
    if (elen == 1) {
        atpname = (char*)linitial(elemlist);
        namespaceId = PG_CATALOG_NAMESPACE;
    } else if (elen == 2) {
        nspname = (char*)linitial(elemlist);
        atpname = (char*)lsecond(elemlist);
        // Take care of temp namespace specially
        //
        if (nspname[0] == '%') {
            namespaceId = get_my_temp_schema();
        } else {
            namespaceId = get_namespace_oid(nspname, false);
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid syntax for type: \"%s\"", namestr)));
    }
    // Search the unique type oid by namespace and typename.
    //
    Assert(OidIsValid(namespaceId));
    typid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(atpname), ObjectIdGetDatum(namespaceId));
    pfree_ext(namestr);
    list_free_ext(elemlist);
    return typid;
}

/*
 * get_pgxc_nodeoid
 *		Obtain PGXC Node Oid for given node name
 *		Return Invalid Oid if object does not exist
 */
Oid get_pgxc_nodeoid(const char* nodename)
{
    Oid node_oid = InvalidOid;
    CatCList* memlist = NULL;
    int i;
    memlist = SearchSysCacheList1(PGXCNODENAME, PointerGetDatum(nodename));
    for (i = 0; i < memlist->n_members; i++) {
        HeapTuple tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, i);
        char node_type = ((Form_pgxc_node)GETSTRUCT(tup))->node_type;
        if (node_type == PGXC_NODE_COORDINATOR || node_type == PGXC_NODE_DATANODE) {
            node_oid = HeapTupleGetOid(tup);
            break;
        }
    }
    ReleaseSysCacheList(memlist);
    return node_oid;
}

Oid get_pgxc_datanodeoid(const char* nodename, bool missingOK)
{
    Oid dn_oid = InvalidOid;
    CatCList* memlist = NULL;
    int i;
    memlist = SearchSysCacheList1(PGXCNODENAME, PointerGetDatum(nodename));
    for (i = 0; i < memlist->n_members; i++) {
        HeapTuple tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, i);
        char node_type = ((Form_pgxc_node)GETSTRUCT(tup))->node_type;
        if (node_type == PGXC_NODE_DATANODE) {
            dn_oid = HeapTupleGetOid(tup);
            break;
        }
    }
    ReleaseSysCacheList(memlist);

    if (dn_oid == InvalidOid && !missingOK) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("datanode \"%s\" does not exist", nodename)));
    }

    return dn_oid;
}


/*
 * Check if the two given strings indicate the same host.
 */
 inline bool IsSameHost(const char* str1, const char* str2){
     if (str1 == NULL || str2 == NULL){
         return false;
     }
     return (strcmp(str1, "localhost") == 0 && strcmp(str2, "127.0.0.1") == 0) ||
            (strcmp(str1, str2) == 0);
 }

/*
 * Check the datanode is exist.
 * 	this case only make sense under multi-standbys mode.
 */
bool check_pgxc_node_name_is_exist(
    const char* nodename, const char* host, int port, int comm_sctp_port, int comm_control_port)
{
    CatCList* memlist = NULL;
    bool node_has_been_def = false;
    int i;
    Assert(host != NULL);
    memlist = SearchSysCacheList1(PGXCNODENAME, PointerGetDatum(nodename));
    for (i = 0; i < memlist->n_members; i++) {
        HeapTuple tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, i);
        const char* host_exist = NameStr(((Form_pgxc_node)GETSTRUCT(tup))->node_host);
        int port_exist = ((Form_pgxc_node)GETSTRUCT(tup))->node_port;
        int comm_sctp_port_exist = ((Form_pgxc_node)GETSTRUCT(tup))->sctp_port;
        int comm_control_port_exist = ((Form_pgxc_node)GETSTRUCT(tup))->control_port;

        /* check host */
        if (IsSameHost(host_exist, host)) {
            /* check port */
            if (port_exist == port) {
                node_has_been_def = true;
                break;
            }
            /* check comm ports */
            if (comm_sctp_port_exist == comm_sctp_port || comm_control_port_exist == comm_control_port) {
                node_has_been_def = true;
                break;
            }
        }
    }
    ReleaseSysCacheList(memlist);
    return node_has_been_def;
}

Datum node_oid_name(PG_FUNCTION_ARGS)
{
    Oid nodeoid = PG_GETARG_OID(0);
    PG_RETURN_NAME(get_pgxc_nodename(nodeoid));
}

/*
 * get_pgxc_groupname
 *		Get nodegroup name by given nodeoid, return NULL if nodegroup not found or
 *		given an invalid oid.
 */
char* get_pgxc_groupname(Oid groupoid, char* groupname)
{
    HeapTuple tuple;
    Form_pgxc_group groupForm;
    if (!OidIsValid(groupoid)) {
        return NULL;
    }
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(groupoid));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }
    groupForm = (Form_pgxc_group)GETSTRUCT(tuple);
    if (groupname == NULL) {
        groupname = pstrdup(NameStr(groupForm->group_name));
    } else {
        errno_t errval = strncpy_s(groupname, NAMEDATALEN, NameStr(groupForm->group_name), NAMEDATALEN - 1);
        securec_check_errval(errval,, LOG);
    }
    ReleaseSysCache(tuple);
    return groupname;
}

void get_node_info(const char* nodename, bool* node_is_ccn, ItemPointer tuple_pos)
{
    HeapTuple tuple;
    Relation rel;
    bool is_null = false;
    CatCList* memlist = NULL;
    Assert(nodename);
    Assert(node_is_ccn);
    Assert(tuple_pos);
    if (!StringIsValid(nodename)) {
        return;
    }
    if (node_is_ccn == NULL || tuple_pos == NULL) {
        return;
    }
    if (t_thrd.proc == NULL) {
        *node_is_ccn = false;
        return;
    }
    memlist = NULL;
    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    PG_TRY();
    {
        memlist = SearchSysCacheList1(PGXCNODENAME, PointerGetDatum(nodename));

        if (memlist->n_members != 1) {
            *node_is_ccn = false;
            ReleaseSysCacheList(memlist);
            heap_close(rel, AccessShareLock);
            return;
        }
        tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, 0);
        /* get nodeis_central for the node */
        Datum centralDatum = SysCacheGetAttr(PGXCNODEOID, tuple, Anum_pgxc_node_is_central, &is_null);
        if (!is_null) {
            *node_is_ccn = DatumGetBool(centralDatum);
            ItemPointerCopy(&tuple->t_self, tuple_pos);
        } else {
            *node_is_ccn = false;
        }
        ReleaseSysCacheList(memlist);
        heap_close(rel, AccessShareLock);
    }
    PG_CATCH();
    {
        if (memlist != NULL) {
            ReleaseSysCacheList(memlist);
        }
        heap_close(rel, AccessShareLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return;
}

/*
 * get_pgxc_nodename
 *		Get node name for given Oid
 */
char* get_pgxc_nodename(Oid nodeid, NameData* namedata)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    errno_t rc;
    char* nodename = NULL;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    if (namedata != NULL) {
        nodename = namedata->data;
        rc = strncpy_s(nodename, NAMEDATALEN, NameStr(nodeForm->node_name), NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
    } else {
        nodename = pstrdup(NameStr(nodeForm->node_name));
    }
    ReleaseSysCache(tuple);
    return nodename;
}

/*
 * get_pgxc_nodename_noexcept
 *		Get node name for given Oid, for print log.
 */

char* get_pgxc_nodename_noexcept(Oid nodeid, NameData* nodename)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    errno_t rc;
    if (t_thrd.utils_cxt.CurrentResourceOwner == NULL || nodename == NULL) {
        return NULL;
    }
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);

    rc = strncpy_s(nodename->data, NAMEDATALEN, NameStr(nodeForm->node_name), NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");
    ReleaseSysCache(tuple);

    return nodename->data;
}

bool is_pgxc_central_nodeid(Oid nodeid)
{
    if (t_thrd.proc == NULL) {
        return false;
    }
    HeapTuple tuple;
    bool is_central = false;
    bool is_null = false;
    if (!OidIsValid(nodeid)) {
        return false;
    }
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        return false;
    }
    /* get nodeis_central for the node */
    Datum centralDatum = SysCacheGetAttr(PGXCNODEOID, tuple, Anum_pgxc_node_is_central, &is_null);
    if (!is_null) {
        is_central = DatumGetBool(centralDatum);
    }
    ReleaseSysCache(tuple);
    return is_central;
}

bool is_pgxc_central_nodename(const char* nodename)
{
    bool is_central = false;
    Datum central_datum;
    CatCList* memlist = NULL;
    HeapTuple tup;
    memlist = SearchSysCacheList1(PGXCNODENAME, PointerGetDatum(nodename));
    if (memlist->n_members != 1) {
        /*
         * If we get more then one node named by 'nodename', we meet datanode under
         *	repliation_type == 1. we just return false;
         * If we find nothing, return false too.
         */
        is_central = false;
        goto result;
    }
    tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, 0);
    central_datum = ((Form_pgxc_node)GETSTRUCT(tup))->nodeis_central;
    is_central = DatumGetBool(central_datum);
result:
    ReleaseSysCacheList(memlist);
    return is_central;
}

char* get_pgxc_node_formdata(const char* nodename)
{
    if (!StringIsValid(nodename)) {
        return NULL;
    }
    return get_pgxc_nodehost(get_pgxc_nodeoid(nodename));
}

/*
 * Description: Get node name for a given Oid.
 *
 * Parameters:
 *	@in node_id: Node Oid.
 *	@in handle_error: need report error when tuple is invalid(default: true)
 * Returns: char*
 */
char* get_pgxc_node_name_by_node_id(int4 node_id, bool handle_error)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    char* result = NULL;
    tuple = SearchSysCache1(PGXCNODEIDENTIFIER, Int32GetDatum(node_id));
    if (!HeapTupleIsValid(tuple)) {
        if (handle_error) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", node_id)));
        } else {
            return NULL;
        }
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = pstrdup(NameStr(nodeForm->node_name));
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_node_id
 *		Get node identifier for a given Oid
 */
uint32 get_pgxc_node_id(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    uint32 result;
    if (nodeid == InvalidOid) {
        return 0;
    }
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->node_id;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodetype
 *		Get node type for given Oid
 */
char get_pgxc_nodetype(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    char result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->node_type;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodetype
 *		Get node type for given Oid
 */
char get_pgxc_nodetype_refresh_cache(Oid nodeid)
{
    AcceptInvalidationMessages();
    return get_pgxc_nodetype(nodeid);
}

/*
 * get_pgxc_nodeport
 *		Get node port for given Oid
 */
int get_pgxc_nodeport(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    int result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->node_port;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodehost
 *		Get node host for given Oid
 */
char* get_pgxc_nodehost(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    char* result = NULL;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = pstrdup(NameStr(nodeForm->node_host));
    ReleaseSysCache(tuple);
    return result;
}
/*
 * get_pgxc_nodeport1
 *		Get node port for given Oid
 */
int get_pgxc_nodeport1(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    int result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->node_port1;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodehost
 *		Get node host for given Oid
 */
char* get_pgxc_nodehost1(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    char* result = NULL;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = pstrdup(NameStr(nodeForm->node_host1));
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodesctpport
 *		Get node sctp port for given Oid
 */
int get_pgxc_nodesctpport(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    int result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->sctp_port;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodesctpport
 *		Get node strmctl port for given Oid
 */
int get_pgxc_nodestrmctlport(Oid nodeid)

{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    int result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->control_port;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodesctpport
 *		Get node sctp port for given Oid
 */
int get_pgxc_nodesctpport1(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    int result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->sctp_port1;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_nodesctpport
 *		Get node strmctl port for given Oid
 */
int get_pgxc_nodestrmctlport1(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    int result;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->control_port1;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * is_pgxc_nodepreferred
 *		Determine if node is a preferred one
 */
bool is_pgxc_nodepreferred(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    bool result = false;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->nodeis_preferred;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * is_pgxc_hostprimary
 *		Determine if node host is a primary ip
 */
bool is_pgxc_hostprimary(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    bool result = false;
    LockRelationOid(PgxcNodeRelationId, AccessShareLock);
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        UnlockRelationOid(PgxcNodeRelationId, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->hostis_primary;
    ReleaseSysCache(tuple);
    UnlockRelationOid(PgxcNodeRelationId, AccessShareLock);
    return result;
}

/*
 * is_pgxc_nodeprimary
 *		Determine if node is a primary one
 */
bool is_pgxc_nodeprimary(Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    bool result = false;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    result = nodeForm->nodeis_primary;
    ReleaseSysCache(tuple);
    return result;
}

bool is_pgxc_nodeactive(Oid nodeid)
{
    HeapTuple tuple;
    bool isactive = false;
    bool isNull = false;
    if (!OidIsValid(nodeid)) {
        return false;
    }
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    Datum activeDatum = SysCacheGetAttr(PGXCNODEOID, tuple, Anum_pgxc_node_is_active, &isNull);
    if (!isNull) {
        isactive = DatumGetBool(activeDatum);
    }
    ReleaseSysCache(tuple);
    return isactive;
}

/*
 * @Description: check host is match with node's Primary Host
 * @in host: host
 * @in nodeid: nodeid
 * @return: true if match, false if not
 *
 * Notes: suppose primary and standby deployed on different machine,
 *			so, we just check host, no need to check port.
 */
bool node_check_host(const char* host, Oid nodeid)
{
    HeapTuple tuple;
    Form_pgxc_node nodeForm;
    bool result = true;
    char node_type = PGXC_NODE_COORDINATOR;
    bool is_primary_dn = false;
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for node %u", nodeid)));
    }
    nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
    node_type = nodeForm->node_type;
    if (node_type == PGXC_NODE_COORDINATOR) {
        is_primary_dn = true;
    } else {
        if (IS_DN_DUMMY_STANDYS_MODE()) {
            is_primary_dn = nodeForm->hostis_primary;
        } else {
            /* multi standby */
            is_primary_dn = IS_PRIMARY_DATA_NODE(node_type);
        }
    }

    if (is_primary_dn) {
        /* host is primary, check node_host is match with input */
        if (strncmp(NameStr(nodeForm->node_host), host, strlen(host)) != 0) {
            result = false;
        }
    } else {
        /* standby mode */
        if (IS_DN_DUMMY_STANDYS_MODE()) {
            if (strncmp(NameStr(nodeForm->node_host1), host, strlen(host)) != 0) {
                result = false;
            }
        } else {
            /* multi standby */
            Oid primary_oid = PgxcNodeGetPrimaryDNFromMatric(nodeid);
            char* primary_host = get_pgxc_nodehost(primary_oid);
            if (primary_host != NULL) {
                if (strncmp(primary_host, host, strlen(host)) != 0) {
                    result = false;
                }
                pfree_ext(primary_host);
                primary_host = NULL;
            }
        }
    }
    ReleaseSysCache(tuple);

    return result;
}

/*
 * get_pgxc_groupoid
 *		Obtain PGXC Group Oid for given group name
 *		Return Invalid Oid if group does not exist
 */
Oid get_pgxc_groupoid(const char* groupname, bool missing_ok)
{
    Oid groupoid = InvalidOid;
    if (!ng_is_valid_group_name(groupname)) {
        return InvalidOid;
    }

    groupoid = GetSysCacheOid1(PGXCGROUPNAME, PointerGetDatum(groupname));
    if (!OidIsValid(groupoid) && !missing_ok) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Target node group \"%s\" does not exist", groupname)));
    }

    return groupoid;
}

int get_pgxc_group_bucketcnt(Oid group_oid)
{
    HeapTuple tuple;
    bool is_null = false;
    Datum group_datum;
    int bucketcnt = 0;
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(group_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("get_pgxc_group_bucketcnt lookup failed for group %u", group_oid)));
    }
    group_datum = SysCacheGetAttr(PGXCGROUPOID, tuple, Anum_pgxc_group_buckets, &is_null);
    if (is_null) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("get_pgxc_group_bucketcnt invalid bucketcnt for group %u", group_oid)));
    }
    text_to_bktmap((text*)group_datum, NULL, &bucketcnt);
    ReleaseSysCache(tuple);
    return bucketcnt;
}

bool is_pgxc_group_bucketcnt_exists(Oid parentOid, int bucketCnt, char **groupName, Oid *groupOid)
{
    HeapTuple tuple;
    bool ret = false;

    Relation rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    TableScanDesc scan = heap_beginscan(rel, SnapshotNow, 0, NULL);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        bool isnull;
        int  bucketlen;
        Datum val = heap_getattr(tuple, Anum_pgxc_group_parent, RelationGetDescr(rel), &isnull);
        /* We only check child of node group with OID parentOid */
        if (isnull || parentOid != DatumGetObjectId(val)) {
            continue;
        }

        val = heap_getattr(tuple, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isnull);
        if (isnull) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("is_pgxc_group_bucketcnt_exists invalid bucketcnt")));
        }
        text_to_bktmap((text*)val, NULL, &bucketlen);
        if (bucketlen == bucketCnt) {
            ret = true;
            if (groupName) {
                Datum group_name_datum = heap_getattr(tuple, Anum_pgxc_group_name, RelationGetDescr(rel), &isnull);
                *groupName = (char*)pstrdup((const char*)DatumGetCString(group_name_datum));
            }
            if (groupOid) {
                *groupOid = HeapTupleGetOid(tuple);
            }
            break;
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    return ret;
}

char* get_pgxc_groupparent(Oid group_oid)
{
    HeapTuple tuple;
    bool is_null = false;
    Datum group_datum;
    char *parent = NULL;

    Relation rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(group_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("get_pgxc_group_parentcache lookup failed for group %u", group_oid)));
    }
    group_datum = SysCacheGetAttr(PGXCGROUPOID, tuple, Anum_pgxc_group_parent, &is_null);
    if (!is_null) {
        Oid parentOid = DatumGetObjectId(group_datum);
        parent = get_pgxc_groupname(parentOid);
    }
    ReleaseSysCache(tuple);
    heap_close(rel, AccessShareLock);

    return parent;
}

/*
 * get_pgxc_groupkind
 *		Obtain PGXC groupkind for given group name
 */
char get_pgxc_groupkind(Oid group_oid)
{
    HeapTuple tuple;
    bool isNull = false;
    bool is_null = false;
    char group_kind;
    Datum group_datum;
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(group_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for group %u", group_oid)));
    }
    group_datum = SysCacheGetAttr(PGXCGROUPOID, tuple, Anum_pgxc_group_is_installation, &isNull);
    if (!isNull && DatumGetBool(group_datum)) {
        group_kind = PGXC_GROUPKIND_INSTALLATION;
    } else {
        group_datum = SysCacheGetAttr(PGXCGROUPOID, tuple, Anum_pgxc_group_kind, &is_null);
        if (is_null) {
            group_kind = PGXC_GROUPKIND_NODEGROUP;
        } else if (!isNull && DatumGetChar(group_datum) == PGXC_GROUPKIND_INSTALLATION) {
            group_kind = PGXC_GROUPKIND_NODEGROUP;
        } else {
            group_kind = DatumGetChar(group_datum);
        }
    }
    ReleaseSysCache(tuple);
    return group_kind;
}

/*
 * get_pgxc_groupmembers
 *		Obtain PGXC Group members for given group Oid
 *		Return number of members and their list
 *
 * Member list is returned as a palloc'd array
 */
int get_pgxc_groupmembers(Oid groupid, Oid** members)
{
    HeapTuple tuple;
    int nmembers;
    oidvector* group_members = NULL;
    Oid* nodes = NULL;
    bool isNull = true;
    Relation rel;
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(groupid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for group %u", groupid)));
    }
    /* Get the raw data which contain group memeber's columns */
    rel = relation_open(PgxcGroupRelationId, NoLock);
    Datum group_member_datum = heap_getattr(tuple, Anum_pgxc_group_members, rel->rd_att, &isNull);
    /* if the raw value of group member is null, then report error */
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null group_members for tuple %u", HeapTupleGetOid(tuple))));
    }
    group_members = (oidvector*)PG_DETOAST_DATUM(group_member_datum);
    nmembers = group_members->dim1;
    nodes = group_members->values;
    /* The argument members maybe NULL if the caller only get nmembers */
    if (members != NULL) {
        *members = (Oid*)palloc0(nmembers * sizeof(Oid));
        for (int i = 0; i < nmembers; i++) {
            (*members)[i] = PgxcNodeGetPrimaryDNFromMatric(nodes[i]);
        }
    }
    if (group_members != (oidvector*)DatumGetPointer(group_member_datum)) {
        pfree_ext(group_members);
    }
    relation_close(rel, NoLock);
    ReleaseSysCache(tuple);
    return nmembers;
}

/*
 * Description: Get group redistribution status for a given group oid.
 *
 * Parameters:
 *	@group oid
 * Return: a char stand for group redistribution status
 */
char get_pgxc_group_redistributionstatus(Oid groupid)
{
    HeapTuple tuple;
    Form_pgxc_group groupForm;
    char result;
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(groupid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for group %u", groupid)));
    }
    groupForm = (Form_pgxc_group)GETSTRUCT(tuple);
    result = groupForm->in_redistribution;
    Assert(result != '\0');
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_pgxc_groupmembers_redist
 *		Obtain PGXC Group members for given group Oid
 *		Return number of members and their list
 *
 * Member list is returned as a palloc'd array
 * The function think of NodeGroup in redistribution.
 * If the NodeGroup is redistributing for adding new nodes,
 * we need to return the members including new nodes;
 * but if the NodeGroup is redistribuing for deleting nodes,
 * we return the original members.
 */
int get_pgxc_groupmembers_redist(Oid groupid, Oid** members)
{
    HeapTuple tuple;
    int nmembers;
    oidvector* group_members = NULL;
    Oid* nodes = NULL;
    bool isNull = true;
    Relation rel;
    Datum group_member_datum;
    if (members != NULL) {
        *members = NULL;
    }
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(groupid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for group %u", groupid)));
    }
    /* Get the raw data which contain group memeber's columns */
    rel = relation_open(PgxcGroupRelationId, NoLock);
    group_member_datum = heap_getattr(tuple, Anum_pgxc_group_members, rel->rd_att, &isNull);
    /* if the raw value of group member is null, then report error */
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null group_members for tuple %u", HeapTupleGetOid(tuple))));
    }
    group_members = (oidvector*)PG_DETOAST_DATUM(group_member_datum);
    nmembers = group_members->dim1;
    nodes = group_members->values;
    if (in_logic_cluster()) {
        Form_pgxc_group groupForm = (Form_pgxc_group)GETSTRUCT(tuple);
        if (groupForm->in_redistribution == PGXC_REDISTRIBUTION_SRC_GROUP) {
            int dst_nmembers = 0;
            Oid* dst_members = NULL;
            Oid dst_group = PgxcGroupGetRedistDestGroupOid();
            if (OidIsValid(dst_group)) {
                dst_nmembers = get_pgxc_groupmembers(dst_group, &dst_members);
                /* judge whether the node group is expanding. */
                if (dst_nmembers > nmembers) {
                    nmembers = dst_nmembers;
                    if (members != NULL) {
                        *members = dst_members;
                    }
                } else {
                    pfree_ext(dst_members);
                }
            }
        }
    }
    /* The argument members maybe NULL if the caller only get nmembers */
    if (members && *members == NULL) {
        *members = (Oid*)palloc0(nmembers * sizeof(Oid));
        for (int i = 0; i < nmembers; i++) {
            (*members)[i] = nodes[i];
        }
    }
    if (group_members != (oidvector*)DatumGetPointer(group_member_datum)) {
        pfree_ext(group_members);
    }
    relation_close(rel, NoLock);
    ReleaseSysCache(tuple);
    return nmembers;
}

static List* GetAllSliceTuples(Relation pgxcSliceRel, Oid tableOid)
{
    ScanKeyData skey[2];
    SysScanDesc sliceScan = NULL;
    HeapTuple htup = NULL;
    HeapTuple dtup = NULL;
    List* sliceList = NULL;

    ScanKeyInit(&skey[0], Anum_pgxc_slice_relid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(tableOid));
    ScanKeyInit(&skey[1], Anum_pgxc_slice_type, BTEqualStrategyNumber,
        F_CHAREQ, CharGetDatum(PGXC_SLICE_TYPE_SLICE));
    sliceScan = systable_beginscan(pgxcSliceRel, PgxcSliceOrderIndexId, true, NULL, 2, skey);
    while (HeapTupleIsValid((htup = systable_getnext(sliceScan)))) {
        dtup = heap_copytuple(htup);
        sliceList = lappend(sliceList, dtup);
    }
    systable_endscan(sliceScan);

    return sliceList;
}

static List* GetSliceBoundary(Relation rel, Datum boundaries, List* distKeyPosList, bool attIsNull, bool isRangeSlice)
{
    List* boundaryValueList = NIL;
    List* resultBoundaryList = NIL;
    ListCell* boundaryCell = NULL;
    Value* boundaryValue = NULL;
    Node* boundaryNode = NULL;
    bool isFirstDefault = true;
        
    if (attIsNull) {
        resultBoundaryList = NIL;
    } else {
        /* unstransform string items to Value list */
        boundaryValueList = untransformPartitionBoundary(boundaries);
        foreach(boundaryCell, boundaryValueList) {
            boundaryValue = (Value*)lfirst(boundaryCell);
            /* deal with null */
            if (!PointerIsValid(boundaryValue->val.str)) {
                if (isRangeSlice) {
                    boundaryNode = (Node*)makeMaxConst(UNKNOWNOID, -1, InvalidOid);
                } else {
                    /* when it's list slice, we just append one DEFAULT */
                    if (isFirstDefault) {
                        boundaryNode = (Node*)makeMaxConst(UNKNOWNOID, -1, InvalidOid);
                        isFirstDefault = false;
                    } else {
                        continue;
                    }
                }
            } else {
                /* To keep same with original text, we don't do type in function here, just keep the original string */
                boundaryNode = (Node*)make_const(NULL, makeString(boundaryValue->val.str), 0);
            }
            resultBoundaryList = lappend(resultBoundaryList, boundaryNode);
        }
    }

    return resultBoundaryList;
}

static DistState* BuildDistStateHelper(DistributionType distType, List* sliceDefinitions) 
{
    DistState* distState = makeNode(DistState);
    distState->strategy = (distType == DISTTYPE_RANGE) ? 'r' : 'l';
    distState->sliceList = sliceDefinitions;
    return distState;
}

static DistState* BuildDistState(Relation rel, List* distKeyPosList, DistributionType distType)
{
    bool tmpNull, attIsNull, specified;
    ListCell* sliceCell = NULL;
    List* boundary = NULL;
    List* sliceDefinitions = NIL;
    ListSliceDefState* listSliceDef = NULL;
    ListSliceDefState* lastListSliceDef = NULL;
    RangePartitionDefState* rangeSliceDef = NULL;

    Relation pgxcSliceRel = heap_open(PgxcSliceRelationId, AccessShareLock);
    TupleDesc desc = RelationGetDescr(pgxcSliceRel);
    Oid tableOid = RelationGetRelid(rel);

    List* sliceList = GetAllSliceTuples(pgxcSliceRel, tableOid);

    foreach (sliceCell, sliceList) {
        HeapTuple sliceTuple = (HeapTuple)lfirst(sliceCell);
        Datum boundDatum = heap_getattr(sliceTuple, Anum_pgxc_slice_boundaries, desc, &attIsNull);
        int4 sindex = DatumGetInt32(heap_getattr(sliceTuple, Anum_pgxc_slice_sindex, desc, &tmpNull));
        char* sliceName = DatumGetCString(heap_getattr(sliceTuple, Anum_pgxc_slice_relname, desc, &tmpNull));
        specified = DatumGetBool(heap_getattr(sliceTuple, Anum_pgxc_slice_specified, desc, &tmpNull));
        Oid nodeOid = DatumGetObjectId(heap_getattr(sliceTuple, Anum_pgxc_slice_nodeoid, desc, &tmpNull));

        if (distType == DISTTYPE_LIST) {
            boundary = GetSliceBoundary(rel, boundDatum, distKeyPosList, attIsNull, false);
            if (sindex == 0) {
                listSliceDef = makeNode(ListSliceDefState);
                listSliceDef->name = pstrdup(sliceName);
                if (specified) {
                    listSliceDef->datanode_name = get_pgxc_nodename(nodeOid, NULL);
                }

                listSliceDef->boundaries = lappend(listSliceDef->boundaries, boundary);
                sliceDefinitions = lappend(sliceDefinitions, listSliceDef);
                lastListSliceDef = listSliceDef;
            } else {
                lastListSliceDef->boundaries = lappend(lastListSliceDef->boundaries, boundary);
            }
        } else if (distType == DISTTYPE_RANGE) {
            rangeSliceDef = makeNode(RangePartitionDefState);
            rangeSliceDef->partitionName = pstrdup(sliceName);
            if (specified) {
                rangeSliceDef->tablespacename = get_pgxc_nodename(nodeOid, NULL);
            }
            rangeSliceDef->boundary = GetSliceBoundary(rel, boundDatum, distKeyPosList, attIsNull, true);
            sliceDefinitions = lappend(sliceDefinitions, rangeSliceDef);
        }
    }

    relation_close(pgxcSliceRel, AccessShareLock);
    list_free_ext(sliceList);

    DistState* distState = BuildDistStateHelper(distType, sliceDefinitions);

    return distState;
}

DistributeBy* getTableDistribution(Oid srcRelid)
{
    Relation rel;
    HeapTuple tuple;
    Form_pgxc_class classForm;
    List* distKeyPosList = NULL;
    DistributeBy* result = makeNode(DistributeBy);

    rel = relation_open(srcRelid, AccessShareLock);

    tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(srcRelid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed on distribution relation %u", srcRelid)));
    }
    classForm = (Form_pgxc_class)GETSTRUCT(tuple);
    switch (classForm->pclocatortype) {
        case LOCATOR_TYPE_HASH:  // hash
            result->disttype = DISTTYPE_HASH;
            break;
        case LOCATOR_TYPE_REPLICATED:  // replication
            result->disttype = DISTTYPE_REPLICATION;
            break;
        case LOCATOR_TYPE_MODULO:  // module
            result->disttype = DISTTYPE_MODULO;
            break;
        case LOCATOR_TYPE_RROBIN:  // round robin
            result->disttype = DISTTYPE_ROUNDROBIN;
            break;
        case LOCATOR_TYPE_LIST:  // list
            result->disttype = DISTTYPE_LIST;
            break;
        case LOCATOR_TYPE_RANGE:  // range
            result->disttype = DISTTYPE_RANGE;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized distribution option found in source like table")));
            break;
    }
    for (int i = 0; i < classForm->pcattnum.dim1; i++) {
        result->colname = lappend(result->colname, makeString(get_attname(srcRelid, classForm->pcattnum.values[i])));
        distKeyPosList = lappend_int(distKeyPosList, classForm->pcattnum.values[i]);
    }

    if (IsLocatorDistributedBySlice(classForm->pclocatortype)) {
        result->distState = BuildDistState(rel, distKeyPosList, result->disttype);
    }

    ReleaseSysCache(tuple);
    list_free_ext(distKeyPosList);
    relation_close(rel, AccessShareLock);

    return result;
}

DistributeBy* getTableHBucketDistribution(Relation rel)
{
    DistributeBy* result = NULL;
    if (REALTION_BUCKETKEY_VALID(rel)) {
        if (!RELATION_HAS_BUCKET(rel)) {
		    Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized distribution key found in source like table")));
        }
        result = makeNode(DistributeBy);
        result->disttype = DISTTYPE_HASH;
        for (int i = 0; i < rel->rd_bucketkey->bucketKey->dim1; i++) {
            result->colname = lappend(result->colname, 
                                      makeString(get_attname(rel->rd_id, rel->rd_bucketkey->bucketKey->values[i])));
        }
    }
    return result;
}


/*
 * get_pgxc_classnodes
 *		Obtain PGXC class Datanode list for given relation Oid
 *		Return number of Datanodes and their list
 *
 * Node list is returned as a palloc'd array
 */
int get_pgxc_classnodes(Oid tableid, Oid** nodes)
{
    HeapTuple tuple;
    int numnodes;
    Relation relation;
    oidvector* xc_node = NULL;
    bool isNull = false;
    relation = heap_open(PgxcClassRelationId, AccessShareLock);
    tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(tableid));
    if (!HeapTupleIsValid(tuple)) {
        if (!is_sys_table(tableid)) {
            Relation rel = relation_open(tableid, NoLock);
            StringInfo relName = makeStringInfo();
            appendStringInfo(relName, "%s", RelationGetRelationName(rel));
            relation_close(rel, NoLock);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The relation \"%s\" has no distribute type.", relName->data)));
        } else {
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", tableid)));
        }
    }
    /*
     * After cluster resizeing, the node group is dropped, but the temp table's info in pg_class and
     * pgxc_class still exists, so we should do check here.
     */
    (void)checkGroup(tableid, false);
    Datum xc_node_datum = heap_getattr(tuple, Anum_pgxc_class_nodes, RelationGetDescr(relation), &isNull);
    if (isNull) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("Can't get nodeoid for relation %s", RelationGetRelationName(relation))));
    }
    xc_node = (oidvector*)PG_DETOAST_DATUM(xc_node_datum);
    numnodes = (int)xc_node->dim1;
    if (nodes != NULL) {
        errno_t rc = EOK;
        *nodes = (Oid*)palloc(numnodes * sizeof(Oid));
        rc = memcpy_s(*nodes, numnodes * sizeof(Oid), xc_node->values, numnodes * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }
    if (xc_node != (oidvector*)DatumGetPointer(xc_node_datum)) {
        pfree_ext(xc_node);
    }
    ReleaseSysCache(tuple);
    heap_close(relation, AccessShareLock);
    return numnodes;
}

/*
 * get_pgxc_class_groupoid
 *		Obtain PGXC class groupoid by tableoid
 */
Oid get_pgxc_class_groupoid(Oid tableoid)
{
    HeapTuple tuple;
    Relation relation;
    bool isNull = false;
    char* groupname = NULL;
    Oid groupoid = InvalidOid;

    relation = heap_open(PgxcClassRelationId, AccessShareLock);
    tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(tableoid));
    if (!HeapTupleIsValid(tuple)) {
        if (!is_sys_table(tableoid)) {
            Relation rel = relation_open(tableoid, NoLock);
            StringInfo relName = makeStringInfo();
            appendStringInfo(relName, "%s", RelationGetRelationName(rel));
            relation_close(rel, NoLock);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The relation \"%s\" has no distribute type.", relName->data)));
        } else {
            /* system tables are deemed to be under installation group */
            groupname = PgxcGroupGetInstallationGroup();
            if (groupname == NULL) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("There is no installation group for system catalogs!")));
            }
            isNull = false;
        }
    } else {
        groupname =
            NameStr(*(DatumGetName(heap_getattr(tuple, Anum_pgxc_class_pgroup, RelationGetDescr(relation), &isNull))));
    }
    /* We should have table created in nodegroup */
    Assert(groupname != NULL && !isNull);
    groupoid = get_pgxc_groupoid(groupname);
    if (HeapTupleIsValid(tuple)) {
        ReleaseSysCache(tuple);
    }
    heap_close(relation, AccessShareLock);
    return groupoid;
}

/*
 * @Description :  Check curent table in pgxc_class
 * @in tableoid - table oid
 * @return - bool
 */
bool is_pgxc_class_table(Oid tableoid)
{
    HeapTuple tuple;
    Relation relation;
    bool result = true;
    relation = heap_open(PgxcClassRelationId, AccessShareLock);
    tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(tableoid));
    if (!HeapTupleIsValid(tuple)) {
        result = false;
    } else {
        result = true;
        ReleaseSysCache(tuple);
    }
    heap_close(relation, AccessShareLock);
    return result;
}

Oid get_resource_pool_oid(const char* poolname)
{
    return GetSysCacheOid1(PGXCRESOURCEPOOLNAME, PointerGetDatum(poolname));
}

char* get_resource_pool_name(Oid poolid)
{
    HeapTuple tuple;
    Form_pg_resource_pool poolForm;
    char* result = NULL;
    tuple = SearchSysCache1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(poolid));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }
    poolForm = (Form_pg_resource_pool)GETSTRUCT(tuple);
    result = pstrdup(NameStr(poolForm->respool_name));
    ReleaseSysCache(tuple);
    return result;
}

Form_pg_resource_pool get_resource_pool_param(Oid poolid, Form_pg_resource_pool rp)
{
    HeapTuple tuple;
    Form_pg_resource_pool poolForm;
    bool isnull = false;
    Oid parentid = InvalidOid;
    int io_limits_value = 0;
    char* io_priority_value = NULL;
    char* node_group = NULL;
    bool is_foreign = false;
    int nRet = 0;
    tuple = SearchSysCache1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(poolid));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }
    poolForm = (Form_pg_resource_pool)GETSTRUCT(tuple);
    Datum ParentDatum = SysCacheGetAttr(PGXCRESOURCEPOOLOID, tuple, Anum_pg_resource_pool_parent, &isnull);
    if (!isnull) {
        parentid = DatumGetObjectId(ParentDatum);
    }
    Datum IOLimitsDatum = SysCacheGetAttr(PGXCRESOURCEPOOLOID, tuple, Anum_pg_resource_pool_iops_limits, &isnull);
    if (!isnull) {
        io_limits_value = DatumGetInt32(IOLimitsDatum);
    }
    Datum IOPrioityDatum = SysCacheGetAttr(PGXCRESOURCEPOOLOID, tuple, Anum_pg_resource_pool_io_priority, &isnull);
    if (!isnull) {
        io_priority_value = DatumGetCString(DirectFunctionCall1(nameout, IOPrioityDatum));
    }
    Datum NodeGroupDatum = SysCacheGetAttr(PGXCRESOURCEPOOLOID, tuple, Anum_pg_resource_pool_nodegroup, &isnull);
    if (!isnull) {
        node_group = DatumGetCString(DirectFunctionCall1(nameout, NodeGroupDatum));
    }
    Datum IsForeignDatum = SysCacheGetAttr(PGXCRESOURCEPOOLOID, tuple, Anum_pg_resource_pool_is_foreign, &isnull);
    if (!isnull) {
        is_foreign = DatumGetBool(IsForeignDatum);
    }
    if (rp) {
        nRet = memcpy_s(NameStr(rp->respool_name), NAMEDATALEN, NameStr(poolForm->respool_name), NAMEDATALEN);
        securec_check(nRet, "\0", "\0");
        rp->mem_percent = poolForm->mem_percent;
        rp->active_statements = poolForm->active_statements;
        rp->max_dop = poolForm->max_dop;
        rp->cpu_affinity = poolForm->cpu_affinity;
        nRet = memcpy_s(NameStr(rp->control_group), NAMEDATALEN, NameStr(poolForm->control_group), NAMEDATALEN);
        securec_check(nRet, "\0", "\0");
        nRet = memcpy_s(NameStr(rp->memory_limit), NAMEDATALEN, NameStr(poolForm->memory_limit), NAMEDATALEN);
        securec_check(nRet, "\0", "\0");
        rp->parentid = parentid;
        rp->iops_limits = io_limits_value;
        if (io_priority_value != NULL) {
            errno_t errval = strncpy_s(NameStr(rp->io_priority), NAMEDATALEN, io_priority_value, NAMEDATALEN - 1);
            securec_check_errval(errval,, LOG);
            pfree_ext(io_priority_value);
        } else {
            errno_t errval = strncpy_s(NameStr(rp->io_priority), NAMEDATALEN, "None", NAMEDATALEN - 1);
            securec_check_errval(errval,, LOG);
        }
        if (node_group != NULL) {
            errno_t errval = strncpy_s(NameStr(rp->nodegroup), NAMEDATALEN, node_group, NAMEDATALEN - 1);
            securec_check_errval(errval,, LOG);
            pfree_ext(node_group);
        } else {
            errno_t errval = strncpy_s(NameStr(rp->nodegroup), NAMEDATALEN, "installation", NAMEDATALEN - 1);
            securec_check_errval(errval,, LOG);
        }
        rp->is_foreign = is_foreign;
    }
    ReleaseSysCache(tuple);
    return rp;
}

char* get_resource_pool_ngname(Oid poolid)
{
    HeapTuple tuple;
    char* result = NULL;
    Relation rel;
    bool isNull = false;
    Datum groupanme_datum;
    rel = heap_open(ResourcePoolRelationId, AccessShareLock);
    if (!rel) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("can not open pg_resource_pool")));
    }
    tuple = SearchSysCache1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(poolid));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }
    groupanme_datum = heap_getattr(tuple, Anum_pg_resource_pool_nodegroup, RelationGetDescr(rel), &isNull);
    if (!isNull) {
        result = (char*)pstrdup((const char*)DatumGetCString(groupanme_datum));
    } else {
        result = (char*)pstrdup(DEFAULT_NODE_GROUP);
    }
    ReleaseSysCache(tuple);
    heap_close(rel, AccessShareLock);
    return result;
}

/*
 * is_resource_pool_foreign
 *		Determine if resource pool is foreign one
 */
bool is_resource_pool_foreign(Oid poolid)
{
    HeapTuple tuple;
    bool result = false;
    Relation rel;
    bool isNull = false;
    Datum datum;
    rel = heap_open(ResourcePoolRelationId, AccessShareLock);
    if (!rel) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("can not open pg_resource_pool")));
    }
    tuple = SearchSysCache1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(poolid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for resource pool %u", poolid)));
    }
    datum = heap_getattr(tuple, Anum_pg_resource_pool_is_foreign, RelationGetDescr(rel), &isNull);
    if (!isNull) {
        result = DatumGetBool(datum);
    } else {
        result = false;
    }
    ReleaseSysCache(tuple);
    heap_close(rel, AccessShareLock);

    return result;
}

Oid get_workload_group_oid(const char* groupname)
{
    return GetSysCacheOid1(PGXCWORKLOADGROUPNAME, PointerGetDatum(groupname));
}

char* get_workload_group_name(Oid groupid)
{
    HeapTuple tuple;
    Form_pg_workload_group groupForm;
    char* result = NULL;
    tuple = SearchSysCache1(PGXCWORKLOADGROUPOID, ObjectIdGetDatum(groupid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for workload group %u", groupid)));
    }
    groupForm = (Form_pg_workload_group)GETSTRUCT(tuple);
    result = pstrdup(NameStr(groupForm->workload_gpname));
    ReleaseSysCache(tuple);
    return result;
}

Oid get_application_mapping_oid(const char* appname)
{
    return GetSysCacheOid1(PGXCAPPWGMAPPINGNAME, PointerGetDatum(appname));
}

char* get_application_mapping_name(Oid appoid)
{
    HeapTuple tuple;
    Form_pg_app_workloadgroup_mapping mappingForm;
    char* result = NULL;
    tuple = SearchSysCache1(PGXCAPPWGMAPPINGOID, ObjectIdGetDatum(appoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for application %u", appoid)));
    }
    mappingForm = (Form_pg_app_workloadgroup_mapping)GETSTRUCT(tuple);
    result = pstrdup(NameStr(mappingForm->appname));
    ReleaseSysCache(tuple);
    return result;
}

#endif

/*
 * get_typavgwidth
 *
 *	  Given a type OID and a typmod value (pass -1 if typmod is unknown),
 *	  estimate the average width of values of the type.  This is used by
 *	  the planner, which doesn't require absolutely correct results;
 *	  it's OK (and expected) to guess if we don't know for sure.
 */
int32 get_typavgwidth(Oid typid, int32 typmod)
{
    int typlen = get_typlen(typid);
    int32 maxwidth;
    /*
     * Easy if it's a fixed-width type
     */
    if (typlen > 0) {
        return typlen;
    }
    /*
     * type_maximum_size knows the encoding of typmod for some datatypes;
     * don't duplicate that knowledge here.
     */
    maxwidth = type_maximum_size(typid, typmod);
    if (maxwidth > 0) {
        /*
         * For BPCHAR, the max width is also the only width.  Otherwise we
         * need to guess about the typical data width given the max. A sliding
         * scale for percentage of max width seems reasonable.
         */
        if (typid == BPCHAROID) {
            return maxwidth;
        }
        if (maxwidth <= 32) {
            return maxwidth; /* assume full width */
        }
        if (maxwidth < 1000) {
            return 32 + (maxwidth - 32) / 2; /* assume 50% */
        }
        /*
         * Beyond 1000, assume we're looking at something like
         * "varchar(10000)" where the limit isn't actually reached often, and
         * use a fixed estimate.
         */
        return 32 + (1000 - 32) / 2;
    }
    /*
     * Ooops, we have no idea ... wild guess time.
     */
    return 32;
}

/*
 * get_typtype
 *
 *		Given the type OID, find if it is a basic type, a complex type, etc.
 *		It returns the null char if the cache lookup fails...
 */
char get_typtype(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        char result;
        result = typtup->typtype;
        ReleaseSysCache(tp);
        return result;
    } else {
        return '\0';
    }
}

/*
 * type_is_rowtype
 *
 *		Convenience function to determine whether a type OID represents
 *		a "rowtype" type --- either RECORD or a named composite type.
 */
bool type_is_rowtype(Oid typid)
{
    return (typid == RECORDOID || get_typtype(typid) == TYPTYPE_COMPOSITE);
}

/*
 * type_is_enum
 *	  Returns true if the given type is an enum type.
 */
bool type_is_enum(Oid typid)
{
    return (get_typtype(typid) == TYPTYPE_ENUM);
}

/*
 * type_is_range
 *	  Returns true if the given type is a range type.
 */
bool type_is_range(Oid typid)
{
    return (get_typtype(typid) == TYPTYPE_RANGE);
}

/*
 * get_type_category_preferred
 *
 *		Given the type OID, fetch its category and preferred-type status.
 *		Throws error on failure.
 */
void get_type_category_preferred(Oid typid, char* typcategory, bool* typispreferred)
{
    HeapTuple tp;
    Form_pg_type typtup;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typtup = (Form_pg_type)GETSTRUCT(tp);
    *typcategory = typtup->typcategory;
    *typispreferred = typtup->typispreferred;
    ReleaseSysCache(tp);
}

/*
 * get_typ_typrelid
 *
 *		Given the type OID, get the typrelid (InvalidOid if not a complex
 *		type).
 */
Oid get_typ_typrelid(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        Oid result;
        result = typtup->typrelid;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_element_type
 *
 *		Given the type OID, get the typelem (InvalidOid if not an array type).
 *
 * NB: this only considers varlena arrays to be true arrays; InvalidOid is
 * returned if the input is a fixed-length array type.
 */
Oid get_element_type(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        Oid result;
        if (typtup->typlen == -1) {
            result = typtup->typelem;
        } else {
            result = InvalidOid;
        }
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_array_type
 *
 *		Given the type OID, get the corresponding "true" array type.
 *		Returns InvalidOid if no array type can be found.
 */
Oid get_array_type(Oid typid)
{
    HeapTuple tp;
    Oid result = InvalidOid;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        result = ((Form_pg_type)GETSTRUCT(tp))->typarray;
        ReleaseSysCache(tp);
    }
    return result;
}

/*
 * get_base_element_type
 *		Given the type OID, get the typelem, looking "through" any domain
 *		to its underlying array type.
 *
 * This is equivalent to get_element_type(getBaseType(typid)), but avoids
 * an extra cache lookup.  Note that it fails to provide any information
 * about the typmod of the array.
 */
Oid get_base_element_type(Oid typid)
{
    /*
     * We loop to find the bottom base type in a stack of domains.
     */
    for (;;) {
        HeapTuple tup;
        Form_pg_type typTup;
        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if (!HeapTupleIsValid(tup)) {
            break;
        }
        typTup = (Form_pg_type)GETSTRUCT(tup);
        if (typTup->typtype != TYPTYPE_DOMAIN) {
            /* Not a domain, so stop descending */
            Oid result;
            /* This test must match get_element_type */
            if (typTup->typlen == -1) {
                result = typTup->typelem;
            } else {
                result = InvalidOid;
            }
            ReleaseSysCache(tup);
            return result;
        }
        typid = typTup->typbasetype;
        ReleaseSysCache(tup);
    }
    /* Like get_element_type, silently return InvalidOid for bogus input */
    return InvalidOid;
}

/*
 * getTypeInputInfo
 *
 *		Get info needed for converting values of a type to internal form
 */
void getTypeInputInfo(Oid type, Oid* typInput, Oid* typIOParam)
{
    HeapTuple typeTuple;
    Form_pg_type pt;
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", type)));
    }
    pt = (Form_pg_type)GETSTRUCT(typeTuple);

    if (!pt->typisdefined) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    }
    if (!OidIsValid(pt->typinput)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("no input function available for type %s", format_type_be(type))));
    }
    *typInput = pt->typinput;
    *typIOParam = getTypeIOParam(typeTuple);
    ReleaseSysCache(typeTuple);
}

/*
 * getTypeOutputInfo
 *
 *		Get info needed for printing values of a type
 */
void getTypeOutputInfo(Oid type, Oid* typOutput, bool* typIsVarlena)
{
    HeapTuple typeTuple;
    Form_pg_type pt;
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", type)));
    }
    pt = (Form_pg_type)GETSTRUCT(typeTuple);
    if (!pt->typisdefined) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    }
    if (!OidIsValid(pt->typoutput)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("no output function available for type %s", format_type_be(type))));
    }
    *typOutput = pt->typoutput;
    *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
    ReleaseSysCache(typeTuple);
}

/*
 * getTypeBinaryInputInfo
 *
 *		Get info needed for binary input of values of a type
 */
void getTypeBinaryInputInfo(Oid type, Oid* typReceive, Oid* typIOParam)
{
    HeapTuple typeTuple;
    Form_pg_type pt;
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", type)));
    }
    pt = (Form_pg_type)GETSTRUCT(typeTuple);
    if (!pt->typisdefined) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    }
    if (!OidIsValid(pt->typreceive)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("no binary input function available for type %s", format_type_be(type))));
    }
    *typReceive = pt->typreceive;
    *typIOParam = getTypeIOParam(typeTuple);
    ReleaseSysCache(typeTuple);
}

/*
 * getTypeBinaryOutputInfo
 *
 *		Get info needed for binary output of values of a type
 */
void getTypeBinaryOutputInfo(Oid type, Oid* typSend, bool* typIsVarlena)
{
    HeapTuple typeTuple;
    Form_pg_type pt;
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", type)));
    }
    pt = (Form_pg_type)GETSTRUCT(typeTuple);
    if (!pt->typisdefined) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    }
    if (!OidIsValid(pt->typsend)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("no binary output function available for type %s", format_type_be(type))));
    }
    *typSend = pt->typsend;
    *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
    ReleaseSysCache(typeTuple);
}

/*
 * get_typmodin
 *
 *		Given the type OID, return the type's typmodin procedure, if any.
 */
Oid get_typmodin(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        Oid result;
        result = typtup->typmodin;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

#ifdef NOT_USED
/*
 * get_typmodout
 *
 *		Given the type OID, return the type's typmodout procedure, if any.
 */
Oid get_typmodout(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        Oid result;
        result = typtup->typmodout;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}
#endif /* NOT_USED */

/*
 * get_typcollation
 *
 *		Given the type OID, return the type's typcollation attribute.
 */
Oid get_typcollation(Oid typid)
{
    HeapTuple tp;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
        Oid result;
        result = typtup->typcollation;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * type_is_collatable
 *
 *		Return whether the type cares about collations
 */
bool type_is_collatable(Oid typid)
{
    return OidIsValid(get_typcollation(typid));
}

/*
 * get_attavgwidth
 *
 *	  Given the table and attribute number of a column, get the average
 *	  width of entries in the column.  Return zero if no data available.
 *
 * Currently this is only consulted for individual tables, not for inheritance
 * trees, so we don't need an "inh" parameter.
 *
 * Calling a hook at this point looks somewhat strange, but is required
 * because the optimizer calls this function without any other way for
 * plug-ins to control the result.
 */
int32 get_attavgwidth(Oid relid, AttrNumber attnum, bool ispartition)
{
    HeapTuple tp;
    int32 stawidth;
    char stakind = ispartition ? STARELKIND_PARTITION : STARELKIND_CLASS;

    if (!ispartition && get_rel_persistence(relid) == RELPERSISTENCE_GLOBAL_TEMP) {
        tp = get_gtt_att_statistic(relid, attnum);
        if (!HeapTupleIsValid(tp)) {
            return 0;
        }
        stawidth = ((Form_pg_statistic) GETSTRUCT(tp))->stawidth;
        if (stawidth > 0) {
            return stawidth;
        } else {
            return 0;
        }
    }
    tp = SearchSysCache4(
        STATRELKINDATTINH, ObjectIdGetDatum(relid), CharGetDatum(stakind), Int16GetDatum(attnum), BoolGetDatum(false));

    if (HeapTupleIsValid(tp)) {
        stawidth = ((Form_pg_statistic)GETSTRUCT(tp))->stawidth;
        ReleaseSysCache(tp);
        if (stawidth > 0)
            return stawidth;
    }
    return 0;
}

/*
 * For global stat: get stadndistinct from pg_statistic.
 */
double get_attstadndistinct(HeapTuple statstuple)
{
    Datum val;
    bool isnull = false;

    val = SysCacheGetAttr(STATRELKINDATTINH, statstuple, Anum_pg_statistic_stadndistinct, &isnull);
    if (isnull) {
        return 0;
    } else {
        return DatumGetFloat4(val);
    }
}

static void get_attstatsslotnumber(HeapTuple statstuple, float4** numbers, int* nnumbers, int idx)
{
    bool isnull = false;
    ArrayType* statarray = NULL;
    int narrayelem;
    Datum val = SysCacheGetAttr(STATRELKINDATTINH, statstuple, Anum_pg_statistic_stanumbers1 + idx, &isnull);
    if (isnull) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("stanumbers is null")));
    }
    statarray = DatumGetArrayTypeP(val);
    /*
     * We expect the array to be a 1-D float4 array; verify that. We don't
     * need to use deconstruct_array() since the array data is just going
     * to look like a C array of float4 values.
     */
    narrayelem = ARR_DIMS(statarray)[0];
    if (ARR_NDIM(statarray) != 1 || narrayelem <= 0 || ARR_HASNULL(statarray) ||
        ARR_ELEMTYPE(statarray) != FLOAT4OID) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("stanumbers is not a 1-D float4 array")));
    }
    *numbers = (float4*)palloc(narrayelem * sizeof(float4));
    int rc = memcpy_s(*numbers, narrayelem * sizeof(float4), ARR_DATA_PTR(statarray), narrayelem * sizeof(float4));
    securec_check(rc, "\0", "\0");
    *nnumbers = narrayelem;
    /*
     * Free statarray if it's a detoasted copy.
     */
    if ((Pointer)statarray != DatumGetPointer(val)) {
        pfree_ext(statarray);
    }
}

/*
 * get_attstatsslot
 *
 *		Extract the contents of a "slot" of a pg_statistic tuple.
 *		Returns TRUE if requested slot type was found, else FALSE.
 *
 * Unlike other routines in this file, this takes a pointer to an
 * already-looked-up tuple in the pg_statistic cache.  We do this since
 * most callers will want to extract more than one value from the cache
 * entry, and we don't want to repeat the cache lookup unnecessarily.
 * Also, this API allows this routine to be used with statistics tuples
 * that have been provided by a stats hook and didn't really come from
 * pg_statistic.
 *
 * statstuple: pg_statistics tuple to be examined.
 * atttype: type OID of slot's stavalues (can be InvalidOid if values == NULL).
 * atttypmod: typmod of slot's stavalues (can be 0 if values == NULL).
 * reqkind: STAKIND code for desired statistics slot kind.
 * reqop: STAOP value wanted, or InvalidOid if don't care.
 * actualop: if not NULL, *actualop receives the actual STAOP value.
 * values, nvalues: if not NULL, the slot's stavalues are extracted.
 * numbers, nnumbers: if not NULL, the slot's stanumbers are extracted.
 *
 * If assigned, values and numbers are set to point to palloc'd arrays.
 * If the stavalues datatype is pass-by-reference, the values referenced by
 * the values array are themselves palloc'd.  The palloc'd stuff can be
 * freed by calling free_attstatsslot.
 *
 * Note: at present, atttype/atttypmod aren't actually used here at all.
 * But the caller must have the correct (or at least binary-compatible)
 * type ID to pass to free_attstatsslot later.
 */
bool get_attstatsslot(HeapTuple statstuple, Oid atttype, int32 atttypmod, int reqkind, Oid reqop, Oid* actualop,
    Datum** values, int* nvalues, float4** numbers, int* nnumbers)
{
    Form_pg_statistic stats = (Form_pg_statistic)GETSTRUCT(statstuple);
    int i, j;
    Datum val;
    bool isnull = false;
    ArrayType* statarray = NULL;
    Oid arrayelemtype;
    HeapTuple typeTuple;
    Form_pg_type typeForm;

    for (i = 0; i < STATISTIC_NUM_SLOTS; ++i) {
        if ((&stats->stakind1)[i] == reqkind && (reqop == InvalidOid || (&stats->staop1)[i] == reqop)) {
            break;
        }
    }
    if (i >= STATISTIC_NUM_SLOTS) {
        return false; /* not there */
    }
    if (actualop != NULL) {
        *actualop = (&stats->staop1)[i];
    }
    if (values != NULL) {
        val = SysCacheGetAttr(STATRELKINDATTINH, statstuple, Anum_pg_statistic_stavalues1 + i, &isnull);
        if (isnull) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("stavalues is null")));
        }
        statarray = DatumGetArrayTypeP(val);
        /*
         * Need to get info about the array element type.  We look at the
         * actual element type embedded in the array, which might be only
         * binary-compatible with the passed-in atttype.  The info we extract
         * here should be the same either way, but deconstruct_array is picky
         * about having an exact type OID match.
         */
        arrayelemtype = ARR_ELEMTYPE(statarray);
        typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(arrayelemtype));
        if (!HeapTupleIsValid(typeTuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", arrayelemtype)));
        }
        typeForm = (Form_pg_type)GETSTRUCT(typeTuple);
        /* Deconstruct array into Datum elements; NULLs not expected */
        deconstruct_array(
            statarray, arrayelemtype, typeForm->typlen, typeForm->typbyval, typeForm->typalign, values, NULL, nvalues);
        /*
         * If the element type is pass-by-reference, we now have a bunch of
         * Datums that are pointers into the syscache value.  Copy them to
         * avoid problems if syscache decides to drop the entry.
         */
        if (!typeForm->typbyval) {
            for (j = 0; j < *nvalues; j++) {
                (*values)[j] = datumCopy((*values)[j], typeForm->typbyval, typeForm->typlen);
            }
        }
        ReleaseSysCache(typeTuple);
        /*
         * Free statarray if it's a detoasted copy.
         */
        if ((Pointer)statarray != DatumGetPointer(val)) {
            pfree_ext(statarray);
        }
    }

    if (numbers != NULL) {
        get_attstatsslotnumber(statstuple, numbers, nnumbers, i);
    }
    return true;
}

bool get_attmultistatsslot(HeapTuple statstuple, Oid atttype, int32 atttypmod, int reqkind, Oid reqop, Oid* actualop,
    Datum** values, int* nvalues, float4** numbers, int* nnumbers, bool** nulls)
{
    Form_pg_statistic_ext stats = (Form_pg_statistic_ext)GETSTRUCT(statstuple);
    Datum val;
    bool isnull = false;
    ArrayType* statarray = NULL;
    Oid arrayelemtype;
    int narrayelem;
    HeapTuple typeTuple;
    Form_pg_type typeForm;
    int slot_index = 0;
    for (slot_index = 0; slot_index < STATISTIC_NUM_SLOTS; ++slot_index) {
        if ((&stats->stakind1)[slot_index] == reqkind && (reqop == InvalidOid ||
            (&stats->staop1)[slot_index] == reqop)) {
            break;
        }
    }
    if (slot_index >= STATISTIC_NUM_SLOTS) {
        return false; /* not there */
    }
    if (actualop != NULL) {
        *actualop = (&stats->staop1)[slot_index];
    }
    if (values != NULL) {
        val = SysCacheGetAttr(STATRELKINDKEYINH, statstuple, Anum_pg_statistic_ext_stavalues1 + slot_index, &isnull);

        if (isnull) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("stavalues is null")));
        }
        statarray = DatumGetArrayTypeP(val);
        /*
         * Need to get info about the array element type.  We look at the
         * actual element type embedded in the array, which might be only
         * binary-compatible with the passed-in atttype.  The info we extract
         * here should be the same either way, but deconstruct_array is picky
         * about having an exact type OID match.
         */
        arrayelemtype = ARR_ELEMTYPE(statarray);
        typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(arrayelemtype));
        if (!HeapTupleIsValid(typeTuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", arrayelemtype)));
        }
        typeForm = (Form_pg_type)GETSTRUCT(typeTuple);
        /* Deconstruct array into Datum elements; NULLs not expected */
        deconstruct_array(
            statarray, arrayelemtype, typeForm->typlen, typeForm->typbyval, typeForm->typalign, values, NULL, nvalues);
        Datum* all_attr_values = NULL;
        bool* all_attr_nulls = NULL;
        if (reqkind == STATISTIC_KIND_MCV || reqkind == STATISTIC_KIND_NULL_MCV) {
            int attr_num = *nvalues;
            for (int i = 0; i < attr_num; ++i) {
                ArrayType* attr_value_array = DatumGetArrayTypeP((*values)[i]);
                Oid attr_type = ARR_ELEMTYPE(attr_value_array);
                HeapTuple attr_type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(attr_type));
                if (!HeapTupleIsValid(attr_type_tuple)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", attr_type)));
                }
                Form_pg_type attr_type_form = (Form_pg_type)GETSTRUCT(attr_type_tuple);
                Datum* attr_values = NULL;
                bool* attr_nulls = NULL;
                int attr_nvalues = 0;
                deconstruct_array(attr_value_array,
                    attr_type,
                    attr_type_form->typlen,
                    attr_type_form->typbyval,
                    attr_type_form->typalign,
                    &attr_values,
                    &attr_nulls,
                    &attr_nvalues);
                if (all_attr_values == NULL) {
                    all_attr_values = (Datum*)palloc0(sizeof(Datum) * attr_nvalues * attr_num);
                    if (reqkind == STATISTIC_KIND_NULL_MCV)
                        all_attr_nulls = (bool*)palloc0(sizeof(bool) * attr_nvalues * attr_num);
                    *nvalues = attr_nvalues * attr_num;
                }
                for (int j = 0; j < attr_nvalues; ++j) {
                    if (all_attr_nulls != NULL) {
                        all_attr_nulls[attr_nvalues * i + j] = attr_nulls[j];
                    }
                    if (!attr_nulls[j]) {
                        all_attr_values[attr_nvalues * i + j] =
                            attr_type_form->typbyval
                                ? attr_values[j]
                                : datumCopy(attr_values[j], attr_type_form->typbyval, attr_type_form->typlen);
                    }
                }

                ReleaseSysCache(attr_type_tuple);
                if ((Pointer)attr_value_array != DatumGetPointer((*values)[i])) {
                    pfree_ext(attr_value_array);
                }
                pfree_ext(attr_values);
                pfree_ext(attr_nulls);
            }
        }
        /*
         * If the element type is pass-by-reference, we now have a bunch of
         * Datums that are pointers into the syscache value.  Copy them to
         * avoid problems if syscache decides to drop the entry.
         */
        if (all_attr_values != NULL) {
            *values = all_attr_values;
            if (nulls != NULL) {
                *nulls = all_attr_nulls;
            }
        }
        ReleaseSysCache(typeTuple);
        /*
         * Free statarray if it's a detoasted copy.
         */
        if ((Pointer)statarray != DatumGetPointer(val)) {
            pfree_ext(statarray);
        }
    }
    if (numbers != NULL) {
        val = SysCacheGetAttr(STATRELKINDKEYINH, statstuple, Anum_pg_statistic_ext_stanumbers1 + slot_index, &isnull);
        if (isnull) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("stanumbers is null")));
        }
        statarray = DatumGetArrayTypeP(val);
        /*
         * We expect the array to be a 1-D float4 array; verify that. We don't
         * need to use deconstruct_array() since the array data is just going
         * to look like a C array of float4 values.
         */
        narrayelem = ARR_DIMS(statarray)[0];
        if (ARR_NDIM(statarray) != 1 || narrayelem <= 0 || ARR_HASNULL(statarray) ||
            ARR_ELEMTYPE(statarray) != FLOAT4OID) {
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("stanumbers is not a 1-D float4 array")));
        }
        *numbers = (float4*)palloc(narrayelem * sizeof(float4));
        int rc = memcpy_s(*numbers, narrayelem * sizeof(float4), ARR_DATA_PTR(statarray), narrayelem * sizeof(float4));
        securec_check(rc, "\0", "\0");
        *nnumbers = narrayelem;
        /*
         * Free statarray if it's a detoasted copy.
         */
        if ((Pointer)statarray != DatumGetPointer(val)) {
            pfree_ext(statarray);
        }
    }
    return true;
}

/*
 * free_attstatsslot
 *		Free data allocated by get_attstatsslot
 *
 * atttype is the type of the individual values in values[].
 * It need be valid only if values != NULL.
 */
void free_attstatsslot(Oid atttype, Datum* values, int nvalues, float4* numbers, int nnumbers)
{
    if (values != NULL) {
        if (!get_typbyval(atttype)) {
            int i;
            for (i = 0; i < nvalues; i++) {
                pfree(DatumGetPointer(values[i]));
            }
        }
        pfree_ext(values);
    }
    if (numbers != NULL) {
        pfree_ext(numbers);
    }
}

/*
 * get_namespace_name
 *		Returns the name of a given namespace
 *
 * Returns a palloc'd copy of the string, or NULL if no such namespace.
 */
char* get_namespace_name(Oid nspid)
{
    HeapTuple tp;

    tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
        char* result = NULL;

        result = pstrdup(NameStr(nsptup->nspname));
        ReleaseSysCache(tp);
        return result;
    } else
        return NULL;
}

/*				---------- PG_RANGE CACHE ----------				 */

/*
 * get_range_subtype
 *		Returns the subtype of a given range type
 *
 * Returns InvalidOid if the type is not a range type.
 */
Oid get_range_subtype(Oid rangeOid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(rangeOid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_range rngtup = (Form_pg_range)GETSTRUCT(tp);
        Oid result;
        result = rngtup->rngsubtype;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_cfgnamespace
 *		Returns the namespace of a given cfgid
 *
 */
char* get_cfgnamespace(Oid cfgid)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    char* result = NULL;
    tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", cfgid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    result = pstrdup(get_namespace_name(typeForm->typnamespace));
    ReleaseSysCache(tuple);
    return result;
}

/*
 * get_range_subtype
 *		Returns the namespace of a given typeid
 *
 */
char* get_typenamespace(Oid typid)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    char* result = NULL;
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    result = pstrdup(get_namespace_name(typeForm->typnamespace));
    ReleaseSysCache(tuple);
    return result;
}
/*
 * get_range_subtype
 *		Returns the namespace of the enumtype corresponding to the given enumtype labelid
 */
char* get_enumtypenamespace(Oid enumlabelid)
{
    HeapTuple tuple;
    Form_pg_enum en;
    Form_pg_type typeForm;
    Oid enumtypid = InvalidOid;
    char* result = NULL;
    tuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumlabelid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for enumlabelid %u", enumlabelid)));
    }
    en = (Form_pg_enum)GETSTRUCT(tuple);
    enumtypid = en->enumtypid;
    ReleaseSysCache(tuple);
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(enumtypid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for enumtype %u", enumtypid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    result = get_namespace_name(typeForm->typnamespace);
    ReleaseSysCache(tuple);
    return result;
}

Oid get_typeoid(Oid namespaceId, const char* typname)
{
    Oid result;
    result = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId));
    if (OidIsValid(result)) {
        return result;
    }
    /* Not found in path */
    return InvalidOid;
}

Oid get_enumlabeloid(Oid enumtypoid, const char* enumlabelname)
{
    Oid result;
    HeapTuple tup;
    tup = SearchSysCache2(ENUMTYPOIDNAME, ObjectIdGetDatum(enumtypoid), CStringGetDatum(enumlabelname));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input value for enum %s: \"%s\"", format_type_be(enumtypoid), enumlabelname)));
    }
    result = HeapTupleGetOid(tup);
    ReleaseSysCache(tup);
    return result;
}

/*
 * Returns the operator oid with the given operatorName, operatorNamespace, leftObjectId, rightObjectId
 */
Oid get_operator_oid(const char* operatorName, Oid operatorNamespace, Oid leftObjectId, Oid rightObjectId)
{
    HeapTuple tup;
    Oid operatorObjectId;
    tup = SearchSysCache(OPERNAMENSP,
        PointerGetDatum(operatorName),
        ObjectIdGetDatum(leftObjectId),
        ObjectIdGetDatum(rightObjectId),
        ObjectIdGetDatum(operatorNamespace));
    if (HeapTupleIsValid(tup)) {
        operatorObjectId = HeapTupleGetOid(tup);
        ReleaseSysCache(tup);
    } else {
        operatorObjectId = InvalidOid;
    }
    return operatorObjectId;
}

/*
 * Returns the operator name, operator namespace, left operator type oid, right operator type
 * oid of the given operator oid
 */
void get_oper_name_namespace_oprs(Oid operid, char** oprname, char** nspname, Oid* oprleft, Oid* oprright)
{
    HeapTuple opertup;
    Form_pg_operator operform;
    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
    if (!HeapTupleIsValid(opertup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", operid)));
    }
    operform = (Form_pg_operator)GETSTRUCT(opertup);
    *oprname = pstrdup(NameStr(operform->oprname));
    *nspname = get_namespace_name(operform->oprnamespace);
    *oprleft = operform->oprleft;
    *oprright = operform->oprright;
    ReleaseSysCache(opertup);
}

/* check PolymorphicType
 * If the return type is polymorphic, continue
 * For example: CREATE FUNCTION array_abs(x anyarray) RETURN anyarray ...
 * And we have SQL: select array_abs({1,-2,3,-4});
 * Obviously, the output type(output_id) is INT, not anyarray
 * So we don't check the return type.
 */
static inline bool CompareRetType(Oid inputOid, Oid outputOid)
{
    if (!IsPolymorphicType(inputOid)) {
        return inputOid == outputOid;
    }

    return true;
}

Oid get_func_oid(const char* funcname, Oid funcnamespace, Expr* expr)
{
    CatCList* catlist = NULL;
    int i;
    Oid argtypes[FUNC_MAX_ARGS] = {0};
    int nargs = 0;
    ListCell* l = NULL;
    if (expr != NULL && IsA(expr, FuncExpr)) {
        foreach (l, ((FuncExpr*)expr)->args) {
            Node* arg = (Node*)lfirst(l);

            argtypes[nargs] = exprType(arg);
            nargs++;
        }
    }
    /* Search syscache by name only */

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
    if (expr) {
        HeapTuple proctupl = NULL;
        proctupl = SearchSysCache1(PROCOID, ObjectIdGetDatum(((FuncExpr*)expr)->funcid));
        if (HeapTupleIsValid(proctupl)) {
            Form_pg_proc procform;
            char* proname = NULL;
            Datum pkgOiddatum;
            bool isnull = true;
            NameData* pkgname = NULL;
            Oid pkgOid = InvalidOid;
            procform = (Form_pg_proc)GETSTRUCT(proctupl);
            proname = NameStr(procform->proname);
            pkgOiddatum = SysCacheGetAttr(PROCOID, proctupl, Anum_pg_proc_packageid, &isnull);
            if (!isnull) {
                pkgOid = DatumGetObjectId(pkgOiddatum);
                pkgname = GetPackageName(pkgOid);
            }
            if (pkgname && OidIsValid(pkgOid)) {
                ReleaseSysCacheList(catlist);
                ReleaseSysCache(proctupl);
                return (((FuncExpr*)expr)->funcid);
            }
            ReleaseSysCache(proctupl);
        }
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
        if (OidIsValid(funcnamespace)) {
            /* Consider only procs in specified namespace */
            if (procform->pronamespace != funcnamespace) {
                continue;
            }
	    }

        if (expr != NULL && IsA(expr, FuncExpr) && procform->pronargs != list_length(((FuncExpr*)expr)->args)
            && procform->pronargs != list_length(((FuncExpr*)expr)->args) + procform->pronargdefaults
#ifdef ENABLE_MULTIPLE_NODES
            && !is_streaming_hash_group_func(funcname, funcnamespace)
#endif   /* ENABLE_MULTIPLE_NODES */
            )
            continue;

        /*
         * User will define their own function (UDF).
         * 
         * GaussDB compares the number of input parameters, the type of each parameters, and return type
         * to find the corresponding function.
         *
         *                   function(arg1, arg2, arg3)
         *                   $$
                                return ret;
                             $$
         *
         * Two Scenario:
         * 1) In MPP, same UDF in cn/dn has differnet oid.
         * 2) In MPP or SingleNode, UDF in VIEW or pg_rewrite.
         */
        if (expr != NULL && IsA(expr, FuncExpr) &&
            !CompareRetType(procform->prorettype, ((FuncExpr*)expr)->funcresulttype)) {
            /*
             * Compare the return oid of the function, if the returned Oid doesnot match,
             * it means it's not the function we want.
             */
            continue;
        }
        /*
         * Support the same function name, same number of arguments and different argument type
         */
        if (expr != NULL && IsA(expr, FuncExpr) 
#ifdef ENABLE_MULTIPLE_NODES
            && !is_streaming_hash_group_func(funcname, funcnamespace)
#endif   /* ENABLE_MULTIPLE_NODES */
            ) {
            int j = 0;
            bool matched = true;

            oidvector* proargs = ProcedureGetArgTypes(proctup);
            for (j = 0; j < nargs; j++) {
                if (!CompareRetType(proargs->values[j], argtypes[j])) {
                    matched = false;
                    break;
                }
            }

            if (!matched)
                continue;
        }

        Oid func_oid = HeapTupleGetOid(proctup);
        ReleaseSysCacheList(catlist);
        return func_oid;
    }
    ReleaseSysCacheList(catlist);
    return InvalidOid;
}

Oid get_func_oid_ext(const char* funcname, Oid funcnamespace, Oid funcrettype, int funcnargs, Oid* funcargstype)
{
    CatCList* catlist = NULL;
    int i, j;
    /* Search syscache by name only */
#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif
    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
        if (OidIsValid(funcnamespace)) {
            /* Consider only procs in specified namespace */
            if (procform->pronamespace != funcnamespace) {
                continue;
            }
        }
        if (procform->pronargs != funcnargs) {
            continue;
        }
        if (procform->prorettype != funcrettype) {
            continue;
        }
        /*
         * Support the same function name, same number of arguments and different argument type
         */
        oidvector* proargs = ProcedureGetArgTypes(proctup);
        for (j = 0; j < funcnargs; j++) {
            if (funcargstype[j] != proargs->values[j]) {
                break;
            }
        }
        if (j < funcnargs) {
            continue;
        }
        Oid func_oid = HeapTupleGetOid(proctup);
        ReleaseSysCacheList(catlist);
        return func_oid;
    }
    ReleaseSysCacheList(catlist);
    return InvalidOid;
}

Oid partid_get_parentid(Oid partid)
{
    Form_pg_partition partitionForm;
    HeapTuple partitionTup;
    Oid relid;
    /* Search the partition's relcache entry */
    partitionTup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
    if (!HeapTupleIsValid(partitionTup)) {
        return InvalidOid;
    }
    partitionForm = (Form_pg_partition)GETSTRUCT(partitionTup);
    /* Get the partitioned table's oid */
    relid = partitionForm->parentid;
    ReleaseSysCache(partitionTup);

    return relid;
}

/* function is not strict and not agg*/
bool is_not_strict_agg(Oid funcOid)
{
    HeapTuple func_tuple;
    Form_pg_proc func_form;
    func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(func_tuple)) {
        return false;
    }
    func_form = (Form_pg_proc)GETSTRUCT(func_tuple);

    if (func_form->proisstrict == false && func_form->proisagg == false) {
        ReleaseSysCache(func_tuple);
        return true;
    }
    ReleaseSysCache(func_tuple);
    return false;
}

char get_typecategory(Oid typid)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    char result;
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typeForm = (Form_pg_type)GETSTRUCT(tuple);
    result = typeForm->typcategory;
    ReleaseSysCache(tuple);
    return result;
}
