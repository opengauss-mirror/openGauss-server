/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *  vecagg.cpp
 *       Routines to handle vector aggregae nodes.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecagg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/vechashaggcodegen.h"
#include "codegen/vecsortcodegen.h"
#include "codegen/vecexprcodegen.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecfunc.h"
#include "vectorsonic/vsonichashagg.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "pgxc/pgxc.h"
#include "utils/int8.h"
#include "vecexecutor/vecagg.h"
#include "vecexecutor/vecplainagg.h"
#include "vecexecutor/vecsortagg.h"
#include "vecexecutor/vechashagg.h"

static void DispatchAggFunction(AggStatePerAgg agg_state, VecAggInfo* agg_info, bool use_sonichash = false);

extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

/*
 * @Description: set vector agg function.
 */
static void DispatchAggFunction(AggStatePerAgg agg_state, VecAggInfo* agg_info, bool use_sonichash)
{
    VecFuncCacheEntry* entry = NULL;
    Oid aggfnoid = agg_state->aggref->aggfnoid;
    bool found = false;

    /*
     * In pg_aggregate.h,  one aggtransfn  may by used  more than once,
     * we can not find the actual function in g_instance.vec_func_hash by agg_state->transfn_oid.
     * eg:
     * DATA(insert ( 2104	float4_accum	float8_collect	float8_avg		0	1022	"{0,0,0}" "{0,0,0}" ));
     * DATA(insert ( 2644	float4_accum	float8_collect	float8_var_samp 0		1022	"{0,0,0}" "{0,0,0}" ));
     * Both avg(float4) and sttdev_samp(float4) using float4_accum, but final func is not equal.
     * So aggfnoid is unique and can be used directly.
     */
    entry = (VecFuncCacheEntry*)hash_search(g_instance.vec_func_hash, &aggfnoid, HASH_FIND, &found);

    if (found) {
        if (use_sonichash) {
            agg_info->vec_agg_cache = &entry->vec_sonic_agg_cache[0];
            agg_info->vec_agg_final = &entry->vec_sonic_transform_function[0];
        } else {
            agg_info->vec_agg_cache = &entry->vec_agg_cache[0];
            agg_info->vec_agg_final = &entry->vec_transform_function[0];
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_VEC_EXECUTOR),
                errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("UnSupported vector aggregation function %u", aggfnoid)));
    }
}

/*
 * @Description: Creates the run-time information for the agg node.
 */
VecAggState* ExecInitVecAggregation(VecAgg* node, EState* estate, int eflags)
{
    VecAggState* aggstate = NULL;
    AggStatePerAgg peragg;
    Plan* outer_plan = NULL;
    int numaggs, aggno;
    ListCell* l = NULL;
    ScalarDesc unknown_desc;  // unknown desc
    bool is_singlenode = false;
    int col_number = 0;
    int phase;
    Bitmapset* all_grouped_cols = NULL;
    int num_grp_sets = 1;
    int num_phases;
    int currentsortno = 0;
    int i = 0;
    int j = 0;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    aggstate = makeNode(VecAggState);
    aggstate->ss.ps.plan = (Plan*)node;
    aggstate->ss.ps.state = estate;
    aggstate->ss.ps.vectorized = true;
    aggstate->aggs = NIL;
    aggstate->numaggs = 0;
    aggstate->eqfunctions = NULL;
    aggstate->hashfunctions = NULL;
    aggstate->peragg = NULL;
    aggstate->agg_done = false;
    aggstate->pergroup = NULL;
    aggstate->grp_firstTuple = NULL;
    aggstate->aggcontexts = NULL;
    aggstate->maxsets = 0;
    aggstate->projected_set = -1;
    aggstate->current_set = 0;
    aggstate->input_done = false;
    aggstate->sort_in = NULL;
    aggstate->sort_out = NULL;

    is_singlenode = node->single_node || node->is_final;

    if (IS_PGXC_COORDINATOR && node->is_final == false)
        is_singlenode = true;

    if (node->aggstrategy == AGG_HASHED) {
        int64 operator_mem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop);
        AllocSetContext* set = (AllocSetContext*)(estate->es_query_cxt);
        set->maxSpaceSize = operator_mem * 1024L + SELF_GENRIC_MEMCTX_LIMITATION;
    }
    /*
     * Create expression contexts.  We need two, one for per-input-tuple
     * processing and one for per-output-tuple processing.  We cheat a little
     * by using ExecAssignExprContext() to build both.
     */
    ExecAssignExprContext(estate, &aggstate->ss.ps);
    aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;
    ExecAssignExprContext(estate, &aggstate->ss.ps);

    if (node->groupingSets) {
        Assert(node->aggstrategy != AGG_HASHED);

        num_grp_sets = list_length(node->groupingSets);

        foreach (l, node->chain) {
            Agg* agg = (Agg*)lfirst(l);

            num_grp_sets = Max(num_grp_sets, list_length(agg->groupingSets));
        }
    }

    aggstate->maxsets = num_grp_sets;
    aggstate->numphases = num_phases = 1 + list_length(node->chain);

    /*
     * tuple table initialization
     */
    ExecInitScanTupleSlot(estate, &aggstate->ss);
    ExecInitResultTupleSlot(estate, &aggstate->ss.ps);
    aggstate->sort_slot = ExecInitExtraTupleSlot(estate);

    /*
     * initialize child expressions
     *
     * Note: ExecInitExpr finds Aggrefs for us, and also checks that no aggs
     * contain other agg calls in their arguments.  This would make no sense
     * under SQL semantics anyway (and it's forbidden by the spec). Because
     * that is true, we don't need to worry about evaluating the aggs in any
     * particular order.
     */
    aggstate->ss.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)aggstate);
    aggstate->ss.ps.qual = (List*)ExecInitVecExpr((Expr*)node->plan.qual, (PlanState*)aggstate);

    /*
     * initialize child nodes
     *
     * If we are doing a hashed aggregation then the child plan does not need
     * to handle REWIND efficiently; see ExecReScanAgg.
     */
    eflags &= ~EXEC_FLAG_REWIND;
    outer_plan = outerPlan(node);
    outerPlanState(aggstate) = ExecInitNode(outer_plan, estate, eflags);

    /*
     * initialize source tuple type.
     */
    ExecAssignScanTypeFromOuterPlan(&aggstate->ss);
    if (node->chain)
        ExecSetSlotDescriptor(aggstate->sort_slot, aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);

    /*
     * Initialize result tuple type and projection info.
     * Result tuple slot of Aggregation always contains a virtual tuple,
     * Default tableAMtype for this slot is Heap.
     */
    ExecAssignResultTypeFromTL(&aggstate->ss.ps, TAM_HEAP);

    aggstate->ss.ps.ps_ProjInfo = ExecBuildVecProjectionInfo(aggstate->ss.ps.targetlist,
        node->plan.qual,
        aggstate->ss.ps.ps_ExprContext,
        aggstate->ss.ps.ps_ResultTupleSlot,
        NULL);

    aggstate->ss.ps.ps_TupFromTlist = false;

    /*
     * get the count of aggregates in targetlist and quals
     */
    numaggs = aggstate->numaggs;
    Assert(numaggs == list_length(aggstate->aggs));
    if (numaggs <= 0) {
        /*
         * This is not an error condition: we might be using the Agg node just
         * to do hash-based grouping.  Even in the regular case,
         * constant-expression simplification could optimize away all of the
         * Aggrefs in the targetlist and qual.  So keep going, but force local
         * copy of numaggs positive so that palloc()s below don't choke.
         */
        numaggs = 1;
    }

    /*
     * If we are grouping, precompute fmgr lookup data for inner loop. We need
     * both equality and hashing functions to do it by hashing, but only
     * equality if not hashing.
     */
    if (node->numCols > 0) {
        if (node->aggstrategy == AGG_HASHED)
            execTuplesHashPrepare(node->numCols, node->grpOperators, &aggstate->eqfunctions, &aggstate->hashfunctions);
        else
            aggstate->eqfunctions = execTuplesMatchPrepare(node->numCols, node->grpOperators);
    }

    /*
     * For each phase, prepare grouping set data and fmgr lookup data for
     * compare functions.  Accumulate all_grouped_cols in passing.
     */
    aggstate->phases = (AggStatePerPhaseData*)palloc0(num_phases * sizeof(AggStatePerPhaseData));

    for (phase = 0; phase < num_phases; ++phase) {
        AggStatePerPhase phase_data = &aggstate->phases[phase];
        Agg* agg_node = NULL;
        Sort* sort_node = NULL;
        int num_sets;

        if (phase > 0) {
            agg_node = (Agg*)list_nth(node->chain, phase - 1);
            sort_node = (Sort*)agg_node->plan.lefttree;
            Assert(IsA(sort_node, Sort));
        } else {
            agg_node = node;
            sort_node = NULL;
        }

        phase_data->numsets = num_sets = list_length(agg_node->groupingSets);

        if (num_sets) {
            phase_data->gset_lengths = (int*)palloc(num_sets * sizeof(int));
            phase_data->grouped_cols = (Bitmapset**)palloc(num_sets * sizeof(Bitmapset*));

            i = 0;
            foreach (l, agg_node->groupingSets) {
                int current_length = list_length((List*)lfirst(l));
                Bitmapset* cols = NULL;

                /* planner forces this to be correct */
                for (j = 0; j < current_length; ++j)
                    cols = bms_add_member(cols, agg_node->grpColIdx[j]);

                phase_data->grouped_cols[i] = cols;
                phase_data->gset_lengths[i] = current_length;
                ++i;
            }

            all_grouped_cols = bms_add_members(all_grouped_cols, phase_data->grouped_cols[0]);
        } else {
            Assert(phase == 0);

            phase_data->gset_lengths = NULL;
            phase_data->grouped_cols = NULL;
        }

        /*
         * If we are grouping, precompute fmgr lookup data for inner loop.
         */
        if (agg_node->aggstrategy == AGG_SORTED) {
            Assert(agg_node->numCols > 0);

            phase_data->eqfunctions = execTuplesMatchPrepare(agg_node->numCols, agg_node->grpOperators);
        }

        phase_data->aggnode = agg_node;
        phase_data->sortnode = sort_node;
    }

    /*
     * Convert all_grouped_cols to a descending-order list.
     */
    i = -1;
    while ((i = bms_next_member(all_grouped_cols, i)) >= 0)
        aggstate->all_grouped_cols = lcons_int(i, aggstate->all_grouped_cols);

    /*
     * Hashing can only appear in the initial phase.
     */
    if (node->aggstrategy == AGG_HASHED) {
        execTuplesHashPrepare(
            node->numCols, node->grpOperators, &aggstate->phases[0].eqfunctions, &aggstate->hashfunctions);

        if (u_sess->attr.attr_sql.enable_fast_numeric) {
            replace_numeric_hash_to_bi(node->numCols, aggstate->hashfunctions);
        }
    }
    /*
     * Initialize current phase-dependent values to initial phase
     */
    aggstate->current_phase = 0;

    /*
     * Set up aggregate-result storage in the output expr context, and also
     * allocate my private per-agg working storage
     */
    peragg = (AggStatePerAgg)palloc0(sizeof(AggStatePerAggData) * numaggs * num_grp_sets);
    aggstate->peragg = peragg;

    /* Compute the columns we actually need to hash on */
    aggstate->hash_needed = find_hash_columns(aggstate);

    if (aggstate->all_grouped_cols) {
        Bitmapset* all_need_cols = NULL;

        ListCell* lc = NULL;
        foreach (lc, aggstate->all_grouped_cols) {
            int varNumber = lfirst_int(lc) - 1;
            all_need_cols = bms_add_member(all_need_cols, varNumber);
        }
        foreach (lc, aggstate->hash_needed) {
            int varNumber = lfirst_int(lc) - 1;
            all_need_cols = bms_add_member(all_need_cols, varNumber);
        }
        col_number = bms_num_members(all_need_cols);
        bms_free_ext(all_need_cols);
    } else {
        col_number = list_length(aggstate->hash_needed);
    }

    /* Create aggregation list */
    aggstate->aggInfo = (VecAggInfo*)palloc0(sizeof(VecAggInfo) * numaggs);

    aggno = -1;
    foreach (l, aggstate->aggs) {
        AggrefExprState* aggrefstate = (AggrefExprState*)lfirst(l);
        Aggref* aggref = (Aggref*)aggrefstate->xprstate.expr;
        AggStatePerAgg peraggstate;
        Oid input_types[FUNC_MAX_ARGS];
        int num_arguments;
        int num_inputs;
        int num_sort_cols;
        int num_dist_cols;
        List* sortlist = NIL;
        HeapTuple agg_tuple;
        Form_pg_aggregate aggform;
        Oid aggtranstype;
        AclResult aclresult;
        Oid transfn_oid, finalfn_oid;
        Expr* transfnexpr = NULL;
        Expr* finalfnexpr = NULL;
        Datum text_init_val;
        ListCell* lc = NULL;

        /* Planner should have assigned aggregate to correct level */
        Assert(aggref->agglevelsup == 0);

        /* Nope, so assign a new PerAgg record */
        peraggstate = &peragg[++aggno];

        /* Mark Aggref state node with assigned index in the result array */
        aggrefstate->aggno = aggno;

        /* Fill in the peraggstate data */
        peraggstate->aggrefstate = aggrefstate;
        peraggstate->aggref = aggref;
        num_inputs = list_length(aggref->args);
        peraggstate->numInputs = num_inputs;
        peraggstate->sortstate = NULL;

        peraggstate->sortstates = (Tuplesortstate**)palloc0(sizeof(Tuplesortstate*) * num_grp_sets);

        for (currentsortno = 0; currentsortno < num_grp_sets; currentsortno++)
            peraggstate->sortstates[currentsortno] = NULL;
        /*
         * Get actual datatypes of the inputs.  These could be different from
         * the agg's declared input types, when the agg accepts ANY or a
         * polymorphic type.
         */
        num_arguments = 0;
        foreach (lc, aggref->args) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);

            if (!tle->resjunk)
                input_types[num_arguments++] = exprType((Node*)tle->expr);
        }
        peraggstate->numArguments = num_arguments;

        agg_tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
        if (!HeapTupleIsValid(agg_tuple))
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for aggregate %u", aggref->aggfnoid)));

        aggform = (Form_pg_aggregate)GETSTRUCT(agg_tuple);

        /* Check permission to call aggregate function */
        aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(aggref->aggfnoid));

        peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
        peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

#ifdef PGXC
        peraggstate->collectfn_oid = aggform->aggcollectfn;
        if (aggref->aggstage == 0)
            peraggstate->collectfn_oid = InvalidOid;
#endif /* PGXC */

        /* Check that aggregate owner has permission to call component fns */
        {
            HeapTuple proc_tuple;
            Oid agg_owner;

            proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggref->aggfnoid));
            if (!HeapTupleIsValid(proc_tuple))
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for function %u", aggref->aggfnoid)));

            agg_owner = ((Form_pg_proc)GETSTRUCT(proc_tuple))->proowner;
            ReleaseSysCache(proc_tuple);

            aclresult = pg_proc_aclcheck(transfn_oid, agg_owner, ACL_EXECUTE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(transfn_oid));
            if (OidIsValid(finalfn_oid)) {
                aclresult = pg_proc_aclcheck(finalfn_oid, agg_owner, ACL_EXECUTE);
                if (aclresult != ACLCHECK_OK)
                    aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(finalfn_oid));
            }
        }

        /* resolve actual type of transition state, if polymorphic */
        aggtranstype = aggform->aggtranstype;
        if (IsPolymorphicType(aggtranstype)) {
            /* have to fetch the agg's declared input types... */
            Oid* declared_arg_types = NULL;
            int agg_nargs;

            (void)get_func_signature(aggref->aggfnoid, &declared_arg_types, &agg_nargs);
            Assert(agg_nargs == num_arguments);
            aggtranstype =
                enforce_generic_type_consistency(input_types, declared_arg_types, agg_nargs, aggtranstype, false);
            pfree_ext(declared_arg_types);
        }

        /* build expression trees using actual argument & result types */
        build_aggregate_fnexprs(input_types,
            num_arguments,
            aggtranstype,
            aggref->aggtype,
            aggref->inputcollid,
            transfn_oid,
            finalfn_oid,
            &transfnexpr,
            &finalfnexpr);

        fmgr_info(transfn_oid, &peraggstate->transfn);
        fmgr_info_set_expr((Node*)transfnexpr, &peraggstate->transfn);

        if (OidIsValid(finalfn_oid)) {
            fmgr_info(finalfn_oid, &peraggstate->finalfn);
            fmgr_info_set_expr((Node*)finalfnexpr, &peraggstate->finalfn);
        }

        peraggstate->aggCollation = aggref->inputcollid;

        get_typlenbyval(aggref->aggtype, &peraggstate->resulttypeLen, &peraggstate->resulttypeByVal);
        get_typlenbyval(aggtranstype, &peraggstate->transtypeLen, &peraggstate->transtypeByVal);

        /*
         * initval is potentially null, so don't try to access it as a struct
         * field. Must do it the hard way with SysCacheGetAttr.
         */
        text_init_val = SysCacheGetAttr(AGGFNOID, agg_tuple, Anum_pg_aggregate_agginitval, &peraggstate->initValueIsNull);

        if (peraggstate->initValueIsNull)
            peraggstate->initValue = (Datum)0;
        else
            peraggstate->initValue = GetAggInitVal(text_init_val, aggtranstype);

        /*
         * If the transfn is strict and the initval is NULL, make sure input
         * type and transtype are the same (or at least binary-compatible), so
         * that it's OK to use the first input value as the initial
         * transValue.  This should have been checked at agg definition time,
         * but just in case...
         */
        if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull) {
            if (num_arguments < 1 || !IsBinaryCoercible(input_types[0], aggtranstype))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg(
                            "aggregate %u needs to have compatible input type and transition type", aggref->aggfnoid)));
        }

        /*
         * Get a tupledesc corresponding to the inputs (including sort
         * expressions) of the agg.
         */
        peraggstate->evaldesc = ExecTypeFromTL(aggref->args, false);

        /* Create slot we're going to do argument evaluation in */
        peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

        /* Set up projection info for evaluation */
        peraggstate->evalproj = NULL;
        if (ExecTargetListLength(aggrefstate->args) > 0) {
            peraggstate->evalproj = ExecBuildVecProjectionInfo(
                aggrefstate->args, node->plan.qual, aggstate->tmpcontext, peraggstate->evalslot, NULL);
        }

        /*
         * If we're doing either DISTINCT or ORDER BY, then we have a list of
         * SortGroupClause nodes; fish out the data in them and stick them
         * into arrays.
         *
         * Note that by construction, if there is a DISTINCT clause then the
         * ORDER BY clause is a prefix of it (see transformDistinctClause).
         */
        if (aggref->aggdistinct) {
            sortlist = aggref->aggdistinct;
            num_sort_cols = num_dist_cols = list_length(sortlist);
            Assert(num_sort_cols >= list_length(aggref->aggorder));
        } else {
            sortlist = aggref->aggorder;
            num_sort_cols = list_length(sortlist);
            num_dist_cols = 0;
        }

        peraggstate->numSortCols = num_sort_cols;
        peraggstate->numDistinctCols = num_dist_cols;

        if (num_sort_cols > 0) {
            /*
             * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
             * (yet)
             */
            Assert(node->aggstrategy != AGG_HASHED);

            /* If we have only one input, we need its len/byval info. */
            if (num_inputs == 1) {
                get_typlenbyval(input_types[0], &peraggstate->inputtypeLen, &peraggstate->inputtypeByVal);
            } else if (num_dist_cols > 0) {
                /* we will need an extra slot to store prior values */
                peraggstate->uniqslot = NULL;
            }

            /* Extract the sort information for use later */
            peraggstate->sortColIdx = (AttrNumber*)palloc(num_sort_cols * sizeof(AttrNumber));
            peraggstate->sortOperators = (Oid*)palloc(num_sort_cols * sizeof(Oid));
            peraggstate->sortCollations = (Oid*)palloc(num_sort_cols * sizeof(Oid));
            peraggstate->sortNullsFirst = (bool*)palloc(num_sort_cols * sizeof(bool));

            i = 0;
            foreach (lc, sortlist) {
                SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc);
                TargetEntry* tle = get_sortgroupclause_tle(sortcl, aggref->args);

                /* the parser should have made sure of this */
                Assert(OidIsValid(sortcl->sortop));

                peraggstate->sortColIdx[i] = tle->resno;
                peraggstate->sortOperators[i] = sortcl->sortop;
                peraggstate->sortCollations[i] = exprCollation((Node*)tle->expr);
                peraggstate->sortNullsFirst[i] = sortcl->nulls_first;
                i++;
            }
            Assert(i == num_sort_cols);
        }

        if (aggref->aggdistinct) {
            Assert(num_arguments > 0);

            if (num_dist_cols > 1) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_AGG),
                        errmsg("Vector aggregation does not support this distinct clause in aggregate function")));
            }
            /*
             * We need the equal function for each DISTINCT comparison we will
             * make.
             */
            peraggstate->equalfns = (FmgrInfo*)palloc(num_dist_cols * sizeof(FmgrInfo));

            i = 0;
            foreach (lc, aggref->aggdistinct) {
                SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc);

                fmgr_info(get_opcode(sortcl->eqop), &peraggstate->equalfns[i]);
                switch (peraggstate->evaldesc->tdtypeid) {
                    case TIMETZOID:
                        peraggstate->equalfns[i].fn_addr = timetz_eq_withhead;
                        break;
                    case TINTERVALOID:
                        peraggstate->equalfns[i].fn_addr = tintervaleq_withhead;
                        break;
                    case INTERVALOID:
                        peraggstate->equalfns[i].fn_addr = interval_eq_withhead;
                        break;
                    case NAMEOID:
                        peraggstate->equalfns[i].fn_addr = nameeq_withhead;
                        break;
                    default:
                        break;
                }
                i++;
            }

            Assert(i == num_dist_cols);
        }

        ReleaseSysCache(agg_tuple);
        int idx = aggstate->numaggs - 1 - aggno;
        {
            DBG_ASSERT(idx >= 0);
            bool use_sonichash = (node->aggstrategy == AGG_HASHED && node->is_sonichash);

            DispatchAggFunction(&aggstate->peragg[aggno], &aggstate->aggInfo[idx], use_sonichash);

            /* Initialize the function call parameter struct as well */
            InitFunctionCallInfoData(aggstate->aggInfo[idx].vec_agg_function,
                &peraggstate->transfn,
                4,
                peraggstate->aggCollation,
                NULL,
                NULL);
            /* Choose vector agg funtion, when agg node is only on datanode or coordinator, same function to be choosed.
             */
            if (OidIsValid(peraggstate->collectfn_oid) == false)
                aggstate->aggInfo[idx].vec_agg_function.flinfo->vec_fn_addr = aggstate->aggInfo[idx].vec_agg_cache[0];
            else
                aggstate->aggInfo[idx].vec_agg_function.flinfo->vec_fn_addr = aggstate->aggInfo[idx].vec_agg_cache[1];

            if (OidIsValid(peraggstate->finalfn_oid)) {
                InitFunctionCallInfoData(aggstate->aggInfo[idx].vec_final_function,
                    &peraggstate->finalfn,
                    2,
                    peraggstate->aggCollation,
                    NULL,
                    NULL);

                if (OidIsValid(peraggstate->collectfn_oid) == false) {
                    if (is_singlenode)
                        aggstate->aggInfo[idx].vec_final_function.flinfo->fn_addr =
                            aggstate->aggInfo[idx].vec_agg_final[0];
                    else
                        aggstate->aggInfo[idx].vec_final_function.flinfo->fn_addr =
                            aggstate->aggInfo[idx].vec_agg_final[1];

                    aggstate->aggInfo[idx].vec_agg_function.flinfo->vec_fn_addr =
                        aggstate->aggInfo[idx].vec_agg_cache[0];
                } else {
                    if (is_singlenode)
                        aggstate->aggInfo[idx].vec_final_function.flinfo->fn_addr =
                            aggstate->aggInfo[idx].vec_agg_final[2];
                    else
                        aggstate->aggInfo[idx].vec_final_function.flinfo->fn_addr =
                            aggstate->aggInfo[idx].vec_agg_final[3];

                    aggstate->aggInfo[idx].vec_agg_function.flinfo->vec_fn_addr =
                        aggstate->aggInfo[idx].vec_agg_cache[1];
                }
            }

            ScalarVector* p_vec = New(CurrentMemoryContext) ScalarVector[4];
            for (int k = 0; k < 4; k++)
                p_vec[k].init(CurrentMemoryContext, unknown_desc);

            aggstate->aggInfo[idx].vec_agg_function.argVector = p_vec;

            /* Allocate vector for boolean check purpose. */
            if (peraggstate->evalproj != NULL)
                ExecAssignVectorForExprEval(peraggstate->evalproj->pi_exprContext);

            aggrefstate->m_htbOffset = col_number + idx;
        }
    }

    ExecAssignVectorForExprEval(aggstate->ss.ps.ps_ExprContext);

    /* Update numaggs to match number of unique aggregates found */
    aggstate->numaggs = aggno + 1;
    aggstate->aggRun = NULL;

#ifdef ENABLE_LLVM_COMPILE
    /*
     * Generate IR function for HashAggRunner::BuildAggTbl function, which
     * contains hashing part, allocate hashcell and agg part
     */
    bool consider_codegen =
        CodeGenThreadObjectReady() &&
        CodeGenPassThreshold(((Plan*)outer_plan)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)outer_plan)->dop);
    if (consider_codegen) {
        if (node->aggstrategy == AGG_HASHED && node->is_sonichash) {
            dorado::VecHashAggCodeGen::SonicHashAggCodeGen(aggstate);
        } else if (node->aggstrategy == AGG_HASHED) {
            dorado::VecHashAggCodeGen::HashAggCodeGen(aggstate);
        }
    }
#endif

    return aggstate;
}

/*
 * @Description: vec agg entrance function, according to agg strategy call different function.
 */
VectorBatch* ExecVecAggregation(VecAggState* node)
{
    Assert(!node->ss.ps.ps_TupFromTlist);

    /*
     * just for cooperation analysis. do nothing if is_dummy is true.
     * is_dummy is true that means Agg node is deparsed to remote sql in ForeignScan node.
     */
    if (((VecAgg*)node->ss.ps.plan)->is_dummy) {
        PlanState* outer_node = outerPlanState(node);
        VectorBatch* result_batch = VectorEngine(outer_node);

        return result_batch;
    }

    VecAgg* plan = (VecAgg*)(node->ss.ps.plan);
    if (node->aggRun == NULL) {
        switch (plan->aggstrategy) {
            case AGG_PLAIN:
                node->aggRun = New(CurrentMemoryContext) PlainAggRunner(node);
                break;
            case AGG_HASHED:
                if (plan->is_sonichash)
                    node->aggRun = New(CurrentMemoryContext) SonicHashAgg(node, INIT_DATUM_ARRAY_SIZE);
                else
                    node->aggRun = New(CurrentMemoryContext) HashAggRunner(node);
                break;
            case AGG_SORTED:
                node->aggRun = New(CurrentMemoryContext) SortAggRunner(node);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unsupported aggregation type")));
                break;
        }
    }

    if (plan->is_sonichash)
        return ((SonicHashAgg*)node->aggRun)->Run();
    else
        return DO_AGGEGATION(node->aggRun);
}

/*
 * @Description: agg rescan.
 */
void ExecReScanVecAggregation(VecAggState* node)
{
    bool reset_left_tree = false;
    VecAgg* plan = (VecAgg*)(node->ss.ps.plan);

    if (plan->is_sonichash) {
        SonicHashAgg* tb = (SonicHashAgg*)node->aggRun;
        if (tb != NULL) {
            /* Sign lefttree if need reset */
            reset_left_tree = tb->ResetNecessary(node);
        }
    } else {
        BaseAggRunner* tb = (BaseAggRunner*)node->aggRun;
        if (tb != NULL) {
            /* Sign lefttree if need reset */
            reset_left_tree = tb->ResetNecessary(node);
        }
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (reset_left_tree && node->ss.ps.lefttree->chgParam == NULL) {
        VecExecReScan(node->ss.ps.lefttree);
    }
}

/*
 * @Description: agg end.
 */
void ExecEndVecAggregation(VecAggState* node)
{
    PlanState* outer_plan = NULL;

    VecAgg* plan = (VecAgg*)(node->ss.ps.plan);
    if (plan->aggstrategy == AGG_HASHED) {
        /* free memory context according to current runtime */
        if (plan->is_sonichash) {
            SonicHashAgg* vshashagg = (SonicHashAgg*)node->aggRun;
            if (vshashagg != NULL) {
                vshashagg->freeMemoryContext();
            }
        } else {
            HashAggRunner* vechashTbl = (HashAggRunner*)node->aggRun;
            if (vechashTbl != NULL) {
                vechashTbl->closeFile();
                vechashTbl->freeMemoryContext();
            }
        }
    } else if (plan->aggstrategy == AGG_SORTED) {
        SortAggRunner* vecSortAgg = (SortAggRunner*)node->aggRun;
        if (vecSortAgg != NULL) {
            vecSortAgg->endSortAgg();
        }
    } else {
        PlainAggRunner* vecPlainAgg = (PlainAggRunner*)node->aggRun;
        if (vecPlainAgg != NULL) {
            vecPlainAgg->endPlainAgg();
        }
    }

    /*
     * Free both the expr contexts.
     */
    ExecFreeExprContext(&node->ss.ps);
    node->ss.ps.ps_ExprContext = node->tmpcontext;
    ExecFreeExprContext(&node->ss.ps);

    /* clean up tuple table */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);
    outer_plan = outerPlanState(node);
    ExecEndNode(outer_plan);
}

/*
 * @Description: Init aggregation information.
 * @in agg_num - the number of agg
 * @in aggInfo - agg information
 * @return - void
 */
void BaseAggRunner::init_aggInfo(int agg_num, VecAggInfo* agg_info)
{
    m_aggNum = agg_num;

    m_cols = m_cellVarLen;
    int agg_idx = m_cols;
    if (m_aggNum > 0) {
        m_aggIdx = (int*)palloc(sizeof(int) * m_aggNum);
        m_aggCount = (bool*)palloc0(sizeof(bool) * m_aggNum);
        m_finalAggInfo = (finalAggInfo*)palloc0(sizeof(finalAggInfo) * m_aggNum);
    }

    m_finalAggNum = 0;
    for (int i = 0; i < m_aggNum; i++) {
        m_cols++;
        m_aggIdx[i] = agg_idx;
        if (agg_info[i].vec_agg_function.flinfo->fn_addr == int8inc_any || /* count(col) */
            agg_info[i].vec_agg_function.flinfo->fn_addr == int8inc) { /* count(*) */
            m_aggCount[i] = true;
        }

        if (agg_info[i].vec_final_function.flinfo != NULL) {
            m_finalAggInfo[m_finalAggNum].idx = agg_idx;
            m_finalAggInfo[m_finalAggNum].info = &agg_info[i];
            m_finalAggNum++;

            /*
             * For vector stddev_samp transvalue, needs to store  count, sum(x), sum(x^2).
             */
            agg_idx += 3;
            m_cols += 2;
        } else {
            agg_idx++;
        }
    }
}

/*
 * @Description: To common group, we only need set this values. To Ap function, we need set them again,
 * this be done only in sortAgg
 */
void BaseAggRunner::init_Baseindex()
{
    VecAgg* node = NULL;
    int i;

    node = (VecAgg*)(m_runtime->ss.ps.plan);

    m_eqfunctions = m_runtime->eqfunctions;
    m_outerHashFuncs = m_runtime->hashfunctions;

    /* grouping column num */
    m_key = node->numCols;

    if (m_key > 0) {
        /* grouping column mean */
        m_keyIdx = (int*)palloc(sizeof(int) * m_key);

        for (i = 0; i < m_key; i++) {
            m_keyIdx[i] = node->grpColIdx[i] - 1;
        }
    }

    /*
     * The hash_needed include all var indexs that be needed in agg operator.
     */
    if (m_runtime->hash_needed) {
        m_cellVarLen = list_length(m_runtime->hash_needed);

        if (m_cellVarLen > 0) {
            m_cellBatchMap = (int*)palloc(sizeof(int) * m_cellVarLen);

            /* m_cellBatchMap Mapping position of outbatch column in cell
             * for example, m_cellBatchMap[0] = 3 say: batch's 3 column keep in 0 column of cell
             */
            i = 0;
            ListCell* lc = NULL;
            foreach (lc, m_runtime->hash_needed) {
                int var_num = lfirst_int(lc) - 1;
                m_cellBatchMap[i] = var_num;
                i++;
            }
        }
    }

    if (m_key > 0) {
        /* Keep indexes of grouping columns in cell */
        m_keyIdxInCell = (int*)palloc(sizeof(int) * m_key);

        for (i = 0; i < m_key; i++) {
            bool isFound = false;
            for (int k = 0; k < m_cellVarLen; k++) {
                if (m_keyIdx[i] == m_cellBatchMap[k]) {
                    m_keyIdxInCell[i] = k;
                    isFound = true;
                    break;
                }
            }
            Assert(isFound);
        }
    }

    init_aggInfo(m_runtime->numaggs, m_runtime->aggInfo);
}

/*
 * @Description: Build batch which will be used in agg.
 */
void BaseAggRunner::build_batch()
{
    int i = 0;
    int idx = 0;
    ListCell* l = NULL;

    ScalarDesc* type_arr = (ScalarDesc*)palloc(sizeof(ScalarDesc) * (m_cellVarLen + m_aggNum));

    TupleDesc out_desc = outerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;

    for (i = 0; i < m_cellVarLen; i++) {
        type_arr[i].typeId = out_desc->attrs[m_cellBatchMap[i]]->atttypid;
        type_arr[i].typeMod = out_desc->attrs[m_cellBatchMap[i]]->atttypmod;
        type_arr[i].encoded = COL_IS_ENCODE(type_arr[i].typeId);
        if (type_arr[i].encoded) {
            m_keySimple = false;
        }
    }

    idx = m_cellVarLen + m_aggNum - 1;

    foreach (l, m_runtime->aggs) {
        AggrefExprState* aggrefstate = (AggrefExprState*)lfirst(l);
        Aggref* aggref = (Aggref*)aggrefstate->xprstate.expr;

        type_arr[idx].typeId = aggref->aggtype;
        type_arr[idx].typeMod = -1;
        type_arr[idx].encoded = COL_IS_ENCODE(type_arr[idx].typeId);
        idx--;
    }

    m_scanBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, type_arr, m_cellVarLen + m_aggNum);
    m_proBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, out_desc);
    m_outerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, out_desc);
}

/*
 * @Description: constructed function for init  agg information.
 */
BaseAggRunner::BaseAggRunner()
{
    m_cellVarLen = 0;
    m_hashContext = AllocSetContextCreate(CurrentMemoryContext,
        "HashCacheContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    for (int i = 0; i < BatchMaxSize; i++) {
        m_Loc[i] = NULL;
    }
    m_finalAggInfo = NULL;
    m_okeyIdx = NULL;
    m_colWidth = 0;
    m_strategy = 0;
    m_overflowsource = NULL;
    m_maxMem = 0;
    m_projected_set = 0;
    m_innerHashFuncs = NULL;
    m_aggCount = NULL;
    m_outerBatch = NULL;
    m_econtext = NULL;
    m_spreadNum = 0;
    m_outerHashFuncs = NULL;
    m_finalAggNum = 0;
    m_keyIdxInCell = NULL;
    m_scanBatch = NULL;
    m_sortDistinct = NULL;
    m_eqfunctions = NULL;
    m_keySimple = 0;
    m_sortCurrentBuf = NULL;
    m_cellSize = 0;
    m_hashTbl = NULL;
    m_keyDesc = NULL;
    m_filesource = NULL;
    m_runtime = NULL;
    m_sysBusy = false;
    m_key = 0;
    m_tmpContext = NULL;
    m_hasDistinct = false;
    m_cols = 0;
    m_tupleCount = 0;
    m_buildScanBatch = NULL;
    m_aggNum = 0;
    m_fill_table_rows = 0;
    m_cellBatchMap = NULL;
    m_finish = false;
    m_keyIdx = NULL;
    m_aggIdx = NULL;
    m_colDesc = NULL;
    m_proBatch = NULL;
    m_runState = 0;
}

/*
 * @Description: constructed function init agg information.
 */
BaseAggRunner::BaseAggRunner(VecAggState* runtime, bool is_hash_agg = false)
    : m_runtime(runtime),
      m_cellVarLen(0),
      m_finish(false),
      m_hasDistinct(false),
      m_keySimple(true),
      m_runState(AGG_PREPARE),
      m_sortDistinct(NULL),
      m_sortCurrentBuf(NULL)
{
    m_aggIdx = 0;
    m_okeyIdx = 0;
    m_colWidth = 0;
    m_tupleCount = 0;
    m_fill_table_rows = 0;
    m_spreadNum = 0;
    m_cellSize = 0;
    m_maxMem = 0;
    m_sysBusy = false;
    m_colDesc = NULL;
    m_buildScanBatch = NULL;
    m_overflowsource = NULL;
    m_filesource = NULL;
    m_tmpContext = NULL;
    m_keyDesc = NULL;
    m_hashTbl = NULL;
    m_innerHashFuncs = NULL;
    m_aggCount = NULL;
    m_finalAggInfo = NULL;
    m_keyIdxInCell = NULL;
    m_outerBatch = NULL;
    m_scanBatch = NULL;
    m_cols = 0;
    m_aggNum = 0;
    m_outerHashFuncs = NULL;
    m_key = 0;
    m_proBatch = NULL;
    m_finalAggNum = 0;
    m_cellBatchMap = NULL;
    m_eqfunctions = NULL;
    m_keyIdx = NULL;

    VecAgg* node = (VecAgg*)(runtime->ss.ps.plan);
    if (node->groupingSets && node->aggstrategy == AGG_SORTED) {
        /* This is Ap function operate, we achieve it only use sortAgg. we will set this values in sortAgg */
    } else {
        /* We will set this values, if it is only group or plain agg */
        init_Baseindex();
        build_batch();
        m_cellSize = offsetof(hashCell, m_val) + m_cols * sizeof(hashVal);
        if (m_finalAggNum > 0)
            m_buildScanBatch = &BaseAggRunner::BuildScanBatchFinal;
        else
            m_buildScanBatch = &BaseAggRunner::BuildScanBatchSimple;
    }

    /* Both sort and hash aggregation use STANDARD_CONTEXT */
    if (is_hash_agg == false) {
        m_hashContext = AllocSetContextCreate(CurrentMemoryContext,
            "HashContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    } else {
        int64 local_work_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
        m_hashContext = AllocSetContextCreate(CurrentMemoryContext,
            "HashContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            STANDARD_CONTEXT,
            local_work_mem * 1024L);
    }

    AddControlMemoryContext(runtime->ss.ps.instrument, m_hashContext);

    m_strategy = HASH_IN_MEMORY;
    for (int i = 0; i < BatchMaxSize; i++) {
        m_Loc[i] = NULL;
    }

    m_projected_set = -1;
    m_econtext = m_runtime->ss.ps.ps_ExprContext;
}

/*
 * @Description: Project and compute Aggregation.
 * @in batch - current batch.
 */
void BaseAggRunner::BatchAggregation(VectorBatch* batch)
{
    int i;
    int nrows;

    nrows = batch->m_rows;

    for (i = 0; i < m_aggNum; i++) {
        VectorBatch* p_batch = NULL;
        ScalarVector* p_vector = NULL;
        AggStatePerAgg per_agg_state = &m_runtime->peragg[m_aggNum - 1 - i];
        ExprContext* econtext = NULL;

        /* count(*) per_agg_state->evalproj is null. */
        if (per_agg_state->evalproj != NULL) {
            econtext = per_agg_state->evalproj->pi_exprContext;
            econtext->ecxt_outerbatch = batch;
            p_batch = ExecVecProject(per_agg_state->evalproj);
            Assert(!per_agg_state->evalproj || p_batch->m_cols == 1);
            p_vector = &p_batch->m_arr[0];
        } else
            p_vector = &batch->m_arr[0];

        p_vector->m_rows = Min(p_vector->m_rows, nrows);

        AggregationOnScalar(&m_runtime->aggInfo[i], p_vector, m_aggIdx[i], &m_Loc[0]);

        if (econtext != NULL)
            ResetExprContext(econtext);
    }
}

/*
 * @Description: Compute Aggregation.
 */
void BaseAggRunner::AggregationOnScalar(VecAggInfo* agg_info, ScalarVector* p_vector, int idx, hashCell** location)
{
    AutoContextSwitch memGuard(m_econtext->ecxt_per_tuple_memory);
    FunctionCallInfo fcinfo = &agg_info->vec_agg_function;

    fcinfo->arg[0] = (Datum)p_vector;
    fcinfo->arg[1] = (Datum)idx;
    fcinfo->arg[2] = (Datum)location;
    fcinfo->arg[3] = (Datum)m_hashContext;

    VecFunctionCallInvoke(fcinfo);
    ResetExprContext(m_econtext);
}

/*
 * @Description: set value to scanBatch include field value and agg value.
 */
void BaseAggRunner::BuildScanBatchSimple(hashCell* cell)
{
    int i;
    int nrows = m_scanBatch->m_rows;
    ScalarVector* p_vector = NULL;

    for (i = 0; i < m_cols; i++) {
        p_vector = &m_scanBatch->m_arr[i];

        p_vector->m_vals[nrows] = cell->m_val[i].val;
        p_vector->m_flag[nrows] = cell->m_val[i].flag;
        p_vector->m_rows++;
    }

    m_scanBatch->m_rows++;
}

/*
 * @Description: compute final agg and set value to scanBatch include field value and agg value.
 */
void BaseAggRunner::BuildScanBatchFinal(hashCell* cell)
{
    int i, j;
    int nrows = m_scanBatch->m_rows;
    ScalarVector* p_vector = NULL;
    int colIdx = 0;

    ExprContext* econtext = m_runtime->ss.ps.ps_ExprContext;
    AutoContextSwitch mem_guard(econtext->ecxt_per_tuple_memory);

    j = 0;

    for (i = 0; i < m_cols; i++) {
        p_vector = &m_scanBatch->m_arr[colIdx];

        if (i == m_finalAggInfo[j].idx) {
            /* to invoke function */
            FunctionCallInfo fcinfo = &m_finalAggInfo[j].info->vec_final_function;
            fcinfo->arg[0] = (Datum)cell;
            fcinfo->arg[1] = (Datum)i;
            fcinfo->arg[2] = (Datum)(&p_vector->m_vals[nrows]);
            fcinfo->arg[3] = (Datum)(&p_vector->m_flag[nrows]);

            /*
             * for var final function , we must make sure the return val
             * has added pointer val header.
             */
            FunctionCallInvoke(fcinfo);

            p_vector->m_rows++;
            j++;

            /*
             * For vector stddev_samp transvalue, needs to store  count, sum(x), sum(x^2).
             */
            i += 2;
        } else {
            p_vector->m_vals[nrows] = cell->m_val[i].val;
            p_vector->m_flag[nrows] = cell->m_val[i].flag;
            p_vector->m_rows++;
        }

        colIdx++;
    }
    m_scanBatch->m_rows++;
}

/*
 * @Description: Exec qual and exec project, return result.
 */
VectorBatch* BaseAggRunner::ProducerBatch()
{
    ExprContext* econtext = NULL;
    VectorBatch* p_res = NULL;

    /* Guard when there is no input rows */
    if (m_proBatch == NULL)
        return NULL;

    for (int i = 0; i < m_cellVarLen; i++) {
        m_outerBatch->m_arr[m_cellBatchMap[i]] = m_scanBatch->m_arr[i];
    }
    m_outerBatch->m_rows = m_scanBatch->m_rows;

    /* To AP functio, set column to NULL when group columns has not include it. */
    prepare_projection_batch();

    if (list_length(m_runtime->ss.ps.qual) != 0) {
        ScalarVector* p_vector = NULL;

        econtext = m_runtime->ss.ps.ps_ExprContext;
        econtext->ecxt_scanbatch = m_scanBatch;
        econtext->ecxt_aggbatch = m_scanBatch;
        econtext->ecxt_outerbatch = m_outerBatch;
        p_vector = ExecVecQual(m_runtime->ss.ps.qual, econtext, false);

        if (p_vector == NULL) {
            return NULL;
        }

        m_scanBatch->Pack(econtext->ecxt_scanbatch->m_sel);
    }

    for (int i = 0; i < m_cellVarLen; i++) {
        m_proBatch->m_arr[m_cellBatchMap[i]] = m_scanBatch->m_arr[i];
    }

    /* Do the copy out projection.*/
    m_proBatch->m_rows = m_scanBatch->m_rows;

    econtext = m_runtime->ss.ps.ps_ExprContext;
    econtext->ecxt_outerbatch = m_proBatch;
    econtext->ecxt_aggbatch = m_scanBatch;
    econtext->m_fUseSelection = m_runtime->ss.ps.ps_ExprContext->m_fUseSelection;
    p_res = ExecVecProject(m_runtime->ss.ps.ps_ProjInfo);
    return p_res;
}

/*
 * @Description: Init to distinct operator, in this case sort will be need in sortAgg
 */
void BaseAggRunner::initialize_sortstate(int work_mem, int max_mem, int plan_id, int dop)
{
    int agg_no;
    AggStatePerAgg per_agg = m_runtime->peragg;
    AggStatePerAgg per_agg_state = NULL;

    for (agg_no = 0; agg_no < m_aggNum; agg_no++) {
        per_agg_state = &per_agg[agg_no];
        if (per_agg_state->numDistinctCols > 0) {
            m_hasDistinct = true;
            break;
        }
    }
    if (m_hasDistinct) {
        /*
         * We need apply max numsets of phases instead of first parse's numsets space
         * otherwise may lead to access illegal memory.
         */
        int numset = Max(m_runtime->maxsets, 1);

        m_sortDistinct = (SortDistinct*)palloc0(numset * sizeof(SortDistinct));

        for (int i = 0; i < numset; i++) {
            m_sortDistinct[i].aggDistinctBatch = (VectorBatch**)palloc(m_aggNum * sizeof(VectorBatch*));
            m_sortDistinct[i].batchsortstate = (Batchsortstate**)palloc0(m_aggNum * sizeof(Batchsortstate*));

            m_sortDistinct[i].lastVal = (ScalarValue*)palloc0(m_aggNum * sizeof(ScalarValue));
            m_sortDistinct[i].lastValLen = (int*)palloc0(m_aggNum * sizeof(int));

            for (agg_no = 0; agg_no < m_aggNum; agg_no++) {
                per_agg_state = &per_agg[agg_no];

                if (per_agg_state->numSortCols > 0) {
                    m_sortDistinct[i].aggDistinctBatch[agg_no] =
                        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, per_agg_state->evaldesc);

                    m_sortDistinct[i].batchsortstate[agg_no] = batchsort_begin_heap(per_agg_state->evaldesc,
                        per_agg_state->numSortCols,
                        per_agg_state->sortColIdx,
                        per_agg_state->sortOperators,
                        per_agg_state->sortCollations,
                        per_agg_state->sortNullsFirst,
                        work_mem,
                        false,
                        max_mem,
                        plan_id,
                        dop);

                    if (COL_IS_ENCODE(m_sortDistinct[i].aggDistinctBatch[agg_no]->m_arr->m_desc.typeId)) {
                        m_sortDistinct[i].lastVal[agg_no] = PointerGetDatum((char*)palloc(100));
                        m_sortDistinct[i].lastValLen[agg_no] = 100;
                    }
                } else {
                    m_sortDistinct[i].batchsortstate[agg_no] = NULL;
                    m_sortDistinct[i].aggDistinctBatch[agg_no] = NULL;
                }
            }
        }
    }
}

/*
 * @Description: Delete repeat value.
 * @in equalfns: Equal function.
 * @in currentSet: Current set index.
 * @in aggno: Current agg index.
 * @in first: If is first batch current group.
 * @retutn: If current batch all is null return false else return true.
 */
bool BaseAggRunner::FilterRepeatValue(FmgrInfo* equalfns, int curr_set, int agg_no, bool first)
{
    VectorBatch** agg_batch = m_sortDistinct[curr_set].aggDistinctBatch;
    ScalarVector* vector = agg_batch[agg_no]->m_arr;

    /* Switch Memcontext to tmpcomtext, so that free equalfns apply memcontext in time. */
    AutoContextSwitch mem_guard(m_runtime->tmpcontext->ecxt_per_tuple_memory);

    /* If this value is null we need not do any operation, aggregation need not do compute to null. */
    if (vector->IsNull(0)) {
        return false;
    }

    if (first == false) {
        /* Need compare with the last value of previous batch.*/
        if (DatumGetBool(FunctionCall2(equalfns, m_sortDistinct[curr_set].lastVal[agg_no], vector->m_vals[0]))) {
            agg_batch[agg_no]->m_sel[0] = false;
        }
    }

    for (int j = 1; j < agg_batch[agg_no]->m_rows; j++) {
        /*
         * Previous value compare with next value, if they is same the sel will be set to false.
         * In other words, to same value, we only keep one row.
         */
        if (vector->IsNull(j) || DatumGetBool(FunctionCall2(equalfns, vector->m_vals[j - 1], vector->m_vals[j]))) {
            agg_batch[agg_no]->m_sel[j] = false;
        }
    }

    agg_batch[agg_no]->Pack(agg_batch[agg_no]->m_sel);

    ResetExprContext(m_runtime->tmpcontext);

    return true;
}

/*
 * @Description: compute distinct agg.
 */
void BaseAggRunner::BatchSortAggregation(int curr_set, int work_mem, int max_mem, int plan_id, int dop)
{
    int i, aggno;
    bool first = false;

    ScalarValue m_vals;
    hashCell* tmp_loc[BatchMaxSize];

    Batchsortstate** sort_stat = m_sortDistinct[curr_set].batchsortstate;
    VectorBatch** agg_batch = m_sortDistinct[curr_set].aggDistinctBatch;

    /* These data point same cell. */
    for (int k = 0; k < BatchMaxSize; k++) {
        tmp_loc[k] = m_sortDistinct[curr_set].m_distinctLoc;
    }

    AggStatePerAgg peragg = m_runtime->peragg;
    for (i = 0; i < m_aggNum; i++) {
        aggno = m_aggNum - 1 - i;
        first = true;
        AggStatePerAgg peragg_stat = &peragg[aggno];
        if (peragg_stat->numSortCols > 0) {
            batchsort_performsort(sort_stat[aggno]);
            batchsort_getbatch(sort_stat[aggno], true, agg_batch[aggno]);

            while (!unlikely(BatchIsNull(agg_batch[aggno]))) {
                ScalarVector* col = agg_batch[aggno]->m_arr;
                Oid type_oid = col->m_desc.typeId;
                FmgrInfo* equalfns = &peragg_stat->equalfns[0];

                /* This vector's values all is NULL. */
                if (equalfns && !FilterRepeatValue(equalfns, curr_set, aggno, first)) {
                    break;
                }

                if (agg_batch[aggno]->m_rows > 0) {
                    ScalarVector* p_vector = NULL;
                    p_vector = &agg_batch[aggno]->m_arr[0];

                    AggregationOnScalar(&m_runtime->aggInfo[i], p_vector, m_aggIdx[i], &tmp_loc[0]);

                    m_vals = agg_batch[aggno]->m_arr->m_vals[agg_batch[aggno]->m_rows - 1];

                    /* Keep last value which need compare with first value of next batch. */
                    copyLastValues(type_oid, m_vals, aggno, curr_set);
                }

                first = false;

                agg_batch[aggno]->ResetSelection(true);

                /* Get next sort batch. */
                batchsort_getbatch(sort_stat[aggno], true, agg_batch[aggno]);
            }
            /* End this sort and apply again. It is used by next group. */
            if (sort_stat[aggno]) {
                batchsort_end(sort_stat[aggno]);

                sort_stat[aggno] = batchsort_begin_heap(peragg_stat->evaldesc,
                    peragg_stat->numSortCols,
                    peragg_stat->sortColIdx,
                    peragg_stat->sortOperators,
                    peragg_stat->sortCollations,
                    peragg_stat->sortNullsFirst,
                    work_mem,
                    false,
                    max_mem,
                    plan_id,
                    dop);
            }
        }
    }
}

/*
 * @Description: append batch to sortstate.
 * @in batch - current batch
 * @in start - start of batch
 * @in end - end of batch
 * @in currentSet - current group
 */
void BaseAggRunner::AppendBatchForSortAgg(VectorBatch* batch, int start, int end, int curr_set)
{
    int i, aggno;
    VectorBatch* p_batch = NULL;

    for (i = 0; i < m_aggNum; i++) {
        aggno = m_aggNum - 1 - i;
        AggStatePerAgg peragg_stat = &m_runtime->peragg[aggno];

        if (peragg_stat->numSortCols > 0) {
            ExprContext* econtext = NULL;

            Assert(peragg_stat->evalproj != NULL);

            econtext = peragg_stat->evalproj->pi_exprContext;
            econtext->ecxt_outerbatch = batch;
            econtext->m_fUseSelection = true;

            VectorBatch* p_proj_batch = peragg_stat->evalproj->pi_batch;

            /*
             * Only project to batch from start to end rows rather than all rows, other rows's
             * m_sel need be set to false.
             */
            for (int k = 0; k < start; k++) {
                p_proj_batch->m_sel[k] = false;
            }

            for (int k = end; k < batch->m_rows; k++) {
                p_proj_batch->m_sel[k] = false;
            }

            p_batch = ExecVecProject(peragg_stat->evalproj, false);
            p_proj_batch->ResetSelection(true);
            Assert(!peragg_stat->evalproj || p_batch->m_cols == 1);

            m_sortDistinct[curr_set].batchsortstate[aggno]->sort_putbatch(
                m_sortDistinct[curr_set].batchsortstate[aggno], p_batch, start, end);

            /* Reset this econtext, it's memory can be used in ExecVecProject. */
            ResetExprContext(econtext);
            econtext->m_fUseSelection = false;
        }
    }
}

/*
 * @Description: keep last data.
 * @in typOid - data type oid.
 * @in m_vals - actual value.
 * @in aggno - agg number.
 * @in currentSet - current group number.
 */
void BaseAggRunner::copyLastValues(Oid type_oid, ScalarValue m_vals, int aggno, int curr_set)
{
    char* tmp = NULL;
    int len = 0;
    errno_t err_no = EOK;

    if (COL_IS_ENCODE(type_oid)) {
        len = VARSIZE_ANY(m_vals);
        tmp = (char*)m_sortDistinct[curr_set].lastVal[aggno];
        if (len >= m_sortDistinct[curr_set].lastValLen[aggno]) {
            m_sortDistinct[curr_set].lastValLen[aggno] = len + 10;
            tmp = (char*)repalloc(tmp, m_sortDistinct[curr_set].lastValLen[aggno]);
            err_no = memcpy_s(tmp, m_sortDistinct[curr_set].lastValLen[aggno], (char*)m_vals, len);
            securec_check(err_no, "\0", "\0");
            m_sortDistinct[curr_set].lastVal[aggno] = PointerGetDatum(tmp);
        } else {
            err_no = memcpy_s(tmp, m_sortDistinct[curr_set].lastValLen[aggno], (char*)m_vals, len);
            securec_check(err_no, "\0", "\0");
        }
    } else {
        m_sortDistinct[curr_set].lastVal[aggno] = m_vals;
    }
}

/*
 * @Description: compute not distinct agg.
 * @in batch - current batch.
 */
void BaseAggRunner::BatchNoSortAgg(VectorBatch* batch)
{
    int i, aggno;

    for (i = 0; i < m_aggNum; i++) {
        aggno = m_aggNum - 1 - i;
        AggStatePerAgg peragg_stat = &m_runtime->peragg[aggno];

        if (peragg_stat->numSortCols <= 0) {
            VectorBatch* p_batch = NULL;
            ScalarVector* p_vector = NULL;
            ExprContext* econtext = NULL;

            if (peragg_stat->evalproj != NULL) {
                econtext = peragg_stat->evalproj->pi_exprContext;
                econtext->ecxt_outerbatch = batch;
                p_batch = ExecVecProject(peragg_stat->evalproj);

                Assert(!peragg_stat->evalproj || p_batch->m_cols == 1);
                p_vector = &p_batch->m_arr[0];
                p_vector->m_rows = Min(p_vector->m_rows, p_batch->m_rows);
            } else {
                p_vector = &batch->m_arr[0];
            }

            p_vector->m_rows = Min(p_vector->m_rows, batch->m_rows);

            AggregationOnScalar(&m_runtime->aggInfo[i], p_vector, m_aggIdx[i], &m_Loc[0]);

            if (econtext != NULL)
                ResetExprContext(econtext);
        }
    }
}

/*
 * @Description: To olap function, we need set null for not to take part in group columns.
 */
void BaseAggRunner::prepare_projection_batch()
{
    if (m_runtime->phase && m_runtime->phase->grouped_cols) {
        Bitmapset* grouped_cols = m_runtime->phase->grouped_cols[m_projected_set];

        m_runtime->grouped_cols = grouped_cols;

        ListCell* lc = NULL;
        foreach (lc, m_runtime->all_grouped_cols) {
            int att_num = lfirst_int(lc);
            if (!bms_is_member(att_num, grouped_cols)) {
                for (int i = 0; i < m_scanBatch->m_rows; i++) {
                    m_outerBatch->m_arr[att_num - 1].SetNull(i);
                }
            }
        }
    }
}

/*
 * @Description: Early free the memory for VecAggregation.
 *
 * @param[IN] node:  vector executor state for Agg
 * @return: void
 */
void ExecEarlyFreeVecAggregation(VecAggState* node)
{
    PlanState* plan_state = &node->ss.ps;
    VecAgg* plan = (VecAgg*)node->ss.ps.plan;

    if (plan_state->earlyFreed)
        return;

    if (plan->aggstrategy == AGG_HASHED) {
        if (plan->is_sonichash) {
            SonicHashAgg* vshash_agg = (SonicHashAgg*)node->aggRun;
            if (vshash_agg != NULL) {
                vshash_agg->freeMemoryContext();
            }
        } else {
            HashAggRunner* vec_hash_tbl = (HashAggRunner*)node->aggRun;
            if (vec_hash_tbl != NULL) {
                vec_hash_tbl->closeFile();
                vec_hash_tbl->freeMemoryContext();
            }
        }
    } else if (plan->aggstrategy == AGG_SORTED) {
        SortAggRunner* vec_sort_agg = (SortAggRunner*)node->aggRun;
        if (vec_sort_agg != NULL) {
            vec_sort_agg->endSortAgg();
        }
    } else {
        PlainAggRunner* vec_plain_agg = (PlainAggRunner*)node->aggRun;
        if (vec_plain_agg != NULL) {
            vec_plain_agg->endPlainAgg();
        }
    }

    /*
     * Free both the expr contexts.
     */
    ExecFreeExprContext(&node->ss.ps);
    node->ss.ps.ps_ExprContext = node->tmpcontext;
    ExecFreeExprContext(&node->ss.ps);

    /* clean up tuple table */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Agg "
        "at node %d, memory used %d MB.",
        plan->plan.plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));
}
