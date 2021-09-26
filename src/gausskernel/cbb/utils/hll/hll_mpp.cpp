/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * hll_mpp.cpp
 *    distribute hll aggregation
 *
 * Description: distribute hll aggregation
 *
 * The design of HLL distribute agg
 * ---------------------------------
 *
 *            client
 *             |
 *             CN <---(2) collect phase (union hll_trans_type) (3) final phase (hll_trans_type to hll_type)
 *            /  \
 *           /    \ <-- dn pass the partial aggregated hll_trans_type value to cn
 *          /      \
 *        DN1      DN2 <---(1) trans phase: iterate every tuple on DN and apply transfunc
 *      -----     -----
 *      | 1 |     | 2 |
 *      | 3 |     | 4 |
 *      | 5 |     | 6 | <--- tuples
 *      | 7 |     | 8 |
 *      -----     -----
 *
 * Thanks to the nature of HyperLogLog algorithm we can use three phased aggregation
 * for the sake of ultimate performance in distribute computing.
 * In Three-Phased-Agg each DN can apply the thrans phase function
 * on the hashval/hll available at the Datanode. The result of transition
 * phase is then transferred to the Coordinator node for collect & final
 * phase processing.
 *
 * We create a temporary variable of data type namely hll_trans_type
 * to hold the current internal state of the trans phase and collection phase
 * For every input from the datanode(result of transition phase on that node),
 * the collection function is invoked with the current collection state
 * value and the new transition value(obtained from the datanode) to
 * caculate a new internal collection state value.
 *
 * After all the transition values from the data nodes have been processed.
 * In the third or finalization phase the final function is invoked once
 * to covert the internal state type(hll_trans_type) to the hll type.
 *
 *
 * A word about the hll_trans_type
 * ---------------------------------
 * we have two different data structure to repersent HyperLogLog
 * namely the hll_type(packed) and hll_trans_type(unpakced). The packed type
 * is small in size(1.2k typical) and good for storing the hll data in a table
 * and pass hll around through network channel. However it not well suited
 * for computing(add/union of hll). So we have a hll_trans_type(unpakced)
 * to do the computing and for internal state passing in distribute aggeration.
 *
 * Compared to greenplumn implementation of distribute hll, we avoid
 * the high cost of pack-unpack. Therefore we have a performance boost
 * under distribute aggeration. Roughly, 1x-10x performance gain compared with
 * sort/hash agg implementation of distinct algorithm.
 *
 * hll_add_agg
 * ------------
 * trans phase function  : hll_trans_type hll_add_trans0(hll_trans_type, hll_hash_val)
 * collect phase function: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 * final phase function  : hll hll_pack(hll_trans_type)
 *
 *            client
 *             |
 *             CN <---(2) collect phase: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 *                <---(3) final phase  : hll hll_pack(hll_trans_type)
 *            /  \
 *           /    \ <-- dn pass the partial aggregated hll_trans_type value to cn
 *          /      \
 *        DN1      DN2
 *      -----     -----
 *      | 1 |     | 2 |
 *      | 3 |     | 4 |
 *      | 5 |     | 6 | <--- (1) trans phase: hll_trans_type hll_add_trans0(hll_trans_type, hll_hash_val)
 *      | 7 |     | 8 |
 *      -----     -----
 *
 * hll_union_agg
 * --------------
 * trans phase function  : hll_trans_type hll_union_trans(hll_trans_type, hll)
 * collect phase function: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 * final phase function  : hll hll_pack(hll_trans_type)
 *
 *            client
 *             |
 *             CN <---(2) collect phase: hll_trans_type hll_union_collect(hll_trans_type, hll_trans_type)
 *                <---(3) final phase  : hll hll_pack(hll_trans_type)
 *            /  \
 *           /    \ <-- dn pass the partial aggregated hll_trans_type value to cn
 *          /      \
 *        DN1      DN2
 *      -----     -----
 *      | 1 |     | 2 |
 *      | 3 |     | 4 |
 *      | 5 |     | 6 | <--- (1) trans phase:  hll_trans_type hll_union_trans(hll_trans_type, hll)
 *      | 7 |     | 8 |
 *      -----     -----
 * IDENTIFICATION
 *    src/gausskernel/cbb/utils/hll/hll_mpp.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <postgres.h>

#include "catalog/pg_type.h"
#include "catalog/pg_aggregate.h"
#include "commands/extension.h"
#include "nodes/nodeFuncs.h"
#include "libpq/pqformat.h"
#include "utils/hll.h"
#include "utils/hll_mpp.h"
#include "utils/bytea.h"

PG_FUNCTION_INFO_V1(hll_add_trans0);
PG_FUNCTION_INFO_V1(hll_add_trans1);
PG_FUNCTION_INFO_V1(hll_add_trans2);
PG_FUNCTION_INFO_V1(hll_add_trans3);
PG_FUNCTION_INFO_V1(hll_add_trans4);
PG_FUNCTION_INFO_V1(hll_union_collect);
PG_FUNCTION_INFO_V1(hll_union_trans);
PG_FUNCTION_INFO_V1(hll_pack);
PG_FUNCTION_INFO_V1(hll_trans_recv);
PG_FUNCTION_INFO_V1(hll_trans_send);
PG_FUNCTION_INFO_V1(hll_trans_in);
PG_FUNCTION_INFO_V1(hll_trans_out);

static void handleParameter(HllPara *hllpara, FunctionCallInfo fcinfo);
static bool contain_hll_agg_walker(Node *node, int *context);
static int contain_hll_agg(Node *node);

static Datum hll_add_trans_n(FunctionCallInfo fcinfo);

/* Support disabling hash aggregation functionality for PG > 9.6 */
#if PG_VERSION_NUM >= 90600

#define EXTENSION_NAME "hll"
#define ADD_AGG_NAME "hll_add_agg"
#define UNION_AGG_NAME "hll_union_agg"
#define HLL_AGGREGATE_COUNT 6

static Oid hllAggregateArray[HLL_AGGREGATE_COUNT];
static bool aggregateValuesInitialized = false;
static bool ForceGroupAgg = false;
static create_upper_paths_hook_type previous_upper_path_hook;

void _PG_init(void);
void _PG_fini(void);
#if (PG_VERSION_NUM >= 110000)
static void hll_aggregation_restriction_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
    RelOptInfo *output_rel, void *extra);
#else
static void hll_aggregation_restriction_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
    RelOptInfo *output_rel);
#endif
static void InitializeHllAggregateOids(void);
static Oid FunctionOid(const char *schemaName, const char *functionName, int argumentCount, bool missingOk);
static void MaximizeCostOfHashAggregate(Path *path);
static bool HllAggregateOid(Oid aggregateOid);
static void RegisterConfigVariables(void);

/* _PG_init is the shared library initialization function */
void _PG_init(void)
{
    /*
     * Register HLL configuration variables.
     */
    RegisterConfigVariables();

    previous_upper_path_hook = create_upper_paths_hook;
    create_upper_paths_hook = hll_aggregation_restriction_hook;
}

/* _PG_fini uninstalls extension hooks */
void _PG_fini(void)
{
    create_upper_paths_hook = previous_upper_path_hook;
}

/*
 * hll_aggregation_restriction_hook is assigned to create_upper_paths_hook to
 * check whether there exist a path with hash aggregate. If that aggregate is
 * introduced by hll, it's cost is maximized to force planner to not to select
 * hash aggregate.
 *
 * Since the signature of the hook changes after PG 11, we define the signature
 * and the previous hook call part of this function depending on the PG version.
 */
static oid
#if (PG_VERSION_NUM >= 110000)
hll_aggregation_restriction_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
    RelOptInfo *output_rel, void *extra)
#else
hll_aggregation_restriction_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
    RelOptInfo *output_rel)
#endif
{
    Oid extensionOid = InvalidOid;

    /* If previous hook exist, call it first to get most update path */
    if (previous_upper_path_hook != NULL) {
#if (PG_VERSION_NUM >= 110000)
        previous_upper_path_hook(root, stage, input_rel, output_rel, extra);
#else
        previous_upper_path_hook(root, stage, input_rel, output_rel);
#endif
    }

    /* If HLL extension is not loaded, do nothing */
    extensionOid = get_extension_oid(EXTENSION_NAME, true);
    if (!OidIsValid(extensionOid)) {
        return;
    }

    /* If we have the extension, that means we also have aggregations */
    if (!aggregateValuesInitialized) {
        InitializeHllAggregateOids();
    }

    /*
     * If the client force the group agg, maximize the cost of the path with
     * the hash agg to force planner to choose group agg instead.
     */
    if (ForceGroupAgg) {
        if (stage == UPPERREL_GROUP_AGG || stage == UPPERREL_FINAL) {
            ListCell *pathCell = list_head(output_rel->pathlist);
            foreach (pathCell, output_rel->pathlist) {
                Path *path = (Path *)lfirst(pathCell);
                if (path->pathtype == T_Agg && ((AggPath *)path)->aggstrategy == AGG_HASHED) {
                    MaximizeCostOfHashAggregate(path);
                }
            }
        }
    }
}

/*
 * InitializeHllAggregateOids initializes the array of hll aggregate oids.
 */
static void InitializeHllAggregateOids()
{
    Oid extensionId = get_extension_oid(EXTENSION_NAME, false);
    Oid hllSchemaOid = get_extension_schema(extensionId);
    const char *hllSchemaName = get_namespace_name(hllSchemaOid);
    char *aggregateName = NULL;
    Oid aggregateOid = InvalidOid;
    int addAggArgumentCounter;

    /* Initialize HLL_UNION_AGG oid */
    aggregateName = UNION_AGG_NAME;
    aggregateOid = FunctionOid(hllSchemaName, aggregateName, 1, true);
    hllAggregateArray[0] = aggregateOid;

    /* Initialize HLL_ADD_AGG with different signatures */
    aggregateName = ADD_AGG_NAME;
    for (addAggArgumentCounter = 1; addAggArgumentCounter < HLL_AGGREGATE_COUNT; addAggArgumentCounter++) {
        aggregateOid = FunctionOid(hllSchemaName, aggregateName, addAggArgumentCounter, true);
        hllAggregateArray[addAggArgumentCounter] = aggregateOid;
    }

    aggregateValuesInitialized = true;
}

/*
 * FunctionOid searches for a given function identified by schema, functionName
 * and argumentCount. It reports error if the function is not found or there
 * are more than one match. If the missingOK parameter is set and there are
 * no matches, then the function returns InvalidOid.
 */
static Oid FunctionOid(const char *schemaName, const char *functionName, int argumentCount, bool missingOK)
{
    FuncCandidateList functionList = NULL;
    Oid functionOid = InvalidOid;

    char *qualifiedFunctionName = quote_qualified_identifier(schemaName, functionName);
    List *qualifiedFunctionNameList = stringToQualifiedNameList(qualifiedFunctionName);
    List *argumentList = NIL;
    const bool findVariadics = false;
    const bool findDefaults = false;

    functionList = FuncnameGetCandidates(qualifiedFunctionNameList, argumentCount, argumentList, findVariadics,
        findDefaults, true);

    if (functionList == NULL) {
        if (missingOK) {
            return InvalidOid;
        }

        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("function \"%s\" does not exist", functionName)));
    } else if (functionList->next != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_AMBIGUOUS_FUNCTION), errmsg("more than one function named \"%s\"", functionName)));
    }

    /* get function oid from function list's head */
    functionOid = functionList->oid;

    return functionOid;
}

/*
 * MaximizeCostOfHashAggregate maximizes the cost of the path if it tries
 * to run hll aggregate function with hash aggregate.
 */
static void MaximizeCostOfHashAggregate(Path *path)
{
    List *varList = pull_var_clause((Node *)path->pathtarget->exprs, PVC_INCLUDE_AGGREGATES);
    ListCell *varCell = NULL;

    foreach (varCell, varList) {
        Var *var = (Var *)lfirst(varCell);

        if (nodeTag(var) == T_Aggref) {
            Aggref *aggref = (Aggref *)var;

            if (HllAggregateOid(aggref->aggfnoid)) {
                path->total_cost = INT_MAX;
            }
        }
    }
}

/*
 * HllAggregateOid checkes whether the given Oid is an id of any hll aggregate
 * function using the pre-initialized hllAggregateArray.
 */
static bool HllAggregateOid(Oid aggregateOid)
{
    int arrayCounter;

    for (arrayCounter = 0; arrayCounter < HLL_AGGREGATE_COUNT; arrayCounter++) {
        if (aggregateOid == hllAggregateArray[arrayCounter]) {
            return true;
        }
    }

    return false;
}

/* Register HLL configuration variables. */
static void RegisterConfigVariables(void)
{
    DefineCustomBoolVariable("hll.force_groupagg",
        gettext_noop("Forces using group aggregate with hll aggregate functions"), NULL, &ForceGroupAgg, false,
        PGC_USERSET, 0, NULL, NULL, NULL);
}

#endif

/* similar to function hll_emptyn */
static void handleParameter(HllPara *hllpara, FunctionCallInfo fcinfo)
{
    int32_t log2Registers = u_sess->attr.attr_sql.hll_default_log2m;
    int32_t log2Explicitsize = u_sess->attr.attr_sql.hll_default_log2explicit;
    int32_t log2Sparsesize = u_sess->attr.attr_sql.hll_default_log2sparse;
    int32_t duplicateCheck = u_sess->attr.attr_sql.hll_duplicate_check;

    switch (PG_NARGS()) {
        case HLL_AGG_PARAMETER4:
            if (!PG_ARGISNULL(5) && PG_GETARG_INT32(5) != -1) {
                duplicateCheck = PG_GETARG_INT32(5);
            }
        case HLL_AGG_PARAMETER3:
            if (!PG_ARGISNULL(4) && PG_GETARG_INT64(4) != -1) {
                log2Sparsesize = (int32_t)PG_GETARG_INT64(4);
            }
        case HLL_AGG_PARAMETER2:
            if (!PG_ARGISNULL(3) && PG_GETARG_INT32(3) != -1) {
                log2Explicitsize = PG_GETARG_INT32(3);
            }
        case HLL_AGG_PARAMETER1:
            if (!PG_ARGISNULL(2) && PG_GETARG_INT32(2) != -1) {
                log2Registers = PG_GETARG_INT32(2);
            }
        case HLL_AGG_PARAMETER0:
            break;
        default: /* can never reach here */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("no such parameters of input")));
    }
    HllCheckPararange(log2Registers, log2Explicitsize, log2Sparsesize, duplicateCheck);

    hllpara->log2Registers = (uint8_t)log2Registers;
    hllpara->log2Explicitsize = (uint8_t)log2Explicitsize;
    hllpara->log2Sparsesize = (uint8_t)log2Sparsesize;
    hllpara->duplicateCheck = (uint8_t)duplicateCheck;
}

/*
 * @Description: transfunc for agg func hll_add_agg()
 *
 * these function works on the first phase of aggeration.
 * namely the transfunc. And they are *not* strict regrading
 * of NULL handling upon the first invoke of the function in
 * the aggretation.
 *
 * we provide hll_add_trans function with different number
 * of parameters which maximum to four and minimum to zero
 */
Datum hll_add_trans0(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans1(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans2(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans3(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

Datum hll_add_trans4(PG_FUNCTION_ARGS)
{
    return hll_add_trans_n(fcinfo);
}

/*
 * @Description:
 * this function will be called by hll_add_agg, similar to function hll_add.
 *
 * @param[IN] hll: NULL indicates the first call, hll passed as input.
 * @param[IN] hashval: the hashval need to be add to multiset_t.
 * @return hll.
 */
static Datum hll_add_trans_n(FunctionCallInfo fcinfo)
{
    /* We must be called as a transition routine or we fail. */
    MemoryContext aggctx = NULL;
    if (!AggCheckCallContext(fcinfo, &aggctx)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_add_trans_n outside transition context")));
    }

    /* handle numbers of parameter we need to take */
    HllPara hllpara;
    handleParameter(&hllpara, fcinfo);

    Hll hll(aggctx);
    hll.HllInit();

    /*
     If the first argument is a NULL on first call, init an hll_empty
     If it's NON-NULL, unpacked it
     */
    if (PG_ARGISNULL(0)) {
        hll.HllEmpty();
        hll.HllSetPara(hllpara);
    } else {
        bytea *byteval1 = PG_GETARG_BYTEA_P(0);
        size_t hllsize1 = BYTEVAL_LENGTH(byteval1);
        hll.HllObjectUnpack((uint8_t *)VARDATA(byteval1), hllsize1);
    }

    /* Is the second argument non-null? */
    if (!PG_ARGISNULL(1)) {
        uint64_t hashvalue = (uint64_t)PG_GETARG_INT64(1);
        hll.HllAdd(hashvalue);
    }

    /* packed hll then return it, hllsize should allocate at least 1 more byte */
    size_t hllsize2 = HLL_HEAD_SIZE + hll.HllDatalen() + 1;
    bytea *byteval2 = (bytea *)palloc0(VARHDRSZ + hllsize2);
    SET_VARSIZE(byteval2, VARHDRSZ + hllsize2);
    hll.HllObjectPack((uint8_t *)VARDATA(byteval2), hllsize2);
    hll.HllFree();

    PG_RETURN_BYTEA_P(byteval2);
}

/*
 * @Description: collectfunc for agg func hll_add_agg() and hll_union_agg(), similar to function hll_union.
 *
 * @param[IN] hll: NULL indicates the first call, passed in the
 * transvalue in the successive call
 * @param[IN] hll: the other hll to be unioned.
 * @return the unioned hll as the transvalue.
 */
Datum hll_union_collect(PG_FUNCTION_ARGS)
{
    /*
     * If both arguments are null then we return null, because we shouldn't
     * return any hll with UNINIT type state.
     */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }

    /* now inputs are all hll, we need unpack them, calculate and return hll. */
    MemoryContext aggctx = NULL;
    if (!AggCheckCallContext(fcinfo, &aggctx)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_union_collect outside transition context")));
    }

    Hll hll1(aggctx);
    hll1.HllInit();
    if (!PG_ARGISNULL(0)) {
        bytea *byteval1 = PG_GETARG_BYTEA_P(0);
        size_t hllsize1 = BYTEVAL_LENGTH(byteval1);
        hll1.HllObjectUnpack((uint8_t *)VARDATA(byteval1), hllsize1);
    }

    if (!PG_ARGISNULL(1)) {
        Hll hll2(aggctx);
        hll2.HllInit();

        bytea *byteval2 = PG_GETARG_BYTEA_P(1);
        size_t hllsize2 = BYTEVAL_LENGTH(byteval2);
        hll2.HllObjectUnpack((uint8_t *)VARDATA(byteval2), hllsize2);

        if (hll1.HllGetType() != HLL_UNINIT) {
            HllCheckParaequal(*hll1.HllGetPara(), *hll2.HllGetPara());
        }
        hll1.HllUnion(hll2);
        hll2.HllFree();
    }

    /* packed multiset_t to hll then return it, hllsize should allocate at least 1 more byte */
    size_t hllsize3 = HLL_HEAD_SIZE + hll1.HllDatalen() + 1;
    bytea *byteval3 = (bytea *)palloc0(VARHDRSZ + hllsize3);
    SET_VARSIZE(byteval3, VARHDRSZ + hllsize3);
    hll1.HllObjectPack((uint8_t *)VARDATA(byteval3), hllsize3);
    hll1.HllFree();

    PG_RETURN_BYTEA_P(byteval3);
}

/*
 * @Description: transfunc for agg func hll_union_agg()
 *
 * this function works on the first stage for agg func hll_union_agg()
 *
 * @param[IN] hll: the transvalue for aggeration
 * @param[IN] hll: passed column(type is hll) for hll_union_agg()
 * @return hll
 */
Datum hll_union_trans(PG_FUNCTION_ARGS)
{
    /*
     * If both arguments are null then we return null, because we shouldn't
     * return any hll with UNINIT type state.
     */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }

    /* now inputs are all hll, we need unpack them, calculate and return hll. */
    MemoryContext aggctx = NULL;
    if (!AggCheckCallContext(fcinfo, &aggctx)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll_union_trans outside transition context")));
    }

    Hll hll1(aggctx);
    hll1.HllInit();
    if (!PG_ARGISNULL(0)) {
        bytea *byteval1 = PG_GETARG_BYTEA_P(0);
        size_t hllsize1 = BYTEVAL_LENGTH(byteval1);
        hll1.HllObjectUnpack((uint8_t *)VARDATA(byteval1), hllsize1);
    }

    if (!PG_ARGISNULL(1)) {
        Hll hll2(aggctx);
        hll2.HllInit();

        bytea *byteval2 = PG_GETARG_BYTEA_P(1);
        size_t hllsize2 = BYTEVAL_LENGTH(byteval2);
        hll2.HllObjectUnpack((uint8_t *)VARDATA(byteval2), hllsize2);

        if (hll1.HllGetType() > HLL_UNINIT) {
            HllCheckParaequal(*hll1.HllGetPara(), *hll2.HllGetPara());
        }
        hll1.HllUnion(hll2);
        hll2.HllFree();
    }

    /* packed multiset_t to hll then return it, hllsize should allocate at least 1 more byte */
    size_t hllsize3 = HLL_HEAD_SIZE + hll1.HllDatalen() + 1;
    bytea *byteval3 = (bytea *)palloc0(VARHDRSZ + hllsize3);
    SET_VARSIZE(byteval3, VARHDRSZ + hllsize3);
    hll1.HllObjectPack((uint8_t *)VARDATA(byteval3), hllsize3);
    hll1.HllFree();

    PG_RETURN_BYTEA_P(byteval3);
}

/*
 * @Description: finalfunc for agg func hll_add_agg() and hll_union_agg()
 *
 * this function works on the final stage for hll agg funcs
 */
Datum hll_pack(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);
    HllType type = hll.HllGetType();

    hll.HllFree();

    /* Was the aggregation uninitialized? */
    if (type == HLL_UNINIT) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_BYTEA_P(byteval);
    }
}

Datum hll_trans_recv(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));
}

Datum hll_trans_send(PG_FUNCTION_ARGS)
{
    bytea *byteval = (bytea *)PG_GETARG_POINTER(0);

    StringInfoData buf;
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA(byteval), BYTEVAL_LENGTH(byteval));
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum hll_trans_in(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));
}

Datum hll_trans_out(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));
}

static bool contain_hll_agg_walker(Node *node, int *context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Aggref)) {
        Aggref *arf = (Aggref *)node;
        if (arf->aggfnoid == HLL_ADD_TRANS0_OID || arf->aggfnoid == HLL_ADD_TRANS1_OID ||
            arf->aggfnoid == HLL_ADD_TRANS2_OID || arf->aggfnoid == HLL_ADD_TRANS3_OID ||
            arf->aggfnoid == HLL_ADD_TRANS4_OID || arf->aggfnoid == HLL_UNION_TRANS_OID)
            (*context)++;
        return false;
    }

    return expression_tree_walker(node, (bool (*)())contain_hll_agg_walker, context);
}

static int contain_hll_agg(Node *node)
{
    int count = 0;

    (void)contain_hll_agg_walker(node, &count);
    return count;
}

/*
 * @Description: the function is uesd for estimate used memory in hll mode.
 *
 * we use function contain_hll_agg and contain_hll_agg_walker to parse query node
 * to get numbers of hll_agg functions.
 *
 * @return: the size of hash_table_size in hll mode which is used for estimate e-memory.
 */
double estimate_hllagg_size(double numGroups, List *tleList)
{
    int tmp = contain_hll_agg((Node *)tleList);
    double hash_table_size;

    int64_t hll_size = (int64_t)(HLL_HEAD_SIZE + BITS_TO_VALUE(u_sess->attr.attr_sql.hll_default_log2m));
    hash_table_size = ((numGroups * tmp * hll_size) / 1024L);

    return hash_table_size * 2;
}
