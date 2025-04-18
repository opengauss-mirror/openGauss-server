/*
* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * query_parameterization_views.cpp
 *      A catlog view that stores information about parameterized queries
 *
 * IDENTIFICATION
 *        src/common/backend/catalog/query_parameterization_views.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "knl/knl_session.h"
#include "nodes/pg_list.h"
#include "utils/plancache.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "funcapi.h"

#include "commands/auto_parameterization.h"
#include "catalog/query_parameterization_views.h"


static void FillParamViewValues(Datum* paramViewsValues, bool* paramViewsNulls, ParamView* paramView);
static int2vector* MakeInt2Vec(Oid* paramTypes, int paramNums);
static char* MakeQueryTypeString(Node* parsetree);

static int2vector* MakeInt2Vec(Oid* paramTypes, int paramNums)
{
    int2vector* vec = buildint2vector(NULL, paramNums);
    for (int i = 0; i < paramNums; i++) {
        vec->values[i] = (int2)paramTypes[i];
    }
    return vec;
}

static void FillParamViewValues(Datum* paramViewsValues, bool* paramViewsNulls, ParamView* paramView)
{
    paramViewsValues[Anum_parameterization_views_reloid - 1] = ObjectIdGetDatum(paramView->relOid);
    paramViewsValues[Anum_parameterization_views_is_bypass - 1] = BoolGetDatum(paramView->isBypass);
    paramViewsValues[Anum_parameterization_views_query_type - 1] = NameGetDatum(paramView->queryType);
    if (paramView->paramNums > 0) {
        paramViewsValues[Anum_parameterization_views_types - 1] = PointerGetDatum(paramView->paramTypes);
    } else {
        paramViewsNulls[Anum_parameterization_views_types - 1] = true;
    }
    paramViewsValues[Anum_parameterization_views_param_nums - 1] = Int16GetDatum(paramView->paramNums);
    paramViewsValues[Anum_parameterization_views_parameterized_query - 1] =
        CStringGetTextDatum(paramView->parameterizedQuery);
    return;
}

ParamView* GetAllParamQueries(uint32* num)
{
    HASH_SEQ_STATUS seq;
    uint32 viewIndex = 0;
    bool found = false;
    CachedPlanSource* psrc = NULL;
    ParamCachedPlan* entry = NULL;

    ParamView* results = (ParamView*)palloc(sizeof(ParamView) * MAX_PARAMETERIZED_QUERY_STORED);
    int2vector* vec = NULL;
    hash_seq_init(&seq, u_sess->param_cxt.parameterized_queries);
    while (((entry = (ParamCachedPlan*)hash_seq_search(&seq))) != NULL) {
        psrc = entry->psrc;
        if (!psrc->is_valid) {
            continue;
        }
        Assert(psrc->magic == CACHEDPLANSOURCE_MAGIC);
        ParamCachedKey pck = entry->paramCachedKey;
        results[viewIndex].relOid = pck.relOid;
        results[viewIndex].isBypass = (psrc->opFusionObj != NULL);
        results[viewIndex].queryType = MakeQueryTypeString(psrc->raw_parse_tree);
        results[viewIndex].paramNums = pck.num_param;
        if (pck.num_param > 0) {
            vec = MakeInt2Vec(psrc->param_types, psrc->num_params);
            results[viewIndex].paramTypes = int2vectorCopy(vec);
        } else {
            results[viewIndex].paramTypes = NULL;
        }
        results[viewIndex].parameterizedQuery = psrc->query_string;
        viewIndex++;
    }
    *num = viewIndex;
    pfree_ext(vec);
    return results;
}

Datum query_parameterization_views(PG_FUNCTION_ARGS)
{
    Datum result;
    ParamView* paramView = NULL;
    FuncCallContext* funcCtx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupDesc = NULL;
        MemoryContext oldContext = NULL;
        funcCtx = SRF_FIRSTCALL_INIT();

        if (u_sess->param_cxt.parameterized_queries == NULL) {
            SRF_RETURN_DONE(funcCtx);
        }
        
        oldContext = MemoryContextSwitchTo(funcCtx->multi_call_memory_ctx);
        tupDesc = CreateTemplateTupleDesc(Natts_parameterization_views, false, TableAmHeap);

        TupleDescInitEntry(tupDesc, (AttrNumber)Anum_parameterization_views_reloid, "reloid", OIDOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber)Anum_parameterization_views_query_type, "query_type", NAMEOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber)Anum_parameterization_views_is_bypass, "is_bypass", BOOLOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber)Anum_parameterization_views_types, "param_types", INT2VECTOROID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber)Anum_parameterization_views_param_nums, "param_nums", INT2OID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber)Anum_parameterization_views_parameterized_query, "parameterized_query",
                           TEXTOID, -1, 0);

        funcCtx->user_fctx = GetAllParamQueries(&(funcCtx->max_calls));
        funcCtx->tuple_desc = BlessTupleDesc(tupDesc);
        (void)MemoryContextSwitchTo(oldContext);
    }

    funcCtx = SRF_PERCALL_SETUP();

    paramView = (ParamView*)funcCtx->user_fctx;
    if (funcCtx->call_cntr < funcCtx->max_calls) {
        HeapTuple tuple = NULL;
        Datum paramViewsValues[Natts_parameterization_views];
        bool paramViewsNulls[Natts_parameterization_views] = {false};

        paramView += funcCtx->call_cntr;
        FillParamViewValues(paramViewsValues, paramViewsNulls, paramView);
        tuple = heap_form_tuple(funcCtx->tuple_desc, paramViewsValues, paramViewsNulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcCtx, result);
    } else {
        SRF_RETURN_DONE(funcCtx);
    }
    return 0;
}

static char* MakeQueryTypeString(Node* parsetree)
{
    char* res = NULL;
    switch (nodeTag(parsetree)) {
        case T_InsertStmt:
            res = query_type_text[QUERY_TYPE_INSERT];
            break;
        case T_UpdateStmt:
            res = query_type_text[QUERY_TYPE_UPDATE];
            break;
        case T_DeleteStmt:
            res = query_type_text[QUERY_TYPE_DELETE];
            break;
        default:
            res = query_type_text[QUERY_TYPE_UNKNOWN];
            break;
    }

    return res;
}