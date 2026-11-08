/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * ogai.cpp
 *    OGAI extension implementation
 *
 * IDENTIFICATION
 *    contrib/ogai/ogai.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/extension.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern "C" void create_ogai_extension(void)
{
    int ret;

    start_xact_command();
    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("OGAI SPI_connect failed: %s", SPI_result_code_string(ret))));
    }

    if (SPI_execute("CREATE EXTENSION IF NOT EXISTS ogai", false, 0) != SPI_OK_UTILITY) {
        ereport(WARNING,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("invalid query: CREATE EXTENSION IF NOT EXISTS ogai")));
    }

    SPI_finish();
    finish_xact_command();
}

/*
 * init_ogai_object
 *     Initialize OGAI hooks and objects.
 *     This function is called after the extension is loaded.
 */
extern "C" void init_ogai_object(void)
{
    /* Currently no hooks to initialize */
    /* Future: Initialize any function hooks or callbacks here */
    ereport(DEBUG1,
            (errmsg("OGAI extension objects initialized")));
}

/*
 * _PG_init
 *     Library initialization function (called when shared library is loaded).
 */
extern "C" void _PG_init(void)
{
    /* Register background workers, GUC variables, etc. if needed */
    ereport(DEBUG1,
            (errmsg("OGAI extension library loaded")));
}

/*
 * _PG_fini
 *     Library cleanup function (called when shared library is unloaded).
 */
extern "C" void _PG_fini(void)
{
    /* Cleanup any resources allocated in _PG_init */
    ereport(DEBUG1,
            (errmsg("OGAI extension library unloaded")));
}

/*
 * Execute one already validated HybridRecall marker query with two external
 * parameters and materialize every result row as JSONB.  SPI_execute_with_args
 * drives ExecutorRun to completion before returning; unlike a PL/pgSQL FOR
 * cursor, this gives the two task streams one complete stream-flow lifecycle.
 */
PG_FUNCTION_INFO_V1(ogai_hybrid_search_v2_execute);
extern "C" Datum ogai_hybrid_search_v2_execute(PG_FUNCTION_ARGS);

Datum ogai_hybrid_search_v2_execute(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    MemoryContext per_query_ctx;
    MemoryContext old_context;
    Tuplestorestate* tupstore = NULL;
    TupleDesc result_desc;
    char* query = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Oid argtypes[2] = {TEXTOID, get_fn_expr_argtype(fcinfo->flinfo, 2)};
    Datum values[2] = {PG_GETARG_DATUM(1), PG_GETARG_DATUM(2)};
    int spi_result;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
        !(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("hybrid_search_v2 materialize mode is required")));
    if (!OidIsValid(argtypes[1]))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid_search_v2 could not resolve the query vector type")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    old_context = MemoryContextSwitchTo(per_query_ctx);
    result_desc = CreateTemplateTupleDesc(1, false);
    TupleDescInitEntry(result_desc, (AttrNumber)1, "result_record", JSONBOID, -1, 0);
    result_desc = BlessTupleDesc(result_desc);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = result_desc;
    (void)MemoryContextSwitchTo(old_context);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("hybrid_search_v2 SPI_connect failed")));

    spi_result = SPI_execute_with_args(query, 2, argtypes, values, NULL, true, 0, NULL, NULL);
    if (spi_result != SPI_OK_SELECT || SPI_tuptable == NULL) {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("hybrid_search_v2 query execution failed: %s",
                        SPI_result_code_string(spi_result))));
    }

    for (uint64 i = 0; i < SPI_processed; i++) {
        HeapTupleHeader row = SPI_returntuple(SPI_tuptable->vals[i], SPI_tuptable->tupdesc);
        Datum json = DirectFunctionCall1(row_to_json, PointerGetDatum(row));
        char* json_text = text_to_cstring(DatumGetTextP(json));
        Datum jsonb = DirectFunctionCall1(jsonb_in, CStringGetDatum(json_text));
        Datum output_values[1] = {jsonb};
        bool output_nulls[1] = {false};

        tuplestore_putvalues(tupstore, result_desc, output_values, output_nulls);
    }

    SPI_finish();
    tuplestore_donestoring(tupstore);
    PG_RETURN_NULL();
}
