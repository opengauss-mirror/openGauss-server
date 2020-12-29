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
 * src/common/backend/tsearch/ts_zh_zhparser.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/tsearch/ts_zh_zhparser.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "executor/executor.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "tsearch/ts_cache.h"
#include "tsearch/ts_public.h"
#include "tsearch/ts_zh_zhparser.h"
#include "utils/guc.h"
#include "utils/builtins.h"

/* prototypes */
PG_FUNCTION_INFO_V1(zhprs_start);
Datum zhprs_start(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(zhprs_getlexeme);
Datum zhprs_getlexeme(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(zhprs_end);
Datum zhprs_end(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(zhprs_lextype);
Datum zhprs_lextype(PG_FUNCTION_ARGS);

/* deprecated function zhparser, caller can only get error report or NULL return (with try-catch) */
Datum zhprs_start(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errmodule(MOD_TS), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Zhparser is not supported!")));
    PG_RETURN_POINTER(NULL);
}

/* deprecated function zhparser, caller can only get error report or NULL return (with try-catch) */
Datum zhprs_getlexeme(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errmodule(MOD_TS), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Zhparser is not supported!")));
    PG_RETURN_POINTER(NULL);
}

/* deprecated function zhparser, caller can only get error report or NULL return (with try-catch) */
Datum zhprs_end(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errmodule(MOD_TS), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Zhparser is not supported!")));
    PG_RETURN_POINTER(NULL);
}

/* deprecated function zhparser, caller can only get error report or NULL return (with try-catch) */
Datum zhprs_lextype(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errmodule(MOD_TS), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Zhparser is not supported!")));
    PG_RETURN_POINTER(NULL);
}