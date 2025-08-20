/* -------------------------------------------------------------------------
 *
 * pl_funcs.c           - Misc functions for the PL/pgSQL
 *                        procedural language
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *        src/pl/plpgsql/src/pl_funcs.c
 *
 * -------------------------------------------------------------------------
 */

#include "utils/builtins.h"
#include "utils/plpgsql_domain.h"
#include "utils/plpgsql.h"
#include "optimizer/pgxcship.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pl_package.h"
#include "catalog/gs_package.h"


extern "C" {
Datum error_number(PG_FUNCTION_ARGS);
Datum error_severity(PG_FUNCTION_ARGS);
Datum error_state(PG_FUNCTION_ARGS);
Datum error_procedure(PG_FUNCTION_ARGS);
Datum error_line(PG_FUNCTION_ARGS);
Datum error_message(PG_FUNCTION_ARGS);
}

PG_FUNCTION_INFO_V1(error_number);
PG_FUNCTION_INFO_V1(error_severity);
PG_FUNCTION_INFO_V1(error_state);
PG_FUNCTION_INFO_V1(error_procedure);
PG_FUNCTION_INFO_V1(error_line);
PG_FUNCTION_INFO_V1(error_message);

void CheckEdata()
{
    if (u_sess->plsql_cxt.cur_exception_cxt->cur_edata == NULL) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("trycatch error data was lost.")));
    }
}

Datum error_number(PG_FUNCTION_ARGS)
{
    if (u_sess->plsql_cxt.cur_exception_cxt == NULL) {
        PG_RETURN_NULL();
    }
    CheckEdata();

    ErrorData* edata = u_sess->plsql_cxt.cur_exception_cxt->cur_edata;
    PG_RETURN_INT32(edata->sqlerrcode);
}

Datum error_severity(PG_FUNCTION_ARGS)
{
    if (u_sess->plsql_cxt.cur_exception_cxt == NULL) {
        PG_RETURN_NULL();
    }
    CheckEdata();
    
    ErrorData* edata = u_sess->plsql_cxt.cur_exception_cxt->cur_edata;
    PG_RETURN_INT32(edata->elevel);
}

Datum error_state(PG_FUNCTION_ARGS)
{
    if (u_sess->plsql_cxt.cur_exception_cxt == NULL) {
        PG_RETURN_NULL();
    }
    CheckEdata();

    PG_RETURN_INT32(1);
}

Datum error_procedure(PG_FUNCTION_ARGS)
{
    if (u_sess->plsql_cxt.cur_exception_cxt == NULL) {
        PG_RETURN_NULL();
    }
    CheckEdata();

    ErrorData* edata = u_sess->plsql_cxt.cur_exception_cxt->cur_edata;
    if (edata->plpgsqlProcedure == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_TEXT_P(cstring_to_text(edata->plpgsqlProcedure));
}

Datum error_line(PG_FUNCTION_ARGS)
{
    if (u_sess->plsql_cxt.cur_exception_cxt == NULL) {
        PG_RETURN_NULL();
    }
    CheckEdata();

    ErrorData* edata = u_sess->plsql_cxt.cur_exception_cxt->cur_edata;
    PG_RETURN_INT32(edata->plpgsqlLine);
}

Datum error_message(PG_FUNCTION_ARGS)
{
    if (u_sess->plsql_cxt.cur_exception_cxt == NULL) {
        PG_RETURN_NULL();
    }
    CheckEdata();

    ErrorData* edata = u_sess->plsql_cxt.cur_exception_cxt->cur_edata;
    PG_RETURN_TEXT_P(cstring_to_text(edata->message));
}
