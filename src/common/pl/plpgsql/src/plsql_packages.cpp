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
 * plsql_packages.cpp
 *
 * IDENTIFICATION
 *    src/common/pl/plpgsql/src/plsql_packages.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/plpgsql.h"
#include "access/hash.h"
#include "utils/numeric.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum textregexsubstr_enforce_a(PG_FUNCTION_ARGS);
extern Datum regexp_substr(PG_FUNCTION_ARGS);
extern Datum intervaltonum(PG_FUNCTION_ARGS);
extern Datum report_application_error(PG_FUNCTION_ARGS);

extern "C" {
Datum regexp_substr(PG_FUNCTION_ARGS);
Datum intervaltonum(PG_FUNCTION_ARGS);
Datum report_application_error(PG_FUNCTION_ARGS);
}

PG_FUNCTION_INFO_V1(regexp_substr);
PG_FUNCTION_INFO_V1(intervaltonum);
PG_FUNCTION_INFO_V1(report_application_error);

// Convert interval(day) to numeric
Datum intervaltonum(PG_FUNCTION_ARGS)
{
    Interval* it = PG_GETARG_INTERVAL_P(0);
    Datum result;
#ifdef HAVE_INT64_TIMESTAMP
    float8 day = (float8)it->month * DAYS_PER_MONTH + (float8)it->day + (float8)it->time / USECS_PER_DAY;
    result = DirectFunctionCall3(numeric_in,
        DirectFunctionCall1(float8out, Float8GetDatumFast(day)),
        ObjectIdGetDatum(InvalidOid),
        Int32GetDatum(-1));
#else
    float4 day = (float4)it->month * DAYS_PER_MONTH + (float4)it->day + (float4)it->time / SECS_PER_DAY;
    result = DirectFunctionCall3(numeric_in,
        DirectFunctionCall1(float4out, Float8GetDatumFast(day)),
        ObjectIdGetDatum(InvalidOid),
        Int32GetDatum(-1));
#endif
    PG_RETURN_NUMERIC(result);
}

/*convert string to hexadecimal*/
Datum rawtohex(PG_FUNCTION_ARGS)
{
    char* str = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    Oid collation = PG_GET_COLLATION();
    const char* fmt = "HEX";
    Datum result;

    CHECK_RETNULL_INIT();
    result = CHECK_RETNULL_CALL2(binary_encode,
        collation,
        CHECK_RETNULL_CALL1(byteain, collation, CStringGetDatum(str)),
        CHECK_RETNULL_CALL1(textin, collation, CStringGetDatum(fmt)));

    CHECK_RETNULL_RETURN_DATUM(result);
}

/* report self-defined error: -20999 <= errorcode <=  -20000 */
Datum report_application_error(PG_FUNCTION_ARGS)
{
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    char* log = NULL;
    int errnum = 0;

    if (PG_GETARG_DATUM(0) == 0) {
        log = "";
    } else {
        log = text_to_cstring(PG_GETARG_TEXT_P(0));
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("%s", log)));
    } else {
        errnum = PG_GETARG_INT32(1);
        if ((errnum > -20000) || (errnum < -20999)) {
            ereport(
                ERROR, (errcode(ERRCODE_RAISE_EXCEPTION), errmsg("custom error code must be between -20000 and -20999")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_RAISE_EXCEPTION), errmsg("ORA%d: %s", errnum, log)));
        }
    }
    
    PG_RETURN_VOID();
}

Datum regexp_substr(PG_FUNCTION_ARGS)
{
    Oid collation = PG_GET_COLLATION();
    CHECK_RETNULL_INIT();
    CHECK_RETNULL_RETURN_DATUM(
        CHECK_RETNULL_CALL2(textregexsubstr_enforce_a, collation, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1)));
}
