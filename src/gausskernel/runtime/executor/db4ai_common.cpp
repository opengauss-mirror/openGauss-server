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
*---------------------------------------------------------------------------------------
*
* db4ai_common.cpp
*        Implementation of Public Methods of DB4AI
*
* IDENTIFICATION
*        src/gausskernel/runtime/executor/db4ai_common.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "db4ai/db4ai_common.h"

FORCE_INLINE
uint64_t time_diff(struct timespec *time_p1, struct timespec *time_p2)
{
    return ((time_p1->tv_sec * 1000000000) + time_p1->tv_nsec) - ((time_p2->tv_sec * 1000000000) + time_p2->tv_nsec);
}

FORCE_INLINE
double interval_to_sec(double time_interval)
{
    return time_interval / 1000000000.0;
}

FORCE_INLINE
double interval_to_msec(double time_interval)
{
    return time_interval / 1000000.0;
}


Datum float8_get_datum(Oid type, float8 value)
{
    Datum datum = 0;
    switch (type) {
        case BOOLOID:
            datum = BoolGetDatum(value != 0.0);
            break;
        case INT1OID:
            datum = Int8GetDatum(value);
            break;
        case INT2OID:
            datum = Int16GetDatum(value);
            break;
        case INT4OID:
            datum = Int32GetDatum(value);
            break;
        case INT8OID:
            datum = Int64GetDatum(value);
            break;
        case FLOAT4OID:
            datum = Float4GetDatum(value);
            break;
        case FLOAT8OID:
            datum = Float8GetDatum(value);
            break;
        case NUMERICOID:
            datum = DirectFunctionCall1(float8_numeric, Float8GetDatum(value));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Oid type %u not yet supported", type)));
            break;
    }
    return datum;
}

float8 datum_get_float8(Oid type, Datum datum)
{
    float8 value = 0;
    switch (type) {
        case BOOLOID:
            value = DatumGetBool(datum) ? 1.0 : 0.0;
            break;
        case INT1OID:
            value = DatumGetInt8(datum);
            break;
        case INT2OID:
            value = DatumGetInt16(datum);
            break;
        case INT4OID:
            value = DatumGetInt32(datum);
            break;
        case INT8OID:
            value = DatumGetInt64(datum);
            break;
        case FLOAT4OID:
            value = DatumGetFloat4(datum);
            break;
        case FLOAT8OID:
            value = DatumGetFloat8(datum);
            break;
        case NUMERICOID:
            value = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, datum));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Oid type %u not yet supported", type)));
            break;
    }
    return value;
}

Datum string_to_datum(const char *str, Oid datatype)
{
    switch (datatype) {
        case BOOLOID:
            return DirectFunctionCall1(boolin, CStringGetDatum(str));
        case INT1OID:
        case INT2OID:
        case INT4OID:
            return Int32GetDatum(atoi(str));
        case INT8OID:
            return Int64GetDatum(atoi(str));
        case VARCHAROID:
        case BPCHAROID:
        case CHAROID:
        case TEXTOID:
            return CStringGetTextDatum(str);
        case FLOAT4OID:
        case FLOAT8OID:
            return DirectFunctionCall1(float8in, CStringGetDatum(str));
        case CSTRINGOID:
            return CStringGetDatum(str);
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("The type is not supported: %u", datatype)));
            return CStringGetTextDatum(str);
    }
}

void check_hyper_bounds(unsigned int num_x, unsigned int num_y, const char *hyper)
{
    if (unlikely(UINT32_MAX / num_x < num_y))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Number multiplication is out of bounds. Hyperparemeter: %s.", hyper)));
}
