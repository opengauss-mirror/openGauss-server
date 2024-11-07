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
 * --------------------------------------------------------------------------------------
 *
 * gms_i18n.cpp
 *  gms_i18n package
 *
 *
 * IDENTIFICATION
 *        contrib/gms_i18n/gms_i18n.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/builtins.h"

#include "gms_i18n.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gms_i18n_raw_to_char);
PG_FUNCTION_INFO_V1(gms_i18n_string_to_raw);

Datum gms_i18n_raw_to_char(PG_FUNCTION_ARGS)
{
    Datum raw = PG_GETARG_DATUM(0);
    Datum src_encoding_name;
    Datum dest_encoding_name;
    Datum result;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    dest_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.DatabaseEncoding->name));

    if (PG_ARGISNULL(1)) {
        src_encoding_name = dest_encoding_name;
    } else {
        src_encoding_name = DirectFunctionCall1(text_name, PG_GETARG_DATUM(1));
    }

    /*
     * pg_convert expects a bytea as its first argument. We're passing it a
     * raw argument here, relying on the fact that they are both in fact
     * varlena types, and thus structurally identical.
     */
    result = DirectFunctionCall3(pg_convert, raw, src_encoding_name, dest_encoding_name);

    PG_RETURN_DATUM(result);
}

Datum gms_i18n_string_to_raw(PG_FUNCTION_ARGS)
{
    Datum string = PG_GETARG_DATUM(0);
    Datum dest_encoding_name;
    Datum src_encoding_name;
    Datum result;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    src_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.DatabaseEncoding->name));

    if (PG_ARGISNULL(1)) {
        dest_encoding_name = src_encoding_name;
    } else {
        dest_encoding_name = DirectFunctionCall1(text_name, PG_GETARG_DATUM(1));
    }

    /*
     * pg_convert expects a bytea as its first argument. We're passing it a
     * varchar2 argument here, relying on the fact that they are both in fact
     * varlena types, and thus structurally identical.
     */
    result = DirectFunctionCall3(pg_convert, string, src_encoding_name, dest_encoding_name);

    PG_RETURN_DATUM(result);
}
