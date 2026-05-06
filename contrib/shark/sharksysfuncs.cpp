/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2026. All rights reserved.
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
 * sharksysfuncs.cpp
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/shark/sharksysfuncs.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <cctype>
#include <cfloat>
#include <climits>
#include <cmath>
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "utils/memutils.h"
#include "libpq/pqformat.h"
#include "parser/scansup.h"
#include "common/int.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "shark.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_attrdef.h"
#include "catalog/indexing.h"
#include "access/sysattr.h"
#include "utils/fmgroids.h"
#include "utils/cash.h"
#include "utils/lsyscache.h"
#include "sharksysfuncs.h"

extern "C" Datum object_name(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(object_name);

extern "C" Datum object_schema_name(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(object_schema_name);

extern "C" Datum object_define(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(object_define);

extern "C" Datum float_str(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(float_str);

extern "C" Datum sysdatetime(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(sysdatetime);

extern "C" Datum eomonth(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(eomonth);

extern "C" Datum is_numeric(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(is_numeric);

extern "C" Datum quotename(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(quotename);

extern "C" Datum sql_variant_property(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(sql_variant_property);

extern "C" Datum truncate_identifier(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(truncate_identifier);

extern "C" Datum translate_pg_type_to_tsql(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(translate_pg_type_to_tsql);

extern "C" Datum is_date(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(is_date);

extern "C" Datum columnproperty(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(columnproperty);

TypeInfoMap g_type_maps[TOTAL_TYPEMAP_COUNT] = {
    {"sql_variant", "sql_variant"},
    {"datetime", "datetime2"},
    {"smalldatetime", "smalldatetime"},
    {"date", "date"},
    {"time", "time"},
    {"float8", "float"},
    {"float4", "real"},
    {"numeric", "numeric"},
    {"money", "money"},
    {"int8", "bigint"},
    {"int4", "int"},
    {"int2", "smallint"},
    {"int1", "tinyint"},
    {"tinyint", "tinyint"},
    {"bool", "tinyint"},
    {"bit", "bit"},
    {"nvarchar", "nvarchar"},
    {"nvarchar2", "nvarchar"},
    {"nchar", "nchar"},
    {"varchar", "varchar"},
    {"bpchar", "char"},
    {"varbinary", "varbinary"},
    {"binary", "binary"},
    {"text", "text"},
    {"xml", "xml"},
    {"bpchar", "char"},
    {"decimal", "decimal"},
    {"timestamp", "timestamp"},
};

void FreeObjectInfo(object_info_t* info)
{
    if (info == NULL) {
        return;
    }

    pfree_ext(info->object_name);
    pfree_ext(info);
}

object_info_t* oid_to_pg_class_object(Oid object_id, Oid user_id)
{
    HeapTuple tuple;
    object_info_t* object_info = NULL;
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        /* check if user have right permission on object */
        if (pg_class_aclcheck(object_id, user_id, ACL_SELECT) == ACLCHECK_OK) {
            Form_pg_class pg_class = (Form_pg_class) GETSTRUCT(tuple);
            object_info = (object_info_t*)palloc0(sizeof(object_info_t));
            object_info->object_name = pstrdup(NameStr(pg_class->relname));
            object_info->object_schema = pg_class->relnamespace;
        }
        ReleaseSysCache(tuple);
    }
    return object_info;
}

object_info_t* oid_to_pg_proc_object(Oid object_id, Oid user_id)
{
    HeapTuple tuple;
    object_info_t* object_info = NULL;
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        if (pg_proc_aclcheck(object_id, user_id, ACL_EXECUTE) == ACLCHECK_OK) {
            Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);
            object_info = (object_info_t*)palloc0(sizeof(object_info_t));
            object_info->object_name = pstrdup(NameStr(procform->proname));
            object_info->object_schema = procform->pronamespace;
        }
        ReleaseSysCache(tuple);
    }
    return object_info;
}

object_info_t* oid_to_pg_type_object(Oid object_id, Oid user_id)
{
    HeapTuple tuple;
    object_info_t* object_info = NULL;
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        if (pg_type_aclcheck(object_id, user_id, ACL_USAGE) == ACLCHECK_OK) {
            Form_pg_type pg_type = (Form_pg_type) GETSTRUCT(tuple);
            object_info = (object_info_t*)palloc0(sizeof(object_info_t));
            object_info->object_name = pstrdup(NameStr(pg_type->typname));
            object_info->object_schema = pg_type->typnamespace;
        }
        ReleaseSysCache(tuple);
    }
    return object_info;
}

object_info_t* oid_to_pg_trigger_object(Oid object_id, Oid user_id)
{
    HeapTuple tuple;
    object_info_t* object_info = NULL;
    Relation tgrel;
    ScanKeyData key;
    SysScanDesc tgscan;

    tgrel = heap_open(TriggerRelationId, AccessShareLock);
    ScanKeyInit(&key, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object_id));
    tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, NULL, 1, &key);
    tuple = systable_getnext(tgscan);
    if (HeapTupleIsValid(tuple)) {
        Form_pg_trigger pg_trigger = (Form_pg_trigger) GETSTRUCT(tuple);
        if (OidIsValid(pg_trigger->tgrelid) &&
            pg_class_aclcheck(pg_trigger->tgrelid, user_id, ACL_SELECT) == ACLCHECK_OK) {
                object_info = (object_info_t*)palloc0(sizeof(object_info_t));
                object_info->object_name = pstrdup(NameStr(pg_trigger->tgname));
                object_info->object_schema = get_rel_namespace(pg_trigger->tgrelid);
        }
    }
    systable_endscan(tgscan);
    relation_close(tgrel, AccessShareLock);
    return object_info;
}

object_info_t* oid_to_pg_constraint_object(Oid object_id, Oid user_id)
{
    HeapTuple tuple;
    object_info_t* object_info = NULL;
    tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
        if (OidIsValid(con->conrelid) &&
            (pg_class_aclcheck(con->conrelid, user_id, ACL_SELECT) == ACLCHECK_OK)) {
            object_info = (object_info_t*)palloc0(sizeof(object_info_t));
            object_info->object_name = pstrdup(NameStr(con->conname));
            object_info->object_schema = con->connamespace;
        }
        ReleaseSysCache(tuple);
    }
    return object_info;
}

char* object_id_to_view_def(Oid object_id, Oid user_id)
{
    char* result = NULL;
    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        Form_pg_class pg_class = (Form_pg_class) GETSTRUCT(tuple);
        if (pg_class_aclcheck(object_id, user_id, ACL_SELECT) == ACLCHECK_OK
             && (pg_class->relkind == 'v' || pg_class->relkind == 'm')) {
                result = pg_get_viewdef_worker(object_id, 0, -1);
        }
        ReleaseSysCache(tuple);
    }
    return result;
}

char* object_id_to_proc_def(Oid object_id, Oid user_id)
{
    char* result = NULL;
    HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        if (pg_proc_aclcheck(object_id, user_id, ACL_EXECUTE) == ACLCHECK_OK) {
            Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);
            int headerlines = 0;
            result = pg_get_functiondef_worker(object_id, &headerlines);
        }
        ReleaseSysCache(tuple);
    }
    return result;
}

char* object_id_to_trigger_def(Oid object_id, Oid user_id)
{
    Relation tgrel;
    ScanKeyData key;
    SysScanDesc tgscan;
    HeapTuple tuple;
    char* result = NULL;
    tgrel = heap_open(TriggerRelationId, AccessShareLock);
    ScanKeyInit(&key, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object_id));
    tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, NULL, 1, &key);
    tuple = systable_getnext(tgscan);
    if (HeapTupleIsValid(tuple)) {
        Form_pg_trigger pg_trigger = (Form_pg_trigger) GETSTRUCT(tuple);
        if (OidIsValid(pg_trigger->tgrelid) &&
            pg_class_aclcheck(pg_trigger->tgrelid, user_id, ACL_SELECT) == ACLCHECK_OK) {
                result = pg_get_triggerdef_string(object_id);
        }
    }
    systable_endscan(tgscan);
    relation_close(tgrel, AccessShareLock);
    return result;
}

char* object_id_to_constraint_def(Oid object_id, Oid user_id)
{
    char* result = NULL;
    HeapTuple tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object_id));
    if (HeapTupleIsValid(tuple)) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
        if (OidIsValid(con->conrelid) && con->contype == 'c' &&
            (pg_class_aclcheck(con->conrelid, user_id, ACL_SELECT) == ACLCHECK_OK)) {
                result = pg_get_constraintdef_string(object_id);
            }
        ReleaseSysCache(tuple);
    }
    return result;
}

bool check_input_valid(PG_FUNCTION_ARGS)
{
    int32 input1 = PG_GETARG_INT32(0);
    if (input1 < 0) {
        return false;
    }

    if (!PG_ARGISNULL(1)) {
        int32 database_id = (Oid)PG_GETARG_INT32(1);
        if (database_id != u_sess->proc_cxt.MyDatabaseId) {
            return false;
        }
    }
    return true;
}

Datum object_name(PG_FUNCTION_ARGS)
{
    Oid object_id;
    Oid user_id = GetUserId();
    text *result = NULL;
    object_info_t* object_info = NULL;

    if (!check_input_valid(fcinfo)) {
        PG_RETURN_NULL();
    }

    object_id = (Oid)PG_GETARG_INT32(0);
    oid_to_object_ptr oid_to_objects[MAX_OBJECT_FUNCTION] = {
        (oid_to_object_ptr)oid_to_pg_class_object,
        (oid_to_object_ptr)oid_to_pg_proc_object,
        (oid_to_object_ptr)oid_to_pg_type_object,
        (oid_to_object_ptr)oid_to_pg_trigger_object,
        (oid_to_object_ptr)oid_to_pg_constraint_object
        };

    for (int i = 0; i < MAX_OBJECT_FUNCTION; i++) {
        oid_to_object_ptr oid_to_object_ptr_func = oid_to_objects[i];
        object_info = oid_to_object_ptr_func(object_id, user_id);
        if (object_info != NULL) {
            result = cstring_to_text(object_info->object_name);
            FreeObjectInfo(object_info);
            PG_RETURN_NVARCHAR2_P((VarChar *) result);
        }
    }

    PG_RETURN_NULL();
}

Datum object_schema_name(PG_FUNCTION_ARGS)
{
    Oid object_id;
    Oid user_id = GetUserId();
    text *result = NULL;
    object_info_t* object_info = NULL;

    if (!check_input_valid(fcinfo)) {
        PG_RETURN_NULL();
    }

    object_id = (Oid)PG_GETARG_INT32(0);
    oid_to_object_ptr oid_to_objects[MAX_OBJECT_FUNCTION] = {
        (oid_to_object_ptr)oid_to_pg_class_object,
        (oid_to_object_ptr)oid_to_pg_proc_object,
        (oid_to_object_ptr)oid_to_pg_type_object,
        (oid_to_object_ptr)oid_to_pg_trigger_object,
        (oid_to_object_ptr)oid_to_pg_constraint_object
        };

    for (int i = 0; i < MAX_OBJECT_FUNCTION; i++) {
        oid_to_object_ptr oid_to_object_ptr_func = oid_to_objects[i];
        object_info = oid_to_object_ptr_func(object_id, user_id);
        if (object_info != NULL && OidIsValid(object_info->object_schema)) {
            Oid namespace_oid = object_info->object_schema;
            char* namespace_name = get_namespace_name(namespace_oid);
            if (pg_namespace_aclcheck(namespace_oid, user_id, ACL_USAGE) != ACLCHECK_OK) {
                PG_RETURN_NULL();
            }
            FreeObjectInfo(object_info);
            result = cstring_to_text(namespace_name);
            PG_RETURN_NVARCHAR2_P((VarChar *) result);
        }
    }

    PG_RETURN_NULL();
}

Datum object_define(PG_FUNCTION_ARGS)
{
    int32 object_id = PG_GETARG_INT32(0);
    Oid user_id = GetUserId();
    text *result = NULL;
    char* def;

    def = object_id_to_view_def(object_id, user_id);
    if (def == NULL) {
        def = object_id_to_proc_def(object_id, user_id);
    }

    if (def == NULL) {
        def = object_id_to_trigger_def(object_id, user_id);
    }

    if (def == NULL) {
        def = object_id_to_constraint_def(object_id, user_id);
    }

    if (def != NULL) {
        result = cstring_to_text(def);
        pfree_ext(def);
        PG_RETURN_VARCHAR_P(result);
    }
    PG_RETURN_NULL();
}

static int round_float_char(char *float_char, int round_pos, int has_neg_sign)
{
    int            curr_digit;
    int            carry = 1;
    errno_t rc = EOK;

    while (round_pos > (0 + has_neg_sign) && carry) {
        if (float_char[round_pos] == '.') {
            round_pos--;
            continue;
        }
        curr_digit = float_char[round_pos] - '0' + carry;
        carry = curr_digit / DECIMAL_CARRY;
        rc = memset_s(float_char + round_pos, SINGLE_DIGIT_LEN, '0' + curr_digit % DECIMAL_CARRY, SINGLE_DIGIT_LEN);
        securec_check(rc, "\0", "\0");
        /* update the curr digit */
        round_pos--;
    }

    return carry;
}

static int find_round_pos(char *float_char, int has_neg_sign, int int_digits, int deci_digits,
    int input_deci_digits, int input_deci_point, int deci_sig)
{
    int            round_pos = 0;
    int            curr_digit;

    if (int_digits + input_deci_digits > MAX_DBL_DIG) {
        /*
         * exceeds the max precision, need to round to 17th digit(excluding -
         * and .)
         */
        if (int_digits > MAX_DBL_DIG) {
            /* round in int part */
            /* STR(12345678901234567890, 20) returns "1234567890123456800" */
            curr_digit = float_char[MAX_DBL_DIG + has_neg_sign] - '0';

            if (curr_digit >= MAX_FLOAT_DIG) {
                round_pos = MAX_DBL_DIG_INDEX + has_neg_sign;
            }
        } else {
            /* round in decimal part */

            /*
             * STR(1234567890.1234567890, 22, 20) returns
             * "1234567890.12345670000"
             */
            curr_digit = float_char[MAX_DBL_DIG + has_neg_sign + input_deci_point] - '0';
            if (curr_digit >= MAX_FLOAT_DIG) {
                round_pos = MAX_DBL_DIG_INDEX + has_neg_sign + input_deci_point;
            }
        }
    } else if (deci_digits && input_deci_digits > deci_sig) {
        /* input decimal digits > needed, round to last output decimal digit  */
        /* STR(-1.123456, 8, 5) retuns "-1.12346" */
        curr_digit = float_char[has_neg_sign + int_digits + input_deci_point + deci_sig] - '0';
        if (curr_digit >= MAX_FLOAT_DIG) {
            round_pos = has_neg_sign + int_digits + input_deci_point + deci_sig - 1;
        }
    } else if (!deci_sig && input_deci_digits) {
        /* int part == length and has deci digit input, round to integer */
        /* STR(-1234.9, 5, 1) returns "-1235" */
        curr_digit = float_char[has_neg_sign + int_digits + 1] - '0';
        if (curr_digit >= MAX_FLOAT_DIG) {
            round_pos = has_neg_sign + int_digits - 1;
        }
        /* last digit of integer */
    }

    return round_pos;
}

Datum return_n_star(int length)
{
    VarChar* result = NULL;
    errno_t rc = EOK;
    char* buf = NULL;
    buf = (char*)palloc(length + 1);
    rc = memset_s(buf, length, '*', length);
    securec_check(rc, "\0", "\0");
    buf[length] = '\0';
    result = cstring_to_text(buf);
    pfree_ext(buf);
    PG_RETURN_VARCHAR_P(result);
}

Datum float_str(PG_FUNCTION_ARGS)
{
    Numeric        float_numeric;
    int            precision;
    char       *float_char;
    int32        length;
    int32        decimal;
    char       *buf;
    int            size;
    char       *ptr;
    int            input_deci_digits;
    int            has_neg_sign = 0;
    int            input_deci_point = 0;
    int            has_deci_point = 0;
    int            num_spaces = 0;
    int            int_digits = 0;
    int            deci_digits = 0;
    int            int_part_zeros = 0;
    int            deci_part_zeros = 0;
    int            deci_sig;
    int            round_pos = -1;
    VarChar* result = NULL;
    errno_t rc = EOK;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    float_numeric = PG_GETARG_NUMERIC(0);
    if (numeric_is_nan(float_numeric) || NUMERIC_IS_INF(float_numeric)) {
        PG_RETURN_NULL();
    }

    float_char = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(float_numeric)));
    /* precision = num of digits - negative sign - decimal point */
    precision = strlen(float_char);

    /* count number of numeric digits in the numeric input */
    /* find - and . in input */
    if (strchr(float_char, '-') != NULL) {
        precision--;
        has_neg_sign = 1;
    }

    if (strchr(float_char, '.') != NULL) {
        precision--;
        input_deci_point = 1;
        /* int_digits is num digits before decimal point (excluding -) */
        /* STR(-1234.56), int_digits = 4 */
        int_digits = strchr(float_char, '.') - float_char - has_neg_sign;
    } else {
        /* no decimal point, all digits are int digits */
        /* STR(123400), int_digits = precision = 6 */
        int_digits = precision;
    }
    input_deci_digits = precision - int_digits;

    length = PG_GETARG_INT32(1);
    decimal = PG_GETARG_INT32(SECOND_ARG_INDEX);
    if (length <= 0 || length > MAX_STRING_LEN || decimal < 0) {
        PG_RETURN_NULL();
    }

    if (int_digits + has_neg_sign > length) {
        /* return string of length filled with *  */
        /* STR(-1234, 4), return "****" */
        return return_n_star(length);
    } else if (int_digits > MAX_DBL_DIG) {
        /*
         * max precision is 17 so remaining int digits will be padded with
         * zeros
         */
        /*
         * STR(12345678901234567890, 20), int_part_zeros = 3, will return
         * "12345678901234568000"
         */
        int_part_zeros = int_digits - MAX_DBL_DIG;
    }

    /* calculate num_spaces and deci_part_zeros */
    size = length + 1;
    /* allocate buffer for putting result together */
    buf = (char*)palloc(size);
    rc = memset_s(buf, size, '\0', size);
    securec_check(rc, "\0", "\0");

    if (has_neg_sign)
        length--;
    if (decimal > 0 && length > int_digits) {
        /* result will have decimal part and it will take up 1 space */
        has_deci_point = 1;
        length--;
    }

    /* update atual max decimal digits in result */
    /*
     * STR(1234.5678, 8, 4), int_digits=4, length=8-1=7, decimal=7-4=3, will
     * return "1234.567"
     */
    if (length < (decimal + int_digits))
        decimal = length - int_digits;

    if (decimal > 0) {
        /* max scale is 16, so actual deci_digits is min(decimal, 16), */
        /* rest of the decimal digits will be disgarded */

        /*
         * STR(0.123456789012345678, 20, 18), decimal=18, deci_digits=16. will
         * return "  0.1234567890123457"
         */
        deci_digits = Min(decimal, MAX_DBL_DIG_INDEX);
    } else {
        /* no decimal digits */
        deci_digits = 0;
    }

    num_spaces = length - int_digits - deci_digits;
    /* comp space for the decimal point */
    if (has_deci_point && !deci_digits) {
        /*
         * no enough space for decimal part, remove the decimal point and add
         * one space. STR(1.234, 2, 1) returns " 1"
         */
        num_spaces++;
        has_deci_point--;
    }

    /*
     * max precision is 17, max significant digits in decimal part =
     * min(remaining significant digits, input decimal digits)
     */
    deci_sig = Min(Max(MAX_DBL_DIG - int_digits, 0), input_deci_digits);
    if (deci_digits > 0 && length > MAX_DBL_DIG && deci_digits > deci_sig) {
        /* total significant digits > 17 and last sig digit in decimal part */
        /* STR(1234567890.1234567890, 22, 20) returns "1234567890.12345670000" */
        deci_part_zeros = deci_digits - deci_sig;
    } else if (deci_digits > input_deci_digits) {
        /* decimal digits needed more than input */
        /*
         * STR(1.1234, 8, 6) returns "1.123400", deci_sig = 4, deci_part_zeros
         * = 2
         */
        deci_part_zeros = deci_digits - input_deci_digits;
        deci_sig = input_deci_digits;
    } else {
        /* no zeros in the decimal part */
        /* STR(1.1234, 5, 3) returns "1.123" */
        deci_sig = deci_digits;
    }

    /* set the spaces at the start of the string */
    if (num_spaces > 0) {
        rc = memset_s(buf, num_spaces, ' ', num_spaces);
        securec_check(rc, "\0", "\0");
    }

    /* find if need to round, if round_pos = 0, do not need rounding */
    round_pos = find_round_pos(float_char, has_neg_sign, int_digits, deci_digits, input_deci_digits,
                               input_deci_point, deci_sig);
    if (round_pos > 0) {
        if (round_float_char(float_char, round_pos, has_neg_sign)) {
            if (num_spaces > 0) {
                if (has_neg_sign) {
                    rc = memset_s(buf + num_spaces - 1, SINGLE_DIGIT_LEN, '-', SINGLE_DIGIT_LEN);
                    securec_check(rc, "\0", "\0");
                    num_spaces++;
                }
                rc = memset_s(buf + num_spaces - 1, SINGLE_DIGIT_LEN, '1', SINGLE_DIGIT_LEN);
                securec_check(rc, "\0", "\0");
                rc = memset_s(float_char, SINGLE_DIGIT_LEN, '0', SINGLE_DIGIT_LEN);
                securec_check(rc, "\0", "\0");
            } else {
                pfree_ext(buf);
                return return_n_star(size - 1);
            }
        }
    }

    rc = strncpy_s(buf + num_spaces, size - num_spaces, float_char, size - 1 - num_spaces);
    securec_check(rc, "\0", "\0");
    if (has_deci_point && !input_deci_digits) {
        rc = memset_s(buf + num_spaces + has_neg_sign + int_digits, SINGLE_DIGIT_LEN, '.', SINGLE_DIGIT_LEN);
        securec_check(rc, "\0", "\0");
    }

    /* set the zeros */
    if (deci_part_zeros > 0) {
        rc = memset_s(buf + size - deci_part_zeros - 1, deci_part_zeros, '0', deci_part_zeros);
        securec_check(rc, "\0", "\0");
    }

    if (int_part_zeros > 0) {
        if (deci_digits > 0) {
            ptr = strchr(buf, '.');
            rc = memset_s(ptr - int_part_zeros, int_part_zeros, '0', int_part_zeros);
            securec_check(rc, "\0", "\0");
        } else {
            rc = memset_s(buf + size - int_part_zeros - 1, int_part_zeros, '0', int_part_zeros);
            securec_check(rc, "\0", "\0");
        }
    }
    result = cstring_to_text_with_len(buf, size);
    pfree_ext(buf);
    PG_RETURN_VARCHAR_P(result);
}

Datum sysdatetime(PG_FUNCTION_ARGS)
{
    return TimestampTzGetDatum(GetCurrentStatementStartTimestamp());
}

Datum eomonth(PG_FUNCTION_ARGS)
{
    int year = 0;
    int month = 0;
    int day = 0;
    int offset = 0;
    DateADT date;
    bool isOffsetGiven = false;
    bool isOriginalDateOutsideTSQLEndLimit = false;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    } else {
        date = PG_GETARG_DATEADT(0);
    }
    if (!PG_ARGISNULL(1)) {
        offset = PG_GETARG_INT32(1);
        isOffsetGiven = true;
    }

    j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);
    isOriginalDateOutsideTSQLEndLimit = year < 1 || year > MAX_SHARK_YEAR;
    month += offset;

    if (month > 0) {
        year += (month - 1) / MONTHS_COUNT_IN_A_YEAR;
        month = (month - 1) % MONTHS_COUNT_IN_A_YEAR + 1;
    } else {
        year += month / MONTHS_COUNT_IN_A_YEAR - 1;
        month = month % MONTHS_COUNT_IN_A_YEAR + MONTHS_COUNT_IN_A_YEAR;
    }

    month++;
    if (year < 1 || year > MAX_SHARK_YEAR) {
        if (isOffsetGiven && !isOriginalDateOutsideTSQLEndLimit) {
            ereport(ERROR,
                (errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
                 errmsg("Adding a value to a 'date' column caused an overflow.")));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
                 errmsg("The date exceeds openGauss compatibility limits.")));
        }
    }
    date = date2j(year, month, 1) - POSTGRES_EPOCH_JDATE - 1;
    PG_RETURN_DATEADT(date);
}

Datum cast_to_numeric_or_cash_quietly(char        * value_str)
{
    Datum converted;
    bool is_error = false;
    PG_TRY();
    {
        converted = DirectFunctionCall3(numeric_in, CStringGetDatum(value_str),
                                        ObjectIdGetDatum(0), Int32GetDatum(-1));
        is_error = false;
    }
    PG_CATCH();
    {
        is_error = true;
        FlushErrorState();
    }
    PG_END_TRY();
    
    if (!is_error) {
        return 1;
    }
    
    PG_TRY();
    {
        converted = DirectFunctionCall3(cash_in, CStringGetDatum(value_str),
                                        ObjectIdGetDatum(0), Int32GetDatum(-1));
        is_error = false;
    }
    PG_CATCH();
    {
        is_error = true;
        FlushErrorState();
    }
    PG_END_TRY();

    return !is_error;
}

Datum is_numeric(PG_FUNCTION_ARGS)
{
    Oid        argtypeid;
    Oid        numeric_typiofunc;
    Oid        money_typiofunc;
    char        *value_str;
    bool        result = false;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_INT32(0);
    }
    argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);
    if (!OidIsValid(argtypeid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("could not determine input data type")));

    /* Fast path for known numeric types. */
    if (argtypeid != TEXTOID) {
        if ((argtypeid == INT1OID) || (argtypeid == INT2OID) ||
            (argtypeid == INT4OID) || (argtypeid == INT8OID) ||
            (argtypeid == INT16OID) || (argtypeid == FLOAT4OID) ||
            (argtypeid == FLOAT8OID) || (argtypeid == NUMERICOID) ||
            (argtypeid == CASHOID) || (argtypeid == BOOLOID)) {
            PG_RETURN_INT32(1);
        }
    }

    /* Get the string representation from input datum. */
    if (argtypeid == TEXTOID) {
        value_str = text_to_cstring(PG_GETARG_TEXT_P(0));
    } else {
        Oid typoutput;
        bool typisvarlena;
        getTypeOutputInfo(argtypeid, &typoutput, &typisvarlena);
        value_str = OidOutputFunctionCall(typoutput, PG_GETARG_DATUM(0));
    }

    result = cast_to_numeric_or_cash_quietly(value_str);
    pfree(value_str);
    PG_RETURN_INT32(result);
}

sv_property_t get_property_type(const char *arg, int len)
{
    /* Incase sensitive match, No prefix/suffix spaces handling */
    if (pg_strncasecmp(arg, "basetype", len) == 0) {
        return SV_PROPERTY_BASETYPE;
    } else if (pg_strncasecmp(arg, "precision", len) == 0) {
        return SV_PROPERTY_PRECISION;
    } else if (pg_strncasecmp(arg, "scale", len) == 0) {
        return SV_PROPERTY_SCALE;
    } else if (pg_strncasecmp(arg, "totalbytes", len) == 0) {
        return SV_PROPERTY_TOTALBYTES;
    } else if (pg_strncasecmp(arg, "collation", len) == 0) {
        return SV_PROPERTY_COLLATION;
    } else if (pg_strncasecmp(arg, "maxlength", len) == 0) {
        return SV_PROPERTY_MAXLENGTH;
    } else {
        return SV_PROPERTY_INVALID;
    }
}

bool get_left_right_delim(const char delimiter, char*        left_delim, char* right_delim)
{
    switch (delimiter) {
        case ']':
        case '[':
            *left_delim = '[';
            *right_delim = ']';
            break;
        case '`':
        case '\'':
        case '"':
            *left_delim = delimiter;
            *right_delim = delimiter;
            break;
        case '(':
        case ')':
            *left_delim = '(';
            *right_delim = ')';
            break;
        case '<':
        case '>':
            *left_delim = '<';
            *right_delim = '>';
            break;
        case '{':
        case '}':
            *left_delim = '{';
            *right_delim = '}';
            break;
        default:
            return false;
    }
    return true;
}

Datum quotename(PG_FUNCTION_ARGS)
{
    const char *input_string = text_to_cstring(PG_GETARG_TEXT_P(0));
    const char *delimiter = text_to_cstring(PG_GETARG_TEXT_P(1));

    char        left_delim;
    char        right_delim;
    char       *buf;
    int         buf_i = 0;
    VarChar* result = NULL;

    /* Validate input len */
    if (strlen(input_string) > MAX_INDETITY_LEN || strlen(delimiter) != 1) {
        PG_RETURN_NULL();
    }

    if (!get_left_right_delim(*delimiter, &left_delim, &right_delim)) {
        PG_RETURN_NULL();
    }

    buf = (char*)palloc(MAX_INDETITY_OUT_LEN * sizeof(char));
    errno_t rc = memset_s(buf, MAX_INDETITY_OUT_LEN, '\0', MAX_INDETITY_OUT_LEN);
    securec_check(rc, "\0", "\0");

    /* Process input string to include escape characters */
    buf[buf_i++] = left_delim;
    int str_len = strlen(input_string);
    for (int i = 0; i < str_len; i++) {
        switch (input_string[i]) {
                /* Escape chars */
            case '\'':
            case ']':
            case '"':
                buf[buf_i++] = input_string[i];
                buf[buf_i++] = input_string[i];
                break;
            default:
                buf[buf_i++] = input_string[i];
        }
    }
    buf[buf_i++] = right_delim;
    buf[buf_i] = '\0';
    result = cstring_to_text(buf);
    pfree_ext(buf);
    PG_RETURN_VARCHAR_P(result);
}

Datum sql_variant_property(PG_FUNCTION_ARGS)
{
    bytea* sv_value = PG_GETARG_BYTEA_PP(0);
    int prop_len = VARSIZE_ANY_EXHDR(PG_GETARG_BYTEA_PP(1));
    const char *prop_str = VARDATA_ANY(PG_GETARG_BYTEA_PP(1));
    sv_property_t prop_type;

    /* CHECK Validity of Property */
    prop_type = get_property_type(prop_str, prop_len);
    if (prop_type == SV_PROPERTY_INVALID) {
        PG_RETURN_NULL();
    }

    /* Dispatch to property functions */
    switch (prop_type) {
        case SV_PROPERTY_BASETYPE:
            return get_base_type(fcinfo, sv_value);
        case SV_PROPERTY_PRECISION:
            return get_precision(fcinfo, sv_value);
        case SV_PROPERTY_SCALE:
            return get_scale(fcinfo, sv_value);
        case SV_PROPERTY_TOTALBYTES:
            return get_total_bytes(fcinfo, sv_value);
        case SV_PROPERTY_COLLATION:
            return get_collation(fcinfo, sv_value);
        case SV_PROPERTY_MAXLENGTH:
            return get_max_length(fcinfo, sv_value);
        default:
            break;
    }
    PG_RETURN_NULL();       /* SHOULD NOT HAPPEN */
}

Datum truncate_identifier(PG_FUNCTION_ARGS)
{
    char *ident = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int len = strlen(ident);
    if (len >= NAMEDATALEN) {
        len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);
        ident[len] = '\0';
    }
    PG_RETURN_TEXT_P(cstring_to_text(ident));
}

Datum translate_pg_type_to_tsql(PG_FUNCTION_ARGS)
{
    Oid pg_type = PG_GETARG_OID(0);
    char* type_name = NULL;
    if (OidIsValid(pg_type)) {
        type_name = get_typename(pg_type);
        for (int i = 0; i < TOTAL_TYPEMAP_COUNT; i++) {
            if (strcmp(g_type_maps[i].pg_typname, type_name) == 0) {
                pfree_ext(type_name);
                PG_RETURN_TEXT_P(cstring_to_text(g_type_maps[i].tsql_typname));
            }
        }
    }
    text* result = cstring_to_text(type_name);
    pfree_ext(type_name);
    PG_RETURN_TEXT_P(result);
}

bool is_precision_more_than_3(char* timestampstr)
{
    int len = strlen(timestampstr);
    int i = len - 1;
    while (i >= 0 && timestampstr[i] == ' ') {
        i--;
    }
    while (i >= 0 && timestampstr[i] != '.') {
        i--;
    }
    if (len - i > SHARK_MAX_DATETIME_PRECISION) {
        return true;
    }
    return false;
}

/*
 * the fsec precision of datetime2 is greater than 3
 */
bool check_is_datetime2(char* timestampstr, Datum timestamp_result)
{
    struct pg_tm tt;
    struct pg_tm* tm = &tt;
    Timestamp timestampVal = DatumGetTimestamp(timestamp_result);
    fsec_t fsec;
    if (timestamp2tm(timestampVal, NULL, tm, &fsec, NULL, NULL) != 0) {
        return true;
    }
    if (fsec != 0) {
        return is_precision_more_than_3(timestampstr);
    }
    return false;
}

bool check_is_time2(char* timestampstr, Datum timestamp_result)
{
    struct pg_tm tt;
    struct pg_tm* tm = &tt;
    TimeADT time = DatumGetTimeADT(timestamp_result);
    fsec_t fsec;
    if (time2tm(time, tm, &fsec) != 0) {
        return true;
    }
    if (fsec != 0) {
        return is_precision_more_than_3(timestampstr);
    }
    return false;
}

bool is_time(char* tmp)
{
    Datum time_result;
    bool is_error = false;
    PG_TRY();
    {
        time_result = DirectFunctionCall3(time_in, CStringGetDatum(tmp),
                                          ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    }
    PG_CATCH();
    {
        FlushErrorState();
        is_error = true;
    }
    PG_END_TRY();
    
    if (!is_error) {
        is_error = check_is_time2(tmp, time_result);
    }
    return is_error;
}


Datum is_date(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_INT32(0);
    }
    bool is_error = false;
    Datum timestamp_result;
    char* tmp = NULL;
    struct pg_tm tt;
    struct pg_tm* tm = &tt;
    tmp = DatumGetCString(DirectFunctionCall1(textout, PG_GETARG_DATUM(0)));

    PG_TRY();
    {
        timestamp_result = DirectFunctionCall1(text_timestamp, PG_GETARG_DATUM(0));
    }
    PG_CATCH();
    {
        FlushErrorState();
        is_error = true;
    }
    PG_END_TRY();

    if (!is_error) {
        is_error = check_is_datetime2(tmp, timestamp_result);
    }

    if (!is_error) {
        pfree_ext(tmp);
        PG_RETURN_INT32(1);
    }

    is_error = is_time(tmp);
    pfree_ext(tmp);
    if (is_error) {
        PG_RETURN_INT32(0);
    }
    
    PG_RETURN_INT32(1);
}

bool is_column_exist(Oid rel_oid, char* colname)
{
    HeapTuple attTuple;
    bool attisdropped;
    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(rel_oid), PointerGetDatum(colname));
    if (!HeapTupleIsValid(attTuple)) {
        return 0;
    }

    attisdropped = ((Form_pg_attribute)GETSTRUCT(attTuple))->attisdropped;
    ReleaseSysCache(attTuple);
    return !attisdropped;
}

char* formated_string(text *input)
{
    char* input_str = text_to_cstring(input);
    if (strlen(input_str) < 1) {
        return NULL;
    }

    input_str = downcase_truncate_identifier(input_str, strlen(input_str), true);
    input_str = remove_trailing_spaces(input_str);
    return input_str;
}

int get_charmaxlen(Oid rel_oid, char* colname)
{
    HeapTuple attTuple;
    int atttypmod;
    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(rel_oid), PointerGetDatum(colname));
    if (!HeapTupleIsValid(attTuple)) {
        return -1;
    }

    atttypmod = ((Form_pg_attribute)GETSTRUCT(attTuple))->atttypmod;
    ReleaseSysCache(attTuple);
    if (atttypmod > 0) {
        return atttypmod - NUMERIC_PRECISION_BASE_LEN;
    }
    return -1;
}

int get_allow_null(Oid rel_oid, char* colname)
{
    HeapTuple attTuple;
    bool attnotnull;
    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(rel_oid), PointerGetDatum(colname));
    if (!HeapTupleIsValid(attTuple)) {
        return -1;
    }

    attnotnull = ((Form_pg_attribute)GETSTRUCT(attTuple))->attnotnull;
    ReleaseSysCache(attTuple);
    return !attnotnull;
}

int get_column_id(Oid rel_oid, char* colname)
{
    HeapTuple attTuple;
    int column_id;
    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(rel_oid), PointerGetDatum(colname));
    if (!HeapTupleIsValid(attTuple)) {
        return -1;
    }

    column_id = ((Form_pg_attribute)GETSTRUCT(attTuple))->attnum;
    ReleaseSysCache(attTuple);
    return column_id;
}

int get_is_compute(Oid rel_oid, char* colname)
{
    HeapTuple attTuple;
    int attnum;
    Relation attrdef_rel;
    ScanKeyData scankeys[TWO];
    SysScanDesc scan;
    HeapTuple tuple;
    bool is_compute = false;
    attnum = get_column_id(rel_oid, colname);

    attrdef_rel = heap_open(AttrDefaultRelationId, AccessShareLock);
    ScanKeyInit(&scankeys[0], Anum_pg_attrdef_adrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(rel_oid));
    ScanKeyInit(&scankeys[1], Anum_pg_attrdef_adnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));
    scan = systable_beginscan(attrdef_rel, AttrDefaultIndexId, true, NULL, TWO, scankeys);

    /* There should be at most one matching tuple, but we loop anyway */
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        bool isnull = false;
        Datum val = fastgetattr(tuple, Anum_pg_attrdef_adgencol, attrdef_rel->rd_att, &isnull);
        if (isnull) {
            continue;
        }
        bool is_gencol = DatumGetBool(val);
        if (is_gencol) {
            is_compute = true;
            break;
        }
    }
    systable_endscan(scan);
    heap_close(attrdef_rel, AccessShareLock);

    return is_compute;
}

int get_is_identity(Oid relid, char* colname)
{
    Oid seqOid = InvalidOid;

    seqOid = getIdentitySequenceForD(relid, colname, NULL);
    return OidIsValid(seqOid);
}

int get_is_hidden(Oid rel_oid, char* colname)
{
    int attnum;
    attnum = get_column_id(rel_oid, colname);
    if (attnum >= 0) {
        return 1;
    } else {
        return 0;
    }
}

int get_ordinal(Oid rel_oid, char* colname)
{
    HeapTuple attr_tuple;
    Form_pg_attribute attr_form;
    int col_position = 0;
    int current_pos = 0;
    ScanKeyData scan_key[1];
    SysScanDesc scan_desc;
    Relation rel_att;
    ScanKeyInit(&scan_key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(rel_oid));
    rel_att = heap_open(AttributeRelationId, AccessShareLock);
    scan_desc = systable_beginscan(rel_att, AttributeRelidNumIndexId, true, NULL, 1, scan_key);
    while (HeapTupleIsValid(attr_tuple = systable_getnext(scan_desc))) {
        attr_form = (Form_pg_attribute)GETSTRUCT(attr_tuple);
        if (attr_form->attisdropped || attr_form->attnum <= 0) {
            continue;
        }
    
        current_pos++;
        if (strcmp(NameStr(attr_form->attname), colname) == 0) {
            col_position = current_pos;
            break;
        }
    }
    systable_endscan(scan_desc);
    heap_close(rel_att, AccessShareLock);

    return col_position;
}

int get_column_type_andtymod(Oid rel_oid, char* colname, Oid* type, int* tymod)
{
    HeapTuple attTuple;
    int column_id;
    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(rel_oid), PointerGetDatum(colname));
    if (!HeapTupleIsValid(attTuple)) {
        return -1;
    }

    *type = ((Form_pg_attribute)GETSTRUCT(attTuple))->atttypid;
    *tymod = ((Form_pg_attribute)GETSTRUCT(attTuple))->atttypmod;
    ReleaseSysCache(attTuple);
    return 0;
}

int get_type_no_mod_scale(Oid typeoid)
{
    int scale = -1;
    switch (typeoid) {
        case SMALLDATETIMEOID:
            scale = 0;
            break;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            scale = TIMESTAMP_DEFAULT_SCALE;
            break;

        case DATEOID:
            scale = 0;
            break;

        case TIMEOID:
        case TIMETZOID:
            scale = TIMESTAMP_DEFAULT_SCALE;
            break;

        case NUMERICOID:
            scale = NUMERIC_DEFAULT_SCALE;
            break;

        case CASHOID:
            scale = CASH_DEFAULT_SCALE;
            break;

        default:
            scale = 0;
            break;
    }

    return scale;
}

int get_type_scale(Oid typeoid, int typmod)
{
    if (typmod == -1) {
        return get_type_no_mod_scale(typeoid);
    }
    int scale = -1;
    switch (typeoid) {
        case NUMERICOID:
            scale = ((typmod - NUMERIC_PRECISION_BASE_LEN)) & NUMERIC_PRECISION_MASK;
            break;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case TIMEOID:
        case TIMETZOID:
            scale = typmod;
            break;

        case CHAROID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
            scale = typmod - NUMERIC_PRECISION_BASE_LEN;
            break;

        default:
            scale = typmod;
            break;
    }
    return scale;
}

int get_scale(Oid rel_oid, char* colname)
{
    Oid typid = 0;
    int32 typmod = 0;
    if (get_column_type_andtymod(rel_oid, colname, &typid, &typmod) == -1) {
        return -1;
    }
    return get_type_scale(typid, typmod);
}

int get_type_no_mod_precision(Oid typeoid)
{
    int precision = -1;
    switch (typeoid) {
        case SMALLDATETIMEOID:
            precision = SMALLDATETIME_DEFAULT_PRECISION;
            break;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            precision = DATETIME_DEFAULT_PRECISION;
            break;

        case DATEOID:
            precision = DATE_DEFAULT_PRECISION;
            break;

        case TIMEOID:
        case TIMETZOID:
            precision = TIME_DEFAULT_PRECISION;
            break;

        case FLOAT8OID:
            precision = FLOAT8_DEFAULT_PRECISION;
            break;

        case FLOAT4OID:
            precision = FLOAT4_DEFAULT_PRECISION;
            break;

        case NUMERICOID:
            precision = NUMERIC_DEFAULT_PRECISION;
            break;

        case CASHOID:
            precision = CASH_DEFAULT_PRECISION;
            break;

        case INT8OID:
            precision = INT8_DEFAULT_PRECISION;
            break;

        case INT4OID:
            precision = INT4_DEFAULT_PRECISION;
            break;

        case INT2OID:
            precision = INT2_DEFAULT_PRECISION;
            break;

        case INT1OID:
            precision = INT1_DEFAULT_PRECISION;
            break;

        case BITOID:
            precision = BIT_DEFAULT_PRECISION;
            break;

        default:
            precision = 0;
            break;
    }

    return precision;
}

int get_type_precision(Oid typeoid, int typmod)
{
    if (typmod == -1) {
        return get_type_no_mod_precision(typeoid);
    }
    int precision = -1;
    switch (typeoid) {
        case NUMERICOID:
            precision = ((typmod - NUMERIC_PRECISION_BASE_LEN) >> SHORT_LENGTH) & NUMERIC_PRECISION_MASK;
            break;

        case SMALLDATETIMEOID:
            precision = SMALLDATETIME_DEFAULT_PRECISION;
            break;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            {
                if (typmod == 0) {
                    precision = DATETIME_MIN_PRECISION;
                } else {
                    precision = DATETIME_MIN_PRECISION + 1 + typmod;
                }
            }
            break;

        case TIMEOID:
        case TIMETZOID:
            {
                if (typmod == 0) {
                    precision = TIME_MIN_PRECISION;
                } else {
                    precision = TIME_MIN_PRECISION + 1 + typmod;
                }
            }
            break;

        case CHAROID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
            precision = typmod - NUMERIC_PRECISION_BASE_LEN;
            break;

        default:
            precision = typmod;
            break;
    }
    return precision;
}

int get_precision(Oid rel_oid, char* colname)
{
    Oid typid = 0;
    int32 typmod = 0;
    if (get_column_type_andtymod(rel_oid, colname, &typid, &typmod) == -1) {
        return -1;
    }
    return get_type_precision(typid, typmod);
}

Datum columnproperty(PG_FUNCTION_ARGS)
{
    int32 object_id = PG_GETARG_INT32(0);
    char* column_name = formated_string(PG_GETARG_TEXT_PP(1));
    char* property = formated_string(PG_GETARG_TEXT_PP(2));
    int result = -1;

    if (column_name == NULL || property == NULL || !is_column_exist(object_id, column_name)) {
        pfree_ext(column_name);
        pfree_ext(property);
        PG_RETURN_NULL();
    }

    if (strcmp("charmaxlen", property) == 0) {
        result = get_charmaxlen(object_id, column_name);
    } else if (strcmp("allowsnull", property) == 0) {
        result = get_allow_null(object_id, column_name);
    } else if (strcmp("iscomputed", property) == 0) {
        result = get_is_compute(object_id, column_name);
    } else if (strcmp("columnid", property) == 0) {
        result = get_column_id(object_id, column_name);
    } else if (strcmp("ishidden", property) == 0) {
        result = get_is_hidden(object_id, column_name);
    } else if (strcmp("isidentity", property) == 0) {
        result = get_is_identity(object_id, column_name);
    } else if (strcmp("ordinal", property) == 0) {
        result = get_ordinal(object_id, column_name);
    } else if (strcmp("precision", property) == 0) {
        result = get_precision(object_id, column_name);
    } else if (strcmp("scale", property) == 0) {
        result = get_scale(object_id, column_name);
    }

    pfree_ext(column_name);
    pfree_ext(property);
    if (result == -1) {
        PG_RETURN_NULL();
    }

    PG_RETURN_INT32(result);
}

