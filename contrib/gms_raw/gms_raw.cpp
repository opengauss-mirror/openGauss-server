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
 * gms_raw.cpp
 *  gms_raw can process raw type data.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_raw/gms_raw.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "gms_raw.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(bit_and);
PG_FUNCTION_INFO_V1(bit_or);
PG_FUNCTION_INFO_V1(bit_complement);
PG_FUNCTION_INFO_V1(bit_xor);
PG_FUNCTION_INFO_V1(cast_from_binary_double);
PG_FUNCTION_INFO_V1(cast_from_binary_float);
PG_FUNCTION_INFO_V1(cast_from_binary_integer);
PG_FUNCTION_INFO_V1(cast_from_number);
PG_FUNCTION_INFO_V1(cast_to_binary_double);
PG_FUNCTION_INFO_V1(cast_to_binary_float);
PG_FUNCTION_INFO_V1(cast_to_binary_integer);
PG_FUNCTION_INFO_V1(cast_to_number);
PG_FUNCTION_INFO_V1(cast_to_nvarchar2);
PG_FUNCTION_INFO_V1(cast_to_raw);
PG_FUNCTION_INFO_V1(cast_to_varchar2);
PG_FUNCTION_INFO_V1(compare);
PG_FUNCTION_INFO_V1(concat);
PG_FUNCTION_INFO_V1(convert);
PG_FUNCTION_INFO_V1(copies);
PG_FUNCTION_INFO_V1(reverse);
PG_FUNCTION_INFO_V1(func_translate);
PG_FUNCTION_INFO_V1(transliterate);
PG_FUNCTION_INFO_V1(xrange);

static uint32 get_machine_byte_order();
static uint64 reverse_bytes64(uint64 value);
static uint32 reverse_bytes32(uint32 value);
static bool isNeedReverse(uint32 endianness);
static void isValidEndianness(uint32 endianness);
static void compareTwoRaw(bytea* r1, bytea* r2, uint32* len_short, uint32* len_long, bytea** raw_short, bytea** raw_long);
static bytea* replace_bytea(char* src, uint32 srcLen, char* from, uint32 fromLen, char* to, uint32 toLen, bool isPad, const char pad);

Datum bit_and(PG_FUNCTION_ARGS)
{
    bytea* r1 = PG_GETARG_RAW_VARLENA_P(0);
    bytea* r2 = PG_GETARG_RAW_VARLENA_P(1);

    uint32 len_short = VARSIZE_ANY_EXHDR(r1);
    uint32 len_long = VARSIZE_ANY_EXHDR(r2);
    bytea* raw_short = r1;
    bytea* raw_long = r2;

    compareTwoRaw(r1, r2, &len_short, &len_long, &raw_short, &raw_long);

    bytea* result = (bytea*)palloc(len_long + VARHDRSZ);
    SET_VARSIZE(result, (len_long + VARHDRSZ));
    errno_t rc = memcpy_s(VARDATA(result), len_long, VARDATA(raw_long), len_long);
    securec_check(rc, "\0", "\0");

    uint8* long_data = (uint8*)VARDATA(result);
    uint8* short_data = (uint8*)VARDATA(raw_short);

    for (uint32 i = 0; i < len_short; i++) {
        long_data[i] &= short_data[i];
    }

    PG_RETURN_BYTEA_P(result);
}

static void compareTwoRaw(bytea* r1, bytea* r2, uint32* len_short, uint32* len_long, bytea** raw_short, bytea** raw_long)
{
    if (VARSIZE_ANY_EXHDR(r2) < VARSIZE_ANY_EXHDR(r1)) {
        *len_short = VARSIZE_ANY_EXHDR(r2);
        *len_long = VARSIZE_ANY_EXHDR(r1);
        *raw_short = r2;
        *raw_long = r1;
    }
}

Datum bit_or(PG_FUNCTION_ARGS)
{
    bytea* r1 = PG_GETARG_RAW_VARLENA_P(0);
    bytea* r2 = PG_GETARG_RAW_VARLENA_P(1);

    uint32 len_short = VARSIZE_ANY_EXHDR(r1);
    uint32 len_long = VARSIZE_ANY_EXHDR(r2);
    bytea* raw_short = r1;
    bytea* raw_long = r2;

    compareTwoRaw(r1, r2, &len_short, &len_long, &raw_short, &raw_long);

    bytea* result = (bytea*)palloc(len_long + VARHDRSZ);
    SET_VARSIZE(result, (len_long + VARHDRSZ));
    errno_t rc = memcpy_s(VARDATA(result), len_long, VARDATA(raw_long), len_long);
    securec_check(rc, "\0", "\0");

    uint8* long_data = (uint8*)VARDATA(result);
    uint8* short_data = (uint8*)VARDATA(raw_short);

    for (uint32 i = 0; i < len_short; i++) {
        long_data[i] |= short_data[i];
    }

    PG_RETURN_BYTEA_P(result);
}

Datum bit_xor(PG_FUNCTION_ARGS)
{
    bytea* r1 = PG_GETARG_RAW_VARLENA_P(0);
    bytea* r2 = PG_GETARG_RAW_VARLENA_P(1);

    uint32 len_short = VARSIZE_ANY_EXHDR(r1);
    uint32 len_long = VARSIZE_ANY_EXHDR(r2);
    bytea* raw_short = r1;
    bytea* raw_long = r2;

    compareTwoRaw(r1, r2, &len_short, &len_long, &raw_short, &raw_long);

    bytea* result = (bytea*)palloc(len_long + VARHDRSZ);
    SET_VARSIZE(result, (len_long + VARHDRSZ));
    errno_t rc = memcpy_s(VARDATA(result), len_long, VARDATA(raw_long), len_long);
    securec_check(rc, "\0", "\0");

    uint8* long_data = (uint8*)VARDATA(result);
    uint8* short_data = (uint8*)VARDATA(raw_short);

    for (uint32 i = 0; i < len_short; i++) {
        long_data[i] ^= short_data[i];
    }

    PG_RETURN_BYTEA_P(result);
}

Datum bit_complement(PG_FUNCTION_ARGS)
{
    bytea* r1 = PG_GETARG_RAW_VARLENA_P(0);

    uint32 length1 = VARSIZE_ANY_EXHDR(r1);

    bytea* result = (bytea*)palloc(length1 + VARHDRSZ);
    SET_VARSIZE(result, (length1 + VARHDRSZ));
    errno_t rc = memcpy_s(VARDATA(result), length1, VARDATA(r1), length1);
    securec_check(rc, "\0", "\0");

    uint8* data = (uint8*)VARDATA(result);

    for (uint32 i = 0; i < length1; i++) {
        data[i] = (uint8)(~data[i]);
    }

    PG_RETURN_BYTEA_P(result);
}

Datum cast_from_binary_double(PG_FUNCTION_ARGS)
{
    float8 input = PG_GETARG_FLOAT8(0);
    uint32 endianness = PG_GETARG_UINT32(1);

    isValidEndianness(endianness);

    uint64 input_bits = *(uint64*)&input;
    uint64 value = isNeedReverse(endianness) ? reverse_bytes64(input_bits) : input_bits;

    uint32 length = sizeof(input_bits);
    bytea* result = (bytea*)palloc(length + VARHDRSZ);
    SET_VARSIZE(result, length + VARHDRSZ);
    errno_t rc = memcpy_s(VARDATA(result), length, &value, length);
    securec_check(rc, "\0", "\0");

    PG_RETURN_BYTEA_P(result);
}

static void isValidEndianness(uint32 endianness)
{
    if ((BIG_ENDIAN_FLAG != endianness) && (LITTLE_ENDIAN_FLAG != endianness) &&
     (MACHINE_ENDIAN_FLAG != endianness)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The value of second parameter is invalid, it should be 1 for big_endian, 2 for little_endian or 3 for machine_endian.")));
    }
}

static bool isNeedReverse(uint32 endianness)
{
    if (endianness != MACHINE_ENDIAN_FLAG) {
        return endianness != get_machine_byte_order();
    }
    return false;
}

static uint32 get_machine_byte_order()
{
    uint32 value = 1;
    if (*(char*)&value == 0) {
        return BIG_ENDIAN_FLAG;
    } else {
        return LITTLE_ENDIAN_FLAG;
    }
}

static uint64 reverse_bytes64(uint64 value)
{
    return ((value & 0X00000000000000FF) << 56) | ((value & 0X000000000000FF00) << 40) |
        ((value & 0X0000000000FF0000) << 24) | ((value & 0X00000000FF000000) << 8) |
        ((value & 0X000000FF00000000) >> 8) | ((value & 0X0000FF0000000000) >> 24) |
        ((value & 0X00FF000000000000) >> 40) | ((value &0XFF00000000000000) >> 56);
}

Datum cast_from_binary_float(PG_FUNCTION_ARGS)
{
    float4 input = PG_GETARG_FLOAT8(0);
    uint32 endianness = PG_GETARG_UINT32(1);

    isValidEndianness(endianness);

    uint32 input_bits = *(uint32*)&input;
    uint32 value = isNeedReverse(endianness) ? reverse_bytes32(input_bits) : input_bits;

    uint32 length = sizeof(input_bits);
    bytea* result = (bytea*)palloc(length + VARHDRSZ);
    SET_VARSIZE(result, length + VARHDRSZ);
    errno_t rc = memcpy_s(VARDATA(result), length, &value, length);
    securec_check(rc, "\0", "\0");

    PG_RETURN_BYTEA_P(result);
}

Datum cast_from_binary_integer(PG_FUNCTION_ARGS)
{
    int64 input = PG_GETARG_INT64(0);
    uint32 endianness = PG_GETARG_UINT32(1);

    isValidEndianness(endianness);

    if (input > INT32_MAX_VALUE) {
        input = INT32_MAX_VALUE;
    } else if (input < INT32_MIN_VALUE) {
        input = INT32_MIN_VALUE;
    }
    
    uint32 input_bits = (uint32)input;
    uint32 value = isNeedReverse(endianness) ? reverse_bytes32(input_bits) : input_bits;

    uint32 length = sizeof(input_bits);
    bytea* result = (bytea*)palloc(length + VARHDRSZ);
    SET_VARSIZE(result, length + VARHDRSZ);
    errno_t rc = memcpy_s(VARDATA(result), length, &value, length);
    securec_check(rc, "\0", "\0");

    PG_RETURN_BYTEA_P(result);
}

Datum cast_from_number(PG_FUNCTION_ARGS)
{
    Numeric value = PG_GETARG_NUMERIC(0);
    PG_RETURN_BYTEA_P(value);
}

Datum cast_to_binary_double(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    uint32 endianness = PG_GETARG_UINT32(1);

    isValidEndianness(endianness);

    uint32 length = VARSIZE_ANY_EXHDR(input);
    if (length < BINARY_DOUBLE_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The value of first parameter is invalid")));
    }
    length = BINARY_DOUBLE_SIZE;

    uint8 item[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    errno_t rc = memcpy_s((uint8*)item, length, VARDATA_ANY(input), length);
    securec_check(rc, "\0", "\0");
    uint64 output_bits = isNeedReverse(endianness) ? reverse_bytes64(*(uint64*)item) : *(uint64*)item;
    float8 output = *(float8*)&output_bits;
    PG_RETURN_FLOAT8(output);
}

Datum cast_to_binary_float(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    uint32 endianness = PG_GETARG_UINT32(1);

    isValidEndianness(endianness);

    uint32 length = VARSIZE_ANY_EXHDR(input);
    if (length < BINARY_FLOAT_INTEGER_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The value of first parameter is invalid")));
    }
    length = BINARY_FLOAT_INTEGER_SIZE;

    uint8 item[4] = {0, 0, 0, 0};
    errno_t rc = memcpy_s((uint8*)item, length, VARDATA_ANY(input), length);
    securec_check(rc, "\0", "\0");
    uint32 output_bits = isNeedReverse(endianness) ? reverse_bytes32(*(uint32*)item) : *(uint32*)item;
    float4 output = *(float4*)&output_bits;
    PG_RETURN_FLOAT4(output);
}

static uint32 reverse_bytes32(uint32 value)
{
    return ((value & 0X000000FF) << 24) | ((value & 0X0000FF00) << 8) |
        ((value & 0X00FF0000) >> 8) | ((value & 0XFF000000) >> 24);
}

Datum cast_to_binary_integer(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    uint32 endianness = PG_GETARG_UINT32(1);

    isValidEndianness(endianness);

    uint32 length = VARSIZE_ANY_EXHDR(input);
    if (length > BINARY_FLOAT_INTEGER_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The value of first parameter is invalid")));
    }
    length = BINARY_FLOAT_INTEGER_SIZE;

    uint8 item[4] = {0, 0, 0, 0};
    errno_t rc = memcpy_s((uint8*)item, length, VARDATA_ANY(input), length);
    securec_check(rc, "\0", "\0");
    uint32 output_bits = isNeedReverse(endianness) ? reverse_bytes32(*(uint32*)item) : *(uint32*)item;
    int32 output = *(int32*)&output_bits;
    PG_RETURN_INT32(output);
}

Datum cast_to_number(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    PG_RETURN_NUMERIC(input);
}

Datum cast_to_nvarchar2(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    PG_RETURN_NVARCHAR2_P(input);
}
Datum cast_to_raw(PG_FUNCTION_ARGS)
{
    VarChar *input = PG_GETARG_VARCHAR_P(0);
    PG_RETURN_BYTEA_P(input);
}

Datum cast_to_varchar2(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    PG_RETURN_VARCHAR_P(input);
}

Datum compare(PG_FUNCTION_ARGS)
{
    bytea* r1 = PG_GETARG_RAW_VARLENA_P(0);
    bytea* r2 = PG_GETARG_RAW_VARLENA_P(1);
    bytea* pad = PG_GETARG_RAW_VARLENA_P(2);
    int64 result = 0;
    uint8 first_byte = 0;
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_NUMERIC(convert_int64_to_numeric(0, 0));
    }
    if (!PG_ARGISNULL(2)) {
        first_byte = ((uint8*)VARDATA(pad))[0];
    }

    int64 len_short = 0;
    int64 len_long = 0;
    uint8* short_data = NULL;
    uint8* long_data = NULL;

    if (!PG_ARGISNULL(0)) {
        len_short = VARSIZE_ANY_EXHDR(r1);
        short_data = (uint8*)VARDATA(r1);
    }

    if (!PG_ARGISNULL(1)) {
        len_long = VARSIZE_ANY_EXHDR(r2);
        long_data = (uint8*)VARDATA(r2);
    }

    if (len_short > len_long) {
        int64 len_temp = len_short;
        uint8* temp_data = short_data;
        len_short = len_long;
        len_long = len_temp;
        short_data = long_data;
        long_data = temp_data;
    }

    for (int64 i = 0; i < len_short; i++) {
        if (short_data[i] != long_data[i]) {
            PG_RETURN_NUMERIC(convert_int64_to_numeric(i + 1, 0));
        }
    }
    for (int64 i = len_short; i < len_long; i++) {
        if (long_data[i] != first_byte) {
            PG_RETURN_NUMERIC(convert_int64_to_numeric(i + 1, 0));
        }
    }
    PG_RETURN_NUMERIC(convert_int64_to_numeric(result, 0));
}

Datum concat(PG_FUNCTION_ARGS)
{
    int8 arg_num = PG_NARGS();
    uint32 total_length = 0;
    bytea* raw_array[arg_num] = {};
    for (int8 i = 0; i < arg_num; i++) {
        if (!PG_ARGISNULL(i)) {
            raw_array[i] = PG_GETARG_RAW_VARLENA_P(i);
            total_length += VARSIZE_ANY_EXHDR(raw_array[i]);
        }
    }
    
    bytea* result = (bytea*)palloc(total_length + VARHDRSZ);
    SET_VARSIZE(result, (total_length + VARHDRSZ));
    int32 offset = 0;
    for (int8 i = 0; i < arg_num; i++) {
        if (!PG_ARGISNULL(i)) {
            uint32 raw_length = VARSIZE_ANY_EXHDR(raw_array[i]);
            errno_t rc = memcpy_s(VARDATA(result) + offset, raw_length, VARDATA(raw_array[i]), raw_length);
            securec_check(rc, "\0", "\0");
            offset += raw_length;
        }
    }
    PG_RETURN_BYTEA_P(result);
}

Datum convert(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The input parameters contaion null value, please check the parameter.")));
    }

    char* src = text_to_cstring(PG_GETARG_TEXT_PP(2));
    char* dest = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Datum src_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(src));
    Datum dest_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(dest));
    
    bytea* result = (bytea*)DirectFunctionCall3(pg_convert, PG_GETARG_DATUM(0), src_encoding_name, dest_encoding_name);
    PG_RETURN_BYTEA_P(result);
}

Datum copies(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The input parameters contaion null value, please check the parameter.")));
    }

    bytea* raw = PG_GETARG_RAW_VARLENA_P(0);
    char* rawStr = VARDATA(raw);
    uint32 rawLen = VARSIZE_ANY_EXHDR(raw);

    Datum num = PG_GETARG_DATUM(1);

    int64 copiedNum = (int64)DirectFunctionCall1(numeric_int8, num);

    if (copiedNum < 1 || copiedNum > MAX_RAW_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The second parameter is less than 1 or more than the max value %d, please check the parameter.", MAX_RAW_SIZE)));
    }

    uint128 newLen = rawLen * copiedNum;
    if (newLen > MAX_RAW_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The total hexadecimal string length is more than the max value %d, please check the parameter.", MAX_RAW_SIZE)));
    }

    bytea* result = (bytea*)palloc(newLen + (uint128)VARHDRSZ);
    SET_VARSIZE(result, newLen + (uint128)VARHDRSZ);
    int offset = 0;
    for (int128 i = 0; i < copiedNum; i++) {
        errno_t rc = memcpy_s(VARDATA(result) + offset, rawLen, rawStr, rawLen);
        securec_check(rc, "\0", "\0");
        offset += rawLen;
    }
    PG_RETURN_BYTEA_P(result);
}

Datum reverse(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The input parameter contaions null value, please check the parameter.")));
    }
    bytea* raw = PG_GETARG_RAW_VARLENA_P(0);
    char* rawStr = VARDATA(raw);
    uint32 rawLen = VARSIZE_ANY_EXHDR(raw);

    bytea* result = (bytea*)palloc(rawLen + VARHDRSZ);
    SET_VARSIZE(result, rawLen + VARHDRSZ);
    char* resultStr = VARDATA(result);
    for (uint32 i = 0; i < rawLen; i++) {
        resultStr[i] = rawStr[rawLen - 1 - i];
    }
    PG_RETURN_BYTEA_P(result);
}

Datum func_translate(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The input parameters contaion null value, please check the parameter.")));
    }
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    bytea* from = PG_GETARG_RAW_VARLENA_P(1);
    bytea* to = PG_GETARG_RAW_VARLENA_P(2);
    uint32 inputSize = VARSIZE_ANY_EXHDR(input);
    uint32 fromSize = VARSIZE_ANY_EXHDR(from);
    uint32 toSize = VARSIZE_ANY_EXHDR(to);

    bytea* result = replace_bytea(VARDATA(input), inputSize, VARDATA(from), fromSize, VARDATA(to), toSize, false, 0);
    if (result == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_BYTEA_P(result);
}

/*
 * Each byte of 'src' will have three cases:
 * 1) be replaced with the corresponding byte in 'to', depending on the mapping between 'from' and 'to';
 * 2) be deleted when no mapping between 'from' and 'to', and isPad is false;
 * 3) be replaced with 'pad' when between 'from' and 'to', but isPad is true.
 * 
 * Return the result after replacement or deletion for the input 'src'.
 */
static bytea* replace_bytea(char* src, uint32 srcLen, char* from, uint32 fromLen, char* to, uint32 toLen, bool isPad, const char pad) {
    // If from is null, replace all with pad.
    if (fromLen == 0 && isPad) {
        uint32 resLen = srcLen + (uint32)VARHDRSZ;
        bytea* res = (bytea*)palloc(resLen);
        SET_VARSIZE(res, resLen);
        errno_t rc = memset_s(VARDATA(res), srcLen, pad, srcLen);
        securec_check(rc, "\0", "\0");
        return res;
    } 
    
    const uint32 ARRAY_SIZE = 256;

    // Mapping array, record the mapping between 'from' and 'to'.
    char fromToArray[ARRAY_SIZE] = {0};
    // Flag array, record replacement and deletion flag.
    char fromToFlag[ARRAY_SIZE] = {0};

    const char FLAG_REPLACE = 0x01;
    const char FLAG_DELETE = 0x02;

    // record the mapping and flag for each byte in 'src'.
    uint8 index = 0;
    for (uint32 i = 0; i < fromLen; i++) {
        index = (uint8)from[i];
        // only set once for the mapping and flag
        if (fromToFlag[index] == FLAG_DELETE || fromToFlag[index] == FLAG_REPLACE) {
            continue;
        }
        // when fromLen > toLen, there is no mapping between 'from' and 'to',
        // the bytes in 'src', which index >= toLen, will be replaced or deleted.
        if (i >= toLen) {
            if (isPad) {
                // when isPad is true, the byte in 'src' will be replaced eith pad.
                fromToFlag[index] = FLAG_REPLACE;
                fromToArray[index] = pad;
            } else {
                // when isPad is true, the byte in 'src' will be deleted.
                fromToFlag[index] = FLAG_DELETE;
            }
            continue;
        }
        // set the mapping and flag is replacement when there is mapping between 'src' and 'to'.
        fromToFlag[index] = FLAG_REPLACE;
        fromToArray[index] = to[i];
    }

    // the maximum length of the result is srcLen.
    uint32 maxResultLen = srcLen + (uint32)VARHDRSZ;
    bytea* maxResult = (bytea*)palloc(maxResultLen);
    SET_VARSIZE(maxResult, maxResultLen);
    char* maxResultData = VARDATA(maxResult);

    uint32 len = 0;
    // based on the rule above, process each byte in 'src', either replacement or deletion.
    for (uint i = 0; i < srcLen; i++) {
        index = (uint8)src[i];
        if (fromToFlag[index] == FLAG_DELETE) {
            continue;
        }
        if (fromToFlag[index] == FLAG_REPLACE) {
            maxResultData[len] = fromToArray[index];
            len++;
        } else {
            maxResultData[len] = src[i];
            len++;
        }
    }
    if (len == srcLen) {
        return maxResult;
    }
    // when result length is less than srcLen, calculate the actual length.
    bytea* result = NULL;
    if (len > 0) {
        uint32 resultLen = len + (uint32)VARHDRSZ;
        result = (bytea*)palloc(resultLen);
        SET_VARSIZE(result, resultLen);
        errno_t rc = memcpy_s(VARDATA(result), len, maxResultData, len);
        securec_check(rc, "\0", "\0");
    }
    pfree_ext(maxResult);
    return result;
}

Datum transliterate(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The first parameter is null, please check the parameter.")));
    }
    bytea* input = PG_GETARG_RAW_VARLENA_P(0);
    bytea* from = PG_GETARG_RAW_VARLENA_P(2);
    bytea* to = PG_GETARG_RAW_VARLENA_P(1);
    bytea* pad = PG_GETARG_RAW_VARLENA_P(3);

    uint32 inputSize = (PG_ARGISNULL(0)) ? 0 : VARSIZE_ANY_EXHDR(input);
    char* fromString = NULL;
    uint32 fromSize = 0;
    char* toString = NULL;
    uint32 toSize = 0;
    const char defaultPad = 0x00;

    if (!PG_ARGISNULL(2)) {
        fromString =  VARDATA(from);
        fromSize = VARSIZE_ANY_EXHDR(from);
    }
    
    if (!PG_ARGISNULL(1)) {
        toString =  VARDATA(to);
        toSize = VARSIZE_ANY_EXHDR(to);
    }

    char padChar = (PG_ARGISNULL(3)) ? defaultPad : VARDATA(pad)[0];

    bytea* result = replace_bytea(VARDATA(input), inputSize, fromString, fromSize, toString, toSize, true, padChar);
    if (result == NULL) {
        PG_RETURN_NULL();
    }

    PG_RETURN_BYTEA_P(result);
}

Datum xrange(PG_FUNCTION_ARGS)
{
    bytea* start = PG_GETARG_RAW_VARLENA_P(0);
    bytea* end = PG_GETARG_RAW_VARLENA_P(1);
    const uint8 MIN_ONE_BYTE = 0x00;
    const uint8 MAX_ONE_BYTE = 0xff;
    int32 MAX_SIZE = 256;
    uint8 startChar = (PG_ARGISNULL(0)) ? MIN_ONE_BYTE : (uint8)VARDATA(start)[0];
    uint8 endChar = (PG_ARGISNULL(1)) ? MAX_ONE_BYTE : (uint8)VARDATA(end)[0];

    if (startChar == endChar) {
        PG_RETURN_BYTEA_P(start);
    }

    int32 startPosition = (int32)startChar;
    int32 endPosition = (int32)endChar;
    int32 resultLen;

    if (endPosition > startPosition) {
        resultLen = endPosition - startPosition + 1;
    } else {
        resultLen = endPosition - startPosition + MAX_SIZE + 1;
    }
    bytea* result = (bytea*)palloc(resultLen + VARHDRSZ);
    SET_VARSIZE(result, resultLen + VARHDRSZ);
    char * resultData = VARDATA(result);
    for (int32 i = 0; i < resultLen; i++) {
        resultData[i] = (char)((startPosition + i + MAX_SIZE) % MAX_SIZE);
    }
    PG_RETURN_BYTEA_P(result);
}
