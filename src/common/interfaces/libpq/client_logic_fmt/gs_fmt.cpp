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
 * gs_fmt.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_fmt.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "gs_fmt.h"
#include "gs_num.h"
#include "gs_char.h"
#include "gs_bool.h"
#include "client_logic_common/client_logic_utils.h"

/*
 * type_char_bin -
 * converts char style to binary array(unigned char)
 */
unsigned char *Format::type_char_bin(const char *text, Oid type, int atttypmod, size_t *binary_size,
    char *err_msg)
{
    unsigned char *binary = fallback_bin(text, binary_size);
    if (binary != NULL) {
        switch (type) {
            case VARCHAROID: {
                return varchar_badjust(binary, binary_size, atttypmod, err_msg);
            }
            case NVARCHAR2OID: {
                return nvarchar2_badjust(binary, binary_size, atttypmod, err_msg);
            }
            case BPCHAROID: {
                return bpchar_badjust(binary, binary_size, atttypmod, err_msg);
            }
            default:
                break;
        }
    }
    return binary;
}

/*
 * text_to_binary -
 * converts a assic string to a binary array
 */
unsigned char *Format::text_to_binary(const PGconn* conn,  const char *text, Oid type,
    int atttypmod, size_t *binary_size, char *err_msg)
{
    if (!text || !binary_size) {
        return NULL;
    }

    switch (type) {
        case BYTEAOID: {
            return bytea_bin(text, binary_size, err_msg);
        }
        case CHAROID: {
            return char_bin(text, binary_size, err_msg);
        }
        case INT8OID: {
            return int8_bin(conn, text, binary_size, err_msg);
        }
        case INT2OID: {
            return int2_bin(conn, text, binary_size, err_msg);
        }
        case INT1OID: {
            return int1_bin(conn, text, binary_size, err_msg);
        }
        case INT4OID: {
            return int4_bin(conn, text, binary_size, err_msg);
        }
        case FLOAT4OID: {
            return float4_bin(text, binary_size, err_msg);
        }
        case FLOAT8OID: {
            return float8_bin(text, binary_size, err_msg);
        }
        case NUMERICOID: {
            return numeric_bin(conn, text, atttypmod, binary_size, err_msg);
        }
        case VARCHAROID:
        case NVARCHAR2OID:
        case BPCHAROID: {
            return type_char_bin(text, type, atttypmod, binary_size, err_msg);
        }
        case BOOLOID: {
            return bool_bin(text, binary_size, err_msg);
        }
        default: {
            return fallback_bin(text, binary_size);
        }
    }
    return NULL;
}

/*
 * binary_to_text -
 * converts a binary array to a assic string
 */
char *Format::binary_to_text(const unsigned char *binary, size_t length, Oid type, size_t *result_size)
{
    if (binary == NULL || result_size == NULL) {
        return NULL;
    }
    switch (type) {
        case BYTEAOID: {
            return bytea_bout(binary, length, result_size);
        }
        case CHAROID: {
            return char_bout(binary, length, result_size);
        }
        case INT8OID: {
            return int8_bout(binary, length, result_size);
        }
        case INT2OID: {
            return int2_bout(binary, length, result_size);
        }
        case INT1OID: {
            return int1_bout(binary, length, result_size);
        }
        case INT4OID: {
            return int4_bout(binary, length, result_size);
        }
        case FLOAT4OID: {
            return float4_bout(binary, length, result_size);
        }
        case FLOAT8OID: {
            return float8_bout(binary, length, result_size);
        }
        case NUMERICOID: {
            return numeric_bout(binary, length, result_size);
        }
        case BOOLOID: {
            return bool_bout(binary, length, result_size);
        }
        default: {
            return fallback_bout(binary, length, result_size);
        }
    }
}

/*
 * verify_and_adjust_binary -
 * verify and adjust binary array,
 * realloc the memory for data, and because how the data is saved in disk and
 * how the client/application expects to retrieve it are different, change the endianess
 */
unsigned char *Format::verify_and_adjust_binary(unsigned char *binary, size_t *binary_size, Oid type,
    int atttypmod, char *err_msg)
{
    if (!binary || !binary_size) {
        return NULL;
    }
    switch (type) {
        case FLOAT8OID:
        case INT8OID: {
            return int8_badjust(binary, binary_size, err_msg);
        }
        case INT2OID: {
            return int2_badjust(binary, binary_size, err_msg);
        }
        case INT1OID: {
            return int1_badjust(binary, binary_size, err_msg);
        }
        case FLOAT4OID:
        case INT4OID: {
            return int4_badjust(binary, binary_size, err_msg);
        }
        case VARCHAROID: {
            return varchar_badjust(binary, binary_size, atttypmod, err_msg);
        }
        case NVARCHAR2OID: {
            return nvarchar2_badjust(binary, binary_size, atttypmod, err_msg);
        }
        case BPCHAROID: {
            return bpchar_badjust(binary, binary_size, atttypmod, err_msg);
        }
        case NUMERICOID: {
            return numeric_badjust(binary, binary_size, atttypmod, err_msg);
        }
        default: {
            return binary;
        }
    }
    return NULL;
}

/*
 * restore_binary -
 * restore binary array, change the endianess
 */
unsigned char *Format::restore_binary(const unsigned char *binary, size_t size, Oid type, size_t *result_size,
    const char *err_msg)
{
    switch (type) {
        case FLOAT8OID:
        case INT8OID: {
            return int8_brestore(binary, size, result_size, err_msg);
        }
        case INT2OID: {
            return int2_brestore(binary, size, result_size, err_msg);
        }
        case INT1OID: {
            return int1_brestore(binary, size, result_size, err_msg);
        }
        case FLOAT4OID:
        case INT4OID: {
            return int4_brestore(binary, size, result_size, err_msg);
        }
        default: {
            return fallback_brestore(binary, size, result_size, err_msg);
        }
    }
    return NULL;
}
