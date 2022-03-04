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
 * gs_num.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_num.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#define ereport(a, b) return false;
#define ereport_null(a, b) return NULL;
#include <iostream>
#include <arpa/inet.h>
#include <endian.h>
#include "gs_num.h"
#include "int8.h"
#include "gs_float.h"
#include "numeric.h"
#include "numutils.h"
#include "libpq-int.h"
#define MAXINT1LEN 5
#define MAXINT2LEN 7
#define MAXINT4LEN 12
#define MAXINT8LEN 25
#define MAXFLOATWIDTH 64
#define MAXDOUBLEWIDTH 128

/* we do 64-bit alignment for integers so all of the JOIN operations on processed values can work. */

/*
 * int1_bin -
 * convert string to integer
 */
unsigned char *int1_bin(const PGconn* conn, const char *text, size_t *binary_size, char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, sizeof(int64), 0, sizeof(int64));
    securec_check_c(rc, "\0", "\0");
    if (!fe_pg_atoi8(conn, text, (int8 *)binary, err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(int64);
    return binary;
}

/*
 * int1_bout -
 * converts a unsigned 8-bit integer to its string representation
 */
char *int1_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    char *text = (char *)malloc(MAXINT1LEN);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, MAXINT1LEN, 0, MAXINT1LEN);
    securec_check_c(rc, "\0", "\0");
    bool ret = fe_pg_ctoa(*(int8 *)binary, text);
    *result_size = ret ? strlen(text) : 0;
    return text;
}

unsigned char *int1_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg)
{
    binary = (unsigned char *)libpq_realloc(binary, *binary_size, sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary + *binary_size, sizeof(int64) - *binary_size, 0, sizeof(int64) - *binary_size);
    securec_check_c(rc, "\0", "\0");
    *binary_size = sizeof(int64);
    return binary;
}

unsigned char *int1_brestore(const unsigned char *binary_in, size_t size, size_t *result_size, const char *err_msg)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    unsigned char *result = (unsigned char *)calloc(1, sizeof(int8));
    if (result == NULL) {
        return NULL;
    }
    *(int8 *)result = *(const int8 *)binary_in;
    *result_size = sizeof(int8);
    return result;
}

/*
 * int2_bin -
 * Convert input string to a signed 16 bit integer.
 */
unsigned char *int2_bin(const PGconn *conn, const char *text, size_t *binary_size,
    char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, sizeof(int64), 0, sizeof(int64));
    securec_check_c(rc, "\0", "\0");
    if (!fe_pg_strtoint16(conn, text, (int16 *)binary, err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(int64);
    return binary;
}

/*
 * int2_bin -
 * converts a signed 16-bit integer to its string representation
 */
char *int2_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    char *text = (char *)malloc(MAXINT2LEN);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, MAXINT2LEN, 0, MAXINT2LEN);
    securec_check_c(rc, "\0", "\0");
    bool ret = fe_pg_itoa(*(int16 *)binary, text);
    *result_size = ret ? strlen(text) : 0;
    return text;
}

unsigned char *int2_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg)
{
    binary = (unsigned char *)libpq_realloc(binary, *binary_size, sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary + *binary_size, sizeof(int64) - *binary_size, 0, sizeof(int64) - *binary_size);
    securec_check_c(rc, "\0", "\0");
    *binary_size = sizeof(int64);
    short &n = *(short *)binary;
    n = ntohs(n);
    return binary;
}

unsigned char *int2_brestore(const unsigned char *binary, size_t size, size_t *result_size,
    const char *err_msg)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    unsigned char *result = (unsigned char *)malloc(sizeof(int16));
    if (result == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(result, sizeof(int8), 0, sizeof(int8));
    securec_check_c(rc, "\0", "\0");
    *(int16 *)result = htons(*(const int16 *)binary);
    *result_size = sizeof(int16);
    return result;
}

/*
 * int4_bin -
 * Convert input string to a signed 32 bit integer.
 */
unsigned char *int4_bin(const PGconn *conn, const char *text, size_t *binary_size,
    char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, sizeof(int64), 0, sizeof(int64));
    securec_check_c(rc, "\0", "\0");
    if (!fe_pg_strtoint32(conn, text, (int32 *)binary, err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(int64);
    return binary;
}

/*
 * int4_bout -
 * converts a signed 32-bit integer to its string representation
 */
char *int4_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }

    char *text = (char *)malloc(MAXINT4LEN);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, MAXINT4LEN, 0, MAXINT4LEN);
    securec_check_c(rc, "\0", "\0");
    bool ret = fe_pg_ltoa(*(int32 *)binary, text);
    *result_size = ret ? strlen(text) : 0;
    return text;
}

unsigned char *int4_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg)
{
    binary = (unsigned char *)libpq_realloc(binary, *binary_size, sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary + *binary_size, sizeof(int64) - *binary_size, 0, sizeof(int64) - *binary_size);
    securec_check_c(rc, "\0", "\0");
    *binary_size = sizeof(int64);
    long &n = *(long *)binary;
    n = ntohl(n);
    return binary;
}

unsigned char *int4_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    unsigned char *result = (unsigned char *)malloc(sizeof(int32));
    if (result == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(result, sizeof(int32), 0, sizeof(int32));
    securec_check_c(rc, "\0", "\0");
    *(int32 *)result = htonl(*(const int32 *)binary);
    *result_size = sizeof(int32);
    return result;
}

/*
 * int8_bin -
 * try to parse a string into an 64-bit integer.
 */
unsigned char *int8_bin(const PGconn *conn, const char *text, size_t *binary_size,
    char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, sizeof(int64), 0, sizeof(int64));
    securec_check_c(rc, "\0", "\0");
    if (!scanint8(conn, text, false, (int64 *)binary, err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(int64);
    return binary;
}

/*
 * int8_bout -
 * convert a signed 64-bit integer to its string representation
 */
char *int8_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    char *text = (char *)malloc(MAXINT8LEN);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, MAXINT8LEN, 0, MAXINT8LEN);
    securec_check_c(rc, "\0", "\0");
    bool ret = fe_pg_lltoa(*(int64 *)binary, text);
    *result_size = ret ? strlen(text) : 0;
    return text;
}

/*
 * int8_badjust -
 * adjust binary array for int8 data,
 * because how the data is saved in disk and how the client/application expects to retrieve it
 * are different, change the endianess
 */
unsigned char *int8_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg)
{
    *binary_size = sizeof(int64);
    long &n = *(long *)binary;
    n = be64toh(n);
    return binary;
}

/*
 * restore_binary -
 * restore binary array, change the endianess
 */
unsigned char *int8_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    unsigned char *result = (unsigned char *)malloc(sizeof(int64));
    if (result == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(result, sizeof(int64), 0, sizeof(int64));
    securec_check_c(rc, "\0", "\0");
    *(int64 *)result = htobe64(*(const int64 *)binary);
    *result_size = sizeof(int64);
    return result;
}

/*
 * float4_bin -
 * try to parse a string into a float.
 */
unsigned char *float4_bin(const char *text, size_t *binary_size, char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(int64));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, sizeof(int64), 0, sizeof(int64));
    securec_check_c(rc, "\0", "\0");
    if (!scan_float4(text, (float4 *)binary, err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(int64);
    return binary;
}

/*
 * float4_bout -
 * converts a float4 number to a ascii string
 */
char *float4_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != sizeof(int64)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    char *text = (char *)malloc(MAXFLOATWIDTH + 1);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, MAXFLOATWIDTH + 1, 0, MAXFLOATWIDTH + 1);
    securec_check_c(rc, "\0", "\0");
    bool ret = float4toa(*(float4 *)binary, text);
    *result_size = ret ? strlen(text) : 0;
    return text;
}

/*
 * float8_bin -
 * try to parse a string into a float8.
 */
unsigned char *float8_bin(const char *text, size_t *binary_size, char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(float8));
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, sizeof(float8), 0, sizeof(float8));
    securec_check_c(rc, "\0", "\0");
    if (!scan_float8(text, (float8 *)binary, err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(float8);
    return binary;
}

/*
 * float4_bout -
 * converts a float8 number to a ascii string
 */
char *float8_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != sizeof(float8)) {
        ereport_null(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }
    char *text = (char *)malloc(MAXDOUBLEWIDTH + 1);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, MAXDOUBLEWIDTH + 1, 0, MAXDOUBLEWIDTH + 1);
    securec_check_c(rc, "\0", "\0");
    if (!float8toa(*(float8 *)binary, text)) {
        free(text);
        return NULL;
    }
    *result_size = MAXDOUBLEWIDTH + 1;
    return text;
}

/*
 * numeric_bin -
 * Input function for numeric data type
 * converts a numeric data type string to
 * a NumericChoice struct(binary array)
 */
unsigned char *numeric_bin(const PGconn* conn, const char *text, int atttypmod, size_t *binary_size, char *err_msg)
{
    return scan_numeric(conn, text, atttypmod, binary_size, err_msg);
}

/*
 * numeric_bout -
 * Output function for numeric data type
 * converts a NumericChoice struct(binary array)
 * to a numeric data type string
 */
char *numeric_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    size_t len = NUMERIC_MAX_PRECISION + NUMERIC_MAX_RESULT_SCALE + 1;
    if (size > len) {
        fprintf(stderr, "unsupported result size: %zu\n", size);
        return NULL;
    }
    char *text = (char *)malloc(len);
    if (text == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(text, len, 0, len);
    securec_check_c(rc, "\0", "\0");
    if (!numerictoa((NumericData *)binary, text, len)) {
        free(text);
        return NULL;
    }
    *result_size = len;
    return text;
}

unsigned char *numeric_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg)
{
    if (apply_typmod((NumericVar *)binary, atttypmod, err_msg)) {
        NumericVar nv_copy = *(NumericVar *)binary;
        return make_result(&nv_copy, binary_size, err_msg);
    }
    return NULL;
}
