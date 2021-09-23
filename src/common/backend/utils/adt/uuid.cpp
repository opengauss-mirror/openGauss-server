/* -------------------------------------------------------------------------
 *
 * uuid.c
 *	  Functions for the built-in type "uuid".
 *
 * Copyright (c) 2007-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/uuid.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/uuid.h"

static void string_to_uuid(const char* source, pg_uuid_t* uuid);
static int uuid_internal_cmp(const pg_uuid_t* arg1, const pg_uuid_t* arg2);

Datum uuid_in(PG_FUNCTION_ARGS)
{
    char* uuid_str = PG_GETARG_CSTRING(0);
    pg_uuid_t* uuid = NULL;

    uuid = (pg_uuid_t*)palloc(sizeof(*uuid));
    string_to_uuid(uuid_str, uuid);
    PG_RETURN_UUID_P(uuid);
}

Datum uuid_out(PG_FUNCTION_ARGS)
{
    pg_uuid_t* uuid = PG_GETARG_UUID_P(0);
    static const char hex_chars[] = "0123456789abcdef";
    StringInfoData buf;
    int i;

    initStringInfo(&buf);
    for (i = 0; i < UUID_LEN; i++) {
        int hi;
        int lo;

        /*
         * We print uuid values as a string of 8, 4, 4, 4, and then 12
         * hexadecimal characters, with each group is separated by a hyphen
         * ("-"). Therefore, add the hyphens at the appropriate places here.
         */
        if (i == 4 || i == 6 || i == 8 || i == 10)
            appendStringInfoChar(&buf, '-');

        hi = uuid->data[i] >> 4;
        lo = uuid->data[i] & 0x0F;

        appendStringInfoChar(&buf, hex_chars[hi]);
        appendStringInfoChar(&buf, hex_chars[lo]);
    }

    PG_RETURN_CSTRING(buf.data);
}

/*
 * We allow UUIDs as a series of 32 hexadecimal digits with an optional dash
 * after each group of 4 hexadecimal digits, and optionally surrounded by {}.
 * (The canonical format 8x-4x-4x-4x-12x, where "nx" means n hexadecimal
 * digits, is the only one used for output.)
 */
static void string_to_uuid(const char* source, pg_uuid_t* uuid)
{
    const char* src = source;
    bool braces = false;
    int i;
    errno_t rc = EOK;

    if (src[0] == '{') {
        src++;
        braces = true;
    }

    for (i = 0; i < UUID_LEN; i++) {
        char str_buf[3];

        if (src[0] == '\0' || src[1] == '\0')
            goto syntax_error;

        rc = memcpy_s(str_buf, sizeof(str_buf), src, 2);
        securec_check(rc, "\0", "\0");

        if (!isxdigit((unsigned char)str_buf[0]) || !isxdigit((unsigned char)str_buf[1]))
            goto syntax_error;

        str_buf[2] = '\0';
        uuid->data[i] = (unsigned char)strtoul(str_buf, NULL, 16);
        src += 2;
        if (src[0] == '-' && (i % 2) == 1 && i < UUID_LEN - 1)
            src++;
    }

    if (braces) {
        if (*src != '}')
            goto syntax_error;
        src++;
    }

    if (*src != '\0')
        goto syntax_error;

    return;

syntax_error:
    ereport(
        ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for uuid: \"%s\"", source)));
}

Datum uuid_recv(PG_FUNCTION_ARGS)
{
    StringInfo buffer = (StringInfo)PG_GETARG_POINTER(0);
    pg_uuid_t* uuid = NULL;

    uuid = (pg_uuid_t*)palloc(UUID_LEN);
    errno_t rc = memcpy_s(uuid->data, UUID_LEN, pq_getmsgbytes(buffer, UUID_LEN), UUID_LEN);
    securec_check(rc, "\0", "\0");
    PG_RETURN_POINTER(uuid);
}

Datum uuid_send(PG_FUNCTION_ARGS)
{
    pg_uuid_t* uuid = PG_GETARG_UUID_P(0);
    StringInfoData buffer;

    pq_begintypsend(&buffer);
    pq_sendbytes(&buffer, (char*)uuid->data, UUID_LEN);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buffer));
}

/* internal uuid compare function */
static int uuid_internal_cmp(const pg_uuid_t* arg1, const pg_uuid_t* arg2)
{
    return memcmp(arg1->data, arg2->data, UUID_LEN);
}

Datum uuid_lt(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_BOOL(uuid_internal_cmp(arg1, arg2) < 0);
}

Datum uuid_le(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_BOOL(uuid_internal_cmp(arg1, arg2) <= 0);
}

Datum uuid_eq(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_BOOL(uuid_internal_cmp(arg1, arg2) == 0);
}

Datum uuid_ge(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_BOOL(uuid_internal_cmp(arg1, arg2) >= 0);
}

Datum uuid_gt(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_BOOL(uuid_internal_cmp(arg1, arg2) > 0);
}

Datum uuid_ne(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_BOOL(uuid_internal_cmp(arg1, arg2) != 0);
}

/* handler for btree index operator */
Datum uuid_cmp(PG_FUNCTION_ARGS)
{
    pg_uuid_t* arg1 = PG_GETARG_UUID_P(0);
    pg_uuid_t* arg2 = PG_GETARG_UUID_P(1);

    PG_RETURN_INT32(uuid_internal_cmp(arg1, arg2));
}

/* hash index support */
Datum uuid_hash(PG_FUNCTION_ARGS)
{
    pg_uuid_t* key = PG_GETARG_UUID_P(0);

    return hash_any(key->data, UUID_LEN);
}

static int hash_ctoa(char ch)
{
    int res = 0;
    if (ch >= 'a' && ch <= 'f') {
        res = 10 + (ch - 'a');
    } else {
        res = ch - '0';
    }
    return res;
}

Datum hash16in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    int len = strlen(str);
    uint64 res = 0;
    if (len > 16) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for hash16: \"%s\"", str)));
    }

    for (int i = 0; i < len; ++i) {
        if (str[0] == '-') {
            res = 0;
            break;
        }
        if (!((str[i] >= 'a' && str[i] <= 'f') || (str[i] >= '0' && str[i] <= '9'))) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for hash16: \"%s\"", str)));
        }
        res = res * 16 + hash_ctoa(str[i]);
    }

    PG_RETURN_TRANSACTIONID(res);
}

Datum hash16out(PG_FUNCTION_ARGS)
{
    uint64 hash16 = PG_GETARG_TRANSACTIONID(0);
    static const char hex_chars[] = "0123456789abcdef";
    StringInfoData buf;
    StringInfoData res;

    initStringInfo(&buf);
    initStringInfo(&res);

    if (hash16 >= 0) {
        for (int i = 0; i < 16; ++i) {
            int ho = hash16 & 0x0F;
            hash16 = hash16 >> 4;
            appendStringInfoChar(&buf, hex_chars[ho]);
        }
        for (int j = strlen(buf.data) - 1; j >= 0; --j) {
            appendStringInfoChar(&res, buf.data[j]);
        }
    }
    PG_RETURN_CSTRING(res.data);
}

static void string_to_hash32(const char* source, hash32_t* hash32)
{
    const char* src = source;
    int len = strlen(src);
    if (len != HASH32_LEN * 2) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for hash32: \"%s\"", source)));
    }

    for (int i = 0; i < HASH32_LEN; ++i) {
        hash32->data[i] = hash_ctoa(src[i * 2]) * 16 + hash_ctoa(src[i * 2 + 1]);
    }
}

Datum hash32in(PG_FUNCTION_ARGS)
{
    char *hash32_str = PG_GETARG_CSTRING(0);
    hash32_t *hash32 = NULL;

    hash32 = (hash32_t*)palloc(sizeof(hash32_t));
    string_to_hash32(hash32_str, hash32);
    PG_RETURN_HASH32_P(hash32);
}

Datum hash32out(PG_FUNCTION_ARGS)
{
    hash32_t* hash32 = PG_GETARG_HASH32_P(0);
    static const char hex_chars[] = "0123456789abcdef";
    StringInfoData buf;
    int i;

    initStringInfo(&buf);
    for (i = 0; i < HASH32_LEN; i++) {
        int h_low;
        int h_high;

        h_low = hash32->data[i] & 0x0F;
        h_high = (hash32->data[i] >> 4) & 0x0F;

        appendStringInfoChar(&buf, hex_chars[h_high]);
        appendStringInfoChar(&buf, hex_chars[h_low]);
    }

    PG_RETURN_CSTRING(buf.data);
}

Datum hash16_add(PG_FUNCTION_ARGS)
{
    uint64 arg1 = DatumGetUInt64(PG_GETARG_DATUM(0));
    uint64 arg2 = DatumGetUInt64(PG_GETARG_DATUM(1));

    PG_RETURN_DATUM(UInt64GetDatum(arg1 + arg2));
}

Datum hash16_eq(PG_FUNCTION_ARGS)
{
    uint64 arg1 = DatumGetUInt64(PG_GETARG_DATUM(0));
    uint64 arg2 = DatumGetUInt64(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(arg1 == arg2);
}
