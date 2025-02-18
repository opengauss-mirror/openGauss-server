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
 * bfile.cpp
 *  Implementation of BFile Data Type
 *
 *
 * IDENTIFICATION
 *        src/backend/utils/adt/bfile.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/hash.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "mb/pg_wchar.h"
#include "fmgr.h"
#include "storage/ipc.h"
#include "executor/spi.h"
#include "commands/extension.h"
#include "utils/bfile.h"

constexpr int INVALID_SLOTID = 0;   /* invalid slot id */
constexpr int ADD_BFILENAMESTR_LEN = 20;

Datum bfilein(PG_FUNCTION_ARGS)
{
    char *input = PG_GETARG_CSTRING(0);
    int inputLen = 0;
    int bfileTotalSize = 0;
    int bfileFixedSize = 0;
    char *location = NULL;
    char *locationRaw = NULL;
    char *filename = NULL;
    char *filenameRaw = NULL;
    BFile *result = NULL;
    int locationDataLen = 0;
    int filenameDataLen = 0;
    char *start = NULL;
    char *end = NULL;
    errno_t rc;

    if (unlikely(input == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfilein")));
    }

    bfileFixedSize = sizeof(BFile);

    if (strncasecmp(input, "bfilename(NULL)", strlen("bfilename(NULL)")) == 0) {
        result = (BFile *)palloc0(bfileFixedSize);
        result->slot_id = INVALID_SLOTID;
        result->location_len = locationDataLen;
        result->filename_len = filenameDataLen;
        SET_VARSIZE(result, bfileFixedSize);
        PG_RETURN_POINTER(result);
    }

    inputLen = strlen(input);
    location = static_cast<char*>(palloc0(inputLen));
    locationRaw = static_cast<char*>(palloc0(inputLen));
    filename = static_cast<char*>(palloc0(inputLen));
    filenameRaw = static_cast<char*>(palloc0(inputLen));

    /* trim space */
    start = input;
    while (*start && isspace(*start)) {
        start++;
    }

    if (*start == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    /* should have bfilename */
    if (strncasecmp(start, "bfilename", strlen("bfilename")) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    /* skip bfilename */
    start += strlen("bfilename");

    /* trim space after bfilename */
    while (*start && isspace(*start)) {
        start++;
    }
    
    /* after bfilename must be ( */
    if (*start != '(') {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    start++;

    /* after ( before , is location dir */
    end = start;
    while (*end && *end != ',') {
        end++;
    }

    if (*end != ',') {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    /* copy data after ( before , */
    rc = memcpy_s(locationRaw, inputLen, start, (size_t)(end - start));
    securec_check(rc, "\0", "\0");
    locationRaw[end - start] = 0;

    start = end + 1;
    end = start;

    /* after ,before ) is file name */
    while (*end && *end != ')') {
        end++;
    }

    if (*end == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    /* copy data after , before ) */
    rc = memcpy_s(filenameRaw, inputLen, start, (size_t)(end - start));
    securec_check(rc, "\0", "\0");
    filenameRaw[end - start] = 0;

    /* trim space and ' around locationRaw */
    start = locationRaw;
    while (*start && (isspace(*start) || *start == '\'')) {
        start++;
    }

    if (*start == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    end = start + strlen(start) - 1;
    while (*end && end >= start && (isspace(*end) || *end == '\'')) {
        end--;
    }

    if (end < start) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    end++;

    rc = memcpy_s(location, inputLen, start, (end - start));
    securec_check(rc, "\0", "\0");
    location[end - start] = 0;

    /* trim space and ' around filenameRaw */
    start = filenameRaw;
    while (*start && (isspace(*start) || *start == '\'')) {
        start++;
    }

    if (*start == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    end = start + strlen(start) - 1;
    while (*end && end >= start && (isspace(*end) || *end == '\'')) {
        end--;
    }
    
    if (end < start) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for bfile: \"%s\"", input)));
        PG_RETURN_POINTER(result);
    }

    end++;

    rc = memcpy_s(filename, inputLen, start, (end - start));
    securec_check(rc, "\0", "\0");
    filename[end - start] = 0;

    locationDataLen = strlen(location) + 1;
    filenameDataLen = strlen(filename) + 1;

    bfileTotalSize = locationDataLen + filenameDataLen + bfileFixedSize;
    result = (BFile *)palloc0(bfileTotalSize);
    result->slot_id = INVALID_SLOTID;
    result->location_len = locationDataLen;
    result->filename_len = filenameDataLen;
    rc = memcpy_s(result->data, locationDataLen, location, locationDataLen);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(result->data + locationDataLen, filenameDataLen, filename, filenameDataLen);
    securec_check(rc, "\0", "\0");
    SET_VARSIZE(result, bfileTotalSize);

    pfree(location);
    pfree(filename);
    pfree(locationRaw);
    pfree(filenameRaw);
    PG_RETURN_POINTER(result);
}

Datum bfileout(PG_FUNCTION_ARGS)
{
    BFile *bfile = (BFile *)PG_GETARG_POINTER(0);
    char *result = NULL;
    int maxResultLen = 0;
    int rc = 0;

    if (unlikely(bfile == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfileout")));
    }

    maxResultLen = VARSIZE_ANY(bfile) + ADD_BFILENAMESTR_LEN;
    result = static_cast<char*>palloc0(maxResultLen);

    if ((bfile->location_len == 0)  || (bfile->filename_len == 0)) {
        rc = snprintf_s(result, maxResultLen, maxResultLen - 1, "bfilename(NULL)");
    } else {
        rc = snprintf_s(result, maxResultLen, maxResultLen - 1, "bfilename('%s', '%s')", bfile->data,
                        (bfile->data + bfile->location_len));
    }
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_CSTRING(result);
}

Datum bfilerecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    BFile* result = NULL;
    char *location = NULL;
    char *filename = NULL;
    errno_t rc;

    if (unlikely(buf == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfilerecv")));
    }

    result = (BFile *)palloc0(buf->len + ADD_BFILENAMESTR_LEN);
    result->slot_id = INVALID_SLOTID;
    result->location_len = (int)pq_getmsgint(buf, sizeof(int));
    result->filename_len = (int)pq_getmsgint(buf, sizeof(int));
    location = (char *)pq_getmsgstring(buf);
    filename = (char *)pq_getmsgstring(buf);
    rc = memcpy_s(result->data, result->location_len, location, result->location_len);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(result->data + result->location_len, result->filename_len, filename, result->filename_len);
    securec_check(rc, "\0", "\0");

    PG_RETURN_POINTER(result);
}

Datum bfilesend(PG_FUNCTION_ARGS)
{
    BFile *bfile = (BFile *)PG_GETARG_POINTER(0);
    StringInfoData buf;

    if (unlikely(bfile == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfilesend")));
    }

    pq_begintypsend(&buf);
    pq_sendint32(&buf, bfile->location_len);
    pq_sendint32(&buf, bfile->filename_len);
    pq_sendstring(&buf, bfile->data);
    pq_sendstring(&buf, (bfile->data + bfile->location_len));
    PG_RETURN_POINTER(pq_endtypsend(&buf));
}

Datum bfilename(PG_FUNCTION_ARGS)
{
    int bfileFixedSize = 0;
    int bfileTotalSize = 0;
    int locationStrLen = 0;
    int filenameStrLen = 0;
    BFile *result = NULL;
    text *location = NULL;
    text *filename = NULL;
    char *locationStr = NULL;
    char *filenameStr = NULL;
    errno_t rc;

    location = (text *)PG_GETARG_POINTER(0);
    filename = (text *)PG_GETARG_POINTER(1);

    bfileFixedSize = sizeof(BFile);

    if (!location || !filename) {
        result = (BFile *)palloc0(bfileFixedSize);
        result->slot_id = INVALID_SLOTID;
        result->location_len = locationStrLen;
        result->filename_len = filenameStrLen;
        SET_VARSIZE(result, bfileFixedSize);
        PG_RETURN_POINTER(result);
    }

    locationStr = text_to_cstring(location);
    filenameStr = text_to_cstring(filename);
    locationStrLen = strlen(locationStr) + 1;
    filenameStrLen = strlen(filenameStr) + 1;

    bfileTotalSize = locationStrLen + filenameStrLen + bfileFixedSize;
    result = (BFile *)palloc0(bfileTotalSize);
    result->slot_id = INVALID_SLOTID;
    result->location_len = locationStrLen;
    result->filename_len = filenameStrLen;
    rc = memcpy_s(result->data, result->location_len, locationStr, result->location_len);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s((result->data + result->location_len), result->filename_len, filenameStr, result->filename_len);
    securec_check(rc, "\0", "\0");
    SET_VARSIZE(result, bfileTotalSize);

    pfree(locationStr);
    pfree(filenameStr);
    PG_RETURN_POINTER(result);
}
