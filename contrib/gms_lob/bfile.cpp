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
 *        contrib/gms_lob/bfile.cpp
 * 
 * --------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/hash.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/bfile.h"
#include "knl/knl_session.h"
#include "mb/pg_wchar.h"
#include "fmgr.h"
#include "storage/ipc.h"
#include "executor/spi.h"
#include "commands/extension.h"
#include "gms_lob.h"
#include "bfile.h"

PG_FUNCTION_INFO_V1(bfileopen);
PG_FUNCTION_INFO_V1(bfileclose);
PG_FUNCTION_INFO_V1(getlength);
PG_FUNCTION_INFO_V1(bfileread);

#define CUSTOM_EXCEPTION(msg, detail) \
    ereport(ERROR, \
    (errcode(ERRCODE_RAISE_EXCEPTION), \
    errmsg("%s", msg), \
    errdetail("%s", detail)))

#define STRERROR_EXCEPTION(msg) \
do { char *strerr = strerror(errno); CUSTOM_EXCEPTION(msg, strerr); } while (0);

#define INVALID_SLOTID  0        /* invalid slot id */
#define OPEN_READONLY    0

static void IO_EXCEPTION(void)
{
    switch (errno)
    {
    case EACCES:
    case ENAMETOOLONG:
    case ENOENT:
    case ENOTDIR:
        STRERROR_EXCEPTION("INVALID_PATH");
        break;

    default:
        STRERROR_EXCEPTION("INVALID_OPERATION");
    }
}

static int get_descriptor(FILE *file, int max_linesize, int encoding)
{
    int i;

    if (unlikely(file == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for get_descriptor")));
    }

    for (i = 0; i < MAX_SLOTS; i++)
    {
        if (get_session_context()->slots[i].id == INVALID_SLOTID)
        {
            get_session_context()->slots[i].id = ++get_session_context()->slotid;
            get_session_context()->slots[i].file = file;
            get_session_context()->slots[i].max_linesize = max_linesize;
            get_session_context()->slots[i].encoding = encoding;
            return get_session_context()->slots[i].id;
        }
    }

    return INVALID_SLOTID;
}

static FILE* get_file_handle(int d)
{
    int i;

    if (d == INVALID_SLOTID)
        CUSTOM_EXCEPTION("INVALID_FILEHANDLE", "File handle isn't valid.");

    for (i = 0; i < MAX_SLOTS; i++)
    {
        if (get_session_context()->slots[i].id == d)
        {
            return get_session_context()->slots[i].file;
        }
    }

    CUSTOM_EXCEPTION("INVALID_FILEHANDLE", "File handle isn't valid.");
    return NULL;    /* keep compiler quiet */
}

static char* get_dir_path(char *dirname)
{
    MemoryContext old_cxt;
    StringInfoData buf;
    int ret;
    char *dirpath = NULL;
    char *result = NULL;

    if (unlikely(dirname == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for get_dir_path")));
    }

    old_cxt = CurrentMemoryContext;

    if (SPI_connect() < 0)
        ereport(ERROR,
        (errcode(ERRCODE_INTERNAL_ERROR),
        errmsg("SPI_connect failed")));

    initStringInfo(&buf);
    appendStringInfo(&buf, " select dirpath from pg_directory where dirname = lower('%s');", dirname);

    ret = SPI_exec(buf.data, 0);
    pfree(buf.data);

    if ((ret == SPI_OK_SELECT) && (SPI_processed > 1))
        ereport(
        ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("dirname matched more than one dirpath")));
    else if (ret == SPI_OK_SELECT && SPI_processed == 1) {
        dirpath = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        result = MemoryContextStrdup(old_cxt, dirpath);
    }
    SPI_finish();

    if (!result)
        ereport(
        ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("dirname can not match any dirpath")));

    MemoryContextSwitchTo(old_cxt);

    return result;
}

static void bfile_fclose_all(int code, Datum arg)
{
    int i;
    GmsLobContext* psc = get_session_context();
    for (i = 0; i < MAX_SLOTS; i++)
    {
        if (psc->slots[i].id != INVALID_SLOTID)
        {
            if (psc->slots[i].file && fclose(psc->slots[i].file) != 0)
            {
                if (errno == EBADF)
                    ereport(WARNING, (errcode(ERRCODE_RAISE_EXCEPTION), errmsg("INVALID_FILEHANDLE"), errdetail("File is not an opened")));
                else
                    ereport(WARNING, (errmsg("WRITE_ERROR")));
            }
            psc->slots[i].file = NULL;
            psc->slots[i].id = INVALID_SLOTID;
        }
    }
}

Datum bfileopen(PG_FUNCTION_ARGS)
{
    BFile *source = NULL;
    BFile *result = NULL;
    int len = 0;
    char *fullname = NULL;
    int d = 0;
    int open_mode;
    FILE *file = NULL;
    char *dirpath = NULL;
    errno_t rc;

    source = (BFile *)PG_GETARG_POINTER(0);
    open_mode = (int)PG_GETARG_INT32(1);

    if (unlikely(source == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfileopen")));
    }

    if (open_mode != OPEN_READONLY) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("The file is only allowed to be read")));
    }

    len = VARSIZE_ANY(source);

    /* If dirpath is NULL, get_dir_path will throw exeception. */
    dirpath = get_dir_path(source->data);

    fullname = (char *)palloc0(strlen(dirpath) + 1 + source->filename_len);
    rc = strcpy_s(fullname, strlen(dirpath) + 1, dirpath);
    securec_check(rc, "\0", "\0");
    fullname[strlen(dirpath)] = '/';
    rc = memcpy_s(fullname + strlen(dirpath) + 1, source->filename_len,
                    source->data + source->location_len, source->filename_len);
    securec_check(rc, "\0", "\0");
    pfree(dirpath);
    canonicalize_path(fullname);

    file = fopen((const char *)fullname, (const char *) "r");
    if (!file) {
        pfree(fullname);
        IO_EXCEPTION();
    }
    
    if (get_session_context()->slotid == INVALID_SLOTID) {
        if (IS_THREAD_POOL_SESSION) {
            u_sess->ext_fdw_ctx[BFILE_TYPE_FDW].fdwExitFunc = bfile_fclose_all;
        } else {
            on_proc_exit(bfile_fclose_all, 0);
        }
    }

    pfree(fullname);
    d = get_descriptor(file, -1, 0);
    if (d == INVALID_SLOTID)
    {
        fclose(file);
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("program limit exceeded"),
            errdetail("Too much concurent opened files"),
            errhint("You can only open a maximum of fifty files for each session")));
    }

    result = (BFile  *)palloc(len);
    rc = memcpy_s(result, len, source, len);
    securec_check(rc, "\0", "\0");
    result->slot_id = d;

    PG_RETURN_POINTER(result);
}

Datum bfileclose(PG_FUNCTION_ARGS)
{
    BFile *source = NULL;
    int d = 0;

    source = (BFile *)PG_GETARG_POINTER(0);

    if (unlikely(source == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfileclose")));
    }

    d = source->slot_id;

    for (int i = 0; i < MAX_SLOTS; i++)
    {
        if (get_session_context()->slots[i].id == d)
        {
            if (get_session_context()->slots[i].file && fclose(get_session_context()->slots[i].file) != 0)
            {
                if (errno == EBADF)
                    CUSTOM_EXCEPTION("INVALID_FILEHANDLE", "File is not an opened");
                else
                    STRERROR_EXCEPTION("WRITE_ERROR");
            }
            get_session_context()->slots[i].file = NULL;
            get_session_context()->slots[i].id = INVALID_SLOTID;
            PG_RETURN_VOID();
        }
    }

    CUSTOM_EXCEPTION("INVALID_FILEHANDLE", "Used file handle isn't valid.");
    PG_RETURN_VOID();
}

Datum getlength(PG_FUNCTION_ARGS)
{
    BFile *source = NULL;
    char *fullname = NULL;
    struct stat    st;
    char *dirpath = NULL;
    errno_t rc;

    source = (BFile *)PG_GETARG_POINTER(0);

    if (unlikely(source == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for getlength")));
    }

    /* If dirpath is NULL, get_dir_path will throw exeception. */
    dirpath = get_dir_path(source->data);

    fullname = (char *)palloc0(strlen(dirpath) + 1 + source->filename_len);
    rc = strcpy_s(fullname, strlen(dirpath) + 1, dirpath);
    securec_check(rc, "\0", "\0");
    fullname[strlen(dirpath)] = '/';
    rc = memcpy_s(fullname + strlen(dirpath) + 1, source->filename_len,
                    source->data + source->location_len, source->filename_len);
    securec_check(rc, "\0", "\0");
    pfree(dirpath);
    canonicalize_path(fullname);

    if (stat(fullname, &st) != 0) {
        pfree(fullname);
        IO_EXCEPTION();
    }

    pfree(fullname);
    PG_RETURN_INT32(st.st_size);
}

Datum bfileread(PG_FUNCTION_ARGS)
{
    BFile *source = NULL;
    bytea *result = NULL;
    int d = 0;
    FILE *current_file = NULL;
    int32 amount = 0;
    int32 offset = 0;
    size_t read_len = 0;

    source = (BFile *)PG_GETARG_POINTER(0);

    if (unlikely(source == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("invalid null input for bfileread")));
    }

    amount = PG_GETARG_INT32(1);
    offset = PG_GETARG_INT32(2);

    d = source->slot_id;
    current_file = get_file_handle(d);

    if (amount < 1) {
        CUSTOM_EXCEPTION("INVALID_ARG", "arg amount is invalid, amount at lease is 1");
    }

    if (amount > 1024 * 1024) {
        CUSTOM_EXCEPTION("INVALID_ARG", "arg amount is invalid, amount should not exceed 1M");
    }

    if (offset < 1) {
        CUSTOM_EXCEPTION("INVALID_ARG", "arg offset is invalid, offset value at least is 1");
    }

    if (fseek(current_file, offset - 1, SEEK_SET) != 0)
        IO_EXCEPTION();

    result = (bytea *)palloc0(amount + VARHDRSZ);
    read_len = fread((void *)result->vl_dat, 1, amount, current_file);
    if (read_len < 0) {
        pfree(result);
        IO_EXCEPTION();
    }

    SET_VARSIZE(result, read_len + VARHDRSZ);
    PG_RETURN_POINTER(result);
}