/* -------------------------------------------------------------------------
 *
 * genfile.c
 *		Functions for direct access to files
 *
 *
 * Copyright (c) 2004-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/genfile.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <dirent.h>

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/alarmchecker.h"
#include "postmaster/syslogger.h"
#include "storage/smgr/fd.h"
#include "storage/checksum.h"
#include "replication/basebackup.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

typedef struct {
    char* location;
    DIR* dirdesc;
} directory_fctx;

typedef struct stated_file {
    char* path;
    char* filename;
    stated_file* next;
} stated_file;

typedef struct StatFile_State {
    stated_file* cur_file;
} StatFile_State;

/*
 * Convert a "text" filename argument to C string, and check it's allowable.
 *
 * Filename may be absolute or relative to the t_thrd.proc_cxt.DataDir, but we only allow
 * absolute paths that match t_thrd.proc_cxt.DataDir or Log_directory.
 */
static char* convert_and_check_filename(text* arg)
{
    char* filename = NULL;

    filename = text_to_cstring(arg);
    canonicalize_path(filename); /* filename can change length here */

    if (is_absolute_path(filename)) {
        /* Disallow '/a/b/data/..' */
        if (path_contains_parent_reference(filename))
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("reference to parent directory (\"..\") not allowed"))));

        /*
         * Allow absolute paths if within t_thrd.proc_cxt.DataDir or Log_directory, even
         * though Log_directory might be outside t_thrd.proc_cxt.DataDir.
         */
        if (!path_is_prefix_of_path(t_thrd.proc_cxt.DataDir, filename) &&
            (!is_absolute_path(u_sess->attr.attr_common.Log_directory) ||
                !path_is_prefix_of_path(u_sess->attr.attr_common.Log_directory, filename)))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("absolute path not allowed"))));
    } else if (!path_is_relative_and_below_cwd(filename))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("path must be in or below the current directory"))));

    return filename;
}

/*
 * Read a section of a file, returning it as bytea
 *
 * Caller is responsible for all permissions checking.
 *
 * We read the whole of the file when bytes_to_read is negative.
 */
bytea* read_binary_file(const char* filename, int64 seek_offset, int64 bytes_to_read, bool missing_ok, bool need_check)
{
    bytea* buf = NULL;
    size_t nbytes;
    FILE* file = NULL;
    int64 offset = 0;
    bool isNeedCheck = false;
    int segNo = 0;
    const int MAX_RETRY_LIMIT = 60;
    int retryCnt = 0;
    errno_t rc = 0;
    UndoFileType undoFileType = UNDO_INVALID;

    if (bytes_to_read < 0) {
        if (seek_offset < 0)
            bytes_to_read = -seek_offset;
        else {
            struct stat fst;

            if (stat(filename, &fst) < 0) {
                if (missing_ok && errno == ENOENT)
                    return NULL;
                else
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", filename)));
            }

            bytes_to_read = fst.st_size - seek_offset;
        }
    }

    /* not sure why anyone thought that int64 length was a good idea */
    if ((unsigned int)(bytes_to_read) > (MaxAllocSize - VARHDRSZ))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("requested length too large")));

    if ((file = AllocateFile(filename, PG_BINARY_R)) == NULL) {
        if (missing_ok && errno == ENOENT)
            return NULL;
        else
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\" for reading: %m", filename)));
    }

    isNeedCheck = is_row_data_file(filename, &segNo, &undoFileType);
    ereport(DEBUG1, (errmsg("read_binary_file, filename is %s, isNeedCheck is %d", filename, isNeedCheck)));

    buf = (bytea*)palloc((Size)bytes_to_read + VARHDRSZ);

recheck:
    if (fseeko(file, (off_t)seek_offset, (seek_offset >= 0) ? SEEK_SET : SEEK_END) != 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek in file \"%s\": %m", filename)));

    nbytes = fread(VARDATA(buf), 1, (size_t)bytes_to_read, file);
    if (nbytes != (size_t) bytes_to_read) {
        if (ferror(file))
            ereport(ERROR,
                (errcode_for_file_access(),
                errmsg("could not read file \"%s\": %m", filename)));
    }

    if (ENABLE_INCRE_CKPT && isNeedCheck && need_check) {
        uint32 check_loc = 0;
        BlockNumber blkno = 0;
        uint16 checksum = 0;
        PageHeader phdr = NULL;
        uint32 segSize;
        GET_SEG_SIZE(undoFileType, segSize);

        if (seek_offset < 0) {
            struct stat fst;

            if (stat(filename, &fst) < 0) {
                if (missing_ok && errno == ENOENT) {
                    return NULL;
                } else {
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", filename)));
                }
            }

            offset = fst.st_size + seek_offset;
        } else {
            offset = seek_offset;
        }

        /*offset and nbytes must be integer multiple of BLCKSZ.*/
        if (offset % BLCKSZ != 0 || nbytes % BLCKSZ != 0) {
            ereport(ERROR,
               (errcode_for_file_access(),
               errmsg("file length cannot be divisibed by 8k: file %s, offset %ld, nbytes %lu",
               filename, offset, nbytes)));
        }

        for (check_loc = 0; check_loc < nbytes; check_loc += BLCKSZ) {
            blkno = offset / BLCKSZ + check_loc / BLCKSZ + (segNo * segSize);
            phdr = PageHeader((char*)VARDATA(buf) + check_loc);
            if (!CheckPageZeroCases(phdr)) {
                continue;
            }
            checksum = pg_checksum_page((char*)VARDATA(buf) + check_loc, blkno);

            if (phdr->pd_checksum != checksum) {
                rc = memset_s(buf, (Size)bytes_to_read + VARHDRSZ, 0, (Size)bytes_to_read + VARHDRSZ);
                securec_check_c(rc, "", "");
                if (retryCnt < MAX_RETRY_LIMIT) {
                    retryCnt++;
                    pg_usleep(1000000);
                    goto recheck;
                } else {
                    ereport(ERROR,
                       (errcode_for_file_access(),
                       errmsg("cheksum failed in file \"%s\"(computed: %d, recorded: %d)",
                       filename, checksum, phdr->pd_checksum)));
                }
            }
            retryCnt = 0;
        }
    }

    SET_VARSIZE(buf, nbytes + VARHDRSZ);

    FreeFile(file);

    return buf;
}

/*
 * Similar to read_binary_file, but we verify that the contents are valid
 * in the database encoding.
 */
static text* read_text_file(const char* filename, int64 seek_offset, int64 bytes_to_read)
{
    bytea* buf = NULL;

    buf = read_binary_file(filename, seek_offset, bytes_to_read, false);

    /* Make sure the input is valid */
    pg_verifymbstr(VARDATA(buf), VARSIZE(buf) - VARHDRSZ, false);

    /* OK, we can cast it to text safely */
    return (text*)buf;
}

/*
 * Read a section of a file, returning it as text
 */
Datum pg_read_file(PG_FUNCTION_ARGS)
{
    text* filename_t = PG_GETARG_TEXT_P(0);
    int64 seek_offset = PG_GETARG_INT64(1);
    int64 bytes_to_read = PG_GETARG_INT64(2);
    char* filename = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));

    filename = convert_and_check_filename(filename_t);

    if (bytes_to_read < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("requested length cannot be negative")));

    PG_RETURN_TEXT_P(read_text_file(filename, seek_offset, bytes_to_read));
}

/*
 * Read the whole of a file, returning it as text
 */
Datum pg_read_file_all(PG_FUNCTION_ARGS)
{
    text* filename_t = PG_GETARG_TEXT_P(0);
    char* filename = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));

    filename = convert_and_check_filename(filename_t);

    PG_RETURN_TEXT_P(read_text_file(filename, 0, -1));
}

/*
 * Read a section of a file, returning it as bytea
 */
Datum pg_read_binary_file(PG_FUNCTION_ARGS)
{
    text* filename_t = PG_GETARG_TEXT_P(0);
    int64 seek_offset = 0;
    int64 bytes_to_read = -1;
    bool missing_ok = false;
    char* filename = NULL;
    bytea* result = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    if (PG_NARGS() >= 3) {
        seek_offset = PG_GETARG_INT64(1);
        bytes_to_read = PG_GETARG_INT64(2);
        if (bytes_to_read < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("requested length cannot be negative")));
    }
    if (PG_NARGS() >= 4)
        missing_ok = PG_GETARG_BOOL(3);

    filename = convert_and_check_filename(filename_t);
    result = read_binary_file(filename, seek_offset, bytes_to_read, missing_ok, true);

    if (NULL != result)
        PG_RETURN_BYTEA_P(result);
    else
        PG_RETURN_NULL();
}

/*
 * Read the whole of a file, returning it as bytea
 */
Datum pg_read_binary_file_all(PG_FUNCTION_ARGS)
{
    text* filename_t = PG_GETARG_TEXT_P(0);
    char* filename = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));

    filename = convert_and_check_filename(filename_t);

    PG_RETURN_BYTEA_P(read_binary_file(filename, 0, -1, false));
}
struct CompressAddressItemState {
    uint32 blkno;
    int segmentNo;
    ReadBlockChunksStruct rbStruct;
    FILE *pcaFile;
};

static void ReadBinaryFileBlocksFirstCall(PG_FUNCTION_ARGS, int32 startBlockNum, int32 blockCount)
{
    char* path = convert_and_check_filename(PG_GETARG_TEXT_PP(0));
    int segmentNo = 0;
    UndoFileType undoFileType = UNDO_INVALID;
    if (!is_row_data_file(path, &segmentNo, &undoFileType)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s is not a relation file.", path)));
    }
    /* create a function context for cross-call persistence */
    FuncCallContext* fctx = SRF_FIRSTCALL_INIT();

    /* switch to memory context appropriate for multiple function calls */
    MemoryContext mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

    /* initialize file scanning code */
    CompressAddressItemState* itemState = (CompressAddressItemState*)palloc(sizeof(CompressAddressItemState));

    /* save mmap to inter_call_data->pcMap */
    char pcaFilePath[MAXPGPATH];
    errno_t rc = snprintf_s(pcaFilePath, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, path);
    securec_check_ss(rc, "\0", "\0");
    FILE* pcaFile = AllocateFile((const char*)pcaFilePath, "rb");
    if (pcaFile == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", pcaFilePath)));
    }
    PageCompressHeader* map = pc_mmap(fileno(pcaFile), ReadChunkSize(pcaFile, pcaFilePath, MAXPGPATH), true);
    if (map == MAP_FAILED) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Failed to mmap %s: %m", pcaFilePath)));
    }
    if ((BlockNumber)startBlockNum + (BlockNumber)blockCount > map->nblocks) {
        auto blockNum = map->nblocks;
        ReleaseMap(map, pcaFilePath);
        ereport(ERROR,
            (ERRCODE_INVALID_PARAMETER_VALUE,
                errmsg("invalid blocknum \"%d\" and block count \"%d\", the max blocknum is \"%u\"",
                    startBlockNum,
                    blockCount,
                    blockNum)));
    }
    /* construct ReadBlockChunksStruct */
    char* pcdFilePath = (char*)palloc0(MAXPGPATH);
    rc = snprintf_s(pcdFilePath, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, path);
    securec_check_ss(rc, "\0", "\0");
    FILE* fp = AllocateFile(pcdFilePath, "rb");
    if (fp == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", pcdFilePath)));
    }
    itemState->pcaFile = pcaFile;
    itemState->rbStruct.header = map;
    itemState->rbStruct.fp = fp;
    itemState->rbStruct.segmentNo = segmentNo;
    itemState->rbStruct.fileName = pcdFilePath;

    /*
     * build tupdesc for result tuples. This must match this function's
     * pg_proc entry!
     */
    TupleDesc tupdesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "path", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "blocknum", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "len", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "data", BYTEAOID, -1, 0);
    fctx->tuple_desc = BlessTupleDesc(tupdesc);

    itemState->blkno = startBlockNum;
    fctx->max_calls = blockCount;
    fctx->user_fctx = itemState;

    MemoryContextSwitchTo(mctx);
}

Datum pg_read_binary_file_blocks(PG_FUNCTION_ARGS)
{
    int32 startBlockNum = PG_GETARG_INT32(1);
    int32 blockCount = PG_GETARG_INT32(2);

    if (startBlockNum < 0 || blockCount <= 0 || startBlockNum + blockCount > RELSEG_SIZE) {
        ereport(ERROR, (ERRCODE_INVALID_PARAMETER_VALUE,
                        errmsg("invalid blocknum \"%d\" or block count \"%d\"", startBlockNum, blockCount)));
    }

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        ReadBinaryFileBlocksFirstCall(fcinfo, startBlockNum, blockCount);
    }

    /* stuff done on every call of the function */
    FuncCallContext *fctx = SRF_PERCALL_SETUP();
    CompressAddressItemState *itemState = (CompressAddressItemState *)fctx->user_fctx;

    if (fctx->call_cntr < fctx->max_calls) {
        bytea *buf = (bytea *)palloc(BLCKSZ + VARHDRSZ);
        size_t len = ReadAllChunkOfBlock(VARDATA(buf), BLCKSZ, itemState->blkno, itemState->rbStruct);
        SET_VARSIZE(buf, len + VARHDRSZ);
        Datum values[4];
        values[0] = PG_GETARG_DATUM(0);
        values[1] = Int32GetDatum(itemState->blkno);
        values[2] = Int32GetDatum(len);
        values[3] = PointerGetDatum(buf);

        /* Build and return the result tuple. */
        bool nulls[4];
        securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");
        HeapTuple tuple = heap_form_tuple(fctx->tuple_desc, (Datum*)values, (bool*)nulls);
        Datum result = HeapTupleGetDatum(tuple);
        itemState->blkno++;
        SRF_RETURN_NEXT(fctx, result);
    } else {
        if (itemState->rbStruct.header != NULL) {
            pc_munmap(itemState->rbStruct.header);
        }
        FreeFile(itemState->pcaFile);
        FreeFile(itemState->rbStruct.fp);
        SRF_RETURN_DONE(fctx);
    }
}

/*
 * stat a file
 */
Datum pg_stat_file(PG_FUNCTION_ARGS)
{
    text* filename_t = PG_GETARG_TEXT_P(0);
    char* filename = NULL;
    struct stat fst;
    Datum values[6];
    bool isnull[6];
    HeapTuple tuple;
    TupleDesc tupdesc;
    errno_t rc;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to get file information"))));

    filename = convert_and_check_filename(filename_t);
    if (stat(filename, &fst) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", filename)));

    /*
     * This record type had better match the output parameters declared for me
     * in pg_proc.h.
     */
    tupdesc = CreateTemplateTupleDesc(6, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "size", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "access", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "modification", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "change", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "creation", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "isdir", BOOLOID, -1, 0);
    BlessTupleDesc(tupdesc);

    rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
    securec_check(rc, "\0", "\0");

    values[0] = Int64GetDatum((int64)fst.st_size);
    values[1] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_atime));
    values[2] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_mtime));
    /* Unix has file status change time, while Win32 has creation time */
#if !defined(WIN32) && !defined(__CYGWIN__)
    values[3] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
    isnull[4] = true;
#else
    isnull[3] = true;
    values[4] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
#endif
    values[5] = BoolGetDatum(S_ISDIR(fst.st_mode));

    tuple = heap_form_tuple(tupdesc, values, isnull);

    pfree_ext(filename);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * List a directory (returns the filenames only)
 */
Datum pg_ls_dir(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    struct dirent* de = NULL;
    directory_fctx* fctx = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to get directory listings"))));

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        fctx = (directory_fctx*)palloc(sizeof(directory_fctx));
        fctx->location = convert_and_check_filename(PG_GETARG_TEXT_P(0));

        fctx->dirdesc = AllocateDir(fctx->location);

        if (NULL == fctx->dirdesc)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", fctx->location)));

        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    fctx = (directory_fctx*)funcctx->user_fctx;

    while ((de = ReadDir(fctx->dirdesc, fctx->location)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(de->d_name));
    }

    FreeDir(fctx->dirdesc);

    SRF_RETURN_DONE(funcctx);
}

/*
 * List directories  recursive(returns the filenames and  paths)
 */
static stated_file* pg_ls_dir_recursive(const char* location, stated_file* sd_file)
{
    struct dirent* de = NULL;
    DIR* dirdesc = NULL;
    errno_t rc = 0;
    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to get directory listings"))));

    dirdesc = AllocateDir(location);

    while ((de = ReadDir(dirdesc, location)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        int maxPathLen = strlen(location) + strlen(de->d_name) + 2;
        sd_file->path = (char*)palloc0(maxPathLen);

        int filenameLen = strlen(de->d_name) + 1;
        sd_file->filename = (char*)palloc0(filenameLen);

        char* tmp_dir = (char*)palloc0(maxPathLen);

        if (*location != '.') {
            rc = strcpy_s(sd_file->path, maxPathLen, location);
            securec_check(rc, "\0", "\0");
            rc = snprintf_s(tmp_dir, maxPathLen, maxPathLen - 1, "%s/", sd_file->path);
            securec_check_ss(rc, "\0", "\0");
        }

        rc = strcpy_s(sd_file->filename, filenameLen, de->d_name);
        securec_check(rc, "\0", "\0");
        rc = strcat_s(tmp_dir, maxPathLen, sd_file->filename);
        securec_check(rc, "\0", "\0");

        struct stated_file* next_file = (stated_file*)palloc0(sizeof(stated_file));

        sd_file->next = next_file;
        sd_file = next_file;
        if (isDirExist(tmp_dir)) {
            ereport(DEBUG3, (errmsg("current path %s.", tmp_dir)));
            sd_file = pg_ls_dir_recursive(tmp_dir, sd_file);
        }
    }
    FreeDir(dirdesc);
    dirdesc = NULL;
    return sd_file;
}

/*
 * stat a few  file
 */
Datum pg_stat_file_recursive(PG_FUNCTION_ARGS)
{
    struct stat fst;
    struct stated_file* sd_file = NULL;
    Datum values[4];
    bool isnull[4];
    HeapTuple tuple;
    TupleDesc tupdesc;
    errno_t rc = 0;

    FuncCallContext* funcctx = NULL;
    StatFile_State* status = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to get file information"))));

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        directory_fctx* fctx = (directory_fctx*)palloc(sizeof(directory_fctx));
        fctx->location = convert_and_check_filename(PG_GETARG_TEXT_P(0));
        fctx->dirdesc = AllocateDir(fctx->location);

        sd_file = (stated_file*)palloc0(sizeof(stated_file));
        status = (StatFile_State*)palloc0(sizeof(StatFile_State));
        status->cur_file = sd_file;
        funcctx->user_fctx = (void*)status;

        /*
         * This record type had better match the output parameters declared for me
         * in pg_proc.h.
         */
        tupdesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "path", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "filename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "size", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "isdir", BOOLOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        (void)pg_ls_dir_recursive(fctx->location, sd_file);

        if (NULL == fctx->dirdesc) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", fctx->location)));
        }

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status = (StatFile_State*)funcctx->user_fctx;

    while (status->cur_file != NULL) {
        /*Only for the last cur_file, palloced but not assigned yet.*/
        if (status->cur_file->path == NULL || status->cur_file->filename == NULL) {
            status->cur_file = status->cur_file->next;
            continue;
        }

        int pathLen = strlen(status->cur_file->path) + strlen(status->cur_file->filename) + 2;
        int buf_used = 0;
        if (*(status->cur_file->path) != '\0') {
            rc = strcat_s(status->cur_file->path, pathLen, "/");
            buf_used++;
            securec_check(rc, "\0", "\0");
        }
        rc = strcat_s(status->cur_file->path, pathLen, status->cur_file->filename);
        buf_used += strlen(status->cur_file->filename);
        securec_check(rc, "\0", "\0");

        if (*(status->cur_file->path) == '\0') {
            status->cur_file = status->cur_file->next;
            continue;
        }
        if (stat(status->cur_file->path, &fst) < 0) {
            /* If the file is not exist, maybe it's deleted, we will skip it. */
            if (errno == ENOENT) {
                status->cur_file = status->cur_file->next;
                continue;
            } else
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", status->cur_file->path)));
        }

        // skip big file
        if (fst.st_size > MAX_TAR_MEMBER_FILELEN) {
            ereport(WARNING,
                (errcode_for_file_access(),
                    errmsg("archive member \"%s\" too large for tar format", status->cur_file->path)));
            status->cur_file = status->cur_file->next;
            continue;
        }

        rc = memset_s(isnull, sizeof(isnull), false, sizeof(isnull));
        securec_check(rc, "\0", "\0");
        values[0] = CStringGetTextDatum(status->cur_file->path);
        values[1] = CStringGetTextDatum(status->cur_file->filename);
        values[2] = Int64GetDatum((int64)fst.st_size);
        values[3] = BoolGetDatum(S_ISDIR(fst.st_mode));

        status->cur_file = status->cur_file->next;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, isnull);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
