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
#include "commands/tablespace.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/alarmchecker.h"
#include "postmaster/syslogger.h"
#include "storage/smgr/fd.h"
#include "storage/cfs/cfs_tools.h"
#include "storage/checksum.h"
#include "replication/basebackup.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "catalog/pg_partition_fn.h"
#include "storage/cfs/cfs_buffers.h"
#include "storage/file/fio_device.h"

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

#include "storage/cfs/cfs.h"
#include "storage/cfs/cfs_converter.h"

typedef struct {
    FILE* fd;
    BlockNumber maxExtent;
    BlockNumber extentCount;
    BlockNumber blockNumber;
    char extentHeaderChar[BLCKSZ];
} CompressAddrState;

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

static bool IsCheckFileBlank(FILE* file, const char* filename)
{
    int rc = fseeko(file, 0L, SEEK_END);
    if (rc != 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek in file \"%s\": %m", filename)));
    }

    return (ftell(file) == 0);
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
    const int MAX_RETRY_LIMITS = 60;
    int retryCnt = 0;
    errno_t rc = 0;
    UndoFileType undoFileType = UNDO_INVALID;
    bool isCompressed = IsCompressedFile(filename, strlen(filename));

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

    /* do not need to check if table is compressed table file */
    isNeedCheck = is_row_data_file(filename, &segNo, &undoFileType) && !isCompressed;
    ereport(DEBUG1, (errmsg("read_binary_file, filename is %s, isNeedCheck is %d", filename, isNeedCheck)));

    buf = (bytea*)palloc((Size)bytes_to_read + VARHDRSZ);
    if (isCompressed && IsCheckFileBlank(file, filename)) {
        rc = memset_s(VARDATA(buf), bytes_to_read, 0, bytes_to_read);
        securec_check_c(rc, "", "");

        SET_VARSIZE(buf, bytes_to_read + VARHDRSZ);
        FreeFile(file);
        return buf;
    }

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
                if (retryCnt < MAX_RETRY_LIMITS) {
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
    off_t fileLen;
    CfsHeaderMap rbStruct;
    FILE *compressedFd;
};

static void ReadBinaryFileBlocksFirstCall(PG_FUNCTION_ARGS, int32 startBlockNum, int32 blockCount)
{
    // todo compatibility of cfs
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
    itemState->fileLen = 0;
    FILE *compressedFd = AllocateFile((const char *)path, "rb");
    auto blockNumber = ReadBlockNumberOfCFile(compressedFd, &itemState->fileLen);
    if (blockNumber >= MIN_COMPRESS_ERROR_RT) {
        ereport(ERROR, (ERRCODE_INVALID_PARAMETER_VALUE,
                        errmsg("can not read actual block from %s, error code: %lu,", path, blockNumber)));
    }

    if ((BlockNumber)startBlockNum + (BlockNumber)blockCount > blockNumber) {
        (void)FreeFile(compressedFd);
        ereport(ERROR,
            (ERRCODE_INVALID_PARAMETER_VALUE,
                errmsg("invalid blocknum \"%d\" and block count \"%d\", the max blocknum is \"%lu\"",
                    startBlockNum,
                    blockCount,
                    blockNumber)));
    }
    /* construct ReadBlockChunksStruct */
    itemState->rbStruct.header = NULL;
    itemState->rbStruct.extentCount = 0;
    itemState->rbStruct.pointer = NULL;
    itemState->rbStruct.mmapLen = 0;
    itemState->compressedFd = compressedFd;
    itemState->segmentNo = segmentNo;

    /*
     * build tupdesc for result tuples. This must match this function's
     * pg_proc entry!
     */
    TupleDesc tupdesc = CreateTemplateTupleDesc(6, false, TableAmHeap);
    int i = 1;
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "path", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "blocknum", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "len", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "algorithm", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "chunksize", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "data", BYTEAOID, -1, 0);
    fctx->tuple_desc = BlessTupleDesc(tupdesc);

    itemState->blkno = (uint32)startBlockNum;
    fctx->max_calls = (uint32)blockCount;
    fctx->user_fctx = itemState;

    (void)MemoryContextSwitchTo(mctx);
}

static RelFileNode GetCompressRelFileNode(Oid oid)
{
    HeapTuple tuple;
    Form_pg_partition pgPartitionForm;
    RelFileNode rnode;

    rnode.spcNode = InvalidOid;
    rnode.dbNode = InvalidOid;
    rnode.relNode = InvalidOid;
    rnode.bucketNode = InvalidBktId;
    rnode.opt = 0;

    // try to get origin oid
    Relation relation = try_relation_open(oid, AccessShareLock);
    if (RelationIsValid(relation)) {
        rnode = relation->rd_node;
        relation_close(relation, AccessShareLock);
        return rnode;
    }

    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(oid));
    if (!HeapTupleIsValid(tuple)) {
        return rnode;
    }

    pgPartitionForm = (Form_pg_partition)GETSTRUCT(tuple);
    switch (pgPartitionForm->parttype) {
        case PARTTYPE_PARTITIONED_RELATION:
        case PARTTYPE_VALUE_PARTITIONED_RELATION:
        case PARTTYPE_SUBPARTITIONED_RELATION:
            rnode.spcNode = ConvertToRelfilenodeTblspcOid(pgPartitionForm->reltablespace);
            if (pgPartitionForm->relfilenode) {
                rnode.relNode = pgPartitionForm->relfilenode;
            } else {
                rnode.relNode = RelationMapOidToFilenode(oid, false);
            }
            if (rnode.spcNode == GLOBALTABLESPACE_OID) {
                rnode.dbNode = InvalidOid;
            } else {
                rnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
            }
            rnode.bucketNode = InvalidBktId;
            break;
        default:
            break;
    }

    ReleaseSysCache(tuple);
    return rnode;
}

static CompressAddrState* CompressAddressInit(PG_FUNCTION_ARGS)
{
    Oid old = PG_GETARG_OID(0);
    int64 segmentNo = PG_GETARG_INT64(1);
    RelFileNode relFileNode;

    relFileNode = GetCompressRelFileNode(old);
    if (!OidIsValid(relFileNode.relNode)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("can not find table ")));
    }
    char *path = relpathbackend(relFileNode, InvalidBackendId, MAIN_FORKNUM);

    char pcaFilePath[MAXPGPATH];
    if (segmentNo == 0) {
        errno_t rc = snprintf_s(pcaFilePath, MAXPGPATH, MAXPGPATH - 1, COMPRESS_SUFFIX, path);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = snprintf_s(pcaFilePath, MAXPGPATH, MAXPGPATH - 1, "%s.%d%s", path, segmentNo, COMPRESS_STR);
        securec_check_ss(rc, "\0", "\0");
    }
    pfree(path);
    FILE* pcaFile = AllocateFile((const char*)pcaFilePath, "rb");
    if (pcaFile == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("compress File: can not find file")));
    }
    struct stat fileStat;
    if (fstat(fileno(pcaFile), &fileStat) != 0) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("fileStat: can not find file")));
    }
    auto compressAddressState = (CompressAddrState*)palloc(sizeof(CompressAddrState));
    compressAddressState->fd = pcaFile;
    auto maxExtent = (fileStat.st_size / BLCKSZ) / CFS_EXTENT_SIZE;
    compressAddressState->maxExtent = (BlockNumber)maxExtent;
    compressAddressState->extentCount= 0;
    compressAddressState->blockNumber= 0;

    return compressAddressState;
}

Datum compress_address_details(PG_FUNCTION_ARGS)
{
    if (SRF_IS_FIRSTCALL()) {
        int i = 1;
        FuncCallContext* fctx = SRF_FIRSTCALL_INIT();
        MemoryContext mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

        TupleDesc tupdesc = CreateTemplateTupleDesc(6, false, TableAmHeap);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "extent", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "extent_block_number", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "block_number", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "alocated_chunks", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "nchunks", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "chunknos", INT4ARRAYOID, -1, 0);
        fctx->tuple_desc = BlessTupleDesc(tupdesc);

        CompressAddrState *pState = CompressAddressInit(fcinfo);
        fctx->user_fctx = pState;
        fctx->max_calls = pState->maxExtent;

        (void)MemoryContextSwitchTo(mctx);
    }

    /* stuff done on every call of the function */
    FuncCallContext *fctx = SRF_PERCALL_SETUP();
    auto itemState = (CompressAddrState *)fctx->user_fctx;

    if (itemState->extentCount < itemState->maxExtent) {
        CfsExtentHeader* extentHeader = NULL;
        if (itemState->blockNumber == 0) {
            if (fseeko(itemState->fd, (((itemState->extentCount + 1) * CFS_EXTENT_SIZE) - 1) * BLCKSZ, SEEK_SET) != 0) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                errmsg("fseeko: can not seek file")));
            }
            if (fread(itemState->extentHeaderChar, 1, BLCKSZ, itemState->fd) == BLCKSZ) {
                extentHeader = (CfsExtentHeader*)itemState->extentHeaderChar;
            } else {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                errmsg("fread: can not read file")));
            }
        } else {
            extentHeader = (CfsExtentHeader*)itemState->extentHeaderChar;
        }
        if (extentHeader->nblocks == 0) {
            (void)FreeFile(itemState->fd);
            SRF_RETURN_DONE(fctx);
        }
        CfsExtentAddress *cfsExtentAddress = GetExtentAddress(extentHeader, (uint16)itemState->blockNumber);
        auto allocatedChunk = cfsExtentAddress->allocated_chunks;

        Datum values[6];
        values[0] = Int64GetDatum(itemState->extentCount);
        values[1] = Int64GetDatum(itemState->blockNumber);
        values[2] = Int64GetDatum(PG_GETARG_INT64(1) * CFS_LOGIC_BLOCKS_PER_FILE + itemState->extentCount *
                                  CFS_LOGIC_BLOCKS_PER_EXTENT + itemState->blockNumber);
        values[3] = Int32GetDatum(allocatedChunk);
        values[4] = Int32GetDatum(cfsExtentAddress->nchunks);

        Datum* chunknos = (Datum*)MemoryContextAlloc(fctx->multi_call_memory_ctx, (allocatedChunk) * sizeof(Datum));
        for (int i = 0; i < allocatedChunk; i++) {
            chunknos[i] = Int32GetDatum(cfsExtentAddress->chunknos[i]);
        }
        values[5] = PointerGetDatum(construct_array(chunknos, allocatedChunk, INT4OID, sizeof(int32), true, 'i'));

         /* Build and return the result tuple. */
        bool nulls[6];
        securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");
        HeapTuple tuple = heap_form_tuple(fctx->tuple_desc, (Datum*)values, (bool*)nulls);
        Datum result = HeapTupleGetDatum(tuple);
        if (itemState->blockNumber >= extentHeader->nblocks - 1) {
            itemState->extentCount++;
            itemState->blockNumber = 0;
        } else {
            itemState->blockNumber++;
        }
        SRF_RETURN_NEXT(fctx, result);
    } else {
        (void)FreeFile(itemState->fd);
        SRF_RETURN_DONE(fctx);
    }
}

Datum compress_address_header(PG_FUNCTION_ARGS)
{
    if (SRF_IS_FIRSTCALL()) {
        int i = 1;
        FuncCallContext* fctx = SRF_FIRSTCALL_INIT();
        MemoryContext mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

        TupleDesc tupdesc = CreateTemplateTupleDesc(5, false, TableAmHeap);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "extent", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "nblocks", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "alocated_chunks", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "chunk_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "algorithm", INT8OID, -1, 0);
        fctx->tuple_desc = BlessTupleDesc(tupdesc);

        CompressAddrState *pState = CompressAddressInit(fcinfo);
        fctx->user_fctx = pState;
        fctx->max_calls = pState->maxExtent;

        (void)MemoryContextSwitchTo(mctx);
    }

    /* stuff done on every call of the function */
    FuncCallContext *fctx = SRF_PERCALL_SETUP();
    auto itemState = (CompressAddrState *)fctx->user_fctx;

    if (fctx->call_cntr < fctx->max_calls) {
        CfsExtentHeader* extentHeader = NULL;
        if (fseeko(itemState->fd, (((itemState->extentCount + 1) * CFS_EXTENT_SIZE) - 1) * BLCKSZ, SEEK_SET) != 0) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("fseeko: can not seek file")));
        }
        if (fread(itemState->extentHeaderChar, 1, BLCKSZ, itemState->fd) == BLCKSZ) {
            extentHeader = (CfsExtentHeader*)itemState->extentHeaderChar;
        } else {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("fread: can not read file")));
        }

        Datum values[5];
        values[0] = Int64GetDatum(fctx->call_cntr);
        values[1] = Int64GetDatum(extentHeader->nblocks);
        values[2] = Int32GetDatum(extentHeader->allocated_chunks);
        values[3] = Int32GetDatum(extentHeader->chunk_size);
        values[4] = Int64GetDatum((int)extentHeader->algorithm);

         /* Build and return the result tuple. */
        bool nulls[5];
        securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");
        HeapTuple tuple = heap_form_tuple(fctx->tuple_desc, (Datum*)values, (bool*)nulls);
        Datum result = HeapTupleGetDatum(tuple);
        itemState->extentCount++;
        SRF_RETURN_NEXT(fctx, result);
    } else {
        (void)FreeFile(itemState->fd);
        SRF_RETURN_DONE(fctx);
    }
}

#define PCA_BUFFERS_STAT_INFO_COLS (4)
Datum compress_buffer_stat_info(PG_FUNCTION_ARGS)
{
    int i = 1;
    TupleDesc tupdesc = CreateTemplateTupleDesc(PCA_BUFFERS_STAT_INFO_COLS, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "ctrl_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "main_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "free_cnt", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "recycle_times", INT8OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    uint64 main_cnt = 0;
    uint64 free_cnt = 0;
    for (int j = 0; j < PCA_PART_LIST_NUM; j++) {
        main_cnt += g_pca_buf_ctx->main_lru[j].count;
        free_cnt += g_pca_buf_ctx->free_lru[j].count;
    }

    Datum values[PCA_BUFFERS_STAT_INFO_COLS];
    values[0] = Int64GetDatum(g_pca_buf_ctx->max_count);
    values[1] = Int64GetDatum(main_cnt);
    values[2] = Int64GetDatum(free_cnt);
    values[3] = Int64GetDatum(g_pca_buf_ctx->stat.recycle_cnt);

     /* Build and return the result tuple. */
    bool nulls[PCA_BUFFERS_STAT_INFO_COLS];
    securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");
    HeapTuple tuple = heap_form_tuple(tupdesc, (Datum*)values, (bool*)nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void compress_ratio_info_single(char* path, int64 *file_count, int64 *logic_size, int64 *physic_size)
{
    char pathname[MAXPGPATH] = {0};
    unsigned int segcount = 0;
    errno_t rc;

    for (segcount = 0;; segcount++) {
        struct stat fst;
        rc = memset_s(pathname, MAXPGPATH, 0, MAXPGPATH);
        securec_check(rc, "\0", "\0");

        CHECK_FOR_INTERRUPTS();  // can be interrputed at any time

        if (segcount == 0) {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s%s", path, COMPRESS_STR);
        } else {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s.%u%s", path, segcount, COMPRESS_STR);
        }
        securec_check_ss(rc, "\0", "\0");

        if (stat(pathname, &fst) < 0) {
            if (errno == ENOENT && segcount > 0) {
                break;
            } else {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": ", pathname)));
            }
        }

        *logic_size += fst.st_size;
        *physic_size += (fst.st_blocks * FILE_BLOCK_SIZE_512);
    }

    *file_count += segcount;
}

void checkPath(char* path)
{
    if (path == NULL || strlen(path) <= 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("The input path is incorrect.")));
    for (uint32 n = 0; n < strlen(path) - 1; n++) {
        if (path[n] == '/' && path[n + 1] == '/') {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("The input path is incorrect.")));
        }
    }
}

#define PCA_COMPRESS_RATIO_INFO_COLS (6)
Datum compress_ratio_info(PG_FUNCTION_ARGS)
{
    int i = 0, j = 1;

    // get input params
    char* path = text_to_cstring(PG_GETARG_TEXT_P(0));
    checkPath(path);

    // format the tupdesc
    TupleDesc tupdesc = CreateTemplateTupleDesc(PCA_COMPRESS_RATIO_INFO_COLS, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "path", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "is_compress", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "file_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "logic_size", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "physic_size", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "compress_ratio", TEXTOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    // calculate the values
    int64 file_count = 0, logic_size = 0, physic_size = 0;

    // get complete RelFileNode
    RelFileNodeForkNum relfilenode = relpath_to_filenode(path);

    // get Oid by RelFileNode
    Oid relid = DatumGetObjectId(DirectFunctionCall2Coll(
                                        pg_filenode_relation, 
                                        InvalidOid, 
                                        ObjectIdGetDatum(relfilenode.rnode.node.spcNode), 
                                        ObjectIdGetDatum(relfilenode.rnode.node.relNode)));
    Relation relation = try_relation_open(relid, AccessShareLock);
    if (!RelationIsValid(relation)) {
        PG_RETURN_NULL();
    }
    relfilenode.rnode.node = relation->rd_node;
    relation_close(relation, AccessShareLock);

    bool isCompress = IS_COMPRESSED_RNODE(relfilenode.rnode.node, MAIN_FORKNUM);
    if (!isCompress) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the specified file(relation) is non-compression relation.")));
    }

    if (relation->rd_rel->parttype == PARTTYPE_NON_PARTITIONED_RELATION) {
        compress_ratio_info_single(path, &file_count, &logic_size, &physic_size);
    } else if (PART_OBJ_TYPE_TABLE_PARTITION == relation->rd_rel->parttype ||
           PART_OBJ_TYPE_INDEX_PARTITION == relation->rd_rel->parttype) {
        List *partid_list = getPartitionObjectIdList(relfilenode.rnode.node.relNode, relation->rd_rel->parttype);
        ListCell *cell;
        RelFileNode rnode = relfilenode.rnode.node;
        foreach (cell, partid_list) {
            rnode.relNode = DatumGetObjectId(DirectFunctionCall1Coll(pg_partition_filenode, 
                                                                    InvalidOid, 
                                                                    ObjectIdGetDatum(lfirst_oid(cell))));
            if (!rnode.relNode) {
                PG_RETURN_NULL();
            }
            char *sub_path = relpathbackend(rnode, InvalidBackendId, MAIN_FORKNUM);
            compress_ratio_info_single(sub_path, &file_count, &logic_size, &physic_size);
            pfree(sub_path);
        }
    } else if (PART_OBJ_TYPE_TABLE_SUB_PARTITION == relation->rd_rel->parttype) {
        List *subpartid_list = getSubPartitionObjectIdList(relfilenode.rnode.node.relNode);
        ListCell *cell;
        RelFileNode rnode = relfilenode.rnode.node;
        foreach (cell, subpartid_list) {
            rnode.relNode = DatumGetObjectId(DirectFunctionCall1Coll(pg_partition_filenode, 
                                                                    InvalidOid, 
                                                                    ObjectIdGetDatum(lfirst_oid(cell))));
            if (!rnode.relNode) {
                PG_RETURN_NULL();
            }
            char *sub_path = relpathbackend(rnode, InvalidBackendId, MAIN_FORKNUM);
            compress_ratio_info_single(sub_path, &file_count, &logic_size, &physic_size);
            pfree(sub_path);
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the specified file(relation) is unsupported compressed type.")));
    }

    // input the values
    Datum values[PCA_COMPRESS_RATIO_INFO_COLS];
    values[i++] = CStringGetTextDatum(path);
    values[i++] = BoolGetDatum(isCompress);
    values[i++] = Int64GetDatum(file_count);
    values[i++] = Int64GetDatum(logic_size);
    values[i++] = Int64GetDatum(physic_size);

    double ratio = (logic_size == 0 ? 0 : (double)physic_size / (double)logic_size) * 100;
    int ratio_len = 8;
    char* ratio_str = (char*)palloc0((uint32)ratio_len);

    errno_t rc = sprintf_s(ratio_str, (uint32)ratio_len, "%.2lf%%", ratio);
    securec_check_ss(rc, "\0", "\0");
    values[i++] = CStringGetTextDatum(ratio_str);

     /* Build and return the result tuple. */
    bool nulls[PCA_COMPRESS_RATIO_INFO_COLS];
    securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");

    HeapTuple tuple = heap_form_tuple(tupdesc, (Datum*)values, (bool*)nulls);

    pfree(ratio_str);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void compress_statistic_single_file_info(char* pathname,
    int64 *extent_count, int64 *dispersion_count, int64 *void_count, size_t file_size, int16 step)
{
    FILE *compressedFd = AllocateFile((const char *)pathname, "rb");

    uint32 extCnt = (uint32)(file_size / (BLCKSZ * CFS_EXTENT_SIZE) +
                             (file_size % (BLCKSZ * CFS_EXTENT_SIZE) != 0 ? 1 : 0));
    for (uint32 extentIndex = 0; extentIndex < extCnt; extentIndex += (uint16)step) {
        CHECK_FOR_INTERRUPTS();  // can be interrputed at any time
        auto mmapHeaderResult = MMapHeader(compressedFd, extentIndex, true);

        CfsExtentHeader *header = mmapHeaderResult.header;
        // get statistic
        uint16 sum_allocated_chunks = 0;
        for (uint16 pageIdx = 0; pageIdx < header->nblocks; pageIdx++) {
            CfsExtentAddress *cfsExtentAddress = GetExtentAddress(header, pageIdx);
            sum_allocated_chunks += cfsExtentAddress->allocated_chunks;
            uint16 minChrunkNo = 0xFFFF, maxChrunkNo = 0;
            for (uint16 idx = 0; idx < cfsExtentAddress->allocated_chunks; idx++) {
                minChrunkNo = minChrunkNo <
                    cfsExtentAddress->chunknos[idx] ? minChrunkNo : cfsExtentAddress->chunknos[idx];
                maxChrunkNo = maxChrunkNo >
                    cfsExtentAddress->chunknos[idx] ? maxChrunkNo : cfsExtentAddress->chunknos[idx];
            }
            if (((maxChrunkNo - minChrunkNo) + 1) != cfsExtentAddress->allocated_chunks) {
                (*dispersion_count)++;
            }
        }

        *void_count += (header->allocated_chunks - sum_allocated_chunks);
        MmapFree(&mmapHeaderResult);
    }

    *extent_count += extCnt;
    (void)FreeFile(compressedFd);
}

void compress_statistic_info_single(char* path, int16 step,
    int64 *extent_count, int64 *dispersion_count, int64 *void_count)
{
    char pathname[MAXPGPATH] = {0};
    unsigned int segcount = 0;
    errno_t rc;

    for (segcount = 0;; segcount++) {
        struct stat fst;
        rc = memset_s(pathname, MAXPGPATH, 0, MAXPGPATH);
        securec_check(rc, "\0", "\0");

        CHECK_FOR_INTERRUPTS();  // can be interrputed at any time

        if (segcount == 0) {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s%s", path, COMPRESS_STR);
        } else {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s.%u%s", path, segcount, COMPRESS_STR);
        }
        securec_check_ss(rc, "\0", "\0");

        if (stat(pathname, &fst) < 0) {
            if (errno == ENOENT && segcount > 0) {
                break;
            } else {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": ", pathname)));
            }
        }

        // get every file statistic
        compress_statistic_single_file_info(pathname, extent_count, dispersion_count, void_count,
                                            (size_t)fst.st_size, step);
    }
}

#define PCA_COMPRESS_STATISTIC_INFO_COLS (4)
#define PCA_COMPRESS_STATISTIC_SKIP_MIN  (1)
#define PCA_COMPRESS_STATISTIC_SKIP_MAX  (99)
Datum compress_statistic_info(PG_FUNCTION_ARGS)
{
    int i = 0, j = 1;

    // get input params
    char* path = text_to_cstring(PG_GETARG_TEXT_P(0));
    checkPath(path);

    int16 step = PG_GETARG_INT16(1);
    if (step < PCA_COMPRESS_STATISTIC_SKIP_MIN || step > PCA_COMPRESS_STATISTIC_SKIP_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the value of skip-count is between [1, 99]")));
    }

    // get complete RelFileNode
    RelFileNodeForkNum relfilenode = relpath_to_filenode(path);

    // get Oid by RelFileNode
    Oid relid = DatumGetObjectId(DirectFunctionCall2Coll(pg_filenode_relation, 
                                                        InvalidOid, 
                                                        ObjectIdGetDatum(relfilenode.rnode.node.spcNode), 
                                                        ObjectIdGetDatum(relfilenode.rnode.node.relNode)));
    Relation relation = try_relation_open(relid, AccessShareLock);
    if (!RelationIsValid(relation)) {
        PG_RETURN_NULL();
    }
    relfilenode.rnode.node = relation->rd_node;
    relation_close(relation, AccessShareLock);

    if (!IS_COMPRESSED_RNODE(relfilenode.rnode.node, MAIN_FORKNUM)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the specified file(relation) is non-compression relation.")));
    }

    // format the tupdesc
    TupleDesc tupdesc = CreateTemplateTupleDesc(PCA_COMPRESS_STATISTIC_INFO_COLS, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "path", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "extent_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "dispersion_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "void_count", INT8OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    // calculate the values
    int64 extent_count = 0, dispersion_count = 0, void_count = 0;

    if (relation->rd_rel->parttype == PARTTYPE_NON_PARTITIONED_RELATION) {
        compress_statistic_info_single(path, step, &extent_count, &dispersion_count, &void_count);
    } else if (PART_OBJ_TYPE_TABLE_PARTITION == relation->rd_rel->parttype ||
           PART_OBJ_TYPE_INDEX_PARTITION == relation->rd_rel->parttype) {
        List *partid_list = getPartitionObjectIdList(relfilenode.rnode.node.relNode, relation->rd_rel->parttype);
        ListCell *cell;
        RelFileNode rnode = relfilenode.rnode.node;
        foreach (cell, partid_list) {
            rnode.relNode = DatumGetObjectId(DirectFunctionCall1Coll(pg_partition_filenode, 
                                                                    InvalidOid, 
                                                                    ObjectIdGetDatum(lfirst_oid(cell))));
            if (!rnode.relNode) {
                PG_RETURN_NULL();
            }
            char *sub_path = relpathbackend(rnode, InvalidBackendId, MAIN_FORKNUM);
            compress_statistic_info_single(sub_path, step, &extent_count, &dispersion_count, &void_count);
            pfree(sub_path);
        }
    } else if (PART_OBJ_TYPE_TABLE_SUB_PARTITION == relation->rd_rel->parttype) {
        List *subpartid_list = getSubPartitionObjectIdList(relfilenode.rnode.node.relNode);
        ListCell *cell;
        RelFileNode rnode = relfilenode.rnode.node;
        foreach (cell, subpartid_list) {
            rnode.relNode = DatumGetObjectId(DirectFunctionCall1Coll(pg_partition_filenode, 
                                                                    InvalidOid, 
                                                                    ObjectIdGetDatum(lfirst_oid(cell))));
            if (!rnode.relNode) {
                PG_RETURN_NULL();
            }
            char *sub_path = relpathbackend(rnode, InvalidBackendId, MAIN_FORKNUM);
            compress_statistic_info_single(sub_path, step, &extent_count, &dispersion_count, &void_count);
            pfree(sub_path);
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the specified file(relation) is unsupported compressed type.")));
    }

    // input the values
    Datum values[PCA_COMPRESS_STATISTIC_INFO_COLS];
    values[i++] = CStringGetTextDatum(path);
    values[i++] = Int64GetDatum(extent_count);
    values[i++] = Int64GetDatum(dispersion_count);
    values[i++] = Int64GetDatum(void_count);

     /* Build and return the result tuple. */
    bool nulls[PCA_COMPRESS_STATISTIC_INFO_COLS];
    securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");

    HeapTuple tuple = heap_form_tuple(tupdesc, (Datum*)values, (bool*)nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void item_state_free(CompressAddressItemState *itemState)
{
    if (itemState->rbStruct.header != NULL) {
        MmapFree(&itemState->rbStruct);
        itemState->rbStruct.header = NULL;
    }
    (void)FreeFile(itemState->compressedFd);
    itemState->compressedFd = NULL;
}

Datum pg_read_binary_file_blocks(PG_FUNCTION_ARGS)
{
    int32 startBlockNum = PG_GETARG_INT32(1);
    int32 blockCount = PG_GETARG_INT32(2);

    if (startBlockNum < 0 || blockCount < 0 || startBlockNum + blockCount > RELSEG_SIZE) {
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
        size_t len = 0;
        if (itemState->fileLen > 0) {
            BlockNumber extentCount = itemState->blkno / CFS_LOGIC_BLOCKS_PER_EXTENT;
            if (itemState->rbStruct.extentCount != extentCount || itemState->rbStruct.header == NULL) {
                MmapFree(&itemState->rbStruct);
                auto curHeader = MMapHeader(itemState->compressedFd, extentCount, true);


                itemState->rbStruct.header = curHeader.header;
                itemState->rbStruct.extentCount = curHeader.extentCount;
                itemState->rbStruct.pointer = curHeader.pointer;
                itemState->rbStruct.mmapLen = curHeader.mmapLen;
            }

            CfsReadStruct cfsReadStruct{itemState->compressedFd, itemState->rbStruct.header, extentCount};
            len = CfsReadCompressedPage(VARDATA(buf), BLCKSZ, itemState->blkno % CFS_LOGIC_BLOCKS_PER_EXTENT,
                                        &cfsReadStruct, CFS_LOGIC_BLOCKS_PER_FILE * itemState->segmentNo + itemState->blkno);
            if (len > MIN_COMPRESS_ERROR_RT) {
                item_state_free(itemState);
                ereport(ERROR, (ERRCODE_INVALID_PARAMETER_VALUE,
                                errmsg("can not read actual block %u, error code: %lu,", itemState->blkno, len)));
            }
        }
        SET_VARSIZE(buf, len + VARHDRSZ);
        Datum values[6];
        values[0] = PG_GETARG_DATUM(0);
        values[1] = Int32GetDatum(itemState->blkno);
        values[2] = Int32GetDatum(len);
        values[3] = Int32GetDatum(itemState->fileLen == 0 ? 0 : itemState->rbStruct.header->algorithm);
        values[4] = Int32GetDatum(itemState->fileLen == 0 ? 0 : itemState->rbStruct.header->chunk_size);
        values[5] = PointerGetDatum(buf);

        /* Build and return the result tuple. */
        bool nulls[6];
        securec_check(memset_s(nulls, sizeof(nulls), 0, sizeof(nulls)), "\0", "\0");
        HeapTuple tuple = heap_form_tuple(fctx->tuple_desc, (Datum*)values, (bool*)nulls);
        Datum result = HeapTupleGetDatum(tuple);
        itemState->blkno++;
        SRF_RETURN_NEXT(fctx, result);
    } else {
        item_state_free(itemState);
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
    tupdesc = CreateTemplateTupleDesc(6, false);
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

        /*
         * Do not copy log directory during gs_rewind. Currently we only consider such case that
         * log_directory is relative path and only one level deep. More complicated cases should
         * be taken care of in the future.
         */
        if (u_sess->proc_cxt.clientIsGsrewind && strcmp(de->d_name, u_sess->attr.attr_common.Log_directory) == 0)
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
        tupdesc = CreateTemplateTupleDesc(4, false);
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

/*
 * Generic function to return a directory listing of files.
 *
 * If the directory isn't there, silently return an empty set if missing_ok.
 * Other unreadable-directory cases throw an error.
 */
static Datum pg_ls_dir_files(FunctionCallInfo fcinfo, const char *dir, bool missing_ok)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    bool randomAccess;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    DIR *dirdesc;
    struct dirent *de;
    MemoryContext oldcontext;
    errno_t rc = EOK;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("materialize mode required, but it is not allowed in this context")));

    /* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    tupstore = tuplestore_begin_heap(randomAccess, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);

    /*
     * Now walk the directory.  Note that we must do this within a single SRF
     * call, not leave the directory open across multiple calls, since we
     * can't count on the SRF being run to completion.
     */
    dirdesc = AllocateDir(dir);
    if (!dirdesc) {
        /* Return empty tuplestore if appropriate */
        if (missing_ok && errno == ENOENT)
            return (Datum)0;
        /* Otherwise, we can let ReadDir() throw the error */
    }

    while ((de = ReadDir(dirdesc, dir)) != NULL) {
        Datum values[3];
        bool nulls[3];
        char path[MAXPGPATH * 2];
        struct stat attrib;

        /* Skip hidden files */
        if (de->d_name[0] == '.')
            continue;

        /* Get the file info */
        rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/%s", dir, de->d_name);
        securec_check_ss(rc, "\0", "\0");

        if (stat(path, &attrib) < 0) {
            /* Ignore concurrently-deleted files, else complain */
            if (errno == ENOENT)
                continue;
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", path)));
        }

        /* Ignore anything but regular files */
        if (!S_ISREG(attrib.st_mode))
            continue;

        values[0] = CStringGetTextDatum(de->d_name);
        values[1] = Int64GetDatum((int64)attrib.st_size);
        values[2] = TimestampTzGetDatum(time_t_to_timestamptz(attrib.st_mtime));
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    (void)FreeDir(dirdesc);
    return (Datum)0;
}

/* Function to return the list of files in the WAL directory */
Datum pg_ls_waldir(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId()))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("only system/monitor admin can check WAL directory!")));
    return pg_ls_dir_files(fcinfo, SS_XLOGDIR, false);
}

/*
 * Generic function to return the list of files in pgsql_tmp
 */
static Datum pg_ls_tmpdir(FunctionCallInfo fcinfo, Oid tblspc)
{
    char path[MAXPGPATH];
    if (!superuser() && !isMonitoradmin(GetUserId()))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("only system/monitor admin can check pgsql_tmp directory!")));
    if (OidIsValid(tblspc)) {
        if (!SearchSysCacheExists1(TABLESPACEOID, ObjectIdGetDatum(tblspc)))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace with OID %u does not exist", tblspc)));
    }

    TempTablespacePath(path, tblspc);
    return pg_ls_dir_files(fcinfo, path, true);
}

/*
 * Function to return the list of temporary files in the pg_default tablespace's
 * pgsql_tmp directory
 */
Datum pg_ls_tmpdir_noargs(PG_FUNCTION_ARGS)
{
    if (!ENABLE_DSS) {
        return pg_ls_tmpdir(fcinfo, DEFAULTTABLESPACE_OID);
    } else {
        return pg_ls_tmpdir(fcinfo, InvalidOid);
    }
}

/*
 * Function to return the list of temporary files in the specified tablespace's
 * pgsql_tmp directory
 */
Datum pg_ls_tmpdir_1arg(PG_FUNCTION_ARGS)
{
    return pg_ls_tmpdir(fcinfo, PG_GETARG_OID(0));
}
