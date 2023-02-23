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
 * ---------------------------------------------------------------------------------------
 *
 * remote_adapter.cpp
 *         using simple C API interface for rpc call PG founction
 *         Don't include any of RPC header file.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/remote/remote_adapter.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"
#include "postmaster/postmaster.h"
#include "storage/smgr/segment_internal.h"
#include "storage/smgr/segment.h"
#include "storage/custorage.h"
#include "storage/ipc.h"
#include "storage/remote_adapter.h"
#include "libpq/pqformat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/aiomem.h"

const int DEFAULT_WAIT_TIMES = 60;
int ReadFileForRemote(RemoteReadFileKey *key, XLogRecPtr lsn, bytea** fileData, int timeout, int32 read_size);

int ReadCOrCsnFileForRemote(RelFileNode rnode, bytea** fileData);

/*
 * @Description: wait lsn to replay
 * @IN primary_insert_lsn: remote request lsn
 * @Return: remote read error code
 * @See also:
 */
int XLogWaitForReplay(uint64 primary_insert_lsn, int timeout = DEFAULT_WAIT_TIMES)
{
    int wait_times = 0;

    /* local replayed lsn */
    XLogRecPtr standby_replay_lsn = GetXLogReplayRecPtr(NULL, NULL);

    /* if primary_insert_lsn  >  standby_replay_lsn then need wait */
    while (!XLByteLE(primary_insert_lsn, standby_replay_lsn)) {
        /* if sleep to much times */
        if (wait_times >= timeout) {
            ereport(LOG,
                    (errmodule(MOD_REMOTE),
                     errmsg("replay slow. requre lsn %X/%X, replayed lsn %X/%X",
                            (uint32)(primary_insert_lsn >> 32),
                            (uint32)primary_insert_lsn,
                            (uint32)(standby_replay_lsn >> 32),
                            (uint32)standby_replay_lsn)));
            return REMOTE_READ_NEED_WAIT;
        }

        /* sleep 1s */
        pg_usleep(1000000L);
        ++wait_times;

        /* get current replay lsn again */
        (void)GetXLogReplayRecPtr(NULL, &standby_replay_lsn);
    }

    return REMOTE_READ_OK;
}

/*
 * Read block from buffer from primary, returning it as bytea
 * RelFileNode has changed, add a opt to compress relation.
 */
const int GS_READ_BLOK_PARMS_V1 = 10;
const int GS_READ_BLOK_PARMS_V2 = 11;
Datum gs_read_block_from_remote(PG_FUNCTION_ARGS)
{
    RepairBlockKey key;
    uint32 blockSize;
    uint64 lsn;
    int timeout = 0;
    bool isForCU = false;
    int parano = 0;
    bytea* result = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    key.relfilenode.spcNode = PG_GETARG_UINT32(parano++);
    key.relfilenode.dbNode = PG_GETARG_UINT32(parano++);
    key.relfilenode.relNode = PG_GETARG_UINT32(parano++);
    key.relfilenode.bucketNode = PG_GETARG_INT16(parano++);
    if (PG_NARGS() == GS_READ_BLOK_PARMS_V2) {
        key.relfilenode.opt = (uint2)PG_GETARG_INT16(parano++);
    } else if (PG_NARGS() == GS_READ_BLOK_PARMS_V1) {
        key.relfilenode.opt = 0;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("Invalid parameters count."))));
    }
    key.forknum = PG_GETARG_INT32(parano++);
    key.blocknum = (uint64)PG_GETARG_TRANSACTIONID(parano++);
    blockSize = PG_GETARG_UINT32(parano++);
    lsn = (uint64)PG_GETARG_TRANSACTIONID(parano++);
    isForCU = PG_GETARG_BOOL(parano++);
    timeout = PG_GETARG_INT32(parano++);
    /* get block from local buffer */
    if (isForCU) {
        /* if request to read CU block, we use forkNum column to replace colid. */
        (void)StandbyReadCUforPrimary(key, key.blocknum, (int)blockSize, lsn, timeout, &result);
    } else {
        (void)StandbyReadPageforPrimary(key, blockSize, lsn, &result, timeout, NULL);
    }

    if (NULL != result) {
        PG_RETURN_BYTEA_P(result);
    } else {
        PG_RETURN_NULL();
    }
}


/*
 * Read block from buffer from primary, returning it as bytea
 */
Datum gs_read_block_from_remote_compress(PG_FUNCTION_ARGS)
{
    RepairBlockKey key;
    uint32 blockSize;
    uint64 lsn;
    int timeout = 0;
    bool isForCU = false;
    bytea* result = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    key.relfilenode.spcNode = PG_GETARG_UINT32(0);
    key.relfilenode.dbNode = PG_GETARG_UINT32(1);
    key.relfilenode.relNode = PG_GETARG_UINT32(2);
    key.relfilenode.bucketNode = PG_GETARG_INT16(3);
    key.relfilenode.opt = PG_GETARG_UINT16(4);
    key.forknum = PG_GETARG_INT32(5);
    key.blocknum = (uint64)PG_GETARG_TRANSACTIONID(6);
    blockSize = PG_GETARG_UINT32(7);
    lsn = (uint64)PG_GETARG_TRANSACTIONID(8);
    isForCU = PG_GETARG_BOOL(9);
    timeout = PG_GETARG_INT32(10);
    /* get block from local buffer */
    if (isForCU) {
        /* if request to read CU block, we use forkNum column to replace colid. */
        (void)StandbyReadCUforPrimary(key, key.blocknum, blockSize, lsn, timeout, &result);
    } else {
        (void)StandbyReadPageforPrimary(key, blockSize, lsn, &result, timeout, NULL);
    }
    
    if (NULL != result) {
        PG_RETURN_BYTEA_P(result);
    } else {
        PG_RETURN_NULL();
    }
}

/*
 * @Description: read cu for primary
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN colid: column id
 * @IN offset: cu offset
 * @IN size: cu size
 * @IN lsn: lsn wait for replay
 * @IN/OUT context: read context
 * @OUT cudata: output cu data
 * @Return: remote read error code
 * @See also:
 */
int StandbyReadCUforPrimary(RepairBlockKey key, uint64 offset, int32 size, uint64 lsn, int32 timeout,
    bytea** cudata)
{
    Assert(cudata);

    int ret_code = REMOTE_READ_OK;

    /* wait request lsn for replay */
    if (RecoveryInProgress()) {
        ret_code = XLogWaitForReplay(lsn, timeout);
        if (ret_code != REMOTE_READ_OK) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("could not redo request lsn.")));
            return ret_code;
        }
    }

    RelFileNode relfilenode {key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode, InvalidBktId, 0};

    {
        /* read from disk */
        CFileNode cfilenode(relfilenode, key.forknum, MAIN_FORKNUM);

        CUStorage* custorage = New(CurrentMemoryContext) CUStorage(cfilenode);
        CU* cu = New(CurrentMemoryContext) CU();
        cu->m_inCUCache = false;

        custorage->LoadCU(cu, offset, size, false, false);

        /* check crc */
        if (ret_code == REMOTE_READ_OK) {
            if (!cu->CheckCrc()) {
                ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("check CU crc error.")));
                ret_code = REMOTE_READ_CRC_ERROR;
            } else {
                bytea* buf = (bytea*)palloc0(VARHDRSZ + size);
                SET_VARSIZE(buf, size + VARHDRSZ);
                errno_t rc = memcpy_s(VARDATA(buf), size, cu->m_compressedLoadBuf, size);
                if (rc != EOK) {
                    ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("memcpy_s error, retcode=%d", rc)));
                    ret_code = REMOTE_READ_MEMCPY_ERROR;
                }
                *cudata = buf;
                DELETE_EX(custorage);
                DELETE_EX(cu);
            }
        }
    }

    return ret_code;
}

/*
 * @Description: read page for primary
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN forknum: forknum
 * @IN blocknum: block number
 * @IN blocksize: block size
 * @IN lsn: lsn wait for replay
 * @IN/OUT context: read context
 * @OUT pagedata: output page data
 * @Return: remote read error code
 * @See also:
 */
int StandbyReadPageforPrimary(RepairBlockKey key, uint32 blocksize, uint64 lsn, bytea** pagedata,
    int timeout, const XLogPhyBlock *pblk)
{
    Assert(pagedata);

    if (unlikely(blocksize != BLCKSZ))
        return REMOTE_READ_BLCKSZ_NOT_SAME;

    int ret_code = REMOTE_READ_OK;
    BlockNumber checksum_block = key.blocknum;

    /* wait request lsn for replay */
    if (RecoveryInProgress()) {
        ret_code = XLogWaitForReplay(lsn, timeout);
        if (ret_code != REMOTE_READ_OK) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("could not redo request lsn.")));
            return ret_code;
        }
    }

    RelFileNode relfilenode{key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                            key.relfilenode.bucketNode, key.relfilenode.opt};

    if (NULL != pblk) {
        SegPageLocation loc = seg_get_physical_location(relfilenode, key.forknum, key.blocknum, false);
        uint8 standby_relNode = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size);
        BlockNumber standby_block = loc.blocknum;
        checksum_block = loc.blocknum;
        if (standby_relNode != pblk->relNode || standby_block != pblk->block) {
            ereport(ERROR, (errmodule(MOD_REMOTE),
                    errmsg("Standby page file is invalid! Standby relnode is %u, "
                           "master is %u; Standby block is %u, master is %u.",
                           standby_relNode, pblk->relNode, standby_block, pblk->block)));
            return REMOTE_READ_BLCKSZ_NOT_SAME;
        }
    }

    bytea* pageData = (bytea*)palloc(BLCKSZ + VARHDRSZ);
    SET_VARSIZE(pageData, BLCKSZ + VARHDRSZ);
    if (IsSegmentPhysicalRelNode(relfilenode)) {
        Buffer buffer = InvalidBuffer;
        SegSpace *spc = spc_open(relfilenode.spcNode, relfilenode.dbNode, false, false);
        BlockNumber spc_nblocks = spc_size(spc, relfilenode.relNode, key.forknum);

        if (key.blocknum < spc_nblocks) {
            buffer = ReadBufferFast(spc, relfilenode, key.forknum, key.blocknum, RBM_FOR_REMOTE);
        }

        if (BufferIsValid(buffer)) {
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            Block block = BufferGetBlock(buffer);

            errno_t rc = memcpy_s(VARDATA(pageData), BLCKSZ, block, BLCKSZ);
            if (rc != EOK) {
                pfree(pageData);
                ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("memcpy_s error, retcode=%d", rc)));
                ret_code = REMOTE_READ_MEMCPY_ERROR;
            }

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
        } else {
            ret_code = REMOTE_READ_SIZE_ERROR;
        }
    } else {
        bool hit = false;

        /* read page, if PageIsVerified failed will long jump to PG_CATCH() */
        Buffer buf = ReadBufferForRemote(relfilenode, key.forknum, key.blocknum, RBM_FOR_REMOTE, NULL, &hit, pblk);

        if (BufferIsInvalid(buf)) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("standby page buffer is invalid!")));
            return REMOTE_READ_BLCKSZ_NOT_SAME;
        }
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Block block = BufferGetBlock(buf);

        errno_t rc = memcpy_s(VARDATA(pageData), BLCKSZ, block, BLCKSZ);
        if (rc != EOK) {
            pfree(pageData);
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("memcpy_s error, retcode=%d", rc)));
            ret_code = REMOTE_READ_MEMCPY_ERROR;
        }

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buf);
    }

    if (ret_code == REMOTE_READ_OK) {
        *pagedata = pageData;
        PageSetChecksumInplace((Page) VARDATA(*pagedata), checksum_block);
    }

    return ret_code;
}

/* RelFileNode has changed, add a opt to compress relation. */
const int RES_COL_NUM = 2;
const int GS_READ_FILE_PARMS_V1 = 8;
const int GS_READ_FILE_PARMS_V2 = 10;
#define REMOTE_READ_FILE_SIZE_INVALID (-1)
Datum gs_read_file_from_remote(PG_FUNCTION_ARGS)
{
    RelFileNode rnode;
    RemoteReadFileKey key;
    int32 forknum;
    uint32 blockstart;
    int read_size = REMOTE_READ_FILE_SIZE_INVALID;
    uint64 lsn;
    bytea* result = NULL;
    Datum values[RES_COL_NUM];
    bool nulls[RES_COL_NUM] = {false};
    HeapTuple tuple = NULL;
    TupleDesc tupdesc;
    int ret_code = 0;
    int32 timeout;
    int parano = 0;
    XLogRecPtr current_lsn = InvalidXLogRecPtr;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    rnode.spcNode = PG_GETARG_UINT32(parano++);
    rnode.dbNode = PG_GETARG_UINT32(parano++);
    rnode.relNode = PG_GETARG_UINT32(parano++);
    rnode.bucketNode = PG_GETARG_INT16(parano++);
    if (PG_NARGS() == GS_READ_FILE_PARMS_V2) {
        rnode.opt = (uint2)PG_GETARG_INT16(parano++);
        read_size = PG_GETARG_INT32(parano++);
    } else if (PG_NARGS() == GS_READ_FILE_PARMS_V1) {
        rnode.opt = 0;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("Invalid parameters count."))));
    }

    if (rnode.opt == 0) {
        read_size = REMOTE_READ_FILE_SIZE_INVALID;
    }

    forknum = PG_GETARG_INT32(parano++);
    blockstart = (uint32)PG_GETARG_INT32(parano++);
    lsn = (uint64)PG_GETARG_TRANSACTIONID(parano++);
    timeout = PG_GETARG_INT32(parano++);

    if (rnode.spcNode != 1 && rnode.spcNode != 2) {
         /* get tale data file */
        key.relfilenode = rnode;
        key.forknum = forknum;
        key.blockstart = blockstart;
        if (forknum != MAIN_FORKNUM) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Forknum should be 0. Now is %d. \n", forknum))));
            PG_RETURN_NULL();
        }
        ret_code = ReadFileForRemote(&key, lsn, &result, timeout, read_size);
    } else {
        ret_code = ReadCOrCsnFileForRemote(rnode, &result);
    }

    if (ret_code != REMOTE_READ_OK) {
        PG_RETURN_NULL();
    }

    if (!RecoveryInProgress()) {
        current_lsn = GetXLogInsertRecPtr();
    }

    tupdesc = CreateTemplateTupleDesc(RES_COL_NUM, false);
    parano = 1;
    TupleDescInitEntry(tupdesc, (AttrNumber)parano++, "file", BYTEAOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)parano++, "lsn", XIDOID, -1, 0);
    values[0] = PointerGetDatum(result);
    nulls[0] = false;
    values[1] = UInt64GetDatum(current_lsn);
    nulls[1] = false;

    tupdesc = BlessTupleDesc(tupdesc);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* RelFileNode has changed, add a opt to compress relation. */
const int GS_READ_FILE_SIZE_PARMS_V1 = 7;
const int GS_READ_FILE_SIZE_PARMS_V2 = 8;
Datum gs_read_file_size_from_remote(PG_FUNCTION_ARGS)
{
    RelFileNode rnode;
    int32 forknum;
    uint64 lsn;
    int64 size = 0;
    int32 timeout;
    int parano = 0;
    int ret_code = REMOTE_READ_OK;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    rnode.spcNode = PG_GETARG_UINT32(parano++);
    rnode.dbNode = PG_GETARG_UINT32(parano++);
    rnode.relNode = PG_GETARG_UINT32(parano++);
    rnode.bucketNode = PG_GETARG_INT16(parano++);
    if (PG_NARGS() == GS_READ_FILE_SIZE_PARMS_V2) {
        rnode.opt = (uint2)PG_GETARG_INT16(parano++);
    } else if (PG_NARGS() == GS_READ_FILE_SIZE_PARMS_V1) {
        rnode.opt = 0;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("Invalid parameters count."))));
    }
    forknum = PG_GETARG_INT32(parano++);
    lsn = (uint64)PG_GETARG_TRANSACTIONID(parano++);
    timeout = PG_GETARG_INT32(parano++);

    /* get file size */
    ret_code = ReadFileSizeForRemote(rnode, forknum, lsn, &size, timeout);
    if (ret_code == REMOTE_READ_OK) {
        PG_RETURN_INT64(size);
    } else {
        PG_RETURN_NULL();
    }
}

int ReadFileSizeForRemote(RelFileNode rnode, int32 forknum, XLogRecPtr lsn, int64* res, int timeout)
{
    SMgrRelation smgr = NULL;
    int64 nblock = 0;
    int ret_code = REMOTE_READ_OK;

    /* wait request lsn for replay */
    if (RecoveryInProgress()) {
        ret_code = XLogWaitForReplay(lsn, timeout);
        if (ret_code != REMOTE_READ_OK) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("could not redo request lsn.")));
            return ret_code;
        }
    }

    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /* check whether the file exists. not exist, return size -1 */
    struct stat statBuf;
    char* path = relpathperm(rnode, forknum);
    char* openFilePath = path;
    char dst[MAXPGPATH];
    if (IS_COMPRESSED_RNODE(rnode, MAIN_FORKNUM)) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }
    if (stat(openFilePath, &statBuf) < 0 && errno == ENOENT) {
        *res = -1;
        pfree(path);
        return ret_code;
    }
    pfree(path);

    if (!IsSegmentFileNode(rnode)) {
        smgr = smgropen(rnode, InvalidBackendId);
        nblock = smgrnblocks(smgr, forknum);
        smgrcloseall();
    } else {
        SegSpace *spc = spc_open(rnode.spcNode, rnode.dbNode, true, true);
        spc_datafile_create(spc, rnode.relNode, forknum);

        nblock = spc_size(spc, rnode.relNode, forknum);
    }
    *res = nblock * BLCKSZ;

    return ret_code;
}

int ReadFileByReadBufferComom(RemoteReadFileKey *key, bytea* pageData, uint32 blk_start, uint32 blk_end)
{
    int ret_code = REMOTE_READ_OK;
    uint32 i = 0;
    uint32 j = 0;
    bool hit = false;
    char* bufBlock = NULL;
    errno_t rc;

    for (i = blk_start, j = 0; i < blk_end; i++, j++) {
        /* read page, if PageIsVerified failed will long jump */
        BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_REPAIR);
        Buffer buf = ReadBufferForRemote(key->relfilenode, key->forknum, i, RBM_FOR_REMOTE, bstrategy, &hit, NULL);

        if (BufferIsInvalid(buf)) {
            ereport(WARNING, (errmodule(MOD_REMOTE), errmsg("repair file failed!")));
            return REMOTE_READ_BLCKSZ_NOT_SAME;
        }

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        bufBlock = (char*)BufferGetBlock(buf);

        rc = memcpy_s(VARDATA(pageData) + j * BLCKSZ, BLCKSZ, bufBlock, BLCKSZ);
        if (rc != EOK) {
            ereport(WARNING, (errmodule(MOD_REMOTE), errmsg("repair file failed, memcpy_s error, retcode=%d", rc)));
            ret_code = REMOTE_READ_MEMCPY_ERROR;
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buf);
            return ret_code;
        }

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buf);
        PageSetChecksumInplace((Page) (VARDATA(pageData) + j * BLCKSZ), i);
    }

    return ret_code;
}

const int MAX_RETRY_TIMES = 60;
int ReadFileByReadDisk(SegSpace* spc, RemoteReadFileKey *key, char* bufBlock, BlockNumber blocknum)
{
    int ret_code = REMOTE_READ_OK;
    int pageStatus;
    int retryTimes = 0;

    if (IsSegmentFileNode(key->relfilenode)) {
        RelFileNode fakenode = {
            .spcNode = key->relfilenode.spcNode,
            .dbNode = key->relfilenode.dbNode,
            .relNode = key->relfilenode.relNode,
            .bucketNode = SegmentBktId,
            .opt = 0
        };
SEG_RETRY:
        seg_physical_read(spc, fakenode, key->forknum, blocknum, (char *)bufBlock);
        retryTimes++;
        if (PageIsVerified((Page)bufBlock, blocknum)) {
            pageStatus = SMGR_RD_OK;
        } else {
            pageStatus = SMGR_RD_CRC_ERROR;
            if (retryTimes < MAX_RETRY_TIMES) {
                /* sleep 10ms */
                pg_usleep(10000L);
                goto SEG_RETRY;
            } else {
                pfree(bufBlock);
                ereport(WARNING, (errmodule(MOD_REMOTE),
                    errmsg("repair file failed, read page crc check error, page: %u/%u/%u/%d, "
                    "forknum is %d, block num is %u", key->relfilenode.spcNode, key->relfilenode.dbNode,
                    key->relfilenode.relNode, key->relfilenode.bucketNode, key->forknum, blocknum)));
                ret_code = REMOTE_READ_CRC_ERROR;
                return ret_code;
            }
        }
    } else {
        SMgrRelation smgr = smgropen(key->relfilenode, InvalidBackendId);
        /* standby read page, replay finish, there will be no synchronous changes. */
        pageStatus = smgrread(smgr, key->forknum, blocknum, (char *)bufBlock);
        retryTimes++;
        if (pageStatus != SMGR_RD_OK) {
            pfree(bufBlock);
            ereport(WARNING, (errmodule(MOD_REMOTE),
                errmsg("repair file failed, read page crc check error, page: %u/%u/%u/%d, "
                "forknum is %d, block num is %u", key->relfilenode.spcNode, key->relfilenode.dbNode,
                key->relfilenode.relNode, key->relfilenode.bucketNode, key->forknum, blocknum)));
            ret_code = REMOTE_READ_CRC_ERROR;
            smgrclose(smgr);
            return ret_code;
        }
        smgrclose(smgr);
    }
    return ret_code;
}

int ReadFileForRemote(RemoteReadFileKey *key, XLogRecPtr lsn, bytea** fileData, int timeout, int32 read_size)
{
    int ret_code = REMOTE_READ_OK;
    SMgrRelation smgr = NULL;
    SegSpace* spc = NULL;
    bytea* pageData = NULL;
    char* bufBlock = NULL;
    uint32 nblock = 0;
    uint32 blk_start = 0;
    uint32 blk_end = 0;
    uint32 i = 0;
    uint32 j = 0;
    errno_t rc;

    /* wait request lsn for replay */
    if (RecoveryInProgress()) {
        ret_code = XLogWaitForReplay(lsn, timeout);
        if (ret_code != REMOTE_READ_OK) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("could not redo request lsn.")));
            return ret_code;
        }
    }
    if (RecoveryInProgress() || IsSegmentFileNode(key->relfilenode)) {
        RequestCheckpoint(CHECKPOINT_WAIT | CHECKPOINT_FORCE | CHECKPOINT_IMMEDIATE);
    }

    /* get block num */
    if (read_size == REMOTE_READ_FILE_SIZE_INVALID) {
        if (!IsSegmentFileNode(key->relfilenode)) {
            smgr = smgropen(key->relfilenode, InvalidBackendId);
            nblock = smgrnblocks(smgr, key->forknum);
            smgrclose(smgr);
        } else {
            spc = spc_open(key->relfilenode.spcNode, key->relfilenode.dbNode, false, false);
            if (!spc) {
                ereport(WARNING, (errmodule(MOD_REMOTE),
                        errmsg("Spc open failed. spcNode is: %u, dbNode is %u",
                        key->relfilenode.spcNode, key->relfilenode.dbNode)));
                return REMOTE_READ_IO_ERROR;
            }
            nblock = spc_size(spc, key->relfilenode.relNode, key->forknum);
        }

        if (nblock <= key->blockstart) {
            ret_code = REMOTE_READ_SIZE_ERROR;
            return ret_code;
        }

        /* get the segno file block start and block end */
        blk_start = key->blockstart;
        blk_end = (nblock >= blk_start + MAX_BATCH_READ_BLOCKNUM ? blk_start + MAX_BATCH_READ_BLOCKNUM : nblock);

        pageData = (bytea*)palloc((blk_end - blk_start) * BLCKSZ + VARHDRSZ);
        SET_VARSIZE(pageData, ((blk_end - blk_start) * BLCKSZ + VARHDRSZ));
    } else {
        if (IsSegmentFileNode(key->relfilenode)) {
            spc = spc_open(key->relfilenode.spcNode, key->relfilenode.dbNode, false, false);
            if (!spc) {
                ereport(WARNING, (errmodule(MOD_REMOTE),
                        errmsg("Spc open failed. spcNode is: %u, dbNode is %u",
                        key->relfilenode.spcNode, key->relfilenode.dbNode)));
                return REMOTE_READ_IO_ERROR;
            }
        }

        /* get the segno file block start and block end */
        blk_start = key->blockstart;
        blk_end = key->blockstart + (uint32)read_size / BLCKSZ;

        pageData = (bytea*)palloc((uint32)(read_size + VARHDRSZ));
        SET_VARSIZE(pageData, (read_size + VARHDRSZ));
    }

    /* primary read page, need read page by ReadBuffer_common */
    if (!IsSegmentFileNode(key->relfilenode) && !RecoveryInProgress()) {
        ret_code = ReadFileByReadBufferComom(key, pageData, blk_start, blk_end);
        if (ret_code != REMOTE_READ_OK) {
            pfree(pageData);
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("read file failed!")));
            return ret_code;
        }
    } else {
        ADIO_RUN()
        {
            bufBlock = (Page)adio_align_alloc(BLCKSZ);
        }
        ADIO_ELSE()
        {
            bufBlock = (Page)palloc(BLCKSZ);
        }
        ADIO_END();
        for (i = blk_start, j = 0; i < blk_end; i++, j++) {
            ret_code = ReadFileByReadDisk(spc, key, bufBlock, i);
            if (ret_code != REMOTE_READ_OK) {
                pfree(pageData);
                ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("repair file failed, read block %u error, retcode=%d",
                    i, rc)));
                return ret_code;
            }
            rc = memcpy_s(VARDATA(pageData) + j * BLCKSZ, BLCKSZ, bufBlock, BLCKSZ);
            if (rc != EOK) {
                pfree(bufBlock);
                pfree(pageData);
                ret_code = REMOTE_READ_MEMCPY_ERROR;
                ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("repair file failed, memcpy_s error, retcode=%d", rc)));
                return ret_code;
            }
        }
        pfree(bufBlock);
        if (ret_code != REMOTE_READ_OK) {
            pfree(pageData);
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("read file failed!")));
            return ret_code;
        }
    }

    *fileData = pageData;

    return ret_code;
}

const int REGR_MCR_SIZE_1MB = 1048576;
const int CLOG_NODE = 1;
const int CSN_NODE = 2;
int ReadCOrCsnFileForRemote(RelFileNode rnode, bytea** fileData)
{
    uint32 flags = O_RDWR | PG_BINARY;
    int fd = -1;
    char* logType = NULL;
    char* path = (char*)palloc0(MAX_PATH);
    errno_t rc;
    uint32 logSize = 16 * REGR_MCR_SIZE_1MB;
    char *buffer = (char*)palloc(logSize);
    int result = -1;

    if (rnode.spcNode == CLOG_NODE) {
        logType = "pg_clog";
    } else if (rnode.spcNode == CSN_NODE) {
        logType = "pg_csnlog";
    } else {
        ereport(LOG, (errmodule(MOD_SEGMENT_PAGE), errmsg("File type\"%u\" does not exist, stop read here.",
                                                          rnode.spcNode)));
    }

    rc = snprintf_s(path, MAX_PATH, MAX_PATH - 1, "%s/%012u", logType, rnode.relNode);
    securec_check_ss(rc, "\0", "\0");

    fd = BasicOpenFile(path, flags, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        pfree(buffer);
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }
        // The file does not exist, break.
        ereport(LOG,
                (errmodule(MOD_SEGMENT_PAGE), errmsg("File \"%s\" does not exist, stop read here.", path)));
        pfree(path);
        return -1;
    }
    pgstat_report_waitevent(WAIT_EVENT_DATA_FILE_READ);
    uint32 nbytes = pread(fd, buffer, logSize, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (close(fd)) {
        pfree(path);
        pfree(buffer);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", path)));
    }

    if (nbytes > logSize) {
        pfree(buffer);
        ereport(ERROR,
                (errcode(MOD_SEGMENT_PAGE),
                        errcode_for_file_access(),
                        errmsg("could not read file %s. nbytes:%u, logSize:%u", path, nbytes, logSize)));
        pfree(path);
        return -1;
    } else {
        bytea* pageData = (bytea*)palloc(nbytes + VARHDRSZ);
        SET_VARSIZE(pageData, (nbytes + VARHDRSZ));
        rc = memcpy_s(VARDATA(pageData), nbytes, buffer, nbytes);
        if (rc != EOK) {
            pfree(path);
            pfree(pageData);
            pfree(buffer);
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("repair file failed, memcpy_s error, retcode=%d", rc)));
            return -1;
        } else {
            *fileData = pageData;
            result = 0;
        }
    }
    pfree(path);
    pfree(buffer);
    return result;
}
