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
#include "storage/custorage.h"
#include "storage/ipc.h"
#include "storage/remote_adapter.h"
#include "libpq/pqformat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/builtins.h"

#define MAX_WAIT_TIMES 5

/*
 * @Description: wait lsn to replay
 * @IN primary_insert_lsn: remote request lsn
 * @Return: remote read error code
 * @See also:
 */
int XLogWaitForReplay(uint64 primary_insert_lsn)
{
    int wait_times = 0;

    /* local replayed lsn */
    XLogRecPtr standby_replay_lsn = GetXLogReplayRecPtr(NULL, NULL);

    /* if primary_insert_lsn  >  standby_replay_lsn then need wait */
    while (!XLByteLE(primary_insert_lsn, standby_replay_lsn)) {
        /* if sleep to much times */
        if (wait_times >= MAX_WAIT_TIMES) {
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
 */
Datum gs_read_block_from_remote(PG_FUNCTION_ARGS)
{
    uint32 spcNode;
    uint32 dbNode;
    uint32 relNode;
    int16 bucketNode;
    int32 forkNum;
    uint64 blockNum;
    uint32 blockSize;
    uint64 lsn;
    bool isForCU = false;
    bytea* result = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    spcNode = PG_GETARG_UINT32(0);
    dbNode = PG_GETARG_UINT32(1);
    relNode = PG_GETARG_UINT32(2);
    bucketNode = PG_GETARG_INT16(3);
    forkNum = PG_GETARG_INT32(4);
    blockNum = (uint64)PG_GETARG_TRANSACTIONID(5);
    blockSize = PG_GETARG_UINT32(6);
    lsn = (uint64)PG_GETARG_TRANSACTIONID(7);
    isForCU = PG_GETARG_BOOL(8);

    /* get block from local buffer */
    if (isForCU) {
        /* if request to read CU block, we use forkNum column to replace colid. */
        (void)StandbyReadCUforPrimary(spcNode, dbNode, relNode, forkNum, blockNum, blockSize, lsn, &result);
    } else {
        (void)StandbyReadPageforPrimary(spcNode, dbNode, relNode, bucketNode, 0, forkNum, blockNum, blockSize,
            lsn, &result);
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
    uint32 spcNode;
    uint32 dbNode;
    uint32 relNode;
    int16 bucketNode;
    uint16 opt = 0;
    int32 forkNum;
    uint64 blockNum;
    uint32 blockSize;
    uint64 lsn;
    bool isForCU = false;
    bytea* result = NULL;

    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be initial account to read files"))));
    }
    /* handle optional arguments */
    spcNode = PG_GETARG_UINT32(0);
    dbNode = PG_GETARG_UINT32(1);
    relNode = PG_GETARG_UINT32(2);
    bucketNode = PG_GETARG_INT16(3);
    opt = PG_GETARG_UINT16(4);
    forkNum = PG_GETARG_INT32(5);
    blockNum = (uint64)PG_GETARG_TRANSACTIONID(6);
    blockSize = PG_GETARG_UINT32(7);
    lsn = (uint64)PG_GETARG_TRANSACTIONID(8);
    isForCU = PG_GETARG_BOOL(9);
    /* get block from local buffer */
    if (isForCU) {
        /* if request to read CU block, we use forkNum column to replace colid. */
        (void)StandbyReadCUforPrimary(spcNode, dbNode, relNode, forkNum, blockNum, blockSize, lsn, &result);
    } else {
        (void)StandbyReadPageforPrimary(spcNode, dbNode, relNode, bucketNode, opt, forkNum, blockNum, blockSize,
                                        lsn, &result);
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
int StandbyReadCUforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset, int32 size,
                            uint64 lsn, bytea** cudata)
{
    Assert(cudata);

    int ret_code = REMOTE_READ_OK;

    /* wait request lsn for replay */
    ret_code = XLogWaitForReplay(lsn);
    if (ret_code != REMOTE_READ_OK) {
        ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("could not redo request lsn.")));
        return ret_code;
    }

    RelFileNode relfilenode {spcnode, dbnode, relnode, InvalidBktId};

    {
        /* read from disk */
        CFileNode cfilenode(relfilenode, colid, MAIN_FORKNUM);

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
                errno_t rc = memcpy_s(VARDATA(buf), size + 1, cu->m_compressedLoadBuf, size);
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
int StandbyReadPageforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int16 bucketnode, uint2 opt, int32 forknum,
                              uint32 blocknum, uint32 blocksize, uint64 lsn, bytea** pagedata)
{
    Assert(pagedata);

    if (unlikely(blocksize != BLCKSZ))
        return REMOTE_READ_BLCKSZ_NOT_SAME;

    int ret_code = REMOTE_READ_OK;

    /* wait request lsn for replay */
    ret_code = XLogWaitForReplay(lsn);
    if (ret_code != REMOTE_READ_OK) {
        ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("could not redo request lsn.")));
        return ret_code;
    }

    RelFileNode relfilenode {spcnode, dbnode, relnode, bucketnode, opt};

    {
        bytea* pageData = (bytea*)palloc(BLCKSZ + VARHDRSZ);
        SET_VARSIZE(pageData, BLCKSZ + VARHDRSZ);
        bool hit = false;

        /* read page, if PageIsVerified failed will long jump to PG_CATCH() */
        Buffer buf = ReadBufferForRemote(relfilenode, forknum, blocknum, RBM_FOR_REMOTE, NULL, &hit);

        if (BufferIsInvalid(buf)) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("standby page buffer is invalid!")));
            return REMOTE_READ_BLCKSZ_NOT_SAME;
        }
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Block block = BufferGetBlock(buf);

        errno_t rc = memcpy_s(VARDATA(pageData), BLCKSZ, block, BLCKSZ);
        if (rc != EOK) {
            ereport(ERROR, (errmodule(MOD_REMOTE), errmsg("memcpy_s error, retcode=%d", rc)));
            ret_code = REMOTE_READ_MEMCPY_ERROR;
        }

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buf);

        if (ret_code == REMOTE_READ_OK) {
            *pagedata = pageData;
            PageSetChecksumInplace((Page) VARDATA(*pagedata), blocknum);
        }
    }

    return ret_code;
}
