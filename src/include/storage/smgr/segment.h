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
 * segment.h
 *
 *
 * IDENTIFICATION
 *        src/include/storage/segment.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STORAGE_SEGMENT_H
#define STORAGE_SEGMENT_H

#include "storage/smgr/segment_internal.h"

/* smgr API functions */
void seg_init();
void seg_shutdown();
void seg_close(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
void seg_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
bool seg_exists(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
void seg_unlink(const RelFileNodeBackend &rnode, ForkNumber forknum, bool isRedo, BlockNumber blockNum);
void seg_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync);
void seg_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
SMGR_READ_STATUS seg_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer);
void seg_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync);
void seg_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
BlockNumber seg_nblocks(SMgrRelation reln, ForkNumber forknum);
BlockNumber seg_totalblocks(SMgrRelation reln, ForkNumber forknum);
void seg_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
void seg_immedsync(SMgrRelation reln, ForkNumber forknum);
void seg_pre_ckpt(void);
void seg_sync(void);
void seg_post_ckpt(void);
void seg_async_read(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn);
void seg_async_write(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn);
void seg_move_buckets(const RelFileNodeBackend &dest, const RelFileNodeBackend &src, List *bucketList);
bool seg_fork_exists(SegSpace *spc, SMgrRelation reln, ForkNumber forknum, const XLogPhyBlock *pblk);

/* Read/write by physical block number; used for segment meta data */
void seg_physical_read(SegSpace *spc, RelFileNode &rNode, ForkNumber forknum, BlockNumber blocknum, char *buffer);
void seg_physical_write(SegSpace *spc, RelFileNode &rNode, ForkNumber forknum, BlockNumber blocknum, const char *buffer,
                        bool skipFsync);
XLogRecPtr seg_get_headlsn(SegSpace *spc, BlockNumber blockNum, bool isbucket);

/* segment sync callback */
void forget_space_fsync_request(SegSpace *spc);
void seg_register_dirty_file(SegLogicFile *sf, int segno);
void seg_register_forget_request(SegLogicFile *sf, int segno);
int seg_sync_filetag(const FileTag *ftag, char *path);
int seg_unlink_filetag(const FileTag *ftag, char *path);
void segForgetDatabaseFsyncRequests(Oid dbid);
bool seg_filetag_matches(const FileTag *ftag, const FileTag *candidate);

/*
 * XLog Atomic Operation APIs
 */
enum SegmentXLogOperationCode {
    SPCXLOG_SET_BITMAP = 0,
    SPCXLOG_FREE_BITMAP,
    SPCXLOG_MAPHEAD_ALLOCATED_EXTENTS,
    SPCXLOG_INIT_SEGHEAD,
    SPCXLOG_UPDATE_SEGHEAD,
    SPCXLOG_NEW_LEVEL0_PAGE,
    SPCXLOG_LEVEL0_PAGE_ADD_EXTENT,
    SPCXLOG_SEGHEAD_ADD_FORK_SEGMENT,
    SPCXLOG_UNLINK_SEGHEAD_PTR,   // unlink a segment head for its owner
    SPCXLOG_INIT_BUCKET_HEAD,     // init BktMainHead
    SPCXLOG_BUCKET_FREE_MAPBLOCK, // delete a BktHeadMapBlock from BktMainHead
    SPCXLOG_BUCKET_ADD_MAPBLOCK,  // add a MapBlock to BktMainHead
    SPCXLOG_BUCKET_INIT_MAPBLOCK, // init a MapBlock page
    SPCXLOG_BUCKET_ADD_BKTHEAD,   // create a segment for a bucket
    SPCXLOG_SPACE_UPDATE_HWM,     // update high water mark
    SPCXLOG_SET_INVERSE_POINTER,  // set inverse pointer
    SPCXLOG_SEG_MOVE_BUCKETS,
    SPCXLOG_BUCKET_ADD_REDISINFO,
    SPCXLOG_SHRINK_SEGHEAD_UPDATE,  // update segment head during extent movement when shrink
    SPCXLOG_INVALID_OP_CODE = 255
};

static const int MaxOperationsPerBlock = 16;

struct DecodedXLogBlockOp {
    uint8 operations;
    int data_len[MaxOperationsPerBlock];
    char *data[MaxOperationsPerBlock];
    uint8 op[MaxOperationsPerBlock];
};

/*
 * Flags passed to XLogAtomicOpRegisterBuffer, denoting whether release/unlock the dirty buffer
 * when xlog is committed. In some cases we want to keep buffer pinned or locked.
 */
#define XLOG_COMMIT_KEEP_BUFFER_STATE 0x0000
#define XLOG_COMMIT_RELEASE_BUFFER 0x0001
#define XLOG_COMMIT_UNLOCK_BUFFER 0x0002
#define XLOG_COMMIT_UNLOCK_RELEASE_BUFFER (XLOG_COMMIT_UNLOCK_BUFFER | XLOG_COMMIT_RELEASE_BUFFER)

#define BUF_NEED_RELEASE(flag) ((flag)&XLOG_COMMIT_RELEASE_BUFFER)
#define BUF_NEED_UNLOCK(flag) ((flag)&XLOG_COMMIT_UNLOCK_BUFFER)
#define BUF_NEED_UNLOCK_RELEASE(flag) (BUF_NEED_UNLOCK(flag) && BUF_NEED_RELEASE(flag))

void XLogAtomicOpInit();
void XLogAtomicOpReset();

void XLogAtomicOpStart();
void XLogAtomicOpRegisterBuffer(Buffer buffer, uint8 flags, uint8 op, uint32 clean_flag);
void XLogAtomicOpRegisterBufData(char *data, int len);
void XLogAtomicOpCommit();

DecodedXLogBlockOp XLogAtomicDecodeBlockData(char *data, int len);

/*
 * APIs used for segment store metadata.
 */
BufferDesc *SegBufferAlloc(SegSpace *spc, RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum, bool *foundPtr);
Buffer ReadBufferFast(SegSpace *spc, RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode);
void SegReleaseBuffer(Buffer buffer);
void SegUnlockReleaseBuffer(Buffer buffer);
void SegMarkBufferDirty(Buffer buffer);
void SegFlushBuffer(BufferDesc* bufHdr, SMgrRelation);
void SegDropSpaceMetaBuffers(Oid spcNode, Oid dbNode);
void AbortSegBufferIO(void);
void FlushDataBufferOfSegment(SegSpace *spc, BlockNumber head_block, ForkNumber forknum);
void FlushOneSegmentBuffer(Buffer buffer);
void FlushOneBufferIncludeDW(BufferDesc *buf_desc);
Buffer try_get_moved_pagebuf(RelFileNode *rnode, int forknum, BlockNumber logic_blocknum);

/* Segment Remain API */
enum StatRemainExtentType {
    ALLOC_SEGMENT = 1,
    DROP_SEGMENT,
    SHRINK_EXTENT,
    INVALID_REMAIN_TYPE,
};

typedef struct RemainExtentHashTag {
    RelFileNode rnode;
    Oid extentType;
} RemainExtentHashTag;

typedef struct ExtentTag {
    RemainExtentHashTag remainExtentHashTag;
    ForkNumber forkNum;
    StatRemainExtentType remainExtentType;
    TransactionId xid;
    XLogRecPtr lsn;
} ExtentTag;

#define XLOG_REMAIN_SEGS_FILE_PATH "global/pg_remain_segs"
#define XLOG_REMAIN_SEGS_BACKUP_FILE_PATH "global/pg_remain_segs.backup"
#define XLOG_REMAIN_SEGS_SIZE 8192
#define XLOG_REMAIN_SEG_FILE_LEAST_LEN (sizeof(XLogRecPtr) + sizeof(uint32) * 3)
#define XLOG_REMAIN_SEGS_BATCH_NUM 20
#define XLOG_REMAIN_SEGS_BATCH_BUFFER_LEN (sizeof(ExtentTag) * XLOG_REMAIN_SEGS_BATCH_NUM)


static const int REMAIN_SEGS_INFO_COL_NUM = 4;
const uint32 REMAIN_SEG_MAGIC_NUM = 0x12345678;
bool IsRemainSegsFileExist();
void xlogRemoveRemainSegsByDropDB(Oid dbId, Oid tablespaceId);
const char* XlogGetRemainExtentTypeName(StatRemainExtentType remainExtentType);

#define CurrentThreadIsWorker()                                                                                        \
    (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == STREAMING_BACKEND)

#define SegmentCheck(condition)                              \
    do {                                                      \
        if (!(condition))                                     \
            ereport(PANIC, (errmsg("SegmentCheck failed. File:%s, Line:%d", __FILE__, __LINE__)));  \
    } while (0)

extern Oid get_database_oid_by_name(const char *dbname);
extern Oid get_tablespace_oid_by_name(const char *tablespacename);
extern void redo_xlog_deal_alloc_seg(uint8 opCode, Buffer buffer, const char* data, int data_len,
    TransactionId xid);
extern StorageType PartitionGetStorageType(Partition partition, Oid parentOid);

#endif
