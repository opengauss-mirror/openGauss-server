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
 * smb.h
 *
 *
 * IDENTIFICATION
 *        src/include/access/smb.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SMB_H
#define SMB_H

#include "postgres.h"
#include "utils/elog.h"
#include "access/xlog_internal.h"
#include "access/xlogproc.h"
#include "storage/buf/buf_internals.h"
#include "storage/rack_mem.h"

/* avoid checkpoint if async redo */
#define ENABLE_ASYNC_REDO false
#define ENABLE_SMB_CKPT (g_instance.smb_cxt.use_smb && !RecoveryInProgress() && !g_instance.smb_cxt.shutdownSMBWriter)
#define SIZE_K(n) (uint32)((n) * 1024)
#define SHARED_MEM_NAME "smb_shared_meta"
#define MAX_SHM_CHUNK_NAME_LENGTH 64
#define BLOCKS_PER_CHUNK (MAX_RACK_ALLOC_SIZE/(8 * 1024))
#define SMB_INVALID_ID (-1)
#define SMB_WRITER_MAX_ITEM_SIZE (g_instance.smb_cxt.NSMBBuffers * sizeof(smb_recovery::SMBBufItem))
#define SMB_WRITER_MAX_BUCKET_PER_FILE SIZE_K(200)
#define SMB_WRITER_MAX_BUCKET_SIZE \
	(smb_recovery::SMB_WRITER_MAX_FILE * static_cast<uint64>(SMB_WRITER_MAX_BUCKET_PER_FILE) * sizeof(int))
#define SMB_WRITER_ITEM_SIZE_PERMETA (SMB_WRITER_MAX_ITEM_SIZE / smb_recovery::SMB_BUF_MGR_NUM)
#define SMB_WRITER_BUCKET_SIZE_PERMETA (SMB_WRITER_MAX_BUCKET_SIZE / smb_recovery::SMB_BUF_MGR_NUM)
#define SMB_BUF_META_SIZE \
	(SMB_WRITER_ITEM_SIZE_PERMETA + SMB_WRITER_BUCKET_SIZE_PERMETA + 2 * sizeof(int) + 2 * sizeof(XLogRecPtr))
#define SMB_WRITER_ITEM_PER_MGR (g_instance.smb_cxt.NSMBBuffers / smb_recovery::SMB_BUF_MGR_NUM)

#define SMB_NEED_REDO 0
#define SMB_REDOING 1
#define SMB_REDO_DONE 2

namespace smb_recovery {

constexpr int SMB_BUF_MGR_NUM = 8;
constexpr int SMB_WRITER_MAX_FILE = 10;

struct SMBAnalyzer {
    ServerMode initialServerMode;
    TimeLineID initialTimeLineID;
    List *expectedTLIs;
    HotStandbyState standbyState;
    bool StandbyMode;
    char *DataDir;
    TransactionId RecentXmin;
    TransactionId latestObservedXid;
    TimeLineID recoveryTargetTLI;
    char* recoveryRestoreCommand;
    bool ArchiveRecoveryRequested;
    bool StandbyModeRequested;
    bool InArchiveRecovery;
    bool ArchiveRestoreRequested;
    bool InRecovery;
    XLogRecPtr minRecoveryPoint;
};

// Analyse Mem
typedef struct SMBAnalyseItem {
    BufferTag tag;
    XLogPhyBlock pblk;
    XLogRecPtr lsn;
    bool is_verified;
    struct SMBAnalyseItem *next;
} SMBAnalyseItem;

typedef struct SMBAnalyseBucket {
    uint32 count;
    SMBAnalyseItem *first;
    LWLock lock;
} SMBAnalyseBucket;

typedef struct SMBAnalyseMem {
    SMBAnalyseItem *smbAlyItems;
    SMBAnalyseBucket *smbAlyBuckets;
    SMBAnalyseBucket freelist;
} SMBAnalyseMem;

typedef struct SMBAnalysePage {
    BufferTag tag;
    XLogPhyBlock pblk;
    XLogRecPtr lsn;
} SMBAnalysePage;

typedef struct SMBAnalysePageQueue {
    uint64 size;
    pg_atomic_uint64 head;
    pg_atomic_uint64 tail;
    SMBAnalysePage *pages;
} SMBAnalysePageQueue;

// Write Mem
typedef struct SMBBufItem {
    BufferTag tag;
    int nextLRUId;
    int prevLRUId;
    int nextBktId;
    int prevBktId;
    int id;
    XLogRecPtr lsn;
} SMBBufItem;

typedef struct SMBBufMetaMem {
    SMBBufItem *smbWriterItems;
    int *smbWriterBuckets;
    int *lruHeadId;
    int *lruTailId;
    XLogRecPtr *startLsn;
    XLogRecPtr *endLsn;
} SMBBufMetaMem;

typedef struct SMBDirtyPageQueueSlot {
    volatile int buffer;
    BufferTag tag;
    pg_atomic_uint32 slot_state;
} SMBDirtyPageQueueSlot;

typedef struct SMBDirtyPageQueue {
    struct SMBDirtyPageQueueSlot *pageQueueSlot;
    uint64 size;
    pg_atomic_uint64 head;
    pg_atomic_uint64 tail;
} SMBDirtyPageQueue;

typedef struct SMBStatusInfo {
    pg_atomic_uint64 pageDirtyNums;
    int lruRemovedNum;
} SMBStatusInfo;

extern void SMBAnalysisMain(void);
extern bool IsSMBSafe(XLogRecPtr curPtr);
extern void InitSMBAly();
extern void SetSMBAnalyzerInfo(SMBAnalyzer *g_analyzer);

extern void SMBAnalysisAuxiliaryMain(void);
extern void SMBAlyMemInit();
extern void SMBSetPageLsn(BufferTag tag, XLogPhyBlock pblk, XLogRecPtr lsn);
extern SMBAnalyseItem *SMBAlyGetPageItem(BufferTag tag);
extern SMBAnalyseBucket *SMBAlyGetBucket(BufferTag tag);

extern inline void GetSmbShmChunkName(char* chunkName, size_t shmChunkNumber)
{
    errno_t rc = EOK;
    rc = sprintf_s(chunkName, MAX_SHM_CHUNK_NAME_LENGTH, "smb_shared_%u", shmChunkNumber);
    securec_check_ss_c(rc, "", "");
}

extern inline SMBBufItem *SMBWriterIdGetItem(SMBBufMetaMem *mgr, int id)
{
    int mgrId = id / SMB_WRITER_ITEM_PER_MGR;
    int itemId = id % SMB_WRITER_ITEM_PER_MGR;

    return &mgr[mgrId].smbWriterItems[itemId];
}

extern inline Page SMBWriterGetPage(int id)
{
    return ((Page)g_instance.smb_cxt.SMBBufMem[(id) / BLOCKS_PER_CHUNK + 1] + ((id) % BLOCKS_PER_CHUNK * BLCKSZ));
}

extern void SMBWriterMain(void);
extern void InitSMBWriter();
extern int CheckPagePullStateFromSMB(BufferTag tag);
extern void SMBPullOnePageWithBuf(BufferDesc *bufHdr);
extern void SMBStartPullPages(XLogRecPtr curPtr);
extern void SMBMarkDirty(Buffer buffer, XLogRecPtr lsn);
extern int SMBWriterLoadThreadIndex(void);
extern void SMBWriterAuxiliaryMain(void);

} // namespace smb_recovery

#endif