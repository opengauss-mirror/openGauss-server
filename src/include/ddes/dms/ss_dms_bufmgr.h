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
 * ss_dms_bufmgr.h
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_dms_bufmgr.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SS_DMS_BUFMGR_H
#define SS_DMS_BUFMGR_H

#include "ddes/dms/ss_common_attr.h"
#include "ddes/dms/ss_dms.h"
#include "storage/buf/buf_internals.h"
#include "access/xlogproc.h"

#define GetDmsBufCtrl(id) (&t_thrd.storage_cxt.dmsBufCtl[(id)])
#define SS_BUF_MAX_WAIT_TIME (1000L * 1000 * 20) // 20s
#define SS_BUF_WAIT_TIME_IN_ONDEMAND_REALTIME_BUILD (100000L)  // 100ms

#define DmsInitLatch(drid, _type, _oid, _idx, _parent_part, _part, _uid) \
    do {                                                      \
        (drid)->type = _type;                                 \
        (drid)->uid = _uid;                                   \
        (drid)->oid = _oid;                                   \
        (drid)->index = _idx;                                 \
        (drid)->parent_part = _parent_part;                   \
        (drid)->part = _part;                                 \
    } while (0)

typedef struct SSBroadcastDDLLock {
    SSBroadcastOp type; // must be first
    LOCKTAG locktag;
    LOCKMODE lockmode;
    bool sessionlock;
    bool dontWait;
} SSBroadcastDDLLock;

void InitDmsBufCtrl(void);
void InitDmsContext(dms_context_t* dmsContext);
void InitDmsBufContext(dms_context_t* dmsBufCxt, BufferTag buftag);
void MarkReadHint(int buf_id, char persistence, bool extend, const XLogPhyBlock *pblk);
bool LockModeCompatible(dms_buf_ctrl_t *buf_ctrl, LWLockMode mode);
bool StartReadPage(BufferDesc *buf_desc, LWLockMode mode);
void ClearReadHint(int buf_id, bool buf_deleted = false);
Buffer TerminateReadPage(BufferDesc* buf_desc, ReadBufferMode read_mode, const XLogPhyBlock *pblk);
Buffer TerminateReadSegPage(BufferDesc *buf_desc, ReadBufferMode read_mode, SegSpace *spc = NULL);
Buffer DmsReadPage(Buffer buffer, LWLockMode mode, ReadBufferMode read_mode, bool *with_io);
Buffer DmsReadSegPage(Buffer buffer, LWLockMode mode, ReadBufferMode read_mode, bool *with_io);
bool DmsReleaseOwner(BufferTag buf_tag, int buf_id);
int SSLockAcquire(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock, bool dontWait,
    dms_opengauss_lock_req_type_t reqType = LOCK_NORMAL_MODE);
int SSLockRelease(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock);
void SSLockReleaseAll();
void SSLockAcquireAll();
void MarkReadPblk(int buf_id, const XLogPhyBlock *pblk);
void SSCheckBufferIfNeedMarkDirty(Buffer buf);
void SSRecheckBufferPool();
void SSOndemandClearRedoDoneState();
void TransformLockTagToDmsLatch(dms_drlatch_t* dlatch, const LOCKTAG locktag);
bool CheckPageNeedSkipInRecovery(Buffer buf);
void SmgrNetPageCheckDiskLSN(BufferDesc* buf_desc, ReadBufferMode read_mode, const XLogPhyBlock *pblk);
void SegNetPageCheckDiskLSN(BufferDesc* buf_desc, ReadBufferMode read_mode, SegSpace *spc);
dms_session_e DMSGetProcType4RequestPage();
void BufValidateDrc(BufferDesc *buf_desc);
bool SSPageCheckIfCanEliminate(BufferDesc* buf_desc, uint64 flags);
bool SSSegRead(SMgrRelation reln, ForkNumber forknum, char *buffer);
bool DmsCheckBufAccessible();
bool SSHelpFlushBufferIfNeed(BufferDesc* buf_desc);
void SSMarkBufferDirtyForERTO(RedoBufferInfo* bufferinfo);
long SSGetBufSleepTime(int retry_times);
SMGR_READ_STATUS SmgrNetPageCheckRead(Oid spcNode, Oid dbNode, Oid relNode, ForkNumber forkNum,
    BlockNumber blockNo, char *blockbuf);
bool SSPinBuffer(BufferDesc *buf_desc);
void SSUnPinBuffer(BufferDesc* buf_desc);
bool SSOndemandRequestPrimaryRedo(BufferTag tag);
bool SSLWLockAcquireTimeout(LWLock* lock, LWLockMode mode);
bool SSWaitIOTimeout(BufferDesc *buf);
void buftag_get_buf_info(BufferTag tag, stat_buf_info_t *buf_info);
Buffer SSReadBuffer(BufferTag *tag, ReadBufferMode mode);
void DmsReleaseBuffer(int buffer, bool is_seg);
bool SSRequestPageInOndemandRealtimeBuild(BufferTag *bufferTag, XLogRecPtr recordLsn, XLogRecPtr *pageLsn);
bool SSOndemandRealtimeBuildAllowFlush(BufferDesc *buf);
bool SSNeedTerminateRequestPageInReform(dms_buf_ctrl_t *buf_ctrl);
#endif
