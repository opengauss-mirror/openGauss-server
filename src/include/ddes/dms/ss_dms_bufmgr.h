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

#define SS_BUF_MAX_WAIT_TIME (1000L * 1000 * 20) // 20s
#define SS_BUF_WAIT_TIME_IN_ONDEMAND_REALTIME_BUILD (100000L)  // 100ms

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
bool CheckPageNeedSkipInRecovery(Buffer buf, uint64 xlogLsn);
void SegPageCheckDiskLSNForRelease(BufferDesc *buf_desc);
void SmgrNetPageCheckDiskLSN(BufferDesc* buf_desc, ReadBufferMode read_mode, const XLogPhyBlock *pblk);
void SegNetPageCheckDiskLSN(BufferDesc* buf_desc, ReadBufferMode read_mode, SegSpace *spc);
dms_session_e DMSGetProcType4RequestPage();
void BufValidateDrc(BufferDesc *buf_desc);
bool SSPageCheckIfCanEliminate(BufferDesc* buf_desc, uint64 flags);
bool SSSegRead(SMgrRelation reln, ForkNumber forknum, char *buffer);
bool DmsCheckBufAccessible();
bool SSHelpFlushBufferIfNeed(BufferDesc* buf_desc);
void SSMarkBufferDirtyForERTO(RedoBufferInfo* bufferinfo);
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
bool SSNeedTerminateRequestPageInReform(dms_buf_ctrl_t *buf_ctrl);
bool SSNeedTerminateRequestMetaPageInReform(BufferDesc *buf_desc);
const char *SSPageReadCancelCauseName(SSPageReadCancelCause cause);
const char *SSPageReadCancelPointName(SSPageReadCancelPoint point);
void ForgetBufferNeedCheckPin(Buffer buf_id);

/* Whether this thread should handle failover page-read cancel. */
static inline bool SSPageReadCancelRoleAllowed()
{
    return t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == TRACK_STMT_CLEANER ||
        t_thrd.role == TRACK_STMT_WORKER || t_thrd.role == AUTOVACUUM_WORKER || t_thrd.role == JOB_WORKER ||
        t_thrd.role == JOB_SCHEDULER || t_thrd.role == RBWORKER || t_thrd.role == TXNSNAP_WORKER ||
        t_thrd.role == APPLY_WORKER || t_thrd.role == UNDO_WORKER || t_thrd.role == COMM_POOLER_CLEAN ||
        t_thrd.role == CATCHUP;
}

/* Whether this thread should exit page read during failover. */
static inline bool SSNeedExitPageReadInFailover()
{
    return ENABLE_DMS && SSPageReadCancelRoleAllowed() &&
        (g_instance.dms_cxt.SSRecoveryInfo.in_failover || SS_PERFORMING_FAILOVER);
}

/* Whether this thread should retry page read during primary restart. */
static inline bool SSNeedRetryPageReadInPrimaryRestart()
{
    return ENABLE_DMS && SSPageReadCancelRoleAllowed() && SS_STANDBY_IN_PRIMARY_RESTART;
}

/* Whether page-read cancel is allowed now. */
static inline bool SSPageReadCancelAllowed()
{
    return SSNeedExitPageReadInFailover() || SSNeedRetryPageReadInPrimaryRestart();
}

/* Whether the current stack has enabled page-read cancel. */
static inline bool SSPageReadCancelEnabled()
{
    return t_thrd.dms_cxt.enable_page_read_cancel && SSPageReadCancelAllowed();
}

/* Whether a page-read cancel reason has been recorded. */
static inline bool SSPageReadCancelCauseSet()
{
    return t_thrd.dms_cxt.page_read_cancel_cause != SS_PAGE_READ_CANCEL_NONE;
}

/* Whether the recorded cancel reason matches the expected page type. */
static inline bool SSPageReadCancelPending(SSPageReadCancelCause cause)
{
    return ENABLE_DMS && t_thrd.dms_cxt.page_need_retry && t_thrd.dms_cxt.page_read_cancel_cause == cause;
}

/* Whether the pending page-read cancel should be reported as ERROR. */
static inline bool SSNeedExitByPageReadCancel()
{
    return SSPageReadCancelCauseSet() && SSNeedExitPageReadInFailover();
}

/* Clear page-read cancel state in this thread. */
static inline void SSClearPageReadCancel()
{
    t_thrd.dms_cxt.page_need_retry = false;
    t_thrd.dms_cxt.page_read_cancel_cause = SS_PAGE_READ_CANCEL_NONE;
    t_thrd.dms_cxt.page_read_cancel_point = SS_PAGE_READ_CANCEL_POINT_UNKNOWN;
}

#ifdef USE_ASSERT_CHECKING
inline dms_buf_ctrl_t* GetDmsBufCtrl(int id)
{
    Assert(id >= 0);
    return &t_thrd.storage_cxt.dmsBufCtl[(id)];
}

#else
#define GetDmsBufCtrl(id) (&t_thrd.storage_cxt.dmsBufCtl[(id)])

#endif

inline bool SSBufferIsDirty(BufferDesc *buf_desc)
{
    uint64 state = pg_atomic_read_u64(&buf_desc->state);
    // no need to judge (BM_DIRTY | BM_JUST_DIRTIED), BM_DIRTY is enough
    if (state & BM_DIRTY) {
        return true;
    }
    if (ENABLE_DSS_AIO && buf_desc->extra->aio_in_progress) {
        return true;
    }
    return false;
}

#endif
