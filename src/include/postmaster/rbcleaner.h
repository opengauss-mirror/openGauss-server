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
 * -------------------------------------------------------------------------
 * rbcleaner.h
 * 
 * IDENTIFICATION
 *        src/include/postmaster/rbcleaner.h
 * 
 * ---------------------------------------------------------------------------------------
 */

#ifndef RBCLEANER_H
#define RBCLEANER_H

typedef enum PurgeMsgType {
    PURGE_MSG_TYPE_INVALID        = 0,
    PURGE_MSG_TYPE_DML            = 1,  /* when execute dml no space. */
    PURGE_MSG_TYPE_TABLESPACE     = 2,  /* purge tablespace. */
    PURGE_MSG_TYPE_CAS_TABLESPACE = 3,  /* drop tablespace. */
    PURGE_MSG_TYPE_RECYCLEBIN     = 4,  /* purge recyclebin. */
    PURGE_MSG_TYPE_CAS_SCHEMA     = 5,  /* dorp schema. */
    PURGE_MSG_TYPE_CAS_USER       = 6,  /* drop user. */
    PURGE_MSG_TYPE_AUTO           = 7,  /* auto purge. */
} PurgeMsgType;

typedef enum PurgeMsgStatus {
    /* Set by message client */
    PURGE_MSG_STATUS_CREATED      = 0,

    /* Set by rbworker, indicates work start normal. */
    PURGE_MSG_STATUS_STEPIN       = 1,

    /* Set by rbworker, indicates work done. */
    PURGE_MSG_STATUS_STEPDONE     = 2,

    /* Set by rbcleaner, indicates purge command done. */
    PURGE_MSG_STATUS_ERROR        = 3,
    PURGE_MSG_STATUS_SUCCESS      = 4,

    /* Used for local PurgeMsgRes to indicate overrided. */
    PURGE_MSG_STATUS_OVERRIDE     = 5,
} PurgeMsgStatus;

#define RbMsgIsDone(status) ((status) >= PURGE_MSG_STATUS_ERROR)

typedef struct PurgeMsgReq {
    PurgeMsgType type;
    Oid authId;
    Oid objId;
    Oid dbId;
    bool cancel;
} PurgeMsgReq;

#define RB_MAX_ERRMSG_SIZE 256
typedef struct PurgeMsgRes {
    PurgeMsgStatus status;

    /* Count of purged rb obj successfully. */
    uint32 purgedNum;
    /* Count of rb obj lock conflicted. */
    uint32 skippedNum;
    /* Count of rb obj not exists during to concurrent purge. */
    uint32 undefinedNum;

    /* Unexcepted ERROR except for `lock conflicted` and `obj not exists`. */
    int errCode; 
    char errMsg[RB_MAX_ERRMSG_SIZE];
} PurgeMsgRes;

#define RbResIsError(errCode) ((errCode) != ERRCODE_SUCCESSFUL_COMPLETION)
#define RB_INVALID_MSGID 0

typedef struct PurgeMsg {
    /* Auto-increment id as unique message Id. */
    uint64 id;

    /* Message body: DML, [cas] tablespace, recyclebin, cas schema, cas user. */
    PurgeMsgReq req;

    /* Message handle result. */
    PurgeMsgRes res;

    /* Spinlock to protect above access, the whole signal one request. */
    slock_t mutex;

    /* postgres wait `latch` until rbcleaner set. */
    Latch latch;  /* the request from whom */
} PurgeMsg;

/**
  * ==================================
  * functions for signal one PurgeMsg
  * ==================================
  */

static inline void RbMsgInit(PurgeMsg *msg, PurgeMsgType type, 
    Oid objId = InvalidOid, Oid dbId = InvalidOid)
{
    errno_t rc = memset_s(msg, sizeof(PurgeMsg), 0, sizeof(PurgeMsg));
    securec_check(rc, "\0", "\0");

    msg->req.type = type;
    msg->req.authId = GetUserId();
    msg->req.objId = objId;
    msg->req.dbId = dbId;
}

static inline void RbMsgResetRes(PurgeMsgRes *res)
{
    errno_t rc = memset_s(res, sizeof(PurgeMsgRes), 0, sizeof(PurgeMsgRes));
    securec_check(rc, "\0", "\0");
}

static inline void RbMsgCopyReq(PurgeMsgReq *dst, PurgeMsgReq *src)
{
    errno_t rc = memcpy_s(dst, sizeof(PurgeMsgReq), src, sizeof(PurgeMsgReq));
    securec_check(rc, "\0", "\0");
}

static inline void RbMsgCopyRes(PurgeMsgRes *dst, PurgeMsgRes *res)
{
    errno_t rc = memcpy_s(dst, sizeof(PurgeMsgRes), res, sizeof(PurgeMsgRes));
    securec_check(rc, "\0", "\0");
}

#define RB_MAX_MSGQ_SIZE 8192
typedef struct PurgeMsgQueue {
    PurgeMsg item[RB_MAX_MSGQ_SIZE + 1];

    uint32 head;
    uint32 tail;

    uint64 nextMsgId;

    slock_t mutex;        /* protect all the queue */
} PurgeMsgQueue;

/**
  * ==================================
  * functions for PurgeMsg queue
  * ==================================
  */
static inline void RbQueueInit(PurgeMsgQueue *q)
{
    q->head = 1;
    q->tail = 1;
    q->nextMsgId = 1;
    SpinLockInit(&q->mutex);
    for (int i = 0; i < RB_MAX_MSGQ_SIZE + 1; i++) {
        SpinLockInit(&q->item[i].mutex);
    }
}

static inline PurgeMsg *RbQueueNext(PurgeMsgQueue *q)
{
    return &q->item[q->tail];
}

static inline PurgeMsg *RbQueueFront(PurgeMsgQueue *q)
{
    return &q->item[q->head];
}

static inline void RbQueuePop(PurgeMsgQueue *q)
{
    SpinLockAcquire(&q->mutex);
    q->head = (q->head + 1) % RB_MAX_MSGQ_SIZE;
    SpinLockRelease(&q->mutex);
}

static inline void RbQueuePush(PurgeMsgQueue *q)
{
    q->tail = (q->tail + 1) % RB_MAX_MSGQ_SIZE;
}

static inline bool RbQueueIsFull(PurgeMsgQueue *q)
{
    return (q->tail + 1) % RB_MAX_MSGQ_SIZE == q->head;
}

static inline bool RbQueueIsEmpty(PurgeMsgQueue *q)
{
    bool isEmpty = false;

    SpinLockAcquire(&q->mutex);
    isEmpty = q->head == q->tail;
    SpinLockRelease(&q->mutex);

    return isEmpty;
}

typedef struct RbWorkerInfo {
    uint64 id;
    NameData dbName;
    ThreadId rbworkerPid; /* PID (0 if not started) */
    Latch latch;
} RbWorkerInfo;

typedef struct RbCleanerShmemStruct {
    ThreadId rbCleanerPid; /* PID (0 if not started) */
    Latch *rbCleanerLatch;

    PurgeMsgQueue queue;

    RbWorkerInfo workerInfo;
} RbCleanerShmemStruct;

/**
  * ==================================
  * functions for RbCleanerShmemStruct
  * ==================================
  */

#define RbGetWorkerInfo() (&t_thrd.rbcleaner_cxt.RbCleanerShmem->workerInfo)
#define RbGetQueue() (&t_thrd.rbcleaner_cxt.RbCleanerShmem->queue)
#define RbGetNextMsgId() (t_thrd.rbcleaner_cxt.RbCleanerShmem->queue.nextMsgId++)

static inline RbWorkerInfo *RbInitWorkerInfo(uint64 id, char *dbName)
{
    RbWorkerInfo *workerInfo = RbGetWorkerInfo();
    errno_t rc;

    workerInfo->id = id;
    rc = strcpy_s(NameStr(workerInfo->dbName), NAMEDATALEN, dbName);
    securec_check(rc, "\0", "\0");

    InitLatch(&workerInfo->latch);

    return workerInfo;
}

static inline PurgeMsg *RbMsg(uint64 id)
{
    return &t_thrd.rbcleaner_cxt.RbCleanerShmem->queue.item[(id) % RB_MAX_MSGQ_SIZE];
}

static inline bool RbMsgStepIn(uint64 id)
{
    PurgeMsg *rbMsg = RbMsg(id);
    return (*(volatile PurgeMsgStatus *)&rbMsg->res.status >= PURGE_MSG_STATUS_STEPIN);
}

static inline bool RbMsgStepDone(uint64 id)
{
    PurgeMsg *rbMsg = RbMsg(id);
    return (*(volatile PurgeMsgStatus *)&rbMsg->res.status == PURGE_MSG_STATUS_STEPDONE);
}

static inline bool RbMsgCanceled(uint64 id)
{
    PurgeMsg *rbMsg = RbMsg(id);
    return (*(volatile bool *)&rbMsg->req.cancel);
}

static inline void RbMsgGetRes(uint64 id, PurgeMsgRes *localRes, bool reset = false)
{
    PurgeMsg *rbMsg = RbMsg(id);

    RbMsgResetRes(localRes);
    SpinLockAcquire(&rbMsg->mutex);
    if (rbMsg->id != id) {
        localRes->status = PURGE_MSG_STATUS_OVERRIDE;
    } else {
        errno_t rc = 0;
        rc = memcpy_s(localRes, sizeof(PurgeMsgRes), &rbMsg->res, sizeof(PurgeMsgRes));
        securec_check(rc, "\0", "\0");
    }
    if (reset && RbMsgIsDone(rbMsg->res.status)) {
        rbMsg->id = RB_INVALID_MSGID;
    }
    SpinLockRelease(&rbMsg->mutex);
}

static inline void RbMsgSetStatus(uint64 id, PurgeMsgStatus status, bool setLatch = false)
{
    PurgeMsg *rbMsg = RbMsg(id);
    SpinLockAcquire(&rbMsg->mutex);
    rbMsg->res.status = status;
    if (setLatch) {
        SetLatch(&rbMsg->latch);
    }
    SpinLockRelease(&rbMsg->mutex);
}

/* Collect statistics after transaction committed. */
static inline void RbMsgSetStatistics(uint64 id, PurgeMsgRes *localRes)
{
    PurgeMsg *rbMsg = RbMsg(id);
    errno_t rc = EOK;

    if (rbMsg->id != id) {
        ereport(LOG, (errmodule(MOD_TIMECAPSULE),
            errmsg("before mutex rbMsg id is %lu, workerinfo id is %lu", rbMsg->id, id)));
    }
 
    SpinLockAcquire(&rbMsg->mutex);
    if (rbMsg->id != id) {
        ereport(LOG, (errmodule(MOD_TIMECAPSULE),
            errmsg("after mutex rbMsg id is %lu, workerinfo id is %lu", rbMsg->id, id)));
    }
    Assert(id == rbMsg->id);
    rbMsg->res.purgedNum += localRes->purgedNum;
    rbMsg->res.skippedNum += localRes->skippedNum;
    rbMsg->res.undefinedNum += localRes->undefinedNum;
    if (localRes->errMsg[0] != '\0') {
        rc = strncpy_s(rbMsg->res.errMsg, RB_MAX_ERRMSG_SIZE, localRes->errMsg, RB_MAX_ERRMSG_SIZE - 1);
        securec_check(rc, "\0", "\0");
    }
    SpinLockRelease(&rbMsg->mutex);
}

static inline void RbMsgSetStatusErr(uint32 id, PurgeMsgStatus status, 
    int errCode, const char *errMsg, bool setLatch = false)
{
    PurgeMsg *rbMsg = RbMsg(id);
    errno_t rc;

    SpinLockAcquire(&rbMsg->mutex);
    rbMsg->res.status = status;
    rbMsg->res.errCode = errCode;
    rc = strncpy_s(rbMsg->res.errMsg, RB_MAX_ERRMSG_SIZE, errMsg, RB_MAX_ERRMSG_SIZE - 1);
    securec_check(rc, "\0", "\0");

    if (setLatch) {
        SetLatch(&rbMsg->latch);
    }
    SpinLockRelease(&rbMsg->mutex);
}

static inline void RbMsgSetStatusErrLocal(PurgeMsgRes *localRes, PurgeMsgStatus status, 
    int errCode, const char *errMsg)
{
    errno_t rc = EOK;

    localRes->status = status;
    localRes->errCode = errCode;
    rc = strncpy_s(localRes->errMsg, RB_MAX_ERRMSG_SIZE, errMsg, RB_MAX_ERRMSG_SIZE - 1);
    securec_check(rc, "\0", "\0");

}

extern void RbCltPurgeSchema(Oid nspId);
extern void RbCltPurgeUser(Oid roleId);
extern void RbCltPurgeRecyclebin();
extern void RbCltPurgeSpace(Oid spcId);
extern bool RbCltPurgeSpaceDML(Oid spcId);
extern void RbCltPurgeDatabase(Oid dbId);

extern ThreadId StartRbCleaner(void);
extern bool IsRbCleanerProcess(void);
extern Size RbCleanerShmemSize(void);
extern void RbCleanerShmemInit(void);
extern NON_EXEC_STATIC void RbCleanerMain();

bool IsRbWorkerProcess(void);
NON_EXEC_STATIC void RbWorkerMain();

#define ENABLE_TCAP_RECYCLEBIN (u_sess->attr.attr_storage.enable_recyclebin)

#endif
