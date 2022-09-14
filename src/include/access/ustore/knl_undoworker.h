/* -------------------------------------------------------------------------
 *
 * knl_undoworker.h
 * access interfaces of the async undo worker for the ustore engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * opengauss_server/src/include/access/ustore/knl_undoworker.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UNDOWORKER_H
#define KNL_UNDOWORKER_H

#include "access/ustore/undo/knl_uundotype.h"

#define MAX_UNDO_WORKERS 100

typedef struct UndoWorkInfoData {
    TransactionId xid;
    UndoRecPtr startUndoPtr;
    UndoRecPtr endUndoPtr;
    Oid dbid;
    UndoSlotPtr slotPtr;
} UndoWorkInfoData;

typedef UndoWorkInfoData *UndoWorkInfo;

typedef struct UndoWorkerItem {
    ThreadId pid;
    TransactionId xid;
    UndoRecPtr startUndoPtr;
    TimestampTz rollbackStartTime;
} UndoWorkerItem;

/* -------------
 * This main purpose of this shared memory is for the undo launcher and
 * undo worker to communicate.
 *
 * undo_launcher_pid   - Thread Id of the Undo launcher
 * rollback_request    - Current rollback request that needs to be picked up
 * by an Undo worker
 * active_undo_workers - Current active Undo workers. The launcher cannot launch
 * a new undo worker if active_undo_workers is maxed out.
 *
 * -------------
 */
typedef struct UndoWorkerShmemStruct {
    /* Latch used by backends to wake the undo launcher when it has work to do */
    Latch latch;

    ThreadId undo_launcher_pid;
    UndoWorkInfo rollback_request;
    uint32 active_undo_workers;
    UndoWorkerItem undo_worker_status[MAX_UNDO_WORKERS];
} UndoWorkerShmemStruct;


#ifdef EXEC_BACKEND
extern void UndoLauncherMain();
extern void UndoWorkerMain();
#endif

bool IsUndoWorkerProcess(void);

/* shared memory specific */
extern Size UndoWorkerShmemSize(void);
extern void UndoWorkerShmemInit(void);

#endif
