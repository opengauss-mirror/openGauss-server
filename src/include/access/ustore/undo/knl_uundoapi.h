/* -------------------------------------------------------------------------
 *
 * knl_uundoapi.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/undo/knl_uundoapi.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOAPI_H__
#define __KNL_UUNDOAPI_H__

#include "c.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundotype.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/undo/knl_uundozone.h"

namespace undo {
bool CheckNeedSwitch(UndoPersistence upersistence, uint64 size, UndoRecPtr undoPtr = INVALID_ZONE_ID);

void RollbackIfUndoExceeds(TransactionId xid, uint64 size);

UndoRecPtr AllocateUndoSpace(TransactionId xid, UndoPersistence upersistence, uint64 size,
    bool needSwitch, XlogUndoMeta *xlundometa);

void UndoRecycleMain();

bool IsSkipInsertUndo(UndoRecPtr urp);
bool IsSkipInsertSlot(UndoSlotPtr urp);

/* Check undo record valid.. */
bool CheckUndoRecordValid(UndoRecPtr urp, bool checkForceRecycle);

/* Check undo record recovery status for ROS */
UndoRecoveryStatus CheckUndoRecordRecoveryStatus(UndoRecPtr urp);

void CheckPointUndoSystemMeta(XLogRecPtr checkPointRedo);

void RecoveryUndoSystemMeta();

void AllocateUndoZone();

void UpdateRollbackFinish(UndoSlotPtr slotPtr);
void RedoRollbackFinish(UndoSlotPtr slotPtr, XLogRecPtr lsn);

void UndoLogInit();
void OnUndoExit(int code, Datum arg);

UndoRecPtr AdvanceUndoPtr(UndoRecPtr undoPtr, uint64 size);

void PrepareUndoMeta(XlogUndoMeta *meta, UndoPersistence upersistence, 
    UndoRecPtr lastRecord, UndoRecPtr lastRecordSize);
void FinishUndoMeta(XlogUndoMeta *meta, UndoPersistence upersistence);
void UpdateTransactionSlot(TransactionId xid, XlogUndoMeta *meta, UndoRecPtr startUndoPtr,
    UndoPersistence upersistence);
void SetUndoMetaLSN(XlogUndoMeta *meta, XLogRecPtr lsn);
void RedoUndoMeta(XLogReaderState *record, XlogUndoMeta *meta, UndoRecPtr startUndoPtr, 
    UndoRecPtr lastRecord, uint32 lastRecordSize);
void ReleaseSlotBuffer();

UndoRecPtr GetPrevUrp(UndoRecPtr currUrp);
} // namespace undo

extern void GetUndoFileDirectory(char *path, int len, UndoPersistence upersistence);

#endif // __KNL_UUNDOAPI_H__
