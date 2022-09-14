/* -------------------------------------------------------------------------
 *
 * knl_uundoxlog.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/undo/knl_uundoxlog.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOXLOG_H__
#define __KNL_UUNDOXLOG_H__

#include "access/ustore/undo/knl_uundotype.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/xlog_basic.h"
#include "lib/stringinfo.h"

namespace undo {
/* xlog type of extend undo */
#define XLOG_UNDO_EXTEND 0x00
/* xlog type of unlink undo */
#define XLOG_UNDO_UNLINK 0x10
/* xlog type of clean undo */
#define XLOG_SLOT_EXTEND 0x30
/* xlog type of clean undo */
#define XLOG_SLOT_UNLINK 0x40
/* xlog type of discard undo */
#define XLOG_UNDO_DISCARD 0x60

#define XLOG_ROLLBACK_FINISH 0x00

#define XLOG_UNDOMETA_INFO_SLOT 0x01
#define XLOG_UNDOMETA_INFO_SWITCH 0x02
#define XLOG_UNDOMETA_INFO_SKIP 0x04

/* Alloc undospace meta info. */
typedef struct XlogUndoMeta {
    XlogUndoMeta():info(0), slotPtr(INVALID_UNDO_REC_PTR), lastRecordSize(0), dbid(InvalidOid){}
    uint8 info : 4;
    UndoSlotOffset slotPtr : 44;
    uint16 lastRecordSize : 16;
    Oid   dbid;
    bool IsTranslot() const
    {
        return info & XLOG_UNDOMETA_INFO_SLOT;
    }
    bool IsSwitchZone() const
    {
        return info & XLOG_UNDOMETA_INFO_SWITCH;
    }
    bool IsSkipInsert() const
    {
        return info & XLOG_UNDOMETA_INFO_SKIP;
    }
    void SetInfo(uint8 minfo)
    {
        info |= minfo;
    }
    uint32 Size() const
    {
        if (IsTranslot()) {
            return offsetof(struct XlogUndoMeta, dbid) + sizeof(Oid);
        } else {
            return offsetof(struct XlogUndoMeta, dbid);
        }
    }
} XlogUndoMeta;

void CopyUndoMeta(const XlogUndoMeta &src, XlogUndoMeta &dest);

/* Discard undo meta info. */
typedef struct XlogUndoDiscard {
    UndoSlotPtr endSlot;   // transaction slot recycle end point
    UndoSlotPtr startSlot; // transaction slot recycle start point
    uint64 recycleLoops;
    UndoRecPtr endUndoPtr;
    TransactionId recycledXid;
    TransactionId oldestXmin;
} XlogUndoDiscard;

/* Extend undo log info. */
typedef struct XlogUndoExtend {
    XlogUndoExtend() : prevtail(INVALID_UNDO_REC_PTR), tail(INVALID_UNDO_REC_PTR) {}
    UndoRecPtr prevtail;
    UndoRecPtr tail;
} XlogUndoExtend;

/* Unlink undo log info. */
typedef struct XlogUndoUnlink {
    XlogUndoUnlink() : head(INVALID_UNDO_REC_PTR), prevhead(INVALID_UNDO_REC_PTR) {}
    UndoRecPtr head;
    UndoRecPtr prevhead;
} XlogUndoUnlink;

typedef struct XlogRollbackFinish {
    UndoSlotPtr slotPtr;
} XlogRollbackFinish;

extern void UndoXlogRedo(XLogReaderState *record);
extern void UndoXlogDesc(StringInfo buf, XLogReaderState *record);
extern const char* undo_xlog_type_name(uint8 subtype);
extern void UndoXlogRollbackFinishRedo(XLogReaderState *record);
extern void UndoXlogRollbackFinishDesc(StringInfo buf, XLogReaderState *record);
extern const char* undo_xlog_roll_back_finish_type_name(uint8 subtype);
XLogRecPtr WriteUndoXlog(void *xlrec, uint8 type);
void LogUndoMeta(const XlogUndoMeta *xlum);
}

#endif // __KNL_UUNDOXLOG_H__
