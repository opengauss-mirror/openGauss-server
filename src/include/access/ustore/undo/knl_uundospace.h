/* -------------------------------------------------------------------------
 *
 * knl_uundospace.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/undo/knl_uundospace.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOSPACE_H__
#define __KNL_UUNDOSPACE_H__

#include "access/ustore/undo/knl_uundotype.h"
#include "storage/lock/lwlock.h"

namespace undo {
#define UNDOSPACE_CLEAN 0
#define UNDOSPACE_DIRTY 1
#define UNDO_SPACE_META_VERSION 1

/* Information about the undospace persistency metadata. */
typedef struct UndoSpaceMetaInfo {
    uint32 version;
    XLogRecPtr lsn;
    UndoRecPtr head;
    UndoRecPtr tail;
} UndoSpaceMetaInfo;

#define UNDOSPACE_COUNT_PER_PAGE ((UNDO_META_PAGE_SIZE - UNDO_META_PAGE_CRC_LENGTH) / sizeof(undo::UndoSpaceMetaInfo))
#define UNDOSPACE_COUNT_PER_WRITE (UNDOSPACE_COUNT_PER_PAGE * 8)

#define UNDOSPACE_META_PAGE_COUNT(total, unit, count)                      \
    do {                                                                   \
        count = (total % unit == 0) ? (total / unit) : (total / unit) + 1; \
    } while (0)

/*
 * UndoSpace class is used as a proxy to manipulate undo zone(segment) file.
 */
class UndoSpace {
public:
    /* Getter. */
    inline UndoLogOffset Head(void)
    {
        return this->head_;
    }
    inline UndoLogOffset Head_exrto(void)
    {
        return this->head_exrto;
    }
    inline UndoLogOffset Tail(void)
    {
        return this->tail_;
    }
    inline XLogRecPtr LSN(void)
    {
        return lsn_;
    }
    uint32 Used(void);

    /* Setter, used for redo. */
    inline void SetHead(UndoRecPtr head)
    {
        this->head_ = head;
    }
    inline void set_head_exrto(UndoRecPtr head)
    {
        this->head_exrto = head;
    }
    inline void SetTail(UndoRecPtr tail)
    {
        this->tail_ = tail;
    }
    inline void SetLSN(XLogRecPtr lsn)
    {
        lsn_ = lsn;
    }

    /* Space lock/unlock. */
    void LockSpace(void)
    {
        (void)LWLockAcquire(lock_, LW_EXCLUSIVE);
    }
    void UnlockSpace(void)
    {
        (void)LWLockRelease(lock_);
    }
    void LockInit(void)
    {
        lock_ = LWLockAssign(LWTRANCHE_UNDO_SPACE);
    }
    /* Extend the end of this undo log to cover newInsert */
    void ExtendUndoLog(int zid, UndoLogOffset offset, uint32 dbId);
    /* Unlink unused undo segment file. */
    void UnlinkUndoLog(int zid, UndoLogOffset offset, uint32 dbId);

    /* Check and set dirty. */
    bool IsDirty(void)
    {
        return dirty_ == UNDOSPACE_DIRTY;
    }
    void MarkClean(void)
    {
        dirty_ = UNDOSPACE_CLEAN;
    }
    void MarkDirty(void)
    {
        dirty_ = UNDOSPACE_DIRTY;
    }
    void CreateNonExistsUndoFile(int zid, uint32 dbId);
    static void CheckPointUndoSpace(int fd, UndoSpaceType type);
    static void RecoveryUndoSpace(int fd, UndoSpaceType type);
    UndoLogOffset find_oldest_offset(int zid, uint32 db_id) const;
    void unlink_residual_log(int zid, UndoLogOffset start, UndoLogOffset end, uint32 db_id) const;

private:
    /* next insertion point (head), this backend is the only one that can modify insert. */
    UndoLogOffset head_;
    /* real next insertion point (head), this backend is the only one that can modify insert. */
    UndoLogOffset head_exrto;
    /* one past end of highest segment, need lock befor modify end. */
    UndoLogOffset tail_;

    /* Lock for space. */
    LWLock *lock_;

    /* Check whether undo zone is dirty. */
    bool dirty_;

    /* Space meta lsn. */
    XLogRecPtr lsn_;
}; /* class UndoSpace */
}

#endif /* __KNL_UUNDOSPACE_H__ */
