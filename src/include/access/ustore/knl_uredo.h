/* -------------------------------------------------------------------------
 *
 * knl_uredo.h
 * the access interfaces of uheap recovery.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uredo.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UREDO_H
#define KNL_UREDO_H


#include "postgres.h"

#include "miscadmin.h"

#include "access/xlog.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"

/*
 * WAL record definitions for uheap WAL operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field.  We use 3 for opcode and one for init bit.
 */
#define XLOG_UHEAP_INSERT 0x00
#define XLOG_UHEAP_DELETE 0x10
#define XLOG_UHEAP_UPDATE 0x20
#define XLOG_UHEAP_FREEZE_TD_SLOT 0x30
#define XLOG_UHEAP_INVALID_TD_SLOT 0x40
#define XLOG_UHEAP_CLEAN 0x50
#define XLOG_UHEAP_MULTI_INSERT 0x60
#define XLOG_UHEAP_OPMASK 0x70
/*
 * When we insert 1st item on new page in INSERT, UPDATE, HOT_UPDATE,
 * or MULTI_INSERT, we can (and we do) restore entire page in redo
 */
#define XLOG_UHEAP_INIT_PAGE 0x80
#define XLOG_UHEAP_INIT_TOAST_PAGE 0x08

/*
 * XlUHeap* ->flag values, 8 bits are available
 */
#define XLOG_UHEAP_CONTAINS_NEW_TUPLE (1 << 4)
#define XLZ_INSERT_IS_FROZEN (1 << 5)
#define XLOG_UHEAP_CONTAINS_OLD_HEADER (1 << 6)
#define XLOG_UHEAP_INSERT_LAST_IN_MULTI (1 << 7)
/*
 * XlUndoHeader -> flage values, 8 bits are available
 */
#define XLOG_UNDO_HEADER_HAS_SUB_XACT (1 << 1)
#define XLOG_UNDO_HEADER_HAS_BLK_PREV (1 << 2)
#define XLOG_UNDO_HEADER_HAS_PREV_URP (1 << 3)
#define XLOG_UNDO_HEADER_HAS_PARTITION_OID (1 << 4)
#define XLOG_UNDO_HEADER_HAS_CURRENT_XID (1 << 5)
#define XLOG_UNDO_HEADER_HAS_TOAST (1 << 6)
/*
 * XlUHeapDelete flag values, 8 bits are available.
 */
/* undo tuple is present in xlog record? */
#define XLZ_HAS_DELETE_UNDOTUPLE (1 << 1)

/* all fields in UHeapDiskTuple */
/* size=8 alignment=2 */
typedef struct XlUHeapHeader {
    uint16 td_id : 8, reserved : 8;
    uint16 flag;
    uint16 flag2;
    uint8 t_hoff;
    char padding;
} XlUHeapHeader;

#define SizeOfUHeapHeader (offsetof(XlUHeapHeader, padding)) // 7 Bytes

/* size=24 alignment=8 */
typedef struct XlUndoHeader {
    UndoRecPtr urecptr; /* location of undo record */
    Oid relOid;         /* relation id */
    uint8 flag;
} XlUndoHeader;

#define SizeOfXLUndoHeader (offsetof(XlUndoHeader, flag) + sizeof(uint8)) // 13 Bytes

struct XlUndoHeaderExtra {
    UndoRecPtr blkprev;
    UndoRecPtr prevurp;
    Oid partitionOid;
    bool hasSubXact;
    uint32 size;
};

/* size=24 alignment=8 */
typedef struct XlUHeapDelete {
    TransactionId oldxid;  /* xid in oldTD, i.e. the xid of last operation on the tuple */
    OffsetNumber offnum;
    uint8 td_id;
    uint8 flag;
    char padding[3];
} XlUHeapDelete;

#define SizeOfUHeapDelete (offsetof(XlUHeapDelete, padding)) // 20 Bytes

/* size=4 alignment=2 */
typedef struct XlUHeapInsert {
    OffsetNumber offnum;
    uint8 flags;
    char padding;
} XlUHeapInsert;

#define SizeOfUHeapInsert (offsetof(XlUHeapInsert, padding)) // 3 Bytes

#define XLZ_UPDATE_PREFIX_FROM_OLD (1 << 0)
#define XLZ_UPDATE_SUFFIX_FROM_OLD (1 << 1)
#define XLZ_NON_INPLACE_UPDATE (1 << 2)
#define XLZ_HAS_UPDATE_UNDOTUPLE (1 << 3)
#define XLZ_LINK_UPDATE (1 << 4)

/* size=24 alignment=8 */
typedef struct XlUHeapUpdate {
    /* UHeap related info */
    TransactionId oldxid;    /* xid in oldTD, i.e. the xid of last operation on the tuple */
    OffsetNumber old_offnum; /* old tuple's offset */
    uint16 old_tuple_flag;
    OffsetNumber new_offnum; /* new tuple's offset */
    uint8 old_tuple_td_id;
    uint8 flags; /* reduced from uint16 */
} XlUHeapUpdate;

#define SizeOfUHeapUpdate (offsetof(XlUHeapUpdate, flags) + sizeof(uint8)) // 24 Bytes

/* size=16 alignment=8 */
typedef struct XlUHeapFreezeTdSlot {
    TransactionId latestFrozenXid; /* latest frozen xid */
    uint16 nFrozen;                /* number of transaction slots to freeze */
    char padding[6];
} XlUHeapFreezeTdSlot;

#define SizeOfUHeapFreezeTDSlot (offsetof(XlUHeapFreezeTdSlot, padding)) // 10 Bytes

/*
 * This is what we need to know about vacuum page cleanup/redirect
 *
 * The array of OffsetNumbers following the fixed part of the record contains:
 * for each redirected item: the item offset, then the offset redirected to
 * for each now-dead item: the item offset for each now-unused item: the item offset
 * The total number of OffsetNumbers is therefore 2*nredirected+ndead+nunused.
 * Note that nunused is not explicitly stored, but may be found by reference to the
 * total record length.
 */
#define XLZ_CLEAN_CONTAINS_OFFSET (1 << 0)
#define XLZ_CLEAN_ALLOW_PRUNING (1 << 1)
#define XLZ_CLEAN_CONTAINS_TUPLEN (1 << 2)

/* size=16 alignment=8 */
typedef struct XlUHeapClean {
    TransactionId latestRemovedXid;
    uint16 ndeleted;
    uint16 ndead;
    uint8 flags;
    char padding[3];
    /* OFFSET NUMBERS are in the block reference 0 */
} XlUHeapClean;

#define SizeOfUHeapClean (offsetof(XlUHeapClean, padding)) // 13 Bytes

typedef struct XlUHeapMultiInsert {
    int ntuples;
    uint8 flags;
} XlUHeapMultiInsert;

#define SizeOfUHeapMultiInsert (offsetof(XlUHeapMultiInsert, flags) + sizeof(uint8))

typedef struct XlMultiInsertUTuple {
    int datalen;
    ShortTransactionId xid;
    uint16 td_id : 8, locker_td_id : 8;
    uint16 flag;
    uint16 flag2;
    uint8 t_hoff;
} XlMultiInsertUTuple;

#define SizeOfMultiInsertUTuple (offsetof(XlMultiInsertUTuple, t_hoff) + sizeof(uint8))

/*
 * WAL record definitions for UStore xid base related operations
 */
#define XLOG_UHEAP2_BASE_SHIFT 0x00
#define XLOG_UHEAP2_FREEZE 0x10
#define XLOG_UHEAP2_EXTEND_TD_SLOTS 0x20
typedef struct XlUHeapBaseShift {
    bool multi;
    int64 delta;
} XlUHeapBaseShift;

#define SizeOfUHeapBaseShift (offsetof(XlUHeapBaseShift, delta) + sizeof(int64))

typedef struct XlUHeapFreeze {
    TransactionId cutoff_xid;
} XlUHeapFreeze;

#define SizeOfUHeapFreeze (offsetof(XlUHeapFreeze, cutoff_xid) + sizeof(TransactionId))

/*
 * WAL record definitions for rollback WAL operations
 */
#define XLOG_UHEAPUNDO_PAGE 0x00
#define XLOG_UHEAPUNDO_RESET_SLOT 0x10
#define XLOG_UHEAPUNDO_ABORT_SPECINSERT 0x20

/*
 * xl_undoaction_page flag values, 8 bits are available.
 */
#define XLU_INIT_PAGE (1 << 0)

/* This is used to write WAL for undo actions */
typedef struct UHeapUndoActionWALInfo {
    Buffer buffer;
    OffsetNumber xlogMinLPOffset;
    OffsetNumber xlogMaxLPOffset;
    Offset xlogCopyStartOffset;
    Offset xlogCopyEndOffset;
    int tdSlotId;
    TransactionId xid;
    UndoRecPtr slotPrevUrp;
    TransactionId pd_prune_xid;
    uint16 pd_flags;
    uint16 potential_freespace;
    bool needInit;
} UHeapUndoActionWALInfo;

#define SizeOfUHeapUndoActionWALInfo (offsetof(UHeapUndoActionWALInfo, needInit) + sizeof(bool))

/*
 * XlUHeapUndoAbortSpecInsert flag values, 8 bits are available
 */
#define XLU_ABORT_SPECINSERT_INIT_PAGE (1 << 0)
#define XLU_ABORT_SPECINSERT_XID_VALID (1 << 1)
#define XLU_ABORT_SPECINSERT_PREVURP_VALID (1 << 2)
#define XLU_ABORT_SPECINSERT_REL_HAS_INDEX (1 << 3)

typedef struct XlUHeapUndoAbortSpecInsert {
    OffsetNumber offset;
    int zone_id;
} XlUHeapUndoAbortSpecInsert;

#define SizeOfUHeapUndoAbortSpecInsert (offsetof(XlUHeapUndoAbortSpecInsert, zone_id) + sizeof(int))

typedef struct XlUHeapUndoResetSlot {
    UndoRecPtr urec_ptr;
    int zone_id;
    int td_slot_id; /* td transaction slot id */
} XlUHeapUndoResetSlot;

#define SizeOfUHeapUndoResetSlot (offsetof(XlUHeapUndoResetSlot, td_slot_id) + sizeof(int))

typedef struct XlUHeapExtendTdSlots {
    uint8   nPrevSlots; /* Previous number of TD slots */
    uint8   nExtended; /* Number of slots after extension */
} XlUHeapExtendTdSlots;
#define SizeOfUHeapExtendTDSlot        (offsetof(XlUHeapExtendTdSlots, nExtended) + sizeof(uint8))

extern void UHeapRedo(XLogReaderState *record);
extern void UHeapDesc(StringInfo buf, XLogReaderState *record);
extern const char* uheap_type_name(uint8 subtype);
extern char* GetUndoHeader(XlUndoHeader *xlundohdr, Oid *partitionOid, UndoRecPtr* blkprev);
extern void UHeap2Redo(XLogReaderState *record);
extern void UHeap2Desc(StringInfo buf, XLogReaderState *record);
extern const char* uheap2_type_name(uint8 subtype);
extern void UHeapUndoRedo(XLogReaderState *record);
extern void UHeapUndoDesc(StringInfo buf, XLogReaderState *record);
extern TransactionId UHeapXlogGetCurrentXid(XLogReaderState *record, bool hasCSN);
extern const char* uheap_undo_type_name(uint8 subtype);
extern TransactionId UHeapXlogGetCurrentXid(XLogReaderState *record);
#endif
