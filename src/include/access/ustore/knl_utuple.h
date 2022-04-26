/* -------------------------------------------------------------------------
 *
 * knl_utuple.h
 * the row format of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_utuple.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UTUPLE_H
#define KNL_UTUPLE_H

#include "access/tupdesc.h"
#include "access/rmgr.h"

#include "access/ustore/knl_utype.h"
#include "storage/item/itemptr.h"
#include "access/xlog.h"

struct TupleTableSlot;

#define InvalidTDSlotId (-1)

/* we use frozen slot to indicate that the tuple is all visible now */
#define UHEAPTUP_SLOT_FROZEN 0x000

#define UHEAP_HAS_NULL 0x0001
#define UHEAP_HASVARWIDTH 0x0002 /* has variable-width attribute(s) */
#define UHEAP_HASEXTERNAL 0x0004 /* has external stored attribute(s) */
/* unused bits */
#define UHEAP_DELETED 0x0010         /* tuple deleted */
#define UHEAP_INPLACE_UPDATED 0x0020 /* tuple is updated inplace */
#define UHEAP_UPDATED 0x0040         /* tuple is not updated inplace */

#define UHEAP_XID_KEYSHR_LOCK 0x0100     /* xid is a key-shared locker */
#define UHEAP_XID_NOKEY_EXCL_LOCK 0x0200 /* xid is a nokey-exclusive locker */

/* xid is a shared locker */
#define UHEAP_XID_SHR_LOCK (UHEAP_XID_NOKEY_EXCL_LOCK | UHEAP_XID_KEYSHR_LOCK)

#define UHEAP_XID_EXCL_LOCK 0x0400         /* tuple was updated and key cols \
                                            * modified, or tuple deleted */
#define UHEAP_MULTI_LOCKERS 0x0800         /* tuple was locked by multiple \
                                            * lockers */
#define UHEAP_INVALID_XACT_SLOT 0x1000     /* transaction slot on tuple got \
                                            * reused */
#define SINGLE_LOCKER_XID_IS_LOCK 0x2000 /* locked from SELECT FOR UPDATE/SHARE */
#define SINGLE_LOCKER_XID_IS_SUBXACT 0x4000
#define SINGLE_LOCKER_INFOMASK (SINGLE_LOCKER_XID_IS_LOCK | UHEAP_XID_EXCL_LOCK | UHEAP_XID_SHR_LOCK)
#define SINGLE_LOCKER_XID_EXCL_LOCK (SINGLE_LOCKER_XID_IS_LOCK | UHEAP_XID_EXCL_LOCK)
#define SINGLE_LOCKER_XID_SHR_LOCK (SINGLE_LOCKER_XID_IS_LOCK | UHEAP_XID_SHR_LOCK)

#define UHEAP_MOVED                                   \
    (UHEAP_DELETED | UHEAP_UPDATED) /* moved tuple to \
                                     * another partition */
#define UHEAP_LOCK_MASK (UHEAP_XID_SHR_LOCK | UHEAP_XID_EXCL_LOCK)

#define UHEAP_VIS_STATUS_MASK 0x7FF0 /* mask for visibility bits (5 ~ 14 \
                                      * bits) */
#define UHEAP_LOCK_STATUS_MASK                                                                       \
    (UHEAP_XID_KEYSHR_LOCK | UHEAP_XID_NOKEY_EXCL_LOCK | UHEAP_XID_EXCL_LOCK | UHEAP_MULTI_LOCKERS)


/*
 * Use these to test whether a particular lock is applied to a tuple
 */
#define SINGLE_LOCKER_XID_IS_EXCL_LOCKED(infomask) ((infomask & SINGLE_LOCKER_INFOMASK) == SINGLE_LOCKER_XID_EXCL_LOCK)
#define SINGLE_LOCKER_XID_IS_SHR_LOCKED(infomask) ((infomask & SINGLE_LOCKER_INFOMASK) == SINGLE_LOCKER_XID_SHR_LOCK)

#define UHEAP_XID_IS_LOCK(infomask) (((infomask) & SINGLE_LOCKER_XID_IS_LOCK) != 0)
#define UHEAP_XID_IS_SHR_LOCKED(infomask) (((infomask)&UHEAP_LOCK_MASK) == UHEAP_XID_SHR_LOCK)
#define UHEAP_XID_IS_EXCL_LOCKED(infomask) (((infomask)&UHEAP_LOCK_MASK) == UHEAP_XID_EXCL_LOCK)
#define UHeapTupleHasExternal(tuple) (((tuple)->disk_tuple->flag & UHEAP_HASEXTERNAL) != 0)

#define UHeapTupleHasMultiLockers(infomask) (((infomask)&UHEAP_MULTI_LOCKERS) != 0)

#define UHeapTupleIsInPlaceUpdated(infomask) ((infomask & UHEAP_INPLACE_UPDATED) != 0)

#define UHeapTupleIsUpdated(infomask) ((infomask & UHEAP_UPDATED) != 0)

#define UHeapTupleIsMoved(infomask) ((infomask & UHEAP_MOVED) == UHEAP_MOVED)

#define UHeapTupleHasInvalidXact(infomask) ((infomask & UHEAP_INVALID_XACT_SLOT) != 0)

#define UHeapDiskTupHasNulls(_udisk_tuple) (((_udisk_tuple)->flag & UHEAP_HAS_NULL) != 0)

/*
 * information stored in t_infomask_locker
 */
#define UHEAP_XACT_SLOT 0x00FF /* 8 bits for transaction slot */
#define UHeapTupleHeaderGetXactSlot(tup) (((tup)->locker_td_id & UHEAP_XACT_SLOT))


#define UHeapDiskTupNoNulls(_udisk_tuple) (((_udisk_tuple)->flag & UHEAP_HAS_NULL) == 0)

#define UHeapDiskTupSetHasNulls(_udisk_tuple) ((_udisk_tuple)->flag |= UHEAP_HAS_NULL)

#define UHeapDiskTupHasVarWidth(_udisk_tuple) (((_udisk_tuple)->flag & UHEAP_HASVARWIDTH) != 0)

#define UHeapDiskTupleDeleted(_udisk_tuple) (((_udisk_tuple)->flag & (UHEAP_DELETED | UHEAP_UPDATED)) != 0)

#define UHeapTupleHeaderGetTDSlot(udisk_tuple) ((udisk_tuple)->td_id)

#define UHeapTupleHeaderSetTDSlot(udisk_tuple, slot_num) ((udisk_tuple)->td_id = ((uint8)slot_num))

#define UHeapTupleHeaderGetLockerTDSlot(udisk_tuple) ((udisk_tuple)->locker_td_id)

#define UHeapTupleHeaderSetLockerTDSlot(udisk_tuple, slot_num) ((udisk_tuple)->locker_td_id = ((uint8)slot_num))

#define UHeapTupleHeaderSetMovedPartitions(udisk_tuple) ((udisk_tuple)->flag |= UHEAP_MOVED)

#define UHeapTupleHeaderClearSingleLocker(utuple)                                         \
    do {                                                                                  \
        (utuple)->xid = (ShortTransactionId)FrozenTransactionId;                         \
        (utuple)->flag &= ~(SINGLE_LOCKER_XID_IS_LOCK | SINGLE_LOCKER_XID_IS_SUBXACT);   \
    } while (0)

#define IsUHeapTupleModified(infomask) \
    ((infomask & (UHEAP_DELETED | UHEAP_UPDATED | UHEAP_INPLACE_UPDATED)) != 0)

/* UHeap tuples do not have the idea of xmin/xmax but a single XID */
#define UHEAP_XID_COMMITTED 0x0800
#define UHEAP_XID_INVALID 0x1000
#define UHEAP_XID_FROZEN (UHEAP_XID_COMMITTED | UHEAP_XID_INVALID)
#define UHEAP_XID_STATUS_MASK UHEAP_XID_FROZEN

/*
 * Use these to test whether a particular lock is applied to a tuple
 */
#define UHEAP_XID_IS_KEYSHR_LOCKED(infomask) (((infomask)&UHEAP_LOCK_MASK) == UHEAP_XID_KEYSHR_LOCK)
#define UHEAP_XID_IS_NOKEY_EXCL_LOCKED(infomask) (((infomask)&UHEAP_LOCK_MASK) == UHEAP_XID_NOKEY_EXCL_LOCK)
#define UHEAP_XID_IS_SHR_LOCKED(infomask) (((infomask)&UHEAP_LOCK_MASK) == UHEAP_XID_SHR_LOCK)
#define UHEAP_XID_IS_EXCL_LOCKED(infomask) (((infomask)&UHEAP_LOCK_MASK) == UHEAP_XID_EXCL_LOCK)

/* Information stored in flag2 */
#define UHEAP_NATTS_MASK 0x07FF /* 11 bits for number of attributes */

#define UHeapTupleHeaderGetNatts(tup) (((tup)->flag2 & UHEAP_NATTS_MASK))

#define UHeapTupleHeaderSetNatts(tup, natts) ((tup)->flag2 = ((tup)->flag2 & ~UHEAP_NATTS_MASK) | (natts))

typedef struct UHeapDiskTupleData {
    ShortTransactionId xid;
    uint16 td_id : 8, locker_td_id : 8; /* Locker as well as the last updater, 8 bits each */
    uint16 flag;                          /* Flag for tuple attributes */
    uint16 flag2;                         /* Number of attributes for now(11 bits) */
    uint8 t_hoff;                         /* Sizeof header incl. bitmap, padding */
    uint8 data[FLEXIBLE_ARRAY_MEMBER];
    /* Followed by isnull array and col data. */
} UHeapDiskTupleData;

typedef UHeapDiskTupleData *UHeapDiskTuple;

#define SizeOfUHeapDiskTupleData (offsetof(UHeapDiskTupleData, data))
#define SizeOfUHeapDiskTupleTillThoff (offsetof(UHeapDiskTupleData, t_hoff))
#define OffsetTdId (sizeof(ShortTransactionId))
#define SizeOfUHeapDiskTupleHeaderExceptXid (SizeOfUHeapDiskTupleData - OffsetTdId)

static inline bool NAttrsReserveSpace(int nattrs)
{
    return ((nattrs) <= (int)((255 - SizeOfUHeapDiskTupleData) * 8 /2));
}

// Waste 6 bytes due to padding
// Note: Use uheaptup_alloc to allocate the memory for UHeapTuple instead of palloc.
typedef struct UHeapTupleData {
    uint32               disk_tuple_size;
    uint1                tupTableType = UHEAP_TUPLE;
    uint1                tupInfo;
    int2                 t_bucketId;
    ItemPointerData      ctid;
    Oid                  table_oid;
    TransactionId        t_xid_base;
    TransactionId        t_multi_base;
#ifdef PGXC
    uint32 xc_node_id; /* Data node the tuple came from */
#endif
    UHeapDiskTupleData*  disk_tuple;
} UHeapTupleData;

typedef void *TupData;

typedef UHeapTupleData *UHeapTuple;

inline UHeapTuple uheaptup_alloc(Size size)
{
    UHeapTuple tup = (UHeapTuple)palloc0(size);
    tup->t_bucketId = InvalidBktId;
    tup->tupTableType = UHEAP_TUPLE;
    return tup;
}

/*
 * Possible lock modes for a tuple.
 */
typedef enum LockOper {
    /* SELECT FOR 'KEY SHARE/SHARE/NO KEY UPDATE/UPDATE' */
    LockOnly,
    /* Via EvalPlanQual where after locking we will update it */
    LockForUpdate,
    /* Update/Delete */
    ForUpdate
} LockOper;

#define UHEAP_SPECIAL_SIZE (0)
#define UHeapDiskTupleDataHeaderSize (offsetof(UHeapDiskTupleData, data))
#define UHeapTupleDataSize (sizeof(UHeapTupleData))
#define DefaultTdMaxUHeapTupleSize                                                                     \
    (BLCKSZ - MAXALIGN(SizeOfUHeapPageHeaderData + UHEAP_DEFAULT_TD * sizeof(TD) +                    \
    sizeof(ItemIdData) + UHEAP_SPECIAL_SIZE))
#define MaxPossibleUHeapTupleSize                                                                       \
    (BLCKSZ - MAXALIGN(SizeOfUHeapPageHeaderData + UHEAP_MIN_TD * sizeof(TD) +                        \
    sizeof(ItemIdData) + UHEAP_SPECIAL_SIZE))
#define MaxUHeapTupleSize(relation)                                                                     \
    (BLCKSZ - MAXALIGN(SizeOfUHeapPageHeaderData + RelationGetInitTd(relation) * sizeof(TD) +         \
    sizeof(ItemIdData) + UHEAP_SPECIAL_SIZE))
#define UHeapTupleIsValid(tuple) PointerIsValid(tuple)
#define uheap_getattr(tup, attnum, tupleDesc, isnull)(                                    \
                ((attnum) > 0) ? \
                ( \
                        ((attnum) > (int) UHeapTupleHeaderGetNatts((tup)->disk_tuple)) ? \
                        ( \
                                  \
                                heapGetInitDefVal( (attnum), (tupleDesc), (isnull)) \
                        ) \
                        :        \
                        (                                \
                                UHeapFastGetAttr((tup), (attnum), (tupleDesc), (isnull))       \
                        )                                                                                    \
                ) \
                : \
                UHeapGetSysAttr((tup), (InvalidBuffer), (attnum), (tupleDesc), (isnull))    \
        )

#define UHeapTupleGetRawXid(tup)                                                  \
    (UHeapTupleHasMultiLockers((tup)->disk_tuple->flag) ?                         \
        ShortTransactionIdToNormal((tup)->t_multi_base, (tup)->disk_tuple->xid) : \
        ShortTransactionIdToNormal((tup)->t_xid_base, (tup)->disk_tuple->xid))

#define UHeapTupleCopyBaseFromPage(tup, page)                                 \
    do {                                                                      \
        (tup)->t_xid_base = ((UHeapPageHeaderData *)(page))->pd_xid_base;     \
        (tup)->t_multi_base = ((UHeapPageHeaderData *)(page))->pd_multi_base; \
    } while (0)

#ifndef FRONTEND
inline void UHeapTupleSetRawXid(UHeapTuple tup, TransactionId xid)
{
    ((tup)->disk_tuple->xid = NormalTransactionIdToShort(((tup)->t_xid_base), (xid)));
}
#endif

const int2 LEN_VARLENA = -1;
const int2 LEN_CSTRING = -2;
const int ATTNUM_BMP_SHIFT = 3;

UHeapTuple UHeapFormTuple(TupleDesc rowDesc, Datum *values, bool *isnull);
void UHeapDeformTuple(UHeapTuple utuple, TupleDesc rowDesc, Datum *values, bool *isNulls);
void UHeapDeformTupleGuts(UHeapTuple utuple, TupleDesc rowDesc, Datum *values, bool *isNulls, int unatts);
UHeapTuple HeapToUHeap(TupleDesc tuple_desc, HeapTuple heaptuple);
HeapTuple UHeapToHeap(TupleDesc tuple_desc, UHeapTuple uheaptuple);
void UHeapTupleSetHintBits(UHeapDiskTuple tuple, Buffer buffer, uint16 infomask, TransactionId xid);
Datum UHeapGetSysAttr(UHeapTuple uhtup, Buffer buf, int attnum, TupleDesc tupleDesc, bool *isnull);
Bitmapset *UHeapTupleAttrEquals(TupleDesc tupdesc, Bitmapset *att_list, UHeapTuple tup1, UHeapTuple tup2);
UHeapTuple UHeapCopyTuple(UHeapTuple uhtup);
void UHeapCopyTupleWithBuffer(UHeapTuple src, UHeapTuple dest);

/*
 * UHeapFreeTuple
 * Free memory used to store uheap tuple.
 */
#define UHeapFreeTuple(uhtup) \
    do {                                 \
        if ((uhtup) != NULL) {           \
            pfree_ext(uhtup);            \
            uhtup = NULL;                \
        }                                \
    } while (0)

UHeapTuple UHeapModifyTuple(
    UHeapTuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* repl_isnull, const bool* do_replace);
void UHeapTupleHeaderAdvanceLatestRemovedXid(UHeapDiskTuple tuple, TransactionId xid, TransactionId *latestRemovedXid);
bool UHeapAttIsNull(UHeapTuple tup, int attnum, TupleDesc tupleDesc);
Datum UHeapNoCacheGetAttr(UHeapTuple tuple, uint32 attnum, TupleDesc tupleDesc);
void SlotDeformUTuple(TupleTableSlot *slot, UHeapTuple tuple, long *offp, int natts);
uint32 UHeapCalcTupleDataSize(TupleDesc tuple_desc, Datum *values, const bool *is_nulls, uint32 hoff,
    bool enableReverseBitmap, bool enableReserve);
HeapTuple UHeapCopyHeapTuple(TupleTableSlot *slot);
void UHeapSlotStoreUHeapTuple(UHeapTuple utuple, TupleTableSlot *slot, bool shouldFree, bool batchMode);

void UHeapSlotClear(TupleTableSlot *slot);
void UHeapSlotGetSomeAttrs(TupleTableSlot *slot, int attnum);
void UHeapSlotFormBatch(TupleTableSlot* slot, VectorBatch* batch, int cur_rows, int attnum);
void UHeapSlotGetAllAttrs(TupleTableSlot *slot);
Datum UHeapSlotGetAttr(TupleTableSlot *slot, int attnum, bool *isnull);
bool UHeapSlotAttIsNull(const TupleTableSlot *slot, int attnum);

MinimalTuple UHeapSlotCopyMinimalTuple(TupleTableSlot *slot);
MinimalTuple UHeapSlotGetMinimalTuple(TupleTableSlot *slot);
void UHeapSlotStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree);

void UHeapCopyDiskTupleNoNull(TupleDesc tupleDesc, const bool *destNull, UHeapTuple dest_tuple, 
                           AttrNumber fillatts, const UHeapDiskTupleData *src_tuple);
void UHeapCopyDiskTupleWithNulls(TupleDesc tupleDesc, const bool *destNull, UHeapTuple dest_tuple,
                           AttrNumber fillatts, UHeapDiskTupleData *src_tuple);

template<bool hasnull>
void UHeapFillDiskTuple(TupleDesc tupleDesc, Datum *values, const bool *isnull, UHeapDiskTupleData *diskTuple,
    uint32 dataSize, bool enableReverseBitmap, bool enableReserve);
void CheckTupleValidity(Relation rel, UHeapTuple utuple);
Tuple UHeapMaterialize(TupleTableSlot *slot);
#endif
