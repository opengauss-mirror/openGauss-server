/* -------------------------------------------------------------------------
 *
 * htup.h
 *	  openGauss heap tuple definitions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/htup.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HTUP_H
#define HTUP_H

#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "access/xlogreader.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufpage.h"
#include "storage/item/itemptr.h"
#include "storage/smgr/relfilenode.h"
#include "utils/relcache.h"

    /* Number of internal columns added by Redis during scale-in/scale-out. */
#define REDIS_NUM_INTERNAL_COLUMNS	(4)

#define InvalidTransactionId ((TransactionId)0)
#define BootstrapTransactionId ((TransactionId)1)
#define FrozenTransactionId ((TransactionId)2)
#define FirstNormalTransactionId ((TransactionId)3)
#define MaxTransactionId ((TransactionId)0xFFFFFFFFFFFFFFF) /* First four bits reserved */
#define MaxShortTransactionId ((TransactionId)0xFFFFFFFF)

#define TransactionIdIsValid(xid) ((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid) ((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2) ((id1) == (id2))
#define TransactionIdStore(xid, dest) (*(dest) = (xid))

#define StoreInvalidTransactionId(dest) (*(dest) = InvalidTransactionId)

#define ShortTransactionIdToNormal(base, xid) \
    (TransactionIdIsNormal(xid) ? (TransactionId)(xid) + (base) : (TransactionId)(xid))
#define NormalTransactionIdToShort(base, xid)                                                                   \
    (TransactionIdIsNormal(xid) ? (ShortTransactionId)(AssertMacro((xid) >= (base) + FirstNormalTransactionId), \
                                      AssertMacro((xid) <= (base) + MaxShortTransactionId),                     \
                                      (xid) - (base))                                                           \
                                : (ShortTransactionId)(xid))

/*
 * MaxTupleAttributeNumber limits the number of (user) columns in a tuple.
 * The key limit on this value is that the size of the fixed overhead for
 * a tuple, plus the size of the null-values bitmap (at 1 bit per column),
 * plus MAXALIGN alignment, must fit into t_hoff which is uint8.  On most
 * machines the upper limit without making t_hoff wider would be a little
 * over 1700.  We use round numbers here and for MaxHeapAttributeNumber
 * so that alterations in HeapTupleHeaderData layout won't change the
 * supported max number of columns.
 */
#define MaxTupleAttributeNumber 1664 /* 8 * 208 */

/*
 * MaxHeapAttributeNumber limits the number of (user) columns in a table.
 * This should be somewhat less than MaxTupleAttributeNumber.  It must be
 * at least one less, else we will fail to do UPDATEs on a maximal-width
 * table (because UPDATE has to form working tuples that include CTID).
 * In practice we want some additional daylight so that we can gracefully
 * support operations that add hidden "resjunk" columns, for example
 * SELECT * FROM wide_table ORDER BY foo, bar, baz.
 * In any case, depending on column data types you will likely be running
 * into the disk-block-based limit on overall tuple size if you have more
 * than a thousand or so columns.  TOAST won't help.
 */
#define MaxHeapAttributeNumber 1600 /* 8 * 200 */

/*
 * Heap tuple header.  To avoid wasting space, the fields should be
 * laid out in such a way as to avoid structure padding.
 *
 * Datums of composite types (row types) share the same general structure
 * as on-disk tuples, so that the same routines can be used to build and
 * examine them.  However the requirements are slightly different: a Datum
 * does not need any transaction visibility information, and it does need
 * a length word and some embedded type information.  We can achieve this
 * by overlaying the xmin/cmin/xmax/cmax/xvac fields of a heap tuple
 * with the fields needed in the Datum case.  Typically, all tuples built
 * in-memory will be initialized with the Datum fields; but when a tuple is
 * about to be inserted in a table, the transaction fields will be filled,
 * overwriting the datum fields.
 *
 * The overall structure of a heap tuple looks like:
 *			fixed fields (HeapTupleHeaderData struct)
 *			nulls bitmap (if HEAP_HASNULL is set in t_infomask)
 *			alignment padding (as needed to make user data MAXALIGN'd)
 *			object ID (if HEAP_HASOID is set in t_infomask)
 *			user data fields
 *
 * We store five "virtual" fields Xmin, Cmin, Xmax, Cmax, and Xvac in three
 * physical fields.  Xmin and Xmax are always really stored, but Cmin, Cmax
 * and Xvac share a field.	This works because we know that Cmin and Cmax
 * are only interesting for the lifetime of the inserting and deleting
 * transaction respectively.  If a tuple is inserted and deleted in the same
 * transaction, we store a "combo" command id that can be mapped to the real
 * cmin and cmax, but only by use of local state within the originating
 * backend.  See combocid.c for more details.  Meanwhile, Xvac is only set by
 * old-style VACUUM FULL, which does not have any command sub-structure and so
 * does not need either Cmin or Cmax.  (This requires that old-style VACUUM
 * FULL never try to move a tuple whose Cmin or Cmax is still interesting,
 * ie, an insert-in-progress or delete-in-progress tuple.)
 *
 * A word about t_ctid: whenever a new tuple is stored on disk, its t_ctid
 * is initialized with its own TID (location).	If the tuple is ever updated,
 * its t_ctid is changed to point to the replacement version of the tuple.
 * Thus, a tuple is the latest version of its row iff XMAX is invalid or
 * t_ctid points to itself (in which case, if XMAX is valid, the tuple is
 * either locked or deleted).  One can follow the chain of t_ctid links
 * to find the newest version of the row.  Beware however that VACUUM might
 * erase the pointed-to (newer) tuple before erasing the pointing (older)
 * tuple.  Hence, when following a t_ctid link, it is necessary to check
 * to see if the referenced slot is empty or contains an unrelated tuple.
 * Check that the referenced tuple has XMIN equal to the referencing tuple's
 * XMAX to verify that it is actually the descendant version and not an
 * unrelated tuple stored into a slot recently freed by VACUUM.  If either
 * check fails, one may assume that there is no live descendant version.
 *
 * Following the fixed header fields, the nulls bitmap is stored (beginning
 * at t_bits).	The bitmap is *not* stored if t_infomask shows that there
 * are no nulls in the tuple.  If an OID field is present (as indicated by
 * t_infomask), then it is stored just before the user data, which begins at
 * the offset shown by t_hoff.	Note that t_hoff must be a multiple of
 * MAXALIGN.
 */

typedef struct HeapTupleFields {
    ShortTransactionId t_xmin; /* inserting xact ID */
    ShortTransactionId t_xmax; /* deleting or locking xact ID */

    union {
        CommandId t_cid;           /* inserting or deleting command ID, or both */
        ShortTransactionId t_xvac; /* old-style VACUUM FULL xact ID */
    } t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields {
    int32 datum_len_; /* varlena header (do not touch directly!) */

    int32 datum_typmod; /* -1, or identifier of a record type */

    Oid datum_typeid; /* composite type OID, or RECORDOID */

    /*
     * Note: field ordering is chosen with thought that Oid might someday
     * widen to 64 bits.
     */
} DatumTupleFields;

typedef struct HeapTupleHeaderData {
    union {
        HeapTupleFields t_heap;
        DatumTupleFields t_datum;
    } t_choice;

    ItemPointerData t_ctid; /* current TID of this or newer tuple */

    /* Fields below here must match MinimalTupleData! */

    uint16 t_infomask2; /* number of attributes + various flags */

    uint16 t_infomask; /* various flag bits, see below */

    uint8 t_hoff; /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

    bits8 t_bits[FLEXIBLE_ARRAY_MEMBER]; /* bitmap of NULLs -- VARIABLE LENGTH */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
} HeapTupleHeaderData;
typedef HeapTupleHeaderData* HeapTupleHeader;

#define SizeofMinimalTupleHeader offsetof(MinimalTupleData, t_bits)
#define SizeofHeapTupleHeader offsetof(HeapTupleHeaderData, t_bits)

/*
 * information stored in t_infomask:
 */
#define HEAP_HASNULL 0x0001          /* has null attribute(s) */
#define HEAP_HASVARWIDTH 0x0002      /* has variable-width attribute(s) */
#define HEAP_HASEXTERNAL 0x0004      /* has external stored attribute(s) */
#define HEAP_HASOID 0x0008           /* has an object-id field */
#define HEAP_COMPRESSED 0x0010       /* has compressed data */
#define HEAP_COMBOCID 0x0020         /* t_cid is a combo cid */
#define HEAP_XMAX_EXCL_LOCK 0x0040   /* xmax is exclusive locker */
#define HEAP_XMAX_SHARED_LOCK 0x0080 /* xmax is shared locker */
/* xmax is a key-shared locker */
#define HEAP_XMAX_KEYSHR_LOCK (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_SHARED_LOCK)
#define HEAP_LOCK_MASK (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_SHARED_LOCK | HEAP_XMAX_KEYSHR_LOCK)
#define HEAP_XMIN_COMMITTED 0x0100 /* t_xmin committed */
#define HEAP_XMIN_INVALID 0x0200   /* t_xmin invalid/aborted */
#define HEAP_XMIN_FROZEN (HEAP_XMIN_INVALID | HEAP_XMIN_COMMITTED)
#define HEAP_XMAX_COMMITTED 0x0400 /* t_xmax committed */
#define HEAP_XMAX_INVALID 0x0800   /* t_xmax invalid/aborted */
#define HEAP_XMAX_IS_MULTI 0x1000  /* t_xmax is a MultiXactId */
#define HEAP_UPDATED 0x2000        /* this is UPDATEd version of row */

#define HEAP_HAS_8BYTE_UID (0x4000) /* tuple has 8 bytes uid */
#define HEAP_UID_MASK (0x4000)
#define HEAP_RESERVED_BIT (0x8000) /* tuple uid related bits */

#define HEAP_XACT_MASK (0x3FE0) /* visibility-related bits */

/*
 * information stored in t_infomask2:
 */
#define HEAP_NATTS_MASK 0x07FF /* 11 bits for number of attributes */
#define HEAP_XMAX_LOCK_ONLY 0x0800 /* xmax, if valid, is only a locker */
#define HEAP_KEYS_UPDATED 0x1000 /* tuple was updated and key cols modified, or tuple deleted */
#define HEAP_HAS_REDIS_COLUMNS 0x2000 /* tuple has hidden columns added by redis */
#define HEAP_HOT_UPDATED 0x4000 /* tuple was HOT-updated */
#define HEAP_ONLY_TUPLE 0x8000  /* this is heap-only tuple */

#define HEAP2_XACT_MASK 0xD800 /* visibility-related bits */

/*
 * HEAP_TUPLE_HAS_MATCH is a temporary flag used during hash joins.  It is
 * only used in tuples that are in the hash table, and those don't need
 * any visibility information, so we can overlay it on a visibility flag
 * instead of using up a dedicated bit.
 */
#define HEAP_TUPLE_HAS_MATCH HEAP_ONLY_TUPLE /* tuple has a join match */

/*
 * A tuple is only locked (i.e. not updated by its Xmax) if it the
 * HEAP_XMAX_LOCK_ONLY bit is set.
 *
 * See also HeapTupleIsOnlyLocked, which also checks for a possible
 * aborted updater transaction.
 */
#define HEAP_XMAX_IS_LOCKED_ONLY(infomask, infomask2) \
    (((infomask2) & HEAP_XMAX_LOCK_ONLY) ||           \
     ((infomask) & HEAP_XMAX_SHARED_LOCK) ||          \
     (((infomask) & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK))

/*
 * Use these to test whether a particular lock is applied to a tuple
 */
#define HEAP_XMAX_IS_SHR_LOCKED(infomask) (((infomask) & HEAP_LOCK_MASK) == HEAP_XMAX_SHARED_LOCK)
#define HEAP_XMAX_IS_EXCL_LOCKED(infomask) (((infomask) & HEAP_LOCK_MASK) == HEAP_XMAX_EXCL_LOCK)
#define HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) (((infomask) & HEAP_LOCK_MASK) == HEAP_XMAX_KEYSHR_LOCK)

/* turn these all off when Xmax is to change */
#define HEAP_XMAX_BITS (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)

/*
 * HeapTupleHeader accessor macros
 *
 * Note: beware of multiple evaluations of "tup" argument.	But the Set
 * macros evaluate their other argument only once.
 */
/*
 * HeapTupleHeaderGetRawXmin returns the "raw" xmin field, which is the xid
 * originally used to insert the tuple.  However, the tuple might actually
 * be frozen (via HeapTupleHeaderSetXminFrozen) in which case the tuple's xmin
 * is visible to every snapshot.  Prior to PostgreSQL 9.4, we actually changed
 * the xmin to FrozenTransactionId, and that value may still be encountered
 * on disk.
 */

#define HeapTupleCopyBaseFromPage(tup, page)        \
    do {                                            \
        (tup)->t_xid_base = ((HeapPageHeader)(page))->pd_xid_base;     \
        (tup)->t_multi_base = ((HeapPageHeader)(page))->pd_multi_base; \
    } while (0)

#define HeapTupleCopyBase(dest, src)                \
    do {                                            \
        (dest)->t_xid_base = (src)->t_xid_base;     \
        (dest)->t_multi_base = (src)->t_multi_base; \
    } while (0)

#define HeapTupleSetZeroBase(tup)                   \
    {                                               \
        (tup)->t_xid_base = InvalidTransactionId;   \
        (tup)->t_multi_base = InvalidTransactionId; \
    }
#define HeapTupleHeaderXminCommitted(tup) ((tup)->t_infomask & HEAP_XMIN_COMMITTED)

#define HeapTupleHeaderXminInvalid(tup) \
    (((tup)->t_infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) == HEAP_XMIN_INVALID)
#define HeapTupleHeaderGetXmin(page, tup) \
    (ShortTransactionIdToNormal(          \
        ((HeapPageHeader)(page))->pd_xid_base, (tup)->t_choice.t_heap.t_xmin))
#define HeapTupleHeaderGetRawXmax(page, tup) HeapTupleHeaderGetXmax(page, tup)
#define HeapTupleGetRawXmin(tup) (ShortTransactionIdToNormal((tup)->t_xid_base, (tup)->t_data->t_choice.t_heap.t_xmin))

#define HeapTupleHeaderXminInvalid(tup) \
    (((tup)->t_infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) == HEAP_XMIN_INVALID)

#define HeapTupleHeaderXminFrozen(tup) (((tup)->t_infomask & (HEAP_XMIN_FROZEN)) == HEAP_XMIN_FROZEN)

#define HeapTupleHeaderSetXminCommitted(tup) \
    (AssertMacro(!HeapTupleHeaderXminInvalid(tup)), ((tup)->t_infomask |= HEAP_XMIN_COMMITTED))

#define HeapTupleHeaderSetXminInvalid(tup) \
    (AssertMacro(!HeapTupleHeaderXminCommitted(tup)), ((tup)->t_infomask |= HEAP_XMIN_INVALID))

#define HeapTupleHeaderSetXminFrozen(tup) \
    (AssertMacro(!HeapTupleHeaderXminInvalid(tup)), ((tup)->t_infomask |= HEAP_XMIN_FROZEN))

#define HeapTupleGetRawXmax(tup)                                                                    \
    (ShortTransactionIdToNormal(                                                                    \
        ((tup)->t_data->t_infomask & HEAP_XMAX_IS_MULTI ? (tup)->t_multi_base: (tup)->t_xid_base),  \
        ((tup)->t_data->t_choice.t_heap.t_xmax)                                                     \
    ))

/*
 * HeapTupleGetRawXmax gets you the raw Xmax field.  To find out the Xid
 * that updated a tuple, you might need to resolve the MultiXactId if certain
 * bits are set.  HeapTupleGetUpdateXid checks those bits and takes care
 * to resolve the MultiXactId if necessary.  This might involve multixact I/O,
 * so it should only be used if absolutely necessary.
 */
#define HeapTupleGetUpdateXid(tup)                              \
    ((!((tup)->t_data->t_infomask & HEAP_XMAX_INVALID) &&       \
      ((tup)->t_data->t_infomask & HEAP_XMAX_IS_MULTI) &&       \
      !((tup)->t_data->t_infomask2 & HEAP_XMAX_LOCK_ONLY)) ?    \
        HeapTupleMultiXactGetUpdateXid(tup) :                   \
        HeapTupleGetRawXmax(tup))

#define HeapTupleHeaderGetUpdateXid(page, tup)                  \
    ((!((tup)->t_infomask & HEAP_XMAX_INVALID) &&               \
      ((tup)->t_infomask & HEAP_XMAX_IS_MULTI) &&               \
      !((tup)->t_infomask2 & HEAP_XMAX_LOCK_ONLY)) ?            \
        HeapTupleHeaderMultiXactGetUpdateXid(page, tup) :       \
        HeapTupleHeaderGetRawXmax(page, tup))

#define HeapTupleHeaderGetXmax(page, tup)                                                                          \
    (ShortTransactionIdToNormal(((tup)->t_infomask & HEAP_XMAX_IS_MULTI)                                           \
                                    ? (((HeapPageHeader)(page))->pd_multi_base) \
                                    : (((HeapPageHeader)(page))->pd_xid_base),  \
        ((tup)->t_choice.t_heap.t_xmax)))

#define HeapTupleHeaderSetXmax(page, tup, xid)                                                  \
    ((tup)->t_choice.t_heap.t_xmax = NormalTransactionIdToShort(                                \
         (((tup)->t_infomask & HEAP_XMAX_IS_MULTI)                                              \
                 ? (((HeapPageHeader)(page))->pd_multi_base) \
                 : (((HeapPageHeader)(page))->pd_xid_base)), \
         (xid)))
#define HeapTupleHeaderSetXmin(page, tup, xid)                   \
    ((tup)->t_choice.t_heap.t_xmin = NormalTransactionIdToShort( \
         (((HeapPageHeader)(page))->pd_xid_base), (xid)))

#define HeapTupleSetXmax(tup, xid)                                       \
    ((tup)->t_data->t_choice.t_heap.t_xmax = NormalTransactionIdToShort( \
         ((tup)->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ? (tup)->t_multi_base : (tup)->t_xid_base, (xid)))

#define HeapTupleSetXmin(tup, xid) \
    ((tup)->t_data->t_choice.t_heap.t_xmin = NormalTransactionIdToShort((tup)->t_xid_base, (xid)))

/*
 * HeapTupleHeaderGetRawCommandId will give you what's in the header whether
 * it is useful or not.  Most code should use HeapTupleHeaderGetCmin or
 * HeapTupleHeaderGetCmax instead, but note that those Assert that you can
 * get a legitimate result, ie you are in the originating transaction!
 */
#define HeapTupleHeaderGetRawCommandId(tup) ((tup)->t_choice.t_heap.t_field3.t_cid)

/* SetCmin is reasonably simple since we never need a combo CID */
#define HeapTupleHeaderSetCmin(tup, cid)               \
    do {                                               \
        (tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
        (tup)->t_infomask &= ~HEAP_COMBOCID;           \
    } while (0)

/* SetCmax must be used after HeapTupleHeaderAdjustCmax; see combocid.c */
#define HeapTupleHeaderSetCmax(tup, cid, iscombo)      \
    do {                                               \
        (tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
        if (iscombo)                                   \
            (tup)->t_infomask |= HEAP_COMBOCID;        \
        else                                           \
            (tup)->t_infomask &= ~HEAP_COMBOCID;       \
    } while (0)

#define HeapTupleHeaderGetDatumLength(tup) VARSIZE(tup)

#define HeapTupleHeaderSetDatumLength(tup, len) SET_VARSIZE(tup, len)

#define HeapTupleHeaderGetTypeId(tup) ((tup)->t_choice.t_datum.datum_typeid)

#define HeapTupleHeaderSetTypeId(tup, typeid) ((tup)->t_choice.t_datum.datum_typeid = (typeid))

#define HeapTupleHeaderGetTypMod(tup) ((tup)->t_choice.t_datum.datum_typmod)

#define HeapTupleHeaderSetTypMod(tup, typmod) ((tup)->t_choice.t_datum.datum_typmod = (typmod))

#define HeapTupleHeaderGetOid(tup) \
    (((tup)->t_infomask & HEAP_HASOID) ? *((Oid*)((char*)(tup) + (tup)->t_hoff - sizeof(Oid))) : InvalidOid)

#define HeapTupleHeaderSetOid(tup, oid)                                \
    do {                                                               \
        Assert((tup)->t_infomask& HEAP_HASOID);                        \
        *((Oid*)((char*)(tup) + (tup)->t_hoff - sizeof(Oid))) = (oid); \
    } while (0)

#define HeapTupleHeaderHasOid(tup) (((tup)->t_infomask & HEAP_HASOID) != 0)

#define HeapTupleHeaderHasUid(tup) (((tup)->t_infomask & HEAP_HAS_8BYTE_UID) != 0)
#define GetUidByteLen(uid) (sizeof(uint64))
#define HeapUidMask ()
#define GetUidByteLenInfomask(uid) (HEAP_HAS_8BYTE_UID)
#define HeapTupleHeaderSetUid(tup, uid, uidLen)                              \
    do {                                                                     \
        Assert((tup)->t_infomask & HEAP_HAS_8BYTE_UID);                      \
        Assert(!HeapTupleHeaderHasOid(tup));                                 \
        *((uint64*)((char*)(tup) + (tup)->t_hoff - uidLen)) = (uid);        \
    } while (0)

extern uint64 HeapTupleGetUid(HeapTuple tup);
extern void HeapTupleSetUid(HeapTuple tup, uint64 uid, int nattrs);

/*
 * Note that we stop considering a tuple HOT-updated as soon as it is known
 * aborted or the would-be updating transaction is known aborted.  For best
 * efficiency, check tuple visibility before using this macro, so that the
 * INVALID bits will be as up to date as possible.
 */
#define HeapTupleHeaderIsHotUpdated(tup)                                                             \
    (((tup)->t_infomask2 & HEAP_HOT_UPDATED) != 0 && ((tup)->t_infomask & HEAP_XMAX_INVALID) == 0 && \
        !HeapTupleHeaderXminInvalid(tup))

#define HeapTupleHeaderSetHotUpdated(tup) ((tup)->t_infomask2 |= HEAP_HOT_UPDATED)

#define HeapTupleHeaderClearHotUpdated(tup) ((tup)->t_infomask2 &= ~HEAP_HOT_UPDATED)

#define HeapTupleHeaderIsHeapOnly(tup) (((tup)->t_infomask2 & HEAP_ONLY_TUPLE) != 0)

#define HeapTupleHeaderSetHeapOnly(tup) ((tup)->t_infomask2 |= HEAP_ONLY_TUPLE)

#define HeapTupleHeaderClearHeapOnly(tup) ((tup)->t_infomask2 &= ~HEAP_ONLY_TUPLE)

#define HeapTupleHeaderHasRedisColumns(tup) (((tup)->t_infomask2 & HEAP_HAS_REDIS_COLUMNS) != 0)

#define HeapTupleHeaderSetRedisColumns(tup) ((tup)->t_infomask2 |= HEAP_HAS_REDIS_COLUMNS)

#define HeapTupleHeaderUnsetRedisColumns(tup) ((tup)->t_infomask2 &= ~HEAP_HAS_REDIS_COLUMNS)

#define HeapTupleHeaderHasMatch(tup) (((tup)->t_infomask2 & HEAP_TUPLE_HAS_MATCH) != 0)

#define HeapTupleHeaderSetMatch(tup) ((tup)->t_infomask2 |= HEAP_TUPLE_HAS_MATCH)

#define HeapTupleHeaderClearMatch(tup) ((tup)->t_infomask2 &= ~HEAP_TUPLE_HAS_MATCH)

/*
 * Tuple Descriptor is added to ignore the hidden columns added by redis (if any)
 * from the tuple.
 */
static inline uint32 HeapTupleHeaderGetNatts(HeapTupleHeader tup, TupleDesc tup_desc)
{
    Assert(tup != NULL);
    uint32 natts = (tup->t_infomask2 & HEAP_NATTS_MASK);

    /*
     * If the tuple header has HEAP_HAS_REDIS_COLUMNS bit set, it means that the
     * tuple has values for the REDIS_NUM_INTERNAL_COLUMNS added by redis.
     * These column values are valid only for the duration of table redistribution.
     * Once the table redistribution is completed (the relation is not
     * REDIS_REL_DESTINATION anymore), these column values must be ignored from
     * the tuple.
     */
    if (HeapTupleHeaderHasRedisColumns(tup) && tup_desc && !tup_desc->tdisredistable) {
        Assert(natts >= REDIS_NUM_INTERNAL_COLUMNS);
        natts -= REDIS_NUM_INTERNAL_COLUMNS;
    }

    return natts;
}

#define HeapTupleHeaderSetNatts(tup, natts) ((tup)->t_infomask2 = ((tup)->t_infomask2 & ~HEAP_NATTS_MASK) | (natts))

/* tuple' compression macro */
#define HEAP_TUPLE_SET_COMPRESSED(tup) ((tup)->t_infomask |= HEAP_COMPRESSED)

#define HEAP_TUPLE_IS_COMPRESSED(tup) (((tup)->t_infomask & HEAP_COMPRESSED) != 0)

#define HEAP_TUPLE_CLEAR_COMPRESSED(tup) ((tup)->t_infomask &= ~HEAP_COMPRESSED)

/* Struct for forming compressed tuple.
 *
 * compressed: comppressed flag for each attribute in one tuple.
 * delta/prefix/dict values are mixed in values of heapFormTuple(desc, values, isnulls, void*)
 * delta compression: --> char* delta, whose size is in valsize[]
 * prefix compression: --> char* ended with '\0', whose size is in valsize[]
 * dict compression: --> the index of dict item, whose size is in valsize[]
 */
typedef struct {
    bool* isnulls;
    bool* compressed;
    Datum* values;
    int* valsize;
} FormCmprTupleData;

/*
 * BITMAPLEN(NATTS) -
 *		Computes size of null bitmap given number of data columns.
 */
#define BITMAPLEN(NATTS) (((int)(NATTS) + 7) / 8)

/*
 * MaxHeapTupleSize is the maximum allowed size of a heap tuple, including
 * header and MAXALIGN alignment padding.  Basically it's BLCKSZ minus the
 * other stuff that has to be on a disk page.  Since heap pages use no
 * "special space", there's no deduction for that.
 *
 * NOTE: we allow for the ItemId that must point to the tuple, ensuring that
 * an otherwise-empty page can indeed hold a tuple of this size.  Because
 * ItemIds and tuples have different alignment requirements, don't assume that
 * you can, say, fit 2 tuples of size MaxHeapTupleSize/2 on the same page.
 */
#define MaxHeapTupleSize (BLCKSZ - MAXALIGN(SizeOfHeapPageHeaderData + sizeof(ItemIdData)))
#define MinHeapTupleSize MAXALIGN(offsetof(HeapTupleHeaderData, t_bits))

/*
 * MaxHeapTuplesPerPage is an upper bound on the number of tuples that can
 * fit on one heap page.  (Note that indexes could have more, because they
 * use a smaller tuple header.)  We arrive at the divisor because each tuple
 * must be maxaligned, and it must have an associated item pointer.
 *
 * Note: with HOT, there could theoretically be more line pointers (not actual
 * tuples) than this on a heap page.  However we constrain the number of line
 * pointers to this anyway, to avoid excessive line-pointer bloat and not
 * require increases in the size of work arrays.
 */
#define MaxHeapTuplesPerPage                     \
    ((int)((BLCKSZ - SizeOfHeapPageHeaderData) / \
           (MAXALIGN(offsetof(HeapTupleHeaderData, t_bits)) + sizeof(ItemIdData))))

/*
 * MaxAttrSize is a somewhat arbitrary upper limit on the declared size of
 * data fields of char(n) and similar types.  It need not have anything
 * directly to do with the *actual* upper limit of varlena values, which
 * is currently 1Gb (see TOAST structures in postgres.h).  I've set it
 * at 10Mb which seems like a reasonable number --- tgl 8/6/00.
 */
#define MaxAttrSize (10 * 1024 * 1024)

/*
 * MinimalTuple is an alternative representation that is used for transient
 * tuples inside the executor, in places where transaction status information
 * is not required, the tuple rowtype is known, and shaving off a few bytes
 * is worthwhile because we need to store many tuples.	The representation
 * is chosen so that tuple access routines can work with either full or
 * minimal tuples via a HeapTupleData pointer structure.  The access routines
 * see no difference, except that they must not access the transaction status
 * or t_ctid fields because those aren't there.
 *
 * For the most part, MinimalTuples should be accessed via TupleTableSlot
 * routines.  These routines will prevent access to the "system columns"
 * and thereby prevent accidental use of the nonexistent fields.
 *
 * MinimalTupleData contains a length word, some padding, and fields matching
 * HeapTupleHeaderData beginning with t_infomask2. The padding is chosen so
 * that offsetof(t_infomask2) is the same modulo MAXIMUM_ALIGNOF in both
 * structs.   This makes data alignment rules equivalent in both cases.
 *
 * When a minimal tuple is accessed via a HeapTupleData pointer, t_data is
 * set to point MINIMAL_TUPLE_OFFSET bytes before the actual start of the
 * minimal tuple --- that is, where a full tuple matching the minimal tuple's
 * data would start.  This trick is what makes the structs seem equivalent.
 *
 * Note that t_hoff is computed the same as in a full tuple, hence it includes
 * the MINIMAL_TUPLE_OFFSET distance.  t_len does not include that, however.
 *
 * MINIMAL_TUPLE_DATA_OFFSET is the offset to the first useful (non-pad) data
 * other than the length word.	tuplesort.c and tuplestore.c use this to avoid
 * writing the padding to disk.
 */
#define MINIMAL_TUPLE_OFFSET \
    ((offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) / MAXIMUM_ALIGNOF * MAXIMUM_ALIGNOF)
#define MINIMAL_TUPLE_PADDING ((offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) % MAXIMUM_ALIGNOF)
#define MINIMAL_TUPLE_DATA_OFFSET offsetof(MinimalTupleData, t_infomask2)

typedef struct MinimalTupleData {
    uint32 t_len; /* actual length of minimal tuple */
    char mt_padding[MINIMAL_TUPLE_PADDING];

    /* Fields below here must match HeapTupleHeaderData! */
    uint16 t_infomask2; /* number of attributes + various flags */

    uint16 t_infomask; /* various flag bits, see below */

    uint8 t_hoff; /* sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

    bits8 t_bits[FLEXIBLE_ARRAY_MEMBER]; /* bitmap of NULLs -- VARIABLE LENGTH */

    /* MORE DATA FOLLOWS AT END OF STRUCT */
} MinimalTupleData;

typedef MinimalTupleData* MinimalTuple;

/*
 * HeapTupleData is an in-memory data structure that points to a tuple.
 *
 * There are several ways in which this data structure is used:
 *
 * * Pointer to a tuple in a disk buffer: t_data points directly into the
 *	 buffer (which the code had better be holding a pin on, but this is not
 *	 reflected in HeapTupleData itself).
 *
 * * Pointer to nothing: t_data is NULL.  This is used as a failure indication
 *	 in some functions.
 *
 * * Part of a palloc'd tuple: the HeapTupleData itself and the tuple
 *	 form a single palloc'd chunk.  t_data points to the memory location
 *	 immediately following the HeapTupleData struct (at offset HEAPTUPLESIZE).
 *	 This is the output format of heap_form_tuple and related routines.
 *
 * * Separately allocated tuple: t_data points to a palloc'd chunk that
 *	 is not adjacent to the HeapTupleData.	(This case is deprecated since
 *	 it's difficult to tell apart from case #1.  It should be used only in
 *	 limited contexts where the code knows that case #1 will never apply.)
 *
 * * Separately allocated minimal tuple: t_data points MINIMAL_TUPLE_OFFSET
 *	 bytes before the start of a MinimalTuple.	As with the previous case,
 *	 this can't be told apart from case #1 by inspection; code setting up
 *	 or destroying this representation has to know what it's doing.
 *
 * t_len should always be valid, except in the pointer-to-nothing case.
 * t_self and t_tableOid should be valid if the HeapTupleData points to
 * a disk buffer, or if it represents a copy of a tuple on disk.  They
 * should be explicitly set invalid in manufactured tuples.
 */
typedef struct HeapTupleData {
    uint32 t_len;           /* length of *t_data */
    uint1 tupTableType = HEAP_TUPLE;
    uint1 tupInfo;
    int2   t_bucketId;
    ItemPointerData t_self; /* SelfItemPointer */
    Oid t_tableOid;         /* table the tuple came from */
    TransactionId t_xid_base;
    TransactionId t_multi_base;
#ifdef PGXC
    uint32 t_xc_node_id; /* Data node the tuple came from */
#endif
    HeapTupleHeader t_data; /* -> tuple header and data */
} HeapTupleData;

typedef HeapTupleData* HeapTuple;
typedef void* Tuple;

inline HeapTuple heaptup_alloc(Size size)
{
    HeapTuple tup = (HeapTuple)palloc0(size);
    tup->tupTableType = HEAP_TUPLE;
    return tup;
}

#define HEAPTUPLESIZE MAXALIGN(sizeof(HeapTupleData))

/*
 * GETSTRUCT - given a HeapTuple pointer, return address of the user data
 */
#define GETSTRUCT(TUP) ((char*)((TUP)->t_data) + (TUP)->t_data->t_hoff)

/*
 * Accessor macros to be used with HeapTuple pointers.
 */
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

#define HeapTupleHasNulls(tuple) (((tuple)->t_data->t_infomask & HEAP_HASNULL) != 0)

#define HeapTupleNoNulls(tuple) (!((tuple)->t_data->t_infomask & HEAP_HASNULL))

#define HeapTupleHasVarWidth(tuple) (((tuple)->t_data->t_infomask & HEAP_HASVARWIDTH) != 0)

#define HeapTupleAllFixed(tuple) (!((tuple)->t_data->t_infomask & HEAP_HASVARWIDTH))

#define HeapTupleHasExternal(tuple) (((tuple)->t_data->t_infomask & HEAP_HASEXTERNAL) != 0)

#define HeapTupleIsHotUpdated(tuple) HeapTupleHeaderIsHotUpdated((tuple)->t_data)

#define HeapTupleSetHotUpdated(tuple) HeapTupleHeaderSetHotUpdated((tuple)->t_data)

#define HeapTupleClearHotUpdated(tuple) HeapTupleHeaderClearHotUpdated((tuple)->t_data)

#define HeapTupleIsHeapOnly(tuple) HeapTupleHeaderIsHeapOnly((tuple)->t_data)

#define HeapTupleSetHeapOnly(tuple) HeapTupleHeaderSetHeapOnly((tuple)->t_data)

#define HeapTupleClearHeapOnly(tuple) HeapTupleHeaderClearHeapOnly((tuple)->t_data)

#define HeapTupleGetOid(tuple) HeapTupleHeaderGetOid((tuple)->t_data)

#define HeapTupleSetOid(tuple, oid) HeapTupleHeaderSetOid((tuple)->t_data, (oid))

/*
 * WAL record definitions for heapam.c's WAL operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field.  We use 3 for opcode and one for init bit.
 */
#define XLOG_HEAP_INSERT 0x00
#define XLOG_HEAP_DELETE 0x10
#define XLOG_HEAP_UPDATE 0x20
#define XLOG_HEAP_BASE_SHIFT 0x30
#define XLOG_HEAP_HOT_UPDATE 0x40
#define XLOG_HEAP_NEWPAGE 0x50
#define XLOG_HEAP_LOCK 0x60
#define XLOG_HEAP_INPLACE 0x70

#define XLOG_HEAP_OPMASK 0x70
/*
 * When we insert 1st item on new page in INSERT, UPDATE, HOT_UPDATE,
 * or MULTI_INSERT, we can (and we do) restore entire page in redo
 */
#define XLOG_HEAP_INIT_PAGE 0x80

/* Upgrade support for enhanced tupl lock mode */
#define XLOG_TUPLE_LOCK_UPGRADE_FLAG 0x01

/*
 * We ran out of opcodes, so heapam.c now has a second RmgrId.	These opcodes
 * are associated with RM_HEAP2_ID, but are not logically different from
 * the ones above associated with RM_HEAP_ID.  XLOG_HEAP_OPMASK applies to
 * these, too.
 */
#define XLOG_HEAP2_FREEZE 0x00
#define XLOG_HEAP2_CLEAN 0x10
/* 0x20 is free, was XLOG_HEAP2_PAGE_UPGRADE */
#define XLOG_HEAP2_PAGE_UPGRADE 0x20
#define XLOG_HEAP2_CLEANUP_INFO 0x30
#define XLOG_HEAP2_VISIBLE 0x40
#define XLOG_HEAP2_MULTI_INSERT 0x50
#define XLOG_HEAP2_BCM 0x60

#define XLOG_HEAP2_LOGICAL_NEWPAGE 0x70

/*
 * When we prune page, sometimes not call PageRepairFragmentation (e.g freeze_single_heap_page),
 * so need a flag to notify the standby DN the PageRepairFragmentation is not required.
 */
#define XLOG_HEAP2_NO_REPAIR_PAGE		0x80

/* XLOG_HEAP_NEW_CID with 0x30 in heap is XLOGHEAP2_NEW_CID with 0x70 in heap2 in PG9.4 */
#define XLOG_HEAP3_NEW_CID 0x00
#define XLOG_HEAP3_REWRITE 0x10
#define XLOG_HEAP3_INVALID 0x20

/* we used to put all xl_heap_* together, which made us run out of opcodes (quickly)
 * when trying to add a DELETE_IS_SUPER operation. Thus we split the codes carefully
 * for INSERT, UPDATE, DELETE individually. each has 8 bits available to use.
 */
/*
 * xl_heap_insert/xl_heap_multi_insert flag values, 8 bits are available
 */
/* PD_ALL_VISIBLE was cleared */
#define XLH_INSERT_ALL_VISIBLE_CLEARED		   (1<<0)
#define XLH_INSERT_CONTAINS_NEW_TUPLE		   (1<<4)
#define XLH_INSERT_LAST_IN_MULTI               (1<<7)

/*
 * xl_heap_update flag values, 8 bits are available.
*/
/* PD_ALL_VISIBLE was cleared */
#define XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED	   (1<<0)
/* PD_ALL_VISIBLE was cleared in the 2nd page */
#define XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED	   (1<<1)
#define XLH_UPDATE_CONTAINS_OLD_TUPLE		   (1<<2)
#define XLH_UPDATE_CONTAINS_OLD_KEY			   (1<<3)
#define XLH_UPDATE_CONTAINS_NEW_TUPLE		   (1<<4)
#define XLH_UPDATE_PREFIX_FROM_OLD			   (1<<5)
#define XLH_UPDATE_SUFFIX_FROM_OLD			   (1<<6)

/* convenience macro for checking whether any form of old tuple was logged */
#define XLH_UPDATE_CONTAINS_OLD					   \
    (XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY)

/*
* xl_heap_delete flag values, 8 bits are available.
*/
/* PD_ALL_VISIBLE was cleared */
#define XLH_DELETE_ALL_VISIBLE_CLEARED		   (1<<0)
#define XLH_DELETE_IS_SUPER                    (1<<1)
#define XLH_DELETE_CONTAINS_OLD_TUPLE		   (1<<2)
#define XLH_DELETE_CONTAINS_OLD_KEY			   (1<<3)

/* convenience macro for checking whether any form of old tuple was logged */
#define XLH_DELETE_CONTAINS_OLD					   \
    (XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY)

/* This is what we need to know about delete */
typedef struct xl_heap_delete {
    OffsetNumber offnum; /* deleted tuple's offset */
    uint8 flags;
    TransactionId xmax;  /* xmax of the deleted tuple */
    uint8 infobits_set;  /* infomask bits */
} xl_heap_delete;

#define SizeOfOldHeapDelete (offsetof(xl_heap_delete, flags) + sizeof(uint8))
#define SizeOfHeapDelete (offsetof(xl_heap_delete, infobits_set) + sizeof(uint8))

/*
 * We don't store the whole fixed part (HeapTupleHeaderData) of an inserted
 * or updated tuple in WAL; we can save a few bytes by reconstructing the
 * fields that are available elsewhere in the WAL record, or perhaps just
 * plain needn't be reconstructed.  These are the fields we must store.
 * NOTE: t_hoff could be recomputed, but we may as well store it because
 * it will come for free due to alignment considerations.
 */
typedef struct xl_heap_header {
    uint16 t_infomask2;
    uint16 t_infomask;
    uint8 t_hoff;
} xl_heap_header;

#define SizeOfHeapHeader (offsetof(xl_heap_header, t_hoff) + sizeof(uint8))

/* This is what we need to know about insert */
typedef struct xl_heap_insert {
    OffsetNumber offnum; /* inserted tuple's offset */
    uint8 flags;

    /* xl_heap_header & TUPLE DATA in backup block 0 */
} xl_heap_insert;

#define SizeOfHeapInsert (offsetof(xl_heap_insert, flags) + sizeof(uint8))

/*
 * This is what we need to know about a multi-insert.
 *
 * The main data of the record consists of this xl_heap_multi_insert header.
 * 'offsets' array is omitted if the whole page is reinitialized
 * (XLOG_HEAP_INIT_PAGE).
 *
 * If this block is compressed with dictionary, then this dictionary will follow
 * <xl_heap_multi_insert> header, but before <xl_multi_insert_tuple> tuples and
 * tuples' data. <isCompressed> indicates whether there is a dictionary.
 *
 * In block 0's data portion, there is an xl_multi_insert_tuple struct,
 * followed by the tuple data for each tuple. There is padding to align
 * each xl_multi_insert struct.
 */
typedef struct xl_heap_multi_insert {
    uint8 flags;
    bool isCompressed;
    uint16 ntuples;
    OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} xl_heap_multi_insert;

#define SizeOfHeapMultiInsert offsetof(xl_heap_multi_insert, offsets)

typedef struct xl_multi_insert_tuple {
    uint16 datalen; /* size of tuple data that follows */
    uint16 t_infomask2;
    uint16 t_infomask;
    uint8 t_hoff;
    /* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_multi_insert_tuple;

#define SizeOfMultiInsertTuple (offsetof(xl_multi_insert_tuple, t_hoff) + sizeof(uint8))

/*
 * This is what we need to know about update|hot_update
 *
 * Backup blk 0: new page
 *
 * If XLOG_HEAP_PREFIX_FROM_OLD or XLOG_HEAP_SUFFIX_FROM_OLD flags are set,
 * the prefix and/or suffix come first, as one or two uint16s.
 *
 * After that, xl_heap_header and new tuple data follow.  The new tuple
 * data doesn't include the prefix and suffix, which are copied from the
 * old tuple on replay.
 *
 * If HEAP_CONTAINS_NEW_TUPLE_DATA flag is given, the tuple data is
 * included even if a full-page image was taken.
 *
 * Backup blk 1: old page, if different. (no data, just a reference to the blk)
 */
typedef struct xl_heap_update {
    OffsetNumber old_offnum; /* old tuple's offset */
    OffsetNumber new_offnum; /* new tuple's offset */
    uint8 flags;             /* NEW TUPLE xl_heap_header AND TUPLE DATA FOLLOWS AT END OF STRUCT */
    TransactionId old_xmax;  /* xmax of the old tuple */
    TransactionId new_xmax;  /* xmax of the new tuple */
    uint8 old_infobits_set;  /* infomask bits to set on old tuple */
} xl_heap_update;

#define SizeOfOldHeapUpdate (offsetof(xl_heap_update, flags) + sizeof(uint8))
#define SizeOfHeapUpdate (offsetof(xl_heap_update, old_infobits_set) + sizeof(uint8))
/*
 * This is what we need to know about vacuum page cleanup/redirect
 *
 * The array of OffsetNumbers following the fixed part of the record contains:
 *	* for each redirected item: the item offset, then the offset redirected to
 *	* for each now-dead item: the item offset
 *	* for each now-unused item: the item offset
 * The total number of OffsetNumbers is therefore 2*nredirected+ndead+nunused.
 * Note that nunused is not explicitly stored, but may be found by reference
 * to the total record length.
 */
typedef struct xl_heap_clean {
    TransactionId latestRemovedXid;
    uint16 nredirected;
    uint16 ndead;
    /* OFFSET NUMBERS are in the block reference 0 */
} xl_heap_clean;

#define SizeOfHeapClean (offsetof(xl_heap_clean, ndead) + sizeof(uint16))

/*
 * Cleanup_info is required in some cases during a lazy VACUUM.
 * Used for reporting the results of HeapTupleHeaderAdvanceLatestRemovedXid()
 * see vacuumlazy.c for full explanation
 */
typedef struct xl_heap_cleanup_info {
    RelFileNodeOld node;
    TransactionId latestRemovedXid;
} xl_heap_cleanup_info;

#define SizeOfHeapCleanupInfo (sizeof(xl_heap_cleanup_info))

/* Logical xlog for multi_insert or new index when data replication store is row*/
typedef struct xl_heap_logical_newpage {
    RelFileNodeOld node;   // relfilenode
    BlockNumber blkno;  // block number
    ForkNumber forknum;
    StorageEngine type;
    bool hasdata;  // flag of save cu xlog
    int attid;     // column id
    Size offset;   // CU offset
    int32 blockSize;
    /* Other infos?: offset datalen Row or column store */
} xl_heap_logical_newpage;

#define SizeOfHeapLogicalNewPage (offsetof(xl_heap_logical_newpage, blockSize) + sizeof(int32))

/* flags for infobits_set */
#define XLHL_XMAX_IS_MULTI      0x01
#define XLHL_XMAX_LOCK_ONLY     0x02
#define XLHL_XMAX_EXCL_LOCK     0x04
#define XLHL_XMAX_KEYSHR_LOCK   0x08
#define XLHL_KEYS_UPDATED       0x10

/* This is what we need to know about lock */
typedef struct xl_heap_lock {
    TransactionId locking_xid; /* might be a MultiXactId not xid */
    OffsetNumber offnum;       /* locked tuple's offset on page */
    bool xid_is_mxact;         /* is it? */
    bool shared_lock;          /* shared or exclusive row lock? */
    uint8 infobits_set;        /* infomask and infomask2 bits to set */
    bool lock_updated;         /* lock an updated version of a row */
} xl_heap_lock;

#define SizeOfOldHeapLock (offsetof(xl_heap_lock, shared_lock) + sizeof(bool))
#define SizeOfHeapLock (offsetof(xl_heap_lock, lock_updated) + sizeof(bool))

/* This is what we need to know about in-place update */
typedef struct xl_heap_inplace {
    OffsetNumber offnum; /* updated tuple's offset on page */
                         /* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_heap_inplace;

#define SizeOfHeapInplace (offsetof(xl_heap_inplace, offnum) + sizeof(OffsetNumber))

/*
 * This is what we need to know about a block being frozen during vacuum
 *
 * Backup block 0's data contains an array of xl_heap_freeze structs,
 * one for each tuple.
 */
typedef struct xl_heap_freeze {
    TransactionId cutoff_xid;
    MultiXactId cutoff_multi;
    /* TUPLE OFFSET NUMBERS FOLLOW AT THE END */
} xl_heap_freeze;

#define SizeOfOldHeapFreeze (offsetof(xl_heap_freeze, cutoff_xid) + sizeof(TransactionId))
#define SizeOfHeapFreeze (offsetof(xl_heap_freeze, cutoff_multi) + sizeof(MultiXactId))

typedef struct xl_heap_invalid {
    TransactionId cutoff_xid;
    /* TUPLE OFFSET NUMBERS FOLLOW AT THE END */
} xl_heap_invalid;
#define SizeOfHeapInvalid (offsetof(xl_heap_invalid, cutoff_xid) + sizeof(TransactionId))

typedef struct xl_heap_freeze_tuple {
    TransactionId xmax;
    OffsetNumber offset;
    uint16 t_infomask2;
    uint16 t_infomask;
    uint8 frzflags;
} xl_heap_freeze_tuple;

/*
 * This is what we need to know about setting a visibility map bit
 *
 * Backup blk 0: visibility map buffer
 * Backup blk 1: heap buffer if exists (except partition merge)
 */
typedef struct xl_heap_visible {
    BlockNumber block;
    TransactionId cutoff_xid;
    bool free_dict; /* dick will be checked and freed when switching visibility. */
} xl_heap_visible;

#define SizeOfHeapVisible (offsetof(xl_heap_visible, free_dict) + sizeof(bool))

/* This is what we need to know about setting a bcm map bit */
typedef struct xl_heap_bcm {
    RelFileNodeOld node;
    uint32 col;
    uint64 block;
    int count;
    int status;
} xl_heap_bcm;

#define SizeOfHeapBcm (offsetof(xl_heap_bcm, status) + sizeof(int))

/* shift the base of xids on heap page */
typedef struct xl_heap_base_shift {
    int64 delta; /* delta value to shift the base */
    bool multi;  /* true to shift multixact base */
} xl_heap_base_shift;

#define SizeOfHeapBaseShift (offsetof(xl_heap_base_shift, multi) + sizeof(bool))

typedef struct xl_heap_new_cid {
    /*
     * store toplevel xid so we don't have to merge cids from different
     * transactions
     */
    TransactionId top_xid;
    CommandId cmin;
    CommandId cmax;
    CommandId combocid; /* just for debugging */

    /*
     * Store the relfilenode/ctid pair to facilitate lookups.
     */
    RelFileNodeOld target_node;
    ItemPointerData target_tid;
} xl_heap_new_cid;

#define SizeOfHeapNewCid (offsetof(xl_heap_new_cid, target_tid) + sizeof(ItemPointerData))

/* logical rewrite xlog record header */
typedef struct xl_heap_rewrite_mapping {
    TransactionId mapped_xid; /* xid that might need to see the row */
    Oid mapped_db;            /* DbOid or InvalidOid for shared rels */
    Oid mapped_rel;           /* Oid of the mapped relation */
    off_t offset;             /* How far have we written so far */
    uint32 num_mappings;      /* Number of in-memory mappings */
    XLogRecPtr start_lsn;     /* Insert LSN at begin of rewrite */
} xl_heap_rewrite_mapping;
extern void HeapTupleHeaderAdvanceLatestRemovedXid(HeapTuple tuple, TransactionId* latestRemovedXid);

/* HeapTupleHeader functions implemented in utils/time/combocid.c */
extern CommandId HeapTupleGetCmin(HeapTuple tup);
extern CommandId HeapTupleGetCmax(HeapTuple tup);
extern CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tup, Page page);
extern CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tup, Page page);
extern bool CheckStreamCombocid(HeapTupleHeader tup, CommandId current_cid, Page page);
extern void HeapTupleHeaderAdjustCmax(HeapTupleHeader tup, CommandId* cmax, bool* iscombo, Buffer buffer);
extern TransactionId HeapTupleMultiXactGetUpdateXid(HeapTuple tuple);
extern TransactionId HeapTupleHeaderMultiXactGetUpdateXid(Page page, HeapTupleHeader tuple);

/* ----------------
 *		fastgetattr && fastgetattr_with_dict
 *
 *		Fetch a user attribute's value as a Datum (might be either a
 *		value, or a pointer into the data area of the tuple).
 *
 *		This must not be used when a system attribute might be requested.
 *		Furthermore, the passed attnum MUST be valid.  Use heap_getattr()
 *		instead, if in doubt.
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocachegetattr() for the rest.
 * ----------------
 */

#if !defined(DISABLE_COMPLEX_MACRO)

#define fastgetattr(tup, attnum, tuple_desc, isnull)                                              \
    (AssertMacro(!HEAP_TUPLE_IS_COMPRESSED((tup)->t_data)),                                      \
        AssertMacro((attnum) > 0),                                                               \
        (*(isnull) = false),                                                                     \
        HeapTupleNoNulls(tup)                                                                    \
            ? ((tuple_desc)->attrs[(attnum)-1]->attcacheoff >= 0                                  \
                      ? (fetchatt((tuple_desc)->attrs[(attnum)-1],                                \
                            (char*)(tup)->t_data + (tup)->t_data->t_hoff +                       \
                                (tuple_desc)->attrs[(attnum)-1]->attcacheoff))                    \
                      : nocachegetattr((tup), (attnum), (tuple_desc)))                            \
            : (att_isnull((attnum)-1, (tup)->t_data->t_bits) ? ((*(isnull) = true), (Datum)NULL) \
                                                             : (nocachegetattr((tup), (attnum), (tuple_desc)))))

#define fastgetattr_with_dict(tup, attnum, tuple_desc, isnull, page_dict)           \
    (AssertMacro(HEAP_TUPLE_IS_COMPRESSED((tup)->t_data)),                        \
        AssertMacro((attnum) > 0),                                                \
        (*(isnull) = false),                                                      \
        (HeapTupleHasNulls(tup) && att_isnull((attnum)-1, (tup)->t_data->t_bits)) \
            ? ((*(isnull) = true), (Datum)NULL)                                   \
            : nocache_cmprs_get_attr(tup, attnum, tuple_desc, page_dict))

#else  /* defined(DISABLE_COMPLEX_MACRO) */

extern Datum fastgetattr_with_dict(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool* isnull, char* pageDict);
extern Datum fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool* isnull);
#endif /* defined(DISABLE_COMPLEX_MACRO) */

/* ----------------
 *		heap_getattr
 *
 *		Extract an attribute of a heap tuple and return it as a Datum.
 *		This works for either system or user attributes.  The given attnum
 *		is properly range-checked.
 *
 *		If the field in question has a NULL value, we return a zero Datum
 *		and set *isnull == true.  Otherwise, we set *isnull == false.
 *
 *		<tup> is the pointer to the heap tuple.  <attnum> is the attribute
 *		number of the column (field) caller wants.	<tupleDesc> is a
 *		pointer to the structure describing the row and all its fields.
 * ----------------
 */
#define heap_getattr_with_dict(tup, attnum, tuple_desc, isnull, pagedict)                                              \
    (((int)(attnum) > 0) ? (((int)(attnum) > (int)HeapTupleHeaderGetNatts((tup)->t_data, tuple_desc))                            \
                              ? (/* get init default value from tupleDesc.*/                                          \
                                    heapGetInitDefVal((attnum), (tuple_desc), (isnull)))                               \
                              : (HEAP_TUPLE_IS_COMPRESSED((tup)->t_data)                                              \
                                        ? (fastgetattr_with_dict((tup), (attnum), (tuple_desc), (isnull), (pagedict))) \
                                        : (fastgetattr((tup), (attnum), (tuple_desc), (isnull)))))                     \
                    : heap_getsysattr((tup), (attnum), (tuple_desc), (isnull)))

#define heap_getattr(tup, attnum, tuple_desc, isnull) heap_getattr_with_dict(tup, attnum, tuple_desc, isnull, NULL)

/* prototypes for functions in common/heaptuple.c */
extern Size heap_compute_data_size(TupleDesc tuple_desc, Datum* values, const bool* isnull);

extern void heap_fill_tuple(
    TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit);
extern bool heap_attisnull(HeapTuple tup, int attnum, TupleDesc tuple_desc);
extern Datum nocache_cmprs_get_attr(HeapTuple tuple, uint32 attnum, TupleDesc tuple_desc, char* page_dict);
extern Datum nocachegetattr(HeapTuple tup, uint32 attnum, TupleDesc att);
extern Datum heap_getsysattr(HeapTuple tup, int attnum, TupleDesc tuple_desc, bool* isnull);

extern HeapTuple heap_copytuple(HeapTuple tuple);
extern HeapTuple heapCopyCompressedTuple(HeapTuple tuple, TupleDesc tup_desc, Page dict_page, HeapTuple dest = NULL);
extern HeapTuple heapCopyTuple(HeapTuple tuple, TupleDesc tup_desc, Page page);

#define COPY_TUPLE_HEADERINFO(_dest_tup, _src_tup)          \
    do {                                                  \
        (_dest_tup)->t_choice = (_src_tup)->t_choice;       \
        (_dest_tup)->t_ctid = (_src_tup)->t_ctid;           \
        (_dest_tup)->t_infomask2 = (_src_tup)->t_infomask2; \
        (_dest_tup)->t_infomask = (_src_tup)->t_infomask;   \
    } while (0)

#define COPY_TUPLE_HEADER_XACT_INFO(_dest_tuple, _src_tuple)                                                            \
    do {                                                                                                              \
        HeapTupleHeader _dest = (_dest_tuple)->t_data;                                                                 \
        HeapTupleHeader _src = (_src_tuple)->t_data;                                                                   \
        (_dest)->t_choice = (_src)->t_choice;                                                                         \
        (_dest)->t_ctid = (_src)->t_ctid;                                                                             \
        (_dest)->t_infomask2 = (((_dest)->t_infomask2 & ~HEAP2_XACT_MASK) | ((_src)->t_infomask2 & HEAP2_XACT_MASK)); \
        (_dest)->t_infomask = (((_dest)->t_infomask & ~HEAP_XACT_MASK) | ((_src)->t_infomask & HEAP_XACT_MASK));      \
    } while (0)

extern void heap_copytuple_with_tuple(HeapTuple src, HeapTuple dest);

extern HeapTuple heap_form_tuple(TupleDesc tuple_descriptor, Datum* values, bool* isnull);
extern HeapTuple heap_form_cmprs_tuple(TupleDesc tuple_descriptor, FormCmprTupleData* cmprs_info);

extern HeapTuple heap_modify_tuple(
    HeapTuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* repl_isnull, const bool* do_replace);

extern void heap_deform_cmprs_tuple(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, char* cmprs_info);
extern void heap_deform_tuple(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull);
extern void heap_deform_tuple2(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, Buffer buffer);
extern void heap_deform_tuple3(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, Page page);

/* these three are deprecated versions of the three above: */
extern HeapTuple heap_formtuple(TupleDesc tuple_descriptor, Datum* values, const char* nulls);
extern HeapTuple heap_modifytuple(
    HeapTuple tuple, TupleDesc tuple_desc, Datum* repl_values, const char* repl_nulls, const char* repl_actions);
extern void heap_deformtuple(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, char* nulls);
extern void heap_freetuple(HeapTuple htup);

#define heap_freetuple_ext(htup)  \
    do {                          \
        if ((htup) != NULL) {     \
            heap_freetuple((HeapTuple)htup); \
            htup = NULL;          \
        }                         \
    } while (0)

extern MinimalTuple heap_form_minimal_tuple(
    TupleDesc tuple_descriptor, Datum* values, const bool* isnull, MinimalTuple in_tuple = NULL);
extern void heap_free_minimal_tuple(MinimalTuple mtup);
extern MinimalTuple heap_copy_minimal_tuple(MinimalTuple mtup);
extern HeapTuple heap_tuple_from_minimal_tuple(MinimalTuple mtup);
extern MinimalTuple minimal_tuple_from_heap_tuple(HeapTuple htup);

extern Datum heapGetInitDefVal(int att_num, TupleDesc tuple_desc, bool* is_null);
extern bool relationAttIsNull(HeapTuple tup, int att_num, TupleDesc tuple_desc);
extern MinimalTuple heapFormMinimalTuple(HeapTuple tuple, TupleDesc tuple_desc);

extern MinimalTuple heapFormMinimalTuple(HeapTuple tuple, TupleDesc tuple_desc, Page page);

/* for GPI clean up metadata */
typedef bool (*KeepInvisbleTupleFunc)(Datum checkDatum);
typedef struct KeepInvisbleOpt {
    Oid tableOid;
    int checkAttnum;
    KeepInvisbleTupleFunc checkKeepFunc;
} KeepInvisbleOpt;

bool HeapKeepInvisibleTuple(HeapTuple tuple, TupleDesc tupleDesc, KeepInvisbleTupleFunc checkKeepFunc = NULL);
void HeapCopyTupleNoAlloc(HeapTuple dest, HeapTuple src);

// for ut test
extern HeapTuple test_HeapUncompressTup2(HeapTuple tuple, TupleDesc tuple_desc, Page dict_page);

/*
 * Prefix for delete delta table name used in redis.
 * There is no other suitable common header file included in pg_redis.cpp, so defining it here.
 */
#define REDIS_DELETE_DELTA_TABLE_PREFIX "pg_delete_delta_"
#define REDIS_MULTI_CATCHUP_DELETE_DELTA_TABLE_PREFIX "pg_delete_delta_x_"

#endif /* HTUP_H */
