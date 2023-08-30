/* -------------------------------------------------------------------------
 *
 * knl_utuptoaster.cpp
 * the tuple toaster for ustore.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_utuptoaster.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "nodes/relation.h"
#include "access/tuptoaster.h"
#include "access/ustore/knl_utuptoaster.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/pg_lzcompress.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/typcache.h"
#include "commands/vacuum.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"

static void UHeapToastDeleteDatum(Relation rel, Datum value, int options);
static Datum UHeapToastSaveDatum(Relation rel, Datum value, struct varlena *oldexternal, int options);
static Datum UHeapToastCompressDatum(Datum value);
static bool UHeapToastIdValueIdExists(Oid toastrelid, Oid valueid, int2 bucketid);
static bool UHeapToastRelValueidExists(Relation toastrel, Oid valueid);
static Oid UHeapGetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn);

static Datum UHeapToastCompressDatum(Datum value)
{
    return toast_compress_datum(value);
}

Oid UHeapGetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
    Oid newOid;
    SysScanDesc scan;
    ScanKeyData key;
    bool collides = false;
    Assert(RelationIsUstoreFormat(relation) || RelationIsToast(relation));
    TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(relation), false, relation->rd_tam_ops);
    /* Generate new OIDs until we find one not in the table */
    do {
        CHECK_FOR_INTERRUPTS();
        /*
         * See comments in GetNewObjectId.
         * In the future, we might turn to SnapshotToast when getting new
         * chunk_id for toast datum to prevent wrap around.
         */
        newOid = GetNewObjectId(IsToastNamespace(RelationGetNamespace(relation)));

        ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(newOid));

        /* see notes above about using SnapshotAny */
        scan = systable_beginscan(relation, indexId, true, SnapshotAny, ATTR_FIRST, &key);
        collides = UHeapSysIndexGetnextSlot(scan, ForwardScanDirection, slot);

        systable_endscan(scan);
    } while (collides);
    ExecDropSingleTupleTableSlot(slot);
    return newOid;
}

void UHeapToastDelete(Relation relation, UHeapTuple utuple)
{
    TupleDesc tupleDesc;
    int numAttrs;
    int i;
    Datum toastValues[MaxHeapAttributeNumber] = {0};
    bool toastIsNull[MaxHeapAttributeNumber] = {0};

    WHITEBOX_TEST_STUB(UHEAP_TOAST_DELETE_FAILED, WhiteboxDefaultErrorEmit);

    Assert(relation->rd_rel->relkind == RELKIND_RELATION);
    Assert(utuple->tupTableType == UHEAP_TUPLE);

    tupleDesc = relation->rd_att;
    numAttrs = tupleDesc->natts;

    Assert(numAttrs <= MaxHeapAttributeNumber);
    UHeapDeformTuple(utuple, tupleDesc, toastValues, toastIsNull);

    /*
     * Check for external stored attributes and delete them from the secondary
     * relation.
     */
    for (i = 0; i < numAttrs; i++) {
        if (((TupleDescAttr(tupleDesc, i)))->attlen == -1) {
            Datum value = toastValues[i];

            if (toastIsNull[i]) {
                continue;
            } else if (VARATT_IS_EXTERNAL_ONDISK(PointerGetDatum(value))) {
                UHeapToastDeleteDatum(relation, value, 0);
            }
        }
    }
}

UHeapTuple UHeapToastInsertOrUpdate(Relation relation, UHeapTuple newtup, UHeapTuple oldtup, int options)
{
    UHeapTuple resTup;
    TupleDesc tupleDesc;
    int numAttrs;
    int i;

    bool needChange = false;
    bool needFree = false;
    bool needDelOld = false;
    bool hasNulls = false;

    Size maxDataLen;
    Size hoff;

    char toastAction[MaxHeapAttributeNumber] = {0};
    bool toastIsNull[MaxHeapAttributeNumber] = {0};
    bool toastOldIsNull[MaxHeapAttributeNumber] = {0};
    Datum toastValues[MaxHeapAttributeNumber] = {0};
    Datum toastOldValues[MaxHeapAttributeNumber]  = {0};
    struct varlena *toastOldExternal[MaxHeapAttributeNumber] = {0};
    uint32 toastSizes[MaxHeapAttributeNumber] = {0};
    bool toastFree[MaxHeapAttributeNumber] = {0};
    bool toastDelOld[MaxHeapAttributeNumber] = {0};

    bool enableReserve = u_sess->attr.attr_storage.reserve_space_for_nullable_atts;

    WHITEBOX_TEST_STUB(UHEAP_TOAST_INSERT_UPDATE_FAILED, WhiteboxDefaultErrorEmit);
    
    /*
     * We should only ever be called for tuples of plain relations or
     * materialized views --- recursing on a toast rel is bad news.
     */
    Assert(relation->rd_rel->relkind == RELKIND_RELATION);

    /*
     * Get the tuple descriptor and break down the tuple(s) into fields.
     */
    tupleDesc = relation->rd_att;
    numAttrs = tupleDesc->natts;
    
    bool enableReverseBitmap = NAttrsReserveSpace(numAttrs);
    enableReserve = enableReserve && enableReverseBitmap;
    Assert(!enableReserve || (enableReserve && enableReverseBitmap));
    Assert(enableReverseBitmap || (enableReverseBitmap == false && enableReserve == false));
    Assert(numAttrs <= MaxHeapAttributeNumber);

    UHeapDeformTuple(newtup, tupleDesc, toastValues, toastIsNull);

    if (oldtup != NULL)
        UHeapDeformTuple(oldtup, tupleDesc, toastOldValues, toastOldIsNull);

    errno_t rc = memset_s(toastAction, sizeof(toastAction), ' ', numAttrs * sizeof(char));
    securec_check(rc, "\0", "\0");
    rc = memset_s(toastOldExternal, sizeof(toastOldExternal), 0, numAttrs * sizeof(struct varlena *));
    securec_check(rc, "\0", "\0");
    rc = memset_s(toastFree, sizeof(toastFree), 0, numAttrs * sizeof(bool));
    securec_check(rc, "\0", "\0");
    rc = memset_s(toastDelOld, sizeof(toastDelOld), 0, numAttrs * sizeof(bool));
    securec_check(rc, "\0", "\0");

    for (i = 0; i < numAttrs; i++) {
        Form_pg_attribute att = (TupleDescAttr(tupleDesc, i));
        struct varlena *oldValue = NULL;
        struct varlena *newValue = NULL;

        if (oldtup != NULL) {
            /*
             * For UPDATE get the old and new values of this attribute
             */
            oldValue = (struct varlena *)DatumGetPointer(toastOldValues[i]);
            newValue = (struct varlena *)DatumGetPointer(toastValues[i]);

            /*
             * If the old value is stored on disk, check if it has changed so
             * we have to delete it later.
             */
            if (att->attlen == -1 && !toastOldIsNull[i] && VARATT_IS_EXTERNAL_ONDISK(oldValue)) {
                if (toastIsNull[i] || !VARATT_IS_EXTERNAL_ONDISK(newValue) || RelationIsLogicallyLogged(relation) ||
                    memcmp((char *)oldValue, (char *)newValue, VARSIZE_EXTERNAL(oldValue)) != 0) {
                    /*
                     * The old external stored value isn't needed any more
                     * after the update
                     */
                    toastDelOld[i] = true;
                    needDelOld = true;
                } else {
                    /*
                     * This attribute isn't changed by this update so we reuse
                     * the original reference to the old value in the new
                     * tuple.
                     */
                    toastAction[i] = 'p';
                    continue;
                }
            }
        } else {
            /*
             * For INSERT simply get the new value
             */
            newValue = (struct varlena *)DatumGetPointer(toastValues[i]);
        }

        if (toastIsNull[i]) {
            toastAction[i] = 'p';
            hasNulls = true;
            continue;
        }

        /*
         * Now look at varlena attributes
         */
        if (att->attlen == -1) {
            if (VARATT_IS_HUGE_TOAST_POINTER(newValue)) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support clob/blob type more than 1GB of Ustore")));
            }
            /*
             * If the table's attribute says PLAIN always, force it so.
             */
            if (att->attstorage == 'p')
                toastAction[i] = 'p';

            /*
             * We took care of UPDATE above, so any external value we find
             * still in the tuple must be someone else's that we cannot reuse
             * (this includes the case of an out-of-line in-memory datum).
             * Fetch it back (without decompression, unless we are forcing
             * PLAIN storage).  If necessary, we'll push it out as a new
             * external value below.
             */
            if (VARATT_IS_EXTERNAL(newValue)) {
                toastOldExternal[i] = newValue;
                if (att->attstorage == 'p')
                    newValue = heap_tuple_untoast_attr(newValue);
                else
                    newValue = heap_tuple_fetch_attr(newValue);
                toastValues[i] = PointerGetDatum(newValue);
                toastFree[i] = true;
                needChange = true;
                needFree = true;
            }

            /*
             * Remember the size of this attribute
             */
            toastSizes[i] = VARSIZE_ANY(newValue);
        } else {
            /*
             * Not a varlena attribute, plain storage always
             */
            toastAction[i] = 'p';
        }
    }

    /* ----------
     * Compress and/or save external until data fits into target length
     *
     * 1: Inline compress attributes with attstorage 'x', and store very
     * large attributes with attstorage 'x' or 'e' external immediately
     * 2: Store attributes with attstorage 'x' or 'e' external
     * 3: Inline compress attributes with attstorage 'm'
     * 4: Store attributes with attstorage 'm' external
     * ----------
     */

    /* compute header overhead --- this should match heap_form_tuple() */
    hoff = SizeOfUHeapDiskTupleData;

    if (hasNulls) {
        int nullcount = 0;
        int i = 0;
        for (i = 0; i < numAttrs; i++) {
            if (toastIsNull[i]) {
                nullcount++;
            }
        }
        if (enableReverseBitmap) {
            hoff += BITMAPLEN(numAttrs + nullcount);
        } else {
            hoff += BITMAPLEN(numAttrs);
        }
    }

    /* now convert to a limit on the tuple data size */
    maxDataLen = UTOAST_TUPLE_TARGET - hoff;

    /*
     * Look for attributes with attstorage 'x' to compress.  Also find large
     * attributes with attstorage 'x' or 'e', and store them external.
     */
    while (UHeapCalcTupleDataSize(tupleDesc, toastValues, toastIsNull, hoff, enableReverseBitmap, enableReserve) > 
            maxDataLen) {
        int biggestAttno = -1;
        uint32 biggestSize = MAXALIGN(TOAST_POINTER_SIZE);
        Datum oldValue;
        Datum newValue;

        /*
         * Search for the biggest yet unprocessed internal attribute
         */
        for (i = 0; i < numAttrs; i++) {
            Form_pg_attribute att = (TupleDescAttr(tupleDesc, i));

            if (toastAction[i] != ' ')
                continue;
            if (VARATT_IS_EXTERNAL(DatumGetPointer(toastValues[i])))
                continue; /* can't happen, toast_action would be 'p' */
            if (VARATT_IS_COMPRESSED(DatumGetPointer(toastValues[i])))
                continue;
            if (att->attstorage != 'x' && att->attstorage != 'e')
                continue;
            if (toastSizes[i] > biggestSize) {
                biggestAttno = i;
                biggestSize = toastSizes[i];
            }
        }

        if (biggestAttno < 0) {
            break;
        }

        /*
         * Attempt to compress it inline, if it has attstorage 'x'
         */
        i = biggestAttno;
        if (((TupleDescAttr(tupleDesc, i)))->attstorage == 'x') {
            oldValue = toastValues[i];
            newValue = UHeapToastCompressDatum(oldValue);
            if (DatumGetPointer(newValue) != NULL) {
                /* successful compression */
                if (toastFree[i])
                    pfree(DatumGetPointer(oldValue));
                toastValues[i] = newValue;
                toastFree[i] = true;
                toastSizes[i] = VARSIZE(DatumGetPointer(toastValues[i]));
                needChange = true;
                needFree = true;
            } else {
                /* incompressible, ignore on subsequent compression passes */
                toastAction[i] = 'x';
            }
        } else {
            /* has attstorage 'e', ignore on subsequent compression passes */
            toastAction[i] = 'x';
        }

        /*
         * If this value is by itself more than maxDataLen (after compression
         * if any), push it out to the toast table immediately, if possible.
         * This avoids uselessly compressing other fields in the common case
         * where we have one long field and several short ones.
         *
         * XXX maybe the threshold should be less than maxDataLen?
         */
        if (toastSizes[i] > maxDataLen && relation->rd_rel->reltoastrelid != InvalidOid) {
            oldValue = toastValues[i];
            toastAction[i] = 'p';
            toastValues[i] = UHeapToastSaveDatum(relation, toastValues[i], toastOldExternal[i], options);
            if (toastFree[i])
                pfree(DatumGetPointer(oldValue));
            toastFree[i] = true;
            needChange = true;
            needFree = true;
        }
    }

    /*
     * Second we look for attributes of attstorage 'x' or 'e' that are still
     * inline.  But skip this if there's no toast table to push them to.
     */
    while (UHeapCalcTupleDataSize(tupleDesc, toastValues, toastIsNull, hoff, enableReverseBitmap, enableReserve) >
            maxDataLen && relation->rd_rel->reltoastrelid != InvalidOid) {
        int biggestAttno = -1;
        uint32 biggestSize = MAXALIGN(TOAST_POINTER_SIZE);
        Datum oldValue;

        /* ------
         * Search for the biggest yet inlined attribute with
         * attstorage equals 'x' or 'e'
         * ------
         */
        for (i = 0; i < numAttrs; i++) {
            Form_pg_attribute att = (TupleDescAttr(tupleDesc, i));

            if (toastAction[i] == 'p')
                continue;
            if (VARATT_IS_EXTERNAL(DatumGetPointer(toastValues[i])))
                continue; /* can't happen, toast_action would be 'p' */
            if (att->attstorage != 'x' && att->attstorage != 'e')
                continue;
            if (toastSizes[i] > biggestSize) {
                biggestAttno = i;
                biggestSize = toastSizes[i];
            }
        }

        if (biggestAttno < 0) {
            break;
        }

        /*
         * Store this external
         */
        i = biggestAttno;
        oldValue = toastValues[i];
        toastAction[i] = 'p';
        toastValues[i] = UHeapToastSaveDatum(relation, toastValues[i], toastOldExternal[i], options);
        if (toastFree[i])
            pfree(DatumGetPointer(oldValue));
        toastFree[i] = true;

        needChange = true;
        needFree = true;
    }

    /*
     * Round 3 - this time we take attributes with storage 'm' into
     * compression
     */
    while (UHeapCalcTupleDataSize(tupleDesc, toastValues, toastIsNull, hoff, enableReverseBitmap, enableReserve) > 
            maxDataLen) {
        int biggestAttno = -1;
        uint32 biggestSize = MAXALIGN(TOAST_POINTER_SIZE);
        Datum oldValue;
        Datum newValue;

        /*
         * Search for the biggest yet uncompressed internal attribute
         */
        for (i = 0; i < numAttrs; i++) {
            if (toastAction[i] != ' ')
                continue;
            if (VARATT_IS_EXTERNAL(DatumGetPointer(toastValues[i])))
                continue; /* can't happen, toast_action would be 'p' */
            if (VARATT_IS_COMPRESSED(DatumGetPointer(toastValues[i])))
                continue;
            if (((TupleDescAttr(tupleDesc, i)))->attstorage != 'm')
                continue;
            if (toastSizes[i] > biggestSize) {
                biggestAttno = i;
                biggestSize = toastSizes[i];
            }
        }

        if (biggestAttno < 0) {
            break;
        }

        /*
         * Attempt to compress it inline
         */
        i = biggestAttno;
        oldValue = toastValues[i];
        newValue = UHeapToastCompressDatum(oldValue);
        if (DatumGetPointer(newValue) != NULL) {
            /* successful compression */
            if (toastFree[i])
                pfree(DatumGetPointer(oldValue));
            toastValues[i] = newValue;
            toastFree[i] = true;
            toastSizes[i] = VARSIZE(DatumGetPointer(toastValues[i]));
            needChange = true;
            needFree = true;
        } else {
            /* incompressible, ignore on subsequent compression passes */
            toastAction[i] = 'x';
        }
    }

    /*
     * Finally we store attributes of type 'm' externally.  At this point we
     * increase the target tuple size, so that 'm' attributes aren't stored
     * externally unless really necessary.
     */
    maxDataLen = MaxUHeapTupleSize(relation) - hoff;

    while (UHeapCalcTupleDataSize(tupleDesc, toastValues, toastIsNull, hoff, enableReverseBitmap,enableReserve) >
            maxDataLen && relation->rd_rel->reltoastrelid != InvalidOid) {
        int biggestAttno = -1;
        uint32 biggestSize = MAXALIGN(TOAST_POINTER_SIZE);
        Datum oldValue;

        /* --------
         * Search for the biggest yet inlined attribute with
         * attstorage = 'm'
         * --------
         */
        for (i = 0; i < numAttrs; i++) {
            if (toastAction[i] == 'p')
                continue;
            if (VARATT_IS_EXTERNAL(DatumGetPointer(toastValues[i])))
                continue; /* can't happen, toast_action would be 'p' */
            if (((TupleDescAttr(tupleDesc, i)))->attstorage != 'm')
                continue;
            if (toastSizes[i] > biggestSize) {
                biggestAttno = i;
                biggestSize = toastSizes[i];
            }
        }

        if (biggestAttno < 0)
            break;

        /*
         * Store this external
         */
        i = biggestAttno;
        oldValue = toastValues[i];
        toastAction[i] = 'p';
        toastValues[i] = UHeapToastSaveDatum(relation, toastValues[i], toastOldExternal[i], options);

        if (toastFree[i])
            pfree(DatumGetPointer(oldValue));
        toastFree[i] = true;

        needChange = true;
        needFree = true;
    }

    /*
     * In the case we toasted any values, we need to build a new heap tuple
     * with the changed values.
     */
    if (needChange) {
        UHeapDiskTuple oldData = newtup->disk_tuple;
        UHeapDiskTuple newData;
        int32 newHeaderLen;
        int32 newDataLen;
        int32 newTupleLen;
        errno_t rc;

        /*
         * Calculate the new size of the tuple.
         *
         * Note: we used to assume here that the old tuple's t_hoff must equal
         * the new_header_len value, but that was incorrect.  The old tuple
         * might have a smaller-than-current natts, if there's been an ALTER
         * TABLE ADD COLUMN since it was stored; and that would lead to a
         * different conclusion about the size of the null bitmap, or even
         * whether there needs to be one at all.
         */
        newHeaderLen = SizeOfUHeapDiskTupleData;
        if (hasNulls) {
            int nullcount = 0;
            int i = 0;
            for (i = 0; i < numAttrs; i++) {
                if (toastIsNull[i]) {
                    nullcount++;
                }
            }
            if (enableReverseBitmap) {
                newHeaderLen += BITMAPLEN(numAttrs + nullcount);
            } else {
                newHeaderLen += BITMAPLEN(numAttrs);
            }
        }
        newDataLen = UHeapCalcTupleDataSize(tupleDesc, toastValues, toastIsNull, newHeaderLen, enableReverseBitmap, 
            enableReserve);
        newTupleLen = newHeaderLen + newDataLen;

        /*
         * Allocate and zero the space needed, and fill InplaceHeapTupleData fields.
         */
        resTup = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize + newTupleLen);
        resTup->disk_tuple_size = newTupleLen;
        resTup->ctid = newtup->ctid;
        resTup->table_oid = newtup->table_oid;
        resTup->xc_node_id = newtup->xc_node_id;
        newData = (UHeapDiskTuple)((char *)resTup + UHeapTupleDataSize);
        resTup->disk_tuple = newData;

        /*
         * Copy the existing tuple header, but adjust natts and t_hoff. Also clear
         * the first three bits of the flag, which are reset in UHeapFillDiskTuple
         */
        rc = memcpy_s(newData, SizeOfUHeapDiskTupleData, oldData, SizeOfUHeapDiskTupleData);
        securec_check(rc, "\0", "\0");

        UHeapTupleHeaderSetNatts(newData, numAttrs);
        newData->t_hoff = newHeaderLen;
        newData->flag &= UHEAP_VIS_STATUS_MASK;

        if (hasNulls) {
            UHeapFillDiskTuple<true>(tupleDesc, toastValues, toastIsNull, newData, newDataLen, enableReverseBitmap,
                    enableReserve);
        } else {
            UHeapFillDiskTuple<false>(tupleDesc, toastValues, toastIsNull, newData, newDataLen, enableReverseBitmap,
                    enableReserve);
        }
    } else {
        resTup = newtup;
    }

    /*
     * Free allocated temp values
     */
    if (needFree)
        for (i = 0; i < numAttrs; i++)
            if (toastFree[i])
                pfree(DatumGetPointer(toastValues[i]));

    /*
     * Delete external values from the old tuple
     */
    if (needDelOld)
        for (i = 0; i < numAttrs; i++)
            if (toastDelOld[i])
                UHeapToastDeleteDatum(relation, toastOldValues[i], 0);

    return resTup;
}
/* ----------
 * toast_save_datum -
 *
 * 	Save one single datum into the secondary relation and return
 * 	a Datum reference for it.
 *
 * rel: the main relation we're working with (not the toast rel!)
 * value: datum to be pushed to toast storage
 * oldexternal: if not NULL, toast pointer previously representing the datum
 * options: options to be passed to heap_insert() for toast rows
 * ----------
 */
static Datum UHeapToastSaveDatum(Relation rel, Datum value, struct varlena *oldexternal, int options)
{
    Relation toastrel;
    Relation toastidx;
    UHeapTuple toasttup;
    TupleDesc toastTupDesc;
    Datum tValues[3] = {0};
    bool tIsnull[3] = {0};
    CommandId mycid = GetCurrentCommandId(true);
    struct varlena *result = NULL;
    struct varatt_external toastPointer;
    union {
        struct varlena hdr;
        char data[UTOAST_MAX_CHUNK_SIZE + sizeof(struct varlena) + sizeof(int32)]; /* make struct big enough */
        int32 align_it;                   /* ensure struct is aligned well enough */
    } chunkData;
    int32 chunkSize;
    int32 chunkSeq = 0;
    char *dataP = NULL;
    int32 dataTodo;
    Pointer dval = DatumGetPointer(value);
    errno_t rc;
    int2 bucketid = InvalidBktId;
    Assert(!VARATT_IS_EXTERNAL(value));
    rc = memset_s(&chunkData, sizeof(chunkData), 0, sizeof(chunkData));
    securec_check(rc, "", "");

    /*
     * Open the toast relation and its index.  We can use the index to check
     * uniqueness of the OID we assign to the toasted item, even though it has
     * additional columns besides OID.
     */
    if (RelationIsBucket(rel)) {
        bucketid = rel->rd_node.bucketNode;
    }
    toastrel = heap_open(rel->rd_rel->reltoastrelid, RowExclusiveLock, bucketid);
    toastTupDesc = toastrel->rd_att;
    toastidx = index_open(toastrel->rd_rel->reltoastidxid, RowExclusiveLock, bucketid);

    /*
     * Get the data pointer and length, and compute va_rawsize and va_extsize.
     *
     * va_rawsize is the size of the equivalent fully uncompressed datum, so
     * we have to adjust for short headers.
     *
     * va_extsize is the actual size of the data payload in the toast records.
     */
    if (VARATT_IS_SHORT(dval)) {
        dataP = VARDATA_SHORT(dval);
        dataTodo = VARSIZE_SHORT(dval) - VARHDRSZ_SHORT;
        toastPointer.va_rawsize = dataTodo + VARHDRSZ; /* as if not short */
        toastPointer.va_extsize = dataTodo;
    } else if (VARATT_IS_COMPRESSED(dval)) {
        dataP = VARDATA(dval);
        dataTodo = VARSIZE(dval) - VARHDRSZ;
        /* rawsize in a compressed datum is just the size of the payload */
        toastPointer.va_rawsize = VARRAWSIZE_4B_C(dval) + VARHDRSZ;
        toastPointer.va_extsize = dataTodo;
        /* Assert that the numbers look like it's compressed */
        Assert(VARATT_EXTERNAL_IS_COMPRESSED(toastPointer));
    } else {
        dataP = VARDATA(dval);
        dataTodo = VARSIZE(dval) - VARHDRSZ;
        toastPointer.va_rawsize = VARSIZE(dval);
        toastPointer.va_extsize = dataTodo;
    }

    /*
     * Insert the correct table OID into the result TOAST pointer.
     *
     * Normally this is the actual OID of the target toast table, but during
     * table-rewriting operations such as CLUSTER, we have to insert the OID
     * of the table's real permanent toast table instead.  rd_toastoid is set
     * if we have to substitute such an OID.
     */
    if (OidIsValid(rel->rd_toastoid))
        toastPointer.va_toastrelid = rel->rd_toastoid;
    else
        toastPointer.va_toastrelid = RelationGetRelid(toastrel);

    /*
     * Choose an OID to use as the value ID for this toast value.
     *
     * Normally we just choose an unused OID within the toast table.  But
     * during table-rewriting operations where we are preserving an existing
     * toast table OID, we want to preserve toast value OIDs too.  So, if
     * rd_toastoid is set and we had a prior external value from that same
     * toast table, re-use its value ID.  If we didn't have a prior external
     * value (which is a corner case, but possible if the table's attstorage
     * options have been changed), we have to pick a value ID that doesn't
     * conflict with either new or existing toast value OIDs.
     */
    if (!OidIsValid(rel->rd_toastoid)) {
        /* normal case: just choose an unused OID */
        toastPointer.va_valueid = UHeapGetNewOidWithIndex(toastrel, RelationGetRelid(toastidx), (AttrNumber)1);
    } else {
        /* rewrite case: check to see if value was in old toast table */
        toastPointer.va_valueid = InvalidOid;
        if (oldexternal != NULL) {
            struct varatt_external oldToastPointer;
            int2 toastbid;

            Assert(VARATT_IS_EXTERNAL_ONDISK_B(oldexternal));
            /* Must copy to access aligned fields */
            VARATT_EXTERNAL_GET_POINTER_B(oldToastPointer, oldexternal, toastbid);
            if (oldToastPointer.va_toastrelid == rel->rd_toastoid) {
                Assert(bucketid == toastbid);
                /* This value came from the old toast table; reuse its OID */
                toastPointer.va_valueid = oldToastPointer.va_valueid;

                /*
                 * There is a corner case here: the table rewrite might have
                 * to copy both live and recently-dead versions of a row, and
                 * those versions could easily reference the same toast value.
                 * When we copy the second or later version of such a row,
                 * reusing the OID will mean we select an OID that's already
                 * in the new toast table.	Check for that, and if so, just
                 * fall through without writing the data again.
                 *
                 * While annoying and ugly-looking, this is a good thing
                 * because it ensures that we wind up with only one copy of
                 * the toast value when there is only one copy in the old
                 * toast table.  Before we detected this case, we'd have made
                 * multiple copies, wasting space; and what's worse, the
                 * copies belonging to already-deleted heap tuples would not
                 * be reclaimed by VACUUM.
                 */
                if (UHeapToastRelValueidExists(toastrel, toastPointer.va_valueid)) {
                    /* Match, so short-circuit the data storage loop below */
                    dataTodo = 0;
                }
            }
        }
        if (toastPointer.va_valueid == InvalidOid) {
            /*
             * new value; must choose an OID that doesn't conflict in either
             * old or new toast table
             */
            do {
                toastPointer.va_valueid = UHeapGetNewOidWithIndex(toastrel, RelationGetRelid(toastidx), (AttrNumber)1);
            } while (UHeapToastIdValueIdExists(rel->rd_toastoid, toastPointer.va_valueid, bucketid));
        }
    }

    /*
     * Initialize constant parts of the tuple data
     */
    tValues[ATTR_FIRST - 1] = ObjectIdGetDatum(toastPointer.va_valueid);
    tValues[ATTR_SECOND] = PointerGetDatum(&chunkData);
    tIsnull[ATTR_FIRST - 1] = false;
    tIsnull[ATTR_SECOND - 1] = false;
    tIsnull[ATTR_THIRD - 1] = false;

    /*
     * Split up the item into chunks
     */
    while (dataTodo > 0) {
        /*
         * Calculate the size of this chunk
         */
        chunkSize = Min(UTOAST_MAX_CHUNK_SIZE, (uint32)dataTodo);

        /*
         * Build a tuple and store it
         */
        tValues[1] = Int32GetDatum(chunkSeq++);
        SET_VARSIZE(&chunkData, chunkSize + VARHDRSZ);
        rc = memcpy_s(VARDATA(&chunkData), UTOAST_MAX_CHUNK_SIZE, dataP, chunkSize);
        securec_check(rc, "", "");
        toasttup = UHeapFormTuple(toastTupDesc, tValues, tIsnull);

        (void)UHeapInsert(toastrel, toasttup, mycid, NULL, true);

        /*
         * Create the index entry.	We cheat a little here by not using
         * FormIndexDatum: this relies on the knowledge that the index columns
         * are the same as the initial columns of the table.
         *
         * Note also that there had better not be any user-created index on
         * the TOAST table, since we don't bother to update anything else.
         */
        (void)index_insert(toastidx, tValues, tIsnull, &(toasttup->ctid), toastrel,
            toastidx->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);

        /*
         * Free memory
         */
        UHeapFreeTuple(toasttup);

        /*
         * Move on to next chunk
         */
        dataTodo -= chunkSize;
        dataP += chunkSize;
    }

    /*
     * Done - close toast relation
     */
    index_close(toastidx, RowExclusiveLock);
    heap_close(toastrel, RowExclusiveLock);

    /*
     * Create the TOAST pointer value that we'll return
     */
    bool isBucketRelation = RelationIsBucket(rel);
    Size resultSize = TOAST_POINTER_SIZE;
    if (isBucketRelation) {
        resultSize += sizeof(int2);
    }

    result = (struct varlena *)palloc(resultSize);

    if (isBucketRelation) {
        SET_VARTAG_EXTERNAL(result, VARTAG_BUCKET);
    } else {
        SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
    }

    rc = memcpy_s(VARDATA_EXTERNAL(result), TOAST_POINTER_SIZE, &toastPointer, sizeof(toastPointer));
    securec_check(rc, "", "");

    if (isBucketRelation) {
        rc = memcpy_s((char *)result + TOAST_POINTER_SIZE, sizeof(int2), &bucketid, sizeof(int2));
        securec_check(rc, "", "");
    }

    return PointerGetDatum(result);
}

static void UHeapToastDeleteDatum(Relation rel, Datum value, int options)
{
    struct varlena *attr = (struct varlena *)DatumGetPointer(value);
    struct varatt_external toastPointer;
    Relation toastrel;
    Relation toastidx;
    ScanKeyData toastkey;
    SysScanDesc toastscan;
    UHeapTuple toasttup;
    int2 bucketid;

    if (!VARATT_IS_EXTERNAL_ONDISK_B(attr))
        return;

    /* Must copy to access aligned fields */
    VARATT_EXTERNAL_GET_POINTER_B(toastPointer, attr, bucketid);

    /*
     * Open the toast relation and its index
     */

    Assert(bucketid == rel->rd_node.bucketNode);
    toastrel = heap_open(toastPointer.va_toastrelid, RowExclusiveLock, bucketid);
    toastidx = index_open(toastrel->rd_rel->reltoastidxid, RowExclusiveLock, bucketid);

    /* The toast table of ustore table should also be of ustore type */
    Assert(RelationIsUstoreFormat(toastrel));
    /* should index must be ustore format ? */
    TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(toastrel), false, toastrel->rd_tam_ops);

    /*
     * Setup a scan key to find chunks with matching va_valueid
     */
    ScanKeyInit(&toastkey, (AttrNumber)1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(toastPointer.va_valueid));

    /*
     * Find all the chunks.  (We don't actually care whether we see them in
     * sequence or not, but since we've already locked the index we might as
     * well use systable_beginscan_ordered.)
     */
    toastscan = systable_beginscan_ordered(toastrel, toastidx, SnapshotToast, 1, &toastkey);
    while (UHeapSysIndexGetnextSlot(toastscan, ForwardScanDirection, slot)) {
        /*
         * Have a chunk, delete it
         */
        toasttup = ExecGetUHeapTupleFromSlot(slot);
        SimpleUHeapDelete(toastrel, &toasttup->ctid, SnapshotToast);
    }

    /*
     * End scan and close relations
     */
    systable_endscan_ordered(toastscan);
    ExecDropSingleTupleTableSlot(slot);
    index_close(toastidx, RowExclusiveLock);
    heap_close(toastrel, RowExclusiveLock);
}

/* ----------
 * toast_fetch_datum -
 *
 * 	Reconstruct an in memory Datum from the chunks saved
 * 	in the toast relation
 * ----------
 */
struct varlena *UHeapInternalToastFetchDatum(struct varatt_external toastPointer, Relation toastrel, Relation toastidx)
{
    ScanKeyData toastkey;
    SysScanDesc toastscan;
    UHeapTuple ttup;
    TupleDesc toastTupDesc;
    struct varlena *result = NULL;
    int32 ressize;
    int32 residx, nextidx;
    int32 numchunks;
    Pointer chunk;
    bool isnull = false;
    char *chunkdata = NULL;
    int32 chunksize;

    ressize = toastPointer.va_extsize;
    numchunks = ((ressize - 1) / UTOAST_MAX_CHUNK_SIZE) + 1;

    result = (struct varlena *)palloc(ressize + VARHDRSZ);

    if (VARATT_EXTERNAL_IS_COMPRESSED(toastPointer))
        SET_VARSIZE_COMPRESSED(result, ressize + VARHDRSZ);
    else
        SET_VARSIZE(result, ressize + VARHDRSZ);

    toastTupDesc = toastrel->rd_att;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(toastrel), false, toastrel->rd_tam_ops);

    /*
     * Setup a scan key to fetch from the index by va_valueid
     */
    ScanKeyInit(&toastkey, (AttrNumber)1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(toastPointer.va_valueid));

    /*
     * Read the chunks by index
     *
     * Note that because the index is actually on (valueid, chunkidx) we will
     * see the chunks in chunkidx order, even though we didn't explicitly ask
     * for it.
     */
    nextidx = 0;

    toastscan = systable_beginscan_ordered(toastrel, toastidx, get_toast_snapshot(), 1, &toastkey);
    while (UHeapSysIndexGetnextSlot(toastscan, ForwardScanDirection, slot)) {
        /*
         * Have a chunk, extract the sequence number and the data
         */
        ttup = ExecGetUHeapTupleFromSlot(slot);
        residx = DatumGetInt32(UHeapFastGetAttr(ttup, ATTR_SECOND, toastTupDesc, &isnull));
        Assert(!isnull);
        chunk = DatumGetPointer(UHeapFastGetAttr(ttup, ATTR_THIRD, toastTupDesc, &isnull));
        Assert(!isnull);
        if (!VARATT_IS_EXTENDED(chunk)) {
            chunksize = VARSIZE(chunk) - VARHDRSZ;
            chunkdata = VARDATA(chunk);
        } else if (VARATT_IS_SHORT(chunk)) {
            /* could happen due to heap_form_tuple doing its thing */
            chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
            chunkdata = VARDATA_SHORT(chunk);
        } else {
            /* should never happen */
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE), errmsg("found toasted toast chunk for toast value %u in %s",
                    toastPointer.va_valueid, RelationGetRelationName(toastrel))));
            chunksize = 0; /* keep compiler quiet */
            chunkdata = NULL;
        }

        /*
         * Some checks on the data we've found
         */
        if (residx != nextidx) {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                errmsg("unexpected chunk number %d (expected %d) for toast value %u in %s", residx, nextidx,
                    toastPointer.va_valueid, RelationGetRelationName(toastrel))));
        }
        if (residx < numchunks - 1) {
            if (chunksize != UTOAST_MAX_CHUNK_SIZE) {
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                    errmsg("unexpected chunk size %d (expected %d) in chunk %d of %d for toast value %u in %s",
                        chunksize, (int)UTOAST_MAX_CHUNK_SIZE, residx, numchunks, toastPointer.va_valueid,
                        RelationGetRelationName(toastrel))));
            }
        } else if (residx == numchunks - 1) {
            if ((residx * UTOAST_MAX_CHUNK_SIZE + chunksize) != (uint32)ressize) {
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                    errmsg("unexpected chunk size %d (expected %d) in final chunk %d for toast value %u in %s",
                        chunksize, (int)(ressize - residx * UTOAST_MAX_CHUNK_SIZE), residx, toastPointer.va_valueid,
                        RelationGetRelationName(toastrel))));
            }
        } else {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                errmsg("unexpected chunk number %d (out of range %d..%d) for toast value %u in %s", residx, 0,
                    numchunks - 1, toastPointer.va_valueid, RelationGetRelationName(toastrel))));
        }
        /*
         * Copy the data into proper place in our result
         */
        if ((ressize < chunksize) && residx == 0) {
            pfree(result);
            result = (struct varlena*)palloc(chunksize + VARHDRSZ);
            ressize = chunksize;
        }
         
        errno_t rc = memcpy_s(VARDATA(result) + residx * UTOAST_MAX_CHUNK_SIZE,
            ressize + VARHDRSZ - residx * UTOAST_MAX_CHUNK_SIZE, chunkdata, chunksize);
        securec_check(rc, "", "");

        nextidx++;
    }

    /*
     * Final checks that we successfully fetched the datum
     */
    if (nextidx != numchunks) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE), errmsg("missing chunk number %d for toast value %u in %s",
                nextidx, toastPointer.va_valueid, RelationGetRelationName(toastrel))));
    }

    /*
     * End scan and close relations
     */
    systable_endscan_ordered(toastscan);
    ExecDropSingleTupleTableSlot(slot);

    return result;
}

struct varlena *UHeapInternalToastFetchDatumSlice(struct varatt_external toastPointer, Relation toastrel,
    Relation toastidx, int64 sliceoffset, int32 length)
{
    int32 attrsize;
    int32 residx;
    int32 nextidx;
    int numchunks;
    int startchunk;
    int endchunk;
    int32 startoffset;
    int32 endoffset;
    int totalchunks;
    Pointer chunk;
    bool isnull = false;
    char *chunkdata = NULL;
    int32 chunksize;
    int32 chcpystrt;
    int32 chcpyend;
    errno_t rc = EOK;
    ScanKeyData toastkey[3];
    int nscankeys;
    SysScanDesc toastscan;
    UHeapTuple ttup;
    TupleDesc toastTupDesc;

    struct varlena *result = NULL;
    /*
     * It's nonsense to fetch slices of a compressed datum -- this isn't lo_*
     * we can't return a compressed datum which is meaningful to toast later
     */
    Assert(!VARATT_EXTERNAL_IS_COMPRESSED(toastPointer));

    attrsize = toastPointer.va_extsize;
    totalchunks = ((attrsize - 1) / UTOAST_MAX_CHUNK_SIZE) + 1;

    if (sliceoffset >= attrsize) {
        sliceoffset = 0;
        length = 0;
    }

    if (((sliceoffset + length) > attrsize) || length < 0)
        length = attrsize - sliceoffset;

    result = (struct varlena *)palloc(length + VARHDRSZ);

    if (VARATT_EXTERNAL_IS_COMPRESSED(toastPointer))
        SET_VARSIZE_COMPRESSED(result, length + VARHDRSZ);
    else
        SET_VARSIZE(result, length + VARHDRSZ);

    if (length == 0)
        return result; /* Can save a lot of work at this point! */

    startchunk = sliceoffset / UTOAST_MAX_CHUNK_SIZE;
    endchunk = (sliceoffset + length - 1) / UTOAST_MAX_CHUNK_SIZE;
    numchunks = (endchunk - startchunk) + 1;

    startoffset = sliceoffset % UTOAST_MAX_CHUNK_SIZE;
    endoffset = (sliceoffset + length - 1) % UTOAST_MAX_CHUNK_SIZE;

    /*
     * Open the toast relation and its index
     */
    toastTupDesc = toastrel->rd_att;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(toastrel), false, toastrel->rd_tam_ops);

    /*
     * Setup a scan key to fetch from the index. This is either two keys or
     * three depending on the number of chunks.
     */
    ScanKeyInit(&toastkey[0], (AttrNumber)1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(toastPointer.va_valueid));

    /*
     * Use equality condition for one chunk, a range condition otherwise:
     */
    if (numchunks == ATTR_FIRST) {
        ScanKeyInit(&toastkey[1], (AttrNumber)ATTR_SECOND, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(startchunk));

        nscankeys = ATTR_SECOND;
    } else {
        ScanKeyInit(&toastkey[1], (AttrNumber)ATTR_SECOND, BTGreaterEqualStrategyNumber, F_INT4GE,
            Int32GetDatum(startchunk));

        ScanKeyInit(&toastkey[ATTR_SECOND], (AttrNumber)ATTR_SECOND, BTLessEqualStrategyNumber, F_INT4LE,
            Int32GetDatum(endchunk));

        nscankeys = ATTR_THIRD;
    }

    /*
     * Read the chunks by index
     * The index is on (valueid, chunkidx) so they will come in order
     */
    nextidx = startchunk;
    toastscan = systable_beginscan_ordered(toastrel, toastidx, get_toast_snapshot(), nscankeys, toastkey);
    while (UHeapSysIndexGetnextSlot(toastscan, ForwardScanDirection, slot)) {
        /*
         * Have a chunk, extract the sequence number and the data
         */
        ttup = ExecGetUHeapTupleFromSlot(slot);
        residx = DatumGetInt32(UHeapFastGetAttr(ttup, CHUNK_ID_ATTR, toastTupDesc, &isnull));
        Assert(!isnull);
        chunk = DatumGetPointer(UHeapFastGetAttr(ttup, CHUNK_DATA_ATTR, toastTupDesc, &isnull));
        Assert(!isnull);
        if (!VARATT_IS_EXTENDED(chunk)) {
            chunksize = VARSIZE(chunk) - VARHDRSZ;
            chunkdata = VARDATA(chunk);
        } else if (VARATT_IS_SHORT(chunk)) {
            /* could happen due to heap_form_tuple doing its thing */
            chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
            chunkdata = VARDATA_SHORT(chunk);
        } else {
            /* should never happen */
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE), errmsg("found toasted toast chunk for toast value %u in %s",
                    toastPointer.va_valueid, RelationGetRelationName(toastrel))));
            chunksize = 0; /* keep compiler quiet */
            chunkdata = NULL;
        }

        /*
         * Some checks on the data we've found
         */
        if ((residx != nextidx) || (residx > endchunk) || (residx < startchunk)) {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                errmsg("unexpected chunk number %d (expected %d) for toast value %u in %s", residx, nextidx,
                    toastPointer.va_valueid, RelationGetRelationName(toastrel))));
        }
        if (residx < totalchunks - 1) {
            if (chunksize != UTOAST_MAX_CHUNK_SIZE) {
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                    errmsg("unexpected chunk size %d (expected %d) in chunk %d of %d for toast value %u in %s when "
                        "fetching slice",
                        chunksize, (int)UTOAST_MAX_CHUNK_SIZE, residx, totalchunks, toastPointer.va_valueid,
                        RelationGetRelationName(toastrel))));
            }
        } else if (residx == totalchunks - 1) {
            if ((residx * UTOAST_MAX_CHUNK_SIZE + chunksize) != (uint32)attrsize) {
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                    errmsg("unexpected chunk size %d (expected %d) in final chunk %d for toast value %u in %s when "
                        "fetching slice",
                        chunksize, (int)(attrsize - residx * UTOAST_MAX_CHUNK_SIZE), residx, toastPointer.va_valueid,
                        RelationGetRelationName(toastrel))));
            }
        } else {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE),
                errmsg("unexpected chunk number %d (out of range %d..%d) for toast value %u in %s", residx, 0,
                    totalchunks - 1, toastPointer.va_valueid, RelationGetRelationName(toastrel))));
        }
        /*
         * Copy the data into proper place in our result
         */
        chcpystrt = 0;
        chcpyend = chunksize - 1;
        if (residx == startchunk)
            chcpystrt = startoffset;
        if (residx == endchunk)
            chcpyend = endoffset;

        rc = memcpy_s(VARDATA(result) + (residx * UTOAST_MAX_CHUNK_SIZE - sliceoffset) + chcpystrt,
            length + VARHDRSZ - (residx * UTOAST_MAX_CHUNK_SIZE - sliceoffset + chcpystrt), chunkdata + chcpystrt,
            (chcpyend - chcpystrt) + 1);
        securec_check(rc, "", "");
        nextidx++;
    }

    /*
     * Final checks that we successfully fetched the datum
     */
    if (nextidx != (endchunk + 1)) {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE), errmsg("missing chunk number %d for toast value %u in %s",
                nextidx, toastPointer.va_valueid, RelationGetRelationName(toastrel))));
    }
    systable_endscan_ordered(toastscan);
    ExecDropSingleTupleTableSlot(slot);
    return result;
}
/* ----------
 * toastrel_valueid_exists -
 *
 * 	Test whether a toast value with the given ID exists in the toast relation.
 * For safety, we consider a value to exist if there are either live or dead
 * toast rows with that ID; see notes for GetNewOid().
 * ----------
 */
static bool UHeapToastRelValueidExists(Relation toastrel, Oid valueid)
{
    bool result = false;
    ScanKeyData toastkey;
    SysScanDesc toastscan;
    TupleTableSlot *slot = NULL;
    Assert(RelationIsUstoreFormat(toastrel));
    slot = MakeSingleTupleTableSlot(RelationGetDescr(toastrel), false, toastrel->rd_tam_ops);

    /*
     * Setup a scan key to find chunks with matching va_valueid
     */
    ScanKeyInit(&toastkey, (AttrNumber)1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(valueid));

    /*
     * Is there any such chunk?
     */
    toastscan = systable_beginscan(toastrel, toastrel->rd_rel->reltoastidxid, true, SnapshotAny, 1, &toastkey);
    result = UHeapSysIndexGetnextSlot(toastscan, ForwardScanDirection, slot);

    systable_endscan(toastscan);
    ExecDropSingleTupleTableSlot(slot);

    return result;
}

/* ----------
 * toastid_valueid_exists -
 *
 * 	As above, but work from toast rel's OID not an open relation
 * ----------
 */
static bool UHeapToastIdValueIdExists(Oid toastrelid, Oid valueid, int2 bucketid)
{
    bool result = false;
    Relation toastrel;

    toastrel = heap_open(toastrelid, AccessShareLock, bucketid);

    result = UHeapToastRelValueidExists(toastrel, valueid);

    heap_close(toastrel, AccessShareLock);

    return result;
}
