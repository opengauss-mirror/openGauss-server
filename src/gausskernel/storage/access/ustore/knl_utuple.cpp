/* -------------------------------------------------------------------------
 *
 * knl_utuple.cpp
 * the row format of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_utuple.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/tupmacs.h"
#include "access/htup.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_whitebox_test.h"

#define TUPLE_IS_HEAP_TUPLE(tup) (((HeapTuple)tup)->tupTableType == HEAP_TUPLE)
#define TUPLE_IS_UHEAP_TUPLE(tup) (((UHeapTuple)tup)->tupTableType == UHEAP_TUPLE)

static const int ISNULL_ATTRS_PER_BYTE = 8;

static inline bool ATT_IS_DROPPED(Form_pg_attribute attr)
{
    return (attr->attisdropped || strstr(attr->attname.data, "........pg.dropped."));
}

/*
 * The primary goal of this function is to deform the tuple before
 * it makes its way into the data page. This acts as a validation
 * of the tuple data, especially in case of varlenas.
 */
void CheckTupleValidity(Relation rel, UHeapTuple utuple)
{
    bool isNull[MaxHeapAttributeNumber];
    Datum values[MaxHeapAttributeNumber];

    UHeapDeformTuple(utuple, RelationGetDescr(rel), values, isNull);
}

/* The reason why we pass enableReserve to UHeapCalcTupleDataSize but not read it from GUC(PGC_USERSET level) is:
 * the process of form a tuple is a two-phase operation, firstly calculating the size and then filling the tuple.
 * The value of enableReserve should not be change between the two steps, otherwise it is a bug.
 * So only read enableReserve from GUC at the begining of forming a tuple, and then pass it to UHeapCalcTupleDataSize
 * and UheapFillDiskTuple. In this way we can make sure that they use the same value.
 * The same for toast operations.
 */
uint32 UHeapCalcTupleDataSize(TupleDesc tuple_desc, Datum *values, const bool *is_nulls, uint32 hoff,
    bool enableReverseBitmap, bool enableReserve)
{
    Form_pg_attribute *attrs = tuple_desc->attrs;
    int attrNum = tuple_desc->natts;
    int size = hoff;

    Assert(NAttrsReserveSpace(attrNum) == enableReverseBitmap);
    Assert(!enableReserve || (enableReserve && enableReverseBitmap));
    Assert(enableReverseBitmap || (enableReverseBitmap == false && enableReserve == false));

    for (int i = 0; i < attrNum; ++i) {
        Form_pg_attribute attr = attrs[i];
        Datum val = values[i];

        if (is_nulls[i] && (!enableReserve || !attr->attbyval || ATT_IS_DROPPED(attr))) {
            continue;
        }

        if (attr->attbyval) {
            /* attbyval attributes are stored unaligned in uheap. */
            size += attr->attlen;
        } else if (attr->attlen == -1 && attr->attstorage != 'p' && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val))) {
            /*
             * we're anticipating converting to a short varlena header, so
             * adjust length and don't count any alignment
             */
            int attrLen = VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
            size += attrLen;
        } else {
            /*
             * We'll reach this case when storing a varlena that needs a
             * 4-byte header, a variable-width type that requires alignment
             * such as a record type, and for fixed-width types that are not
             * pass-by-value (e.g. aclitem).
             */
            size = att_align_datum(size, attr->attalign, attr->attlen, val);
            int attrLen = att_addlength_datum(size, attr->attlen, val) - size;
            size += attrLen;
        }
    }

    return size - hoff;
}

/*  no align for pack tuple data
 * The reason why we pass enableReserve to UHeapCalcTupleDataSize but not read it from GUC(PGC_USERSET level) is:
 * the process of form a tuple is a two-phase operation, firstly calculating the size and then filling the tuple.
 * The value of enableReserve should not be change between the two steps, otherwise it is a bug.
 * So only read enableReserve from GUC at the begining of forming a tuple, and then pass it to UHeapCalcTupleDataSize
 * and UheapFillDiskTuple. In this way we can make sure that they use the same value.
 * The same for toast operations.
 */
template<bool hasnull>
void UHeapFillDiskTuple(TupleDesc tupleDesc, Datum *values, const bool *isnull, UHeapDiskTupleData *diskTuple,
    uint32 dataSize, bool enableReverseBitmap, bool enableReserve)
{
    bits8 *bitP = NULL;
    uint32 bitmask;
    bits8 *bitPReserve = NULL;
    uint32 bitmaskReserve = 0;
    char *begin = NULL;
    int numberOfAttributes = tupleDesc->natts;

    Assert(NAttrsReserveSpace(numberOfAttributes) == enableReverseBitmap);
    Assert(!enableReserve || enableReverseBitmap);
    Assert(enableReverseBitmap || (enableReverseBitmap == false && enableReserve == false));

    Form_pg_attribute *att = tupleDesc->attrs;
    char *data = (char *)diskTuple->data;

    if (hasnull) {
        int nullcount = 0;
        int bitmapBytes = 0;
        int tupleAttrs = UHeapTupleHeaderGetNatts(diskTuple);
        Assert(tupleAttrs == numberOfAttributes);
        for (int i = 0; i < numberOfAttributes; i++) {
            if (isnull[i]) {
                nullcount++;
            }
        }
        bitP = &diskTuple->data[0] - 1; /* data[-1] doesnt make compiler happy */
        bitmask = HIGHBIT;
        UHeapDiskTupSetHasNulls(diskTuple);
        if (enableReverseBitmap) {
            bitmapBytes = BITMAPLEN(tupleAttrs + nullcount);
        } else {
            bitmapBytes = BITMAPLEN(tupleAttrs);
        }

        errno_t rc = memset_s(data, bitmapBytes, 0, bitmapBytes);
        securec_check(rc, "", "");

        bitPReserve = (bits8*)(data + (tupleAttrs / ISNULL_ATTRS_PER_BYTE));
        bitmaskReserve = (1 << (uint32)(tupleAttrs % ISNULL_ATTRS_PER_BYTE));
        data = data + bitmapBytes;
    }
    begin = data;

    for (int i = 0; i < numberOfAttributes; i++) {
        errno_t rc = EOK;
        Size attrLength;
        Size remainingLen = dataSize - (size_t)(data - begin);

        if (hasnull) {
            if (bitmask != HIGHBIT) {
                bitmask <<= 1;
            } else {
                bitP += 1;
                bitmask = 1;
            }

            if (isnull[i]) {
                if (!enableReserve || !att[i]->attbyval || ATT_IS_DROPPED(att[i])) {
                    ;
                } else {
                    Assert(att[i]->attlen > 0);
                    data += att[i]->attlen;
                    *bitPReserve |= bitmaskReserve;
                }

                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }

                continue;
            }

            *bitP |= bitmask;
        }

        if (att[i]->attbyval) {
            /* pass-by-value
             * Not aligned
             */
            store_att_byval(data, values[i], att[i]->attlen);
            attrLength = att[i]->attlen;
        } else if (att[i]->attlen == LEN_VARLENA) {
            /* varlena */
            Pointer val = DatumGetPointer(values[i]);

            diskTuple->flag |= HEAP_HASVARWIDTH;

            if (VARATT_IS_EXTERNAL(val)) {
                diskTuple->flag |= HEAP_HASEXTERNAL;
                attrLength = VARSIZE_EXTERNAL(val);
                rc = memcpy_s(data, remainingLen, val, attrLength);
                securec_check(rc, "\0", "\0");
            } else if (VARATT_IS_SHORT(val)) {
                attrLength = VARSIZE_SHORT(val);
                Assert(attrLength <= MaxPossibleUHeapTupleSize);
                rc = memcpy_s(data, remainingLen, val, attrLength);
                securec_check(rc, "\0", "\0");
            } else if (att[i]->attstorage != 'p' && VARATT_CAN_MAKE_SHORT(val)) {
                attrLength = VARATT_CONVERTED_SHORT_SIZE(val);
                SET_VARSIZE_SHORT(data, attrLength);
                Assert(attrLength <= MaxPossibleUHeapTupleSize);
                rc = memcpy_s(data + 1, remainingLen, VARDATA(val), attrLength - 1);
                securec_check(rc, "\0", "\0");
            } else {
                data = (char *)att_align_nominal(data, att[i]->attalign);
                attrLength = VARSIZE(val);
                rc = memcpy_s(data, remainingLen, val, attrLength);
                securec_check(rc, "\0", "\0");
            }
        } else if (att[i]->attlen == LEN_CSTRING) {
            diskTuple->flag |= HEAP_HASVARWIDTH;
            Assert(att[i]->attalign == 'c');
            attrLength = strlen(DatumGetCString(values[i])) + 1;
            Assert(attrLength <= MaxPossibleUHeapTupleSize);
            rc = memcpy_s(data, remainingLen, DatumGetPointer(values[i]), attrLength);
            securec_check(rc, "\0", "\0");
        } else {
            data = (char *)att_align_nominal(data, att[i]->attalign);
            Assert(att[i]->attlen > 0);
            attrLength = att[i]->attlen;
            rc = memcpy_s(data, remainingLen, DatumGetPointer(values[i]), attrLength);
            securec_check(rc, "\0", "\0");
        }

        data += attrLength;
    }

    Assert((size_t)(data - begin) == dataSize);
}

template void UHeapFillDiskTuple<true>(TupleDesc tupleDesc, Datum *values, const bool *isnull,
    UHeapDiskTupleData *diskTuple, uint32 dataSize, bool enableReverseBitmap, bool enableReserve);
template void UHeapFillDiskTuple<false>(TupleDesc tupleDesc, Datum *values, const bool *isnull,
    UHeapDiskTupleData *diskTuple, uint32 dataSize, bool enableReverseBitmap, bool enableReserve);
	
/*
 * Copy a varlena column from source disk tuple (val) to destination (data)
 * Param names match UHeapCopyDiskTupleNoNull and UHeapCopyDiskTupleWithNulls
 */
static Pointer UHeapCopyDtupleVarlena(Pointer val, Pointer data, Size& attrLength, const Size remainingLen, 
                            const Form_pg_attribute att)
{
    errno_t rc = EOK;
    if (VARATT_IS_EXTERNAL(val)) {
        attrLength = VARSIZE_EXTERNAL(val);
        rc = memcpy_s(data, remainingLen, val, attrLength);
    } else if (VARATT_IS_SHORT(val)) {
        attrLength = VARSIZE_SHORT(val);
        Assert(attrLength <= MaxPossibleUHeapTupleSize);
        rc = memcpy_s(data, remainingLen, val, attrLength);
    } else if (att->attstorage != 'p' && VARATT_CAN_MAKE_SHORT(val)) {
        attrLength = VARATT_CONVERTED_SHORT_SIZE(val);
        SET_VARSIZE_SHORT(data, attrLength);
        Assert(attrLength <= MaxPossibleUHeapTupleSize);
        rc = memcpy_s(data + 1, remainingLen, VARDATA(val), attrLength - 1);
    } else {
        data = (char *)att_align_nominal(data, att->attalign);
        attrLength = VARSIZE(val);
        rc = memcpy_s(data, remainingLen, val, attrLength);
    }
    securec_check(rc, "\0", "\0");

    return data;
}

/*
 * Partially copy a src disk tuple (lacking nulls) to a dest disk tuple
 * Only columns marked false in destnull are copied
 * Dest disk tuple has a null bitmap which marks which cols were copied
 */
void UHeapCopyDiskTupleNoNull(TupleDesc tupleDesc, const bool *destNull, UHeapTuple destTup,
                           AttrNumber lastVar, const UHeapDiskTupleData *srcDtup)
{
    UHeapDiskTupleData *destDtup = destTup->disk_tuple;
    long srcOff = srcDtup->t_hoff;
    char *srcTupPtr = (char *)srcDtup;
    char *data = (char *)destDtup + destDtup->t_hoff;
    char *begin = data;
    bool enableReverseBitmap = NAttrsReserveSpace(UHeapTupleHeaderGetNatts(destDtup));
    bool enableReserve = u_sess->attr.attr_storage.reserve_space_for_nullable_atts;

    enableReserve = enableReserve && enableReverseBitmap;
    Assert(NAttrsReserveSpace(UHeapTupleHeaderGetNatts(destDtup)) == enableReverseBitmap);
    Assert(!enableReserve || (enableReserve && enableReverseBitmap));
    Assert(enableReverseBitmap || (enableReverseBitmap == false && enableReserve == false));

    uint32 dataSize = destTup->disk_tuple_size;
    bits8 *bitP = &destDtup->data[0] - 1;
    uint32 bitmask = HIGHBIT;
    bits8 *bitPReserve = NULL;
    uint32 bitmaskReserve = 0;
    Form_pg_attribute *att = tupleDesc->attrs;

    int tupleAttrs = UHeapTupleHeaderGetNatts(srcDtup);
    bitPReserve = (bits8*)((char *)destDtup->data  + (tupleAttrs / ISNULL_ATTRS_PER_BYTE));
    bitmaskReserve = (1 << (tupleAttrs % ISNULL_ATTRS_PER_BYTE));

    for (int i = 0; i < lastVar; i++) {
        errno_t rc = EOK;
        Size attrLength;
        Size remainingLen = dataSize - (size_t)(data - begin);

        if (bitmask != HIGHBIT) {
            bitmask <<= 1;
        } else {
            bitP += 1;
            bitmask = 1;
        }

        if (!destNull[i]) {
            *bitP |= bitmask;
        }

        if (att[i]->attbyval) {
            /* attribute passed by value */
            if (destNull[i]) {
                srcOff += att[i]->attlen;
                if (enableReserve) {
                    Assert(att[i]->attlen > 0);
                    data += att[i]->attlen;
                    *bitPReserve |= bitmaskReserve;
                }

                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }

                continue;
            }
            attrLength = att[i]->attlen;
            store_att_byval(data, *((Datum *)((char *)srcTupPtr + srcOff)), att[i]->attlen);
        } else if (att[i]->attlen == LEN_VARLENA) {
            /* varlena */
            srcOff = att_align_pointer(srcOff, att[i]->attalign, -1, srcTupPtr + srcOff);
            attrLength = VARSIZE_ANY(srcTupPtr + srcOff);

            if (!destNull[i]) {
                Pointer val = (Pointer) (srcTupPtr + srcOff);
                data = UHeapCopyDtupleVarlena(val, data, attrLength, remainingLen, att[i]);
            } else {
                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }
            }
        } else if (att[i]->attlen == LEN_CSTRING) {
            /* null terminated cstring */
            srcOff = att_align_nominal(srcOff, att[i]->attalign);
            Assert(att[i]->attalign == 'c');
            attrLength = (uint32)strlen(srcTupPtr + srcOff) + 1;
            Assert(attrLength <= MaxPossibleUHeapTupleSize);
            if (!destNull[i]) {
                data = (char *)att_align_nominal(data, att[i]->attalign);
                rc = memcpy_s(data, remainingLen, (srcTupPtr + srcOff), attrLength);
                securec_check(rc, "\0", "\0");
            } else {
                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }
            }
        } else {
            /* fixed length */
            srcOff = att_align_nominal(srcOff, att[i]->attalign);
            Assert(att[i]->attlen > 0);
            attrLength = att[i]->attlen;

            if (!destNull[i]) {
                data = (char *)att_align_nominal(data, att[i]->attalign);
                rc = memcpy_s(data, remainingLen, (srcTupPtr + srcOff), attrLength);
                securec_check(rc, "\0", "\0");
            } else {
                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }
            }
        }
        /* Compute the offset of the next attribute */
        srcOff += attrLength;

        if (!destNull[i])
            data += attrLength;
    }
}

/*
 * Partially copy a src disk tuple with nulls to a dest disk tuple
 * Only columns marked false in destNull as false are copied unless they are null in src
 */
void UHeapCopyDiskTupleWithNulls(TupleDesc tupleDesc, const bool *destNull, UHeapTuple destTup,
                           AttrNumber lastVar, UHeapDiskTupleData *srcDtup)
{
    UHeapDiskTupleData *destDtup = destTup->disk_tuple;
    long srcOff = srcDtup->t_hoff;
    char *srcTupPtr = (char *)srcDtup;
    char *data = (char *)destDtup + destDtup->t_hoff;
    char *begin = data;
    bool enableReverseBitmap = NAttrsReserveSpace(UHeapTupleHeaderGetNatts(destDtup));
    bool enableReserve = u_sess->attr.attr_storage.reserve_space_for_nullable_atts;

    enableReserve = enableReserve && enableReverseBitmap;
    Assert(NAttrsReserveSpace(UHeapTupleHeaderGetNatts(destDtup)) == enableReverseBitmap);
    Assert(!enableReserve || (enableReserve && enableReverseBitmap));
    Assert(enableReverseBitmap || (enableReverseBitmap == false && enableReserve == false));

    uint32 dataSize = destTup->disk_tuple_size;
    bits8 *srcBp = srcDtup->data;
    bits8 *bitP = &destDtup->data[0] - 1;
    uint32 bitmask = HIGHBIT;
    bits8 *bitPReserve = NULL;
    uint32 bitmaskReserve = 0;
    Form_pg_attribute *att = tupleDesc->attrs;

    int srcnullcount = 0;

    int tupleAttrs = UHeapTupleHeaderGetNatts(srcDtup);
    bitPReserve = (bits8*)((char *)destDtup->data  + (tupleAttrs / ISNULL_ATTRS_PER_BYTE));
    bitmaskReserve = (1 << (tupleAttrs % ISNULL_ATTRS_PER_BYTE));

    for (int i = 0; i < lastVar; i++) {
        errno_t rc = EOK;
        Size attrLength;
        Size remainingLen = dataSize - (size_t)(data - begin);

        if (bitmask != HIGHBIT) {
            bitmask <<= 1;
        } else {
            bitP += 1;
            bitmask = 1;
        }

        if (att_isnull(i, srcBp)) {
            if (enableReverseBitmap) {
                if (!att_isnull(tupleAttrs + srcnullcount, srcBp)) {
                    Assert(att[i]->attlen > 0);
                    data += att[i]->attlen;
                    srcOff += att[i]->attlen;
                    *bitPReserve |= bitmaskReserve;
                }
            }

            if (bitmaskReserve != HIGHBIT) {
                bitmaskReserve <<= 1;
            } else {
                bitPReserve += 1;
                bitmaskReserve = 1;
            }

            srcnullcount++;
            continue;
        }

        if (!destNull[i])
            *bitP |= bitmask;

        if (att[i]->attbyval) {
            /* attribute is passed by value */
            if (destNull[i]) {
                srcOff += att[i]->attlen;
                if (enableReserve) {
                    Assert(att[i]->attlen > 0);
                    data += att[i]->attlen;
                    *bitPReserve |= bitmaskReserve;
                }

                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }

                continue;
            }
            attrLength = att[i]->attlen;
            store_att_byval(data, *((Datum *)((char *)srcTupPtr + srcOff)), att[i]->attlen);
        } else if (att[i]->attlen == LEN_VARLENA) {
            /* varlena */
            srcOff = att_align_pointer(srcOff, att[i]->attalign, -1, srcTupPtr + srcOff);
            attrLength = VARSIZE_ANY(srcTupPtr + srcOff);

            if (!destNull[i]) {
                Pointer val = (Pointer) (srcTupPtr + srcOff);
                data = UHeapCopyDtupleVarlena(val, data, attrLength, remainingLen, att[i]);
            } else {
                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }
            }
        } else if (att[i]->attlen == LEN_CSTRING) {
            /* null terminated cstring */
            srcOff = att_align_nominal(srcOff, att[i]->attalign);
            Assert(att[i]->attalign == 'c');
            attrLength = (uint32)strlen(srcTupPtr + srcOff) + 1;
            Assert(attrLength <= MaxPossibleUHeapTupleSize);
            if (!destNull[i]) {
                data = (char *)att_align_nominal(data, att[i]->attalign);
                rc = memcpy_s(data, remainingLen, (srcTupPtr + srcOff), attrLength);
                securec_check(rc, "\0", "\0");
            } else {
                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }
            }
        } else {
            /* fixed length */
            srcOff = att_align_nominal(srcOff, att[i]->attalign);
            Assert(att[i]->attlen > 0);
            attrLength = att[i]->attlen;
            if (!destNull[i]) {
                data = (char *)att_align_nominal(data, att[i]->attalign);
                rc = memcpy_s(data, remainingLen, (srcTupPtr + srcOff), attrLength);
                securec_check(rc, "\0", "\0");
            } else {
                if (bitmaskReserve != HIGHBIT) {
                    bitmaskReserve <<= 1;
                } else {
                    bitPReserve += 1;
                    bitmaskReserve = 1;
                }
            }
        }
        /* Compute the offset of the next attribute */
        srcOff += attrLength;

        if (!destNull[i])
            data += attrLength;
    }
}

UHeapTuple UHeapFormTuple(TupleDesc tuple_desc, Datum *values, bool *is_nulls)
{
    uint8 hoff = 0;
    uint32 diskTupleSize = 0;
    uint32 i = 0;
    uint32 attrNum = tuple_desc->natts;
    bool enableReverseBitmap = NAttrsReserveSpace(attrNum);
    bool enableReserve = u_sess->attr.attr_storage.reserve_space_for_nullable_atts;
    UHeapDiskTupleData *diskTuple = NULL;
    int nullcount = 0;

    enableReserve = enableReserve && enableReverseBitmap;

    if (attrNum > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
            errmsg("number of columns (%d) exceeds limit (%d)", attrNum, MaxTupleAttributeNumber)));
    }

    WHITEBOX_TEST_STUB(UHEAP_FORM_TUPLE_FAILED, WhiteboxDefaultErrorEmit);

    /* Step 1: caculate uheap_tuple size for allocate memory */
    for (i = 0; i < attrNum; i++) {
        if (is_nulls[i]) {
            nullcount++;
        }
    }
    diskTupleSize = SizeOfUHeapDiskTupleData;

    uint32 dataSize = 0;
    if (nullcount > 0) {
        if (enableReverseBitmap) {
            diskTupleSize += BITMAPLEN(attrNum + nullcount);
        } else {
            diskTupleSize += BITMAPLEN(attrNum);
        }
        dataSize = UHeapCalcTupleDataSize(tuple_desc, values, is_nulls, diskTupleSize, enableReverseBitmap,
            enableReserve);
    } else {
        dataSize = UHeapCalcTupleDataSize(tuple_desc, values, is_nulls, diskTupleSize, enableReverseBitmap,
            enableReserve);
    }

    hoff = diskTupleSize;
    diskTupleSize += dataSize;

    UHeapTuple uheapTuplePtr = (UHeapTuple)uheaptup_alloc(diskTupleSize + sizeof(UHeapTupleData));
    uheapTuplePtr->disk_tuple = (UHeapDiskTupleData *)((char *)uheapTuplePtr + sizeof(UHeapTupleData));
    diskTuple = uheapTuplePtr->disk_tuple;

    /* Step 2: fill disk tuple header and data */
    UHeapTupleHeaderSetNatts(diskTuple, attrNum);
    diskTuple->t_hoff = hoff;

    if (nullcount > 0) {
        UHeapFillDiskTuple<true>(tuple_desc, values, is_nulls, diskTuple, dataSize, enableReverseBitmap,
            enableReserve);
    } else {
        UHeapFillDiskTuple<false>(tuple_desc, values, is_nulls, diskTuple, dataSize, enableReverseBitmap,
            enableReserve);
    }

    uheapTuplePtr->disk_tuple_size = diskTupleSize;

    return uheapTuplePtr;
}

UHeapTuple UHeapModifyTuple(UHeapTuple tuple, TupleDesc tupleDesc, Datum *replValues, const bool *replIsnull,
                            const bool *doReplace)
{
    int numberOfAttributes = tupleDesc->natts;
    int attoff;
    Datum *values = NULL;
    bool *isnull = NULL;
    UHeapTuple newTuple;

    /*
     * allocate and fill values and isnull arrays from either the tuple or the
     * repl information, as appropriate.
     *
     * NOTE: it's debatable whether to use heap_deform_tuple() here or just
     * heap_getattr() only the non-replaced columns.  The latter could win if
     * there are many replaced columns and few non-replaced ones. However,
     * heap_deform_tuple costs only O(N) while the heap_getattr way would cost
     * O(N^2) if there are many non-replaced columns, so it seems better to
     * err on the side of linear cost.
     */
    values = (Datum *)palloc(numberOfAttributes * sizeof(Datum));
    isnull = (bool *)palloc(numberOfAttributes * sizeof(bool));

    tableam_tops_deform_tuple(tuple, tupleDesc, values, isnull);

    for (attoff = 0; attoff < numberOfAttributes; attoff++) {
        if (doReplace[attoff]) {
            values[attoff] = replValues[attoff];
            isnull[attoff] = replIsnull[attoff];
        }
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = (UHeapTuple)tableam_tops_form_tuple(tupleDesc, values, isnull, UHEAP_TUPLE);

    pfree(values);
    pfree(isnull);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    newTuple->ctid = tuple->ctid;
    newTuple->table_oid = tuple->table_oid;
    HeapTupleCopyBase(newTuple, tuple);
#ifdef PGXC
    newTuple->xc_node_id = tuple->xc_node_id;
#endif


    Assert (tupleDesc->tdhasoid == false);

    return newTuple;
}

void SlotDeformUTuple(TupleTableSlot *slot, UHeapTuple tuple, long *offp, int natts)
{
    Assert(tuple->tupTableType == UHEAP_TUPLE);

    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    Datum *values = slot->tts_values;
    bool *isNulls = slot->tts_isnull;
    UHeapDiskTuple tup = tuple->disk_tuple;
    Form_pg_attribute *att = tupleDesc->attrs;
    bool hasnulls = UHeapDiskTupHasNulls(tup);
    char *tp = (char *) tup;
    bits8 *bp = tup->data;                 /* ptr to null bitmap in tuple */
    int nullcount = 0;
    int tupleAttrs = UHeapTupleHeaderGetNatts(tuple->disk_tuple);
    bool enableReverseBitmap = NAttrsReserveSpace(tupleAttrs);

    /* We can only fetch as many attributes as the tuple has. */
    int nattsAvailable = Min(tupleAttrs, natts);
    Assert(*offp >= 0);

    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    long off = tup->t_hoff;
    int attnum = slot->tts_nvalid;
    /* if we're not starting from the first attribute */
    if (attnum != 0) {
        off = *offp; /* Restore state from previous execution */
    }

    for (; attnum < nattsAvailable; attnum++) {
        Form_pg_attribute thisatt = att[attnum];

        if (hasnulls && att_isnull(attnum, bp)) {
            /* Skip attribute length in case the tuple was stored with
               space reserved for null attributes */
            if (enableReverseBitmap) {
                if (!att_isnull(tupleAttrs + nullcount, bp)) {
                    off += thisatt->attlen;
                }
            }
            nullcount++;

            values[attnum] = (Datum)0;
            isNulls[attnum] = true;
            continue;

        }

        isNulls[attnum] = false;

        if (thisatt->attlen == LEN_VARLENA) {
            off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
        } else if (!thisatt->attbyval) {
            /* not varlena, so safe to use att_align_nominal */
            off = att_align_nominal(off, thisatt->attalign);
        }

        values[attnum] = fetchatt(thisatt, tp + off);
        off = att_addlength_pointer(off, thisatt->attlen, tp + off);        
    }

    /*
     * If the tuple doesn't have values for any column, populate the
     * data from default values of that column.
     * This is the original behavior for row table.
     */
    for (; attnum < natts; attnum++) {
        slot->tts_values[attnum] = heapGetInitDefVal(attnum + 1, slot->tts_tupleDescriptor, &slot->tts_isnull[attnum]);
    }

    /*
     * Save state for next execution
     */
    slot->tts_nvalid = attnum;
    *offp = off;
}

void UHeapDeformTuple(UHeapTuple utuple, TupleDesc rowDesc, Datum *values, bool *isNulls)
{
    UHeapDeformTupleGuts(utuple, rowDesc, values, isNulls, rowDesc->natts);
}

void UHeapDeformTupleGuts(UHeapTuple utuple, TupleDesc rowDesc, Datum *values, bool *isNulls, int unatts)
{
    Assert(utuple->tupTableType == UHEAP_TUPLE);

    UHeapDiskTuple diskTuple = utuple->disk_tuple;
    bool hasnulls = UHeapDiskTupHasNulls(diskTuple);
    Form_pg_attribute *att = rowDesc->attrs;
    int natts; /* number of atts to extract */
    int attnum;
    bits8 *bp = diskTuple->data;
    long off = diskTuple->t_hoff;
    char *tupPtr = (char *)diskTuple;
    int nullcount = 0;
    int tupleAttrs = UHeapTupleHeaderGetNatts(diskTuple);
    bool enableReverseBitmap = NAttrsReserveSpace(tupleAttrs);

    WHITEBOX_TEST_STUB(UHEAP_DEFORM_TUPLE_FAILED, WhiteboxDefaultErrorEmit);

    natts = Min(tupleAttrs, unatts);
    for (attnum = 0; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = att[attnum];

        if (hasnulls && att_isnull(attnum, bp)) {
            /* Skip attribute length in case the tuple was stored with
               space reserved for null attributes */
            if (enableReverseBitmap) {
                if (!att_isnull(tupleAttrs + nullcount, bp)) {
                    off += thisatt->attlen;
                }
            }

            nullcount++;

            values[attnum] = (Datum)0;
            isNulls[attnum] = true;
            continue;
        }

        isNulls[attnum] = false;

        /*
         * If this is a varlena, there might be alignment padding, if it has a
         * 4-byte header.  Otherwise, there will only be padding if it's not
         * pass-by-value.
         */
        if (thisatt->attlen == -1) {
            off = att_align_pointer(off, thisatt->attalign, -1, tupPtr + off);
        } else if (!thisatt->attbyval) {
            off = att_align_nominal(off, thisatt->attalign);
        }

        values[attnum] = fetchatt(thisatt, tupPtr + off);

        off = att_addlength_pointer(off, thisatt->attlen, tupPtr + off);
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
    for (; attnum < unatts; attnum++) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: values[attnum] = (Datum) 0;
         * example code: isNulls[attnum] = true;
         */
        values[attnum] = heapGetInitDefVal(attnum + 1, rowDesc, &isNulls[attnum]);
    }

}


/*
 * This function is used to cast heap type tuple to uheap type tuple
 */
UHeapTuple HeapToUHeap(TupleDesc tuple_desc, HeapTuple heaptuple)
{
    Assert(TUPLE_IS_HEAP_TUPLE(heaptuple));

    Datum *values = (Datum *)palloc(sizeof(Datum) * tuple_desc->natts);
    bool *isnull = (bool *)palloc(sizeof(bool) * tuple_desc->natts);
    heap_deform_tuple(heaptuple, tuple_desc, values, isnull);
    UHeapTuple uheaptuple = UHeapFormTuple(tuple_desc, values, isnull);
    uheaptuple->ctid = heaptuple->t_self;
    pfree_ext(values);
    pfree_ext(isnull);
    return uheaptuple;
}

/*
 * This function is used to cast uheap type tuple to heap type tuple
 */
HeapTuple UHeapToHeap(TupleDesc tuple_desc, UHeapTuple uheaptuple)
{
    Assert(uheaptuple->tupTableType == UHEAP_TUPLE);
    Datum *values = (Datum *)palloc(sizeof(Datum) * tuple_desc->natts);
    bool *isnull = (bool *)palloc(sizeof(bool) * tuple_desc->natts);
    UHeapDeformTuple(uheaptuple, tuple_desc, values, isnull);
    HeapTuple heaptuple = heap_form_tuple(tuple_desc, values, isnull);
    heaptuple->t_self = uheaptuple->ctid;
    pfree_ext(values);
    pfree_ext(isnull);
    return heaptuple;
}

/*
 * This is similar to heap version's SetHintBits but handles Inplace tuples instead.
 */
void UHeapTupleSetHintBits(UHeapDiskTuple tuple, Buffer buffer, uint16 infomask, TransactionId xid)
{
    // The following scenario may use local snapshot, so do not set hint bits.
    // Notice: we don't support two or more bits within infomask.
    //
    Assert(infomask > 0);
    Assert((infomask & (infomask - 1)) == 0);
    if ((t_thrd.xact_cxt.useLocalSnapshot && !(infomask & UHEAP_XID_COMMITTED)) || RecoveryInProgress() ||
        g_instance.attr.attr_storage.IsRoachStandbyCluster) {
        ereport(DEBUG2, (errmsg("ignore setting tuple hint bits when local snapshot is used.")));
        return;
    }

    if (XACT_READ_UNCOMMITTED == u_sess->utils_cxt.XactIsoLevel && !(infomask & UHEAP_XID_COMMITTED)) {
        ereport(DEBUG2, (errmsg("ignore setting tuple hint bits when XACT_READ_UNCOMMITTED is used.")));
        return;
    }

    if (TransactionIdIsValid(xid)) {
        /* NB: xid must be known committed here! */
        XLogRecPtr commitLSN = TransactionIdGetCommitLSN(xid);
        if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) && XLByteLT(BufferGetLSNAtomic(buffer), commitLSN)) {
            /* not flushed and no LSN interlock, so don't set hint */
            return;
        }
    }

    MarkBufferDirtyHint(buffer, true);
}

/*
 * UHeapGetSysAttr
 * Fetch the value of a system attribute for a tuple.
 */
Datum UHeapGetSysAttr(UHeapTuple uhtup, Buffer buf, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    Datum result = (Datum)0;

    Assert(uhtup);
    Assert(uhtup->tupTableType == UHEAP_TUPLE);

    /* Currently, no sys attribute ever reads as NULL. */
    *isnull = false;

    switch (attnum) {
        case SelfItemPointerAttributeNumber:
            /* pass-by-reference datatype */
            result = PointerGetDatum(&(uhtup->ctid));
            break;
        case MinTransactionIdAttributeNumber:
        case MaxTransactionIdAttributeNumber:
        case MinCommandIdAttributeNumber:
        case MaxCommandIdAttributeNumber:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("xmin, xmax, cmin, and cmax are not supported for ustore tuples")));
            break;
        case TableOidAttributeNumber:
            result = ObjectIdGetDatum(uhtup->table_oid);
            break;
        case XC_NodeIdAttributeNumber:
            result = UInt32GetDatum(uhtup->xc_node_id);
            break;
        default:
            elog(ERROR, "invalid attnum: %d", attnum);
            result = 0; /* keep compiler quiet */
            break;
    }

    return result;
}

/*
 * UHeapTupleAttrEquals
 * Subroutine for UHeapDetermineModifiedColumns which returns the set of
 * attributes from the given att_list that are different in tup1 and tup2.
 */
Bitmapset *UHeapTupleAttrEquals(TupleDesc tupdesc, Bitmapset *att_list, UHeapTuple tup1, UHeapTuple tup2)
{
    int lAttno = 0, col = -1;
    bool oldIsnull[MaxHeapAttributeNumber];
    bool newIsnull[MaxHeapAttributeNumber];
    Datum oldValues[MaxHeapAttributeNumber];
    Datum newValues[MaxHeapAttributeNumber];
    Bitmapset *modified = NULL;

    /* Find the largest attno in the given Bitmapset. */
    while ((col = bms_next_member(att_list, col)) >= 0) {
        /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
        AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

        /*
         * Donot allow update on any system attribute other than tableOID,
         * which is stored in index tuples of Global Partition Indexes
         * and might not have been set correctly yet in the new tuple.
         */
        if (attno <= InvalidAttrNumber && attno != TableOidAttributeNumber) {
            elog(ERROR, "system-column update is not supported");
        }

        if (lAttno < attno)
            lAttno = attno;
    }

    Assert(lAttno <= tupdesc->natts);

    /* Deform both the old and new tuple. */
    UHeapDeformTupleGuts(tup1, tupdesc, oldValues, oldIsnull, lAttno);
    UHeapDeformTupleGuts(tup2, tupdesc, newValues, newIsnull, lAttno);

    /* Loop through atts and add every non-equal attno to modified. */
    col = -1;
    while ((col = bms_next_member(att_list, col)) >= 0) {
        /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
        AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

        /* possible for Global Partition Indexes */
        if (attno == TableOidAttributeNumber)
            continue;

        /* If one value is NULL and other is not, they are not equal. */
        if (oldIsnull[attno - 1] != newIsnull[attno - 1])
            modified = bms_add_member(modified, attno - FirstLowInvalidHeapAttributeNumber);

        /* If both are NULL, they can be considered equal. */
        else if (oldIsnull[attno - 1])
            continue;

        else {
            Form_pg_attribute att = (TupleDescAttr(tupdesc, attno - 1));
            /*
             * We do simple binary comparison of the two datums.  This may be
             * overly strict because there can be multiple binary
             * representations for the same logical value.  But we should be
             * OK as long as there are no false positives.  Using a
             * type-specific equality operator is messy because there could be
             * multiple notions of equality in different operator classes;
             * furthermore, we cannot safely invoke user-defined functions
             * while holding exclusive buffer lock.
             */
            if (!datumIsEqual(oldValues[attno - 1], newValues[attno - 1], att->attbyval, att->attlen))
                modified = bms_add_member(modified, attno - FirstLowInvalidHeapAttributeNumber);
        }
    }

    return modified;
}


/*
 * UHeapCopyTuple
 * Returns a copy of an entire tuple.
 *
 * The UHeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 */
UHeapTuple UHeapCopyTuple(UHeapTuple uhtup)
{
    UHeapTuple newTuple;
    errno_t rc = EOK;

    if (!UHeapTupleIsValid(uhtup) || uhtup->disk_tuple == NULL) {
        return NULL;
    }

    Assert(uhtup->tupTableType == UHEAP_TUPLE);

    newTuple = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize + uhtup->disk_tuple_size);
    newTuple->ctid = uhtup->ctid;
    newTuple->table_oid = uhtup->table_oid;
    newTuple->xc_node_id = uhtup->xc_node_id;
    newTuple->disk_tuple_size = uhtup->disk_tuple_size;
    newTuple->t_xid_base = uhtup->t_xid_base;
    newTuple->t_multi_base = uhtup->t_multi_base;
    newTuple->disk_tuple = (UHeapDiskTuple)((char *)newTuple + UHeapTupleDataSize);
    rc = memcpy_s((char *)newTuple->disk_tuple, newTuple->disk_tuple_size, (char *)uhtup->disk_tuple,
        uhtup->disk_tuple_size);
    securec_check(rc, "\0", "\0");
    return newTuple;
}

void UHeapCopyTupleWithBuffer(UHeapTuple srcTup, UHeapTuple destTup)
{
    Assert(UHeapTupleIsValid(srcTup));
    Assert(srcTup->disk_tuple != NULL);
    Assert(UHeapTupleIsValid(destTup));
    Assert(destTup->disk_tuple != NULL);

    destTup->ctid = srcTup->ctid;
    destTup->table_oid = srcTup->table_oid;
    destTup->xc_node_id = srcTup->xc_node_id;
    destTup->t_xid_base = srcTup->t_xid_base;
    destTup->t_multi_base = srcTup->t_multi_base;
    destTup->disk_tuple_size = srcTup->disk_tuple_size;
    errno_t rc = memcpy_s((char *)destTup->disk_tuple, destTup->disk_tuple_size, (char *)srcTup->disk_tuple,
        srcTup->disk_tuple_size);
    securec_check(rc, "\0", "\0");
}

/*
 * UHeapTupleHeaderAdvanceLatestRemovedXid - Advance the latestRemovedXid, if
 * tuple is deleted by a transaction greater than latestRemovedXid.  This is
 * required to generate conflicts on hot standby.
 *
 * If we change this function then we need a similar change in
 * *_xlog_vacuum_get_latestRemovedXid functions as well.
 *
 * This is quite similar to HeapTupleHeaderAdvanceLatestRemovedXid.
 */
void UHeapTupleHeaderAdvanceLatestRemovedXid(UHeapDiskTuple tuple, TransactionId xid, TransactionId *latestRemovedXid)
{
    /*
     * Ignore tuples inserted by an aborted transaction.
     *
     * XXX we can ignore the tuple if it was non-in-place updated/deleted by
     * the inserting transaction, but for that we need to traverse the
     * complete undo chain to find the root tuple, is it really worth?
     */
    if (UHeapTransactionIdDidCommit(xid)) {
        Assert((tuple->flag & UHEAP_DELETED) || (tuple->flag & UHEAP_UPDATED));
        if (TransactionIdFollows(xid, *latestRemovedXid))
            *latestRemovedXid = xid;
    }

    /* latestRemovedXid may still be invalid at end */
}

/*
 * UHeapAttIsNull
 * Returns TRUE if uheap tuple attribute is not present.
 */
bool UHeapAttIsNull(UHeapTuple tup, int attnum, TupleDesc tupleDesc)
{
    Assert(tup->tupTableType == UHEAP_TUPLE);

    /*
     * We allow a NULL tupledesc for relations not expected to have missing
     * values, such as catalog relations and indexes.
     */
    Assert(!tupleDesc || attnum <= tupleDesc->natts);
    if (attnum > (int)UHeapTupleHeaderGetNatts(tup->disk_tuple)) {
        return true;
    }

    if (attnum > 0) {
        if (UHeapDiskTupNoNulls(tup->disk_tuple)) {
            return false;
        }

        return att_isnull(attnum - 1, tup->disk_tuple->data);
    }

    switch (attnum) {
        case TableOidAttributeNumber:
        case SelfItemPointerAttributeNumber:
        case MinTransactionIdAttributeNumber:
        case MinCommandIdAttributeNumber:
        case MaxTransactionIdAttributeNumber:
        case MaxCommandIdAttributeNumber:
            /* these are never null */
            break;
        default:
            elog(ERROR, "invalid attnum: %d", attnum);
    }

    return false;
}

/* UHeapNoCacheGetAttr
 * uheap equivalent to nocachegetattr
 */
Datum UHeapNoCacheGetAttr(UHeapTuple tuple, uint32 attnum, TupleDesc tupleDesc)
{
    UHeapDiskTuple tup = tuple->disk_tuple;
    Form_pg_attribute *att = tupleDesc->attrs;
    char *tp = (char *)tup; /* ptr to data part of tuple */
    bits8 *bp = tup->data;  /* ptr to null bitmap in tuple */
    bool slow = false;      /* do we have to walk attrs? */
    int off;                /* current offset within data */
    int hoff = tup->t_hoff; /* header length on tuple data */

    /* ----------------
     * Three cases:
     *
     * 1: No nulls and no variable-width attributes.
     * 2: Has a null or a var-width AFTER att.
     * 3: Has nulls or var-widths BEFORE att.
     * ----------------
     */

    /*
     * important:
     * maybe this function is not safe for accessing some attribute, which is different
     * from methods slot_getattr(), slot_getallattrs(), slot_getsomeattrs(). those three
     * always make sure that attnum is always valid between 1 and InplaceHeapTupleHeaderGetNatts(),
     * because the caller has guarantee that.
     * we find that the caller fastgetattr() doesn't guarantee the validition, and top callers
     * are almost in system table level, for example pg_class and so on. so that it's NOT
     * recommended that users' table functions call fastgetattr() and nocachegetattr();
     */
    Assert(attnum <= UHeapTupleHeaderGetNatts(tup));
    attnum--;

    if (!UHeapDiskTupNoNulls(tup)) {
        /*
         * there's a null somewhere in the tuple
         *
         * check to see if any preceding bits are null...
         */
        int byte = attnum >> ATTNUM_BMP_SHIFT;
        int finalbit = attnum & 0x07;

        /* check for nulls "before" final bit of last byte */
        if ((~bp[byte]) & ((1 << finalbit) - 1))
            slow = true;
        else {
            /* check for nulls in any "earlier" bytes */
            for (int i = 0; i < byte; i++) {
                if (bp[i] != 0xFF) {
                    slow = true;
                    break;
                }
            }
        }
    }

    if (!slow) {
        /*
         * If we get here, there are no nulls up to and including the target
         * attribute. Check for non-fixed-length attrs up to and including
         * target. If there aren't any, it's safe to cheaply initialize the
         * cached offsets for these attrs.
         */
        if (UHeapDiskTupHasVarWidth(tup)) {
            for (uint32 j = 0; j <= attnum; j++) {
                if (att[j]->attlen <= 0) {
                    slow = true;
                    break;
                }
            }
        }
    }

    if (!slow) {
        uint32 natts = tupleDesc->natts;
        uint32 j = 1;

        /*
         * If we get here, we have a tuple with no nulls or var-widths up to
         * and including the target attribute, so we can use the cached offset
         * ... only we don't have it yet, or we'd not have got here.  Since
         * it's cheap to compute offsets for fixed-width columns, we take the
         * opportunity to initialize the cached offsets for *all* the leading
         * fixed-width columns, in hope of avoiding future visits to this
         * routine.
         */
        att[0]->attcacheoff = 0;

        /* we might have set some offsets in the slow path previously */
        while (j < natts && att[j]->attcacheoff > 0)
            j++;

        off = att[j - 1]->attcacheoff + att[j - 1]->attlen;

        for (; j < natts; j++) {
            if (att[j]->attlen <= 0)
                break;

            att[j]->attcacheoff = off;

            off += att[j]->attlen;
        }

        Assert(j > attnum);

        off = att[attnum]->attcacheoff + hoff;
    } else {
        bool usecache = true;

        /*
         * Now we know that we have to walk the tuple CAREFULLY.  But we still
         * might be able to cache some offsets for next time.
         *
         * Note - This loop is a little tricky.  For each non-null attribute,
         * we have to first account for alignment padding before the attr,
         * then advance over the attr based on its length.      Nulls have no
         * storage and no alignment padding either.  We can use/set
         * attcacheoff until we reach either a null or a var-width attribute.
         */
        off = hoff;
        int nullcount = 0;
        int tupleAttrs = UHeapTupleHeaderGetNatts(tup);
        bool enableReverseBitmap = NAttrsReserveSpace(tupleAttrs);
        for (uint32 i = 0;; i++) { /* loop exit is at "break" */
            int attrLen = att[i]->attlen;

            Assert(i < (uint32)tupleDesc->natts);

            if (UHeapDiskTupHasNulls(tup) && att_isnull(i, bp)) {
                if (enableReverseBitmap && !att_isnull(tupleAttrs + nullcount, bp))
                    off += att[i]->attlen;

                nullcount++;

                usecache = false;
                continue; /* this cannot be the target att */
            }

            if (att[i]->attlen == LEN_VARLENA) {
                off = att_align_pointer(off, att[i]->attalign, -1, tp + off);
                attrLen = VARSIZE_ANY(tp + off);
            } else if (!att[i]->attbyval) {
                off = att_align_nominal(off, att[i]->attalign);
            } else if (usecache) {
                att[i]->attcacheoff = off - hoff;
            }

            if (i == attnum)
                break;

            off = att_addlength_pointer(off, att[i]->attlen, tp + off);

            if (usecache && att[i]->attlen <= 0)
                usecache = false;
        }
    }

    return fetchatt(att[attnum], tp + off);
}


/*
 * UHeapCopyHeapTuple
 * Return a heap tuple constructed from the contents of the slot.
 *
 * heap_form_tuple will always a build a new tuple, so we don't need an
 * explicit copy step.
 */
HeapTuple UHeapCopyHeapTuple(TupleTableSlot *slot)
{
    HeapTuple tuple;

    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    UHeapSlotGetAllAttrs(slot);

    tuple = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);

    // Some code like analyze, needs
    // the t_self item pointer in the
    // tuple for sorting purposes,
    // so we set it here
    if (slot->tts_tuple != NULL) {
        tuple->t_self = ((UHeapTuple)slot->tts_tuple)->ctid;
        tuple->t_tableOid = ((UHeapTuple)slot->tts_tuple)->table_oid;
    }

    return tuple;
}

/*
 * Clears the contents of the table slot that contains uheap table tuple data.
 */
void UHeapSlotClear(TupleTableSlot *slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree && (UHeapTuple)slot->tts_tuple != NULL) {
        UHeapFreeTuple(slot->tts_tuple);
        slot->tts_tuple = NULL;
        slot->tts_shouldFree = false;
    }

    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
        slot->tts_shouldFreeMin = false;
    }
}

/*
 * UHeapSlotGetSomeAttrs
 * This function forces the entries of the slot's Datum/isnull
 * arrays to be valid at least up through the attnum'th entry.
 *
 * @param slot:input Tuple Table slot from which attributes are extracted.
 * @param attnum: index until which slots attributes are extracted.
 */
void UHeapSlotGetSomeAttrs(TupleTableSlot *slot, int attnum)
{
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid >= attnum) {
        return;
    }
    UHeapDeformTupleGuts((UHeapTuple)slot->tts_tuple, slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull,
        attnum);
}

void UHeapSlotFormBatch(TupleTableSlot* slot, VectorBatch* batch, int cur_rows, int attnum)
{
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    /* Quick out if we have all already */
    if (slot->tts_nvalid >= attnum) {
        return;
    }

    UHeapTuple utuple = (UHeapTuple)slot->tts_tuple;
    TupleDesc rowDesc = slot->tts_tupleDescriptor;
    bool isNull = slot->tts_isnull;
    UHeapDiskTuple diskTuple = utuple->disk_tuple;
    bool hasNull = UHeapDiskTupHasNulls(diskTuple);
    Form_pg_attribute* att = rowDesc->attrs;
    int attno;
    bits8* bp = diskTuple->data;
    long off = diskTuple->t_hoff;
    char* tupPtr = (char*)diskTuple;
    int nullcount = 0;
    int tupleAttrs = UHeapTupleHeaderGetNatts(diskTuple);
    bool enableReverseBitmap = NAttrsReserveSpace(tupleAttrs);

    WHITEBOX_TEST_STUB(UHEAP_DEFORM_TUPLE_FAILED, WhiteboxDefaultErrorEmit);

    int natts = Min(tupleAttrs, attnum);
    for (attno = 0; attno < natts; attno++) {
        Form_pg_attribute thisatt = att[attno];
        ScalarVector* pVector = &batch->m_arr[attno];

        if (hasNull && att_isnull(attno, bp)) {
            /* Skip attribute length in case the tuple was stored with
               space reserved for null attributes */
            if (enableReverseBitmap && !att_isnull(tupleAttrs + nullcount, bp)) {
                off += thisatt->attlen;
            }

            nullcount++;

            pVector->m_vals[cur_rows] = (Datum)0;
            SET_NULL(pVector->m_flag[cur_rows]);
            /* stole the flag for perf */
            pVector->m_const = true;
            continue;
        }

        SET_NOTNULL(pVector->m_flag[cur_rows]);

        /*
         * If this is a varlena, there might be alignment padding, if it has a
         * 4-byte header.  Otherwise, there will only be padding if it's not
         * pass-by-value.
         */
        if (thisatt->attlen == -1) {
            off = att_align_pointer(off, thisatt->attalign, -1, tupPtr + off);
        } else if (!thisatt->attbyval) {
            off = att_align_nominal(off, thisatt->attalign);
        }

        pVector->m_vals[cur_rows] = fetchatt(thisatt, tupPtr + off);

        off = att_addlength_pointer(off, thisatt->attlen, tupPtr + off);
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
    for (; attno < attnum; attno++) {
        ScalarVector* pVector = &batch->m_arr[attno];
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: values[attnum] = (Datum) 0;
         * example code: isNulls[attnum] = true;
         */
        pVector->m_vals[cur_rows] = heapGetInitDefVal(attno + 1, rowDesc, &isNull);
        if (isNull) {
            SET_NULL(pVector->m_flag[cur_rows]);
            pVector->m_const = true;
        } else {
            SET_NOTNULL(pVector->m_flag[cur_rows]);
        }
    }
}

/*
 * UHeapSlotAttIsNull
 * Detect whether an attribute of the slot is null, without
 * actually fetching it.
 *
 * @param slot: Tabletuple slot
 * @para attnum: attribute index that should be checked for null value.
 */
bool UHeapSlotAttIsNull(const TupleTableSlot *slot, int attnum)
{
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    UHeapTuple uhtup = (UHeapTuple)slot->tts_tuple;

    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    /*
     * system attributes are handled by heap_attisnull
     */

    if (attnum <= 0) {
        /* internal error */
        if (uhtup == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract system attribute from virtual tuple")));
        }
        return UHeapAttIsNull(uhtup, attnum, tupleDesc);
    }

    /*
     * fast path if desired attribute already cached
     */
    if (attnum <= slot->tts_nvalid) {
        return slot->tts_isnull[attnum - 1];
    }

    /*
     * return NULL if attnum is out of range according to the tupdesc
     */
    if (attnum > tupleDesc->natts) {
        return true;
    }

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    /* internal error */
    if (uhtup == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /* and let the tuple tell it */
    /* get init default value from tupleDesc.
     * The original Code is:
     * return heap_attisnull(tuple, attnum, tupleDesc);
     */
    return UHeapAttIsNull(uhtup, attnum, tupleDesc);
}

/*
 * UHeapSlotGetAllAttrs
 * This function forces all the entries of the slot's Datum/isnull
 * arrays to be valid.  The caller may then extract data directly
 * from those arrays instead of using heap_slot_getattr.
 *
 * @param slot: TableTuple slot from this attributes are extracted
 */
void UHeapSlotGetAllAttrs(TupleTableSlot *slot)
{
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid == slot->tts_tupleDescriptor->natts) {
        return;
    }

    SlotDeformUTuple(slot, (UHeapTuple)slot->tts_tuple, &slot->tts_off, slot->tts_tupleDescriptor->natts);
}

/*
 * UHeapSlotGetAttr
 * This function fetches an attribute of the slot's current tuple.
 * It is functionally equivalent to heap_getattr, but fetches of
 * multiple attributes of the same tuple will be optimized better,
 * because we avoid O(N^2) behavior from multiple calls of
 * nocachegetattr(), even when attcacheoff isn't usable.
 *
 * A difference from raw heap_getattr is that attnums beyond the
 * slot's tupdesc's last attribute will be considered NULL even
 * when the physical tuple is longer than the tupdesc.
 *
 * @param slot: TableTuple slot from this attribute is extracted
 * @param attnum: index of the atribute to be extracted.
 * @param isnull: set to true, if the attribute is NULL.
 */
Datum UHeapSlotGetAttr(TupleTableSlot *slot, int attnum, bool *isnull)
{
    /* sanity checks */
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    UHeapTuple utuple = (UHeapTuple)slot->tts_tuple;
    UHeapDiskTuple utup = NULL;

    /*
     * system attributes are handled by heap_getsysattr
     */
    if (attnum <= 0) {
        if (utuple == NULL) { /* internal error */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract system attribute from virtual tuple")));
        }
        return UHeapGetSysAttr(utuple, InvalidBuffer, attnum, tupleDesc, isnull);
    }

    /*
     * fast path if desired attribute already cached
     */
    if (attnum <= slot->tts_nvalid) {
        *isnull = slot->tts_isnull[attnum - 1];
        return slot->tts_values[attnum - 1];
    }

    /*
     * return NULL if attnum is out of range according to the tupdesc
     */
    if (attnum > tupleDesc->natts) {
        *isnull = true;
        return (Datum)0;
    }

    /*
     * If the attribute's column has been dropped, we force a NULL result.
     * This case should not happen in normal use, but it could happen if we
     * are executing a plan cached before the column was dropped.
     */
    if (tupleDesc->attrs[attnum - 1]->attisdropped) {
        *isnull = true;
        return (Datum)0;
    }

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    /* internal error */
    if (utuple == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /*
     * return NULL if attnum is out of range according to the tuple
     *
     * (We have to check this separately because of various inheritance and
     * table-alteration scenarios: the utuple could be either longer or shorter
     * than the tupdesc.)
     */
    utup = utuple->disk_tuple;
    if (attnum > (int)UHeapTupleHeaderGetNatts(utup)) {
            /* get init default value from tupleDesc.
             * The original Code is:
             * example code: *isnull = true;
             * example code: return (Datum) 0;
             */
            return heapGetInitDefVal(attnum, tupleDesc, isnull);
    }

    /*
     * check if target attribute is null: no point in groveling through tuple
     */
    if (UHeapDiskTupHasNulls(utup) && att_isnull(attnum - 1, utup->data)) {
        *isnull = true;
        return (Datum)0;
    }

    /*
     * Extract the attribute, along with any preceding attributes.
     */
    UHeapSlotGetSomeAttrs(slot, attnum);

    /*
     * The result is acquired from tts_values array.
     */
    *isnull = slot->tts_isnull[attnum - 1];
    return slot->tts_values[attnum - 1];
}

/*
 * Return a copy of uheap table minimal tuple representing the contents of the slot.
 * The copy needs to be palloc'd in the current memory context. The slot
 * itself is expected to remain unaffected. It is *not* expected to have
 * meaningful "system columns" in the copy. The copy is not be "owned" by
 * the slot i.e. the caller has to take responsibility to free memory
 * consumed by the slot.
 *
 * @param slot: slot from which minimal tuple to be copied.
 * @return slot's tuple minimal tuple copy
 */
MinimalTuple UHeapSlotCopyMinimalTuple(TupleTableSlot *slot)
{
    /*
     * sanity checks.
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    UHeapSlotGetAllAttrs(slot);

    return heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
}

/*
 * Return a minimal tuple "owned" by the slot. It is slot's responsibility
 * to free the memory consumed by the minimal tuple. If the slot can not
 * "own" a minimal tuple, it should not implement this callback and should
 * set it as NULL.
 *
 * @param slot: slot from minimal tuple to fetch.
 * @return slot's minimal tuple.
 *
 */
MinimalTuple UHeapSlotGetMinimalTuple(TupleTableSlot *slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    /*
     * If we have a minimal physical tuple (local or not) then just return it.
     */
    if (slot->tts_mintuple != NULL) {
        return slot->tts_mintuple;
    }
    /*
     * Otherwise, copy or build a minimal tuple, and store it into the slot.
     *
     * We may be called in a context that is shorter-lived than the tuple
     * slot, but we have to ensure that the materialized tuple will survive
     * anyway.
     */
    MemoryContext oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
    slot->tts_mintuple = UHeapSlotCopyMinimalTuple(slot);
    slot->tts_shouldFreeMin = true;
    MemoryContextSwitchTo(oldContext);

    /*
     * Note: we may now have a situation where we have a local minimal tuple
     * attached to a virtual or non-local physical tuple.  There seems no harm
     * in that at the moment, but if any materializes, we should change this
     * function to force the slot into minimal-tuple-only state.
     */
    return slot->tts_mintuple;
}

/*
 * Stores uheap minimal tuple in the TupleTableSlot. Release the current slots buffer and Free's any slot's
 * minimal and uheap tuple.
 *
 * @param mtup: minimal tuple to be stored.
 * @param slot: slot to store tuple.
 * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple.
 */
void UHeapSlotStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    /*
     * sanity checks
     */
    Assert(mtup != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree && (UHeapTuple)slot->tts_tuple != NULL) {
        UHeapFreeTuple(slot->tts_tuple);
        slot->tts_tuple = NULL;
    }
    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
    }

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer)) {
        ReleaseBuffer(slot->tts_buffer);
    }
    slot->tts_buffer = InvalidBuffer;

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = shouldFree;
    slot->tts_tuple = &slot->tts_minhdr;
    slot->tts_mintuple = mtup;

    slot->tts_minhdr.tupTableType = HEAP_TUPLE;
    slot->tts_minhdr.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
    slot->tts_minhdr.t_data = (HeapTupleHeader)((char *)mtup - MINIMAL_TUPLE_OFFSET);

    /* This slot now contains a HEAP_TUPLE so make sure to let callers know how to read it */
    slot->tts_tupslotTableAm = TAM_HEAP;

    /* no need to set t_self or t_tableOid since we won't allow access */
    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;
}

/*
 * Stores UHeapTuple in the slot, slot contents are cleared prior to storing.
 *
 * @param utuple: uheap to be stored.
 * @param slot: slot inwhich tuple needs to be stored.
 * @param should_free: whether slot assumes responsibility of freeing up the tuple.
 */
void UHeapSlotStoreUHeapTuple(UHeapTuple utuple, TupleTableSlot *slot, bool shouldFree, bool batchMode)
{
    /*
     * sanity checks
     */
    Assert(utuple != NULL && utuple->tupTableType == UHEAP_TUPLE);
    Assert(slot != NULL && slot->tts_tupslotTableAm == TAM_USTORE);
    Assert(slot->tts_tupleDescriptor != NULL);

    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
        slot->tts_shouldFreeMin = false;
    }

    UHeapSlotClear(slot);

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = shouldFree;
    slot->tts_shouldFreeMin = false;
    slot->tts_tuple = utuple;
    slot->tts_mintuple = NULL;

    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;
}

/*
 * Make the contents of the uheap table's slot contents solely depend on the slot(make them a local copy),
 * and not on underlying external resources like another memory context, buffers etc.
 *
 * @pram slot: slot to be materialized.
 */
Tuple UHeapMaterialize(TupleTableSlot *slot)
{
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupslotTableAm == TAM_USTORE);
    Assert(slot->tts_tupleDescriptor != NULL);
    /*
     * If we have a regular physical tuple, and it's locally palloc'd, we have
     * nothing to do.
     */
    if (slot->tts_tuple && slot->tts_shouldFree) {
        return slot->tts_tuple;
    }

    /*
     * Otherwise, copy or build a physical tuple, and store it into the slot.
     *
     * We may be called in a context that is shorter-lived than the tuple
     * slot, but we have to ensure that the materialized tuple will survive
     * anyway.
     */
    MemoryContext old_context = MemoryContextSwitchTo(slot->tts_mcxt);
    if (slot->tts_tuple != NULL) {
        slot->tts_tuple = UHeapCopyTuple((UHeapTuple)slot->tts_tuple);
    } else {
        slot->tts_tuple = UHeapFormTuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
    }
    slot->tts_shouldFree = true;
    MemoryContextSwitchTo(old_context);

    /*
     * Have to deform from scratch, otherwise tts_values[] entries could point
     * into the non-materialized tuple (which might be gone when accessed).
     */
    slot->tts_nvalid = 0;
    return slot->tts_tuple;
}

