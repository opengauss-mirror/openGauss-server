/* -------------------------------------------------------------------------
 *
 * heaptuple.cpp
 *	  This file contains heap tuple accessor and mutator routines, as well
 *	  as various tuple utilities.
 *
 * Some notes about varlenas and this code:
 *
 * Before Postgres 8.3 varlenas always had a 4-byte length header, and
 * therefore always needed 4-byte alignment (at least).  This wasted space
 * for short varlenas, for example CHAR(1) took 5 bytes and could need up to
 * 3 additional padding bytes for alignment.
 *
 * Now, a short varlena (up to 126 data bytes) is reduced to a 1-byte header
 * and we don't align it.  To hide this from datatype-specific functions that
 * don't want to deal with it, such a datum is considered "toasted" and will
 * be expanded back to the normal 4-byte-header format by pg_detoast_datum.
 * (In performance-critical code paths we can use pg_detoast_datum_packed
 * and the appropriate access macros to avoid that overhead.)  Note that this
 * conversion is performed directly in heap_form_tuple, without invoking
 * tuptoaster.c.
 *
 * This change will break any code that assumes it needn't detoast values
 * that have been put into a tuple but never sent to disk.	Hopefully there
 * are few such places.
 *
 * Varlenas still have alignment 'i' (or 'd') in pg_type/pg_attribute, since
 * that's the normal requirement for the untoasted format.  But we ignore that
 * for the 1-byte-header format.  This means that the actual start position
 * of a varlena datum may vary depending on which format it has.  To determine
 * what is stored, we have to require that alignment padding bytes be zero.
 * (openGauss actually has always zeroed them, but now it's required!)  Since
 * the first byte of a 1-byte-header varlena can never be zero, we can examine
 * the first byte after the previous datum to tell if it's a pad byte or the
 * start of a 1-byte-header varlena.
 *
 * Note that while formerly we could rely on the first varlena column of a
 * system catalog to be at the offset suggested by the C struct for the
 * catalog, this is now risky: it's only safe if the preceding field is
 * word-aligned, so that there will never be any padding.
 *
 * We don't pack varlenas whose attstorage is 'p', since the data type
 * isn't expecting to have to detoast values.  This is used in particular
 * by oidvector and int2vector, which are used in the system catalogs
 * and we'd like to still refer to them via C struct offsets.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/heaptuple.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef PGXC
#include "funcapi.h"
#include "catalog/pg_type.h"
#endif
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/tableam.h"
#include "access/ustore/knl_utuple.h"
#include "catalog/pg_proc.h"
#include "executor/tuptable.h"
#include "storage/buf/bufmgr.h"
#include "storage/pagecompress.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "access/ustore/knl_utuple.h"
#include "vecexecutor/vectorbatch.h"

#ifdef ENABLE_UT
#define static
#endif

/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) ((att)->attlen == -1 && (att)->attstorage != 'p')
/* Use this if it's already known varlena */
#define VARLENA_ATT_IS_PACKABLE(att) ((att)->attstorage != 'p')

/*
 * check to see if the attrIdx'th bit of and array of 8-bit bytes is set !!!
 * 0 represent this bit is set, otherwise it's 1 . please refer to heap_fill_bitmap().
 */
#define isAttrCompressed(attrIdx, bits) (!((bits)[(attrIdx) >> 3] & (1 << ((attrIdx)&0x07))))

/* ----------------------------------------------------------------
 *						misc support routines
 * ----------------------------------------------------------------
 */
/*
 * heap_compute_data_size
 *		Determine size of the data area of a tuple to be constructed
 */
Size heap_compute_data_size(TupleDesc tupleDesc, Datum *values, const bool *isnull)
{
    Size data_length = 0;
    int i;
    int numberOfAttributes = tupleDesc->natts;
    Form_pg_attribute *att = tupleDesc->attrs;

    for (i = 0; i < numberOfAttributes; i++) {
        Datum val;

        if (isnull[i]) {
            continue;
        }

        val = values[i];

        if (ATT_IS_PACKABLE(att[i]) && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val))) {
            /*
             * we're anticipating converting to a short varlena header, so
             * adjust length and don't count any alignment
             */
            data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
        } else {
            data_length = att_align_datum(data_length, att[i]->attalign, att[i]->attlen, val);
            data_length = att_addlength_datum(data_length, att[i]->attlen, val);
        }
    }

    return data_length;
}

/*
 * heap_fill_tuple
 *		Load data portion of a tuple from values/isnull arrays
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 */
void heap_fill_tuple(TupleDesc tupleDesc, Datum *values, const bool *isnull, char *data, Size data_size,
                     uint16 *infomask, bits8 *bit)
{
    bits8 *bitP = NULL;
    uint32 bitmask;
    int i;
    int numberOfAttributes = tupleDesc->natts;
    Form_pg_attribute *att = tupleDesc->attrs;
    errno_t rc = EOK;
    char *begin = data;

#ifdef USE_ASSERT_CHECKING
    char *start = data;
#endif

    if (bit != NULL) {
        bitP = &bit[-1];
        bitmask = HIGHBIT;
    } else {
        /* just to keep compiler quiet */
        bitP = NULL;
        bitmask = 0;
    }

    *infomask &= ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);

    for (i = 0; i < numberOfAttributes; i++) {
        Size data_length;
        Size remain_length = data_size - (size_t)(data - begin);

        if (bit != NULL) {
            if (bitmask != HIGHBIT) {
                bitmask <<= 1;
            } else {
                bitP += 1;
                *bitP = 0x0;
                bitmask = 1;
            }

            if (isnull[i]) {
                *infomask |= HEAP_HASNULL;
                continue;
            }

            *bitP |= bitmask;
        }

        /*
         * XXX we use the att_align macros on the pointer value itself, not on
         * an offset.  This is a bit of a hack.
         */
        if (att[i]->attbyval) {
            /* pass-by-value */
            data = (char *)att_align_nominal(data, att[i]->attalign);
            store_att_byval(data, values[i], att[i]->attlen);
            data_length = att[i]->attlen;
        } else if (att[i]->attlen == -1) {
            /* varlena */
            Pointer val = DatumGetPointer(values[i]);

            *infomask |= HEAP_HASVARWIDTH;
            if (VARATT_IS_EXTERNAL(val)) {
                *infomask |= HEAP_HASEXTERNAL;
                /* no alignment, since it's short by definition */
                data_length = VARSIZE_EXTERNAL(val);
                rc = memcpy_s(data, remain_length, val, data_length);
                securec_check(rc, "\0", "\0");
            } else if (VARATT_IS_SHORT(val)) {
                /* no alignment for short varlenas */
                data_length = VARSIZE_SHORT(val);
                rc = memcpy_s(data, remain_length, val, data_length);
                securec_check(rc, "\0", "\0");
            } else if (VARLENA_ATT_IS_PACKABLE(att[i]) && VARATT_CAN_MAKE_SHORT(val)) {
                /* convert to short varlena -- no alignment */
                data_length = VARATT_CONVERTED_SHORT_SIZE(val);
                SET_VARSIZE_SHORT(data, data_length);
                if (data_length > 1) {
                    rc = memcpy_s(data + 1, remain_length - 1, VARDATA(val), data_length - 1);
                    securec_check(rc, "\0", "\0");
                }
            } else {
                /* full 4-byte header varlena */
                data = (char *)att_align_nominal(data, att[i]->attalign);
                data_length = VARSIZE(val);
                rc = memcpy_s(data, remain_length, val, data_length);
                securec_check(rc, "\0", "\0");
            }
        } else if (att[i]->attlen == -2) {
            /* cstring ... never needs alignment */
            *infomask |= HEAP_HASVARWIDTH;
            Assert(att[i]->attalign == 'c');
            data_length = strlen(DatumGetCString(values[i])) + 1;
            rc = memcpy_s(data, remain_length, DatumGetPointer(values[i]), data_length);
            securec_check(rc, "\0", "\0");
        } else {
            /* fixed-length pass-by-reference */
            data = (char *)att_align_nominal(data, att[i]->attalign);
            Assert(att[i]->attlen > 0);
            data_length = att[i]->attlen;
            rc = memcpy_s(data, remain_length, DatumGetPointer(values[i]), data_length);
            securec_check(rc, "\0", "\0");
        }

        data += data_length;
    }

    Assert((size_t)(data - start) == data_size);
}

/* ----------------------------------------------------------------
 *						heap tuple interface
 * ----------------------------------------------------------------
 */
/* ----------------
 *		heap_attisnull	- returns TRUE iff tuple attribute is not present
 * ----------------
 */
bool heap_attisnull(HeapTuple tup, int attnum, TupleDesc tupDesc)
{
    if (attnum > (int)HeapTupleHeaderGetNatts(tup->t_data, tupDesc)) {
        return true;
    }

    if (attnum > 0) {
        if (HeapTupleNoNulls(tup)) {
            return false;
        }
        return att_isnull(((uint)(attnum - 1)), tup->t_data->t_bits);
    }

    switch (attnum) {
        case TableOidAttributeNumber:
        case SelfItemPointerAttributeNumber:
        case ObjectIdAttributeNumber:
        case MinTransactionIdAttributeNumber:
        case MinCommandIdAttributeNumber:
        case MaxTransactionIdAttributeNumber:
        case MaxCommandIdAttributeNumber:
#ifdef PGXC
        case XC_NodeIdAttributeNumber:
        case BucketIdAttributeNumber:
        case UidAttributeNumber:
#endif
            /* these are never null */
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("invalid attnum: %d", attnum)));
    }

    return false;
}

/* get init default value from tupleDesc.
 * attrinitdefvals of tupleDesc come from the attrinitdefval of pg_attribute
 */
Datum heapGetInitDefVal(int attNum, TupleDesc tupleDesc, bool *isNull)
{
    *isNull = true;

    if (tupleDesc->initdefvals != NULL) {
        *isNull = tupleDesc->initdefvals[attNum - 1].isNull;
        if (!(*isNull)) {
            return fetchatt(tupleDesc->attrs[attNum - 1], tupleDesc->initdefvals[attNum - 1].datum);
        }
    }

    return (Datum)0;
}

/* Another version of heap_attisnull. Think about attinitdefval of pg_attribute.
 * The function is for ordinary relation table, not for system table such as pg_class etc,
 * because attribute number of tuple is equal to that of tupledesc for ever.
 */
bool relationAttIsNull(HeapTuple tup, int attNum, TupleDesc tupleDesc)
{
    if (attNum > (int)HeapTupleHeaderGetNatts(tup->t_data, tupleDesc)) {
        return (tupleDesc->initdefvals == NULL) ? true : tupleDesc->initdefvals[attNum - 1].isNull;
    }
    return heap_attisnull(tup, attNum, tupleDesc);
}

/* ----------------
 *		nocachegetattr && nocache_cmprs_get_attr
 *
 *		This only gets called from fastgetattr() macro, in cases where
 *		we can't use a cacheoffset and the value is not null.
 *
 *		This caches attribute offsets in the attribute descriptor.
 *
 *		An alternative way to speed things up would be to cache offsets
 *		with the tuple, but that seems more difficult unless you take
 *		the storage hit of actually putting those offsets into the
 *		tuple you send to disk.  Yuck.
 *
 *		This scheme will be slightly slower than that, but should
 *		perform well for queries which hit large #'s of tuples.  After
 *		you cache the offsets once, examining all the other tuples using
 *		the same attribute descriptor will go much quicker. -cim 5/4/91
 *
 *		NOTE: if you need to change this code, see also heap_deform_tuple.
 *		Also see nocache_index_getattr, which is the same code for index
 *		tuples.
 * ----------------
 */
Datum nocachegetattr(HeapTuple tuple, uint32 attnum, TupleDesc tupleDesc)
{
    HeapTupleHeader tup = tuple->t_data;
    Form_pg_attribute *att = tupleDesc->attrs;
    char *tp = NULL;         /* ptr to data part of tuple */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* do we have to walk attrs? */
    int off;                 /* current offset within data */
    bool heapToUHeap = tupleDesc->tdTableAmType == TAM_USTORE;

    /*
     * Ustore has different alignment rules so we force slow = true here.
     * See the comments in heap_deform_tuple() for more information.
     */
    slow = heapToUHeap;
    
    /* ----------------
     *	 Three cases:
     *
     *	 1: No nulls and no variable-width attributes.
     *	 2: Has a null or a var-width AFTER att.
     *	 3: Has nulls or var-widths BEFORE att.
     * ----------------
     */
    Assert(!HEAP_TUPLE_IS_COMPRESSED(tup));

    /*
     * important:
     *     maybe this function is not safe for accessing some attribute, which is different
     *     from methods slot_getattr(), slot_getallattrs(), slot_getsomeattrs(). those three
     *     always make sure that attnum is always valid between 1 and HeapTupleHeaderGetNatts(),
     *     because the caller has guarantee that.
     *     we find that the caller fastgetattr() doesn't guarantee the validition, and top callers
     *     are almost in system table level, for example pg_class and so on. so that it's NOT
     *     recommended that users' table functions call fastgetattr() and nocachegetattr();
     */
    Assert(attnum <= HeapTupleHeaderGetNatts(tup, tupleDesc));
    attnum--;

    if (!HeapTupleNoNulls(tuple)) {
        /*
         * there's a null somewhere in the tuple
         *
         * check to see if any preceding bits are null...
         */
        int byte = attnum >> 3;
        int finalbit = attnum & 0x07;

        /* check for nulls "before" final bit of last byte */
        if ((~bp[byte]) & (((uint)1 << finalbit) - 1)) {
            slow = true;
        } else {
            /* check for nulls in any "earlier" bytes */
            int i;

            for (i = 0; i < byte; i++) {
                if (bp[i] != 0xFF) {
                    slow = true;
                    break;
                }
            }
        }
    }

    tp = (char *)tup + tup->t_hoff;

    if (!slow) {
        /*
         * If we get here, there are no nulls up to and including the target
         * attribute.  If we have a cached offset, we can use it.
         */
        if (att[attnum]->attcacheoff >= 0) {
            return fetchatt(att[attnum], tp + att[attnum]->attcacheoff);
        }

        /*
         * Otherwise, check for non-fixed-length attrs up to and including
         * target.	If there aren't any, it's safe to cheaply initialize the
         * cached offsets for these attrs.
         */
        if (HeapTupleHasVarWidth(tuple)) {
            uint32 j;

            for (j = 0; j <= attnum; j++) {
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
        while (j < natts && att[j]->attcacheoff > 0) {
            j++;
        }

        off = att[j - 1]->attcacheoff + att[j - 1]->attlen;

        for (; j < natts; j++) {
            if (att[j]->attlen <= 0) {
                break;
            }

            off = att_align_nominal((uint32)off, att[j]->attalign);

            att[j]->attcacheoff = off;

            off += att[j]->attlen;
        }

        Assert(j > attnum);

        off = att[attnum]->attcacheoff;
    } else {
        bool usecache = !heapToUHeap;
        uint32 i;

        /*
         * Now we know that we have to walk the tuple CAREFULLY.  But we still
         * might be able to cache some offsets for next time.
         *
         * Note - This loop is a little tricky.  For each non-null attribute,
         * we have to first account for alignment padding before the attr,
         * then advance over the attr based on its length.	Nulls have no
         * storage and no alignment padding either.  We can use/set
         * attcacheoff until we reach either a null or a var-width attribute.
         */
        off = 0;
        for (i = 0;; i++) { /* loop exit is at "break" */
            Assert(i < (uint32)tupleDesc->natts);
            if (HeapTupleHasNulls(tuple) && att_isnull(i, bp)) {
                usecache = false;
                continue; /* this cannot be the target att */
            }

            /* If we know the next offset, we can skip the rest */
            if (usecache && att[i]->attcacheoff >= 0) {
                off = att[i]->attcacheoff;
            } else if (att[i]->attlen == -1) {
                /*
                 * We can only cache the offset for a varlena attribute if the
                 * offset is already suitably aligned, so that there would be
                 * no pad bytes in any case: then the offset will be valid for
                 * either an aligned or unaligned value.
                 */
                if (usecache && (uintptr_t)(off) == att_align_nominal((uint32)off, att[i]->attalign)) {
                    att[i]->attcacheoff = off;
                } else {
                    off = att_align_pointer((uint32)off, att[i]->attalign, -1, tp + off);
                    usecache = false;
                }
            } else {
                /* not varlena, so safe to use att_align_nominal */
                off = att_align_nominal((uint32)off, att[i]->attalign);

                if (usecache) {
                    att[i]->attcacheoff = off;
                }
            }

            if (i == attnum) {
                break;
            }

            off = att_addlength_pointer(off, att[i]->attlen, tp + off);

            if (usecache && att[i]->attlen <= 0) {
                usecache = false;
            }
        }
    }

    return fetchatt(att[attnum], tp + off);
}

/* ----------------
 *		heap_getsysattr
 *
 *		Fetch the value of a system attribute for a tuple.
 *
 * This is a support routine for the heap_getattr macro.  The macro
 * has already determined that the attnum refers to a system attribute.
 * ----------------
 */
Datum heap_getsysattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    Datum result;

    Assert(tup);

    /* Currently, no sys attribute ever reads as NULL. */
    *isnull = false;

    switch (attnum) {
        case SelfItemPointerAttributeNumber:
            /* pass-by-reference datatype */
            result = PointerGetDatum(&(tup->t_self));
            break;
        case ObjectIdAttributeNumber:
            result = ObjectIdGetDatum(HeapTupleGetOid(tup));
            break;
        case MinTransactionIdAttributeNumber:
            result = TransactionIdGetDatum(HeapTupleGetRawXmin(tup));
            break;
        case MaxTransactionIdAttributeNumber:
            result = TransactionIdGetDatum(HeapTupleGetRawXmax(tup));
            break;
        case MinCommandIdAttributeNumber:
        case MaxCommandIdAttributeNumber:

            /*
             * cmin and cmax are now both aliases for the same field, which
             * can in fact also be a combo command id.	XXX perhaps we should
             * return the "real" cmin or cmax if possible, that is if we are
             * inside the originating transaction?
             */
            result = CommandIdGetDatum(HeapTupleHeaderGetRawCommandId(tup->t_data));
            break;
        case TableOidAttributeNumber:
            result = ObjectIdGetDatum(tup->t_tableOid);
            break;
#ifdef PGXC
        case BucketIdAttributeNumber:
            result = ObjectIdGetDatum((uint2)tup->t_bucketId);
            break;
        case XC_NodeIdAttributeNumber:
            result = UInt32GetDatum(tup->t_xc_node_id);
            break;
        case UidAttributeNumber:
            result = UInt64GetDatum(HeapTupleGetUid(tup));
            break;
#endif
        default:
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("invalid attnum: %d", attnum)));
            result = 0; /* keep compiler quiet */
            break;
    }
    return result;
}

/* ----------------
 *		heap_copytuple && heapCopyCompressedTuple
 *
 * returns a copy of an entire tuple, and decompressed first if it's row-compressed.
 *
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 *
 * heap_copytuple is used for tuples uncompressed, For all openGauss System Relations,
 * their tuples MUST be uncompressed.  And This is also suitable for that tuples which
 * are not compressed at all, for example, index tuples now.
 *
 * heapCopyCompressedTuple serves for tuples compressed. <dictPage> should be passed in.
 *
 * And macro HEAP_COPY_TUPLE are provided, both wrapper for uncompressed and compressed tuples.
 * ----------------
 */
HeapTuple heap_copytuple(HeapTuple tuple)
{
    HeapTuple newTuple;
    errno_t rc = EOK;

    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL) {
        return NULL;
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));

    newTuple = (HeapTuple)palloc(HEAPTUPLESIZE + tuple->t_len);
    newTuple->tupTableType = HEAP_TUPLE;
    newTuple->t_len = tuple->t_len;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
    newTuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(newTuple, tuple);

#ifdef PGXC
    newTuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    newTuple->t_data = (HeapTupleHeader)((char *)newTuple + HEAPTUPLESIZE);
    rc = memcpy_s((char *)newTuple->t_data, tuple->t_len, (char *)tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    return newTuple;
}

/* ----------------
 *		heap_copytuple_with_tuple && heapCopyTupleWithCompressedTuple
 *
 * heap_copytuple_with_tuple
 *		copy a tuple into a caller-supplied HeapTuple management struct
 * 		Note that after calling function heap_copytuple_with_tuple, the "dest"
 * 		HeapTuple will not be allocated as a single palloc() block (unlike with
 *		heap_copytuple()).
 *
 * heapCopyTupleWithCompressedTuple
 * 		decompress and copy a tuple into a aller-supplied HeapTuple management struct.
 * 		Important: caller must provider the memory of dest->t_data.
 *
 * ----------------
 */
void heap_copytuple_with_tuple(HeapTuple src, HeapTuple dest)
{
    errno_t rc = EOK;

    /* case 1: empty tuple */
    if (!HeapTupleIsValid(src) || src->t_data == NULL) {
        dest->t_data = NULL;
        return;
    }

    /* case 2: copy the normal tuple without compressing */
    Assert(!HEAP_TUPLE_IS_COMPRESSED(src->t_data));
    dest->t_len = src->t_len;
    dest->t_self = src->t_self;
    dest->t_tableOid = src->t_tableOid;
    dest->t_bucketId = src->t_bucketId;
    HeapTupleCopyBase(dest, src);
#ifdef PGXC
    dest->t_xc_node_id = src->t_xc_node_id;
#endif
    dest->t_data = (HeapTupleHeader)palloc(src->t_len);
    rc = memcpy_s((char *)dest->t_data, src->t_len, (char *)src->t_data, src->t_len);
    securec_check(rc, "\0", "\0");
}

/*
 * heap_form_tuple
 *		construct a tuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 */
HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull)
{
    HeapTuple tuple;    /* return tuple */
    HeapTupleHeader td; /* tuple data */
    Size len, data_len;
    int hoff;
    bool hasnull = false;
    Form_pg_attribute *att = tupleDescriptor->attrs;
    int numberOfAttributes = tupleDescriptor->natts;
    int i;

    if (numberOfAttributes > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS), errmsg("number of columns (%d) exceeds limit (%d)",
                                                                  numberOfAttributes, MaxTupleAttributeNumber)));
    }

    /*
     * Check for nulls and embedded tuples; expand any toasted attributes in
     * embedded tuples.  This preserves the invariant that toasting can only
     * go one level deep.
     *
     * We can skip calling toast_flatten_tuple_attribute() if the attribute
     * couldn't possibly be of composite type.  All composite datums are
     * varlena and have alignment 'd'; furthermore they aren't arrays. Also,
     * if an attribute is already toasted, it must have been sent to disk
     * already and so cannot contain toasted attributes.
     */
    for (i = 0; i < numberOfAttributes; i++) {
        if (isnull[i]) {
            hasnull = true;
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
                   !VARATT_IS_EXTENDED(DatumGetPointer(values[i]))) {
            values[i] = toast_flatten_tuple_attribute(values[i], att[i]->atttypid, att[i]->atttypmod);
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'i' &&
            VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(values[i])) &&
            !(att[i]->atttypid == CLOBOID || att[i]->atttypid == BLOBOID)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("only suport type(clob/blob) for more than 1G toast")));
        }
    }

    /*
     * Determine total space needed
     */
    len = offsetof(HeapTupleHeaderData, t_bits);

    if (hasnull) {
        len += BITMAPLEN(numberOfAttributes);
    }

    if (tupleDescriptor->tdhasoid) {
        len += sizeof(Oid);
    }

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.	Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    Size allocSize = HEAPTUPLESIZE + len;
    if (tupleDescriptor->tdhasuids) { /* prealloc 8 bytes */
        allocSize = HEAPTUPLESIZE + MAXALIGN(hoff + sizeof(uint64)) + data_len;
    }
    tuple = (HeapTuple)heaptup_alloc(allocSize);
    tuple->t_data = td = (HeapTupleHeader)((char *)tuple + HEAPTUPLESIZE);

    /*
     * And fill in the information.  Note we fill the Datum fields even though
     * this tuple may never become a Datum.
     */
    tuple->t_len = len;
    ItemPointerSetInvalid(&(tuple->t_self));
    tuple->t_tableOid = InvalidOid;
    tuple->t_bucketId = InvalidBktId;
    HeapTupleSetZeroBase(tuple);
#ifdef PGXC
    tuple->t_xc_node_id = 0;
#endif

    HeapTupleHeaderSetDatumLength(td, len);
    HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);

    HeapTupleHeaderSetNatts(td, numberOfAttributes);
    td->t_hoff = hoff;

    /* else leave infomask = 0 */
    if (tupleDescriptor->tdhasoid) {
        td->t_infomask = HEAP_HASOID;
    }

    td->t_infomask &= ~HEAP_UID_MASK;

    heap_fill_tuple(tupleDescriptor, values, isnull, (char *)td + hoff, data_len, &td->t_infomask,
                    (hasnull ? td->t_bits : NULL));

    return tuple;
}

/*
 *		heap_formtuple
 *
 *		construct a tuple from the given values[] and nulls[] arrays
 *
 *		Null attributes are indicated by a 'n' in the appropriate byte
 *		of nulls[]. Non-null attributes are indicated by a ' ' (space).
 *
 * OLD API with char 'n'/' ' convention for indicating nulls.
 * This is deprecated and should not be used in new code, but we keep it
 * around for use by old add-on modules.
 */
HeapTuple heap_formtuple(TupleDesc tupleDescriptor, Datum *values, const char *nulls)
{
    HeapTuple tuple; /* return tuple */
    int numberOfAttributes = tupleDescriptor->natts;
    bool *boolNulls = (bool *)palloc(numberOfAttributes * sizeof(bool));
    int i;

    for (i = 0; i < numberOfAttributes; i++) {
        boolNulls[i] = (nulls[i] == 'n');
    }

    tuple = heap_form_tuple(tupleDescriptor, values, boolNulls);

    pfree(boolNulls);

    return tuple;
}

/*
 * heap_modify_tuple
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * The replValues, replIsnull, and doReplace arrays must be of the length
 * indicated by tupleDesc->natts.  The new tuple is constructed using the data
 * from replValues/replIsnull at columns where doReplace is true, and using
 * the data from the old tuple at columns where doReplace is false.
 *
 * The result is allocated in the current memory context.
 */
HeapTuple heap_modify_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *replValues, const bool *replIsnull,
                            const bool *doReplace)
{
    int numberOfAttributes = tupleDesc->natts;
    int attoff;
    Datum *values = NULL;
    bool *isnull = NULL;
    HeapTuple newTuple;

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

    heap_deform_tuple(tuple, tupleDesc, values, isnull);

    for (attoff = 0; attoff < numberOfAttributes; attoff++) {
        if (doReplace[attoff]) {
            values[attoff] = replValues[attoff];
            isnull[attoff] = replIsnull[attoff];
        }
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = heap_form_tuple(tupleDesc, values, isnull);

    pfree(values);
    pfree(isnull);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
    newTuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(newTuple, tuple);
#ifdef PGXC
    newTuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    if (tupleDesc->tdhasoid) {
        HeapTupleSetOid(newTuple, HeapTupleGetOid(tuple));
    }

    return newTuple;
}

/*
 *		heap_modifytuple
 *
 *		forms a new tuple from an old tuple and a set of replacement values.
 *		returns a new palloc'ed tuple.
 *
 * OLD API with char 'n'/' ' convention for indicating nulls, and
 * char 'r'/' ' convention for indicating whether to replace columns.
 * This is deprecated and should not be used in new code, but we keep it
 * around for use by old add-on modules.
 */
HeapTuple heap_modifytuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *replValues, const char *replNulls,
                           const char *replActions)
{
    HeapTuple result;
    int numberOfAttributes = tupleDesc->natts;
    bool *boolNulls = (bool *)palloc(numberOfAttributes * sizeof(bool));
    bool *boolActions = (bool *)palloc(numberOfAttributes * sizeof(bool));
    int attnum;

    for (attnum = 0; attnum < numberOfAttributes; attnum++) {
        boolNulls[attnum] = (replNulls[attnum] == 'n');
        boolActions[attnum] = (replActions[attnum] == 'r');
    }

    result = heap_modify_tuple(tuple, tupleDesc, replValues, boolNulls, boolActions);

    pfree(boolNulls);
    pfree(boolActions);

    return result;
}

/*
 * heap_deform_tuple
 *		Given a tuple, extract data into values/isnull arrays; this is
 *		the inverse of heap_form_tuple.
 *
 *		Storage for the values/isnull arrays is provided by the caller;
 *		it should be sized according to tupleDesc->natts not
 *		HeapTupleHeaderGetNatts(tuple->t_data, tupleDesc).
 *
 *		Note that for pass-by-reference datatypes, the pointer placed
 *		in the Datum will point into the given tuple.
 *
 *		When all or most of a tuple's fields need to be extracted,
 *		this routine will be significantly quicker than a loop around
 *		heap_getattr; the loop will become O(N^2) as soon as any
 *		noncacheable attribute offsets are involved.
 */
void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull)
{
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    uint32 tdesc_natts = tupleDesc->natts;
    uint32 natts; /* number of atts to extract */
    uint32 attnum;
    char *tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool heapToUHeap = tupleDesc->tdTableAmType == TAM_USTORE;

    /*
     * We need to default to slow if the TupleDesc type is UStore because
     * UStore has different alignment rules than Heap.
     *
     * Consider a row data where the first column is INT (4 byte alignment)
     * and the second column is a timestamp (8 byte alignment).
     * In UStore, the atcacheoff (if set) for the first and second columns are
     * 0 and 4, respectively because UStore does not do alignment padding for attbyval columns.
     * However, this is not the case for Heap. For Heap, the 1st and 2nd columns will be
     * at 0 and 8, respectively because of alignment padding. see heap_fill_tuple().
     * So during heap_deform_tuple(), if atcacheoff is set for the second column,
     * deform thinks the second column is at offset 4 because that is what the tupleDesc says
     * when in fact the 2nd column was written at offset 8.
    */
    bool slow = heapToUHeap;       /* can we use/set attcacheoff? */

    Assert(!HEAP_TUPLE_IS_COMPRESSED(tup));
    natts = HeapTupleHeaderGetNatts(tup, tupleDesc);

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = Min(natts, tdesc_natts);
    if (natts > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
                        errmsg("number of columns (%u) exceeds limit (%d)", natts, MaxTupleAttributeNumber)));
    }

    tp = (char *)tup + tup->t_hoff;
    off = 0;

    for (attnum = 0; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = att[attnum];

        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            slow = true; /* can't use attcacheoff anymore */
            continue;
        }

        isnull[attnum] = false;

        if (!slow && thisatt->attcacheoff >= 0) {
            off = thisatt->attcacheoff;
        } else if (thisatt->attlen == -1) {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if (!slow && (uintptr_t)(off) == att_align_nominal(off, thisatt->attalign)) {
                thisatt->attcacheoff = off;
            } else {
                off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
                slow = true;
            }
        } else {
            /* not varlena, so safe to use att_align_nominal */
            off = att_align_nominal(off, thisatt->attalign);

            if (!slow)
                thisatt->attcacheoff = off;
        }

        values[attnum] = fetchatt(thisatt, tp + off);

        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0) {
            slow = true; /* can't use attcacheoff anymore */
        }
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
    for (; attnum < tdesc_natts; attnum++) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: values[attnum] = (Datum) 0;
         * example code: isnull[attnum] = true;
         */
        values[attnum] = heapGetInitDefVal(attnum + 1, tupleDesc, &isnull[attnum]);
    }
}

/*
 *		heap_deformtuple
 *
 *		Given a tuple, extract data into values/nulls arrays; this is
 *		the inverse of heap_formtuple.
 *
 *		Storage for the values/nulls arrays is provided by the caller;
 *		it should be sized according to tupleDesc->natts not
 *		HeapTupleHeaderGetNatts(tuple->t_data, tupleDesc).
 *
 *		Note that for pass-by-reference datatypes, the pointer placed
 *		in the Datum will point into the given tuple.
 *
 *		When all or most of a tuple's fields need to be extracted,
 *		this routine will be significantly quicker than a loop around
 *		heap_getattr; the loop will become O(N^2) as soon as any
 *		noncacheable attribute offsets are involved.
 *
 * OLD API with char 'n'/' ' convention for indicating nulls.
 * This is deprecated and should not be used in new code, but we keep it
 * around for use by old add-on modules.
 */
void heap_deformtuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, char *nulls)
{
    int natts = tupleDesc->natts;
    bool *boolNulls = (bool *)palloc(natts * sizeof(bool));
    int attnum;

    heap_deform_tuple(tuple, tupleDesc, values, boolNulls);

    for (attnum = 0; attnum < natts; attnum++) {
        nulls[attnum] = (boolNulls[attnum] ? 'n' : ' ');
    }

    pfree(boolNulls);
}

static void slot_deform_cmprs_tuple(TupleTableSlot *slot, uint32 natts);

static void deform_next_attribute(bool& slow, long& off, Form_pg_attribute thisatt, char* tp)
{
    if (!slow && thisatt->attcacheoff >= 0) {
        off = thisatt->attcacheoff;
    } else if (thisatt->attlen == -1) {
        /*
         * We can only cache the offset for a varlena attribute if the
         * offset is already suitably aligned, so that there would be no
         * pad bytes in any case: then the offset will be valid for either
         * an aligned or unaligned value.
         */
        if (!slow && (uintptr_t)(off) == att_align_nominal(off, thisatt->attalign)) {
            thisatt->attcacheoff = off;
        } else {
            off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
            slow = true;
        }
    } else {
        /* not varlena, so safe to use att_align_nominal */
        off = att_align_nominal(off, thisatt->attalign);

        if (!slow) {
            thisatt->attcacheoff = off;
        }
    }
}

/*
 * slot_deform_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple
 *		into its Datum/isnull arrays.  Data is extracted up through the
 *		natts'th column (caller must ensure this is a legal column number).
 *
 *		This is essentially an incremental version of heap_deform_tuple:
 *		on each call we extract attributes up to the one needed, without
 *		re-computing information about previously extracted attributes.
 *		slot->tts_nvalid is the number of attributes already extracted.
 */
static void slot_deform_tuple(TupleTableSlot *slot, uint32 natts)
{
    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    Assert(tuple->tupTableType == HEAP_TUPLE);
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    Datum *values = slot->tts_values;
    bool *isnull = slot->tts_isnull;
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    uint32 attnum;
    char *tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* can we use/set attcacheoff? */
    bool heapToUHeap = tupleDesc->tdTableAmType == TAM_USTORE;
	
    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    attnum = slot->tts_nvalid;
    if (attnum == 0) {
        /* Start from the first attribute */
        off = 0;
        slow = false;
    } else {
        /* Restore state from previous execution */
        off = slot->tts_off;
        slow = slot->tts_slow;
    }

    /*
     * Ustore has different alignment rules so we force slow = true here.
     * See the comments in heap_deform_tuple() for more information.
     */
    slow = heapToUHeap ? true : slow;

    tp = (char *)tup + tup->t_hoff;

    for (; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = att[attnum];

        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            slow = true; /* can't use attcacheoff anymore */
            continue;
        }

        isnull[attnum] = false;

        deform_next_attribute(slow, off, thisatt, tp);

        values[attnum] = fetchatt(thisatt, tp + off);

        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0) {
            slow = true; /* can't use attcacheoff anymore */
        }
    }

    /*
     * Save state for next execution
     */
    slot->tts_nvalid = attnum;
    slot->tts_off = off;
    slot->tts_slow = slow;
}


static void slot_deform_batch(TupleTableSlot *slot, VectorBatch* batch, int cur_rows, uint32 natts)
{
    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    Assert(tuple->tupTableType == HEAP_TUPLE);
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    uint32 attnum;
    char *tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* can we use/set attcacheoff? */
    bool heapToUHeap = tupleDesc->tdTableAmType == TAM_USTORE;

    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    attnum = 0;
    off = 0;
    slow = false;

    /*
     * Ustore has different alignment rules so we force slow = true here.
     * See the comments in heap_deform_tuple() for more information.
     */
    slow = heapToUHeap ? true : slow;

    tp = (char *)tup + tup->t_hoff;

    for (; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = att[attnum];
        ScalarVector* pVector = &batch->m_arr[attnum];

        if (hasnulls && att_isnull(attnum, bp)) {
            pVector->m_vals[cur_rows] = (Datum)0;
            SET_NULL(pVector->m_flag[cur_rows]);
            slow = true; /* can't use attcacheoff anymore */

            /* stole the flag for perf */
            pVector->m_const = true;
            continue;
        }

        SET_NOTNULL(pVector->m_flag[cur_rows]);

        deform_next_attribute(slow, off, thisatt, tp);

        pVector->m_vals[cur_rows] = fetchatt(thisatt, tp + off);

        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0) {
            slow = true; /* can't use attcacheoff anymore */
        }
    }
}

#ifdef PGXC

/*
 * slot_extract_anyarray_from_buff
 *  Extract one row anyarray data from the buffer message into slot tts_value.
 *  'need_transform_anyarray':
 *    When buffer->data is from remote CN/DN, remote CN/DN use printtup -> anyarray_out
 *    output just string not varattrib to this CN, so the content is string, not varattrib.but if
 *    buffer->data is from local, the content shoud be varattrib. so we must handle the data in different way.
 *    when call from parallel remote process, the 'need_transform_anyarray' is true, and we use 'anyarray_in' transform
 *    string to array. because in parallel remote proess, we just use this data for printtup-> array_out to output.
 *    and in printtup, it handle the tts_values with attr in tts_values, so it is OK.
 *    the remote query(not parallel), put slot in tup, but when printtup, it use info in slot not tup. even data in tup
 *    is not correct, it can print correct.
 *  index means attribute index.
 *  buff is the input message buff.
 *  len is length of anyarray in buff.
 */
static void slot_extract_anyarray_from_buff(TupleTableSlot *slot, int index, const StringInfo buffer, int len,
                                            bool need_transform_anyarray)
{
    char *pstr = NULL;
    Datum array_datum;
    Size data_length;
    errno_t rc = EOK;
    Form_pg_attribute *att = slot->tts_tupleDescriptor->attrs;
    int attnum = slot->tts_tupleDescriptor->natts;

    if (index >= attnum) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("index is not correct")));
    }

    /* If from remote cn, the datatype in buffer is string, not varattrib, so we should not use it as varattrib */
    if ((att[index]->attlen == -1) && (!need_transform_anyarray)) {
        data_length = VARSIZE_ANY(buffer->data);
        if (data_length <= (Size)((uint32)len + 1)) {
            data_length = len + 1;
        }
    } else {
        data_length = len + 1;
    }

    if (data_length > MaxAllocSize) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("data length is not correct")));
    }
    pstr = (char *)palloc0(data_length);
    rc = memcpy_s(pstr, len + 1, buffer->data, len);
    securec_check(rc, "", "");
    pstr[len] = '\0';
    array_datum = (Datum)pstr;
    if (need_transform_anyarray) {
        array_datum = OidFunctionCall3Coll(ANYARRAYINFUNCOID, InvalidOid, CStringGetDatum(pstr),
                                           UInt32GetDatum(CSTRINGOID),
                                           Int32GetDatum(slot->tts_tupleDescriptor->tdtypmod));
        pfree_ext(pstr);
    }
    slot->tts_values[index] = array_datum;

    return;
}

/*
 * slot_deform_datarow
 * 		Extract data from the DataRow message into Datum/isnull arrays.
 * 		We always extract all atributes, as specified in tts_tupleDescriptor,
 * 		because there is no easy way to find random attribute in the DataRow.
 */
static void slot_deform_datarow(TupleTableSlot *slot, bool need_transform_anyarray)
{
    int attnum;
    int i;
    int col_count;
    char *cur = slot->tts_dataRow;
    StringInfo buffer;
    uint16 n16;
    uint32 n32;
    MemoryContext oldcontext;
    errno_t rc = EOK;

    if (slot->tts_tupleDescriptor == NULL || slot->tts_dataRow == NULL) {
        return;
    }

    Form_pg_attribute *att = slot->tts_tupleDescriptor->attrs;
    attnum = slot->tts_tupleDescriptor->natts;

    /* fastpath: exit if values already extracted */
    if (slot->tts_nvalid == attnum) {
        return;
    }

    Assert(slot->tts_dataRow);

    rc = memcpy_s(&n16, sizeof(uint16), cur, 2);
    securec_check(rc, "\0", "\0");
    cur += 2;
    col_count = ntohs(n16);
    if (col_count != attnum) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Tuple does not match the descriptor")));
    }

    /*
     * Ensure info about input functions is available as long as slot lives
     * as well as deformed values
     */
    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

    if (slot->tts_attinmeta == NULL) {
        slot->tts_attinmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);
    }

    if (slot->tts_per_tuple_mcxt == NULL) {
        slot->tts_per_tuple_mcxt = AllocSetContextCreate(slot->tts_mcxt, "SlotPerTupleMcxt", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }

    MemoryContextSwitchTo(slot->tts_per_tuple_mcxt);
    buffer = makeStringInfo();
    for (i = 0; i < attnum; i++) {
        int len;

        /* get size */
        rc = memcpy_s(&n32, sizeof(uint32), cur, 4);
        securec_check(rc, "\0", "\0");
        cur += 4;
        len = ntohl(n32);
        /* get data */
        if (len == -1) {
            slot->tts_values[i] = (Datum)0;
            slot->tts_isnull[i] = true;
        } else {
            appendBinaryStringInfo(buffer, cur, len);
            cur += len;
            if (att[i]->atttypid == ANYARRAYOID) {
                /* For anyarray, it need more information to handle it, so leave it, just copy */
                slot_extract_anyarray_from_buff(slot, i, buffer, len, need_transform_anyarray);
            } else {
                /* for abstimein, transfer str to time in select has some problem, so distinguish
                 * insert and select for ABSTIMEIN to avoid problem */
                t_thrd.time_cxt.is_abstimeout_in = true;

                slot->tts_values[i] = InputFunctionCall(slot->tts_attinmeta->attinfuncs + i, buffer->data,
                                                        slot->tts_attinmeta->attioparams[i],
                                                        slot->tts_attinmeta->atttypmods[i]);
                t_thrd.time_cxt.is_abstimeout_in = false;
            }
            slot->tts_isnull[i] = false;

            resetStringInfo(buffer);
        }
    }
    pfree(buffer->data);
    pfree(buffer);

    slot->tts_nvalid = attnum;

    MemoryContextSwitchTo(oldcontext);
}
#endif

/*
 * heap_slot_getattr
 *		This function fetches an attribute of the slot's current tuple.
 *		It is functionally equivalent to heap_getattr, but fetches of
 *		multiple attributes of the same tuple will be optimized better,
 *		because we avoid O(N^2) behavior from multiple calls of
 *		nocachegetattr(), even when attcacheoff isn't usable.
 *
 *		A difference from raw heap_getattr is that attnums beyond the
 *		slot's tupdesc's last attribute will be considered NULL even
 *		when the physical tuple is longer than the tupdesc.
 *
 *		@param slot: TableTuple slot from this attribute is extracted
 *		@param attnum: index of the atribute to be extracted.
 *		@param isnull: set to true, if the attribute is NULL.
 */
Datum heap_slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull, bool need_transform_anyarray)
{
    /* sanity checks */
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    HeapTupleHeader tup;

    /* XXXTAM: This will be moved into UHeapSlotGetAttr */
    /*
     * system attributes are handled by heap_getsysattr
     */
    if (attnum <= 0) {
        /* internal error */
        if (tuple == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract system attribute from virtual tuple")));
        }
        /* internal error */
        if (tuple == &(slot->tts_minhdr)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract system attribute from minimal tuple")));
        }
        return tableam_tops_getsysattr(tuple, attnum, tupleDesc, isnull);
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

#ifdef PGXC
    /* If it is a data row tuple extract all and return requested */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot, need_transform_anyarray);
        *isnull = slot->tts_isnull[attnum - 1];
        return slot->tts_values[attnum - 1];
    }
#endif

    /*
     * If the attribute's column has been dropped, we force a NULL result.
     * This case should not happen in normal use, but it could happen if we
     * are executing a plan cached before the column was dropped.
     */
    if (tupleDesc->attrs[attnum - 1]->attisdropped) {
        *isnull = true;
        return (Datum) 0;
    }

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    /* internal error */
    if (tuple == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /*
     * return NULL if attnum is out of range according to the tuple
     *
     * (We have to check this separately because of various inheritance and
     * table-alteration scenarios: the tuple could be either longer or shorter
     * than the tupdesc.)
     */
    tup = tuple->t_data;
    if (attnum > (int)HeapTupleHeaderGetNatts(tup, tupleDesc)) {
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
    if (HeapTupleHasNulls(tuple) && att_isnull(((unsigned int)(attnum - 1)), tup->t_bits)) {
            *isnull = true;
            return (Datum)0;
    }

    /*
     * Extract the attribute, along with any preceding attributes.
     */
    if (HEAP_TUPLE_IS_COMPRESSED(((HeapTuple)slot->tts_tuple)->t_data)) {
            slot_deform_cmprs_tuple(slot, attnum);
    } else {
            slot_deform_tuple(slot, attnum);
    }

    /*
     * The result is acquired from tts_values array.
     */
    *isnull = slot->tts_isnull[attnum - 1];
    return slot->tts_values[attnum - 1];
}

/*
 * heap_slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using heap_slot_getattr.
 *
 *		@param slot: TableTuple slot from this attributes are extracted
 */
void heap_slot_getallattrs(TupleTableSlot *slot, bool need_transform_anyarray)
{
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    int tdesc_natts = slot->tts_tupleDescriptor->natts;
    int attnum;
    HeapTuple tuple;

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid == tdesc_natts) {
        return;
    }

#ifdef PGXC
    /* Handle the DataRow tuple case */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot, need_transform_anyarray);
        return;
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    tuple = (HeapTuple)slot->tts_tuple;
    /* internal error */
    if (tuple == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /*
     * load up any slots available from physical tuple
     */
    attnum = HeapTupleHeaderGetNatts(tuple->t_data, slot->tts_tupleDescriptor);
    attnum = Min(attnum, tdesc_natts);

    if (HEAP_TUPLE_IS_COMPRESSED(((HeapTuple)slot->tts_tuple)->t_data)) {
        slot_deform_cmprs_tuple(slot, attnum);
    } else {
        slot_deform_tuple(slot, attnum);
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
    for (; attnum < tdesc_natts; attnum++) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: slot->tts_values[attnum] = (Datum) 0;
         * example code: slot->tts_isnull[attnum] = true;
         */
        slot->tts_values[attnum] = heapGetInitDefVal(attnum + 1, slot->tts_tupleDescriptor, &slot->tts_isnull[attnum]);
    }
    slot->tts_nvalid = tdesc_natts;
}

static inline int GetAttrNumber(TupleTableSlot* slot, int attnum)
{
    /* Check for caller error */
    if (attnum <= 0 || attnum > slot->tts_tupleDescriptor->natts) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("invalid attribute number %d", attnum)));
    }

    /* internal error */
    if (slot->tts_tuple == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot extract attribute from empty tuple slot")));
    }

    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    int attno = HeapTupleHeaderGetNatts(tuple->t_data, slot->tts_tupleDescriptor);
    attno = Min(attno, attnum);

    return attno;
}

void heap_slot_formbatch(TupleTableSlot* slot, VectorBatch* batch, int cur_rows, int attnum)
{
    int attno = GetAttrNumber(slot, attnum);

    slot_deform_batch(slot, batch, cur_rows, attno);

    /* If tuple doesn't have all the atts indicated by tupleDesc, read the rest as null */
    for (; attno < attnum; attno++) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: slot->tts_values[attno] = (Datum) 0;
         * example code: slot->tts_isnull[attno] = true;
         */
        ScalarVector* pVector = &batch->m_arr[attno];
        pVector->m_vals[cur_rows] = heapGetInitDefVal(attno + 1, slot->tts_tupleDescriptor, &slot->tts_isnull[attno]);
        if (slot->tts_isnull[attno]) {
            pVector->m_const = true;
            SET_NULL(pVector->m_flag[cur_rows]);
        } else {
            SET_NOTNULL(pVector->m_flag[cur_rows]);
        }
    }
}

/*
 * heap_slot_getsomeattrs
 *		This function forces the entries of the slot's Datum/isnull
 *		arrays to be valid at least up through the attnum'th entry.
 *
 *		@param slot:input Tuple Table slot from which attributes are extracted.
 *		@param attnum: index until which slots attributes are extracted.
 */
void heap_slot_getsomeattrs(TupleTableSlot *slot, int attnum)
{
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid >= attnum) {
        return;
    }

#ifdef PGXC
    /* Handle the DataRow tuple case */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot, false);
        return;
    }
#endif

    int attno = GetAttrNumber(slot, attnum);

    slot_deform_tuple(slot, attno);

    /* If tuple doesn't have all the atts indicated by tupleDesc, read the rest as null */
    for (; attno < attnum; attno++) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: slot->tts_values[attno] = (Datum) 0;
         * example code: slot->tts_isnull[attno] = true;
         */
        slot->tts_values[attno] = heapGetInitDefVal(attno + 1, slot->tts_tupleDescriptor, &slot->tts_isnull[attno]);
    }
    slot->tts_nvalid = attnum;
}

/*
 * heap_slot_attisnull
 *		Detect whether an attribute of the slot is null, without
 *		actually fetching it.
 *
 *	 @param slot: Tabletuple slot
 *	 @para attnum: attribute index that should be checked for null value.
 */
bool heap_slot_attisnull(TupleTableSlot *slot, int attnum)
{
    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;

    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    /*
     * system attributes are handled by heap_attisnull
     */

    // XXXTAM Needs to be revisited as part of slot re-factoring
    // We can then use tableam_tops_tuple_attisnull()
    if (attnum <= 0) {
            /* internal error */
            if (tuple == NULL) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot extract system attribute from virtual tuple")));
            }

            /* internal error */
            if (tuple == &(slot->tts_minhdr)) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot extract system attribute from minimal tuple")));
            }
            return heap_attisnull(tuple, attnum, NULL);
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

#ifdef PGXC
    /* If it is a data row tuple extract all and return requested */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot, false);
        return slot->tts_isnull[attnum - 1];
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    /* internal error */
    if (tuple == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /* and let the tuple tell it */
    /* get init default value from tupleDesc.
     * The original Code is:
     * return heap_attisnull(tuple, attnum, tupleDesc);
     */
    return relationAttIsNull(tuple, attnum, tupleDesc);
}

/*
 * heap_freetuple
 */
void heap_freetuple(HeapTuple htup)
{
    pfree(htup);
}

void check_column_num(int column_num)
{
    if (column_num > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
            errmsg("number of columns (%d) exceeds limit (%d)", column_num, MaxTupleAttributeNumber)));
    }
}

/*
 * heap_form_minimal_tuple
 *		construct a MinimalTuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * This is exactly like heap_form_tuple() except that the result is a
 * "minimal" tuple lacking a HeapTupleData header as well as room for system
 * columns.
 *
 * The result is allocated in the current memory context.
 */
MinimalTuple heap_form_minimal_tuple(TupleDesc tupleDescriptor, Datum *values, const bool *isnull, MinimalTuple inTuple)
{
    MinimalTuple tuple; /* return tuple */
    Size len, data_len;
    int hoff;
    bool hasnull = false;
    Form_pg_attribute *att = tupleDescriptor->attrs;
    int numberOfAttributes = tupleDescriptor->natts;
    int i;

    check_column_num(numberOfAttributes);
    /*
     * Check for nulls and embedded tuples; expand any toasted attributes in
     * embedded tuples.  This preserves the invariant that toasting can only
     * go one level deep.
     *
     * We can skip calling toast_flatten_tuple_attribute() if the attribute
     * couldn't possibly be of composite type.  All composite datums are
     * varlena and have alignment 'd'; furthermore they aren't arrays. Also,
     * if an attribute is already toasted, it must have been sent to disk
     * already and so cannot contain toasted attributes.
     */
    for (i = 0; i < numberOfAttributes; i++) {
        if (isnull[i]) {
            hasnull = true;
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
                   !VARATT_IS_EXTENDED(values[i])) {
            values[i] = toast_flatten_tuple_attribute(values[i], att[i]->atttypid, att[i]->atttypmod);
        } else if (att[i]->attlen == -1 && VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(values[i])) &&
            !(att[i]->atttypid == CLOBOID || att[i]->atttypid == BLOBOID)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("only suport type(clob/blob) for more than 1G toast")));
        }
    }

    /*
     * Determine total space needed
     */
    len = offsetof(MinimalTupleData, t_bits);

    if (hasnull) {
        len += BITMAPLEN(numberOfAttributes);
    }

    if (tupleDescriptor->tdhasoid) {
        len += sizeof(Oid);
    }

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

    len += data_len;

    Size allocSize = len;
    if (tupleDescriptor->tdhasuids) { /* prealloc 8 bytes */
        allocSize = MAXALIGN(hoff + sizeof(uint64)) + data_len;
    }

    /*
     * Allocate and zero the space needed.
     */
    if (inTuple == NULL) {
        tuple = (MinimalTuple)palloc0(allocSize);
    } else {
        if (inTuple->t_len < allocSize) {
            pfree(inTuple);
            inTuple = NULL;
            tuple = (MinimalTuple)palloc0(allocSize);
        } else {
            errno_t rc = memset_s(inTuple, inTuple->t_len, 0, inTuple->t_len);
            securec_check(rc, "\0", "\0");
            tuple = inTuple;
        }
    }
    /*
     * And fill in the information.
     */
    tuple->t_len = len;
    HeapTupleHeaderSetNatts(tuple, numberOfAttributes);
    tuple->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;
    /* else leave infomask = 0 */
    if (tupleDescriptor->tdhasoid) {
        tuple->t_infomask = HEAP_HASOID;
    }

    tuple->t_infomask &= ~HEAP_UID_MASK;

    heap_fill_tuple(tupleDescriptor, values, isnull, (char *)tuple + hoff, data_len, &tuple->t_infomask,
                    (hasnull ? tuple->t_bits : NULL));

    return tuple;
}

/*
 * heap_free_minimal_tuple
 */
void heap_free_minimal_tuple(MinimalTuple mtup)
{
    pfree(mtup);
}

/*
 * heap_copy_minimal_tuple
 *		copy a MinimalTuple
 *
 * The result is allocated in the current memory context.
 */
MinimalTuple heap_copy_minimal_tuple(MinimalTuple mtup)
{
    MinimalTuple result;
    errno_t rc = EOK;

    result = (MinimalTuple)palloc(mtup->t_len);
    rc = memcpy_s(result, mtup->t_len, mtup, mtup->t_len);
    securec_check(rc, "\0", "\0");
    return result;
}

/*
 * heap_tuple_from_minimal_tuple
 *		create a HeapTuple by copying from a MinimalTuple;
 *		system columns are filled with zeroes
 *
 * The result is allocated in the current memory context.
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 */
HeapTuple heap_tuple_from_minimal_tuple(MinimalTuple mtup)
{
    HeapTuple result;
    uint32 len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
    errno_t rc = EOK;

    result = (HeapTuple)heaptup_alloc(HEAPTUPLESIZE + len);
    result->t_len = len;
    ItemPointerSetInvalid(&(result->t_self));
    result->t_tableOid = InvalidOid;
    result->t_bucketId = InvalidBktId;
    HeapTupleSetZeroBase(result);
#ifdef PGXC
    result->t_xc_node_id = 0;
#endif
    result->t_data = (HeapTupleHeader)((char *)result + HEAPTUPLESIZE);
    rc = memcpy_s((char *)result->t_data + MINIMAL_TUPLE_OFFSET, mtup->t_len, mtup, mtup->t_len);
    securec_check(rc, "\0", "\0");
    rc = memset_s(result->t_data, offsetof(HeapTupleHeaderData, t_infomask2), 0,
                  offsetof(HeapTupleHeaderData, t_infomask2));
    securec_check(rc, "\0", "\0");
    return result;
}

/*
 * minimal_tuple_from_heap_tuple
 *		create a MinimalTuple by copying from a HeapTuple
 *
 * The result is allocated in the current memory context.
 */
MinimalTuple minimal_tuple_from_heap_tuple(HeapTuple htup)
{
    MinimalTuple result;
    uint32 len;
    errno_t rc = EOK;

    Assert(!HEAP_TUPLE_IS_COMPRESSED(htup->t_data));
    Assert(htup->t_len > MINIMAL_TUPLE_OFFSET);
    len = htup->t_len - MINIMAL_TUPLE_OFFSET;
    result = (MinimalTuple)palloc(len);
    rc = memcpy_s(result, len, (char *)htup->t_data + MINIMAL_TUPLE_OFFSET, len);
    securec_check(rc, "\0", "\0");
    result->t_len = len;
    return result;
}

/* ---------------------------------------------------------------------------
 *            Area for Compressing && Decompressing Tuple
 * ---------------------------------------------------------------------------
 */
/*
 * heap_compute_cmprs_data_size
 *		Determine size of the data area of a compressed tuple to be constructed
 */
static Size heap_compute_cmprs_data_size(TupleDesc tupleDesc, FormCmprTupleData *cmprsInfo)
{
    Size data_length = 0;
    int i;
    int numberOfAttributes = tupleDesc->natts;
    Form_pg_attribute *att = tupleDesc->attrs;
    if (numberOfAttributes > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS), errmsg("number of columns (%d) exceeds limit (%d)",
                                                                  numberOfAttributes, MaxTupleAttributeNumber)));
    }

    /*
     * Important:
     * (1) compression-bitmap will be placed between header and data part of tuple.
     * (2) When <data_length> is not 0, data position MUST be MAXALIGN,  which is 8 bytes, alligned!
     * (3) It's bytes-alligned factor that results in computing space in order, first compression-map,
     *     then fields step by step !
     */
    data_length += BITMAPLEN(numberOfAttributes);

    for (i = 0; i < numberOfAttributes; i++) {
        /* NULLs first: no compression, no storage. */
        if (cmprsInfo->isnulls[i]) {
            continue;
        }

        /* compression second: specail value, special size, and special alligned. */
        if (cmprsInfo->compressed[i]) {
            data_length += cmprsInfo->valsize[i];
            continue;
        }

        /* the normal field is the last */
        Datum val = cmprsInfo->values[i];

        if (ATT_IS_PACKABLE(att[i]) && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val))) {
            /*
             * we're anticipating converting to a short varlena header, so
             * adjust length and don't count any alignment
             */
            data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
        } else {
            data_length = att_align_datum(data_length, att[i]->attalign, att[i]->attlen, val);
            data_length = att_addlength_datum(data_length, att[i]->attlen, val);
        }
    }

    return data_length;
}

/* fill bitmap according to flags[nflag] */
static void heap_fill_bitmap(char *buf, const bool *flags, int nflag)
{
    char *byteBuf = (buf - 1);
    uint32 bitmask = HIGHBIT;
    int cnt;

    for (cnt = 0; cnt < nflag; cnt++) {
        if (bitmask != HIGHBIT) {
            bitmask <<= 1;
        } else {
            byteBuf++;
            *byteBuf = 0x00;
            bitmask = 1;
        }

        if (flags[cnt]) {
            continue;
        }

        /* bit=0 when flag is true;
         * bit=1 when flag is false;
         * please refer to isAttrCompressed macro. */
        *byteBuf |= bitmask;
    }
}

/*
 * heap_fill_cmprs_tuple
 *		Load data portion of a tuple from cmprsInfo data
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * At the other hand, we will fill the attrs compression-bitmap
 * according to cmprsInfo, which is placed in the front of <data> part.
 * And then the valid value in cmprsInfo->values[] is written
 * and mixed with attribute content.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 */
static void heap_fill_cmprs_tuple(TupleDesc tupleDesc, FormCmprTupleData *cmprsInfo, char *data, Size data_size,
                                  uint16 *infomask, bits8 *bit)
{
    bits8 *bitP = NULL;
    uint32 bitmask;
    int i;
    int numberOfAttributes = tupleDesc->natts;
    errno_t retno = EOK;
    Form_pg_attribute *att = tupleDesc->attrs;
    char *start = data;

    /* compression-bitmap MUST be put firstly.
     * refer to heap_compute_cmprs_data_size()
     */
    heap_fill_bitmap(data, cmprsInfo->compressed, numberOfAttributes);
    data += (int16)BITMAPLEN(numberOfAttributes);

    if (bit != NULL) {
        bitP = &bit[-1];
        bitmask = HIGHBIT;
    } else {
        /* just to keep compiler quiet */
        bitP = NULL;
        bitmask = 0;
    }

    *infomask &= ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL | HEAP_COMPRESSED);

    for (i = 0; i < numberOfAttributes; i++) {
        Size data_length;
        Size remian_length = data_size - (size_t)(data - start);

        /* NULLs first: no compression, no storage. */
        if (bit != NULL) {
            if (bitmask != HIGHBIT) {
                bitmask <<= 1;
            } else {
                bitP += 1;
                *bitP = 0x0;
                bitmask = 1;
            }

            if (cmprsInfo->isnulls[i]) {
                *infomask |= HEAP_HASNULL;
                continue;
            }

            *bitP |= bitmask;
        }

        /* compression second: specail value, special size, and special alligned. */
        if (cmprsInfo->compressed[i]) {
            /* Important: compressed value is passed by pointer (char*) */
            Assert(!cmprsInfo->isnulls[i]);
            if (cmprsInfo->valsize[i] != 0) {
                Assert(cmprsInfo->valsize[i] > 0);
                Pointer val = DatumGetPointer(cmprsInfo->values[i]);
                retno = memcpy_s(data, remian_length, val, cmprsInfo->valsize[i]);
                securec_check(retno, "\0", "\0");
                data += cmprsInfo->valsize[i];
            }
            *infomask |= HEAP_COMPRESSED;

            /* Important: we must keep setting <infomask> flag */
            if (att[i]->attlen < 0) {
                Assert((-1 == att[i]->attlen) || (-2 == att[i]->attlen));
                *infomask |= HEAP_HASVARWIDTH;
            }

            continue;
        }

        /*
         * XXX we use the att_align macros on the pointer value itself, not on
         * an offset.  This is a bit of a hack.
         */
        if (att[i]->attbyval) {
            /* pass-by-value */
            data = (char *)att_align_nominal(data, att[i]->attalign);
            store_att_byval(data, cmprsInfo->values[i], att[i]->attlen);
            data_length = att[i]->attlen;
        } else if (att[i]->attlen == -1) {
            /* varlena */
            Pointer val = DatumGetPointer(cmprsInfo->values[i]);

            *infomask |= HEAP_HASVARWIDTH;
            if (VARATT_IS_EXTERNAL(val)) {
                *infomask |= HEAP_HASEXTERNAL;
                /* no alignment, since it's short by definition */
                data_length = VARSIZE_EXTERNAL(val);
                retno = memcpy_s(data, remian_length, val, data_length);
                securec_check(retno, "\0", "\0");
            } else if (VARATT_IS_SHORT(val)) {
                /* no alignment for short varlenas */
                data_length = VARSIZE_SHORT(val);
                retno = memcpy_s(data, remian_length, val, data_length);
                securec_check(retno, "\0", "\0");
            } else if (VARLENA_ATT_IS_PACKABLE(att[i]) && VARATT_CAN_MAKE_SHORT(val)) {
                /* convert to short varlena -- no alignment */
                data_length = VARATT_CONVERTED_SHORT_SIZE(val);
                SET_VARSIZE_SHORT(data, data_length);
                retno = memcpy_s(data + 1, remian_length - 1, VARDATA(val), data_length - 1);
                securec_check(retno, "\0", "\0");
            } else {
                /* full 4-byte header varlena */
                data = (char *)att_align_nominal(data, att[i]->attalign);
                data_length = VARSIZE(val);
                retno = memcpy_s(data, remian_length, val, data_length);
                securec_check(retno, "\0", "\0");
            }
        } else if (att[i]->attlen == -2) {
            /* cstring ... never needs alignment */
            *infomask |= HEAP_HASVARWIDTH;
            Assert(att[i]->attalign == 'c');
            data_length = strlen(DatumGetCString(cmprsInfo->values[i])) + 1;
            retno = memcpy_s(data, remian_length, DatumGetPointer(cmprsInfo->values[i]), data_length);
            securec_check(retno, "\0", "\0");
        } else {
            /* fixed-length pass-by-reference */
            data = (char *)att_align_nominal(data, att[i]->attalign);
            Assert(att[i]->attlen > 0);
            data_length = att[i]->attlen;
            retno = memcpy_s(data, remian_length, DatumGetPointer(cmprsInfo->values[i]), data_length);
            securec_check(retno, "\0", "\0");
        }

        data += data_length;
    }

    Assert((size_t)(data - start) == data_size);
}

Datum nocache_cmprs_get_attr(HeapTuple tuple, unsigned int attnum, TupleDesc tupleDesc, char *cmprsInfo)
{
    HeapTupleHeader tup = tuple->t_data;
    Form_pg_attribute *att = tupleDesc->attrs;
    char *tp = NULL;         /* ptr to data part of tuple */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bits8 *cmprsBitmap = NULL;
    int off = 0; /* current offset within data */
    uint32 i = 0;

    Assert(HEAP_TUPLE_IS_COMPRESSED(tup) && (cmprsInfo != NULL));
    Assert(attnum <= HeapTupleHeaderGetNatts(tup, tupleDesc));
    Assert(tupleDesc->natts >= (int)HeapTupleHeaderGetNatts(tup, tupleDesc));

    attnum--;
    tp = (char *)tup + tup->t_hoff;
    cmprsBitmap = (bits8 *)tp;
    off = BITMAPLEN(HeapTupleHeaderGetNatts(tup, tupleDesc));

    int cmprsOff = 0; /* pointer to the start of compression meta */
    void *metaInfo = NULL;
    char mode = 0;

    for (i = 0;; i++) { /* loop exit is at "break" */
        Assert(i < HeapTupleHeaderGetNatts(tup, tupleDesc));

        /* first parse compression metaInfo data of this attr */
        int metaSize = 0;
        metaInfo = PageCompress::FetchAttrCmprMeta(cmprsInfo + cmprsOff, att[i]->attlen, &metaSize, &mode);
        cmprsOff += metaSize;

        if (HeapTupleHasNulls(tuple) && att_isnull(i, bp)) {
            continue; /* this cannot be the target att */
        }

        if (isAttrCompressed(i, cmprsBitmap)) {
            if (attnum != i) {
                off += PageCompress::GetAttrCmprValSize(mode, att[i]->attlen, metaInfo, tp + off);
                continue;
            }

            break;
        }

        if (att[i]->attlen == -1) {
            off = att_align_pointer((uint32)off, att[i]->attalign, -1, tp + off);
        } else {
            off = att_align_nominal((uint32)off, att[i]->attalign);
        }

        if (i == attnum) {
            break;
        }

        off = att_addlength_pointer(off, att[i]->attlen, tp + off);
    }

    Assert(attnum == i);
    if (isAttrCompressed(attnum, cmprsBitmap)) {
        int attsize = 0;
        Datum attr_val = PageCompress::UncompressOneAttr(mode, metaInfo, att[i]->atttypid, att[i]->attlen, tp + off,
                                                         &attsize);
        return attr_val;
    }
    return fetchatt(att[attnum], tp + off);
}

/*
 * HeapUncompressTup
 * 		Uncompress tuple into destTup
 * Note that toast tuple will not be compressed, so please be carefull.
 */
template <bool hasnulls>
static HeapTuple HeapUncompressTup(HeapTuple srcTuple, TupleDesc tupleDesc, char *cmprsInfo, HeapTuple destTuple)
{
    Assert(srcTuple && tupleDesc && cmprsInfo);

    HeapTupleHeader srcTup = srcTuple->t_data;
    Form_pg_attribute *att = tupleDesc->attrs;
    uint32 tdesc_natts = tupleDesc->natts;
    uint32 natts; /* number of atts to extract */
    uint32 attrIdx;
    char *srcTupData = NULL;             /* ptr to srcTuple data */
    long srcOff;                         /* offset in srcTuple data */
    bits8 *srcNullBits = srcTup->t_bits; /* ptr to null bitmap in srcTuple */
    bits8 *cmprsBitmap = NULL;           /* pointer to compression bitmap in srcTuple */
#ifdef USE_ASSERT_CHECKING
    uint16 testInfomask = tupleDesc->tdhasoid ? HEAP_HASOID : 0;
#endif

    Assert(HEAP_TUPLE_IS_COMPRESSED(srcTuple->t_data) && (cmprsInfo != NULL));

    /*
     * In inheritance situations, it is possible that the given srcTuple actually
     * has more fields than the caller is expecting.  Don't run srcOff the end of
     * the caller's arrays.
     */
    Assert(tdesc_natts >= HeapTupleHeaderGetNatts(srcTup, tupleDesc));
    natts = Min(HeapTupleHeaderGetNatts(srcTup, tupleDesc), tdesc_natts);
    srcTupData = (char *)srcTup + srcTup->t_hoff;
    cmprsBitmap = (bits8 *)srcTupData;
    srcOff = BITMAPLEN(natts);

    errno_t retno = EOK;
    int cmprsOff = 0; /* pointer to the start of compression meta */
    void *metaInfo = NULL;
    char mode = 0;

    /* Dest srcTuple header size */
    int hoff = srcTup->t_hoff;
    int destDataLength = 0;

    if (destTuple == NULL) {
        destTuple = (HeapTuple)heaptup_alloc(MaxHeapTupleSize + HEAPTUPLESIZE);
        destTuple->t_data = (HeapTupleHeader)((char *)destTuple + HEAPTUPLESIZE);
    }

    Assert(destTuple->t_data != NULL);
    HeapTupleHeader destTup = destTuple->t_data;
    Assert(((size_t)MAXALIGN(destTup)) == (size_t)destTup);

    bits8 *destNullBits = destTup->t_bits;
    char *destTupData = (char *)destTup + hoff;
    Datum val = 0;

    /* Copy null bitmap */
    if (hasnulls) {
        retno = memcpy_s(destNullBits, BITMAPLEN(natts), srcNullBits, BITMAPLEN(natts));
        securec_check(retno, "\0", "\0");
#ifdef USE_ASSERT_CHECKING
        testInfomask |= HEAP_HASNULL;
#endif
    }

    for (attrIdx = 0; attrIdx < natts; ++attrIdx) {
        Form_pg_attribute thisatt = att[attrIdx];
        /* parse compression metaInfo data of this attr */
        int metaSize = 0;
        metaInfo = PageCompress::FetchAttrCmprMeta(cmprsInfo + cmprsOff, thisatt->attlen, &metaSize, &mode);
        cmprsOff += metaSize;

        /* IMPORTANT: NULLs first, row-compression second, and the normal fields the last; */
        if (hasnulls && att_isnull(attrIdx, srcNullBits)) {
            continue;
        }

        if (isAttrCompressed(attrIdx, cmprsBitmap)) {
            int attsize = 0;

            if (PageCompress::NeedExternalBuf(mode)) {
#ifdef USE_ASSERT_CHECKING
                testInfomask |= HEAP_HASVARWIDTH;
#endif

                destDataLength = PageCompress::UncompressOneAttr(mode, thisatt->attalign, thisatt->attlen, metaInfo,
                                                                 srcTupData + srcOff, &attsize, destTupData);
                /* attsize is the size of compressed value. */
                srcOff = srcOff + attsize;
                /* both datum size and padding size are included in destDataLength. */
                Assert(destDataLength > 0);
                destTupData += destDataLength;
                continue;
            } else {
                val = PageCompress::UncompressOneAttr(mode, metaInfo, thisatt->atttypid, thisatt->attlen,
                                                      srcTupData + srcOff, &attsize);
                srcOff = srcOff + attsize; /* attsize is the size of compressed value. */
            }
        } else {
            if (thisatt->attlen == -1) {
                srcOff = att_align_pointer(srcOff, thisatt->attalign, -1, srcTupData + srcOff);
            } else {
                /* not varlena, so safe to use att_align_nominal */
                srcOff = att_align_nominal(srcOff, thisatt->attalign);
            }

            val = fetchatt(thisatt, srcTupData + srcOff);
            srcOff = att_addlength_pointer(srcOff, thisatt->attlen, srcTupData + srcOff);
        }

        /* Now we fill dest srcTuple with the val */
        if (thisatt->attbyval) {
            /* pass-by-value */
            destTupData = (char *)att_align_nominal(destTupData, thisatt->attalign);
            store_att_byval(destTupData, val, thisatt->attlen);
            destDataLength = thisatt->attlen;
        } else if (thisatt->attlen == -1) {
            /* varlena */
            Pointer tmpVal = DatumGetPointer(val);
#ifdef USE_ASSERT_CHECKING
            testInfomask |= HEAP_HASVARWIDTH;
#endif

            if (VARATT_IS_EXTERNAL(tmpVal)) {
                /* no alignment, since it's short by definition */
                destDataLength = VARSIZE_EXTERNAL(tmpVal);
                retno = memcpy_s(destTupData, destDataLength, tmpVal, destDataLength);
                securec_check(retno, "\0", "\0");
            } else if (VARATT_IS_SHORT(tmpVal)) {
                /* no alignment for short varlenas */
                destDataLength = VARSIZE_SHORT(tmpVal);
                retno = memcpy_s(destTupData, destDataLength, tmpVal, destDataLength);
                securec_check(retno, "\0", "\0");
            } else if (VARLENA_ATT_IS_PACKABLE(thisatt) && VARATT_CAN_MAKE_SHORT(tmpVal)) {
                /* convert to short varlena -- no alignment */
                destDataLength = VARATT_CONVERTED_SHORT_SIZE(tmpVal);
                SET_VARSIZE_SHORT(destTupData, destDataLength);
                retno = memcpy_s(destTupData + 1, destDataLength - 1, VARDATA(tmpVal), destDataLength - 1);
                securec_check(retno, "\0", "\0");
            } else {
                /*
                 * Memset padding bytes, because att_align_pointer will judge
                 * padding bytes whether zero. Please refer to att_align_pointer
                 */
                *destTupData = 0;

                /* full 4-byte header varlena */
                destTupData = (char *)att_align_nominal(destTupData, thisatt->attalign);
                destDataLength = VARSIZE(tmpVal);
                retno = memcpy_s(destTupData, destDataLength, tmpVal, destDataLength);
                securec_check(retno, "\0", "\0");
            }
        } else if (thisatt->attlen == -2) {
#ifdef USE_ASSERT_CHECKING
            testInfomask |= HEAP_HASVARWIDTH;
#endif
            Assert(thisatt->attalign == 'c');
            destDataLength = strlen(DatumGetCString(val)) + 1;
            retno = memcpy_s(destTupData, destDataLength, DatumGetPointer(val), destDataLength);
            securec_check(retno, "\0", "\0");
        } else {
            /* fixed-length pass-by-reference */
            destTupData = (char *)att_align_nominal(destTupData, thisatt->attalign);
            Assert(thisatt->attlen > 0);
            destDataLength = thisatt->attlen;
            retno = memcpy_s(destTupData, destDataLength, DatumGetPointer(val), destDataLength);
            securec_check(retno, "\0", "\0");
        }
        destTupData += destDataLength;
    }

    /* complete destTuple other info excluding t_data */
    destTuple->t_len = (uint32)(destTupData - (char *)destTup);
    destTuple->t_self = srcTuple->t_self;
    destTuple->t_tableOid = srcTuple->t_tableOid;
    destTuple->t_bucketId = srcTuple->t_bucketId;
#ifdef PGXC
    destTuple->t_xc_node_id = srcTuple->t_xc_node_id;
#endif

    /* complete destTup header info excluding data part */
    COPY_TUPLE_HEADERINFO(destTup, srcTup);
    HEAP_TUPLE_CLEAR_COMPRESSED(destTuple->t_data);
    Assert(testInfomask == (destTup->t_infomask & 0x0F));

    destTup->t_hoff = hoff;
    if (tupleDesc->tdhasoid) {
        HeapTupleHeaderSetOid(destTup, HeapTupleGetOid(srcTuple));
    }

    return destTuple;
}

/* the same to HeapUncompressTup() function which is a faster and
 * lighter-weight implement.
 * we highly recommand that you would not using HeapUncompressTup2()
 * only when alter-table-instant happens.
 */
static HeapTuple HeapUncompressTup2(HeapTuple tuple, TupleDesc tupleDesc, Page dictPage)
{
    Assert(HeapTupleIsValid(tuple) && (tuple->t_data != NULL));
    Assert(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));
    Assert((tupleDesc != NULL) && (dictPage != NULL));

    Datum *values = (Datum *)palloc(sizeof(Datum) * tupleDesc->natts);
    bool *isnulls = (bool *)palloc(sizeof(bool) * tupleDesc->natts);

    heap_deform_cmprs_tuple(tuple, tupleDesc, values, isnulls, dictPage);
    HeapTuple newTuple = heap_form_tuple(tupleDesc, values, isnulls);

    /* don't copy tuple->t_len, that has been set in heap_form_tuple */
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
    newTuple->t_bucketId = tuple->t_bucketId;
#ifdef PGXC
    newTuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    if (tupleDesc->tdhasoid) {
        HeapTupleSetOid(newTuple, HeapTupleGetOid(tuple));
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(newTuple->t_data));
    COPY_TUPLE_HEADER_XACT_INFO(newTuple, tuple);

    pfree_ext(isnulls);
    pfree_ext(values);
    return newTuple;
}

HeapTuple test_HeapUncompressTup2(HeapTuple tuple, TupleDesc tupleDesc, Page dictPage)
{
    return HeapUncompressTup2(tuple, tupleDesc, dictPage);
}

/*
 * heap_form_cmprs_tuple
 *		construct a compressed tuple from the given cmprsInfo.
 *
 * The result is allocated in the current memory context.
 */
HeapTuple heap_form_cmprs_tuple(TupleDesc tupleDescriptor, FormCmprTupleData *cmprsInfo)
{
    HeapTuple tuple;    /* return tuple */
    HeapTupleHeader td; /* tuple data */
    Size len, data_len;
    int hoff;
    bool hasnull = false;
#ifdef USE_ASSERT_CHECKING
    bool hascmpr = false;
#endif
    Form_pg_attribute *att = tupleDescriptor->attrs;
    int numberOfAttributes = tupleDescriptor->natts;
    int i;

    if (numberOfAttributes > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS), errmsg("number of columns (%d) exceeds limit (%d)",
                                                                  numberOfAttributes, MaxTupleAttributeNumber)));
    }

    /*
     * Check for nulls and embedded tuples; expand any toasted attributes in
     * embedded tuples.  This preserves the invariant that toasting can only
     * go one level deep.
     *
     * We can skip calling toast_flatten_tuple_attribute() if the attribute
     * couldn't possibly be of composite type.  All composite datums are
     * varlena and have alignment 'd'; furthermore they aren't arrays. Also,
     * if an attribute is already toasted, it must have been sent to disk
     * already and so cannot contain toasted attributes.
     */
    for (i = 0; i < numberOfAttributes; i++) {
        if (cmprsInfo->isnulls[i]) {
            hasnull = true;
            continue;
        }

        if (cmprsInfo->compressed[i]) {
#ifdef USE_ASSERT_CHECKING
            hascmpr = true;
#endif
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
                   !VARATT_IS_EXTENDED(DatumGetPointer(cmprsInfo->values[i]))) {
            cmprsInfo->values[i] = toast_flatten_tuple_attribute(cmprsInfo->values[i], att[i]->atttypid,
                                                                 att[i]->atttypmod);
        }
    }
    Assert(hascmpr == true);

    /*
     * Determine total space needed
     */
    len = offsetof(HeapTupleHeaderData, t_bits);

    if (hasnull) {
        len += BITMAPLEN(numberOfAttributes);
    }

    if (tupleDescriptor->tdhasoid) {
        len += sizeof(Oid);
    }

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_cmprs_data_size(tupleDescriptor, cmprsInfo);

    len += data_len;

    /*
     * Allocate and zero the space needed.	Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    Size allocSize = HEAPTUPLESIZE + len;
    if (tupleDescriptor->tdhasuids) { /* prealloc 8 bytes */
        allocSize = HEAPTUPLESIZE + MAXALIGN(hoff + sizeof(uint64)) + data_len;
    }
    tuple = (HeapTuple)heaptup_alloc(allocSize);
    tuple->t_data = td = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);

    /*
     * And fill in the information.  Note we fill the Datum fields even though
     * this tuple may never become a Datum.
     */
    tuple->t_len = len;
    ItemPointerSetInvalid(&(tuple->t_self));
    HeapTupleSetZeroBase(tuple);
    tuple->t_tableOid = InvalidOid;
    tuple->t_bucketId = InvalidBktId;
#ifdef PGXC
    tuple->t_xc_node_id = 0;
#endif

    HeapTupleHeaderSetDatumLength(td, len);
    HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);

    HeapTupleHeaderSetNatts(td, numberOfAttributes);
    td->t_hoff = hoff;

    td->t_infomask &= ~HEAP_UID_MASK;

    /* else leave infomask = 0 */
    if (tupleDescriptor->tdhasoid) {
        td->t_infomask = HEAP_HASOID;
    }

    heap_fill_cmprs_tuple(tupleDescriptor, cmprsInfo, (char *)td + hoff, data_len, &td->t_infomask,
                          (hasnull ? td->t_bits : NULL));
    Assert(HEAP_TUPLE_IS_COMPRESSED(td));
    Assert(numberOfAttributes == (int)HeapTupleHeaderGetNatts(td, tupleDescriptor));

    return tuple;
}

/*
 * heap_deform_cmprs_tuple
 *		Given a tuple, extract data into values/isnull arrays; this is
 *		the inverse of heap_form_cmprs_tuple.
 *
 *		Storage for the values/isnull arrays is provided by the caller;
 *		it should be sized according to tupleDesc->natts not
 *		HeapTupleHeaderGetNatts(tuple->t_data, tupleDesc).
 *
 *		Note that for pass-by-reference datatypes, the pointer placed
 *		in the Datum will point into the given tuple. This rule is both
 *		suitable for compressed and uncompressed tuples.
 *
 *		When all or most of a tuple's fields need to be extracted,
 *		this routine will be significantly quicker than a loop around
 *		heap_getattr; the loop will become O(N^2) as soon as any
 *		noncacheable attribute offsets are involved.
 */
void heap_deform_cmprs_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, char *cmprsInfo)
{
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    uint32 tdesc_natts = tupleDesc->natts;
    uint32 natts; /* number of atts to extract */
    uint32 attnum;
    char *tp = NULL;           /* ptr to tuple data */
    long off;                  /* offset in tuple data */
    bits8 *bp = tup->t_bits;   /* ptr to null bitmap in tuple */
    bits8 *cmprsBitmap = NULL; /* pointer to compression bitmap in tuple */

    Assert(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) && (cmprsInfo != NULL));

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = Min(HeapTupleHeaderGetNatts(tup, tupleDesc), tdesc_natts);
    tp = (char *)tup + tup->t_hoff;
    cmprsBitmap = (bits8 *)tp;
    off = BITMAPLEN(natts);
    if (natts > MaxTupleAttributeNumber) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
                        errmsg("number of columns (%u) exceeds limit (%d)", natts, MaxTupleAttributeNumber)));
    }

    int cmprsOff = 0; /* pointer to the start of compression meta */
    void *metaInfo = NULL;
    char mode = 0;

    for (attnum = 0; attnum < natts; attnum++) {
        /* parse compression metaInfo data of this attr */
        int metaSize = 0;
        metaInfo = PageCompress::FetchAttrCmprMeta(cmprsInfo + cmprsOff, att[attnum]->attlen, &metaSize, &mode);
        cmprsOff += metaSize;

        /* IMPORTANT: NULLs first, row-compression second, and the normal fields the last; */
        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            continue;
        }

        isnull[attnum] = false;

        if (isAttrCompressed(attnum, cmprsBitmap)) {
            int attsize = 0;
            values[attnum] = PageCompress::UncompressOneAttr(mode, metaInfo, att[attnum]->atttypid, att[attnum]->attlen,
                                                             tp + off, &attsize);
            off = off + attsize; /* attsize is the size of compressed value. */

            continue;
        }

        Form_pg_attribute thisatt = att[attnum];
        if (thisatt->attlen == -1) {
            off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
        } else {
            /* not varlena, so safe to use att_align_nominal */
            off = att_align_nominal(off, thisatt->attalign);
        }

        values[attnum] = fetchatt(thisatt, tp + off);
        off = att_addlength_pointer(off, thisatt->attlen, tp + off);
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
    for (; attnum < tdesc_natts; attnum++) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: values[attnum] = (Datum) 0;
         * example code: isnull[attnum] = true;
         */
        values[attnum] = heapGetInitDefVal(attnum + 1, tupleDesc, &isnull[attnum]);
    }
}

void heap_deform_tuple2(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Buffer buffer)
{
    Assert((tuple != NULL) && (tuple->t_data != NULL));
    if (!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data)) {
        heap_deform_tuple(tuple, tupleDesc, values, isnull);
        return;
    }

    Assert(BufferIsValid(buffer));
    Page page = BufferGetPage(buffer);
    Assert((page != NULL) && (PageIsCompressed(page)));
    heap_deform_cmprs_tuple(tuple, tupleDesc, values, isnull, (char *)getPageDict(page));
}

/* deform the passed heap tuple.
 * call heap_deform_tuple() if it's not compressed,
 * otherwise call heap_deform_cmprs_tuple().
 */
void heap_deform_tuple3(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Page page)
{
    Assert((tuple != NULL) && (tuple->t_data != NULL));
    if (!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data)) {
        heap_deform_tuple(tuple, tupleDesc, values, isnull);
        return;
    }

    Assert((page != NULL) && (PageIsCompressed(page)));
    heap_deform_cmprs_tuple(tuple, tupleDesc, values, isnull, (char *)getPageDict(page));
}

/* decompress one tuple and return a copy of uncompressed tuple */
HeapTuple heapCopyCompressedTuple(HeapTuple tuple, TupleDesc tupleDesc, Page page, HeapTuple destTup)
{
    HeapTuple newTuple = NULL;
    Assert(HeapTupleIsValid(tuple) && (tuple->t_data != NULL));
    Assert(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));
    Assert((tupleDesc != NULL) && (page != NULL));
    Assert(!PageIsEncrypt(page));

    /* we have to handle alter-table-instant case, because
     * HeapUncompressTup() don't think about that case
     * and now is difficult to handle this problem.
     */
    if (tupleDesc->initdefvals && tupleDesc->natts > (int)HeapTupleHeaderGetNatts(tuple->t_data, tupleDesc)) {
        newTuple = HeapUncompressTup2(tuple, tupleDesc, (Page)getPageDict(page));
        if (destTup) {
            errno_t retno = EOK;
            Assert(MAXALIGN(newTuple->t_len) <= MaxHeapTupleSize);

            /* copy the new tuple into existing space. */
            destTup->t_len = newTuple->t_len;
            destTup->t_self = newTuple->t_self;
            destTup->t_tableOid = newTuple->t_tableOid;
            destTup->t_bucketId = newTuple->t_bucketId;
            destTup->t_xc_node_id = newTuple->t_xc_node_id;
            retno = memcpy_s(destTup->t_data, destTup->t_len, newTuple->t_data, newTuple->t_len);
            securec_check(retno, "\0", "\0");

            /* release unused space and make *newTuple* point to *destTup*. */
            heap_freetuple(newTuple);
            newTuple = destTup;
        }
    } else {
        if (!HeapTupleHasNulls(tuple)) {
            newTuple = HeapUncompressTup<false>(tuple, tupleDesc, (char *)getPageDict(page), destTup);
        } else {
            newTuple = HeapUncompressTup<true>(tuple, tupleDesc, (char *)getPageDict(page), destTup);
        }
    }

    return newTuple;
}

/* copy new tuple from given tuple, and fill the
 * default value for all the new and added attrubutes.
 */
static HeapTuple HeapCopyInitdefvalTup(HeapTuple tuple, TupleDesc tupDesc)
{
    /* malloc and set doReplace[] to be false. */
    bool *doReplace = (bool *)palloc0(tupDesc->natts * sizeof(bool));

    /* rebuild heapTuple */
    HeapTuple newTuple = heap_modify_tuple(tuple, tupDesc, NULL, NULL, doReplace);
    COPY_TUPLE_HEADER_XACT_INFO(newTuple, tuple);

    pfree_ext(doReplace);
    return newTuple;
}

/* ---------------------------------------------------------------------
 *		heapCopyTuple
 *
 *		wrapper for heap_copytuple && heapCopyCompressedTuple
 *
 * ---------------------------------------------------------------------
 */
FORCE_INLINE HeapTuple heapCopyTuple(HeapTuple tuple, TupleDesc tupDesc, Page page)
{
    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          (errmsg("tuple copy failed, because tuple is invalid or tuple data is null "))));
        return NULL;
    }

    if (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data)) {
        return heapCopyCompressedTuple(tuple, tupDesc, page);
    }

    if (unlikely(tupDesc->initdefvals && tupDesc->natts > (int)HeapTupleHeaderGetNatts(tuple->t_data, tupDesc))) {
        return HeapCopyInitdefvalTup(tuple, tupDesc);
    }

    return heap_copytuple(tuple);
}

/*
 * heapFormMinimalTuple
 *		create a MinimalTuple by getting values from a HeapTuple
 *
 * The result is allocated in the current memory context.
 * wrapper for uncompressed/compressed tuple.
 */
MinimalTuple heapFormMinimalTuple(HeapTuple tuple, TupleDesc tupleDesc, Page page)
{
    bool tupIsCompressed = HEAP_TUPLE_IS_COMPRESSED(tuple->t_data);
    Assert(!tupIsCompressed || (page != NULL));

    /* we cannot copy heap tuple directly when
     * 1. the tuple is compressed, or
     * 2. the relation has added column and this
     *    column has default value by expression.
     * so deform the tuple and form a new one.
     */
    if (tupIsCompressed ||
        (tupleDesc->initdefvals != NULL && tupleDesc->natts > (int)HeapTupleHeaderGetNatts(tuple->t_data, tupleDesc))) {
        Datum *values = (Datum *)palloc(tupleDesc->natts * sizeof(Datum));
        bool *isNull = (bool *)palloc(tupleDesc->natts * sizeof(bool));

        heap_deform_tuple3(tuple, tupleDesc, values, isNull, (tupIsCompressed ? page : NULL));
        MinimalTuple result = heap_form_minimal_tuple(tupleDesc, values, isNull);

        pfree_ext(values);
        pfree_ext(isNull);
        return result;
    }

    /* copy heap tuple directly. */
    return minimal_tuple_from_heap_tuple(tuple);
}

/* a copy of slot_deform_tuple for compressied tuple */
static void slot_deform_cmprs_tuple(TupleTableSlot *slot, uint32 natts)
{
    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    Datum *values = slot->tts_values;
    bool *isnull = slot->tts_isnull;
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute *att = tupleDesc->attrs;
    uint32 attnum;
    char *tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */

    bits8 *cmprsBitmap = NULL;
    char *cmprsInfo = NULL;
    void *metaInfo = NULL;
    int cmprsOff = 0;
    char mode = 0;

    Assert(HEAP_TUPLE_IS_COMPRESSED(tup));
    Assert(BufferIsValid(slot->tts_buffer));

    if (slot->tts_per_tuple_mcxt == NULL) {
        slot->tts_per_tuple_mcxt = AllocSetContextCreate(slot->tts_mcxt, "SlotPerTupleMcxt", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }

    AutoContextSwitch memContextGuard(slot->tts_per_tuple_mcxt);

    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    attnum = slot->tts_nvalid;
    if (attnum == 0) {
        /* Start from the first attribute */
        Assert(tupleDesc->natts >= (int)HeapTupleHeaderGetNatts(tup, tupleDesc));
        off = BITMAPLEN(HeapTupleHeaderGetNatts(tup, tupleDesc));
        cmprsOff = 0;
    } else {
        /* Restore state from previous execution */
        off = slot->tts_off;
        cmprsOff = slot->tts_meta_off;
    }

    tp = (char *)tup + tup->t_hoff;
    cmprsBitmap = (bits8 *)tp;

    Page page = BufferGetPage(slot->tts_buffer);
    Assert((page != NULL) && PageIsCompressed(page));
    cmprsInfo = (char *)getPageDict(page);

    /* parse compression metaInfo data of this attr */
    int metaSize = 0;
    for (; attnum < natts; attnum++) {
        metaInfo = PageCompress::FetchAttrCmprMeta(cmprsInfo + cmprsOff, att[attnum]->attlen, &metaSize, &mode);
        cmprsOff += metaSize;

        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            continue;
        }

        isnull[attnum] = false;

        if (isAttrCompressed(attnum, cmprsBitmap)) {
            int attsize = 0;
            values[attnum] = PageCompress::UncompressOneAttr(mode, metaInfo, att[attnum]->atttypid, att[attnum]->attlen,
                                                             tp + off, &attsize);
            off = off + attsize; /* attsize is the size of compressed value. */

            continue;
        }

        Form_pg_attribute thisatt = att[attnum];
        if (thisatt->attlen == -1) {
            off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
        } else {
            /* not varlena, so safe to use att_align_nominal */
            off = att_align_nominal(off, thisatt->attalign);
        }

        values[attnum] = fetchatt(thisatt, tp + off);
        off = att_addlength_pointer(off, thisatt->attlen, tp + off);
    }

    /*
     * Save state for next execution
     */
    slot->tts_nvalid = attnum;
    slot->tts_off = off;
    slot->tts_meta_off = cmprsOff;
    slot->tts_slow = true;
}

/*
 * Clears the contents of the table slot that contains heap table tuple data.
 */
void heap_slot_clear(TupleTableSlot *slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree) {
        heap_freetuple((HeapTuple)slot->tts_tuple);
        slot->tts_tuple = NULL;
        slot->tts_shouldFree = false;
    }

    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
        slot->tts_shouldFreeMin = false;
    }
}

/*
 * Make the contents of the heap table's slot contents solely depend on the slot(make them a local copy),
 *  and not on underlying external resources like another memory context, buffers etc.
 *
 * @pram slot: slot to be materialized.
 */
void heap_slot_materialize(TupleTableSlot *slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * If we have a regular physical tuple, and it's locally palloc'd, we have
     * nothing to do.
     */
    if (slot->tts_tuple && slot->tts_shouldFree && !HEAP_TUPLE_IS_COMPRESSED(((HeapTuple)slot->tts_tuple)->t_data))
        return ;

    /*
     * Otherwise, copy or build a physical tuple, and store it into the slot.
     *
     * We may be called in a context that is shorter-lived than the tuple
     * slot, but we have to ensure that the materialized tuple will survive
     * anyway.
     */
    MemoryContext old_context = MemoryContextSwitchTo(slot->tts_mcxt);
    slot->tts_tuple = heap_slot_copy_heap_tuple(slot);
    slot->tts_shouldFree = true;
    MemoryContextSwitchTo(old_context);

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer)) {
        ReleaseBuffer(slot->tts_buffer);
    }
    slot->tts_buffer = InvalidBuffer;

    /*
     * Mark extracted state invalid.  This is important because the slot is
     * not supposed to depend any more on the previous external data; we
     * mustn't leave any dangling pass-by-reference datums in tts_values.
     * However, we have not actually invalidated any such datums, if there
     * happen to be any previously fetched from the slot.  (Note in particular
     * that we have not pfree'd tts_mintuple, if there is one.)
     */
    slot->tts_nvalid = 0;

    /*
     * On the same principle of not depending on previous remote storage,
     * forget the mintuple if it's not local storage.  (If it is local
     * storage, we must not pfree it now, since callers might have already
     * fetched datum pointers referencing it.)
     */
    if (!slot->tts_shouldFreeMin) {
        slot->tts_mintuple = NULL;
    }
#ifdef PGXC
    if (!slot->tts_shouldFreeRow) {
        slot->tts_dataRow = NULL;
        slot->tts_dataLen = -1;
    }
#endif
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
MinimalTuple heap_slot_get_minimal_tuple(TupleTableSlot *slot) {
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

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
    MemoryContext old_context = MemoryContextSwitchTo(slot->tts_mcxt);
    slot->tts_mintuple = heap_slot_copy_minimal_tuple(slot);
    slot->tts_shouldFreeMin = true;
    MemoryContextSwitchTo(old_context);

    /*
     * Note: we may now have a situation where we have a local minimal tuple
     * attached to a virtual or non-local physical tuple.  There seems no harm
     * in that at the moment, but if any materializes, we should change this
     * function to force the slot into minimal-tuple-only state.
     */
    return slot->tts_mintuple;
}

/*
 * Return a copy of heap table minimal tuple representing the contents of the slot.
 * The copy needs to be palloc'd in the current memory context. The slot
 * itself is expected to remain unaffected. It is *not* expected to have
 * meaningful "system columns" in the copy. The copy is not be "owned" by
 * the slot i.e. the caller has to take responsibility to free memory
 * consumed by the slot.
 *
 * @param slot: slot from which minimal tuple to be copied.
 * @return slot's tuple minimal tuple copy
 */
MinimalTuple heap_slot_copy_minimal_tuple(TupleTableSlot *slot)
{
    /*
     * sanity checks.
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    /*
     * If we have a physical tuple then just copy it.  Prefer to copy
     * tts_mintuple since that's a tad cheaper.
     */
    if (slot->tts_mintuple) {
        return heap_copy_minimal_tuple(slot->tts_mintuple);
    } else if (slot->tts_tuple != NULL ) {
        return heapFormMinimalTuple((HeapTuple)slot->tts_tuple,
                slot->tts_tupleDescriptor,
                (BufferIsValid(slot->tts_buffer) ? BufferGetPage(slot->tts_buffer) : NULL));
    }

#ifdef PGXC

/*
 * Ensure values are extracted from data row to the Datum array
 */
    if (slot->tts_dataRow != NULL) {
        heap_slot_getallattrs(slot);
    }

#endif

    /*
     * Otherwise we need to build the heaps minimum tuple from the Datum array.
     */
    return heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
}

/*
 * Stores heaps minimal tuple in the TupleTableSlot. Release the current slots buffer and Free's any slot's
 * minimal and heap tuple.
 *
 * @param mtup: minimal tuple to be stored.
 * @param slot: slot to store tuple.
 * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple.
 */
void heap_slot_store_minimal_tuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    /*
     * sanity checks
     */
    Assert(mtup != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_tupslotTableAm == TAM_HEAP);

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree && (HeapTuple)slot->tts_tuple != NULL) {
        heap_freetuple((HeapTuple)slot->tts_tuple);
        slot->tts_tuple = NULL;
    }
    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
    }

#ifdef PGXC
if (slot->tts_shouldFreeRow) {
    pfree_ext(slot->tts_dataRow);
}
slot->tts_shouldFreeRow = false;
slot->tts_dataRow = NULL;
slot->tts_dataLen = -1;
#endif

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
    slot->tts_minhdr.t_data = (HeapTupleHeader)((char*)mtup - MINIMAL_TUPLE_OFFSET);

    /* no need to set t_self or t_tableOid since we won't allow access */
    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;
}

/*
 * Returns a heap tuple "owned" by the slot. It is the slot's responsibility to free the memory
 * associated with this tuple. If the slot cannot own the tuple constructed or returned, it should
 * not implement this method, and should return NULL.
 *
 * @param slot: slot from tuple to fetch.
 * @return slot's tuple.
 */
HeapTuple heap_slot_get_heap_tuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * If we have a regular physical tuple then just return it.
     */
    if (TTS_HAS_PHYSICAL_TUPLE(slot)) {
        return (HeapTuple)slot->tts_tuple;
    }
    /*
     * Otherwise materialize the slot...
     */
    heap_slot_materialize(slot);

    return (HeapTuple)slot->tts_tuple;
}

/*
 * Return a copy of heap tuple representing the contents of the slot. The
 * copy needs to be palloc'd in the current memory context. The slot
 * itself is expected to remain unaffected. It is *not* expected to have
 * meaningful "system columns" in the copy. The copy is not be "owned" by
 * the slot i.e. the caller has to take responsibility to free memory
 * consumed by the slot.
 *
 * @param slot: slot from which tuple to be copied.
 * @return slot's tuple copy
 */
HeapTuple heap_slot_copy_heap_tuple(TupleTableSlot *slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * If we have a physical tuple (either format) then just copy it.
     */
    if (TTS_HAS_PHYSICAL_TUPLE(slot)) {
        return heapCopyTuple((HeapTuple)slot->tts_tuple,
            slot->tts_tupleDescriptor,
            NULL);
    }
    if (slot->tts_mintuple != NULL) {
        return heap_tuple_from_minimal_tuple(slot->tts_mintuple);
    }
#ifdef PGXC
    /*
     * Ensure values are extracted from data row to the Datum array
     */
    if (slot->tts_dataRow != NULL) {
        heap_slot_getallattrs(slot);
    }
#endif
    /*
     * Otherwise we need to build a tuple from the Datum array.
     */
    return heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
}

/*
 * Stores heaps physical tuple in the TupleTableSlot. Release the current slots buffer and Free's any slot's
 * minimal and heap tuple.
 *
 * @param tuple: tuple to be stored.
 * @param slot: slot to store tuple.
 * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple.
 */
void heap_slot_store_heap_tuple(HeapTuple tuple, TupleTableSlot* slot, Buffer buffer, bool should_free, bool batchMode)
{
    /*
     * sanity checks
     */
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    /* passing shouldFree=true for a tuple on a disk page is not sane */
    Assert(BufferIsValid(buffer) ? (!should_free) : true);
    Assert(!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) || BufferIsValid(buffer));

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree && (HeapTuple)slot->tts_tuple != NULL) {
        heap_freetuple((HeapTuple)slot->tts_tuple);
        slot->tts_tuple = NULL;
    }
    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
    }
#ifdef ENABLE_MULTIPLE_NODES
#ifdef PGXC
    if (slot->tts_shouldFreeRow) {
        pfree_ext(slot->tts_dataRow);
    }
    slot->tts_shouldFreeRow = false;
    slot->tts_dataRow = NULL;
    slot->tts_dataLen = -1;

    /* Batch Mode only first tuple need reset context */
    if (!batchMode) {
        /*
         * Row uncompression use slot->tts_per_tuple_mcxt in some case, So we need reset memory context.
         * this memory context is introduced by PGXC and it only used in function 'slot_deform_datarow'.
         * PGXC also do reset in function 'FetchTuple'. So it is safe
         */
        ResetSlotPerTupleContext(slot);
    }
#endif
#endif

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = should_free;
    slot->tts_shouldFreeMin = false;
    slot->tts_tuple = tuple;
    slot->tts_mintuple = NULL;

    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;

    /*
     * If tuple is on a disk page, keep the page pinned as long as we hold a
     * pointer into it.  We assume the caller already has such a pin.
     *
     * This is coded to optimize the case where the slot previously held a
     * tuple on the same disk page: in that case releasing and re-acquiring
     * the pin is a waste of cycles.  This is a common situation during
     * seqscans, so it's worth troubling over.
     *
     * Batch Mode only first tuple need do buffer reference.
     */
    if (!batchMode && slot->tts_buffer != buffer) {
        if (BufferIsValid(slot->tts_buffer)) {
            ReleaseBuffer(slot->tts_buffer);
        }
        slot->tts_buffer = buffer;
        if (BufferIsValid(buffer)) {
            IncrBufferRefCount(buffer);
        }
    }
}

/*
 * Checks whether a dead tuple can be retained
 *
 * Note: Only the dead tuple of pg_partition needs to be verified in the current code.
 */
bool HeapKeepInvisibleTuple(HeapTuple tuple, TupleDesc tupleDesc, KeepInvisbleTupleFunc checkKeepFunc)
{
    static KeepInvisbleOpt keepInvisibleArray[] = {
        {PartitionRelationId, Anum_pg_partition_parttype, PartitionLocalIndexSkipping},
        {PartitionRelationId, Anum_pg_partition_reloptions, PartitionInvisibleMetadataKeep},
        {PartitionRelationId, Anum_pg_partition_parentid, PartitionParentOidIsLive}};

    bool ret = true;
    for (int i = 0; i < (int)lengthof(keepInvisibleArray); i++) {
        bool isNull = false;
        KeepInvisbleOpt keepOpt = keepInvisibleArray[i];

        if (keepOpt.tableOid != tuple->t_tableOid || !ret) {
            return false;
        }

        Datum checkDatum = fastgetattr(tuple, keepOpt.checkAttnum, tupleDesc, &isNull);
        if (isNull) {
            return false;
        }

        if (checkKeepFunc != NULL) {
            ret &= checkKeepFunc(checkDatum);
        } else if (keepOpt.checkKeepFunc != NULL) {
            ret &= keepOpt.checkKeepFunc(checkDatum);
        } else {
            return false;
        }
    }

    return ret;
}

void HeapCopyTupleNoAlloc(HeapTuple dest, HeapTuple src)
{
    if (!HeapTupleIsValid(src) || src->t_data == NULL) {
        return;
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(src->t_data));

    Assert(dest && dest->t_data);
    dest->t_len = src->t_len;
    dest->t_self = src->t_self;
    dest->t_tableOid = src->t_tableOid;
    dest->t_bucketId = src->t_bucketId;
    dest->t_xc_node_id = src->t_xc_node_id;
    HeapTupleCopyBase(dest, src);

    errno_t errorNo = memcpy_s((char *) dest->t_data, src->t_len, (char *) src->t_data, src->t_len);
    securec_check(errorNo, "\0", "\0");
}

uint64 HeapTupleGetUid(HeapTuple tup)
{
    HeapTupleHeader tupHeader = tup->t_data;
    if (!HeapTupleHeaderHasUid(tupHeader)) {
        return 0;
    }
    return *((uint64*)((char*)(tupHeader) + tupHeader->t_hoff - sizeof(uint64)));
}
void HeapTupleSetUid(HeapTuple tup, uint64 uid, int nattrs)
{
    /* catalog table not supportted uids */
    Assert(!(tup->t_data->t_infomask & HEAP_HASOID));
    int uidLen = GetUidByteLen(uid);
    errno_t rc = 0;
    Size len = offsetof(HeapTupleHeaderData, t_bits);
    Size data_len = tup->t_len - tup->t_data->t_hoff;
    len += HeapTupleHasNulls(tup) ? BITMAPLEN(nattrs) : 0;
    int hoff = MAXALIGN(len + uidLen);
    rc = memmove_s((char*)tup->t_data + hoff, data_len, (char*)tup->t_data + tup->t_data->t_hoff, data_len);
    securec_check(rc, "", "");
    tup->t_data->t_hoff = hoff;
    tup->t_data->t_infomask |= GetUidByteLenInfomask(uid);
    tup->t_len = hoff + data_len;
    HeapTupleHeaderSetDatumLength(tup->t_data, hoff + data_len);
    HeapTupleHeaderSetUid(tup->t_data, uid, uidLen);
}

