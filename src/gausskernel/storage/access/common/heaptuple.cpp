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
 * (Postgres actually has always zeroed them, but now it's required!)  Since
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
#include "catalog/pg_proc.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "storage/pagecompress.h"
#include "utils/memutils.h"
#include "utils/elog.h"

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
Size heap_compute_data_size(TupleDesc tuple_desc, Datum* values, const bool* isnull)
{
    Size data_length = 0;
    int i;
    int attribute_num = tuple_desc->natts;
    Form_pg_attribute* att = tuple_desc->attrs;

    for (i = 0; i < attribute_num; i++) {
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
void heap_fill_tuple(
    TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit)
{
    bits8* bitP = NULL;
    uint32 bitmask;
    int i;
    int attribute_num = tuple_desc->natts;
    Form_pg_attribute* att = tuple_desc->attrs;
    errno_t rc = EOK;
    char* begin = data;

#ifdef USE_ASSERT_CHECKING
    char* start = data;
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

    for (i = 0; i < attribute_num; i++) {
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
            data = (char*)att_align_nominal(data, att[i]->attalign);
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
                data = (char*)att_align_nominal(data, att[i]->attalign);
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
            data = (char*)att_align_nominal(data, att[i]->attalign);
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
bool heap_attisnull(HeapTuple tup, int attnum, TupleDesc tup_desc)
{
    if (attnum > (int)HeapTupleHeaderGetNatts(tup->t_data, tup_desc)) {
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
Datum heapGetInitDefVal(int att_num, TupleDesc tuple_desc, bool* is_null)
{
    *is_null = true;

    if (tuple_desc->initdefvals != NULL) {
        *is_null = tuple_desc->initdefvals[att_num - 1].isNull;
        if (!(*is_null)) {
            return fetchatt(tuple_desc->attrs[att_num - 1], tuple_desc->initdefvals[att_num - 1].datum);
        }
    }

    return (Datum)0;
}

/* Another version of heap_attisnull. Think about attinitdefval of pg_attribute.
 * The function is for ordinary relation table, not for system table such as pg_class etc,
 * because attribute number of tuple is equal to that of tupledesc for ever.
 */
bool relationAttIsNull(HeapTuple tup, int att_num, TupleDesc tuple_desc)
{
    if (att_num > (int)HeapTupleHeaderGetNatts(tup->t_data, tuple_desc)) {
        return (tuple_desc->initdefvals == NULL) ? true : tuple_desc->initdefvals[att_num - 1].isNull;
    }
    return heap_attisnull(tup, att_num, tuple_desc);
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
Datum nocachegetattr(HeapTuple tuple, uint32 attnum, TupleDesc tuple_desc)
{
    HeapTupleHeader tup = tuple->t_data;
    Form_pg_attribute* att = tuple_desc->attrs;
    char* tp = NULL;         /* ptr to data part of tuple */
    bits8* bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* do we have to walk attrs? */
    int off;                 /* current offset within data */

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
    Assert(attnum <= HeapTupleHeaderGetNatts(tup, tuple_desc));
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

    tp = (char*)tup + tup->t_hoff;

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
        uint32 natts = tuple_desc->natts;
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
        bool usecache = true;
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
            Assert(i < (uint32)tuple_desc->natts);
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
Datum heap_getsysattr(HeapTuple tup, int attnum, TupleDesc tuple_desc, bool* isnull)
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
 * heap_copytuple is used for tuples uncompressed, For all Postgresql System Relations,
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
    HeapTuple new_tuple;
    errno_t rc = EOK;

    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL) {
        return NULL;
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));
    new_tuple = (HeapTuple)palloc(HEAPTUPLESIZE + tuple->t_len);
    new_tuple->t_len = tuple->t_len;
    new_tuple->t_self = tuple->t_self;
    new_tuple->t_tableOid = tuple->t_tableOid;
    new_tuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(new_tuple, tuple);
#ifdef PGXC
    new_tuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    new_tuple->t_data = (HeapTupleHeader)((char*)new_tuple + HEAPTUPLESIZE);
    rc = memcpy_s((char*)new_tuple->t_data, tuple->t_len, (char*)tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    return new_tuple;
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
    rc = memcpy_s((char*)dest->t_data, src->t_len, (char*)src->t_data, src->t_len);
    securec_check(rc, "\0", "\0");
}

/*
 * heap_form_tuple
 *		construct a tuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 */
HeapTuple heap_form_tuple(TupleDesc tuple_descriptor, Datum* values, bool* isnull)
{
    HeapTuple tuple;    /* return tuple */
    HeapTupleHeader td; /* tuple data */
    Size len, data_len;
    int hoff;
    bool hasnull = false;
    Form_pg_attribute* att = tuple_descriptor->attrs;
    int attribute_num = tuple_descriptor->natts;
    int i;

    if (attribute_num > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of columns (%d) exceeds limit (%d)", attribute_num, MaxTupleAttributeNumber)));
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
    for (i = 0; i < attribute_num; i++) {
        if (isnull[i]) {
            hasnull = true;
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
            !VARATT_IS_EXTENDED(DatumGetPointer(values[i]))) {
            values[i] = toast_flatten_tuple_attribute(values[i], att[i]->atttypid, att[i]->atttypmod);
        }
    }

    /*
     * Determine total space needed
     */
    len = offsetof(HeapTupleHeaderData, t_bits);

    if (hasnull) {
        len += BITMAPLEN(attribute_num);
    }

    if (tuple_descriptor->tdhasoid) {
        len += sizeof(Oid);
    }

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_data_size(tuple_descriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.	Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    tuple = (HeapTuple)palloc0(HEAPTUPLESIZE + len);
    tuple->t_data = td = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);

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
    HeapTupleHeaderSetTypeId(td, tuple_descriptor->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tuple_descriptor->tdtypmod);

    HeapTupleHeaderSetNatts(td, attribute_num);
    td->t_hoff = hoff;

    /* else leave infomask = 0 */
    if (tuple_descriptor->tdhasoid) {
        td->t_infomask = HEAP_HASOID;
    }

    heap_fill_tuple(
        tuple_descriptor, values, isnull, (char*)td + hoff, data_len, &td->t_infomask, (hasnull ? td->t_bits : NULL));

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
HeapTuple heap_formtuple(TupleDesc tuple_descriptor, Datum* values, const char* nulls)
{
    HeapTuple tuple; /* return tuple */
    int attribute_num = tuple_descriptor->natts;
    bool* bool_nulls = (bool*)palloc(attribute_num * sizeof(bool));
    int i;

    for (i = 0; i < attribute_num; i++) {
        bool_nulls[i] = (nulls[i] == 'n');
    }

    tuple = heap_form_tuple(tuple_descriptor, values, bool_nulls);

    pfree(bool_nulls);

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
HeapTuple heap_modify_tuple(
    HeapTuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* null_repl, const bool* do_replace)
{
    int attrubute_num = tuple_desc->natts;
    int attoff;
    Datum* values = NULL;
    bool* isnull = NULL;
    HeapTuple new_tuple;

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
    values = (Datum*)palloc(attrubute_num * sizeof(Datum));
    isnull = (bool*)palloc(attrubute_num * sizeof(bool));

    heap_deform_tuple(tuple, tuple_desc, values, isnull);

    for (attoff = 0; attoff < attrubute_num; attoff++) {
        if (do_replace[attoff]) {
            values[attoff] = repl_values[attoff];
            isnull[attoff] = null_repl[attoff];
        }
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    new_tuple = heap_form_tuple(tuple_desc, values, isnull);

    pfree(values);
    pfree(isnull);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    new_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
    new_tuple->t_self = tuple->t_self;
    new_tuple->t_tableOid = tuple->t_tableOid;
    new_tuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(new_tuple, tuple);
#ifdef PGXC
    new_tuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    if (tuple_desc->tdhasoid) {
        HeapTupleSetOid(new_tuple, HeapTupleGetOid(tuple));
    }

    return new_tuple;
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
HeapTuple heap_modifytuple(
    HeapTuple tuple, TupleDesc tuple_desc, Datum* repl_values, const char* repl_nulls, const char* repl_actions)
{
    HeapTuple result;
    int attrubute_num = tuple_desc->natts;
    bool* bool_nulls = (bool*)palloc(attrubute_num * sizeof(bool));
    bool* bool_actions = (bool*)palloc(attrubute_num * sizeof(bool));
    int attnum;

    for (attnum = 0; attnum < attrubute_num; attnum++) {
        bool_nulls[attnum] = (repl_nulls[attnum] == 'n');
        bool_actions[attnum] = (repl_actions[attnum] == 'r');
    }

    result = heap_modify_tuple(tuple, tuple_desc, repl_values, bool_nulls, bool_actions);

    pfree(bool_nulls);
    pfree(bool_actions);

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
void heap_deform_tuple(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull)
{
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute* att = tuple_desc->attrs;
    uint32 tdesc_natts = tuple_desc->natts;
    uint32 natts; /* number of atts to extract */
    uint32 attnum;
    char* tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8* bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* can we use/set attcacheoff? */

    Assert(!HEAP_TUPLE_IS_COMPRESSED(tup));
    natts = HeapTupleHeaderGetNatts(tup, tuple_desc);

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = Min(natts, tdesc_natts);
    if (natts > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of columns (%u) exceeds limit (%d)", natts, MaxTupleAttributeNumber)));
    }

    tp = (char*)tup + tup->t_hoff;

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
        values[attnum] = heapGetInitDefVal(attnum + 1, tuple_desc, &isnull[attnum]);
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
void heap_deformtuple(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, char* nulls)
{
    int natts = tuple_desc->natts;
    bool* bool_nulls = (bool*)palloc(natts * sizeof(bool));
    int attnum;

    heap_deform_tuple(tuple, tuple_desc, values, bool_nulls);

    for (attnum = 0; attnum < natts; attnum++) {
        nulls[attnum] = (bool_nulls[attnum] ? 'n' : ' ');
    }

    pfree(bool_nulls);
}

static void slot_deform_cmprs_tuple(TupleTableSlot* slot, uint32 natts);

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
static void slot_deform_tuple(TupleTableSlot* slot, uint32 natts)
{
    HeapTuple tuple = slot->tts_tuple;
    TupleDesc tuple_desc = slot->tts_tupleDescriptor;
    Datum* values = slot->tts_values;
    bool* isnull = slot->tts_isnull;
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute* att = tuple_desc->attrs;
    uint32 attnum;
    char* tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8* bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* can we use/set attcacheoff? */

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

    tp = (char*)tup + tup->t_hoff;

    for (; attnum < natts; attnum++) {
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

            if (!slow) {
                thisatt->attcacheoff = off;
            }
        }

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

#ifdef PGXC
/*
 * slot_deform_datarow
 * 		Extract data from the DataRow message into Datum/isnull arrays.
 * 		We always extract all atributes, as specified in tts_tupleDescriptor,
 * 		because there is no easy way to find random attribute in the DataRow.
 */
static void slot_deform_datarow(TupleTableSlot* slot)
{
    int attnum;
    int i;
    int col_count;
    char* cur = slot->tts_dataRow;
    StringInfo buffer;
    uint16 n16;
    uint32 n32;
    MemoryContext oldcontext;
    errno_t rc = EOK;

    if (slot->tts_tupleDescriptor == NULL || slot->tts_dataRow == NULL) {
        return;
    }

    Form_pg_attribute* att = slot->tts_tupleDescriptor->attrs;
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
                char* pstr = NULL;
                if (att[i]->attlen == -1) {
                    Size data_length = VARSIZE_ANY(buffer->data);
                    if (data_length > (Size)((uint32)len + 1)) {
                        pstr = (char*)palloc0(data_length);
                    } else {
                        pstr = (char*)palloc0(len + 1);
                    }
                } else {
                    pstr = (char*)palloc0(len + 1);
                }
                rc = memcpy_s(pstr, len + 1, buffer->data, len);
                securec_check(rc, "", "");
                pstr[len] = '\0';
                slot->tts_values[i] = (Datum)pstr;
            } else {
                /* for abstimein, transfer str to time in select has some problem, so distinguish 
                 * insert and select for ABSTIMEIN to avoid problem */
                t_thrd.time_cxt.is_abstimeout_in = true;

                slot->tts_values[i] = InputFunctionCall(slot->tts_attinmeta->attinfuncs + i,
                    buffer->data,
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
 * slot_getattr
 *		This function fetches an attribute of the slot's current tuple.
 *		It is functionally equivalent to heap_getattr, but fetches of
 *		multiple attributes of the same tuple will be optimized better,
 *		because we avoid O(N^2) behavior from multiple calls of
 *		nocachegetattr(), even when attcacheoff isn't usable.
 *
 *		A difference from raw heap_getattr is that attnums beyond the
 *		slot's tupdesc's last attribute will be considered NULL even
 *		when the physical tuple is longer than the tupdesc.
 */
Datum slot_getattr(TupleTableSlot* slot, int attnum, bool* isnull)
{
    HeapTuple tuple = slot->tts_tuple;
    TupleDesc tuple_desc = slot->tts_tupleDescriptor;
    HeapTupleHeader tup;

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
        return heap_getsysattr(tuple, attnum, tuple_desc, isnull);
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
    if (attnum > tuple_desc->natts) {
        *isnull = true;
        return (Datum)0;
    }

#ifdef PGXC
    /* If it is a data row tuple extract all and return requested */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot);
        *isnull = slot->tts_isnull[attnum - 1];
        return slot->tts_values[attnum - 1];
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    /* internal error */
    if (tuple == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /*
     * return NULL if attnum is out of range according to the tuple
     *
     * (We have to check this separately because of various inheritance and
     * table-alteration scenarios: the tuple could be either longer or shorter
     * than the tupdesc.)
     */
    tup = tuple->t_data;
    if (attnum > (int)HeapTupleHeaderGetNatts(tup, tuple_desc)) {
        /* get init default value from tupleDesc.
         * The original Code is:
         * example code: *isnull = true;
         * example code: return (Datum) 0;
         */
        return heapGetInitDefVal(attnum, tuple_desc, isnull);
    }

    /*
     * check if target attribute is null: no point in groveling through tuple
     */
    if (HeapTupleHasNulls(tuple) && att_isnull(((unsigned int)(attnum - 1)), tup->t_bits)) {
        *isnull = true;
        return (Datum)0;
    }

    /*
     * If the attribute's column has been dropped, we force a NULL result.
     * This case should not happen in normal use, but it could happen if we
     * are executing a plan cached before the column was dropped.
     */
    if (tuple_desc->attrs[attnum - 1]->attisdropped) {
        *isnull = true;
        return (Datum)0;
    }

    /*
     * Extract the attribute, along with any preceding attributes.
     */
    if (HEAP_TUPLE_IS_COMPRESSED(slot->tts_tuple->t_data)) {
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
 * slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using slot_getattr.
 */
void slot_getallattrs(TupleTableSlot* slot)
{
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
        slot_deform_datarow(slot);
        return;
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    tuple = slot->tts_tuple;
    /* internal error */
    if (tuple == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /*
     * load up any slots available from physical tuple
     */
    attnum = HeapTupleHeaderGetNatts(tuple->t_data, slot->tts_tupleDescriptor);
    attnum = Min(attnum, tdesc_natts);

    if (HEAP_TUPLE_IS_COMPRESSED(slot->tts_tuple->t_data)) {
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

/*
 * slot_getsomeattrs
 *		This function forces the entries of the slot's Datum/isnull
 *		arrays to be valid at least up through the attnum'th entry.
 */
void slot_getsomeattrs(TupleTableSlot* slot, int attnum)
{
    HeapTuple tuple;
    int attno;

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid >= attnum) {
        return;
    }

#ifdef PGXC
    /* Handle the DataRow tuple case */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot);
        return;
    }
#endif

    /* Check for caller error */
    if (attnum <= 0 || attnum > slot->tts_tupleDescriptor->natts) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("invalid attribute number %d", attnum)));
    }

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    tuple = slot->tts_tuple;
    /* internal error */
    if (tuple == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /*
     * load up any slots available from physical tuple
     */
    attno = HeapTupleHeaderGetNatts(tuple->t_data, slot->tts_tupleDescriptor);
    attno = Min(attno, attnum);

    if (HEAP_TUPLE_IS_COMPRESSED(slot->tts_tuple->t_data)) {
        slot_deform_cmprs_tuple(slot, attno);
    } else {
        slot_deform_tuple(slot, attno);
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
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
 * slot_attisnull
 *		Detect whether an attribute of the slot is null, without
 *		actually fetching it.
 */
bool slot_attisnull(TupleTableSlot* slot, int attnum)
{
    HeapTuple tuple = slot->tts_tuple;
    TupleDesc tuple_desc = slot->tts_tupleDescriptor;

    /*
     * system attributes are handled by heap_attisnull
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
    if (attnum > tuple_desc->natts) {
        return true;
    }

#ifdef PGXC
    /* If it is a data row tuple extract all and return requested */
    if (slot->tts_dataRow) {
        slot_deform_datarow(slot);
        return slot->tts_isnull[attnum - 1];
    }
#endif

    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    /* internal error */
    if (tuple == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot extract attribute from empty tuple slot")));
    }

    /* and let the tuple tell it */
    /* get init default value from tupleDesc.
     * The original Code is:
     * return heap_attisnull(tuple, attnum, tupleDesc);
     */
    return relationAttIsNull(tuple, attnum, tuple_desc);
}

/*
 * heap_freetuple
 */
void heap_freetuple(HeapTuple htup)
{
    pfree(htup);
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
MinimalTuple heap_form_minimal_tuple(TupleDesc tuple_descriptor, Datum* values, const bool* isnull, MinimalTuple in_tuple)
{
    MinimalTuple tuple; /* return tuple */
    Size len, data_len;
    int hoff;
    bool hasnull = false;
    Form_pg_attribute* att = tuple_descriptor->attrs;
    int attrubute_num = tuple_descriptor->natts;
    int i;

    if (attrubute_num > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of columns (%d) exceeds limit (%d)", attrubute_num, MaxTupleAttributeNumber)));
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
    for (i = 0; i < attrubute_num; i++) {
        if (isnull[i]) {
            hasnull = true;
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
                 !VARATT_IS_EXTENDED(values[i])) {
            values[i] = toast_flatten_tuple_attribute(values[i], att[i]->atttypid, att[i]->atttypmod);
        }
    }

    /*
     * Determine total space needed
     */
    len = offsetof(MinimalTupleData, t_bits);

    if (hasnull) {
        len += BITMAPLEN(attrubute_num);
    }

    if (tuple_descriptor->tdhasoid) {
        len += sizeof(Oid);
    }

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_data_size(tuple_descriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.
     */
    if (in_tuple == NULL) {
        tuple = (MinimalTuple)palloc0(len);
    } else {
        if (in_tuple->t_len < len) {
            pfree(in_tuple);
            in_tuple = NULL;
            tuple = (MinimalTuple)palloc0(len);
        } else {
            errno_t rc = memset_s(in_tuple, in_tuple->t_len, 0, in_tuple->t_len);
            securec_check(rc, "\0", "\0");
            tuple = in_tuple;
        }
    }
    /*
     * And fill in the information.
     */
    tuple->t_len = len;
    HeapTupleHeaderSetNatts(tuple, attrubute_num);
    tuple->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;
    /* else leave infomask = 0 */
    if (tuple_descriptor->tdhasoid) {
        tuple->t_infomask = HEAP_HASOID;
    }

    heap_fill_tuple(tuple_descriptor,
        values,
        isnull,
        (char*)tuple + hoff,
        data_len,
        &tuple->t_infomask,
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

    result = (HeapTuple)palloc(HEAPTUPLESIZE + len);
    result->t_len = len;
    ItemPointerSetInvalid(&(result->t_self));
    result->t_tableOid = InvalidOid;
    result->t_bucketId = InvalidBktId;
    HeapTupleSetZeroBase(result);
#ifdef PGXC
    result->t_xc_node_id = 0;
#endif
    result->t_data = (HeapTupleHeader)((char*)result + HEAPTUPLESIZE);
    rc = memcpy_s((char*)result->t_data + MINIMAL_TUPLE_OFFSET, mtup->t_len, mtup, mtup->t_len);
    securec_check(rc, "\0", "\0");
    rc = memset_s(
        result->t_data, offsetof(HeapTupleHeaderData, t_infomask2), 0, offsetof(HeapTupleHeaderData, t_infomask2));
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
    rc = memcpy_s(result, len, (char*)htup->t_data + MINIMAL_TUPLE_OFFSET, len);
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
static Size heap_compute_cmprs_data_size(TupleDesc tuple_desc, FormCmprTupleData* cmprs_info)
{
    Size data_length = 0;
    int i;
    int attribute_num = tuple_desc->natts;
    Form_pg_attribute* att = tuple_desc->attrs;
    if (attribute_num > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of columns (%d) exceeds limit (%d)", attribute_num, MaxTupleAttributeNumber)));
    }

    /*
     * Important:
     * (1) compression-bitmap will be placed between header and data part of tuple.
     * (2) When <data_length> is not 0, data position MUST be MAXALIGN,  which is 8 bytes, alligned!
     * (3) It's bytes-alligned factor that results in computing space in order, first compression-map,
     *     then fields step by step !
     */
    data_length += BITMAPLEN(attribute_num);

    for (i = 0; i < attribute_num; i++) {
        /* NULLs first: no compression, no storage. */
        if (cmprs_info->isnulls[i]) {
            continue;
        }

        /* compression second: specail value, special size, and special alligned. */
        if (cmprs_info->compressed[i]) {
            data_length += cmprs_info->valsize[i];
            continue;
        }

        /* the normal field is the last */
        Datum val = cmprs_info->values[i];

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
static void heap_fill_bitmap(char* buf, const bool* flags, int nflag)
{
    char* byteBuf = (buf - 1);
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
static void heap_fill_cmprs_tuple(
    TupleDesc tuple_desc, FormCmprTupleData* cmprs_info, char* data, Size data_size, uint16* infomask, bits8* bit)
{
    bits8* bitP = NULL;
    uint32 bitmask;
    int i;
    int attrubute_num = tuple_desc->natts;
    errno_t retno = EOK;
    Form_pg_attribute* att = tuple_desc->attrs;
    char* start = data;

    /* compression-bitmap MUST be put firstly.
     * refer to heap_compute_cmprs_data_size()
     */
    heap_fill_bitmap(data, cmprs_info->compressed, attrubute_num);
    data += (int16)BITMAPLEN(attrubute_num);

    if (bit != NULL) {
        bitP = &bit[-1];
        bitmask = HIGHBIT;
    } else {
        /* just to keep compiler quiet */
        bitP = NULL;
        bitmask = 0;
    }

    *infomask &= ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL | HEAP_COMPRESSED);

    for (i = 0; i < attrubute_num; i++) {
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

            if (cmprs_info->isnulls[i]) {
                *infomask |= HEAP_HASNULL;
                continue;
            }

            *bitP |= bitmask;
        }

        /* compression second: specail value, special size, and special alligned. */
        if (cmprs_info->compressed[i]) {
            /* Important: compressed value is passed by pointer (char*) */
            Assert(!cmprs_info->isnulls[i]);
            if (cmprs_info->valsize[i] != 0) {
                Assert(cmprs_info->valsize[i] > 0);
                Pointer val = DatumGetPointer(cmprs_info->values[i]);
                retno = memcpy_s(data, remian_length, val, cmprs_info->valsize[i]);
                securec_check(retno, "\0", "\0");
                data += cmprs_info->valsize[i];
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
            data = (char*)att_align_nominal(data, att[i]->attalign);
            store_att_byval(data, cmprs_info->values[i], att[i]->attlen);
            data_length = att[i]->attlen;
        } else if (att[i]->attlen == -1) {
            /* varlena */
            Pointer val = DatumGetPointer(cmprs_info->values[i]);

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
                data = (char*)att_align_nominal(data, att[i]->attalign);
                data_length = VARSIZE(val);
                retno = memcpy_s(data, remian_length, val, data_length);
                securec_check(retno, "\0", "\0");
            }
        } else if (att[i]->attlen == -2) {
            /* cstring ... never needs alignment */
            *infomask |= HEAP_HASVARWIDTH;
            Assert(att[i]->attalign == 'c');
            data_length = strlen(DatumGetCString(cmprs_info->values[i])) + 1;
            retno = memcpy_s(data, remian_length, DatumGetPointer(cmprs_info->values[i]), data_length);
            securec_check(retno, "\0", "\0");
        } else {
            /* fixed-length pass-by-reference */
            data = (char*)att_align_nominal(data, att[i]->attalign);
            Assert(att[i]->attlen > 0);
            data_length = att[i]->attlen;
            retno = memcpy_s(data, remian_length, DatumGetPointer(cmprs_info->values[i]), data_length);
            securec_check(retno, "\0", "\0");
        }

        data += data_length;
    }

    Assert((size_t)(data - start) == data_size);
}

Datum nocache_cmprs_get_attr(HeapTuple tuple, unsigned int attnum, TupleDesc tuple_desc, char* cmprs_info)
{
    HeapTupleHeader tup = tuple->t_data;
    Form_pg_attribute* att = tuple_desc->attrs;
    char* tp = NULL;         /* ptr to data part of tuple */
    bits8* bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bits8* cmprs_bitmap = NULL;
    int off = 0; /* current offset within data */
    uint32 i = 0;

    Assert(HEAP_TUPLE_IS_COMPRESSED(tup) && (cmprs_info != NULL));
    Assert(attnum <= HeapTupleHeaderGetNatts(tup, tuple_desc));
    Assert(tuple_desc->natts >= (int)HeapTupleHeaderGetNatts(tup, tuple_desc));

    attnum--;
    tp = (char*)tup + tup->t_hoff;
    cmprs_bitmap = (bits8*)tp;
    off = BITMAPLEN(HeapTupleHeaderGetNatts(tup, tuple_desc));

    int cmprs_off = 0; /* pointer to the start of compression meta */
    void* meta_info = NULL;
    char mode = 0;

    for (i = 0;; i++) { /* loop exit is at "break" */
        Assert(i < HeapTupleHeaderGetNatts(tup, tuple_desc));

        /* first parse compression metaInfo data of this attr */
        int meta_size = 0;
        meta_info = PageCompress::FetchAttrCmprMeta(cmprs_info + cmprs_off, att[i]->attlen, &meta_size, &mode);
        cmprs_off += meta_size;

        if (HeapTupleHasNulls(tuple) && att_isnull(i, bp)) {
            continue; /* this cannot be the target att */
        }

        if (isAttrCompressed(i, cmprs_bitmap)) {
            if (attnum != i) {
                off += PageCompress::GetAttrCmprValSize(mode, att[i]->attlen, meta_info, tp + off);
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
    if (isAttrCompressed(attnum, cmprs_bitmap)) {
        int attsize = 0;
        Datum attr_val =
            PageCompress::UncompressOneAttr(mode, meta_info, att[i]->atttypid, att[i]->attlen, tp + off, &attsize);
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
static HeapTuple HeapUncompressTup(HeapTuple src_tuple, TupleDesc tuple_desc, char* cmprs_info, HeapTuple dest_tuple)
{
    Assert(src_tuple && tuple_desc && cmprs_info);

    HeapTupleHeader src_tup = src_tuple->t_data;
    Form_pg_attribute* att = tuple_desc->attrs;
    uint32 tdesc_natts = tuple_desc->natts;
    uint32 natts; /* number of atts to extract */
    uint32 attr_idx;
    char* src_tup_data = NULL;             /* ptr to srcTuple data */
    long src_off;                         /* offset in srcTuple data */
    bits8* src_null_bits = src_tup->t_bits; /* ptr to null bitmap in srcTuple */
    bits8* cmprs_bitmap = NULL;           /* pointer to compression bitmap in srcTuple */
#ifdef USE_ASSERT_CHECKING
    uint16 test_infomask = tuple_desc->tdhasoid ? HEAP_HASOID : 0;
#endif

    Assert(HEAP_TUPLE_IS_COMPRESSED(src_tuple->t_data) && (cmprs_info != NULL));

    /*
     * In inheritance situations, it is possible that the given srcTuple actually
     * has more fields than the caller is expecting.  Don't run srcOff the end of
     * the caller's arrays.
     */
    Assert(tdesc_natts >= HeapTupleHeaderGetNatts(src_tup, tuple_desc));
    natts = Min(HeapTupleHeaderGetNatts(src_tup, tuple_desc), tdesc_natts);
    src_tup_data = (char*)src_tup + src_tup->t_hoff;
    cmprs_bitmap = (bits8*)src_tup_data;
    src_off = BITMAPLEN(natts);

    errno_t retno = EOK;
    int cmprs_off = 0; /* pointer to the start of compression meta */
    void* meta_info = NULL;
    char mode = 0;

    /* Dest srcTuple header size */
    int hoff = src_tup->t_hoff;
    int dest_data_length = 0;

    if (dest_tuple == NULL) {
        dest_tuple = (HeapTuple)palloc0(MaxHeapTupleSize + HEAPTUPLESIZE);
        dest_tuple->t_data = (HeapTupleHeader)((char*)dest_tuple + HEAPTUPLESIZE);
    }

    Assert(dest_tuple->t_data != NULL);
    HeapTupleHeader dest_tup = dest_tuple->t_data;
    Assert(((size_t)MAXALIGN(dest_tup)) == (size_t)dest_tup);

    bits8* dest_null_bits = dest_tup->t_bits;
    char* dest_tup_data = (char*)dest_tup + hoff;
    Datum val = 0;

    /* Copy null bitmap */
    if (hasnulls) {
        retno = memcpy_s(dest_null_bits, BITMAPLEN(natts), src_null_bits, BITMAPLEN(natts));
        securec_check(retno, "\0", "\0");
#ifdef USE_ASSERT_CHECKING
        test_infomask |= HEAP_HASNULL;
#endif
    }

    for (attr_idx = 0; attr_idx < natts; ++attr_idx) {
        Form_pg_attribute thisatt = att[attr_idx];
        /* parse compression metaInfo data of this attr */
        int meta_size = 0;
        meta_info = PageCompress::FetchAttrCmprMeta(cmprs_info + cmprs_off, thisatt->attlen, &meta_size, &mode);
        cmprs_off += meta_size;

        /* IMPORTANT: NULLs first, row-compression second, and the normal fields the last; */
        if (hasnulls && att_isnull(attr_idx, src_null_bits)) {
            continue;
        }

        if (isAttrCompressed(attr_idx, cmprs_bitmap)) {
            int attsize = 0;

            if (PageCompress::NeedExternalBuf(mode)) {
#ifdef USE_ASSERT_CHECKING
                test_infomask |= HEAP_HASVARWIDTH;
#endif

                dest_data_length = PageCompress::UncompressOneAttr(
                    mode, thisatt->attalign, thisatt->attlen, meta_info, src_tup_data + src_off, &attsize, dest_tup_data);
                /* attsize is the size of compressed value. */
                src_off = src_off + attsize;
                /* both datum size and padding size are included in destDataLength. */
                Assert(dest_data_length > 0);
                dest_tup_data += dest_data_length;
                continue;
            } else {
                val = PageCompress::UncompressOneAttr(
                    mode, meta_info, thisatt->atttypid, thisatt->attlen, src_tup_data + src_off, &attsize);
                src_off = src_off + attsize; /* attsize is the size of compressed value. */
            }
        } else {
            if (thisatt->attlen == -1) {
                src_off = att_align_pointer(src_off, thisatt->attalign, -1, src_tup_data + src_off);
            } else {
                /* not varlena, so safe to use att_align_nominal */
                src_off = att_align_nominal(src_off, thisatt->attalign);
            }

            val = fetchatt(thisatt, src_tup_data + src_off);
            src_off = att_addlength_pointer(src_off, thisatt->attlen, src_tup_data + src_off);
        }

        /* Now we fill dest srcTuple with the val */
        if (thisatt->attbyval) {
            /* pass-by-value */
            dest_tup_data = (char*)att_align_nominal(dest_tup_data, thisatt->attalign);
            store_att_byval(dest_tup_data, val, thisatt->attlen);
            dest_data_length = thisatt->attlen;
        } else if (thisatt->attlen == -1) {
            /* varlena */
            Pointer tmp_val = DatumGetPointer(val);
#ifdef USE_ASSERT_CHECKING
            test_infomask |= HEAP_HASVARWIDTH;
#endif

            if (VARATT_IS_EXTERNAL(tmp_val)) {
                /* no alignment, since it's short by definition */
                dest_data_length = VARSIZE_EXTERNAL(tmp_val);
                retno = memcpy_s(dest_tup_data, dest_data_length, tmp_val, dest_data_length);
                securec_check(retno, "\0", "\0");
            } else if (VARATT_IS_SHORT(tmp_val)) {
                /* no alignment for short varlenas */
                dest_data_length = VARSIZE_SHORT(tmp_val);
                retno = memcpy_s(dest_tup_data, dest_data_length, tmp_val, dest_data_length);
                securec_check(retno, "\0", "\0");
            } else if (VARLENA_ATT_IS_PACKABLE(thisatt) && VARATT_CAN_MAKE_SHORT(tmp_val)) {
                /* convert to short varlena -- no alignment */
                dest_data_length = VARATT_CONVERTED_SHORT_SIZE(tmp_val);
                SET_VARSIZE_SHORT(dest_tup_data, dest_data_length);
                retno = memcpy_s(dest_tup_data + 1, dest_data_length - 1, VARDATA(tmp_val), dest_data_length - 1);
                securec_check(retno, "\0", "\0");
            } else {
                /*
                 * Memset padding bytes, because att_align_pointer will judge
                 * padding bytes whether zero. Please refer to att_align_pointer
                 */
                *dest_tup_data = 0;

                /* full 4-byte header varlena */
                dest_tup_data = (char*)att_align_nominal(dest_tup_data, thisatt->attalign);
                dest_data_length = VARSIZE(tmp_val);
                retno = memcpy_s(dest_tup_data, dest_data_length, tmp_val, dest_data_length);
                securec_check(retno, "\0", "\0");
            }
        } else if (thisatt->attlen == -2) {
#ifdef USE_ASSERT_CHECKING
            test_infomask |= HEAP_HASVARWIDTH;
#endif
            Assert(thisatt->attalign == 'c');
            dest_data_length = strlen(DatumGetCString(val)) + 1;
            retno = memcpy_s(dest_tup_data, dest_data_length, DatumGetPointer(val), dest_data_length);
            securec_check(retno, "\0", "\0");
        } else {
            /* fixed-length pass-by-reference */
            dest_tup_data = (char*)att_align_nominal(dest_tup_data, thisatt->attalign);
            Assert(thisatt->attlen > 0);
            dest_data_length = thisatt->attlen;
            retno = memcpy_s(dest_tup_data, dest_data_length, DatumGetPointer(val), dest_data_length);
            securec_check(retno, "\0", "\0");
        }
        dest_tup_data += dest_data_length;
    }

    /* complete destTuple other info excluding t_data */
    dest_tuple->t_len = (uint32)(dest_tup_data - (char*)dest_tup);
    dest_tuple->t_self = src_tuple->t_self;
    dest_tuple->t_tableOid = src_tuple->t_tableOid;
    dest_tuple->t_bucketId = src_tuple->t_bucketId;
#ifdef PGXC
    dest_tuple->t_xc_node_id = src_tuple->t_xc_node_id;
#endif

    /* complete destTup header info excluding data part */
    COPY_TUPLE_HEADERINFO(dest_tup, src_tup);
    HEAP_TUPLE_CLEAR_COMPRESSED(dest_tuple->t_data);
    Assert(test_infomask == (dest_tup->t_infomask & 0x0F));

    dest_tup->t_hoff = hoff;
    if (tuple_desc->tdhasoid) {
        HeapTupleHeaderSetOid(dest_tup, HeapTupleGetOid(src_tuple));
    }

    return dest_tuple;
}

/* the same to HeapUncompressTup() function which is a faster and
 * lighter-weight implement.
 * we highly recommand that you would not using HeapUncompressTup2()
 * only when alter-table-instant happens.
 */
static HeapTuple HeapUncompressTup2(HeapTuple tuple, TupleDesc tuple_desc, Page dictPage)
{
    Assert(HeapTupleIsValid(tuple) && (tuple->t_data != NULL));
    Assert(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));
    Assert((tuple_desc != NULL) && (dictPage != NULL));

    Datum* values = (Datum*)palloc(sizeof(Datum) * tuple_desc->natts);
    bool* isnulls = (bool*)palloc(sizeof(bool) * tuple_desc->natts);

    heap_deform_cmprs_tuple(tuple, tuple_desc, values, isnulls, dictPage);
    HeapTuple new_tuple = heap_form_tuple(tuple_desc, values, isnulls);

    /* don't copy tuple->t_len, that has been set in heap_form_tuple */
    new_tuple->t_self = tuple->t_self;
    new_tuple->t_tableOid = tuple->t_tableOid;
    new_tuple->t_bucketId = tuple->t_bucketId;
#ifdef PGXC
    new_tuple->t_xc_node_id = tuple->t_xc_node_id;
#endif
    if (tuple_desc->tdhasoid) {
        HeapTupleSetOid(new_tuple, HeapTupleGetOid(tuple));
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(new_tuple->t_data));
    COPY_TUPLE_HEADER_XACT_INFO(new_tuple, tuple);

    pfree_ext(isnulls);
    pfree_ext(values);
    return new_tuple;
}

HeapTuple test_HeapUncompressTup2(HeapTuple tuple, TupleDesc tuple_desc, Page dictPage)
{
    return HeapUncompressTup2(tuple, tuple_desc, dictPage);
}

/*
 * heap_form_cmprs_tuple
 *		construct a compressed tuple from the given cmprsInfo.
 *
 * The result is allocated in the current memory context.
 */
HeapTuple heap_form_cmprs_tuple(TupleDesc tuple_descriptor, FormCmprTupleData* cmprs_info)
{
    HeapTuple tuple;    /* return tuple */
    HeapTupleHeader td; /* tuple data */
    Size len, data_len;
    int hoff;
    bool hasnull = false;
#ifdef USE_ASSERT_CHECKING
    bool hascmpr = false;
#endif
    Form_pg_attribute* att = tuple_descriptor->attrs;
    int attribute_num = tuple_descriptor->natts;
    int i;

    if (attribute_num > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of columns (%d) exceeds limit (%d)", attribute_num, MaxTupleAttributeNumber)));
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
    for (i = 0; i < attribute_num; i++) {
        if (cmprs_info->isnulls[i]) {
            hasnull = true;
        }

        if (cmprs_info->compressed[i]) {
#ifdef USE_ASSERT_CHECKING
            hascmpr = true;
#endif
        } else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
                   !VARATT_IS_EXTENDED(DatumGetPointer(cmprs_info->values[i]))) {
            cmprs_info->values[i] =
                toast_flatten_tuple_attribute(cmprs_info->values[i], att[i]->atttypid, att[i]->atttypmod);
        }
    }
    Assert(hascmpr == true);

    /*
     * Determine total space needed
     */
    len = offsetof(HeapTupleHeaderData, t_bits);

    if (hasnull) {
        len += BITMAPLEN(attribute_num);
    }

    if (tuple_descriptor->tdhasoid) {
        len += sizeof(Oid);
    }

    hoff = len = MAXALIGN(len); /* align user data safely */

    data_len = heap_compute_cmprs_data_size(tuple_descriptor, cmprs_info);

    len += data_len;

    /*
     * Allocate and zero the space needed.	Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    tuple = (HeapTuple)palloc0(HEAPTUPLESIZE + len);
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
    HeapTupleHeaderSetTypeId(td, tuple_descriptor->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tuple_descriptor->tdtypmod);

    HeapTupleHeaderSetNatts(td, attribute_num);
    td->t_hoff = hoff;

    /* else leave infomask = 0 */
    if (tuple_descriptor->tdhasoid) {
        td->t_infomask = HEAP_HASOID;
    }

    heap_fill_cmprs_tuple(
        tuple_descriptor, cmprs_info, (char*)td + hoff, data_len, &td->t_infomask, (hasnull ? td->t_bits : NULL));
    Assert(HEAP_TUPLE_IS_COMPRESSED(td));
    Assert(attribute_num == (int)HeapTupleHeaderGetNatts(td, tuple_descriptor));

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
void heap_deform_cmprs_tuple(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, char* cmprs_info)
{
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute* att = tuple_desc->attrs;
    uint32 tdesc_natts = tuple_desc->natts;
    uint32 natts; /* number of atts to extract */
    uint32 attnum;
    char* tp = NULL;           /* ptr to tuple data */
    long off;                  /* offset in tuple data */
    bits8* bp = tup->t_bits;   /* ptr to null bitmap in tuple */
    bits8* cmprs_bitmap = NULL; /* pointer to compression bitmap in tuple */

    Assert(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) && (cmprs_info != NULL));

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = Min(HeapTupleHeaderGetNatts(tup, tuple_desc), tdesc_natts);
    tp = (char*)tup + tup->t_hoff;
    cmprs_bitmap = (bits8*)tp;
    off = BITMAPLEN(natts);
    if (natts > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of columns (%u) exceeds limit (%d)", natts, MaxTupleAttributeNumber)));
    }

    int cmprs_off = 0; /* pointer to the start of compression meta */
    void* meta_info = NULL;
    char mode = 0;

    for (attnum = 0; attnum < natts; attnum++) {
        /* parse compression metaInfo data of this attr */
        int meta_size = 0;
        meta_info = PageCompress::FetchAttrCmprMeta(cmprs_info + cmprs_off, att[attnum]->attlen, &meta_size, &mode);
        cmprs_off += meta_size;

        /* IMPORTANT: NULLs first, row-compression second, and the normal fields the last; */
        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            continue;
        }

        isnull[attnum] = false;

        if (isAttrCompressed(attnum, cmprs_bitmap)) {
            int attsize = 0;
            values[attnum] = PageCompress::UncompressOneAttr(
                mode, meta_info, att[attnum]->atttypid, att[attnum]->attlen, tp + off, &attsize);
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
        values[attnum] = heapGetInitDefVal(attnum + 1, tuple_desc, &isnull[attnum]);
    }
}

void heap_deform_tuple2(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, Buffer buffer)
{
    Assert((tuple != NULL) && (tuple->t_data != NULL));
    if (!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data)) {
        heap_deform_tuple(tuple, tuple_desc, values, isnull);
        return;
    }

    Assert(BufferIsValid(buffer));
    Page page = BufferGetPage(buffer);
    Assert((page != NULL) && (PageIsCompressed(page)));
    heap_deform_cmprs_tuple(tuple, tuple_desc, values, isnull, (char*)getPageDict(page));
}

/* deform the passed heap tuple.
 * call heap_deform_tuple() if it's not compressed,
 * otherwise call heap_deform_cmprs_tuple().
 */
void heap_deform_tuple3(HeapTuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, Page page)
{
    Assert((tuple != NULL) && (tuple->t_data != NULL));
    if (!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data)) {
        heap_deform_tuple(tuple, tuple_desc, values, isnull);
        return;
    }

    Assert((page != NULL) && (PageIsCompressed(page)));
    heap_deform_cmprs_tuple(tuple, tuple_desc, values, isnull, (char*)getPageDict(page));
}

/* decompress one tuple and return a copy of uncompressed tuple */
HeapTuple heapCopyCompressedTuple(HeapTuple tuple, TupleDesc tuple_desc, Page page, HeapTuple dest_tup)
{
    HeapTuple new_tuple = NULL;
    Assert(HeapTupleIsValid(tuple) && (tuple->t_data != NULL));
    Assert(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));
    Assert((tuple_desc != NULL) && (page != NULL));

    /* we have to handle alter-table-instant case, because
     * HeapUncompressTup() don't think about that case
     * and now is difficult to handle this problem.
     */
    if (tuple_desc->initdefvals && tuple_desc->natts > (int)HeapTupleHeaderGetNatts(tuple->t_data, tuple_desc)) {
        new_tuple = HeapUncompressTup2(tuple, tuple_desc, (Page)getPageDict(page));
        if (dest_tup) {
            errno_t retno = EOK;
            Assert(MAXALIGN(new_tuple->t_len) <= MaxHeapTupleSize);

            /* copy the new tuple into existing space. */
            dest_tup->t_len = new_tuple->t_len;
            dest_tup->t_self = new_tuple->t_self;
            dest_tup->t_tableOid = new_tuple->t_tableOid;
            dest_tup->t_bucketId = new_tuple->t_bucketId;
            dest_tup->t_xc_node_id = new_tuple->t_xc_node_id;
            dest_tup->t_data = (HeapTupleHeader)((char*)dest_tup + HEAPTUPLESIZE);
            retno = memcpy_s(dest_tup->t_data, dest_tup->t_len, new_tuple->t_data, new_tuple->t_len);
            securec_check(retno, "\0", "\0");

            /* release unused space and make *newTuple* point to *destTup*. */
            heap_freetuple(new_tuple);
            new_tuple = dest_tup;
        }
    } else {
        if (!HeapTupleHasNulls(tuple)) {
            new_tuple = HeapUncompressTup<false>(tuple, tuple_desc, (char*)getPageDict(page), dest_tup);
        } else {
            new_tuple = HeapUncompressTup<true>(tuple, tuple_desc, (char*)getPageDict(page), dest_tup);
        }
    }

    return new_tuple;
}

/* copy new tuple from given tuple, and fill the
 * default value for all the new and added attrubutes.
 */
static HeapTuple HeapCopyInitdefvalTup(HeapTuple tuple, TupleDesc tup_desc)
{
    /* malloc and set doReplace[] to be false. */
    bool* do_replace = (bool*)palloc0(tup_desc->natts * sizeof(bool));

    /* rebuild heapTuple */
    HeapTuple new_tuple = heap_modify_tuple(tuple, tup_desc, NULL, NULL, do_replace);
    COPY_TUPLE_HEADER_XACT_INFO(new_tuple, tuple);

    pfree_ext(do_replace);
    return new_tuple;
}

/* ---------------------------------------------------------------------
 *		heapCopyTuple
 *
 *		wrapper for heap_copytuple && heapCopyCompressedTuple
 *
 * ---------------------------------------------------------------------
 */
HeapTuple heapCopyTuple(HeapTuple tuple, TupleDesc tup_desc, Page page)
{
    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL) {
        ereport(WARNING,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                (errmsg("tuple copy failed, because tuple is invalid or tuple data is null "))));
        return NULL;
    }

    if (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data)) {
        return heapCopyCompressedTuple(tuple, tup_desc, page);
    }

    if (tup_desc->initdefvals && tup_desc->natts > (int)HeapTupleHeaderGetNatts(tuple->t_data, tup_desc)) {
        return HeapCopyInitdefvalTup(tuple, tup_desc);
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
MinimalTuple heapFormMinimalTuple(HeapTuple tuple, TupleDesc tuple_desc, Page page)
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
        (tuple_desc->initdefvals != NULL && tuple_desc->natts > (int)HeapTupleHeaderGetNatts(tuple->t_data, tuple_desc))) {
        Datum* values = (Datum*)palloc(tuple_desc->natts * sizeof(Datum));
        bool* is_null = (bool*)palloc(tuple_desc->natts * sizeof(bool));

        heap_deform_tuple3(tuple, tuple_desc, values, is_null, (tupIsCompressed ? page : NULL));
        MinimalTuple result = heap_form_minimal_tuple(tuple_desc, values, is_null);

        pfree_ext(values);
        pfree_ext(is_null);
        return result;
    }

    /* copy heap tuple directly. */
    return minimal_tuple_from_heap_tuple(tuple);
}

/* a copy of slot_deform_tuple for compressied tuple */
static void slot_deform_cmprs_tuple(TupleTableSlot* slot, uint32 natts)
{
    HeapTuple tuple = slot->tts_tuple;
    TupleDesc tuple_desc = slot->tts_tupleDescriptor;
    Datum* values = slot->tts_values;
    bool* isnull = slot->tts_isnull;
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    Form_pg_attribute* att = tuple_desc->attrs;
    uint32 attnum;
    char* tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8* bp = tup->t_bits; /* ptr to null bitmap in tuple */

    bits8* cmprs_bitmap = NULL;
    char* cmprs_info = NULL;
    void* meta_info = NULL;
    int cmprs_off = 0;
    char mode = 0;

    Assert(HEAP_TUPLE_IS_COMPRESSED(tup));
    Assert(BufferIsValid(slot->tts_buffer));

    AutoContextSwitch memContextGuard(slot->tts_per_tuple_mcxt);

    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    attnum = slot->tts_nvalid;
    if (attnum == 0) {
        /* Start from the first attribute */
        Assert(tuple_desc->natts >= (int)HeapTupleHeaderGetNatts(tup, tuple_desc));
        off = BITMAPLEN(HeapTupleHeaderGetNatts(tup, tuple_desc));
        cmprs_off = 0;
    } else {
        /* Restore state from previous execution */
        off = slot->tts_off;
        cmprs_off = slot->tts_meta_off;
    }

    tp = (char*)tup + tup->t_hoff;
    cmprs_bitmap = (bits8*)tp;

    Page page = BufferGetPage(slot->tts_buffer);
    Assert((page != NULL) && PageIsCompressed(page));
    cmprs_info = (char*)getPageDict(page);

    /* parse compression metaInfo data of this attr */
    int meta_size = 0;
    for (; attnum < natts; attnum++) {
        meta_info = PageCompress::FetchAttrCmprMeta(cmprs_info + cmprs_off, att[attnum]->attlen, &meta_size, &mode);
        cmprs_off += meta_size;

        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            continue;
        }

        isnull[attnum] = false;

        if (isAttrCompressed(attnum, cmprs_bitmap)) {
            int attsize = 0;
            values[attnum] = PageCompress::UncompressOneAttr(
                mode, meta_info, att[attnum]->atttypid, att[attnum]->attlen, tp + off, &attsize);
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
    slot->tts_meta_off = cmprs_off;
    slot->tts_slow = true;
}
