/* -------------------------------------------------------------------------
 *
 * indextuple.cpp
 *	   This file contains index tuple accessor and mutator routines,
 *	   as well as various tuple utilities.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/indextuple.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/itup.h"
#include "access/tuptoaster.h"
#include "access/sysattr.h"
#include "utils/rel.h"

/* ----------------------------------------------------------------
 *				  index_ tuple interface routines
 * ----------------------------------------------------------------
 */
static inline bool index_findattr(Relation irel, IndexTuple itup, AttrNumber attrno, Datum *value)
{
    bool isnull = false;

    TupleDesc tupdesc = RelationGetDescr(irel);
    int nattrs = IndexRelationGetNumberOfAttributes(irel);

    for (int i = 0; i < nattrs; i++) {
        if (irel->rd_index->indkey.values[i] == attrno) {
            *value = index_getattr(itup, i + 1, tupdesc, &isnull);
            break;
        }
    }

    return !isnull;
}
Oid index_getattr_tableoid(Relation irel, IndexTuple itup)
{
    Datum val = 0;
    Oid tableoid = InvalidOid;

    Assert(RelationIsIndex(irel));

    if (index_findattr(irel, itup, TableOidAttributeNumber, &val)) {
        tableoid = DatumGetUInt32(val);
    }

    return tableoid;
}
int2 index_getattr_bucketid(Relation irel, IndexTuple itup)
{
    Datum val = 0;
    int2 bucketid = InvalidBktId;

    Assert(RelationIsIndex(irel));

    if (index_findattr(irel, itup, BucketIdAttributeNumber, &val)) {
        bucketid = DatumGetInt16(val);
    }

    return bucketid;
}

/* ----------------
 *		index_form_tuple
 *
 * 		This shouldn't leak any memory; otherwise, callers such as
 *		tuplesort_putindextuplevalues() will be very unhappy.
 * ----------------
 */
IndexTuple index_form_tuple(TupleDesc tuple_descriptor, Datum* values, const bool* isnull)
{
    char *tp = NULL;         /* tuple pointer */
    IndexTuple tuple = NULL; /* return tuple */
    Size size, data_size, hoff;
    int i;
    unsigned short infomask = 0;
    bool hasnull = false;
    uint16 tupmask = 0;
    int attributeNum = tuple_descriptor->natts;

    Size (*computedatasize_tuple)(TupleDesc tuple_desc, Datum* values, const bool* isnull);
    void (*filltuple)(TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit);

    computedatasize_tuple = &heap_compute_data_size;
    filltuple = &heap_fill_tuple;

#ifdef TOAST_INDEX_HACK
    Datum untoasted_values[INDEX_MAX_KEYS];
    bool untoasted_free[INDEX_MAX_KEYS];
#endif

    if (attributeNum > INDEX_MAX_KEYS)
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("number of index columns (%d) exceeds limit (%d)", attributeNum, INDEX_MAX_KEYS)));

#ifdef TOAST_INDEX_HACK
    uint32 toastTarget = TOAST_INDEX_TARGET;
    if (tuple_descriptor->tdTableAmType == TAM_USTORE) {
        toastTarget = UTOAST_INDEX_TARGET;
    }   
    for (i = 0; i < attributeNum; i++) {
        Form_pg_attribute att = tuple_descriptor->attrs[i];

        untoasted_values[i] = values[i];
        untoasted_free[i] = false;

        /* Do nothing if value is NULL or not of varlena type */
        if (isnull[i] || att->attlen != -1)
            continue;

        /*
         * If value is stored EXTERNAL, must fetch it so we are not depending
         * on outside storage.	This should be improved someday.
         */
        Pointer val = DatumGetPointer(values[i]);
        checkHugeToastPointer((varlena *)val);
        if (VARATT_IS_EXTERNAL(val)) {
            untoasted_values[i] = PointerGetDatum(heap_tuple_fetch_attr((struct varlena*)DatumGetPointer(values[i])));
            untoasted_free[i] = true;
        }

        /*
         * If value is above size target, and is of a compressible datatype,
         * try to compress it in-line.
         */
        if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
            VARSIZE(DatumGetPointer(untoasted_values[i])) > toastTarget &&
            (att->attstorage == 'x' || att->attstorage == 'm')) {
            Datum cvalue = toast_compress_datum(untoasted_values[i]);
            if (DatumGetPointer(cvalue) != NULL) {
                /* successful compression */
                if (untoasted_free[i])
                    pfree(DatumGetPointer(untoasted_values[i]));
                untoasted_values[i] = cvalue;
                untoasted_free[i] = true;
            }
        }
    }
#endif

    for (i = 0; i < attributeNum; i++) {
        if (isnull[i]) {
            hasnull = true;
            break;
        }
    }

    if (hasnull)
        infomask |= INDEX_NULL_MASK;

    hoff = IndexInfoFindDataOffset(infomask);
#ifdef TOAST_INDEX_HACK
    data_size = computedatasize_tuple(tuple_descriptor, untoasted_values, isnull);
#else
    data_size = computedatasize_tuple(tuple_descriptor, values, isnull);
#endif
    size = hoff + data_size;
    size = MAXALIGN(size); /* be conservative */

    tp = (char*)palloc0(size);
    tuple = (IndexTuple)tp;

    filltuple(tuple_descriptor,
#ifdef TOAST_INDEX_HACK
        untoasted_values,
#else
        values,
#endif
        isnull,
        (char*)tp + hoff,
        data_size,
        &tupmask,
        (hasnull ? (bits8*)tp + sizeof(IndexTupleData) : NULL));

#ifdef TOAST_INDEX_HACK
    for (i = 0; i < attributeNum; i++) {
        if (untoasted_free[i])
            pfree(DatumGetPointer(untoasted_values[i]));
    }
#endif

    /*
     * We do this because heap_fill_tuple wants to initialize a "tupmask"
     * which is used for HeapTuples, but we want an indextuple infomask. The
     * only relevant info is the "has variable attributes" field. We have
     * already set the hasnull bit above.
     */
    if (tupmask & HEAP_HASVARWIDTH)
        infomask |= INDEX_VAR_MASK;

    /*
     * Here we make sure that the size will fit in the field reserved for it
     * in t_info.
     */
    if ((size & INDEX_SIZE_MASK) != size)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("index row requires %lu bytes, maximum size is %lu",
                    (unsigned long)size,
                    (unsigned long)INDEX_SIZE_MASK)));

    infomask |= size;

    /*
     * initialize metadata
     */
    tuple->t_info = infomask;
    return tuple;
}

/* ----------------
 *		nocache_index_getattr
 *
 *		This gets called from index_getattr() macro, and only in cases
 *		where we can't use cacheoffset and the value is not null.
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
 * ----------------
 */
Datum nocache_index_getattr(IndexTuple tup, uint32 attnum, TupleDesc tuple_desc)
{
    Form_pg_attribute* att = tuple_desc->attrs;
    char* tp = NULL;   /* ptr to data part of tuple */
    bits8* bp = NULL;  /* ptr to null bitmap in tuple */
    bool slow = false; /* do we have to walk attrs? */
    int data_off;      /* tuple data offset */
    int off;           /* current offset within data */

    /*
     * Three cases:
     *	 1: No nulls and no variable-width attributes.
     *	 2: Has a null or a var-width AFTER att.
     *	 3: Has nulls or var-widths BEFORE att.
     */
    data_off = IndexInfoFindDataOffset(tup->t_info);

    attnum--;

    /*
     * there's a null somewhere in the tuple
     *
     * check to see if desired att is null
     */
    if (IndexTupleHasNulls(tup)) {
        /* XXX "knows" t_bits are just after fixed tuple header! */
        bp = (bits8*)((char*)tup + sizeof(IndexTupleData));

        /*
         * Now check to see if any preceding bits are null...
         */
        {
            int byte = attnum >> 3;
            int finalbit = attnum & 0x07;

            /* check for nulls "before" final bit of last byte */
            if ((~bp[byte]) & (((uint)1 << finalbit) - 1))
                slow = true;
            else {
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
    }

    tp = (char*)tup + data_off;

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
        if (IndexTupleHasVarwidths(tup)) {
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
        while (j < natts && att[j]->attcacheoff > 0)
            j++;

        off = att[j - 1]->attcacheoff + att[j - 1]->attlen;

        for (; j < natts; j++) {
            if (att[j]->attlen <= 0)
                break;

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
            if (IndexTupleHasNulls(tup) && att_isnull(i, bp)) {
                usecache = false;
                continue; /* this cannot be the target att */
            }

            /* If we know the next offset, we can skip the rest */
            if (usecache && att[i]->attcacheoff >= 0)
                off = att[i]->attcacheoff;
            else if (att[i]->attlen == -1) {
                /*
                 * We can only cache the offset for a varlena attribute if the
                 * offset is already suitably aligned, so that there would be
                 * no pad bytes in any case: then the offset will be valid for
                 * either an aligned or unaligned value.
                 */
                if (usecache && (uintptr_t)(off) == att_align_nominal((uint32)off, att[i]->attalign))
                    att[i]->attcacheoff = off;
                else {
                    off = att_align_pointer((uint32)off, att[i]->attalign, -1, tp + off);
                    usecache = false;
                }
            } else {
                /* not varlena, so safe to use att_align_nominal */
                off = att_align_nominal((uint32)off, att[i]->attalign);

                if (usecache)
                    att[i]->attcacheoff = off;
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
 * Convert an index tuple into Datum/isnull arrays.
 *
 * The caller must allocate sufficient storage for the output arrays.
 * (INDEX_MAX_KEYS entries should be enough.)
 */
void index_deform_tuple(IndexTuple tup, TupleDesc tuple_descriptor, Datum* values, bool* isnull)
{
    int i;

    /* Assert to protect callers who allocate fixed-size arrays */
    Assert(tuple_descriptor->natts <= INDEX_MAX_KEYS);

    for (i = 0; i < tuple_descriptor->natts; i++) {
        values[i] = index_getattr(tup, i + 1, tuple_descriptor, &isnull[i]);
    }
}

/*
 * Create a palloc'd copy of an index tuple.
 */
IndexTuple CopyIndexTuple(IndexTuple source)
{
    IndexTuple result;
    Size size;
    errno_t rc = EOK;

    size = IndexTupleSize(source);
    result = (IndexTuple)palloc(size);
    rc = memcpy_s(result, size, source, size);
    securec_check(rc, "\0", "\0");
    return result;
}

/*
 * Create a palloc'd copy of an index tuple with a reserved space.
 */
IndexTuple CopyIndexTupleAndReserveSpace(IndexTuple source, Size reserved_size)
{
    IndexTuple result;
    Size size;
    errno_t rc = EOK;

    size = IndexTupleSize(source);
    result = (IndexTuple)palloc0(size + reserved_size);
    rc = memcpy_s(result, size, source, size);
    securec_check(rc, "\0", "\0");
    IndexTupleSetSize(result, size + reserved_size);
    return result;
}

/*
 * Truncate tailing attributes from given index tuple leaving it with
 * new_indnatts number of attributes.
 */
IndexTuple index_truncate_tuple(TupleDesc tupleDescriptor, IndexTuple olditup, int new_indnatts)
{
    TupleDesc itupdesc = CreateTupleDescCopyConstr(tupleDescriptor);
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    IndexTuple newitup;

    Assert(tupleDescriptor->natts <= INDEX_MAX_KEYS);
    Assert(new_indnatts > 0);
    Assert(new_indnatts < tupleDescriptor->natts);

    index_deform_tuple(olditup, tupleDescriptor, values, isnull);

    /* form new tuple that will contain only key attributes */
    itupdesc->natts = new_indnatts;
    newitup = index_form_tuple(itupdesc, values, isnull);
    newitup->t_tid = olditup->t_tid;

    FreeTupleDesc(itupdesc);
    Assert(IndexTupleSize(newitup) <= IndexTupleSize(olditup));
    return newitup;
}

/*
 * Truncate tailing attributes from given index tuple leaving it with
 * new_indnatts number of attributes.
 */
IndexTuple UBTreeIndexTruncateTuple(TupleDesc tupleDescriptor, IndexTuple olditup, int leavenatts, bool itup_extended)
{
    TupleDesc itupdesc = CreateTupleDescCopyConstr(tupleDescriptor);
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    IndexTuple newitup;
    int indnatts = tupleDescriptor->natts;

    if (indnatts > INDEX_MAX_KEYS) {
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_COLUMNS),
                        errmsg("number of index columns (%d) exceeds limit (%d)", indnatts, INDEX_MAX_KEYS)));
    }

    Assert(leavenatts > 0);
    Assert(leavenatts <= indnatts);

    /* Easy case: no truncation actually required */
    if (leavenatts == indnatts) {
        if (itup_extended) {
            /* itup_extended indicates that the itup's size is not accurate */
            IndexTuple copiedItup;
            Size oldsz = IndexTupleSize(olditup);
            Size newsz = oldsz - TXNINFOSIZE;

            /* subtract 8B before copy, see _bt_split() for more details */
            IndexTupleSetSize(olditup, newsz);
            copiedItup = CopyIndexTuple(olditup);
            IndexTupleSetSize(olditup, oldsz);

            return copiedItup;
        }
        return CopyIndexTuple(olditup);
    }

    /* Deform, form copy of tuple with fewer attributes */
    index_deform_tuple(olditup, tupleDescriptor, values, isnull);

    /* form new tuple that will contain only key attributes */
    itupdesc->natts = leavenatts;
    newitup = index_form_tuple(itupdesc, values, isnull);
    newitup->t_tid = olditup->t_tid;
    Assert(IndexTupleSize(newitup) <= IndexTupleSize(olditup));

    FreeTupleDesc(itupdesc);

    return newitup;
}

