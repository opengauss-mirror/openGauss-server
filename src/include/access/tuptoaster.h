/* -------------------------------------------------------------------------
 *
 * tuptoaster.h
 *	  openGauss definitions for external and compressed storage
 *	  of variable size attributes.
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/include/access/tuptoaster.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TUPTOASTER_H
#define TUPTOASTER_H

#include "access/htup.h"
#include "utils/relcache.h"

/*
 * This enables de-toasting of index entries.  Needed until VACUUM is
 * smart enough to rebuild indexes from scratch.
 */
#define TOAST_INDEX_HACK

/*
 * Find the maximum size of a tuple if there are to be N tuples per page.
 */
#define MaximumBytesPerTuple(tuplesPerPage) \
    MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + (tuplesPerPage) * sizeof(ItemIdData))) / (tuplesPerPage))

/*
 * These symbols control toaster activation.  If a tuple is larger than
 * TOAST_TUPLE_THRESHOLD, we will try to toast it down to no more than
 * TOAST_TUPLE_TARGET bytes through compressing compressible fields and
 * moving EXTENDED and EXTERNAL data out-of-line.
 *
 * The numbers need not be the same, though they currently are.  It doesn't
 * make sense for TARGET to exceed THRESHOLD, but it could be useful to make
 * it be smaller.
 *
 * Currently we choose both values to match the largest tuple size for which
 * TOAST_TUPLES_PER_PAGE tuples can fit on a heap page.
 *
 * XXX while these can be modified without initdb, some thought needs to be
 * given to needs_toast_table() in toasting.c before unleashing random
 * changes.  Also see LOBLKSIZE in large_object.h, which can *not* be
 * changed without initdb.
 */
#define TOAST_TUPLES_PER_PAGE 4

#define TOAST_TUPLE_THRESHOLD MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE)

#define TOAST_TUPLE_TARGET TOAST_TUPLE_THRESHOLD

/*
 * The code will also consider moving MAIN data out-of-line, but only as a
 * last resort if the previous steps haven't reached the target tuple size.
 * In this phase we use a different target size, currently equal to the
 * largest tuple that will fit on a heap page.	This is reasonable since
 * the user has told us to keep the data in-line if at all possible.
 */
#define TOAST_TUPLES_PER_PAGE_MAIN 1

#define TOAST_TUPLE_TARGET_MAIN MAXALIGN_DOWN(BLCKSZ - MAXALIGN(SizeOfHeapPageHeaderData + sizeof(ItemIdData)))

/*
 * If an index value is larger than TOAST_INDEX_TARGET, we will try to
 * compress it (we can't move it out-of-line, however).  Note that this
 * number is per-datum, not per-tuple, for simplicity in index_form_tuple().
 * Note that 16 is used as the divisor because it seems to work well in most cases.
 */
#define TOAST_INDEX_TARGET (MaxHeapTupleSize / 16) // 509
#define UTOAST_INDEX_TARGET (DefaultTdMaxUHeapTupleSize / 16)

/*
 * When we store an oversize datum externally, we divide it into chunks
 * containing at most TOAST_MAX_CHUNK_SIZE data bytes.	This number *must*
 * be small enough that the completed toast-table tuple (including the
 * ID and sequence fields and all overhead) will fit on a page.
 * The coding here sets the size on the theory that we want to fit
 * EXTERN_TUPLES_PER_PAGE tuples of maximum size onto a page.
 *
 * NB: Changing TOAST_MAX_CHUNK_SIZE requires an initdb.
 */
#define EXTERN_TUPLES_PER_PAGE 4 /* tweak only this */

#define EXTERN_TUPLE_MAX_SIZE MaximumBytesPerTuple(EXTERN_TUPLES_PER_PAGE)

#define TOAST_MAX_CHUNK_SIZE \
    (EXTERN_TUPLE_MAX_SIZE - MAXALIGN(offsetof(HeapTupleHeaderData, t_bits)) - sizeof(Oid) - sizeof(int32) - VARHDRSZ)

/* Size of an EXTERNAL datum that contains a standard TOAST pointer */
#define TOAST_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(struct varatt_external))
#define LARGE_TOAST_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(struct varatt_lob_external))
#define LOB_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(struct varatt_lob_pointer))

/* Size of an indirect datum that contains a standard TOAST pointer */
#define INDIRECT_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(struct varatt_indirect))
/*
 * Testing whether an externally-stored value is compressed now requires
 * comparing extsize (the actual length of the external data) to rawsize
 * (the original uncompressed datum's size).  The latter includes VARHDRSZ
 * overhead, the former doesn't.  We never use compression unless it actually
 * saves space, so we expect either equality or less-than.
 */
#define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
    ((toast_pointer).va_extsize < (toast_pointer).va_rawsize - VARHDRSZ)

#define VARATT_EXTERNAL_GET_HUGE_POINTER(large_toast_pointer, attr) \
do { \
   varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
   errno_t rcs; \
   Assert(VARATT_IS_HUGE_TOAST_POINTER(attre)); \
   if (VARATT_IS_EXTERNAL_BUCKET(attre)) \
   { \
       Assert(VARSIZE_EXTERNAL(attre) == sizeof(large_toast_pointer) + VARHDRSZ_EXTERNAL + sizeof(int2)); \
   }\
   else\
   { \
       Assert(VARSIZE_EXTERNAL(attre) == sizeof(large_toast_pointer) + VARHDRSZ_EXTERNAL); \
   }\
   rcs = memcpy_s(&(large_toast_pointer), sizeof(large_toast_pointer), VARDATA_EXTERNAL(attre), sizeof(large_toast_pointer)); \
   securec_check(rcs, "", ""); \
} while (0)

/*
 * Macro to fetch the possibly-unaligned contents of an EXTERNAL datum
 * into a local "struct varatt_external" toast pointer.  This should be
 * just a memcpy, but some versions of gcc seem to produce broken code
 * that assumes the datum contents are aligned.  Introducing an explicit
 * intermediate "varattrib_1b_e *" variable seems to fix it.
 */
#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr) \
do { \
   varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
   errno_t rcs; \
   Assert(VARATT_IS_EXTERNAL(attre)); \
   if (VARATT_IS_EXTERNAL_BUCKET(attre)) \
   { \
       Assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL + sizeof(int2)); \
   }\
   else\
   { \
       Assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
   }\
   rcs = memcpy_s(&(toast_pointer), sizeof(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
   securec_check(rcs, "", ""); \
} while (0)


#define VARATT_EXTERNAL_GET_POINTER_B(toast_pointer, attr, bucketid) \
do { \
   varattrib_1b_e *_attre = (varattrib_1b_e *) (attr); \
   errno_t _rc; \
   Assert(VARATT_IS_EXTERNAL(_attre)); \
   if (VARATT_IS_EXTERNAL_BUCKET(_attre)) \
   { \
       Assert(VARSIZE_EXTERNAL(_attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL + sizeof(int2)); \
       _rc = memcpy_s(&(toast_pointer), sizeof(toast_pointer), VARDATA_EXTERNAL(_attre), sizeof(toast_pointer)); \
       securec_check(_rc, "", ""); \
       _rc= memcpy_s(&(bucketid), sizeof(int2), VARDATA_EXTERNAL(_attre)+sizeof(toast_pointer), sizeof(int2)); \
       securec_check(_rc, "", ""); \
   } \
   else \
   { \
       Assert(VARSIZE_EXTERNAL(_attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
       _rc = memcpy_s(&(toast_pointer), sizeof(toast_pointer), VARDATA_EXTERNAL(_attre), sizeof(toast_pointer)); \
       securec_check(_rc, "", ""); \
       bucketid = -1;  \
   } \
} while (0)

class ScalarVector;


/* ----------
 * toast_insert_or_update -
 *
 *	Called by heap_insert() and heap_update().
 * ----------
 */
extern HeapTuple toast_insert_or_update(
    Relation rel, HeapTuple newtup, HeapTuple oldtup, int options, Page pageForOldTup, bool allow_update_self = false);

extern Datum toast_save_datum(Relation rel, Datum value, struct varlena* oldexternal, int options);

extern void toast_delete_datum(Relation rel, Datum value, int options, bool allow_update_self = false);
extern void toast_delete_datum_internal(varatt_external toast_pointer, int options, bool allow_update_self, int2 bucketid = InvalidBktId);
extern void toast_huge_delete_datum(Relation rel, Datum value, int options, bool allow_update_self = false);
extern void checkHugeToastPointer(struct varlena *value);

/* ----------
 * toast_delete -
 *
 *	Called by heap_delete().
 * ----------
 */
extern void toast_delete(Relation rel, HeapTuple oldtup, int options);

/* ----------
 * heap_tuple_fetch_attr() -
 *
 *		Fetches an external stored attribute from the toast
 *		relation. Does NOT decompress it, if stored external
 *		in compressed format.
 * ----------
 */
extern struct varlena* heap_tuple_fetch_attr(struct varlena* attr);
extern struct varlena* heap_internal_toast_fetch_datum(struct varatt_external toast_pointer,
    Relation toastrel, Relation toastidx);

/* ----------
 * heap_tuple_untoast_attr() -
 *
 *		Fully detoasts one attribute, fetching and/or decompressing
 *		it as needed.
 * ----------
 */
extern struct varlena* heap_tuple_untoast_attr(struct varlena* attr, ScalarVector *arr = NULL);

/* ----------
 * heap_tuple_untoast_attr_slice() -
 *
 *		Fetches only the specified portion of an attribute.
 *		(Handles all cases for attribute storage)
 * ----------
 */
extern struct varlena* heap_tuple_untoast_attr_slice(struct varlena* attr, int64 sliceoffset, int32 slicelength);

/* ----------
 * toast_flatten_tuple -
 *
 *	"Flatten" a tuple to contain no out-of-line toasted fields.
 *	(This does not eliminate compressed or short-header datums.)
 * ----------
 */
extern HeapTuple toast_flatten_tuple(HeapTuple tup, TupleDesc tupleDesc);

/* ----------
 * toast_flatten_tuple_attribute -
 *
 *	If a Datum is of composite type, "flatten" it to contain no toasted fields.
 *	This must be invoked on any potentially-composite field that is to be
 *	inserted into a tuple.	Doing this preserves the invariant that toasting
 *	goes only one level deep in a tuple.
 * ----------
 */
extern Datum toast_flatten_tuple_attribute(Datum value, Oid typeId, int32 typeMod);
extern text* text_catenate_huge(text* t1, text* t2, Oid toastOid);
extern int64 calculate_huge_length(text* t);

/* ----------
 * toast_compress_datum -
 *
 *	Create a compressed version of a varlena datum, if possible
 * ----------
 */
extern Datum toast_compress_datum(Datum value);

/* ----------
 * toast_raw_datum_size -
 *
 *	Return the raw (detoasted) size of a varlena datum
 * ----------
 */
extern Size toast_raw_datum_size(Datum value);

/* ----------
 * toast_datum_size -
 *
 *	Return the storage size of a varlena datum
 * ----------
 */
extern Size toast_datum_size(Datum value);

extern bool toastrel_valueid_exists(Relation toastrel, Oid valueid);

extern bool create_toast_by_sid(Oid *toastOid);
extern Oid get_toast_oid();
extern varlena* toast_huge_write_datum_slice(struct varlena* attr1, struct varlena* attr2, int64 sliceoffset, int32 length);
extern varlena* toast_pointer_fetch_data(TupleTableSlot* varSlot, Form_pg_attribute attr, int varNumber);
extern Datum fetch_lob_value_from_tuple(varatt_lob_pointer* lob_pointer, Oid update_oid, bool* is_null);

inline Datum fetch_real_lob_if_need(Datum toast_pointer)
{
    Datum ret = toast_pointer;
    if (VARATT_IS_EXTERNAL_LOB(toast_pointer)) {
        bool isNull = false;
        struct varatt_lob_pointer* lob_pointer = (varatt_lob_pointer*)(VARDATA_EXTERNAL(toast_pointer));
        ret = fetch_lob_value_from_tuple(lob_pointer, InvalidOid, &isNull);
        if (unlikely(isNull)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid lob pointer.")));
        }
    }
    return ret;
}

extern HeapTuple ctid_get_tuple(Relation relation, ItemPointer tid);
extern struct varlena* toast_fetch_datum(struct varlena* attr);
#endif /* TUPTOASTER_H */

