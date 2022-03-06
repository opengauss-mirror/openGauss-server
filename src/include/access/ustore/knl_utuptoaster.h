/* -------------------------------------------------------------------------
 *
 * knl_utuptoaster.h
 * the access interfaces of ustore tuple toaster.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_utuptoaster.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_TUPTOASTER_H
#define KNL_TUPTOASTER_H

#include "access/ustore/knl_utuple.h"

#define EXTERN_UHEAP_TUPLES_PER_PAGE (4)
#define UTOAST_TUPLES_PER_PAGE (4)

/*
 * Find the maximum size of a tuple if there are to be N utuples per page.
 */
#define UHeapMaximumBytesPerTuple(tuplesPerPage)                                                                 \
    MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfUHeapPageHeaderData + (UHEAP_DEFAULT_TOAST_TD_COUNT * sizeof(TD)) + \
        UHEAP_SPECIAL_SIZE + (tuplesPerPage) * sizeof(ItemIdData))) /                                            \
        (tuplesPerPage))

#define UTOAST_TUPLE_THRESHOLD UHeapMaximumBytesPerTuple(UTOAST_TUPLES_PER_PAGE)

#define UTOAST_TUPLE_TARGET UTOAST_TUPLE_THRESHOLD

#define UTOAST_TUPLE_TARGET_MAIN \
    MAXALIGN_DOWN(BLCKSZ -       \
        MAXALIGN(SizeOfUHeapPageHeaderData + (UHEAP_DEFAULT_TOAST_TD_COUNT * sizeof(TD)) + sizeof(ItemIdData)))

#define EXTERN_UHEAP_TUPLE_MAX_SIZE UHeapMaximumBytesPerTuple(EXTERN_UHEAP_TUPLES_PER_PAGE)

#define UTOAST_MAX_CHUNK_SIZE                                                                                   \
    (EXTERN_UHEAP_TUPLE_MAX_SIZE - MAXALIGN(offsetof(UHeapDiskTupleData, data)) - sizeof(Oid) - sizeof(int32) - \
        VARHDRSZ)

#define CHUNK_ID_ATTR 2
#define CHUNK_DATA_ATTR 3

#define ATTR_FIRST 1
#define ATTR_SECOND 2
#define ATTR_THIRD 3

void UHeapToastDelete(Relation relation, UHeapTuple utuple);
UHeapTuple UHeapToastInsertOrUpdate(Relation relation, UHeapTuple newtup, UHeapTuple oldtup, int options);
extern struct varlena *UHeapInternalToastFetchDatum(struct varatt_external toastPointer, Relation toastrel,
    Relation toastidx);
extern struct varlena *UHeapInternalToastFetchDatumSlice(struct varatt_external toast_pointer, Relation toastrel,
    Relation toastidx, int64 sliceoffset, int32 length);
#endif
