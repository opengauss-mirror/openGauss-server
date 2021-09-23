/* -------------------------------------------------------------------------
 *
 * datum.c
 *	  openGauss Datum (abstract data type) manipulation routines.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/datum.c
 *
 * -------------------------------------------------------------------------
 */
/*
 * In the implementation of the next routines we assume the following:
 *
 * A) if a type is "byVal" then all the information is stored in the
 * Datum itself (i.e. no pointers involved!). In this case the
 * length of the type is always greater than zero and not more than
 * "sizeof(Datum)"
 *
 * B) if a type is not "byVal" and it has a fixed length (typlen > 0),
 * then the "Datum" always contains a pointer to a stream of bytes.
 * The number of significant bytes are always equal to the typlen.
 *
 * C) if a type is not "byVal" and has typlen == -1,
 * then the "Datum" always points to a "struct varlena".
 * This varlena structure has information about the actual length of this
 * particular instance of the type and about its value.
 *
 * D) if a type is not "byVal" and has typlen == -2,
 * then the "Datum" always points to a null-terminated C string.
 *
 * Note that we do not treat "toasted" datums specially; therefore what
 * will be copied or compared is the compressed data or toast reference.
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/datum.h"
#include "access/tuptoaster.h"
#include "fmgr.h"

/* -------------------------------------------------------------------------
 * datumGetSize
 *
 * Find the "real" size of a datum, given the datum value,
 * whether it is a "by value", and the declared type length.
 *
 * This is essentially an out-of-line version of the att_addlength_datum()
 * macro in access/tupmacs.h.  We do a tad more error checking though.
 * -------------------------------------------------------------------------
 */
Size datumGetSize(Datum value, bool typByVal, int typLen)
{
    Size size;

    if (typByVal) {
        /* Pass-by-value types are always fixed-length */
        Assert(typLen > 0 && (unsigned int)(typLen) <= sizeof(Datum));
        size = (Size)typLen;
    } else {
        if (typLen > 0) {
            /* Fixed-length pass-by-ref type */
            size = (Size)typLen;
        } else if (typLen == -1) {
            /* It is a varlena datatype */
            struct varlena* s = (struct varlena*)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)VARSIZE_ANY(s);
        } else if (typLen == -2) {
            /* It is a cstring datatype */
            char* s = (char*)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)(strlen(s) + 1);
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid typLen: %d", typLen)));
            size = 0; /* keep compiler quiet */
        }
    }

    return size;
}

/* -------------------------------------------------------------------------
 * datumCopyInternal
 *
 * make a copy of a datum
 *
 * If the datatype is pass-by-reference, memory is obtained with palloc().
 * -------------------------------------------------------------------------
 */
Datum datumCopyInternal(Datum value, bool typByVal, int typLen, Size* copySize)
{
    Datum res;

    // *copySize is 0 when the datum is assigned by value, or NULL pointer.
    // otherwise compute the real size and remember it.
    //
    *copySize = 0;

    if (typByVal)
        res = value;
    else {
        Size realSize;
        char* s = NULL;
        errno_t rc;

        if (DatumGetPointer(value) == NULL)
            return PointerGetDatum(NULL);

        realSize = datumGetSize(value, typByVal, typLen);
        s = (char*)palloc(realSize);
        rc = memcpy_s(s, realSize, DatumGetPointer(value), realSize);
        securec_check(rc, "", "");
        res = PointerGetDatum(s);
        *copySize = realSize;
    }
    return res;
}

Datum datumCopy(Datum value, bool typByVal, int typLen)
{
    Size copySize = 0;

    return datumCopyInternal(value, typByVal, typLen, &copySize);
}

/* -------------------------------------------------------------------------
 * datumFree
 *
 * Free the space occupied by a datum CREATED BY "datumCopy"
 *
 * NOTE: DO NOT USE THIS ROUTINE with datums returned by heap_getattr() etc.
 * ONLY datums created by "datumCopy" can be freed!
 * -------------------------------------------------------------------------
 */
#ifdef NOT_USED
void datumFree(Datum value, bool typByVal, int typLen)
{
    if (!typByVal) {
        Pointer s = DatumGetPointer(value);

        pfree_ext(s);
    }
}
#endif

/* -------------------------------------------------------------------------
 * datumIsEqual
 *
 * Return true if two datums are equal, false otherwise
 *
 * NOTE: XXX!
 * We just compare the bytes of the two values, one by one.
 * This routine will return false if there are 2 different
 * representations of the same value (something along the lines
 * of say the representation of zero in one's complement arithmetic).
 * Also, it will probably not give the answer you want if either
 * datum has been "toasted".
 * -------------------------------------------------------------------------
 */
bool datumIsEqual(Datum value1, Datum value2, bool typByVal, int typLen)
{
    bool res = false;

    if (typByVal) {
        /*
         * just compare the two datums. NOTE: just comparing "len" bytes will
         * not do the work, because we do not know how these bytes are aligned
         * inside the "Datum".	We assume instead that any given datatype is
         * consistent about how it fills extraneous bits in the Datum.
         */
        res = (value1 == value2);
    } else {
        Size size1, size2;
        char *s1 = NULL, *s2 = NULL;

        /*
         * Compare the bytes pointed by the pointers stored in the datums.
         */
        size1 = datumGetSize(value1, typByVal, typLen);
        size2 = datumGetSize(value2, typByVal, typLen);
        if (size1 != size2)
            return false;
        s1 = (char*)DatumGetPointer(value1);
        s2 = (char*)DatumGetPointer(value2);
        res = (memcmp(s1, s2, size1) == 0);
    }
    return res;
}

/* -------------------------------------------------------------------------
 * DatumImageEq
 *
 * Compares two datums for identical contents, based on byte images.  Return
 * true if the two datums are equal, false otherwise.
 * -------------------------------------------------------------------------
 */
bool DatumImageEq(Datum value1, Datum value2, bool typByVal, int typLen)
{
    Size len1, len2;
    bool result = true;

    if (typByVal) {
        result = (value1 == value2);
    } else if (typLen > 0) {
        result = (memcmp(DatumGetPointer(value1), DatumGetPointer(value2), typLen) == 0);
    } else if (typLen == -1) {
        len1 = toast_raw_datum_size(value1);
        len2 = toast_raw_datum_size(value2);
        /* No need to de-toast if lengths don't match. */
        if (len1 != len2)
            result = false;
        else {
            struct varlena *arg1val;
            struct varlena *arg2val;

            arg1val = PG_DETOAST_DATUM_PACKED(value1);
            arg2val = PG_DETOAST_DATUM_PACKED(value2);

            result = (memcmp(VARDATA_ANY(arg1val), VARDATA_ANY(arg2val), len1 - VARHDRSZ) == 0);

            /* Only free memory if it's a copy made here. */
            if ((Pointer)arg1val != (Pointer)value1)
                pfree(arg1val);
            if ((Pointer)arg2val != (Pointer)value2)
                pfree(arg2val);
        }
    } else if (typLen == -2) {
        char *s1, *s2;

        /* Compare cstring datums */
        s1 = DatumGetCString(value1);
        s2 = DatumGetCString(value2);
        len1 = strlen(s1) + 1;
        len2 = strlen(s2) + 1;
        if (len1 != len2)
            return false;
        result = (memcmp(s1, s2, len1) == 0);
    } else {
        elog(ERROR, "unexpected typLen: %d", typLen);
    }

    return result;
}
