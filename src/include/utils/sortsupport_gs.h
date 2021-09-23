/* ---------------------------------------------------------------------------------------
 * 
 * sortsupport_gs.h
 *        Framework for accelerated sorting.
 * 
 * Traditionally, openGauss has implemented sorting by repeatedly invoking
 * an SQL-callable comparison function "cmp(x, y) returns int" on pairs of
 * values to be compared, where the comparison function is the BTORDER_PROC
 * pg_amproc support function of the appropriate btree index opclass.
 *
 * This file defines alternative APIs that allow sorting to be performed with
 * reduced overhead.  To support lower-overhead sorting, a btree opclass may
 * provide a BTSORTSUPPORT_PROC pg_amproc entry, which must take a single
 * argument of type internal and return void.  The argument is actually a
 * pointer to a SortSupportData struct, which is defined below.
 *
 * If provided, the BTSORTSUPPORT function will be called during sort setup,
 * and it must initialize the provided struct with pointers to function(s)
 * that can be called to perform sorting.  This API is defined to allow
 * multiple acceleration mechanisms to be supported, but no opclass is
 * required to provide all of them.  The BTSORTSUPPORT function should
 * simply not set any function pointers for mechanisms it doesn't support.
 * (However, all opclasses that provide BTSORTSUPPORT are required to provide
 * the comparator function.)
 *
 * All sort support functions will be passed the address of the
 * SortSupportData struct when called, so they can use it to store
 * additional private data as needed.  In particular, for collation-aware
 * datatypes, the ssup_collation field is set before calling BTSORTSUPPORT
 * and is available to all support functions.  Additional opclass-dependent
 * data can be stored using the ssup_extra field.  Any such data
 * should be allocated in the ssup_cxt memory context.
 *
 * Note: since pg_amproc functions are indexed by (lefttype, righttype)
 * it is possible to associate a BTSORTSUPPORT function with a cross-type
 * comparison.	This could sensibly be used to provide a fast comparator
 * function for such cases, but probably not any other acceleration method.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/utils/sortsupport_gs.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SORTSUPPORT_GS_H
#define SORTSUPPORT_GS_H

#include "access/attnum.h"

/* ApplySortComparator should be inlined if possible */
#ifdef USE_INLINE

/*
 * Apply a sort comparator function and return a 3-way comparison using full,
 * authoritative comparator.  This takes care of handling reverse-sort and
 * NULLs-ordering properly.
 */
static inline int ApplySortAbbrevFullComparator(
    Datum datum1, bool isNull1, Datum datum2, bool isNull2, SortSupport ssup)
{
    int compare;

    if (isNull1) {
        if (isNull2)
            compare = 0; /* NULL "=" NULL */
        else if (ssup->ssup_nulls_first)
            compare = -1; /* NULL "<" NOT_NULL */
        else
            compare = 1; /* NULL ">" NOT_NULL */
    } else if (isNull2) {
        if (ssup->ssup_nulls_first)
            compare = 1; /* NOT_NULL ">" NULL */
        else
            compare = -1; /* NOT_NULL "<" NULL */
    } else {
        compare = (*ssup->abbrev_full_comparator)(datum1, datum2, ssup);
        if (ssup->ssup_reverse)
            compare = -compare;
    }

    return compare;
}
#else
extern int ApplySortAbbrevFullComparator(Datum datum1, bool isNull1, Datum datum2, bool isNull2, SortSupport ssup);
#endif /* USE_INLINE */

#endif /* SORTSUPPORT_GS_H */