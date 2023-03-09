/* -------------------------------------------------------------------------
 *
 * tidbitmap.h
 *	  PostgreSQL tuple-id (TID) bitmap package
 *
 * This module provides bitmap data structures that are spiritually
 * similar to Bitmapsets, but are specially adapted to store sets of
 * tuple identifiers (TIDs), or ItemPointers.  In particular, the division
 * of an ItemPointer into BlockNumber and OffsetNumber is catered for.
 * Also, since we wish to be able to store very large tuple sets in
 * memory with this data structure, we support "lossy" storage, in which
 * we no longer remember individual tuple offsets on a page but only the
 * fact that a particular page needs to be visited.
 *
 *
 * Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 * src/include/nodes/tidbitmap.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TIDBITMAP_H
#define TIDBITMAP_H

#include "storage/item/itemptr.h"

/*
 * Actual bitmap representation is private to tidbitmap.c.	Callers can
 * do IsA(x, TIDBitmap) on it, but nothing else.
 */

typedef struct TIDBitmap TIDBitmap;
/* Likewise, TBMIterator is private */
typedef struct TBMIterator TBMIterator;

/* Result structure for tbm_iterate */
typedef struct {
    BlockNumber blockno; /* page number containing tuples */
    Oid partitionOid;
    int2 bucketid;
    int ntuples;         /* -1 indicates lossy result */
    bool recheck;        /* should the tuples be rechecked? */
    /* Note: recheck is always true if ntuples < 0 */
    OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} TBMIterateResult;

/*
 * We want the caller to choose between their own best hash between
 * dynamic hash and a more cache-friendly simple simple hash table.
 * Therefore a set of handler is required to avoid all kinds of
 * unnecessary branches inside this performance-critical area.
 * 
 * All handlers defined here can be templated base on the hash
 * table the caller used.And the caller can invoke the handler
 * with little to no overheads.
 * 
 * Most of external use of tbm related functions are exposed by
 * this handler interface.Some of others like tbm_oterate does
 * not templated like handlers are not included.
 */
typedef struct TMBHandler {
    /* page generic handlers */
    void (*_add_tuples)(TIDBitmap*, const ItemPointer, int, bool, Oid, int2);
    void (*_add_page)(TIDBitmap*, BlockNumber, Oid, int2);

    /* page operator handlers */
    void (*_union)(TIDBitmap*, const TIDBitmap*);
    void (*_intersect)(TIDBitmap*, const TIDBitmap*);

    /* iterator handlers */
    TBMIterator* (*_begin_iterate)(TIDBitmap*);
} TBMHandler;

/* function prototypes in nodes/tidbitmap.c */
extern TIDBitmap* tbm_create(long maxbytes, bool is_global_part = false, bool is_crossbucket = false, bool is_partitioned = false, bool is_ustore = false);
extern void tbm_free(TIDBitmap* tbm);
extern long tbm_calculate_entries(double maxbytes, bool complex_key);

/* iterator prototypes in nodes/tidbitmap.c */
extern TBMIterateResult* tbm_iterate(TBMIterator* iterator);
extern void tbm_end_iterate(TBMIterator* iterator);

/* function prototypes for TIDBitmap member checks */
extern void tbm_set_global(TIDBitmap* tbm, bool val);
extern bool tbm_is_global(const TIDBitmap* tbm);
extern bool tbm_is_empty(const TIDBitmap* tbm);
extern bool tbm_is_crossbucket(const TIDBitmap* tbm);
extern TBMHandler tbm_get_handler(TIDBitmap* tbm);

#endif /* TIDBITMAP_H */
