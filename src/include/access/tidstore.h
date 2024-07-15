/*-------------------------------------------------------------------------
 *
 * tidstore.h
 *      TidStore interface.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tidstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIDSTORE_H
#define TIDSTORE_H

#include "storage/item/itemptr.h"

typedef struct TidStore TidStore;
typedef struct TidStoreIter TidStoreIter;

/* Result struct for TidStoreIterateNext */
typedef struct TidStoreIterResult
{
    BlockNumber blkno;
    int         max_offset;
    int         num_offsets;
    OffsetNumber *offsets;
    int2        bktId;
} TidStoreIterResult;

extern TidStore *TidStoreCreateLocal(size_t max_bytes, bool insert_only);
extern void TidStoreDestroy(TidStore *ts);
extern void TidStoreSetBlockOffsets(TidStore *ts, int2 bktId, BlockNumber blkno,
                                    OffsetNumber *offsets, int num_offsets);
extern bool TidStoreIsMember(TidStore *ts, int2 bktId, ItemPointer tid);
extern TidStoreIter *TidStoreBeginIterate(TidStore *ts);
extern TidStoreIterResult *TidStoreIterateNext(TidStoreIter *iter);
extern void TidStoreEndIterate(TidStoreIter *iter);
extern size_t TidStoreMemoryUsage(TidStore *ts);

#endif                            /* TIDSTORE_H */
