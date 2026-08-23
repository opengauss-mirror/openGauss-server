/* -------------------------------------------------------------------------
 *
 * hio.h
 *	  openGauss heap access method input/output definitions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/hio.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HIO_H
#define HIO_H

#include "access/heapam.h"
#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufmgr.h"

/*
 * state for bulk inserts --- private to heapam.c and hio.c
 *
 * If current_buf isn't InvalidBuffer, then we are holding an extra pin
 * on that buffer.
 *
 * "typedef struct BulkInsertStateData *BulkInsertState" is in heapam.h
 */
typedef struct BulkInsertStateData {
    BufferAccessStrategy strategy; /* our BULKWRITE strategy object */
    Buffer current_buf;            /* current insertion target page */
    /*
     * State for bulk extensions.
     *
     * last_free..next_free are further pages that were unused at the time of
     * the last extension. They might be in use by the time we use them
     * though, so rechecks are needed.
     *
     * XXX: Eventually these should probably live in RelationData instead,
     * alongside targetblock.
     *
     * already_extended_by remembers the size of the most recent contended
     * extension. It may guide another contended extension, but must not make
     * later uncontended extensions allocate speculative pages.
     */
    BlockNumber next_free;
    BlockNumber last_free;
    uint32 already_extended_by;
} BulkInsertStateData;

extern void RelationPutHeapTuple(Relation relation, Buffer buffer, HeapTuple tuple, TransactionId xid);
extern Buffer RelationGetBufferForTuple(Relation relation, Size len, Buffer otherBuffer, int options,
    BulkInsertState bistate, Buffer* vmbuffer, Buffer* vmbuffer_other, BlockNumber end_rel_block, int num_pages = 0);
extern Buffer RelationGetNewBufferForBulkInsert(Relation relation, Size len, Size dictSize, BulkInsertState bistate);
extern Buffer ReadBufferBI(Relation relation, BlockNumber targetBlock, ReadBufferMode mode, BulkInsertState bistate);
extern void RelationAddExtraBlocks(Relation relation, BulkInsertState bistate);

#endif /* HIO_H */
