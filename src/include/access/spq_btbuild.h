/* -------------------------------------------------------------------------
 *
 * spq_btbuild.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/spq_btbuild.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifdef USE_SPQ
#ifndef SPQ_BTBUILD_H
#define SPQ_BTBUILD_H
 
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "utils/portal.h"
 
#define NANOSECONDS_PER_MILLISECOND 1000000L
#define NANOSECONDS_PER_SECOND 1000000000L
 
#define SPQ_BATCH_SIZE (u_sess->attr.attr_spq.spq_batch_size)
#define SPQ_MEM_SIZE (u_sess->attr.attr_spq.spq_mem_size)
#define SPQ_QUEUE_SIZE (u_sess->attr.attr_spq.spq_queue_size)
 
#define ENABLE_SPQ (u_sess->attr.attr_spq.spq_enable_btbuild)
 
#define GET_IDX(i) ((i + 1) % SPQ_QUEUE_SIZE)
 
#define SPQ_BUFFER_SIZE \
    (sizeof(IndexTupleBuffer) + sizeof(Size) * SPQ_BATCH_SIZE + sizeof(char) * SPQ_MEM_SIZE * (INDEX_MAX_KEYS + 1))
 
#define SPQ_SHARED_SIZE SPQ_BUFFER_SIZE *SPQ_QUEUE_SIZE
 
#define GET_BUFFER(SPQ_SHARED, INDEX) ((IndexTupleBuffer *)((char *)SPQ_SHARED->addr + SPQ_BUFFER_SIZE * INDEX))
 
#define GET_BUFFER_MEM(ITUPLE) ((char *)ITUPLE->addr + sizeof(Size) * SPQ_BATCH_SIZE)
 
typedef struct SPQSharedContext {
    /*
     * These fields are not modified during the sort.  They primarily exist
     * for the benefit of worker processes that need to create BTSpool state
     * corresponding to that used by the leader.
     */
    Oid heaprelid;
    Oid indexrelid;
    bool isunique;
    bool isconcurrent;
 
    slock_t mutex;
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
 
    volatile bool done;     /* flag if all tuples have been fetched */
    volatile int bufferidx; /* buffer index */
    volatile int consumer;  /* buffer consume */
    volatile int producer;  /* buffer produce */
    char addr[0];           /* varlen */
    Snapshot snapshot;
    int dop;
    int num_nodes;
} SPQSharedContext;
 
typedef struct IndexTupleBuffer {
    volatile int queue_size; /* the number of index tuples in this buffer */
    volatile int idx;        /* current tuple index in this buffer */
    volatile int offset;     /* memory offset in this buffer */
    Size addr[0];            /* varlen offset: ituple + mem */
} IndexTupleBuffer;
 
typedef struct SPQLeaderState {
    Relation heap;
    Relation index;
    IndexTupleBuffer *buffer;
    SPQSharedContext *shared;
    double processed;
    Snapshot snapshot;
} SPQLeaderState;
 
typedef struct SPQWorkerState {
    SPIPlanPtr plan;
    Portal portal;
    StringInfo sql;
 
    Relation heap;
    Relation index;
    SPQSharedContext *shared;
    uint64 processed; /* location from the last produce buffer */
    bool all_fetched; /* flag if worker has managed all tuples */
} SPQWorkerState;
 
Datum spqbtbuild(Relation heap, Relation index, IndexInfo *indexInfo);
 
IndexTuple spq_consume(SPQLeaderState *spqleader);
 
bool enable_spq_btbuild(Relation rel);

bool enable_spq_btbuild_cic(Relation rel);

#endif  // SPQ_BTBUILD_H
#endif
