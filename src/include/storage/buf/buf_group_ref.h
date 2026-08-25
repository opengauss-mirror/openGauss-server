/* -------------------------------------------------------------------------
 *
 * buf_group_ref.h
 *        Header file for per-group buffer reference counting.
 *
 * IDENTIFICATION
 *        src/include/storage/buf/buf_group_ref.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUF_GROUP_REF_H
#define BUF_GROUP_REF_H

#include "c.h"
#include "utils/atomic.h"

/*
 * Per-group buffer reference counting structure.
 * This is a separate global array for all buffers' group reference counts.
 * Layout: group_ref_counts[group_id][buf_id]
 * Each group's data is allocated on its corresponding NUMA node.
 */
typedef struct GroupRefCounts {
    pg_atomic_uint16** counts;  /* 2D array: [group_id][buf_id] */
    int32 num_groups;           /* Number of groups */
    int32 num_buffers;          /* Number of buffers */
    bool initialized;           /* Whether the structure has been initialized */
} GroupRefCounts;

extern GroupRefCounts g_group_ref_counts;

/*
 * Initialize per-group buffer reference counts.
 * This function should be called after buffer pool initialization.
 */
extern void InitGroupRefCounts(int32 num_buffers);

/*
 * Initialize the cached CPU group and reference count array for a thread.
 * Threads use group 0 unless the caller supplies another NUMA group.
 */
extern void InitWorkerCPUGroup(int32 group = 0);

/*
 * Helper function to get buffer's effective refcount.
 * In group mode, it sums all group counters; otherwise uses global state.
 */
extern uint32 GetBufferRefCount(uint64 buf_state, int32 buf_id);

/*
 * Helper functions to check buffer refcount - replacement for BUF_STATE_GET_REFCOUNT
 */
extern bool IsBufferRefCountZero(uint64 buf_state, int32 buf_id);
extern bool IsBufferRefCountOne(uint64 buf_state, int32 buf_id);
extern bool IsBufferRefCountGreaterThanZero(uint64 buf_state, int32 buf_id);
extern bool IsBufferRefCountNotZero(uint64 buf_state, int32 buf_id);

#endif /* BUF_GROUP_REF_H */