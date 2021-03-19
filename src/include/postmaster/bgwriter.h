/* -------------------------------------------------------------------------
 *
 * bgwriter.h
 *	  Exports from postmaster/bgwriter.c and postmaster/checkpointer.c.
 *
 * The bgwriter process used to handle checkpointing duties too.  Now
 * there is a separate process, but we did not bother to split this header.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/include/postmaster/bgwriter.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _BGWRITER_H
#define _BGWRITER_H

#include "storage/buf/block.h"
#include "storage/relfilenode.h"
#include "postmaster/pagewriter.h"

typedef struct CkptSortItem CkptSortItem;

#ifdef ENABLE_MOT
typedef enum {
    EVENT_CHECKPOINT_CREATE_SNAPSHOT,
    EVENT_CHECKPOINT_SNAPSHOT_READY,
    EVENT_CHECKPOINT_BEGIN_CHECKPOINT,
    EVENT_CHECKPOINT_ABORT
} CheckpointEvent;

typedef void (*CheckpointCallback)(CheckpointEvent checkpointEvent, XLogRecPtr lsn, void* arg);

extern void RegisterCheckpointCallback(CheckpointCallback callback, void* arg);
extern void CallCheckpointCallback(CheckpointEvent checkpointEvent, XLogRecPtr lsn);
#endif

extern void BackgroundWriterMain(void);
extern void CheckpointerMain(void);

extern void RequestCheckpoint(int flags);
extern void CheckpointWriteDelay(int flags, double progress);

extern bool ForwardFsyncRequest(const RelFileNode rnode, ForkNumber forknum, BlockNumber segno);
extern void AbsorbFsyncRequests(void);

extern Size CheckpointerShmemSize(void);
extern void CheckpointerShmemInit(void);

extern bool FirstCallSinceLastCheckpoint(void);
extern bool IsBgwriterProcess(void);

/* incremental checkpoint bgwriter thread */
const int INCRE_CKPT_BGWRITER_VIEW_COL_NUM = 6;
extern const incre_ckpt_view_col g_bgwriter_view_col[INCRE_CKPT_BGWRITER_VIEW_COL_NUM];
extern void candidate_buf_init(void);
extern void incre_ckpt_bgwriter_cxt_init();
extern void incre_ckpt_background_writer_main(void);
extern void ckpt_shutdown_bgwriter();
extern int get_bgwriter_thread_id(void);
extern bool candidate_buf_pop(int *bufId, int threadId);
extern uint32 get_curr_candidate_nums(void);

typedef struct BgWriterProc {
    PGPROC *proc;
    CkptSortItem *dirty_buf_list;
    uint32 dirty_list_size;
    int *cand_buf_list;   /* thread candidate buffer list */
    volatile int cand_list_size;     /* thread candidate list max size */
    volatile int buf_id_start;     /* buffer id start loc */
    pg_atomic_uint64 head;
    pg_atomic_uint64 tail;
    bool need_flush;
    ThrdDwCxt thrd_dw_cxt;         /* thread double writer cxt */
    volatile uint32 thread_last_flush;
    int32 next_scan_loc;
} BgWriterProc;
#endif /* _BGWRITER_H */

