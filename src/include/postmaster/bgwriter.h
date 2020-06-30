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

#include "storage/block.h"
#include "storage/relfilenode.h"

typedef enum {
    EVENT_CHECKPOINT_CREATE_SNAPSHOT,
    EVENT_CHECKPOINT_SNAPSHOT_READY,
    EVENT_CHECKPOINT_BEGIN_CHECKPOINT,
    EVENT_CHECKPOINT_ABORT
} CheckpointEvent;

typedef void (*CheckpointCallback)(CheckpointEvent checkpointEvent, XLogRecPtr lsn, void* arg);

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

extern void RegisterCheckpointCallback(CheckpointCallback callback, void* arg);
extern void CallCheckpointCallback(CheckpointEvent checkpointEvent, XLogRecPtr lsn);

#endif /* _BGWRITER_H */
