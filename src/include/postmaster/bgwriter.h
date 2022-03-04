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
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/knl_usync.h"
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

extern bool CkptForwardSyncRequest(const FileTag *ftag, SyncRequestType type);
extern void CkptAbsorbFsyncRequests(void);

extern Size CheckpointerShmemSize(void);
extern void CheckpointerShmemInit(void);

extern bool FirstCallSinceLastCheckpoint(void);
extern bool IsBgwriterProcess(void);

/* incremental checkpoint bgwriter thread */
const int INCRE_CKPT_BGWRITER_VIEW_COL_NUM = 6;
extern const incre_ckpt_view_col g_bgwriter_view_col[INCRE_CKPT_BGWRITER_VIEW_COL_NUM];

extern void invalid_buffer_bgwriter_main();
extern void ckpt_shutdown_bgwriter();
extern HTAB* relfilenode_hashtbl_create(const char* name, bool use_heap_mem);
extern HTAB *relfilenode_fork_hashtbl_create(const char* name, bool use_heap_mem);

typedef struct DelFileTag {
    RelFileNode rnode;
    int32 maxSegNo;
} DelFileTag;

typedef struct ForkRelFileNode {
    RelFileNode rnode; /* physical relation identifier */
    ForkNumber forkNum;
} ForkRelFileNode;

typedef struct DelForkFileTag {
    ForkRelFileNode forkrnode;
    int32 maxSegNo;
} DelForkFileTag;

#endif /* _BGWRITER_H */

