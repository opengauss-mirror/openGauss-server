/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * pagewriter.h
 *        Data struct to store pagewriter thread variables.
 *
 *
 * IDENTIFICATION
 *        src/include/postmaster/pagewriter.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _PAGEWRITER_H
#define _PAGEWRITER_H
#include "storage/buf/buf.h"
#include "storage/lock/lwlock.h"
#include "catalog/pg_control.h"
#include "ddes/dms/ss_aio.h"

#define ENABLE_INCRE_CKPT g_instance.attr.attr_storage.enableIncrementalCheckpoint
#define NEED_CONSIDER_USECOUNT u_sess->attr.attr_storage.enable_candidate_buf_usage_count

#define INIT_CANDIDATE_LIST(L, list, size, xx_head, xx_tail) \
    ((L).cand_buf_list = (list), \
        (L).cand_list_size = (size), \
        (L).head = (xx_head), \
        (L).tail = (xx_tail))

typedef struct PGPROC PGPROC;
typedef struct BufferDesc BufferDesc;
typedef struct CkptSortItem CkptSortItem;

typedef struct ThrdDwCxt {
    char* dw_buf;
    uint16 write_pos;
    volatile int dw_page_idx;      /* -1 means data files have been flushed. */
    bool is_new_relfilenode;
} ThrdDwCxt;

typedef enum CandListType {
    CAND_LIST_NORMAL,
    CAND_LIST_NVM,
    CAND_LIST_SEG
} CandListType;

typedef struct CandidateList {
    Buffer *cand_buf_list;
    volatile int cand_list_size;
    pg_atomic_uint64 head;
    pg_atomic_uint64 tail;
    volatile int buf_id_start;
    int32 next_scan_loc;
    int32 next_scan_ratio_loc;
} CandidateList;

typedef struct PageWriterProc {
    PGPROC* proc;
    /* dirty page queue */
    volatile uint32 start_loc;
    volatile uint32 need_flush_num;
    volatile bool need_flush;

    /* thread double writer cxt */
    ThrdDwCxt thrd_dw_cxt;

    /* thread dirty list */
    CkptSortItem *dirty_buf_list;
    uint32 dirty_list_size;

    /* thread candidate list, main thread store the segment buffer information */
    CandidateList normal_list;
    CandidateList nvm_list;
    CandidateList seg_list;

    /* auxiluary structs for implementing AIO in DSS */
    DSSAioCxt aio_cxt;
    char *aio_buf;
} PageWriterProc;

typedef struct PageWriterProcs {
    PageWriterProc* writer_proc;
    volatile int num;             /* number of pagewriter thread */
    volatile int sub_num;
    pg_atomic_uint32 running_num; /* number of pagewriter thread which flushing dirty page */
    volatile uint32 queue_flush_max;
    volatile uint32 list_flush_max;
} PageWriterProcs;

typedef struct DirtyPageQueueSlot {
    volatile int buffer;
    pg_atomic_uint32 slot_state;
} DirtyPageQueueSlot;

typedef Datum (*incre_ckpt_view_get_data_func)();

const int INCRE_CKPT_VIEW_NAME_LEN = 128;

typedef struct incre_ckpt_view_col {
    char name[INCRE_CKPT_VIEW_NAME_LEN];
    Oid data_type;
    incre_ckpt_view_get_data_func get_val;
} incre_ckpt_view_col;

const int RESTART_POINT_QUEUE_LEN = 40;
/* recovery checkpoint queue */
typedef struct CheckPointItem {
    XLogRecPtr CkptLSN;
    CheckPoint checkpoint;
} CheckPointItem;

typedef struct RecoveryQueueState {
    uint64 start;
    uint64 end;
    CheckPointItem *ckpt_rec_queue;
    LWLock *recovery_queue_lock;
} RecoveryQueueState;

typedef struct {
    SyncRequestType type;   /* request type */
    FileTag ftag;           /* file identifier */
} CheckpointerRequest;

typedef struct IncreCkptSyncShmemStruct {
    ThreadId pagewritermain_pid; /* PID (0 if not started) */
    slock_t sync_lock;          /* protects all the fsync_* fields */
    int64 fsync_start;
    int64 fsync_done;
    LWLock *sync_queue_lwlock;         /* */
    int num_requests;                /* current # of requests */
    int max_requests;                /* allocated array size */
    CheckpointerRequest requests[1]; /* VARIABLE LENGTH ARRAY */
} IncreCkptSyncShmemStruct;

/*
 * The slot location is pre-occupied. When the slot buffer is set, the state will set
 * to valid. when remove dirty page form queue, don't change the state, only when move
 * the dirty page head, need set the slot state is invalid.
 */
const int SLOT_VALID = 1;

extern bool IsPagewriterProcess(void);
extern void incre_ckpt_pagewriter_cxt_init();
extern void ckpt_pagewriter_main(void);

extern bool push_pending_flush_queue(Buffer buffer);
extern void remove_dirty_page_from_queue(BufferDesc* buf);
extern int64 get_dirty_page_num();
extern uint64 get_dirty_page_queue_tail();
extern int get_pagewriter_thread_id(void);
extern bool is_dirty_page_queue_full(BufferDesc* buf);
extern int get_dirty_page_queue_head_buffer();
/* Shutdown all the page writer threads. */
extern void ckpt_shutdown_pagewriter();
extern uint64 get_dirty_page_queue_rec_lsn();
extern XLogRecPtr ckpt_get_min_rec_lsn(void);
extern uint64 get_loc_for_lsn(XLogRecPtr target_lsn);
extern uint64 get_time_ms();

const int PAGEWRITER_VIEW_COL_NUM = 8;
const int INCRE_CKPT_VIEW_COL_NUM = 7;
const int CANDIDATE_VIEW_COL_NUM = 7;

extern const incre_ckpt_view_col g_ckpt_view_col[INCRE_CKPT_VIEW_COL_NUM];
extern const incre_ckpt_view_col g_pagewriter_view_col[PAGEWRITER_VIEW_COL_NUM];
extern const incre_ckpt_view_col g_pagewirter_view_two_col[CANDIDATE_VIEW_COL_NUM];

extern bool candidate_buf_pop(CandidateList *list, int *buf_id);
extern void candidate_buf_init(void);

extern uint32 get_curr_candidate_nums(CandListType type);
extern void PgwrAbsorbFsyncRequests(void);
extern Size PageWriterShmemSize(void);
extern void PageWriterSyncShmemInit(void);
extern void RequestPgwrSync(void);
extern void PageWriterSync(void);
extern bool PgwrForwardSyncRequest(const FileTag *ftag, SyncRequestType type);
extern void PageWriterSyncWithAbsorption(void);

void crps_create_ctxs(knl_thread_role role);
void crps_destory_ctxs();

#endif /* _PAGEWRITER_H */
