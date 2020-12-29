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
 * double_write.h
 *        Define some inline function of double write and export some interfaces.
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/double_write.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DOUBLE_WRITE_H
#define DOUBLE_WRITE_H

#include "double_write_basic.h"
#include "storage/buf/buf_internals.h"
#include "storage/checksum_impl.h"

typedef enum BufTagVer {
    ORIGIN_TAG = 0,
    HASHBUCKET_TAG
} BufTagVer;

typedef struct st_dw_batch {
    dw_page_head_t head;
    uint16 page_num; /* for batch head, number of data pages */
    uint16 buftag_ver;
    BufferTag buf_tag[0]; /* to locate the data pages in batch */
} dw_batch_t;

typedef struct st_dw_batch_nohbkt {
    dw_page_head_t head;
    uint16 page_num; /* for batch head, number of data pages */
    uint16 buftag_ver;
    BufferTagFirstVer buf_tag[0]; /* to locate the data pages in batch */
} dw_batch_first_ver;

typedef struct dw_single_flush_item {
    uint16 data_page_idx;     /* from zero start, indicates the slot of the data page. */
    uint16 dwn;             /* double write number, updated when file header changed */
    BufferTag buf_tag;
    pg_crc32c crc;         /* CRC of all above ... MUST BE LAST! */
}dw_single_flush_item;

/* Used by double_write to mark the buffers which are not flushed in the given buf_id array. */
static const int DW_INVALID_BUFFER_ID = -1;
/* steal high bit from pagenum as the flag of hashbucket */
#define IS_HASH_BKT_MASK (0x8000)
#define GET_REL_PGAENUM(pagenum) (pagenum & ~IS_HASH_BKT_MASK)

/**
 * Dirty data pages in one batch
 * The number of data pages depends on the number of BufferTag one page can hold
 */
static const uint16 DW_BATCH_DATA_PAGE_MAX =
    (uint16)((BLCKSZ - sizeof(dw_batch_t) - sizeof(dw_page_tail_t)) / sizeof(BufferTag));

static const uint16 DW_BATCH_DATA_PAGE_MAX_FOR_NOHBK =
    (uint16)((BLCKSZ - sizeof(dw_batch_first_ver) - sizeof(dw_page_tail_t)) / sizeof(BufferTagFirstVer));


/* 1 head + data + 1 tail */
static const uint16 DW_EXTRA_FOR_ONE_BATCH = 2;

/* 1 head + data + [1 tail, 2 head] + data + 2 tail */
static const uint16 DW_EXTRA_FOR_TWO_BATCH = 3;

static const uint16 DW_BATCH_MIN = (1 + DW_EXTRA_FOR_ONE_BATCH);

static const uint16 DW_BATCH_MAX = (DW_BATCH_DATA_PAGE_MAX + DW_EXTRA_FOR_ONE_BATCH);

/* 2 batches at most for one perform */
static const uint16 DW_DIRTY_PAGE_MAX = (DW_BATCH_DATA_PAGE_MAX + DW_BATCH_DATA_PAGE_MAX);

static const uint16 DW_BUF_MAX = (DW_DIRTY_PAGE_MAX + DW_EXTRA_FOR_TWO_BATCH);

static const uint16 DW_BATCH_MAX_FOR_NOHBK = (DW_BATCH_DATA_PAGE_MAX_FOR_NOHBK + DW_EXTRA_FOR_ONE_BATCH);

/* 2 batches at most for one perform */
static const uint16 DW_DIRTY_PAGE_MAX_FOR_NOHBK = (DW_BATCH_DATA_PAGE_MAX_FOR_NOHBK + DW_BATCH_DATA_PAGE_MAX_FOR_NOHBK);

static const uint16 DW_BUF_MAX_FOR_NOHBK = (DW_DIRTY_PAGE_MAX_FOR_NOHBK + DW_EXTRA_FOR_TWO_BATCH);


#define GET_DW_BATCH_DATA_PAGE_MAX(contain_hashbucket) (!contain_hashbucket ? DW_BATCH_DATA_PAGE_MAX_FOR_NOHBK : DW_BATCH_DATA_PAGE_MAX)

#define GET_DW_BATCH_MAX(contain_hashbucket) (!contain_hashbucket ? DW_BATCH_MAX_FOR_NOHBK : DW_BATCH_MAX)


#define GET_DW_DIRTY_PAGE_MAX(contain_hashbucket) (!contain_hashbucket ? DW_DIRTY_PAGE_MAX_FOR_NOHBK : DW_DIRTY_PAGE_MAX)

#define GET_DW_MEM_CTX_MAX_BLOCK_SIZE(contain_hashbucket) (!contain_hashbucket ? DW_MEM_CTX_MAX_BLOCK_SIZE_FOR_NOHBK : DW_MEM_CTX_MAX_BLOCK_SIZE)

/*
 * 1 block for alignment, 1 for file_head, 1 for reading data_page during recovery
 * and DW_BUF_MAX for double_write buffer.
 */
static const uint32 DW_MEM_CTX_MAX_BLOCK_SIZE = ((1 + 1 + 1 + DW_BUF_MAX) * BLCKSZ);

static const uint32 DW_MEM_CTX_MAX_BLOCK_SIZE_FOR_NOHBK = ((1 + 1 + 1 + DW_BUF_MAX_FOR_NOHBK) * BLCKSZ);

const uint16 SINGLE_BLOCK_TAG_NUM = BLCKSZ / sizeof(dw_single_flush_item);

static const uint32 DW_BOOTSTRAP_VERSION = 91261;
const uint32 DW_SUPPORT_SINGLE_FLUSH_VERSION = 92266;

/* dw single flush file information */
/* file head + storage buffer tag page + data page */
const int DW_SINGLE_FILE_SIZE = (1 + 161 + 32768) * 8192;

/* Reserve 8 bytes for bufferTag upgrade. now usepage num is 32768 * sizeof(dw_single_flush_item) / 8192 */
const int DW_SINGLE_BUFTAG_PAGE_NUM = 161;
const int DW_SINGLE_DIRTY_PAGE_NUM = 32768;

inline bool dw_buf_valid_dirty(uint32 buf_state)
{
    return ((buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY));
}

inline bool dw_buf_ckpt_needed(uint32 buf_state)
{
    return ((buf_state & (BM_VALID | BM_DIRTY | BM_CHECKPOINT_NEEDED)) == (BM_VALID | BM_DIRTY | BM_CHECKPOINT_NEEDED));
}

inline bool dw_verify_file_head_checksum(dw_file_head_t* file_head)
{
    uint32 checksum;
    uint16 org_cks = file_head->tail.checksum;

    file_head->tail.checksum = 0;
    checksum = pg_checksum_block((char*)file_head, sizeof(dw_file_head_t));
    file_head->tail.checksum = org_cks;

    return (org_cks == REDUCE_CKS2UINT16(checksum));
}

inline bool dw_verify_file_head(dw_file_head_t* file_head)
{
    return file_head->head.dwn == file_head->tail.dwn && dw_verify_file_head_checksum(file_head);
}

inline void dw_calc_file_head_checksum(dw_file_head_t* file_head)
{
    uint32 checksum;
    file_head->tail.checksum = 0;
    checksum = pg_checksum_block((char*)file_head, sizeof(dw_file_head_t));
    file_head->tail.checksum = REDUCE_CKS2UINT16(checksum);
}

inline bool dw_verify_batch_checksum(dw_batch_t* batch)
{
    uint32 checksum;
    uint16 org_cks = DW_PAGE_CHECKSUM(batch);

    DW_PAGE_CHECKSUM(batch) = 0;
    checksum = pg_checksum_block((char*)batch, BLCKSZ);
    DW_PAGE_CHECKSUM(batch) = org_cks;

    return (org_cks == REDUCE_CKS2UINT16(checksum));
}

inline bool dw_verify_page(dw_batch_t* page)
{
    return (page)->head.dwn == DW_PAGE_TAIL(page)->dwn && dw_verify_batch_checksum(page);
}

inline void dw_calc_batch_checksum(dw_batch_t* batch)
{
    uint32 checksum;

    DW_PAGE_CHECKSUM(batch) = 0;
    checksum = pg_checksum_block((char*)batch, BLCKSZ);
    DW_PAGE_CHECKSUM(batch) = REDUCE_CKS2UINT16(checksum);
}

inline dw_batch_t* dw_batch_tail_page(dw_batch_t* head_page)
{
    return (dw_batch_t*)((char*)head_page + BLCKSZ * (GET_REL_PGAENUM(head_page->page_num) + 1));
}

/**
 * verify the batch head and tail page, including dwn and checksum
 * @param head_page batch head
 * @param dwn double write number
 * @return true dwn and checksum match
 */
inline bool dw_verify_batch(dw_batch_t* head_page, uint16 dwn)
{
    if (head_page->head.dwn == dwn && dw_verify_page(head_page)) {
        dw_batch_t* tail_page = dw_batch_tail_page(head_page);
        return tail_page->head.dwn == dwn && dw_verify_page(tail_page);
    }

    return false;
}

inline uint64 dw_page_distance(void* left, void* right)
{
    return ((char*)right - (char*)left) / BLCKSZ;
}

int64 dw_seek_file(int fd, int64 offset, int32 origin);

void dw_pread_file(int fd, void* buf, int size, int64 offset);

void dw_pwrite_file(int fd, const void* buf, int size, int64 offset);

/**
 * generate the file for the database first boot
 */
void dw_bootstrap();

/**
 * do the memory allocate, spin_lock init, LWLock assign and double write recovery
 * all the half-written pages should be recovered after this
 * it should be finished before XLOG module start which may replay redo log
 */
void dw_init(bool shutdown);

/**
 * double write only work when incremental checkpoint enabled and double write enabled
 * @return true if both enabled
 */
inline bool dw_enabled()
{
    return (
        g_instance.attr.attr_storage.enableIncrementalCheckpoint && g_instance.attr.attr_storage.enable_double_write);
}

/**
 * flush the buffers identified by the buf_id in buf_id_arr to double write file
 * a token_id is returned, thus double write wish the caller to return it after the
 * caller finish flushing the buffers to data file and forwarding the fsync request
 * @param buf_id_arr the buffer id array which is used to get page from global buffer
 * @param size the array size
 */
void dw_perform_batch_flush(uint32 size, CkptSortItem *dirty_buf_list, ThrdDwCxt* thrd_dw_cxt);

/**
 * truncate the pages in double write file after ckpt or before exit
 * wait for tokens, thus all the relative data file flush and fsync request forwarded
 * then its safe to call fsync to make sure pages on data file
 * and then safe to discard those pages on double write file
 */
void dw_truncate();

/**
 * double write exit after XLOG exit.
 * data file flushing, page writer and checkpointer thread may still running. wait for them.
 */
void dw_exit(bool single);

/**
 * If double write is enabled and pagewriter is running,
 * the dirty pages should only be flushed by pagewriter.
 */
inline bool dw_page_writer_running()
{
    return (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0);
}

extern uint16 dw_single_flush(BufferDesc *buf_desc);
extern bool dw_single_file_recycle(bool trunc_file);
extern bool backend_can_flush_dirty_page();
extern void dw_force_reset_single_file();
extern void reset_dw_pos_flag();
extern void clean_proc_dw_buf();
extern void init_proc_dw_buf();
#endif /* DOUBLE_WRITE_H */
