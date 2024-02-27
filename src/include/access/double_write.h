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
    HASHBUCKET_TAG,
    PAGE_COMPRESS_TAG
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

typedef struct dw_single_first_flush_item {
    uint16 dwn;             /* double write number, updated when file header changed */
    BufferTag buf_tag;
}dw_first_flush_item;


typedef struct dw_single_flush_item {
    uint16 data_page_idx;     /* from zero start, indicates the slot of the data page. */
    uint16 dwn;             /* double write number, updated when file header changed */
    BufferTag buf_tag;
    pg_crc32c crc;         /* CRC of all above ... MUST BE LAST! */
}dw_single_flush_item;

/* Used by double_write to mark the buffers which are not flushed in the given buf_id array. */
static const int DW_INVALID_BUFFER_ID = -1;
/* steal high bit from pagenum as the flag of hashbucket or segpage */
#define IS_HASH_BKT_SEGPAGE_MASK (0x8000)
#define GET_REL_PGAENUM(pagenum) (pagenum & ~IS_HASH_BKT_SEGPAGE_MASK)

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
const uint32 DW_SUPPORT_NEW_SINGLE_FLUSH = 92433;
const uint32 DW_SUPPORT_MULTIFILE_FLUSH = 92568;
const uint32 DW_SUPPORT_BCM_VERSION = 92550;
const uint32 DW_SUPPORT_REABLE_DOUBLE_WRITE = 92590;


/* dw single flush file information, version is DW_SUPPORT_SINGLE_FLUSH_VERSION */
/* file head + storage buffer tag page + data page */
const int DW_SINGLE_FILE_SIZE = (1 + 161 + 32768) * BLCKSZ;

/* Reserve 8 bytes for bufferTag upgrade. now usepage num is 32768 * sizeof(dw_single_flush_item) / 8192 */
const int DW_SINGLE_BUFTAG_PAGE_NUM = 161;
const int DW_SINGLE_DIRTY_PAGE_NUM = 32768;


/* new dw single flush file, version is DW_SUPPORT_NEW_SINGLE_FLUSH */
/* file head + first version data page + file head + storage buffer tag page + second version data page */
const uint32 DW_NEW_SINGLE_FILE_SIZE = (32768 * BLCKSZ);
const uint16 DW_SECOND_BUFTAG_PAGE_NUM = 4;
const uint16 DW_SECOND_DATA_PAGE_NUM = (SINGLE_BLOCK_TAG_NUM * DW_SECOND_BUFTAG_PAGE_NUM);
const uint16 DW_FIRST_DATA_PAGE_NUM = (32768 - DW_SECOND_DATA_PAGE_NUM - DW_SECOND_BUFTAG_PAGE_NUM - 2);
const uint16 DW_SECOND_BUFTAG_START_IDX = 1 + DW_FIRST_DATA_PAGE_NUM + 1; /* two head */
const uint16 DW_SECOND_DATA_START_IDX = DW_SECOND_BUFTAG_START_IDX + DW_SECOND_BUFTAG_PAGE_NUM;

inline bool dw_buf_valid_dirty(uint64 buf_state)
{
    if (ENABLE_DMS && ENABLE_DSS_AIO) {
        return true;
    }

    return ((buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY));
}

inline bool dw_buf_ckpt_needed(uint64 buf_state)
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

inline void dw_calc_meta_checksum(dw_batch_meta_file* meta)
{
    uint32 checksum;
    meta->checksum = 0;
    checksum = pg_checksum_block((char*)meta, sizeof(dw_batch_meta_file));
    meta->checksum = REDUCE_CKS2UINT16(checksum);
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

void dw_pwrite_file(int fd, const void* buf, int size, int64 offset, const char* fileName);

/**
 * generate the file for the database first boot
 */
void dw_bootstrap();

/**
 * do the memory allocate, spin_lock init, LWLock assign and double write recovery
 * all the half-written pages should be recovered after this
 * it should be finished before XLOG module start which may replay redo log
 */
void dw_init();
void dw_ext_init();

/**
 * double write only work when incremental checkpoint enabled and double write enabled
 * @return true if both enabled
 */
inline bool dw_enabled()
{
    return (ENABLE_INCRE_CKPT && g_instance.attr.attr_storage.enable_double_write);
}

/**
 * flush the buffers identified by the buf_id in buf_id_arr to double write file
 * a token_id is returned, thus double write wish the caller to return it after the
 * caller finish flushing the buffers to data file and forwarding the fsync request
 * @param buf_id_arr the buffer id array which is used to get page from global buffer
 * @param size the array size
 */
void dw_perform_batch_flush(uint32 size, CkptSortItem *dirty_buf_list, int thread_id, ThrdDwCxt* thrd_dw_cxt);

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

/**
 * If enable dms and aio, the aio_in_process should be false.
 */
inline bool dw_buf_valid_aio_finished(BufferDesc *buf_desc, uint64 buf_state)
{
    if (!ENABLE_DMS || !ENABLE_DSS_AIO) {
        return true;
    }

    return ((buf_state & BM_VALID) && ((buf_state & BM_DIRTY) || buf_desc->extra->aio_in_progress));
}

extern bool free_space_enough(int buf_id);

extern void dw_generate_single_file();
extern void dw_recovery_partial_write_single();
extern void dw_single_file_truncate(bool is_first);
extern void dw_generate_new_single_file();
extern void dw_cxt_init_single();

extern bool dw_verify_pg_checksum(PageHeader page_header, BlockNumber blockNum, bool dw_file);
extern void dw_log_recovery_page(int elevel, const char *state, BufferTag buf_tag);
extern bool dw_read_data_page(BufferTag buf_tag, SMgrRelation reln, char* data_block);
extern void dw_log_page_header(PageHeader page);
extern int buftag_compare(const void *pa, const void *pb);
extern void dw_encrypt_page(BufferTag tag, char* buf);

extern uint16 first_version_dw_single_flush(BufferDesc *buf_desc);
extern void dw_single_file_recycle(bool is_first);
extern bool backend_can_flush_dirty_page();
extern void dw_force_reset_single_file();
extern void reset_dw_pos_flag();
extern void clean_proc_dw_buf();
extern void init_proc_dw_buf();
extern void dw_prepare_file_head(char *file_head, uint16 start, uint16 dwn, int32 dw_version = -1);
extern void dw_set_pg_checksum(char *page, BlockNumber blockNum);
extern void dw_extend_file(int fd, const void *buf, int buf_size, int64 size,
    int64 file_expect_size, bool single, char* file_name);

extern void dw_transfer_phybuffer_addr(const BufferDesc *buf_desc, BufferTag *buf_tag);
uint16 second_version_dw_single_flush(BufferTag tag, Block block, XLogRecPtr page_lsn,
    bool encrypt, BufferTag phy_tag);

extern uint16 seg_dw_single_flush_without_buffer(BufferTag tag, Block block, bool* flush_old_file);
extern uint16 seg_dw_single_flush(BufferDesc *buf_desc, bool* flush_old_file);
extern void wait_all_single_dw_finish_flush_old();
extern void wait_all_single_dw_finish_flush(bool is_first);
extern uint16 dw_single_flush_internal_old(BufferTag tag, Block block, XLogRecPtr page_lsn,
    BufferTag phy_tag, bool *dw_flush);
extern void dw_single_old_file_truncate();

extern void dw_recover_batch_meta_file(int fd, dw_batch_meta_file *batch_meta_file);
extern void dw_fetch_batch_file_name(int i, char* buf);
extern void wait_all_dw_page_finish_flush();
extern void dw_generate_meta_file(dw_batch_meta_file* batch_meta_file);
extern void dw_generate_batch_files(int batch_file_num, uint64 dw_file_size);
extern void dw_remove_batch_file(int dw_file_num);
extern void dw_remove_batch_meta_file();
extern void dw_recover_all_partial_write_batch(knl_g_dw_context *batch_cxt);
extern void dw_cxt_init_batch();
extern void dw_remove_file(const char* file_name);
extern int dw_open_file(const char* file_name);
extern int dw_create_file(const char* file_name);
extern void dw_upgrade_renable_double_write();

extern void dw_blocked_for_snapshot();
extern void dw_released_after_snapshot();
extern bool is_dw_snapshot_blocked();


#endif /* DOUBLE_WRITE_H */
