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
 * double_write_basic.h
 *        Define some basic structs of double write which is needed in knl_instance.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/double_write_basic.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DOUBLE_WRITE_BASIC_H
#define DOUBLE_WRITE_BASIC_H

#include <fcntl.h> /* need open() flags */
#include "c.h"
#include "knl/knl_thread.h"
#include "utils/palloc.h"

static const uint32 DW_BOOTSTRAP_VERSION = 91261;

static const uint32 HALF_K = 512;

static const char DW_FILE_NAME[] = "global/pg_dw";

static const char DW_BUILD_FILE_NAME[] = "global/pg_dw.build";

static const uint32 DW_TRY_WRITE_TIMES = 8;

static const int DW_FILE_FLAG = (O_RDWR | O_SYNC | O_DIRECT | PG_BINARY);

static const mode_t DW_FILE_PERM = (S_IRUSR | S_IWUSR);

static const int DW_FILE_EXTEND_SIZE = (BLCKSZ * HALF_K);

/* 32k pages, 8k each, file size 256M in total */
static const uint16 DW_FILE_PAGE = 32768;

static const int64 DW_FILE_SIZE = (DW_FILE_PAGE * BLCKSZ);

/* make file head size to 512 bytes in total, 10 bytes including head and tail, 502 bytes alignment */
static const uint32 DW_FILE_HEAD_ALIGN_BYTES = 502;

/**
 * | file_head | batch head | data pages ... | batch tail/next batch head | ... |
 * |    0      |     1      |   409 at most  |          1                 | ... |
 */
static const uint16 DW_BATCH_FILE_START = 1;

#define REDUCE_CKS2UINT16(cks) (((cks) >> 16) ^ ((cks)&0xFFFF))

typedef struct st_dw_page_head {
    uint16 page_id; /* page_id in file */
    uint16 dwn;     /* double write number, updated when file header changed */
} dw_page_head_t;

typedef struct st_dw_page_tail {
    uint16 checksum;
    uint16 dwn;
} dw_page_tail_t;

typedef struct st_dw_file_head {
    dw_page_head_t head;
    uint16 start;
    uint8 unused[DW_FILE_HEAD_ALIGN_BYTES]; /* 512 bytes total, one sector for most disks */
    dw_page_tail_t tail;
} dw_file_head_t;

static const uint16 DW_FILE_HEAD_ID_MAX = (uint16)(BLCKSZ / sizeof(dw_file_head_t));

static const uint16 DW_FILE_HEAD_MID_ID = DW_FILE_HEAD_ID_MAX >> 1U;

static const uint32 DW_FILE_HEAD_ID_NUM = 3;

/* write file head 3 times, distributed in start, middle, end of the first page of dw file */
static const uint16 g_dw_file_head_ids[DW_FILE_HEAD_ID_NUM] = {0, DW_FILE_HEAD_MID_ID, DW_FILE_HEAD_ID_MAX - 1};

const static uint64 DW_SLEEP_US = 1000L;

const static uint16 DW_WRITE_STAT_LOWER_LIMIT = 16;

const static int DW_VIEW_COL_NUM = 11;

const static uint32 DW_VIEW_COL_NAME_LEN = 32;

#define DW_PAGE_TAIL(page) ((dw_page_tail_t*)((char*)(page) + (BLCKSZ - sizeof(dw_page_tail_t))))

#define DW_PAGE_CHECKSUM(page) (DW_PAGE_TAIL(page)->checksum)

#ifdef DW_DEBUG
#define DW_LOG_LEVEL LOG
#else
#define DW_LOG_LEVEL DEBUG1
#endif

typedef Datum (*dw_view_get_data_func)();

typedef struct st_dw_view_col {
    char name[DW_VIEW_COL_NAME_LEN];
    Oid data_type;
    dw_view_get_data_func get_data;
} dw_view_col_t;

typedef struct st_dw_read_asst {
    int fd;
    uint16 file_start;    /* reading start page id in file */
    uint16 file_capacity; /* max pages of the file */
    uint16 buf_start;     /* start page of the buf */
    uint16 buf_end;       /* end page of the buf */
    uint16 buf_capacity;  /* max pages the buf can hold */
    char* buf;
} dw_read_asst_t;

typedef struct st_dw_stat_info {
    volatile uint64 file_trunc_num;        /* truncate file */
    volatile uint64 file_reset_num;        /* file full and restart from beginning */
    volatile uint64 total_writes;          /* total double write */
    volatile uint64 low_threshold_writes;  /* less than 16 pages */
    volatile uint64 high_threshold_writes; /* more than one full batch (409 pages) */
    volatile uint64 total_pages;           /* pages total */
    volatile uint64 low_threshold_pages;   /* less than 16 pages total */
    volatile uint64 high_threshold_pages;  /* more than one full batch (409 pages) total */
} dw_stat_info;

typedef struct knl_g_dw_context {
    int fd;
    struct LWLock* flush_lock;

    volatile uint16 write_pos; /* the copied pages in buffer, updated when mark page */
#ifndef ENABLE_THREAD_CHECK
    volatile slock_t initialized;
    volatile slock_t closed;
#else
    volatile int initialized;
    volatile int closed;
#endif
    uint16 flush_page; /* total number of flushed pages before truncate or reset */
    uint16 last_flush_page; /* total number of flushed pages before last dw_perform */
    uint16 unused;

    char* buf;
    bool contain_hashbucket; /* */
    dw_file_head_t* file_head;
    char* unaligned_buf;
    dw_stat_info stat_info;
    MemoryContext mem_ctx;
} dw_context_t;

extern const dw_view_col_t g_dw_view_col_arr[DW_VIEW_COL_NUM];

#endif /* DOUBLE_WRITE_BASIC_H */
