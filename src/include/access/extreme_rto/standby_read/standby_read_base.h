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
 * standby_read_base.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/extreme_rto/standby_read/standby_read_base.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STANDBY_READ_BASE_H
#define STANDBY_READ_BASE_H

#include "gs_thread.h"
#include "postgres.h"
#include "storage/buf/bufpage.h"
#include "postmaster/alarmchecker.h"

#define EXRTO_FILE_DIR "standby_read"
#define EXRTO_OLD_FILE_DIR "standby_read_old"

static const uint32 EXRTO_BASE_PAGE_FILE_MAXSIZE = 64 * 1024 * 1024; /* 64MB */
static const uint32 EXRTO_LSN_INFO_FILE_MAXSIZE = 16 * 1024 * 1024; /* 16MB */
static const uint32 EXRTO_BLOCK_INFO_FILE_MAXSIZE = RELSEG_SIZE * BLCKSZ;

extern const char* EXRTO_FILE_SUB_DIR[];
extern const uint32 EXRTO_FILE_PATH_LEN;

#define UINT64_HALF 32
#define LOW_WORKERID_BITS 16
#define LOW_WORKERID_MASK ((1U << LOW_WORKERID_BITS) - 1)

#define EXRTODEBUGINFO , __FUNCTION__, __LINE__
#define EXRTODEBUGSTR "[%s:%d]"
#define EXRTOFORMAT(f) EXRTODEBUGSTR f EXRTODEBUGINFO

enum ExRTOFileType {
    BASE_PAGE = 0,
    LSN_INFO_META,
    BLOCK_INFO_META,
};

typedef uint64 BasePagePosition;

typedef struct _StandbyReadMetaInfo {
    uint32 batch_id;
    uint32 redo_id;
    uint64 lsn_table_recyle_position;
    uint64 lsn_table_next_position;  // next position can insert node, shoud jump page header before use
    BasePagePosition base_page_recyle_position;
    BasePagePosition base_page_next_position;  // next position can insert page
    XLogRecPtr recycle_lsn_per_worker;
} StandbyReadMetaInfo;

inline void standby_read_meta_page_set_lsn(Page page, XLogRecPtr LSN)
{
    if (XLByteLT(LSN, PageGetLSN(page))) {
        return;
    }
    PageSetLSNInternal(page, LSN);
}

void exrto_clean_dir(void);
void exrto_recycle_old_dir(void);
void exrto_standby_read_init();
#endif