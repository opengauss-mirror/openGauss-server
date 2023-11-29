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
 * block_info_meta.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/extreme_rto/standby_read/block_info_meta.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BLOCK_INFO_META_H
#define BLOCK_INFO_META_H

#include "gs_thread.h"
#include "postgres.h"
#include "access/xlogdefs.h"
#include "access/extreme_rto/standby_read/lsn_info_double_list.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"
#include "storage/buf/bufpage.h"
#include "storage/buf/buf_internals.h"

namespace extreme_rto_standby_read {

const static uint32 BLOCK_INFO_PAGE_HEAD_PAD_SIZE = 40;
const static uint32 BLOCK_INFO_PAGE_VERSION = 1;  // currently the first version of extreme rto standby read

typedef struct _BlockInfoPageHeader {
    PageXLogRecPtr lsn; /* LSN: next byte after last byte of wal record for last change to this page */
    uint16 checksum;    /* checksum */
    uint16 flags;
    uint32 version;
    uint64 total_block_num;  // all blocks of this table, only update on the first page
    uint8 pad[BLOCK_INFO_PAGE_HEAD_PAD_SIZE];
} BlockInfoPageHeader;

#define BLOCK_INFO_PAGE_VALID_FLAG  0x0400

typedef struct _BlockMetaInfo {
    uint32 timeline;
    uint32 record_num;
    XLogRecPtr min_lsn;
    XLogRecPtr max_lsn;
    uint32 flags;
    uint32 pad;
    LsnInfoDoubleList lsn_info_list;
    LsnInfoDoubleList base_page_info_list;
} BlockMetaInfo;

#define BLOCK_INFO_NODE_VALID_FLAG (1 << 24)
#define BLOCK_INFO_NODE_UPDATE_FLAG (1 << 25)
#define BLOCK_INFO_NODE_REFCOUNT_MASK 0xFFFFF
#define IS_BLOCK_INFO_UPDATING(_flags) ((_flags & BLOCK_INFO_NODE_UPDATE_FLAG) == BLOCK_INFO_NODE_UPDATE_FLAG)

const static uint32 BLOCK_INFO_HEAD_SIZE = 64;  // do not modify
const static uint32 BLOCK_INFO_SIZE = 64;       // do not modify

static const uint32 BLOCK_INFO_NUM_PER_PAGE = (BLCKSZ - BLOCK_INFO_HEAD_SIZE) / BLOCK_INFO_SIZE;

typedef enum {
    STANDBY_READ_RECLYE_NONE,
    STANDBY_READ_RECLYE_UPDATE,
    STANDBY_READ_RECLYE_ALL,
} StandbyReadRecyleState;

BlockMetaInfo *get_block_meta_info_by_relfilenode(const BufferTag &buf_tag, BufferAccessStrategy strategy,
                                                  ReadBufferMode mode, Buffer *buffer, bool need_share_lock = false);
void insert_lsn_to_block_info(
    StandbyReadMetaInfo *mete_info, const BufferTag &buf_tag, const Page base_page, XLogRecPtr next_lsn);
void insert_lsn_to_block_info_for_opt(
    StandbyReadMetaInfo *mete_info, const BufferTag &buf_tag, const Page base_page, XLogRecPtr next_lsn);

StandbyReadRecyleState recyle_block_info(const BufferTag &buf_tag, LsnInfoPosition base_page_info_pos,
                                         XLogRecPtr next_base_page_lsn, XLogRecPtr recyle_lsn,
                                         XLogRecPtr *block_info_max_lsn);
bool get_page_lsn_info(const BufferTag& buf_tag, BufferAccessStrategy strategy, XLogRecPtr read_lsn,
    StandbyReadLsnInfoArray* lsn_info);
static inline bool is_block_info_page_valid(BlockInfoPageHeader* header)
{
    return ((header->flags & BLOCK_INFO_PAGE_VALID_FLAG) == BLOCK_INFO_PAGE_VALID_FLAG);
}

static inline bool is_block_meta_info_valid(BlockMetaInfo* meta_info)
{
    return (((meta_info->flags & BLOCK_INFO_NODE_VALID_FLAG) == BLOCK_INFO_NODE_VALID_FLAG) &&
        meta_info->timeline == t_thrd.shemem_ptr_cxt.ControlFile->timeline);
}

void remove_one_block_info_file(const RelFileNode rnode);

void remove_block_meta_info_files_of_db(Oid db_oid);

}  // namespace extreme_rto_standby_read

#endif