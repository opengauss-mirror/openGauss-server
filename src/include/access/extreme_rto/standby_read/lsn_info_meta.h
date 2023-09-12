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
 * lsn_info_meta.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/extreme_rto/standby_read/lsn_info_meta.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LSN_INFO_META_H
#define LSN_INFO_META_H

#include "gs_thread.h"
#include "postgres.h"
#include "storage/buf/bufpage.h"
#include "storage/buf/buf_internals.h"
#include "access/extreme_rto/standby_read/lsn_info_double_list.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"

namespace extreme_rto_standby_read {
const static uint32 BASE_PAGE_MAP_SIZE = 16;
const static uint32 LSN_INFO_PAGE_HEAD_PAD_SIZE = 32;
const static uint32 LSN_INFO_PAGE_VERSION = 1; /* currently the first version of extreme rto standby read */
const static uint32 LSN_NUM_PER_NODE = 5;
const static uint32 BYTE_BITS = 8;

typedef struct _LsnInfoPageHeader {
    PageXLogRecPtr lsn; /* LSN: next byte after last byte of wal record for last change to this page */
    uint16 checksum;    /* checksum */
    uint16 flags;
    uint32 version;
    uint8 base_page_map[BASE_PAGE_MAP_SIZE];
    uint8 pad[LSN_INFO_PAGE_HEAD_PAD_SIZE];
} LsnInfoPageHeader;

typedef struct _LsnInfoNode {
    LsnInfoDoubleList lsn_list;
    uint32 flags;
    uint16 type;
    uint16 used;
    XLogRecPtr lsn[LSN_NUM_PER_NODE];
} LsnInfoNode;

typedef struct _BasePageInfoNode {
    LsnInfoNode lsn_info_node;
    LsnInfoDoubleList base_page_list;
    XLogRecPtr cur_page_lsn;
    RelFileNode relfilenode;
    ForkNumber fork_num;
    BlockNumber block_num;
    XLogRecPtr next_base_page_lsn;
    BasePagePosition base_page_position;
} BasePageInfoNode;

typedef LsnInfoNode* LsnInfo;
typedef BasePageInfoNode* BasePageInfo;

const static uint32 LSN_INFO_HEAD_SIZE = 64; // do not modify
const static uint32 LSN_INFO_NODE_SIZE = 64; // do not modify
const static uint32 BASE_PAGE_INFO_NODE_SIZE = 128; // do not modify

#define LSN_INFO_NODE_VALID_FLAG   (1 << 24)
#define LSN_INFO_NODE_UPDATE_FLAG  (1 << 25)
#define LSN_INFO_PAGE_VALID_FLAG   0x0400

typedef enum {
    LSN_INFO_TYPE_BASE_PAGE = 1,
    LSN_INFO_TYPE_LSNS,
} LsnInfoType;

static inline bool is_lsn_info_node_valid(uint32 flags)
{
    return ((flags & LSN_INFO_NODE_VALID_FLAG) == LSN_INFO_NODE_VALID_FLAG);
}

static inline bool is_lsn_info_node_updating(uint32 flags)
{
    return ((flags & LSN_INFO_NODE_UPDATE_FLAG) == LSN_INFO_NODE_UPDATE_FLAG);
}

static inline bool is_lsn_info_page_valid(LsnInfoPageHeader *header)
{
    return ((header->flags & LSN_INFO_PAGE_VALID_FLAG) == LSN_INFO_PAGE_VALID_FLAG);
}

static inline bool is_base_page_type(uint16 type)
{
    return (type == LSN_INFO_TYPE_BASE_PAGE);
}

static inline bool is_lsn_type(uint16 type)
{
    return (type == LSN_INFO_TYPE_LSNS);
}

inline uint32 lsn_info_postion_to_offset(LsnInfoPosition position)
{
    return position % BLCKSZ;
}

static inline uint32 bit_to_offset(uint32 which_bit)
{
    return which_bit * LSN_INFO_NODE_SIZE;
}

Page get_lsn_info_page(uint32 batch_id, uint32 worker_id, LsnInfoPosition position, ReadBufferMode mode,
    Buffer* buffer);
void read_lsn_info_before(uint64 start_position, XLogRecPtr *readed_array, XLogRecPtr end_lsn);
LsnInfoDoubleList* lsn_info_position_to_node_ptr(LsnInfoPosition pos);

// block meta table's page lock is held
void insert_lsn_to_lsn_info(StandbyReadMetaInfo* mete_info, LsnInfoDoubleList* head,
    XLogRecPtr next_lsn);

// block meta table's page lock is held
void insert_base_page_to_lsn_info(StandbyReadMetaInfo* meta_info, LsnInfoDoubleList* lsn_head,
    LsnInfoDoubleList* base_page_head, const BufferTag& buf_tag, const Page base_page, XLogRecPtr curent_page_lsn,
    XLogRecPtr next_lsn);

void get_lsn_info_for_read(const BufferTag& buf_tag, LsnInfoPosition latest_lsn_base_page_pos,
    StandbyReadLsnInfoArray* lsn_info_list, XLogRecPtr read_lsn);

Buffer buffer_read_base_page(uint32 batch_id, uint32 redo_id, BasePagePosition position, ReadBufferMode mode);
void generate_base_page(StandbyReadMetaInfo* meta_info, const Page src_page);
void read_base_page(const BufferTag& buf_tag, BasePagePosition position, BufferDesc* dest_buf_desc);
void recycle_base_page_file(uint32 batch_id, uint32 redo_id, BasePagePosition recycle_pos);

void set_base_page_map_bit(Page page, uint32 base_page_loc);
bool is_base_page_map_bit_set(Page page, uint32 which_bit);
void recycle_one_lsn_info_list(const BufferTag& buf_tag, LsnInfoPosition page_info_pos,
    XLogRecPtr recycle_lsn, LsnInfoPosition *min_page_info_pos, XLogRecPtr *min_lsn);
void standby_read_recyle_per_workers(StandbyReadMetaInfo *standby_read_meta_info, XLogRecPtr recycle_lsn);
LsnInfoPosition get_nearest_base_page_pos(
    const BufferTag &buf_tag, const LsnInfoDoubleList &lsn_info_list, XLogRecPtr read_lsn);

}  // namespace extreme_rto_standby_read
#endif