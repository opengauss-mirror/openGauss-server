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
 * -------------------------------------------------------------------------
 *
 * lsn_info_double_list.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/standby_read/lsn_info_double_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/extreme_rto/standby_read/lsn_info_double_list.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"

namespace extreme_rto_standby_read {

void lsn_info_list_init(LsnInfoDoubleList* node)
{
    node->next = LSN_INFO_LIST_HEAD;
    node->prev = LSN_INFO_LIST_HEAD;
}

/*
 * modify the tail of list to link new node (block meta table's page lock is held)
 */
void info_list_modify_old_tail(StandbyReadMetaInfo *meta_info, LsnInfoPosition old_tail_pos,
    LsnInfoPosition insert_pos, XLogRecPtr current_page_lsn, XLogRecPtr next_lsn, bool is_lsn_info)
{
    Page page = NULL;
    LsnInfo lsn_info = NULL;
    BasePageInfo base_page_info = NULL;
    uint32 batch_id = meta_info->batch_id;
    uint32 worker_id = meta_info->redo_id;
    Buffer buffer = InvalidBuffer;
    uint32 offset;

    page = get_lsn_info_page(batch_id, worker_id, old_tail_pos, RBM_ZERO_ON_ERROR, &buffer);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    offset = lsn_info_postion_to_offset(old_tail_pos);
    Assert(offset >= LSN_INFO_HEAD_SIZE);
    Assert(offset % LSN_INFO_NODE_SIZE == 0);
    if (is_lsn_info) {
        lsn_info = (LsnInfo)(page + offset);
        Assert(lsn_info->lsn_list.next == LSN_INFO_LIST_HEAD);
        lsn_info->lsn_list.next = insert_pos;
        Assert(is_lsn_info_node_valid(lsn_info->flags));
    } else {
        base_page_info = (BasePageInfo)(page + offset);
        Assert(base_page_info->base_page_list.next == LSN_INFO_LIST_HEAD);
        base_page_info->base_page_list.next = insert_pos;
        base_page_info->next_base_page_lsn = current_page_lsn;
        Assert(is_lsn_info_node_valid(base_page_info->lsn_info_node.flags));
    }

    standby_read_meta_page_set_lsn(page, next_lsn);
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);
}

}  // namespace extreme_rto_standby_read
