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
 * lsn_info_proc.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/standby_read/lsn_info_proc.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/extreme_rto/batch_redo.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/extreme_rto/standby_read/lsn_info_double_list.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"

namespace extreme_rto_standby_read {

void lsn_info_page_init(Page page)
{
    static_assert(sizeof(LsnInfoPageHeader) == LSN_INFO_HEAD_SIZE, "LsnInfoPageHeader size is not 64 bytes");
    static_assert(sizeof(LsnInfoNode) == LSN_INFO_NODE_SIZE, "LsnInfoNode size is not 64 bytes");
    static_assert(sizeof(BasePageInfoNode) == BASE_PAGE_INFO_NODE_SIZE, "BasePageInfoNode size is not 128 bytes");

    LsnInfoPageHeader *page_header = (LsnInfoPageHeader *)page;
    errno_t ret = memset_s(page_header, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "", "");
    page_header->flags |= LSN_INFO_PAGE_VALID_FLAG;
    page_header->version = LSN_INFO_PAGE_VERSION;
}

void lsn_info_init(LsnInfo lsn_info)
{
    errno_t ret = memset_s(lsn_info, LSN_INFO_NODE_SIZE, 0, LSN_INFO_NODE_SIZE);
    securec_check(ret, "", "");

    lsn_info->flags |= LSN_INFO_NODE_VALID_FLAG;
    lsn_info->type = LSN_INFO_TYPE_LSNS;
    lsn_info_list_init(&lsn_info->lsn_list);
}
void base_page_info_init(BasePageInfo base_page_info)
{
    errno_t ret = memset_s(base_page_info, BASE_PAGE_INFO_NODE_SIZE, 0, BASE_PAGE_INFO_NODE_SIZE);
    securec_check(ret, "", "");

    base_page_info->lsn_info_node.flags |= LSN_INFO_NODE_VALID_FLAG;
    base_page_info->lsn_info_node.type = LSN_INFO_TYPE_BASE_PAGE;
    lsn_info_list_init(&base_page_info->lsn_info_node.lsn_list);
    lsn_info_list_init(&base_page_info->base_page_list);
}

RelFileNode make_lsn_info_relfilenode(uint32 batch_id, uint32 worker_id, LsnInfoPosition position)
{
    RelFileNode rnode = {0};
    rnode.spcNode = EXRTO_LSN_INFO_SPACE_OID;
    rnode.dbNode = (batch_id << LOW_WORKERID_BITS) | worker_id;
    rnode.relNode = (uint32)((position / BLCKSZ) >> UINT64_HALF);
    rnode.bucketNode = InvalidBktId;
    rnode.opt = DefaultFileNodeOpt;

    return rnode;
}

Page get_lsn_info_page(uint32 batch_id, uint32 worker_id, LsnInfoPosition position, ReadBufferMode mode,
    Buffer* buffer)
{
    RelFileNode rnode;
    BlockNumber block_num;
    bool hit = false;
    Page page = NULL;
 
    rnode = make_lsn_info_relfilenode(batch_id, worker_id, position);
    block_num = (uint32)(position / BLCKSZ); /* high 32 bits are stored in the relNode. */
 
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
    *buffer = ReadBuffer_common(smgr, RELPERSISTENCE_PERMANENT, MAIN_FORKNUM, block_num, mode, NULL, &hit, NULL);
 
    if (*buffer == InvalidBuffer) {
        ereport(WARNING, (errcode_for_file_access(),
            errmsg("block is invalid %u/%u/%u %d %u, batch_id: %u, redo_worker_id: %u",
                    rnode.spcNode, rnode.dbNode, rnode.relNode, MAIN_FORKNUM, block_num,
                    batch_id, worker_id)));
        return NULL;
    }

#ifdef ENABLE_UT
    page = get_page_from_buffer(*buffer);
#else
    page = BufferGetPage(*buffer);
#endif
    if (!is_lsn_info_page_valid((LsnInfoPageHeader *)page)) {
        if (mode == RBM_NORMAL) {
            ReleaseBuffer(*buffer);
            *buffer = InvalidBuffer;
            return NULL;
        }
        /* make sure to make buffer dirty outside */
        lsn_info_page_init(page);
    }

    return page;
}

LsnInfoPosition create_lsn_info_node(StandbyReadMetaInfo *meta_info, LsnInfoPosition old_tail_pos,
    XLogRecPtr next_lsn, bool create_in_old_page, Page old_page)
{
    Page page = NULL;
    LsnInfo lsn_info = NULL;
    uint32 batch_id = meta_info->batch_id;
    uint32 worker_id = meta_info->redo_id;
    LsnInfoPosition insert_pos = meta_info->lsn_table_next_position;
    Buffer buffer = InvalidBuffer;
    uint32 offset;
 
    offset = lsn_info_postion_to_offset(insert_pos);
    if (offset == 0) {
        insert_pos += LSN_INFO_HEAD_SIZE; /* actual insert position */
        offset += LSN_INFO_HEAD_SIZE;
    }
    Assert(offset % LSN_INFO_NODE_SIZE == 0);
 
    if (create_in_old_page) {
        /* in old page, buffer is already locked */
        lsn_info = (LsnInfo)(old_page + offset);
    } else {
        page = get_lsn_info_page(batch_id, worker_id, insert_pos, RBM_ZERO_ON_ERROR, &buffer);
 
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        lsn_info = (LsnInfo)(page + offset);
    }
 
    lsn_info_init(lsn_info);
    lsn_info->lsn[lsn_info->used] = next_lsn;
    lsn_info->used++;
    lsn_info->lsn_list.prev = old_tail_pos;
 
    if (!create_in_old_page) {
        standby_read_meta_page_set_lsn(page, next_lsn);
        MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
    }
    /* update meta info */
    meta_info->lsn_table_next_position = insert_pos + LSN_INFO_NODE_SIZE;
 
    return insert_pos;
}

void insert_lsn_to_lsn_info(StandbyReadMetaInfo *meta_info, LsnInfoDoubleList *lsn_head, XLogRecPtr next_lsn)
{
    Page page = NULL;
    LsnInfo lsn_info = NULL;
    uint32 batch_id = meta_info->batch_id;
    uint32 worker_id = meta_info->redo_id;
    LsnInfoPosition tail_pos = lsn_head->prev; /* lsn info node tail */
    LsnInfoPosition insert_pos = meta_info->lsn_table_next_position;
    Buffer buffer = InvalidBuffer;
    uint32 offset;
 
    Assert(!INFO_POSITION_IS_INVALID(tail_pos));
    page = get_lsn_info_page(batch_id, worker_id, tail_pos, RBM_ZERO_ON_ERROR, &buffer);
 
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    offset = lsn_info_postion_to_offset(tail_pos);
    lsn_info = (LsnInfo)(page + offset);
    Assert(offset >= LSN_INFO_HEAD_SIZE);
    Assert(offset % LSN_INFO_NODE_SIZE == 0);
    Assert(is_lsn_info_node_valid(lsn_info->flags));
    Assert(lsn_info->lsn_list.next == LSN_INFO_LIST_HEAD);
    if (lsn_info->used < LSN_NUM_PER_NODE) {
        lsn_info->lsn[lsn_info->used] = next_lsn;
        lsn_info->used++;
 
        standby_read_meta_page_set_lsn(page, next_lsn);
        MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
    } else {
        /*
         * There is no free space in the old lsn info node, create a new one.
         */
        bool create_in_old_page = (insert_pos / BLCKSZ) == (tail_pos / BLCKSZ);
        /* insert position maybe changed */
        insert_pos = create_lsn_info_node(meta_info, tail_pos, next_lsn, create_in_old_page, page);
 
        /* modify lsn info list */
        lsn_info->lsn_list.next = insert_pos;
        standby_read_meta_page_set_lsn(page, next_lsn);
        MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
        /* update lsn info tail in block info meta */
        lsn_head->prev = insert_pos;
    }
}

LsnInfoPosition create_base_page_info_node(StandbyReadMetaInfo *meta_info,
    LsnInfoPosition old_lsn_tail_pos, LsnInfoPosition old_base_page_tail_pos, const BufferTag* buf_tag,
    XLogRecPtr current_page_lsn, XLogRecPtr next_lsn)
{
    Page page = NULL;
    BasePageInfo base_page_info = NULL;
    uint32 batch_id = meta_info->batch_id;
    uint32 worker_id = meta_info->redo_id;
    LsnInfoPosition insert_pos = meta_info->lsn_table_next_position;
    BasePagePosition base_page_pos = meta_info->base_page_next_position;
    Buffer buffer = InvalidBuffer;
    uint32 offset;
    uint32 remain_size;
 
    /*
     * If there is not enough space in current page, we insert base page info node in next page.
     */
    remain_size = BLCKSZ - insert_pos % BLCKSZ;
    if (remain_size < BASE_PAGE_INFO_NODE_SIZE) {
        Assert(remain_size == LSN_INFO_NODE_SIZE);
        insert_pos += LSN_INFO_NODE_SIZE; /* switch to next page */
    }
 
    offset = lsn_info_postion_to_offset(insert_pos);
    Assert(offset % LSN_INFO_NODE_SIZE == 0);
    if (offset == 0) {
        insert_pos += LSN_INFO_HEAD_SIZE; /* actual insert position */
        offset += LSN_INFO_HEAD_SIZE;
    }
 
    page = get_lsn_info_page(batch_id, worker_id, insert_pos, RBM_ZERO_ON_ERROR, &buffer);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
 
    base_page_info = (BasePageInfo)(page + offset);
 
    base_page_info_init(base_page_info);
    base_page_info->lsn_info_node.lsn_list.prev = old_lsn_tail_pos;
    base_page_info->lsn_info_node.lsn[0] = next_lsn;
    base_page_info->lsn_info_node.used++;
    base_page_info->base_page_list.prev = old_base_page_tail_pos;
    base_page_info->cur_page_lsn = current_page_lsn;
    base_page_info->relfilenode = buf_tag->rnode;
    base_page_info->fork_num = buf_tag->forkNum;
    base_page_info->block_num = buf_tag->blockNum;
    base_page_info->next_base_page_lsn = InvalidXLogRecPtr;
    base_page_info->base_page_position = base_page_pos;
 
    set_base_page_map_bit(page, offset);
    ereport(DEBUG1,
        (errmsg("create_base_page_info_node, block is %u/%u/%u %d %u, batch_id: %u, redo_worker_id: %u"
                "page lsn %lu, next lsn %lu, base page pos %lu, insert pos %lu",
            buf_tag->rnode.spcNode,
            buf_tag->rnode.dbNode,
            buf_tag->rnode.relNode,
            buf_tag->forkNum,
            buf_tag->blockNum,
            batch_id,
            worker_id,
            current_page_lsn,
            next_lsn,
            base_page_pos,
            insert_pos)));

    standby_read_meta_page_set_lsn(page, next_lsn);
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);
 
    /* update meta info */
    meta_info->lsn_table_next_position = insert_pos + BASE_PAGE_INFO_NODE_SIZE;
 
    return insert_pos;
}

void insert_base_page_to_lsn_info(StandbyReadMetaInfo *meta_info, LsnInfoDoubleList *lsn_head,
    LsnInfoDoubleList *base_page_head, const BufferTag& buf_tag, const Page base_page, XLogRecPtr current_page_lsn,
    XLogRecPtr next_lsn)
{
    LsnInfoPosition old_lsn_tail_pos = lsn_head->prev;
    LsnInfoPosition old_base_page_tail_pos = base_page_head->prev;
    LsnInfoPosition insert_pos;
 
    /* possibly modified meta_info */
    insert_pos = create_base_page_info_node(meta_info, old_lsn_tail_pos, old_base_page_tail_pos, &buf_tag,
        current_page_lsn, next_lsn);
 
    /* modify old tail information of lsn info node and base page info node */
    if (old_lsn_tail_pos != LSN_INFO_LIST_HEAD) {
        info_list_modify_old_tail(meta_info, old_lsn_tail_pos, insert_pos, current_page_lsn, next_lsn, true);
    }
    if (old_base_page_tail_pos != LSN_INFO_LIST_HEAD) {
        info_list_modify_old_tail(meta_info, old_base_page_tail_pos, insert_pos, current_page_lsn, next_lsn, false);
    }
 
    /* modify block info meta */
    lsn_head->prev = insert_pos;
    base_page_head->prev = insert_pos;
 
    if (INFO_POSITION_IS_INVALID(lsn_head->next)) {
        lsn_head->next = insert_pos;
    }
    if (INFO_POSITION_IS_INVALID(base_page_head->next)) {
        base_page_head->next = insert_pos;
    }
 
    /* generate base page */
    generate_base_page(meta_info, base_page);
}

void get_lsn_info_for_read(const BufferTag& buf_tag, LsnInfoPosition latest_lsn_base_page_pos,
    StandbyReadLsnInfoArray* lsn_info_list, XLogRecPtr read_lsn)
{
    BasePageInfo base_page_info = NULL;
    LsnInfoPosition next_lsn_info_pos;
    Buffer buffer;
 
    XLogRecPtr page_lsn;
    XLogRecPtr xlog_lsn;
    uint32 batch_id;
    uint32 worker_id;
    XLogRecPtr *lsn_arry = lsn_info_list->lsn_array;
 
    /* get batch id and page redo worker id */
    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);
    /* batch id and worker id start from 1 when reading a page */
    batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    worker_id = extreme_rto::GetWorkerId(&redo_item_tag, extreme_rto::get_page_redo_worker_num_per_manager()) + 1;
 
    /* find fisrt base page whose lsn less than read lsn form tail to head */
    do {
        /* reach the end of the list */
        if (INFO_POSITION_IS_INVALID(latest_lsn_base_page_pos)) {
            ereport(PANIC, (
                errmsg("can not find base page, block is %u/%u/%u %d %u, batch_id: %u, redo_worker_id: %u",
                    buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum,
                    buf_tag.blockNum, batch_id, worker_id)));
            break;
        }
        buffer = InvalidBuffer;
        Page page = get_lsn_info_page(batch_id, worker_id, latest_lsn_base_page_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(ERROR,
                     (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"), batch_id,
                              worker_id, latest_lsn_base_page_pos)));
        }
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
 
        uint32 offset = lsn_info_postion_to_offset(latest_lsn_base_page_pos);
        base_page_info = (BasePageInfo)(page + offset);
 
        page_lsn = base_page_info->cur_page_lsn;
        lsn_info_list->base_page_pos = base_page_info->base_page_position;
        lsn_info_list->base_page_lsn = base_page_info->cur_page_lsn;
        Assert(is_base_page_type(base_page_info->lsn_info_node.type));
 
        /* If we find the desired page, keep it locked */
        if (XLByteLE(page_lsn, read_lsn)) {
            break;
        }
        UnlockReleaseBuffer(buffer);
        latest_lsn_base_page_pos = base_page_info->base_page_list.prev;
    } while (true);
 
    LsnInfo lsn_info = &base_page_info->lsn_info_node;
    bool find_end = false;
    uint32 lsn_num = 0;
    do {
        for (uint16 i = 0; i < lsn_info->used; ++i) {
            xlog_lsn = lsn_info->lsn[i];
            if (XLByteLE(read_lsn, xlog_lsn)) {
                find_end = true;
                break;
            }
 
            lsn_arry[lsn_num++] = xlog_lsn;
        }
        next_lsn_info_pos = lsn_info->lsn_list.next;
        UnlockReleaseBuffer(buffer);
        /* reach the end of the list */
        if (find_end || next_lsn_info_pos == LSN_INFO_LIST_HEAD) {
            break;
        }
 
        Page page = get_lsn_info_page(batch_id, worker_id, next_lsn_info_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(ERROR,
                     (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"), batch_id,
                              worker_id, next_lsn_info_pos)));
        }
        Assert(buffer != InvalidBuffer);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
 
        uint32 offset = lsn_info_postion_to_offset(next_lsn_info_pos);
        lsn_info = (LsnInfo)(page + offset);
    } while (true);
 
    lsn_info_list->lsn_num = lsn_num;
}

static bool check_base_page_loc_valid(uint32 base_page_loc)
{
    if (base_page_loc < LSN_INFO_HEAD_SIZE || base_page_loc > BLCKSZ - BASE_PAGE_INFO_NODE_SIZE ||
        base_page_loc % LSN_INFO_HEAD_SIZE != 0) {
        ereport(ERROR, (errmsg("invalid BasePageInfo location:%u, page size:%d", base_page_loc, BLCKSZ)));
        return false;
    }
    return true;
}

/*
 * set LsnInfoPageHeader::base_page_map specific bit from 0 to 1.
 * the bit is correspond to some 64bytes range space in this page.
 * params explanation.
 * page: some page block in RAM(one block occupies 8192bytes in memory).
 * base_page_loc: the offset of some BasePageInfoNode object from the beginning of this page.
 * LsnInfoPageHeader::base_page_map has 128 bit which is mapped to 8192bytes page.
 * every bit represent 64 bytes (64 = 8192/128).
 * we can assume bit 0 map to [0, 64) of the page;
 *               bit 1 map to [64, 128) of the page;
 *               ......
 *               bit 127 map to [8128, 8192) of the page;
 * LsnInfoPageHeader is the page header which occupies 64bytes, so bit 0 is always 0.
 * LSN_INFO_HEAD_SIZE,LSN_INFO_NODE_SIZE,BASE_PAGE_INFO_NODE_SIZE must be integer mutiple of 64,
 * so we can use base_page_map to map page memory.
 */
void set_base_page_map_bit(Page page, uint32 base_page_loc)
{
    /*
     * make sure base_page_loc is in specific range
     * base_page_loc must be an integer multiple of LSN_INFO_HEAD_SIZE
     */
    check_base_page_loc_valid(base_page_loc);

    LsnInfoPageHeader *page_header = (LsnInfoPageHeader *)page;
    uint8 *base_page_map = page_header->base_page_map;
    uint32 which_bit = base_page_loc / LSN_INFO_NODE_SIZE;
    uint32 which_bytes = which_bit / BYTE_BITS; // uint8 has 8 bits or 8*sizeof(uint8) bits
    uint32 bit_offset = which_bit % BYTE_BITS;
    base_page_map[which_bytes] |= ((uint8)((uint8)1 << bit_offset));
}

static void check_base_page_map_bit_loc_valid(uint32 which_bit)
{
    if (which_bit >= BASE_PAGE_MAP_SIZE * BYTE_BITS) {
        ereport(ERROR, (errmsg("invalid base_page_map bit location:%u, "
                "the valid range is [%u, %u).", which_bit, 0U, BASE_PAGE_MAP_SIZE * BYTE_BITS)));
    }
}

/*
 * check if LsnInfoPageHeader::base_page_map specific bit equal to 1.
 * page: the page in which LsnInfoPageHeader object you want to check.
 * which_bit: the bit you want to check.
 * if the target bit is equal to 1, return true.
 */
bool is_base_page_map_bit_set(Page page, uint32 which_bit)
{
    check_base_page_map_bit_loc_valid(which_bit);

    LsnInfoPageHeader *page_header = (LsnInfoPageHeader *)page;
    uint8 *base_page_map = page_header->base_page_map;
    uint32 which_bytes = which_bit / BYTE_BITS; // uint8 has 8 bits or 8*sizeof(uint8) bits
    uint32 bit_offset = which_bit % BYTE_BITS;
    return (base_page_map[which_bytes] & (((uint8)1) << bit_offset)) != 0;
}

void recycle_lsn_info_file(uint32 batch_id, uint32 redo_id, BasePagePosition recycle_pos)
{
    RelFileNode rnode = make_lsn_info_relfilenode(batch_id, redo_id, recycle_pos);
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
 
    smgrdounlink(smgr, true, (BlockNumber)(recycle_pos / BLCKSZ));
}

void recycle_one_lsn_info_list(const BufferTag& buf_tag, LsnInfoPosition page_info_pos,
    XLogRecPtr recycle_lsn, LsnInfoPosition *min_page_info_pos, XLogRecPtr *min_lsn)
{
    /* get batch id and page redo worker id */
    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);

    const uint32 worker_num_per_mng = extreme_rto::get_page_redo_worker_num_per_manager();
    /* batch id and worker id start from 1 when reading a page */
    uint32 batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    uint32 worker_id = extreme_rto::GetWorkerId(&redo_item_tag, worker_num_per_mng) + 1;
 
    while (INFO_POSITION_IS_VALID(page_info_pos)) {
        Buffer buffer = InvalidBuffer;
        Page page = get_lsn_info_page(batch_id, worker_id, page_info_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(PANIC, (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"),
                                   batch_id, worker_id, page_info_pos)));
        }
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        uint32 offset = lsn_info_postion_to_offset(page_info_pos);
        BasePageInfo base_page_info = (BasePageInfo)(page + offset);
        Assert(is_base_page_type(base_page_info->lsn_info_node.type));

        *min_page_info_pos = page_info_pos;
        *min_lsn = base_page_info->cur_page_lsn;
 
        /* retain a page version with page lsn less than recycle lsn */
        XLogRecPtr next_base_page_lsn = base_page_info->next_base_page_lsn;
        if (g_instance.attr.attr_storage.enable_exrto_standby_read_opt) {
            XLogRecPtr next_lsn = base_page_info->lsn_info_node.lsn[0];
            if (XLogRecPtrIsValid(next_lsn) && XLByteLE(recycle_lsn, next_lsn)) {
                UnlockReleaseBuffer(buffer);
                break;
            }
        } else {
            if (XLogRecPtrIsInvalid(next_base_page_lsn) || XLByteLT(recycle_lsn, next_base_page_lsn)) {
                UnlockReleaseBuffer(buffer);
                break;
            }
        }

        base_page_info->lsn_info_node.flags &= ~LSN_INFO_NODE_VALID_FLAG;
        page_info_pos = base_page_info->base_page_list.next;
        MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
    }
}

void invalid_base_page_list(StandbyReadMetaInfo *meta_info, Buffer buffer, uint32 offset)
{
    LsnInfoPosition page_info_pos;
#ifdef ENABLE_UT
    Page page = get_page_from_buffer(buffer);
#else
    Page page = BufferGetPage(buffer);
#endif
    BasePageInfo base_page_info = (BasePageInfo)(page + offset);
    /* set invalid flags */
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    base_page_info->lsn_info_node.flags &= ~LSN_INFO_NODE_VALID_FLAG;
    page_info_pos = base_page_info->base_page_list.next;
    MarkBufferDirty(buffer);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK); /* keep buffer pinned */
 
    uint32 batch_id = meta_info->batch_id;
    uint32 worker_id = meta_info->redo_id;
    while (INFO_POSITION_IS_VALID(page_info_pos)) {
        page = get_lsn_info_page(batch_id, worker_id, page_info_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(PANIC, (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"),
                                   batch_id, worker_id, page_info_pos)));
        }
        offset = lsn_info_postion_to_offset(page_info_pos);
        base_page_info = (BasePageInfo)(page + offset);

        /* unset valid flags */
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        base_page_info->lsn_info_node.flags &= ~LSN_INFO_NODE_VALID_FLAG;
        page_info_pos = base_page_info->base_page_list.next;
        MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
    }
}

inline void update_recycle_lsn_per_worker(StandbyReadMetaInfo *meta_info, XLogRecPtr base_page_lsn,
                                          XLogRecPtr next_base_page_lsn,
                                          XLogRecPtr block_info_max_lsn = InvalidXLogRecPtr)
{
    Assert(XLogRecPtrIsValid(base_page_lsn));
    if (XLogRecPtrIsInvalid(meta_info->recycle_lsn_per_worker) ||
        XLByteLT(meta_info->recycle_lsn_per_worker, base_page_lsn)) {
        meta_info->recycle_lsn_per_worker = base_page_lsn;
    }
    uint64 cur_base_page_recyle_segno = meta_info->base_page_recyle_position / EXRTO_BASE_PAGE_FILE_MAXSIZE;
    uint64 cur_lsn_table_recyle_segno = meta_info->lsn_table_recyle_position / EXRTO_LSN_INFO_FILE_MAXSIZE;
    ereport(LOG,
            (errmsg(EXRTOFORMAT("[exrto_recycle] update recycle lsn per worker , batch_id: %u, redo_id: %u, recycle "
                                "base_page_lsn: %08X/%08X, next_base_page_lsn: %08X/%08X, block_info_max_lsn: "
                                "%08X/%08X, base page recycle segno: "
                                "%lu, lsn info recycle segno: %lu"),
                    meta_info->batch_id, meta_info->redo_id, (uint32)(base_page_lsn >> UINT64_HALF),
                    (uint32)base_page_lsn, (uint32)(next_base_page_lsn >> UINT64_HALF), (uint32)next_base_page_lsn,
                    (uint32)(block_info_max_lsn >> UINT64_HALF), (uint32)block_info_max_lsn, cur_base_page_recyle_segno,
                    cur_lsn_table_recyle_segno)));
}

bool recycle_one_lsn_info_page(StandbyReadMetaInfo *meta_info, XLogRecPtr recycle_lsn,
    BasePagePosition *base_page_position)
{
    uint32 batch_id = meta_info->batch_id;
    uint32 worker_id = meta_info->redo_id;
    Buffer buffer = InvalidBuffer;
    LsnInfoPosition recycle_pos = meta_info->lsn_table_recyle_position;
    Page page = get_lsn_info_page(batch_id, worker_id, recycle_pos, RBM_NORMAL, &buffer);
    if (unlikely(page == NULL || buffer == InvalidBuffer)) {
        ereport(PANIC, (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"), batch_id,
                               worker_id, recycle_pos)));
    }

    bool buffer_is_locked = false;
    /* skip page header */
    for (uint32 bit = 1; bit < BASE_PAGE_MAP_SIZE * BYTE_BITS; bit++) {
        if (!buffer_is_locked) {
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            buffer_is_locked = true;
        }
 
        if (!is_base_page_map_bit_set(page, bit)) {
            continue;
        }
        uint32 offset = bit_to_offset(bit);
        BasePageInfo base_page_info = (BasePageInfo)(page + offset);
        LsnInfoPosition cur_base_page_info_pos = recycle_pos + offset;
        Assert(is_base_page_type(base_page_info->lsn_info_node.type));
 
        /* block meta file may be dropped */
        if (!is_lsn_info_node_valid(base_page_info->lsn_info_node.flags)) {
            continue;
        }
 
        /* retain a page version with page lsn less than recycle lsn */
        XLogRecPtr base_page_lsn = base_page_info->cur_page_lsn;
        if (XLogRecPtrIsInvalid(base_page_lsn)) {
            base_page_lsn = base_page_info->lsn_info_node.lsn[0];
        }
        XLogRecPtr next_base_page_lsn = base_page_info->next_base_page_lsn;
        *base_page_position = base_page_info->base_page_position;

        if (g_instance.attr.attr_storage.enable_exrto_standby_read_opt) {
            XLogRecPtr next_lsn = base_page_info->lsn_info_node.lsn[0];
            if (XLogRecPtrIsValid(next_lsn) && XLByteLE(recycle_lsn, next_lsn)) {
                update_recycle_lsn_per_worker(meta_info, base_page_lsn, next_base_page_lsn);
                UnlockReleaseBuffer(buffer);
                return false;
            }
        } else {
            if (XLogRecPtrIsValid(next_base_page_lsn) && XLByteLT(recycle_lsn, next_base_page_lsn)) {
                update_recycle_lsn_per_worker(meta_info, base_page_lsn, next_base_page_lsn);
                UnlockReleaseBuffer(buffer);
                return false;
            }
        }

        BufferTag buf_tag;
        INIT_BUFFERTAG(buf_tag, base_page_info->relfilenode, base_page_info->fork_num, base_page_info->block_num);
 
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        buffer_is_locked = false;

        XLogRecPtr block_info_max_lsn = InvalidXLogRecPtr;
        StandbyReadRecyleState stat =
            recyle_block_info(buf_tag, cur_base_page_info_pos, next_base_page_lsn, recycle_lsn, &block_info_max_lsn);
        if (stat == STANDBY_READ_RECLYE_ALL) {
            invalid_base_page_list(meta_info, buffer, offset);
        } else if (stat == STANDBY_READ_RECLYE_NONE) {
            update_recycle_lsn_per_worker(meta_info, base_page_lsn, next_base_page_lsn, block_info_max_lsn);
            ReleaseBuffer(buffer);
            return false;
        }
    }
 
    if (buffer_is_locked) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    }
    ReleaseBuffer(buffer);
    return true;
}

void standby_read_recyle_per_workers(StandbyReadMetaInfo *meta_info, XLogRecPtr recycle_lsn)
{
    Assert(meta_info->batch_id > 0);
    Assert(meta_info->redo_id > 0);
    bool recycle_next_page = true;
    BasePagePosition base_page_position = meta_info->base_page_recyle_position;
    uint64 last_base_page_recyle_segno = meta_info->base_page_recyle_position / EXRTO_BASE_PAGE_FILE_MAXSIZE;
    uint64 last_lsn_table_recyle_segno = meta_info->lsn_table_recyle_position / EXRTO_LSN_INFO_FILE_MAXSIZE;
    uint64 cur_base_page_recyle_segno, cur_lsn_table_recyle_segno;

    uint64 recyled_page_len = 0;
    const uint32 recyle_ratio = 32; // no need recyle so fast
    while (meta_info->lsn_table_recyle_position + BLCKSZ * recyle_ratio < meta_info->lsn_table_next_position) {
        recycle_next_page = recycle_one_lsn_info_page(meta_info, recycle_lsn, &base_page_position);
        if (!recycle_next_page) {
            break;
        }
        /* update recycle position */
        meta_info->lsn_table_recyle_position += BLCKSZ;
        recyled_page_len += BLCKSZ;
        Assert(meta_info->lsn_table_recyle_position % BLCKSZ == 0);
        if (recyled_page_len >= EXRTO_LSN_INFO_FILE_MAXSIZE) {
            RedoInterruptCallBack();
            pg_usleep(100); // sleep 0.1ms
            recyled_page_len = 0;
        }
    }
 
    meta_info->base_page_recyle_position = base_page_position;
    Assert(meta_info->base_page_recyle_position % BLCKSZ == 0);
    Assert(meta_info->base_page_recyle_position <= meta_info->base_page_next_position);
 
    cur_base_page_recyle_segno = meta_info->base_page_recyle_position / EXRTO_BASE_PAGE_FILE_MAXSIZE;
    cur_lsn_table_recyle_segno = meta_info->lsn_table_recyle_position / EXRTO_LSN_INFO_FILE_MAXSIZE;
    if (cur_base_page_recyle_segno > last_base_page_recyle_segno ||
        cur_lsn_table_recyle_segno > last_lsn_table_recyle_segno) {
        buffer_drop_exrto_standby_read_buffers(meta_info);
    }
    if (cur_lsn_table_recyle_segno > last_lsn_table_recyle_segno) {
        recycle_lsn_info_file(meta_info->batch_id, meta_info->redo_id, meta_info->lsn_table_recyle_position);
    }
    if (cur_base_page_recyle_segno > last_base_page_recyle_segno) {
        recycle_base_page_file(meta_info->batch_id, meta_info->redo_id, meta_info->base_page_recyle_position);
    }
}

LsnInfoPosition get_nearest_base_page_pos(
    const BufferTag &buf_tag, const LsnInfoDoubleList &lsn_info_list, XLogRecPtr read_lsn)
{
    Buffer buffer;

    XLogRecPtr page_lsn = InvalidXLogRecPtr;
    LsnInfoPosition base_page_pos = LSN_INFO_LIST_HEAD;
    uint32 batch_id;
    uint32 worker_id;

    /* get batch id and page redo worker id */
    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);
    /* batch id and worker id start from 1 when reading a page */
    batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    worker_id =
        extreme_rto::GetWorkerId(&redo_item_tag, (uint32)extreme_rto::get_page_redo_worker_num_per_manager()) + 1;
    LsnInfoPosition latest_lsn_base_page_pos = lsn_info_list.prev;

    /* Find the base page with the smallest lsn and greater than read lsn from tail to head */
    do {
        /* reach the end of the list */
        if (INFO_POSITION_IS_INVALID(latest_lsn_base_page_pos)) {
            ereport(DEBUG1, (errmsg("can not find base page, block is %u/%u/%u %d %u, batch_id: %u, redo_worker_id: %u",
                                    buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum,
                                    buf_tag.blockNum, batch_id, worker_id)));
            break;
        }
        buffer = InvalidBuffer;
        Page page = get_lsn_info_page(batch_id, worker_id, latest_lsn_base_page_pos, RBM_NORMAL, &buffer);
        if (page == NULL || buffer == InvalidBuffer) {
            ereport(ERROR, (errmsg(EXRTOFORMAT("get_nearest_base_page_pos failed, batch_id: %u, redo_id: %u, pos: %lu"),
                                   batch_id, worker_id, latest_lsn_base_page_pos)));
        }
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        uint32 offset = lsn_info_postion_to_offset(latest_lsn_base_page_pos);
        BasePageInfo base_page_info = (BasePageInfo)(page + offset);

        Assert(is_base_page_type(base_page_info->lsn_info_node.type));
        if (!is_base_page_type(base_page_info->lsn_info_node.type)) {
            UnlockReleaseBuffer(buffer);
            ereport(
                ERROR,
                (errmsg(EXRTOFORMAT("get_nearest_base_page_pos failed, not base page type, block is %u/%u/%u %d %u, "
                                    "batch_id: %u, redo_id: %u, pos: %lu"),
                        buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum,
                        buf_tag.blockNum, batch_id, worker_id, latest_lsn_base_page_pos)));
        }

        UnlockReleaseBuffer(buffer);
        page_lsn = base_page_info->lsn_info_node.lsn[0];
        LsnInfoPosition prev_lsn_base_page_pos = base_page_info->base_page_list.prev;

        if (XLByteLT(page_lsn, read_lsn)) {
            break;
        }

        /* the base page's lsn >= read_lsn */
        base_page_pos = base_page_info->base_page_position;

        // the last base page info
        if (XLByteEQ(lsn_info_list.next, latest_lsn_base_page_pos)) {
            break;
        }
 
        latest_lsn_base_page_pos = prev_lsn_base_page_pos;
    } while (true);

    if (page_lsn == InvalidXLogRecPtr || base_page_pos == LSN_INFO_LIST_HEAD) {
        ereport(DEBUG1, (errmsg(EXRTOFORMAT("get_nearest_base_page_pos failed, block is %u/%u/%u/%hd/%hu %d %u, "
                                            "batch_id: %u, redo_id: %u, pos: %lu, page_lsn: %lu"),
                                buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode,
                                buf_tag.rnode.bucketNode, buf_tag.rnode.opt, buf_tag.forkNum, buf_tag.blockNum,
                                batch_id, worker_id, latest_lsn_base_page_pos, page_lsn)));
    }

    return base_page_pos;
}
}  // namespace extreme_rto_standby_read
