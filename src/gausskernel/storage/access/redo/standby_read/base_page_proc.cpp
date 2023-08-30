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
 * base_page_proc.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/standby_read/base_page_proc.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/extreme_rto/batch_redo.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"
#include "storage/buf/buf_internals.h"

namespace extreme_rto_standby_read {

inline RelFileNode make_base_page_relfilenode(uint32 batch_id, uint32 redo_worker_id, BasePagePosition position)
{
    RelFileNode rnode;
    rnode.spcNode = EXRTO_BASE_PAGE_SPACE_OID;
    rnode.dbNode = (batch_id << LOW_WORKERID_BITS) | redo_worker_id;
    rnode.relNode = (uint32)((position / BLCKSZ) >> UINT64_HALF);
    rnode.bucketNode = InvalidBktId;
    rnode.opt = DefaultFileNodeOpt;

    return rnode;
}

Buffer buffer_read_base_page(uint32 batch_id, uint32 redo_id, BasePagePosition position, ReadBufferMode mode)
{
    RelFileNode rnode = make_base_page_relfilenode(batch_id, redo_id, position);
    BlockNumber blocknum = (BlockNumber)(position / BLCKSZ);
    bool hit = false;
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
    Buffer buffer =
        ReadBuffer_common(smgr, RELPERSISTENCE_PERMANENT, MAIN_FORKNUM, blocknum, mode, NULL, &hit, NULL);
    if (buffer == InvalidBuffer) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        (errmsg("invalid buffer when read base page, batch_id: %u, redo_worker_id: %u, blocknum: %lu",
                                batch_id, redo_id, position / BLCKSZ))));
    }

    return buffer;
}

void generate_base_page(StandbyReadMetaInfo* meta_info, const Page src_page)
{
    BasePagePosition position = meta_info->base_page_next_position;

    Buffer dest_buf = buffer_read_base_page(meta_info->batch_id, meta_info->redo_id, position, RBM_ZERO_AND_LOCK);

#ifdef ENABLE_UT
    Page dest_page = get_page_from_buffer(dest_buf);
#else
    Page dest_page = BufferGetPage(dest_buf);
#endif
    errno_t rc = memcpy_s(dest_page, BLCKSZ, src_page, BLCKSZ);
    securec_check(rc, "\0", "\0");
    MarkBufferDirty(dest_buf);
    UnlockReleaseBuffer(dest_buf);

    meta_info->base_page_next_position += BLCKSZ;
}

void read_base_page(const BufferTag& buf_tag, BasePagePosition position, BufferDesc* dest_buf_desc)
{
    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);

    const uint32 worker_num_per_mng = extreme_rto::get_page_redo_worker_num_per_manager();
    /* batch id and worker id start from 1 when reading a page */
    uint32 batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    uint32 redo_worker_id = extreme_rto::GetWorkerId(&redo_item_tag, worker_num_per_mng) + 1;

    Buffer buffer = buffer_read_base_page(batch_id, redo_worker_id, position, RBM_NORMAL);

    LockBuffer(buffer, BUFFER_LOCK_SHARE);
#ifdef ENABLE_UT
    Page src_page = get_page_from_buffer(buffer);
#else
    Page src_page = BufferGetPage(buffer);
#endif
    Size page_size = BufferGetPageSize(buffer);
    Page dest_page = (Page)BufHdrGetBlock(dest_buf_desc);
    errno_t rc = memcpy_s(dest_page, page_size, src_page, page_size);
    securec_check(rc, "\0", "\0");
    UnlockReleaseBuffer(buffer);
}

void recycle_base_page_file(uint32 batch_id, uint32 redo_id, BasePagePosition recycle_pos)
{
    RelFileNode rnode = make_base_page_relfilenode(batch_id, redo_id, recycle_pos);
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);

    smgrdounlink(smgr, true, (BlockNumber)(recycle_pos / BLCKSZ));
}

#ifdef ENABLE_UT
Page get_page_from_buffer(Buffer buf)
{
    return BufferGetPage(buf);
}
#endif

}  // namespace extreme_rto_standby_read

