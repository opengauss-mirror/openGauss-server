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

inline RelFileNode make_base_page_relfilenode(BasePagePosition position)
{
    RelFileNode rnode;
    rnode.spcNode = EXRTO_BASE_PAGE_SPACE_OID;
    rnode.dbNode = 0;
    rnode.relNode = (uint32)((position / BLCKSZ) >> UINT64_HALF);
    rnode.bucketNode = InvalidBktId;
    rnode.opt = DefaultFileNodeOpt;

    return rnode;
}

Buffer buffer_read_base_page(BasePagePosition position, ReadBufferMode mode)
{
    RelFileNode rnode = make_base_page_relfilenode(position);
    BlockNumber blocknum = (BlockNumber)(position / BLCKSZ);
    bool hit = false;
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
    uint32 batch_id = 0;
    uint32 redo_id = 0;
    Buffer buffer =
        ReadBuffer_common(smgr, RELPERSISTENCE_PERMANENT, MAIN_FORKNUM, blocknum, mode, NULL, &hit, NULL);
    if (buffer == InvalidBuffer) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        (errmsg("invalid buffer when read base page, batch_id: %u, redo_worker_id: %u, blocknum: %lu",
                                batch_id, redo_id, position / BLCKSZ))));
    }

    return buffer;
}

void generate_base_page(const Page src_page, BasePagePosition base_page_pos)
{
    Buffer dest_buf = buffer_read_base_page(base_page_pos, RBM_ZERO_AND_LOCK);
#ifdef ENABLE_UT
    Page dest_page = get_page_from_buffer(dest_buf);
#else
    Page dest_page = BufferGetPage(dest_buf);
#endif
    errno_t rc = memcpy_s(dest_page, BLCKSZ, src_page, BLCKSZ);
    securec_check(rc, "\0", "\0");
    MarkBufferDirty(dest_buf);
    UnlockReleaseBuffer(dest_buf);
}

void read_base_page(BasePagePosition position, BufferDesc* dest_buf_desc)
{
    Buffer buffer = buffer_read_base_page(position, RBM_NORMAL);
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

void recycle_base_page_file(BasePagePosition recycle_pos)
{
    RelFileNode rnode = make_base_page_relfilenode(recycle_pos);
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);

    smgrdounlink(smgr, true, (BlockNumber)(recycle_pos / BLCKSZ));
}

}  // namespace extreme_rto_standby_read
