/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 *
 * gv_page_storage.cpp
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_page_storage.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include "access/xlog.h"
#include "utils/aiomem.h"
#include "storage/buf/bufmgr.h"

#include "gv_light_env_impl.h"
#include "lite/container/storage/header.h"
#include "gv_page_storage.h"

namespace gs_vector {

using BlockPointer = annlite::light_env::PageStorage::BlockPointer;
using BlockId = annlite::light_env::PageStorage::BlockId;
using BlockModification = annlite::light_env::BlockModification;
using OpenOptions = annlite::light_env::OpenOptions;
using Page = annlite::light_env::PageStorage::Page;

thread_local RelationData* GVPageStorage::t_worker_index = nullptr;

GVPageStorage::GVPageStorage(annlite::LightEnv* env, RelationData* index)
    : annlite::light_env::PageStorage(), m_env(env),
    m_index(index),
    m_alloc(env->get_allocator())
{}

ReadBufferMode GVPageStorage::get_read_buffer_mode(annlite::PageInitStrategy pis)
{
    switch (pis) {
        case annlite::PageInitStrategy::pisBlank:
            return RBM_ZERO;
        case annlite::PageInitStrategy::pisDefault:
            return RBM_NORMAL;
        case annlite::PageInitStrategy::pisBlankOnError:
            return RBM_ZERO_ON_ERROR;
        default:
            return RBM_NORMAL;
    }
}

void GVPageStorage::block_set_accessable(BlockId start_block, BlockId last_block)
{
    RelationData* index = current_index();
    char *zero_buf = NULL;
    if (last_block == InvalidBlockNumber) {
        return;
    }
    RelationOpenSmgr(index);
    zero_buf = g_instance.attr.attr_storage.enable_adio_function ?
        (char *)adio_align_alloc(BLCKSZ) :
        (char *)MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), BLCKSZ);
    Assert(zero_buf != NULL);
    errno_t ret = memset_s(zero_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "\0", "\0");
    // extend blocks.
    {
        if (IsSegmentFileNode(index->rd_node)) {
            for (BlockNumber b = start_block; b <= last_block; ++b) {
                Buffer buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_ZERO, NULL);
                ReleaseBuffer(buf);
            }
        } else {
            if (g_instance.attr.attr_storage.enable_adio_function && u_sess->attr.attr_sql.enable_fast_allocate) {
                smgrextend(index->rd_smgr, MAIN_FORKNUM, last_block, nullptr, false);
            } else {
                PageSetChecksumInplace(zero_buf, last_block);
                smgrextend(index->rd_smgr, MAIN_FORKNUM, last_block, zero_buf, false);
            }
        }
    }

    if (g_instance.attr.attr_storage.enable_adio_function) {
        adio_align_free(zero_buf);
    } else {
        pfree_ext(zero_buf);
    }
}

BlockPointer GVPageStorage::block_open(BlockId id, const OpenOptions& options, BlockModification*& modify)
{
    RelationData* index = current_index();
    BlockNumber blkno = id;
    ReadBufferMode rbm = get_read_buffer_mode(options.pis());

    auto* wal = static_cast<GVAtomicImpl*>(options.atomic());
    Buffer buf = wal != nullptr ? wal->reopen(blkno, modify) : InvalidBuffer;

    // try reopen first
    bool is_reopen = buf != InvalidBuffer;
    if (!is_reopen) {
        buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, rbm, nullptr);
        if (wal != nullptr && wal->is_enabled()) {
            size_t count = wal->append(id, buf);
            modify = &(wal->buffer_state(count));
            modify->mark_dirty(options.flush() || is_reopen);
        }
    }

    if (options.needs_lock() && !is_reopen) {
        LockBuffer(buf, options.locktype());
    }

    return buf;
}

void GVPageStorage::block_close(BlockPointer pointer, bool locked, bool dirty)
{
    Buffer buf = (Buffer)pointer;
    if (locked) {
        if (dirty) {
            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);
        } else {
            UnlockReleaseBuffer(buf);
        }
    } else {
        ReleaseBuffer(buf);
    }
}

bool GVPageStorage::block_is_open(BlockPointer pointer)
{
    Buffer buf = (Buffer)pointer;
    if (buf == InvalidBuffer) {
        return false;
    }
    if (BufferIsLocal(buf)) {
        return u_sess->storage_cxt.LocalRefCount[-(buf)-1] > 0;
    }
    return BufferIsValid(buf);
}

void GVPageStorage::block_init(void* page, size_t reserved)
{
    if (reserved != 0) {
        PageInit(static_cast<char*>(page), BLCKSZ, reserved);
    } else {
        SegPageInit(static_cast<char*>(page), BLCKSZ);
    }
}

bool GVPageStorage::block_is_initialized(const void* page)
{
    const PageHeaderData* header = static_cast<const PageHeaderData*>(page);
    uint16_t expected_v1 = BLCKSZ | PG_COMM_PAGE_LAYOUT_VERSION;
    return header->pd_pagesize_version == expected_v1;
}

size_t GVPageStorage::block_header_size()
{
    return offsetof(PageHeaderData, pd_linp);
}

size_t GVPageStorage::block_tail_size()
{
    return BLCKSZ - sizeof(PageHeaderData) - data_size;
}

void GVPageStorage::item_set_content_size(const void* page, size_t size)
{
    PageHeaderData* header = (PageHeaderData*)const_cast<void*>(page);
    header->pd_lower = block_header_size() + size;
}

size_t GVPageStorage::item_get_content_size(const void* page)
{
    PageHeaderData* header = (PageHeaderData*)const_cast<void*>(page);
    return (size_t)(header->pd_lower) - block_header_size();
}

size_t GVPageStorage::item_get_free_space(const void* page)
{
    return PageGetFreeSpace(static_cast<char*>(const_cast<void*>(page)));
}

size_t GVPageStorage::item_count(const void* page)
{
    return PageGetMaxOffsetNumber(static_cast<char*>(const_cast<void*>(page)));
}

GVPageStorage::OffsetId GVPageStorage::item_add(void* page, const void* data, size_t size)
{
    OffsetNumber offno = PageAddItem(static_cast<char*>(page),
        const_cast<char*>(static_cast<const char*>(data)), size, InvalidOffsetNumber, false, false);
    Assert(offno != InvalidOffsetNumber);
    return offno;
}

bool GVPageStorage::item_pop(void* page)
{
    OffsetNumber maxoff = PageGetMaxOffsetNumber(static_cast<char*>(page));
    if (maxoff == 0) {
        return false;
    }
    PageIndexTupleDelete(static_cast<char*>(page), maxoff);
    return true;
}

void* GVPageStorage::item_at(void* page, OffsetId item_id)
{
    return static_cast<char*>(page) + item_offset(page, item_id);
}

size_t GVPageStorage::item_offset(const void* page, OffsetId item_id)
{
    // item_id is 0-based, so it need +1 to be 1-based
    return ItemIdGetOffset(PageGetItemId(const_cast<char*>(static_cast<const char*>(page)), item_id + 1));
}

bool GVPageStorage::item_offset_is_valid(OffsetId item_id)
{
    return item_id != InvalidOffsetNumber;
}

bool GVPageStorage::item_set_null(Page page, OffsetId item_id, bool is_null)
{
    ItemIdData* iid = PageGetItemId(static_cast<char*>(page), item_id + 1);
    if (is_null) {
        ItemIdMarkDead(iid);
        return true;
    } else {
        ItemIdSetNormal(iid, iid->lp_off, iid->lp_len);
        return true;
    }
}

bool GVPageStorage::item_is_null(Page page, OffsetId item_id)
{
    return ItemIdIsDead(PageGetItemId(static_cast<char*>(page), item_id + 1));
}

// WAL 实现
void GVPageStorage::atomic_mark_init(Atomic* atomic_context, BlockId block_id)
{
    auto* wal = static_cast<GVAtomicImpl*>(atomic_context);
    if (wal != nullptr && wal->is_enabled()) {
        wal->mark_init(block_id);
    }
}

annlite::light_env::Atomic* GVPageStorage::atomic_create()
{
    return ((gs_vector::GVAllocatorImpl*)m_alloc)->template construct<GVAtomicImpl>(current_index());
}

void GVPageStorage::atomic_destroy(Atomic* atomic)
{
    ((gs_vector::GVAllocatorImpl*)m_alloc)->template destroy(atomic);
}

bool GVPageStorage::atomic_start(Atomic* atomic)
{
    auto* wal = static_cast<GVAtomicImpl*>(atomic);
    if (wal != nullptr) {
        wal->start(RM_GRAPH_ID, 0);
        return true;
    }
    return false;
}

void GVPageStorage::atomic_release(Atomic* atomic, BlockPointer buffer)
{
    auto* wal = static_cast<GVAtomicImpl*>(atomic);
    if (wal != nullptr) {
        wal->release((Buffer)buffer);
    }
}

void GVPageStorage::atomic_end(Atomic* atomic)
{
    auto* wal = static_cast<GVAtomicImpl*>(atomic);
    if (wal != nullptr) {
        wal->end();
    }
}

// PostgreSQL page layout metadata implementation
// The itemId array starts from teh end of PageHeaderData (pd_linp). Each ItemId occupies 4 bytes.
// The pd_lower field stores the start offset of the free space (the end of the ItemId array + 1).
uint16_t GVPageStorage::offsetof_itemiddata(OffsetId item_id) const
{
    return static_cast<uint16_t>(offsetof(PageHeaderData, pd_linp) + item_id * sizeof(ItemIdData));
}
uint16_t GVPageStorage::sizeof_itemiddata() const
{
    return sizeof(ItemIdData);
}

uint16_t GVPageStorage::offsetof_itemcount_meta() const
{
    return offsetof(PageHeaderData, pd_lower);
}
uint16_t GVPageStorage::sizeof_itemcount_meta() const
{
    return sizeof(PageHeaderData::pd_lower) + sizeof(PageHeaderData::pd_upper);
}

size_t GVPageStorage::max_item_count_of(annlite::ItemLayoutT layout, uint16_t itemsize) const
{
    if (itemsize == 0) {
        return 0;
    }
    return layout == annlite::ItemLayoutT::iltCompressedItemPage ?
            data_size / itemsize :
            data_size / (sizeof(ItemIdData) + annlite::AlignedAllocate::aligned_bytes(itemsize));
}

} /* gs_vector */
