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
 * gv_page_storage.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_page_storage.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_PAGE_STORAGE_H
#define GVGRAPH_PAGE_STORAGE_H

#include "c.h"
#include "access/xlog.h"
#include "utils/rel.h"
#include "storage/lmgr.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "gv_atomic_impl.h"
#include "lite/index/light_env.h"
#include "lite/index/light_env/allocator.h"
#include "lite/index/light_env/page_dirty_state.h"
#include "lite/index/light_env/page_storage.h"
#include "lite/container/storage/header.h"
#include "lite/index/toolkit/volatile/aligned_allocate.h"

namespace gs_vector {

struct GVLightEnvImpl;

struct GVPageStorage : public annlite::light_env::PageStorage {
    static constexpr size_t page_size = BLCKSZ;
    static constexpr size_t header_size = annlite::AlignedAllocate::aligned_bytes(sizeof(PageHeaderData));
    static constexpr auto invalid_buffer = InvalidBuffer;
    static constexpr auto invalid_docid = InvalidBlockNumber;
    using DocID = BlockNumber;
    using Buffer = int;
    using Page = void*;
    using PageHeaderFormat = PageHeaderData;
    /* a page (8192) = header(24) + data_size(8000) + tail(168) */
    static constexpr size_t data_size = 8000;

    using Atomic = annlite::light_env::Atomic;
    using BlockModification = annlite::light_env::BlockModification;

    explicit GVPageStorage(annlite::LightEnv* env, RelationData* index);
    ~GVPageStorage() {}

    /* The worker thread must use a relation opened in its own session. */
    static void set_worker_index(RelationData* rel) { t_worker_index = rel; }
    static RelationData* get_worker_index() { return t_worker_index; }

    using IndexContext = void*;
    using BlockPointer = uintptr_t;
    using BlockId = uint64_t;

    virtual BlockId block_get_id(BlockPointer pointer) override
    {
        return BufferGetBlockNumber((Buffer)pointer);
    }

    virtual void* block_get_data(BlockPointer pointer) override
    {
        return BufferGetPage((Buffer)pointer);
    }

    virtual size_t block_get_size() const override
    {
        return BLCKSZ;
    }

    virtual void block_set_accessable(BlockId start_block, BlockId last_block) override;

    virtual BlockPointer block_open(BlockId id, const annlite::light_env::OpenOptions& options,
                                    BlockModification*& modify) override;
    virtual void block_close(BlockPointer pointer, bool locked, bool dirty) override;
    virtual bool block_is_open(BlockPointer pointer) override;

    virtual void block_init(void* page, size_t reserved) override;
    virtual bool block_is_initialized(const void* page) override;

    virtual size_t block_header_size() override;
    virtual size_t block_tail_size() override;

    using OffsetId = uint64_t;
    virtual void item_set_content_size(const void* page, size_t size) override;
    virtual size_t item_get_content_size(const void* page) override;
    virtual size_t item_get_free_space(const void* page) override;
    virtual size_t item_count(const void* page) override;
    virtual OffsetId item_add(void* page, const void* data, size_t size) override;
    virtual bool item_pop(void* page) override;
    virtual void* item_at(void* page, OffsetId item_id) override;
    virtual size_t item_offset(const void* page, OffsetId item_id) override;
    virtual bool item_offset_is_valid(OffsetId item_id) override;
    virtual bool item_set_null(Page page, OffsetId item_id, bool is_null) override;
    virtual bool item_is_null(Page page, OffsetId item_id) override;

    virtual void atomic_mark_init(Atomic* atomic_context, BlockId block_id) override;
    virtual Atomic* atomic_create() override;
    virtual void atomic_destroy(Atomic* atomic) override;
    virtual bool atomic_start(Atomic* atomic) override;
    virtual void atomic_release(Atomic* atomic, BlockPointer buffer) override;
    virtual void atomic_end(Atomic* atomic) override;

    virtual uint16_t offsetof_itemiddata(OffsetId item_id) const override;
    virtual uint16_t sizeof_itemiddata() const override;
    virtual uint16_t offsetof_itemcount_meta() const override;
    virtual uint16_t sizeof_itemcount_meta() const override;
    virtual size_t max_item_count_of(annlite::ItemLayoutT layout, uint16_t itemsize) const override;

private:
    annlite::LightEnv* m_env;
    RelationData* m_index;
    annlite::light_env::Allocator* m_alloc;

    ReadBufferMode get_read_buffer_mode(annlite::PageInitStrategy pis);

    /* Each worker has its own smgr hash and file descriptor cache. */
    static thread_local RelationData* t_worker_index;

    RelationData* current_index() const
    {
        return t_worker_index != nullptr ? t_worker_index : m_index;
    }
};

}

#endif /* GV_PAGE_STORAGE_H */
