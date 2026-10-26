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
 * gv_atomic_impl.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_atomic_impl.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_ATOMIC_IMPL_H
#define GVGRAPH_ATOMIC_IMPL_H

#include "lite/index/light_env/atomic.h"
#include "lite/index/light_env/page_dirty_state.h"
#include "lite/index/toolkit/allocator.h"
#include "lite/index/toolkit/dirty_state.h"

#include "access/generic_xlog.h"
#include "access/xloginsert.h"
#include "utils/elog.h"
#include "storage/buf/bufmgr.h"

/* for xlog */
#ifndef RM_GRAPH_ID
#define RM_GRAPH_ID RM_GENERIC_ID
#endif
#ifndef XLOG_GRAPH_WRITE_FULL_PAGES
#define XLOG_GRAPH_WRITE_FULL_PAGES XLOG_GENERIC_LOG
#endif

namespace gs_vector {

struct BlockData {
    uint16_t st = 0;
    uint16_t datalen = 0;
};

const int SIZE_OF_BLOCK_DATA = (offsetof(BlockData, datalen) + sizeof(uint16_t));

#define BLOCK_MAX_COUNT 32
#define RESERVED_BUFFER_COUNT 8

struct GVAtomicImpl : public annlite::light_env::Atomic {
    static constexpr size_t block_max = BLOCK_MAX_COUNT;
    static constexpr size_t buffer_max = BLOCK_MAX_COUNT + RESERVED_BUFFER_COUNT;
    static constexpr size_t bytes_max = block_max * BLCKSZ;

    using Allocator = annlite::light_env::Allocator;

    struct WalBufferState : public annlite::light_env::BlockModification {
        WalBufferState(Buffer _buffer = InvalidBuffer, BlockNumber _blkno = InvalidBlockNumber,
            bool _released = false, bool _need_init = false)
            : buffer(_buffer), blkno(_blkno), dirty_state(), released(_released), need_init(_need_init),
              reserved(0), init_specialsize(0)
        {}
        bool is_empty() const override {return dirty_state.is_empty(); }
        void append(size_t offset, size_t len, annlite::light_env::BlockModifyType type) override
        {
            dirty_state.append(offset, len, type);
        }
        void merge(const BlockModification& other) override { dirty_state.merge(other); }
        void clear() override { dirty_state.clear(); }
        void mark_dirty(bool value) override { dirty_state.mark_dirty(value); }
        bool is_marked_dirty() const override { return dirty_state.is_marked_dirty(); }

        Buffer buffer;
        BlockNumber blkno;
        annlite::toolkit::DirtyState dirty_state;
        uint32_t released : 1;
        uint32_t need_init : 1;
        uint32_t reserved : 30;
        uint32_t init_specialsize;
    };

    explicit GVAtomicImpl(RelationData* index)
        : m_index(index), m_alloc(), m_rmid(0), m_info(0), m_started(false),
          m_blockid(0), m_buffers(), m_buffercount(0)
    {}

    bool is_enabled() const { return m_started && RelationNeedsWAL(m_index); }

    void destructor() {}

    void unlock_release_buffer()
    {
        for (size_t i = 0; i < m_buffercount; i++) {
            if (!m_buffers[i].released) {
                UnlockReleaseBuffer(m_buffers[i].buffer);
            }
        }
    }

    void start(const RmgrId rmid = RM_GRAPH_ID, const uint8_t info = 0)
    {
        m_started = true;
        m_info = info;
        m_rmid = rmid;
        m_buffercount = 0;
    }

    void start_like(annlite::light_env::Atomic* atomic)
    {
        auto* other = static_cast<GVAtomicImpl*>(atomic);
        if (other != nullptr) {
            RmgrId rmid = 0;
            uint8_t info = 0;
            other->get_base_info(m_alloc, rmid, info);
            start(rmid, info);
        }
    }

    void end()
    {
        if (m_started == false) {
            return;
        }
        end_flush_dirty_interval();
    }

    void end_flush_dirty_interval()
    {
        Assert(m_started);
        
        size_t totalblocks = 0;
        for (size_t i = 0; i < m_buffercount; i++) {
            if (!m_buffers[i].released) {
                totalblocks++;
            }
        }

        if (!is_enabled() || totalblocks == 0) {
            if (totalblocks > 0) {
                unlock_release_buffer();
            }
            m_buffercount = 0;
            m_started = false;
            return;
        }
        static constexpr int TWO = 2;

        XLogEnsureRecordSpace((int)block_max,
            (int)(totalblocks * annlite::toolkit::DirtyState::block_types * TWO));

        START_CRIT_SECTION();
        XLogRecPtr recptr;
        size_t blocks = 0;
        XLogBeginInsert();
        BlockData block_datas[BLOCK_MAX_COUNT * 4];
        uint8_t block_data_count[BLOCK_MAX_COUNT];
        size_t idx = 0;

        for (size_t i = 0; i < m_buffercount; i++) {
            if (m_buffers[i].released) {
                continue;
            }

            Buffer buf = m_buffers[i].buffer;
            Page page = BufferGetPage(buf);
            MarkBufferDirty(buf);

            if (m_buffers[i].need_init) {
                XLogRegisterBuffer((uint8_t)blocks, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
            } else {
                block_data_count[blocks] = 0;
                for (uint8_t j = 0; j < annlite::toolkit::DirtyState::block_types; j++) {
                    uint16_t datalen = m_buffers[i].dirty_state.blocks[j].ed - m_buffers[i].dirty_state.blocks[j].st;
                    if (datalen == 0) {
                        continue;
                    }
                    block_data_count[blocks]++;
                }

                XLogRegisterBuffer((uint8_t)blocks, buf, REGBUF_STANDARD);
                /*
                 * Generic WAL expects each block payload to be a sequence of
                 * uint16 offset, uint16 length, and bytes.  Keep the count
                 * local for deciding whether there is payload, but do not
                 * serialize it into the WAL record.
                 */
                if (block_data_count[blocks] != 0) {
                    for (uint8_t j = 0; j < annlite::toolkit::DirtyState::block_types; j++) {
                        uint16_t datalen = m_buffers[i].dirty_state.blocks[j].ed -
                                            m_buffers[i].dirty_state.blocks[j].st;
                        if (datalen == 0) {
                            continue;
                        }
                        block_datas[idx].st = m_buffers[i].dirty_state.blocks[j].st;
                        Assert(0 <= block_datas[idx].st && block_datas[idx].st <= BLCKSZ);
                        block_datas[idx].datalen = datalen;
                        Assert((size_t)block_datas[idx].st + block_datas[idx].datalen <= BLCKSZ);
                        XLogRegisterBufData((uint8_t)blocks, (char*)&block_datas[idx], SIZE_OF_BLOCK_DATA);
                        XLogRegisterBufData((uint8_t)blocks, (char*)page +
                                                m_buffers[i].dirty_state.blocks[j].st, datalen);
                        idx++;
                    }
                }
            }
            blocks++;
            Assert(blocks < block_max);
        }

        Assert(totalblocks == blocks);
        recptr = XLogInsert(m_rmid, m_info, RelationGetBktid(m_index));
        
        for (size_t i = 0; i < m_buffercount; i++) {
            if (m_buffers[i].released) {
                continue;
            }
            Page page = BufferGetPage(m_buffers[i].buffer);
            PageSetLSN(page, recptr);
        }
        END_CRIT_SECTION();

        for (size_t i = 0; i < m_buffercount; i++) {
            if (!m_buffers[i].released) {
                UnlockReleaseBuffer(m_buffers[i].buffer);
            }
        }

        m_buffercount = 0;
        m_started = false;
    }

    size_t append(BlockNumber block_id, Buffer buffer)
    {
        Assert(m_started);
        Assert(m_buffercount < buffer_max);
        m_buffers[m_buffercount++] = WalBufferState(buffer, block_id, false, false);
        Assert(m_buffercount <= buffer_max);
        return m_buffercount - 1;
    }

    void mark_dirty(Buffer buffer, const annlite::toolkit::DirtyState& state)
    {
        size_t offset = 0;
        for (offset = 0; offset < m_buffercount; ++offset) {
            if (!m_buffers[offset].released && m_buffers[offset].buffer == buffer) {
                break;
            }
        }
        Assert(offset < m_buffercount);
        m_buffers[offset].dirty_state.merge(state);
    }

    void release(Buffer buffer)
    {
        int i = 0;
        Assert(m_buffercount > 0);
        for (i = (int)m_buffercount - 1; i >= 0; --i) {
            if (m_buffers[i].buffer == buffer) {
                break;
            }
        }
        Assert(i < (int)m_buffercount);
        m_buffers[i].released = 1;
        if (i == (int)m_buffercount - 1) {
            m_buffercount--;
        }
    }

    void mark_init(Buffer buffer, size_t specialsize = 0)
    {
        int i = 0;
        for (i = 0; i < (int)m_buffercount; ++i) {
            if (m_buffers[i].buffer == buffer) {
                break;
            }
        }
        Assert(i < (int)m_buffercount);
        m_buffers[i].need_init = 1;
    }

    Buffer reopen(BlockNumber blkno, annlite::light_env::BlockModification*& state)
    {
        for (size_t i = 0; i < m_buffercount; ++i) {
            if (!m_buffers[i].released && m_buffers[i].blkno == blkno) {
                return m_buffers[i].buffer;
            }
        }
        return InvalidBuffer;
    }

    void get_base_info(Allocator*& alloc, RmgrId& rmid, uint8_t& info) const
    {
        alloc = m_alloc;
        rmid = m_rmid;
        info = m_info;
    }

    WalBufferState& buffer_state(size_t offset)
    {
        return m_buffers[offset];
    }

private:
    RelationData* m_index;
    Allocator* m_alloc;
    RmgrId m_rmid;
    uint8_t m_info;
    bool m_started;
    uint8_t m_blockid;
    WalBufferState m_buffers[buffer_max];
    size_t m_buffercount;
};

}  // namespace gs_vector

#endif /* GV_ATOMIC_IMPL_H */
