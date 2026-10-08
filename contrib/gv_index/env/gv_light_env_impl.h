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
 * gv_light_env_impl.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_light_env_impl.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_LIGHT_ENV_IMPL_H
#define GVGRAPH_LIGHT_ENV_IMPL_H

#include "c.h"
#include "utils/rel.h"

#include "gv_allocator_impl.h"
#include "gv_atomic_impl.h"
#include "gv_datasource.h"
#include "gv_event_handler_impl.h"
#include "gv_page_storage.h"
#include "gv_thread_impl.h"

#include "lite/index/light_env.h"
#include "lite/index/toolkit_env.h"
#include "lite/index/page_based_toolkit_env.h"

namespace gs_vector {

struct GVLightEnvImpl : public annlite::LightEnv {
    using ArrayType = annlite::light_env::ArrayType;
    using ArrayIdType = annlite::light_env::ArrayIdType;

    template <typename Key, typename Value>
    using DataSourceImpl = gs_vector::GVDataSourceImpl<Key, Value>;

    explicit GVLightEnvImpl(RelationData* index)
        : annlite::LightEnv(),
          m_index(index),
          m_allocator(),
          m_threadpool(),
          m_storage(this, index),
          m_eventhandler()
    {
        m_threadpool.set_index_oid(RelationGetRelid(index));
    }

    RelationData* index_relation() const { return m_index; }

    annlite::light_env::Allocator* get_allocator() override
    {
        return &m_allocator;
    }

    annlite::light_env::EventHandler* get_event_handler() override
    {
        return &m_eventhandler;
    }

    annlite::light_env::ComputeTaskRunner* get_compute_task_runner() override
    {
        return &m_threadpool;
    }

    annlite::light_env::PageStorage* get_page_storage() override
    {
        return &m_storage;
    }

    annlite::light_env::Atomic* create_atomic() override IMPLEMENT_CHECK

    annlite::light_env::Atomic* destroy_atomic() override IMPLEMENT_CHECK

    ArrayIdType first_array_id() const override { return 128; }
    ArrayIdType create_array(ArrayType type, size_t item_size, size_t item_count = 0) override IMPLEMENT_CHECK
    bool reinit_array(ArrayIdType id, size_t item_size, size_t item_count = 0) override IMPLEMENT_CHECK
    bool extend_array(ArrayIdType arr_id, size_t old_size, size_t new_size, size_t item_size) override IMPLEMENT_CHECK
    void destory_array(ArrayIdType arr_id) override IMPLEMENT_CHECK

private:
    RelationData* m_index;
    GVAllocatorImpl m_allocator;
    GVThreadPoolImpl m_threadpool;
    GVPageStorage m_storage;
    GVEventHandlerImpl m_eventhandler;
};

} // namespace gs_vector

#endif /* GV_LIGHT_ENV_IMPL_H */
