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
 * gc_context.h
 *    Global GC managers pool.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/gc_context.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GC_CONTEXT_H
#define GC_CONTEXT_H

#include "global.h"
#include "mm_gc_manager.h"
#include "utilities.h"
#include "mot_error.h"

namespace MOT {
/**
 * @class GcContext
 * @brief Global GC managers pool
 */
class GcContext {
public:
    GcContext(){};

    ~GcContext()
    {
        if (m_isInitialized) {
            delete[] m_privateHandler;
        }
        m_privateHandler = nullptr;
    };

    /**
     * @brief Initialize all connection with null for max connections
     * @param rcuMaxFreeCount - optional parameter for pre-init all managers
     * @return False on error, and true otherwise
     */
    inline bool Init(int rcuMaxFreeCount = MASSTREE_OBJECT_COUNT_PER_CLEANUP)
    {
        m_isInitialized = false;
        m_privateHandler = new (std::nothrow) GcManager*[MAX_NUM_THREADS];
        if (m_privateHandler == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Initialize GC Context",
                "Failed to allocate memory for %u GcManager object pointers",
                (unsigned)MAX_NUM_THREADS);
            return false;
        }
        for (uint16_t t = 0; t < MAX_NUM_THREADS; ++t) {
            m_privateHandler[t] = nullptr;
        }
        m_isInitialized = true;
        return true;
    }

    inline GcManager* GetGcContext(uint32_t id)
    {
        return m_privateHandler[id];
    }

    inline void SetGcContext(uint32_t id, GcManager* gcMgr)
    {
        m_privateHandler[id] = gcMgr;
    }

private:
    GcManager** m_privateHandler = nullptr;

    bool m_isInitialized = false;

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif /* GC_CONTEXT_H */
