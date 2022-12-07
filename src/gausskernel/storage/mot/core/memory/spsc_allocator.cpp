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
 * spsc_allocator.cpp
 *    SPSC Variable size allocator implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "spsc_allocator.h"
#include "sys_numa_api.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(SPSCVarSizeAllocator, Memory)

SPSCVarSizeAllocator* SPSCVarSizeAllocator::GetSPSCAllocator(uint32_t size)
{
    SPSCVarSizeAllocator* res = nullptr;

    if (size > SPSC_ALLOCATOR_MAXSIZE) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Allocate SPSCVarSizeAllocator",
            "Requested size %u exceeds max allowed %u",
            size,
            SPSC_ALLOCATOR_MAXSIZE);
        return res;
    }

    void* buf = MotSysNumaAllocInterleaved(size);
    if (buf == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Allocate SPSCVarSizeAllocator", "Failed to allocate memory of %u size", size);
    } else {
        res = new (buf) SPSCVarSizeAllocator(size);
    }

    return res;
}

void SPSCVarSizeAllocator::FreeSPSCAllocator(SPSCVarSizeAllocator* spsc)
{
    if (spsc != nullptr) {
        MotSysNumaFree((void*)spsc, spsc->GetSize());
    }
}
}  // namespace MOT