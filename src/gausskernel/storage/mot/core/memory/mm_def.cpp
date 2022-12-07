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
 * mm_def.cpp
 *    Common memory management definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_def.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_def.h"
#include <cstring>

namespace MOT {

#define MEM_RESERVE_VIRTUAL_STR "virtual"
#define MEM_RESERVE_PHYSICAL_STR "physical"

extern MemReserveMode MemReserveModeFromString(const char* reserveModeStr)
{
    MemReserveMode result = MEM_RESERVE_INVALID;
    if (strcmp(reserveModeStr, MEM_RESERVE_VIRTUAL_STR) == 0) {
        result = MEM_RESERVE_VIRTUAL;
    } else if (strcmp(reserveModeStr, MEM_RESERVE_PHYSICAL_STR) == 0) {
        result = MEM_RESERVE_PHYSICAL;
    }
    return result;
}

extern const char* MemReserveModeToString(MemReserveMode reserveMode)
{
    switch (reserveMode) {
        case MEM_RESERVE_VIRTUAL:
            return MEM_RESERVE_VIRTUAL_STR;

        case MEM_RESERVE_PHYSICAL:
            return MEM_RESERVE_PHYSICAL_STR;

        default:
            return "N/A";
    }
}

#define MEM_STORE_COMPACT_STR "compact"
#define MEM_STORE_EXPANDING_STR "expanding"

extern MemStorePolicy MemStorePolicyFromString(const char* storePolicyStr)
{
    MemStorePolicy result = MEM_STORE_INVALID;
    if (strcmp(storePolicyStr, MEM_STORE_COMPACT_STR) == 0) {
        result = MEM_STORE_COMPACT;
    } else if (strcmp(storePolicyStr, MEM_STORE_EXPANDING_STR) == 0) {
        result = MEM_STORE_EXPANDING;
    }
    return result;
}

extern const char* MemStorePolicyToString(MemStorePolicy storePolicy)
{
    switch (storePolicy) {
        case MEM_STORE_COMPACT:
            return MEM_STORE_COMPACT_STR;

        case MEM_STORE_EXPANDING:
            return MEM_STORE_EXPANDING_STR;

        default:
            return "N/A";
    }
}

#define MEM_ALLOC_GLOBAL_STR "global"
#define MEM_ALLOC_LOCAL_STR "local"

extern const char* MemAllocTypeToString(MemAllocType allocType)
{
    switch (allocType) {
        case MEM_ALLOC_GLOBAL:
            return MEM_ALLOC_GLOBAL_STR;

        case MEM_ALLOC_LOCAL:
            return MEM_ALLOC_LOCAL_STR;

        default:
            return "N/A";
    }
}

#define MEM_ALLOC_POLICY_PAGE_INTERLEAVED_STR "page-interleaved"
#define MEM_ALLOC_POLICY_CHUNK_INTERLEAVED_STR "chunk-interleaved"
#define MEM_ALLOC_POLICY_LOCAL_STR "local"
#define MEM_ALLOC_POLICY_NATIVE_STR "native"
#define MEM_ALLOC_POLICY_AUTO_STR "auto"

extern MemAllocPolicy MemAllocPolicyFromString(const char* allocPolicyStr)
{
    MemAllocPolicy result = MEM_ALLOC_POLICY_INVALID;
    if (strcmp(allocPolicyStr, MEM_ALLOC_POLICY_PAGE_INTERLEAVED_STR) == 0) {
        result = MEM_ALLOC_POLICY_PAGE_INTERLEAVED;
    } else if (strcmp(allocPolicyStr, MEM_ALLOC_POLICY_CHUNK_INTERLEAVED_STR) == 0) {
        result = MEM_ALLOC_POLICY_CHUNK_INTERLEAVED;
    } else if (strcmp(allocPolicyStr, MEM_ALLOC_POLICY_LOCAL_STR) == 0) {
        result = MEM_ALLOC_POLICY_LOCAL;
    } else if (strcmp(allocPolicyStr, MEM_ALLOC_POLICY_NATIVE_STR) == 0) {
        result = MEM_ALLOC_POLICY_NATIVE;
    } else if (strcmp(allocPolicyStr, MEM_ALLOC_POLICY_AUTO_STR) == 0) {
        result = MEM_ALLOC_POLICY_AUTO;
    }
    return result;
}

extern const char* MemAllocPolicyToString(MemAllocPolicy allocPolicy)
{
    switch (allocPolicy) {
        case MEM_ALLOC_POLICY_PAGE_INTERLEAVED:
            return MEM_ALLOC_POLICY_PAGE_INTERLEAVED_STR;

        case MEM_ALLOC_POLICY_CHUNK_INTERLEAVED:
            return MEM_ALLOC_POLICY_CHUNK_INTERLEAVED_STR;

        case MEM_ALLOC_POLICY_LOCAL:
            return MEM_ALLOC_POLICY_LOCAL_STR;

        case MEM_ALLOC_POLICY_NATIVE:
            return MEM_ALLOC_POLICY_NATIVE_STR;

        case MEM_ALLOC_POLICY_AUTO:
            return MEM_ALLOC_POLICY_AUTO_STR;

        default:
            return "N/A";
    }
}

extern bool ValidateMemReserveMode(const char* reserveModeStr)
{
    if (MemReserveModeFromString(reserveModeStr) == MemReserveMode::MEM_RESERVE_INVALID) {
        return false;
    }
    return true;
}

extern bool ValidateMemStorePolicy(const char* storePolicyStr)
{
    if (MemStorePolicyFromString(storePolicyStr) == MemStorePolicy::MEM_STORE_INVALID) {
        return false;
    }
    return true;
}

extern bool ValidateMemAllocPolicy(const char* allocPolicyStr)
{
    if (MemAllocPolicyFromString(allocPolicyStr) == MemAllocPolicy::MEM_ALLOC_POLICY_INVALID) {
        return false;
    }
    return true;
}
}  // namespace MOT
