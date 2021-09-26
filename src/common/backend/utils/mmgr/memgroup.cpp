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
 * memgroup.cpp
 *    Add memory context group
 *
 * IDENTIFICATION
 *    src/common/backend/utils/mmgr/memgroup.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memgroup.h"
#include "utils/memutils.h"

const char* MemoryContextGroup::memory_context_group_name[MEMORY_CONTEXT_MAX] = {
    "CBBTopMemoryContext",
    "CommunicationTopMemoryContext",
    "DefaultTopMemoryContext",
    "DFXTopMemoryContext",
    "ExecutorTopMemoryContext",
    "NumaTopMemoryContext",
    "OptimizerTopMemoryContext",
    "SecurityTopMemoryContext",
    "StorageTopMemoryContext",
    "TSDBTopMemoryContext",
    "StreamingTopMemoryContext",
    "AITopMemoryContext"
};

MemoryContextGroup::MemoryContextGroup()
{
    int i ;
    for (i = 0; i < MEMORY_CONTEXT_MAX; i++) {
        memory_context_group[i] = NULL;
    }
}

void MemoryContextGroup::Init(MemoryContext parent, bool is_shared)
{
    MemoryContextType type;
    int i;

    if (is_shared) {
        type = SHARED_CONTEXT;
    } else {
        type = STANDARD_CONTEXT;
    }

    for (i = 0; i < MEMORY_CONTEXT_MAX; i++) {
        memory_context_group[i] =
            AllocSetContextCreate(parent,
                                  memory_context_group_name[i],
                                  ALLOCSET_DEFAULT_MINSIZE,
                                  ALLOCSET_DEFAULT_INITSIZE,
                                  ALLOCSET_DEFAULT_MAXSIZE,
                                  type);
    }
}

MemoryContext const MemoryContextGroup::GetMemCxtGroup(MemoryGroupType type)
{
    return memory_context_group[type];
}
