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
 * memgroup.h
 *    Add memory context group
 *
 * IDENTIFICATION
 *    src/include/utils/memgroup.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MEMGROUP_H
#define MEMGROUP_H

#include "utils/palloc.h"

typedef enum {
    MEMORY_CONTEXT_CBB,
    MEMORY_CONTEXT_COMMUNICATION,
    MEMORY_CONTEXT_DEFAULT,
    MEMORY_CONTEXT_DFX,
    MEMORY_CONTEXT_EXECUTOR,
    MEMORY_CONTEXT_NUMA,
    MEMORY_CONTEXT_OPTIMIZER,
    MEMORY_CONTEXT_SECURITY,
    MEMORY_CONTEXT_STORAGE,
    MEMORY_CONTEXT_TSDB,
    MEMORY_CONTEXT_STREAMING,
    MEMORY_CONTEXT_AI,
    MEMORY_CONTEXT_MAX
} MemoryGroupType;

class MemoryContextGroup : public BaseObject {
public:
    MemoryContextGroup();
    ~MemoryContextGroup() {}
    void Init(MemoryContext parent, bool is_shared = false);
    MemoryContext const GetMemCxtGroup(MemoryGroupType type);

private:
    MemoryContext memory_context_group[MEMORY_CONTEXT_MAX];
    static const char* memory_context_group_name[MEMORY_CONTEXT_MAX];
};

#endif   /* MEMGROUP_H */
