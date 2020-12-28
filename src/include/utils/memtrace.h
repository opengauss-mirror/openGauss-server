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
 * ---------------------------------------------------------------------------------------
 * 
 * memtrace.h
 * 
 * IDENTIFICATION
 *        src/include/utils/memtrace.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef MEMTRACE_H
#define MEMTRACE_H

#include "c.h"
#include "utils/aset.h"
#include "nodes/memnodes.h"

typedef struct MemoryAllocInfo {
    void* pointer;
    const char* file;
    int line;
    Size size;
    MemoryContext context;
    char context_name[NAMEDATALEN];
} MemoryAllocInfo;

typedef struct MemoryAllocDetailKey {
    char name[NAMEDATALEN];
    const char* file;
    int line;
} MemoryAllocDetailKey;

typedef struct MemoryAllocDetail {
    MemoryAllocDetailKey detail_key;
    Size size;
} MemoryAllocDetail;

typedef struct MemoryAllocDetailList {
    MemoryAllocDetail* entry;
    MemoryAllocDetailList* next;
} MemoryAllocDetailList;

extern Datum track_memory_context(PG_FUNCTION_ARGS);
extern bool MemoryContextShouldTrack(const char* name);
extern void InsertTrackMemoryInfo(const void* pointer, const MemoryContext context, const char* file, int line, Size size);
extern void RemoveTrackMemoryInfo(const void* pointer);
extern void RemoveTrackMemoryContext(const MemoryContext context);
extern MemoryAllocDetailList* GetMemoryTrackInfo();

#endif
