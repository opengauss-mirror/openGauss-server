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
 * aset.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/aset.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ASET_H_
#define ASET_H_
#include "postgres.h"
#include "utils/memutils.h"

// GUC variables for uncontrolled_memory_context which limit memory alloctations.
typedef struct memory_context_list {
    char* value;
    struct memory_context_list* next;
} memory_context_list;

/*
 * list all templated value used by method function
 */
extern volatile ThreadId ReaperBackendPID;
typedef enum { IS_PROTECT = 0x0001, IS_SHARED = 0x0010, IS_TRACKED = 0x0100 } template_value;

typedef enum {
    MEM_THRD,
    MEM_SESS,
    MEM_SHRD,
} MemType;

class GenericMemoryAllocator {
public:
    static MemoryContext AllocSetContextCreate(_in_ MemoryContext parent, _in_ const char* name,
        _in_ Size minContextSize, _in_ Size initBlockSize, _in_ Size maxBlockSize, _in_ Size maxSize,
        _in_ bool isShared, _in_ bool isSession);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void* AllocSetAlloc(_in_ MemoryContext context, _in_ Size align, _in_ Size size, const char* file, int line);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetFree(_in_ MemoryContext context, _in_ void* pointer);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void* AllocSetRealloc(
        _in_ MemoryContext context, _in_ void* pointer, _in_ Size align, _in_ Size size, const char* file, int line);

    static void AllocSetInit(_in_ MemoryContext context);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetReset(_in_ MemoryContext context);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetDelete(_in_ MemoryContext context);

    static Size AllocSetGetChunkSpace(_in_ MemoryContext context, _in_ void* pointer);

    static bool AllocSetIsEmpty(_in_ MemoryContext context);

    static void AllocSetStats(_in_ MemoryContext context, _in_ int level);

#ifdef MEMORY_CONTEXT_CHECKING
    static void AllocSetCheck(_in_ MemoryContext context);
#endif

private:
    static void AllocSetContextSetMethods(_in_ unsigned long value, MemoryContextMethods* method);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetMethodDefinition(MemoryContextMethods* method);
};

class AsanMemoryAllocator {
public:
    static MemoryContext AllocSetContextCreate(_in_ MemoryContext parent, _in_ const char* name,
        _in_ Size minContextSize, _in_ Size initBlockSize, _in_ Size maxBlockSize, _in_ Size maxSize,
        _in_ bool isShared, _in_ bool isSession);
    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void* AllocSetAlloc(_in_ MemoryContext context, _in_ Size align, _in_ Size size, const char* file, int line);
    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetFree(_in_ MemoryContext context, _in_ void* pointer);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void* AllocSetRealloc(
        _in_ MemoryContext context, _in_ void* pointer, _in_ Size align, _in_ Size size, const char* file, int line);

    static void AllocSetInit(_in_ MemoryContext context);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetReset(_in_ MemoryContext context);

    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetDelete(_in_ MemoryContext context);

    static Size AllocSetGetChunkSpace(_in_ MemoryContext context, _in_ void* pointer);

    static bool AllocSetIsEmpty(_in_ MemoryContext context);

    static void AllocSetStats(_in_ MemoryContext context, _in_ int level);
#ifdef MEMORY_CONTEXT_CHECKING
    static void AllocSetCheck(_in_ MemoryContext context);
#endif
private:
    static void AllocSetContextSetMethods(_in_ unsigned long value, MemoryContextMethods* method);
    template <bool memoryprotect_enable, bool is_shared, bool is_tracked>
    static void AllocSetMethodDefinition(MemoryContextMethods* method);
};

class AlignMemoryAllocator {
public:
    static MemoryContext AllocSetContextCreate(_in_ MemoryContext parent, _in_ const char* name,
        _in_ Size minContextSize, _in_ Size initBlockSize, _in_ Size maxBlockSize, _in_ Size maxSize,
        _in_ bool isShared, _in_ bool isSession);

    template <bool is_shared, bool is_tracked>
    static void* AllocSetAlloc(_in_ MemoryContext context, _in_ Size align, _in_ Size size, const char* file, int line);

    template <bool is_shared, bool is_tracked>
    static void AllocSetFree(_in_ MemoryContext context, _in_ void* pointer);

    template <bool is_shared, bool is_tracked>
    static void* AllocSetRealloc(
        _in_ MemoryContext context, _in_ void* pointer, _in_ Size align, _in_ Size size, const char* file, int line);

    static void AllocSetInit(_in_ MemoryContext context);

    template <bool is_shared, bool is_tracked>
    static void AllocSetReset(_in_ MemoryContext context);

    template <bool is_shared, bool is_tracked>
    static void AllocSetDelete(_in_ MemoryContext context);

    static Size AllocSetGetChunkSpace(_in_ MemoryContext context, _in_ void* pointer);

    static bool AllocSetIsEmpty(_in_ MemoryContext context);

    static void AllocSetStats(_in_ MemoryContext context, _in_ int level);

#ifdef MEMORY_CONTEXT_CHECKING
    static void AllocSetCheck(_in_ MemoryContext context);
#endif

private:
    static void AllocSetContextSetMethods(_in_ unsigned long value, MemoryContextMethods* method);

    template <bool is_shared, bool is_tracked>
    static void AllocSetMethodDefinition(MemoryContextMethods* method);
};

// a stack-based memory allocator which
// 1) do not support single pointer free
// 2) more efficient than index-based memory allocator
// 3) do not contain header overhead.
class StackMemoryAllocator {
public:
    static MemoryContext AllocSetContextCreate(_in_ MemoryContext parent, _in_ const char* name,
        _in_ Size minContextSize, _in_ Size initBlockSize, _in_ Size maxBlockSize, _in_ Size maxSize,
        _in_ bool isShared, _in_ bool isSession);

    template <bool is_tracked>
    static void* AllocSetAlloc(
        _in_ MemoryContext context, _in_ Size align, _in_ Size size, _in_ const char* file, _in_ int line);

    static void AllocSetFree(_in_ MemoryContext context, _in_ void* pointer);

    static void* AllocSetRealloc(_in_ MemoryContext context, _in_ void* pointer, _in_ Size align, _in_ Size size,
        _in_ const char* file, _in_ int line);

    static void AllocSetInit(_in_ MemoryContext context);

    template <bool is_tracked>
    static void AllocSetReset(_in_ MemoryContext context);

    template <bool is_tracked>
    static void AllocSetDelete(_in_ MemoryContext context);

    static Size AllocSetGetChunkSpace(_in_ MemoryContext context, _in_ void* pointer);

    static bool AllocSetIsEmpty(_in_ MemoryContext context);

    static void AllocSetStats(_in_ MemoryContext context, _in_ int level);

#ifdef MEMORY_CONTEXT_CHECKING
    static void AllocSetCheck(_in_ MemoryContext context);
#endif

private:
    static void AllocSetContextSetMethods(_in_ unsigned long value, MemoryContextMethods* method);

    template <bool is_tracked>
    static void AllocSetMethodDefinition(MemoryContextMethods* method);
};

class MemoryProtectFunctions {
public:
    template <MemType mem_type>
    static void* gs_memprot_malloc(Size sz, bool needProtect);

    template <MemType mem_type>
    static void gs_memprot_free(void* ptr, Size sz);

    template <MemType mem_type>
    static void* gs_memprot_realloc(void* ptr, Size sz, Size newsz, bool needProtect);

    template <MemType mem_type>
    static int gs_posix_memalign(void** memptr, Size alignment, Size sz, bool needProtect);

    template <MemType mem_type>
    static bool gs_memprot_reserve(Size sz, bool needProtect);

    template <MemType mem_type>
    static void gs_memprot_release(Size sz);
};

extern int alloc_trunk_size(int width);

#endif /* ASET_H_ */
