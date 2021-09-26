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
 * mm_global_api.h
 *    Global memory API provides objects that can be allocated by one thread,
 *    and be used and de-allocated by other threads.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_global_api.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_GLOBAL_API_H
#define MM_GLOBAL_API_H

#include "string_buffer.h"
#include "utilities.h"
#include "mm_session_allocator.h"

#include <stddef.h>

// unlike session-local memory API, which provides session-local objects that can be used only in session context,
// global memory API provides objects that can be allocated by one thread, and be used and deallocated by other threads.
// while session-local memory API relies on local chunk pools, the global memory API relies on global chunk pools.
// there is a global memory allocator per NUMA node.

namespace MOT {
/** @typedef MemGlobalAllocatorStats. A synonym for session statistics. */
typedef MemSessionAllocatorStats MemGlobalAllocatorStats;

/**
 * @brief Initialize the global memory API.
 * @return Zero if succeeded, otherwise an error code.
 * @note The thread id pool must be already initialized.
 * @see InitThreadIdPool().
 */
extern int MemGlobalApiInit();

/**
 * @brief Destroys the global memory API.
 */
extern void MemGlobalApiDestroy();

/**
 * @brief Allocates memory from the global allocator bound to the current NUMA node.
 * @param sizeBytes The allocation size in bytes.
 * @return A pointer to the allocated memory or NULL if allocation failed (i.e. out of memory).
 */
extern void* MemGlobalAlloc(uint64_t sizeBytes);

/**
 * @brief Allocates aligned memory from the global allocator bound to the current NUMA node.
 * @param sizeBytes The allocation size in bytes.
 * @param alignment The requested alignment in bytes. Must be a power of two.
 * @return A pointer to the allocated memory or NULL if allocation failed (i.e. out of memory).
 */
extern void* MemGlobalAllocAligned(uint64_t sizeBytes, uint32_t alignment);

/**
 * @brief Allocates memory from the global allocator bound to the specified NUMA node.
 * @param sizeBytes The allocation size in bytes.
 * @param node The NUMA node identifier.
 * @return A pointer to the allocated memory or NULL if allocation failed (i.e. out of memory).
 */
extern void* MemGlobalAllocOnNode(uint64_t sizeBytes, int node);

/**
 * @brief Allocates aligned memory from the global allocator bound to the specified NUMA node.
 * @param sizeBytes The allocation size in bytes.
 * @param alignment The requested alignment in bytes. Must be a power of two.
 * @param node The NUMA node identifier.
 * @return A pointer to the allocated memory or NULL if allocation failed (i.e. out of memory).
 */
extern void* MemGlobalAllocAlignedOnNode(uint64_t sizeBytes, uint32_t alignment, int node);

/**
 * @brief Reallocate a block of memory according to requested memory size.
 * @param newSizeBytes The amount of memory requested.
 * @param flags One of the reallocation flags @ref MM_REALLOC_COPY or @ref MM_REALLOC_ZERO.
 * @return The reallocated object or NULL if failed. Call @ref mm_get_last_error() to find out the
 * failure reason. In case of failure the memory region that was supposed to be reallocated is kept intact.
 */
extern void* MemGlobalRealloc(void* object, uint64_t newSizeBytes, MemReallocFlags flags);

/**
 * @brief Reclaims a memory previously allocated from a global allocator.
 * @param object The object to reclaim.
 */
extern void MemGlobalFree(void* object);

/**
 * @brief Prints global memory status into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemGlobalApiPrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps global memory status into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemGlobalApiToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Retrieves global memory allocator statistics on a specific NUMA node.
 * @param node The NUMA node identifier.
 * @param globalStats The global memory allocator statistics.
 */
extern void MemGlobalGetStats(int node, MemGlobalAllocatorStats* globalStats);

/**
 * @brief Prints global memory allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 * @param globalStats The global memory allocator statistics.
 */
extern void MemGlobalPrintStats(const char* name, LogLevel logLevel, MemGlobalAllocatorStats* globalStats);

/**
 * @brief Retrieves global memory consumption statistics on all nodes.
 * @param globalStatsArray The object statistics array.
 * @param nodeCount The number of entries in the given array (should be the active node count).
 * @return The actual number of nodes for which statistics were reported.
 */
extern uint32_t MemGlobalGetAllStats(MemGlobalAllocatorStats* globalStatsArray, uint32_t nodeCount);

/**
 * @brief Prints all object allocator statistics to log.
 * @param name The name of the report to print.
 * @param logLevel The log level to use in printing.
 */
extern void MemGlobalPrintAllStats(const char* name, LogLevel logLevel);

/**
 * @brief Prints to log a summary report of all global memory allocators.
 * @param name The name of the report to print.
 * @param logLevel The log level to use in printing.
 * @param[opt] fullReport Specifies whether to print an elaborate report.
 */
extern void MemGlobalPrintSummary(const char* name, LogLevel logLevel, bool fullReport = false);

// forward declarations
template <typename T>
class MemGlobalPtr;

/**
 * @brief Allocate a typed object based on the global memory allocator of the current NUMA node.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline T* MemGlobalAllocObject(Args&&... args)
{
    T* object = NULL;
    void* buffer = MemGlobalAlloc(sizeof(T));
    if (buffer != NULL) {
        object = new (buffer) T(std::forward<Args>(args)...);
    }
    return object;
}

/**
 * @brief Allocate an aligned typed object based on the global memory allocator of the current NUMA node.
 * @param alignment The requested alignment in bytes.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline T* MemGlobalAllocAlignedObject(uint32_t alignment, Args&&... args)
{
    T* object = NULL;
    void* buffer = MemGlobalAllocAligned(sizeof(T), alignment);
    if (buffer != NULL) {
        object = new (buffer) T(std::forward<Args>(args)...);
    }
    return object;
}

/**
 * @brief Allocate a typed object based on the global memory allocator of a specific NUMA node.
 * @param node The NUMA node identifier.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline T* MemGlobalAllocOnNodeObject(int node, Args&&... args)
{
    T* object = NULL;
    void* buffer = MemGlobalAllocOnNode(sizeof(T), node);
    if (buffer != NULL) {
        object = new (buffer) T(std::forward<Args>(args)...);
    }
    return object;
}

/**
 * @brief Allocate an aligned typed object based on the global memory allocator of a specific NUMA node.
 * @param alignment The requested alignment in bytes.
 * @param node The NUMA node identifier.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline T* MemGlobalAllocAlignedOnNodeObject(uint32_t alignment, int node, Args&&... args)
{
    T* object = NULL;
    void* buffer = MemGlobalAllocAlignedOnNode(sizeof(T), alignment, node);
    if (buffer != NULL) {
        object = new (buffer) T(std::forward<Args>(args)...);
    }
    return object;
}

/**
 * @brief Deallocates a typed object previously allocated with any one of the calls to @ref MemAllocObject(), @ref
 * MemAllocAlignedObject(), @ref MemAllocOnNodeObject() or @ref MemAllocAlignedOnNodeObject().
 * @param object The object to deallocate.
 */
template <typename T>
inline void MemGlobalFreeObject(T* object)
{
    if (object) {
        object->~T();
        MemGlobalFree(object);
    }
}

/**
 * @class MemGlobalPtr
 * @brief A template class for unique (i.e. cannot be copied or assigned) pointer to object allocated on the global
 * memory.
 */
template <typename T>
class MemGlobalPtr {
public:
    /**
     * @brief Default constructor for a NULL pointer.
     */
    MemGlobalPtr() : m_ptr(nullptr)
    {}

    /**
     * @brief Constructs an object on pre-allocated memory buffer.
     * @param target Address of memory buffer for placement new.
     */
    explicit MemGlobalPtr(T* target) : m_ptr(target)
    {}

    /**
     * @brief Copy-move constructor. The underlying managed object is passed from the parameter to this object. The
     * parameter object loses its reference to the underlying object.
     * @param other The moved object.
     */
    MemGlobalPtr(MemGlobalPtr<T>&& other) : m_ptr(other.m_ptr)
    {
        other.m_ptr = nullptr;
    }

    /**
     * @brief Retrieves a pointer to the managed object.
     * @return A pointer to the object.
     */
    inline T* Get()
    {
        return m_ptr;
    }

    /**
     * @brief Dereference operator implementation.
     * @return A reference to the managed object.
     */
    inline T& operator*()
    {
        return *m_ptr;
    }

    /**
     * @brief Member access operator implementation.
     * @return Retrieves a pointer to the managed object.
     */
    inline T* operator->()
    {
        return m_ptr;
    }

    /**
     * @brief Retrieves a pointer to the managed object (non-modifying variant).
     * @return A pointer to the object.
     */
    inline T* Get() const
    {
        return m_ptr;
    }

    /**
     * @brief Dereference operator implementation (non-modifying variant).
     * @return A reference to the managed object.
     */
    inline T& operator*() const
    {
        return *m_ptr;
    }

    /**
     * @brief Member access operator implementation (non-modifying variant).
     * @return Retrieves a pointer to the managed object.
     */
    inline T* operator->() const
    {
        return m_ptr;
    }

    /**
     * @brief Assignment-move operator. The underlying managed object is passed from
     * the right-hand-side parameter to this object. The right-hand-side parameter
     * loses its reference to the underlying managed object.
     * @param right The object from which the managed object is moved.
     * @return A reference to this object after the assignment-move is done.
     */
    MemGlobalPtr<T>& operator=(MemGlobalPtr<T>&& right)
    {
        if (this == &right) {
            return *this;
        }
        Reset(right.m_ptr);
        right.m_ptr = nullptr;
        return *this;
    }

    /**
     * @brief Template cast operator.
     * @return An object having a copy of the managed object converted to the target type.
     */
    template <class Other>
    explicit inline operator MemGlobalPtr<Other>()
    {
        T* ptr = this->m_ptr;
        this->m_ptr = nullptr;
        return MemGlobalPtr<Other>(static_cast<Other*>(ptr));
    }

    /**
     * @brief Destructor. Deallocates the managed object.
     */
    ~MemGlobalPtr() noexcept
    {
        Clear();
    }

    /**
     * @brief Deallocates the managed object and resets all members to NULL.
     */
    void Reset()
    {
        Reset(nullptr);
    }

private:
    // Allow private access to other specializations.
    template <class Other>
    friend class MemGlobalPtr;

    /**
     * @brief Deallocates the managed object.
     */
    inline void Clear()
    {
        if (m_ptr) {
            MemGlobalFreeObject<T>(m_ptr);
        }
        m_ptr = nullptr;
    }

    /**
     * @brief Deallocates the managed object and resets it to a new managed object.
     * @param new_ptr The new managed object.
     * @param new_manager The new memory manager to which the new managed object
     * belongs.
     */
    inline void Reset(T* newPtr)
    {
        Clear();
        m_ptr = newPtr;
    }

    /**
     * @var The managed object
     */
    T* m_ptr;

    // No copies allowed. This is a unique_ptr
    /** @cond EXCLUDE_DOC */
    MemGlobalPtr(const MemGlobalPtr<T>& other) = delete;
    MemGlobalPtr<T>& operator=(const MemGlobalPtr<T>& right) = delete;
    /** @endcond */
};

/**
 * @brief Allocate a typed object based on the global chunk pool.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline MemGlobalPtr<T> MemGlobalAllocObjectPtr(Args&&... args)
{
    return std::move(MemGlobalPtr<T>(MemGlobalAllocObject<T>(args...)));
}

/**
 * @brief Allocate an aligned typed object based on the global chunk pool.
 * @param alignment The requested alignment in bytes.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline MemGlobalPtr<T> MemGlobalAllocAlignedObjectPtr(uint32_t alignment, Args&&... args)
{
    return std::move(MemGlobalPtr<T>(MemGlobalAllocAlignedObject<T>(alignment, args...)));
}

/**
 * @brief Allocate a typed object based on the node chunk pool.
 * @param node The NUMA node identifier.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline MemGlobalPtr<T> MemGlobalAllocOnNodeObjectPtr(int node, Args&&... args)
{
    return std::move(MemGlobalPtr<T>(MemGlobalAllocOnNodeObject<T>(node, args...)));
}

/**
 * @brief Allocate an aligned typed object based on the node chunk pool.
 * @param node The NUMA node identifier.
 * @param alignment The requested alignment in bytes.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline MemGlobalPtr<T> MemGlobalAllocAlignedOnNodeObjectPtr(uint32_t alignment, int node, Args&&... args)
{
    return std::move(MemGlobalPtr<T>(MemGlobalAllocAlignedOnNodeObject<T>(alignment, node, args...)));
}

template <typename T>
class MemGlobalArray {
public:
    /**
     * @brief Default constructor for NULL pointer.
     */
    MemGlobalArray() : m_array(nullptr), m_arraySize(0)
    {}

    /**
     * @brief Copy-move constructor. The underlying managed array is passed from
     * the parameter to this object. The parameter object loses its reference to
     * the underlying array.
     * @param other The moved object.
     */
    MemGlobalArray(MemGlobalArray<T>&& other) : m_array(other.m_array), m_arraySize(other.m_arraySize)
    {
        other.m_array = nullptr;
    }

    /**
     * @brief Constructor for in-place allocation of object array
     * @param manager The memory manager to which the managed object array belongs.
     * @param target A pointer to the managed object array.
     * @param sz The amount of items in the managed array.
     */
    MemGlobalArray(T* target, size_t sz) : m_array(new (target) T[sz]), m_arraySize(sz)
    {}

    /**
     * @brief Subscript operator (non-modifying variant).
     * @param i The requested item index.
     * @return A non-modifiable reference to the item at the requested index.
     */
    inline const T& operator[](size_t i) const
    {
        return m_array[i];
    }

    /**
     * @brief Subscript operator.
     * @param i The requested item index.
     * @return A reference to the item at the requested index.
     */
    inline T& operator[](size_t i)
    {
        return m_array[i];
    }

    /**
     * @brief Assignment-move operator. The underlying managed object is passed from
     * the right-hand-side parameter to this object. The right-hand-side parameter
     * loses its reference to the underlying managed object.
     * @param right The object from which the managed object is moved.
     * @return A reference to this object after the assignment-move is done.
     */
    inline MemGlobalArray<T>& operator=(MemGlobalArray<T>&& right)
    {
        if (this == &right) {
            return *this;
        }
#ifdef MOT_DEBUG
        MOT_ASSERT(this->m_array != right.m_array);  // It is a unique pointer
        if (__builtin_expect(this->m_array != right.m_array, 1)) {
#endif
            Reset(right.m_array, right.m_arraySize);
#ifdef MOT_DEBUG
        }
#endif
        right.m_array = nullptr;
        return *this;
    }

    /**
     * @brief Destructor. Deallocates the managed object.
     */
    ~MemGlobalArray() noexcept
    {
        Clear();
    }

    /**
     * @brief Retrieves a pointer to the managed array (non-modifying variant).
     * @return A pointer to the managed array.
     */
    inline T* Get() const
    {
        return m_array;
    }

    /**
     * @brief Retrieves the array size (i.e. capacity).
     * @return The amount of items the array can hold.
     */
    inline size_t GetArraySize() const
    {
        return m_arraySize;
    }

    /**
     * @brief Deallocates the managed array and resets all members to NULL.
     */
    inline void Reset()
    {
        Reset(nullptr, 0);
    }

private:
    /**
     * @brief Deallocates the managed object.
     */
    inline void Clear()
    {
        if (m_array != nullptr) {
            for (size_t i = 0; i < m_arraySize; i++) {
                (m_array[i]).~T();
            }
            MemGlobalFree(m_array);
        }
    }

    /**
     * @brief Deallocates the managed array and resets it to a new managed array.
     * @param newArray The new managed array.
     * @param arraySize The new array size (i.e. amount of items it can hold).
     */
    inline void Reset(T* newArray, size_t arraySize)
    {
        Clear();
        m_array = newArray;
        m_arraySize = arraySize;
    }

    /**
     * @var The managed array.
     */
    T* m_array;

    /**
     * The managed array size (i.e. capacity - the amount of items it can hold).
     */
    size_t m_arraySize;

    // No copies allowed. This is a unique_ptr
    /** @cond EXCLUDE_DOC */
    MemGlobalArray(const MemGlobalArray<T>& other) = delete;
    MemGlobalArray<T>& operator=(const MemGlobalArray<T>& right) = delete;
    /** @endcond */
};

/**
 * @brief Allocate a typed object array based on the global chunk pool.
 * @param arraySize The array size.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T>
inline MemGlobalArray<T> MemGlobalAllocObjectArray(uint32_t arraySize)
{
    T* inplaceBuffer = (T*)MemGlobalAlloc(sizeof(T) * arraySize);
    return std::move(MemGlobalArray<T>(inplaceBuffer, arraySize));
}

/**
 * @brief Allocate an aligned typed object array based on the global chunk pool.
 * @param alignment The requested alignment in bytes.
 * @param arraySize The array size.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T>
inline MemGlobalArray<T> MemGlobalAllocAlignedObjectArray(uint32_t alignment, uint32_t arraySize)
{
    T* inplaceBuffer = (T*)MemGlobalAllocAligned(sizeof(T) * arraySize, alignment);
    return std::move(MemGlobalArray<T>(inplaceBuffer, arraySize));
}

/**
 * @brief Allocate a typed object array based on the node chunk pool.
 * @param node The NUMA node identifier.
 * @param arraySize The array size.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T>
inline MemGlobalArray<T> MemGlobalAllocOnNodeObjectArray(int node, uint32_t arraySize)
{
    T* inplaceBuffer = (T*)MemGlobalAllocOnNode(sizeof(T) * arraySize, node);
    return std::move(MemGlobalArray<T>(inplaceBuffer, arraySize));
}

/**
 * @brief Allocate an aligned typed object array based on the node chunk pool.
 * @param node The NUMA node identifier.
 * @param alignment The requested alignment in bytes.
 * @param arraySize The array size.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T>
inline MemGlobalArray<T> MemGlobalAllocAlignedOnNodeObjectPtr(uint32_t alignment, int node, uint32_t arraySize)
{
    T* inplaceBuffer = (T*)MemGlobalAllocAlignedOnNode(sizeof(T) * arraySize, alignment, node);
    return std::move(MemGlobalArray<T>(inplaceBuffer, arraySize));
}
}  // namespace MOT

/**
 * @brief Dumps all global memory API status to standard error stream.
 */
extern "C" void MemGlobalApiDump();

/**
 * @brief Analyzes the memory status of a given buffer address.
 * @param address The buffer to analyze.
 * @return Non-zero value if buffer was found.
 */
extern "C" int MemGlobalApiAnalyze(void* buffer);

#endif /* MM_OBJECT_API_H */
