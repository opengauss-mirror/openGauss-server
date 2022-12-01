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
 * mm_session_api.h
 *    Session-local memory API, which provides session-local objects that can be used only in session context.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_api.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_SESSION_API_H
#define MM_SESSION_API_H

#include "session_context.h"
#include "utilities.h"
#include "mm_session_allocator.h"
#include "connection_id.h"

#include <cstdint>
#include <utility>

namespace MOT {
/**
 * @brief Initialize the session API.
 * @param None.
 */
extern int MemSessionApiInit();

/**
 * @brief Destroy the session API.
 * @param None.
 */
extern void MemSessionApiDestroy();

/**
 * @brief Reserve as many chunks as needs from the current node chunk pool.
 * @param sizeBytes The amount of memory you would like to reserve for a session when it begins (in bytes).
 */
extern int MemSessionReserve(uint32_t sizeBytes);

/**
 * @brief Unreserve all chunks allocated for the session before.
 */
extern void MemSessionUnreserve();

/**
 * @brief Allocate a block of memory according to requested memory size.
 * @param sizeBytes The amount of memory requested.
 * @return The allocated object or NULL if failed.
 */
extern void* MemSessionAlloc(uint64_t sizeBytes);

/**
 * @brief Allocate an aligned block of memory according to requested memory size.
 * @param sizeBytes The amount of memory requested.
 * @param alignment The requested alignment in bytes.
 * @return The allocated object or NULL if failed.
 */
extern void* MemSessionAllocAligned(uint64_t sizeBytes, uint32_t alignment);

/**
 * @brief Reallocate a block of memory according to requested memory size.
 * @param sizeBytes The amount of memory requested.
 * @param flags One of the reallocation flags @ref MM_REALLOC_COPY or @ref MM_REALLOC_ZERO.
 * @return The reallocated object or NULL if failed. Call @ref mm_get_last_error() to find out the
 * failure reason.
 */
extern void* MemSessionRealloc(void* object, uint64_t newSizeBytes, MemReallocFlags flags);

/**
 * @brief Free a memory block.
 * @param sizeBytes The amount of memory requested.
 */
extern void MemSessionFree(void* object);

/**
 * @brief Free all un-released memory at the end of a transaction to avoid memory leak.
 * @param None.
 */
extern void MemSessionCleanup();

/**
 * @brief Prints all buffer API status into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionApiPrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps all buffer API status into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionApiToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Retrieves session allocator statistics.
 * @param ConnectionId The connection identifier of the session.
 * @param stats The session allocator statistics.
 * @return Non-zero value if the session statistics were retrieved successfully.
 */
extern int MemSessionGetStats(ConnectionId connectionId, MemSessionAllocatorStats* stats);

/**
 * @brief Prints current session allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 */
extern void MemSessionPrintStats(ConnectionId connectionId, const char* name, LogLevel logLevel);

/**
 * @brief Retrieves current session allocator statistics.
 * @param stats The session allocator statistics.
 * @return Non-zero value if the session statistics were retrieved successfully.
 */
extern int MemSessionGetCurrentStats(MemSessionAllocatorStats* stats);

/**
 * @brief Prints current session allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 */
extern void MemSessionPrintCurrentStats(const char* name, LogLevel logLevel);

/**
 * @brief Retrieves memory consumption statistics for all running sessions..
 * @param sessionStatsArray The session statistics array.
 * @param sessionCount The number of entries in the given array (should be the maximum thread count).
 * @return The actual number of session for which statistics were reported.
 */
extern uint32_t MemSessionGetAllStats(MemSessionAllocatorStats* sessionStatsArray, uint32_t sessionCount);

/**
 * @brief Prints all session allocator statistics to log.
 * @param name The name of the report to print.
 * @param logLevel The log level to use in printing.
 */
extern void MemSessionPrintAllStats(const char* name, LogLevel logLevel);

/**
 * @brief Prints a summary report of all session allocator statistics to log.
 * @param name The name of the report to print.
 * @param logLevel The log level to use in printing.
 * @param[opt] fullReport Specifies whether to print an elaborate report.
 */
extern void MemSessionPrintSummary(const char* name, LogLevel logLevel, bool fullReport = false);

// forward declaration
template <typename T>
class MemSessionPtr;

/**
 * @brief Allocate a typed object based on session-local memory.
 * @tparam T The allocated object type.
 * @tparam Args Arguments type list according to a target object constructor.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline T* MemSessionAllocObject(Args&&... args)
{
    T* object = nullptr;
    void* buffer = MemSessionAlloc(sizeof(T));
    if (buffer != nullptr) {
        object = new (buffer) T(std::forward<Args>(args)...);
    }
    return object;
}

/**
 * @brief Allocate an aligned typed object based on session-local memory.
 * @tparam T The allocated object type.
 * @tparam Args Arguments type list according to a target object constructor.
 * @param alignment The requested alignment in bytes.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline T* MemSessionAllocAlignedObject(uint32_t alignment, Args&&... args)
{
    T* object = nullptr;
    void* buffer = MemSessionAllocAligned(sizeof(T), alignment);
    if (buffer != nullptr) {
        object = new (buffer) T(std::forward<Args>(args)...);
    }
    return object;
}

/**
 * @brief Deallocates a typed object previously allocated with a call to @ref MemSessionAllocObject() or
 * @ref MemSessionAllocObjectAligned().
 * @tparam T The allocated object type.
 * @param object The object to deallocate.
 */
template <typename T>
inline void MemSessionFreeObject(T* object)
{
    if (object != nullptr) {
        object->~T();
        MemSessionFree(object);
    }
}

/**
 * @class MemSessionPtr
 * @brief A template class for unique (i.e. cannot be copied or assigned) pointer allocated in the
 * session local pool.
 */
template <typename T>
class MemSessionPtr {
public:
    /**
     * @brief Default constructor for a NULL pointer.
     */
    MemSessionPtr() : m_ptr(nullptr)
    {}

    /**
     * @brief Constructs an objects on pre-allocated memory buffer.
     * @param manager The memory manager to which the allocated object belongs.
     * @param target Address of memory buffer for placement new.
     * @param args Arguments for object constructor.
     */
    explicit MemSessionPtr(T* target) : m_ptr(target)
    {}

    /**
     * @brief Copy-move constructor. The underlying managed object is passed from
     * the parameter to this object. The parameter object loses its reference to
     * the underlying object.
     * @param other The moved object.
     */
    MemSessionPtr(MemSessionPtr<T>&& other) : m_ptr(other.m_ptr)
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
    MemSessionPtr<T>& operator=(MemSessionPtr<T>&& right)
    {
        if (this != &right) {
            Reset(right.m_ptr);
            right.m_ptr = nullptr;
        }
        return *this;
    }

    /**
     * @brief Template cast operator.
     * @return An object having a copy of the managed object converted to the target type.
     */
    template <class Other>
    inline operator MemSessionPtr<Other>()
    {
        T* ptr = this->m_ptr;
        this->m_ptr = nullptr;
        return MemSessionPtr<Other>(static_cast<Other*>(ptr));
    }

    /**
     * @brief Destructor. Deallocates the managed object.
     */
    ~MemSessionPtr()
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

    // No copies allowed. This is a unique_ptr
    /** @cond EXCLUDE_DOC */
    MemSessionPtr(const MemSessionPtr<T>& other) = delete;
    MemSessionPtr<T>& operator=(const MemSessionPtr<T>& right) = delete;
    /** @endcond */

private:
    // Allow private access to other specializations.
    template <class Other>
    friend class MemSessionPtr;

    /**
     * @brief Deallocates the managed object.
     */
    inline void Clear()
    {
        if (m_ptr != nullptr) {
            MemSessionFreeObject<T>(m_ptr);
        }
        m_ptr = nullptr;
    }

    /**
     * @brief Deallocates the managed object and resets it to a new managed object.
     * @param new_ptr The new managed object.
     * @param new_manager The new memory manager to which the new managed object
     * belongs.
     */
    inline void Reset(T* new_ptr)
    {
        Clear();
        m_ptr = new_ptr;
    }

    /**
     * @var The managed object
     */
    T* m_ptr;
};

/**
 * @brief Allocate a typed object based on session-local memory.
 * @tparam T The allocated object type.
 * @tparam Args Arguments type list according to a target object constructor.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline MemSessionPtr<T> MemSessionAllocObjectPtr(Args&&... args)
{
    return std::move(MemSessionPtr<T>(MemSessionAllocObject<T>(args...)));
}

/**
 * @brief Allocate an aligned typed object based on session-local memory.
 * @tparam T The allocated object type.
 * @tparam Args Arguments type list according to a target object constructor.
 * @param alignment The requested alignment in bytes.
 * @param args The arguments passed to the object constructor.
 * @return The constructed object or NULL if memory allocation failed.
 */
template <typename T, class... Args>
inline MemSessionPtr<T> MemSessionAllocAlignedObjectPtr(uint32_t alignment, Args&&... args)
{
    return std::move(MemSessionPtr<T>(MemSessionAllocAlignedObject<T>(alignment, args...)));
}
}  // namespace MOT

/**
 * @brief Dumps all session API status to standard error stream.
 */
extern "C" void MemSessionApiDump();

/**
 * @brief Analyzes the memory status of a given buffer address.
 * @param address The buffer to analyze.
 * @return Non-zero value if buffer was found.
 */
extern "C" int MemSessionApiAnalyze(void* buffer);

#endif
