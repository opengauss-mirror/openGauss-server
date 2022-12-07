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
 * infra.h
 *    Basic infrastructure helper functions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/infra.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INFRA_H
#define INFRA_H

#include <cstdlib>
#include <cstdint>
#include <utility>

#include "debug_utils.h"
#include "global.h"

#ifdef MOT_DEBUG
#define MOT_INFRA_ASSERT(invariant)                           \
    do {                                                      \
        if (!(invariant)) {                                   \
            mot_infra_assert(#invariant, __FILE__, __LINE__); \
        }                                                     \
    } while (0);
#else
#define MOT_INFRA_ASSERT(invariant)
#endif

namespace MOT {
/**
 * @brief Helper for reporting infra sub-module errors (required for breaking cyclic dependency).
 * @param error_code The error code to report.
 * @param The error context (string description).
 * @param format The error format string.
 * @param .. Any additional parameters required by the format string.
 */
extern void mot_infra_report_error(int errorCode, const char* errorContext, const char* format, ...);

/**
 * @brief Helper for using MOT_ASSERT in infra code (required for breaking cyclic dependency).
 * @param invariant The broken invariant string.
 * @param file The failing file.
 * @param line The failing line.
 */
extern void mot_infra_assert(const char* invariant, const char* file, int line);

/**
 * @class mot_default_allocator
 * @brief The default allocator used in infra sub-module collections. It works over regular malloc(), free() and
 * realloc().
 */
class mot_default_allocator {
public:
    /**
     * @brief Allocates memory.
     * @param sizeBytes The number of bytes to allocate.
     * @return The allocated memory, or NULL if failed.
     */
    static void* allocate(uint32_t sizeBytes)
    {
        return ::malloc(sizeBytes);
    }

    /**
     * @brief Deallocates memory.
     * @param buf The memory to deallocate.
     */
    static void free(void* buf)
    {
        ::free(buf);
    }

    /**
     * @brief Reallocates existing memory.
     * @param buf The existing memory
     * @param currentSizeBytes The current buffer size.
     * @param newSizeBytes The new allocated size.
     * @return The reallocated memory, or NULL if failed (in which case the existing memory is unaffected).
     */
    static void* realloc(void* buf, uint32_t currentSizeBytes, uint32_t newSizeBytes)
    {
        void* newBuf = ::malloc(newSizeBytes);
        if ((newBuf != nullptr) && (buf != nullptr)) {  // could possible reallocate from NULL
            // attention: new size might be smaller (highly unexpected)
            uint32_t copySize = std::min(currentSizeBytes, newSizeBytes);
            errno_t erc = memcpy_s(newBuf, newSizeBytes, buf, copySize);
            securec_check(erc, "\0", "\0");
            ::free(buf);
        }
        return newBuf;
    }
};

/**
 * @class mot_default_assigner
 * @brief The default assigner used in infra sub-module collections.
 * @tparam T The assigned type.
 */
template <typename T>
class mot_default_assigner {
public:
    /**
     * @brief Assigns one value to another. Relies on assignment operator (can be overloaded in target type).
     * @param lhs Receives the assignment.
     * @param rhs The value to assign into lhs.
     * @return Always true.
     */
    static bool assign(T& lhs, const T& rhs)
    {
        lhs = rhs;
        return true;
    }

    /**
     * @brief Assigns one value to another. Relies on move operator (can be overloaded in target type).
     * @param lhs The target.
     * @param rhs The value to move into lhs.
     */
    static void move(T& lhs, T&& rhs)
    {
        lhs = rhs;  // std::move(rhs);
    }
};

/**
 * @class mot_assigner
 * @brief A template assigner for objects that implement the assign() and move() methods.
 * @tparam T The assigned type. Must implement the assign() and move() meothods as expected below.
 */
template <typename T>
class mot_assigner {
public:
    /**
     * @brief Assigns one value to another. Relies on assign method (must exist in target type).
     * @param lhs Receieves the assignment.
     * @param rhs The value to assign into lhs.
     * @return Always true.
     */
    static bool assign(T& lhs, const T& rhs)
    {
        return lhs.assign(rhs);
    }

    /**
     * @brief Assigns one value to another. Relies on move method (must exist in target type).
     * @param lhs The target.
     * @param rhs The value to move into lhs.
     */
    static void move(T& lhs, T&& rhs)
    {
        lhs.move(rhs);
    }
};

/**
 * @class mot_helper
 * @brief A specialize-able template used for easily specializing containers. Each type participating in mot containers,
 * that does not use the default assigner, should specialize this class and define the assigner type below to the
 * correct type.
 * @tparam T The type for which an assigner type is to be define.
 */
template <typename T>
class mot_helper {
public:
    /** @typedef The assigner type for the template type T. */
    typedef mot_default_assigner<T> assigner;
};

/**
 * @class mot_pair
 * @brief A pair of values.
 * @tparam T1 The type of the first pair member.
 * @tparam T2 The type of the second pair member.
 */
template <typename T1, typename T2>
struct mot_pair {
    /** @brief Default constructor. */
    mot_pair() : first(), second()
    {}

    /**
     * @brief Constructs a pair object.
     * @first_ The first item in the pair.
     * @second_ The second item in the pair.
     */
    mot_pair(const T1& first_, const T2& second_) : first(first_), second(second_)
    {}

    /** @var The first value. */
    T1 first;

    /** @var The second value. */
    T2 second;
};

/**
 * @brief Utility function for making a pair.
 * @tparam T1 The type of the first pair member.
 * @tparam T2 The type of the second pair member.
 * @param t1 The first item in the pair.
 * @param t2 The second item in the pair.
 * @return The resulting pair.
 */
template <typename T1, typename T2>
inline mot_pair<T1, T2> mot_make_pair(const T1& t1, const T2& t2)
{
    return mot_pair<T1, T2>(t1, t2);
}

/**
 * @class mot_pair_assigner
 * @brief Assigner for key-value pair objects.
 * @tparam K The key type.
 * @tparam V The value type.
 * @tparam KeyAssigner The key assigner type.
 * @tparam ValueAssigner The value assigner type.
 */
template <typename K, typename V, typename KeyAssigner = typename mot_helper<K>::assigner,
    typename ValueAssigner = typename mot_helper<V>::assigner>
class mot_pair_assigner {
public:
    /**
     * @brief Assigns one pair to another.
     * @param lhs Receives the value of the other pair.
     * @param rhs The pair to assign into lhs.
     * @return True if assignment succeeded.
     */
    static bool assign(mot_pair<K, V>& lhs, const mot_pair<K, V>& rhs)
    {
        return KeyAssigner::assign(lhs.first, rhs.first) && ValueAssigner::assign(lhs.second, rhs.second);
    }

    /**
     * @brief Moves one pair to another.
     * @param lhs Receives the value of the other pair.
     * @param rhs The pair to move into lhs.
     * @return True if move operation succeeded.
     */
    static void move(mot_pair<K, V>& lhs, mot_pair<K, V>&& rhs)
    {
        KeyAssigner::move(lhs.first, std::move(rhs.first));
        ValueAssigner::move(lhs.second, std::move(rhs.second));
    }
};

/**
 * @class mot_helper Provides helper for container instantiation with mot_pair objects.
 * @brief Helper for mot_pair template. This assumes the mot_helper for both types T1 and T2 is properly defined.
 * @tparam T1 The type of the first pair member.
 * @tparam T2 The type of the second pair member.
 */
template <typename T1, typename T2>
class mot_helper<mot_pair<T1, T2> > {
public:
    typedef mot_pair_assigner<T1, T2> assigner;
};

/** @enum Insertion status codes. */
enum InsertStatus {
    /** @var Denotes insertion failed (probably due to resource limit or lack of memory). */
    INSERT_FAILED,

    /** @var Denotes insertion failed since the item already exists. */
    INSERT_EXISTS,

    /** @var Denotes insertion succeeded.   */
    INSERT_SUCCESS
};
}  // namespace MOT

#endif /* INFRA_H */
