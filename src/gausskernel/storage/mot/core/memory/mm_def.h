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
 * mm_def.h
 *    Common memory management definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_def.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_DEF_H
#define MM_DEF_H

#include <stdint.h>
#include <stddef.h>
#include <string>

#include "type_formatter.h"

/** @define General alignment macro. */
#define MEM_ALIGN(size, align) (((size) + (align)-1) / (align) * (align))

/** @define L1 Cache line size in  bytes. */
#define L1_CACHE_LINE 64

/** @define L1 alignment macro. */
#define L1_ALIGN(size) MEM_ALIGN(size, L1_CACHE_LINE)

/** @define Utility macro for defining packed structures. */
#define PACKED __attribute__((packed))

/** @define Utility macro for defining L1 cache line aligned structures. */
#define L1_ALIGNED __attribute__((aligned(L1_CACHE_LINE)))

/** @define Utility macro for defining 8-byte aligned structures. */
#define ALIGNED8 __attribute__((aligned(8)))

/**
 * @define Utility macro for defining L1 cache line aligned structure size (rounds up to next cache
 * line alignment).
 */
#define L1_ALIGNED_SIZEOF(T) ((sizeof(T) + L1_CACHE_LINE - 1) / L1_CACHE_LINE * L1_CACHE_LINE)

/**
 * @define Utility macro for defining L1 cache line aligned structure start address (rounds up to
 * next cache line alignment).
 */
#define L1_ALIGNED_PTR(P) ((((uintptr_t)(P)) + L1_CACHE_LINE - 1) / L1_CACHE_LINE * L1_CACHE_LINE)

/** @define Half L1 Cache line size in  bytes. */
#define HALF_L1_CACHE_LINE (L1_CACHE_LINE / 2)

/**
 * @define Utility macro for defining L1 cache line aligned structure size (rounds up to next half
 * cache line alignment).
 */
#define HALF_L1_ALIGNED_SIZEOF(T) ((sizeof(T) + HALF_L1_CACHE_LINE - 1) / HALF_L1_CACHE_LINE * HALF_L1_CACHE_LINE)

/** @define Constant for one kilobyte. */
#define KILO_BYTE 1024

/** @define Converts kilo-bytes to bytes. */
#define K2B(kilo_bytes) ((kilo_bytes) << 10)

/** @define The number of bits required for shift operations to divide/multiply by a kilo-byte. */
#define KILO_BYTE_LSH_BITS 10

/** @define Constant for one megabyte. */
#define MEGA_BYTE (KILO_BYTE * KILO_BYTE)

/** @define The number of bits required for shift operations to divide/multiply by a mage-byte. */
#define MEGA_BYTE_LSH_BITS 20

/** @define Constant for one gigabyte. */
#define GIGA_BYTE (MEGA_BYTE * KILO_BYTE)

/** @define The number of bits required for shift operations to divide/multiply by a giga-byte. */
#define GIGA_BYTE_LSH_BITS 30

/** @define System page size in bytes. */
#define PAGE_SIZE_KB 4

/** @define System page size in bytes. */
#define PAGE_SIZE_BYTES (PAGE_SIZE_KB * KILO_BYTE)

/** @define Utility macro for calculating page aligned sizes (rounds up to next page size). */
#define PAGE_ALIGNED_SIZE(size) (((size) + PAGE_SIZE_BYTES - 1) / PAGE_SIZE_BYTES * PAGE_SIZE_BYTES)

// System Limits

/** @define Upper bound on number of NUMA nodes. */
#define MEM_MAX_NUMA_NODES 32

/** @define Chunk size in mega-bytes. */
#define MEM_CHUNK_SIZE_MB 2

/** @define Node ID constant denoting node-interleaved page allocation. */
#define MEM_INTERLEAVE_NODE (-1)

/** @define Constant denoting invalid NUMA node id. */
#define MEM_INVALID_NODE (-2)

/** @define Constant denoting default NUMA node id. */
#define MEM_DEFAULT_NODE 0

// Memory Debug
/** @define Clean land memory pattern constant. */
#define MEM_CLEAN_LAND 0xCD

/** @define Dead land memory pattern constant. */
#define MEM_DEAD_LAND 0xFD

namespace MOT {

/** @enum Report modes when reporting memory status. */
enum MemReportMode : uint32_t {
    /** @var Specifies a summary report. */
    MEM_REPORT_SUMMARY,

    /** @var Specifies a detailed report. */
    MEM_REPORT_DETAILED
};

/** @enum Constants for re-allocation flags. */
enum MemReallocFlags : uint16_t {
    /**
     * @var Specifies to leave new memory contents uninitialized when memory reallocation requires
     * allocating a new object.
     */
    MEM_REALLOC_NO_OP = 0,

    /**
     * @var Specifies to copy old memory contents into new memory contents when memory reallocation
     * requires allocating a new object.
     */
    MEM_REALLOC_COPY,

    /**
     * @var Specifies to reset new memory contents to zero when memory reallocation requires
     * allocating a new object.
     */
    MEM_REALLOC_ZERO,

    /**
     * @var Specifies to copy old memory contents into new memory contents, and zero the rest of the bytes, when memory
     * reallocation requires allocating a new object.
     */
    MEM_REALLOC_COPY_ZERO
};

/** @typedef MemReserveMode Constants for defining memory reservation modes. */
enum MemReserveMode : uint32_t {
    /** @var Constant designating invalid memory reservation mode. */
    MEM_RESERVE_INVALID,

    /** @var Constant designating reservation of virtual memory only. */
    MEM_RESERVE_VIRTUAL,

    /** @var Constant designating reservation of physical memory. */
    MEM_RESERVE_PHYSICAL
};

/**
 * @brief Converts string value to memory reserve mode enumeration.
 * @param reserveModeStr The reserve mode string.
 * @return The reserve mode enumeration.
 */
extern MemReserveMode MemReserveModeFromString(const char* reserveModeStr);

/**
 * @brief Converts memory reservation mode enumeration into string form.
 * @param reserveMode The memory reservation mode.
 * @return The memory reservation mode string.
 */
extern const char* MemReserveModeToString(MemReserveMode reserveMode);

/**
 * @class TypeFormatter<MemReserveMode>
 * @brief Specialization of TypeFormatter<T> with [ T = MemReserveMode ].
 */
template <>
class TypeFormatter<MemReserveMode> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const MemReserveMode& value, mot_string& stringValue)
    {
        stringValue = MemReserveModeToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, MemReserveMode& value)
    {
        value = MemReserveModeFromString(stringValue);
        return value != MemReserveMode::MEM_RESERVE_INVALID;
    }
};

/** @typedef MemStorePolicy Constants for defining memory storage policies. */
enum MemStorePolicy : uint32_t {
    /** @var Constant designating invalid memory storage policy. */
    MEM_STORE_INVALID,

    /** @var Constant designating unused memory should be returned to kernel. */
    MEM_STORE_COMPACT,

    /** @var Constant designating unused memory should not be returned to kernel. */
    MEM_STORE_EXPANDING
};

/**
 * @brief Converts string value to memory storage policy enumeration.
 * @param storePolicyStr The storage policy string.
 * @return The storage policy enumeration.
 */
extern MemStorePolicy MemStorePolicyFromString(const char* storePolicyStr);

/**
 * @brief Converts memory storage policy enumeration into string form.
 * @param storePolicy The memory storage policy.
 * @return The memory storage policy string.
 */
extern const char* MemStorePolicyToString(MemStorePolicy storePolicy);

/**
 * @class TypeFormatter<MemStorePolicy>
 * @brief Specialization of TypeFormatter<T> with [ T = MemStorePolicy ].
 */
template <>
class TypeFormatter<MemStorePolicy> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const MemStorePolicy& value, mot_string& stringValue)
    {
        stringValue = MemStorePolicyToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, MemStorePolicy& value)
    {
        value = MemStorePolicyFromString(stringValue);
        return value != MemStorePolicy::MEM_STORE_INVALID;
    }
};

/** @typedef Allocation type constants (interchangeable with @ref mm_usage_context_t above). */
enum MemAllocType : uint16_t {
    /** @var Invalid allocation type. */
    MEM_ALLOC_INVALID,

    /** @var Global allocation type. */
    MEM_ALLOC_GLOBAL,

    /** @var Local allocation type. */
    MEM_ALLOC_LOCAL
};

/**
 * @brief Converts allocation type enumeration into string form.
 * @param allocType The allocation type.
 * @return The allocation type string.
 */
extern const char* MemAllocTypeToString(MemAllocType allocType);

/** @typedef Allocation policy. */
enum MemAllocPolicy : uint32_t {
    /** @var Designates invalid allocation policy. */
    MEM_ALLOC_POLICY_INVALID,

    /** @var Use native NUMA page-interleaved allocation. */
    MEM_ALLOC_POLICY_PAGE_INTERLEAVED,

    /**
     * @var Use internally manufactured chunk-interleaved allocation (each new chunk is allocated on
     * the next round-robin NUMA node.
     */
    MEM_ALLOC_POLICY_CHUNK_INTERLEAVED,

    /** @var Disable interleaving. Each new chunk is allocated on the current NUMA node using NUMA API. */
    MEM_ALLOC_POLICY_LOCAL,

    /** @var Disable interleaving. Each new chunk is allocated on the current NUMA node using native API. */
    MEM_ALLOC_POLICY_NATIVE,

    /**
     * @var Let's the Main Memory engine decide by itself about the best fitting chunk allocation
     * policy. This value is used only during configuration loading and is replaced by the actual
     * value decided.
     */
    MEM_ALLOC_POLICY_AUTO
};

/**
 * @brief Converts string value to allocation policy enumeration.
 * @param allocPolicyStr The memory allocation policy string.
 * @return The allocation policy enumeration.
 */
extern MemAllocPolicy MemAllocPolicyFromString(const char* allocPolicyStr);

/**
 * @brief Converts memory allocation policy enumeration into string form.
 * @param allocPolicy The memory allocation policy.
 * @return The memory allocation policy string.
 */
extern const char* MemAllocPolicyToString(MemAllocPolicy allocPolicy);

/**
 * @class TypeFormatter<MemAllocPolicy>
 * @brief Specialization of TypeFormatter<T> with [ T = MemAllocPolicy ].
 */
template <>
class TypeFormatter<MemAllocPolicy> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const MemAllocPolicy& value, mot_string& stringValue)
    {
        stringValue = MemAllocPolicyToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, MemAllocPolicy& value)
    {
        value = MemAllocPolicyFromString(stringValue);
        return value != MemAllocPolicy::MEM_ALLOC_POLICY_INVALID;
    }
};

}  // namespace MOT

/** @define Enables/disable entire memory module. */
#define MEM_ACTIVE

/** @define Enables/disable usage of memory module in session-local memory. */
#define MEM_SESSION_ACTIVE

// MEM_SESSION_ACTIVE cannot be defined without MEM_ACTIVE being also defined
#if !defined(MEM_ACTIVE) && defined(MEM_SESSION_ACTIVE)
#undef MEM_SESSION_ACTIVE
#endif

#endif /* MM_DEF_H */
