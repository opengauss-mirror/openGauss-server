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
 * mm_buffer_class.h
 *    Various buffer classes.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_class.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_CLASS_H
#define MM_BUFFER_CLASS_H

#include "mm_def.h"
#include <stdint.h>

/** @define Invalid buffer size. */
#define MEM_BUFFER_SIZE_INVALID ((uint32_t)0)

namespace MOT {
/**
 * @enum Constants denoting various buffer classes. Our original design followed whole powers of
 * two to facilitate bit-hacking in our computations, but this led to inefficient *virtual* memory
 * usage. Therefore, we changed our design to use optimal buffer sizes, such that chunk utilization
 * is optimal (especially with large buffers).
 */
enum MemBufferClass : uint8_t {
    /** @var Constant denoting the smallest buffer size class. */
    MEM_BUFFER_CLASS_SMALLEST,

    /** @var Constant denoting buffer size of 1 kilobyte. */
    MEM_BUFFER_CLASS_KB_1 = MEM_BUFFER_CLASS_SMALLEST,

    /** @var Constant denoting buffer size of 2 kilobytes. */
    MEM_BUFFER_CLASS_KB_2,

    /** @var Constant denoting buffer size of 4 kilobytes. */
    MEM_BUFFER_CLASS_KB_4,

    /** @var Constant denoting buffer size of 8 kilobytes. */
    MEM_BUFFER_CLASS_KB_8,

    /** @var Constant denoting buffer size of 16 kilobytes. */
    MEM_BUFFER_CLASS_KB_16,

    /** @var Constant denoting buffer size of 32 kilobytes. */
    MEM_BUFFER_CLASS_KB_32,

    /** @var Constant denoting buffer size of 64 kilobytes. */
    MEM_BUFFER_CLASS_KB_64,

    /** @var Constant denoting buffer size of 127 kilobytes. */
    MEM_BUFFER_CLASS_KB_127,

    /** @var Constant denoting buffer size of 255 kilobytes. */
    MEM_BUFFER_CLASS_KB_255,

    /** @var Constant denoting buffer size of 511 kilobytes. */
    MEM_BUFFER_CLASS_KB_511,

    /** @var Constant denoting buffer size of 1022 kilobytes. */
    MEM_BUFFER_CLASS_KB_1022,

    /** @var Constant denoting the largest buffer size class. */
    MEM_BUFFER_CLASS_LARGEST = MEM_BUFFER_CLASS_KB_1022,

    /** @var Constant denoting the number of buffer size classes. */
    MEM_BUFFER_CLASS_COUNT,

    /** @var Constant denoting an invalid buffer size class. */
    MEM_BUFFER_CLASS_INVALID
};

/** @define Minimum buffer size in kilobytes. */
#define MEM_MIN_BUFFER_SIZE_KB 1

/** @define Constant denoting the first large buffer size class. */
#define MEM_BUFFER_CLASS_LARGE_FIRST MEM_BUFFER_CLASS_KB_16

/** @define Constant denoting the number of small buffer size classes. */
#define MEM_BUFFER_CLASS_SMALL_COUNT MEM_BUFFER_CLASS_KB_16

/** @define The number of large buffer size classes. */
#define MEM_BUFFER_CLASS_LARGE_COUNT (MEM_BUFFER_CLASS_COUNT - MEM_BUFFER_CLASS_SMALL_COUNT)

/** @var A global translation table from buffer class to buffer size. */
extern const uint32_t g_memBufferClassSizeKb[];

/**
 * @brief Converts a buffer class enumeration value to size in kilobytes.
 * @param bufferClass The buffer class to convert.
 * @return The size in kilobytes or @ref MEM_BUFFER_SIZE_INVALID if failed (i.e. input parameter is
 * out of bounds).
 */
inline uint32_t MemBufferClassToSizeKb(MemBufferClass bufferClass)
{
    // we cannot use bit-wise operations since not all buffer sizes are a whole power of two
    uint32_t result = MEM_BUFFER_SIZE_INVALID;
    if (bufferClass <= MEM_BUFFER_CLASS_LARGEST) {
        result = g_memBufferClassSizeKb[(int)bufferClass];
    }
    return result;
}

/**
 * @brief Converts a size in kilobytes to its equivalent buffer class enumeration value.
 * @param size_kb The size in kilobytes to convert.
 * @return The buffer class or @ref MEM_BUFFER_CLASS_INVALID if failed.
 * @note The argument is expected to be a power of 2, otherwise the behavior is undefined.
 */
inline MemBufferClass MemBufferClassFromSizeKb(uint32_t size_kb)
{
    // we cannot use directly bit-wise operations since not all buffer sizes are a whole power of two,
    // BUT we can use it selectively as follows:
    // Computation explanation: (assuming size_kb input is a whole power of two)
    // -----------------------
    // Buffer size 1 (in units of KB) has 0 trailing zeros. Buffer size 2 has 1 trailing zeros, and so forth.
    // Up until buffer size 64 we are good, since the number of trailing zeros is exactly the corresponding
    // enumeration value we want to compute.
    // For buffer sizes 127, 255, 512 we need to add 1, and then we get the right number of trailing zeros.
    // Buffer size 1022 is a special case.

    MemBufferClass result = MEM_BUFFER_CLASS_INVALID;

    if (size_kb <= 64) {
        // up until this buffer size we have while powers of two
        result = ((MemBufferClass)__builtin_ctz(size_kb));
    } else if (size_kb <= 511) {
        // in this range we need to add 1 to the input to get a whole power of two
        // this will give rise to the correct enumeration value
        result = ((MemBufferClass)__builtin_ctz(size_kb + 1));
    } else if (size_kb == 1022) {
        // special case
        result = MEM_BUFFER_CLASS_KB_1022;
    }

    // stay on the safe side
    if (result > MEM_BUFFER_CLASS_LARGEST) {
        result = MEM_BUFFER_CLASS_INVALID;
    }
    return result;
}

/**
 * @brief Converts a buffer class to string form.
 * @param bufferClass The buffer class to convert.
 * @return The resulting string form.
 */
extern const char* MemBufferClassToString(MemBufferClass bufferClass);

/**
 * @brief Choose the most fitting buffer class for a buffer size.
 * @detail The returned buffer class is the largest class that still fits in the given buffer size.
 * If the buffer size exceeds the minimum supported, then MEM_BUFFER_CLASS_INVALID is returned. If
 * the buffer size exceeds the maximum supported, then MEM_BUFFER_CLASS_LARGEST is returned. The
 * mathematical notation for this function is as follows:
 * ~~~
 * MAX(buffer-class | size-of(buffer-class) is NOT greater than buffer-size).
 * ~~~
 * @param bufferSize The buffer size in bytes (not required to be a whole power of two).
 * @return The most fitting buffer class, or MEM_BUFFER_CLASS_INVALID if the given buffer size is
 * too small.
 */
inline MemBufferClass MemBufferClassLowerBound(uint32_t bufferSize)
{
    // Computation explanation:
    // -----------------------
    // We need to look at the leftmost raised bit in the given buffer size. This is the required buffer
    // class, unless we are out of bounds. So, buffer-sizes in the range [1 KB, 2 KB) has bit 11 raised (21 leading
    // zeros), buffer size in the range [2 KB, 4 KB) has bit 12 raised (20 leading zeros), etc. This needs to be
    // translated to the values 0, 1, and so forth. Therefore:
    //    leading_zeros = __builtin_clz(bufferSize)
    //    leftmost_raised_bit = (32 - leading_zeros)
    //    buffer_class_lower_bound = leftmost_raised_bit - 11
    // The resulting formula is:
    //    (21 - __builtin_clz(bufferSize))
    // The above computation is good for buffer sizes up to 64 KB (including)
    MemBufferClass result = MEM_BUFFER_CLASS_INVALID;

    // check if out of bounds (underflow)
    if (bufferSize >= 1024) {
        if (bufferSize < 127 * KILO_BYTE) {
            result = (MemBufferClass)(21 - __builtin_clz(bufferSize));
        } else if (bufferSize < 255 * KILO_BYTE) {
            result = MEM_BUFFER_CLASS_KB_127;
        } else if (bufferSize < 511 * KILO_BYTE) {
            result = MEM_BUFFER_CLASS_KB_255;
        } else if (bufferSize < 1022 * KILO_BYTE) {
            result = MEM_BUFFER_CLASS_KB_511;
        } else {
            result = MEM_BUFFER_CLASS_KB_1022;
        }

        if (result > MEM_BUFFER_CLASS_LARGEST) {
            result = MEM_BUFFER_CLASS_LARGEST;
        }
    }
    return result;
}

/**
 * @brief Overload global prefix increment of buffer class enumeration (for loops).
 */
inline MemBufferClass& operator++(MemBufferClass& bufferClass)
{
    return bufferClass = (MemBufferClass)(((int)bufferClass) + 1);
}
}  // namespace MOT

#endif /* MM_BUFFER_CLASS_H */
