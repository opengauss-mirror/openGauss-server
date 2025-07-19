/*---------------------------------------------------------------------------
*
* Common routines for Ryu floating-point output.
*
* Portions Copyright (c) 2025, openGauss Contributors
* Portions Copyright (c) 2018-2024, PostgreSQL Global Development Group
*
* IDENTIFICATION
*    src/include/access/datavec/ryu_common.h
*
* This is a modification of code taken from github.com/ulfjack/ryu under the
* terms of the Boost license (not the Apache license). The original copyright
* notice follows:
*
* Copyright 2018 Ulf Adams
*
* The contents of this file may be used under the terms of the Apache
* License, Version 2.0.
*
*     (See accompanying file LICENSE-Apache or copy at
*      http://www.apache.org/licenses/LICENSE-2.0)
*
* Alternatively, the contents of this file may be used under the terms of the
* Boost Software License, Version 1.0.
*
*     (See accompanying file LICENSE-Boost or copy at
*      https://www.boost.org/LICENSE_1_0.txt)
*
* Unless required by applicable law or agreed to in writing, this software is
* distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.
*
*---------------------------------------------------------------------------
*/

#ifndef RYU_COMMON_H
#define RYU_COMMON_H

/*
 * Upstream Ryu's output is always the shortest possible. But we adjust that
 * slightly to improve portability: we avoid outputting the exact midpoint
 * value between two representable floats, since that relies on the reader
 * getting the round-to-even rule correct, which seems to be the common
 * failure mode.
 *
 * Defining this to 1 would restore the upstream behavior.
 */
#define STRICTLY_SHORTEST 0

#if SIZEOF_SIZE_T < 8
#define RYU_32_BIT_PLATFORM
#endif

/*
 * A table of all two-digit numbers. This is used to speed up decimal digit
 * generation by copying pairs of digits into the final output.
 */
static const char DIGIT_TABLE[200] = {
    '0', '0', '0', '1', '0', '2', '0', '3', '0', '4', '0', '5', '0', '6', '0', '7', '0', '8', '0', '9', '1', '0', '1',
    '1', '1', '2', '1', '3', '1', '4', '1', '5', '1', '6', '1', '7', '1', '8', '1', '9', '2', '0', '2', '1', '2', '2',
    '2', '3', '2', '4', '2', '5', '2', '6', '2', '7', '2', '8', '2', '9', '3', '0', '3', '1', '3', '2', '3', '3', '3',
    '4', '3', '5', '3', '6', '3', '7', '3', '8', '3', '9', '4', '0', '4', '1', '4', '2', '4', '3', '4', '4', '4', '5',
    '4', '6', '4', '7', '4', '8', '4', '9', '5', '0', '5', '1', '5', '2', '5', '3', '5', '4', '5', '5', '5', '6', '5',
    '7', '5', '8', '5', '9', '6', '0', '6', '1', '6', '2', '6', '3', '6', '4', '6', '5', '6', '6', '6', '7', '6', '8',
    '6', '9', '7', '0', '7', '1', '7', '2', '7', '3', '7', '4', '7', '5', '7', '6', '7', '7', '7', '8', '7', '9', '8',
    '0', '8', '1', '8', '2', '8', '3', '8', '4', '8', '5', '8', '6', '8', '7', '8', '8', '8', '9', '9', '0', '9', '1',
    '9', '2', '9', '3', '9', '4', '9', '5', '9', '6', '9', '7', '9', '8', '9', '9'};

/*  Returns e == 0 ? 1 : ceil(log_2(5^e)). */
static inline uint32 pow5bits(const int32 e)
{
    /*
     * This approximation works up to the point that the multiplication
     * overflows at e = 3529.
     *
     * If the multiplication were done in 64 bits, it would fail at 5^4004
     * which is just greater than 2^9297.
     */
    Assert(e >= 0);
    Assert(e <= 3528);
    return ((((uint32)e) * 1217359) >> 19) + 1;
}

/*  Returns floor(log_10(2^e)). */
static inline int32 log10Pow2(const int32 e)
{
    /*
     * The first value this approximation fails for is 2^1651 which is just
     * greater than 10^297.
     */
    Assert(e >= 0);
    Assert(e <= 1650);
    return (int32)((((uint32)e) * 78913) >> 18);
}

/*  Returns floor(log_10(5^e)). */
static inline int32 log10Pow5(const int32 e)
{
    /*
     * The first value this approximation fails for is 5^2621 which is just
     * greater than 10^1832.
     */
    Assert(e >= 0);
    Assert(e <= 2620);
    return (int32)((((uint32)e) * 732923) >> 20);
}

static inline int copy_special_str(char *const result, const bool sign, const bool exponent, const bool mantissa)
{
    errno_t rc = EOK;
    if (mantissa) {
        rc = memcpy_s(result, 3, "NaN", 3);
        securec_check(rc, "\0", "\0");
        return 3;
    }
    if (sign) {
        result[0] = '-';
    }
    if (exponent) {
        rc = memcpy_s(result + sign, 8, "Infinity", 8);
        securec_check(rc, "\0", "\0");
        return sign + 8;
    }
    result[sign] = '0';
    return sign + 1;
}

static inline uint32 float_to_bits(const float f)
{
    uint32 bits = 0;
    errno_t rc = EOK;

    rc = memcpy_s(&bits, sizeof(float), &f, sizeof(float));
    securec_check(rc, "\0", "\0");
    return bits;
}

static inline uint64 double_to_bits(const double d)
{
    uint64 bits = 0;
    errno_t rc = EOK;

    rc = memcpy_s(&bits, sizeof(double), &d, sizeof(double));
    securec_check(rc, "\0", "\0");
    return bits;
}

#endif /* RYU_COMMON_H */
