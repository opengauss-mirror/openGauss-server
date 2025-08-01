/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * halfutils.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/halfutils.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HALFUTILS_H
#define HALFUTILS_H

#include <math.h>

#include "access/datavec/halfvec.h"
#include "utils/shortest_dec.h"

#ifdef F16C_SUPPORT
#include <immintrin.h>
#endif

extern float (*HalfvecL2SquaredDistance)(int dim, half *ax, half *bx);
extern float (*HalfvecInnerProduct)(int dim, half *ax, half *bx);
extern double (*HalfvecCosineSimilarity)(int dim, half *ax, half *bx);
extern float (*HalfvecL1Distance)(int dim, half *ax, half *bx);

extern float HalfvecL2SquaredDistanceDefault(int dim, half *ax, half *bx);
extern float HalfvecInnerProductDefault(int dim, half *ax, half *bx);
extern double HalfvecCosineSimilarityDefault(int dim, half *ax, half *bx);
extern float HalfvecL1DistanceDefault(int dim, half *ax, half *bx);

void HalfvecInit(void);

/*
 * Check if half is NaN
 */
static inline bool HalfIsNan(half num)
{
#ifdef FLT16_SUPPORT
    return isnan(num);
#else
    return (num & 0x7C00) == 0x7C00 && (num & 0x7FFF) != 0x7C00;
#endif
}

/*
 * Check if half is infinite
 */
static inline bool HalfIsInf(half num)
{
#ifdef FLT16_SUPPORT
    return isinf(num);
#else
    return (num & 0x7FFF) == 0x7C00;
#endif
}

/*
 * Check if half is zero
 */
static inline bool HalfIsZero(half num)
{
#ifdef FLT16_SUPPORT
    return num == 0;
#else
    return (num & 0x7FFF) == 0x0000;
#endif
}

/*
 * Convert a half to a float4
 */
static inline float HalfToFloat4(half num)
{
#if defined(F16C_SUPPORT)
    return _cvtsh_ss(num);
#elif defined(FLT16_SUPPORT)
    return (float)num;
#else
    union {
        float f;
        uint32 i;
    } swapfloat;

    union {
        half h;
        uint16 i;
    } swaphalf;

    uint16 bin;
    uint32 exponent;
    uint32 mantissa;
    uint32 result;

    swaphalf.h = num;
    bin = swaphalf.i;
    exponent = (bin & 0x7C00) >> 10;
    mantissa = bin & 0x03FF;

    /* Sign */
    result = (bin & 0x8000) << 16;

    if (unlikely(exponent == 31)) {
        if (mantissa == 0) {
            /* Infinite */
            result |= 0x7F800000;
        } else {
            /* NaN */
            result |= 0x7FC00000;
        }
    } else if (unlikely(exponent == 0)) {
        /* Subnormal */
        if (mantissa != 0) {
            exponent = -14;

            for (int i = 0; i < 10; i++) {
                mantissa <<= 1;
                exponent -= 1;

                if ((mantissa >> 10) % 2 == 1) {
                    mantissa &= 0x03ff;
                    break;
                }
            }

            result |= (exponent + 127) << 23;
        }
    } else {
        /* Normal */
        result |= (exponent - 15 + 127) << 23;
    }

    result |= mantissa << 13;

    swapfloat.i = result;
    return swapfloat.f;
#endif
}

/*
 * Convert a float4 to a half
 */
static inline half Float4ToHalfUnchecked(float num)
{
#if defined(F16C_SUPPORT)
    return _cvtss_sh(num, 0);
#elif defined(FLT16_SUPPORT)
    return num;
#else
    union {
        float f;
        uint32 i;
    } swapfloat;

    union {
        half h;
        uint16 i;
    } swaphalf;

    uint32 bin;
    int exponent;
    int mantissa;
    uint16 result;

    swapfloat.f = num;
    bin = swapfloat.i;
    exponent = (bin & 0x7F800000) >> 23;
    mantissa = bin & 0x007FFFFF;

    /* Sign */
    result = (bin & 0x80000000) >> 16;

    if (isinf(num)) {
        /* Infinite */
        result |= 0x7C00;
    } else if (isnan(num)) {
        /* NaN */
        result |= 0x7E00;
        result |= mantissa >> 13;
    } else if (exponent > 98) {
        int m;
        int gr;
        int s;

        exponent -= 127;
        s = mantissa & 0x00000FFF;

        /* Subnormal */
        if (exponent < -14) {
            int diff = -exponent - 14;

            mantissa >>= diff;
            mantissa += 1 << (23 - diff);
            s |= mantissa & 0x00000FFF;
        }

        m = mantissa >> 13;

        /* Round */
        gr = (mantissa >> 12) % 4;
        if (gr == 3 || (gr == 1 && s != 0))
            m += 1;

        if (m == 1024) {
            m = 0;
            exponent += 1;
        }

        if (exponent > 15) {
            /* Infinite */
            result |= 0x7C00;
        } else {
            if (exponent >= -14)
                result |= (exponent + 15) << 10;

            result |= m;
        }
    }

    swaphalf.i = result;
    return swaphalf.h;
#endif
}

/*
 * Convert a float4 to a half
 */
static inline half Float4ToHalf(float num)
{
    half result = Float4ToHalfUnchecked(num);
    if (unlikely(HalfIsInf(result)) && !isinf(num)) {
        char *buf = (char *)palloc(FLOAT_SHORTEST_DECIMAL_LEN);

        float_to_shortest_decimal_buf(num, buf);

        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("\"%s\" is out of range for type halfvec", buf)));
    }

    return result;
}

#endif
