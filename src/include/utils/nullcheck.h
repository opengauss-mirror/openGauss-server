/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * nullcheck.h
 *    Declarations for accelerated NULL checks.
 *
 * IDENTIFICATION
 *    src/include/utils/nullcheck.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef UTILS_NULLCHECK_H
#define UTILS_NULLCHECK_H

static const int MIN_NULL_SKIP_ATTRIBUTE_COUNT = 512;

#ifdef __aarch64__
#include <arm_neon.h>

static const int NULL_CHECK_NEON_WIDTH = 16;
static_assert(sizeof(bool) == sizeof(uint8), "bool must occupy one byte for the NULL SIMD check");

/*
 * Return the leading NULL count, capped at NULL_CHECK_NEON_WIDTH.
 * Callers must ensure that isnull has at least NULL_CHECK_NEON_WIDTH
 * accessible elements and that isnull[0] is true.
 */
static inline int count_leading_nulls(const bool* isnull)
{
    Assert(isnull[0]);

    /* vminvq_u8() is non-zero only when all 16 loaded bool flags are true. */
    if (vminvq_u8(vld1q_u8((const uint8*)isnull)) != 0) {
        return NULL_CHECK_NEON_WIDTH;
    }

    /* The vector check proved that this 16-byte block contains a non-NULL flag. */
    int nullCount = 1;
    while (isnull[nullCount]) {
        nullCount++;
        Assert(nullCount < NULL_CHECK_NEON_WIDTH);
    }
    return nullCount;
}
#endif

#endif
