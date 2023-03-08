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
 * hashutils.h
 *        Utilities for working with hash values.
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/hashutils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef HASHUTILS_H
#define HASHUTILS_H

/*
 * Simple inline murmur hash implementation hashing a 32 bit integer, for
 * performance.
 */
static inline uint32 murmurhash32(uint32 data)
{
    uint32 h = data;

    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

static inline uint32 hash_combine(uint32 a, uint32 b)
{
    a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
    return a;
}

#endif /* HASHUTILS_H */
