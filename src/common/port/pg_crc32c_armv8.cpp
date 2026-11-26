/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * pg_crc32c_armv8.cpp
 *	  Compute CRC-32C checksum using ARMv8 CRC Extension instructions
 *
 *
 * IDENTIFICATION
 *	  src/common/port/pg_crc32c_armv8.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include <arm_acle.h>
#include <arm_neon.h>

#include "port/pg_crc32c.h"

#define SEGMENTBYTES 256
#define PARALLEL_CRC32_BATCH 1024

#define CRC32C32BYTES(P, IND)                                                              \
    do {                                                                                   \
        crc1 = __crc32cd(crc1, *((const uint64_t *)(P) + (SEGMENTBYTES / 8) * 1 + (IND))); \
        crc2 = __crc32cd(crc2, *((const uint64_t *)(P) + (SEGMENTBYTES / 8) * 2 + (IND))); \
        crc3 = __crc32cd(crc3, *((const uint64_t *)(P) + (SEGMENTBYTES / 8) * 3 + (IND))); \
        crc0 = __crc32cd(crc0, *((const uint64_t *)(P) + (SEGMENTBYTES / 8) * 0 + (IND))); \
    } while (0)

#define CRC32C256BYTES(P, IND)           \
    do {                                 \
        CRC32C32BYTES(P, (IND) * 8 + 0); \
        CRC32C32BYTES(P, (IND) * 8 + 1); \
        CRC32C32BYTES(P, (IND) * 8 + 2); \
        CRC32C32BYTES(P, (IND) * 8 + 3); \
        CRC32C32BYTES(P, (IND) * 8 + 4); \
        CRC32C32BYTES(P, (IND) * 8 + 5); \
        CRC32C32BYTES(P, (IND) * 8 + 6); \
        CRC32C32BYTES(P, (IND) * 8 + 7); \
    } while (0)

#define CRC32C1024BYTES(P)       \
    do {                         \
        CRC32C256BYTES(P, 0);    \
        CRC32C256BYTES(P, 1);    \
        CRC32C256BYTES(P, 2);    \
        CRC32C256BYTES(P, 3);    \
        (P) += 4 * SEGMENTBYTES; \
    } while (0)

#define SMALL_SEGMENT_BYTES 112
#define PARALLEL_CRC32_SMALL_BATCH 448

#define CRC32C32BYTES_SMALL_SEGMENT(P, IND)                                                       \
    do {                                                                                          \
        crc1 = __crc32cd(crc1, *((const uint64_t *)(P) + (SMALL_SEGMENT_BYTES / 8) * 1 + (IND))); \
        crc2 = __crc32cd(crc2, *((const uint64_t *)(P) + (SMALL_SEGMENT_BYTES / 8) * 2 + (IND))); \
        crc3 = __crc32cd(crc3, *((const uint64_t *)(P) + (SMALL_SEGMENT_BYTES / 8) * 3 + (IND))); \
        crc0 = __crc32cd(crc0, *((const uint64_t *)(P) + (SMALL_SEGMENT_BYTES / 8) * 0 + (IND))); \
    } while (0)

#define CRC32C224BYTES(P, IND)                         \
    do {                                               \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 0); \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 1); \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 2); \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 3); \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 4); \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 5); \
        CRC32C32BYTES_SMALL_SEGMENT(P, (IND) * 7 + 6); \
    } while (0)

#define CRC32C448BYTES(P)               \
    do {                                \
        CRC32C224BYTES(P, 0);           \
        CRC32C224BYTES(P, 1);           \
        (P) += 4 * SMALL_SEGMENT_BYTES; \
    } while (0)

pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len)
{
    const unsigned char *p = (const unsigned char*)data;
    const unsigned char *pend = p + len;

    pg_crc32c crc0;
    pg_crc32c crc1;
    pg_crc32c crc2;
    pg_crc32c crc3;
    uint64 t0;
    uint64 t1;
    uint64 t2;

    const static poly64_t k256[3] = {0xd7a4825c, 0xdd7e3b0c, 0xb9e02b86};
    const static poly64_t k112[3] = {0xa60ce07b, 0x83348832, 0x47db8317};

   /*
    * ARMv8 doesn't require alignment, but aligned memory access is
    * significantly faster. Process leading bytes so that the loop below
    * starts with a pointer aligned to eight bytes.
    */
    if (!PointerIsAligned(p, uint16) && p + 1 <= pend) {
        crc = __crc32cb(crc, *p);
        p += 1;
    }
    if (!PointerIsAligned(p, uint32) && p + 2 <= pend) {
        crc = __crc32ch(crc, *(uint16 *) p);
        p += 2;
    }
    if (!PointerIsAligned(p, uint64) && p + 4 <= pend) {
        crc = __crc32cw(crc, *(uint32 *) p);
        p += 4;
    }

    while (p + PARALLEL_CRC32_BATCH <= pend) {
        crc0 = crc;
        crc1 = 0;
        crc2 = 0;
        crc3 = 0;
        CRC32C1024BYTES(p);
        t2 = (uint64)vmull_p64(crc2, k256[2]);
        t1 = (uint64)vmull_p64(crc1, k256[1]);
        t0 = (uint64)vmull_p64(crc0, k256[0]);
        crc = crc3;
        crc ^= __crc32cd(0, t2);
        crc ^= __crc32cd(0, t1);
        crc ^= __crc32cd(0, t0);
    }

    /* specific case for xlog */
    while (p + PARALLEL_CRC32_SMALL_BATCH <= pend) {
        crc0 = crc;
        crc1 = 0;
        crc2 = 0;
        crc3 = 0;
        CRC32C448BYTES(p);
        t2 = (uint64)vmull_p64(crc2, k112[2]);
        t1 = (uint64)vmull_p64(crc1, k112[1]);
        t0 = (uint64)vmull_p64(crc0, k112[0]);
        crc = crc3;
        crc ^= __crc32cd(0, t2);
        crc ^= __crc32cd(0, t1);
        crc ^= __crc32cd(0, t0);
    }

    /* Process eight bytes at a time, as far as we can. */
    while (p + 8 <= pend) {
        crc = __crc32cd(crc, *(uint64 *) p);
        p += 8;
    }

    /* Process remaining 0-7 bytes. */
    if (p + 4 <= pend) {
        crc = __crc32cw(crc, *(uint32 *) p);
        p += 4;
    }
    if (p + 2 <= pend) {
        crc = __crc32ch(crc, *(uint16 *) p);
        p += 2;
    }
    if (p < pend) {
        crc = __crc32cb(crc, *p);
    }

    return crc;
}
