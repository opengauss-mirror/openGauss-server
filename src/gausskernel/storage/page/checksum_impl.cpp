/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 *  checksum_impl.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/page/checksum_impl.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "utils/elog.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/checksum_impl.h"

#define CSI_DT_TWO 2

void ChecksumForZeroPadding(uint32 *sums, const uint32 *dataArr, uint32 currentLeft);

static inline uint32 pg_checksum_init(uint32 seed, uint32 value)
{
    CHECKSUM_COMP(seed, value);
    return seed;
}

uint32 DataBlockChecksum(char* data, uint32 size, bool zeroing)
{
    uint32 sums[N_SUMS];
    uint32* dataArr = (uint32*)data;
    uint32 result = 0;
    uint32 i, j;
    uint32 currentLeft = size;

    /* ensure that the size is compatible with the algorithm */
    uint32 alignSize = sizeof(uint32) * N_SUMS;
#ifndef ROACH_COMMON
    Assert(zeroing || (size % alignSize == 0));
#endif

    /* initialize partial checksums to their corresponding offsets */
    auto realSize = size < alignSize ? size : alignSize;

    uint32 *initUint32 = NULL;
    char usedForInit[sizeof(uint32) * N_SUMS] = {0};
    if (zeroing && size < alignSize) {
        errno_t rc = memcpy_s(usedForInit, alignSize, (char *) dataArr, realSize);
#ifndef ROACH_COMMON
        securec_check(rc, "", "");
#else
        (void)rc;
#endif
        currentLeft -= realSize;
        initUint32 = (uint32*)usedForInit;
    } else {
        initUint32 = dataArr;
        currentLeft -= alignSize;
    }

    uint8 step = 2;
    for (j = 0; j < N_SUMS; j += step) {
        sums[j] = pg_checksum_init(g_checksumBaseOffsets[j], initUint32[j]);
        sums[j + 1] = pg_checksum_init(g_checksumBaseOffsets[j + 1], initUint32[j + 1]);
    }
    dataArr += N_SUMS;

    /* main checksum calculation */
    for (i = 1; i < size / alignSize; i++) {
        for (j = 0; j < N_SUMS; j += step) {
            CHECKSUM_COMP(sums[j], dataArr[j]);
            CHECKSUM_COMP(sums[j + 1], dataArr[j + 1]);
        }
        dataArr += N_SUMS;
    }

    /* checksum for zero padding */
    currentLeft -= alignSize * (i - 1);
    if (currentLeft > 0 && currentLeft < alignSize && zeroing) {
        ChecksumForZeroPadding(sums, dataArr, currentLeft);
    }

    /* finally add in two rounds of zeroes for additional mixing */
    for (j = 0; j < N_SUMS; j++) {
        CHECKSUM_COMP(sums[j], 0);
        CHECKSUM_COMP(sums[j], 0);

        /* xor fold partial checksums together */
        result ^= sums[j];
    }

    return result;
}

void ChecksumForZeroPadding(uint32 *sums, const uint32 *dataArr, uint32 currentLeft)
{
    auto maxLen = sizeof(uint32) * N_SUMS;
    char currentLeftChars[maxLen] = {0};
    errno_t rc = memcpy_s(currentLeftChars, maxLen, (char *)dataArr, currentLeft);
#ifndef ROACH_COMMON
    securec_check(rc, "", "");
#else
    (void)rc;
#endif
    for (int j = 0; j < N_SUMS; j += CSI_DT_TWO) {
        CHECKSUM_COMP(sums[j], ((uint32 *)currentLeftChars)[j]);
        CHECKSUM_COMP(sums[j + 1], ((uint32 *)currentLeftChars)[j + 1]);
    }
}

#pragma GCC push_options
#pragma GCC optimize ("unroll-loops")
uint32 AUTO_VECTORIZE pg_checksum_block(char* data, uint32 size)
{
    uint32 sums[N_SUMS];
    uint32* dataArr = (uint32*)data;
    uint32 result = 0;
    uint32 i, j;

#ifndef ROACH_COMMON
    /* ensure that the size is compatible with the algorithm */
    Assert((size % (sizeof(uint32) * N_SUMS)) == 0);
#endif

    uint8 step = 2;
    /* initialize partial checksums to their corresponding offsets */
    for (j = 0; j < N_SUMS; j += step) {
        sums[j] = pg_checksum_init(g_checksumBaseOffsets[j], dataArr[j]);
        sums[j + 1] = pg_checksum_init(g_checksumBaseOffsets[j + 1], dataArr[j + 1]);
    }
    dataArr += N_SUMS;

    /* main checksum calculation */
    for (i = 1; i < size / (sizeof(uint32) * N_SUMS); i++) {
        for (j = 0; j < N_SUMS; j += step) {
            CHECKSUM_COMP(sums[j], dataArr[j]);
            CHECKSUM_COMP(sums[j + 1], dataArr[j + 1]);
        }
        dataArr += N_SUMS;
    }

    /* finally add in two rounds of zeroes for additional mixing */
    for (j = 0; j < N_SUMS; j++) {
        CHECKSUM_COMP(sums[j], 0);
        CHECKSUM_COMP(sums[j], 0);

        /* xor fold partial checksums together */
        result ^= sums[j];
    }

    return result;
#pragma GCC pop_options
}

/*
 * Compute the checksum for a openGauss page.  The page must be aligned on a
 * 4-byte boundary.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 */
uint16 pg_checksum_page(char* page, BlockNumber blkno)
{
    PageHeader phdr = (PageHeader)page;
    uint16 save_checksum;
    uint32 checksum;

    /*
     * Save pd_checksum and temporarily set it to zero, so that the checksum
     * calculation isn't affected by the old checksum stored on the page.
     * Restore it after, because actually updating the checksum is NOT part of
     * the API of this function.
     */
    save_checksum = phdr->pd_checksum;
    phdr->pd_checksum = 0;
    checksum = pg_checksum_block(page, BLCKSZ);
    phdr->pd_checksum = save_checksum;

    /* Mix in the block number to detect transposed pages */
    checksum ^= blkno;

    /*
     * Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
     * one. That avoids checksums of zero, which seems like a good idea.
     */
    return (checksum % UINT16_MAX) + 1;
}
