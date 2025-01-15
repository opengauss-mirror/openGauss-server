#include "postgres_fe.h"
#include "storage/checksum_impl.h"

static inline uint32 pg_checksum_init(uint32 seed, uint32 value)
{
    CHECKSUM_COMP(seed, value);
    return seed;
}

uint32 pg_checksum_block(char *data, uint32 size)
{
    uint32 sums[N_SUMS];
    uint32 *dataArr = (uint32 *)data;
    uint32 result = 0;
    uint32 i;
    uint32 j;

    /* ensure that the size is compatible with the algorithm */
    Assert((size % (sizeof(uint32) * N_SUMS)) == 0);

    uint32 CHECK_STEP = 2;
    /* initialize partial checksums to their corresponding offsets */
    for (j = 0; j < N_SUMS; j += CHECK_STEP) {
        sums[j] = pg_checksum_init(g_checksumBaseOffsets[j], dataArr[j]);
        sums[j + 1] = pg_checksum_init(g_checksumBaseOffsets[j + 1], dataArr[j + 1]);
    }
    dataArr += N_SUMS;

    /* main checksum calculation */
    for (i = 1; i < size / (sizeof(uint32) * N_SUMS); i++) {
        for (j = 0; j < N_SUMS; j += CHECK_STEP) {
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
}

/*
 * Compute the checksum for an openGauss page.  The page must be aligned on a
 * 4-byte boundary.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 */
uint16 pg_checksum_page(char *page, BlockNumber blkno)
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