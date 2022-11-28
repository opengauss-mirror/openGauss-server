/*
 * page_compression.h
 *		internal declarations for page compression
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/storage/page_compression_impl.h
 */

#ifndef RC_INCLUDE_STORAGE_PAGE_COMPRESSION_IMPL_H
#define RC_INCLUDE_STORAGE_PAGE_COMPRESSION_IMPL_H

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/mman.h>

#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(_M_ARM64) || defined(__aarch64__)
#include <arm_neon.h>
#else  /* none */
#endif

#include "storage/page_compression.h"
#include "storage/checksum_impl.h"
#include "utils/pg_lzcompress.h"

#include "access/ustore/knl_upage.h"

#include <zstd.h>

#define DEFAULT_ZSTD_COMPRESSION_LEVEL (1)
#define MIN_ZSTD_COMPRESSION_LEVEL ZSTD_minCLevel()
#define MAX_ZSTD_COMPRESSION_LEVEL ZSTD_maxCLevel()

#define COMPRESS_DEFAULT_ERROR (-1)
#define COMPRESS_UNSUPPORTED_ERROR (-2)
#define GS_INVALID_ID16     (uint16)0xFFFF
#define MIN_DIFF_SIZE (64)
#define MIN_CONVERT_CNT (4)

#ifndef USE_ASSERT_CHECKING
#define ASSERT(condition)
#else
#define ASSERT(condition) assert(condition)
#endif

typedef struct UHeapPageCompressData {
    char page_header[SizeOfUHeapPageHeaderData]; /* page header */
    uint32 crc32;
    uint32 size : 16;                       /* size of compressed data */
    uint32 byte_convert : 1;
    uint32 diff_convert : 1;
    uint32 algorithm : 4;
    uint32 unused : 10;
    char data[FLEXIBLE_ARRAY_MEMBER];       /* compressed page, except for the page header */
} UHeapPageCompressData;

#define GET_ITEMID_BY_IDX(buf, i) ((ItemIdData *)(((char *)buf) + GetPageHeaderSize(buf) + (i) * sizeof(ItemIdData)))
#define GET_UPAGE_ITEMID_BY_IDX(buf, offset, i) ((RowPtr *)(((char *)buf) + offset + i * sizeof(RowPtr)))  // for upage only

THR_LOCAL char aux_abuf[BLCKSZ];
THR_LOCAL char src_copy[BLCKSZ];

/**
 * return data of page
 * @param dst HeapPageCompressData or HeapPageCompressData
 * @param heapPageData heapPageData or pagedata
 * @return dst->data
 */
static inline char* GetPageCompressedData(char* dst, uint8 pagetype)
{
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        return ((UHeapPageCompressData*)dst)->data;
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        return ((HeapPageCompressData*)dst)->data;
    } else {
        return ((PageCompressData*)dst)->data;
    }
}

static inline void FreePointer(void* pointer)
{
    if (pointer != NULL) {
        pfree(pointer);
    }
}

/*======================================================================================*/
#define COMMON ""

#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)
void Transpose8x8U(uint8 **src, uint8 **dst)
{
    /* load and crossed-store 0~3 lines */
    __m128i str0 = _mm_loadl_epi64((__m128i *)(src[0]));
    __m128i str1 = _mm_loadl_epi64((__m128i *)(src[1]));
    __m128i str2 = _mm_loadl_epi64((__m128i *)(src[2]));
    __m128i str3 = _mm_loadl_epi64((__m128i *)(src[3]));
    __m128i str2x1 = _mm_unpacklo_epi8(str0, str1);
    __m128i str2x2 = _mm_unpacklo_epi8(str2, str3);
    __m128i str4x1L = _mm_unpacklo_epi16(str2x1, str2x2);
    __m128i str4x1H = _mm_unpackhi_epi16(str2x1, str2x2);

    /* load and crossed-store 4~7 lines */
    __m128i str4 = _mm_loadl_epi64((__m128i *)(src[4]));
    __m128i str5 = _mm_loadl_epi64((__m128i *)(src[5]));
    __m128i str6 = _mm_loadl_epi64((__m128i *)(src[6]));
    __m128i str7 = _mm_loadl_epi64((__m128i *)(src[7]));
    __m128i str2x3 = _mm_unpacklo_epi8(str4, str5);
    __m128i str2x4 = _mm_unpacklo_epi8(str6, str7);
    __m128i str4x2L = _mm_unpacklo_epi16(str2x3, str2x4);
    __m128i str4x2H = _mm_unpackhi_epi16(str2x3, str2x4);

    /* store into dst */
    __m128i dst0 = _mm_unpacklo_epi32(str4x1L, str4x2L);
    _mm_storel_epi64((__m128i *)(dst[0]), dst0);
    _mm_storel_epi64((__m128i *)(dst[1]), _mm_srli_si128(dst0, 8));

    __m128i dst1 = _mm_unpackhi_epi32(str4x1L, str4x2L);
    _mm_storel_epi64((__m128i *)(dst[2]), dst1);
    _mm_storel_epi64((__m128i *)(dst[3]), _mm_srli_si128(dst1, 8));

    __m128i dst2 = _mm_unpacklo_epi32(str4x1H, str4x2H);
    _mm_storel_epi64((__m128i *)(dst[4]), dst2);
    _mm_storel_epi64((__m128i *)(dst[5]), _mm_srli_si128(dst2, 8));

    __m128i dst3 = _mm_unpackhi_epi32(str4x1H, str4x2H);
    _mm_storel_epi64((__m128i *)(dst[6]), dst3);
    _mm_storel_epi64((__m128i *)(dst[7]), _mm_srli_si128(dst3, 8));
}

#elif defined(_M_ARM64) || defined(__aarch64__)
void Transpose8x16U(uint8 **src, uint8 **dst)
{
    /* load and crossed-store 0~3 lines */
    uint8x16x4_t matLine4x1;
    uint8x16x4_t matLine4x2;
    uint8 sptr4x1[64];
    uint8 sptr4x2[64];

    matLine4x1.val[0] = vld1q_u8(src[0]);
    matLine4x1.val[1] = vld1q_u8(src[1]);
    matLine4x1.val[2] = vld1q_u8(src[2]);
    matLine4x1.val[3] = vld1q_u8(src[3]);
    vst4q_u8(sptr4x1, matLine4x1);

    /* load and crossed-store 4~7 lines */
    matLine4x2.val[0] = vld1q_u8(src[4]);
    matLine4x2.val[1] = vld1q_u8(src[5]);
    matLine4x2.val[2] = vld1q_u8(src[6]);
    matLine4x2.val[3] = vld1q_u8(src[7]);
    vst4q_u8(sptr4x2, matLine4x2);

    uint32x4x2_t dstLine04;
    uint32x4x2_t dstLine15;
    uint32x4x2_t dstLine26;
    uint32x4x2_t dstLine37;
    uint8 dstPtr[128];

    dstLine04.val[0] = vld1q_u32((uint32_t *)sptr4x1);
    dstLine04.val[1] = vld1q_u32((uint32_t *)sptr4x2);
    dstLine15.val[0] = vld1q_u32((uint32_t *)(sptr4x1 + 16));
    dstLine15.val[1] = vld1q_u32((uint32_t *)(sptr4x2 + 16));
    dstLine26.val[0] = vld1q_u32((uint32_t *)(sptr4x1 + 32));
    dstLine26.val[1] = vld1q_u32((uint32_t *)(sptr4x2 + 32));
    dstLine37.val[0] = vld1q_u32((uint32_t *)(sptr4x1 + 48));
    dstLine37.val[1] = vld1q_u32((uint32_t *)(sptr4x2 + 48));

    vst2q_u32((uint32_t *)dstPtr, dstLine04);
    vst2q_u32((uint32_t *)(dstPtr + 32), dstLine15);
    vst2q_u32((uint32_t *)(dstPtr + 64), dstLine26);
    vst2q_u32((uint32_t *)(dstPtr + 96), dstLine37);

    /* store into dst */
    *((uint64 *)(dst[0])) = *((uint64 *)(dstPtr + 0 * 8));
    *((uint64 *)(dst[1])) = *((uint64 *)(dstPtr + 1 * 8));
    *((uint64 *)(dst[2])) = *((uint64 *)(dstPtr + 2 * 8));
    *((uint64 *)(dst[3])) = *((uint64 *)(dstPtr + 3 * 8));
    *((uint64 *)(dst[4])) = *((uint64 *)(dstPtr + 4 * 8));
    *((uint64 *)(dst[5])) = *((uint64 *)(dstPtr + 5 * 8));
    *((uint64 *)(dst[6])) = *((uint64 *)(dstPtr + 6 * 8));
    *((uint64 *)(dst[7])) = *((uint64 *)(dstPtr + 7 * 8));
    *((uint64 *)(dst[8])) = *((uint64 *)(dstPtr + 8 * 8));
    *((uint64 *)(dst[9])) = *((uint64 *)(dstPtr + 9 * 8));
    *((uint64 *)(dst[10])) = *((uint64 *)(dstPtr + 10 * 8));
    *((uint64 *)(dst[11])) = *((uint64 *)(dstPtr + 11 * 8));
    *((uint64 *)(dst[12])) = *((uint64 *)(dstPtr + 12 * 8));
    *((uint64 *)(dst[13])) = *((uint64 *)(dstPtr + 13 * 8));
    *((uint64 *)(dst[14])) = *((uint64 *)(dstPtr + 14 * 8));
    *((uint64 *)(dst[15])) = *((uint64 *)(dstPtr + 15 * 8));

    return ;
}

void Transpose8x8U(uint8 **src, uint8 **dst)
{
    /* load and crossed-store 0~3 lines */
    uint8x8x4_t matLine4x1;
    uint8x8x4_t matLine4x2;
    uint8 sptr4x1[32];
    uint8 sptr4x2[32];

    matLine4x1.val[0] = vld1_u8(src[0]);
    matLine4x1.val[1] = vld1_u8(src[1]);
    matLine4x1.val[2] = vld1_u8(src[2]);
    matLine4x1.val[3] = vld1_u8(src[3]);
    vst4_u8(sptr4x1, matLine4x1);

    /* load and crossed-store 4~7 lines */
    matLine4x2.val[0] = vld1_u8(src[4]);
    matLine4x2.val[1] = vld1_u8(src[5]);
    matLine4x2.val[2] = vld1_u8(src[6]);
    matLine4x2.val[3] = vld1_u8(src[7]);
    vst4_u8(sptr4x2, matLine4x2);

   /* store into dst */
    uint32x4x2_t dstLine4x1;
    uint32x4x2_t dstLine4x2;
    uint8 dstPtr[64];

    dstLine4x1.val[0] = vld1q_u32((uint32_t *)sptr4x1);
    dstLine4x1.val[1] = vld1q_u32((uint32_t *)sptr4x2);
    dstLine4x2.val[0] = vld1q_u32((uint32_t *)(sptr4x1 + 16));
    dstLine4x2.val[1] = vld1q_u32((uint32_t *)(sptr4x2 + 16));

    vst2q_u32((uint32_t *)dstPtr, dstLine4x1);
    vst2q_u32((uint32_t *)(dstPtr + 32), dstLine4x2);

    *((uint64 *)(dst[0])) = *((uint64 *)(dstPtr + 0 * 8));
    *((uint64 *)(dst[1])) = *((uint64 *)(dstPtr + 1 * 8));
    *((uint64 *)(dst[2])) = *((uint64 *)(dstPtr + 2 * 8));
    *((uint64 *)(dst[3])) = *((uint64 *)(dstPtr + 3 * 8));
    *((uint64 *)(dst[4])) = *((uint64 *)(dstPtr + 4 * 8));
    *((uint64 *)(dst[5])) = *((uint64 *)(dstPtr + 5 * 8));
    *((uint64 *)(dst[6])) = *((uint64 *)(dstPtr + 6 * 8));
    *((uint64 *)(dst[7])) = *((uint64 *)(dstPtr + 7 * 8));

    return;
}

#else  /* none */
#endif


/*======================================================================================*/
#define COMPRESS ""

static void CompressDiffItemIds(char *buf, uint32 offset, uint16 real_row_cnt) {
    uint16 row_cnt = real_row_cnt;
    uint8 *copy_begin = (uint8 *)(buf + offset);
    int16 i;
    uint8 *prev, *curr;

    // from the last to first
    for (i = row_cnt - 1; i > 0; i--) {
        prev = copy_begin + (i - 1) * sizeof(ItemIdData);
        curr = copy_begin + i  * sizeof(ItemIdData);

        // do difference, for itemid has sizeof(ItemIdData) = 4 Bytes = (32 bits), only for speed improvment.
        curr[3] -= prev[3];
        curr[2] -= prev[2];
        curr[1] -= prev[1];
        curr[0] -= prev[0];
    }

    return ;
}

static inline void CompressDiffCommRow(uint8* prev, uint8* curr, uint16 min_row_len)
{
    int16 j = 0;

#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)
    // try 16 Bytes
    for (j = 0; (j + 16) <= min_row_len; j += 16) {
        __m128i prev_val = _mm_loadu_si128((__m128i *)(prev + j));
        __m128i curr_val = _mm_loadu_si128((__m128i *)(curr + j));

        __m128i diff_val = _mm_sub_epi8(curr_val, prev_val);
        _mm_storeu_si128((__m128i *)(curr + j), diff_val);
    }
    // try 8 Bytes
    if ((j + 8) <= min_row_len) {
        __m128i prev_val = _mm_castpd_si128(_mm_load_sd((double *)(prev + j)));
        __m128i curr_val = _mm_castpd_si128(_mm_load_sd((double *)(curr + j)));

        __m128i diff_val = _mm_sub_epi8(curr_val, prev_val);
        _mm_store_sd((double *)(curr + j), _mm_castsi128_pd(diff_val));
        j += 8;
    }

#elif defined(_M_ARM64) || defined(__aarch64__)
    // try 16 Bytes
    for (j = 0; (j + 16) <= min_row_len; j += 16) {
        uint8x16_t prev_val = vld1q_u8(prev + j);
        uint8x16_t curr_val = vld1q_u8(curr + j);

        uint8x16_t diff_val = vsubq_u8(curr_val, prev_val);
        vst1q_u8(curr + j, diff_val);
    }
    // try 8 Bytes
    if ((j + 8) <= min_row_len) {
        uint8x8_t prev_val = vld1_u8(prev + j);
        uint8x8_t curr_val = vld1_u8(curr + j);

        uint8x8_t diff_val = vsub_u8(curr_val, prev_val);
        vst1_u8(curr + j, diff_val);
        j += 8;
    }

#endif
    // try n(n < 8) Bytes
    for (; j < min_row_len; j++) {
        curr[j] -= prev[j];
    }
}

static void CompressDiffRows(char *buf, int16 *real_order, uint16 min_row_len, uint16 real_row_cnt, bool is_heap) {
    uint16 row_cnt;
    uint8 *copy_begin = (uint8 *)(buf);

    int16 i;
    uint8 *prev, *curr;
    uint16 curr_off, prev_off;

    if (is_heap) {
        row_cnt = real_row_cnt;
        for (i = row_cnt - 1; i > 0; i--) {
            curr_off = GET_ITEMID_BY_IDX(buf, (real_order[i]))->lp_off;
            prev_off = GET_ITEMID_BY_IDX(buf, (real_order[i - 1]))->lp_off;

            curr = copy_begin + curr_off;
            prev = copy_begin + prev_off;

            CompressDiffCommRow(prev, curr, min_row_len);
        }
    } else {
        row_cnt = (((PageHeaderData *)buf)->pd_lower - GetPageHeaderSize(buf)) / sizeof(ItemIdData);
        bool ready = false;
        for (i = row_cnt - 1; i >= 0; i--) {
            if (!ItemIdIsNormal(GET_ITEMID_BY_IDX(buf, i))) {
                continue;
            }

            if (!ready) {
                curr_off = GET_ITEMID_BY_IDX(buf, i)->lp_off;
                ready = true;
                continue;
            }

            prev_off = GET_ITEMID_BY_IDX(buf, i)->lp_off;

            curr = copy_begin + curr_off;
            prev = copy_begin + prev_off;

            CompressDiffCommRow(prev, curr, min_row_len);

            curr_off = prev_off;
        }
    }

    return;
}

void CompressConvertItemIds(char *buf, char *aux_buf) {
    errno_t ret;
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    uint32 total_size = row_cnt * sizeof(ItemIdData);
    char *copy_begin = buf + GetPageHeaderSize(page);
    uint16 i, k;

    k = 0;
    for (i = 0; i < row_cnt; i++) {
        aux_buf[0  * row_cnt + i] = copy_begin[k++];
        aux_buf[1  * row_cnt + i] = copy_begin[k++];
        aux_buf[2  * row_cnt + i] = copy_begin[k++];
        aux_buf[3  * row_cnt + i] = copy_begin[k++];
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

static void CompressConvertIndexKeysPart2(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 max_row_len, uint16 real_row_cnt)
{
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);

    uint8 *copt_begin = aux_buf + min_row_len * real_row_cnt;
    uint8 *row;
    int16 *index_order = real_order + row_cnt; // for index-page only.
    int16 *normal_indexes = real_order + row_cnt * 3; // for index-page only.
    uint16 i, j, k, cur, up, row_size, itemid_idx;

    k = 0;
    for (i = min_row_len; i < max_row_len; i++) {
        for (j = 0; j < real_row_cnt; j++) {
            itemid_idx = normal_indexes[j];
            cur = GET_ITEMID_BY_IDX(buf, itemid_idx)->lp_off;
            up = (index_order[itemid_idx] == (real_row_cnt - 1)) ? page->pd_special :
                GET_ITEMID_BY_IDX(buf, (real_order[index_order[itemid_idx] + 1]))->lp_off;
            row_size = up - cur;
            row = buf + cur;
            if (i < row_size) {
                copt_begin[k++] = row[i];  // this part is reshaped
            }
        }
    }

    return ;
}

static void CompressConvertIndexKeysPart1(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 real_row_cnt)
{
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    int16 *normal_indexes = real_order + row_cnt * 3; // for index-page only.
    int w = min_row_len, h = real_row_cnt;

    // from left to right and change to down line
    // aux_buf is Continuous memory but buf is not
#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y;
    for(y = 0; y < sh; y = (y + 8)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 7]))->lp_off;
        for(x = 0; x < sw; x = (x + 8)) {
            uint8 *src[8] = {buf + line0_off + x, buf + line1_off + x, buf + line2_off + x, buf + line3_off + x,
                buf + line4_off + x, buf + line5_off + x, buf + line6_off + x, buf + line7_off + x};
            uint8 *dst[8] = {aux_buf + x * h + y, aux_buf + (x + 1) * h + y,
                aux_buf + (x + 2) * h + y, aux_buf + (x + 3) * h + y,
                aux_buf + (x + 4) * h + y, aux_buf + (x + 5) * h + y,
                aux_buf + (x + 6) * h + y, aux_buf + (x + 7) * h + y};
            Transpose8x8U(src, dst);
        }
    }
    for(y = sh; y < h; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y]))->lp_off;
        for(x=0;x<w;x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }
    for(y = 0; y < sh; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y]))->lp_off;
        for(x = sw; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }

#elif defined(_M_ARM64) || defined(__aarch64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y, step = 8;

    for(y = 0; y < sh; y = (y + step)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y + 7]))->lp_off;

        for(x = 0; (x + 16) <= sw; x = (x + 16)) {
            uint8 *src[8] = {buf + line0_off + x, buf + line1_off + x, buf + line2_off + x, buf + line3_off + x,
                    buf + line4_off + x, buf + line5_off + x, buf + line6_off + x, buf + line7_off + x};
            uint8 *dst[16] = {aux_buf + x * h + y, aux_buf + (x + 1) * h + y,
                    aux_buf + (x + 2) * h + y, aux_buf + (x +3) * h + y,
                    aux_buf + (x + 4) * h + y, aux_buf + (x +5) * h + y,
                    aux_buf + (x + 6) * h + y, aux_buf + (x +7) * h + y,
                    aux_buf + (x + 8) * h + y, aux_buf + (x +9) * h + y,
                    aux_buf + (x + 10) * h + y, aux_buf + (x +11) * h + y,
                    aux_buf + (x + 12) * h + y, aux_buf + (x +13) * h + y,
                    aux_buf + (x + 14) * h + y, aux_buf + (x +15) * h + y};
            Transpose8x16U(src, dst);
        }
        if ((x + 8) <= sw) {
            uint8 *src[8] = {buf + line0_off + x, buf + line1_off + x, buf + line2_off + x, buf + line3_off + x,
                        buf + line4_off + x, buf + line5_off + x, buf + line6_off + x, buf + line7_off + x};
            uint8 *dst[8] = {aux_buf + x * h + y, aux_buf + (x + 1) * h + y, aux_buf + (x + 2) * h + y,
                             aux_buf + (x + 3) * h + y, aux_buf + (x + 4) * h + y, aux_buf + (x + 5) * h + y,
                             aux_buf + (x + 6) * h + y, aux_buf + (x + 7) * h + y};
            Transpose8x8U(src, dst);
        }
    }

    for(y = sh; y < h; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y]))->lp_off;
        for(x = 0; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }

    for(y = 0; y < sh; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[y]))->lp_off;
        for(x = sw - 1; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }

#else  /* none */
    CompressConvertIndexKeysPart2(buf, aux_buf, real_order, 0, min_row_len, real_row_cnt);
#endif

}

static void CompressConvertIndexKeys(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 max_row_len, uint16 real_row_cnt)
{
    errno_t ret;
    PageHeaderData *page = (PageHeaderData *)buf;

    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off;
    uint32 total_size = page->pd_special - min_offset;
    uint8 *copy_begin = buf + min_offset;

    // step 1: transpose the matrix [0:real_row_cnt, 0:min_row_len]
    CompressConvertIndexKeysPart1(buf, aux_buf, real_order, min_row_len, real_row_cnt);

    // step 2: transpose the matrix [0:real_row_cnt, min_row_len:max_row_len]
    if (min_row_len < max_row_len) {
        CompressConvertIndexKeysPart2(buf, aux_buf, real_order,
            min_row_len, max_row_len, real_row_cnt);
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
}

static void CompressConvertHeapRowsPart2(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 max_row_len, uint16 real_row_cnt)
{
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = real_row_cnt;
    uint8 *row;
    uint16 i, j, k, cur, up, row_size;
    uint8 *copt_begin = aux_buf + min_row_len * real_row_cnt;

    k = 0;
    for (i = min_row_len; i < max_row_len; i++) {
        for (j = 0; j < row_cnt; j++) {
            up = (j == (row_cnt - 1)) ? page->pd_special : GET_ITEMID_BY_IDX(buf, (real_order[j + 1]))->lp_off;
            cur = GET_ITEMID_BY_IDX(buf, (real_order[j]))->lp_off;
            row_size = up - cur;
            row = buf + cur;
            if (i < row_size) {
                copt_begin[k++] = row[i];  // this part is reshaped
            }
        }
    }

    return ;
}

static void CompressConvertHeapRowsPart1(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 real_row_cnt)
{
    int w = min_row_len, h = real_row_cnt;

    // from left to right and change to down line
    // aux_buf is Continuous memory but buf is not
#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y;
    for(y = 0; y < sh; y = (y + 8)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[y]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 7]))->lp_off;
        for(x = 0; x < sw; x = (x + 8)) {
            uint8 *src[8] = {buf + line0_off + x, buf + line1_off + x, buf + line2_off + x, buf + line3_off + x,
                buf + line4_off + x, buf + line5_off + x, buf + line6_off + x, buf + line7_off + x};
            uint8 *dst[8] = {aux_buf + x * h + y, aux_buf + (x + 1) * h + y,
                aux_buf + (x + 2) * h + y, aux_buf + (x + 3) * h + y,
                aux_buf + (x + 4) * h + y, aux_buf + (x + 5) * h + y,
                aux_buf + (x + 6) * h + y, aux_buf + (x + 7) * h + y};
            Transpose8x8U(src, dst);
        }
    }
    for(y = sh; y < h; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[y]))->lp_off;
        for(x = 0; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }
    for(y = 0; y < sh; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[y]))->lp_off;
        for(x = sw; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }

#elif defined(_M_ARM64) || defined(__aarch64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y;

    for(y=0;y<sh;y=y+8) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[y]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (real_order[y + 7]))->lp_off;

        for(x = 0; (x + 16) <= sw; x = (x + 16)) {
            uint8 *src[8] = {buf + line0_off + x, buf + line1_off + x, buf + line2_off + x, buf + line3_off + x,
                            buf + line4_off + x, buf + line5_off + x, buf + line6_off + x, buf + line7_off + x};
            uint8 *dst[16] = {aux_buf + x * h + y, aux_buf + (x + 1) * h + y,
                            aux_buf + (x + 2) * h + y, aux_buf + (x + 3) * h + y,
                            aux_buf + (x + 4) * h + y, aux_buf + (x + 5) * h + y,
                            aux_buf + (x + 6) * h + y, aux_buf + (x + 7) * h + y,
                            aux_buf + (x + 8) * h + y, aux_buf + (x + 9) * h + y,
                            aux_buf + (x + 10) * h + y, aux_buf + (x + 11) * h + y,
                            aux_buf + (x + 12) * h + y, aux_buf + (x + 13) * h + y,
                            aux_buf + (x + 14) * h + y, aux_buf + (x + 15) * h + y};
            Transpose8x16U(src, dst);
        }

        if ((x + 8) <= sw) {
            uint8 *src[8] = {buf + line0_off + x, buf + line1_off + x, buf + line2_off + x, buf + line3_off + x,
                            buf + line4_off + x, buf + line5_off + x, buf + line6_off + x, buf + line7_off + x};
            uint8 *dst[8] = {aux_buf + x * h + y, aux_buf + (x + 1) * h + y,
                            aux_buf + (x + 2) * h + y, aux_buf + (x + 3) * h + y,
                            aux_buf + (x + 4) * h + y, aux_buf + (x + 5) * h + y,
                            aux_buf + (x + 6) * h + y, aux_buf + (x + 7) * h + y};
            Transpose8x8U(src, dst);
        }
    }

    for(y = sh; y < h; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[y]))->lp_off;
        for(x = 0; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }
    for(y = 0; y < sh; y++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[y]))->lp_off;
        for(x = sw - 1; x < w; x++)
            aux_buf[x * h + y] = buf[line0_off + x];
    }

#else  /* none */
    CompressConvertHeapRowsPart2(buf, aux_buf, real_order, 0, min_row_len, real_row_cnt);
#endif

}

static void CompressConvertHeapRows(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 max_row_len, uint16 real_row_cnt)
{
    errno_t ret;
    PageHeaderData *page = (PageHeaderData *)buf;

    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off;
    uint32 total_size = page->pd_special - min_offset;
    uint8 *copy_begin = buf + min_offset;

    // step 1: transpose the matrix [0:real_row_cnt, 0:min_row_len]
    CompressConvertHeapRowsPart1(buf, aux_buf, real_order, min_row_len, real_row_cnt);

    // step 2: transpose the matrix [0:real_row_cnt, min_row_len:max_row_len]
    if (min_row_len < max_row_len) {
        CompressConvertHeapRowsPart2(buf, aux_buf, real_order,
            min_row_len, max_row_len, real_row_cnt);
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
}

// 1: sorted as tuple_offset order, that means asc order.
// 2: store all itemid's idx.
// 3:maybe some itemid is not in order.
void CompressConvertItemRealOrder(char *buf, int16 *real_order, uint16 real_row_cnt) {
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    ItemIdData *begin = (ItemIdData *)(buf + GetPageHeaderSize(page));
    int16 *index_order = real_order + row_cnt; // for index of itemids only.
    int16 *link_order = real_order + row_cnt * 2;  // tmp area for sorting procession.
    int16 *normal_indexes = real_order + row_cnt * 3; // for index-page only.

    int16 i, k, head, curr, prev;
    int16 end = -1; // invalid index

    head = end;
    k = 0;
    // very likely to seems that itemids stored by desc order, and ignore invalid itemid
    for (i = 0; i < row_cnt; i++) {
        if (!ItemIdIsNormal(begin + i)) {
            continue;
        }
        normal_indexes[k++] = i;

        if (head == end) {  // set the head idx, insert the first
            link_order[i] = end;
            head = i;
            continue;
        }

        if ((begin + i)->lp_off < (begin + head)->lp_off) {
            link_order[i] = head; // update the head idx
            head = i;
            continue;
        }

        prev = head;
        curr = link_order[head];
        while ((curr != end) && ((begin + i)->lp_off > (begin + curr)->lp_off)) {
            prev = curr;
            curr = link_order[curr];
        }

        link_order[prev] = i;
        link_order[i] = curr;
    }

    // arrange the link to array
    curr = head;
    for (i = 0; i < real_row_cnt; i++) {
        real_order[i] = curr;
        index_order[curr] = i;
        curr = link_order[curr];
    }

    if (curr != end) {
        printf("ERROR!!! pre_convert_real_order error...!!!\n");
        ASSERT(0);
        return;
    }

}

// maybe some itemid is not valid
uint16 HeapPageCalcRealRowCnt(char *buf) {
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 cnt = 0;
    uint16 i;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);

    for (i = 0; i < row_cnt; i++) {
        if (ItemIdIsNormal(GET_ITEMID_BY_IDX(buf, i))) {
            cnt++;
        }
    }
    return cnt;
}

// to find all row size are diffs in MIN_DIFF_SIZE byts.
bool CompressConvertCheck(char *buf, int16 **real_order, uint16 *max_row_len, uint16 *min_row_len, uint16 *real_row_cnt) {
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    int16 i, row_size;
    ItemIdData *ptr = NULL;
    uint16 up = page->pd_special;
    uint16 min_size = GS_INVALID_ID16;
    uint16 max_size = 0;
    errno_t ret;
    if (page->pd_lower < GetPageHeaderSize(page) || (page->pd_lower >  page->pd_upper)) {
        return false;
    }

    uint16 normal_row_cnt = HeapPageCalcRealRowCnt(buf);
    if (normal_row_cnt < MIN_CONVERT_CNT) {  // no need convert
        return false;
    }

    // to store the real tuple order.
    /*
            --------------------------|--------------------------|--------------------------
            xxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxxxxxxxxxxxxxxxxxxxxxx
            --------------------------|--------------------------|--------------------------
    */
    // the first part is real array order, and the second part is for index order, and the third part is link.
    *real_order = (int16 *)palloc(sizeof(uint16) * row_cnt * 4);
    if (*real_order == NULL) {
        printf("zfunc compress file");
        return false;
    }
    ret = memset_sp(*real_order, sizeof(uint16) * row_cnt * 4, 0, sizeof(uint16) * row_cnt * 4);
    securec_check(ret, "", "");

    // order the ItemIds by tuple_offset order.
    CompressConvertItemRealOrder(buf, *real_order, normal_row_cnt);

    // do the check,  to check all size of tuples.
    for (i = normal_row_cnt - 1; i >= 0; i--) {
        ptr = GET_ITEMID_BY_IDX(buf, ((*real_order)[i]));

        row_size = up - ptr->lp_off;
        if (row_size < MIN_CONVERT_CNT * 2) {
            return false;
        }

        min_size = (row_size < min_size) ? row_size : min_size;
        max_size = (row_size > max_size) ? row_size : max_size;

        if ((max_size - min_size) > MIN_DIFF_SIZE) {  // no need convert
            return false;
        }
        up = ptr->lp_off;
    }

    // get the min row common size.
    *max_row_len = max_size;
    *min_row_len = min_size;
    *real_row_cnt = normal_row_cnt;
    return true;
}

bool CompressConvertOnePage(char *buf, char *aux_buf, bool diff_convert) {
    uint16 max_row_len = 0;
    uint16 min_row_len = 0;
    int16 *real_order = NULL; // itemids are not in order sometimes. we must find the real
    uint16 real_row_cnt = 0;
    PageHeaderData *page = (PageHeaderData *)buf;
    if (!CompressConvertCheck(buf, &real_order, &max_row_len, &min_row_len, &real_row_cnt)) {
        FreePointer((void*)real_order);
        return false;
    }

    // =======firstly, arrange the tuples.
    if (page->pd_special < BLCKSZ) {
        // diff first
        if (diff_convert) {
            CompressDiffRows(buf, real_order, min_row_len, real_row_cnt, false);
        }
        // convert by order of itemid location in itemd-area
        CompressConvertIndexKeys((uint8 *)buf, (uint8 *)aux_buf, real_order, min_row_len, max_row_len, real_row_cnt);  // for index
    } else {
        // diff first
        if (diff_convert) {
            CompressDiffRows(buf, real_order, min_row_len, real_row_cnt, true);
        }
        // convert by order of row location offset
        CompressConvertHeapRows((uint8 *)buf, (uint8 *)aux_buf, real_order, min_row_len, max_row_len, real_row_cnt);  // for heap
    }

    // =======finally, the itemids.
    if (diff_convert) {
        CompressDiffItemIds(buf, GetPageHeaderSize(page),
            (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData));
    }
    CompressConvertItemIds(buf, aux_buf);

    FreePointer((void*)real_order);
    return true;
}

static void CompressDiffItemIds_U(char *buf, uint32 offset, uint16 real_row_cnt) {
    /* because sizeof(RowPtr) is equals sizeof(ItemIdData) */
    CompressDiffItemIds(buf, offset, real_row_cnt);
    return ;
}

static void CompressDiffTDs_U(char *buf, uint32 offset, uint16 min_row_len, uint16 real_row_cnt) {
    uint16 row_cnt;
    uint8 *copy_begin = (uint8 *)(buf + offset);

    int16 i;
    uint8 *prev, *curr;
    uint16 curr_off, prev_off;

    row_cnt = real_row_cnt;
    for (i = row_cnt - 1; i > 0; i--) {
        curr_off = min_row_len * i;
        prev_off = min_row_len * (i - 1);

        curr = copy_begin + curr_off;
        prev = copy_begin + prev_off;

        CompressDiffCommRow(prev, curr, min_row_len);
    }

    return ;
}

static void CompressDiffRows_U(char *buf, int16 *real_order, uint16 min_row_len, uint16 real_row_cnt) {
    uint16 row_cnt;
    uint8 *copy_begin = (uint8 *)(buf);
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;

    int16 i;
    uint8 *prev, *curr;
    uint16 curr_off, prev_off;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);

    /* for table only */
    row_cnt = real_row_cnt;
    for (i = row_cnt - 1; i > 0; i--) {
        curr_off = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[i])));
        prev_off = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[i - 1])));

        curr = copy_begin + curr_off;
        prev = copy_begin + prev_off;

        CompressDiffCommRow(prev, curr, min_row_len);
    }

    return;
}

void CompressConvertTDs_U(char *buf, char *aux_buf) {
    errno_t ret;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 row_cnt = page->td_count;
    uint32 total_size = row_cnt * sizeof(TD);
    char *copy_begin = buf + SizeOfUHeapPageHeaderData;
    uint16 i, j, k;

    k = 0;
    for (i = 0; i < row_cnt; i++) {
        for (j = 0; j < sizeof(TD); j++) {
            aux_buf[j  * row_cnt + i] = copy_begin[k++];
        }
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

void CompressConvertItemIds_U(char *buf, char *aux_buf) {
    errno_t ret;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 row_cnt = (page->pd_lower - uheap_header_len) / sizeof(RowPtr);
    uint32 total_size = row_cnt * sizeof(RowPtr);
    char *copy_begin = buf + uheap_header_len;
    uint16 i, j, k;

    k = 0;
    for (i = 0; i < row_cnt; i++) {
        for (j = 0; j < sizeof(RowPtr); j++) {
            aux_buf[j  * row_cnt + i] = copy_begin[k++];
        }
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

void CompressConvertHeapRows_U(char *buf, char *aux_buf, int16 *real_order, uint16 max_row_len, uint16 real_row_cnt) {
    errno_t ret;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 row_cnt = real_row_cnt;

    uint16 min_offset = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, real_order[0]));
    uint32 total_size = page->pd_special - min_offset;
    char *copy_begin = buf + min_offset;
    char *row;
    uint16 i, j, k, cur, up, row_size;

    k = 0;
    for (i = 0; i < max_row_len; i++) {
        for (j = 0; j < row_cnt; j++) {
            up = (j == (row_cnt - 1)) ? page->pd_special :
                RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[j + 1])));
            cur = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[j])));
            row_size = up - cur;
            row = buf + cur;
            if (i < row_size) {
                aux_buf[k++] = row[i];  // this part is reshaped
            }
        }
    }

    if (k != total_size) {
        printf("ERROR!!! convert_rows_2 error...!!!\n");
        ASSERT(0);
        return;
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

// 1: sorted as tuple_offset order, that means asc order.
// 2: store all itemid's idx.
// 3:maybe some itemid is not in order.
void CompressConvertItemRealOrder_U(char *buf, int16 *real_order, uint16 real_row_cnt) {
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 row_cnt = (page->pd_lower - uheap_header_len) / sizeof(RowPtr);
    RowPtr *begin = (RowPtr *)(buf + uheap_header_len);
    int16 *index_order = real_order + row_cnt; // for index of itemids only.
    int16 *link_order = real_order + row_cnt * 2;  // tmp area for sorting procession.

    int16 i, head, curr, prev;
    int16 end = -1; // invalid index

    head = end;
    // very likely to seems that itemids stored by desc order, and ignore invalid itemid
    for (i = 0; i < row_cnt; i++) {
        if (!RowPtrIsNormal(begin + i)) {
            continue;
        }

        if (head == end) {  // set the head idx, insert the first
            link_order[i] = end;
            head = i;
            continue;
        }

        if (RowPtrGetOffset(begin + i) < RowPtrGetOffset(begin + head)) {
            link_order[i] = head; // update the head idx
            head = i;
            continue;
        }

        prev = head;
        curr = link_order[head];
        while ((curr != end) && (RowPtrGetOffset(begin + i) > RowPtrGetOffset(begin + curr))) {
            prev = curr;
            curr = link_order[curr];
        }

        link_order[prev] = i;
        link_order[i] = curr;
    }

    // arrange the link to array
    curr = head;
    for (i = 0; i < real_row_cnt; i++) {
        real_order[i] = curr;
        index_order[curr] = i;
        curr = link_order[curr];
    }

    if (curr != end) {
        printf("ERROR!!! pre_convert_real_order error...!!!\n");
        ASSERT(0);
        return;
    }

}

// maybe some itemid is not valid
uint16 HeapPageCalcRealRowCnt_U(char *buf) {
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 cnt = 0;
    uint16 i;
    uint16 row_cnt = (page->pd_lower - uheap_header_len) / sizeof(RowPtr);

    for (i = 0; i < row_cnt; i++) {
        if (RowPtrIsNormal(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, i))) {
            cnt++;
        }
    }
    return cnt;
}

// to find all row size are diffs in MIN_DIFF_SIZE byts.
bool CompressConvertCheck_U(char *buf, int16 **real_order, uint16 *max_row_len, uint16 *min_row_len, uint16 *real_row_cnt) {
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 row_cnt = (page->pd_lower - uheap_header_len) / sizeof(RowPtr);
    int16 i, row_size;
    RowPtr *ptr = NULL;
    uint16 up = page->pd_special;
    uint16 min_size = GS_INVALID_ID16;
    uint16 max_size = 0;
    errno_t ret;
    if (page->pd_lower < uheap_header_len || (page->pd_lower >  page->pd_upper)) {
        return false;
    }

    uint16 normal_row_cnt = HeapPageCalcRealRowCnt_U(buf);
    if (normal_row_cnt < MIN_CONVERT_CNT) {  // no need convert
        return false;
    }

    // to store the real tuple order.
    /*
            --------------------------|--------------------------|--------------------------
            xxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxxxxxxxxxxxxxxxxxxxxxx
            --------------------------|--------------------------|--------------------------
    */
    // the first part is real array order, and the second part is for index order, and the third part is link.
    *real_order = (int16 *)palloc(sizeof(uint16) * row_cnt * 3);
    if (*real_order == NULL) {
        printf("zfunc compress file");
        return false;
    }
    ret = memset_sp(*real_order, sizeof(uint16) * row_cnt * 3, 0, sizeof(uint16) * row_cnt * 3);
    securec_check(ret, "", "");

    // order the ItemIds by tuple_offset order.
    CompressConvertItemRealOrder_U(buf, *real_order, normal_row_cnt);

    // do the check,  to check all size of tuples.
    for (i = normal_row_cnt - 1; i >= 0; i--) {
        ptr = GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, ((*real_order)[i]));

        row_size = up - RowPtrGetOffset(ptr);
        if (row_size < MIN_CONVERT_CNT * 2) {
            return false;
        }

        min_size = (row_size < min_size) ? row_size : min_size;
        max_size = (row_size > max_size) ? row_size : max_size;

        if ((max_size - min_size) > MIN_DIFF_SIZE) {  // no need convert
            return false;
        }
        up = RowPtrGetOffset(ptr);
    }

    // get the min row common size.
    *max_row_len = max_size;
    *min_row_len = min_size;
    *real_row_cnt = normal_row_cnt;
    return true;
}

bool CompressConvertOnePage_U(char *buf, char *aux_buf, bool diff_convert) {
    uint16 max_row_len = 0;
    uint16 min_row_len = 0;
    int16 *real_order = NULL; // itemids are not in order sometimes. we must find the real
    uint16 real_row_cnt = 0;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);

    if (!CompressConvertCheck_U(buf, &real_order, &max_row_len, &min_row_len, &real_row_cnt)) {
        FreePointer((void*)real_order);
        return false;
    }

    // =======firstly, arrange the tuples.
    if (page->pd_special < BLCKSZ) {
        FreePointer((void*)real_order);
        return false;
    } else {
        // diff first
        if (diff_convert) {
            CompressDiffRows_U(buf, real_order, min_row_len, real_row_cnt);
        }
        // convert by order of row location offset
        CompressConvertHeapRows_U(buf, aux_buf, real_order, max_row_len, real_row_cnt);  // for heap
    }

    // =======finally, the itemids and TDs.
    if (diff_convert) {
        CompressDiffItemIds_U(buf, uheap_header_len, (page->pd_lower - uheap_header_len) / sizeof(RowPtr));
        CompressDiffTDs_U(buf, SizeOfUHeapPageHeaderData, sizeof(TD), page->td_count);
    }
    CompressConvertItemIds_U(buf, aux_buf);
    CompressConvertTDs_U(buf, aux_buf);

    FreePointer((void*)real_order);
    return true;
}

void CompressPagePrepareConvert(char *src, bool diff_convert, bool *real_ByteConvert, uint8 pagetype)
{
    //char *aux_abuf = NULL;
    errno_t rc;

    rc = memset_sp(aux_abuf, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "", "");

    // do convert
    *real_ByteConvert = false;
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        // for ustore only
        if (CompressConvertOnePage_U(src, aux_abuf, diff_convert)) {
            *real_ByteConvert = true;
        }
    } else {
        // common-type page and heap page and segment page have same struct.
        if (CompressConvertOnePage(src, aux_abuf, diff_convert)) {
            *real_ByteConvert = true;
        }
    }
}

inline size_t CompressReservedLen(const char* page)
{
    auto length = offsetof(HeapPageCompressData, page_header) - offsetof(HeapPageCompressData, data);
    return GetPageHeaderSize(page) + length;
}

/**
 * CompressPageBufferBound()
 * -- Get the destination buffer boundary to compress one page.
 * Return needed destination buffer size for compress one page or
 *     -1 for unrecognized compression algorithm
 */
int CompressPageBufferBound(const char* page, uint8 algorithm)
{
    switch (algorithm) {
        case COMPRESS_ALGORITHM_PGLZ:
            return BLCKSZ * 2;
        case COMPRESS_ALGORITHM_ZSTD:
            return ZSTD_compressBound(BLCKSZ - CompressReservedLen(page));
        case COMPRESS_ALGORITHM_PGZSTD:
            return BLCKSZ + 4;
        default:
            return -1;
    }
}

int CompressPage(const char* src, char* dst, int dst_size, RelFileCompressOption option)
{
    uint8 pagetype = PageGetPageLayoutVersion(src);
    switch (pagetype) {
        case PG_UHEAP_PAGE_LAYOUT_VERSION:
            return TemplateCompressPage<PG_UHEAP_PAGE_LAYOUT_VERSION>(src, dst, dst_size, option);
        case PG_HEAP_PAGE_LAYOUT_VERSION:
            return TemplateCompressPage<PG_HEAP_PAGE_LAYOUT_VERSION>(src, dst, dst_size, option);
        case PG_COMM_PAGE_LAYOUT_VERSION:
            return TemplateCompressPage<PG_COMM_PAGE_LAYOUT_VERSION>(src, dst, dst_size, option);
        default :
            break;
    }

    // no need to compress
    return BLCKSZ;
}

int DecompressPage(const char* src, char* dst)
{
    uint8 pagetype = PageGetPageLayoutVersion(src);
    switch (pagetype) {
        case PG_UHEAP_PAGE_LAYOUT_VERSION:
            return TemplateDecompressPage<PG_UHEAP_PAGE_LAYOUT_VERSION>(src, dst);
        case PG_HEAP_PAGE_LAYOUT_VERSION:
            return TemplateDecompressPage<PG_HEAP_PAGE_LAYOUT_VERSION>(src, dst);
        case PG_COMM_PAGE_LAYOUT_VERSION:
            return TemplateDecompressPage<PG_COMM_PAGE_LAYOUT_VERSION>(src, dst);
        default :
            break;
    }

    // no need to compress
    return -1;
}

inline size_t GetSizeOfHeadData(uint8 pagetype)
{
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        return SizeOfUHeapPageHeaderData;
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        return SizeOfHeapPageHeaderData;
    } else {
        return SizeOfPageHeaderData;
    }
}

inline size_t GetSizeOfCprsHeadData(uint8 pagetype)
{
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        return offsetof(UHeapPageCompressData, data);
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        return offsetof(HeapPageCompressData, data);
    } else {
        return offsetof(PageCompressData, data);
    }
}

/**
 * CompressPage() -- Compress one page.
 *
 *		Only the parts other than the page header will be compressed. The
 *		compressed data is rounded by chunck_size, The insufficient part is
 *		filled with zero.  Compression needs to be able to save at least one
 *		chunk of space, otherwise it fail.
 *		This function returen the size of compressed data or
 *		    -1 for compression fail
 *		    COMPRESS_UNSUPPORTED_ERROR for unrecognized compression algorithm
 */
template <uint8 pagetype>
int TemplateCompressPage(const char* src, char* dst, int dst_size, RelFileCompressOption option)
{
    int compressed_size;
    int8 level = option.compressLevelSymbol ? option.compressLevel : -option.compressLevel;
    size_t sizeOfHeaderData = GetSizeOfHeadData(pagetype);
    //char* src_copy = NULL;
    bool real_ByteConvert = false;
    errno_t rc;

    if (option.byteConvert) {
        rc = memcpy_s(src_copy, BLCKSZ, src, BLCKSZ);
        securec_check(rc, "", "");
        CompressPagePrepareConvert(src_copy, option.diffConvert, &real_ByteConvert, pagetype); /* preprocess convert src */
    }

    char* data = GetPageCompressedData(dst, pagetype);
    PageHeaderData *page = (PageHeaderData *)src;
    bool heapPageData = page->pd_special == BLCKSZ;
    switch (option.compressAlgorithm) {
        case COMPRESS_ALGORITHM_PGLZ: {
            bool success;
            if (real_ByteConvert) {
                success = pglz_compress(src_copy + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, (PGLZ_Header *)data,
                    heapPageData ? PGLZ_strategy_default : PGLZ_strategy_always);
            } else {
                success = pglz_compress(src + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, (PGLZ_Header *)data,
                    heapPageData ? PGLZ_strategy_default : PGLZ_strategy_always);
            }
            compressed_size = success ? VARSIZE(data) : BLCKSZ;
            compressed_size = compressed_size < BLCKSZ ? compressed_size : BLCKSZ;
            break;
        }
        case COMPRESS_ALGORITHM_ZSTD: {
            if (level == 0 || level < MIN_ZSTD_COMPRESSION_LEVEL || level > MAX_ZSTD_COMPRESSION_LEVEL) {
                level = DEFAULT_ZSTD_COMPRESSION_LEVEL;
            }
#ifndef FRONTEND
            if (t_thrd.page_compression_cxt.zstd_cctx == NULL) {
                if (real_ByteConvert) {
                    compressed_size =
                        ZSTD_compress(data, dst_size, src_copy + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, level);
                } else {
                    compressed_size =
                        ZSTD_compress(data, dst_size, src + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, level);
                }
            } else {
                if (real_ByteConvert) {
                    compressed_size =
                        ZSTD_compressCCtx((ZSTD_CCtx *)t_thrd.page_compression_cxt.zstd_cctx,
                        data, dst_size, src_copy + sizeOfHeaderData,
                        BLCKSZ - sizeOfHeaderData, level);
                } else {
                    compressed_size =
                        ZSTD_compressCCtx((ZSTD_CCtx *)t_thrd.page_compression_cxt.zstd_cctx,
                        data, dst_size, src + sizeOfHeaderData,
                        BLCKSZ - sizeOfHeaderData, level);
                }
            }
#else
            if (real_ByteConvert) {
                compressed_size =
                    ZSTD_compress(data, dst_size, src_copy + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, level);
            } else {
                compressed_size =
                    ZSTD_compress(data, dst_size, src + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, level);
            }
#endif

            if (ZSTD_isError(compressed_size)) {
                return -1;
            }
            break;
        }
        default:
            return COMPRESS_UNSUPPORTED_ERROR;
    }

    if ((compressed_size < 0) || ((GetSizeOfCprsHeadData(pagetype) + compressed_size) >= BLCKSZ)) {
        return -1;
    }

    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        UHeapPageCompressData* pcdptr = ((UHeapPageCompressData*)dst);
        rc = memcpy_s(pcdptr->page_header, sizeOfHeaderData, src, sizeOfHeaderData);
        securec_check(rc, "", "");
        pcdptr->size = compressed_size;
        pcdptr->crc32 = DataBlockChecksum(data, compressed_size, true);
        pcdptr->byte_convert = real_ByteConvert;
        pcdptr->diff_convert = option.diffConvert;
        pcdptr->algorithm = option.compressAlgorithm;
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        HeapPageCompressData* pcdptr = ((HeapPageCompressData*)dst);
        rc = memcpy_s(pcdptr->page_header, sizeOfHeaderData, src, sizeOfHeaderData);
        securec_check(rc, "", "");
        pcdptr->size = compressed_size;
        pcdptr->crc32 = DataBlockChecksum(data, compressed_size, true);
        pcdptr->byte_convert = real_ByteConvert;
        pcdptr->diff_convert = option.diffConvert;
        pcdptr->algorithm = option.compressAlgorithm;
    } else {
        PageCompressData* pcdptr = ((PageCompressData*)dst);
        rc = memcpy_s(pcdptr->page_header, sizeOfHeaderData, src, sizeOfHeaderData);
        securec_check(rc, "", "");
        pcdptr->size = compressed_size;
        pcdptr->crc32 = DataBlockChecksum(data, compressed_size, true);
        pcdptr->byte_convert = real_ByteConvert;
        pcdptr->diff_convert = option.diffConvert;
        pcdptr->algorithm = option.compressAlgorithm;
    }

    return GetSizeOfCprsHeadData(pagetype) + compressed_size;
}

/*======================================================================================*/
#define DECOMPRESS ""

static void CompressDeDiffItemIds(char *buf, uint32 offset, uint16 real_row_cnt) {
    uint16 row_cnt = real_row_cnt;
    uint8 *copy_begin = (uint8 *)(buf + offset);
    int16 i;
    uint8 *prev, *curr;

    // from the first to last
    for (i = 1; i < row_cnt; i++) {
        prev = copy_begin + (i - 1)  * sizeof(ItemIdData);
        curr = copy_begin + i  * sizeof(ItemIdData);

        // do difference, for itemid has sizeof(ItemIdData) = 4 bytes, for speed improvment.
        curr[3] += prev[3];
        curr[2] += prev[2];
        curr[1] += prev[1];
        curr[0] += prev[0];
    }
    return ;
}

static inline void CompressDeDiffCommRow(uint8* prev, uint8* curr, uint16 min_row_len)
{
    int16 j = 0;

#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)
    // try 16 Bytes
    for (j = 0; (j + 16) <= min_row_len; j += 16) {
        __m128i prev_val = _mm_loadu_si128((__m128i *)(prev + j));
        __m128i curr_val = _mm_loadu_si128((__m128i *)(curr + j));

        __m128i diff_val = _mm_add_epi8(curr_val, prev_val);
        _mm_storeu_si128((__m128i *)(curr + j), diff_val);
    }
    // try 8 Bytes
    if ((j + 8) <= min_row_len) {
        __m128i prev_val = _mm_castpd_si128(_mm_load_sd((double *)(prev + j)));
        __m128i curr_val = _mm_castpd_si128(_mm_load_sd((double *)(curr + j)));

        __m128i diff_val = _mm_add_epi8(curr_val, prev_val);
        _mm_store_sd((double *)(curr + j), _mm_castsi128_pd(diff_val));
        j += 8;
    }

#elif defined(_M_ARM64) || defined(__aarch64__)
    // try 16 Bytes
    for (j = 0; (j + 16) <= min_row_len; j += 16) {
        uint8x16_t prev_val = vld1q_u8(prev + j);
        uint8x16_t curr_val = vld1q_u8(curr + j);

        uint8x16_t diff_val = vaddq_u8(curr_val, prev_val);
        vst1q_u8(curr + j, diff_val);
    }
    // try 8 Bytes
    if ((j + 8) <= min_row_len) {
        uint8x8_t prev_val = vld1_u8(prev + j);
        uint8x8_t curr_val = vld1_u8(curr + j);

        uint8x8_t diff_val = vadd_u8(curr_val, prev_val);
        vst1_u8(curr + j, diff_val);
        j += 8;
    }

#endif
    // try n(n < 8) Bytes
    for (; j < min_row_len; j++) {
        curr[j] += prev[j];
    }

}

static void CompressDeDiffRows(char *buf, int16 *real_order, uint16 min_row_len, uint16 real_row_cnt, bool is_table) {
    uint16 row_cnt;
    uint8 *copy_begin = (uint8 *)(buf);

    int16 i;
    uint8 *prev, *curr;
    uint16 curr_off, prev_off;

    if (is_table) {
        row_cnt = real_row_cnt;
        for (i = 1; i < row_cnt; i++) {
            curr_off = GET_ITEMID_BY_IDX(buf, (real_order[i]))->lp_off;
            prev_off = GET_ITEMID_BY_IDX(buf, (real_order[i - 1]))->lp_off;

            curr = copy_begin + curr_off;
            prev = copy_begin + prev_off;

            CompressDeDiffCommRow(prev, curr, min_row_len);
        }
    } else {
        row_cnt = (((PageHeaderData *)buf)->pd_lower - GetPageHeaderSize(buf)) / sizeof(ItemIdData);
        bool ready = false;
        for (i = 0; i < row_cnt; i++) {
            if (!ItemIdIsNormal(GET_ITEMID_BY_IDX(buf, i))) {
                continue;
            }

            if (!ready) {
                prev_off = GET_ITEMID_BY_IDX(buf, i)->lp_off;
                ready = true;
                continue;
            }

            curr_off = GET_ITEMID_BY_IDX(buf, i)->lp_off;

            curr = copy_begin + curr_off;
            prev = copy_begin + prev_off;

            CompressDeDiffCommRow(prev, curr, min_row_len);

            prev_off = curr_off;
        }
    }

    return;
}

static void DecompressDeconvertIndexKeysPart2(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 max_row_len, uint16 real_row_cnt) {
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);

    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off + min_row_len * real_row_cnt;
    uint8 *copy_begin = buf + min_offset;
    uint8 *row;
    int16 *index_order = real_order + row_cnt; // for index-page only.
    int16 *normal_indexes = real_order + row_cnt * 3; // for index-page only.
    uint16 i, j, k, cur, up, row_size, itemid_idx;

    k = 0;
    for (i = min_row_len; i < max_row_len; i++) {
        for (j = 0; j < real_row_cnt; j++) {
            itemid_idx = normal_indexes[j];
            cur = GET_ITEMID_BY_IDX(buf, itemid_idx)->lp_off;
            up = (index_order[itemid_idx] == (real_row_cnt - 1))
                  ? page->pd_special
                  : GET_ITEMID_BY_IDX(buf, (real_order[index_order[itemid_idx] + 1]))->lp_off;
            row_size = up - cur;
            row = aux_buf + cur;
            if (i < row_size) {
                row[i] = copy_begin[k++];  // this part is reshaped
            }
        }
    }

    return ;
}

static void DecompressDeconvertIndexKeysPart1(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 real_row_cnt)
{
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    int16 *normal_indexes = real_order + row_cnt * 3; // for index-page only.
    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off;
    uint8 *copy_begin = buf + min_offset;

    int w = real_row_cnt, h = min_row_len;

    // from up to down and change to right col
    // buf is Continuous memory but aux_buf is not
#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y;
    for(x = 0; x < sw; x = (x + 8)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 7]))->lp_off;
        for(y = 0; y < sh; y = (y + 8)) {
            uint8 *src[8] = {copy_begin + y * w + x, copy_begin + (y + 1) * w + x,
                            copy_begin + (y + 2) * w + x, copy_begin + (y + 3) * w + x,
                            copy_begin + (y + 4) * w + x, copy_begin + (y + 5) * w + x,
                            copy_begin + (y + 6) * w + x, copy_begin + (y + 7) * w + x};
            uint8 *dst[8] = {aux_buf + line0_off + y, aux_buf + line1_off + y,
                            aux_buf + line2_off + y, aux_buf + line3_off + y,
                            aux_buf + line4_off + y, aux_buf + line5_off + y,
                            aux_buf + line6_off + y, aux_buf + line7_off + y};
            Transpose8x8U(src, dst);
        }
    }
    for(x = 0;x < sw; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        for(y = sh; y < h; y++)
            aux_buf[line0_off + y] = copy_begin[y * w + x];
    }
    for(x = sw; x < w; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        for(y = 0; y < h; y++)
            aux_buf[line0_off+y] = copy_begin[y * w + x];
    }

#elif defined(_M_ARM64) || defined(__aarch64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x,y;
    for(x = 0; (x + 16) <= sw; x = (x + 16)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 7]))->lp_off;
        uint16 line8_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 8]))->lp_off;
        uint16 line9_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 9]))->lp_off;
        uint16 line10_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 10]))->lp_off;
        uint16 line11_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 11]))->lp_off;
        uint16 line12_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 12]))->lp_off;
        uint16 line13_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 13]))->lp_off;
        uint16 line14_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 14]))->lp_off;
        uint16 line15_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 15]))->lp_off;
        for(y = 0; y < sh; y = (y + 8)) {
            uint8 *src[8] = {copy_begin + y * w + x, copy_begin + (y + 1) * w + x,
                            copy_begin + (y + 2) * w + x, copy_begin + (y + 3) * w + x,
                            copy_begin + (y + 4) * w + x, copy_begin + (y + 5) * w + x,
                            copy_begin + (y + 6) * w + x, copy_begin + (y + 7) * w + x};
            uint8 *dst[16] = {aux_buf + line0_off + y, aux_buf + line1_off + y,
                            aux_buf + line2_off + y, aux_buf + line3_off + y,
                            aux_buf + line4_off + y, aux_buf + line5_off + y,
                            aux_buf + line6_off + y, aux_buf + line7_off + y,
                            aux_buf + line8_off + y, aux_buf + line9_off + y,
                            aux_buf + line10_off + y, aux_buf + line11_off + y,
                            aux_buf + line12_off + y, aux_buf + line13_off + y,
                            aux_buf + line14_off + y, aux_buf + line15_off + y};
            Transpose8x16U(src, dst);
        }
    }
    if ((x + 8) <= sw) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x + 7]))->lp_off;
        for(y = 0; y < sh; y = y + 8) {
            uint8 *src[8] = {copy_begin + y * w + x, copy_begin + (y + 1) * w + x,
                            copy_begin + (y + 2) * w + x, copy_begin + (y + 3) * w + x,
                            copy_begin + (y + 4) * w + x, copy_begin + (y + 5) * w + x,
                            copy_begin + (y + 6) * w + x, copy_begin + (y + 7) * w + x};
            uint8 *dst[8] = {aux_buf + line0_off + y, aux_buf + line1_off + y,
                            aux_buf + line2_off + y, aux_buf + line3_off + y,
                            aux_buf + line4_off + y, aux_buf + line5_off + y,
                            aux_buf + line6_off + y, aux_buf + line7_off + y};
            Transpose8x8U(src, dst);
        }
    }
    for(x = 0; x < sw; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        for(y = sh; y < h; y++) {
            aux_buf[line0_off + y] = copy_begin[y * w + x];
        }
    }
    for(x = sw; x < w; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (normal_indexes[x]))->lp_off;
        for(y = 0;y < h; y++)
            aux_buf[line0_off + y] = copy_begin[y * w + x];
    }

#else  /* none */
    DecompressDeconvertIndexKeysPart2(buf, aux_buf, real_order, 0, min_row_len, real_row_cnt);
#endif

}

static void DecompressDeconvertIndexKeys(uint8 *buf, uint8 *aux_buf, int16 *real_order, uint16 min_row_len,
    uint16 max_row_len, uint16 real_row_cnt)
{
    errno_t ret;
    PageHeaderData *page = (PageHeaderData *)buf;

    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off;
    uint16 total_size = page->pd_special - min_offset;
    uint8 *copy_begin = buf + min_offset;

    // step 1: transpose the matrix [0:real_row_cnt, 0:min_row_len]
    DecompressDeconvertIndexKeysPart1(buf, aux_buf, real_order, min_row_len, real_row_cnt);

    // step 2: transpose the matrix [0:real_row_cnt, min_row_len:max_row_len]
    if (min_row_len < max_row_len) {
        DecompressDeconvertIndexKeysPart2(buf, aux_buf, real_order, min_row_len, max_row_len, real_row_cnt);
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf + min_offset, total_size);
    securec_check(ret, "", "");
}

static void DecompressDeconvertHeapRowsPart2(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 max_row_len, uint16 real_row_cnt) {
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = real_row_cnt;

    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off + min_row_len * real_row_cnt;
    uint8 *copy_begin = buf + min_offset;
    uint8 *row;
    uint16 i, j, k, cur, up, row_size;

    k = 0;
    for (i = min_row_len; i < max_row_len; i++) {
        for (j = 0; j < row_cnt; j++) {
            up = (j == (row_cnt - 1)) ? page->pd_special : GET_ITEMID_BY_IDX(buf, (real_order[j + 1]))->lp_off;
            cur = GET_ITEMID_BY_IDX(buf, (real_order[j]))->lp_off;
            row_size = up - cur;
            row = aux_buf + cur;
            if (i < row_size) {
                row[i] = copy_begin[k++];  // this part is reshaped
            }
        }
    }

    return ;
}

static void DecompressDeconvertHeapRowsPart1(uint8 *buf, uint8 *aux_buf, int16 *real_order,
    uint16 min_row_len, uint16 real_row_cnt)
{
    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off;
    uint8 *copy_begin = buf + min_offset;
    int h = min_row_len, w = real_row_cnt;

    // from up to down and change to right col
    // buf is Continuous memory but aux_buf is not
#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y;
    for(x = 0; x < sw; x = (x + 8)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 7]))->lp_off;

        uint8 step = 8;
        for(y = 0; y < sh; y = (y + step)) {
            uint8 *src[8] = {copy_begin + y * w + x, copy_begin + (y + 1) * w + x,
                            copy_begin + (y + 2) * w + x, copy_begin + (y + 3) * w + x,
                            copy_begin + (y + 4) * w + x, copy_begin + (y + 5) * w + x,
                            copy_begin + (y + 6) * w + x, copy_begin + (y + 7) * w + x};
            uint8 *dst[8] = {aux_buf + line0_off + y, aux_buf + line1_off + y,
                            aux_buf + line2_off + y, aux_buf + line3_off + y,
                            aux_buf + line4_off + y, aux_buf + line5_off + y,
                            aux_buf + line6_off + y, aux_buf + line7_off + y};
            Transpose8x8U(src, dst);
        }
    }
    for(x = 0; x < sw; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        for(y = sh; y < h; y++)
            aux_buf[line0_off + y] = copy_begin[y * w + x];
    }
    for(x = sw; x < w; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        for(y = 0; y < h; y++) {
            aux_buf[line0_off + y] = copy_begin[y * w + x];
        }
    }

#elif defined(_M_ARM64) || defined(__aarch64__)

    int dw = w & 7;
    int dh = h & 7;
    int sw = w - dw;
    int sh = h - dh;
    int x, y;
    for(x = 0;(x + 16) <= sw; x = (x + 16)) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 7]))->lp_off;
        uint16 line8_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 8]))->lp_off;
        uint16 line9_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 9]))->lp_off;
        uint16 line10_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 10]))->lp_off;
        uint16 line11_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 11]))->lp_off;
        uint16 line12_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 12]))->lp_off;
        uint16 line13_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 13]))->lp_off;
        uint16 line14_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 14]))->lp_off;
        uint16 line15_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 15]))->lp_off;
        for(y = 0; y < sh; y = (y + 8)) {
            uint8 *src[8] = {copy_begin + y * w + x, copy_begin + (y + 1) * w + x,
                             copy_begin + (y + 2)*w + x, copy_begin + (y + 3) * w + x,
                             copy_begin + (y + 4)*w + x, copy_begin + (y + 5) * w + x,
                             copy_begin + (y + 6)*w + x, copy_begin + (y + 7) * w + x};
            uint8 *dst[16] = {aux_buf + line0_off + y, aux_buf + line1_off + y, aux_buf + line2_off + y,
                aux_buf + line3_off + y, aux_buf + line4_off + y, aux_buf + line5_off + y,  aux_buf + line6_off + y,
                aux_buf + line7_off + y, aux_buf + line8_off + y, aux_buf + line9_off + y, aux_buf + line10_off + y,
                aux_buf + line11_off + y, aux_buf + line12_off + y, aux_buf + line13_off + y, aux_buf + line14_off + y,
                aux_buf + line15_off + y};
            Transpose8x16U(src, dst);
        }
    }
    if ((x + 8) <= sw) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        uint16 line1_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 1]))->lp_off;
        uint16 line2_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 2]))->lp_off;
        uint16 line3_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 3]))->lp_off;
        uint16 line4_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 4]))->lp_off;
        uint16 line5_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 5]))->lp_off;
        uint16 line6_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 6]))->lp_off;
        uint16 line7_off = GET_ITEMID_BY_IDX(buf, (real_order[x + 7]))->lp_off;

        for(y = 0; y < sh; y=y + 8) {
            uint8 *src[8] = {copy_begin + y * w + x, copy_begin + (y + 1) * w + x,
                             copy_begin + (y + 2) * w + x, copy_begin + (y + 3) * w + x,
                             copy_begin + (y + 4) * w + x, copy_begin + (y + 5) * w + x,
                             copy_begin + (y + 6) * w + x, copy_begin + (y + 7) * w + x};
            uint8 *dst[8] = {aux_buf + line0_off + y, aux_buf + line1_off + y, aux_buf + line2_off + y,
                aux_buf + line3_off + y, aux_buf + line4_off + y, aux_buf + line5_off + y,
                aux_buf + line6_off + y, aux_buf + line7_off + y};
            Transpose8x8U(src, dst);
        }
    }
    for(x = 0; x < sw; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        for(y = sh; y < h; y++)
            aux_buf[line0_off + y] = copy_begin[y * w + x];
    }
    for(x = sw; x < w; x++) {
        uint16 line0_off = GET_ITEMID_BY_IDX(buf, (real_order[x]))->lp_off;
        for(y = 0; y < h; y++)
            aux_buf[line0_off + y] = copy_begin[y * w + x];
    }

#else  /* none */
    DecompressDeconvertHeapRowsPart2(buf, aux_buf, real_order, 0, min_row_len, real_row_cnt);
#endif

}

static void DecompressDeconvertHeapRows(uint8 *buf, uint8 *aux_buf, int16 *real_order, uint16 min_row_len,
    uint16 max_row_len, uint16 real_row_cnt)
{
    errno_t ret;
    PageHeaderData *page = (PageHeaderData *)buf;

    uint16 min_offset = GET_ITEMID_BY_IDX(buf, real_order[0])->lp_off;
    uint16 total_size = page->pd_special - min_offset;
    uint8 *copy_begin = buf + min_offset;

    // step 1: transpose the matrix [0:real_row_cnt, 0:min_row_len]
    DecompressDeconvertHeapRowsPart1(buf, aux_buf, real_order, min_row_len, real_row_cnt);

    // step 2: transpose the matrix [0:real_row_cnt, min_row_len:max_row_len]
    if (min_row_len < max_row_len) {
        DecompressDeconvertHeapRowsPart2(buf, aux_buf, real_order, min_row_len, max_row_len, real_row_cnt);
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf + min_offset, total_size);
    securec_check(ret, "", "");
}

void DecompressDeconvertItemIds(char *buf, char *aux_buf) {
    errno_t ret;
    PageHeaderData *page = (PageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    uint32 total_size = row_cnt * sizeof(ItemIdData);
    char* copy_begin = buf + GetPageHeaderSize(page);
    uint16 i, k;

    k = 0;
    for (i = 0; i < row_cnt; i++) {
        aux_buf[k++] = copy_begin[0  * row_cnt + i];
        aux_buf[k++] = copy_begin[1  * row_cnt + i];
        aux_buf[k++] = copy_begin[2  * row_cnt + i];
        aux_buf[k++] = copy_begin[3  * row_cnt + i];
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

void DecompressDeconvertOnePage(char *buf, char *aux_buf, bool diff_convert) {
    uint16 max_row_len = 0;
    uint16 min_row_len = 0;
    int16 *real_order = NULL; // itemids are not in order sometimes. we must find the real
    uint16 real_row_cnt = 0;
    PageHeaderData *page = (PageHeaderData *)buf;

    // =======firstly, arrange the itemids.
    DecompressDeconvertItemIds(buf, aux_buf);
    if (diff_convert) {
        CompressDeDiffItemIds(buf, GetPageHeaderSize(buf),
            (page->pd_lower - GetPageHeaderSize(buf)) / sizeof(ItemIdData));
    }

    if (!CompressConvertCheck(buf, &real_order, &max_row_len, &min_row_len, &real_row_cnt)) {
        FreePointer((void*)real_order);
        ASSERT(0);
        return ;
    }

    // =======and last, the tuples
    if (page->pd_special < BLCKSZ) {
        // deconvert by order of itemid location in itemd-area
        DecompressDeconvertIndexKeys((uint8 *)buf, (uint8 *)aux_buf, real_order, min_row_len, max_row_len, real_row_cnt);
        if (diff_convert) {
            CompressDeDiffRows(buf, real_order, min_row_len, real_row_cnt, false);
        }
    } else {
        // convert by order of row location offset
        DecompressDeconvertHeapRows((uint8 *)buf, (uint8 *)aux_buf, real_order, min_row_len, max_row_len, real_row_cnt);
        if (diff_convert) {
            CompressDeDiffRows(buf, real_order, min_row_len, real_row_cnt, true);
        }
    }

    FreePointer((void*)real_order);
}

static void CompressDeDiffItemIds_U(char *buf, uint32 offset, uint16 real_row_cnt) {
    CompressDeDiffItemIds(buf, offset, real_row_cnt);
    return ;
}

static void CompressDeDiffTDs_U(char *buf, uint32 offset, uint16 min_row_len, uint16 real_row_cnt) {
    uint16 row_cnt;
    uint8 *copy_begin = (uint8 *)(buf + offset);

    int16 i;
    uint8 *prev, *curr;
    uint16 curr_off, prev_off;

    row_cnt = real_row_cnt;
    for (i = 1; i < row_cnt; i++) {
        curr_off = min_row_len * i;
        prev_off = min_row_len * (i - 1);

        curr = copy_begin + curr_off;
        prev = copy_begin + prev_off;

        CompressDeDiffCommRow(prev, curr, min_row_len);
    }

    return ;
}

static void CompressDeDiffRows_U(char *buf, int16 *real_order, uint16 min_row_len, uint16 real_row_cnt) {
    uint16 row_cnt;
    uint8 *copy_begin = (uint8 *)(buf);
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;

    int16 i;
    uint8 *prev, *curr;
    uint16 curr_off, prev_off;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);

    /* for table only */
    row_cnt = real_row_cnt;
    for (i = 1; i < row_cnt; i++) {
        curr_off = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[i])));
        prev_off = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[i - 1])));

        curr = copy_begin + curr_off;
        prev = copy_begin + prev_off;

        CompressDeDiffCommRow(prev, curr, min_row_len);
    }

    return;
}

void DecompressDeconvertHeapRows_U(char *buf, char *aux_buf, int16 *real_order, uint16 max_row_len, uint16 real_row_cnt) {
    errno_t ret;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 row_cnt = real_row_cnt;

    uint16 min_offset = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, real_order[0]));
    uint32 total_size = page->pd_special - min_offset;
    char *copy_begin = buf + min_offset;

    char *row;
    uint16 i, j, k, cur, up, row_size;

    for (i = 0, k = 0; i < max_row_len; i++) {
        for (j = 0; j < row_cnt; j++) {
            up = (j == (row_cnt - 1)) ? page->pd_special :
                RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[j + 1])));
            cur = RowPtrGetOffset(GET_UPAGE_ITEMID_BY_IDX(buf, uheap_header_len, (real_order[j])));
            row_size = up - cur;
            row = aux_buf + cur;
            if (i < row_size) {
                row[i] = copy_begin[k++];  // this part is reshaped
            }
        }
    }

    if (k != total_size) {
        printf("ERROR!!! pg_deconvert_rows error...!!!\n");
        ASSERT(0);
        return;
    }

    // copy aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf + min_offset, total_size);
    securec_check(ret, "", "");
    return ;
}

void DeCompressDeConvertTDs_U(char *buf, char *aux_buf) {
    errno_t ret;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 row_cnt = page->td_count;
    uint32 total_size = row_cnt * sizeof(TD);
    char* copy_begin = buf + SizeOfUHeapPageHeaderData;
    uint16 i, j, k;

    for (i = 0, k = 0; i < sizeof(TD); i++) {
        for (j = 0; j < row_cnt; j++) {
            aux_buf[j * sizeof(TD) + i] = copy_begin[k++];
        }
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

void DecompressDeconvertItemIds_U(char *buf, char *aux_buf) {
    errno_t ret;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);
    uint16 row_cnt = (page->pd_lower - uheap_header_len) / sizeof(RowPtr);
    uint32 total_size = row_cnt * sizeof(RowPtr);
    char* copy_begin = buf + uheap_header_len;
    uint16 i, j, k;

    for (i = 0, k = 0; i < sizeof(RowPtr); i++) {
        for (j = 0; j < row_cnt; j++) {
            aux_buf[j * sizeof(RowPtr) + i] = copy_begin[k++];
        }
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}

void DecompressDeconvertOnePage_U(char *buf, char *aux_buf, bool diff_convert) {
    uint16 max_row_len = 0;
    uint16 min_row_len = 0;
    int16 *real_order = NULL; // itemids are not in order sometimes. we must find the real
    uint16 real_row_cnt = 0;
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)buf;
    uint16 uheap_header_len = SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(page);

    // =======firstly, arrange the itemids.
    DecompressDeconvertItemIds_U(buf, aux_buf);
    if (diff_convert) {
        CompressDeDiffItemIds_U(buf, uheap_header_len,
            (page->pd_lower - uheap_header_len) / sizeof(ItemIdData));
    }

    if (!CompressConvertCheck_U(buf, &real_order, &max_row_len, &min_row_len, &real_row_cnt)) {
        FreePointer((void*)real_order);
        ASSERT(0);
        return ;
    }

    // =======last, arrange the tds and tuples.
    DeCompressDeConvertTDs_U(buf, aux_buf);
    if (diff_convert) {
        CompressDeDiffTDs_U(buf, SizeOfUHeapPageHeaderData, sizeof(TD), page->td_count);
    }

    if (page->pd_special < BLCKSZ) {
        FreePointer((void*)real_order);
        ASSERT(0);
        return ;
    } else {
        // convert by order of row location offset
        DecompressDeconvertHeapRows_U(buf, aux_buf, real_order, max_row_len, real_row_cnt);
        if (diff_convert) {
            CompressDeDiffRows_U(buf, real_order, min_row_len, real_row_cnt);
        }
    }

    FreePointer((void*)real_order);
}

void DecompressPageDeconvert(char *src, bool diff_convert, uint8 pagetype)
{
    errno_t rc = memset_s(aux_abuf, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "", "");

    // do convert
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        // for ustore page only
        DecompressDeconvertOnePage_U(src, aux_abuf, diff_convert);
    } else {
        // common-type page and heap page and segment page have same page-struct. so deal with the same procession.
        DecompressDeconvertOnePage(src, aux_abuf, diff_convert);
    }
}

/**
 * DecompressPage() -- Decompress one compressed page.
 *  return size of decompressed page which should be BLCKSZ or
 *         -1 for decompress error
 *         -2 for unrecognized compression algorithm
 *
 * 		note:The size of dst must be greater than or equal to BLCKSZ.
 */
template <uint8 pagetype>
int TemplateDecompressPage(const char* src, char* dst)
{
    int decompressed_size;
    char* data;
    uint32 size;
    uint32 crc32;
    int algorithm;
    bool byte_convert, diff_convert;
    size_t headerSize = GetSizeOfHeadData(pagetype);
    int rc = memcpy_s(dst, headerSize, src, headerSize);
    securec_check(rc, "", "");

    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        data = ((UHeapPageCompressData*)src)->data;
        size = ((UHeapPageCompressData*)src)->size;
        crc32 = ((UHeapPageCompressData*)src)->crc32;
        byte_convert = ((UHeapPageCompressData*)src)->byte_convert;
        diff_convert = ((UHeapPageCompressData*)src)->diff_convert;
        algorithm = ((UHeapPageCompressData*)src)->algorithm;
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        data = ((HeapPageCompressData*)src)->data;
        size = ((HeapPageCompressData*)src)->size;
        crc32 = ((HeapPageCompressData*)src)->crc32;
        byte_convert = ((HeapPageCompressData*)src)->byte_convert;
        diff_convert = ((HeapPageCompressData*)src)->diff_convert;
        algorithm = ((HeapPageCompressData*)src)->algorithm;
    } else {
        data = ((PageCompressData*)src)->data;
        size = ((PageCompressData*)src)->size;
        crc32 = ((PageCompressData*)src)->crc32;
        byte_convert = ((PageCompressData*)src)->byte_convert;
        diff_convert = ((PageCompressData*)src)->diff_convert;
        algorithm = ((PageCompressData*)src)->algorithm;
    }

    if (DataBlockChecksum(data, size, true) != crc32) {
        return -2;
    }

    switch (algorithm) {
        case COMPRESS_ALGORITHM_PGLZ:
            decompressed_size = pglz_decompress((const PGLZ_Header* )data, dst + headerSize);
            if (decompressed_size == -1) {
                return -1;
            }
            break;
        case COMPRESS_ALGORITHM_ZSTD:
#ifndef FRONTEND
            if (t_thrd.page_compression_cxt.zstd_dctx == NULL) {
                decompressed_size = ZSTD_decompress(dst + headerSize, BLCKSZ - headerSize, data, size);
            } else {
                decompressed_size = ZSTD_decompressDCtx((ZSTD_DCtx *)t_thrd.page_compression_cxt.zstd_dctx,
                    dst + headerSize, BLCKSZ - headerSize, data, size);
            }
#else
            decompressed_size = ZSTD_decompress(dst + headerSize, BLCKSZ - headerSize, data, size);
            if (ZSTD_isError(decompressed_size)) {
                return -1;
            }
#endif
            break;
        default:
            return COMPRESS_UNSUPPORTED_ERROR;
            break;
    }

    if (byte_convert) {
        DecompressPageDeconvert(dst, diff_convert, pagetype);
    }

    return headerSize + decompressed_size;
}

bool IsCompressedFile(const char *fileName, size_t fileNameLen)
{
    size_t suffixLen = strlen(COMPRESS_STR);
    if (fileNameLen >= suffixLen) {
        const char *suffix = fileName + fileNameLen - suffixLen;
        if (strncmp(suffix, COMPRESS_STR, suffixLen) == 0) {
            return true;
        }
    }
    return false;
}

#endif
