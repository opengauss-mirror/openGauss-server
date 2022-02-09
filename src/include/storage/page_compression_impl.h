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

#include "storage/page_compression.h"
#include "storage/checksum_impl.h"
#include "utils/pg_lzcompress.h"

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


/**
 * return data of page
 * @param dst HeapPageCompressData or HeapPageCompressData
 * @param heapPageData heapPageData or pagedata
 * @return dst->data
 */
static inline char* GetPageCompressedData(char* dst, bool heapPageData)
{
    return heapPageData ? ((HeapPageCompressData*)dst)->data : ((PageCompressData*)dst)->data;
}

static inline void FreePointer(void* pointer)
{
    if (pointer != NULL) {
        pfree(pointer);
    }
}

/*======================================================================================*/
#define COMPRESS ""
void cprs_diff_convert_rows(char *buf, uint32 offset,uint16 min_row_len, uint16 real_row_cnt) {
    uint16 row_cnt = real_row_cnt;
    uint32 common_size = min_row_len;
    uint8 *copy_begin = (uint8 *)(buf + offset);
    uint16 i, j;

    for (i = 0; i < common_size; i++) {
        for (j = row_cnt - 1; j > 0; j--) {
            copy_begin[i  * row_cnt + j] -= copy_begin[i  * row_cnt + (j - 1)];
        }
    }
    return ;
}

void cprs_diff_deconvert_rows(char *buf, uint32 offset, uint16 min_row_len, uint16 real_row_cnt) {
    uint16 row_cnt = real_row_cnt;
    uint32 common_size = min_row_len;
    uint8 *copy_begin = (uint8 *)(buf + offset);
    uint16 i, j;

    for (i = 0; i < common_size; i++) {
        for (j = 1; j < row_cnt; j++) {
            copy_begin[i  * row_cnt + j] += copy_begin[i  * row_cnt + (j - 1)];
        }
    }
    return ;
}

void CompressConvertItemIds(char *buf, char *aux_buf) {
    errno_t ret;
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    uint32 total_size = row_cnt * sizeof(ItemIdData);
    char *copy_begin = buf + GetPageHeaderSize(page);
    uint16 i, j, k;

    // clear aux_buf
    ret = memset_sp(aux_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "", "");

    k = 0;
    for (i = 0; i < row_cnt; i++) {
        for (j = 0; j < sizeof(ItemIdData); j++) {
            aux_buf[j  * row_cnt + i] = copy_begin[k++];
        }
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf, total_size);
    securec_check(ret, "", "");
    return ;
}


void CompressConvertRows(char *buf, char *aux_buf, int16 *real_order, uint16 max_row_len, uint16 real_row_cnt) {
    errno_t ret;
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
    uint16 row_cnt = real_row_cnt;
    uint32 total_size = page->pd_special - page->pd_upper;
    char *copy_begin = buf + page->pd_upper;
    char *row;
    uint16 i, j, k, cur, up, row_size;

    ret = memset_sp(aux_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "", "");

    k = 0;
    for (i = 0; i < max_row_len; i++) {
        for (j = 0; j < row_cnt; j++) {
            up = (j == (row_cnt - 1)) ? page->pd_special : GET_ITEMID_BY_IDX(buf, (real_order[j + 1]))->lp_off;
            cur = GET_ITEMID_BY_IDX(buf, (real_order[j]))->lp_off;
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

// 1: as tuple_offset order, that means asc order.
// 2: store all itemid's idx.
// 3:maybe some itemid is not in order.
void CompressConvertItemRealOrder(char *buf, int16 *real_order, uint16 real_row_cnt) {
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    ItemIdData *begin = (ItemIdData *)(buf + GetPageHeaderSize(page));
    int16 *link_order = real_order + real_row_cnt;

    int16 i, head, curr, prev;
    int16 end = -1; // invalid index

    head = end;
    // very likely to seems that itemids stored by desc order, and ignore invalid itemid
    for (i = 0; i < row_cnt; i++) {
        if (!ItemIdIsNormal(begin + i)) {
            continue;
        }

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
        curr = link_order[curr];
    }

    if (curr != end) {
        printf("ERROR!!! pre_convert_real_order error...!!!\n");
        ASSERT(0);
        return;
    }

}

// maybe some itemid is not valid
uint16 HeapPageCalcRealRowCnt (char *buf) {
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
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
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
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
            --------------------------|--------------------------
            xxxxxxxxxxxxxxxxxxxxxxxxxx|xxxxxxxxxxxxxxxxxxxxxxxxxx
            --------------------------|--------------------------
    */
    // the first part is real array order, and the second part is link.
    *real_order = (int16 *)palloc(sizeof(uint16) * row_cnt * 2);
    if (*real_order == NULL) {
        printf("zfunc compress file");
        return false;
    }
    ret = memset_sp(*real_order, sizeof(uint16) * row_cnt * 2, 0, sizeof(uint16) * row_cnt * 2);
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
    if (!CompressConvertCheck(buf, &real_order, &max_row_len, &min_row_len, &real_row_cnt)) {
        FreePointer((void*)real_order);
        return false;
    }

    CompressConvertRows(buf, aux_buf, real_order, max_row_len, real_row_cnt);
    CompressConvertItemIds(buf, aux_buf);

    if (diff_convert) {
        cprs_diff_convert_rows(buf, ((HeapPageHeaderData *)buf)->pd_upper, min_row_len, real_row_cnt);
        cprs_diff_convert_rows(buf, GetPageHeaderSize(buf), sizeof(ItemIdData), 
            (((HeapPageHeaderData *)buf)->pd_lower - GetPageHeaderSize(buf)) / sizeof(ItemIdData));
    }

    FreePointer((void*)real_order);
    return true;
}

void CompressPagePrepareConvert(char *src, bool diff_convert, bool *real_ByteConvert)
{
    char *aux_buf = NULL;
    errno_t rc;

    aux_buf = (char *)palloc(BLCKSZ);
    if (aux_buf == NULL) {
        // add log
        return;
    }
    rc = memset_sp(aux_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "", "");

    // do convert
    *real_ByteConvert = false;
    if (CompressConvertOnePage(src, aux_buf, diff_convert)) {
        *real_ByteConvert = true;
    }

    FreePointer((void*)aux_buf);
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
            return BLCKSZ + 4;
        case COMPRESS_ALGORITHM_ZSTD:
            return ZSTD_compressBound(BLCKSZ - CompressReservedLen(page));
        default:
            return -1;
    }
}

int CompressPage(const char* src, char* dst, int dst_size, RelFileCompressOption option)
{
    if (PageIs8BXidHeapVersion(src)) {
        return TemplateCompressPage<true>(src, dst, dst_size, option);
    } else {
        return TemplateCompressPage<false>(src, dst, dst_size, option);
    }
}

int DecompressPage(const char* src, char* dst, uint8 algorithm)
{
    if (PageIs8BXidHeapVersion(src)) {
        return TemplateDecompressPage<true>(src, dst, algorithm);
    } else {
        return TemplateDecompressPage<false>(src, dst, algorithm);
    }
}

inline size_t GetSizeOfHeadData(bool heapPageData)
{
    if (heapPageData) {
        return SizeOfHeapPageHeaderData;
    } else {
        return SizeOfPageHeaderData;
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
template <bool heapPageData>
int TemplateCompressPage(const char* src, char* dst, int dst_size, RelFileCompressOption option)
{
    int compressed_size;
    int8 level = option.compressLevelSymbol ? option.compressLevel : -option.compressLevel;
    size_t sizeOfHeaderData = GetSizeOfHeadData(heapPageData);
    char* src_copy = NULL;
    bool real_ByteConvert = false;
    errno_t rc;

    if (option.byteConvert) {
        // copy and maybe change it
        src_copy = (char*)palloc(BLCKSZ);
        if (src_copy == NULL) {
            // add log
            return -1;
        }
        rc = memcpy_s(src_copy, BLCKSZ, src, BLCKSZ);
        securec_check(rc, "", "");
        CompressPagePrepareConvert(src_copy, option.diffConvert, &real_ByteConvert); /* preprocess convert src */
    }

    char* data = GetPageCompressedData(dst, heapPageData);

    switch (option.compressAlgorithm) {
        case COMPRESS_ALGORITHM_PGLZ: {
            bool success;
            if (real_ByteConvert) {
                success = pglz_compress(src_copy + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, (PGLZ_Header *)data,
                    PGLZ_strategy_default);
            } else {
                success = pglz_compress(src + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, (PGLZ_Header *)data,
                    PGLZ_strategy_default);
            }
            compressed_size = success ? VARSIZE(data) : BLCKSZ;
            compressed_size = compressed_size < BLCKSZ ? compressed_size : BLCKSZ;
            break;
        }
        case COMPRESS_ALGORITHM_ZSTD: {
            if (level == 0 || level < MIN_ZSTD_COMPRESSION_LEVEL || level > MAX_ZSTD_COMPRESSION_LEVEL) {
                level = DEFAULT_ZSTD_COMPRESSION_LEVEL;
            }

            if (real_ByteConvert) {
                compressed_size =
                    ZSTD_compress(data, dst_size, src_copy + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, level);
            } else {
                compressed_size =
                    ZSTD_compress(data, dst_size, src + sizeOfHeaderData, BLCKSZ - sizeOfHeaderData, level);
            }

            if (ZSTD_isError(compressed_size)) {
                FreePointer((void*)src_copy);
                return -1;
            }
            break;
        }
        default:
            FreePointer((void*)src_copy);
            return COMPRESS_UNSUPPORTED_ERROR;
    }

    if (compressed_size < 0) {
        FreePointer((void*)src_copy);
        return -1;
    }

    if (heapPageData) {
        HeapPageCompressData* pcdptr = ((HeapPageCompressData*)dst);
        rc = memcpy_s(pcdptr->page_header, sizeOfHeaderData, src, sizeOfHeaderData);
        securec_check(rc, "", "");
        pcdptr->size = compressed_size;
        pcdptr->crc32 = DataBlockChecksum(data, compressed_size, true);
        pcdptr->byte_convert = real_ByteConvert;
        pcdptr->diff_convert = option.diffConvert;
    } else {
        PageCompressData* pcdptr = ((PageCompressData*)dst);
        rc = memcpy_s(pcdptr->page_header, sizeOfHeaderData, src, sizeOfHeaderData);
        securec_check(rc, "", "");
        pcdptr->size = compressed_size;
        pcdptr->crc32 = DataBlockChecksum(data, compressed_size, true);
        pcdptr->byte_convert = real_ByteConvert;
        pcdptr->diff_convert = option.diffConvert;
    }

    FreePointer((void*)src_copy);
    return SIZE_OF_PAGE_COMPRESS_DATA_HEADER_DATA(heapPageData) + compressed_size;
}

/*======================================================================================*/
#define DECOMPRESS ""
void DecompressDeconvertRows(char *buf, char *aux_buf, int16 *real_order, uint16 max_row_len, uint16 real_row_cnt) {
    errno_t ret;
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
    uint16 row_cnt = real_row_cnt;
    uint32 total_size = page->pd_special - page->pd_upper;
    char *copy_begin = buf + page->pd_upper;
    char *row;
    uint16 i, j, k, cur, up, row_size;

    ret = memset_sp(aux_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "", "");

    for (i = 0, k = 0; i < max_row_len; i++) {
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

    if (k != total_size) {
        printf("ERROR!!! pg_deconvert_rows error...!!!\n");
        ASSERT(0);
        return;
    }

    // cp aux_buf to page_buf
    ret = memcpy_sp(copy_begin, total_size, aux_buf + page->pd_upper, total_size);
    securec_check(ret, "", "");
    return ;
}

void DecompressDeconvertItemIds(char *buf, char *aux_buf) {
    errno_t ret;
    HeapPageHeaderData *page = (HeapPageHeaderData *)buf;
    uint16 row_cnt = (page->pd_lower - GetPageHeaderSize(page)) / sizeof(ItemIdData);
    uint32 total_size = row_cnt * sizeof(ItemIdData);
    char* copy_begin = buf + GetPageHeaderSize(page);
    uint16 i, j, k;

    // clear aux_buf
    ret = memset_sp(aux_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "", "");

    for (i = 0, k = 0; i < sizeof(ItemIdData); i++) {
        for (j = 0; j < row_cnt; j++) {
            aux_buf[j * sizeof(ItemIdData) + i] = copy_begin[k++];
        }
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

    if (diff_convert) {
        cprs_diff_deconvert_rows(buf, GetPageHeaderSize(buf), sizeof(ItemIdData), 
            (((HeapPageHeaderData *)buf)->pd_lower - GetPageHeaderSize(buf)) / sizeof(ItemIdData));
    }

    // =======firstly, arrange the itemids.
    DecompressDeconvertItemIds(buf, aux_buf);

    if (!CompressConvertCheck(buf, &real_order, &max_row_len, &min_row_len, &real_row_cnt)) {
        FreePointer((void*)real_order);
        ASSERT(0);
        return ;
    }

    // =======and last, the tuples
    if (diff_convert) {
        cprs_diff_deconvert_rows(buf, ((HeapPageHeaderData *)buf)->pd_upper, min_row_len, real_row_cnt);
    }
    DecompressDeconvertRows(buf, aux_buf, real_order, max_row_len, real_row_cnt);
    FreePointer((void*)real_order);
}

void DecompressPageDeconvert(char *src, bool diff_convert)
{
    char *aux_buf = NULL;
    errno_t rc;

    aux_buf = (char *)palloc(BLCKSZ);
    if (aux_buf == NULL) {
        // add log
        return;
    }
    rc = memset_s(aux_buf, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "", "");

    // do convert
    DecompressDeconvertOnePage(src, aux_buf, diff_convert);

    FreePointer((void*)aux_buf);
}

/**
 * DecompressPage() -- Decompress one compressed page.
 *  return size of decompressed page which should be BLCKSZ or
 *         -1 for decompress error
 *         -2 for unrecognized compression algorithm
 *
 * 		note:The size of dst must be greater than or equal to BLCKSZ.
 */
template <bool heapPageData>
int TemplateDecompressPage(const char* src, char* dst, uint8 algorithm)
{
    int decompressed_size;
    char* data;
    uint32 size;
    uint32 crc32;
    bool byte_convert, diff_convert;
    size_t headerSize = GetSizeOfHeadData(heapPageData);
    int rc = memcpy_s(dst, headerSize, src, headerSize);
    securec_check(rc, "", "");

    if (heapPageData) {
        data = ((HeapPageCompressData*)src)->data;
        size = ((HeapPageCompressData*)src)->size;
        crc32 = ((HeapPageCompressData*)src)->crc32;
        byte_convert = ((HeapPageCompressData*)src)->byte_convert;
        diff_convert = ((HeapPageCompressData*)src)->diff_convert;
    } else {
        data = ((PageCompressData*)src)->data;
        size = ((PageCompressData*)src)->size;
        crc32 = ((PageCompressData*)src)->crc32;
        byte_convert = ((PageCompressData*)src)->byte_convert;
        diff_convert = ((PageCompressData*)src)->diff_convert;
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
            decompressed_size = ZSTD_decompress(dst + headerSize, BLCKSZ - headerSize, data, size);
            if (ZSTD_isError(decompressed_size)) {
                return -1;
            }
            break;
        default:
            return COMPRESS_UNSUPPORTED_ERROR;
            break;
    }

    if (byte_convert) {
        DecompressPageDeconvert(dst, diff_convert);
    }

    return headerSize + decompressed_size;
}

/**
 * pc_mmap() -- create memory map for page compress file's address area.
 *
 */
PageCompressHeader* pc_mmap(int fd, int chunk_size, bool readonly)
{
    int pc_memory_map_size = SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunk_size);
    return pc_mmap_real_size(fd, pc_memory_map_size, readonly);
}

/**
 * pc_mmap_real_size() -- create memory map for page compress file's address area.
 *
 */
extern PageCompressHeader* pc_mmap_real_size(int fd, int pc_memory_map_size, bool readonly)
{
    PageCompressHeader* map = NULL;
    int fileSize = lseek(fd, 0, SEEK_END);
    if (fileSize != pc_memory_map_size) {
        if (ftruncate(fd, pc_memory_map_size) != 0) {
            return (PageCompressHeader*) MAP_FAILED;
        }
    }
    if (readonly) {
        map = (PageCompressHeader*) mmap(NULL, pc_memory_map_size, PROT_READ, MAP_SHARED, fd, 0);
    } else {
        map = (PageCompressHeader*) mmap(NULL, pc_memory_map_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
    }
    return map;
}

/**
 * pc_munmap() -- release memory map of page compress file.
 *
 */
int pc_munmap(PageCompressHeader *map)
{
    return munmap(map, SIZE_OF_PAGE_COMPRESS_ADDR_FILE(map->chunk_size));
}

/**
 * pc_msync() -- sync memory map of page compress file.
 *
 */
int pc_msync(PageCompressHeader *map)
{
    return msync(map, SIZE_OF_PAGE_COMPRESS_ADDR_FILE(map->chunk_size), MS_SYNC);
}


uint32 AddrChecksum32(BlockNumber blockNumber, const PageCompressAddr* pageCompressAddr, uint16 chunkSize)
{
#define UINT_LEN sizeof(uint32)
    uint32 checkSum = 0;
    char* addr = ((char*) pageCompressAddr) + UINT_LEN;
    size_t len = SIZE_OF_PAGE_COMPRESS_ADDR(chunkSize) - UINT_LEN;
    do {
        if (len >= UINT_LEN) {
            checkSum += *((uint32*) addr);
            addr += UINT_LEN;
            len -= UINT_LEN;
        } else {
            char finalNum[UINT_LEN] = {0};
            size_t i = 0;
            for (; i < len; ++i) {
                finalNum[i] = addr[i];
            }
            checkSum += *((uint32*) finalNum);
            len -= i;
        }
    } while (len);
    return checkSum;
}

CompressedFileType IsCompressedFile(char *fileName, size_t fileNameLen)
{
    size_t suffixLen = 4;
    if (fileNameLen >= suffixLen) {
        const char *suffix = fileName + fileNameLen - suffixLen;
        if (strncmp(suffix, "_pca", suffixLen) == 0) {
            return COMPRESSED_TABLE_PCA_FILE;
        } else if (strncmp(suffix, "_pcd", suffixLen) == 0) {
            return COMPRESSED_TABLE_PCD_FILE;
        }
    }
    return COMPRESSED_TYPE_UNKNOWN;
}

#endif
