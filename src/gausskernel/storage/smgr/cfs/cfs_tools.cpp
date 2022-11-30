/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * -------------------------------------------------------------------------
 */

#include "storage/page_compression.h"

#include "storage/cfs/cfs_converter.h"
#include "storage/cfs/cfs_tools.h"
#include "storage/checksum_impl.h"
#include "access/ustore/knl_upage.h"
#include "unistd.h"

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

bool CompressedChecksum(const char *compressedData)
{
    char *data = NULL;
    size_t dataLen;
    uint32 crc32;
    uint8 pagetype = PageGetPageLayoutVersion(compressedData);
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        UHeapPageCompressData *heapPageData = (UHeapPageCompressData *)(void *)compressedData;
        data = heapPageData->data;
        dataLen = heapPageData->size;
        crc32 = heapPageData->crc32;
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        HeapPageCompressData *heapPageData = (HeapPageCompressData *)(void *)compressedData;
        data = heapPageData->data;
        dataLen = heapPageData->size;
        crc32 = heapPageData->crc32;
    } else if (pagetype == PG_COMM_PAGE_LAYOUT_VERSION) {
        PageCompressData *heapPageData = (PageCompressData *)(void *)compressedData;
        data = heapPageData->data;
        dataLen = heapPageData->size;
        crc32 = heapPageData->crc32;
    } else {
        return false;
    }
    if (DataBlockChecksum(data, (uint32)dataLen, true) == crc32) {
        return true;
    }
    return false;
}

size_t CfsReadCompressedPage(char *dst, size_t destLen, BlockNumber extent_offset_blkno, CfsReadStruct *cfsReadStruct,
    BlockNumber blockNum)
{
    CfsExtentHeader *cfsExtentHeader = cfsReadStruct->header;
    auto fd = cfsReadStruct->fd;
    if (extent_offset_blkno >= cfsExtentHeader->nblocks) {
        return COMPRESS_BLOCK_ERROR;
    }
    decltype(CfsExtentHeader::chunk_size) chunkSize = cfsExtentHeader->chunk_size;
    auto extentStart = (cfsReadStruct->extentCount * CFS_EXTENT_SIZE) * BLCKSZ;
    uint8 nchunks;
    size_t tryCount = 0;
    do {
        CfsExtentAddress *cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)extent_offset_blkno);
        nchunks = cfsExtentAddress->nchunks;

        for (uint8 i = 0; i < nchunks; i++) {
            off_t seekPos = OffsetOfPageCompressChunk(chunkSize, cfsExtentAddress->chunknos[i]) + extentStart;
            uint8 start = i;
            while (i < cfsExtentAddress->nchunks - 1 &&
                   cfsExtentAddress->chunknos[i + 1] == cfsExtentAddress->chunknos[i] + 1) {
                i++;
            }
            size_t readAmount = (chunkSize * ((i - start) + 1));
            if (fseeko(fd, seekPos, SEEK_SET) != 0) {
                return COMPRESS_FSEEK_ERROR;
            }
            if (fread(dst + (long)start * chunkSize, 1, readAmount, fd) != readAmount && ferror(fd)) {
                return COMPRESS_FREAD_ERROR;
            }
        }
        if (nchunks * chunkSize == BLCKSZ) {  // non-compressed page
            if (InvalidBlockNumber == blockNum) {  // no need to check it, as we do not know the real blockNum.
                break;
            }
            uint16 checksum = pg_checksum_page((char *)dst, (BlockNumber)blockNum);
            if (checksum == (PageHeader(dst))->pd_checksum) {
                break;
            }
        } else {
            if (nchunks == 0 || CompressedChecksum(dst)) {
                break;
            }
        }
        if (tryCount < MAX_RETRY_LIMIT) {
            ++tryCount;
            pg_usleep(RETRY_SLEEP_TIME);
        } else {
            return COMPRESS_CHECKSUM_ERROR;
        }
    } while (true);

    return nchunks * chunkSize;
}

const size_t CUR_PAGE_SIZE = (uint32)getpagesize();

CfsHeaderMap MMapHeader(FILE *fd, BlockNumber extentIndex, bool readOnly)
{
    extentIndex = extentIndex % CFS_EXTENT_COUNT_PER_FILE;
    auto extentOffset = ((extentIndex + 1) * CFS_EXTENT_SIZE - 1) * BLCKSZ;
    int extentOffsetToPageSize = (int)(extentOffset % CUR_PAGE_SIZE);
    auto realOffset = extentOffset;
    if (extentOffsetToPageSize != 0) {
        realOffset = realOffset - (uint32)extentOffsetToPageSize;
    }

    int offssetLen = (int)(extentOffset + BLCKSZ - realOffset);
    size_t realLen = CUR_PAGE_SIZE * ((unsigned int)offssetLen / CUR_PAGE_SIZE +
                                      (((unsigned int)offssetLen % CUR_PAGE_SIZE != 0) ? 1 : 0));
    void *freePointer = mmap(nullptr, realLen,
        readOnly ? PROT_READ : (PROT_WRITE | PROT_READ), MAP_SHARED, fileno(fd), realOffset);
    return {.header = (CfsExtentHeader *)(void *)((char *) freePointer + extentOffsetToPageSize),
            .extentCount = extentIndex, .pointer = freePointer, .mmapLen = realLen
    };
}

void MmapFree(CfsHeaderMap *cfsHeaderMap)
{
    if (cfsHeaderMap->pointer != nullptr) {
        (void)munmap(cfsHeaderMap->pointer, cfsHeaderMap->mmapLen);
        cfsHeaderMap->pointer = nullptr;
    }
}

uint32 AddrChecksum32(const CfsExtentAddress *cfsExtentAddress, const int needChunks)
{
    constexpr size_t uintLen = sizeof(uint32);
    uint32 checkSum = 0;
    char *addr = ((char *) cfsExtentAddress) + sizeof(uint32);
    size_t len = SizeOfExtentAddressByChunks((uint8)needChunks) - sizeof(uint32);
    do {
        if (len >= uintLen) {
            checkSum += *((uint32 *)(void *)addr);
            addr += uintLen;
            len -= uintLen;
        } else {
            char finalNum[uintLen] = {0};
            size_t i = 0;
            for (; i < len; ++i) {
                finalNum[i] = addr[i];
            }
            checkSum += *((uint32 *) finalNum);
            len -= i;
        }
    } while (len);
    return checkSum;
}

template <typename T>
bool ReadCompressedInfo(T &t, off_t offset, FILE *file)
{
    if (fseeko(file, offset, SEEK_SET) != 0) {
        return false;
    }
    if (fread((void *)(&t), sizeof(t), 1, file) <= 0) {
        return false;
    }
    return true;
}

size_t ReadBlockNumberOfCFile(FILE* compressFd, off_t *curFileLen)
{
    if (fseek(compressFd, 0L, SEEK_END) < 0) {
        return COMPRESS_FSEEK_ERROR;
    }
    /* read file size of toFullPath */
    off_t fileLen = ftell(compressFd);
    *curFileLen = fileLen;
    if (fileLen == 0) {
        return 0;
    } else {
        BlockNumber fileBlockNum = (BlockNumber) fileLen / BLCKSZ;
        BlockNumber extentCount = fileBlockNum / CFS_EXTENT_SIZE;
        BlockNumber result = (extentCount - 1) * (CFS_EXTENT_SIZE - 1);

        /* read header of last extent */
        off_t offset = (off_t)offsetof(CfsExtentHeader, nblocks);

        BlockNumber blockNumber;
        if (ReadCompressedInfo(blockNumber, (fileLen - BLCKSZ) + offset, compressFd)) {
            return result + blockNumber;
        }
        return COMPRESS_FREAD_ERROR;
    }
}