/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "storage/cfs/cfs_converter.h"
#include "PageCompression.h"
#include "utils/pg_lzcompress.h"
#include "storage/page_compression_impl.h"

#include <memory>

#define PC_CUSTOM_VALUE_LARGESIZE 4096

/**
 * write RewindCompressInfo
 * @param file file fp
 * @param pcaFilePath file path,for ereport
 * @param rewindCompressInfo pointer of return
 * @return sucesss or not
 */
static bool ReadRewindCompressedInfo(FILE *file, RewindCompressInfo *rewindCompressInfo)
{
    off_t curFileLen = 0;
    auto result = ReadBlockNumberOfCFile(file, &curFileLen);
    if (result >= MIN_COMPRESS_ERROR_RT) {
        return false;
    }
    rewindCompressInfo->compressed = true;
    rewindCompressInfo->oldBlockNumber = (BlockNumber)result;
    return true;
}

bool FetchSourcePca(unsigned char *header, size_t len, RewindCompressInfo *rewindCompressInfo, size_t fileSize)
{
    CfsExtentHeader *ptr = (CfsExtentHeader *)(void *)header;
    rewindCompressInfo->compressed = false;
    if (len != sizeof(CfsExtentHeader)) {
        return rewindCompressInfo->compressed;
    }

    rewindCompressInfo->compressed = true;
    if (fileSize == 0) {
        rewindCompressInfo->newBlockNumber = 0;
        rewindCompressInfo->oldBlockNumber = 0;
    } else {
        BlockNumber fileBlockNum = (BlockNumber) fileSize / BLCKSZ;
        BlockNumber extentCount = fileBlockNum / CFS_EXTENT_SIZE;
        BlockNumber result = (extentCount - 1) * (CFS_EXTENT_SIZE - 1);
        rewindCompressInfo->newBlockNumber = result + ptr->nblocks;
        rewindCompressInfo->oldBlockNumber = 0;
    }

    return rewindCompressInfo->compressed;
}

bool ProcessLocalPca(const char *tablePath, RewindCompressInfo *rewindCompressInfo, const char *prefix)
{
    if (!PageCompression::SkipCompressedFile(tablePath, strlen(tablePath))) {
        return false;
    }
    rewindCompressInfo->compressed = false;
    char dst[MAXPGPATH];
    errno_t rc = snprintf_s(dst, MAXPGPATH, MAXPGPATH - 1, "%s/%s", prefix, tablePath);
    securec_check_ss_c(rc, "\0", "\0");

    FILE *file = fopen(dst, "rb");
    if (file == NULL) {
        return false;
    }
    bool success = ReadRewindCompressedInfo(file, rewindCompressInfo);
    (void)fclose(file);
    return success;
}

BlockNumber PageCompression::GetSegmentNo() const
{
    return this->segmentNo;
}

size_t PageCompression::ReadCompressedBuffer(BlockNumber blockNum, char *buffer, size_t bufferLen, bool zeroAlign)
{
    CfsExtentHeader *header = GetStruct(blockNum);
    BlockNumber globalBlockNumber = (this->segmentNo * CFS_LOGIC_BLOCKS_PER_FILE) + blockNum;
    CfsReadStruct cfsReadStruct {
        .fd = this->fd,
        .header = header,
        .extentCount =  blockNum / CFS_LOGIC_BLOCKS_PER_EXTENT
    };
    size_t actualSize = CfsReadCompressedPage(buffer, bufferLen,
        blockNum % CFS_LOGIC_BLOCKS_PER_EXTENT, &cfsReadStruct, globalBlockNumber);
    /* valid check */
    if (actualSize > MIN_COMPRESS_ERROR_RT) {
        return actualSize;
    }

    if (zeroAlign && actualSize != bufferLen) {
        error_t rc = memset_s(buffer + actualSize, bufferLen - actualSize, 0, bufferLen - actualSize);
        securec_check(rc, "\0", "\0");
        actualSize = bufferLen;
    }
    return actualSize;
}

CfsExtentHeader* PageCompression::GetHeaderByExtentNumber(BlockNumber extentCount, CfsCompressOption *option)
{
    if (this->cfsHeaderMap.extentCount != extentCount ||
        this->cfsHeaderMap.header == nullptr) {
        MmapFree(&(cfsHeaderMap));
        bool needExtend = (this->extentIdx == InvalidBlockNumber || extentCount > this->extentIdx);
        /* need extend one extent */
        if (needExtend) {
            if (!option) {
                return nullptr;
            }
            auto extentOffset = ((extentCount + 1) * CFS_EXTENT_SIZE - 1) * BLCKSZ;
            if (fallocate(fileno(fd), 0, extentOffset, BLCKSZ) < 0) {
                return nullptr;
            }
            auto extentStart = extentCount * CFS_EXTENT_SIZE * BLCKSZ;
            auto allocateSize = CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ;
            if (fallocate(fileno(fd), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, extentStart, allocateSize) < 0) {
                return nullptr;
            }
            this->extentIdx = extentCount;
        }
        auto curHeader = MMapHeader(this->fd, extentCount);
        if (curHeader.header == MAP_FAILED) {
            return nullptr;
        }
        if (needExtend) {
            curHeader.header->chunk_size = option->chunk_size;
            curHeader.header->algorithm = option->algorithm;
        }

        this->cfsHeaderMap.header = curHeader.header;
        this->cfsHeaderMap.extentCount = curHeader.extentCount;
        this->cfsHeaderMap.pointer = curHeader.pointer;
        this->cfsHeaderMap.mmapLen = curHeader.mmapLen;
    }
    return this->cfsHeaderMap.header;
}

CfsExtentHeader* PageCompression::GetStruct(BlockNumber blockNum, CfsCompressOption *option)
{
    return GetHeaderByExtentNumber(blockNum / CFS_LOGIC_BLOCKS_PER_EXTENT, option);
}

PageCompression::~PageCompression()
{
    if (this->fd) {
        (void)fclose(this->fd);
    }

    if (this->cfsHeaderMap.pointer != nullptr) {
        (void)munmap(this->cfsHeaderMap.pointer, this->cfsHeaderMap.mmapLen);
        this->cfsHeaderMap.pointer = nullptr;
    }
}

const char *PageCompression::GetInitPath() const
{
    return this->initPath;
}

COMPRESS_ERROR_STATE PageCompression::Init(const char *filePath, BlockNumber inSegmentNo, bool create)
{
    if(!IsCompressedFile(filePath, strlen(filePath))){
        return PCA_OPEN_ERROR;
    }

    auto file = fopen(filePath, create ? "wb+" : "rb+");
    if (file == nullptr) {
        return PCA_OPEN_ERROR;
    }

    this->segmentNo = inSegmentNo;
    this->fd = file;
    this->blockNumber = 0;
    this->algorithm = 0;
    this->chunkSize = 0;
    this->extentIdx = InvalidBlockNumber;

    errno_t rc = memset_s(&this->cfsHeaderMap, sizeof(CfsHeaderMap), 0, sizeof(CfsHeaderMap));
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(initPath, MAXPGPATH, MAXPGPATH - 1, "%s", filePath);
    securec_check_ss_c(rc, "\0", "\0");

    if (create) {
        return SUCCESS;
    }

    /* read file size of toFullPath */
    if (fseek(this->fd, 0L, SEEK_END) < 0) {
        (void)fclose(file);
        return PCA_SEEK_ERROR;
    }
    off_t fileLen = ftell(this->fd);
    if (fileLen % CFS_EXTENT_SIZE != 0) {
        (void)fclose(file);
        return NORMAL_READ_ERROR;
    }
    if (fileLen > 0) {
        BlockNumber fileBlockNum = (BlockNumber) fileLen / BLCKSZ;
        BlockNumber extentCount = fileBlockNum / CFS_EXTENT_SIZE;
        BlockNumber result = (extentCount - 1) * (CFS_EXTENT_SIZE - 1);

        this->extentIdx = extentCount - 1;

        /* read header of last extent */
        auto header = this->GetHeaderByExtentNumber(extentCount - 1);
        if (header == nullptr) {
            (void)fclose(file);
            return NORMAL_READ_ERROR;
        }
        this->blockNumber = result + header->nblocks;
        this->algorithm = header->algorithm;
        this->chunkSize = header->chunk_size;
    }
    return SUCCESS;
}

bool PageCompression::SkipCompressedFile(const char *fileName, size_t len)
{
    auto realSize = strlen(fileName);
    return IsCompressedFile(fileName, realSize < len ? realSize : len);
}

FILE *PageCompression::GetCompressionFile()
{
    return this->fd;
}

BlockNumber PageCompression::GetMaxBlockNumber() const
{
    return this->blockNumber;
}

bool PageCompression::WriteBufferToCurrentBlock(char *buf, BlockNumber blkNumber, int32 size,
                                                CfsCompressOption *option)
{
    CfsExtentHeader *cfsExtentHeader = this->GetStruct(blkNumber, option);
    if (cfsExtentHeader == nullptr) {
        return false;
    }
    uint16 chkSize = cfsExtentHeader->chunk_size;

    BlockNumber logicBlockNumber = blkNumber % CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentOffset = (blkNumber / CFS_LOGIC_BLOCKS_PER_EXTENT) % CFS_EXTENT_COUNT_PER_FILE;
    int needChunks = size / (int32)chkSize;
    if (logicBlockNumber >= cfsExtentHeader->nblocks) {
        cfsExtentHeader->nblocks = logicBlockNumber + 1;
    }

    CfsExtentAddress *cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)logicBlockNumber);

    /* allocate chunks */
    if (cfsExtentAddress->allocated_chunks < needChunks) {
        uint16 chunkno = (uint16)pg_atomic_fetch_add_u32(&cfsExtentHeader->allocated_chunks,
                                                         (uint32)needChunks - cfsExtentAddress->allocated_chunks);
        for (int i = cfsExtentAddress->allocated_chunks; i < needChunks; i++) {
            cfsExtentAddress->chunknos[i] = ++chunkno;
        }
        cfsExtentAddress->allocated_chunks = (uint8)needChunks;
    }
    cfsExtentAddress->nchunks = (uint8)needChunks;
    cfsExtentAddress->checksum = AddrChecksum32(cfsExtentAddress, cfsExtentAddress->allocated_chunks);

    for (int32 i = 0; i < needChunks; ++i) {
        char *buffer_pos = buf + (long)chkSize * i;
        off_t seekPos = OffsetOfPageCompressChunk(chkSize, cfsExtentAddress->chunknos[i]) +
                        extentOffset * CFS_EXTENT_SIZE * BLCKSZ;
        int32 start = i;
        /* merge continuous write */
        while (i < needChunks - 1 && cfsExtentAddress->chunknos[i + 1] == cfsExtentAddress->chunknos[i] + 1) {
            i++;
        }
        size_t write_amount = (size_t)(chkSize * ((i - start) + 1));

        if (fseek(this->fd, seekPos, SEEK_SET) < 0) {
            return false;
        }
        if (fwrite(buffer_pos, 1, write_amount, this->fd) != write_amount) {
            return false;
        }
    }
    /* set other data of pcAddr */
    return true;
}

/**
 * return chunk-aligned size of buffer
 * @param buffer compressed page buffer
 * @param chunkSize chunk size
 * @return return chunk-aligned size of buffer
 */
size_t CalRealWriteSize(char *buffer)
{
    PageHeader phdr = (PageHeader)buffer;
    /* blank page */
    if (PageIsNew(buffer)) {
        return BLCKSZ;
    }

    /* check the assignment made during backup */
    if ((phdr->pd_lower & COMP_ASIGNMENT) == 0) {
        return BLCKSZ;
    }

    size_t compressedBufferSize;
    uint8 pagetype = PageGetPageLayoutVersion(buffer);
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        UHeapPageCompressData *heapPageData = (UHeapPageCompressData *)(void *)buffer;
        compressedBufferSize = heapPageData->size + offsetof(UHeapPageCompressData, data);
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        HeapPageCompressData *heapPageData = (HeapPageCompressData *)(void *)buffer;
        compressedBufferSize = heapPageData->size + offsetof(HeapPageCompressData, data);
    } else {
        PageCompressData *heapPageData = (PageCompressData *)(void *)buffer;
        compressedBufferSize = heapPageData->size + offsetof(PageCompressData, data);
    }

    return compressedBufferSize;
}

inline void PunchHoleForCnmpExt(CfsExtentHeader *pcaHead, BlockNumber extNo, FILE *destFd)
{
    uint32 usedSize = pcaHead->allocated_chunks * pcaHead->chunk_size;
    usedSize = TYPEALIGN((PC_CUSTOM_VALUE_LARGESIZE), usedSize);
    off_t punchSize = (off_t)(CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ - usedSize);
    off_t offset = (off_t)(extNo * CFS_EXTENT_SIZE * BLCKSZ + usedSize);
    (void)fallocate(fileno(destFd), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, punchSize);
}

inline void InitCompPcaHead(CfsExtentHeader *pcaHead, CfsCompressOption *option)
{
    pcaHead->chunk_size = option->chunk_size;
    pcaHead->algorithm = option->algorithm;
    pcaHead->nblocks = 0;
    pcaHead->allocated_chunks = 0;
    pcaHead->recycleInOrder = 1;
}

inline void SetCompPageAddrInfo(CfsExtentAddress *destExtAddr, uint8 chunkNum, uint32 chunkStart)
{
    for (uint8 i = 0; i < chunkNum; i++) {
        destExtAddr->chunknos[i] = (uint16)(chunkStart + i);
    }

    destExtAddr->allocated_chunks = chunkNum;
    destExtAddr->nchunks = chunkNum;
    destExtAddr->checksum = AddrChecksum32(destExtAddr, chunkNum);
}

COMPRESS_ERROR_STATE TranferIntoCompFile(FILE *srcFile, CfsCompressOption *option, FILE *destFd)
{
    BlockNumber extNo = 0;
    BlockNumber blockNumber = 0;
    char buffer[BLCKSZ] = {0};

    /* read file size of toFullPath */
    if (fseek(srcFile, 0L, SEEK_END) < 0) {
        return NORMAL_SEEK_ERROR;
    }
    off_t size = ftell(srcFile);
    BlockNumber maxBlockNumber = (BlockNumber)(size / BLCKSZ);

    /* read file size of toFullPath */
    if (fseek(srcFile, 0L, 0) < 0) {
        return NORMAL_SEEK_ERROR;
    }

    char *unaligned_buf = (char *) palloc(CFS_EXTENT_SIZE * BLCKSZ + BLCKSZ);
    if (unaligned_buf == NULL) {
        return BUFFER_ALLOC_ERROR;
    }

    char *extBuf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    errno_t rc = memset_s(extBuf, CFS_EXTENT_SIZE * BLCKSZ, 0, CFS_EXTENT_SIZE * BLCKSZ);
    securec_check_ss_c(rc, "\0", "\0");

    CfsExtentHeader *pcaHead = (CfsExtentHeader *)(void *)(extBuf + CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ);
    InitCompPcaHead(pcaHead, option);

    /* read page by page */
    for (blockNumber = 0; blockNumber < maxBlockNumber; blockNumber++) {
        if (fread(buffer, 1, BLCKSZ, srcFile) != BLCKSZ) {
            pfree(unaligned_buf);
            return NORMAL_READ_ERROR;
        }

        pcaHead->nblocks++;
        size_t compSize = CalRealWriteSize(buffer);
        uint8 chunkNum = (uint8)((compSize + option->chunk_size - 1) / option->chunk_size);

        /* remove compressed flag to recover original page */
        PageHeader phdr = (PageHeader)buffer;
        phdr->pd_lower &= (COMP_ASIGNMENT - 1);

        CfsExtentAddress *destExtAddr = GetExtentAddress(pcaHead, (uint16)(blockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT));
        uint32 chunkStart = pcaHead->allocated_chunks + 1;
        pcaHead->allocated_chunks += (uint32)chunkNum;
        SetCompPageAddrInfo(destExtAddr, chunkNum, chunkStart);

        rc = memcpy_s(extBuf + ((chunkStart - 1) * option->chunk_size),
            compSize, buffer, compSize);
        securec_check_ss_c(rc, "\0", "\0");

        if ((blockNumber + 1) % CFS_LOGIC_BLOCKS_PER_EXTENT == 0) {
            /* an ext includes 128 pages, enough to write down */
            if (fwrite(extBuf, 1, CFS_EXTENT_SIZE * BLCKSZ, destFd) != CFS_EXTENT_SIZE * BLCKSZ) {
                pfree(unaligned_buf);
                return NORMAL_WRITE_ERROR;
            }

            /* punch hole */
            PunchHoleForCnmpExt(pcaHead, extNo, destFd);
            extNo++;

            /* init extbuf for next 128 pages */
            rc = memset_s(extBuf, CFS_EXTENT_SIZE * BLCKSZ, 0, CFS_EXTENT_SIZE * BLCKSZ);
            securec_check_ss_c(rc, "\0", "\0");
            InitCompPcaHead(pcaHead, option);
        }
    }

    /* write last ext, which is not full */
    if (blockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT != 0) {
        /* an ext includes 128 pages, enough to write down */
        if (fwrite(extBuf, 1, CFS_EXTENT_SIZE * BLCKSZ, destFd) != CFS_EXTENT_SIZE * BLCKSZ) {
            pfree(unaligned_buf);
            return NORMAL_WRITE_ERROR;
        }

        PunchHoleForCnmpExt(pcaHead, extNo, destFd);
    }

    pfree(unaligned_buf);
    return SUCCESS;
}

COMPRESS_ERROR_STATE ConstructCompressedFile(const char *toFullPath, uint16 chunkSize,
                                             uint8 algorithm)
{
    /* create tmp file to transfer file of toFullPath into the file with format of compression */
    char path[MAXPGPATH];
    errno_t rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s_tmp", toFullPath);
    securec_check_ss_c(rc, "\0", "\0");

    CfsCompressOption option = {
        .chunk_size = chunkSize,
        .algorithm = algorithm
    };

    /* create tmp file */
    FILE *destFd = fopen(path, "wb+");
    if (destFd == NULL) {
        return NORMAL_CREATE_ERROR;
    }

    FILE *srcFile = fopen(toFullPath, "rb+");
    if (srcFile == NULL) {
        (void)fclose(destFd);
        return NORMAL_OPEN_ERROR;
    }

    COMPRESS_ERROR_STATE ret = TranferIntoCompFile(srcFile, &option, destFd);
    if (fclose(srcFile) != 0 || fclose(destFd) != 0) {
        (void)fclose(destFd);
        (void)fclose(srcFile);
        return FILE_CLOSE_ERROR;
    }

    if (ret != SUCCESS) {
        return ret;
    }

    /* delete toFullPath file */
    if (unlink(toFullPath) != 0) {
        (void)fclose(destFd);
        (void)fclose(srcFile);
        return NORMAL_UNLINK_ERROR;
    }

    /* use path to take place of toFullPath */
    if (rename(path, toFullPath) != 0) {
        (void)fclose(destFd);
        (void)fclose(srcFile);
        return NORMAL_RENAME_ERROR;
    }

    return SUCCESS;
}

bool PageCompression::DecompressedPage(const char *src, char *dest) const
{
    if (DecompressPage(src, dest) == BLCKSZ) {
        return true;
    }
    return false;
}

bool PageCompression::IsIntegratedPage(const char *buffer, int segmentNo, BlockNumber blkNumber)
{
    if (PageIsNew(buffer) ||
        pg_checksum_page((char *)buffer,
                         (uint32)segmentNo * RELSEG_SIZE + blkNumber) == (PageHeader(buffer))->pd_checksum) {
        return true;
    }
    return false;
}

bool PageCompression::WriteBackUncompressedData(const char *compressed, size_t compressedLen, char *buffer, size_t size,
                                                BlockNumber blkNumber)
{
    /* if compressed page is uncompressed, write back directly */
    if (compressedLen == BLCKSZ) {
        return this->WriteBufferToCurrentBlock(buffer, blkNumber, (int32)size);
    }

    bool byteConvert;
    bool diffConvert;
    uint8 algorithm;
    uint8 pagetype = PageGetPageLayoutVersion(compressed);
    if (pagetype == PG_UHEAP_PAGE_LAYOUT_VERSION) {
        byteConvert = (bool)((const UHeapPageCompressData *)(void *)compressed)->byte_convert;
        diffConvert = (bool)((const UHeapPageCompressData *)(void *)compressed)->diff_convert;
        algorithm = ((const UHeapPageCompressData *)(void *)compressed)->algorithm;
    } else if (pagetype == PG_HEAP_PAGE_LAYOUT_VERSION) {
        byteConvert = (bool)((const HeapPageCompressData *)(void *)compressed)->byte_convert;
        diffConvert = (bool)((const HeapPageCompressData *)(void *)compressed)->diff_convert;
        algorithm = ((const HeapPageCompressData *)(void *)compressed)->algorithm;
    } else {
        byteConvert = (bool)((const PageCompressData *)(void *)compressed)->byte_convert;
        diffConvert = (bool)((const PageCompressData *)(void *)compressed)->diff_convert;
        algorithm = ((const PageCompressData *)(void *)compressed)->algorithm;
    }

    auto workBufferSize = CompressPageBufferBound(buffer, (uint8)algorithm);
    if (workBufferSize < 0) {
        return false;
    }
    char *workBuffer = (char *)malloc((uint32)workBufferSize);
    if (workBuffer == NULL) {
        fprintf(stderr, "failed to malloc work buffer\n");
        return false;
    }

    RelFileCompressOption relFileCompressOption;
    relFileCompressOption.compressPreallocChunks = 0;
    relFileCompressOption.compressLevelSymbol = (uint32)true;
    relFileCompressOption.compressLevel = 1;
    relFileCompressOption.compressAlgorithm = algorithm;
    relFileCompressOption.byteConvert = (uint32)byteConvert;
    relFileCompressOption.diffConvert = (uint32)diffConvert;

    auto compress_buffer_size = CompressPage(buffer, workBuffer, workBufferSize, relFileCompressOption);
    if (compress_buffer_size < 0) {
        free(workBuffer);
        return false;
    }
    if (compress_buffer_size >= BLCKSZ) {
        /* store original page if can not save space? */
        free(workBuffer);
        workBuffer = (char *)buffer;
        compress_buffer_size = BLCKSZ;
    }
    bool result = this->WriteBufferToCurrentBlock(workBuffer, blkNumber, compress_buffer_size);
    if (workBuffer != nullptr && workBuffer != buffer) {
        free(workBuffer);
    }
    return result;
}

COMPRESS_ERROR_STATE PageCompression::TruncateFile(BlockNumber newBlockNumber)
{
    BlockNumber extentNumber = newBlockNumber / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentOffset = newBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentStart = (extentNumber * CFS_EXTENT_SIZE) % CFS_MAX_BLOCK_PER_FILE;  // 0   129      129*2 129*3
    BlockNumber extentHeader = extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;  //              128 129+128  129*2+128

    if (newBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT == 0) {
        if (ftruncate(fileno(this->fd), extentStart * BLCKSZ) != 0) {
            return PCD_TRUNCATE_ERROR;
        }
    }

    auto cfsExtentHeader = this->GetStruct(newBlockNumber);
    for (uint16 i = (uint16)extentOffset; i < CFS_LOGIC_BLOCKS_PER_EXTENT; i++) {
        auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, i);

        cfsExtentAddress->nchunks = 0;
        cfsExtentAddress->checksum = AddrChecksum32(cfsExtentAddress, cfsExtentAddress->allocated_chunks);
    }
    pg_atomic_write_u32(&cfsExtentHeader->nblocks, extentOffset);

    uint16 max = 0;
    for (uint16 i = 0; i < extentOffset; i++) {
        auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, i);
        for (int j = 0; j < cfsExtentAddress->allocated_chunks; j++) {
            max = (max > cfsExtentAddress->chunknos[j]) ? max : cfsExtentAddress->chunknos[j];
        }
    }
    uint32 start = extentStart * BLCKSZ + max * cfsExtentHeader->chunk_size;
    uint32 len = (uint32)((CFS_MAX_LOGIC_CHRUNKS_NUMBER(cfsExtentHeader->chunk_size) - max) *
                          cfsExtentHeader->chunk_size);
    if (len >= PC_CUSTOM_VALUE_LARGESIZE) {
        start += len % PC_CUSTOM_VALUE_LARGESIZE;
        len -= len % PC_CUSTOM_VALUE_LARGESIZE;
        if (fallocate(fileno(this->fd), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, start, len) < 0) {
            return PCD_TRUNCATE_ERROR;
        }
    }
    /* truncate pcd qfile */
    if (ftruncate(fileno(this->fd), (extentHeader + 1) * BLCKSZ) != 0) {
        return PCD_TRUNCATE_ERROR;
    }
    return SUCCESS;
}

void PunchHoleForCompressedFile(FILE* file, const char *filename)
{
    /* check if it is compressed file */
    if (!IsCompressedFile(filename, strlen(filename))) {
        return;
    }
    
    /* punch hole ext by ext */
    if (fseek(file, 0L, SEEK_END) < 0) {
        return;
    }

    // read file size of toFullPath
    off_t fileLen = ftell(file);
    BlockNumber fileBlockNum = (BlockNumber) fileLen / BLCKSZ;
    char unaligned_buf[BLCKSZ + BLCKSZ] = {0};
    char *pcabuffer = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    for (BlockNumber block = 0; block < fileBlockNum; block += CFS_EXTENT_SIZE) {
        off_t offset = (block + CFS_EXTENT_SIZE - 1) * BLCKSZ;
        int32 total_size = (int32)pread64(fileno(file), pcabuffer, BLCKSZ, offset);
        if (total_size != BLCKSZ) {
            continue;
        }

        CfsExtentHeader *cfsExtentHeader = (CfsExtentHeader *)(void *)pcabuffer;
        uint32 chunkSize = cfsExtentHeader->chunk_size;
        uint32 freeChunk = CFS_MAX_LOGIC_CHRUNKS_NUMBER(chunkSize) - cfsExtentHeader->allocated_chunks;
        uint32 freeSize = freeChunk * chunkSize;
        if (freeSize < PC_CUSTOM_VALUE_LARGESIZE) {
            continue;
        }

        // punch hole
        off_t punchSize = (off_t)((freeSize / PC_CUSTOM_VALUE_LARGESIZE) * PC_CUSTOM_VALUE_LARGESIZE);
        offset -= (off_t)punchSize;
        if (fallocate(fileno(file), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, punchSize) < 0) {
            continue;
        }
    }
}
