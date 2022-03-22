/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "PageCompression.h"
#include "utils/pg_lzcompress.h"
#include "storage/page_compression_impl.h"

#include <memory>


void FormatPathToPca(const char *path, char *dst, size_t len, const char *prefix)
{
    errno_t rc;
    if (prefix) {
        rc = snprintf_s(dst, len, len - 1, "%s/" PCA_SUFFIX, prefix, path);
    } else {
        rc = snprintf_s(dst, len, len - 1, PCA_SUFFIX, path);
    }
    securec_check_ss_c(rc, "\0", "\0");
}

void FormatPathToPcd(const char *path, char *dst, size_t len, const char *prefix)
{
    errno_t rc;
    if (prefix) {
        rc = snprintf_s(dst, len, len - 1, "%s/" PCD_SUFFIX, prefix, path);
    } else {
        rc = snprintf_s(dst, len, len - 1, PCD_SUFFIX, path);
    }
    securec_check_ss_c(rc, "\0", "\0");
}

template <typename T>
COMPRESS_ERROR_STATE ReadCompressedInfo(T &t, off_t offset, FILE *file)
{
    if (fseeko(file, offset, SEEK_SET) != 0) {
        return PCA_SEEK_ERROR;
    }
    if (fread((void *)(&t), sizeof(t), 1, file) <= 0) {
        return PCA_READ_ERROR;
    }
    return SUCCESS;
}

/**
 * write RewindCompressInfo
 * @param file file fp
 * @param pcaFilePath file path,for ereport
 * @param rewindCompressInfo pointer of return
 * @return sucesss or not
 */
static bool ReadRewindCompressedInfo(FILE *file, RewindCompressInfo *rewindCompressInfo)
{
    off_t offset = (off_t)offsetof(PageCompressHeader, chunk_size);
    if (ReadCompressedInfo(rewindCompressInfo->chunkSize, offset, file) != SUCCESS) {
        return false;
    }
    offset = (off_t)offsetof(PageCompressHeader, algorithm);
    if (ReadCompressedInfo(rewindCompressInfo->algorithm, offset, file) != SUCCESS) {
        return false;
    }
    offset = (off_t)offsetof(PageCompressHeader, nblocks);
    if (ReadCompressedInfo(rewindCompressInfo->oldBlockNumber, offset, file) != SUCCESS) {
        return false;
    }
    rewindCompressInfo->compressed = true;
    return true;
}

bool FetchSourcePca(unsigned char *pageCompressHeader, size_t len, RewindCompressInfo *rewindCompressInfo)
{
    PageCompressHeader *ptr = (PageCompressHeader *)pageCompressHeader;
    rewindCompressInfo->compressed = false;
    if (len == sizeof(PageCompressHeader)) {
        rewindCompressInfo->compressed = true;
        rewindCompressInfo->algorithm = ptr->algorithm;
        rewindCompressInfo->newBlockNumber = ptr->nblocks;
        rewindCompressInfo->oldBlockNumber = 0;
        rewindCompressInfo->chunkSize = ptr->chunk_size;
    }
    return rewindCompressInfo->compressed;
}

bool ProcessLocalPca(const char *tablePath, RewindCompressInfo *rewindCompressInfo, const char *prefix)
{
    rewindCompressInfo->compressed = false;
    char pcaFilePath[MAXPGPATH];
    FormatPathToPca(tablePath, pcaFilePath, MAXPGPATH, prefix);
    FILE *file = fopen(pcaFilePath, "rb");
    if (file == NULL) {
        if (errno == ENOENT) {
            return false;
        }
        return false;
    }
    bool success = ReadRewindCompressedInfo(file, rewindCompressInfo);
    fclose(file);
    return success;
}

constexpr int MAX_RETRY_LIMIT = 60;
constexpr long RETRY_SLEEP_TIME = 1000000L;

BlockNumber PageCompression::GetSegmentNo() const
{
    return this->segmentNo;
}

size_t PageCompression::ReadCompressedBuffer(BlockNumber blockNum, char *buffer, size_t bufferLen, bool zeroAlign)
{
    auto chunkSize = this->header->chunk_size;
    PageCompressAddr *currentAddr = GET_PAGE_COMPRESS_ADDR(this->header, chunkSize, blockNum);
    size_t tryCount = 0;
    size_t actualSize = 0;
    do {
        auto chunkNum = currentAddr->nchunks;
        actualSize = chunkSize * chunkNum;
        for (uint8 i = 0; i < chunkNum; i++) {
            off_t seekPos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunkSize, currentAddr->chunknos[i]);
            uint8 start = i;
            while (i < chunkNum - 1 && currentAddr->chunknos[i + 1] == currentAddr->chunknos[i] + 1) {
                i++;
            }
            if (fseeko(this->pcdFile, seekPos, SEEK_SET) != 0) {
                return 0;
            }
            size_t readAmount = chunkSize * (i - start + 1);
            if (fread(buffer + start * chunkSize, 1, readAmount, this->pcdFile) != readAmount &&
                ferror(this->pcdFile)) {
                return 0;
            }
        }
        if (chunkNum == 0) {
            return 0;
        }

        /* compressed chunk */
        if (chunkNum * chunkSize < BLCKSZ) {
            if (PageCompression::InnerPageCompressChecksum(buffer)) {
                break;
            }
        } else if (PageIsNew(buffer) || pg_checksum_page(buffer, this->segmentNo * RELSEG_SIZE + blockNum) ==
                                            (PageHeader(buffer))->pd_checksum) {
            break;
        }

        if (tryCount < MAX_RETRY_LIMIT) {
            ++tryCount;
            pg_usleep(RETRY_SLEEP_TIME);
        } else {
            return 0;
        }
    } while (true);

    if (zeroAlign) {
        error_t rc = memset_s(buffer + actualSize, bufferLen - actualSize, 0, bufferLen - actualSize);
        securec_check(rc, "\0", "\0");
        actualSize = bufferLen;
    }
    return actualSize;
}

PageCompression::~PageCompression()
{
    if (this->header) {
        pc_munmap(this->header);
    }
    if (this->pcaFile) {
        fclose(this->pcaFile);
    }
    if (this->pcdFile) {
        fclose(this->pcdFile);
    }
}

bool PageCompression::InnerPageCompressChecksum(const char *buffer)
{
    char *data = NULL;
    size_t dataLen;
    uint32 crc32;
    if (PageIs8BXidHeapVersion(buffer)) {
        HeapPageCompressData *heapPageData = (HeapPageCompressData *)buffer;
        data = heapPageData->data;
        dataLen = heapPageData->size;
        crc32 = heapPageData->crc32;
    } else {
        PageCompressData *heapPageData = (PageCompressData *)buffer;
        data = heapPageData->data;
        dataLen = heapPageData->size;
        crc32 = heapPageData->crc32;
    }
    return DataBlockChecksum(data, dataLen, true) == crc32;
}

const char *PageCompression::GetInitPath() const
{
    return this->initPath;
}

COMPRESS_ERROR_STATE PageCompression::Init(const char *filePath, size_t len, BlockNumber inSegmentNo, uint16 chunkSize,
                                           bool create)
{
    errno_t rc = memcpy_s(this->initPath, MAXPGPATH, filePath, len);
    securec_check(rc, "", "");

    this->segmentNo = inSegmentNo;
    char compressedFilePath[MAXPGPATH];
    FormatPathToPca(filePath, compressedFilePath, MAXPGPATH);
    if ((this->pcaFile = fopen(compressedFilePath, create ? "wb+" : "rb+")) == nullptr) {
        return PCA_OPEN_ERROR;
    }

    FormatPathToPcd(filePath, compressedFilePath, MAXPGPATH);
    if ((this->pcdFile = fopen(compressedFilePath, create ? "wb+" : "rb+")) == nullptr) {
        return PCD_OPEN_ERROR;
    }

    if (chunkSize == 0) {
        /* read chunk size from pca file if chunk size is invalid */
        auto state = ReadCompressedInfo(chunkSize, (off_t)offsetof(PageCompressHeader, chunk_size), this->pcaFile);
        if (state != SUCCESS) {
            return state;
        }
    }
    this->chunkSize = chunkSize;
    if ((this->header = pc_mmap(fileno(this->pcaFile), chunkSize, false)) == MAP_FAILED) {
        return PCA_MMAP_ERROR;
    }
    return SUCCESS;
}

bool PageCompression::SkipCompressedFile(const char *fileName, size_t len)
{
    auto realSize = strlen(fileName);
    auto fileType = IsCompressedFile((char *)fileName, realSize < len ? realSize : len);
    return fileType != COMPRESSED_TYPE_UNKNOWN;
}

bool PageCompression::IsCompressedTableFile(const char *fileName, size_t len)
{
    char pcdFilePath[MAXPGPATH];
    errno_t rc = snprintf_s(pcdFilePath, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, fileName);
    securec_check_ss_c(rc, "\0", "\0");
    struct stat buf;
    int result = stat(pcdFilePath, &buf);
    return result == 0;
}

void PageCompression::ResetPcdFd()
{
    this->pcdFile = NULL;
}

FILE *PageCompression::GetPcdFile() const
{
    return this->pcdFile;
}

PageCompressHeader *PageCompression::GetPageCompressHeader() const
{
    return this->header;
}

BlockNumber PageCompression::GetMaxBlockNumber() const
{
    return (BlockNumber)pg_atomic_read_u32(&header->nblocks);
}

bool PageCompression::WriteBufferToCurrentBlock(const char *buf, BlockNumber blockNumber, int32 size)
{
    decltype(PageCompressHeader::chunk_size) curChunkSize = this->chunkSize;
    int needChunks = size / curChunkSize;

    PageCompressHeader *pcMap = this->header;
    PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, curChunkSize, blockNumber);

    /* allocate chunks */
    if (pcAddr->allocated_chunks < needChunks) {
        auto chunkno = pg_atomic_fetch_add_u32(&pcMap->allocated_chunks, needChunks - pcAddr->allocated_chunks);
        for (int i = pcAddr->allocated_chunks; i < needChunks; i++) {
            pcAddr->chunknos[i] = ++chunkno;
        }
        pcAddr->allocated_chunks = needChunks;
    }

    for (int32 i = 0; i < needChunks; ++i) {
        auto buffer_pos = buf + curChunkSize * i;
        off_t seekpos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(curChunkSize, pcAddr->chunknos[i]);
        int32 start = i;
        /* merge continuous write */
        while (i < needChunks - 1 && pcAddr->chunknos[i + 1] == pcAddr->chunknos[i] + 1) {
            i++;
        }
        size_t write_amount = curChunkSize * (i - start + 1);
        if (fseek(this->pcdFile, seekpos, SEEK_SET) < 0) {
            return false;
        }
        if (fwrite(buffer_pos, 1, write_amount, this->pcdFile) != write_amount) {
            return false;
        }
    }
    /* set other data of pcAddr */
    pcAddr->nchunks = needChunks;
    pcAddr->checksum = AddrChecksum32(blockNumber, pcAddr, curChunkSize);
    return true;
}

/**
 * return chunk-aligned size of buffer
 * @param buffer compressed page buffer
 * @param chunkSize chunk size
 * @return return chunk-aligned size of buffer
 */
size_t CalRealWriteSize(char *buffer, BlockNumber segmentNo, BlockNumber blockNumber,
                        decltype(PageCompressHeader::chunk_size) chunkSize)
{
    size_t compressedBufferSize;
    uint32 crc32;
    char *data;
    if (PageIs8BXidHeapVersion(buffer)) {
        HeapPageCompressData *heapPageData = (HeapPageCompressData *)buffer;
        compressedBufferSize = heapPageData->size + offsetof(HeapPageCompressData, data);
        crc32 = heapPageData->crc32;
        data = heapPageData->data;
    } else {
        PageCompressData *heapPageData = (PageCompressData *)buffer;
        compressedBufferSize = heapPageData->size + offsetof(PageCompressData, data);
        crc32 = heapPageData->crc32;
        data = heapPageData->data;
    }
    if (compressedBufferSize > 0 && compressedBufferSize <= ((size_t)chunkSize * (BLCKSZ / chunkSize - 1)) &&
        DataBlockChecksum(data, compressedBufferSize, true) == crc32) {
        return ((compressedBufferSize - 1) / chunkSize + 1) * chunkSize;
    }
    /* uncompressed page */
    return BLCKSZ;
}

template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&...args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

COMPRESS_ERROR_STATE ConstructCompressedFile(const char *toFullPath, BlockNumber segmentNo, uint16 chunkSize,
                                             uint8 algorithm)
{
    std::unique_ptr<PageCompression> pageCompression = make_unique<PageCompression>();
    auto result = pageCompression->Init(toFullPath, MAXPGPATH, segmentNo, chunkSize, true);
    if (result != SUCCESS) {
        return result;
    }

    PageCompressHeader *header = pageCompression->GetPageCompressHeader();
    header->chunk_size = chunkSize;
    header->algorithm = algorithm;

    /* read page by page */
    FILE *dataFile = fopen(toFullPath, "rb+");
    if (dataFile == NULL) {
        return NORMAL_OPEN_ERROR;
    }
    if (fseek(dataFile, 0L, SEEK_END) < 0) {
        return NORMAL_SEEK_ERROR;
    }
    /* read file size of toFullPath */
    off_t size = ftell(dataFile);
    if (fseek(dataFile, 0L, 0) < 0) {
        return NORMAL_SEEK_ERROR;
    }

    BlockNumber maxBlockNumber = size / BLCKSZ;
    BlockNumber blockNumber = 0;
    char buffer[BLCKSZ];
    for (blockNumber = 0; blockNumber < maxBlockNumber; blockNumber++) {
        if (fread(buffer, 1, BLCKSZ, dataFile) != BLCKSZ) {
            return NORMAL_READ_ERROR;
        }
        size_t realSize = CalRealWriteSize(buffer, pageCompression->GetSegmentNo(), blockNumber, chunkSize);
        pageCompression->WriteBufferToCurrentBlock(buffer, blockNumber, realSize);
    }
    header->nblocks = blockNumber;
    /* truncate tmp oid file */
    if (ftruncate(fileno(dataFile), 0L)) {
        return PCD_TRUNCATE_ERROR;
    }
    return SUCCESS;
}

bool PageCompression::DecompressedPage(const char *src, char *dest) const
{
    if (DecompressPage(src, dest, header->algorithm) == BLCKSZ) {
        return true;
    }
    return false;
}

bool PageCompression::WriteBackUncompressedData(const char *compressed, size_t compressedLen, char *buffer, size_t size,
                                                BlockNumber blockNumber)
{
    /* if compressed page is uncompressed, write back directly */
    if (compressedLen == BLCKSZ) {
        return this->WriteBufferToCurrentBlock(buffer, blockNumber, size);
    }

    bool byteConvert;
    bool diffConvert;
    if (PageIs8BXidHeapVersion(compressed)) {
        byteConvert = ((HeapPageCompressData *)compressed)->byte_convert;
        diffConvert = ((HeapPageCompressData *)compressed)->diff_convert;
    } else {
        byteConvert = ((PageCompressData *)compressed)->byte_convert;
        diffConvert = ((PageCompressData *)compressed)->diff_convert;
    }

    auto algorithm = header->algorithm;
    auto workBufferSize = CompressPageBufferBound(buffer, algorithm);
    if (workBufferSize < 0) {
        return false;
    }
    char *workBuffer = (char *)malloc(workBufferSize);
    RelFileCompressOption relFileCompressOption;
    relFileCompressOption.compressPreallocChunks = 0;
    relFileCompressOption.compressLevelSymbol = true;
    relFileCompressOption.compressLevel = 1;
    relFileCompressOption.compressAlgorithm = algorithm;
    relFileCompressOption.byteConvert = byteConvert;
    relFileCompressOption.diffConvert = diffConvert;

    auto compress_buffer_size = CompressPage(buffer, workBuffer, workBufferSize, relFileCompressOption);
    if (compress_buffer_size < 0) {
        return false;
    }
    uint8 nchunks = (compress_buffer_size - 1) / chunkSize + 1;
    auto bufferSize = chunkSize * nchunks;
    if (bufferSize >= BLCKSZ) {
        /* store original page if can not save space? */
        free(workBuffer);
        workBuffer = (char *)buffer;
        nchunks = BLCKSZ / chunkSize;
    } else {
        /* fill zero in the last chunk */
        if (compress_buffer_size < bufferSize) {
            auto leftSize = bufferSize - compress_buffer_size;
            errno_t rc = memset_s(workBuffer + compress_buffer_size, leftSize, 0, leftSize);
            securec_check(rc, "", "");
        }
    }
    return this->WriteBufferToCurrentBlock(workBuffer, blockNumber, bufferSize > BLCKSZ ? BLCKSZ : bufferSize);
}

COMPRESS_ERROR_STATE PageCompression::TruncateFile(BlockNumber oldBlockNumber, BlockNumber newBlockNumber)
{
    auto map = this->header;
    /* write zero to truncated addr */
    for (BlockNumber blockNumber = newBlockNumber; blockNumber < oldBlockNumber; ++blockNumber) {
        PageCompressAddr *addr = GET_PAGE_COMPRESS_ADDR(map, this->chunkSize, blockNumber);
        for (size_t i = 0; i < addr->allocated_chunks; ++i) {
            addr->chunknos[i] = 0;
        }
        addr->nchunks = 0;
        addr->allocated_chunks = 0;
        addr->checksum = 0;
    }
    map->last_synced_nblocks = map->nblocks = newBlockNumber;

    /* find the max used chunk number */
    pc_chunk_number_t beforeUsedChunks = map->allocated_chunks;
    pc_chunk_number_t max_used_chunkno = 0;
    for (BlockNumber blockNumber = 0; blockNumber < newBlockNumber; ++blockNumber) {
        PageCompressAddr *addr = GET_PAGE_COMPRESS_ADDR(map, this->chunkSize, blockNumber);
        for (uint8 i = 0; i < addr->allocated_chunks; i++) {
            if (addr->chunknos[i] > max_used_chunkno) {
                max_used_chunkno = addr->chunknos[i];
            }
        }
    }
    map->allocated_chunks = map->last_synced_allocated_chunks = max_used_chunkno;

    /* truncate pcd qfile */
    if (beforeUsedChunks > max_used_chunkno) {
        if (ftruncate(fileno(this->pcdFile), max_used_chunkno * chunkSize) != 0) {
            return PCD_TRUNCATE_ERROR;
        }
    }
    return SUCCESS;
}

COMPRESS_ERROR_STATE PageCompression::RemoveCompressedFile(const char *path)
{
    char dst[MAXPGPATH];
    if (unlink(path) != 0) {
        if (errno == ENOENT) {
            return NORMAL_MISSING_ERROR;
        }
        return NORMAL_UNLINK_ERROR;
    }
    FormatPathToPca(path, dst, MAXPGPATH);
    if (unlink(dst) != 0) {
        if (errno == ENOENT) {
            return PCA_MISSING_ERROR;
        }
        return PCA_UNLINK_ERROR;
    }
    FormatPathToPcd(path, dst, MAXPGPATH);
    if (unlink(dst) != 0) {
        if (errno == ENOENT) {
            return PCD_MISSING_ERROR;
        }
        return PCA_UNLINK_ERROR;
    }
    return SUCCESS;
}
decltype(PageCompressHeader::chunk_size) PageCompression::GetChunkSize() const
{
    return this->chunkSize;
}
decltype(PageCompressHeader::algorithm) PageCompression::GetAlgorithm() const
{
    return this->header->algorithm;
}
