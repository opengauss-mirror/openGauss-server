/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "openGaussCompression.h"
#include "storage/checksum_impl.h"
#include "storage/page_compression_impl.h"

void OpenGaussCompression::SetFilePath(const char *filePath, int segNo)
{
    int rc = snprintf_s(pcaFilePath, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, filePath);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(pcdFilePath, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, filePath);
    securec_check_ss_c(rc, "\0", "\0");

    this->segmentNo = segNo;
}

OpenGaussCompression::~OpenGaussCompression()
{
    if (pcaFd != nullptr) {
        fclose(pcaFd);
    }
    if (pcdFd != nullptr) {
        fclose(pcdFd);
    }
    if (header != nullptr) {
        pc_munmap(header);
    }
}

bool OpenGaussCompression::TryOpen()
{
    if ((pcaFd = fopen(this->pcaFilePath, "rb+")) == nullptr) {
        return false;
    }
    if ((pcdFd = fopen(this->pcdFilePath, "rb+")) == nullptr) {
        return false;
    }
    if (fseeko(pcaFd, (off_t)offsetof(PageCompressHeader, chunk_size), SEEK_SET) != 0) {
        return false;
    }
    if (fread(&chunkSize, sizeof(chunkSize), 1, this->pcaFd) <= 0) {
        return false;
    }
    header = pc_mmap(fileno(pcaFd), chunkSize, false);
    return true;
}
bool OpenGaussCompression::ReadChunkOfBlock(char *dst, size_t *dstLen, BlockNumber blockNumber)
{
    auto currentAddr = GET_PAGE_COMPRESS_ADDR(header, chunkSize, blockNumber);
    do {
        auto chunkNum = currentAddr->nchunks;
        for (uint8 i = 0; i < chunkNum; i++) {
            off_t seekPos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunkSize, currentAddr->chunknos[i]);
            uint8 start = i;
            while (i < chunkNum - 1 && currentAddr->chunknos[i + 1] == currentAddr->chunknos[i] + 1) {
                i++;
            }
            if (fseeko(this->pcdFd, seekPos, SEEK_SET) != 0) {
                return false;
            }
            size_t readAmount = chunkSize * (i - start + 1);
            if (fread(dst + start * chunkSize, 1, readAmount, this->pcdFd) != readAmount && ferror(this->pcdFd)) {
                return false;
            }
            *dstLen += readAmount;
        }
        if (chunkNum == 0 || DecompressPage(dst, decompressedBuffer, header->algorithm) == BLCKSZ) {
            break;
        }
    } while (true);
    if (PageIs8BXidHeapVersion(dst)) {
        byteConvert = ((HeapPageCompressData *)dst)->byte_convert;
        diffConvert = ((HeapPageCompressData *)dst)->diff_convert;
    } else {
        byteConvert = ((PageCompressData *)dst)->byte_convert;
        diffConvert = ((PageCompressData *)dst)->diff_convert;
    }
    this->blockNumber = blockNumber;
    return true;
}

bool OpenGaussCompression::WriteBackCompressedData(char *source, size_t sourceLen, BlockNumber blockNumber)
{
    auto currentAddr = GET_PAGE_COMPRESS_ADDR(header, chunkSize, blockNumber);
    for (size_t i = 0; i < currentAddr->nchunks; ++i) {
        off_t seekPos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunkSize, currentAddr->chunknos[i]);
        if (fseeko(this->pcdFd, seekPos, SEEK_SET) != 0) {
            return false;
        }
        Assert(sourceLen >= i * chunkSize);
        auto writeCount = fwrite(source + i * chunkSize, 1, chunkSize, this->pcdFd);
        bool success = chunkSize == writeCount;
        if (!success) {
            return false;
        }
    }
    fflush(this->pcdFd);
    return true;
}

void OpenGaussCompression::MarkUncompressedDirty()
{
    constexpr int writeLen = BLCKSZ / 2;
    unsigned char fill_byte[writeLen] = {0xFF};
    for (int i = 0; i < writeLen; i++)
        fill_byte[i] = 0xFF;
    auto rc = memcpy_s(decompressedBuffer + writeLen, BLCKSZ - writeLen, fill_byte, writeLen);
    securec_check(rc, "", "");
}

BlockNumber OpenGaussCompression::GetMaxBlockNumber()
{
    return (BlockNumber)pg_atomic_read_u32(&header->nblocks);
}

char *OpenGaussCompression::GetPcdFilePath()
{
    return this->pcdFilePath;
}

char *OpenGaussCompression::GetDecompressedPage()
{
    return this->decompressedBuffer;
}

bool OpenGaussCompression::WriteBackUncompressedData()
{
    auto algorithm = header->algorithm;
    auto workBufferSize = CompressPageBufferBound(decompressedBuffer, algorithm);
    if (workBufferSize < 0) {
        return false;
    }
    char *work_buffer = (char *)malloc(workBufferSize);
    RelFileCompressOption relFileCompressOption;
    relFileCompressOption.compressPreallocChunks = 0;
    relFileCompressOption.compressLevelSymbol = true;
    relFileCompressOption.compressLevel = 1;
    relFileCompressOption.compressAlgorithm = algorithm;
    relFileCompressOption.byteConvert = byteConvert;
    relFileCompressOption.diffConvert = diffConvert;

    auto compress_buffer_size = CompressPage(decompressedBuffer, work_buffer, workBufferSize, relFileCompressOption);
    if (compress_buffer_size < 0) {
        return false;
    }
    uint8 nchunks = (compress_buffer_size - 1) / chunkSize + 1;
    auto bufferSize = chunkSize * nchunks;
    if (bufferSize >= BLCKSZ) {
        /* store original page if can not save space? */
        free(work_buffer);
        work_buffer = (char *)decompressedBuffer;
        nchunks = BLCKSZ / chunkSize;
    } else {
        /* fill zero in the last chunk */
        if (compress_buffer_size < bufferSize) {
            auto leftSize = bufferSize - compress_buffer_size;
            errno_t rc = memset_s(work_buffer + compress_buffer_size, leftSize, 0, leftSize);
            securec_check(rc, "", "");
        }
    }
    uint8 need_chunks = nchunks;
    PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(header, chunkSize, blockNumber);
    if (pcAddr->allocated_chunks < need_chunks) {
        auto chunkno = pg_atomic_fetch_add_u32(&header->allocated_chunks, need_chunks - pcAddr->allocated_chunks);
        for (uint8 i = pcAddr->allocated_chunks; i < need_chunks; ++i) {
            pcAddr->chunknos[i] = ++chunkno;
        }
        pcAddr->allocated_chunks = need_chunks;
        pcAddr->nchunks = need_chunks;
    }
    return this->WriteBackCompressedData(work_buffer, compress_buffer_size, blockNumber);
}


#include "compression_algorithm.ini"