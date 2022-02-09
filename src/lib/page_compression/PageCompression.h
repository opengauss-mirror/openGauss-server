/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OPENGAUSS_SERVER_PAGECOMPRESSION_H
#define OPENGAUSS_SERVER_PAGECOMPRESSION_H

#include "c.h"
#include "compressed_common.h"
#include "storage/page_compression.h"

#ifndef palloc
#define palloc(sz) malloc(sz)
#endif
#ifndef pfree
#define pfree(ptr) free(ptr)
#endif

enum COMPRESS_ERROR_STATE {
    SUCCESS,
    NORMAL_OPEN_ERROR,
    NORMAL_READ_ERROR,
    NORMAL_SEEK_ERROR,
    NORMAL_MISSING_ERROR,
    NORMAL_UNLINK_ERROR,
    PCA_OPEN_ERROR,
    PCA_READ_ERROR,
    PCA_SEEK_ERROR,
    PCA_WRITE_ERROR,
    PCA_MISSING_ERROR,
    PCA_UNLINK_ERROR,
    PCD_OPEN_ERROR,
    PCD_READ_ERROR,
    PCD_SEEK_ERROR,
    PCD_WRITE_ERROR,
    PCD_MISSING_ERROR,
    PCD_UNLINK_ERROR,
    PCD_TRUNCATE_ERROR,
    PCA_MMAP_ERROR
};
COMPRESS_ERROR_STATE ConstructCompressedFile(const char *toFullPath, BlockNumber segmentNo, uint16 chunkSize,
                                             uint8 algorithm);
extern bool FetchSourcePca(unsigned char* pageCompressHeader, size_t len, RewindCompressInfo* rewindCompressInfo);
bool ProcessLocalPca(const char *tablePath, RewindCompressInfo *rewindCompressInfo, const char *prefix = NULL);
void FormatPathToPca(const char *path, char *dst, size_t len, const char *pg_data = NULL);
void FormatPathToPcd(const char *path, char *dst, size_t len, const char *pg_data = NULL);

class PageCompression {
public:
    ~PageCompression();
    COMPRESS_ERROR_STATE Init(const char *filePath, size_t len, BlockNumber inSegmentNo,
              decltype(PageCompressHeader::chunk_size) chunkSize = 0, bool create = false);
    FILE *GetPcdFile() const;
    BlockNumber GetSegmentNo() const;
    BlockNumber GetMaxBlockNumber() const;
    decltype(PageCompressHeader::chunk_size) GetChunkSize() const;
    decltype(PageCompressHeader::algorithm) GetAlgorithm() const;
    PageCompressHeader *GetPageCompressHeader() const;
    size_t ReadCompressedBuffer(BlockNumber blockNum, char *buffer, size_t bufferLen, bool zeroAlign = false);
    bool WriteBufferToCurrentBlock(const char *buf, BlockNumber blockNumber, int32 size);
    bool DecompressedPage(const char *src, char *dest) const;
    bool WriteBackUncompressedData(const char *uncompressed, size_t uncompressedLen, char *buffer, size_t size,
                                   BlockNumber blockNumber);
    COMPRESS_ERROR_STATE TruncateFile(BlockNumber oldBlockNumber, BlockNumber newBlockNumber);
    const char *GetInitPath() const;
    void ResetPcdFd();
public:
    static bool SkipCompressedFile(const char *fileName, size_t len);
    static bool IsCompressedTableFile(const char *fileName, size_t len);
    static COMPRESS_ERROR_STATE RemoveCompressedFile(const char *path);
    static bool InnerPageCompressChecksum(const char *buffer);
private:
    PageCompressHeader *header;
    char initPath[MAXPGPATH];
    decltype(PageCompressHeader::chunk_size) chunkSize = 0;
    FILE *pcaFile = nullptr;
    FILE *pcdFile = nullptr;
    BlockNumber segmentNo;
};

#endif  // OPENGAUSS_SERVER_PAGECOMPRESSION_H
