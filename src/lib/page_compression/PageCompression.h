/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OPENGAUSS_SERVER_PAGECOMPRESSION_H
#define OPENGAUSS_SERVER_PAGECOMPRESSION_H

#include "c.h"
#include "compressed_common.h"
#include "storage/cfs/cfs_tools.h"
#include "storage/page_compression.h"

#ifndef palloc
#define palloc(sz) malloc(sz)
#endif
#ifndef pfree
#define pfree(ptr) free(ptr)
#endif

#define COMP_ASIGNMENT 0x8000

enum COMPRESS_ERROR_STATE {
    SUCCESS,
    NORMAL_OPEN_ERROR,
    NORMAL_READ_ERROR,
    NORMAL_WRITE_ERROR,
    NORMAL_CREATE_ERROR,
    NORMAL_SEEK_ERROR,
    NORMAL_MISSING_ERROR,
    NORMAL_UNLINK_ERROR,
    NORMAL_RENAME_ERROR,
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
    PCA_MMAP_ERROR,
    PCA_RENAME_ERROR,
    BUFFER_ALLOC_ERROR,
    FILE_CLOSE_ERROR
};
COMPRESS_ERROR_STATE ConstructCompressedFile(const char *toFullPath, uint16 chunkSize,
                                             uint8 algorithm);
extern bool FetchSourcePca(unsigned char* header, size_t len, RewindCompressInfo* info, size_t fileSize);
bool ProcessLocalPca(const char *tablePath, RewindCompressInfo *rewindCompressInfo, const char *prefix = NULL);
void PunchHoleForCompressedFile(FILE* file, const char *filename);

/**
 * thread unsafe
 */
class PageCompression {
public:
    ~PageCompression();
    COMPRESS_ERROR_STATE Init(const char *filePath, BlockNumber inSegmentNo, bool create = false);
    FILE *GetCompressionFile();
    FILE* fd = nullptr;
    BlockNumber GetSegmentNo() const;
    BlockNumber GetMaxBlockNumber() const;
    size_t ReadCompressedBuffer(BlockNumber blockNum, char *buffer, size_t bufferLen, bool zeroAlign = false);
    bool WriteBufferToCurrentBlock(char *buf, BlockNumber blkNumber, int32 size, CfsCompressOption *option = nullptr);
    bool DecompressedPage(const char *src, char *dest) const;
    bool WriteBackUncompressedData(const char *uncompressed, size_t uncompressedLen, char *buffer, size_t size,
                                   BlockNumber blkNumber);
    COMPRESS_ERROR_STATE TruncateFile(BlockNumber newBlockNumber);
    const char *GetInitPath() const;
public:
    static bool SkipCompressedFile(const char *fileName, size_t len);
    static bool IsIntegratedPage(const char *buffer, int segmentNo, BlockNumber blkNumber);
    decltype(PageCompressHeader::chunk_size) chunkSize = 0;
    decltype(PageCompressHeader::algorithm) algorithm = 0;
private:
    char initPath[MAXPGPATH];
    BlockNumber blockNumber;
    BlockNumber extentIdx;
    CfsHeaderMap cfsHeaderMap;
    BlockNumber segmentNo;
    CfsExtentHeader* GetStruct(BlockNumber blockNum, CfsCompressOption *option = nullptr);
    CfsExtentHeader* GetHeaderByExtentNumber(BlockNumber extentCount, CfsCompressOption *option = nullptr);
};

#endif  // OPENGAUSS_SERVER_PAGECOMPRESSION_H
