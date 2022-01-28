#ifndef OPENGAUSS_SERVER_OPENGAUSSCOMPRESSION_H
#define OPENGAUSS_SERVER_OPENGAUSSCOMPRESSION_H
#define FRONTEND 1


#include <stdio.h>
#include "c.h"
#include "storage/buf/block.h"
#include "storage/page_compression.h"

class OpenGaussCompression {
private:
    FILE* pcaFd = nullptr;
    FILE* pcdFd = nullptr;
    char pcaFilePath[MAXPGPATH];
    char pcdFilePath[MAXPGPATH];
    PageCompressHeader* header = nullptr;

private:
    int segmentNo;
    BlockNumber blockNumber;
    decltype(PageCompressHeader::chunk_size) chunkSize;
    char decompressedBuffer[BLCKSZ];
    bool byteConvert;
    bool diffConvert;

public:
    void SetFilePath(const char* filePath, int segNo);
    virtual ~OpenGaussCompression();
    bool TryOpen();
    bool ReadChunkOfBlock(char* dst, size_t* dstLen, BlockNumber blockNumber);
    bool WriteBackCompressedData(char* source, size_t sourceLen, BlockNumber blockNumber);
    bool WriteBackUncompressedData();
    void MarkCompressedDirty(char* source, size_t sourceLen);
    void MarkUncompressedDirty();
    BlockNumber GetMaxBlockNumber();
    char* GetPcdFilePath();
    char* GetDecompressedPage();
};

#endif  // OPENGAUSS_SERVER_OPENGAUSSCOMPRESSION_H
