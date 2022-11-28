//
// Created by cfs on 4/8/22.
//

#ifndef OPENGAUSS_CFS_TOOLS_H
#define OPENGAUSS_CFS_TOOLS_H

#include "c.h"
#include "storage/cfs/cfs.h"
#include "storage/buf/block.h"

#define COMPRESS_FSEEK_ERROR SIZE_MAX
#define COMPRESS_FREAD_ERROR SIZE_MAX - 1
#define COMPRESS_CHECKSUM_ERROR SIZE_MAX - 2
#define COMPRESS_BLOCK_ERROR SIZE_MAX - 3
#define MIN_COMPRESS_ERROR_RT SIZE_MAX / 2

struct CfsHeaderMap {
    CfsExtentHeader *header;
    BlockNumber extentCount;
    void *pointer;
    size_t mmapLen;
};

struct CfsReadStruct {
    FILE *fd;
    CfsExtentHeader *header;
    BlockNumber extentCount;
};

constexpr int MAX_RETRY_LIMIT = 60;
constexpr long RETRY_SLEEP_TIME = 1000000L;

bool CompressedChecksum(const char *compressedData);
size_t CfsReadCompressedPage(char *dst, size_t destLen, BlockNumber extent_offset_blkno, CfsReadStruct *cfsReadStruct,
    BlockNumber blockNum);

/**
 *
 * @param blockNumber block number
 * @param pageCompressAddr addr of block
 * @return checksum uint32
 */
uint32 AddrChecksum32(const CfsExtentAddress *cfsExtentAddress, const int needChunks);

CfsHeaderMap MMapHeader(FILE* fd, BlockNumber extentIndex, bool readOnly = false);
void MmapFree(CfsHeaderMap *cfsHeaderMap);


size_t ReadBlockNumberOfCFile(FILE* compressFd, off_t *curFileLen);

#endif //OPENGAUSS_CFS_TOOLS_H
