//
// Created by cfs on 2/10/22.
//

#ifndef OPENGAUSS_CFS_H
#define OPENGAUSS_CFS_H

#include <sys/mman.h>

#include "utils/atomic.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "datatype/timestamp.h"

struct CfsExtentAddress {
    uint32 checksum;
    volatile uint8 nchunks;          /* number of chunks for this block */
    volatile uint8 allocated_chunks; /* number of allocated chunks for this block */
    /* variable-length fields, 1 based chunk no array for this block, size of the array must be 2, 4 or 8 */
    uint16 chunknos[FLEXIBLE_ARRAY_MEMBER];  // valid value from 1
};

struct CfsCompressOption {
    uint16 chunk_size;                             /* size of each chunk, must be 1/2 1/4 or 1/8 of BLCKSZ */
    uint8 algorithm;                           /* compress algorithm, 1=pglz, 2=lz4 */
};

struct CfsExtentHeader {
    pg_atomic_uint32 nblocks;                      /* number of total blocks in this segment */
    pg_atomic_uint32 allocated_chunks;             /* number of total allocated chunks in data area */
    uint16 chunk_size;                             /* size of each chunk, must be 1/2 1/4 or 1/8 of BLCKSZ */
    uint8 algorithm : 7;                           /* compress algorithm, 1=pglz, 2=lz4 */
    uint8 recycleInOrder : 1;                      /* show if pca is recycled */
    uint8 recv;                                    /* for aligin */
    CfsExtentAddress cfsExtentAddress[FLEXIBLE_ARRAY_MEMBER];
};

struct CfsExtInfo {
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber extentNumber;
    uint32 assistFlag;
};

inline size_t SizeOfExtentAddress(uint16 chunkSize) {
    if (chunkSize == 0) {
        return -1;
    }
    return offsetof(CfsExtentAddress, chunknos) + sizeof(uint16) * BLCKSZ / chunkSize;
}

inline off_t OffsetOfPageCompressChunk(uint16 chunkSize, int chunkNo) {
    return chunkSize * (chunkNo - 1);
}

inline size_t SizeOfExtentAddressByChunks(uint8 nChunks) {
    return offsetof(CfsExtentAddress, chunknos) + sizeof(uint16) * nChunks;
}

inline CfsExtentAddress *GetExtentAddress(CfsExtentHeader *header, uint16 blockOffset) {
    auto chunkSize = header->chunk_size;
    auto headerOffset = offsetof(CfsExtentHeader, cfsExtentAddress);
    auto sizeOfExtentAddress = SizeOfExtentAddress(chunkSize);
    return (CfsExtentAddress *) (((char *) header) + headerOffset + blockOffset * sizeOfExtentAddress);
}

#endif // OPENGAUSS_CFS_H
