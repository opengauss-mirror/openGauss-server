/* -------------------------------------------------------------------------
 *
 * compressed_common.h
 *
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * -------------------------------------------------------------------------
 */
#ifndef OPENGAUSS_SERVER_COMPRESS_COMPRESSED_COMMON_H
#define OPENGAUSS_SERVER_COMPRESS_COMPRESSED_COMMON_H

#include "utils/atomic.h"



struct RewindCompressInfo {
    bool compressed = false;    /* compressed table or not */
    uint32 oldBlockNumber = 0;
    uint32 newBlockNumber = 0;
    uint8 algorithm = 0;    /* compressed algorithm */
    uint16 chunkSize = 0;   /* compressed chunk size */
};

struct CompressedPcaInfo {
    char *pcaMap = NULL;
    int pcaFd = -1;
    char path[MAXPGPATH];
    int32 chunkSize = 0;
    int32 algorithm = 0;
};

#define COPY_REWIND_COMPRESS_INFO(entry, infoPointer, oldBlock, newBlock)    \
    (entry)->rewindCompressInfo.oldBlockNumber = 0;                          \
    (entry)->rewindCompressInfo.newBlockNumber = 0;                          \
    (entry)->rewindCompressInfo.compressed = false;                          \
    (entry)->rewindCompressInfo.algorithm = 0;                               \
    (entry)->rewindCompressInfo.chunkSize = 0;                               \
    if ((infoPointer) != NULL && (infoPointer)->compressed) {                \
        (entry)->rewindCompressInfo.oldBlockNumber = (oldBlock);             \
        (entry)->rewindCompressInfo.newBlockNumber = (newBlock);             \
        (entry)->rewindCompressInfo.compressed = true;                       \
        (entry)->rewindCompressInfo.algorithm = (infoPointer)->algorithm;    \
        (entry)->rewindCompressInfo.chunkSize = (infoPointer)->chunkSize;    \
    }

#endif  // OPENGAUSS_SERVER_COMPRESS_COMPRESSED_COMMON_H
