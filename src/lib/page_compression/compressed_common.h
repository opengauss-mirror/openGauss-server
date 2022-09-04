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
};

#define COPY_REWIND_COMPRESS_INFO(entry, infoPointer, oldBlock, newBlock)    \
    (entry)->rewindCompressInfo.oldBlockNumber = 0;                          \
    (entry)->rewindCompressInfo.newBlockNumber = 0;                          \
    (entry)->rewindCompressInfo.compressed = false;                          \
    if ((infoPointer) != NULL && (infoPointer)->compressed) {                \
        (entry)->rewindCompressInfo.oldBlockNumber = (oldBlock);             \
        (entry)->rewindCompressInfo.newBlockNumber = (newBlock);             \
        (entry)->rewindCompressInfo.compressed = true;                       \
    }

#endif  // OPENGAUSS_SERVER_COMPRESS_COMPRESSED_COMMON_H
