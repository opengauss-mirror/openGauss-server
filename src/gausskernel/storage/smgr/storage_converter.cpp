/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * storage_converter.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/storage_converter.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "storage/smgr/cfs_addressing.h"

LocationConvert g_location_convert[INVALID_EXTEND_STORAGE] = {StorageConvert, SegStorageConvert};

/** get location of the block .
 @param[in]     sRel               SMgrRelation.
 @param[in]     relNode            RelFileNode
 @param[in]     fd                 file handler
 @param[in]     extent_size        extent size of cfs
 @param[in]     fork_num           Fork number.
 @param[in]     logicBlockNumber   Logical block number.
 @return  ExtentLocation. */
ExtentLocation StorageConvert(SMgrRelation sRel, const RelFileNode& relNode, int fd,
                              int extent_size, ForkNumber forknum, BlockNumber logicBlockNumber)
{
    RelFileCompressOption option;
    TransCompressOptions(relNode, &option);

    BlockNumber extentNumber = logicBlockNumber / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentOffset = logicBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentStart = (extentNumber * CFS_EXTENT_SIZE) % CFS_MAX_BLOCK_PER_FILE;
    /* Block offset of pca page. */
    BlockNumber extentHeader = extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;

    return {.fd = fd,
            .relFileNode = relNode,
            .extentNumber = extentNumber,
            .extentStart = extentStart,
            .extentOffset = extentOffset,
            .headerNum = extentHeader,
            .chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
            .algorithm = (uint8)option.compressAlgorithm,
            .is_segment_page = false,
            .is_compress_allowed = true
    };
}