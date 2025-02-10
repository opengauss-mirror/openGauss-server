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
 * cfs_addressing.h
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/cfs_addressing.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OPENGAUSS_STORAGE_CONVERTER_H
#define OPENGAUSS_STORAGE_CONVERTER_H
#include "storage/smgr/smgr.h"
#include "storage/cfs/cfs_converter.h"

struct ExtentLocation {
    int fd;
    /* file node of the relation file */
    RelFileNode relFileNode;
    /* segment extent number, not used currently */
    BlockNumber extentNumber;
    /* the cfs extent start and offset block */
    BlockNumber extentStart;
    BlockNumber extentOffset;
    /* headerNum is global physical page number, which is different from storage_convert(COM_STORAGE).
    The reason why we do like this: headerNum is used as a part of pca key in segment-page mode, if it's
    a slice local page number, hash conflicts will occur in pca buffer. */
    BlockNumber headerNum;
    /* compression mata data */
    uint16 chunk_size;
    uint8 algorithm;
    bool is_segment_page;
    /* In some cases, like cfs extent cross 1G files, we don't compress blocks in it, is_compress_allowed is false */
    bool is_compress_allowed;
    void info() const
    {
        ereport(LOG, (errmsg("ExtentLocation Info, fd:%d, relFileNode:%u, extentNumber:%d, extentStart:%d,"
                             "extentOffset:%d, headerNum:%d, chunk_size:%d, algorithm:%d, is_segment_page:%d,"
                             " is_compress_allowed:%d", fd, relFileNode.relNode, extentNumber, extentStart,
                             extentOffset, headerNum, chunk_size, algorithm, is_segment_page, is_compress_allowed)));
    }
    /* convert global block number to slice local block number, only for special cfs extent in segment page mode */
    BlockNumber get_local_block_num() const
    {
        BlockNumber local_block_num = (extentStart + extentOffset) % RELSEG_SIZE;
        return local_block_num;
    }
    /* convert global header number to slice local block number, only used in segment page mode */
    BlockNumber get_local_header_num() const
    {
        BlockNumber local_header_num = headerNum % RELSEG_SIZE;
        return local_header_num;
    }
};

typedef enum EXTEND_STORAGE_TYPE {
    COMMON_STORAGE = 0,
    SEG_STORAGE,
    INVALID_EXTEND_STORAGE
} EXTEND_STORAGE_TYPE;

void SegGetCfsExtentByPhyBlockNum(int extent_size,  ForkNumber forknum, BlockNumber blocknum,
                                  BlockNumber *cfs_extent_offset_page, BlockNumber *cfs_extent_start_page);
bool SegIsDataBlock(BlockNumber blocknum, int extent_size);
bool SegCompressAllowed(const RelFileNode& relNode, ForkNumber forknum, BlockNumber blocknum, int extent_size);
ExtentLocation StorageConvert(SMgrRelation sRel, const RelFileNode& relNode, int fd, int extent_size,
                              ForkNumber forknum, BlockNumber logicBlockNumber);
ExtentLocation SegStorageConvert(SMgrRelation rel, const RelFileNode& rel_node, int fd, int extent_size,
                                 ForkNumber forknum, BlockNumber phy_blocknum);
typedef ExtentLocation (*LocationConvert)(SMgrRelation sRel, const RelFileNode& relNode, int fd, int extent_size,
                                          ForkNumber forknum, BlockNumber block_no);
extern LocationConvert g_location_convert[INVALID_EXTEND_STORAGE];
#endif