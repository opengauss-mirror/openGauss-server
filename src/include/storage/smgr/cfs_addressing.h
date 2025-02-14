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
    /** file node of the relation file */
    RelFileNode relFileNode;

    /** Not used in segment store. in common storage, it's the compression extent number in the relFileNode. */
    BlockNumber extentNumber;

    /** The cfs extent start and offset block, i.e. The block no of 0-th block of current extent in current
    disk file. */
    BlockNumber extentStart;

    /** The n-th block in current compression extent. */
    BlockNumber extentOffset;

    /** For Segment store: it's the PCA page Blocknumber in current relFileNode. the physical position offset
    in current file(fd) is (pcaBlkNo % RELSEG_SIZE) * BLCKSZ. otherwise, it's the PCA block number of current
    the disk file（fd). */
    BlockNumber headerNum;

    /** The compression mata data. */
    uint16 chunk_size;

    /** The compression algorithm. */
    uint8 algorithm;

    /** Whether or not is a segment store page. */
    bool is_segment_page;

    /* In some cases, like cfs extent cross 1G files, we don't compress blocks in it, is_compress_allowed is false */
    bool is_compress_allowed;

    void info() const
    {
        ereport(LOG, (errmsg("ExtentLocation Info, fd: %d, relFileNode: %u, extentNumber: %d, extentStart: %d,"
                             "extentOffset: %d, pca block numner: %d, chunk_size: %d, algorithm: %d, "
                             "is_segment_page: %d, is_compress_allowed: %d", fd, relFileNode.relNode, extentNumber,
                             extentStart, extentOffset, headerNum, chunk_size, algorithm, is_segment_page,
                             is_compress_allowed)));
    }

    /** Get curreunt block's physical offset in current disk file, only for special cfs extent in segment page mode
     @return the curreunt block's physical offset in current disk file. */
    off_t GetBlockPhysicalOffset() const
    {
        return ((extentStart + extentOffset) % RELSEG_SIZE) * BLCKSZ;
    }

    /** Get PCA page's physical offset in current disk file.
     @return the PCA page's physical offset in current disk file. */
    off_t GetPcaPhysicalOffset() const
    {
        Assert(headerNum <= RELSEG_SIZE || is_segment_page);
        if (is_segment_page) {
            return (headerNum % RELSEG_SIZE) * BLCKSZ;
        }
        return headerNum * BLCKSZ;
    }

    /** Get PCA's block number in current RelFileNode.
     @return the PCA's block number in current RelFileNode. */
    BlockNumber GetFileNodePcaBlkno() const
    {
        if (is_segment_page) {
            return headerNum;
        }

        auto totalExBlks = extentNumber * CFS_EXTENT_SIZE;
        return totalExBlks - (totalExBlks % RELSEG_SIZE) + headerNum;
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