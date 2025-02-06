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
 * segment_converter.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/segment_converter.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "storage/cfs/cfs_converter.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/cfs_addressing.h"

/** We use this method to caculate the compress extent position by physical block number.
 1. FIXME: group_total_blocks exceeds maximum of BlockNumer(uint32) when extent_size is ext_size_8196,
 it's not right, waiting to be fixed...
 2. FIXME: BlockNumber is uint32 type, actually it's not suitable to use a uint32 type as block number is segment-page.
 Because tables in a segment-page tablespace are store together, therefore the number will exceeds max of uint32 easily.
 Maximum storage space can be repesented by uint32 is : (2^32 - 1) * (BLKSZ) == 32T
 @param[in]     extent_size        The extent size of the segment extent.
 @param[in]     forknum   Fork number.
 @param[in]     blocknum   The physical block number in the relation.
 @param[in/out]     cfs_extent_offset_page   Offset in the cfs extent.
 @param[in/out] cfs_extent_start_page   Start block number of the cfs extent. */
void SegGetCfsExtentByPhyBlockNum(int extent_size,  ForkNumber forknum, BlockNumber blocknum,
                                  BlockNumber *cfs_extent_offset_page, BlockNumber *cfs_extent_start_page)
{
    /* get total block number of one group in segment store. */
    BlockNumber group_total_blocks = DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE + DF_MAP_GROUP_EXTENTS * extent_size;
    /* get our blocknum's corresponding offset in the group */
    BlockNumber offset_in_group = (blocknum - DF_MAP_HEAD_PAGE - 1) % group_total_blocks;
    /* offset_in_group contains meta data such as IPBLOCK, we want offset in data blocks here. */
    BlockNumber offset_in_group_data = offset_in_group - (DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE);
    /* By design, extent_size must be a multiple of CFS_EXTENT_SIZE, so we can get *cfs_extent_offset_page
    by % CFS_EXTENT_SIZE */
    *cfs_extent_offset_page = offset_in_group_data % CFS_EXTENT_SIZE;
    /* blocknum = *cfs_extent_start_page + *cfs_extent_offset_page. */
    *cfs_extent_start_page = blocknum - *cfs_extent_offset_page;

    ereport(LOG,
            (errmodule(MOD_SEGMENT_PAGE),
             errmsg("seg_get_cfs_extent_by_phyblocknum"
                    " physical_block_num:%d, "
                    " group_total_blocks: %u, "
                    " offset_in_group:%d, offset_in_group_data:%d, "
                    " cfs_extent_offset_page:%d, "
                    " cfs_extent_start_page:%d, "
                    " extent_size:%d, ",
                    blocknum,
                    group_total_blocks,
                    offset_in_group,
                    offset_in_group_data,
                    *cfs_extent_offset_page,
                    *cfs_extent_start_page,
                    extent_size)));
    return;
}

/** Check if this block is a data block.
 @param[in]     extent_size   The extent size of the segment extent.
 @param[in]     blocknum   The physical block number in the relation.
 @param[in/out]     cfs_extent_offset_page   Offset in the cfs extent.
 @return  Return true if it's data block, return false if it's file header, bitmap page, reverse page. */
bool SegIsDataBlock(BlockNumber blocknum, int extent_size)
{
    if (blocknum <= DF_MAP_HEAD_PAGE) {
        return false;
    }
    BlockNumber group_total_blocks =
        DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE + DF_MAP_GROUP_EXTENTS * extent_size;
    if (((blocknum - DF_MAP_HEAD_PAGE - 1) % group_total_blocks) <
        (DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE)) {
        return false;
    }
    return true;
}

/* check whether this cfs extent is slice-accrossed
 @param[in]     cfs_extent_start_page   start page.
 @param[in]     cfs_extent_end_page     end page.
 @return  Return true if is slice-accrossed. */
bool seg_page_accross_cfs_extent(BlockNumber cfs_extent_start_page, BlockNumber cfs_extent_end_page)
{
    if (DF_OFFSET_TO_SLICENO(((off_t)cfs_extent_start_page) * BLCKSZ) !=
        DF_OFFSET_TO_SLICENO(((off_t)cfs_extent_end_page) * BLCKSZ)) {
        ereport(LOG, (errmsg("block is slice-accrossed, cfs_extent_start_page:%d, cfs_extent_end_page:%d",
                             cfs_extent_start_page, cfs_extent_end_page)));
        return true;
    }
    return false;
}

/** Although segement page use head->nblocks to represent block usage quantity, we have to update pca page,
otherwise we will get a wrong page count from pca.
 @param[in]     rel_node    RelFileNode, which should always be fake node.
 @param[in]     forknum     Fork number, should always be MAIN_FORKNUM.
 @param[in]     blocknum    Physical blocknum.
 @param[in]     extent_size    Segment extent size.
 @return  Return true if can be compressed otherwise return false. */
bool SegCompressAllowed(const RelFileNode& rel_node, ForkNumber forknum, BlockNumber blocknum, int extent_size)
{
    BlockNumber cfs_extent_offset_page;
    BlockNumber cfs_extent_start_page;

    if (!IS_SEG_COMPRESSED_RNODE(rel_node, forknum) || ENABLE_DSS) {
        ereport(LOG, (errmsg("[sgement compress] Not a compression allowed block:%d", blocknum)));
        return false;
    }

    if (blocknum <= DF_MAP_HEAD_PAGE) {
        ereport(LOG, (errmsg("[sgement compress] Not a compression allowed block:%d due to slice meta block,"
                             "max slice meta block no:%d.", blocknum, DF_MAP_HEAD_PAGE)));
        return false;
    }

    if (extent_size <= EXT_SIZE_8) {
        ereport(LOG, (errmsg("[sgement compress] Not a compression allowed block:%d due to small extent size: %d.",
                             blocknum, extent_size)));
        return false;
    }

    if (!SegIsDataBlock(blocknum, extent_size)) {
        ereport(LOG, (errmsg("[sgement compress] Not a compression allowed block:%d due to it's a data block.",
                             blocknum)));
        return false;
    }

    SegGetCfsExtentByPhyBlockNum(extent_size, forknum, blocknum, &cfs_extent_offset_page, &cfs_extent_start_page);
    off_t offset = ((off_t)blocknum) * BLCKSZ;
    int sliceno = DF_OFFSET_TO_SLICENO(offset);
    BlockNumber cfs_extent_end_page = cfs_extent_start_page + CFS_LOGIC_BLOCKS_PER_EXTENT;
    /* We will not compress block if the cfs extent is slice-across, but we will leave a pca page for
    this kind of cfs extent either And this special type pca page wil not be persisted but will be padded
    by pca_buf_padding when it's required. */
    if (seg_page_accross_cfs_extent(cfs_extent_start_page, cfs_extent_end_page)) {
        ereport(LOG,(errmsg("[sgement compress]  not compression allowed block:%d due to accross slice,"
                            "cfs_extent_start_page:%d,"
                            "cfs_extent_end_page:%d,"
                            "sliceno%d,"
                            "extent_size:%d",
                            blocknum,
                            cfs_extent_start_page,
                            cfs_extent_end_page,
                            sliceno,
                            extent_size)));
        return false;
    }

    if (blocknum == cfs_extent_end_page) {
        return false;
    }

    return true;
}

/** Convert logical block number to compression extent location, introduced by segment-page adaptation.
 @param[in] sRel   Relation smgr object.
 @param[in] forknum   Only main fork is allowed to be compressed.
 @param[in] phy_blocknum  The global physical position of the block, pca page has been taken into aacount.
 @param[in] skipSync   SkipFsync indicates that the caller will make other provisions to fsync the relation.
 @param[in] type   The type of storagem, it should always be SEG_STORAGE when this function called.
 @param[out] ExtentLocation  Location info to describe a page postion in compression storage. */
ExtentLocation SegStorageConvert(SMgrRelation rel, const RelFileNode& rel_node, int fd, int extent_size,
                                 ForkNumber forknum, BlockNumber phy_blocknum)
{
    RelFileCompressOption option;
    TransCompressOptions(rel_node, &option);
    ExtentLocation extention_location;

    /* used for as random value for pca recycle victim select, it's not consist with it's name, but ok
    for this case. */
    BlockNumber extent_number = phy_blocknum / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extent_offset = InvalidBlockNumber;
    BlockNumber extent_start = InvalidBlockNumber;
    SegGetCfsExtentByPhyBlockNum(extent_size, forknum, phy_blocknum, &extent_offset, &extent_start);

    extention_location.fd = fd;
    extention_location.relFileNode = rel_node;
    extention_location.extentNumber = extent_number;
    extention_location.chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize];
    extention_location.algorithm = (uint8)option.compressAlgorithm;
    extention_location.is_segment_page = true;
    extention_location.extentStart = extent_start % ((BlockNumber)RELSEG_SIZE);
    extention_location.extentOffset = extent_offset;
    extention_location.headerNum = extent_start + CFS_LOGIC_BLOCKS_PER_EXTENT;
    if (SegCompressAllowed(rel_node, forknum, phy_blocknum, extent_size)) {
        extention_location.is_compress_allowed = true;
    } else {
        extention_location.is_compress_allowed = false;
    }
    return extention_location;
}