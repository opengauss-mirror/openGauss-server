#include "storage/cfs/cfs_converter.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/storage_converter.h"

/** We use this method to caculate the compress extent position by physical block number.
 * 1. FIXME: group_total_blocks exceeds maximum of BlockNumer(uint32) when extent_size is ext_size_8196,
 * it's not right, waiting to be fixed...
 * 2. FIXME: BlockNumber is uint32 type, actually it's not suitable to use a uint32 type as block number is segment-page.
 * Because tables in a segment-page tablespace are store together, therefore the number will exceeds max of uint32 easily.
 * Maximum storage space can be repesented by uint32 is : (2^32 - 1) * (BLKSZ) == 32T
 * @param[in]     extent_size        The extent size of the segment extent.
 * @param[in]     forknum   Fork number.
 * @param[in]     blocknum   The physical block number in the relation.
 * @param[in/out]     cfs_extent_offset_page   Offset in the cfs extent.
 * @param[in/out] cfs_extent_start_page   Start block number of the cfs extent.
 */
void seg_get_cfs_extent_by_phyblocknum(int extent_size,  ForkNumber forknum, BlockNumber blocknum,
									   BlockNumber *cfs_extent_offset_page, BlockNumber *cfs_extent_start_page)
{
    /* ext_size_1024 : 64+4090 + 4175872*8192 = 34208747578 > 4294967295 */            
    BlockNumber group_total_blocks = DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE + DF_MAP_GROUP_EXTENTS * extent_size;
    uint32 seg_group_num = (blocknum - DF_MAP_HEAD_PAGE) / group_total_blocks;
    BlockNumber offset_in_group = (blocknum - DF_MAP_HEAD_PAGE) % group_total_blocks;

    /* remove map-block and ip-page */
    BlockNumber offset_in_data_pages = offset_in_group - (DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE);
    /* pca page is included in offset_in_data_pages, so divided by CFS_EXTENT_SIZE
     *other than CFS_LOGIC_BLOCKS_PER_EXTENT. */
    uint32 cfs_group_num = offset_in_data_pages / CFS_EXTENT_SIZE;
    /* offset_in_data_pages is definitely larger than 1 because there is always a pca page at the end of extent */
    Assert(offset_in_data_pages > 0);
	*cfs_extent_offset_page = (offset_in_data_pages - 1) % CFS_EXTENT_SIZE;

    *cfs_extent_start_page  = DF_MAP_HEAD_PAGE + 1 + (seg_group_num) * group_total_blocks +
                              DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE + cfs_group_num * CFS_EXTENT_SIZE;
    ereport(LOG, (errmodule(MOD_SEGMENT_PAGE), errmsg("seg_get_cfs_extent_by_phyblocknum"
	    " physical_block_num:%d, "
	    " group_total_blocks: %u, "
	    " offset_in_group:%d, offsoet_in_data_pages:%d, "
	    " cfs_extent_offset_page:%d, "
	    " cfs_extent_start_page:%d, "
	    " extent_size:%d, "
	    " cfs_group_num:%d",
	    blocknum,
	    group_total_blocks,
	    offset_in_group,
	    offset_in_data_pages,
	    *cfs_extent_offset_page,
	    *cfs_extent_start_page,
	    extent_size,
	    cfs_group_num
    )));
    return;
}

/** Check if this block is a data block.
 * @param[in]     blocknum      Physical block number.
 * @param[in]     extent_size   The extent size of the segment extent.
 * @param[in]     blocknum   The physical block number in the relation.
 * @param[in/out]     cfs_extent_offset_page   Offset in the cfs extent.
 * @return  Return true if it's data block, return false if it's file header, bitmap page, reverse page.
 */
bool seg_is_data_block(BlockNumber blocknum, int extent_size)
{
    bool is_data_page = false;
    if (blocknum <= DF_MAP_HEAD_PAGE) {
        is_data_page = false;
        return is_data_page;
    }
    BlockNumber group_total_blocks = DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE + DF_MAP_GROUP_EXTENTS * extent_size;
    is_data_page = ((blocknum - DF_MAP_HEAD_PAGE - 1) % group_total_blocks) >= (DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE)
        ? true : false;
    return is_data_page;
}

/* check whether this cfs extent is slice-accrossed */
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
 * otherwise we will get a wrong page count from pca.
 * @param[in]     rel_node    RelFileNode, which should always be fake node.
 * @param[in]     forknum     Fork number, should always be MAIN_FORKNUM.
 * @param[in]     blocknum    Physical blocknum.
 * @param[in]     extent_size    Segment extent size.
 * @return  Return true if can be compressed otherwise return false.
 */
bool seg_compress_allowed(const RelFileNode& rel_node, ForkNumber forknum, BlockNumber blocknum, int extent_size)
{
    BlockNumber cfs_extent_offset_page, cfs_extent_start_page;

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

    if (!seg_is_data_block(blocknum, extent_size)) {
        ereport(LOG, (errmsg("[sgement compress] Not a compression allowed block:%d due to it's a data block.",
                            blocknum)));
        return false;
    }

    seg_get_cfs_extent_by_phyblocknum(extent_size, forknum, blocknum,
        &cfs_extent_offset_page, &cfs_extent_start_page);
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
 * @param[in] sRel   Relation smgr object.
 * @param[in] forknum   Only main fork is allowed to be compressed.
 * @param[in] phy_blocknum  The global physical position of the block, pca page has been taken into aacount.
 * @param[in] skipSync   SkipFsync indicates that the caller will make other provisions to fsync the relation.
 * @param[in] type   The type of storagem, it should always be SEG_STORAGE when this function called.
 * @param[out] ExtentLocation  Location info to describe a page postion in compression storage.
 */
ExtentLocation seg_storage_convert(SMgrRelation rel, const RelFileNode& rel_node, int fd, int extent_size, ForkNumber forknum, BlockNumber phy_blocknum)
{
    RelFileCompressOption option;
    TransCompressOptions(rel_node, &option);
    ExtentLocation extention_location;

    BlockNumber extent_number = phy_blocknum / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extent_offset = InvalidBlockNumber;
    BlockNumber extent_start = InvalidBlockNumber;
    seg_get_cfs_extent_by_phyblocknum(extent_size, forknum, phy_blocknum, &extent_offset, &extent_start);

    extention_location.fd = fd;
    extention_location.relFileNode = rel_node;
    extention_location.extentNumber = extent_number;
    extention_location.chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize];
    extention_location.algorithm = (uint8)option.compressAlgorithm;
    extention_location.is_segment_page = true;
    extention_location.extentStart = extent_start % ((BlockNumber)RELSEG_SIZE);
    extention_location.extentOffset = extent_offset;
    /* headerNum is global physical page number, which is different from storage_convert(COM_STORAGE).
    The reason why we do like this: headerNum is used as a part of pca key in segment-page mode, if it's
    a slice local page number, hash conflicts will occur in pca buffer. */
    extention_location.headerNum = extent_start + CFS_LOGIC_BLOCKS_PER_EXTENT;
    if (seg_compress_allowed(rel_node, forknum, phy_blocknum, extent_size)) {
        extention_location.is_compress_allowed = true;
    } else {
        extention_location.is_compress_allowed = false;
    }
    return extention_location;
}

class SegStorageConverterRegister
{
public:
    SegStorageConverterRegister()
    {
        g_location_convert[SEG_STORAGE] = seg_storage_convert;
    }
};

SegStorageConverterRegister g_seg_storage_convert_register;