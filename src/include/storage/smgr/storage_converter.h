#ifndef OPENGAUSS_STORAGE_CONVERTER_H
#define OPENGAUSS_STORAGE_CONVERTER_H
#include "storage/smgr/smgr.h"
#include "storage/cfs/cfs_converter.h"


struct ExtentLocation {
    int fd;
    RelFileNode relFileNode;
    BlockNumber extentNumber;
    BlockNumber extentStart;
    BlockNumber extentOffset;
    BlockNumber headerNum;
    uint16 chunk_size;
    uint8 algorithm;
    bool is_segment_page;
    bool is_compress_allowed;
    void info() const
    {
        ereport(LOG, (errmsg("ExtentLocation Info, fd:%d, relFileNode:%u, extentNumber:%d, extentStart:%d,\
            extentOffset:%d, headerNum:%d, chunk_size:%d, algorithm:%d, is_segment_page:%d, is_compress_allowed:%d",
            fd, relFileNode.relNode, extentNumber, extentStart, extentOffset, headerNum, chunk_size, algorithm,
            is_segment_page, is_compress_allowed)));
    }
    /* convert global block number to slice local block number, only for special cfs extent in segment page mode */
    BlockNumber get_local_block_num() const
    {
        BlockNumber local_block_num;
        local_block_num = (extentStart + extentOffset) % RELSEG_SIZE;
        return local_block_num;
    }
    /* convert global header number to slice local block number, only used in segment page mode */
    BlockNumber get_local_header_num() const
    {
        BlockNumber local_header_num;
        local_header_num = headerNum % RELSEG_SIZE;
        return local_header_num;
    }
};

typedef enum EXTEND_STORAGE_TYPE {
    COMMON_STORAGE = 0,
    SEG_STORAGE,
    INVALID_EXTEND_STORAGE
} EXTEND_STORAGE_TYPE;

void seg_get_cfs_extent_by_phyblocknum(int extent_size,  ForkNumber forknum, BlockNumber blocknum,
    BlockNumber *cfs_extent_offset_page, BlockNumber *cfs_extent_start_page);
bool seg_is_data_block(BlockNumber blocknum, int extent_size);
bool seg_compress_allowed(const RelFileNode& relNode, ForkNumber forknum, BlockNumber blocknum, int extent_size);
ExtentLocation StorageConvert(SMgrRelation sRel, const RelFileNode& relNode, int fd, int extent_size,
    ForkNumber forknum, BlockNumber logicBlockNumber);
typedef ExtentLocation (*LocationConvert)(SMgrRelation sRel, const RelFileNode& relNode, int fd, int extent_size,
    ForkNumber forknum, BlockNumber block_no);
extern LocationConvert g_location_convert[INVALID_EXTEND_STORAGE];
#endif