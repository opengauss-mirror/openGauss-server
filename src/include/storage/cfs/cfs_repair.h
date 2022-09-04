#ifndef OPENGAUSS_CFS_REPAIR_H
#define OPENGAUSS_CFS_REPAIR_H

#include "storage/smgr/smgr.h"

extern int WriteRepairFile_Compress_extent(SMgrRelation reln, BlockNumber logicBlockNumber, 
    char* reapirpath, char *buf, BlockNumber offset, uint32 blocks);
extern int WriteRepairFile_Compress(const RelFileNode &rd_node, int fd, char* path, char *buf, BlockNumber blkno_segno_offset, uint32 blk_cnt);
extern void CfsHeaderPageCheckAndRepair(SMgrRelation reln, BlockNumber logicBlockNumber, 
    char *pca_page_res, uint32 strLen, bool *need_repair_pca);
extern int CfsGetPhysicsFD(const RelFileNode &relnode, BlockNumber logicBlockNumber);

typedef enum CfsHeaderPagerCheckStatus {
    CFS_HEADER_CHECK_STATUS_OK,         // cfs header page checked ok without any error
    CFS_HEADER_CHECK_STATUS_REPAIRED,   // cfs header page checked  error but has been repaired
    CFS_HEADER_CHECK_STATUS_ERROR       // can not be repaired.
} CfsHeaderPagerCheckStatus;

#endif