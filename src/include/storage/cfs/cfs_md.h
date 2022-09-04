#ifndef OPENGAUSS_CFS_MD_H
#define OPENGAUSS_CFS_MD_H

#include "storage/smgr/smgr.h"

#define MIN_FALLOCATE_SIZE (4096)

/* cfs interface */
extern void CfsWriteBack(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks, CFS_STORAGE_TYPE type);
extern size_t CfsWritePage(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, const char *buffer, bool skipSync, CFS_STORAGE_TYPE type);
void CfsExtendExtent(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, const char *buffer, CFS_STORAGE_TYPE type);
extern BlockNumber CfsNBlock(const RelFileNode &relFileNode, int fd, BlockNumber segNo, off_t len);
extern int CfsReadPage(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, char *buffer, CFS_STORAGE_TYPE type);
void CfsMdPrefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, CFS_STORAGE_TYPE type);
off_t CfsMdTruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, CFS_STORAGE_TYPE type);

#endif