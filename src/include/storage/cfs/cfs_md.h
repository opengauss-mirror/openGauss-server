#ifndef OPENGAUSS_CFS_MD_H
#define OPENGAUSS_CFS_MD_H

#include "storage/smgr/smgr.h"
#include "storage/smgr/storage_converter.h"

#define MIN_FALLOCATE_SIZE (4096)
/* cfs interface */
extern void CfsWriteBack(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber blocknum, BlockNumber nblocks, EXTEND_STORAGE_TYPE type);
extern size_t CfsWritePage(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber logicBlockNumber, const char *buffer, EXTEND_STORAGE_TYPE type);
void CfsExtendExtent(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber logicBlockNumber, const char *buffer, EXTEND_STORAGE_TYPE type);
void cfs_extend_for_seg(const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber logicBlockNumber, const char *buffer, EXTEND_STORAGE_TYPE type, const ExtentLocation &location);
extern BlockNumber CfsNBlock(const RelFileNode &relFileNode, int fd, BlockNumber segNo, off_t len);
extern int CfsReadPage(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber logicBlockNumber, char *buffer, EXTEND_STORAGE_TYPE type);
void CfsMdPrefetch(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber logicBlockNumber, EXTEND_STORAGE_TYPE type);
off_t CfsMdTruncate(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
    BlockNumber logicBlockNumber, EXTEND_STORAGE_TYPE type);
int CfsGetFd(SMgrRelation sRel, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, int type);
#endif
