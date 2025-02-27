#ifndef OPENGAUSS_CFS_MD_H
#define OPENGAUSS_CFS_MD_H

#include "storage/smgr/smgr.h"
#include "storage/smgr/cfs_addressing.h"

#define MIN_FALLOCATE_SIZE (4096)

/** write pages back to storage, called by mdwriteback or spc_writeback. Note: we only flush one file segment, if there
 are some blocks are not in current segment file, the calller should revoke this function again.
 @param[in]     reln             SMgrRelation.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     blocknum         block number, when under segment table, physical.
 @param[in]     nblocks          block amount to write back.
 @param[in]     type             storage type, heap or segment.
 @return  the number blocks has been flushed. */
extern BlockNumber CfsWriteBack(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size,
                                ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks,
                                EXTEND_STORAGE_TYPE type);

/** main entry to write compress pages to storage.
 @param[in]     reln             SMgrRelation.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     buffer           page data.
 @param[in]     isExtend         is called under extend situation.
 @param[in]     type             storage type, heap or segment */
extern size_t CfsWritePage(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                           BlockNumber logicBlockNumber, const char *buffer, bool isExtend, EXTEND_STORAGE_TYPE type);

/** extend one extent, not used under segment storage.
 @param[in]     reln             SMgrRelation.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     buffer           page data
 @param[in]     type             storage type, heap or segment */
void CfsExtendExtent(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                     BlockNumber logicBlockNumber, const char *buffer, EXTEND_STORAGE_TYPE type);

/** extend one extent, under segment storage.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     buffer           page data
 @param[in]     location         block location info */
void CfsExtendForSeg(const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                     BlockNumber logicBlockNumber, const char *buffer, const ExtentLocation &location);

/** Get number of blocks present in a single disk file, only used for normal heap storage.
 @param[in]     relFileNode      RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     segNo            number of current file consist of the logical file
 @param[in]     len              file length.
 @return number of blocks present in the file */
extern BlockNumber CfsNBlock(const RelFileNode &relFileNode, int fd, BlockNumber segNo, off_t len);

/** main entry to read compress pages from storage.
 @param[in]     reln             SMgrRelation.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     buffer           page data
 @param[in]     type             storage type, heap or segment */
extern int CfsReadPage(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                       BlockNumber logicBlockNumber, char *buffer, EXTEND_STORAGE_TYPE type);

/** called by mdprefetch.
 @param[in]     reln             SMgrRelation.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     type             storage type, heap or segment */
void CfsMdPrefetch(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                   BlockNumber logicBlockNumber, EXTEND_STORAGE_TYPE type);

/** Truncate relation to specified number of blocks.
 @param[in]     reln             SMgrRelation.
 @param[in]     relNode          RelFileNode.
 @param[in]     fd               file discriptor.
 @param[in]     extent_size      extent size.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     type             storage type, heap or segment
 @return byte location truncated to */
off_t CfsMdTruncate(SMgrRelation reln, const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                    BlockNumber logicBlockNumber, EXTEND_STORAGE_TYPE type);

/** Get the real file handler..
 @param[in]     sRel             SMgrRelation.
 @param[in]     forknum          fork number.
 @param[in]     logicBlockNumber block number, when under segment table, physical.
 @param[in]     skipSync         skip file sync
 @param[in]     type             storage type, heap or segment
 @return handler */
int CfsGetFd(SMgrRelation sRel, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, int type);
#endif