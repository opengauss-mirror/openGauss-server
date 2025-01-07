//
// Created by cfs on 2/11/22.
//

#ifndef OPENGAUSS_CFS_CONVERTER_H
#define OPENGAUSS_CFS_CONVERTER_H

#include "utils/atomic.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/smgr.h"

/* 129 blocks per extent */
constexpr int CFS_EXTENT_SIZE = 128;
constexpr int CFS_EXTENT_COUNT_PER_FILE = RELSEG_SIZE / CFS_EXTENT_SIZE;
constexpr int CFS_MAX_BLOCK_PER_FILE = CFS_EXTENT_COUNT_PER_FILE * CFS_EXTENT_SIZE;

constexpr int CFS_LOGIC_BLOCKS_PER_EXTENT = CFS_EXTENT_SIZE - 1;
constexpr int CFS_LOGIC_BLOCKS_PER_FILE = CFS_LOGIC_BLOCKS_PER_EXTENT * CFS_EXTENT_COUNT_PER_FILE;

#define CFS_MAX_LOGIC_CHUNKS_NUMBER(chunk_size) (CFS_LOGIC_BLOCKS_PER_EXTENT * (BLCKSZ / (chunk_size)))
extern MdfdVec *CfsMdOpenReln(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior);
extern BlockNumber CfsGetBlocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);

constexpr int EXTENT_OPEN_FILE = 0;
constexpr int WRITE_BACK_OPEN_FILE = 1;
constexpr int EXTENT_CREATE_FILE = 2;

#endif //OPENGAUSS_CFS_CONVERTER_H
