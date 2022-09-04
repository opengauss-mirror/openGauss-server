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
constexpr int CFS_EXTENT_SIZE = 129;
constexpr int CFS_EXTENT_COUNT_PER_FILE = RELSEG_SIZE / CFS_EXTENT_SIZE;
constexpr int CFS_MAX_BLOCK_PER_FILE = CFS_EXTENT_COUNT_PER_FILE * CFS_EXTENT_SIZE;

constexpr int CFS_LOGIC_BLOCKS_PER_EXTENT = CFS_EXTENT_SIZE - 1;
constexpr int CFS_LOGIC_BLOCKS_PER_FILE = CFS_LOGIC_BLOCKS_PER_EXTENT * CFS_EXTENT_COUNT_PER_FILE;


#define CFS_MAX_LOGIC_CHRUNKS_NUMBER(chrunk_size)  (CFS_LOGIC_BLOCKS_PER_EXTENT * (BLCKSZ / chrunk_size))


struct ExtentLocation {
    int fd;
    RelFileNode relFileNode;
    BlockNumber extentNumber;
    BlockNumber extentStart;
    BlockNumber extentOffset;
    BlockNumber headerNum;
    uint16 chrunk_size;
    uint8 algorithm;
};

typedef size_t CFS_STORAGE_TYPE;
constexpr CFS_STORAGE_TYPE COMMON_STORAGE = 0;

extern ExtentLocation StorageConvert(SMgrRelation sRel, ForkNumber forcknum, BlockNumber logicBlockNumber, bool skipSync, int type);
extern MdfdVec *CfsMdOpenReln(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior);
extern BlockNumber CfsGetBlocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);

typedef ExtentLocation (*CfsLocationConvert)(SMgrRelation sRel, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, int type);
extern CfsLocationConvert cfsLocationConverts[2];

constexpr int EXTENT_OPEN_FILE = 0;
constexpr int WRITE_BACK_OPEN_FILE = 1;
constexpr int EXTENT_CREATE_FILE = 2;

#endif //OPENGAUSS_CFS_CONVERTER_H
