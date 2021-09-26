/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 * 
 * bcm.h
 *      bcm map interface
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/bcm.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef BCM_H
#define BCM_H

#include "storage/buf/block.h"
#include "storage/buf/buf.h"
#include "storage/smgr/relfilenode.h"
#include "utils/relcache.h"

typedef uint32 BCMBitStatus;

#define SYNCED 0U
#define NOTSYNCED 1
#define BACKUP 0
#define NOTBACKUP 1

typedef struct BCMHeader {
    StorageEngine type;
    RelFileNode node;
    int32 blockSize; /* one bit corresponding size */
} BCMHeader;

/*
 * Size of the bitmap on each bcm map page, in bytes. There's no
 * extra headers, so the whole page minus the standard page header is
 * used for the bitmap.
 */
#define BCMMAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))

/* Number of bits allocated for each heap block. */
#define BCM_BITS_PER_BLOCK 2

/* Number of heap blocks we can represent in one byte. */
#define BCM_BLOCKS_PER_BYTE 4

/* Number of blocks we can represent in one bcm map page. */
#define BCM_BLOCKS_PER_PAGE (BCMMAPSIZE * BCM_BLOCKS_PER_BYTE)

/* 10 */
#define BCM_SYNC_BITMASK 2U
/* 01 */
#define BCM_BACKUP_BITMASK 1

/* max continous bits which should be no more than max CU load sizes */
#define MAXCONTIBITS ((512 * 1024) / ALIGNOF_CUSIZE)

/* 1000 */
#define META_SYNC0_BITMASK 8U
/* 0100 */
#define META_BACKUP0_BITMASK 4
/* 0010 */
#define META_SYNC1_BITMASK 2U
/* 0001 */
#define META_BACKUP1_BITMASK 1

/* Number of bits allocated for each bcm block. */
#define META_BITS_PER_BLOCK 4

/* Number of bcm blocks we can represent in one byte. */
#define META_BLOCKS_PER_BYTE 2

/* Number of blocks we can represent in one mata page. */
#define META_BLOCKS_PER_PAGE (BCMMAPSIZE * META_BLOCKS_PER_BYTE)

/*
 * Mapping from bcm block number to the right bit in the right bcm meta page
 * Page0 is for bcm file header, so logically the bcm file is just like as follow:
 * Page0-Meta1-Page11-Page12-...Page1N-Meta2-Page21-Page22...
 */
#define BCMBLK_TO_BCMGROUP(x) (((x)-1) / (META_BLOCKS_PER_PAGE + 1))
#define BCMBLK_TO_METABLOCK(x) (BCMBLK_TO_BCMGROUP(x) * (META_BLOCKS_PER_PAGE + 1) + 1)
#define BCMBLK_TO_METABYTE(x) (((x)-BCMBLK_TO_METABLOCK(x) - 1) / META_BLOCKS_PER_BYTE)
#define BCMBLK_TO_METABIT(x) (((x)-BCMBLK_TO_METABLOCK(x) - 1) % META_BLOCKS_PER_BYTE)

/* Mapping from heap block number to the right bit in the BCM map */
/* Page 0 is for bcm file header, Page 1 is for meta page, so we begin from page 2 */
#define UNITBLK_TO_BCMGROUP(x) ((x) / BCM_BLOCKS_PER_PAGE / META_BLOCKS_PER_PAGE)
#define HEAPBLK_TO_BCMBLOCK(x) (((x) / BCM_BLOCKS_PER_PAGE) + UNITBLK_TO_BCMGROUP(x) + 2)
#define HEAPBLK_TO_BCMBYTE(x) (((x) % BCM_BLOCKS_PER_PAGE) / BCM_BLOCKS_PER_BYTE)
#define HEAPBLK_TO_BCMBIT(x) ((x) % BCM_BLOCKS_PER_BYTE)

#define SET_SYNC_BYTE_STATUS(byte, status, bshift)               \
    do {                                                         \
        unsigned char byteval = byte;                            \
        byteval &= ~(unsigned char)(BCM_SYNC_BITMASK << bshift); \
        byteval |= (status << (bshift + 1));                     \
        byte = byteval;                                          \
    } while (0)

#define SET_SYNC0_BYTE_STATUS(byte, status, bshift)                \
    do {                                                           \
        unsigned char byteval = byte;                              \
        byteval &= ~(unsigned char)(META_SYNC0_BITMASK << bshift); \
        byteval |= (status << (bshift + 3));                       \
        byte = byteval;                                            \
    } while (0)

#define SET_SYNC1_BYTE_STATUS(byte, status, bshift)                \
    do {                                                           \
        unsigned char byteval = byte;                              \
        byteval &= ~(unsigned char)(META_SYNC1_BITMASK << bshift); \
        byteval |= (status << (bshift + 1));                       \
        byte = byteval;                                            \
    } while (0)

/*
 * The algorithm is very clear, we devide this into two parts:
 * 1. the total heap blocks in previous actual bcm pages.
 * 2. the total heap blocks in current bcm page
 */
#define GET_HEAP_BLOCK(block, byte, bshift) \
    ((block - BCMBLK_TO_BCMGROUP(block) - 2) * BCM_BLOCKS_PER_PAGE + (byte)*BCM_BLOCKS_PER_BYTE + (bshift))

/* Here the block should be meta block */
#define GET_BCM_BLOCK(block, byte, bshift) (block + (byte)*META_BLOCKS_PER_BYTE + (bshift) + 1)

/* Create a bcm file and init the file header */
extern void createBCMFile(Relation rel);

/* record one bcm xlog for cu */
extern void BCMLogCU(Relation rel, uint64 offset, int col, BCMBitStatus status, int count);

/* Set the corresponding bit of the block as status */
extern void BCMSetStatusBit(Relation rel, uint64 heapBlk, Buffer buf, BCMBitStatus status, int col = 0);

/* BCM clear */
extern void BCMClearRel(Relation rel, int col = 0);
extern void BCM_truncate(Relation rel);

/* BCM Traversal */
extern void GetBcmFileList(bool clear);
extern void GetIncrementalBcmFileList();

/* BCM xlog for setStatusBit is (RM_HEAP2_ID, XLOG_HEAP2_BCM) type */
/* Functions in heapam.cpp: log_heap_bcm(...);heap_xlog_bcm(...)*/

/* BCM pin */
extern void BCM_pin(Relation rel, BlockNumber heapBlk, Buffer* bcmbuf);

extern void BCM_CStore_pin(Relation rel, int col, uint64 offset, Buffer* buf);

extern void check_cu_block(char* mem, int size, int alignSize);

extern uint64 cstore_offset_to_cstoreblock(uint64 offset, uint64 align_size);

extern uint64 cstore_offset_to_bcmblock(uint64 offset, uint64 align_size);

#endif
