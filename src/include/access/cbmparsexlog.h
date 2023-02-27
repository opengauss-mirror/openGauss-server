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
 * cbmparsexlog.h
 *      Definitions for cbm-parsing-xlog facility
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/cbmparsexlog.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CBMPARSEXLOG_H
#define CBMPARSEXLOG_H

#include "access/xlogrecord.h"
#include "c.h"
#include "lib/dllist.h"
#include "port/pg_crc32c.h"
#include "securec_check.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "utils/hsearch.h"

/* Struct for single bitmap file information */
typedef struct BitmapFileStruct {
    char name[MAXPGPATH]; /* Name with full path */
    int fd;               /* Handle to opened file */
    uint64 size;          /* Size of the file */
    off_t offset;         /* Offset of the next read */
} BitmapFile;

#define CBMDIR "pg_cbm/"
#define MAX_CBM_FILENAME_LENGTH 66

/* CBM xlog read structure */
typedef struct CBMXlogReadStruct {
    int fd;
    XLogSegNo logSegNo;
    char filePath[MAXPGPATH];
} CBMXlogRead;

/* Xlog parsing and bitmap output data structure */
typedef struct XlogBitmapStruct {
    char cbmFileHome[MAXPGPATH]; /* directory for bitmap files */
    BitmapFile out;              /* The current bitmap file */
    uint64 outSeqNum;            /* the bitmap file sequence number */
    XLogRecPtr startLSN;         /* the LSN of the next unparsed
                             record and the start of the next LSN
                             interval to be parsed.  */
    XLogRecPtr endLSN;           /* the end of the LSN interval to be
                                 parsed, equal to the next checkpoint
                                 LSN at the time of parse */
    HTAB* cbmPageHash;           /* the current modified page set,
                                organized as htab with the keys of
                                (rNode, forkNum) pairs */
    Dllist pageFreeList;         /* doublely-linked list of freed CBM pages */
    uint64 totalPageNum;         /* total cbm page number during one parse cycle */
    CBMXlogRead xlogRead;        /* file information of xlog files to be parsed */
    bool xlogParseFailed;        /* true if failed during last xlog parse */
    bool needReset;              /* true if failed during last CBMFollowXlog */
    bool firstCPCreated;         /* if first checkpoint has been created after recovery */
} XlogBitmap;

typedef struct cbmpageheader {
    pg_crc32c pageCrc;
    bool isLastBlock;
    uint8 pageType;
    XLogRecPtr pageStartLsn;
    XLogRecPtr pageEndLsn;
    RelFileNode rNode;
    ForkNumber forkNum;
    BlockNumber firstBlkNo;
    BlockNumber truncBlkNo;
} CbmPageHeader;

typedef struct XLogPageReadPrivateCbm {
    const char* datadir;
    TimeLineID tli;
} XLogPageReadPrivateCBM;

#define PAGETYPE_DROP 0x04
#define PAGETYPE_CREATE 0x02
#define PAGETYPE_TRUNCATE 0x01
#define PAGETYPE_MODIFY 0x00

/* a segment of data file shared a list */
#define BLOCK_NUM_PER_CBMLIST 131072

#define GET_SEG_INDEX_FROM_BLOCK_NUM(block_num) ((block_num)/(BLOCK_NUM_PER_CBMLIST))

/* Max size of each bitmap file, in bytes. */
#define MAXCBMFILESIZE (128 * 1024 * 1024)

/* Size of the bitmap page, in bytes. */
#define CBMPAGESIZE 512

/* Size of the bitmap on each cbm map page, in bytes. */
#define CBMMAPSIZE (CBMPAGESIZE - MAXALIGN(sizeof(CbmPageHeader)))

/* Max total size of all reserved free pages, in bytes. */
#define MAXCBMFREEPAGESIZE (64 * 1024 * 1024)

/* Max total number of all reserved free pages. */
#define MAXCBMFREEPAGENUM (MAXCBMFREEPAGESIZE / CBMPAGESIZE)

/* Number of bits allocated for each heap block. */
#define CBM_BITS_PER_BLOCK 1

/* Number of heap blocks we can represent in one byte. */
#define CBM_BLOCKS_PER_BYTE 8

/* Number of blocks we can represent in one cbm map page. */
#define CBM_BLOCKS_PER_PAGE (CBMMAPSIZE * CBM_BLOCKS_PER_BYTE)

#define BLKNO_TO_CBM_PAGEFIRSTBOCK(x) (BlockNumberIsValid(x) ? ((x) - ((x) % CBM_BLOCKS_PER_PAGE)) : InvalidBlockNumber)
#define BLKNO_TO_CBMBYTEOFPAGE(x) (((x) % CBM_BLOCKS_PER_PAGE) / CBM_BLOCKS_PER_BYTE)
#define BLKNO_TO_CBMBITOFBYTE(x) ((x) % CBM_BLOCKS_PER_BYTE)

#define CBM_PAGE_NOT_CHANGED 0
#define CBM_PAGE_CHANGED ((uint32)1)

#define CBM_PAGE_BITMASK ((uint32)1)

#define SET_CBM_PAGE_BITMAP(byte, bit, status) \
    do {                                       \
        char byteval = byte;                   \
        byteval &= ~(CBM_PAGE_BITMASK << bit); \
        byteval |= (status << bit);            \
        byte = byteval;                        \
    } while (0)

#define CLEAR_CBM_PAGE_BITMAP(byte, bit) (byte) &= ~(CBM_PAGE_BITMASK << (bit))

#define CBM_BLOCKNO_CMP(a, b) \
    ((a) == (b) ? 0 : ((a) == InvalidBlockNumber ? -1 : ((b) == InvalidBlockNumber ? 1 : ((a) < (b) ? -1 : 1))))

/*
 * Note: if there are any pad bytes in the struct, INIT_CBMPAGETAG have
 * to be fixed to zero them, since this struct is used as a hash key.
 */
typedef struct cbmpagetag {
    RelFileNode rNode;
    ForkNumber forkNum;
} CBMPageTag;

#define INIT_CBMPAGETAG(a, xx_rnode, xx_forkNum) ((a).rNode = (xx_rnode), (a).forkNum = (xx_forkNum))

#define InvalidRelFileNode ((RelFileNode){0, 0, 0, -1})

#define INIT_DUMMYCBMPAGETAG(a) ((a).rNode = InvalidRelFileNode, (a).forkNum = InvalidForkNumber)

#define CBMPAGETAG_EQUAL(a, b) (RelFileNodeEquals((a).rNode, (b).rNode) && (a).forkNum == (b).forkNum)

typedef struct cbmhashentry {
    CBMPageTag cbmTag;
    Dllist cbmSegPageList;
    int pageNum;
} CbmHashEntry;

typedef struct cbmsegpagelist {
    int segIndex;
    Dllist pageDllist;
} CbmSegPageList;

#define INIT_CBMPAGEENTRY(a)            \
    do {                                \
        (a)->pageNum = 0;               \
        DLInitList(&((a)->cbmSegPageList)); \
    } while (0)

#define INIT_CBMPAGEHEADER(a, xx_pagetag, xx_firstblkno) \
    ((a)->rNode = (xx_pagetag.rNode),                    \
        (a)->forkNum = (xx_pagetag.forkNum),             \
        (a)->firstBlkNo = (xx_firstblkno),               \
        (a)->pageType = PAGETYPE_MODIFY,                 \
        (a)->truncBlkNo = InvalidBlockNumber)

#define CBM_PAGE_IS_DUMMY(a) ((RelFileNodeEquals((a)->rNode, InvalidRelFileNode)) && ((a)->forkNum == InvalidForkNumber))

#define INITCBMPAGEHASHSIZE 400

/* Struct for cbm filename information */
typedef struct cbmfilename {
    char* name;          /* file name pointer */
    uint64 seqNum;       /* file sequence number */
    XLogRecPtr startLSN; /* file start lsn */
    XLogRecPtr endLSN;   /* file end lsn */
} CbmFileName;

#define INIT_CBMFILENAME(a, xx_name, xx_seqnum, xx_startlsn, xx_endlsn) \
    do {                                                                \
        (a)->name = pstrdup(xx_name);                                   \
        (a)->seqNum = xx_seqnum;                                        \
        (a)->startLSN = xx_startlsn;                                    \
        (a)->endLSN = xx_endlsn;                                        \
    } while (0)

typedef struct cbmpageiterator {
    FILE* file;
    off_t readOffset;
    char* buffer;
    XLogRecPtr pageStartLsn;
    XLogRecPtr pageEndLsn;
    bool isLastBlock;
    bool checksumOk;
    XLogRecPtr prevStartLsn;
    XLogRecPtr prevEndLsn;
    bool isPrevLastBlock;
} cbmPageIterator;

#define INIT_CBMPAGEITERATOR(a, xx_file, xx_buffer) \
    ((a).file = (xx_file),                          \
        (a).readOffset = (off_t)0,                  \
        (a).buffer = (xx_buffer),                   \
        (a).pageStartLsn = InvalidXLogRecPtr,       \
        (a).pageEndLsn = InvalidXLogRecPtr,         \
        (a).checksumOk = false,                     \
        (a).isLastBlock = false,                    \
        (a).prevStartLsn = InvalidXLogRecPtr,       \
        (a).prevEndLsn = InvalidXLogRecPtr,         \
        (a).isPrevLastBlock = false)

typedef struct cbmarrayentry {
    CBMPageTag cbmTag;
    uint8 changeType;
    BlockNumber truncBlockNum;
    uint32 totalBlockNum;
    uint32 maxSize;
    BlockNumber* changedBlock;
} CBMArrayEntry;

#define INIT_CBMARRAYENTRY(a, xx_cbmTag)         \
    do {                                         \
        (a)->cbmTag = (xx_cbmTag);               \
        (a)->changeType = PAGETYPE_MODIFY;       \
        (a)->truncBlockNum = InvalidBlockNumber; \
        (a)->totalBlockNum = 0;                  \
        InitCBMArrayEntryBlockArray(a);          \
    } while (0)

typedef struct cbmarray {
    XLogRecPtr startLSN;
    XLogRecPtr endLSN;
    long arrayLength;
    CBMArrayEntry* arrayEntry;
} CBMArray;

#define INITBLOCKARRAYSIZE 16

typedef struct cbmbitmapiterator {
    char* bitmap;
    BlockNumber nextBlkNo;
    BlockNumber startBlkNo;
    BlockNumber endBlkNo;
} CBMBitmapIterator;

/* for block number printed in tuple */
#define MAX_STRLEN_PER_BLOCKNO 12
#define MAX_BLOCKNO_PER_TUPLE ((MaxAllocSize - 1) / MAX_STRLEN_PER_BLOCKNO)

#define INIT_CBMBITMAPITERATOR(a, xx_bitmap, xx_start, xx_end) \
    ((a).bitmap = (xx_bitmap), (a).nextBlkNo = (xx_start), (a).startBlkNo = (xx_start), (a).endBlkNo = (xx_end))

extern void InitXlogCbmSys(void);
extern void CBMTrackInit(bool startupXlog, XLogRecPtr startupCPRedo);
extern void ResetXlogCbmSys(void);
extern void CBMFollowXlog(void);
extern void CBMGetMergedFile(XLogRecPtr startLSN, XLogRecPtr endLSN, char* mergedFileName);
extern CBMArray* CBMGetMergedArray(XLogRecPtr startLSN, XLogRecPtr endLSN);
extern void FreeCBMArray(CBMArray* cbmArray);
extern CBMArray *SplitCBMArray(CBMArray **orgCBMArrayPtr);
extern void CBMRecycleFile(XLogRecPtr targetLSN, XLogRecPtr* endLSN);
extern XLogRecPtr ForceTrackCBMOnce(XLogRecPtr targetLSN, int timeOut, bool wait, bool lockHeld, bool isRecEnd = true);
extern void advanceXlogPtrToNextPageIfNeeded(XLogRecPtr* recPtr);
extern void cbm_rotate_file(XLogRecPtr rotateLsn);

#endif
