/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * instr_mfchain.h
 *   functions for full/slow SQL in standby
 *
 * IDENTIFICATION
 *    src/include/instruments/instr_mfchain.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef MFCHAIN_H
#define MFCHAIN_H

#include "c.h"
#include "utils/palloc.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
#include "access/htup.h"
/*
 * README
 * Item:
 *      We consider one data as an item. 
 * Block:
 *      A logical structure, manages a 16M buffer and file where item stored in.
 * Chain:
 *      Manages the double linked list composed of blocks. 
 */
struct MemFileItem;
struct MemFileBlock;
struct MemFileChain;

#define MFBLOCK_NAME_LENGTH   64
#define MFBLOCK_VERSION       1
#define MFBLOCK_HEADER_SIZE   4096        // 4 * 1024B = 4KB
#define MFBLOCK_SIZE          16777216    // 16 * 1024 * 1024B = 16MB

#define MFCHAIN_INVALID_ID    0
#define MFCHAIN_FIRST_ID      1

#define MIN_MBLOCK_NUM        1          // 16M
#define MAX_MBLOCK_NUM        64         // 1G
#define MIN_FBLOCK_NUM        32         // 512M
#define MAX_FBLOCK_NUM        65536      // 1T

/* ------------------------------------------------
 * MEM-FILE ITEM
 * ------------------------------------------------
 * MemFileItem  the format in which we store data.
 */
typedef struct MemFileItem {
    uint32 len;                         // length of whole MemFileItem
    char data[FLEXIBLE_ARRAY_MEMBER];   // store the data
} MemFileItem;

#define GetMemFileItemDataLen(item) (((MemFileItem*)(item))->len - sizeof(uint32))


/* ------------------------------------------------
 * MEM-FILE BLOCK
 * ------------------------------------------------
 *
 *  FORMAT OF MemFileBlockBuff like:  [[ 4K header ][    16M - 4K body   ]]
 *     It is a 16MB file, first 4KB using for store MemFileBlockBuffHeader, it describ information of this
 *     block, so do not set any pointer in MemFileBlockBuffHeader. Others for Items, We store it from back
 *     to front, read it from front to back.
 * 
 *  Notice that: 
 *     The content of MemFileBlockBuffHeader mabey changed between two database version, so we use 
 *     MFBLOCK_VERSION to distinguish it.
 */
typedef struct SimpleTupleDesc {
    int natts;
    Oid attrs[FLEXIBLE_ARRAY_MEMBER];
} SimpleTupleDesc;

typedef struct MemFileBlockBuffHeader {
    int  version;
    TimestampTz createTime;
    TimestampTz flushTime;
    uint32 firstItem;               // offset of first item
    uint32 checksum;
    SimpleTupleDesc sDesc;
} MemFileBlockBuffHeader;

typedef struct MemFileBlockBuff {
    char data[MFBLOCK_SIZE];
} MemFileBlockBuff;

typedef enum MemFileBlockStat {
    MFBLOCK_IN_MEMORY,               // include: buffer
    MFBLOCK_IN_BOTH,                 // include: buffer + file
    MFBLOCK_IN_FILE,                 // include:          file
    MFBLOCK_DELETED                  // include: nothing
} MemFileBlockStat;

typedef struct MemFileBlock {
    struct MemFileChain* parent;
    MemFileBlockStat state;

    uint32 id;                        // also file name, we can find path in parent mfchain
    TimestampTz createTime;           // create time of this block
    TimestampTz flushTime;            // flush time of this block

    MemFileBlockBuff* buff;           // point to the 16M memory buffer
    volatile char* firstItem;         // point of the first item in buff
    char* barrier1;                   // barrier pointer of the body and header in the buff, the last bit can wirte
    char* barrier2;                   // point of buffer tail. we cannot touch this byte.

    MemFileBlock* next;
    MemFileBlock* prev;
} MemFileBlock;


/* ------------------------------------------------
 * MEM-FILE CHAIN
 * ------------------------------------------------
 */
typedef enum MemFileChainStat {
    MFCHAIN_STATE_NOT_READY,
    MFCHAIN_STATE_OFFLINE,
    MFCHAIN_STATE_ONLINE
} MemFileChainStat;
typedef struct MemFileChain {
    MemoryContext memCxt;
    LWLock* lock;
    volatile MemFileChainStat state;
    bool needClean;         // when create and destory chain,  save\reload or remove exists file

    const char* path;       // the path in which block file will be stored into
    const char* name;       // name of the chain

    // Memory-File Chain
    MemFileBlock* chainHead;
    MemFileBlock* chainBoundary;
    MemFileBlock* chainTail;
    int blockNumM;           // current num of blocks that has a memory buffer
    int blockNum;            // current num of blocks
    int maxBlockNumM;        // max num of blocks that has a memory buffer
    int maxBlockNum;         // max num of blocks
    int retentionTime;       // the max retention of a block

    // relation information
    Oid relOid;
    SimpleTupleDesc* sDesc;
} MemFileChain;

/*
 * just a temp struct using for Create
 * there are two steps.
 *     1. create but not init. it will just create and return a empty MemFileChain struction in parent context,
 *        so the MemFileChain state will be not ready.
 *     2. create and init. the initTarget is that we created before, we will create a new memorycontext as child
 *        of parent, and init all things of initTarget in it.
 */
typedef struct MemFileChainCreateParam {
    bool notInit;
    MemFileChain* initTarget;

    const char* dir;
    const char* name;
    MemoryContext parent;

    int maxBlockNumM;
    int maxBlockNum;
    int retentionTime;

    Relation rel;
    LWLock* lock;

    bool needClean;
} MemFileChainCreateParam;

typedef enum MemFileChainRegulateType {
    MFCHAIN_TRIM_OLDEST,           // trim out of date blocks
    MFCHAIN_ASSIGN_NEW_SIZE        // trim to size and assign it permanently
} MemFileChainRegulateType;

/* create a mfchain, see MemFileChainCreateParam before */
extern MemFileChain* MemFileChainCreate(MemFileChainCreateParam* param);
/* regulate on/off state automatically, trim blocks that out of date and out of size */
void MemFileChainRegulate(MemFileChain* mfchain, MemFileChainRegulateType type,
    int maxBlockNumM = 0, int maxBlockNum = 0, int retentionTime = 0);
/* destory the mfchain, if set deep, it will delete the block file, otherwise only memory will be free */
extern void MemFileChainDestory(MemFileChain* mfchain);
/* insert a HeapTuple into mfchain */
extern bool MemFileChainInsert(MemFileChain* mfchain, HeapTuple tup, Relation rel);


/* ------------------------------------------------
 * SCAN
 * ------------------------------------------------
 * We using a Scanner to scan a mfchain
 */
typedef struct MemFileChainScanner {
    MemFileChain* mfchain;

    // desc scan progress
    bool scanDone;                    // all block is scan done
    TimestampTz range[2];             // time range, scan from range[0] to range[1]
    uint32 nextBlockId;               // id of next block to scan
    MemFileBlock* nextBlock;          // point to next block of the chain

    // desc Block, buffer and point
    MemFileBlockBuff* buff;           // A buffer, store current block to scan
    char* nextItem;                   // nextItem to read in buffer
    char* barrier;                    // tail of buff, means read over, we cannot read this byte
} MemFileChainScanner;

extern MemFileChainScanner* MemFileChainScanStart(MemFileChain* mfchain, TimestampTz time1 = 0, TimestampTz time2 = 0);
extern MemFileItem* MemFileChainScanGetNext(MemFileChainScanner* scanner);
extern void MemFileChainScanEnd(MemFileChainScanner* scanner);

#endif /* MFCHAIN_H */
