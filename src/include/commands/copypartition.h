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
 * copypartition.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/copypartition.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef COPYPARTITION_H
#define COPYPARTITION_H

#define MAX_BUFFERED_TUPLES 20000
#define MAX_TUPLES_SIZE (1024 * 1024)

#define MIN_MEMCXT_NUM (16)
#define MAX_MEMCXT_NUM (1024)
#define PART_NUM_PER_MEMCXT (4)
#define LMT_SIZE_PER_PART (BLCKSZ * 4)
#define MAX_SIZE_PER_MEMCXT (LMT_SIZE_PER_PART * PART_NUM_PER_MEMCXT)
#define MAX_HASH_ENTRY_NUM (PART_NUM_PER_MEMCXT * MAX_MEMCXT_NUM)
#define DEF_BUFFERED_TUPLES (128)

struct CopyFromMemCxtData;
typedef struct CopyFromMemCxtData* CopyFromMemCxt;
typedef struct CopyFromManagerData* CopyFromManager;
typedef struct CopyFromBulkData* CopyFromBulk;

typedef struct CopyFromMemCxtData {
    /* used for tuples' memory in the partition chunk */
    MemoryContext memCxtCandidate;
    uint32 memCxtSize;

    /* members for bulk chunk */
    int16 nextBulk; /* nextBulk < PART_NUM_PER_MEMCXT, otherwise this chunk is full */
    int16 id;
    CopyFromBulk chunk[PART_NUM_PER_MEMCXT];
} CopyFromMemCxtData;

typedef struct CopyFromBulkKey
{
	/* the hash key */
	Oid				partOid;
	int4			bucketId;
} CopyFromBulkKey;

typedef struct CopyFromBulkData {
    /* the hash key */
    Oid  partOid;
	int4 bucketId;

    /* the other entry info in hash table */
    int numTuples;
    int maxTuples;
    // XXXTAM: replace below with generic Tuple
    Tuple* tuples;
    Tuple* utuples;
    Size sizeTuples;
    /* bulk-insert  Memory Context */
    CopyFromMemCxt memCxt;
} CopyFromBulkData;

typedef struct CopyFromManagerData {
    /* members under PARTITION condition */
    MemoryContext parent;
    CopyFromMemCxt* memCxt;
    uint16 numMemCxt;
    uint16 maxMemCxt;
    uint16 nextMemCxt;
    uint16 numFreeMemCxt;
    uint16 freeMemCxt[MAX_MEMCXT_NUM];

    HTAB* hash;

    /* members without PARTITION */
    bool isPartRel;
    bool LastFlush;
    bool isUHeapRel;
    CopyFromBulk bulk;
    CopyFromBulkKey	switchKey;	
} CopyFromManagerData;

extern void CopyFromBulkInsert(EState* estate, CopyFromBulk bulk, PageCompress* pcState, CommandId mycid,
    int hi_options, ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, bool forceToFlush, BulkInsertState bistate);

extern CopyFromBulk findBulk(CopyFromManager mgr, Oid partOid, int2 bucketId, bool *toFlush);
extern bool isBulkFull(CopyFromBulk bulk);
template<bool isInsertSelect> void AddToBulk(CopyFromBulk bulk, HeapTuple tup, bool needCopy);
template<bool isInsertSelect> void AddToUHeapBulk(CopyFromBulk bulk, UHeapTuple tup, bool needCopy);

template<bool isInsertSelect>
bool CopyFromChunkInsert(CopyState cstate, EState* estate, CopyFromBulk bulk, CopyFromManager mgr,
    PageCompress* pCState, CommandId mycid, int hiOptions, ResultRelInfo* resultRelInfo, TupleTableSlot* myslot,
    BulkInsertState bistate)
{
    int cnt;
    CopyFromMemCxt copyFromMemCxt = bulk->memCxt;
    MemoryContext oldCxt;

    if (!mgr->isPartRel) {

        if (isInsertSelect) {
            CopyFromBulkInsert(estate, bulk, pCState, mycid, hiOptions, resultRelInfo, myslot, mgr->LastFlush, bistate);
            if (bulk->numTuples == 0) {
                MemoryContextReset(copyFromMemCxt->memCxtCandidate);
                copyFromMemCxt->memCxtSize = 0;
            }
        } else
            CopyFromBulkInsert(estate, bulk, pCState, mycid, hiOptions, resultRelInfo, myslot, mgr->LastFlush, bistate);
        return (0 == bulk->numTuples);
    }

    Assert(((InvalidOid != bulk->partOid) && (InvalidOid == mgr->switchKey.partOid)) ||
           ((InvalidOid == bulk->partOid) && (InvalidOid != mgr->switchKey.partOid)));
    for (cnt = 0; cnt < copyFromMemCxt->nextBulk; cnt++) {
        CopyFromBulk bulkItem = copyFromMemCxt->chunk[cnt];
        CopyFromBulkKey key = {bulkItem->partOid, bulkItem->bucketId};
        bool found = false;
        CopyFromBulkInsert(estate, bulkItem, pCState, mycid, hiOptions, resultRelInfo, myslot, true, bistate);
        (void)hash_search(mgr->hash, &key, HASH_REMOVE, &found);
        Assert(found);
    }

    /* reset CopyFromMemCxt */
    copyFromMemCxt->nextBulk = 0;
    MemoryContextReset(copyFromMemCxt->memCxtCandidate);
    copyFromMemCxt->memCxtSize = 0;

    if (InvalidOid == mgr->switchKey.partOid) {
        /* add to freelist */
        mgr->freeMemCxt[mgr->numFreeMemCxt++] = copyFromMemCxt->id;
    } else {
        copyFromMemCxt->chunk[copyFromMemCxt->nextBulk++] = bulk;

        /* reset Bulk, excluding memCxt */
        bulk->partOid = mgr->switchKey.partOid;
        bulk->bucketId = mgr->switchKey.bucketId;
		mgr->switchKey.partOid = InvalidOid;
		mgr->switchKey.bucketId = InvalidBktId;
        bulk->maxTuples = DEF_BUFFERED_TUPLES;
        oldCxt = MemoryContextSwitchTo(copyFromMemCxt->memCxtCandidate);

        bulk->tuples = (Tuple *)palloc(sizeof(Tuple) * bulk->maxTuples);

        (void)MemoryContextSwitchTo(oldCxt);
    }

    return true;
}
#endif
