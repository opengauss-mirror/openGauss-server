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
 * cstorealloc.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/cstore/cstorealloc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_ALLOC_H
#define CSTORE_ALLOC_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/relfilenode.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "cstore.h"

typedef struct {
    uint64 beginOffset;
    uint64 size;
} CStoreFreeSpaceDesc;

typedef CFileNode CStoreColumnFileTag;
typedef CStoreFreeSpaceDesc CStoreSpaceDesc;

#define DEFAULT_NUM_OF_FREE_SPACE_SLOTS 2000
#define InvalidCStoreFreeSpace 0xffffffffffffffff
#define InvalidCStoreOffset InvalidCStoreFreeSpace
#define MinAvailableCStoreFSMSize 1024

/*Defines max number of holds in column.
 *  If there are too many holds, we should exec vacuum and do not use the free space.
 */
#define MaxNumOfHoleFSM (100 * 1024 * 1024 / 16)

#define IsValidCStoreFreeSpaceDesc(desc) ((desc)->size != InvalidCStoreFreeSpace)

extern Size CStoreAllocatorShmSize();

class CStoreFreeSpace : public BaseObject {
public:
    CStoreFreeSpace(int maxSize)
    {
        Initialize(maxSize);
    };
    CStoreFreeSpace()
    {
        Initialize(DEFAULT_NUM_OF_FREE_SPACE_SLOTS);
    };
    virtual ~CStoreFreeSpace();
    virtual void Destroy();

    void Push(_in_ const CStoreFreeSpaceDesc& desc);

    void PopDescWithMaxSize(_out_ CStoreFreeSpaceDesc& desc);

    void GetDescWithMaxSize(_out_ CStoreFreeSpaceDesc& desc);

    bool IsEmpty()
    {
        return m_descNum == 0;
    }

    bool IsFull()
    {
        return m_descNum == m_maxSize;
    }

    bool HasEnoughSpace(Size size);

public:  // STATIC METHODS
    static void ComputeFreeSpace(_in_ AttrNumber attno, _in_ Relation cudescIndexRel, _in_ Relation cudescHeapRel,
        __inout CStoreFreeSpace* fspace);

private:
    void Initialize(int maxSize);

    CStoreFreeSpaceDesc* m_descs;
    int m_descNum;
    int m_maxSize;
};

class CStoreAllocator : public BaseObject {

private:
    CStoreAllocator();
    virtual ~CStoreAllocator();

public:
    static void InitColSpaceCache(void);
    static void ResetColSpaceCache(void);

    static uint32 GetNextCUID(Relation rel);

    // Now we need lock the relation before acquire space.
    static void LockRelForAcquireSpace(Relation rel);

    static void ReleaseRelForAcquireSpace(Relation rel);

    static uint32 AcquireFileSpace(const CFileNode& cnode, uint64 extend_offset, uint64 cu_offset, uint32 cu_size);
    // Acquire space for CU
    static uint64 AcquireSpace(const CFileNode& cnode, Size size, int align_size);

    // Acquire space for CU from free space
    static uint64 TryAcquireSpaceFromFSM(CStoreFreeSpace* fsm, Size size, int align_size);

    // Invalid column cache
    static void InvalidColSpaceCache(const CFileNode& cnode);

    static void BuildColSpaceCacheForRel(const CFileNode* cnodes, int nColumn, uint64* offsets, uint32 maxCUID);
    static void BuildColSpaceCacheForRel(
        _in_ Relation heapRel, _in_ AttrNumber* attrIds, _in_ int attrNum, _in_ List* btreeIndex = NIL);

    static bool ColSpaceCacheExist(const CFileNode* cnodes, int nColumn);

    static uint64 GetExtendOffset(uint64 max_offset);
    static uint32 CalcExtendSize(uint64 cu_offset, uint32 cu_size, uint64 extend_offset);
};

#endif
