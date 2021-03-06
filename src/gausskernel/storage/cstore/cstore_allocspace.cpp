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
 * cstore_allocspace.cpp
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_allocspace.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/cstore_am.h"
#include "access/genam.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "storage/cstore/cstorealloc.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "storage/lmgr.h"
#include "storage/lock/lwlock.h"
#include "utils/aiomem.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"

HTAB* CStoreColspaceCache = NULL;

typedef struct {
    CStoreColumnFileTag tag;
    uint64 maxOffset;
    uint32 maxCuid;
    uint64 extendOffset;
} CStoreColFileDesc;

Size CStoreAllocatorShmSize()
{
    Size size = 0;
    size = add_size(size, hash_estimate_size(256, sizeof(CStoreColFileDesc)));
    return size;
}

void CStoreAllocator::InitColSpaceCache(void)
{
    HASHCTL ctl;

    if (CStoreColspaceCache == NULL) {
        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "", "");
        ctl.keysize = sizeof(CStoreColumnFileTag);
        ctl.entrysize = sizeof(CStoreColFileDesc);
        ctl.hash = tag_hash;
        CStoreColspaceCache =
            HeapMemInitHash("CStore Column Space Cache", 40960, 81920, &ctl, HASH_ELEM | HASH_FUNCTION);
        if (CStoreColspaceCache == NULL)
            ereport(PANIC, (errmsg("could not initialize CStore Column space desc hash table")));
    }
}

void CStoreAllocator::ResetColSpaceCache(void)
{
    if (CStoreColspaceCache != NULL) {
        HeapMemResetHash(CStoreColspaceCache, "CStore Column Space Cache");
    }
}

CStoreAllocator::CStoreAllocator()
{}

CStoreAllocator::~CStoreAllocator()
{}

uint32 CStoreAllocator::GetNextCUID(Relation rel)
{
    bool found = false;
    CStoreColFileDesc* entry = NULL;
    uint32 cuid = InValidCUID;
    CStoreColumnFileTag tag(rel->rd_node, VirtualSpaceCacheColID, MAIN_FORKNUM);

    (void)LWLockAcquire(CStoreColspaceCacheLock, LW_EXCLUSIVE);
    entry = (CStoreColFileDesc*)hash_search(CStoreColspaceCache, (void*)&tag, HASH_FIND, &found);
    Assert(found);
    cuid = entry->maxCuid;
    if (cuid == MaxCUID)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("No CUID is left for new CU in relation \"%u\". Please execute the VACUUM FULL before do "
                        "anything else",
                        rel->rd_id)));
    if (cuid > CUIDWarningThreshold && cuid < MaxCUID)
        ereport(WARNING, (errmsg("CUID is almost to be used up in relation \"%u\"", rel->rd_id)));
    entry->maxCuid++;
    LWLockRelease(CStoreColspaceCacheLock);
    return cuid;
}

/*
 * @Description: calculate extend size when allocate space
 * @Param[IN] cu_offset: max cu offset
 * @Param[IN] cu_size: write cu size
 * @Param[IN] extend_offset: record extend offset
 * @Return:0 -- no need extend, others extend size
 * @See also:
 */
uint32 CStoreAllocator::CalcExtendSize(uint64 cu_offset, uint32 cu_size, uint64 extend_offset)
{
    uint32 extend_segment = (uint32)(u_sess->attr.attr_storage.fast_extend_file_size * 1024LL);
    uint32 extend_size = 0;
    uint32 need_file_size = 0;
    uint32 left_extend_size = 0;

    Assert(cu_offset <= extend_offset);
    left_extend_size = extend_offset - cu_offset;

    if (cu_size <= left_extend_size) {
        return 0;  // no need fast entend
    }

    need_file_size = cu_size - left_extend_size;
    if (need_file_size <= extend_segment) {
        extend_size = extend_segment;
    } else {
        uint32 remainder = need_file_size % extend_segment;
        extend_size = need_file_size + extend_segment - remainder;
    }

    return extend_size;
}

/*
 * @Description: allocate file size
 * @Param[IN] extend_offset: cu pointer
 * @Param[IN] size: cu size
 * @See also:
 */
uint32 CStoreAllocator::AcquireFileSpace(const CFileNode& cnode, uint64 extend_offset, uint64 cu_offset, uint32 cu_size)
{
    uint32 extend_size = 0;
    CUStorage* cuStorage = New(CurrentMemoryContext) CUStorage(cnode);

    ADIO_RUN()
    {
        if (u_sess->attr.attr_sql.enable_fast_allocate) {
            extend_size = CStoreAllocator::CalcExtendSize(cu_offset, (uint32)cu_size, extend_offset);
            if (extend_size != 0) {
                cuStorage->FastExtendFile(extend_offset, extend_size, true);
            }
            cuStorage->FastExtendFile(cu_offset, cu_size, false);
        } else {
            char* buffer = (char*)adio_align_alloc(cu_size);
            errno_t rc = memset_s(buffer, cu_size, 0, cu_size);
            securec_check(rc, "\0", "\0");
            cuStorage->SaveCU(buffer, cu_offset, cu_size, true);
            adio_align_free(buffer);
            extend_size = cu_size;
        }
    }
    ADIO_ELSE()
    {
        char* buffer = (char*)palloc0(cu_size);
        cuStorage->SaveCU(buffer, cu_offset, cu_size, false, true);
        pfree(buffer);
        buffer = NULL;
        extend_size = cu_size;
    }
    ADIO_END();

    DELETE_EX(cuStorage);

    return extend_size;
}

uint64 CStoreAllocator::AcquireSpace(const CFileNode& cnode, Size size, int align_size)
{
    Assert(align_size > 0);
    bool found = false;
    CStoreColFileDesc* entry = NULL;
    uint64 offset = InvalidCStoreOffset;
    uint32 extend_size = 0;

    LWLockAcquire(CStoreColspaceCacheLock, LW_EXCLUSIVE);
    entry = (CStoreColFileDesc*)hash_search(CStoreColspaceCache, (const void*)&cnode, HASH_FIND, &found);
    Assert(found);
    if (found) {
        offset = entry->maxOffset;
        // when upgrade, last cu need add padding. so cu_point must align backward
        int remainder = offset % align_size;
        if (remainder != 0) {
            ereport(WARNING, (errmsg("AcquireSpace: find un align size(%lu)", offset)));
            offset = offset + align_size - remainder;
            entry->maxOffset = offset;
        }

        // must finish fast extend here before update, because we can not leave hole
        extend_size = CStoreAllocator::AcquireFileSpace(cnode, entry->extendOffset, entry->maxOffset, size);

        entry->maxOffset += size;
        entry->extendOffset += extend_size;
    }
    LWLockRelease(CStoreColspaceCacheLock);
    return offset;
}

uint64 CStoreAllocator::TryAcquireSpaceFromFSM(CStoreFreeSpace* fsm, Size size, int align_size)
{
    CStoreFreeSpaceDesc desc;
    uint64 offset = InvalidCStoreOffset;

    Assert(fsm != NULL);
    Assert(align_size > 0);
    if (!fsm->HasEnoughSpace(size + align_size))
        return offset;

    fsm->PopDescWithMaxSize(desc);
    offset = desc.beginOffset;

    // when upgrade, last cu need add padding. so cu_point must align backward
    int remainder = offset % align_size;
    if (remainder != 0) {
        ereport(WARNING, (errmsg("TryAcquireSpaceFromFSM: find un align size(%lu)", offset)));
        offset = offset + align_size - remainder;
        desc.beginOffset = offset;
        desc.size -= remainder;
    }

    desc.beginOffset += size;
    desc.size -= size;
    if (desc.size > 0)
        fsm->Push(desc);
    return offset;
}

void CStoreAllocator::LockRelForAcquireSpace(Relation rel)
{
    LockRelationForExtension(rel, ExclusiveLock);
}

void CStoreAllocator::ReleaseRelForAcquireSpace(Relation rel)
{
    UnlockRelationForExtension(rel, ExclusiveLock);
}

void CStoreAllocator::InvalidColSpaceCache(const CFileNode& cnode)
{
    LWLockAcquire(CStoreColspaceCacheLock, LW_EXCLUSIVE);
    hash_search(CStoreColspaceCache, (void*)&cnode, HASH_REMOVE, NULL);
    LWLockRelease(CStoreColspaceCacheLock);
}

// build space cache for attrno[ attrNum ].
void CStoreAllocator::BuildColSpaceCacheForRel(_in_ Relation heapRel,
                                                _in_ AttrNumber* attrIds,  // equal to attrno[]
                                                _in_ int attrNum, _in_ List* indexRel)
{
    CFileNode* cFileNode = (CFileNode*)palloc(sizeof(CFileNode) * attrNum);
    for (int i = 0; i < attrNum; ++i) {
        cFileNode[i].m_rnode = heapRel->rd_node;
        cFileNode[i].m_forkNum = MAIN_FORKNUM;
        cFileNode[i].m_attid = attrIds[i];
    }

    if (!CStoreAllocator::ColSpaceCacheExist(cFileNode, attrNum)) {
        uint64* offset = (uint64*)palloc(sizeof(uint64) * attrNum);

        // it's very important to make maxCUID and all the maxCUPointers the newest and biggest.
        // so lock and forbit this relation inserting new tuples, see also SaveAll() method.
        //
        LockRelationForExtension(heapRel, ExclusiveLock);

        Oid cudesOid = heapRel->rd_rel->relcudescrelid;
        uint32 maxCUID = CStore::GetMaxCUID(cudesOid, heapRel->rd_att) + 1;

        /* If there is gin/btree index, we need to find the biggest CU ID stored in the btree index. */
        if (indexRel != NULL) {
            uint32 maxIdxCUID = CStore::GetMaxIndexCUID(heapRel, indexRel) + 1;
            if (maxIdxCUID > maxCUID)
                maxCUID = maxIdxCUID;
        }
        for (int i = 0; i < attrNum; ++i) {
            if (!heapRel->rd_att->attrs[i]->attisdropped) {
                offset[i] = CStore::GetMaxCUPointer(attrIds[i], heapRel);
            } else {
                offset[i] = 0;
            }
        }
        CStoreAllocator::BuildColSpaceCacheForRel(cFileNode, attrNum, offset, maxCUID);

        UnlockRelationForExtension(heapRel, ExclusiveLock);

        pfree_ext(offset);
    }

    pfree_ext(cFileNode);
}

/*
 * @Description: calc fast extend offset
 * @Param[IN] max_offset: max cu_pointer of the file
 * @Return: extend offset
 * @See also:
 */
uint64 CStoreAllocator::GetExtendOffset(uint64 max_offset)
{
    int extend_segment = (int)(u_sess->attr.attr_storage.fast_extend_file_size * 1024LL);
    uint64 offset = CU_FILE_OFFSET(max_offset);
    uint64 remainder = offset % extend_segment;
    if (remainder != 0) {
        max_offset = max_offset + extend_segment - remainder;
    }
    return max_offset;
}

void CStoreAllocator::BuildColSpaceCacheForRel(const CFileNode* cnodes, int nColumn, uint64* offsets, uint32 maxCUID)
{
    CFileNode tag(cnodes[0].m_rnode, VirtualSpaceCacheColID, MAIN_FORKNUM);
    bool found = false;
    CStoreColFileDesc* entry = NULL;

    LWLockAcquire(CStoreColspaceCacheLock, LW_EXCLUSIVE);

    // We should check if all cache entries of the relation are valid.
    // If not, update them.
    // If yes, skip.
    //
    entry = (CStoreColFileDesc*)hash_search(CStoreColspaceCache, (void*)&tag, HASH_ENTER, &found);
    if (entry == NULL)
        ereport(PANIC, (errmsg("build global column space cache hash table failed")));

    // Other session has insert some columns or all columns into hash table
    // !!!Note that we reuse variable 'found'
    //
    if (found) {
        for (int i = 0; i < nColumn; i++) {
            hash_search(CStoreColspaceCache, (void*)&cnodes[i], HASH_FIND, &found);

            // Other session has insert some columns
            // It is incomplete
            //
            if (!found)
                break;
        }
    }

    if (!found) {
        entry->maxCuid = maxCUID;
        entry->maxOffset = InvalidCStoreOffset;

        for (int i = 0; i < nColumn; i++) {
            entry = (CStoreColFileDesc*)hash_search(CStoreColspaceCache, (void*)&cnodes[i], HASH_ENTER, NULL);
            if (entry == NULL)
                ereport(PANIC, (errmsg("build global column space cache hash table failed")));
            entry->maxOffset = offsets[i];
            entry->maxCuid = InValidCUID;
            entry->extendOffset = CStoreAllocator::GetExtendOffset(entry->maxOffset);
        }
    }

    LWLockRelease(CStoreColspaceCacheLock);
}

bool CStoreAllocator::ColSpaceCacheExist(const CFileNode* cnodes, int nColumn)
{
    bool found = false;

    LWLockAcquire(CStoreColspaceCacheLock, LW_SHARED);
    for (int i = 0; i < nColumn; i++) {
        hash_search(CStoreColspaceCache, (void*)&cnodes[i], HASH_FIND, &found);
        if (!found)
            break;
    }

    LWLockRelease(CStoreColspaceCacheLock);
    return found;
}

/**
 * science we loose lock after get max cuid we will doubt the max cuid system 
 * here we recheck max cuid located in index
 * we want to make sure if there is on another larger cuid in index 
 */
uint32 CStoreAllocator::recheck_max_cuid(Relation m_rel, uint32 max_cuid, int index_num, Relation* m_idxRelation)
{
    bool find = false;
    List* index_rel_list = NIL;

    for (int i = 0; i < index_num; ++i) {
        Oid am_oid = m_idxRelation[i]->rd_rel->relam;
        if (am_oid == CBTREE_AM_OID || am_oid == CGIN_AM_OID) {
            index_rel_list = lappend(index_rel_list, m_idxRelation[i]);
        }
    }

    if (list_length(index_rel_list) == 0) {
        return max_cuid;
    }
    uint32 max_idx_cuid = CStore::GetMaxIndexCUID(m_rel, index_rel_list) + 1;
    list_free_ext(index_rel_list);
    
    if (max_idx_cuid == MaxCUID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
        errmsg("No CUID is left for new CU in relation \"%u\".", m_rel->rd_id)));
    }
    
    if (max_idx_cuid > max_cuid) {
        CStoreColFileDesc* entry = NULL;
        CStoreColumnFileTag tag(m_rel->rd_node, VirtualSpaceCacheColID, MAIN_FORKNUM);
        (void)LWLockAcquire(CStoreColspaceCacheLock, LW_EXCLUSIVE);
        entry = (CStoreColFileDesc*)hash_search(CStoreColspaceCache, (void*)&tag, HASH_FIND, &find);
        Assert(find);
        entry->maxCuid = max_idx_cuid + 1;
        LWLockRelease(CStoreColspaceCacheLock);
        return max_idx_cuid;
    }
    return max_cuid;
}
    

void CStoreFreeSpace::Initialize(int maxSize)
{
    m_maxSize = maxSize;
    m_descNum = 0;
    m_descs = (CStoreFreeSpaceDesc*)palloc0(sizeof(CStoreFreeSpaceDesc) * (m_maxSize + 1));
}

CStoreFreeSpace::~CStoreFreeSpace()
{
    m_descs = NULL;
}

void CStoreFreeSpace::Destroy()
{
    pfree(m_descs);
    m_descs = NULL;
}

void CStoreFreeSpace::Push(const CStoreFreeSpaceDesc& desc)
{
    int i;

    if (m_maxSize == m_descNum)
        return;

    i = ++m_descNum;
    while (i != 1 && desc.size > m_descs[i / 2].size) {
        m_descs[i] = m_descs[i / 2];
        i /= 2;
    }
    m_descs[i] = desc;
}

void CStoreFreeSpace::PopDescWithMaxSize(CStoreFreeSpaceDesc& desc)
{
    CStoreFreeSpaceDesc tmp;
    int i = 1;
    int subi = 2;

    if (m_descNum == 0)
        return;

    desc = m_descs[1];
    tmp = m_descs[m_descNum--];

    while (subi <= m_descNum) {
        if (subi < m_descNum && m_descs[subi].size < m_descs[subi + 1].size)
            subi++;
        if (tmp.size >= m_descs[subi].size)
            break;
        m_descs[i] = m_descs[subi];
        i = subi;
        subi *= 2;
    }
    m_descs[i] = tmp;
}

void CStoreFreeSpace::GetDescWithMaxSize(_out_ CStoreFreeSpaceDesc& desc)
{
    if (m_descNum == 0)
        desc.size = ~0;
    else
        desc = m_descs[1];
}

bool CStoreFreeSpace::HasEnoughSpace(Size size)
{
    return IsEmpty() ? false : size <= m_descs[1].size;
}

// compute free space data for the *attrno* attribute, which
// belongs to the relation specified by *cudescHeapRel*.
// *cudescIndexRel* used to index-scan.
//
void CStoreFreeSpace::ComputeFreeSpace(
    _in_ AttrNumber attrno, _in_ Relation cudescHeapRel, _in_ Relation cudescIndexRel, __inout CStoreFreeSpace* fspace)
{
    bool isnull = false;
    List* beginOffsetOrderedList = NIL;
    ListCell* currCell = NULL;
    ListCell* prevCell = NULL;
    ListCell* nextCell = NULL;
    TupleDesc cudescTupDesc = RelationGetDescr(cudescHeapRel);
#ifdef USE_ASSERT_CHECKING
    List* tupList = NIL;
#endif

    // Setup scan key to fetch from the index by col_id.
    ScanKeyData key;
    ScanKeyInit(&key, (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attrno));

    // DIRTY snapshot will be used so that we can get the newest data.
    SnapshotData SnapshotDirty;
    InitDirtySnapshot(SnapshotDirty);
    SysScanDesc cudescScan = systable_beginscan_ordered(cudescHeapRel, cudescIndexRel, &SnapshotDirty, 1, &key);

    // Step 1:
    //   Scan the CUDesc of column from the CUDesc table, and put them in a list
    //   ordered by beginoffset. And then merge the CUDesc if
    //     1). desc1.beginoffset + desc1.size == desc2.beginoffset, or
    //     2). desc2.beginoffset + desc2.size == desc1.beginoffset
    // Note: if the number of holds in the column > MaxNumOfHoleFSM, we should
    //     give up the scan, and go back to the 'APPEND_ONLY'.
    //
    HeapTuple tup = NULL;
    while ((tup = systable_getnext_ordered(cudescScan, BackwardScanDirection)) != NULL) {
        CStoreSpaceDesc spaceDesc;

        char* cuPointer = DatumGetPointer(fastgetattr(tup, CUDescCUPointerAttr, cudescTupDesc, &isnull));

        // skip cuPointer is null
        if (isnull)
            continue;
        Assert(cuPointer);

        spaceDesc.beginOffset = *((uint64*)VARDATA_ANY(cuPointer));
        spaceDesc.size = DatumGetInt32(fastgetattr(tup, CUDescSizeAttr, cudescTupDesc, &isnull));

        // skip those special CUs with total NULL or the SAME value.
        if (spaceDesc.size == 0)
            continue;

#ifdef USE_ASSERT_CHECKING
        HeapTuple tupForCheck = heap_copytuple(tup);
        tupList = lappend(tupList, tupForCheck);
#endif

        // try to merge the descs.
        for (currCell = list_head(beginOffsetOrderedList), prevCell = NULL; currCell != NULL; currCell = nextCell) {
            CStoreSpaceDesc* curEntry = (CStoreSpaceDesc*)lfirst(currCell);
            nextCell = lnext(currCell);
            Assert(spaceDesc.beginOffset != curEntry->beginOffset);

            // if   |--  curEntry  --|-- spaceDesc --|-- nextEntry --|
            // then |------      curEntry      ------|-- nextEntry --|
            // then |------------        curEntry        ------------|
            if (curEntry->beginOffset + curEntry->size == spaceDesc.beginOffset) {
                curEntry->size += spaceDesc.size;
                if (nextCell != NULL) {
                    CStoreSpaceDesc* nextEntry = (CStoreSpaceDesc*)lfirst(nextCell);
                    if (nextEntry->beginOffset == curEntry->beginOffset + curEntry->size) {
                        curEntry->size += nextEntry->size;
                        beginOffsetOrderedList = list_delete_cell(beginOffsetOrderedList, nextCell, currCell);
                        pfree(nextEntry);
                    }
                }

                // mark spaceDesc as handled.
                spaceDesc.beginOffset = InvalidCStoreOffset;
                break;
            }

            // do Insertion sort
            if (spaceDesc.beginOffset < curEntry->beginOffset) {
                // if	|--  spaceDesc  --|-- curEntry --|
                // then |------ 	 curEntry	   ------|
                if (spaceDesc.beginOffset + spaceDesc.size == curEntry->beginOffset) {
                    curEntry->beginOffset = spaceDesc.beginOffset;
                    curEntry->size += spaceDesc.size;
                } else {
                    CStoreSpaceDesc* newEntry = (CStoreSpaceDesc*)palloc0(sizeof(CStoreSpaceDesc));
                    newEntry->beginOffset = spaceDesc.beginOffset;
                    newEntry->size = spaceDesc.size;
                    if (prevCell == NULL) {
                        beginOffsetOrderedList = list_concat(lappend(NIL, newEntry), beginOffsetOrderedList);
                    } else {
                        lappend_cell(beginOffsetOrderedList, prevCell, newEntry);
                    }

                    // check the size of beginOffsetOrderedList to avoid too many space
                    if (list_length(beginOffsetOrderedList) > MaxNumOfHoleFSM)
                        goto scan_end;
                }

                // mark spaceDesc as handled.
                spaceDesc.beginOffset = InvalidCStoreOffset;
                break;
            }

            Assert(spaceDesc.beginOffset > curEntry->beginOffset + curEntry->size);
            prevCell = currCell;
        }

        // by here, we should append the spaceDesc at the end of list.
        if (spaceDesc.beginOffset != InvalidCStoreOffset) {
            CStoreSpaceDesc* newEntry = (CStoreSpaceDesc*)palloc0(sizeof(CStoreSpaceDesc));
            newEntry->beginOffset = spaceDesc.beginOffset;
            newEntry->size = spaceDesc.size;
            beginOffsetOrderedList = lappend(beginOffsetOrderedList, newEntry);

            if (list_length(beginOffsetOrderedList) > MaxNumOfHoleFSM)
                goto scan_end;
        }
    }

    // Step2 : Calculate the space hole of the column
    // Check if there's a hole at the begin of Column.
    currCell = list_head(beginOffsetOrderedList);
    if (currCell != NULL) {
        CStoreSpaceDesc* entry = (CStoreSpaceDesc*)lfirst(currCell);
        if (entry->beginOffset > MinAvailableCStoreFSMSize) {
            CStoreFreeSpaceDesc desc;
            desc.beginOffset = 0;
            desc.size = entry->beginOffset;
            fspace->Push(desc);
        }
    }

    // Calculate the space hole of the column
    for (currCell = list_head(beginOffsetOrderedList); currCell != NULL; currCell = nextCell) {
        CStoreSpaceDesc *curEntry = NULL, *nextEntry = NULL;
        nextCell = lnext(currCell);
        if (nextCell == NULL)
            break;

        curEntry = (CStoreSpaceDesc*)lfirst(currCell);
        nextEntry = (CStoreSpaceDesc*)lfirst(nextCell);
        Assert(curEntry->beginOffset < nextEntry->beginOffset);

        if (curEntry->beginOffset + curEntry->size + MinAvailableCStoreFSMSize < nextEntry->beginOffset) {
            CStoreFreeSpaceDesc freeSpace;
            freeSpace.beginOffset = curEntry->beginOffset + curEntry->size;
            freeSpace.size = nextEntry->beginOffset - (curEntry->beginOffset + curEntry->size);

            fspace->Push(freeSpace);
            if (fspace->IsFull())
                break;
        }
    }

scan_end:

#ifdef USE_ASSERT_CHECKING
    list_free_deep(tupList);
    tupList = NIL;
#endif

    list_free_deep(beginOffsetOrderedList);
    beginOffsetOrderedList = NIL;

    systable_endscan_ordered(cudescScan);
    cudescScan = NULL;
}
