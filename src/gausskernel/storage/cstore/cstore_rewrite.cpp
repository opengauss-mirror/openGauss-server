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
 * cstore_rewrite.cpp
 *     routinues to support Column Store Rewrite
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_rewrite.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/cstore_rewrite.h"
#include "catalog/catalog.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "replication/dataqueue.h"
#include "utils/aiomem.h"
#include "utils/gs_bitmap.h"
#include "utils/syscache.h"
#include "commands/tablecmds.h"
#include "catalog/objectaccess.h"
#include "access/cstore_insert.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "catalog/dependency.h"
#include "utils/lsyscache.h"
#include "catalog/index.h"
#include "storage/remote_read.h"
#include "utils/snapmgr.h"

#define IsBitmapSet(_bitmap, _i) (((_bitmap)[(_i) >> 3] & (1 << ((_i) % 8))) != 0)

#define ARRAY_2_LEN 2

static HeapTuple CreateFakeHeapTup(TupleDesc tupDesc, int attIdx, Datum attVal)
{
    int nAttrs = tupDesc->natts;
    Datum values[nAttrs];
    bool isnull[nAttrs];

    for (int k = 0; k < nAttrs; ++k) {
        values[k] = (Datum)0;
        isnull[k] = true;
    }

    if (attIdx >= 0 && attIdx < nAttrs) {
        isnull[attIdx] = false;
        values[attIdx] = attVal;
    }

    return heap_form_tuple(tupDesc, values, isnull);
}

CStoreRewriteColumn* CStoreRewriteColumn::CreateForAddColumn(AttrNumber attNo)
{
    CStoreRewriteColumn* item = (CStoreRewriteColumn*)palloc(sizeof(CStoreRewriteColumn));

    item->attrno = attNo;
    item->isDropped = false;
    item->isAdded = true;
    item->notNull = false;
    item->newValue = NULL;

    return item;
}

CStoreRewriteColumn* CStoreRewriteColumn::CreateForSetDataType(AttrNumber attNo)
{
    CStoreRewriteColumn* item = (CStoreRewriteColumn*)palloc(sizeof(CStoreRewriteColumn));

    item->attrno = attNo;
    item->isDropped = false;
    item->isAdded = false;
    item->notNull = false;
    item->newValue = NULL;

    return item;
}

void CStoreRewriteColumn::Destroy(CStoreRewriteColumn** rewriteInfo)
{
    Assert(rewriteInfo != NULL);
    Assert(*rewriteInfo != NULL);
    pfree_ext(*rewriteInfo);
}

HeapBulkInsert::HeapBulkInsert(Relation heapRel)
{
    m_HeapRel = heapRel;

    /*
     * the default value is HEAP_INSERT_FROZEN; it means don't set (TABLE_INSERT_SKIP_FSM | TABLE_INSERT_SKIP_WAL).
     * TABLE_INSERT_FROZEN means ,after modify table , other sessions can always see it.
     */
    m_InsertOpt = TABLE_INSERT_FROZEN;
    if (heapRel->rd_createSubid != InvalidSubTransactionId ||
        heapRel->rd_newRelfilenodeSubid != InvalidSubTransactionId) {
        m_InsertOpt |= TABLE_INSERT_SKIP_FSM;
        if (!XLogIsNeeded()) {
            m_InsertOpt |= TABLE_INSERT_SKIP_WAL;
        }
    }

    /* Notice: keep the following object in parent memory context. */
    m_BIState = GetBulkInsertState();
    m_CmdId = GetCurrentCommandId(true);
    m_OldMemCnxt = NULL;
    m_MemCnxt = AllocSetContextCreate(CurrentMemoryContext,
                                      "HeapBulkInsert Memmory",
                                      ALLOCSET_DEFAULT_MINSIZE,
                                      ALLOCSET_DEFAULT_INITSIZE,
                                      ALLOCSET_DEFAULT_MAXSIZE);

    m_BufferedTupsSize = 0;
    m_BufferedTupsNum = 0;
    m_BufferedTups = (HeapTuple*)palloc0(sizeof(HeapTuple) * MaxBufferedTupNum);
}

void HeapBulkInsert::Destroy()
{
    m_HeapRel = NULL;
    m_InsertOpt = 0x7fffffff;
    m_CmdId = 0xffffffff;

    FreeBulkInsertState(m_BIState);
    m_BIState = NULL;

    Assert(m_OldMemCnxt == NULL);
    MemoryContextDelete(m_MemCnxt);
    m_MemCnxt = NULL;

    pfree_ext(m_BufferedTups);
    m_BufferedTupsSize = 0;
    m_BufferedTupsNum = 0;
}

FORCE_INLINE bool HeapBulkInsert::Full() const
{
    return ((m_BufferedTupsNum == MaxBufferedTupNum) || (m_BufferedTupsSize > MaxBufferedTupSize));
}

template <bool needCopy>
void HeapBulkInsert::Append(HeapTuple tuple)
{
    // add *this* prefix, so sourceinsight can hilight these memthods.
    MemoryContext oldMemCnxt = MemoryContextSwitchTo(this->m_MemCnxt);

    // buffer the input tuples as more as possible.
    this->m_BufferedTups[this->m_BufferedTupsNum++] = needCopy ? heap_copytuple(tuple) : tuple;
    this->m_BufferedTupsSize += tuple->t_len;

    if (this->Full()) {
        this->Flush();
    }

    (void)MemoryContextSwitchTo(oldMemCnxt);
}

FORCE_INLINE void HeapBulkInsert::Flush()
{
    Assert(m_BufferedTupsNum > 0);

    /* 1. not use page compression
     * 2. not use page replication
     */
    HeapMultiInsertExtraArgs args = {NULL, 0, true};

    (void)tableam_tuple_multi_insert(m_HeapRel, m_HeapRel, (Tuple*)m_BufferedTups, m_BufferedTupsNum, m_CmdId,
        m_InsertOpt, m_BIState, &args);

    MemoryContextReset(m_MemCnxt);
    m_BufferedTupsNum = 0;
    m_BufferedTupsSize = 0;
}

void HeapBulkInsert::BulkInsert(HeapTuple tuple)
{
    Append<false>(tuple);
}

void HeapBulkInsert::BulkInsertCopy(HeapTuple tuple)
{
    Append<true>(tuple);
}

void HeapBulkInsert::Finish()
{
    if (m_BufferedTupsNum > 0) {
        Flush();
    }
}

CStoreRewriter::CStoreRewriter(Relation oldHeapRel, TupleDesc oldTupDesc, TupleDesc newTupDesc)
    : m_OldHeapRel(oldHeapRel),
      m_OldTupDesc(oldTupDesc),
      m_NewTupDesc(newTupDesc),
      m_NewCuDescHeap(InvalidOid),
      m_ColsRewriteFlag(NULL),
      m_TblspcChanged(false),
      m_TargetTblspc(InvalidOid),
      m_TargetRelFileNode(InvalidOid),
      m_CUReplicationRel(NULL),
      m_SDTColAppendOffset(NULL),
      m_AddColsNum(0),
      m_AddColsInfo(NULL),
      m_AddColsStorage(NULL),
      m_AddColsMinMaxFunc(NULL),
      m_AddColsAppendOffset(NULL),
      m_SDTColsNum(0),
      m_SDTColsInfo(NULL),
      m_SDTColsReader(NULL),
      m_SDTColsMinMaxFunc(NULL),
      m_SDTColsWriter(NULL),
      m_SDTColValues(NULL),
      m_SDTColIsNull(NULL),
      m_NewCudescRel(NULL),
      m_NewCudescBulkInsert(NULL),
      m_NewCudescFrozenXid(InvalidTransactionId)
{
    Assert(newTupDesc->natts >= oldTupDesc->natts);
    m_estate = CreateExecutorState();
    m_econtext = GetPerTupleExprContext(m_estate);

    /*
     * default value is the current old tablespace.
     * Since spcid is always from a pg_class tuple, InvalidOid implies the default.
     */
    m_TargetTblspc = ConvertToRelfilenodeTblspcOid(oldHeapRel->rd_rel->reltablespace);
    m_TargetRelFileNode = oldHeapRel->rd_node.relNode;
}

void CStoreRewriter::Destroy()
{
    // m_econtext will be released during freeing m_estate.
    FreeExecutorState(m_estate);
    m_estate = NULL;
    m_econtext = NULL;
}

/*
 * description: future plan-when rewriting the table isn't needed, we may should check
 *   whether the existing data violate the new constraints or not.
 */
FORCE_INLINE bool CStoreRewriter::NeedRewrite() const
{
    return (m_AddColsNum > 0 || m_SDTColsNum > 0);
}

/* Change tablespace and set the new CU Replication relation.
 * All new tablespace oid and relfilenode oid will be got from CUReplicationRel.
 */
void CStoreRewriter::ChangeTableSpace(Relation CUReplicationRel)
{
    Assert(OidIsValid(CUReplicationRel->rd_node.relNode));
    Assert(m_TargetTblspc != CUReplicationRel->rd_node.spcNode);

    /* tablespace and relfilenode are changed, and remember them. */
    m_TblspcChanged = true;
    m_TargetTblspc = CUReplicationRel->rd_node.spcNode;
    m_TargetRelFileNode = CUReplicationRel->rd_node.relNode;

    /* refer to new CU replication relation */
    m_CUReplicationRel = CUReplicationRel;
}

void CStoreRewriter::BeginRewriteCols(_in_ int nRewriteCols, _in_ CStoreRewriteColumn** pRewriteCols,
                                      _in_ const int* rewriteColsNum, _in_ bool* rewriteFlags)
{
    Assert(nRewriteCols == m_NewTupDesc->natts);
    m_AddColsNum = rewriteColsNum[CSRT_ADD_COL];
    m_SDTColsNum = rewriteColsNum[CSRT_SET_DATA_TYPE];
    m_ColsRewriteFlag = rewriteFlags;

    if (!NeedRewrite())
        return;

    if (m_AddColsNum > 0) {
        AddColumnInitPhrase1();
    }

    if (m_SDTColsNum > 0) {
        SetDataTypeInitPhase1();
    }

    int nAddCols = 0;
    int nChangeCols = 0;

    for (int i = 0; i < nRewriteCols; ++i) {
        if (pRewriteCols[i] == NULL) {
            Assert(!rewriteFlags[i]);
            continue;
        }

        Assert(rewriteFlags[i]);
        if (pRewriteCols[i]->isAdded) {
            AddColumnInitPhrase2(pRewriteCols[i], nAddCols);
            ++nAddCols;
        } else if (!pRewriteCols[i]->isDropped) {
            SetDataTypeInitPhase2(pRewriteCols[i], nChangeCols);
            ++nChangeCols;
        } else
            /* description: future plan-dropped columns case */
            ;
    }
    Assert(nAddCols == m_AddColsNum);
    Assert(nChangeCols == m_SDTColsNum);

    // create new cudesc relation for rewriting
    Oid oldCuDescHeap = m_OldHeapRel->rd_rel->relcudescrelid;
    m_NewCuDescHeap = make_new_heap(oldCuDescHeap, m_TargetTblspc);

    m_NewCudescRel = heap_open(m_NewCuDescHeap, AccessExclusiveLock);
    m_NewCudescBulkInsert = New(CurrentMemoryContext) HeapBulkInsert(m_NewCudescRel);
}

void CStoreRewriter::RewriteColsData()
{
    if (!NeedRewrite())
        return;

    // the first valid CU ID is (FirstCUID + 1) but not FirstCUID.
    uint32 nextCuId = FirstCUID + 1;
    Oid oldCudescOid = m_OldHeapRel->rd_rel->relcudescrelid;
    uint32 maxCuId = CStore::GetMaxCUID(oldCudescOid, m_OldTupDesc);

    // open the CUDESC relation and its index relation
    Relation oldCudescHeap = heap_open(oldCudescOid, AccessExclusiveLock);
    Relation oldCudescIndex = index_open(oldCudescHeap->rd_rel->relcudescidx, AccessExclusiveLock);
    TupleDesc oldCudescTupDesc = oldCudescHeap->rd_att;

    Oid oldCudescToastId = oldCudescHeap->rd_rel->reltoastrelid;
    bool swapToastByContent = false;

    // If the OldHeap has a toast table, get lock on the toast table to keep
    // it from being vacuumed.	This is needed because autovacuum processes
    // toast tables independently of their main tables, with no lock on the
    // latter.	If an autovacuum were to start on the toast table after we
    // compute our OldestXmin below, it would use a later OldestXmin, and then
    // possibly remove as DEAD toast tuples belonging to main tuples we think
    // are only RECENTLY_DEAD.	Then we'd fail while trying to copy those
    // tuples.
    //
    // We don't need to open the toast relation here, just lock it.  The lock
    // will be held till end of transaction.
    //
    if (OidIsValid(oldCudescToastId))
        LockRelationOid(oldCudescToastId, AccessExclusiveLock);

    FetchCudescFrozenXid(oldCudescHeap);

    // If both tables have TOAST tables, perform toast swap by content.  It is
    // possible that the old table has a toast table but the new one doesn't,
    // if toastable columns have been dropped.	In that case we have to do
    // swap by links.  This is okay because swap by content is only essential
    // for system catalogs, and we don't support schema changes for them.
    //
    if (OidIsValid(oldCudescToastId) && OidIsValid(m_NewCudescRel->rd_rel->reltoastrelid)) {
        swapToastByContent = true;

        // When doing swap by content, any toast pointers written into NewHeap
        // must use the old toast table's OID, because that's where the toast
        // data will eventually be found.  Set this up by setting rd_toastoid.
        // This also tells toast_save_datum() to preserve the toast value
        // OIDs, which we want so as not to invalidate toast pointers in
        // system catalog caches, and to avoid making multiple copies of a
        // single toast value.
        //
        // Note that we must hold NewHeap open until we are done writing data,
        // since the relcache will not guarantee to remember this setting once
        // the relation is closed.  Also, this technique depends on the fact
        // that no one will try to read from the NewHeap until after we've
        // finished writing it and swapping the rels --- otherwise they could
        // follow the toast pointers to the wrong place.  (It would actually
        // work for values copied over from the old toast table, but not for
        // any values that we toast which were previously not toasted.)
        //
        m_NewCudescRel->rd_toastoid = oldCudescToastId;
    } else {
        swapToastByContent = false;

        // set rd_toastoid to be InvalidOid explicitly.
        m_NewCudescRel->rd_toastoid = InvalidOid;
    }

    bool isnull = false;
    ScanKeyData key[ARRAY_2_LEN];

    int nOldAttrs = m_OldTupDesc->natts;
    int rc = 0;
    int const cudescTupsLen = sizeof(HeapTuple) * nOldAttrs;
    HeapTuple* cudescTups = (HeapTuple*)palloc(cudescTupsLen);
    HeapTuple virtualDelTup = NULL;
    HeapTuple tup = NULL;

    // only used during the data rewriting.
    // we will reset and free it periodically in the loop.
    MemoryContext oneLoopMemCnxt = AllocSetContextCreate(CurrentMemoryContext,
                                                         "Cstore Rewriting Memmory",
                                                         ALLOCSET_DEFAULT_MINSIZE,
                                                         ALLOCSET_DEFAULT_INITSIZE,
                                                         ALLOCSET_DEFAULT_MAXSIZE);

    while (nextCuId <= maxCuId) {
        // If we got a cancel signal during rewriting data, quit
        CHECK_FOR_INTERRUPTS();

        // at the begin of each loop, reset the spaces
        // used by the previous loop.
        MemoryContextReset(oneLoopMemCnxt);

        // reset all the tuples' points.
        rc = memset_s(cudescTups, cudescTupsLen, 0, cudescTupsLen);
        securec_check(rc, "", "");
        virtualDelTup = NULL;
        tup = NULL;

        // then, switch to the private memory context.
        AutoContextSwitch newMemCnxt(oneLoopMemCnxt);
        int scanedTuples = 0;

        // init scankey
        // explaination: key[0]  --> col_id = 1
        ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(1));

        // explaination: key[1]  --> cu_id = nextCuId
        ScanKeyInit(&key[1], (AttrNumber)CUDescCUIDAttr, BTEqualStrategyNumber, F_OIDEQ, UInt32GetDatum(nextCuId));

        // each col_id of cudesc
        for (int i = 0; i < nOldAttrs; i++) {
            // skip the droped column
            if (unlikely(m_OldTupDesc->attrs[i]->attisdropped))
                continue;

            // explaination: key[0]  --> col_id = 1 to natts
            key[0].sk_argument = Int32GetDatum(i + 1);

            SysScanDesc oldCuDescScan = systable_beginscan_ordered(oldCudescHeap, oldCudescIndex, SnapshotNow, ARRAY_2_LEN, key);
            /* if some existing cu data will be rewrited,
             * the deleted tuple will be skipped and possibly marked NULL simply.
             * so that remember all the tuples with the same cu id and the virtual-delete tuple.
             */
            if ((tup = systable_getnext_ordered(oldCuDescScan, ForwardScanDirection)) != NULL) {
                Assert((i + 1) == DatumGetInt32(fastgetattr(tup, CUDescColIDAttr, oldCudescTupDesc, &isnull)));
                Assert(!isnull);
                Assert(nextCuId == DatumGetUInt32(fastgetattr(tup, CUDescCUIDAttr, oldCudescTupDesc, &isnull)));
                Assert(!isnull);
                Assert(scanedTuples < nOldAttrs);

                // have to copy this tuple, because it will be accessed outside this WHILE loop later.
                cudescTups[i] = heap_copytuple(tup);
                ++scanedTuples;
            }
            systable_endscan_ordered(oldCuDescScan);
            oldCuDescScan = NULL;
        }

        if (0 == scanedTuples) {
            /* this means that, virtual deleted tuple lives only if
             * cudesc tuples about living columns exists. the two
             * live together, or disappear together.
             * skip the unused CU IDs which are wasted by aborted transactions.
             */
            ++nextCuId;

            // the above using memory will be freed by reseting memory context.
            continue;
        }

        // vcu of cudesc
        // explaination: key[0]  --> col_id = VitrualDelColID
        key[0].sk_argument = Int32GetDatum(VitrualDelColID);

        SysScanDesc oldCuDescScan = systable_beginscan_ordered(oldCudescHeap, oldCudescIndex, SnapshotNow, ARRAY_2_LEN, key);
        if ((tup = systable_getnext_ordered(oldCuDescScan, ForwardScanDirection)) != NULL) {
            // this means that only one virtual deleted tuple for one cu.
            Assert(virtualDelTup == NULL);

            // have to copy this tuple, because it will be accessed
            // outside this WHILE loop later.
            virtualDelTup = heap_copytuple(tup);
        }
        systable_endscan_ordered(oldCuDescScan);
        oldCuDescScan = NULL;

        if (virtualDelTup == NULL) {
            Assert(false);
            ereport(ERROR,
                    (errcode(ERRCODE_NO_DATA),
                     errmsg("Relation \'%s\' virtual cudesc tuple(cuid %u) not found",
                            RelationGetRelationName(m_OldHeapRel),
                            nextCuId)));
        }

        // get rows number within this cu.
        uint32 rowCount = DatumGetUInt32(fastgetattr(virtualDelTup, CUDescRowCountAttr, oldCudescTupDesc, &isnull));
        Assert(!isnull);

        // get rows delete-mask data.
        char* delMaskDataPtr = NULL;
        bool wholeCuIsDeleted = false;
        char* delMaskVarDatum =
            DatumGetPointer(fastgetattr(virtualDelTup, CUDescCUPointerAttr, oldCudescTupDesc, &isnull));
        if (!isnull) {
            // notice: if the left is not equal to input argument *delMaskVarDatum*
            //    in the right, we must free this memory at end of this loop.
            //    but in fact at the begin we reset the memory *delMaskVarDatum*
            //    belonged to, so i's fine. please don'tt care memroy leak.
            delMaskVarDatum = (char*)PG_DETOAST_DATUM(delMaskVarDatum);
            Assert(VARSIZE_ANY_EXHDR(delMaskVarDatum) == bitmap_size(rowCount));
            delMaskDataPtr = VARDATA_ANY(delMaskVarDatum);

            wholeCuIsDeleted = CStore::IsTheWholeCuDeleted(delMaskDataPtr, rowCount);
        }

        // ADD COLUMN can ignore the deleted heap tuples, and it's
        // the default value that will be inserted into.
        //
        AddColumns(nextCuId, rowCount, wholeCuIsDeleted);

        // SET DATA TYPE must skip the deleted heap tuples.
        //
        SetDataType(nextCuId, cudescTups, oldCudescTupDesc, delMaskDataPtr, rowCount, wholeCuIsDeleted);

        // insert remaining cudesc tuples of other columns
        // and the virtual-delete tuple into cudesc table.
        //
        for (int i = 0; i < nOldAttrs; ++i) {
            if (m_OldTupDesc->attrs[i]->attisdropped || m_NewTupDesc->attrs[i]->attisdropped)
                continue;

            if (!m_ColsRewriteFlag[i]) {
                m_NewCudescBulkInsert->BulkInsertCopy(cudescTups[i]);
            }
        }

        /* at last we must copy the virtual deleting tuple.
         * notice: we must detoast first and then form the new tuple.
         *    also because we use the private memory context, so it
         *    needn't to copy this tuple again.
         */
        m_NewCudescBulkInsert->EnterBulkMemCnxt();
        m_NewCudescBulkInsert->BulkInsert(CStore::FormVCCUDescTup(
                                              oldCudescTupDesc, delMaskDataPtr, nextCuId, rowCount, GetCurrentTransactionIdIfAny()));
        m_NewCudescBulkInsert->LeaveBulkMemCnxt();

        /* ok, all the tuples are inserted into cudesc table.
         * it's safe not to free cudescTups[] and virtualDelTup,
         * because their memory context will be reset in the front of each loop.
         * we scan all the cudesc tuples in order by increasing CU id by 1.
         */
        ++nextCuId;
    }

    FlushAllCUData();

    m_NewCudescBulkInsert->Finish();
    DELETE_EX(m_NewCudescBulkInsert);

    MemoryContextDelete(oneLoopMemCnxt);
    pfree_ext(cudescTups);

    if (m_TblspcChanged) {
        ereport(LOG,
                (errmsg("Row [Rewrite]: %s(%u) tblspc %u/%u/%u => %u/%u/%u",
                        RelationGetRelationName(oldCudescHeap),
                        RelationGetRelid(oldCudescHeap),
                        oldCudescHeap->rd_node.spcNode,
                        oldCudescHeap->rd_node.dbNode,
                        oldCudescHeap->rd_node.relNode,
                        m_NewCudescRel->rd_node.spcNode,
                        m_NewCudescRel->rd_node.dbNode,
                        m_NewCudescRel->rd_node.relNode)));
    }

    // Reset rd_toastoid just to be tidy before relation is closed.
    // it shouldn't be looked at again.
    m_NewCudescRel->rd_toastoid = InvalidOid;
    heap_close(m_NewCudescRel, NoLock);
    m_NewCudescRel = NULL;

    heap_close(oldCudescHeap, NoLock);
    index_close(oldCudescIndex, NoLock);

    // finish rewriting cudesc relation.
    finish_heap_swap(m_OldHeapRel->rd_rel->relcudescrelid, m_NewCuDescHeap, false,
                     swapToastByContent, false, m_NewCudescFrozenXid, FirstMultiXactId);
}

void CStoreRewriter::EndRewriteCols()
{
    m_ColsRewriteFlag = NULL;
    if (!NeedRewrite())
        return;

    // clean up work at last.
    AddColumnDestroy();
    SetDataTypeDestroy();
}

// called to flush all the data of CUs.
void CStoreRewriter::FlushAllCUData() const
{
    for (int i = 0; i < m_SDTColsNum; ++i) {
        m_SDTColsWriter[i]->FlushDataFile();
    }
    for (int i = 0; i < m_AddColsNum; ++i) {
        m_AddColsStorage[i]->FlushDataFile();
    }
}

void CStoreRewriter::AddColumnInitPhrase1()
{
    Assert(m_AddColsNum > 0);

    m_AddColsInfo = (CStoreRewriteColumn**)palloc(sizeof(CStoreRewriteColumn*) * m_AddColsNum);
    m_AddColsStorage = (CUStorage**)palloc(sizeof(CUStorage*) * m_AddColsNum);
    m_AddColsMinMaxFunc = (FuncSetMinMax*)palloc(sizeof(FuncSetMinMax) * m_AddColsNum);
    m_AddColsAppendOffset = (CUPointer*)palloc(sizeof(CUPointer) * m_AddColsNum);
}

void CStoreRewriter::AddColumnInitPhrase2(_in_ CStoreRewriteColumn* addColInfo, _in_ int idx)
{
    Assert(m_AddColsNum > 0);
    Assert(idx >= 0 && idx < m_AddColsNum);

    m_AddColsInfo[idx] = addColInfo;

    /* Min/Max functions for rough checking. */
    m_AddColsMinMaxFunc[idx] = GetMinMaxFunc(m_NewTupDesc->attrs[addColInfo->attrno - 1]->atttypid);

    /* maybe different tablespace and relfilenode will be used,
     * so we create a new temp RelFileNode object.
     */
    RelFileNode relfilenode = m_OldHeapRel->rd_node;
    relfilenode.spcNode = m_TargetTblspc;
    relfilenode.relNode = m_TargetRelFileNode;
    CFileNode cFileNode(relfilenode, (int)addColInfo->attrno, MAIN_FORKNUM);
    m_AddColsStorage[idx] = New(CurrentMemoryContext) CUStorage(cFileNode);

    if (m_TblspcChanged) {
        ereport(LOG,
                (errmsg("Column [ADD COLUMN]: %s(%u C%d) tblspc %u/%u/%u => %u/%u/%u",
                        RelationGetRelationName(m_OldHeapRel),
                        RelationGetRelid(m_OldHeapRel),
                        (addColInfo->attrno - 1),
                        m_OldHeapRel->rd_node.spcNode,
                        m_OldHeapRel->rd_node.dbNode,
                        m_OldHeapRel->rd_node.relNode,
                        m_TargetTblspc,
                        m_OldHeapRel->rd_node.dbNode,
                        m_TargetRelFileNode)));
    }

    /* IF (relation's tablespace is not changed)
     *   1. create new cu files for rewriting.
     *   2. log and append it into pending delete list.
     *   NB: here set *isRedo* true, because Cxx.0 file maybe exists.
     *      so we have to reuse the exising regular file. test case is:
     *      senssion1: START TRANSACTION;
     *                 ALTER TABLE column_table ADD COLUMN c int default 4;
     *      senssion2: kill -9 all the nodes of the cluster;
     *                 restart the cluster;  -- Cxx.0 file remain in disk.
     *      senssion1: quit and reenter;
     *                 ALTER TABLE column_table ADD COLUMN c int default 4; -- ERROR, file exists
     *
     * ELSE
     *   it must be an new relfilenode.
     */
    LWLockAcquire(RelfilenodeReuseLock, LW_SHARED);
    m_AddColsStorage[idx]->CreateStorage(0, !m_TblspcChanged);
    CStoreRelCreateStorage(
        &cFileNode.m_rnode, addColInfo->attrno, m_OldHeapRel->rd_rel->relpersistence, m_OldHeapRel->rd_rel->relowner);
    LWLockRelease(RelfilenodeReuseLock);

    /* APPEND ONLY method will be used for new cu data. */
    m_AddColsAppendOffset[idx] = 0;
}

// free all the unused resource after ADD COLUMN.
void CStoreRewriter::AddColumnDestroy()
{
    if (m_AddColsNum > 0) {
        for (int i = 0; i < m_AddColsNum; ++i) {
            DELETE_EX(m_AddColsStorage[i]);
        }

        pfree_ext(m_AddColsInfo);
        pfree_ext(m_AddColsStorage);
        pfree_ext(m_AddColsMinMaxFunc);
        pfree_ext(m_AddColsAppendOffset);
    }
}

void CStoreRewriter::AddColumns(_in_ uint32 cuId, _in_ int rowsCntInCu, _in_ bool wholeCuIsDeleted)
{
    Assert(cuId > (uint32)FirstCUID);

    /* description: future plan-remove this branch after WHOLE DELETED CU data can be resued. */
    if (wholeCuIsDeleted) {
        // if the whole cu is deleted, we needn't write any cu data.
        // but cudesc tuples of each column must be formed and inserted into.
        //
        HandleWholeDeletedCu(cuId, rowsCntInCu, m_AddColsNum, m_AddColsInfo);
        return;
    }

    // the new value of adding column shouldn't depend on any other
    // existing columns or their exsiting values, so construct a fake
    // tuple to cheat the func ExecEvalExpr().
    //
    HeapTuple fakeTuple = CreateFakeHeapTup(m_NewTupDesc, -1, 0);
    TupleTableSlot* fakeSlot = MakeSingleTupleTableSlot(m_NewTupDesc);
    (void)ExecStoreTuple(fakeTuple, fakeSlot, InvalidBuffer, false);

    for (int i = 0; i < m_AddColsNum; ++i) {
        CStoreRewriteColumn* newColInfo = m_AddColsInfo[i];
        Assert(newColInfo && newColInfo->isAdded && !newColInfo->isDropped);
        int attrIndex = newColInfo->attrno - 1;
        Form_pg_attribute newColAttr = m_NewTupDesc->attrs[attrIndex];

        CUDesc newColCudesc;
        newColCudesc.cu_id = cuId;
        newColCudesc.row_count = rowsCntInCu;

        Datum newColVal = (Datum)0;
        bool newColValIsNull = !newColInfo->notNull;
        if (newColInfo->newValue) {
            // compute the new value by expression
            m_econtext->ecxt_scantuple = fakeSlot;
            newColVal = ExecEvalExpr(newColInfo->newValue->exprstate, m_econtext, &newColValIsNull, NULL);
        } else if (newColInfo->notNull) {
            // DEFAULT must be defined if NOT NULL exists and
            // the table is not empty now.
            //
            ereport(ERROR,
                    (errcode(ERRCODE_NOT_NULL_VIOLATION),
                     errmsg("column \"%s\" contains null values", NameStr(newColAttr->attname)),
                     errdetail("existing data violate the NOT NULL constraint of new column."),
                     errhint("define DEFAULT constraint also.")));
        }

        newColCudesc.magic = GetCurrentTransactionIdIfAny();

        HandleCuWithSameValue<true>(m_OldHeapRel,
                                    newColAttr,
                                    newColVal,
                                    newColValIsNull,
                                    m_AddColsMinMaxFunc[i],
                                    m_AddColsStorage[i],
                                    &newColCudesc,
                                    m_AddColsAppendOffset + i);

        InsertNewCudescTup(&newColCudesc, RelationGetDescr(m_NewCudescRel), newColAttr);

        ResetExprContext(m_econtext);
    }

    heap_freetuple(fakeTuple);
    fakeTuple = NULL;

    ExecDropSingleTupleTableSlot(fakeSlot);
    fakeSlot = NULL;
}

// Notice: if writing cu data is needed, then
//   1) append = true,  APPEND_ONLY way will be used.
//      so that we increase pOffset directly by new cu size.
//   2) append = false, free space will be first used.
//      *colStorage* will do the real work in fact.
//
// Notice: RelFileNode of this *pColAttr* is up to *rel* argument,
//   and it's used by both SaveCuData() and PushCUToDataQueue(), so
//   it's the caller's responsibility that pass in the right *rel*.
template <bool append>
void CStoreRewriter::HandleCuWithSameValue(_in_ Relation rel, _in_ Form_pg_attribute pColAttr, _in_ Datum colValue,
                                           _in_ bool colIsNull, _in_ FuncSetMinMax MinMaxFunc, _in_ CUStorage* colStorage, __inout CUDesc* pColCudescData,
                                           __inout CUPointer* pOffset)
{
    // step 1: set Cudesc mode. if true is returned, we must write new data into cu file.
    if (CStore::SetCudescModeForTheSameVal(colIsNull, MinMaxFunc, pColAttr->attlen, colValue, pColCudescData)) {
        CU* cuPtr = New(CurrentMemoryContext) CU(pColAttr->attlen, pColAttr->atttypmod, pColAttr->atttypid);

        // step 2: form the new cu data quickly.
        CStoreRewriter::FormCuDataForTheSameVal(cuPtr, pColCudescData->row_count, colValue, pColAttr);

        // step 3: compress cu data in memory, and prepare to write them into cu file.
        int16 compressing_modes = 0;
        heaprel_set_compressing_modes(rel, &compressing_modes);
        CStoreRewriter::CompressCuData(cuPtr, pColCudescData, pColAttr, compressing_modes);

        // step 4: alloc file space for this cu data.
        if (append) {
            pColCudescData->cu_pointer = *pOffset;
            // update *pOffset* for the next writing.
            *pOffset += pColCudescData->cu_size;
        } else {
            Assert(pOffset == NULL);
            pColCudescData->cu_pointer = colStorage->AllocSpace(pColCudescData->cu_size);
        }

        // step 5: save cu data into file in disk.
        CStoreRewriter::SaveCuData(cuPtr, pColCudescData, colStorage);

        // step 6: about HA, push CU data into replicate queue
        CStoreCUReplication((this->m_TblspcChanged ? this->m_CUReplicationRel : rel),
                            pColAttr->attnum,
                            cuPtr->m_compressedBuf,
                            pColCudescData->cu_size,
                            pColCudescData->cu_pointer);

        DELETE_EX(cuPtr);
    } else {
        Assert(pColCudescData->cu_size == 0);
        Assert(pColCudescData->cu_pointer == 0);
    }
}

void CStoreRewriter::FormCuDataForTheSameVal(CU* cuPtr, int rowsCntInCu, Datum newColVal, Form_pg_attribute newColAttr)
{
    const Size valSize = datumGetSize(newColVal, newColAttr->attbyval, newColAttr->attlen);

    // Notice: here we don't skip the deleted heap tuples.
    const Size initSize = valSize * rowsCntInCu;

    // Check the *initSize* overflow.
    if (initSize < UINT32_MAX) {
        // until here it's safe to case *initSize* uint32.
        cuPtr->InitMem((uint32)initSize, rowsCntInCu, false);
        CU::AppendCuData(newColVal, rowsCntInCu, newColAttr, cuPtr);
        return;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("column \"%s\" needs too many memory", NameStr(newColAttr->attname)),
             errdetail("its size of default value is too big.")));
}

/*
 * @Description: compress CU data
 * @IN/ compressing_modes: compressing-modes value
 * @IN/OUT cuDesc: CU Description
 * @IN/OUT cuPtr: CU object
 * @IN newColAttr: new column's attribute
 * @See also:
 */
void CStoreRewriter::CompressCuData(CU* cuPtr, CUDesc* cuDesc, Form_pg_attribute newColAttr, int16 compressing_modes)
{
    /* we will use temp compression_options && cu_tmp_compress_info during rewriting CU file */
    compression_options tmp_filter;
    tmp_filter.reset();

    cu_tmp_compress_info cu_temp_info;
    cu_temp_info.m_options = &tmp_filter;
    cu_temp_info.m_valid_minmax = !NeedToRecomputeMinMax(newColAttr->atttypid);
    if (cu_temp_info.m_valid_minmax) {
        cu_temp_info.m_min_value = ConvertToInt64Data(cuDesc->cu_min, newColAttr->attlen);
        cu_temp_info.m_max_value = ConvertToInt64Data(cuDesc->cu_max, newColAttr->attlen);
    }
    cuPtr->m_tmpinfo = &cu_temp_info;

    cuPtr->SetMagic(cuDesc->magic);
    cuPtr->Compress(cuDesc->row_count, compressing_modes, ALIGNOF_CUSIZE);
    cuDesc->cu_size = cuPtr->GetCUSize();
    Assert(cuDesc->cu_size > 0);
}

FORCE_INLINE
void CStoreRewriter::SaveCuData(CU* cuPtr, CUDesc* cuDesc, CUStorage* cuStorage)
{
    Assert(cuDesc->cu_id >= (uint32)FirstCUID);
    Assert(cuDesc->cu_size > 0);

    cuStorage->SaveCU(cuPtr->m_compressedBuf, cuDesc->cu_pointer, cuDesc->cu_size, false);
}

/* description: future plan-check fetch method of pFreezeXid. refer to copy_heap_data(). */
void CStoreRewriter::FetchCudescFrozenXid(Relation oldCudescHeap)
{
    // compute xids used to freeze and weed out dead tuples.  We use -1
    // freeze_min_age to avoid having CLUSTER freeze tuples earlier than a
    // plain VACUUM would.
    //
    Assert(oldCudescHeap->rd_rel->relisshared == false);
    TransactionId OldestXmin = InvalidTransactionId;
    vacuum_set_xid_limits(oldCudescHeap, -1, -1, &OldestXmin, &m_NewCudescFrozenXid, NULL, NULL);

    // FreezeXid will become the table's new relfrozenxid, and that mustn't go
    // backwards, so take the max.
    //
    bool isNull = false;
    TransactionId oldFrozenXid;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(oldCudescHeap)));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                 errmsg("cache lookup failed for relation %u", RelationGetRelid(oldCudescHeap))));
    }
    Datum xid64datum = heap_getattr(tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
    heap_close(rel, AccessShareLock);
    heap_freetuple(tuple);
    tuple = NULL;

    if (isNull) {
        oldFrozenXid = oldCudescHeap->rd_rel->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, oldFrozenXid))
            oldFrozenXid = FirstNormalTransactionId;
    } else
        oldFrozenXid = DatumGetTransactionId(xid64datum);

    if (TransactionIdPrecedes(m_NewCudescFrozenXid, oldFrozenXid)) {
        m_NewCudescFrozenXid = oldFrozenXid;
    }
}

void CStoreRewriter::SetDataTypeInitPhase1()
{
    Assert(m_SDTColsNum > 0);

    m_SDTColsInfo = (CStoreRewriteColumn**)palloc(sizeof(CStoreRewriteColumn*) * m_SDTColsNum);
    m_SDTColsReader = (CUStorage**)palloc(sizeof(CUStorage*) * m_SDTColsNum);
    m_SDTColsMinMaxFunc = (FuncSetMinMax*)palloc(sizeof(FuncSetMinMax) * m_SDTColsNum);
    m_SDTColsWriter = (CUStorage**)palloc(sizeof(CUStorage*) * m_SDTColsNum);

    m_SDTColValues = (Datum*)palloc(sizeof(Datum) * RelMaxFullCuSize);
    m_SDTColIsNull = (bool*)palloc(sizeof(bool) * RelMaxFullCuSize);

    if (m_TblspcChanged) {
        m_SDTColAppendOffset = (CUPointer*)palloc(sizeof(CUPointer) * m_SDTColsNum);
    }
}

void CStoreRewriter::SetDataTypeInitPhase2(_in_ CStoreRewriteColumn* sdtColInfo, _in_ int idx)
{
    m_SDTColsInfo[idx] = sdtColInfo;

    /* Min/Max functions for columns of changing data type. */
    m_SDTColsMinMaxFunc[idx] = GetMinMaxFunc(m_NewTupDesc->attrs[sdtColInfo->attrno - 1]->atttypid);

    CFileNode cFileNode(m_OldHeapRel->rd_node, (int)sdtColInfo->attrno, MAIN_FORKNUM);
    m_SDTColsReader[idx] = New(CurrentMemoryContext) CUStorage(cFileNode);

    /* when tablespace is not change:
     * 1. don't create new cu file, and just reuse the existing cu file.
     * 2. either overwrite the free spaces within cu file, or append directly.
     *
     * tablespace may be changed, so create a new CFileNode object
     */
    RelFileNode wrtRelFileNode = m_OldHeapRel->rd_node;
    wrtRelFileNode.spcNode = m_TargetTblspc;
    wrtRelFileNode.relNode = m_TargetRelFileNode;
    CFileNode wrtCFileNode(wrtRelFileNode, (int)sdtColInfo->attrno, MAIN_FORKNUM);
    m_SDTColsWriter[idx] = New(CurrentMemoryContext) CUStorage(wrtCFileNode);

    if (m_TblspcChanged) {
        /*
         *	1. create new cu files for rewriting.
         *	2. log and append it into pending delete list.
         */
        m_SDTColsWriter[idx]->CreateStorage(0, false);
        CStoreRelCreateStorage(&wrtCFileNode.m_rnode,
                               sdtColInfo->attrno,
                               m_OldHeapRel->rd_rel->relpersistence,
                               m_OldHeapRel->rd_rel->relowner);

        /* APPEND_ONLY strategy will be used. */
        m_SDTColAppendOffset[idx] = 0;

        ereport(LOG,
                (errmsg("Column [SET TYPE]: %s(%u C%d) tblspc %u/%u/%u => %u/%u/%u",
                        RelationGetRelationName(m_OldHeapRel),
                        RelationGetRelid(m_OldHeapRel),
                        (sdtColInfo->attrno - 1),
                        m_OldHeapRel->rd_node.spcNode,
                        m_OldHeapRel->rd_node.dbNode,
                        m_OldHeapRel->rd_node.relNode,
                        m_TargetTblspc,
                        m_OldHeapRel->rd_node.dbNode,
                        m_TargetRelFileNode)));
    } else {
        /* at first, set column allocate strategy. */
        m_SDTColsWriter[idx]->SetAllocateStrategy(APPEND_ONLY);

        /* and then, build space cache for APPEND_ONLY strategy. */
        CStoreAllocator::BuildColSpaceCacheForRel(m_OldHeapRel, &m_SDTColsInfo[idx]->attrno, 1);
    }

    /* register the relation oid to be changed datatype.
     * do it after finish free space building.
     * APPEND_ONLY will be applied if updating or inserting
     * cu data happen later.
     */
    CStoreAlterRegister* alterReg = GetCstoreAlterReg();
    alterReg->Add(RelationGetRelid(m_OldHeapRel));
}

// free all the unused resource after SET DATA TYPE.
void CStoreRewriter::SetDataTypeDestroy()
{
    if (m_SDTColsNum) {
        for (int i = 0; i < m_SDTColsNum; ++i) {
            DELETE_EX(m_SDTColsReader[i]);
            DELETE_EX(m_SDTColsWriter[i]);
        }

        pfree_ext(m_SDTColsInfo);
        pfree_ext(m_SDTColsReader);
        pfree_ext(m_SDTColsMinMaxFunc);
        pfree_ext(m_SDTColsWriter);
        pfree_ext(m_SDTColValues);
        pfree_ext(m_SDTColIsNull);
        if (m_TblspcChanged) {
            pfree_ext(m_SDTColAppendOffset);
        }
    }
}

void CStoreRewriter::HandleWholeDeletedCu(
    _in_ uint32 cuId, _in_ int rowsCntInCu, _in_ int nRewriteCols, _in_ CStoreRewriteColumn** rewriteColsInfo)
{
    CUDesc fullNullCudesc;
    fullNullCudesc.cu_id = cuId;
    fullNullCudesc.row_count = rowsCntInCu;
    fullNullCudesc.SetNullCU();
    fullNullCudesc.magic = GetCurrentTransactionIdIfAny();

    for (int i = 0; i < nRewriteCols; ++i) {
        InsertNewCudescTup(
            &fullNullCudesc, RelationGetDescr(m_NewCudescRel), m_NewTupDesc->attrs[(rewriteColsInfo[i]->attrno - 1)]);
    }
}

// main body of ALTER COLUMN SET DATA TYPE clause.
void CStoreRewriter::SetDataType(_in_ uint32 cuId, _in_ HeapTuple* cudescTup, _in_ TupleDesc cudescTupDesc,
                                 _in_ const char* delMaskDataPtr, _in_ int rowsCntInCu, _in_ bool wholeCuIsDeleted)
{
    /* description: future plan-remove this branch after WHOLE DELETED CU data can be resued. */
    if (wholeCuIsDeleted) {
        // if the whole cu is deleted, we needn't handle the existing cu data.
        // but cudesc tuples of each column must be formed and inserted into.
        //
        HandleWholeDeletedCu(cuId, rowsCntInCu, m_SDTColsNum, m_SDTColsInfo);
        return;
    }

    DataCacheMgr* cucache = CUCache;

    for (int sdtIndex = 0; sdtIndex < m_SDTColsNum; ++sdtIndex) {
        CStoreRewriteColumn* setDataTypeColInfo = m_SDTColsInfo[sdtIndex];
        Assert(setDataTypeColInfo);
        Assert(!setDataTypeColInfo->isAdded);
        Assert(!setDataTypeColInfo->isDropped);

        int attrIndex = setDataTypeColInfo->attrno - 1;

        // deform the cudesc tuple and set old CUDesc object.
        CUDesc oldColCudesc;
        CStore::DeformCudescTuple(cudescTup[attrIndex], cudescTupDesc, m_OldTupDesc->attrs[attrIndex], &oldColCudesc);
        Assert(oldColCudesc.cu_id == cuId);
        Assert(oldColCudesc.row_count == rowsCntInCu);

        CUDesc newColCudesc;
        if (oldColCudesc.IsNullCU()) {
            SetDataTypeHandleFullNullCu(&oldColCudesc, &newColCudesc);
        } else if (oldColCudesc.IsSameValCU()) {
            SetDataTypeHandleSameValCu(sdtIndex, &oldColCudesc, &newColCudesc);
        } else {
            SetDataTypeHandleNormalCu(sdtIndex, delMaskDataPtr, &oldColCudesc, &newColCudesc);
        }

        InsertNewCudescTup(&newColCudesc, RelationGetDescr(m_NewCudescRel), m_NewTupDesc->attrs[attrIndex]);

        ResetExprContext(m_econtext);

        // until here this cu data will never be used, so invalid its cache data.
        cucache->InvalidateCU(
            (RelFileNodeOld *)&(m_OldHeapRel->rd_node), setDataTypeColInfo->attrno - 1,
            oldColCudesc.cu_id, oldColCudesc.cu_pointer);
    }

    // *econtext* is under control of *estate*.
    // so needn't care it.
    cucache = NULL;
}

FORCE_INLINE void CStoreRewriter::SetDataTypeHandleFullNullCu(_in_ CUDesc* oldColCudesc, _out_ CUDesc* newColCudesc)
{
    Assert(oldColCudesc->IsNullCU());
    Assert(!oldColCudesc->IsSameValCU());

    // nothing to do if it's a FULL NULL cu.
    // here only need to update magic field.
    *newColCudesc = *oldColCudesc;
    newColCudesc->magic = GetCurrentTransactionIdIfAny();
}

void CStoreRewriter::SetDataTypeHandleSameValCu(
    _in_ int sdtIndex, _in_ CUDesc* oldColCudesc, _out_ CUDesc* newColCudesc)
{
    Assert(!oldColCudesc->IsNullCU());
    Assert(oldColCudesc->IsSameValCU());

    newColCudesc->cu_id = oldColCudesc->cu_id;
    newColCudesc->row_count = oldColCudesc->row_count;
    newColCudesc->magic = GetCurrentTransactionIdIfAny();

    CStoreRewriteColumn* setDataTypeColInfo = m_SDTColsInfo[sdtIndex];
    TupleTableSlot* fakeSlot = MakeSingleTupleTableSlot(m_OldTupDesc);

    int attrIndex = setDataTypeColInfo->attrno - 1;
    Form_pg_attribute pColOldAttr = m_OldTupDesc->attrs[attrIndex];
    Form_pg_attribute pColNewAttr = m_NewTupDesc->attrs[attrIndex];

    // limit the memory size used in running time.
    MemoryContext oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(m_estate));

    // its new value shouldn't depend on any other existing columns
    // or their exsiting values, so construct a fake tuple to cheat
    // the func ExecEvalExpr().
    //
    bool shouldFree = false;
    Datum attOldVal = CStore::CudescTupGetMinMaxDatum(oldColCudesc, pColOldAttr, true, &shouldFree);
    HeapTuple fakeTuple = CreateFakeHeapTup(m_OldTupDesc, attrIndex, attOldVal);
    (void)ExecStoreTuple(fakeTuple, fakeSlot, InvalidBuffer, false);
    m_econtext->ecxt_scantuple = fakeSlot;

    // compute the new value only once if it's cu with the same value.
    Assert(setDataTypeColInfo->newValue);
    bool attNewIsNull = false;
    Datum attNewValue = ExecEvalExpr(setDataTypeColInfo->newValue->exprstate, m_econtext, &attNewIsNull, NULL);
    if (attNewIsNull && setDataTypeColInfo->notNull) {
        ereport(ERROR,
                (errcode(ERRCODE_NOT_NULL_VIOLATION),
                 errmsg("column \"%s\" contains null values", NameStr(pColOldAttr->attname)),
                 errdetail("existing data violate the NOT NULL constraint.")));
    }

    if (m_TblspcChanged) {
        HandleCuWithSameValue<true>(m_OldHeapRel,
                                    pColNewAttr,
                                    attNewValue,
                                    attNewIsNull,
                                    m_SDTColsMinMaxFunc[sdtIndex],
                                    m_SDTColsWriter[sdtIndex],
                                    newColCudesc,
                                    m_SDTColAppendOffset + sdtIndex);
    } else {
        HandleCuWithSameValue<false>(m_OldHeapRel,
                                     pColNewAttr,
                                     attNewValue,
                                     attNewIsNull,
                                     m_SDTColsMinMaxFunc[sdtIndex],
                                     m_SDTColsWriter[sdtIndex],
                                     newColCudesc,
                                     NULL);
    }

    if (shouldFree) {
        pfree(DatumGetPointer(attOldVal));
        attOldVal = (Datum)0;
    }

    heap_freetuple(fakeTuple);
    fakeTuple = NULL;

    ExecDropSingleTupleTableSlot(fakeSlot);
    fakeSlot = NULL;

    (void)MemoryContextSwitchTo(oldCxt);
}

/*
 * load one cu for old column, and convert each value into
 * the new datatype. then write them into new cu file.
 */
void CStoreRewriter::SetDataTypeHandleNormalCu(
    _in_ int sdtIndex, _in_ const char* delMaskDataPtr, _in_ CUDesc* oldColCudesc, _out_ CUDesc* newColCudesc)
{
    Assert(!oldColCudesc->IsNullCU());
    Assert(!oldColCudesc->IsSameValCU());

    CStoreRewriteColumn* setDataTypeColInfo = m_SDTColsInfo[sdtIndex];
    TupleTableSlot* fakeSlot = MakeSingleTupleTableSlot(m_OldTupDesc);

    int attrIndex = setDataTypeColInfo->attrno - 1;
    Form_pg_attribute pColOldAttr = m_OldTupDesc->attrs[attrIndex];
    Form_pg_attribute pColNewAttr = m_NewTupDesc->attrs[attrIndex];

    /* the other fields will be set later. */
    int rowsCntInCu = oldColCudesc->row_count;
    Assert(rowsCntInCu <= RelMaxFullCuSize);
    newColCudesc->cu_id = oldColCudesc->cu_id;
    newColCudesc->row_count = oldColCudesc->row_count;
    newColCudesc->magic = GetCurrentTransactionIdIfAny();

    /* load all values of the old cu. */
    CU* oldCu = LoadSingleCu::LoadSingleCuData(oldColCudesc,
                                               attrIndex,
                                               pColOldAttr->attlen,
                                               pColOldAttr->atttypmod,
                                               pColOldAttr->atttypid,
                                               m_OldHeapRel,
                                               m_SDTColsReader[sdtIndex]);

    GetValFunc getValFuncPtr[1];
    InitGetValFunc(pColOldAttr->attlen, getValFuncPtr, 0);
    int oldCuGetValFuncId = oldCu->HasNullValue() ? 1 : 0;

    bool firstFlag = true;
    bool fullNull = true;
    bool hasNull = false;
    int maxVarStrLen = 0;

    /* limit the memory size used in running time. */
    MemoryContext oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(m_estate));

    for (int cnt = 0; cnt < rowsCntInCu; ++cnt) {
        if ((delMaskDataPtr && IsBitmapSet((unsigned char*)delMaskDataPtr, (uint32)cnt)) || oldCu->IsNull(cnt)) {
            /*
             * case 1: the old value is deleted, or
             * case 2: the old value is a NULL value.
             * so skip the old data, and set NULL in the new CU.
             */
            m_SDTColValues[cnt] = (Datum)0;
            m_SDTColIsNull[cnt] = true;
            hasNull = true;
            continue;
        }

        /*
         * because getValFuncPtr[0]() don't handle NULL value, so that
         * we must consider NULL value earlier.
         */
        Datum attOldVal = getValFuncPtr[0][oldCuGetValFuncId](oldCu, cnt);
        HeapTuple fakeTuple = CreateFakeHeapTup(m_OldTupDesc, attrIndex, attOldVal);

        (void)ExecStoreTuple(fakeTuple, fakeSlot, InvalidBuffer, false);
        m_econtext->ecxt_scantuple = fakeSlot;

        m_SDTColIsNull[cnt] = false;
        m_SDTColValues[cnt] =
            ExecEvalExpr(setDataTypeColInfo->newValue->exprstate, m_econtext, (m_SDTColIsNull + cnt), NULL);
        if (!m_SDTColIsNull[cnt]) {
            fullNull = false;

            if (m_SDTColsMinMaxFunc[sdtIndex]) {
                /* compute the min/max for all the new values of this column. */
                m_SDTColsMinMaxFunc[sdtIndex](m_SDTColValues[cnt], newColCudesc, &firstFlag);

                /* we don't care *maxVarStrLen* if the new data type is without vary length. */
                if (pColNewAttr->attlen < 0) {
                    maxVarStrLen = Max(maxVarStrLen,
                                       (int)datumGetSize(m_SDTColValues[cnt], pColNewAttr->attbyval, pColNewAttr->attlen));
                }
            }
        } else {
            if (setDataTypeColInfo->notNull) {
                ereport(ERROR,
                        (errcode(ERRCODE_NOT_NULL_VIOLATION),
                         errmsg("column \"%s\" contains null values", NameStr(pColOldAttr->attname)),
                         errdetail("existing data violate the NOT NULL constraint.")));
            }

            m_SDTColValues[cnt] = (Datum)0;
            hasNull = true;
        }

        /*
         * Notice: NEVER free *fakeTuple* because *m_SDTColValues[]* maybe
         *   be using its values and memory space.
         */
    }

    ExecDropSingleTupleTableSlot(fakeSlot);
    fakeSlot = NULL;

    (void)MemoryContextSwitchTo(oldCxt);

    /*
     * we must tell whether the new cu has NULL values when *InitMem()* is called.
     * it's up to the three cases: 1) the old cu has null values; 2) some values
     * of the old cu have been deleted. 3) the new values is a null value.
     */
    CU* newCu = New(CurrentMemoryContext) CU(pColNewAttr->attlen, pColNewAttr->atttypmod, pColNewAttr->atttypid);
    newCu->InitMem(sizeof(Datum) * rowsCntInCu, rowsCntInCu, hasNull);

    /* form the new cu data */
    if (!hasNull) {
        /* try our best to reduce IF jumps. */
        for (int cnt = 0; cnt < rowsCntInCu; ++cnt) {
            CU::AppendCuData(m_SDTColValues[cnt], 1, pColNewAttr, newCu);
        }
    } else {
        for (int cnt = 0; cnt < rowsCntInCu; ++cnt) {
            if (!m_SDTColIsNull[cnt]) {
                CU::AppendCuData(m_SDTColValues[cnt], 1, pColNewAttr, newCu);
            } else {
                newCu->AppendNullValue(cnt);
            }
        }
    }

    /* set cudesc mode and write data into cu file if needed. */
    if (CStore::SetCudescModeForMinMaxVal(fullNull,
                                          m_SDTColsMinMaxFunc[sdtIndex] != NULL,
                                          hasNull,
                                          maxVarStrLen,
                                          pColNewAttr->attlen,
                                          newColCudesc)) {
        int16 compressing_modes = 0;
        /* set compressing modes */
        heaprel_set_compressing_modes(m_OldHeapRel, &compressing_modes);
        CompressCuData(newCu, newColCudesc, pColNewAttr, compressing_modes);

        if (m_TblspcChanged) {
            /* get the writing offset directly */
            newColCudesc->cu_pointer = m_SDTColAppendOffset[sdtIndex];

            /* update *m_SDTColAppendOffset* for the next writing. */
            m_SDTColAppendOffset[sdtIndex] += newColCudesc->cu_size;
        } else {
            /*
             * first use free space within the old cu file as possible.
             * at the worst case, append directly new data to the old cu file.
             */
            newColCudesc->cu_pointer = m_SDTColsWriter[sdtIndex]->AllocSpace(newColCudesc->cu_size);
        }
        SaveCuData(newCu, newColCudesc, m_SDTColsWriter[sdtIndex]);

        /* push CU data into replicate queue */
        CStoreCUReplication((this->m_TblspcChanged ? this->m_CUReplicationRel : m_OldHeapRel),
                            setDataTypeColInfo->attrno,
                            newCu->m_compressedBuf,
                            newColCudesc->cu_size,
                            newColCudesc->cu_pointer);
    }

    DELETE_EX(oldCu);
    DELETE_EX(newCu);
}

void CStoreRewriter::InsertNewCudescTup(
    _in_ CUDesc* pCudesc, _in_ TupleDesc pCudescTupDesc, _in_ Form_pg_attribute pColNewAttr)
{
    Datum values[CUDescMaxAttrNum];
    bool isnull[CUDescMaxAttrNum];

    m_NewCudescBulkInsert->EnterBulkMemCnxt();

    // bulk insert the cudesc tuples.
    HeapTuple tup = CStore::FormCudescTuple(pCudesc, pCudescTupDesc, values, isnull, pColNewAttr);
    m_NewCudescBulkInsert->BulkInsert(tup);

    m_NewCudescBulkInsert->LeaveBulkMemCnxt();
}

CU* LoadSingleCu::LoadSingleCuData(_in_ CUDesc* pCuDesc, _in_ int colIdx, _in_ int colAttrLen, _in_ int colTypeMode,
                                   _in_ uint32 colAtttyPid, _in_ Relation rel, __inout CUStorage* pCuStorage)
{
    CU* cu = New(CurrentMemoryContext) CU(colAttrLen, colTypeMode, colAtttyPid);

    pCuStorage->LoadCU(cu, pCuDesc->cu_pointer, pCuDesc->cu_size, false, false);

    if (cu->IsVerified(pCuDesc->magic) == false) {
        addBadBlockStat(&pCuStorage->m_cnode.m_rnode, ColumnId2ColForkNum(pCuStorage->m_cnode.m_attid));

        if (RelationNeedsWAL(rel) && CanRemoteRead()) {
            ereport(WARNING,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, try to remote read",
                             pCuDesc->cu_id,
                             RelationGetRelationName(rel),
                             relcolpath(pCuStorage),
                             pCuDesc->cu_pointer),
                      handle_in_client(true))));

            pCuStorage->RemoteLoadCU(
                cu, pCuDesc->cu_pointer, pCuDesc->cu_size, g_instance.attr.attr_storage.enable_adio_function, false);

            if (cu->IsVerified(pCuDesc->magic)) {
                pCuStorage->OverwriteCU(cu->m_compressedBuf, pCuDesc->cu_pointer, pCuDesc->cu_size, false);
            } else {
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         (errmsg("failed to remotely read CU, data corrupted in network"))));
            }
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu",
                             pCuDesc->cu_id,
                             RelationGetRelationName(rel),
                             relcolpath(pCuStorage),
                             pCuDesc->cu_pointer))));
        }
    }

    cu->UnCompress(pCuDesc->row_count, pCuDesc->magic, ALIGNOF_CUSIZE);
    return cu;
}

#define ALTER_REG_INIT_OIDS 16

CStoreAlterRegister* GetCstoreAlterReg()
{
    if (t_thrd.cstore_cxt.gCStoreAlterReg == NULL)
        t_thrd.cstore_cxt.gCStoreAlterReg =
            New(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE)) CStoreAlterRegister;

    return t_thrd.cstore_cxt.gCStoreAlterReg;
}

void DestroyCstoreAlterReg()
{
    if (t_thrd.cstore_cxt.gCStoreAlterReg) {
        DELETE_EX(t_thrd.cstore_cxt.gCStoreAlterReg);
    }
}

void CStoreAlterRegister::Destroy()
{
    if (m_Oids) {
        pfree_ext(m_Oids);
    }

    m_maxs = 0;
    m_used = 0;
}

// insert the relid, and then make the
// newest list in order.
void CStoreAlterRegister::Add(Oid relid)
{
    // verify the free slot.
    if (m_used == m_maxs) {
        AutoContextSwitch topMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

        if (m_maxs > 0) {
            m_maxs += ALTER_REG_INIT_OIDS;
            m_Oids = (Oid*)repalloc(m_Oids, sizeof(Oid) * m_maxs);
        } else {
            m_maxs = ALTER_REG_INIT_OIDS;
            m_Oids = (Oid*)palloc(sizeof(Oid) * m_maxs);
        }
    }

    // until here there is at least one slot for the new item.
    int pos = -1;
    if (DynSearch(relid, &pos)) {
        Assert(pos >= 0 && pos < m_used);
        return;
    }

    Assert(pos >= 0 && pos <= m_used);

    if (pos < m_used) {
        // insert and make the list ordered.
        errno_t rc = memmove_s(
                         (m_Oids + (pos + 1)), sizeof(Oid) * (m_maxs - (pos + 1)), (m_Oids + pos), sizeof(Oid) * (m_used - pos));
        securec_check(rc, "", "");

        m_Oids[pos] = relid;
        ++m_used;
    } else {
        // append relid to oid list
        m_Oids[m_used++] = relid;
    }
}

bool CStoreAlterRegister::Find(Oid relid) const
{
    int pos = 0;
    return DynSearch(relid, &pos);
}

// search and find the relid from the oid array.
// if found, remember its position into pos,
// otherwise indicate where relid will be inserted into.
bool CStoreAlterRegister::DynSearch(Oid relid, int* pos) const
{
    if (this->m_used == 0) {
        // oid list is empty.
        *pos = 0;
        return false;
    }

    Assert(this->m_Oids);
    Assert(this->m_maxs % ALTER_REG_INIT_OIDS == 0);

    int left = 0;
    int right = m_used - 1;

    while (left <= right) {
        int middle = left + (((uint32)(right - left)) >> 1);
        if (m_Oids[middle] > relid) {
            right = middle - 1;
        } else if (m_Oids[middle] < relid) {
            left = middle + 1;
        } else {
            // item found
            *pos = middle;
            return true;
        }
    }

    // not found, and the new item should be inserted
    // into the slot which is bigger.
    *pos = Max(left, right);
    return false;
}

/*
 * @Description: Handle all files of one column of column relation.
 * @Param[IN] attr: which column files to be handled
 * @Param[IN] CUReplicationRel: relation for HA data replication
 * @Param[IN] rel: column heap relation for copying files
 * @See also:
 */
void CStoreCopyColumnData(Relation CUReplicationRel, Relation rel, AttrNumber attr)
{
    /* extract target relation node, tablespace and relfilenode from CU Repliation relation */
    RelFileNode destRelFileNode = CUReplicationRel->rd_node;
    Oid targetTableSpace = destRelFileNode.spcNode;
    Oid targetRelFileNode = destRelFileNode.relNode;

    /* require that targetTableSpace is valid */
    Assert(OidIsValid(targetTableSpace));

    /* No need to flush out shared buffer about CU data, because we dont' use it
     *
     * compute the total size of this column data
     */
    uint64 maxOffset = CStore::GetMaxCUPointerFromDesc(attr, rel->rd_rel->relcudescrelid);
    uint64 fileSize = GetColDataFileSize(rel, attr);
    Assert(maxOffset <= fileSize);

    /* storage space processing before copying column data. */
    STORAGE_SPACE_OPERATION(CUReplicationRel, maxOffset);

    /* open the source fd to read from and destination fd to write to */
    CFileNode destColFileNode(destRelFileNode, attr, MAIN_FORKNUM);
    CFileNode srcColFileNode(rel->rd_node, attr, MAIN_FORKNUM);

    CUStorage* destCUStorage = New(CurrentMemoryContext) CUStorage(destColFileNode);
    CUStorage* srcCUStorage = New(CurrentMemoryContext) CUStorage(srcColFileNode);

    /* create new file.
     * because targetRelFileNode is new and doesn't exist anyway,
     * so set isRedo false.
     */
    destCUStorage->CreateStorage(0, false);
    CStoreRelCreateStorage(&destRelFileNode, attr, rel->rd_rel->relpersistence, rel->rd_rel->relowner);

    /* notify wal writer to flush xlog */
    XLogSetAsyncXactLSN(t_thrd.xlog_cxt.XactLastRecEnd);
    ereport(LOG,
            (errmsg("Begin to copy column data: %s(%u C%d) tblspc %u/%u/%u => %u/%u/%u,  maxOffset(%lu), fileSize(%lu)",
                    RelationGetRelationName(rel),
                    RelationGetRelid(rel),
                    (attr - 1),
                    rel->rd_node.spcNode,
                    rel->rd_node.dbNode,
                    rel->rd_node.relNode,
                    targetTableSpace,
                    rel->rd_node.dbNode,
                    targetRelFileNode,
                    maxOffset,
                    fileSize)));

    /* copy all the column data to a new file under target tablespace */
    const int tempBufSize = 4 * 1024 * 1024;
    const uint64 loops = maxOffset / tempBufSize;

    char* tempBuf = NULL;
    ADIO_RUN()
    {
        tempBuf = (char*)adio_align_alloc(tempBufSize);
    }
    ADIO_ELSE()
    {
        tempBuf = (char*)palloc(tempBufSize);
    }
    ADIO_END();

    uint64 offset = 0;

    for (uint64 i = 0; i < loops; ++i) {
        /* If we got a cancel signal during the copy of the data, quit */
        CHECK_FOR_INTERRUPTS();

        srcCUStorage->Load(offset, tempBufSize, tempBuf, false);
        destCUStorage->SaveCU(tempBuf, offset, tempBufSize, false);

        /* send column data into standby node by data replication */
        CStoreCUReplication(CUReplicationRel, attr, tempBuf, tempBufSize, offset);

        /* update this read/write offset */
        offset += tempBufSize;
    }

    const int remainSize = maxOffset - tempBufSize * loops;
    if (remainSize > 0) {
        srcCUStorage->Load(offset, remainSize, tempBuf, false);
        destCUStorage->SaveCU(tempBuf, offset, remainSize, false);

        /* send column data into standby node by data replication */
        CStoreCUReplication(CUReplicationRel, attr, tempBuf, remainSize, offset);

        /* update this read/write offset */
        offset += remainSize;
    }

    /* must be flush all the column data to disk at last time */
    destCUStorage->FlushDataFile();

    ereport(LOG,
            (errmsg("End to copy column data: %s(%u C%d) tblspc %u/%u/%u => %u/%u/%u",
                    RelationGetRelationName(rel),
                    RelationGetRelid(rel),
                    (attr - 1),
                    rel->rd_node.spcNode,
                    rel->rd_node.dbNode,
                    rel->rd_node.relNode,
                    targetTableSpace,
                    rel->rd_node.dbNode,
                    targetRelFileNode)));

    /* release the memory */
    ADIO_RUN()
    {
        adio_align_free(tempBuf);
    }
    ADIO_ELSE()
    {
        pfree(tempBuf);
    }
    ADIO_END();

    DELETE_EX(srcCUStorage);
    DELETE_EX(destCUStorage);
}

/*
 * @Description: finish the copy actions after all column files are handled.
 *    Create the main file and drop the old storage files.
 * @Param[IN] colRel: column relation to be changed tablespace
 * @Param[IN] newrelfilenode: the new relfilenode under targetTableSpace
 * @Param[IN] targetTableSpace: the new/target tablespace
 * @See also: CStoreCopyColumnData()
 */
void CStoreCopyColumnDataEnd(Relation colRel, Oid targetTableSpace, Oid newrelfilenode)
{
    /* require that newTableSpace is valid */
    targetTableSpace = ConvertToRelfilenodeTblspcOid(targetTableSpace);
    Assert(OidIsValid(targetTableSpace));

    /* Important:
     * Above we push the column file, column bcm file to the pendingDeletes.
     * Now we push the logical table file to pendingDeletes.
     *
     * When abort and delete the file, smgrDoPendingDeletes will
     * 1. delete the logical table file.
     *    it will call DropRelFileNodeAllBuffers to make all buffer invaild,
     *    include the column bcm buffer.
     * 2. then we can drop the column file and column bcm file.
     *
     * Examples: if a column table relfilenode is 16384, it will create 16384, 16384_C1.0,
     *     16384_C1_bcm... push into pendingDeletes
     *     push:						 pop:
     * 	        16384(third)				 16384(make all buffer invaild, unlink file)
     * 	        16384_C1_bcm(second)		 16384_C1_bcm(unlink file)
     * 	        16384_C1.0(first)			 16384_C1.0(unlink file)
     */
    RelFileNode destRelFileNode = colRel->rd_node;
    destRelFileNode.spcNode = targetTableSpace;
    destRelFileNode.relNode = newrelfilenode;
    RelationCreateStorage(destRelFileNode, colRel->rd_rel->relpersistence, colRel->rd_rel->relowner);

    ereport(LOG,
            (errmsg("Column Data End: %s(%u) tblspc %u/%u/%u => %u/%u/%u",
                    RelationGetRelationName(colRel),
                    RelationGetRelid(colRel),
                    colRel->rd_node.spcNode,
                    colRel->rd_node.dbNode,
                    colRel->rd_node.relNode,
                    targetTableSpace,
                    colRel->rd_node.dbNode,
                    newrelfilenode)));

    /* Ok, will drop this column table */
    RelationDropStorage(colRel);
}

/*
 * @Description: Copy all column files to a new tablespace for
 *    column relation.
 * @Param[IN] colRel: column relation to be changed tablespace
 * @Param[IN] targetTableSpace: the new/target tablespace
 * @Return: the new relfilenode under targetTableSpace
 * @See also:
 */
Oid CStoreSetTableSpaceForColumnData(Relation colRel, Oid targetTableSpace)
{
    /*
     * Relfilenodes are not unique across tablespaces, so we need to allocate
     * a new one in the new tablespace.
     */
    Oid newrelfilenode = GetNewRelFileNode(targetTableSpace, NULL, colRel->rd_rel->relpersistence);

    /* create CU replication relation */
    RelFileNode CUReplicationFile = {
        ConvertToRelfilenodeTblspcOid(targetTableSpace), colRel->rd_node.dbNode, newrelfilenode, InvalidBktId
    };
    Relation CUReplicationRel = CreateCUReplicationRelation(
                                    CUReplicationFile, colRel->rd_backend, colRel->rd_rel->relpersistence, RelationGetRelationName(colRel));

    int nattrs = RelationGetDescr(colRel)->natts;
    for (int i = 0; i < nattrs; ++i) {
        Form_pg_attribute thisattr = RelationGetDescr(colRel)->attrs[i];
        if (!thisattr->attisdropped) {
            /* change tablespace for each column' data */
            CStoreCopyColumnData(CUReplicationRel, colRel, thisattr->attnum);
        }
    }

    CStoreCopyColumnDataEnd(colRel, targetTableSpace, newrelfilenode);

    /* destroy fake relation */
    FreeFakeRelcacheEntry(CUReplicationRel);

    return newrelfilenode;
}

/*
 * @Description: Check the validity of number of partitions
 * @Param[IN] partTableRel: partitioned table
 * @Param[IN] partNum: number of partitions
 * @See also: ATExecCStoreMergePartition
 */
void ATCheckPartitionNum(Relation partTableRel, int partNum)
{
    if (partNum < ARRAY_2_LEN) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("source partitions must be at least two partitions")));
    }
    if (partNum > MAX_CSTORE_MERGE_PARTITIONS) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merge partitions of relation \"%s\", source partitions must be no more than %d partitions",
                        RelationGetRelationName(partTableRel),
                        MAX_CSTORE_MERGE_PARTITIONS)));
    }
}

/*
 * @Description: Check the usability of indices of the give relation
 * @Param[IN] relation: relation to be checked
 * @See also: ATExecCStoreMergePartition
 */
void ATCheckIndexUsability(Relation relation)
{
    List* index_list = NIL;
    ListCell* cell = NULL;

    index_list = RelationGetIndexList(relation);

    foreach (cell, index_list) {
        Oid indexId = lfirst_oid(cell);
        Relation currentIndex;
        /* Open the index relation, use AccessShareLock */
        currentIndex = index_open(indexId, AccessShareLock);
        if (!IndexIsUsable(currentIndex->rd_index)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("merge partitions cannot process inusable index relation \''%s\''",
                            RelationGetRelationName(currentIndex))));
        }

        index_close(currentIndex, NoLock);
    }
    list_free(index_list);
}

/*
 * @Description: Check if a partition of a partitioned table needs to be renamed
 * @Param[IN] relation: partitioned table
 * @Param[IN] oldPartName: original partition name
 * @Param[IN] destPartName: new partition name
 * @See also: ATExecCStoreMergePartition
 */
bool ATCheckIfRenameNeeded(Relation relation, const char* oldPartName, char* destPartName)
{
    if (oldPartName == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid original partition name")));
    }

    if (strcmp(oldPartName, destPartName) != 0) {
        /*  check partition new name does not exist. */
        if (InvalidOid != GetSysCacheOid3(PARTPARTOID, NameGetDatum(destPartName),
                                          CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION), ObjectIdGetDatum(relation->rd_id))) {
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                     errmsg("target partition's name \"%s\" already exists", destPartName)));
        }

        return true;
    }

    return false;
}

/*
 * @Description: Check if a partition of a partitioned table needs to be renamed
 * @Param[IN] relation: partitioned table
 * @Param[IN] srcPartitions: list of parititions in this partitioned table
 * @Param[IN] destPartName: new partition name
 * @Param[OUT] srcPartOids: list of Oids of parititions in this partitioned table
 * @Param[OUT] destPartOid: Oid of the partition we are merging onto
 * @See also: ATExecCStoreMergePartition
 */
bool ATTraverseSrcPartitions(Relation relation, List* srcPartitions, char* destPartName, int partNum,
                             _out_ List** srcPartOidsPPtr, _out_ Oid& destPartOid)
{
    ListCell* cell = NULL;
    int curPartSeq = 0;
    int prevPartSeq = -1;
    int iterator = 0;
    char* oldPartName = NULL;

    foreach (cell, srcPartitions) {
        char* partName = NULL;
        Oid srcPartOid = InvalidOid;

        iterator++;
        partName = strVal(lfirst(cell));

        /* from name to partition oid */
        srcPartOid = partitionNameGetPartitionOid(relation->rd_id,
                                                partName,
                                                PART_OBJ_TYPE_TABLE_PARTITION,
                                                ExclusiveLock,  // get ExclusiveLock lock on src partitions
                                                false,          // no missing
                                                false,          // wait
                                                NULL,
                                                NULL,
                                                NoLock);
        /* check local index 'usable' state */
        if (!checkPartitionLocalIndexesUsable(srcPartOid)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot merge partition bacause partition %s has unusable local index", partName)));
        }

        /* from partitionoid to partition sequence */
        curPartSeq = partOidGetPartSequence(relation, srcPartOid);

        /* check the continuity of sequence, not the first round loop */
        if (iterator != 1 && (curPartSeq - prevPartSeq != 1)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("source partitions must be continuous and in ascending order of boundary")));
        }
        prevPartSeq = curPartSeq;

        /* save the last source partition name */
        if (iterator == partNum) {
            oldPartName = partName;
            destPartOid = srcPartOid;
        }

        /* save oid of src partition */
        *srcPartOidsPPtr = lappend_oid(*srcPartOidsPPtr, srcPartOid);
    }

    return ATCheckIfRenameNeeded(relation, oldPartName, destPartName);
}

/*
 * @Description: Create a temp table that we could merge partitions onto and later to be swapped
 * @Param[IN] partTableRel: partitioned table
 * @Param[IN] cmd: Merging command
 * @Param[IN] destPartOid: Oid of the partition that we will eventually merge onto
 * @Param[OUT] object: ObjectAddress that holds this temp table
 * @Param[OUT] tempTableOid: Oid of this temp table
 * @See also: ATExecCStoreMergePartition
 */
void ATCreateTempTableForMerge(
    Relation partTableRel, AlterTableCmd* cmd, Oid destPartOid, _out_ ObjectAddress& object, _out_ Oid& tempTableOid)
{
    TupleDesc partedTableHeapDesc;
    Datum partedTableRelOptions = 0;
    Partition destPart = NULL;
    Relation destPartRel = NULL;
    HeapTuple tuple = NULL;
    bool isNull = false;
    Oid targetPartTablespaceOid = InvalidOid;

    partedTableHeapDesc = RelationGetDescr(partTableRel);

    tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(RelationGetRelid(partTableRel)), LOG);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                 errmsg("cache lookup failed for relation: %u", RelationGetRelid(partTableRel))));
    }
    partedTableRelOptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
    if (isNull) {
        partedTableRelOptions = (Datum)0;
    }

    /* open the dest partition, it was already locked by partitionNameGetPartitionOid() call */
    destPart = partitionOpen(partTableRel, destPartOid, NoLock);
    destPartRel = partitionGetRelation(partTableRel, destPart);

    /* check target partition tablespace */
    if (PointerIsValid(cmd->target_partition_tablespace)) {
        targetPartTablespaceOid = get_tablespace_oid(cmd->target_partition_tablespace, false);
    } else {
        targetPartTablespaceOid = destPartRel->rd_rel->reltablespace;
    }

    /* create temp table and open it */
    tempTableOid = makePartitionNewHeap(partTableRel,
                                        partedTableHeapDesc,
                                        partedTableRelOptions,
                                        destPartRel->rd_id,
                                        destPartRel->rd_rel->reltoastrelid,
                                        targetPartTablespaceOid,
                                        true);
    object.classId = RelationRelationId;
    object.objectId = tempTableOid;
    object.objectSubId = 0;

    ReleaseSysCache(tuple);
    partitionClose(partTableRel, destPart, NoLock);
    releaseDummyRelation(&destPartRel);
}

/*
 * Merge different partitions p1,p2,...,px of Cstore table t into px
 *
 * @param partTableRel the logical table of partitions
 * @param cmd AlterTableCmd
 * @version 2.0 11/16/2016
 */
void ATExecCStoreMergePartition(Relation partTableRel, AlterTableCmd* cmd)
{
    List* srcPartitions = NIL;
    List* srcPartOids = NIL;
    ListCell* cell = NULL;

    char* destPartName = NULL;
    Oid destPartOid = InvalidOid;
    Partition destPart = NULL;

    bool renameTargetPart = false;
    int partNum = 0;

    TupleDesc partedTableHeapDesc;
    Oid tempTableOid = InvalidOid;
    Relation tempTableRel = NULL;
    ObjectAddress object;
    int reindex_flags;

    srcPartitions = (List*)cmd->def;
    destPartName = cmd->name;
    partNum = srcPartitions->length;

    /* Check the length of srcPartitions and throw error if needed */
    ATCheckPartitionNum(partTableRel, partNum);

    /* lock the index relation on partitioned table and check the usability */
    ATCheckIndexUsability(partTableRel);

    /*
     * step 1: lock src partitions, and check the continuity of srcPartitions.
     * for 1...nth partition, we use AccessExclusiveLock lockmode
     * althought merge is something like delete and insert.
     */
    renameTargetPart =
        ATTraverseSrcPartitions(partTableRel, srcPartitions, destPartName, partNum, &srcPartOids, destPartOid);

    /*
     * step 2: create a temp table for merge
     * get desc of partitioned table
     */
    ATCreateTempTableForMerge(partTableRel, cmd, destPartOid, object, tempTableOid);

    tempTableRel = heap_open(tempTableOid, AccessExclusiveLock);
    RelationOpenSmgr(tempTableRel);

    /*
     * step 3: merge src partitions into temp table
     */
    partedTableHeapDesc = RelationGetDescr(partTableRel);

    CStoreInsert* cstoreOpt = NULL;
    CStoreScanDesc scan = NULL;
    int16* colIdx = NULL;
    Form_pg_attribute* oldAttrs = NULL;

    // Init CStore insertion.
    InsertArg args;
    CStoreInsert::InitInsertArg(tempTableRel, NULL, true, args);
    args.sortType = BATCH_SORT;
    cstoreOpt = (CStoreInsert*)New(CurrentMemoryContext) CStoreInsert(tempTableRel, args, false, NULL, NULL);

    // create partition memory context
    MemoryContext partitionMemContext = AllocSetContextCreate(CurrentMemoryContext,
                                                              "partition merge memory context",
                                                              ALLOCSET_DEFAULT_MINSIZE,
                                                              ALLOCSET_DEFAULT_INITSIZE,
                                                              ALLOCSET_DEFAULT_MAXSIZE);

    // switch to partition memory context
    MemoryContext oldMemContext = MemoryContextSwitchTo(partitionMemContext);

    foreach (cell, srcPartOids) {
        Oid srcPartOid = lfirst_oid(cell);
        Partition srcPartition = NULL;
        Relation srcPartRel = NULL;

        srcPartition = partitionOpen(partTableRel, srcPartOid, NoLock);
        srcPartRel = partitionGetRelation(partTableRel, srcPartition);
        PartitionOpenSmgr(srcPartition);

        /*
         * Init CStore scan.
         */
        colIdx = (int16*)palloc0(sizeof(int16) * partedTableHeapDesc->natts);
        oldAttrs = partedTableHeapDesc->attrs;
        for (int i = 0; i < partedTableHeapDesc->natts; i++)
            colIdx[i] = oldAttrs[i]->attnum;
        scan = CStoreBeginScan(srcPartRel, partedTableHeapDesc->natts, colIdx, SnapshotNow, true);

        /* scan and insert CU data */
        BatchCUData* batchCU = New(CurrentMemoryContext) BatchCUData();
        batchCU->Init(partedTableHeapDesc->natts);
        do {
            /* In CUStorage::LoadCU we palloc CU_ptrs, which is used in BatchCUData
               and freed in CStoreInsert::SaveAll */
            CStoreScanNextTrunkOfCU(scan, batchCU);

            if (!batchCU->batchCUIsNULL())
                cstoreOpt->CUInsert(batchCU, TABLE_INSERT_FROZEN);

            /* In here we don't need to free CUptrs and CUDescs in batchCU
             * since we are moving all CUs in the same row direction.
             * Therefore we either always override all CUptrs and CUDescs
             * in batchCU on every loop or not loading any data into it and insert
             * at all (when we reach the end of this while loop).
             */
            batchCU->reset();
        } while (!scan->m_CStore->IsEndScan());

        // free pointers in batchCU and batchCU itself
        DELETE_EX(batchCU);

        /* scan insert delta data */
        VectorBatch* vecBatch = scan->m_pScanBatch;
        while (!CStoreIsEndScan(scan)) {
            vecBatch->Reset();
            ScanDeltaStore(scan, vecBatch, NULL);
            vecBatch->FixRowCount();
            if (!BatchIsNull(vecBatch))
                cstoreOpt->BatchInsert(vecBatch, TABLE_INSERT_FROZEN);
        }

        CStoreEndScan(scan);
        PartitionCloseSmgr(srcPartition);
        partitionClose(partTableRel, srcPartition, NoLock);
        releaseDummyRelation(&srcPartRel);

        pfree(colIdx);

        // force flash data
        cstoreOpt->EndBatchInsert();

        // clean partition memory context
        MemoryContextReset(partitionMemContext);
    }

    // delete partition memory context
    (void)MemoryContextSwitchTo(oldMemContext);
    MemoryContextDelete(partitionMemContext);

    cstoreOpt->SetEndFlag();
    cstoreOpt->BatchInsert((VectorBatch*)NULL, TABLE_INSERT_FROZEN);
    DELETE_EX(cstoreOpt);
    CStoreInsert::DeInitInsertArg(args);

    /* close temp relation */
    RelationCloseSmgr(tempTableRel);
    heap_close(tempTableRel, NoLock);

    /* before swap refilenode, promote lock on heap partition from ExclusiveLock to AccessExclusiveLock */
    destPart = partitionOpenWithRetry(partTableRel, destPartOid, AccessExclusiveLock, "MERGE PARTITIONS");
    /* step 4: swap relfilenode and delete temp table */
    if (!destPart) {
        performDeletion(&object, DROP_CASCADE, PERFORM_DELETION_INTERNAL);
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                 errmsg("could not acquire AccessExclusiveLock on dest table partition \"%s\", MERGE PARTITIONS failed",
                        getPartitionName(destPartOid, false))));
    }

    finishPartitionHeapSwap(destPartOid, tempTableOid, true, u_sess->utils_cxt.RecentXmin, InvalidMultiXactId);
    partitionClose(partTableRel, destPart, NoLock);

#ifndef ENABLE_MULTIPLE_NODES
    /*
    * Each delta table of destPartOid and tempTableOid will not be swapped.
    * We will build index of new delta table. After partition swap,
    * tempTableOid has the old relfilenode, destPartOid has the new relfilenode.
    */
    BuildIndexOnNewDeltaTable(tempTableOid, destPartOid, RelationGetRelid(partTableRel));
#endif

    /* ensure that preceding changes are all visible to the next deletion step. */
    CommandCounterIncrement();

    /* delete temp table */
    performDeletion(&object, DROP_CASCADE, PERFORM_DELETION_INTERNAL);

    /* step 5: drop src partitions */
    foreach (cell, srcPartOids) {
        Oid srcPartOid = lfirst_oid(cell);
        if (destPartOid != srcPartOid) {
            fastDropPartition(partTableRel, srcPartOid, "MERGE PARTITIONS");
        }
    }

    /* step 6: rename p(n) to p(target) if needed, the dest partition is now locked by swap refilenode processing step
     */
    if (renameTargetPart) {
        renamePartitionInternal(partTableRel->rd_id, destPartOid, destPartName);
    }

    /*
     * step 7: Reindex the target partition.
     */
    destPart = partitionOpenWithRetry(partTableRel, destPartOid, AccessExclusiveLock, "MERGE PARTITIONS");

    reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
    (void)reindexPartition(RelationGetRelid(partTableRel), destPartOid, reindex_flags, REINDEX_ALL_INDEX);

    partitionClose(partTableRel, destPart, NoLock);
}
