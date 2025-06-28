/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * imcstore_insert.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/imcstore_delta.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/cu.h"
#include "access/htap/imcucache_mgr.h"
#include "access/htap/imcs_hash_table.h"
#include "access/htap/imcs_ctlg.h"
#include "component/thread/mpmcqueue.h"

#include "access/htap/imcstore_delta.h"

#ifdef ENABLE_HTAP

constexpr uint32 AUTO_VACUUM_TRIGGER_LIMIT = Min(IMCSTORE_MAX_ROW_PER_CU / 3, DEFAULT_DELTAPAGE_ELEMENTS * 10);
constexpr int CHAR_BIT_SIZE = 8;

void IMCStoreInsertHook(Oid relid, ItemPointer ctid, bool isRelNode, TransactionId xid)
{
    IMCSDesc* imcsDesc = NULL;
    if (isRelNode) {
        imcsDesc = IMCS_HASH_TABLE->GetImcsDescByRelNode(relid);
    } else {
        imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(relid);
    }
    if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) return;

    if (xid == InvalidTransactionId) {
        xid = GetTopTransactionId();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    uint32 cuId = ItemPointerGetBlockNumber(ctid) / MAX_IMCS_PAGES_ONE_CU;
    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(ctid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();
    MemoryContextSwitchTo(oldcontext);
}

void IMCStoreDeleteHook(Oid relid, ItemPointer ctid, bool isRelNode, TransactionId xid)
{
    IMCSDesc* imcsDesc = NULL;
    if (isRelNode) {
        imcsDesc = IMCS_HASH_TABLE->GetImcsDescByRelNode(relid);
    } else {
        imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(relid);
    }
    if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) return;

    if (xid == InvalidTransactionId) {
        xid = GetTopTransactionId();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    uint32 cuId = ItemPointerGetBlockNumber(ctid) / MAX_IMCS_PAGES_ONE_CU;
    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(ctid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();
    MemoryContextSwitchTo(oldcontext);
}

void IMCStoreUpdateHook(Oid relid, ItemPointer ctid, ItemPointer newCtid, bool isRelNode, TransactionId xid)
{
    IMCSDesc* imcsDesc = NULL;
    if (isRelNode) {
        imcsDesc = IMCS_HASH_TABLE->GetImcsDescByRelNode(relid);
    } else {
        imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(relid);
    }
    if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) return;

    if (xid == InvalidTransactionId) {
        xid = GetTopTransactionId();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    uint32 cuId = ItemPointerGetBlockNumber(ctid) / MAX_IMCS_PAGES_ONE_CU;
    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(ctid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();

    cuId = ItemPointerGetBlockNumber(newCtid) / MAX_IMCS_PAGES_ONE_CU;
    rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(newCtid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();
    MemoryContextSwitchTo(oldcontext);
}

void DeltaPage::Insert(ItemPointer ctid, TransactionId xid)
{
    data[used].ctid = *ctid;
    data[used].xid = xid;
    ++used;
}

uint32 DeltaPage::Vacuum(TransactionId xid, ListCell* &currPage)
{
    if (currPage == NULL) {
        return 0;
    }
    DeltaPage* page = (DeltaPage*)lfirst(currPage);
    uint32 currentUsedElements = used;
    used = 0;
    uint32 restRecords = 0;

    for (uint32 i = 0; i < currentUsedElements; ++i) {
        if (data[i].xid < xid) {
            continue;
        }
        ++restRecords;

        page->data[page->used] = data[i];
        ++page->used;
        if (page->IsFull()) {
            currPage = lnext(currPage);
            if (currPage == NULL) {
                break;
            }
            page = (DeltaPage*)lfirst(currPage);
        }
    }
    return restRecords;
}

/* will return true if delta table should vacuum */
void DeltaTable::Insert(ItemPointer ctid, TransactionId xid, Oid relid, uint32 cuId)
{
    DeltaPage* page = NULL;
    if (pages == NULL) {
        pages = lappend(pages, New(CurrentMemoryContext) DeltaPage());
    }
    page = (DeltaPage*)lfirst(list_tail(pages));
    if (page->IsFull()) {
        page = New(CurrentMemoryContext) DeltaPage();
        pages = lappend(pages, page);
    }
    page->Insert(ctid, xid);
    ++rowNumber;
    if (!vacuumInProcess && rowNumber > AUTO_VACUUM_TRIGGER_LIMIT && IMCStoreVacuumPushWork(relid, cuId)) {
        // change the statistic to avoid push work in a short time
        vacuumInProcess = true;
    }
}

void DeltaTable::Vacuum(TransactionId xid)
{
    ListCell* currPage = list_head(pages);
    ListCell* cell = NULL;
    rowNumber = 0;
    foreach(cell, pages) {
        DeltaPage* page = (DeltaPage*)lfirst(cell);
        uint32 rest = page->Vacuum(xid, currPage);
        rowNumber += rest;
    }
    while (currPage != NULL && lnext(currPage) != NULL) {
        DeltaPage* page = (DeltaPage*)lfirst(lnext(currPage));
        delete page;
        pages = list_delete_cell(pages, lnext(currPage), currPage);
    }
    vacuumInProcess = false;
}

DeltaTableIterator DeltaTable::ScanInit()
{
    return DeltaTableIterator(list_head(pages));
}

ItemPointer DeltaTableIterator::GetNext()
{
    if (currentPage == NULL) {
        return nullptr;
    }
    while (currentPage != NULL) {
        DeltaPage* page = (DeltaPage*)lfirst(currentPage);
        for (;currentRow < page->used; ++currentRow) {
            ItemPointer item = &page->data[currentRow].ctid;
            ++currentRow;
            return item;
        }
        currentRow = 0;
        currentPage = lnext(currentPage);
    }
    return nullptr;
}

#endif /* ENABLE_HTAP */
