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
#include "access/htap/imcs_ctlg.h"
#include "component/thread/mpmcqueue.h"

#include "access/htap/imcstore_delta.h"

#ifdef ENABLE_HTAP

constexpr uint32 AUTO_VACUUM_TRIGGER_LIMIT = Min(IMCSTORE_MAX_ROW_PER_CU / 3, DEFAULT_DELTAPAGE_ELEMENTS * 10);

void IMCStoreInsertHook(Oid relid, ItemPointer ctid, TransactionId xid)
{
    IMCSDesc* imcsDesc = IMCU_CACHE->GetImcsDesc(relid);
    if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) return;

    if (xid == InvalidTransactionId) {
        xid = GetTopTransactionId();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    uint32 cuId = ItemPointerGetBlockNumber(ctid) / MAX_IMCS_PAGES_ONE_CU;
    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(DeltaOperationType::IMCSTORE_INSERT, ctid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();
    MemoryContextSwitchTo(oldcontext);
}

void IMCStoreDeleteHook(Oid relid, ItemPointer ctid, TransactionId xid)
{
    IMCSDesc* imcsDesc = IMCU_CACHE->GetImcsDesc(relid);
    if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) return;

    if (xid == InvalidTransactionId) {
        xid = GetTopTransactionId();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    uint32 cuId = ItemPointerGetBlockNumber(ctid) / MAX_IMCS_PAGES_ONE_CU;
    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(DeltaOperationType::IMCSTORE_DELETE, ctid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();
    MemoryContextSwitchTo(oldcontext);
}

void IMCStoreUpdateHook(Oid relid, ItemPointer ctid, ItemPointer newCtid, TransactionId xid)
{
    IMCSDesc* imcsDesc = IMCU_CACHE->GetImcsDesc(relid);
    if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) return;

    if (xid == InvalidTransactionId) {
        xid = GetTopTransactionId();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    uint32 cuId = ItemPointerGetBlockNumber(ctid) / MAX_IMCS_PAGES_ONE_CU;
    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(DeltaOperationType::IMCSTORE_DELETE, ctid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();

    cuId = ItemPointerGetBlockNumber(newCtid) / MAX_IMCS_PAGES_ONE_CU;
    rowgroup = imcsDesc->GetNewRGForCUInsert(cuId);

    rowgroup->Insert(DeltaOperationType::IMCSTORE_INSERT, newCtid, xid, relid, cuId);
    imcsDesc->UnReferenceRowGroup();
    MemoryContextSwitchTo(oldcontext);
}

void DeltaPage::Insert(DeltaOperationType type, ItemPointer ctid, TransactionId xid)
{
    data[used].operationType = type;
    data[used].ctid = *ctid;
    data[used].xid = xid;
    ++used;
}

void DeltaPage::Delete(uint32 offset)
{
    data[offset].operationType = DeltaOperationType::OPERATION_DELETED;
    ++deadElement;
}

bool DeltaPage::IsDeadPage()
{
    if (used >= DEFAULT_DELTAPAGE_ELEMENTS && deadElement == used) {
        return true;
    }
    return false;
}

bool DeltaPage::Vacuum(TransactionId xid)
{
    uint32 currentUsedElements = used;
    used = 0;
    deadElement = 0;

    for (uint32 i = 0; i < currentUsedElements; ++i) {
        if (data[i].operationType == OPERATION_DELETED) {
            continue;
        }
        if (data[i].xid > xid) {
            data[used] = data[i];
            ++used;
            continue;
        }
    }
    return used == 0;
}

/* will return true if delta table should vacuum */
void DeltaTable::Insert(DeltaOperationType type, ItemPointer ctid, TransactionId xid, Oid relid, uint32 cuId)
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
    page->Insert(type, ctid, xid);
    ++rowNumber;
    if (!vacuumInProcess && rowNumber > AUTO_VACUUM_TRIGGER_LIMIT) {
        IMCStoreVacuumPushWork(relid, cuId);
        // change the statistic to avoid push work in a short time
        vacuumInProcess = true;
    }
}

void DeltaTable::Vacuum(TransactionId xid)
{
    List* newpages = NIL;
    ListCell* cell = NULL;
    rowNumber = 0;
    foreach(cell, pages) {
        DeltaPage* page = (DeltaPage*)lfirst(cell);
        if (page->Vacuum(xid)) {
            delete page;
            continue;
        }
        rowNumber += page->used;
        newpages = lappend(newpages, (void*)page);
    }
    list_free(pages);
    pages = newpages;
    vacuumInProcess = false;
}

DeltaTableIterator DeltaTable::ScanInit()
{
    return DeltaTableIterator(list_head(pages));
}

ItemPointer DeltaTableIterator::GetNext(DeltaOperationType *type, TransactionId *xid)
{
    if (currentPage == NULL) {
        return nullptr;
    }
    while (currentPage != NULL) {
        DeltaPage* page = (DeltaPage*)lfirst(currentPage);
        // there is no valid data in this page
        if (page->IsDeadPage()) {
            currentRow = 0;
            currentPage = lnext(currentPage);
            continue;
        }
        for (;currentRow < page->used; ++currentRow) {
            if (page->data[currentRow].operationType == DeltaOperationType::OPERATION_DELETED) continue;
            ItemPointer item = &page->data[currentRow].ctid;
            if (type) {
                *type = page->data[currentRow].operationType;
            }
            if (xid) {
                *xid = page->data[currentRow].xid;
            }
            ++currentRow;
            return item;
        }
        currentRow = 0;
        currentPage = lnext(currentPage);
    }
    return nullptr;
}

#endif /* ENABLE_HTAP */
