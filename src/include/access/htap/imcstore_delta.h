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
 * imcstore_delta.h
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/imcstore_delta.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMCSTORE_DELTA_H
#define IMCSTORE_DELTA_H

#include "knl/knl_instance.h"

#ifdef ENABLE_HTAP
#define HAVE_HTAP_TABLES (pg_atomic_read_u32(&g_instance.imcstore_cxt.imcs_tbl_cnt) != 0)

#define DEFAULT_DELTAPAGE_SIZE (8192)

#define DEFAULT_DELTAPAGE_ELEMENTS (DEFAULT_DELTAPAGE_SIZE / sizeof(DeltaElement))

enum DeltaOperationType {
    IMCSTORE_DELETE,
    IMCSTORE_INSERT,
    OPERATION_DELETED
};

struct DeltaElement {
    enum DeltaOperationType operationType;
    ItemPointerData ctid;
    TransactionId xid;
};

struct DeltaTableIterator {
    ListCell* currentPage;
    uint32 currentRow;
    DeltaTableIterator(ListCell* beginPage) : currentPage(beginPage), currentRow(0) {};
    ItemPointer GetNext(DeltaOperationType *type, TransactionId *xid);
};

class DeltaPage : public BaseObject {
public:
    DeltaElement data[DEFAULT_DELTAPAGE_ELEMENTS];
    uint32 used;
    uint32 deadElement;

    DeltaPage() : used(0), deadElement(0) {};
    ~DeltaPage() {};
    void Insert(DeltaOperationType type, ItemPointer ctid, TransactionId xid);
    void Delete(uint32 offset);
    bool IsFull()
    {
        return used >= DEFAULT_DELTAPAGE_ELEMENTS;
    }
    bool IsDeadPage();
    bool Vacuum(TransactionId xid);
};

class DeltaTable : public BaseObject {
public:
    List* pages;
    uint32 rowNumber;
    bool vacuumInProcess;

    DeltaTable() : pages(NULL), rowNumber(0) {}
    ~DeltaTable() {}
    void Insert(DeltaOperationType type, ItemPointer ctid, TransactionId xid, Oid relid, uint32 cuId);
    void Vacuum(TransactionId xid);
    DeltaTableIterator ScanInit();
};

struct IMCStoreVacuumTarget {
    uint32 relOid;
    uint32 rowGroupId;
};

struct knl_g_imcstore_context;

extern void InitIMCStoreVacuumQueue(knl_g_imcstore_context* context);
extern bool IMCStoreVacuumPushWork(Oid relid, uint32 cuId);
extern void IMCStoreVacuumWorkerMain(void);

extern bool IMCStoreHookPreLock(Oid relid, ItemPointer ctid);
extern void IMCStoreInsertHook(Oid relid, ItemPointer ctid, TransactionId xid = InvalidTransactionId,
                               bool locked = false);
extern void IMCStoreDeleteHook(Oid relid, ItemPointer ctid, TransactionId xid = InvalidTransactionId,
                               bool locked = false);
extern void IMCStoreUpdateHook(
    Oid relid, ItemPointer ctid, ItemPointer newCtid, TransactionId xid = InvalidTransactionId);

#endif /* ENABLE_HTAP */
#endif /* IMCSTORE_DELTA_H */
