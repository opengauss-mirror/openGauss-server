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
 * cstore_am.h
 *        routines to support InMemoryColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/imcstore_am.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMCSTORE_AM_H
#define IMCSTORE_AM_H

#include "postgres.h"
#include "access/cstore_am.h"
#include "access/cstore_roughcheck_func.h"
#include "access/cstore_minmax_func.h"
#include "cstore.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "access/htap/imcustorage.h"
#include "access/htap/imcs_ctlg.h"
#include "storage/cucache_mgr.h"
#include "utils/snapshot.h"
#include "vecexecutor/vecnodeimcstorescan.h"
#include "access/cstore_am.h"

#ifdef ENABLE_HTAP

class IMCStore : public CStore {
    // public static area
public:
    static uint32 GetMaxCUID(IMCSDesc* imcstoreDesc);

public:
    IMCStore();
    virtual ~IMCStore();
    void Destroy() override;

    // Scan APIs
    void InitScan(CStoreScanState *state, Snapshot snapshot = NULL) override;
    void InitPartReScan(Relation rel) override;

    // Load CUDesc information of column according to loadInfoPtr
    // LoadCUDescCtrl include maxCUDescNum for this load, because if we load all
    // it need big memory to hold
    //
    bool LoadCUDesc(_in_ int col, __inout LoadCUDescCtl *loadInfoPtr, _in_ bool prefetch_control,
                    _in_ Snapshot snapShot = NULL) override;

    // Get tuple deleted information from VC CU description.
    void GetCUDeleteMaskIfNeed(_in_ uint32 cuid, _in_ Snapshot snapShot) override;

    // Get CU data.
    // Note that the CU is pinned
    CU *GetCUData(_in_ CUDesc *cuDescPtr, _in_ int colIdx, _in_ int valSize, _out_ int &slotId) override;

    /* Set CU range for scan in redistribute. */
    void SetScanRange() override;

    void FillPerRowGroupDelta(_in_ IMCStoreScanState* state, _in_ uint32 cuid, _out_ VectorBatch* vecBatchOut);
    bool InsertDeltaRowToBatch(_in_ IMCStoreScanState* state, ItemPointerData item, _out_ VectorBatch* vecBatchOut);
    bool ImcstoreFillByDeltaScan(_in_ CStoreScanState* state, _out_ VectorBatch* vecBatchOut) override;
    void LoadCU(int imcsColIdx, CU *cuPtr, CUDesc *cuDescPtr);

private:
    IMCUStorage** m_imcuStorage;
    IMCSDesc* m_imcstoreDesc;
    bool m_isCtidCU;
    int m_ctidCol;

    unsigned char m_cuDeltaMask[MAX_IMCSTORE_DEL_BITMAP_SIZE];
    uint64 m_deltaMaskMax;
    uint64 m_deltaScanCurr;
    List* m_currentRowGroups;
};

typedef struct PinnedRowGroup : public BaseObject {
    RowGroup *rowgroup;
    IMCSDesc *desc;
    PinnedRowGroup(RowGroup *rowgroup, IMCSDesc *desc) : rowgroup(rowgroup), desc(desc) {};
} PinnedRowGroup;
void UnlockRowGroups();

#endif // ENABLE_HTAP
#endif // IMCSTORE_AM_H
