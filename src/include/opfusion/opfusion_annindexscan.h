/*
*   Copyright (c) 2020 Huawei Technologies Co.,Ltd.
*   openGauss is licensed under Mulan PSL v2.
*   You can use this software according to the terms and conditions of the Mulan PSL v2.
*   You may obtain a copy of Mulan PSL v2 at:
*        http://license.coscl.org.cn/MulanPSL2
*   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
*   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
*   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
*   See the Mulan PSL v2 for more details.
*   opfusion_annindexscan.h
*      Declaration of class annIndexScanFusion for bypass executor.
*   IDENTIFICATION
*      src/include/opfusion/opfusion_annindexscan.h
*/
#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_ANNINDEXSCAN_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_ANNINDEXSCAN_H_

#include "opfusion/opfusion_index.h"

class BaseGetTuple : public BaseObject {
public:
    virtual TupleTableSlot* getTuple(IndexFusion* index) = 0;
};
class GetATuple : public BaseGetTuple {
public:
    GetATuple(){};
    ~GetATuple(){};
    TupleTableSlot* getTuple(IndexFusion* index);
};
class GetUTuple : public BaseGetTuple {
public:
    GetUTuple(){};
    ~GetUTuple(){};
    TupleTableSlot* getTuple(IndexFusion* index);
};

class AnnIndexScanFusion : public IndexFusion {
public:
    AnnIndexScanFusion()
    {}

    ~AnnIndexScanFusion(){};

    AnnIndexScanFusion(AnnIndexScan* node, PlannedStmt* planstmt, ParamListInfo params);

    void Init(long max_rows);

    HeapTuple getTuple();

    UHeapTuple getUTuple();

    void End(bool isCompleted);

    TupleTableSlot* getTupleSlot();

    void ResetAnnIndexScanFusion(AnnIndexScan* node, PlannedStmt* planstmt, ParamListInfo params);

    void AnnRefreshParameterIfNecessary();

    void IndexOrderByKey(List* indexqual);

    void SetUseLimit();

    void setAttrMap();
    
    bool CheckTlistMatchesTupdesc();

private:
    int m_orderbyNum;
    ScanKey m_orderbyKeys;
    bool m_limit;
    struct AnnIndexScan* m_node;
    bool m_can_reused;
    BaseGetTuple* m_tuple;
public:
    int m_numSimpleVars;
    int m_lastScanVar;
    bool m_isustore;
    TupleTableSlot* m_scanslot;
};
#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_ANNINDEXSCAN_H_ */