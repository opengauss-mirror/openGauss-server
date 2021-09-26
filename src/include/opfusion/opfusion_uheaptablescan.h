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
 * opfusion_uheaptablescan.h
 *        Declaration of class UHeapTableScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_uheaptablescan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_UHEAPTABLESCAN_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_UHEAPTABLESCAN_H_

#include "opfusion/opfusion.h"

class UHeapTableScanFusion : public ScanFusion {
public:
    UHeapTableScanFusion() {};

    UHeapTableScanFusion(SeqScan *node, PlannedStmt *planstmt, ParamListInfo params);

    ~UHeapTableScanFusion() {};

    void Init(long max_rows);

    void setAttrNo();
    HeapTuple getTuple()
    {
        return NULL;
    };

    UHeapTuple getUTuple();

    void End(bool isCompleted);

    TupleTableSlot *getTupleSlot();
    bool EpqCheck(Datum *values, const bool *isnull)
    {
        return true;
    }

    Oid m_reloid; /* relation oid of range table */

    List *m_targetList;

    int16 *m_attrno; /* target attribute number, length is m_tupDesc->natts */

    Datum *m_values;
    bool *m_isnull;

    Datum *m_tmpvals;  /* for mapping m_values */
    bool *m_tmpisnull; /* for mapping m_isnull */

private:
    SeqScan *m_node;
    UHeapScanDesc m_scan;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_UHEAPTABLESCAN_H_ */