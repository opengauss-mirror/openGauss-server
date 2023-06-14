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
 * opfusion_indexscan.h
 *        Declaration of class IndexScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_indexscan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_INDEXSCAN_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_INDEXSCAN_H_

#include "opfusion/opfusion_index.h"

class IndexScanFusion : public IndexFusion {
public:
    IndexScanFusion()
    {}

    ~IndexScanFusion(){};

    IndexScanFusion(IndexScan* node, PlannedStmt* planstmt, ParamListInfo params);

    void Init(long max_rows);

    HeapTuple getTuple();

    UHeapTuple getUTuple();

    void End(bool isCompleted);

    TupleTableSlot* getTupleSlot();

    void ResetIndexScanFusion(IndexScan* node, PlannedStmt* planstmt, ParamListInfo params);
private:
    struct IndexScan* m_node;
    bool m_can_reused;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_INDEXSCAN_H_ */