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
 * opfusion_indexonlyscan.h
 *        Declaration of class IndexOnlyScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_indexonlyscan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_INDEXONLYSCAN_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_INDEXONLYSCAN_H_

#include "opfusion/opfusion_index.h"

class IndexOnlyScanFusion : public IndexFusion {
public:
    IndexOnlyScanFusion()
    {}

    ~IndexOnlyScanFusion(){};

    IndexOnlyScanFusion(IndexOnlyScan* node, PlannedStmt* planstmt, ParamListInfo params);

    void Init(long max_rows);

    HeapTuple getTuple();

    UHeapTuple getUTuple();

    void End(bool isCompleted);

    TupleTableSlot* getTupleSlot();
    TupleTableSlot *getTupleSlotInternal();

private:
    struct IndexOnlyScan* m_node;

    Buffer m_VMBuffer;
};
#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_INDEXONLYSCAN_H_ */