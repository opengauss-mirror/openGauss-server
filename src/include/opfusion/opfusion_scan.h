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
 * opfusion_scan.h
 *        Declaration of base class ScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_scan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_SCAN_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_SCAN_H_

#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_uscan.h"
#include "commands/prepare.h"
#include "executor/node/nodeIndexonlyscan.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "pgxc/pgxcnode.h"
#include "storage/buf/buf.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

struct ParamLoc {
    int paramId;
    int scanKeyIndx;
};

class ScanFusion : public BaseObject {
public:
    ScanFusion();

    ScanFusion(ParamListInfo params, PlannedStmt* planstmt);

    static ScanFusion* getScanFusion(Node* node, PlannedStmt* planstmt, ParamListInfo params);

    void refreshParameter(ParamListInfo params);

    virtual void Init(long max_rows) = 0;

    virtual HeapTuple getTuple() = 0;

    virtual UHeapTuple getUTuple() = 0;

    virtual void End(bool isCompleted) = 0;

    virtual bool EpqCheck(Datum* values, const bool* isnull) = 0;

    virtual void UpdateCurrentRel(Relation* rel) = 0;

    virtual void setAttrNo() = 0;

    virtual TupleTableSlot* getTupleSlot() = 0;

    ParamListInfo m_params;

    PlannedStmt* m_planstmt;

    Relation m_rel;

    Relation m_parentRel;

    Partition m_partRel;

    TupleTableSlot* m_reslot;

    TupleDesc m_tupDesc;

    ScanDirection* m_direction;

    uint64 start_row = 0;
    uint64 get_rows = 0;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_SCAN_H_ */
