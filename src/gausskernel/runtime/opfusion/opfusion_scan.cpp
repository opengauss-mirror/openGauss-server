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
 * opfusion_scan.cpp
 *        The implementation for the scan operator of bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/opfusion_scan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "opfusion/opfusion_scan.h"

#include "access/tableam.h"
#include "executor/node/nodeIndexscan.h"
#include "executor/node/nodeIndexonlyscan.h"
#include "opfusion/opfusion.h"
#include "opfusion/opfusion_indexscan.h"
#include "opfusion/opfusion_indexonlyscan.h"
#include "opfusion/opfusion_util.h"
#include "utils/snapmgr.h"
#include "nodes/plannodes.h"

ScanFusion::ScanFusion(ParamListInfo params, PlannedStmt* planstmt)
{
    m_params = params;
    m_planstmt = planstmt;
    m_rel = NULL;
    m_parentRel = NULL;
    m_tupDesc = NULL;
    m_reslot = NULL;
    m_direction = NULL;
    m_partRel = NULL;
};

ScanFusion* ScanFusion::getScanFusion(Node* node, PlannedStmt* planstmt, ParamListInfo params)
{
    ScanFusion* scan = NULL;

    switch (nodeTag(node)) {
        case T_IndexScan:
            scan = New(CurrentMemoryContext) IndexScanFusion((IndexScan*)node, planstmt, params);
            break;

        case T_IndexOnlyScan:
            scan = New(CurrentMemoryContext) IndexOnlyScanFusion((IndexOnlyScan*)node, planstmt, params);
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when executing executor node fusion.", (int)nodeTag(node))));
            break;
    }

    return scan;
}

void ScanFusion::refreshParameter(ParamListInfo params)
{
    m_params = params;
}
/* IndexFetchPart */
IndexFusion::IndexFusion(ParamListInfo params, PlannedStmt* planstmt) : ScanFusion(params, planstmt)
{
    m_direction = NULL;
    m_attrno = NULL;
    m_targetList = NULL;
    m_epq_indexqual = NULL;
    m_tmpvals = NULL;
    m_isnull = NULL;
    m_values = NULL;
    m_reloid = 0;
    m_paramNum = 0;
    m_tmpisnull = NULL;
    m_keyNum = 0;
    m_paramLoc = NULL;
    m_scandesc = NULL;
    m_scanKeys = NULL;
    m_parentRel = NULL;
    m_partRel = NULL;
    m_index = NULL;
    m_parentIndex = NULL;
    m_partIndex = NULL;
    m_keyInit = false;
}