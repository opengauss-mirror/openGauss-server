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
 * opfusion_uheaptablescan.cpp
 *        Definition of class UHeapTableScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_uheaptablescan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_uheaptablescan.h"

#include "access/tableam.h"

UHeapTableScanFusion::UHeapTableScanFusion(SeqScan *node, PlannedStmt *planstmt, ParamListInfo params)
    : ScanFusion(params, planstmt)
{
    m_node = node;
    m_reloid = getrelid(m_node->scanrelid, m_planstmt->rtable);
    m_targetList = m_node->plan.targetlist;

    m_tupDesc = ExecCleanTypeFromTL(m_targetList, false);

    m_targetList = m_node->plan.targetlist;
    m_rel = heap_open(m_reloid, AccessShareLock);
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);

    m_attrno = (int16 *)palloc(m_tupDesc->natts * sizeof(int16));

    m_values = (Datum *)palloc(RelationGetDescr(m_rel)->natts * sizeof(Datum));
    m_isnull = (bool *)palloc(RelationGetDescr(m_rel)->natts * sizeof(bool));

    m_tmpvals = (Datum *)palloc(m_tupDesc->natts * sizeof(Datum));
    m_tmpisnull = (bool *)palloc(m_tupDesc->natts * sizeof(bool));
    m_direction = (ScanDirection *)palloc0(sizeof(ScanDirection));

    setAttrNo();
    heap_close(m_rel, AccessShareLock);
}

void UHeapTableScanFusion::Init(long max_rows)
{
    m_rel = heap_open(m_reloid, AccessShareLock);
    m_scan = (UHeapScanDesc)UHeapBeginScan(m_rel, GetActiveSnapshot(), m_tupDesc->natts);
    *m_direction = ForwardScanDirection;
    Assert(m_scan != NULL);
}

void UHeapTableScanFusion::setAttrNo()
{
    ListCell *lc = NULL;
    int cur_resno = 1;
    TargetEntry *res = NULL;
    foreach (lc, m_targetList) {
        res = (TargetEntry *)lfirst(lc);
        if (res->resjunk) {
            continue;
        }

        m_attrno[cur_resno - 1] = res->resorigcol;
        cur_resno++;
    }
}

UHeapTuple UHeapTableScanFusion::getUTuple()
{
    return UHeapGetTupleFromPage(m_scan, ForwardScanDirection);
}

TupleTableSlot *UHeapTableScanFusion::getTupleSlot()
{
    do {
        UHeapTuple tuple = getUTuple();
        if (tuple == NULL) {
            return NULL;
        }

        Assert(tuple->tupTableType == UHEAP_TUPLE);

        tableam_tops_deform_tuple(tuple, RelationGetDescr(m_rel), m_values, m_isnull);

        /* mapping */
        for (int i = 0; i < m_tupDesc->natts; i++) {
            Assert(m_attrno[i] > 0);
            m_tmpvals[i] = m_values[m_attrno[i] - 1];
            m_tmpisnull[i] = m_isnull[m_attrno[i] - 1];
        }

        HeapTuple tmptup = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull, HEAP_TUPLE);
        Assert(tmptup != NULL);

        (void)ExecStoreTuple(tmptup, /* tuple to store */
            m_reslot,                /* slot to store in */
            InvalidBuffer,           /* TO DO: survey */
            false);                  /* don't pfree this pointer */
                                     /* AMNEEDED need to move the logic from heap to ustore */
        heap_slot_getsomeattrs(m_reslot, m_tupDesc->natts);

        return m_reslot;
    } while (1);
    return NULL;
}

void UHeapTableScanFusion::End(bool isCompleted)
{
    if (!isCompleted) {
        return;
    }

    UHeapEndScan((TableScanDesc)m_scan);

    if (m_rel != NULL) {
        heap_close(m_rel, AccessShareLock);
    }
    if (m_reslot != NULL) {
        (void)ExecClearTuple(m_reslot);
    }
}
