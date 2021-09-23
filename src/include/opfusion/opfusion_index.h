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
 * opfusion_index.h
 *        Declaration of class IndexFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_index.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_INDEX_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_INDEX_H_

#include "opfusion/opfusion_scan.h"

class IndexFusion : public ScanFusion {
public:
    IndexFusion(ParamListInfo params, PlannedStmt* planstmt);

    IndexFusion()
    {}

    void refreshParameterIfNecessary();

    void BuildNullTestScanKey(Expr* clause, Expr* leftop, ScanKey this_scan_key);

    void IndexBuildScanKey(List* indexqual);

    virtual void Init(long max_rows) = 0;

    virtual HeapTuple getTuple() = 0;

    virtual UHeapTuple getUTuple() = 0;

    void setAttrNo();

    virtual TupleTableSlot* getTupleSlot() = 0;

    bool EpqCheck(Datum* values, const bool* isnull);

    void UpdateCurrentRel(Relation* rel);
    
    Relation m_index; /* index relation */

    Relation m_parentIndex; /* index parent relation in partiton */

    Partition m_partIndex;

    Oid m_reloid; /* relation oid of range table */

    IndexScanDesc m_scandesc;

    List* m_epq_indexqual; /* indexqual list */

    bool m_keyInit; /* true if m_scanKeys has been initialized */

    int m_keyNum; /* num of scan key */

    ScanKey m_scanKeys;

    ParamLoc* m_paramLoc; /* location of m_params, include paramId and the location in indexqual */

    int m_paramNum;

    Datum* m_values;

    bool* m_isnull;

    Datum* m_tmpvals; /* for mapping m_values */

    bool* m_tmpisnull; /* for mapping m_isnull */

    List* m_targetList;

    int16* m_attrno; /* target attribute number, length is m_tupDesc->natts */

};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_INDEX_H_ */