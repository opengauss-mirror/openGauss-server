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
 *   vecconstraints.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecconstraints.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.h"

#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "optimizer/clauses.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"

static char* ExecBuildBatchValueDescription(VectorBatch* batch, int nrows, int maxfieldlen)
{
    StringInfoData buf;
    int i;

    initStringInfo(&buf);

    appendStringInfoChar(&buf, '(');

    for (i = 0; i < batch->m_cols; i++) {
        char* val = NULL;
        int vallen;
        ScalarVector* pVec = &(batch->m_arr[i]);

        if (pVec->IsNull(nrows))
            val = "null";
        else {
            Oid foutoid;
            bool typisvarlena = false;
            Datum tempVal;

            getTypeOutputInfo(pVec->m_desc.typeId, &foutoid, &typisvarlena);
            if (pVec->m_desc.encoded)
                tempVal = pVec->Decode(pVec->m_vals[nrows]);
            else
                tempVal = pVec->m_vals[nrows];

            val = OidOutputFunctionCall(foutoid, tempVal);
        }

        if (i > 0)
            appendStringInfoString(&buf, ", ");

        vallen = strlen(val);
        if (vallen <= maxfieldlen)
            appendStringInfoString(&buf, val);
        else {
            vallen = pg_mbcliplen(val, vallen, maxfieldlen);
            appendBinaryStringInfo(&buf, val, vallen);
            appendStringInfoString(&buf, "...");
        }
    }

    appendStringInfoChar(&buf, ')');

    return buf.data;
}

void ExecVecConstraints(ResultRelInfo* resultRelInfo, VectorBatch* batch, EState* estate)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleConstr* constr = rel->rd_att->constr;

    Assert(constr);

    if (constr->has_not_null) {
        int natts = rel->rd_att->natts;
        int attrChk;

        for (attrChk = 1; attrChk <= natts; attrChk++) {
            if (rel->rd_att->attrs[attrChk - 1]->attnotnull) {
                int nrow = batch->m_rows;
                ScalarVector* pVector = &(batch->m_arr[attrChk - 1]);
                for (int i = 0; i < nrow; i++)
                    if (pVector->IsNull(i))
                        ereport(ERROR,
                            (errcode(ERRCODE_NOT_NULL_VIOLATION),
                                errmsg("null value in column \"%s\" violates not-null constraint",
                                    NameStr(rel->rd_att->attrs[attrChk - 1]->attname)),
                                errdetail("Failing row contains %s.", ExecBuildBatchValueDescription(batch, i, 64))));
            }
        }
    }

    if (constr->num_check > 0) {
        ereport(ERROR, (errcode(ERRCODE_CHECK_VIOLATION), errmsg("Un-support CHECK constraint")));
    }
}
