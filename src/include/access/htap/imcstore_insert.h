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
 * imcstore_insert.h
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/imcstore_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMCSTORE_INSERT_H
#define IMCSTORE_INSERT_H

#include "access/cstore_insert.h"

/*
 * This class provides some API for batch insert in ImColStore.
 */
class IMCStoreInsert : public CStoreInsert {
public:
    IMCStoreInsert(_in_ Relation relation, _in_ TupleDesc imcsTupleDescWithCtid, _in_ int2vector* imcsAttsNum);

    ~IMCStoreInsert();
    void Destroy() override;

    void RemoveCUFromCache(int col, uint32 cuid, CUDesc* origdesc);
    void BatchInsertCommon(uint32 cuid);
    void BatchReInsertCommon(IMCSDesc* imcsDesc, uint32 cuid, TransactionId xid);
    void AppendOneTuple(Datum *values, const bool *isnull);
    void ResetBatchRows(bool reuse_blocks);
    CU *FormCU(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr) override;
    Oid m_relOid;
};

#endif /* IMCSTORE_INSERT_H */
