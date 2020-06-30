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
 * ts_store_insert.h
 *      inserter of timeseries data
 *
 * IDENTIFICATION
 *        src/include/tsdb/ts_store_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TSDB_TS_STORE_INSERT_H
#define TSDB_TS_STORE_INSERT_H

#include "utils/rel.h"
#include "utils/relcache.h"
#include "vecexecutor/vectorbatch.h"
class DataRowVector;
class DataRow;
struct PartitionIdentifier;

class TsStoreInsert : public BaseObject {
public:
    TsStoreInsert(Relation relation);
    virtual ~TsStoreInsert();
    bool batch_insert(_in_ Datum* values, _in_ const bool* nulls, _in_ int options);
    void BatchInsert(VectorBatch* batch, int hi_options);
    bool end_batch_insert();
};

#endif /* TSDB_TS_STORE_INSERT_H */

