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
 * vecvar.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecvar.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECVAR_H_
#define VECVAR_H_

#include "vecexecutor/vectorbatch.h"
#include "utils/relcache.h"

#define GETSLOTID(val) (((val)&0x3FFFFFFFFFFFFFFFULL) >> 48)
#define GETCODE(val) ((val)&0x0000FFFFFFFFFFFFULL)
#define STORAGEVAR_SIZE_STEP 10

class StorageVar : public BaseObject {
public:
    // constructor  deconstructor
    //
    StorageVar();

    ~StorageVar();

    // set the Attr
    //
    void SetAttr(Relation rel, int colId);

    // decode the data.
    //
    Datum Decode(Size code);

private:
    // related colid and relation.
    int m_colId;

    Relation m_rel;
};

class StorageVarManager {
public:
    StorageVarManager();

    ~StorageVarManager();

    // add a variable storage var container.
    StorageVar* AddStorageVar();

    // get the storage variable length.
    StorageVar* GetStorageVar(int16 vindex);

    // Decode a value.
    Datum Decode(ScalarValue val);

private:
    StorageVar** m_varData;

    // storage var manager size.
    int m_size;

    // current storage var index.
    int m_index;

    // manager related context.
    //
    MemoryContext m_context;
};

#endif /* VECVAR_H_ */
