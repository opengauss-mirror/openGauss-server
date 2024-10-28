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
 * imcustorage.h
 *         routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/imcustorage.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef IMCU_STORAGE_H
#define IMCU_STORAGE_H

#include "storage/custorage.h"
#include "storage/cu.h"
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/fd.h"
#include "storage/cstore/cstorealloc.h"

#ifdef ENABLE_HTAP
class IMCUStorage : public CUStorage {
public:
    IMCUStorage(const CFileNode& cFileNode);
    ~IMCUStorage() {};
    void Destroy() override;

    // Write CU data into storage
    //
    void SaveCU(char* writeBuf, _in_ uint32 cuId, _in_ int size);

    // Load CU data from storage
    //
    void LoadCU(_in_ CU* cuPtr, _in_ uint32 cuId, _in_ int size, _in_ IMCSDesc* imcsDesc);

    // Load data from file into outbuf
    //
    void Load(_in_ uint32 cuId, _in_ int size, __inout char* outbuf);

    void TryRemoveCUFile(_in_ uint32 cuId, _in_ int colId);

    void InitFileNamePrefix(_in_ const CFileNode& cFileNode) override;

    File WSOpenFile(char* file_name, int fileId, bool direct_flag) override;
};

#endif /* ENABLE_HTAP */
#endif /* IMCU_STORAGE_H */