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
 * imcs_hash_table.h
 *      routines to support IMColStore
 *
 * 
 * IDENTIFICATION
 *        src/include/access/htap/imcs_hash_table.h
 * 
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMCS_HASH_TABLE_H
#define IMCS_HASH_TABLE_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/relfilenode.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/hsearch.h"
#include "storage/lock/lwlock.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "storage/cu.h"
#include "storage/cache_mgr.h"
#include "storage/custorage.h"
#include "storage/cucache_mgr.h"

#define IMCSTORE_HASH_TAB_CAPACITY 50000
#define IMCS_HASH_TABLE (IMCSHashTable::GetInstance())

typedef enum {
    IMCS_POPULATE_INITIAL = 0,
    IMCS_POPULATE_COMPLETE = 1,
    IMCS_POPULATE_ONSTANDBY = 2,
    IMCS_POPULATE_ERROR = 3,

    // count status num, not real status
    IMCS_POPULATE_STATUS_NUM = 4,
} IMCSStatus;

const char IMCSStatusMap[IMCS_POPULATE_STATUS_NUM][NAMEDATALEN] = {
    "INITIAL",
    "COMPLETE",
    "ONSTANDBY",
    "ERROR"
};

/*
 * This class is to manage imcstore desc hash table.
 */
class IMCSHashTable : public BaseObject {
public:
    static IMCSHashTable* GetInstance(void);
    static void InitImcsHash();
    static void NewSingletonInstance(void);

public:
    void CreateImcsDesc(Relation rel, int2vector* imcsAttsNum, int imcsNatts, bool useShareMemroy = false);
    IMCSDesc* GetImcsDesc(Oid relOid);
    void UpdateImcsStatus(Oid relOid, int imcsStatus);
    void DeleteImcsDesc(Oid relOid, RelFileNode* relNode);
    void ClearImcsMem(Oid relOid, RelFileNode* relNode);
    void UpdatePrimaryImcsStatus(Oid relOid, int imcsStatus);
    bool HasInitialImcsTable();
    void FreeAllShareMemPool();
    void FreeAllBorrowMemPool();
    HTAB* m_imcs_hash;
    LWLock *m_imcs_lock;
    MemoryContext m_imcs_context;
    pg_atomic_uint64 m_xlog_latest_lsn;

private:
    IMCSHashTable()
    {}
    ~IMCSHashTable()
    {}
    static IMCSHashTable* m_imcs_hash_tbl;
};

#endif /* IMCS_HASH_TABLE_H */