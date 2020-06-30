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
 * dfs_delete.h
 *        routines to support DFS(such as HDFS, DFS,...)
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_delete.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_DELETE_H
#define DFS_DELETE_H

#include "access/cstore_delete.h"
#include "dfsdesc.h"

class DfsDelete : public CStoreDelete {
public:
    DfsDelete(Relation, EState *, bool is_update = false, Plan *plan = NULL, MemInfoArg *m_dfsInsertMemInfo = NULL);
    virtual ~DfsDelete();

    /*
     * save batch to sort cache
     */
    void PutDeleteBatch(VectorBatch *, JunkFilter *);

    /*
     * called on deinitialization, make sure that tids in sort cache can be
     * deleted.
     */
    uint64 ExecDelete();

    /*
     * do "delete" job really with tids in sort cache, and return the number of
     * deleted rows.
     */
    uint64 ExecDeleteForTable();

protected:
    /*
     * used to handle desc table, such as mark deletemap, ...
     */
    DFSDescHandler *m_handler;
};

#endif /* DFS_DELETE_H */
