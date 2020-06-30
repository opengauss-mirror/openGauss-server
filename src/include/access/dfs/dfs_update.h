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
 * dfs_update.h
 *        routines to support dfs
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_update.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_UPDATE_H
#define DFS_UPDATE_H

#include "access/cstore_update.h"
#include "access/dfs/dfs_insert.h"
#include "access/dfs/dfs_delete.h"

class DfsUpdate : public BaseObject {
public:
    DfsUpdate(Relation, EState *, Plan *plan = NULL);
    virtual ~DfsUpdate();

    virtual void Destroy();

    /* initialize the memory info */
    void InitUpdateMemArg(Plan *plan = NULL);

    /*
     * initialize sort cache with tuple descriptor.
     */
    void InitSortState(TupleDesc sortTupDesc);

    /*
     * do "update" job really, and return the number of updated rows.
     */
    uint64 ExecUpdate(VectorBatch *batch, int options);

    /*
     * called at last step, make sure that data in cache can be deleted and new
     * data can be inserted.
     */
    void EndUpdate(int options);

private:
    /*
     * target relation to be updated
     */
    Relation m_relation;

    /*
     * objects used to implement "delete" and "insert"
     */
    DfsDelete *m_delete;
    DfsInsertInter *m_insert;

    /*
     * state data used on update
     */
    ResultRelInfo *m_resultRelInfo;
    EState *m_estate;

    /*
     * the memory info to  "delete" and "insert"
     */
    MemInfoArg *m_dfsUpDelMemInfo;
    MemInfoArg *m_dfsUpInsMemInfo;
};

#endif /* DFS_UPDATE_H */
