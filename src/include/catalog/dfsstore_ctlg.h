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
 * dfsstore_ctlg.h
 *        routines to support HdfsStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/dfsstore_ctlg.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFSSTORE_CTLG_H
#define DFSSTORE_CTLG_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/objectaddress.h"
#include "lib/stringinfo.h"
#include "utils/relcache.h"
#include "storage/buf/block.h"
#include "storage/dfs/dfs_connector.h"


/*
 * DropDfsTblInfo holds the some information when drop
 * a Dfs table. 
 */
typedef struct DropDfsTblInfo {
	DfsSrvOptions *srvOptions;
	StringInfo dfsTblPath;
    Oid tblSpaceOid;
}DropDfsTblInfo;


void createDfsDescTable(Relation rel, Datum reloptions);
StringInfo getDfsStorePath(Relation rel);
extern Oid getDfsDescTblOid(Oid mainTblOid);
extern Oid getDfsDescIndexOid(Oid mainTblOid);
extern List* getDataFiles(Oid tbloid);
extern void InsertDeletedFilesForTransaction(Relation dataDestRelation);
extern int64 getDFSRelSize(Relation rel);
extern  StringInfo getHDFSTblSpcStorePath(Oid tblSpcOid);

#endif
