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
 * cstore_ctlg.h
 *        routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/cstore_ctlg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_CTLG_H
#define	CSTORE_CTLG_H

#include "utils/relcache.h"
#include "nodes/parsenodes.h"

extern void	AlterCStoreCreateTables(Oid relOid, Datum reloptions, CreateStmt *mainTblStmt);

extern bool	CreateDeltaTable(Relation rel, Datum reloptions, bool isPartition, CreateStmt *mainTblStmt);
extern bool	CreateCUDescTable(Relation rel, Datum reloptions, bool isPartition);
extern bool createDeltaTableForPartition(Oid relOid, Oid partOid, Datum reloptions, CreateStmt *mainTblStmt);
extern bool createCUDescTableForPartition(Oid relOid, Oid partOid, Datum reloptions);
extern Datum AddInternalOption(Datum reloptions, int mask);
extern Datum AddOrientationOption(Datum relOptions, bool isColStore);

#endif

