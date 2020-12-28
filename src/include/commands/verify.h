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
 * verify.h
 *        header file for anlayse verify commands to check the datafile.
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/verify.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef VERIFY_H
#define VERIFY_H

#include "pgxc/pgxcnode.h"
#include "tcop/tcopprot.h"
#include "dfsdesc.h"
#include "access/htup.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_user_status.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufmgr.h"
#include "storage/cu.h"
#include "storage/lock/lock.h"
#include "storage/remote_read.h"
#include "utils/relcache.h"

extern void DoGlobalVerifyMppTable(VacuumStmt* stmt, const char* queryString, bool sentToRemote);
extern void DoGlobalVerifyDatabase(VacuumStmt* stmt, const char* queryString, bool sentToRemote);
extern void DoVerifyTableOtherNode(VacuumStmt* stmt, bool sentToRemote);
extern void VerifyAbortBufferIO(void);

#endif /* VERIFY_H */
