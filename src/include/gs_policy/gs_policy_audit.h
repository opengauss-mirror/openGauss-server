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
 * gs_auditing_policy_priv.h
 *     Main enterance for Auditing Policy
 * 
 * 
 * IDENTIFICATION
 *        src/include/gs_policy/gs_policy_audit.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_POLICY_AUDIT_H
#define GS_POLICY_AUDIT_H

#include "access/xact.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "catalog/pg_proc.h"
#include "catalog/indexing.h"

#include "gs_policy/gs_string.h"
#include "nodes/parsenodes.h"
#include "postgres.h"
#include "storage/lock/lock.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

extern void create_audit_policy(CreateAuditPolicyStmt *stmt);
extern void alter_audit_policy(AlterAuditPolicyStmt *stmt);
extern void drop_audit_policy(DropAuditPolicyStmt *stmt);
void construct_resource_name(const RangeVar *rel, gs_stl::gs_string *target_name_s);

#endif

