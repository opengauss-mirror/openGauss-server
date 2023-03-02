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
 * sec_rls_cmds.h
 *        Security Module.
 *        Variables and function commands used in Row Level Security Policy.
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/sec_rls_cmds.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_RLS_CMDS_H
#define SEC_RLS_CMDS_H

#include "nodes/parsenodes.h"

extern void CreateRlsPolicy(CreateRlsPolicyStmt* stmt);
extern void AlterRlsPolicy(AlterRlsPolicyStmt* stmt);
extern ObjectAddress RenameRlsPolicy(RenameStmt* stmt);
extern void RemoveRlsPolicyById(Oid rlsPolicyOid);
extern bool RemoveRoleFromRlsPolicy(Oid roleid, Oid rlsrelid, Oid rlspolicyid);
extern void RelationBuildRlsPolicies(Relation relation);
extern Oid get_rlspolicy_oid(Oid relid, const char* policy_name, bool missing_ok);
extern bool RelationHasRlspolicy(Oid relid);
extern void CreateRlsPolicyForSystem(
    char* schemaName, char* relName, char* policyName, char* funcName, char* relColname, char* privType);

#endif /* SEC_RLS_CMDS_H */
