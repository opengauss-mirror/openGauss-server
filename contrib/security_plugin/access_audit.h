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
 * access_audit.h
 * 
 * IDENTIFICATION
 *        src/contrib/security_plugin/access_audit.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ACCESS_AUDIT_H_
#define ACCESS_AUDIT_H_

#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "gs_audit_policy.h"
#include "nodes/params.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "pgaudit.h"

typedef gs_stl::gs_set<gs_stl::gs_string> _checked_tables;

bool handle_table_entry(RangeTblEntry *rte, int access_type, const policy_set *policy_ids,
    const policy_set *security_policy_ids, policy_result *pol_result);
void check_access_table(const policy_set *policy_ids, RangeVar *rel, int access_type, int object_type = O_TABLE,
    const char *as_command = "", const char *access_name = "");
void check_access_table(const policy_set *policy_ids, const char *name, int access_type, int object_type,
    const char *as_command = "");
void get_fqdn_by_relid(RangeTblEntry *rte, PolicyLabelItem *schema_name, Var *col_att = nullptr,
    PolicyLabelItem *view_full_column = nullptr);

void flush_policy_result(const policy_result *pol_result, int access_type, const char *as_command = "",  const char *access_name = "");
void flush_access_logs(AuditResult audit_result);
void save_access_logs(int type, const char *event);
void init_logs();
void destroy_logs();
void open_relation(List *list, Var *col_att, PolicyLabelItem *full_column, bool *is_found);
void handle_subquery(RangeTblEntry *rte, int commandType, policy_result *pol_result, _checked_tables *checked_tables,
    const policy_set *policy_ids, const policy_set *security_policy_ids, int *recursion_deep);
void audit_open_relation(List *list, Var *col_att, PolicyLabelItem *full_column, bool *is_found);
void access_audit_policy_run(const List* rtable, CmdType cmd_type);
void opfusion_unified_audit_executor(const PlannedStmt *plannedstmt);
#endif /* ACCESS_AUDIT_H_ */
