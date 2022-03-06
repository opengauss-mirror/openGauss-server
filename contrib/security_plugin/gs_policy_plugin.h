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
 * -------------------------------------------------------------------------
 *
 * gs_policy_plugin.h
 *
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_plugin.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_POLICY_PLUGIN_H_
#define GS_POLICY_PLUGIN_H_

#include <string>
#include "gs_policy_labels.h"
#include "gs_audit_policy.h"
#include "executor/executor.h"

#define MESSAGESIZE 2048

extern AccessControl_SecurityAuditObject_hook_type accesscontrol_securityAuditObject_hook;
extern Check_acl_privilige_hook_type check_acl_privilige_hook;
extern CheckSecurityAccess_hook_type CheckSecurityAccess_hook;
extern Reset_security_policies_hook_type reset_security_policies_hook; 
extern Reset_security_filters_hook_type reset_security_filters_hook; 
extern Reset_security_access_hook_type reset_security_access_hook; 
extern Reset_security_privilige_hook_type reset_security_privilige_hook; 
extern CheckSecurityPolicyFilter_hook_type checkSecurityPolicyFilter_hook;

typedef bool (*IsRoleInUse_hook_type)(Oid roleid);

void gs_audit_issue_syslog_message(const char* module, const char* message, int event_type, int result_type);
void get_remote_addr(IPV6 *ip);
const char* get_session_app_name();
const char* GetUserName(char* user_name, size_t user_name_size);
bool get_ipaddress(gs_stl::gs_string& ipaddress);
extern void set_result_set_function(const PolicyLabelItem &func);
void get_name_range_var(const RangeVar *rangevar, gs_stl::gs_string *buffer, bool enforce = true);
extern void load_database_policy_info();
bool is_audit_policy_exist_load_policy_info();

#endif /* GS_POLICY_PLUGIN_H_ */
