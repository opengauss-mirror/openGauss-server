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
 *     Declaration of functions for define auditing policy
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_audit_policy.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _PGAUDIT_POLICY_H
#define _PGAUDIT_POLICY_H

#include <string>
#include <time.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <boost/functional/hash.hpp>
#include "postgres.h"
#include "catalog/gs_policy_label.h"
#include "gs_policy_object_types.h"
#include "gs_policy_filter.h"
#include "gs_policy/policy_common.h"
#include "iprange/iprange.h"
#include "gs_policy_logical_tree.h"
#include "gs_policy/gs_string.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy/gs_set.h"

bool load_audit_policies(bool reload = false);
gs_policy_set* get_audit_policies(const char *dbname = NULL);
bool is_audit_policy_exist(const char *dbname = NULL);

bool load_policy_accesses(bool reload = false);
gs_policy_base_map* get_policy_accesses();

bool load_policy_privileges(bool reload = false);
gs_policy_base_map* get_policy_privileges(const char *dbname = NULL);

bool check_audited_access(int access);
bool check_audited_privilige(int privilige);

bool load_policy_filters(bool reload = false);
void reset_policy_filters();
gs_policy_filter_map* get_policy_filters();
bool is_role_in_use(Oid roleid);

bool reload_audit_policy();

bool check_audit_policy_filter(const FilterData *arg, policy_set *policy_ids, const char *dbname = NULL);

const char* get_access_name(int type);

bool check_audit_policy_access(const PolicyLabelItem *item,
                               const PolicyLabelItem *view_item,
                               int access_type, const policy_set *policy_ids,
                               policy_result *pol_result,
                               gs_policy_base_map *policy_access,
                               int *block_behaviour);

using LimitPair = std::pair<bool /* set limit */, int /* location */>;
typedef gs_stl::gs_set<long long> policy_simple_set;

bool check_privileges(const policy_set *policy_ids, policy_simple_set *policy_result, int privileges_type,
                      gs_policy_base_map *privileges, const PolicyLabelItem *item);

bool check_audit_policy_privileges(const policy_set *policy_ids,
                                   policy_simple_set *policy_result,
                                   int privileges_type,
                                   const PolicyLabelItem *item,
                                   const char *dbname = NULL);

bool check_audit_policy_privileges_for_label(const policy_labels_map *labels_to_drop);

bool check_audit_policy_access_for_label(const policy_labels_map *labels_to_drop);

int get_access_type(const char *name);

void clear_thread_local_auditing();

typedef bool (*AccessControl_SecurityAuditObject_hook_type)(const policy_set *policy_ids, 
                                                            const PolicyLabelItem *item, 
                                                            int priv_type, const char *priv_name);
typedef bool (*Check_acl_privilige_hook_type)(int privilige);
typedef bool (*CheckSecurityAccess_hook_type)(const policy_set *policy_ids,
                                              policy_result *pol_result,
                                              const PolicyLabelItem *item,
                                              const PolicyLabelItem *view_item,
                                              int access_type, bool is_column,
                                              LimitPair *limitCount,
                                              int location);
typedef void (*Reset_security_policies_hook_type)();
typedef void (*Reset_security_filters_hook_type)();
typedef void (*Reset_security_access_hook_type)();
typedef void (*Reset_security_privilige_hook_type)();

typedef bool (*CheckSecurityPolicyFilter_hook_type)(const FilterData arg, policy_set *policy_ids);
typedef bool (*Security_isRoleInUse_hook_type)(Oid roleid);
typedef bool (*Security_Check_acl_privilige_hook_type)(int privilige);
typedef bool (*Reload_security_policy_hook_type)();

#endif

