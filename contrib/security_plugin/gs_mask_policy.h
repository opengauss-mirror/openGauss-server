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
 * gs_mask_policy.h
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_mask_policy.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PGMASKING_POLICY_H_
#define PGMASKING_POLICY_H_

#include <string>
#include <time.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "catalog/gs_policy_label.h"
#include "catalog/gs_masking_policy.h"
#include "catalog/gs_masking_policy_actions.h"
#include "catalog/gs_masking_policy_filters.h"
#include "nodes/nodes.h"
#include "gs_policy_object_types.h"
#include "gs_policy_filter.h"
#include "gs_policy/policy_common.h"
#include "gs_policy/gs_policy_utils.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy/gs_set.h"
#include "gs_policy/gs_map.h"

/* <set limit, location> */
using LimitPair = std::pair<bool, int>;

enum MaskBehaviour {
    M_UNKNOWN,
    M_CREDIT_CARD,
    M_MASKALL,
    M_BASICEMAIL,
    M_FULLEMAIL,
    M_ALLDIGITS,
    M_SHUFFLE,
    M_RANDOM,
    M_REGEXP
};

struct GsMaskingAction {
    GsMaskingAction():m_func_id(0), m_policy_id(0), m_modify_date(0){}
    bool operator == (const GsMaskingAction &args) const 
    {
        return m_func_id == args.m_func_id && !strcasecmp(m_label_name.c_str(), args.m_label_name.c_str());
    }

    int m_func_id; /* MaskBehaviour enum */
    gs_stl::gs_string m_label_name;
    long long   m_policy_id;
    time_t      m_modify_date;
    gs_stl::gs_vector<gs_stl::gs_string> m_params;
};

void parse_params(const gs_stl::gs_string& arg, gs_stl::gs_vector<gs_stl::gs_string> *params);
struct PolicyAccessHash {
    size_t operator()(const GsMaskingAction& k) const;
};

int gs_maksing_action_cmp(const void *key1, const void *key2);

struct EqualToPolicyAccess {
    bool operator()(const GsMaskingAction& l, const GsMaskingAction& r) const;
};

typedef gs_stl::gs_set<GsMaskingAction, gs_maksing_action_cmp> pg_masking_action_set;
/* policy id */
typedef gs_stl::gs_map<long long, pg_masking_action_set> pg_masking_action_map;
/* db id */
typedef gs_stl::gs_map<int, pg_masking_action_map*> pg_masking_action_map_by_db;

bool load_masking_policies(bool reload = false);
gs_policy_set* get_masking_policies(const char *dbname = NULL);

bool load_masking_actions(bool reload = false);
pg_masking_action_map* get_masking_actions();

bool load_masking_policy_filters(bool reload = false);
void reset_masking_policy_filters();
gs_policy_filter_map* get_masking_filters();
bool is_masking_role_in_use(Oid roleid);

bool is_masking_role_in_use(const char* rolename);
bool reload_masking_policy();

bool check_masking_policy_filter(const FilterData *arg, policy_set *policy_ids);
bool check_masking_policy_action(const policy_set *policy_ids, const PolicyLabelItem *col_name,
    const PolicyLabelItem *view_name, int *masking_behavious, long long *polid,
    gs_stl::gs_vector<gs_stl::gs_string> *params);

typedef gs_stl::gs_set<gs_stl::gs_string> masking_column_set;
/* behaviour */
typedef gs_stl::gs_map<int, masking_column_set> masking_policy_result;
/* policy id */
typedef gs_stl::gs_map<long long, masking_policy_result> masking_result;

void flush_masking_result(const masking_result *result);
void set_reload_for_all_stmts();
bool prepare_stmt_is_reload(const char* name);
void unprepare_stmt(const char* name);
void prepare_stmt(const char* name);

bool validate_function_name(const char *func_name, const char* func_parameters,
    bool* invalid_params, bool is_audit);

bool is_masking_has_object(bool column_type_is_changed, const gs_stl::gs_string labelname);

bool check_masking_policy_actions_for_label(const policy_labels_map *labels_to_drop);

void validate_masking_function_name(const List* full_funcname, bool is_audit = true);
bool get_masking_policy_name_by_oid(Oid polid, gs_stl::gs_string *polname);
void clear_thread_local_masking();
#endif /* PGMASKING_POLICY_H_ */
