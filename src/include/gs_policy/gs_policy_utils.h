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
 *     Declaration of functions for dealing with policy records
 * 
 * 
 * IDENTIFICATION
 *        src/include/gs_policy/gs_policy.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _GS_POLICY_UTILS_H
#define _GS_POLICY_UTILS_H

#include "access/heapam.h"
#include "access/tableam.h"
#include "nodes/parsenodes.h"
#include "policy_common.h"
#include "gs_vector.h"
#include "postgres.h"
#include "pgaudit.h"
#include "utils/snapmgr.h"

#define MAX_POLICIES_NUM  98
#define MAX_IP_LEN 129

enum {POLICY_OPT_UNKNOWN, POLICY_OPT_PRIVILEGES, POLICY_OPT_ACCESS, POLICY_OPT_FILTER, POLICY_OPT_BLOCK};

static inline int get_option_type(const char *option_type)
{
    if (!strcasecmp(option_type, "privileges")) {
        return POLICY_OPT_PRIVILEGES;
    } 
    if (!strcasecmp(option_type, "access"))
        return POLICY_OPT_ACCESS;
    if (!strcasecmp(option_type, "filter"))
        return POLICY_OPT_FILTER;

    return POLICY_OPT_UNKNOWN;
}

enum {POLICY_CMD_UNKNOWN, POLICY_CMD_ADD, POLICY_CMD_REMOVE, POLICY_CMD_MODIFY, POLICY_CMD_DROP_FILTER};

static inline int get_policy_command_type(const char *command)
{
    if (!strcasecmp(command, "add")) {
        return POLICY_CMD_ADD;
    }
    if (!strcasecmp(command, "remove")) {
        return POLICY_CMD_REMOVE;
    }
    if (!strcasecmp(command, "modify")) {
        return POLICY_CMD_MODIFY;
    }
    if (!strcasecmp(command, "drop_filter")) {
        return POLICY_CMD_DROP_FILTER;
    }
    return POLICY_CMD_UNKNOWN;
}

struct GsPolicyStruct {
    GsPolicyStruct(const gs_stl::gs_string name = ""):m_id(0), m_name(name), m_enabled(false){}
    bool operator == (const GsPolicyStruct &arg) const;
    bool operator < (const GsPolicyStruct &arg) const;
    int operator - (const GsPolicyStruct &arg) const;
    long long   m_id;
    gs_stl::gs_string m_name;
    gs_stl::gs_string m_comments;
    bool m_enabled;
};

/* set of existing policies keyed by name */
typedef gs_stl::gs_set<GsPolicyStruct> policies_set;

struct PgPolicyFiltersStruct {
    bool operator == (const PgPolicyFiltersStruct &arg) const;
    bool operator < (const PgPolicyFiltersStruct &arg) const;
    int  operator - (const PgPolicyFiltersStruct &arg) const;
    long long m_id;
    gs_stl::gs_string m_type;
    gs_stl::gs_string m_label_name;
    gs_stl::gs_string m_tree_string;
    long long m_policy_oid;
};

/* set of existing filters keyed by type & label name */
typedef gs_stl::gs_set<PgPolicyFiltersStruct> filters_set;

struct PgPolicyMaskingActionStruct {
    bool operator == (const PgPolicyMaskingActionStruct &arg) const;
    bool operator < (const PgPolicyMaskingActionStruct &arg) const;
    int  operator - (const PgPolicyMaskingActionStruct &arg) const;
    long long m_id;
    gs_stl::gs_string m_type;
    gs_stl::gs_string m_params;
    gs_stl::gs_string m_label_name;
    long long m_policy_oid;
};
typedef gs_stl::gs_set<PgPolicyMaskingActionStruct> masking_actions_set;

struct PgPolicyPrivilegesAccessStruct {
    PgPolicyPrivilegesAccessStruct() : m_id(0), m_type(""), m_label_name(""), m_policy_oid(0) {}
    PgPolicyPrivilegesAccessStruct(const PgPolicyPrivilegesAccessStruct &arg)
    {
        m_id = arg.m_id;
        m_type = arg.m_type;
        m_label_name = arg.m_label_name;
        m_policy_oid = arg.m_policy_oid;
    }
    bool operator == (const PgPolicyPrivilegesAccessStruct &arg) const;
    bool operator < (const PgPolicyPrivilegesAccessStruct &arg) const;
    int operator - (const PgPolicyPrivilegesAccessStruct &arg) const;
    long long m_id;
    gs_stl::gs_string m_type;
    gs_stl::gs_string m_label_name;
    long long   m_policy_oid;
};

/* set of existing privileges keyed by type & label name */
typedef gs_stl::gs_set<PgPolicyPrivilegesAccessStruct> privileges_access_set;

template <typename T> 
extern void load_existing_policies(Relation rel, policies_set *existing_policies)
{
    if (rel == nullptr)
        return;
    HeapTuple   rtup;
    T rel_data;
    GsPolicyStruct item;
    TableScanDesc scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL)
            continue;
        item.m_id = HeapTupleGetOid(rtup);
        item.m_name = rel_data->polname.data;
        item.m_enabled = rel_data->polenabled;
        existing_policies->insert(item);
    }
    tableam_scan_end(scan);
}

template <typename T> 
extern size_t get_num_of_existing_policies(Relation rel)
{
    size_t amount = 0;
    if (rel == nullptr)
        return amount;
    HeapTuple   rtup;
    T rel_data;
    TableScanDesc scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    while (scan && (rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL)
            continue;
        amount++;
    }
    heap_endscan(scan);
    return amount;
}

template <typename T>
void drop_policy_by_name(unsigned int rel_oid, const char  *polname, 
                         gs_stl::gs_set<long long> *ids)
{
    Relation relation = heap_open(rel_oid, RowExclusiveLock);
    if (!relation)
        return;
    HeapTuple   rtup;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        T rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        if (!strcasecmp(polname, rel_data->polname.data)) {
            ids->insert(HeapTupleGetOid(rtup));
            simple_heap_delete(relation, &rtup->t_self);
        }
    }
    tableam_scan_end(scan);

    heap_close(relation, RowExclusiveLock);
}

template <typename T>
void drop_policy_reference(unsigned int rel_oid, long long policy_oid)
{
    Relation relation = heap_open(rel_oid, RowExclusiveLock);
    if (!relation)
        return;
    HeapTuple   rtup;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        T rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        if ((long long)rel_data->policyoid == policy_oid) {
            simple_heap_delete(relation, &rtup->t_self);
        }
    }
    tableam_scan_end(scan);
 
    heap_close(relation, RowExclusiveLock);
}

template <typename T>
void load_existing_filters(Relation relation, filters_set *fltrs, long long policy_oid = 0)
{
    if (!relation)
        return;
    HeapTuple   rtup;
    T rel_data;
    PgPolicyFiltersStruct item;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        item.m_id = HeapTupleGetOid(rtup);
        item.m_type = rel_data->filtertype.data;
        item.m_label_name = rel_data->labelname.data;
        item.m_policy_oid = (long long)(rel_data->policyoid);
        /* when policy_oid is zero - load all privileges
         * when policy_oid greater than zero, load only matching
         */
        if ((policy_oid == 0) || ((policy_oid > 0) && (item.m_policy_oid == policy_oid)))
            fltrs->insert(item);
    }
    tableam_scan_end(scan);
}

template <typename T>
void load_existing_privileges(int relationId, privileges_access_set *privs, long long policy_oid = 0)
{
    Relation relation = heap_open(relationId, RowExclusiveLock);
    if (!relation)
        return;
    HeapTuple   rtup;
    T rel_data;
    PgPolicyPrivilegesAccessStruct item;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL)
            continue;
        item.m_id = HeapTupleGetOid(rtup);
        item.m_type = rel_data->privilegetype.data;
        item.m_label_name = rel_data->labelname.data;
        item.m_policy_oid = (long long)(rel_data->policyoid);
        /* load only matching privileges to policy id */
        if (item.m_policy_oid == policy_oid) {
            privs->insert(item);
        }
    }
    tableam_scan_end(scan);
    heap_close(relation, RowExclusiveLock);
}

template <typename T>
void load_existing_access(int relationId, privileges_access_set *acc, long long policy_oid = 0)
{
    Relation relation = heap_open(relationId, RowExclusiveLock);
    if (!relation) {
        return;
    }
    HeapTuple   rtup;
    T rel_data;
    PgPolicyPrivilegesAccessStruct item;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (T)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        item.m_id = HeapTupleGetOid(rtup);
        item.m_type = rel_data->accesstype.data;
        item.m_label_name = rel_data->labelname.data;
        item.m_policy_oid = (long long)(rel_data->policyoid);
        /* load only matching access to policy id */
        if (item.m_policy_oid == policy_oid) {
            acc->insert(item);
        }
    }
    tableam_scan_end(scan);
    heap_close(relation, RowExclusiveLock);
}

/* Process new filters from parser tree and tranform them into string */
extern bool process_new_filters(const List *policy_filters, gs_stl::gs_string *flat_tree);

extern bool verify_ip_role_app(const char *obj_type, const char *obj_value, gs_stl::gs_string *return_value);

/* Parses & validates (recursively) polish-notation format string into logical tree; on error uses ereport() */
extern bool validate_logical_expression(const gs_stl::gs_string logical_expr_str, int *offset);

extern bool scan_to_delete_from_relation(long long row_id, Relation relation, unsigned int index_id);

extern void construct_resource_name(const RangeVar *rel, gs_stl::gs_string *target_name_s);

bool handle_target(ListCell *target,
                   int opt_type,
                   bool is_add,
                   gs_stl::gs_string *err_msg,
                   privileges_access_set *access_to_add, privileges_access_set *access_to_remove,
                   privileges_access_set *privs_to_add, privileges_access_set *privs_to_remove,
                   const policy_labels_map *existing_labels,
                   const GsPolicyStruct *policy, const char *acc_action_type);

void save_manage_message(const char *message);
void send_manage_message(AuditResult result_type);

extern void get_session_ip(char *session_ip, int len);
extern void get_client_ip(const struct sockaddr* remote_addr, char *ip_str);

bool is_database_valid(const char* dbname);

/* create and release temp resource owner should use together to solve the empty owner of table */
ResourceOwnerData* create_temp_resourceowner();
void release_temp_resourceowner(ResourceOwnerData* resource_owner);

#endif
