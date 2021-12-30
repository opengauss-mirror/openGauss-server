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
 * gs_audit_policy.cpp
 *    auditing flags and objects information for the policy.
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_audit_policy.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <ctype.h>
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "catalog/gs_auditing_policy.h"
#include "catalog/gs_auditing_policy_acc.h"
#include "catalog/gs_auditing_policy_filter.h"
#include "catalog/gs_auditing_policy_priv.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "storage/lock/lock.h"
#include "storage/spin.h"
#include "access/heapam.h"
#include "commands/tablespace.h"
#include "commands/dbcommands.h"
#include "utils/atomic.h"
#include "utils/snapmgr.h"
#include "gs_audit_policy.h"
#include "gs_policy_plugin.h"
#include "gs_policy_logical_tree.h"

/* audit policy */
static THR_LOCAL gs_policy_set *loaded_auditing_policies = NULL;

/* audit access */
static THR_LOCAL gs_policy_base_map *loaded_access = NULL;
static THR_LOCAL access_privilege_set *loaded_audited_access = NULL;

/* audit filter */
static THR_LOCAL gs_policy_filter_map *loaded_policy_filters = NULL;
static THR_LOCAL global_roles_in_use *audit_roles_in_use = NULL;

/* audit priv */
static THR_LOCAL gs_policy_base_map *loaded_privileges = NULL;
static THR_LOCAL access_privilege_set *loaded_audited_privileges = NULL;

static pg_atomic_uint64 audit_global_version = 1;
static pg_atomic_uint64 access_global_version = 1;
static pg_atomic_uint64 audit_filter_global_version = 1;
static pg_atomic_uint64 priv_global_version = 1;

THR_LOCAL pg_atomic_uint64 audit_local_version = 0;
THR_LOCAL pg_atomic_uint64 access_local_version = 0;
THR_LOCAL pg_atomic_uint64 filter_local_version = 0;
THR_LOCAL pg_atomic_uint64 priv_local_version = 0;

const char* get_access_name(int type)
{
    switch (type) {
        case CMD_SELECT:
            return "SELECT";
        case CMD_INSERT:
            return "INSERT";
        case CMD_UPDATE:
            return "UPDATE";
        case CMD_DELETE:
            return "DELETE";
        case CMD_TRUNCATE:
            return "TRUNCATE";
        case CMD_PREPARE:
            return "PREPARE";
        case CMD_DEALLOCATE:
            return "DEALLOCATE";
        case CMD_EXECUTE:
            return "EXECUTE";
        case CMD_REINDEX:
            return "REINDEX";
        default:
            return "NONE";
    }
    return "NONE";
}

int get_access_type(const char *name)
{
    if (!strcasecmp(name, "SELECT")) {
        return CMD_SELECT;
    }
    if (!strcasecmp(name, "INSERT")) {
        return CMD_INSERT;
    }
    if (!strcasecmp(name, "UPDATE")) {
        return CMD_UPDATE;
    }
    if (!strcasecmp(name, "DELETE")) {
        return CMD_DELETE;
    }
    if (!strcasecmp(name, "TRUNCATE")) {
        return CMD_TRUNCATE;
    }
    if (!strcasecmp(name, "PREPARE")) {
        return CMD_PREPARE;
    }
    if (!strcasecmp(name, "DEALLOCATE")) {
        return CMD_DEALLOCATE;
    }
    if (!strcasecmp(name, "EXECUTE")) {
        return CMD_EXECUTE;
    }
    if (!strcasecmp(name, "REINDEX")) {
        return CMD_REINDEX;
    }
    if (!strcasecmp(name, "COPY")) {
        return CMD_UTILITY;
    }
    if (!strcasecmp(name, "ALL")) {
        return CMD_UTILITY;
    }
    return CMD_UNKNOWN;
}

bool reload_audit_policy()
{
    load_policy_accesses(true);
    load_policy_privileges(true);
    load_audit_policies(true);
    /* load filters must be last */
    return load_policy_filters(true);
}

bool load_audit_policies(bool reload)
{
    if (!reload) {
        pg_atomic_add_fetch_u64(&audit_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&audit_global_version, (uint64*)&audit_local_version, audit_global_version)) {
        /* Latest audit policy, changes nothing */
        return false;
    }

    GET_RELATION(GsAuditingPolicyRelationId, AccessShareLock);
    TableScanDesc scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup;
    Form_gs_auditing_policy rel_data;

    gs_policy_set *tmp_policies = new gs_policy_set;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_auditing_policy)GETSTRUCT(rtup);
        if (rel_data == NULL || !rel_data->polenabled) {
            continue;
        }
        gs_base_policy item;
        item.m_id = HeapTupleGetOid(rtup);
        item.m_name = rel_data->polname.data;
        item.m_enabled = true;
        item.m_modify_date = rel_data->modifydate;
        tmp_policies->insert(item);
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    reset_policy_filters(); /* must reload filters */

    if (loaded_auditing_policies != NULL) {
        gs_policy_set *pol = loaded_auditing_policies;
        loaded_auditing_policies = tmp_policies;
        delete pol;
        pol = NULL;
    } else {
        loaded_auditing_policies = tmp_policies;
    }
    return true;
}

/*
 * get audit policy info from cache, Note that u_sess->proc_cxt.MyDatabaseId maybe invalid in some
 * senario so that need get the oid from dbname directlly
 */ 
gs_policy_set *get_audit_policies(const char *dbname)
{
    load_audit_policies(true);
    return loaded_auditing_policies;
}

bool load_policy_accesses(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }
    if (!reload) {
        pg_atomic_add_fetch_u64(&access_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&access_global_version, (uint64*)&access_local_version, access_global_version)) {
        /* Latest policy access, changes nothing */
        return false;
    }

    GET_RELATION(GsAuditingPolicyAccessRelationId, AccessShareLock);
    TableScanDesc scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup;
    Form_gs_auditing_policy_access rel_data;

    gs_policy_base_map *tmp_accesses = new gs_policy_base_map;
    access_privilege_set *tmp_access = new access_privilege_set;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_auditing_policy_access)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        GsPolicyBase item;
        item.m_type = get_access_type(rel_data->accesstype.data);
        tmp_access->insert(item.m_type);
        item.m_label_name = rel_data->labelname.data;
        item.m_modify_date = rel_data->modifydate;
        item.m_policy_id = rel_data->policyoid;
        (*tmp_accesses)[item.m_policy_id].insert(item);
    }   
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (loaded_access != NULL) {
        gs_policy_base_map *acc = loaded_access;
        loaded_access = tmp_accesses;
        delete acc;
        acc = NULL;
    } else {
        loaded_access = tmp_accesses;
    }

    /* update audited access */
    if (loaded_audited_access != NULL) {
        access_privilege_set *audit_acc = loaded_audited_access;
        loaded_audited_access = tmp_access;
        delete audit_acc;
        audit_acc = NULL;
    } else {
        loaded_audited_access = tmp_access;
    }

    return true;
}

bool check_audited_access(int access)
{
    load_policy_accesses(true);
    access_privilege_set *tmp = loaded_audited_access;
    if (tmp) {
        if (tmp->find(access) != tmp->end()) {
            return true;
        }
        /* all */
        return tmp->find(CMD_UTILITY) != tmp->end();
    }
    return false;
}

gs_policy_base_map *get_policy_accesses()
{
    (void)load_policy_accesses(true);
    return loaded_access;
}

void reset_policy_filters()
{
    pg_atomic_exchange_u64(&filter_local_version, 0);
}

bool load_policy_filters(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }
    if (!reload) {
        pg_atomic_add_fetch_u64(&audit_filter_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&audit_filter_global_version, (uint64*)&filter_local_version,
                                       audit_filter_global_version)) {
        /* Latest audit filter, changes nothing */
        return false;
    }

    GET_RELATION(GsAuditingPolicyFiltersRelationId, AccessShareLock);
    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup;
    Form_gs_auditing_policy_filters rel_data;
    gs_policy_filter_map *tmp_filters = new gs_policy_filter_map;
    global_roles_in_use *audit_roles_in_use_tmp = new global_roles_in_use;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_auditing_policy_filters)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }

        bool    isNull = true;
        Datum logical_operator_datum = heap_getattr(rtup, Anum_gs_auditing_policy_fltr_logical_operator, 
                                                    RelationGetDescr(rel), &isNull);
        const char *logical_operator = "";
        if (!isNull) {
            logical_operator = TextDatumGetCString(logical_operator_datum);
        }

        PolicyLogicalTree ltree;
        ltree.parse_logical_expression(logical_operator);
        ltree.get_roles(audit_roles_in_use_tmp);
        GsPolicyFilter item(ltree, rel_data->policyoid, rel_data->modifydate);
        set_filter(&item, tmp_filters);
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    /* add policies without filter */
    gs_policy_set* all_policies = get_audit_policies();
    if (all_policies) {
        gs_policy_set::const_iterator it = all_policies->begin();
        gs_policy_set::const_iterator eit = all_policies->end();
        for (; it != eit; ++it) {
            (*tmp_filters)[it->m_id];
        }
    }

    if (loaded_policy_filters != NULL) {
        gs_policy_filter_map *cur_filter = loaded_policy_filters;
        loaded_policy_filters = tmp_filters;
        delete cur_filter;
        cur_filter = NULL;
    } else {
        loaded_policy_filters = tmp_filters;
    }

    if (audit_roles_in_use != NULL) {
        global_roles_in_use *audit_role = audit_roles_in_use;
        audit_roles_in_use = audit_roles_in_use_tmp;
        delete audit_role;
        audit_role = NULL;
    } else {
        audit_roles_in_use = audit_roles_in_use_tmp;
    }

    return true;
}

gs_policy_filter_map* get_policy_filters()
{
    (void)load_policy_filters(true);
    return loaded_policy_filters;
}

bool check_audit_policy_filter(const FilterData *arg, policy_set *policy_ids, const char *dbname)
{
    return check_policy_filter(arg, policy_ids, get_audit_policies(dbname), get_policy_filters());
}


static bool verify_object_by_itself(const typed_labels *labels, const PolicyLabelItem *item)
{
    typed_labels::const_iterator it;
    if ((it = labels->find(item->m_obj_type)) != labels->end()) {
        return (it->second->find(*item) != it->second->end());
    }
    return false;
}

static bool check_label_by_type_and_object(const typed_labels labels, const PolicyLabelItem *item)
{
    /* check schema level */
    typed_labels::const_iterator it;
    if ((it = labels.find(O_SCHEMA)) != labels.end()) {
        PolicyLabelItem tmp(item->m_schema, 0, O_SCHEMA);
        if (it->second->find(tmp) != it->second->end()) {
            return true;
        }
    }

    if (item->m_obj_type == O_COLUMN) {
        /* check by table lebel */
        if ((it = labels.find(O_TABLE)) != labels.end()) {
            PolicyLabelItem tmp(item->m_schema, item->m_object, O_TABLE);
            if (it->second->find(tmp) != it->second->end()) {
                return true;
            }
        }
    }

    return verify_object_by_itself(&labels, item);
}

bool check_privileges(const policy_set *policy_ids, policy_simple_set *policy_result, int privileges_type,
    gs_policy_base_map *privileges, const PolicyLabelItem *item)
{
    if (privileges == NULL || privileges->empty() || policy_ids == NULL) {
        return false;
    }
    loaded_labels *all_label = get_policy_labels();
    /* check policy */
    policy_set::const_iterator pol_it = policy_ids->begin();
    policy_set::const_iterator pol_eit = policy_ids->end();
    for (; pol_it != pol_eit; ++pol_it) {
        gs_policy_base_map::const_iterator priv_it = privileges->find(pol_it->m_id);
        if (priv_it == privileges->end()) {
            continue;
        }
        const gs_policy_base_set& privileges = *(priv_it.second);
        gs_policy_base_set::const_iterator ait = privileges.begin();
        gs_policy_base_set::const_iterator aeit = privileges.end();
        for (; ait != aeit; ++ait) {
            /* check access type */
            if (ait->m_type == T_LOGIN) {
                if (privileges_type != T_LOGIN_SUCCESS && privileges_type != T_LOGIN_FAILURE) {
                    continue;
                }
            } else if (ait->m_type != privileges_type && ait->m_type != T_ALL) {
                continue;
            }
            /* check if label is not any */
            if (!strcasecmp(ait->m_label_name.c_str(), "all")) {
                policy_result->insert(pol_it->m_id);
                continue;
            }
            /* check label */
            if (all_label) {
                loaded_labels::const_iterator lit = all_label->find(ait->m_label_name);
                if (lit != all_label->end()) {
                    if (check_label_by_type_and_object((*(lit->second)), item)) {
                        policy_result->insert(pol_it->m_id);
                    }
                }
            }
        }
    }
    return !policy_result->empty();
}
bool check_audit_policy_privileges(const policy_set *policy_ids,
                                   policy_simple_set *policy_result,
                                   int privileges_type,
                                   const PolicyLabelItem *item,
                                   const char *dbname)
{
    return check_privileges(policy_ids, policy_result, privileges_type, get_policy_privileges(dbname), item);
}

/*
 *  Iterate policy labels to match the object labels to generate audit access info
 *  Note: when there is no label of object access, "all" is the default label
 */
bool check_audit_policy_access(const PolicyLabelItem *item, const PolicyLabelItem *view_item, int access_type,
    const policy_set *policy_ids, policy_result *pol_result, gs_policy_base_map *policy_access, int *block_behaviour)
{
    if (policy_access == NULL || policy_access->empty() || policy_ids == NULL) {
        return false;
    }

    /*
     * load all labels from access object
     */
    policy_label_map labels;
    policy_set::const_iterator pol_it = policy_ids->begin();
    policy_set::const_iterator pol_eit = policy_ids->end();
    for (; pol_it != pol_eit; ++pol_it) {
        const PolicyPair& pol_item = *pol_it;
        gs_policy_base_map::const_iterator acc_it = policy_access->find(pol_item.m_id);
        if (acc_it == policy_access->end()) {
            continue;
        }
        const gs_policy_base_set& accesses = *(acc_it.second);
        gs_policy_base_set::const_iterator ait = accesses.begin();
        gs_policy_base_set::const_iterator aeit = accesses.end();
        for (; ait != aeit; ++ait) {
            /* check access type */
            if (ait->m_type != access_type && ait->m_type != CMD_UTILITY) {
                continue;
            }

            labels[pol_item] = ait->m_label_name.c_str();
        }
    }

    /*
     * walk through all the access audit label info to match with label of object
     */
    if (labels.empty()) {
        return false;
    }

    bool has_column = false;
    PolicyLabelItem view_object_item(view_item->m_schema, view_item->m_object,
                                     O_COLUMN, view_item->m_column);
    AccessPair object_item("", item->m_obj_type);
    {
        gs_stl::gs_string value;
        item->get_fqdn_value(&value);
        object_item.first = value.c_str();
    }
    loaded_labels *tmp_labels = get_policy_labels();
    policy_label_map::const_iterator lit = labels.begin();
    policy_label_map::const_iterator elit = labels.end();
    for (; lit != elit; ++lit) {
        const PolicyPair &pol_item = *(lit.first);
        loaded_labels::const_iterator it;
        if (tmp_labels != NULL) {
            it = tmp_labels->find(lit->second->c_str());
        }
        if ((tmp_labels == NULL || it == tmp_labels->end()) && *(lit->second) == "all") {
            (*pol_result)[pol_item.m_id][object_item];
            if (pol_item.m_block_type > 0) {
                *block_behaviour = pol_item.m_block_type;
                return has_column;
            }
        } else if (it != tmp_labels->end()) {
            /* <label_type, PolicyLabelItem> */
            typed_labels::const_iterator fit = it->second->begin();
            typed_labels::const_iterator feit = it->second->end();
            for (; fit != feit; ++fit) {
                bool is_columnn = (*(fit->first) == O_COLUMN);
                has_column = has_column || is_columnn;
                if (*(fit->first) != item->m_obj_type && (*fit->first) != O_SCHEMA) {
                    continue;
                }

                const gs_policy_label_set &objects = *(fit->second);
                if (is_columnn) {
                    if (view_object_item.m_object && !view_object_item.empty() &&
                        objects.find(view_object_item) != objects.end()) {
                        gs_stl::gs_string value;
                        view_object_item.get_fqdn_value(&value);
                        object_item.first = value.c_str();

                        (*pol_result)[pol_item.m_id][object_item].insert(view_object_item.m_column);
                        if (pol_item.m_block_type > 0) {
                            *block_behaviour = pol_item.m_block_type;
                            return has_column;
                        }
                    }
                    if (objects.find(*item) != objects.end()) {
                        (*pol_result)[pol_item.m_id][object_item].insert(item->m_column);
                        if (pol_item.m_block_type > 0) {
                            *block_behaviour = pol_item.m_block_type;
                            return has_column;
                        }
                    }
                } else if (*(fit->first) == O_SCHEMA) {
                    PolicyLabelItem sch_item;
                    sch_item.m_schema = item->m_schema;
                    if (objects.find(sch_item) != objects.end()) {
                        (*pol_result)[pol_item.m_id][object_item];
                    }
                } else if (objects.find(*item) != objects.end()) {
                    (*pol_result)[pol_item.m_id][object_item];
                    if (pol_item.m_block_type > 0) {
                        *block_behaviour = pol_item.m_block_type;
                        return has_column;
                    }
                }
            }
        }
    }
    return has_column;
}

bool check_audited_privilige(int privilige)
{
    access_privilege_set *tmp = loaded_audited_privileges;
    if (tmp) {
        if (tmp->find(privilige) != tmp->end()) {
            return true;
        }
        return tmp->find(T_ALL) != tmp->end();
    }
    return false;
}

bool load_policy_privileges(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }

    if (!reload) {
        pg_atomic_add_fetch_u64(&priv_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&priv_global_version, (uint64*)&priv_local_version, priv_global_version)) {
        /* Latest audit priviledges, changes nothing */
        return false;
    }

    GET_RELATION(GsAuditingPolicyPrivilegesRelationId, AccessShareLock);
    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup;
    Form_gs_auditing_policy_privileges rel_data;

    gs_policy_base_map *tmp_privileges = new gs_policy_base_map;
    access_privilege_set *tmp_audited_priviliges = new access_privilege_set;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_auditing_policy_privileges)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        GsPolicyBase item;
        item.m_type = get_privilege_type(rel_data->privilegetype.data);
        tmp_audited_priviliges->insert(item.m_type);
        item.m_label_name = rel_data->labelname.data;
        item.m_modify_date = rel_data->modifydate;
        item.m_policy_id = rel_data->policyoid;
        (*tmp_privileges)[item.m_policy_id].insert(item);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (loaded_privileges != NULL) {
        gs_policy_base_map *priv = loaded_privileges;
        loaded_privileges = tmp_privileges;
        delete priv;
        priv = NULL;
    } else {
        loaded_privileges = tmp_privileges;
    }
    /* update audited privilege */
    if (loaded_audited_privileges != NULL) {
        access_privilege_set* audit_priv = loaded_audited_privileges;
        loaded_audited_privileges = tmp_audited_priviliges;
        delete audit_priv;
        audit_priv = NULL;
    } else {
        loaded_audited_privileges = tmp_audited_priviliges;
    }

    return true;
}

/* get privileges policy */
gs_policy_base_map* get_policy_privileges(const char *dbname)
{
    (void)load_policy_privileges(true);
    return loaded_privileges;
}

bool is_role_in_use(Oid roleid)
{
    reload_audit_policy();
    global_roles_in_use *tmp = audit_roles_in_use;
    return (tmp != NULL && tmp->find(roleid) != tmp->end());
}

bool check_audit_policy_privileges_for_label(const policy_labels_map *labels_to_drop)
{
    GET_RELATION(GsAuditingPolicyPrivilegesRelationId, RowExclusiveLock);

    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup;
    Form_gs_auditing_policy_privileges rel_data;
    bool is_found = false;
    while ((rtup = heap_getnext(scan, ForwardScanDirection)) && !is_found) {
        rel_data = (Form_gs_auditing_policy_privileges)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        is_found = (labels_to_drop->find(rel_data->privilegetype.data) != labels_to_drop->end());
    }

    heap_endscan(scan);
    heap_close(rel, RowExclusiveLock);

    return is_found;
}

bool check_audit_policy_access_for_label(const policy_labels_map *labels_to_drop)
{
    GET_RELATION(GsAuditingPolicyAccessRelationId, RowExclusiveLock);

    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup;
    Form_gs_auditing_policy_access rel_data;
    bool is_found = false;
    while ((rtup = heap_getnext(scan, ForwardScanDirection)) && !is_found) {
        rel_data = (Form_gs_auditing_policy_access)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        is_found = (labels_to_drop->find(rel_data->labelname.data) != labels_to_drop->end());
    }
    heap_endscan(scan);
    heap_close(rel, RowExclusiveLock);
    return is_found;
}

bool is_audit_policy_exist(const char *dbname)
{
    gs_policy_set *policies = get_audit_policies(dbname);
    if (policies == NULL) {
        return false;
    }
    return !policies->empty();
}

void clear_thread_local_auditing()
{
    if (loaded_auditing_policies != NULL) {
        delete loaded_auditing_policies;
        loaded_auditing_policies = NULL;
    }

    if (loaded_access != NULL) {
        delete loaded_access;
        loaded_access = NULL;
    }

    if (loaded_audited_access != NULL) {
        delete loaded_audited_access;
        loaded_audited_access = NULL;
    }

    if (loaded_policy_filters != NULL) {
        delete loaded_policy_filters;
        loaded_policy_filters = NULL;
    }

    if (audit_roles_in_use != NULL) {
        delete audit_roles_in_use;
        audit_roles_in_use = NULL;
    }

    if (loaded_privileges != NULL) {
        delete loaded_privileges;
        loaded_privileges = NULL;
    }

    if (loaded_audited_privileges != NULL) {
        delete loaded_audited_privileges;
        loaded_audited_privileges = NULL;
    }
}
