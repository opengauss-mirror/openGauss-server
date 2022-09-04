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
 * gs_policy_audit.cpp
 *    grammer for create/update/delete auding policy informations into catalog
 * 
 * IDENTIFICATION
 *    src/gausskernel/security/gs_policy/gs_policy_audit.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/xact.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "catalog/gs_auditing_policy.h"
#include "catalog/gs_policy_label.h"
#include "catalog/gs_auditing_policy_acc.h"
#include "catalog/gs_auditing_policy_priv.h"
#include "catalog/gs_auditing_policy_filter.h"
#include "catalog/pg_proc.h"
#include "catalog/indexing.h"
#include "commands/user.h"
#include "gs_policy/gs_policy_audit.h"
#include "gs_policy/policy_common.h"
#include "gs_policy/gs_policy_utils.h"
#include "nodes/parsenodes.h"
#include "postgres.h"
#include "pgxc/pgxc.h"
#include "storage/lock/lock.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "pgaudit.h"

LoadPoliciesPtr load_audit_policies_hook = NULL;
LoadPolicyAccessPtr load_policy_access_hook = NULL;
LoadPolicyPrivilegesPtr load_policy_privileges_hook = NULL;
LoadPolicyFilterPtr load_policy_filter_hook = NULL;
THR_LOCAL LightUnifiedAuditExecutorPtr light_unified_audit_executor_hook = NULL;
OpFusionUnifiedAuditExecutorPtr opfusion_unified_audit_executor_hook = NULL;
OpFusionUnifiedAuditFlushLogsPtr opfusion_unified_audit_flush_logs_hook = NULL;

static const char* privileges_type[] = { "alter", "analyze", "comment", "create", "drop", "grant", "revoke",
                                         "set", "show", "login_any", "login_failure", "login_success", "logout"};

static const char* access_type[] = {"copy", "deallocate", "delete", "execute", "insert", "prepare", "reindex",
                                    "select", "truncate", "update"};

/* reserve for masking with function name get_option_type */
/* reserver for maksing with function name construct_resource_name */

/* 
 * @Description : Adds specified access / privilege to label 
 * @is_access : true means ACCESS; false means PRIVILEGES
 * @action_type : specified access / privilege
 * @target_name_s : label name
 * @relation : relation to add configuration
 * @policyOid : policy id 
 */
static void add_action_type(bool is_access, const char *action_type, const gs_stl::gs_string target_name_s,
    Relation relation, Oid policyOid)
{
    HeapTuple   policy_htup = NULL;
    if (is_access) {
        bool    pol_nulls[Natts_gs_auditing_policy_acc] = {false};
        Datum   pol_values[Natts_gs_auditing_policy_acc] = {0};
        pol_values[Anum_gs_auditing_policy_acc_type - 1] = DirectFunctionCall1(namein, CStringGetDatum(action_type));
        pol_values[Anum_gs_auditing_policy_acc_label_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(target_name_s.c_str()));
        pol_values[Anum_gs_auditing_policy_acc_policy_oid - 1] = ObjectIdGetDatum(policyOid);
        pol_values[Anum_gs_auditing_policy_acc_modify_date - 1] = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
        policy_htup = heap_form_tuple(relation->rd_att, pol_values, pol_nulls);
    } else {
        bool    pol_nulls[Natts_gs_auditing_policy_priv] = {false};
        Datum   pol_values[Natts_gs_auditing_policy_priv] = {0};
        pol_values[Anum_gs_auditing_policy_priv_type- 1] = DirectFunctionCall1(namein, CStringGetDatum(action_type));
        pol_values[Anum_gs_auditing_policy_priv_label_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(target_name_s.c_str()));
        pol_values[Anum_gs_auditing_policy_priv_policy_oid - 1] = ObjectIdGetDatum(policyOid);
        pol_values[Anum_gs_auditing_policy_priv_modify_date - 1] = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
        policy_htup = heap_form_tuple(relation->rd_att, pol_values, pol_nulls);
    }
    /* Do the insertion */
    (void)simple_heap_insert(relation, policy_htup);
    CatalogUpdateIndexes(relation, policy_htup);
    heap_freetuple(policy_htup);
}

/*
 * Handle 'all' syntax in auding access expression operations; 
 */
static void handle_add_remove_all_types(int opt_type, privileges_access_set *add_actions,
    privileges_access_set *rem_actions, const privileges_access_set *exist_actions, bool is_add, long long polID,
    const char *object)
{
    /* add all supported type */
    PgPolicyPrivilegesAccessStruct item;
    item.m_label_name = "all";
    item.m_policy_oid = polID;
    if (strcasecmp(object, "all")) {
        item.m_type = object;
        if (is_add) {
            if (exist_actions->find(item) == exist_actions->end()) {
                (void)add_actions->insert(item);
            }
        } else {
            privileges_access_set::const_iterator it = exist_actions->find(item);
            if (it != exist_actions->end()) {
                (void)rem_actions->insert(*it);
            }
        }
        return;
    }

    int array_size = (opt_type == POLICY_OPT_ACCESS) ? (sizeof(access_type) / sizeof(access_type[0])) :
        (sizeof(privileges_type) / sizeof(privileges_type[0]));

    for (int i = 0; i < array_size; ++i) {
        item.m_type = (opt_type == POLICY_OPT_ACCESS) ? access_type[i] : privileges_type[i];
        if (is_add) {
            if (exist_actions->find(item) == exist_actions->end()) {
                (void)add_actions->insert(item);
            }
        } else {
            (void)rem_actions->insert(item);
        }
    }
}

/**
 * Adds all possible access / priviliges to label 
 * @is_access - true means ACCESS; false means PRIVILEGES
 * @target_name_s - label name
 * @relation - relation to add configuration
 * @policyOid - policy id 
 */
static inline void add_all_supported_types(bool is_access, const gs_stl::gs_string target_name_s, 
                                        Relation relation, Oid policyOid)
{
    /* add all supported types */
    int array_size = is_access ? (sizeof(access_type) / sizeof(access_type[0])) : 
        (sizeof(privileges_type) / sizeof(privileges_type[0]));
    for (int i = 0; i < array_size; ++i) {
        const char *action_type = is_access ? access_type[i] : privileges_type[i];
        add_action_type(is_access, action_type, target_name_s, relation, policyOid);
    }
}

/**
 * Parse resource labels associated with auditing policy according to the request
 * @is_access - true means ACCESS; false means PRIVILEGES
 * @action_type - SELECT/INSERT/.../ALL
 * @policy_item - list of labels
 * @relation - relation to add configuration
 * @policyOid - policy id
 */
static void add_labels_to_policy(bool is_access, const char *action_type, DefElem *policy_item, Relation relation,
    Oid policyOid)
{
    List *targets = policy_item ? (List *) policy_item->arg : NULL; /* labels list */
    ListCell *target_name = NULL;

    /* no targets - means for all db objects */
    if (targets == NULL) {
        if (strcasecmp(action_type, "all") == 0) {
            add_all_supported_types(is_access, "all", relation, policyOid);
        } else {
            add_action_type(is_access, action_type, "all", relation, policyOid);
        }
        return;
    }

    /* one or more targets */
    foreach (target_name, targets) {
        RangeVar *rel = (RangeVar*)lfirst(target_name);
        gs_stl::gs_string target_name_s;
        construct_resource_name(rel, &target_name_s);
        if (target_name_s == "all") { /* no validation whether "ALL" label exists */
            if (strcasecmp(action_type, "all") == 0) {
                add_all_supported_types(is_access, "all", relation, policyOid);
            } else {
                add_action_type(is_access, action_type, "all", relation, policyOid);
            }
        } else {
            if (verify_label_hook) { /* validate whether this label exists */
                if (!verify_label_hook(target_name_s.c_str())) {
                    heap_close(relation, RowExclusiveLock);
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("[%s] no such label found", target_name_s.c_str())));
                }
            }
            if (strcasecmp(action_type, "all") == 0) {
                add_all_supported_types(is_access, target_name_s, relation, policyOid);
            } else {
                add_action_type(is_access, action_type, target_name_s, relation, policyOid);
            }
        }
    }
}

/**
 * Update labels for access or privileges infomation and insert into catalog, since each policy 
 * is only for access or privileges, flag is needed to distinct.
 * @is_access : Mask if this is for access syntax.
 * @actions : actual DDL operations that can be audited.
 * @policy : auditing oplicy information associated with the gs_auditing_policy catalog
 * @relation : access catalog or privilege catalog.
 */
static void add_labels_to_privileges_access(bool is_access, const privileges_access_set *actions,
    const GsPolicyStruct *policy, Relation relation)
{
    for (privileges_access_set::const_iterator it = actions->begin(); it != actions->end(); ++it) {
        HeapTuple   policy_htup = NULL;
        const char *action_type = it->m_type.c_str();
        gs_stl::gs_string target_name_s = it->m_label_name;
        Oid policyOid = policy->m_id; 
        if (verify_label_hook) {
            if (!verify_label_hook(target_name_s.c_str())) {
                heap_close(relation, RowExclusiveLock);
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), 
                    errmsg("[%s] no such label found", target_name_s.c_str())));
            }
        }

        if (is_access) {
            bool    policy_nulls[Natts_gs_auditing_policy_acc] = {false};
            Datum    policy_values[Natts_gs_auditing_policy_acc] = {0};
            policy_values[Anum_gs_auditing_policy_acc_type - 1] = DirectFunctionCall1(namein, CStringGetDatum(action_type));
            policy_values[Anum_gs_auditing_policy_acc_label_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(target_name_s.c_str()));
            policy_values[Anum_gs_auditing_policy_acc_policy_oid - 1] = ObjectIdGetDatum(policyOid);
            policy_values[Anum_gs_auditing_policy_acc_modify_date - 1] = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
            policy_htup = heap_form_tuple(relation->rd_att, policy_values, policy_nulls);
        } else {
            bool    policy_nulls[Natts_gs_auditing_policy_priv] = {false};
            Datum    policy_values[Natts_gs_auditing_policy_priv] = {0};
            policy_values[Anum_gs_auditing_policy_priv_type- 1] = DirectFunctionCall1(namein, CStringGetDatum(action_type));
            policy_values[Anum_gs_auditing_policy_priv_label_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(target_name_s.c_str()));
            policy_values[Anum_gs_auditing_policy_priv_policy_oid - 1] = ObjectIdGetDatum(policyOid);
            policy_values[Anum_gs_auditing_policy_priv_modify_date - 1] = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
            policy_htup = heap_form_tuple(relation->rd_att, policy_values, policy_nulls);
        }
        /* Do the insertion */
        (void)simple_heap_insert(relation, policy_htup);

        CatalogUpdateIndexes(relation, policy_htup);
        heap_freetuple(policy_htup);
    }
}

static bool remove_labels_from_privileges_access(bool is_access, const privileges_access_set *actions, 
    privileges_access_set *existing_actions, Relation relation, gs_stl::gs_string *err_msg)
{
    bool is_deleted = false;
    for (privileges_access_set::const_iterator it = actions->begin(); it != actions->end(); ++it) {
        /* Removing access or privilege from policy having only one item is not allowed */
        if (existing_actions->size() == 1) {
            *err_msg = (is_access) ? "Removing auditing access from policy with a single item not allowed" :
                "Removing auditing privilege from policy with a single item not allowed";
            break;
        }
        privileges_access_set::iterator i_it = existing_actions->find(*it);
        if (i_it != existing_actions->end()) {
            if (!scan_to_delete_from_relation(i_it->m_id, relation, is_access ? GsAuditingPolicyAccessOidIndexId :
                GsAuditingPolicyPrivilegesOidIndexId))
                break;
            (void)existing_actions->erase(i_it);
            is_deleted = true;
        }
    }
    return is_deleted;
}

/**
 * Add filter expr information into auditing policy.
 */
static void add_filters(const filters_set *filters_to_add, Relation relation)
{
    Datum       curtime;
    HeapTuple   policy_filters_htup;
    bool        policy_filters_nulls[Natts_gs_auditing_policy_filters];
    Datum       policy_filters_values[Natts_gs_auditing_policy_filters];
    errno_t     rc;
    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    for (filters_set::const_iterator it = filters_to_add->begin(); it != filters_to_add->end(); ++it) {
        /* restore values and nulls for insert new node group record */
        rc = memset_s(policy_filters_values, sizeof(policy_filters_values), 0, sizeof(policy_filters_values));
        securec_check(rc, "\0", "\0");
        rc =  memset_s(policy_filters_nulls, sizeof(policy_filters_nulls), false, sizeof(policy_filters_nulls));
        securec_check(rc, "\0", "\0");

        policy_filters_values[Anum_gs_auditing_policy_fltr_filter_type - 1] = DirectFunctionCall1(namein, CStringGetDatum(it->m_type.c_str()));
        policy_filters_values[Anum_gs_auditing_policy_fltr_label_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(it->m_label_name.c_str()));
        policy_filters_values[Anum_gs_auditing_policy_fltr_logical_operator - 1] =
            CStringGetTextDatum(it->m_tree_string.c_str());
        policy_filters_values[Anum_gs_auditing_policy_fltr_policy_oid - 1] = ObjectIdGetDatum(it->m_policy_oid);
        policy_filters_values[Anum_gs_auditing_policy_fltr_modify_date - 1] = curtime;
        policy_filters_htup = heap_form_tuple(relation->rd_att, policy_filters_values, policy_filters_nulls);
        /* Do the insertion */
        (void)simple_heap_insert(relation, policy_filters_htup);

        CatalogUpdateIndexes(relation, policy_filters_htup);
        heap_freetuple(policy_filters_htup);
    }
}

/**
 * Update filter expr information into auditing policy.
 */
static void update_filters(const filters_set *filters_to_update, Relation policy_filters_relation)
{
    for (filters_set::const_iterator it = filters_to_update->begin(); it != filters_to_update->end(); ++it) {
        ScanKeyData scanKey[1];
        ScanKeyInit(&scanKey[0], Anum_gs_auditing_policy_fltr_policy_oid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(it->m_policy_oid));

        /* Search tuple by index */
        SysScanDesc scanDesc = systable_beginscan(policy_filters_relation, GsAuditingPolicyFiltersPolicyOidIndexId, 
                                                  true, NULL, 1, scanKey);

        HeapTuple auditingPolicyTuple = systable_getnext(scanDesc);
        if (!HeapTupleIsValid(auditingPolicyTuple)) {
            add_filters(filters_to_update, policy_filters_relation); /* curtime */
        } else {
            Datum values[Natts_gs_auditing_policy_filters] = { 0 };
            bool  nulls[Natts_gs_auditing_policy_filters] = { false };
            bool  replaces[Natts_gs_auditing_policy_filters] = { false };
            errno_t rc;
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "", "");
            rc =  memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
            securec_check(rc, "", "");
            rc =  memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
            securec_check(rc, "", "");

            values[Anum_gs_auditing_policy_fltr_logical_operator - 1] = CStringGetTextDatum(it->m_tree_string.c_str());
            nulls[Anum_gs_auditing_policy_fltr_logical_operator - 1] = false;
            replaces[Anum_gs_auditing_policy_fltr_logical_operator - 1] = true;

            HeapTuple newtuple = heap_modify_tuple(auditingPolicyTuple, RelationGetDescr(policy_filters_relation),
                values, nulls, replaces);
            simple_heap_update(policy_filters_relation, &newtuple->t_self, newtuple);
            CatalogUpdateIndexes(policy_filters_relation, newtuple);
        }
        systable_endscan(scanDesc);
    }
}

static void handle_alter_add_update_filter(List *filter, Oid policyOid, bool to_add)
{
    if (filter == NULL) {
        return;
    }

    gs_stl::gs_string flat_tree;
    if (!process_new_filters(filter, &flat_tree)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Unsupported policy filter values")));
    }

    filters_set filters_to_alter;
    if (flat_tree.size() > 0) {
        PgPolicyFiltersStruct item;
        item.m_type = "logical_expr";
        item.m_label_name = "logical_expr";
        item.m_tree_string = flat_tree;
        item.m_policy_oid = policyOid;
        (void)filters_to_alter.insert(item);
    }

    if (filters_to_alter.size() > 0) {
        Relation policy_filters_relation = heap_open(GsAuditingPolicyFiltersRelationId, RowExclusiveLock);
        if (policy_filters_relation) {
            if (to_add) {
                add_filters(&filters_to_alter, policy_filters_relation);
            } else {
                update_filters(&filters_to_alter, policy_filters_relation);
            }
            heap_close(policy_filters_relation, RowExclusiveLock);
        }
    }
}

/*
 * Load existing auditing policy about DML synatax.
 */
static void load_existing_privileges(privileges_access_set *privs, long long policy_oid)
{
    Relation relation = heap_open(GsAuditingPolicyPrivilegesRelationId, RowExclusiveLock);
    if (relation == NULL) {
        return;
    }

    HeapTuple rtup;
    Form_gs_auditing_policy_privileges rel_data;
    PgPolicyPrivilegesAccessStruct item;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_auditing_policy_privileges)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        item.m_id = HeapTupleGetOid(rtup);
        item.m_type = rel_data->privilegetype.data;
        item.m_label_name = rel_data->labelname.data;
        item.m_policy_oid = (long long)(rel_data->policyoid);
        /* load only matching privileges to policy id */
        if (item.m_policy_oid == policy_oid) {
            (void)privs->insert(item);
        }
    }
    tableam_scan_end(scan);

    heap_close(relation, RowExclusiveLock);
}

/*
 * Load existing auditing policy about DDL synatax.
 */
static void load_existing_access(privileges_access_set *acc, long long policy_oid)
{
    Relation relation = heap_open(GsAuditingPolicyAccessRelationId, RowExclusiveLock);
    if (!relation) {
        return;
    }

    HeapTuple rtup;
    Form_gs_auditing_policy_access rel_data;
    PgPolicyPrivilegesAccessStruct item;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_auditing_policy_access)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        item.m_id = HeapTupleGetOid(rtup);
        item.m_type = rel_data->accesstype.data;
        item.m_label_name = rel_data->labelname.data;
        item.m_policy_oid = (long long)(rel_data->policyoid);
        /* load only matching access to policy id */
        if (item.m_policy_oid == policy_oid) {
            (void)acc->insert(item);
        }
    }
    tableam_scan_end(scan);

    heap_close(relation, RowExclusiveLock);
}


static bool update_policy(const GsPolicyStruct *policy, Relation relation, bool policy_status_changed,
    gs_stl::gs_string *err_msg)
{
    bool        policy_nulls[Natts_gs_auditing_policy];
    bool        policy_replaces[Natts_gs_auditing_policy];
    Datum       policy_values[Natts_gs_auditing_policy];
    Datum       curtime;
    errno_t     rc;

    /* restore values and nulls for insert new node group record */
    rc = memset_s(policy_values, sizeof(policy_values), 0, sizeof(policy_values));
    securec_check(rc, "", "");
    rc =  memset_s(policy_nulls, sizeof(policy_nulls), false, sizeof(policy_nulls));
    securec_check(rc, "", "");
    rc =  memset_s(policy_replaces, sizeof(policy_replaces), false, sizeof(policy_replaces));
    securec_check(rc, "", "");

    ScanKeyData skey;
    /* Find the policy row to update */
    ScanKeyInit(&skey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(policy->m_id));

    /*
     * set up for heap-or-index scan, not need to check tgscan as systable_getnext will deal with sysscan->irel = NULL
     */
    SysScanDesc tgscan = systable_beginscan(relation, GsAuditingPolicyOidIndexId, true, NULL, 1, &skey);
    HeapTuple tup;
    tup = systable_getnext(tgscan);
    if (!tup || !HeapTupleIsValid(tup)) {
        systable_endscan(tgscan);
        (void)err_msg->append("could not find tuple for policy ");
        (void)err_msg->append(policy->m_name);
        return false;
    }

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    if (policy_status_changed) {
        policy_replaces[Anum_gs_auditing_policy_pol_enabled     - 1] = true;
        policy_values[Anum_gs_auditing_policy_pol_enabled     - 1] = BoolGetDatum(policy->m_enabled);
    } else { /* in case that comments changed */
        policy_replaces[Anum_gs_auditing_policy_pol_comments - 1] = true;
        policy_values[Anum_gs_auditing_policy_pol_comments     - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(policy->m_comments.c_str()));
    }
    policy_replaces[Anum_gs_auditing_policy_pol_modify_date - 1] = true;
    policy_values[Anum_gs_auditing_policy_pol_modify_date - 1] = curtime;
    HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation),
                                           policy_values, policy_nulls, policy_replaces);
    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);
    systable_endscan(tgscan);
    return true;
}


/*
 * function name: create_audit_policy
 * description  : create auditing policy
 */
void create_audit_policy(CreateAuditPolicyStmt *stmt)
{
    /* check that if has access to config audit policy */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    char user_name[USERNAME_LEN] = {0};
    (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));
    char buff[512] = {0};
    char session_ip[MAX_IP_LEN] = {0};
    get_session_ip(session_ip, MAX_IP_LEN);

    int ret = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
        "user name: [%s], app_name: [%s], ip: [%s], CREATE AUDIT POLICY [%s], TYPE: [%s]",
        user_name, u_sess->attr.attr_common.application_name, session_ip, stmt->policy_name, stmt->policy_type);
    securec_check_ss(ret, "\0", "\0");
    save_manage_message(buff);

    const char *policy_type = stmt->policy_type;
    const char *policy_name = stmt->policy_name;
    bool        policy_enabled = stmt->policy_enabled;
    Datum       curtime; 
    ListCell   *policy_target_item = NULL;

    errno_t     rc;
    HeapTuple   policy_htup;
    bool        policy_nulls[Natts_gs_auditing_policy] = {false};
    Datum       policy_values[Natts_gs_auditing_policy] = {0};

    /* restore values and nulls for insert new node group record */
    rc = memset_s(policy_values, sizeof(policy_values), 0, sizeof(policy_values));
    securec_check(rc, "", "");
    rc =  memset_s(policy_nulls, sizeof(policy_nulls), false, sizeof(policy_nulls));
    securec_check(rc, "", "");

    /* start to process policy */
    Relation policy_relation = heap_open(GsAuditingPolicyRelationId, RowExclusiveLock);
    if (!policy_relation) {
        /* generate an error */
        send_manage_message(AUDIT_FAILED);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s", "failed to open policies relation")));
        return;
    }

    /* less than MAX_POLICIES_NUM is recommended */
    if (get_num_of_existing_policies<Form_gs_auditing_policy>(policy_relation) >= MAX_POLICIES_NUM) {
        ereport(WARNING,
            (errmsg("%s", "Too many policies, adding new policy is not recommended")));
    }

    /* check whether such policy exists */
    policies_set existing_policies;
    load_existing_policies<Form_gs_auditing_policy>(policy_relation, &existing_policies);

    GsPolicyStruct cur_policy;
    cur_policy.m_name = policy_name;
    policies_set::iterator it = existing_policies.find(cur_policy);
    if (it != existing_policies.end()) { /* policy already exists */
        heap_close(policy_relation, RowExclusiveLock);
        /* while the 'if not exists' is specified generate a notice, else an error */
        if (stmt->if_not_exists == true) {
            send_manage_message(AUDIT_OK);
            ereport(NOTICE, (errmsg("%s policy already exists, create skipping", policy_name)));
        } else {
            send_manage_message(AUDIT_FAILED);
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_POLICY),
                     errmsg("%s policy already exists, create failed", policy_name)));
        }
        return;
    }

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    policy_values[Anum_gs_auditing_policy_pol_name        - 1] = DirectFunctionCall1(namein, CStringGetDatum(policy_name));
    policy_values[Anum_gs_auditing_policy_pol_comments    - 1] = DirectFunctionCall1(namein, CStringGetDatum(""));
    policy_values[Anum_gs_auditing_policy_pol_modify_date - 1] = curtime;
    policy_values[Anum_gs_auditing_policy_pol_enabled     - 1] = BoolGetDatum(policy_enabled);
    policy_htup = heap_form_tuple(policy_relation->rd_att, policy_values, policy_nulls);
    /* Do the insertion */
    (void)simple_heap_insert(policy_relation, policy_htup);
    CatalogUpdateIndexes(policy_relation, policy_htup);
    Oid policyOid = HeapTupleGetOid(policy_htup);
    heap_freetuple(policy_htup);
    heap_close(policy_relation, RowExclusiveLock);

    /* Start to Process PRIVILEGES(DML) && ACCESS(DDL) exprs */
    int opt_type = get_option_type(policy_type);

    /* Extract policy targets from the statement node tree */
    foreach (policy_target_item, stmt->policy_targets) {
        DefElem *defel = (DefElem *) lfirst(policy_target_item);
        const char *action_type = defel->defname;    /* action: DELETE, INSERT, UPDATE, etc. */
        DefElem *policy_items = (DefElem *) defel->arg;
        switch (opt_type) {
            case POLICY_OPT_PRIVILEGES: {
                Relation priv_relation = heap_open(GsAuditingPolicyPrivilegesRelationId, RowExclusiveLock);
                if (priv_relation) {
                    add_labels_to_policy(false, action_type, policy_items, priv_relation, policyOid);
                    heap_close(priv_relation, RowExclusiveLock);
                }
            }
            break;
            case POLICY_OPT_ACCESS: {
                Relation acc_relation = heap_open(GsAuditingPolicyAccessRelationId, RowExclusiveLock);
                if (acc_relation) {
                    add_labels_to_policy(true, action_type, policy_items, acc_relation, policyOid);
                    heap_close(acc_relation, RowExclusiveLock);
                }
            }
            break;
            /* handled later */
            case POLICY_OPT_FILTER:
            break;
            default: {
                /* report about unknown policy type */
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Unsupported policy type")));
            }
            break;
        }
    }

    handle_alter_add_update_filter(stmt->policy_filters, policyOid, true /* add new filter row */);
    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    if (load_policy_privileges_hook) {
        load_policy_privileges_hook(false);
    }
    if (load_policy_access_hook) {
        load_policy_access_hook(false);
    }
    if (load_audit_policies_hook) {
        load_audit_policies_hook(false);
    }
    /* load filters must be last */
    if (load_policy_filter_hook) {
        load_policy_filter_hook(false);
    }
}

static void handle_alter_audit_node(AlterAuditPolicyStmt *stmt, gs_stl::gs_string& err_msg,
    GsPolicyStruct& cur_policy, privileges_access_set& access_to_add, privileges_access_set& access_to_remove,
    privileges_access_set& privs_to_add, privileges_access_set& privs_to_remove, policy_labels_map& existing_labels,
    privileges_access_set& existing_privileges, privileges_access_set& existing_access)
{
    /* Extract policy items from the statement node tree */
    if (stmt->policy_type != NULL) {
        ListCell   *policy_item = NULL;
        bool is_add = strcasecmp(stmt->policy_action, "add") == 0; /* action: add or remove */
        int opt_type = get_option_type(stmt->policy_type);
        foreach (policy_item, stmt->policy_items) {
            DefElem *pol_option_item = (DefElem *) lfirst(policy_item); /* policy option & list of targets */
            if (pol_option_item == NULL) { /* for optional parts of statement */
                continue;
            }
            /* pol_option_item->defname; copy, ... */
            bool ret = true;
            DefElem* arguments = (DefElem*)pol_option_item->arg;
            List *targets = arguments ? (List *) arguments->arg : nullptr; /* policy targets */
            ListCell *target = NULL;
            if (targets && list_length(targets) > 0) {
                /* arguments->defname; LABEL */
                foreach (target, targets) {
                    if (!(ret = handle_target(target, opt_type, is_add, &err_msg, &access_to_add, &access_to_remove,
                        &privs_to_add, &privs_to_remove, &existing_labels, &cur_policy, pol_option_item->defname))) {
                        break;
                    }
                }
            } else { /* all objects */
                switch (opt_type) {
                    case POLICY_OPT_ACCESS:
                        handle_add_remove_all_types(opt_type, &access_to_add, &access_to_remove, &existing_access, is_add,
                            cur_policy.m_id, pol_option_item->defname);
                        break;
                    case POLICY_OPT_PRIVILEGES:
                        handle_add_remove_all_types(opt_type, &privs_to_add, &privs_to_remove, &existing_privileges, is_add,
                            cur_policy.m_id, pol_option_item->defname);
                        break;
                    default:
                        break;
                }
            }

            /* validations, If anything is added/removed to label, label must exist unless it's for ALL */
            if (!ret) {
                /* generate an error */
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s", err_msg.c_str())));
                return;
            }
        }
    }
}

static void update_audit_policy_actions(privileges_access_set& privs_to_add, privileges_access_set& privs_to_remove,
    privileges_access_set& access_to_add, privileges_access_set& access_to_remove, GsPolicyStruct& cur_policy,
    privileges_access_set& existing_privileges, privileges_access_set& existing_access,
    gs_stl::gs_string& err_msg)
{
    if ((privs_to_add.size() > 0) || (privs_to_remove.size() > 0)) {
        Relation priv_relation = heap_open(GsAuditingPolicyPrivilegesRelationId, RowExclusiveLock);
        if (priv_relation != NULL) {
            /* add privileges */
            if (privs_to_add.size() > 0) {
                add_labels_to_privileges_access(false, &privs_to_add, &cur_policy, priv_relation);
            } else if (!remove_labels_from_privileges_access(false, &privs_to_remove, &existing_privileges, 
                                                             priv_relation,
                                                             &err_msg)) { /* remove privileges */
                heap_close(priv_relation, RowExclusiveLock);
                /* generate an error */
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("%s", (err_msg.size() > 0) ? err_msg.c_str() : "No matching privilege to delete found")));
                return;
            }
            heap_close(priv_relation, RowExclusiveLock);
        }
    }
    if ((access_to_add.size() > 0) || (access_to_remove.size() > 0)) {
        Relation acc_relation = heap_open(GsAuditingPolicyAccessRelationId, RowExclusiveLock);
        if (acc_relation != NULL) {
            /* add access */
            if (access_to_add.size() > 0) {
                add_labels_to_privileges_access(true, &access_to_add, &cur_policy, acc_relation);
            } else if (!remove_labels_from_privileges_access(true, &access_to_remove, &existing_access, acc_relation,
                &err_msg)) { /* remove access */
                heap_close(acc_relation, RowExclusiveLock);
                /* generate an error */
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("%s", (err_msg.size() > 0) ? err_msg.c_str() : "No matching access to delete found")));
                return;
            }
            heap_close(acc_relation, RowExclusiveLock);
        }
    }
}

/*
 * function name: alter_audit_policy
 * description  : alter auditing policy
 */
void alter_audit_policy(AlterAuditPolicyStmt *stmt)
{
    /* check that if has access to config audit policy */
    if (!is_policy_enabled()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        return;
    }

    char buff[512] = {0};
    char user_name[USERNAME_LEN] = {0};
    char session_ip[MAX_IP_LEN] = {0};

    get_session_ip(session_ip, MAX_IP_LEN);
    (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
        "user name: [%s], app_name: [%s], ip: [%s], ALTER AUDIT POLICY [%s] FOR %s",
        user_name, u_sess->attr.attr_common.application_name, session_ip, stmt->policy_name, stmt->policy_action);
    securec_check_ss(rc, "", "");
    save_manage_message(buff);
    const char *policy_name = stmt->policy_name;
    policies_set existing_policies;
    policy_labels_map existing_labels;

    privileges_access_set existing_privileges;
    privileges_access_set privs_to_remove;
    privileges_access_set privs_to_add;

    privileges_access_set existing_access;
    privileges_access_set access_to_remove;
    privileges_access_set access_to_add;

    Relation    policy_relation = NULL;
    Relation    labels_relation = NULL;

    /* Open the relation for read and insertion */
    policy_relation = heap_open(GsAuditingPolicyRelationId, RowExclusiveLock);
    load_existing_policies<Form_gs_auditing_policy>(policy_relation, &existing_policies);

    /* first check whether such policy exists */
    GsPolicyStruct cur_policy;
    cur_policy.m_name = policy_name;
    policies_set::iterator it = existing_policies.find(cur_policy);
    if (it == existing_policies.end()) {
        heap_close(policy_relation, RowExclusiveLock);
        if (stmt->missing_ok) { /* IF EXISTS is specified, generate a notice */
            send_manage_message(AUDIT_OK);
            ereport(NOTICE, (errmsg("%s policy not found, alter skipping", policy_name)));
        } else {
            /* generate an error */
            send_manage_message(AUDIT_FAILED);
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s no such policy found, alter failed", policy_name)));
        }
        return;
    }
    cur_policy.m_id = it->m_id;
    cur_policy.m_enabled = it->m_enabled;

    /* Update policy if needed */
    bool policy_status_changed = false;
    if (stmt->policy_enabled != NULL) {
        DefElem *defel = (DefElem *) stmt->policy_enabled;
        if (strcasecmp(defel->defname, "status") == 0) {
            bool policy_new_status = (strcasecmp(strVal(defel->arg), "enable") == 0) ? true : false;
            if (it->m_enabled != policy_new_status) {
                policy_status_changed = true;
                cur_policy.m_enabled = policy_new_status;
            }
        }
    }
    if (((stmt->policy_comments != NULL) && (strlen(stmt->policy_comments) > 0)) || policy_status_changed) {
        cur_policy.m_comments = stmt->policy_comments;
        gs_stl::gs_string err_msg;
        if (!update_policy(&cur_policy, policy_relation, policy_status_changed, &err_msg)) {
            heap_close(policy_relation, RowExclusiveLock);
            /* generate an error */
            send_manage_message(AUDIT_FAILED);
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s", err_msg.c_str())));
            return;        
        }
    }
    heap_close(policy_relation, RowExclusiveLock);

    labels_relation = heap_open(GsPolicyLabelRelationId, RowExclusiveLock); 
    load_existing_labels(labels_relation, &existing_labels);
    heap_close(labels_relation, RowExclusiveLock);

    /* Get current timestamp */
    (void)DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    load_existing_privileges(&existing_privileges, cur_policy.m_id);
    load_existing_access(&existing_access, cur_policy.m_id);

    /* Extract policy items from the statement node tree */
    gs_stl::gs_string err_msg;

    handle_alter_audit_node(stmt, err_msg, cur_policy, access_to_add, access_to_remove, privs_to_add, privs_to_remove,
                            existing_labels, existing_privileges, existing_access);
    update_audit_policy_actions(privs_to_add, privs_to_remove, access_to_add, access_to_remove, cur_policy,
                                existing_privileges, existing_access, err_msg);
    handle_alter_add_update_filter(stmt->policy_filters, cur_policy.m_id, false /* update filter */);

    if (stmt->policy_action && !strcmp(stmt->policy_action, "drop_filter")) {
        drop_policy_reference<Form_gs_auditing_policy_filters>(GsAuditingPolicyFiltersRelationId, cur_policy.m_id);
    }

    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    if (load_policy_access_hook) {
        load_policy_access_hook(false);
    }
    if (load_policy_privileges_hook) {
        load_policy_privileges_hook(false);
    }
    if (load_audit_policies_hook) {
        load_audit_policies_hook(false);
    }
    /* load filters must be last */
    if (load_policy_filter_hook) {
        load_policy_filter_hook(false);
    }
}

/**
 * Main enterance for droping audit policy, which will drop all the catalog information associated with this Policy.
 * @stmt : Data structure for Drop Policy syntax
 */
void drop_audit_policy(DropAuditPolicyStmt *stmt)
{
    /* check that if has access to config audit policy */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    /* save Mng logs */
    ListCell* policy_obj = NULL;
    foreach (policy_obj, stmt->policy_names) {
        const char* polname = (const char *)(((Value*)lfirst(policy_obj))->val.str);
        char buff[512] = {0};
        char user_name[USERNAME_LEN] = {0};
        char session_ip[MAX_IP_LEN] = {0};

        get_session_ip(session_ip, MAX_IP_LEN);
        (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

        int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
            "user name: [%s], app_name: [%s], ip: [%s], DROP AUDIT POLICY [%s]",
            user_name, u_sess->attr.attr_common.application_name, session_ip, polname);
        securec_check_ss(rc, "", "");
        save_manage_message(buff);
    }

    foreach (policy_obj, stmt->policy_names) {
        const char* polname = (const char *)(((Value*)lfirst(policy_obj))->val.str);
        gs_stl::gs_set<long long> ids;
        drop_policy_by_name<Form_gs_auditing_policy>(GsAuditingPolicyRelationId, polname, &ids);
        if (ids.empty()) {
            if (stmt->missing_ok) {
                ereport(NOTICE, (errmsg("%s policy does not exist, drop skipping", polname)));
                continue;
            } else {
                send_manage_message(AUDIT_FAILED);
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("%s policy does not exist, drop failed", polname)));
                return;
            }
        }
        for (long long _id : ids) {
            /* drop gs_auditing_policy_access catalog information */
            drop_policy_reference<Form_gs_auditing_policy_access>(GsAuditingPolicyAccessRelationId, _id);
            /* drop gs_auditing_policy_privilege catalog information */
            drop_policy_reference<Form_gs_auditing_policy_privileges>(GsAuditingPolicyPrivilegesRelationId, _id);
            /* drop gs_auditing_policy_filter catalog information */
            drop_policy_reference<Form_gs_auditing_policy_filters>(GsAuditingPolicyFiltersRelationId, _id);
        }
    }

    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    if (load_policy_access_hook) {
        load_policy_access_hook(false);
    }
    if (load_policy_privileges_hook) {
        load_policy_privileges_hook(false);
    }
    if (load_audit_policies_hook) {
        load_audit_policies_hook(false);
    }
    /* load filters must be last */
    if (load_policy_filter_hook) {
        load_policy_filter_hook(false);
    }
}

