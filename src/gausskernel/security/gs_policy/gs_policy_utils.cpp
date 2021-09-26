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
 * gs_policy_utils.cpp
 *     Utility functions for dealing with auditing policy, like filters, label object.
 * 
 * 
 * IDENTIFICATION
 *        src/gausskernel/securityg/gs_policy/gs_policy_util.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include "gs_policy/gs_policy_utils.h"
#include "gs_policy/policy_common.h"
#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/acl.h"
#include "storage/lock/lock.h"
#include "access/xact.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "gs_policy/gs_vector.h"
#include "iprange/iprange.h"
#include "pgaudit.h"
#include "commands/dbcommands.h"

#ifdef ENABLE_UT
#define static
#endif

GsSaveManagementEvent gs_save_mng_event_hook = NULL;
GsSendManagementEvent gs_send_mng_event_hook = NULL;

void save_manage_message(const char* message)
{
    if (gs_save_mng_event_hook != NULL) {
        gs_save_mng_event_hook(message);
    }
}

void send_manage_message(AuditResult result_type)
{
    if (gs_send_mng_event_hook != NULL) {
        gs_send_mng_event_hook(result_type);
    }
}

bool GsPolicyStruct::operator == (const GsPolicyStruct &arg) const
{
    return strcasecmp(m_name.c_str(), arg.m_name.c_str()) == 0;
}

bool GsPolicyStruct::operator < (const GsPolicyStruct &arg) const
{
    /* compare by name */
    return strcasecmp(m_name.c_str(), arg.m_name.c_str()) < 0;
}

int GsPolicyStruct::operator - (const GsPolicyStruct &arg) const
{
    if (*this < arg) {
        return -1;
    } else if (arg < *this) {
        return 1;
    } else {
        return 0;
    }
}

bool PgPolicyFiltersStruct::operator == (const PgPolicyFiltersStruct &arg) const
{
    if (*this < arg) {
        return false;
    } else if (arg < *this) {
        return false;
    } else {
        return true;
    }
}

bool PgPolicyFiltersStruct::operator < (const PgPolicyFiltersStruct &arg) const
{
    int res = strcasecmp(m_type.c_str(), arg.m_type.c_str());
    if (res < 0) {
        return true;
    }
    if (res > 0) {
        return false;
    }
    res = strcasecmp(m_label_name.c_str(), arg.m_label_name.c_str());
    if (res < 0) {
        return true;
    }
    if (res > 0) {
        return false;
    }
    return m_policy_oid < arg.m_policy_oid;
}

int  PgPolicyFiltersStruct::operator - (const PgPolicyFiltersStruct &arg) const
{
    if (*this < arg) {
        return -1;
    } else if (arg < *this) {
        return 1;
    } else {
        return 0;
    }
}

bool PgPolicyPrivilegesAccessStruct::operator == (const PgPolicyPrivilegesAccessStruct &arg) const
{
    return (strcasecmp(m_type.c_str(), arg.m_type.c_str()) == 0) &&
           (strcasecmp(m_label_name.c_str(), arg.m_label_name.c_str()) == 0);
}

bool PgPolicyPrivilegesAccessStruct::operator < (const PgPolicyPrivilegesAccessStruct &arg) const
{
    int res = strcasecmp(m_type.c_str(), arg.m_type.c_str());
    if (res < 0) {
        return true;
    }
    if (res > 0) {
        return false;
    }
    return strcasecmp(m_label_name.c_str(), arg.m_label_name.c_str()) < 0;
}

int PgPolicyPrivilegesAccessStruct::operator - (const PgPolicyPrivilegesAccessStruct &arg) const
{
    if (*this < arg) {
        return -1;
    } else if (arg < *this) {
        return 1;
    } else {
        return 0;
    }
}

/* Process new filters from parser tree and tranform them into string */
bool process_new_filters(const List *policy_filters, gs_stl::gs_string *flat_tree)
{
    if (!policy_filters)
        return true;
    flat_tree->clear();
    ListCell   *policy_filter_item = NULL;
    gs_stl::gs_vector<PolicyFilterNode *> nodes;

    foreach(policy_filter_item, policy_filters) {
        PolicyFilterNode *root = (PolicyFilterNode *) lfirst(policy_filter_item);
        nodes.push_back(root);
        while (nodes.size() > 0) {
            PolicyFilterNode* n = nodes.back();
            nodes.pop_back();
            /* operator type node */
            if (!strcmp(n->node_type, "op")) {
                if (!strcmp(n->op_value, "and")) {
                    (void)flat_tree->append("*");
                } else if (!strcmp(n->op_value, "or")) {
                    (void)flat_tree->append("+");
                } else { /* unsupported operator */
                    return false;
                }
                nodes.push_back((PolicyFilterNode *)n->right);
                nodes.push_back((PolicyFilterNode *)n->left);
            } else if (!strcmp(n->node_type, "filter")) { /* value type node */
                if (n->has_not_operator == true) {
                    (void)flat_tree->append("!");
                }
                (void)flat_tree->append(n->filter_type);
                (void)flat_tree->append("[");
                List *filter_item_objects = (List *) n->values;
                ListCell   *filter_obj = NULL;
                foreach(filter_obj, filter_item_objects) {
                    const char *filter_value = (const char *)(((Value*)lfirst(filter_obj))->val.str);
                    if (!verify_ip_role_app(n->filter_type, filter_value, flat_tree)) {
                        return false;
                    }
                    (void)flat_tree->append(",");
                }
                if (flat_tree->back() == ',') {
                    flat_tree->pop_back();
                }
                (void)flat_tree->append("]");
            }
        }
    }
    return true;
}

bool scan_to_delete_from_relation(long long row_id, Relation relation, unsigned int index_id)
{
    if (relation == NULL) {
        return false;
    }
    ScanKeyData skey;
    /* Find the row to delete. */
    ScanKeyInit(&skey,
                ObjectIdAttributeNumber,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(row_id));

    SysScanDesc tgscan = systable_beginscan(relation, index_id, true, NULL, 1, &skey);

    HeapTuple tup = systable_getnext(tgscan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(tgscan);
        return false;
    }
    /* Delete the label tuple */
    simple_heap_delete(relation, &tup->t_self);

    systable_endscan(tgscan);

    return true;
}

void construct_resource_name(const RangeVar *rel, gs_stl::gs_string *target_name_s)
{
    if (rel->catalogname) {
        (void)target_name_s->append((const char *)rel->catalogname);
        target_name_s->push_back('.');
    }
    if (rel->schemaname) {
        (void)target_name_s->append((const char *)rel->schemaname);
        target_name_s->push_back('.');
    }
    if (rel->relname) {
        (void)target_name_s->append(rel->relname);
    }
}

/**
 * Check if current app is valid or not.
 */
bool verify_app_filter(const char* obj_value)
{
    if (strlen(obj_value) == 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("app: [%s] is invalid", obj_value)));
        return false;
    }

    /* The first character id numbers or dollar */
    char c = obj_value[0];
    if ((c >= '0' && c <= '9') || c == '$') {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("app: [%s] is invalid", obj_value)));
        return false;
    }

    int len = strlen(obj_value);
    for (int i = 0; i < len; i++) {
        c = obj_value[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$') {
            continue;
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("app: [%s] is invalid", obj_value)));
            return false;
        }
    }

    return true;
}

/**
 * Check the effective of the filter information.
 * @ obj_type : filter type, which inlcudes IP, ROLES, and APPS.
 * @ obj_value : the actual filter information.
 * @ return_value : record about the filter information.
 */
bool verify_ip_role_app(const char* obj_type, const char* obj_value, gs_stl::gs_string *return_value)
{
    if (!strcasecmp(obj_type, "ip")) {
        const char* check_value = obj_value;
        if (!IPRange::is_range_valid(check_value)) {
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("ip range: [%s] is invalid, please identify", obj_value)));
            return false;
        }
        (void)return_value->append(check_value);
        return true;
    } else if (!strcasecmp(obj_type, "roles")) {
        Oid uid = get_role_oid(obj_value, true);
        if (!OidIsValid(uid)) {
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("role: [%s] is invalid", obj_value)));
            return false;
        }
        char buffer[64]; /* buffer to store the oid int as string. 64 is the max length of oid. */
        int nRet = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d", uid);
        securec_check_ss(nRet, "\0", "\0");
        (void)return_value->append(buffer);
        return true;
    } else if (!strcasecmp(obj_type, "app")) {
        bool is_valid_app = verify_app_filter(obj_value);
        if (!is_valid_app) {
            return false;
        }
    }
    (void)return_value->append(obj_value);
    return true;
}

static bool add_privileges_access(const char *action_type, const char *label_name,
    privileges_access_set *actions, const policy_labels_map *existing_labels, const GsPolicyStruct *policy,
    gs_stl::gs_string *err_msg)
{
    PgPolicyPrivilegesAccessStruct item;
    item.m_type = action_type;
    item.m_label_name = label_name;
    item.m_policy_oid = policy->m_id;
    /* validate that such label exists */
    if (existing_labels->find(label_name) == existing_labels->end()) {
        err_msg->clear();
        (void)err_msg->append("Trying to add/remove privilege/access [");
        (void)err_msg->append(action_type);
        (void)err_msg->append("] to non-existing label [");
        (void)err_msg->append(label_name);
        (void)err_msg->append("]");
        return false;
    }
    (void)actions->insert(item);
    return true;
}

bool handle_target(ListCell *target,
                   int opt_type,
                   bool is_add,
                   gs_stl::gs_string *err_msg,
                   privileges_access_set *access_to_add, privileges_access_set *access_to_remove,
                   privileges_access_set *privs_to_add, privileges_access_set *privs_to_remove,
                   const policy_labels_map *existing_labels,
                   const GsPolicyStruct *policy, const char *acc_action_type)
{
    bool ret = false;
    switch (opt_type) {
        case POLICY_OPT_PRIVILEGES: {
            RangeVar *rel = (RangeVar*)lfirst(target);
            gs_stl::gs_string target_name_s;
            construct_resource_name((const RangeVar*)rel, &target_name_s);
            if (is_add) {
                ret = add_privileges_access(acc_action_type, target_name_s.c_str(), privs_to_add, 
                                            existing_labels, policy, err_msg);
            } else {
                ret = add_privileges_access(acc_action_type, target_name_s.c_str(), privs_to_remove, existing_labels, 
                                            policy, err_msg);
            }
        }
            break;
        case POLICY_OPT_ACCESS: {
            RangeVar *rel = (RangeVar*)lfirst(target);
            gs_stl::gs_string target_name_s;
            construct_resource_name((const RangeVar*)rel, &target_name_s);
            if (is_add) {
                ret = add_privileges_access(acc_action_type, target_name_s.c_str(), access_to_add, existing_labels,
                                            policy, err_msg);
            } else {
                ret = add_privileges_access(acc_action_type, target_name_s.c_str(), access_to_remove, existing_labels,
                                            policy, err_msg);
            }
        }
            break;
          // : this is not handled here...
        default:
            break;
    }
    return ret;
}

static bool parse_values(const gs_stl::gs_string logical_expr_str, int *offset, const char* obj_type)
{
    std::size_t found = gs_stl::gs_string::npos;
    char buff[512] = {0};
    size_t limit_pos = logical_expr_str.find(']', *offset);
    gs_stl::gs_string tmp_res;
    bool parsed = true;
    bool filter_valid = false;
    int nRet;
    /* not finding last ']' means error */
    if (limit_pos == gs_stl::gs_string::npos) {
        ereport(ERROR, 
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), 
                errmsg("filter: [%s] is invalid", logical_expr_str.c_str())));
        return false;
    }
    while ((found = logical_expr_str.find(',', *offset)) != gs_stl::gs_string::npos && found < limit_pos) {
        nRet = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%.*s", (int)(found - *offset),
            logical_expr_str.c_str() + *offset);
        securec_check_ss(nRet, "\0", "\0");
        filter_valid = verify_ip_role_app(obj_type, buff, &tmp_res);
        parsed = parsed && filter_valid;
        *offset = found + 1;
    }

    if (*offset < (int)limit_pos) {
        nRet = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%.*s", (int)(limit_pos - *offset),
            logical_expr_str.c_str() + *offset);
        securec_check_ss(nRet, "\0", "\0");
        filter_valid = verify_ip_role_app(obj_type, buff, &tmp_res);
        parsed = parsed && filter_valid;
        *offset = limit_pos + 1;
        return parsed;
    }
    /* getting here means error */
    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("filter: [%s] is invalid", logical_expr_str.c_str())));
    return false;
}

/* Parses & validates (recursively) polish-notation format string into logical tree */
bool validate_logical_expression(const gs_stl::gs_string logical_expr_str, int *offset)
{
    int logical_expr_len = logical_expr_str.size();

    while (*offset < logical_expr_len) {
        /* AND/OR node */
        if ((logical_expr_str[*offset] == '*') || (logical_expr_str[*offset] == '+')) {
            (*offset)++;
            return  (validate_logical_expression(logical_expr_str, offset) /* go left */
                    && validate_logical_expression(logical_expr_str, offset)); /* go right */
        } else if (logical_expr_str[*offset] == '!') { /* NOT operator */
            (*offset)++;
        } else if (logical_expr_str[*offset] == 'i') { /* IP filter node */
            *offset += 3; /* 3 : skip 'ip[' */
            return parse_values(logical_expr_str, offset, "ip");
        } else if (logical_expr_str[*offset] == 'r') { /* ROLE filter node */
            *offset += 6; /* 6 : skip 'roles[' */
            return parse_values(logical_expr_str, offset, "roles");
        } else if (logical_expr_str[*offset] == 'a') { /* APPLICATION filter node */
            *offset += 4; /* 4 : skip 'app[' */
            return parse_values(logical_expr_str, offset, "app");
        }
    }

    return false;
}

void get_session_ip(char *session_ip, int len)
{
    if (len < MAX_IP_LEN) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("length of session ip buffer should more than 128")));
    }
    struct sockaddr* remote_addr = (struct sockaddr *)&u_sess->proc_cxt.MyProcPort->raddr.addr;
    errno_t rc = EOK;
    /* parse the remote ip address */
    if (AF_UNIX == remote_addr->sa_family) {
        char *localstr = "local";
        rc = memcpy_s(session_ip, len, localstr, strlen(localstr));
        securec_check(rc, "\0", "\0");
    } else {
        get_client_ip(remote_addr, session_ip);
    }
}

void get_client_ip(const struct sockaddr* remote_addr, char *ip_str)
{
    /* parse the remote ip address */
    if (AF_INET6 == remote_addr->sa_family) {
        (void)inet_ntop(AF_INET6, &((struct sockaddr_in6*)remote_addr)->sin6_addr, ip_str, MAX_IP_LEN - 1);
    } else if (AF_INET == remote_addr->sa_family) {
        (void)inet_ntop(AF_INET, &((struct sockaddr_in*)remote_addr)->sin_addr, ip_str, MAX_IP_LEN - 1);
    }
}

bool is_database_valid(const char* dbname)
{
    if (dbname == NULL) {
        return false;
    }
    Oid db_oid = get_database_oid(dbname, false);
    if (OidIsValid(db_oid)) {
        return true;
    }

    return false;
}

ResourceOwnerData* create_temp_resourceowner() 
{
    ResourceOwner tmpOwner =  ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner,
        "CheckUserOid", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY));
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;
    return currentOwner;
}

void release_temp_resourceowner(ResourceOwnerData* resource_owner)
{
    ResourceOwner tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    t_thrd.utils_cxt.CurrentResourceOwner = resource_owner;
    ResourceOwnerDelete(tmpOwner);
}
