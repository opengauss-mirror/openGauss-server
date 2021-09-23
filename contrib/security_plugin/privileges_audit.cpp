/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *            http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * privileges_audit.cpp
 *      grammer for create/update/delete auding policy informations into catalog
 * 
 * IDENTIFICATION
 *      src/contrib/secuirty_plugin/privileges_audit.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "gs_audit_policy.h"
#include <memory>
#include "access_audit.h"
#include "postgres.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "gs_policy/gs_policy_utils.h"

#include "nodes/params.h"
#include "nodes/nodes.h"
#include "commands/user.h"
#include "workload/gscgroup.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "utils/lsyscache.h"
#include "privileges_audit.h"
#include "pgaudit.h"
#include "gs_threadlocal.h"
#include "gs_policy_plugin.h"
#include "gs_policy_labels.h"
#include "gs_mask_policy.h"
#include "iprange/iprange.h"

#define ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(type) \
    ((check_acl_privilige_hook == NULL) ? true : check_acl_privilige_hook(type))

static Security_LoginHandle_access_hook_type security_LoginHandle_access_hook = NULL;

void add_current_path(int objtype, List *fqdn, gs_stl::gs_string *buffer);

/* function overloading, notice - different implementation */
void internal_audit_object_str(const policy_set *security_policy_ids, const policy_set *policy_ids,
    const names_pair names, int priv_type, const char *priv_name, int objtype, bool ignore_db)
{
    policy_simple_set policy_result;
    PolicyLabelItem item(0, 0, objtype);
    bool security_auditobject_res = (accesscontrol_securityAuditObject_hook != NULL) ?
        accesscontrol_securityAuditObject_hook(security_policy_ids, &item, priv_type, priv_name) :
        true;
    if (security_auditobject_res && check_audit_policy_privileges(policy_ids, &policy_result, priv_type,&item)) {
        char buff[2048] = {0};
        char user_name[USERNAME_LEN];
        const char *direction = (priv_type == T_GRANT) ? "TO" : "FROM";
        const char *dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        policy_simple_set::iterator it = policy_result.begin();
        policy_simple_set::iterator eit = policy_result.end();
        int rc;
        for (; it != eit; ++it) {
            char session_ip[MAX_IP_LEN] = {0};
            get_session_ip(session_ip, MAX_IP_LEN);
            if (ignore_db) {
                rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                    "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s ON %s %s %s "
                    "%s], "
                    "policy id: [%lld]",
                    GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_name,
                    get_privilege_object_name(item.m_obj_type), names.first.c_str(), direction, names.second.c_str(),
                    *it);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                    "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s ON %s %s.%s %s "
                    "%s], "
                    "policy id: [%lld]",
                    GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_name,
                    get_privilege_object_name(item.m_obj_type), dbname, names.first.c_str(), direction,
                    names.second.c_str(), *it);
                securec_check_ss(rc, "\0", "\0");
            }

            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
}

void login_object_audit(const policy_set security_policy_ids, const policy_set policy_ids,
                        const char *login_str, int priv_type, const char *priv_name, const char *dbname)
{
    policy_simple_set policy_result;
    PolicyLabelItem item(0, 0, T_LOGIN);
    if (check_audit_policy_privileges(&policy_ids, &policy_result, priv_type, &item, dbname)) {
        char buff[2048] = {0};
        policy_simple_set::iterator it = policy_result.begin();
        policy_simple_set::iterator eit = policy_result.end();
        for (; it != eit; ++it) {
            char session_ip[MAX_IP_LEN] = {0};
            get_session_ip(session_ip, MAX_IP_LEN);

            int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                "AUDIT EVENT: app_name: [%s], client_ip: [%s], privilege type: [%s], policy id: [%lld]",
                get_session_app_name(), session_ip, login_str, *it);
            securec_check_ss(rc, "\0", "\0");
            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
    return;
}

void login_handle_audit(const char *dbname, const char *username, bool success, bool login)
{
    IPV6 ip;
    get_remote_addr(&ip);
    FilterData filter_item(u_sess->attr.attr_common.application_name, ip);
    policy_set audit_policy_ids, security_policy_ids;
    check_audit_policy_filter(&filter_item, &audit_policy_ids, dbname);

    char tmp[512] = {0};
    int rc;
    if (login && success) {
        rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "LOGIN SUCCESS: [%s] to DATABASE: [%s]", username, dbname);
        securec_check_ss(rc, "\0", "\0");
        login_object_audit(security_policy_ids, audit_policy_ids, tmp, T_LOGIN_SUCCESS, "LOGIN SUCCESS", dbname);
    } else if (login && !success) {
        rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "LOGIN FAILED: [%s] to DATABASE: [%s]", username, dbname);
        securec_check_ss(rc, "\0", "\0");
        login_object_audit(security_policy_ids, audit_policy_ids, tmp, T_LOGIN_FAILURE, "LOGIN FAILED", dbname);
    } else if (!login && success) {
        rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "LOGOUT: [%s] to DATABASE: [%s]", username, dbname);
        securec_check_ss(rc, "\0", "\0");
        login_object_audit(security_policy_ids, audit_policy_ids, tmp, T_LOGOUT, "LOGOUT", dbname);
    } else { /* !login && !success */
        rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "LOGOUT: [%s] to DATABASE: [%s]", username, dbname);
        securec_check_ss(rc, "\0", "\0");
        login_object_audit(security_policy_ids, audit_policy_ids, tmp, T_LOGOUT, "LOGOUT", dbname);
    }
}

static void gen_priv_audit_logs(policy_simple_set& policy_result, bool ignore_db, const char* priv_name, const PolicyLabelItem* item,
    const char *obj_value)
{
    char buff[2048] = {0};
    char user_name[USERNAME_LEN];
    int rc;
    const char* dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    policy_simple_set::iterator it = policy_result.begin();
    policy_simple_set::iterator eit = policy_result.end();
    for (; it != eit; ++it) {
        char session_ip[MAX_IP_LEN] = {0};
        get_session_ip(session_ip, MAX_IP_LEN);

        if (ignore_db) {
            rc = snprintf_s(buff,
                sizeof(buff),
                sizeof(buff) - 1,
                "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s %s %s], policy "
                "id: [%lld]",
                GetUserName(user_name, sizeof(user_name)),
                get_session_app_name(),
                session_ip,
                priv_name,
                get_privilege_object_name(item->m_obj_type),
                obj_value,
                *it);
            securec_check_ss(rc, "\0", "\0");
        } else {
            rc = snprintf_s(buff,
                sizeof(buff),
                sizeof(buff) - 1,
                "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s %s %s.%s], "
                "policy id: [%lld]",
                GetUserName(user_name, sizeof(user_name)),
                get_session_app_name(),
                session_ip,
                priv_name,
                get_privilege_object_name(item->m_obj_type),
                dbname,
                obj_value,
                *it);
            securec_check_ss(rc, "\0", "\0");
        }

        save_access_logs(AUDIT_POLICY_EVENT, buff);
    }
}

void internal_audit_str(const policy_set *security_policy_ids, const policy_set *policy_ids,
                        const char *value, int priv_type, const char *priv_name, int objtype, bool ignore_db)
{
    policy_simple_set policy_result;
    int policy_object = (objtype == O_CURSOR) ? O_TABLE : objtype;
    PolicyLabelItem item(0, 0, policy_object);

    bool security_auditobject_res = (accesscontrol_securityAuditObject_hook != NULL) ?
        accesscontrol_securityAuditObject_hook(security_policy_ids, &item, priv_type, priv_name) : 
        true;
    if (security_auditobject_res && check_audit_policy_privileges(policy_ids, &policy_result, priv_type, &item)) {
        gen_priv_audit_logs(policy_result, ignore_db, priv_name, &item, value);
    }
}

bool internal_audit_object_str(const policy_set* security_policy_ids, const policy_set* policy_ids,
    const PolicyLabelItem* item, int priv_type, const char* priv_name, const char* objname, bool ignore_db)
{
    bool is_found = false;
    policy_simple_set policy_result;

    bool security_auditobject_res = (accesscontrol_securityAuditObject_hook != NULL) ?
        accesscontrol_securityAuditObject_hook(security_policy_ids, item, priv_type, priv_name) :
        true;
    if (security_auditobject_res && check_audit_policy_privileges(policy_ids, &policy_result, priv_type, item)) {
        gs_stl::gs_string obj_value;
        switch (item->m_obj_type) {
            case O_DATABASE:
            case O_ROLE:
                obj_value = objname;
                break;
            case O_SCHEMA:
                item->get_fqdn_value(&obj_value);
                break;
            default:
                item->get_fqdn_value(&obj_value);
                if (!item->m_object && strlen(objname) > 0) {
                    obj_value.push_back('.');
                    obj_value.append(objname);
                }
                break;
        }
        is_found = !policy_result.empty();
        gen_priv_audit_logs(policy_result, ignore_db, priv_name, item, obj_value.c_str());
    }
    return is_found;
}

void audit_object(const policy_set *security_policy_ids, const policy_set *policy_ids, const char *relname,
    int priv_type, const char *priv_name, int objtype)
{
    switch (objtype) {
        case OBJECT_ROLE:
            internal_audit_str(security_policy_ids, policy_ids, relname, priv_type, priv_name, O_ROLE);
            break;
        case OBJECT_USER:
            internal_audit_str(security_policy_ids, policy_ids, relname, priv_type, priv_name, O_USER);
            break;
        case OBJECT_SCHEMA:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_SCHEMA);
            break;
        case OBJECT_SEQUENCE:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_SEQUENCE);
            break;
        case OBJECT_DATABASE:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_DATABASE);
            break;
        case OBJECT_FOREIGN_SERVER:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_SERVER);
            break;
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
        case OBJECT_TABLE:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name,
                (objtype == OBJECT_TABLE) ? O_TABLE : O_FOREIGNTABLE);
            break;
        case OBJECT_COLUMN:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_COLUMN);
            break;
        case OBJECT_TRIGGER:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_TRIGGER);
            break;
        case OBJECT_FUNCTION:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_FUNCTION);
            break;
        case OBJECT_CONTQUERY:
        case OBJECT_VIEW:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_VIEW);
            break;
        case OBJECT_INDEX:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_INDEX);
            break;
        case OBJECT_TABLESPACE:
            internal_audit_str(policy_ids, policy_ids, relname, priv_type, priv_name, O_TABLESPACE);
            break;
        default:
            break;
    }
}

void acl_audit_object(const policy_set *security_policy_ids, const policy_set *policy_ids,
                      const names_pair names, int priv_type, const char *priv_name, int objtype)
{
    switch (objtype) {
        case ACL_OBJECT_COLUMN:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_COLUMN);
            break;
        case ACL_OBJECT_RELATION:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_RELATION);
            break;
        case ACL_OBJECT_SEQUENCE:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_SEQUENCE);
            break;
        case ACL_OBJECT_DATABASE:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_DATABASE, true);
            break;
        case ACL_OBJECT_DOMAIN:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_DOMAIN);
            break;
        case ACL_OBJECT_FOREIGN_SERVER:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_SERVER);
            break;
        case ACL_OBJECT_FUNCTION:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_FUNCTION);
            break;
        case ACL_OBJECT_LANGUAGE:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_LANGUAGE);
            break;
        case ACL_OBJECT_NAMESPACE:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_SCHEMA);
            break;
        case ACL_OBJECT_TABLESPACE:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_TABLESPACE);
            break;
        case ACL_OBJECT_DATA_SOURCE:
            internal_audit_object_str(security_policy_ids, policy_ids, names, priv_type, priv_name, O_DATA_SOURCE);
            break;
        default:
            break;
    }
}

void alter_table(const policy_set *security_policy_ids, const policy_set *policy_ids, RangeVar *rel,
                 int priv_type, const char *priv_name, int objtype)
{
    audit_table(security_policy_ids, policy_ids, rel, priv_type, priv_name, objtype);
}

void audit_table(const policy_set *security_policy_ids, const policy_set *policy_ids, RangeVar *rel, int priv_type,
    const char *priv_name, int objtype)
{
    if (rel->relname == NULL)
        return;
    PolicyLabelItem item(rel->schemaname, rel->relname, "", objtype);
    char buff[2048] = {0};
    if (priv_type == T_DROP) {
        if (check_label_has_object(&item, is_masking_has_object)) {
            gs_stl::gs_string table_name;
            get_name_range_var(rel, &table_name);
            int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                "Table: %s is part of some resource label, can not be dropped.", table_name.c_str());
            securec_check_ss(rc, "\0", "\0");
            gs_audit_issue_syslog_message("PGAUDIT", buff, AUDIT_POLICY_EVENT, AUDIT_FAILED);
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\"", buff)));
            return;
        }
    }
    policy_simple_set policy_result;
    bool security_auditobject_res = (accesscontrol_securityAuditObject_hook != NULL) ?
        accesscontrol_securityAuditObject_hook(security_policy_ids, &item, priv_type, priv_name) : true;
    if (security_auditobject_res && check_audit_policy_privileges(policy_ids, &policy_result, priv_type, &item)) {
        gs_stl::gs_string obj_value;
        item.get_fqdn_value(&obj_value);
        if (item.m_object == 0) { /* create case */
            obj_value.push_back('.');
            obj_value.append(rel->relname);
        }
        char user_name[USERNAME_LEN];
        const char *dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        policy_simple_set::iterator it = policy_result.begin();
        policy_simple_set::iterator eit = policy_result.end();
        for (; it != eit; ++it) {
            char session_ip[MAX_IP_LEN] = {0};
            get_session_ip(session_ip, MAX_IP_LEN);

            int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s %s %s.%s], policy "
                "id: [%lld]", GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_name,
                get_privilege_object_name(objtype), dbname, obj_value.c_str(), *it);
                securec_check_ss(rc, "\0", "\0");

            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
}

void audit_schema(const policy_set security_policy_ids, const policy_set policy_ids, const char *schemaname,
    const char *newname, int priv_type, const char *priv_name)
{
    char buff[2048] = {0};
    policy_simple_set policy_result;
    int check_type = (priv_type == T_RENAME) ? T_ALTER : priv_type;
    PolicyLabelItem item(schemaname, "", "", O_SCHEMA);
    bool security_auditobject_res = (accesscontrol_securityAuditObject_hook != NULL) ?
        accesscontrol_securityAuditObject_hook(&security_policy_ids, &item, check_type, priv_name) :
        true;
    if (security_auditobject_res &&
        check_audit_policy_privileges(&policy_ids, &policy_result, check_type, &item)) {
        const char *dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        char user_name[USERNAME_LEN];
        policy_simple_set::iterator it = policy_result.begin();
        policy_simple_set::iterator eit = policy_result.end();
        for (; it != eit; ++it) {
            char session_ip[MAX_IP_LEN] = {0};
            get_session_ip(session_ip, MAX_IP_LEN);

            switch (priv_type) {
                case T_RENAME: {
                    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [ALTER SCHEMA "
                        "%s.%s RENAME TO %s], policy id: [%lld]",
                        GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, dbname,
                        schemaname, newname, *it);
                        securec_check_ss(rc, "\0", "\0");
                    break;
                }
                case T_ALTER: {
                    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [ALTER SCHEMA "
                        "%s.%s OWNER TO %s], policy id: [%lld]",
                        GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, dbname,
                        schemaname, newname, *it);
                        securec_check_ss(rc, "\0", "\0");
                    break;
                }
                default: {
                    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s SCHEMA "
                        "%s.%s], policy id: [%lld]",
                        GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_name,
                        dbname, schemaname, *it);
                        securec_check_ss(rc, "\0", "\0");
                    break;
                }
            }
            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
}

void drop_command(DropStmt *stmt, const policy_set *policy_ids, const policy_set *security_policy_ids)
{
    if (stmt == NULL || stmt->objects == NIL)
        return;
    ListCell *arg = NULL;
    foreach (arg, stmt->objects) {
        List *names = (List *)lfirst(arg);
        RangeVar *rel = NULL;
        switch (stmt->removeType) {
            case OBJECT_TABLE: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP))
                    break;
                rel = makeRangeVarFromNameList(names);
                audit_table(security_policy_ids, policy_ids, rel, T_DROP, "DROP", O_TABLE);
                break;
            }
            case OBJECT_STREAM:
            case OBJECT_FOREIGN_TABLE: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP))
                    break;
                rel = makeRangeVarFromNameList(names);
                audit_table(security_policy_ids, policy_ids, rel, T_DROP, "DROP", O_TABLE);
                break;
            }
            case OBJECT_SEQUENCE: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP))
                    break;
                gs_stl::gs_string tmp;
                add_current_path(OBJECT_SEQUENCE, names, &tmp);
                internal_audit_str(security_policy_ids, policy_ids, tmp.c_str(), T_DROP, "DROP",
                    O_SEQUENCE);
                break;
            }
            case OBJECT_CONTQUERY:
            case OBJECT_VIEW: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP))
                    break;
                rel = makeRangeVarFromNameList(names);
                audit_table(security_policy_ids, policy_ids, rel, T_DROP, "DROP", O_VIEW);
                break;
            }
            case OBJECT_SCHEMA: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP))
                    break;
                const char *objectname = strVal(linitial(names));
                audit_schema(*security_policy_ids, *policy_ids, objectname, "", T_DROP, "DROP");
                break;
            }
            case OBJECT_INDEX: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP))
                    break;
                rel = makeRangeVarFromNameList(names);
                gs_stl::gs_string index_name;
                get_name_range_var(rel, &index_name);
                internal_audit_str(security_policy_ids, policy_ids, index_name.c_str(), T_DROP, "DROP",
                    O_INDEX);
                break;
            }
            case OBJECT_FUNCTION: {
                PolicyLabelItem find_obj(0, 0, O_FUNCTION);
                name_list_to_label(&find_obj, names);
                if (check_label_has_object(&find_obj, is_masking_has_object)) {
                    gs_stl::gs_string tmp;
                    find_obj.get_fqdn_value(&tmp);
                    char buff[512] = {0};
                    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "Function: %s is part of some resource label, can not be dropped.", tmp.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    gs_audit_issue_syslog_message("PGAUDIT", buff, AUDIT_POLICY_EVENT, AUDIT_FAILED);
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\"", buff)));
                    return;
                }

                if (check_audited_privilige(T_DROP) || ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP)) {
                    internal_audit_object_str(security_policy_ids, policy_ids, &find_obj, T_DROP, "DROP");
                }
                break;
            }
            case OBJECT_FOREIGN_SERVER: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP)) {
                    break;
                }
                gs_stl::gs_string name;
                name_list_to_string(names, &name);
                internal_audit_str(security_policy_ids, policy_ids, name.c_str(), T_DROP, "DROP", O_SERVER);
                break;
            }
            case OBJECT_DATA_SOURCE: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP)) {
                    break;
                }
                gs_stl::gs_string name;
                name_list_to_string(names, &name);
                internal_audit_str(security_policy_ids, policy_ids, name.c_str(), T_DROP, "DROP",
                    O_DATA_SOURCE);
                break;
            }
            case OBJECT_TRIGGER: {
                if (!check_audited_privilige(T_DROP) && !ACCESS_CONTROL_CHECK_ACL_PRIVILIGE(T_DROP)) {
                    break;
                }
                const char *objectname = strVal(lfirst(list_tail(names)));
                internal_audit_str(security_policy_ids, policy_ids, objectname, T_DROP, "DROP", O_TRIGGER);
                break;
            }
            default:
                break;
        }
    }
}

static inline void check_object_policy(const policy_set *policy_ids, int objecttype, const PolicyLabelItem *item,
    const char *priv_type)
{
    char buff[2048] = {0};
    policy_simple_set policy_result;
    if (check_audit_policy_privileges(policy_ids, &policy_result, T_ALTER, item)) {
        gs_stl::gs_string objname;
        item->get_fqdn_value(&objname);
        const char *dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        char user_name[USERNAME_LEN];
        policy_simple_set::iterator it = policy_result.begin();
        policy_simple_set::iterator eit = policy_result.end();
        for (; it != eit; ++it) {
            char session_ip[MAX_IP_LEN] = {0};
            get_session_ip(session_ip, MAX_IP_LEN);

            int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s %s %s.%s], policy "
                "id: [%lld]", GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_type,
                get_privilege_object_name(objecttype), dbname, objname.c_str(), *it);
            securec_check_ss(rc, "\0", "\0");
            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
}

static inline void check_object_policy_str(const policy_set *policy_ids, const PolicyLabelItem *item,
    const char *priv_type, const char *obj_name, bool ignore_db = false)
{
    char buff[2048] = {0};
    policy_simple_set policy_result;
    if (check_audit_policy_privileges(policy_ids, &policy_result, T_ALTER, item)) {
        gs_stl::gs_string objname;
        switch (item->m_obj_type) {
            case O_DATABASE:
            case O_ROLE:
                objname = obj_name;
                break;
            case O_SCHEMA:
                item->get_fqdn_value(&objname);
                break;
            default:
                item->get_fqdn_value(&objname);
                if (!item->m_object && strlen(obj_name) > 0) {
                    objname.push_back('.');
                    objname.append(obj_name);
                }
                break;
        }
        const char *dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        char user_name[USERNAME_LEN];
        policy_simple_set::iterator it = policy_result.begin();
        policy_simple_set::iterator eit = policy_result.end();
        for (; it != eit; ++it) {
            char session_ip[MAX_IP_LEN] = {0};
            get_session_ip(session_ip, MAX_IP_LEN);

            if (ignore_db) {
                int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                    "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s %s %s], policy "
                    "id: [%lld]",
                    GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_type,
                    get_privilege_object_name(item->m_obj_type), objname.c_str(), *it);
                    securec_check_ss(rc, "\0", "\0");
            } else {
                int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                    "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], privilege type: [%s %s %s.%s], "
                    "policy id: [%lld]",
                    GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, priv_type,
                    get_privilege_object_name(item->m_obj_type), dbname, objname.c_str(), *it);
                    securec_check_ss(rc, "\0", "\0");
            }

            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
}

void rename_object(RenameStmt *stmt, const policy_set policy_ids, const policy_set security_policy_ids,
    RenameMap *renamed_objects)
{
    if (stmt == NULL) {
        return;
    }
    gs_stl::gs_string buffer;
    bool ignore_db = false;
    PolicyLabelItem item;
    const char *objectname = "";
    switch (stmt->renameType) {
        case OBJECT_DATABASE:
            item.m_obj_type = O_DATABASE;
            objectname = stmt->subname;
            ignore_db = true;
            break;
        case OBJECT_ROLE:
            item.m_obj_type = O_ROLE;
            objectname = stmt->subname;
            ignore_db = true;
            break;
        case OBJECT_USER:
            item.m_obj_type = O_ROLE;
            objectname = stmt->subname;
            ignore_db = true;
            break;
        case OBJECT_TABLESPACE:
            item.m_obj_type = O_TABLESPACE;
            objectname = stmt->subname;
            break;
        case OBJECT_TRIGGER:
            item.m_obj_type = O_TRIGGER;
            objectname = stmt->subname;
            break;
        case OBJECT_FOREIGN_SERVER:
            objectname = stmt->subname;
            item.m_obj_type = O_SERVER;
            break;
        case OBJECT_FUNCTION: {
            item.m_obj_type = O_FUNCTION;
            name_list_to_label(&item, stmt->object);
            break;
        }
        case OBJECT_TABLE:
        case OBJECT_STREAM:
        case OBJECT_FOREIGN_TABLE:
            item.m_obj_type = O_TABLE;
            item.m_schema = SchemaNameGetSchemaOid(stmt->relation->schemaname, true);
            item.set_object(stmt->relation->relname);
            break;
        case OBJECT_CONTQUERY:
        case OBJECT_VIEW:
            item.m_obj_type = O_VIEW;
            item.m_schema = SchemaNameGetSchemaOid(stmt->relation->schemaname, true);
            item.set_object(stmt->relation->relname);
            break;
        case OBJECT_INDEX:
            item.m_obj_type = O_INDEX;
            get_name_range_var(stmt->relation, &buffer);
            objectname = buffer.c_str();
            break;
        case OBJECT_COLUMN: {
            item.m_obj_type = O_COLUMN;
            item.m_schema = SchemaNameGetSchemaOid(stmt->relation->schemaname, true);
            item.set_object(stmt->relation->relname);
            int rec = snprintf_s(item.m_column, sizeof(item.m_column), sizeof(item.m_column) - 1, "%s", stmt->subname);
            securec_check_ss(rec, "\0", "\0");
            if (renamed_objects) {
                (*renamed_objects)[O_COLUMN].push_back(RenamePair(stmt->subname, stmt->newname));
            }
            break;
        }
        default:
            return;
    }
    if (accesscontrol_securityAuditObject_hook == NULL ||
        accesscontrol_securityAuditObject_hook(&security_policy_ids, &item, T_RENAME, "RENAME")) {
        check_object_policy_str(&policy_ids, &item, "RENAME", objectname, ignore_db);
    }
}

void alter_owner(AlterOwnerStmt *stmt, const policy_set policy_ids, const policy_set security_policy_ids)
{
    gs_stl::gs_string tmp;
    PolicyLabelItem item;
    const char *objectname = "";
    switch (stmt->objectType) {
        case OBJECT_DATABASE:
            item.m_obj_type = O_DATABASE;
            objectname = strVal(linitial(stmt->object));
            break;
        case OBJECT_TABLESPACE:
            item.m_obj_type = O_TABLESPACE;
            objectname = strVal(linitial(stmt->object));
            break;
        case OBJECT_FOREIGN_SERVER:
            item.m_obj_type = O_SERVER;
            objectname = strVal(linitial(stmt->object));
            break;
        case OBJECT_FUNCTION: {
            item.m_obj_type = O_FUNCTION;
            name_list_to_label(&item, stmt->object);
            break;
        }
        default:
            break;
    }
    if (accesscontrol_securityAuditObject_hook == NULL ||
        accesscontrol_securityAuditObject_hook(&security_policy_ids, &item, T_ALTER, "ALTER")) {
        check_object_policy_str(&policy_ids, &item, "ALTER", objectname);
    }
}

void add_current_path(int objtype, List *fqdn, gs_stl::gs_string *buffer)
{
    const char *schemaname = get_namespace_name(SchemaNameGetSchemaOid(NULL, true));
    int listsize = list_length(fqdn);

    switch (objtype) {
        case OBJECT_INDEX:
        case OBJECT_SEQUENCE:
        case OBJECT_TABLE:
        case OBJECT_VIEW:
        case OBJECT_CONTQUERY:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
        case OBJECT_FUNCTION: {
            switch (listsize) {
                case 1:
                    SET_DB_SCHEMA_TABLE
                    /* don't put break here */
                default:
                    name_list_to_string(fqdn, buffer);
                    break;
            }
            break;
        }
        case OBJECT_COLUMN: {
            switch (listsize) {
                case 2:
                    SET_DB_SCHEMA_TABLE
                    /* don't put break here */
                default:
                    name_list_to_string(fqdn, buffer);
                    break;
            }
            break;
        }
        case OBJECT_SCHEMA: {
            name_list_to_string(fqdn, buffer);
            break;
        }
        case OBJECT_ROLE:
        case OBJECT_USER:
        case OBJECT_DATABASE:
        case OBJECT_FOREIGN_SERVER:
        case OBJECT_TABLESPACE:
        case OBJECT_TRIGGER:
        default:
            name_list_to_string(fqdn, buffer);
            break;
    }
}
void get_cursor_tables(List *rtable, char *buff, size_t buff_size, int _printed_size,
    gs_stl::gs_vector<PolicyLabelItem> *cursor_objects)
{
    int rc;
    if (rtable != NIL) {
        ListCell *lc = NULL;
        bool first = true;
        bool has_object = false;
        int printed_size = _printed_size;
        foreach (lc, rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
            if (rte->relname && rte->rtekind == RTE_RELATION) {
                PolicyLabelItem item;
                get_fqdn_by_relid(rte, &item);
                if (cursor_objects) {
                    cursor_objects->push_back(item);
                }
                gs_stl::gs_string tbl_name;
                item.get_fqdn_value(&tbl_name);
                has_object = true;
                if (first) {
                    rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1,
                        ", TABLES {%s", tbl_name.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    printed_size += rc;
                } else {
                    rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1, ", %s",
                        tbl_name.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    printed_size += rc;
                }
                first = false;
            }
        }
        if (has_object) {
            rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1, "}");
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
        }
    }
}

void get_open_cursor_info(PlannedStmt *stmt, char *buff, size_t buff_size)
{
    int printed_size = -1;
    int rc;
    if (stmt->utilityStmt) {
        DeclareCursorStmt *cstmt = (DeclareCursorStmt *)stmt->utilityStmt;
        printed_size = snprintf_s(buff, buff_size, buff_size - 1, "%s ", cstmt->portalname);
        securec_check_ss(printed_size, "\0", "\0");
    }
    switch (stmt->commandType) {
        case CMD_SELECT: {
            rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1,
                "FOR SELECT FROM");
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
            break;
        }
        case CMD_INSERT: {
            rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1,
                "FOR INSERT TO");
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
            break;
        }
        case CMD_UPDATE: {
            rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1,
                "FOR UPDATE FROM");
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
            break;
        }
        case CMD_DELETE: {
            rc = snprintf_s(buff + printed_size, buff_size - printed_size, buff_size - printed_size - 1,
                "FOR DELETE FROM");
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
            break;
        }
        default:
            break;
    }
    get_cursor_tables(stmt->rtable, buff, buff_size, printed_size);
}

void login_handle(const char *dbname, const char *username, bool success, bool login)
{
    /* do nothing when enable_security_policy is off */
    if (!u_sess->attr.attr_security.Enable_Security_Policy || 
        !IsConnFromApp() || !OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return;
    }

    ResourceOwnerData *old_owner = NULL;
    if (t_thrd.utils_cxt.CurrentResourceOwner == NULL) {
        old_owner = create_temp_resourceowner();
    }

    /* dbname is necessary for login hook as u_sess info may not be ok now so that invalid oid */
    if (!is_database_valid(dbname) || !is_audit_policy_exist(dbname)) {
        return;
    }

    /* if access control policy worked, no need to run the audit policy
     * related audit logs would be recorded in LoginHandle_access
     */
    if ((security_LoginHandle_access_hook != NULL) ? 
        security_LoginHandle_access_hook(dbname, username, success, login) : true) {
        login_handle_audit(dbname, username, success, login);
    }

    if (old_owner != NULL) {
        release_temp_resourceowner(old_owner);
    }

    flush_access_logs(success ? AUDIT_OK : AUDIT_FAILED);
}

