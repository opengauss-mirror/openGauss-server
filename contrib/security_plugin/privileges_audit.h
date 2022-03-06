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
 * privileges_audit.h
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/privileges_audit.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PRIVILEGES_AUDIT_H_
#define PRIVILEGES_AUDIT_H_
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy_object_types.h"

#define SET_DB_SCHEMA_TABLE    buffer->append(schemaname);    \
                            buffer->push_back('.');

typedef std::pair<gs_stl::gs_string, gs_stl::gs_string> names_pair;
void acl_audit_object(const policy_set *security_policy_ids, const policy_set *policy_ids, const ListCell *rel,
    const names_pair names, int priv_type, const char *priv_name, int objtype, int target_type);
bool internal_audit_object_str(const policy_set* security_policy_ids, const policy_set* policy_ids,
    const PolicyLabelItem* item, int priv_type, const char* priv_name, const char* objname = "",
    bool ignore_db = false);
void internal_audit_str(const policy_set *security_policy_ids, const policy_set *policy_ids, const char *value,
    int priv_type, const char *priv_name, int objtype, bool ignore_db = false);
void login_object(const policy_set *security_policy_ids, const policy_set *policy_ids, const char *login_str,
    int priv_type, const char *priv_name);
void internal_audit_object_str(const policy_set *security_policy_ids, const policy_set *policy_ids, const ListCell *rel,
    const names_pair names, int priv_type, const char *priv_name, int objtype, int target_type = ACL_TARGET_OBJECT, bool is_rolegrant = false, bool ignore_db = false);
void audit_table(const policy_set *security_policy_ids, const policy_set *policy_ids,
    RangeVar *rel, int priv_type, const char *priv_name, int objtype);
void alter_table(const policy_set *security_policy_ids, const policy_set *policy_ids,
    RangeVar *rel, int priv_type, const char *priv_name, int objtype);
void audit_schema(const policy_set security_policy_ids, const policy_set policy_ids,
    const char *schemaname, const char *newname, int priv_type, const char *priv_name = "");
void drop_command(DropStmt  *stmt, const policy_set *policy_ids, const policy_set *security_policy_ids);
void rename_object(RenameStmt  *stmt, const policy_set policy_ids, const policy_set security_policy_ids,
    RenameMap *renamed_objects = nullptr);
void alter_owner(AlterOwnerStmt *stmt, const policy_set policy_ids, const policy_set security_policy_ids);
void add_current_path(int objtype, List  *fqdn, gs_stl::gs_string *buffer);
void fill_label_item(PolicyLabelItem *item, int objtype, List *fqdn);
void destroy_logs();
void get_cursor_tables(List *rtable, char *buff, size_t buff_size, int _printed_size,
    gs_stl::gs_vector<PolicyLabelItem> *cursor_objects = nullptr);
void get_open_cursor_info(PlannedStmt *stmt, char *buff, size_t buff_size);
PrivObject get_privtype_from_aclobject(GrantObjectType acl_type);

#endif /* PRIVILEGES_AUDIT_H_ */