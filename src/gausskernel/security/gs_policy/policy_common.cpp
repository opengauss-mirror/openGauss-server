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
 * policy_common.cpp
 *    functions for policy management
 *
 * IDENTIFICATION
 *    src/gausskernel/security/gs_policy/policy_common.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_authid.h"
#include "catalog/gs_policy_label.h"
#include "catalog/gs_auditing_policy.h"
#include "catalog/gs_auditing_policy_acc.h"
#include "catalog/gs_auditing_policy_filter.h"
#include "catalog/gs_auditing_policy_priv.h"
#include "catalog/gs_masking_policy.h"
#include "catalog/gs_masking_policy_actions.h"
#include "catalog/gs_masking_policy_filters.h"
#include "catalog/gs_policy_label.h"
#include "commands/dbcommands.h"
#include "commands/user.h"
#include "gs_policy/gs_string.h"
#include "gs_policy/policy_common.h"
#include "gs_policy/gs_policy_utils.h"
#include "libpq/auth.h"
#include "pgxc/pgxc.h"
#include "storage/lock/lock.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "pgaudit.h"

#define MAX_MSG_BUFF_SIZE 512
#define POLICY_VIEWS_NUM 5

LoadLabelsPtr load_labels_hook = NULL;
CheckPolicyPrivilegesForLabelPtr check_audit_policy_privileges_for_label_hook = NULL;
CheckPolicyAccessForLabelPtr check_audit_policy_access_for_label_hook = NULL;
CheckPolicyActionsForLabelPtr check_masking_policy_actions_for_label_hook = NULL;
IsMaskingHasObjPtr isMaskingHasObj_hook = NULL;
IsLabelExistPtr verify_label_hook = NULL;

bool GsPolicyLabel::operator == (const GsPolicyLabel &arg) const
{
    return (strcasecmp(m_data_value.c_str(), arg.m_data_value.c_str()) == 0) &&
           (strcasecmp(m_data_type.c_str(), arg.m_data_type.c_str()) == 0);
}

bool GsPolicyLabel::operator < (const GsPolicyLabel& arg) const
{
    /* first compare by data value */
    int res = strcasecmp(m_data_value.c_str(), arg.m_data_value.c_str());
    if (res < 0) {
        return true;
    }
    if (res > 0) {
        return false;
    }
    /* if data value is equal, compare by data type */
    return (strcasecmp(m_data_type.c_str(), arg.m_data_type.c_str()) < 0);
}

int GsPolicyLabel::operator - (const GsPolicyLabel& arg) const
{
    if (*this < arg) {
        return -1;
    } else if (arg < *this) {
        return 1;
    } else {
        return 0;
    }
}

/*
 * append_schema
 * 
 * add oid of schema into GsPolicyFQDN label
 */
static inline void append_schema(GsPolicyFQDN *fqdn, const char *schema)
{
    if (schema != NULL) {
        fqdn->m_value_schema = get_namespace_oid(schema, true);
    } else {
        fqdn->m_value_schema = SchemaNameGetSchemaOid(NULL);
    }
}

/*
 * append_object
 * 
 * add object into GsPolicyFQDN of label, we use lablel_item_type to idendify type of object
 */
static bool append_object(GsPolicyFQDN *fqdn, const char *object, const char *label_item_type)
{
    fqdn->is_function = false;
    if (object == NULL) {
        return false;
    }
    if (!strcasecmp(label_item_type, "function")) {
        fqdn->m_value_object = get_func_oid(object, fqdn->m_value_schema, NULL);
        fqdn->is_function = true;
    } else if (!strcasecmp(label_item_type, "table") ||
               !strcasecmp(label_item_type, "view") ||
               !strcasecmp(label_item_type, "column")) {
        fqdn->m_value_object = get_relname_relid(object, fqdn->m_value_schema);
    }
    if (OidIsValid(fqdn->m_value_object)) {
        return true;
    }
    return false;
}

/*
 * fqdn_get_name
 * 
 * get policy FQDN info from RangeVar
 */
static bool fqdn_get_name(const char *label_item_type, GsPolicyFQDN *fqdn, RangeVar *rel)
{
    if (!strcasecmp(label_item_type, "schema")) {
        append_schema(fqdn, rel->relname);
        return true;
    }

    bool is_column = !strcasecmp(label_item_type, "column");
    if (is_column) {
        append_schema(fqdn, rel->catalogname);
        if (!append_object(fqdn, rel->schemaname, label_item_type)) {
            return false;
        }
        if (rel->relname != NULL) {
            fqdn->m_value_object_attrib = rel->relname;
        } else {
            return false;
        }
    } else {
        append_schema(fqdn, rel->schemaname);
        if (!append_object(fqdn, rel->relname, label_item_type)) {
            return false;
        }
    }

    if (is_column && fqdn->m_value_object == 0) {
        return false;
    }
    return true;
}

/*
 * get_relation_id
 * 
 * get oid from schemaname or relname
 */
static Oid get_relation_id(RangeVar *rel, bool is_column)
{
    const char *schemaname = is_column ? rel->catalogname : rel->schemaname;
    const char *relname = is_column ? rel->schemaname : rel->relname;
    if (schemaname != NULL) {
        Oid namespaceId = get_namespace_oid(schemaname, true);
        if (OidIsValid(namespaceId)) {
            return get_relname_relid(relname, namespaceId);
        }
        return 0;
    }
    return (relname != NULL) ? RelnameGetRelid(relname) : 0;
}

/*
 * verify_function_name
 * 
 * walk through cache list looking for the func
 */
static bool verify_function_name(Oid namespaceId, const char *funcname)
{
    if (!OidIsValid(namespaceId)) {
        return false;
    }

    bool is_found = false;

    CatCList   *catlist = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif
    if (catlist != NULL) {
        for (int i = 0; i < catlist->n_members && !is_found; ++i) {
            HeapTuple    proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
            Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
            if (procform != NULL && procform->pronamespace == namespaceId) {
                Oid funcid = HeapTupleGetOid(proctup);
                is_found = OidIsValid(funcid);
            }
        }
        ReleaseSysCacheList(catlist);
    }
    return is_found;
}

/*
 * get_view_rules
 * 
 * walk through cache list looking for the func
 */
static List *get_view_rules(RuleLock *rules)
{
    List *locks = NIL;
    for (int i = 0; i < rules->numLocks && rules->rules[i]; ++i) {
        RewriteRule *rule = rules->rules[i];
        if (rule->event != CMD_SELECT) {
            continue;
        }
        locks = lappend(locks, rule);
    }
    return locks;
}

/*
 * verify_column_name
 * 
 * walk through all the rules for relid to check whether contain colname 
 */
static bool verify_column_name(Oid relid, const char *colname)
{
    bool is_found = false;
    Relation tbl_rel = relation_open(relid, AccessShareLock);
    if (RelationIsValid(tbl_rel)) {
        if (tbl_rel->rd_rules != NULL) { /* view */
            List *locks = get_view_rules(tbl_rel->rd_rules);
            if (locks != NULL) {
                ListCell *item = NULL;
                foreach (item, locks) {
                    RewriteRule *rule = (RewriteRule *)lfirst(item);
                    Query *rule_action = (Query *)linitial(rule->actions);
                    ListCell *target_item = NULL;
                    foreach (target_item, rule_action->targetList) {
                        TargetEntry *old_tle = (TargetEntry *)lfirst(target_item);
                        if (!strcasecmp(old_tle->resname, colname)) {
                            heap_close(tbl_rel, AccessShareLock);
                            return true;
                        }
                    }
                }
                list_free(locks);
            }
        }
        /* verify column */
        if (tbl_rel->rd_att != NULL) {
            for (int i = 0; i < tbl_rel->rd_att->natts && !is_found; ++i) {
                Form_pg_attribute attr = tbl_rel->rd_att->attrs[i];
                is_found = (strcasecmp(attr->attname.data, colname) == 0);
            }
        }
        heap_close(tbl_rel, AccessShareLock);
    }
    return is_found;
}

/*
 * verify_temp_table
 * 
 * walk through all the rules for relid to check whether contain relname 
 */
static void verify_temp_table(Oid relid, const char *relname, Relation relation, bool ignore = false)
{
    HeapTuple tuple = SearchSysCache1(RELOID, relid);
    if (!HeapTupleIsValid(tuple)) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                 errmsg("Cache lookup failed for relation %u", relid)));
    }
    Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tuple);
    /* Current object must be a normal table.
     * 'ignore' means whether report error when resource type is not TABLE.
     */
    if (reltup->relkind != RELKIND_RELATION && !ignore) {
        ReleaseSysCache(tuple);
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("\"%s\" is not a normal table", relname)));
    }

    /* Check temp table or not */
    if (reltup->relpersistence == RELPERSISTENCE_TEMP) {
        ReleaseSysCache(tuple);
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("Do not support policy label on temp table \"%s\"", relname)));
    }

    /* relase sys cache tuple */
    ReleaseSysCache(tuple);
}

enum LabelTypeEnum {
    T_TABLE,
    T_COLUMN,
    T_FUNCTION,
    T_VIEW,
    T_SCHEMA,
    T_UNKNOWN
};

static int get_label_type(const char *label_type)
{
    if (!strcasecmp(label_type, "table")) {
        return T_TABLE;
    }
    if (!strcasecmp(label_type, "column")) {
        return T_COLUMN;
    }
    if (!strcasecmp(label_type, "function")) {
        return T_FUNCTION;
    }
    if (!strcasecmp(label_type, "view")) {
        return T_VIEW;
    }
    if (!strcasecmp(label_type, "schema")) {
        return T_SCHEMA;
    }
    return T_UNKNOWN;
}

static void check_schema_type_exists(RangeVar *rel, Relation relation)
{
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !get_namespace_oid(rel->relname, true)) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("[%s] no such schema found", rel->relname)));
    }
}

static void check_table_type_exists(RangeVar *rel, Relation relation)
{
    Oid relid = get_relation_id(rel, false);
    if (!OidIsValid(relid)) {
        if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
            /* return error */
            heap_close(relation, RowExclusiveLock);
            const char *schemaname = rel->schemaname ?
                rel->schemaname : get_namespace_name(SchemaNameGetSchemaOid(NULL));
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("[%s.%s] no such relation found", schemaname, rel->relname)));
        }
    } else {
        verify_temp_table(relid, rel->relname, relation);
    }
}

static void check_view_type_exists(RangeVar *rel, Relation relation)
{
    Oid relid = get_relation_id(rel, false);
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !OidIsValid(relid)) {
        heap_close(relation, RowExclusiveLock);
        const char *schemaname = rel->schemaname ?
            rel->schemaname : get_namespace_name(SchemaNameGetSchemaOid(NULL));
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("[%s.%s] no such view found", schemaname, rel->relname)));
    }
}

static void check_column_type_exists(RangeVar *rel, Relation relation)
{
    Oid relid = get_relation_id(rel, true);
    /* Create resource label on temp table is not allowed. */
    if (g_instance.role != VDATANODE && OidIsValid(relid)) {
        verify_temp_table(relid, rel->schemaname, relation, true);
    }

    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !OidIsValid(relid)) {
        /* return error */
        heap_close(relation, RowExclusiveLock);
        const char *schemaname = rel->catalogname ?
            rel->catalogname : get_namespace_name(SchemaNameGetSchemaOid(NULL));
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("[%s.%s] no such relation found", schemaname, rel->schemaname)));
    }
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !verify_column_name(relid, rel->relname)) {
        /* return error */
        heap_close(relation, RowExclusiveLock);
        const char *schemaname = rel->catalogname ?
            rel->catalogname : get_namespace_name(SchemaNameGetSchemaOid(NULL));
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("[%s.%s.%s] no such relation column found", schemaname, rel->schemaname, rel->relname)));
    }
}

static void check_function_type_exists(RangeVar *rel, Relation relation)
{
    Oid namespaceId;
    if (rel->schemaname != NULL) {
        namespaceId = get_namespace_oid(rel->schemaname, true);
    } else {
        namespaceId = SchemaNameGetSchemaOid(NULL);
    }
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !verify_function_name(namespaceId, rel->relname)) {
        /* return error */
        heap_close(relation, RowExclusiveLock);
        const char *schemaname = rel->schemaname ? rel->schemaname : get_namespace_name(namespaceId);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("[%s.%s] no such function found", schemaname, rel->relname)));
    }
}

/*
 * verify_label_resource
 *
 * check whether relation existed for different label type
 */
static void verify_label_resource(const char *label_item_type, RangeVar *rel, Relation relation)
{
    if (!strcasecmp(rel->relname, "all")) {
        return;
    }

    int label_type = get_label_type(label_item_type);
    switch (label_type) {
        case T_VIEW:
            check_view_type_exists(rel, relation);
            break;
        case T_TABLE:
            check_table_type_exists(rel, relation);
            break;
        case T_COLUMN:
            check_column_type_exists(rel, relation);
            break;
        case T_FUNCTION:
            check_function_type_exists(rel, relation);
            break;
        case T_SCHEMA:
            check_schema_type_exists(rel, relation);
            break;
        default:
            break;
    }
}

/*
 * verify_column_is_bound_to_label
 * 
 * Note that the fqdn should not be bound to any labels info, output error when bounded
 */
void verify_column_is_bound_to_label(policy_labels_map *existing_labels, GsPolicyFQDN *fqdn)
{
    policy_labels_map::iterator lit = existing_labels->begin();
    policy_labels_map::iterator elit = existing_labels->end();
    for (; lit != elit; ++lit) {
        policy_labels_set::iterator sit = lit->second->begin();
        policy_labels_set::iterator esit = lit->second->end();
        for (; sit != esit; ++sit) {
            if (sit->m_data_value_fqdn.m_value_schema == fqdn->m_value_schema &&
                sit->m_data_value_fqdn.m_value_object == fqdn->m_value_object &&
                sit->m_data_value_fqdn.m_value_object_attrib == fqdn->m_value_object_attrib) {
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("Column %s already in other label", fqdn->m_value_object_attrib.c_str())));
                return;
            }
        }
    }
}

/*
 *  insert_policy_tuple
 * 
 *  insert a label tuple into relation, if is_empty, just insert blank
 */
static void insert_policy_tuple(CreatePolicyLabelStmt *stmt, Relation relation, const char *label_item_type, 
                                GsPolicyFQDN *fqdn, bool is_empty)
{
    const char *label_name = stmt->label_name;
    const char *label_type = stmt->label_type;
    HeapTuple htup;
    bool nulls[Natts_gs_policy_label] = {false};
    Datum values[Natts_gs_policy_label] = {0};

    values[Anum_gs_policy_label_labelname - 1] = DirectFunctionCall1(namein, CStringGetDatum(label_name));
    values[Anum_gs_policy_label_labeltype - 1] = DirectFunctionCall1(namein, CStringGetDatum(label_type));
    values[Anum_gs_policy_label_fqdnnamespace - 1] = ObjectIdGetDatum(fqdn->m_value_schema);
    values[Anum_gs_policy_label_fqdnid - 1] = ObjectIdGetDatum(fqdn->m_value_object);
    if (is_empty) {
        values[Anum_gs_policy_label_relcolumn - 1] = DirectFunctionCall1(namein, CStringGetDatum(""));
    } else {
        values[Anum_gs_policy_label_relcolumn - 1] = DirectFunctionCall1(namein, CStringGetDatum(fqdn->m_value_object_attrib.c_str()));
    }
    values[Anum_gs_policy_label_fqdntype - 1] = DirectFunctionCall1(namein, CStringGetDatum(label_item_type));

    htup = heap_form_tuple(relation->rd_att, values, nulls);
    /* Do the insertion */
    simple_heap_insert(relation, htup);
    CatalogUpdateIndexes(relation, htup);
    heap_freetuple(htup);
}

/*
 *  insert_stmt_label_item
 * 
 *  insert label items from CreatePolicyLabelStmt into relation,
 *  if is a column without table, close relation and report error.
 */
static void insert_stmt_label_item(CreatePolicyLabelStmt *stmt, Relation relation, policy_labels_map *existing_labels) 
{
    GsPolicyFQDN fqdn;
    if (stmt->label_items == NULL) { /* empty label */
        insert_policy_tuple(stmt, relation, "", &fqdn, true);
    }
    ListCell *label_item_itr = NULL;
    ListCell *label_obj = NULL;
    foreach (label_item_itr, stmt->label_items) {
        DefElem *defel = (DefElem *) lfirst(label_item_itr);
        const char *label_item_type = defel->defname;
        List *label_item_objects = (List *) defel->arg;
        if (g_instance.role == VDATANODE && !strcasecmp(label_item_type, "view")) {
            continue;
        }
        foreach (label_obj, label_item_objects) {
            RangeVar *rel = (RangeVar*)lfirst(label_obj);
            verify_label_resource(label_item_type, rel, relation);
            if (!fqdn_get_name(label_item_type, &fqdn, rel)) {
                if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
                    heap_close(relation, RowExclusiveLock);
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Column name without table")));
                }
                continue;
            }
            /* verify Column is unique in label catalog */
            if (!strcasecmp(label_item_type, "column")) {
                verify_column_is_bound_to_label(existing_labels, &fqdn);
            }
            insert_policy_tuple(stmt, relation, label_item_type, &fqdn, false);
        }
    }
}

/*
 *  create policy label
 * 
 *  load policy labels info into buffer to make it faster using load_labels_hook
 *  only works when policy feature is set
 */
void create_policy_label(CreatePolicyLabelStmt *stmt)
{
    /* check that if has access to config resource label */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    char buff[MAX_MSG_BUFF_SIZE] = {0};
    char user_name[USERNAME_LEN] = {0};
    char session_ip[MAX_IP_LEN] = {0};

    get_session_ip(session_ip, MAX_IP_LEN);
    (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

    errno_t rc = snprintf_s(buff, MAX_MSG_BUFF_SIZE, MAX_MSG_BUFF_SIZE - 1,
        "user name: [%s], app_name: [%s], ip: [%s], CREATE RESOURCE LABEL [%s], TYPE: [%s]",
        user_name, u_sess->attr.attr_common.application_name, session_ip, stmt->label_name, stmt->label_type);
    securec_check_ss(rc, "\0", "\0");
    save_manage_message(buff);

    const char *label_name = stmt->label_name;
    Relation relation;
    policy_labels_map existing_labels;

    /* Open the relation for read and insertion */
    relation = heap_open(GsPolicyLabelRelationId, RowExclusiveLock);

    /* validate that there is no such label name */
    load_existing_labels(relation, &existing_labels);

    policy_labels_map::iterator it = existing_labels.find(label_name);
    if (it != existing_labels.end()) {
        heap_close(relation, RowExclusiveLock);
        /* generate an error or notice */
        if (stmt->if_not_exists == true) {
            send_manage_message(AUDIT_OK);
            ereport(NOTICE, (errmsg("%s label already defined, skipping", label_name)));
        } else {
            send_manage_message(AUDIT_FAILED);
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_LABEL), errmsg("%s label already defined", label_name)));
        }
        return;
    }
    insert_stmt_label_item(stmt, relation, &existing_labels);
    heap_close(relation, RowExclusiveLock);

    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    if (load_labels_hook != NULL) {
        load_labels_hook(false);
    }
}

/*
 * fqdn_to_string
 * 
 * get fqdn string info depending on whehter fqdn is function
 */
static void fqdn_to_string(const GsPolicyFQDN *fqdn, gs_stl::gs_string *res, bool is_function)
{
    if (OidIsValid(fqdn->m_value_schema)) {
        const char *schema = get_namespace_name(fqdn->m_value_schema);
        if (schema != NULL && strlen(schema)) {
            res->append(schema);
        } else {
            return;
        }
    } else {
        return;
    }
    if (OidIsValid(fqdn->m_value_object)) {
        const char *object = is_function ? get_func_name(fqdn->m_value_object) : get_rel_name(fqdn->m_value_object);
        if (object != NULL && strlen(object)) {
            res->push_back('.');
            res->append(object);
        }
    } else {
        return;
    }
    if (!fqdn->m_value_object_attrib.empty()) {
        res->append(".");
        res->append(fqdn->m_value_object_attrib);
    }
}

/*
 * load_existing_labels
 * 
 * loading all labels into buffer
 */
void load_existing_labels(Relation rel, policy_labels_map *existing_labels)
{
    if (rel == NULL) {
        return;
    }
    HeapTuple               rtup;
    Form_gs_policy_label    rel_data;

    TableScanDesc scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_policy_label)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        GsPolicyLabel item;
        item.m_id = HeapTupleGetOid(rtup);
        item.m_name = rel_data->labelname.data;
        item.m_type = rel_data->labeltype.data;
        item.m_data_value_fqdn.m_value_schema = rel_data->fqdnnamespace;
        item.m_data_value_fqdn.m_value_object = rel_data->fqdnid;
        item.m_data_value_fqdn.m_value_object_attrib = rel_data->relcolumn.data;
        fqdn_to_string(&item.m_data_value_fqdn, &item.m_data_value, !strcasecmp(rel_data->fqdntype.data, "function"));
        item.m_data_type = rel_data->fqdntype.data;
        (*existing_labels)[item.m_name].insert(item);
    }
    tableam_scan_end(scan);
}

enum {
    S_UNKNOWN,
    S_REMOVE,
    S_ADD
};

static inline int get_stmt_type(const char *stmt_type)
{
    if (!strcasecmp(stmt_type, "remove")) {
        return S_REMOVE;
    }
    if (!strcasecmp(stmt_type, "add")) {
        return S_ADD;
    }
    return S_UNKNOWN;
}

/*
 * add_policy_label
 * 
 * add a GsPolicyLabel item to policy label table, if this item exists, just report an error.
 */
static void add_policy_label(Relation relation, GsPolicyLabel *item, 
                             policy_labels_map *existing_labels, bool is_exist)
{
    /* if the label exists in relation, just return an error */
    if (is_exist) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Resource %s %s already exists in label %s",
            item->m_data_type.c_str(), item->m_data_value.c_str(), item->m_name.c_str())));
        return;
    }
    bool nulls[Natts_gs_policy_label] = {false};
    Datum values[Natts_gs_policy_label] = {0};
    const char *column = item->m_data_value_fqdn.m_value_object_attrib.c_str();

    /* verify Column is unique in label catalog */
    if (!strcasecmp(item->m_data_type.c_str(), "column")) {
        verify_column_is_bound_to_label(existing_labels, &item->m_data_value_fqdn);
    }

    /* generate label name from item */
    values[Anum_gs_policy_label_labelname - 1] = DirectFunctionCall1(namein, CStringGetDatum(item->m_name.c_str()));
    values[Anum_gs_policy_label_labeltype - 1] = DirectFunctionCall1(namein, CStringGetDatum(item->m_type.c_str()));
    values[Anum_gs_policy_label_fqdnnamespace - 1] = ObjectIdGetDatum(item->m_data_value_fqdn.m_value_schema);
    values[Anum_gs_policy_label_fqdnid - 1] = ObjectIdGetDatum(item->m_data_value_fqdn.m_value_object);
    values[Anum_gs_policy_label_relcolumn - 1] = DirectFunctionCall1(namein, CStringGetDatum(column));
    values[Anum_gs_policy_label_fqdntype - 1] = DirectFunctionCall1(namein, CStringGetDatum(item->m_data_type.c_str()));

    /* Insert Data correctly */
    HeapTuple htup = heap_form_tuple(relation->rd_att, values, nulls);

    /* Do the insertion */
    simple_heap_insert(relation, htup);

    CatalogUpdateIndexes(relation, htup);
    heap_freetuple(htup);
}

/*
 * remove_policy_label
 * 
 * delete a GsPolicyLabel item in policy label table, if this item doesn't exists, just report an error.
 */
static void remove_policy_label(Relation relation, GsPolicyLabel *item, 
                                policy_labels_map *existing_labels, bool is_exist)
{
    /* if the label doesn't exist in relation, just return an error */
    if (!is_exist) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("No such resource %s %s found in label %s",
            item->m_data_type.c_str(), item->m_data_value.c_str(), item->m_name.c_str())));
        return;
    }
    ScanKeyData skey;
    /* Find the label row to delete. */
    ScanKeyInit(&skey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(item->m_id));
    SysScanDesc tgscan = systable_beginscan(relation, GsPolicyLabelOidIndexId, true, NULL, 1, &skey);
    HeapTuple tup = systable_getnext(tgscan);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_TRIGGERED_INVALID_TUPLE), 
            errmsg("Could not find tuple for label %lld", item->m_id)));
    }
    /* Delete the label tuple */
    simple_heap_delete(relation, &tup->t_self);
    systable_endscan(tgscan);
}


/*
 * alter_stmt_label_item
 * 
 * deal with statements about alter label, include add label and remove label.
 * input relation is GsPolicyLabelRelationId, and existing_labels include all labels in GsPolicyLabelRelationId.
 */
static void alter_stmt_label_item(AlterPolicyLabelStmt *stmt, Relation relation, 
                                  policy_labels_map *existing_labels)
{
    const char *label_name = stmt->label_name;
    gs_stl::gs_string label_type = "resource";
    policy_labels_map::iterator it = existing_labels->find(label_name);
    int stmt_type = get_stmt_type(stmt->stmt_type);    /* ADD or REMOVE */
    ListCell *label_item_itr = NULL;
    ListCell *label_obj = NULL;

    foreach (label_item_itr, stmt->label_items) {
        DefElem *defel = (DefElem *) lfirst(label_item_itr);
        const char *label_item_type = defel->defname;
        List *label_item_objects = (List *) defel->arg;
        if (g_instance.role == VDATANODE && !strcasecmp(label_item_type, "view")) {
            continue;
        }
        foreach (label_obj, label_item_objects) {
            RangeVar *rel = (RangeVar *)lfirst(label_obj);
            GsPolicyLabel item;
            bool item_founded = false;
            verify_label_resource(label_item_type, rel, relation);
            /* validate that provided resource is legal */
            if (!fqdn_get_name(label_item_type, &item.m_data_value_fqdn, rel)) {
                if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
                    heap_close(relation, RowExclusiveLock);
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Column name without table")));
                    return;
                }
                continue;
            }

            /* fill in the attributes of item */
            item.m_name = label_name;
            item.m_type = label_type;
            item.m_data_type = label_item_type;
            fqdn_to_string(&item.m_data_value_fqdn, &item.m_data_value, !strcasecmp(label_item_type, "function"));
            
            /*
            * Whether labelname exists has been verified before.
            * So empty label means the label for view type in DN (nothing records).
            * If label not found here, we just create a new label when adding a label item.
            */
            if (it != existing_labels->end()) {
                policy_labels_set::iterator i_it = it->second->find(item);
                item_founded = (i_it != it->second->end());
                if (item_founded) {
                    item.m_id = i_it->m_id;
                }
            }
            /* 
             * if item founded in existing_labels, we can only remove it, 
             * and if not found, we can only add it.
             */
            switch (stmt_type) {
                case S_REMOVE:
                    remove_policy_label(relation, &item, existing_labels, item_founded);
                    return;
                case S_ADD:
                    add_policy_label(relation, &item, existing_labels, item_founded);
                    break;
                default:
                    break;
            }
        }
    }
}

/*
 *  alter policy label
 * 
 *  load policy labels info into buffer to make it faster using load_labels_hook
 *  only works when policy feature is set
 */
void alter_policy_label(AlterPolicyLabelStmt *stmt)
{
    /* check that if has access to config resource label */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    const char *label_name = stmt->label_name;
    Relation relation;
    errno_t rc;
    policy_labels_map existing_labels;

    char buff[MAX_MSG_BUFF_SIZE] = {0};
    char user_name[USERNAME_LEN] = {0};
    char session_ip[MAX_IP_LEN] = {0};

    get_session_ip(session_ip, MAX_IP_LEN);
    (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

    rc = snprintf_s(buff, MAX_MSG_BUFF_SIZE, MAX_MSG_BUFF_SIZE - 1,
        "user name: [%s], app_name: [%s], ip: [%s], ALTER RESOURCE LABEL [%s], FOR %s",
        user_name, u_sess->attr.attr_common.application_name, session_ip, stmt->label_name, stmt->stmt_type);
    securec_check_ss(rc, "\0", "\0");
    save_manage_message(buff);

    /* Open the relation for read and insertion */
    relation = heap_open(GsPolicyLabelRelationId, RowExclusiveLock);

    load_existing_labels(relation, &existing_labels);

    /* first check whether such label exists */
    policy_labels_map::iterator it = existing_labels.find(label_name);
    if (g_instance.role != VDATANODE && it == existing_labels.end()) {
        heap_close(relation, RowExclusiveLock);
        send_manage_message(AUDIT_FAILED);
        /* generate an error */
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s no such label found", label_name)));
        return;
    }
    alter_stmt_label_item(stmt, relation, &existing_labels);

    heap_close(relation, RowExclusiveLock);

    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    if (load_labels_hook != NULL) {
        load_labels_hook(false);
    }
}

/*
 * remove_label
 * 
 * remove labels from the system table
 */
static void remove_label(const policy_labels_set *label_resources, Relation relation)
{
    for (policy_labels_set::const_iterator it = label_resources->begin(); it != label_resources->end(); ++it) {
        ScanKeyData skey;
        /*
         * Find the label row to delete.
         */
        ScanKeyInit(&skey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(it->m_id));
        SysScanDesc tgscan = systable_beginscan(relation, GsPolicyLabelOidIndexId, true, NULL, 1, &skey);
        HeapTuple tup = systable_getnext(tgscan);
        if (!HeapTupleIsValid(tup))
            ereport(ERROR,
                    (errcode(ERRCODE_TRIGGERED_INVALID_TUPLE),
                     errmsg("Could not find tuple for label %lld", it->m_id)));

        /* Delete the label tuple */
        simple_heap_delete(relation, &tup->t_self);
        systable_endscan(tgscan);
    }
}

/*
 * check_labels_to_drop
 * 
 * validate that this label is not bound to any policy feature such as mask audit or rls
 */
static bool check_labels_to_drop(const policy_labels_map *labels_to_drop)
{
    /* check label in accesses */
    if (check_audit_policy_privileges_for_label_hook != NULL) {
        if (check_audit_policy_privileges_for_label_hook(labels_to_drop)) {
            return false;
        }
    }
    /* check label in privileges */
    if (check_audit_policy_access_for_label_hook != NULL) {
        if (check_audit_policy_access_for_label_hook(labels_to_drop)) {
            return false;
        }
    }
    /* check label in masking actions */
    if (check_masking_policy_actions_for_label_hook != NULL) {
        if (check_masking_policy_actions_for_label_hook(labels_to_drop)) {
            return false;
        }
    }

    return true;
}


/*
 * drop_policy_label
 * 
 * go through all the labels to validate whether can drop it safely, then update the label buffer 
 */
void drop_policy_label(DropPolicyLabelStmt *stmt)
{
    /* check that if has access to config resource label */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    ListCell *label_obj = NULL;
    policy_labels_map existing_labels;
    policy_labels_map labels_to_drop;
    Relation labels_relation;

    foreach(label_obj, stmt->label_names) {
        const char* label_name = (const char *)(((Value*)lfirst(label_obj))->val.str);

        char buff[MAX_MSG_BUFF_SIZE] = {0};
        char user_name[USERNAME_LEN] = {0};
        char session_ip[MAX_IP_LEN] = {0};

        get_session_ip(session_ip, MAX_IP_LEN);
        (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

        errno_t rc = snprintf_s(buff, MAX_MSG_BUFF_SIZE, MAX_MSG_BUFF_SIZE - 1,
            "user name: [%s], app_name: [%s], ip: [%s], DROP RESOURCE LABEL [%s]",
            user_name, u_sess->attr.attr_common.application_name, session_ip, label_name);
        securec_check_ss(rc, "\0", "\0");
        save_manage_message(buff);
    }

    /* Open the relation for read and insertion */
    labels_relation = heap_open(GsPolicyLabelRelationId, RowExclusiveLock);
    load_existing_labels(labels_relation, &existing_labels);

    foreach (label_obj, stmt->label_names) {
        const char *label_name = (const char *)(((Value*)lfirst(label_obj))->val.str);
        /* first check whether such label exists */
        policy_labels_map::iterator it = existing_labels.find(label_name);
        if (it == existing_labels.end()) {
            if (g_instance.role == VDATANODE) {
                continue;
            }
            if (stmt->if_exists == false) {
                heap_close(labels_relation, RowExclusiveLock);
                send_manage_message(AUDIT_FAILED);
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s no such label found", label_name)));
                return;
            }
            ereport(NOTICE, (errmsg("%s no such label found, skipping", label_name)));
        } else {
            /* add this label to list of labels to be removed */
            labels_to_drop[*(it->first)] = *(it->second);
        }
    }
    if (labels_to_drop.size() == 0) {
        heap_close(labels_relation, RowExclusiveLock);
        if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
            send_manage_message(AUDIT_OK);
            ereport(NOTICE, (errmsg("no labels found to drop, skipping")));
        }
        return;
    }
    /* validate that this label is not bound to any policy */
    if (!check_labels_to_drop(&labels_to_drop)) {
        heap_close(labels_relation, RowExclusiveLock);
        send_manage_message(AUDIT_FAILED);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), 
                        errmsg("One or more labels bound to policies - cannot drop")));
        return;
    }

    for (policy_labels_map::iterator it = labels_to_drop.begin(); it != labels_to_drop.end(); ++it) {
        remove_label(it->second, labels_relation);
    }

    heap_close(labels_relation, RowExclusiveLock);

    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    if (load_labels_hook != NULL) {
        load_labels_hook(false);
    }
}

/*
 * check that policy plugin/feature is enabled, only used policy configurating by client
 * return error if policy plugin is off directly
 */
bool is_policy_enabled()
{
    if (superuser() || isPolicyadmin(GetUserId())) {
        return true;
    }
    return false;
}

bool is_security_policy_relation(Oid relid)
{
    if (!OidIsValid(relid)) {
        return false;
    }

    char *relname = get_rel_name(relid);
    if (relname == NULL) {
        return false;
    }

    const char *namelist[POLICY_VIEWS_NUM] = {"gs_labels", "gs_masking",
                                              "gs_auditing", "gs_auditing_access", "gs_auditing_privilege"};
    for (int i = 0; i < POLICY_VIEWS_NUM; ++i) {
        if (strcmp(relname, namelist[i]) == 0) {
            return true;
        }
    }

    /* check relation id is security policy catalog */
    switch (relid) {
        case GsAuditingPolicyRelationId:
        case GsAuditingPolicyAccessRelationId:
        case GsAuditingPolicyFiltersRelationId:
        case GsAuditingPolicyPrivilegesRelationId:
        case GsMaskingPolicyRelationId:
        case GsMaskingPolicyActionsId:
        case GsMaskingPolicyFiltersId:
        case GsPolicyLabelRelationId:
            return true;
        default:
            break;
    }

    return false;
}

#ifndef WIN32
bool get_files_list(const char *dir, std::vector<std::string>& files, const char *ext, int queue_size)
{
    struct dirent **namelist = NULL;
    int n;
    n = scandir(dir, &namelist, 0, alphasort);
    if (n < 0) {
        return false;
    }
    int ext_len = strlen(ext);
    bool next_file  = true;
    while (n-- && next_file) {
        int filelen = strlen(namelist[n]->d_name);
        if (!strncasecmp(namelist[n]->d_name + filelen - ext_len, ext, ext_len)) {
            files.emplace_back(namelist[n]->d_name);
            if (queue_size > 0 && queue_size < (int)files.size()) {
                next_file = false;
            }
        }
        free(namelist[n]);
    }
    free(namelist);
    return files.size() > 0;
}
#else
bool get_files_list(const char *dir,std::vector<std::string>& files, const char *ext, int queue_size)
{
    string path;
    string pattern;
    string filedir;
    WIN32_FIND_DATA fd;
    HANDLE hFind = INVALID_HANDLE_VALUE;
    size_t pos;
    path = UriToLocalPath(dir);
    filedir = path;
    pos = filedir.rfind('/');
    if (pos != string::npos) {
        pattern = path.substr(pos + 1);
        filedir = filedir.erase(pos);
    }
    if (pattern.size() == 0)
        gs_ereport(LEVEL_ERROR, "Invalid audit policy dir: \"%s\"", dir);
    std::replace(filedir.begin(), filedir.end(), '/', '\\');
    int ext_len = strlen(ext);
    hFind = FindFirstFile(path.c_str(), &fd);
    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            /* directory */
            if ((fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == FILE_ATTRIBUTE_DIRECTORY) {
                continue;
            }
            int filelen = strlen(fd.cFileName);
            if (!strncasecmp(fd.cFileName + filelen - ext_len, ext, ext_len))
                files.push_back(fd.cFileName);
        }while (FindNextFile(hFind, &fd));
        FindClose(hFind);
    }
}
#endif

