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
 * gs_policy_masking.cpp
 *    functions for masking policy management
 *
 * IDENTIFICATION
 *    src/gausskernel/security/gs_policy/gs_policy_masking.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "gs_policy/gs_policy_masking.h"
#include "gs_policy/policy_common.h"
#include "gs_policy/gs_policy_utils.h"

#include "gs_policy/gs_string.h"
#include "gs_policy/gs_map.h"
#include "catalog/gs_policy_label.h"
#include "catalog/gs_masking_policy.h"
#include "catalog/gs_masking_policy_actions.h"
#include "catalog/gs_masking_policy_filters.h"
#include "catalog/indexing.h"
#include "commands/user.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "storage/lock/lock.h"
#include "access/xact.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "pgxc/pgxc.h"
#include "pgaudit.h"
#include "utils/snapmgr.h"


LoadPoliciesPtr load_masking_policies_hook = NULL;
LoadPolicyAccessPtr load_masking_policy_actions_hook = NULL;
LoadPolicyFilterPtr load_masking_policy_filter_hook = NULL;
ValidateBehaviourPtr validate_masking_behaviour_hook = NULL;
VerifyLabelsByPolicy gs_verify_labels_by_policy_hook = NULL;

static const char* g_maskFunctions[] = {
    "creditcardmasking",
    "basicemailmasking",
    "fullemailmasking",
    "alldigitsmasking",
    "shufflemasking",
    "randommasking",
    "regexpmasking",
    NULL
};

static void reload_masking_policies()
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return;
    }

    if (load_masking_policies_hook != NULL) {
        load_masking_policies_hook(false);
    }
    if (load_masking_policy_actions_hook != NULL) {
        load_masking_policy_actions_hook(false);
    }
    /* load filters must be last */
    if (load_masking_policy_filter_hook != NULL) {
        load_masking_policy_filter_hook(false);
    }
}

#define BUFFSIZE 512
bool PgPolicyMaskingActionStruct::operator == (const PgPolicyMaskingActionStruct &arg) const
{
    if (*this < arg) {
        return false;
    } else if (arg < *this) {
        return false;
    } else {
        return true;
    }
}

bool PgPolicyMaskingActionStruct::operator < (const PgPolicyMaskingActionStruct &arg) const
{
    int res = strcasecmp(m_type.c_str(), arg.m_type.c_str());
    if (res < 0)return true;
    if (res > 0)return false;
    res = strcasecmp(m_label_name.c_str(), arg.m_label_name.c_str());
    if (res < 0)return true;
    if (res > 0)return false;
    return m_policy_oid < arg.m_policy_oid;
}

int PgPolicyMaskingActionStruct::operator - (const PgPolicyMaskingActionStruct &arg) const
{
    if (*this < arg) {
        return -1;
    } else if (arg < *this) {
        return 1;
    } else {
        return 0;
    }
}

typedef gs_stl::gs_map<gs_stl::gs_string, gs_stl::gs_vector<PgPolicyMaskingActionStruct>> masking_label_to_actions_map;

/*
 * load_existing_masking_labels
 * 
 * load all labels of policyOid in catalog gs_masking_policy_actions into policy_labels
 */
void load_existing_masking_labels(policy_labelname_set *policy_labels, Oid policyOid)
{
    HeapTuple maskingPolicyTuple = NULL;
    Relation relation = heap_open(GsMaskingPolicyActionsId, RowExclusiveLock);
    if (relation == NULL) {
        return;
    }

    Form_gs_masking_policy_actions rel_data = NULL;
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0],
                Anum_gs_masking_policy_act_policy_oid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(policyOid));

    /* Search tuple by index */
    SysScanDesc scanDesc = systable_beginscan(relation, GsMaskingPolicyActionsPolicyOidIndexId,
                                              true, NULL, 1, scanKey);

    while ((maskingPolicyTuple = systable_getnext(scanDesc)) != NULL) {
        rel_data = (Form_gs_masking_policy_actions)GETSTRUCT(maskingPolicyTuple);
        if (rel_data == NULL) {
            continue;
        }
        policy_labels->insert(rel_data->actlabelname.data);
    }
    systable_endscan(scanDesc);
    heap_close(relation, RowExclusiveLock);

}

/*
 * load_existing_masking_actions
 * 
 * load all action items of policyOid in catalog gs_masking_policy_actions actions
 * load all action items of policyOid in to masking label map
 */
void load_existing_masking_actions(masking_actions_set* actions,
    masking_label_to_actions_map* labels_to_actions, long long policy_oid = 0)
{
    Relation relation = heap_open(GsMaskingPolicyActionsId, RowExclusiveLock);
    if (relation == NULL) {
        return;
    }
    HeapTuple rtup = NULL;
    Form_gs_masking_policy_actions rel_data = NULL;
    PgPolicyMaskingActionStruct item;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_masking_policy_actions)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        item.m_id = HeapTupleGetOid(rtup);
        item.m_type = rel_data->actiontype.data;
        item.m_params = rel_data->actparams.data;
        item.m_label_name = rel_data->actlabelname.data;
        item.m_policy_oid = (long long)(rel_data->policyoid);
        /* 
         * when policy_oid is zero - load all privileges
         * when policy_oid greater than zero, load only matching
         */
        if ((policy_oid == 0) || ((policy_oid > 0) && (item.m_policy_oid == policy_oid))) {
            actions->insert(item);
            /* we want to track all masking actions for each label */
            (*labels_to_actions)[item.m_label_name].push_back(item);
        }
    }
    tableam_scan_end(scan);

    heap_close(relation, RowExclusiveLock);
}

static bool check_masking_attrtype(Oid atttypeid)
{
    if (!OidIsValid(atttypeid)) {
        return false;
    }
    bool result = false;
    switch (atttypeid) {
        case BOOLOID:
        case RELTIMEOID:
        case TIMEOID:
        case TIMETZOID:
        case INTERVALOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case SMALLDATETIMEOID:
        case ABSTIMEOID:
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case NAMEOID:
        case INT8OID:
        case INT4OID:
        case INT2OID:
        case INT1OID:
        case NUMERICOID:
        case FLOAT4OID:
        case FLOAT8OID:
            result = true;
            break;
        default:
            break;
    }
    return result;
}

static bool column_allow_to_masking(Oid relid, const char *column)
{
    if (!OidIsValid(relid)) {
        return false;
    }
    Oid column_typeid;
    Form_pg_attribute attr_struct = NULL;
    HeapTuple attr_tuple = SearchSysCacheAttName(relid, column);

    if (!HeapTupleIsValid(attr_tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" does not exist", column)));
    }
    attr_struct = (Form_pg_attribute)GETSTRUCT(attr_tuple);
    column_typeid = attr_struct->atttypid;
    ReleaseSysCache(attr_tuple);
    return check_masking_attrtype(column_typeid);
}

/*
 * add_labels_to_masking_action
 * 
 * insert row of catalog gs_masking_policy_actions
 */
static inline void add_labels_to_masking_action(const gs_stl::gs_string action_type,
    const gs_stl::gs_string action_params, const gs_stl::gs_string label_name,
    Relation relation, Oid policyOid, Datum curtime)
{
    bool policy_actions_nulls[Natts_gs_masking_policy_actions] = {false};
    Datum policy_actions_values[Natts_gs_masking_policy_actions] = {0};

    if (verify_label_hook) {
        if (!verify_label_hook(label_name.c_str())) {
            heap_close(relation, RowExclusiveLock);
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("[%s] no such label found", label_name.c_str())));
        }
    }

    /* for now mask on table/view is not allowed */
    Relation label_rel = NULL;
    policy_labels_map existing_labels;
    label_rel = heap_open(GsPolicyLabelRelationId, RowExclusiveLock);
    load_existing_labels(label_rel, &existing_labels);
    heap_close(label_rel, RowExclusiveLock);
    policy_labels_map::iterator lbit = existing_labels.find(label_name);
    if (lbit != existing_labels.end()) {
        policy_labels_set::iterator lit = (lbit->second)->begin();
        policy_labels_set::iterator eit = (lbit->second)->end();
        for (; lit != eit; ++lit) {
            Oid fqdnOid = lit->m_data_value_fqdn.m_value_object;
            /* masking only allowed operator on column */
            if (strcmp(lit->m_data_type.c_str(), "column") != 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("Masking policy can only operate on column object.")));
            }
            /* masked column should belong to an ordinary table */
            if (OidIsValid(fqdnOid)) {
                char relkind = get_rel_relkind(fqdnOid);
                if (relkind != RELKIND_RELATION || get_rel_persistence(fqdnOid) != 'p') {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("Masking policy can only operate on column of ordinary table.")));
                }
                const char *column_name = lit->m_data_value_fqdn.m_value_object_attrib.c_str();
                if (!column_allow_to_masking(fqdnOid, column_name)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("Type of column %s is not supported to mask.", column_name)));
                }
            }
        }
    }

    policy_actions_values[Anum_gs_masking_policy_act_action_type        - 1] = DirectFunctionCall1(namein, CStringGetDatum(action_type.c_str()));
    policy_actions_values[Anum_gs_masking_policy_act_action_params      - 1] = DirectFunctionCall1(namein, CStringGetDatum(action_params.c_str()));
    policy_actions_values[Anum_gs_masking_policy_act_label_name         - 1] = DirectFunctionCall1(namein, CStringGetDatum(label_name.c_str()));
    policy_actions_values[Anum_gs_masking_policy_act_policy_oid         - 1] = ObjectIdGetDatum(policyOid);
    policy_actions_values[Anum_gs_masking_policy_act_modify_date        - 1] = curtime;

    HeapTuple policy_htup = heap_form_tuple(relation->rd_att, policy_actions_values, policy_actions_nulls);

    simple_heap_insert(relation, policy_htup);
    CatalogUpdateIndexes(relation, policy_htup);
    heap_freetuple(policy_htup);
}

/*
 * add_masking_filters
 * insert a set of filters into gs_masking_policy_filters
 */ 
static inline void add_masking_filters(const filters_set* filters_to_add, Relation relation)
{
    Datum       curtime;
    HeapTuple   policy_filters_htup;
    bool        policy_filters_nulls[Natts_gs_masking_policy_filters] = {false};
    Datum       policy_filters_values[Natts_gs_masking_policy_filters] = {0};
    errno_t     rc = EOK;

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    for (filters_set::const_iterator it = filters_to_add->begin(); it != filters_to_add->end(); ++it) {
        /* restore values and nulls for insert new node group record */
        rc = memset_s(policy_filters_values, sizeof(policy_filters_values), 0, sizeof(policy_filters_values));
        securec_check(rc, "\0", "\0");
        rc =  memset_s(policy_filters_nulls, sizeof(policy_filters_nulls), false, sizeof(policy_filters_nulls));
        securec_check(rc, "\0", "\0");

        policy_filters_values[Anum_gs_masking_policy_fltr_filter_type      - 1] = DirectFunctionCall1(namein, CStringGetDatum(it->m_type.c_str()));
        policy_filters_values[Anum_gs_masking_policy_fltr_label_name       - 1] = DirectFunctionCall1(namein, CStringGetDatum(it->m_label_name.c_str()));
        policy_filters_values[Anum_gs_masking_policy_fltr_logical_operator - 1] = CStringGetTextDatum(it->m_tree_string.c_str());
        policy_filters_values[Anum_gs_masking_policy_fltr_policy_oid       - 1] = ObjectIdGetDatum(it->m_policy_oid);
        policy_filters_values[Anum_gs_masking_policy_fltr_modify_date      - 1] = curtime;

        policy_filters_htup = heap_form_tuple(relation->rd_att, policy_filters_values, policy_filters_nulls);

        /* Do the insertion */
        simple_heap_insert(relation, policy_filters_htup);

        CatalogUpdateIndexes(relation, policy_filters_htup);
        heap_freetuple(policy_filters_htup);
    }
}


struct MaskingActionItem  {
    gs_stl::gs_string m_action;
    gs_stl::gs_string m_params;
    policy_labelname_set m_labels;
};

/*
 * parse_masking_action
 * for input item, verify the using function is a masking function
 * and parse function params into m_params, parse relate labels into m_labels
 */
static bool parse_masking_action(DefElem* policy_item, MaskingActionItem* item, gs_stl::gs_string* err_msg)
{
    if (policy_item == NULL) {
        return true;
    }

    int index = 0;
    bool is_found = false;
    if (policy_item->defnamespace != NULL) {
        item->m_action.append(policy_item->defnamespace);
        item->m_action.append(".");
    } else {
        for (int i = 0; g_maskFunctions[i] != NULL; ++i) {
            if (strcmp(g_maskFunctions[i], (policy_item->defname)) == 0 ||
                strcasecmp("maskall", (policy_item->defname)) == 0) {
                is_found = true;
                break;
            }
        }
        if (!is_found) {
            item->m_action.append(get_namespace_name(GetOidBySchemaName()));
            item->m_action.append(".");
        }
    }
    item->m_action.append(policy_item->defname);
    bool is_validated = false;
    ListCell   *item_list = NULL;
    foreach(item_list,  (List *)policy_item->arg) {
        if (index == 0) { /* masking function params */
            List *action_params = (List *)(lfirst(item_list));
            ListCell   *param = NULL;
            foreach(param, action_params) {
                DefElem *param_value_node = (DefElem *)(lfirst(param));
                if (param_value_node) {
                    const char * param_type = param_value_node->defname;
                    const char * param_val = strVal(param_value_node->arg);
                    (void)item->m_params.append(param_type);
                    (void)item->m_params.append(":");
                    (void)item->m_params.append(param_val);
                    (void)item->m_params.append(",");
                }
            }
            if (item->m_params.size()) {
                item->m_params.pop_back(); /* remove last ',' */
            }
            if (validate_masking_behaviour_hook) {
                is_validated = true;
                /* check function by parameters */
                bool invalid_params = false;
                if (!validate_masking_behaviour_hook(item->m_action.c_str(), item->m_params.c_str(), 
                    &invalid_params, false)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmodule(MOD_UDF),
                        errmsg("failed to mask"),
                            errdetail("wrong masking function %s", invalid_params ? ": invalid parameters" : " ")));
                }
            }
        } else if (index == 1) { /* labels to apply the policy */
            DefElem *label_defel = (DefElem *)(lfirst(item_list));
            ListCell   *label_name_item = NULL;
            foreach(label_name_item, (List *)label_defel->arg) {
                RangeVar *rel = (RangeVar*)lfirst(label_name_item);
                gs_stl::gs_string label_name;
                construct_resource_name(rel, &label_name);
                item->m_labels.insert(label_name);
            }
        } else {   /* wrong num of params */
            (*err_msg) = "wrong number of policy arguments";
            return false;
        }
        index++;
    }
    return true;
}

/*
 * update_masking_filters
 * 
 * update gs_masking_policy_filters using filters_to_update
 */
static void update_masking_filters(const filters_set* filters_to_update, Relation policy_filters_relation)
{
    for (filters_set::const_iterator it = filters_to_update->begin(); it != filters_to_update->end(); ++it) {
        ScanKeyData scanKey[1];
        ScanKeyInit(&scanKey[0],
                    Anum_gs_masking_policy_fltr_policy_oid,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(it->m_policy_oid));

        /* Search tuple by index */
        SysScanDesc scanDesc = systable_beginscan(policy_filters_relation,
                                                  GsMaskingPolicyFiltersPolicyOidIndexId,
                                                  true,
                                                  NULL,
                                                  1,
                                                  scanKey);

        HeapTuple maskingPolicyTuple = systable_getnext(scanDesc);
        if (!HeapTupleIsValid(maskingPolicyTuple)) {
            add_masking_filters(filters_to_update, policy_filters_relation);
        } else {
            Datum values[Natts_gs_masking_policy_filters] = {0};
            bool  nulls[Natts_gs_masking_policy_filters] = {false};
            bool  replaces[Natts_gs_masking_policy_filters] = {false};

            values[Anum_gs_masking_policy_fltr_logical_operator - 1] = CStringGetTextDatum(it->m_tree_string.c_str());
            nulls[Anum_gs_masking_policy_fltr_logical_operator - 1] = false;
            replaces[Anum_gs_masking_policy_fltr_logical_operator - 1] = true;

            HeapTuple newtuple = heap_modify_tuple(maskingPolicyTuple,
                                                   RelationGetDescr(policy_filters_relation),
                                                   values,
                                                   nulls,
                                                   replaces);
            simple_heap_update(policy_filters_relation, &newtuple->t_self, newtuple);
            CatalogUpdateIndexes(policy_filters_relation, newtuple);
        }

        systable_endscan(scanDesc);
    }
}

static void get_policy_filter(gs_stl::gs_string *filter, Oid polid)
{
    Relation rel = heap_open(GsMaskingPolicyFiltersId, RowExclusiveLock);
    if (rel == NULL) {
        return;
    }
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0],
                Anum_gs_masking_policy_fltr_policy_oid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(polid));
    SysScanDesc scanDesc = systable_beginscan(rel,
                                              GsMaskingPolicyFiltersPolicyOidIndexId, 
                                              true,
                                              NULL,
                                              1,
                                              scanKey);

    HeapTuple rtup = systable_getnext(scanDesc);
    if (HeapTupleIsValid(rtup)) {
        bool isNull = true;
        const char *logical_operator = "";
        Datum logical_operator_datum = heap_getattr(rtup, Anum_gs_masking_policy_fltr_logical_operator,
                                                    RelationGetDescr(rel), &isNull);
        if (!isNull) {
            logical_operator = TextDatumGetCString(logical_operator_datum);
        }
        (*filter) = logical_operator;
    }

    systable_endscan(scanDesc);
    heap_close(rel, RowExclusiveLock);
}

/*
 * handle_alter_add_or_update_filter
 * 
 * handle alter commands of filter
 */
static void handle_alter_add_or_update_filter(List* filter, Oid policyOid,
    policy_labelname_set *policy_labels, bool to_add, bool is_alter)
{
    /* If defined policy stmt has no filters
     * 1. create stmt but not filters, then using filter "" to verify
     * 2. alter stmt but not filters, then get filters of this policy oid, and then verify
     */     
    if (filter == NULL) {
        if (!policy_labels->empty()) {
            gs_stl::gs_string flat_tree = "";
            if (is_alter) {
                get_policy_filter(&flat_tree, policyOid);
            }
            if (gs_verify_labels_by_policy_hook != NULL) {
                gs_stl::gs_string polname;
                if (!gs_verify_labels_by_policy_hook(flat_tree.c_str(), policy_labels, policyOid, &polname)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("current policy is conflict with exist policy: %s", polname.c_str())));
                }
            }
        }
        return;
    }

    gs_stl::gs_string flatten_tree;
    /* Process new filters from parser tree and tranform them into string */
    if (!process_new_filters(filter, &flatten_tree)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unsupported policy filter values")));
    }
    filters_set filters_to_alter;
    if (flatten_tree.size() > 0) {
        PgPolicyFiltersStruct item;
        item.m_type = "logical_expr";
        item.m_label_name = "logical_expr";
        item.m_tree_string = flatten_tree;
        item.m_policy_oid = policyOid;
        filters_to_alter.insert(item);
    }
    /* when stmt has filter clause, just do verify using it's filter */
    if (gs_verify_labels_by_policy_hook != NULL) {
        gs_stl::gs_string polname;
        /* If it is alter stmt and this stmt is alter filter statement*/
        if (is_alter && policy_labels->empty()) {
            policy_labelname_set cur_labels;
            load_existing_masking_labels(&cur_labels, policyOid);
            if (!gs_verify_labels_by_policy_hook(flatten_tree.c_str(), &cur_labels, policyOid, &polname)) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("current policy is conflict with exist policy: %s", polname.c_str())));
            }
        /* if it is create stmt with filter_clause and have intersect */
        } else if (!policy_labels->empty() &&
            !gs_verify_labels_by_policy_hook(flatten_tree.c_str(), policy_labels, policyOid, &polname)) {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("current policy is conflict with exist policy: %s", polname.c_str())));
        }
    }

    if (filters_to_alter.size() > 0) {
        Relation policy_filters_relation = heap_open(GsMaskingPolicyFiltersId, RowExclusiveLock);
        if (policy_filters_relation) {
            if (to_add) {
                add_masking_filters(&filters_to_alter, policy_filters_relation);
            } else {
                update_masking_filters(&filters_to_alter, policy_filters_relation);
            }

            heap_close(policy_filters_relation, RowExclusiveLock);
        }
    }
}

/*
 * create_masking_policy
 * 
 * handle create masking policy command
 */
void create_masking_policy(CreateMaskingPolicyStmt *stmt)
{
    /* check that if has access to config masking policy */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    char user_name[USERNAME_LEN] = {0};
    (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));
    char buff[BUFFSIZE] = {0};
    char session_ip[MAX_IP_LEN] = {0};
    get_session_ip(session_ip, MAX_IP_LEN);

    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
        "user name: [%s], app_name: [%s], ip: [%s], CREATE MASKING POLICY [%s]",
        user_name, u_sess->attr.attr_common.application_name, session_ip, stmt->policy_name);
    securec_check_ss(rc, "\0", "\0");
    save_manage_message(buff);

    const char *policy_name = stmt->policy_name;
    bool        policy_enabled = stmt->policy_enabled;
    Datum       curtime;

    HeapTuple   policy_htup;
    bool        policy_nulls[Natts_gs_masking_policy] = {false};
    Datum       policy_values[Natts_gs_masking_policy] = {0};

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    Relation policy_relation = heap_open(GsMaskingPolicyRelationId, RowExclusiveLock);
    if (policy_relation == NULL) {
        send_manage_message(AUDIT_FAILED);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s", "failed to open policies relation")));
        return;
    }
    
    /* less than MAX_POLICIES_NUM is recommended */
    if (get_num_of_existing_policies<Form_gs_masking_policy>(policy_relation) >= MAX_POLICIES_NUM) {
        ereport(WARNING,
                (errmsg("%s", "Too many policies, adding new policy is not recommended")));
    }

    /* check whether such policy exists */
    if (stmt->if_not_exists == true) {
        policies_set existing_policies;
        load_existing_policies<Form_gs_masking_policy>(policy_relation, &existing_policies);

        GsPolicyStruct cur_policy;
        cur_policy.m_name = policy_name;
        policies_set::iterator it = existing_policies.find(cur_policy);
        if (it != existing_policies.end()) {
            heap_close(policy_relation, RowExclusiveLock);
            ereport(NOTICE,
                    (errmsg("%s policy already exists, skipping", policy_name)));
            send_manage_message(AUDIT_OK);
            return;
        }
    }

    policy_values[Anum_gs_masking_policy_pol_name        - 1] = DirectFunctionCall1(namein, CStringGetDatum(policy_name));
    policy_values[Anum_gs_masking_policy_pol_comments    - 1] = DirectFunctionCall1(namein, CStringGetDatum(""));
    policy_values[Anum_gs_masking_policy_pol_modify_date - 1] = curtime;
    policy_values[Anum_gs_masking_policy_pol_enabled     - 1] = BoolGetDatum(policy_enabled);
    policy_htup = heap_form_tuple(policy_relation->rd_att, policy_values, policy_nulls);
    /* Do the insertion */
    simple_heap_insert(policy_relation, policy_htup);
    CatalogUpdateIndexes(policy_relation, policy_htup);
    Oid policyOid = HeapTupleGetOid(policy_htup);
    heap_freetuple(policy_htup);
    heap_close(policy_relation, RowExclusiveLock);

    /* Process masking actions */
    Relation masking_actions_relation = heap_open(GsMaskingPolicyActionsId, RowExclusiveLock);
    if (masking_actions_relation == NULL) {
        send_manage_message(AUDIT_FAILED);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s", "failed to open masking actions relation")));
        return;
    }
    /* Extract policy targets from the statement node tree */
    ListCell   *policy_item_iter = NULL;
    policy_labelname_set policy_labels;
    foreach(policy_item_iter, stmt->policy_data) {
        gs_stl::gs_string err_msg;
        DefElem *policy_item = (DefElem *) lfirst(policy_item_iter);
        /* for each maskfun - labels pair: action_item = (maskfunc, param, labels) */
        MaskingActionItem action_item;
        bool res = parse_masking_action(policy_item, &action_item, &err_msg);
        if (res) {
            policy_labels.insert(action_item.m_labels.begin(), action_item.m_labels.end());
            gs_stl::gs_set<gs_stl::gs_string>::const_iterator it;
            for (it = action_item.m_labels.begin(); it != action_item.m_labels.end(); ++it) {
                add_labels_to_masking_action(action_item.m_action, action_item.m_params,
                                             *it, masking_actions_relation, policyOid, curtime);
            }
        } else {
            heap_close(masking_actions_relation, RowExclusiveLock);
            /* report about unknown policy type */
            send_manage_message(AUDIT_FAILED);
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("%s", err_msg.c_str())));
            return;
        }
    }
    heap_close(masking_actions_relation, RowExclusiveLock);

    /* Process masking policy filters */
    handle_alter_add_or_update_filter(stmt->policy_filters, policyOid, &policy_labels, true, false);
    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    reload_masking_policies();
}

/*
 * update_policy
 * 
 * update masking policy into catalog gs_masking_policy
 */
static bool update_policy(const GsPolicyStruct* policy, Relation relation,
                          bool policy_status_changed, gs_stl::gs_string* err_msg)
{
    bool        policy_nulls[Natts_gs_masking_policy] = {false};
    bool        policy_replaces[Natts_gs_masking_policy] = {false};
    Datum       policy_values[Natts_gs_masking_policy] = {0};
    Datum       curtime;

    ScanKeyData skey;
    /* Find the policy row to update */
    ScanKeyInit(&skey,
                ObjectIdAttributeNumber,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(policy->m_id));

    SysScanDesc tgscan = systable_beginscan(relation, GsMaskingPolicyOidIndexId, true, NULL, 1, &skey);
    HeapTuple tup = NULL;
    if (tgscan == NULL) {
        (void)err_msg->append("could not scan index GsMaskingPolicyOidIndexId.");
        return false;
    }
    tup = systable_getnext(tgscan);
    if (!tup || !HeapTupleIsValid(tup)) {
        systable_endscan(tgscan);
        (void)err_msg->append("could not find tuple for policy ");
        (void)err_msg->append(policy->m_name.c_str());
        return false;
    }

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    if (policy_status_changed) {
        policy_replaces[Anum_gs_masking_policy_pol_enabled     - 1] = true;
        policy_values[Anum_gs_masking_policy_pol_enabled     - 1] = BoolGetDatum(policy->m_enabled);
    } else {
        policy_replaces[Anum_gs_masking_policy_pol_comments - 1] = true;
        policy_values[Anum_gs_masking_policy_pol_comments     - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(policy->m_comments.c_str()));
    }
    policy_replaces[Anum_gs_masking_policy_pol_modify_date - 1] = true;
    policy_values[Anum_gs_masking_policy_pol_modify_date - 1] = curtime;

    HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), policy_values, policy_nulls, policy_replaces);
    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);
    systable_endscan(tgscan);

    return true;
}

/*
 * update_labels_in_masking_action
 * 
 * update labels of actions and verify whether label can insert
 */
static inline bool update_labels_in_masking_action(const PgPolicyMaskingActionStruct* action_item,
    const masking_label_to_actions_map* labels_to_actions, Relation relation, gs_stl::gs_string* err_msg)
{
    bool policy_actions_nulls[Natts_gs_masking_policy_actions] = {false};
    bool policy_actions_replaces[Natts_gs_masking_policy_actions] = {false};
    Datum policy_actions_values[Natts_gs_masking_policy_actions] = {0};
    Datum       curtime;

    masking_label_to_actions_map::const_iterator it = labels_to_actions->find(action_item->m_label_name);
    if (it == labels_to_actions->end()) { /* no such label */
        err_msg->clear();
        (void)err_msg->append("No such label [");
        (void)err_msg->append(action_item->m_label_name);
        (void)err_msg->append("] bound to policy");
        return false;
    } else {
        /* should not happen */
        if (it->second->size() == 0) {
            err_msg->clear();
            (void)err_msg->append("No masking actions for label [");
            (void)err_msg->append(action_item->m_label_name);
            (void)err_msg->append("] bound to policy");
            return false;
        }
        /* should not happen */
        if (it->second->size() > 1) {
            err_msg->clear();
            (void)err_msg->append("Too many masking actions for label [");
            (void)err_msg->append(action_item->m_label_name);
            (void)err_msg->append("] bound to policy");
            return false;
        }
    }

    ScanKeyData skey;
    /* Find the policy row to update */
    ScanKeyInit(&skey,
                ObjectIdAttributeNumber,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(it->second->begin()->m_id));

    SysScanDesc tgscan = systable_beginscan(relation, GsMaskingPolicyActionsOidIndexId, true, NULL, 1, &skey);
    HeapTuple tup = NULL;
    if (tgscan == NULL) {
        (void)err_msg->append("could not scan index GsMaskingPolicyActionsOidIndexId.");
        return false;
    }
    tup = systable_getnext(tgscan);
    if (!tup || !HeapTupleIsValid(tup)) {
        systable_endscan(tgscan);
        (void)err_msg->append("could not find tuple for masking action ");
        (void)err_msg->append(action_item->m_type);
        return false;
    }

    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    policy_actions_replaces[Anum_gs_masking_policy_act_action_type        - 1] = true;
    policy_actions_replaces[Anum_gs_masking_policy_act_action_params      - 1] = true;
    policy_actions_replaces[Anum_gs_masking_policy_act_modify_date        - 1] = true;
    policy_actions_values[Anum_gs_masking_policy_act_action_type        - 1] = DirectFunctionCall1(namein, CStringGetDatum(action_item->m_type.c_str()));
    policy_actions_values[Anum_gs_masking_policy_act_action_params      - 1] = DirectFunctionCall1(namein, CStringGetDatum(action_item->m_params.c_str()));
    policy_actions_values[Anum_gs_masking_policy_act_modify_date        - 1] = curtime;

    HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), policy_actions_values, policy_actions_nulls, policy_actions_replaces);
    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);
    systable_endscan(tgscan);

    return true;
}

static inline bool remove_labels_from_masking_action(const PgPolicyMaskingActionStruct* action_item,
    masking_actions_set* existing_actions, Relation relation, gs_stl::gs_string* err_msg)
{
    /* Removing masking action from policy having only one action is not allowed */
    if (existing_actions->size() == 1) {
        (*err_msg) = "Removing masking action from policy with a single masking action not allowed";
        return false;
    }
    masking_actions_set::iterator i_it = existing_actions->find(*action_item);
    if (i_it != existing_actions->end()) {
        if (scan_to_delete_from_relation(i_it->m_id, relation, GsMaskingPolicyActionsOidIndexId))
            existing_actions->erase(i_it);
    } else {
        err_msg->clear();
        (void)err_msg->append("No such masking action [");
        (void)err_msg->append(action_item->m_type);
        (void)err_msg->append("] found");
        return false;
    }
    return true;
}

static void handle_drop_filter_and_condition(const char* policy_action, Oid policyOid)
{
    if (policy_action == NULL) {
        return;
    }

    if (!strcmp(policy_action, "drop_filter")) {
        drop_policy_reference<Form_gs_masking_policy_filters>(GsMaskingPolicyFiltersId, policyOid);
    }
}

static void handle_alter_policy_params(const char* policy_name, Node* policy_enabled,
                                       const char* policy_comments, Oid& policyOid)
{
    policies_set existing_policies;
    Relation policy_relation = heap_open(GsMaskingPolicyRelationId, RowExclusiveLock);
    load_existing_policies<Form_gs_masking_policy>(policy_relation, &existing_policies);

    /* first check whether such policy exists */
    GsPolicyStruct current_policy;
    current_policy.m_name = policy_name;
    policies_set::iterator it = existing_policies.find(current_policy);
    if (it == existing_policies.end()) {
        heap_close(policy_relation, RowExclusiveLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("policy %s not exists.", policy_name)));
        return;
    }

    current_policy.m_id = policyOid = it->m_id;
    current_policy.m_enabled = it->m_enabled;

    /* Update policy status (enable/disable) if needed */
    bool policy_status_changed = false;
    if (policy_enabled != NULL) {
        DefElem *defel = (DefElem *) policy_enabled;
        if (strcasecmp(defel->defname, "status") == 0) {
            bool policy_new_status = (strcasecmp(strVal(defel->arg), "enable") == 0) ? true : false;
            if (it->m_enabled != policy_new_status) {
                policy_status_changed = true;
                current_policy.m_enabled = policy_new_status;
            }
        }
    }

    /* Update policy comments if needed */
    if (((policy_comments != NULL) && (strlen(policy_comments) > 0)) || policy_status_changed) {
        current_policy.m_comments = policy_comments;
        gs_stl::gs_string err_msg;
        if (!update_policy(&current_policy, policy_relation, policy_status_changed, &err_msg)) {
            heap_close(policy_relation, RowExclusiveLock);
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s", err_msg.c_str())));
            return;
        }
    }
    heap_close(policy_relation, RowExclusiveLock);
}

/* alter masking actions */
static void alter_masking_actions(const masking_actions_set actions_to_remove, const masking_actions_set actions_to_update,
    const masking_actions_set actions_to_add, const Oid policyOid, const Datum curtime, masking_actions_set* existing_actions,
    masking_label_to_actions_map* labels_to_actions)
{
    gs_stl::gs_string err_msg;
    if ((actions_to_add.size() > 0) || (actions_to_remove.size() > 0) || (actions_to_update.size() > 0)) {
        Relation masking_actions_relation = heap_open(GsMaskingPolicyActionsId, RowExclusiveLock);
        if (masking_actions_relation == NULL) {
            return;
        }
        for (masking_actions_set::const_iterator it = actions_to_add.begin(); it != actions_to_add.end(); ++it) {
            add_labels_to_masking_action(it->m_type, it->m_params, it->m_label_name,
                masking_actions_relation, policyOid, curtime);
        }

        for (masking_actions_set::const_iterator it = actions_to_remove.begin(); it != actions_to_remove.end(); ++it) {
            if (!remove_labels_from_masking_action(&(*it), existing_actions, masking_actions_relation, &err_msg)) {
                heap_close(masking_actions_relation, RowExclusiveLock);
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s", err_msg.c_str())));
                return;
            }
        }

        for (masking_actions_set::const_iterator it = actions_to_update.begin(); it != actions_to_update.end(); ++it) {
            if (!update_labels_in_masking_action(&(*it), labels_to_actions, masking_actions_relation, &err_msg)) {
                heap_close(masking_actions_relation, RowExclusiveLock);
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s", err_msg.c_str())));
                return;
            }
        }
        heap_close(masking_actions_relation, RowExclusiveLock);
    }
    return;
}

/*
 * handle_alter_add_remove_modify_action
 * 
 * handle verify altering masking action
 * insert into currect sets for update, add, remove item
 * for each modify set, insert update catalog gs_masking_policy_actions
 */
static void handle_alter_add_remove_modify_action(List* policy_items, const char* policy_action, Oid policyOid,
    policy_labelname_set *policy_labels, masking_actions_set* existing_actions, 
    const policy_labels_map* existing_labels, masking_label_to_actions_map* labels_to_actions, Datum curtime)
{
    masking_actions_set actions_to_remove;
    masking_actions_set actions_to_update;
    masking_actions_set actions_to_add;

    ListCell   *policy_item_iter = NULL;
    int alter_opt = get_policy_command_type(policy_action);
    foreach(policy_item_iter, policy_items)
    {
        DefElem *pol_option_item = (DefElem *) lfirst(policy_item_iter);
        if (pol_option_item == NULL) {
            continue;
        }

        bool ret = true;
        gs_stl::gs_string err_msg;

        /* handle masking actions */
        MaskingActionItem action_item;
        ret = parse_masking_action(pol_option_item, &action_item, &err_msg);
        if (ret) {
            /* validate that such label exists */
            gs_stl::gs_set<gs_stl::gs_string>::const_iterator it;
            for (it = action_item.m_labels.begin(); it != action_item.m_labels.end(); ++it) {
                if (existing_labels->find(*it) == existing_labels->end()) {
                    err_msg.clear();
                    (void)err_msg.append("Trying to bind masking policy [");
                    (void)err_msg.append(action_item.m_action);
                    (void)err_msg.append("] with non-existing label [");
                    (void)err_msg.append(*it);
                    (void)err_msg.append("]");
                    ret = false;
                    break;
                } else {
                    PgPolicyMaskingActionStruct item;
                    item.m_type = action_item.m_action;
                    item.m_params = action_item.m_params;
                    item.m_label_name = *it;
                    item.m_policy_oid = policyOid;
                    switch (alter_opt) {
                        case POLICY_CMD_ADD:
                            actions_to_add.insert(item);
                            policy_labels->insert(it->c_str());
                            break;
                        case POLICY_CMD_REMOVE:
                            actions_to_remove.insert(item);
                            break;
                        case POLICY_CMD_MODIFY:
                            actions_to_update.insert(item);
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        /* If anything is added OR removed to label, label must exist unless it's for ALL */
        if (!ret) {
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s", err_msg.c_str())));
            return;
        }
    }
    alter_masking_actions(actions_to_remove, actions_to_update, actions_to_add,
        policyOid, curtime, existing_actions, labels_to_actions);
}

/*
 * alter_masking_policy
 * 
 * handle alter masking stmt
 */
void alter_masking_policy(AlterMaskingPolicyStmt *stmt)
{
    /* check that if have access to config masking policy */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    char buff[BUFFSIZE] = {0};
    char user_name[USERNAME_LEN] = {0};
    char session_ip[MAX_IP_LEN] = {0};

    get_session_ip(session_ip, MAX_IP_LEN);
    (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

    int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
        "user name: [%s], app_name: [%s], ip: [%s], ALTER MASKING POLICY [%s]",
        user_name, u_sess->attr.attr_common.application_name, session_ip, stmt->policy_name);
    securec_check_ss(rc, "\0", "\0");
    save_manage_message(buff);

    Relation    labels_relation = NULL;
    policy_labels_map existing_labels;
    masking_actions_set existing_actions;
    masking_label_to_actions_map labels_to_actions;
    policy_labelname_set policy_labels;
    Oid policyOid = 0;

    handle_alter_policy_params(stmt->policy_name, stmt->policy_enabled, stmt->policy_comments, policyOid);

    /* load existing labels for further validation */
    labels_relation = heap_open(GsPolicyLabelRelationId, RowExclusiveLock);
    load_existing_labels(labels_relation, &existing_labels);
    heap_close(labels_relation, RowExclusiveLock);
    load_existing_masking_actions(&existing_actions, &labels_to_actions, policyOid);

    Datum curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    if (stmt->policy_items != NULL) {
        handle_alter_add_remove_modify_action(stmt->policy_items, stmt->policy_action, policyOid, &policy_labels,
                                              &existing_actions, &existing_labels, &labels_to_actions, curtime);
    }

    /* if ALTER MASKING POLICY DROP FILTER, should load all labelname of policyOid */
    if (get_policy_command_type(stmt->policy_action) == POLICY_CMD_DROP_FILTER) {
        load_existing_masking_labels(&policy_labels, policyOid);
    }
    handle_alter_add_or_update_filter(stmt->policy_filters, policyOid, &policy_labels, false, true);
    handle_drop_filter_and_condition(stmt->policy_action, policyOid);
    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    reload_masking_policies();
}


/*
 * drop_masking_policy
 * 
 * handle drop masking policy stmt
 */
void drop_masking_policy(DropMaskingPolicyStmt *stmt)
{
    /* check that if have access to config masking policy */
    if (!is_policy_enabled()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("Permission denied.")));
        return;
    }

    ListCell* policy_obj = NULL;
    /* save Mng logs */
    foreach(policy_obj, stmt->policy_names) {
        const char* polname = (const char *)(((Value*)lfirst(policy_obj))->val.str);
        char buff[BUFFSIZE] = {0};
        char user_name[USERNAME_LEN] = {0};
        char session_ip[MAX_IP_LEN] = {0};

        get_session_ip(session_ip, MAX_IP_LEN);
        (void)GetRoleName(GetCurrentUserId(), user_name, sizeof(user_name));

        int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
            "user name: [%s], app_name: [%s], ip: [%s], DROP MASKING POLICY [%s]",
            user_name, u_sess->attr.attr_common.application_name, session_ip, polname);
        securec_check_ss(rc, "\0", "\0");
        save_manage_message(buff);
    }

    Relation policy_relation = heap_open(GsMaskingPolicyRelationId, RowExclusiveLock);
    policies_set existing_policies;
    load_existing_policies<Form_gs_masking_policy>(policy_relation, &existing_policies);
    heap_close(policy_relation, RowExclusiveLock);

    foreach(policy_obj, stmt->policy_names) {
        const char* polname = (const char *)(((Value*)lfirst(policy_obj))->val.str);
        GsPolicyStruct cur_policy;
        cur_policy.m_name = polname;
        policies_set::iterator it = existing_policies.find(cur_policy);
        if (it == existing_policies.end()) {
            if (!stmt->if_exists) {
                send_manage_message(AUDIT_FAILED);
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("policy %s not exists.", polname)));
                return;
            }
            ereport(NOTICE, (errmsg("policy %s not exists, skipping", polname)));
        }
        gs_stl::gs_set<long long> ids;
        drop_policy_by_name<Form_gs_masking_policy>(GsMaskingPolicyRelationId, polname, &ids);

        for (long long _id : ids) {
            /* for masking  filters */
            drop_policy_reference<Form_gs_masking_policy_filters>(GsMaskingPolicyFiltersId, _id);
            /* for masking  actions */
            drop_policy_reference<Form_gs_masking_policy_actions>(GsMaskingPolicyActionsId, _id);
        }
    }
    CommandCounterIncrement();
    send_manage_message(AUDIT_OK);

    reload_masking_policies();
}

static List *relid_get_policy_label_namelist(Oid relid, const char *column = NULL)
{
    Relation label_rel = NULL;
    List *label_list = NIL;
    TableScanDesc scan = NULL;
    HeapTuple tup = NULL;
    Form_gs_policy_label label_data = NULL;

    label_rel = heap_open(GsPolicyLabelRelationId, AccessShareLock);
    scan = tableam_scan_begin(label_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        label_data = (Form_gs_policy_label)GETSTRUCT(tup);
        if (label_data->fqdnid == relid && (column == NULL || strcasecmp(column, label_data->relcolumn.data) == 0)) {
            char *label_name = pstrdup(label_data->labelname.data);
            label_list = lappend(label_list, label_name);
        }
    }
    tableam_scan_end(scan);
    heap_close(label_rel, AccessShareLock);

    return label_list;
}

static List *get_masking_policy_oidlist(bool check_enabled)
{
    List *oid_list = NIL;
    Relation policy_rel = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tup = NULL;
    Form_gs_masking_policy policy_data = NULL;

    policy_rel = heap_open(GsMaskingPolicyRelationId, AccessShareLock);
    scan = tableam_scan_begin(policy_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple)tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        policy_data = (Form_gs_masking_policy)GETSTRUCT(tup);
        if (check_enabled && !policy_data->polenabled) {
            continue;
        }
        oid_list = lappend_oid(oid_list, HeapTupleGetOid(tup));
    }
    tableam_scan_end(scan);
    heap_close(policy_rel, AccessShareLock);

    return oid_list;
}

bool is_masked_relation(Oid relid, const char *column)
{
    bool is_masked = false;
    List *labels = NIL;
    ListCell *lc = NULL;
    Relation action_rel = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tup = NULL;
    Form_gs_masking_policy_actions action_data = NULL;

    labels = relid_get_policy_label_namelist(relid, column);
    if (labels == NIL) {
        return false;
    }
    action_rel = heap_open(GsMaskingPolicyActionsId, AccessShareLock);
    scan = tableam_scan_begin(action_rel, SnapshotNow, 0, NULL);

    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL && !is_masked) {
        action_data = (Form_gs_masking_policy_actions)GETSTRUCT(tup);
        foreach (lc, labels) {
            const char *labelname = (const char *)lfirst(lc);
            if (strcmp(labelname, action_data->actlabelname.data) == 0) {
                is_masked = true;
                break;
            }
        }
    }
    tableam_scan_end(scan);
    heap_close(action_rel, AccessShareLock);
    list_free_deep(labels);

    return is_masked;
}

bool is_masked_relation_enabled(Oid relid)
{
    bool is_match = false;
    List *label_list = NIL;
    List *polid_list = NIL;
    ListCell *lc = NULL;
    Relation action_rel = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Form_gs_masking_policy_actions action_data = NULL;

    polid_list = get_masking_policy_oidlist(true);
    label_list = relid_get_policy_label_namelist(relid);
    /* No masking policies or no relative resource labels */
    if (polid_list == NIL || label_list == NIL) {
        list_free(polid_list);
        list_free(label_list);
        return false;
    }
    action_rel = heap_open(GsMaskingPolicyActionsId, AccessShareLock);
    scan = tableam_scan_begin(action_rel, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL && !is_match) {
        action_data = (Form_gs_masking_policy_actions)GETSTRUCT(tuple);
        foreach (lc, label_list) {
            const char *labelname = (const char *)lfirst(lc);
            if (strcmp(labelname, action_data->actlabelname.data) == 0 &&
                list_member_oid(polid_list, action_data->policyoid)) {
                is_match = true;
                break;
            }
        }
    }
    tableam_scan_end(scan);
    heap_close(action_rel, AccessShareLock);
    list_free_deep(label_list);
    list_free(polid_list);

    return is_match;
}

/* separate namespace and function name when user input to create masking policy */
void parse_function_name(const char* func_name, char** parsed_funcname, Oid& schemaid)
{
    errno_t rc = EOK;
    const char* sch = strchr(func_name, '.');
    *parsed_funcname = (char *)palloc(strlen(func_name) + 1);
    if (sch) { /* function name includes schema */
        int name_pos = (sch - func_name);
        rc = strncpy_s(*parsed_funcname, strlen(func_name) + 1, func_name + name_pos + 1, strlen(func_name) - name_pos);
        securec_check(rc, "\0", "\0");

        char* schema_name = NULL;
        schema_name = (char *)palloc(name_pos + 1);
        rc = strncpy_s(schema_name, name_pos + 1, func_name, name_pos);
        securec_check(rc, "\0", "\0");
        schemaid = SchemaNameGetSchemaOid(schema_name, true);
        pfree(schema_name);
    } else {
        rc = strncpy_s(*parsed_funcname, strlen(func_name) + 1, func_name, strlen(func_name));
        securec_check(rc, "\0", "\0");
        schemaid = PG_CATALOG_NAMESPACE;
    }
}

bool IsMaskingFunctionOid(Oid funcid)
{
    if (!OidIsValid(funcid)) {
        return false;
    }
    Oid nspoid = get_func_namespace(funcid);
    char *funcname = get_func_name(funcid);
    if (nspoid == PG_CATALOG_NAMESPACE) {
        for (int i = 0; g_maskFunctions[i] != NULL; ++i) {
            if (strcmp(g_maskFunctions[i], funcname) == 0) {
                return true;
            }
        }
    }
    return IsUDF(funcname, nspoid); 
}

bool IsUDF(const char *funcname, Oid nspoid)
{
    Relation rel = NULL;
    rel = heap_open(GsMaskingPolicyActionsId, AccessShareLock);
    
    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup    = NULL;
    Form_gs_masking_policy_actions rel_data = NULL;
    bool is_found = false;
    char *parsed_funcname = NULL;
    Oid nsp_oid = InvalidOid;
    /* verify whether this function is used to be a masking function by searching gs_masking_policy_actions */
    while ((rtup = heap_getnext(scan, ForwardScanDirection)) && !is_found) {
        rel_data = (Form_gs_masking_policy_actions)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        parse_function_name(rel_data->actiontype.data, &parsed_funcname, nsp_oid);
        if (strcasecmp(parsed_funcname, funcname) == 0 && nspoid == nsp_oid) {
            is_found = true;
        }
        pfree(parsed_funcname);
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    return is_found;
}
