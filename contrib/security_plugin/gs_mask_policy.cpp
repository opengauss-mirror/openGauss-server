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


#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/acl.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "catalog/gs_policy_label.h"
#include "catalog/gs_masking_policy.h"
#include "catalog/gs_masking_policy_actions.h"
#include "catalog/gs_masking_policy_filters.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "commands/user.h"
#include "storage/lock/lock.h"
#include "storage/spin.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "commands/tablespace.h"
#include "commands/dbcommands.h"
#include "gs_policy_labels.h"
#include "gs_policy_plugin.h"
#include "gs_mask_policy.h"
#include "gs_policy/gs_policy_masking.h"
#include "parser/parse_node.h"
#include "pgaudit.h"
#include "tcop/tcopprot.h"

#define CMP_STR(a, b)        \
int res = strcasecmp(a, b);  \
if (res < 0)return true;     \
if (res > 0)return false;

#define BUFFSIZE 2048

static THR_LOCAL gs_policy_set *loaded_policies = NULL;
static THR_LOCAL bool masking_policy_reloaded = false;
static THR_LOCAL pg_masking_action_map *loaded_action = NULL;
static THR_LOCAL gs_policy_filter_map *loaded_masking_filters = NULL;
static THR_LOCAL global_roles_in_use *masking_roles_in_use = NULL;

using StrMap = gs_stl::gs_map<gs_stl::gs_string, masking_result>;
using PrepareStmtMap = gs_stl::gs_map<gs_stl::gs_string, bool>;

static pg_atomic_uint64 mask_global_version = 1;
static pg_atomic_uint64 action_global_version = 1;
static pg_atomic_uint64 mask_filter_global_version = 1;

static THR_LOCAL pg_atomic_uint64 mask_local_version = 0;
static THR_LOCAL pg_atomic_uint64 action_local_version = 0;
static THR_LOCAL pg_atomic_uint64 filter_local_version = 0;
static THR_LOCAL PrepareStmtMap *prepared_stamts_state = NULL;

/* masking policy is changed, need to recompile all prepare stmts */
void set_reload_for_all_stmts()
{
    if (prepared_stamts_state == NULL) {
        return;
    }
    PrepareStmtMap::iterator it = prepared_stamts_state->begin();
    PrepareStmtMap::iterator eit = prepared_stamts_state->end();
    for (; it != eit; ++it) {
        *(it->second) = true;
    }
}

/* check and reset state of prepare stmt (loaded) */
bool prepare_stmt_is_reload(const char* name)
{
    if (prepared_stamts_state == NULL) {
        return false;
    }

    bool res = false;
    PrepareStmtMap::iterator it = prepared_stamts_state->find(name);
    if (it != prepared_stamts_state->end()) {
        res = it->second;
        *(it->second) = false;
    }
    return res;
}

/* delete prepare stmt state */
void unprepare_stmt(const char* name)
{
    if (prepared_stamts_state == NULL) {
        return;
    }

    if (!strcasecmp(name, "all")) {
        prepared_stamts_state->clear();
    } else {
        prepared_stamts_state->erase(name);
    }
}

/* initilize prepare stmt state */
void prepare_stmt(const char* name)
{
    if (prepared_stamts_state == NULL) {
        prepared_stamts_state = new PrepareStmtMap();
    }
    (*prepared_stamts_state)[name] = false;
}

/* we allways get the func name all lowercase from parser so no need to change the compare function. */
typedef struct MaskingFuncsInfo
{
    const char*     func;
    MaskBehaviour   type;
} MaskingFuncsInfo;

static MaskingFuncsInfo masking_funcs_infos[] =
{
    { "creditcardmasking", M_CREDIT_CARD },
    { "maskall", M_MASKALL },
    { "basicemailmasking", M_BASICEMAIL },
    { "fullemailmasking", M_FULLEMAIL },
    { "alldigitsmasking", M_ALLDIGITS },
    { "shufflemasking", M_SHUFFLE },
    { "randommasking", M_RANDOM },
    { "regexpmasking", M_REGEXP },
    { NULL, M_UNKNOWN }
};

/* verify if language is supported */
static bool is_valid_language(Oid lang_oid)
{
    HeapTuple lang_tuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(lang_oid));
    if (!HeapTupleIsValid(lang_tuple)) {
        return false;
    }
    Form_pg_language pg_language_tuple = (Form_pg_language)GETSTRUCT(lang_tuple);
    const char* lang_name = NameStr(pg_language_tuple->lanname);
    if (lang_name == NULL) {
        ReleaseSysCache(lang_tuple);
        return false;
    }
    if (strcasecmp(lang_name, "sql") == 0 || strcasecmp(lang_name, "plpgsql") == 0) {
        /* supported language */
        ReleaseSysCache(lang_tuple);
        return true;
    }
    ReleaseSysCache(lang_tuple);
    return false;
}

/* validate function.
 *
 * function exists.
 * function's language.
 * validate function's params.
 */
static bool is_valid_for_masking(const char* func_name, Oid funcnsp, int& funcid,
    const char* func_parameters, bool* invalid_params)
{
    CatCList   *catlist = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    int cacheId = (t_thrd.proc->workingVersionNum < 92470) ? PROCNAMEARGSNSP : PROCALLARGS;
    catlist = SearchSysCacheList1(cacheId, CStringGetDatum(func_name));
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(func_name));
#endif
    bool is_found = false;
    if (catlist != NULL) {
        func_params f_params;
        if (func_parameters != NULL && strlen(func_parameters) > 0) {
            parse_params(func_parameters, &f_params);
        }
        bool is_valid = true;
        /* try to find function on pg_proc */
        for (int i = 0; i < catlist->n_members && is_valid; ++i) {
            HeapTuple	proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
            Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
            /* verify namespace */
            if (procform->pronamespace != funcnsp) {
                continue;
            }
            /* verify owner is policy admin */
            if (procform->pronamespace == funcnsp && isPolicyadmin(procform->proowner)) {
                is_found = true;
            } else {
                continue;
            }
            /* verify function language */
            is_valid = is_valid_language(procform->prolang);
            bool isNull = false;
            Datum tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prosrc, &isNull);
            const char* procdescr = TextDatumGetCString(tmp);
            if (is_valid && procdescr && strlen(procdescr) > 0) {
                List* raw_parsetree_list = pg_parse_query(procdescr);
                ListCell* lc = NULL;
                ParseState *pstate = make_parsestate(NULL);
                pstate->p_sourcetext = procdescr;
                foreach(lc, raw_parsetree_list) {
                    Node *parsetree = (Node *)lfirst(lc);
                    is_valid &= IsA(parsetree, DoStmt);
                }
                free_parsestate(pstate);
                pfree_ext(procdescr);
            }
            if (is_valid) {
                func_types types;
                int default_params = 0;
                get_function_parameters(proctup, &types, &default_params);
                /* verify parameter size and type */
                if ((f_params.size() + default_params + 1) >= types.size() && verify_proc_params(&f_params, &types)) {
                    funcid = HeapTupleGetOid(proctup);
                    ReleaseSysCacheList(catlist);
                    return true;
                } else if (invalid_params) {
                    *invalid_params = true;
                }
            }
        }
        if (!is_found) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmodule(MOD_UDF),
                errmsg("Don't find function which can be used by masking"),
                    errdetail("function should be created by poladmin or function is not found")));
        }
        ReleaseSysCacheList(catlist);
        return is_valid && (funcid > 0);
    }
    return false;
}

static bool get_function_behavious(const char *func_name, Oid funcnsp, int *masking_behavious,
    const char* func_parameters, bool* invalid_params = NULL, bool is_audit = false)
{
    (*masking_behavious) = M_UNKNOWN;
    /* verify predefine function */
    for (int i = 0; funcnsp == PG_CATALOG_NAMESPACE && masking_funcs_infos[i].func != NULL; ++i) {
        if (strcmp(masking_funcs_infos[i].func, func_name) == 0) {
            (*masking_behavious) = masking_funcs_infos[i].type;
            return true;
        }
    }
    /* it looks like UDF, verify it */
    return is_audit ? false : is_valid_for_masking(func_name, funcnsp, *masking_behavious,
        func_parameters, invalid_params);
}

/* is_audit is used to identify whether the call is from audit */
bool validate_function_name(const char *func_name, const char* func_parameters, bool* invalid_params, bool is_audit)
{
    bool ret = false;
    int masking_behavious_dummy = M_UNKNOWN;
    char *parsed_funcname = NULL;
    Oid funcnsp = InvalidOid;
    parse_function_name(func_name, &parsed_funcname, funcnsp);
    ret =  get_function_behavious(parsed_funcname, funcnsp, &masking_behavious_dummy, 
                                  func_parameters, invalid_params, is_audit);
    pfree(parsed_funcname);
    return ret;
}

bool load_masking_policies(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }

    if (!reload) {
        pg_atomic_add_fetch_u64(&mask_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&mask_global_version, (uint64*)&mask_local_version, mask_global_version)) {
        /* Latest masking policy, changes nothing */
        return false;
    }

    Relation rel = NULL;
    rel = heap_open(GsMaskingPolicyRelationId, AccessShareLock);

    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup    = NULL;
    Form_gs_masking_policy rel_data = NULL;

    gs_policy_set* tmp_policies = new gs_policy_set;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_masking_policy)GETSTRUCT(rtup);
        if (rel_data == NULL || !rel_data->polenabled) {
            continue;
        }
        gs_base_policy item;
        item.m_id = HeapTupleGetOid(rtup);
        item.m_name = rel_data->polname.data;
        item.m_modify_date = rel_data->modifydate;
        item.m_enabled = true;
        tmp_policies->insert(item);
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    reset_masking_policy_filters(); /* must reload filters */
    /* swap masking policy container */
    if (loaded_policies != NULL) {
        gs_policy_set *curpol = loaded_policies;
        loaded_policies = tmp_policies;
        delete curpol;
        curpol = NULL;
    } else {
        loaded_policies = tmp_policies;
    }
    set_reload_for_all_stmts();

    return true;
}

gs_policy_set* get_masking_policies(const char *dbname)
{
    load_masking_policies(true);
    return loaded_policies;
}

void parse_params(const gs_stl::gs_string& arg, gs_stl::gs_vector<gs_stl::gs_string> *params)
{
    params->clear();
    if (arg.empty()) {
        return;
    }
    size_t pos = 0;
    size_t start = 0;
    while ((pos = arg.find(',', start)) != gs_stl::gs_string::npos) {
        gs_stl::gs_string tmp(arg.c_str() + start, pos - start);
        params->push_back(tmp.c_str());
        start = ++pos;
    }
    gs_stl::gs_string tmp(arg.c_str() + start, arg.size() - start);
    params->push_back(tmp.c_str());
}

size_t PolicyAccessHash::operator()(const GsMaskingAction& k) const
{
    size_t seed = 0;
    seed = policy_hash_combine(seed, hash_uint32(k.m_func_id));
    seed = policy_hash_combine(seed, policy_str_hash(k.m_label_name));
    return seed;
}

bool EqualToPolicyAccess::operator()(const GsMaskingAction& l, const GsMaskingAction& r) const
{
    return l.m_func_id == r.m_func_id && !strcasecmp(l.m_label_name.c_str(), r.m_label_name.c_str());
}

bool load_masking_actions(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }

    if (!reload) {
        pg_atomic_add_fetch_u64(&action_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&action_global_version, (uint64*)&action_local_version, action_global_version)) {
        /* Latest masking action, changes nothing */
        return false;
    }

    Relation rel = NULL;
    rel = heap_open(GsMaskingPolicyActionsId, AccessShareLock);

    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup    = NULL;
    Form_gs_masking_policy_actions rel_data = NULL;

    pg_masking_action_map* tmp_actions = new pg_masking_action_map;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_masking_policy_actions)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        GsMaskingAction item;
        char *parsed_funcname = NULL;
        Oid funcnsp = InvalidOid;
        parse_function_name(rel_data->actiontype.data, &parsed_funcname, funcnsp);
        get_function_behavious(parsed_funcname, funcnsp, &item.m_func_id, rel_data->actparams.data);
        pfree(parsed_funcname);
        item.m_label_name = rel_data->actlabelname.data;
        item.m_modify_date = rel_data->actmodifydate;
        item.m_policy_id = rel_data->policyoid;
        parse_params(rel_data->actparams.data, &item.m_params);
        (*tmp_actions)[item.m_policy_id].insert(item);
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (loaded_action != NULL) {
        pg_masking_action_map *curact = loaded_action;
        loaded_action = tmp_actions;
        delete curact;
        curact = NULL;
    } else {
        loaded_action = tmp_actions;
    }
    masking_policy_reloaded = true;
    set_reload_for_all_stmts();

    return true;
}

pg_masking_action_map* get_masking_actions()
{
    load_masking_actions(true);
    return loaded_action;
}

static inline int parse_operator_type(const char* type)
{
    if (!strcasecmp(type, "string"))
        return T_String;
    if (!strcasecmp(type, "float"))
        return T_Float;
    return T_Integer;
}

void reset_masking_policy_filters()
{
    pg_atomic_exchange_u64(&filter_local_version, 0);
}

bool load_masking_policy_filters(bool reload)
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }

    if (!reload) {
        pg_atomic_add_fetch_u64(&mask_filter_global_version, 1);
    }
    if (pg_atomic_compare_exchange_u64(&mask_filter_global_version, (uint64*)&filter_local_version,
                                       mask_filter_global_version)) {
        /* Latest masking filter, changes nothing */
        return false;
    }

    Relation rel = NULL;
    rel = heap_open(GsMaskingPolicyFiltersId, AccessShareLock);

    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup    = NULL;
    Form_gs_masking_policy_filters rel_data = NULL;
    gs_policy_filter_map* tmp_filters = new gs_policy_filter_map;
    global_roles_in_use* masking_roles_in_use_tmp = new global_roles_in_use;
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_masking_policy_filters)GETSTRUCT(rtup);
        if (rel_data == NULL)
            continue;

        bool    isNull = true;
        Datum logical_operator_datum = heap_getattr(rtup, Anum_gs_masking_policy_fltr_logical_operator,
                                                    RelationGetDescr(rel), &isNull);
        const char* logical_operator = "";
        if (!isNull)
            logical_operator = TextDatumGetCString(logical_operator_datum);
        PolicyLogicalTree ltree;
        ltree.parse_logical_expression(logical_operator);
        ltree.get_roles(masking_roles_in_use_tmp);
        GsPolicyFilter item(ltree, rel_data->policyoid, rel_data->modifydate);
        set_filter(&item, tmp_filters);
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    /* add policies without filter */
    gs_policy_set* all_policies = get_masking_policies();
    if (all_policies) {
        gs_policy_set::const_iterator it = all_policies->begin();
        gs_policy_set::const_iterator eit = all_policies->end();
        for (; it != eit; ++it) {
            (*tmp_filters)[it->m_id];
        }
    }

    if (loaded_masking_filters != NULL) {
        gs_policy_filter_map *curfilter = loaded_masking_filters;
        loaded_masking_filters = tmp_filters;
        delete curfilter;
        curfilter = NULL;
    } else {
        loaded_masking_filters = tmp_filters;
    }

    if (masking_roles_in_use != NULL) {
        global_roles_in_use *cur = masking_roles_in_use;
        masking_roles_in_use = masking_roles_in_use_tmp;
        delete cur;
        cur = NULL;
    } else {
        masking_roles_in_use = masking_roles_in_use_tmp;
    }
    set_reload_for_all_stmts();

    return true;
}

gs_policy_filter_map* get_masking_filters()
{
    load_masking_policy_filters(true);
    return loaded_masking_filters;
}

bool reload_masking_policy()
{
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }

    load_masking_actions(true);
    load_masking_policies(true);
    /* load filters must be last */
    load_masking_policy_filters(true);
    if (masking_policy_reloaded) {
        set_reload_for_all_stmts();
        masking_policy_reloaded = false;
    }
    return masking_policy_reloaded;
}

bool check_masking_policy_filter(const FilterData *arg, policy_set *policy_ids)
{
    return check_policy_filter(arg, policy_ids, get_masking_policies(), get_masking_filters());
}

static bool table_base_policy(const typed_labels *labels, long long func_id,
    const PolicyLabelItem *tbl_name, int *masking_behavious, int obj_type)
{
    typed_labels::const_iterator tit = labels->find(obj_type);
    if (tit != labels->end()) {
        const gs_policy_label_set& found_labels = *(tit->second);
        if (found_labels.find(*tbl_name) != found_labels.end()) {
            (*masking_behavious) = func_id;
            return true;
        }
    }
    return false;
}

static bool verify_policy_object(const typed_labels *labels, const GsMaskingAction *arg,
    const PolicyLabelItem *col_name, int *masking_behavious, 
    gs_stl::gs_vector<gs_stl::gs_string> *params, int obj_type, long long polid)
{
    if (table_base_policy(labels, arg->m_func_id, col_name, masking_behavious, O_COLUMN)) {
        if (arg->m_params.size()) {
            (*params) = arg->m_params;
        }
        return true;
    }
    PolicyLabelItem tbl_name(col_name->m_schema, col_name->m_object, obj_type);
    if (table_base_policy(labels, arg->m_func_id, &tbl_name, masking_behavious, obj_type)) {
        if (arg->m_params.size()) {
            (*params) = arg->m_params;
        }
        return true;
    }
    return false;
}

static bool verify_policy(const policy_set *policy_ids, long long *polid,
    gs_stl::gs_vector<gs_stl::gs_string> *params,
    const PolicyLabelItem *col_name, const PolicyLabelItem *view_name, int *masking_behavious)
{
    pg_masking_action_map* tmp_action = get_masking_actions();
    if (tmp_action == NULL || tmp_action->empty()) {
        return false;
    }
    loaded_labels* tmp_labels = get_policy_labels();
    if (tmp_labels == NULL || tmp_labels->empty()) {
        return false;
    }
    /* find by table */
    PolicyLabelItem tbl_name(col_name->m_schema, col_name->m_object, O_TABLE);
    policy_set::const_iterator pol_it = policy_ids->begin();
    policy_set::const_iterator pol_eit = policy_ids->end();
    for (; pol_it != pol_eit; ++pol_it) {
        pg_masking_action_map::const_iterator acc_it = tmp_action->find(pol_it->m_id);
        if (acc_it == tmp_action->end()) {
            continue;
        }
        const pg_masking_action_set& actions = *(acc_it.second);
        pg_masking_action_set::const_iterator ait = actions.begin();
        pg_masking_action_set::const_iterator aeit = actions.end();
        for (; ait != aeit; ++ait) {
            /* check label */
            loaded_labels::const_iterator lit = tmp_labels->find(ait->m_label_name);
            if (lit != tmp_labels->end()) {
                /* find by view */
                if (view_name->m_object) {
                    if (verify_policy_object(lit->second, &(*ait), view_name, masking_behavious,
                                             params, O_VIEW, pol_it->m_id)) {
                        (*polid) = pol_it->m_id;
                        return true;
                    }
                }
                if (verify_policy_object(lit->second, &(*ait), col_name, masking_behavious, params,
                                         O_TABLE, pol_it->m_id)) {
                    (*polid) = pol_it->m_id;
                    return true;
                }
            }
        }
    }
    return false;
}

bool check_masking_policy_action(const policy_set *policy_ids, const PolicyLabelItem *col_name,
    const PolicyLabelItem *view_name, int *masking_behavious, long long *polid,
    gs_stl::gs_vector<gs_stl::gs_string> *params)
{
    return verify_policy(policy_ids, polid, params, col_name, view_name, masking_behavious);
}

static void get_behaviour(int mtype, char* buffer, size_t buffer_size, int *ret_size)
{
    errno_t rc = EOK;
    switch (mtype) {
        case M_CREDIT_CARD:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [CREDIT CARD]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
        case M_MASKALL:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [MASK ALL]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
        case M_BASICEMAIL:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [BASIC EMAIL]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
        case M_FULLEMAIL:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [FULL EMAIL]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
        case M_ALLDIGITS:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [ALL DIGITS]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
        case M_SHUFFLE:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [SHUFFLE]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
        case M_RANDOM:
            rc = snprintf_s(buffer + (*ret_size), buffer_size - (*ret_size),
                            buffer_size - (*ret_size) - 1, ", behavior: [RANDOM]");
            securec_check_ss(rc, "\0", "\0");
            (*ret_size) += rc;
            break;
    }
}

void flush_masking_result(const masking_result *result)
{
    if (result->empty()) {
        return;
    }
    char user_name[USERNAME_LEN];
    errno_t rc = EOK;
    masking_result::const_iterator pit = result->begin();
    masking_result::const_iterator peit = result->end();
    for (; pit != peit; ++pit) {
        char buff[BUFFSIZE] = {0};
        char session_ip[MAX_IP_LEN] = {0};

        get_session_ip(session_ip, MAX_IP_LEN);
        int printed_size = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, 
            "MASKING EVENT:user name: [%s], app_name: [%s], client_ip: [%s], policy id: [%lld]",
            GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip, (*pit->first));
        securec_check_ss(printed_size, "\0", "\0");
        masking_policy_result::const_iterator bit = pit->second->begin();
        masking_policy_result::const_iterator beit = pit->second->end();

        int tmp_size = printed_size;
        for (; bit != beit; ++bit) {
            get_behaviour(*(bit->first), buff, sizeof(buff), &tmp_size);

            if (bit->second->size()) {
                rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1, ", columns: [");
                securec_check_ss(rc, "\0", "\0");
                tmp_size += rc;
                masking_column_set::iterator cit = bit->second->begin();
                masking_column_set::iterator ceit = bit->second->end();
                for (int i = 0; cit != ceit; ++cit, ++i) {
                    rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1,
                                    "%s%s", (i > 0) ? ", " : "", cit->c_str());
                    securec_check_ss(rc, "\0", "\0");
                    tmp_size += rc;
                }
                rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1, "]");
                securec_check_ss(rc, "\0", "\0");
                tmp_size += rc;
            }
        }
        gs_audit_issue_syslog_message("PGMASKING", buff, MASKING_POLICY_EVENT, AUDIT_OK);
    }
}

bool is_masking_role_in_use(Oid roleid)
{
    reload_masking_policy();
    global_roles_in_use *tmp = masking_roles_in_use;
    return (tmp != NULL && tmp->find(roleid) != tmp->end());
}

bool is_masking_has_object(bool column_type_is_changed, const gs_stl::gs_string labelname)
{
    if (!column_type_is_changed) {
        return true;
    }
    pg_masking_action_map* mask_action = get_masking_actions();
    if (!mask_action) {
        return true;
    }
    pg_masking_action_map::const_iterator ait = mask_action->begin();
    pg_masking_action_map::const_iterator aeit = mask_action->end();
    for (; ait != aeit; ++ait) {
        pg_masking_action_set::const_iterator mit = ait.second->begin();
        pg_masking_action_set::const_iterator meit = ait.second->end();
        for (; mit != meit; ++mit) {
            if (!strcasecmp(labelname.c_str(), mit->m_label_name.c_str())) {
                return true;
            }
        }
    }
    return false;
}

bool check_masking_policy_actions_for_label(const policy_labels_map *labels_to_drop)
{
    Relation rel = NULL;
    rel = heap_open(GsMaskingPolicyActionsId, RowExclusiveLock);

    TableScanDesc scan   = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple   rtup    = NULL;
    Form_gs_masking_policy_actions rel_data = NULL;
    bool is_found = false;
    while ((rtup = heap_getnext(scan, ForwardScanDirection)) && !is_found) {
        rel_data = (Form_gs_masking_policy_actions)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        is_found = (labels_to_drop->find(rel_data->actlabelname.data) != labels_to_drop->end());
    }

    heap_endscan(scan);
    heap_close(rel, RowExclusiveLock);

    return is_found;
}

/*
 * validate_masking_function_name
 *
 * validate function is part of predefined masking function
 * dont allow drop/replace/alter/rename/owner to/set schema
 */
void validate_masking_function_name(const List* full_funcname, bool is_audit)
{
    /* Ignore checking when upgrade */
    if (u_sess->attr.attr_common.IsInplaceUpgrade) {
        return;
    }
    char* funcname = NULL;
    char* nspname = NULL;
    errno_t rc = EOK;
    DeconstructQualifiedName(full_funcname, &nspname, &funcname);

    if ((nspname == NULL || (nspname != NULL && !strcmp(nspname, "pg_catalog"))) &&
        validate_function_name(funcname, "", NULL, is_audit)) {
        char buff[BUFFSIZE] = {0};
        rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
                        "function: %s is part of predefined masking functions.", funcname);
        securec_check_ss(rc, "\0", "\0");
        gs_audit_issue_syslog_message("PGAUDIT", buff, AUDIT_POLICY_EVENT, AUDIT_FAILED);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\"", buff)));
        return;
    }
}

bool get_masking_policy_name_by_oid(Oid polid, gs_stl::gs_string *polname)
{
    gs_policy_set *masking_policies = get_masking_policies();
    if (masking_policies == NULL || masking_policies->empty()) {
        return false;
    }
    gs_base_policy target_policy(polid);
    gs_policy_set::const_iterator polit = masking_policies->find(target_policy);
    if (polit != masking_policies->end()) {
        *polname = polit->m_name;
        return true;
    }
    return false;
}

int gs_maksing_action_cmp(const void *key1, const void *key2)
{
    GsMaskingAction *l = (GsMaskingAction *)key1;
    GsMaskingAction *r = (GsMaskingAction *)key2;
    if (l->m_func_id < r->m_func_id) {
        return -1;
    }
    if (r->m_func_id < l->m_func_id) {
        return 1;
    }
    return strcasecmp(l->m_label_name.c_str(), r->m_label_name.c_str());
}

void clear_thread_local_masking()
{
    if (loaded_policies != NULL) {
        delete loaded_policies;
        loaded_policies = NULL;
    }

    if (loaded_action != NULL) {
        delete loaded_action;
        loaded_action = NULL;
    }

    if (loaded_masking_filters != NULL) {
        delete loaded_masking_filters;
        loaded_masking_filters = NULL;
    }

    if (masking_roles_in_use != NULL) {
        delete masking_roles_in_use;
        masking_roles_in_use = NULL;
    }

    if (prepared_stamts_state != NULL) {
        delete prepared_stamts_state;
        prepared_stamts_state = NULL;
    }
}
