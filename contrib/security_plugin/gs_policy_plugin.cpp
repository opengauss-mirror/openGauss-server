/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * gs_policy_plugin.cpp
 * init hook pointers of policy plugin for gaussdb kernel
 *
 * IDENTIFICATION
 * contrib/security_plugin/gs_policy_plugin.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "gs_policy_plugin.h"
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <syslog.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unordered_map>
#include <unordered_set>

#include "access/sysattr.h"
#include "access/xact.h"
#include "bulkload/utils.h"
#include "catalog/catalog.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/namespace.h"
#include "catalog/gs_global_config.h"
#include "commands/user.h"
#include "commands/tablespace.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "gs_policy_labels.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "gs_policy/policy_common.h"
#include "parser/parse_utilcmd.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "postgres.h"

#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "commands/user.h"
#include "commands/tablespace.h"
#include <stdlib.h>
#include <syslog.h>
#include <unordered_map>
#include <unordered_set>
#include "gs_audit_policy.h"
#include "gs_policy/policy_common.h"
#include "parser/parse_utilcmd.h"
#include "parser/parsetree.h"
#include "access_audit.h"
#include "privileges_audit.h"
#include "gs_mask_policy.h"
#include "masking.h"
#include "gs_policy_labels.h"
#include "pgaudit.h"
#include "bulkload/utils.h"
#include "iprange/iprange.h"
#include "parser/parse_clause.h"
#include "gs_policy/gs_string.h"
#include "gs_policy/gs_policy_masking.h"

PG_MODULE_MAGIC;

extern "C" void _PG_init(void);
extern "C" void _PG_fini(void);

#define POLICY_STR_BUFF_LEN 512
#define POLICY_TMP_BUFF_LEN 256

/*
 * Hook functions for kernel SQL process, it's safe to use local var
 * as the type of kernel hooks is thread local, keeping the same with it 
 * will not make any impact on any other process
 */
static THR_LOCAL post_parse_analyze_hook_type next_post_parse_analyze_hook = NULL;
static THR_LOCAL ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static THR_LOCAL ExecutorStart_hook_type next_ExecutorStart_hook = NULL;

/*
 * Hook for access control
 */
AccessControl_SecurityAuditObject_hook_type accesscontrol_securityAuditObject_hook = NULL;
Check_acl_privilige_hook_type check_acl_privilige_hook = NULL;
CheckSecurityAccess_hook_type CheckSecurityAccess_hook = NULL;
Reset_security_policies_hook_type reset_security_policies_hook = NULL;
Reset_security_filters_hook_type reset_security_filters_hook = NULL;
Reset_security_access_hook_type reset_security_access_hook = NULL;
Reset_security_privilige_hook_type reset_security_privilige_hook = NULL;
CheckSecurityPolicyFilter_hook_type checkSecurityPolicyFilter_hook = NULL;
Security_isRoleInUse_hook_type security_isRoleInUse_hook = NULL;
Security_Check_acl_privilige_hook_type security_Check_acl_privilige_hook = NULL;
Reload_security_policy_hook_type reload_security_policy_hook = NULL;

IsRoleInUse_hook_type isRoleInUse_hook = NULL;

#define SECURITY_CHECK_ACL_PRIV(type) \
    ((security_Check_acl_privilige_hook != NULL) ? security_Check_acl_privilige_hook(type) : false)

using MngEventsVector = gs_stl::gs_vector<gs_stl::gs_string>;
static THR_LOCAL gs_policy_label_set *result_set_functions = NULL;
static THR_LOCAL bool query_inside_view = false;
static THR_LOCAL char original_query[256];
static THR_LOCAL MngEventsVector *mng_events = NULL;

using StrMap = gs_stl::gs_map<gs_stl::gs_string, masking_result>;

static void gsaudit_next_PostParseAnalyze_hook(ParseState *pstate, Query *query);

static void destroy_local_parameter();
static void destory_thread_variables()
{
    destroy_local_parameter();
    clear_thread_local_label();
    clear_thread_local_masking();
    clear_thread_local_auditing();
    gs_stl::DeleteVectorMemory();
    gs_stl::DeleteStringMemory();
    gs_stl::DeleteSetMemory();
    gs_stl::DeleteMapMemory();
}

static inline void clear_function_from_result_set()
{
    if (result_set_functions) {
        delete result_set_functions;
        result_set_functions = NULL;
    }
}

static inline void set_original_query(const char *query)
{
    int rc;
    query = (query != NULL) ? query : "";
    if (strlen(query) > 255) { /* 255 : if the length of query exceed 255, using ... instead of query */
        /* 252: the max length of query apart from ...*/
        rc = snprintf_s(original_query, sizeof(original_query), sizeof(original_query) - 1, "%.*s...", 252, query);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(original_query, sizeof(original_query), sizeof(original_query) - 1, "%s", query);
        securec_check_ss(rc, "\0", "\0");
    }
}

static void set_view_query_state(bool state)
{
    query_inside_view = state;
}

/*
 * parse remote host ip to IPV6 struct including local/ipv4/ipv6
 */
void get_remote_addr(IPV6 *ip)
{
    char ip_str[MAX_IP_LEN] = { 0 };
    get_session_ip(ip_str, MAX_IP_LEN);

    // format local unix 
    errno_t rc = EOK;
    char *local = "127.0.0.1";
    if (!strcmp("local", ip_str)) {
        rc = memcpy_s(ip_str, MAX_IP_LEN, local, strlen(local));
        securec_check_c(rc, "\0", "\0"); 
    }

    IPRange iprange;
    iprange.str_to_ip(ip_str, ip);
    return;
}

const char *get_session_app_name()
{
    return u_sess->attr.attr_common.application_name;
}

const char *GetUserName(char *user_name, size_t user_name_size)
{
    return GetRoleName(GetCurrentUserId(), user_name, user_name_size);
}

/*
 * write binary audit logs for sending to Elastic Search system, we not need write any
 * log when no elastic search config in the cluster. if elastic search system config, logs should
 * keep written, the audit thread keep parsing and sending to the ES system at the same time.
 */
static inline void gs_elastic_log(const char *ip, const char *event, int event_type, int result_type)
{
    /* AUDIT_POLICY_EVENT, AUDIT_OK */
    if (IS_PGXC_COORDINATOR && g_instance.attr.attr_security.use_elastic_search) {
        audit_report((AuditType)event_type, (AuditResult)result_type, ip, event, UNIFIED_AUDIT_TYPE);
    }
}

static void save_mng_event(const char *buffer)
{
    if (mng_events == NULL) {
        mng_events = new MngEventsVector;
    }
    mng_events->push_back(buffer);
}

static inline void send_sys_log(const char *module, const char *message, AuditResult result_type)
{
    if (!strlen(message)) {
        return;
    }
    int option = 0;
    openlog(module, option, LOG_LOCAL0);
    syslog(LOG_DEBUG, "%s, result: [%s]", message, (result_type == AUDIT_OK) ? "OK" : "FAILED");
    closelog();
}

void gs_audit_issue_syslog_message(const char *module, const char *message, int event_type, int result_type)
{
    char session_ip[MAX_IP_LEN] = {0};
    get_session_ip(session_ip, MAX_IP_LEN);

    send_sys_log(module, message, (AuditResult)result_type);
    if (event_type != SECURITY_EVENT) {
        char query_message[MESSAGESIZE] = { 0 };
        int rc = snprintf_s(query_message, sizeof(query_message), sizeof(query_message) - 1, "QUERY: [%s], %s",
            original_query, message);
        securec_check_ss(rc, "\0", "\0");
        gs_elastic_log(session_ip, query_message, event_type, result_type);
    } else {
        gs_elastic_log(session_ip, original_query, event_type, result_type);
    }
}

/*
 * flush security policy configuration logs into log system
 */
static void send_mng_events(AuditResult result_type)
{
    if (mng_events == NULL) {
        return;
    }
    for (const gs_stl::gs_string &event : *mng_events) {
        send_sys_log("PGSECURITYMNG", event.c_str(), result_type);
    }
    delete mng_events;
    mng_events = NULL;
    gs_audit_issue_syslog_message("PGSECURITYMNG", "", SECURITY_EVENT, result_type);
}

static void destroy_local_parameter()
{
    if (mng_events != NULL) {
        delete mng_events;
        mng_events = NULL; 
    }

    free_masked_cursor_stmts();
}

/* 
 * append object name, the format is: schema.table
 */
void get_name_range_var(const RangeVar *rangevar, gs_stl::gs_string *buffer, bool enforce)
{
    if (rangevar == NULL) {
        return;
    }
    if (rangevar->schemaname == NULL || strlen(rangevar->schemaname) == 0) {
        if (enforce) {
            const char *name = get_namespace_name(SchemaNameGetSchemaOid(NULL, true));
            if (name && strlen(name)) {
                buffer->append(name);
            }
        }
    } else {
        buffer->append(rangevar->schemaname);
    }

    if (rangevar->relname && strlen(rangevar->relname) != 0) {
        if (!buffer->empty()) {
            buffer->push_back('.');
        }
        buffer->append(rangevar->relname);
    }
}

/*
 * get_relevant_policies
 * 
 * two policies will have conflict only when their labels are instersect
 * so first we should get all policies that have intersection with check_labels
 */
static void get_relevant_policies(policy_oid_set *relevant_policies,
    const policy_labelname_set *check_labels, Oid policyOid)
{
    pg_masking_action_map *actions = get_masking_actions();
    if (actions == NULL || actions->empty()) {
        return;
    }
    pg_masking_action_map::const_iterator pol_iter = actions->begin();
    while (pol_iter != actions->end()) {
        if (*(pol_iter.first) == policyOid) {
            ++pol_iter;
            continue;
        }
        pg_masking_action_set::const_iterator act_iter = pol_iter.second->begin();
        while (act_iter != pol_iter.second->end()) {
            if (check_labels->find(act_iter->m_label_name) != check_labels->end()) {
                relevant_policies->insert(*(pol_iter.first));
                break;
            }
            ++act_iter;
        }
        ++pol_iter;
    }
}

/*
 * is_filter_conflict_with_policies
 * 
 * check whether relevant policies are conflict with current filter_tree
 */
static bool is_filter_conflict_with_policies(const char *filter_tree,
    policy_oid_set *relevant_policies, gs_stl::gs_string *polname)
{
    /* empty means no exist polices */
    if (relevant_policies->empty()) {
        return false;
    }
    PolicyLogicalTree ltree;
    gs_policy_filter_map *masking_filters = get_masking_filters();
    policy_oid_set::iterator polit = relevant_policies->begin();

    if (masking_filters == NULL || masking_filters->empty()) {
        (void)get_masking_policy_name_by_oid(*polit, polname);
        return true;
    }

    ltree.parse_logical_expression(filter_tree);
    while (polit != relevant_policies->end()) {
        if (masking_filters->find(*polit) == masking_filters->end() ||
            (*masking_filters)[*polit].m_tree.has_intersect(&ltree)) {
            (void)get_masking_policy_name_by_oid(*polit, polname);
            return true;
        }
        ++polit;
    }
    return false;
}

/*
 * is_label_valid_by_policy
 * 
 * given filter_tree and check_labels of a policy, check if there are conflict policies in policy catalog
 */ 
static bool is_label_valid_by_policy(const char *filter_tree, const policy_labelname_set *check_labels,
    Oid policyOid, gs_stl::gs_string *polname)
{
    if (check_labels == NULL || check_labels->empty()) {
        return true;
    }
    /* get polices which maybe has intersect witch check_labels */
    policy_oid_set relevant_policies;
    get_relevant_policies(&relevant_policies, check_labels, policyOid);

    /* check relevant policy is conflict with filter_tree */
    return !is_filter_conflict_with_policies(filter_tree, &relevant_policies, polname);
}

static bool is_masking_policy_exist()
{
    gs_policy_set *policies = get_masking_policies();
    if (policies == NULL) {
        return false;
    }
    return !policies->empty();
}

/* translating copy statement to select, to work with masking policy. */
bool verify_copy_command_is_reparsed(List* parsetree_list, const char* query_string,
                                     gs_stl::gs_string& replaced_query_string)
{
    /* do nothing when enable_security_policy is off */
    bool is_exist = (!u_sess->attr.attr_security.Enable_Security_Policy ||
        !is_masking_policy_exist());
    if (is_exist) return false;
    ListCell* item = NULL;
    foreach(item, parsetree_list) {
        Node* parsetree = (Node *) lfirst(item);
        if (nodeTag(parsetree) == T_CopyStmt) {
            CopyStmt* stmt = (CopyStmt*)parsetree;
            bool is_from_query = (stmt->is_from || stmt->query);
            if (is_from_query) return false;
            /* verify policies */
            IPV6 ip;
            get_remote_addr(&ip);
            FilterData filter_item(u_sess->attr.attr_common.application_name, ip);
            policy_set policy_ids;
            check_masking_policy_filter(&filter_item, &policy_ids);
            if (policy_ids.empty()) {
                if (checkSecurityPolicyFilter_hook != NULL) {
                    checkSecurityPolicyFilter_hook(filter_item, &policy_ids);
                }
            }
            bool is_empty_filter = (policy_ids.empty() && !check_audit_policy_filter(&filter_item, &policy_ids));
            if (is_empty_filter) return false;
            gs_stl::gs_string replace_buffer("(select ");

            char replace_name[POLICY_TMP_BUFF_LEN] = {0};
            int replace_size = 0;

            if (stmt->relation->length) {
                replace_size = (stmt->relation->length - stmt->relation->location);
            } else {
                gs_stl::gs_string rel;
                get_name_range_var(stmt->relation, &rel, false);
                replace_size = (int)rel.size();
                /* check quote */
                if (query_string[stmt->relation->location] == '\"') {
                    replace_size += 2;
                }
            }

            int rc = snprintf_s(replace_name, sizeof(replace_name), sizeof(replace_name) - 1,
                                "%.*s", replace_size, query_string + stmt->relation->location);
            securec_check_ss(rc, "\0", "\0");
            /* parse options */
            ListCell   *option = NULL;
            foreach(option, stmt->options) {
                DefElem    *defel = (DefElem *) lfirst(option);
                if (!strcasecmp(defel->defname, "oids")) {
                    replace_buffer.append("Oid,");
                }
            }

            replaced_query_string = query_string;
            if (stmt->attlist != NIL) {
                ListCell* att_item = NULL;
                foreach(att_item, stmt->attlist) {
                    const char* att_name = strVal(lfirst(att_item));
                    replace_buffer.append(att_name);
                    replace_buffer.push_back(',');
                }
                replace_buffer.pop_back();

                int find_start = (stmt->relation->location + replace_size);
                size_t start_pos = 0;
                size_t end_pos = 0;
                while (true) {
                    start_pos = replaced_query_string.find('(', find_start);
                    if (start_pos == gs_stl::gs_string::npos) {
                        return false;
                    }
                    end_pos = replaced_query_string.find(')', start_pos);
                    if (end_pos == gs_stl::gs_string::npos) {
                        return false;
                    }
                    if ((end_pos - start_pos) > 1) {
                        break;
                    }
                    find_start = end_pos + 1;
                }
                replaced_query_string.erase(start_pos, end_pos-start_pos + 1);
            } else {
                replace_buffer.push_back('*');
            }

            replace_buffer.append(" from ");
            replace_buffer.append(replace_name);
            replace_buffer.push_back(')');

            replaced_query_string.replace(stmt->relation->location, replace_size, replace_buffer.c_str());
            return true;
        }
    }
    return false;
}

void set_result_set_function(const PolicyLabelItem &func)
{
    if (result_set_functions == NULL) {
        result_set_functions = new gs_policy_label_set;
    }
    if (result_set_functions) {
        result_set_functions->insert(func);
    }
}

static void gsaudit_next_PostParseAnalyze_hook(ParseState *pstate, Query *query)
{
    /* do nothing when enable_security_policy is off */
    if (!u_sess->attr.attr_security.Enable_Security_Policy) {
        if (next_post_parse_analyze_hook) {
            next_post_parse_analyze_hook(pstate, query);
        }
        return;
    }

    /* disable unified auditing on datanode */
    bool enable_dml_auditing = false;
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !IsConnFromCoord() && is_audit_policy_exist()) {
        enable_dml_auditing = true;
    }

    if (u_sess->proc_cxt.IsNoMaskingInnerTools || (t_thrd.role != WORKER && t_thrd.role != THREADPOOL_WORKER) ||
        (!enable_dml_auditing && !is_masking_policy_exist())) {
        if (next_post_parse_analyze_hook) {
            next_post_parse_analyze_hook(pstate, query);
        }
        return;
    }
    set_original_query(pstate->p_sourcetext);

    switch (query->commandType) {
        case CMD_SELECT: {
            IPV6 ip;
            get_remote_addr(&ip);
            FilterData filter_item(u_sess->attr.attr_common.application_name, ip);
            policy_set policy_ids;
            /* when query inside view , we do not need make masking */
            if (!query_inside_view) {
                check_masking_policy_filter(&filter_item, &policy_ids);
            }
            reset_node_location();

            /* Main entrance for masking */
            process_masking(pstate, query, &policy_ids, enable_dml_auditing);
            /* check functions */
            if (enable_dml_auditing && result_set_functions) {
                policy_set audit_policy_ids;
                check_audit_policy_filter(&filter_item, &audit_policy_ids);
                policy_result pol_result;
                PolicyLabelItem view_item(0, 0, O_VIEW);
                for (const PolicyLabelItem func : *result_set_functions) {
                    int block_behaviour = 0;
                    check_audit_policy_access(&func, &view_item, query->commandType, &audit_policy_ids, &pol_result,
                        get_policy_accesses(), &block_behaviour);
                }
                flush_policy_result(&pol_result, query->commandType);
                flush_access_logs(AUDIT_OK);
            }
        } break;
        case CMD_UPDATE:
        case CMD_DELETE:
        case CMD_INSERT: /* for auditing insert to table */
        case CMD_MERGE:
        {
            IPV6 ip;
            get_remote_addr(&ip);
            FilterData filter_item(get_session_app_name(), ip);
            policy_set security_policy_ids;
            if (checkSecurityPolicyFilter_hook != NULL) {
                checkSecurityPolicyFilter_hook(filter_item, &security_policy_ids);
            }

            /* when query inside view , we do not need make masking */
            policy_set masking_policy_ids;
            if (!query_inside_view) {
                check_masking_policy_filter(&filter_item, &masking_policy_ids);
            }

            if (query->rtable != NIL) {
                ListCell *lc = NULL;
                /* For INSERT/UPDATE/DELETE with RETURNING clause, and we will handle returning list by masking */
                if (query->returningList != NIL) {
                    handle_masking(query->returningList, pstate, &masking_policy_ids,
                                   query->rtable, query->utilityStmt);
                }

                if (query->targetList != NIL) {
                    handle_masking(query->targetList, pstate, &masking_policy_ids,
                                   query->rtable, query->utilityStmt);
                }

                /* For MERGE INTO, handle targetlist of mergeActionList */
                if (query->mergeActionList != NIL) {
                    foreach (lc, query->mergeActionList) {
                        MergeAction *action = (MergeAction*)lfirst(lc);
                        handle_masking(action->targetList, pstate, &masking_policy_ids,
                                       query->rtable, query->utilityStmt);
                    }
                }

                foreach (lc, query->rtable) {
                    RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
                    if (rte->rtekind == RTE_REMOTE_DUMMY) {
                        continue;
                    } else if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL) { /* check masking */
                        reset_node_location();
                        handle_masking(rte->subquery->targetList, pstate,
                                       &masking_policy_ids, rte->subquery->rtable, rte->subquery->utilityStmt);
                    }
                }
            }
            break;
        }
        case CMD_UTILITY:
        {
            /* For ALTER TABLE EXCHANGE, will not allowed if the ordinary table(it's columns) has masking policy.*/
            if (query->utilityStmt != NULL && nodeTag(query->utilityStmt) == T_AlterTableStmt) {
                AlterTableStmt *alter_table = (AlterTableStmt *)(query->utilityStmt);
                Oid relid = RangeVarGetRelid(alter_table->relation, NoLock, true);

                ListCell *lc = NULL;
                foreach (lc, alter_table->cmds) {
                    AlterTableCmd *cmd = (AlterTableCmd *)lfirst(lc);
                    if (cmd->subtype == AT_ExchangePartition) {
                        Assert(PointerIsValid(cmd->exchange_with_rel));
                        if (is_masked_relation(relid)) {
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("ALTER TABLE EXCHANGE can not execute with masked partition table.")));
                        }

                        Oid ordTableOid = RangeVarGetRelid(cmd->exchange_with_rel, NoLock, true);
                        if (is_masked_relation(ordTableOid)) {
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("ALTER TABLE EXCHANGE can not execute with masked ordinary table.")));
                        }
                    }
                }
            }
            break;
        }
        default:
            break;
    }

    clear_function_from_result_set();
    if (next_post_parse_analyze_hook) {
        next_post_parse_analyze_hook(pstate, query);
    }
}

struct OfiraCmd {
    int id;
    char *url;
};
static const char *ACL_get_object_name(int targetype, int objtype, ListCell *objCell,
    gs_stl::gs_string *buffname);
/* create from list of name fqdn string */
static inline void get_object_name(List *fqdn, gs_stl::gs_string *name)
{
    switch (list_length(fqdn)) {
        case 1:
            *name = strVal(linitial(fqdn));
            break;
        case 2:
            *name = strVal(linitial(fqdn));
            name->push_back('.');
            name->append(strVal(lsecond(fqdn)));
            break;
        case 3:
            *name = strVal(linitial(fqdn));
            name->push_back('.');
            name->append(strVal(lsecond(fqdn)));
            name->push_back('.');
            name->append(strVal(lthird(fqdn)));
            break;
        default:
            break;
    }
}
static inline bool get_prepare_command_object_name(Node *parsetree, RangeVar *&rel)
{
    switch (nodeTag(parsetree)) {
        case T_InsertStmt: {
            InsertStmt *_stmt = (InsertStmt *)parsetree;
            rel = _stmt->relation;
            return true;
        }
        case T_UpdateStmt: {
            UpdateStmt *_stmt = (UpdateStmt *)parsetree;
            ListCell *l = NULL;
            foreach (l, _stmt->relationClause) {
                Node *n = (Node *)lfirst(l);
                if (IsA(n, RangeVar)) {
                    rel = (RangeVar *)n;
                    return true;
                }
            }
            return true;
        }
        case T_SelectStmt: {
            SelectStmt *_stmt = (SelectStmt *)parsetree;
            if (_stmt->intoClause) {
                rel = _stmt->intoClause->rel;
            } else if (_stmt->op == SETOP_NONE && _stmt->fromClause) {
                ListCell *fl = NULL;
                foreach (fl, _stmt->fromClause) {
                    Node *n = (Node *)lfirst(fl);
                    if (IsA(n, RangeVar)) {
                        rel = (RangeVar *)n;
                        return true;
                    }
                }
            }
            return true;
        }
        case T_DeleteStmt: {
            DeleteStmt *_stmt = (DeleteStmt *)parsetree;
            rel = (RangeVar*)linitial(_stmt->relations);
            return true;
        }
        default:
            return false;
    }
    return false;
}

static void verify_drop_user(const char *rolename)
{
    /* Ignore checking when upgrade */
    if (u_sess->attr.attr_common.IsInplaceUpgrade) {
        return;
    }
    Oid roleid = get_role_oid(rolename, true);
    if ((isRoleInUse_hook != NULL && isRoleInUse_hook(roleid)) || is_masking_role_in_use(roleid) ||
        is_role_in_use(roleid) || (security_isRoleInUse_hook != NULL && security_isRoleInUse_hook(roleid))) {
        char buff[POLICY_STR_BUFF_LEN] = {0};
        int rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
            "Role: %s is used by one or more policy filter, and can be dropped only after removing from policy",
            rolename);
        securec_check_ss(rc, "\0", "\0");
        gs_audit_issue_syslog_message("PGAUDIT", buff, AUDIT_POLICY_EVENT, AUDIT_FAILED);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\"", buff)));
    }
}

/*
 * Hook ProcessUtility to do session auditing for DDL and utility commands.
 */
static inline void get_copy_table_name(CopyStmt *stmt, gs_stl::gs_string *name)
{
    if (stmt->relation) {
        get_name_range_var(stmt->relation, name);
    } else if (stmt->query) {
        if (nodeTag(stmt->query) == T_SelectStmt) {
            SelectStmt *_stmt = (SelectStmt *)stmt->query;
            if (_stmt->op == SETOP_NONE) {
                if (_stmt->fromClause) {
                    ListCell *fl = NULL;
                    foreach (fl, _stmt->fromClause) {
                        Node *n = (Node *)lfirst(fl);
                        if (IsA(n, RangeVar)) {
                            RangeVar *rv = (RangeVar *)n;
                            get_name_range_var(rv, name);
                        }
                    }
                }
            }
        }
    }
}

bool is_audit_policy_exist_load_policy_info()
{
    load_database_policy_info();
    gs_policy_set *policies = get_audit_policies();
    return policies != NULL && !policies->empty();
}

static void light_unified_audit_executor(const Query *query)
{
    /* do nothing when enable_security_policy is off */
    if (!u_sess->attr.attr_security.Enable_Security_Policy ||
        u_sess->proc_cxt.IsInnerMaintenanceTools || IsConnFromCoord() ||
        !is_audit_policy_exist_load_policy_info()) {
            return;
    }

    ereport(DEBUG1, (errmsg("light_unified_audit_executor routine enter")));

    if (!query->rtable) {
        return;
    }
    access_audit_policy_run(query->rtable, query->commandType);
}

static void gsaudit_ProcessUtility_hook(processutility_context* processutility_cxt,
    DestReceiver *dest, bool sentToRemote, char *completionTag, ProcessUtilityContext context,bool isCTAS = false)
{
    /* do nothing when enable_security_policy is off */
    if (!u_sess->attr.attr_security.Enable_Security_Policy || !IsConnFromApp() ||
        u_sess->proc_cxt.IsInnerMaintenanceTools || IsConnFromCoord() ||
        !is_audit_policy_exist_load_policy_info()) {
        if (next_ProcessUtility_hook) {
            next_ProcessUtility_hook(processutility_cxt, dest, sentToRemote, completionTag,
                context, isCTAS);
        } else {
            standard_ProcessUtility(processutility_cxt, dest, sentToRemote, completionTag,
                context, isCTAS);
        }
        return;
    }
    IPV6 ip;
    get_remote_addr(&ip);
    FilterData filter_item(u_sess->attr.attr_common.application_name, ip);
    policy_set audit_policy_ids;
    check_audit_policy_filter(&filter_item, &audit_policy_ids);
    policy_set security_policy_ids;
    if (checkSecurityPolicyFilter_hook != NULL) {
        checkSecurityPolicyFilter_hook(filter_item, &security_policy_ids);
    }
    RenameMap renamed_objects;
    Node* parsetree = processutility_cxt->parse_tree;
    if (parsetree != NULL) {
        switch (nodeTag(parsetree)) {
            case T_PlannedStmt: {
                if (!check_audited_privilige(T_CURSOR) && !SECURITY_CHECK_ACL_PRIV(T_CURSOR)) {
                    break;
                }
                char buff[POLICY_STR_BUFF_LEN] = {0};
                PlannedStmt *stmt = (PlannedStmt *)parsetree;
                get_open_cursor_info(stmt, buff, sizeof(buff));
                internal_audit_str(&security_policy_ids, &audit_policy_ids, buff, T_CURSOR, "OPEN", O_CURSOR);
                break;
            }
            case T_FetchStmt: {
                FetchStmt *stmt = (FetchStmt *)parsetree;
                Portal portal = GetPortalByName(stmt->portalname);
                if (portal) {
                    char buff[POLICY_STR_BUFF_LEN] = {0};
                    int printed_size = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%s FOR %s", stmt->portalname,
                        portal->commandTag);
                    securec_check_ss(printed_size, "\0", "\0");
                    gs_stl::gs_vector<PolicyLabelItem> cursor_objects;
                    if (portal && portal->queryDesc && portal->queryDesc->plannedstmt &&
                        portal->queryDesc->plannedstmt->rtable) {
                        get_cursor_tables(portal->queryDesc->plannedstmt->rtable, buff, sizeof(buff), printed_size,
                            &cursor_objects);
                    }
                    for (const PolicyLabelItem item : cursor_objects) {
                        internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item, T_FETCH, "FETCH",
                            stmt->portalname);
                    }
                    flush_cursor_stmt_masking_result(stmt->portalname); /* invoke masking event in this case */
                }
                break;
            }
            case T_ClosePortalStmt: {
                ClosePortalStmt *stmt = (ClosePortalStmt *)parsetree;
                char tmp[POLICY_TMP_BUFF_LEN] = {0};
                int rc;
                if (stmt->portalname) {
                    rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "%s", stmt->portalname);
                    securec_check_ss(rc, "\0", "\0");
                } else {
                    rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "ALL");
                    securec_check_ss(rc, "\0", "\0");
                }
                internal_audit_str(&security_policy_ids, &audit_policy_ids, tmp, T_CLOSE, "CLOSE", O_CURSOR);
                close_cursor_stmt_as_masked(tmp);
                break;
            }
            case T_CopyStmt: {
                CopyStmt *stmt = (CopyStmt *)parsetree;
                gs_stl::gs_string name;
                get_copy_table_name(stmt, &name);
                char buff[POLICY_STR_BUFF_LEN] = {0};
                int rc;
                if (stmt->is_from) {
                    if (stmt->filename != NULL) {
                        rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%s FROM FILE: %s", name.c_str(),
                            stmt->filename);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%s FROM STDIN", name.c_str());
                        securec_check_ss(rc, "\0", "\0");
                    }
                } else {
                    if (stmt->filename != NULL) {
                        rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%s TO FILE: %s", name.c_str(),
                            stmt->filename);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%s TO STDOUT", name.c_str());
                        securec_check_ss(rc, "\0", "\0");
                    }
                }
                if (stmt->relation) {
                    check_access_table(&audit_policy_ids, stmt->relation, CMD_UTILITY, O_TABLE, buff, "COPY");
                }
                break;
            }
            case T_ReindexStmt: {
                if (!check_audited_access(CMD_REINDEX))
                    break;
                ReindexStmt *stmt = (ReindexStmt *)parsetree;
                switch (stmt->kind) {
                    case OBJECT_INDEX:
                        if (stmt->relation)
                            check_access_table(&audit_policy_ids, stmt->relation, CMD_REINDEX, O_INDEX);
                        break;
                    case OBJECT_TABLE:
                        if (stmt->relation)
                            check_access_table(&audit_policy_ids, stmt->relation, CMD_REINDEX, O_TABLE);
                        break;
                    case OBJECT_DATABASE:
                        check_access_table(&audit_policy_ids,
                            get_database_name(u_sess->proc_cxt.MyDatabaseId), CMD_REINDEX, O_DATABASE);
                        break;
                    default:
                        break;
                }
                break;
            }
            case T_ExecuteStmt: {
                ExecuteStmt *stmt = (ExecuteStmt *)parsetree;
                if (check_audited_access(CMD_EXECUTE)) {
                    check_access_table(&audit_policy_ids, stmt->name, CMD_EXECUTE, O_UNKNOWN, stmt->name);
                }
                flush_prepare_stmt_masking_result(stmt->name); /* invoke masking event in this case */
                break;
            }
            case T_DeallocateStmt: {
                DeallocateStmt *stmt = (DeallocateStmt *)parsetree;
                char tmp[POLICY_TMP_BUFF_LEN] = {0};
                int rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "%s", stmt->name == NULL ? "ALL" : stmt->name);
                securec_check_ss(rc, "\0", "\0");
                check_access_table(&audit_policy_ids, tmp, CMD_DEALLOCATE, O_UNKNOWN, tmp);
                unprepare_stmt_as_masked(tmp);
                break;
            }
            case T_PrepareStmt: {
                PrepareStmt *stmt = (PrepareStmt *)parsetree;
                const char *name = stmt->name;
                prepare_stmt(name);
                if (!check_audited_access(CMD_PREPARE)) {
                    break;
                }
                char tmp[POLICY_TMP_BUFF_LEN] = {0};
                const char *prepare_command = CreateCommandTag(stmt->query);
                int rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "%s as %s", name, prepare_command);
                securec_check_ss(rc, "\0", "\0");
                RangeVar *rel = NULL;
                get_prepare_command_object_name(stmt->query, rel);
                if (rel) {
                    check_access_table(&audit_policy_ids, rel, CMD_PREPARE, O_TABLE, tmp);
                }
                break;
            }
            case T_AlterSeqStmt: {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterSeqStmt *stmt = (AlterSeqStmt *)parsetree;
                if (stmt != NULL && stmt->sequence) {
                    gs_stl::gs_string tmp;
                    get_name_range_var(stmt->sequence, &tmp);
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, tmp.c_str(), T_ALTER, "ALTER",
                        O_SEQUENCE);
                }
                break;
            }
            case T_CreateSeqStmt: {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateSeqStmt *stmt = (CreateSeqStmt *)parsetree;
                if (stmt != NULL && stmt->sequence) {
                    gs_stl::gs_string tmp;
                    get_name_range_var(stmt->sequence, &tmp);
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, tmp.c_str(), T_CREATE,
                        "CREATE", O_SEQUENCE);
                }
                break;
            }
            case T_AlterDatabaseSetStmt: {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterDatabaseSetStmt *alterdatabasesetstmt = (AlterDatabaseSetStmt *)(parsetree);
                if (alterdatabasesetstmt && alterdatabasesetstmt->dbname) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, alterdatabasesetstmt->dbname,
                        T_ALTER, "ALTER", O_DATABASE, true);
                }
                break;
            }
            case T_AlterDatabaseStmt: {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterDatabaseStmt *alterdatabasestmt = (AlterDatabaseStmt *)(parsetree);
                if (alterdatabasestmt && alterdatabasestmt->dbname) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, alterdatabasestmt->dbname,
                        T_ALTER, "ALTER", O_DATABASE, true);
                }
                break;
            }
            case T_CreatedbStmt: {
                CreatedbStmt *createdbstmt = (CreatedbStmt *)(parsetree);
                if (createdbstmt && createdbstmt->dbname) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, createdbstmt->dbname,
                        T_CREATE, "CREATE", O_DATABASE, true);
                }
                break;
            }
            case T_DropdbStmt: {
                if (!check_audited_privilige(T_DROP) && !SECURITY_CHECK_ACL_PRIV(T_DROP)) {
                    break;
                }
                DropdbStmt *dropdbstmt = (DropdbStmt *)(parsetree);
                if (dropdbstmt && dropdbstmt->dbname) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, dropdbstmt->dbname, T_DROP,
                        "DROP", O_DATABASE, true);
                }
                break;
            }
            case T_CreateForeignServerStmt: {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateForeignServerStmt *createforeignserverstmt = (CreateForeignServerStmt *)(parsetree);
                if (createforeignserverstmt && createforeignserverstmt->servername) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids,
                        createforeignserverstmt->servername, T_CREATE, "CREATE", O_SERVER);
                }
                break;
            }
            case T_AlterForeignServerStmt: {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterForeignServerStmt *alterforeignserverstmt = (AlterForeignServerStmt *)(parsetree);
                if (alterforeignserverstmt && alterforeignserverstmt->servername) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids,
                        alterforeignserverstmt->servername, T_ALTER, "ALTER", O_SERVER);
                }
                break;
            }
            case T_GrantStmt: {
                if (!check_audited_privilige(T_GRANT) && !check_audited_privilige(T_REVOKE) &&
                    !SECURITY_CHECK_ACL_PRIV(T_GRANT) && !SECURITY_CHECK_ACL_PRIV(T_REVOKE)) {
                    break;
                }
                GrantStmt *stmt = (GrantStmt *)parsetree;
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                if (stmt && stmt->grantees && stmt->objects) {
                    forboth(lc1, stmt->grantees, lc2, stmt->objects)
                    {
                        PrivGrantee *rte1 = (PrivGrantee *)lfirst(lc1);
                        ListCell *rel2 = (ListCell *)lfirst(lc2);
                        gs_stl::gs_string tmp;
                        const char *granted_name = ACL_get_object_name(stmt->targtype, stmt->objtype, lc2, &tmp);
                        if (granted_name != NULL) {
                            acl_audit_object(&security_policy_ids, &audit_policy_ids, rel2,
                                names_pair(granted_name,
                                    rte1->rolname ? rte1->rolname : "ALL" /* grantee_name */),
                                stmt->is_grant ? T_GRANT : T_REVOKE, stmt->is_grant ? "GRANT" : "REVOKE",
                                stmt->objtype, stmt->targtype);
                        }
                    }
                }
                break; /* PrivGrantee case group or user, audit is role */
            }
            case T_GrantRoleStmt: {
                if (!check_audited_privilige(T_GRANT) && !check_audited_privilige(T_REVOKE) &&
                    !SECURITY_CHECK_ACL_PRIV(T_GRANT) && !SECURITY_CHECK_ACL_PRIV(T_REVOKE)) {
                    break;
                }
                GrantRoleStmt *grantrolestmt = (GrantRoleStmt *)(parsetree);
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                if (grantrolestmt && grantrolestmt->grantee_roles && grantrolestmt->granted_roles) {
                    forboth(lc1, grantrolestmt->grantee_roles, lc2, grantrolestmt->granted_roles)
                    {
                        PrivGrantee *rte1 = (PrivGrantee *)lfirst(lc1);
                        PrivGrantee *rte2 = (PrivGrantee *)lfirst(lc2);

                        internal_audit_object_str(&security_policy_ids, &audit_policy_ids, NULL,
                            names_pair(rte2->rolname /* granted_name */, rte1->rolname /* grantee_name */),
                            grantrolestmt->is_grant ? T_GRANT : T_REVOKE, grantrolestmt->is_grant ? "GRANT" : "REVOKE",
                            O_ROLE, ACL_TARGET_OBJECT, true, true);
                    }
                }
                break;
            }
            case T_GrantDbStmt: {
                if (!check_audited_privilige(T_GRANT) && !check_audited_privilige(T_REVOKE) &&
                    !SECURITY_CHECK_ACL_PRIV(T_GRANT) && !SECURITY_CHECK_ACL_PRIV(T_REVOKE)) {
                    break;
                }
                GrantDbStmt *grantdbstmt = (GrantDbStmt *)(parsetree);
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                if (grantdbstmt && grantdbstmt->grantees && grantdbstmt->privileges) {
                    forboth(lc1, grantdbstmt->grantees, lc2, grantdbstmt->privileges)
                    {
                        PrivGrantee *rte1 = (PrivGrantee *)lfirst(lc1);
                        DbPriv *rte2 = (DbPriv*)lfirst(lc2);

                        internal_audit_object_str(&security_policy_ids, &audit_policy_ids, NULL,
                            names_pair(rte2->db_priv_name, rte1->rolname /* grantee_name */),
                            grantdbstmt->is_grant ? T_GRANT : T_REVOKE, grantdbstmt->is_grant ? "GRANT" : "REVOKE",
                            O_UNKNOWN, ACL_TARGET_OBJECT, true, false);
                    }
                }
                break;
            }
            case T_VacuumStmt: {
                if (!check_audited_privilige(T_ANALYZE) && !SECURITY_CHECK_ACL_PRIV(T_ANALYZE)) {
                    break;
                }
                VacuumStmt *stmt = (VacuumStmt *)parsetree;
                if (stmt) {
                    if (stmt->relation) {
                        PolicyLabelItem item;
                        gen_policy_labelitem(item, (const ListCell *)stmt->relation, O_TABLE);
                        if (stmt->va_cols) {
                            ListCell *citem = NULL;
                            foreach (citem, stmt->va_cols) {
                                /* analyze by column */
                                const char *column = strVal(lfirst(citem));
                                if (column && strlen(column)) {
                                    item.m_obj_type = O_COLUMN;
                                    int rc = snprintf_s(item.m_column, sizeof(item.m_column), sizeof(item.m_column) - 1,
                                        "%s", column);
                                    securec_check_ss(rc, "\0", "\0");
                                    if (internal_audit_object_str(&security_policy_ids, &audit_policy_ids,
                                        &item, T_ANALYZE, "ANALYZE")) {
                                        break;
                                    }
                                }
                            }
                        } else
                            internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item,
                                T_ANALYZE, "ANALYZE");
                    } else {
                        internal_audit_str(&security_policy_ids, &audit_policy_ids,
                            get_database_name(u_sess->proc_cxt.MyDatabaseId), T_ANALYZE, "ANALYZE", O_DATABASE, true);
                    }
                }
                break;
            }
            case T_CommentStmt: {
                if (!check_audited_privilige(T_COMMENT)) {
                    break;
                }

                CommentStmt *commentstmt = (CommentStmt *)(parsetree);
                PolicyLabelItem item(0, 0, get_objtype(commentstmt->objtype), "");
                gen_policy_label_for_commentstmt(item, commentstmt);

                gs_stl::gs_string objectname;
                add_current_path(commentstmt->objtype, commentstmt->objname, &objectname);
                internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item, T_COMMENT, "COMMENT",
                    objectname.c_str());
                break;
            }
            case T_VariableSetStmt: {
                if (check_audited_privilige(T_SET) || SECURITY_CHECK_ACL_PRIV(T_SET)) {
                    VariableSetStmt *variablesetstmt = (VariableSetStmt *)(parsetree);
                    if (variablesetstmt && variablesetstmt->name) {
                        gs_stl::gs_string tmp = variablesetstmt->name;
                        if (variablesetstmt->args) {
                            tmp.append(" TO ");
                            name_list_to_string(variablesetstmt->args, &tmp, 1);
                        }
                        internal_audit_str(&security_policy_ids, &audit_policy_ids, tmp.c_str(), T_SET, "SET",
                            O_PARAMETER);
                    }
                }
                RelationCacheInvalidate();
                break;
            }
            case T_VariableShowStmt: {
                if (!check_audited_privilige(T_SHOW) && !SECURITY_CHECK_ACL_PRIV(T_SHOW)) {
                    break;
                }
                VariableShowStmt *stmt = (VariableShowStmt *)parsetree;
                if (stmt && stmt->name)
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, stmt->name, T_SHOW, "SHOW",
                        O_PARAMETER);
                break;
            }
            case T_CreateSchemaStmt: /* create schema */
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateSchemaStmt *schemastmt = (CreateSchemaStmt *)(parsetree);
                audit_schema(security_policy_ids, audit_policy_ids, schemastmt->schemaname, "", T_CREATE,
                    "CREATE");
                break;
            }
            case T_RenameStmt: {
                RenameStmt *stmt = (RenameStmt *)parsetree;
                switch (stmt->renameType) {
                    case OBJECT_SCHEMA:
                        if (check_audited_privilige(T_ALTER)) {
                            audit_schema(security_policy_ids, audit_policy_ids, stmt->subname,
                                stmt->newname, T_RENAME);
                        }
                        break;
                    case OBJECT_COLUMN:
                        if (check_audited_privilige(T_ALTER)) {
                            rename_object(stmt, audit_policy_ids, security_policy_ids, &renamed_objects);
                        }
                        break;
                    case OBJECT_FUNCTION:
                        validate_masking_function_name(stmt->object);
                    default:
                        if (check_audited_privilige(T_ALTER)) {
                            rename_object(stmt, audit_policy_ids, security_policy_ids);
                        }
                        break;
                }
                break;
            }
            case T_AlterOwnerStmt: {
                AlterOwnerStmt *stmt = (AlterOwnerStmt *)parsetree;
                switch (stmt->objectType) {
                    case OBJECT_SCHEMA: {
                        if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                            const char *objectname = strVal(linitial(stmt->object));
                            audit_schema(security_policy_ids, audit_policy_ids, objectname, stmt->newowner,
                                T_ALTER);
                        }
                    } break;
                    case OBJECT_FUNCTION:
                        validate_masking_function_name(stmt->object);
                    default:
                        if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                            alter_owner(stmt, audit_policy_ids, security_policy_ids);
                        }
                        break;
                }
                break;
            }
            case T_DropOwnedStmt: {
                if (!check_audited_privilige(T_DROP) && !SECURITY_CHECK_ACL_PRIV(T_DROP)) {
                    break;
                }
                DropOwnedStmt *stmt = (DropOwnedStmt *)parsetree;
                ListCell *arg = NULL;
                foreach (arg, stmt->roles) {
                    const char *objectname = strVal(lfirst(arg));
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, objectname, T_DROP, "DROP",
                        O_ROLE, true);
                }
                break;
            }
            case T_AlterObjectSchemaStmt: {
                AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *)parsetree;
                if (stmt) {
                    switch (stmt->objectType) {
                        case OBJECT_TABLE:
                        case OBJECT_FOREIGN_TABLE:
                        case OBJECT_STREAM:
                            if (stmt->relation) {
                                if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                                    alter_table(&security_policy_ids, &audit_policy_ids, stmt->relation,
                                        T_ALTER, "ALTER", O_TABLE);
                                }
                                if (stmt->newschema && strlen(stmt->newschema) > 0) {
                                    reset_policy_labels();
                                }
                            }
                            break;
                        case OBJECT_CONTQUERY:
                        case OBJECT_VIEW:
                            if (stmt->relation) {
                                if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                                    audit_table(&security_policy_ids, &audit_policy_ids, stmt->relation,
                                        T_ALTER, "ALTER", O_VIEW);
                                }
                                if (stmt->newschema && strlen(stmt->newschema) > 0) {
                                    reset_policy_labels();
                                }
                            }
                            break;
                        case OBJECT_FUNCTION: {
                            if (stmt->object) {
                                validate_masking_function_name(stmt->object);
                                if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                                    PolicyLabelItem item(0, 0, O_FUNCTION);
                                    name_list_to_label(&item, stmt->object);
                                    internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item,
                                        T_ALTER, "ALTER");
                                }

                                if (stmt->newschema && strlen(stmt->newschema) > 0) {
                                    reset_policy_labels();
                                }
                            }
                        } break;
                        default:
                            break;
                    }
                }
                break;
            }
            case T_IndexStmt: /* create index */
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                IndexStmt *stmt = (IndexStmt *)parsetree;
                if (stmt && stmt->relation && stmt->idxname) {
                    gs_stl::gs_string objectname;
                    get_name_range_var(stmt->relation, &objectname);
                    if (!objectname.empty()) {
                        objectname.push_back('.');
                    }
                    objectname.append(stmt->idxname);
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, objectname.c_str(), T_CREATE,
                        "CREATE", O_INDEX);
                }
                break;
            }
            case T_CreateDataSourceStmt:
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateDataSourceStmt *stmt = (CreateDataSourceStmt *)parsetree;
                gs_stl::gs_string objectname;
                if (stmt && stmt->srcname) {
                    objectname.append(stmt->srcname);
                }
                internal_audit_str(&security_policy_ids, &audit_policy_ids, objectname.c_str(), T_CREATE,
                                   "CREATE", O_DATA_SOURCE);
                break;
            }
            case T_ViewStmt: /* create View */
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                ViewStmt *stmt = (ViewStmt *)parsetree;
                if (stmt && stmt->view) {
                    PolicyLabelItem item(stmt->view->schemaname, stmt->view->relname, "", O_VIEW);
                    internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item, T_CREATE,
                        "CREATE", stmt->view->relname);
                }
                break;
            }
            case T_CreateFunctionStmt: /* create procedure */
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateFunctionStmt *stmt = (CreateFunctionStmt *)parsetree;
                if (stmt && stmt->funcname) {
                    validate_masking_function_name(stmt->funcname);
                    PolicyLabelItem item(0, 0, O_FUNCTION);
                    char fname[POLICY_TMP_BUFF_LEN] = {0};
                    name_list_to_label(&item, stmt->funcname, fname, sizeof(fname));
                    internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item, T_CREATE,
                        "CREATE", fname);
                }
                break;
            }
            case T_AlterFunctionStmt: {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterFunctionStmt *stmt = (AlterFunctionStmt *)parsetree;
                if (stmt && stmt->func && stmt->func->funcname) {
                    validate_masking_function_name(stmt->func->funcname);
                    PolicyLabelItem item(0, 0, O_FUNCTION);
                    name_list_to_label(&item, stmt->func->funcname);
                    internal_audit_object_str(&security_policy_ids, &audit_policy_ids, &item, T_ALTER,
                        "ALTER");
                }
                break;
            }
            case T_CreateTrigStmt: {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateTrigStmt *stmt = (CreateTrigStmt *)(parsetree);
                if (stmt && stmt->trigname) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, stmt->trigname, T_CREATE,
                        "CREATE", O_TRIGGER);
                }
                break;
            }
            case T_DropTableSpaceStmt: {
                if (!check_audited_privilige(T_DROP) && !SECURITY_CHECK_ACL_PRIV(T_DROP)) {
                    break;
                }
                DropTableSpaceStmt *stmt = (DropTableSpaceStmt *)(parsetree);
                if (stmt && stmt->tablespacename) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, stmt->tablespacename, T_DROP,
                        "DROP", O_TABLESPACE);
                }
                break;
            }
            case T_AlterTableSpaceOptionsStmt: {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterTableSpaceOptionsStmt *altertabelspaceoptionsstmt = (AlterTableSpaceOptionsStmt *)(parsetree);
                if (altertabelspaceoptionsstmt && altertabelspaceoptionsstmt->tablespacename) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids,
                        altertabelspaceoptionsstmt->tablespacename, T_ALTER, "ALTER", O_TABLESPACE);
                }
                break;
            }
            case T_DropStmt: {
                DropStmt *stmt = (DropStmt *)parsetree;
                drop_command(stmt, &audit_policy_ids, &security_policy_ids);
                break;
            }
            case T_AlterTableStmt: /* alter table, foreigntable, index */
            {
                AlterTableStmt *altertablestmt = (AlterTableStmt *)(parsetree);
                if (altertablestmt && altertablestmt->relation) {
                    switch (altertablestmt->relkind) {
                        case OBJECT_FOREIGN_TABLE:
                        case OBJECT_STREAM:
                            if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                                alter_table(&security_policy_ids, &audit_policy_ids, altertablestmt->relation,
                                    T_ALTER, "ALTER", O_TABLE);
                            }
                            verify_drop_column(altertablestmt);
                            break;
                        case OBJECT_INDEX: {
                            if (!check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                                break;
                            }
                            gs_stl::gs_string index_name;
                            get_name_range_var(altertablestmt->relation, &index_name);
                            internal_audit_str(&security_policy_ids, &audit_policy_ids, index_name.c_str(),
                                T_ALTER, "ALTER", O_INDEX);
                            break;
                        }
                        case OBJECT_TABLE: {
                            if (check_audited_privilige(T_ALTER) || SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                                alter_table(&security_policy_ids, &audit_policy_ids, altertablestmt->relation,
                                    T_ALTER, "ALTER", O_TABLE);
                            }
                            verify_drop_column(altertablestmt);
                            break;
                        }
                        default:
                            break;
                    }
                }
                break;
            }
            case T_TruncateStmt: {
                /* 
                 * truncate is dml sql but go through the processutility routine
                 * so that use access function to deal with it in privilege hook entrance
                 */
                if (!check_audited_access(CMD_TRUNCATE)) {
                    break;
                }
                TruncateStmt *stmt = (TruncateStmt *)parsetree;
                ListCell *arg = NULL;
                foreach (arg, stmt->relations) {
                    RangeVar *rel = (RangeVar *)lfirst(arg);
                    check_access_table(&audit_policy_ids, rel, CMD_TRUNCATE);
                }
                break;
            }
            case T_CreateTableAsStmt: /* table */
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateTableAsStmt *createtablestmt = (CreateTableAsStmt *)(parsetree);
                if (createtablestmt != NULL && createtablestmt->into != NULL) {
                    audit_table(&security_policy_ids, &audit_policy_ids, createtablestmt->into->rel, T_CREATE, "CREATE",
                        O_TABLE);
                }
                break;
            }
            case T_CreateStmt:
            case T_CreateForeignTableStmt: {
                if (!check_audited_privilige(T_CREATE) && SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateStmt *createforeignstmt = (CreateStmt *)(parsetree);
                if (createforeignstmt && createforeignstmt->relation) {
                    audit_table(&security_policy_ids, &audit_policy_ids, createforeignstmt->relation,
                        T_CREATE, "CREATE", O_TABLE);
                }
                break;
            }
            case T_CreateTableSpaceStmt: {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateTableSpaceStmt *createtabelspacestmt = (CreateTableSpaceStmt *)(parsetree);
                if (createtabelspacestmt && createtabelspacestmt->tablespacename) {
                    internal_audit_str(&security_policy_ids, &audit_policy_ids,
                        createtabelspacestmt->tablespacename, T_CREATE, "CREATE", O_TABLESPACE);
                }
                break;
            }
            case T_CreateRoleStmt: /* create user, group */
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                CreateRoleStmt *stmt = (CreateRoleStmt *)parsetree;
                int objectype;
                switch (stmt->stmt_type) {
                    case ROLESTMT_ROLE:
                        objectype = O_ROLE;
                        break;
                    case ROLESTMT_USER:
                        objectype = O_USER;
                        break;
                    case ROLESTMT_GROUP:
                        objectype = O_GROUP;
                        break;
                    default:
                        objectype = O_UNKNOWN;
                        break;
                }
                internal_audit_str(&security_policy_ids, &audit_policy_ids, stmt->role, T_CREATE, "CREATE",
                    objectype, true);
                break;
            }
            case T_AlterRoleSetStmt: /* differentiate group or user, current audit is "ROLE" */
            {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterRoleSetStmt *stmt = (AlterRoleSetStmt *)parsetree;
                if (stmt && stmt->role)
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, stmt->role, T_ALTER, "ALTER",
                        O_ROLE, true);
                break;
            }
            case T_AlterRoleStmt: /* differentiate group or user, current audit is "ROLE" */
            {
                if (!check_audited_privilige(T_ALTER) && !SECURITY_CHECK_ACL_PRIV(T_ALTER)) {
                    break;
                }
                AlterRoleStmt *stmt = (AlterRoleStmt *)parsetree;
                if (stmt && stmt->role)
                    internal_audit_str(&security_policy_ids, &audit_policy_ids, stmt->role, T_ALTER, "ALTER",
                        O_ROLE, true);
                break;
            }
            case T_CreateWeakPasswordDictionaryStmt: 
            {
                if (!check_audited_privilige(T_CREATE) && !SECURITY_CHECK_ACL_PRIV(T_CREATE)) {
                    break;
                }
                internal_audit_str(&security_policy_ids, &audit_policy_ids, "pg_catalog.gs_global_config",
                                   T_CREATE, "CREATE WEAK PASSWORD DICTIONARY ********", O_TABLE);
                break;
            }            
            case T_DropWeakPasswordDictionaryStmt: 
            {
                if (!check_audited_privilige(T_DROP) && !SECURITY_CHECK_ACL_PRIV(T_DROP)) {
                    break;
                }
                internal_audit_str(&security_policy_ids, &audit_policy_ids, "pg_catalog.gs_global_config",
                                   T_DROP, "DROP WEAK PASSWORD DICTIONARY", O_TABLE);
                break;    
            }                    
            case T_DropRoleStmt: /* differentiate group or user, current audit is "ROLE" */
            {
                DropRoleStmt *stmt = (DropRoleStmt *)parsetree;
                ListCell *arg = NULL;
                if (stmt && stmt->roles) {
                    foreach (arg, stmt->roles) {
                        const char *rolename = strVal(lfirst(arg));
                        if (check_audited_privilige(T_DROP) || SECURITY_CHECK_ACL_PRIV(T_DROP)) {
                            internal_audit_str(&security_policy_ids, &audit_policy_ids, rolename, T_DROP,
                                "DROP", O_ROLE, true);
                        }
                        verify_drop_user(rolename);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    PG_TRY();
    {
        if (next_ProcessUtility_hook) {
            next_ProcessUtility_hook(processutility_cxt, dest, sentToRemote, completionTag,
                context, false);
        } else {
            standard_ProcessUtility(processutility_cxt, dest, sentToRemote, completionTag,
                context, false);
        }
        flush_access_logs(AUDIT_OK);
        send_mng_events(AUDIT_OK);
        if (!renamed_objects.empty()) {
            RenameMap::const_iterator rit = renamed_objects.begin();
            RenameMap::const_iterator reit = renamed_objects.end();
            for (; rit != reit; ++rit) {
                for (const ::RenamePair &item : *(rit.second)) {
                    update_label_value(item.first, item.second, *(rit.first));
                }
            }
            reset_policy_labels();
            renamed_objects.clear();
        }
    }
    PG_CATCH();
    {
        flush_access_logs(AUDIT_FAILED);
        send_mng_events(AUDIT_FAILED);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* in cases of struct GrantStmt */
static const char *ACL_get_object_name(int targetype, int objtype, ListCell *objCell,
    gs_stl::gs_string *buffname)
{
    switch (objtype) {
        case ACL_OBJECT_DATABASE:
        case ACL_OBJECT_NAMESPACE:
        case ACL_OBJECT_TABLESPACE: {
            RangeVar *rte = (RangeVar *)lfirst(objCell);
            if (rte && rte->catalogname) {
                return rte->catalogname;
            }
            break;
        }
        case ACL_OBJECT_RELATION:
        case ACL_OBJECT_SEQUENCE: {
            RangeVar *rte = (RangeVar *)lfirst(objCell);
            if (targetype == ACL_TARGET_ALL_IN_SCHEMA) {
                if (rte && rte->catalogname) {
                    return rte->catalogname;
                }
            }
            if (rte && rte->relname) {
                get_name_range_var(rte, buffname);
                return buffname->c_str();
            }
            break;
        }
        case ACL_OBJECT_FUNCTION: {
            if (targetype == ACL_TARGET_ALL_IN_SCHEMA) {
                return strVal(lfirst(objCell));
            }

            FuncWithArgs *func = (FuncWithArgs *)lfirst(objCell);
            if (func && func->funcname) {
                add_current_path(OBJECT_FUNCTION, func->funcname, buffname);
                return buffname->c_str();
            }
            break;
        }
        default:
            break;
    }
    return NULL;
}

static void gs_audit_executor_start_hook(QueryDesc *queryDesc, int eflags)
{
    /* verify parameter and audit policy */
    if (!u_sess->attr.attr_security.Enable_Security_Policy ||
        u_sess->proc_cxt.IsInnerMaintenanceTools || IsConnFromCoord() ||
        !is_audit_policy_exist_load_policy_info() || queryDesc == NULL) {
        if (next_ExecutorStart_hook) {
            next_ExecutorStart_hook(queryDesc, eflags);
        } else {
            standard_ExecutorStart(queryDesc, eflags);
        }
        return;
    }

    /* execute audit policy by target application info/object/operation */
    const PlannedStmt *plannedstmt = queryDesc->plannedstmt;
    if (plannedstmt != NULL) {
        access_audit_policy_run(plannedstmt->rtable, plannedstmt->commandType);
    }

    /* flush the audit logs with result flag */
    PG_TRY();
    {
        if (next_ExecutorStart_hook) {
            next_ExecutorStart_hook(queryDesc, eflags);
        } else {
            standard_ExecutorStart(queryDesc, eflags);
        }
        flush_access_logs(AUDIT_OK);
    }
    PG_CATCH();
    {
        flush_access_logs(AUDIT_FAILED);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* 
 * load policy info for specific database(holding it as oid or dbname) including label, audit and mask
 * always run it at the begining of hooks as resource will decide whether to load the info for one specific databaseid
 */
void load_database_policy_info()
{
    /* databaseid will be invalid when database init or shutdown */
    if (u_sess->proc_cxt.MyDatabaseId == 0) {
        return;
    }

    /* label */
    load_policy_labels(true);

    /* auditing */
    reload_audit_policy();

    /* masking */
    if (reload_masking_policy())
    {
        free_masked_prepared_stmts();
    }

    return;
}

/*
 * hooks to implement security policy for audit
 */
void install_audit_hook()
{
    next_post_parse_analyze_hook = post_parse_analyze_hook;
    post_parse_analyze_hook = gsaudit_next_PostParseAnalyze_hook;
    if (!IS_PGXC_COORDINATOR && !IS_SINGLE_NODE) {
        ereport(DEBUG1, (errmsg("security plugin is disabled in datanode")));
        return;
    }
    /*
     * internal hooks in security policy plugin for decouple audit & masking & rls features
     */
    load_audit_policies_hook = load_audit_policies;
    load_policy_access_hook = load_policy_accesses;
    load_policy_privileges_hook = load_policy_privileges;
    load_policy_filter_hook = load_policy_filters;
    check_audit_policy_privileges_for_label_hook = check_audit_policy_privileges_for_label;
    check_audit_policy_access_for_label_hook = check_audit_policy_access_for_label;
    gs_save_mng_event_hook = save_mng_event;
    gs_send_mng_event_hook = send_mng_events;

    /*
     * Install our hook functions after saving the existing pointers to
     * preserve the chains.
     */
    next_ExecutorStart_hook = ExecutorStart_hook;
    next_ProcessUtility_hook = ProcessUtility_hook;

    /*
     * Install audit hooks, the interface for GaussDB kernel user as below
     * user_login_hook: hook when login in failed or success, it will insert the same place with pgaudit_user_login()
     * ExecutorStart_hook: hook when ExecutorStart is called, which will run the audit process for DML subtables
     * ProcessUtility_hook: hook when when ProcessUtility is called, which will run the DDL auti process
     * light_unified_audit_executor_hook : hook when cn light proxy
     * opfusion_unified_audit_executor_hook: hook for sqlbypass
     * opfusion_unified_audit_flush_logs_hook: hook for sqlbypass
     */
    user_login_hook = NULL;
    ExecutorStart_hook = gs_audit_executor_start_hook;
    ProcessUtility_hook = gsaudit_ProcessUtility_hook;
    light_unified_audit_executor_hook = light_unified_audit_executor;
    opfusion_unified_audit_executor_hook = opfusion_unified_audit_executor;
    opfusion_unified_audit_flush_logs_hook = flush_access_logs;
}

void install_masking_hook()
{
    load_masking_policies_hook                  = load_masking_policies;
    load_masking_policy_actions_hook            = load_masking_actions;
    load_masking_policy_filter_hook             = load_masking_policy_filters;
    check_masking_policy_actions_for_label_hook = check_masking_policy_actions_for_label;
    validate_masking_behaviour_hook             = validate_function_name;
    isMaskingHasObj_hook                        = is_masking_has_object;
    copy_need_to_be_reparse                     = verify_copy_command_is_reparsed;
    gs_verify_labels_by_policy_hook             = is_label_valid_by_policy;
}

void install_label_hook()
{
    load_labels_hook = load_policy_labels;
    query_from_view_hook = set_view_query_state;
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        verify_label_hook = is_label_exist;
    }
}

/*
 * Define GUC variables and install hooks upon module load.
 * NOTE: _PG_init will be invoked(installed) many times 
 * so that func should make sure there is no any multiple-initialized step including locks.
 */
void _PG_init(void)
{
    ereport(DEBUG1, (errmsg("Gsaudit extension init")));

    /* Must be loaded with shared_preload_libraries */
    if (!u_sess->misc_cxt.process_shared_preload_libraries_in_progress) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("Policy plugin must be loaded while shared_preload_libraries")));
    }

    /* for for non-worker and non-threadpoolworker thread, we do nothing */
    if ((t_thrd.role != WORKER) && (t_thrd.role != THREADPOOL_WORKER)) {
        return;
    }
    /*
     * Install our hook functions after saving the existing pointers to
     * preserve the chains.
     */
    reset_policy_thr_hook = destory_thread_variables;
    install_audit_hook();
    install_label_hook();
    install_masking_hook();
}

/*
 * Uninstall hooks and release local memory context
 * NOTE: Now the uninstall hooks process is disabled referring funciton internal_unload_library
 * we just put the release function here to adapt the uninstall process in the feature.
 */
void _PG_fini(void)
{
    ereport(LOG, (errmsg("Gsaudit extension finished")));
}
