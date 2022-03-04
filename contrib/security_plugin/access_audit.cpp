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
 * ---------------------------------------------------------------------------------------
 * 
 * access_audit.cpp
 *        Utility functions for auditing logs, including privileges checking, flush logs.
 * 
 * IDENTIFICATION
 *        src/contrib/security_plugin/access_audit.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/heapam.h"
#include "access_audit.h"
#include "access/sysattr.h"
#include "catalog/gs_auditing_policy.h"
#include "catalog/gs_auditing_policy_acc.h"
#include "catalog/gs_auditing_policy_filter.h"
#include "catalog/gs_auditing_policy_priv.h"
#include "catalog/pg_namespace.h"
#include "commands/user.h"
#include "workload/gscgroup.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "gs_mask_policy.h"
#include "gs_policy_labels.h"
#include "gs_policy_plugin.h"
#include "pgaudit.h"
#include "privileges_audit.h"
#include "rewrite/prs2lock.h"
#include "gs_policy/policy_common.h"
#include "gs_policy/gs_policy_utils.h"
#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy/gs_string.h"

struct log_item {
    log_item(int f = 0, const char *s = ""):first(f), second(s){}
    bool operator <(const log_item &arg) const
    {
        return false;
    }
    int first; /* event type */
    gs_stl::gs_string second; /* event data */
};
using access_event_logs = gs_stl::gs_vector<log_item>;
static THR_LOCAL access_event_logs *access_logs = NULL;

/*
 * Recoed access logs(DDL) for auditing policy. If no logs, create first.
 */
void save_access_logs(int type, const char *event)
{
    if (access_logs == NULL) {
        access_logs = new access_event_logs;
    }
    access_logs->push_back(log_item(type, event));
}

/*
 * Flush the audit logs including ddl & dml into the log system(rsyslog or remote log system) from buffer
 * Note that: we use the audit_result to recognize SQL result info(success or fail) by monitor main process
 */
void flush_access_logs(AuditResult audit_result)
{
    if (access_logs == NULL) {
        return;
    }

    /* go through logs buffer then flush every log item independenly */
    access_event_logs::iterator it = access_logs->begin();
    access_event_logs::iterator eit = access_logs->end();
    for (; it != eit; ++it) {
        if (it->first == AUDIT_POLICY_EVENT) {
            gs_audit_issue_syslog_message("PGAUDIT", it->second.c_str(), AUDIT_POLICY_EVENT, audit_result);
        } else {
            ereport(DEBUG1, (errmsg("flush_access_logs failed as unsupported audit policy type")));
        }
    }

    /* safety clear as access_logs is thread local */
    delete access_logs;
    access_logs = NULL;
}

static void get_from_bitmapset(const Bitmapset *columns,
                               gs_stl::gs_set<int> *column_pos)
{
    if (!bms_is_empty(columns)) {
        Bitmapset *tmpset = bms_copy(columns);
        int col = 0;
        while ((col = bms_first_member(tmpset)) >= 0) {
            int real_pos = col + FirstLowInvalidHeapAttributeNumber - 1;
            if (real_pos >= 0) {
                column_pos->insert(real_pos);
            }
        }
        bms_free_ext(tmpset);
    }
}

static void get_access_columns_pos(gs_stl::gs_set<int> *column_pos,
                                RangeTblEntry *rte)
{
    if (rte->insertedCols) {
        get_from_bitmapset(rte->insertedCols, column_pos);
    }
    if (rte->updatedCols) {
        get_from_bitmapset(rte->updatedCols, column_pos);
    }
    if (rte->selectedCols) {
        get_from_bitmapset(rte->selectedCols, column_pos);
    }
}

void flush_policy_result(const policy_result *pol_result, int access_type,
    const char *as_command, const char *access_name)
{
    if (!pol_result->size()) {
        return;
    }
    char user_name[USERNAME_LEN];
    int rc;
    policy_result::const_iterator it = pol_result->begin();
    policy_result::const_iterator eit = pol_result->end();
    for (; it != eit; ++it) {
        char buff[2048] = {0};
        char session_ip[MAX_IP_LEN] = {0};

        get_session_ip(session_ip, MAX_IP_LEN);
        const char *acc_name = get_access_name(access_type);
        if (!strcasecmp(acc_name, "NONE") && access_name && strlen(access_name) > 0) {
            acc_name = access_name;
        }
        int printed_size = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1,
            "AUDIT EVENT: user name: [%s], app_name: [%s], client_ip: [%s], access type: [%s",
            GetUserName(user_name, sizeof(user_name)), get_session_app_name(), session_ip,
            acc_name);
        securec_check_ss(printed_size, "\0", "\0");
        if (as_command && strlen(as_command) > 0) {
            rc = snprintf_s(buff + printed_size, sizeof(buff) - printed_size, sizeof(buff) - printed_size - 1,
                " %s], policy id: [%lld]", as_command, *(it->first));
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
        } else {
            rc = snprintf_s(buff + printed_size, sizeof(buff) - printed_size, sizeof(buff) - printed_size - 1,
                "], policy id: [%lld]", *(it->first));
            securec_check_ss(rc, "\0", "\0");
            printed_size += rc;
        }
        table_policy_result::const_iterator tit = it->second->begin();
        table_policy_result::const_iterator teit = it->second->end();
        for (; tit != teit; ++tit) {
            int tmp_size = printed_size;
            const AccessPair obj_item = *(tit.first);
            switch (obj_item.second) {
                case O_FUNCTION: {
                    rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1,
                        ", function: [%s]", obj_item.first.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    tmp_size += rc;
                    break;
                }
                case O_VIEW: {
                    rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1,
                        ", view: [%s]", obj_item.first.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    tmp_size += rc;
                    break;
                }
                case O_TABLE: {
                    rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1,
                        ", table: [%s]", obj_item.first.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    tmp_size += rc;
                    break;
                }
                case O_DATABASE: {
                    rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1,
                        ", database: [%s]", obj_item.first.c_str());
                    securec_check_ss(rc, "\0", "\0");
                    tmp_size += rc;
                    break;
                }
                default:
                    break;
            }
            if (!tit->second->size()) {
                save_access_logs(AUDIT_POLICY_EVENT, buff);
                continue;
            }
            rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1, ", columns: {");
            securec_check_ss(rc, "\0", "\0");
            tmp_size += rc;
            column_set::iterator cit = tit->second->begin();
            column_set::iterator ceit = tit->second->end();
            for (int i = 0; cit != ceit; ++cit, ++i) {
                rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1, "%s%s",
                    (i > 0) ? ", " : "", cit->c_str());
                securec_check_ss(rc, "\0", "\0");
                tmp_size += rc;
            }
            rc = snprintf_s(buff + tmp_size, sizeof(buff) - tmp_size, sizeof(buff) - tmp_size - 1, "}");
            securec_check_ss(rc, "\0", "\0");
            tmp_size += rc;
            save_access_logs(AUDIT_POLICY_EVENT, buff);
        }
    }
}

void check_access_table(const policy_set *policy_ids, const char *name, int access_type, int object_type,
                        const char *as_command)
{
    policy_result pol_result;
    int block_behaviour = 0;
    PolicyLabelItem item("", name, "", object_type);
    PolicyLabelItem view_item(0, 0, O_VIEW);
    check_audit_policy_access(&item, &view_item, access_type, policy_ids, &pol_result, 
                              get_policy_accesses(), &block_behaviour);
    flush_policy_result(&pol_result, access_type, as_command);
}

void check_access_table(const policy_set *policy_ids, RangeVar *rel, int access_type, int object_type,
                        const char *as_command, const char *access_name)
{
    if (rel == NULL) {
        return;
    }

    /* PolicyLabelItem construction will append schema oid by relid */
    PolicyLabelItem item;
    PolicyLabelItem view_item(0, 0, O_VIEW);
    gen_policy_labelitem(item, (const ListCell *)rel, object_type);
    policy_result pol_result;
    int block_behaviour = 0;
    check_audit_policy_access(&item, &view_item, access_type, policy_ids, &pol_result,
                              get_policy_accesses(), &block_behaviour);
    flush_policy_result(&pol_result, access_type, as_command, access_name);
}

void audit_open_relation(List *list, Var *col_att, PolicyLabelItem *full_column, bool *is_found)
{
    int relation_pos = col_att->varno ? (col_att->varno - 1) : col_att->varno; /* relation position */
    if (relation_pos >= list_length(list)) {
        return;
    }
    RangeTblEntry *rte = (RangeTblEntry *)list_nth(list, relation_pos);
    if (rte && rte->relid > 0) {
        Relation tbl_rel = relation_open(rte->relid, AccessShareLock);
        if (tbl_rel->rd_rel) {
            /* schema */
            full_column->m_schema = tbl_rel->rd_rel->relnamespace;
        }
        relation_close(tbl_rel, AccessShareLock);

        if (rte->rtekind == RTE_REMOTE_DUMMY) {
            return;
        } else if (rte->rtekind == RTE_SUBQUERY) { 
            /* subquery  in from */
            if (rte->subquery) {
                audit_open_relation(rte->subquery->rtable, col_att, full_column, is_found);
            }
        } else if (rte->relkind == RELKIND_RELATION && !(*is_found)) {
            int col_num = col_att->varattno ? (col_att->varattno - 1) : col_att->varattno;
            if (col_num >= 0 && rte->eref->colnames && list_length(rte->eref->colnames) > col_num) {
                full_column->set_object(rte->relid, O_COLUMN);
                int rc = snprintf_s(full_column->m_column, sizeof(full_column->m_column),
                                    sizeof(full_column->m_column) - 1, "%s",
                                    strVal(list_nth(rte->eref->colnames, col_num)));
                securec_check_ss(rc, "\0", "\0");
                *is_found = true;
            }
        }
    }
}

static void audit_cursor_view(RuleLock *rules, Var *col_att, PolicyLabelItem *full_column,
    PolicyLabelItem *view_full_column)
{
    if (col_att == NULL)
        return;
    List *locks = NIL;
    for (int i = 0; i < rules->numLocks && rules->rules[i]; ++i) {
        RewriteRule *rule = rules->rules[i];
        if (rule->event != CMD_SELECT)
            continue;
        locks = lappend(locks, rule);
    }
    if (locks != NIL) {
        ListCell *item = NULL;
        bool is_found = false;
        foreach (item, locks) {
            RewriteRule *rule = (RewriteRule *)lfirst(item);
            Query *rule_action = (Query*)linitial(rule->actions);
            int column_pos = col_att->varattno ? (col_att->varattno - 1) : col_att->varattno;
            if (column_pos >= 0 && rule_action->targetList && column_pos < list_length(rule_action->targetList)) {
                TargetEntry *old_tle = (TargetEntry *)list_nth(rule_action->targetList, column_pos);
                if (old_tle && old_tle->resno == col_att->varattno) {
                    Node *src_expr = (Node*)old_tle->expr;
                    Var *var = NULL;
                    if (IsA(src_expr, Var)) {
                        var = (Var*)src_expr;
                        audit_open_relation(rule_action->rtable, var, full_column, &is_found);
                        if (view_full_column && old_tle->resname) {
                            int rc = snprintf_s(view_full_column->m_column, sizeof(view_full_column->m_column),
                                                sizeof(view_full_column->m_column) - 1, "%s", old_tle->resname);
                            securec_check_ss(rc, "\0", "\0");         
                        }
                        return;
                    }
                }
            }
        }
    }
    if (locks != NIL) {
        list_free(locks);
    }
}

static void get_column_from_relation(List *colnames, char *col_name, size_t col_name_size, int col_index)
{
    int num = 1;
    ListCell *c = NULL;
    foreach(c, colnames) {
        if (col_index == num) {
            int rc = snprintf_s(col_name, col_name_size, col_name_size - 1, "%s", strVal(lfirst(c)));
            securec_check_ss(rc, "\0", "\0");
            break;
        }
        ++num;
    }
}

/* Append column name to resouce label */
static inline void append_column_name(PolicyLabelItem *full_column, int col_num, RangeTblEntry *rte)
{
    full_column->set_object(rte->relid);
    if (col_num > 0 && rte->eref->colnames && list_length(rte->eref->colnames) > 0) {
        get_column_from_relation(rte->eref->colnames, full_column->m_column, sizeof(full_column->m_column), col_num);
    }
}

void get_fqdn_by_relid(RangeTblEntry *rte, PolicyLabelItem *full_column, Var *col_att,
                       PolicyLabelItem *view_full_column)
{
    /* database */
    if (rte && rte->relid > 0) {
        Relation tbl_rel = relation_open(rte->relid, AccessShareLock);
        if (tbl_rel && tbl_rel->rd_rel) {
            /* schema */
            full_column->m_schema = tbl_rel->rd_rel->relnamespace;
            if (tbl_rel->rd_rules) { /* view */
                audit_cursor_view(tbl_rel->rd_rules, col_att, full_column, view_full_column);
                if (view_full_column) {
                    view_full_column->m_schema = tbl_rel->rd_rel->relnamespace;
                    view_full_column->set_object(rte->relid, O_VIEW);
                }
            } else {
                if (full_column->m_obj_type == O_COLUMN) {
                    append_column_name(full_column, col_att ? col_att->varattno : 0, rte);
                } else {
                    full_column->set_object(rte->relid, O_TABLE);
                }
            }
            relation_close(tbl_rel, AccessShareLock);
        }
        return;
    }
    /* record schema name */
    full_column->m_schema = SchemaNameGetSchemaOid(NULL, true);
    if (rte != NULL && rte->relname) {
        full_column->set_object(rte->relname);
    }
}

bool handle_table_entry(RangeTblEntry *rte, int access_type, const policy_set *policy_ids,
                        const policy_set *security_policy_ids,
                        policy_result *pol_result)
{
    if (rte == NULL || rte->relname == NULL) {
        return false;
    }
    PolicyLabelItem item;
    item.m_obj_type = (rte->relkind == RELKIND_VIEW || rte->relkind == RELKIND_CONTQUERY) ? O_VIEW : O_TABLE;
    get_fqdn_by_relid(rte, &item);
    if ((policy_ids->size() || security_policy_ids->size()) && rte->eref) {
        bool check_column = false;
        if (security_policy_ids->size()) {
            if (CheckSecurityAccess_hook != NULL) {
                check_column = CheckSecurityAccess_hook(security_policy_ids, pol_result, &item, &item, access_type,
                                                        false, NULL, 0);
            }
        }

        int block_behaviour = 0;
        if (check_audit_policy_access(&item, &item, access_type, policy_ids, pol_result, get_policy_accesses(),
            &block_behaviour) && rte->eref->colnames) {
            item.m_obj_type = O_COLUMN;
            gs_stl::gs_set<int> column_pos;
            get_access_columns_pos(&column_pos, rte);
            int ind = 0;
            ListCell   *citem = NULL;
            foreach (citem, rte->eref->colnames) {
                if (column_pos.find(ind) != column_pos.end()) {
                    const char *cname = strVal(lfirst(citem));
                    int rc = snprintf_s(item.m_column, sizeof(item.m_column), sizeof(item.m_column) - 1,
                                        "%s", cname);
                    securec_check_ss(rc, "\0", "\0");
                    if (security_policy_ids->size() && check_column) {
                        if (CheckSecurityAccess_hook != NULL) {
                            CheckSecurityAccess_hook(security_policy_ids, pol_result, &item, &item, 
                                                     access_type, true, NULL, 0);
                        }
                    }
                    if (policy_ids->size()) {
                        check_audit_policy_access(&item, &item, access_type, policy_ids, pol_result,
                            get_policy_accesses(), &block_behaviour);
                    }
                }
                ++ind;
            }
        }
    }
    return true;
}

/*
 * Hook ExecutorStart to get the query text and basic command type for queries
 * that do not contain a table and so can't be idenitified accurately in
 * ExecutorCheckPerms. walk through all the subqueries and check the tables in access_audit process.
 */
void handle_subquery(RangeTblEntry *rte, int commandType, policy_result *pol_result, 
                     _checked_tables *checked_tables, const policy_set *policy_ids,
                     const policy_set *security_policy_ids, int *recursion_deep)
{
    if (*recursion_deep >= 5) {
        return;
    }
    ListCell *lc = NULL;
    foreach(lc, rte->subquery->rtable) {
        RangeTblEntry *sub_rte = (RangeTblEntry *)lfirst(lc);
        if (sub_rte == NULL) {
            break;
        }

        if (sub_rte->rtekind == RTE_REMOTE_DUMMY) {
            continue;
        } else if (sub_rte->rtekind == RTE_SUBQUERY && sub_rte->subquery) {
            /* recursive call handle_subquery till find a table object */
            handle_subquery(sub_rte, commandType, pol_result, checked_tables, policy_ids,
                            security_policy_ids, &(++(*recursion_deep)));
        } else if (sub_rte->relname) {
            /*
             * if the table object has not checked before, then run the handle_table_entry to deal with it
             */
            if (checked_tables->insert(sub_rte->relname).second) {
                CmdType cmd_type = get_rte_commandtype(rte);
                cmd_type = (cmd_type == CMD_UNKNOWN) ? (CmdType)commandType : cmd_type;
                if (!handle_table_entry(sub_rte, cmd_type, policy_ids, security_policy_ids, pol_result)) {
                    continue;
                }

                flush_policy_result(pol_result, cmd_type);
            }
        }
    }
}

void access_audit_policy_run(const List* rtable, CmdType cmd_type)
{
    if (rtable == NULL) {
        return;
    }
    /* filt audit policys by application info */
    policy_set policy_ids;
    IPV6 ip;
    get_remote_addr(&ip);
    FilterData filter_item(get_session_app_name(), ip);
    check_audit_policy_filter(&filter_item, &policy_ids);

    ListCell *lc = NULL;
    policy_set security_policy_ids;
    _checked_tables checked_tables;
    foreach (lc, rtable) {
        /* table object */
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        policy_result pol_result;
        if (rte == NULL || rte->rtekind == RTE_REMOTE_DUMMY) {
            continue;
        }

        if (rte->rtekind == RTE_SUBQUERY && rte->subquery) { /* relation is subquery */
            int recursion_deep = 0;
            handle_subquery(rte, rte->subquery->commandType, &pol_result, &checked_tables, &policy_ids,
                &security_policy_ids, &recursion_deep);
        } else if (rte->relname != NULL &&
            checked_tables.insert(rte->relname).second) { /* verify if table object already checked */
            /* use query plan commandtype here but not get it from rte directly */
            if (!handle_table_entry(rte, cmd_type, &policy_ids, &security_policy_ids, &pol_result)) {
                continue;
            }
            flush_policy_result(&pol_result, cmd_type);
        }
    }

    flush_access_logs(AUDIT_OK);
}

void opfusion_unified_audit_executor(const PlannedStmt *plannedstmt)
{
    /* verify parameter and audit policy */
    if (!u_sess->attr.attr_security.Enable_Security_Policy || u_sess->proc_cxt.IsInnerMaintenanceTools ||
        IsConnFromCoord() || !is_audit_policy_exist_load_policy_info()) {
        return;
    }

    ereport(DEBUG1, (errmsg("opfusion_unified_audit_executor routine enter")));
    if (!plannedstmt) {
        return;
    }
    access_audit_policy_run(plannedstmt->rtable, plannedstmt->commandType);
}