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
 * gs_policy_logical_tree.cpp
 *    dealing polish-notation format string into policy logical node of policy 
 * plugin for gaussdb kernel
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_logical_tree.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "gs_policy_logical_tree.h"
#include <list>
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/acl.h"

PolicyLogicalNode &PolicyLogicalNode::operator =(const PolicyLogicalNode &arg)
{
    if (&arg == this) {
        return *this;
    }
    m_type = arg.m_type;
    m_apps = arg.m_apps;
    m_roles = arg.m_roles;
    m_has_operator_NOT = arg.m_has_operator_NOT;
    m_left = arg.m_left;
    m_right = arg.m_right;
    m_eval_res = arg.m_eval_res;
    m_ip_range = arg.m_ip_range;
    return *this;
}

bool PolicyLogicalNode::operator <(const PolicyLogicalNode &arg) const
{
    return true;
}

void PolicyLogicalNode::make_eval(const FilterData *filter_item)
{
    switch (m_type) {
        case E_FILTER_ROLE_NODE:          /* filter type is role */
            m_eval_res = (m_roles.find(GetCurrentUserId()) != m_roles.end());
            break;
        case E_FILTER_APP_NODE:          /* filter type is app */
            m_eval_res = (m_apps.find(filter_item->m_app.c_str()) != m_apps.end());
            break;
        case E_FILTER_IP_NODE:           /* filter type is ip */
            m_eval_res = m_ip_range.is_in_range(&filter_item->m_ip);
            break;
        default:
            /* should not get here */
            m_eval_res = false;
            break;  
    }
    if (m_has_operator_NOT) {
        m_eval_res = !m_eval_res;
    }
}

/* Parses polish-notation format string into policy logical node */
static bool parse_values(const gs_stl::gs_string logical_expr_str, int *offset, PolicyLogicalNode *root)
{
    std::size_t found = gs_stl::gs_string::npos;
    char buff[512] = {0};
    int nRet;
    size_t limit_pos = logical_expr_str.find(']', *offset);
    if (limit_pos == gs_stl::gs_string::npos) {
        return false;
    }
    while ((found = logical_expr_str.find(',', *offset)) != gs_stl::gs_string::npos && found < limit_pos) {
        nRet = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%.*s", (int)(found - *offset),
                          logical_expr_str.c_str() + *offset);
        securec_check_ss(nRet, "\0", "\0");
        switch (root->m_type) {
            case E_FILTER_IP_NODE:
                root->m_ip_range.add_range(buff, strlen(buff));
                break;
            case E_FILTER_ROLE_NODE:
                root->m_roles.push_back(isdigit(buff[0]) ? atol(buff) : get_role_oid(buff, true));
            break;
            default:
                root->m_apps.push_back(buff);
                break;
        }
        *offset = found + 1;
    }

    if (*offset < (int)limit_pos) {
        nRet = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "%.*s", (int)(limit_pos - *offset), 
                          logical_expr_str.c_str() + *offset);
        securec_check_ss(nRet, "\0", "\0");
        switch (root->m_type) {
            case E_FILTER_IP_NODE:
                root->m_ip_range.add_range(buff, strlen(buff));
                break;
            case E_FILTER_ROLE_NODE:
            {
                root->m_roles.push_back(isdigit(buff[0]) ? atol(buff) : get_role_oid(buff, true));
            }
            break;
            default:
                root->m_apps.push_back(buff);
                break;
        }
        *offset = limit_pos + 1;
        return true;
    }
    return false;
}

/* C-tor */
PolicyLogicalTree::PolicyLogicalTree() : m_has_ip(false), m_has_role(false), m_has_app(false) { }
/* D-tor */
PolicyLogicalTree::~PolicyLogicalTree()
{
    reset();
}

PolicyLogicalTree &PolicyLogicalTree::operator =(const PolicyLogicalTree &arg)
{
    if (&arg == this) {
        return *this;
    }
    reset();
    for (size_t i = 0; i < arg.m_nodes.size(); ++i) {
        m_nodes.push_back(arg.m_nodes[i]);
    }
    flatten_tree();
    m_has_ip = arg.m_has_ip;
    m_has_role = arg.m_has_role;
    m_has_app = arg.m_has_app;

    return *this;
}

/* Resets logical tree vector */
void PolicyLogicalTree::reset()
{
    m_nodes.clear();
    m_flat_tree.clear();
}

/* Matches logical tree with provided filter item */
bool PolicyLogicalTree::match(const FilterData *filter_item)  
{
    /* optimizations */
    if (m_flat_tree.size() == 0) {
        return false;
    }
    size_t nodes_num = m_flat_tree.size();
    while (nodes_num > 0) {
        int idx = m_flat_tree[nodes_num - 1];
        PolicyLogicalNode *item = &m_nodes[idx];
        switch (item->m_type) {
            case E_AND_NODE:
                item->m_eval_res = m_nodes[item->m_left].m_eval_res && m_nodes[item->m_right].m_eval_res;
                break;
            case E_OR_NODE:
                item->m_eval_res = m_nodes[item->m_left].m_eval_res || m_nodes[item->m_right].m_eval_res;
                break;
            default:
                m_nodes[idx].make_eval(filter_item);
                break;
        }
        nodes_num--;
    }
    return m_nodes[0].m_eval_res;
}

bool PolicyLogicalTree::get_roles(global_roles_in_use *roles)
{
    for (size_t i = 0; i < m_flat_tree.size(); ++i) {
        PolicyLogicalNode *item = &m_nodes[m_flat_tree[i]];
        if (item->m_type == E_FILTER_ROLE_NODE) {
            for (size_t idx = 0; idx < item->m_roles.size(); ++idx) {
                roles->insert(item->m_roles[idx]);
            }
        }
    }
    return !roles->empty();
}

/* Creates node structure for logical tree */
inline void PolicyLogicalTree::create_node(int *idx, EnodeType type, bool has_operator_NOT)
{
    m_nodes.push_back(PolicyLogicalNode(type, has_operator_NOT));
    *idx = (m_nodes.size() - 1);
}

/*
 * Parses (recursively) polish-notation foramt string into logical tree, we support *(and) +(or) !(not) operation here
 * take an example: we change the polish-notation into  operator expression to make it clean
 * *ip[127.0.0.1]roles[10]: ip && role
 * **ip[127.0.0.1]app[javaw]roles[10]: ip && role && app
 * *!ip[127.0.0.1]+app[javaw]roles[10]: (!ip) && (app || role)
 */
bool PolicyLogicalTree::parse_logical_expression_impl(const gs_stl::gs_string logical_expr_str, int *offset,
    int *idx, Edirection direction)
{
    int logical_expr_len = logical_expr_str.size();
    
    bool have_operator_NOT = false;
    while (*offset < logical_expr_len) {
        /* AND/OR node */
        if ((logical_expr_str[*offset] == '*') || (logical_expr_str[*offset] == '+')) {
            create_node(idx, ((logical_expr_str[*offset] == '*') ? E_AND_NODE : E_OR_NODE), have_operator_NOT);
            PolicyLogicalNode *item = &m_nodes.back();
            (*offset)++;
            return  (parse_logical_expression_impl(logical_expr_str, offset, &item->m_left, E_LEFT) &&
                     parse_logical_expression_impl(logical_expr_str, offset, &item->m_right, E_RIGHT));
        } else if (logical_expr_str[*offset] == '!') { /* NOT operator */
            have_operator_NOT = true;
            (*offset)++;
        } else if (logical_expr_str[*offset] == 'i') { /* IP filter node */
            create_node(idx, E_FILTER_IP_NODE, have_operator_NOT);
            *offset += 3; /* 3: skip 'ip['  */
            have_operator_NOT = false; /* forget that we have met NOT operator */
            return parse_values(logical_expr_str, offset, &m_nodes.back());
        } else if (logical_expr_str[*offset] == 'r') { /* ROLE filter node */
            create_node(idx, E_FILTER_ROLE_NODE, have_operator_NOT);
            *offset += 6; /* 6: skip 'roles[' */
            have_operator_NOT = false; /* forget that we have met NOT operator */
            return parse_values(logical_expr_str, offset, &m_nodes.back());
        } else if (logical_expr_str[*offset] == 'a') { /* APPLICATION filter node */
            create_node(idx, E_FILTER_APP_NODE, have_operator_NOT);
            *offset += 4; /* 4: skip 'app[' */
            have_operator_NOT = false; /* forget that we have met NOT operator */
            return parse_values(logical_expr_str, offset, &m_nodes.back());
        } else {
            /* unsupported node or out of stream */
        }
    }

    return false;
}

/* Parses polish-notation format string into logical tree (wrapper around real implementation function) */
bool PolicyLogicalTree::parse_logical_expression(const gs_stl::gs_string logical_expr_str)
{
    int offset = 0;
    m_flat_tree.clear();
    m_nodes.clear();
    int idx = 0;
    if (logical_expr_str.size() > 0 && parse_logical_expression_impl(logical_expr_str, &offset, &idx, E_LEFT)) {
        flatten_tree();
        return true;
    }
    /* in case of error reset tree */
    return false;
}

/* Flattens logical tree into vector for later logical evaluation */
void PolicyLogicalTree::flatten_tree()
{
    if (m_nodes.size() == 0) {
        return;
    }
    gs_stl::gs_vector<int> nodes_stack;
    nodes_stack.push_back(0);
    while (nodes_stack.size() > 0) {
        int idx = nodes_stack.front();
        if (idx < (int)m_nodes.size()) {
            PolicyLogicalNode cur_node = m_nodes[idx];
            nodes_stack.pop_front();
            switch (cur_node.m_type) {
                case E_AND_NODE:
                case E_OR_NODE:
                    m_flat_tree.push_back(idx);
                    nodes_stack.push_back(cur_node.m_left);
                    nodes_stack.push_back(cur_node.m_right);
                    break;
                case E_FILTER_IP_NODE:
                case E_FILTER_ROLE_NODE:
                case E_FILTER_APP_NODE:
                    m_flat_tree.push_back(idx);
                    break;
                default:
                    Assert(true);
                    break;
            }
        }
    }
}

/*
 * check_apps_intersect
 * 
 * check two apps set have interscetion
 */
bool PolicyLogicalTree::check_apps_intersect(string_sort_vector *apps_first, string_sort_vector *apps_second)
{
    if (apps_first == NULL || apps_second == NULL) {
        return false;
    }

    for (size_t i = 0; i < apps_first->size(); ++i) {
        if (apps_second->find((*apps_first)[i]) != apps_second->end()) {
            return true;
        }
    }
    return false;
}

/*
 * check_roles_intersect
 * 
 * check two role set have interscetion
 */
bool PolicyLogicalTree::check_roles_intersect(oid_sort_vector *roles_first, oid_sort_vector *roles_second)
{
    if (roles_first == NULL || roles_second == NULL) {
        return false;
    }
    for (size_t i = 0; i < roles_first->size(); ++i) {
        if (roles_second->find((*roles_first)[i]) != roles_second->end()) {
            return true;
        }
    }
    return false;
}

/*
 * has_intersect
 * 
 * check this logical tree has intersect whith logicaltree(arg)
 */
bool PolicyLogicalTree::has_intersect(PolicyLogicalTree *arg)
{
    if (m_flat_tree.empty()) {
        return true;
    }
    /* flag intersection for each filter item */
    bool is_app_intersect = false;
    bool is_ip_intersect = false;
    bool is_role_intersect = false;

    /* is this logical contains app/ip/role */
    bool has_app = false;
    bool has_ip = false;
    bool has_role = false;

    /* arg logical tree vs this logical tree */
    bool has_arg_app = false;
    bool has_arg_ip = false;
    bool has_arg_role = false;

    for (size_t idx = 0; idx < m_flat_tree.size(); ++idx) {
        PolicyLogicalNode *item = &m_nodes[m_flat_tree[idx]];
        /*
         * because ',' in logical tree mean 'AND', so there is at least one intersect when all flags are true
         * for example: filterA: **ip[xxx.xxx.1.1]app[jdbc]role[dev]
         * and          filterB: **ip[xxx.xxx.1.2]app[jdbc]role[dev] are no intersection,
         * because filterA:only dev using jdbc with ip 123.123.1.1
         *         filterB:only dev using jdbc with ip 123.123.1.2
         * filterA and filterB are different user scenarioes
         */
        if (is_app_intersect && is_ip_intersect && is_role_intersect) {
            break;
        }
        /* ignore operator node */
        if (item->m_type == E_AND_NODE || item->m_type == E_OR_NODE) {
            continue;
        }
        has_app = has_app || (item->m_type == E_FILTER_APP_NODE);
        has_ip = has_ip || (item->m_type == E_FILTER_IP_NODE);
        has_role = has_role || (item->m_type == E_FILTER_ROLE_NODE);

        for (size_t arg_idx = 0; arg_idx < arg->m_flat_tree.size(); ++arg_idx) {
            PolicyLogicalNode *arg_item = &arg->m_nodes[arg->m_flat_tree[arg_idx]];
            switch (arg_item->m_type) {
                case E_FILTER_APP_NODE:
                {
                    has_arg_app = true;
                    if (item->m_type == arg_item->m_type && !is_app_intersect) {
                        /* for now 'NOT' operator is not supported */
                        is_app_intersect = check_apps_intersect(&item->m_apps, &arg_item->m_apps);
                    }
                }
                break;
                case E_FILTER_ROLE_NODE:
                {
                    has_arg_role = true;
                    if (item->m_type == arg_item->m_type && !is_role_intersect) {
                        /* for now 'NOT' operator is not supported */
                        is_role_intersect = check_roles_intersect(&item->m_roles, &arg_item->m_roles);
                    }
                }
                break;
                case E_FILTER_IP_NODE:
                {
                    has_arg_ip = true;
                    if (item->m_type == arg_item->m_type && !is_ip_intersect) {
                        /* for now 'NOT' operator is not supported */
                        is_ip_intersect = item->m_ip_range.is_intersect(&arg_item->m_ip_range);
                    }
                }
                break;
                /* ignore operator node */
                case E_AND_NODE:
                case E_OR_NODE:
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Unknown logical filter node")));
                    break;
            }
        }
    }
    /* if filter not contains means for all scenario
     * ip/app/role:
     *     if one logical contains but the other not contains, they must have intersection
     *     if both logicals are not contains, they must have intersection
     */
    is_app_intersect = !has_app || !has_arg_app || is_app_intersect;
    is_ip_intersect = !has_ip || !has_arg_ip || is_ip_intersect;
    is_role_intersect = !has_role || !has_arg_role || is_role_intersect;

    return is_app_intersect && is_ip_intersect && is_role_intersect;
}
