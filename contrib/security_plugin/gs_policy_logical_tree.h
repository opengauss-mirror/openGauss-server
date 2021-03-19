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
 * gs_policy_logical_tree.h
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_logical_tree.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PG_POLICY_LOGICAL_TREE_H
#define PG_POLICY_LOGICAL_TREE_H 

#include <string>
#include <vector>
#include <set>
#include <memory>
#include <unordered_set>
#include "iprange/iprange.h"
#include "gs_policy/gs_string.h"
#include "gs_policy_object_types.h"
#include "gs_policy/gs_vector.h"

typedef unsigned int Oid;

struct FilterData {
    FilterData(const char *app = "", const IPV6& _ip = IPV6()): m_app(app), m_ip(_ip){}
    gs_stl::gs_string m_app;
    IPV6 m_ip;
};

enum EnodeType {
    E_AND_NODE,
    E_OR_NODE,
    E_FILTER_IP_NODE,
    E_FILTER_APP_NODE,
    E_FILTER_ROLE_NODE,
    E_UNKNOWN_NODE
};

enum Edirection {
    E_LEFT,
    E_RIGHT
};

typedef gs_stl::gs_vector<gs_stl::gs_string, true> string_sort_vector;
typedef gs_stl::gs_vector<Oid, true> oid_sort_vector;

struct PolicyLogicalNode {
    PolicyLogicalNode(EnodeType type = E_UNKNOWN_NODE, bool has_not = false)
        : m_type(type), m_has_operator_NOT(has_not), m_left(0), m_right(0), m_eval_res(false){}

    PolicyLogicalNode(const PolicyLogicalNode *other)
        : m_type(other->m_type), m_apps(other->m_apps), m_has_operator_NOT(other->m_has_operator_NOT),
          m_left(other->m_left), m_right(other->m_right), m_eval_res(other->m_eval_res),
          m_roles(other->m_roles), m_ip_range(other->m_ip_range){}

    PolicyLogicalNode &operator = (const PolicyLogicalNode &arg);
    bool operator < (const PolicyLogicalNode &arg) const;
    /* evaluates filter_item and stores the result in eval_res */
    void make_eval(const FilterData *filter_item);

    /* node type (logical operator or some filter) */
    EnodeType m_type;
    /* values for leaves kind of nodes */
    string_sort_vector m_apps;
    /* whether operator NOT should be applied */
    bool m_has_operator_NOT;
    int m_left;  /* left son */
    int m_right; /* right son */
 
    /* for evaluation when walking through tree */
    bool m_eval_res;
    /* Role handle */
    oid_sort_vector m_roles; /* sorted vector (has no duplicate item) */
    /* ip range */
    IPRange m_ip_range;
};

class PolicyLogicalTree {
public:
    /* C-tor */
    PolicyLogicalTree();
    /* D-tor */
    ~PolicyLogicalTree();
    PolicyLogicalTree(const PolicyLogicalTree &arg) 
    {
        *this = arg;
    }

    PolicyLogicalTree &operator = (const PolicyLogicalTree &arg);

    /* Constructs logical tree; returns true upon success; false in case of parsing error */
    bool parse_logical_expression(const gs_stl::gs_string logical_expr_str);
    /* Resets logical tree */
    void reset();
    /* Checks whether logical tree matches given filter item */
    bool match(const FilterData *filter_item);
    bool is_empty() const {return m_flat_tree.empty();}
    bool get_roles(global_roles_in_use *roles);
    bool has_intersect(PolicyLogicalTree *arg);
private:
    /* holds tree nodes - for easy cleaning */
    gs_stl::gs_vector<PolicyLogicalNode> m_nodes;
    gs_stl::gs_vector<int> m_flat_tree;

    /* needed for recusive tree construction */
    bool parse_logical_expression_impl(const gs_stl::gs_string logical_expr_str, int *offset,
                                       int *idx, Edirection direction);
    /* creates a single tree node */
    inline void create_node(int *idx, EnodeType type, bool has_operator_not);
    /* prepares flat tree for evalueation */
    void flatten_tree();
    bool check_apps_intersect(string_sort_vector*, string_sort_vector*);
    bool check_roles_intersect(oid_sort_vector*, oid_sort_vector*);
    /* optimiszations */
    bool m_has_ip;
    bool m_has_role;
    bool m_has_app;
};

#endif

