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
 * gs_policy_filter.h
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_filter.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_POLICY_FILTER_H_
#define GS_POLICY_FILTER_H_

#include <string.h>
#include <time.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <boost/functional/hash.hpp>
#include "iprange/iprange.h"

#include "gs_policy/gs_string.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "gs_policy_logical_tree.h"
#include "nodes/pg_list.h"
#include "gs_policy_object_types.h"

enum FilterType
{
	F_UNKNOWN,
	F_ROLE,
	F_IP,
	F_APP
};

struct GsPolicyFilter
{
	GsPolicyFilter(const PolicyLogicalTree &tree = PolicyLogicalTree(), long long pol_id = 0, time_t date = 0)
		:m_tree(tree), m_policy_id(pol_id),m_modify_date(date){}

	GsPolicyFilter(const GsPolicyFilter& arg):m_policy_id(arg.m_policy_id), m_modify_date(arg.m_modify_date)
	{
		m_tree = arg.m_tree;
	}
	GsPolicyFilter& operator =(const GsPolicyFilter& arg)
	{
		if (this == &arg) {
			return *this;
		}
		m_tree = arg.m_tree;
		m_policy_id = arg.m_policy_id;
		m_modify_date = arg.m_modify_date;
		return *this;
	}
	~GsPolicyFilter() {}
	bool is_empty() const {return m_tree.is_empty();}
	bool is_match(const FilterData *arg) {
		return m_tree.match(arg);
	}

	PolicyLogicalTree m_tree;
	long long   m_policy_id;
	time_t      m_modify_date;
};

typedef gs_stl::gs_map<long long/*policy id*/, GsPolicyFilter> gs_policy_filter_map;
typedef gs_stl::gs_map<int/*db id*/, gs_policy_filter_map*> gs_policy_filter_map_by_db;

bool check_policy_filter(const FilterData *arg, policy_set *policy_ids,
                         gs_policy_set *policies, gs_policy_filter_map *filters);

void set_filter(const GsPolicyFilter* item, gs_policy_filter_map *tmp_filters);

int get_filter_type(const char *name);

#endif /* GS_POLICY_FILTER_H_ */
