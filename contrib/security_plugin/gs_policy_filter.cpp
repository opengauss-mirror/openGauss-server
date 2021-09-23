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
 * gs_policy_filter.cpp
 * checking policy filter of policy plugin for gaussdb kernel
 *
 * IDENTIFICATION
 * contrib/security_plugin/gs_policy_filter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "gs_policy_filter.h"
#include "access/htup.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "access/heapam.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "commands/user.h"

int get_filter_type(const char *name)
{
    if (!strcasecmp(name, "ip")) {
        return F_IP;
    }
    if (!strcasecmp(name, "roles")) {
        return F_ROLE;
    }
    if (!strcasecmp(name, "app")) {
        return F_APP;
    }
    return B_UNKNOWN;
}

bool check_policy_filter(const FilterData* arg, policy_set *policy_ids, gs_policy_set *policies, gs_policy_filter_map *filters)
{
    if (policies == NULL) {
        return false;
    }
    if (filters && filters->size()) {
        gs_policy_filter_map::iterator it = filters->begin(), eit = filters->end();
        for (; it != eit; ++it) {
            /* by type */
            gs_policy_set::iterator pit = policies->find(*(it->first));
            if (pit == policies->end())
                continue; /* does not exist policy */
            /* get policy item */
            PolicyPair pol_item;
            pit->get_pol(pol_item);
            GsPolicyFilter& filter = *(it->second);
            if (filter.is_empty() || filter.is_match(arg)) {
                policy_ids->insert(pol_item);
            }
        }
    } else {
        gs_policy_set::iterator it = policies->begin(), eit = policies->end();
        for (; it != eit; ++it) {
            PolicyPair pol_item;
            it->get_pol(pol_item);
            policy_ids->insert(pol_item);
        }
    }
    return !policy_ids->empty();
}

void set_filter(const GsPolicyFilter *item, gs_policy_filter_map *tmp_filters)
{
    (*tmp_filters)[item->m_policy_id] = *item;
}

