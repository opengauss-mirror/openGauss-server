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
 * nodegroups.cpp
 *     Variables and functions used in multiple node group optimizer
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/nodegroups.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "optimizer/nodegroups.h"

#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "workload/memctl.h"
#include "access/hash.h"
#include "utils/dynahash.h"

/* ------------------------------------------------------------------------- */
/*  Data structure                                                         */
/* ------------------------------------------------------------------------- */
/* Context to find all base relation range table entry */
typedef struct FindBaserelRTEContext {
    List* baserel_rte_list;
} FindBaserelRTEContext;

/* ------------------------------------------------------------------------- */
/*  Variables                                                              */
/* ------------------------------------------------------------------------- */
/* ------------------------------------------------------------------------- */
/*  Static functions declaration                                           */
/* ------------------------------------------------------------------------- */
/* Generic functions */
static bool get_baserel_rte_list(Node* node, FindBaserelRTEContext* context);
static List* get_baserel_rte_list_from_query(Query* query);

/* Private functions */
static Bitmapset* ng_node_oid_array_to_id_bms(Oid* members, int nmembers, char node_type);

/* ------------------------------------------------------------------------- */
/*  Generic functions                                                      */
/* ------------------------------------------------------------------------- */
/*
 * get_baserel_rte_list
 *     get base relation range table entry from a node
 *
 * @param (in) node:
 *     the source node
 * @param (in) context:
 *     the context to find all base relation range table entry
 *
 * @return:
 *     context marker of results, true means walker works well
 */
static bool get_baserel_rte_list(Node* node, FindBaserelRTEContext* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Query)) {
        Query* query = (Query*)node;
        List* rte_list = get_baserel_rte_list_from_query((Query*)query);
        context->baserel_rte_list = list_concat_unique(context->baserel_rte_list, rte_list);

        if (query_tree_walker(query, (bool (*)())get_baserel_rte_list, (void*)context, 0)) {
            return true;
        }
    }

    return expression_tree_walker(node, (bool (*)())get_baserel_rte_list, (void*)context);
}

/*
 * get_baserel_rte_list_from_query
 *     get base relation range table entry from a Query node
 *
 * @param (in) query:
 *     the source Query node
 *
 * @return:
 *     the list of base relation range table entry
 */
static List* get_baserel_rte_list_from_query(Query* query)
{
    List* baserel_rte_list = NIL;

    ListCell* lc = NULL;
    foreach (lc, query->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        /* Other parts will be searched by another get_baserel_rte_list */
        if (RTE_RELATION == rte->rtekind) {
            /* Append to result lists if current rte is base relation */
            baserel_rte_list = lappend(baserel_rte_list, rte);
        }
    }

    return baserel_rte_list;
}

/* ------------------------------------------------------------------------- */
/*  Private functions                                                      */
/* ------------------------------------------------------------------------- */
/*
 * ng_node_oid_array_to_id_bms
 *     convert node oid array to node index bitmap set
 *
 * @param (in) members:
 *     the node oid array
 * @param (in) nmembers:
 *     the length of "members"
 * @param (in) node_type:
 *     the node type, CN or DN
 *
 * @return:
 *     the node index bitmap set
 */
static Bitmapset* ng_node_oid_array_to_id_bms(Oid* members, int nmembers, char node_type)
{
    Bitmapset* bms_nodeids = NULL;

    for (int i = 0; i < nmembers; ++i) {
        /* transform node oid to node id */
        int nodeId = PGXCNodeGetNodeId(members[i], node_type);
        bms_nodeids = bms_add_member(bms_nodeids, nodeId);
    }

    return bms_nodeids;
}

/*
 * ng_node_oid_array_to_id_bms_skip_null
 *     convert node oid array to node index bitmap set
 *     node not found in handles means group add new node but handles not updated
 *     if installation group's node not found,we skip it
 *
 * @param (in) members:
 *     the node oid array
 * @param (in) nmembers:
 *     the length of "members"
 * @param (in) node_type:
 *     the node type, CN or DN
 *
 * @return:
 *     the node index bitmap set
 */
static Bitmapset* ng_node_oid_array_to_id_bms_skip_null(Oid* members, int nmembers, char node_type)
{
    Bitmapset* bms_nodeids = NULL;

    for (int i = 0; i < nmembers; ++i) {
        /* transform node oid to node id */
        int nodeId = PGXCNodeGetNodeId(members[i], node_type);
        if (nodeId < 0) {
            continue;
        }
        bms_nodeids = bms_add_member(bms_nodeids, nodeId);
    }

    return bms_nodeids;
}

/* ------------------------------------------------------------------------- */
/*  Public functions                                                       */
/* ------------------------------------------------------------------------- */
/* ----------
 * General functions
 * ----------
 */
/*
 * ng_init_nodegroup_optimizer
 *     initializer of node group optimizer to set initial values of:
 *     (1) u_sess->opt_cxt.in_redistribution_group_distribution
 *     (2) u_sess->opt_cxt.compute_permission_group_distribution
 *     (3) u_sess->opt_cxt.query_union_set_group_distribution
 *     (4) u_sess->opt_cxt.is_multiple_nodegroup_scenario
 *     (5) u_sess->opt_cxt.different_nodegroup_count
 *     if it's a multiple node group scenario, we should shut down SMP here
 *
 * @param (in) query:
 *     the Query tree
 *
 * @return: void
 */
void ng_init_nodegroup_optimizer(Query* query)
{
    /*
     * Get 'in redistribution' group distribution,
     * it will be NULL if there is no 'in redistribution' group.
     */
    u_sess->opt_cxt.in_redistribution_group_distribution = NULL;
    u_sess->opt_cxt.in_redistribution_group_distribution = ng_get_in_redistribution_group_distribution();

    /* Get installation group distribution */
    Distribution* installation_group_distribution = ng_get_installation_group_distribution();

    /*
     * Init compute permission group distribution
     * As we use u_sess->opt_cxt.compute_permission_group_distribution in CNG_MODE_COSTBASED_OPTIMAL mode only
     * so, in this mode, we set u_sess->opt_cxt.compute_permission_group_distribution,
     * and in other modes, we set u_sess->opt_cxt.compute_permission_group_distribution to NULL
     */
    ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();
    u_sess->opt_cxt.compute_permission_group_distribution = NULL;
    if (CNG_MODE_COSTBASED_OPTIMAL == cng_mode) {
        u_sess->opt_cxt.compute_permission_group_distribution = ng_get_compute_permission_group_distribution();
    }

    /*
     * Get and set query union group distribution and check wheather base relations are in same group
     * 1. Get base relations' range table entry list
     * 2. Construct query union-set group
     */
    FindBaserelRTEContext* findBaserelRTEContext = (FindBaserelRTEContext*)palloc0(sizeof(FindBaserelRTEContext));
    (void)get_baserel_rte_list((Node*)query, findBaserelRTEContext);
    bool baserels_in_same_group = true;
    u_sess->opt_cxt.query_union_set_group_distribution =
        ng_get_query_union_set_group_distribution(findBaserelRTEContext->baserel_rte_list, &baserels_in_same_group);
    pfree_ext(findBaserelRTEContext);

    /* Check wheather it is a multiple node group scenario */
    u_sess->opt_cxt.is_multiple_nodegroup_scenario = true;
    u_sess->opt_cxt.different_nodegroup_count = 2;
    if (baserels_in_same_group) {
        Distribution* default_computing_group_distribution = ng_get_default_computing_group_distribution();
        if (ng_is_same_group(
                u_sess->opt_cxt.query_union_set_group_distribution, default_computing_group_distribution)) {
            u_sess->opt_cxt.is_multiple_nodegroup_scenario = false;

            if (ng_is_same_group(u_sess->opt_cxt.query_union_set_group_distribution, installation_group_distribution)) {
                u_sess->opt_cxt.different_nodegroup_count--;
            }
        }
    }

    u_sess->opt_cxt.single_node_distribution = NULL;
    /* Init dn gather's single node distribution */
    if (IS_PGXC_COORDINATOR && u_sess->attr.attr_sql.enable_dngather 
        && bms_num_members(u_sess->opt_cxt.query_union_set_group_distribution->bms_data_nodeids) >= 1
        && u_sess->opt_cxt.query_dop <= 1) {
        u_sess->opt_cxt.single_node_distribution = ng_get_random_single_dn_distribution(u_sess->opt_cxt.query_union_set_group_distribution);
        if (!ng_is_same_group(u_sess->opt_cxt.query_union_set_group_distribution, u_sess->opt_cxt.single_node_distribution)) {
            u_sess->opt_cxt.is_multiple_nodegroup_scenario = true;
            u_sess->opt_cxt.different_nodegroup_count++;
        }
    }

    u_sess->opt_cxt.enable_nodegroup_explain = u_sess->opt_cxt.different_nodegroup_count > 1;
}

/*
 * ng_backup_nodegroup_options
 *     Backup options used by node group optimizer,
 *     as there may be some procedures may call standard_planner recursively.
 *     We also need to backup u_sess->opt_cxt.query_dop for SMP here.
 *
 * @params (out) all params:
 *     the params to backup options
 */
void ng_backup_nodegroup_options(bool* p_is_multiple_nodegroup_scenario,
    int* p_different_nodegroup_count, Distribution** p_in_redistribution_group_distribution,
    Distribution** p_compute_permission_group_distribution, Distribution** p_query_union_set_group_distribution,
    Distribution** p_single_node_distribution)
{
    *p_is_multiple_nodegroup_scenario = u_sess->opt_cxt.is_multiple_nodegroup_scenario;
    *p_different_nodegroup_count = u_sess->opt_cxt.different_nodegroup_count;
    *p_in_redistribution_group_distribution = u_sess->opt_cxt.in_redistribution_group_distribution;
    *p_compute_permission_group_distribution = u_sess->opt_cxt.compute_permission_group_distribution;
    *p_query_union_set_group_distribution = u_sess->opt_cxt.query_union_set_group_distribution;
    *p_single_node_distribution = u_sess->opt_cxt.single_node_distribution;
}

/*
 * ng_restore_nodegroup_options
 *     Restore options used by node group optimizer,
 *     as there may be some procedures may call standard_planner recursively.
 *     These options was backup by ng_backup_nodegroup_options.
 *     We also need to reset u_sess->opt_cxt.query_dop for SMP here.
 *
 * @params (in) all params:
 *     the params holds the original options
 */
void ng_restore_nodegroup_options(bool p_is_multiple_nodegroup_scenario,
    int p_different_nodegroup_count, Distribution* p_in_redistribution_group_distribution,
    Distribution* p_compute_permission_group_distribution, Distribution* p_query_union_set_group_distribution,
    Distribution* p_single_node_distribution)
{
    /* for further explain */
    u_sess->opt_cxt.enable_nodegroup_explain = u_sess->opt_cxt.different_nodegroup_count > 1;

    /* restore nodegroup optimizer options */
    u_sess->opt_cxt.is_multiple_nodegroup_scenario = p_is_multiple_nodegroup_scenario;
    u_sess->opt_cxt.different_nodegroup_count = p_different_nodegroup_count;
    u_sess->opt_cxt.in_redistribution_group_distribution = p_in_redistribution_group_distribution;
    u_sess->opt_cxt.compute_permission_group_distribution = p_compute_permission_group_distribution;
    u_sess->opt_cxt.query_union_set_group_distribution = p_query_union_set_group_distribution;
    u_sess->opt_cxt.single_node_distribution = p_single_node_distribution;
}

/*
 * ng_get_computing_nodegroup_mode
 *     get computing node group mode from GUC variables
 *
 * @return:
 *     the mode of current computing node group mode
 */
ComputingNodeGroupMode ng_get_computing_nodegroup_mode()
{
    if (OidIsValid(lc_replan_nodegroup)) {
        return CNG_MODE_FORCE;
    } else if (0 == strncasecmp(u_sess->attr.attr_sql.expected_computing_nodegroup,
                                CNG_OPTION_OPTIMAL, strlen(CNG_OPTION_OPTIMAL))) {
        if (in_logic_cluster())
            return CNG_MODE_COSTBASED_QUERY;
        else
            return CNG_MODE_COSTBASED_OPTIMAL;
    } else if (0 == strncasecmp(u_sess->attr.attr_sql.expected_computing_nodegroup,
                                CNG_OPTION_QUERY, strlen(CNG_OPTION_QUERY))) {
        return CNG_MODE_COSTBASED_QUERY;
    } else {
        if (u_sess->attr.attr_sql.enable_nodegroup_debug && !t_thrd.postmaster_cxt.forceNoSeparate) {
            return CNG_MODE_FORCE;
        } else {
            return CNG_MODE_COSTBASED_EXPECT;
        }
    }
}

/* ----------
 * Get general node groups
 * ----------
 */
/*
 * ng_get_installation_group_name
 *     get group name of installation group
 *
 * @return:
 *     the name of installation group
 */
char* ng_get_installation_group_name()
{
    return PgxcGroupGetInstallationGroup();
}

/*
 * ng_get_installation_group_oid
 *     get group oid of installation group
 *
 * @return:
 *     the oid of installation group
 */
Oid ng_get_installation_group_oid()
{
    char* installation_group_name = ng_get_installation_group_name();

    Oid oid = ng_get_group_groupoid(installation_group_name);
    return oid;
}

/*
 * ng_get_installation_group_nodeids
 *     get node index bitmap set of installation group
 *
 * @return:
 *     the node index bitmap set of installation group
 */
Bitmapset* ng_get_installation_group_nodeids()
{
    Oid oid = ng_get_installation_group_oid();

    Bitmapset* bms_nodeids = ng_get_group_nodeids(oid);
    return bms_nodeids;
}

/*
 * ng_get_installation_group_distribution
 *     get Distribution information of installation group
 *
 * @return:
 *     the Distribution information of installation group
 */
Distribution* ng_get_installation_group_distribution()
{
    Oid oid = ng_get_installation_group_oid();

    Distribution* distribution = ng_get_group_distribution(oid);
    return distribution;
}

/*
 * ng_get_installation_group_exec_node
 *     get exec nodes of installation group
 *
 * @return:
 *     the exec nodes of installation group
 */
ExecNodes* ng_get_installation_group_exec_node()
{
    Distribution* distribution = ng_get_installation_group_distribution();
    ExecNodes* exec_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_REPLICATED, RELATION_ACCESS_READ);
    return exec_nodes;
}

/*
 * ng_get_u_sess->opt_cxt.compute_permission_group_distribution
 *     get Distribution information of compute permission group
 *
 * @return:
 *     the Distribution information of compute permission group
 */
Distribution* ng_get_compute_permission_group_distribution()
{
    /* Just for initialize of computing permission group */
    if (u_sess->opt_cxt.compute_permission_group_distribution == NULL) {
        Oid user_oid = GetUserId();
        List* group_oid_list = GetNodeGroupOidCompute(user_oid);
        Distribution* distribution = NULL;
        ListCell* lc = NULL;
        foreach (lc, group_oid_list) {
            Oid group_oid = (Oid)lfirst_oid(lc);
            Distribution* d = ng_get_group_distribution(group_oid);
            distribution = ng_get_union_distribution_recycle(distribution, d);
        }

        if (distribution == NULL || bms_is_empty(distribution->bms_data_nodeids)) {
            if (IS_PGXC_COORDINATOR) {
                /* We should report error here when there are no COMPUTE permission group */
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("No privilage assigned to user %u.", user_oid)));
            } else {
                u_sess->opt_cxt.compute_permission_group_distribution = ng_get_single_node_group_distribution();
            }
        } else {
            u_sess->opt_cxt.compute_permission_group_distribution = distribution;
        }
    }

    return u_sess->opt_cxt.compute_permission_group_distribution;
}

/*
 * ng_get_u_sess->opt_cxt.query_union_set_group_distribution
 *     Get Distribution information of union set group of all base relations in a query.
 *     Another function is mark wheather these base relations in a same node group.
 *     If all base relations have no distribution information (such as all relations are
 *     foreign tables), we set query union set as same as installation group.
 *
 * @param (in) baserel_rte_list:
 *     the range table entry list of all base relations
 * @param (out) baserels_in_same_group:
 *     mark wheather these base relations in same node group
 *
 * @return:
 *     the Distribution information of union set group of all base relations in a query
 */
Distribution* ng_get_query_union_set_group_distribution(List* baserel_rte_list, bool* baserels_in_same_group)
{
    Distribution* union_distribution = NULL;
    *baserels_in_same_group = true;

    /* Step 1: check base tables' storage group */
    ListCell* lc = NULL;
    foreach (lc, baserel_rte_list) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        AssertEreport(rte->rtekind == RTE_RELATION, MOD_OPT, "");

        /* Only check plain relaion */
        if (RELKIND_VIEW == rte->relkind || RELKIND_CONTQUERY == rte->relkind) {
            continue;
        }

        Distribution* rel_distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);

        if (union_distribution == NULL) {
            union_distribution = rel_distribution;
        } else {
            if (!ng_is_same_group(union_distribution, rel_distribution)) {
                *baserels_in_same_group = false;
                union_distribution = ng_get_union_distribution_recycle(union_distribution, rel_distribution);
            }
        }
    }

    /* No base table, or all base tables have no distribution information */
    if (union_distribution == NULL || bms_is_empty(union_distribution->bms_data_nodeids)) {
        *baserels_in_same_group = true;
        union_distribution = ng_get_installation_group_distribution();
    }

    return union_distribution;
}

/*
 * ng_get_u_sess->opt_cxt.query_union_set_group_distribution
 *     get Distribution information of query union set node group
 *
 * @return:
 *     the Distribution information of query union set node group
 */
Distribution* ng_get_query_union_set_group_distribution()
{
    /* If query union-set group is not exists, shift it to installation group */
    if (u_sess->opt_cxt.query_union_set_group_distribution == NULL) {
        u_sess->opt_cxt.query_union_set_group_distribution = ng_get_installation_group_distribution();
    }

    return u_sess->opt_cxt.query_union_set_group_distribution;
}

/*
 * ng_get_expected_computing_group_distribution
 *     Get Distribution information of expected computing node group.
 *     If the 'expected_computing_nodegroup' is not exists, we will use query union-set node group.
 *
 * @return:
 *     the Distribution information of expected computing node group
 */
Distribution* ng_get_expected_computing_group_distribution()
{
    ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();
    if (CNG_MODE_COSTBASED_OPTIMAL == cng_mode || CNG_MODE_COSTBASED_QUERY == cng_mode) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("No expected computing node group in 'optimal' or 'query' mode")));
    }

    Distribution* distribution = NULL;
    if (OidIsValid(lc_replan_nodegroup))
        distribution = ng_get_group_distribution(lc_replan_nodegroup);
    else
        distribution = ng_get_group_distribution(u_sess->attr.attr_sql.expected_computing_nodegroup);

    /*
     * (1) If expected_computing_nodegroup is not exists, shift it to query union-set group
     * (2) If the user have no COMPUTE permission on expected_computing_nodegroup, report error
     */
    if (InvalidOid == distribution->group_oid) {
        ereport(DEBUG1, (errmodule(MOD_OPT), errmsg("Expected computing node group does not exist.")));

        distribution = ng_copy_distribution(ng_get_query_union_set_group_distribution());
    }

    return distribution;
}

/*
 * ng_get_default_computing_group_exec_node
 *     get exec nodes of default computing group
 *
 * @return:
 *     the exec nodes of default computing group
 */
ExecNodes* ng_get_default_computing_group_exec_node()
{
    Distribution* distribution = ng_get_default_computing_group_distribution();
    ExecNodes* exec_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_REPLICATED, RELATION_ACCESS_READ);
    return exec_nodes;
}

/*
 * ng_get_in_redistribution_group_oid
 *     get group oid of in-redistribution group
 *
 * @return:
 *     the group oid of in-redistribution group
 */
Oid ng_get_in_redistribution_group_oid()
{
    char* in_redistribution_group_name = PgxcGroupGetInRedistributionGroup();
    if (NULL == in_redistribution_group_name) {
        return InvalidOid;
    } else {
        return ng_get_group_groupoid(in_redistribution_group_name);
    }
}

/*
 * ng_get_u_sess->opt_cxt.in_redistribution_group_distribution
 *     get Distribution information of in-redistribution group
 *
 * @return:
 *     the Distribution information of in-redistribution group
 */
Distribution* ng_get_in_redistribution_group_distribution()
{
    Oid group_oid = ng_get_in_redistribution_group_oid();
    if (InvalidOid == group_oid) {
        return NULL;
    } else {
        Distribution* distribution = NewDistribution();
        distribution->group_oid = group_oid;
        distribution->bms_data_nodeids = ng_get_group_nodeids(group_oid);
        return distribution;
    }
}

/*
 * ng_get_redist_dest_group_oid
 *     get destination group oid in redistribution
 *
 * @return:
 *     the group oid of redistribution distination group
 */
Oid ng_get_redist_dest_group_oid()
{
    if (in_logic_cluster()) {
        return PgxcGroupGetRedistDestGroupOid();
    } else {
        return ng_get_installation_group_oid();
    }
}

/*
 * ng_get_default_computing_group_distribution
 *     get default node group's Distribution information of corresponding mode
 *
 * @return:
 *     the default node group's Distribution information of corresponding mode
 */
Distribution* ng_get_default_computing_group_distribution()
{
    ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();

    switch (cng_mode) {
        case CNG_MODE_COSTBASED_OPTIMAL:
            return ng_get_compute_permission_group_distribution();
        case CNG_MODE_COSTBASED_QUERY:
            return ng_get_query_union_set_group_distribution();
        case CNG_MODE_COSTBASED_EXPECT:
        case CNG_MODE_FORCE:
            return ng_get_expected_computing_group_distribution();
        default:
            break;
    }

    AssertEreport(false, MOD_OPT, "get default computing group distribution failed");
    return NULL;
}

Distribution* ng_get_single_node_distribution() {
    return u_sess->opt_cxt.single_node_distribution;
}

/*
 * ng_get_correlated_subplan_group_distribution
 *     Get Distribution information for a correlated sub-plan.
 *     In each computing mode, this group will be different, see code for detail.
 *
 * @return:
 *     the Distribution information for a correlated sub-plan
 */
Distribution* ng_get_correlated_subplan_group_distribution()
{
    ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();

    switch (cng_mode) {
        case CNG_MODE_COSTBASED_OPTIMAL: {
            Distribution* distribution_1 = ng_get_compute_permission_group_distribution();
            Distribution* distribution_2 = ng_get_query_union_set_group_distribution();
            return ng_get_union_distribution(distribution_1, distribution_2);
        }
        case CNG_MODE_COSTBASED_QUERY: {
            return ng_get_query_union_set_group_distribution();
        }
        case CNG_MODE_COSTBASED_EXPECT:
        case CNG_MODE_FORCE: {
            Distribution* distribution_1 = ng_copy_distribution(ng_get_query_union_set_group_distribution());
            Distribution* distribution_2 = ng_get_expected_computing_group_distribution();
            return ng_get_union_distribution_recycle(distribution_1, distribution_2);
        }
        default:
            break;
    }

    AssertEreport(false, MOD_OPT, "get correlated subplan group distribution failed");
    return NULL;
}

/*
 * ng_get_max_computable_group_distribution
 *     Get Distribution information for a "exec on everywhere" node.
 *
 * @return:
 *     the Distribution information for a "exec on everywhere" node
 */
Distribution* ng_get_max_computable_group_distribution()
{
    return ng_get_correlated_subplan_group_distribution();
}

/* ----------
 * Get distribution information of a node group
 * ----------
 */
/*
 * ng_get_group_groupoid
 *     get group oid from group name
 *
 * @param (in) group_name:
 *     the name of group
 *
 * @return:
 *     the oid of group
 */
Oid ng_get_group_groupoid(const char* group_name)
{
    if (group_name == NULL || IS_PGXC_DATANODE) {
        return InvalidOid;
    } else {
        return get_pgxc_groupoid(group_name);
    }
}

/*
 * ng_get_group_group_name
 *     get group name from group oid
 *
 * @param (in) group_oid:
 *     the oid of group
 *
 * @return:
 *     the name of group
 */
char* ng_get_group_group_name(Oid group_oid)
{
    if (InvalidOid == group_oid) {
        return "GenGroup";
    } else {
        return get_pgxc_groupname(group_oid);
    }
}

/*
 * ng_get_dist_group_name
 *     get group name from distribution

 */
char* ng_get_dist_group_name(Distribution *distribution) 
{
    if (IS_PGXC_COORDINATOR && u_sess->attr.attr_sql.enable_dngather 
        && u_sess->opt_cxt.is_dngather_support && distribution->group_oid == InvalidOid) {
        char *sep = "";
        StringInfo node_names = makeStringInfo();
        NameData nodename = {{0}};
        int node_no = -1;
        while ((node_no = bms_next_member(distribution->bms_data_nodeids, node_no)) >= 0) { 
            appendStringInfo(node_names,
                "%s%s",
                sep, 
                get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, PGXC_NODE_DATANODE), &nodename));
                sep = ", ";
        }    

        return node_names->data;
    }

    return ng_get_group_group_name(distribution->group_oid);
}


/*
 * ng_get_group_nodeids
 *     Get node index bitmap set from group oid
 *     If the group oid is invalid, return all nodes
 *
 * @param (in) groupoid:
 *     the oid of group
 *
 * @return:
 *     the node index bitmap set
 */
Bitmapset* ng_get_group_nodeids(const Oid groupoid)
{
    Bitmapset* bms_nodeids = NULL;

    if (InvalidOid == groupoid) {
        List* nodeid_list = GetAllDataNodes();
        return ng_convert_to_nodeids(nodeid_list);
    }

    bms_nodeids = ngroup_info_hash_search(groupoid);
    if (!bms_nodeids) {
        /* First check if we already have default nodegroup set */
        Oid* members = NULL;
        int nmembers = get_pgxc_groupmembers(groupoid, &members);

        /* in logic cluster case, elastic_nodegroup can contain no members */
        if (nmembers == 0 && in_logic_cluster() && groupoid == ng_get_group_groupoid(VNG_OPTION_ELASTIC_GROUP))
            return NULL;

        Assert(nmembers > 0);

        /*
         * Creating a bitmap from array.
         * Notice : installation group's group_members in pgxc_group may be not match with u_sess->pgxc_cxt.dn_handles
         * when node changed in online expansion, so we skip it here and this currently looks ok.
         *
         * Note: we only have to do the special processing for installation node group, because in
         * cluster expansion stage(adding node), only installation group's node will change.
         */
        Oid ng_installation_group_oid = ng_get_installation_group_oid();
        if (groupoid == ng_installation_group_oid) {
            bms_nodeids = ng_node_oid_array_to_id_bms_skip_null(members, nmembers, PGXC_NODE_DATANODE);
        } else {
            const char *group_parent = get_pgxc_groupparent(groupoid);
            Oid group_parent_oid = InvalidOid;
            if (group_parent != NULL) {
                group_parent_oid = get_pgxc_groupoid(group_parent, false);
            }
            if (group_parent_oid != InvalidOid && group_parent_oid == ng_installation_group_oid) {
                bms_nodeids = ng_node_oid_array_to_id_bms_skip_null(members, nmembers, PGXC_NODE_DATANODE);
            } else {
                bms_nodeids = ng_node_oid_array_to_id_bms(members, nmembers, PGXC_NODE_DATANODE);
            }
        }
        pfree_ext(members);

        ngroup_info_hash_insert(groupoid, bms_nodeids);
    }

    return bms_nodeids;
}

/*
 * ng_get_group_distribution
 *     get Distribution information from group oid
 *
 * @param (in) groupoid:
 *     the oid of group
 *
 * @return:
 *     the Distribute information of a group
 */
Distribution* ng_get_group_distribution(const Oid groupoid)
{
    Distribution* distribution = NewDistribution();
    distribution->group_oid = groupoid;
    distribution->bms_data_nodeids = ng_get_group_nodeids(groupoid);

    /* special case for elastic_group */
    if (distribution->bms_data_nodeids == NULL) {
        distribution->group_oid = InvalidOid;
        distribution->bms_data_nodeids = ng_get_group_nodeids(InvalidOid);
    }

    return distribution;
}

/*
 * ng_get_group_distribution
 *     get Distribution information from group name
 *
 * @param (in) group_name:
 *     the name of group
 *
 * @return:
 *     the Distribute information of a group
 */
Distribution* ng_get_group_distribution(const char* group_name)
{
    Oid group_oid = ng_get_group_groupoid(group_name);
    Distribution* distribution = ng_get_group_distribution(group_oid);
    return distribution;
}

/* ----------
 * Whether the table is distributed foreign table in logic cluster.
 * ----------
 */
/*
 * need_get_installation_group
 *     Whether the table need to get installation group.
 *
 * @param (in) tableoid:
 *     the oid of the relation
 * @param (in) relkind:
 *     the relation kind of the relation
 *
 * @return:
 *     Whether the table need to get installation group.
 */
static bool need_get_installation_group(Oid tableoid, char relkind)
{
    /* System table  will use installation group */
    if (is_sys_table(tableoid))
        return true;

    /* Normal tables  will not use installation group */
    if (relkind == RELKIND_RELATION || relkind == RELKIND_INDEX || relkind == RELKIND_MATVIEW) {
        return false;
    }

    if (in_logic_cluster()) {
        /* Distributed foreign tables(in pgxc_class)  will not use installation group in logic cluster.*/
        if ((RELKIND_FOREIGN_TABLE == relkind || RELKIND_STREAM == relkind) && is_pgxc_class_table(tableoid))
            return false;
    }

    /* sequence table or foreign table in non logic cluster will use installation group */
    return true;
}

/* ----------
 * Get distribution information of base relation
 * ----------
 */
/*
 * ng_get_baserel_groupoid
 *     get group oid of a base relation
 *
 * @param (in) tableoid:
 *     the oid of the relation
 * @param (in) relkind:
 *     the relation kind of the relation
 *
 * @return:
 *     the group oid where the relation lacated
 */
Oid ng_get_baserel_groupoid(Oid tableoid, char relkind)
{
    /* Fast query shipping to dn */
    if (IS_PGXC_DATANODE) {
        return InvalidOid;
    }

    if (need_get_installation_group(tableoid, relkind)) {
        return ng_get_installation_group_oid();
    }

    /* Get the base relation oid of the index */
    if (relkind == RELKIND_INDEX) {
        tableoid = IndexGetRelation(tableoid, false);
    }

    Oid group_oid = get_pgxc_class_groupoid(tableoid);

    return group_oid;
}

/*
 * ng_get_baserel_data_nodeids
 *     get node index bitmap set of a base relation
 *
 * @param (in) tableoid:
 *     the oid of the relation
 * @param (in) relkind:
 *     the relation kind of the relation
 *
 * @return:
 *     the node index bitmap set where the base relation located
 */
Bitmapset* ng_get_baserel_data_nodeids(Oid tableoid, char relkind)
{
    Bitmapset* bms_nodeids = NULL;

    /* Fast query shipping to dn */
    if (IS_PGXC_DATANODE) {
        int nodeid = u_sess->pgxc_cxt.PGXCNodeId;
        if (nodeid >= 0) {
            bms_nodeids = bms_add_member(bms_nodeids, nodeid);
        } else {
            bms_nodeids = NULL;
        }
        return bms_nodeids;
    }

    if (need_get_installation_group(tableoid, relkind)) {
        return ng_get_installation_group_nodeids();
    }

    /* Get the base relation oid of the index */
    if (relkind == RELKIND_INDEX) {
        tableoid = IndexGetRelation(tableoid, false);
    }

    /* Get oid array of data nodes */
    Oid* members = NULL;
    int nmembers = get_pgxc_classnodes(tableoid, &members);
    AssertEreport(nmembers > 0, MOD_OPT, "");

    /* Creating a index bitmap from array */
    bms_nodeids = ng_node_oid_array_to_id_bms(members, nmembers, PGXC_NODE_DATANODE);

    return bms_nodeids;
}

/*
 * ng_get_baserel_data_distribution
 *     get Distribution information of a base relation
 *
 * @param (in) tableoid:
 *     the oid of the relation
 * @param (in) relkind:
 *     the relation kind of the relation
 *
 * @return:
 *     the Distribution information where the base relation located
 */
Distribution* ng_get_baserel_data_distribution(Oid tableoid, char relkind)
{
    Distribution* distribution = NewDistribution();
    distribution->group_oid = ng_get_baserel_groupoid(tableoid, relkind);
    distribution->bms_data_nodeids = ng_get_baserel_data_nodeids(tableoid, relkind);
    return distribution;
}

/*
 * ng_get_baserel_num_data_nodes
 *     get number of data nodes of a base relation
 *
 * @param (in) tableoid:
 *     the oid of the relation
 * @param (in) relkind:
 *     the relation kind of the relation
 *
 * @return:
 *     the number of data nodes where the base relation located
 */
unsigned int ng_get_baserel_num_data_nodes(Oid tableoid, char relkind)
{
    Bitmapset* bms_nodeids = ng_get_baserel_data_nodeids(tableoid, relkind);
    unsigned int num_datanodes = bms_num_members(bms_nodeids);
    num_datanodes = 0 == num_datanodes ? 1 : num_datanodes;
    return num_datanodes;
}

/* ----------
 * Get distribution information of path and plan
 * ----------
 */
/*
 * ng_get_dest_nodeids
 *     get data node index bitmap set of a Path
 *
 * @param (in) path:
 *     the source path
 *
 * @return:
 *     the data node index bitmap set of the Path
 */
Bitmapset* ng_get_dest_nodeids(Path* path)
{
    AssertEreport(path != NULL, MOD_OPT, "");
    if (IsA(path, StreamPath)) {
        StreamPath* stream_path = (StreamPath*)path;
        return stream_path->consumer_distribution.bms_data_nodeids;
    } else {
        return path->distribution.bms_data_nodeids;
    }
}

/*
 * ng_get_dest_distribution
 *     get Distribution information of a Path node
 *
 * @param (in) path:
 *     the target Path node
 *
 * @return:
 *     the Distribution information of the Path node
 */
Distribution* ng_get_dest_distribution(Path* path)
{
    AssertEreport(path != NULL, MOD_OPT, "");

    if (IsA(path, StreamPath)) {
        StreamPath* stream_path = (StreamPath*)path;
        return &(stream_path->consumer_distribution);
    } else {
        return &(path->distribution);
    }
}

/*
 * ng_get_dest_nodeids
 *     get data node index bitmap set of a Plan node
 *
 * @param (in) plan:
 *     the target Plan node
 *
 * @return:
 *     the data node index bitmap set of a Plan node
 */
Bitmapset* ng_get_dest_nodeids(Plan* plan)
{
    AssertEreport(plan != NULL, MOD_OPT, "");

    if (plan->exec_type == EXEC_ON_DATANODES || plan->exec_type == EXEC_ON_ALL_NODES) {
        ExecNodes* en = ng_get_dest_execnodes(plan);
        return ng_convert_to_nodeids(en);
    } else {
        return NULL;
    }
}

/*
 * ng_get_dest_distribution
 *     get Distribution information of a Plan node
 *
 * @param (in) plan:
 *     the target Plan node
 *
 * @return:
 *     the Distribution information of a Plan node
 */
Distribution* ng_get_dest_distribution(Plan* plan)
{
    if (plan->exec_type == EXEC_ON_DATANODES || plan->exec_type == EXEC_ON_ALL_NODES) {
        ExecNodes* en = ng_get_dest_execnodes(plan);
        return &en->distribution;
    } else {
        Distribution* distribution = NewDistribution();
        distribution->group_oid = InvalidOid;
        distribution->bms_data_nodeids = NULL;
        return distribution;
    }
}

/*
 * ng_get_dest_execnodes
 *     get the destination exec nodes of a Plan,
 *     mainly get the consumer side exec nodes for Stream or RemoteQuery
 *
 * @param (in) plan:
 *     the target Plan node
 *
 * @return:
 *     the destination exec nodes of a Plan
 */
ExecNodes* ng_get_dest_execnodes(Plan* plan)
{
    if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        Stream* stream_plan = (Stream*)plan;
        return stream_plan->consumer_nodes;
    } else if (IsA(plan, RemoteQuery)) {
        RemoteQuery* remote_query_plan = (RemoteQuery*)plan;
        if (NULL != remote_query_plan->exec_nodes) {
            return remote_query_plan->exec_nodes;
        } else {
            return plan->exec_nodes;
        }
    } else {
        return plan->exec_nodes;
    }
}

/*
 * ng_get_dest_num_data_nodes
 *     get the number of data nodes of a Path node
 *
 * @param (in) path:
 *     the target Path node
 *
 * @return:
 *     the number of data nodes of a Path node
 */
unsigned int ng_get_dest_num_data_nodes(Path* path)
{
    AssertEreport(path != NULL, MOD_OPT, "");

#ifndef ENABLE_MULTIPLE_NODES
    return 1;
#endif

    if (!IS_STREAM_PLAN) {
        return 1;
    }

    if (ng_is_all_in_installation_nodegroup_scenario()) {
        switch (path->pathtype) {
            case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
            case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
                return u_sess->pgxc_cxt.NumDataNodes;
            default:
                break;
        }
    }

    Bitmapset* bms_nodeids = ng_get_dest_nodeids(path);
    unsigned int num_data_nodes = (unsigned int)bms_num_members(bms_nodeids);
    if (num_data_nodes == 0) {
        elog(DEBUG1, "[ng_get_dest_num_data_nodes] num of data nodes is 0");
        num_data_nodes = 1;
    }
    return num_data_nodes;
}

/*
 * ng_get_dest_num_data_nodes
 *     get the number of data nodes of a Plan node
 *
 * @param (in) plan:
 *     the target Plan node
 *
 * @return:
 *     the number of data nodes of a Plan node
 */
unsigned int ng_get_dest_num_data_nodes(Plan* plan)
{
    AssertEreport(plan != NULL, MOD_OPT, "");

#ifndef ENABLE_MULTIPLE_NODES
    return 1;
#endif

    if (!IS_STREAM_PLAN) {
        return 1;
    }

    if (ng_is_all_in_installation_nodegroup_scenario() && !ng_enable_nodegroup_explain()) {
        switch (plan->type) {
            case T_SeqScan:
            case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
            case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            case T_IndexScan:
            case T_IndexOnlyScan:
            case T_CStoreIndexScan:
            case T_DfsScan:
            case T_DfsIndexScan:
            case T_BitmapIndexScan:
            case T_BitmapHeapScan:
            case T_CStoreIndexCtidScan:
            case T_CStoreIndexHeapScan:
            case T_TidScan:
            case T_PartIterator:
            case T_SubqueryScan:
                return u_sess->pgxc_cxt.NumDataNodes;
            default:
                break;
        }
    }

    if (plan->exec_type == EXEC_ON_DATANODES) {
        Bitmapset* bms_nodeids = ng_get_dest_nodeids(plan);
        unsigned int num_data_nodes = (unsigned int)bms_num_members(bms_nodeids);
        if (num_data_nodes == 0) {
            elog(DEBUG1, "[ng_get_dest_num_data_nodes] num of data nodes is 0");
            num_data_nodes = 1;
        }
        return num_data_nodes;
    } else {
        return 1;
    }
}

/*
 * ng_get_dest_num_data_nodes
 *     get the number of data nodes from a RelOptInfo
 *
 * @param (in) rel:
 *     the target RelOptInfo node
 *
 * @return:
 *     the number of data nodes from a RelOptInfo
 */
unsigned int ng_get_dest_num_data_nodes(RelOptInfo* rel)
{
    AssertEreport(rel != NULL, MOD_OPT, "");

#ifndef ENABLE_MULTIPLE_NODES
    return 1;
#endif

    if (!IS_STREAM_PLAN) {
        return 1;
    }

    unsigned int num_data_nodes = u_sess->pgxc_cxt.NumDataNodes;

    if (rel->cheapest_total_path != NULL) {
        Path* path = (Path*)linitial(rel->cheapest_total_path);
        num_data_nodes = ng_get_dest_num_data_nodes(path);
    } else if (rel->subplan) {
        num_data_nodes = ng_get_dest_num_data_nodes(rel->subplan);
    }

    if (num_data_nodes == 0) {
        elog(DEBUG1, "[ng_get_dest_num_data_nodes] num of data nodes is 0");
        num_data_nodes = 1;
    }
    return num_data_nodes;
}

/*
 * ng_get_dest_num_data_nodes
 *     get the number of data nodes from a RelOptInfo
 *
 * @param (in) root:
 *     the PlannerInfo node of a query block
 * @param (in) rel:
 *     the target RelOptInfo node
 *
 * @return:
 *     the number of data nodes from a RelOptInfo
 */
unsigned int ng_get_dest_num_data_nodes(PlannerInfo* root, RelOptInfo* rel)
{
    AssertEreport(rel != NULL, MOD_OPT, "");

#ifndef ENABLE_MULTIPLE_NODES
    return 1;
#endif

    if (rel->num_data_nodes != 0) {
        return rel->num_data_nodes;
    }

    if (!IS_STREAM_PLAN) {
        rel->num_data_nodes = 1;
        return 1;
    }

    unsigned int num_data_nodes = u_sess->pgxc_cxt.NumDataNodes;

    switch (rel->rtekind) {
        case RTE_RELATION: {
            /* RELKIND_FOREIGN_TABLE or not */
            RangeTblEntry* rte = root->simple_rte_array[rel->relid];
            num_data_nodes = ng_get_baserel_num_data_nodes(rte->relid, rte->relkind);
            break;
        }
        case RTE_SUBQUERY: {
            Plan* subplan = rel->subplan;
            if (subplan != NULL) {
                num_data_nodes = ng_get_dest_num_data_nodes(subplan);
            } else {
                elog(DEBUG1, "[ng_get_dest_num_data_nodes] RTE_SUBQUERY has no subplan");
                num_data_nodes = u_sess->pgxc_cxt.NumDataNodes;
            }
            break;
        }
        case RTE_JOIN: {
            AssertEreport(rel->cheapest_total_path != NULL, MOD_OPT, "");
            Path* path = (Path*)linitial(rel->cheapest_total_path);
            AssertEreport(path != NULL, MOD_OPT, "");
            num_data_nodes = ng_get_dest_num_data_nodes(path);
            break;
        }
        case RTE_FUNCTION:
        case RTE_VALUES:
        case RTE_CTE:
            num_data_nodes = u_sess->pgxc_cxt.NumDataNodes;
            break;
        case RTE_RESULT:
            num_data_nodes = 1;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Unexpected range table entry type.")));
            break;
    }

    if (num_data_nodes == 0) {
        elog(DEBUG1, "[ng_get_dest_num_data_nodes] num of data nodes is 0");
        num_data_nodes = 1;
    }

    rel->num_data_nodes = num_data_nodes;
    return num_data_nodes;
}

/*
 * ng_get_dest_locator_type
 *     get the destination locator type of a Plan node
 *
 * @param (in) plan:
 *     the target Plan node
 *
 * @return:
 *     the destination locator type of a Plan node
 */
char ng_get_dest_locator_type(Plan* plan)
{
    if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        Stream* stream_plan = (Stream*)plan;
        return stream_plan->consumer_nodes->baselocatortype;
    } else {
        return plan->exec_nodes->baselocatortype;
    }
}

/*
 * ng_get_dest_distribute_keys
 *     get the destination distribute keys of a Plan node
 *
 * @param (in) plan:
 *     the target Plan node
 *
 * @return:
 *     the destination distribute keys of a Plan node
 */
List* ng_get_dest_distribute_keys(Plan* plan)
{
    if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        Stream* stream_plan = (Stream*)plan;
        return stream_plan->distribute_keys;
    } else {
        return plan->distributed_keys;
    }
}

/* ----------
 * Distribution data structure management
 * ----------
 */
/*
 * ng_copy_distribution
 *     copy a Distribution
 *
 * @param (in) src_distribution:
 *     the source Distribution
 *
 * @return:
 *     the destination Distribution
 */
Distribution* ng_copy_distribution(Distribution* src_distribution)
{
    AssertEreport(src_distribution != NULL, MOD_OPT, "");

    Distribution* dest_distribution = NewDistribution();

    ng_copy_distribution(dest_distribution, src_distribution);

    return dest_distribution;
}

/*
 * ng_copy_distribution
 *     copy a Distribution
 *
 * @param (in) dest_distribution:
 *     the destination Distribution
 * @param (in) src_distribution:
 *     the source Distribution
 *
 * @return: void
 */
void ng_copy_distribution(Distribution* dest_distribution, const Distribution* src_distribution)
{
    dest_distribution->group_oid = src_distribution->group_oid;
    /* bms_copy will palloc a new bms */
    if (dest_distribution->bms_data_nodeids != NULL)
        bms_free(dest_distribution->bms_data_nodeids);
    dest_distribution->bms_data_nodeids = bms_copy(src_distribution->bms_data_nodeids);
}

/*
 * ng_set_distribution
 *     set a Distribution by another Distribution
 *
 * @param (in) dest_distribution:
 *     the destination Distribution
 * @param (in) src_distribution:
 *     the source Distribution
 *
 * @return: void
 */
void ng_set_distribution(Distribution* dest_distribution, Distribution* src_distribution)
{
    dest_distribution->group_oid = src_distribution->group_oid;
    dest_distribution->bms_data_nodeids = src_distribution->bms_data_nodeids;
}

/*
 * ng_get_overlap_distribution
 *     get overlap Distribution of two Distribution node
 *
 * @param (in) distribution_1:
 *     first Distribution(s) to overlap, it could be NULL
 *     if this parameter is NULL, reture the second Distribution as overlap
 * @param (in) distribution_2:
 *     second Distribution(s) to overlap
 *
 * @return:
 *     the overlap Distribution
 */
Distribution* ng_get_overlap_distribution(Distribution* distribution_1, Distribution* distribution_2)
{
    AssertEreport(distribution_2 != NULL, MOD_OPT, "");

    Distribution* distribution = NewDistribution();

    if (distribution_1 == NULL) {
        ng_copy_distribution(distribution, distribution_2);
        return distribution;
    }

    if (ng_is_same_group(distribution_1, distribution_2)) {
        Distribution* better_distribution = InvalidOid != distribution_1->group_oid ? distribution_1 : distribution_2;
        ng_copy_distribution(distribution, better_distribution);
        return distribution;
    } else {
        distribution->group_oid = InvalidOid;
        distribution->bms_data_nodeids =
            bms_intersect(distribution_1->bms_data_nodeids, distribution_2->bms_data_nodeids);
        return distribution;
    }
}

/*
 * ng_get_union_distribution
 *     get union of two Distribution(s)
 *
 * @param (in) distribution_1:
 *     first Distribution(s) to union, it could be NULL
 *     if this parameter is NULL, reture the second Distribution as union
 * @param (in) distribution_2:
 *     second Distribution(s) to union
 *
 * @return:
 *     the copied union Distribution and leaving their inputs untouched
 */
Distribution* ng_get_union_distribution(Distribution* distribution_1, Distribution* distribution_2)
{
    AssertEreport(distribution_2 != NULL, MOD_OPT, "");

    Distribution* distribution = NewDistribution();

    if (distribution_1 == NULL) {
        ng_copy_distribution(distribution, distribution_2);
        return distribution;
    }

    if (ng_is_same_group(distribution_1, distribution_2)) {
        Distribution* better_distribution = InvalidOid != distribution_1->group_oid ? distribution_1 : distribution_2;
        ng_copy_distribution(distribution, better_distribution);
        return distribution;
    } else {
        distribution->group_oid = InvalidOid;
        distribution->bms_data_nodeids = bms_union(distribution_1->bms_data_nodeids, distribution_2->bms_data_nodeids);
        return distribution;
    }
}

/*
 * ng_get_union_distribution_recycle
 *     get union of two Distribution(s)
 *
 * @param (in) distribution_1:
 *     first Distribution(s) to union, it could be NULL
 *     if this parameter is NULL, reture the second Distribution as union
 * @param (in) distribution_2:
 *     second Distribution(s) to union
 *
 * @return:
 *     the copied union Distribution and recycle their inputs
 */
Distribution* ng_get_union_distribution_recycle(Distribution* distribution_1, Distribution* distribution_2)
{
    Distribution* result = ng_get_union_distribution(distribution_1, distribution_2);
    DestroyDistribution(distribution_1);
    DestroyDistribution(distribution_2);
    return result;
}

Distribution* ng_get_random_single_dn_distribution(Distribution* distribution)
{
    Distribution* result_distribution = NewDistribution();
    int random = pickup_random_datanode(bms_num_members(distribution->bms_data_nodeids));
    int dn_oid = -1;
    for (int i = 0; i <= random; ++i) {
        dn_oid = bms_next_member(distribution->bms_data_nodeids, dn_oid);
    }
    AssertEreport(dn_oid >= 0, MOD_OPT, "");
    result_distribution->bms_data_nodeids = bms_add_member(result_distribution->bms_data_nodeids, dn_oid);
    result_distribution->group_oid = InvalidOid;
    return result_distribution;
}

/* ----------
 * convert functions between:
 * ----------
 */
/*
 * ng_convert_to_nodeid
 *     convert node oid to node index
 */
int ng_convert_to_nodeid(Oid nodeoid)
{
    int nodeid = PGXCNodeGetNodeId(nodeoid, PGXC_NODE_DATANODE);
    return nodeid;
}

/*
 * ng_convert_to_nodeoid
 *     convert node index to node oid
 */
Oid ng_convert_to_nodeoid(int nodeid)
{
    Oid nodeoid = PGXCNodeGetNodeOid(nodeid, PGXC_NODE_DATANODE);
    return nodeoid;
}

/*
 * ng_convert_to_nodeid_list
 *     convert node index bitmap set to node index list
 */
List* ng_convert_to_nodeid_list(Bitmapset* bms_nodeids)
{
    List* nodeid_list = NIL;

    for (int nodeid = -1; (nodeid = bms_next_member(bms_nodeids, nodeid)) >= 0;) {
        nodeid_list = lappend_int(nodeid_list, nodeid);
    }

    return nodeid_list;
}

/*
 * ng_convert_to_nodeids
 *     convert exec nodes to node index bitmap set
 */
Bitmapset* ng_convert_to_nodeids(ExecNodes* exec_nodes)
{
    if (exec_nodes == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("exec_nodes could not be NULL")));
    }
    return ng_convert_to_nodeids(exec_nodes->nodeList);
}

/*
 * ng_convert_to_nodeids
 *     convert node index list to node index bitmap set
 */
Bitmapset* ng_convert_to_nodeids(List* nodeid_list)
{
    Bitmapset* bms_nodeids = NULL;

    ListCell* lc = NULL;
    foreach (lc, nodeid_list) {
        int nodeid = lfirst_int(lc);
        bms_nodeids = bms_add_member(bms_nodeids, nodeid);
    }

    return bms_nodeids;
}

/*
 * ng_convert_to_distribution
 *     convert node index list to Distribution
 */
Distribution* ng_convert_to_distribution(List* nodeid_list)
{
    Distribution* distribution = NewDistribution();
    distribution->group_oid = InvalidOid;
    distribution->bms_data_nodeids = ng_convert_to_nodeids(nodeid_list);
    return distribution;
}

/*
 * ng_convert_to_distribution
 *     convert exec nodes to Distribution,
 *     this function concern on the exec nodes of an ExecNodes,
 *     not the data nodes in the ExecNodes
 */
Distribution* ng_convert_to_distribution(ExecNodes* exec_nodes)
{
    /* Make a new instance and set data nodeids from ExecNodes::nodeList */
    Distribution* distribution = ng_convert_to_distribution(exec_nodes->nodeList);

    /* Set group oid */
    if (bms_equal(distribution->bms_data_nodeids, exec_nodes->distribution.bms_data_nodeids)) {
        distribution->group_oid = exec_nodes->distribution.group_oid;
    } else {
        distribution->group_oid = InvalidOid;
    }

    return distribution;
}

/*
 * ng_convert_to_exec_nodes
 *     convert a Distribution to an ExecNodes
 *
 * @param (in) distribution:
 *     the Distribution information, provide exec nodes and data nodes for the ExecNodes
 * @param (in) locator_type:
 *     the locator type of this ExecNodes
 * @param (in) access_type:
 *     the access type of this ExecNodes
 *
 * @return:
 *     the new ExecNodes
 */
ExecNodes* ng_convert_to_exec_nodes(Distribution* distribution, char locator_type, RelationAccessType access_type)
{
    ExecNodes* execnodes = makeNode(ExecNodes);

    if (distribution == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("distribution could not be NULL")));
    }

    execnodes->nodeList = ng_convert_to_nodeid_list(distribution->bms_data_nodeids);
    ng_copy_distribution(&execnodes->distribution, distribution);
    execnodes->baselocatortype = locator_type;
    execnodes->accesstype = access_type;
    execnodes->primarynodelist = NIL;
    execnodes->en_expr = NULL;

    return execnodes;
}

/*
 * Check whether the distribution exists in the distributions. 
 */
bool is_distribution_exists(List* distributions, Distribution* distribution) 
{
    bool find = false;
    ListCell* lc = NULL;
    foreach(lc, distributions) {
        Distribution* tmp_distribution = (Distribution*)lfirst(lc);
        if (tmp_distribution != NULL && ng_is_same_group(tmp_distribution, distribution)) {
            find = true;
            break;
        }
    }
    
    return find;
}

/*
 * Heuristic methods
 */
/*
 * ng_get_join_candidate_distribution_list
 *     Get candidate Distribution list of a Join
 *     Three candidates will be taken:
 *     (1) the node group of outer path
 *     (2) the node group of inner path
 *     (3) the default computing group of current computing mode
 *     Same group will be throw away
 *
 * @param (in) outer_path:
 *     the outer path
 * @param (in) inner_path:
 *     the inner path
 *
 * @return:
 *     the candidates list
 */
List* ng_get_join_candidate_distribution_list(Path* outer_path, Path* inner_path, DistrbutionPreferenceType type) 
{
    List* candidate_list = NIL;
    Distribution* distribution = NULL;

    /* Default compute distribution. */
    if (type == DPT_ALL || type == DPT_SHUFFLE) {
        distribution = ng_get_default_computing_group_distribution();
        candidate_list = lappend(candidate_list, distribution);
        ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();
        if (CNG_MODE_FORCE == cng_mode) {
            return candidate_list;
        }
    } 

    /* Single node distribution. */
    if (type == DPT_ALL || type == DPT_SINGLE) {
        distribution = ng_get_single_node_distribution();
        if (distribution != NULL && !is_distribution_exists(candidate_list, distribution)) {
            candidate_list = lappend(candidate_list, distribution);
        }
    }

    /* Join outer's distribution. */
    distribution = ng_get_dest_distribution(outer_path);
    if (!is_distribution_exists(candidate_list, distribution)) {
        candidate_list = lappend(candidate_list, distribution);
    }

    /* Join inner's distribution. */
    distribution = ng_get_dest_distribution(inner_path);
    if (!is_distribution_exists(candidate_list, distribution)) {
        candidate_list = lappend(candidate_list, distribution);
    }
    return candidate_list;
}

/*
 * ng_get_join_candidate_distribution_list
 *     Get candidate Distribution list of a Join
 *     It will be different for correlated query block and un-correlated query block
 *     (1) for correlated query block, it's join should be in a special node group for correlated sub-plan
 *     (2) for un-correlated query block, it will use heuristic methods to get candidate group node list
 *
 * @param (in) outer_path:
 *     the outer path
 * @param (in) inner_path:
 *     the inner path
 *
 * @return:
 *     the candidates list
 */
List* ng_get_join_candidate_distribution_list(Path* outer_path, Path* inner_path, bool is_correlated, DistrbutionPreferenceType type)
{
    if (is_correlated) {
        Distribution* installation_distribution = ng_get_correlated_subplan_group_distribution();
        List* candidate_list = list_make1(installation_distribution);
        return candidate_list;
    } else {
        return ng_get_join_candidate_distribution_list(outer_path, inner_path, type);
    }
}

/*
 * ng_get_agg_candidate_distribution_list
 *     Get candidate Distribution list of an Agg
 *     Two candidates will be taken:
 *     (1) the default computing group of current computing mode
 *     (2) the original distribution below the agg
 *
 * @param (in) plan:
 *     the Plan node to add Agg
 *
 * @return:
 *     the candidate Distribution list
 */
List* ng_get_agg_candidate_distribution_list(Plan* plan, bool is_correlated, DistrbutionPreferenceType type) 
{
    if (is_correlated) {
        Distribution* installation_distribution = ng_get_correlated_subplan_group_distribution();
        List* candidate_list = list_make1(installation_distribution);
        return candidate_list;
    }

    List* candidate_list = NIL;
    Distribution* distribution = NULL;

    /* Default compute distribution. */
    if (type == DPT_ALL || type == DPT_SHUFFLE) {
        distribution = ng_get_default_computing_group_distribution();
        candidate_list = lappend(candidate_list, distribution);
        ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();
        if (CNG_MODE_FORCE == cng_mode) {
            return candidate_list;
        }
    }

    /* Single node distribution. */
    if (type == DPT_ALL || type == DPT_SINGLE) {
        distribution = ng_get_single_node_distribution();
        if (distribution != NULL && !is_distribution_exists(candidate_list, distribution)) {
            candidate_list = lappend(candidate_list, distribution);
        }
    }

    /* Agg's dest distribution. */
    distribution = ng_get_dest_distribution(plan);
    if (!is_distribution_exists(candidate_list, distribution)) {
        candidate_list = lappend(candidate_list, distribution);
    } 
    return candidate_list;
}

/*
 * ng_get_setop_candidate_distribution_list
 *     Get candidate Distribution list of an setop
 *     Groups of all branchs will be taken.
 *
 * @param (in) subPlans:
 *     the branchs of a setop
 *
 * @return:
 *     the candidate Distribution list
 */
List* ng_get_setop_candidate_distribution_list(List* subPlans, bool is_correlated)
{
    if (is_correlated) {
        Distribution* installation_distribution = ng_get_correlated_subplan_group_distribution();
        List* candidate_list = list_make1(installation_distribution);
        return candidate_list;
    }

    List* candidate_distribution_list = NIL;

    ListCell* lc = NULL;
    foreach (lc, subPlans) {
        Plan* subPlan = (Plan*)lfirst(lc);
        Assert(subPlan->exec_nodes->nodeList != NIL);
        Distribution* distribution = ng_get_dest_distribution(subPlan);
        if (!is_distribution_exists(candidate_distribution_list,  distribution)) {
            candidate_distribution_list = lappend(candidate_distribution_list, distribution);
        }
    }

    return candidate_distribution_list;
}

/*
 * Cost based algorithms
 */
/*
 * ng_get_nodegroup_stream_weight
 *     calculate cost weight for stream in multiple node group scenario
 *
 * @param (in) producer_num_dn, consumer_num_dn:
 *     the number of data nodes of producer and consumer
 *
 * @return:
 *     the weight
 */
double ng_get_nodegroup_stream_weight(unsigned int producer_num_dn, unsigned int consumer_num_dn)
{
    double nodegroup_weight = 1.0;

    if (!u_sess->opt_cxt.is_multiple_nodegroup_scenario || producer_num_dn == consumer_num_dn) {
        return 1.0;
    } else {
        /*
         * Constant for node group optimizer
         * EXAMPLE: ngc {a, b, c, d, e}
         *          b = 1
         *          c + e = 1
         *          -1 < d < 0
         */
        double ngc[5] = {0.4163, 1, 0.1124, -0.9, 0.8876};

        /* cost factors for node group */
        double num_dn_ratio = (double)consumer_num_dn / (double)producer_num_dn;

        if (producer_num_dn < consumer_num_dn) {
            /* small group to big group, num_dn_ratio > 1 */
            nodegroup_weight = ngc[0] * log(num_dn_ratio) / log(exp(1)) + ngc[1];
        } else {
            /* big group to small group */
            nodegroup_weight = ngc[2] * pow(num_dn_ratio, ngc[3]) + ngc[4];
        }
    }

    return nodegroup_weight;
}

/*
 * ng_calculate_setop_branch_stream_cost
 *     Calculate stream cost when redistribute branchs of setop into same node group and distribute key.
 *     In future version, take bigger cluster into consideration for upper agg.
 *
 * @param (in) subPlan:
 *     one branch of setop
 *
 * @return:
 *     the redistribute cost
 */
Cost ng_calculate_setop_branch_stream_cost(
    Plan* subPlan, unsigned int producer_num_datanodes, unsigned int consumer_num_datanodes)
{
    Cost cost = 0;

    if (is_replicated_plan(subPlan)) {
        cost = (PLAN_LOCAL_ROWS(subPlan) / ((double)(consumer_num_datanodes))) * Max(subPlan->plan_width, 8);
    } else {
        cost = PLAN_LOCAL_ROWS(subPlan) * Max(subPlan->plan_width, 8);
    }

    return cost;
}

/*
 * Distribution management for each operators
 */
/*
 * ng_get_join_distribution
 *     get Distribution information from two child for a Join node
 *
 * @param (in) outer_path, inner_path:
 *     the outer and inner path
 *
 * @return:
 *     the Distribution of Join node
 */
Distribution* ng_get_join_distribution(Path* outer_path, Path* inner_path)
{
    Distribution* distribution_outer = ng_get_dest_distribution(outer_path);
    Distribution* distribution_inner = ng_get_dest_distribution(inner_path);

    AssertEreport(NULL != distribution_outer, MOD_OPT, "");
    AssertEreport(NULL != distribution_inner, MOD_OPT, "");

    if (is_subplan_exec_on_coordinator(inner_path)) {
        return ng_copy_distribution(distribution_inner);
    } else if (is_subplan_exec_on_coordinator(outer_path)) {
        return ng_copy_distribution(distribution_outer);
    } else if (ng_is_same_group(distribution_outer, distribution_inner)) {
        return distribution_outer;
    } else {
        ereport(
            ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Node group of outer and inner should be same.")));
        return NULL;
    }
}

/*
 * ng_get_join_data_distribution
 *     get data Distribution for a Join node
 *
 * @param (in) lefttree, righttree:
 *     the lefttree and righttree plan
 * @param (in) nodeid_list:
 *     the exec data node index list
 *
 * @return:
 *     the data Distribution for a Join node
 */
Distribution* ng_get_join_data_distribution(Plan* lefttree, Plan* righttree, List* nodeid_list)
{
    Distribution* left_distribution = ng_get_dest_distribution(lefttree);
    Distribution* right_distribution = ng_get_dest_distribution(righttree);

    if (ng_is_same_group(left_distribution, right_distribution)) {
        return left_distribution;
    } else {
        elog(DEBUG1, "Left and right side of join are not in same group");
        return ng_convert_to_distribution(nodeid_list);
    }
}

/*
 * ng_get_union_distribution_for_union_all
 *     In union all cases, when all branchs are not replicate, and in same group,
 *     this group will be used as target Distribution of this setop
 *
 * @param (in) subPlans:
 *     the branchs sub plans
 *
 * @return:
 *     NULL : there are replicate branch, which we will determine target node group later
 *     Distribution : the selected node group
 */
Distribution* ng_get_union_distribution_for_union_all(List* subPlans)
{
    bool all_hash_in_same_group = true;
    Distribution* all_hash_distribution = NULL;

    ListCell* lc = NULL;
    foreach (lc, subPlans) {
        Plan* subPlan = (Plan*)lfirst(lc);
        ExecNodes* ori_exec_nodes = ng_get_dest_execnodes(subPlan);
        if (NIL == ori_exec_nodes->nodeList) {
            elog(DEBUG1, "Union all branch's exec node list is empty");
        }

        if (is_replicated_plan(subPlan)) {
            all_hash_in_same_group = false;
        }

        if (all_hash_in_same_group) {
            if (all_hash_distribution == NULL) {
                all_hash_distribution = NewDistribution();
                ng_copy_distribution(all_hash_distribution, &ori_exec_nodes->distribution);
            } else {
                if (!ng_is_same_group(all_hash_distribution, &ori_exec_nodes->distribution)) {
                    all_hash_in_same_group = false;
                }
            }
        }
    }

    if (all_hash_in_same_group) {
        AssertEreport(
            all_hash_distribution != NULL && !bms_is_empty(all_hash_distribution->bms_data_nodeids), MOD_OPT, "");

        return all_hash_distribution;
    } else {
        return NULL;
    }
}

/*
 * ng_get_best_setop_distribution
 *     get target Distribution from branchs of setop
 *     (1) union all, all hash, all in same group => this group
 *     (2) others => need to shuffle to same group, choose cheapest one
 *
 * @param (in) subPlans:
 *     branchs
 * @param (in) isUnionAll:
 *     mark wheather it's union all
 *
 * @return:
 *     the target Distribution
 */
Distribution* ng_get_best_setop_distribution(List* subPlans, bool isUnionAll, bool is_correlated)
{
    Distribution* union_all_with_no_replicate = NULL;
    if (isUnionAll) {
        union_all_with_no_replicate = ng_get_union_distribution_for_union_all(subPlans);
    }

    if (union_all_with_no_replicate != NULL) {
        return union_all_with_no_replicate;
    }

    List* candidate_distribution_list = ng_get_setop_candidate_distribution_list(subPlans, is_correlated);

    Cost min_total_cost = -1.0;
    Distribution* best_distribution = NULL;

    ListCell* lc_candidate_distribution = NULL;
    foreach (lc_candidate_distribution, candidate_distribution_list) {
        Distribution* target_distribution = (Distribution*)lfirst(lc_candidate_distribution);
        Cost total_cost = 0.0;

        ListCell* lc_subPlan = NULL;
        foreach (lc_subPlan, subPlans) {
            Plan* subPlan = (Plan*)lfirst(lc_subPlan);

            Distribution* src_distribution = ng_get_dest_distribution(subPlan);
            if (ng_is_same_group(target_distribution, src_distribution)) {
                continue;
            }

            unsigned int producer_num_datanodes = ng_get_dest_num_data_nodes(subPlan);
            unsigned int consumer_num_datanodes = bms_num_members(target_distribution->bms_data_nodeids);
            Cost cost = ng_calculate_setop_branch_stream_cost(subPlan, producer_num_datanodes, consumer_num_datanodes);
            total_cost += cost;
        }

        if (min_total_cost < 0 || min_total_cost > total_cost) {
            min_total_cost = total_cost;
            best_distribution = target_distribution;
        }
    }

    return best_distribution;
}

/*
 * do shuffle between node groups if needed
 */
/*
 * ng_stream_side_paths_for_replicate
 *     Check wheather shuffle is needed when there are replicate table in outer and inner path,
 *     and shuffle them if needed.
 *
 * @param (in) root:
 *     the PlannerInfo
 * @param (in) outer_path, inner_path:
 *     the outer and inner path of a join
 * @param (in) jointype:
 *     the join type
 * @param (in) is_mergejoin:
 *     wheather it's a merge join
 * @param (in) target_distribution:
 *     the target Distribution to calculate the join
 *
 * @return: void
 */
void ng_stream_side_paths_for_replicate(PlannerInfo* root, Path** outer_path, Path** inner_path, JoinType jointype,
    bool is_mergejoin, Distribution* target_distribution)
{
    const double SKEW = 0.5;

    /* Return if one side is on CN */
    if (is_subplan_exec_on_coordinator(*outer_path) || is_subplan_exec_on_coordinator(*inner_path))
        return;

    bool is_outer_replicated = is_replicated_path(*outer_path);
    bool is_inner_replicated = is_replicated_path(*inner_path);

    if (!is_outer_replicated && !is_inner_replicated) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("is_outer_replicated and is_inner_replicated could not all be false")));
    }

    List* distribute_keys_outer = (*outer_path)->distribute_keys;
    List* distribute_keys_inner = (*inner_path)->distribute_keys;
    List* outer_pathkeys = is_mergejoin ? (*outer_path)->pathkeys : NIL;
    List* inner_pathkeys = is_mergejoin ? (*inner_path)->pathkeys : NIL;

    bool is_outer_need_shuffle = ng_is_shuffle_needed(root, *outer_path, target_distribution);
    bool is_inner_need_shuffle = ng_is_shuffle_needed(root, *inner_path, target_distribution);

    ParallelDesc* outer_smpDesc = NULL;
    ParallelDesc* inner_smpDesc = NULL;

    if (is_outer_need_shuffle) {
        if (is_outer_replicated) {
            /* stream for shuffle's producer and consumer dop are the same */
            if ((*outer_path)->dop > 1)
                outer_smpDesc = create_smpDesc((*outer_path)->dop, (*outer_path)->dop, REMOTE_SPLIT_BROADCAST);

            *outer_path = stream_side_path(root,
                *outer_path,
                jointype,
                is_outer_replicated,
                STREAM_BROADCAST,
                NIL,
                outer_pathkeys,
                false,
                SKEW,
                target_distribution,
                outer_smpDesc);
        } else {
            if (!ng_is_distribute_key_valid(root, distribute_keys_outer, (*outer_path)->parent->reltargetlist)) {
                *outer_path = NULL;
                return;
            }

            /* stream for shuffle's producer and consumer dop are the same */
            if ((*outer_path)->dop > 1)
                outer_smpDesc = create_smpDesc((*outer_path)->dop, (*outer_path)->dop, REMOTE_SPLIT_DISTRIBUTE);

            *outer_path = stream_side_path(root,
                *outer_path,
                jointype,
                is_outer_replicated,
                STREAM_REDISTRIBUTE,
                distribute_keys_outer,
                outer_pathkeys,
                false,
                SKEW,
                target_distribution,
                outer_smpDesc);
        }
    }

    if (is_inner_need_shuffle) {
        if (is_inner_replicated) {
            /* stream for shuffle's producer and consumer dop are the same */
            if ((*inner_path)->dop > 1)
                inner_smpDesc = create_smpDesc((*inner_path)->dop, (*inner_path)->dop, REMOTE_SPLIT_BROADCAST);

            *inner_path = stream_side_path(root,
                *inner_path,
                jointype,
                is_inner_replicated,
                STREAM_BROADCAST,
                NIL,
                inner_pathkeys,
                true,
                SKEW,
                target_distribution);
        } else {
            if (!ng_is_distribute_key_valid(root, distribute_keys_inner, (*inner_path)->parent->reltargetlist)) {
                *inner_path = NULL;
                return;
            }

            /* stream for shuffle's producer and consumer dop are the same */
            if ((*inner_path)->dop > 1)
                inner_smpDesc = create_smpDesc((*inner_path)->dop, (*inner_path)->dop, REMOTE_SPLIT_DISTRIBUTE);

            *inner_path = stream_side_path(root,
                *inner_path,
                jointype,
                is_inner_replicated,
                STREAM_REDISTRIBUTE,
                distribute_keys_inner,
                inner_pathkeys,
                true,
                SKEW,
                target_distribution,
                inner_smpDesc);
        }
    }
}

/*
 * ng_stream_non_broadcast_side_for_join
 *     need add redistribution step if the non-broadcast side doesn't match the computing node group
 *
 * @param (in) non_stream_path:
 *     the non-broadcast side path
 * @param (in) target_distribution:
 *     the target Distribution to calculate the join
 *
 * @return:
 *     the streamed Path
 */
Path* ng_stream_non_broadcast_side_for_join(PlannerInfo* root, Path* non_stream_path, JoinType save_jointype,
    List* non_stream_pathkeys, bool is_replicate, bool stream_outer, Distribution* target_distribution)
{
    if (ng_is_shuffle_needed(root, non_stream_path, target_distribution)) {
        List* stream_distribute_key = non_stream_path->distribute_keys;
        double skew_stream = non_stream_path->multiple;

        if (stream_distribute_key == NIL) {
            stream_distribute_key = get_distributekey_from_tlist(root,
                non_stream_path->parent->reltargetlist,
                non_stream_path->parent->reltargetlist,
                non_stream_path->parent->rows,
                &skew_stream);
        }

        if (ng_is_distribute_key_valid(root, stream_distribute_key, non_stream_path->parent->reltargetlist)) {
            ParallelDesc* smpDesc = NULL;

            /* stream for shuffle's producer and consumer dop are the same */
            if (non_stream_path->dop > 1)
                smpDesc = create_smpDesc(non_stream_path->dop, non_stream_path->dop, REMOTE_SPLIT_DISTRIBUTE);

            non_stream_path = stream_side_path(root,
                non_stream_path,
                save_jointype,
                is_replicate,
                STREAM_REDISTRIBUTE,
                stream_distribute_key,
                non_stream_pathkeys,
                stream_outer,
                skew_stream,
                target_distribution,
                smpDesc);
        } else {
            return NULL;
        }
    }
    return non_stream_path;
}

/*
 * ng_agg_force_shuffle
 *     in FORCE mode (CNG_MODE_FORCE), add stream node before add agg nodes
 *
 * @param (in) root:
 *     the plan
 * @param (in) groupcls:
 *     the group column list
 * @param (in) subplan:
 *     the sub-plan tree
 * @param (in) tlist:
 *     tlist where we search distribute keys
 * @param (in) subpath:
 *     path used to reduce the targetlist if needed
 *
 * @return:
 *     the result plan
 */
Plan* ng_agg_force_shuffle(PlannerInfo* root, List* groupcls, Plan* subplan, List* tlist, Path* subpath)
{
    Plan* result_plan = subplan;

    ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();
    Distribution* distribution = ng_get_dest_distribution(subplan);
    Distribution* target_distribution = ng_get_default_computing_group_distribution();
    if (is_execute_on_datanodes(subplan) && (!root->is_correlated) && CNG_MODE_FORCE == cng_mode &&
        !ng_is_same_group(distribution, target_distribution)) {
        if (subpath != NULL)
            disuse_physical_tlist(subplan, subpath);

        double multiple_force = 1.0;
        List* distribute_keys =
            get_distributekey_from_tlist(root, tlist, groupcls, subplan->plan_rows, &multiple_force);
        if (distribute_keys == NULL) {
            List* final_list_exprs = get_tlist_exprs(subplan->targetlist, false);
            distribute_keys =
                get_distributekey_from_tlist(root, NIL, final_list_exprs, subplan->plan_rows, &multiple_force);
        }
        if (ng_is_distribute_key_valid(root, distribute_keys, subplan->targetlist)) {
            result_plan = make_stream_plan(root, subplan, distribute_keys, multiple_force, target_distribution);
        }
    }

    return result_plan;
}

/*
 * compare functions, judge functions
 */
/*
 * ng_u_sess->opt_cxt.is_multiple_nodegroup_scenario
 *     get wheather it's a multiple node group scenario
 */
bool ng_is_multiple_nodegroup_scenario()
{
    return u_sess->opt_cxt.is_multiple_nodegroup_scenario;
}

/*
 * ng_u_sess->opt_cxt.different_nodegroup_count
 *     get wheather it's a everything in installation node group scenario
 */
bool ng_is_all_in_installation_nodegroup_scenario()
{
    return u_sess->opt_cxt.different_nodegroup_count == 1;
}

/*
 * ng_u_sess->opt_cxt.enable_nodegroup_explain
 *     get wheather show node group information in explain (node group DFX)
 */
bool ng_enable_nodegroup_explain()
{
    return u_sess->opt_cxt.enable_nodegroup_explain;
}

/*
 * ng_is_valid_group_name
 *     check wheather the group name is valid
 */
bool ng_is_valid_group_name(const char* group_name)
{
    if (group_name == NULL) {
        return false;
    }

    if (0 == strncasecmp(CNG_OPTION_OPTIMAL, group_name, strlen(group_name)) ||
        0 == strncasecmp(CNG_OPTION_QUERY, group_name, strlen(group_name)) ||
        0 == strncasecmp(CNG_OPTION_INSTALLATION, group_name, strlen(group_name))) {
        return false;
    } else {
        return true;
    }
}

/*
 * ng_is_special_group
 *     check wheather a group is a spacial group whose bucket map will be disordered
 */
bool ng_is_special_group(Distribution* distribution)
{
    /* Group built by optimizer is not special group */
    if (InvalidOid == distribution->group_oid) {
        return false;
    }

    /* Check 'in redistribution' group */
    if (u_sess->opt_cxt.in_redistribution_group_distribution != NULL &&
        u_sess->opt_cxt.in_redistribution_group_distribution->group_oid == distribution->group_oid) {
        return true;
    }

    /* Check installation group */
    Distribution* installation_distribution = ng_get_installation_group_distribution();
    AssertEreport(NULL != installation_distribution, MOD_OPT, "");
    if (installation_distribution->group_oid == distribution->group_oid) {
        return true;
    }
    const char *group_parent = get_pgxc_groupparent(distribution->group_oid);
    if (group_parent != NULL) {
        Oid group_parent_oid = get_pgxc_groupoid(group_parent, false);
        if ((u_sess->opt_cxt.in_redistribution_group_distribution != NULL &&
            u_sess->opt_cxt.in_redistribution_group_distribution->group_oid == group_parent_oid) ||
            installation_distribution->group_oid == group_parent_oid) {
            return true;
        }
    }
    /* Not a special group */
    return false;
}

/*
 * ng_is_same_group
 *     check wheather two node index bitmap set is same group
 */
bool ng_is_same_group(Bitmapset* bms_nodeids_1, Bitmapset* bms_nodeids_2)
{
    return bms_equal(bms_nodeids_1, bms_nodeids_2);
}

/*
 * ng_is_same_group
 *     check wheather two node index list is same group
 */
bool ng_is_same_group(List* nodeid_list_1, List* nodeid_list_2)
{
    Bitmapset* bms_nodeids_1 = ng_convert_to_nodeids(nodeid_list_1);
    Bitmapset* bms_nodeids_2 = ng_convert_to_nodeids(nodeid_list_2);

    return ng_is_same_group(bms_nodeids_1, bms_nodeids_2);
}

/*
 * ng_is_same_group
 *     check wheather ExecNodes (data nodes) and node index bitmap set are same group
 */
bool ng_is_same_group(ExecNodes* exec_nodes, Bitmapset* bms_nodeids)
{
    return ng_is_same_group(exec_nodes->distribution.bms_data_nodeids, bms_nodeids);
}

/*
 * ng_is_same_group
 *     check wheather two Distribution is same group
 */
bool ng_is_same_group(Distribution* distribution_1, Distribution* distribution_2)
{
    int bucketlen1 = -1;
    int bucketlen2 = -1;
    /* Compare using group oid */
    if (InvalidOid != distribution_1->group_oid && InvalidOid != distribution_2->group_oid &&
        distribution_1->group_oid == distribution_2->group_oid) {
        return true;
    }

    /* Check spacial node group (bucket map could be disordered) */
    bool is_special_group_1 = ng_is_special_group(distribution_1);
    bool is_special_group_2 = ng_is_special_group(distribution_2);
    if (is_special_group_1 || is_special_group_2) {
        return false;
    }
    if (InvalidOid != distribution_1->group_oid) {
        (void)BucketMapCacheGetBucketmap(distribution_1->group_oid, &bucketlen1);
    }
    if (InvalidOid != distribution_2->group_oid) {
        (void)BucketMapCacheGetBucketmap(distribution_2->group_oid, &bucketlen2);
    }
    /* Bucket map are all ordered? just compare data node index bitmap set */
    return (bucketlen1 == bucketlen2) &&
        ng_is_same_group(distribution_1->bms_data_nodeids, distribution_2->bms_data_nodeids);
}

/*
 * ng_is_exec_on_subset_nodes
 *     check wheather one ExecNodes (exec nodes) is subset of another ExecNodes (exec nodes)
 */
bool ng_is_exec_on_subset_nodes(ExecNodes* en1, ExecNodes* en2)
{
    Bitmapset* bms_en1 = ng_convert_to_nodeids(en1->nodeList);
    Bitmapset* bms_en2 = ng_convert_to_nodeids(en2->nodeList);

    return bms_is_subset(bms_en1, bms_en2);
}

/*
 * ng_is_shuffle_needed
 *     check wheather need to shuffle between two node groups
 */
bool ng_is_shuffle_needed(Distribution* current_distribution, Distribution* target_distribution)
{
    return !ng_is_same_group(current_distribution, target_distribution);
}

/*
 * ng_is_shuffle_needed
 *     check wheather need to shuffle a path to a target node groups
 *
 * @param (in) root:
 *     the PlannerInfo
 * @param (in) path:
 *     the path to be checked
 * @param (in) target_distribution:
 *     the target node group
 *
 * @return:
 *     wheather shuffle is needed
 */
bool ng_is_shuffle_needed(PlannerInfo* root, Path* path, Distribution* target_distribution)
{
    bool need_shuffle = false;

    Distribution* current_distribution = ng_get_dest_distribution(path);

    /*
     * Check if exec nodes is not matching.
     * for correlated subplan, destination node depends on the plan using the subplan,
     * not default node group, we should have shuffled to a same group,
     * even we will add broadcast node later
     */
    if (ng_is_shuffle_needed(current_distribution, target_distribution)) {
        /* Mark join rel needs shuffle */
        need_shuffle = true;
    }

    return need_shuffle;
}

bool ng_is_distribute_key_valid(PlannerInfo* root, List* distribute_key, List* target_list)
{
    if (distribute_key == NIL) {
        return false;
    }

    ListCell* lc = NULL;
    foreach (lc, distribute_key) {
        Node* dkey = (Node*)lfirst(lc);

        /* If we can not find distribute key in targetlist, further process it. */
        if (find_node_in_targetlist(dkey, target_list) < 0) {
            /* Try to find a qualify equal class. */
            Node* equal_expr = find_qualify_equal_class(root, dkey, target_list);
            if (NULL == equal_expr) {
                if (IsA(dkey, Var)) {
                    return false;
                }

                /* search into expression to find each var(s) */
                List* var_list =
                    pull_var_clause(dkey, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR);
                ListCell* lc_var = NULL;
                foreach (lc_var, var_list) {
                    Node* dkey_var = (Node*)lfirst(lc_var);

                    if (!find_node_in_targetlist(dkey_var, target_list)) {
                        Node* equal_expr_var = find_qualify_equal_class(root, dkey_var, target_list);
                        if (NULL == equal_expr_var) {
                            return false;
                        }
                    }
                }
            }
        }
    }

    return true;
}

/*
 * Other functions
 */
/*
 * ng_get_single_node_group_nodeids
 *     get node index bitmap set of a single node group
 */
Bitmapset* ng_get_single_node_group_nodeids()
{
    int nodeid = 0;
    if (IS_PGXC_DATANODE) {
        if (u_sess->pgxc_cxt.PGXCNodeId < 0)
            return NULL;
        else
            nodeid = u_sess->pgxc_cxt.PGXCNodeId;
    }
    return bms_make_singleton(nodeid);
}

/*
 * ng_get_single_node_group_distribution
 *     get Distribution of a single node group
 */
Distribution* ng_get_single_node_group_distribution()
{
    Distribution* distribution = NewDistribution();
    distribution->group_oid = InvalidOid;
    distribution->bms_data_nodeids = ng_get_single_node_group_nodeids();
    return distribution;
}

/*
 * ng_get_single_node_group_exec_node
 *     get ExecNodes of a single node group
 */
ExecNodes* ng_get_single_node_group_exec_node()
{
    Distribution* distribution = NewDistribution();

    if (IS_PGXC_DATANODE) {
        int nodeid = u_sess->pgxc_cxt.PGXCNodeId;
        if (nodeid >= 0) {
            distribution->bms_data_nodeids = bms_add_member(distribution->bms_data_nodeids, nodeid);
        } else {
            distribution->bms_data_nodeids = NULL;
        }
    } else if (IS_PGXC_COORDINATOR) {
        distribution->bms_data_nodeids = ng_get_single_node_group_nodeids();
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("[ng_get_single_dn_group_exec_node] unknown exec location.")));
    }

    distribution->group_oid = InvalidOid;

    ExecNodes* exec_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_REPLICATED, RELATION_ACCESS_READ);

    return exec_nodes;
}

char* dist_to_str(Distribution* distribution)
{
    StringInfo str = makeStringInfo();

    appendStringInfo(str, "groupid(%u) nodeids(", distribution->group_oid);
    
    _outBitmapset(str, distribution->bms_data_nodeids);

    appendStringInfo(str, ")");

    return str->data;
}

bool ng_is_single_node_group_distribution(Distribution* distribution) 
{
    Distribution* single_node_distribution = u_sess->opt_cxt.single_node_distribution;
    if (single_node_distribution == NULL || distribution == NULL) {
        return false;
    }
	
    if (single_node_distribution == distribution) {
        return true;
    }

    return ng_is_same_group(single_node_distribution, distribution);
}

int ng_get_different_nodegroup_count() 
{
    return u_sess->opt_cxt.different_nodegroup_count; 
}

static int ngroup_hash_partition_id(int id)
{
    return id % NUM_NGROUP_INFO_PARTITIONS;
}

static LWLock *ngroup_mapping_partitionlock(int hashcode)
{
    int id = FirstNGroupMappingLock + ngroup_hash_partition_id(hashcode);
    return &t_thrd.shemem_ptr_cxt.mainLWLockArray[id].lock;
}

void ngroup_info_hash_create()
{
    const int num_hash_elems = 1024;
    HASHCTL hash_ctl;

    errno_t rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "", "");
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(NGroupInfo);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = g_instance.cache_cxt.global_cache_mem;
    hash_ctl.num_partitions = NUM_NGROUP_INFO_PARTITIONS;

    int flags =  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_EXTERN_CONTEXT | HASH_NOEXCEPT | HASH_PARTITION;
    g_instance.ngroup_hash_table = hash_create("ngroup cache hash", num_hash_elems, &hash_ctl, flags);

    if (g_instance.ngroup_hash_table == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("nodegroup hash create failed")));
    }
}

Bitmapset *ngroup_info_hash_search(Oid ngroup_oid)
{
    Bitmapset *bms_nodeids = NULL;
    bool found = false;

    if (InvalidOid == ngroup_oid) {
        List *nodeid_list = GetAllDataNodes();
        bms_nodeids = ng_convert_to_nodeids(nodeid_list);
        list_free(nodeid_list);
        return bms_nodeids;
    }

    uint32 hashcode = oid_hash((const void *)&ngroup_oid, sizeof(ngroup_oid));
    LWLock *new_partition_lock = ngroup_mapping_partitionlock(hashcode);
    (void)LWLockAcquire(NgroupDestoryLock, LW_SHARED);
    (void)LWLockAcquire(new_partition_lock, LW_SHARED);
    NGroupInfo *ngroup_info = (NGroupInfo *)hash_search(g_instance.ngroup_hash_table, &ngroup_oid, HASH_FIND, &found);

    if (found) {
        /* the memory is release by caller */
        bms_nodeids = bms_copy(ngroup_info->bms_nodeids);
    }
    LWLockRelease(new_partition_lock);
    LWLockRelease(NgroupDestoryLock);

    return bms_nodeids;
}

void  ngroup_info_hash_insert(Oid ngroup_oid, Bitmapset *bms_node_ids)
{
    bool found = false;
    uint32 hashcode = oid_hash((const void *)&ngroup_oid, sizeof(ngroup_oid));
    LWLock *new_partition_lock = ngroup_mapping_partitionlock(hashcode);
    Bitmapset *bms_node_ids_copy = NULL;

    MemoryContext old_mem_context = MemoryContextSwitchTo(g_instance.ngroup_hash_table->hcxt);
    bms_node_ids_copy = bms_copy(bms_node_ids);
    MemoryContextSwitchTo(old_mem_context);
    (void)LWLockAcquire(NgroupDestoryLock, LW_SHARED);
    (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
    NGroupInfo *ngroup_info = (NGroupInfo *)hash_search(g_instance.ngroup_hash_table, &ngroup_oid, HASH_ENTER, &found);
    
    if (ngroup_info) {
        ngroup_info->oid = ngroup_oid;
        ngroup_info->bms_nodeids = bms_node_ids_copy;
    } else {
        LWLockRelease(new_partition_lock);
        LWLockRelease(NgroupDestoryLock);
        pfree(bms_node_ids_copy);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("failed to insert node group hash table")));
    }
    LWLockRelease(new_partition_lock);
    LWLockRelease(NgroupDestoryLock);
}

void ngroup_info_hash_delete(Oid ngroup_oid, bool is_destory)
{
    if (InvalidOid == ngroup_oid) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("NodeGroup Oid is invalid, invalid Oid is %u.", ngroup_oid)));
    }
    bool found = false;
    Bitmapset *bms_ptr = NULL;
    uint32 hashcode = oid_hash((const void *)&ngroup_oid, sizeof(ngroup_oid));
    LWLock *new_partition_lock = ngroup_mapping_partitionlock(hashcode);
    if (!is_destory) {
        (void)LWLockAcquire(NgroupDestoryLock, LW_SHARED);
    }
    (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
    NGroupInfo *ngroup_info = (NGroupInfo *)hash_search(g_instance.ngroup_hash_table, &ngroup_oid, HASH_FIND, &found);

    if (ngroup_info)
        bms_ptr = ngroup_info->bms_nodeids;
        
    hash_search(g_instance.ngroup_hash_table, &ngroup_oid, HASH_REMOVE, &found);
    LWLockRelease(new_partition_lock);
    if (!is_destory) {
        LWLockRelease(NgroupDestoryLock);
    }
    if (!found && ngroup_info) {
        if (is_destory) {
            LWLockRelease(NgroupDestoryLock);
        }
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("delete failed from nodegroup hash table, Oid is %u.", ngroup_oid)));
    }
    if (bms_ptr)
        pfree(bms_ptr);
}

void ngroup_info_hash_destory(void)
{
    HASH_SEQ_STATUS hash_seq;
    NGroupInfo* entry = NULL;

    (void)LWLockAcquire(NgroupDestoryLock, LW_EXCLUSIVE);

    hash_seq_init(&hash_seq, g_instance.ngroup_hash_table);
    while ((entry = (NGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        ereport(LOG, (errmsg(" ngroup_info_hash_print ngroup_info_hash__delete_all entry->oid: %d ", entry->oid)));
        ngroup_info_hash_delete(entry->oid, true);
    }
    LWLockRelease(NgroupDestoryLock);
}


