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
 * rewriteRlsPolicy.cpp
 *     Add Row-Level-Security policy to Query during query rewrite.
 *
 * Portions Copyright (c) 2019, Huawei Tech. Co., Ltd.
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/rewrite/rewriteRlsPolicy.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "storage/lock/lock.h"
#include "utils/acl.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/sec_rls_utils.h"
#include "utils/syscache.h"

static bool CheckRoleForRlsPolicy(const RlsPolicy* policy, Oid roleid);
static void PullRlsPoliciesForRel(CmdType cmd, Oid roleid, const List* relRlsPolicies, List** permissivePolicies,
    List** restrictivePolicies, bool& hasSubLink);
static void AddRlsUsingQuals(
    CmdType cmd, int rtIndex, const List* permissivePolicies, const List* restrictivePolicies, List** rlsUsingQuals);
/*
 * CheckRoleForRlsPolicy
 *     Check policy applied to this role or not.
 *
 * @param (in) policy: Row Level Security policy
 * @param (in) roleid: Role Oid
 * @return: This Row-Level-Security policy apply to role or not.
 */
static bool CheckRoleForRlsPolicy(const RlsPolicy* policy, Oid roleid)
{
    Oid* roles = (Oid*)ARR_DATA_PTR(policy->roles);
    int roleNums = ARR_DIMS(policy->roles)[0];

    /*
     * ACL_ID_PUBLIC means this policy applies to all users,
     * and ACL_ID_PUBLIC is the only applied user for this policy.
     */
    if (roles[0] == ACL_ID_PUBLIC) {
        return true;
    }

    for (int i = 0; i < roleNums; i++) {
        /* Check this user has the privilege for this policy */
        if (has_privs_of_role(roleid, roles[i])) {
            return true;
        }
    }

    /* This policy does not apply to current user */
    return false;
}

/*
 * PullRlsPoliciesForRel
 *
 *     Get permissive and restrictive policies lists for specific relation, role and command.
 *
 * @param (in) cmd: SQL Command(SELECT, UPDATE, DELETE)
 * @param (in) roleid: Role Oid
 * @param (in) relRlsPolicies: All RLS policies for relation
 * @param (out) permissivePolicies: Conditional permissive RLS policies
 * @param (out) restrictivePolicies: Conditional restrictive RLS policies
 * @param (out) hasSubLink: Matched RLS policies include sublink or not
 * @return: void.
 */
static void PullRlsPoliciesForRel(CmdType cmd, Oid roleid, const List* relRlsPolicies, List** permissivePolicies,
    List** restrictivePolicies, bool& hasSubLink)
{
    ListCell* item = NULL;
    RlsPolicy* policy = NULL;
    bool roleForPolicy = false;
    bool cmdMatch = false;
    foreach (item, relRlsPolicies) {
        policy = (RlsPolicy*)lfirst(item);
        /* Check this R.L.S policy affect this user, if not just skip */
        roleForPolicy = CheckRoleForRlsPolicy(policy, roleid);
        if (roleForPolicy) {
            cmdMatch = false;
            switch (cmd) {
                case CMD_SELECT:
                    if ((policy->cmdName == ACL_SELECT_CHR) || (policy->cmdName == RLS_CMD_ALL_CHR))
                        cmdMatch = true;
                    break;
                case CMD_UPDATE:
                    if ((policy->cmdName == ACL_UPDATE_CHR) || (policy->cmdName == RLS_CMD_ALL_CHR))
                        cmdMatch = true;
                    break;
                case CMD_DELETE:
                    if ((policy->cmdName == ACL_DELETE_CHR) || (policy->cmdName == RLS_CMD_ALL_CHR))
                        cmdMatch = true;
                    break;
                case CMD_MERGE:
                    if ((policy->cmdName == ACL_UPDATE_CHR) || (policy->cmdName == RLS_CMD_ALL_CHR))
                        cmdMatch = true;
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unsupported command type: %d.", cmd)));
                    break;
            }
            /* This policy can apply to current user and SQL query statement */
            if (cmdMatch) {
                /* Check qual has sublink or not */
                hasSubLink = hasSubLink || policy->hasSubLink;
                /* Seperate permissive and restrictive policies */
                if (policy->isPermissive)
                    *permissivePolicies = lcons(policy, *permissivePolicies);
                else
                    *restrictivePolicies = lcons(policy, *restrictivePolicies);
            }
        }
    }
    return;
}

/*
 * AddRlsUsingQuals
 *
 *     Bind permissive policy quals and restrictive policy quals to rlsUsingQuals list.
 *
 * @param (in) cmd: SQL Command(SELECT, UPDATE, DELETE)
 * @param (in) rtIndex: Range table index
 * @param (in) permissivePolicies: Conditional permissive RLS policies
 * @param (in) restrictivePolicies: Conditional restrictive RLS policies
 * @param (out) rlsUsingQuals: Combine together the USING caluses from all the permissive policies
 *              and restrictive policies.
 * @return: void.
 */
static void AddRlsUsingQuals(
    CmdType cmd, int rtIndex, const List* permissivePolicies, const List* restrictivePolicies, List** rlsUsingQuals)
{
    ListCell* item = NULL;
    List* permissiveQuals = NIL;
    RlsPolicy* policy = NULL;
    Expr* rlsExpr = NULL;

    /* Combine together the USING caluses from all the restrictive policies using AND. */
    foreach (item, restrictivePolicies) {
        policy = (RlsPolicy*)lfirst(item);

        if (policy->usingExpr != NULL) {
            rlsExpr = (Expr*)copyObject(policy->usingExpr);
            ChangeVarNodes((Node*)rlsExpr, 1, rtIndex, 0);
            *rlsUsingQuals = list_append_unique(*rlsUsingQuals, rlsExpr);
        }
    }

    /* Collect up the permissive quals using OR. */
    foreach (item, permissivePolicies) {
        policy = (RlsPolicy*)lfirst(item);

        if (policy->usingExpr != NULL) {
            permissiveQuals = lappend(permissiveQuals, copyObject(policy->usingExpr));
        }
    }
    /*
     * Add a single security qual combining together the USING clauses from all
     * the permissive policies using OR. If there were no permissive policies
     * exist, we did not construct one-time False filter, this is different with
     * openGauss (PG will generate one-time False filter when no permissive
     * policies exist).
     */
    rlsExpr = NULL;
    if (list_length(permissiveQuals) == 1) {
        rlsExpr = (Expr*)linitial(permissiveQuals);
    } else if (list_length(permissiveQuals) > 1) {
        rlsExpr = makeBoolExpr(OR_EXPR, permissiveQuals, -1);
    } else {
        return;
    }
    ChangeVarNodes((Node*)rlsExpr, 1, rtIndex, 0);
    *rlsUsingQuals = list_append_unique(*rlsUsingQuals, rlsExpr);
    return;
}

/*
 * GetRlsPolicies
 *     Get Row-Level-Security policies to specified relation.
 *     Find Row-Level-Security policy for this relation and store in rlsQuals.
 *
 * @param (in) query: Query tree for current query, provide extra information.
 * @param (in) rte: Range table entry, get all the conditional R.L.S policies.
 * @param (in) relation: Relation info for target table.
 * @param (in) rlsQuals: Store all the R.L.S policy quals for rte.
 * @param (out) hasRowSecurity: Check relation enable R.L.S or not.
 * @param (out) hasSubLink: Check the R.L.S policy quals has sublink or not.
 * @return: void.
 */
void GetRlsPolicies(const Query* query, const RangeTblEntry* rte, const Relation relation, List** rlsQuals, int rtIndex,
    bool& hasRowSecurity, bool& hasSubLink)
{
    /* Choose current user */
    Oid roleid = GetUserId();

    /* Check whether enabled Row-Level-Security for this relation */
    EnableRlsFeature rlsStatus = CheckEnableRlsPolicies(relation, roleid);
    /* relation did not enable row level security */
    if (rlsStatus == RLS_DISABLED) {
        hasRowSecurity = false;
        return;
    } else if (rlsStatus == RLS_DEPEND) {
        /*
         * relation enable row level security, but current user can bypass it.
         * hasRowSecurity is marked as true to force a re-plan when the environment
         * changes. Example: for plan cache scenario, same query execute in one session
         * may have different plans(set role *** password ***).
         */
        hasRowSecurity = true;
        return;
    } else {
        hasRowSecurity = true;
    }

    hasSubLink = false;

    /*
     * RLS is enabled for this relation.
     * Get the security policies that should be applied, based on the command
     * type.  Note that if this isn't the target relation, we actually want
     * the relation's SELECT policies, regardless of the query command type,
     * for example in UPDATE t1 ... FROM t2 we need to apply t1's UPDATE
     * policies and t2's SELECT policies.
     */
    CmdType cmdType = (rtIndex == linitial2_int(query->resultRelations)) ? query->commandType : CMD_SELECT;
    List* rlsPermissivePolicies = NULL;
    List* rlsRestrictivePolicies = NULL;

    /*
     * Get matched policies related with current relation. For SELECT, UPDATE
     * and DELETE, add security quals to enforce the USING policies. These
     * security quals control access to existing table rows.  Restrictive
     * policies are combined together using AND, and permissive policies
     * are combined together using OR.
     */
    PullRlsPoliciesForRel(cmdType, roleid, relation->rd_rlsdesc->rlsPolicies, &rlsPermissivePolicies, 
                          &rlsRestrictivePolicies, hasSubLink);
    AddRlsUsingQuals(cmdType, rtIndex, rlsPermissivePolicies, rlsRestrictivePolicies, rlsQuals);

    /*
     * For SQL Statement: SELECT ... FOR UPDATE|SHARE statement, need to collect up all
     * the CMD_UPDATE using policies and add them to RTE via AddRlsUsingQuals.
     * This way, we need to filter out any records which are not visible through an
     * ALL or UPDATE USING policy.
     */
    if ((cmdType == CMD_SELECT) && (rte->requiredPerms & ACL_UPDATE)) {
        List* updateMermissivePolicies = NIL;
        List* updateRestrictivePolicies = NIL;

        PullRlsPoliciesForRel(CMD_UPDATE,
            roleid,
            relation->rd_rlsdesc->rlsPolicies,
            &updateMermissivePolicies,
            &updateRestrictivePolicies,
            hasSubLink);

        AddRlsUsingQuals(CMD_UPDATE, rtIndex, updateMermissivePolicies, updateRestrictivePolicies, rlsQuals);
    }

    /*
     * For SQL Statement: UPDATE SET ... RETURNING, DELETE FROM ... RETURNING,
     * or the user has provided a WHERE clause which involves columns from the relation.
     * Need to collect up CMD_SELECT policies and add them via AddRlsUsingQuals.
     * This way, we need to filter out any records which are not visible through an
     * ALL or SELECT USING policy.
     */
    if (((cmdType == CMD_UPDATE) || (cmdType == CMD_DELETE)) && (rte->requiredPerms & ACL_SELECT)) {
        List* selectPermissivePolicies = NIL;
        List* selectRestrictivePolicies = NIL;

        PullRlsPoliciesForRel(CMD_SELECT,
            roleid,
            relation->rd_rlsdesc->rlsPolicies,
            &selectPermissivePolicies,
            &selectRestrictivePolicies,
            hasSubLink);

        AddRlsUsingQuals(CMD_SELECT, rtIndex, selectPermissivePolicies, selectRestrictivePolicies, rlsQuals);
    }

    return;
}
