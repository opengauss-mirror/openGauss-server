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
 * ---------------------------------------------------------------------------------------
 * 
 * rewriteRlsPolicy.h
 *        External interface to row level security rewriter.
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/rewrite/rewriteRlsPolicy.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef REWRITERLSPOLICY_H
#define REWRITERLSPOLICY_H

#include "nodes/parsenodes.h"
#include "utils/array.h"

/*
 *  * This Row Level Security policy can apply to all commands.
 *   * Include SELECT, INSERT, UPDATE, DELETE, currently only support
 *    * SELECT, UPDATE, DELETE.
 *     */
#define RLS_CMD_ALL_CHR '*'

/*
 * The struct that can describe Row Level Security Policy.
 * Aleady know the relation information, no need to store relation info.
 */
typedef struct RlsPolicy {
    Oid policyOid;     /* Policy's oid */
    char cmdName;      /* the command name the policy applies to */
    bool isPermissive; /* restrictive or permissive policy */
    bool hasSubLink;   /* policy quals include sublink or not */
    char* policyName;  /* Policy's name */
    ArrayType* roles;  /* the roles associated with the policy */
    Expr* usingExpr;   /* the policy's condition */
} RlsPolicy;

/*
 * The struct describe the Row-Level-Security policies related with current relation.
 */
typedef struct RlsPoliciesDesc {
    MemoryContext rlsCxt; /* row level security memory context */
    List* rlsPolicies;    /* list of row level security policies */
} RlsPoliciesDesc;

enum RlsPolicyCheckResult { RLS_POLICY_MATCH, RLS_POLICY_MISMATCH };

enum RelationRlsStatus {
    RELATION_RLS_DISABLE,
    RELATION_RLS_ENABLE,
    RELATION_RLS_FORCE_DISABLE,
    RELATION_RLS_FORCE_ENABLE
};

extern void GetRlsPolicies(const Query* query, const RangeTblEntry* rte, const Relation relation, List** rlsQuals,
    int rtIndex, bool& hasRowSecurity, bool& hasSubLink);

#endif /* REWRITERLSPOLICY_H */
