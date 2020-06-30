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
 * sec_rls_utils.h
 *        Security Module.
 *        Utility commands used in Row Level Security Policy.
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/sec_rls_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef UTILS_SEC_RLS_UTILS_H
#define UTILS_SEC_RLS_UTILS_H

#include "pgxc/pgxc.h"
/*
 * If enable_rowsecurity for one relation is false then return RLS_DISABLED.
 * If enable_rowsecurity for one relation is true, but current user can
 * bypass the RLS policies for this relation, then return RLS_DEPEND to
 * indicate that if the environment changes, we need to invalidate and
 * replan. Finally, if enable_rowsecurity for one relation is true and
 * current user can not bypss the RLS policies, then return RLS_ENABLED,
 * which means we also need to invalidate if the environment changes.
 */
enum EnableRlsFeature { RLS_DISABLED, RLS_DEPEND, RLS_ENABLED };

/*
 * SupportRlsOnCurrentNode
 *     Check whether support ddl(create, change, drop) operation or apply
 *     rls policy on current node(cn, dn, single_node).
 *
 * @return: support row level security on current node or not.
 */
inline bool SupportRlsOnCurrentNode()
{
    return (IS_PGXC_COORDINATOR || IS_SINGLE_NODE);
}

/*
 * RelationEnableRowSecurity
 *     Returns whether the relation enable row level security, or not
 *
 * @param (in) relation: Relation information.
 * @return: enable row level security or not.
 */
inline bool RelationEnableRowSecurity(const Relation relation)
{
    /* Check whether support RLS on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return false;
    }
    /* Only check this option for normal relation */
    if (relation->rd_rel->relkind != RELKIND_RELATION) {
        return false;
    }
    if (relation->rd_options != NULL) {
        /*
         * Only when relation is normal relation, rd_options
         * can be casted to StdRdOptions type. Index has its
         * own rd_options type, so it is dangerous to cast
         * rd_options to StdRdOptions for index relation.
         */
        StdRdOptions* rd_options = (StdRdOptions*)relation->rd_options;
        return rd_options->enable_rowsecurity;
    }
    return false;
}

/*
 * RelationForceRowSecurity
 *     Returns whether the relation force row level security, or not
 *
 * @param (in) relation: Relation information.
 * @return: force row level security or not.
 */
inline bool RelationForceRowSecurity(const Relation relation)
{
    /* Check whether support RLS on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return false;
    }
    /* Only check this option for normal relation */
    if (relation->rd_rel->relkind != RELKIND_RELATION) {
        return false;
    }
    if (relation->rd_options != NULL) {
        /*
         * Only when relation is normal relation, rd_options
         * can be casted to StdRdOptions type. Index has its
         * own rd_options type, so it is dangerous to cast
         * rd_options to StdRdOptions for index relation.
         */
        StdRdOptions* rd_options = (StdRdOptions*)relation->rd_options;
        return rd_options->force_rowsecurity;
    }
    return false;
}

extern EnableRlsFeature CheckEnableRlsPolicies(const Relation relation, Oid roleid);
extern SelectStmt* MakeRlsSelectStmtForCopyTo(const Relation relation, const CopyStmt* stmt);
extern void LicenseSupportRls();
extern void SupportRlsForRel(const Relation relation);

#endif /* UTILS_SEC_RLS_UTILS_H */
