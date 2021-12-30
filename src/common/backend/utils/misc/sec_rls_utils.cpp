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
 * sec_rls_utils.cpp
 *     Row-Level-Security related utility functions.
 *
 * IDENTIFICATION
 *     src/common/backend/utils/misc/sec_rls_utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gaussdb_version.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/sec_rls_utils.h"
#include "utils/syscache.h"

/*
 * CheckBypassRlsPolicies
 *     The role which rolbypassrls is true can by pass all the row level security.
 *     If rolbypassrls is true for current user, return true, else return false.
 *
 * @param (in) roleid: Role Oid
 * @return: Role can bypass the row-level-security policies or not.
 */
static bool CheckBypassRlsPolicies(Oid roleid)
{
    /* Superusers and systemadmin can bypass all RLS policies */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    return false;
}

/*
 * CheckEnableRlsPolicies
 *     Check relation enable row level security or not.
 *     Also check user can bypass R.L.S policy or not.
 *
 * @param (in) relOid: Relation Oid
 * @param (in) roleid: Role Oid
 * @return: Apply Row-Level-Security policy succeed or not.
 */
EnableRlsFeature CheckEnableRlsPolicies(const Relation relation, Oid roleid)
{
    if (false == RelationIsValid(relation)) {
        return RLS_DISABLED;
    }

    /* Get relrowsecurity information from pg_class.reloption */
    bool enableRlsPolicy = RelationEnableRowSecurity(relation);
    bool forceRlsPolicy = RelationForceRowSecurity(relation);

    /* If relation does not enable row-level-security, nothing to do here */
    if (false == enableRlsPolicy) {
        return RLS_DISABLED;
    }

    /* Check whether this role can bypass the row-level-security policy */
    if (true == CheckBypassRlsPolicies(roleid)) {
        return RLS_DEPEND;
    }

    /*
     * If FORCE ROW LEVEL SECURITY has not been set on the relation then we
     * should bypass the owner of relation.
     */
    if ((false == forceRlsPolicy) && (true == pg_class_ownercheck(RelationGetRelid(relation), roleid))) {
        return RLS_DEPEND;
    }

    return RLS_ENABLED;
}

/*
 * MakeRlsSelectStmtForCopyTo
 *     Generate SelectStmt for "COPY relname [(columnList)] TO file [WITH] [(options)]" query
 *     when RLS is turn on for this relation. "COPY ( SELECT ... ) TO file [WITH] [(options)]"
 *     query is not influenced because of parser already generate SelectStmt in grammar parsing
 *     stage.
 *
 * @param (in) relation: Relation information
 * @param (in) stmt: Copy Statement
 * @return: Build new SelectStmt
 */
SelectStmt* MakeRlsSelectStmtForCopyTo(const Relation relation, const CopyStmt* stmt)
{
    SelectStmt* selectStmt = makeNode(SelectStmt);
    List* targetList = NIL;
    List* fromClause = NIL;
    ColumnRef* cr = NULL;
    ResTarget* rt = NULL;
    char* schemaname = NULL;
    char* relname = NULL;
    RangeVar* rangevar = NULL;

    /*
     * Construct target list
     * If no columns are specified in the attribute list of the COPY command,
     * then Star('*') should be used as the target list for the resulting SELECT
     * statement. In the case that columns are specified in the attribute list,
     * create a ColumnRef and ResTarget for each column and add them to the target
     * list for the resulting SELECT statement.
     */
    if (stmt->attlist == NIL) {
        cr = makeNode(ColumnRef);
        cr->fields = list_make1(makeNode(A_Star));
        cr->location = -1;
        rt = makeNode(ResTarget);
        rt->name = NULL;
        rt->indirection = NIL;
        rt->val = (Node*)cr;
        rt->location = -1;
        targetList = list_make1(rt);
    } else {
        ListCell* lc = NULL;
        /* attlist is String 'Value' node list */
        foreach (lc, stmt->attlist) {
            /* build ColumnRef for each column */
            cr = makeNode(ColumnRef);
            cr->fields = list_make1(lfirst(lc));
            cr->location = -1;
            rt = makeNode(ResTarget);
            rt->name = NULL;
            rt->indirection = NIL;
            rt->val = (Node*)cr;
            rt->location = -1;
            targetList = lappend(targetList, rt);
        }
    }
    schemaname = get_namespace_name(RelationGetNamespace(relation));
    relname = pstrdup(RelationGetRelationName(relation));
    rangevar = makeRangeVar(schemaname, relname, -1);
    selectStmt->targetList = targetList;
    fromClause = list_make1(rangevar);
    selectStmt->fromClause = fromClause;
    return selectStmt;
}

/*
 * LicenseSupportRls
 *     Check licnese support Row Level Security feature or not.
 *     If license not support, generate error.
 *
 * @return: void
 */
void LicenseSupportRls()
{
    if (is_feature_disabled(ROW_LEVEL_SECURITY) == true) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Row Level Security is not supported.")));
    }
}

/*
 * SupportRlsForRel
 *     Check whether support ddl(create, change, drop) operation or apply
 *     rls policy on current relation.
 *
 * @param (in) relation: Relation information
 * @return: support or do not support.
 */
void SupportRlsForRel(const Relation relation)
{
    /* Check rel is valid */
    if (!RelationIsValid(relation)) {
        return;
    }

    /* Get heap tuple for rel */
    HeapTuple tuple = SearchSysCache1(RELOID, RelationGetRelid(relation));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(relation))));
    }
    Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);

    /* Current object must be a normal table */
    if (reltup->relkind != RELKIND_RELATION) {
        ReleaseSysCache(tuple);
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a normal table", RelationGetRelationName(relation))));
    }

    /* Check temp table or not */
    if (reltup->relpersistence == RELPERSISTENCE_TEMP) {
        ReleaseSysCache(tuple);
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("do not support row level security policy on temp table \"%s\"",
                    RelationGetRelationName(relation))));
    }

    if (reltup->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
        ReleaseSysCache(tuple);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        (errmsg("Un-support feature"),
                         errdetail("Do not support row level security policy on subpartition table."),
                         errcause("The function is not implemented."), erraction("Use other actions instead."))));
    }

    /* Do not support dfs table */
    if (RelationIsDfsStore(relation)) {
        ReleaseSysCache(tuple);
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("do not support row level security policy on dfs table \"%s\"",
                    RelationGetRelationName(relation))));
    }

    /* relase sys cache tuple */
    ReleaseSysCache(tuple);
}
