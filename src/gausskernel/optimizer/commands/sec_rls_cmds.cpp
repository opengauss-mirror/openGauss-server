/* -------------------------------------------------------------------------
 *
 * sec_rls_cmds.cpp
 *     Security Module.
 *     Variables and function commands used in Row Level Security Policy.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019, Huawei Tech. Co., Ltd.
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/commands/sec_rls_cmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_type.h"
#include "commands/sec_rls_cmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "storage/lock/lock.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sec_rls_utils.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/knl_relcache.h"

/*
 * The row level security policies for one relation should be
 * less than or equal to 100
 */
#define MaxRlsPolciesForRelation 100

/*
 * This struct is used for ContainRteInRlsPolicyWalker
 * checkedRelId is the relation oid whether exist in rls policy
 * skipRlsPolicyId is the rls policy oid that should be skipped
 */
struct RlsPolicyRteInfo {
    Oid checkedRelId;
    Oid skipRlsPolicyId;
};

static char ParseRlsPolicyCommandName(const char* cmdName);
static Datum* RlsPolicyRolesToArray(List* roles, int& numRoles);
static void RangeVarCallbackForRlsPolicy(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool target_is_partition, void* callback_arg);
static bool ContainRteInRlsPolicyWalker(Node* node, const RlsPolicyRteInfo* relid);

/*
 * Add shared dependencies on roles in pg_shdepend.
 */
void addShdependForRoles(int roleNums, Datum* roleOids, ObjectAddress* myself, ObjectAddress* referenced)
{
    referenced->classId = AuthIdRelationId;
    referenced->objectSubId = 0;
    for (int i = 0; i < roleNums; i++) {
        referenced->objectId = DatumGetObjectId(roleOids[i]);
        /* If specified user is public, skip */
        if (referenced->objectId == ACL_ID_PUBLIC) {
            continue;
        }
        recordSharedDependencyOn(myself, referenced, SHARED_DEPENDENCY_RLSPOLICY);
    }
}

/*
 * CreateRlsPolicy
 *     handles the execution of the CREATE POLICY(Row Level Security) command.
 *
 * @param (in) stmt: CreateRlsPolicyStmt describes the policy to create.
 * @return: void
 */
void CreateRlsPolicy(CreateRlsPolicyStmt* stmt)
{
    Assert(stmt != NULL);
    /* Check whether need to create rls policy on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return;
    }
    /* Check license whether support this feature */
    LicenseSupportRls();

    char polCmd = ParseRlsPolicyCommandName(stmt->cmdName);

    /* Policy condition is empty, report error */
    if (stmt->usingQual == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Do not specify any information for CREATE POLICY \"%s\" ON \"%s\"",
                    stmt->policyName,
                    stmt->relation->relname)));
    }

    /* Collect role ids and construct role oid array */
    int roleNums = 0;
    Datum* roleOids = RlsPolicyRolesToArray(stmt->roleList, roleNums);
    ArrayType* roleIds = construct_array(roleOids, roleNums, OIDOID, sizeof(Oid), true, 'i');

    /* Parse USING qual */
    ParseState* usingQualState = make_parsestate(NULL);

    /* Get id of table.  Also handles permissions checks. */
    Oid tableOid = RangeVarGetRelidExtended(
        stmt->relation, RowExclusiveLock, false, false, false, false, RangeVarCallbackForRlsPolicy, (void*)stmt);

    /* Open target table to build quals. No additional lock is necessary. */
    Relation targetTable = relation_open(tableOid, NoLock);

    /* Check whether support RLS for this relation */
    SupportRlsForRel(targetTable);

    /* Add target table info to using qual state */
    RangeTblEntry* rte = addRangeTableEntryForRelation(usingQualState, targetTable, NULL, false, false);
    addRTEtoQuery(usingQualState, rte, false, true, true);

    /* Transform expr clause */
    Node* usingQual = transformWhereClause(usingQualState, (Node*)copyObject(stmt->usingQual),
                                           EXPR_KIND_POLICY, "POLICY");

    /* Take care of collations */
    assign_expr_collations(usingQualState, usingQual);

    /* Check whether contain RangeTblEntry in usingQual */
    RlsPolicyRteInfo rteInfo;
    rteInfo.checkedRelId = tableOid;
    rteInfo.skipRlsPolicyId = 0;  // do not skip rls policy
    if (ContainRteInRlsPolicyWalker(usingQual, &rteInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Create row level security policy \"%s\" failed, because it will result in "
                       "infinite recursion for DML queries",
                    stmt->policyName)));
    }

    /* Change ParseState to text */
    char* usingQualStr = nodeToString(usingQual);

    /* Use the index to search for the matching old tuple */
    ScanKeyData scanKey[2];
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableOid));
    ScanKeyInit(
        &scanKey[1], Anum_pg_rlspolicy_polname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(stmt->policyName));

    /* Open pg_rlspolicy relation */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, RowExclusiveLock);

    /* Search old tuple by index */
    SysScanDesc scanDesc =
        systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 2, scanKey);
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);

    /* Policy already exists */
    if (HeapTupleIsValid(rlsPolicyTuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("row level policy \"%s\" for table \"%s\" already exists",
                    stmt->policyName,
                    RelationGetRelationName(targetTable))));
    }
    systable_endscan(scanDesc);

    /* RLS policies for one relation should not large than MaxRlsPolciesForRelation */
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableOid));
    scanDesc = systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 1, scanKey);
    int existRlsNum = 0;
    while ((rlsPolicyTuple = systable_getnext(scanDesc)) != NULL) {
        existRlsNum++;
    }

    /* 
     * existRlsNum refers to the number of row level policies that already exist in the table, 
     * excluding the new policy that will be inserted. When the existing number of policies in the 
     * table already reach MaxRlsPolciesForRelation, excute exit in advance. 
     */
    if (existRlsNum >= MaxRlsPolciesForRelation) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("Num of row level policies for relation should less than or equal to %d",
                    MaxRlsPolciesForRelation)));
    }

    char polName[NAMEDATALEN] = {0};
    int rc = strcpy_s(polName, sizeof(polName), stmt->policyName);
    securec_check(rc, "\0", "\0");
    /* Set up values and nulls arrays */
    Datum values[Natts_pg_rlspolicy] = {0};
    bool nulls[Natts_pg_rlspolicy] = {false};
    values[Anum_pg_rlspolicy_polname - 1] = NameGetDatum(polName);
    values[Anum_pg_rlspolicy_polrelid - 1] = ObjectIdGetDatum(tableOid);
    values[Anum_pg_rlspolicy_polcmd - 1] = CharGetDatum(polCmd);
    values[Anum_pg_rlspolicy_polpermissive - 1] = BoolGetDatum(stmt->isPermissive);
    values[Anum_pg_rlspolicy_polroles - 1] = PointerGetDatum(roleIds);
    values[Anum_pg_rlspolicy_polqual - 1] = CStringGetTextDatum(usingQualStr);

    /* Ready to store new pg_rlspolicy tuple */
    rlsPolicyTuple = heap_form_tuple(RelationGetDescr(pg_rlspolicy), values, nulls);
    Oid newPolicyId = simple_heap_insert(pg_rlspolicy, rlsPolicyTuple);
    CatalogUpdateIndexes(pg_rlspolicy, rlsPolicyTuple);

    /*
     * Install dependency on policy's relation to ensure it will go away on
     * relation deletion.
     */
    ObjectAddress myself;
    myself.classId = RlsPolicyRelationId;
    myself.objectId = newPolicyId;
    myself.objectSubId = 0;

    ObjectAddress referenced;
    referenced.classId = RelationRelationId;
    referenced.objectId = tableOid;
    referenced.objectSubId = 0;

    /* When drop table, this rls policy will be dropped automatically */
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Istall dependencies on objects referenced in using quals and with check quals */
    recordDependencyOnExpr(&myself, usingQual, usingQualState->p_rtable, DEPENDENCY_NORMAL);

    /* Add shared dependencies on users in pg_shdepend */
    addShdependForRoles(roleNums, roleOids, &myself, &referenced);

    /* Invalidate relcache so that others see the new R.L.S policies */
    CacheInvalidateRelcache(targetTable);

    /* Create row level security policy done */
    free_parsestate(usingQualState);
    systable_endscan(scanDesc);
    heap_freetuple(rlsPolicyTuple);
    relation_close(targetTable, NoLock);
    heap_close(pg_rlspolicy, RowExclusiveLock);
    return;
}

/*
 * AlterRlsPolicy
 *     handles the execution of the ALTER POLICY(Row Level Security) command.
 *
 * @param (in) stmt: AlterRlsPolicyStmt describes the policy and how to alter it.
 * @return: void
 */
void AlterRlsPolicy(AlterRlsPolicyStmt* stmt)
{
    Assert(stmt != NULL);
    /* Check whether need to alter rls policy on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return;
    }
    /* Check license whether support this feature */
    LicenseSupportRls();

    /* Alter Policy do not specify any information */
    if ((stmt->roleList == NULL) && (stmt->usingQual == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Do not specify any information for ALTER POLICY \"%s\" ON \"%s\"",
                    stmt->policyName,
                    stmt->relation->relname)));
    }

    /*
     * Get relation oid by name, grab an exclusive lock on the target table,
     * which we will NOT release until end of transaction.
     */
    Oid relid = InvalidOid;
    relid = RangeVarGetRelidExtended(
        stmt->relation, RowExclusiveLock, false, false, false, false, RangeVarCallbackForRlsPolicy, (void*)stmt);

    TrForbidAccessRbObject(RelationRelationId, relid, stmt->relation->relname);

    /* Open pg_rlspolicy relation */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, RowExclusiveLock);
    /* Open target table */
    Relation targetTable = relation_open(relid, NoLock);

    /* Check whether support RLS for this relation */
    SupportRlsForRel(targetTable);

    /* Check ploicy already exist or not */
    ScanKeyData scanKey[2];
    /* Bind relation oid and policy new name */
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(
        &scanKey[1], Anum_pg_rlspolicy_polname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(stmt->policyName));

    /* Search tuple by index */
    SysScanDesc scanDesc =
        systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 2, scanKey);
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);

    /* Policy does not exists */
    if (HeapTupleIsValid(rlsPolicyTuple) == false) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("row level security policy \"%s\" for relation \"%s\" does not exists",
                    stmt->policyName,
                    stmt->relation->relname)));
    }
    /* Get rls policy oid */
    Oid rlspolicyOid = HeapTupleGetOid(rlsPolicyTuple);

    /* Set up values and nulls arrays */
    Datum values[Natts_pg_rlspolicy] = {0};
    bool nulls[Natts_pg_rlspolicy] = {false};
    bool replaces[Natts_pg_rlspolicy] = {false};

    /* Get new role list for this policy */
    int roleNums = 0;
    Datum* roleOids = NULL;
    ArrayType* roleArray = NULL;
    bool isNull = false;
    if (stmt->roleList != NULL) {
        roleNums = 0;
        roleOids = RlsPolicyRolesToArray(stmt->roleList, roleNums);
        roleArray = construct_array(roleOids, roleNums, OIDOID, sizeof(Oid), true, 'i');
        values[Anum_pg_rlspolicy_polroles - 1] = PointerGetDatum(roleArray);
        nulls[Anum_pg_rlspolicy_polroles - 1] = false;
        replaces[Anum_pg_rlspolicy_polroles - 1] = true;
    } else {
        // Get the original role list, used for update dependency
        /* get policy apply role list */
        Datum datumValue =
            heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polroles, RelationGetDescr(pg_rlspolicy), &isNull);
        Assert(isNull == false);
        roleArray = DatumGetArrayTypeP(datumValue);
        roleNums = ARR_DIMS(roleArray)[0];
        Oid* roles = (Oid*)ARR_DATA_PTR(roleArray);
        roleOids = (Datum*)palloc(roleNums * sizeof(Datum));
        for (int i = 0; i < roleNums; i++) {
            roleOids[i] = ObjectIdGetDatum(roles[i]);
        }
    }

    /* Get using quals*/
    ParseState* usingQualState = NULL;
    RangeTblEntry* rte = NULL;
    Node* usingQual = NULL;
    char* usingQualStr = NULL;
    if (stmt->usingQual != NULL) {
        /* Transform USING qual */
        usingQualState = make_parsestate(NULL);
        /* Add target table info to using qual state */
        rte = addRangeTableEntryForRelation(usingQualState, targetTable, NULL, false, false);
        addRTEtoQuery(usingQualState, rte, false, true, true);
        /* Transform expr clause */
        usingQual = transformWhereClause(usingQualState, (Node*)copyObject(stmt->usingQual),
                                         EXPR_KIND_POLICY, "POLICY");
        /* Take cate of collations */
        assign_expr_collations(usingQualState, usingQual);
        /* Check whether contain RangeTblEntry in usingQual */
        RlsPolicyRteInfo rteInfo;
        rteInfo.checkedRelId = relid;
        rteInfo.skipRlsPolicyId = rlspolicyOid;  // skip old rls policy
        if (ContainRteInRlsPolicyWalker(usingQual, &rteInfo)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Alter row level security policy \"%s\" failed, because it will result in "
                           "infinite recursion for DML queries",
                        stmt->policyName)));
        }
        /* Change ParseState to text */
        usingQualStr = nodeToString(usingQual);
        values[Anum_pg_rlspolicy_polqual - 1] = CStringGetTextDatum(usingQualStr);
        nulls[Anum_pg_rlspolicy_polqual - 1] = false;
        replaces[Anum_pg_rlspolicy_polqual - 1] = true;
    } else {
        // Get the original using policy, used for update dependency
        Datum datumValue =
            heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polqual, RelationGetDescr(pg_rlspolicy), &isNull);
        if (isNull == false) {
            /* Transform USING qual */
            usingQualState = make_parsestate(NULL);
            /* Add target table info to using qual state */
            rte = addRangeTableEntryForRelation(usingQualState, targetTable, NULL, false, false);
            usingQualStr = TextDatumGetCString(datumValue);
            usingQual = (Node*)stringToNode(usingQualStr);
        }
    }

    /* Form new tuple, update catalog, no need to update index */
    HeapTuple newtuple = (HeapTuple) tableam_tops_modify_tuple(rlsPolicyTuple, RelationGetDescr(pg_rlspolicy), values, nulls, replaces);
    simple_heap_update(pg_rlspolicy, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(pg_rlspolicy, newtuple);

    /*  Remove old dependency for relation and exprs */
    (void)deleteDependencyRecordsFor(RlsPolicyRelationId, rlspolicyOid, false);

    /* Add new dependency */
    ObjectAddress myself;
    myself.classId = RlsPolicyRelationId;
    myself.objectId = rlspolicyOid;
    myself.objectSubId = 0;

    ObjectAddress referenced;
    referenced.classId = RelationRelationId;
    referenced.objectId = relid;
    referenced.objectSubId = 0;
    /* Policy id and relation id */
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
    /* Policy id and using quals */
    if (usingQualState == NULL) {
        return;
    }
    recordDependencyOnExpr(&myself, usingQual, usingQualState->p_rtable, DEPENDENCY_NORMAL);

    /* Remove old shared dependency for roles */
    deleteSharedDependencyRecordsFor(RlsPolicyRelationId, rlspolicyOid, 0);

    /* Add new shared dependency for roles */
    addShdependForRoles(roleNums, roleOids, &myself, &referenced);

    /*
     * Invalidate relation's relcache entry so that other backends (and this
     * one too!) are sent SI message to make them rebuild relcache entries.
     * (Ideally this should happen automatically...)
     */
    CacheInvalidateRelcache(targetTable);

    /* Release resource */
    if (usingQualState != NULL) {
        free_parsestate(usingQualState);
    }
    systable_endscan(scanDesc);
    heap_close(pg_rlspolicy, RowExclusiveLock);
    relation_close(targetTable, NoLock);
    return;
}

/*
 * RenameRlsPolicy
 *    handles the execution of the ALTER POLICY name RENAME command.
 *
 * @param (in) renameStmt: RenameStmt describes the policy name, table name and new rls policy name
 * @return: void
 */
ObjectAddress RenameRlsPolicy(RenameStmt* renameStmt)
{
    Oid rlsp_id;    
    ObjectAddress address;

    Assert(renameStmt != NULL);
    /* Check whether need to rename rls policy on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return InvalidObjectAddress;
    }
    /* Check license whether support this feature */
    LicenseSupportRls();

    /*
     * Get relation oid by name, grab an exclusive lock on the target table,
     * which we will NOT release until end of transaction.
     */
    Oid relid = InvalidOid;
    relid = RangeVarGetRelidExtended(renameStmt->relation,
        RowExclusiveLock,
        false,
        false,
        false,
        false,
        RangeVarCallbackForRlsPolicy,
        (void*)renameStmt);

    TrForbidAccessRbObject(RelationRelationId, relid, renameStmt->relation->relname);

    /* Open pg_rlspolicy relation */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, RowExclusiveLock);
    /* Open target table */
    Relation targetTable = relation_open(relid, NoLock);

    /* Check whether support RLS for this relation */
    SupportRlsForRel(targetTable);

    /* First search -- check new ploicy already exist or not */
    ScanKeyData scanKey[2];
    /* Bind relation oid and policy new name */
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(
        &scanKey[1], Anum_pg_rlspolicy_polname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(renameStmt->newname));

    /* Search tuple by index */
    SysScanDesc scanDesc =
        systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 2, scanKey);
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);

    /* Policy already exists */
    if (HeapTupleIsValid(rlsPolicyTuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("row level policy \"%s\" for relation \"%s\" already exists",
                    renameStmt->newname,
                    renameStmt->relation->relname)));
    }
    systable_endscan(scanDesc);

    /* Second search -- get old ploicy */
    /* Bind relation oid and policy new name */
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(
        &scanKey[1], Anum_pg_rlspolicy_polname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(renameStmt->subname));
    scanDesc = systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 2, scanKey);
    rlsPolicyTuple = systable_getnext(scanDesc);
    /* Policy does not exists */
    if (HeapTupleIsValid(rlsPolicyTuple) == false) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("row level security policy \"%s\" for relation \"%s\" does not exists",
                    renameStmt->subname,
                    renameStmt->relation->relname)));
    }
    rlsp_id = HeapTupleGetOid(rlsPolicyTuple);
    /* Copy tuple here, because of update index later */
    rlsPolicyTuple = (HeapTuple)tableam_tops_copy_tuple(rlsPolicyTuple);
    /* Update RLS policy name */
    (void)namestrcpy(&(((Form_pg_rlspolicy)GETSTRUCT(rlsPolicyTuple))->polname), renameStmt->newname);
    simple_heap_update(pg_rlspolicy, &rlsPolicyTuple->t_self, rlsPolicyTuple);
    CatalogUpdateIndexes(pg_rlspolicy, rlsPolicyTuple);

    /*
     * Invalidate relation's relcache entry so that other backends (and this
     * one too!) are sent SI message to make them rebuild relcache entries.
     * (Ideally this should happen automatically...)
     */
    CacheInvalidateRelcache(targetTable);

    /* Clean up resource */
    systable_endscan(scanDesc);
    heap_close(pg_rlspolicy, RowExclusiveLock);
    heap_close(targetTable, NoLock);
    ObjectAddressSet(address, RlsPolicyRelationId, rlsp_id);
    return address;
}

/*
 * RemoveRoleFromRlsPolicy
 *     Remove a role from a policy by roleid. If the role is removed successfully,
 *     then return true. If the role cannot be removed due to being the only role
 *     for the policy, then return false and the RLS policy will be force removed.
 *     If the role is not a member of the policy, then raise an error.
 *
 * @param (in) roleid - the oid of the role to remove
 * @param (in) rlsrelid - always be RlsPolicyRelationId
 * @param (in) rlspolicyid - the oid of the row level security policy
 */
bool RemoveRoleFromRlsPolicy(Oid roleid, Oid rlsrelid, Oid rlspolicyid)
{
    Assert(rlsrelid == RlsPolicyRelationId);
    /* Check whether need to remove role from rls policy */
    if (SupportRlsOnCurrentNode() == false) {
        return true;
    }

    /* search rls policy by rlspolicyid */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, RowExclusiveLock);
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, rlspolicyid);
    /* search tuple by index */
    SysScanDesc scanDesc = systable_beginscan(pg_rlspolicy, PgRlspolicyOidIndex, true, NULL, 1, scanKey);
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);
    if (false == HeapTupleIsValid(rlsPolicyTuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find tuple for policy %u", rlspolicyid)));
    }

    /* Get relation oid */
    Form_pg_rlspolicy rlspolicy = (Form_pg_rlspolicy)GETSTRUCT(rlsPolicyTuple);
    Oid relid = rlspolicy->polrelid;
    Relation relation = relation_open(relid, AccessShareLock);
    if (relation->rd_rel->relkind != RELKIND_RELATION) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a table", RelationGetRelationName(relation))));
    }

    /* Get role list for policy */
    Datum datumValue = 0;
    bool isNull = true;
    datumValue = heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polroles, RelationGetDescr(pg_rlspolicy), &isNull);
    Assert(false == isNull);
    ArrayType* roles = DatumGetArrayTypeP(datumValue);
    int roleNums = ARR_DIMS(roles)[0];
    Assert(roleNums >= 1);
    bool hasperm = pg_class_ownercheck(relid, GetUserId());

    /* Current do not have permission for table */
    if (false == hasperm) {
        ereport(WARNING,
            (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
                errmsg("role \"%s\" could not be removed from row level security policy \"%s\" on \"%s\"",
                    GetUserNameFromId(roleid),
                    NameStr(rlspolicy->polname),
                    RelationGetRelationName(relation))));
    }

    if (hasperm && roleNums > 1) {
        /* Set up values and nulls arrays */
        Datum values[Natts_pg_rlspolicy] = {0};
        bool nulls[Natts_pg_rlspolicy] = {false};
        bool replaces[Natts_pg_rlspolicy] = {false};
        Oid* oldroles = (Oid*)ARR_DATA_PTR(roles);
        Datum* newroles = NULL;
        newroles = (Datum*)palloc((roleNums - 1) * sizeof(Datum));
        int i = 0;
        int j = 0;
        for (i = 0; i < roleNums; i++) {
            if (roleid == oldroles[i])
                continue;
            newroles[j++] = ObjectIdGetDatum(oldroles[i]);
        }
        Assert(j == roleNums - 1);
        ArrayType* roleIds = construct_array(newroles, roleNums - 1, OIDOID, sizeof(Oid), true, 'i');
        values[Anum_pg_rlspolicy_polroles - 1] = PointerGetDatum(roleIds);
        nulls[Anum_pg_rlspolicy_polroles - 1] = false;
        replaces[Anum_pg_rlspolicy_polroles - 1] = true;
        HeapTuple newTuple = (HeapTuple) tableam_tops_modify_tuple(rlsPolicyTuple, RelationGetDescr(pg_rlspolicy), values, nulls, replaces);
        simple_heap_update(pg_rlspolicy, &newTuple->t_self, newTuple);
        CatalogUpdateIndexes(pg_rlspolicy, newTuple);

        /* Remove the old shared dependencies (roles) */
        deleteSharedDependencyRecordsFor(RlsPolicyRelationId, rlspolicyid, 0);

        /* Record the new shared dependencies (roles) */
        ObjectAddress target;
        ObjectAddress myself;
        myself.classId = RlsPolicyRelationId;
        myself.objectId = rlspolicyid;
        myself.objectSubId = 0;
        target.classId = AuthIdRelationId;
        target.objectSubId = 0;
        for (i = 0; i < roleNums - 1; i++) {
            target.objectId = DatumGetObjectId(newroles[i]);
            /* no need for dependency on the public role */
            if (target.objectId != ACL_ID_PUBLIC) {
                recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_RLSPOLICY);
            }
        }

        /* Invalidate relcache so that others see the new R.L.S policies */
        CacheInvalidateRelcache(relation);
        heap_freetuple(newTuple);
    }
    /* Clean up resource */
    systable_endscan(scanDesc);
    heap_close(pg_rlspolicy, RowExclusiveLock);
    relation_close(relation, AccessShareLock);
    return ((false == hasperm) || (roleNums > 1));
}

/*
 * RemoveRlsPolicyById
 *     Handle row level security poicy deletion.
 *
 * @param (in) rlsPolicyOid: row level seurity policy oid.
 * @return: void
 */
void RemoveRlsPolicyById(Oid rlsPolicyOid)
{
    /* Check whether need to remove rls policy on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return;
    }
    /* Check license whether support this feature */
    LicenseSupportRls();

    if (!OidIsValid(rlsPolicyOid)) {
        return;
    }
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(rlsPolicyOid));
    /* Open pg_rlspolicy relation */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, RowExclusiveLock);

    /* Search tuple by index */
    SysScanDesc scanDesc = systable_beginscan(pg_rlspolicy, PgRlspolicyOidIndex, true, NULL, 1, scanKey);

    HeapTuple tuple = systable_getnext(scanDesc);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find tuple for policy %u", rlsPolicyOid)));
    }

    /* Open and exclusive-lock the relation the trigger belongs to. */
    Oid relationOid = ((Form_pg_rlspolicy)GETSTRUCT(tuple))->polrelid;
    Assert(OidIsValid(relationOid));
    Relation targetTable = relation_open(relationOid, RowExclusiveLock);

    /* Check whether support RLS for this relation */
    SupportRlsForRel(targetTable);

    /* Delete the pg_rlspolicy tuple for the policy */
    simple_heap_delete(pg_rlspolicy, &tuple->t_self);
    systable_endscan(scanDesc);

    /* Notice to force all backends (including me!) to update
     * relcache entries with the new R.L.S policy set.
     */
    CacheInvalidateRelcache(targetTable);

    /* Remove tuple done */
    relation_close(targetTable, RowExclusiveLock);
    heap_close(pg_rlspolicy, RowExclusiveLock);
    return;
}

/*
 * get_rlspolicy_oid
 *     Look up a row level security policy by name to find its OID.
 *     If missing_ok is false, throw an error if policy not found.
 *     If true, just return InvalidOid.
 *
 * @param (in) relid: system table id.
 * @param (in) policy_name: row level security policy name.
 * @param (in) missing_ok: report error or just return InvalidOid when policy not found.
 * @return: policy oid of policy_name.
 */
Oid get_rlspolicy_oid(Oid relid, const char* policy_name, bool missing_ok)
{
    ScanKeyData scanKey[2];
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&scanKey[1], Anum_pg_rlspolicy_polname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(policy_name));

    /* Open pg_rlspolicy relation with AccessShareLock */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, AccessShareLock);
    Oid rlspolicy_oid = InvalidOid;

    /* Search old tuple by index */
    SysScanDesc scanDesc =
        systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 2, scanKey);
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);

    if (HeapTupleIsValid(rlsPolicyTuple)) {
        rlspolicy_oid = HeapTupleGetOid(rlsPolicyTuple);
    } else {
        if (missing_ok == false) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg(
                        "row level policy \"%s\" for table \"%s\" does not exists", policy_name, get_rel_name(relid))));
        }
        rlspolicy_oid = InvalidOid;
    }

    systable_endscan(scanDesc);
    heap_close(pg_rlspolicy, AccessShareLock);
    return rlspolicy_oid;
}

/*
 * Build row level security policies data to attach to the given relcache entry.
 *
 */
void RelationBuildRlsPolicies(Relation relation)
{
    Assert(RelationIsValid(relation));
    if (SupportRlsOnCurrentNode() == false) {
        return;
    }

    /* Open pg_rlspolicy relation */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, AccessShareLock);

    /* Use the index to search for the matching old tuple */
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0],
        Anum_pg_rlspolicy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)));

    /* Search old tuple by index */
    SysScanDesc scanDesc =
        systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 1, scanKey);

    MemoryContext oldcxt = NULL;
    /*
     * Set up memory context, always set up some kind of policy here.
     * If no explicit policies are found then an implicit default-deny policy is created.
     */
    MemoryContext rlscxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(),
        "Row-level-security policy descriptor",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);
    RlsPoliciesDesc* rlsDesc = (RlsPoliciesDesc*)MemoryContextAllocZero(rlscxt, sizeof(RlsPoliciesDesc));
    rlsDesc->rlsCxt = rlscxt;
    rlsDesc->rlsPolicies = NULL;
    relation->rd_rlsdesc = rlsDesc;

    Datum datumValue = 0;
    bool isNull = true;
    Oid policyOid = InvalidOid;
    Name policyName = NULL;
    char policyCommand = '\0';
    bool policyPermissive = true;
    ArrayType* roles = NULL;
    Expr* usingExpr = NULL;
    char* usingStr = NULL;
    int strLen = 0;
    error_t rc = 0;
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);
    while (HeapTupleIsValid(rlsPolicyTuple)) {
        oldcxt = MemoryContextSwitchTo(rlscxt);
        /* get policy oid */
        policyOid = HeapTupleGetOid(rlsPolicyTuple);
        Assert(OidIsValid(policyOid));
        /* get policy name */
        datumValue = heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polname, RelationGetDescr(pg_rlspolicy), &isNull);
        Assert(isNull == false);
        policyName = DatumGetName(datumValue);
        /* get policy command */
        datumValue = heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polcmd, RelationGetDescr(pg_rlspolicy), &isNull);
        Assert(isNull == false);
        policyCommand = DatumGetChar(datumValue);
        /* check policy is permissive or not */
        datumValue =
            heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polpermissive, RelationGetDescr(pg_rlspolicy), &isNull);
        Assert(isNull == false);
        policyPermissive = DatumGetBool(datumValue);
        /* get policy apply role list */
        datumValue = heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polroles, RelationGetDescr(pg_rlspolicy), &isNull);
        Assert(isNull == false);
        roles = DatumGetArrayTypeP(datumValue);
        /* get using qual */
        datumValue = heap_getattr(rlsPolicyTuple, Anum_pg_rlspolicy_polqual, RelationGetDescr(pg_rlspolicy), &isNull);
        if (isNull == false) {
            usingStr = TextDatumGetCString(datumValue);
            usingExpr = (Expr*)stringToNode(usingStr);
        } else {
            usingExpr = NULL;
        }

        /* construct RlsPolicy struct here */
        RlsPolicy* curPolicy = (RlsPolicy*)palloc0(sizeof(RlsPolicy));
        curPolicy->policyOid = policyOid;
        strLen = strlen(policyName->data);
        curPolicy->policyName = (char*)palloc0(strLen + 1);
        rc = strcpy_s(curPolicy->policyName, strLen + 1, policyName->data);
        securec_check(rc, "\0", "\0");
        curPolicy->cmdName = policyCommand;
        curPolicy->isPermissive = policyPermissive;
        curPolicy->hasSubLink = checkExprHasSubLink((Node*)usingExpr);
        curPolicy->roles = roles;
        curPolicy->usingExpr = (Expr*)copyObject(usingExpr);
        rlsDesc->rlsPolicies = lcons(curPolicy, rlsDesc->rlsPolicies);
        (void)MemoryContextSwitchTo(oldcxt);
        if (usingExpr != NULL) {
            pfree(usingExpr);
        }
        /* get next policy tuple */
        rlsPolicyTuple = systable_getnext(scanDesc);
    }

    /* Scan complete */
    systable_endscan(scanDesc);
    heap_close(pg_rlspolicy, AccessShareLock);
    return;
}

/*
 * ParseRlsPolicyCommandName
 *     Convert full command string to char representation.
 *
 * @param (in) cmdName: command string.
 * @return: char which represent RLS policy command.
 *          'a' represent "INSERT", 'r' represent "SELECT",
 *          'w' represent "UPDATE", 'd' represent "DELETE",
 *          otherwise return report error.
 */
static char ParseRlsPolicyCommandName(const char* cmdName)
{
    char policyCmd = '\0';

    if (cmdName == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized row level security policy command")));
    }

    if (strcmp(cmdName, "all") == 0) {
        policyCmd = '*';
    } else if (strcmp(cmdName, "select") == 0) {
        policyCmd = ACL_SELECT_CHR;
    } else if (strcmp(cmdName, "update") == 0) {
        policyCmd = ACL_UPDATE_CHR;
    } else if (strcmp(cmdName, "delete") == 0) {
        policyCmd = ACL_DELETE_CHR;
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized row level security policy command")));
    }

    return policyCmd;
}

/*
 * RlsPolicyRolesToArray
 *     Convert role name list to role oid array. If roles is NULL, return ACL_ID_PUBLIC directly.
 *
 * @param (in) roles: Role name list.
 * @param (out) numRoles: Number of roles in list.
 * @return: Datum, role oid array.
 */
static Datum* RlsPolicyRolesToArray(List* roles, int& numRoles)
{
    Datum* roleOids = NULL;

    /* If user do not specified, parser will autofill "public", never empty */
    numRoles = list_length(roles);
    Assert(numRoles > 0);
    if (numRoles > (int)(MAX_INT32 / sizeof(Datum))) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("too many roles specified in SQL statement.")));
    }
    roleOids = (Datum*)palloc(numRoles * sizeof(Datum));

    /* Get role oids by role names */
    ListCell* item = NULL;
    const char* roleName = NULL;
    Oid roleOid = InvalidOid;
    int currNum = 0;
    foreach (item, roles) {
        roleName = strVal(lfirst(item));

        /*
         * PUBLIC means that can cover all users, it makes sence alone.
         * When match role "public", set the unique role to ACL_ID_PUBLIC
         */
        if (strcmp(roleName, "public") == 0) {
            if (roles->length > 1) {
                ereport(WARNING,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Ignoring specified roles other than PUBLIC"),
                        errhint("All roles are members of the PUBLIC role.")));
            }
            roleOid = ACL_ID_PUBLIC;
            roleOids[0] = ObjectIdGetDatum(roleOid);
            numRoles = 1;
            return roleOids;
        } else if (strcmp(roleName, "current_user") == 0) {
            /* CURRENT_USER */
            roleOid = GetUserId();
        } else if (strcmp(roleName, "session_user") == 0) {
            /* SESSION_USER */
            roleOid = GetSessionUserId();
        } else {
            /* Normal User */
            roleOid = get_role_oid(roleName, false);
        }

        Assert((roleOid) != InvalidOid);
        roleOids[currNum] = ObjectIdGetDatum(roleOid);
        currNum++;
    }

    return roleOids;
}

/*
 * RangeVarCallbackForRlsPolicy
 *     Callback function for RangeVarGetRelidExtended().
 *     Checks the following:
 *      - the relation is a table.
 *      - current user owns the table.
 *
 * @param (in) relation: relation table info.
 * @param (in) relId: relation table Oid.
 * @param (in) oldRelId: old relation oid.
 * @param (in) isPartition: target is relation level or partition level.
 * @param (in) callbackArg: extra args for this function.
 * @return: void.
 */
static void RangeVarCallbackForRlsPolicy(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool isPartition, void* callbackArg)
{
    /* Get tuple info */
    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relId));
    /* Invalid relation name, return here, upper function should check */
    if (!HeapTupleIsValid(tuple))
        return;

    Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);
    /* Get some information from callbackArg */
    Node* stmt = (Node*)callbackArg;
    Assert(stmt != NULL);
    bool externalCommand = true;
    switch (nodeTag(stmt)) {
        case T_CreateRlsPolicyStmt:
            externalCommand = ((CreateRlsPolicyStmt*)callbackArg)->fromExternal;
            break;
        /* All the Rename|Alter RLS commands from external */
        case T_RenameStmt:
        case T_AlterRlsPolicyStmt:
            externalCommand = true;
            break;
        default:
            break;
    }

    /* For externalCommand, only the owner of the table can do this operation. */
    if (externalCommand && !pg_class_ownercheck(relId, GetUserId())) {
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, relation->relname);
    }

    /* Check this is system table or not */
    if (externalCommand && IsSystemClass(reltup)) {
        /* Do not support create R.L.S policy for system table by external interface(user side) */
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog", relation->relname)));
    }
    ReleaseSysCache(tuple);
    return;
}

/*
 * RelationHasRlspolicy
 *     Check this relation whether has Row Level Security policy.
 *
 * @param (in) relid: relation oid.
 * @return: bool, if this relation has RLS policy return true, else return false.
 */
bool RelationHasRlspolicy(Oid relid)
{
    /* Check this is valid relation oid */
    Assert(relid > 0);
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0], Anum_pg_rlspolicy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    /* Open pg_rlspolicy relation with AccessShareLock lock */
    Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, AccessShareLock);

    /* Search tuple by index */
    SysScanDesc scanDesc =
        systable_beginscan(pg_rlspolicy, PgRlspolicyPolrelidPolnameIndex, true, NULL, 1, scanKey);
    HeapTuple rlsPolicyTuple = systable_getnext(scanDesc);
    /* Find RLS policy for this relation */
    int findPolicy = false;
    if (HeapTupleIsValid(rlsPolicyTuple))
        findPolicy = true;
    /* Clean up resource */
    systable_endscan(scanDesc);
    heap_close(pg_rlspolicy, AccessShareLock);
    return findPolicy;
}

/*
 * CreateRlsPolicyForSystem
 *     create row level seccurity policy for system catalog
 *
 * @param (in)schemaName: system catalog schema name(pg_catalog)
 * @param (in)relName: system catalog relation name
 * @param (in)policyName: row level security policy name for relation
 * @param (in)funcName: function used for row level security policy
 * @param (in)relColname: relation column name used in function
 * @param (in)privType: pervate type('select', 'execute', etc)
 * @return: void
 */
void CreateRlsPolicyForSystem(
    char* schemaName, char* relName, char* policyName, char* funcName, char* relColname, char* privType)
{
    /* Check whether support RLS on current node */
    if (SupportRlsOnCurrentNode() == false) {
        return;
    }
    char rolName[NAMEDATALEN] = {0};
    char cmdName[NAMEDATALEN] = {0};
    FuncCall* currentuser = NULL;
    List* ldata = NIL;
    ColumnRef* column = NULL;
    A_Const* constval = NULL;

    /* RLS Policy apply to all users(public) */
    int rc = strcpy_s(rolName, sizeof(rolName), "public");
    securec_check(rc, "\0", "\0");
    /* RLS Policy only affect select */
    rc = strcpy_s(cmdName, sizeof(cmdName), "select");
    securec_check(rc, "\0", "\0");

    /* Function Call for current_user */
    currentuser = makeNode(FuncCall);
    currentuser->funcname = SystemFuncName("current_user");
    currentuser->args = NIL;
    currentuser->agg_order = NIL;
    currentuser->agg_star = FALSE;
    currentuser->agg_distinct = FALSE;
    currentuser->func_variadic = FALSE;
    currentuser->over = NULL;
    currentuser->location = -1;
    currentuser->call_func = false;
    /* reference to column */
    column = makeNode(ColumnRef);
    column->location = -1;
    column->fields = list_make1(makeString(relColname));
    /* Const string for privilege type */
    constval = makeNode(A_Const);
    constval->val.type = T_String;
    constval->val.val.str = privType;
    constval->location = -1;

    FuncCall* func = makeNode(FuncCall);
    ldata = NIL;
    ldata = lappend(ldata, makeString(schemaName));
    ldata = lappend(ldata, makeString(funcName));
    func->funcname = ldata;
    ldata = NIL;
    ldata = lappend(ldata, currentuser);
    ldata = lappend(ldata, column);
    ldata = lappend(ldata, constval);
    func->args = ldata;
    func->agg_order = NIL;
    func->agg_star = FALSE;
    func->agg_distinct = FALSE;
    func->func_variadic = FALSE;
    func->over = NULL;
    func->location = -1;
    func->call_func = false;

    CreateRlsPolicyStmt* rlsPolicyStmt = makeNode(CreateRlsPolicyStmt);
    rlsPolicyStmt->isPermissive = true;
    rlsPolicyStmt->fromExternal = false; /* internal */
    rlsPolicyStmt->policyName = policyName;
    rlsPolicyStmt->relation = makeRangeVar(schemaName, relName, -1);
    rlsPolicyStmt->cmdName = cmdName;
    rlsPolicyStmt->roleList = list_make1(makeString(rolName));
    rlsPolicyStmt->usingQual = (Node*)func;
    CreateRlsPolicy(rlsPolicyStmt);
}

/*
 * ContainRteInRlsPolicyWalker
 *     Check node whether contain specified RangeTblEntry
 *
 * @param (in)node: Node info
 * @param (in)rteInfo: rte information
 * @return: Find specified RangeTblEntry in this node or not
 */
static bool ContainRteInRlsPolicyWalker(Node* node, const RlsPolicyRteInfo* rteInfo)
{
    if (node == NULL) {
        return false;
    }

    /* Check range table entry */
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry* rte = (RangeTblEntry*)node;
        /* only check ordinary relation */
        if ((rte->rtekind != RTE_RELATION) || (rte->relkind != RELKIND_RELATION)) {
            return false;
        }

        /* Find the same range table entry or not */
        if (rte->relid == rteInfo->checkedRelId) {
            return true;
        }

        Assert(OidIsValid(rte->relid));
        Relation rel = relation_open(rte->relid, NoLock);
        if (!RelationIsValid(rel)) {
            return false;
        }
        bool ret = false;
        /* Check whether include infinite recursion expressuion in rls policies */
        if (rel->rd_rlsdesc) {
            ListCell* item = NULL;
            RlsPolicy* policy = NULL;
            foreach (item, rel->rd_rlsdesc->rlsPolicies) {
                policy = (RlsPolicy*)lfirst(item);
                /* skip specified row policy, used for alter row level security policy */
                if (policy->policyOid == rteInfo->skipRlsPolicyId) {
                    continue;
                }
                ret = expression_tree_walker(
                    (Node*)policy->usingExpr, (bool (*)())ContainRteInRlsPolicyWalker, (void*)rteInfo);
                if (ret) {
                    break;
                }
            }
        }
        relation_close(rel, NoLock);
        return ret;
    }
    /* Check query */
    if (IsA(node, Query)) {
        return query_tree_walker(
            (Query*)node, (bool (*)())ContainRteInRlsPolicyWalker, (void*)rteInfo, QTW_EXAMINE_RTES);
    }
    return expression_tree_walker(node, (bool (*)())ContainRteInRlsPolicyWalker, (void*)rteInfo);
}
