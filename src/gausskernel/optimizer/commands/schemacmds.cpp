/* -------------------------------------------------------------------------
 *
 * schemacmds.cpp
 *	  schema creation/manipulation commands
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/schemacmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gtm.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/gs_package.h"
#include "commands/dbcommands.h"
#include "commands/schemacmds.h"
#include "miscadmin.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "optimizer/pgxcplan.h"
#endif

static const int TABLE_TYPE_HDFS = 1;
static const int TABLE_TYPE_TIMESERIES = 2;
static const int TABLE_TYPE_ANY = 3;

static void AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId);
static StringInfo TableExistInSchema(Oid schemaOid, const int tableType);

/*
 * CREATE SCHEMA
 */

#ifdef PGXC
void CreateSchemaCommand(CreateSchemaStmt* stmt, const char* queryString, bool sentToRemote)
#else
void CreateSchemaCommand(CreateSchemaStmt* stmt, const char* queryString)
#endif
{
    const char* schemaName = stmt->schemaname;
    const char* authId = stmt->authid;
    bool hasBlockChain = stmt->hasBlockChain;
    Oid namespaceId;
    OverrideSearchPath* overridePath = NULL;
    List* parsetree_list = NIL;
    ListCell* parsetree_item = NULL;
    Oid owner_uid;
    Oid saved_uid;
    int save_sec_context;
    AclResult aclresult;
    char* queryStringwithinfo = (char*)queryString;

    GetUserIdAndSecContext(&saved_uid, &save_sec_context);
    if (IsExistPackageName(schemaName)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("schema name can not same as package"),
                errdetail("same package name exists"),
                errcause("schema name conflict"),
                erraction("rename schema name")));
    }
    /*
     * Who is supposed to own the new schema?
     */
    if (authId != NULL)
        owner_uid = get_role_oid(authId, false);
    else
        owner_uid = saved_uid;

    if (IS_PGXC_DATANODE) {
        if (isToastTempNamespaceName(stmt->schemaname))
            stmt->temptype = Temp_Toast;
        else if (isTempNamespaceName(stmt->schemaname))
            stmt->temptype = Temp_Rel;
    }

#ifdef PGXC
    char* FirstExecNode = NULL;
    bool isFirstNode = false;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        FirstExecNode = find_first_exec_cn();
        isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);
    }

    /* gen uuid before send query to other node */
    if (IS_MAIN_COORDINATOR)
        queryStringwithinfo = gen_hybirdmsg_for_CreateSchemaStmt(stmt, queryString);

    /*
     * If I am the main execute CN but not CCN,
     * Notify the CCN to create firstly, and then notify other CNs except me.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
            if (!sentToRemote) {
                RemoteQuery* step = makeNode(RemoteQuery);
                step->combine_type = COMBINE_TYPE_SAME;
                step->sql_statement = (char*)queryStringwithinfo;
                step->exec_type = (stmt->temptype == Temp_None ? EXEC_ON_COORDS : EXEC_ON_NONE);
                step->exec_nodes = NULL;
                step->is_temp = false;
                ExecRemoteUtility_ParallelDDLMode(step, FirstExecNode);
                pfree_ext(step);
            }
        }
    }
#endif

    if (stmt->temptype == Temp_None) {
        /*
         * To create a schema, must have schema-create privilege on the current
         * database and must be able to become the target role (this does not
         * imply that the target role itself must have create-schema privilege).
         * The latter provision guards against "giveaway" attacks.	Note that a
         * superuser will always have both of these privileges a fortiori.
         */
        aclresult = pg_database_aclcheck(u_sess->proc_cxt.MyDatabaseId, saved_uid, ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_DATABASE, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId));

        check_nodegroup_privilege(saved_uid, saved_uid, ACL_CREATE);

        check_is_member_of_role(saved_uid, owner_uid);

        /* Additional check to protect reserved schema names */
        //@Temp Table. We allow datanode to create pg_temp namespace to enable create namespace stmt by CN to execute on
        // DN
        if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
#ifdef ENABLE_MULTIPLE_NODES
            !IS_PGXC_DATANODE &&
#endif
            IsReservedName(schemaName))
            ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME),
                    errmsg("unacceptable schema name \"%s\"", schemaName),
                    errdetail("The prefix \"pg_\" is reserved for system schemas.")));
    }

    /*
     * If the requested authorization is different from the current user,
     * temporarily set the current user so that the object(s) will be created
     * with the correct ownership.
     *
     * (The setting will be restored at the end of this routine, or in case of
     * error, transaction abort will clean things up.)
     */
    if (saved_uid != owner_uid)
        SetUserIdAndSecContext(owner_uid, (uint32)save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

    /* Create the schema's namespace */
    namespaceId = NamespaceCreate(schemaName, owner_uid, false, hasBlockChain);

    /* Advance cmd counter to make the namespace visible */
    CommandCounterIncrement();

    SetTempNamespace((Node*)stmt, namespaceId);

    /*
     * Temporarily make the new namespace be the front of the search path, as
     * well as the default creation target namespace.  This will be undone at
     * the end of this routine, or upon error.
     */
    overridePath = GetOverrideSearchPath(CurrentMemoryContext);
    overridePath->schemas = lcons_oid(namespaceId, overridePath->schemas);

    /* If create temp schema, we should add the nspOid to current overrideStack. */
    if (((CreateSchemaStmt*)stmt)->temptype == Temp_Rel) {
        AddTmpNspToOverrideSearchPath(namespaceId);
    }

    /* XXX should we clear overridePath->useTemp? */
    PushOverrideSearchPath(overridePath);

    /*
     * Examine the list of commands embedded in the CREATE SCHEMA command, and
     * reorganize them into a sequentially executable order with no forward
     * references.	Note that the result is still a list of raw parsetrees ---
     * we cannot, in general, run parse analysis on one statement until we
     * have actually executed the prior ones.
     */
    parsetree_list = transformCreateSchemaStmt(stmt);

    if (!IS_MAIN_COORDINATOR && !IS_SINGLE_NODE)
        gen_uuid_for_CreateSchemaStmt(parsetree_list, stmt->uuids);

#ifdef PGXC
    /*
     * Add a RemoteQuery node for a query at top level on a remote Coordinator,
     * if not done already.
     */
    if (!sentToRemote) {
        if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
            parsetree_list = AddRemoteQueryNode(parsetree_list, queryStringwithinfo, EXEC_ON_DATANODES, false);
        } else {
            parsetree_list = AddRemoteQueryNode(parsetree_list,
                queryStringwithinfo,
                stmt->temptype == Temp_None ? EXEC_ON_ALL_NODES : EXEC_ON_DATANODES,
                false);
        }
    }
#endif

    bool withHashbucket = find_hashbucket_options(parsetree_list);
    /*
     * Execute each command contained in the CREATE SCHEMA.  Since the grammar
     * allows only utility commands in CREATE SCHEMA, there is no need to pass
     * them through parse_analyze() or the rewriter; we can just hand them
     * straight to ProcessUtility.
     */
    foreach (parsetree_item, parsetree_list) {
        Node* stmt_tmp = (Node*)lfirst(parsetree_item);

        /*
         * we have two basic rules for view
         * 1. system view on all datanodes and coordinators
         * 2. user-defined view only on coordinator
         *
         * From a different perspective
         * 1. if it is a coordinator, always create view
         * 2. if it is a datanode, skip create view unless
         *     a) create system view in initdb process
         *     b) create system view in inplaceUpgrade process
         *
         * In restore mode both coordinator and datanode are internally
         * treated as a datanode. so we allow ViewStmt in restore mode
         * anyway.
         *
         */
        if (IsA(stmt_tmp, ViewStmt) && IS_PGXC_DATANODE && !IS_SINGLE_NODE && !IsInitdb &&
            !u_sess->attr.attr_common.IsInplaceUpgrade && !isRestoreMode)
            continue;

        if (IsA(stmt_tmp, IndexStmt)) {
            IndexStmt* iStmt = (IndexStmt*)stmt_tmp;
            iStmt->skip_mem_check = true;
        }

        if (IsA(stmt_tmp, RemoteQuery) && withHashbucket) {
            RemoteQuery* rquery = (RemoteQuery*)stmt_tmp;
            rquery->is_send_bucket_map = true;
        }
        /* do this step */
        ProcessUtility(stmt_tmp,
            queryString,
            NULL,
            false, /* not top level */
            None_Receiver,
#ifdef PGXC
            true,
#endif /* PGXC */
            NULL);
        /* make sure later steps can see the object created here */
        CommandCounterIncrement();
    }

    /* Reset search path to normal state */
    PopOverrideSearchPath();

    /* Reset current user and security context */
    SetUserIdAndSecContext(saved_uid, save_sec_context);

    list_free(parsetree_list);
}

void AlterSchemaCommand(AlterSchemaStmt* stmt)
{
    char *nspName = stmt->schemaname;
    Assert(nspName != NULL);
    bool withBlockchain = stmt->hasBlockChain;
    bool nspIsBlockchain = false;
    HeapTuple tup;
    Relation rel;
    AclResult aclresult;
    const int STR_SCHEMA_NAME_LENGTH = 9;
    const int STR_SNAPSHOT_LENGTH = 8;

    if (withBlockchain && ((strncmp(nspName, "dbe_perf", STR_SCHEMA_NAME_LENGTH) == 0) ||
        (strncmp(nspName, "snapshot", STR_SNAPSHOT_LENGTH) == 0))) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("The schema '%s' doesn't allow to alter to blockchain schema", nspName)));
    }

    if (get_namespace_oid(nspName, true) == PG_BLOCKCHAIN_NAMESPACE) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("The schema blockchain doesn't allow to alter")));
    }

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(NAMESPACENAME, CStringGetDatum(nspName));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema \"%s\" does not exist", nspName)));

    /* Must be owner or have alter privilege of the schema. */
    aclresult = pg_namespace_aclcheck(HeapTupleGetOid(tup), GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_namespace_ownercheck(HeapTupleGetOid(tup), GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_NAMESPACE, nspName);
    }

    /* must have CREATE privilege on database */
    aclresult = pg_database_aclcheck(u_sess->proc_cxt.MyDatabaseId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_DATABASE, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId));

    if (withBlockchain && !g_instance.attr.attr_common.allowSystemTableMods &&
        !u_sess->attr.attr_common.IsInplaceUpgrade && IsReservedName(nspName))
        ereport(ERROR,
            (errcode(ERRCODE_RESERVED_NAME),
                errmsg("The system schema \"%s\" doesn't allow to alter to blockchain schema", nspName)));
    /*
     * If the any table exists in the schema, do not change to ledger schema.
     */
    StringInfo existTbl = TableExistInSchema(HeapTupleGetOid(tup), TABLE_TYPE_ANY);
    if (existTbl->len != 0) {
        if (withBlockchain) {
            ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME),
                    errmsg("It is not supported to change \"%s\" to blockchain schema which includes tables.",
                        nspName)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME),
                    errmsg("It is not supported to change \"%s\" to normal schema which includes tables.",
                        nspName)));
        }

    }

    /* modify nspblockchain attribute */
    bool is_null = true;
    Datum datum = SysCacheGetAttr(NAMESPACENAME, tup, Anum_pg_namespace_nspblockchain, &is_null);
    if (!is_null) {
        nspIsBlockchain = DatumGetBool(datum);
    }
    if (nspIsBlockchain != withBlockchain) {
        Datum new_record[Natts_pg_namespace] = {0};
        bool new_record_nulls[Natts_pg_namespace] = {false};
        bool new_record_repl[Natts_pg_namespace] = {false};

        new_record[Anum_pg_namespace_nspblockchain - 1] = BoolGetDatum(withBlockchain);
        new_record_repl[Anum_pg_namespace_nspblockchain - 1] = true;

        HeapTuple new_tuple = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel),
            new_record, new_record_nulls, new_record_repl);
        simple_heap_update(rel, &tup->t_self, new_tuple);
        /* Update indexes */
        CatalogUpdateIndexes(rel, new_tuple);
    }

    heap_close(rel, NoLock);
    tableam_tops_free_tuple(tup);
}

/*
 * Guts of schema deletion.
 */
void RemoveSchemaById(Oid schemaOid)
{
    Relation relation;
    HeapTuple tup;

    if (IsPerformanceNamespace(schemaOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("The schema 'dbe_perf' doesn't allow to drop")));
    }
    if (IsSnapshotNamespace(schemaOid)) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("The schema 'snapshot' doesn't allow to drop")));
    }

    if (schemaOid == PG_BLOCKCHAIN_NAMESPACE) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("The schema 'blockchain' doesn't allow to drop")));
    }

    relation = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for namespace %u", schemaOid)));

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * Rename schema
 */
void RenameSchema(const char* oldname, const char* newname)
{
    HeapTuple tup;
    Relation rel;
    AclResult aclresult;
    const int STR_SCHEMA_NAME_LENGTH = 9;
    const int STR_SNAPSHOT_LENGTH = 8;
    bool is_nspblockchain = false;
    if ((strncmp(oldname, "dbe_perf", STR_SCHEMA_NAME_LENGTH) == 0) ||
        (strncmp(oldname, "snapshot", STR_SNAPSHOT_LENGTH) == 0)) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("The schema '%s' doesn't allow to rename", oldname)));
    }

    if (get_namespace_oid(oldname, true) == PG_BLOCKCHAIN_NAMESPACE) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("The schema blockchain doesn't allow to rename")));
    }

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(NAMESPACENAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema \"%s\" does not exist", oldname)));

    /* make sure the new name doesn't exist */
    if (OidIsValid(get_namespace_oid(newname, true)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("schema \"%s\" already exists", newname)));

    /* Must be owner or have alter privilege of the schema. */
    aclresult = pg_namespace_aclcheck(HeapTupleGetOid(tup), GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_namespace_ownercheck(HeapTupleGetOid(tup), GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_NAMESPACE, oldname);
    }

    /* must have CREATE privilege on database */
    aclresult = pg_database_aclcheck(u_sess->proc_cxt.MyDatabaseId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_DATABASE, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId));

    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        IsReservedName(newname))
        ereport(ERROR,
            (errcode(ERRCODE_RESERVED_NAME),
                errmsg("unacceptable schema name \"%s\"", newname),
                errdetail("The prefix \"pg_\" is reserved for system schemas.")));
    /*
     * If the HDFS table or timeseries table exists in the schema, do not rename it.
     */
    StringInfo existDFSTbl = TableExistInSchema(HeapTupleGetOid(tup), TABLE_TYPE_HDFS);
    StringInfo existTimeSeriesTbl = TableExistInSchema(HeapTupleGetOid(tup), TABLE_TYPE_TIMESERIES);
    if (0 != existDFSTbl->len) {
        ereport(ERROR,
            (errcode(ERRCODE_RESERVED_NAME),
                errmsg("It is not supported to rename schema \"%s\" which includes DFS table \"%s\".",
                    oldname,
                    existDFSTbl->data)));
    }
    if (0 != existTimeSeriesTbl->len) {
        ereport(ERROR,
            (errcode(ERRCODE_RESERVED_NAME),
                errmsg("It is not supported to rename schema \"%s\" which includes timeseries table \"%s\".",
                    oldname,
                    existTimeSeriesTbl->data)));
    }

    /* Before rename schema (with blockchain) rename related ledger tables first */
    bool is_null = true;
    Datum datum = SysCacheGetAttr(NAMESPACENAME, tup, Anum_pg_namespace_nspblockchain, &is_null);
    if (!is_null) {
        is_nspblockchain = DatumGetBool(datum);
    }

    if (is_nspblockchain) {
        List *related_table = namespace_get_depended_relid(HeapTupleGetOid(tup));
        rename_histlist_by_newnsp(related_table, newname);
        if (related_table != NIL) {
            list_free(related_table);
        }
    }

    /* rename */
    (void)namestrcpy(&(((Form_pg_namespace)GETSTRUCT(tup))->nspname), newname);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    heap_close(rel, NoLock);
    tableam_tops_free_tuple(tup);
}

/*
 * Whether or not HDFS or timeseries table exists in Scheam.
 * @_in_param schemaName: The schema name to be checked.
 * @return Return StringInfo of schema name if exists, otherwise return
 * StringInfo, data of which is empty.
 */
static StringInfo TableExistInSchema(Oid namespaceId, const int tableType)
{
    ScanKeyData key[2];
    TableScanDesc scan;
    HeapTuple tuple;
    Relation classRel;
    StringInfo existTbl = makeStringInfo();

    ScanKeyInit(&key[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));
    ScanKeyInit(&key[1], Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_RELATION));

    classRel = heap_open(RelationRelationId, AccessShareLock);
    scan = tableam_scan_begin(classRel, SnapshotNow, 2, key);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Oid RelOid = HeapTupleGetOid(tuple);

        Relation rel = relation_open(RelOid, AccessShareLock);
        if (tableType == TABLE_TYPE_ANY) {
            appendStringInfo(existTbl, "%s", RelationGetRelationName(rel));
            relation_close(rel, AccessShareLock);
            break;
        } else if (tableType == TABLE_TYPE_HDFS && RelationIsDfsStore(rel)) {
            appendStringInfo(existTbl, "%s", RelationGetRelationName(rel));
            relation_close(rel, AccessShareLock);
            break;
        } else if (tableType == TABLE_TYPE_TIMESERIES && RelationIsTsStore(rel)) {
            appendStringInfo(existTbl, "%s", RelationGetRelationName(rel));
            relation_close(rel, AccessShareLock);
            break;
        }

        relation_close(rel, AccessShareLock);
    }

    tableam_scan_end(scan);
    heap_close(classRel, AccessShareLock);

    return existTbl;
}

void AlterSchemaOwner_oid(Oid oid, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(oid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for schema %u", oid)));

    AlterSchemaOwner_internal(tup, rel, newOwnerId);

    ReleaseSysCache(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Change schema owner
 */
void AlterSchemaOwner(const char* name, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACENAME, CStringGetDatum(name));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema \"%s\" does not exist", name)));

    AlterSchemaOwner_internal(tup, rel, newOwnerId);

    ReleaseSysCache(tup);

    heap_close(rel, RowExclusiveLock);
}

static void AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId)
{
    Form_pg_namespace nspForm;

    Assert(tup->t_tableOid == NamespaceRelationId);
    Assert(RelationGetRelid(rel) == NamespaceRelationId);

    nspForm = (Form_pg_namespace)GETSTRUCT(tup);

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (nspForm->nspowner != newOwnerId) {
        Datum repl_val[Natts_pg_namespace];
        bool repl_null[Natts_pg_namespace];
        bool repl_repl[Natts_pg_namespace];
        Acl* newAcl = NULL;
        Datum aclDatum;
        bool isNull = false;
        HeapTuple newtuple;
        AclResult aclresult;
        Oid nspid = HeapTupleGetOid(tup);

        /* Otherwise, must be owner of the existing object */
        if (IsSystemNamespace(nspid)) {
            if (!initialuser()) {
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_NAMESPACE, NameStr(nspForm->nspname));
            }
        } else if (!pg_namespace_ownercheck(nspid, GetUserId())) {
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_NAMESPACE, NameStr(nspForm->nspname));
        }

        /* Must be able to become new owner */
        check_is_member_of_role(GetUserId(), newOwnerId);

        /*
         * must have create-schema rights
         *
         * NOTE: This is different from other alter-owner checks in that the
         * current user is checked for create privileges instead of the
         * destination owner.  This is consistent with the CREATE case for
         * schemas.  Because superusers will always have this right, we need
         * no special case for them.
         */
        aclresult = pg_database_aclcheck(u_sess->proc_cxt.MyDatabaseId, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_DATABASE, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId));
        errno_t errorno = EOK;
        errorno = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
        securec_check(errorno, "\0", "\0");
        errorno = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
        securec_check(errorno, "\0", "\0");

        repl_repl[Anum_pg_namespace_nspowner - 1] = true;
        repl_val[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = SysCacheGetAttr(NAMESPACENAME, tup, Anum_pg_namespace_nspacl, &isNull);
        if (!isNull) {
            newAcl = aclnewowner(DatumGetAclP(aclDatum), nspForm->nspowner, newOwnerId);
            repl_repl[Anum_pg_namespace_nspacl - 1] = true;
            repl_val[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(newAcl);
        }

        newtuple = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

        simple_heap_update(rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);

        tableam_tops_free_tuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(NamespaceRelationId, HeapTupleGetOid(tup), newOwnerId);
    }
}
