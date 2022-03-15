/* -------------------------------------------------------------------------
 *
 * namespace.cpp
 *	  code to support accessing and searching namespaces
 *
 * This is separate from pg_namespace.c, which contains the routines that
 * directly manipulate the pg_namespace system catalog.  This module
 * provides routines associated with defining a "namespace search path"
 * and implementing search-path-controlled searches.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/namespace.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "access/xlog.h"
#ifdef PGXC
#include "access/transam.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/redistrib.h"
#endif
#include "catalog/gs_client_global_keys.h"
#include "catalog/gs_column_keys.h"
#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_conversion_fn.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "catalog/gs_package_fn.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_group.h"
#include "catalog/indexing.h"
#include "catalog/gs_db_privilege.h"
#include "commands/dbcommands.h"
#include "commands/proclang.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/sinval.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/pl_package.h"
#include "tcop/utility.h"
#include "nodes/nodes.h"
#include "c.h"
#include "pgstat.h"
#include "catalog/pg_proc_fn.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "streaming/planner.h"
#endif
#define MAXSTRLEN ((1 << 11) - 1)
/*
 * The namespace search path is a possibly-empty list of namespace OIDs.
 * In addition to the explicit list, implicitly-searched namespaces
 * may be included:
 *
 * 1. If a TEMP table namespace has been initialized in this session, it
 * is implicitly searched first.  (The only time this doesn't happen is
 * when we are obeying an override search path spec that says not to use the
 * temp namespace, or the temp namespace is included in the explicit list.)
 *
 * 2. The system catalog namespace is always searched.	If the system
 * namespace is present in the explicit path then it will be searched in
 * the specified order; otherwise it will be searched after TEMP tables and
 * *before* the explicit list.	(It might seem that the system namespace
 * should be implicitly last, but this behavior appears to be required by
 * SQL99.  Also, this provides a way to search the system namespace first
 * without thereby making it the default creation target namespace.)
 *
 * For security reasons, searches using the search path will ignore the temp
 * namespace when searching for any object type other than relations and
 * types.  (We must allow types since temp tables have rowtypes.)
 *
 * The default creation target namespace is always the first element of the
 * explicit list.  If the explicit list is empty, there is no default target.
 *
 * The textual specification of search_path can include "$user" to refer to
 * the namespace named the same as the current user, if any.  (This is just
 * ignored if there is no such namespace.)	Also, it can include "pg_temp"
 * to refer to the current backend's temp namespace.  This is usually also
 * ignorable if the temp namespace hasn't been set up, but there's a special
 * case: if "pg_temp" appears first then it should be the default creation
 * target.	We kluge this case a little bit so that the temp namespace isn't
 * set up until the first attempt to create something in it.  (The reason for
 * klugery is that we can't create the temp namespace outside a transaction,
 * but initial GUC processing of search_path happens outside a transaction.)
 * activeTempCreationPending is TRUE if "pg_temp" appears first in the string
 * but is not reflected in activeCreationNamespace because the namespace isn't
 * set up yet.
 *
 * In bootstrap mode, the search path is set equal to "pg_catalog", so that
 * the system namespace is the only one searched or inserted into.
 * initdb is also careful to set search_path to "pg_catalog" for its
 * post-bootstrap standalone backend runs.	Otherwise the default search
 * path is determined by GUC.  The factory default path contains the PUBLIC
 * namespace (if it exists), preceded by the user's personal namespace
 * (if one exists).
 *
 * We support a stack of "override" search path settings for use within
 * specific sections of backend code.  namespace_search_path is ignored
 * whenever the override stack is nonempty.  activeSearchPath is always
 * the actually active path; it points either to the search list of the
 * topmost stack entry, or to baseSearchPath which is the list derived
 * from namespace_search_path.
 *
 * If baseSearchPathValid is false, then baseSearchPath (and other
 * derived variables) need to be recomputed from namespace_search_path.
 * We mark it invalid upon an assignment to namespace_search_path or receipt
 * of a syscache invalidation event for pg_namespace.  The recomputation
 * is done during the next non-overridden lookup attempt.  Note that an
 * override spec is never subject to recomputation.
 *
 * Any namespaces mentioned in namespace_search_path that are not readable
 * by the current user ID are simply left out of baseSearchPath; so
 * we have to be willing to recompute the path when current userid changes.
 * namespaceUser is the userid the path has been computed for.
 *
 * Note: all data pointed to by these List variables is in t_thrd.top_mem_cxt.
 */

/* Local functions */
static void InitTempTableNamespace(void);
static void RemoveTempRelations(Oid tempNamespaceId);
static void NamespaceCallback(Datum arg, int cacheid, uint32 hashvalue);
static bool MatchNamedCall(HeapTuple proctup, int nargs, List* argnames, int** argnumbers, bool include_out);
static bool CheckTSObjectVisible(NameData name, SysCacheIdentifier id, Oid nmspace);
static bool InitTempTblNamespace();
static void CheckTempTblAlias();
static Oid GetTSObjectOid(const char* objname, SysCacheIdentifier id);
static FuncCandidateList FuncnameAddCandidates(FuncCandidateList resultList, HeapTuple procTup, List* argNames,
    Oid namespaceId, Oid objNsp, int nargs, CatCList* catList, bool expandVariadic, bool expandDefaults,
    bool includeOut, Oid refSynOid, bool enable_outparam_override = false);
#ifdef ENABLE_UT
void dropExistTempNamespace(char* namespaceName);
#else
static void dropExistTempNamespace(char* namespaceName);
#endif

/* These don't really need to appear in any header file */
Datum pg_table_is_visible(PG_FUNCTION_ARGS);
Datum pg_type_is_visible(PG_FUNCTION_ARGS);
Datum pg_function_is_visible(PG_FUNCTION_ARGS);
Datum pg_operator_is_visible(PG_FUNCTION_ARGS);
Datum pg_opclass_is_visible(PG_FUNCTION_ARGS);
Datum pg_opfamily_is_visible(PG_FUNCTION_ARGS);
Datum pg_collation_is_visible(PG_FUNCTION_ARGS);
Datum pg_conversion_is_visible(PG_FUNCTION_ARGS);
Datum pg_ts_parser_is_visible(PG_FUNCTION_ARGS);
Datum pg_ts_dict_is_visible(PG_FUNCTION_ARGS);
Datum pg_ts_template_is_visible(PG_FUNCTION_ARGS);
Datum pg_ts_config_is_visible(PG_FUNCTION_ARGS);
Datum pg_my_temp_schema(PG_FUNCTION_ARGS);
Datum pg_is_other_temp_schema(PG_FUNCTION_ARGS);

/*
 * RangeVarGetRelid
 *		Given a RangeVar describing an existing relation,
 *		select the proper namespace and look up the relation OID.
 *
 * If the relation is not found, return InvalidOid if missing_ok = true,
 * otherwise raise an error.
 *
 * If nowait = true, throw an error if we'd have to wait for a lock.
 *
 * If isSupportSynonym = true, means that we regard it as a synonym to search
 * for its referenced object name when relation not found.
 *
 * Callback allows caller to check permissions or acquire additional locks
 * prior to grabbing the relation lock.
 *
 * target_is_partition: the operation assigned by caller of RangeVarGetRelidExtended(),
 *                             will be pushed on relation level or partition level?
 *
 * detailInfo: more detail infomation during synonym mapping and checking.
 *
 * refSynOid: store the referenced synonym Oid if mapping successfully.
 */
Oid RangeVarGetRelidExtended(const RangeVar* relation, LOCKMODE lockmode, bool missing_ok, bool nowait,
    bool target_is_partition, bool isSupportSynonym, RangeVarGetRelidCallback callback, void* callback_arg,
    StringInfo detailInfo, Oid* refSynOid)
{
    Oid relId;
    Oid oldRelId = InvalidOid;
    bool retry = false;
    char* errDetail = NULL;

    /*
     * We check the catalog name and then ignore it.
     */
    if (relation->catalogname) {
        char* database_name = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if ((database_name == NULL) || (strcmp(relation->catalogname, database_name) != 0)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cross-database references are not implemented: \"%s.%s.%s\"",
                        relation->catalogname,
                        relation->schemaname,
                        relation->relname)));
        }
    }

    /*
     * DDL operations can change the results of a name lookup.	Since all such
     * operations will generate invalidation messages, we keep track of
     * whether any such messages show up while we're performing the operation,
     * and retry until either (1) no more invalidation messages show up or (2)
     * the answer doesn't change.
     *
     * But if lockmode = NoLock, then we assume that either the caller is OK
     * with the answer changing under them, or that they already hold some
     * appropriate lock, and therefore return the first answer we get without
     * checking for invalidation messages.	Also, if the requested lock is
     * already held, no LockRelationOid will not AcceptInvalidationMessages,
     * so we may fail to notice a change.  We could protect against that case
     * by calling AcceptInvalidationMessages() before beginning this loop, but
     * that would add a significant amount overhead, so for now we don't.
     */
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        /*
         * Remember this value, so that, after looking up the relation name
         * and locking its OID, we can check whether any invalidation messages
         * have been processed that might require a do-over.
         */
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }

        /*
         * Some non-default relpersistence value may have been specified.  The
         * parser never generates such a RangeVar in simple DML, but it can
         * happen in contexts such as "CREATE TEMP TABLE foo (f1 int PRIMARY
         * KEY)".  Such a command will generate an added CREATE INDEX
         * operation, which must be careful to find the temp table, even when
         * pg_temp is not first in the search path.
         */
        if (relation->relpersistence == RELPERSISTENCE_TEMP) {
            if (!OidIsValid(u_sess->catalog_cxt.myTempNamespace))
                relId = InvalidOid; /* this probably can't happen? */
            else {
                if (relation->schemaname) {
                    Oid namespaceId;

                    namespaceId = LookupExplicitNamespace(relation->schemaname);
                    if (namespaceId != u_sess->catalog_cxt.myTempNamespace)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                errmsg("temporary tables cannot specify a schema name")));
                }
                pfree_ext(errDetail);
                errDetail = get_relname_relid_extend(
                    relation->relname, u_sess->catalog_cxt.myTempNamespace, &relId, isSupportSynonym, refSynOid);
            }
        } else if (relation->schemaname) {
            Oid namespaceId;

            /* use exact schema given */
            namespaceId = LookupExplicitNamespace(relation->schemaname);
            pfree_ext(errDetail);
            errDetail = get_relname_relid_extend(relation->relname, namespaceId, &relId, isSupportSynonym, refSynOid);

            if (OidIsValid(relId) && namespaceId == u_sess->catalog_cxt.myTempNamespace)
                (void)checkGroup(relId, false);
        } else {
            /* search the namespace path */
            if (isSupportSynonym) {
                pfree_ext(errDetail);
                errDetail = RelnameGetRelidExtended(relation->relname, &relId, refSynOid, detailInfo);
            } else {
                relId = RelnameGetRelid(relation->relname, detailInfo);
            }
        }

        /*
         * Invoke caller-supplied callback, if any.
         *
         * This callback is a good place to check permissions: we haven't
         * taken the table lock yet (and it's really best to check permissions
         * before locking anything!), but we've gotten far enough to know what
         * OID we think we should lock.  Of course, concurrent DDL might
         * change things while we're waiting for the lock, but in that case
         * the callback will be invoked again for the new OID.
         */
        if (callback)
            callback(relation, relId, oldRelId, target_is_partition, callback_arg);

        /*
         * If no lock requested, we assume the caller knows what they're
         * doing.  They should have already acquired a heavyweight lock on
         * this relation earlier in the processing of this same statement, so
         * it wouldn't be appropriate to AcceptInvalidationMessages() here, as
         * that might pull the rug out from under them.
         */
        if (lockmode == NoLock)
            break;

        /*
         * If, upon retry, we get back the same OID we did last time, then the
         * invalidation messages we processed did not change the final answer.
         * So we're done.
         *
         * If we got a different OID, we've locked the relation that used to
         * have this name rather than the one that does now.  So release the
         * lock.
         */
        if (retry) {
            if (relId == oldRelId)
                break;
            if (OidIsValid(oldRelId))
                UnlockRelationOid(oldRelId, lockmode);
        }

        /*
         * We need to tell LockCheckConflict if we could cancel redistribution xact
         * if we are doing drop/truncate table, redistribution xact is holding
         * the lock on our relId and relId is being redistributed now.
         */
        if (u_sess->exec_cxt.could_cancel_redistribution) {
            u_sess->catalog_cxt.redistribution_cancelable = CheckRelationInRedistribution(relId);
            u_sess->exec_cxt.could_cancel_redistribution = false;
        }

        /*
         * Lock relation.  This will also accept any pending invalidation
         * messages.  If we got back InvalidOid, indicating not found, then
         * there's nothing to lock, but we accept invalidation messages
         * anyway, to flush any negative catcache entries that may be
         * lingering.
         */
        if (!OidIsValid(relId))
            AcceptInvalidationMessages();
        else if (!nowait)
            LockRelationOid(relId, lockmode);
        else if (!ConditionalLockRelationOid(relId, lockmode)) {
            if (relation->schemaname)
                ereport(ERROR,
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg(
                            "could not obtain lock on relation \"%s.%s\"", relation->schemaname, relation->relname)));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("could not obtain lock on relation \"%s\"", relation->relname)));
        }

        /*
         * If no invalidation message were processed, we're done!
         */
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                break;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                break;
            }
        }

        /*
         * Something may have changed.	Let's repeat the name lookup, to make
         * sure this name still references the same relation it did
         * previously.
         */
        retry = true;
        oldRelId = relId;
    }

    /* If using synonym is allowed, firstly must check errDetail info and report it. */
    if (isSupportSynonym && errDetail != NULL && strlen(errDetail) > 0) {
        if (!missing_ok) {
            if (relation->schemaname) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation \"%s.%s\" does not exist", relation->schemaname, relation->relname),
                        errdetail("%s", errDetail)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation \"%s\" does not exist", relation->relname),
                        errdetail("%s", errDetail)));
            }
        } else if (detailInfo != NULL) {
            /* Skipping report error, but store the error detail info and report later. */
            appendStringInfo(detailInfo, _("%s"), errDetail);
        }
    }
    pfree_ext(errDetail);

    if (!OidIsValid(relId) && !missing_ok) {
        if (relation->schemaname)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("relation \"%s.%s\" does not exist", relation->schemaname, relation->relname)));
        else
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", relation->relname)));
    }
    return relId;
}

/*
 * RangeVarGetCreationNamespace
 *		Given a RangeVar describing a to-be-created relation,
 *		choose which namespace to create it in.
 *
 * Note: calling this may result in a CommandCounterIncrement operation.
 * That will happen on the first request for a temp table in any particular
 * backend run; we will need to either create or clean out the temp schema.
 */
Oid RangeVarGetCreationNamespace(const RangeVar* newRelation)
{
    Oid namespaceId;

    /*
     * We check the catalog name and then ignore it.
     */
    if (newRelation->catalogname) {
        if (strcmp(newRelation->catalogname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cross-database references are not implemented: \"%s.%s.%s\"",
                        newRelation->catalogname,
                        newRelation->schemaname,
                        newRelation->relname)));
    }

    if (newRelation->schemaname) {
        /* check for pg_temp alias */
        if (strcmp(newRelation->schemaname, "pg_temp") == 0) {
            CheckTempTblAlias();
            return u_sess->catalog_cxt.myTempNamespace;
        }
        /* use exact schema given */
        namespaceId = get_namespace_oid(newRelation->schemaname, false);
        /* we do not check for USAGE rights here! */
    } else if (newRelation->relpersistence == RELPERSISTENCE_TEMP) {
        CheckTempTblAlias();
        return u_sess->catalog_cxt.myTempNamespace;
    } else {
        namespaceId = GetOidBySchemaName();
    }

    /* Note: callers will check for CREATE rights when appropriate */

    return namespaceId;
}

bool CheckRelationCreateAnyPrivilege(Oid userId, char relkind)
{
    AclResult aclResult = ACLCHECK_NO_PRIV;
    switch (relkind) {
        case RELKIND_COMPOSITE_TYPE:
            if (HasSpecAnyPriv(userId, CREATE_ANY_TYPE, false)) {
                aclResult = ACLCHECK_OK;
            }
            break;
        /* sequence object */
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            if (HasSpecAnyPriv(userId, CREATE_ANY_SEQUENCE, false)) {
                aclResult = ACLCHECK_OK;
            }
            break;
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
            if (HasSpecAnyPriv(userId, CREATE_ANY_INDEX, false)) {
                aclResult = ACLCHECK_OK;
            }
            break;
        /* table */
        default:
            if (HasSpecAnyPriv(userId, CREATE_ANY_TABLE, false)) {
                aclResult = ACLCHECK_OK;
            }
            break;
    }
    return (aclResult == ACLCHECK_OK) ? true : false;
}

/*
 * checking whether the user has create any permission
 */
bool CheckCreatePrivilegeInNamespace(Oid namespaceId, Oid roleId, const char* anyPrivilege)
{
    /* Check we have creation rights in target namespace */
    AclResult aclResult = pg_namespace_aclcheck(namespaceId, roleId, ACL_CREATE);
    /*
     * anyResult is true, explain that the current user is granted create any permission
     */
    bool anyResult = false;
    if (aclResult != ACLCHECK_OK && !IsSysSchema(namespaceId)) {
        anyResult = HasSpecAnyPriv(roleId, anyPrivilege, false);
    }
    if (aclResult != ACLCHECK_OK && !anyResult) {
        aclcheck_error(aclResult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
    }
    return anyResult;
}

static void CheckCreateRelPrivilegeInNamespace(char relkind, Oid namespaceId)
{
    bool anyResult = false;
    AclResult aclResult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
    if (aclResult != ACLCHECK_OK && !IsSysSchema(namespaceId)) {
        if (relkind != '\0') {
            anyResult = CheckRelationCreateAnyPrivilege(GetUserId(), relkind);
        }
    }
    if (aclResult != ACLCHECK_OK && (!anyResult)) {
        aclcheck_error(aclResult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
    }
}

/*
 * RangeVarGetAndCheckCreationNamespace
 *
 * This function returns the OID of the namespace in which a new relation
 * with a given name should be created.  If the user does not have CREATE
 * permission on the target namespace, this function will instead signal
 * an ERROR.
 *
 * If non-NULL, *existing_oid is set to the OID of any existing relation with
 * the same name which already exists in that namespace, or to InvalidOid if
 * no such relation exists.
 *
 * If lockmode != NoLock, the specified lock mode is acquire on the existing
 * relation, if any, provided that the current user owns the target relation.
 * However, if lockmode != NoLock and the user does not own the target
 * relation, we throw an ERROR, as we must not try to lock relations the
 * user does not have permissions on.
 *
 * As a side effect, this function acquires AccessShareLock on the target
 * namespace.  Without this, the namespace could be dropped before our
 * transaction commits, leaving behind relations with relnamespace pointing
 * to a no-longer-exstant namespace.
 *
 * As a further side-effect, if the select namespace is a temporary namespace,
 * we mark the RangeVar as RELPERSISTENCE_TEMP.
 */
Oid RangeVarGetAndCheckCreationNamespace(RangeVar* relation, LOCKMODE lockmode,
    Oid* existing_relation_id, char relkind)
{
    Oid relid;
    Oid oldrelid = InvalidOid;
    Oid nspid;
    Oid oldnspid = InvalidOid;
    bool retry = false;

    /*
     * We check the catalog name and then ignore it.
     */
    if (relation->catalogname) {
        if (strcmp(relation->catalogname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cross-database references are not implemented: \"%s.%s.%s\"",
                        relation->catalogname,
                        relation->schemaname,
                        relation->relname)));
    }

    /*
     * As in RangeVarGetRelidExtended(), we guard against concurrent DDL
     * operations by tracking whether any invalidation messages are processed
     * while we're doing the name lookups and acquiring locks.  See comments
     * in that function for a more detailed explanation of this logic.
     */
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }
        /* Look up creation namespace and check for existing relation. */
        nspid = RangeVarGetCreationNamespace(relation);
        Assert(OidIsValid(nspid));
        if (existing_relation_id != NULL)
            relid = get_relname_relid(relation->relname, nspid);
        else
            relid = InvalidOid;

        /*
         * In bootstrap processing mode, we don't bother with permissions or
         * locking.  Permissions might not be working yet, and locking is
         * unnecessary.
         */
        if (IsBootstrapProcessingMode())
            break;
        /* Check namespace permissions. */
        CheckCreateRelPrivilegeInNamespace(relkind, nspid);
        if (retry) {
            /* If nothing changed, we're done. */
            if (relid == oldrelid && nspid == oldnspid)
                break;
            /* If creation namespace has changed, give up old lock. */
            if (nspid != oldnspid)
                UnlockDatabaseObject(NamespaceRelationId, oldnspid, 0, AccessShareLock);
            /* If name points to something different, give up old lock. */
            bool isDifferent = relid != oldrelid && OidIsValid(oldrelid) && lockmode != NoLock;
            if (isDifferent)
                UnlockRelationOid(oldrelid, lockmode);
        }

        /* Lock namespace. */
        if (nspid != oldnspid)
            LockDatabaseObject(NamespaceRelationId, nspid, 0, AccessShareLock);

        /* Lock relation, if required if and we have permission. */
        if (lockmode != NoLock && OidIsValid(relid)) {
            if (!pg_class_ownercheck(relid, GetUserId()))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, relation->relname);
            if (relid != oldrelid)
                LockRelationOid(relid, lockmode);
        }

        /* If no invalidation message were processed, we're done! */
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                break;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                break;
            }
        }

        /* Something may have changed, so recheck our work. */
        retry = true;
        oldrelid = relid;
        oldnspid = nspid;
    }

    RangeVarAdjustRelationPersistence(relation, nspid);
    if (existing_relation_id != NULL)
        *existing_relation_id = relid;
    return nspid;
}

/*
 * Adjust the relpersistence for an about-to-be-created relation based on the
 * creation namespace, and throw an error for invalid combinations.
 */
void RangeVarAdjustRelationPersistence(RangeVar* newRelation, Oid nspid)
{
    switch (newRelation->relpersistence) {
        case RELPERSISTENCE_TEMP:
            if (!isTempOrToastNamespace(nspid)) {
                if (isAnyTempNamespace(nspid))
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("cannot create relations in temporary schemas of other sessions")));
                else
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("cannot create temporary relation in non-temporary schema")));
            }
            break;
        case RELPERSISTENCE_GLOBAL_TEMP:       /* global temp table */
            if (isAnyTempNamespace(nspid))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                         errmsg("cannot create global temp relations in temporary schemas")));
            break;
        case RELPERSISTENCE_PERMANENT:
            if (isTempOrToastNamespace(nspid))
                newRelation->relpersistence = RELPERSISTENCE_TEMP;
            else if (isAnyTempNamespace(nspid))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("cannot create relations in temporary schemas of other sessions")));
            break;
        default:
            if (isAnyTempNamespace(nspid))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("only temporary relations may be created in temporary schemas")));
            break;
    }
}

static void AddSchemaSearchPathInfo(List* activeSearchPath, StringInfo detailInfo)
{
    if (detailInfo == NULL) {
        return;
    }
    appendStringInfo(detailInfo, "search path oid: ");
    ListCell* l = NULL;
    foreach (l, activeSearchPath) {
        Oid namespaceId = lfirst_oid(l);
        appendStringInfo(detailInfo, "%u ", namespaceId);
    }
    if (u_sess->attr.attr_common.namespace_search_path != NULL) {
        appendStringInfo(detailInfo, "namespace_search_path: %s", u_sess->attr.attr_common.namespace_search_path);
    }
    return;
}
/*
 * RelnameGetRelid
 *		Try to resolve an unqualified relation name.
 *		Returns OID if relation found in search path, else InvalidOid.
 */
Oid RelnameGetRelid(const char* relname, StringInfo detailInfo)
{
    Oid relid;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);

    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        relid = get_relname_relid(relname, namespaceId);
        if (OidIsValid(relid)) {
            if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
                (void)checkGroup(relid, false);

            list_free_ext(tempActiveSearchPath);
            return relid;
        }
    }
    /* log detail info of searching path if MOD_SCHEMA is on */
    if (!OidIsValid(relid) && module_logging_is_on(MOD_SCHEMA)) {
        AddSchemaSearchPathInfo(tempActiveSearchPath, detailInfo);
    }
    if (module_logging_is_on(MOD_SCHEMA)) {
        char* str = nodeToString(tempActiveSearchPath);
        ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("RelnameGetRelid search path:%s", str)));
        pfree(str);
    }
 
    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

/*
 * RelnameGetRelidExtended
 *      Try to resolve an unqualified relation name.
 *      If not found, to search for whether it is a synonym object.
 *      Store OID if relation found in search path, else InvalidOid.
 *      Returns the details info of supported cases checking.
 */
char* RelnameGetRelidExtended(const char* relname, Oid* relOid, Oid* refSynOid, StringInfo detailInfo)
{
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;
    char* errDetail = NULL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);

    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);
        errDetail = get_relname_relid_extend(relname, namespaceId, relOid, true, refSynOid);
        if (relOid != NULL && OidIsValid(*relOid)) {
            if (namespaceId == u_sess->catalog_cxt.myTempNamespace) {
                (void)checkGroup(*relOid, false);
            }
            break;
        } else if (errDetail != NULL && strlen(errDetail) > 0) {
            /* maybe not relation, but another synonym, also break. */
            break;
        }
    }
    /* log detail info of searching path if MOD_SCHEMA is on */
    if (relOid != NULL && !OidIsValid(*relOid) && module_logging_is_on(MOD_SCHEMA)) {
        AddSchemaSearchPathInfo(tempActiveSearchPath, detailInfo);
    }    
    list_free_ext(tempActiveSearchPath);

    /* return checking details. */
    return errDetail;
}

/*
 * RelationIsVisible
 *		Determine whether a relation (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified relation name".
 */
bool RelationIsVisible(Oid relid)
{
    HeapTuple reltup;
    Form_pg_class relform;
    Oid relnamespace;
    bool visible = false;

    reltup = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(relid), LOG);
    if (!HeapTupleIsValid(reltup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
    relform = (Form_pg_class)GETSTRUCT(reltup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    relnamespace = relform->relnamespace;
    if (relnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, relnamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another relation of the same name earlier in the path. So
         * we must do a slow check for conflicting relations.
         */
        char* relname = NameStr(relform->relname);
        ListCell* l = NULL;
        List* tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);

        visible = false;
        foreach (l, tempActiveSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == relnamespace) {
                /* Found it first in path */
                visible = true;
                break;
            }
            if (OidIsValid(get_relname_relid(relname, namespaceId))) {
                /* Found something else first in path */
                break;
            }
        }

        list_free_ext(tempActiveSearchPath);
    }

    ReleaseSysCache(reltup);

    return visible;
}

/*
 * TypenameGetTypid
 *     Wrapper for binary compatibility.
 */
Oid TypenameGetTypid(const char *typname)
{
        return TypenameGetTypidExtended(typname, true);
}

Oid TryLookForSynonymType(const char* typname, const Oid namespaceId)
{
    Oid typid = InvalidOid;
    RangeVar* objVar = SearchReferencedObject(typname, namespaceId);
    if (objVar != NULL) {
        char* synTypname = objVar->relname;
        Oid objNamespaceOid = get_namespace_oid(objVar->schemaname, true);
        ereport(DEBUG3, (errmodule(MOD_PARSER),
            errmsg("Found synonym %s in namespace %ud", synTypname, objNamespaceOid)));
        typid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(synTypname), ObjectIdGetDatum(objNamespaceOid));
    }
    pfree_ext(objVar);
    return typid;
}

/*
 * TypenameGetTypidExtended
 *		Try to resolve an unqualified datatype name.
 *		Returns OID if type found in search path, else InvalidOid.
 *
 * This is essentially the same as RelnameGetRelid.
 */
Oid TypenameGetTypidExtended(const char* typname, bool temp_ok)
{
    Oid typid;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        /* do not look in temp namespace if temp_ok if false */
        if (!temp_ok && namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue;

        typid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId));
        if (!OidIsValid(typid)) {
            typid = TryLookForSynonymType(typname, namespaceId);
        }

        if (OidIsValid(typid)) {
            list_free_ext(tempActiveSearchPath);
            return typid;
        }
    }

    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

/*
 * TypeIsVisible
 *		Determine whether a type (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified type name".
 */
bool TypeIsVisible(Oid typid)
{
    HeapTuple typtup;
    Form_pg_type typform;
    Oid typnamespace;
    bool visible = false;

    typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typtup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    typform = (Form_pg_type)GETSTRUCT(typtup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    typnamespace = typform->typnamespace;
    if (typnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, typnamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another type of the same name earlier in the path. So we
         * must do a slow check for conflicting types.
         */
        char* typname = NameStr(typform->typname);
        ListCell* l = NULL;
        List* tempActiveSearchPath = NIL;
        tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);

        visible = false;
        foreach (l, tempActiveSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == typnamespace) {
                /* Found it first in path */
                visible = true;
                break;
            }
            if (SearchSysCacheExists2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId))) {
                /* Found something else first in path */
                break;
            }
        }

        list_free_ext(tempActiveSearchPath);
    }

    ReleaseSysCache(typtup);

    return visible;
}

bool isTableofIndexbyType(Oid typeOid)
{
    bool result = false;
    HeapTuple typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(typeTup)) {
        return result;
    }

    if (((Form_pg_type)GETSTRUCT(typeTup))->typtype == TYPTYPE_TABLEOF &&
        (((Form_pg_type)GETSTRUCT(typeTup))->typcategory == TYPCATEGORY_TABLEOF_VARCHAR ||
        ((Form_pg_type)GETSTRUCT(typeTup))->typcategory == TYPCATEGORY_TABLEOF_INTEGER)) {
        result = true;
    }

    ReleaseSysCache(typeTup);

    return result;
}

bool isTableofType(Oid typeOid, Oid* base_oid, Oid* indexbyType)
{
    bool result = false;
    HeapTuple typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(typeTup)) {
        return result;
    }

    if (((Form_pg_type)GETSTRUCT(typeTup))->typtype == TYPTYPE_TABLEOF) {
        *base_oid = ((Form_pg_type)GETSTRUCT(typeTup))->typelem;
        result = true;
    }

    if (indexbyType != NULL) {
        if (((Form_pg_type)GETSTRUCT(typeTup))->typcategory == TYPCATEGORY_TABLEOF_VARCHAR) {
            *indexbyType = VARCHAROID;
        } else if (((Form_pg_type)GETSTRUCT(typeTup))->typcategory == TYPCATEGORY_TABLEOF_INTEGER) {
            *indexbyType = INT4OID;
        } else {
            *indexbyType = InvalidOid;
        }
    }

    ReleaseSysCache(typeTup);

    return result;
}

bool IsPlpgsqlLanguageOid(Oid langoid)
{
    HeapTuple tp;
    bool isNull = true;
    char* langName = NULL;

    Relation relation = heap_open(LanguageRelationId, NoLock);
    tp = SearchSysCache1(LANGOID, ObjectIdGetDatum(langoid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        (errmsg("cache lookup failed for language %u", langoid), errdetail("N/A."),
                         errcause("System error."), erraction("Contact engineer to support."))));
    }
    Datum datum = heap_getattr(tp, Anum_pg_language_lanname, RelationGetDescr(relation), &isNull);
    if (isNull) {
        heap_close(relation, NoLock);
        ReleaseSysCache(tp);
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for language name %u", langoid)));
    }
    langName = NameStr(*DatumGetName(datum));
    int result = strcasecmp(langName, "plpgsql");
    heap_close(relation, NoLock);
    ReleaseSysCache(tp);

    if (result == 0) {
        return true;
    } else {
        return false;
    }
}

static FuncCandidateList FuncnameAddCandidates(FuncCandidateList resultList, HeapTuple procTup, List* argNames,
    Oid namespaceId, Oid objNsp, int nargs, CatCList* catList, bool expandVariadic, bool expandDefaults,
    bool includeOut, Oid refSynOid, bool enable_outparam_override)
{
    Form_pg_proc procForm = (Form_pg_proc)GETSTRUCT(procTup);
#ifndef ENABLE_MULTIPLE_NODES
    oidvector* allArgTypes = NULL;
#endif
    int proNargs = -1;
    int effectiveNargs;
    int pathPos = 0;
    bool variadic = false;
    bool useDefaults = false;
    bool anySpecial = false;
    Oid vaElemType;
    int* argNumbers = NULL;
    FuncCandidateList newResult;
    bool isNull = false;

#ifndef ENABLE_MULTIPLE_NODES
    if (enable_outparam_override) {
        Datum argTypes = ProcedureGetAllArgTypes(procTup, &isNull);
        if (!isNull) {
            allArgTypes = (oidvector *)PG_DETOAST_DATUM(argTypes);
            proNargs = allArgTypes->dim1;
            
            // For compatiable with the non-A special cases, for example function with out
            // param can't be called by SQL in A, but some of these are stilled called by
            // such as gsql in SQL.
            Datum prolangoid = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prolang, &isNull);
            if (strcasecmp(get_language_name((Oid)prolangoid), "plpgsql") != 0 ||
                u_sess->attr.attr_common.IsInplaceUpgrade || IsInitdb) {
                Datum pprokind = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prokind, &isNull);
                if ((!isNull && PROC_IS_FUNC(pprokind)) || isNull) {
                    proNargs = procForm->pronargs;
                    allArgTypes = NULL;
                    includeOut = false;
                }
            }
        } else {
            proNargs = procForm->pronargs;
        }
    } else {
        proNargs = procForm->pronargs;
    }
#else
    proNargs = procForm->pronargs;
#endif

    if (OidIsValid(namespaceId)) {
        /* Consider only procs in specified namespace */
        if (procForm->pronamespace != namespaceId) {
            return resultList;
        }
    } else {
        /*
         * Consider only procs that are in the search path and are not in
         * the temp namespace.
         */
        ListCell* nsp = NULL;

        foreach (nsp, u_sess->catalog_cxt.activeSearchPath) {
            if (procForm->pronamespace == lfirst_oid(nsp) &&
                procForm->pronamespace != u_sess->catalog_cxt.myTempNamespace) {
                break;
            }
            pathPos++;
        }
        if (nsp == NULL) {
            return resultList; /* proc is not in search path */
        }
    }

    Datum proAllArgTypes;
    Datum packageOidDatum;	
    ArrayType* arr = NULL;
    int numProcAllArgs = proNargs;
    packageOidDatum = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_packageid, &isNull);
#ifndef ENABLE_MULTIPLE_NODES
    if (!enable_outparam_override && includeOut) {
#else
    if (includeOut) {
#endif
        proAllArgTypes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proallargtypes, &isNull);

        if (!isNull) {
            arr = DatumGetArrayTypeP(proAllArgTypes); /* ensure not toasted */
            numProcAllArgs = ARR_DIMS(arr)[0];

            /* for function which in parameter and out parameter is same */
            if (numProcAllArgs == proNargs) {
                return resultList;
            }
        } else {
            return resultList;
        }
    }

    if (argNames != NIL) {
        /*
         * Call uses named or mixed notation
         *
         * Named or mixed notation can match a variadic function only if
         * expandVariadic is off; otherwise there is no way to match the
         * presumed-nameless parameters expanded from the variadic array.
         */
        if (OidIsValid(procForm->provariadic) && expandVariadic) {
            return resultList;
        }
        vaElemType = InvalidOid;
        variadic = false;

        /*
         * Check argument count.
         */
        Assert(nargs >= 0); /* -1 not supported with argNames */

        if (numProcAllArgs > nargs && expandDefaults) {
            /* Ignore if not enough default expressions */
            if (nargs + procForm->pronargdefaults < numProcAllArgs) {
                return resultList;
            }
            useDefaults = true;
        } else {
            useDefaults = false;
        }

        /* Ignore if it doesn't match requested argument count */
        if (numProcAllArgs != nargs && !useDefaults) {
            return resultList;
        }

        /* Check for argument name match, generate positional mapping */
        if (!MatchNamedCall(procTup, nargs, argNames, &argNumbers, includeOut)) {
            return resultList;
        }

        /* Named argument matching is always "special" */
        anySpecial = true;
    } else {
        /*
         * Call uses positional notation
         *
         * Check if function is variadic, and get variadic element type if
         * so.	If expandVariadic is false, we should just ignore
         * variadic-ness.
         */
        if (numProcAllArgs <= nargs && expandVariadic) {
            vaElemType = procForm->provariadic;
            variadic = OidIsValid(vaElemType);
            anySpecial = anySpecial || variadic;
        } else {
            vaElemType = InvalidOid;
            variadic = false;
        }

        /*
         * Check if function can match by using parameter defaults.
         */
        if (numProcAllArgs > nargs && expandDefaults) {
            /* Ignore if not enough default expressions */
            if (nargs + procForm->pronargdefaults < numProcAllArgs) {
                return resultList;
            }
            useDefaults = true;
            anySpecial = true;
        } else {
            useDefaults = false;
        }

        /* Ignore if it doesn't match requested argument count */
        if (nargs >= 0 && numProcAllArgs != nargs && !variadic && !useDefaults) {
            return resultList;
        }
    }

    int allArgNum = 0;
#ifndef ENABLE_MULTIPLE_NODES
    Datum allArgs = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proallargtypes, &isNull);
    if (!isNull) {
        ArrayType* arr1 = DatumGetArrayTypeP(allArgs); /* ensure not toasted */
        allArgNum = ARR_DIMS(arr1)[0];
    }
#endif

    /*
     * We must compute the effective argument list so that we can easily
     * compare it to earlier results.  We waste a palloc cycle if it gets
     * masked by an earlier result, but really that's a pretty infrequent
     * case so it's not worth worrying about.
     */
    effectiveNargs = Max(numProcAllArgs, nargs);
    newResult = (FuncCandidateList)palloc(MinSizeOfFuncCandidateList + effectiveNargs * sizeof(Oid));
    newResult->pathpos = pathPos;
    newResult->oid = HeapTupleGetOid(procTup);
    newResult->nargs = effectiveNargs;
    newResult->argnumbers = argNumbers;
    newResult->packageOid = DatumGetObjectId(packageOidDatum);
    /* record the referenced synonym oid for building view dependency. */
    newResult->refSynOid = refSynOid;
    newResult->allArgNum = allArgNum;

    Oid* proargtypes = NULL;	
#ifndef ENABLE_MULTIPLE_NODES
    if (!enable_outparam_override || allArgTypes == NULL) {
        if (!includeOut || arr == NULL) {
            oidvector* proargs = ProcedureGetArgTypes(procTup);
            proargtypes = proargs->values;
        } else {
            proargtypes = (Oid*)ARR_DATA_PTR(arr);
        }
    } else {
        proargtypes = allArgTypes->values;
    }
#else
    if (!includeOut) {
        oidvector* proargs = ProcedureGetArgTypes(procTup);
        proargtypes = proargs->values;
    } else {
        proargtypes = (Oid*)ARR_DATA_PTR(arr);
    }
#endif

    if (argNumbers != NULL) {
        /* Re-order the argument types into call's logical order */
        for (int i = 0; i < numProcAllArgs; i++) {
            newResult->args[i] = proargtypes[argNumbers[i]];
        }
    } else if (numProcAllArgs > 0) {
        /* Simple positional case, just copy proargtypes as-is */
        errno_t rc = EOK;
        rc = memcpy_s(newResult->args, numProcAllArgs * sizeof(Oid), proargtypes, numProcAllArgs * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }

    /* 
     * some procedure args have tableof variable,
     * when match the proc parameters' type,
     * we should change to its base type.
     */
    if (numProcAllArgs > 0 && newResult->args != NULL) {
        for (int i = 0; i < numProcAllArgs; i++) {
            Oid base_oid = InvalidOid;
            if(isTableofType(newResult->args[i], &base_oid, NULL)) {
                newResult->args[i] = base_oid;
            }
        }
    }

    if (variadic) {
        int i;

        newResult->nvargs = effectiveNargs - numProcAllArgs + 1;
        /* Expand variadic argument into N copies of element type */
        for (i = numProcAllArgs - 1; i < effectiveNargs; i++) {
            newResult->args[i] = vaElemType;
        }
    } else {
        newResult->nvargs = 0;
    }
    newResult->ndargs = useDefaults ? numProcAllArgs - nargs : 0;

    /*
     * Does it have the same arguments as something we already accepted?
     * If so, decide what to do to avoid returning duplicate argument
     * lists.  We can skip this check for the single-namespace case if no
     * special (named, variadic or defaults) match has been made, since
     * then the unique index on pg_proc guarantees all the matches have
     * different argument lists.
     */
    if (resultList != NULL && (anySpecial || !OidIsValid(namespaceId))) {
        /*
         * If we have an ordered list from SearchSysCacheList (the normal
         * case), then any conflicting proc must immediately adjoin this
         * one in the list, so we only need to look at the newest result
         * item.  If we have an unordered list, we have to scan the whole
         * result list.  Also, if either the current candidate or any
         * previous candidate is a special match, we can't assume that
         * conflicts are adjacent.
         *
         * We ignore defaulted arguments in deciding what is a match.
         */
        FuncCandidateList prevResult;

        if (catList->ordered && !anySpecial) {
            /* ndargs must be 0 if !anySpecial */
            if (effectiveNargs == resultList->nargs &&
                memcmp(newResult->args, resultList->args, effectiveNargs * sizeof(Oid)) == 0) {
                prevResult = resultList;
            } else {
                prevResult = NULL;
            }
        } else {
            int cmp_nargs = newResult->nargs - newResult->ndargs;

            for (prevResult = resultList; prevResult; prevResult = prevResult->next) {
                if (cmp_nargs == prevResult->nargs - prevResult->ndargs &&
                    memcmp(newResult->args, prevResult->args, cmp_nargs * sizeof(Oid)) == 0) {
                    break;
                }
            }
        }

        if (prevResult) {
            /*
             * We have a match with a previous result.	Decide which one
             * to keep, or mark it ambiguous if we can't decide.  The
             * logic here is preference > 0 means prefer the old result,
             * preference < 0 means prefer the new, preference = 0 means
             * ambiguous.
             */
            int preference;

            if (pathPos != prevResult->pathpos) {
                /*
                 * Prefer the one that's earlier in the search path.
                 */
                preference = pathPos - prevResult->pathpos;
            } else if ((variadic && prevResult->nvargs == 0) || (prevResult->packageOid != InvalidOid 
                                 && newResult->packageOid == InvalidOid)) {
                /*
                 * With variadic functions we could have, for example,
                 * both foo(numeric) and foo(variadic numeric[]) in the
                 * same namespace; if so we prefer the non-variadic match
                 * on efficiency grounds.
                 */
                preference = 1;
            } else if ((!variadic && prevResult->nvargs > 0) || (prevResult->packageOid == InvalidOid 
                                  && newResult->packageOid != InvalidOid)) {
                preference = -1;
            } else {
                /* ----------
                 * We can't decide.  This can happen with, for example,
                 * both foo(numeric, variadic numeric[]) and
                 * foo(variadic numeric[]) in the same namespace, or
                 * both foo(int) and foo (int, int default something)
                 * in the same namespace, or both foo(a int, b text)
                 * and foo(b text, a int) in the same namespace.
                 * ----------
                 */
                preference = 0;
            }

            if (preference > 0) {
                /* keep previous result */
                pfree_ext(newResult);
                return resultList;
            } else if (preference < 0) {
                /* remove previous result from the list */
                if (prevResult == resultList) {
                    resultList = prevResult->next;
                } else {
                    FuncCandidateList prevPrevResult;

                    for (prevPrevResult = resultList; prevPrevResult; prevPrevResult = prevPrevResult->next) {
                        if (prevResult == prevPrevResult->next) {
                            prevPrevResult->next = prevResult->next;
                            break;
                        }
                    }
                    Assert(prevPrevResult); /* assert we found it */
                }
                pfree_ext(prevResult);
                /* fall through to add newResult to list */
            } else {
                /* mark old result as ambiguous, discard new */
                prevResult->oid = InvalidOid;
                pfree_ext(newResult);
                return resultList;
            }
        }
    }

    /*
     * Okay to add it to result list
     */
    newResult->next = resultList;
    resultList = newResult;

    return resultList;
}

/*
 * FuncnameGetCandidates
 *		Given a possibly-qualified function name and argument count,
 *		retrieve a list of the possible matches.
 *
 * If nargs is -1, we return all functions matching the given name,
 * regardless of argument count.  (argnames must be NIL, and expand_variadic
 * and expand_defaults must be false, in this case.)
 *
 * If argnames isn't NIL, we are considering a named- or mixed-notation call,
 * and only functions having all the listed argument names will be returned.
 * (We assume that length(argnames) <= nargs and all the passed-in names are
 * distinct.)  The returned structs will include an argnumbers array showing
 * the actual argument index for each logical argument position.
 *
 * If expand_variadic is true, then variadic functions having the same number
 * or fewer arguments will be retrieved, with the variadic argument and any
 * additional argument positions filled with the variadic element type.
 * nvargs in the returned struct is set to the number of such arguments.
 * If expand_variadic is false, variadic arguments are not treated specially,
 * and the returned nvargs will always be zero.
 *
 * If expand_defaults is true, functions that could match after insertion of
 * default argument values will also be retrieved.	In this case the returned
 * structs could have nargs > passed-in nargs, and ndargs is set to the number
 * of additional args (which can be retrieved from the function's
 * proargdefaults entry).
 *
 * It is not possible for nvargs and ndargs to both be nonzero in the same
 * list entry, since default insertion allows matches to functions with more
 * than nargs arguments while the variadic transformation requires the same
 * number or less.
 *
 * When argnames isn't NIL, the returned args[] type arrays are not ordered
 * according to the functions' declarations, but rather according to the call:
 * first any positional arguments, then the named arguments, then defaulted
 * arguments (if needed and allowed by expand_defaults).  The argnumbers[]
 * array can be used to map this back to the catalog information.
 * argnumbers[k] is set to the proargtypes index of the k'th call argument.
 *
 * We search a single namespace if the function name is qualified, else
 * all namespaces in the search path.  In the multiple-namespace case,
 * we arrange for entries in earlier namespaces to mask identical entries in
 * later namespaces.
 *
 * When expanding variadics, we arrange for non-variadic functions to mask
 * variadic ones if the expanded argument list is the same.  It is still
 * possible for there to be conflicts between different variadic functions,
 * however.
 *
 * It is guaranteed that the return list will never contain multiple entries
 * with identical argument lists.  When expand_defaults is true, the entries
 * could have more than nargs positions, but we still guarantee that they are
 * distinct in the first nargs positions.  However, if argnames isn't NIL or
 * either expand_variadic or expand_defaults is true, there might be multiple
 * candidate functions that expand to identical argument lists.  Rather than
 * throw error here, we report such situations by returning a single entry
 * with oid = 0 that represents a set of such conflicting candidates.
 * The caller might end up discarding such an entry anyway, but if it selects
 * such an entry it should react as though the call were ambiguous.
 */
FuncCandidateList FuncnameGetCandidates(List* names, int nargs, List* argnames, bool expand_variadic,
    bool expand_defaults, bool func_create, bool include_out, char expect_prokind)
{
    FuncCandidateList resultList = NULL;
    char* schemaname = NULL;
    char* funcname = NULL;
    Oid namespaceId;
    char* pkgname = NULL;
    int i;
    bool isNull;
    Oid funcoid;
    Oid caller_pkg_oid = InvalidOid;
    Oid initNamesapceId = InvalidOid;
    bool enable_outparam_override = false;

#ifndef ENABLE_MULTIPLE_NODES
    enable_outparam_override = enable_out_param_override();
    if (enable_outparam_override) {
        include_out = true;
    }
#endif
	
    if (OidIsValid(u_sess->plsql_cxt.running_pkg_oid)) {
        caller_pkg_oid = u_sess->plsql_cxt.running_pkg_oid;
    } else if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        caller_pkg_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    }
    /* check for caller error */
    Assert(nargs >= 0 || !(expand_variadic || expand_defaults));


    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &funcname, &pkgname);
    if (schemaname != NULL) {
        // if this function called by ProcedureCreate(A db style), ignore usage right.
        if (func_create)
            namespaceId = LookupNamespaceNoError(schemaname);
        else
            /* use exact schema given */
            namespaceId = LookupExplicitNamespace(schemaname);
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }
    initNamesapceId = namespaceId;

    /* Step1. search syscache by name only and add candidates from pg_proc */
    CatCList* catlist = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif

    for (i = 0; i < catlist->n_members; i++) {
        namespaceId = initNamesapceId;
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        if (!OidIsValid(HeapTupleGetOid(proctup)) || !HeapTupleIsValid(proctup)) {
            continue;
        }

#ifndef ENABLE_MULTIPLE_NODES
        Datum pprokind = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prokind, &isNull);
        if (!isNull) {
            char prokind = CharGetDatum(pprokind);
            if (!PROC_IS_UNKNOWN(expect_prokind) && prokind != expect_prokind) {
                continue;
            }
        }
#endif

        /* judge the function in package is called by a function in same package or another package.
           if it's called by another package,it will continue*/
        funcoid = HeapTupleGetOid(proctup);
        Oid pkg_oid = InvalidOid;
        Oid package_oid = InvalidOid;
        Datum package_oid_datum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isNull);
        if (!isNull) {
            package_oid = DatumGetObjectId(package_oid_datum);
        }
        
        if (OidIsValid(package_oid)) {
            if (schemaname != NULL && pkgname == NULL) {
                continue;
            }
            if (caller_pkg_oid != package_oid) {
                if (pkgname == NULL) {
                    continue;
                } else  {
                    pkg_oid = PackageNameGetOid(pkgname, namespaceId);
                }
                if (pkg_oid != package_oid) {
                    continue;
                }
                namespaceId = GetPackageNamespace(pkg_oid);
            } else if (caller_pkg_oid == package_oid) {
                if (pkgname != NULL) {
                    pkg_oid = PackageNameGetOid(pkgname, namespaceId);
                    if (pkg_oid != caller_pkg_oid) {
                        continue;
                    }
                }
                namespaceId = GetPackageNamespace(caller_pkg_oid);
            }
        } else if (!OidIsValid(package_oid)) {
            if (pkgname != NULL) {
                continue;
            }
        }
        if (schemaname == NULL) {
            Form_pg_proc procForm = (Form_pg_proc)GETSTRUCT(proctup);
            if (procForm->pronamespace == PG_CATALOG_NAMESPACE && OidIsValid(caller_pkg_oid)) {
                resultList = FuncnameAddCandidates(resultList,
                    proctup,
                    argnames,
                    PG_CATALOG_NAMESPACE,
                    ((Form_pg_proc)GETSTRUCT(proctup))->pronamespace,
                    nargs,
                    catlist,
                    expand_variadic,
                    expand_defaults,
                    include_out,
                    InvalidOid,
                    enable_outparam_override);
                    continue;
            }
        }

        resultList = FuncnameAddCandidates(resultList,
            proctup,
            argnames,
            namespaceId,
            ((Form_pg_proc)GETSTRUCT(proctup))->pronamespace,
            nargs,
            catlist,
            expand_variadic,
            expand_defaults,
            include_out,
            InvalidOid,
            enable_outparam_override);
    }
    ReleaseSysCacheList(catlist);

    /* avoid POC error temporary */
    if (resultList != NULL) {
        return resultList;
    }

    /* Step2. try to add candidates with referenced name if funcname is regarded as synonym object. */
    if (IsNormalProcessingMode() && !IsInitdb
        && t_thrd.proc->workingVersionNum >= SYNONYM_VERSION_NUM && pkgname == NULL) {
        if (SearchSysCacheExists1(RELOID, PgSynonymRelationId)) {
            HeapTuple synTuple = NULL;
            List* tempActiveSearchPath = NIL;
            ListCell* l = NULL;

            /* Recompute and fill up the default namespace.*/
            if (schemaname == NULL) {
                recomputeNamespacePath();
                tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
            } else {
                tempActiveSearchPath = list_make1_oid(namespaceId);
            }
            foreach (l, tempActiveSearchPath) {
                Oid tempnamespaceId = lfirst_oid(l);
                synTuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(funcname),
                    ObjectIdGetDatum(tempnamespaceId));

                if (HeapTupleIsValid(synTuple)) {
                    Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(synTuple);
#ifndef ENABLE_MULTIPLE_NODES
                    if (t_thrd.proc->workingVersionNum < 92470) {
                        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(NameStr(synForm->synobjname)));
                    } else {
                        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(NameStr(synForm->synobjname)));
                    }
#else
                    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(NameStr(synForm->synobjname)));
#endif
                    for (i = 0; i < catlist->n_members; i++) {
                        HeapTuple procTuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
                        if (!OidIsValid(HeapTupleGetOid(procTuple))) {
                            continue;
                        }
                        if (!OidIsValid(get_namespace_oid(NameStr(synForm->synobjschema), true))) {
                            continue;
                        }
                        resultList = FuncnameAddCandidates(resultList,
                            procTuple,
                            argnames,
                            get_namespace_oid(NameStr(synForm->synobjschema), false),
                            ((Form_pg_proc)GETSTRUCT(procTuple))->pronamespace,
                            nargs,
                            catlist,
                            expand_variadic,
                            expand_defaults,
                            include_out,
                            HeapTupleGetOid(synTuple),
                            enable_outparam_override);
                    }
                    ReleaseSysCache(synTuple);
                    ReleaseSysCacheList(catlist);
                }
            }
        }
    }
    return resultList;
}

KeyCandidateList CeknameGetCandidates(const List *names, bool key_create)
{
    KeyCandidateList resultList = NULL;
    char *schemaname = NULL;
    char *keyname = NULL;
    Oid namespaceId;
    CatCList *catlist = NULL;
    int i;
    int pathpos = 0;
    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &keyname);

    if (schemaname != NULL) {
        /* if this key called by ProcedureCreate(A db style), ignore usage right. */
        if (key_create)
            namespaceId = LookupNamespaceNoError(schemaname);
        else
            /* use exact schema given */
            namespaceId = LookupExplicitNamespace(schemaname);
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    /* Search syscache by name only */
    catlist = SearchSysCacheList1(COLUMNSETTINGNAME, CStringGetDatum(keyname));

    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple keytup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_gs_column_keys keyform = (Form_gs_column_keys)GETSTRUCT(keytup);
        KeyCandidateList newResult;

        if (OidIsValid(namespaceId)) {
            /* Consider only procs in specified namespace */
            if (keyform->key_namespace != namespaceId)
                continue;
        } else {
            /*
             * Consider only procs that are in the search path and are not in
             * the temp namespace.
             */
            ListCell *nsp = NULL;

            foreach (nsp, u_sess->catalog_cxt.activeSearchPath) {
                if (keyform->key_namespace == lfirst_oid(nsp) &&
                    keyform->key_namespace != u_sess->catalog_cxt.myTempNamespace)
                    break;
                pathpos++;
            }
            if (nsp == NULL)
                continue; /* proc is not in search path */
        }

        newResult = (KeyCandidateList)palloc(sizeof(struct _FuncCandidateList) - sizeof(Oid));
        newResult->oid = HeapTupleGetOid(keytup);
        newResult->pathpos = pathpos;

        /*
         * Okay to add it to result list
         */
        newResult->next = resultList;
        resultList = newResult;
    }
    ReleaseSysCacheList(catlist);
    return resultList;
}
KeyCandidateList GlobalSettingGetCandidates(const List *names, bool key_create)
{
    KeyCandidateList resultList = NULL;
    char *schemaname = NULL;
    char *keyname = NULL;
    Oid namespaceId;
    CatCList *catlist = NULL;
    int i;

    int pathpos = 0;
    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &keyname);

    if (schemaname != NULL) {
        /* if this key called by ProcedureCreate(A db style), ignore usage right. */
        if (key_create)
            namespaceId = LookupNamespaceNoError(schemaname);
        else
            /* use exact schema given */
            namespaceId = LookupExplicitNamespace(schemaname);
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    /* Search syscache by name only */
    catlist = SearchSysCacheList1(GLOBALSETTINGNAME, CStringGetDatum(keyname));

    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple keytup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_gs_client_global_keys keyform = (Form_gs_client_global_keys)GETSTRUCT(keytup);
        KeyCandidateList newResult;

        if (OidIsValid(namespaceId)) {
            /* Consider only procs in specified namespace */
            if (keyform->key_namespace != namespaceId)
                continue;
        } else {
            /*
             * Consider only procs that are in the search path and are not in
             * the temp namespace.
             */
            ListCell *nsp = NULL;

            foreach (nsp, u_sess->catalog_cxt.activeSearchPath) {
                if (keyform->key_namespace == lfirst_oid(nsp) &&
                    keyform->key_namespace != u_sess->catalog_cxt.myTempNamespace)
                    break;
                pathpos++;
            }
            if (nsp == NULL)
                continue; /* proc is not in search path */
        }

        newResult = (KeyCandidateList)palloc(sizeof(struct _FuncCandidateList) - sizeof(Oid));
        newResult->oid = HeapTupleGetOid(keytup);
        newResult->pathpos = pathpos;

        /*
         * Okay to add it to result list
         */
        newResult->next = resultList;
        resultList = newResult;
    }

    ReleaseSysCacheList(catlist);

    return resultList;
}

/*
 * MatchNamedCall
 * 		Given a pg_proc heap tuple and a call's list of argument names,
 * 		check whether the function could match the call.
 *
 * The call could match if all supplied argument names are accepted by
 * the function, in positions after the last positional argument, and there
 * are defaults for all unsupplied arguments.
 *
 * The number of positional arguments is nargs - list_length(argnames).
 * Note caller has already done basic checks on argument count.
 *
 * On match, return true and fill *argnumbers with a palloc'd array showing
 * the mapping from call argument positions to actual function argument
 * numbers.  Defaulted arguments are included in this map, at positions
 * after the last supplied argument.
 */
static bool MatchNamedCall(HeapTuple proctup, int nargs, List* argnames, int** argnumbers, bool include_out)
{
    Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
    int pronargs = procform->pronargs;
    int numposargs = nargs - list_length(argnames);
    int pronallargs;
    Oid* p_argtypes = NULL;
    char** p_argnames = NULL;
    char* p_argmodes = NULL;
    bool arggiven[FUNC_MAX_ARGS];
    bool isnull = false;
    int ap; /* call args position */
    int pp; /* proargs position */
    ListCell* lc = NULL;

    Assert(argnames != NIL);
    Assert(numposargs >= 0);

    /* Ignore this function if its proargnames is null */
    (void)SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargnames, &isnull);
    if (isnull)
        return false;

    /* OK, let's extract the argument names and types */
    pronallargs = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
    Assert(p_argnames != NULL);

    if (include_out)
        pronargs = pronallargs;

    Assert(nargs <= pronargs);

    /* initialize state for matching */
    *argnumbers = (int*)palloc(FUNC_MAX_ARGS * sizeof(int));
    errno_t rc = memset_s(arggiven, FUNC_MAX_ARGS, false, pronargs * sizeof(bool));
    securec_check(rc, "", "");

    /* there are numposargs positional args before the named args */
    for (ap = 0; ap < numposargs; ap++) {
        (*argnumbers)[ap] = ap;
        arggiven[ap] = true;
    }

    /* now examine the named args */
    foreach (lc, argnames) {
        char* argname = (char*)lfirst(lc);
        bool found = false;
        int i;

        pp = 0;
        found = false;
        for (i = 0; i < pronallargs; i++) {
            /* consider only input parameters */
            if (p_argmodes != NULL && (p_argmodes[i] != FUNC_PARAM_IN && p_argmodes[i] != FUNC_PARAM_INOUT &&
                                          p_argmodes[i] != FUNC_PARAM_VARIADIC && !include_out))
                continue;
            if (p_argnames[i] && strcmp(p_argnames[i], argname) == 0) {
                /* fail if argname matches a positional argument */
                if (arggiven[pp])
                    return false;
                arggiven[pp] = true;
                (*argnumbers)[ap] = pp;
                found = true;
                break;
            }
            /* increase pp only for input parameters */
            pp++;
        }
        /* if name isn't in proargnames, fail */
        if (!found)
            return false;
        ap++;
    }

    Assert(ap == nargs); /* processed all actual parameters */

    /* Check for default arguments */
    if (nargs < pronargs) {
        int j = -1;
        int* defaultpos = NULL;
        Datum defposdatum;
        int2vector* defpos = NULL;
        bool defisnull = false;

        if (pronargs <= FUNC_MAX_ARGS_INROW) {
            defposdatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prodefaultargpos, &defisnull);
            if (defisnull) {
                return false;
            }
            defpos = (int2vector*)DatumGetPointer(defposdatum);
        } else {
            defposdatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prodefaultargposext, &defisnull);
            if (defisnull) {
                return false;
            }
            defpos = (int2vector*)PG_DETOAST_DATUM(defposdatum);
        }
        FetchDefaultArgumentPos(&defaultpos, defpos, p_argmodes, pronallargs);
        Assert(defpos);

        /*
         * record the arguments that have to use default value
         */

        if (!include_out) {
            for (j = 0; j < defpos->dim1; j++) {
                /*
                 * if the parameter has been evaluated,skip it.
                 */
                if (arggiven[defaultpos[j]])
                    continue;
                /*
                 * record the argument if it hasn't been assigned
                 */
                (*argnumbers)[ap++] = defaultpos[j];
            }
        } else {
            int outer_pos = 0;
            int start_id = 0;
            for (int k = 0; k < pronargs; k++) {
                if (p_argmodes != NULL && p_argmodes[k] == FUNC_PARAM_OUT) {
                    outer_pos++;
                    continue;
                    ;
                } else {
                    for (j = start_id; j < defpos->dim1; j++) {
                        /*
                         * if the parameter has been evaluated,skip it.
                         */
                        if (arggiven[defaultpos[j] + outer_pos])
                            continue;

                        /*
                         * record the argument if it hasn't been assigned
                         */
                        (*argnumbers)[ap++] = defaultpos[j];
                        start_id++;
                    }
                }
            }
        }

        pfree_ext(defaultpos);
    }

    // A compatibility: overload function
    if (ap != pronargs) /* processed all function parameters */
        return false;

    return true;
}

/*
 * FunctionIsVisible
 *		Determine whether a function (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified function name with exact argument matches".
 */
bool FunctionIsVisible(Oid funcid)
{
    HeapTuple proctup;
    Form_pg_proc procform;
    Oid pronamespace;
    bool visible = false;

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    procform = (Form_pg_proc)GETSTRUCT(proctup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    pronamespace = procform->pronamespace;
    if (pronamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, pronamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another proc of the same name and arguments earlier in
         * the path.  So we must do a slow check to see if this is the same
         * proc that would be found by FuncnameGetCandidates.
         */
        char* proname = NameStr(procform->proname);
        int nargs = procform->pronargs;
        FuncCandidateList clist;

        visible = false;

        oidvector* proargs = ProcedureGetArgTypes(proctup);

#ifndef ENABLE_MULTIPLE_NODES
        bool enable_outparam_override = false;
        enable_outparam_override = enable_out_param_override();
        if (enable_outparam_override) {
            bool isNull = false;
            Datum argTypes = ProcedureGetAllArgTypes(proctup, &isNull);
            if (!isNull) {
                oidvector* allArgTypes = (oidvector *)PG_DETOAST_DATUM(argTypes);
                int proNargs = allArgTypes->dim1;
                clist = FuncnameGetCandidates(list_make1(makeString(proname)),
                                              proNargs, NIL, false, false, false, true);
            } else {
                clist = FuncnameGetCandidates(list_make1(makeString(proname)), nargs, NIL, false, false, false);
            }
        } else {
            clist = FuncnameGetCandidates(list_make1(makeString(proname)), nargs, NIL, false, false, false);
        }
#else
        clist = FuncnameGetCandidates(list_make1(makeString(proname)), nargs, NIL, false, false, false);
#endif
        for (; clist; clist = clist->next) {
            if (memcmp(clist->args, proargs->values, nargs * sizeof(Oid)) == 0) {
                /* Found the expected entry; is it the right proc? */
                visible = (clist->oid == funcid);
                break;
            }
        }
    }

    ReleaseSysCache(proctup);

    return visible;
}

/*
 * OpernameGetOprid
 *		Given a possibly-qualified operator name and exact input datatypes,
 *		look up the operator.  Returns InvalidOid if not found.
 *
 * Pass oprleft = InvalidOid for a prefix op, oprright = InvalidOid for
 * a postfix op.
 *
 * If the operator name is not schema-qualified, it is sought in the current
 * namespace search path.
 */
Oid OpernameGetOprid(List* names, Oid oprleft, Oid oprright)
{
    char* schemaname = NULL;
    char* opername = NULL;
    CatCList* catlist = NULL;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &opername);

    if (schemaname != NULL) {
        /* search only in exact schema given */
        Oid namespaceId;
        HeapTuple opertup;

        namespaceId = LookupExplicitNamespace(schemaname);
        opertup = SearchSysCache4(OPERNAMENSP,
            CStringGetDatum(opername),
            ObjectIdGetDatum(oprleft),
            ObjectIdGetDatum(oprright),
            ObjectIdGetDatum(namespaceId));
        if (HeapTupleIsValid(opertup)) {
            Oid result = HeapTupleGetOid(opertup);

            ReleaseSysCache(opertup);
            return result;
        }
        return InvalidOid;
    }

    /* Search syscache by name and argument types */
    catlist = SearchSysCacheList3(
        OPERNAMENSP, CStringGetDatum(opername), ObjectIdGetDatum(oprleft), ObjectIdGetDatum(oprright));

    if (catlist->n_members == 0) {
        /* no hope, fall out early */
        ReleaseSysCacheList(catlist);
        return InvalidOid;
    }

    /*
     * We have to find the list member that is first in the search path, if
     * there's more than one.  This doubly-nested loop looks ugly, but in
     * practice there should usually be few catlist members.
     */
    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);

    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);
        int i;

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        for (i = 0; i < catlist->n_members; i++) {
            HeapTuple opertup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
            Form_pg_operator operform = (Form_pg_operator)GETSTRUCT(opertup);

            if (operform->oprnamespace == namespaceId) {
                Oid result = HeapTupleGetOid(opertup);

                ReleaseSysCacheList(catlist);

                list_free_ext(tempActiveSearchPath);
                return result;
            }
        }
    }

    list_free_ext(tempActiveSearchPath);
    ReleaseSysCacheList(catlist);
    return InvalidOid;
}

/*
 * OpernameGetCandidates
 *		Given a possibly-qualified operator name and operator kind,
 *		retrieve a list of the possible matches.
 *
 * If oprkind is '\0', we return all operators matching the given name,
 * regardless of arguments.
 *
 * We search a single namespace if the operator name is qualified, else
 * all namespaces in the search path.  The return list will never contain
 * multiple entries with identical argument lists --- in the multiple-
 * namespace case, we arrange for entries in earlier namespaces to mask
 * identical entries in later namespaces.
 *
 * The returned items always have two args[] entries --- one or the other
 * will be InvalidOid for a prefix or postfix oprkind.	nargs is 2, too.
 */
FuncCandidateList OpernameGetCandidates(List* names, char oprkind)
{
    FuncCandidateList resultList = NULL;
    char* resultSpace = NULL;
    int nextResult = 0;
    char* schemaname = NULL;
    char* opername = NULL;
    Oid namespaceId;
    CatCList* catlist = NULL;
    int i;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &opername);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    /* Search syscache by name only */
    catlist = SearchSysCacheList1(OPERNAMENSP, CStringGetDatum(opername));

    /*
     * In typical scenarios, most if not all of the operators found by the
     * catcache search will end up getting returned; and there can be quite a
     * few, for common operator names such as '=' or '+'.  To reduce the time
     * spent in palloc, we allocate the result space as an array large enough
     * to hold all the operators.  The original coding of this routine did a
     * separate palloc for each operator, but profiling revealed that the
     * pallocs used an unreasonably large fraction of parsing time.
     */
#define SPACE_PER_OP MAXALIGN(sizeof(struct _FuncCandidateList) + sizeof(Oid))

    if (catlist->n_members > 0)
        resultSpace = (char*)palloc(catlist->n_members * SPACE_PER_OP);

    for (i = 0; i < catlist->n_members; i++) {
        HeapTuple opertup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_operator operform = (Form_pg_operator)GETSTRUCT(opertup);
        int pathpos = 0;
        FuncCandidateList newResult;

        /* Ignore operators of wrong kind, if specific kind requested */
        if (oprkind && operform->oprkind != oprkind)
            continue;

        if (OidIsValid(namespaceId)) {
            /* Consider only opers in specified namespace */
            if (operform->oprnamespace != namespaceId)
                continue;
            /* No need to check args, they must all be different */
        } else {
            /*
             * Consider only opers that are in the search path and are not in
             * the temp namespace.
             */
            ListCell* nsp = NULL;

            foreach (nsp, u_sess->catalog_cxt.activeSearchPath) {
                if (operform->oprnamespace == lfirst_oid(nsp) &&
                    operform->oprnamespace != u_sess->catalog_cxt.myTempNamespace)
                    break;
                pathpos++;
            }
            if (nsp == NULL)
                continue; /* oper is not in search path */

            /*
             * Okay, it's in the search path, but does it have the same
             * arguments as something we already accepted?	If so, keep only
             * the one that appears earlier in the search path.
             *
             * If we have an ordered list from SearchSysCacheList (the normal
             * case), then any conflicting oper must immediately adjoin this
             * one in the list, so we only need to look at the newest result
             * item.  If we have an unordered list, we have to scan the whole
             * result list.
             */
            if (resultList) {
                FuncCandidateList prevResult;

                if (catlist->ordered) {
                    if (operform->oprleft == resultList->args[0] && operform->oprright == resultList->args[1])
                        prevResult = resultList;
                    else
                        prevResult = NULL;
                } else {
                    for (prevResult = resultList; prevResult; prevResult = prevResult->next) {
                        if (operform->oprleft == prevResult->args[0] && operform->oprright == prevResult->args[1])
                            break;
                    }
                }
                if (prevResult) {
                    /* We have a match with a previous result */
                    Assert(pathpos != prevResult->pathpos);
                    if (pathpos > prevResult->pathpos)
                        continue; /* keep previous result */
                    /* replace previous result */
                    prevResult->pathpos = pathpos;
                    prevResult->oid = HeapTupleGetOid(opertup);
                    continue; /* args are same, of course */
                }
            }
        }

        /*
         * Okay to add it to result list
         */
        newResult = (FuncCandidateList)(resultSpace + nextResult);
        nextResult += SPACE_PER_OP;

        newResult->pathpos = pathpos;
        newResult->oid = HeapTupleGetOid(opertup);
        newResult->nargs = 2;
        newResult->packageOid = InvalidOid;
        newResult->nvargs = 0;
        newResult->ndargs = 0;
        newResult->argnumbers = NULL;
        newResult->args[0] = operform->oprleft;
        newResult->args[1] = operform->oprright;
        newResult->next = resultList;
        resultList = newResult;
    }

    ReleaseSysCacheList(catlist);

    return resultList;
}

/*
 * OperatorIsVisible
 *		Determine whether an operator (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified operator name with exact argument matches".
 */
bool OperatorIsVisible(Oid oprid)
{
    HeapTuple oprtup;
    Form_pg_operator oprform;
    Oid oprnamespace;
    bool visible = false;

    oprtup = SearchSysCache1(OPEROID, ObjectIdGetDatum(oprid));
    if (!HeapTupleIsValid(oprtup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", oprid)));
    oprform = (Form_pg_operator)GETSTRUCT(oprtup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    oprnamespace = oprform->oprnamespace;
    if (oprnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, oprnamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another operator of the same name and arguments earlier
         * in the path.  So we must do a slow check to see if this is the same
         * operator that would be found by OpernameGetOprId.
         */
        char* oprname = NameStr(oprform->oprname);

        visible = (OpernameGetOprid(list_make1(makeString(oprname)), oprform->oprleft, oprform->oprright) == oprid);
    }

    ReleaseSysCache(oprtup);

    return visible;
}

/*
 * OpclassnameGetOpcid
 *		Try to resolve an unqualified index opclass name.
 *		Returns OID if opclass found in search path, else InvalidOid.
 *
 * This is essentially the same as TypenameGetTypid, but we have to have
 * an extra argument for the index AM OID.
 */
Oid OpclassnameGetOpcid(Oid amid, const char* opcname)
{
    Oid opcid;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);

    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        opcid = GetSysCacheOid3(
            CLAAMNAMENSP, ObjectIdGetDatum(amid), PointerGetDatum(opcname), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(opcid)) {
            list_free_ext(tempActiveSearchPath);
            return opcid;
        }
    }

    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

/*
 * OpclassIsVisible
 *		Determine whether an opclass (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified opclass name".
 */
bool OpclassIsVisible(Oid opcid)
{
    HeapTuple opctup;
    Form_pg_opclass opcform;
    Oid opcnamespace;
    bool visible = false;

    opctup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opcid));
    if (!HeapTupleIsValid(opctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for opclass %u", opcid)));
    opcform = (Form_pg_opclass)GETSTRUCT(opctup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    opcnamespace = opcform->opcnamespace;
    if (opcnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, opcnamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another opclass of the same name earlier in the path. So
         * we must do a slow check to see if this opclass would be found by
         * OpclassnameGetOpcid.
         */
        char* opcname = NameStr(opcform->opcname);

        visible = (OpclassnameGetOpcid(opcform->opcmethod, opcname) == opcid);
    }

    ReleaseSysCache(opctup);

    return visible;
}

/*
 * OpfamilynameGetOpfid
 *		Try to resolve an unqualified index opfamily name.
 *		Returns OID if opfamily found in search path, else InvalidOid.
 *
 * This is essentially the same as TypenameGetTypid, but we have to have
 * an extra argument for the index AM OID.
 */
Oid OpfamilynameGetOpfid(Oid amid, const char* opfname)
{
    Oid opfid;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        opfid = GetSysCacheOid3(
            OPFAMILYAMNAMENSP, ObjectIdGetDatum(amid), PointerGetDatum(opfname), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(opfid)) {
            list_free_ext(tempActiveSearchPath);
            return opfid;
        }
    }

    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

/*
 * OpfamilyIsVisible
 *		Determine whether an opfamily (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified opfamily name".
 */
bool OpfamilyIsVisible(Oid opfid)
{
    HeapTuple opftup;
    Form_pg_opfamily opfform;
    Oid opfnamespace;
    bool visible = false;

    opftup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
    if (!HeapTupleIsValid(opftup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for opfamily %u", opfid)));
    opfform = (Form_pg_opfamily)GETSTRUCT(opftup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    opfnamespace = opfform->opfnamespace;
    if (opfnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, opfnamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another opfamily of the same name earlier in the path. So
         * we must do a slow check to see if this opfamily would be found by
         * OpfamilynameGetOpfid.
         */
        char* opfname = NameStr(opfform->opfname);

        visible = (OpfamilynameGetOpfid(opfform->opfmethod, opfname) == opfid);
    }

    ReleaseSysCache(opftup);

    return visible;
}

/*
 * CollationGetCollid
 *		Try to resolve an unqualified collation name.
 *		Returns OID if collation found in search path, else InvalidOid.
 */
Oid CollationGetCollid(const char* collname)
{
    int32 dbencoding = GetDatabaseEncoding();
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);
        Oid collid;

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        /* Check for database-encoding-specific entry */
        collid = GetSysCacheOid3(
            COLLNAMEENCNSP, PointerGetDatum(collname), Int32GetDatum(dbencoding), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(collid)) {
            list_free_ext(tempActiveSearchPath);
            return collid;
        }

        /* Check for any-encoding entry */
        collid = GetSysCacheOid3(
            COLLNAMEENCNSP, PointerGetDatum(collname), Int32GetDatum(-1), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(collid)) {
            list_free_ext(tempActiveSearchPath);
            return collid;
        }
    }

    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

/*
 * CollationIsVisible
 *		Determine whether a collation (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified collation name".
 */
bool CollationIsVisible(Oid collid)
{
    HeapTuple colltup;
    Form_pg_collation collform;
    Oid collnamespace;
    bool visible = false;

    colltup = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
    if (!HeapTupleIsValid(colltup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for collation %u", collid)));
    collform = (Form_pg_collation)GETSTRUCT(colltup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    collnamespace = collform->collnamespace;
    if (collnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, collnamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another conversion of the same name earlier in the path.
         * So we must do a slow check to see if this conversion would be found
         * by CollationGetCollid.
         */
        char* collname = NameStr(collform->collname);

        visible = (CollationGetCollid(collname) == collid);
    }

    ReleaseSysCache(colltup);

    return visible;
}

/*
 * ConversionGetConid
 *		Try to resolve an unqualified conversion name.
 *		Returns OID if conversion found in search path, else InvalidOid.
 *
 * This is essentially the same as RelnameGetRelid.
 */
Oid ConversionGetConid(const char* conname)
{
    Oid conid;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        conid = GetSysCacheOid2(CONNAMENSP, PointerGetDatum(conname), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(conid)) {
            list_free_ext(tempActiveSearchPath);
            return conid;
        }
    }

    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

/*
 * ConversionIsVisible
 *		Determine whether a conversion (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified conversion name".
 */
bool ConversionIsVisible(Oid conid)
{
    HeapTuple contup;
    Form_pg_conversion conform;
    Oid connamespace;
    bool visible = false;

    contup = SearchSysCache1(CONVOID, ObjectIdGetDatum(conid));
    if (!HeapTupleIsValid(contup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for conversion %u", conid)));
    conform = (Form_pg_conversion)GETSTRUCT(contup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    connamespace = conform->connamespace;
    if (connamespace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, connamespace))
        visible = false;
    else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another conversion of the same name earlier in the path.
         * So we must do a slow check to see if this conversion would be found
         * by ConversionGetConid.
         */
        char* conname = NameStr(conform->conname);

        visible = (ConversionGetConid(conname) == conid);
    }

    ReleaseSysCache(contup);

    return visible;
}

/*
 * get_ts_parser_oid - find a TS parser by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
Oid get_ts_parser_oid(List* names, bool missing_ok)
{
    char* schemaname = NULL;
    char* parser_name = NULL;
    Oid namespaceId;
    Oid prsoid = InvalidOid;
    ListCell* l = NULL;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &parser_name);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);
        prsoid = GetSysCacheOid2(TSPARSERNAMENSP, PointerGetDatum(parser_name), ObjectIdGetDatum(namespaceId));
    } else {
        /* search for it in search path */
        List* tempActiveSearchPath = NULL;

        recomputeNamespacePath();

        tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, tempActiveSearchPath) {
            namespaceId = lfirst_oid(l);

            if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
                continue; /* do not look in temp namespace */

            prsoid = GetSysCacheOid2(TSPARSERNAMENSP, PointerGetDatum(parser_name), ObjectIdGetDatum(namespaceId));
            if (OidIsValid(prsoid))
                break;
        }

        list_free_ext(tempActiveSearchPath);
    }

    if (!OidIsValid(prsoid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("text search parser \"%s\" does not exist", NameListToString(names))));

    return prsoid;
}

/*
 * TSParserIsVisible
 *		Determine whether a parser (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified parser name".
 */
bool TSParserIsVisible(Oid prsId)
{
    HeapTuple tup;
    Form_pg_ts_parser form;
    Oid nmspace;
    bool visible = false;

    tup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(prsId));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for text search parser %u", prsId)));
    form = (Form_pg_ts_parser)GETSTRUCT(tup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    nmspace = form->prsnamespace;
    if (nmspace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, nmspace))
        visible = false;
    else {
        visible = CheckTSObjectVisible(form->prsname, TSPARSERNAMENSP, nmspace);
    }

    ReleaseSysCache(tup);

    return visible;
}

/*
 * get_ts_dict_oid - find a TS dictionary by possibly qualified name
 *
 * If not found, returns InvalidOid if failOK, else throws error
 */
Oid get_ts_dict_oid(List* names, bool missing_ok)
{
    char* schemaname = NULL;
    char* dict_name = NULL;
    Oid namespaceId;
    Oid dictoid = InvalidOid;
    ListCell* l = NULL;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &dict_name);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);
        dictoid = GetSysCacheOid2(TSDICTNAMENSP, PointerGetDatum(dict_name), ObjectIdGetDatum(namespaceId));
    } else {
        /* search for it in search path */
        List* tempActiveSearchPath = NULL;
        recomputeNamespacePath();

        tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, tempActiveSearchPath) {
            namespaceId = lfirst_oid(l);

            if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
                continue; /* do not look in temp namespace */

            dictoid = GetSysCacheOid2(TSDICTNAMENSP, PointerGetDatum(dict_name), ObjectIdGetDatum(namespaceId));
            if (OidIsValid(dictoid))
                break;
        }

        list_free_ext(tempActiveSearchPath);
    }

    if (!OidIsValid(dictoid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("text search dictionary \"%s\" does not exist", NameListToString(names))));

    return dictoid;
}

/*
 * TSDictionaryIsVisible
 *		Determine whether a dictionary (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified dictionary name".
 */
bool TSDictionaryIsVisible(Oid dictId)
{
    HeapTuple tup;
    Form_pg_ts_dict form;
    Oid nmspace;
    bool visible = false;

    tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for text search dictionary %u", dictId)));
    form = (Form_pg_ts_dict)GETSTRUCT(tup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    nmspace = form->dictnamespace;
    if (nmspace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, nmspace))
        visible = false;
    else {
        visible = CheckTSObjectVisible(form->dictname, TSDICTNAMENSP, nmspace);
    }

    ReleaseSysCache(tup);

    return visible;
}

/*
 * get_ts_template_oid - find a TS template by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
Oid get_ts_template_oid(List* names, bool missing_ok)
{
    char* schemaname = NULL;
    char* template_name = NULL;
    Oid namespaceId;
    Oid tmploid = InvalidOid;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &template_name);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);
        tmploid = GetSysCacheOid2(TSTEMPLATENAMENSP, PointerGetDatum(template_name), ObjectIdGetDatum(namespaceId));
    } else {
        tmploid = GetTSObjectOid(template_name, TSTEMPLATENAMENSP);
    }

    if (!OidIsValid(tmploid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("text search template \"%s\" does not exist", NameListToString(names))));

    return tmploid;
}

/*
 * TSTemplateIsVisible
 *		Determine whether a template (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified template name".
 */
bool TSTemplateIsVisible(Oid tmplId)
{
    HeapTuple tup;
    Form_pg_ts_template form;
    Oid nmspace;
    bool visible = false;

    tup = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(tmplId));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for text search template %u", tmplId)));
    form = (Form_pg_ts_template)GETSTRUCT(tup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    nmspace = form->tmplnamespace;
    if (nmspace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, nmspace))
        visible = false;
    else {
        visible = CheckTSObjectVisible(form->tmplname, TSTEMPLATENAMENSP, nmspace);
    }

    ReleaseSysCache(tup);

    return visible;
}

/*
 * get_ts_config_oid - find a TS config by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
Oid get_ts_config_oid(List* names, bool missing_ok)
{
    char* schemaname = NULL;
    char* config_name = NULL;
    Oid namespaceId;
    Oid cfgoid = InvalidOid;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, &config_name);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);
        cfgoid = GetSysCacheOid2(TSCONFIGNAMENSP, PointerGetDatum(config_name), ObjectIdGetDatum(namespaceId));
    } else {
        /* search for it in search path */
        cfgoid = GetTSObjectOid(config_name, TSCONFIGNAMENSP);
    }

    if (!OidIsValid(cfgoid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("text search configuration \"%s\" does not exist", NameListToString(names))));

    return cfgoid;
}

/*
 * TSConfigIsVisible
 *		Determine whether a text search configuration (identified by OID)
 *		is visible in the current search path.	Visible means "would be found
 *		by searching for the unqualified text search configuration name".
 */
bool TSConfigIsVisible(Oid cfgid)
{
    HeapTuple tup;
    Form_pg_ts_config form;
    Oid nmspace;
    bool visible = false;

    tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for text search configuration %u", cfgid)));
    form = (Form_pg_ts_config)GETSTRUCT(tup);

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    nmspace = form->cfgnamespace;
    if (nmspace != PG_CATALOG_NAMESPACE && !list_member_oid(u_sess->catalog_cxt.activeSearchPath, nmspace))
        visible = false;
    else {
        visible = CheckTSObjectVisible(form->cfgname, TSCONFIGNAMENSP, nmspace);
    }

    ReleaseSysCache(tup);

    return visible;
}

/*
 * DeconstructQualifiedName
 *		Given a possibly-qualified name expressed as a list of String nodes,
 *		extract the schema name and object name.
 *
 * *nspname_p is set to NULL if there is no explicit schema name.
 */

void DeconstructQualifiedName(const List* names, char** nspname_p, char** objname_p, char **pkgname_p)
{
    char* catalogname = NULL;
    char* schemaname = NULL;
    char* objname = NULL;
    char* pkgname = NULL;
    Oid nspoid = InvalidOid;
    switch (list_length(names)) {
        case 1:
            objname = strVal(linitial(names));
            break;
        case 2:
            objname = strVal(lsecond(names));
            schemaname = strVal(linitial(names));
            if (nspname_p != NULL) {
                nspoid = get_namespace_oid(schemaname, true);
            }   
            pkgname = strVal(linitial(names));
            if (!OidIsValid(PackageNameGetOid(pkgname, nspoid))) {
                pkgname = strVal(lsecond(names));
                if (OidIsValid(PackageNameGetOid(pkgname, nspoid))) {
                    schemaname = strVal(linitial(names));
                    objname = pkgname;
                } else {
                    pkgname = NULL;
                }
            } else {
                schemaname = NULL;
            }
            break;
        case 3:
            objname = strVal(lthird(names));
            pkgname = strVal(lsecond(names));
            schemaname = strVal(linitial(names));
            if (nspname_p != NULL) {
                nspoid = get_namespace_oid(schemaname, true);
            }   
            if (!OidIsValid(PackageNameGetOid(pkgname, nspoid))) {
                catalogname = strVal(linitial(names));
                schemaname = strVal(lsecond(names));
                pkgname = NULL;
                break;
            }
            break;
        case 4:
            catalogname = strVal(linitial(names));
            schemaname = strVal(lsecond(names));
            pkgname = strVal(lthird(names));
            objname = strVal(lfourth(names));
            /*
             * We check the catalog name and then ignore it.
             */
            if (strcmp(catalogname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)) != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cross-database references are not implemented: %s", NameListToString(names))));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("improper qualified name (too many dotted names): %s", NameListToString(names))));
            break;
    }

    *nspname_p = schemaname;
    *objname_p = objname;
    if (pkgname_p != NULL)
        *pkgname_p = pkgname;
}

/*
 * @Description: check function is package function,if the function in package,still return true
 * @in funcname -function name
 * @return - function is package
 */
bool IsPackageFunction(List* funcname)
{
    CatCList* catlist = NULL;
    char* schemaname = NULL;
    char* func_name = NULL;
    bool isNull = false;
    bool result = false;
    Oid namespaceId = InvalidOid;
    char *pkgname = NULL;   
    bool isFirstFunction = true; 

    /* deconstruct the name list */
    DeconstructQualifiedName(funcname, &schemaname, &func_name, &pkgname);

    if (schemaname != NULL) {
        namespaceId = LookupExplicitNamespace(schemaname);
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(func_name));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(func_name));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(func_name));
#endif
    bool isFirstPackageFunction = false;
    for (int i = 0; i < catlist->n_members; i++) {
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
        Datum packageid_datum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isNull);
        Oid packageid = InvalidOid;
        if (!isNull) {
            packageid = DatumGetObjectId(packageid_datum);
        } else {
            packageid = InvalidOid;
        }
        if (OidIsValid(namespaceId)) {
            /* Consider only procs in specified namespace */
            if (procform->pronamespace != namespaceId)
                continue;
        } else {
            /*
             * Consider only procs that are in the search path and are not in
             * the temp namespace.
             */
            ListCell* nsp = NULL;

            foreach (nsp, u_sess->catalog_cxt.activeSearchPath) {
                if (procform->pronamespace == lfirst_oid(nsp) &&
                    procform->pronamespace != u_sess->catalog_cxt.myTempNamespace)
                    break;
            }

            if (nsp == NULL)
                continue; /* proc is not in search path */
        }

        /* package function and not package function can not overload */
        proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Datum ispackage = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_package, &isNull);
        result = DatumGetBool(ispackage);
        if (IsSystemObjOid(HeapTupleGetOid(proctup))) {
            continue;
        }
        if (isFirstFunction && !OidIsValid(packageid)) {
            isFirstFunction = false;
            if (result) {
                isFirstPackageFunction = true;
            }
        } else if (!isFirstFunction && !OidIsValid(packageid)) {
            if (result && isFirstPackageFunction) {
                continue;
            } else if (!result) {
                ReleaseSysCacheList(catlist);
                return false;
            }
        } else if (OidIsValid(packageid)) {
            ReleaseSysCacheList(catlist);
            return true;
        }
    }

    ReleaseSysCacheList(catlist);
    if (isFirstPackageFunction) {
        return true;
    } else {
        return false;
    }
    return false;
}

/*
 * LookupNamespaceNoError
 *		Look up a schema name.
 *
 * Returns the namespace OID, or InvalidOid if not found.
 *
 * Note this does NOT perform any permissions check --- callers are
 * responsible for being sure that an appropriate check is made.
 * In the majority of cases LookupExplicitNamespace is preferable.
 */
Oid LookupNamespaceNoError(const char* nspname)
{
    /* check for pg_temp alias */
    if (strcmp(nspname, "pg_temp") == 0) {
        if (OidIsValid(u_sess->catalog_cxt.myTempNamespace))
            return u_sess->catalog_cxt.myTempNamespace;

        /*
         * Since this is used only for looking up existing objects, there is
         * no point in trying to initialize the temp namespace here; and doing
         * so might create problems for some callers. Just report "not found".
         */
        return InvalidOid;
    }

    return get_namespace_oid(nspname, true);
}

/*
 * LookupExplicitNamespace
 *		Process an explicitly-specified schema name: look up the schema
 *		and verify we have USAGE (lookup) rights in it.
 *
 * Returns the namespace OID.  Raises ereport if any problem.
 */
Oid LookupExplicitNamespace(const char* nspname, bool missing_ok)
{
    Oid namespaceId;
    AclResult aclresult;

    if (isTempNamespaceName(nspname) || strcmp(nspname, "pg_temp") == 0) {
        if (OidIsValid(u_sess->catalog_cxt.myTempNamespace)) {
            char* myTempNamesapceName = get_namespace_name(u_sess->catalog_cxt.myTempNamespace);
            if (myTempNamesapceName == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Temp namespace %s not found in pg_namespace. ", myTempNamesapceName),
                        errhint("Temp namespace could be dropped by gs_clean, please check gs_clean.log.")));
            }
            if (strlen(nspname) != 7 && strcmp(nspname, myTempNamesapceName) != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can only access temp objects of the current session.")));
            else
                return u_sess->catalog_cxt.myTempNamespace;
        } else
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can only access temp objects of the current session.")));

        /*
         * Since this is used only for looking up existing objects, there is
         * no point in trying to initialize the temp namespace here; and doing
         * so might create problems for some callers. Just fall through and
         * give the "does not exist" error.
         */
    }

    namespaceId = get_namespace_oid(nspname, missing_ok);
    if (missing_ok && !OidIsValid(namespaceId)) {
        return InvalidOid;
    }

    if (!(u_sess->analyze_cxt.is_under_analyze || (IS_PGXC_DATANODE && IsConnFromCoord())) ||
        u_sess->exec_cxt.is_exec_trigger_func) {
        aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE, nspname);
    }

    return namespaceId;
}

/*
 * LookupCreationNamespace
 *		Look up the schema and verify we have CREATE rights on it.
 *
 * This is just like LookupExplicitNamespace except for the different
 * permission check, and that we are willing to create pg_temp if needed.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
Oid LookupCreationNamespace(const char* nspname)
{
    Oid namespaceId;
    AclResult aclresult;

    /* check for pg_temp alias */
    if (strcmp(nspname, "pg_temp") == 0) {
        (void)InitTempTblNamespace();
        return u_sess->catalog_cxt.myTempNamespace;
    }

    namespaceId = get_namespace_oid(nspname, false);

    aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, nspname);

    return namespaceId;
}

/*
 * Common checks on switching namespaces.
 *
 * We complain if (1) the old and new namespaces are the same, (2) either the
 * old or new namespaces is a temporary schema (or temporary toast schema), or
 * (3) either the old or new namespaces is the TOAST schema.
 */
void CheckSetNamespace(Oid oldNspOid, Oid nspOid, Oid classid, Oid objid)
{
    if (oldNspOid == nspOid)
        ereport(ERROR,
            (classid == RelationRelationId ? errcode(ERRCODE_DUPLICATE_TABLE)
                                           : classid == ProcedureRelationId ? errcode(ERRCODE_DUPLICATE_FUNCTION)
                                                                            : errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("%s is already in schema \"%s\"",
                    getObjectDescriptionOids(classid, objid),
                    get_namespace_name(nspOid))));

    /* disallow renaming into or out of temp schemas */
    if (isAnyTempNamespace(nspOid) || isAnyTempNamespace(oldNspOid))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move objects into or out of temporary schemas")));

    /* same for TOAST schema */
    if (nspOid == PG_TOAST_NAMESPACE || oldNspOid == PG_TOAST_NAMESPACE)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move objects into or out of TOAST schema")));

    /*disallow set into cstore schema*/
    if (nspOid == CSTORE_NAMESPACE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move objects into CSTORE schema")));

    if (nspOid == PG_CATALOG_NAMESPACE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move objects into system schema")));

    /* disallow set into dbe_perf schema */
    if (nspOid == PG_DBEPERF_NAMESPACE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move objects into dbe_perf schema")));
    
    /* disallow set into snapshot schema */
    if (nspOid == PG_SNAPSHOT_NAMESPACE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move objects into snapshot schema")));
}

/*
 * QualifiedNameGetCreationNamespace
 *		Given a possibly-qualified name for an object (in List-of-Values
 *		format), determine what namespace the object should be created in.
 *		Also extract and return the object name (last component of list).
 *
 * Note: this does not apply any permissions check.  Callers must check
 * for CREATE rights on the selected namespace when appropriate.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
Oid QualifiedNameGetCreationNamespace(const List* names, char** objname_p)
{
    char* schemaname = NULL;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &schemaname, objname_p);

    Oid nspid = InvalidOid;
    Oid oldnspid = InvalidOid;
    bool retry = false;

    /*
     * As for relations, we guard against concurrent DDL operations by
     * tracking whether any invalidation messages are processed
     * while we're doing the name lookups and acquiring locks.
     */
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }
        /* Look up creation namespace. */
        nspid = SchemaNameGetSchemaOid(schemaname);
        Assert(OidIsValid(nspid));

        /* In bootstrap processing mode, we don't bother with locking. */
        if (IsBootstrapProcessingMode())
            break;

        if (retry) {
            /* If nothing changed, we're done. */
            if (nspid == oldnspid)
                break;
            /* If creation namespace has changed, give up old lock. */
            if (nspid != oldnspid)
                UnlockDatabaseObject(NamespaceRelationId, oldnspid, 0, AccessShareLock);
        }

        /* Lock namespace. */
        if (nspid != oldnspid)
            LockDatabaseObject(NamespaceRelationId, nspid, 0, AccessShareLock);

        /* If no invalidation message were processed, we're done! */
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                break;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                break;
            }
        }

        /* Something may have changed, so recheck our work. */
        retry = true;
        oldnspid = nspid;
    }

    return nspid;
}

Oid SchemaNameGetSchemaOid(const char* schemaname, bool missing_ok)
{
    Oid namespaceId;

    if (schemaname != NULL) {
        /* check for pg_temp alias */
        if (strcmp(schemaname, "pg_temp") == 0) {
            (void)InitTempTblNamespace();
            return u_sess->catalog_cxt.myTempNamespace;
        }
        /* use exact schema given */
        namespaceId = get_namespace_oid(schemaname, missing_ok);
        /* we do not check for USAGE rights here! */
    } else {
        namespaceId = GetOidBySchemaName(missing_ok);
    }

    return namespaceId;
}

Datum get_schema_oid(PG_FUNCTION_ARGS)
{
    char* nschema = NULL;
    nschema = PG_GETARG_CSTRING(0);
    PG_RETURN_OID(get_namespace_oid(nschema, true));
}
/*
 * get_namespace_oid - given a namespace name, look up the OID
 *
 * If missing_ok is false, throw an error if namespace name not found.	If
 * true, just return InvalidOid.
 */
Oid get_namespace_oid(const char* nspname, bool missing_ok)
{
    Oid oid = InvalidOid;

    oid = GetSysCacheOid1(NAMESPACENAME, CStringGetDatum(nspname));
    if (!OidIsValid(oid) && !missing_ok) {      
        char message[MAXSTRLEN]; 
        errno_t rc = sprintf_s(message, MAXSTRLEN, "schema \"%s\" does not exist", nspname);
        if (strlen(nspname) > MAXSTRLEN) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("The schema name exceeds the maximum length.")));
        }
        securec_check_ss(rc, "", "");
        InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc, true);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema \"%s\" does not exist", nspname)));
    }

    return oid;
}


/*
 * makeRangeVarFromNameList
 *		Utility routine to convert a qualified-name list into RangeVar form.
 */
RangeVar* makeRangeVarFromNameList(List* names)
{
    RangeVar* rel = makeRangeVar(NULL, NULL, -1);

    switch (list_length(names)) {
        case 1:
            rel->relname = strVal(linitial(names));
            break;
        case 2:
            rel->schemaname = strVal(linitial(names));
            rel->relname = strVal(lsecond(names));
            break;
        case 3:
            rel->catalogname = strVal(linitial(names));
            rel->schemaname = strVal(lsecond(names));
            rel->relname = strVal(lthird(names));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("improper relation name (too many dotted names): %s", NameListToString(names))));
            break;
    }

    return rel;
}

/*
 * NameListToString
 *		Utility routine to convert a qualified-name list into a string.
 *
 * This is used primarily to form error messages, and so we do not quote
 * the list elements, for the sake of legibility.
 *
 * In most scenarios the list elements should always be Value strings,
 * but we also allow A_Star for the convenience of ColumnRef processing.
 */
char* NameListToString(const List* names)
{
    StringInfoData string;
    ListCell* l = NULL;

    initStringInfo(&string);

    foreach (l, names) {
        Node* name = (Node*)lfirst(l);

        if (l != list_head(names))
            appendStringInfoChar(&string, '.');

        if (IsA(name, String))
            appendStringInfoString(&string, strVal(name));
        else if (IsA(name, A_Star))
            appendStringInfoString(&string, "*");
        else
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("unexpected node type in name list: %d", (int)nodeTag(name))));
    }

    return string.data;
}

/*
 * NameListToQuotedString
 *		Utility routine to convert a qualified-name list into a string.
 *
 * Same as above except that names will be double-quoted where necessary,
 * so the string could be re-parsed (eg, by textToQualifiedNameList).
 */
char* NameListToQuotedString(List* names)
{
    StringInfoData string;
    ListCell* l = NULL;

    initStringInfo(&string);

    foreach (l, names) {
        if (l != list_head(names))
            appendStringInfoChar(&string, '.');
        appendStringInfoString(&string, quote_identifier(strVal(lfirst(l))));
    }

    return string.data;
}

/*
 * isTempNamespace - is the given namespace my temporary-table namespace?
 */
bool isTempNamespace(Oid namespaceId)
{
    if (OidIsValid(u_sess->catalog_cxt.myTempNamespace) && u_sess->catalog_cxt.myTempNamespace == namespaceId)
        return true;
    return false;
}

/*
 * isTempToastNamespace - is the given namespace my temporary-toast-table
 *		namespace?
 */
bool isTempToastNamespace(Oid namespaceId)
{
    if (OidIsValid(u_sess->catalog_cxt.myTempToastNamespace) && u_sess->catalog_cxt.myTempToastNamespace == namespaceId)
        return true;
    return false;
}

/*
 * isTempOrToastNamespace - is the given namespace my temporary-table
 *		namespace or my temporary-toast-table namespace?
 */
bool isTempOrToastNamespace(Oid namespaceId)
{
    if (OidIsValid(u_sess->catalog_cxt.myTempNamespace) &&
        (u_sess->catalog_cxt.myTempNamespace == namespaceId || u_sess->catalog_cxt.myTempToastNamespace == namespaceId))
        return true;
    return false;
}

/*
 * isAnyTempNamespace - is the given namespace a temporary-table namespace
 * (either my own, or another backend's)?  Temporary-toast-table namespaces
 * are included, too.
 */
bool isAnyTempNamespace(Oid namespaceId)
{
    bool result = false;
    char* nspname = NULL;

    /* True if the namespace name starts with "pg_temp_" or "pg_toast_temp_" */
    nspname = get_namespace_name(namespaceId);
    if (nspname == NULL)
        return false; /* no such namespace? */
    result = isTempNamespaceName(nspname) || isToastTempNamespaceName(nspname);
    pfree_ext(nspname);
    return result;
}

/*
 * isOtherTempNamespace - is the given namespace some other backend's
 * temporary-table namespace (including temporary-toast-table namespaces)?
 *
 * Note: for most purposes in the C code, this function is obsolete.  Use
 * RELATION_IS_OTHER_TEMP() instead to detect non-local temp relations.
 */
bool isOtherTempNamespace(Oid namespaceId)
{
    /* If it's my own temp namespace, say "false" */
    if (isTempOrToastNamespace(namespaceId))
        return false;
    /* Else, if it's any temp namespace, say "true" */
    return isAnyTempNamespace(namespaceId);
}

/*
 * GetTempToastNamespace - get the OID of my temporary-toast-table namespace,
 * which must already be assigned.	(This is only used when creating a toast
 * table for a temp table, so we must have already done InitTempTableNamespace)
 */
Oid GetTempToastNamespace(void)
{
    Assert(OidIsValid(u_sess->catalog_cxt.myTempToastNamespace));
    return u_sess->catalog_cxt.myTempToastNamespace;
}

static void ValidateNamespace(Oid namespaceOid)
{
    if (!OidIsValid(namespaceOid)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_SCHEMA_NAME), errmsg("Namespace oid is invalid.")));
    } else {
        char* namespaceName = get_namespace_name(namespaceOid);
        if (namespaceName == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_SCHEMA_NAME), errmsg("Namespace %u is not found.", namespaceOid)));
        } else {
            if (strncmp(namespaceName, "pg_temp", strlen("pg_temp")) == 0) {
                /* 
                 * In this case, most likely, there is a datanode restarted and 
                 * the u_sess->catalog_cxt.myTempNamespace is set invalid.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_SCHEMA_NAME), 
                    errmsg("Current temp namespace %s is invalid.", namespaceName)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_SCHEMA_NAME), 
                    errmsg("Unexpected namespace %s, should be pg_catalog.", namespaceName)));
            }
        }
    }
}

/*
 * GetOverrideSearchPath - fetch current search path definition in form
 * used by PushOverrideSearchPath.
 *
 * The result structure is allocated in the specified memory context
 * (which might or might not be equal to CurrentMemoryContext); but any
 * junk created by revalidation calculations will be in CurrentMemoryContext.
 */
OverrideSearchPath* GetOverrideSearchPath(MemoryContext context)
{
    OverrideSearchPath* result = NULL;
    List* schemas = NIL;
    MemoryContext oldcxt;

    recomputeNamespacePath();

    oldcxt = MemoryContextSwitchTo(context);

    result = (OverrideSearchPath*)palloc0(sizeof(OverrideSearchPath));
    schemas = list_copy(u_sess->catalog_cxt.activeSearchPath);
    while (schemas && linitial_oid(schemas) != u_sess->catalog_cxt.activeCreationNamespace) {
        if (linitial_oid(schemas) == u_sess->catalog_cxt.myTempNamespace)
            result->addTemp = true;
        else {
            if (linitial_oid(schemas) != PG_CATALOG_NAMESPACE) {
                ValidateNamespace(linitial_oid(schemas));
            }
            result->addCatalog = true;
        }
        schemas = list_delete_first(schemas);
    }
    result->schemas = schemas;

    MemoryContextSwitchTo(oldcxt);

    return result;
}

/*
 * CopyOverrideSearchPath - copy the specified OverrideSearchPath.
 *
 * The result structure is allocated in CurrentMemoryContext.
 */
OverrideSearchPath* CopyOverrideSearchPath(OverrideSearchPath* path)
{
    OverrideSearchPath* result = NULL;

    result = (OverrideSearchPath*)palloc(sizeof(OverrideSearchPath));
    result->schemas = list_copy(path->schemas);
    result->addCatalog = path->addCatalog;
    result->addTemp = path->addTemp;

    return result;
}

/*
 * OverrideSearchPathMatchesCurrent - does path match current setting?
 */
bool OverrideSearchPathMatchesCurrent(OverrideSearchPath* path)
{
    ListCell *lc = NULL, *lcp = NULL;

    recomputeNamespacePath();

    /* We scan down the activeSearchPath to see if it matches the input. */
    lc = list_head(u_sess->catalog_cxt.activeSearchPath);

    /* If path->addTemp, first item should be my temp namespace. */
    if (path->addTemp) {
        if (lc != NULL && lfirst_oid(lc) == u_sess->catalog_cxt.myTempNamespace)
            lc = lnext(lc);
        else
            return false;
    }
    /* If path->addCatalog, next item should be pg_catalog. */
    if (path->addCatalog) {
        if (lc != NULL && lfirst_oid(lc) == PG_CATALOG_NAMESPACE)
            lc = lnext(lc);
        else
            return false;
    }
    /* We should now be looking at the activeCreationNamespace. */
    if (u_sess->catalog_cxt.activeCreationNamespace != ((lc != NULL) ? lfirst_oid(lc) : InvalidOid))
        return false;
    /* The remainder of activeSearchPath should match path->schemas. */
    foreach (lcp, path->schemas) {
        if (lc != NULL && lfirst_oid(lc) == lfirst_oid(lcp))
            lc = lnext(lc);
        else
            return false;
    }
    if (lc != NULL)
        return false;
    return true;
}

/*
 * PushOverrideSearchPath - temporarily override the search path
 *
 * We allow nested overrides, hence the push/pop terminology.  The GUC
 * search_path variable is ignored while an override is active.
 *
 * It's possible that newpath->useTemp is set but there is no longer any
 * active temp namespace, if the path was saved during a transaction that
 * created a temp namespace and was later rolled back.	In that case we just
 * ignore useTemp.	A plausible alternative would be to create a new temp
 * namespace, but for existing callers that's not necessary because an empty
 * temp namespace wouldn't affect their results anyway.
 *
 * It's also worth noting that other schemas listed in newpath might not
 * exist anymore either.  We don't worry about this because OIDs that match
 * no existing namespace will simply not produce any hits during searches.
 */
void PushOverrideSearchPath(OverrideSearchPath* newpath, bool inProcedure)
{
    OverrideStackEntry* entry = NULL;
    List* oidlist = NIL;
    Oid firstNS;
    MemoryContext oldcxt;

    /*
     * Copy the list for safekeeping, and insert implicitly-searched
     * namespaces as needed.  This code should track recomputeNamespacePath.
     */
    oldcxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    oidlist = list_copy(newpath->schemas);

    /*
     * Remember the first member of the explicit list.
     */
    if (oidlist == NIL)
        firstNS = InvalidOid;
    else
        firstNS = linitial_oid(oidlist);

    /*
     * Add any implicitly-searched namespaces to the list.	Note these go on
     * the front, not the back; also notice that we do not check USAGE
     * permissions for these.
     */
    if (newpath->addCatalog)
        oidlist = lcons_oid(PG_CATALOG_NAMESPACE, oidlist);

    if (newpath->addTemp && OidIsValid(u_sess->catalog_cxt.myTempNamespace))
        oidlist = lcons_oid(u_sess->catalog_cxt.myTempNamespace, oidlist);

    if (newpath->addUser) {
        Oid roleid = GetUserId();
        char* rawname = NULL;
        List* namelist = NIL;
        ListCell* l = NULL;

        rawname = pstrdup(u_sess->attr.attr_common.namespace_search_path);

        /* Parse string into list of identifiers */
        if (!SplitIdentifierString(rawname, ',', &namelist)) {
            /* syntax error in name list */
            /* this should not happen if GUC checked check_search_path */
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid list syntax")));
        }

        foreach (l, namelist) {
            char* curname = (char*)lfirst(l);

            if (strcmp(curname, "$user") == 0) {
                /* $user --- substitute namespace matching user name, if any */
                HeapTuple tuple;
                Oid namespaceId;

                tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
                if (HeapTupleIsValid(tuple)) {
                    char* rname = NULL;

                    rname = NameStr(((Form_pg_authid)GETSTRUCT(tuple))->rolname);
                    namespaceId = get_namespace_oid(rname, true);
                    ReleaseSysCache(tuple);
                    if (OidIsValid(namespaceId) && !list_member_oid(oidlist, namespaceId) &&
                        pg_namespace_aclcheck(namespaceId, roleid, ACL_USAGE, false) == ACLCHECK_OK) {
                        oidlist = lappend_oid(oidlist, namespaceId);
                    }
                }
                break;
            }
        }
    }

    /*
     * Build the new stack entry, then insert it at the head of the list.
     */
    entry = (OverrideStackEntry*)palloc(sizeof(OverrideStackEntry));
    entry->searchPath = oidlist;
    entry->creationNamespace = firstNS;
    entry->nestLevel = GetCurrentTransactionNestLevel();
    entry->inProcedure = inProcedure;

    u_sess->catalog_cxt.overrideStack = lcons(entry, u_sess->catalog_cxt.overrideStack);

    /* And make it active. */
    u_sess->catalog_cxt.activeSearchPath = entry->searchPath;
    u_sess->catalog_cxt.activeCreationNamespace = entry->creationNamespace;
    u_sess->catalog_cxt.activeTempCreationPending = false; /* XXX is this OK? */
    if (module_logging_is_on(MOD_SCHEMA)) {
        char* str = nodeToString(u_sess->catalog_cxt.activeSearchPath);
        ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("PushOverrideSearchPath:%s", str)));
        pfree(str);
    }

    MemoryContextSwitchTo(oldcxt);
}

/*
 * PopOverrideSearchPath - undo a previous PushOverrideSearchPath
 *
 * Any push during a (sub)transaction will be popped automatically at abort.
 * But it's caller error if a push isn't popped in normal control flow.
 */
void PopOverrideSearchPath(void)
{
    OverrideStackEntry* entry = NULL;

    /* Sanity checks. */
    if (u_sess->catalog_cxt.overrideStack == NIL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("bogus PopOverrideSearchPath call")));

    /*
     * With the creation or release of savepoint after pushing, CurrentTransactionNestLevel can either greater
     * than or less than stack level.
     */
    entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);

    /* Pop the stack and free storage. */
    u_sess->catalog_cxt.overrideStack = list_delete_first(u_sess->catalog_cxt.overrideStack);
    list_free_ext(entry->searchPath);
    pfree_ext(entry);

    /* Activate the next level down. */
    if (u_sess->catalog_cxt.overrideStack) {
        entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);
        u_sess->catalog_cxt.activeSearchPath = entry->searchPath;
        u_sess->catalog_cxt.activeCreationNamespace = entry->creationNamespace;
        u_sess->catalog_cxt.activeTempCreationPending = false; /* XXX is this OK? */
    } else {
        /* If not baseSearchPathValid, this is useless but harmless */
        u_sess->catalog_cxt.activeSearchPath = u_sess->catalog_cxt.baseSearchPath;
        u_sess->catalog_cxt.activeCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
        u_sess->catalog_cxt.activeTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;
    }
    if (module_logging_is_on(MOD_SCHEMA)) {
        char* str = nodeToString(u_sess->catalog_cxt.activeSearchPath);
        ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("PopOverrideSearchPath:%s", str)));
        pfree(str);
    }
}

/*
 * Description: Add temp namespace ID to OverrideSearchPath.
 *
 * Parameters:
 * @in tmpnspId: temp namespace ID
 *
 * Returns: void
 */

void AddTmpNspToOverrideSearchPath(Oid tmpnspId)
{
    /* Sanity checks. */
    if (u_sess->catalog_cxt.overrideStack == NIL) {
        return;
    }

    MemoryContext oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    ListCell* lc = NULL;
    foreach (lc, u_sess->catalog_cxt.overrideStack) {
        OverrideStackEntry* entry = (OverrideStackEntry*)lfirst(lc);
        if (OidIsValid(tmpnspId) && entry->inProcedure && !list_member_oid(entry->searchPath, tmpnspId)) {
            /*
             * Now that we've successfully built the new list of namespace OIDs, save
             * it in permanent storage.
             */
            entry->searchPath = lcons_oid(tmpnspId, entry->searchPath);
        }
    }
    MemoryContextSwitchTo(oldcxt);
}

/*
 * Description: Remove temp namespace ID from OverrideSearchPath and activeSearchPath.
 *
 * Parameters:
 * @in tmpnspId: temp namespace ID
 *
 * Returns: void
 */

void RemoveTmpNspFromSearchPath(Oid tmpnspId)
{
    /* Sanity checks. */

    if (!OidIsValid(tmpnspId))
        return;

    MemoryContext oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    ListCell* lc = NULL;
    foreach (lc, u_sess->catalog_cxt.overrideStack) {
        OverrideStackEntry* entry = (OverrideStackEntry*)lfirst(lc);
        if (OidIsValid(tmpnspId) && entry->inProcedure && list_member_oid(u_sess->catalog_cxt.activeSearchPath, tmpnspId))
            u_sess->catalog_cxt.activeSearchPath = list_delete_oid(u_sess->catalog_cxt.activeSearchPath, tmpnspId);
        if (OidIsValid(tmpnspId) && entry->inProcedure && list_member_oid(entry->searchPath, tmpnspId))
            entry->searchPath = list_delete_oid(entry->searchPath, tmpnspId);
    }

    MemoryContextSwitchTo(oldcxt);
}

/*
 * get_collation_oid - find a collation by possibly qualified name
 */
Oid get_collation_oid(List* name, bool missing_ok)
{
    char* schemaname = NULL;
    char* collation_name = NULL;
    int32 dbencoding = GetDatabaseEncoding();
    Oid namespaceId;
    Oid colloid;
    ListCell* l = NULL;

    /* deconstruct the name list */
    DeconstructQualifiedName(name, &schemaname, &collation_name);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);

        /* first try for encoding-specific entry, then any-encoding */
        colloid = GetSysCacheOid3(
            COLLNAMEENCNSP, PointerGetDatum(collation_name), Int32GetDatum(dbencoding), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(colloid))
            return colloid;
        colloid = GetSysCacheOid3(
            COLLNAMEENCNSP, PointerGetDatum(collation_name), Int32GetDatum(-1), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(colloid))
            return colloid;
    } else {
        /* search for it in search path */

        List* tempActiveSearchPath = NIL;

        recomputeNamespacePath();

        tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, tempActiveSearchPath) {
            namespaceId = lfirst_oid(l);

            if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
                continue; /* do not look in temp namespace */

            colloid = GetSysCacheOid3(COLLNAMEENCNSP,
                PointerGetDatum(collation_name),
                Int32GetDatum(dbencoding),
                ObjectIdGetDatum(namespaceId));
            if (OidIsValid(colloid)) {
                list_free_ext(tempActiveSearchPath);
                return colloid;
            }

            colloid = GetSysCacheOid3(
                COLLNAMEENCNSP, PointerGetDatum(collation_name), Int32GetDatum(-1), ObjectIdGetDatum(namespaceId));
            if (OidIsValid(colloid)) {
                list_free_ext(tempActiveSearchPath);
                return colloid;
            }
        }

        list_free_ext(tempActiveSearchPath);
    }

    /* Not found in path */
    if (!missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("collation \"%s\" for encoding \"%s\" does not exist",
                      NameListToString(name), GetDatabaseEncodingName())));
    return InvalidOid;
}

/*
 * get_conversion_oid - find a conversion by possibly qualified name
 */
Oid get_conversion_oid(List* name, bool missing_ok)
{
    char* schemaname = NULL;
    char* conversion_name = NULL;
    Oid namespaceId;
    Oid conoid = InvalidOid;
    ListCell* l = NULL;

    /* deconstruct the name list */
    DeconstructQualifiedName(name, &schemaname, &conversion_name);

    if (schemaname != NULL) {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname);
        conoid = GetSysCacheOid2(CONNAMENSP, PointerGetDatum(conversion_name), ObjectIdGetDatum(namespaceId));
    } else {
        /* search for it in search path */

        List* tempActiveSearchPath = NULL;

        recomputeNamespacePath();

        tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, tempActiveSearchPath) {
            namespaceId = lfirst_oid(l);

            if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
                continue; /* do not look in temp namespace */

            conoid = GetSysCacheOid2(CONNAMENSP, PointerGetDatum(conversion_name), ObjectIdGetDatum(namespaceId));
            if (OidIsValid(conoid)) {
                list_free_ext(tempActiveSearchPath);
                return conoid;
            }
        }

        list_free_ext(tempActiveSearchPath);
    }

    /* Not found in path */
    if (!OidIsValid(conoid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("conversion \"%s\" does not exist", NameListToString(name))));
    return conoid;
}

/*
 * FindDefaultConversionProc - find default encoding conversion proc
 */
Oid FindDefaultConversionProc(int4 for_encoding, int4 to_encoding)
{
    Oid proc;
    ListCell* l = NULL;
    List* tempActiveSearchPath = NIL;

    recomputeNamespacePath();

    tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        proc = FindDefaultConversion(namespaceId, for_encoding, to_encoding);
        if (OidIsValid(proc)) {
            list_free_ext(tempActiveSearchPath);
            return proc;
        }
    }

    list_free_ext(tempActiveSearchPath);

    /* Not found in path */
    return InvalidOid;
}

static void recomputeOverrideStackTempPath()
{
    ListCell *lc = NULL, *cell = NULL;
    OverrideStackEntry* entry = NULL;
    foreach (lc, u_sess->catalog_cxt.overrideStack) {
        entry = (OverrideStackEntry*)lfirst(lc);
        if (OidIsValid(u_sess->catalog_cxt.myTempNamespaceOld) && entry->searchPath) {
            foreach (cell, entry->searchPath) {
                if (lfirst_oid(cell) == u_sess->catalog_cxt.myTempNamespaceOld) {
                    lfirst_oid(cell) = u_sess->catalog_cxt.myTempNamespace;
                }
            }
        }
    }
}

/*
 * recomputeNamespacePath - recompute path derived variables if needed.
 */
void recomputeNamespacePath(StringInfo error_info)
{
    Oid roleid = GetUserId();
    char* rawname = NULL;
    List* namelist = NIL;
    List* oidlist = NIL;
    List* newpath = NIL;
    ListCell* l = NULL;
    bool temp_missing = false;
    Oid firstNS = InvalidOid;
    Oid firstOid = InvalidOid;
    MemoryContext oldcxt;

    ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("Recomputing namespacePath with namespace_search_path:%s", 
                        u_sess->attr.attr_common.namespace_search_path)));

    /* if sql advisor online model is running, we can't recompute search path. */
    if (u_sess->adv_cxt.isJumpRecompute) {
        ereport(DEBUG2, (errmodule(MOD_SCHEMA), 
                errmsg("recomputeNamespacePath is skipped because isJumpRecompute is on.")));
        return;
    }

    /* Do nothing if an override search spec is active. */
    if (u_sess->catalog_cxt.overrideStack && u_sess->catalog_cxt.overrideStackValid) {
        ereport(DEBUG2, (errmodule(MOD_SCHEMA), 
                errmsg("recomputeNamespacePath is skipped because overrideStack is valid.")));
        return;
    }

    /* Do nothing if path is already valid. */
    if (u_sess->catalog_cxt.baseSearchPathValid && u_sess->catalog_cxt.namespaceUser == roleid) {
        if (module_logging_is_on(MOD_SCHEMA)) {
            char* str = nodeToString(u_sess->catalog_cxt.baseSearchPath);
            ereport(DEBUG2, (errmodule(MOD_SCHEMA), 
                    errmsg("recomputeNamespacePath is skipped because baseSearchPath(%s) is valid.", str)));
            pfree(str);
        }
        return;
    }

    /* Need a modifiable copy of u_sess->attr.attr_common.namespace_search_path string */
    rawname = pstrdup(u_sess->attr.attr_common.namespace_search_path);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawname, ',', &namelist)) {
        /* syntax error in name list */
        /* this should not happen if GUC checked check_search_path */
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid list syntax")));
    }

    /*
     * Convert the list of names to a list of OIDs.  If any names are not
     * recognizable or we don't have read access, just leave them out of the
     * list.  (We can't raise an error, since the search_path setting has
     * already been accepted.)	Don't make duplicate entries, either.
     */
    oidlist = NIL;
    temp_missing = false;
    foreach (l, namelist) {
        char* curname = (char*)lfirst(l);
        Oid namespaceId;

        if (strcmp(curname, "$user") == 0) {
            /* $user --- substitute namespace matching user name, if any */
            HeapTuple tuple;

            tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
            if (HeapTupleIsValid(tuple)) {
                char* rname = NULL;

                rname = NameStr(((Form_pg_authid)GETSTRUCT(tuple))->rolname);
                namespaceId = get_namespace_oid(rname, true);
                ReleaseSysCache(tuple);
                if (OidIsValid(namespaceId) && !list_member_oid(oidlist, namespaceId) &&
                    pg_namespace_aclcheck(namespaceId, roleid, ACL_USAGE, false) == ACLCHECK_OK) {
                    oidlist = lappend_oid(oidlist, namespaceId);
                    if (firstOid == InvalidOid)
                        firstOid = namespaceId;
                }
            }
        } else if (strcmp(curname, "pg_temp") == 0) {
            /* pg_temp --- substitute temp namespace, if any */
            if (OidIsValid(u_sess->catalog_cxt.myTempNamespace)) {
                if (firstOid == InvalidOid)
                    firstOid = u_sess->catalog_cxt.myTempNamespace;
            } else {
                /* If it ought to be the creation namespace, set flag */
                if (oidlist == NIL)
                    temp_missing = true;
            }
        } else {
            /* normal namespace reference */
            namespaceId = get_namespace_oid(curname, true);
            if (!OidIsValid(namespaceId)) {
                if (error_info != NULL) {
                    if (error_info->len == 0)
                        appendStringInfo(error_info, "schema \"%s\" does not exist", curname);
                    else
                        appendStringInfo(error_info, "; schema \"%s\" does not exist", curname);
                }
            } else if (pg_namespace_aclcheck(namespaceId, roleid, ACL_USAGE) != ACLCHECK_OK) {
                if (error_info != NULL) {
                    if (error_info->len == 0)
                        appendStringInfo(error_info, "permission denied for schema %s", curname);
                    else
                        appendStringInfo(error_info, "; permission denied for schema %s", curname);
                }
            } else if (!list_member_oid(oidlist, namespaceId)) {
                if (namespaceId != PG_CATALOG_NAMESPACE)
                    oidlist = lappend_oid(oidlist, namespaceId);
                if (firstOid == InvalidOid)
                    firstOid = namespaceId;
            }
        }
    }

    /*
     * Remember the first member of the explicit list.	(Note: this is
     * nominally wrong if temp_missing, but we need it anyway to distinguish
     * explicit from implicit mention of pg_catalog.)
     */
    if (firstOid != InvalidOid)
        firstNS = firstOid;

    /*
     * Add any implicitly-searched namespaces to the list.	Note these go on
     * the front, not the back; also notice that we do not check USAGE
     * permissions for these.
     * Note: Change the behavior: Always put PG_CATALOG_NAMESPACE and
     * myTempNamespace in front of oidlist for compatilibility with
     * stored procedures.
     */
    oidlist = lcons_oid(PG_CATALOG_NAMESPACE, oidlist);

    if (OidIsValid(u_sess->catalog_cxt.myTempNamespace))
        oidlist = lcons_oid(u_sess->catalog_cxt.myTempNamespace, oidlist);

    /*
     * Now that we've successfully built the new list of namespace OIDs, save
     * it in permanent storage.
     */
    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    newpath = list_copy(oidlist);
    MemoryContextSwitchTo(oldcxt);

    /* Now safe to assign to state variables. */
    list_free_ext(u_sess->catalog_cxt.baseSearchPath);
    u_sess->catalog_cxt.baseSearchPath = newpath;
    u_sess->catalog_cxt.baseCreationNamespace = firstNS;
    u_sess->catalog_cxt.baseTempCreationPending = temp_missing;

    /* Mark the path valid. */
    u_sess->catalog_cxt.baseSearchPathValid = true;
    u_sess->catalog_cxt.namespaceUser = roleid;

    /* And make it active. */
    u_sess->catalog_cxt.activeSearchPath = u_sess->catalog_cxt.baseSearchPath;
    u_sess->catalog_cxt.activeCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
    u_sess->catalog_cxt.activeTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;

    if (u_sess->catalog_cxt.overrideStack) {
        recomputeOverrideStackTempPath();
        u_sess->catalog_cxt.myTempNamespaceOld = InvalidOid;
        u_sess->catalog_cxt.overrideStackValid = true;
    }
    /* Clean up. */
    pfree_ext(rawname);
    list_free_ext(namelist);
    list_free_ext(oidlist);
}

/*
 * InitTempTableNamespace
 *		Initialize temp table namespace on first use in a particular backend
 */
static void InitTempTableNamespace(void)
{
    char namespaceName[NAMEDATALEN];
    char toastNamespaceName[NAMEDATALEN];
    char PGXCNodeNameSimplified[NAMEDATALEN];
    Oid namespaceId;
    Oid toastspaceId;
    uint32 timeLineId = 0;
    CreateSchemaStmt* create_stmt = NULL;
    char str[NAMEDATALEN * 2 + 64] = {0};
    uint32 tempID = 0;
    const uint32 NAME_SIMPLIFIED_LEN = 7;
    uint32 nameLen = strlen(g_instance.attr.attr_common.PGXCNodeName);
    int ret;
    errno_t rc;

#ifndef ENABLE_MULTIPLE_NODES
    Assert(g_instance.exec_cxt.global_application_name != NULL);
    nameLen = strlen(g_instance.exec_cxt.global_application_name);
#endif

    Assert(!OidIsValid(u_sess->catalog_cxt.myTempNamespace));

    /*
     * First, do permission check to see if we are authorized to make temp
     * tables.	We use a nonstandard error message here since "databasename:
     * permission denied" might be a tad cryptic.
     *
     * Note that ACL_CREATE_TEMP rights are rechecked in pg_namespace_aclmask;
     * that's necessary since current user ID could change during the session.
     * But there's no need to make the namespace in the first place until a
     * temp table creation request is made by someone with appropriate rights.
     */
    if (pg_database_aclcheck(u_sess->proc_cxt.MyDatabaseId, GetUserId(), ACL_CREATE_TEMP) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create temporary tables in database \"%s\"",
                    get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId))));

    check_nodegroup_privilege(GetUserId(), GetUserId(), ACL_CREATE);

    /*
     * Do not allow a Hot Standby slave session to make temp tables.  Aside
     * from problems with modifying the system catalogs, there is a naming
     * conflict: pg_temp_N belongs to the session with BackendId N on the
     * master, not to a slave session with the same BackendId.	We should not
     * be able to get here anyway due to XactReadOnly checks, but let's just
     * make real sure.	Note that this also backstops various operations that
     * allow XactReadOnly transactions to modify temp tables; they'd need
     * RecoveryInProgress checks if not for this.
     */
    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("cannot create temporary tables during recovery")));

    timeLineId = get_controlfile_timeline();
    tempID = __sync_add_and_fetch(&gt_tempID_seed, 1);

    t_thrd.shemem_ptr_cxt.MyBEEntry->st_timelineid = timeLineId;
    t_thrd.shemem_ptr_cxt.MyBEEntry->st_tempid = tempID;

    /*
     * Only copy the neccessary bytes to represent the node name so as to prevent buffer overflow when it is too large.
     * The node name looks like "dn_6001_xxx", and we choose the first 7 bytes to represent it, i.e. "dn_6001".
     */
    ret = strncpy_s(PGXCNodeNameSimplified,
        sizeof(PGXCNodeNameSimplified),
#ifndef ENABLE_MULTIPLE_NODES
        g_instance.exec_cxt.global_application_name,
#else
        g_instance.attr.attr_common.PGXCNodeName,
#endif
        nameLen >= NAME_SIMPLIFIED_LEN ? NAME_SIMPLIFIED_LEN : nameLen);
    securec_check(ret, "\0", "\0");

    // Temp Table
    // g_instance.attr.attr_common.PGXCNodeName is to distinguish temp schema in different coordinator
    // timeLineId is to help identify temp schema created before the last restart, so we
    // can delete it after restart.
    // t_thrd.proc_cxt.MyBackendId is to distinguish different temp schema created by different session
    // in one coordinator.
    if (!IsInitdb) {
        ret = snprintf_s(namespaceName,
            sizeof(namespaceName),
            sizeof(namespaceName) - 1,
            "pg_temp_%s_%u_%u_%lu",
            PGXCNodeNameSimplified,
            timeLineId,
            tempID,
            IS_THREAD_POOL_WORKER ? u_sess->session_id : (uint64)t_thrd.proc_cxt.MyProcPid);
    } else {
        ret = snprintf_s(
            namespaceName, sizeof(namespaceName), sizeof(namespaceName) - 1, "pg_temp_%s", PGXCNodeNameSimplified);
    }
    securec_check_ss(ret, "\0", "\0");

    namespaceId = get_namespace_oid(namespaceName, true);

    // If there already is a  namespace exists, we should drip it and create new.
    if (OidIsValid(namespaceId))
        dropExistTempNamespace(namespaceName);

    /*
     * First use of this temp namespace in this database; create it. The
     * temp namespaces are always owned by the superuser.  We leave their
     * permissions at default --- i.e., no access except to superuser ---
     * to ensure that unprivileged users can't peek at other backends'
     * temp tables.  This works because the places that access the temp
     * namespace for my own backend skip permissions checks on it.
     */
    HeapTuple tup = NULL;
    char* bootstrap_username = NULL;
    tup = SearchSysCache1(AUTHOID, BOOTSTRAP_SUPERUSERID);
    if (HeapTupleIsValid(tup)) {
        bootstrap_username = pstrdup(NameStr(((Form_pg_authid)GETSTRUCT(tup))->rolname));
        ReleaseSysCache(tup);
    } else
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for role %u", BOOTSTRAP_SUPERUSERID)));

    create_stmt = makeNode(CreateSchemaStmt);
    create_stmt->authid = bootstrap_username;
    create_stmt->schemaElts = NULL;
    create_stmt->schemaname = namespaceName;
    create_stmt->temptype = Temp_Rel;
    ret = snprintf_s(
        str, sizeof(str), sizeof(str) - 1, "CREATE SCHEMA %s AUTHORIZATION \"%s\"", namespaceName, bootstrap_username);
    securec_check_ss(ret, "\0", "\0");
    ProcessUtility((Node*)create_stmt, str, NULL, false, None_Receiver, false, NULL);

    if (IS_PGXC_COORDINATOR)
        if (PoolManagerSetCommand(POOL_CMD_TEMP, namespaceName) < 0)
            // Add retry query error code ERRCODE_SET_QUERY for error "ERROR SET query".
            ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("openGauss: ERROR SET query")));

    /* Advance command counter to make namespace visible */
    CommandCounterIncrement();

    /*
     * If the corresponding toast-table namespace doesn't exist yet, create
     * it. (We assume there is no need to clean it out if it does exist, since
     * dropping a parent table should make its toast table go away.)
     */
    if (!IsInitdb) {
        ret = snprintf_s(toastNamespaceName,
            sizeof(toastNamespaceName),
            sizeof(toastNamespaceName) - 1,
            "pg_toast_temp_%s_%u_%u_%lu",
            PGXCNodeNameSimplified,
            timeLineId,
            tempID,
            IS_THREAD_POOL_WORKER ? u_sess->session_id : (uint64)t_thrd.proc_cxt.MyProcPid);
    } else {
        ret = snprintf_s(toastNamespaceName,
            sizeof(toastNamespaceName),
            sizeof(toastNamespaceName) - 1,
            "pg_toast_temp_%s",
            PGXCNodeNameSimplified);
    }

    securec_check_ss(ret, "\0", "\0");

    toastspaceId = get_namespace_oid(toastNamespaceName, true);

    if (OidIsValid(toastspaceId))
        dropExistTempNamespace(toastNamespaceName);

    create_stmt = makeNode(CreateSchemaStmt);
    create_stmt->authid = bootstrap_username;
    create_stmt->schemaElts = NULL;
    create_stmt->schemaname = toastNamespaceName;
    create_stmt->temptype = Temp_Toast;
    rc = memset_s(str, sizeof(str), 0, sizeof(str));
    securec_check(rc, "", "");
    ret = snprintf_s(str,
        sizeof(str),
        sizeof(str) - 1,
        "CREATE SCHEMA %s AUTHORIZATION \"%s\"",
        toastNamespaceName,
        bootstrap_username);
    securec_check_ss(ret, "\0", "\0");
    ProcessUtility((Node*)create_stmt, str, NULL, false, None_Receiver, false, NULL);

    /* Advance command counter to make namespace visible */
    CommandCounterIncrement();

    /*
     * Okay, we've prepared the temp namespace ... but it's not committed yet,
     * so all our work could be undone by transaction rollback.  Set flag for
     * AtEOXact_Namespace to know what to do.
     */
    Assert(OidIsValid(u_sess->catalog_cxt.myTempNamespace) && OidIsValid(u_sess->catalog_cxt.myTempToastNamespace));

    u_sess->catalog_cxt.baseSearchPathValid = false; /* need to rebuild list */
}

/*
 * End-of-transaction cleanup for namespaces.
 */
void AtEOXact_Namespace(bool isCommit)
{
    /*
     * If we abort the transaction in which a temp namespace was selected,
     * we'll have to do any creation or cleanout work over again.  So, just
     * forget the namespace entirely until next time.  On the other hand, if
     * we commit then register an exit callback to clean out the temp tables
     * at backend shutdown.  (We only want to register the callback once per
     * session, so this is a good place to do it.)
     */
    if (u_sess->catalog_cxt.myTempNamespaceSubID != InvalidSubTransactionId) {
        //@Temp table. No need to register RemoveTempRelationsCallback here,
        // because we don't drop temp objects by porc_exit();
        if (!isCommit) {
            u_sess->catalog_cxt.myTempNamespace = InvalidOid;
            u_sess->catalog_cxt.myTempToastNamespace = InvalidOid;
            u_sess->catalog_cxt.baseSearchPathValid = false; /* need to rebuild list */
        }
        u_sess->catalog_cxt.myTempNamespaceSubID = InvalidSubTransactionId;
    }

    /*
     * Clean up if someone failed to do PopOverrideSearchPath
     */
    if (u_sess->catalog_cxt.overrideStack) {
        if (isCommit) {
            elog(WARNING, "leaked override search path");
        }
        while (u_sess->catalog_cxt.overrideStack != NULL) {
            OverrideStackEntry* entry = NULL;

            entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);
            u_sess->catalog_cxt.overrideStack = list_delete_first(u_sess->catalog_cxt.overrideStack);
            list_free_ext(entry->searchPath);
            pfree_ext(entry);
        }
        /* If not baseSearchPathValid, this is useless but harmless */
        u_sess->catalog_cxt.activeSearchPath = u_sess->catalog_cxt.baseSearchPath;
        u_sess->catalog_cxt.activeCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
        u_sess->catalog_cxt.activeTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;
    }
    /* make sure clean up create shcema flag, top transaction mem will release memory */
    u_sess->catalog_cxt.setCurCreateSchema = false;
    u_sess->catalog_cxt.curCreateSchema = NULL;
}

/*
 * AtEOSubXact_Namespace
 *
 * At subtransaction commit, propagate the temp-namespace-creation
 * flag to the parent subtransaction.
 *
 * At subtransaction abort, forget the flag if we set it up.
 */
void AtEOSubXact_Namespace(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    OverrideStackEntry* entry = NULL;
    Oid tmpnspId = InvalidOid;

    if (u_sess->catalog_cxt.myTempNamespaceSubID == mySubid) {
        if (isCommit)
            u_sess->catalog_cxt.myTempNamespaceSubID = parentSubid;
        else {
            u_sess->catalog_cxt.myTempNamespaceSubID = InvalidSubTransactionId;
            tmpnspId = u_sess->catalog_cxt.myTempNamespace;
            /* TEMP namespace creation failed, so reset state */
            if (u_sess->catalog_cxt.overrideStack)
                RemoveTmpNspFromSearchPath(u_sess->catalog_cxt.myTempNamespace);

            u_sess->catalog_cxt.myTempNamespace = InvalidOid;
            u_sess->catalog_cxt.myTempToastNamespace = InvalidOid;
            u_sess->catalog_cxt.baseSearchPathValid = false; /* need to rebuild list */
        }
    }

    /*
     * Clean up if someone failed to do PopOverrideSearchPath
     */
    while (u_sess->catalog_cxt.overrideStack) {
        entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);
        if (entry->nestLevel < GetCurrentTransactionNestLevel())
            break;
        if (isCommit) {
            elog(WARNING, "leaked override search path");
        }
        u_sess->catalog_cxt.overrideStack = list_delete_first(u_sess->catalog_cxt.overrideStack);
        list_free_ext(entry->searchPath);
        pfree_ext(entry);
    }

    /* Activate the next level down. */
    if (u_sess->catalog_cxt.overrideStack) {
        entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);
        u_sess->catalog_cxt.activeSearchPath = entry->searchPath;
        u_sess->catalog_cxt.activeCreationNamespace = entry->creationNamespace;
        u_sess->catalog_cxt.activeTempCreationPending = false; /* XXX is this OK? */

        if (tmpnspId != InvalidOid)
            RemoveTmpNspFromSearchPath(tmpnspId);
    } else {
        /* If not baseSearchPathValid, this is useless but harmless */
        u_sess->catalog_cxt.activeSearchPath = u_sess->catalog_cxt.baseSearchPath;
        u_sess->catalog_cxt.activeCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
        u_sess->catalog_cxt.activeTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;
    }
}
/*
 * Remove all relations in the specified temp namespace.
 *
 * This is called at backend shutdown (if we made any temp relations).
 * It is also called when we begin using a pre-existing temp namespace,
 * in order to clean out any relations that might have been created by
 * a crashed backend.
 */
static void RemoveTempRelations(Oid tempNamespaceId)
{
    ObjectAddress object;

    /*
     * We want to get rid of everything in the target namespace, but not the
     * namespace itself (deleting it only to recreate it later would be a
     * waste of cycles).  We do this by finding everything that has a
     * dependency on the namespace.
     */
    object.classId = NamespaceRelationId;
    object.objectId = tempNamespaceId;
    object.objectSubId = 0;

    deleteWhatDependsOn(&object, false);
}

/*
 * Remove all temp tables from the temporary namespace.
 */
void ResetTempTableNamespace(void)
{
    if (OidIsValid(u_sess->catalog_cxt.myTempNamespace))
        RemoveTempRelations(u_sess->catalog_cxt.myTempNamespace);
}

/*
 * Routines for handling the GUC variable 'search_path'.
 */

/* check_hook: validate new search_path value */
bool check_search_path(char** newval, void** extra, GucSource source)
{
    char* rawname = NULL;
    List* namelist = NIL;

    /* Need a modifiable copy of string */
    rawname = pstrdup(*newval);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawname, ',', &namelist)) {
        /* syntax error in name list */
        GUC_check_errdetail("List syntax is invalid.");
        pfree_ext(rawname);
        list_free_ext(namelist);
        return false;
    }

    /*
     * We used to try to check that the named schemas exist, but there are
     * many valid use-cases for having search_path settings that include
     * schemas that don't exist; and often, we are not inside a transaction
     * here and so can't consult the system catalogs anyway.  So now, the only
     * requirement is syntactic validity of the identifier list.
     */

    pfree_ext(rawname);
    list_free_ext(namelist);

    return true;
}

/* assign_hook: do extra actions as needed */
void assign_search_path(const char* newval, void* extra)
{
    /*
     * We mark the path as needing recomputation, but don't do anything until
     * it's needed.  This avoids trying to do database access during GUC
     * initialization, or outside a transaction.
     */
    u_sess->catalog_cxt.baseSearchPathValid = false;
}

/*
 * InitializeSearchPath: initialize module during InitPostgres.
 *
 * This is called after we are up enough to be able to do catalog lookups.
 */
void InitializeSearchPath(void)
{
    if (IsBootstrapProcessingMode()) {
        /*
         * In bootstrap mode, the search path must be 'pg_catalog' so that
         * tables are created in the proper namespace; ignore the GUC setting.
         */
        MemoryContext oldcxt;

        oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
        u_sess->catalog_cxt.baseSearchPath = list_make1_oid(PG_CATALOG_NAMESPACE);
        MemoryContextSwitchTo(oldcxt);
        u_sess->catalog_cxt.baseCreationNamespace = PG_CATALOG_NAMESPACE;
        u_sess->catalog_cxt.baseTempCreationPending = false;
        u_sess->catalog_cxt.baseSearchPathValid = true;
        u_sess->catalog_cxt.namespaceUser = GetUserId();
        u_sess->catalog_cxt.activeSearchPath = u_sess->catalog_cxt.baseSearchPath;
        u_sess->catalog_cxt.activeCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
        u_sess->catalog_cxt.activeTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;
    } else {
        /*
         * In normal mode, arrange for a callback on any syscache invalidation
         * of pg_namespace rows.
         */
        CacheRegisterSessionSyscacheCallback(NAMESPACEOID, NamespaceCallback, (Datum)0);
        /* Force search path to be recomputed on next use */
        u_sess->catalog_cxt.baseSearchPathValid = false;
    }
}

/*
 * NamespaceCallback
 *		Syscache inval callback function
 */
static void NamespaceCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    /* Force search path to be recomputed on next use */
    u_sess->catalog_cxt.baseSearchPathValid = false;
}

/*
 * Fetch the active search path. The return value is a palloc'ed list
 * of OIDs; the caller is responsible for freeing this storage as
 * appropriate.
 *
 * The returned list includes the implicitly-prepended namespaces only if
 * includeImplicit is true.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
List* fetch_search_path(bool includeImplicit)
{
    List* result = NIL;

    recomputeNamespacePath();

    /*
     * If the temp namespace should be first, force it to exist.  This is so
     * that callers can trust the result to reflect the actual default
     * creation namespace.	It's a bit bogus to do this here, since
     * current_schema() is supposedly a stable function without side-effects,
     * but the alternatives seem worse.
     */
    if (u_sess->catalog_cxt.activeTempCreationPending) {
        InitTempTableNamespace();
        recomputeNamespacePath();
    }

    result = list_copy(u_sess->catalog_cxt.activeSearchPath);
    if (!includeImplicit) {
        while (result && linitial_oid(result) != u_sess->catalog_cxt.activeCreationNamespace)
            result = list_delete_first(result);
    }

    return result;
}

/*
 * Fetch the active search path into a caller-allocated array of OIDs.
 * Returns the number of path entries.	(If this is more than sarray_len,
 * then the data didn't fit and is not all stored.)
 *
 * The returned list always includes the implicitly-prepended namespaces,
 * but never includes the temp namespace.  (This is suitable for existing
 * users, which would want to ignore the temp namespace anyway.)  This
 * definition allows us to not worry about initializing the temp namespace.
 */
int fetch_search_path_array(Oid* sarray, int sarray_len)
{
    int count = 0;
    ListCell* l = NULL;

    recomputeNamespacePath();

    foreach (l, u_sess->catalog_cxt.activeSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not include temp namespace */

        if (count < sarray_len)
            sarray[count] = namespaceId;
        count++;
    }

    return count;
}

Oid get_my_temp_schema()
{
    return u_sess->catalog_cxt.myTempNamespace;
}

/*
 * Export the FooIsVisible functions as SQL-callable functions.
 *
 * Note: as of Postgres 8.4, these will silently return NULL if called on
 * a nonexistent object OID, rather than failing.  This is to avoid race
 * condition errors when a query that's scanning a catalog using an MVCC
 * snapshot uses one of these functions.  The underlying IsVisible functions
 * operate on SnapshotNow semantics and so might see the object as already
 * gone when it's still visible to the MVCC snapshot.  (There is no race
 * condition in the current coding because we don't accept sinval messages
 * between the SearchSysCacheExists test and the subsequent lookup.)
 */

Datum pg_table_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

#ifdef ENABLE_MULTIPLE_NODES
    if (is_streaming_invisible_obj(oid)) {
        PG_RETURN_NULL();
    }
#endif

    PG_RETURN_BOOL(RelationIsVisible(oid));
}

Datum pg_type_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(TypeIsVisible(oid));
}

Datum pg_function_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(FunctionIsVisible(oid));
}

Datum pg_operator_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(OPEROID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(OperatorIsVisible(oid));
}

Datum pg_opclass_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(CLAOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(OpclassIsVisible(oid));
}

Datum pg_opfamily_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(OPFAMILYOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(OpfamilyIsVisible(oid));
}

Datum pg_collation_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(COLLOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(CollationIsVisible(oid));
}

Datum pg_conversion_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(CONVOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(ConversionIsVisible(oid));
}

Datum pg_ts_parser_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(TSPARSEROID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(TSParserIsVisible(oid));
}

Datum pg_ts_dict_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(TSDICTOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(TSDictionaryIsVisible(oid));
}

Datum pg_ts_template_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(TSTEMPLATEOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(TSTemplateIsVisible(oid));
}

Datum pg_ts_config_is_visible(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    if (!SearchSysCacheExists1(TSCONFIGOID, ObjectIdGetDatum(oid)))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(TSConfigIsVisible(oid));
}

Datum pg_my_temp_schema(PG_FUNCTION_ARGS)
{
    PG_RETURN_OID(u_sess->catalog_cxt.myTempNamespace);
}

Datum pg_is_other_temp_schema(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);

    PG_RETURN_BOOL(isOtherTempNamespace(oid));
}

void FetchDefaultArgumentPos(int** defpos, int2vector* adefpos, const char* argmodes, int pronallargs)
{
    int alldefnum = 0;
    int count = 0;
    int current = 0;
    int i = 0;

    if ((adefpos == NULL) || (defpos == NULL))
        return;

    alldefnum = adefpos->dim1;

    *defpos = (int*)palloc(alldefnum * sizeof(int));

    /*
     * all the parameters are input(including OUT and INOUT
     * arguments) parameter
     */
    if (NULL == argmodes) {
        for (i = 0; i < alldefnum; i++) {
            (*defpos)[i] = adefpos->values[i];
        }

        return;
    }

    /*
     * not all the parameters is inpout parameter.
     */
    for (i = 0; i < pronallargs; i++) {
        if (i == adefpos->values[current]) {
            (*defpos)[current] = count;
            current++;
        }
        if (argmodes[i] == FUNC_PARAM_IN || argmodes[i] == FUNC_PARAM_INOUT || argmodes[i] == FUNC_PARAM_VARIADIC)
            count++;
    }
}

/*
 * @Description: get the namespace's owner that has the same name as the namespace.
 * @in nspid : namespace oid.
 * @anyPriv : anyPriv is true, explain that the user has create any permission
 * @in is_securityadmin : whether the is a security administrator doing this.
 * @return : return InvalidOid if there is no appropriate role.
 *            return the owner's oid if the namespace has the same name as its owner.
 */
Oid GetUserIdFromNspId(Oid nspid, bool is_securityadmin, bool anyPriv)
{
    char* rolname = NULL;
    Oid nspowner = InvalidOid;
    Form_pg_namespace nsptup = NULL;
    HeapTuple tuple = NULL;
    Oid result = InvalidOid;

    tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));

    if (HeapTupleIsValid(tuple)) {
        /* get namespace owner's oid */
        nsptup = (Form_pg_namespace)GETSTRUCT(tuple);
        nspowner = nsptup->nspowner;

        /* get  namespace owner's name */
        if (OidIsValid(nspowner)) {
            rolname = GetUserNameFromId(nspowner);

            /* if the owner has the same name as the namespce,
             * return the owner's oid
             */
            if (!strcmp(NameStr(nsptup->nspname), rolname)) {
                if (!is_securityadmin && (!superuser_arg(GetUserId()) &&
                    !has_privs_of_role(GetUserId(), nspowner) && (!anyPriv) &&
                    !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
                    ReleaseSysCache(tuple);
                    ereport(ERROR,
                        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("current user does not have privilege to role %s", rolname)));
                }

                result = nspowner;
            }

            pfree_ext(rolname);
        }

        ReleaseSysCache(tuple);
    }

    return result;
}

Oid getCurrentNamespace()
{
    return u_sess->catalog_cxt.activeCreationNamespace;
}

/*
 * Called by CreateSchemaCommand when creating a temporary schema.
 * Could run on DN or CN.
 */
void SetTempNamespace(Node* stmt, Oid namespaceOid)
{
    Assert(IsA(stmt, CreateSchemaStmt));

    if (((CreateSchemaStmt*)stmt)->temptype == Temp_Rel) {
        u_sess->catalog_cxt.myTempNamespace = namespaceOid;
        if (IS_PGXC_COORDINATOR || isSingleMode || u_sess->attr.attr_common.xc_maintenance_mode || IS_SINGLE_NODE)
            u_sess->catalog_cxt.deleteTempOnQuiting = true;

        /*
         * in restore mode, there may exist more than one temp namespace to be
         * create. So we should not expect the myTempNamespaceSubID to be invalid.
         * When the second times stepping in this function,  myTempNamespaceSubID
         * will be overwrited. This doesn't matter because there's no subtransaction
         * in restore mode.
         */
        u_sess->catalog_cxt.myTempNamespaceSubID = GetCurrentSubTransactionId();
    } else if (((CreateSchemaStmt*)stmt)->temptype == Temp_Toast) {
        u_sess->catalog_cxt.myTempToastNamespace = namespaceOid;
    }
}
void setTempToastNspName()
{
    Assert(u_sess->catalog_cxt.myTempNamespace != InvalidOid);
    char* tempNspName = get_namespace_name(u_sess->catalog_cxt.myTempNamespace);
    if (tempNspName == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for namespace %u", u_sess->catalog_cxt.myTempNamespace)));
    }

    /*
     * tempNamespaceName is like "pg_temp_coordinator1_0_1",
     * and tmepToastNamespaceName is like
     * "pg_toast_temp_coordinator1_0_1", so the length should
     * plus 7(with '\0').
     */
    char* tempToastNspName = (char*)palloc0(strlen(tempNspName) + 7);

    errno_t rc = EOK;
    rc = snprintf_s(
        tempToastNspName, strlen(tempNspName) + 7, strlen(tempNspName) + 6, "pg_toast_temp_%s", &tempNspName[8]);
    securec_check_ss(rc, "\0", "\0");

    u_sess->catalog_cxt.myTempToastNamespace = get_namespace_oid(tempToastNspName, false);

    pfree_ext(tempNspName);
    pfree_ext(tempToastNspName);
}

/*
 * Called by set_config_option when setting search_path/current_schema
 * Setting myTempNamespace, myTempToastNamespace, baseSearchPathValid
 * if namelist contains temp namespace.
 * Only use first valid temp namespace.
 */
void SetTempFromSearchPath(List* namelist)
{
    ListCell *item = NULL;
    foreach(item, namelist) {
        char *searchPathName = (char *)lfirst(item);
        if (!isTempNamespaceName(searchPathName)) {
            continue;
        }

        Oid nspid = get_namespace_oid(searchPathName, true);
        if (!OidIsValid(nspid)) {
            continue;
        }

        /*
         * If is not our own created temp namespace, we should not
         * delete it when quitting, just leave it behind and it will
         * be deleted by it's owner.
         */
        if (!(OidIsValid(u_sess->catalog_cxt.myTempNamespace) && 
                nspid == u_sess->catalog_cxt.myTempNamespace)) {
            u_sess->catalog_cxt.deleteTempOnQuiting = false;
        }

        u_sess->catalog_cxt.myTempNamespace = nspid;
        setTempToastNspName();
        u_sess->catalog_cxt.baseSearchPathValid = false;

        /*
         * @Temp Table. Coordinator should send the set temp schema command to each dn too.
         * Add retry query error code ERRCODE_SET_QUERY for error "ERROR SET query".
         */
        if (IS_PGXC_COORDINATOR && PoolManagerSetCommand(POOL_CMD_TEMP, searchPathName) < 0) {
            ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("Failed to set temp namespace.")));
        }

        /* Only use first valid pg_temp_xxx in search_path. */
        break;
    }
}

/*
 * @Description: temp table does not do redistribute when cluster resizing, so it will
 * still on old node group after redistributing. when the old node group is dropped, the
 * temp table must turn invalid. This function is for check if the node group is valid.
 * @in relid - oid of tmep table to be checked.
 * @in missing_ok - if it's invalid, should we raise an error?
 * @return: bool - If the node group where the temp table in is valid
 */
bool checkGroup(Oid relid, bool missing_ok)
{
    Relation pgxc_group = NULL;
    Form_pgxc_class pgxcClassForm = NULL;
    Form_pg_class pgClassForm = NULL;
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    HeapTuple tuple1 = NULL;
    bool found = false;

    /* in datanode there is no node group */
    if (!IS_PGXC_COORDINATOR)
        return true;

    /* the three modes all all singleton mode, no need to check node group*/
    if (isRestoreMode || isSingleMode || u_sess->attr.attr_common.xc_maintenance_mode)
        return true;

    /* first check if the relation is a temp relation, if not,  no need to continue */
    tuple = SearchSysCache1(RELOID, relid);
    if (HeapTupleIsValid(tuple)) {
        pgClassForm = (Form_pg_class)GETSTRUCT(tuple);
        if (pgClassForm->relpersistence != RELPERSISTENCE_TEMP || pgClassForm->relkind != RELKIND_RELATION) {
            ReleaseSysCache(tuple);
            return true;
        }
        ReleaseSysCache(tuple);
    } else
        /*
         * when drop index, the parent relation has been dropped, and the relation
         * def is acuriqued from relcache. So when checkGroup, if there is no entry
         * in pg_class, just return true.
         */
        return true;

    tuple = SearchSysCache1(PGXCCLASSRELID, relid);
    if (HeapTupleIsValid(tuple)) {
        pgxcClassForm = (Form_pgxc_class)GETSTRUCT(tuple);
        if (in_logic_cluster()) {
            Oid group_oid;
            bool isNull = true;
            int nmembers = 0;

            group_oid = get_pgxc_groupoid(NameStr(pgxcClassForm->pgroup));
            if (OidIsValid(group_oid)) {
                nmembers = get_pgxc_groupmembers(group_oid, NULL);

                Datum xc_node_datum = SysCacheGetAttr(PGXCCLASSRELID, tuple, Anum_pgxc_class_nodes, &isNull);
                if (isNull)
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Can't get nodeoid for relation %d", relid)));

                oidvector* xc_node = (oidvector*)PG_DETOAST_DATUM(xc_node_datum);

                if (nmembers == xc_node->dim1)
                    found = true;

                if (xc_node != (oidvector*)DatumGetPointer(xc_node_datum))
                    pfree_ext(xc_node);
            }
        } else {
            /*
             * There's no syscache for pg_database indexed by name, so we must look
             * the hard way.
             */
            pgxc_group = heap_open(PgxcGroupRelationId, AccessShareLock);

            scan = systable_beginscan(pgxc_group, PgxcGroupGroupNameIndexId, true, NULL, 0, NULL);

            while (HeapTupleIsValid(tuple1 = systable_getnext(scan))) {
                Form_pgxc_group groupForm = (Form_pgxc_group)GETSTRUCT(tuple1);

                if (groupForm->in_redistribution == 'n' &&
                    strcmp(NameStr(groupForm->group_name), NameStr(pgxcClassForm->pgroup)) == 0) {
                    found = true;
                    break;
                }
            }

            systable_endscan(scan);
            heap_close(pgxc_group, AccessShareLock);
        }
        ReleaseSysCache(tuple);
        if (!found) {
            /* for temp table not belonged to this seesion, just ignore */
            if (!isOtherTempNamespace(pgClassForm->relnamespace)) {
                char* tempNamespaceName = get_namespace_name(u_sess->catalog_cxt.myTempNamespace);
                char tempToastNamespaceName[NAMEDATALEN] = {0};

                int rc = snprintf_s(
                    tempToastNamespaceName, NAMEDATALEN, NAMEDATALEN - 1, "pg_toast_temp_%s", &tempNamespaceName[8]);
                securec_check_ss(rc, "\0", "\0");

                if (!missing_ok)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("temp relation is invalid because of cluster resizing"),
                            errhint("try \"DROP SCHEMA if exists %s, %s CASCADE\" or quit this session to clean temp "
                                    "object, and then re-create your temp table",
                                tempNamespaceName,
                                tempToastNamespaceName)));
                else
                    ereport(WARNING,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("temp relation is invalid because of cluster resizing"),
                            errhint("try \"DROP SCHEMA if exists %s, %s CASCADE\" or quit this session to clean temp "
                                    "object, and then re-create your temp table",
                                tempNamespaceName,
                                tempToastNamespaceName)));
            }
            return false;
        }
    }
    /*
     * else case: when drop index, the parent relation has been dropped, and the relation
     * def is acuriqued from relcache. So when checkGroup, if there is no entry
     * in pg_class, just return true.
     */

    return true;
}

#ifdef ENABLE_UT
void
#else
static void
#endif
dropExistTempNamespace(char *namespaceName)
{
    DropStmt* drop_stmt = NULL;
    int ret;
    char str[NAMEDATALEN * 2 + 64] = {0};

    drop_stmt = makeNode(DropStmt);
    drop_stmt->objects = list_make1(list_make1(makeString(namespaceName)));
    drop_stmt->arguments = NIL;
    drop_stmt->removeType = OBJECT_SCHEMA;
    drop_stmt->behavior = DROP_CASCADE;
    drop_stmt->missing_ok = true;
    drop_stmt->concurrent = false;
    ereport(NOTICE, (errmsg("Deleting invalid temp schema %s.", namespaceName)));
    ret = snprintf_s(str, sizeof(str), sizeof(str) - 1, "DROP SCHEMA %s CASCADE", namespaceName);
    securec_check_ss(ret, "\0", "\0");
    ProcessUtility((Node*)drop_stmt, str, NULL, false, None_Receiver, false, NULL);
    CommandCounterIncrement();
}

/*
 * @Description: when datanode has restarted, the temp relaton turns invalid because
 * there is no WAL protection. So when we use a temp table's data, we should first
 * check pg_namespace if the temp namespace turn invalid because of restart.
 * @in tmepNspId - oid of tmep namespace which the temp relation used is in.
 * @return: bool - If the temp namespace is valid.
 */
bool validateTempNamespace(Oid tmepNspId)
{
    if (IS_PGXC_DATANODE && !isRestoreMode && !isSingleMode && !u_sess->attr.attr_common.xc_maintenance_mode) {
        HeapTuple tuple = NULL;
        int64 creationTimeline = -1;
        bool isNull = false;

        tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(tmepNspId));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for namespace %u", tmepNspId)));

        creationTimeline = SysCacheGetAttr(NAMESPACENAME, tuple, Anum_pg_namespace_nsptimeline, &isNull);
        Assert(!isNull);

        ReleaseSysCache(tuple);
        if (creationTimeline != get_controlfile_timeline())
            return false;
    }

    return true;
}

/*
 * @Description: when datanode has restarted, the temp relaton turns invalid because
 * there is no WAL protection. So when we use a temp table's data, we should first
 * check pg_namespace if the temp namespace turn invalid because of restart.  Is so,
 * report an error.
 * @in rel - the relation to be checked.
 * @return: void
 */
void validateTempRelation(Relation rel)
{
    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !validateTempNamespace(rel->rd_rel->relnamespace)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                errmsg("Temp table's data is invalid because datanode %s restart. "
                       "Quit your session to clean invalid temp tables.",
                    g_instance.attr.attr_common.PGXCNodeName)));
    }
}

/*
 * If it is in the path, it might still not be visible; it could be
 * hidden by another parser/template/dictionary/configuration of the
 * same name earlier in the path. So we must do a slow check for
 * conflicting configurations.
 */
static bool CheckTSObjectVisible(NameData objname, SysCacheIdentifier id, Oid nmspace)
{
    char* name = NameStr(objname);
    ListCell* l = NULL;
    List* tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    bool visible = false;

    foreach (l, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        if (namespaceId == nmspace) {
            /* Found it first in path */
            visible = true;
            break;
        }
        if (SearchSysCacheExists2(id, PointerGetDatum(name), ObjectIdGetDatum(namespaceId))) {
            /* Found something else first in path */
            break;
        }
    }

    list_free_ext(tempActiveSearchPath);

    return visible;
}

/* Initialize temp namespace if first time through */
static bool InitTempTblNamespace()
{
    bool init = false;

    if (!OidIsValid(u_sess->catalog_cxt.myTempNamespace)) {
        InitTempTableNamespace();
        init = true;
    } else if (get_namespace_name(u_sess->catalog_cxt.myTempNamespace) == NULL) {
        if (u_sess->catalog_cxt.overrideStack) {
            u_sess->catalog_cxt.overrideStackValid = false;
            u_sess->catalog_cxt.myTempNamespaceOld = u_sess->catalog_cxt.myTempNamespace;
        }
        u_sess->catalog_cxt.baseSearchPathValid = false;
        u_sess->catalog_cxt.myTempNamespace = InvalidOid;
        InitTempTableNamespace();
        init = true;
    }

    return init;
}

static void CheckTempTblAlias()
{
    bool result = InitTempTblNamespace();

    if (result) {
        return;
    } else if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
    } else if (!validateTempNamespace(u_sess->catalog_cxt.myTempNamespace)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                errmsg("Temp tables are invalid because datanode %s restart. "
                       "Quit your session to clean invalid temp tables.",
                    g_instance.attr.attr_common.PGXCNodeName)));
    }
}

Oid GetOidBySchemaName(bool missing_ok)
{
    StringInfo error_info;
    error_info = makeStringInfo();
    u_sess->catalog_cxt.baseSearchPathValid = false;

    /* use the default creation namespace */
    recomputeNamespacePath(error_info);
    if (u_sess->catalog_cxt.activeTempCreationPending) {
        pfree_ext(error_info->data);
        pfree_ext(error_info);

        /* Need to initialize temp namespace */
        InitTempTableNamespace();
        return u_sess->catalog_cxt.myTempNamespace;
    }

    Oid namespaceId = u_sess->catalog_cxt.activeCreationNamespace;

    if (!OidIsValid(namespaceId) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("%s", error_info->data)));

    pfree_ext(error_info->data);
    pfree_ext(error_info);

    return namespaceId;
}

static Oid GetTSObjectOid(const char* objname, SysCacheIdentifier id)
{
    Oid objOid = InvalidOid;
    ListCell* lc = NULL;
    List* activeSearchPath = NIL;

    recomputeNamespacePath();

    activeSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    foreach (lc, activeSearchPath) {
        Oid namespaceId = lfirst_oid(lc);

        if (namespaceId == u_sess->catalog_cxt.myTempNamespace)
            continue; /* do not look in temp namespace */

        objOid = GetSysCacheOid2(id, PointerGetDatum(objname), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(objOid))
            break;
    }

    list_free_ext(activeSearchPath);

    return objOid;
}

// end of file
