/* -------------------------------------------------------------------------
 *
 * lockcmds.cpp
 *	  LOCK command support code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/lockcmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits_fn.h"
#include "commands/lockcmds.h"
#include "miscadmin.h"
#include "parser/parse_clause.h"
#include "storage/lmgr.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static void LockTableRecurse(Oid reloid, LOCKMODE lockmode, bool nowait);
static AclResult LockTableAclCheck(Oid relid, LOCKMODE lockmode);
static void RangeVarCallbackForLockTable(
    const RangeVar* rv, Oid relid, Oid oldrelid, bool target_is_partition, void* arg);

/*
 * LOCK TABLE
 */
void LockTableCommand(LockStmt* lockstmt)
{
    if (DB_IS_CMPT(B_FORMAT) && lockstmt->isLockTables) {
        TransactionState s = GetCurrentTransactionState();
        s->blockState = TBLOCK_INPROGRESS;
    }
    ListCell* p = NULL;

    /*
     * During recovery we only accept these variations: LOCK TABLE foo IN
     * ACCESS SHARE MODE LOCK TABLE foo IN ROW SHARE MODE LOCK TABLE foo IN
     * ROW EXCLUSIVE MODE This test must match the restrictions defined in
     * LockAcquire()
     */
    if (lockstmt->mode > RowExclusiveLock)
        PreventCommandDuringRecovery("LOCK TABLE");

    /*
     * Iterate over the list and process the named relations one at a time
     */
    foreach (p, lockstmt->relations) {
        RangeVar* rv = NULL;
        List* rv_lockmode = NULL;
        if (u_sess->attr.attr_sql.dolphin && lockstmt->mode == NoLock){
            // here p is a [releation,lock_mode] list
            rv_lockmode = (List*)lfirst(p);
            rv = (RangeVar*)linitial(rv_lockmode);
            int *mode = (int*)lsecond(rv_lockmode);
            lockstmt->mode = *mode;
        } else {
            rv = (RangeVar*)lfirst(p);
        }
        
        bool recurse = interpretInhOption(rv->inhOpt);
        Oid reloid;

        reloid = get_relname_relid(rv->relname, PG_CATALOG_NAMESPACE);
        if (unlikely(OidIsValid(reloid) && reloid < FirstBootstrapObjectId &&
            lockstmt->mode == AccessExclusiveLock && !u_sess->attr.attr_common.xc_maintenance_mode)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("permission denied: \"%s\" is a system catalog", rv->relname),
                     errhint("use xc_maintenance_mode to lock this system catalog")));
        }
        /* In redistribute, support auto send term to lock holder. */
        if (lockstmt->cancelable && !u_sess->attr.attr_sql.enable_cluster_resize) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Only support gs_redis application using")));
        }
        t_thrd.xact_cxt.enable_lock_cancel = lockstmt->cancelable;

        reloid = RangeVarGetRelidExtended(rv,
            lockstmt->mode,
            false,
            lockstmt->nowait,
            false,
            false,
            RangeVarCallbackForLockTable,
            (void*)&lockstmt->mode);

        TrForbidAccessRbObject(RelationRelationId, reloid, rv->relname);

        if (recurse)
            LockTableRecurse(reloid, lockstmt->mode, lockstmt->nowait);
    }
    t_thrd.xact_cxt.enable_lock_cancel = false;
}

/*
 * Before acquiring a table lock on the named table, check whether we have
 * permission to do so.
 */
static void RangeVarCallbackForLockTable(
    const RangeVar* rv, Oid relid, Oid oldrelid, bool target_is_partition, void* arg)
{
    LOCKMODE lockmode = *(LOCKMODE*)arg;
    char relkind;
    AclResult aclresult;

    if (!OidIsValid(relid))
        return; /* doesn't exist, so no permissions check */
    relkind = get_rel_relkind(relid);
    if (!relkind) {
        return; /* woops, concurrently dropped; no permissions   check */
    }

    /* Currently, we only allow plain tables to be locked */
    if (relkind != RELKIND_RELATION)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a table", rv->relname)));

    /* Check permissions. */
    aclresult = LockTableAclCheck(relid, lockmode);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_CLASS, rv->relname);
}

/*
 * Apply LOCK TABLE recursively over an inheritance tree
 *
 * We use find_inheritance_children not find_all_inheritors to avoid taking
 * locks far in advance of checking privileges.  This means we'll visit
 * multiply-inheriting children more than once, but that's no problem.
 */
static void LockTableRecurse(Oid reloid, LOCKMODE lockmode, bool nowait)
{
    List* children = NIL;
    ListCell* lc = NULL;

    children = find_inheritance_children(reloid, NoLock);

    foreach (lc, children) {
        Oid childreloid = lfirst_oid(lc);
        AclResult aclresult;

        /* Check permissions before acquiring the lock. */
        aclresult = LockTableAclCheck(childreloid, lockmode);
        if (aclresult != ACLCHECK_OK) {
            char* relname = get_rel_name(childreloid);

            if (relname == NULL)
                continue; /* child concurrently dropped, just skip it */
            aclcheck_error(aclresult, ACL_KIND_CLASS, relname);
        }

        /* We have enough rights to lock the relation; do so. */
        if (!nowait)
            LockRelationOid(childreloid, lockmode);
        else if (!ConditionalLockRelationOid(childreloid, lockmode)) {
            /* try to throw error by name; relation could be deleted... */
            char* relname = get_rel_name(childreloid);

            if (relname == NULL)
                continue; /* child concurrently dropped, just skip it */
            ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("could not obtain lock on relation \"%s\"", relname)));
        }

        /*
         * Even if we got the lock, child might have been concurrently
         * dropped. If so, we can skip it.
         */
        if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(childreloid))) {
            /* Release useless lock */
            UnlockRelationOid(childreloid, lockmode);
            continue;
        }

        LockTableRecurse(childreloid, lockmode, nowait);
    }
}

/*
 * Check whether the current user is permitted to lock this relation.
 */
static AclResult LockTableAclCheck(Oid reloid, LOCKMODE lockmode)
{
    AclResult aclresult;

    /*
     * Just return aclok when current user is superuser, although pg_class_aclcheck
     * also used superuser() function but it forbid the INSERT/DELETE/SELECT/UPDATE
     * for superuser in independent condition. Here LOCK is no need to forbid.
     */
    if (superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        return ACLCHECK_OK;

    /* Verify adequate privilege */
    if (lockmode == AccessShareLock)
        aclresult = pg_class_aclcheck(reloid, GetUserId(), ACL_SELECT);
    else
        aclresult = pg_class_aclcheck(reloid, GetUserId(), ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE);
    return aclresult;
}
