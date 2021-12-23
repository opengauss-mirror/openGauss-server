/* -------------------------------------------------------------------------
 *
 * portalcmds.cpp
 *	  Utility commands affecting portals (that is, SQL cursor commands)
 *
 * Note: see also tcop/pquery.c, which implements portal operations for
 * the FE/BE protocol.	This module uses pquery.c for some operations.
 * And both modules depend on utils/mmgr/portalmem.c, which controls
 * storage management for portals (but doesn't run any queries in them).
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/portalcmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "access/xact.h"
#include "commands/portalcmds.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "pgxc/execRemote.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

/*
 * PerformCursorOpen
 *		Execute SQL DECLARE CURSOR command.
 *
 * The query has already been through parse analysis, rewriting, and planning.
 * When it gets here, it looks like a SELECT PlannedStmt, except that the
 * utilityStmt field is set.
 */
void PerformCursorOpen(PlannedStmt* stmt, ParamListInfo params, const char* queryString, bool isTopLevel)
{
    DeclareCursorStmt* cstmt = (DeclareCursorStmt*)stmt->utilityStmt;
    Portal portal;
    MemoryContext oldContext;

    if (cstmt == NULL || !IsA(cstmt, DeclareCursorStmt))
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("PerformCursorOpen called for non-cursor query")));

    /*
     * Disallow empty-string cursor name (conflicts with protocol-level
     * unnamed portal).
     */
    if (!cstmt->portalname || cstmt->portalname[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_NAME), errmsg("invalid cursor name: must not be empty")));

    /*
     * If this is a non-holdable cursor, we require that this statement has
     * been executed inside a transaction block (or else, it would have no
     * user-visible effect).
     */
    if (!(cstmt->options & CURSOR_OPT_HOLD))
        RequireTransactionChain(isTopLevel, "DECLARE CURSOR");

    /*
     * Create a portal and copy the plan and queryString into its memory.
     */
    portal = CreatePortal(cstmt->portalname, false, false);

#ifdef PGXC
    /*
     * Consume the command id of the command creating the cursor
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        GetCurrentCommandId(true);
#endif

    oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));

    stmt = (PlannedStmt*)copyObject(stmt);
    stmt->utilityStmt = NULL; /* make it look like plain SELECT */

    queryString = pstrdup(queryString);

    PortalDefineQuery(portal,
        NULL,
        queryString,
        "SELECT", /* cursor's query is always a SELECT */
        list_make1(stmt),
        NULL);

    /* ----------
     * Also copy the outer portal's parameter list into the inner portal's
     * memory context.	We want to pass down the parameter values in case we
     * had a command like
     *		DECLARE c CURSOR FOR SELECT ... WHERE foo = $1
     * This will have been parsed using the outer parameter set and the
     * parameter value needs to be preserved for use when the cursor is
     * executed.
     * ----------
     */
    params = copyParamList(params);

    (void)MemoryContextSwitchTo(oldContext);

    /*
     * Set up options for portal.
     *
     * If the user didn't specify a SCROLL type, allow or disallow scrolling
     * based on whether it would require any additional runtime overhead to do
     * so.	Also, we disallow scrolling for FOR UPDATE cursors.
     */
    portal->cursorOptions = cstmt->options;
    if (!(portal->cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL))) {
        if (stmt->rowMarks == NIL && ExecSupportsBackwardScan(stmt->planTree))
            portal->cursorOptions |= CURSOR_OPT_SCROLL;
        else
            portal->cursorOptions |= CURSOR_OPT_NO_SCROLL;
    }

    if (portal->cursorOptions & CURSOR_OPT_HOLD) {
        portal->cursorHoldUserId = u_sess->misc_cxt.CurrentUserId;
        portal->cursorHoldSecRestrictionCxt = u_sess->misc_cxt.SecurityRestrictionContext;
    }

    /* For gc_fdw */
    if (u_sess->pgxc_cxt.gc_fdw_snapshot) {
        PushActiveSnapshot(u_sess->pgxc_cxt.gc_fdw_snapshot);
    }

    /*
     * Start execution, inserting parameters if any.
     */
    PortalStart(portal, params, 0, GetActiveSnapshot());

    if (u_sess->pgxc_cxt.gc_fdw_snapshot) {
        PopActiveSnapshot();
    }

    Assert(portal->strategy == PORTAL_ONE_SELECT);

    /*
     * We're done; the query won't actually be run until PerformPortalFetch is
     * called.
     */
}

/*
 * PerformPortalFetch
 *		Execute SQL FETCH or MOVE command.
 *
 *	stmt: parsetree node for command
 *	dest: where to send results
 *	completionTag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *		in which to store a command completion status string.
 *
 * completionTag may be NULL if caller doesn't want a status string.
 */
void PerformPortalFetch(FetchStmt* stmt, DestReceiver* dest, char* completionTag)
{
    Portal portal;
    long nprocessed;

    /*
     * Disallow empty-string cursor name (conflicts with protocol-level
     * unnamed portal).
     */
    if (!stmt->portalname || stmt->portalname[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_NAME), errmsg("invalid cursor name: must not be empty")));

    /* get the portal from the portal name */
    portal = GetPortalByName(stmt->portalname);
    if (!PortalIsValid(portal)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("cursor \"%s\" does not exist", stmt->portalname)));
        return; /* keep compiler happy */
    }

    /* Adjust dest if needed.  MOVE wants destination DestNone */
    if (stmt->ismove)
        dest = None_Receiver;

    bool savedIsAllowCommitRollback;
    bool needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedIsAllowCommitRollback, STP_XACT_OPEN_FOR);

    /* Do it */
    nprocessed = PortalRunFetch(portal, stmt->direction, stmt->howMany, dest);

    stp_reset_xact_state_and_err_msg(savedIsAllowCommitRollback, needResetErrMsg);

    /* Return command status if wanted */
    if (completionTag != NULL) {
        int rc = snprintf_s(completionTag,
            COMPLETION_TAG_BUFSIZE,
            COMPLETION_TAG_BUFSIZE - 1,
            "%s %ld",
            stmt->ismove ? "MOVE" : "FETCH",
            nprocessed);
        securec_check_ss(rc, "", "");
    }
}

/*
 * PerformPortalClose
 *		Close a cursor.
 */
void PerformPortalClose(const char* name)
{
    Portal portal;

    /* NULL means CLOSE ALL */
    if (name == NULL) {
        PortalHashTableDeleteAll();
        return;
    }

    /*
     * Disallow empty-string cursor name (conflicts with protocol-level
     * unnamed portal).
     */
    if (name[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_NAME), errmsg("invalid cursor name: must not be empty")));

    /*
     * get the portal from the portal name
     */
    portal = GetPortalByName(name);
    if (!PortalIsValid(portal)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("cursor \"%s\" does not exist", name)));
        return; /* keep compiler happy */
    }

    /*
     * Note: PortalCleanup is called as a side-effect, if not already done.
     */
    PortalDrop(portal, false);
}

/*
 * PortalCleanup
 *
 * Clean up a portal when it's dropped.  This is the standard cleanup hook
 * for portals.
 *
 * Note: if portal->status is PORTAL_FAILED, we are probably being called
 * during error abort, and must be careful to avoid doing anything that
 * is likely to fail again.
 */
void PortalCleanup(Portal portal)
{
    QueryDesc* queryDesc = NULL;

    /*
     * sanity checks
     */
    AssertArg(PortalIsValid(portal));
    AssertArg(portal->cleanup == PortalCleanup);

    /*
     * Shut down executor, if still running.  We skip this during error abort,
     * since other mechanisms will take care of releasing executor resources,
     * and we can't be sure that ExecutorEnd itself wouldn't fail.
     */
    queryDesc = PortalGetQueryDesc(portal);
    if (queryDesc != NULL) {
        /*
         * Reset the queryDesc before anything else.  This prevents us from
         * trying to shut down the executor twice, in case of an error below.
         * The transaction abort mechanisms will take care of resource cleanup
         * in such a case.
         */
        portal->queryDesc = NULL;

        if (portal->status != PORTAL_FAILED) {
            ResourceOwner saveResourceOwner;

            /* We must make the portal's resource owner current */
            saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
            PG_TRY();
            {
                t_thrd.utils_cxt.CurrentResourceOwner = portal->resowner;
                ExecutorFinish(queryDesc);
                ExecutorEnd(queryDesc);
                FreeQueryDesc(queryDesc);
            }
            PG_CATCH();
            {
                /* Ensure CurrentResourceOwner is restored on error */
                t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
                PG_RE_THROW();
            }
            PG_END_TRY();
            t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        }
    }
}

#ifdef ENABLE_MULTIPLE_NODES
static bool check_remote_plan(Plan* plan)
{
    if (plan == NULL)
        return false;

    if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery)) {
        RemoteQuery* remote_query = (RemoteQuery*)plan;
        if (remote_query->exec_type != EXEC_ON_COORDS)
            return true;
    }
    return check_remote_plan(plan->lefttree) || check_remote_plan(plan->righttree);
}

static bool PortalCheckRemotePlan(PlannedStmt* plannedstmt)
{
    bool is_remote_plan = false;

    if (IsA(plannedstmt, PlannedStmt)) {
        if (check_remote_plan(plannedstmt->planTree)) {
            is_remote_plan = true;
        }
    }
    return is_remote_plan;
}

#endif


/*
 * PersistHoldablePortal
 *
 * Prepare the specified Portal for access outside of the current
 * transaction. When this function returns, all future accesses to the
 * portal must be done via the Tuplestore (not by invoking the
 * executor).
 */
void PersistHoldablePortal(Portal portal)
{
    QueryDesc* queryDesc = PortalGetQueryDesc(portal);
    Portal saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext oldcxt;
    Oid saveUserid;
    int saveSecContext;

    /*
     * If we're preserving a holdable portal, we had better be inside the
     * transaction that originally created it.
     */
    Assert(portal->createSubid != InvalidSubTransactionId);
    Assert(queryDesc != NULL);

    /*
     * Caller must have created the tuplestore already.
     */
    Assert(portal->holdContext != NULL);
    Assert(portal->holdStore != NULL);

    /*
     * Before closing down the executor, we must copy the tupdesc into
     * long-term memory, since it was created in executor memory.
     */
    oldcxt = MemoryContextSwitchTo(portal->holdContext);

    portal->tupDesc = CreateTupleDescCopy(portal->tupDesc);

    MemoryContextSwitchTo(oldcxt);

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    savePortalContext = t_thrd.mem_cxt.portal_mem_cxt;

    if (OidIsValid(portal->cursorHoldUserId)) {
        /* GetUserIdAndSecContext is cheap enough that no harm in a wasted call */
        GetUserIdAndSecContext(&saveUserid, &saveSecContext);
        SetUserIdAndSecContext(portal->cursorHoldUserId, 
            portal->cursorHoldSecRestrictionCxt | SECURITY_LOCAL_USERID_CHANGE);
    }
    PG_TRY();
    {
        ActivePortal = portal;
        t_thrd.utils_cxt.CurrentResourceOwner = portal->resowner;
        t_thrd.mem_cxt.portal_mem_cxt = PortalGetHeapMemory(portal);

        MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);

        PushActiveSnapshot(queryDesc->snapshot);

        /*
         * Rewind the executor: we need to store the entire result set in the
         * tuplestore, so that subsequent backward FETCHs can be processed.
         */
        ExecutorRewind(queryDesc);

        /*
         * Change the destination to output to the tuplestore.	Note we tell
         * the tuplestore receiver to detoast all data passed through it.
         */
        queryDesc->dest = CreateDestReceiver(DestTuplestore);
        SetTuplestoreDestReceiverParams(queryDesc->dest, portal->holdStore, portal->holdContext, true);

        /* Fetch the result set into the tuplestore */
        ExecutorRun(queryDesc, ForwardScanDirection, 0L);
#ifdef ENABLE_MULTIPLE_NODES
        bool is_remote_plan = PortalCheckRemotePlan(queryDesc->plannedstmt);
#endif

        (*queryDesc->dest->rDestroy)(queryDesc->dest);
        queryDesc->dest = NULL;

        /*
         * Now shut down the inner executor.
         */
        portal->queryDesc = NULL; /* prevent double shutdown */
        ExecutorFinish(queryDesc);
        ExecutorEnd(queryDesc);
        FreeQueryDesc(queryDesc);

        /*
         * Set the position in the result set: ideally, this could be
         * implemented by just skipping straight to the tuple # that we need
         * to be at, but the tuplestore API doesn't support that. So we start
         * at the beginning of the tuplestore and iterate through it until we
         * reach where we need to be.  (Fortunately, the
         * typical case is that we're supposed to be at or near the start of
         * the result set, so this isn't as bad as it sounds.)
         */
        MemoryContextSwitchTo(portal->holdContext);

        /*
         * This code, previously used only in cursor hold mode, is reused when we add commit/rollback features;
         * distributed or single nodes will call tuplestore_rescan to rewind the active read pointer to start, 
         * but singlenode needs to call tuplestore_advance separately to mark the data that has been read before the deletion
         */

#ifdef ENABLE_MULTIPLE_NODES
        if ((portal->cursorOptions & CURSOR_OPT_HOLD) || (!is_remote_plan)) {
#endif
            if (portal->atEnd) {
                /* we can handle this case even if posOverflow */
                while (tuplestore_advance(portal->holdStore, true))
                    /* continue */;
            } else {
                long store_pos;

                if (portal->posOverflow) /* oops, cannot trust portalPos */
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("could not reposition held cursor")));

                tuplestore_rescan(portal->holdStore);

                for (store_pos = 0; store_pos < portal->portalPos; store_pos++) {
                    if (!tuplestore_advance(portal->holdStore, true))
                        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_CHUNK_VALUE), errmsg("unexpected end of tuple stream")));
                }
            }
#ifdef ENABLE_MULTIPLE_NODES
        }
        else {
            tuplestore_rescan(portal->holdStore);
        }
#endif
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;
        if (OidIsValid(portal->cursorHoldUserId)) {
            SetUserIdAndSecContext(saveUserid, saveSecContext);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcxt);

    /* Mark portal not active */
    portal->status = PORTAL_READY;

    ActivePortal = saveActivePortal;
    t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
    t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;
    if (OidIsValid(portal->cursorHoldUserId)) {
        SetUserIdAndSecContext(saveUserid, saveSecContext);
    }
    PopActiveSnapshot();

    /*
     * We can now release any subsidiary memory of the portal's heap context;
     * we'll never use it again.  The executor already dropped its context,
     * but this will clean up anything that glommed onto the portal's heap via
     * t_thrd.mem_cxt.portal_mem_cxt.
     */
    MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
}
