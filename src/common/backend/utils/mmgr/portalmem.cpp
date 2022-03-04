/* -------------------------------------------------------------------------
 *
 * portalmem.c
 *	  backend portal memory management
 *
 * Portals are objects representing the execution state of a query.
 * This module provides memory management services for portals, but it
 * doesn't actually run the executor for them.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/portalmem.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/portalcmds.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamMain.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/plpgsql.h"
#include "utils/timestamp.h"
#include "utils/resowner.h"
#include "nodes/execnodes.h"
#include "opfusion/opfusion.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/hash.h"
#include "catalog/pg_collation.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#endif


extern void ReleaseSharedCachedPlan(CachedPlan* plan, bool useResOwner);
/*
 * Estimate of the maximum number of open portals a user would have,
 * used in initially sizing the PortalHashTable in EnablePortalManager().
 * Since the hash table can expand, there's no need to make this overly
 * generous, and keeping it small avoids unnecessary overhead in the
 * hash_seq_search() calls executed during transaction end.
 */
#define PORTALS_PER_USER 16

/* ----------------
 *		Global state
 * ----------------
 */

#define MAX_PORTALNAME_LEN NAMEDATALEN

typedef struct portalhashent {
    char portalname[MAX_PORTALNAME_LEN];
    Portal portal;
} PortalHashEnt;

#define PortalHashTableLookup(NAME, PORTAL)                                                              \
    do {                                                                                                 \
        PortalHashEnt* hentry = NULL;                                                                    \
                                                                                                         \
        hentry = (PortalHashEnt*)hash_search(u_sess->exec_cxt.PortalHashTable, (NAME), HASH_FIND, NULL); \
        if (hentry != NULL) {                                                                            \
            PORTAL = hentry->portal;                                                                     \
        } else {                                                                                         \
            PORTAL = NULL;                                                                               \
        }                                                                                                \
    } while (0)

#define PortalHashTableInsert(PORTAL, NAME)                                                                 \
    do {                                                                                                    \
        PortalHashEnt* hentry = NULL;                                                                       \
        bool found = false;                                                                                 \
                                                                                                            \
        hentry = (PortalHashEnt*)hash_search(u_sess->exec_cxt.PortalHashTable, (NAME), HASH_ENTER, &found); \
        if (found) {                                                                                        \
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("duplicate portal name")));               \
        }                                                                                                   \
        hentry->portal = PORTAL;                                                                            \
        /* To avoid duplicate storage, make PORTAL->name point to htab entry */                             \
        (PORTAL)->name = hentry->portalname;                                                                \
    } while (0)

#define PortalHashTableDelete(PORTAL)                                                                              \
    do {                                                                                                           \
        PortalHashEnt* hentry = NULL;                                                                              \
                                                                                                                   \
        hentry = (PortalHashEnt*)hash_search(u_sess->exec_cxt.PortalHashTable, (PORTAL)->name, HASH_REMOVE, NULL); \
        if (hentry == NULL) {                                                                                      \
            ereport(WARNING, (errmsg("trying to delete portal name that does not exist")));                        \
        }                                                                                                          \
    } while (0)

/* -------------------portal_mem_cxt---------------------------------
 *				   public portal interface functions
 * ----------------------------------------------------------------
 */
/*
 * EnablePortalManager
 *		Enables the portal management module at backend startup.
 */
void EnablePortalManager(void)
{
    HASHCTL ctl;

    Assert(u_sess->top_portal_cxt == NULL);

    u_sess->top_portal_cxt = AllocSetContextCreate(u_sess->top_mem_cxt,
        "portal_mem_cxt",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    ctl.keysize = MAX_PORTALNAME_LEN;
    ctl.entrysize = sizeof(PortalHashEnt);
    ctl.hcxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB);

    /*
     * use PORTALS_PER_USER as a guess of how many hash table entries to
     * create, initially
     */
    u_sess->exec_cxt.PortalHashTable = hash_create("Portal hash", PORTALS_PER_USER, &ctl, HASH_ELEM | HASH_CONTEXT);
}

/*
 * GetPortalByName
 *		Returns a portal given a portal name, or NULL if name not found.
 */
Portal GetPortalByName(const char* name)
{
    Portal portal;

    if (PointerIsValid(name))
        PortalHashTableLookup(name, portal);
    else
        portal = NULL;

    return portal;
}

/*
 * PortalListGetPrimaryStmt
 *		Get the "primary" stmt within a portal, ie, the one marked canSetTag.
 *
 * Returns NULL if no such stmt.  If multiple PlannedStmt structs within the
 * portal are marked canSetTag, returns the first one.	Neither of these
 * cases should occur in present usages of this function.
 *
 * Copes if given a list of Querys --- can't happen in a portal, but this
 * code also supports plancache.c, which needs both cases.
 *
 * Note: the reason this is just handed a List is so that plancache.c
 * can share the code.	For use with a portal, use PortalGetPrimaryStmt
 * rather than calling this directly.
 */
Node* PortalListGetPrimaryStmt(List* stmts)
{
    ListCell* lc = NULL;

    foreach (lc, stmts) {
        Node* stmt = (Node*)lfirst(lc);

        if (IsA(stmt, PlannedStmt)) {
            if (((PlannedStmt*)stmt)->canSetTag)
                return stmt;
        } else if (IsA(stmt, Query)) {
            if (((Query*)stmt)->canSetTag)
                return stmt;
        } else {
            /* Utility stmts are assumed canSetTag if they're the only stmt */
            if (list_length(stmts) == 1)
                return stmt;
        }
    }
    return NULL;
}

/*
 * CreatePortal
 *		Returns a new portal given a name.
 *
 * allowDup: if true, automatically drop any pre-existing portal of the
 * same name (if false, an error is raised).
 *
 * dupSilent: if true, don't even emit a WARNING.
 */
Portal CreatePortal(const char* name, bool allowDup, bool dupSilent, bool is_from_spi)
{
    Portal portal;

    AssertArg(PointerIsValid(name));

    portal = GetPortalByName(name);
    if (PortalIsValid(portal)) {
        if (allowDup == false)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_CURSOR), errmsg("cursor \"%s\" already exists", name)));
        if (dupSilent == false)
            ereport(WARNING, (errcode(ERRCODE_DUPLICATE_CURSOR), errmsg("closing existing cursor \"%s\"", name)));
        PortalDrop(portal, false);
    }

    /* make new portal structure */
    portal = (Portal)MemoryContextAllocZero(u_sess->top_portal_cxt, sizeof *portal);

    /* initialize portal heap context; typically it won't store much */
    portal->heap = AllocSetContextCreate(u_sess->top_portal_cxt,
        "PortalHeapMemory",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);

    /* create a resource owner for the portal */
    portal->resowner = ResourceOwnerCreate(t_thrd.utils_cxt.CurTransactionResourceOwner, "Portal",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    /* initialize portal fields that don't start off zero */
    portal->status = PORTAL_NEW;
    portal->cleanup = PortalCleanup;
    portal->createSubid = GetCurrentSubTransactionId();
    portal->activeSubid = portal->createSubid;
    portal->strategy = PORTAL_MULTI_QUERY;
    portal->cursorOptions = CURSOR_OPT_NO_SCROLL;
    portal->cursorHoldUserId = InvalidOid;
    portal->cursorHoldSecRestrictionCxt = 0;
    portal->atStart = true;
    portal->atEnd = true; /* disallow fetches until query is set */
    portal->visible = true;
    portal->creation_time = GetCurrentStatementStartTimestamp();
    portal->funcOid = InvalidOid;
    portal->is_from_spi = is_from_spi;
    portal->copyCxt = NULL;
    int rc = memset_s(portal->cursorAttribute, CURSOR_ATTRIBUTE_NUMBER * sizeof(void*), 0,
                      CURSOR_ATTRIBUTE_NUMBER * sizeof(void*));
    securec_check(rc, "\0", "\0");
    portal->funcUseCount = 0;
    portal->hasStreamForPlpgsql = false;
#ifndef ENABLE_MULTIPLE_NODES
    portal->streamInfo.Reset();
    portal->isAutoOutParam = false;
    portal->isPkgCur = false;
#endif
    /* put portal in table (sets portal->name) */
    PortalHashTableInsert(portal, name);

#ifdef PGXC
    if (u_sess->pgxc_cxt.PGXCNodeIdentifier == 0 && !IsAbortedTransactionBlockState()) {
        /* get pgxc_node id */
        Oid node_oid = get_pgxc_nodeoid(g_instance.attr.attr_common.PGXCNodeName);
        u_sess->pgxc_cxt.PGXCNodeIdentifier = get_pgxc_node_id(node_oid);
    }
#endif

    return portal;
}

/*
 * CreateNewPortal
 *		Create a new portal, assigning it a random nonconflicting name.
 */
Portal CreateNewPortal(bool is_from_spi)
{
    char portalname[MAX_PORTALNAME_LEN];

    /* Select a nonconflicting name */
    for (;;) {
        u_sess->exec_cxt.unnamed_portal_count++;
        errno_t rc =
            sprintf_s(portalname, sizeof(portalname), "<unnamed portal %u>", u_sess->exec_cxt.unnamed_portal_count);
        securec_check_ss_c(rc, "\0", "\0");
        if (GetPortalByName(portalname) == NULL)
            break;
    }

    return CreatePortal(portalname, false, false, is_from_spi);
}

/*
 * PortalDefineQuery
 *		A simple subroutine to establish a portal's query.
 *
 * Notes: as of PG 8.4, caller MUST supply a sourceText string; it is not
 * allowed anymore to pass NULL.  (If you really don't have source text,
 * you can pass a constant string, perhaps "(query not available)".)
 *
 * commandTag shall be NULL if and only if the original query string
 * (before rewriting) was an empty string.	Also, the passed commandTag must
 * be a pointer to a constant string, since it is not copied.
 *
 * If cplan is provided, then it is a cached plan containing the stmts, and
 * the caller must have done GetCachedPlan(), causing a refcount increment.
 * The refcount will be released when the portal is destroyed.
 *
 * If cplan is NULL, then it is the caller's responsibility to ensure that
 * the passed plan trees have adequate lifetime.  Typically this is done by
 * copying them into the portal's heap context.
 *
 * The caller is also responsible for ensuring that the passed prepStmtName
 * (if not NULL) and sourceText have adequate lifetime.
 *
 * NB: this function mustn't do much beyond storing the passed values; in
 * particular don't do anything that risks elog(ERROR).  If that were to
 * happen here before storing the cplan reference, we'd leak the plancache
 * refcount that the caller is trying to hand off to us.
 */
void PortalDefineQuery(Portal portal, const char* prepStmtName, const char* sourceText, const char* commandTag,
    List* stmts, CachedPlan* cplan)
{
    AssertArg(PortalIsValid(portal));
    AssertState(portal->status == PORTAL_NEW);

    AssertArg(sourceText != NULL);
    AssertArg(commandTag != NULL || stmts == NIL);

    portal->prepStmtName = prepStmtName;
    portal->sourceText = sourceText;
    portal->commandTag = commandTag;
    portal->stmts = stmts;
    portal->cplan = cplan;
    if (cplan) {
        pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);
    }
    portal->status = PORTAL_DEFINED;
}

/*
 * PortalReleaseCachedPlan
 *		Release a portal's reference to its cached plan, if any.
 */
static void PortalReleaseCachedPlan(Portal portal)
{
    if (portal->cplan) {
        if (!portal->cplan->isShared()) {
            Assert(portal->cplan->global_refcount > 0);
            pg_atomic_fetch_sub_u32((volatile uint32*)&portal->cplan->global_refcount, 1);
            ReleaseCachedPlan(portal->cplan, false);
        } else {
            ReleaseSharedCachedPlan(portal->cplan, false);
        }
        portal->cplan = NULL;

        /*
         * We must also clear portal->stmts which is now a dangling reference
         * to the cached plan's plan list.  This protects any code that might
         * try to examine the Portal later.
         */
        portal->stmts = NIL;
    }
}

/*
 * PortalCreateHoldStore
 *		Create the tuplestore for a portal.
 */
void PortalCreateHoldStore(Portal portal)
{
    MemoryContext oldcxt;

    Assert(portal->holdContext == NULL);
    Assert(portal->holdStore == NULL);

    /*
     * Create the memory context that is used for storage of the tuple set.
     * Note this is NOT a child of the portal's heap memory.
     */
#ifndef ENABLE_MULTIPLE_NODES
    if (portal->isAutoOutParam) {
        portal->holdContext = GetAvailableHoldContext(u_sess->plsql_cxt.auto_parent_session_pkgs->portalContext);
    } else {
        portal->holdContext = AllocSetContextCreate(u_sess->top_portal_cxt,
            "PortalHoldContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }
#else
    portal->holdContext = AllocSetContextCreate(u_sess->top_portal_cxt,
        "PortalHoldContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
#endif

    /*
     * Create the tuple store, selecting cross-transaction temp files, and
     * enabling random access only if cursor requires scrolling.
     *
     * XXX: Should maintenance_u_sess->attr.attr_memory.work_mem be used for the portal size?
     */
    oldcxt = MemoryContextSwitchTo(portal->holdContext);

    portal->holdStore =
        tuplestore_begin_heap(portal->cursorOptions & CURSOR_OPT_SCROLL, true, u_sess->attr.attr_memory.work_mem);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * PinPortal
 *		Protect a portal from dropping.
 *
 * A pinned portal is still unpinned and dropped at transaction or
 * subtransaction abort.
 */
void PinPortal(Portal portal)
{
    if (portal->portalPinned)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("portal already pinned")));

    portal->portalPinned = true;
}

void UnpinPortal(Portal portal)
{
    if (!portal->portalPinned)
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("portal not pinned")));

    portal->portalPinned = false;
}

/*
 * MarkPortalActive
 *		Transition a portal from READY to ACTIVE state.
 *
 * NOTE: never set portal->status = PORTAL_ACTIVE directly; call this instead.
 */
void MarkPortalActive(Portal portal)
{
    /* For safety, this is a runtime test not just an Assert */
    if (portal->status != PORTAL_READY)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("portal \"%s\" cannot be run", portal->name)));
    /* Perform the state transition */
    portal->status = PORTAL_ACTIVE;
    ereport(DEBUG2, (errmodule(MOD_SPI), errcode(ERRCODE_LOG),
                     errmsg("MarkPortalActive(portal name:%s, status %d, portalPinned %d, autoHeld %d)",
                                portal->name, portal->status, portal->portalPinned, portal->autoHeld)));
    portal->activeSubid = GetCurrentSubTransactionId();
}

/*
 * MarkPortalDone
 *		Transition a portal from ACTIVE to DONE state.
 *
 * NOTE: never set portal->status = PORTAL_DONE directly; call this instead.
 */
void MarkPortalDone(Portal portal)
{
    /* Perform the state transition */
    Assert(portal->status == PORTAL_ACTIVE);
    portal->status = PORTAL_DONE;
    ereport(DEBUG2, (errmodule(MOD_SPI), errcode(ERRCODE_LOG),
                     errmsg("MarkPortalDone(portal:%p, portal name:%s, status %d, portalPinned %d, autoHeld %d)",
                                portal, portal->name, portal->status, portal->portalPinned, portal->autoHeld)));

    /*
     * Allow portalcmds.c to clean up the state it knows about.  We might as
     * well do that now, since the portal can't be executed any more.
     *
     * In some cases involving execution of a ROLLBACK command in an already
     * aborted transaction, this prevents an assertion failure caused by
     * reaching AtCleanup_Portals with the cleanup hook still unexecuted.
     */
    if (PointerIsValid(portal->cleanup)) {
        (*portal->cleanup)(portal);
        portal->cleanup = NULL;
    }
}

/*
 * MarkPortalFailed
 *		Transition a portal into FAILED state.
 *
 * NOTE: never set portal->status = PORTAL_FAILED directly; call this instead.
 */
void MarkPortalFailed(Portal portal)
{
    /* Perform the state transition */
    Assert(portal->status != PORTAL_DONE);
    portal->status = PORTAL_FAILED;
    ereport(DEBUG2, (errmodule(MOD_SPI), errcode(ERRCODE_LOG),
                     errmsg("MarkPortalFailed(portal:%p, portal name:%s, status %d, portalPinned %d, autoHeld %d)",
                                portal, portal->name, portal->status, portal->portalPinned, portal->autoHeld)));

    /*
     * Allow portalcmds.c to clean up the state it knows about.  We might as
     * well do that now, since the portal can't be executed any more.
     *
     * In some cases involving cleanup of an already aborted transaction, this
     * prevents an assertion failure caused by reaching AtCleanup_Portals with
     * the cleanup hook still unexecuted.
     */
    if (PointerIsValid(portal->cleanup)) {
        (*portal->cleanup)(portal);
        portal->cleanup = NULL;
    }
}

/*
 * PortalDrop
 *		Destroy the portal.
 */
void PortalDrop(Portal portal, bool isTopCommit)
{
    if (!PortalIsValid(portal)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("portal is NULL")));
    }

    /*
     * Allow portalcmds.c to clean up the state it knows about, in particular
     * shutting down the executor if still active.	This step potentially runs
     * user-defined code so failure has to be expected.  It's the cleanup
     * hook's responsibility to not try to do that more than once, in the case
     * that failure occurs and then we come back to drop the portal again
     * during transaction abort.
     *
     * Note: in most paths of control, this will have been done already in
     * MarkPortalDone or MarkPortalFailed.	We're just making sure.
     */
    if (PointerIsValid(portal->cleanup)) {
        (*portal->cleanup)(portal);
        portal->cleanup = NULL;
    }

    /*
     * Remove portal from hash table.  Because we do this here, we will not
     * come back to try to remove the portal again if there's any error in the
     * subsequent steps.  Better to leak a little memory than to get into an
     * infinite error-recovery loop.
     */
    PortalHashTableDelete(portal);

    /* drop cached plan reference, if any */
    PortalReleaseCachedPlan(portal);

    /*
     * Release any resources still attached to the portal.	There are several
     * cases being covered here:
     *
     * Top transaction commit (indicated by isTopCommit): normally we should
     * do nothing here and let the regular end-of-transaction resource
     * releasing mechanism handle these resources too.	However, if we have a
     * FAILED portal (eg, a cursor that got an error), we'd better clean up
     * its resources to avoid resource-leakage warning messages.
     *
     * Sub transaction commit: never comes here at all, since we don't kill
     * any portals in AtSubCommit_Portals().
     *
     * Main or sub transaction abort: we will do nothing here because
     * portal->resowner was already set NULL; the resources were already
     * cleaned up in transaction abort.
     *
     * Ordinary portal drop: must release resources.  However, if the portal
     * is not FAILED then we do not release its locks.	The locks become the
     * responsibility of the transaction's ResourceOwner (since it is the
     * parent of the portal's owner) and will be released when the transaction
     * eventually ends.
     */
    if (portal->resowner && (!isTopCommit || portal->status == PORTAL_FAILED)) {
        bool isCommit = (portal->status != PORTAL_FAILED);
        ResourceOwnerRelease(portal->resowner, RESOURCE_RELEASE_BEFORE_LOCKS, isCommit, false);
        ResourceOwnerRelease(portal->resowner, RESOURCE_RELEASE_LOCKS, isCommit, false);
        ResourceOwnerRelease(portal->resowner, RESOURCE_RELEASE_AFTER_LOCKS, isCommit, false);
        ResourceOwnerDelete(portal->resowner);
    }
    portal->resowner = NULL;

    /*
     * Delete tuplestore if present.  We should do this even under error
     * conditions; since the tuplestore would have been using cross-
     * transaction storage, its temp files need to be explicitly deleted.
     */
#ifndef ENABLE_MULTIPLE_NODES
    /* autonomous transactions procedure out param portal cleaned by its parent session */
    if (portal->holdStore && !portal->isAutoOutParam) {
#else
    if (portal->holdStore) {
#endif
        MemoryContext oldcontext;

        oldcontext = MemoryContextSwitchTo(portal->holdContext);
        tuplestore_end(portal->holdStore);
        MemoryContextSwitchTo(oldcontext);
        portal->holdStore = NULL;
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (!StreamThreadAmI()) {
        portal->streamInfo.AttachToSession();
        if (u_sess->stream_cxt.global_obj != NULL) {
            StreamTopConsumerIam();
            /* Set sync point for waiting all stream threads complete. */
            StreamNodeGroup::syncQuit(STREAM_COMPLETE);
            UnRegisterStreamSnapshots();
            StreamNodeGroup::destroy(STREAM_COMPLETE);
        }
        // reset some flag related to stream
        ResetStreamEnv();
    }
#endif

    /* delete tuplestore storage, if any */
#ifndef ENABLE_MULTIPLE_NODES
    if (portal->holdContext && !portal->isAutoOutParam)
#else
    if (portal->holdContext)
#endif
        MemoryContextDelete(portal->holdContext);

    /* release subsidiary storage */
    MemoryContextDelete(PortalGetHeapMemory(portal));

    /* release gpc's copy plan */
    if (ENABLE_GPC && portal->copyCxt)
        MemoryContextDelete(portal->copyCxt);

    u_sess->parser_cxt.param_message = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        u_sess->parser_cxt.param_info = NULL;
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* reset portal cursor attribute */
    ResetCursorAtrribute(portal);
#endif
    /* release portal struct (it's in u_sess->portal_mem_cxt) */
    pfree(portal);
}

/*
 * Delete all declared cursors.
 *
 * Used by commands: CLOSE ALL, DISCARD ALL
 */
void PortalHashTableDeleteAll(void)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);
    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        /* Can't close the active portal (the one running the command) */
        if (portal->status == PORTAL_ACTIVE)
            continue;

        PortalDrop(portal, false);

        /* Restart the iteration in case that led to other drops */
        hash_seq_term(&status);
        hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);
    }
}

/*
 * "Hold" a portal.  Prepare it for access by later transactions.
 */
void HoldPortal(Portal portal)
{
    /*
     * Note that PersistHoldablePortal() must release all resources
     * used by the portal that are local to the creating transaction.
     */
    PortalCreateHoldStore(portal);
    PersistHoldablePortal(portal);

    /* drop cached plan reference, if any */
    PortalReleaseCachedPlan(portal);

    /*
     * Any resources belonging to the portal will be released in the
     * upcoming transaction-wide cleanup; the portal will no longer
     * have its own resources.
     */
    portal->resowner = NULL;

    /*
     * Having successfully exported the holdable cursor, mark it as
     * not belonging to this transaction.
     */
    portal->createSubid = InvalidSubTransactionId;
    portal->activeSubid = InvalidSubTransactionId;
}

/*
 * Pre-commit processing for portals.
 *
 * Holdable cursors created in this transaction need to be converted to
 * materialized form, since we are going to close down the executor and
 * release locks.  Non-holdable portals created in this transaction are
 * simply removed.	Portals remaining from prior transactions should be
 * left untouched.
 *
 * Returns TRUE if any portals changed state (possibly causing user-defined
 * code to be run), FALSE if not.
 */
bool PreCommit_Portals(bool isPrepare, bool STP_commit)
{
    bool result = false;
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return false;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;
        ResourceOwner   owner = portal->resowner;

        /*
         * Do not touch active portals --- this can only happen in the case of
         * a multi-transaction utility command, such as VACUUM.
         *
         * Note however that any resource owner attached to such a portal is
         * still going to go away, so don't leave a dangling pointer.
         */
        if (portal->status == PORTAL_ACTIVE) {
            /*
             *  if we are in multi commit and we also have owner, then need to clean up all the snapshots
             *  during commit time. Otherwise it will cause leak snapshots reference warning.
             */
            if(owner && STP_commit) {
                ResourceOwnerDecrementNsnapshots(owner, portal->queryDesc);
            }
              
            /*
             * if we are in commit within stored procedure need to keep resowner and it will be used to 
             * connect new local resources. OTherwise it will cause leak snapshots reference warning, beasue the
             * the new snapshtos does not have owner.
             */
            if(!STP_commit) {
                 portal->resowner = NULL;
            }
            continue;
        }

        /* Is it a holdable portal created in the current xact? */
        if (((unsigned int)(portal->cursorOptions) & CURSOR_OPT_HOLD) &&
            portal->createSubid != InvalidSubTransactionId &&
            portal->status == PORTAL_READY) {
            /*
             * We are exiting the transaction that created a holdable cursor.
             * Instead of dropping the portal, prepare it for access by later
             * transactions.
             *
             * However, if this is PREPARE TRANSACTION rather than COMMIT,
             * refuse PREPARE, because the semantics seem pretty unclear.
             */
            if (isPrepare)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot PREPARE a transaction that has created a cursor WITH HOLD")));

            HoldPortal(portal);
        } else if (portal->createSubid == InvalidSubTransactionId) {
            /*
             * Do nothing to cursors held over from a previous transaction
             * (including ones we just froze in a previous cycle of this loop)
             */
            continue;
        } else {
            /* Zap all non-holdable portals */
            PortalDrop(portal, true);

            /* Report we changed state */
            result = true;
        }

        /*
         * After either freezing or dropping a portal, we have to restart the
         * iteration, because we could have invoked user-defined code that
         * caused a drop of the next portal in the hash chain.
         */
        hash_seq_term(&status);
        hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);
    }

    return result;
}

/*
 * Abort processing for portals.
 *
 * At this point we reset "active" status and run the cleanup hook if
 * present, but we can't release the portal's memory until the cleanup call.
 *
 * The reason we need to reset active is so that we can replace the unnamed
 * portal, else we'll fail to execute ROLLBACK when it arrives.
 * At this point we run the cleanup hook if present, but we can't release the
 * portal's memory until the cleanup call.
 */
void AtAbort_Portals(bool STP_rollback)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        /*
         * Do nothing else to cursors held over from a previous transaction.
         * its reource owner.
         */
        if (portal->createSubid == InvalidSubTransactionId)
            continue;

        /*
         * Do nothing to auto-held cursors.  This is similar to the case of a
         * cursor from a previous transaction, but it could also be that the
         * cursor was auto-held in this transaction, so it wants to live on.
         */
        if (portal->autoHeld && STP_rollback) {
            continue;
        }

        /*
         * Within mult rollback need to clean up snapshots before release
         * its reource owner.
         */
        if(portal->resowner && STP_rollback) {
            ResourceOwnerDecrementNsnapshots(portal->resowner, portal->queryDesc);
        }

        /*
         * If it was created in the current transaction, we can't do normal
         * shutdown on a READY portal either; it might refer to objects
         * created in the failed transaction.  See comments in
         * AtSubAbort_Portals.
         */
        if (portal->status == PORTAL_READY)
            MarkPortalFailed(portal);

        /*
         * Allow portalcmds.c to clean up the state it knows about, if we
         * haven't already. Should haven't already. Should not clean up protal during multi commit
         * and rollback.
         */
        if (PointerIsValid(portal->cleanup) && !STP_rollback) {
            (*portal->cleanup)(portal);
            portal->cleanup = NULL;
        }

        /* Drop cached plan reference, if any.
         * When we deal with STP_rollback cases,
         * we are supposed to release the cachedplan context only if the portal is from current SPI
         * due to the fact that we skip the cleanup step above so we need to assure the cachedplan context
         * which the portal is referenced is legal.
         */
        if (!STP_rollback || portal->is_from_spi)
            PortalReleaseCachedPlan(portal);

        /*
         * Any resources belonging to the portal will be released in the
         * upcoming transaction-wide cleanup; they will be gone before we run
         * PortalDrop. Can not reset resonwer to NUll if
         * we are in STP_rollback, because the Portal is still alive. If we set 
         * resowner to NUll it will cause leak snapshots reference error, because
         * the new snapshots does not have owner.
         */
        if(!STP_rollback) {
            portal->resowner = NULL;
        }

        /*
         * Although we can't delete the portal data structure proper, we can
         * release any memory in subsidiary contexts, such as executor state.
         * The cleanup hook was the last thing that might have needed data
         * there. But leave active portal alone. This change is from pg11
         * commit and rollback patch.
         */
        if(portal->status != PORTAL_ACTIVE) {
            MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
        }
    }
}

/*
 * Post-abort cleanup for portals.
 *
 * Delete all portals not held over from prior transactions.  */
void AtCleanup_Portals(void)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        /*
         * Do not touch active portals --- this can only happen in the case of
         * a multi-transaction command.
         */
        if (portal->status == PORTAL_ACTIVE) {
            continue;
        }

		/*
		 * Do nothing to cursors held over from a previous transaction or
		 * auto-held ones.
		 */
        if (portal->createSubid == InvalidSubTransactionId || portal->autoHeld) {
            Assert(portal->status != PORTAL_ACTIVE);
            Assert(portal->resowner == NULL);
            continue;
        }

        /*
         * If a portal is still pinned, forcibly unpin it. PortalDrop will not
         * let us drop the portal otherwise. Whoever pinned the portal was
         * interrupted by the abort too and won't try to use it anymore.
         */
        if (portal->portalPinned)
            portal->portalPinned = false;

#ifdef PGXC
            /* XXX This is a PostgreSQL bug (already reported on the list by
             * Pavan). We comment out the assertion until the bug is fixed
             * upstream.
             */
#endif

        /* Zap it. */
        PortalDrop(portal, false);
    }
}

/*
 * Portal-related cleanup when we return to the main loop on error.
 *
 * This is different from the cleanup at transaction abort.  Auto-held portals
 * are cleaned up on error but not on transaction abort.
 */
void PortalErrorCleanup(void)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt *hentry = NULL;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);
    while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL) {
	Portal portal = hentry->portal;
        if (portal->autoHeld) {
            portal->portalPinned = false;
            PortalDrop(portal, false);
        }
    }
}

/*
 * Pre-subcommit processing for portals.
 *
 * Reassign the portals created in the current subtransaction to the parent
 * subtransaction.
 */
void AtSubCommit_Portals(SubTransactionId mySubid, SubTransactionId parentSubid, ResourceOwner parentXactOwner)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        if (portal->createSubid == mySubid) {
            portal->createSubid = parentSubid;
            if (portal->resowner)
                ResourceOwnerNewParent(portal->resowner, parentXactOwner);
        }
        if (portal->activeSubid == mySubid)
            portal->activeSubid = parentSubid;
    }
}

/*
 * Subtransaction abort handling for portals.
 *
 * Deactivate portals created or used during the failed subtransaction.
 * Note that per AtSubCommit_Portals, this will catch portals created/used
 * in descendants of the subtransaction too.
 *
 * We don't destroy any portals here; that's done in AtSubCleanup_Portals.
 */
void AtSubAbort_Portals(SubTransactionId mySubid, SubTransactionId parentSubid,
    ResourceOwner myXactOwner, ResourceOwner parentXactOwner, bool inSTP)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        /* Was it created in this subtransaction? */
        if (portal->createSubid != mySubid) {
            /* No, but maybe it was used in this subtransaction? */
            if (portal->activeSubid == mySubid) {
                /* Maintain activeSubid until the portal is removed */
                portal->activeSubid = parentSubid;

                /*
                 * A MarkPortalActive() caller ran an upper-level portal in
                 * this subtransaction and left the portal ACTIVE.  This can't
                 * happen, but force the portal into FAILED state for the same
                 * reasons discussed below.
                 *
                 * We assume we can get away without forcing upper-level READY
                 * portals to fail, even if they were run and then suspended.
                 * In theory a suspended upper-level portal could have
                 * acquired some references to objects that are about to be
                 * destroyed, but there should be sufficient defenses against
                 * such cases: the portal's original query cannot contain such
                 * references, and any references within, say, cached plans of
                 * PL/pgSQL functions are not from active queries and should
                 * be protected by revalidation logic.
                 */
                if (portal->status == PORTAL_ACTIVE)
                    MarkPortalFailed(portal);

                /*
                 * Also, if we failed it during the current subtransaction
                 * (either just above, or earlier), reattach its resource
                 * owner to the current subtransaction's resource owner, so
                 * that any resources it still holds will be released while
                 * cleaning up this subtransaction.  This prevents some corner
                 * cases wherein we might get Asserts or worse while cleaning
                 * up objects created during the current subtransaction
                 * (because they're still referenced within this portal).
                 */
                if (portal->status == PORTAL_FAILED && portal->resowner != NULL) {
                    ResourceOwnerNewParent(portal->resowner, myXactOwner);
                    portal->resowner = NULL;
                }
            }
            /* Done if it wasn't created in this subtransaction */
            continue;
        }

        /*
         * Force any live portals of my own subtransaction into FAILED state.
         * We have to do this because they might refer to objects created or
         * changed in the failed subtransaction, leading to crashes within
         * ExecutorEnd when portalcmds.cpp tries to close down the portal.
         */
        if (portal->status == PORTAL_READY)
            MarkPortalFailed(portal);

        if (!inSTP || portal->is_from_spi) {
            /*
             * Force any active portals into FAILED state, see comments above.
             */
            if (portal->status == PORTAL_ACTIVE) {
                MarkPortalFailed(portal);
            }

            /*
             * Allow portalcmds.c to clean up the state it knows about, if we
             * haven't already.
             */
            if (PointerIsValid(portal->cleanup)) {
                (*portal->cleanup)(portal);
                portal->cleanup = NULL;
            }

            /* drop cached plan reference, if any */
            PortalReleaseCachedPlan(portal);

            /*
             * Any resources belonging to the portal will be released in the
             * upcoming transaction-wide cleanup; they will be gone before we run
             * PortalDrop.
             */
             portal->resowner = NULL;

            /*
             * Although we can't delete the portal data structure proper, we can
             * release any memory in subsidiary contexts, such as executor state.
             * The cleanup hook was the last thing that might have needed data
             * there.
             */
            MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
        } else {
            /* clean up snapshots before release its reource owner. */
            if (portal->resowner != NULL) {
                ResourceOwnerDecrementNsnapshots(portal->resowner, portal->queryDesc);
            }

            /*
             * While rollback to savepoint outside STP, stmt top portal is still active.
             * don't destory its memory context.
             */
            if (portal->status != PORTAL_ACTIVE) {
                MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
            }
        }
    }
}

/*
 * Post-subabort cleanup for portals.
 *
 * Drop all portals created in the failed subtransaction (but note that
 * we will not drop any that were reassigned to the parent above).
 */
void AtSubCleanup_Portals(SubTransactionId mySubid)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    if (u_sess->exec_cxt.PortalHashTable == NULL)
        return;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        if (portal->createSubid != mySubid)
            continue;

        /*
         * Do not touch active portals --- this can only happen in the case of
         * a multi-transaction command.
         */
        if (portal->status == PORTAL_ACTIVE) {
            continue;
        }

        /*
         * If a portal is still pinned, forcibly unpin it. PortalDrop will not
         * let us drop the portal otherwise. Whoever pinned the portal was
         * interrupted by the abort too and won't try to use it anymore.
         */
        if (portal->portalPinned)
            portal->portalPinned = false;

        /*
         * We had better not be calling any user-defined code here
         */
        if (!t_thrd.proc_cxt.proc_exit_inprogress) {
            Assert(portal->cleanup == NULL);

            /* Zap it. */
            PortalDrop(portal, false);
        }
    }
}

/* Find all available cursors */
Datum pg_cursor(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    HASH_SEQ_STATUS hash_seq;
    PortalHashEnt* hentry = NULL;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not "
                       "allowed in this context")));

    /* need to build tuplestore in query context */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /*
     * build tupdesc for result tuples. This must match the definition of the
     * pg_cursors view in system_views.sql
     */
    tupdesc = CreateTemplateTupleDesc(6, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "statement", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "is_holdable", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "is_binary", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "is_scrollable", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "creation_time", TIMESTAMPTZOID, -1, 0);

    /*
     * We put all the tuples into a tuplestore in one scan of the hashtable.
     * This avoids any issue of the hashtable possibly changing between calls.
     */
    tupstore =
        tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);

    /* generate junk in short-term context */
    MemoryContextSwitchTo(oldcontext);

    hash_seq_init(&hash_seq, u_sess->exec_cxt.PortalHashTable);
    while ((hentry = (PortalHashEnt*)hash_seq_search(&hash_seq)) != NULL) {
        Portal portal = hentry->portal;
        Datum values[6];
        bool nulls[6];
        char* mask_string = NULL;

        /* report only "visible" entries */
        if (!portal->visible)
            continue;

        int ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        values[0] = CStringGetTextDatum(portal->name);
        mask_string = maskPassword(portal->sourceText);
        if (mask_string == NULL) {
            values[1] = CStringGetTextDatum(portal->sourceText);
        } else {
            values[1] = CStringGetTextDatum(mask_string);
            pfree_ext(mask_string);
        }
        values[2] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_HOLD);
        values[3] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_BINARY);
        values[4] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_SCROLL);
        values[5] = TimestampTzGetDatum(portal->creation_time);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    return (Datum)0;
}

bool ThereAreNoReadyPortals(void)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt* hentry = NULL;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt*)hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        if (portal->status == PORTAL_READY)
            return false;
    }
    return true;
}

/*
 * Description: reset cursor's option which is opened under current subtransaction
 *              when current transaction is abort.
 * Parameters:
 * @in mySubid: current transaction Id.
 * @in funOid: function oid, 0 if in exception block.
 * @in funUseCount：times of function had been used,  0 if in exception block.
 * @in reset: wether reset cursor's option or not.
 * Return: void
 */
void ResetPortalCursor(SubTransactionId mySubid, Oid funOid, int funUseCount, bool reset)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt *hentry = NULL;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);

    while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;
        if (portal == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("Accessing null portal entry found in portal hash table.")));
        }

        if (portal->createSubid != mySubid) {
            continue;
        }

        if (OidIsValid(funOid) && portal->funcOid != funOid) {
            continue;
        }

        if (funUseCount > 0 && portal->funcUseCount != funUseCount) {
            continue;
        }

        ResetCursorOption(portal, reset);
    }
}

/*
 * Hold all pinned portals.
 *
 * A procedural language implementation that uses pinned portals for its
 * internally generated cursors can call this in its COMMIT command to convert
 * those cursors to held cursors, so that they survive the transaction end.
 * We mark those portals as "auto-held" so that exception exit knows to clean
 * them up.  (In normal, non-exception code paths, the PL needs to clean those
 * portals itself, since transaction end won't do it anymore, but that should
 * be normal practice anyway.)
 */
void
HoldPinnedPortals(void)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt *hentry = NULL;

    hash_seq_init(&status, u_sess->exec_cxt.PortalHashTable);
    while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;
        if (portal->portalPinned && !portal->autoHeld) {
            /*
             * Doing transaction control, especially abort, inside a cursor
             * loop that is not read-only, for example using UPDATE
             * ... RETURNING, has weird semantics issues.  Also, this
             * implementation wouldn't work, because such portals cannot be
             * held.  (The core grammar enforces that only SELECT statements
             * can drive a cursor, but for example PL/pgSQL does not restrict
             * it.)
             */
            if (portal->strategy != PORTAL_ONE_SELECT) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION),
                    errmsg("cannot perform transaction commands inside a cursor loop that is not read-only")));
            }

            /* skip portal got error */
            if (portal->status == PORTAL_FAILED)
                continue;

            /* Verify it's in a suitable state to be held */
            if (portal->status != PORTAL_READY)
                ereport(ERROR, (errmsg("pinned portal(%s) is not ready to be auto-held, with status[%d]",
                                portal->name, portal->status)));
       
            HoldPortal(portal);
            portal->autoHeld = true;
        }
    }
}
