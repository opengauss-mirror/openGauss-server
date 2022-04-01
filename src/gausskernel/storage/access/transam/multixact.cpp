/* -------------------------------------------------------------------------
 *
 * multixact.cpp
 *		PostgreSQL multi-transaction-log manager
 *
 * The pg_multixact manager is a pg_clog-like manager that stores an array of
 * MultiXactMember for each MultiXactId.  It is a fundamental part of the
 * shared-row-lock implementation.  Each MultiXactMember is comprised of a
 * TransactionId a set of flag bits(high 3 bits record status, low 60 bits record
 * transactionid).
 *
 * The meaning of the flag bits is opaque to this module, but they are mostly
 * used in heapam.cpp to identify lock modes that each of the member transactions
 * is holding on any given tuple.  This module just contains support to store
 * and retrieve the arrays.
 *
 * We use two SLRU areas, one for storing the offsets at which the data
 * starts for each MultiXactId in the other one.  This trick allows us to
 * store variable length arrays of TransactionIds.	(We could alternatively
 * use one area containing counts and TransactionIds, with valid MultiXactId
 * values pointing at slots containing counts; but that way seems less robust
 * since it would get completely confused if someone inquired about a bogus
 * MultiXactId that pointed to an intermediate slot containing an XID.)
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * OFFSETs or MEMBERs page is initialized to zeroes, as well as an XLOG record
 * whenever a new MultiXactId is defined.  This allows us to completely
 * rebuild the data entered since the last checkpoint during XLOG replay.
 * Because this is possible, we need not follow the normal rule of
 * "write WAL before data"; the only correctness guarantee needed is that
 * we flush and sync all dirty OFFSETs and MEMBERs pages to disk before a
 * checkpoint is considered complete.  If a page does make it to disk ahead
 * of corresponding WAL records, it will be forcibly zeroed before use anyway.
 * Therefore, we don't need to mark our pages with LSN information; we have
 * enough synchronization already.
 *
 * Like clog.c, and unlike subtrans.c, we have to preserve state across
 * crashes and ensure that MXID and offset numbering increases monotonically
 * across a crash.	We do this in the same way as it's done for transaction
 * IDs: the WAL record is guaranteed to contain evidence of every MXID we
 * could need to worry about, and we just make sure that at the end of
 * replay, the next-MXID and next-offset counters are at least as large as
 * anything we saw during replay.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/multixact.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/multixact.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogproc.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "storage/smgr/fd.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/*
 * Defines for MultiXactOffset page sizes.	A page is the same BLCKSZ as is
 * used everywhere else in openGauss.
 *
 * We need four bytes per offset and also four bytes per member
 */
#define MULTIXACT_OFFSETS_PER_PAGE (BLCKSZ / sizeof(MultiXactOffset))
#define MULTIXACT_MEMBERS_PER_PAGE (BLCKSZ / sizeof(TransactionId))

#define MultiXactIdToOffsetPage(xid) ((xid) / (MultiXactOffset)MULTIXACT_OFFSETS_PER_PAGE)
#define MultiXactIdToOffsetEntry(xid) ((xid) % (MultiXactOffset)MULTIXACT_OFFSETS_PER_PAGE)

#define MXOffsetToMemberPage(xid) ((xid) / (TransactionId)MULTIXACT_MEMBERS_PER_PAGE)
#define MXOffsetToMemberEntry(xid) ((xid) % (TransactionId)MULTIXACT_MEMBERS_PER_PAGE)

/*
 * MultiXact state shared across all backends.	All this state is protected
 * by MultiXactGenLock.  (We also use MultiXactOffsetControlLock and
 * MultiXactMemberControlLock to guard accesses to the two sets of SLRU
 * buffers.  For concurrency's sake, we avoid holding more than one of these
 * locks at a time.)
 */
typedef struct MultiXactStateData {
    /* next-to-be-assigned MultiXactId */
    MultiXactId nextMXact;

    /* next-to-be-assigned offset */
    MultiXactOffset nextOffset;

    /* the Offset SLRU area was last truncated at this MultiXactId */
    MultiXactId lastTruncationPoint;

    /*
     * oldest multixact that is still on disk.  Anything older than this should
     * not be consulted.
     */
    MultiXactId oldestMultiXactId;
    Oid oldestMultiXactDB;
    MultiXactId multiVacLimit;

    /*
     * Per-backend data starts here.  We have two arrays stored in the area
     * immediately following the MultiXactStateData struct. Each is indexed by
     * BackendId.
     *
     * In both arrays, there's a slot for all normal backends (1..g_instance.shmem_cxt.MaxBackends)
     * followed by a slot for max_prepared_xacts prepared transactions. Valid
     * BackendIds start from 1; element zero of each array is never used.
     *
     * OldestMemberMXactId[k] is the oldest MultiXactId each backend's current
     * transaction(s) could possibly be a member of, or InvalidMultiXactId
     * when the backend has no live transaction that could possibly be a
     * member of a MultiXact.  Each backend sets its entry to the current
     * nextMXact counter just before first acquiring a shared lock in a given
     * transaction, and clears it at transaction end. (This works because only
     * during or after acquiring a shared lock could an XID possibly become a
     * member of a MultiXact, and that MultiXact would have to be created
     * during or after the lock acquisition.)
     *
     * OldestVisibleMXactId[k] is the oldest MultiXactId each backend's
     * current transaction(s) think is potentially live, or InvalidMultiXactId
     * when not in a transaction or not in a transaction that's paid any
     * attention to MultiXacts yet.  This is computed when first needed in a
     * given transaction, and cleared at transaction end.  We can compute it
     * as the minimum of the valid OldestMemberMXactId[] entries at the time
     * we compute it (using nextMXact if none are valid).  Each backend is
     * required not to attempt to access any SLRU data for MultiXactIds older
     * than its own OldestVisibleMXactId[] setting; this is necessary because
     * the checkpointer could truncate away such data at any instant.
     *
     * The checkpointer can compute the safe truncation point as the oldest
     * valid value among all the OldestMemberMXactId[] and
     * OldestVisibleMXactId[] entries, or nextMXact if none are valid.
     * Clearly, it is not possible for any later-computed OldestVisibleMXactId
     * value to be older than this, and so there is no risk of truncating data
     * that is still needed.
     */
    MultiXactId perBackendXactIds[1]; /* VARIABLE LENGTH ARRAY */
} MultiXactStateData;

/*
 * Last element of OldestMemberMXactID and OldestVisibleMXactId arrays.
 * Valid elements are (1..MaxOldestSlot); element 0 is never used.
 */
#define MaxOldestSlot (g_instance.shmem_cxt.MaxBackends + \
    g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS)

/*
 * Definitions for the backend-local MultiXactId cache.
 *
 * We use this cache to store known MultiXacts, so we don't need to go to
 * SLRU areas every time.
 *
 * The cache lasts for the duration of a single transaction, the rationale
 * for this being that most entries will contain our own TransactionId and
 * so they will be uninteresting by the time our next transaction starts.
 * (XXX not clear that this is correct --- other members of the MultiXact
 * could hang around longer than we did.  However, it's not clear what a
 * better policy for flushing old cache entries would be.)
 *
 * We allocate the cache entries in a memory context that is deleted at
 * transaction end, so we don't need to do retail freeing of entries.
 */
typedef struct mXactCacheEnt {
    struct mXactCacheEnt *next;
    MultiXactId multi;
    int nmembers;
    MultiXactMember members[FLEXIBLE_ARRAY_MEMBER];
} mXactCacheEnt;

#ifdef MULTIXACT_DEBUG
#define debug_elog2(a, b) elog(a, b)
#define debug_elog3(a, b, c) elog(a, b, c)
#define debug_elog4(a, b, c, d) elog(a, b, c, d)
#else
#define debug_elog2(a, b)
#define debug_elog3(a, b, c)
#define debug_elog4(a, b, c, d)
#endif

/* internal MultiXactId management */
static void MultiXactIdSetOldestVisible(void);
static MultiXactId CreateMultiXactId(int nmembers, MultiXactMember *members);
static void RecordNewMultiXact(MultiXactId multi, MultiXactOffset offset, int nmembers, TransactionId *xidsWtihStatus);
static MultiXactId GetNewMultiXactId(int nxids, MultiXactOffset *offset);

/* MultiXact cache management */
static int MXactMemberComparator(const void *arg1, const void *arg2);
static MultiXactId mXactCacheGetBySet(int nmembers, MultiXactMember *members);
static int mXactCacheGetById(MultiXactId multi, MultiXactMember **members);
static void mXactCachePut(MultiXactId multi, int nmembers, MultiXactMember *members);

static char *mxid_to_string(MultiXactId multi, int nmembers, MultiXactMember *members);
static const char *MXStatusToString(MultiXactStatus status);

/* management of SLRU infrastructure */
static int ZeroMultiXactOffsetPage(int64 pageno, bool writeXlog);
static int ZeroMultiXactMemberPage(int64 pageno, bool writeXlog);
static void ExtendMultiXactOffset(MultiXactId multi);
static void ExtendMultiXactMember(MultiXactOffset offset, int nmembers);
static void WriteMZeroPageXlogRec(int64 pageno, uint8 info);

static void get_multixact_pageno(uint8 info, int64 *pageno, XLogReaderState *record);

/*
 * MultiXactIdCreate
 *		Construct a MultiXactId representing two TransactionIds.
 *
 * The two XIDs must be different, or be requesting different statuses.
 *
 * NB - we don't worry about our local MultiXactId cache here, because that
 * is handled by the lower-level routines.
 */
MultiXactId MultiXactIdCreate(TransactionId xid1, MultiXactStatus status1,
    TransactionId xid2, MultiXactStatus status2)
{
    MultiXactId newMulti;
    MultiXactMember members[2];
    int nmembers = 2;

    AssertArg(TransactionIdIsValid(xid1));
    AssertArg(TransactionIdIsValid(xid2));

    Assert(!TransactionIdEquals(xid1, xid2) || (status1 != status2));

    /*
     * Note: unlike MultiXactIdExpand, we don't bother to check that both XIDs
     * are still running.  In typical usage, xid2 will be our own XID and the
     * caller just did a check on xid1, so it'd be wasted effort.
     */
    members[0].xid = xid1;
    members[0].status = status1;
    members[1].xid = xid2;
    members[1].status = status2;

    newMulti = CreateMultiXactId(2, members);

    ereport(DEBUG2, (errmsg("Create: :%s", mxid_to_string(newMulti, nmembers, members))));

    return newMulti;
}

/*
 * MultiXactIdExpand
 *		Add a TransactionId to a pre-existing MultiXactId.
 *
 * If the TransactionId is already a member of the passed MultiXactId with the,
 * same status, just return it as-is.
 *
 * Note that we do NOT actually modify the membership of a pre-existing
 * MultiXactId; instead we create a new one.  This is necessary to avoid
 * a race condition against code trying to wait for one MultiXactId to finish;
 * see notes in heapam.cpp.
 *
 * NB - we don't worry about our local MultiXactId cache here, because that
 * is handled by the lower-level routines.
 */
MultiXactId MultiXactIdExpand(MultiXactId multi, TransactionId xid, MultiXactStatus status)
{
    MultiXactId newMulti;
    MultiXactMember *members = NULL;
    MultiXactMember *newMembers = NULL;
    int nmembers;
    int i;
    int j;

    AssertArg(MultiXactIdIsValid(multi));
    AssertArg(TransactionIdIsValid(xid));

    ereport(DEBUG2, (errmsg("Expand: received multi " XID_FMT ", xid " XID_FMT ", status %s", multi, xid,
        MXStatusToString(status))));

    nmembers = GetMultiXactIdMembers(multi, &members);
    if (nmembers < 0) {
        /*
         * The MultiXactId is obsolete.  This can only happen if all the
         * MultiXactId members stop running between the caller checking and
         * passing it to us.  It would be better to return that fact to the
         * caller, but it would complicate the API and it's unlikely to happen
         * too often, so just deal with it by creating a singleton MultiXact.
         */
        MultiXactMember member;
        member.xid = xid;
        member.status = status;
        newMulti = CreateMultiXactId(1, &member);

        ereport(DEBUG2, (errmsg("Expand: " XID_FMT " has no members, create singleton " XID_FMT, multi, newMulti)));
        return newMulti;
    }

    /*
     * If the TransactionId is already a member of the MultiXactId with the
     * same status, just return the existing MultiXactId.
     */
    for (i = 0; i < nmembers; i++) {
        if (TransactionIdEquals(members[i].xid, xid) && members[i].status == status) {
            ereport(DEBUG2, (errmsg("Expand: " XID_FMT " is already a member of " XID_FMT, xid, multi)));
            pfree(members);
            members = NULL;
            return multi;
        }
    }

    /*
     * Determine which of the members of the MultiXactId are still of interest.
     * This is any running transaction, and also any transaction that grabbed
     * something stronger than just a lock and was committed.  (An update that
     * aborted is of no interest here.)
     *
     * (Removing dead members is just an optimization, but a useful one.
     * Note we have the same race condition here as above: j could be 0 at the
     * end of the loop.)
     */
    newMembers = (MultiXactMember *)palloc(sizeof(MultiXactMember) * (unsigned)(nmembers + 1));

    for (i = 0, j = 0; i < nmembers; i++) {
        if (TransactionIdIsInProgress(members[i].xid) ||
            (ISUPDATE_from_mxstatus(members[i].status) && TransactionIdDidCommit(members[i].xid))) {
                newMembers[j].xid = members[i].xid;
                newMembers[j++].status = members[i].status;
            }
    }

    newMembers[j].xid = xid;
    newMembers[j++].status = status;
    newMulti = CreateMultiXactId(j, newMembers);

    pfree(members);
    pfree(newMembers);
    members = NULL;
    newMembers = NULL;

    ereport(DEBUG2, (errmsg("Expand: returning new multi " XID_FMT, newMulti)));

    return newMulti;
}

/*
 * MultiXactIdIsRunning
 *		Returns whether a MultiXactId is "running".
 *
 * We return true if at least one member of the given MultiXactId is still
 * running.  Note that a "false" result is certain not to change,
 * because it is not legal to add members to an existing MultiXactId.
 */
bool MultiXactIdIsRunning(MultiXactId multi)
{
    MultiXactMember *members = NULL;
    int nmembers;
    int i;

    ereport(DEBUG2, (errmsg("IsRunning " XID_FMT "?", multi)));

    nmembers = GetMultiXactIdMembers(multi, &members);
    if (nmembers <= 0) {
        ereport(DEBUG2, (errmsg("IsRunning: no members")));
        return false;
    }

    /*
     * Checking for myself is cheap compared to looking in shared memory;
     * return true if any live subtransaction of the current top-level
     * transaction is a member.
     *
     * This is not needed for correctness, it's just a fast path.
     */
    for (i = 0; i < nmembers; i++) {
        if (TransactionIdIsCurrentTransactionId(members[i].xid)) {
            ereport(DEBUG2, (errmsg("IsRunning: I (%d) am running!", i)));
            pfree(members);
            members = NULL;
            return true;
        }
    }

    /*
     * This could be made faster by having another entry point in procarray.c,
     * walking the PGPROC array only once for all the members.	But in most
     * cases nmembers should be small enough that it doesn't much matter.
     */
    for (i = 0; i < nmembers; i++) {
        if (TransactionIdIsInProgress(members[i].xid)) {
            ereport(DEBUG2, (errmsg("IsRunning: member %d (" XID_FMT ") is running", i, members[i].xid)));
            pfree(members);
            members = NULL;
            return true;
        }
    }

    pfree(members);
    members = NULL;
    ereport(DEBUG2, (errmsg("IsRunning: " XID_FMT " is not running", multi)));

    return false;
}

/*
 * MultiXactIdIsCurrent
 *		Returns true if the current transaction is a member of the MultiXactId.
 *
 * We return true if any live subtransaction of the current top-level
 * transaction is a member.  This is appropriate for the same reason that a
 * lock held by any such subtransaction is globally equivalent to a lock
 * held by the current subtransaction: no such lock could be released without
 * aborting this subtransaction, and hence releasing its locks.  So it's not
 * necessary to add the current subxact to the MultiXact separately.
 */
bool MultiXactIdIsCurrent(MultiXactId multi)
{
    bool result = false;
    MultiXactMember *members = NULL;
    int nmembers;
    int i;

    nmembers = GetMultiXactIdMembers(multi, &members);
    if (nmembers < 0) {
        return false;
    }

    for (i = 0; i < nmembers; i++) {
        if (TransactionIdIsCurrentTransactionId(members[i].xid)) {
            result = true;
            break;
        }
    }

    pfree(members);
    members = NULL;
    return result;
}

/*
 * MultiXactIdSetOldestMember
 * 		Save the oldest MultiXactId this transaction could be a member of.
 *
 * We set the OldestMemberMXactId for a given transaction the first time it's
 * going to do some operation that might require a MultiXactId (tuple lock,
 * update or delete).  We need to do this even if we end up using a
 * TransactionId instead of a MultiXactId, because there is a chance that
 * another transaction would add our XID to a MultiXactId.
 *
 * The value to set is the next-to-be-assigned MultiXactId, so this is meant to
 * be called just before doing any such possibly-MultiXactId-able operation.
 */
void MultiXactIdSetOldestMember(void)
{
    if (!MultiXactIdIsValid(t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId])) {
        MultiXactId nextMXact;

        /*
         * You might think we don't need to acquire a lock here, since
         * fetching and storing of TransactionIds is probably atomic, but in
         * fact we do: suppose we pick up nextMXact and then lose the CPU for
         * a long time.  Someone else could advance nextMXact, and then
         * another someone else could compute an OldestVisibleMXactId that
         * would be after the value we are going to store when we get control
         * back.  Which would be wrong.
         *
         * Note that a shared lock is sufficient, because it's enough to stop
         * someone from advancing nextMXact; and nobody else could be trying to
         * write to our OldestMember entry, only reading (and we assume storing
         * it is atomic.)
         */
        (void)LWLockAcquire(MultiXactGenLock, LW_SHARED);

        /*
         * We have to beware of the possibility that nextMXact is in the
         * wrapped-around state.  We don't fix the counter itself here, but we
         * must be sure to store a valid value in our array entry.
         */
        nextMXact = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
        if (nextMXact < FirstMultiXactId)
            nextMXact = FirstMultiXactId;

        t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId] = nextMXact;

        LWLockRelease(MultiXactGenLock);

        ereport(DEBUG2, (errmsg("MultiXact: setting OldestMember[%d] = %lu", t_thrd.proc_cxt.MyBackendId, nextMXact)));
    }
}

/*
 * MultiXactIdSetOldestVisible
 *		Save the oldest MultiXactId this transaction considers possibly live.
 *
 * We set the OldestVisibleMXactId for a given transaction the first time
 * it's going to inspect any MultiXactId.  Once we have set this, we are
 * guaranteed that the checkpointer won't truncate off SLRU data for
 * MultiXactIds at or after our OldestVisibleMXactId.
 *
 * The value to set is the oldest of nextMXact and all the valid per-backend
 * OldestMemberMXactId[] entries.  Because of the locking we do, we can be
 * certain that no subsequent call to MultiXactIdSetOldestMember can set
 * an OldestMemberMXactId[] entry older than what we compute here.	Therefore
 * there is no live transaction, now or later, that can be a member of any
 * MultiXactId older than the OldestVisibleMXactId we compute here.
 */
static void MultiXactIdSetOldestVisible(void)
{
    if (!MultiXactIdIsValid(t_thrd.shemem_ptr_cxt.OldestVisibleMXactId[t_thrd.proc_cxt.MyBackendId])) {
        MultiXactId oldestMXact;
        int i;

        (void)LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);

        /*
         * We have to beware of the possibility that nextMXact is in the
         * wrapped-around state.  We don't fix the counter itself here, but we
         * must be sure to store a valid value in our array entry.
         */
        oldestMXact = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
        if (oldestMXact < FirstMultiXactId)
            oldestMXact = FirstMultiXactId;

        for (i = 1; i <= MaxOldestSlot; i++) {
            MultiXactId thisoldest = t_thrd.shemem_ptr_cxt.OldestMemberMXactId[i];

            if (MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldestMXact))
                oldestMXact = thisoldest;
        }

        t_thrd.shemem_ptr_cxt.OldestVisibleMXactId[t_thrd.proc_cxt.MyBackendId] = oldestMXact;

        LWLockRelease(MultiXactGenLock);

        ereport(DEBUG2,
                (errmsg("MultiXact: setting OldestVisible[%d] = %lu", t_thrd.proc_cxt.MyBackendId, oldestMXact)));
    }
}

/*
 * ReadNextMultiXactId
 *        Return the next MultiXactId to be assigned, but don't allocate it
 */
MultiXactId ReadNextMultiXactId(void)
{
    MultiXactId mxid;

    /* XXX we could presumably do this without a lock. */
    LWLockAcquire(MultiXactGenLock, LW_SHARED);
    mxid = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    LWLockRelease(MultiXactGenLock);

    if (mxid < FirstMultiXactId)
        mxid = FirstMultiXactId;

    return mxid;
}

/*
 * DoMultiXactIdWait
 *      Actual implementation for the two functions below.
 *
 * We do this by sleeping on each member using XactLockTableWait.  Any
 * members that belong to the current backend are *not* waited for, however;
 * this would not merely be useless but would lead to Assert failure inside
 * XactLockTableWait.  By the time this returns, it is certain that all
 * transactions *of other backends* that were members of the MultiXactId
 * that conflict with the requested status are dead (and no new ones can have
 * been added, since it is not legal to add members to an existing
 * MultiXactId).
 *
 * But by the time we finish sleeping, someone else may have changed the Xmax
 * of the containing tuple, so the caller needs to iterate on us somehow.
 *
 * Note that in case we return false, the number of remaining members is
 * not to be trusted.
 */
bool DoMultiXactIdWait(MultiXactId multi, MultiXactStatus status, int *remaining, bool nowait, int waitSec)
{
    bool result = true;
    MultiXactMember *members = NULL;
    int nmembers;
    int remain = 0;

    nmembers = GetMultiXactIdMembers(multi, &members);

    for (int i = 0; i < nmembers; i++) {
        TransactionId memxid = members[i].xid;
        MultiXactStatus memstatus = members[i].status;

        if (TransactionIdIsCurrentTransactionId(memxid)) {
            remain++;
            continue;
        }

        if (!DoLockModesConflict(LOCKMODE_FROM_MXSTATUS(memstatus), LOCKMODE_FROM_MXSTATUS(status))) {
            if (remaining && TransactionIdIsInProgress(memxid))
                remain++;
            continue;
        }

        /*
        * This member conflicts with our multi, so we have to sleep (or
        * return failure, if asked to avoid waiting.)
        */
        if (nowait) {
            result = ConditionalXactLockTableWait(memxid);
            if (!result) {
                break;
            }
        } else {
            XactLockTableWait(memxid, true, waitSec);
        }
    }

    pfree_ext(members);

    if (remaining)
        *remaining = remain;

    return result;
}

/*
 * MultiXactIdWait
 *      Sleep on a MultiXactId.
 *
 * By the time we finish sleeping, someone else may have changed the Xmax
 * of the containing tuple, so the caller needs to iterate on us somehow.
 *
 * We return (in *remaining, if not NULL) the number of members that are still
 * running, including any (non-aborted) subtransactions of our own transaction.
 */
void MultiXactIdWait(MultiXactId multi, MultiXactStatus status, int *remaining, int waitSec)
{
    DoMultiXactIdWait(multi, status, remaining, false, waitSec);
}

/*
 * ConditionalMultiXactIdWait
 * 		As above, but only lock if we can get the lock without blocking.
 *
 * By the time we finish sleeping, someone else may have changed the Xmax
 * of the containing tuple, so the caller needs to iterate on us somehow.
 *
 * If the multixact is now all gone, return true.  Returns false if some
 * transactions might still be running.
 *
 * We return (in *remaining, if not NULL) the number of members that are still
 * running, including any (non-aborted) subtransactions of our own transaction.
 */
bool ConditionalMultiXactIdWait(MultiXactId multi, MultiXactStatus status, int *remaining)
{
    return DoMultiXactIdWait(multi, status, remaining, true);
}

/*
 * CreateMultiXactId
 *		Make a new MultiXactId
 *
 * Make XLOG, SLRU and cache entries for a new MultiXactId, recording the
 * given TransactionIds as members.  Returns the newly created MultiXactId.
 *
 * NB: the passed members[] array will be sorted in-place.
 */
static MultiXactId CreateMultiXactId(int nmembers, MultiXactMember *members)
{
    MultiXactId multi;
    MultiXactOffset offset;
    TransactionId *xidsWithStatus;
    xl_multixact_create xlrec;

    if (t_thrd.proc->workingVersionNum < ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        for (int i = 0; i < nmembers; ++i) {
            if (members[i].status != MultiXactStatusForShare) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                    errmsg("New MultiXact feature isn't support in this version. Please upgrade to version: %d",
                        ENHANCED_TUPLE_LOCK_VERSION_NUM)));
            }
        }
    }

    debug_elog3(DEBUG2, "Create: %s", mxid_to_string(InvalidMultiXactId, nmembers, members));

    /*
     * See if the same set of members already exists in our cache; if so, just
     * re-use that MultiXactId.  (Note: it might seem that looking in our
     * cache is insufficient, and we ought to search disk to see if a
     * duplicate definition already exists.  But since we only ever create
     * MultiXacts containing our own XID, in most cases any such MultiXacts
     * were in fact created by us, and so will be in our cache.  There are
     * corner cases where someone else added us to a MultiXact without our
     * knowledge, but it's not worth checking for.)
     */
    multi = mXactCacheGetBySet(nmembers, members);
    if (MultiXactIdIsValid(multi)) {
        ereport(DEBUG2, (errmsg("Create: in cache!")));
        return multi;
    }

    /* Verify that there is a single update Xid among the given members. */
    {
        int i;
        bool has_update = false;

        for (i = 0; i < nmembers; i++) {
            if (ISUPDATE_from_mxstatus(members[i].status)) {
                if (has_update)
                    ereport(ERROR, (errmsg("new multixact has more than one updating member")));
                has_update = true;
            }
        }
    }

    xidsWithStatus = (TransactionId *)palloc((unsigned)nmembers * sizeof(TransactionId));

    /* High 3 bits record the status, low 60 bits record the xid */
    for (int i = 0; i < nmembers; ++i) {
        xidsWithStatus[i] = GET_SLRU_XID_FROM_MULTIXACT_MEMBER(members + i);
    }

    /*
     * Assign the MXID and offsets range to use, and make sure there is space
     * in the OFFSETs and MEMBERs files.  NB: this routine does START_CRIT_SECTION().
     */
    multi = GetNewMultiXactId(nmembers, &offset);

    /*
     * Make an XLOG entry describing the new MXID.
     *
     * Note: we need not flush this XLOG entry to disk before proceeding. The
     * only way for the MXID to be referenced from any data page is for
     * heap_lock_tuple() to have put it there, and heap_lock_tuple() generates
     * an XLOG record that must follow ours.  The normal LSN interlock between
     * the data page and that XLOG record will ensure that our XLOG record
     * reaches disk first.	If the SLRU members/offsets data reaches disk
     * sooner than the XLOG record, we do not care because we'll overwrite it
     * with zeroes unless the XLOG record is there too; see notes at top of
     * this file.
     */
    xlrec.mid = multi;
    xlrec.moff = offset;
    xlrec.nxids = nmembers;

    XLogBeginInsert();
    XLogRegisterData((char *)(&xlrec), MinSizeOfMultiXactCreate);
    XLogRegisterData((char *)xidsWithStatus, (unsigned)nmembers * sizeof(TransactionId));

    (void)XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID);

    /* Now enter the information into the OFFSETs and MEMBERs logs */
    RecordNewMultiXact(multi, offset, nmembers, xidsWithStatus);

    /* Done with critical section */
    END_CRIT_SECTION();

    /* Store the new MultiXactId in the local cache, too */
    mXactCachePut(multi, nmembers, members);

    pfree(xidsWithStatus);

    ereport(DEBUG2, (errmsg("Create: all done")));

    return multi;
}

/*
 * RecordNewMultiXact
 *		Write info about a new multixact into the offsets and members files
 *
 * This is broken out of CreateMultiXactId so that xlog replay can use it.
 */
static void RecordNewMultiXact(MultiXactId multi, MultiXactOffset offset, int nmembers, TransactionId *xidsWithStatus)
{
    int64 pageno;
    int64 prev_pageno;
    int entryno;
    int slotno;
    MultiXactOffset *offptr = NULL;
    int i;

    (void)LWLockAcquire(MultiXactOffsetControlLock, LW_EXCLUSIVE);

    pageno = (int64)MultiXactIdToOffsetPage(multi);
    entryno = MultiXactIdToOffsetEntry(multi);

    /*
     * Note: we pass the MultiXactId to SimpleLruReadPage as the "transaction"
     * to complain about if there's any I/O error.  This is kinda bogus, but
     * since the errors will always give the full pathname, it should be clear
     * enough that a MultiXactId is really involved.  Perhaps someday we'll
     * take the trouble to generalize the slru.c error reporting code.
     */
    slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, pageno, true, multi);
    offptr = (MultiXactOffset *)t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_buffer[slotno];
    offptr += entryno;

    *offptr = offset;

    t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_dirty[slotno] = true;

    /* Exchange our lock */
    LWLockRelease(MultiXactOffsetControlLock);

    (void)LWLockAcquire(MultiXactMemberControlLock, LW_EXCLUSIVE);

    prev_pageno = -1;

    for (i = 0; i < nmembers; i++, offset++) {
        TransactionId *memberptr = NULL;

        pageno = (int64)MXOffsetToMemberPage(offset);
        entryno = MXOffsetToMemberEntry(offset);

        if (pageno != prev_pageno) {
            slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, pageno, true, multi);
            prev_pageno = pageno;
        }

        memberptr = (TransactionId *)t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_buffer[slotno];
        memberptr += entryno;

        /* High 3 bits record the status, low 60 bits record the xid */
        *memberptr = xidsWithStatus[i];

        t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_dirty[slotno] = true;
    }

    LWLockRelease(MultiXactMemberControlLock);
}

/*
 * GetNewMultiXactId
 *		Get the next MultiXactId.
 *
 * Also, reserve the needed amount of space in the "members" area.	The
 * starting offset of the reserved space is returned in *offset.
 *
 * This may generate XLOG records for expansion of the offsets and/or members
 * files.  Unfortunately, we have to do that while holding MultiXactGenLock
 * to avoid race conditions --- the XLOG record for zeroing a page must appear
 * before any backend can possibly try to store data in that page!
 *
 * We start a critical section before advancing the shared counters.  The
 * caller must end the critical section after writing SLRU data.
 */
static MultiXactId GetNewMultiXactId(int nmembers, MultiXactOffset *offset)
{
    MultiXactId result;
    MultiXactOffset nextOffset;

    ereport(DEBUG2, (errmsg("GetNew: for %d xids", nmembers)));

    /* MultiXactIdSetOldestMember() must have been called already */
    Assert(MultiXactIdIsValid(t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId]));

    (void)LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);

    /* Handle nextMXact first init value is 0 */
    if (t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact < FirstMultiXactId)
        t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact = FirstMultiXactId;

    /*
     * Assign the MXID, and make sure there is room for it in the file.
     */
    result = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;

    ExtendMultiXactOffset(result);

    /*
     * Reserve the members space, similarly to above.  Also, be careful not to
     * return zero as the starting offset for any multixact. See
     * GetMultiXactIdMembers() for motivation.
     */
    nextOffset = t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset;
    if (nextOffset == 0) {
        *offset = 1;
        nmembers++; /* allocate member slot 0 too */
    } else
        *offset = nextOffset;

    ExtendMultiXactMember(nextOffset, nmembers);

    /*
     * Critical section from here until caller has written the data into the
     * just-reserved SLRU space; we don't want to error out with a partly
     * written MultiXact structure.  (In particular, failing to write our
     * start offset after advancing nextMXact would effectively corrupt the
     * previous MultiXact.)
     */
    START_CRIT_SECTION();

    /*
     * Advance counters.  As in GetNewTransactionId(), this must not happen
     * until after file extension has succeeded!
     *
     * Note that nextMXact may be InvalidMultiXactId after this routine exits,
     * so anyone else looking at the variable must be prepared to deal with that.
     * Similarly, nextOffset may be zero, but we won't use that as the
     * actual start offset of the next multixact.
     */
    (t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact)++;

    t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset += (unsigned)nmembers;

    LWLockRelease(MultiXactGenLock);

    ereport(DEBUG2, (errmsg("GetNew: returning %lu offset %lu", result, *offset)));
    return result;
}

/*
 * GetMultiXactIdMembers
 *      Returns the set of MultiXactMembers that make up a MultiXactId
 *
 * If the given MultiXactId is older than the value we know to be oldest, we
 * return -1.
 *
 * Other border conditions, such as trying to read a value that's larger than
 * the value currently known as the next to assign, raise an error.  Previously
 * these also returned -1, but since this can lead to the wrong visibility
 * results, it is dangerous to do that.
 */
int GetMultiXactIdMembers(MultiXactId multi, MultiXactMember **members)
{
    int64 pageno;
    int64 prev_pageno;
    int entryno;
    int slotno;
    MultiXactOffset *offptr = NULL;
    MultiXactOffset offset;
    int length;
    int truelength;
    int i;
    MultiXactId nextMXact;
    MultiXactId tmpMXact;
    MultiXactOffset nextOffset;
    MultiXactMember *ptr = NULL;
    MultiXactId oldestMXact;

    ereport(DEBUG2, (errmsg("GetMembers: asked for " XID_FMT, multi)));

    Assert(MultiXactIdIsValid(multi));

    /* See if the MultiXactId is in the local cache */
    length = mXactCacheGetById(multi, members);
    if (length >= 0) {
        debug_elog3(DEBUG2, "GetMembers: found %s in the cache", mxid_to_string(multi, length, *members));
        return length;
    }

    /* Set our OldestVisibleMXactId[] entry if we didn't already */
    MultiXactIdSetOldestVisible();

    /*
     * We check known limits on MultiXact before resorting to the SLRU area.
     *
     * An ID >= nextMXact shouldn't ever be seen here;
     *
     * Shared lock is enough here since we aren't modifying any global state.
     * Acquire it just long enough to grab the current counter values.	We may
     * need both nextMXact and nextOffset; see below.
     */
    (void)LWLockAcquire(MultiXactGenLock, LW_SHARED);

    oldestMXact = t_thrd.shemem_ptr_cxt.MultiXactState->oldestMultiXactId;
    nextMXact = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    nextOffset = t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset;

    LWLockRelease(MultiXactGenLock);

    if (MultiXactIdPrecedes(multi, oldestMXact)) {
        ereport(DEBUG2, (errmsg("MultiXactId %lu does no longer exist -- apparent wraparound", multi)));
        *members = NULL;
        return -1;
    }

    if (!MultiXactIdPrecedes(multi, nextMXact)) {
        ereport(DEBUG2, (errmsg("MultiXactId %lu has not been created yet -- apparent wraparound", multi)));
        *members = NULL;
        return -1;
    }

    /*
     * Find out the offset at which we need to start reading MultiXactMembers
     * and the number of members in the multixact.	We determine the latter as
     * the difference between this multixact's starting offset and the next
     * one's.  However, there are some corner cases to worry about:
     *
     * 1. This multixact may be the latest one created, in which case there is
     * no next one to look at.	In this case the nextOffset value we just
     * saved is the correct endpoint.
     *
     * 2. The next multixact may still be in process of being filled in: that
     * is, another process may have done GetNewMultiXactId but not yet written
     * the offset entry for that ID.  In that scenario, it is guaranteed that
     * the offset entry for that multixact exists (because GetNewMultiXactId
     * won't release MultiXactGenLock until it does) but contains zero
     * (because we are careful to pre-zero offset pages). Because
     * GetNewMultiXactId will never return zero as the starting offset for a
     * multixact, when we read zero as the next multixact's offset, we know we
     * have this case.	We sleep for a bit and try again.
     *
     * 3. Because GetNewMultiXactId increments offset zero to offset one to
     * handle case #2, there is an ambiguity near the point of offset
     * wraparound. If we see next multixact's offset is one, is that our multixact's actual
     * endpoint, or did it end at zero with a subsequent increment? We
     * handle this using the knowledge that if the zero'th member slot wasn't
     * filled, it'll contain zero, and zero isn't a valid transaction ID so it can't
     * be a multixact member.  Therefore, if we read a zero from the
     * members array, just ignore it.
     *
     * This is all pretty messy, but the mess occurs only in infrequent corner
     * cases, so it seems better than holding the MultiXactGenLock for a long
     * time on every multixact creation.
     */
retry:
    (void)LWLockAcquire(MultiXactOffsetControlLock, LW_EXCLUSIVE);

    pageno = MultiXactIdToOffsetPage(multi);
    entryno = MultiXactIdToOffsetEntry(multi);

    slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, pageno, true, multi);
    offptr = (MultiXactOffset *)t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_buffer[slotno];
    offptr += entryno;
    offset = *offptr;

    Assert(offset != 0);

    /* Use the same increment rule as GetNewMultiXactId() */
    tmpMXact = multi + 1;

    if (nextMXact == tmpMXact) {
        /* Corner case 1: there is no next multixact */
        length = nextOffset - offset;
    } else {
        MultiXactOffset nextMXOffset;

        prev_pageno = pageno;

        pageno = (int64)MultiXactIdToOffsetPage(tmpMXact);
        entryno = MultiXactIdToOffsetEntry(tmpMXact);

        if (pageno != prev_pageno)
            slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, pageno, true, tmpMXact);

        offptr = (MultiXactOffset *)t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_buffer[slotno];
        offptr += entryno;
        nextMXOffset = *offptr;

        if (nextMXOffset == 0) {
            /* Corner case 2: next multixact is still being filled in */
            LWLockRelease(MultiXactOffsetControlLock);
            pg_usleep(1000L);
            goto retry;
        }

        length = nextMXOffset - offset;
    }

    LWLockRelease(MultiXactOffsetControlLock);

    ptr = (MultiXactMember *)palloc((unsigned)length * sizeof(MultiXactMember));
    *members = ptr;

    /* Now get the members themselves. */
    (void)LWLockAcquire(MultiXactMemberControlLock, LW_EXCLUSIVE);

    truelength = 0;
    prev_pageno = -1;
    for (i = 0; i < length; i++, offset++) {
        TransactionId *xactptr = NULL;
        TransactionId memberXid;

        pageno = (int64)MXOffsetToMemberPage(offset);
        entryno = MXOffsetToMemberEntry(offset);

        if (pageno != prev_pageno) {
            slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, pageno, true, multi);
            prev_pageno = pageno;
        }

        xactptr = (TransactionId *)t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_buffer[slotno];
        xactptr += entryno;

        memberXid = GET_MEMBER_XID_FROM_SLRU_XID(*xactptr);
        if (!TransactionIdIsValid(memberXid)) {
            /* Corner case 3: we must be looking at unused slot zero */
            Assert(offset == 0);
            continue;
        }

        ptr[truelength].xid = memberXid;
        ptr[truelength].status = GET_MEMBER_STATUS_FROM_SLRU_XID(*xactptr);
        ++truelength;
    }

    LWLockRelease(MultiXactMemberControlLock);

    /*
     * Copy the result into the local cache.
     */
    mXactCachePut(multi, truelength, ptr);

    debug_elog3(DEBUG2, "GetMembers: no cache for %s", mxid_to_string(multi, truelength, ptr));
    return truelength;
}

/*
 * MXactMemberComparator
 *      qsort comparison function for MultiXactMember
 */
static int MXactMemberComparator(const void *arg1, const void *arg2)
{
    MultiXactMember member1 = *(const MultiXactMember *)arg1;
    MultiXactMember member2 = *(const MultiXactMember *)arg2;

    if (member1.xid > member2.xid) {
        return 1;
    }
    if (member1.xid < member2.xid) {
        return -1;
    }

    /*
     * Because of compatibility, we set MultiXactStatusForShare = 0x00.
     * But for status strength, MultiXactStatusForKeyShare = 0x01 is
     * weaker thean MultiXactStatusForShare, so we exchange these two
     * value.
     */
    int status1 = (member1.status == MultiXactStatusForShare) ? 1 :
        ((member1.status == MultiXactStatusForKeyShare) ? 0 : (int)(member1.status));
    int status2 = (member2.status == MultiXactStatusForShare) ? 1 :
        ((member2.status == MultiXactStatusForKeyShare) ? 0 : (int)(member2.status));
    if (status1 > status2) {
        return 1;
    }
    if (status1 < status2) {
        return -1;
    }
    return 0;
}

/*
 * mXactCacheGetBySet
 *		returns a MultiXactId from the cache based on the set of
 *		TransactionIds that compose it, or InvalidMultiXactId if
 *		none matches.
 *
 * This is helpful, for example, if two transactions want to lock a huge
 * table.  By using the cache, the second will use the same MultiXactId
 * for the majority of tuples, thus keeping MultiXactId usage low (saving
 * both I/O).
 *
 * NB: the passed members[] array will be sorted in-place.
 */
static MultiXactId mXactCacheGetBySet(int nmembers, MultiXactMember *members)
{
    mXactCacheEnt *entry = NULL;

    debug_elog3(DEBUG2, "CacheGet: looking for %s", mxid_to_string(InvalidMultiXactId, nmembers, members));

    /* sort the array so comparison is easy */
    qsort(members, nmembers, sizeof(MultiXactMember), MXactMemberComparator);

    for (entry = t_thrd.xact_cxt.MXactCache; entry != NULL; entry = entry->next) {
        if (entry->nmembers != nmembers)
            continue;

        /* We assume the cache entries are sorted */
        if (memcmp(members, entry->members, (unsigned)nmembers * sizeof(MultiXactMember)) == 0) {
            ereport(DEBUG2, (errmsg("CacheGet: found " XID_FMT, entry->multi)));
            return entry->multi;
        }
    }

    ereport(DEBUG2, (errmsg("CacheGet: not found :-(")));
    return InvalidMultiXactId;
}

/*
 * mXactCacheGetById
 *		returns the composing MultiXactMember set from the cache for a
 *		given MultiXactId, if present.
 *
 * If successful, *members is set to the address of a palloc'd copy of the
 * MultiXactMember set.  Return value is number of members, or -1 on failure.
 */
static int mXactCacheGetById(MultiXactId multi, MultiXactMember **members)
{
    mXactCacheEnt *entry = NULL;
    errno_t rc = EOK;

    ereport(DEBUG2, (errmsg("CacheGet: looking for " XID_FMT, multi)));

    for (entry = t_thrd.xact_cxt.MXactCache; entry != NULL; entry = entry->next) {
        if (entry->multi == multi) {
            MultiXactMember *ptr = NULL;
            Size size;

            size = sizeof(MultiXactMember) * (unsigned)entry->nmembers;
            ptr = (MultiXactMember *)palloc(size);
            *members = ptr;

            rc = memcpy_s(ptr, size, entry->members, size);
            securec_check(rc, "", "");

            debug_elog3(DEBUG2, "CacheGet: found %s", mxid_to_string(multi, entry->nmembers, entry->members));
            return entry->nmembers;
        }
    }

    ereport(DEBUG2, (errmsg("CacheGet: not found")));
    return -1;
}

/*
 * mXactCachePut
 *		Add a new MultiXactId and its composing set into the local cache.
 */
static void mXactCachePut(MultiXactId multi, int nmembers, MultiXactMember *members)
{
    mXactCacheEnt *entry = NULL;
    errno_t rc = EOK;

    debug_elog3(DEBUG2, "CachePut: storing %s", mxid_to_string(multi, nmembers, members));

    if (t_thrd.xact_cxt.MXactContext == NULL) {
        /* The cache only lives as long as the current transaction */
        ereport(DEBUG2, (errmsg("CachePut: initializing memory context")));
        t_thrd.xact_cxt.MXactContext = AllocSetContextCreate(u_sess->top_transaction_mem_cxt, "MultiXact Cache Context",
                                                             ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE,
                                                             ALLOCSET_SMALL_MAXSIZE);
    }

    entry = (mXactCacheEnt *)MemoryContextAlloc(t_thrd.xact_cxt.MXactContext,
        offsetof(mXactCacheEnt, members) + (unsigned)nmembers * sizeof(MultiXactMember));

    entry->multi = multi;
    entry->nmembers = nmembers;
    rc = memcpy_s(entry->members, (unsigned)nmembers * sizeof(MultiXactMember), members,
        (unsigned)nmembers * sizeof(MultiXactMember));
    securec_check(rc, "", "");

    /* mXactCacheGetBySet assumes the entries are sorted, so sort them */
    qsort(entry->members, nmembers, sizeof(MultiXactMember), MXactMemberComparator);

    entry->next = t_thrd.xact_cxt.MXactCache;
    t_thrd.xact_cxt.MXactCache = entry;
}

static const char *MXStatusToString(MultiXactStatus status)
{
    switch (status) {
        case MultiXactStatusForKeyShare:
            return "keysh";
        case MultiXactStatusForShare:
            return "sh";
        case MultiXactStatusForNoKeyUpdate:
            return "fornokeyupd";
        case MultiXactStatusForUpdate:
            return "forupd";
        case MultiXactStatusNoKeyUpdate:
            return "nokeyupd";
        case MultiXactStatusUpdate:
            return "upd";
        default:
            elog(ERROR, "unrecognized multixact status %d", (int)status);
            return "";
    }
}

static char *mxid_to_string(MultiXactId multi, int nmembers, MultiXactMember *members)
{
    char *str = NULL;
    StringInfoData buf;
    int i;

    initStringInfo(&buf);

    appendStringInfo(&buf, XID_FMT " %d[" XID_FMT " (%s)", multi, nmembers,
        members[0].xid, MXStatusToString(members[0].status));

    for (i = 1; i < nmembers; i++) {
        appendStringInfo(&buf, ", " XID_FMT " (%s)", members[i].xid, MXStatusToString(members[i].status));
    }

    appendStringInfoChar(&buf, ']');
    str = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), buf.data);
    pfree(buf.data);
    return str;
}

/*
 * AtEOXact_MultiXact
 *		Handle transaction end for MultiXact
 *
 * This is called at top transaction commit or abort (we don't care which).
 */
void AtEOXact_MultiXact(void)
{
    /*
     * Reset our OldestMemberMXactId and OldestVisibleMXactId values, both of
     * which should only be valid while within a transaction.
     *
     * We assume that storing a MultiXactId is atomic and so we need not take
     * MultiXactGenLock to do this.
     */
    t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId] = InvalidMultiXactId;
    t_thrd.shemem_ptr_cxt.OldestVisibleMXactId[t_thrd.proc_cxt.MyBackendId] = InvalidMultiXactId;

    /*
     * Discard the local MultiXactId cache.  Since MXactContext was created as
     * a child of u_sess->top_transaction_mem_cxt, we needn't delete it explicitly.
     */
    t_thrd.xact_cxt.MXactContext = NULL;
    t_thrd.xact_cxt.MXactCache = NULL;
}

/*
 * AtPrepare_MultiXact
 *		Save multixact state at 2PC transaction prepare
 *
 * In this phase, we only store our OldestMemberMXactId value in the two-phase
 * state file.
 */
void AtPrepare_MultiXact(void)
{
    MultiXactId myOldestMember = t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId];

    if (MultiXactIdIsValid(myOldestMember))
        RegisterTwoPhaseRecord(TWOPHASE_RM_MULTIXACT_ID, 0, &myOldestMember, sizeof(MultiXactId));
}

/*
 * PostPrepare_MultiXact
 *		Clean up after successful PREPARE TRANSACTION
 */
void PostPrepare_MultiXact(TransactionId xid)
{
    MultiXactId myOldestMember;

    /*
     * Transfer our OldestMemberMXactId value to the slot reserved for the
     * prepared transaction.
     */
    myOldestMember = t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId];
    if (MultiXactIdIsValid(myOldestMember)) {
        BackendId dummyBackendId = TwoPhaseGetDummyBackendId(xid);

        /*
         * Even though storing MultiXactId is atomic, acquire lock to make
         * sure others see both changes, not just the reset of the slot of the
         * current backend. Using a volatile pointer might suffice, but this
         * isn't a hot spot.
         */
        (void)LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);

        t_thrd.shemem_ptr_cxt.OldestMemberMXactId[dummyBackendId] = myOldestMember;
        t_thrd.shemem_ptr_cxt.OldestMemberMXactId[t_thrd.proc_cxt.MyBackendId] = InvalidMultiXactId;

        LWLockRelease(MultiXactGenLock);
    }

    /*
     * We don't need to transfer OldestVisibleMXactId value, because the
     * transaction is not going to be looking at any more multixacts once it's
     * prepared.
     *
     * We assume that storing a MultiXactId is atomic and so we need not take
     * MultiXactGenLock to do this.
     */
    t_thrd.shemem_ptr_cxt.OldestVisibleMXactId[t_thrd.proc_cxt.MyBackendId] = InvalidMultiXactId;

    /*
     * Discard the local MultiXactId cache like in AtEOX_MultiXact
     */
    t_thrd.xact_cxt.MXactContext = NULL;
    t_thrd.xact_cxt.MXactCache = NULL;
}

/*
 * multixact_twophase_recover
 *		Recover the state of a prepared transaction at startup
 */
void multixact_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
    BackendId dummyBackendId = TwoPhaseGetDummyBackendId(xid);
    MultiXactId oldestMember;

    /*
     * Get the oldest member XID from the state file record, and set it in the
     * OldestMemberMXactId slot reserved for this prepared transaction.
     */
    Assert(len == sizeof(MultiXactId));
    oldestMember = *((MultiXactId *)recdata);

    t_thrd.shemem_ptr_cxt.OldestMemberMXactId[dummyBackendId] = oldestMember;
}

/*
 * multixact_twophase_postcommit
 *		Similar to AtEOX_MultiXact but for COMMIT PREPARED
 */
void multixact_twophase_postcommit(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
    BackendId dummyBackendId = TwoPhaseGetDummyBackendId(xid);

    Assert(len == sizeof(MultiXactId));

    t_thrd.shemem_ptr_cxt.OldestMemberMXactId[dummyBackendId] = InvalidMultiXactId;
}

/*
 * multixact_twophase_postabort
 * 		This is actually just the same as the COMMIT case.
 */
void multixact_twophase_postabort(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
    multixact_twophase_postcommit(xid, info, recdata, len);
}

/*
 * Initialization of shared memory for MultiXact.  We use two SLRU areas,
 * thus double memory.	Also, reserve space for the shared MultiXactState
 * struct and the per-backend MultiXactId arrays (two of those, too).
 */
Size MultiXactShmemSize(void)
{
    Size size;

#define SHARED_MULTIXACT_STATE_SIZE \
    add_size(sizeof(MultiXactStateData), mul_size(sizeof(MultiXactId) * 2, MaxOldestSlot))

    size = SHARED_MULTIXACT_STATE_SIZE;
    size = add_size(size, SimpleLruShmemSize(NUM_MXACTOFFSET_BUFFERS, 0));
    size = add_size(size, SimpleLruShmemSize(NUM_MXACTMEMBER_BUFFERS, 0));

    return size;
}

void MultiXactShmemInit(void)
{
    bool found = false;
    errno_t rc = EOK;

    debug_elog2(DEBUG2, "Shared Memory Init for MultiXact");

    SimpleLruInit(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, GetBuiltInTrancheName(LWTRANCHE_MULTIXACTOFFSET_CTL),
                  LWTRANCHE_MULTIXACTOFFSET_CTL, NUM_MXACTOFFSET_BUFFERS, 0, MultiXactOffsetControlLock,
                  "pg_multixact/offsets");
    SimpleLruInit(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, GetBuiltInTrancheName(LWTRANCHE_MULTIXACTMEMBER_CTL),
                  LWTRANCHE_MULTIXACTMEMBER_CTL, NUM_MXACTMEMBER_BUFFERS, 0, MultiXactMemberControlLock,
                  "pg_multixact/members");

    /* Initialize our shared state struct */
    t_thrd.shemem_ptr_cxt.MultiXactState = (MultiXactStateData *)ShmemInitStruct("Shared MultiXact State",
                                                                                 SHARED_MULTIXACT_STATE_SIZE, &found);
    if (!IsUnderPostmaster) {
        Assert(!found);

        /* Make sure we zero out the per-backend state */
        rc = memset_s(t_thrd.shemem_ptr_cxt.MultiXactState, SHARED_MULTIXACT_STATE_SIZE, 0,
                      SHARED_MULTIXACT_STATE_SIZE);
        securec_check(rc, "", "");
    } else
        Assert(found);

    /*
     * Set up array pointers.  Note that perBackendXactIds[0] is wasted space
     * since we only use indexes 1..MaxOldestSlot in each array.
     */
    t_thrd.shemem_ptr_cxt.OldestMemberMXactId = t_thrd.shemem_ptr_cxt.MultiXactState->perBackendXactIds;
    t_thrd.shemem_ptr_cxt.OldestVisibleMXactId = t_thrd.shemem_ptr_cxt.OldestMemberMXactId + MaxOldestSlot;
}

/*
 * This func must be called ONCE on system install.  It creates the initial
 * MultiXact segments.	(The MultiXacts directories are assumed to have been
 * created by initdb, and MultiXactShmemInit must have been called already.)
 */
void BootStrapMultiXact(void)
{
    int slotno;

    (void)LWLockAcquire(MultiXactOffsetControlLock, LW_EXCLUSIVE);

    /* Create and zero the first page of the offsets log */
    slotno = ZeroMultiXactOffsetPage(0, false);

    /* Make sure it's written out */
    SimpleLruWritePage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, slotno);
    Assert(!t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_dirty[slotno]);

    LWLockRelease(MultiXactOffsetControlLock);

    (void)LWLockAcquire(MultiXactMemberControlLock, LW_EXCLUSIVE);

    /* Create and zero the first page of the members log */
    slotno = ZeroMultiXactMemberPage(0, false);

    /* Make sure it's written out */
    SimpleLruWritePage(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, slotno);
    Assert(!t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_dirty[slotno]);

    LWLockRelease(MultiXactMemberControlLock);
}

/*
 * Initialize (or reinitialize) a page of MultiXactOffset to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int ZeroMultiXactOffsetPage(int64 pageno, bool writeXlog)
{
    int slotno;

    slotno = SimpleLruZeroPage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, pageno);

    if (writeXlog)
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_OFF_PAGE);

    return slotno;
}

/*
 * Ditto, for MultiXactMember
 */
static int ZeroMultiXactMemberPage(int64 pageno, bool writeXlog)
{
    int slotno;
    bool bPZeroPage = true;
    slotno = SimpleLruZeroPage(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, pageno, &bPZeroPage);

    if (writeXlog && bPZeroPage)
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_MEM_PAGE);

    return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup.
 *
 * StartupXLOG has already established nextMXact/nextOffset by calling
 * MultiXactSetNextMXact and/or MultiXactAdvanceNextMXact.	Note that we
 * may already have replayed WAL data into the SLRU files.
 *
 * We don't need any locks here, really; the SLRU locks are taken
 * only because slru.c expects to be called with locks held.
 */
void StartupMultiXact(void)
{
    MultiXactId multi = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    MultiXactOffset offset = t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset;
    errno_t rc = EOK;
    int64 pageno;
    int64 entryno;

    /* Clean up offsets state */
    (void)LWLockAcquire(MultiXactOffsetControlLock, LW_EXCLUSIVE);

    /*
     * Initialize our idea of the latest page number.
     */
    pageno = (int64)MultiXactIdToOffsetPage(multi);
    t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->latest_page_number = pageno;

    /*
     * Zero out the remainder of the current offsets page.	See notes in
     * StartupCLOG() for motivation.
     */
    entryno = (int64)MultiXactIdToOffsetEntry(multi);
    if (entryno != 0) {
        int slotno;
        MultiXactOffset *offptr = NULL;

        slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, pageno, true, multi);
        offptr = (MultiXactOffset *)t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_buffer[slotno];
        offptr += entryno;

        rc = memset_s(offptr, BLCKSZ - ((unsigned)entryno * sizeof(MultiXactOffset)), 0,
                      BLCKSZ - ((unsigned)entryno * sizeof(MultiXactOffset)));
        securec_check(rc, "", "");

        t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_dirty[slotno] = true;
    }

    LWLockRelease(MultiXactOffsetControlLock);

    /* And the same for members */
    (void)LWLockAcquire(MultiXactMemberControlLock, LW_EXCLUSIVE);

    /*
     * Initialize our idea of the latest page number.
     */
    pageno = (int64)MXOffsetToMemberPage(offset);
    t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->latest_page_number = pageno;

    /*
     * Zero out the remainder of the current members page.	See notes in
     * TrimCLOG() for motivation.
     */
    entryno = (int64)MXOffsetToMemberEntry(offset);
    if (entryno != 0) {
        int slotno;
        TransactionId *xidptr = NULL;

        slotno = SimpleLruReadPage(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, pageno, true, offset);
        xidptr = (TransactionId *)t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_buffer[slotno];
        xidptr += entryno;

        rc = memset_s(xidptr, BLCKSZ - ((unsigned)entryno * sizeof(TransactionId)), 0,
                      BLCKSZ - ((unsigned)entryno * sizeof(TransactionId)));
        securec_check(rc, "", "");

        t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_dirty[slotno] = true;
    }

    LWLockRelease(MultiXactMemberControlLock);

    /*
     * Initialize lastTruncationPoint to invalid, ensuring that the first
     * checkpoint will try to do truncation.
     */
    t_thrd.shemem_ptr_cxt.MultiXactState->lastTruncationPoint = InvalidMultiXactId;
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void ShutdownMultiXact(void)
{
    /* Flush dirty MultiXact pages to disk */
    TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_START(false);
    (void)SimpleLruFlush(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, false);
    (void)SimpleLruFlush(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, false);
    TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_DONE(false);
}

/*
 * Get the next MultiXactId and offset to save in a checkpoint record
 */
void MultiXactGetCheckptMulti(bool is_shutdown, MultiXactId *nextMulti, MultiXactOffset *nextMultiOffset)
{
    (void)LWLockAcquire(MultiXactGenLock, LW_SHARED);

    *nextMulti = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    *nextMultiOffset = t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset;

    LWLockRelease(MultiXactGenLock);

    ereport(DEBUG2, (errmsg("MultiXact: checkpoint is nextMulti %lu, nextOffset %lu", *nextMulti, *nextMultiOffset)));
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void CheckPointMultiXact(void)
{
    TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_START(true);
    int flush_num;
    /* Flush dirty MultiXact pages to disk */
    flush_num = SimpleLruFlush(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, true);
    g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_multixact_flush_num += flush_num;
    flush_num = SimpleLruFlush(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, true);
    g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_multixact_flush_num += flush_num;

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * Truncate the SLRU files.  This could be done at any time, but
     * checkpoint seems a reasonable place for it.	There is one exception: if
     * we are called during xlog recovery, then shared->latest_page_number
     * isn't valid (because StartupMultiXact hasn't been called yet) and so
     * SimpleLruTruncate would get confused.  It seems best not to risk
     * removing any data during recovery anyway, so don't truncate.
     */
    if (!RecoveryInProgress())
        TruncateMultiXact();
#endif

    TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_DONE(true);
}

/*
 * Set the next-to-be-assigned MultiXactId and offset
 *
 * This is used when we can determine the correct next ID/offset exactly
 * from a checkpoint record.  Although this is only called during bootstrap
 * and XLog replay, we take the lock in case any hot-standby backends are
 * examining the values.
 */
void MultiXactSetNextMXact(MultiXactId nextMulti, MultiXactOffset nextMultiOffset)
{
    debug_elog4(DEBUG2, "MultiXact: setting next multi to %u offset %u", nextMulti, nextMultiOffset);
    (void)LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
    t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact = nextMulti;
    t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset = nextMultiOffset;
    LWLockRelease(MultiXactGenLock);
}

/*
 * Determine the last safe MultiXactId to allocate given the currently oldest
 * datminmxid (ie, the oldest MultiXactId that might exist in any database
 * of our cluster), and the OID of the (or a) database with that value.
 */
void SetMultiXactIdLimit(MultiXactId oldest_datminmxid, Oid oldest_datoid)
{
    MultiXactId multiVacLimit;
    MultiXactId curMulti;

    Assert(MultiXactIdIsValid(oldest_datminmxid));

    /*
     * We'll start trying to force autovacuums when oldest_datminmxid gets
     * to be more than autovacuum_freeze_max_age mxids old.
     *
     * It's a bit ugly to just reuse limits for xids that way, but it doesn't
     * seem worth adding separate GUCs for that purpose.
     */
    multiVacLimit = oldest_datminmxid + g_instance.attr.attr_storage.autovacuum_freeze_max_age;
    if (multiVacLimit < FirstMultiXactId)
        multiVacLimit += FirstMultiXactId;

    /* Grab lock for just long enough to set the new limit values */
    LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
    t_thrd.shemem_ptr_cxt.MultiXactState->oldestMultiXactId = oldest_datminmxid;
    t_thrd.shemem_ptr_cxt.MultiXactState->oldestMultiXactDB = oldest_datoid;
    t_thrd.shemem_ptr_cxt.MultiXactState->multiVacLimit = multiVacLimit;
    curMulti = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    LWLockRelease(MultiXactGenLock);

    /*
     * If past the autovacuum force point, immediately signal an autovac
     * request.  The reason for this is that autovac only processes one
     * database per invocation.  Once it's finished cleaning up the oldest
     * database, it'll call here, and we'll signal the postmaster to start
     * another iteration immediately if there are still any old databases.
     */
    if (MultiXactIdPrecedes(multiVacLimit, curMulti) && IsUnderPostmaster && !t_thrd.xlog_cxt.InRecovery)
        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
}

/*
 * Ensure the next-to-be-assigned MultiXactId is at least minMulti,
 * and similarly nextOffset is at least minMultiOffset.
 *
 * This is used when we can determine minimum safe values from an XLog
 * record (either an on-line checkpoint or an mxact creation log entry).
 * Although this is only called during XLog replay, we take the lock in case
 * any hot-standby backends are examining the values.
 */
void MultiXactAdvanceNextMXact(MultiXactId minMulti, MultiXactOffset minMultiOffset)
{
    (void)LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
    if (MultiXactIdPrecedes(t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact, minMulti)) {
        ereport(DEBUG2, (errmsg("MultiXact: setting next multi to %lu", minMulti)));
        t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact = minMulti;
    }
    if (t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset < minMultiOffset) {
        ereport(DEBUG2, (errmsg("MultiXact: setting next offset to %lu", minMultiOffset)));
        t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset = minMultiOffset;
    }
    LWLockRelease(MultiXactGenLock);
}

/*
 * Update our oldestMultiXactId value, but only if it's more recent than
 * what we had.
 */
void MultiXactAdvanceOldest(MultiXactId oldestMulti, Oid oldestMultiDB)
{
    if (MultiXactIdPrecedes(t_thrd.shemem_ptr_cxt.MultiXactState->oldestMultiXactId, oldestMulti))
        SetMultiXactIdLimit(oldestMulti, oldestMultiDB);
}

/*
 * Make sure that MultiXactOffset has room for a newly-allocated MultiXactId.
 *
 * NB: this is called while holding MultiXactGenLock.  We want it to be very
 * fast most of the time; even when it's not so fast, no actual I/O need
 * happen unless we're forced to write out a dirty log or xlog page to make
 * room in shared memory.
 */
static void ExtendMultiXactOffset(MultiXactId multi)
{
    int64 pageno;

    /*
     * No work except at first MultiXactId of a page.
     */
    if (MultiXactIdToOffsetEntry(multi) != 0 && multi != FirstMultiXactId)
        return;

    pageno = (int64)MultiXactIdToOffsetPage(multi);

    (void)LWLockAcquire(MultiXactOffsetControlLock, LW_EXCLUSIVE);

    /* Zero the page and make an XLOG entry about it */
    ZeroMultiXactOffsetPage(pageno, true);

    LWLockRelease(MultiXactOffsetControlLock);
}

/*
 * Make sure that MultiXactMember has room for the members of a newly-
 * allocated MultiXactId.
 *
 * Like the above routine, this is called while holding MultiXactGenLock;
 * same comments apply.
 */
static void ExtendMultiXactMember(MultiXactOffset offset, int nmembers)
{
    /*
     * It's possible that the members span more than one page of the members
     * file, so we loop to ensure we consider each page.  The coding is not
     * optimal if the members span several pages, but that seems unusual
     * enough to not worry much about.
     */
    while (nmembers > 0) {
        int entryno;

        /*
         * Only zero when at first entry of a page.
         */
        entryno = MXOffsetToMemberEntry(offset);
        if (entryno == 0) {
            int64 pageno;

            pageno = (int64)MXOffsetToMemberPage(offset);

            (void)LWLockAcquire(MultiXactMemberControlLock, LW_EXCLUSIVE);

            /* Zero the page and make an XLOG entry about it */
            ZeroMultiXactMemberPage(pageno, true);

            LWLockRelease(MultiXactMemberControlLock);
        }

        /* Advance to next page (OK if nmembers goes negative) */
        offset += (MULTIXACT_MEMBERS_PER_PAGE - entryno);
        nmembers -= (MULTIXACT_MEMBERS_PER_PAGE - entryno);
    }
}

/*
 * GetOldestMultiXactId
 *
 * Return the oldest MultiXactId that's still possibly still seen as live by
 * any running transaction.  Older ones might still exist on disk, but they no
 * longer have any running member transaction.
 *
 * It's not safe to truncate MultiXact SLRU segments on the value returned by
 * this function; however, it can be used by a full-table vacuum to set the
 * point at which it will be possible to truncate SLRU for that table.
 */
MultiXactId GetOldestMultiXactId(void)
{
    MultiXactId oldestMXact;
    MultiXactId nextMXact;
    int i;

    /*
     * This is the oldest valid value among all the OldestMemberMXactId[] and
     * OldestVisibleMXactId[] entries, or nextMXact if none are valid.
     */
    (void)LWLockAcquire(MultiXactGenLock, LW_SHARED);

    /*
     * We have to beware of the possibility that nextMXact is in the
     * wrapped-around state.  We don't fix the counter itself here, but we
     * must be sure to use a valid value in our calculation.
     */
    nextMXact = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    if (nextMXact < FirstMultiXactId)
        nextMXact = FirstMultiXactId;

    oldestMXact = nextMXact;
    for (i = 1; i <= MaxOldestSlot; i++) {
        MultiXactId thisoldest;

        thisoldest = t_thrd.shemem_ptr_cxt.OldestMemberMXactId[i];
        if (MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldestMXact))
            oldestMXact = thisoldest;
        thisoldest = t_thrd.shemem_ptr_cxt.OldestVisibleMXactId[i];
        if (MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldestMXact))
            oldestMXact = thisoldest;
    }

    LWLockRelease(MultiXactGenLock);

    return oldestMXact;
}

#ifndef ENABLE_MULTIPLE_NODES
typedef struct mxtruncinfo {
    int earliestExistingPage;
} mxtruncinfo;

/*
 * Decide whether a MultiXactOffset page number is "older" for truncation
 * purposes.  Analogous to CLOGPagePrecedes().
 *
 * Offsetting the values is optional, because MultiXactIdPrecedes() has
 * translational symmetry.
 */
static bool MultiXactOffsetPagePrecedes(int page1, int page2)
{
    MultiXactId multi1;
    MultiXactId multi2;

    multi1 = ((MultiXactId) page1) * MULTIXACT_OFFSETS_PER_PAGE;
    multi1 += FirstMultiXactId + 1;
    multi2 = ((MultiXactId) page2) * MULTIXACT_OFFSETS_PER_PAGE;
    multi2 += FirstMultiXactId + 1;

    return (MultiXactIdPrecedes(multi1, multi2) &&
            MultiXactIdPrecedes(multi1, multi2 + MULTIXACT_OFFSETS_PER_PAGE - 1));
}

/*
 * SlruScanDirectory callback
 * 		This callback determines the earliest existing page number.
 */
static bool SlruScanDirCbFindEarliest(SlruCtl ctl, const char* filename, int64 segpage, const void* data)
{
    mxtruncinfo *trunc = (mxtruncinfo *)data;

    if (trunc->earliestExistingPage == -1 || MultiXactOffsetPagePrecedes(segpage, trunc->earliestExistingPage)) {
        trunc->earliestExistingPage = segpage;
    }

    return false; /* keep going */
}
#endif

/*
 * Remove all MultiXactOffset and MultiXactMember segments before the oldest
 * ones still of interest.
 *
 * This is called by vacuum after it has successfully advanced a database's
 * datminmxid value; the cutoff value we're passed is the minimum of all
 * databases' datminmxid values.
 */
void TruncateMultiXact(MultiXactId oldestMXact)
{
    MultiXactOffset oldestOffset;

#ifndef ENABLE_MULTIPLE_NODES
    mxtruncinfo trunc;
    MultiXactId earliest;
    /*
     * Note we can't just plow ahead with the truncation; it's possible that
     * there are no segments to truncate, which is a problem because we are
     * going to attempt to read the offsets page to determine where to truncate
     * the members SLRU.  So we first scan the directory to determine the
     * earliest offsets page number that we can read without error.
     */
    trunc.earliestExistingPage = -1;
    SlruScanDirectory(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, SlruScanDirCbFindEarliest, &trunc);
    earliest = trunc.earliestExistingPage * MULTIXACT_OFFSETS_PER_PAGE;

    /* nothing to do */
    if (MultiXactIdPrecedes(oldestMXact, earliest))
        return;
#else
    MultiXactOffset nextOffset;
    MultiXactId nextMXact;
    oldestMXact = GetOldestMultiXactId();

    (void)LWLockAcquire(MultiXactGenLock, LW_SHARED);

    nextMXact = t_thrd.shemem_ptr_cxt.MultiXactState->nextMXact;
    if (nextMXact < FirstMultiXactId)
        nextMXact = FirstMultiXactId;

    /* Save the current nextOffset too */
    nextOffset = t_thrd.shemem_ptr_cxt.MultiXactState->nextOffset;
    
    LWLockRelease(MultiXactGenLock);

    ereport(DEBUG2, (errmsg("MultiXact: truncation point = %lu", oldestMXact)));

    /*
     * If we already truncated at this point, do nothing.  This saves time
     * when no MultiXacts are getting used, which is probably not uncommon.
     */
    if (t_thrd.shemem_ptr_cxt.MultiXactState->lastTruncationPoint == oldestMXact)
        return;

    /*
     * We need to determine where to truncate MultiXactMember.	If we found a
     * valid oldest MultiXactId, read its starting offset; otherwise we use
     * the nextOffset value we saved above.
     */
    if (oldestMXact == nextMXact)
        oldestOffset = nextOffset;
    else
#endif
    /*
     * First, compute the safe truncation point for MultiXactMember.
     * This is the starting offset of the multixact we were passed
     * as MultiXactOffset cutoff.
     */
    {
        int64 pageno;
        int slotno;
        int entryno;
        MultiXactOffset *offptr = NULL;

        /* lock is acquired by SimpleLruReadPage_ReadOnly */
        pageno = (int64)MultiXactIdToOffsetPage(oldestMXact);
        entryno = MultiXactIdToOffsetEntry(oldestMXact);

        slotno = SimpleLruReadPage_ReadOnly(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, pageno, oldestMXact);
        offptr = (MultiXactOffset *)t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_buffer[slotno];
        offptr += entryno;
        oldestOffset = *offptr;

        LWLockRelease(MultiXactOffsetControlLock);
    }

    /* truncate MultiXactOffset */
    SimpleLruTruncate(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, MultiXactIdToOffsetPage(oldestMXact),
        NUM_SLRU_DEFAULT_PARTITION);

    /* truncate MultiXactMembers and we're done */
    SimpleLruTruncate(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, MXOffsetToMemberPage(oldestOffset),
        NUM_SLRU_DEFAULT_PARTITION);

    /*
     * Set the last known truncation point.  We don't need a lock for this
     * since only one backend does checkpoints at a time.
     */
    t_thrd.shemem_ptr_cxt.MultiXactState->lastTruncationPoint = oldestMXact;
}

void XLogRecSetMultiXactOffState(XLogBlockMultiXactOffParse *blockmultistate, MultiXactOffset moffset,
                                 MultiXactId multi)
{
    blockmultistate->moffset = moffset;
    blockmultistate->multi = multi;
}
/*
 * Write an xlog record reflecting the zeroing of either a MEMBERs or
 * OFFSETs page (info shows which)
 */
static void WriteMZeroPageXlogRec(int64 pageno, uint8 info)
{
    XLogBeginInsert();
    if (t_thrd.proc->workingVersionNum >= 92068) {
        info |= XLOG_MULTIXACT_INT64_PAGENO;
        XLogRegisterData((char *)(&pageno), sizeof(int64));
    } else {
        XLogRegisterData((char *)(&pageno), sizeof(int));
    }
    (void)XLogInsert(RM_MULTIXACT_ID, info);
}
XLogRecParseState *multixact_xlog_ddl_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    int64 pageno = 0;
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    XLogRecParseState *recordstatehead = NULL;
    int ddltype = BLOCK_DDL_TYPE_NONE;
    *blocknum = 0;

    if ((info & XLOG_MULTIXACT_MASK) == XLOG_MULTIXACT_ZERO_OFF_PAGE) {
        get_multixact_pageno(info, &pageno, record);
        ddltype = BLOCK_DDL_MULTIXACT_OFF_ZERO;
    } else if ((info & XLOG_MULTIXACT_MASK) == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
        get_multixact_pageno(info, &pageno, record);
        ddltype = BLOCK_DDL_MULTIXACT_MEM_ZERO;
    }
    forknum = (ForkNumber)((uint64)pageno >> LOW_BLOKNUMBER_BITS);
    lowblknum = (BlockNumber)((uint64)pageno & LOW_BLOKNUMBER_MASK);
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_DDL_TYPE, filenode, recordstatehead);
    XLogRecSetBlockDdlState(&(recordstatehead->blockparse.extra_rec.blockddlrec), ddltype,
        (char *)XLogRecGetData(record));
    return recordstatehead;
}
XLogRecParseState *multixact_xlog_offset_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint64 pageno;
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    XLogRecParseState *recordstatehead = NULL;
    xl_multixact_create *xlrec = (xl_multixact_create *)XLogRecGetData(record);
    pageno = MultiXactIdToOffsetPage(xlrec->mid);
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    forknum = (ForkNumber)(pageno >> LOW_BLOKNUMBER_BITS);
    lowblknum = (BlockNumber)(pageno & LOW_BLOKNUMBER_MASK);
    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_MULITACT_OFF_TYPE, filenode, recordstatehead);
    XLogRecSetMultiXactOffState(&(recordstatehead->blockparse.extra_rec.blockmultixactoff), xlrec->moff, xlrec->mid);
    return recordstatehead;
}
void XLogRecSetMultiXactMemState(XLogBlockMultiXactMemParse *blockmultistate, MultiXactOffset startoffset,
                                 MultiXactId multi, uint64 xidnum, TransactionId *xidsarry)
{
    blockmultistate->startoffset = startoffset;
    blockmultistate->multi = multi;
    blockmultistate->xidnum = xidnum;
    for (uint64 i = 0; i < xidnum; i++) {
        blockmultistate->xidsarry[i] = xidsarry[i];
    }
}
XLogRecParseState *multixact_xlog_mem_parse_to_block(XLogReaderState *record, uint32 *blocknum,
                                                     XLogRecParseState *recordstatehead)
{
    uint64 pageno;
    MultiXactOffset offset = 0;
    MultiXactOffset startoffset = 0;
    uint64 prev_pageno;
    int continuenum = 0;
    TransactionId xidsarry[MAX_BLOCK_XID_NUMS];
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    XLogRecParseState *blockstate = NULL;
    xl_multixact_create *xlrec = (xl_multixact_create *)XLogRecGetData(record);
    if (xlrec->nxids > 0) {
        offset = xlrec->moff;
        startoffset = offset;
        prev_pageno = MXOffsetToMemberPage(offset);
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        forknum = (ForkNumber)(prev_pageno >> LOW_BLOKNUMBER_BITS);
        lowblknum = (BlockNumber)(prev_pageno & LOW_BLOKNUMBER_MASK);
        RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
        XLogRecSetBlockCommonState(record, BLOCK_DATA_MULITACT_MEM_TYPE, filenode, blockstate);
        xidsarry[continuenum] = xlrec->xids[0];
        offset++;
        continuenum++;
    }
    for (int i = 1; i < xlrec->nxids; i++, offset++) {
        pageno = MXOffsetToMemberPage(offset);
        if ((pageno != prev_pageno) || (continuenum == MAX_BLOCK_XID_NUMS)) {
            XLogRecSetMultiXactMemState(&(blockstate->blockparse.extra_rec.blockmultixactmem), startoffset, xlrec->mid,
                                        continuenum, xidsarry);
            prev_pageno = pageno;
            startoffset = offset;
            continuenum = 0;
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }
            forknum = (ForkNumber)(prev_pageno >> LOW_BLOKNUMBER_BITS);
            lowblknum = (BlockNumber)(prev_pageno & LOW_BLOKNUMBER_MASK);
            RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
            XLogRecSetBlockCommonState(record, BLOCK_DATA_MULITACT_MEM_TYPE, filenode, blockstate);
        }
        xidsarry[continuenum] = xlrec->xids[i];
        continuenum++;
    }
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetMultiXactMemState(&(blockstate->blockparse.extra_rec.blockmultixactmem), startoffset, xlrec->mid,
                                continuenum, xidsarry);
    return recordstatehead;
}
void XLogRecSetMultiXactUpdatOidState(XLogBlockMultiUpdateParse *blockmultistate, MultiXactOffset nextoffset,
                                      MultiXactId nextmulti, TransactionId maxxid)
{
    blockmultistate->nextmulti = nextmulti;
    blockmultistate->nextoffset = nextoffset;
    blockmultistate->maxxid = maxxid;
}
XLogRecParseState *multixact_xlog_updateoid_parse_to_block(XLogReaderState *record, uint32 *blocknum,
                                                           XLogRecParseState *recordstatehead)
{
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    MultiXactId nextmulti;
    MultiXactOffset nextoffset;
    TransactionId max_xid;
    XLogRecParseState *blockstate = NULL;
    xl_multixact_create *xlrec = (xl_multixact_create *)XLogRecGetData(record);
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_MULITACT_UPDATEOID_TYPE, filenode, blockstate);
    nextmulti = xlrec->mid + 1;
    nextoffset = xlrec->moff + xlrec->nxids;
    max_xid = XLogRecGetXid(record);
    for (int32 i = 0; i < xlrec->nxids; i++) {
        TransactionId memberXid = GET_MEMBER_XID_FROM_SLRU_XID(xlrec->xids[i]);
        if (TransactionIdPrecedes(max_xid, memberXid))
            max_xid = memberXid;
    }
    XLogRecSetMultiXactUpdatOidState(&(blockstate->blockparse.extra_rec.blockmultiupdate), nextoffset, nextmulti,
                                     max_xid);
    return recordstatehead;
}
XLogRecParseState *multixact_xlog_createxid_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    recordstatehead = multixact_xlog_offset_parse_to_block(record, blocknum);
    if (recordstatehead == NULL) {
        return NULL;
    }
    recordstatehead = multixact_xlog_mem_parse_to_block(record, blocknum, recordstatehead);
    if (recordstatehead == NULL) {
        return NULL;
    }
    recordstatehead = multixact_xlog_updateoid_parse_to_block(record, blocknum, recordstatehead);
    if (recordstatehead == NULL) {
        return NULL;
    }
    return recordstatehead;
}
XLogRecParseState *multixact_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 0;
    if (((info & XLOG_MULTIXACT_MASK) == XLOG_MULTIXACT_ZERO_OFF_PAGE) ||
        ((info & XLOG_MULTIXACT_MASK) == XLOG_MULTIXACT_ZERO_MEM_PAGE)) {
        recordstatehead = multixact_xlog_ddl_parse_to_block(record, blocknum);
    } else if (info == XLOG_MULTIXACT_CREATE_ID) {
        recordstatehead = multixact_xlog_createxid_parse_to_block(record, blocknum);
    } else {
        ereport(PANIC, (errmsg("multixact_redo_parse_to_block: unknown op code %u", info)));
    }
    return recordstatehead;
}

static void get_multixact_pageno(uint8 info, int64 *pageno, XLogReaderState *record)
{
    errno_t rc = EOK;
    if ((info & XLOG_MULTIXACT_INT64_PAGENO) != 0) {
        rc = memcpy_s(pageno, sizeof(int64), XLogRecGetData(record), sizeof(int64));
        securec_check(rc, "", "");
    } else {
        rc = memcpy_s(pageno, sizeof(int64), XLogRecGetData(record), sizeof(int));
        securec_check(rc, "", "");
    }
}
/*
 * MULTIXACT resource manager's routines
 */
void multixact_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 mask_info = info & XLOG_MULTIXACT_MASK;
    /* Backup blocks are not used in multixact records */
    Assert(!XLogRecHasAnyBlockRefs(record));
    if (mask_info == XLOG_MULTIXACT_ZERO_OFF_PAGE) {
        int64 pageno = 0;
        int slotno;

        get_multixact_pageno(info, &pageno, record);
        (void)LWLockAcquire(MultiXactOffsetControlLock, LW_EXCLUSIVE);
        slotno = ZeroMultiXactOffsetPage(pageno, false);
        SimpleLruWritePage(t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl, slotno);
        Assert(!t_thrd.shemem_ptr_cxt.MultiXactOffsetCtl->shared->page_dirty[slotno]);

        LWLockRelease(MultiXactOffsetControlLock);
    } else if (mask_info == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
        int64 pageno = 0;
        int slotno;

        get_multixact_pageno(info, &pageno, record);

        (void)LWLockAcquire(MultiXactMemberControlLock, LW_EXCLUSIVE);

        slotno = ZeroMultiXactMemberPage(pageno, false);
        SimpleLruWritePage(t_thrd.shemem_ptr_cxt.MultiXactMemberCtl, slotno);
        Assert(!t_thrd.shemem_ptr_cxt.MultiXactMemberCtl->shared->page_dirty[slotno]);

        LWLockRelease(MultiXactMemberControlLock);
    } else if (mask_info == XLOG_MULTIXACT_CREATE_ID) {
        xl_multixact_create *xlrec = (xl_multixact_create *)XLogRecGetData(record);
        TransactionId *xidsWithStatus = xlrec->xids;
        TransactionId max_xid;
        int i;

        /* Store the data back into the SLRU files */
        RecordNewMultiXact(xlrec->mid, xlrec->moff, xlrec->nxids, xidsWithStatus);

        /* Make sure nextMXact/nextOffset are beyond what this record has */
        MultiXactAdvanceNextMXact(xlrec->mid + 1, xlrec->moff + xlrec->nxids);

        /*
         * Make sure nextXid is beyond any XID mentioned in the record. This
         * should be unnecessary, since any XID found here ought to have other
         * evidence in the XLOG, but let's be safe.
         */
        max_xid = XLogRecGetXid(record);
        for (i = 0; i < xlrec->nxids; i++) {
            TransactionId memberXid = GET_MEMBER_XID_FROM_SLRU_XID(xidsWithStatus[i]);
            if (TransactionIdPrecedes(max_xid, memberXid))
                max_xid = memberXid;
        }

        /*
         * We don't expect anyone else to modify nextXid, hence startup
         * process doesn't need to hold a lock while checking this. We still
         * acquire the lock to modify it, though.
         */
        if (TransactionIdFollowsOrEquals(max_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
            if (TransactionIdFollowsOrEquals(max_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
                t_thrd.xact_cxt.ShmemVariableCache->nextXid = max_xid;
                TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
            }
            LWLockRelease(XidGenLock);
        }
    } else {
        ereport(PANIC, (errmsg("multixact_redo: unknown op code %u", (uint32)info)));
    }
}
