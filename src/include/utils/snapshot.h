/* -------------------------------------------------------------------------
 *
 * snapshot.h
 *	  openGauss snapshot definition
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/utils/snapshot.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SNAPSHOT_H
#define SNAPSHOT_H

#include "access/htup.h"
#include "storage/buf/buf.h"
#include "gtm/gtm_c.h"

/*
 * The different snapshot types.  We use SnapshotData structures to represent
 * both "regular" (MVCC) snapshots and "special" snapshots that have non-MVCC
 * semantics.  The specific semantics of a snapshot are encoded by its type.
 *
 * The behaviour of each type of snapshot should be documented alongside its
 * enum value, best in terms that are not specific to an individual table AM.
 *
 * The reason the snapshot type rather than a callback as it used to be is
 * that that allows to use the same snapshot for different table AMs without
 * having one callback per AM.
 */
typedef enum SnapshotSatisfiesMethod {
    /* -------------------------------------------------------------------------
     * A tuple is visible iff the tuple is valid for the given MVCC snapshot.
     *
     * Here, we consider the effects of:
     * - all transactions committed as of the time of the given snapshot
     * - previous commands of this transaction
     *
     * Does _not_ include:
     * - transactions shown as in-progress by the snapshot
     * - transactions started after the snapshot was taken
     * - changes made by the current command
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_MVCC = 0,

    /* -------------------------------------------------------------------------
     * A tuple is visible if the tuple is valid for the given snapshot.
     *
     * Here, we consider the effects of:
     * - all transactions committed before the CSN of the given snapshot, that is:
           - all transactions committed as of the time of the given snapshot
     *
     * Does _not_ include:
     * - transactions committed after CSN of the given snapshot, that is:
     *     - transactions shown as in-progress by the snapshot
     *     - transactions started after the snapshot was taken
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_VERSION_MVCC,

    /* -------------------------------------------------------------------------
     * A tuple is visible if the tuple is invalid for the given snapshot, and
     * is valid for SnapshotNow. the pseudocode as follow:
     * !TupleIsVisible(SNAPSHOT_VERSION_MVCC) && TupleIsVisible(SnapshotNow)
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_DELTA,

    /* -------------------------------------------------------------------------
     * A tuple is visible if the tuple is valid for the given CSN snapshot
     * but invalid for SnapshotNow.
     *
     * Here, we consider the effects of:
     * - all transactions committed before given CSN
     *
     * Does _not_ include:
     * - transactions committed after given CSN
     * -------------------------------------------------------------------------
     */
    /* -------------------------------------------------------------------------
     * A tuple is visible if the tuple is valid for the given snapshot, and
     * is invalid for SnapshotNow. the pseudocode as follow:
     * TupleIsVisible(SNAPSHOT_VERSION_MVCC) && !TupleIsVisible(SnapshotNow)
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_LOST,

    /* -------------------------------------------------------------------------
     * A tuple is visible iff heap tuple is valid "now".
     *
     * Here, we consider the effects of:
     * - all committed transactions (as of the current instant)
     * - previous commands of this transaction
     *
     * Does _not_ include:
     * - changes made by the current command.
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_NOW,

#ifdef ENABLE_MULTIPLE_NODES
    /* -------------------------------------------------------------------------
     * Same as SNAPSHOT_NOW, skip sync in distribute mode.
     * If we scan pg_class in seqscan, skip sync.
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_NOW_NO_SYNC,
#endif

    /* -------------------------------------------------------------------------
     * A tuple is visible iff the tuple is valid "for itself".
     *
     * Here, we consider the effects of:
     * - all committed transactions (as of the current instant)
     * - previous commands of this transaction
     * - changes made by the current command
     *
     * Does _not_ include:
     * - in-progress transactions (as of the current instant)
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_SELF,

    /*
     * Any tuple is visible.
     */
    SNAPSHOT_ANY,

    /*
     * A tuple is visible iff the tuple is valid as a TOAST row.
     */
    SNAPSHOT_TOAST,

    /* -------------------------------------------------------------------------
     * A tuple is visible iff the tuple is valid including effects of open
     * transactions.
     *
     * Here, we consider the effects of:
     * - all committed and in-progress transactions (as of the current instant)
     * - previous commands of this transaction
     * - changes made by the current command
     *
     * This is essentially like SNAPSHOT_SELF as far as effects of the current
     * transaction and committed/aborted xacts are concerned.  However, it
     * also includes the effects of other xacts still in progress.
     *
     * A special hack is that when a snapshot of this type is used to
     * determine tuple visibility, the passed-in snapshot struct is used as an
     * output argument to return the xids of concurrent xacts that affected
     * the tuple.  snapshot->xmin is set to the tuple's xmin if that is
     * another transaction that's still in progress; or to
     * InvalidTransactionId if the tuple's xmin is committed good, committed
     * dead, or my own xact.  Similarly for snapshot->xmax and the tuple's
     * xmax.  If the tuple was inserted speculatively, meaning that the
     * inserter might still back down on the insertion without aborting the
     * whole transaction, the associated token is also returned in
     * snapshot->speculativeToken.  See also InitDirtySnapshot().
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_DIRTY,

    /*
     * A tuple is visible iff it follows the rules of SNAPSHOT_MVCC, but
     * supports being called in timetravel context (for decoding catalog
     * contents in the context of logical decoding).
     */
    SNAPSHOT_HISTORIC_MVCC,
    /*
     * Whether a tuple is visible is decided by CSN,
     * which is used in parallel decoding.
     */
    SNAPSHOT_DECODE_MVCC
} SnapshotSatisfiesMethod;

typedef struct SnapshotData* Snapshot;

#define InvalidSnapshot ((Snapshot)NULL)

/*
 * Struct representing all kind of possible snapshots.
 *
 * There are several different kinds of snapshots:
 * * Normal MVCC snapshots
 * * MVCC snapshots taken during recovery (in Hot-Standby mode)
 * * Historic MVCC snapshots used during logical decoding
 * * snapshots passed to HeapTupleSatisfiesDirty()
 * * snapshots used for SatisfiesAny, Toast, Self where no members are
 *   accessed.
 *
 * It's probably a good idea to split this struct using a NodeTag
 * similar to how parser and executor nodes are handled, with one type for
 * each different kind of snapshot to avoid overloading the meaning of
 * individual fields.
 */

typedef struct SnapshotData {
    SnapshotSatisfiesMethod satisfies; /* satisfies type. */

    /*
     * The remaining fields are used only for MVCC snapshots, and are normally
     * just zeroes in special snapshots.  (But xmin and xmax are used
     * specially by HeapTupleSatisfiesDirty.)
     *
     * An MVCC snapshot can never see the effects of XIDs >= xmax. It can see
     * the effects of all older XIDs except those listed in the snapshot. xmin
     * is stored as an optimization to avoid needing to search the XID arrays
     * for most tuples.
     */
    TransactionId xmin; /* all XID < xmin are visible to me */
    TransactionId xmax; /* all XID >= xmax are invisible to me */

     /* subxid is in progress and it's the last one modify tuple */
    SubTransactionId subxid;

    /*
     * For normal MVCC snapshot this contains the all xact IDs that are in
     * progress, unless the snapshot was taken during recovery in which case
     * it's empty. For historic MVCC snapshots, the meaning is inverted,
     * i.e. it contains *committed* transactions between xmin and xmax.
     */
    TransactionId* xip;
    /*
     * For non-historic MVCC snapshots, this contains subxact IDs that are in
     * progress (and other transactions that are in progress if taken during
     * recovery). For historic snapshot it contains *all* xids assigned to the
     * replayed transaction, including the toplevel xid.
     */
    TransactionId* subxip;

    uint32 xcnt;           /* # of xact ids in xip[] */
    GTM_Timeline timeline; /* GTM timeline */
#ifdef PGXC                /* PGXC_COORD */
    uint32 max_xcnt;       /* Max # of xact in xip[] */
#endif
    /* note: all ids in xip[] satisfy xmin <= xip[i] < xmax */
    int32 subxcnt;      /* # of xact ids in subxip[] */
    int32 maxsubxcnt;   /* # max xids could store in subxip[] */
    bool suboverflowed; /* has the subxip array overflowed? */

    /*
     * This snapshot can see the effects of all transactions with CSN <=
     * snapshotcsn.
     */
    CommitSeqNo snapshotcsn;

    /* For GTMLite local snapshot, we keep an array of prepared transaction xids for MVCC */
    int prepared_array_capacity;
    int prepared_count;
    TransactionId* prepared_array;

    bool takenDuringRecovery; /* recovery-shaped snapshot? */
    bool copied;              /* false if it's a static snapshot */

    /*
     * note: all ids in subxip[] are >= xmin, but we don't bother filtering
     * out any that are >= xmax
     */
    CommandId curcid;    /* in my xact, CID < curcid are visible */
    uint32 active_count; /* refcount on ActiveSnapshot stack */
    uint32 regd_count;   /* refcount on RegisteredSnapshotList */
    void* user_data;     /* for local multiversion snapshot */
    GTM_SnapshotType gtm_snapshot_type;
} SnapshotData;

/*
 * Result codes for AM API tuple_{update,delete,lock}, and for visibility.
 */
typedef enum TM_Result
{
    /*
     * Signals that the action succeeded (i.e. update/delete performed, lock
     * was acquired)
     */
    TM_Ok,

    /* The affected tuple wasn't visible to the relevant snapshot */
    TM_Invisible,

    /* The affected tuple was already modified by the calling backend */
    TM_SelfModified,

    /*
     * The affected tuple was updated by another transaction. 
     */
    TM_Updated,

    /* The affected tuple was deleted by another transaction */
    TM_Deleted,

    /*
     * The affected tuple is currently being modified by another session. This
     * will only be returned if (update/delete/lock)_tuple are instructed not
     * to wait.
     */
    TM_BeingModified,
    TM_SelfCreated,
    TM_SelfUpdated
} TM_Result;

#endif /* SNAPSHOT_H */
