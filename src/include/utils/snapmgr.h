/* -------------------------------------------------------------------------
 *
 * snapmgr.h
 *	  openGauss snapshot manager
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/snapmgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SNAPMGR_H
#define SNAPMGR_H

#include "utils/resowner.h"
#include "utils/snapshot.h"
#include "access/transam.h"

/* Static variables representing various special snapshot semantics */
extern THR_LOCAL PGDLLIMPORT SnapshotData SnapshotNowData;
extern THR_LOCAL PGDLLIMPORT SnapshotData SnapshotSelfData;
extern THR_LOCAL PGDLLIMPORT SnapshotData SnapshotAnyData;
extern THR_LOCAL PGDLLIMPORT SnapshotData SnapshotToastData;
#ifdef ENABLE_MULTIPLE_NODES
extern THR_LOCAL PGDLLIMPORT SnapshotData SnapshotNowNoSyncData;
#endif

#define SnapshotNow (&SnapshotNowData)
#ifdef ENABLE_MULTIPLE_NODES
#define SnapshotNowNoSync (&SnapshotNowNoSyncData)
#endif
#define SnapshotSelf (&SnapshotSelfData)
#define SnapshotAny (&SnapshotAnyData)
#define SnapshotToast (&SnapshotToastData)

#ifdef USE_ASSERT_CHECKING
#define LatestTransactionStatusError(xid, snapshot, action) PrintCurrentSnapshotInfo(PANIC, xid, snapshot, action)
#else
#define LatestTransactionStatusError(xid, snapshot, action) PrintCurrentSnapshotInfo(ERROR, xid, snapshot, action)
#endif

extern bool XidVisibleInSnapshot(TransactionId xid, Snapshot snapshot, TransactionIdStatus *hintstatus,
                                                        Buffer buffer, bool *sync);
extern bool UHeapXidVisibleInSnapshot(TransactionId xid, Snapshot snapshot, TransactionIdStatus *hintstatus,
    Buffer buffer, bool *sync);
extern bool XidVisibleInDecodeSnapshot(TransactionId xid, Snapshot snapshot,
    TransactionIdStatus* hintstatus, Buffer buffer);
extern bool CommittedXidVisibleInSnapshot(TransactionId xid, Snapshot snapshot, Buffer buffer);
extern bool CommittedXidVisibleInDecodeSnapshot(TransactionId xid, Snapshot snapshot, Buffer buffer);
extern bool IsXidVisibleInGtmLiteLocalSnapshot(TransactionId xid, Snapshot snapshot, TransactionIdStatus hint_status,
                                                                                    bool xmin_equal_xmax, Buffer buffer, bool *sync);
extern void RecheckXidFinish(TransactionId xid, CommitSeqNo csn);
/*
 * We don't provide a static SnapshotDirty variable because it would be
 * non-reentrant.  Instead, users of that snapshot type should declare a
 * local variable of type SnapshotData, and initialize it with this macro.
 */
#define InitDirtySnapshot(snapshotdata) ((snapshotdata).satisfies = SNAPSHOT_DIRTY)

#define IsVersionMVCCSnapshot(snapshot) \
    (((snapshot)->satisfies) == SNAPSHOT_VERSION_MVCC || \
    ((snapshot)->satisfies) == SNAPSHOT_DELTA || \
    ((snapshot)->satisfies) == SNAPSHOT_LOST)

/* This macro encodes the knowledge of which snapshots are MVCC-safe */
#define IsMVCCSnapshot(snapshot) \
    ((((snapshot)->satisfies) == SNAPSHOT_MVCC) || (((snapshot)->satisfies) == SNAPSHOT_HISTORIC_MVCC) || \
        (((snapshot)->satisfies) == SNAPSHOT_DECODE_MVCC) || IsVersionMVCCSnapshot(snapshot))

extern Snapshot GetTransactionSnapshot(bool force_local_snapshot = false);
extern Snapshot GetLatestSnapshot(void);
extern Snapshot GetCatalogSnapshot();
extern Snapshot get_toast_snapshot();
extern void SnapshotSetCommandId(CommandId curcid);

extern void PushActiveSnapshot(Snapshot snapshot);
extern void PushCopiedSnapshot(Snapshot snapshot);
extern void UpdateActiveSnapshotCommandId(void);
extern void PopActiveSnapshot(void);
extern Snapshot GetActiveSnapshot(void);
extern bool ActiveSnapshotSet(void);

extern void FreeSnapshotDeepForce(Snapshot snap);

extern Snapshot RegisterSnapshot(Snapshot snapshot);
extern void UnregisterSnapshot(Snapshot snapshot);
extern Snapshot RegisterSnapshotOnOwner(Snapshot snapshot, ResourceOwner owner);
extern void UnregisterSnapshotFromOwner(Snapshot snapshot, ResourceOwner owner);
extern void RegisterStreamSnapshots();
extern void ForgetRegisterStreamSnapshots();

extern void UnRegisterStreamSnapshots();

extern void AtSubCommit_Snapshot(int level);
extern void AtSubAbort_Snapshot(int level);
extern void AtEOXact_Snapshot(bool isCommit);

extern Datum pg_export_snapshot(PG_FUNCTION_ARGS);
extern void ImportSnapshot(const char* idstr);
extern bool XactHasExportedSnapshots(void);
extern void DeleteAllExportedSnapshotFiles(void);

extern void StreamTxnContextSaveSnapmgr(void* stc);
extern void StreamTxnContextRestoreSnapmgr(const void* stc);
extern void StreamTxnContextSetSnapShot(void* snapshotPtr);
extern bool ThereAreNoPriorRegisteredSnapshots(void);
extern Snapshot PgFdwCopySnapshot(Snapshot snapshot);
extern char* ExportSnapshot(Snapshot snapshot, CommitSeqNo *snapshotcsn);

extern Snapshot CopySnapshotByCurrentMcxt(Snapshot snapshot);


/* Support for catalog timetravel for logical decoding */
struct HTAB;
extern struct HTAB* HistoricSnapshotGetTupleCids(void);
extern void SetupHistoricSnapshot(Snapshot snapshot_now, struct HTAB* tuplecids);
extern void TeardownHistoricSnapshot(bool is_error);
extern bool HistoricSnapshotActive(void);

extern void SetTransactionSnapshot(Snapshot sourcesnap, VirtualTransactionId *sourcevxid, ThreadId sourcepid);
extern Snapshot get_toast_snapshot();

#endif /* SNAPMGR_H */
