/* -------------------------------------------------------------------------
 *
 * snapmgr.h
 *	  POSTGRES snapshot manager
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

extern Snapshot GetTransactionSnapshot(bool force_local_snapshot = false);
extern Snapshot GetLatestSnapshot(void);
extern Snapshot GetCatalogSnapshot(Oid relid);
extern void SnapshotSetCommandId(CommandId curcid);
extern Snapshot GetNonHistoricCatalogSnapshot(Oid relid);

extern void PushActiveSnapshot(Snapshot snapshot);
extern void PushCopiedSnapshot(Snapshot snapshot);
extern void UpdateActiveSnapshotCommandId(void);
extern void PopActiveSnapshot(void);
extern Snapshot GetActiveSnapshot(void);
extern bool ActiveSnapshotSet(void);

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

/* Support for catalog timetravel for logical decoding */
struct HTAB;
extern struct HTAB* HistoricSnapshotGetTupleCids(void);
extern void SetupHistoricSnapshot(Snapshot snapshot_now, struct HTAB* tuplecids);
extern void TeardownHistoricSnapshot(bool is_error);
extern bool HistoricSnapshotActive(void);
#endif /* SNAPMGR_H */
