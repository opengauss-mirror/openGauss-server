/* ---------------------------------------------------------------------------------------
 * 
 * slot.h
 *	   Replication slot management.
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/slot.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SLOT_H
#define SLOT_H

#include "fmgr.h"
#include "access/xlog.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
/*
 * Behaviour of replication slots, upon release or crash.
 *
 * Slots marked as PERSISTENT are crashsafe and will not be dropped when
 * released. Slots marked as EPHEMERAL will be dropped when released or after
 * restarts.
 *
 * EPHEMERAL slots can be made PERSISTENT by calling ReplicationSlotPersist().
 */
typedef enum ReplicationSlotPersistency { RS_PERSISTENT, RS_EPHEMERAL } ReplicationSlotPersistency;

/*
 * On-Disk data of a replication slot, preserved across restarts.
 */

typedef struct ReplicationSlotPersistentData {
    /* The slot's identifier */
    NameData name;

    /* database the slot is active on */
    Oid database;
    /*
     * The slot's behaviour when being dropped (or restored after a crash).
     */
    ReplicationSlotPersistency persistency;
    bool isDummyStandby;

    /*
     * xmin horizon for data
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    TransactionId xmin;
    /*
     * xmin horizon for catalog tuples
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    TransactionId catalog_xmin;

    /* oldest LSN that might be required by this replication slot */
    XLogRecPtr restart_lsn;
    /* oldest LSN that the client has acked receipt for */
    XLogRecPtr confirmed_flush;
    /* plugin name */
    NameData plugin;
} ReplicationSlotPersistentData;

/*
 * Replication slot on-disk data structure.
 */
typedef struct ReplicationSlotOnDisk {
    /* first part of this struct needs to be version independent */

    /* data not covered by checksum */
    uint32 magic;
    pg_crc32c checksum;

    /* data covered by checksum */
    uint32 version;
    uint32 length;

    ReplicationSlotPersistentData slotdata;
} ReplicationSlotOnDisk;

/*
 * Shared memory state of a single replication slot.
 */
typedef struct ReplicationSlot {
    /* lock, on same cacheline as effective_xmin */
    slock_t mutex;

    /* is this slot defined */
    bool in_use;

    /* is somebody streaming out changes for this slot */
    bool active;

    /* any outstanding modifications? */
    bool just_dirtied;
    bool dirty;

    /*
     * For logical decoding, it's extremely important that we never remove any
     * data that's still needed for decoding purposes, even after a crash;
     * otherwise, decoding will produce wrong answers.  Ordinary streaming
     * replication also needs to prevent old row versions from being removed
     * too soon, but the worst consequence we might encounter there is unwanted
     * query cancellations on the standby.  Thus, for logical decoding,
     * this value represents the latest xmin that has actually been
     * written to disk, whereas for streaming replication, it's just the
     * same as the persistent value (data.xmin).
     */
    TransactionId effective_xmin;
    TransactionId effective_catalog_xmin;

    /* data surviving shutdowns and crashes */
    ReplicationSlotPersistentData data;

    /* is somebody performing io on this slot? */
    LWLock  *io_in_progress_lock;

    /* all the remaining data is only used for logical slots */

    /* ----
     * When the client has confirmed flushes >= candidate_xmin_lsn we can
     * advance the catalog xmin, when restart_valid has been passed,
     * restart_lsn can be increased.
     * ----
     */
    TransactionId candidate_catalog_xmin;
    XLogRecPtr candidate_xmin_lsn;
    XLogRecPtr candidate_restart_valid;
    XLogRecPtr candidate_restart_lsn;
} ReplicationSlot;

/* size of the part of the slot that is version independent */
#define ReplicationSlotOnDiskConstantSize offsetof(ReplicationSlotOnDisk, slotdata)
/* size of the slots that is not version indepenent */
#define ReplicationSlotOnDiskDynamicSize sizeof(ReplicationSlotOnDisk) - ReplicationSlotOnDiskConstantSize
#define SLOT_MAGIC 0x1051CA1 /* format identifier */
#define SLOT_VERSION 1       /* version for new files */

#define XLOG_SLOT_CREATE 0x00
#define XLOG_SLOT_ADVANCE 0x10
#define XLOG_SLOT_DROP 0x20
#define XLOG_SLOT_CHECK 0x30
#define XLOG_TERM_LOG 0x40

typedef struct xl_slot_header {
    ReplicationSlotPersistentData data;
} xl_slot_header;
#define SizeOfSlotHeader (offsetof(xl_slot_header, data) + sizeof(ReplicationSlotPersistentData))
/*
 * Shared memory control area for all of replication slots.
 */
typedef struct ReplicationSlotCtlData {
    ReplicationSlot replication_slots[1];
} ReplicationSlotCtlData;
/*
 * ret for computing all of replication slots.
 */
typedef struct ReplicationSlotState {
    XLogRecPtr min_required;
    XLogRecPtr max_required;
    bool exist_in_use;
} ReplicationSlotState;
/*
 * Shared memory control area for all of replication slots.
 */
typedef struct LogicalPersistentData {
    int SlotNum;
    ReplicationSlotPersistentData replication_slots[FLEXIBLE_ARRAY_MEMBER];
} LogicalPersistentData;

/* shmem initialization functions */
extern Size ReplicationSlotsShmemSize(void);
extern void ReplicationSlotsShmemInit(void);

/* management of individual slots */
extern void ReplicationSlotCreate(const char* name, ReplicationSlotPersistency persistency, bool isDummyStandby,
    Oid databaseId, XLogRecPtr restart_lsn);
extern void ReplicationSlotPersist(void);
extern void ReplicationSlotDrop(const char* name);
extern void ReplicationSlotAcquire(const char* name, bool isDummyStandby);
bool ReplicationSlotFind(const char* name);
extern void ReplicationSlotRelease(void);
extern void ReplicationSlotSave(void);
extern void ReplicationSlotMarkDirty(void);
extern void CreateSlotOnDisk(ReplicationSlot* slot);

/* misc stuff */
extern bool ReplicationSlotValidateName(const char* name, int elevel);
extern bool ValidateName(const char* name);
extern void ReplicationSlotsComputeRequiredXmin(bool already_locked);
extern void ReplicationSlotsComputeRequiredLSN(ReplicationSlotState* repl_slt_state);
extern void ReplicationSlotReportRestartLSN(void);
extern void StartupReplicationSlots();
extern void CheckPointReplicationSlots(void);
extern void SetDummyStandbySlotLsnInvalid(void);

extern void CheckSlotRequirements(void);

/* SQL callable functions */
extern Datum pg_get_replication_slots(PG_FUNCTION_ARGS);
extern void create_logical_replication_slot(
    Name name, Name plugin, bool isDummyStandby, NameData* databaseName, char* str_tmp_lsn);
extern XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void);
extern bool ReplicationSlotsCountDBSlots(Oid dboid, int* nslots, int* nactive);
extern Datum pg_create_physical_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_drop_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_get_replication_slot_name(PG_FUNCTION_ARGS);

/* slot redo */
extern void slot_redo(XLogReaderState* record);
extern void slot_desc(StringInfo buf, XLogReaderState* record);
extern void redo_slot_advance(const ReplicationSlotPersistentData* slotInfo);
extern void log_slot_advance(const ReplicationSlotPersistentData* slotInfo);
extern void log_slot_drop(const char* name);
extern void log_slot_create(const ReplicationSlotPersistentData* data);
extern void LogCheckSlot();
extern Size GetAllLogicalSlot(LogicalPersistentData*& LogicalSlot);
extern char* get_my_slot_name();
#endif /* SLOT_H */

