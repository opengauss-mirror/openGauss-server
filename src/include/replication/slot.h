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
#include "storage/lock/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
extern const uint32 EXTRA_SLOT_VERSION_NUM;

/*
 * Behaviour of replication slots, upon release or crash.
 *
 * Slots marked as PERSISTENT are crashsafe and will not be dropped when
 * released. Slots marked as EPHEMERAL will be dropped when released or after
 * restarts. BACKUP is basically similar to PERSISTENT, except that such slots are
 * taken into consideration when computing restart_lsn or delay_ddl_lsn even if inactive.
 *
 * EPHEMERAL slots can be made PERSISTENT by calling ReplicationSlotPersist().
 */
typedef enum ReplicationSlotPersistency { RS_PERSISTENT, RS_EPHEMERAL, RS_BACKUP } ReplicationSlotPersistency;

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
     /* !!!!!!!!!!!!!!!!!!we steal two bytes from persistency for extra content length in new version */
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

typedef struct ObsArchiveConfig {
    char *obs_address;
    char *obs_bucket;
    char *obs_ak;
    char *obs_sk;
    char *obs_prefix;
} ObsArchiveConfig;

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
    ObsArchiveConfig* archive_obs;
    char* extra_content;
} ReplicationSlot;


#define ReplicationSlotPersistentDataConstSize sizeof(ReplicationSlotPersistentData)
/* size of the part of the slot that is version independent */
#define ReplicationSlotOnDiskConstantSize offsetof(ReplicationSlotOnDisk, slotdata)
/* size of the slots that is not version indepenent */
#define ReplicationSlotOnDiskDynamicSize sizeof(ReplicationSlotOnDisk) - ReplicationSlotOnDiskConstantSize
#define SLOT_MAGIC 0x1051CA1 /* format identifier */
#define SLOT_VERSION 1       /* version for new files */
#define SLOT_VERSION_TWO 2       /* version for new files */


#define XLOG_SLOT_CREATE 0x00
#define XLOG_SLOT_ADVANCE 0x10
#define XLOG_SLOT_DROP 0x20
#define XLOG_SLOT_CHECK 0x30
#define XLOG_TERM_LOG 0x40
#define INT32_HIGH_MASK 0xFF00
#define INT32_LOW_MASK 0x00FF

/* we steal two bytes from persistency for updagrade */
#define GET_SLOT_EXTRA_DATA_LENGTH(data) (((int)((data).persistency)) >> 16)
#define SET_SLOT_EXTRA_DATA_LENGTH(data, length) ((data).persistency = (ReplicationSlotPersistency)((int)(((data).persistency) & INT32_LOW_MASK) | ((length) << 16)))
#define GET_SLOT_PERSISTENCY(data) (ReplicationSlotPersistency(int16((data).persistency)))
#define SET_SLOT_PERSISTENCY(data, mypersistency) ((data).persistency = (ReplicationSlotPersistency)((int)(((data).persistency) & INT32_HIGH_MASK) | (int16(mypersistency))))
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
    Oid databaseId, XLogRecPtr restart_lsn, char* extra_content = NULL, bool encrypted = false);
extern void ReplicationSlotPersist(void);
extern void ReplicationSlotDrop(const char* name, bool for_backup = false);
extern void ReplicationSlotAcquire(const char* name, bool isDummyStandby, bool allowDrop = false);
extern bool IsReplicationSlotActive(const char *name);
bool ReplicationSlotFind(const char* name);
extern void ReplicationSlotRelease(void);
extern void ReplicationSlotSave(void);
extern void ReplicationSlotMarkDirty(void);
extern void CreateSlotOnDisk(ReplicationSlot* slot);

/* misc stuff */
extern bool ReplicationSlotValidateName(const char* name, int elevel);
extern void ValidateName(const char* name);
extern void ReplicationSlotsComputeRequiredXmin(bool already_locked);
extern void ReplicationSlotsComputeRequiredLSN(ReplicationSlotState* repl_slt_state);
extern XLogRecPtr ReplicationSlotsComputeConfirmedLSN(void);
extern void ReplicationSlotReportRestartLSN(void);
extern void StartupReplicationSlots();
extern void CheckPointReplicationSlots(void);
extern void SetDummyStandbySlotLsnInvalid(void);
extern XLogRecPtr slot_advance_confirmed_lsn(const char *slotname, XLogRecPtr target_lsn);
extern bool slot_reset_confirmed_lsn(const char *slotname, XLogRecPtr *start_lsn);
extern void CheckSlotRequirements(void);

/* SQL callable functions */
extern Datum pg_get_replication_slots(PG_FUNCTION_ARGS);
extern void create_logical_replication_slot(
    Name name, Name plugin, bool isDummyStandby, NameData* databaseName, char* str_tmp_lsn);
extern XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void);
extern bool ReplicationSlotsCountDBSlots(Oid dboid, int* nslots, int* nactive);
extern Datum pg_create_physical_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_create_physical_replication_slot_extern(PG_FUNCTION_ARGS);
extern Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_drop_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_get_replication_slot_name(PG_FUNCTION_ARGS);

/* slot redo */
extern void slot_redo(XLogReaderState* record);
extern void slot_desc(StringInfo buf, XLogReaderState* record);
extern void redo_slot_advance(const ReplicationSlotPersistentData* slotInfo);
extern void log_slot_advance(const ReplicationSlotPersistentData* slotInfo);
extern void log_slot_drop(const char* name);
extern void LogCheckSlot();
extern Size GetAllLogicalSlot(LogicalPersistentData*& LogicalSlot);
extern char* get_my_slot_name();
extern ObsArchiveConfig* formObsConfigFromStr(char *content, bool encrypted);
extern char *formObsConfigStringFromStruct(ObsArchiveConfig *obs_config);
extern bool is_archive_slot(ReplicationSlotPersistentData data);
extern void log_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content = NULL);
extern ReplicationSlot *getObsReplicationSlot();
extern void advanceObsSlot(XLogRecPtr restart_pos);
extern void redo_slot_reset_for_backup(const ReplicationSlotPersistentData *xlrec);
extern void markObsSlotOperate(int p_slot_num);

#endif /* SLOT_H */
