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
#include "replication/walprotocol.h"
#include "storage/lock/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"

extern const uint32 EXTRA_SLOT_VERSION_NUM;
#define ARCHIVE_PITR_PREFIX  "pitr_"

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

typedef enum ArchiveMediaType {
    ARCHIVE_NONE,
    ARCHIVE_OBS,
    ARCHIVE_NAS
} ArchiveMediaType;


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

typedef struct ArchiveConnConfig {
    char *obs_address;
    char *obs_bucket;
    char *obs_ak;
    char *obs_sk;
} ArchiveConnConfig;

typedef struct ArchiveConfig {
    ArchiveMediaType media_type;
    ArchiveConnConfig *conn_config;
    char *archive_prefix;
    bool is_recovery;
    bool vote_replicate_first;
} ArchiveConfig;

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
    ArchiveConfig* archive_config;
    bool is_recovery;
    char* extra_content;
} ReplicationSlot;

typedef struct ArchiveSlotConfig {
    char slotname[NAMEDATALEN];
    bool in_use;
    int slot_tline;
    slock_t mutex;
    ArchiveConfig archive_config;
}ArchiveSlotConfig;


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

/*
 * Interval in which standby snapshots are logged into the WAL stream, in
 * milliseconds.
 */
#define LOG_SNAPSHOT_INTERVAL_MS 15000

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
    XLogRecPtr quorum_min_required;
    XLogRecPtr min_tools_required;
    XLogRecPtr min_archive_slot_required;
    bool exist_in_use;
} ReplicationSlotState;
/*
 * Shared memory control area for all of replication slots.
 */
typedef struct LogicalPersistentData {
    int SlotNum;
    ReplicationSlotPersistentData replication_slots[FLEXIBLE_ARRAY_MEMBER];
} LogicalPersistentData;

typedef struct ArchiveTaskStatus {
    /* communicate between archive and walreceiver */
    /* archive thread set when archive done */
    bool pitr_finish_result;
    /*
     * walreceiver set when get task from walsender
     * 0 for no task
     * 1 walreceive get task from walsender and set it for archive thread
     * 2 archive thread set when task is done
     */
    volatile unsigned int pitr_task_status;
    /* for standby */
    ArchiveXlogMessage archive_task;
    XLogRecPtr archived_lsn;
    slock_t mutex;
    int sync_walsender_term;
    char slotname[NAMEDATALEN];
    bool in_use;
    Latch* archiver_latch;
}ArchiveTaskStatus;

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
extern bool IsLogicalReplicationSlot(const char *name);
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
extern const char* slot_type_name(uint8 subtype);
extern void redo_slot_advance(const ReplicationSlotPersistentData* slotInfo);
extern void log_slot_advance(const ReplicationSlotPersistentData* slotInfo, char* extra_content = NULL);
extern void log_slot_drop(const char* name);
extern void LogCheckSlot();
extern Size GetAllLogicalSlot(LogicalPersistentData*& LogicalSlot);
extern char* get_my_slot_name();
extern ArchiveConfig* formArchiveConfigFromStr(char *content, bool encrypted);
extern char *formArchiveConfigStringFromStruct(ArchiveConfig *obs_config);
extern bool is_archive_slot(ReplicationSlotPersistentData data);
extern void log_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content = NULL);
extern void AdvanceArchiveSlot(XLogRecPtr restart_pos);
extern void redo_slot_reset_for_backup(const ReplicationSlotPersistentData *xlrec);
extern void MarkArchiveSlotOperate();
extern void init_instance_slot();
extern void init_instance_slot_thread();
extern void add_archive_slot_to_instance(ReplicationSlot *slot);
extern void remove_archive_slot_from_instance_list(const char *slot_name);
extern ArchiveSlotConfig* find_archive_slot_from_instance_list(const char* name);
extern void release_archive_slot(ArchiveSlotConfig** archive_conf);
extern ArchiveSlotConfig* copy_archive_slot(ArchiveSlotConfig* archive_conf_origin);
extern ArchiveSlotConfig *getArchiveReplicationSlotWithName(const char *slot_name);
extern ArchiveSlotConfig *getArchiveRecoverySlotWithName(const char *slot_name);
extern ArchiveSlotConfig* getArchiveReplicationSlot();
extern ArchiveSlotConfig* GetArchiveRecoverySlot();
extern List *GetAllArchiveSlotsName();
extern List *GetAllRecoverySlotsName();
extern ArchiveTaskStatus* find_archive_task_status(const char* name);
extern ArchiveTaskStatus* find_archive_task_status(int *idx);
extern ArchiveTaskStatus* walreceiver_find_archive_task_status(unsigned int expected_pitr_task_status);
extern void get_hadr_cn_info(char* keyCn, bool* isExitKey, char* deleteCn, bool* isExitDelete, 
    ArchiveSlotConfig *archive_conf);


#endif /* SLOT_H */
