/* -------------------------------------------------------------------------
 *
 * slot.cpp
 *	   Replication slot management.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/slot.cpp
 *
 * NOTES
 *
 * Replication slots are used to keep state about replication streams
 * originating from this cluster.  Their primary purpose is to prevent the
 * premature removal of WAL or of old tuple versions in a manner that would
 * interfere with replication; they are also useful for monitoring purposes.
 * Slots need to be permanent (to allow restarts), crash-safe, and allocatable
 * on standbys (to support cascading setups).  The requirement that slots be
 * usable on standbys precludes storing them in the system catalogs.
 *
 * Each replication slot gets its own directory inside the $PGDATA/pg_replslot
 * directory. Inside that directory the state file will contain the slot's
 * own data. Additional data can be stored alongside that file if required.
 * While the server is running, the state data is also cached in memory for
 * efficiency.
 *
 * ReplicationSlotAllocationLock must be taken in exclusive mode to allocate
 * or free a slot. ReplicationSlotControlLock must be taken in shared mode
 * to iterate over the slots, and in exclusive mode to change the in_use flag
 * of a slot.  The remaining data in each slot is protected by its mutex.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/syncrep.h"
#include "storage/copydir.h"
#include "storage/smgr/fd.h"
#include "storage/file/fio_device.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "replication/walsender_private.h"

extern bool PMstateIsRun(void);

static void ReplicationSlotDropAcquired(void);

/* internal persistency functions */
static void RestoreSlotFromDisk(const char *name);
static void RecoverReplSlotFile(const ReplicationSlotOnDisk &cp, const char *name);
static void SaveSlotToPath(ReplicationSlot *slot, const char *path, int elevel);
static char *trim_str(char *str, int str_len, char sep);
static char *get_application_name(void);
static void ReleaseArchiveSlotInfo(ReplicationSlot *slot);
static int cmp_slot_lsn(const void *a, const void *b);
static int RenameReplslotPath(char *path1, char *path2);
static bool CheckExistReplslotPath(char *path);

/*
 * Report shared-memory space needed by ReplicationSlotShmemInit.
 */
Size ReplicationSlotsShmemSize(void)
{
    Size size = 0;

    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        return size;

    size = offsetof(ReplicationSlotCtlData, replication_slots);
    size = add_size(size, mul_size((Size)(uint32)g_instance.attr.attr_storage.max_replication_slots, sizeof(ReplicationSlot)));

    return size;
}

/*
 * Allocate and initialize walsender-related shared memory.
 */
void ReplicationSlotsShmemInit(void)
{
    bool found = false;

    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        return;

    t_thrd.slot_cxt.ReplicationSlotCtl = (ReplicationSlotCtlData *)ShmemInitStruct("ReplicationSlot Ctl",
                                                                                   ReplicationSlotsShmemSize(), &found);

    if (!found) {
        int i;
        errno_t rc = 0;

        /* First time through, so initialize */
        rc = memset_s(t_thrd.slot_cxt.ReplicationSlotCtl, ReplicationSlotsShmemSize(), 0, ReplicationSlotsShmemSize());
        securec_check(rc, "\0", "\0");

        for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

            /* everything else is zeroed by the memset above */
            SpinLockInit(&slot->mutex);
            slot->io_in_progress_lock = LWLockAssign(LWTRANCHE_REPLICATION_SLOT);
            rc = pthread_condattr_init(&slot->slotAttr);
            if (rc != 0) {
                elog(FATAL, "Fail to init conattr for replication slot");
            }
            rc = pthread_condattr_setclock(&slot->slotAttr, CLOCK_MONOTONIC);
            if (rc != 0) {
                elog(FATAL, "Fail to setclock replication slot");
            }
            rc = pthread_cond_init(&slot->active_cv, &slot->slotAttr);
            if (rc != 0) {
                elog(FATAL, "Fail to init cond for replication slot");
            }
            slot->active_mutex = PTHREAD_MUTEX_INITIALIZER;
        }
    }
}

/*
 * Check whether the passed slot name is valid and report errors at elevel.
 *
 * Slot names may consist out of [a-z0-9_]{1,NAMEDATALEN-1} which should allow
 * the name to be used as a directory name on every supported OS.
 *
 * Returns whether the directory name is valid or not if elevel < ERROR.
 */
bool ReplicationSlotValidateName(const char *name, int elevel)
{
    const char *cp = NULL;

    if (name == NULL || strlen(name) == 0) {
        ereport(elevel, (errcode(ERRCODE_INVALID_NAME), errmsg("replication slot name should not be NULL.")));
        return false;
    }

    if (strlen(name) >= NAMEDATALEN) {
        ereport(elevel, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("replication slot name \"%s\" is too long", name)));
        return false;
    }

    if ((strlen(name) == 1 && name[0] == '.') ||
        (strlen(name) == strlen("..") && strncmp(name, "..", strlen("..")) == 0)) {
        ereport(elevel, (errcode(ERRCODE_INVALID_NAME),
            errmsg("'.' and '..' are not allowed to be slot names independently.")));
    }

    for (cp = name; *cp; cp++) {
        if (!((*cp >= 'a' && *cp <= 'z') || (*cp >= '0' && *cp <= '9') || (*cp == '_') || (*cp == '?') ||
            (*cp == '.') || (*cp == '-'))) {
            ereport(elevel,
                    (errcode(ERRCODE_INVALID_NAME),
                     errmsg("replication slot name \"%s\" contains invalid character", name),
                     errhint("Replication slot names may only contain lower letters, "
                             "numbers and the 4 following special characters: '_', '?', '.', '-'.\n"
                             "'.' and '..' are also not allowed to be slot names independently.")));
            return false;
        }
    }

    if (strncmp(name, "gs_roach", strlen("gs_roach")) == 0 &&
        strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0 && !RecoveryInProgress()) {
        ereport(ERROR,
               (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("replication slot name starting with gs_roach is internally reserverd")));
    }

    return true;
}

/*
 * Check whether the passed string contains danerous characters and report errors.
 */
void ValidateInputString(const char* inputString)
{
    if (inputString == NULL || strlen(inputString) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("inputString should not be NULL.")));
    }

    if (strlen(inputString) >= NAMEDATALEN) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("inputString \"%s\" is too long", inputString)));
    }

    const char *danger_character_list[] = { ";", "`", "\\", "'", "\"", ">", "<", "&", "|", "!", "\n", NULL };
    int i = 0;

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(inputString, danger_character_list[i]) != NULL) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_NAME),
                     errmsg("inputString \"%s\" contains dangerous character", inputString),
                     errhint("the dangerous character is %s", danger_character_list[i])));
        }
    }

    if (strncmp(inputString, "gs_roach", strlen("gs_roach")) == 0 &&
        strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0 && !RecoveryInProgress()) {
        ereport(ERROR,
               (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("replication slot name starting with gs_roach is internally reserverd")));
    }

    return;
}

/*
 * reset backup slot if created from in-use one.
 * confirm_flush is not reset, so that there is chance to clean residual delayed ddl recycle.
 */
void slot_reset_for_backup(ReplicationSlotPersistency persistency, bool isDummyStandby,
                           Oid databaseId, XLogRecPtr restart_lsn)
{
    volatile ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

    SpinLockAcquire(&slot->mutex);
    SET_SLOT_PERSISTENCY(slot->data, persistency);
    slot->data.xmin = InvalidTransactionId;
    slot->effective_xmin = InvalidTransactionId;
    slot->effective_catalog_xmin = InvalidTransactionId;
    slot->data.database = databaseId;
    slot->data.restart_lsn = restart_lsn;
    slot->data.isDummyStandby = isDummyStandby;
    SpinLockRelease(&slot->mutex);
}

/*
 * reset backup slot during recovery for backup.
 */
void redo_slot_reset_for_backup(const ReplicationSlotPersistentData *xlrec)
{
    if (GET_SLOT_PERSISTENCY(*xlrec) != RS_BACKUP) {
        return;
    }

    ReplicationSlotAcquire(xlrec->name.data, xlrec->isDummyStandby);
    int rc = memcpy_s(&t_thrd.slot_cxt.MyReplicationSlot->data, sizeof(ReplicationSlotPersistentData), xlrec,
                      sizeof(ReplicationSlotPersistentData));
    securec_check(rc, "\0", "\0");
    t_thrd.slot_cxt.MyReplicationSlot->effective_xmin = t_thrd.slot_cxt.MyReplicationSlot->data.xmin;
    t_thrd.slot_cxt.MyReplicationSlot->effective_catalog_xmin = t_thrd.slot_cxt.MyReplicationSlot->data.catalog_xmin;
    /* ok, slot is now fully created, mark it as persistent */
    ReplicationSlotMarkDirty();
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN(NULL);
    ReplicationSlotSave();
    ReplicationSlotRelease();
}

/*
 * Drop the old hadr replication slot when hadr main standby is changed.
 */
static void DropOldHadrReplicationSlot(const char *name)
{
    if (strstr(name, "_hadr") == NULL) {
        return;
    }

    ReplicationSlot *slot;
    char dropSlotName[NAMEDATALEN] = {0};
    errno_t rc;

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (slot->active && strstr(NameStr(slot->data.name), "_hadr") != NULL) {
            ereport(ERROR, (errmsg("only one replication slot for main standby is allowed, "
                "failed to create \"%s\"", name)));
        }
        if (slot->in_use && !slot->active &&
            strcmp(name, NameStr(slot->data.name)) != 0 &&
            strstr(NameStr(slot->data.name), "_hadr") != NULL) {
            rc = strcpy_s(dropSlotName, NAMEDATALEN, NameStr(slot->data.name));
            securec_check_ss(rc, "\0", "\0");
            break;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    if (strlen(dropSlotName) > 0) {
        ReplicationSlotDrop(dropSlotName);
    }
}

static void ReleaseArchiveSlotInfo(ReplicationSlot *slot)
{
    pfree_ext(slot->extra_content);
    if (slot->archive_config != NULL) {
        if (slot->archive_config->conn_config != NULL) {
            pfree_ext(slot->archive_config->conn_config->obs_address);
            pfree_ext(slot->archive_config->conn_config->obs_bucket);
            pfree_ext(slot->archive_config->conn_config->obs_ak);
            pfree_ext(slot->archive_config->conn_config->obs_sk);
            pfree_ext(slot->archive_config->conn_config);
        }
        pfree_ext(slot->archive_config->archive_prefix);
    }
    pfree_ext(slot->archive_config);
}

static bool HasArchiveSlot()
{
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->in_use && s->data.database == InvalidOid  && GET_SLOT_PERSISTENCY(s->data) != RS_BACKUP &&
            s->extra_content != NULL) {
            return true;
        }
    }
    return false;
}

/*
 * Create a new replication slot and mark it as used by this backend.
 *
 * name: Name of the slot
 * db_specific: logical decoding is db specific; if the slot is going to
 *     be used for that pass true, otherwise false.
 */
void ReplicationSlotCreate(const char *name, ReplicationSlotPersistency persistency, bool isDummyStandby,
                           Oid databaseId, XLogRecPtr restart_lsn, XLogRecPtr confirmed_lsn, char* extra_content,
                           bool encrypted)
{
    ReplicationSlot *slot = NULL;
    int i;
    errno_t rc = 0;
    int slot_idx = -1;

    Assert(t_thrd.slot_cxt.MyReplicationSlot == NULL);

    (void)ReplicationSlotValidateName(name, ERROR);

    /* Drop old hadr replication slot for streaming disaster cluster. */
    DropOldHadrReplicationSlot(name);

    /*
     * If some other backend ran this code currently with us, we'd likely
     * both allocate the same slot, and that would be bad.  We'd also be
     * at risk of missing a name collision.  Also, we don't want to try to
     * create a new slot while somebody's busy cleaning up an old one, because
     * we might both be monkeying with the same directory.
     */
    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    /*
     * Check for name collision, and identify an allocatable slot.  We need
     * to hold ReplicationSlotControlLock in shared mode for this, so that
     * nobody else can change the in_use flags while we're looking at them.
     */
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    if (extra_content != NULL && strlen(extra_content) != 0 && HasArchiveSlot()) {
        ereport(ERROR, (errmsg("currently multi-archive replication slot isn't supported")));
    }
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0) {
            LWLockRelease(ReplicationSlotControlLock);
            LWLockRelease(ReplicationSlotAllocationLock);
            if (databaseId == InvalidOid)
                /* For physical replication slot, report WARNING to let libpqrcv continue */
                ereport(WARNING,
                        (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("replication slot \"%s\" already exists", name)));
            else
                /* For logical replication slot, report ERROR followed PG9.4 */
                ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("replication slot \"%s\" already exists", name)));
            ReplicationSlotAcquire(name, isDummyStandby);
            if (persistency == RS_BACKUP) {
                slot_reset_for_backup(persistency, isDummyStandby, databaseId, restart_lsn);
            }
            return;
        }
        if (!s->in_use && slot == NULL) {
            slot = s;
            slot_idx = i;
        }
    }
    /* If all slots are in use, we're out of luck. */
    if (slot == NULL) {
        for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

            if (s->in_use) {
                ereport(LOG, (errmsg("Slot Name: %s", s->data.name.data)));
            }
        }

        LWLockRelease(ReplicationSlotControlLock);
        LWLockRelease(ReplicationSlotAllocationLock);
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("all replication slots are in use"),
                        errhint("Free one or increase max_replication_slots.")));
    }

    LWLockRelease(ReplicationSlotControlLock);

    /*
     * Since this slot is not in use, nobody should be looking at any
     * part of it other than the in_use field unless they're trying to allocate
     * it.  And since we hold ReplicationSlotAllocationLock, nobody except us
     * can be doing that.  So it's safe to initialize the slot.
     */
    Assert(!slot->in_use);
    /* first initialize persistent data */
    rc = memset_s(&slot->data, sizeof(ReplicationSlotPersistentData), 0, sizeof(ReplicationSlotPersistentData));
    securec_check(rc, "\0", "\0");

    SET_SLOT_PERSISTENCY(slot->data, persistency);
    slot->data.xmin = InvalidTransactionId;
    slot->effective_xmin = InvalidTransactionId;
    slot->effective_catalog_xmin = InvalidTransactionId;
    rc = strncpy_s(NameStr(slot->data.name), NAMEDATALEN, name, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");
    NameStr(slot->data.name)[NAMEDATALEN - 1] = '\0';
    slot->data.database = databaseId;
    slot->data.restart_lsn = restart_lsn;
    slot->data.confirmed_flush = confirmed_lsn;
    slot->data.isDummyStandby = isDummyStandby;
    if (extra_content != NULL && strlen(extra_content) != 0) {
        MemoryContext curr;
        curr = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
        ReleaseArchiveSlotInfo(slot);
        slot->archive_config = formArchiveConfigFromStr(extra_content, encrypted);
        slot->extra_content = formArchiveConfigStringFromStruct(slot->archive_config);
        if (slot->extra_content == NULL) {
            LWLockRelease(ReplicationSlotControlLock);
            LWLockRelease(ReplicationSlotAllocationLock);
            ereport(ERROR, (errmsg("Failed get slot extra content form struct")));
        }
        SET_SLOT_EXTRA_DATA_LENGTH(slot->data, strlen(slot->extra_content));
        MemoryContextSwitchTo(curr);
    }

    /*
     * Create the slot on disk.  We haven't actually marked the slot allocated
     * yet, so no special cleanup is required if this errors out.
     */
    CreateSlotOnDisk(slot);

    /*
     * We need to briefly prevent any other backend from iterating over the
     * slots while we flip the in_use flag. We also need to set the active
     * flag while holding the control_lock as otherwise a concurrent
     * SlotAcquire() could acquire the slot as well.
     */
    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);

    slot->in_use = true;

    /* We can now mark the slot active, and that makes it our slot. */
    {
        volatile ReplicationSlot *vslot = slot;

        SpinLockAcquire(&slot->mutex);
        vslot->active = true;
        SpinLockRelease(&slot->mutex);
        t_thrd.slot_cxt.MyReplicationSlot = slot;
        if (t_thrd.walsender_cxt.MyWalSnd != NULL && t_thrd.walsender_cxt.MyWalSnd->pid != 0) {
            t_thrd.walsender_cxt.MyWalSnd->slot_idx = slot_idx;
        }
    }

    LWLockRelease(ReplicationSlotControlLock);

    /*
     * Now that the slot has been marked as in_use and in_active, it's safe to
     * let somebody else try to allocate a slot.
     */
    LWLockRelease(ReplicationSlotAllocationLock);

    /* Let everybody know we've modified this slot. */
    (void)pthread_mutex_lock(&slot->active_mutex);
    pthread_cond_broadcast(&slot->active_cv);
    (void)pthread_mutex_unlock(&slot->active_mutex);
}

/*
 * @param[IN] allowDrop: only used in dropping a logical replication slot.
 * Find a previously created slot and mark it as used by this backend.
 */
void ReplicationSlotAcquire(const char *name, bool isDummyStandby, bool allowDrop, bool nowait)
{
    ReplicationSlot *slot = NULL;
    int i;
    bool active = false;
    int slot_idx = -1;
    struct timespec time_to_wait;

retry:
    Assert(t_thrd.slot_cxt.MyReplicationSlot == NULL);

    CHECK_FOR_INTERRUPTS();

    ReplicationSlotValidateName(name, ERROR);

    /* Search for the named slot and mark it active if we find it. */
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0) {
            volatile ReplicationSlot *vslot = s;

            SpinLockAcquire(&s->mutex);
            active = vslot->active;
            vslot->active = true;
            SpinLockRelease(&s->mutex);
            slot = s;
            slot_idx = i;
            break;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    /* If we did not find the slot or it was already active, error out. */
    if (slot == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("replication slot \"%s\" does not exist", name)));
    /* We allow dropping active logical replication slots on standby or for subscription in opengauss. */
    if (active && nowait) {
        if (((slot->data.database != InvalidOid
#ifndef ENABLE_MULTIPLE_NODES
            && !allowDrop
#endif
            ) || isDummyStandby != slot->data.isDummyStandby))
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("replication slot \"%s\" is already active", name)));
        else {
            ereport(WARNING,
                    (errcode(ERRCODE_OBJECT_IN_USE), errmsg("replication slot \"%s\" is already active", name)));
        }
    } else if (active && !nowait) {
        (void)pthread_mutex_lock(&slot->active_mutex);
        clock_gettime(CLOCK_MONOTONIC, &time_to_wait);
        time_to_wait.tv_sec += SECS_PER_MINUTE / 2;
        pgstat_report_waitevent(WAIT_EVENT_REPLICATION_SLOT_DROP);
        (void)pthread_cond_timedwait(&slot->active_cv, &slot->active_mutex, &time_to_wait);
        pgstat_report_waitevent(WAIT_EVENT_END);
        (void)pthread_mutex_unlock(&slot->active_mutex);

        goto retry;
    }

    /* Let everybody know we've modified this slot */
    (void)pthread_mutex_lock(&slot->active_mutex);
    pthread_cond_broadcast(&slot->active_cv);
    (void)pthread_mutex_unlock(&slot->active_mutex);

    if (slot->data.database != InvalidOid) {
        slot->candidate_restart_lsn = InvalidXLogRecPtr;
        slot->candidate_restart_valid = InvalidXLogRecPtr;
        slot->candidate_xmin_lsn = InvalidXLogRecPtr;
        slot->candidate_catalog_xmin = InvalidTransactionId;
    }

    /* We made this slot active, so it's ours now. */
    t_thrd.slot_cxt.MyReplicationSlot = slot;
    if (t_thrd.walsender_cxt.MyWalSnd != NULL && t_thrd.walsender_cxt.MyWalSnd->pid != 0) {
        t_thrd.walsender_cxt.MyWalSnd->slot_idx = slot_idx;
    }
}

/*
 * Find out if we have an active slot by slot name
 */
bool IsReplicationSlotActive(const char *name)
{
    bool active = false;

    Assert(t_thrd.slot_cxt.MyReplicationSlot == NULL);

    ReplicationSlotValidateName(name, ERROR);

    /* Search for the named slot to identify whether it is active or not. */
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->active && strcmp(name, NameStr(s->data.name)) == 0) {
            active = true;
            break;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    return active;
}

/*
 * Find out if we have an logical replication slot by slot name
 */
bool IsLogicalReplicationSlot(const char *name)
{
    bool isLogical = false;

    Assert(t_thrd.slot_cxt.MyReplicationSlot == NULL);

    ReplicationSlotValidateName(name, ERROR);

    /* Search for the named slot to identify whether it is a logical replication slot or not. */
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0 &&
            s->data.database != InvalidOid ) {
            isLogical = true;
            break;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    return isLogical;
}

/*
 * Find out if we have a slot by slot name
 */
bool ReplicationSlotFind(const char *name)
{
    bool hasSlot = false;
    ReplicationSlotValidateName(name, ERROR);

    /* Search for the named slot and mark it active if we find it. */
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0) {
            hasSlot = true;
            break;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);
    return hasSlot;
}

/*
 * Release a replication slot, this or another backend can ReAcquire it
 * later. Resources this slot requires will be preserved.
 */
void ReplicationSlotRelease(void)
{
    ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

    if (slot == NULL || !slot->active) {
        t_thrd.slot_cxt.MyReplicationSlot = NULL;
        return;
    }

    if (GET_SLOT_PERSISTENCY(slot->data) == RS_EPHEMERAL) {
        /*
         * Delete the slot. There is no !PANIC case where this is allowed to
         * fail, all that may happen is an incomplete cleanup of the on-disk
         * data.
         */
        ReplicationSlotDropAcquired();
    }

    /*
     * If slot needed to temporarily restrain both data and catalog xmin to
     * create the catalog snapshot, remove that temporary constraint.
     * Snapshots can only be exported while the initial snapshot is still
     * acquired.
     */
    if (!TransactionIdIsValid(slot->data.xmin) && TransactionIdIsValid(slot->effective_xmin)) {
        SpinLockAcquire(&slot->mutex);
        slot->effective_xmin = InvalidTransactionId;
        SpinLockRelease(&slot->mutex);
        ReplicationSlotsComputeRequiredXmin(false);
    }

    if (GET_SLOT_PERSISTENCY(slot->data) != RS_EPHEMERAL) {
        /* Mark slot inactive.  We're not freeing it, just disconnecting. */
        volatile ReplicationSlot *vslot = slot;
        SpinLockAcquire(&slot->mutex);
        vslot->active = false;
        SpinLockRelease(&slot->mutex);
        (void)pthread_mutex_lock(&slot->active_mutex);
        pthread_cond_broadcast(&slot->active_cv);
        (void)pthread_mutex_unlock(&slot->active_mutex);
    }

    t_thrd.slot_cxt.MyReplicationSlot = NULL;
    /* might not have been set when we've been a plain slot */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    t_thrd.pgxact->xmin = InvalidTransactionId;
    t_thrd.pgxact->vacuumFlags &= ~PROC_IN_LOGICAL_DECODING;
    t_thrd.proc->exrto_read_lsn = 0;
    t_thrd.proc->exrto_min = 0;
    t_thrd.proc->exrto_gen_snap_time = 0;
    LWLockRelease(ProcArrayLock);
}

/*
 * Permanently drop replication slot identified by the passed in name.
 */
void ReplicationSlotDrop(const char *name, bool for_backup, bool nowait)
{
    bool isLogical = false;
    bool is_archive_slot = false;

    (void)ReplicationSlotValidateName(name, ERROR);
    /*
     * If some other backend ran this code currently with us, we might both
     * try to free the same slot at the same time.  Or we might try to delete
     * a slot with a certain name while someone else was trying to create a
     * slot with the same name.
     */
    Assert(t_thrd.slot_cxt.MyReplicationSlot == NULL);

    /* We allow dropping active logical replication slots on standby in opengauss. */
    ReplicationSlotAcquire(name, false, RecoveryInProgress(), nowait);
    if (t_thrd.slot_cxt.MyReplicationSlot->archive_config != NULL) {
        is_archive_slot = true;
    }

    isLogical = (t_thrd.slot_cxt.MyReplicationSlot->data.database != InvalidOid);
    ReplicationSlotDropAcquired();
    if (PMstateIsRun() && !RecoveryInProgress()) {
        if (isLogical) {
            log_slot_drop(name);
        } else if (u_sess->attr.attr_sql.enable_slot_log || for_backup) {
            log_slot_drop(name);
        }
    }
    if (is_archive_slot) {
        MarkArchiveSlotOperate();
        LWLockAcquire(DropArchiveSlotLock, LW_EXCLUSIVE);
        remove_archive_slot_from_instance_list(name);
        LWLockRelease(DropArchiveSlotLock);
    }
}
/*
 * Permanently drop the currently acquired replication slot which will be
 * released by the point this function returns.
 */
static void ReplicationSlotDropAcquired(void)
{
    char path[MAXPGPATH];
    char tmppath[MAXPGPATH];
    bool is_archive_slot = (t_thrd.slot_cxt.MyReplicationSlot->archive_config != NULL);
    ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

    Assert(t_thrd.slot_cxt.MyReplicationSlot != NULL);
    /* slot isn't acquired anymore */
    t_thrd.slot_cxt.MyReplicationSlot = NULL;

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);

    /*
     * If some other backend ran this code concurrently with us, we might try
     * to delete a slot with a certain name while someone else was trying to
     * create a slot with the same name.
     */
    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    /* Generate pathnames. */
    int nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s", replslot_path, NameStr(slot->data.name));
    securec_check_ss(nRet, "\0", "\0");

    nRet = snprintf_s(tmppath, sizeof(tmppath), MAXPGPATH - 1, "%s/%s.tmp", replslot_path, NameStr(slot->data.name));
    securec_check_ss(nRet, "\0", "\0");

    /*
* Rename the slot directory on disk, so that we'll no longer recognize
* this as a valid slot.  Note that if this fails, we've got to mark the
* slot inactive before bailing out.  If we're dropping a ephemeral slot,
* we better never fail hard as the caller won't expect the slot to
* survive and this might get called during error handling.

 */
    if (RenameReplslotPath(path, tmppath) == 0) {
        /*
         * We need to fsync() the directory we just renamed and its parent to
         * make sure that our changes are on disk in a crash-safe fashion.  If
         * fsync() fails, we can't be sure whether the changes are on disk or
         * not.  For now, we handle that by panicking;
         * StartupReplicationSlots() will try to straighten it out after
         * restart.
         */
        START_CRIT_SECTION();
        fsync_fname(tmppath, true);
        fsync_fname(replslot_path, true);
        END_CRIT_SECTION();
    } else {
        volatile ReplicationSlot *vslot = slot;

        bool fail_softly = GET_SLOT_PERSISTENCY(slot->data) == RS_EPHEMERAL ||
            (GET_SLOT_PERSISTENCY(slot->data) == RS_PERSISTENT && slot->extra_content != NULL);
        SpinLockAcquire(&slot->mutex);
        vslot->active = false;
        SpinLockRelease(&slot->mutex);

        (void)pthread_mutex_lock(&slot->active_mutex);
        pthread_cond_broadcast(&slot->active_cv);
        (void)pthread_mutex_unlock(&slot->active_mutex);

        ereport(fail_softly ? WARNING : ERROR,
                (errcode_for_file_access(), errmsg("could not rename \"%s\" to \"%s\": %m", path, tmppath)));
    }

    /*
     * The slot is definitely gone.  Lock out concurrent scans of the array
     * long enough to kill it.  It's OK to clear the active flag here without
     * grabbing the mutex because nobody else can be scanning the array here,
     * and nobody can be attached to this slot and thus access it without
     * scanning the array.
     */
    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
    slot->active = false;
    slot->in_use = false;
    if (is_archive_slot) {
        ReleaseArchiveSlotInfo(slot);
    }
    LWLockRelease(ReplicationSlotControlLock);
    (void)pthread_mutex_lock(&slot->active_mutex);
    pthread_cond_broadcast(&slot->active_cv);
    (void)pthread_mutex_unlock(&slot->active_mutex);

    /*
     * Slot is dead and doesn't prevent resource removal anymore, recompute
     * limits.
     */
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN(NULL);

    /* reset the slot_idx to invalid */
    if (t_thrd.walsender_cxt.MyWalSnd != NULL && t_thrd.walsender_cxt.MyWalSnd->slot_idx != -1) {
        t_thrd.walsender_cxt.MyWalSnd->slot_idx = -1;
    }

    /*
     * If removing the directory fails, the worst thing that will happen is
     * that the user won't be able to create a new slot with the same name
     * until the next server restart.  We warn about it, but that's all.
     */
    if (!rmtree(tmppath, true))
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove directory \"%s\"", tmppath)));

    /*
     * We release this at the very end, so that nobody starts trying to create
     * a slot while we're still cleaning up the detritus of the old one.
     */
    LWLockRelease(ReplicationSlotAllocationLock);
}

/*
 * Serialize the currently acquired slot's state from memory to disk, thereby
 * guaranteeing the current state will survive a crash.
 */
void ReplicationSlotSave(void)
{
    char path[MAXPGPATH];
    int nRet = 0;

    Assert(t_thrd.slot_cxt.MyReplicationSlot != NULL);

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);
    nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", replslot_path,
        NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name));
    securec_check_ss(nRet, "\0", "\0");
    if (unlikely(CheckFileExists(path) == FILE_NOT_EXIST)) {
        CreateSlotOnDisk(t_thrd.slot_cxt.MyReplicationSlot);
    }

    SaveSlotToPath(t_thrd.slot_cxt.MyReplicationSlot, path, ERROR);
}

/*
 * Signal that it would be useful if the currently acquired slot would be
 * flushed out to disk.
 *
 * Note that the actual flush to disk can be delayed for a long time, if
 * required for correctness explicitly do a ReplicationSlotSave().
 */
void ReplicationSlotMarkDirty(void)
{
    Assert(t_thrd.slot_cxt.MyReplicationSlot != NULL);

    {
        volatile ReplicationSlot *vslot = t_thrd.slot_cxt.MyReplicationSlot;

        SpinLockAcquire(&vslot->mutex);
        t_thrd.slot_cxt.MyReplicationSlot->just_dirtied = true;
        t_thrd.slot_cxt.MyReplicationSlot->dirty = true;
        SpinLockRelease(&vslot->mutex);
    }
}

/*
 * Set dummy standby replication slot's lsn invalid
 */
void SetDummyStandbySlotLsnInvalid(void)
{
    Assert(t_thrd.slot_cxt.MyReplicationSlot != NULL);

    volatile ReplicationSlot *vslot = t_thrd.slot_cxt.MyReplicationSlot;

    Assert(vslot->data.isDummyStandby);

    if (!XLByteEQ(vslot->data.restart_lsn, InvalidXLogRecPtr)) {
        SpinLockAcquire(&vslot->mutex);
        vslot->data.restart_lsn = 0;
        SpinLockRelease(&vslot->mutex);

        ReplicationSlotMarkDirty();
        ReplicationSlotsComputeRequiredLSN(NULL);
    }
}

/*
 * Convert a slot that's marked as RS_DROP_ON_ERROR to a RS_PERSISTENT slot,
 * guaranteeing it will be there after a eventual crash.
 */
void ReplicationSlotPersist(void)
{
    ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

    Assert(slot != NULL);
    Assert(GET_SLOT_PERSISTENCY(slot->data) != RS_PERSISTENT);
    LWLockAcquire(LogicalReplicationSlotPersistentDataLock, LW_EXCLUSIVE);

    {
        volatile ReplicationSlot *vslot = slot;

        SpinLockAcquire(&slot->mutex);
        SET_SLOT_PERSISTENCY(vslot->data, RS_PERSISTENT);
        SpinLockRelease(&slot->mutex);
    }
    LWLockRelease(LogicalReplicationSlotPersistentDataLock);

    ReplicationSlotMarkDirty();
    ReplicationSlotSave();
}

static bool IfIgnoreStandbyXmin(TransactionId xmin, TimestampTz last_xmin_change_time)
{
    if (u_sess->attr.attr_storage.ignore_feedback_xmin_window <= 0) {
        return false;
    }

    TimestampTz nowTime = GetCurrentTimestamp();
    if (timestamptz_cmp_internal(nowTime, TimestampTzPlusMilliseconds(last_xmin_change_time,
        u_sess->attr.attr_storage.ignore_feedback_xmin_window)) >= 0) {
        /* If the xmin is older than recentGlobalXmin, ignore it */
        TransactionId recentGlobalXmin = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
        if (xmin < recentGlobalXmin) {
            return true;
        }
    }
    return false;
}

/*
 * Compute the oldest xmin across all slots and store it in the ProcArray.
 *
 * If already_locked is true, ProcArrayLock has already been acquired
 * exclusively.
 */
void ReplicationSlotsComputeRequiredXmin(bool already_locked)
{
    int i;
    TransactionId agg_xmin = InvalidTransactionId;
    TransactionId agg_catalog_xmin = InvalidTransactionId;

    Assert(t_thrd.slot_cxt.ReplicationSlotCtl != NULL);
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        TransactionId effective_xmin;
        TransactionId effective_catalog_xmin;
        TimestampTz last_xmin_change_time;

        if (!s->in_use)
            continue;

        {
            volatile ReplicationSlot *vslot = s;

            SpinLockAcquire(&s->mutex);
            effective_xmin = vslot->effective_xmin;
            effective_catalog_xmin = vslot->effective_catalog_xmin;
            last_xmin_change_time = vslot->last_xmin_change_time;
            SpinLockRelease(&s->mutex);
        }

        /* check the data xmin */
        if (TransactionIdIsValid(effective_xmin) && !IfIgnoreStandbyXmin(effective_xmin, last_xmin_change_time) &&
            (!TransactionIdIsValid(agg_xmin) || TransactionIdPrecedes(effective_xmin, agg_xmin)))
            agg_xmin = effective_xmin;

        if (s->data.database == InvalidOid) {
            continue;
        }
        /* check the catalog xmin */
        if (TransactionIdIsValid(effective_catalog_xmin) &&
            (!TransactionIdIsValid(agg_catalog_xmin) ||
             TransactionIdPrecedes(effective_catalog_xmin, agg_catalog_xmin)))
            agg_catalog_xmin = effective_catalog_xmin;
    }
    LWLockRelease(ReplicationSlotControlLock);

    ProcArraySetReplicationSlotXmin(agg_xmin, agg_catalog_xmin, already_locked);
}

static void CalculateMinAndMaxRequiredPtr(ReplicationSlot *s, XLogRecPtr* standby_slots_list, int i,
        XLogRecPtr restart_lsn, XLogRecPtr* min_tools_required, XLogRecPtr* min_required,
        XLogRecPtr* min_archive_restart_lsn, XLogRecPtr* max_required)
{
    if (s->data.database == InvalidOid && GET_SLOT_PERSISTENCY(s->data) != RS_BACKUP && s->extra_content == NULL) {
        standby_slots_list[i] = restart_lsn;
    } else {
        XLogRecPtr min_tools_lsn = restart_lsn;
        if (s->extra_content == NULL && (!XLByteEQ(min_tools_lsn, InvalidXLogRecPtr)) &&
            (XLByteEQ(*min_tools_required, InvalidXLogRecPtr) || XLByteLT(min_tools_lsn, *min_tools_required))) {
            *min_tools_required = min_tools_lsn;
        }
    }
    if (s->extra_content != NULL) {
        if ((!XLByteEQ(restart_lsn, InvalidXLogRecPtr)) &&
            (XLByteEQ(*min_archive_restart_lsn, InvalidXLogRecPtr)
            || XLByteLT(restart_lsn, *min_archive_restart_lsn))) {
            *min_archive_restart_lsn = restart_lsn;
        }
    }
    if ((!XLByteEQ(restart_lsn, InvalidXLogRecPtr)) &&
        (XLByteEQ(*min_required, InvalidXLogRecPtr) || XLByteLT(restart_lsn, *min_required))) {
        *min_required = restart_lsn;
    }

    if (XLByteLT(*max_required, restart_lsn)) {
        *max_required = restart_lsn;
    }
}

/*
 * Compute the oldest restart LSN across all slots and inform xlog module.
 */
void ReplicationSlotsComputeRequiredLSN(ReplicationSlotState *repl_slt_state)
{
    int i;
    XLogRecPtr min_required = InvalidXLogRecPtr;
    XLogRecPtr min_tools_required = InvalidXLogRecPtr;
    XLogRecPtr max_required = InvalidXLogRecPtr;
    XLogRecPtr standby_slots_list[g_instance.attr.attr_storage.max_replication_slots];
    XLogRecPtr min_archive_restart_lsn = InvalidXLogRecPtr;
    XLogRecPtr max_confirmed_lsn = InvalidXLogRecPtr;
    bool in_use = false;
    errno_t rc = EOK;

    if (g_instance.attr.attr_storage.max_replication_slots == 0) {
        return;
    }

    Assert(t_thrd.slot_cxt.ReplicationSlotCtl != NULL);
    /* server_mode must be set before computing LSN */
    load_server_mode();

    rc = memset_s(standby_slots_list, sizeof(standby_slots_list), 0, sizeof(standby_slots_list));
    securec_check(rc, "\0", "\0");

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        volatile ReplicationSlot *vslot = s;
        SpinLockAcquire(&s->mutex);
        XLogRecPtr restart_lsn;
        bool isNoNeedCheck = (t_thrd.xlog_cxt.server_mode != PRIMARY_MODE &&
            !(t_thrd.xlog_cxt.server_mode == STANDBY_MODE && t_thrd.xlog_cxt.is_hadr_main_standby) &&
            t_thrd.xlog_cxt.server_mode != PENDING_MODE && s->data.database == InvalidOid &&
            GET_SLOT_PERSISTENCY(s->data) != RS_BACKUP);
        if (isNoNeedCheck) {
            goto lock_release;
        }

        if (!s->in_use) {
            goto lock_release;
        }

        in_use = true;
        restart_lsn = vslot->data.restart_lsn;

        SpinLockRelease(&s->mutex);
        CalculateMinAndMaxRequiredPtr(s, standby_slots_list, i, restart_lsn, &min_tools_required, &min_required,
            &min_archive_restart_lsn, &max_required);
#ifndef ENABLE_MULTIPLE_NODES
        if (g_instance.attr.attr_storage.enable_save_confirmed_lsn &&
            GET_SLOT_PERSISTENCY(s->data) == RS_PERSISTENT &&
            XLogRecPtrIsValid(s->data.confirmed_flush) &&
            XLByteLT(max_confirmed_lsn, s->data.confirmed_flush)) {
            max_confirmed_lsn = s->data.confirmed_flush;
        }
#endif
        continue;
    lock_release:
        SpinLockRelease(&s->mutex);
    }
    LWLockRelease(ReplicationSlotControlLock);

    if(InvalidXLogRecPtr != min_required)
        XLogSetReplicationSlotMinimumLSN(min_required);
    if(InvalidXLogRecPtr != max_required)
        XLogSetReplicationSlotMaximumLSN(max_required);
    qsort(standby_slots_list, g_instance.attr.attr_storage.max_replication_slots, sizeof(XLogRecPtr), cmp_slot_lsn);
    if (repl_slt_state != NULL) {
        repl_slt_state->min_required = min_required;
        repl_slt_state->max_required = max_required;
        if (*standby_slots_list == InvalidXLogRecPtr || t_thrd.syncrep_cxt.SyncRepConfig == NULL) {
            repl_slt_state->quorum_min_required = InvalidXLogRecPtr;
            repl_slt_state->min_tools_required = min_tools_required;
            repl_slt_state->min_archive_slot_required = min_archive_restart_lsn;
            repl_slt_state->exist_in_use = in_use;
            return;
        }
        if (t_thrd.syncrep_cxt.SyncRepConfig != NULL) {
            i = Min(t_thrd.syncrep_cxt.SyncRepMaxPossib, g_instance.attr.attr_storage.max_replication_slots) - 1;
            for ( ; i >= 0; i--) {
                if (standby_slots_list[i] != InvalidXLogRecPtr) {
                    repl_slt_state->quorum_min_required = standby_slots_list[i];
                    break;
                }
            }
        }
#ifndef ENABLE_MULTIPLE_NODES
        if (g_instance.attr.attr_storage.enable_save_confirmed_lsn &&
            XLogRecPtrIsValid(max_confirmed_lsn) &&
            XLByteLT(max_confirmed_lsn, repl_slt_state->quorum_min_required)) {
            repl_slt_state->quorum_min_required = max_confirmed_lsn;
        }
#endif
        repl_slt_state->min_tools_required = min_tools_required;
        repl_slt_state->min_archive_slot_required = min_archive_restart_lsn;
        repl_slt_state->exist_in_use = in_use;
    }
}

/*
 * Report the restart LSN in replication slots.
 */
void ReplicationSlotReportRestartLSN(void)
{
    int i;

    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        return;

    Assert(t_thrd.slot_cxt.ReplicationSlotCtl != NULL);

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        volatile ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (!s->in_use)
            continue;

        ereport(LOG,
                (errmsg("slotname: %s, dummy: %d, restartlsn: %X/%X", NameStr(s->data.name), s->data.isDummyStandby,
                        (uint32)(s->data.restart_lsn >> 32), (uint32)s->data.restart_lsn)));
    }
    LWLockRelease(ReplicationSlotControlLock);
}

/*
 * Compute the oldest WAL LSN required by *logical* decoding slots..
 *
 * Returns InvalidXLogRecPtr if logical decoding is disabled or no logicals
 * slots exist.
 *
 * NB: this returns a value >= ReplicationSlotsComputeRequiredLSN(), since it
 * ignores physical replication slots.
 *
 * The results aren't required frequently, so we don't maintain a precomputed
 * value like we do for ComputeRequiredLSN() and ComputeRequiredXmin().
 */
XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void)
{
    XLogRecPtr result = InvalidXLogRecPtr;
    int i;

    if (g_instance.attr.attr_storage.max_replication_slots <= 0)
        return InvalidXLogRecPtr;

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = NULL;
        XLogRecPtr restart_lsn;

        s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        /* cannot change while ReplicationSlotCtlLock is held */
        if (!s->in_use)
            continue;

        /* we're only interested in logical slots */
        if (s->data.database == InvalidOid)
            continue;

        /* read once, it's ok if it increases while we're checking */
        SpinLockAcquire(&s->mutex);
        restart_lsn = s->data.restart_lsn;
        SpinLockRelease(&s->mutex);

        if (XLByteEQ(result, InvalidXLogRecPtr) || XLByteLT(restart_lsn, result))

            result = restart_lsn;
    }

    LWLockRelease(ReplicationSlotControlLock);

    return result;
}

/*
 * ReplicationSlotsCountDBSlots -- count the number of slots that refer to the
 * passed database oid.
 *
 * Returns true if there are any slots referencing the database. *nslots will
 * be set to the absolute number of slots in the database, *nactive to ones
 * currently active.
 */
bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive)
{
    int i;

    *nslots = *nactive = 0;

    if (g_instance.attr.attr_storage.max_replication_slots <= 0)
        return false;

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        volatile ReplicationSlot *s = NULL;

        s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        /* cannot change while ReplicationSlotCtlLock is held */
        if (!s->in_use)
            continue;

        /* not database specific, skip */
        if (s->data.database == InvalidOid)
            continue;

        /* not our database, skip */
        if (s->data.database != dboid)
            continue;

        /* count slots with spinlock held */
        SpinLockAcquire(&s->mutex);
        (*nslots)++;
        if (s->active)
            (*nactive)++;
        SpinLockRelease(&s->mutex);
    }
    LWLockRelease(ReplicationSlotControlLock);

    if (*nslots > 0) {
        return true;
    }
    return false;
}

/*
 * Check whether the server's configuration supports using replication
 * slots.
 */
void CheckSlotRequirements(void)
{
    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        (errmsg("replication slots can only be used if max_replication_slots > 0"))));

    if (g_instance.attr.attr_storage.wal_level < WAL_LEVEL_ARCHIVE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("replication slots can only be used if wal_level >= archive")));
}

/*
 * Returns whether the string `str' has the postfix `end'.
 */
static bool string_endswith(const char *str, const char *end)
{
    size_t slen = strlen(str);
    size_t elen = strlen(end);
    /* can't be a postfix if longer */
    if (elen > slen)
        return false;

    /* compare the end of the strings */
    str += slen - elen;
    return strcmp(str, end) == 0;
}

/*
 * Flush all replication slots to disk.
 *
 * This needn't actually be part of a checkpoint, but it's a convenient
 * location.
 */
void CheckPointReplicationSlots(void)
{
    int i;
    int nRet = 0;

    ereport(DEBUG1, (errmsg("performing replication slot checkpoint")));

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);

    /*
     * Prevent any slot from being created/dropped while we're active. As we
     * explicitly do *not* want to block iterating over replication_slots or
     * acquiring a slot we cannot take the control lock - but that's OK,
     * because holding ReplicationSlotAllocationLock is strictly stronger,
     * and enough to guarantee that nobody can change the in_use bits on us.
     */
    LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);

    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        char path[MAXPGPATH];

        if (!s->in_use)
            continue;

        /* save the slot to disk, locking is handled in SaveSlotToPath() */
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", replslot_path, NameStr(s->data.name));
        securec_check_ss(nRet, "\0", "\0");

        if (unlikely(CheckFileExists(path) == FILE_NOT_EXIST)) {
            CreateSlotOnDisk(s);
        }
        SaveSlotToPath(s, path, LOG);
    }
    LWLockRelease(ReplicationSlotAllocationLock);
}

/*
 * Load all replication slots from disk into memory at server startup. This
 * needs to be run before we start crash recovery.
 */
void StartupReplicationSlots()
{
    DIR *replication_dir = NULL;
    struct dirent *replication_de = NULL;
    int nRet = 0;

    ereport(DEBUG1, (errmsg("starting up replication slots")));

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);

    /* restore all slots by iterating over all on-disk entries */
    replication_dir = AllocateDir(replslot_path);
    if (replication_dir == NULL) {
        char tmppath[MAXPGPATH];

        nRet = snprintf_s(tmppath, sizeof(tmppath), MAXPGPATH - 1, "%s", replslot_path);
        securec_check_ss(nRet, "\0", "\0");

        if (mkdir(tmppath, S_IRWXU) < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", tmppath)));
        fsync_fname(tmppath, true);
        return;
    }
    while ((replication_de = ReadDir(replication_dir, replslot_path)) != NULL) {
        struct stat statbuf;
        char path[MAXPGPATH];

        if (strcmp(replication_de->d_name, ".") == 0 || strcmp(replication_de->d_name, "..") == 0)
            continue;

        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s", replslot_path, replication_de->d_name);
        securec_check_ss(nRet, "\0", "\0");

        /* we're only creating directories here, skip if it's not our's */
        if (lstat(path, &statbuf) == 0 && !S_ISDIR(statbuf.st_mode))
            continue;

        /* we crashed while a slot was being setup or deleted, clean up */
        if (string_endswith(replication_de->d_name, ".tmp")) {
            if (ENABLE_DSS && CheckExistReplslotPath(path)) {
                RestoreSlotFromDisk(replication_de->d_name);
                continue;
            }
            if (!rmtree(path, true)) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove directory \"%s\"", path)));
                continue;
            }
            fsync_fname(replslot_path, true);
            continue;
        }

        if (ENABLE_DSS && !CheckExistReplslotPath(path)) {
            if (!unlink(path)) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove directory \"%s\"", path)));
            }
            continue;
        }

        /* looks like a slot in a normal state, restore */
        RestoreSlotFromDisk(replication_de->d_name);
    }
    FreeDir(replication_dir);

    /* currently no slots exist, we're done. */
    if (g_instance.attr.attr_storage.max_replication_slots <= 0) {
        return;
    }

    /* Now that we have recovered all the data, compute replication xmin */
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN(NULL);
}

/* ----
 * Manipulation of ondisk state of replication slots
 *
 * NB: none of the routines below should take any notice whether a slot is the
 * current one or not, that's all handled a layer above.
 * ----
 */
void CreateSlotOnDisk(ReplicationSlot *slot)
{
    char tmppath[MAXPGPATH];
    char path[MAXPGPATH];
    struct stat st;
    int nRet = 0;

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);

    /*
     * No need to take out the io_in_progress_lock, nobody else can see this
     * slot yet, so nobody else will write. We're reusing SaveSlotToPath which
     * takes out the lock, if we'd take the lock here, we'd deadlock.
     */
    nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s", replslot_path, NameStr(slot->data.name));
    securec_check_ss(nRet, "\0", "\0");

    nRet = snprintf_s(tmppath, sizeof(tmppath), MAXPGPATH - 1, "%s/%s.tmp", replslot_path, NameStr(slot->data.name));
    securec_check_ss(nRet, "\0", "\0");

    /*
     * It's just barely possible that some previous effort to create or
     * drop a slot with this name left a temp directory lying around.
     * If that seems to be the case, try to remove it.  If the rmtree()
     * fails, we'll error out at the mkdir() below, so we don't bother
     * checking success.
     */
    if (stat(tmppath, &st) == 0 && S_ISDIR(st.st_mode)) {
        if (!rmtree(tmppath, true)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not rm directory \"%s\": %m", tmppath)));
        }
    }

    /* Create and fsync the temporary slot directory. */
    if (mkdir(tmppath, S_IRWXU) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", tmppath)));
    fsync_fname(tmppath, true);

    /* Write the actual state file. */
    slot->dirty = true; /* signal that we really need to write */
    SaveSlotToPath(slot, tmppath, ERROR);

    /* Rename the directory into place. */
    if (RenameReplslotPath(tmppath, path) != 0)
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m", tmppath, path)));

    /*
     * If we'd now fail - really unlikely - we wouldn't know whether this slot
     * would persist after an OS crash or not - so, force a restart. The
     * restart would try to fysnc this again till it works.
     */
    START_CRIT_SECTION();

    fsync_fname(path, true);
    fsync_fname(replslot_path, true);

    END_CRIT_SECTION();

    ereport(LOG, (errcode_for_file_access(), errmsg("create slot \"%s\" on disk successfully", path)));
}

static bool CheckAndWriteSlotExtra(ReplicationSlot *slot, ReplicationSlotOnDisk cp, char* tmppath, int fd, int elevel)
{
    if (slot->extra_content == NULL || strlen(slot->extra_content) == 0) {
        return true;
    }
    if ((uint32)GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata) != (uint32)strlen(slot->extra_content)) {
        LWLockRelease(slot->io_in_progress_lock);
        ereport(elevel, (errcode_for_file_access(),
            errmsg("update archive slot info to file \"%s\" fail, because extra content length incorrect", tmppath)));
        return false;
    }
    if ((write(fd, slot->extra_content, (uint32)strlen(slot->extra_content)) !=
        ((uint32)strlen(slot->extra_content)))) {
        int save_errno = errno;
        pgstat_report_waitevent(WAIT_EVENT_END);
        if (close(fd)) {
            ereport(elevel, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
        }
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;
        LWLockRelease(slot->io_in_progress_lock);
        ereport(elevel, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
        return false;
    }
    return true;
}

/*
 * Shared functionality between saving and creating a replication slot.
 */
static void SaveSlotToPath(ReplicationSlot *slot, const char *dir, int elevel)
{
    char tmppath[MAXPGPATH];
    char path[MAXPGPATH];
    const int STATE_FILE_NUM = 2;
    char *fname[STATE_FILE_NUM];
    int fd;
    ReplicationSlotOnDisk cp;
    bool was_dirty = false;
    errno_t rc = EOK;

    /* first check whether there's something to write out */
    {
        volatile ReplicationSlot *vslot = slot;

        SpinLockAcquire(&vslot->mutex);
        was_dirty = vslot->dirty;
        vslot->just_dirtied = false;
        SpinLockRelease(&vslot->mutex);
    }

    /* and don't do anything if there's nothing to write */
    if (!was_dirty) {
        return;
    }

    LWLockAcquire(slot->io_in_progress_lock, LW_EXCLUSIVE);

    /* silence valgrind :( */
    rc = memset_s(&cp, sizeof(ReplicationSlotOnDisk), 0, sizeof(ReplicationSlotOnDisk));
    securec_check(rc, "\0", "\0");

    fname[0] = "%s/state.backup";
    fname[1] = "%s/state.tmp";

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/state", dir);
    securec_check_ss(rc, "\0", "\0");

    for (int i = 0; i < STATE_FILE_NUM; i++) {
        rc = snprintf_s(tmppath, MAXPGPATH, MAXPGPATH - 1, fname[i], dir);
        securec_check_ss(rc, "\0", "\0");

        fd = BasicOpenFile(tmppath, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            LWLockRelease(slot->io_in_progress_lock);
            ereport(elevel, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", tmppath)));
            return;
        }

        cp.magic = SLOT_MAGIC;
        INIT_CRC32C(cp.checksum);
        cp.version = 1;
        cp.length = ReplicationSlotOnDiskDynamicSize;

        SpinLockAcquire(&slot->mutex);

        rc = memcpy_s(&cp.slotdata, sizeof(ReplicationSlotPersistentData), &slot->data,
                      sizeof(ReplicationSlotPersistentData));
        securec_check(rc, "\0", "\0");

        SpinLockRelease(&slot->mutex);
        COMP_CRC32C(cp.checksum, (char *)(&cp) + ReplicationSlotOnDiskConstantSize, ReplicationSlotOnDiskDynamicSize);
        if (GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata) > 0) {
            COMP_CRC32C(cp.checksum, slot->extra_content, GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata));
        }
        FIN_CRC32C(cp.checksum);

        /* Causing errno to potentially come from a previous system call. */
        errno = 0;
        pgstat_report_waitevent(WAIT_EVENT_REPLICATION_SLOT_WRITE);
        if ((write(fd, &cp, (uint32)sizeof(cp))) != sizeof(cp)) {
            int save_errno = errno;
            pgstat_report_waitevent(WAIT_EVENT_END);
            if (close(fd)) {
                ereport(elevel, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
            }
            /* if write didn't set errno, assume problem is no disk space */
            errno = save_errno ? save_errno : ENOSPC;
            LWLockRelease(slot->io_in_progress_lock);
            ereport(elevel, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
            return;
        }
        if (!CheckAndWriteSlotExtra(slot, cp, tmppath, fd, elevel)) {
            return;
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        /* fsync the temporary file */
        pgstat_report_waitevent(WAIT_EVENT_REPLICATION_SLOT_SYNC);
        if (pg_fsync(fd) != 0) {
            int save_errno = errno;
            pgstat_report_waitevent(WAIT_EVENT_END);
            if (close(fd)) {
                ereport(elevel, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
            }
            errno = save_errno;
            LWLockRelease(slot->io_in_progress_lock);
            ereport(elevel, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmppath)));
            return;
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
        if (close(fd)) {
            ereport(elevel, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
        }
    }

    /* rename to permanent file, fsync file and directory */
    if (rename(tmppath, path) != 0) {
        LWLockRelease(slot->io_in_progress_lock);
        ereport(elevel, (errcode_for_file_access(), errmsg("could not rename \"%s\" to \"%s\": %m", tmppath, path)));
        return;
    }

    /* Check CreateSlot() for the reasoning of using a crit. section. */
    START_CRIT_SECTION();

    fsync_fname(path, false);
    fsync_fname(dir, true);
    if (!ENABLE_DSS) {
        fsync_fname("pg_replslot", true);
    }

    END_CRIT_SECTION();

    /*
     * Successfully wrote, unset dirty bit, unless somebody dirtied again
     * already.
     */
    {
        volatile ReplicationSlot *vslot = slot;

        SpinLockAcquire(&vslot->mutex);
        if (!vslot->just_dirtied)
            vslot->dirty = false;
        SpinLockRelease(&vslot->mutex);
    }

    LWLockRelease(slot->io_in_progress_lock);
}

/*
 * Load a single slot from disk into memory.
 */
static void RestoreSlotFromDisk(const char *name)
{
    ReplicationSlotOnDisk cp;
    char path[MAXPGPATH];
    int fd;
    int readBytes;
    pg_crc32c checksum;
    errno_t rc = EOK;
    int ret;
    int i = 0;
    bool ignore_bak = false;
    bool restored = false; 
    bool retry = false;
    char *extra_content = NULL;
    ArchiveConfig *archive_cfg = NULL;

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);

    /* no need to lock here, no concurrent access allowed yet
     *
     * delete temp file if it exists
     */
    rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state.tmp", replslot_path, name);
    securec_check_ss(rc, "\0", "\0");

    ret = unlink(path);
    if (ret < 0 && errno != ENOENT)
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path)));

    /* unlink backup file if rename failed */
    if (ret == 0) {
        rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state.backup", replslot_path, name);
        securec_check_ss(rc, "\0", "\0");
        if (unlink(path) < 0 && errno != ENOENT)
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path)));
        ignore_bak = true;
    }

    rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state", replslot_path, name);
    securec_check_ss(rc, "\0", "\0");

    elog(DEBUG1, "restoring replication slot from \"%s\"", path);

loop:
    fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);

    /*
     * We do not need to handle this as we are rename()ing the directory into
     * place only after we fsync()ed the state file.
     */
    if (fd < 0)
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));

    /*
     * Sync state file before we're reading from it. We might have crashed
     * while it wasn't synced yet and we shouldn't continue on that basis.
     */
    pgstat_report_waitevent(WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC);
    if (pg_fsync(fd) != 0) {
        int save_errno = errno;
        if (close(fd)) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", path)));
        }
        errno = save_errno;
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", path)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    /* Also sync the parent directory */
    START_CRIT_SECTION();
    fsync_fname(path, true);
    END_CRIT_SECTION();

    /* read part of statefile that's guaranteed to be version independent */
    pgstat_report_waitevent(WAIT_EVENT_REPLICATION_SLOT_READ);

    errno = 0;
    /* read the whole state file */
    readBytes = read(fd, &cp, (uint32)sizeof(ReplicationSlotOnDisk));
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (readBytes != sizeof(ReplicationSlotOnDisk)) {
        goto ERROR_READ;    
    }

    /* recovery extra content */
    if (GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata) != 0) {
        MemoryContext curr;
        curr = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
        extra_content = (char*)palloc0((uint32)GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata) + 1);
        readBytes = read(fd, extra_content, (uint32)GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata));
        if (readBytes != GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata)) {
            MemoryContextSwitchTo(curr);
            goto ERROR_READ;
        }
        if ((archive_cfg = formArchiveConfigFromStr(extra_content, true)) == NULL) {
            pfree_ext(extra_content);
            MemoryContextSwitchTo(curr);
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not read file \"%s\", content is %s", path,
                extra_content)));
        }
        MemoryContextSwitchTo(curr);
    }

    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", path)));
    }

    /* now verify the CRC */
    INIT_CRC32C(checksum);
    COMP_CRC32C(checksum, &cp.slotdata, sizeof(ReplicationSlotPersistentData));
    if (GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata) > 0) {
        COMP_CRC32C(checksum, extra_content, GET_SLOT_EXTRA_DATA_LENGTH(cp.slotdata));
    }
    FIN_CRC32C(checksum);
    if (!EQ_CRC32C(checksum, cp.checksum)) {
        if (ignore_bak == false) {
            ereport(WARNING,
                    (errmsg("replication slot file %s: checksum mismatch, is %u, should be %u, try backup file", path,
                            checksum, cp.checksum)));
            rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state.backup", replslot_path, name);
            securec_check_ss(rc, "\0", "\0");
            ignore_bak = true;
            retry = true;
            goto loop;
        } else {
            ereport(PANIC, (errmsg("replication slot file %s: checksum mismatch, is %u, should be %u", path, checksum,
                                   cp.checksum)));
        }
    }
    /* verify magic */
    if (cp.magic != SLOT_MAGIC) {
        if (ignore_bak == false) {
            ereport(WARNING, (errcode_for_file_access(),
                              errmsg("replication slot file \"%s\" has wrong magic %u instead of %d, try backup file",
                                     path, cp.magic, SLOT_MAGIC)));
            rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state.backup", replslot_path, name);
            securec_check_ss(rc, "\0", "\0");
            ignore_bak = true;
            retry = true;
            goto loop;
        } else {
            ereport(PANIC,
                    (errcode_for_file_access(), errmsg("replication slot file \"%s\" has wrong magic %u instead of %d",
                                                       path, cp.magic, SLOT_MAGIC)));
        }
    }
    /* boundary check on length */
    if (cp.length != ReplicationSlotOnDiskDynamicSize) {
        if (ignore_bak == false) {
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("replication slot file \"%s\" has corrupted length %u, try backup file", path, cp.length)));
            rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state.backup", replslot_path, name);
            securec_check_ss(rc, "\0", "\0");
            ignore_bak = true;
            retry = true;
            goto loop;
        } else {
            ereport(PANIC, (errcode_for_file_access(),
                            errmsg("replication slot file \"%s\" has corrupted length %u", path, cp.length)));
        }
    }

    /*
     * If we crashed with an ephemeral slot active, don't restore but delete it.
     */
    if (GET_SLOT_PERSISTENCY(cp.slotdata) != RS_PERSISTENT && GET_SLOT_PERSISTENCY(cp.slotdata) != RS_BACKUP) {
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", replslot_path, name);
        securec_check_ss(rc, "\0", "\0");
        if (!rmtree(path, true)) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove directory \"%s\"", path)));
        }
        fsync_fname(replslot_path, true);
        return;
    }

    if (retry == true)
        RecoverReplSlotFile(cp, name);

    /* nothing can be active yet, don't lock anything */
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *slot = NULL;

        slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (slot->in_use)
            continue;

        /* restore the entire set of persistent data */
        rc = memcpy_s(&slot->data, sizeof(ReplicationSlotPersistentData), &cp.slotdata,
                      sizeof(ReplicationSlotPersistentData));
        securec_check(rc, "\0", "\0");
        /* reset physical slot catalog xmin to invalid transaction id */
        if (slot->data.database == InvalidOid) {
            slot->data.catalog_xmin = InvalidTransactionId;
        }

        /* initialize in memory state */
        slot->effective_xmin = slot->data.xmin;
        slot->effective_catalog_xmin = slot->data.catalog_xmin;

        slot->candidate_catalog_xmin = InvalidTransactionId;
        slot->candidate_xmin_lsn = InvalidXLogRecPtr;
        slot->candidate_restart_lsn = InvalidXLogRecPtr;
        slot->candidate_restart_valid = InvalidXLogRecPtr;
        slot->in_use = true;
        slot->active = false;
        slot->extra_content = extra_content;
        slot->archive_config = archive_cfg;
        slot->last_xmin_change_time = GetCurrentTimestamp();
        restored = true;
        if (extra_content != NULL) {
            MarkArchiveSlotOperate();
            add_archive_slot_to_instance(slot);
        }
        break;
    }

    if (!restored)
        ereport(PANIC, (errmsg("too many replication slots active before shutdown"),
                        errhint("Increase g_instance.attr.attr_storage.max_replication_slots and try again.")));
    return;
ERROR_READ:
    int saved_errno = errno;
    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", path)));
    }
    errno = saved_errno;
    ereport(PANIC, (errcode_for_file_access(), errmsg("could not read file \"%s\", read %d of %u: %m", path,
                                                      readBytes, (uint32)sizeof(ReplicationSlotOnDisk))));
}

/*
 * when incorrect checksum is detected in slot file,
 * we should recover the slot file using the content of backup file
 */
static void RecoverReplSlotFile(const ReplicationSlotOnDisk &cp, const char *name)
{
    int fd;
    char path[MAXPGPATH];
    errno_t rc = EOK;

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);

    rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state", replslot_path, name);
    securec_check_ss(rc, "\0", "\0");

    ereport(WARNING, (errmsg("recover the replication slot file %s", name)));

    fd = BasicOpenFile(path, O_TRUNC | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0)
        ereport(PANIC, (errcode_for_file_access(), errmsg("recover failed could not open slot file \"%s\": %m", path)));

    errno = 0;
    if ((write(fd, &cp, sizeof(cp))) != sizeof(cp)) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0)
            errno = ENOSPC;
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("recover failed could not write to slot file \"%s\": %m", path)));
    }

    /* fsync the temporary file */
    if (pg_fsync(fd) != 0)
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("recover failed could not fsync slot file \"%s\": %m", path)));

    if (close(fd))
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("recover failed could not close slot file \"%s\": %m", path)));
}
/*
 * get cur nodes slotname
 *
 */
char *get_my_slot_name(void)
{
    ReplConnInfo *conninfo = NULL;
    errno_t retcode = EOK;

    /* local and local port is the same, so we choose the first one is ok */
    int repl_idx = 0;
    char *slotname = NULL;
    char *t_appilcation_name = NULL;
    slotname = (char *)palloc0(NAMEDATALEN);
    t_appilcation_name = get_application_name();
    /* get current repl conninfo, */
    conninfo = GetRepConnArray(&repl_idx);
    if (u_sess->attr.attr_storage.PrimarySlotName != NULL) {
        retcode = strncpy_s(slotname, NAMEDATALEN, u_sess->attr.attr_storage.PrimarySlotName, NAMEDATALEN - 1);
        securec_check(retcode, "\0", "\0");
    } else if (t_appilcation_name && strlen(t_appilcation_name) > 0) {
        int rc = 0;
        rc = snprintf_s(slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s", t_appilcation_name);
        securec_check_ss(rc, "\0", "\0");
    } else if (g_instance.attr.attr_common.PGXCNodeName != NULL) {
        int rc = 0;
        if (IS_DN_DUMMY_STANDYS_MODE()) {
            rc = snprintf_s(slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s", g_instance.attr.attr_common.PGXCNodeName);
        } else if (conninfo != NULL) {
            rc = snprintf_s(slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s_%s_%d",
                            g_instance.attr.attr_common.PGXCNodeName, conninfo->localhost, conninfo->localport);
        }
        securec_check_ss(rc, "\0", "\0");
    }
    pfree(t_appilcation_name);
    t_appilcation_name = NULL;
    return slotname;
}

static int cmp_slot_lsn(const void *a, const void *b)
{
    XLogRecPtr lsn1 = *((const XLogRecPtr *)a);
    XLogRecPtr lsn2 = *((const XLogRecPtr *)b);

    if (!XLByteLE(lsn1, lsn2))
        return -1;
    else if (XLByteEQ(lsn1, lsn2))
        return 0;
    else
        return 1;
}

/*
 * get the  application_name specified in postgresql.conf
 */
static char *get_application_name(void)
{
#define INVALID_LINES_IDX (int)(~0)
    char **optlines = NULL;
    int lines_index = 0;
    int optvalue_off;
    int optvalue_len;
    char arg_str[NAMEDATALEN] = {0};
    const char *config_para_build = "application_name";
    int rc;
    char conf_path[MAXPGPATH] = {0};
    char *trim_app_name = NULL;
    char *app_name = NULL;
    app_name = (char *)palloc0(MAXPGPATH);
    rc = snprintf_s(conf_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, "postgresql.conf");
    securec_check_ss_c(rc, "\0", "\0");

    if ((optlines = (char **)read_guc_file(conf_path)) != NULL) {
        lines_index = find_guc_option(optlines, config_para_build, NULL, NULL, &optvalue_off, &optvalue_len, false);
        if (lines_index != INVALID_LINES_IDX) {
            rc = strncpy_s(arg_str, NAMEDATALEN, optlines[lines_index] + optvalue_off, optvalue_len);
            securec_check_c(rc, "\0", "\0");
        }
        /* first free one-dimensional array memory in case memory leak */
        int i = 0;
        while (optlines[i] != NULL) {
            selfpfree(const_cast<char *>(optlines[i]));
            optlines[i] = NULL;
            i++;
        }
        selfpfree(optlines);
        optlines = NULL;
    } else {
        return app_name;
    }
    /* construct slotname */
    trim_app_name = trim_str(arg_str, NAMEDATALEN, '\'');
    if (trim_app_name != NULL) {
        rc = snprintf_s(app_name, NAMEDATALEN, NAMEDATALEN - 1, "%s", trim_app_name);
        securec_check_ss_c(rc, "\0", "\0");
        pfree(trim_app_name);
        trim_app_name = NULL;
    }
    return app_name;
}

/*
 * Get the string beside space or sep
 */
static char *trim_str(char *str, int str_len, char sep)
{
    int len;
    char *begin = NULL;
    char *end = NULL;
    char *cur = NULL;
    char *cpyStr = NULL;
    errno_t rc;

    if (str == NULL || str_len <= 0) {
        return NULL;
    }
    cpyStr = (char *)palloc(str_len);
    begin = str;
    while (begin != NULL && (isspace((int)*begin) || *begin == sep)) {
        begin++;
    }
    for (end = cur = begin; *cur != '\0'; cur++) {
        if (!isspace((int)*cur) && *cur != sep) {
            end = cur;
        }
    }
    if (*begin == '\0') {
        pfree(cpyStr);
        cpyStr = NULL;
        return NULL;
    }
    len = end - begin + 1;
    rc = memmove_s(cpyStr, (uint32)str_len, begin, len);
    securec_check_c(rc, "\0", "\0");
    cpyStr[len] = '\0';
    return cpyStr;
}

char *formArchiveConfigStringFromStruct(ArchiveConfig *archive_config)
{
    if (archive_config == NULL || archive_config->media_type == ARCHIVE_NONE) {
        return NULL;
    }
    /* For media type header OBS;/NAS; */
    int length = 4;
    char *result = NULL;
    int rc = 0;
    char encryptSecretAccessKeyStr[DEST_CIPHER_LENGTH] = {'\0'};
    if (archive_config->media_type == ARCHIVE_OBS) {
        if (archive_config->conn_config == NULL) {
            return NULL;
        }
        encryptKeyString(archive_config->conn_config->obs_sk, encryptSecretAccessKeyStr, DEST_CIPHER_LENGTH);
        length += strlen(archive_config->conn_config->obs_address) + 1;
        length += strlen(archive_config->conn_config->obs_bucket) + 1;
        length += strlen(archive_config->conn_config->obs_ak) + 1;
        length += strlen(encryptSecretAccessKeyStr);
    }
    // for archive_prefix
    length += strlen(archive_config->archive_prefix) + 1;
    // for is_recovery
    length += 1 + 1;
    // for vote_replicate_first
    length += 1 + 1;
    result = (char *)palloc0(length + 1);
    if (archive_config->media_type == ARCHIVE_OBS) {
        rc = snprintf_s(result, length + 1, length, "OBS;%s;%s;%s;%s;%s;%s;%s",
            archive_config->conn_config->obs_address,
            archive_config->conn_config->obs_bucket,
            archive_config->conn_config->obs_ak,
            encryptSecretAccessKeyStr, 
            archive_config->archive_prefix,
            archive_config->is_recovery ? "1" : "0",
            archive_config->vote_replicate_first ? "1" : "0");
    } else {
        rc = snprintf_s(result, length + 1, length, "NAS;%s;%s;%s",
            archive_config->archive_prefix,
            archive_config->is_recovery ? "1" : "0",
            archive_config->vote_replicate_first ? "1" : "0");
    }
    securec_check_ss_c(rc, "\0", "\0");
    return result;
}

ArchiveConfig* formArchiveConfigFromStr(char *content, bool encrypted)
{
    ArchiveConfig* archive_config = NULL;
    ArchiveConnConfig* conn_config = NULL;
    char* media_type = NULL;
    errno_t rc = EOK;
    char *content_copy = NULL;
    char *tmp = NULL;
    List* elemlist = NIL;
    int param_num = 0;
    size_t elem_index = 0;
    archive_config = (ArchiveConfig *)palloc0(sizeof(ArchiveConfig));
    char decryptSecretAccessKeyStr[DEST_CIPHER_LENGTH] = {'\0'};
    if (content == NULL || strlen(content) == 0) {
        goto FAILURE;
    }
    /* SplitIdentifierString will change origin string */
    content_copy = pstrdup(content);
    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(content_copy, ';', &elemlist, false, false)) {
        goto FAILURE;
    }
    param_num = list_length(elemlist);
    /*
     * The extra_content when create archive slot
     * OBS: OBS;obs_server_ip;obs_bucket_name;obs_ak;obs_sk;archive_prefix;is_recovery;is_vote_replication_first
     * NAS: NAS;archive_prefix;is_recovery;is_vote_replication_first
     */
    if (param_num != 7 && param_num != 4 && param_num != 8) {
        goto FAILURE;
    }

    if (param_num == 7) {
        archive_config->media_type = ARCHIVE_OBS;
    } else {
        media_type = pstrdup((char*)list_nth(elemlist, elem_index++));
        if (strcmp(media_type, "OBS") == 0) {
            archive_config->media_type = ARCHIVE_OBS;
        } else if (strcmp(media_type, "NAS") == 0) {
            archive_config->media_type = ARCHIVE_NAS;
        } else {
            goto FAILURE;
        }
    }

    if (archive_config->media_type == ARCHIVE_OBS) {
        conn_config = (ArchiveConnConfig *)palloc0(sizeof(ArchiveConnConfig));
        conn_config->obs_address = pstrdup((char*)list_nth(elemlist, elem_index++));
        conn_config->obs_bucket = pstrdup((char*)list_nth(elemlist, elem_index++));
        conn_config->obs_ak = pstrdup((char*)list_nth(elemlist, elem_index++));
        if (encrypted == false) {
            conn_config->obs_sk = pstrdup((char*)list_nth(elemlist, elem_index++));
            if (strlen(conn_config->obs_sk) >= DEST_CIPHER_LENGTH || strlen(conn_config->obs_sk) == 0) {
                goto FAILURE;
            }
        } else {
            decryptKeyString((char*)list_nth(elemlist, elem_index++), decryptSecretAccessKeyStr, DEST_CIPHER_LENGTH,
                NULL);
            conn_config->obs_sk = pstrdup(decryptSecretAccessKeyStr);
            rc = memset_s(decryptSecretAccessKeyStr, DEST_CIPHER_LENGTH, 0, DEST_CIPHER_LENGTH);
            securec_check(rc, "\0", "\0");
        }
    }
    archive_config->conn_config = conn_config;
    archive_config->archive_prefix = pstrdup((char*)list_nth(elemlist, elem_index++));
    tmp = (char*)list_nth(elemlist, elem_index++);
    archive_config->is_recovery = (tmp != NULL && tmp[0] != '\0' && tmp[0] == '1') ? true : false;
    tmp = (char*)list_nth(elemlist, elem_index++);
    archive_config->vote_replicate_first = (tmp != NULL && tmp[0] != '\0' && tmp[0] == '1') ? true : false;

    pfree_ext(content_copy);
    pfree_ext(media_type);
    list_free_ext(elemlist);
    return archive_config;
FAILURE:
    if (archive_config != NULL) {
        if (archive_config->conn_config != NULL) {
            pfree_ext(archive_config->conn_config->obs_address);
            pfree_ext(archive_config->conn_config->obs_bucket);
            pfree_ext(archive_config->conn_config->obs_ak);
            pfree_ext(archive_config->conn_config->obs_sk);
            pfree_ext(archive_config->conn_config);
        }
        pfree_ext(archive_config->archive_prefix);
    }
    pfree_ext(archive_config);
    pfree_ext(content_copy);
    pfree_ext(media_type);
    list_free_ext(elemlist);
    ereport(ERROR,
        (errcode_for_file_access(), errmsg("message is inleagel, please check the extra content for archive")));

    return NULL;
}

/*
* this function is called by backend thread only
*/
ArchiveSlotConfig *getArchiveReplicationSlotWithName(const char *slot_name)
{
    volatile int *slot_num = &g_instance.archive_obs_cxt.archive_slot_num;
    if (*slot_num == 0) {
        return NULL;
    }
    if (slot_name == NULL) {
        return NULL;
    }
    ArchiveSlotConfig* archive_conf_copy = find_archive_slot_from_instance_list(slot_name);
    if (archive_conf_copy == NULL || archive_conf_copy->archive_config.is_recovery == true) {
        return NULL;
    }
    return archive_conf_copy;
}

/*
* this function is called by backend thread only
*/
ArchiveSlotConfig *getArchiveRecoverySlotWithName(const char *slot_name)
{
    volatile int *slot_num = &g_instance.archive_obs_cxt.archive_recovery_slot_num;
    if (*slot_num == 0) {
        return NULL;
    }
    if (slot_name == NULL) {
        return NULL;
    }
    ArchiveSlotConfig* archive_conf_copy = find_archive_slot_from_instance_list(slot_name);
    if (archive_conf_copy == NULL || archive_conf_copy->archive_config.is_recovery == false) {
        return NULL;
    }
    return archive_conf_copy;
}

/*
* this function only used for archiver thread
*/
ArchiveSlotConfig* getArchiveReplicationSlot()
{
    Assert(t_thrd.role == ARCH);
    volatile int *slot_num = &g_instance.archive_obs_cxt.archive_slot_num;
    if (*slot_num == 0) {
        return NULL;
    }
    if (t_thrd.arch.slot_name == NULL) {
        return NULL;
    }
    volatile int *g_tline = &g_instance.archive_obs_cxt.slot_tline;
    volatile int *l_tline = &t_thrd.arch.slot_tline;
    if (likely(*g_tline == *l_tline) && t_thrd.arch.archive_config) {
        return t_thrd.arch.archive_config;
    }
    
    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery == false 
            && strcmp(t_thrd.arch.slot_name, slot->data.name.data) == 0) {
            release_archive_slot(&t_thrd.arch.archive_config);
            t_thrd.arch.archive_config = (ArchiveSlotConfig *)palloc0(sizeof(ArchiveSlotConfig));
            t_thrd.arch.archive_config->archive_config.media_type = slot->archive_config->media_type;
            if (slot->archive_config->conn_config != NULL) {
                t_thrd.arch.archive_config->archive_config.conn_config =
                    (ArchiveConnConfig *)palloc0(sizeof(ArchiveConnConfig));
                t_thrd.arch.archive_config->archive_config.conn_config->obs_address =
                    pstrdup(slot->archive_config->conn_config->obs_address);
                t_thrd.arch.archive_config->archive_config.conn_config->obs_bucket =
                    pstrdup(slot->archive_config->conn_config->obs_bucket);
                t_thrd.arch.archive_config->archive_config.conn_config->obs_ak =
                    pstrdup(slot->archive_config->conn_config->obs_ak);
                t_thrd.arch.archive_config->archive_config.conn_config->obs_sk =
                    pstrdup(slot->archive_config->conn_config->obs_sk);
            }
            t_thrd.arch.archive_config->archive_config.archive_prefix =
                pstrdup_ext(slot->archive_config->archive_prefix);
            t_thrd.arch.slot_idx = slotno;
            SpinLockRelease(&slot->mutex);
            *l_tline = *g_tline;
            return t_thrd.arch.archive_config;
        }
        SpinLockRelease(&slot->mutex);
    }
    return NULL;
}

/*
* this function only used for archiver thread
*/
ArchiveSlotConfig* GetArchiveRecoverySlot()
{
    volatile int *slot_num = &g_instance.archive_obs_cxt.archive_recovery_slot_num;
    if (*slot_num == 0) {
        return NULL;
    }
    
    volatile int *g_tline = &g_instance.archive_obs_cxt.slot_tline;
    volatile int *l_tline = &t_thrd.arch.slot_tline;
    if (likely(*g_tline == *l_tline) && t_thrd.arch.archive_config) {
        return t_thrd.arch.archive_config;
    }
    
    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery) {
            ArchiveSlotConfig* archive_conf_copy = find_archive_slot_from_instance_list(slot->data.name.data);
            t_thrd.arch.archive_config = archive_conf_copy;
            *l_tline = *g_tline;
            SpinLockRelease(&slot->mutex);
            return archive_conf_copy;
        }
        SpinLockRelease(&slot->mutex);
    }
    return NULL;
}

List *GetAllArchiveSlotsName()
{
    List *result = NULL;
    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery == false &&
            GET_SLOT_PERSISTENCY(slot->data) != RS_BACKUP) {
            result = lappend(result, (void *)pstrdup(slot->data.name.data));
        }
        SpinLockRelease(&slot->mutex);
    }
    return result;
}

List *GetAllRecoverySlotsName()
{
    List *result = NULL;
    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery == true) {
            result = lappend(result, (void *)pstrdup(slot->data.name.data));
        }
        SpinLockRelease(&slot->mutex);
    }
    return result;
}

void AdvanceArchiveSlot(XLogRecPtr restart_pos) 
{
    volatile int *slot_idx = &t_thrd.arch.slot_idx;
    char* extra_content = NULL;
    if (likely(*slot_idx != -1) && *slot_idx < g_instance.attr.attr_storage.max_replication_slots) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[*slot_idx];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL) {
            t_thrd.slot_cxt.MyReplicationSlot = slot;
            slot->data.restart_lsn = restart_pos;
            extra_content = slot->extra_content;
            SpinLockRelease(&slot->mutex);
        } else {
            SpinLockRelease(&slot->mutex);
            ereport(WARNING,
                (errcode_for_file_access(), errmsg("slot idx not valid, obs slot %X/%X not advance ",
                    (uint32)(restart_pos >> 32), (uint32)(restart_pos))));
        }

        if (extra_content == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("archive thread could not get slot extra content when advance slot.")));
        }
        ReplicationSlotMarkDirty();
        log_slot_advance(&slot->data, extra_content);
    }
}

XLogRecPtr slot_advance_confirmed_lsn(const char *slotname, XLogRecPtr target_lsn)
{
    XLogRecPtr endlsn;
    XLogRecPtr curlsn;

    Assert(!t_thrd.slot_cxt.MyReplicationSlot);

    /* Acquire the slot so we "own" it */
    ReplicationSlotAcquire(slotname, false);

    if (GET_SLOT_PERSISTENCY(t_thrd.slot_cxt.MyReplicationSlot->data) != RS_BACKUP) {
        ReplicationSlotRelease();
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("slot %s is not backup slot.", slotname)));
    }

    curlsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;

    if (!XLogRecPtrIsInvalid(curlsn) && XLByteLE(curlsn, target_lsn)) {
        ereport(LOG, (errmsg("current ddl lsn %X/%X, target ddl lsn %X/%X, do not advance.",
            (uint32)(curlsn >> 32), (uint32)curlsn, (uint32)(target_lsn >> 32), (uint32)(target_lsn))));
        endlsn = curlsn;
    } else {
        SpinLockAcquire(&t_thrd.slot_cxt.MyReplicationSlot->mutex);
        t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush = target_lsn;
        SpinLockRelease(&t_thrd.slot_cxt.MyReplicationSlot->mutex);
        endlsn = target_lsn;

        /* save change to disk. */
        ReplicationSlotMarkDirty();
        ReplicationSlotsComputeRequiredXmin(false);
        ReplicationSlotsComputeRequiredLSN(NULL);
        ReplicationSlotSave();
        if (!RecoveryInProgress()) {
            log_slot_advance(&t_thrd.slot_cxt.MyReplicationSlot->data);
        }
    }

    ReplicationSlotRelease();

    return endlsn;
}

bool slot_reset_confirmed_lsn(const char *slotname, XLogRecPtr *start_lsn)
{
    XLogRecPtr local_confirmed_lsn;
    XLogRecPtr ret_lsn;
    int cand_index = -1;
    bool need_recycle = true;
    ReplicationSlot *s = NULL;
    int i;

    Assert(!t_thrd.slot_cxt.MyReplicationSlot);

    /* Acquire the slot and get ddl delay start lsn */
    ReplicationSlotAcquire(slotname, false);

    if (GET_SLOT_PERSISTENCY(t_thrd.slot_cxt.MyReplicationSlot->data) != RS_BACKUP) {
        ReplicationSlotRelease();
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("slot %s is not backup slot.", slotname)));
    }

    local_confirmed_lsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;
    ReplicationSlotRelease();
    *start_lsn = local_confirmed_lsn;

    if (XLogRecPtrIsInvalid(local_confirmed_lsn)) {
        ereport(LOG, (errmsg("find invalid ddl delay start lsn for backup slot %s, do nothing.", slotname)));
        return false;
    }

    /* Loop through all slots and check concurrent delay ddl. need to guard againt concurrent slot drop. */
    LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (s->in_use && strcmp(slotname, NameStr(s->data.name)) != 0 &&
            GET_SLOT_PERSISTENCY(s->data) == RS_BACKUP && !XLogRecPtrIsInvalid(s->data.confirmed_flush)) {
            need_recycle = false;

            if (XLByteLT(local_confirmed_lsn, s->data.confirmed_flush)) {
                cand_index = i;
            } else {
                cand_index = -1;
                break;
            }
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    if (cand_index != -1) {
        s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[cand_index];
        ret_lsn = slot_advance_confirmed_lsn(NameStr(s->data.name), local_confirmed_lsn);
        Assert(XLByteEQ(local_confirmed_lsn, ret_lsn));
    }
    LWLockRelease(ReplicationSlotAllocationLock);

    /* Acquire the slot again to modify */
    ret_lsn = slot_advance_confirmed_lsn(slotname, InvalidXLogRecPtr);
    Assert(XLogRecPtrIsInvalid(ret_lsn));

    return need_recycle;
}

/*
 * Compute the oldest confirmed LSN, i.e. delay ddl start lsn, across all slots.
 */
XLogRecPtr ReplicationSlotsComputeConfirmedLSN(void)
{
    int i;
    XLogRecPtr min_required = InvalidXLogRecPtr;

    if (g_instance.attr.attr_storage.max_replication_slots == 0) {
        return InvalidXLogRecPtr;
    }

    Assert(t_thrd.slot_cxt.ReplicationSlotCtl != NULL);

    /* ReplicationSlotControlLock seems unnecessary since delay ddl lock must be held to modify confirmed lsn */
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        volatile ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        SpinLockAcquire(&slot->mutex);
        XLogRecPtr confirmed_lsn;

        if (!slot->in_use || GET_SLOT_PERSISTENCY(slot->data) != RS_BACKUP) {
            SpinLockRelease(&slot->mutex);
            continue;
        }

        confirmed_lsn = slot->data.confirmed_flush;
        SpinLockRelease(&slot->mutex);

        if (!XLogRecPtrIsInvalid(confirmed_lsn) &&
            (XLogRecPtrIsInvalid(min_required) || XLByteLT(confirmed_lsn, min_required))) {
            min_required = confirmed_lsn;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    return min_required;
}

void MarkArchiveSlotOperate()
{
    int archive_slot_num = 0;
    int archive_recovery_slot_num = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL) {
            if (slot->archive_config->is_recovery) {
                archive_recovery_slot_num++;
            } else {
                archive_slot_num++;
            }
        }
        SpinLockRelease(&slot->mutex);
    }
    SpinLockAcquire(&g_instance.archive_obs_cxt.mutex);
    volatile int *slot_num = &g_instance.archive_obs_cxt.archive_slot_num;
    *slot_num = archive_slot_num;
    volatile int *recovery_slot_num = &g_instance.archive_obs_cxt.archive_recovery_slot_num;
    *recovery_slot_num = archive_recovery_slot_num;
    volatile int *slot_tline = &g_instance.archive_obs_cxt.slot_tline;
    *slot_tline = *slot_tline + 1;
    if (walrcv && archive_recovery_slot_num > 0) {
        walrcv->archive_slot = GetArchiveRecoverySlot();
    }
    SpinLockRelease(&g_instance.archive_obs_cxt.mutex);
}

#ifndef ENABLE_LITE_MODE
void get_hadr_cn_info(char* keyCn, bool* isExitKey, char* deleteCn, bool* isExitDelete, 
    ArchiveSlotConfig *archive_conf)
{
    size_t readLen = 0;
    *isExitKey = checkOBSFileExist(HADR_KEY_CN_FILE, &archive_conf->archive_config);
    if (*isExitKey) {
        readLen = obsRead(HADR_KEY_CN_FILE, 0, keyCn, MAXPGPATH, &archive_conf->archive_config);
        if (readLen == 0) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                    (errmsg("Cannot read  %s file!", HADR_KEY_CN_FILE))));
        }
    } else {
        ereport(LOG, ((errmsg("The hadr_key_cn file named %s cannot be found.", HADR_KEY_CN_FILE))));
    }

    *isExitDelete = checkOBSFileExist(HADR_DELETE_CN_FILE, &archive_conf->archive_config);
    if (*isExitDelete) {
        readLen = obsRead(HADR_DELETE_CN_FILE, 0, deleteCn, MAXPGPATH, &archive_conf->archive_config);
        if (readLen == 0) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                    (errmsg("Cannot read  %s file!", HADR_DELETE_CN_FILE))));
        }
    } else {
        ereport(LOG, ((errmsg("The file named %s cannot be found.", HADR_DELETE_CN_FILE))));
    }
}
#endif

void GetReplslotPath(char *path)
{
    if (ENABLE_DSS) {
        errno_t rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_replslot",
            g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = strcpy_s(path, MAXPGPATH, "pg_replslot");
        securec_check_ss(rc, "\0", "\0");
    }
}

static int RenameReplslotPath(char *path1, char *path2)
{
    int rc = 0;
    const char* suffix = ".tmp";
    if (ENABLE_DSS) {
        int lenth_of_path1 = strlen(path1);
        if (strstr(path1, suffix) == path1 + lenth_of_path1 - strlen(suffix)) {
            rc = symlink(path1, path2);
        } else {
            rc = unlink(path1);
        }
    } else {
        rc = rename(path1, path2);
    }
    return rc;
}

static bool CheckExistReplslotPath(char *path)
{
    char path_for_check[MAXPGPATH];
    const char* suffix = ".tmp";
    if (strstr(path, suffix) != NULL) {
        int count = strlen(path) - strlen(suffix);
        errno_t rc = strncpy_s(path_for_check, MAXPGPATH, path, count);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = strcat_s(path_for_check, MAXPGPATH, suffix);
        securec_check_ss(rc, "\0", "\0");
    }
    struct stat st;
    if (stat(path_for_check, &st) == 0 && S_ISDIR(st.st_mode)) {
        return true;
    }

    return false;
}

void ResetReplicationSlotsShmem()
{
    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        return;

    if (t_thrd.slot_cxt.ReplicationSlotCtl != NULL) {
        errno_t rc = 0;

        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
            if (slot != NULL && strlen(slot->data.name.data) != 0) {
                slot->in_use = false;
                slot->active = false;
                slot->just_dirtied = false;
                slot->dirty = false;
                slot->effective_xmin = 0;
                slot->effective_catalog_xmin = 0;
                rc = memset_s(&slot->data, sizeof(ReplicationSlotPersistentData), 0,
                    sizeof(ReplicationSlotPersistentData));
                securec_check(rc, "\0", "\0");
                slot->candidate_catalog_xmin = 0;
                slot->candidate_xmin_lsn = 0;
                slot->candidate_restart_valid = 0;
                slot->candidate_restart_lsn = 0;
                slot->is_recovery = false;
                slot->last_xmin_change_time = 0;
            }
        }
    }
}
