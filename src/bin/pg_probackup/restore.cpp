/*-------------------------------------------------------------------------
 *
 * restore.c: restore DB cluster and archived WAL.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"



#include <sys/stat.h>
#include <unistd.h>

#include "thread.h"
#include "common/fe_memutils.h"
#include "catalog/catalog.h"
#include "storage/file/fio_device.h"
#include "logger.h"
#include "oss/include/restore.h"

#define RESTORE_ARRAY_LEN 100

/* Progress Counter */
static int g_directoryFiles = 0;
static int g_doneFiles = 0;
static int g_totalFiles = 0;
static int g_syncFiles = 0;
static volatile bool g_progressFlag = false;
static volatile bool g_progressFlagSync = false;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct
{
    parray       *pgdata_and_dssdata_files;
    parray       *dest_files;
    pgBackup   *dest_backup;
    parray       *dest_external_dirs;
    parray       *parent_chain;
    bool        skip_external_dirs;
    const char *to_root;
    const char *to_dss;
    size_t        restored_bytes;
    bool        use_bitmap;
    IncrRestoreMode        incremental_mode;
    XLogRecPtr  shift_lsn;    /* used only in LSN incremental_mode */

    /*
     * Return value from the thread.
     * 0 means there is no error, 1 - there is an error.
     */
    int            ret;
} restore_files_arg;

static void create_recovery_conf(time_t backup_id,
                                 pgRecoveryTarget *rt,
                                 pgBackup *backup,
                                 pgRestoreParams *params);
static void construct_restore_cmd(FILE *fp, pgRecoveryTarget *rt,
                                  bool restore_command_provided,
                                  bool target_immediate);
static void *restore_files(void *arg);
static void set_orphan_status(parray *backups, pgBackup *parent_backup);
static void pg12_recovery_config(pgBackup *backup, bool add_include);

static void restore_chain(pgBackup *dest_backup, parray *parent_chain,
                          pgRestoreParams *params, const char *pgdata_path,
                          const char *dssdata_path, bool no_sync);
static void check_incremental_compatibility(const char *pgdata, uint64 system_identifier,
                                            IncrRestoreMode incremental_mode);
static pgBackup *find_backup_range(parray *backups,
                                   time_t target_backup_id,
                                   pgRecoveryTarget *rt,
                                   pgRestoreParams *params);
static void check_backup_status(pgBackup *current_backup, pgRestoreParams *params);
static pgBackup * find_full_backup(parray *backups,
                                   pgBackup *dest_backup,
                                   const char *action);
static XLogRecPtr determine_shift_lsn(pgBackup *dest_backup);
static void get_pgdata_and_dssdata_files(const char *pgdata_path,
                             const char *dssdata_path,
                             parray *pgdata_and_dssdata_files,
                             parray *external_dirs);
static bool skip_some_tblspc_files(pgFile *file);
static void remove_redundant_files(const char *pgdata_path,
                                   const char *dssdata_path,
                                   parray *pgdata_and_dssdata_files,
                                   pgBackup *dest_backup,
                                   parray *external_dirs);
static void threads_handle(pthread_t *threads,
                           restore_files_arg *threads_args,
                           pgBackup *dest_backup,
                           parray *dest_files,
                           parray *pgdata_files,
                           parray *external_dirs,
                           parray *parent_chain,
                           pgRestoreParams *params,
                           const char *pgdata_path,
                           const char *dssdata_path,
                           bool use_bitmap,
                           size_t total_bytes);
static void sync_restored_files(parray *dest_files,
                                parray *external_dirs,
                                pgRestoreParams *params,
                                const char *pgdata_path);
#ifdef SUPPORT_MULTI_TIMELINE
static void parse_file(parray *result, FILE *fd,
                       TimeLineHistoryEntry *entry,
                       TimeLineHistoryEntry *last_timeline);
#endif
static void parse_other_options(pgRecoveryTarget *rt,
                                int recovery_target_specified,
                                const char *target_inclusive,
                                const char *target_stop,
                                const char *target_action);

static void *ProgressReportRestore(void *arg);
static void *ProgressReportSyncRestoreFile(void *arg);
/*
 * Iterate over backup list to find all ancestors of the broken parent_backup
 * and update their status to BACKUP_STATUS_ORPHAN
 */
static void
set_orphan_status(parray *backups, pgBackup *parent_backup)
{
    /* chain is intact, but at least one parent is invalid */
    char    *parent_backup_id;
    size_t        j;

    /* parent_backup_id is a human-readable backup ID  */
    parent_backup_id = base36enc_dup(parent_backup->start_time);

    for (j = 0; j < parray_num(backups); j++)
    {

        pgBackup *backup = (pgBackup *) parray_get(backups, j);

        if (is_parent(parent_backup->start_time, backup, false))
        {
            if (backup->status == BACKUP_STATUS_OK ||
                backup->status == BACKUP_STATUS_DONE)
            {
                write_backup_status(backup, BACKUP_STATUS_ORPHAN, instance_name, true);

                elog(WARNING,
                    "Backup %s is orphaned because his parent %s has status: %s",
                    base36enc(backup->start_time),
                    parent_backup_id,
                    status2str(parent_backup->status));
            }
            else
            {
                elog(WARNING, "Backup %s has parent %s with status: %s",
                        base36enc(backup->start_time), parent_backup_id,
                        status2str(parent_backup->status));
            }
        }
    }
    pg_free(parent_backup_id);
}

/*
 * Entry point of pg_probackup RESTORE and VALIDATE subcommands.
 */
int
do_restore_or_validate(time_t target_backup_id, pgRecoveryTarget *rt,
                       pgRestoreParams *params, bool no_sync)
{
    int            i = 0;
    parray       *backups = NULL;
    pgBackup   *tmp_backup = NULL;
    pgBackup   *dest_backup = NULL;
    pgBackup   *base_full_backup = NULL;
    pgBackup   *corrupted_backup = NULL;
    const char       *action = (const char *)(params->is_restore ? "Restore":"Validate");
    parray       *parent_chain = NULL;
    bool        pgdata_is_empty = true;
    bool        dssdata_is_empty = true;
    bool        tblspaces_are_empty = true;

    if (params->is_restore)
    {
        if (instance_config.pgdata == NULL)
            elog(ERROR,
                "required parameter not specified: PGDATA (-D, --pgdata)");

        /* Check if restore destination empty : vgdata and vglog */
        if (IsDssMode())
        {
            if (!dir_is_empty(instance_config.dss.vgdata, FIO_DSS_HOST))
            {
                if (params->incremental_mode != INCR_NONE)
                {
                    elog(INFO, "Running incremental restore into nonempty directory: \"%s\"",
                        instance_config.dss.vgdata);
                }
                else
                {
                    elog(ERROR, "Restore destination is not empty: \"%s\"",
                        instance_config.dss.vgdata);
                }
                dssdata_is_empty = false;
            }
            if (!dir_is_empty(instance_config.dss.vglog, FIO_DSS_HOST))
            {
                if (params->incremental_mode != INCR_NONE)
                {
                    elog(INFO, "Running incremental restore into nonempty directory: \"%s\"",
                        instance_config.dss.vglog);
                }
                else
                {
                    elog(ERROR, "Restore destination is not empty: \"%s\"",
                        instance_config.dss.vglog);
                }
                dssdata_is_empty = false;
            }
        }

        /* Check if restore destination empty : PGDATA */
        if (!dir_is_empty(instance_config.pgdata, FIO_DB_HOST))
        {
            /* Check that remote system is NOT running and systemd id is the same as ours */
            if (params->incremental_mode != INCR_NONE)
            {
                elog(INFO, "Running incremental restore into nonempty directory: \"%s\"",
                     instance_config.pgdata);

                check_incremental_compatibility(instance_config.pgdata,
                                                instance_config.system_identifier,
                                                params->incremental_mode);
            }
            else
                elog(ERROR, "Restore destination is not empty: \"%s\"",
                     instance_config.pgdata);

            /* if destination directory is empty, then incremental restore may be disabled */
            pgdata_is_empty = false;
        }
    }

    if (instance_name == NULL)
        elog(ERROR, "required parameter not specified: --instance");

    elog(LOG, "%s begin.", action);

    /* Get list of all backups sorted in order of descending start time */
    backups = catalog_get_backup_list(instance_name, INVALID_BACKUP_ID);

    dest_backup = find_backup_range(backups, target_backup_id, rt, params);

    /* TODO: Show latest possible target */
    if (dest_backup == NULL)
    {
        /* Failed to find target backup */
        if (target_backup_id)
            elog(ERROR, "Requested backup %s is not found.", base36enc(target_backup_id));
        else
            elog(ERROR, "Backup satisfying target options is not found.");
        /* TODO: check if user asked PITR or just restore of latest backup */
    }

    /* If we already found dest_backup, look for full backup. */
    if (dest_backup->backup_mode == BACKUP_MODE_FULL)
        base_full_backup = dest_backup;
    else
    {
        base_full_backup = find_full_backup(backups, dest_backup, action);
    }

    if (base_full_backup == NULL)
        elog(ERROR, "Full backup satisfying target options is not found.");

    /*
     * Ensure that directories provided in tablespace mapping are valid
     * i.e. empty or not exist.
     */
    if (params->is_restore)
    {
        check_tablespace_mapping(dest_backup, params->incremental_mode != INCR_NONE, &tblspaces_are_empty);

        if (params->incremental_mode != INCR_NONE && pgdata_is_empty && tblspaces_are_empty && dssdata_is_empty)
        {
            elog(INFO, "Destination directory and tablespace directories are empty, "
                    "disable incremental restore");
            params->incremental_mode = INCR_NONE;
        }

        /* no point in checking external directories if their restore is not requested */
        if (!params->skip_external_dirs)
            check_external_dir_mapping(dest_backup, params->incremental_mode != INCR_NONE);
    }

    /* At this point we are sure that parent chain is whole
     * so we can build separate array, containing all needed backups,
     * to simplify validation and restore
     */
    parent_chain = parray_new();

    /* Take every backup that is a child of base_backup AND parent of dest_backup
     * including base_backup and dest_backup
     */

    tmp_backup = dest_backup;
    while (tmp_backup)
    {
        parray_append(parent_chain, tmp_backup);
        tmp_backup = tmp_backup->parent_backup_link;
    }

    /*
     * Determine the shift-LSN
     * Consider the example A:
     *
     *
     *              /----D----------F->
     * -A--B---C---*-------X----->
     *
     * [A,F] - incremental chain
     * X - the state of pgdata
     * F - destination backup
     * * - switch point
     *
     * When running incremental restore in 'lsn' mode, we get a bitmap of pages,
     * whose LSN is less than shift-LSN (backup C stop_lsn).
     * So when restoring file, we can skip restore of pages coming from
     * A, B and C.
     * Pages from D and F cannot be skipped due to incremental restore.
     *
     * Consider the example B:
     *
     *
     *      /----------X---->
     * ----*---A---B---C-->
     *
     * [A,C] - incremental chain
     * X - the state of pgdata
     * C - destination backup
     * * - switch point
     *
     * Incremental restore in shift mode IS NOT POSSIBLE in this case.
     * We must be able to differentiate the scenario A and scenario B.
     *
     */
    if (params->is_restore && params->incremental_mode == INCR_LSN)
    {
        params->shift_lsn = determine_shift_lsn(dest_backup);
    }

    /* for validation or restore with enabled validation */
    if (!params->is_restore || !params->no_validate)
    {
        if (dest_backup->backup_mode != BACKUP_MODE_FULL)
            elog(INFO, "Validating parents for backup %s", base36enc(dest_backup->start_time));

        /*
         * Validate backups from base_full_backup to dest_backup.
         */
        for (i = parray_num(parent_chain) - 1; i >= 0; i--)
        {
            tmp_backup = (pgBackup *) parray_get(parent_chain, i);

            /* Do not interrupt, validate the next backup */
            if (!lock_backup(tmp_backup, true))
            {
                if (params->is_restore)
                    elog(ERROR, "Cannot lock backup %s directory",
                         base36enc(tmp_backup->start_time));
                else
                {
                    elog(WARNING, "Cannot lock backup %s directory, skip validation",
                         base36enc(tmp_backup->start_time));
                    continue;
                }
            }

            /* validate datafiles only */
            if (current.media_type == MEDIA_TYPE_OSS && !params->is_restore &&
                tmp_backup->oss_status != OSS_STATUS_LOCAL) {
                performRestoreOrValidate(tmp_backup, true);
            } else if (current.media_type != MEDIA_TYPE_OSS || tmp_backup->oss_status == OSS_STATUS_LOCAL) {
                pgBackupValidate(tmp_backup, params);
                /* After pgBackupValidate() only following backup
                * states are possible: ERROR, RUNNING, CORRUPT and OK.
                * Validate WAL only for OK, because there is no point
                * in WAL validation for corrupted, errored or running backups.
                */
                if (tmp_backup->status != BACKUP_STATUS_OK)
                {
                    corrupted_backup = tmp_backup;
                    break;
                }
                /* We do not validate WAL files of intermediate backups
                * It`s done to speed up restore
                */
            }
        }

        /* There is no point in wal validation of corrupted backups */
        // TODO: there should be a way for a user to request only(!) WAL validation
        if(current.media_type != MEDIA_TYPE_OSS || tmp_backup->oss_status == OSS_STATUS_LOCAL) {
            if (!corrupted_backup)
            {
                /*
                * Validate corresponding WAL files.
                * We pass base_full_backup timeline as last argument to this function,
                * because it's needed to form the name of xlog file.
                */
                validate_wal(dest_backup, arclog_path, rt->target_time,
                            rt->target_xid, rt->target_lsn,
                            dest_backup->tli, instance_config.xlog_seg_size);
            }
            /* Orphanize every OK descendant of corrupted backup */
            else
                set_orphan_status(backups, corrupted_backup);
        }
    }

    /*
     * If dest backup is corrupted or was orphaned in previous check
     * produce corresponding error message
     */
    if (dest_backup->status == BACKUP_STATUS_OK ||
        dest_backup->status == BACKUP_STATUS_DONE)
    {
        if (params->no_validate)
            elog(WARNING, "Backup %s is used without validation.", base36enc(dest_backup->start_time));
        else
            elog(INFO, "Backup %s is valid.", base36enc(dest_backup->start_time));
    }
    else if (dest_backup->status == BACKUP_STATUS_CORRUPT)
    {
        if (params->force)
            elog(WARNING, "Backup %s is corrupt.", base36enc(dest_backup->start_time));
        else
            elog(ERROR, "Backup %s is corrupt.", base36enc(dest_backup->start_time));
    }
    else if (dest_backup->status == BACKUP_STATUS_ORPHAN)
    {
        if (params->force)
            elog(WARNING, "Backup %s is orphan.", base36enc(dest_backup->start_time));
        else
            elog(ERROR, "Backup %s is orphan.", base36enc(dest_backup->start_time));
    }
    else
        elog(ERROR, "Backup %s has status: %s",
                base36enc(dest_backup->start_time), status2str(dest_backup->status));

    /* We ensured that all backups are valid, now restore if required
     */
    if (params->is_restore)
    {

        restore_chain(dest_backup, parent_chain, params, instance_config.pgdata,
                      instance_config.dss.vgdata, no_sync);

        /* Create recovery.conf with given recovery target parameters */
        create_recovery_conf(target_backup_id, rt, dest_backup, params);
    }

    /* ssh connection to longer needed */
    fio_disconnect();

    elog(INFO, "%s of backup %s completed.",
         action, base36enc(dest_backup->start_time));

    /* cleanup */
    parray_walk(backups, pgBackupFree);
    parray_free(backups);
    parray_free(parent_chain);

    return 0;
}

static pgBackup *find_backup_range(parray *backups,
                                   time_t target_backup_id,
                                   pgRecoveryTarget *rt,
                                   pgRestoreParams *params)
{
    size_t      i = 0;
    pgBackup *dest_backup = NULL;
    pgBackup *current_backup = NULL;

    /* Find backup range we should restore or validate. */
    while ((i < parray_num(backups)) && !dest_backup)
    {
        current_backup = (pgBackup *) parray_get(backups, i);
        i++;

        /* Skip all backups which started after target backup */
        if (target_backup_id && current_backup->start_time > target_backup_id)
            continue;

        /*
         * [PGPRO-1164] If BACKUP_ID is not provided for restore command,
         *  we must find the first valid(!) backup.

         * If target_backup_id is not provided, we can be sure that
         * PITR for restore or validate is requested.
         * So we can assume that user is more interested in recovery to specific point
         * in time and NOT interested in revalidation of invalid backups.
         * So based on that assumptions we should choose only OK and DONE backups
         * as candidates for validate and restore.
         */

        if (target_backup_id == INVALID_BACKUP_ID &&
            (current_backup->status != BACKUP_STATUS_OK &&
             current_backup->status != BACKUP_STATUS_DONE))
        {
            elog(WARNING, "Skipping backup %s, because it has non-valid status: %s",
                base36enc(current_backup->start_time), status2str(current_backup->status));
            continue;
        }

        /*
         * We found target backup. Check its status and
         * ensure that it satisfies recovery target.
         */
        if ((target_backup_id == current_backup->start_time
            || target_backup_id == INVALID_BACKUP_ID))
        {
            check_backup_status(current_backup, params);

            if (rt->target_tli)
            {
                parray       *timelines;

                /* Read timeline history files from archives */
                timelines = read_timeline_history(arclog_path, rt->target_tli, true);

                if (!satisfy_timeline(timelines, current_backup))
                {
                    if (target_backup_id != INVALID_BACKUP_ID)
                        elog(ERROR, "target backup %s does not satisfy target timeline",
                             base36enc(target_backup_id));
                    else
                        /* Try to find another backup that satisfies target timeline */
                        continue;
                }

                parray_walk(timelines, pfree);
                parray_free(timelines);
            }

            if (!satisfy_recovery_target(current_backup, rt))
            {
                if (target_backup_id != INVALID_BACKUP_ID)
                    elog(ERROR, "Requested backup %s does not satisfy restore options",
                         base36enc(target_backup_id));
                else
                    /* Try to find another backup that satisfies target options */
                    continue;
            }

            /*
             * Backup is fine and satisfies all recovery options.
             * Save it as dest_backup
             */
            dest_backup = current_backup;
            return dest_backup;
        }
    }

    return dest_backup;
}

static void check_backup_status(pgBackup *current_backup, pgRestoreParams *params)
{
    /* backup is not ok,
     * but in case of CORRUPT or ORPHAN revalidation is possible
     * unless --no-validate is used,
     * in other cases throw an error.
     */
    // 1. validate
    // 2. validate -i INVALID_ID <- allowed revalidate
    // 3. restore -i INVALID_ID <- allowed revalidate and restore
    // 4. restore <- impossible
    // 5. restore --no-validate <- forbidden
    if (current_backup->status != BACKUP_STATUS_OK &&
        current_backup->status != BACKUP_STATUS_DONE)
    {
        if ((current_backup->status == BACKUP_STATUS_ORPHAN ||
            current_backup->status == BACKUP_STATUS_CORRUPT ||
            current_backup->status == BACKUP_STATUS_RUNNING)
            && (!params->no_validate || params->force)) {
            elog(WARNING, "Backup %s has status: %s",
                    base36enc(current_backup->start_time), status2str(current_backup->status));
        } else {
            elog(ERROR, "Backup %s has status: %s",
                    base36enc(current_backup->start_time), status2str(current_backup->status));
        }
    }
}

static pgBackup *find_full_backup(parray *backups,
                                  pgBackup *dest_backup,
                                  const char *action)
{
    pgBackup *tmp_backup = NULL;
    int result;

    result = scan_parent_chain(dest_backup, &tmp_backup);

    if (result == ChainIsBroken)
    {
        /* chain is broken, determine missing backup ID
         * and orphinize all his descendants
         */
        char       *missing_backup_id;
        time_t        missing_backup_start_time;

        missing_backup_start_time = tmp_backup->parent_backup;
        missing_backup_id = base36enc_dup(tmp_backup->parent_backup);

        for (size_t j = 0; j < parray_num(backups); j++)
        {
            pgBackup *backup = (pgBackup *) parray_get(backups, j);

            /* use parent backup start_time because he is missing
             * and we must orphinize his descendants
             */
            if (is_parent(missing_backup_start_time, backup, false))
            {
                if (backup->status == BACKUP_STATUS_OK ||
                    backup->status == BACKUP_STATUS_DONE)
                {
                    write_backup_status(backup, BACKUP_STATUS_ORPHAN, instance_name, true);

                    elog(WARNING, "Backup %s is orphaned because his parent %s is missing",
                            base36enc(backup->start_time), missing_backup_id);
                }
                else
                {
                    elog(WARNING, "Backup %s has missing parent %s",
                            base36enc(backup->start_time), missing_backup_id);
                }
            }
        }
        pg_free(missing_backup_id);
        /* No point in doing futher */
        elog(ERROR, "%s of backup %s failed.", action, base36enc(dest_backup->start_time));
    }
    else if (result == ChainIsInvalid)
    {
        /* chain is intact, but at least one parent is invalid */
        set_orphan_status(backups, tmp_backup);
        tmp_backup = find_parent_full_backup(dest_backup);

        /* sanity */
        if (!tmp_backup)
            elog(ERROR, "Parent full backup for the given backup %s was not found",
                    base36enc(dest_backup->start_time));
    }

    /* We have found full backup */
    return tmp_backup;
}

static XLogRecPtr determine_shift_lsn(pgBackup *dest_backup)
{
    RedoParams redo;
    parray     *timelines  = NULL;
    pgBackup   *tmp_backup = NULL;
    XLogRecPtr shift_lsn   = InvalidXLogRecPtr;

    get_redo(instance_config.pgdata, &redo);

    if (redo.checksum_version == 0)
        elog(INFO, "Incremental restore in 'lsn' mode require "
            "data_checksums to be enabled in destination data directory");

    timelines = read_timeline_history(arclog_path, redo.tli, false);

    if (!timelines)
        elog(WARNING, "Failed to get history for redo timeline %i, "
            "multi-timeline incremental restore in 'lsn' mode is impossible", redo.tli);

    tmp_backup = dest_backup;

    while (tmp_backup)
    {
        /* Candidate, whose stop_lsn if less than shift LSN, is found */
        if (tmp_backup->stop_lsn < redo.lsn)
        {
            /* if candidate timeline is the same as redo TLI,
             * then we are good to go.
             */
            if (redo.tli == tmp_backup->tli)
            {
                elog(INFO, "Backup %s is chosen as shiftpoint, its Stop LSN will be used as shift LSN",
                    base36enc(tmp_backup->start_time));

                shift_lsn = tmp_backup->stop_lsn;
                break;
            }

            if (!timelines)
            {
                elog(WARNING, "Redo timeline %i differs from target timeline %i, "
                    "in this case, to safely run incremental restore in 'lsn' mode, "
                    "the history file for timeline %i is mandatory",
                    redo.tli, tmp_backup->tli, redo.tli);
                break;
            }

            /* check whether the candidate tli is a part of redo TLI history */
            if (tliIsPartOfHistory(timelines, tmp_backup->tli))
            {
                shift_lsn = tmp_backup->stop_lsn;
                break;
            }
            else
                elog(INFO, "Backup %s cannot be a shiftpoint, "
                        "because its tli %i is not in history of redo timeline %i",
                    base36enc(tmp_backup->start_time), tmp_backup->tli, redo.tli);
        }

        tmp_backup = tmp_backup->parent_backup_link;
    }

    if (XLogRecPtrIsInvalid(shift_lsn))
        elog(ERROR, "Cannot perform incremental restore of backup chain %s in 'lsn' mode, "
                    "because destination directory redo point %X/%X on tli %i is out of reach",
                base36enc(dest_backup->start_time),
                (uint32) (redo.lsn >> 32), (uint32) redo.lsn, redo.tli);
    else
        elog(INFO, "Destination directory redo point %X/%X on tli %i is "
                "within reach of backup %s with Stop LSN %X/%X on tli %i",
            (uint32) (redo.lsn >> 32), (uint32) redo.lsn, redo.tli,
            base36enc(tmp_backup->start_time),
            (uint32) (tmp_backup->stop_lsn >> 32), (uint32) tmp_backup->stop_lsn,
            tmp_backup->tli);

    elog(INFO, "shift LSN: %X/%X",
        (uint32) (shift_lsn >> 32), (uint32) shift_lsn);

    return shift_lsn;
}

/*
 * Restore backup chain.
 */
void
restore_chain(pgBackup *dest_backup, parray *parent_chain,
              pgRestoreParams *params, const char *pgdata_path,
              const char *dssdata_path, bool no_sync)
{
    int         i;
    char        timestamp[100];
    parray      *pgdata_and_dssdata_files = NULL;
    parray      *dest_files = NULL;
    parray      *external_dirs = NULL;
    /* arrays with meta info for multi threaded backup */
    pthread_t   *threads;
    restore_files_arg *threads_args;
    bool        use_bitmap = true;
    /* fancy reporting */
    size_t      total_bytes = 0;

    /* Preparations for actual restoring */
    time2iso(timestamp, lengthof(timestamp), dest_backup->start_time);
    elog(INFO, "Restoring the database from backup at %s", timestamp);

    dest_files = get_backup_filelist(dest_backup, true);

    /* Lock backup chain and make sanity checks */
    for (i = parray_num(parent_chain) - 1; i >= 0; i--)
    {
        pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

        if (!lock_backup(backup, true))
            elog(ERROR, "Cannot lock backup %s", base36enc(backup->start_time));

        if (backup->status != BACKUP_STATUS_OK &&
            backup->status != BACKUP_STATUS_DONE)
        {
            if (params->force)
                elog(WARNING, "Backup %s is not valid, restore is forced",
                     base36enc(backup->start_time));
            else
                elog(ERROR, "Backup %s cannot be restored because it is not valid",
                     base36enc(backup->start_time));
        }

        /* confirm block size compatibility */
        if (backup->block_size != BLCKSZ)
            elog(ERROR,
                "BLCKSZ(%d) is not compatible(%d expected)",
                backup->block_size, BLCKSZ);

        if (backup->wal_block_size != XLOG_BLCKSZ)
            elog(ERROR,
                "XLOG_BLCKSZ(%d) is not compatible(%d expected)",
                backup->wal_block_size, XLOG_BLCKSZ);

        /* populate backup filelist */
        if (backup->start_time != dest_backup->start_time)
            backup->files = get_backup_filelist(backup, true);
        else
            backup->files = dest_files;

        /*
         * this sorting is important, because we rely on it to find
         * destination file in intermediate backups file lists
         * using bsearch.
         */
        parray_qsort(backup->files, pgFileCompareRelPathWithExternal);
    }

    /* If dest backup version is older than 2.4.0, then bitmap optimization
     * is impossible to use, because bitmap restore rely on pgFile.n_blocks,
     * which is not always available in old backups.
     */
    if (parse_program_version(dest_backup->program_version) < 20400)
    {
        use_bitmap = false;

        if (params->incremental_mode != INCR_NONE)
            elog(ERROR, "incremental restore is not possible for backups older than 2.3.0 version");
    }

    /* There is no point in bitmap restore, when restoring a single FULL backup,
     * unless we are running incremental-lsn restore, then bitmap is mandatory.
     */
    if (use_bitmap && parray_num(parent_chain) == 1)
    {
        if (params->incremental_mode == INCR_NONE)
            use_bitmap = false;
        else
            use_bitmap = true;
    }

    /*
     * Restore dest_backup internal directories.
     */

    create_data_directories(dest_files, instance_config.pgdata, NULL,
                            dest_backup->root_dir, true,
                            params->incremental_mode != INCR_NONE,
                            FIO_DB_HOST, true);

    /* some file is in dssserver */
    if (IsDssMode())
        create_data_directories(dest_files, instance_config.dss.vgdata, instance_config.dss.vglog,
                                dest_backup->root_dir, true, params->incremental_mode != INCR_NONE,
                                FIO_DSS_HOST, true);

    /*
     * Restore dest_backup external directories.
     */
    if (dest_backup->external_dir_str && !params->skip_external_dirs)
    {
        external_dirs = make_external_directory_list(dest_backup->external_dir_str, true);

        if (!external_dirs)
            elog(ERROR, "Failed to get a list of external directories");

        if (parray_num(external_dirs) > 0)
            elog(LOG, "Restore external directories");

        for (i = 0; (size_t)i < parray_num(external_dirs); i++)
            fio_mkdir((const char *)parray_get(external_dirs, i),
                      DIR_PERMISSION, FIO_DB_HOST);
    }

    /* Get list of files in destination directory and remove redundant files */
    if (params->incremental_mode != INCR_NONE)
    {
        pgdata_and_dssdata_files = parray_new();
        get_pgdata_and_dssdata_files(pgdata_path, dssdata_path, pgdata_and_dssdata_files, external_dirs);
        remove_redundant_files(pgdata_path, dssdata_path, pgdata_and_dssdata_files,
                                            dest_backup, external_dirs);
    }

    /*
     * Setup directory structure for external directories and file locks
     */
    for (i = 0; (size_t)i < parray_num(dest_files); i++)
    {
        pgFile       *file = (pgFile *) parray_get(dest_files, i);

        if (S_ISDIR(file->mode))
            total_bytes += 4096;

        if (!params->skip_external_dirs &&
            file->external_dir_num && S_ISDIR(file->mode))
        {
            char       *external_path;
            char        dirpath[MAXPGPATH];

            if ((int)parray_num(external_dirs) < file->external_dir_num - 1)
                elog(ERROR, "Inconsistent external directory backup metadata");

            external_path = (char *)parray_get(external_dirs, file->external_dir_num - 1);
            join_path_components(dirpath, external_path, file->rel_path);

            elog(VERBOSE, "Create external directory \"%s\"", dirpath);
            fio_mkdir(dirpath, file->mode, FIO_DB_HOST);
        }

        /* setup threads */
        pg_atomic_clear_flag(&file->lock);
    }


    /*
     * Close ssh connection belonging to the main thread
     * to avoid the possibility of been killed for idleness
     */
    fio_disconnect();

    threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
    threads_args = (restore_files_arg *) palloc(sizeof(restore_files_arg) *
                                                num_threads);

    threads_handle(threads, threads_args, dest_backup, dest_files,
                   pgdata_and_dssdata_files, external_dirs, parent_chain, params,
                   pgdata_path, dssdata_path, use_bitmap, total_bytes);

    /* Close page header maps */
    for (i = parray_num(parent_chain) - 1; i >= 0; i--)
    {
        pgBackup   *backup = (pgBackup *)parray_get(parent_chain, i);
        cleanup_header_map(&(backup->hdr_map));
    }

    if (no_sync)
        elog(WARNING, "Restored files are not synced to disk");
    else
    {
        sync_restored_files(dest_files, external_dirs, params, pgdata_path);
    }

    /* cleanup */
    pfree(threads);
    pfree(threads_args);

    if (external_dirs != NULL)
        free_dir_list(external_dirs);

    if (pgdata_and_dssdata_files)
    {
        parray_walk(pgdata_and_dssdata_files, pgFileFree);
        parray_free(pgdata_and_dssdata_files);
    }

    for (i = parray_num(parent_chain) - 1; i >= 0; i--)
    {
        pgBackup   *backup = (pgBackup *)parray_get(parent_chain, i);

        parray_walk(backup->files, pgFileFree);
        parray_free(backup->files);
    }
}

static void get_pgdata_and_dssdata_files(const char *pgdata_path,
                             const char *dssdata_path,
                             parray *pgdata_and_dssdata_files,
                             parray *external_dirs)
{
    char   pretty_time[20];
    time_t start_time, end_time;

    elog(INFO, "Extracting the content of destination directory for incremental restore");

    time(&start_time);
    if (fio_is_remote(FIO_DB_HOST))
        fio_list_dir(pgdata_and_dssdata_files, pgdata_path, false, true, false, false, true, 0);
    else
        dir_list_file(pgdata_and_dssdata_files, pgdata_path,
                      false, true, false, false, true, 0, FIO_LOCAL_HOST);

    if (IsDssMode())
        dir_list_file(pgdata_and_dssdata_files, dssdata_path,
                      false, true, false, false, true, 0, FIO_DSS_HOST);

    /* get external dirs content */
    if (external_dirs)
    {
        for (int i = 0; (size_t)i < parray_num(external_dirs); i++)
        {
            char *external_path = (char *)parray_get(external_dirs, i);
            parray *external_files = parray_new();

            if (fio_is_remote(FIO_DB_HOST))
                fio_list_dir(external_files, external_path,
                             false, true, false, false, true, i+1);
            else
                dir_list_file(external_files, external_path,
                              false, true, false, false, true, i+1,
                              FIO_LOCAL_HOST);

            parray_concat(pgdata_and_dssdata_files, external_files);
            parray_free(external_files);
        }
    }

    parray_qsort(pgdata_and_dssdata_files, pgFileCompareRelPathWithExternalDesc);

    time(&end_time);
    pretty_time_interval(difftime(end_time, start_time),
                         pretty_time, lengthof(pretty_time));

    elog(INFO, "Destination directory content extracted, time elapsed: %s",
         pretty_time);
}

static bool skip_some_tblspc_files(pgFile *file)
{
    Oid     tblspcOid;
    int     sscanf_res;
    char    tmp_rel_path[MAXPGPATH];
    bool    equ_tbs_version_dir = false;
    bool    prefix_equ_tbs_version_dir = false;

    sscanf_res = sscanf_s(file->rel_path, PG_TBLSPC_DIR "/%u/%[^/]/",
                          &tblspcOid, tmp_rel_path, sizeof(tmp_rel_path));
    equ_tbs_version_dir = (strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) == 0);
    prefix_equ_tbs_version_dir = (strncmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY, 
                                  strlen(TABLESPACE_VERSION_DIRECTORY)) == 0);

    /* In DSS mode, we should skip PG_9.2_201611171 */
    if (sscanf_res == 2 && IsDssMode() && prefix_equ_tbs_version_dir)
        return true;
    /* If the dss is not enabled, we should skip PG_9.2_201611171_node1 */
    if (sscanf_res == 2 && !equ_tbs_version_dir && prefix_equ_tbs_version_dir)
        return true;
    return false;
}

#define CHECK_FALSE                     0
#define CHECK_TRUE                      1
#define CHECK_EXCLUDE_FALSE             2

/*
 * Print a progress report based on the global variables.
 * Execute this function in another thread and print the progress periodically.
 */
static void *ProgressReportRestore(void *arg)
{
    if (g_totalFiles == 0) {
        return nullptr;
    }
    char progressBar[53];
    int percent;
    do {
        /* progress report */
        percent = (int)(g_doneFiles * 100 / g_totalFiles);
        GenerateProgressBar(percent, progressBar);
        fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). Restore file \r",
            progressBar, percent, g_doneFiles, g_totalFiles);
        pthread_mutex_lock(&g_mutex);
        timespec timeout;
        timeval now;
        gettimeofday(&now, nullptr);
        timeout.tv_sec = now.tv_sec + 1;
        timeout.tv_nsec = 0;
        int ret = pthread_cond_timedwait(&g_cond, &g_mutex, &timeout);
        pthread_mutex_unlock(&g_mutex);
        if (ret == ETIMEDOUT) {
            continue;
        } else {
            break;
        }        
    } while (((g_doneFiles + g_directoryFiles) < g_totalFiles) && g_progressFlag);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). Restore file \n",
        progressBar, percent, g_totalFiles, g_totalFiles);
    return nullptr;
}

static void *ProgressReportSyncRestoreFile(void *arg)
{
    if (g_totalFiles == 0) {
        return nullptr;
    }
    char progressBar[53];
    int percent;
    do {
        /* progress report */
        percent = (int)(g_syncFiles * 100 / g_totalFiles);
        GenerateProgressBar(percent, progressBar);
        fprintf(stdout, "Progress: %s %d%% (%d/%d, sync_files/total_files). Sync restore file \r",
            progressBar, percent, g_syncFiles, g_totalFiles);
        pthread_mutex_lock(&g_mutex);
        timespec timeout;
        timeval now;
        gettimeofday(&now, nullptr);
        timeout.tv_sec = now.tv_sec + 1;
        timeout.tv_nsec = 0;
        int ret = pthread_cond_timedwait(&g_cond, &g_mutex, &timeout);
        pthread_mutex_unlock(&g_mutex);
        if (ret == ETIMEDOUT) {
            continue;
        } else {
            break;
        }
    } while ((g_syncFiles < g_totalFiles) && !g_progressFlagSync);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%d/%d, done_files/total_files). Sync restore file \n",
        progressBar, percent, g_totalFiles, g_totalFiles);
    return nullptr;
}

static void remove_redundant_files(const char *pgdata_path,
                                   const char *dssdata_path,
                                   parray *pgdata_and_dssdata_files,
                                   pgBackup *dest_backup,
                                   parray *external_dirs)
{
    char   pretty_time[20];
    time_t start_time, end_time;

    elog(INFO, "Removing redundant files in destination directory");
    time(&start_time);
    for (int i = 0; (size_t)i < parray_num(pgdata_and_dssdata_files); i++) {
        pgFile	   *file = (pgFile *)parray_get(pgdata_and_dssdata_files, i);
        bool    in_tablespace = false;

        /* For incremental backups, we need to skip some files */
        in_tablespace = path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path);
        if (in_tablespace && skip_some_tblspc_files(file))
            continue;

        /* if file does not exists in destination list, then we can safely unlink it */
        if (parray_bsearch(dest_backup->files, file, 
            pgFileCompareRelPathWithExternal) == NULL) {
            char    fullpath[MAXPGPATH];
            fio_location path_location;

            if (file->external_dir_num) {
                char *external_path = (char *)parray_get(external_dirs,
                                                         file->external_dir_num - 1);
                join_path_components(fullpath, external_path, file->rel_path);                
            } else if (is_dss_type(file->type)) {
                join_path_components(fullpath, dssdata_path, file->rel_path);
            } else {
                join_path_components(fullpath, pgdata_path, file->rel_path);
            }            

            path_location = is_dss_type(file->type) ? FIO_DSS_HOST : FIO_DB_HOST;
            fio_delete(file->mode, fullpath, path_location);
            elog(VERBOSE, "Deleted file \"%s\"", fullpath);

            /* shrink pgdata list */
            parray_remove(pgdata_and_dssdata_files, i);
            i--;
        }
    }

    time(&end_time);
    pretty_time_interval(difftime(end_time, start_time),
                         pretty_time, lengthof(pretty_time));

    /* At this point PDATA and DSSDATA do not contain files, that do not exists in dest backup file list */
    elog(INFO, "Redundant files are removed, time elapsed: %s", pretty_time);
}

static void threads_handle(pthread_t *threads,
                           restore_files_arg *threads_args,
                           pgBackup *dest_backup,
                           parray *dest_files,
                           parray *pgdata_and_dssdata_files,
                           parray *external_dirs,
                           parray *parent_chain,
                           pgRestoreParams *params,
                           const char *pgdata_path,
                           const char *dssdata_path,
                           bool use_bitmap,
                           size_t total_bytes)
{
    int    i = 0;
    size_t dest_bytes = 0;
    char   pretty_dest_bytes[20];
    char   pretty_total_bytes[20];
    char   pretty_time[20];
    time_t start_time, end_time;
    bool   restore_isok = true;

    if (dest_backup->stream)
        dest_bytes = dest_backup->pgdata_bytes + dest_backup->dssdata_bytes + dest_backup->wal_bytes;
    else
        dest_bytes = dest_backup->pgdata_bytes + dest_backup->dssdata_bytes;

    pretty_size(dest_bytes, pretty_dest_bytes, lengthof(pretty_dest_bytes));
    elog(INFO, "Start restoring backup files. DATA size: %s", pretty_dest_bytes);
    time(&start_time);
    thread_interrupted = false;

    g_totalFiles = (unsigned long) parray_num(dest_files);
    elog(INFO, "Begin restore file");
    pthread_t progressThread;
    pthread_create(&progressThread, nullptr, ProgressReportRestore, nullptr);

    /* Restore files into target directory */
    if (current.media_type == MEDIA_TYPE_OSS) {
        for (i = parray_num(parent_chain) - 1; i >= 0; i--) {
            pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);
            if (!lock_backup(backup, true)) {
                elog(ERROR, "Cannot lock backup %s", base36enc(backup->start_time));
            }
            if (backup->oss_status == OSS_STATUS_LOCAL) {
                continue;
            }
            if (backup->status != BACKUP_STATUS_OK &&
                backup->status != BACKUP_STATUS_DONE) {
                if (params->force)
                    elog(WARNING, "Backup %s is not valid, restore is forced",
                        base36enc(backup->start_time));
                else
                    elog(ERROR, "Backup %s cannot be restored because it is not valid",
                        base36enc(backup->start_time));
            }
            /* confirm block size compatibility */
            if (backup->block_size != BLCKSZ)
                elog(ERROR,
                    "BLCKSZ(%d) is not compatible(%d expected)",
                    backup->block_size, BLCKSZ);
            if (backup->wal_block_size != XLOG_BLCKSZ)
                elog(ERROR,
                    "XLOG_BLCKSZ(%d) is not compatible(%d expected)",
                    backup->wal_block_size, XLOG_BLCKSZ);
            performRestoreOrValidate(backup, false);
            /* Backup is downloaded. Update backup status */
            backup->end_time = time(NULL);
            backup->oss_status = OSS_STATUS_LOCAL;
            write_backup(backup, true);
        }
    }
    
    for (i = 0; i < num_threads; i++)
    {
        restore_files_arg *arg = &(threads_args[i]);

        arg->dest_files = dest_files;
        arg->pgdata_and_dssdata_files = pgdata_and_dssdata_files;
        arg->dest_backup = dest_backup;
        arg->dest_external_dirs = external_dirs;
        arg->parent_chain = parent_chain;
        arg->skip_external_dirs = params->skip_external_dirs;
        arg->to_root = pgdata_path;
        arg->to_dss = dssdata_path;
        arg->use_bitmap = use_bitmap;
        arg->incremental_mode = params->incremental_mode;
        arg->shift_lsn = params->shift_lsn;
        threads_args[i].restored_bytes = 0;
        /* By default there are some error */
        threads_args[i].ret = 1;

        pthread_create(&threads[i], NULL, restore_files, arg);
    }

    /* Wait theads */
    for (i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
        if (threads_args[i].ret == 1)
            restore_isok = false;

        total_bytes += threads_args[i].restored_bytes;
    }

    time(&end_time);
    
    g_progressFlag = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_cond);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThread, nullptr);

    elog(INFO, "Finish restore file");

    pretty_time_interval(difftime(end_time, start_time),
                         pretty_time, lengthof(pretty_time));
    pretty_size(total_bytes, pretty_total_bytes, lengthof(pretty_total_bytes));

    if (restore_isok)
    {
        elog(INFO, "Backup files are restored. Transfered bytes: %s, time elapsed: %s",
            pretty_total_bytes, pretty_time);

        elog(INFO, "Restore incremental ratio (less is better): %.f%% (%s/%s)",
            ((float) total_bytes / dest_bytes) * 100,
            pretty_total_bytes, pretty_dest_bytes);
    }
    else
        elog(ERROR, "Backup files restoring failed. Transfered bytes: %s, time elapsed: %s",
            pretty_total_bytes, pretty_time);
}

static void sync_restored_files(parray *dest_files,
                                parray *external_dirs,
                                pgRestoreParams *params,
                                const char *pgdata_path)
{
    char        pretty_time[20];
    time_t      start_time, end_time;
    
    elog(INFO, "Start Syncing restored files to disk");
    pthread_t progressThread;
    pthread_create(&progressThread, nullptr, ProgressReportSyncRestoreFile, nullptr);
    time(&start_time);
    for (size_t i = 0; i < parray_num(dest_files); i++)
    {
        char        to_fullpath[MAXPGPATH];
        pgFile       *dest_file = (pgFile *)parray_get(dest_files, i);
        g_syncFiles++;

        if (S_ISDIR(dest_file->mode))
            continue;

        /* skip external files if ordered to do so */
        if (dest_file->external_dir_num > 0 &&
            params->skip_external_dirs)
            continue;

        /* skip dss files, which do not need sync */
        if (is_dss_type(dest_file->type))
            continue;

        /* construct fullpath */
        if (dest_file->external_dir_num == 0)
        {
            if (strcmp(PG_TABLESPACE_MAP_FILE, dest_file->rel_path) == 0)
                continue;
            if (strcmp(DATABASE_MAP, dest_file->rel_path) == 0)
                continue;
            join_path_components(to_fullpath, pgdata_path, dest_file->rel_path);
        }
        else
        {
            char *external_path = (char *)parray_get(external_dirs, dest_file->external_dir_num - 1);
            join_path_components(to_fullpath, external_path, dest_file->rel_path);
        }

        /* TODO: write test for case: file to be synced is missing */
        if (fio_sync(to_fullpath, FIO_DB_HOST) != 0)
            elog(ERROR, "Failed to sync file \"%s\": %s", to_fullpath, strerror(errno));
    }

    time(&end_time);
    pretty_time_interval(difftime(end_time, start_time),
                         pretty_time, lengthof(pretty_time));
    g_progressFlagSync = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_cond);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThread, nullptr);
    elog(INFO, "Finish Syncing restored files.");
    elog(INFO, "Restored backup files are synced, time elapsed: %s", pretty_time);
}

inline void RestoreCompressFile(FILE *out, char *to_fullpath, pgFile *dest_file)
{
    if (dest_file->is_datafile && dest_file->compressed_file && !dest_file->is_cfs) {
        if (!fio_is_remote_file(out)) {
            COMPRESS_ERROR_STATE result = ConstructCompressedFile(
                to_fullpath, dest_file->compressed_chunk_size, dest_file->compressed_algorithm);
            if (result != SUCCESS) {
                elog(ERROR, "Cannot copy compressed file \"%s\": %s", to_fullpath, strerror(errno));
            }
        } else {
            CompressCommunicate communicate;
            errno_t rc = memcpy_s(communicate.path, MAXPGPATH, to_fullpath, MAXPGPATH);
            securec_check(rc, "\0", "\0");
            communicate.chunkSize = dest_file->compressed_chunk_size;
            communicate.segmentNo = (uint32)dest_file->segno;
            communicate.algorithm = dest_file->compressed_algorithm;
            fio_construct_compressed((void*)&communicate, sizeof(communicate));
        }
    }
}

/*
 * Restore files into $PGDATA and $VGNAME.
 */
static void *
restore_files(void *arg)
{
    int         i;
    uint64      n_files;
    char        to_fullpath[MAXPGPATH];
    FILE       *out = NULL;
    char       *out_buf = (char *)pgut_malloc(STDIO_BUFSIZE);
    fio_location out_location;

    int directoryFilesLocal = 0;
    restore_files_arg *arguments = (restore_files_arg *) arg;

    n_files = (unsigned long) parray_num(arguments->dest_files);

    for (i = 0; (size_t)i < parray_num(arguments->dest_files); i++)
    {
        bool     already_exists = false;
        PageState      *checksum_map = NULL; /* it should take ~1.5MB at most */
        datapagemap_t  *lsn_map = NULL;      /* it should take 16kB at most */
        pgFile    *dest_file = (pgFile *)parray_get(arguments->dest_files, i);
        /* Directories were created before */
        if (S_ISDIR(dest_file->mode)) {
            directoryFilesLocal++;
            continue;
        }

        if (!pg_atomic_test_set_flag(&dest_file->lock))
            continue;

        pg_atomic_add_fetch_u32((volatile uint32*) &g_doneFiles, 1);
        /* check for interrupt */
        if (interrupted || thread_interrupted)
            elog(ERROR, "Interrupted during restore");

        if (progress)
            elog_file(INFO, "Progress: (%d/%lu). Restore file \"%s\"",
                i + 1, n_files, dest_file->rel_path);

        /* Do not restore tablespace_map file */
        if ((dest_file->external_dir_num == 0) &&
            strcmp(PG_TABLESPACE_MAP_FILE, dest_file->rel_path) == 0)
        {
            elog(VERBOSE, "Skip tablespace_map");
            continue;
        }

        /* Do not restore database_map file */
        if ((dest_file->external_dir_num == 0) &&
            strcmp(DATABASE_MAP, dest_file->rel_path) == 0)
        {
            elog(VERBOSE, "Skip database_map");
            continue;
        }

        /* Do no restore external directory file if a user doesn't want */
        if (arguments->skip_external_dirs && dest_file->external_dir_num > 0)
            continue;

        /* set fullpath of destination file */
        if (dest_file->external_dir_num != 0)
        {
            char    *external_path = (char *)parray_get(arguments->dest_external_dirs,
                                                        dest_file->external_dir_num - 1);
            join_path_components(to_fullpath, external_path, dest_file->rel_path);
        }
        else if (is_dss_type(dest_file->type))
        {
            join_path_components(to_fullpath, arguments->to_dss, dest_file->rel_path);
        }
        else
            join_path_components(to_fullpath, arguments->to_root, dest_file->rel_path);

        if (arguments->incremental_mode != INCR_NONE &&
            parray_bsearch(arguments->pgdata_and_dssdata_files, dest_file, pgFileCompareRelPathWithExternalDesc))
        {
            already_exists = true;
        }

        out_location = is_dss_type(dest_file->type) ? FIO_DSS_HOST : FIO_DB_HOST;
        /*
         * Handle incremental restore case for data files.
         * If file is already exists in pgdata, then
         * we scan it block by block and get
         * array of checksums for every page.
         */
        if (already_exists &&
            dest_file->is_datafile && !dest_file->is_cfs &&
            dest_file->n_blocks > 0)
        {
            if (arguments->incremental_mode == INCR_LSN)
            {
                lsn_map = fio_get_lsn_map(to_fullpath, arguments->dest_backup->checksum_version,
                                dest_file->n_blocks, arguments->shift_lsn,
                                dest_file->segno * RELSEG_SIZE, out_location);
            }
            else if (arguments->incremental_mode == INCR_CHECKSUM)
            {
                checksum_map = fio_get_checksum_map(to_fullpath, arguments->dest_backup->checksum_version,
                                                    dest_file->n_blocks, arguments->dest_backup->stop_lsn,
                                                    dest_file->segno * RELSEG_SIZE, out_location);
            }
        }

        /*
         * Open dest file and truncate it to zero, if destination
         * file already exists and dest file size is zero, or
         * if file do not exist
         */
        if ((already_exists && dest_file->write_size == 0) || !already_exists)
            out = fio_fopen(to_fullpath, PG_BINARY_W, out_location);
        /*
         * If file already exists and dest size is not zero,
         * then open it for reading and writing.
         */
        else
            out = fio_fopen(to_fullpath, PG_BINARY_R "+", out_location);

        if (out == NULL)
            elog(ERROR, "Cannot open restore target file \"%s\": %s",
                 to_fullpath, strerror(errno));

        /* update file permission */
        if (fio_chmod(to_fullpath, dest_file->mode, out_location) == -1)
            elog(ERROR, "Cannot change mode of \"%s\": %s", to_fullpath,
                 strerror(errno));

        // If destination file is 0 sized, then just close it and go for the next
        if (dest_file->write_size == 0)
            goto done;

        /* Restore destination file */
        if (dest_file->is_datafile && !dest_file->is_cfs)
        {
            /* enable stdio buffering for local destination data file */
            if (!fio_is_remote_file(out))
                setvbuf(out, out_buf, _IOFBF, STDIO_BUFSIZE);
            /* Destination file is data file */
            arguments->restored_bytes += restore_data_file(arguments->parent_chain, dest_file,
                                                           out, to_fullpath,
                                                           arguments->use_bitmap, checksum_map,
                                                           arguments->shift_lsn, lsn_map, true);
        }
        else
        {
            /* disable stdio buffering for local destination nonedata file */
            if (!fio_is_remote_file(out))
                setvbuf(out, NULL, _IONBF, BUFSIZ);
            /* Destination file is nonedata file */
            arguments->restored_bytes += restore_non_data_file(arguments->parent_chain,
                                                               arguments->dest_backup,
                                                               dest_file, out,
                                                               to_fullpath, already_exists);
        }

done:
        /* close file */
        if (fio_fclose(out) != 0)
            elog(ERROR, "Cannot close file \"%s\": %s", to_fullpath,
                 strerror(errno));

        RestoreCompressFile(out, to_fullpath, dest_file);
        /* free pagemap used for restore optimization */
        pg_free(dest_file->pagemap.bitmap);

        if (lsn_map)
            pg_free(lsn_map->bitmap);

        pg_free(lsn_map);
        pg_free(checksum_map);
    }

    pg_atomic_write_u32((volatile uint32*) &g_directoryFiles, directoryFilesLocal);
    free(out_buf);

    /* ssh connection to longer needed */
    fio_disconnect();

    /* Data files restoring is successful */
    arguments->ret = 0;

    return NULL;
}

/*
 * Create recovery.conf (probackup_recovery.conf in case of PG12)
 * with given recovery target parameters
 */
static void
create_recovery_conf(time_t backup_id,
                     pgRecoveryTarget *rt,
                     pgBackup *backup,
                     pgRestoreParams *params)
{
    char        path[MAXPGPATH];
    FILE       *fp;
    bool        pitr_requested;
    bool        target_latest;
    bool        target_immediate;
    bool         restore_command_provided = false;
    errno_t     rc = 0;

    if (instance_config.restore_command &&
        (pg_strcasecmp(instance_config.restore_command, "none") != 0))
    {
        restore_command_provided = true;
    }

    /* restore-target='latest' support */
    target_latest = rt->target_stop != NULL &&
        strcmp(rt->target_stop, "latest") == 0;

    target_immediate = rt->target_stop != NULL &&
        strcmp(rt->target_stop, "immediate") == 0;

    /*
     * Note that setting restore_command alone interpreted
     * as PITR with target - "until all available WAL is replayed".
     * We do this because of the following case:
     * The user is restoring STREAM backup as replica but
     * also relies on WAL archive to catch-up with master.
     * If restore_command is provided, then it should be
     * added to recovery config.
     * In this scenario, "would be" replica will replay
     * all WAL segments available in WAL archive, after that
     * it will try to connect to master via repprotocol.
     *
     * The risk is obvious, what if masters current state is
     * in "the past" relatively to latest state in the archive?
     * We will get a replica that is "in the future" to the master.
     * We accept this risk because its probability is low.
     * 
     * if rt recovery is not bigger than backup, we dont need to 
     * generate recovery.conf file
     * 
     * rt is malloc 0 before
     */
    pitr_requested =
            !backup->stream || backup->recovery_time < rt->target_time || backup->recovery_xid < rt->target_xid ||
            backup->stop_lsn < rt->target_lsn || rt->target_name || target_immediate || target_latest ||
            restore_command_provided;


    /* No need to generate recovery.conf at all. */
    if (!pitr_requested)
    {
        /*
         * Restoring STREAM backup without PITR and not as replica,
         * recovery.signal and standby.signal for PG12 are not needed
         *
         * We do not add "include" option in this case because
         * here we are creating empty "probackup_recovery.conf"
         * to handle possible already existing "include"
         * directive pointing to "probackup_recovery.conf".
         * If don`t do that, recovery will fail.
         */
        pg12_recovery_config(backup, false);
        return;
    }

    elog(LOG, "----------------------------------------");
#if PG_VERSION_NUM >= 120000
    elog(LOG, "creating probackup_recovery.conf");
    pg12_recovery_config(backup, true);
    rc = snprintf_s(path, lengthof(path), lengthof(path) - 1, "%s/probackup_recovery.conf", instance_config.pgdata);
    securec_check_ss_c(rc, "\0", "\0");
#else
    elog(LOG, "creating recovery.conf");
    rc = snprintf_s(path, lengthof(path), lengthof(path) - 1, "%s/recovery.conf", instance_config.pgdata);
    securec_check_ss_c(rc, "\0", "\0");
#endif

    fp = fio_fopen(path, "w", FIO_DB_HOST);
    if (fp == NULL)
        elog(ERROR, "cannot open file \"%s\": %s", path,
            strerror(errno));

    if (fio_chmod(path, FILE_PERMISSION, FIO_DB_HOST) == -1)
        elog(ERROR, "Cannot change mode of \"%s\": %s", path, strerror(errno));

#if PG_VERSION_NUM >= 120000
    fio_fprintf(fp, "# probackup_recovery.conf generated by gs_probackup %s\n",
                PROGRAM_VERSION);
#else
    fio_fprintf(fp, "# recovery.conf generated by gs_probackup %s\n",
                PROGRAM_VERSION);
#endif

    /* construct restore_command */
    if (pitr_requested)
    {
        construct_restore_cmd(fp, rt, restore_command_provided, target_immediate);
    }

    if (fio_fflush(fp) != 0 ||
        fio_fclose(fp))
        elog(ERROR, "cannot write file \"%s\": %s", path,
             strerror(errno));

#if PG_VERSION_NUM >= 120000
    /*
     * Create "recovery.signal" to mark this recovery as PITR for openGauss.
     * In older versions presense of recovery.conf alone was enough.
     * To keep behaviour consistent with older versions,
     * we are forced to create "recovery.signal"
     * even when only restore_command is provided.
     * Presense of "recovery.signal" by itself determine only
     * one thing: do openGauss must switch to a new timeline
     * after successfull recovery or not?
     */
    if (pitr_requested)
    {
        elog(LOG, "creating recovery.signal file");
        rc = snprintf_s(path, lengthof(path), lengthof(path) - 1, "%s/recovery.signal",
                   instance_config.pgdata);
        securec_check_ss_c(rc, "\0", "\0");
        fp = fio_fopen(path, "w", FIO_DB_HOST);
        if (fp == NULL)
            elog(ERROR, "cannot open file \"%s\": %s", path,
                strerror(errno));

        if (fio_fflush(fp) != 0 ||
            fio_fclose(fp))
            elog(ERROR, "cannot write file \"%s\": %s", path,
                 strerror(errno));
    }
#endif
}

static void construct_restore_cmd(FILE *fp, pgRecoveryTarget *rt,
                                  bool restore_command_provided,
                                  bool target_immediate)
{
    fio_fprintf(fp, "\n## recovery settings\n");

    /*
     * We've already checked that only one of the four following mutually
     * exclusive options is specified, so the order of calls is insignificant.
     */
    if (rt->target_name)
        fio_fprintf(fp, "recovery_target_name = '%s'\n", rt->target_name);

    if (rt->time_string)
        fio_fprintf(fp, "recovery_target_time = '%s'\n", rt->time_string);

    if (rt->xid_string)
        fio_fprintf(fp, "recovery_target_xid = '%s'\n", rt->xid_string);

    if (rt->lsn_string)
        fio_fprintf(fp, "recovery_target_lsn = '%s'\n", rt->lsn_string);

    if (rt->target_stop && target_immediate)
        fio_fprintf(fp, "recovery_target = '%s'\n", rt->target_stop);

    if (rt->inclusive_specified)
        fio_fprintf(fp, "recovery_target_inclusive = '%s'\n",
                rt->target_inclusive ? "true" : "false");

    (void)fio_fprintf(fp, "pause_at_recovery_target = '%s'\n", "false");

    if (rt->target_tli)
        fio_fprintf(fp, "recovery_target_timeline = '%u'\n", rt->target_tli);
    else
    {
        /*
         * In PG12 default recovery target timeline was changed to 'latest', which
         * is extremely risky. Explicitly preserve old behavior of recovering to current
         * timneline for PG12.
         */
#if PG_VERSION_NUM >= 120000
        fio_fprintf(fp, "recovery_target_timeline = 'current'\n");
#endif
    }
    
    if (restore_command_provided)
    {
        char restore_command_guc[16384];
        errno_t rc = sprintf_s(restore_command_guc, sizeof(restore_command_guc), "%s", instance_config.restore_command);
        securec_check_ss_c(rc, "\0", "\0");
        fio_fprintf(fp, "restore_command = '%s\n", restore_command_guc);
        elog(LOG, "Setting restore command to '%s'", restore_command_guc);
    } else {
        elog(WARNING, "you need to input restore command manually.");
        
    }
}

/*
 * Create empty probackup_recovery.conf in PGDATA and
 * add "include" directive to postgresql.auto.conf

 * When restoring PG12 we always(!) must do this, even
 * when restoring STREAM backup without PITR or replica options
 * because restored instance may have been previously backed up
 * and restored again and user didn`t cleaned up postgresql.auto.conf.

 * So for recovery to work regardless of all this factors
 * we must always create empty probackup_recovery.conf file.
 */
static void
pg12_recovery_config(pgBackup *backup, bool add_include)
{
#if PG_VERSION_NUM >= 120000
    char        probackup_recovery_path[MAXPGPATH];
    char        postgres_auto_path[MAXPGPATH];
    FILE       *fp;
    errno_t     rc = 0;

    if (add_include)
    {
        char        current_time_str[100];

        time2iso(current_time_str, lengthof(current_time_str), current_time);

        rc = snprintf_s(postgres_auto_path, lengthof(postgres_auto_path), lengthof(postgres_auto_path) - 1,
                    "%s/postgresql.auto.conf", instance_config.pgdata);
        securec_check_ss_c(rc, "\0", "\0");
        fp = fio_fopen(postgres_auto_path, "a", FIO_DB_HOST);
        if (fp == NULL)
            elog(ERROR, "cannot write to file \"%s\": %s", postgres_auto_path,
                strerror(errno));

        // TODO: check if include 'probackup_recovery.conf' already exists
        fio_fprintf(fp, "\n# created by gs_probackup restore of backup %s at '%s'\n",
            base36enc(backup->start_time), current_time_str);
        fio_fprintf(fp, "include '%s'\n", "probackup_recovery.conf");

        if (fio_fflush(fp) != 0 ||
            fio_fclose(fp))
            elog(ERROR, "cannot write to file \"%s\": %s", postgres_auto_path,
                strerror(errno));
    }

    /* Create empty probackup_recovery.conf */
    rc = snprintf_s(probackup_recovery_path, lengthof(probackup_recovery_path), lengthof(probackup_recovery_path) - 1,
        "%s/probackup_recovery.conf", instance_config.pgdata);
    securec_check_ss_c(rc, "\0", "\0");
    fp = fio_fopen(probackup_recovery_path, "w", FIO_DB_HOST);
    if (fp == NULL)
        elog(ERROR, "cannot open file \"%s\": %s", probackup_recovery_path,
            strerror(errno));

    if (fio_fflush(fp) != 0 ||
        fio_fclose(fp))
        elog(ERROR, "cannot write to file \"%s\": %s", probackup_recovery_path,
             strerror(errno));
#endif
    return;
}

/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component TLIs (the ancestor
 * timelines followed by target timeline). If we cannot find the history file,
 * assume that the timeline has no parents, and return a list of just the
 * specified timeline ID.
 * based on readTimeLineHistory() in timeline.c
 */
parray *
read_timeline_history(const char *arclog_path, TimeLineID targetTLI, bool strict)
{
#ifdef SUPPORT_MULTI_TIMELINE
    parray       *result;
    char        path[MAXPGPATH];
    FILE       *fd = NULL;
    TimeLineHistoryEntry *entry;
    TimeLineHistoryEntry *last_timeline = NULL;

    /* Look for timeline history file in archlog_path */
    errno_t rc = snprintf_s(path, lengthof(path), lengthof(path) - 1, "%s/%08X.history", arclog_path,
        targetTLI);
    securec_check_ss_c(rc, "\0", "\0");
    /* Timeline 1 does not have a history file */
    if (targetTLI != 1)
    {
        fd = fopen(path, "rt");
        if (fd == NULL)
        {
            if (errno != ENOENT)
                elog(ERROR, "could not open file \"%s\": %s", path,
                    strerror(errno));

            /* There is no history file for target timeline */
            if (strict)
                elog(ERROR, "recovery target timeline %u does not exist",
                     targetTLI);
            else
                return NULL;
        }
    }

    result = parray_new();

    parse_file(result, fd, entry, last_timeline);

    if (fd && (ferror(fd)))
            elog(ERROR, "Failed to read from file: \"%s\"", path);

    if (fd)
        fclose(fd);

    if (last_timeline && targetTLI <= last_timeline->tli)
        elog(ERROR, "Timeline IDs must be less than child timeline's ID.");

    /* append target timeline */
    entry = pgut_new(TimeLineHistoryEntry);
    entry->tli = targetTLI;
    /* LSN in target timeline is valid */
    entry->end = InvalidXLogRecPtr;
    parray_insert(result, 0, entry);

#endif
    return NULL;
}

#ifdef SUPPORT_MULTI_TIMELINE
static void parse_file(parray *result, FILE *fd,
                       TimeLineHistoryEntry *entry,
                       TimeLineHistoryEntry *last_timeline)
{
    char        fline[MAXPGPATH];

    /*
     * Parse the file...
     */
    while (fd && fgets(fline, sizeof(fline), fd) != NULL)
    {
        char       *ptr;
        TimeLineID    tli;
        uint32        switchpoint_hi;
        uint32        switchpoint_lo;
        int            nfields;

        for (ptr = fline; *ptr; ptr++)
        {
            if (!isspace((unsigned char) *ptr))
                break;
        }
        if (*ptr == '\0' || *ptr == '#')
            continue;

        nfields = sscanf_s(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo);

        if (nfields < 1)
        {
            /* expect a numeric timeline ID as first field of line */
            elog(ERROR,
                 "syntax error in history file: %s. Expected a numeric timeline ID.",
                   fline);
        }
        if (nfields != 3)
            elog(ERROR,
                 "syntax error in history file: %s. Expected a transaction log switchpoint location.",
                   fline);

        if (last_timeline && tli <= last_timeline->tli)
            elog(ERROR,
                   "Timeline IDs must be in increasing sequence.");

        entry = pgut_new(TimeLineHistoryEntry);
        entry->tli = tli;
        entry->end = ((uint64) switchpoint_hi << 32) | switchpoint_lo;

        last_timeline = entry;
        /* Build list with newest item first */
        parray_insert(result, 0, entry);

        /* we ignore the remainder of each line */
    }
}
#endif

/* TODO: do not ignore timelines. What if requested target located in different timeline? */
bool
satisfy_recovery_target(const pgBackup *backup, const pgRecoveryTarget *rt)
{
    if (rt->xid_string)
        return backup->recovery_xid <= rt->target_xid;

    if (rt->time_string)
        return backup->recovery_time <= rt->target_time;

    if (rt->lsn_string)
        return backup->stop_lsn <= rt->target_lsn;
    
    if (rt->target_name)
	    return strcmp(backup->recovery_name, rt->target_name) == 0;

    return true;
}


/* TODO description */
bool
satisfy_timeline(const parray *timelines, const pgBackup *backup)
{

#ifdef SUPPORT_MULTI_TIMELINE
    int            i;

    for (i = 0; i < parray_num(timelines); i++)
    {
        TimeLineHistoryEntry *timeline;

        timeline = (TimeLineHistoryEntry *) parray_get(timelines, i);
        if (backup->tli == timeline->tli &&
            (XLogRecPtrIsInvalid(timeline->end) ||
             backup->stop_lsn <= timeline->end))
            return true;
    }
#endif
    return false;
}

/* timelines represents a history of one particular timeline,
 * we must determine whether a target tli is part of that history.
 *
 *           /--------*
 * ---------*-------------->
 */
bool
tliIsPartOfHistory(const parray *timelines, TimeLineID tli)
{
#ifdef SUPPORT_MULTI_TIMELINE
    int            i;

    for (i = 0; i < parray_num(timelines); i++)
    {
        TimeLineHistoryEntry *timeline = (TimeLineHistoryEntry *) parray_get(timelines, i);

        if (tli == timeline->tli)
            return true;
    }

#endif
    return false;
}

/*
 * Get recovery options in the string format, parse them
 * and fill up the pgRecoveryTarget structure.
 */
pgRecoveryTarget *
parseRecoveryTargetOptions(const char *target_time,
                    const char *target_xid,
                    const char *target_inclusive,
                    TimeLineID    target_tli,
                    const char *target_lsn,
                    const char *target_stop,
                    const char *target_name,
                    const char *target_action)
{
    /*
     * count the number of the mutually exclusive options which may specify
     * recovery target. If final value > 1, throw an error.
     */
    int            recovery_target_specified = 0;
    pgRecoveryTarget *rt = pgut_new(pgRecoveryTarget);

    /* fill all options with default values */
    errno_t rc = memset_s(rt, sizeof(pgRecoveryTarget), 0, sizeof(pgRecoveryTarget));
    securec_check(rc, "\0", "\0");
    /* parse given options */
    if (target_time)
    {
        time_t        dummy_time;

        recovery_target_specified++;
        rt->time_string = target_time;

        if (parse_time(target_time, &dummy_time, false))
            rt->target_time = dummy_time;
        else
            elog(ERROR, "Invalid value for '--recovery-target-time' option '%s'",
                 target_time);
    }

    if (target_xid)
    {
        TransactionId dummy_xid;

        recovery_target_specified++;
        rt->xid_string = target_xid;

#ifdef PGPRO_EE
        if (parse_uint64(target_xid, &dummy_xid, 0))
#else
        if (parse_uint64(target_xid,&dummy_xid, 0))
#endif
            rt->target_xid = dummy_xid;
        else
            elog(ERROR, "Invalid value for '--recovery-target-xid' option '%s'",
                 target_xid);
    }

    if (target_lsn)
    {
        XLogRecPtr    dummy_lsn;

        recovery_target_specified++;
        rt->lsn_string = target_lsn;
        if (parse_lsn(target_lsn, &dummy_lsn))
            rt->target_lsn = dummy_lsn;
        else
            elog(ERROR, "Invalid value of '--recovery-target-lsn' option '%s'",
                 target_lsn);
    }

    rt->target_tli = target_tli;

    if (target_name)
    {
        recovery_target_specified++;
        rt->target_name = target_name;
    }

    parse_other_options(rt, recovery_target_specified,
                        target_inclusive, target_stop, target_action);

    /* More than one mutually exclusive option was defined. */
    if (recovery_target_specified > 1)
        elog(ERROR, "At most one of '--recovery-target', '--recovery-target-name', "
                    "'--recovery-target-time', '--recovery-target-xid' or "
                    "'--recovery-target-lsn' options can be specified");

    /*
     * If none of the options is defined, '--recovery-target-inclusive' option
     * is meaningless.
     */
    if (!(rt->xid_string || rt->time_string || rt->lsn_string) &&
        rt->target_inclusive)
        elog(ERROR, "The '--recovery-target-inclusive' option can be applied only when "
                    "either of '--recovery-target-time', '--recovery-target-xid' or "
                    "'--recovery-target-lsn' options is specified");

    /* If none of the options is defined, '--recovery-target-action' is meaningless */
    if (rt->target_action && recovery_target_specified == 0)
        elog(ERROR, "The '--recovery-target-action' option can be applied only when "
            "either of '--recovery-target', '--recovery-target-time', '--recovery-target-xid', "
            "'--recovery-target-lsn' or '--recovery-target-name' options is specified");

    /* TODO: sanity for recovery-target-timeline */

    return rt;
}

static void parse_other_options(pgRecoveryTarget *rt,
                                int recovery_target_specified,
                                const char *target_inclusive,
                                const char *target_stop,
                                const char *target_action)
{
    bool dummy_bool = false;

    if (target_inclusive)
    {
        rt->inclusive_specified = true;
        if (parse_bool(target_inclusive, &dummy_bool))
            rt->target_inclusive = dummy_bool;
        else
            elog(ERROR, "Invalid value for '--recovery-target-inclusive' option '%s'",
                 target_inclusive);
    }

    if (target_stop)
    {
        if ((strcmp(target_stop, "immediate") != 0)
            && (strcmp(target_stop, "latest") != 0))
            elog(ERROR, "Invalid value for '--recovery-target' option '%s'",
                 target_stop);

        recovery_target_specified++;
        rt->target_stop = target_stop;
    }

    if (target_action)
    {
        if ((strcmp(target_action, "pause") != 0)
            && (strcmp(target_action, "promote") != 0)
            && (strcmp(target_action, "shutdown") != 0))
            elog(ERROR, "Invalid value for '--recovery-target-action' option '%s'",
                 target_action);

        rt->target_action = target_action;
    }
}

/* Check that instance is suitable for incremental restore
 * Depending on type of incremental restore requirements are differs.
 */
void
check_incremental_compatibility(const char *pgdata, uint64 system_identifier,
                                IncrRestoreMode incremental_mode)
{
    uint64    system_id_pgdata;
    bool    success = true;
    pid_t   pid;
    char    backup_label[MAXPGPATH];
    errno_t rc = 0;

    /* slurp pg_control and check that system ID is the same */
    /* check that instance is not running */
    /* if lsn_based, check that there is no backup_label files is around AND
     * get redo point lsn from destination pg_control.

     * It is really important to be sure that pg_control is in cohesion with
     * data files content, because based on pg_control information we will
     * choose a backup suitable for lsn based incremental restore.
     */

    system_id_pgdata = get_system_identifier(pgdata);

    if (system_id_pgdata != instance_config.system_identifier)
    {
        elog(WARNING, "Backup catalog was initialized for system id %lu, "
                    "but destination directory system id is %lu",
                    system_identifier, system_id_pgdata);
        success = false;
    }

    /* check postmaster pid */
    pid = fio_check_postmaster(pgdata, FIO_DB_HOST);

    if (pid == 1) /* postmaster.pid is mangled */
    {
        char pid_file[MAXPGPATH];

        rc = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.pid", pgdata);
        securec_check_ss_c(rc, "\0", "\0");
        elog(WARNING, "Pid file \"%s\" is mangled, cannot determine whether postmaster is running or not",
            pid_file);
        success = false;
    }
    else if (pid > 1) /* postmaster is up */
    {
        elog(WARNING, "Postmaster with pid %u is running in destination directory \"%s\"",
            pid, pgdata);
        success = false;
    }

    /*
     * TODO: maybe there should be some other signs, pointing to pg_control
     * desynchronization with cluster state.
     */
    if (incremental_mode == INCR_LSN)
    {
        rc = snprintf_s(backup_label, MAXPGPATH, MAXPGPATH - 1, "%s/backup_label", pgdata);
        securec_check_ss_c(rc, "\0", "\0");
        if (current.media_type == MEDIA_TYPE_OSS) {
            restoreConfigFile(backup_label, true);
        }
        if (fio_access(backup_label, F_OK, FIO_DB_HOST) == 0)
        {
            elog(WARNING, "Destination directory contains \"backup_control\" file. "
                "This does NOT mean that you should delete this file and retry, only that "
                "incremental restore in 'lsn' mode may produce incorrect result, when applied "
                "to cluster with pg_control not synchronized with cluster state."
                "Consider to use incremental restore in 'checksum' mode");
            success = false;
        }
    }

    if (!success)
        elog(ERROR, "Incremental restore is impossible");
}

